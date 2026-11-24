// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

// recvBufPool recycles byte buffers for frame payload reads on the receive
// path. Buffers larger than maxPoolBufSize are not returned to prevent pool
// pollution from large snapshot transfers.
var recvBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

// snappyDecPool recycles Snappy decode destination buffers on the receive
// path to avoid per-frame allocations during decompression. Buffers are
// returned after the decompressed payload has been fully unmarshaled.
var snappyDecPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 256*1024)
		return &buf
	},
}

// putSnappyDecBuf returns a decode buffer to snappyDecPool, dropping ones above maxPoolBufSize.
func putSnappyDecBuf(bufPtr *[]byte) {
	if cap(*bufPtr) <= maxPoolBufSize {
		*bufPtr = (*bufPtr)[:0]
		snappyDecPool.Put(bufPtr)
	}
}

// batchPool recycles proto.MessageBatch structs on the receive path.
var batchPool = sync.Pool{
	New: func() any {
		return &proto.MessageBatch{
			Requests: make([]proto.Message, 0, 4),
		}
	},
}

// ipConnTracker counts active connections per source IP; every check-and-update runs under one mutex.
type ipConnTracker struct {
	mu     sync.Mutex
	counts map[string]int
	limit  int
}

// newIPConnTracker creates a tracker with the given per-IP connection limit.
func newIPConnTracker(limit int) *ipConnTracker {
	return &ipConnTracker{
		counts: make(map[string]int),
		limit:  limit,
	}
}

// tryAcquire increments the count for the given IP. Returns true if the
// count is within the limit, false if the limit would be exceeded. When
// false is returned, the count is not incremented.
func (tr *ipConnTracker) tryAcquire(ip string) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.counts[ip] >= tr.limit {
		return false
	}
	tr.counts[ip]++
	return true
}

// release decrements ip's count after a successful tryAcquire and removes the entry at zero.
func (tr *ipConnTracker) release(ip string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	count, ok := tr.counts[ip]
	if !ok {
		return
	}
	if count <= 1 {
		delete(tr.counts, ip)
		return
	}
	tr.counts[ip] = count - 1
}

// extractIP extracts the IP address from a net.Addr, stripping the port.
func extractIP(addr net.Addr) string {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// acceptLoop accepts inbound connections under the global and per-IP limits until stopped; supervised, and releases acceptWG exactly once.
func (t *QUICTransport) acceptLoop(sem chan struct{}, ipTracker *ipConnTracker) {
	for !t.stopped.Load() {
		// Back off after a recovered panic; a normal iteration re-enters immediately.
		if panicked := t.acceptLoopIteration(sem, ipTracker); panicked && !supervise.AwaitRestart(t.stopC) {
			break
		}
	}
	// Signalled separately from t.wg so stop() can wait for admissions to
	// stop without waiting for the handlers, which cannot finish until their
	// connections are closed. See QUICTransport.acceptWG.
	t.acceptWG.Done()
}

// startAcceptLoop starts acceptLoop under supervise.Supervise. The caller has
// already counted it in t.acceptWG.
func (t *QUICTransport) startAcceptLoop() {
	t.acceptSem = make(chan struct{}, t.cfg.MaxIncomingConnections)
	t.acceptIPs = newIPConnTracker(t.cfg.MaxConnectionsPerIP)
	sem, ipTracker := t.acceptSem, t.acceptIPs
	supervise.Supervise(&t.wg, t.stopC, func() { t.acceptLoop(sem, ipTracker) }, t.acceptWG.Done)
}

// logRecovered emits the diagnostic for a recovered panic or Goexit through supervise.LogContained; stack is captured by the caller.
func logRecovered(msg string, recovered any, stack []byte, attrs ...any) {
	supervise.LogContained(func() {
		slog.Error(msg, append(attrs, "panic", fmt.Sprint(recovered), "stack", string(stack))...)
	})
}

// acceptLoopIteration runs one accept iteration behind a panic recovery boundary and reports whether a panic was recovered.
func (t *QUICTransport) acceptLoopIteration(sem chan struct{}, ipTracker *ipConnTracker) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			logRecovered("panic recovered in acceptLoop, restarting", r, debug.Stack())
		}
	}()

	conn, err := t.listener.Accept(t.shutdownCtx)
	if err != nil {
		if t.stopped.Load() {
			return
		}
		// Transient error, log and continue.
		supervise.LogContained(func() { slog.Warn("transport accept error", "error", err) })
		return
	}

	// This goroutine owns the connection and its slots until a handler takes them; the deferred settlement releases them on every other exit.
	ip := extractIP(conn.RemoteAddr())
	var (
		owned       = true
		ipHeld      bool
		semHeld     bool
		closeCode   = QUICErrConnLimitReached
		closeReason = "connection limit reached"
		refusal     func()
	)
	defer func() {
		if semHeld {
			<-sem
		}
		if ipHeld {
			ipTracker.release(ip)
		}
		if !owned {
			return
		}
		closeErr := conn.CloseWithError(closeCode, closeReason)
		supervise.LogContained(func() {
			if refusal != nil {
				refusal()
			}
			if closeErr != nil {
				slog.Debug("transport: connection close failed after an admission refusal",
					"ip", ip, "error", closeErr)
			}
		})
	}()

	// Check per-IP connection limit.
	if !ipTracker.tryAcquire(ip) {
		closeCode, closeReason = QUICErrIPLimitReached, "per-IP connection limit reached"
		limit := t.cfg.MaxConnectionsPerIP
		refusal = func() {
			slog.Warn("per-IP connection limit reached", "ip", ip, "limit", limit)
		}
		return
	}
	ipHeld = true

	// Refuse a peer whose certificate also covers this node's host, before it takes a slot; see refuseInboundPromiscuousCert.
	refuse := t.refuseInboundPromiscuousCert
	if t.inboundRefusalForTest != nil {
		refuse = t.inboundRefusalForTest
	}
	if err := refuse(conn); err != nil {
		// The precise verdict is logged (after the cleanup) and kept out of
		// the CONNECTION_CLOSE reason phrase, which travels to the peer
		// verbatim. See closeReasonRedacted.
		closeCode, closeReason = QUICErrPromiscuousCert, closeReasonRedacted
		remote := conn.RemoteAddr().String()
		refusal = func() {
			slog.Warn("refusing inbound connection: peer certificate is not exclusive",
				"remote", remote, "error", err)
		}
		return
	}

	// Try to acquire global semaphore without blocking. At the global
	// connection limit the deferred settlement releases the per-IP slot and
	// closes the connection with QUICErrConnLimitReached.
	select {
	case sem <- struct{}{}:
		semHeld = true
	default:
		return
	}

	// Publish synchronously, after every refusal and before the handler starts, so stop()'s inbound sweep sees every admitted connection; see acceptWG.
	activity := t.publishInboundConn(conn)
	t.wg.Add(1)
	// Ownership passes to the handler goroutine: it releases both slots and
	// the connection's state when handleConnection returns.
	owned, ipHeld, semHeld = false, false, false
	go func() {
		defer func() {
			<-sem
			ipTracker.release(ip)
		}()
		defer t.wg.Done()
		t.handleConnection(conn, activity)
	}()
	return
}

// handleConnection accepts streams on an inbound connection and dispatches them, at most MaxStreamsPerConnection at once; activity is published by the accept loop.
func (t *QUICTransport) handleConnection(conn *quic.Conn, activity *inboundConnState) {
	// Drop the per-connection caches on return; deferred first so it runs after streamWG is waited on.
	defer t.forgetInboundConnState(conn)

	// Register the connection against its peer's refcounted receive budget, so the cap is per peer, not per connection.
	t.acquirePeerMsgBudgetRef(conn)

	// streamWG tracks the per-connection stream goroutines. The transport-wide
	// t.wg does not bound them to this connection's lifetime.
	var streamWG sync.WaitGroup
	defer streamWG.Wait()

	defer func() {
		if r := recover(); r != nil {
			logRecovered("panic recovered in handleConnection", r, debug.Stack(),
				"remote", conn.RemoteAddr())
		}
	}()

	// The connection context is cancelled when it closes, which unblocks AcceptStream.
	connCtx := conn.Context()
	// streamSem bounds data-stream goroutines per connection; a snapshot stream hands its slot to snapSem for the transfer.
	streamSem := make(chan struct{}, t.cfg.MaxStreamsPerConnection)
	snapSem := make(chan struct{}, t.cfg.MaxConcurrentSnapshotRecv)

	for {
		stream, err := conn.AcceptStream(connCtx)
		if err != nil {
			if t.stopped.Load() {
				return
			}
			// Connection closed or error.
			if isConnectionClosed(err) {
				return
			}
			var streamErr *quic.StreamError
			if errors.As(err, &streamErr) {
				continue
			}
			return
		}

		// Acquire a data-stream slot before dispatch. Snapshot streams hand
		// this slot off to snapSem inside handleStream once detected.
		select {
		case streamSem <- struct{}{}:
		case <-connCtx.Done():
			if closeErr := stream.Close(); closeErr != nil {
				remote := conn.RemoteAddr().String()
				supervise.LogContained(func() {
					slog.Debug("transport: stream close failed after connection context cancelled",
						"remote", remote, "error", closeErr)
				})
			}
			return
		}

		t.wg.Add(1)
		streamWG.Add(1)
		go func() {
			defer t.wg.Done()
			defer streamWG.Done()
			t.handleStream(conn, stream, activity, streamSem, snapSem)
		}()
	}
}

// handleStream reads frames from a stream and dispatches them by the first frame's type; it releases the caller's streamSem slot exactly once. activity may be nil in tests.
func (t *QUICTransport) handleStream(inboundConn *quic.Conn, stream *quic.Stream, activity *inboundConnState, streamSem, snapSem chan struct{}) {
	// streamSemHeld tracks ownership of the data-stream slot so it is released
	// exactly once. It is set false once the slot is released (on snapshot
	// handoff) to prevent a double release in the deferred cleanup.
	streamSemHeld := true
	defer func() {
		if streamSemHeld {
			<-streamSem
		}
	}()
	// cancelCode is the STOP_SENDING code; QUICStreamErrFrameRejected on protocol violations.
	cancelCode := QUICStreamErrReceiverGone
	defer func() {
		// Close only ends the send direction; CancelRead sends STOP_SENDING so the peer's next Write fails fast and the stream credit is released.
		stream.CancelRead(cancelCode)
		if closeErr := stream.Close(); closeErr != nil {
			// Contained: this deferred cleanup runs after the recover below
			// has, so a panicking log handler here would escape it, and
			// every cleanup deferred before this one still has to run.
			remote := remoteAddrOf(inboundConn)
			supervise.LogContained(func() {
				slog.Debug("transport: stream close failed after handleStream",
					"remote", remote, "error", closeErr)
			})
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			logRecovered("panic recovered in handleStream", r, debug.Stack(),
				"remote", inboundConn.RemoteAddr())
		}
	}()

	idleTimeout := t.cfg.StreamReadIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = defaultStreamReadIdleTimeout
	}

	for {
		if t.stopped.Load() {
			return
		}

		// A clean idle gap on a live connection returns errStreamIdle and keeps the stream open.
		length, flags, err := t.readFrameHeaderResilient(inboundConn, stream, idleTimeout, frameReadActiveTimeout)
		if err != nil {
			if errors.Is(err, errStreamIdle) {
				continue
			}
			if isProtocolViolation(err) {
				cancelCode = QUICStreamErrFrameRejected
			}
			t.recordProtocolViolation("inbound frame header rejected", inboundConn, err)
			return
		}

		// A frame arrived, so the connection is active; stamped per frame because streams are reused.
		activity.touch(time.Now())

		// A frame has begun arriving: complete its body (and any back-to-back
		// frame on a hot stream) under the active deadline so a half-delivered
		// frame cannot stall the receiver indefinitely.
		if derr := stream.SetReadDeadline(time.Now().Add(frameReadActiveTimeout)); derr != nil {
			return
		}

		if flags&FlagSnapshot != 0 {
			// Move from the data-stream budget to the snapshot budget for the transfer.
			acquired := t.handoffToSnapSem(streamSem, snapSem, inboundConn, &streamSemHeld)
			if !acquired {
				// Snapshot budget exhausted or connection closing: streamSem is
				// already released by handoffToSnapSem, so drop the snapshot
				// rather than blocking acceptance of new streams.
				return
			}
			// Release in a deferred closure so the slot returns even if handleSnapshotStream panics.
			func() {
				defer func() { <-snapSem }()
				t.handleSnapshotStream(inboundConn, stream, length)
			}()
			return
		}

		// Refuse message batches over 0-RTT, which is replayable (RFC 9001 §8); the snapshot path refuses it on its own.
		if inboundConn != nil && inboundConn.ConnectionState().Used0RTT {
			cnt := t.zeroRTTRefusals.Add(1)
			if LogSampled(&t.lastZeroRTTLog, protocolViolationLogInterval) {
				slog.Warn("message frame refused: arrived over 0-RTT early "+
					"data, which is replayable",
					"remote", remoteAddrOf(inboundConn),
					"total", cnt,
				)
			}
			return
		}

		if err := t.handleMessageFrame(inboundConn, stream, length, flags); err != nil {
			// Budget exhaustion is transient and the frame was drained, so keep the stream.
			if errors.Is(err, ErrMessageRecvMemoryBudgetExceeded) {
				continue
			}
			if isProtocolViolation(err) {
				cancelCode = QUICStreamErrFrameRejected
			}
			t.recordProtocolViolation("inbound message frame rejected", inboundConn, err)
			return
		}
	}
}

// protocolViolationLogInterval rate-limits the protocol-violation warning.
const protocolViolationLogInterval = 10 * time.Second

// LogSampled reports whether a peer-driven diagnostic may be emitted now, advancing the stamp; pair each call with an unconditional counter.
func LogSampled(stamp *atomic.Int64, interval time.Duration) bool {
	now := time.Now().UnixNano()
	last := stamp.Load()
	if now-last < int64(interval) {
		return false
	}
	return stamp.CompareAndSwap(last, now)
}

// recordProtocolViolation counts every frame-level protocol violation (shard 0, target 0) and logs a rate-limited warning; ordinary EOF and I/O errors are skipped.
func (t *QUICTransport) recordProtocolViolation(msg string, conn *quic.Conn, err error) {
	if err == nil || !isProtocolViolation(err) {
		return
	}
	if t.metrics != nil {
		t.metrics.IncMessagesDropped(0, 0, dropReasonProtocolViolation)
	}
	now := time.Now().UnixNano()
	last := t.lastProtocolViolationLog.Load()
	if now-last < int64(protocolViolationLogInterval) {
		return
	}
	if !t.lastProtocolViolationLog.CompareAndSwap(last, now) {
		return
	}
	remote := "<unknown>"
	if conn != nil {
		remote = conn.RemoteAddr().String()
	}
	slog.Warn(msg,
		"remote", remote,
		"error", err,
		"rate_limit", protocolViolationLogInterval,
	)
}

// isProtocolViolation reports whether err indicates a peer that is speaking a
// protocol this node cannot accept, as opposed to a transient transport error.
func isProtocolViolation(err error) bool {
	switch {
	case errors.Is(err, ErrInvalidMagic),
		errors.Is(err, ErrUnsupportedVersion),
		errors.Is(err, ErrFrameTooLarge),
		errors.Is(err, ErrBufferTooSmall),
		errors.Is(err, ErrPeerIdentityMismatch),
		errors.Is(err, ErrDecompressedSizeLimitExceeded),
		errors.Is(err, ErrDecompressionMismatch):
		return true
	// The decoder sentinels a malformed batch actually returns. Without them a
	// peer sending unparseable frames tears streams down with no counter and
	// no warning, which is indistinguishable from ordinary connection churn.
	case errors.Is(err, proto.ErrTooManyMessages),
		errors.Is(err, proto.ErrBufferTooSmall),
		errors.Is(err, proto.ErrInvalidEntryType),
		errors.Is(err, proto.ErrInvalidMessageType),
		errors.Is(err, proto.ErrDuplicateMapKey),
		errors.Is(err, proto.ErrUnsupportedVersion):
		return true
	}
	var decompressErr *FrameDecompressError
	return errors.As(err, &decompressErr)
}

// errStreamIdle signals a read deadline at a clean frame boundary on a live connection; the stream is kept open.
var errStreamIdle = errors.New("transport: stream idle on live connection")

// isTimeoutErr reports whether err is an i/o deadline-exceeded (read/write
// timeout), including the timeout error quic-go surfaces from a stream whose
// read/write deadline elapsed.
func isTimeoutErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// readFrameHeaderResilient reads a frame header, returning errStreamIdle for an idle gap on a live connection; a started header must finish within activeTimeout.
func (t *QUICTransport) readFrameHeaderResilient(conn *quic.Conn, stream *quic.Stream, idleTimeout, activeTimeout time.Duration) (length uint32, flags FrameFlags, err error) {
	var header [FrameHeaderSize]byte

	if derr := stream.SetReadDeadline(time.Now().Add(idleTimeout)); derr != nil {
		return 0, FlagNone, derr
	}
	n, rerr := io.ReadFull(stream, header[:])
	if rerr != nil {
		switch {
		case n == 0 && isTimeoutErr(rerr) && conn.Context().Err() == nil:
			// Clean idle boundary on a live connection — not a failure.
			return 0, FlagNone, errStreamIdle
		case n > 0 && n < len(header):
			// A frame started but its header did not complete within
			// idleTimeout. Finish it under the active deadline; a stall now is
			// a genuine slow/dead peer.
			if derr := stream.SetReadDeadline(time.Now().Add(activeTimeout)); derr != nil {
				return 0, FlagNone, derr
			}
			if _, rerr2 := io.ReadFull(stream, header[n:]); rerr2 != nil {
				return 0, FlagNone, rerr2
			}
		default:
			return 0, FlagNone, rerr
		}
	}

	if verr := ValidateFrameHeaderWindow(header[:], t.cfg.Wire); verr != nil {
		return 0, FlagNone, verr
	}
	return binary.BigEndian.Uint32(header[4:8]), FrameFlags(header[3]), nil
}

// handoffToSnapSem releases the streamSem slot (clearing streamSemHeld with it) and tries snapSem without blocking; true means the caller owns a snapSem slot.
func (t *QUICTransport) handoffToSnapSem(streamSem, snapSem chan struct{}, inboundConn *quic.Conn, streamSemHeld *bool) bool {
	<-streamSem
	*streamSemHeld = false
	// Check shutdown first; a select with an available slot could otherwise accept during shutdown.
	if inboundConn.Context().Err() != nil {
		return false
	}
	select {
	case <-t.stopC:
		return false
	default:
	}
	select {
	case snapSem <- struct{}{}:
		return true
	default:
		n := t.snapshotStreamDrops.Add(1)
		if LogSampled(&t.lastSnapshotRejectLog, protocolViolationLogInterval) {
			remote, limit := remoteAddrOf(inboundConn), t.cfg.MaxConcurrentSnapshotRecv
			supervise.LogContained(func() {
				slog.Warn("snapshot stream dropped: per-connection snapshot limit reached",
					"remote", remote, "limit", limit, "total", n)
			})
		}
		return false
	}
}

// handleMessageFrame reads a message frame, Snappy-decompressing it when FlagCompressed is set, validates the deployment ID and dispatches the batch.
//
//nolint:gocyclo // frame handling covers compression, encryption, validation, and routing
func (t *QUICTransport) handleMessageFrame(inboundConn *quic.Conn, stream io.Reader, length uint32, flags FrameFlags) error {
	if length == 0 {
		return nil
	}

	// Reserve the frame's decode charge against the per-peer and aggregate budgets before allocating; on refusal hold nothing and drain the body.
	charge := proto.DecodeCharge(int64(length))
	if !t.reserveConnMsgRecvBudget(inboundConn, charge) {
		n := t.recvBudgetDrops.Add(1)
		if LogSampled(&t.lastRecvBudgetLog, protocolViolationLogInterval) {
			remote, limit := remoteAddrOf(inboundConn), t.maxConnMsgRecv
			supervise.LogContained(func() {
				slog.Warn("message frame dropped: per-connection receive memory budget exhausted",
					"remote", remote, "length", length, "per_conn_limit", limit, "total", n)
			})
		}
		return t.dropFrame(stream, length, ErrMessageRecvMemoryBudgetExceeded)
	}
	if !t.reserveMsgRecvBudget(charge) {
		t.releaseConnMsgRecvBudget(inboundConn, charge)
		n := t.recvBudgetDrops.Add(1)
		if LogSampled(&t.lastRecvBudgetLog, protocolViolationLogInterval) {
			remote := remoteAddrOf(inboundConn)
			supervise.LogContained(func() {
				slog.Warn("message frame dropped: receive memory budget exhausted",
					"remote", remote, "length", length, "total", n)
			})
		}
		return t.dropFrame(stream, length, ErrMessageRecvMemoryBudgetExceeded)
	}
	defer t.releaseConnMsgRecvBudget(inboundConn, charge)
	defer t.releaseMsgRecvBudget(charge)

	var recvStart time.Time
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		recvStart = time.Now()
	}

	buf := t.getRecvBuf(int(length))

	payload := (*buf)[:length]
	if _, err := io.ReadFull(stream, payload); err != nil {
		t.putRecvBuf(buf)
		return err
	}

	// snappyBuf is a pooled decode buffer returned after the payload's Cmd aliases are copied out.
	var snappyBuf *[]byte

	// Decompress if the frame was compressed on the send side.
	if flags&FlagCompressed != 0 {
		// Validate declared decompressed size before allocation.
		declaredLen, err := snappy.DecodedLen(payload)
		if err != nil {
			t.putRecvBuf(buf)
			return &FrameDecompressError{Err: err}
		}
		if declaredLen < 0 || declaredLen > int(t.cfg.MaxDecompressedSize) {
			t.putRecvBuf(buf)
			return ErrDecompressedSizeLimitExceeded
		}
		// Reserve the decompressed size beyond the compressed length before decoding.
		if delta := proto.DecodeCharge(int64(declaredLen) - int64(length)); delta > 0 {
			if !t.reserveConnMsgRecvBudget(inboundConn, delta) {
				t.putRecvBuf(buf)
				n := t.recvBudgetDrops.Add(1)
				if LogSampled(&t.lastRecvBudgetLog, protocolViolationLogInterval) {
					remote, limit := remoteAddrOf(inboundConn), t.maxConnMsgRecv
					supervise.LogContained(func() {
						slog.Warn("compressed message frame dropped: per-connection receive memory budget exhausted",
							"remote", remote, "compressed_length", length,
							"declared_decompressed_length", declaredLen, "per_conn_limit", limit, "total", n)
					})
				}
				return ErrMessageRecvMemoryBudgetExceeded
			}
			defer t.releaseConnMsgRecvBudget(inboundConn, delta)
			if !t.reserveMsgRecvBudget(delta) {
				t.putRecvBuf(buf)
				n := t.recvBudgetDrops.Add(1)
				if LogSampled(&t.lastRecvBudgetLog, protocolViolationLogInterval) {
					remote := remoteAddrOf(inboundConn)
					supervise.LogContained(func() {
						slog.Warn("compressed message frame dropped: receive memory budget exhausted",
							"remote", remote, "compressed_length", length,
							"declared_decompressed_length", declaredLen, "total", n)
					})
				}
				return ErrMessageRecvMemoryBudgetExceeded
			}
			defer t.releaseMsgRecvBudget(delta)
		}
		snappyBuf = snappyDecPool.Get().(*[]byte)
		// snappy.Decode sizes by len(dst), so pass the buffer's full capacity.
		decompressed, err := snappy.Decode((*snappyBuf)[:cap(*snappyBuf)], payload)
		if err != nil {
			putSnappyDecBuf(snappyBuf)
			snappyBuf = nil
			t.putRecvBuf(buf)
			return &FrameDecompressError{Err: err}
		}
		if len(decompressed) != declaredLen {
			*snappyBuf = decompressed[:0]
			putSnappyDecBuf(snappyBuf)
			snappyBuf = nil
			t.putRecvBuf(buf)
			return ErrDecompressionMismatch
		}
		// Update the pool buffer pointer to track the (possibly grown)
		// backing array so future callers benefit from the larger capacity.
		*snappyBuf = decompressed
		t.putRecvBuf(buf)
		payload = decompressed
		buf = nil // buffer already returned to pool
	}

	// returnSnappyBuf returns the Snappy decode buffer to the pool after
	// resetting its length to zero so the capacity is retained for reuse.
	// Oversized buffers are dropped by putSnappyDecBuf's maxPoolBufSize cap.
	returnSnappyBuf := func() {
		if snappyBuf != nil {
			putSnappyDecBuf(snappyBuf)
			snappyBuf = nil
		}
	}

	batch := batchPool.Get().(*proto.MessageBatch)
	if _, err := batch.UnmarshalFromWindow(payload, t.cfg.Wire.Proto); err != nil {
		batch.Reset()
		batchPool.Put(batch)
		if buf != nil {
			t.putRecvBuf(buf)
		}
		returnSnappyBuf()
		return err
	}

	// Validate ShardID != 0 for all inbound messages. Zero is reserved
	// and indicates corruption or a malicious peer. Drop the entire
	// batch rather than returning an error that would close the connection.
	for i := range batch.Requests {
		if batch.Requests[i].ShardID == 0 {
			n := t.zeroShardDrops.Add(1)
			from, msgType, source := batch.Requests[i].From, batch.Requests[i].Type, batch.SourceAddress
			batch.Reset()
			batchPool.Put(batch)
			if buf != nil {
				t.putRecvBuf(buf)
			}
			returnSnappyBuf()
			if LogSampled(&t.lastZeroShardLog, protocolViolationLogInterval) {
				supervise.LogContained(func() {
					slog.Warn("dropping message with ShardID=0",
						"from", from, "type", proto.MessageTypeName(msgType), "source", source, "total", n)
				})
			}
			return nil
		}
	}

	// Validate the deployment ID before any connection-state change.
	if batch.DeploymentID != t.cfg.DeploymentID {
		batch.Reset()
		batchPool.Put(batch)
		if buf != nil {
			t.putRecvBuf(buf)
		}
		returnSnappyBuf()
		return ErrPeerIdentityMismatch
	}

	// Clear a SourceAddress the connection's certificate does not cover; the decision is cached per connection and address.
	if batch.SourceAddress != "" && !t.inboundAuthorizedForCached(inboundConn, batch.SourceAddress) {
		n := t.spoofedSourceClears.Add(1)
		if LogSampled(&t.lastSpoofedSourceLog, protocolViolationLogInterval) {
			claimed, remote := batch.SourceAddress, remoteAddrOf(inboundConn)
			supervise.LogContained(func() {
				slog.Warn("clearing spoofed source address: not covered by peer certificate",
					"claimed_source", claimed, "remote", remote, "total", n)
			})
		}
		batch.SourceAddress = ""
	}

	// Evict a stale outbound on a peer restart or dead cached connection; skipped when SourceAddress was cleared.
	t.evictStaleOutboundOnRecv(batch.SourceAddress, batch.SourceEpoch, inboundConn)

	// Validate message count.
	if len(batch.Requests) > proto.MaxMessagesPerBatch {
		batch.Reset()
		batchPool.Put(batch)
		if buf != nil {
			t.putRecvBuf(buf)
		}
		returnSnappyBuf()
		return proto.ErrTooManyMessages
	}

	// Copy Entry.Cmd out of the receive buffer in one allocation before the buffer is recycled; Cmd is the only alias left.
	totalCmdBytes := 0
	for i := range batch.Requests {
		for j := range batch.Requests[i].Entries {
			totalCmdBytes += len(batch.Requests[i].Entries[j].Cmd)
		}
	}
	if totalCmdBytes > 0 {
		bulk := make([]byte, totalCmdBytes)
		off := 0
		for i := range batch.Requests {
			for j := range batch.Requests[i].Entries {
				cmd := batch.Requests[i].Entries[j].Cmd
				if len(cmd) > 0 {
					copy(bulk[off:], cmd)
					batch.Requests[i].Entries[j].Cmd = bulk[off : off+len(cmd) : off+len(cmd)]
					off += len(cmd)
				}
			}
		}
	}

	// Return the receive buffer and Snappy decode buffer to their pools
	// now that all zero-copy aliases have been replaced with owned copies.
	if buf != nil {
		t.putRecvBuf(buf)
	}
	returnSnappyBuf()

	// Recycle the batch only when no message carries entries; the step worker reads them later.
	hasEntries := false
	for i := range batch.Requests {
		if len(batch.Requests[i].Entries) > 0 {
			hasEntries = true
			break
		}
	}
	// Bind the batch's claims to the connection's authenticated node identity and certificate coverage.
	peer := PeerAuth{
		Identity: peerNodeIdentity(inboundConn),
		AuthorizesAddress: func(address string) bool {
			return t.inboundAuthorizedForCached(inboundConn, address)
		},
	}
	handleErr := t.handler.HandleMessage(*batch, peer)
	// Record metrics after the hand-off; the collector is user code.
	if t.metrics != nil {
		for i := range batch.Requests {
			t.metrics.IncMessagesReceived(
				batch.Requests[i].ShardID,
				batch.Requests[i].From,
				batch.Requests[i].Type,
			)
		}
	}
	if !recvStart.IsZero() && len(batch.Requests) > 0 {
		slog.Debug("message batch received",
			"source", batch.SourceAddress,
			"batch_size", len(batch.Requests),
			"frame_bytes", length,
			"recv_ns", time.Since(recvStart).Nanoseconds(),
		)
	}
	if !hasEntries {
		batch.Reset()
		batchPool.Put(batch)
	}
	return handleErr
}

// handleSnapshotStream receives the chunks of a snapshot stream; inboundConn's verified certificate binds chunk.From before anything is persisted.
func (t *QUICTransport) handleSnapshotStream(inboundConn *quic.Conn, stream io.Reader, headerLen uint32) {
	t.processSnapshotHeader(inboundConn, stream, headerLen)
}

// maxSnapshotHeaderWireLen bounds a snapshot header frame's declared length.
const maxSnapshotHeaderWireLen = 4096

// processSnapshotHeader reads and validates the snapshot header from the
// stream, then delegates to the snapshot receiver for chunk processing.
// Separated from handleSnapshotStream to accept io.Reader for testability.
func (t *QUICTransport) processSnapshotHeader(inboundConn *quic.Conn, r io.Reader, headerLen uint32) {
	// Every drop goes through rejectSnapshotStream so the listener and a warning fire.
	if headerLen < SnapshotHeaderSize {
		t.snapshotRecv.rejectSnapshotStream(snapshotRejectMalformed, "declared header length below minimum", nil,
			&SnapshotHeaderLengthError{Declared: headerLen, Min: SnapshotHeaderSize, Max: maxSnapshotHeaderWireLen})
		return
	}
	if headerLen > maxSnapshotHeaderWireLen {
		t.snapshotRecv.rejectSnapshotStream(snapshotRejectMalformed, "declared header length exceeds bound", nil,
			&SnapshotHeaderLengthError{Declared: headerLen, Min: SnapshotHeaderSize, Max: maxSnapshotHeaderWireLen})
		return
	}

	var hdrBuf [SnapshotHeaderSize]byte
	if _, err := io.ReadFull(r, hdrBuf[:]); err != nil {
		t.snapshotRecv.rejectSnapshotStream(snapshotRejectIO, "header read failed", nil, err)
		return
	}
	// Discard any remaining (forward-compat) header bytes without allocating
	// the declared size: io.Discard copies through a small internal buffer.
	if headerLen > SnapshotHeaderSize {
		if _, err := io.CopyN(io.Discard, r, int64(headerLen-SnapshotHeaderSize)); err != nil {
			t.snapshotRecv.rejectSnapshotStream(snapshotRejectIO, "forward-compat header discard failed", nil, err)
			return
		}
	}

	var hdr SnapshotHeader
	if _, err := hdr.UnmarshalFrom(hdrBuf[:]); err != nil {
		t.snapshotRecv.rejectSnapshotStream(snapshotRejectMalformed, "header unmarshal failed", nil, err)
		return
	}

	// The ceiling is the lower of the protocol maximum and what the configured
	// snapshot memory budget can hold, so a peer cannot commit the receiver to
	// accumulating more chunk headers than the budget accounts for.
	if maxChunks := t.snapshotRecv.maxAdmissibleChunkCount(); hdr.ChunkCount > maxChunks {
		t.snapshotRecv.rejectSnapshotStream(snapshotRejectMalformed, "declared chunk count exceeds bound", &hdr,
			&SnapshotChunkCountError{Declared: hdr.ChunkCount, Max: maxChunks})
		return
	}

	// Reject snapshots over 0-RTT, which is replayable (RFC 9001 §8).
	if inboundConn != nil && inboundConn.ConnectionState().Used0RTT {
		t.snapshotRecv.rejectSnapshotStream(snapshotRejectPolicy, "received over 0-RTT (replayable early data)", &hdr, nil)
		return
	}

	// Delegate to the snapshot receiver; authorization binds the sender to inboundConn's certificate, failing closed when nil.
	if qStream, ok := r.(*quic.Stream); ok {
		peer := PeerAuth{
			Identity: peerNodeIdentity(inboundConn),
			AuthorizesAddress: func(address string) bool {
				return t.inboundAuthorizedForCached(inboundConn, address)
			},
		}
		t.snapshotRecv.receiveSnapshot(qStream, &hdr, snapshotFairnessKey(inboundConn), peer, t.stopC)
	} else {
		slog.Warn("snapshot header received on non-QUIC stream, snapshot dropped",
			"reader_type", fmt.Sprintf("%T", r))
	}
}

// remoteAddrOf returns a printable remote address for a possibly-nil
// connection.
func remoteAddrOf(conn *quic.Conn) string {
	if conn == nil {
		return "<unknown>"
	}
	return conn.RemoteAddr().String()
}

// snapshotFairnessKey returns the peer identity snapshot slots are counted against: the leaf SPKI SHA-256, else the peer IP, never the source port; empty for a nil connection.
func snapshotFairnessKey(conn *quic.Conn) string {
	if conn == nil {
		return ""
	}
	if identity := peerNodeIdentity(conn); identity != "" {
		return identity
	}
	return "ip:" + extractIP(conn.RemoteAddr())
}

// peerNodeIdentity returns the connection peer's verified node identity (leaf SPKI hash), or "" without a verified leaf.
func peerNodeIdentity(conn *quic.Conn) string {
	if conn == nil {
		return ""
	}
	certs := conn.ConnectionState().TLS.PeerCertificates
	if len(certs) == 0 {
		return ""
	}
	return NodeIdentityFromCertificate(certs[0])
}

// NodeIdentityFromCertificate returns "spki:" plus the SHA-256 of cert's RawSubjectPublicKeyInfo, or "" for nil.
func NodeIdentityFromCertificate(leaf *x509.Certificate) string {
	if leaf == nil {
		return ""
	}
	sum := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	return "spki:" + hex.EncodeToString(sum[:])
}

// LocalNodeIdentity returns this node's identity from its own leaf, or "" when none.
func (t *QUICTransport) LocalNodeIdentity() string {
	pair := t.currentTLS()
	if pair == nil {
		return ""
	}
	return NodeIdentityFromCertificate(pair.leaf)
}

// discardFrameBody drains an unprocessed frame body and errors if fewer than length bytes were read; then the stream must not be read further.
func (t *QUICTransport) discardFrameBody(r io.Reader, length uint32) (int64, error) {
	if length == 0 {
		return 0, nil
	}
	n, err := io.CopyN(io.Discard, r, int64(length))
	if err != nil {
		slog.Debug("transport: discarding dropped frame body failed",
			"declared", length, "drained", n, "error", err)
		return n, err
	}
	return n, nil
}

// dropFrame drains an unprocessed frame and returns the caller's error, or a non-retryable FrameDrainError on an incomplete drain.
func (t *QUICTransport) dropFrame(r io.Reader, length uint32, transient error) error {
	drained, err := t.discardFrameBody(r, length)
	if err != nil {
		return &FrameDrainError{Declared: length, Drained: drained, Err: err}
	}
	return transient
}

// reserveMsgRecvBudget reserves n bytes of the aggregate receive budget, reserving nothing on failure; pair success with releaseMsgRecvBudget.
func (t *QUICTransport) reserveMsgRecvBudget(n int64) bool {
	if t.msgRecvBudget.Add(-n) < 0 {
		t.msgRecvBudget.Add(n)
		return false
	}
	return true
}

// releaseMsgRecvBudget returns n reserved bytes to the aggregate receive budget.
func (t *QUICTransport) releaseMsgRecvBudget(n int64) {
	t.msgRecvBudget.Add(n)
}

// getRecvBuf gets a buffer from the pool, ensuring it has at least minCap capacity.
// When the pooled buffer is too small, it is returned to the pool before
// allocating a replacement to avoid leaking pooled buffers.
func (t *QUICTransport) getRecvBuf(minCap int) *[]byte {
	bufPtr := recvBufPool.Get().(*[]byte)
	if cap(*bufPtr) < minCap {
		t.putRecvBuf(bufPtr)
		buf := make([]byte, minCap)
		return &buf
	}
	*bufPtr = (*bufPtr)[:minCap]
	return bufPtr
}

// putRecvBuf returns a buffer to the pool if not oversized.
func (t *QUICTransport) putRecvBuf(bufPtr *[]byte) {
	if cap(*bufPtr) <= maxPoolBufSize {
		*bufPtr = (*bufPtr)[:0]
		recvBufPool.Put(bufPtr)
	}
}
