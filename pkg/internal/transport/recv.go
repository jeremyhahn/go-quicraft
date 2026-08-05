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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/golang/snappy"
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

// putSnappyDecBuf returns a Snappy decode buffer to snappyDecPool, applying
// the same maxPoolBufSize cap every other buffer pool in the package applies.
// A decode destination can grow to MaxDecompressedSize; retaining such
// buffers would pin up to that much unaccounted memory per P.
func putSnappyDecBuf(bufPtr *[]byte) {
	if cap(*bufPtr) <= maxPoolBufSize {
		*bufPtr = (*bufPtr)[:0]
		snappyDecPool.Put(bufPtr)
	}
}

// batchPool recycles proto.MessageBatch structs on the receive path.
// Reusing the Requests slice avoids allocating a new []Message on every
// frame. For heartbeat responses (the SyncRead critical path), this
// eliminates the only heap allocation in the unmarshal path since
// heartbeat messages have no entries or snapshot data.
var batchPool = sync.Pool{
	New: func() any {
		return &proto.MessageBatch{
			Requests: make([]proto.Message, 0, 4),
		}
	},
}

// ipConnTracker tracks the number of active connections per source IP.
// All check-and-update operations run under a single mutex so the count
// for an IP can never be observed or mutated through a stale entry: a
// CAS-outside-the-lock scheme allows an increment to land on a counter
// that release has already pruned from the map, undercounting the IP and
// letting it exceed the limit.
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

// release decrements the count for the given IP. The caller must have
// previously called tryAcquire successfully for the same IP. When the
// count reaches zero, the entry is removed from the map to prevent
// unbounded growth from transient connections.
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

// acceptLoop accepts incoming QUIC connections up to MaxIncomingConnections.
// A semaphore limits total concurrency, and an ipConnTracker enforces the
// per-IP connection limit. The loop runs until the transport is stopped.
func (t *QUICTransport) acceptLoop() {
	defer t.wg.Done()

	sem := make(chan struct{}, t.cfg.MaxIncomingConnections)
	ipTracker := newIPConnTracker(t.cfg.MaxConnectionsPerIP)

	for {
		if t.stopped.Load() {
			return
		}
		// On a recovered panic, back off before re-entering to avoid a tight
		// CPU loop and log flood, mirroring sendQueueWorker and
		// connectionCleanup. A normal iteration (no panic) re-enters
		// immediately so accept latency is unaffected.
		if panicked := t.acceptLoopIteration(sem, ipTracker); panicked {
			select {
			case <-t.stopC:
				return
			case <-time.After(workerPanicRestartDelay):
			}
		}
	}
}

// acceptLoopIteration executes a single iteration of the accept loop.
// It is separated from acceptLoop to provide a deferred panic recovery
// boundary. If a panic occurs during connection acceptance or dispatch,
// the deferred recover logs the panic with a full stack trace and
// returns control to the outer for-loop, which checks the stopped flag
// and re-enters the next iteration. This ensures the accept loop is
// never permanently killed by a panic. It returns true if a panic was
// recovered so the caller can apply a restart backoff.
func (t *QUICTransport) acceptLoopIteration(sem chan struct{}, ipTracker *ipConnTracker) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			slog.Error("panic recovered in acceptLoop, restarting",
				"panic", fmt.Sprint(r),
				"stack", string(debug.Stack()),
			)
		}
	}()

	conn, err := t.listener.Accept(t.shutdownCtx)
	if err != nil {
		if t.stopped.Load() {
			return
		}
		// Transient error, log and continue.
		slog.Warn("transport accept error", "error", err)
		return
	}

	// Check per-IP connection limit.
	ip := extractIP(conn.RemoteAddr())
	if !ipTracker.tryAcquire(ip) {
		slog.Warn("per-IP connection limit reached",
			"ip", ip,
			"limit", t.cfg.MaxConnectionsPerIP,
		)
		if closeErr := conn.CloseWithError(QUICErrIPLimitReached, "per-IP connection limit reached"); closeErr != nil {
			slog.Debug("transport: connection close failed after IP limit reached",
				"ip", ip,
				"error", closeErr,
			)
		}
		return
	}

	// Try to acquire global semaphore without blocking.
	select {
	case sem <- struct{}{}:
		t.wg.Add(1)
		go func() {
			defer func() {
				<-sem
				ipTracker.release(ip)
			}()
			defer t.wg.Done()
			t.handleConnection(conn)
		}()
	default:
		// At global connection limit, reject and release IP slot.
		ipTracker.release(ip)
		if closeErr := conn.CloseWithError(QUICErrConnLimitReached, "connection limit reached"); closeErr != nil {
			slog.Debug("transport: connection close failed after global limit reached",
				"ip", ip,
				"error", closeErr,
			)
		}
	}
	return
}

// handleConnection accepts streams on an inbound connection and dispatches
// them to either the message handler or snapshot handler based on the first
// frame's flags. A channel-based semaphore limits concurrent stream-processing
// goroutines to MaxStreamsPerConnection, preventing resource exhaustion from
// peers opening excessive streams.
func (t *QUICTransport) handleConnection(conn *quic.Conn) {
	// Drop the per-connection caches when this inbound connection's handler
	// returns, so they do not grow unbounded across connection churn.
	//
	// Deferred FIRST so it runs LAST: streamWG below must be waited on before
	// the delete. handleConnection returns as soon as AcceptStream errors,
	// while handleStream goroutines may still be in flight; one of them calling
	// inboundAuthorizedForCached after the delete would LoadOrStore a fresh
	// entry for a connection nothing will ever forget again, permanently
	// pinning the *quic.Conn and its buffers.
	defer t.forgetInboundConnState(conn)

	// streamWG tracks the per-connection stream goroutines. The transport-wide
	// t.wg does not bound them to this connection's lifetime.
	var streamWG sync.WaitGroup
	defer streamWG.Wait()

	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic recovered in handleConnection",
				"remote", conn.RemoteAddr().String(),
				"panic", fmt.Sprint(r),
				"stack", string(debug.Stack()),
			)
		}
	}()

	// Use the QUIC connection's context for AcceptStream. This context is
	// automatically cancelled when the connection closes (peer disconnect,
	// idle timeout, or local close during shutdown), which unblocks
	// AcceptStream on half-open connections without depending on the
	// transport's stopC signal.
	connCtx := conn.Context()
	// streamSem bounds concurrent data-stream processing goroutines per
	// connection. A stream initially holds a streamSem slot while its first
	// frame header is read; if the stream turns out to be a snapshot, the
	// slot is handed off to the dedicated snapSem (sized to
	// MaxConcurrentSnapshotRecv) for the duration of the slow transfer. This
	// guarantees that concurrent multi-second snapshot transfers never consume
	// the data-stream budget and starve acceptance of replication/heartbeat
	// streams. The QUIC MaxIncomingStreams limit (set in NewQUICTransport)
	// reserves snapshotStreamHeadroom on top of the data-stream pool so flow
	// control matches this combined processing capacity.
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
				slog.Debug("transport: stream close failed after connection context cancelled",
					"remote", conn.RemoteAddr().String(),
					"error", closeErr,
				)
			}
			return
		}

		t.wg.Add(1)
		streamWG.Add(1)
		go func() {
			defer t.wg.Done()
			defer streamWG.Done()
			t.handleStream(conn, stream, streamSem, snapSem)
		}()
	}
}

// handleStream reads frames from a QUIC stream and dispatches them to the
// appropriate handler. The first frame determines the stream type: snapshot
// streams have FlagSnapshot set, regular streams carry message batches.
//
// Shutdown detection uses t.stopped.Load() (a single atomic read) instead
// of a channel select on every iteration. The stream's read deadline and
// QUIC connection close (triggered by Stop) naturally break the loop,
// making the channel check redundant overhead on the hot receive path.
//
// On entry the caller holds one slot in streamSem. handleStream releases that
// slot exactly once: either when it returns (data stream) or, for a snapshot
// stream, immediately after it has acquired a snapSem slot, so the slow
// snapshot transfer is accounted against the dedicated snapshot budget rather
// than the data-stream budget.
func (t *QUICTransport) handleStream(inboundConn *quic.Conn, stream *quic.Stream, streamSem, snapSem chan struct{}) {
	// streamSemHeld tracks ownership of the data-stream slot so it is released
	// exactly once. It is set false once the slot is released (on snapshot
	// handoff) to prevent a double release in the deferred cleanup.
	streamSemHeld := true
	defer func() {
		if streamSemHeld {
			<-streamSem
		}
	}()
	defer func() {
		// stream.Close only closes the SEND direction in quic-go; the receive
		// direction would otherwise stay open after handleStream abandons the
		// stream. The peer's sender caches streams, so it would keep writing
		// into a stream nobody drains: up to MaxStreamReceiveWindow of messages
		// silently black-holed while sendBatch reports success, and the
		// abandoned stream permanently holds one of the peer's stream credits
		// (enough of these exhaust MaxIncomingStreams, after which ALL sends
		// from that peer silently fail). CancelRead sends STOP_SENDING so the
		// peer's next Write fails fast — its write-failure path evicts the
		// connection and re-dials — and the stream credit is released.
		//
		// This is safe on every exit path: the deliberately-kept idle stream
		// (errStreamIdle) never exits this function, and on the one exit whose
		// receive direction was fully consumed (snapshot completion) CancelRead
		// is a no-op.
		stream.CancelRead(QUICStreamErrReceiverGone)
		if closeErr := stream.Close(); closeErr != nil {
			slog.Debug("transport: stream close failed after handleStream",
				"remote", inboundConn.RemoteAddr().String(),
				"error", closeErr,
			)
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic recovered in handleStream",
				"remote", inboundConn.RemoteAddr().String(),
				"panic", fmt.Sprint(r),
				"stack", string(debug.Stack()),
			)
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

		// Read the next frame header. A clean idle gap on a LIVE connection is
		// NOT a failure: replication streams sit idle between bursts while the
		// connection is kept alive by heartbeats. Abandoning such a stream here
		// would leave the leader writing into a stream this receiver no longer
		// drains — the post-idle replication stall. readFrameHeaderResilient
		// returns errStreamIdle in that case so we keep the stream open.
		length, flags, err := t.readFrameHeaderResilient(inboundConn, stream, idleTimeout, frameReadActiveTimeout)
		if err != nil {
			if errors.Is(err, errStreamIdle) {
				continue
			}
			t.recordProtocolViolation("inbound frame header rejected", inboundConn, err)
			return
		}

		// A frame has begun arriving: complete its body (and any back-to-back
		// frame on a hot stream) under the active deadline so a half-delivered
		// frame cannot stall the receiver indefinitely.
		if derr := stream.SetReadDeadline(time.Now().Add(frameReadActiveTimeout)); derr != nil {
			return
		}

		if flags&FlagSnapshot != 0 {
			// Hand off from the data-stream budget to the dedicated snapshot
			// budget for the (potentially slow) transfer. The handoff releases
			// the streamSem slot first so it is not held for the transfer's
			// duration, then tries to acquire a snapSem slot.
			acquired := t.handoffToSnapSem(streamSem, snapSem, inboundConn)
			streamSemHeld = false
			if !acquired {
				// Snapshot budget exhausted or connection closing: streamSem is
				// already released by handoffToSnapSem, so drop the snapshot
				// rather than blocking acceptance of new streams.
				return
			}
			defer func() { <-snapSem }()
			t.handleSnapshotStream(inboundConn, stream, length)
			return
		}

		if err := t.handleMessageFrame(inboundConn, stream, length, flags); err != nil {
			// Receive-budget exhaustion is TRANSIENT and must not tear the
			// stream down: handleMessageFrame has already drained the frame
			// body, so the stream sits at a clean frame boundary. Returning
			// here would CancelRead+Close it, and the peer's cached stream
			// would then fail its next write, evicting the whole connection —
			// one peer filling the budget would cascade into cluster-wide
			// connection churn for every OTHER peer sharing it.
			if errors.Is(err, ErrMessageRecvMemoryBudgetExceeded) {
				continue
			}
			t.recordProtocolViolation("inbound message frame rejected", inboundConn, err)
			return
		}
	}
}

// protocolViolationLogInterval rate-limits the protocol-violation warning so a
// peer stuck in a teardown/re-dial loop (wrong DeploymentID, mismatched wire
// version after a partial upgrade) produces a usable diagnostic instead of a
// log flood.
const protocolViolationLogInterval = 10 * time.Second

// recordProtocolViolation counts and emits a rate-limited warning for a
// frame-level protocol violation that terminates an inbound stream. These
// errors (bad magic, unsupported wire version, oversized frame, DeploymentID
// mismatch, too many messages in a batch) are configuration or version faults,
// not transient I/O: without a diagnostic they present only as continuous
// silent stream teardown and re-dial churn. Ordinary end-of-stream and I/O
// errors are not violations and are skipped.
//
// The frame is discarded before its batch is parsed, so neither the shard nor
// the target replica is known; the counter is attributed to shard 0, target 0.
// The counter is incremented before the log rate limit so it reflects every
// violation rather than the sampled subset the warning shows.
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

// errStreamIdle signals that an inbound stream's read deadline elapsed at a
// clean frame boundary (zero bytes buffered) while its QUIC connection is still
// alive. The stream is idle, not dead, so handleStream keeps it open and waits
// for the next frame rather than abandoning it. Abandoning an idle-but-live
// replication stream is the root cause of the post-idle replication stall: the
// leader keeps writing into its cached stream (the connection stays alive via
// heartbeats), but the follower has stopped draining it, so proposals never
// commit and never self-heal.
var errStreamIdle = errors.New("transport: stream idle on live connection")

// isTimeoutErr reports whether err is an i/o deadline-exceeded (read/write
// timeout), including the timeout error quic-go surfaces from a stream whose
// read/write deadline elapsed.
func isTimeoutErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// readFrameHeaderResilient reads one frame header, distinguishing a benign idle
// gap from a real failure. The header is first awaited under idleTimeout: if
// the deadline fires before ANY byte of the next frame arrives and the
// connection is still alive, it returns errStreamIdle so the caller keeps the
// (idle) stream open. Once a frame has begun arriving, the remaining header
// bytes are read under activeTimeout, so a half-delivered frame cannot stall
// the receiver forever. Any other error (dead connection, malformed header,
// reset stream) is returned as-is for the caller to terminate the stream.
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

	if verr := ValidateFrameHeader(header[:]); verr != nil {
		return 0, FlagNone, verr
	}
	return binary.BigEndian.Uint32(header[4:8]), FrameFlags(header[3]), nil
}

// handoffToSnapSem transfers a stream's budget from the data-stream semaphore
// to the dedicated snapshot semaphore. It first releases the held streamSem
// slot (unconditionally, so the slow snapshot transfer is never charged against
// the data-stream budget) and then attempts a non-blocking acquire of snapSem.
//
// It returns true when a snapSem slot was acquired — the caller then owns that
// slot and must release it when the transfer completes. It returns false when
// the snapshot budget is exhausted or the connection/transport is shutting
// down; in that case no snapSem slot is held and the caller must drop the
// snapshot. Either way the streamSem slot is released exactly once, so across
// the handoff exactly one of streamSem/snapSem transitions and neither is
// leaked.
func (t *QUICTransport) handoffToSnapSem(streamSem, snapSem chan struct{}, inboundConn *quic.Conn) bool {
	<-streamSem
	// Check shutdown explicitly before trying the snapshot budget: in a
	// combined select with a default case, a closed stopC/connection context
	// only wins by random choice against an available snapSem slot, so a
	// snapshot could still be accepted during shutdown.
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
		slog.Warn("snapshot stream dropped: per-connection snapshot limit reached",
			"remote", inboundConn.RemoteAddr().String(),
			"limit", t.cfg.MaxConcurrentSnapshotRecv,
		)
		return false
	}
}

// handleMessageFrame reads a message frame payload, unmarshals the batch,
// validates the deployment ID, and dispatches to the handler. When the
// FlagCompressed flag is set, the payload is Snappy-decompressed before
// unmarshaling.
//
//nolint:gocyclo // frame handling covers compression, encryption, validation, and routing
func (t *QUICTransport) handleMessageFrame(inboundConn *quic.Conn, stream io.Reader, length uint32, flags FrameFlags) error {
	if length == 0 {
		return nil
	}

	// Reserve the frame's declared length against the aggregate message-receive
	// budget BEFORE allocating the buffer. The buffer is allocated before the
	// body is read (io.ReadFull below), so without an aggregate cap an
	// authenticated peer could open up to MaxStreamsPerConnection streams that
	// each declare MaxFrameSize and withhold the body, pinning that memory until
	// the read deadline and OOM-ing the host. The reservation is released once
	// the frame has been fully processed (defer), mirroring the snapshot
	// receiver's memory budget.
	//
	// Exhaustion is TRANSIENT, so the frame body is drained and the stream is
	// left at a clean boundary for the caller to continue on (see
	// handleStream): tearing the stream down would punish every peer sharing
	// the aggregate budget for one peer's burst.
	if !t.reserveMsgRecvBudget(int64(length)) {
		slog.Warn("message frame dropped: receive memory budget exhausted",
			"remote", remoteAddrOf(inboundConn),
			"length", length,
		)
		return t.dropFrame(stream, length, ErrMessageRecvMemoryBudgetExceeded)
	}
	defer t.releaseMsgRecvBudget(int64(length))

	// The per-connection sub-budget bounds what one peer can hold of the
	// aggregate, so a single connection cannot starve all the others.
	if !t.reserveConnMsgRecvBudget(inboundConn, int64(length)) {
		slog.Warn("message frame dropped: per-connection receive memory budget exhausted",
			"remote", remoteAddrOf(inboundConn),
			"length", length,
			"per_conn_limit", t.maxConnMsgRecv,
		)
		return t.dropFrame(stream, length, ErrMessageRecvMemoryBudgetExceeded)
	}
	defer t.releaseConnMsgRecvBudget(inboundConn, int64(length))

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

	// snappyBuf tracks a pooled Snappy decode buffer. When non-nil, it must
	// be returned to snappyDecPool after the decompressed payload has been
	// fully consumed (i.e., after the bulk Cmd copy detaches all zero-copy
	// aliases from the buffer).
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
		// The reservation above covers only the COMPRESSED frame length, but the
		// decode below allocates the declared DECOMPRESSED length. Snappy achieves
		// >1000:1 ratios, so without charging the difference an authenticated peer
		// could pin up to MaxDecompressedSize per in-flight frame while only the
		// tiny compressed length counts against MaxMessageReceiveMemory, defeating
		// the aggregate budget. Reserve the positive delta before decoding; the
		// deferred release keeps the accounting exactly balanced on every exit
		// path (success and error) for the remainder of this frame's processing.
		if delta := int64(declaredLen) - int64(length); delta > 0 {
			if !t.reserveMsgRecvBudget(delta) {
				t.putRecvBuf(buf)
				slog.Warn("compressed message frame dropped: receive memory budget exhausted",
					"remote", remoteAddrOf(inboundConn),
					"compressed_length", length,
					"declared_decompressed_length", declaredLen,
				)
				return ErrMessageRecvMemoryBudgetExceeded
			}
			defer t.releaseMsgRecvBudget(delta)
			if !t.reserveConnMsgRecvBudget(inboundConn, delta) {
				t.putRecvBuf(buf)
				slog.Warn("compressed message frame dropped: per-connection receive memory budget exhausted",
					"remote", remoteAddrOf(inboundConn),
					"compressed_length", length,
					"declared_decompressed_length", declaredLen,
					"per_conn_limit", t.maxConnMsgRecv,
				)
				return ErrMessageRecvMemoryBudgetExceeded
			}
			defer t.releaseConnMsgRecvBudget(inboundConn, delta)
		}
		snappyBuf = snappyDecPool.Get().(*[]byte)
		decompressed, err := snappy.Decode(*snappyBuf, payload)
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
	if _, err := batch.UnmarshalFrom(payload); err != nil {
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
			slog.Warn("dropping message with ShardID=0",
				"from", batch.Requests[i].From,
				"type", proto.MessageTypeName(batch.Requests[i].Type),
				"source", batch.SourceAddress,
			)
			batch.Reset()
			batchPool.Put(batch)
			if buf != nil {
				t.putRecvBuf(buf)
			}
			returnSnappyBuf()
			return nil
		}
	}

	// Validate deployment ID FIRST, before any connection-state mutation, so
	// a foreign-deployment (or replayed) batch cannot perturb our epoch
	// tracking / evict a live outbound. mTLS already authenticated the peer;
	// this adds deployment isolation as a precondition for trusting the batch.
	if batch.DeploymentID != t.cfg.DeploymentID {
		batch.Reset()
		batchPool.Put(batch)
		if buf != nil {
			t.putRecvBuf(buf)
		}
		returnSnappyBuf()
		return ErrPeerIdentityMismatch
	}

	// Bind the claimed SourceAddress to the inbound connection's authenticated
	// peer certificate. SourceAddress and SourceEpoch travel in-band in the
	// batch header, which mTLS does NOT bind to the sending peer's identity, so
	// a peer could otherwise inject an address its certificate does not cover —
	// poisoning the engine's (shardID,replicaID)->address registry and the
	// epoch-based outbound eviction. If the claimed address is not authorized by
	// the connection's certificate, clear it before the batch reaches the
	// handler so neither the registry nor eviction can learn it. The messages
	// themselves remain (separately) mTLS-authenticated and are still delivered.
	// The decision is cached per connection, so the costly cert check runs at
	// most once per (connection, claimed-address) rather than on every batch.
	if batch.SourceAddress != "" && !t.inboundAuthorizedForCached(inboundConn, batch.SourceAddress) {
		slog.Warn("clearing spoofed source address: not covered by peer certificate",
			"claimed_source", batch.SourceAddress,
			"remote", remoteAddrOf(inboundConn),
		)
		batch.SourceAddress = ""
	}

	// Detect stale outbound connections on the receive path. A change in the
	// sender's boot epoch (peer restart) or a dead cached outbound evicts the
	// stale outbound so response messages (ReplicateResp, HeartbeatResp) don't
	// hang on a black-holed connection until the write deadline expires. When
	// SourceAddress was cleared above (unauthorized), this early-returns without
	// touching epoch tracking or eviction.
	t.evictStaleOutboundOnRecv(batch.SourceAddress, batch.SourceEpoch, inboundConn)

	// Validate message count.
	if len(batch.Requests) > maxMessagesPerBatch {
		batch.Reset()
		batchPool.Put(batch)
		if buf != nil {
			t.putRecvBuf(buf)
		}
		returnSnappyBuf()
		return proto.ErrTooManyMessages
	}

	// Entry.Cmd slices AND Snapshot.Checksum alias the receive buffer via
	// zero-copy unmarshal. Copy them into a single bulk allocation before
	// returning the buffer to the pool: the buffer is recycled below while the
	// step worker still reads these fields asynchronously, so any alias left
	// behind would be read after the buffer was handed to another frame.
	// This reduces N allocations to 1 per frame, cutting GC pressure on the
	// receive hot path.
	totalCmdBytes := 0
	for i := range batch.Requests {
		for j := range batch.Requests[i].Entries {
			totalCmdBytes += len(batch.Requests[i].Entries[j].Cmd)
		}
		totalCmdBytes += len(batch.Requests[i].Snapshot.Checksum)
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
			if sum := batch.Requests[i].Snapshot.Checksum; len(sum) > 0 {
				copy(bulk[off:], sum)
				batch.Requests[i].Snapshot.Checksum = bulk[off : off+len(sum) : off+len(sum)]
				off += len(sum)
			}
		}
	}

	// Return the receive buffer and Snappy decode buffer to their pools
	// now that all zero-copy aliases have been replaced with owned copies.
	if buf != nil {
		t.putRecvBuf(buf)
	}
	returnSnappyBuf()

	// HandleMessage delivers each message to the engine's inbox queue.
	// Messages are copied by value (Go pass-by-value), but the copied
	// Message.Entries slice header still points to the batch's underlying
	// Entry array. The step worker reads these entries asynchronously
	// via Peer.Handle(), so the batch must not be recycled if any message
	// contains entries. For heartbeat-only batches (no entries), pooling
	// is safe since there are no shared references.
	hasEntries := false
	for i := range batch.Requests {
		if len(batch.Requests[i].Entries) > 0 {
			hasEntries = true
			break
		}
	}
	// Record message received metrics before delivery. The batch is about
	// to be consumed by the engine so we measure it here while we still
	// own the batch reference.
	if t.metrics != nil {
		for i := range batch.Requests {
			t.metrics.IncMessagesReceived(
				batch.Requests[i].ShardID,
				batch.Requests[i].From,
				batch.Requests[i].Type,
			)
		}
	}

	// Bind the in-band batch SourceAddress to the connection's verified
	// certificate (same hook the snapshot path uses), so the handler only
	// learns a return-path address the sender's cert actually covers.
	senderAuthorized := func(address string) bool {
		return t.inboundAuthorizedForCached(inboundConn, address)
	}
	handleErr := t.handler.HandleMessage(*batch, senderAuthorized)
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

// handleSnapshotStream processes a snapshot stream. The snapshot header
// was already received as the first frame's payload. Subsequent frames
// are snapshot chunks. inboundConn is the connection the stream arrived
// on; its verified peer certificate is the authority used to bind the
// in-band sender identity (chunk.From) before any snapshot state is
// persisted.
func (t *QUICTransport) handleSnapshotStream(inboundConn *quic.Conn, stream io.Reader, headerLen uint32) {
	t.processSnapshotHeader(inboundConn, stream, headerLen)
}

// maxSnapshotHeaderWireLen bounds the declared length of a snapshot header
// frame. No sender produces a header larger than SnapshotHeaderSize; the
// slack only exists for forward-compatible header growth. Without this bound
// the declared length (up to MaxFrameSize) is attacker-controlled and sits
// outside every receive budget, so it must never size an allocation or a
// long discard.
const maxSnapshotHeaderWireLen = 4096

// processSnapshotHeader reads and validates the snapshot header from the
// stream, then delegates to the snapshot receiver for chunk processing.
// Separated from handleSnapshotStream to accept io.Reader for testability.
func (t *QUICTransport) processSnapshotHeader(inboundConn *quic.Conn, r io.Reader, headerLen uint32) {
	// Read the snapshot header payload. Every drop below routes through
	// rejectSnapshotStream so the OnSnapshotRejected listener and a structured
	// warning fire uniformly: a silent return leaves the sender waiting out its
	// ack timeout with no local record of why the transfer never landed.
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

	// Reject snapshots delivered over a 0-RTT (early-data) connection.
	// 0-RTT data is replayable per RFC 9001 §8: an attacker who captured
	// a snapshot flight could replay it to re-inject stale state. The
	// snapshot path persists to disk and feeds InstallSnapshot into raft,
	// so unlike idempotent Raft RPCs it must not run on replayable early
	// data. (0-RTT is default-off; this is defense-in-depth when enabled.)
	if inboundConn != nil && inboundConn.ConnectionState().Used0RTT {
		t.snapshotRecv.rejectSnapshotStream(snapshotRejectPolicy, "received over 0-RTT (replayable early data)", &hdr, nil)
		return
	}

	// For actual QUIC streams, delegate to the snapshot receiver. The
	// authorization closure binds the snapshot's in-band sender identity
	// to the inbound connection's verified peer certificate — the same
	// mechanism used for MessageBatch.SourceAddress. A nil inboundConn
	// yields "unauthorized" for every address (fail closed).
	if qStream, ok := r.(*quic.Stream); ok {
		senderAuthorized := func(address string) bool {
			return t.inboundAuthorizedForCached(inboundConn, address)
		}
		t.snapshotRecv.receiveSnapshot(qStream, &hdr, remoteAddrOf(inboundConn), senderAuthorized, t.stopC)
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

// discardFrameBody consumes a frame payload that will not be processed so the
// stream is left at a clean frame boundary and can carry the next frame.
// length is bounded by MaxFrameSize (ValidateFrameHeader) and the body has not
// been read yet, so the discard copies through io.Discard's small internal
// buffer without allocating the declared size.
//
// It returns the bytes drained and a non-nil error when fewer than length
// bytes were consumed. The caller MUST NOT continue reading the stream in that
// case: the next read would start mid-frame and every subsequent header would
// be parsed from payload bytes.
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

// dropFrame drains the body of a frame that will not be processed and reports
// the error the caller must return. A complete drain leaves the stream at a
// clean frame boundary, so the caller's transient error (the receive-budget
// error, which handleStream retries on) applies. An incomplete drain leaves
// the stream desynchronised mid-frame, which is not recoverable by continuing:
// the non-retryable FrameDrainError is returned so the stream is torn down.
func (t *QUICTransport) dropFrame(r io.Reader, length uint32, transient error) error {
	drained, err := t.discardFrameBody(r, length)
	if err != nil {
		return &FrameDrainError{Declared: length, Drained: drained, Err: err}
	}
	return transient
}

// reserveMsgRecvBudget atomically reserves n bytes against the aggregate
// message-receive memory budget. It returns true on success; on failure it
// reserves nothing (restoring the budget) and returns false. Callers MUST pair
// a successful reservation with releaseMsgRecvBudget(n).
func (t *QUICTransport) reserveMsgRecvBudget(n int64) bool {
	if t.msgRecvBudget.Add(-n) < 0 {
		t.msgRecvBudget.Add(n)
		return false
	}
	return true
}

// releaseMsgRecvBudget returns n previously-reserved bytes to the aggregate
// message-receive memory budget.
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
