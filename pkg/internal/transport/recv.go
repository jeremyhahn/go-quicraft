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
//
// It runs under supervise.Supervise (startAcceptLoop), which counts it in
// t.wg and starts it again on a fresh goroutine if a runtime.Goexit ends it;
// sem and ipTracker are created once, by startAcceptLoop, so a restarted loop
// keeps the admission accounting of the connections already running. The
// loop releases t.acceptWG on its normal return only, and startAcceptLoop's
// stopped path releases it when a Goexit ends the loop during shutdown: it is
// released exactly once, and only once no admission can follow.
func (t *QUICTransport) acceptLoop(sem chan struct{}, ipTracker *ipConnTracker) {
	for !t.stopped.Load() {
		// On a recovered panic, back off before re-entering to avoid a tight
		// CPU loop and log flood, mirroring sendQueueWorker and
		// connectionCleanup. A normal iteration (no panic) re-enters
		// immediately so accept latency is unaffected.
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

// logRecovered emits the diagnostic for a panic, or a runtime.Goexit reported
// as supervise.ErrGoexit, that a transport goroutine recovered from. stack is
// captured by the caller on the recovering goroutine. The record is emitted
// through supervise.LogContained, after the caller's settlement: the slog
// handler and the panic value's String or Error method are user code, and a
// panic or Goexit in either must stay on the contained-diagnostics worker,
// never reach the recovery that called it.
func logRecovered(msg string, recovered any, stack []byte, attrs ...any) {
	supervise.LogContained(func() {
		slog.Error(msg, append(attrs, "panic", fmt.Sprint(recovered), "stack", string(stack))...)
	})
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

	// From here until a handler goroutine takes them over, this goroutine
	// owns the accepted connection and every admission slot it acquires. The
	// deferred settlement gives the slots back and closes the connection on
	// every exit that does not hand them over — a refusal, and an abnormal
	// exit alike — and only then emits the refusal's diagnostic, contained:
	// a log handler is user code and must not stand between a refusal and
	// its cleanup. An abnormal exit before any verdict closes the connection
	// as a refused admission (QUICErrConnLimitReached), which the peer
	// retries.
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

	// Refuse a peer whose certificate ALSO covers this node's host, before the
	// connection is allowed to occupy a global slot or open a single stream.
	//
	// This is the one exclusivity check an attacker cannot decline to run: the
	// two in Host (StartShard, requestConfigChange) inspect the LOCAL node's own
	// certificate against operator-supplied addresses, so they detect a
	// misconfigured deployment but never see a malicious joiner. See
	// refuseInboundPromiscuousCert for why "covers me" is the right question and
	// why it is answered once per connection rather than per message.
	//
	// The handshake has completed by the time Accept returns, so the peer
	// certificate is verified and present here, and this runs on RESUMED
	// sessions too — Accept yields every inbound connection regardless of
	// whether its TLS session was resumed.
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

	// Publish SYNCHRONOUSLY, on this goroutine, before the handler is
	// launched. Publishing inside handleConnection would leave every
	// admitted connection invisible for as long as its goroutine took to
	// be scheduled — and stop()'s inbound sweep walks exactly that map. A
	// connection admitted just before a shutdown could then be missed by
	// the sweep, have quicTr.Close() destroy it locally, and only then
	// reach publication, by which point announcing anything on it is a
	// no-op: the peer is left holding a dead connection, which is exactly
	// what this path exists to prevent.
	//
	// Publishing here is what lets stop() close that off for good, because
	// it makes "the accept loop has returned" mean "every connection it
	// admitted is in the map". See acceptWG.
	//
	// It is after every refusal deliberately: the refusal paths close the
	// connection without ever running a handler, and an entry published for
	// one of them would have nothing left to call forgetInboundConnState and
	// would pin the *quic.Conn for the process lifetime.
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

// handleConnection accepts streams on an inbound connection and dispatches
// them to either the message handler or snapshot handler based on the first
// frame's flags. A channel-based semaphore limits concurrent stream-processing
// goroutines to MaxStreamsPerConnection, preventing resource exhaustion from
// peers opening excessive streams.
//
// activity is HANDED IN by acceptLoopIteration rather than created here, and
// that is a guarantee, not a convenience: publication into inboundConns has to
// happen on the accept loop's own goroutine for stop()'s join of that loop to
// mean anything (see QUICTransport.acceptWG). Publishing here instead left every
// admitted connection invisible until this goroutine was scheduled, which is a
// window the shutdown sweep could fall into. The parameter is what stops that
// regressing quietly: this function has no way to publish.
func (t *QUICTransport) handleConnection(conn *quic.Conn, activity *inboundConnState) {
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

	// Register this connection against its PEER's receive budget. The budget
	// is shared by every connection one peer holds, so the entry is refcounted
	// and removed by forgetInboundConnState when the last one goes away.
	// Acquiring here — after the handshake, so the verified leaf is available
	// to msgBudgetKey — is what makes the cap per-peer rather than per-
	// connection, which MaxConnectionsPerIP would otherwise multiply straight
	// back out.
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
//
// activity is the connection's shared liveness record and may be nil on unit
// test paths that drive a stream without a tracked connection.
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
	// cancelCode is the STOP_SENDING code this stream is abandoned with. It is
	// raised to QUICStreamErrFrameRejected on the protocol-violation exits so
	// the peer can tell "your frame was unacceptable" (drop it, keep the
	// connection) from "this receiver is gone" (the connection is suspect).
	cancelCode := QUICStreamErrReceiverGone
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
			if isProtocolViolation(err) {
				cancelCode = QUICStreamErrFrameRejected
			}
			t.recordProtocolViolation("inbound frame header rejected", inboundConn, err)
			return
		}

		// The connection carried a frame, so it is not idle. Stamped on the
		// frame rather than on stream acceptance because a sender reuses one
		// cached stream across bursts: a busy connection can accept no new
		// stream for hours, and reaping it would be a self-inflicted outage.
		// errStreamIdle above deliberately does NOT reach here — a stream
		// waiting at a clean frame boundary produced no traffic.
		activity.touch(time.Now())

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
			acquired := t.handoffToSnapSem(streamSem, snapSem, inboundConn, &streamSemHeld)
			if !acquired {
				// Snapshot budget exhausted or connection closing: streamSem is
				// already released by handoffToSnapSem, so drop the snapshot
				// rather than blocking acceptance of new streams.
				return
			}
			// The release is deferred inside a closure rather than in the
			// loop body: the transfer is the last thing this stream does, so
			// the slot must come back when handleSnapshotStream returns —
			// including when it panics.
			func() {
				defer func() { <-snapSem }()
				t.handleSnapshotStream(inboundConn, stream, length)
			}()
			return
		}

		// 0-RTT early data is REPLAYABLE by definition (RFC 9001 §8): an
		// on-path attacker who captured a 0-RTT flight can re-inject it
		// arbitrarily many times. The SNAPSHOT branch above already refuses it
		// (processSnapshotHeader, with its own rejection accounting); a
		// MessageBatch is no more replay-safe just because raft happens to be
		// idempotent for most message types — that is a property of another
		// package which this one neither enforces nor can check.
		//
		// Checked here rather than at the top of handleStream so the snapshot
		// path keeps producing its own, more specific rejection. Used0RTT is a
		// connection property, so this is a cheap repeated read, not a new
		// handshake decision.
		//
		// 0-RTT is default-off, and this node never SENDS early data
		// (dialAndPool uses Dial, not DialEarly), so enabling it today buys
		// latency only for non-quicraft clients — all risk, no benefit.
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
			if isProtocolViolation(err) {
				cancelCode = QUICStreamErrFrameRejected
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

// LogSampled reports whether a peer-driven diagnostic may be emitted now,
// advancing the stamp when it may. It is exported for the host's message
// handler, whose per-message authorization warnings a peer can drive at the
// same rate.
//
// Every warning on the inbound receive path is triggerable at frame rate by a
// single authenticated peer. slog's default handler writes synchronously to
// stderr under one mutex, so an unsampled line there costs a write syscall on
// the raft receive path and serialises every other log call in the process
// behind it — a denial of service whose cost to the attacker is a few dozen
// bytes per line produced. Pair every call with an unconditional counter so
// the sampled line can report the true total.
func LogSampled(stamp *atomic.Int64, interval time.Duration) bool {
	now := time.Now().UnixNano()
	last := stamp.Load()
	if now-last < int64(interval) {
		return false
	}
	return stamp.CompareAndSwap(last, now)
}

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
//
// streamSemHeld is the caller's ownership flag for the streamSem slot, which
// its deferred cleanup releases while set. It is cleared together with the
// release, before anything else runs: the refusal below logs, and a log
// handler that panicked or called runtime.Goexit between the release and the
// caller clearing the flag made that cleanup release the slot a second time —
// stealing another stream's slot, or blocking forever on an empty semaphore.
// The refusal's diagnostic is contained for the same reason.
func (t *QUICTransport) handoffToSnapSem(streamSem, snapSem chan struct{}, inboundConn *quic.Conn, streamSemHeld *bool) bool {
	<-streamSem
	*streamSemHeld = false
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

	// Reserve the frame's decode charge (proto.DecodeCharge of its declared
	// length) against the per-peer and aggregate message-receive budgets BEFORE
	// allocating the buffer. The buffer is allocated before the body is read
	// (io.ReadFull below), so without an aggregate cap an authenticated peer
	// could open up to MaxStreamsPerConnection streams that each declare
	// MaxFrameSize and withhold the body, pinning that memory until the read
	// deadline and OOM-ing the host. The reservations are released once the
	// frame has been fully processed (defer), mirroring the snapshot
	// receiver's memory budget.
	//
	// The per-peer sub-budget is taken first and both are charged in the same
	// unit, so one peer can never hold more of the aggregate than its cap. A
	// frame refused by either holds NOTHING while its body drains: a refused
	// frame that kept an aggregate reservation through the drain would let one
	// peer that withholds its bodies hold the aggregate regardless of its cap.
	//
	// Exhaustion is TRANSIENT, so the frame body is drained and the stream is
	// left at a clean boundary for the caller to continue on (see
	// handleStream): tearing the stream down would punish every peer sharing
	// the aggregate budget for one peer's burst.
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
		// snappy.Decode sizes its destination by len(dst), not cap(dst): a
		// zero-length pooled buffer would force a fresh heap allocation of the
		// full decompressed size on every frame, defeating the pool. Hand it
		// the buffer's full capacity so an existing backing array is reused
		// whenever it is large enough (the send side does the same at Encode).
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

	// Detect stale outbound connections on the receive path. A change in the
	// sender's boot epoch (peer restart) or a dead cached outbound evicts the
	// stale outbound so response messages (ReplicateResp, HeartbeatResp) don't
	// hang on a black-holed connection until the write deadline expires. When
	// SourceAddress was cleared above (unauthorized), this early-returns without
	// touching epoch tracking or eviction.
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

	// Entry.Cmd slices alias the receive buffer via zero-copy unmarshal. Copy
	// them into a single bulk allocation before returning the buffer to the
	// pool: the buffer is recycled below while the step worker still reads these
	// fields asynchronously, so any alias left behind would be read after the
	// buffer was handed to another frame. This reduces N allocations to 1 per
	// frame, cutting GC pressure on the receive hot path.
	//
	// Cmd is now the ONLY such alias on a decoded Message. Snapshot.Checksum was
	// the other one, and it was detached here at the cost of an allocation and
	// two copies per frame for a field nothing ever computed or verified; it was
	// removed from the wire format in an earlier pre-release layout.
	// Snapshot.Filepath and every membership map are copied by their own
	// decoders (getString, getMap), so they own their storage already.
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
	// Bind the batch's in-band claims to the connection that carried them: the
	// peer's authenticated NODE IDENTITY (which msg.From is checked against
	// wherever committed membership records one) and the certificate's address
	// coverage (the fallback binding, and what gates learning a return-path
	// address). Both are read from the inbound connection, never from the batch.
	peer := PeerAuth{
		Identity: peerNodeIdentity(inboundConn),
		AuthorizesAddress: func(address string) bool {
			return t.inboundAuthorizedForCached(inboundConn, address)
		},
	}
	handleErr := t.handler.HandleMessage(*batch, peer)
	// Received-message metrics are recorded after the hand-off they
	// describe: the MetricsCollector is user code, and one that panicked or
	// called runtime.Goexit before HandleMessage lost the whole batch. The
	// handler copies what it keeps (see MessageHandler), so the batch's
	// Requests are still this frame's to read until the Reset below.
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

// snapshotFairnessKey returns the identity a peer's concurrent snapshot
// receives are accounted against.
//
// It must be something the peer CANNOT VARY, because it is the only thing
// standing between one host and the entire host-global snapshot budget. The
// previous key was conn.RemoteAddr().String(), which includes the client-chosen
// SOURCE PORT: one authenticated host simply opened MaxConnectionsPerIP (16 by
// default) connections, presented 16 distinct keys, and became entitled to
// 16 * maxPerPeer slots against a host-global budget of only
// MaxConcurrentSnapshotRecv (4). Holding all four while trickling bytes made
// every legitimate InstallSnapshot fail — and a follower that has fallen below
// the leader's compaction floor cannot be repaired by replication either, so
// the shard runs below its configured replica count indefinitely.
//
// The key is the SHA-256 of the peer leaf certificate's
// RawSubjectPublicKeyInfo — the peer's authenticated identity under the
// mandatory mTLS handshake. It is preferred over the peer IP because it is
// exactly as unforgeable (the handshake proves possession of the private key)
// while additionally surviving NAT, multi-homing and address changes: a peer
// that reconnects from a second interface is still one peer, and two replicas
// behind one NAT are still two peers. The SPKI rather than the whole
// certificate so a routine certificate RENEWAL on the same keypair does not
// silently reset the peer's accounting mid-transfer.
//
// The peer IP is the fallback for a connection with no verified leaf. Mandatory
// mTLS means that combination should not reach here, but if it ever does the
// fallback still keys on something the client cannot choose per connection —
// never on the source port.
//
// A nil connection (unit-test paths with no connection at all) yields the empty
// key, which acquirePeerSlot does not account.
func snapshotFairnessKey(conn *quic.Conn) string {
	if conn == nil {
		return ""
	}
	if identity := peerNodeIdentity(conn); identity != "" {
		return identity
	}
	return "ip:" + extractIP(conn.RemoteAddr())
}

// peerNodeIdentity returns the authenticated NODE IDENTITY of a connection's
// peer, or "" when the connection presents no verified leaf.
//
// It is the same value NodeIdentityFromCertificate derives, and it is what the
// receive-side sender gate compares against the identity committed membership
// assigns msg.From. Deriving it here — from the leaf the mTLS handshake
// verified, on the connection the message actually arrived on — is what makes
// it unassertable: possession of the matching private key was proved during the
// handshake, and nothing in the message body can influence it.
//
// The SPKI rather than the whole certificate, and rather than the serial or the
// fingerprint, so a routine certificate RENEWAL on the same keypair keeps the
// same identity. An identity that changed on every renewal would have to be
// re-committed through a config change on every renewal, which for a
// short-lived cluster PKI is a config change per node per rotation period — an
// operational burden that guarantees the strict mode gets turned off.
//
// It is deliberately NOT a SAN URI of the form quicraft://<deployment>/<id>,
// which would bind the replica ID directly and be a stronger statement. Four
// downstream products issue these certificates; requiring a new SAN would make
// this control undeployable until every one of them shipped a PKI change, and
// an undeployed control is worth nothing. The SPKI needs no issuance change at
// all — it is a property every certificate already has. What it costs is that
// the certificate does not SAY which replica it is, so the replica ID -> SPKI
// mapping has to be established out of band, through the operator-supplied
// identity on the config-change path. See Host.RequestAddNodeWithIdentity.
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

// NodeIdentityFromCertificate returns the stable node identity a leaf
// certificate carries: the SHA-256 of its RawSubjectPublicKeyInfo, prefixed
// "spki:".
//
// The prefix is a scheme tag, not decoration. It leaves room for a future
// identity form (a SAN URI, say) to coexist in the same committed field, and it
// makes a mis-supplied value — a bare fingerprint, a hostname — visibly not an
// identity rather than a value that silently never matches.
//
// A nil certificate yields "", which every caller treats as "no identity".
func NodeIdentityFromCertificate(leaf *x509.Certificate) string {
	if leaf == nil {
		return ""
	}
	sum := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	return "spki:" + hex.EncodeToString(sum[:])
}

// LocalNodeIdentity returns this node's OWN node identity, derived from the
// leaf certificate it presents to peers. It is what an operator collects from
// each node and hands to the config-change API so the shard can commit it.
//
// It returns "" when no leaf is available, which NewQUICTransport makes
// unreachable (an unparsable leaf fails construction).
func (t *QUICTransport) LocalNodeIdentity() string {
	if t.tlsPair == nil {
		return ""
	}
	return NodeIdentityFromCertificate(t.tlsPair.leaf)
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
