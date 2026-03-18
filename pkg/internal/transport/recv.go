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
// It uses a mutex-protected map with atomic counts for efficient concurrent
// access from the accept loop and connection handler goroutines.
type ipConnTracker struct {
	mu     sync.Mutex
	counts map[string]*atomic.Int32
	limit  int
}

// newIPConnTracker creates a tracker with the given per-IP connection limit.
func newIPConnTracker(limit int) *ipConnTracker {
	return &ipConnTracker{
		counts: make(map[string]*atomic.Int32),
		limit:  limit,
	}
}

// tryAcquire increments the count for the given IP. Returns true if the
// count is within the limit, false if the limit would be exceeded. When
// false is returned, the count is not incremented.
func (tr *ipConnTracker) tryAcquire(ip string) bool {
	tr.mu.Lock()
	counter, ok := tr.counts[ip]
	if !ok {
		counter = &atomic.Int32{}
		tr.counts[ip] = counter
	}
	tr.mu.Unlock()

	for {
		cur := counter.Load()
		if int(cur) >= tr.limit {
			return false
		}
		if counter.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// release decrements the count for the given IP. The caller must have
// previously called tryAcquire successfully for the same IP. When the
// count reaches zero, the entry is removed from the map to prevent
// unbounded growth from transient connections.
func (tr *ipConnTracker) release(ip string) {
	tr.mu.Lock()
	counter, ok := tr.counts[ip]
	if !ok {
		tr.mu.Unlock()
		return
	}
	newVal := counter.Add(-1)
	if newVal <= 0 {
		delete(tr.counts, ip)
	}
	tr.mu.Unlock()
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
		t.acceptLoopIteration(sem, ipTracker)
	}
}

// acceptLoopIteration executes a single iteration of the accept loop.
// It is separated from acceptLoop to provide a deferred panic recovery
// boundary. If a panic occurs during connection acceptance or dispatch,
// the deferred recover logs the panic with a full stack trace and
// returns control to the outer for-loop, which checks the stopped flag
// and re-enters the next iteration. This ensures the accept loop is
// never permanently killed by a panic.
func (t *QUICTransport) acceptLoopIteration(sem chan struct{}, ipTracker *ipConnTracker) {
	defer func() {
		if r := recover(); r != nil {
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
}

// handleConnection accepts streams on an inbound connection and dispatches
// them to either the message handler or snapshot handler based on the first
// frame's flags. A channel-based semaphore limits concurrent stream-processing
// goroutines to MaxStreamsPerConnection, preventing resource exhaustion from
// peers opening excessive streams.
func (t *QUICTransport) handleConnection(conn *quic.Conn) {
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
	streamSem := make(chan struct{}, t.cfg.MaxStreamsPerConnection)

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

		// Acquire stream semaphore to limit concurrent goroutines per connection.
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
		go func() {
			defer t.wg.Done()
			defer func() { <-streamSem }()
			t.handleStream(conn, stream)
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
func (t *QUICTransport) handleStream(inboundConn *quic.Conn, stream *quic.Stream) {
	defer func() {
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

	for {
		if t.stopped.Load() {
			return
		}

		// Set a read deadline to avoid hanging on dead streams.
		if err := stream.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return
		}

		length, flags, err := ReadFrameHeader(stream)
		if err != nil {
			return
		}

		if flags&FlagSnapshot != 0 {
			t.handleSnapshotStream(stream, length)
			return
		}

		if err := t.handleMessageFrame(inboundConn, stream, length, flags); err != nil {
			return
		}
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
		snappyBuf = snappyDecPool.Get().(*[]byte)
		decompressed, err := snappy.Decode(*snappyBuf, payload)
		if err != nil {
			snappyDecPool.Put(snappyBuf)
			snappyBuf = nil
			t.putRecvBuf(buf)
			return &FrameDecompressError{Err: err}
		}
		if len(decompressed) != declaredLen {
			*snappyBuf = decompressed[:0]
			snappyDecPool.Put(snappyBuf)
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
	returnSnappyBuf := func() {
		if snappyBuf != nil {
			*snappyBuf = (*snappyBuf)[:0]
			snappyDecPool.Put(snappyBuf)
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

	// Detect stale outbound connections on the receive path. If our
	// cached outbound to the sender is dead (context cancelled) OR is a
	// different connection than the one delivering this message (peer
	// restarted and dialed us fresh), forcibly evict the stale outbound
	// so response messages (ReplicateResp, HeartbeatResp) don't hang on
	// a dead connection waiting for the write deadline to expire.
	t.evictStaleOutboundOnRecv(batch.SourceAddress, inboundConn)

	// Validate deployment ID.
	if batch.DeploymentID != t.cfg.DeploymentID {
		batch.Reset()
		batchPool.Put(batch)
		if buf != nil {
			t.putRecvBuf(buf)
		}
		returnSnappyBuf()
		return ErrPeerIdentityMismatch
	}

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

	// Entry.Cmd slices alias the receive buffer via zero-copy unmarshal.
	// Copy them into a single bulk allocation before returning the buffer
	// to the pool. This reduces N allocations to 1 per frame, cutting
	// GC pressure on the receive hot path.
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

	handleErr := t.handler.HandleMessage(*batch)
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
// are snapshot chunks.
func (t *QUICTransport) handleSnapshotStream(stream io.Reader, headerLen uint32) {
	t.processSnapshotHeader(stream, headerLen)
}

// processSnapshotHeader reads and validates the snapshot header from the
// stream, then delegates to the snapshot receiver for chunk processing.
// Separated from handleSnapshotStream to accept io.Reader for testability.
func (t *QUICTransport) processSnapshotHeader(r io.Reader, headerLen uint32) {
	// Read the snapshot header payload.
	if headerLen < SnapshotHeaderSize {
		return
	}

	var hdrBuf [SnapshotHeaderSize]byte
	if _, err := io.ReadFull(r, hdrBuf[:]); err != nil {
		return
	}
	// Discard any remaining header bytes.
	if headerLen > SnapshotHeaderSize {
		discard := make([]byte, headerLen-SnapshotHeaderSize)
		if _, err := io.ReadFull(r, discard); err != nil {
			return
		}
	}

	var hdr SnapshotHeader
	if _, err := hdr.UnmarshalFrom(hdrBuf[:]); err != nil {
		return
	}

	if hdr.ChunkCount > defaultMaxSnapshotChunkCount {
		return
	}

	// For actual QUIC streams, delegate to the snapshot receiver.
	// For testing with io.Reader, this path exercises header validation.
	if qStream, ok := r.(*quic.Stream); ok {
		t.snapshotRecv.receiveSnapshot(qStream, &hdr, t.stopC)
	} else {
		slog.Warn("snapshot header received on non-QUIC stream, snapshot dropped",
			"reader_type", fmt.Sprintf("%T", r))
	}
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
