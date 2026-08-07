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
	"log/slog"
	"net"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/quic-go/quic-go"
)

// Connection management constants.
const (
	// connectionCleanupInterval is how often stale connections are checked.
	connectionCleanupInterval = 30 * time.Second

	// dialTimeout is the maximum time for a single dial attempt.
	dialTimeout = 5 * time.Second

	// supersededEpochRing is how many retired boot epochs are remembered per
	// peer. Each slot covers one restart whose late frames may still be in
	// flight; four spans a crash-loop without letting the record grow.
	supersededEpochRing = 4

	// peerIncarnationRetention is how long a peer's incarnation record
	// survives with neither a cached outbound nor an exchanged batch. It is
	// an order of magnitude above the QUIC MaxIdleTimeout, so an ordinary
	// outage never loses the epoch a re-dial needs to be stamped with, while
	// a replica removed from membership is forgotten promptly.
	peerIncarnationRetention = 10 * connectionCleanupInterval
)

// getConnection returns an existing QUIC connection to the target address or
// dials a new one. Concurrent dials to the same target are coalesced via
// singleflight to avoid redundant connections.
func (t *QUICTransport) getConnection(target string) (*quic.Conn, error) {
	if t.stopped.Load() {
		return nil, ErrStreamClosed
	}

	// Fast path: check existing connection.
	t.connMu.RLock()
	conn, ok := t.conns[target]
	t.connMu.RUnlock()
	if ok {
		// Verify the connection is still alive by checking its context.
		select {
		case <-conn.Context().Done():
			// Connection is dead, remove it and evict all associated
			// streams so callers don't get stale stream handles.
			slog.Debug("stale connection detected, reconnecting",
				"target", target,
			)
			t.removeConnection(target)
			t.evictStreamsForTarget(target)
		default:
			return conn, nil
		}
	}

	// Use singleflight to coalesce concurrent dials.
	slog.Debug("initiating connection dial",
		"target", target,
	)
	result, err, _ := t.dialGroup.Do(target, func() (any, error) {
		return t.dialConnection(target)
	})
	if err != nil {
		slog.Debug("connection dial failed",
			"target", target,
			"error", err,
		)
		return nil, err
	}
	return result.(*quic.Conn), nil
}

// dialConnection establishes a new QUIC connection to the target address.
func (t *QUICTransport) dialConnection(target string) (*quic.Conn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return nil, &DialError{Target: target, Err: err}
	}
	if host == "" {
		// An empty host would silently verify the peer cert against
		// "localhost", masking a misconfigured target and failing SAN
		// matching against real-hostname/IP certs. Fail loudly instead.
		return nil, &DialError{Target: target, Err: ErrEmptyDialHost}
	}

	// Capture the peer incarnation this dial is being made under BEFORE the
	// dial, so the stamp recorded below can never be newer than the
	// connection it describes. Stamping a NEWER epoch than the one the
	// connection actually reached would mark a stale outbound as current and
	// re-open the black hole; stamping an older one only costs one extra
	// eviction and re-dial, which is self-correcting.
	dialEpoch := t.knownPeerEpoch(target)

	ctx, cancel := context.WithTimeout(t.shutdownCtx, dialTimeout)
	defer cancel()

	clientTLS := t.tlsPair.client.Clone()
	clientTLS.ServerName = host

	conn, err := t.quicTr.Dial(ctx, udpAddr, clientTLS, t.quicCfg)
	if err != nil {
		slog.Info("peer connection failed", "target", target, "error", err)
		if t.eventListener != nil && t.eventListener.OnConnectionFailed != nil {
			t.eventListener.OnConnectionFailed(config.ConnectionInfo{Address: target, Err: err})
		}
		return nil, err
	}

	// Insert under connMu guarded by the stopped flag. Stop() sets stopped=true
	// before it drains t.conns, so a dial that checks stopped here either sees
	// true (and refuses, closing the fresh conn) or completed its insert before
	// Stop began — in which case Stop's drain will close it. This prevents a
	// connection leaking past shutdown when a dial races Stop().
	t.connMu.Lock()
	if t.stopped.Load() {
		t.connMu.Unlock()
		_ = conn.CloseWithError(QUICErrShutdown, "transport stopped during dial")
		return nil, ErrStreamClosed
	}
	// Re-check the cache under the lock: singleflight only coalesces
	// OVERLAPPING dials, so a dial started after another completed (fast-path
	// miss racing the winner's insert) reaches here with the cache already
	// populated. Overwriting would leak the cached connection — kept alive
	// forever by keep-alives, its pre-opened streams diverging from the
	// stream pool's bookkeeping. Keep the live winner and close the loser.
	if existing, ok := t.conns[target]; ok && existing != conn {
		select {
		case <-existing.Context().Done():
			// Cached connection is dead; replace it with the fresh dial.
		default:
			t.connMu.Unlock()
			_ = conn.CloseWithError(QUICErrDuplicateDial, "duplicate dial: reusing cached connection")
			slog.Debug("dial race lost, reusing cached connection", "target", target)
			return existing, nil
		}
	}
	t.conns[target] = conn
	t.connEpoch[target] = dialEpoch
	t.connMu.Unlock()

	// Pre-open the heartbeat stream (index 0) for this target. This
	// eliminates the OpenStreamSync latency (~20-40us) on the first
	// heartbeat send, which is on the critical path for ReadIndex and
	// liveness detection. The goroutine is tracked by the WaitGroup so
	// Stop() waits for it to finish before tearing down stream shard maps.
	t.wg.Add(1)
	go t.preOpenHeartbeatStream(target, conn)

	// Log connection establishment at Debug level.
	slog.Debug("connection established",
		"target", target,
		"local_addr", conn.LocalAddr().String(),
		"remote_addr", conn.RemoteAddr().String(),
	)

	slog.Info("peer connected", "target", target)
	if t.eventListener != nil && t.eventListener.OnConnectionEstablished != nil {
		t.eventListener.OnConnectionEstablished(config.ConnectionInfo{Address: target})
	}

	return conn, nil
}

// preOpenHeartbeatStream proactively opens a QUIC stream and stores it in the
// stream pool at index 0 (the heartbeat stream slot). This runs asynchronously
// after dialConnection so the dial path is not blocked. If the stream open
// fails (e.g., connection closed before completion), the failure is non-fatal:
// the heartbeat send path will lazily open the stream on its first use.
func (t *QUICTransport) preOpenHeartbeatStream(target string, conn *quic.Conn) {
	defer t.wg.Done()

	// Recover from panics so a fault in quic-go's OpenStreamSync or the stream
	// pool ops (e.g. on an unexpected connection state) cannot crash the whole
	// process — matching every other transport background goroutine.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("preOpenHeartbeatStream panic recovered",
				"target", target,
				"panic", fmt.Sprint(r),
				"stack", string(debug.Stack()),
			)
		}
	}()

	if t.stopped.Load() {
		return
	}

	ctx, cancel := context.WithTimeout(t.shutdownCtx, 5*time.Second)
	defer cancel()

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		// Non-fatal: stream will be opened lazily on first heartbeat send.
		return
	}

	// Store in the stream pool at heartbeat index 0.
	shardIdx := uint64(0) & (streamShardCount - 1)
	shard := &t.streamShards[shardIdx]
	key := streamKey{target: target, idx: 0}

	shard.mu.Lock()
	if _, ok := shard.streams[key]; ok {
		// Another goroutine (e.g., a concurrent heartbeat send) already
		// created the stream. Close ours and use the existing one.
		shard.mu.Unlock()
		if closeErr := s.Close(); closeErr != nil {
			slog.Debug("transport: stream close failed during preopen dedup",
				"target", target,
				"error", closeErr,
			)
		}
		return
	}
	shard.streams[key] = s
	shard.mu.Unlock()
}

// removeConnection removes a connection from the pool without closing it.
//
// The peerEpoch record is deliberately NOT dropped here. Removing a connection
// is the FIRST half of a re-dial, and the epoch is what the re-dial stamps the
// replacement with (see dialConnection). Forgetting it would stamp the fresh
// outbound with "unknown", the next batch from the peer would find a stamp
// mismatch and evict again, and the eviction would forget again -- an endless
// evict/re-dial cascade. A dead connection is no different: it is re-dialed
// too, so dropping the record there has exactly the same effect. The record is
// aged out by reapPeerIncarnations instead, which forgets only an address that
// has been both disconnected AND silent for peerIncarnationRetention.
func (t *QUICTransport) removeConnection(target string) {
	t.connMu.Lock()
	delete(t.conns, target)
	delete(t.connEpoch, target)
	t.connMu.Unlock()
}

// peerIncarnation is what this node remembers about one peer address's
// process incarnations.
//
// Boot epochs are UnixNano ^ crypto/rand (see newBootEpoch), so they carry NO
// usable ordering: "newer" cannot be decided by comparing two values. The only
// ordering available is the one this node WITNESSED, which is what superseded
// records. Without it, restart detection is last-writer-wins over an unordered
// value and a single late frame from a dead incarnation can drag the record
// backwards -- see observePeerEpoch.
type peerIncarnation struct {
	// epoch is the peer's current boot epoch, as last observed from a batch
	// whose claimed source address the sender's certificate covers.
	epoch uint64

	// superseded is a small ring of epochs that were once current and have
	// since been replaced. It is the hysteresis that makes a peer's
	// incarnation history one-way.
	superseded [supersededEpochRing]uint64
	next       int

	// lastSeen is when a batch bearing this peer's SourceAddress last
	// arrived, or when an outbound to it was last dialed. It exists solely
	// so reapPeerIncarnations can tell a peer that is gone from one that is
	// merely quiet.
	lastSeen time.Time
}

// isSuperseded reports whether epoch is an incarnation this node has already
// watched the peer replace.
func (p *peerIncarnation) isSuperseded(epoch uint64) bool {
	for _, e := range p.superseded {
		if e != 0 && e == epoch {
			return true
		}
	}
	return false
}

// retire records epoch as superseded, evicting the oldest ring slot.
func (p *peerIncarnation) retire(epoch uint64) {
	if epoch == 0 || p.isSuperseded(epoch) {
		return
	}
	p.superseded[p.next] = epoch
	p.next = (p.next + 1) % supersededEpochRing
}

// observePeerEpoch folds a batch's SourceEpoch into target's incarnation
// record and reports whether that epoch may drive an eviction decision.
//
// It returns false for an epoch this node has already watched the peer
// replace. That case is not hypothetical: a restart E1->E2 leaves the old
// inbound connection draining, so a frame written by the DEAD incarnation can
// still be delivered after the fresh outbound has been dialed under E2. Under
// plain last-writer-wins that late frame rewrote the record back to E1 and,
// because the fresh outbound is stamped E2, evicted a perfectly healthy
// connection and re-dialed it stamped E1 -- which the next genuine E2 batch
// evicted in turn. Each round costs a dial, a stream re-open and every
// in-flight replication on the connection, and it fires during a peer restart:
// precisely the event the mechanism exists to handle. Epochs cannot be
// compared, so the guard has to be a memory of what has already been retired.
//
// Authorization is unchanged: only a batch whose claimed target the inbound
// connection's verified certificate covers may create or advance a record.
// An unauthorized claim returns true so the caller's own (identical)
// certificate gate keeps deciding the eviction, exactly as before.
func (t *QUICTransport) observePeerEpoch(target string, srcEpoch uint64, inbound *quic.Conn) bool {
	now := time.Now()
	t.peerEpochMu.Lock()
	defer t.peerEpochMu.Unlock()

	rec, known := t.peerEpoch[target]
	if known {
		rec.lastSeen = now
		if rec.epoch == srcEpoch {
			return true
		}
		if rec.isSuperseded(srcEpoch) {
			return false
		}
	}
	if !t.inboundAuthorizedForCached(inbound, target) {
		return true
	}
	if !known {
		rec = &peerIncarnation{}
		t.peerEpoch[target] = rec
	}
	rec.retire(rec.epoch)
	rec.epoch = srcEpoch
	rec.lastSeen = now
	return true
}

// knownPeerEpoch returns the last boot epoch observed from target, or 0 if the
// peer has never been heard from. Dialing counts as contact for reaping
// purposes: an address this node is actively connecting to is not gone.
func (t *QUICTransport) knownPeerEpoch(target string) uint64 {
	t.peerEpochMu.Lock()
	defer t.peerEpochMu.Unlock()
	rec, ok := t.peerEpoch[target]
	if !ok {
		return 0
	}
	rec.lastSeen = time.Now()
	return rec.epoch
}

// reapPeerIncarnations drops the incarnation records of addresses this node
// neither holds an outbound to nor has exchanged a batch with for
// peerIncarnationRetention.
//
// The record must NOT be dropped on a teardown or eviction path: removing a
// connection is the first half of a re-dial and the epoch is what the re-dial
// stamps the replacement with, so forgetting there stamps the fresh outbound
// "unknown" and the next batch evicts it again -- an endless evict/re-dial
// cascade (see removeConnection). Reaping on an idle window instead only ever
// forgets an address that is neither connected nor talking, where the cost of
// being wrong is the ordinary first-contact eviction, which converges after one
// re-dial. Without any reaper the map retains every address ever heard from for
// the process lifetime, which membership churn grows without bound.
func (t *QUICTransport) reapPeerIncarnations(now time.Time) {
	t.connMu.RLock()
	live := make(map[string]struct{}, len(t.conns))
	for addr := range t.conns {
		live[addr] = struct{}{}
	}
	t.connMu.RUnlock()

	cutoff := now.Add(-peerIncarnationRetention)
	t.peerEpochMu.Lock()
	for addr, rec := range t.peerEpoch {
		if _, connected := live[addr]; connected {
			continue
		}
		if rec.lastSeen.After(cutoff) {
			continue
		}
		delete(t.peerEpoch, addr)
	}
	t.peerEpochMu.Unlock()
}

// removeAndCloseConnection atomically removes the connection for target from
// the pool and closes it with the given reason. If no connection exists for
// the target, this is a no-op. The close is performed outside the lock to
// avoid holding connMu during I/O.
func (t *QUICTransport) removeAndCloseConnection(target string, reason string) {
	t.connMu.Lock()
	conn, ok := t.conns[target]
	delete(t.conns, target)
	delete(t.connEpoch, target)
	t.connMu.Unlock()

	if ok && conn != nil {
		if closeErr := conn.CloseWithError(0, reason); closeErr != nil {
			slog.Debug("transport: connection close failed during removeAndClose",
				"target", target,
				"error", closeErr,
			)
		}
	}
}

// getStream returns a pooled QUIC stream for the given target. Heartbeats
// use stream index 0, data messages use (shardID % (poolSize-1)) + 1.
// The stream pool is sharded across streamShardCount buckets to reduce
// lock contention.
func (t *QUICTransport) getStream(target string, shardID uint64, isHeartbeat bool) (*quic.Stream, error) {
	idx := t.streamIndexFor(shardID, isHeartbeat)

	shardIdx := idx & (streamShardCount - 1)
	shard := &t.streamShards[shardIdx]
	key := streamKey{target: target, idx: idx}

	// Fast path: existing stream. Verify the underlying connection is
	// still alive to avoid sending to stale streams after a peer restart.
	// Without this check, a cached stream on a dead connection blocks
	// for the full write deadline (5s) before failing, causing the peer
	// to timeout and start unnecessary elections.
	shard.mu.RLock()
	stream, ok := shard.streams[key]
	shard.mu.RUnlock()
	if ok {
		t.connMu.RLock()
		conn, connOK := t.conns[target]
		t.connMu.RUnlock()
		if connOK {
			select {
			case <-conn.Context().Done():
				// Connection is dead. Evict all streams and reconnect.
				t.removeConnection(target)
				t.evictStreamsForTarget(target)
			default:
				// Re-verify the stream is still in the shard after
				// checking the connection. Between the initial stream
				// read and the connection read, another goroutine may
				// have evicted the connection and all its streams. If
				// the stream was removed, the local variable holds a
				// stale (closed) stream that would block for the full
				// write deadline before failing.
				shard.mu.RLock()
				_, stillCached := shard.streams[key]
				shard.mu.RUnlock()
				if stillCached {
					return stream, nil
				}
				// Stream was evicted between reads. Fall through to
				// the slow path to open a fresh stream.
			}
		}
		// No connection cached or connection is dead: fall through
		// to the slow path which will dial a fresh connection.
	}

	// Slow path: open a new stream.
	conn, err := t.getConnection(target)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(t.shutdownCtx, 5*time.Second)
	defer cancel()

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	shard.mu.Lock()
	// Double-check another goroutine didn't create it.
	if existing, ok := shard.streams[key]; ok {
		// Verify the connection backing the existing stream is still alive.
		// Without this check, a concurrent goroutine may have cached a stream
		// on a connection that has since died, and returning it would block
		// for the full write deadline before failing.
		t.connMu.RLock()
		existingConn, connOK := t.conns[target]
		t.connMu.RUnlock()
		stale := !connOK
		if connOK {
			select {
			case <-existingConn.Context().Done():
				stale = true
			default:
			}
		}
		if stale {
			// Existing stream is on a dead or missing connection. Discard
			// it, close the duplicate we just opened, and retry once with
			// a fresh connection and stream.
			delete(shard.streams, key)
			shard.mu.Unlock()
			if closeErr := existing.Close(); closeErr != nil {
				slog.Debug("transport: stale stream close failed during getStream dedup",
					"target", target,
					"shard_id", shardID,
					"error", closeErr,
				)
			}
			if closeErr := s.Close(); closeErr != nil {
				slog.Debug("transport: new stream close failed during getStream dedup",
					"target", target,
					"shard_id", shardID,
					"error", closeErr,
				)
			}
			t.removeConnection(target)
			t.evictStreamsForTarget(target)
			// Single retry: establish a fresh connection and stream.
			// If this also fails, return the error to the caller.
			retryConn, retryErr := t.getConnection(target)
			if retryErr != nil {
				return nil, retryErr
			}
			retryCtx, retryCancel := context.WithTimeout(t.shutdownCtx, 5*time.Second)
			defer retryCancel()
			retryStream, retryErr := retryConn.OpenStreamSync(retryCtx)
			if retryErr != nil {
				return nil, retryErr
			}
			// Double-check before inserting: between releasing the shard lock
			// above (for eviction) and re-acquiring it here, a concurrent
			// getStream/sendBatch/preOpenHeartbeatStream may have already cached
			// a live stream at this key. Overwriting it would leak that stream
			// (and its flow-control window), which is exactly the peer-restart
			// churn scenario this path handles.
			shard.mu.Lock()
			if existingRetry, ok := shard.streams[key]; ok {
				shard.mu.Unlock()
				if closeErr := retryStream.Close(); closeErr != nil {
					slog.Debug("transport: duplicate retry stream close failed",
						"target", target,
						"shard_id", shardID,
						"error", closeErr,
					)
				}
				return existingRetry, nil
			}
			shard.streams[key] = retryStream
			shard.mu.Unlock()
			return retryStream, nil
		}
		shard.mu.Unlock()
		if closeErr := s.Close(); closeErr != nil {
			slog.Debug("transport: stream close failed during getStream dedup",
				"target", target,
				"shard_id", shardID,
				"error", closeErr,
			)
		}
		return existing, nil
	}
	shard.streams[key] = s
	shard.mu.Unlock()

	return s, nil
}

// connectionCleanup periodically scans all pooled connections and removes
// those whose context is done (i.e., the connection has been closed by the
// peer or timed out). Restarts automatically on panic to prevent silent
// connection leak accumulation.
func (t *QUICTransport) connectionCleanup() {
	defer t.wg.Done()

	for {
		panicked := t.connectionCleanupOnce()
		if !panicked {
			return
		}
		// Check for shutdown before restarting.
		select {
		case <-t.stopC:
			return
		default:
		}
		timer := time.NewTimer(workerPanicRestartDelay)
		select {
		case <-t.stopC:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// connectionCleanupOnce runs one iteration of the connection cleanup loop
// and recovers from panics. Returns true if the loop panicked, false if
// it returned normally (via stop signal).
func (t *QUICTransport) connectionCleanupOnce() (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			stack := debug.Stack()
			panicErr := &WorkerPanicError{
				WorkerName: "connectionCleanup",
				Value:      r,
				Stack:      stack,
			}
			slog.Error("connectionCleanup panic recovered, restarting",
				"panic", fmt.Sprint(r),
				"stack", string(stack),
				"error", panicErr,
			)
		}
	}()

	ticker := time.NewTicker(connectionCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopC:
			return
		case <-ticker.C:
			t.cleanStaleConnections()
		}
	}
}

// cleanStaleConnections removes connections whose context is done and
// evicts all pooled streams that targeted the dead connection. Without
// stream eviction, subsequent getStream calls would return cached but
// broken streams, causing repeated write failures until the stream is
// individually evicted by sendBatch. It also reaps the incarnation records of
// peers that are neither connected nor talking.
func (t *QUICTransport) cleanStaleConnections() {
	var deadTargets []string

	t.connMu.Lock()
	for addr, conn := range t.conns {
		select {
		case <-conn.Context().Done():
			slog.Info("peer disconnected", "target", addr)
			if t.eventListener != nil && t.eventListener.OnConnectionFailed != nil {
				t.eventListener.OnConnectionFailed(config.ConnectionInfo{Address: addr})
			}
			delete(t.conns, addr)
			delete(t.connEpoch, addr)
			deadTargets = append(deadTargets, addr)
		default:
		}
	}
	t.connMu.Unlock()

	// Evict all pooled streams belonging to dead connections. This must happen
	// outside connMu to avoid lock ordering issues with shard locks.
	//
	// The dead targets' incarnation records are deliberately NOT dropped here:
	// a connection found dead is re-dialed, and the re-dial is stamped from
	// that record (see removeConnection). They age out through
	// reapPeerIncarnations instead, which only forgets an address that has
	// been both disconnected and silent for a long window.
	for _, target := range deadTargets {
		t.evictStreamsForTarget(target)
	}

	t.reapPeerIncarnations(time.Now())
}

// evictStreamsForTarget removes and closes all pooled streams whose
// streamKey targets the given address. Called when a connection dies
// so that subsequent getStream calls will open fresh streams on a
// new connection instead of returning stale, broken streams.
func (t *QUICTransport) evictStreamsForTarget(target string) {
	for i := range t.streamShards {
		shard := &t.streamShards[i]

		// Collect streams to close outside the lock.
		var toClose []*quic.Stream

		shard.mu.Lock()
		for key, stream := range shard.streams {
			if key.target == target {
				toClose = append(toClose, stream)
				delete(shard.streams, key)
			}
		}
		shard.mu.Unlock()

		for _, s := range toClose {
			if s != nil {
				if closeErr := s.Close(); closeErr != nil {
					slog.Debug("transport: stream close failed during eviction",
						"target", target,
						"error", closeErr,
					)
				}
			}
		}
	}
}

// evictStaleOutboundOnRecv checks, on the receive path, whether the cached
// outbound connection to the sender has become stale and evicts it if so.
//
// Two signals drive eviction:
//
//  1. A change in the sender's boot epoch (srcEpoch). The epoch is stamped
//     into every MessageBatch and changes ONLY when the peer process
//     restarts. A genuine restart means the peer has no knowledge of our
//     previously-dialed outbound connection — that outbound is black-holed
//     and would otherwise linger until MaxIdleTimeout (~30s), stalling
//     HeartbeatResp/ReplicateResp and triggering avoidable elections. On an
//     epoch change we evict the cached outbound immediately so the next send
//     redials a fresh connection.
//
//  2. A dead outbound (context cancelled), as before.
//
// Keying restart detection on the epoch — rather than on inbound *quic.Conn
// identity — is what makes this safe. In normal operation each peer dials
// independently (two *quic.Conn per pair) and the dial path occasionally
// re-dials (send-queue idle, MaxIdleTimeout). Those benign re-dials carry the
// SAME epoch, so they no longer trigger eviction. The earlier identity-based
// heuristic evicted on every re-dial, producing a self-sustaining cascade
// (A evicts → redials → B sees new inbound → B evicts → ...) that starved
// leader election in small clusters.
//
// Authenticated binding: srcEpoch and target both arrive in-band in the
// MessageBatch header, which mTLS does NOT bind to the sending peer's identity.
// Acting on them unconditionally would let any authenticated cluster member
// spoof another node's address+epoch and force repeated eviction of our live
// outbound to that victim (a liveness/DoS vector). Before recording or acting
// on an epoch we therefore require that the claimed target matches the inbound
// connection's verified peer certificate (inboundAuthorizedFor). This reuses
// the same PKI guarantee the dial path relies on: a node reachable at host:port
// must carry that host in its cert SANs (peers dial it with ServerName=host), so
// a legitimate sender's certificate always authorizes its own SourceAddress.
//
// The dead-outbound branch is intentionally NOT gated by this check: a spoofed
// batch cannot make our own outbound's context Done, so evicting an
// already-dead connection is always safe regardless of who prompted it.
//
// The fast path is a single map read + RLock + pointer compare, safe to call
// on every received batch. The certificate check runs only on the rare epoch
// change / first-contact paths, not on every batch.
func (t *QUICTransport) evictStaleOutboundOnRecv(target string, srcEpoch uint64, inboundConn *quic.Conn) {
	if target == "" {
		return
	}

	// Track the peer's boot epoch and detect a restart (epoch change). Only
	// record/act on the epoch once the claimed target is bound to the inbound
	// connection's authenticated certificate, so a spoofed SourceAddress cannot
	// poison peerEpoch or evict a live outbound. An epoch already witnessed
	// being superseded is a late frame from a dead incarnation: it neither
	// updates the record nor decides attribution below.
	attributable := true
	if srcEpoch != 0 {
		attributable = t.observePeerEpoch(target, srcEpoch, inboundConn)
	}

	t.connMu.RLock()
	outbound, ok := t.conns[target]
	stamp := t.connEpoch[target]
	t.connMu.RUnlock()
	if !ok {
		return
	}

	// Fast path: same connection object. No eviction needed.
	if outbound == inboundConn {
		return
	}

	select {
	case <-outbound.Context().Done():
		// Dead outbound — always evict.
		t.removeConnection(target)
		t.evictStreamsForTarget(target)
	default:
		// Alive outbound. Evict only when it cannot be attributed to the
		// incarnation this batch came from. In the steady state stamp ==
		// srcEpoch and this is a single integer compare, so the common
		// dual-dial case (two live connections per pair) is preserved and
		// the certificate check stays off the hot receive path.
		if srcEpoch == 0 || !attributable || stamp == srcEpoch {
			return
		}
		if !t.inboundAuthorizedForCached(inboundConn, target) {
			return
		}
		slog.Info("evicting outbound not attributable to the sending peer incarnation",
			"target", target, "peer_epoch", srcEpoch, "dialed_under_epoch", stamp)
		if closeErr := outbound.CloseWithError(QUICErrShutdown, "outbound predates peer incarnation"); closeErr != nil {
			slog.Debug("transport: connection close failed during stale eviction",
				"target", target, "error", closeErr)
		}
		t.removeConnection(target)
		t.evictStreamsForTarget(target)
	}
}

// inboundAuthorizedFor reports whether the peer that authenticated on conn is
// allowed to speak for target — i.e. whether target's host is present in the
// connection's verified peer certificate. It is the authorization gate for
// restart-eviction: the epoch/SourceAddress in a MessageBatch are in-band and
// untrusted, so they may only mutate connection state for an address the
// sender's certificate actually covers.
//
// This holds for any reachable node: a peer dials it with ServerName=host and
// TLS verification requires that host in the cert SANs, so the same keypair
// (server and client roles share one cert per node) authorizes the node's own
// advertised SourceAddress. A nil conn or one without a verified leaf
// (handshake incomplete) is treated as unauthorized and the hint is ignored —
// restart detection then degrades gracefully to the MaxIdleTimeout path.
func (t *QUICTransport) inboundAuthorizedFor(conn *quic.Conn, target string) bool {
	if conn == nil {
		return false
	}
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		// target may be a bare host with no port; fall back to it verbatim.
		host = target
	}
	if host == "" {
		return false
	}
	certs := conn.ConnectionState().TLS.PeerCertificates
	if len(certs) == 0 {
		return false
	}
	// certs[0] is the verified leaf. VerifyHostname matches host against the
	// cert's DNSNames/IPAddresses (and legacy CN), exactly as the dial path's
	// ServerName check does.
	return certs[0].VerifyHostname(host) == nil
}

// inboundAuthorizedForCached is the per-connection-cached form of
// inboundAuthorizedFor. Because a connection's peer certificate and a peer's
// advertised SourceAddress are stable for the connection's lifetime, the
// authorization decision is computed once per (conn, target) and reused for
// every subsequent batch on the same connection. This keeps the hot receive
// path (frequent heartbeats) off the expensive ConnectionState()+VerifyHostname
// call.
//
// The cache holds srcAddrAuthSlots addresses so a peer alternating claimed
// addresses cannot force a certificate check on every batch, and stops
// verifying entirely (failing closed) after srcAddrAuthMaxDistinct distinct
// claims from one connection. Entries are dropped by forgetInboundConnState
// when the inbound connection's handler returns.
func (t *QUICTransport) inboundAuthorizedForCached(conn *quic.Conn, target string) bool {
	if conn == nil {
		return false
	}

	v, _ := t.srcAddrAuth.LoadOrStore(conn, &srcAddrAuthEntry{})
	entry := v.(*srcAddrAuthEntry)

	entry.mu.Lock()
	defer entry.mu.Unlock()
	for i := range entry.slots {
		if entry.slots[i].valid && entry.slots[i].addr == target {
			return entry.slots[i].authorized
		}
	}
	if entry.distinct >= srcAddrAuthMaxDistinct {
		// A peer cycling claimed addresses is abusive; assume the negative
		// result rather than paying for another certificate check.
		return false
	}
	authorized := t.inboundAuthorizedFor(conn, target)
	entry.distinct++
	entry.slots[entry.next] = srcAddrAuthSlot{addr: target, authorized: authorized, valid: true}
	entry.next = (entry.next + 1) % srcAddrAuthSlots
	return authorized
}

// forgetInboundConnState removes the per-inbound-connection caches (the
// SourceAddress authorization decisions and the message-receive sub-budget).
// Called when the connection's handler returns — after every stream goroutine
// it spawned has finished — so neither map grows unbounded across connection
// churn. Waiting for the stream goroutines is what makes the delete final: an
// in-flight handleStream would otherwise re-insert an entry keyed by the dead
// connection with nothing left to remove it, pinning the whole *quic.Conn.
func (t *QUICTransport) forgetInboundConnState(conn *quic.Conn) {
	t.srcAddrAuth.Delete(conn)
	t.connMsgBudget.Delete(conn)
}

// reserveConnMsgRecvBudget reserves n bytes against the per-connection
// message-receive sub-budget in addition to the aggregate budget, so a single
// peer cannot occupy the whole of MaxMessageReceiveMemory and force every
// other peer's frames into the exhaustion path. A nil conn (test/unit paths)
// is not accounted. Successful reservations must be paired with
// releaseConnMsgRecvBudget.
func (t *QUICTransport) reserveConnMsgRecvBudget(conn *quic.Conn, n int64) bool {
	if conn == nil || t.maxConnMsgRecv <= 0 {
		return true
	}
	v, _ := t.connMsgBudget.LoadOrStore(conn, new(atomic.Int64))
	reserved := v.(*atomic.Int64)
	if reserved.Add(n) > t.maxConnMsgRecv {
		reserved.Add(-n)
		return false
	}
	return true
}

// releaseConnMsgRecvBudget returns n previously-reserved bytes to the
// per-connection message-receive sub-budget.
func (t *QUICTransport) releaseConnMsgRecvBudget(conn *quic.Conn, n int64) {
	if conn == nil || t.maxConnMsgRecv <= 0 {
		return
	}
	v, ok := t.connMsgBudget.Load(conn)
	if !ok {
		return
	}
	v.(*atomic.Int64).Add(-n)
}

// ConnectionCount returns the number of active connections in the pool.
// Used for testing and metrics.
func (t *QUICTransport) ConnectionCount() int {
	t.connMu.RLock()
	n := len(t.conns)
	t.connMu.RUnlock()
	return n
}

// StreamCount returns the total number of pooled streams across all shards.
// Used for testing and metrics.
func (t *QUICTransport) StreamCount() int {
	total := 0
	for i := range t.streamShards {
		shard := &t.streamShards[i]
		shard.mu.RLock()
		total += len(shard.streams)
		shard.mu.RUnlock()
	}
	return total
}

// isPeerFrameRejection reports whether err is a stream reset the PEER raised
// because a frame we wrote was unacceptable to its decoder
// (QUICStreamErrFrameRejected), as opposed to any other write failure.
//
// Only a live peer that has read and parsed one of our frames sends this code,
// so it proves the connection is healthy: the correct response is to drop the
// offending batch and replace the stream, never to close the connection. The
// Remote check keeps a locally-initiated cancellation from being mistaken for
// a peer verdict.
func isPeerFrameRejection(err error) bool {
	var streamErr *quic.StreamError
	return errors.As(err, &streamErr) &&
		streamErr.Remote &&
		streamErr.ErrorCode == QUICStreamErrFrameRejected
}

// streamIndexFor returns the stream-pool index a message with the given shard
// uses: heartbeats are pinned to index 0 so they are never queued behind a
// replication burst, data messages are spread over the remaining indices.
func (t *QUICTransport) streamIndexFor(shardID uint64, isHeartbeat bool) uint64 {
	if isHeartbeat {
		return 0
	}
	poolSize := uint64(t.cfg.StreamPoolSize)
	if poolSize <= 1 {
		return 0
	}
	return (shardID % (poolSize - 1)) + 1
}

// evictStream removes and closes exactly one pooled stream, leaving the
// connection and every other stream on it untouched. Used when a stream is
// individually unusable (the peer reset it) rather than when the connection is
// suspect — evictStreamsForTarget plus removeAndCloseConnection remains the
// response to a dead connection.
//
// The pooled entry is replaced only if it is still the same stream: a
// concurrent sender may already have opened a healthy replacement at this key,
// and evicting that would leak it along with its flow-control credit.
func (t *QUICTransport) evictStream(target string, shardID uint64, isHeartbeat bool, stream *quic.Stream) {
	if stream == nil {
		return
	}
	idx := t.streamIndexFor(shardID, isHeartbeat)
	shard := &t.streamShards[idx&(streamShardCount-1)]
	key := streamKey{target: target, idx: idx}

	shard.mu.Lock()
	if cached, ok := shard.streams[key]; ok && cached == stream {
		delete(shard.streams, key)
	}
	shard.mu.Unlock()

	if closeErr := stream.Close(); closeErr != nil {
		slog.Debug("transport: stream close failed during single-stream eviction",
			"target", target,
			"shard_id", shardID,
			"error", closeErr,
		)
	}
}

// isConnectionClosed returns true if the error indicates a closed connection.
func isConnectionClosed(err error) bool {
	if err == nil {
		return false
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		return true
	}
	var transportErr *quic.TransportError
	if errors.As(err, &transportErr) {
		return true
	}
	var idleErr *quic.IdleTimeoutError
	return errors.As(err, &idleErr)
}
