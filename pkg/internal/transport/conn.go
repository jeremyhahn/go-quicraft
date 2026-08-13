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
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"sort"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/netaddr"
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

	// evictionBudget is how many epoch-driven evictions ONE peer address may
	// force within evictionWindow. A genuine restart costs exactly one; the
	// remaining headroom covers a peer that restarts a few times in quick
	// succession. Beyond it the hint is ignored.
	//
	// Without a budget an authenticated peer could stamp a fresh SourceEpoch
	// on every batch and, past the supersededEpochRing hysteresis, force a
	// full QUIC handshake plus stream-pool teardown PER BATCH — arbitrary
	// attacker-driven work against a third-party victim address.
	evictionBudget = 4

	// evictionWindow is the period over which evictionBudget applies.
	evictionWindow = 30 * time.Second

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
	if existing, ok := shard.streams[key]; ok {
		existingOwner := shard.owners[key]
		// Keep the pooled entry — discarding ours — unless it belongs to a
		// connection that is already DEAD. An entry owned by the same conn is a
		// concurrent heartbeat open; an entry owned by an unknown conn is
		// test-injected; an entry owned by a different but still-live conn may
		// be a fresher one than ours (dial races leave one conn cached and the
		// loser draining), and clobbering it here could install our stream over
		// a live one out of order. Any entry that is merely superseded — owned
		// by a conn no longer cached for this target — is evicted lazily by
		// getStream's owner check, which reopens on the current conn. Only a
		// dead owner is unambiguously safe to replace right now.
		if existingOwner == nil || existingOwner == conn || existingOwner.Context().Err() == nil {
			shard.mu.Unlock()
			if closeErr := s.Close(); closeErr != nil {
				slog.Debug("transport: stream close failed during preopen dedup",
					"target", target,
					"error", closeErr,
				)
			}
			return
		}
		// Existing entry is on a dead connection: replace it with our fresh
		// stream. Close the stale stream outside the lock.
		shard.drop(key)
		shard.put(key, s, conn)
		shard.mu.Unlock()
		if existing != nil {
			if closeErr := existing.Close(); closeErr != nil {
				slog.Debug("transport: stale stream close failed during preopen replace",
					"target", target,
					"error", closeErr,
				)
			}
		}
		return
	}
	shard.put(key, s, conn)
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

	// evictions counts the epoch-driven evictions this peer address has
	// forced since evictWindowStart, and evictWindowStart is when the
	// current budget window opened. Together they bound how much work one
	// authenticated peer can make this node do; see allowEviction.
	evictions        int
	evictWindowStart time.Time
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
//
// The authorization gate runs OUTSIDE peerEpochMu. peerEpochMu is
// process-wide, while inboundAuthorizedForCached reads the connection's TLS
// state and, on a cache miss, runs an x509 VerifyHostname: holding the lock
// across it serialised every peer's first contact and every epoch change
// behind one certificate verification. Nothing in that decision reads the
// incarnation map, so it is computed between two critical sections and the
// record checks are REPEATED under the second one — a concurrent receive for
// the same target may have advanced or retired the record in the gap, and
// acting on the stale first read would re-open the flapping the superseded
// ring exists to prevent.
func (t *QUICTransport) observePeerEpoch(target string, srcEpoch uint64, inbound *quic.Conn) bool {
	now := time.Now()

	t.peerEpochMu.Lock()
	allow, decided := t.recordedEpochDecision(target, srcEpoch, now)
	t.peerEpochMu.Unlock()
	if decided {
		return allow
	}

	if !t.inboundAuthorizedForCached(inbound, target) {
		return true
	}

	t.peerEpochMu.Lock()
	defer t.peerEpochMu.Unlock()
	if allow, decided := t.recordedEpochDecision(target, srcEpoch, now); decided {
		return allow
	}
	rec, known := t.peerEpoch[target]
	if !known {
		rec = &peerIncarnation{}
		t.peerEpoch[target] = rec
	}
	rec.retire(rec.epoch)
	rec.epoch = srcEpoch
	rec.lastSeen = now
	return true
}

// recordedEpochDecision reports the verdict reachable from target's existing
// incarnation record alone, refreshing its lastSeen stamp. decided is false
// when the record cannot settle the question — the peer is unknown, or it
// claims an epoch that is neither the current one nor a retired one — and the
// caller must run the authorization gate before mutating anything.
//
// The caller must hold peerEpochMu.
func (t *QUICTransport) recordedEpochDecision(target string, srcEpoch uint64, now time.Time) (allow, decided bool) {
	rec, known := t.peerEpoch[target]
	if !known {
		return false, false
	}
	rec.lastSeen = now
	if rec.epoch == srcEpoch {
		return true, true
	}
	if rec.isSuperseded(srcEpoch) {
		return false, true
	}
	return false, false
}

// allowEviction charges one epoch-driven eviction against target's budget and
// reports whether it may proceed.
//
// The epoch mechanism exists to recover from a peer RESTART, which costs one
// eviction. Everything past evictionBudget in a window is either a peer in a
// crash loop — already unreachable, and recovered by the QUIC MaxIdleTimeout
// path exactly as it was before the mechanism existed — or a peer manufacturing
// epochs to make this node tear down and re-handshake a connection on demand.
// Refusing the hint costs at most one idle-timeout's delay; granting it without
// limit is unbounded attacker-driven work.
//
// The budget lives on the incarnation record, so reapPeerIncarnations retires it
// with the rest of the peer's state once the address goes quiet.
func (t *QUICTransport) allowEviction(target string, now time.Time) bool {
	t.peerEpochMu.Lock()
	defer t.peerEpochMu.Unlock()

	rec, known := t.peerEpoch[target]
	if !known {
		rec = &peerIncarnation{}
		t.peerEpoch[target] = rec
	}
	rec.lastSeen = now
	if now.Sub(rec.evictWindowStart) >= evictionWindow {
		rec.evictWindowStart = now
		rec.evictions = 0
	}
	if rec.evictions >= evictionBudget {
		return false
	}
	rec.evictions++
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
//
// It reports whether a pooled entry existed for target and was therefore
// removed. Callers that only tear a suspect connection down ignore the result;
// CloseTarget uses it to decide whether there was anything to close at all.
func (t *QUICTransport) removeAndCloseConnection(target string, reason string) bool {
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
	return ok
}

// CloseTarget tears down the outbound connection to address and every piece of
// address-keyed state that hangs off it: the pooled connection and its epoch
// stamp (conns/connEpoch), every pooled stream opened on it, the per-target
// send queue, and the peer's incarnation record. It reports whether a
// connection was actually cached and therefore closed.
//
// It exists because nothing else ever closes an outbound to a peer that is
// simply GONE. KeepAlivePeriod is below MaxIdleTimeout, so quic-go PINGs the
// connection indefinitely and it never idle-closes; cleanStaleConnections only
// reaps connections whose context is ALREADY dead, and a removed-but-still-
// running member answers keep-alives forever. Without an explicit teardown the
// connection, its streams, its epoch records and its keep-alive traffic persist
// for the process lifetime.
//
// This is EVENT-DRIVEN teardown for a COMMITTED membership removal, and it is
// deliberately not a sweep: a "not in the registry, so close it" scan would
// tear down live, healthy connections, because registry entries legitimately
// vanish (or have not appeared yet) under a healthy connection — a shard
// teardown that precedes its own restart, a rolled-back speculative
// pre-register, a joining replica with empty membership, bootstrap-snapshot
// authorization by address, and multi-minute in-flight snapshot streams.
//
// CALLER CONTRACT: one connection is keyed by ADDRESS and multiplexed across
// every shard that talks to that host (see conns and streamIndexFor, which maps
// distinct shards onto stream indices of the SAME connection). The caller MUST
// have established that no other shard still resolves a replica to this address
// — registry.AddressInUse is that check — before calling. Closing on behalf of
// one shard while another still uses the peer tears down healthy traffic.
//
// A snapshot transfer in flight vetoes the teardown and CloseTarget returns
// false without mutating anything: snapshot sends run over the same cached
// connection (SendSnapshot calls getConnection), and aborting a multi-minute
// stream to reclaim one connection is a bad trade. The check uses the same
// snapshotTracker that Stop consults, which counts transfers process-wide
// rather than per target, so the veto is conservative: any snapshot anywhere
// defers this teardown. The caller may retry; the veto is transient.
//
// bool rather than error is the right result type here: there is no failure to
// report. Either state was reclaimed or there was none to reclaim (unknown
// address) or a snapshot deferred it — none of which is an error the caller can
// or should act on beyond retrying later. Connection close errors are
// diagnostic only and are logged, exactly as on every other teardown path.
//
// CloseTarget is idempotent and safe on an unknown address: it mutates nothing
// and returns false.
func (t *QUICTransport) CloseTarget(address string) bool {
	if address == "" {
		return false
	}

	if inflight := t.snapshotTracker.InFlight(); inflight > 0 {
		slog.Info("deferring removed-peer connection teardown: snapshot transfer in flight",
			"target", address,
			"in_flight", inflight,
		)
		return false
	}

	// Nothing cached: leave every other structure untouched so a call for an
	// address this transport never dialed cannot disturb state that a live
	// shard may still be using.
	if !t.removeAndCloseConnection(address, "peer removed from membership") {
		return false
	}

	// Locks are taken in the established order and never nested: connMu is
	// released by removeAndCloseConnection before the stream shard locks, and
	// sendQueuesMu and peerEpochMu are independent of both.
	t.evictStreamsForTarget(address)
	t.retireSendQueue(address)
	t.forgetPeerIncarnation(address)

	slog.Info("closed connection to removed peer", "target", address)
	return true
}

// forgetPeerIncarnation drops target's incarnation record outright.
//
// Every other teardown path deliberately KEEPS the record, because removing a
// connection there is the first half of a re-dial and the record is what the
// replacement outbound is stamped with (see removeConnection). A committed
// membership removal is the one case where no re-dial is coming: the peer is
// gone, and holding its epoch, superseded ring and eviction budget only grows
// the map across membership churn. Should the address ever be re-added, the
// cost of having forgotten is the ordinary first-contact eviction, which
// converges after a single re-dial.
func (t *QUICTransport) forgetPeerIncarnation(target string) {
	t.peerEpochMu.Lock()
	delete(t.peerEpoch, target)
	t.peerEpochMu.Unlock()
}

// cachedStream is getStream's fast path: it returns the pooled stream for
// (target, shardID, isHeartbeat) and reports whether it is usable as-is.
//
// The connection behind a pooled stream must be verified alive before the
// stream is handed out. Without that check a cached stream on a dead
// connection blocks for the full write deadline (5s) before failing, which is
// long enough for the peer to time out and start an unnecessary election.
//
// When the cached entry is unusable this evicts exactly as much state as the
// failure warrants — see the three cases below — and reports false, leaving
// the caller to open a fresh stream.
func (t *QUICTransport) cachedStream(target string, shardID uint64, isHeartbeat bool) (*quic.Stream, bool) {
	idx := t.streamIndexFor(shardID, isHeartbeat)
	shard := &t.streamShards[idx&(streamShardCount-1)]
	key := streamKey{target: target, idx: idx}

	shard.mu.RLock()
	stream, ok := shard.streams[key]
	owner := shard.owners[key]
	shard.mu.RUnlock()
	if !ok {
		return nil, false
	}

	t.connMu.RLock()
	conn, connOK := t.conns[target]
	t.connMu.RUnlock()

	switch {
	case owner != nil:
		// Judge the pooled stream by its OWN connection, and require that
		// connection to be the one currently cached for the target.
		switch {
		case connOK && conn == owner && owner.Context().Err() == nil:
			// Live and current. Re-verify the stream is still the cached
			// entry after the conn read: a concurrent evict may have
			// removed or replaced it, in which case the local variable
			// holds a stale (closed) stream that would block for the full
			// write deadline before failing.
			shard.mu.RLock()
			cached, stillCached := shard.streams[key]
			shard.mu.RUnlock()
			if stillCached && cached == stream {
				return stream, true
			}
			// Evicted or replaced between reads: fall through.
		case connOK && conn == owner:
			// The stream's owner IS the cached conn, but it is dead: evict
			// the whole connection and its streams, then reconnect.
			t.removeConnection(target)
			t.evictStreamsForTarget(target)
		default:
			// The stream was opened on a connection that is no longer the
			// one cached for this target (a superseded conn from a peer
			// restart / re-dial) or no conn is cached at all. Drop just
			// this stale stream so the slow path opens a fresh one on the
			// current connection — WITHOUT tearing down the cached conn,
			// which may be a healthy replacement.
			t.evictStream(target, shardID, isHeartbeat, stream)
		}
	case connOK:
		// Owner unknown (only manually-constructed test transports inject a
		// raw stream). Preserve the original cached-conn liveness check.
		select {
		case <-conn.Context().Done():
			// Connection is dead. Evict all streams and reconnect.
			t.removeConnection(target)
			t.evictStreamsForTarget(target)
		default:
			shard.mu.RLock()
			_, stillCached := shard.streams[key]
			shard.mu.RUnlock()
			if stillCached {
				return stream, true
			}
			// Stream was evicted between reads. Fall through to
			// the slow path to open a fresh stream.
		}
	}
	return nil, false
}

// getStream returns a pooled QUIC stream for the given target. Heartbeats
// use stream index 0, data messages use (shardID % (poolSize-1)) + 1.
// The stream pool is sharded across streamShardCount buckets to reduce
// lock contention.
func (t *QUICTransport) getStream(target string, shardID uint64, isHeartbeat bool) (*quic.Stream, error) {
	if stream, ok := t.cachedStream(target, shardID, isHeartbeat); ok {
		return stream, nil
	}

	idx := t.streamIndexFor(shardID, isHeartbeat)
	shard := &t.streamShards[idx&(streamShardCount-1)]
	key := streamKey{target: target, idx: idx}

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
		// Judge the existing pooled stream by its OWN owning connection (when
		// known) rather than whatever conn is currently cached: an entry left
		// by a pre-open on a superseded conn must be treated as stale even
		// while a fresh conn is cached, and returning a stream on a dead conn
		// would block for the full write deadline before failing.
		existingOwner := shard.owners[key]
		t.connMu.RLock()
		cachedConn, connOK := t.conns[target]
		t.connMu.RUnlock()

		existingUsable := false
		if existingOwner != nil {
			existingUsable = connOK && cachedConn == existingOwner &&
				existingOwner.Context().Err() == nil
		} else if connOK {
			// Owner unknown (only test-injected raw streams reach this): fall
			// back to the cached conn's liveness, preserving prior behavior.
			existingUsable = cachedConn.Context().Err() == nil
		}
		if existingUsable {
			// Prefer the already-cached stream; close the duplicate we opened.
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

		// The existing entry is stale. If the connection WE just opened on is
		// the current, live cached conn, our fresh stream is good: replace the
		// stale entry in place WITHOUT tearing down the healthy connection.
		if connOK && cachedConn == conn && conn.Context().Err() == nil {
			shard.put(key, s, conn)
			shard.mu.Unlock()
			if closeErr := existing.Close(); closeErr != nil {
				slog.Debug("transport: stale stream close failed during getStream replace",
					"target", target,
					"shard_id", shardID,
					"error", closeErr,
				)
			}
			return s, nil
		}

		// Neither the existing entry nor our stream is on a healthy cached
		// connection (the conn died out from under this dial, or was replaced).
		// Discard both, tear the connection down, and retry once with a fresh
		// dial and stream. If the retry also fails, return the error.
		shard.drop(key)
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
		shard.put(key, retryStream, retryConn)
		shard.mu.Unlock()
		return retryStream, nil
	}
	shard.put(key, s, conn)
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
//
// Nothing but map surgery happens under connMu. connMu is the transport's
// hottest lock — every outbound message on every shard reads it through
// getStream/getConnection — and the OnConnectionFailed callback is USER code
// of unbounded duration. Firing it inside the write section stalled every
// heartbeat host-wide for as long as the listener ran, which is enough to time
// out elections on shards that have nothing to do with the dead peer, and a
// listener that re-entered transport teardown (Stop, CloseTarget) deadlocked
// outright, since sync.RWMutex is not reentrant. The dial path has always
// fired the same callback outside any lock (see dialConnection).
func (t *QUICTransport) cleanStaleConnections() {
	var deadTargets []string

	t.connMu.Lock()
	for addr, conn := range t.conns {
		select {
		case <-conn.Context().Done():
			delete(t.conns, addr)
			delete(t.connEpoch, addr)
			deadTargets = append(deadTargets, addr)
		default:
		}
	}
	t.connMu.Unlock()

	// Evict all pooled streams belonging to dead connections, and notify the
	// listener. This must happen outside connMu to avoid lock ordering issues
	// with shard locks and to keep user callbacks off the send path's lock.
	//
	// The dead targets' incarnation records are deliberately NOT dropped here:
	// a connection found dead is re-dialed, and the re-dial is stamped from
	// that record (see removeConnection). They age out through
	// reapPeerIncarnations instead, which only forgets an address that has
	// been both disconnected and silent for a long window.
	for _, target := range deadTargets {
		slog.Info("peer disconnected", "target", target)
		t.evictStreamsForTarget(target)
		if t.eventListener != nil && t.eventListener.OnConnectionFailed != nil {
			t.eventListener.OnConnectionFailed(config.ConnectionInfo{Address: target})
		}
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
				shard.drop(key)
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
		// Bound how often one peer may drive this. Authorization proves the
		// sender may SPEAK for this address; it does not bound how much work it
		// may ask for.
		if !t.allowEviction(target, time.Now()) {
			slog.Warn("epoch-driven eviction budget exhausted for peer; ignoring restart hint",
				"target", target, "peer_epoch", srcEpoch, "dialed_under_epoch", stamp,
				"budget", evictionBudget, "window", evictionWindow)
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
	host := certAuthHost(target)
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

// certAuthHost extracts the host component a certificate is matched against for
// a Raft address. It is shared by the authorization gate (inboundAuthorizedFor)
// and the startup exclusivity check (VerifyPeerAddressExclusivity) so the check
// characterises exactly the same matching the gate performs — including the fact
// that the PORT is not part of the decision.
func certAuthHost(target string) string {
	return netaddr.Host(target)
}

// canonicalCertHost normalises a host the way x509.VerifyHostname compares it,
// so two spellings of the SAME host are recognised as one. See
// netaddr.CanonicalHost for the rules and for why they live in a package below
// this one: raft's duplicate-address validation must answer "is this the same
// endpoint?" identically, and it cannot import the transport.
func canonicalCertHost(host string) string {
	return netaddr.CanonicalHost(host)
}

// isCoLocatedCertHost reports whether a peer host must be treated as sharing
// this node's machine, and therefore cannot be separated from it by any
// certificate policy.
//
// Two cases qualify:
//
//   - The peer names the SAME host as this node (after canonicalisation): a
//     co-located replica separated only by port, or this node's own entry in
//     the members map written in a different but equivalent form.
//   - The peer names a LOOPBACK address. A loopback address is only reachable
//     from the machine it is on, so a member listed at one is necessarily on
//     this machine — including the case where the members map lists this very
//     node by IP while its RaftAddress is a hostname, which made a node flag
//     ITSELF.
//
// Certificates are matched on host alone, so neither case can be given
// per-node SANs; flagging them would fail single-machine and mixed-form
// deployments while telling the operator nothing they could act on.
func isCoLocatedCertHost(selfHost, peerHost string) bool {
	if peerHost == "" || peerHost == selfHost {
		return true
	}
	if ip := net.ParseIP(peerHost); ip != nil && ip.IsLoopback() {
		return true
	}
	return false
}

// VerifyPeerAddressExclusivity reports whether this node's own leaf certificate
// ALSO covers any of the given peer addresses, which is a fatal deployment
// error.
//
// inboundAuthorizedFor answers "is the claimed address present in the sender's
// certificate", and three separate controls reduce to that answer: clearing a
// spoofed SourceAddress, deciding who may mutate peerEpoch and evict a live
// outbound, and binding a snapshot sender to its shard identity. Presence is
// only a meaningful authorization when SANs are EXCLUSIVE per node. Issue the
// cluster one wildcard certificate (*.raft.example.com) or one certificate
// carrying every node's SAN — both routine, both perfectly valid against the CA
// — and every one of those checks succeeds for EVERY peer address. A single
// compromised replica can then claim any peer's address with a fresh
// SourceEpoch per batch and drive every node to evict its healthy outbound to a
// third-party victim: an attacker-controlled cluster-wide replication stall,
// precisely the cascade the epoch mechanism exists to prevent.
//
// Because a certificate is only ever matched on its HOST, peers that share
// this node's machine are indistinguishable from it by construction and are
// skipped — see isCoLocatedCertHost for the two forms that takes (the same
// host under any spelling, and any loopback address). Flagging them would fail
// every single-machine deployment, and would fail a multi-machine one whose
// members map simply lists this node by IP while its RaftAddress is a
// hostname, while telling the operator nothing they could act on. Per-node
// SANs on DISTINCT hosts are therefore a hard deployment precondition for the
// guarantees above; co-located replicas do not get them.
func (t *QUICTransport) VerifyPeerAddressExclusivity(selfAddr string, peers []string) error {
	if t.tlsPair == nil || t.tlsPair.leaf == nil {
		// Unreachable: NewQUICTransport fails when the leaf cannot be parsed.
		// Fail CLOSED rather than silently skipping a security precondition.
		return ErrNoLocalCertificate
	}
	return verifyCertificateExclusivity(t.tlsPair.leaf, selfAddr, peers)
}

// verifyCertificateExclusivity is VerifyPeerAddressExclusivity over an explicit
// leaf, so the rule is testable without a live transport.
func verifyCertificateExclusivity(leaf *x509.Certificate, selfAddr string, peers []string) error {
	selfHost := canonicalCertHost(certAuthHost(selfAddr))
	seen := make(map[string]struct{}, len(peers))
	var covered []string
	for _, peer := range peers {
		host := canonicalCertHost(certAuthHost(peer))
		if isCoLocatedCertHost(selfHost, host) {
			continue
		}
		if _, dup := seen[host]; dup {
			continue
		}
		seen[host] = struct{}{}
		if leaf.VerifyHostname(host) == nil {
			covered = append(covered, host)
		}
	}
	if len(covered) == 0 {
		return nil
	}
	sort.Strings(covered)
	return &SharedPeerCertificateError{SelfAddress: selfAddr, CoveredPeers: covered}
}

// refuseInboundPromiscuousCert reports the reason to refuse an inbound
// connection whose verified leaf ALSO authorizes this node's own host, and nil
// when the peer may be admitted.
//
// This is the CONNECTION-LEVEL half of the promiscuous-certificate control, and
// it is the only half that is an attack control. Host.StartShard and
// Host.requestConfigChange also run an exclusivity check
// (VerifyPeerAddressExclusivity), but those inspect the LOCAL node's OWN
// certificate against the addresses an operator handed the local API: they
// catch a misconfigured deployment before it starts, and a malicious joiner
// never executes them at all — it simply does not call them. Only a check on
// what arrives can refuse a peer that did not cooperate.
//
// The rule, and why it is the right one: every in-band authorization the
// transport and the message handler perform reduces to "does the sender's
// certificate cover this address". A leaf that covers this node's host covers
// every OTHER member's host too under the certificate shapes that produce it —
// a wildcard SAN (*.raft.example.com) or one shared certificate carrying every
// node's SAN. Its holder can therefore assert any member's msg.From and have
// forged ReplicateResp and RequestVoteResp counted toward quorum. This node
// cannot enumerate the other members here (the transport has no shard
// membership), but it does not need to: it is itself a member, so "covers me"
// is a sound proxy for "covers everyone", and it is a fact this node can check
// against nothing but its own configured address.
//
// It is done ONCE PER CONNECTION, deliberately. The per-message alternative —
// probing each member address through senderAuthorized — thrashes the
// 4-slot-per-connection decision cache and trips its 16-distinct-miss
// fail-closed cap (see inboundAuthorizedForCached), which would permanently
// kill every connection on any shard with five or more members. A certificate
// is fixed for the life of a connection, so once is also sufficient.
//
// Co-located peers are exempt, using the same predicate the startup check uses.
// A certificate is matched on HOST alone, so a peer on this machine cannot be
// given a certificate that excludes this node's host — refusing it would break
// every single-machine deployment while telling the operator nothing they could
// act on. peerHost comes from the QUIC remote address, which is the one thing
// about the peer that is not in-band: a sender cannot choose the source address
// its packets arrive from and still complete a handshake.
func (t *QUICTransport) refuseInboundPromiscuousCert(conn *quic.Conn) error {
	if conn == nil || t.tlsPair == nil {
		return nil
	}
	selfHost := canonicalCertHost(certAuthHost(t.localAddr()))
	if selfHost == "" {
		return nil
	}
	certs := conn.ConnectionState().TLS.PeerCertificates
	if len(certs) == 0 {
		// Mandatory mTLS makes this unreachable; admitting it here would be a
		// latent fail-open, so refuse.
		return ErrNoPeerCertificate
	}
	return peerCertificateExclusivity(certs[0], t.localAddr(), conn.RemoteAddr().String())
}

// peerCertificateExclusivity is refuseInboundPromiscuousCert over explicit
// values, so the rule is testable without a live connection — which matters
// more here than usual, because the co-location exemption makes the rule a
// no-op for every loopback deployment and therefore for every test that could
// stand up two real transports on one machine.
//
// selfAddr is this node's advertised Raft address; peerAddr is the transport
// address the connection arrived from, which is the one thing about the peer
// that is not in-band.
func peerCertificateExclusivity(peerLeaf *x509.Certificate, selfAddr, peerAddr string) error {
	selfHost := canonicalCertHost(certAuthHost(selfAddr))
	if selfHost == "" || peerLeaf == nil {
		return nil
	}
	if isCoLocatedCertHost(selfHost, canonicalCertHost(certAuthHost(peerAddr))) {
		return nil
	}
	// VerifyHostname returning nil is the REFUSAL case: a nil error means the
	// peer's certificate covers this node's host, which is the promiscuous
	// shape. A non-nil error is the required per-node scoping and admits the
	// connection, so the predicate is spelled as a boolean rather than passing
	// an error through.
	coversSelf := peerLeaf.VerifyHostname(selfHost) == nil
	if !coversSelf {
		return nil
	}
	return &PromiscuousPeerCertificateError{PeerAddress: peerAddr, SelfHost: selfHost}
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
		shard.drop(key)
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
