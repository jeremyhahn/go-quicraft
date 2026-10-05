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
	"log/slog"
	"net"
	"runtime/debug"
	"slices"
	"sort"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/netaddr"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

// Connection management constants.
const (
	// connectionCleanupInterval is how often stale connections are checked.
	connectionCleanupInterval = 30 * time.Second

	// dialTimeout is the default maximum time for a single dial attempt;
	// Config.DialTimeout overrides it (test fixtures do, production does not).
	dialTimeout = 5 * time.Second

	// streamOpenTimeout is the maximum time for a single OpenStreamSync on
	// an established connection: the heartbeat pre-open, the pooled-stream
	// slow path, and that path's one retry after a stale-stream eviction.
	streamOpenTimeout = 5 * time.Second

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

	// inboundIdleRetention is how long an INBOUND connection may carry no
	// frame at all before it is reclaimed.
	//
	// Nothing else ever reclaims one. This node's own quic.Config sets
	// KeepAlivePeriod (10s) below MaxIdleTimeout (30s) for BOTH roles, so the
	// listener PINGs every accepted connection indefinitely and QUIC's idle
	// timeout can never fire on it; cleanStaleConnections reaps only
	// connections whose context is ALREADY dead, and CloseTarget reaches the
	// outbound half. A peer that completes a handshake and then says nothing
	// therefore holds one of MaxIncomingConnections (256) and one of
	// MaxConnectionsPerIP (16) for the process lifetime, for the cost of
	// answering a PING every 10 seconds.
	//
	// It is set to the same order as peerIncarnationRetention and an order of
	// magnitude above MaxIdleTimeout: any live Raft peering is far busier than
	// this (a leader heartbeats every tick, and a follower answers), so only a
	// connection with no shard in common with this node — or a peer holding one
	// open for its own reasons — can reach the bound. Reclaiming is cheap to
	// get wrong in only one direction: the peer's next write fails, its
	// write-failure path evicts the connection and re-dials.
	inboundIdleRetention = 10 * connectionCleanupInterval
)

// getConnection returns the pooled connection to target, dialing one when
// there is none. Concurrent dials are coalesced via singleflight.
//
// want is the node identity the peer must present — the identity membership
// binds to the replica the caller is sending to (see wantFor) — or "" when
// identity pinning is disabled. A pooled connection is handed out only when its
// peer presented want, and a dial is kept only when the peer presents exactly
// want, so every write is pinned to its destination at the point it takes the
// connection, whatever re-dial or retry the send path goes through. When the
// pooled peer presented another identity, replaceUnboundPeer decides between
// refusing the send and replacing the connection.
func (t *QUICTransport) getConnection(target, want string) (*quic.Conn, error) {
	if t.stopped.Load() {
		return nil, ErrStreamClosed
	}
	if want == "" && t.cfg.ReplicaIdentity != nil {
		// Pinning is on and the caller named no destination: fail closed.
		return nil, &DialError{Target: target, Err: &DialIdentityError{}}
	}

	// Fast path: check existing connection.
	t.connMu.RLock()
	conn, ok := t.conns[target]
	presented := t.connPeer[conn]
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
			if t.serves(presented, want) {
				return conn, nil
			}
		}
	}

	dialKey := target
	if want != "" {
		dialKey = target + "\x00" + want
	}
	refusalKey := dialRefusalKey{target: target, want: want}
	// A destination just refused is not tried again until the cooldown
	// passes: see dialRefusalCooldown.
	if v, found := t.dialRefusals.Load(refusalKey); found {
		refusal := v.(dialRefusal)
		if time.Since(refusal.at) < dialRefusalCooldown {
			return nil, refusal.err
		}
		t.dialRefusals.CompareAndDelete(refusalKey, v)
	}
	if ok && want != "" && presented != want && conn.Context().Err() == nil {
		if err := t.replaceUnboundPeer(target, conn, presented); err != nil {
			t.dialRefusals.Store(refusalKey, dialRefusal{at: time.Now(), err: err})
			return nil, err
		}
	}

	// Use singleflight to coalesce concurrent dials.
	slog.Debug("initiating connection dial",
		"target", target,
	)
	result, err, _ := t.dialGroup.Do(dialKey, func() (any, error) {
		return t.dialConnection(target, want)
	})
	if err != nil {
		if _, refused := errors.AsType[*DialIdentityError](err); refused {
			t.dialRefusals.Store(refusalKey, dialRefusal{at: time.Now(), err: err})
		}
		slog.Debug("connection dial failed",
			"target", target,
			"error", err,
		)
		return nil, err
	}
	return result.(*quic.Conn), nil
}

// replaceUnboundPeer handles a live pooled connection to target whose peer
// presented, not the identity a send wanted. While presented is still bound to
// a replica routed to target, the connection stays and the send is refused:
// one peer holds an address at a time, and two bound identities meet at one
// address only when a member has moved its own route onto another replica's —
// which must be able neither to receive that replica's traffic nor to churn
// the connection. Once presented is bound to nothing there (its replica
// removed, or rebound to a new keypair at the same address), the connection
// is closed — only if it is still the pooled one — and nil tells the caller to
// dial whoever is bound now.
func (t *QUICTransport) replaceUnboundPeer(target string, conn *quic.Conn, presented string) error {
	if slices.Contains(t.boundIdentitiesAt(target), presented) {
		return &DialError{Target: target, Err: &DialIdentityError{Presented: presented}}
	}
	t.connMu.Lock()
	removed := t.conns[target] == conn
	if removed {
		delete(t.connPeer, conn)
		delete(t.conns, target)
		delete(t.connEpoch, target)
	}
	t.connMu.Unlock()
	if removed {
		_ = conn.CloseWithError(QUICErrDialIdentity, closeReasonRedacted)
		t.evictStreamsForTarget(target)
	}
	return nil
}

// dialRefusalCooldown is how long a target whose peer failed the identity pin
// is not dialed again. Refusing a peer costs a full QUIC and TLS handshake,
// and every send that misses the connection cache would otherwise pay one: a
// handshake and log storm against that peer at the sender's message rate.
const dialRefusalCooldown = time.Second

// dialRefusalKey names the destination a dial refusal is remembered for: the
// target address and the identity the dial wanted there ("" when pinning is
// disabled).
type dialRefusalKey struct {
	target string
	want   string
}

// dialRefusal records a dial refused by the identity pin, for
// dialRefusalCooldown.
type dialRefusal struct {
	at  time.Time
	err error
}

// sweepDialRefusals forgets every remembered dial refusal whose cooldown has
// passed by now. getConnection drops an expired refusal only when the same
// destination is dialed again, so without this a destination never dialed
// again — a removed member, a rotated identity, a moved address — kept its
// entry for the life of the transport.
//
// Run on the connection-cleanup tick, it bounds the map to the refusals made
// within one cleanup interval plus the cooldown. Every refusal is for a
// (target, identity) pair membership routed a send to — never a value a peer
// chooses — and a refused pair is not dialed again inside its cooldown, so
// that window holds at most one entry per pair the host routed to in it: the
// bound follows membership, not uptime, and needs no separate cap.
func (t *QUICTransport) sweepDialRefusals(now time.Time) {
	t.dialRefusals.Range(func(k, v any) bool {
		if refusal := v.(dialRefusal); now.Sub(refusal.at) >= dialRefusalCooldown {
			t.dialRefusals.CompareAndDelete(k, v)
		}
		return true
	})
}

// forgetDialRefusals drops every remembered dial refusal for address,
// whatever identity it was refused for.
func (t *QUICTransport) forgetDialRefusals(address string) {
	t.dialRefusals.Range(func(k, _ any) bool {
		if key := k.(dialRefusalKey); key.target == address {
			t.dialRefusals.Delete(k)
		}
		return true
	})
}

// dialConnection establishes a new QUIC connection to the target address. When
// want is not "", the peer must present exactly that node identity or the
// connection is closed and refused before it is pooled (see getConnection).
func (t *QUICTransport) dialConnection(target, want string) (*quic.Conn, error) {
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

	ctx, cancel := context.WithTimeout(t.shutdownCtx, t.cfg.DialTimeout)
	defer cancel()

	clientTLS := t.tlsPair.client.Clone()
	clientTLS.ServerName = host

	conn, err := t.quicTr.Dial(ctx, udpAddr, clientTLS, t.quicCfg)
	if err == nil && want != "" {
		// The handshake proves only a cluster certificate whose SAN covers
		// the host, which a wildcard or shared-SAN certificate held by anyone
		// on the path to this address also satisfies.
		if presented := peerNodeIdentity(conn); presented != want {
			_ = conn.CloseWithError(QUICErrDialIdentity, closeReasonRedacted)
			err = &DialError{Target: target, Err: &DialIdentityError{Presented: presented}}
		}
	}
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
			delete(t.connPeer, existing)
		default:
			existingPeer := t.connPeer[existing]
			t.connMu.Unlock()
			_ = conn.CloseWithError(QUICErrDuplicateDial, "duplicate dial: reusing cached connection")
			slog.Debug("dial race lost, reusing cached connection", "target", target)
			if want != "" && existingPeer != want {
				// The winner reached another peer; the caller's destination
				// is not reachable over it (see replaceUnboundPeer).
				return nil, &DialError{Target: target, Err: &DialIdentityError{Presented: existingPeer}}
			}
			return existing, nil
		}
	}
	t.conns[target] = conn
	t.connPeer[conn] = peerNodeIdentity(conn)
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

// boundIdentitiesAt returns the node identities membership binds to the
// replicas the registry routes to target.
func (t *QUICTransport) boundIdentitiesAt(target string) []string {
	var ids []string
	for _, at := range t.reg.ReplicasAt(target) {
		if id := t.cfg.ReplicaIdentity(at.ShardID, at.ReplicaID); id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// wantFor returns the node identity a send to (shardID, replicaID) must reach
// (Config.ReplicaIdentity), or "" with ok true when pinning is disabled. ok is
// false when pinning is enabled and nothing binds the replica: the send has no
// destination it may reach.
func (t *QUICTransport) wantFor(shardID, replicaID uint64) (string, bool) {
	if t.cfg.ReplicaIdentity == nil {
		return "", true
	}
	id := t.cfg.ReplicaIdentity(shardID, replicaID)
	return id, id != ""
}

// serves reports whether a connection whose peer presented peer may carry a
// send that must reach want. want == "" means "any peer" only when pinning is
// disabled: with it enabled an empty want fails closed, so a caller that
// forgets to name its destination gets no connection rather than an unpinned
// one.
func (t *QUICTransport) serves(peer, want string) bool {
	if want == "" {
		return t.cfg.ReplicaIdentity == nil
	}
	return peer == want
}

// connServes reports whether conn may carry a send that must reach want (see
// serves).
func (t *QUICTransport) connServes(conn *quic.Conn, want string) bool {
	t.connMu.RLock()
	defer t.connMu.RUnlock()
	return t.serves(t.connPeer[conn], want)
}

// dropIdentityMismatch counts, and logs at a sampled rate, a message the send
// path refused to write because the peer at target is not its destination.
func (t *QUICTransport) dropIdentityMismatch(target string, msg *proto.Message) {
	if t.metrics != nil {
		t.metrics.IncMessagesDropped(msg.ShardID, msg.To, dropReasonIdentityMismatch)
	}
	if LogSampled(&t.lastIdentityMismatchLog, peerRejectionLogInterval) {
		slog.Warn("transport: message dropped: the peer at its replica's address presents another node identity",
			"target", target,
			"shard", msg.ShardID,
			"to", msg.To,
		)
	}
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
			logRecovered("preOpenHeartbeatStream panic recovered", r, debug.Stack(),
				"target", target)
		}
	}()

	if t.stopped.Load() {
		return
	}

	ctx, cancel := context.WithTimeout(t.shutdownCtx, streamOpenTimeout)
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
	delete(t.connPeer, t.conns[target])
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
	delete(t.connPeer, conn)
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

// CloseTarget tears down BOTH directions of this node's connectivity to
// address: every inbound connection whose certificate authorizes that address
// (see closeInboundFromPeer), the outbound connection and its epoch stamp
// (conns/connEpoch), every pooled stream opened on it, the per-target send
// queue, and the peer's incarnation record. It reports whether anything was
// actually reclaimed.
//
// It exists because nothing else ever closes a connection to a peer that is
// simply GONE. KeepAlivePeriod is below MaxIdleTimeout, so quic-go PINGs the
// connection indefinitely and it never idle-closes; cleanStaleConnections only
// reaps connections whose context is ALREADY dead, and a removed-but-still-
// running member answers keep-alives forever. Without an explicit teardown the
// connection, its streams, its epoch records and its keep-alive traffic persist
// for the process lifetime. That is true in BOTH directions, and the inbound
// one is not a subset of the outbound: a removed member that dialed this node
// while this node never dialed it has no outbound entry at all, so an
// outbound-only teardown reclaimed exactly nothing.
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
// and returns false. The snapshot veto covers the inbound half too — a transfer
// in flight is as likely to be arriving on an inbound connection as leaving on
// an outbound one.
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

	// The INBOUND half first, because it is the half that exists even when this
	// node never dialed the peer: a removed member that dialed US keeps an
	// otherwise immortal connection, and the early return below would skip it.
	// See closeInboundFromPeer.
	inbound := t.closeInboundFromPeer(address)

	// A refusal remembered for the address describes a peer membership no
	// longer routes there; whoever holds it next is dialed afresh. A refused
	// dial is never pooled, so this too precedes the early return below. It is
	// cooldown bookkeeping, not a held resource, and does not count towards
	// the result.
	t.forgetDialRefusals(address)

	// Nothing cached: leave every other structure untouched so a call for an
	// address this transport never dialed cannot disturb state that a live
	// shard may still be using.
	if !t.removeAndCloseConnection(address, "peer removed from membership") {
		return inbound > 0
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
// connection blocks for the full write deadline (sendWriteDeadline, 2s) before
// failing, which is long enough to burn a meaningful share of the peer's
// election timeout and risk an unnecessary election.
//
// When the cached entry is unusable this evicts exactly as much state as the
// failure warrants — see the three cases below — and reports false, leaving
// the caller to open a fresh stream.
//
// A stream is handed out only when its connection's peer is want (connServes):
// otherwise the slow path's getConnection decides what the send may reach.
func (t *QUICTransport) cachedStream(target string, shardID uint64, isHeartbeat bool, want string) (*quic.Stream, bool) {
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

	// The pooled connection and its peer's identity are read together: the
	// identity check costs no second acquisition of connMu on the fast path.
	t.connMu.RLock()
	conn, connOK := t.conns[target]
	connServesWant := t.serves(t.connPeer[conn], want)
	t.connMu.RUnlock()

	switch {
	case owner != nil:
		// Judge the pooled stream by its OWN connection, and require that
		// connection to be the one currently cached for the target.
		switch {
		case connOK && conn == owner && owner.Context().Err() == nil:
			if !connServesWant {
				return nil, false
			}
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
			if !connServesWant {
				return nil, false
			}
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

// ownerIsLivePooled reports whether a pooled stream's owning connection is
// alive and is the connection pooled for its target.
func ownerIsLivePooled(owner, pooled *quic.Conn, pooledOK bool) bool {
	return owner != nil && pooledOK && pooled == owner && owner.Context().Err() == nil
}

// closeStreamQuietly closes a stream getStream is discarding; a close error is
// diagnostic only.
func closeStreamQuietly(stream *quic.Stream, msg, target string, shardID uint64) {
	if err := stream.Close(); err != nil {
		slog.Debug(msg, "target", target, "shard_id", shardID, "error", err)
	}
}

// getStream returns a pooled QUIC stream for the given target. Heartbeats
// use stream index 0, data messages use (shardID % (poolSize-1)) + 1.
// The stream pool is sharded across streamShardCount buckets to reduce
// lock contention.
//
// want is the node identity the stream's peer must present (see
// getConnection); every stream returned is on a connection that serves it.
func (t *QUICTransport) getStream(target string, shardID uint64, isHeartbeat bool, want string) (*quic.Stream, error) {
	if stream, ok := t.cachedStream(target, shardID, isHeartbeat, want); ok {
		return stream, nil
	}

	idx := t.streamIndexFor(shardID, isHeartbeat)
	shard := &t.streamShards[idx&(streamShardCount-1)]
	key := streamKey{target: target, idx: idx}

	// Slow path: open a new stream.
	conn, err := t.getConnection(target, want)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(t.shutdownCtx, streamOpenTimeout)
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

		existingLive := ownerIsLivePooled(existingOwner, cachedConn, connOK)
		if existingLive && !t.connServes(existingOwner, want) {
			// The entry is on the live pooled connection, but its peer is not
			// this send's destination: the connection we opened on was
			// replaced meanwhile by one to another peer. Refuse rather than
			// tear the pooled connection down — it is healthy, and other
			// senders use it (see replaceUnboundPeer).
			shard.mu.Unlock()
			closeStreamQuietly(s, "transport: stream close failed during getStream refusal", target, shardID)
			return nil, &DialError{Target: target, Err: &DialIdentityError{}}
		}
		existingUsable := false
		if existingOwner != nil {
			existingUsable = existingLive
		} else if connOK {
			// Owner unknown (only test-injected raw streams reach this): fall
			// back to the cached conn's liveness, preserving prior behavior.
			existingUsable = cachedConn.Context().Err() == nil && t.connServes(cachedConn, want)
		}
		if existingUsable {
			// Prefer the already-cached stream; close the duplicate we opened.
			shard.mu.Unlock()
			closeStreamQuietly(s, "transport: stream close failed during getStream dedup", target, shardID)
			return existing, nil
		}

		// The existing entry is stale. If the connection WE just opened on is
		// the current, live cached conn, our fresh stream is good: replace the
		// stale entry in place WITHOUT tearing down the healthy connection.
		if connOK && cachedConn == conn && conn.Context().Err() == nil {
			shard.put(key, s, conn)
			shard.mu.Unlock()
			closeStreamQuietly(existing, "transport: stale stream close failed during getStream replace", target, shardID)
			return s, nil
		}

		// Neither the existing entry nor our stream is on a healthy cached
		// connection (the conn died out from under this dial, or was replaced).
		// Discard both, tear the connection down, and retry once with a fresh
		// dial and stream. If the retry also fails, return the error.
		shard.drop(key)
		shard.mu.Unlock()
		closeStreamQuietly(existing, "transport: stale stream close failed during getStream dedup", target, shardID)
		closeStreamQuietly(s, "transport: new stream close failed during getStream dedup", target, shardID)
		t.removeConnection(target)
		t.evictStreamsForTarget(target)
		retryConn, retryErr := t.getConnection(target, want)
		if retryErr != nil {
			return nil, retryErr
		}
		retryCtx, retryCancel := context.WithTimeout(t.shutdownCtx, streamOpenTimeout)
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
			retryOwner := shard.owners[key]
			shard.mu.Unlock()
			closeStreamQuietly(retryStream, "transport: duplicate retry stream close failed", target, shardID)
			if !t.connServes(retryOwner, want) {
				// Cached meanwhile on a connection to another peer.
				return nil, &DialError{Target: target, Err: &DialIdentityError{}}
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
//
// It runs under supervise.Supervise, which counts it in t.wg and starts it
// again on a fresh goroutine if a runtime.Goexit ends it.
func (t *QUICTransport) connectionCleanup() {
	for t.connectionCleanupOnce() {
		if !supervise.AwaitRestart(t.stopC) {
			return
		}
	}
}

// connectionCleanupOnce runs one iteration of the connection cleanup loop
// and recovers from panics. Returns true if the loop panicked, false if
// it returned normally (via stop signal).
func (t *QUICTransport) connectionCleanupOnce() (panicked bool) {
	normalReturn := false
	defer func() {
		if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
			panicked = true
			stack := debug.Stack()
			logRecovered("connectionCleanup panic recovered, restarting", r, stack,
				"error", &WorkerPanicError{WorkerName: "connectionCleanup", Value: r, Stack: stack})
		}
	}()
	t.sweepConnectionsUntilStopped()
	normalReturn = true
	return false
}

// sweepConnectionsUntilStopped is the connection cleanup loop's body: it
// sweeps the pooled connections every cleanup interval until the transport
// stops.
func (t *QUICTransport) sweepConnectionsUntilStopped() {
	// The field, not the constant: see QUICTransport.cleanupInterval. A zero
	// value means the transport was built without the constructor, so fall back
	// rather than panicking in NewTicker.
	interval := t.cleanupInterval
	if interval <= 0 {
		interval = connectionCleanupInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopC:
			return
		case <-ticker.C:
			t.cleanStaleConnections()
			t.revokeCheckConnections()
			t.reapIdleInboundConns(time.Now())
			t.sweepDialRefusals(time.Now())
		}
	}
}

// revokeCheckConnections re-runs the revocation verdict against every LIVE
// connection's verified leaf and tears down any that is no longer admissible.
//
// VerifyConnection runs only at handshake, so without this a certificate
// revoked after admission keeps its session for as long as the process lives:
// quic-go's keep-alive (10s) sits under MaxIdleTimeout (30s), so the
// connection never idle-closes, and cleanStaleConnections only reaps
// connections whose context is ALREADY dead. Revocation stopped a compromised
// node reconnecting and did nothing about the connection it already had —
// which, for a process that stays up for months, is close to no revocation.
//
// Running it on the existing 30s cleanup tick bounds post-revocation survival
// to one interval. Both directions are covered: inbound from t.inboundConns,
// outbound from the connection pool.
func (t *QUICTransport) revokeCheckConnections() {
	if t.revocationChecker == nil {
		return
	}
	drop := func(conn *quic.Conn) {
		certs := conn.ConnectionState().TLS.PeerCertificates
		if len(certs) == 0 {
			return
		}
		leaf := certs[0]
		revoked, err := t.revocationChecker.IsRevoked(leaf)
		if !revoked && err == nil {
			return
		}
		// A checker ERROR is treated the same as revoked, matching the
		// fail-closed posture VerifyConnection already takes: an unavailable
		// or stale revocation source must not leave an unverifiable peer
		// connected.
		// The precise verdict is logged HERE and is deliberately not put in the
		// CONNECTION_CLOSE reason phrase, which travels to the peer verbatim.
		// See closeReasonRedacted.
		slog.Warn("closing connection: peer certificate is no longer admissible",
			"remote", conn.RemoteAddr().String(),
			"serial", leaf.SerialNumber.Text(16),
			"revoked", revoked,
			"error", err,
		)
		_ = conn.CloseWithError(QUICErrRevoked, closeReasonRedacted)
	}

	t.inboundConns.Range(func(k, _ any) bool {
		if conn, ok := k.(*quic.Conn); ok {
			drop(conn)
		}
		return true
	})

	// Snapshot the outbound pool rather than calling drop under connMu:
	// connMu is the transport's hottest lock and CloseWithError is not
	// something to hold it across.
	t.connMu.RLock()
	outbound := make([]*quic.Conn, 0, len(t.conns))
	for _, c := range t.conns {
		outbound = append(outbound, c)
	}
	t.connMu.RUnlock()
	for _, c := range outbound {
		drop(c)
	}
}

// inboundConnState is the per-INBOUND-connection liveness record held in
// QUICTransport.inboundConns.
//
// It exists so the reclaim half of inbound tracking has something to decide on.
// Tracking alone only made the connections reachable; without a record of when
// each last did anything, the only inbound connection this node could ever
// close was one whose certificate had been revoked.
type inboundConnState struct {
	// lastActive is the Unix-nanosecond stamp of the last frame HEADER read
	// from this connection. Stamped by handleStream on every frame rather than
	// on stream acceptance: senders cache and reuse a stream across bursts, so
	// a connection carrying continuous replication traffic may accept no new
	// stream for hours, and reaping it would be a self-inflicted outage.
	//
	// Atomic because every stream goroutine on the connection writes it
	// concurrently with the cleanup tick's read. A monotonically-increasing
	// stamp does not need the writes ordered against each other: the reaper
	// only asks whether ANY of them was recent.
	lastActive atomic.Int64
}

// touch stamps the connection as having just carried a frame.
func (s *inboundConnState) touch(now time.Time) {
	if s != nil {
		s.lastActive.Store(now.UnixNano())
	}
}

// reapIdleInboundConns closes every inbound connection that has carried no
// frame for inboundIdleRetention.
//
// This is the IDLE half of the inbound reclaim policy; closeInboundFromPeer is
// the UNAUTHORIZED half and revokeCheckConnections the REVOKED half. All three
// run without a goroutine of their own — the first and third on the existing
// 30s cleanup tick, the second event-driven from CloseTarget — so post-
// admission inbound state is bounded by one tick interval rather than by the
// process lifetime.
//
// The close is all that is needed to reclaim the rest: it cancels the
// connection context, AcceptStream fails, handleConnection returns, and its
// deferred forgetInboundConnState drops the tracking entry, the SourceAddress
// authorization cache and the peer's message-budget reference. The entry is
// NOT deleted here, so a connection that somehow survives its close is
// re-examined on the next tick instead of being forgotten while still live.
func (t *QUICTransport) reapIdleInboundConns(now time.Time) {
	cutoff := now.Add(-inboundIdleRetention).UnixNano()
	t.inboundConns.Range(func(k, v any) bool {
		conn, ok := k.(*quic.Conn)
		if !ok {
			return true
		}
		state, ok := v.(*inboundConnState)
		if !ok {
			return true
		}
		if state.lastActive.Load() > cutoff {
			return true
		}
		slog.Info("reclaiming idle inbound connection",
			"remote", conn.RemoteAddr().String(),
			"idle_for", now.Sub(time.Unix(0, state.lastActive.Load())).String(),
			"limit", inboundIdleRetention.String(),
		)
		_ = conn.CloseWithError(QUICErrInboundIdle, closeReasonRedacted)
		return true
	})
}

// closeInboundFromPeer closes every INBOUND connection whose verified leaf
// authorizes address, and reports how many it closed.
//
// It is the inbound counterpart of CloseTarget's outbound teardown, and it is
// the reason CloseTarget is not just an outbound operation. A committed removal
// says the peer may no longer talk to this node AT ALL; closing only the
// connection this node dialed leaves the one the peer dialed fully alive,
// keep-alived from both ends, holding a global and a per-IP connection slot and
// free to keep opening streams for the process lifetime. Removing a member has
// to reach the half the member controls.
//
// Matching is by CERTIFICATE, not by remote address: the connection's remote
// address carries the peer's ephemeral SOURCE PORT, which never equals the
// Raft address a membership change names. The leaf covering that address is the
// same fact every other in-band authorization on this transport reduces to, and
// it is sound for exactly the same reason — per-node exclusive SANs, which
// refuseInboundPromiscuousCert enforces on admission.
//
// Co-located addresses are exempt, using the same predicate every other
// certificate control here uses. A certificate is matched on HOST alone, so on
// a single machine every replica's leaf covers every other replica's address;
// without the exemption, removing one loopback replica would close the healthy
// inbound connections of all its neighbours. See isCoLocatedCertHost.
func (t *QUICTransport) closeInboundFromPeer(address string) int {
	host := canonicalCertHost(certAuthHost(address))
	if host == "" {
		return 0
	}
	if isCoLocatedCertHost(canonicalCertHost(certAuthHost(t.localAddr())), host) {
		return 0
	}
	closed := 0
	t.inboundConns.Range(func(k, _ any) bool {
		conn, ok := k.(*quic.Conn)
		if !ok {
			return true
		}
		if !t.inboundAuthorizedFor(conn, address) {
			return true
		}
		// The membership verdict is logged HERE; the reason phrase travels to
		// the peer verbatim, so it carries none of it. See closeReasonRedacted.
		slog.Info("closing inbound connection from removed peer",
			"target", address,
			"remote", conn.RemoteAddr().String(),
		)
		_ = conn.CloseWithError(QUICErrPeerRemoved, closeReasonRedacted)
		closed++
		return true
	})
	return closed
}

// closeInboundOnShutdown closes every live inbound connection with
// QUICErrShutdown, and reports how many it closed.
//
// It is the SHUTDOWN member of the inbound reclaim family — closeInboundFromPeer
// is the unauthorized half, reapIdleInboundConns the idle half and
// revokeCheckConnections the revoked half — and the only one whose cause is this
// node rather than the peer. It exists because stop() closed t.conns and nothing
// else: a node that shut down GRACEFULLY was indistinguishable from one that had
// vanished, because quic-go's Transport.Close destroys its connections locally
// (handler.destroy) instead of emitting a CONNECTION_CLOSE.
//
// The reason phrase is NOT redacted, unlike the other three. closeReasonRedacted
// exists because those closes are authorization verdicts ON THE PEER, and the
// phrase travels to the peer verbatim; "transport shutdown" is a statement about
// this node that the peer is entitled to and can observe anyway. It is the same
// code and phrase the OUTBOUND half of stop() already sends, so a peer sees one
// verdict for the event regardless of which side dialed.
//
// Entries are not deleted here, for the reason reapIdleInboundConns gives: the
// close is what reclaims the rest — the connection context is cancelled,
// AcceptStream fails, handleConnection returns and its deferred
// forgetInboundConnState drops the entry — and stop()'s t.wg.Wait() is what
// guarantees that has happened before Stop returns.
func (t *QUICTransport) closeInboundOnShutdown() int {
	closed := 0
	t.inboundConns.Range(func(k, _ any) bool {
		conn, ok := k.(*quic.Conn)
		if !ok {
			return true
		}
		if err := conn.CloseWithError(QUICErrShutdown, "transport shutdown"); err != nil {
			slog.Debug("QUIC inbound connection close failed",
				"remote", conn.RemoteAddr().String(),
				"error", err,
			)
		}
		closed++
		return true
	})
	return closed
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
	// The scan releases connMu by defer: a panic in it must not leave the
	// pool locked for the sweep the cleanup loop restarts, and for every
	// sender after it.
	deadTargets := func() []string {
		var dead []string
		t.connMu.Lock()
		defer t.connMu.Unlock()
		for addr, conn := range t.conns {
			select {
			case <-conn.Context().Done():
				delete(t.connPeer, conn)
				delete(t.conns, addr)
				delete(t.connEpoch, addr)
				dead = append(dead, addr)
			default:
			}
		}
		return dead
	}()

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
// A WILDCARD or otherwise unnameable advertised address is the one input that
// silently disables all of the above, and is reported rather than tolerated —
// see unnameableCertSelfHost and reportPromiscuousCertCoverage.
func (t *QUICTransport) refuseInboundPromiscuousCert(conn *quic.Conn) error {
	if conn == nil || t.tlsPair == nil {
		return nil
	}
	selfHost := canonicalCertHost(certAuthHost(t.localAddr()))
	if unnameableCertSelfHost(selfHost) {
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
	if unnameableCertSelfHost(selfHost) || peerLeaf == nil {
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

// unnameableCertSelfHost reports whether a host is one no certificate can
// meaningfully be checked against, which makes every "does the peer's leaf also
// cover ME" question answer NO regardless of what the leaf contains.
//
// Two forms qualify. An EMPTY host is an address with no host part at all
// (":4001"). An UNSPECIFIED host is the wildcard bind — "0.0.0.0" or "::" —
// which is what localAddr falls back to when no AdvertiseAddress is configured
// and the listener bound every interface: it names the socket, never the node,
// and no CA issues a SAN for it.
//
// Loopback is deliberately NOT here. A loopback self-host is a real, checkable
// host; it simply lands in the co-location exemption (isCoLocatedCertHost),
// which is a decision about the DEPLOYMENT rather than an inability to ask the
// question.
func unnameableCertSelfHost(selfHost string) bool {
	if selfHost == "" {
		return true
	}
	ip := net.ParseIP(selfHost)
	return ip != nil && ip.IsUnspecified()
}

// reportPromiscuousCertCoverage warns when this node's advertised address
// cannot be matched against a certificate, which turns the connection-level
// promiscuous-certificate refusal into a permanent no-op.
//
// That refusal (refuseInboundPromiscuousCert) is the only exclusivity check an
// attacker cannot decline to run, and its whole question is "does the inbound
// peer's leaf also cover MY host". With a wildcard bind and no AdvertiseAddress
// there is no such host: 0.0.0.0 appears in no certificate, so a leaf carrying
// *.raft.example.com — which authorizes its holder to assert every member's
// address on this transport — is admitted exactly as a correctly scoped one is.
// The control does not fail; it silently stops existing, which is the worst
// state for a security control to be in.
//
// It WARNS rather than refusing to start. Failing closed here would take down
// every existing wildcard-bound deployment on upgrade, which is an operator's
// decision to make, not this function's. Called once from Start, after the
// listener has resolved the local address, because the condition is a static
// property of the configuration and a per-connection log would be a flood.
func (t *QUICTransport) reportPromiscuousCertCoverage() {
	selfHost := canonicalCertHost(certAuthHost(t.localAddr()))
	if !unnameableCertSelfHost(selfHost) {
		return
	}
	slog.Warn("promiscuous-certificate refusal is INERT: this node has no "+
		"advertised host a certificate can be matched against, so an inbound "+
		"peer whose certificate also covers this node cannot be detected; set "+
		"AdvertiseAddress to this node's reachable hostname or IP",
		"listen_address", t.cfg.ListenAddress,
		"advertise_address", t.cfg.AdvertiseAddress,
		"resolved_host", selfHost,
	)
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

// publishInboundConn makes an admitted inbound connection reachable to the
// reclaim policies and returns its liveness record.
//
// Publication is what the policies act on: revocation re-checks
// (revokeCheckConnections), the idle sweep (reapIdleInboundConns), committed
// membership removal (closeInboundFromPeer) and the shutdown announcement
// (closeInboundOnShutdown) can only reach a connection that is in the map. The
// record is stamped active on admission so a connection that has only just
// completed its handshake is never reaped before it has had a chance to send
// anything.
//
// It is called from acceptLoopIteration, SYNCHRONOUSLY, before the connection's
// handler goroutine is launched — not from the handler.
// That is the load-bearing half of the shutdown guarantee, and it only works in
// concert with stop() joining the accept loop: publishing on this goroutine is
// what makes "the accept loop has returned" mean "every connection it admitted
// is in the map", which is in turn what lets the shutdown sweep be complete
// rather than best-effort. Published on the handler's goroutine instead, a
// connection admitted just before a shutdown stayed invisible for as long as
// that goroutine took to be scheduled, and the sweep could miss it. There is
// deliberately no second t.stopped check here: two half-guarantees are how the
// window survived, and the barrier in stop() is the whole one. See
// QUICTransport.acceptWG.
func (t *QUICTransport) publishInboundConn(conn *quic.Conn) *inboundConnState {
	activity := &inboundConnState{}
	activity.touch(time.Now())
	t.inboundConns.Store(conn, activity)
	return activity
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
	t.inboundConns.Delete(conn)
	t.releasePeerMsgBudgetRef(conn)
}

// peerMsgBudget is one peer's share of the message-receive budget, shared by
// every connection that peer holds.
type peerMsgBudget struct {
	reserved atomic.Int64
	// conns counts live connections using this entry. The entry is removed
	// when it reaches zero, so the map stays bounded across connection churn
	// without being torn down while another connection is still reserving
	// against it. A negative value marks an entry its last connection has
	// retired: it is being removed and is never counted again, so a
	// connection that arrives meanwhile takes a fresh entry instead of one
	// about to disappear under it.
	conns atomic.Int64
}

// msgBudgetKey returns the accounting key for a connection's receive
// reservations: the peer's authenticated identity, falling back to its source
// IP when no verified leaf is present.
//
// Never the connection pointer and never the source port — both are things one
// peer can multiply at will, and keying on either turns a per-peer cap into a
// per-connection cap that MaxConnectionsPerIP then multiplies straight back
// out.
func msgBudgetKey(conn *quic.Conn) string {
	if id := peerNodeIdentity(conn); id != "" {
		return id
	}
	return "ip:" + extractIP(conn.RemoteAddr())
}

// acquirePeerMsgBudgetRef registers a live connection against its peer's
// budget entry. Paired with releasePeerMsgBudgetRef via forgetInboundConnState.
func (t *QUICTransport) acquirePeerMsgBudgetRef(conn *quic.Conn) {
	if conn == nil || t.maxConnMsgRecv <= 0 {
		return
	}
	t.acquirePeerMsgBudgetKey(msgBudgetKey(conn))
}

// acquirePeerMsgBudgetKey counts one more live connection on key's entry,
// never on a retired one.
func (t *QUICTransport) acquirePeerMsgBudgetKey(key string) {
	for {
		v, _ := t.connMsgBudget.LoadOrStore(key, new(peerMsgBudget))
		pb := v.(*peerMsgBudget)
		for {
			n := pb.conns.Load()
			if n < 0 {
				break // retired: help remove it, then take a fresh entry
			}
			if pb.conns.CompareAndSwap(n, n+1) {
				return
			}
		}
		t.connMsgBudget.CompareAndDelete(key, pb)
	}
}

// releasePeerMsgBudgetRef drops one connection's reference and removes the
// entry once no connection from that peer remains.
func (t *QUICTransport) releasePeerMsgBudgetRef(conn *quic.Conn) {
	if conn == nil || t.maxConnMsgRecv <= 0 {
		return
	}
	t.releasePeerMsgBudgetKey(msgBudgetKey(conn))
}

// releasePeerMsgBudgetKey drops one connection from key's entry. The last one
// retires it (0 -> -1) before removing it, and only if no connection counted
// itself in between: a decrement to zero followed by a plain Delete removed
// an entry a newly arrived connection had just taken, leaving that live
// connection's reservations on an entry no longer in the map.
func (t *QUICTransport) releasePeerMsgBudgetKey(key string) {
	v, ok := t.connMsgBudget.Load(key)
	if !ok {
		return
	}
	pb := v.(*peerMsgBudget)
	if pb.conns.Add(-1) == 0 && pb.conns.CompareAndSwap(0, -1) {
		t.connMsgBudget.CompareAndDelete(key, pb)
	}
}

// reserveConnMsgRecvBudget reserves n bytes against the per-PEER
// message-receive sub-budget in addition to the aggregate budget, in the same
// unit as the aggregate (proto.DecodeCharge), so a single
// peer cannot occupy the whole of MaxMessageReceiveMemory and force every
// other peer's frames into the exhaustion path. A nil conn (test/unit paths)
// is not accounted. Successful reservations must be paired with
// releaseConnMsgRecvBudget.
func (t *QUICTransport) reserveConnMsgRecvBudget(conn *quic.Conn, n int64) bool {
	if conn == nil || t.maxConnMsgRecv <= 0 {
		return true
	}
	v, _ := t.connMsgBudget.LoadOrStore(msgBudgetKey(conn), new(peerMsgBudget))
	pb := v.(*peerMsgBudget)
	if pb.reserved.Add(n) > t.maxConnMsgRecv {
		pb.reserved.Add(-n)
		return false
	}
	return true
}

// releaseConnMsgRecvBudget returns n previously-reserved bytes to the
// per-peer message-receive sub-budget.
func (t *QUICTransport) releaseConnMsgRecvBudget(conn *quic.Conn, n int64) {
	if conn == nil || t.maxConnMsgRecv <= 0 {
		return
	}
	v, ok := t.connMsgBudget.Load(msgBudgetKey(conn))
	if !ok {
		return
	}
	v.(*peerMsgBudget).reserved.Add(-n)
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
