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
	"sync"
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

	// evictionBudget bounds epoch-driven evictions one peer address may force per evictionWindow.
	evictionBudget = 4

	// evictionWindow is the period over which evictionBudget applies.
	evictionWindow = 30 * time.Second

	// staleOutboundDrainGrace is how long an evicted outbound stays open out of the pool so frames already written are delivered.
	staleOutboundDrainGrace = time.Second

	// peerIncarnationRetention is how long a peer's incarnation record survives with no outbound and no batch.
	peerIncarnationRetention = 10 * connectionCleanupInterval

	// inboundIdleRetention is how long an inbound connection may carry no frame before it is reclaimed; keep-alives stop QUIC idle-closing it.
	inboundIdleRetention = 10 * connectionCleanupInterval
)

// getConnection returns the pooled connection to target, dialing one when needed; the peer must present want, or replaceUnboundPeer decides.
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
			// streams so callers don't get stale stream handles. The
			// diagnostic follows the eviction it reports.
			t.removeConnection(target)
			t.evictStreamsForTarget(target)
			slog.Debug("stale connection detected, reconnecting",
				"target", target,
			)
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
	// Report the connection event outside the singleflight, in dial-start order; the turn is released by defer.
	var (
		report func()
		turn   reportTurn
	)
	defer t.dialReports.release(dialKey, &turn)
	v, err, _ := t.dialGroup.Do(dialKey, func() (any, error) {
		turn = t.dialReports.take(dialKey)
		conn, r, err := t.dialAndPool(target, want)
		report = r
		return &dialOutcome{conn: conn, reported: make(chan struct{})}, err
	})
	outcome := v.(*dialOutcome)
	if report != nil {
		func() {
			defer close(outcome.reported)
			defer t.dialReports.release(dialKey, &turn)
			turn.await()
			report()
		}()
	}
	<-outcome.reported
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
	return outcome.conn, nil
}

// dialOutcome is the result of one coalesced dial, shared by every sender
// that joined it: the connection (nil when the dial failed) and reported,
// closed once the dialing sender has delivered the dial's connection event.
type dialOutcome struct {
	conn     *quic.Conn
	reported chan struct{}
}

// dialReportSequencer orders the connection events of consecutive dials to one destination.
type dialReportSequencer struct {
	mu    sync.Mutex
	tails map[string]chan struct{}
}

// reportTurn is one dial's place in its destination's report order.
type reportTurn struct {
	// prev is closed once the previous dial to the destination has reported;
	// nil when none was pending.
	prev <-chan struct{}
	// done is closed once this dial has reported, or ended without
	// reporting; nil once released, or for a goroutine that only waited for
	// another's dial.
	done chan struct{}
}

// take appends a dial to key's report order and returns its turn.
func (s *dialReportSequencer) take(key string) reportTurn {
	done := make(chan struct{})
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tails == nil {
		s.tails = make(map[string]chan struct{})
	}
	prev, pending := s.tails[key]
	s.tails[key] = done
	if !pending {
		return reportTurn{done: done}
	}
	return reportTurn{prev: prev, done: done}
}

// await waits until every dial to the destination before this one has
// reported.
func (r *reportTurn) await() {
	if r.prev != nil {
		<-r.prev
	}
}

// release ends turn after earlier dials, letting the next dial report; a no-op for a released or untaken turn.
func (s *dialReportSequencer) release(key string, turn *reportTurn) {
	if turn.done == nil {
		return
	}
	turn.await()
	close(turn.done)
	s.mu.Lock()
	if s.tails[key] == turn.done {
		delete(s.tails, key)
	}
	s.mu.Unlock()
	turn.done = nil
}

// replaceUnboundPeer refuses the send while presented is still bound at target, else closes the pooled connection and returns nil to re-dial.
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

// dialRefusalCooldown is how long a target whose peer failed the identity pin is not dialed again.
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

// sweepDialRefusals forgets expired dial refusals; run on the cleanup tick, it bounds the map by membership.
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

// dialAndPool dials and pools a QUIC connection to target, refusing a peer not presenting want; the returned non-nil report delivers the event outside the dial.
func (t *QUICTransport) dialAndPool(target, want string) (*quic.Conn, func(), error) {
	noEvent := func() {}
	udpAddr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, noEvent, err
	}

	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return nil, noEvent, &DialError{Target: target, Err: err}
	}
	if host == "" {
		// An empty host would silently verify the peer cert against
		// "localhost", masking a misconfigured target and failing SAN
		// matching against real-hostname/IP certs. Fail loudly instead.
		return nil, noEvent, &DialError{Target: target, Err: ErrEmptyDialHost}
	}

	// Capture the peer epoch before the dial, so the stamp is never newer than the connection it describes.
	dialEpoch := t.knownPeerEpoch(target)

	ctx, cancel := context.WithTimeout(t.shutdownCtx, t.cfg.DialTimeout)
	defer cancel()

	// The current pair is read per dial, so a reloaded leaf or CA pool
	// reaches the next outbound handshake.
	clientTLS := t.currentTLS().client.Clone()
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
		return nil, t.connectionEvent(target, nil, err), err
	}

	// Insert under connMu guarded by stopped, so a dial racing Stop cannot leak a connection.
	t.connMu.Lock()
	if t.stopped.Load() {
		t.connMu.Unlock()
		_ = conn.CloseWithError(QUICErrShutdown, "transport stopped during dial")
		return nil, noEvent, ErrStreamClosed
	}
	// Re-check the cache under the lock; keep the live winner and close the loser.
	if existing, ok := t.conns[target]; ok && existing != conn {
		select {
		case <-existing.Context().Done():
			// Cached connection is dead; replace it with the fresh dial.
			delete(t.connPeer, existing)
		default:
			existingPeer := t.connPeer[existing]
			t.connMu.Unlock()
			_ = conn.CloseWithError(QUICErrDuplicateDial, "duplicate dial: reusing cached connection")
			lost := func() {
				supervise.LogContained(func() {
					slog.Debug("dial race lost, reusing cached connection", "target", target)
				})
			}
			if want != "" && existingPeer != want {
				// The winner reached another peer; the caller's destination
				// is not reachable over it (see replaceUnboundPeer).
				return nil, lost, &DialError{Target: target, Err: &DialIdentityError{Presented: existingPeer}}
			}
			return existing, lost, nil
		}
	}
	t.conns[target] = conn
	t.connPeer[conn] = peerNodeIdentity(conn)
	t.connEpoch[target] = dialEpoch
	t.connMu.Unlock()

	// Pre-open the heartbeat stream (index 0); the goroutine is tracked by the WaitGroup.
	t.wg.Add(1)
	go t.preOpenHeartbeatStream(target, conn)

	return conn, t.connectionEvent(target, conn, nil), nil
}

// connectionEvent returns the report of a dial to target: contained diagnostics, then OnConnectionFailed or OnConnectionEstablished.
func (t *QUICTransport) connectionEvent(target string, conn *quic.Conn, err error) func() {
	return func() {
		supervise.LogContained(func() {
			if err != nil {
				slog.Info("peer connection failed", "target", target, "error", err)
				return
			}
			slog.Debug("connection established",
				"target", target,
				"local_addr", conn.LocalAddr().String(),
				"remote_addr", conn.RemoteAddr().String(),
			)
			slog.Info("peer connected", "target", target)
		})
		if t.eventListener == nil {
			return
		}
		if err != nil {
			if cb := t.eventListener.OnConnectionFailed; cb != nil {
				deliverEvent(func() { cb(config.ConnectionInfo{Address: target, Err: err}) })
			}
			return
		}
		if cb := t.eventListener.OnConnectionEstablished; cb != nil {
			deliverEvent(func() { cb(config.ConnectionInfo{Address: target}) })
		}
	}
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

// wantFor returns the identity a send to (shardID, replicaID) must reach, "" when pinning is off; ok is false when nothing binds the replica.
func (t *QUICTransport) wantFor(shardID, replicaID uint64) (string, bool) {
	if t.cfg.ReplicaIdentity == nil {
		return "", true
	}
	id := t.cfg.ReplicaIdentity(shardID, replicaID)
	return id, id != ""
}

// serves reports whether a connection to peer may carry a send for want; an empty want fails closed when pinning is on.
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

// preOpenHeartbeatStream opens the heartbeat stream (index 0) asynchronously; a failure is non-fatal.
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
		// Keep the pooled entry unless its connection is dead; superseded entries are evicted lazily by getStream.
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

// removeConnection removes a connection from the pool without closing it; the peerEpoch record is kept for the re-dial.
func (t *QUICTransport) removeConnection(target string) {
	t.connMu.Lock()
	delete(t.connPeer, t.conns[target])
	delete(t.conns, target)
	delete(t.connEpoch, target)
	t.connMu.Unlock()
}

// peerIncarnation records one peer address's process incarnations; boot epochs are unordered, so superseded records witnessed order.
type peerIncarnation struct {
	// epoch is the peer's current boot epoch, as last observed from a batch
	// whose claimed source address the sender's certificate covers.
	epoch uint64

	// superseded is a small ring of epochs that were once current and have
	// since been replaced. It is the hysteresis that makes a peer's
	// incarnation history one-way.
	superseded [supersededEpochRing]uint64
	next       int

	// lastSeen is when a batch from this peer last arrived or an outbound was dialed.
	lastSeen time.Time

	// evictions and evictWindowStart bound epoch-driven evictions per window; see allowEviction.
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

// observePeerEpoch folds a batch's SourceEpoch into target's record and reports whether it may drive eviction; retired epochs return false.
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

// recordedEpochDecision returns the verdict target's record alone settles, refreshing lastSeen; the caller holds peerEpochMu.
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

// allowEviction charges one eviction against target's budget and reports whether it may proceed.
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

// reapPeerIncarnations drops records of addresses with no outbound and no batch for peerIncarnationRetention.
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

// removeAndCloseConnection removes and closes target's pooled connection outside the lock, reporting whether one existed.
func (t *QUICTransport) removeAndCloseConnection(target string, reason string) bool {
	t.connMu.Lock()
	conn, ok := t.conns[target]
	delete(t.connPeer, conn)
	delete(t.conns, target)
	delete(t.connEpoch, target)
	t.connMu.Unlock()

	if ok && conn != nil {
		if closeErr := conn.CloseWithError(0, reason); closeErr != nil {
			// Contained: every caller has teardown still to do after this
			// (stream eviction, the send queue, the incarnation record).
			supervise.LogContained(func() {
				slog.Debug("transport: connection close failed during removeAndClose",
					"target", target, "error", closeErr)
			})
		}
	}
	return ok
}

// CloseTarget tears down inbound and outbound connectivity to address for a committed removal, vetoed by any snapshot in flight; the caller checks registry.AddressInUse first. Idempotent.
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

	// The inbound half first: it exists even when this node never dialed the peer.
	inbound := t.closeInboundFromPeer(address)

	// Forget a remembered refusal for the address; it does not count towards the result.
	t.forgetDialRefusals(address)

	// Close an outbound still in its drain grace; it counts towards the result.
	drained := t.closeDrainingTo(address, "peer removed from membership")

	// Nothing cached: leave every other structure untouched so a call for an
	// address this transport never dialed cannot disturb state that a live
	// shard may still be using.
	if !t.removeAndCloseConnection(address, "peer removed from membership") {
		return inbound > 0 || drained > 0
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

// forgetPeerIncarnation drops target's incarnation record; only a committed removal does this.
func (t *QUICTransport) forgetPeerIncarnation(target string) {
	t.peerEpochMu.Lock()
	delete(t.peerEpoch, target)
	t.peerEpochMu.Unlock()
}

// cachedStream returns the pooled stream for (target, shardID, isHeartbeat) if its connection is live and serves want, evicting what a failure warrants.
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
			// Re-verify the stream is still cached after the conn read.
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
			// The stream's connection is superseded: drop the stream, keep the cached conn.
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

// closeStreamQuietly closes a discarded stream; the diagnostic is contained.
func closeStreamQuietly(stream *quic.Stream, msg, target string, shardID uint64) {
	if err := stream.Close(); err != nil {
		supervise.LogContained(func() {
			slog.Debug(msg, "target", target, "shard_id", shardID, "error", err)
		})
	}
}

// getStream returns a pooled QUIC stream for target serving want: index 0 for heartbeats, (shardID % (poolSize-1)) + 1 for data.
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
	if t.beforeStreamInsert != nil {
		t.beforeStreamInsert()
	}

	shard.mu.Lock()
	// Double-check another goroutine didn't create it.
	if existing, ok := shard.streams[key]; ok {
		// Judge the existing stream by its own owning connection.
		existingOwner := shard.owners[key]
		t.connMu.RLock()
		cachedConn, connOK := t.conns[target]
		t.connMu.RUnlock()

		existingLive := ownerIsLivePooled(existingOwner, cachedConn, connOK)
		if existingLive && !t.connServes(existingOwner, want) {
			// The live pooled connection now serves another peer: refuse rather than tear it down.
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

		// Our conn is the live cached one: replace the stale entry in place.
		if connOK && cachedConn == conn && conn.Context().Err() == nil {
			shard.put(key, s, conn)
			shard.mu.Unlock()
			closeStreamQuietly(existing, "transport: stale stream close failed during getStream replace", target, shardID)
			return s, nil
		}

		// Neither stream is on a healthy cached conn: discard both, tear down, retry once.
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
		// Double-check before inserting so a concurrently cached stream is not leaked.
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

// connectionCleanup periodically removes pooled connections whose context is done; supervised and restarted on panic or Goexit.
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

// revokeCheckConnections re-runs the revocation verdict on every live connection each cleanup tick and closes inadmissible ones.
func (t *QUICTransport) revokeCheckConnections() {
	if t.revocationChecker == nil {
		return
	}
	// live collects the leaves of the connections the sweep keeps: an issuer
	// a CA reload dropped is retained by the checker only while one of them
	// needs it, and released at the end of the sweep after the last closes.
	var live []*x509.Certificate
	drop := func(conn *quic.Conn) {
		certs := conn.ConnectionState().TLS.PeerCertificates
		if len(certs) == 0 {
			return
		}
		leaf := certs[0]
		// Established, not IsRevoked: a session admitted under a CA the
		// bundle has since dropped is checked against that CA, not failed
		// for lacking an issuer in the new set.
		revoked, err := t.revocationChecker.IsRevokedEstablished(leaf)
		if !revoked && err == nil {
			live = append(live, leaf)
			return
		}
		// A checker error counts as revoked; close first, then log the verdict, never in the reason phrase.
		_ = conn.CloseWithError(QUICErrRevoked, closeReasonRedacted)
		remote, serial := conn.RemoteAddr().String(), leaf.SerialNumber.Text(16)
		supervise.LogContained(func() {
			slog.Warn("closing connection: peer certificate is no longer admissible",
				"remote", remote, "serial", serial, "revoked", revoked, "error", err)
		})
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
	t.revocationChecker.PruneRetainedIssuers(live)
}

// inboundConnState is the per-inbound-connection liveness record in QUICTransport.inboundConns.
type inboundConnState struct {
	// lastActive is the Unix-nanosecond time of the last frame header read, written atomically per frame.
	lastActive atomic.Int64
}

// touch stamps the connection as having just carried a frame.
func (s *inboundConnState) touch(now time.Time) {
	if s != nil {
		s.lastActive.Store(now.UnixNano())
	}
}

// reapIdleInboundConns closes inbound connections idle for inboundIdleRetention; handleConnection's defer drops the entry.
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
		idleFor := now.Sub(time.Unix(0, state.lastActive.Load()))
		_ = conn.CloseWithError(QUICErrInboundIdle, closeReasonRedacted)
		remote := conn.RemoteAddr().String()
		supervise.LogContained(func() {
			slog.Info("reclaiming idle inbound connection",
				"remote", remote, "idle_for", idleFor.String(), "limit", inboundIdleRetention.String())
		})
		return true
	})
}

// closeInboundFromPeer closes every inbound connection whose verified leaf authorizes address, except co-located hosts, and returns the count.
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
		// Log the verdict after the close; the reason phrase carries none of it.
		_ = conn.CloseWithError(QUICErrPeerRemoved, closeReasonRedacted)
		remote := conn.RemoteAddr().String()
		supervise.LogContained(func() {
			slog.Info("closing inbound connection from removed peer", "target", address, "remote", remote)
		})
		closed++
		return true
	})
	return closed
}

// closeInboundOnShutdown closes every live inbound connection with QUICErrShutdown and returns the count.
func (t *QUICTransport) closeInboundOnShutdown() int {
	closed := 0
	t.inboundConns.Range(func(k, _ any) bool {
		conn, ok := k.(*quic.Conn)
		if !ok {
			return true
		}
		if err := conn.CloseWithError(QUICErrShutdown, "transport shutdown"); err != nil {
			remote := conn.RemoteAddr().String()
			supervise.LogContained(func() {
				slog.Debug("QUIC inbound connection close failed", "remote", remote, "error", err)
			})
		}
		closed++
		return true
	})
	return closed
}

// cleanStaleConnections removes dead connections and their pooled streams and reaps idle incarnation records; callbacks run outside connMu.
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

	// Evict streams for every dead target, then contained diagnostics, then each callback in order, all outside connMu.
	for _, target := range deadTargets {
		t.evictStreamsForTarget(target)
	}
	t.reapPeerIncarnations(time.Now())

	if len(deadTargets) == 0 {
		return
	}
	supervise.LogContained(func() {
		for _, target := range deadTargets {
			slog.Info("peer disconnected", "target", target)
		}
	})
	if t.eventListener == nil || t.eventListener.OnConnectionFailed == nil {
		return
	}
	onFailed := t.eventListener.OnConnectionFailed
	supervise.RunEach(len(deadTargets), func(i int) {
		onFailed(config.ConnectionInfo{Address: deadTargets[i]})
	})
}

// evictStreamsForTarget removes and closes all pooled streams for address.
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

		// Every dropped stream is closed before any diagnostic: they are out
		// of the pool, so one left open by a log handler that panicked or
		// called runtime.Goexit would hold its stream credit for good.
		var closeErrs []error
		for _, s := range toClose {
			if s != nil {
				if closeErr := s.Close(); closeErr != nil {
					closeErrs = append(closeErrs, closeErr)
				}
			}
		}
		if len(closeErrs) > 0 {
			supervise.LogContained(func() {
				for _, closeErr := range closeErrs {
					slog.Debug("transport: stream close failed during eviction",
						"target", target, "error", closeErr)
				}
			})
		}
	}
}

// evictStaleOutboundOnRecv evicts the cached outbound to the sender on a certificate-authorized boot epoch change, or when it is dead.
func (t *QUICTransport) evictStaleOutboundOnRecv(target string, srcEpoch uint64, inboundConn *quic.Conn) {
	if target == "" {
		return
	}

	// Track the peer's boot epoch only once the claimed target is authorized; a superseded epoch is ignored.
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
		// Alive outbound: evict only when it cannot be attributed to this batch's incarnation.
		if srcEpoch == 0 || !attributable || stamp == srcEpoch {
			return
		}
		if !t.inboundAuthorizedForCached(inboundConn, target) {
			return
		}
		// Bound how often one peer may drive this.
		if !t.allowEviction(target, time.Now()) {
			supervise.LogContained(func() {
				slog.Warn("epoch-driven eviction budget exhausted for peer; ignoring restart hint",
					"target", target, "peer_epoch", srcEpoch, "dialed_under_epoch", stamp,
					"budget", evictionBudget, "window", evictionWindow)
			})
			return
		}
		// Take the outbound out of the pool now and close it after staleOutboundDrainGrace; diagnostics after.
		t.drainOutbound(target, outbound)
		t.evictStreamsForTarget(target)
		supervise.LogContained(func() {
			slog.Info("evicting outbound not attributable to the sending peer incarnation",
				"target", target, "peer_epoch", srcEpoch, "dialed_under_epoch", stamp,
				"drain_grace", staleOutboundDrainGrace)
		})
	}
}

// drainOutbound takes conn out of target's pool now and closes it after staleOutboundDrainGrace; stop closes any still draining.
func (t *QUICTransport) drainOutbound(target string, conn *quic.Conn) {
	t.connMu.Lock()
	if t.conns[target] == conn {
		delete(t.connPeer, conn)
		delete(t.conns, target)
		delete(t.connEpoch, target)
	}
	if t.stopped.Load() {
		t.connMu.Unlock()
		t.closeDrained(target, conn)
		return
	}
	// Registered and counted under connMu with stopped false, so stop's own
	// connMu section (after it sets stopped) sees the entry, and the Add
	// happens before its wg.Wait.
	t.draining[conn] = target
	t.wg.Add(1)
	t.connMu.Unlock()
	go t.closeAfterDrain(target, conn)
}

// closeAfterDrain closes a draining outbound once its grace expires, unless
// the connection died first or the transport is stopping, in which case stop
// owns the close (it must precede quicTr.Close to reach the peer).
func (t *QUICTransport) closeAfterDrain(target string, conn *quic.Conn) {
	defer t.wg.Done()
	timer := time.NewTimer(staleOutboundDrainGrace)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-conn.Context().Done():
	case <-t.stopC:
		return
	}
	t.connMu.Lock()
	_, owned := t.draining[conn]
	delete(t.draining, conn)
	t.connMu.Unlock()
	if owned {
		t.closeDrained(target, conn)
	}
}

// closeDrained closes an outbound evicted by evictStaleOutboundOnRecv.
func (t *QUICTransport) closeDrained(target string, conn *quic.Conn) {
	if err := conn.CloseWithError(QUICErrShutdown, "outbound predates peer incarnation"); err != nil {
		supervise.LogContained(func() {
			slog.Debug("transport: connection close failed during stale eviction",
				"target", target, "error", err)
		})
	}
}

// closeDrainingTo closes every draining outbound to target and returns the count.
func (t *QUICTransport) closeDrainingTo(target string, reason string) int {
	var drained []*quic.Conn
	t.connMu.Lock()
	for conn, to := range t.draining {
		if to == target {
			drained = append(drained, conn)
			delete(t.draining, conn)
		}
	}
	t.connMu.Unlock()
	for _, conn := range drained {
		if err := conn.CloseWithError(0, reason); err != nil {
			supervise.LogContained(func() {
				slog.Debug("transport: draining connection close failed during CloseTarget",
					"target", target, "error", err)
			})
		}
	}
	return len(drained)
}

// inboundAuthorizedFor reports whether conn's verified peer certificate covers target's host; no verified leaf is unauthorized.
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
	// cert's DNSNames/IPAddresses only (Go ignores the Subject CommonName),
	// exactly as the dial path's ServerName check does.
	return certs[0].VerifyHostname(host) == nil
}

// certAuthHost extracts the host a certificate is matched against for a Raft address; the port is ignored.
func certAuthHost(target string) string {
	return netaddr.Host(target)
}

// canonicalCertHost normalises a host as x509.VerifyHostname compares it; see netaddr.CanonicalHost.
func canonicalCertHost(host string) string {
	return netaddr.CanonicalHost(host)
}

// isCoLocatedCertHost reports whether a peer host is this node's host or a loopback address, which no certificate policy can separate.
func isCoLocatedCertHost(selfHost, peerHost string) bool {
	if peerHost == "" || peerHost == selfHost {
		return true
	}
	if ip := net.ParseIP(peerHost); ip != nil && ip.IsLoopback() {
		return true
	}
	return false
}

// VerifyPeerAddressExclusivity reports whether this node's own leaf also covers any peer address, a fatal deployment error; co-located peers are skipped.
func (t *QUICTransport) VerifyPeerAddressExclusivity(selfAddr string, peers []string) error {
	pair := t.currentTLS()
	if pair == nil || pair.leaf == nil {
		// Unreachable: NewQUICTransport fails on an unparseable leaf. Fail closed.
		return ErrNoLocalCertificate
	}
	return verifyCertificateExclusivity(pair.leaf, selfAddr, peers)
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

// refuseInboundPromiscuousCert returns why to refuse an inbound connection whose leaf also covers this node's host, or nil; checked once per connection, co-located peers exempt.
func (t *QUICTransport) refuseInboundPromiscuousCert(conn *quic.Conn) error {
	if conn == nil || t.currentTLS() == nil {
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

// peerCertificateExclusivity is refuseInboundPromiscuousCert over explicit selfAddr and peerAddr, for tests.
func peerCertificateExclusivity(peerLeaf *x509.Certificate, selfAddr, peerAddr string) error {
	selfHost := canonicalCertHost(certAuthHost(selfAddr))
	if unnameableCertSelfHost(selfHost) || peerLeaf == nil {
		return nil
	}
	if isCoLocatedCertHost(selfHost, canonicalCertHost(certAuthHost(peerAddr))) {
		return nil
	}
	// VerifyHostname returning nil means the peer's certificate covers this host: refuse.
	coversSelf := peerLeaf.VerifyHostname(selfHost) == nil
	if !coversSelf {
		return nil
	}
	return &PromiscuousPeerCertificateError{PeerAddress: peerAddr, SelfHost: selfHost}
}

// unnameableCertSelfHost reports whether host is empty or unspecified, so no certificate can be checked against it.
func unnameableCertSelfHost(selfHost string) bool {
	if selfHost == "" {
		return true
	}
	ip := net.ParseIP(selfHost)
	return ip != nil && ip.IsUnspecified()
}

// reportPromiscuousCertCoverage warns once at Start when the advertised address is unnameable, which disables the promiscuous-certificate refusal.
func (t *QUICTransport) reportPromiscuousCertCoverage() {
	selfHost := canonicalCertHost(certAuthHost(t.localAddr()))
	if !unnameableCertSelfHost(selfHost) {
		return
	}
	// Contained: Start calls this with the listener bound and its loops not
	// yet started, and a log handler must not be able to leave it there.
	listen, advertise := t.cfg.ListenAddress, t.cfg.AdvertiseAddress
	supervise.LogContained(func() {
		slog.Warn("promiscuous-certificate refusal is INERT: this node has no "+
			"advertised host a certificate can be matched against, so an inbound "+
			"peer whose certificate also covers this node cannot be detected; set "+
			"AdvertiseAddress to this node's reachable hostname or IP",
			"listen_address", listen,
			"advertise_address", advertise,
			"resolved_host", selfHost,
		)
	})
}

// inboundAuthorizedForCached caches inboundAuthorizedFor per (conn, target), failing closed after srcAddrAuthMaxDistinct claims.
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

// publishInboundConn makes an admitted inbound connection reachable to the reclaim policies; called synchronously from acceptLoopIteration so shutdown sees it.
func (t *QUICTransport) publishInboundConn(conn *quic.Conn) *inboundConnState {
	activity := &inboundConnState{}
	activity.touch(time.Now())
	t.inboundConns.Store(conn, activity)
	return activity
}

// forgetInboundConnState removes a connection's caches after its handler and stream goroutines finish.
func (t *QUICTransport) forgetInboundConnState(conn *quic.Conn) {
	t.srcAddrAuth.Delete(conn)
	t.inboundConns.Delete(conn)
	t.releasePeerMsgBudgetRef(conn)
}

// peerMsgBudget is one peer's share of the message-receive budget, shared by
// every connection that peer holds.
type peerMsgBudget struct {
	reserved atomic.Int64
	// conns counts live connections on this entry; negative marks a retiring entry.
	conns atomic.Int64
}

// msgBudgetKey returns the receive-budget key: the peer identity, else its source IP; never the connection or port.
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

// releasePeerMsgBudgetKey drops one connection from key's entry, retiring it (0 -> -1) before removal.
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

// reserveConnMsgRecvBudget reserves n bytes against the per-peer receive budget as well as the aggregate; pair with releaseConnMsgRecvBudget.
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

// releaseConnMsgRecvBudget returns n reserved bytes to the per-peer receive budget.
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

// isPeerFrameRejection reports whether err is a remote QUICStreamErrFrameRejected reset; replace the stream, keep the connection.
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

// evictStream removes and closes one pooled stream if it is still the pooled one, leaving the connection alone.
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
		supervise.LogContained(func() {
			slog.Debug("transport: stream close failed during single-stream eviction",
				"target", target, "shard_id", shardID, "error", closeErr)
		})
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
