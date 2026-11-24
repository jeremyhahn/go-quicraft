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

package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/server"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Peer is the engine's boundary to the raft state machine, owned by one worker at a time; only leaderID and membershipSnap are safe for concurrent reads.
type Peer struct {
	// boundConfigChanges holds proposal keys of config changes bound to their ConfigChangeID; written by the Host, read by the step worker.
	boundConfigChanges sync.Map
	// r is the core Raft state machine. All protocol operations are
	// delegated to it. It is not safe for concurrent access; the engine
	// pipeline enforces single-owner semantics.
	r *raft

	// cfg is the per-shard configuration, retained for role queries
	// (IsObserver, IsWitness) and shard/replica identification.
	cfg config.Config

	// logReader is the persistent log reader, retained for reference.
	logReader LogReader

	// membership is the step worker's mutable membership.
	membership *membership

	// membershipSnap is the published immutable membership, read lock-free by the transport and host.
	membershipSnap atomic.Pointer[membershipSnapshot]

	// pendingSnap is the published pending configuration above the applied one, or nil; written by the step worker.
	pendingSnap atomic.Pointer[pendingConfig]

	// prevState records the hard state from the last GetUpdate call.
	// Used by hasStateChanged to detect whether the engine needs to
	// persist new hard state.
	prevState proto.State

	// durableState is the hard state last confirmed on stable storage; it never runs ahead of the disk.
	durableState proto.State

	// stepMarkIndex and stepMarkTerm are the log's last index and term at the last drain.
	stepMarkIndex uint64
	stepMarkTerm  uint64

	// stateUnpersisted forces the next GetUpdate to report a state change after an extracted update was lost before SaveState.
	stateUnpersisted bool

	// leaderID is written by syncLeaderID and read atomically by any goroutine.
	leaderID atomic.Uint64

	// term mirrors r.term for the same reason leaderID mirrors r.leaderID:
	// callers outside the step worker need the term that accompanies a
	// leadership change, and the raft struct is not thread-safe.
	term atomic.Uint64

	// leaderSeq is a seqlock making leaderID and term readable as a consistent pair; odd while syncLeaderID writes.
	leaderSeq atomic.Uint64

	// readReady reports, under the same seqlock, whether the current term's leader has committed an entry of that term.
	readReady atomic.Bool

	// readyTerm and readyProbeTerm/readyProbeCommit cache computeReadReady's results until the term or commit index moves.
	readyTerm        uint64
	readyProbeTerm   uint64
	readyProbeCommit uint64

	// shardID and replicaID identify this peer. Cached from cfg for
	// fast access without going through raft.
	shardID   uint64
	replicaID uint64

	// hasPendingSnapshot tracks whether a snapshot is waiting to be
	// persisted. Set when raft restores a snapshot, cleared after
	// GetUpdate returns it.
	hasPendingSnapshot bool

	// joining marks a configuration known to be incomplete; it blocks campaigning and the vote candidate test until a member other than self is applied.
	joining atomic.Bool

	// staleConfigThrough is the recovered commit index until every config change at or below it is folded into membership; 0 means current.
	staleConfigThrough uint64

	// appliedClamp records the clamp SetProcessedIndex applied to a state
	// machine applied index ahead of the recovered commit index; zero when
	// none was applied. Applied() is the only trustworthy test for one.
	appliedClamp AppliedClamp

	// unfoldedConfig records the committed config window SetProcessedIndex
	// could not fold; zero when the fold was complete. Recorded() is the only
	// trustworthy test for one.
	unfoldedConfig UnfoldedConfig

	// logger receives configuration-tail diagnostics; nil means the slog default.
	logger *slog.Logger

	// wire is the local set of readable wire versions; 0 means proto.DefaultWireWindow.
	wire proto.WireWindow

	// configFoldedThrough is the highest index offered to applyMembershipEntry, where resolveStaleStartupConfig resumes; seeded from the base ConfigChangeID.
	configFoldedThrough uint64
}

// NewPeer creates a new Peer for the given shard configuration and log
// reader. It initializes the raft state machine, which starts as a
// follower and will participate in elections via Tick/Handle.
func NewPeer(cfg config.Config, logReader LogReader, globalRL *server.RateLimiter) (*Peer, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	r, err := newRaft(cfg, logReader)
	if err != nil {
		return nil, err
	}

	// Wire global rate limiter into the inmem layer.
	if globalRL != nil {
		r.log.inmem.globalRL = globalRL
	}

	_, ms := logReader.NodeState()

	p := &Peer{
		r:          r,
		cfg:        cfg,
		logReader:  logReader,
		membership: newMembershipWithPolicy(ms, cfg.RequireMemberIdentities),
		prevState:  r.hardState(),
		shardID:    cfg.ShardID,
		replicaID:  cfg.ReplicaID,
	}
	p.durableState = p.prevState
	p.markStep()

	// Arm the staleness watermark and its resolver together, only when the recovered commit index is past the base configuration.
	if r.log.committed > ms.ConfigChangeID {
		p.staleConfigThrough = r.log.committed
	}
	p.configFoldedThrough = ms.ConfigChangeID
	r.resolveStaleConfig = p.resolveStaleStartupConfig

	// Use membership.validate as the leader's propose-time gate, so wire-forwarded proposals are validated too.
	r.validateConfigChange = p.validateProposedConfigChange
	r.takeBoundConfigChange = p.takeBoundConfigChange

	p.publishMembership()
	p.syncLeaderID()
	return p, nil
}

// resolveStaleStartupConfig folds the recovered configuration tail from configFoldedThrough on demand before a campaign; false skips this campaign.
func (p *Peer) resolveStaleStartupConfig() bool {
	if p.staleConfigThrough == 0 {
		return true
	}
	complete, err := p.replayConfigChangesFrom(p.configFoldedThrough+1, p.staleConfigThrough)
	if err != nil {
		p.log().Error("configuration tail holds an undecodable committed config entry: "+
			"refusing to campaign on a configuration this replica cannot reconstruct",
			"shard", p.shardID,
			"replica", p.replicaID,
			"folded_through", p.configFoldedThrough,
			"stale_through", p.staleConfigThrough,
			"error", err,
		)
		return false
	}
	if !complete {
		p.log().Warn("configuration tail unresolved: refusing to campaign against a "+
			"possibly superseded voting set",
			"shard", p.shardID,
			"replica", p.replicaID,
			"folded_through", p.configFoldedThrough,
			"stale_through", p.staleConfigThrough,
			"config_change_id", p.membership.configChangeID,
		)
		return false
	}
	p.staleConfigThrough = 0
	return true
}

// pendingConfig is one published view of raftLog.pendingAdds: the adds above
// the applied (or apply-refused) configuration, plus the two version marks that
// decide whether a republish is needed.
type pendingConfig struct {
	version        uint64
	configChangeID uint64
	// committed is the commit index the view was resolved against, part of the key only while something is pending.
	committed uint64
	adds      map[uint64]string
	// removed is every replica a pending RemoveNode removes, indexed by ID and identity.
	removed           map[uint64]string
	removedIdentities map[string]uint64
}

// PublishPendingConfig publishes the pending configuration for PendingMemberIdentity; it must run on the step worker.
func (p *Peer) PublishPendingConfig() {
	l := p.r.log
	// Use the fold marker: a refused entry does not advance configChangeID but must stop being pending.
	applied := max(p.membership.configChangeID, p.configFoldedThrough)
	// The first publish classifies the recovered log above the applied
	// configuration; every later one finds pendingSeeded set.
	l.seedPendingAdds(applied, p.log())
	cur := p.pendingSnap.Load()
	if cur != nil && cur.version == l.pendingVersion && cur.configChangeID == applied &&
		(cur.committed == l.committed || len(l.pendingAdds) == 0) {
		return
	}
	// Forget entries at or below the applied configuration so the map stays bounded.
	l.prunePendingThrough(applied)
	view := l.pendingIdentities(applied)
	if view == nil {
		if cur == nil && l.pendingVersion == 0 {
			return
		}
		// Nothing pending, but the marks moved: publish an empty view so the
		// next call short-circuits on them.
		p.pendingSnap.Store(&pendingConfig{version: l.pendingVersion, configChangeID: applied, committed: l.committed})
		return
	}
	snap := &pendingConfig{version: l.pendingVersion, configChangeID: applied, committed: l.committed, adds: view.adds}
	if len(view.removed) > 0 {
		snap.removed = make(map[uint64]string, len(view.removed))
		snap.removedIdentities = make(map[string]uint64, len(view.removed))
		for id, identity := range view.removed {
			if identity == "" {
				identity = p.membership.identities[id]
			}
			snap.removed[id] = identity
			if identity != "" {
				snap.removedIdentities[identity] = id
			}
		}
	}
	p.pendingSnap.Store(snap)
}

// PendingRemoval reports whether a committed but unfolded RemoveNode removes replicaID or the holder of identity; safe for concurrent use.
func (p *Peer) PendingRemoval(replicaID uint64, identity string) bool {
	snap := p.pendingSnap.Load()
	if snap == nil {
		return false
	}
	if _, ok := snap.removed[replicaID]; ok {
		return true
	}
	if identity != "" {
		if _, ok := snap.removedIdentities[identity]; ok {
			return true
		}
	}
	return false
}

// PendingMemberIdentity returns the identity a pending config entry in this log commits for replicaID; safe for concurrent use.
func (p *Peer) PendingMemberIdentity(replicaID uint64) (string, bool) {
	snap := p.pendingSnap.Load()
	if snap == nil || snap.adds == nil {
		return "", false
	}
	identity, ok := snap.adds[replicaID]
	return identity, ok && identity != ""
}

// publishMembership stores an immutable snapshot of the membership for lock-free readers.
func (p *Peer) publishMembership() {
	snap := p.membership.snapshot()
	p.membershipSnap.Store(snap)
}

// SetProcessedIndex sets the processed marker for an on-disk SM before the first Tick, folds skipped config changes, and returns the clamped index and any fatal decode error.
func (p *Peer) SetProcessedIndex(index uint64) (uint64, error) {
	// Clamp to the committed index; the Host reads the clamp through Peer.AppliedClamp.
	if index > p.r.log.committed {
		p.appliedClamp = AppliedClamp{SMApplied: index, Committed: p.r.log.committed}
		index = p.r.log.committed
	}
	p.r.log.processed = index
	// An incomplete or undecodable fold arms the watermark; an undecodable entry is also returned.
	complete, err := p.replayConfigChangesTo(index)
	switch {
	case err == nil && complete:
		// The fold covered everything up to index, so a watermark at or below it
		// is resolved — the same free resolution processConfigChanges performs
		// when the commit pipeline reaches the watermark.
		if index >= p.staleConfigThrough {
			p.staleConfigThrough = 0
		}
	default:
		if index > p.staleConfigThrough {
			p.staleConfigThrough = index
		}
		// Recorded, not logged, for the same reason as the clamp above: the
		// Host reads it through Peer.UnfoldedConfig and logs it on its own
		// logger.
		p.unfoldedConfig = UnfoldedConfig{
			Index:         index,
			FoldedThrough: p.configFoldedThrough,
			StaleThrough:  p.staleConfigThrough,
			Err:           err,
			recorded:      true,
		}
	}
	// The watermark is armed either way before this returns, so a caller that
	// chooses to ignore the error still gets the old fail-closed campaigning
	// behaviour rather than nothing.
	return index, err
}

// replayConfigChangesTo folds committed config changes at or below index, reporting whether it reached index and any fatal error.
func (p *Peer) replayConfigChangesTo(index uint64) (bool, error) {
	return p.replayConfigChangesFrom(p.r.log.firstIndex(), index)
}

// replayConfigChangesFrom folds committed config changes in [low, index] and reports whether it reached index; a compacted floor counts as reached.
func (p *Peer) replayConfigChangesFrom(low, index uint64) (bool, error) {
	if first := p.r.log.firstIndex(); low < first {
		low = first
	}
	if low > index {
		return true, nil
	}
	// Highest index offered to applyMembershipEntry, starting one below the scan.
	folded := low - 1
	changed := false
	complete := true
	var fatal error
	const chunk uint64 = 1024
	for low <= index {
		high := low + chunk
		if high > index+1 {
			high = index + 1
		}
		// Bound each read in bytes as well as entries.
		entries, err := p.r.log.entries(low, high, p.r.maxReplicationPayload)
		if err != nil || len(entries) == 0 {
			// Compacted: the snapshot membership covers the rest, so the replay is complete.
			complete = errors.Is(err, logdb.ErrCompacted)
			if complete {
				folded = index
			}
			slog.Debug("config change replay stopped",
				"shard", p.shardID,
				"replica", p.replicaID,
				"low", low,
				"complete", complete,
				"error", err,
			)
			break
		}
		for i := range entries {
			// applyMembershipEntry folds the bootstrap entry and every later config change.
			applied, err := p.applyMembershipEntry(entries[i])
			if applied {
				changed = true
			}
			if err != nil {
				// Stop at the corrupt entry, not past it: everything above it is
				// defined relative to a membership this replica cannot build.
				fatal = err
				complete = false
				break
			}
			folded = entries[i].Index
		}
		if fatal != nil {
			break
		}
		low = entries[len(entries)-1].Index + 1
	}
	if changed {
		p.publishMembership()
		// First fault wins: a decode failure already recorded in fatal
		// describes the entry that stopped the replay, which is more specific
		// than the configuration it left behind.
		if err := p.syncRaftRemotes(); err != nil && fatal == nil {
			fatal = err
		}
		p.clearJoiningIfKnown()
	}
	if folded > p.configFoldedThrough {
		p.configFoldedThrough = folded
	}
	return complete, fatal
}

// SetOnCampaign sets the callback that fires when the raft state machine
// starts an election. Must be called before the first Tick.
func (p *Peer) SetOnCampaign(fn func(shardID, replicaID, term uint64)) {
	p.r.onCampaign = fn
}

// SetTickInterval sets the real time per Tick, used for the read lease and LeaseRead vote hold-down; call before the first Tick.
func (p *Peer) SetTickInterval(d time.Duration) {
	p.r.setTickInterval(d)
}

// SetLogger sets the diagnostics logger (nil for the slog default) before the Peer is handed to a worker.
func (p *Peer) SetLogger(l *slog.Logger) {
	p.logger = l
}

// log returns the logger set through SetLogger, or the process slog default.
func (p *Peer) log() *slog.Logger {
	if p.logger != nil {
		return p.logger
	}
	return slog.Default()
}

// Tick advances the logical clock by one tick; a joining node ticks normally and is kept from campaigning by the raft joining gate.
func (p *Peer) Tick() {
	// Ends a post-restart vote hold-down on time even on a replica that sees
	// no vote traffic, so the owed window it lowers reaches the hard state
	// (voteHeldDown). A no-op once the hold-down is over.
	p.r.voteHeldDown()
	p.r.tickFunc(p.r)
	p.syncLeaderID()
}

// DrainMessages returns pending outbound messages without a GetUpdate cycle; the caller must hold raftMu.
func (p *Peer) DrainMessages() []proto.Message {
	return p.r.drainMessages()
}

// DrainFreeOrderMessages returns only free-order messages (and HeartbeatResp when safe), leaving ordered ones for GetUpdate; the caller must hold raftMu.
func (p *Peer) DrainFreeOrderMessages() []proto.Message {
	msgs := p.r.drainFreeOrderMessages(p.heartbeatRespFreeOrder())
	p.markStep()
	return msgs
}

// heartbeatRespFreeOrder reports whether a HeartbeatResp may leave before the WAL write: hard state equals durableState, the log is unmoved, nothing is unpersisted.
func (p *Peer) heartbeatRespFreeOrder() bool {
	r := p.r
	if p.stateUnpersisted || p.hasPendingSnapshot ||
		r.term != p.durableState.Term ||
		r.vote != p.durableState.Vote ||
		r.voteWindowOwed != p.durableState.VoteWindow ||
		r.log.lastIndex() != p.stepMarkIndex ||
		r.log.pendingSnapshot() != nil {
		return false
	}
	t, _ := r.log.lastTerm()
	return t == p.stepMarkTerm
}

// markStep records the log position a later heartbeatRespFreeOrder compares
// against: whatever the log holds now belongs to a step already drained.
func (p *Peer) markStep() {
	p.stepMarkIndex = p.r.log.lastIndex()
	p.stepMarkTerm, _ = p.r.log.lastTerm()
}

// releaseRefusedBindings forgets the bindings (BindConfigChange) of the
// config changes in a refused Propose: it appended nothing, so they will
// never be validated.
func (p *Peer) releaseRefusedBindings(msg proto.Message, err error) {
	if err != nil && msg.Type == proto.Propose {
		p.r.takeBoundConfigChanges(msg.Entries)
	}
}

// Handle routes an incoming Raft message to the state machine.
func (p *Peer) Handle(msg proto.Message) error {
	// A joining node must not campaign on TimeoutNow.
	if msg.Type == proto.TimeoutNow && p.joining.Load() {
		slog.Warn("ignoring TimeoutNow on a joining node with incomplete membership",
			"shard", p.shardID,
			"replica", p.replicaID,
			"from", msg.From,
		)
		return ErrJoiningNodeCannotCampaign
	}

	// The pending snapshot before this step, compared by identity so an older rejected snapshot cannot inherit a newer restore.
	if err := p.checkSnapshotWireVersion(msg); err != nil {
		return err
	}

	var pendingBefore *proto.Snapshot
	var termBefore uint64
	var committedBefore uint64
	if msg.Type == proto.InstallSnapshot {
		pendingBefore = p.r.log.pendingSnapshot()
		termBefore = p.r.term
		committedBefore = p.r.log.committed
	}

	err := p.r.step(msg)

	p.releaseRefusedBindings(msg, err)

	// Membership is adopted only from a snapshot raft acted on and that arrived through the local injection API; the origin stamp is not on the wire.
	wireOrigin := msg.Type == proto.InstallSnapshot && !msg.Origin.IsLocal()
	if wireOrigin {
		slog.Warn("ignoring membership from a wire-delivered InstallSnapshot",
			"shard", p.shardID,
			"replica", p.replicaID,
			"from", msg.From,
			"msg_term", msg.Term,
			"term", termBefore,
			"snapshot_index", msg.Snapshot.Index,
		)
	}

	// Defence in depth: a local snapshot message behind our term is stale; snapshotRejected below is the operative index check.
	staleTerm := msg.Type == proto.InstallSnapshot &&
		msg.Term > 0 && msg.Term < termBefore
	if staleTerm {
		slog.Warn("ignoring membership from a stale-term InstallSnapshot",
			"shard", p.shardID,
			"replica", p.replicaID,
			"from", msg.From,
			"msg_term", msg.Term,
			"term", termBefore,
			"snapshot_index", msg.Snapshot.Index,
		)
	}

	// Config changes take effect on commit, so truncation never removes one; adopt a snapshot's membership only if raft advanced committed to its index.
	snapshotProcessed := msg.Type == proto.InstallSnapshot &&
		p.r.log.committed > committedBefore
	if msg.Type == proto.InstallSnapshot && err == nil && !staleTerm && !wireOrigin &&
		snapshotProcessed {
		// Adopt on restore, on a strictly newer ConfigChangeID, or when joining; applied changes are always below ss.Index, and the replay guard handles the rest.
		ss := msg.Snapshot
		pending := p.r.log.pendingSnapshot()
		restored := pending != nil && pending != pendingBefore
		joining := p.joining.Load()
		if restored || joining || ss.Membership.ConfigChangeID > p.membership.configChangeID {
			p.membership = p.newMembership(ss.Membership)
			// The adopted membership covers the snapshot's index, so the fold marker moves with it.
			if ss.Index > p.configFoldedThrough {
				p.configFoldedThrough = ss.Index
			}
			// Publish immutable snapshot for concurrent readers.
			p.publishMembership()
			// Backstop: fold the over-cap error into err so the leader-ID publication still runs.
			if syncErr := p.syncRaftRemotes(); syncErr != nil && err == nil {
				err = syncErr
			}
			// Snapshot delivers the full cluster membership. A joining
			// node now has complete membership knowledge and can enforce
			// per-shard message authorization.
			p.setJoining(false)
		}
		if restored {
			// Mark pending snapshot for HasUpdate fast-path so the
			// engine processes the snapshot even if no other state
			// changed.
			p.hasPendingSnapshot = true
		}
	}

	p.syncLeaderID()
	return err
}

// Propose routes entries to the raft state machine and returns the last assigned index; a non-leader returns NotLeaderError.
func (p *Peer) Propose(entries []proto.Entry) (uint64, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	if !p.IsLeader() {
		return 0, &NotLeaderError{LeaderID: p.leaderID.Load()}
	}
	msg := proto.Message{
		Type:    proto.Propose,
		From:    p.replicaID,
		Entries: entries,
		Origin:  proto.OriginLocal,
	}
	err := p.r.step(msg)
	p.syncLeaderID()
	if err != nil {
		return 0, err
	}
	return p.r.log.lastIndex(), nil
}

// ProposeConfigChange proposes a membership change, which takes effect when the entry commits; at most one is uncommitted at a time.
func (p *Peer) ProposeConfigChange(cc proto.ConfigChange) error {
	if !p.IsLeader() {
		return &NotLeaderError{
			LeaderID: p.leaderID.Load(),
		}
	}

	// Validate before proposing so the caller gets the membership error; handleLeaderPropose runs the same validator.
	err := p.membership.validate(cc)
	if err != nil {
		return err
	}

	// Serialize the config change into an entry.
	buf := make([]byte, cc.Size())
	_, err = cc.MarshalTo(buf)
	if err != nil {
		return err
	}

	entry := proto.Entry{
		Type: proto.EntryConfigChange,
		Cmd:  buf,
	}
	msg := proto.Message{
		Type:    proto.Propose,
		From:    p.replicaID,
		Entries: []proto.Entry{entry},
		Origin:  proto.OriginLocal,
	}
	err = p.r.step(msg)
	if err != nil {
		return err
	}

	p.syncLeaderID()
	return nil
}

// BindConfigChange binds the entry with key to its ConfigChangeID even when 0; the first Propose carrying key consumes it. Safe for concurrent use.
func (p *Peer) BindConfigChange(key uint64) {
	if key != 0 {
		p.boundConfigChanges.Store(key, struct{}{})
	}
}

// UnbindConfigChange forgets a bound key whose entry was never delivered.
func (p *Peer) UnbindConfigChange(key uint64) {
	p.boundConfigChanges.Delete(key)
}

// takeBoundConfigChange reports whether key was bound and forgets it.
func (p *Peer) takeBoundConfigChange(key uint64) bool {
	_, ok := p.boundConfigChanges.LoadAndDelete(key)
	return ok
}

// validateProposedConfigChange validates against the step worker's mutable membership, which is never stale.
func (p *Peer) validateProposedConfigChange(cc proto.ConfigChange) error {
	return p.membership.validate(cc)
}

// ValidateConfigChange checks a change against the published membership without mutating, for the Host membership API; safe for concurrent use and may falsely reject under a concurrent commit.
func (p *Peer) ValidateConfigChange(cc proto.ConfigChange) error {
	var m *membership
	if snap := p.membershipSnap.Load(); snap == nil {
		m = p.newMembership(proto.Membership{})
	} else {
		m = p.newMembership(snap.toProto())
	}
	return m.validate(cc)
}

// newMembership builds a membership from replicated state under this peer's local admission policy.
func (p *Peer) newMembership(ms proto.Membership) *membership {
	m := newMembershipWithPolicy(ms, p.cfg.RequireMemberIdentities)
	m.wire = p.wireWindow()
	return m
}

// checkSnapshotWireVersion refuses an InstallSnapshot committing a wire version this build cannot read.
func (p *Peer) checkSnapshotWireVersion(msg proto.Message) error {
	if msg.Type != proto.InstallSnapshot {
		return nil
	}
	if v := msg.Snapshot.Membership.WireVersion; v != 0 && !p.wireWindow().Accepts(v) {
		return proto.ErrUnsupportedVersion
	}
	return nil
}

// wireWindow returns the window set through SetWireWindow, or the build's
// default.
func (p *Peer) wireWindow() proto.WireWindow {
	if p.wire == (proto.WireWindow{}) {
		return proto.DefaultWireWindow()
	}
	return p.wire
}

// SetWireWindow sets the readable wire versions before the Peer is handed to a worker; it returns proto.ErrUnsupportedVersion if the recovered membership is outside w.
func (p *Peer) SetWireWindow(w proto.WireWindow) error {
	p.wire = w
	p.membership.wire = p.wireWindow()
	p.r.wire = p.membership.wire
	if v := p.membership.wireVersion; v != 0 && !p.membership.wire.Accepts(v) {
		return proto.ErrUnsupportedVersion
	}
	return nil
}

// WireVersion returns the shard's committed wire version, with zero read as
// proto.PreviousWireVersion. It reads the published membership snapshot, so
// it is safe from any goroutine.
func (p *Peer) WireVersion() uint64 {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return proto.PreviousWireVersion
	}
	return proto.EffectiveWireVersion(snap.wireVersion)
}

// membershipVotingBitCount returns the ReadIndex ackBits positions the current membership needs: other voters and witnesses plus self.
func (p *Peer) membershipVotingBitCount() int {
	count := 1
	for id := range p.membership.addresses {
		if id != p.replicaID {
			count++
		}
	}
	for id := range p.membership.witnesses {
		if id != p.replicaID {
			count++
		}
	}
	return count
}

// syncRaftRemotes mirrors membership into the raft remote maps; it returns ErrTooManyVotingMembers before touching them, which fails the shard.
func (p *Peer) syncRaftRemotes() error {
	if n := p.membershipVotingBitCount(); n > maxVotingMembers {
		slog.Error("shard membership exceeds the ReadIndex ackBits word: failing the shard rather than mirroring a voting set that cannot be tracked",
			"shard", p.shardID,
			"replica", p.replicaID,
			"voting_members", n,
			"max_voting_members", maxVotingMembers,
			"voters", len(p.membership.addresses),
			"witnesses", len(p.membership.witnesses))
		return ErrTooManyVotingMembers
	}
	nextIdx := p.r.log.lastIndex() + 1

	// Add new remotes; a promoted observer keeps its remote and replication progress.
	for id := range p.membership.addresses {
		if id == p.replicaID {
			continue
		}
		if _, ok := p.r.remotes[id]; !ok {
			if promoted, wasObserver := p.r.observers[id]; wasObserver {
				p.r.remotes[id] = promoted
			} else {
				p.r.remotes[id] = newRemote(nextIdx, maxInflightMsgs)
			}
		}
	}
	// Remove departed remotes.
	for id := range p.r.remotes {
		if !p.membership.isVoter(id) {
			delete(p.r.remotes, id)
			// Clear a leader transfer to a departed node.
			if p.r.transferTarget == id {
				p.r.transferTarget = noNode
				p.r.transferElapsed = 0
				// Restore the lease tracking handleLeaderTransfer cleared,
				// as tickLeader's transfer-timeout path does.
				p.r.resumeLeaseAfterTransfer()
			}
		}
	}

	// Add new observers.
	for id := range p.membership.observers {
		if id == p.replicaID {
			continue
		}
		if _, ok := p.r.observers[id]; !ok {
			p.r.observers[id] = newRemote(nextIdx, maxInflightMsgs)
		}
	}
	// Remove departed observers.
	for id := range p.r.observers {
		if !p.membership.isObserver(id) {
			delete(p.r.observers, id)
		}
	}

	// Add new witnesses.
	for id := range p.membership.witnesses {
		if id == p.replicaID {
			continue
		}
		if _, ok := p.r.witnesses[id]; !ok {
			p.r.witnesses[id] = newRemote(nextIdx, maxInflightMsgs)
		}
	}
	// Remove departed witnesses.
	for id := range p.r.witnesses {
		if !p.membership.isWitness(id) {
			delete(p.r.witnesses, id)
		}
	}

	// Mirror the replicated witness policy into the raft layer so the
	// leader-side propose gate (validateConfigChangePolicy) enforces the
	// same policy as the membership validators.
	p.r.maxWitnesses = p.membership.maxWitnesses
	p.r.requireFullReplicaMajority = p.membership.requireFullReplicaMajority

	// A replica removed from the configuration must stop campaigning.
	p.r.setSelfMembership(p.membership.isMember(p.replicaID))

	// Cross-check the local witness and observer roles against the applied configuration.
	p.r.checkSelfRole(p.membership.isMember(p.replicaID),
		p.membership.isWitness(p.replicaID),
		p.membership.isObserver(p.replicaID))

	// Drop vote records of replicas that left, including a self vote before self-removal.
	p.r.purgeDepartedVotes()

	// When the node is leader, rebuild quorum and lease tracking maps
	// so that new voting members' heartbeat acks count correctly.
	if p.r.state == leader {
		if err := p.r.rebuildReplicaToBit(); err != nil {
			return err
		}
		p.syncLeaseAcks()
	}
	return nil
}

// syncLeaseAcks drops remoteLastAck entries for replicas that left the voting set; new members are not seeded.
func (p *Peer) syncLeaseAcks() {
	if p.r.remoteLastAck == nil {
		return
	}
	for id := range p.r.remoteLastAck {
		_, inRemotes := p.r.remotes[id]
		_, inWitnesses := p.r.witnesses[id]
		if !inRemotes && !inWitnesses {
			delete(p.r.remoteLastAck, id)
		}
	}
}

// checkLeaderSelfRemoval checks if the leader has been removed from the
// membership and steps down if so. Per Raft Section 6: "The leader steps
// down once Cnew is committed if it is not part of the new configuration."
func (p *Peer) checkLeaderSelfRemoval() {
	if p.r.state == leader && !p.membership.isMember(p.replicaID) {
		p.r.becomeFollower(p.r.term, noLeader)
		p.syncLeaderID()
	}
}

// applyMembershipEntry folds one committed initial-membership or config-change entry and reports whether membership changed; a decode failure is fatal ErrCorruptConfigEntry, a deterministic refusal is logged.
func (p *Peer) applyMembershipEntry(e proto.Entry) (bool, error) {
	switch e.Type & proto.EntryTypeMask {
	case proto.EntryInitialMembership:
		// The founding configuration is valid only at BootstrapMembershipIndex; elsewhere it is refused.
		if e.Index != BootstrapMembershipIndex {
			// A structural refusal, identical on every replica; it must not fault the shard.
			slog.Error("refusing initial-membership entry outside the bootstrap index",
				"shard_id", p.shardID,
				"replica_id", p.replicaID,
				"entry_index", e.Index,
				"bootstrap_index", BootstrapMembershipIndex,
			)
			return false, nil
		}
		ms, err := initialMembershipFromEntry(e)
		if err != nil {
			slog.Error("initial membership entry unmarshal failed",
				"shard_id", p.shardID,
				"replica_id", p.replicaID,
				"entry_index", e.Index,
				"error", err,
			)
			return false, ErrCorruptConfigEntry
		}
		p.membership = p.newMembership(ms)
		return true, nil
	case proto.EntryConfigChange:
		// A zero-length payload is a no-op on every replica.
		if len(e.Cmd) == 0 {
			return false, nil
		}
		var cc proto.ConfigChange
		if _, err := cc.UnmarshalFrom(e.Cmd); err != nil {
			slog.Error("config change unmarshal failed",
				"shard_id", p.shardID,
				"replica_id", p.replicaID,
				"entry_index", e.Index,
				"entry_type", e.Type,
				"error", err,
			)
			return false, ErrCorruptConfigEntry
		}
		if err := p.membership.apply(cc, e.Index); err != nil {
			// A committed wire version this build cannot read is fatal.
			if errors.Is(err, proto.ErrUnsupportedVersion) {
				slog.Error("committed wire version outside this build's window",
					"shard_id", p.shardID,
					"replica_id", p.replicaID,
					"entry_index", e.Index,
					"wire_version", cc.WireVersion,
				)
				return false, err
			}
			// A skip on the leader's own pre-validated entry is unexpected; log it at WARN.
			slog.Warn("config change apply skipped",
				"shard_id", p.shardID,
				"replica_id", p.replicaID,
				"entry_index", e.Index,
				"entry_type", e.Type,
				"error", err,
			)
			return false, nil
		}
		return true, nil
	default:
		return false, nil
	}
}

// processConfigChanges applies committed config changes, publishes the membership and syncs remotes; it returns the fatal error of an undecodable entry.
func (p *Peer) processConfigChanges(entries []proto.Entry) error {
	needsSync := false
	var fatal error
	for i := range entries {
		applied, err := p.applyMembershipEntry(entries[i])
		if applied {
			needsSync = true
		}
		if err != nil {
			fatal = err
			break
		}
		if entries[i].Index > p.configFoldedThrough {
			p.configFoldedThrough = entries[i].Index
		}
	}
	if fatal != nil {
		// Publish whatever was folded before the corrupt entry so the shard's
		// last coherent configuration is the one its readers see, then fail.
		if needsSync {
			p.publishMembership()
			// fatal already names the corrupt entry; log this one instead.
			if syncErr := p.syncRaftRemotes(); syncErr != nil {
				slog.Error("membership mirror not synced while failing the shard",
					"shard", p.shardID,
					"replica", p.replicaID,
					"error", syncErr)
			}
			p.clearJoiningIfKnown()
		}
		return fatal
	}
	// The commit pipeline reaching the staleness watermark resolves it, whether or not the tail held config changes.
	if n := len(entries); n > 0 && entries[n-1].Index >= p.staleConfigThrough {
		p.staleConfigThrough = 0
	}
	if needsSync {
		// Publish immutable snapshot for concurrent readers.
		p.publishMembership()
		// Terminal: failCommit marks the shard failed and fires OnShardFailed.
		if err := p.syncRaftRemotes(); err != nil {
			return err
		}
		// A committed membership beyond itself ends the joining window.
		p.clearJoiningIfKnown()
	}
	return nil
}

// ReadIndex requests a linearizable read at the current commit index.
func (p *Peer) ReadIndex(key uint64) error {
	msg := proto.Message{
		Type:   proto.ReadIndex,
		From:   p.replicaID,
		Hint:   key,
		Origin: proto.OriginLocal,
	}
	err := p.r.step(msg)
	p.syncLeaderID()
	return err
}

// FlushReadIndex sends one heartbeat round for all pending ReadIndex requests; call it after the inbox and before GetUpdate.
func (p *Peer) FlushReadIndex() {
	p.r.flushReadIndex()
}

// DrainReadStatesOnly returns pending readStates and leaves outbound messages for the next GetUpdate.
func (p *Peer) DrainReadStatesOnly() []proto.ReadyToRead {
	if len(p.r.readStates) == 0 {
		return nil
	}
	readStates := p.r.readStates
	p.r.readStates = nil
	return readStates
}

// DrainReadRejects returns and clears the keys of forwarded reads the leader refused; call it with Handle's exclusion.
func (p *Peer) DrainReadRejects() []uint64 {
	if len(p.r.readRejects) == 0 {
		return nil
	}
	keys := p.r.readRejects
	p.r.readRejects = nil
	return keys
}

// RequestLeaderTransfer asks the raft state machine to transfer leadership to target.
func (p *Peer) RequestLeaderTransfer(target uint64) error {
	if !p.IsLeader() {
		return nil
	}
	if target == p.replicaID {
		return nil
	}
	msg := proto.Message{
		Type:   proto.LeaderTransfer,
		From:   p.replicaID,
		Hint:   target,
		Origin: proto.OriginLocal,
	}
	err := p.r.step(msg)
	p.syncLeaderID()
	return err
}

// HasUpdate reports whether there is work for the engine, checking the cheapest conditions first.
func (p *Peer) HasUpdate(lastApplied uint64) bool {
	if len(p.r.msgs) > 0 {
		return true
	}
	if p.r.log.hasEntriesToApply() {
		return true
	}
	if len(p.r.readStates) > 0 {
		return true
	}
	if p.hasPendingSnapshot {
		return true
	}
	if p.hasStateChanged() {
		return true
	}
	if len(p.r.log.entriesToSave()) > 0 {
		return true
	}
	// Applied entries still in memory need an update to release them from MaxInMemLogSize.
	return p.appliedReleasePending(lastApplied)
}

// appliedReleasePending reports whether the in-memory log still holds
// entries at or below lastApplied that appliedLogTo would release.
func (p *Peer) appliedReleasePending(lastApplied uint64) bool {
	l := p.r.log
	if lastApplied == 0 || lastApplied > l.committed || lastApplied > l.processed {
		return false
	}
	// The same bounds appliedLogTo checks before it releases anything.
	im := &l.inmem
	if len(im.entries) == 0 || lastApplied < im.markerIndex {
		return false
	}
	return lastApplied <= im.entries[len(im.entries)-1].Index
}

// GetUpdate collects pending changes into an Update; committed entries above lastApplied are included only when moreEntriesToApply.
func (p *Peer) GetUpdate(moreEntriesToApply bool, lastApplied uint64) (proto.Update, error) {
	hs := p.r.hardState()

	// Detect a state change before updating prevState; stateUnpersisted forces one.
	stateChanged := p.stateUnpersisted ||
		hs != p.prevState

	update := proto.Update{
		ShardID:     p.shardID,
		ReplicaID:   p.replicaID,
		State:       hs,
		LastApplied: lastApplied,
	}

	// Collect entries to save (not yet persisted).
	entriesToSave := p.r.log.entriesToSave()
	if len(entriesToSave) > 0 {
		update.EntriesToSave = entriesToSave
		update.FirstIndex = entriesToSave[0].Index
		update.LastIndex = entriesToSave[len(entriesToSave)-1].Index
	}

	// Committed entries go in a pooled slice the apply worker releases.
	if moreEntriesToApply {
		committedEntries, pooled, err := p.r.log.entriesToApply()
		if err != nil {
			return proto.Update{}, err
		}
		if len(committedEntries) > 0 {
			update.CommittedEntries = committedEntries
			update.SetCommittedEntriesPooled(pooled)
		}
	}

	// Collect pending messages from raft, then drain.
	if len(p.r.msgs) > 0 {
		update.Messages = p.r.drainMessages()
	}

	// Collect read states.
	if len(p.r.readStates) > 0 {
		update.ReadyToRead = p.r.readStates
		p.r.readStates = nil
	}

	// Include only an in-memory pending snapshot, never the persisted one.
	ss := p.r.log.pendingSnapshot()
	if ss != nil {
		update.Snapshot = *ss
		p.hasPendingSnapshot = false
	}

	// An update with nothing to persist is read-only and skips the commit/apply pipeline.
	update.ReadOnly = !stateChanged &&
		len(update.EntriesToSave) == 0 &&
		len(update.CommittedEntries) == 0 &&
		update.Snapshot.Index == 0
	// A ReadOnly update is never committed, so Commit never releases the
	// applied entries it reports; release them here or HasUpdate would keep
	// returning true for them.
	if update.ReadOnly && p.appliedReleasePending(lastApplied) {
		p.r.log.inmem.appliedLogTo(lastApplied)
	}

	// Update prevState and leaderID; this non-read-only update satisfies the re-persist demand.
	p.prevState = hs
	p.stateUnpersisted = false
	p.markStep()
	p.syncLeaderID()

	return update, nil
}

// InvalidatePersistedState makes the next GetUpdate carry the hard state through SaveState; call with GetUpdate's exclusive access.
func (p *Peer) InvalidatePersistedState() {
	p.stateUnpersisted = true
}

// InvalidateLease ends this leader's LeaseRead lease; call with GetUpdate's exclusive access.
func (p *Peer) InvalidateLease() {
	p.r.invalidateLease()
}

// RejoinAfterPause makes a paused replica a follower at its term with a fresh timeout, keeping its vote; call with raftMu held and no worker stepping.
func (p *Peer) RejoinAfterPause() {
	leaderID := noLeader
	if p.r.state == follower {
		leaderID = p.r.leaderID
	}
	p.r.becomeFollower(p.r.term, leaderID)
	p.syncLeaderID()
}

// GetPiggybackUpdate returns committed entries up to the durable marker, the hard state and releasable messages without advancing prevState; the caller must hold raftMu.
func (p *Peer) GetPiggybackUpdate(lastApplied uint64) (proto.Update, error) {
	if !p.r.log.hasEntriesToApply() {
		return proto.Update{}, nil
	}
	// Apply and advertise only up to the durable marker.
	saved := p.r.log.savedIndex()
	committedEntries, pooled, err := p.r.log.entriesToApplyUpTo(saved)
	if err != nil {
		return proto.Update{}, err
	}
	if len(committedEntries) == 0 {
		return proto.Update{}, nil
	}
	hs := p.r.hardState()
	if hs.Commit > saved {
		hs.Commit = saved
	}
	update := proto.Update{
		ShardID:          p.shardID,
		ReplicaID:        p.replicaID,
		State:            hs,
		CommittedEntries: committedEntries,
		LastApplied:      lastApplied,
	}
	if pooled != nil {
		update.SetCommittedEntriesPooled(pooled)
	}
	// While entries are unsaved, release only free-order messages; ordered ones wait for the next GetUpdate's fsync.
	if len(p.r.msgs) > 0 {
		msgs := p.r.drainMessages()
		if len(p.r.log.entriesToSave()) > 0 {
			msgs = p.r.withholdOrderedMessages(msgs)
		}
		if len(msgs) > 0 {
			update.Messages = msgs
		}
		p.markStep()
	}
	p.syncLeaderID()
	return update, nil
}

// NotePiggybackPersisted sets prevState to hs, the persisted piggyback hard state; the caller must hold raftMu.
func (p *Peer) NotePiggybackPersisted(hs proto.State) {
	p.prevState = hs
	p.durableState = hs
}

// RequeueMessages puts msgs back ahead of newer ones for the next persisting cycle; the caller must hold raftMu.
func (p *Peer) RequeueMessages(msgs []proto.Message) {
	p.r.requeueMessages(msgs)
}

// Commit acknowledges the persisted Update, advancing markers; it returns an error on a gap in the entry range.
func (p *Peer) Commit(update proto.Update) error {
	// Register durable state with the LogReader first, so a failure leaves the batch re-deliverable.
	if !update.Snapshot.IsEmpty() {
		// Without this the LogReader keeps its initial empty range and
		// subsequent Append calls for entries after the snapshot index
		// fail with ErrLogGapDetected.
		if err := p.logReader.ApplySnapshot(update.Snapshot); err != nil {
			return err
		}
	}
	if len(update.EntriesToSave) > 0 {
		// Keep term lookups for entries about to leave inmem.
		if err := p.logReader.Append(update.EntriesToSave); err != nil {
			return err
		}
	}

	// Advance the processed marker before commitUpdate, which asserts
	// that LastApplied <= processed. Everything from here on is
	// infallible, so the update is guaranteed to reach the apply stage.
	if len(update.CommittedEntries) > 0 {
		lastProcessed := update.CommittedEntries[len(update.CommittedEntries)-1].Index
		p.r.log.appliedTo(lastProcessed)

		// Apply committed config changes before checkLeaderSelfRemoval; an undecodable entry is terminal.
		if err := p.processConfigChanges(update.CommittedEntries); err != nil {
			return err
		}

		// A catching-up follower may see several committed config changes in one batch; handle each in order.
		for i := range update.CommittedEntries {
			if update.CommittedEntries[i].Type&proto.EntryTypeMask == proto.EntryConfigChange {
				p.r.pendingConfigChange = false
				// Per Raft Section 6: leader steps down once Cnew
				// is committed if it is not part of new config.
				p.checkLeaderSelfRemoval()
			}
		}
	}

	uc := proto.UpdateCommit{
		Processed: true,
	}

	// Record stable log entries.
	if len(update.EntriesToSave) > 0 {
		last := update.EntriesToSave[len(update.EntriesToSave)-1]
		uc.StableLogTo = last.Index
		uc.StableLogTerm = last.Term
	}

	// Record stable snapshot (registered with the LogReader above).
	if !update.Snapshot.IsEmpty() {
		uc.StableSnapshotTo = update.Snapshot.Index
	}

	// Truncate inmem only through the apply worker's confirmed lastApplied.
	uc.LastApplied = update.LastApplied

	// Record read states.
	if len(update.ReadyToRead) > 0 {
		uc.ReadyToRead = uint64(len(update.ReadyToRead))
	}

	p.r.log.commitUpdate(uc)

	// The engine calls Commit only after SaveState, so the update's hard
	// state is on disk now. A ReadOnly update bypassed SaveState and carries
	// nothing new.
	if !update.ReadOnly {
		p.durableState = update.State
	}

	// Persistence edge: re-evaluate the commit now that savedTo advanced, and broadcast if it moved.
	if p.r.state == leader && uc.StableLogTo > 0 {
		if p.r.tryCommit() {
			p.r.broadcastAppend()
		}
	}
	// The commit just above can be the first of the leader's term, which is
	// the moment it becomes read-ready; publish it now rather than at the
	// next step.
	p.syncLeaderID()
	return nil
}

// LeaderID returns the current leader's replica ID. Returns 0 if no
// leader is known. Safe for concurrent access.
func (p *Peer) LeaderID() uint64 {
	return p.leaderID.Load()
}

// IsLeader returns true if this peer believes it is the leader. Safe
// for concurrent access.
func (p *Peer) IsLeader() bool {
	return p.leaderID.Load() == p.replicaID && p.replicaID != 0
}

// IsFollower returns true if this peer is a follower (not the leader and
// not an observer or witness).
func (p *Peer) IsFollower() bool {
	return !p.IsLeader() && !p.IsObserver() && !p.cfg.IsWitness
}

// IsObserver reports whether this peer is currently an observer per the committed membership; safe for concurrent use.
func (p *Peer) IsObserver() bool {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return p.cfg.IsObserver
	}
	return snap.isObserverOr(p.replicaID, p.cfg.IsObserver)
}

// IsWitness returns true if this peer is configured as a witness.
func (p *Peer) IsWitness() bool {
	return p.cfg.IsWitness
}

// GetMembership returns a copy of the current membership. Safe for
// concurrent access from any goroutine. Reads from an atomic pointer
// to an immutable membership snapshot.
func (p *Peer) GetMembership() proto.Membership {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return proto.Membership{}
	}
	return snap.toProto()
}

// MarkJoining marks this peer's configuration as incomplete before the step worker starts, mirroring the flag into raft.
func (p *Peer) MarkJoining() {
	p.setJoining(true)
	// join=true is a startup flag, so test at once whether this replica is joining at all.
	p.clearJoiningIfKnown()
}

// setJoining writes both the atomic Peer flag and the raft mirror; it is their only writer.
func (p *Peer) setJoining(joining bool) {
	p.joining.Store(joining)
	p.r.setJoining(joining)
}

// clearJoiningIfKnown ends the joining window when the followed leader is a member, self is a member alongside another, or self carries a Removed tombstone.
func (p *Peer) clearJoiningIfKnown() {
	if !p.joining.Load() {
		return
	}
	leaderID := p.r.leaderID
	if (leaderID != noLeader && p.membership.isMember(leaderID)) ||
		(p.membership.isMember(p.replicaID) && p.membership.hasMemberOtherThan(p.replicaID)) ||
		p.membership.isRemoved(p.replicaID) {
		p.setJoining(false)
	}
}

// ReleaseGlobalRateLimit returns this shard's share of the host-wide in-memory limiter; call it once at teardown. It is idempotent.
func (p *Peer) ReleaseGlobalRateLimit() {
	p.r.log.inmem.setGlobalSize(0)
}

// IsMember reports whether replicaID is an active member; it is not an authorization check (see IsRemoved). Safe for concurrent use.
func (p *Peer) IsMember(replicaID uint64) bool {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return false
	}
	return snap.isMember(replicaID)
}

// MemberBinding is what the committed configuration authorizes one replica ID with; Identity supersedes Address.
type MemberBinding struct {
	// Identity is the committed node identity derived from the public key, or "" when none is held.
	Identity string
	// Address is the committed network address. It remains the authorization
	// input only for a replica with no committed Identity, and is otherwise
	// pure routing metadata.
	Address string
	// Exclusive reports whether Address identifies this replica and no other active member.
	Exclusive bool
}

// MemberBinding returns what the committed configuration authorizes replicaID with and whether it is a member, from one snapshot load without allocating. Safe for concurrent use.
func (p *Peer) MemberBinding(replicaID uint64) (MemberBinding, bool) {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return MemberBinding{}, false
	}
	addr, found := snap.memberAddress(replicaID)
	if !found {
		return MemberBinding{}, false
	}
	return MemberBinding{
		Identity:  snap.memberIdentity(replicaID),
		Address:   addr,
		Exclusive: snap.addressIsExclusive(addr),
	}, true
}

// IsJoining reports whether this replica's configuration is known to be incomplete. Safe for concurrent use.
func (p *Peer) IsJoining() bool {
	return p.joining.Load()
}

// IsRemoved reports whether replicaID has a committed Removed tombstone; the monotone removed set is the inbound deny-list. Safe for concurrent use.
func (p *Peer) IsRemoved(replicaID uint64) bool {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return false
	}
	return snap.isRemoved(replicaID)
}

// IsRemovedIdentity reports whether the authenticated connection identity has a committed removal; the empty identity is never removed. Safe for concurrent use.
func (p *Peer) IsRemovedIdentity(identity string) bool {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return false
	}
	return snap.isRemovedIdentity(identity)
}

// IdentityHolder reports the replica the committed configuration binds identity to, if any. Safe for concurrent use.
func (p *Peer) IdentityHolder(identity string) (uint64, bool) {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return 0, false
	}
	return snap.identityHolder(identity)
}

// DiagRaftState returns term, committed, processed, lastIndex, pending messages and a state string that also flags self-removed and unreadable-log-denials=N.
func (p *Peer) DiagRaftState() (term, committed, processed, lastIdx uint64, msgs int, state string) {
	r := p.r
	var stateStr string
	switch r.state {
	case leader:
		stateStr = fmt.Sprintf("leader(hb=%d et=%d)", r.heartbeatTick, r.electionTick)
	case follower:
		stateStr = fmt.Sprintf("follower(et=%d)", r.electionTick)
	case candidate:
		stateStr = "candidate"
	case preCandidate:
		stateStr = "preCandidate"
	default:
		stateStr = "unknown"
	}
	if r.selfRemoved {
		stateStr += " self-removed"
	}
	if r.unreadableLogVoteDenials > 0 {
		stateStr += fmt.Sprintf(" unreadable-log-denials=%d", r.unreadableLogVoteDenials)
	}
	return r.term, r.log.committed, r.log.processed, r.log.lastIndex(), len(r.msgs), stateStr
}

// DiagRemotes returns diagnostic remote state for debugging commit stalls.
// Returns a string describing each remote's match/next/state/paused.
func (p *Peer) DiagRemotes() string {
	progress := p.RemoteProgress()
	if progress == nil {
		return "not leader"
	}
	result := ""
	for i := range progress {
		if result != "" {
			result += " "
		}
		rp := &progress[i]
		result += fmt.Sprintf("r%d[role=%s match=%d next=%d state=%s paused=%v active=%v inflights=%d]",
			rp.ReplicaID, rp.Role, rp.Match, rp.Next, rp.State, rp.Paused, rp.Active, rp.Inflights)
	}
	return result
}

// RemoteProgress returns the leader's view of every remote's replication, tagged by Role, or nil when not leader; the caller must hold raftMu.
func (p *Peer) RemoteProgress() []RemoteProgress {
	r := p.r
	if r.state != leader {
		return nil
	}
	out := make([]RemoteProgress, 0, len(r.remotes)+len(r.observers)+len(r.witnesses))
	add := func(set map[uint64]*remote, role Role) {
		for id, rm := range set {
			state := "unknown"
			switch rm.state {
			case remoteProbe:
				state = "probe"
			case remoteReplicate:
				state = "replicate"
			case remoteSnapshot:
				state = "snapshot"
			}
			out = append(out, RemoteProgress{
				ReplicaID:  id,
				Role:       role,
				Match:      rm.match,
				Next:       rm.next,
				State:      state,
				Paused:     rm.isPaused(),
				Active:     rm.isActive(),
				Inflights:  rm.inflights.Count(),
				IsWitness:  role == RoleWitness,
				IsObserver: role == RoleObserver,
			})
		}
	}
	add(r.remotes, RoleVoter)
	add(r.observers, RoleObserver)
	add(r.witnesses, RoleWitness)
	// Deterministic order: map iteration would otherwise reorder the output on
	// every call, which makes both the diagnostic string and any consumer
	// diffing successive samples useless.
	sort.Slice(out, func(i, j int) bool { return out[i].ReplicaID < out[j].ReplicaID })
	return out
}

// Role is a remote's membership role as reported by RemoteProgress. The set is
// closed: RoleVoter, RoleObserver and RoleWitness.
type Role string

// Membership roles reported by RemoteProgress.
const (
	// RoleVoter is a full voting member.
	RoleVoter Role = "voter"
	// RoleObserver is a non-voting member (learner).
	RoleObserver Role = "observer"
	// RoleWitness is a voting member that carries no payload.
	RoleWitness Role = "witness"
)

// RemoteProgress is one follower's replication position as the leader believes it.
type RemoteProgress struct {
	// ReplicaID identifies the follower.
	ReplicaID uint64
	// Role is RoleVoter, RoleObserver or RoleWitness.
	Role Role
	// Match is the highest log index known to be replicated to it.
	Match uint64
	// Next is the next index the leader will send.
	Next uint64
	// State is the replication state: probe, replicate or snapshot.
	State string
	// Paused is true when the leader is withholding traffic — a full
	// inflight window, or a probe awaiting its response.
	Paused bool
	// Active is true if the follower has responded within the election
	// timeout. An inactive follower still counts in the configuration but
	// not toward a live quorum.
	Active bool
	// Inflights is the number of unacknowledged messages in the window.
	// A follower pinned at the window limit is flow-controlled, not slow.
	Inflights int
	// IsWitness reports a voting member that carries no payload.
	IsWitness bool
	// IsObserver reports a non-voting member.
	IsObserver bool
}

// hasStateChanged reports whether term, vote, commit or the owed vote window changed since the last GetUpdate, or a re-persist is owed.
func (p *Peer) hasStateChanged() bool {
	return p.stateUnpersisted ||
		p.r.term != p.prevState.Term ||
		p.r.vote != p.prevState.Vote ||
		p.r.log.committed != p.prevState.Commit ||
		p.r.voteWindowOwed != p.prevState.VoteWindow
}

// RateLimited returns true if the per-shard in-memory log rate limit
// has been exceeded.
func (p *Peer) RateLimited() bool {
	return p.r.rateLimited()
}

// RateLimiter returns the per-shard in-memory rate limiter, or nil if
// rate limiting is disabled.
func (p *Peer) RateLimiter() *server.InMemRateLimiter {
	return p.r.rl
}

// AppliedToTerm returns the cached term of the last applied entry, valid after compaction.
func (p *Peer) AppliedToTerm() uint64 {
	return p.r.log.inmem.appliedToTerm.Load()
}

// CommittedIndex returns the current committed log index from the Raft log.
// Must only be called from the step worker goroutine (single-owner invariant).
func (p *Peer) CommittedIndex() uint64 {
	return p.r.log.committed
}

// LogMarkers returns the committed and processed markers as a consistent pair; call it with Commit's exclusive access.
func (p *Peer) LogMarkers() (committed, processed uint64) {
	return p.r.log.committed, p.r.log.processed
}

// RecoveryClamp returns the clamp NewPeer applied to a recovered commit index ahead of the log; set once at construction.
func (p *Peer) RecoveryClamp() RecoveryClamp {
	return p.r.log.recoveryClamp
}

// AppliedClamp describes SetProcessedIndex clamping a state machine applied
// index down to the recovered commit index. The zero value means no clamp was
// applied.
type AppliedClamp struct {
	// SMApplied is the applied index the state machine reported.
	SMApplied uint64
	// Committed is the recovered commit index, and the value the applied
	// index was clamped to.
	Committed uint64
}

// Applied reports whether a clamp was applied.
func (c AppliedClamp) Applied() bool {
	return c.SMApplied > c.Committed
}

// AppliedClamp returns the clamp SetProcessedIndex applied to an applied index ahead of the commit index.
func (p *Peer) AppliedClamp() AppliedClamp {
	return p.appliedClamp
}

// UnfoldedConfig describes an incomplete config fold in SetProcessedIndex; the zero value means complete.
type UnfoldedConfig struct {
	// Index is the applied index the fold had to reach.
	Index uint64
	// FoldedThrough is the index the fold did reach.
	FoldedThrough uint64
	// StaleThrough is the staleness watermark the failure armed.
	StaleThrough uint64
	// Err is the fold's error: nil for a window that could not be read,
	// ErrCorruptConfigEntry for one that will never decode.
	Err error

	recorded bool
}

// Recorded reports whether an incomplete fold was recorded.
func (c UnfoldedConfig) Recorded() bool {
	return c.recorded
}

// UnfoldedConfig returns the incomplete fold SetProcessedIndex recorded, if any.
func (p *Peer) UnfoldedConfig() UnfoldedConfig {
	return p.unfoldedConfig
}

// syncLeaderID copies raft's leaderID to the atomic field on Peer for concurrent readers.
func (p *Peer) syncLeaderID() {
	ready := p.computeReadReady()
	// Odd for the duration of the update, even once settled: see leaderSeq.
	p.leaderSeq.Add(1)
	p.leaderID.Store(p.r.leaderID)
	p.term.Store(p.r.term)
	p.readReady.Store(ready)
	p.leaderSeq.Add(1)
}

// computeReadReady reports whether the current term's leader has committed an entry of that term, which a follower can infer from its own log.
func (p *Peer) computeReadReady() bool {
	r := p.r
	if r.leaderID == noLeader {
		return false
	}
	if p.readyTerm == r.term {
		return true
	}
	if p.readyProbeTerm == r.term && p.readyProbeCommit == r.log.committed {
		return false
	}
	p.readyProbeTerm, p.readyProbeCommit = r.term, r.log.committed
	if !r.hasCommittedEntryAtCurrentTerm() {
		return false
	}
	p.readyTerm = r.term
	return true
}

// LeaderAndTerm returns the leader ID and term as a consistent pair. Safe for concurrent use.
func (p *Peer) LeaderAndTerm() (uint64, uint64) {
	leaderID, term, _ := p.LeaderTermReady()
	return leaderID, term
}

// LeaderTermReady returns the leader ID, term and read-readiness as a consistent triple. Safe for concurrent use.
func (p *Peer) LeaderTermReady() (leaderID, term uint64, readReady bool) {
	for {
		before := p.leaderSeq.Load()
		if before&1 != 0 {
			// A write is in flight; its window is three stores.
			continue
		}
		leaderID = p.leaderID.Load()
		term = p.term.Load()
		readReady = p.readReady.Load()
		if p.leaderSeq.Load() == before {
			return leaderID, term, readReady
		}
	}
}

// Term returns the current raft term. Safe for concurrent access; kept in
// step with LeaderID by syncLeaderID.
func (p *Peer) Term() uint64 {
	return p.term.Load()
}
