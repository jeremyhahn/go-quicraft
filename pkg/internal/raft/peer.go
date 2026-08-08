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
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/server"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Peer is the protocol boundary between the engine and the Raft state
// machine. It wraps the unexported raft struct and provides the API that
// the engine's step/commit/apply pipeline operates on.
//
// Peer is NOT safe for concurrent access. It is owned by exactly one
// goroutine at a time, enforced by the engine's worker pipeline. The
// exceptions are the atomically accessed leaderID field (read by
// LeaderID, IsLeader) and the membershipSnap atomic pointer (read by
// IsMember, GetMembership) which can be read concurrently from any
// goroutine.
//
// The wrapped raft struct is a pure state machine: messages in, messages
// out. Peer delegates all protocol operations (elections, heartbeats,
// log replication, read index, leader transfer, quiesce) to raft.
//
// Lifecycle:
//
//	Step Worker -> Tick/Handle/Propose/GetUpdate -> sends Update via channel
//	Commit Worker -> Commit -> ownership returns to Step Worker
type Peer struct {
	// r is the core Raft state machine. All protocol operations are
	// delegated to it. It is not safe for concurrent access; the engine
	// pipeline enforces single-owner semantics.
	r *raft

	// cfg is the per-shard configuration, retained for role queries
	// (IsObserver, IsWitness) and shard/replica identification.
	cfg config.Config

	// logReader is the persistent log reader, retained for reference.
	logReader LogReader

	// membership tracks the cluster membership. It is updated when
	// config change entries are proposed or when snapshots are applied.
	// This is the mutable working copy, owned exclusively by the step
	// worker goroutine.
	membership *membership

	// membershipSnap is an atomic pointer to an immutable snapshot of
	// the membership. It is published after every mutation to membership
	// and read lock-free by the transport recv goroutine (IsMember) and
	// host query methods (GetMembership). This eliminates the data race
	// between the step worker writing membership maps and the transport
	// recv goroutine reading them for per-shard authorization.
	membershipSnap atomic.Pointer[membershipSnapshot]

	// prevState records the hard state from the last GetUpdate call.
	// Used by hasStateChanged to detect whether the engine needs to
	// persist new hard state.
	prevState proto.State

	// leaderID is accessed atomically for concurrent query safety.
	// Written by syncLeaderID (called after step/tick), read by any
	// goroutine via LeaderID() and IsLeader(). This is the only field
	// on Peer that is safe for concurrent reads; all other state is
	// accessed exclusively by the owning worker goroutine.
	leaderID atomic.Uint64

	// term mirrors r.term for the same reason leaderID mirrors r.leaderID:
	// callers outside the step worker need the term that accompanies a
	// leadership change, and the raft struct is not thread-safe.
	term atomic.Uint64

	// shardID and replicaID identify this peer. Cached from cfg for
	// fast access without going through raft.
	shardID   uint64
	replicaID uint64

	// hasPendingSnapshot tracks whether a snapshot is waiting to be
	// persisted. Set when raft restores a snapshot, cleared after
	// GetUpdate returns it.
	hasPendingSnapshot bool

	// joining is true for nodes that were started with join=true. These nodes
	// have incomplete membership until they learn the configuration, so per-shard
	// auth is bypassed while joining is true. Under config-in-log it is cleared
	// deterministically by clearJoiningIfKnown the moment the node applies the
	// bootstrap membership entry (replicated like any other entry) or any config
	// change that names a member other than itself; a snapshot adopt also clears
	// it, for the compacted case.
	joining atomic.Bool
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
		membership: newMembership(ms),
		prevState:  r.hardState(),
		shardID:    cfg.ShardID,
		replicaID:  cfg.ReplicaID,
	}
	p.publishMembership()
	p.syncLeaderID()
	return p, nil
}

// publishMembership creates an immutable snapshot of the current mutable
// membership and stores it via atomic pointer. This makes the membership
// visible to concurrent readers (transport recv goroutine for IsMember,
// host for GetMembership) without any locks. Called after every mutation
// to the mutable membership (config changes, snapshot restore).
func (p *Peer) publishMembership() {
	snap := p.membership.snapshot()
	p.membershipSnap.Store(snap)
}

// SetProcessedIndex overrides the processed marker on the raft log.
// This is used by the host layer after Open() for on-disk state machines
// that report a last-applied index higher than the snapshot index. Without
// this, the engine would re-apply entries that the on-disk SM has already
// persisted, which is wasteful for large logs.
//
// Skipping those entries would also skip their MEMBERSHIP effect, because
// commit-time semantics apply config changes only from CommittedEntries
// (Peer.Commit -> processConfigChanges). Any committed config change in the
// skipped window therefore has to be folded in here, or this replica would
// come up computing quorum over a stale voting set. The scan is idempotent:
// membership.apply skips anything at or below the current configChangeID.
//
// Must be called before the first Tick.
// SetProcessedIndex returns the index actually applied after clamping, so the
// caller can seed ITS OWN markers from the same value. The engine's lastApplied
// and raft's processed must not diverge: a later Update carrying an unclamped
// LastApplied would trip commitUpdate's assertion and kill the commit worker.
func (p *Peer) SetProcessedIndex(index uint64) uint64 {
	// The state machine marker is durable, but it is not authority over
	// what COMMITTED. An on-disk SM whose lastApplied outruns the recovered
	// HardState.Commit (a lost state fsync — the engine acknowledges that
	// hazard) would otherwise fold the uncommitted tail's config changes
	// into the membership permanently, leaving this replica computing
	// quorum over a voting set no other replica shares, and would push
	// l.processed above l.committed so the first Commit trips
	// commitUpdate's LastApplied assertions on every restart.
	if index > p.r.log.committed {
		slog.Warn("state machine applied index exceeds recovered commit index, clamping",
			"shard", p.shardID,
			"replica", p.replicaID,
			"sm_applied", index,
			"committed", p.r.log.committed,
		)
		index = p.r.log.committed
	}
	p.r.log.processed = index
	p.replayConfigChangesTo(index)
	return index
}

// replayConfigChangesTo folds committed config changes at or below index into
// the membership. Entries already covered by the recovered snapshot membership
// are skipped by the replay guard, so only the (snapshotIndex, index] window
// has any effect. index must not exceed p.r.log.committed; callers clamp it.
func (p *Peer) replayConfigChangesTo(index uint64) {
	first := p.r.log.firstIndex()
	if first > index {
		return
	}
	changed := false
	const chunk uint64 = 1024
	for low := first; low <= index; {
		high := low + chunk
		if high > index+1 {
			high = index + 1
		}
		// Bound each read in BYTES as well as in entries. A 1024-entry chunk of
		// arbitrary log entries — config changes are interspersed with ordinary
		// data entries — is otherwise unbounded (maxSize 0), up to
		// 1024×MaxApplyEntrySize per read at startup. limitEntrySize always
		// returns at least one entry and the loop advances by the last index
		// read, so the byte bound cannot stall progress even on a lone oversized
		// entry.
		entries, err := p.r.log.entries(low, high, p.r.maxReplicationPayload)
		if err != nil || len(entries) == 0 {
			// Compacted or unreadable: the snapshot membership already
			// covers everything below the compaction floor.
			slog.Debug("config change replay stopped",
				"shard", p.shardID,
				"replica", p.replicaID,
				"low", low,
				"error", err,
			)
			break
		}
		for i := range entries {
			// applyMembershipEntry folds both the bootstrap EntryInitialMembership
			// at index 1 (which is the first entry replayed, since the loop starts
			// at firstIndex) and every incremental EntryConfigChange above it. It
			// converges on the committed role and logs any deterministic refusal.
			if p.applyMembershipEntry(entries[i]) {
				changed = true
			}
		}
		low = entries[len(entries)-1].Index + 1
	}
	if changed {
		p.publishMembership()
		p.syncRaftRemotes()
		p.clearJoiningIfKnown()
	}
}

// SetOnCampaign sets the callback that fires when the raft state machine
// starts an election. Must be called before the first Tick.
func (p *Peer) SetOnCampaign(fn func(shardID, replicaID, term uint64)) {
	p.r.onCampaign = fn
}

// Tick advances the peer's logical clock by one tick. The engine calls
// this at a regular interval (RTTMillisecond). The raft state machine
// uses ticks to drive election timeouts (followers/candidates) and
// heartbeat intervals (leaders).
//
// A joining node TICKS LIKE ANY OTHER REPLICA. It is kept passive by the
// raft-layer joining gate (see the raft.joining field), which suppresses the
// campaign itself in startElection and handleTimeoutNow — not by freezing the
// clock here.
//
// Freezing the clock was the original suppression and it was an availability
// bug. r.electionTick stayed at 0 while r.leaderID kept naming whichever leader
// last contacted the node, so `leaderID != noLeader && electionTick <
// electionTimeout` was permanently true: handleVoteRequest denied every
// PreVote and dropHighTermVote discarded every higher-term RequestVote without
// a response. Because the joining flag is cleared only by an InstallSnapshot,
// a node that caught up by pure log replay (SnapshotEntries defaults to 0, so
// the leader never compacts and never sends a snapshot) stayed in that state
// for the life of the process — and a three-voter shard that lost its leader
// could not elect a new one despite having a healthy quorum.
func (p *Peer) Tick() {
	p.r.tickFunc(p.r)
	p.syncLeaderID()
}

// DrainMessages extracts and returns all pending outbound messages
// without performing a full GetUpdate cycle. This is used by the step
// worker to flush heartbeat and vote messages produced by Tick() while
// commitPending is true. Without this, heartbeat messages would be
// delayed until the commit+apply pipeline completes, causing follower
// election timeouts and leader checkQuorum failures.
//
// The caller must hold raftMu (or otherwise ensure exclusive access).
func (p *Peer) DrainMessages() []proto.Message {
	return p.r.drainMessages()
}

// DrainFreeOrderMessages extracts only free-order messages (Replicate,
// Ping, Heartbeat, Quiesce) from the outbound buffer, leaving ordered
// messages (VoteResp, HeartbeatResp, ReplicateResp, etc.) in place for
// the next GetUpdate cycle. This is critical during commitPending: the
// step worker must send heartbeats for leader liveness, but ordered
// messages (e.g., VoteResp from processCommitPendingInbox) must not be
// drained until WAL persistence completes. Using DrainMessages() here
// would silently drop ordered messages, preventing elections from
// completing during commit+apply cycles.
//
// The caller must hold raftMu (or otherwise ensure exclusive access).
func (p *Peer) DrainFreeOrderMessages() []proto.Message {
	return p.r.drainFreeOrderMessages()
}

// Handle routes an incoming Raft protocol message to the raft state
// machine. The raft struct handles all message types: term-based
// transitions, vote requests/responses, append entries, heartbeats,
// snapshots, read index, leader transfer, quiesce, and more.
func (p *Peer) Handle(msg proto.Message) error {
	// A joining node must not campaign. Tick() suppresses its election
	// timer for this reason; TimeoutNow is the other campaign entry point
	// and goes straight to the raft layer. A joining node that caught up by
	// pure log replay (the leader never compacted, so no snapshot ever
	// arrived) holds only itself in its membership, so it would win with a
	// quorum of one and acknowledge writes the real cluster later discards.
	if msg.Type == proto.TimeoutNow && p.joining.Load() {
		slog.Warn("ignoring TimeoutNow on a joining node with incomplete membership",
			"shard", p.shardID,
			"replica", p.replicaID,
			"from", msg.From,
		)
		return ErrJoiningNodeCannotCampaign
	}

	// The snapshot the log held BEFORE this message was stepped. restore()
	// installs a fresh pending snapshot, so identity is what distinguishes
	// "THIS snapshot replaced the log" from "an earlier snapshot is still
	// waiting to be persisted". A merely non-nil pendingSnapshot() would let
	// a REJECTED (index at or below committed) retransmission of an older
	// snapshot inherit the newer one's restore below and roll membership
	// backwards. The pending snapshot is cleared only by the engine's
	// persistence ack, so it routinely outlives the message that installed it.
	var pendingBefore *proto.Snapshot
	var termBefore uint64
	if msg.Type == proto.InstallSnapshot {
		pendingBefore = p.r.log.pendingSnapshot()
		termBefore = p.r.term
	}

	err := p.r.step(msg)

	// raft.step DROPS a message whose term is behind ours and returns nil
	// (see the msg.Term < r.term branch in step). A nil error therefore does
	// NOT mean raft acted on the message, and membership must never be
	// adopted from a message raft did not act on: the snapshot's Membership
	// is the very set quorum() is computed over, and a stale-term
	// InstallSnapshot carries no term check, no log proof and no committed
	// entry behind it — only its own ConfigChangeID, which the sender
	// chooses. Any authenticated shard member could hand this node a
	// configuration of its choosing.
	//
	// An InstallSnapshot that did not arrive through the local injection API
	// is refused outright. raft.step already rejects it (ErrWireLocalMessage,
	// so err != nil and the adopt below is skipped), but the check is
	// repeated at the adopt site because THIS is the code that replaces the
	// membership: the property "only a snapshot this host received, verified
	// and persisted may redefine the voting set" has to be checkable here,
	// not inferred from a non-nil error produced three call frames away.
	//
	// The origin is stamped by the delivery API (Node.Deliver /
	// Node.DeliverLocal) and is not part of the wire format, so it is not a
	// value the sender can choose — unlike the term, which the previous
	// version of this check relied on and which a forged Term 0 walked
	// straight past.
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

	// A locally injected snapshot whose term is behind ours is stale: the
	// snapshot receive path can complete after this replica has moved on to a
	// later term, and its membership is then older than what the log already
	// applied.
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

	// Config changes take effect on COMMIT, not on log append: committed
	// EntryConfigChange entries are applied to membership in Peer.Commit
	// via processConfigChanges, on leaders and followers alike. Because
	// only committed entries mutate membership, log truncation on
	// conflict can never remove an applied config change, so no
	// membership rollback/rebuild is needed here.
	if msg.Type == proto.InstallSnapshot && err == nil && !staleTerm && !wireOrigin {
		// Adopt the snapshot's membership on BOTH branches, gated on it
		// not being older than what we already have.
		//
		// The matching-log branch also advances the processed marker to
		// the snapshot index, so config changes in (processed, ss.Index]
		// are never emitted as CommittedEntries and never reach
		// processConfigChanges — the ONLY place membership is applied
		// under commit-time semantics. Skipping the adoption there would
		// silently drop them, leaving this replica computing quorum over
		// a smaller voting set than the rest of the cluster.
		//
		// Adopting is safe because an applied config change always has
		// index <= processed <= committed < ss.Index (handleSnapshot
		// early-rejects ss.Index <= committed), so the snapshot's
		// membership can never be older than the local one. Config
		// changes RETAINED above ss.Index still apply normally: the
		// membership.apply replay guard skips anything at or below the
		// adopted ConfigChangeID and applies the rest.
		// A restore always adopts (the log was fully replaced). The
		// matching branch adopts only when the snapshot is STRICTLY newer,
		// so an equal ConfigChangeID (identical configuration) or a
		// locally-newer one (config changes already applied above
		// ss.Index) leaves the local membership untouched.
		//
		// A JOINING node always adopts. Its membership is incomplete by
		// construction (the bootstrap confState is not in the log), and it
		// has usually already applied its own AddNode — so the local
		// ConfigChangeID EQUALS the snapshot's and the strictly-newer test
		// rejects the only message that can repair it, leaving joining set
		// forever and per-shard authorization bypassed for the shard.
		ss := msg.Snapshot
		pending := p.r.log.pendingSnapshot()
		restored := pending != nil && pending != pendingBefore
		joining := p.joining.Load()
		if restored || joining || ss.Membership.ConfigChangeID > p.membership.configChangeID {
			p.membership = newMembership(ss.Membership)
			// Publish immutable snapshot for concurrent readers.
			p.publishMembership()
			p.syncRaftRemotes()
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

// Propose proposes new entries to the Raft log. The entries are wrapped
// in a Propose message and routed through the raft state machine. Only
// the leader accepts proposals; followers either reject or forward.
//
// On success, returns the last log index assigned to the proposed entries.
// The caller can use this index to track the proposal through the
// commit/apply pipeline. On failure, returns 0 and the error.
//
// Only a leader can assign indices. A follower forwards the proposal and
// r.log.lastIndex() would then describe replicated entries the caller never
// proposed, so this returns NotLeaderError instead of a fabricated index.
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

// ProposeConfigChange proposes a membership change to the Raft log. The
// change is serialized as an EntryConfigChange entry and routed through
// the raft state machine as a Propose message.
//
// Membership takes effect when the entry COMMITS, not when it is
// appended (the etcd/dragonboat model, diverging from Raft thesis
// Section 4.1 append-time semantics). The commit-time application is in
// Peer.Commit via processConfigChanges. Applying at commit is safe
// because at most one config change is ever uncommitted at a time:
// handleLeaderPropose rejects a second change while one is pending, and
// becomeLeader re-arms pendingConfigChange from uncommitted inherited
// entries. Append-time application would let a just-added (possibly not
// yet running) member count toward the quorum required to commit its
// own addition, stalling commit and stepping the leader down.
func (p *Peer) ProposeConfigChange(cc proto.ConfigChange) error {
	if !p.IsLeader() {
		return &NotLeaderError{
			LeaderID: p.leaderID.Load(),
		}
	}

	// Validate the config change before proposing. The replicated witness
	// policy is part of the membership itself, so this path and the Host
	// API path enforce identical rules.
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

// ValidateConfigChange checks a proposed config change against the current
// published membership without mutating any state. It enforces the same
// safety rules as ProposeConfigChange's propose-time validation (the
// 64-voting-member cap, duplicate-add rejection, last-voter protection),
// returning the validation error if the change would be rejected.
//
// This is used by the Host membership API (RequestAddNode, RequestAddWitness,
// RequestRemoveNode, etc.), whose proposals are delivered through the engine
// inbox and reach membership.apply directly — a path that deliberately does
// NOT re-check the cap or the witness policy so that committed changes always
// replay deterministically.
// Pre-validating here gives the caller a prompt error instead of a hung
// RequestState (or, for a 65th voting member, an invariant panic in
// rebuildReplicaToBit).
//
// Safe for concurrent access from any goroutine: it validates against an
// immutable, atomically published membership snapshot rather than the
// step-worker-owned mutable membership. Like the Host's ConfigChangeID
// staleness check, this validation is conservative — a config change that
// commits concurrently may cause a benign false rejection, which the caller
// resolves by re-reading the membership and retrying.
func (p *Peer) ValidateConfigChange(cc proto.ConfigChange) error {
	var m *membership
	if snap := p.membershipSnap.Load(); snap == nil {
		m = newMembership(proto.Membership{})
	} else {
		m = newMembership(snap.toProto())
	}
	return m.validate(cc)
}

// syncRaftRemotes synchronizes the raft state machine's remote tracking
// maps from the peer's membership. This ensures the raft layer tracks
// the correct set of peers after config changes.
func (p *Peer) syncRaftRemotes() {
	nextIdx := p.r.log.lastIndex() + 1

	// Add new remotes. An observer being promoted to voter keeps its
	// existing remote struct: it carries live replication progress
	// (match/next) and possibly an in-flight snapshot transfer or ack
	// wait. Recreating it at next=lastIndex+1 would immediately probe a
	// potentially still-restoring node, drawing the compacted-hint
	// rejection and a redundant full snapshot re-send.
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
			// If the departed node was the leader transfer target, clear
			// the transfer state. Otherwise the cluster stays
			// write-unavailable (rejecting proposals and ReadIndex with
			// ErrTransferInProgress) until electionTimeout ticks elapse.
			if p.r.transferTarget == id {
				p.r.transferTarget = noNode
				p.r.transferElapsed = 0
				// Restore the lease tracking handleLeaderTransfer cleared,
				// mirroring tickLeader's transfer-timeout path. Without it
				// every ReadIndex degrades to a full heartbeat round-trip
				// for the rest of the term.
				if p.r.leaseRead && p.r.remoteLastAck == nil {
					p.r.remoteLastAck = make(map[uint64]int)
				}
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

	// Track whether this replica is still in the configuration. A removed
	// replica must stop campaigning: the surviving configuration computes
	// its quorum without it, so its self-counted vote could elect it leader
	// for a term the real configuration is also electing a leader in.
	p.r.setSelfMembership(p.membership.isMember(p.replicaID))

	// Cross-check the locally configured witness role against the replicated
	// configuration this call just applied. A committed AddWitness naming
	// this replica, or an AddNode promoting it out of the witness set, is the
	// point at which the two can start to disagree.
	p.r.checkSelfRole(p.membership.isMember(p.replicaID),
		p.membership.isWitness(p.replicaID))

	// Drop vote records for replicas that just left. The election quorum is
	// computed over the voting set this call just reshaped; a stale grant
	// from a departed replica would be tallied against the smaller quorum.
	// This now includes a self vote cast before a self-removal committed.
	p.r.purgeDepartedVotes()

	// When the node is leader, rebuild quorum and lease tracking maps
	// so that new voting members' heartbeat acks count correctly.
	if p.r.state == leader {
		p.r.rebuildReplicaToBit()
		p.syncLeaseAcks()
	}
}

// syncLeaseAcks drops remoteLastAck entries for replicas that left the
// voting set (remotes + witnesses).
//
// Newly added members are deliberately NOT seeded: absence from the map
// already means "never acked", and handleHeartbeatResp creates the key on
// the first real ack. A seeded tick of 0 is indistinguishable from an ack
// recorded at tick 0, and hasValidLease computes a negative expiry early in
// each checkQuorum cycle, so the seed would let a member that is not even
// running contribute to the read lease.
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

// applyMembershipEntry folds one committed entry into p.membership and reports
// whether it changed the membership. It handles the two entry types that carry
// configuration:
//
//   - EntryInitialMembership: the bootstrap entry at index 1. Its Cmd is the
//     full initial Membership, adopted wholesale (like handleSnapshot). It is
//     always the lowest committed index, so it is folded before any incremental
//     change, and re-adopting it on replay is idempotent.
//   - EntryConfigChange: an incremental add/remove/policy change.
//
// Any other entry type is ignored. A decode or apply error is logged and
// treated as "no change": a committed entry is never re-delivered, so the caller
// must not abort its scan on one. membership.apply CONVERGES on the role each
// committed add names rather than refusing an entry whose target already exists,
// so the only errors that reach here are the deterministic refusals every
// replica evaluates identically (a Removed tombstone re-add, or an add
// contradicting a role only a committed change could have set).
func (p *Peer) applyMembershipEntry(e proto.Entry) bool {
	switch e.Type & proto.EntryTypeMask {
	case proto.EntryInitialMembership:
		ms, err := initialMembershipFromEntry(e)
		if err != nil {
			slog.Error("initial membership entry unmarshal failed",
				"shard_id", p.shardID,
				"replica_id", p.replicaID,
				"entry_index", e.Index,
				"error", err,
			)
			return false
		}
		p.membership = newMembership(ms)
		return true
	case proto.EntryConfigChange:
		if len(e.Cmd) == 0 {
			return false
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
			return false
		}
		if err := p.membership.apply(cc, e.Index); err != nil {
			// The Host membership API pre-validates against current membership
			// (ValidateConfigChange) before proposing, so a skip on the leader's
			// own freshly proposed entry is unexpected and would mean the
			// caller's RequestState hangs until deadline. Log at WARN so any
			// skip is visible to operators rather than silently swallowed.
			slog.Warn("config change apply skipped",
				"shard_id", p.shardID,
				"replica_id", p.replicaID,
				"entry_index", e.Index,
				"entry_type", e.Type,
				"error", err,
			)
			return false
		}
		return true
	default:
		return false
	}
}

// processConfigChanges scans COMMITTED entries for config changes and
// applies them to the peer's membership, publishing the updated snapshot
// and syncing raft remote tracking. Called from Peer.Commit: membership
// takes effect at commit time (etcd/dragonboat model), not on log append.
func (p *Peer) processConfigChanges(entries []proto.Entry) {
	needsSync := false
	for i := range entries {
		if p.applyMembershipEntry(entries[i]) {
			needsSync = true
		}
	}
	if needsSync {
		// Publish immutable snapshot for concurrent readers.
		p.publishMembership()
		p.syncRaftRemotes()
		// A committed entry that gave this replica a membership beyond itself
		// ends the joining window: it now knows every member and can enforce
		// per-shard authorization. The bootstrap EntryInitialMembership is the
		// ordinary carrier of that on the replicate path.
		p.clearJoiningIfKnown()
	}
}

// ReadIndex requests a linearizable read at the current commit index.
// The request is wrapped in a ReadIndex message and routed through the
// raft state machine, which confirms leadership via heartbeat quorum
// (multi-node) or responds immediately (single-node).
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

// FlushReadIndex sends a single deferred heartbeat broadcast for all
// pending ReadIndex requests accumulated in the current step cycle.
// This batches multiple concurrent reads into one heartbeat round,
// reducing network messages from N*(cluster_size-1) to (cluster_size-1).
// Must be called after processing all inbox messages and before GetUpdate.
func (p *Peer) FlushReadIndex() {
	p.r.flushReadIndex()
}

// DrainReadStatesOnly extracts pending readStates without draining
// outbound messages. This is used by processCommitPendingInbox where
// inbox messages are handled under raftMu but outbound messages must
// remain in the buffer for the next GetUpdate cycle — otherwise ordered
// messages (ReplicateResp, VoteResp) would be lost since they cannot
// be sent before WAL persistence.
func (p *Peer) DrainReadStatesOnly() []proto.ReadyToRead {
	if len(p.r.readStates) == 0 {
		return nil
	}
	readStates := p.r.readStates
	p.r.readStates = nil
	return readStates
}

// RequestLeaderTransfer initiates a leadership transfer to the target
// replica. The request is wrapped in a LeaderTransfer message and
// routed through the raft state machine. The raft state machine handles
// all the mechanics: checking if the target is caught up, sending
// TimeoutNow, and tracking the transfer timeout.
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

// HasUpdate returns true if there is work for the engine to process.
// This includes new entries to save, messages to send, committed entries
// to apply, hard state changes, pending snapshots, or read states.
//
// Checks are ordered cheapest-first: slice length and field comparisons
// before entriesToSave() which requires arithmetic on inmem indices.
func (p *Peer) HasUpdate(_ uint64) bool {
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
	return false
}

// GetUpdate collects the current state changes into an Update for the
// engine to process. The Update contains entries to save, messages to
// send, committed entries to apply, hard state, and snapshots.
//
// moreEntriesToApply controls whether committed entries are included.
// When false (e.g., during snapshot-in-progress), committed entries are
// omitted.
//
// lastApplied is the highest index the engine has applied, used to
// determine which committed entries to include.
func (p *Peer) GetUpdate(moreEntriesToApply bool, lastApplied uint64) (proto.Update, error) {
	hs := p.r.hardState()

	// Detect state change before updating prevState (needed for ReadOnly).
	stateChanged := hs.Term != p.prevState.Term ||
		hs.Vote != p.prevState.Vote ||
		hs.Commit != p.prevState.Commit

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

	// Collect committed entries to apply. Entries are copied into a
	// pooled slice to reduce allocation pressure. The apply worker
	// calls ReleaseCommittedEntries after processing to return the
	// slice to the pool.
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

	// Collect pending snapshot. Only include snapshots from inmem
	// (received via InstallSnapshot), not the LogReader's persisted
	// snapshot. The commit worker needs to know about new snapshots
	// to persist; it must NOT see already-persisted local snapshots
	// or it would incorrectly reset the LogReader's tracked range.
	ss := p.r.log.pendingSnapshot()
	if ss != nil {
		update.Snapshot = *ss
		p.hasPendingSnapshot = false
	}

	// Mark the update as read-only if it carries no data requiring
	// persistence: no entries, no state change, no snapshot. The step
	// worker uses this to bypass the commit/apply pipeline, eliminating
	// unnecessary LogDB writes and goroutine hops for ReadIndex.
	update.ReadOnly = !stateChanged &&
		len(update.EntriesToSave) == 0 &&
		len(update.CommittedEntries) == 0 &&
		update.Snapshot.Index == 0

	// Update previous state for next diff and sync the atomic leaderID.
	p.prevState = hs
	p.syncLeaderID()

	return update, nil
}

// GetPiggybackUpdate extracts committed entries, messages, and the
// current hard state for piggybacking onto an empty-commit cycle.
// Eliminates the second pipeline traversal for multi-node proposals
// where entries are persisted in cycle 1 and the commit index advances
// during commitPending via processCommitPendingInbox.
//
// Unlike GetUpdate, this method:
//   - Does NOT touch entriesToSave (already persisted in cycle 1)
//   - Does NOT touch readStates (drained by processCommitPendingInbox)
//   - Does NOT touch hasPendingSnapshot
//   - DOES drain messages and update prevState
//   - Returns empty Update (CommittedEntries=nil) if nothing to apply
//
// The caller must hold raftMu.
func (p *Peer) GetPiggybackUpdate(lastApplied uint64) (proto.Update, error) {
	if !p.r.log.hasEntriesToApply() {
		return proto.Update{}, nil
	}
	committedEntries, pooled, err := p.r.log.entriesToApply()
	if err != nil {
		return proto.Update{}, err
	}
	if len(committedEntries) == 0 {
		return proto.Update{}, nil
	}
	hs := p.r.hardState()
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
	// Drain pending messages produced by processCommitPendingInbox
	// (e.g., broadcastAppend Replicate after tryCommit).
	if len(p.r.msgs) > 0 {
		update.Messages = p.r.drainMessages()
	}
	// Update prevState so step worker's HasUpdate/hasStateChanged
	// doesn't re-extract the same commit index change.
	p.prevState = hs
	p.syncLeaderID()
	return update, nil
}

// Commit acknowledges that the engine has persisted the Update. This
// advances the log's persistence markers and frees in-memory resources.
//
// appliedTo is called here (not in GetUpdate) so that the processed
// marker only advances after the committed entries are guaranteed to
// have reached the commit/apply pipeline. If appliedTo were called in
// GetUpdate and the downstream send failed, committed entries would be
// permanently lost since hasEntriesToApply would return false.
//
// Returns an error if the log reader detects a gap in the entry range,
// which indicates a bug in the entry persistence pipeline.
func (p *Peer) Commit(update proto.Update) error {
	// Register durable state with the LogReader FIRST. These are the only
	// fallible steps in Commit; running them before any marker advances
	// keeps an error fully RE-DELIVERABLE — the processed marker has not
	// moved, so entriesToApply() will offer the same batch again. If they
	// ran after appliedTo, a failure here would strand committed entries
	// that can never be re-emitted (the state machine would silently miss
	// a range while the shard kept serving reads).
	if !update.Snapshot.IsEmpty() {
		// Without this the LogReader keeps its initial empty range and
		// subsequent Append calls for entries after the snapshot index
		// fail with ErrLogGapDetected.
		if err := p.logReader.ApplySnapshot(update.Snapshot); err != nil {
			return err
		}
	}
	if len(update.EntriesToSave) > 0 {
		// Updates the tracked range and term cache so Term() lookups still
		// resolve for entries commitUpdate -> appliedLogTo is about to
		// truncate from inmem; without it the leader's sendAppend cannot
		// resolve term(prevLogIndex) for slow followers.
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

		// Apply committed config changes to membership and sync raft
		// remotes. This is THE point where membership takes effect
		// (commit-time semantics, see ProposeConfigChange): each
		// committed entry flows through exactly one update's
		// CommittedEntries because the processed marker advances
		// monotonically. Must run before checkLeaderSelfRemoval below so
		// self-removal sees the post-change membership.
		p.processConfigChanges(update.CommittedEntries)

		// The leader enforces the single-pending invariant
		// (handleLeaderPropose rejects proposals when pendingConfigChange
		// is true), so at most one config change is pending at any given
		// time on the leader. However, a follower catching up via log
		// replication can legitimately receive a batch containing
		// multiple committed config changes that were proposed and
		// committed across separate leader rounds. Process each one
		// sequentially: clear pendingConfigChange and check leader self-
		// removal for every config change entry in the batch.
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

	// Record the last applied index from the previous cycle. This is the
	// index confirmed by the apply worker (node.lastApplied), NOT the
	// index of entries being committed right now. appliedLogTo uses this
	// to truncate inmem entries that are safe to discard: they have been
	// both persisted to LogDB and applied to the state machine.
	//
	// Using CommittedEntries[last].Index here would truncate entries that
	// haven't been applied yet, causing marker index mismatches when the
	// inmem state is accessed during rapid create/destroy cycles.
	uc.LastApplied = update.LastApplied

	// Record read states.
	if len(update.ReadyToRead) > 0 {
		uc.ReadyToRead = uint64(len(update.ReadyToRead))
	}

	p.r.log.commitUpdate(uc)

	// Persistence edge: tryCommit uses the leader's durable index
	// (inmem.savedTo) as its own match, so a commit that was blocked only
	// on local durability becomes possible exactly here, after savedTo
	// advanced. Re-evaluate and, if the commit index moved, broadcast so
	// followers learn it promptly. This mirrors etcd's async-storage-
	// writes design (self-directed append-resp after the local append
	// completes). The messages land in r.msgs and are drained by the
	// commit worker's piggyback update or the next step-worker cycle.
	if p.r.state == leader && uc.StableLogTo > 0 {
		if p.r.tryCommit() {
			p.r.broadcastAppend()
		}
	}
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
	return !p.IsLeader() && !p.cfg.IsObserver && !p.cfg.IsWitness
}

// IsObserver returns true if this peer is configured as a non-voting
// observer.
func (p *Peer) IsObserver() bool {
	return p.cfg.IsObserver
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

// MarkJoining marks this peer as a joining node with incomplete
// membership. A joining node starts with empty membership and learns
// the full cluster membership via snapshot. Until the snapshot is
// received, per-shard auth is bypassed. Transport-level mTLS and
// deployment ID provide security during this bootstrapping window.
// The raft state machine gets its own mirror of the flag: it owns the two
// campaign entry points (startElection, handleTimeoutNow) and cannot see a
// field on Peer. The mirror is written only here — before the shard's step
// worker starts — and in Handle's snapshot-adopt branch, which runs on the step
// worker, so it needs no synchronization of its own. Peer.joining stays atomic
// because IsMember reads it from the transport recv goroutine.
func (p *Peer) MarkJoining() {
	p.setJoining(true)
}

// setJoining writes BOTH copies of the joining flag: the atomic one on Peer
// that IsMember reads from the transport recv goroutine, and the raft-layer
// mirror that gates campaigning. It is the only writer of either, so the two
// cannot drift apart — a divergence would leave a node that has learned the
// full membership still refusing to campaign, permanently.
//
// The raft mirror needs no synchronization of its own: this is called from
// MarkJoining (before the shard's step worker starts) and from Handle's
// snapshot-adopt branch (on the step worker).
func (p *Peer) setJoining(joining bool) {
	p.joining.Store(joining)
	p.r.setJoining(joining)
}

// clearJoiningIfKnown ends the joining window once this replica has learned a
// membership that names members other than itself.
//
// Under config-in-log the initial configuration is a committed log entry
// (EntryInitialMembership at index 1), so a joiner learns the full membership by
// ordinary replication of that entry — guaranteed, since the leader replicates
// from the joiner's match point up — or by replaying it on restart. The window
// therefore ends DETERMINISTICALLY at the index that entry occupies, rather than
// waiting for a snapshot the leader may never send (the old permanent-bypass
// bug). A snapshot still clears joining too (Handle's adopt branch), for the
// compacted case.
//
// "Knows a member other than itself" is the exact end condition: the whole
// reason IsMember bypasses authorization while joining is that a fresh joiner
// knows no members and would reject the leader's own replication. The moment it
// knows any other member it can authorize, so the bypass is retired. A
// single-node bootstrap never sets joining, so this is only ever reached by a
// replica added to an existing cluster, whose learned membership always names
// the other members.
func (p *Peer) clearJoiningIfKnown() {
	if !p.joining.Load() {
		return
	}
	if p.membership.hasMemberOtherThan(p.replicaID) {
		p.setJoining(false)
	}
}

// IsMember returns true if the given replicaID is a known member of
// this shard in any active role (voter, observer, or witness). This is
// used by the transport message handler to enforce per-shard message
// authorization, preventing compromised nodes from injecting messages
// into shards they are not members of.
//
// Safe for concurrent access from any goroutine. Reads from an atomic
// pointer to an immutable membership snapshot, avoiding locks on the
// hot recv path.
func (p *Peer) IsMember(replicaID uint64) bool {
	// A joining node starts with empty membership: it must accept the leader's
	// first replication before it can learn who the members are, so while
	// joining it authorizes every replica ID. Under config-in-log this window is
	// BOUNDED and deterministic — the initial configuration is a committed log
	// entry (EntryInitialMembership at index 1), which the leader replicates like
	// any other entry, and clearJoiningIfKnown ends the window the moment this
	// replica applies it (or any config change) and thus knows a member other
	// than itself. Transport-level mTLS and the deployment ID gate the window.
	if p.joining.Load() {
		return true
	}
	snap := p.membershipSnap.Load()
	if snap == nil {
		return false
	}
	return snap.isMember(replicaID)
}

// DiagRaftState returns diagnostic raft state for debugging pipeline stalls.
// Returns term, committed index, processed index, lastIndex, number of
// pending messages, and the raft state name (leader/follower/candidate).
//
// The state string also carries the two conditions under which this replica
// cannot participate in an election. Both are fail-closed and were otherwise
// invisible, which is the worst possible combination: they present as a shard
// that will not elect a leader while every node is up and reachable, with
// nothing in the diagnostics to say why.
//
//   - "self-removed": the replica is absent from the current configuration, so
//     startElection refuses to campaign. Permanent until a config change
//     re-adds it.
//   - "unreadable-log-denials=N": N votes were denied because this replica's
//     own last log term could not be read (a compaction race, a torn tail, a
//     closed LogDB), which denies every candidate regardless of its log.
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
	r := p.r
	if r.state != leader {
		return "not leader"
	}
	result := ""
	for id, rm := range r.remotes {
		if result != "" {
			result += " "
		}
		state := "unknown"
		switch rm.state {
		case remoteProbe:
			state = "probe"
		case remoteReplicate:
			state = "replicate"
		case remoteSnapshot:
			state = "snapshot"
		}
		paused := rm.isPaused()
		active := rm.isActive()
		result += fmt.Sprintf("r%d[match=%d next=%d state=%s paused=%v active=%v inflights=%d]",
			id, rm.match, rm.next, state, paused, active, rm.inflights.Count())
	}
	return result
}

// hasStateChanged returns true if the hard state has changed since the
// last GetUpdate call. Checks term, vote, and commit index.
func (p *Peer) hasStateChanged() bool {
	return p.r.term != p.prevState.Term ||
		p.r.vote != p.prevState.Vote ||
		p.r.log.committed != p.prevState.Commit
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

// AppliedToTerm returns the term of the last applied entry tracked by the
// in-memory log. This is reliable even after compaction because it is
// cached in inMemory.appliedToTerm and never requires a LogDB lookup.
// Used by the snapshot pool to record the correct term in snapshot metadata
// without risking a stale 0 from a compacted LogReader.
func (p *Peer) AppliedToTerm() uint64 {
	return p.r.log.inmem.appliedToTerm.Load()
}

// CommittedIndex returns the current committed log index from the Raft log.
// Must only be called from the step worker goroutine (single-owner invariant).
func (p *Peer) CommittedIndex() uint64 {
	return p.r.log.committed
}

// LogMarkers returns the Raft log's committed and processed markers as a
// consistent pair. Must only be called from the step worker goroutine (the
// single-owner invariant CommittedIndex documents); it exists so that owner
// can PUBLISH both markers for goroutines that may not touch the Peer at all.
// Unlike DiagRaftState it formats nothing and allocates nothing, so it is
// cheap enough to call on every tick.
func (p *Peer) LogMarkers() (committed, processed uint64) {
	return p.r.log.committed, p.r.log.processed
}

// syncLeaderID copies the raft state machine's leaderID to the atomic
// field on Peer. This is called after every operation that may change
// leadership (tick, step, GetUpdate). The raft struct is not thread-safe,
// but the atomic leaderID on Peer provides safe concurrent reads for
// LeaderID() and IsLeader().
func (p *Peer) syncLeaderID() {
	p.leaderID.Store(p.r.leaderID)
	p.term.Store(p.r.term)
}

// Term returns the current raft term. Safe for concurrent access; kept in
// step with LeaderID by syncLeaderID.
func (p *Peer) Term() uint64 {
	return p.term.Load()
}
