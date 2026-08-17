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

	// stateUnpersisted forces the next GetUpdate to report a state change even
	// when the hard state equals prevState.
	//
	// GetUpdate advances prevState at EXTRACTION time, which is optimistic: it
	// assumes the engine will go on to persist the update it just handed out.
	// If that update is lost instead — a panic between extraction and
	// SaveState — prevState claims a term/vote/commit that never reached disk,
	// the next update is classified ReadOnly, and ReadOnly bypasses SaveState
	// entirely. A vote granted in the lost update is then answered by a
	// RequestVoteResp with no WAL write behind it, which is
	// two-leaders-in-one-term after a crash.
	//
	// GetPiggybackUpdate already avoids this by not advancing prevState at all
	// (see NotePiggybackPersisted); this flag is the equivalent for the main
	// extraction path, where not advancing is not an option because the
	// ReadOnly classification is computed from the same diff.
	//
	// Written by InvalidatePersistedState from the engine's panic-recovery
	// path, cleared by the GetUpdate that acts on it. Guarded by the same
	// exclusive step-worker access as the rest of Peer.
	stateUnpersisted bool

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

	// leaderSeq makes leaderID and term readable as a CONSISTENT PAIR via
	// LeaderAndTerm. They are two independent words, so a reader that loads them
	// separately can observe a (leaderID, term) combination that never existed —
	// syncLeaderID may land between the two loads. Reporting such a pair through
	// OnLeaderUpdated tells a listener that a replica led in a term it never led.
	//
	// This is a seqlock: syncLeaderID (the only writer, always serialised by
	// raftMu) makes the counter ODD for the duration of the update and EVEN once
	// the pair is settled, so a reader that sees the same even value before and
	// after its loads knows no write interleaved. Writers never block; readers
	// retry, and the write window is two stores.
	leaderSeq atomic.Uint64

	// shardID and replicaID identify this peer. Cached from cfg for
	// fast access without going through raft.
	shardID   uint64
	replicaID uint64

	// hasPendingSnapshot tracks whether a snapshot is waiting to be
	// persisted. Set when raft restores a snapshot, cleared after
	// GetUpdate returns it.
	hasPendingSnapshot bool

	// joining is true for nodes started with join=true. It means exactly one
	// thing: this replica's configuration is KNOWN-INCOMPLETE — it may hold only
	// itself, or nothing at all, because it has not yet replayed the entries
	// naming the rest of the shard.
	//
	// It is NOT an authorization bypass. Per-shard authorization is a
	// removed-tombstone deny-list (Peer.IsRemoved) which needs no bypass,
	// because it never wrongly refuses a peer it simply has not heard of. The
	// flag gates the two decisions that a configuration this replica knows to be
	// incomplete cannot safely support: campaigning (startElection,
	// handleTimeoutNow — a replica holding only itself would self-elect on a
	// quorum of one) and the candidate test in handleVoteRequest (it would deny
	// every real member and strand the shard).
	//
	// Under config-in-log it is cleared deterministically by clearJoiningIfKnown
	// the moment the node applies the bootstrap membership entry (replicated like
	// any other entry) or any config change that names a member other than
	// itself; a snapshot adopt also clears it, for the compacted case.
	joining atomic.Bool

	// staleConfigThrough is the commit index this replica RECOVERED with, held
	// until every committed config change at or below it has been folded into
	// membership. Zero means the configuration is current.
	//
	// It exists because membership is config-in-log and is reconstructed
	// ASYNCHRONOUSLY. A restart recovers only a BASE configuration — the
	// snapshot record's membership, or the EntryInitialMembership at index 1
	// (Host.reconstructShardMembership reads exactly that one entry) — and
	// learns every change above it by re-applying committed entries through the
	// ordinary pipeline (Peer.Commit -> processConfigChanges). The replica is
	// live and TICKING throughout that replay, so its election timer can fire
	// while its voting set is still the superseded one: it then computes quorum
	// over the wrong denominator, solicits votes from replicas the
	// configuration removed, and fails to solicit from ones it added. A replica
	// restarted against a 3-voter base while the committed log says 5 wins on 2
	// votes, in a term the real 5-voter configuration can elect in as well.
	//
	// The watermark makes that window OBSERVABLE to the campaign gates. It is
	// resolved by whichever of these happens first, and every one of them is
	// driven by state this replica already holds:
	//
	//   - the commit pipeline reaching it (processConfigChanges), which is the
	//     free and overwhelmingly common case;
	//   - replayConfigChangesTo covering it (SetProcessedIndex, for an on-disk
	//     state machine whose durable marker outruns the snapshot);
	//   - resolveStaleStartupConfig folding the remainder on demand, which is
	//     what the campaign gates call.
	//
	// Deliberately NOT a "wait until the state machine catches up" gate. Tying
	// campaigning to apply progress makes election liveness a function of state
	// machine throughput, and it cannot self-clear on a replica whose apply
	// pipeline is not running at all.
	staleConfigThrough uint64

	// configFoldedThrough is the highest index up to which every committed entry
	// HAS BEEN OFFERED to applyMembershipEntry. It is the lower bound
	// resolveStaleStartupConfig resumes its scan from.
	//
	// It exists because "what the state machine APPLIED" and "what has been
	// FOLDED into the membership" are different facts, and conflating them was a
	// silent way to campaign against a superseded voting set. The fold used to
	// resume at r.log.processed+1, on the reasoning that everything at or below
	// processed had already been offered. SetProcessedIndex breaks that: it
	// advances processed UNCONDITIONALLY and then asks replayConfigChangesTo to
	// fold the same window, DISCARDING the report of whether the fold reached the
	// end. One unreadable chunk and the window is skipped forever — processed
	// already covers it, so resolveStaleStartupConfig's low > index early-out
	// declares the configuration resolved and clears the watermark over entries
	// nobody read. Keeping the two markers apart makes the fold's own progress
	// the only thing the fold trusts.
	//
	// Seeded from the recovered base's ConfigChangeID, which is by definition the
	// last config change that base already accounts for.
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
		membership: newMembership(ms),
		prevState:  r.hardState(),
		shardID:    cfg.ShardID,
		replicaID:  cfg.ReplicaID,
	}

	// Arm the config-in-log staleness watermark and install the resolver the
	// campaign gates consult. Both happen HERE, together, so the two halves can
	// never be half-configured: a bare raft (the adversarial harness, the
	// state-machine unit tests) has no Peer, therefore no resolver, therefore
	// no gate at all — it also has no membership to fold, so there is nothing
	// for the gate to protect there.
	//
	// The window exists only when the recovered log commits PAST the
	// configuration the recovered membership describes. ms.ConfigChangeID is
	// the index of the last config change that base already accounts for, so
	// anything at or below it is folded by construction, and a commit index of
	// zero (a genuine first start) has no tail at all.
	if r.log.committed > ms.ConfigChangeID {
		p.staleConfigThrough = r.log.committed
	}
	p.configFoldedThrough = ms.ConfigChangeID
	r.resolveStaleConfig = p.resolveStaleStartupConfig

	// Install the full membership validator as the leader's propose-time gate.
	// ProposeConfigChange and the Host API already run these rules, but a
	// wire-forwarded Propose reaches handleLeaderPropose having run NONE of
	// them, and the apply path never re-validates. Binding the same
	// membership.validate to both entry points is what keeps the wire path from
	// being the laxer one; see raft.validateConfigChange.
	//
	// It closes over p, so it always reads the CURRENT mutable membership rather
	// than a copy taken here. Both this and handleLeaderPropose run on the step
	// worker, which is the goroutine that owns that membership.
	r.validateConfigChange = p.validateProposedConfigChange

	p.publishMembership()
	p.syncLeaderID()
	return p, nil
}

// resolveStaleStartupConfig folds the committed configuration tail recovered at
// construction into the membership, and reports whether the configuration is
// now current through the recovered commit index.
//
// It is the on-demand half of the staleConfigThrough watermark and is called
// from the raft campaign gates (startElection, handleTimeoutNow) — nowhere
// else. That placement is the whole point of the design:
//
//   - On the happy path it never runs. A replica that restarts into a shard
//     with a live leader is heartbeated long before its election timer fires,
//     and the commit pipeline clears the watermark for free in the meantime.
//   - It runs only when this replica is about to campaign, which means the
//     shard is already leaderless. Paying one bounded read of the local log
//     there is strictly better than campaigning against a voting set the
//     configuration has moved past.
//
// The scan starts just above configFoldedThrough — what the FOLD itself has
// covered — not above the processed marker. Those are different facts: processed
// is advanced by SetProcessedIndex whether or not the accompanying fold reached
// the end, and by raftLog.restore, so resuming from it silently declares an
// unread window empty. configFoldedThrough is written only by a fold that
// actually offered the entries to applyMembershipEntry (the commit path, this
// replay, and the snapshot adopt in Handle), so the residual shrinks exactly as
// far as the work that was really done, and by the first election timeout it is
// usually empty.
//
// Returning false suppresses the campaign for THIS timeout only. The caller
// retries on the next one, exactly like the unreadable-last-log-term gate beside
// it. Two things cause it: a local log that cannot be read (transient), and a
// committed config entry that does not decode (permanent — the shard needs an
// operator, and refusing to campaign on an unknowable voting set is the
// fail-closed answer).
func (p *Peer) resolveStaleStartupConfig() bool {
	if p.staleConfigThrough == 0 {
		return true
	}
	complete, err := p.replayConfigChangesFrom(p.configFoldedThrough+1, p.staleConfigThrough)
	if err != nil {
		slog.Error("configuration tail holds an undecodable committed config entry: "+
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
		slog.Warn("configuration tail unresolved: refusing to campaign against a "+
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
//
// It returns TWO things, and the second one is the point of this signature:
//
//   - The index actually applied after clamping, so the caller can seed ITS OWN
//     markers from the same value. The engine's lastApplied and raft's processed
//     must not diverge: a later Update carrying an unclamped LastApplied would
//     trip commitUpdate's assertion and kill the commit worker.
//   - The FATAL error of a committed config entry that does not decode.
//
// The error used to be swallowed here. This path cannot fault the shard itself —
// it runs before the engine node exists — so the strongest thing it could do
// alone was arm the staleness watermark and refuse to campaign, which it still
// does. But refusing to campaign is a LIVENESS answer to a SAFETY fact: the
// replica came up on a configuration it knows is unreconstructable and then
// served reads and acked the leader from it, with one ERROR line as the only
// signal. The commit path faults the shard for the identical bytes
// (processConfigChanges -> failCommit) and recovery aborts StartShard for them
// (ReconstructMembership); reporting it lets the startup path reach the same
// verdict instead of being the one place that fails open.
//
// The INCOMPLETE-but-not-corrupt case is deliberately not an error: an unread
// window is a window this replica may yet read (the commit pipeline, or
// resolveStaleStartupConfig on demand), so it arms the watermark and nothing
// more. Only "these committed bytes will never decode" is terminal.
func (p *Peer) SetProcessedIndex(index uint64) (uint64, error) {
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
	// The fold's REPORT is the point of calling it. Advancing processed says the
	// state machine already applied this window; it says nothing about whether
	// the window's config changes reached the membership. Discarding the report
	// and advancing anyway is what let an unread window be declared resolved: the
	// membership stayed at the superseded voting set, configFoldedThrough never
	// covered the window, and — under the old processed-based lower bound —
	// resolveStaleStartupConfig's early-out then cleared the watermark and let
	// the replica campaign against it.
	//
	// So an incomplete or undecodable fold ARMS the watermark. The campaign gates
	// then refuse until the commit pipeline folds the window for real, which is
	// the right answer for a window that is merely unread. For an UNDECODABLE
	// entry the watermark is no longer the whole answer: the error is returned so
	// Host.StartShard can fault the shard, matching the commit path and recovery.
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
		slog.Error("committed config changes below the state machine's applied index "+
			"were not folded: this replica will not campaign until they are",
			"shard", p.shardID,
			"replica", p.replicaID,
			"index", index,
			"folded_through", p.configFoldedThrough,
			"stale_through", p.staleConfigThrough,
			"error", err,
		)
	}
	// The watermark is armed either way before this returns, so a caller that
	// chooses to ignore the error still gets the old fail-closed campaigning
	// behaviour rather than nothing.
	return index, err
}

// replayConfigChangesTo folds committed config changes at or below index into
// the membership. Entries already covered by the recovered snapshot membership
// are skipped by the replay guard, so only the (snapshotIndex, index] window
// has any effect. index must not exceed p.r.log.committed; callers clamp it.
//
// It reports what replayConfigChangesFrom reports: whether the fold REACHED
// index, and the fatal error of an undecodable committed config entry.
func (p *Peer) replayConfigChangesTo(index uint64) (bool, error) {
	return p.replayConfigChangesFrom(p.r.log.firstIndex(), index)
}

// replayConfigChangesFrom folds committed config changes in [low, index] into
// the membership and reports whether it REACHED index — that is, whether the
// membership can now be trusted to describe the configuration as of index.
//
// low is raised to the log's first available index, so a caller may pass any
// lower bound it can justify; see resolveStaleStartupConfig for why the
// processed marker is one.
//
// A compacted floor counts as reached: every index below it is subsumed by the
// snapshot whose membership this replica recovered, which is precisely the base
// the replay builds on. Any OTHER read failure does not, because the entries it
// could not read may carry config changes and the caller must not treat an
// unread window as an empty one.
//
// The error return is the FATAL case, separate from the incomplete one: a
// committed config-carrying entry whose payload does not decode. It is not a
// window this replica has yet to read, it is one it can never read, and the
// membership it defines is unknowable here while every peer holds it. See
// applyMembershipEntry.
//
// Whatever it reaches is recorded in configFoldedThrough, so a later resumption
// starts where this one really stopped rather than where the state machine's
// marker happens to sit.
func (p *Peer) replayConfigChangesFrom(low, index uint64) (bool, error) {
	if first := p.r.log.firstIndex(); low < first {
		low = first
	}
	if low > index {
		return true, nil
	}
	// The highest index actually OFFERED to applyMembershipEntry, seeded one
	// below the scan's start so a scan that reads nothing records nothing.
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
		// Bound each read in BYTES as well as in entries. A 1024-entry chunk of
		// arbitrary log entries — config changes are interspersed with ordinary
		// data entries — is otherwise unbounded (maxSize 0), up to
		// 1024×MaxApplyEntrySize per read at startup. limitEntrySize always
		// returns at least one entry and the loop advances by the last index
		// read, so the byte bound cannot stall progress even on a lone oversized
		// entry.
		entries, err := p.r.log.entries(low, high, p.r.maxReplicationPayload)
		if err != nil || len(entries) == 0 {
			// Compacted: the snapshot membership already covers everything
			// below the compaction floor, so the replay is COMPLETE at this
			// point rather than merely stopped. Any other outcome leaves a
			// window this replica has not read, and reporting that as complete
			// is what would let a campaign run against a superseded voting set.
			complete = errors.Is(err, ErrCompacted)
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
			// applyMembershipEntry folds both the bootstrap EntryInitialMembership
			// at index 1 (which is the first entry replayed when the scan starts at
			// firstIndex) and every incremental EntryConfigChange above it. It
			// converges on the committed role and logs any deterministic refusal.
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
	var committedBefore uint64
	if msg.Type == proto.InstallSnapshot {
		pendingBefore = p.r.log.pendingSnapshot()
		termBefore = p.r.term
		committedBefore = p.r.log.committed
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

	// A locally injected snapshot whose MESSAGE term is behind ours is stale: the
	// snapshot receive path can complete after this replica has moved on to a
	// later term, and its membership is then older than what the log already
	// applied.
	//
	// This is defence in depth, not the operative protection. Neither production
	// injector (messagehandler's re-delivery of a persisted snapshot, and the
	// post-receive install) sets Message.Term — they populate Snapshot.Term only
	// — so msg.Term is 0 there and this never fires. It is deliberately NOT
	// rewritten to compare Snapshot.Term: that is the term of the snapshot's last
	// included entry, which is routinely far below the current term for a
	// perfectly valid snapshot, and rejecting on it would refuse legitimate
	// installs.
	//
	// What actually bounds this hazard is snapshotRejected below: a snapshot at
	// or below the committed index cannot redefine membership. Index, not term,
	// is the sound staleness test here.
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
	// A nil error from step does NOT mean raft installed the snapshot. Two
	// distinct paths return nil without installing anything, and membership --
	// the very set quorum() is computed over -- must never come from either:
	//
	//   - handleSnapshot REJECTS a snapshot at or below the committed index and
	//     returns nil. Its configuration is older than what this replica has
	//     already committed, so adopting it rolls the voting set backwards. The
	//     joining branch below deliberately bypasses the ConfigChangeID
	//     staleness test, so it adopted EVERY rejected retransmission --
	//     including configurations that do not name this replica, leaving it
	//     self-removed, unelectable, and computing quorum over a stale set.
	//   - stepLeader has NO InstallSnapshot case at all, so on a leader the
	//     message is silently ignored and step returns nil. The leader then
	//     adopted a configuration no committed entry ever established.
	//
	// Both are covered by one positive test: did raft actually take the
	// snapshot? Every accepting path in handleSnapshot -- the log-matching
	// fast-forward and the full restore alike -- advances the committed index
	// to the snapshot's index, and it can only get there from ss.Index >
	// committed. Neither rejection nor the leader's silent drop touches it.
	// step processes exactly this one message, so the advance is attributable.
	snapshotProcessed := msg.Type == proto.InstallSnapshot &&
		p.r.log.committed > committedBefore
	if msg.Type == proto.InstallSnapshot && err == nil && !staleTerm && !wireOrigin &&
		snapshotProcessed {
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
		// rejects the only message that can repair it, leaving the replica
		// permanently unable to campaign or to vote for the shard's real members.
		ss := msg.Snapshot
		pending := p.r.log.pendingSnapshot()
		restored := pending != nil && pending != pendingBefore
		joining := p.joining.Load()
		if restored || joining || ss.Membership.ConfigChangeID > p.membership.configChangeID {
			p.membership = newMembership(ss.Membership)
			// The adopted membership covers every config change up to the
			// snapshot's index, so the fold marker moves with it. Without this the
			// on-demand fold would re-scan a window the snapshot already answered —
			// or, worse for a compacted log, report it unread.
			if ss.Index > p.configFoldedThrough {
				p.configFoldedThrough = ss.Index
			}
			// Publish immutable snapshot for concurrent readers.
			p.publishMembership()
			// handleSnapshot refuses an over-cap membership before it can be
			// adopted, so this is a backstop; it is folded into err rather
			// than returned early so the leader-ID publication below still
			// runs, exactly as it does for a step error.
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
	//
	// handleLeaderPropose now runs the SAME validator (raft.validateConfigChange
	// is wired to validateProposedConfigChange in NewPeer), so this call is no
	// longer the only gate — it is the one that answers the caller before the
	// change is marshaled, and it reports the membership error rather than
	// whichever earlier propose-time refusal the step would have hit first.
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

// validateProposedConfigChange validates a config change against the MUTABLE
// membership, for the leader's propose-time gate in handleLeaderPropose.
//
// It reads p.membership, not the published snapshot ValidateConfigChange uses,
// and that is the point: this runs on the step worker, which owns that
// membership, so it sees every config change committed up to this instant. The
// snapshot is published for readers on other goroutines and can lag by a commit.
// A propose-time gate judging a stale configuration is how a change that the
// current membership forbids — a second replica ID for an identity a config
// change just bound, most sharply — reaches the log.
func (p *Peer) validateProposedConfigChange(cc proto.ConfigChange) error {
	return p.membership.validate(cc)
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

// membershipVotingBitCount returns how many ReadIndex ackBits positions the
// peer's CURRENT membership would occupy once mirrored into the raft layer:
// every voter and every witness other than this replica, plus self, which
// rebuildReplicaToBit seats at bit 0 unconditionally. It counts the membership
// rather than the raft maps because it must be answerable BEFORE syncRaftRemotes
// mutates them.
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

// syncRaftRemotes synchronizes the raft state machine's remote tracking
// maps from the peer's membership. This ensures the raft layer tracks
// the correct set of peers after config changes.
//
// Returns ErrTooManyVotingMembers when the applied membership names more
// voting members than the ReadIndex ackBits word can address. Every caller is
// on an error-returning path that the engine already treats as terminal for
// the shard (processConfigChanges documents this: "The engine's failCommit
// treats it as the fault it is"), so the shard is marked failed and reported
// through EventListener.OnShardFailed while the host and its other shards keep
// running. That replaced an invariant.Assert one frame down in
// rebuildReplicaToBit, which took the whole step worker instead.
//
// The check runs BEFORE any of the raft maps are touched, deliberately. The
// membership itself has already been applied — it is committed replicated
// state and rolling it back would be a divergence — but the raft mirror is
// local, and leaving it on the last configuration that fit is what makes this
// fail CLOSED: the shard stops with a coherent voting set rather than a
// half-synced one, and no ReadIndex is ever answered from a truncated bitmap.
//
// The leader propose gate (validateConfigChangePolicy) is what normally stops
// such a configuration ever being proposed, and the committed-replay path
// deliberately does not re-validate, because refusing an already-committed
// entry there would make replicas diverge. Failing the shard is the honest
// response to a committed entry this build cannot represent.
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

	// Cross-check the locally configured witness AND non-voting roles against
	// the replicated configuration this call just applied. A committed
	// AddWitness or AddObserver naming this replica, or an AddNode promoting it
	// out of either set, is the point at which the two can start to disagree.
	p.r.checkSelfRole(p.membership.isMember(p.replicaID),
		p.membership.isWitness(p.replicaID),
		p.membership.isObserver(p.replicaID))

	// Drop vote records for replicas that just left. The election quorum is
	// computed over the voting set this call just reshaped; a stale grant
	// from a departed replica would be tallied against the smaller quorum.
	// This now includes a self vote cast before a self-removal committed.
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
// Any other entry type is ignored. The two failure modes are reported
// SEPARATELY, because they are opposite kinds of fact:
//
//   - A DECODE failure returns ErrCorruptConfigEntry and is FATAL. The bytes are
//     committed replicated state that every peer can read and this replica
//     cannot, so continuing means running on a configuration that differs from
//     the rest of the shard — silently, and durably once the next snapshot
//     persists it. Recovery already treats these exact bytes as fatal
//     (ReconstructMembership returns the error and StartShard aborts); the
//     runtime failing open while recovery failed closed was the inconsistency.
//     The membership is left untouched either way, so a caller that cannot fault
//     the shard at least does not adopt an empty voting set.
//   - A deterministic membership.apply REFUSAL returns (false, nil) and is
//     logged only. It is not a corruption: membership.apply CONVERGES on the role
//     each committed add names rather than refusing an entry whose target already
//     exists, so the refusals that survive are the ones EVERY replica evaluates
//     identically (a Removed tombstone re-add, an add contradicting a role only a
//     committed change could have set). Faulting on those would kill shards over
//     an outcome the whole cluster agrees on.
//
// A committed entry is never re-delivered, so neither outcome may be treated as
// "try again later" by the caller.
func (p *Peer) applyMembershipEntry(e proto.Entry) (bool, error) {
	switch e.Type & proto.EntryTypeMask {
	case proto.EntryInitialMembership:
		// The founding configuration is defined to live at exactly
		// BootstrapMembershipIndex. Applying one anywhere else would REPLACE the
		// membership wholesale — discarding every config change committed since
		// bootstrap and rolling the voting set back to the founding members,
		// which is a configuration no live replica shares. Only index 1 can
		// legitimately carry it, so anything else is malformed and is refused.
		//
		// The refusal is a pure function of the entry's index, so every replica
		// reaches the identical decision and the membership cannot diverge.
		if e.Index != BootstrapMembershipIndex {
			// Structural, not corrupt: the refusal is a pure function of the
			// entry's INDEX, so every replica reaches it identically and the
			// membership stays convergent. It is the apply-refusal shape, not the
			// undecodable-payload shape, and must not fault the shard.
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
		p.membership = newMembership(ms)
		return true, nil
	case proto.EntryConfigChange:
		// A zero-length payload is left as the silent no-op it has always been.
		// It is not the corruption case: every replica reads the same nothing and
		// skips identically, and no legitimate producer emits one (the
		// propose-time gate now refuses any payload that does not decode), so it
		// is a shape that only appears in synthetic entries.
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
			return false, nil
		}
		return true, nil
	default:
		return false, nil
	}
}

// processConfigChanges scans COMMITTED entries for config changes and
// applies them to the peer's membership, publishing the updated snapshot
// and syncing raft remote tracking. Called from Peer.Commit: membership
// takes effect at commit time (etcd/dragonboat model), not on log append.
// It returns the FATAL error of an undecodable committed config entry, which
// Peer.Commit propagates and the engine turns into a shard fault (failCommit).
// That is the point of the return value: at runtime this used to be a silent
// no-op — logged and the bool ignored — while recovery treated the identical
// bytes as fatal. Runtime failing open and recovery failing closed over the same
// input is a divergence generator: the live replica keeps serving on a
// configuration only it holds, and persists it with the next snapshot.
//
// A deterministic apply REFUSAL is still ignored, and must be: every replica
// evaluates it identically, so it is agreement, not corruption. See
// applyMembershipEntry.
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
			// The shard is already failing on fatal, which names the corrupt
			// entry and is the more useful diagnosis; this one is logged
			// rather than substituted. The raft mirror is left on the last
			// configuration that fit either way.
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
	// The ordinary commit pipeline reaching the startup staleness watermark
	// resolves the recovered configuration for FREE: every committed entry at
	// or below it has now been offered to applyMembershipEntry, which is the
	// same fold resolveStaleStartupConfig would perform by reading the log
	// back. This is the branch that keeps a restart into a healthy shard from
	// ever paying for that read. It is deliberately outside the needsSync test:
	// a tail that contained NO config change resolves the watermark just as
	// conclusively as one that did.
	if n := len(entries); n > 0 && entries[n-1].Index >= p.staleConfigThrough {
		p.staleConfigThrough = 0
	}
	if needsSync {
		// Publish immutable snapshot for concurrent readers.
		p.publishMembership()
		// Terminal for this shard: a committed configuration this build cannot
		// represent. Returning here reaches the engine's failCommit, which
		// marks the node failed and fires OnShardFailed, exactly as it does
		// for a config entry that will not decode.
		if err := p.syncRaftRemotes(); err != nil {
			return err
		}
		// A committed entry that gave this replica a membership beyond itself
		// ends the joining window: it now knows every member and can enforce
		// per-shard authorization. The bootstrap EntryInitialMembership is the
		// ordinary carrier of that on the replicate path.
		p.clearJoiningIfKnown()
	}
	return nil
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
	// stateUnpersisted forces the change on: a previously extracted update was
	// lost before SaveState, so prevState is claiming durability it does not
	// have and this update must carry the hard state back through the WAL.
	stateChanged := p.stateUnpersisted ||
		hs.Term != p.prevState.Term ||
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

	// Update previous state for next diff and sync the atomic leaderID. The
	// re-persist demand is satisfied by THIS update: it is not ReadOnly, so it
	// reaches SaveState.
	p.prevState = hs
	p.stateUnpersisted = false
	p.syncLeaderID()

	return update, nil
}

// InvalidatePersistedState marks the hard state as possibly-unpersisted, so the
// next GetUpdate carries it back through SaveState instead of classifying the
// update ReadOnly and skipping the WAL.
//
// The engine calls this when an extracted update is discarded without being
// persisted — today, when a panic unwinds the step worker between GetUpdate and
// the commit hand-off. Without it, prevState permanently claims a term and vote
// that never reached disk.
//
// Safe to call repeatedly; it costs at most one extra hard-state write.
// Must be called with the same exclusive Peer access GetUpdate requires.
func (p *Peer) InvalidatePersistedState() {
	p.stateUnpersisted = true
}

// GetPiggybackUpdate extracts committed entries, messages, and the
// current hard state for piggybacking onto an empty-commit cycle.
// Eliminates the second pipeline traversal for multi-node proposals
// where entries are persisted in cycle 1 and the commit index advances
// during commitPending via processCommitPendingInbox.
//
// Unlike GetUpdate, this method:
//   - Does NOT write entriesToSave into the Update, and the engine path it
//     feeds persists no entries. That is only safe for the entries this
//     method APPLIES — they are clamped to the durable marker below. It says
//     nothing about entries appended by processCommitPendingInbox during the
//     commitPending window, which are still RAM-only; see the ordered-message
//     withholding below, which is what keeps that distinction honest.
//   - Does NOT touch readStates (drained by processCommitPendingInbox)
//   - Does NOT touch hasPendingSnapshot
//   - DOES drain messages the durability rules permit it to release
//   - Does NOT advance prevState. The engine does that via
//     NotePiggybackPersisted once the hard state is actually on disk, so a
//     failed persist leaves hasStateChanged() true and the state is re-emitted.
//   - Returns empty Update (CommittedEntries=nil) if nothing to apply
//
// The caller must hold raftMu.
func (p *Peer) GetPiggybackUpdate(lastApplied uint64) (proto.Update, error) {
	if !p.r.log.hasEntriesToApply() {
		return proto.Update{}, nil
	}
	// Durability clamp. commitTo advances committed as far as lastIndex(),
	// which counts entries still in memory and not yet fsynced. Applying that
	// tail here, and returning a HardState.Commit that covers it, would let
	// executePiggyback fsync a Commit past the last durable log entry: after a
	// crash HardState.Commit would exceed the recovered log — the invariant
	// this fix protects. Extract and advertise only up to the durable marker;
	// the remainder re-emits on a later cycle once it is persisted, because
	// NotePiggybackPersisted records the CLAMPED hard state as prevState rather
	// than the raw one.
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
	// Drain pending messages produced by processCommitPendingInbox
	// (e.g., broadcastAppend Replicate after tryCommit).
	//
	// Durability split. The engine path this Update feeds (executePiggyback)
	// fsyncs HARD STATE ONLY — its logdb.Update carries no Entries — so any
	// entry appended during the commitPending window is still RAM-only when
	// these messages go out. processCommitPendingInbox steps Replicate here
	// (only InstallSnapshot is filtered), and handleAppend answers it with an
	// ORDERED ReplicateResp naming an index this replica has not made durable.
	// Releasing that is the Figure 2 violation "updated on stable storage
	// before responding to RPCs" exists to prevent: the leader counts the
	// match toward its commit quorum, and a crash here loses entries already
	// acknowledged to a client.
	//
	// So while entriesToSave() is non-empty, hand the sender only the
	// free-order messages and leave the ordered ones in r.msgs. They are
	// withheld, never dropped: len(r.msgs) > 0 keeps HasUpdate true, and the
	// next GetUpdate cycle — which DOES carry EntriesToSave through SaveState —
	// releases them after the fsync. This reuses the existing free-order split
	// (proto.IsFreeOrderMessage) rather than inventing a second rule; the
	// step worker applies exactly the same predicate for the same reason while
	// commitPending is set.
	if len(p.r.msgs) > 0 {
		msgs := p.r.drainMessages()
		if len(p.r.log.entriesToSave()) > 0 {
			msgs = p.r.withholdOrderedMessages(msgs)
		}
		if len(msgs) > 0 {
			update.Messages = msgs
		}
	}
	p.syncLeaderID()
	return update, nil
}

// NotePiggybackPersisted advances prevState to the hard state the engine has
// just written to stable storage on the piggyback path.
//
// It exists because GetPiggybackUpdate deliberately does NOT advance prevState:
// the persist happens after the Update leaves this package and can fail. An
// earlier version advanced it optimistically, so a failed piggyback SaveState
// left prevState claiming a term/vote/commit that never reached disk. A vote
// granted in that window became invisible to hasStateChanged(), letting a later
// update be ReadOnly and transmit its RequestVoteResp with no WAL write at all.
//
// hs must be the State carried by the Update that was persisted (the clamped
// one), not the peer's current hard state, which may already have moved on.
//
// The caller must hold raftMu.
func (p *Peer) NotePiggybackPersisted(hs proto.State) {
	p.prevState = hs
}

// RequeueMessages returns messages to the outbound buffer after the engine
// failed to establish the durability they were waiting on. They are placed
// AHEAD of anything produced since the drain, preserving emission order, and
// go out on the next cycle that persists. Dropping them instead is not
// equivalent: a lost VoteResp stalls an election until the next timeout.
//
// The caller must hold raftMu.
func (p *Peer) RequeueMessages(msgs []proto.Message) {
	p.r.requeueMessages(msgs)
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
		// A committed config entry that does not DECODE is terminal for this
		// shard: it is replicated state every peer can read and this replica
		// cannot, so there is no local state to fall back to that is not a
		// divergence. Returning here is past appliedTo, so the batch is not
		// re-deliverable — which is correct, because retrying decodes the same
		// bytes. The engine's failCommit treats it as the fault it is.
		if err := p.processConfigChanges(update.CommittedEntries); err != nil {
			return err
		}

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

// MarkJoining records that this peer's configuration is known-incomplete: it
// was started with join=true and has not yet replayed the entries naming the
// rest of the shard. See the joining field for exactly what that gates — it is
// not an authorization bypass.
//
// The raft state machine gets its own mirror of the flag: it owns the campaign
// entry points (startElection, handleTimeoutNow) and the vote guard, and cannot
// see a field on Peer. The mirror is written only here — before the shard's step
// worker starts — and in Handle's snapshot-adopt branch, which runs on the step
// worker, so it needs no synchronization of its own. Peer.joining stays atomic
// because MarkJoining runs on the caller's goroutine before the step worker
// exists, while every later read happens on a worker.
func (p *Peer) MarkJoining() {
	p.setJoining(true)
	// Then immediately test whether this replica is joining AT ALL. join=true is
	// a startup flag, not durable state: an operator restarting an established
	// replica keeps it set, and NewPeer has already seeded the membership from
	// the persisted NodeState. Without this evaluation the flag would only ever
	// be reconsidered by a config change or a snapshot, so a restarted replica in
	// a quiescent shard — nothing joining, nothing leaving, no compaction — would
	// refuse to campaign for the entire lifetime of the process.
	p.clearJoiningIfKnown()
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

// clearJoiningIfKnown ends the joining window once this replica's configuration
// is complete enough to trust. "Complete enough" means it can support the two
// decisions the window gates — campaigning, and the candidate test in
// handleVoteRequest — without either self-electing on a quorum of one or denying
// votes to the real members. Three sufficient signals, any of which clears it:
//
//	(a) the LEADER it is following is a known member. Membership is learned
//	    incrementally on the replicate path and may still be PARTIAL: the current
//	    leader may have been ADDED AFTER the initial bootstrap, so it is not in
//	    the founding membership the index-1 EntryInitialMembership carries.
//	    Clearing on "knows any member" would let a joiner that received the
//	    bootstrap entry in an earlier batch than the leader's own AddNode start
//	    denying that leader's votes. Gating on the leader closes that gap.
//
//	(b) this replica is itself a member ALONGSIDE somebody else. It has replayed
//	    or received its own AddNode, so the committed membership up to its
//	    addition is complete. This is what covers the RESTART path:
//	    replayConfigChangesTo (SetProcessedIndex) runs before the first Tick, so
//	    leaderID is noLeader and (a) cannot fire; without (b) a replica restarted
//	    with join=true on a fully-caught-up log would reconstruct its whole
//	    membership yet refuse to campaign until the next snapshot or config
//	    change.
//
//	    The hasMemberOtherThan half matters because self-membership alone is not
//	    knowledge of the shard: a configuration naming only this replica would
//	    otherwise let it campaign and win with a quorum of one.
//
//	(c) this replica carries a Removed TOMBSTONE. A committed RemoveNode for self
//	    is equally durable evidence that the committed configuration has been
//	    learned, and it is the one terminal state (b) misses: a join=true replica
//	    that replays its own removal before any leader is known is neither a
//	    member nor following a leader, so without (c) the flag would persist for
//	    the process — and since clearJoiningIfKnown only runs on membership
//	    changes, no later heartbeat would ever clear it.
//
// No signal fires too early: (a) only once the leader is in the configuration,
// (b) only once this replica is in it alongside somebody else, and (c) only once
// its removal is committed. In the post-bootstrap-leader case all three are
// false after the founding entry alone, so the window correctly persists until
// the leader's AddNode arrives.
//
// A snapshot also clears joining unconditionally (Handle's adopt branch), for
// the compacted case. A single-node bootstrap never sets joining, so this is
// only ever reached by a replica added to an existing cluster.
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

// ReleaseGlobalRateLimit returns this shard's entire contribution to the SHARED
// host-wide in-memory limiter. It must be called exactly once when the shard is
// torn down.
//
// The global limiter is one counter for every shard on the host, and each shard
// tracks its own share (inMemory.globalContribution) so it can apply deltas
// without disturbing the others. That share lives on the raft log and dies with
// the Peer, so a shard that simply stops leaves its bytes counted against a
// shard that no longer exists. Repeated StartShard/StopShard cycles accumulate
// those orphans until the total crosses MaxTotalInMemLogSize, at which point
// every propose on the host fails with ErrSystemBusy, permanently and with no
// memory actually held.
//
// Idempotent: the contribution is zeroed, so a second call releases nothing.
func (p *Peer) ReleaseGlobalRateLimit() {
	p.r.log.inmem.setGlobalSize(0)
}

// IsMember reports whether replicaID is in this shard's current configuration
// in any active role (voter, observer, or witness).
//
// This is a view of REPLICATED STATE, not an authorization decision. It must
// never be used to decide whether to accept an inbound message: see IsRemoved
// for why, and use that instead.
//
// Safe for concurrent access from any goroutine. Reads an atomic pointer to an
// immutable snapshot.
func (p *Peer) IsMember(replicaID uint64) bool {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return false
	}
	return snap.isMember(replicaID)
}

// MemberBinding is everything this shard's COMMITTED configuration authorizes
// one replica ID with. It is the input to the receive-side sender gate, and its
// fields are ranked: Identity supersedes Address wherever it is present.
type MemberBinding struct {
	// Identity is the committed cryptographic node identity, or "" when the
	// configuration holds none for this replica (a shard bootstrapped before
	// identities existed, or by an operator who supplied none).
	//
	// Where it is present it is the ONLY authorization input the gate needs.
	// It is derived from the peer's public key rather than matched against
	// certificate SANs, so — unlike the address — a wildcard or shared-SAN
	// certificate does not let its holder present it, and it survives the
	// holder moving to a new address.
	Identity string
	// Address is the committed network address. It remains the authorization
	// input only for a replica with no committed Identity, and is otherwise
	// pure routing metadata.
	Address string
	// Exclusive reports whether Address identifies this replica and no other
	// active member. A certificate is matched on the address alone, so two
	// active replicas at one address let the single host behind it speak as
	// BOTH. It is meaningless once Identity is present — identities are
	// checked for duplication at config-change time and are not matched on an
	// address at all — but it is reported unconditionally so a caller that
	// falls back to the address never has to ask a second question.
	Exclusive bool
}

// MemberBinding returns what this shard's COMMITTED configuration authorizes
// replicaID with, and whether the replica is in the configuration at all.
//
// Unlike IsMember, this IS usable as an authorization input, and it is what the
// inbound message path binds a claimed msg.From to. The distinction is the
// direction of the failure: IsMember as an allow-list makes a LAGGING replica
// reject a legitimate peer (see IsRemoved), whereas requiring the claimed
// sender to match what the configuration committed for it can only ever refuse
// a peer this replica has not yet been told about — and a replica that has not
// been told about a peer has, by construction, never sent that peer a request
// for it to be answering. Everything here arrived through Raft consensus (a
// committed EntryInitialMembership or ConfigChange), which is what makes it a
// source no inbound message can forge.
//
// It allocates nothing, unlike GetMembership, because it is read once per
// inbound quorum-bearing message, and it takes ONE snapshot load so the
// identity and the address it returns can never straddle a membership change.
// Safe for concurrent access from any goroutine: it reads an atomic pointer to
// an immutable snapshot.
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

// IsJoining reports whether THIS replica's configuration is known-incomplete —
// it was started with join=true and has not yet replayed the entries naming the
// rest of the shard. It is a statement about the local replica, never about a
// peer, and it is not an authorization bypass. See the joining field.
//
// Safe for concurrent access from any goroutine.
func (p *Peer) IsJoining() bool {
	return p.joining.Load()
}

// IsRemoved reports whether replicaID carries a committed Removed tombstone in
// this shard's configuration. It is the per-shard authorization predicate for
// INBOUND messages, and it is a DENY-list on purpose.
//
// A replica's configuration is replicated state that LAGS BY DESIGN: a follower
// legitimately has not yet applied the config change that added the current
// leader. Using the member set as an allow-list therefore makes a stale replica
// drop the very messages that would bring it up to date — it rejects the
// leader's Replicate, rejects its Heartbeat, and rejects the InstallSnapshot
// that would repair it, permanently, while the shard silently runs with reduced
// redundancy. Membership cannot be an authorization oracle because it is not
// guaranteed fresh.
//
// The removed set has the opposite character: it is MONOTONE. Entries are only
// ever added, by a committed config change, so a stale replica's failure mode is
// not yet knowing about a removal — which the next committed entry fixes — and
// never wrongly rejecting a legitimate peer. That is the only membership-derived
// fact it is safe to enforce on the receive path.
//
// This matches both reference implementations. etcd checks exactly this and
// nothing else before stepping a message (EtcdServer.Process:
// "cannot process message from removed member"), and its transport comments name
// the lagging-follower case explicitly. hashicorp/raft, which Consul uses,
// performs no sender-membership check on appendEntries at all and applies its
// configuration test only to VOTE requests — which is where our equivalent guard
// lives (see raft.handleVoteRequest).
//
// Authentication proper is the transport's job: mandatory mTLS with a verified
// node certificate plus the deployment ID. This is defence in depth on top of an
// already-authenticated peer.
//
// Safe for concurrent access from any goroutine.
func (p *Peer) IsRemoved(replicaID uint64) bool {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return false
	}
	return snap.isRemoved(replicaID)
}

// IsRemovedIdentity reports whether identity — the AUTHENTICATED node identity
// of a connection, derived from its leaf certificate's public key — is one this
// shard has committed a removal for.
//
// It is the same deny-list as IsRemoved, asked of the one fact about an inbound
// message that its sender cannot choose. IsRemoved alone was never sufficient:
// it is evaluated against the CLAIMED msg.From, and a removed replica still
// holds its private key, so it authenticates normally and evades its own
// tombstone by naming any other replica ID. That evasion is closed here, and
// only here, because only here is the question asked of the connection rather
// than of the message.
//
// Every property that makes IsRemoved safe to enforce on the receive path holds
// identically: the set is MONOTONE — entries are only ever added, by a committed
// config change, and nothing evicts them — so a stale replica's failure mode is
// not yet knowing about a removal, which the next committed entry fixes, and
// never wrongly refusing a legitimate peer. Two facts specific to this one:
//
//   - An identity is bound to ONE replica ID at a time
//     (raft.membership.identityHolder refuses a duplicate at config-change time)
//     and handleRemoveNode moves it out of the active map as it records it here,
//     so a live member's identity can never appear in this set.
//   - The empty identity is never removed, so a connection with no
//     authenticated identity falls through to the gates that can judge it
//     instead of being denied by a tombstone it cannot be told apart from.
//
// RESIDUAL: a shard whose members carry no committed identities records nothing
// here, and its removal deny-list stays what it was — the claimed-From check,
// evadable by naming another ID. There is no replicated fact for such a shard to
// answer this question with. See proto.Membership.RemovedIdentities.
//
// Safe for concurrent access from any goroutine.
func (p *Peer) IsRemovedIdentity(identity string) bool {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return false
	}
	return snap.isRemovedIdentity(identity)
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
		result += fmt.Sprintf("r%d[match=%d next=%d state=%s paused=%v active=%v inflights=%d]",
			rp.ReplicaID, rp.Match, rp.Next, rp.State, rp.Paused, rp.Active, rp.Inflights)
	}
	return result
}

// RemoteProgress reports how far each follower has replicated, as the leader
// currently understands it. It returns nil when this replica is not the leader,
// because only a leader tracks remote progress at all.
//
// This is the structured form of what DiagRemotes renders as text. Answering
// "which follower is behind, and why" was previously possible only by parsing
// that string, which meant the answer was unavailable to metrics, to the public
// ShardInfo, and to anything that is not a human reading a log line.
//
// Caller must hold raftMu (or be the step worker): it reads Peer.r directly.
func (p *Peer) RemoteProgress() []RemoteProgress {
	r := p.r
	if r.state != leader {
		return nil
	}
	out := make([]RemoteProgress, 0, len(r.remotes))
	for id, rm := range r.remotes {
		state := "unknown"
		switch rm.state {
		case remoteProbe:
			state = "probe"
		case remoteReplicate:
			state = "replicate"
		case remoteSnapshot:
			state = "snapshot"
		}
		_, isWitness := r.witnesses[id]
		_, isObserver := r.observers[id]
		out = append(out, RemoteProgress{
			ReplicaID:  id,
			Match:      rm.match,
			Next:       rm.next,
			State:      state,
			Paused:     rm.isPaused(),
			Active:     rm.isActive(),
			Inflights:  rm.inflights.Count(),
			IsWitness:  isWitness,
			IsObserver: isObserver,
		})
	}
	// Deterministic order: map iteration would otherwise reorder the output on
	// every call, which makes both the diagnostic string and any consumer
	// diffing successive samples useless.
	sort.Slice(out, func(i, j int) bool { return out[i].ReplicaID < out[j].ReplicaID })
	return out
}

// RemoteProgress is one follower's replication position as the leader
// understands it. Every field is the leader's local belief, which is what makes
// it useful for diagnosing a stall: a follower stuck in probe with a stale match
// index looks different from one that is simply behind.
type RemoteProgress struct {
	// ReplicaID identifies the follower.
	ReplicaID uint64
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

// hasStateChanged returns true if the hard state has changed since the
// last GetUpdate call. Checks term, vote, and commit index.
//
// It also reports true while a re-persist is owed (see stateUnpersisted).
// HasUpdate is what wakes the step worker; if this returned false there, the
// shard would sit idle with unpersisted hard state until unrelated traffic
// happened to move term, vote or commit — which on an idle follower that has
// just granted a vote is exactly when it matters.
func (p *Peer) hasStateChanged() bool {
	return p.stateUnpersisted ||
		p.r.term != p.prevState.Term ||
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
	// Odd for the duration of the update, even once settled: see leaderSeq.
	p.leaderSeq.Add(1)
	p.leaderID.Store(p.r.leaderID)
	p.term.Store(p.r.term)
	p.leaderSeq.Add(1)
}

// LeaderAndTerm returns the leader ID and the term TOGETHER, as a pair that
// actually co-existed. Callers that report both — OnLeaderUpdated and anything
// deriving a leadership event — must use this rather than LeaderID() and Term()
// separately, which can straddle a syncLeaderID and yield a combination that
// never held.
//
// Safe for concurrent access from any goroutine.
func (p *Peer) LeaderAndTerm() (uint64, uint64) {
	for {
		before := p.leaderSeq.Load()
		if before&1 != 0 {
			// A write is in flight; its window is two stores.
			continue
		}
		leaderID := p.leaderID.Load()
		term := p.term.Load()
		if p.leaderSeq.Load() == before {
			return leaderID, term
		}
	}
}

// Term returns the current raft term. Safe for concurrent access; kept in
// step with LeaderID by syncLeaderID.
func (p *Peer) Term() uint64 {
	return p.term.Load()
}
