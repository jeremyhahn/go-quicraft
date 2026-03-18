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

	// shardID and replicaID identify this peer. Cached from cfg for
	// fast access without going through raft.
	shardID   uint64
	replicaID uint64

	// hasPendingSnapshot tracks whether a snapshot is waiting to be
	// persisted. Set when raft restores a snapshot, cleared after
	// GetUpdate returns it.
	hasPendingSnapshot bool

	// joining is true for nodes that were started with join=true. These
	// nodes have incomplete membership until they receive a snapshot from
	// the leader. Per-shard auth is bypassed while joining is true.
	// Cleared when a snapshot is applied (handleSnapshot updates the
	// membership to the full cluster state).
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
// Must be called before the first Tick.
func (p *Peer) SetProcessedIndex(index uint64) {
	p.r.log.processed = index
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
	err := p.r.step(msg)

	// Per PhD thesis 4.1: config changes take effect on log append,
	// not on commit. When a follower appends config change entries
	// from the leader, update membership and remotes immediately.
	// Guard: only apply if the entries were actually appended (not
	// rejected due to prevLogIndex/prevLogTerm mismatch). handleAppend
	// always returns nil, so err==nil is not sufficient.
	if msg.Type == proto.Replicate && err == nil && len(msg.Entries) > 0 {
		// Per PhD thesis 4.1: "a log entry for a configuration change
		// can be removed (if leadership changes); in this case, a
		// server must be prepared to fall back to the previous
		// configuration in its log." When tryAppend detects a conflict
		// and truncates entries, some of those truncated entries may
		// have been config changes already applied to membership.
		// Rebuild membership from the committed base before applying
		// new config changes from the replacement entries.
		if p.r.logTruncated {
			p.r.logTruncated = false
			p.rebuildMembershipFromLog()
		}
		lastEntry := msg.Entries[len(msg.Entries)-1]
		if p.r.log.matchTerm(lastEntry.Index, lastEntry.Term) {
			p.processConfigChanges(msg.Entries)
		}
	}
	// When a Propose message with config change entries is delivered
	// through the engine inbox (Host.RequestAddNode etc.), the leader's
	// handleLeaderPropose appends the entries but does not apply the
	// membership change. Process them here so membership takes effect
	// immediately on the leader, matching the ProposeConfigChange path.
	if msg.Type == proto.Propose && err == nil && p.IsLeader() && len(msg.Entries) > 0 {
		p.processConfigChanges(msg.Entries)
	}
	if msg.Type == proto.InstallSnapshot && err == nil {
		// Only overwrite membership when the log was fully replaced
		// (non-matching case). A pending snapshot in inmem indicates
		// that handleSnapshot called restore(), meaning the log does
		// NOT match and all entries were discarded. When entries are
		// retained (matching case), they may contain config changes
		// newer than the snapshot's membership, so overwriting would
		// lose those changes.
		if p.r.log.pendingSnapshot() != nil {
			p.membership = newMembership(msg.Snapshot.Membership)
			// Publish immutable snapshot for concurrent readers.
			p.publishMembership()
			// Snapshot delivers the full cluster membership. A joining
			// node now has complete membership knowledge and can enforce
			// per-shard message authorization.
			p.joining.Store(false)
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
func (p *Peer) Propose(entries []proto.Entry) (uint64, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	msg := proto.Message{
		Type:    proto.Propose,
		From:    p.replicaID,
		Entries: entries,
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
// Per Raft thesis Section 4.1: membership takes effect immediately on
// log append, not on commit. The membership struct is updated after the
// entry is successfully appended by the raft state machine.
func (p *Peer) ProposeConfigChange(cc proto.ConfigChange) error {
	if !p.IsLeader() {
		return &NotLeaderError{
			LeaderID: p.leaderID.Load(),
		}
	}

	// Validate the config change before proposing.
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
	}
	err = p.r.step(msg)
	if err != nil {
		return err
	}

	// Apply the config change to membership immediately (Raft Section 4.1).
	// The entry's index was assigned by the raft state machine during
	// handleLeaderPropose; we use the last index in the log.
	entryIndex := p.r.log.lastIndex()
	applyErr := p.membership.apply(cc, entryIndex)
	if applyErr != nil {
		return applyErr
	}

	// Publish immutable snapshot for concurrent readers.
	p.publishMembership()

	// Sync raft remotes from updated membership.
	p.syncRaftRemotes()

	p.syncLeaderID()
	return nil
}

// syncRaftRemotes synchronizes the raft state machine's remote tracking
// maps from the peer's membership. This ensures the raft layer tracks
// the correct set of peers after config changes.
func (p *Peer) syncRaftRemotes() {
	nextIdx := p.r.log.lastIndex() + 1

	// Add new remotes.
	for id := range p.membership.addresses {
		if id == p.replicaID {
			continue
		}
		if _, ok := p.r.remotes[id]; !ok {
			p.r.remotes[id] = newRemote(nextIdx, maxInflightMsgs)
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

	// When the node is leader, rebuild quorum and lease tracking maps
	// so that new voting members' heartbeat acks count correctly.
	if p.r.state == leader {
		p.r.rebuildReplicaToBit()
		p.syncLeaseAcks()
	}
}

// syncLeaseAcks ensures remoteLastAck tracks exactly the current set of
// voting members (remotes + witnesses). New members are added with a zero
// ack tick; departed members are removed. Without this, members added
// during leadership would be silently ignored in lease and ReadIndex
// quorum checks.
func (p *Peer) syncLeaseAcks() {
	if p.r.remoteLastAck == nil {
		return
	}
	for id := range p.r.remotes {
		if _, ok := p.r.remoteLastAck[id]; !ok {
			p.r.remoteLastAck[id] = 0
		}
	}
	for id := range p.r.witnesses {
		if _, ok := p.r.remoteLastAck[id]; !ok {
			p.r.remoteLastAck[id] = 0
		}
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

// rebuildMembershipFromLog rebuilds the membership from the committed base
// (LogReader snapshot membership) plus any config change entries remaining
// in the log. This is called when log truncation overwrites entries that
// may have contained config changes already applied to membership.
//
// Per PhD thesis 4.1: "a log entry for a configuration change can be
// removed (if leadership changes); in this case, a server must be
// prepared to fall back to the previous configuration in its log."
//
// The rebuild scans from firstIndex to lastIndex, which covers both
// LogDB-persisted entries and in-memory entries. This is safe because
// config changes are rare; the scan cost is amortized over many normal
// entries between config changes.
func (p *Peer) rebuildMembershipFromLog() {
	// Start from the committed base membership. This is the membership
	// from the last snapshot (or bootstrap), which is guaranteed to be
	// correct because snapshots are only taken at committed state.
	_, baseMembership := p.logReader.NodeState()
	p.membership = newMembership(baseMembership)

	// Scan all log entries for config changes and replay them.
	firstIdx := p.r.log.firstIndex()
	lastIdx := p.r.log.lastIndex()
	if firstIdx > lastIdx {
		// Empty log after truncation; base membership is correct.
		p.publishMembership()
		p.syncRaftRemotes()
		return
	}

	// Read entries in chunks to avoid loading the entire log at once.
	// Config changes are rare, so most chunks contain zero config
	// changes and the scan is fast.
	const chunkSize uint64 = 1024
	for low := firstIdx; low <= lastIdx; low += chunkSize {
		high := low + chunkSize
		if high > lastIdx+1 {
			high = lastIdx + 1
		}
		entries, err := p.r.log.entries(low, high, 0)
		if err != nil {
			slog.Warn("rebuildMembershipFromLog: failed to read entries",
				"shard_id", p.shardID,
				"replica_id", p.replicaID,
				"low", low,
				"high", high,
				"error", err,
			)
			// Cannot read entries; keep the base membership.
			break
		}
		for i := range entries {
			if entries[i].Type&proto.EntryTypeMask != proto.EntryConfigChange {
				continue
			}
			if len(entries[i].Cmd) == 0 {
				continue
			}
			var cc proto.ConfigChange
			if _, unmarshalErr := cc.UnmarshalFrom(entries[i].Cmd); unmarshalErr != nil {
				slog.Warn("rebuildMembershipFromLog: config change unmarshal failed",
					"shard_id", p.shardID,
					"replica_id", p.replicaID,
					"entry_index", entries[i].Index,
					"error", unmarshalErr,
				)
				continue
			}
			// Validation errors are expected during rebuild: entries
			// may reference already-removed nodes or duplicate adds
			// from the base membership. Log at debug level.
			if applyErr := p.membership.apply(cc, entries[i].Index); applyErr != nil {
				slog.Debug("rebuildMembershipFromLog: config change apply skipped",
					"shard_id", p.shardID,
					"replica_id", p.replicaID,
					"entry_index", entries[i].Index,
					"error", applyErr,
				)
			}
		}
	}

	// Publish the rebuilt membership and sync raft remotes.
	p.publishMembership()
	p.syncRaftRemotes()

	slog.Debug("membership rebuilt from log after truncation",
		"shard_id", p.shardID,
		"replica_id", p.replicaID,
		"first_index", firstIdx,
		"last_index", lastIdx,
		"voters", len(p.membership.addresses),
		"observers", len(p.membership.observers),
		"witnesses", len(p.membership.witnesses),
	)
}

// processConfigChanges scans entries for config changes and applies them
// to the peer's membership. Per PhD thesis 4.1, config changes take effect
// immediately on log append.
func (p *Peer) processConfigChanges(entries []proto.Entry) {
	needsSync := false
	for i := range entries {
		if entries[i].Type&proto.EntryTypeMask != proto.EntryConfigChange {
			continue
		}
		if len(entries[i].Cmd) == 0 {
			continue
		}
		var cc proto.ConfigChange
		if _, err := cc.UnmarshalFrom(entries[i].Cmd); err != nil {
			slog.Error("config change unmarshal failed",
				"shard_id", p.shardID,
				"replica_id", p.replicaID,
				"entry_index", entries[i].Index,
				"entry_type", entries[i].Type,
				"error", err,
			)
			continue
		}
		if err := p.membership.apply(cc, entries[i].Index); err != nil {
			slog.Debug("config change apply skipped",
				"shard_id", p.shardID,
				"replica_id", p.replicaID,
				"entry_index", entries[i].Index,
				"entry_type", entries[i].Type,
				"error", err,
			)
			continue
		}
		needsSync = true
	}
	if needsSync {
		// Publish immutable snapshot for concurrent readers.
		p.publishMembership()
		p.syncRaftRemotes()
	}
}

// ReadIndex requests a linearizable read at the current commit index.
// The request is wrapped in a ReadIndex message and routed through the
// raft state machine, which confirms leadership via heartbeat quorum
// (multi-node) or responds immediately (single-node).
func (p *Peer) ReadIndex(key uint64) error {
	msg := proto.Message{
		Type: proto.ReadIndex,
		From: p.replicaID,
		Hint: key,
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

// DrainReadState extracts pending readStates and outbound messages from
// the raft state machine without performing a full GetUpdate cycle. This
// is designed for the commitPending fast path in the step worker: when a
// commit/apply pipeline is in-flight, the step worker can still process
// incoming HeartbeatResp messages that satisfy ReadIndex quorum, extract
// the resulting readStates, and deliver OnReadyToRead callbacks without
// waiting for the commit pipeline to complete.
//
// Unlike GetUpdate, this method does NOT touch entries, hard state,
// snapshots, or prevState. It only drains messages and readStates,
// which are independent of the commit/apply pipeline state.
//
// The caller must hold raftMu (or otherwise ensure exclusive access).
//
// Returns the drained readStates and messages. Either or both may be nil.
func (p *Peer) DrainReadState() (readStates []proto.ReadyToRead, msgs []proto.Message) {
	if len(p.r.readStates) > 0 {
		readStates = p.r.readStates
		p.r.readStates = nil
	}
	msgs = p.r.drainMessages()
	return readStates, msgs
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
		Type: proto.LeaderTransfer,
		From: p.replicaID,
		Hint: target,
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
	// Advance the processed marker before commitUpdate, which asserts
	// that LastApplied <= processed.
	if len(update.CommittedEntries) > 0 {
		lastProcessed := update.CommittedEntries[len(update.CommittedEntries)-1].Index
		p.r.log.appliedTo(lastProcessed)

		// Process committed config changes. The leader enforces the
		// single-pending invariant (handleLeaderPropose rejects proposals
		// when pendingConfigChange is true), so at most one config change
		// is pending at any given time on the leader. However, a follower
		// catching up via log replication can legitimately receive a batch
		// containing multiple committed config changes that were proposed
		// and committed across separate leader rounds. Process each one
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

	// Record stable snapshot. When a snapshot is persisted (e.g., from an
	// InstallSnapshot received by a joining node), update the LogReader's
	// marker to the snapshot index. Without this, the LogReader still has
	// its initial empty range and subsequent Append calls for entries
	// after the snapshot index will fail with ErrLogGapDetected.
	if !update.Snapshot.IsEmpty() {
		uc.StableSnapshotTo = update.Snapshot.Index
		if err := p.logReader.ApplySnapshot(update.Snapshot); err != nil {
			return err
		}
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

	// Update the log reader's tracked range and term cache so it can
	// serve Term() lookups for entries that are about to be truncated
	// from inmem by commitUpdate → appliedLogTo. Without this, the
	// leader's sendAppend would fail to resolve term(prevLogIndex) for
	// slow followers whose next points to a truncated entry.
	if len(update.EntriesToSave) > 0 {
		if err := p.logReader.Append(update.EntriesToSave); err != nil {
			return err
		}
	}

	p.r.log.commitUpdate(uc)
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
func (p *Peer) MarkJoining() {
	p.joining.Store(true)
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
	// A joining node starts with empty membership. After processing
	// its own AddNode config change, it has only itself as a member.
	// The initial cluster members were set via bootstrap confState
	// (not config change entries), so the joining node's membership
	// is incomplete until a snapshot delivers the full confState.
	// Transport-level mTLS and deployment ID provide security during
	// this bootstrapping window.
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
	return p.r.log.inmem.appliedToTerm
}

// CommittedIndex returns the current committed log index from the Raft log.
// Must only be called from the step worker goroutine (single-owner invariant).
func (p *Peer) CommittedIndex() uint64 {
	return p.r.log.committed
}

// syncLeaderID copies the raft state machine's leaderID to the atomic
// field on Peer. This is called after every operation that may change
// leadership (tick, step, GetUpdate). The raft struct is not thread-safe,
// but the atomic leaderID on Peer provides safe concurrent reads for
// LeaderID() and IsLeader().
func (p *Peer) syncLeaderID() {
	p.leaderID.Store(p.r.leaderID)
}
