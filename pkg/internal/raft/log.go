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
	"log/slog"
	"slices"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/invariant"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// maxEntriesToApplySize is the maximum total size of entries returned
// by entriesToApply. This prevents unbounded memory usage during apply.
//
// It is a FIXED constant and is NOT configurable: no Config field feeds it.
// In particular it is independent of raft.maxReplicationPayload (set from
// Config.MaxApplyEntrySize), which bounds the entry bytes in a single outbound
// Replicate message. The two bound different things — this one an inbound
// apply batch handed to the state machine, that one an outbound wire message —
// so they are deliberately unrelated and changing MaxApplyEntrySize has no
// effect here. The name similarity is historical and has repeatedly misled
// readers into believing MaxApplyEntrySize sizes the apply batch.
const maxEntriesToApplySize uint64 = 64 * 1024 * 1024 // 64MB

// LogReader is the interface for reading from persistent storage (LogDB).
// It provides access to persisted log entries, terms, snapshots, and
// cluster state. Implementations must be safe for concurrent access
// (the LogReader is shared between the step worker and the LogDB writer).
type LogReader interface {
	// GetRange returns entries in [low, high) up to maxSize bytes.
	GetRange(low, high, maxSize uint64) ([]proto.Entry, error)
	// Term returns the term for the entry at index.
	Term(index uint64) (uint64, error)
	// SetRange updates the LogReader to reflect newly available entries
	// starting at firstIndex with the given length. Returns an error
	// if a gap is detected between the existing range and new entries.
	SetRange(firstIndex, length uint64) error
	// SetState updates the persisted hard state.
	SetState(state proto.State)
	// Append records entries as persisted in LogDB. Returns an error
	// if a gap is detected between the existing range and new entries.
	Append(entries []proto.Entry) error
	// NodeState returns the persisted hard state and membership.
	NodeState() (proto.State, proto.Membership)
	// CreateSnapshot records snapshot metadata.
	CreateSnapshot(ss proto.Snapshot) error
	// ApplySnapshot applies a snapshot, updating the range accordingly.
	ApplySnapshot(ss proto.Snapshot) error
	// Snapshot returns the most recent snapshot metadata known to the
	// LogReader. Returns a zero-value Snapshot (Index==0) if no snapshot
	// has been recorded. Used by the raft layer as a fallback when the
	// in-memory snapshot is nil (e.g., for leaders sending InstallSnapshot
	// to followers that are behind the compacted region).
	Snapshot() proto.Snapshot
	// Compact removes entries up to and including the given index.
	Compact(index uint64) error
	// IndexRange returns the first and last available entry indices.
	// Returns (0, 0) when no entries are available.
	IndexRange() (uint64, uint64)
}

// raftLog manages the Raft log by combining in-memory storage with
// persistent LogDB. Entries flow through inmem first, are persisted
// to LogDB by the engine, then truncated from inmem after application.
//
// The raftLog is owned by a single goroutine and requires no
// synchronization.
type raftLog struct {
	logdb     LogReader
	inmem     inMemory
	committed uint64
	processed uint64 // committed entries already returned for apply

	// confChangeIndex is an UPPER BOUND on the index of a config-change entry
	// whose membership effect may not have landed yet. It is maintained
	// incrementally — append() raises it when a config change enters the log,
	// restore() drops it, and it self-clears as processed advances past it —
	// so the "is a config change still pending?" question (raft's
	// hasConfigChangeToApply) is answered in O(1) instead of by re-reading
	// every unapplied entry.
	//
	// The bound is CONSERVATIVE, never optimistic: newRaftLog seeds it with
	// the recovered last index because entries recovered from LogDB were not
	// classified by this process, so their contents are unknown here. That
	// blocks new config-change proposals on a just-restarted leader until it
	// has applied the tail it started with — the fail-closed direction — and
	// releases automatically, with no log reads and therefore no way for a
	// LogDB read error to latch the gate.
	//
	// An over-estimate (a config change that was later truncated away by a
	// conflicting append) is likewise safe and transient: processed advances
	// past the index as the log is applied.
	confChangeIndex uint64

	// pendingAdds is every (replica, identity) a config-carrying entry IN THIS
	// LOG admits, keyed by the entry's index. It is the PENDING configuration
	// the receive gate consults for a sender the committed configuration does
	// not name yet (Peer.PendingMemberIdentity): a replica whose configuration
	// lags — it holds the AddNode that admits the new leader but has not
	// committed it — can admit that leader's Replicate and Heartbeat on the
	// strength of its own log, bound to the identity the entry carries,
	// instead of admitting any certificate holder that names an unknown
	// replica ID.
	//
	// It is exact, not a bound like confChangeIndex: maintained by
	// noteConfigChanges as entries enter the log, trimmed by dropPendingAbove
	// when a conflicting append truncates them, cleared by restore, and seeded
	// from the recovered UNCOMMITTED tail by newRaftLog. Keyed by index rather
	// than by replica so that truncating a later entry for a replica leaves an
	// earlier surviving entry for the same replica in force; the reader takes
	// the highest surviving index per replica above the applied configuration.
	//
	// A RemoveNode is recorded here too (pendingAdd.remove), because adds and
	// removals are folded in log order and the gate must see them in the same
	// order: a removal above an add cancels the add, and a removal above the
	// applied configuration is a pending tombstone the gate refuses BEFORE it
	// consults the committed binding, the pending adds or the anchor. Without
	// it a restart that seeds the committed-but-unfolded stretch would re-admit
	// a member the shard had removed, for as long as the replay took to reach
	// the removal — the hole the pending configuration exists to close.
	pendingAdds map[uint64][]pendingAdd
	// pendingVersion counts every mutation of pendingAdds, so a publisher can
	// tell whether a new snapshot is needed without comparing maps.
	pendingVersion uint64
	// pendingSeeded records that seedPendingAdds has classified the recovered
	// log once; see PublishPendingConfig.
	pendingSeeded bool
}

// pendingAdd is one (replica, identity) a config-carrying entry admits — or,
// with remove set, one replica a RemoveNode entry removes (identity empty:
// the identity it held is whatever the configuration bound below the entry).
type pendingAdd struct {
	replicaID uint64
	identity  string
	remove    bool
}

// pendingView is the pending configuration resolved above an applied
// configuration: the identity each pending add commits, and the replicas a
// pending removal removes, each with the identity the pending adds below it
// bound (empty when the binding is committed, which the reader resolves
// against the committed configuration).
type pendingView struct {
	adds    map[uint64]string
	removed map[uint64]string
}

// noteConfigChanges raises confChangeIndex to cover any config-carrying entry in
// the batch — an incremental EntryConfigChange or the bootstrap
// EntryInitialMembership, both of which define membership — and records in
// pendingAdds every member such an entry admits with a node identity. The type
// mask is required: a compressed entry carries proto.EntryCompressFlag
// alongside its base type.
//
// Including the bootstrap entry in the bound is for consistency with the rest
// of the config-change gating; it is not load-bearing there, because the
// bootstrap entry only ever occupies index 1 and commits before any replica can
// lead, and newRaftLog already seeds confChangeIndex = lastIndex()
// conservatively on recovery. For pendingAdds it IS load-bearing: a replica
// that joined with a partial configuration and replicates entry 1 from the
// leader holds, in that entry, the identities of the members it has not yet
// committed.
//
// A payload that does not decode is skipped here and refused where it matters:
// the propose-time gate never appends one, and the apply path faults the shard
// on it. Recording nothing for it is the safe direction.
func (l *raftLog) noteConfigChanges(entries []proto.Entry) {
	for i := range entries {
		e := &entries[i]
		switch e.Type & proto.EntryTypeMask {
		case proto.EntryConfigChange:
			if e.Index > l.confChangeIndex {
				l.confChangeIndex = e.Index
			}
			var cc proto.ConfigChange
			if _, err := cc.UnmarshalFrom(e.Cmd); err != nil {
				continue
			}
			switch cc.Type {
			case proto.AddNode, proto.AddNonVoting, proto.AddWitness:
				if cc.NodeIdentity != "" {
					l.recordPendingAdd(cc.ReplicaID, cc.NodeIdentity, e.Index)
				}
			case proto.RemoveNode:
				l.recordPendingRemoval(cc.ReplicaID, e.Index)
			}
		case proto.EntryInitialMembership:
			if e.Index > l.confChangeIndex {
				l.confChangeIndex = e.Index
			}
			ms, err := initialMembershipFromEntry(*e)
			if err != nil {
				continue
			}
			// Only an identity that belongs to an ACTIVE role admits anyone.
			// The identities map is sparse and independently keyed, so an
			// entry could name an ID no role holds; recording it would
			// authorize that keypair under an arbitrary replica ID until the
			// entry applied.
			for id, identity := range ms.Identities {
				if identity == "" {
					continue
				}
				if _, ok := ms.Addresses[id]; !ok {
					if _, ok := ms.Observers[id]; !ok {
						if _, ok := ms.Witnesses[id]; !ok {
							continue
						}
					}
				}
				l.recordPendingAdd(id, identity, e.Index)
			}
		}
	}
}

// recordPendingAdd records that the entry at index admits replicaID with
// identity.
func (l *raftLog) recordPendingAdd(replicaID uint64, identity string, index uint64) {
	if l.pendingAdds == nil {
		l.pendingAdds = make(map[uint64][]pendingAdd)
	}
	l.pendingAdds[index] = append(l.pendingAdds[index], pendingAdd{replicaID: replicaID, identity: identity})
	l.pendingVersion++
}

// recordPendingRemoval records that the entry at index removes replicaID.
func (l *raftLog) recordPendingRemoval(replicaID uint64, index uint64) {
	if l.pendingAdds == nil {
		l.pendingAdds = make(map[uint64][]pendingAdd)
	}
	l.pendingAdds[index] = append(l.pendingAdds[index], pendingAdd{replicaID: replicaID, remove: true})
	l.pendingVersion++
}

// dropPendingAbove forgets every pending add recorded from an entry above
// index: those entries are being truncated by a conflicting append, and the
// entries that replace them re-record their own through noteConfigChanges. An
// earlier entry for the same replica, below the cut, stays in force.
func (l *raftLog) dropPendingAbove(index uint64) {
	for at := range l.pendingAdds {
		if at > index {
			delete(l.pendingAdds, at)
			l.pendingVersion++
		}
	}
}

// prunePendingThrough forgets every pending add recorded from an entry at or
// below index: the configuration has been folded through it, so the add is
// either committed membership now or was refused, and neither is pending.
func (l *raftLog) prunePendingThrough(index uint64) {
	for at := range l.pendingAdds {
		if at <= index {
			delete(l.pendingAdds, at)
			l.pendingVersion++
		}
	}
}

// pendingIdentities resolves the pending changes above applied, in log
// order, into one verdict per replica: the identity its highest surviving
// add commits, or its removal. A removal above an add cancels the add; an
// add above a removal (a replica removed and re-added, which the apply path
// refuses for an identity it removed) supersedes it. nil when nothing is
// pending.
//
// Only a COMMITTED removal (index at or below committed) counts. An add is
// admission — fail-open, and an uncommitted one costs nothing if the entry
// is truncated — but a removal is revocation, and revoking a current member
// on an entry consensus has not reached can wedge the shard: in a two-voter
// shard the leader that appended RemoveNode(B) would refuse B's very ack of
// it, so the entry never commits and no quorum can form. The removal binds
// the gate from the moment it is committed, which is the moment it can no
// longer be truncated, until it is folded.
func (l *raftLog) pendingIdentities(applied uint64) *pendingView {
	if len(l.pendingAdds) == 0 {
		return nil
	}
	indexes := make([]uint64, 0, len(l.pendingAdds))
	for at := range l.pendingAdds {
		if at > applied {
			indexes = append(indexes, at)
		}
	}
	if len(indexes) == 0 {
		return nil
	}
	slices.Sort(indexes)
	view := &pendingView{}
	for _, at := range indexes {
		for _, change := range l.pendingAdds[at] {
			if change.remove {
				if at > l.committed {
					continue
				}
				// The identity the removal tombstones is the one a pending
				// add below it bound; a committed binding is resolved by the
				// reader, which sees the committed configuration.
				identity := ""
				if view.adds != nil {
					identity = view.adds[change.replicaID]
					delete(view.adds, change.replicaID)
				}
				if view.removed == nil {
					view.removed = make(map[uint64]string)
				}
				view.removed[change.replicaID] = identity
				continue
			}
			if view.adds == nil {
				view.adds = make(map[uint64]string)
			}
			view.adds[change.replicaID] = change.identity
			if view.removed != nil {
				delete(view.removed, change.replicaID)
			}
		}
	}
	if view.adds == nil && view.removed == nil {
		return nil
	}
	return view
}

// clearPendingAdds forgets every pending add: the log is being replaced by a
// snapshot whose membership becomes the committed configuration.
func (l *raftLog) clearPendingAdds() {
	if len(l.pendingAdds) == 0 {
		return
	}
	l.pendingAdds = nil
	l.pendingVersion++
}

// pendingSeedBudget bounds the bytes seedPendingAdds reads from the COMMITTED
// tail at construction. The read blocks StartShard, and a shard that never
// snapshots has a committed tail that is its whole log; reading all of it on
// every restart is the StartShard-latency regression
// reconstructShardMembership was rewritten to avoid. Within the budget the
// restart gate is exactly as wide as it was before the restart; past it the
// unfolded remainder is left to the replay, and the gate is narrower until
// the replay reaches it. A variable only so a test can lower it.
var pendingSeedBudget uint64 = 64 << 20

// seedPendingAdds classifies the recovered log above the FOLDED configuration
// once, at construction, so the receive gate admits after the restart what it
// admitted before it: every config change this replica holds that the
// configuration it applied does not yet reflect.
//
// Two stretches of the log qualify. The uncommitted tail (committed,
// lastIndex] — a config change appended but not yet committed when the
// replica went down — is read whole; it is bounded by the replication
// window. The committed-but-unfolded stretch (applied, committed] — a
// snapshot's configuration predates the AddNode that admitted the current
// leader, and the replay that folds it is paced on the state machine — is
// read up to pendingSeedBudget: without it a follower restarting from that
// snapshot refuses its own leader (unknown_member) until the replay reaches
// the entry, which a slow on-disk state machine with a long backlog can
// stretch past an election timeout. Whatever the first PublishPendingConfig
// finds already folded, it prunes, so seeding a committed entry costs
// nothing when the replay is ahead.
//
// The reader caps any one read at its slice limit and returns the short
// result as success, so each stretch is read from the last index seen until
// it is exhausted. A read failure is logged and the rest left unclassified,
// which only costs the admission this seed provides.
//
// It runs once, from the first PublishPendingConfig, above the configuration
// applied by then: for an on-disk state machine that is what
// SetProcessedIndex folded, which is often the whole committed stretch, and
// reading it again here would cost StartShard the read for no gate benefit.
func (l *raftLog) seedPendingAdds(applied uint64) {
	if l.pendingSeeded {
		return
	}
	l.pendingSeeded = true
	last := l.lastIndex()
	from := max(applied, l.processed)
	if last <= from {
		return
	}
	// The committed-but-unfolded stretch, under the budget.
	var read uint64
	next := from + 1
	for next <= l.committed && next <= last {
		if read >= pendingSeedBudget {
			slog.Warn("the recovered committed tail exceeds the pending-config seed budget; "+
				"a config change above the budget admits its member only once the replay folds it",
				"from", next,
				"committed", l.committed,
				"budget_bytes", pendingSeedBudget)
			break
		}
		entries, err := l.entries(next, min(l.committed, last)+1, pendingSeedBudget-read)
		if err != nil || len(entries) == 0 {
			slog.Warn("could not classify the recovered committed tail for pending config changes",
				"from", next,
				"to", l.committed,
				"error", err)
			break
		}
		l.noteConfigChanges(entries)
		for i := range entries {
			read += uint64(entries[i].Size())
		}
		next = entries[len(entries)-1].Index + 1
	}
	// The uncommitted tail, whole.
	for next = max(next, l.committed+1); next <= last; {
		entries, err := l.entries(next, last+1, 0)
		if err != nil || len(entries) == 0 {
			slog.Warn("could not classify the recovered uncommitted tail for pending config changes",
				"from", next,
				"to", last,
				"error", err)
			return
		}
		l.noteConfigChanges(entries)
		next = entries[len(entries)-1].Index + 1
	}
}

// newRaftLog creates a new raftLog backed by the given LogReader.
//
// Initialization follows dragonboat's pattern:
//  1. inmem is initialized with the LogDB's last index so markerIndex
//     correctly reflects where in-memory entries begin. This is critical
//     for restart: the LogDB may contain entries 1..N that need re-apply,
//     and inmem.markerIndex must be N+1 (not committed+1) so that
//     firstIndex() returns the correct LogDB boundary.
//  2. committed is set from the persisted hard state so raft knows which
//     entries are safe to apply.
//  3. processed is set to the snapshot index (or firstIndex-1 if no
//     snapshot) so entries between the snapshot/start and committed are
//     re-applied to the state machine on restart.
func newRaftLog(logdb LogReader) *raftLog {
	state, _ := logdb.NodeState()
	ss := logdb.Snapshot()
	firstIdx, lastIdx := logdb.IndexRange()

	// Use LogDB's last index for the inmem boundary, matching dragonboat.
	//
	// The boundary must describe what the log ACTUALLY holds, and the only
	// two things that establish that are recovered entries and a recovered
	// snapshot. Deriving it from state.Commit — the previous fallback when
	// lastIdx==0 — made the clamp below VACUOUS in the exact case the clamp
	// exists for: newInMemory(state.Commit) sets markerIndex = state.Commit+1,
	// so lastIndex() falls through to inmem.lastIndex() == state.Commit and
	// `committed > lastIndex()` compares state.Commit against itself. A
	// replica whose entry records were lost while its state record survived
	// (disk wipe, partial restore, truncated WAL) came up claiming
	// committed==state.Commit over an EMPTY log, and getConflictIndex skips
	// every index at or below committed as "cannot conflict" — so it ACKed
	// indices it holds nothing for and the leader counted that phantom match
	// toward quorum.
	//
	// A snapshot's index IS a real lower bound on the log's extent: every
	// index at or below it is captured by the snapshot.
	//
	// The max has effect in EXACTLY ONE case: lastIdx == 0, i.e. the LogDB
	// holds no entries. lastIndex() prefers logdb.IndexRange() whenever it is
	// non-zero and only falls through to inmem.lastIndex() when it is zero, so
	// with any recovered entry range this seed is never read back and the max
	// changes nothing (a snapshot record NEWER than a surviving entry range is
	// therefore still clamped down to that entry range by the clamp below —
	// the boundary seed does not protect against it).
	//
	// That last case is NOT reachable through recovery: recoverLogReader calls
	// LogReader.ApplySnapshot before SetState, which moves the reader's marker
	// to ss.Index, so IndexRange — and hence lastIndex() — never reports a last
	// index BELOW a recovered snapshot. Probed directly: entries 1..5 with a
	// snapshot record at 10 yields lastIndex()==10 and committed stays 10.
	// The clamp below therefore cannot pull committed under a snapshot index
	// that is committed by construction.
	//
	// The one case it does cover is the snapshot-only replica: a replica whose
	// entries were all compacted away, or lost, while its snapshot record
	// survived. Seeding the boundary from ss.Index makes inmem.lastIndex() —
	// and hence lastIndex() — report the snapshot index instead of 0, so the
	// clamp below does not read the log as EMPTY and reset a replica that
	// legitimately holds state through ss.Index back to a fresh one.
	inmemStart := lastIdx
	if ss.Index > inmemStart {
		inmemStart = ss.Index
	}

	// processed = snapshot index, or firstIndex-1 if no snapshot.
	// This ensures all entries from the snapshot (or log start) to
	// committed are re-applied on restart.
	processed := ss.Index
	if processed == 0 && firstIdx > 0 {
		processed = firstIdx - 1
	}

	// A snapshot's index is COMMITTED by construction — a snapshot is only
	// ever taken over committed state — so it is a lower bound on committed
	// regardless of what HardState survived. Without this, restoring from a
	// snapshot with a lost or lagging HardState.Commit comes up with
	// processed > committed, which violates commitUpdate's assertion on the
	// very first commit and takes the commit worker down.
	committed := state.Commit
	if ss.Index > committed {
		committed = ss.Index
	}

	l := &raftLog{
		logdb:     logdb,
		inmem:     newInMemory(inmemStart),
		committed: committed,
		processed: processed,
	}

	// Clamp DOWN to what the log actually holds. A recovered
	// HardState.Commit ahead of the last recovered entry (a torn batch that
	// kept the state record but lost entries behind it, or a snapshot record
	// that outlived them) would otherwise be trusted absolutely:
	// getConflictIndex skips every entry at or below committed as
	// "cannot conflict", so tryAppend reports success for indices this
	// replica does not have, handleAppend sends a NON-rejecting
	// ReplicateResp for them, and the leader counts that phantom match
	// toward quorum -- committing entries a majority does not hold. The
	// invariant assertion in commitTo would eventually panic, but only after
	// the false acknowledgement had already gone out.
	if last := l.lastIndex(); l.committed > last {
		if last == 0 {
			// TOTAL loss: the state record survived but the log holds
			// nothing at all — no entries and no snapshot. Clamping to 0
			// makes this replica indistinguishable from a fresh one, which
			// is exactly right: it re-learns its whole log, and its commit
			// index, from the leader. It is also the only state in which
			// the replica has NOTHING to prove any of its prior
			// acknowledgements with, so it is logged at ERROR rather than
			// WARN — the shard is running one replica short of its
			// nominal durability until the leader refills it, and only an
			// operator can tell a wiped volume from a restore that was
			// never finished.
			slog.Error("recovered commit index over an EMPTY log: entry records were lost while the state record survived; clamping to 0 and re-learning the log from the leader",
				"recovered_commit", l.committed,
				"recovered_term", state.Term,
				"snapshot_index", ss.Index)
		} else {
			slog.Warn("recovered commit index exceeds last log entry, clamping",
				"recovered_commit", l.committed,
				"last_index", last,
				"snapshot_index", ss.Index)
		}
		l.committed = last
	}

	// processed must never exceed committed, on ANY recovery shape — not only
	// the one the clamp above fires on. `processed` is seeded from ss.Index,
	// or from firstIdx-1 when there is no snapshot, and neither is bounded by
	// the recovered HardState.Commit. A log recovered with a gapped start and
	// a lower commit (firstIndex 100, Commit 50) therefore comes up with
	// processed 99 > committed 50, and handleSnapshot's matching branch would
	// then call appliedTo(ss.Index) for an index below processed, tripping
	// appliedTo's monotonicity assertion and taking the step worker down.
	//
	// Not reachable through recovery today: an entry range starting above 1
	// with no snapshot record makes LogReader.setRangeLocked return
	// ErrLogGapDetected, so recoverLogReader fails and the shard never starts.
	// Clamped unconditionally anyway — the seed and the clamp are three lines
	// apart and the invariant they maintain is asserted at a panic, so it
	// should not depend on a reachability argument made in a different file.
	// Found by FuzzRaftLogRecovery.
	if l.processed > l.committed {
		l.processed = l.committed
	}

	// Seed the config-change bound conservatively over the recovered range.
	// Nothing in this process classified those entries, so any of them may be
	// an unapplied config change; assuming the worst blocks new config-change
	// proposals until the recovered tail has been applied, and costs nothing
	// once it has (see confChangeIndex).
	l.confChangeIndex = l.lastIndex()
	// The pending configuration is seeded by the first PublishPendingConfig,
	// not here: an on-disk state machine's SetProcessedIndex runs between
	// NewPeer and the node's first publish and folds the committed stretch
	// itself, and seeding above what it folded reads only what the gate can
	// use. See seedPendingAdds.
	return l
}

// lastIndex returns the index of the last log entry. Checks in-memory
// entries first, then falls back to LogDB, then to inmem.lastIndex().
func (l *raftLog) lastIndex() uint64 {
	if idx, ok := l.inmem.getLastIndex(); ok {
		return idx
	}
	if _, lastIdx := l.logdb.IndexRange(); lastIdx > 0 {
		return lastIdx
	}
	return l.inmem.lastIndex()
}

// firstIndex returns the index of the first available log entry.
// Checks for an in-memory snapshot first, then falls back to LogDB,
// then to inmem.markerIndex as the last resort.
func (l *raftLog) firstIndex() uint64 {
	if idx, ok := l.inmem.getSnapshotIndex(); ok {
		return idx + 1
	}
	if firstIdx, _ := l.logdb.IndexRange(); firstIdx > 0 {
		return firstIdx
	}
	return l.inmem.firstIndex()
}

// lastTerm returns the term of the last log entry, and whether it could be
// determined at all.
//
// The distinction matters: term 0 is Raft's "no term" sentinel, and a
// fabricated 0 is dangerous in BOTH directions. Fed to isUpToDate as our own
// last term it makes every candidate look up to date, so a replica holding a
// committed entry no one else has would vote for a candidate missing it -- a
// Leader Completeness violation. Advertised in a campaign it makes this node
// look empty, so every peer rejects it and it can never win. The LogReader
// deliberately refuses to fabricate a zero term for exactly this reason
// (logdb/reader.go termLocked); this layer must not undo that.
func (l *raftLog) lastTerm() (uint64, bool) {
	t, err := l.term(l.lastIndex())
	if err != nil {
		return 0, false
	}
	return t, true
}

// term returns the term at the given index. Checks in-memory first,
// then falls back to LogDB.
//
// Errors from the LogDB layer (logdb.ErrCompacted, logdb.ErrUnavailable)
// are translated to raft-package sentinels so that all callers within the
// raft package see a single, consistent error type. Without this
// translation, getConflictIndex and tryCommit fail to recognize compacted
// entries from the real LogReader and produce false conflicts or missed
// commits.
//
// The LogDB fallback is bounded ABOVE by lastIndex(), because the LogReader is
// not authoritative there and lags in the unsafe direction. A conflicting
// append (tryAppend -> inMemory.merge) and a snapshot restore both DISCARD the
// in-memory suffix immediately, while the LogReader only learns of the
// truncation a whole commit/apply/fsync cycle later, when the commit worker
// reaches Peer.Commit -> logReader.Append. Until then the reader still serves
// the superseded terms, and every message the step worker drains from the inbox
// is handled inside that window.
//
// Unbounded, the fallback contradicts lastIndex(), which already prefers the
// in-memory tail whenever the buffer is non-empty. matchTerm inherits the
// contradiction: a delayed or duplicated Replicate whose prevLogIndex sits in
// the discarded suffix passes the Log Matching check against an entry this
// replica has thrown away. getConflictIndex then reports the first genuinely
// new entry as the conflict, tryAppend hands it to inMemory.merge past the
// in-memory tail, and merge asserts (a hole in the log) -- taking the step
// worker down on a message a PEER chose, which is an availability primitive.
// Were the buffer empty instead of non-empty, merge would open that hole
// silently and the replica would ACK indices it holds nothing for.
func (l *raftLog) term(index uint64) (uint64, error) {
	// Check in-memory first.
	if t, ok := l.inmem.term(index); ok {
		return t, nil
	}
	// Nothing above the end of the log has a term; see above for why the
	// LogReader may still claim otherwise. Reporting it as unavailable is what
	// handleAppend's shorter-log branch already expects, so the leader gets the
	// lastIndex hint and backtracks instead of being falsely acknowledged.
	if index > l.lastIndex() {
		return 0, ErrUnavailable
	}
	// Fall back to LogDB.
	t, err := l.logdb.Term(index)
	if err != nil {
		return 0, translateLogDBError(err)
	}
	return t, nil
}

// entries returns log entries in the range [low, high) up to maxSize
// bytes total. Combines entries from LogDB and in-memory storage.
func (l *raftLog) entries(low, high, maxSize uint64) ([]proto.Entry, error) {
	if low > high {
		return nil, ErrUnavailable
	}
	if low == high {
		return nil, nil
	}
	last := l.lastIndex()
	if low > last+1 {
		return nil, ErrUnavailable
	}
	if high > last+1 {
		high = last + 1
	}

	// Try to get entries from LogDB first (entries below inmem.markerIndex).
	var result []proto.Entry
	if low < l.inmem.markerIndex {
		upperBound := high
		if upperBound > l.inmem.markerIndex {
			upperBound = l.inmem.markerIndex
		}
		dbEntries, err := l.logdb.GetRange(low, upperBound, maxSize)
		if err != nil {
			return nil, translateLogDBError(err)
		}
		// Copy entries into a caller-owned slice. GetRange returns
		// entries from a reuse buffer (LogReader.protoEntryBuf) that
		// is overwritten on the next call. The returned entries may
		// be placed into outbound messages and serialized asynchronously
		// by the transport send goroutine, so they must not alias the
		// reuse buffer.
		owned := make([]proto.Entry, len(dbEntries))
		copy(owned, dbEntries)
		result = owned
		// If we didn't get all requested from LogDB, return what we have.
		if uint64(len(dbEntries)) < upperBound-low {
			return limitEntrySize(result, maxSize), nil
		}
	}

	// Get entries from in-memory.
	if high > l.inmem.markerIndex {
		lowerBound := low
		if lowerBound < l.inmem.markerIndex {
			lowerBound = l.inmem.markerIndex
		}
		inmemEntries, err := l.inmem.getEntries(lowerBound, high)
		if err != nil {
			return nil, err
		}
		if len(inmemEntries) > 0 {
			if len(result) > 0 {
				result = append(result, inmemEntries...)
			} else {
				result = inmemEntries
			}
		}
	}

	return limitEntrySize(result, maxSize), nil
}

// matchTerm checks if the entry at index has the given term.
func (l *raftLog) matchTerm(index, term uint64) bool {
	t, err := l.term(index)
	if err != nil {
		return false
	}
	return t == term
}

// firstIndexOfTerm returns the first log index that has the given term.
// Uses binary search over the monotonically non-decreasing term sequence
// to find the earliest entry with the specified term in O(log N) time.
// If no entry with the given term exists or the term lookup encounters
// an error (e.g., compacted entries), it returns 0.
// This is used for fast log backtracking (Raft Section 5.3) so the leader
// can skip entire conflicting terms rather than one index per round-trip.
func (l *raftLog) firstIndexOfTerm(targetTerm uint64) uint64 {
	first := l.firstIndex()
	last := l.lastIndex()
	if first > last {
		return 0
	}

	// Binary search for the leftmost index where term(idx) >= targetTerm.
	lo, hi := first, last
	for lo < hi {
		mid := lo + (hi-lo)/2
		t, err := l.term(mid)
		if err != nil {
			// Cannot read this entry (compacted or unavailable).
			// Fall back to scanning above the error boundary.
			lo = mid + 1
			continue
		}
		if t < targetTerm {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	// Verify the candidate actually has the target term.
	t, err := l.term(lo)
	if err != nil || t != targetTerm {
		return 0
	}
	return lo
}

// lastIndexOfTerm returns the last log index that has the given term.
// Uses binary search over the monotonically non-decreasing term sequence
// to find the latest entry with the specified term in O(log N) time.
// If no entry with the given term exists or the term lookup encounters
// an error, it returns 0.
// This is used for fast log backtracking (Raft Section 5.3) on the leader
// side to find where its entries at the conflicting term end.
func (l *raftLog) lastIndexOfTerm(targetTerm uint64) uint64 {
	first := l.firstIndex()
	last := l.lastIndex()
	if first > last {
		return 0
	}

	// Binary search for the rightmost index where term(idx) <= targetTerm.
	lo, hi := first, last
	for lo < hi {
		mid := lo + (hi-lo+1)/2 // ceil to avoid infinite loop when lo+1==hi
		t, err := l.term(mid)
		if err != nil {
			// Cannot read this entry (compacted or unavailable).
			// Shrink the search space below the error boundary.
			hi = mid - 1
			continue
		}
		if t <= targetTerm {
			lo = mid
		} else {
			hi = mid - 1
		}
	}

	// Verify the candidate actually has the target term.
	t, err := l.term(lo)
	if err != nil || t != targetTerm {
		return 0
	}
	return lo
}

// isUpToDate reports whether the given (lastIndex, lastTerm) is at least as
// up-to-date as this log, and whether that question could be ANSWERED at all.
// The Raft paper defines "up-to-date" as: the candidate's last log entry has a
// higher term, or the same term with an equal or higher index.
//
// The second return value separates "this candidate is behind us" from "we
// cannot tell". Both deny the vote, but they are completely different
// operational conditions: the first is ordinary Raft, the second is a storage
// fault on THIS replica. Collapsing them into a single false was silent — if
// the fault hits a majority the shard simply cannot elect, and the only
// observable is a leaderless shard with quorum apparently intact. Callers
// report the second case; see handleVoteRequest.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) (upToDate, readable bool) {
	ourLastTerm, ok := l.lastTerm()
	if !ok {
		// Our own last term is unreadable (a compaction race, a torn tail, a
		// closed LogDB). We cannot prove the candidate is at least as
		// up-to-date as us, so we must not grant. Failing OPEN here -- the
		// previous behaviour, via a fabricated term 0 -- meant granting to
		// every candidate including one missing entries we have committed.
		return false, false
	}
	if lastTerm > ourLastTerm {
		return true, true
	}
	if lastTerm == ourLastTerm {
		return lastIndex >= l.lastIndex(), true
	}
	return false, true
}

// tryAppend attempts to append entries after prevLogIndex/prevLogTerm.
// It verifies that the entry at prevLogIndex has prevLogTerm (log matching
// property). If successful, returns the index of the last new entry, true,
// and nil. If the terms don't match, returns 0, false, nil. If conflict
// detection encounters a compacted or unavailable entry, returns the error
// so the caller can drop the message (the leader will retransmit).
func (l *raftLog) tryAppend(prevLogIndex, prevLogTerm uint64, ents []proto.Entry) (lastIndex uint64, ok bool, truncated bool, err error) {
	if !l.matchTerm(prevLogIndex, prevLogTerm) {
		return 0, false, false, nil
	}
	// Reject non-contiguous batches before any log mutation: the offset
	// arithmetic below and inMemory.merge both assume ents[i].Index ==
	// prevLogIndex+1+i. A gapped batch (buggy or corrupted sender) would
	// otherwise corrupt the in-memory log; persistence-time gap detection
	// only fires after the damage is done. Returning an error drops the
	// message without a response, so a healthy leader simply retries.
	for i := range ents {
		if ents[i].Index != prevLogIndex+1+uint64(i) {
			return 0, false, false, ErrGappedEntryBatch
		}
	}
	// Find the first conflicting entry.
	conflictIdx, cerr := l.getConflictIndex(ents)
	if cerr != nil {
		return 0, false, false, cerr
	}
	if conflictIdx == 0 {
		// No conflict: all entries already match. Return the last
		// index covered by the append.
		lastNew := prevLogIndex + uint64(len(ents))
		return lastNew, true, false, nil
	}
	invariant.Assert(conflictIdx > l.committed,
		"conflict index must be greater than committed index")
	// Detect whether existing entries are being overwritten. The
	// conflict index points to the first entry that doesn't match.
	// If it is at or before the current lastIndex, an existing entry
	// has a different term and is being replaced. If it is beyond
	// lastIndex, the "conflict" is just ErrUnavailable (new entries
	// beyond the log end), which is a simple extension, not a
	// truncation. Only real overwrites require membership rebuild.
	isOverwrite := conflictIdx <= l.lastIndex()
	if isOverwrite {
		// The entries from conflictIdx on are gone; whatever they admitted
		// is no longer in this log. The replacements re-record their own.
		l.dropPendingAbove(conflictIdx - 1)
	}
	// Append from the conflict point onward.
	offset := conflictIdx - ents[0].Index
	l.append(ents[offset:])
	lastNew := ents[len(ents)-1].Index
	return lastNew, true, isOverwrite, nil
}

// append directly appends entries to the log via the in-memory layer.
// Returns the last index after appending.
func (l *raftLog) append(entries []proto.Entry) uint64 {
	if len(entries) == 0 {
		return l.lastIndex()
	}
	invariant.Assert(entries[0].Index > l.committed,
		"cannot append entries at or below committed index")
	// Every path that puts an entry into the log runs through here (leader
	// propose, follower append via tryAppend, the leadership no-op), so this
	// is the one place that has to notice a config change.
	l.noteConfigChanges(entries)
	l.inmem.merge(entries)
	return l.lastIndex()
}

// tryCommit updates committed to toCommit if the entry at toCommit has
// the given term. This ensures the leader only commits entries from its
// own term (Raft safety property).
func (l *raftLog) tryCommit(toCommit, term uint64) bool {
	if toCommit <= l.committed {
		return false
	}
	t, err := l.term(toCommit)
	if errors.Is(err, ErrCompacted) {
		t = 0
	} else if err != nil {
		return false
	}
	if t == term {
		l.commitTo(toCommit)
		return true
	}
	return false
}

// commitTo advances the commit index.
func (l *raftLog) commitTo(toCommit uint64) {
	if toCommit <= l.committed {
		return
	}
	invariant.Assert(toCommit <= l.lastIndex(),
		"commitTo: toCommit exceeds lastIndex")
	l.committed = toCommit
}

// appliedTo updates the processed (applied) index.
func (l *raftLog) appliedTo(to uint64) {
	if to == 0 {
		return
	}
	invariant.Assert(to <= l.committed,
		"appliedTo: applied index exceeds committed index")
	invariant.Assert(to >= l.processed,
		"appliedTo: applied index moves backwards")
	l.processed = to
}

// hasEntriesToApply returns true if there are committed but not yet
// processed entries.
func (l *raftLog) hasEntriesToApply() bool {
	return l.committed > l.processed
}

// firstNotAppliedIndex returns the first index that hasn't been applied.
func (l *raftLog) firstNotAppliedIndex() uint64 {
	fi := l.firstIndex()
	p := l.processed + 1
	if p > fi {
		return p
	}
	return fi
}

// savedIndex returns the highest log index durably persisted to LogDB.
// Committed entries above this marker are committed but not yet crash-safe:
// the piggyback apply path must not apply them nor advertise a
// HardState.Commit that covers them, or a crash could leave HardState.Commit
// ahead of the last durable log entry (see GetPiggybackUpdate).
func (l *raftLog) savedIndex() uint64 {
	return l.inmem.savedTo
}

// entriesToApply returns committed but not yet processed entries,
// limited to maxEntriesToApplySize bytes. Entries are copied into a
// pooled slice to reduce GC pressure on the apply hot path. The
// returned pooled pointer must be passed to proto.PutEntrySlice after
// the entries are consumed.
func (l *raftLog) entriesToApply() ([]proto.Entry, *[]proto.Entry, error) {
	return l.entriesToApplyUpTo(l.committed)
}

// entriesToApplyUpTo returns committed but not yet processed entries whose
// index does not exceed ceiling, limited to maxEntriesToApplySize bytes.
// A ceiling below committed lets a caller withhold entries that are committed
// but not yet durable (see savedIndex); the withheld tail is offered again on
// a later cycle once it becomes durable. Ownership semantics match
// entriesToApply: entries are copied into a pooled slice whose pointer must be
// passed to proto.PutEntrySlice after the entries are consumed.
func (l *raftLog) entriesToApplyUpTo(ceiling uint64) ([]proto.Entry, *[]proto.Entry, error) {
	if !l.hasEntriesToApply() {
		return nil, nil, nil
	}
	low := l.firstNotAppliedIndex()
	high := l.committed + 1
	if ceiling+1 < high {
		high = ceiling + 1
	}
	if low >= high {
		// Everything at or below the ceiling has already been applied;
		// the remainder is committed but not yet eligible.
		return nil, nil, nil
	}
	entries, err := l.entries(low, high, maxEntriesToApplySize)
	if err != nil || len(entries) == 0 {
		return entries, nil, err
	}
	// Copy entries into a pooled slice. The entries() method may return
	// a sub-slice of the inmem entries array (which is owned by the log
	// and can be compacted), or a freshly allocated slice from LogDB.
	// Copying into a pooled slice provides uniform ownership semantics:
	// the caller owns the data and the pool reclaims the allocation
	// after apply completes, avoiding per-GetUpdate heap allocations.
	pooled := proto.GetEntrySlice()
	if cap(*pooled) < len(entries) {
		*pooled = make([]proto.Entry, len(entries))
	} else {
		*pooled = (*pooled)[:len(entries)]
	}
	copy(*pooled, entries)
	return *pooled, pooled, nil
}

// entriesToSave returns entries that haven't been persisted to LogDB yet.
func (l *raftLog) entriesToSave() []proto.Entry {
	return l.inmem.entriesToSave()
}

// pendingSnapshot returns the pending snapshot from inmem, if any.
// This is a snapshot received via InstallSnapshot that has not yet been
// persisted. Used by GetUpdate to include in the Update for the commit
// worker to persist and register with the LogReader.
func (l *raftLog) pendingSnapshot() *proto.Snapshot {
	return l.inmem.snapshot
}

// snapshot returns the latest known snapshot, checking inmem first then
// falling back to the LogReader's persisted snapshot. Used by sendSnapshot
// to find a snapshot to send to followers behind the compacted region.
func (l *raftLog) snapshot() *proto.Snapshot {
	if l.inmem.snapshot != nil {
		return l.inmem.snapshot
	}
	ss := l.logdb.Snapshot()
	if ss.Index == 0 {
		return nil
	}
	return &ss
}

// restore resets the log from a snapshot, discarding all in-memory state.
func (l *raftLog) restore(ss proto.Snapshot) {
	invariant.Assert(ss.Index >= l.committed,
		"restore: snapshot index must not be less than committed")
	l.inmem.restore(ss)
	l.committed = ss.Index
	l.processed = ss.Index
	// The snapshot carries the configuration as of ss.Index, and Peer.Handle
	// adopts it, so every config change at or below ss.Index has taken effect
	// and every entry above it was just discarded. Nothing is pending.
	l.confChangeIndex = 0
	l.clearPendingAdds()
}

// getConflictIndex finds where the given entries first conflict with the
// existing log. A conflict exists when an entry has the same index but a
// different term. Returns 0 if all entries match or extend the log.
//
// Entries at or below the committed index are skipped because committed
// entries are guaranteed to have matching terms across all replicas (Raft
// safety property). Any apparent term mismatch at a committed index is
// a transient artifact (e.g., stale retransmission after leader change
// within the same inbox drain batch, or a concurrent term cache miss)
// and must not be treated as a real conflict.
//
//nolint:unparam // Error is always nil but signature kept for consistency with tryAppend
func (l *raftLog) getConflictIndex(entries []proto.Entry) (uint64, error) {
	for i := range entries {
		// Committed entries cannot conflict. The Raft safety property
		// guarantees that all replicas agree on committed entries'
		// terms. Skip them to avoid false conflicts from stale
		// retransmissions or transient term lookup inconsistencies.
		if entries[i].Index <= l.committed {
			continue
		}
		t, err := l.term(entries[i].Index)
		if err != nil {
			if errors.Is(err, ErrCompacted) {
				// Entry was compacted away — it was committed and
				// persisted, so it cannot conflict. Skip it.
				continue
			}
			// ErrUnavailable: entry is beyond our log. This is the
			// first genuinely new entry that needs to be appended.
			// Report it as a conflict so tryAppend appends from here.
			return entries[i].Index, nil
		}
		if t != entries[i].Term {
			return entries[i].Index, nil
		}
	}
	return 0, nil
}

// commitUpdate processes a persistence notification from the engine,
// updating the savedTo marker and applied state. The Processed flag
// is a bool indicating the update was fully processed; the LastApplied
// field carries the actual applied index for inmem truncation.
func (l *raftLog) commitUpdate(up proto.UpdateCommit) {
	l.inmem.commitUpdate(up)
	if up.LastApplied > 0 {
		invariant.Assert(up.LastApplied <= l.committed,
			"commitUpdate: last applied exceeds committed")
		invariant.Assert(up.LastApplied <= l.processed,
			"commitUpdate: last applied exceeds processed")
		l.inmem.appliedLogTo(up.LastApplied)
	}
}

// translateLogDBError maps logdb-layer sentinel errors to their
// raft-package equivalents. The LogReader (logdb.LogReader) returns
// logdb.ErrCompacted and logdb.ErrUnavailable, but callers within the
// raft package check for raft.ErrCompacted and raft.ErrUnavailable.
// Without this translation, errors.Is comparisons silently fail, causing
// getConflictIndex to treat compacted entries as new entries (false
// conflict at committed index) and tryCommit to miss the compacted case.
func translateLogDBError(err error) error {
	if errors.Is(err, logdb.ErrCompacted) {
		return ErrCompacted
	}
	if errors.Is(err, logdb.ErrUnavailable) {
		return ErrUnavailable
	}
	return err
}

// limitEntrySize limits the total byte size of entries to maxSize.
// It always returns at least one entry if the slice is non-empty.
func limitEntrySize(entries []proto.Entry, maxSize uint64) []proto.Entry {
	if len(entries) == 0 || maxSize == 0 {
		return entries
	}
	total := uint64(entries[0].Size())
	var count int
	for count = 1; count < len(entries); count++ {
		total += uint64(entries[count].Size())
		if total > maxSize {
			break
		}
	}
	return entries[:count]
}

// limitEntryCount limits the number of entries in a single outbound message to
// proto.MaxEntriesPerMessage.
//
// maxReplicationPayload (Config.MaxApplyEntrySize) bounds a Replicate message
// in BYTES only, so a follower catching up on small entries — ~16MB of 68-byte
// entries is ~245,000 of them — produces a message that exceeds the wire's
// entry-count limit. Every peer's decoder rejects such a message
// (proto.ErrTooManyMessages) and resets the stream it arrived on, the leader
// cannot advance that follower's next index, and it rebuilds the identical
// message on the next replicate cycle: the follower is starved and the link
// churns. The count bound must therefore be applied wherever the byte bound is.
//
// Truncation costs nothing: sendAppend pipelines, so the remaining entries go
// out in the following message of the same cycle.
func limitEntryCount(entries []proto.Entry) []proto.Entry {
	if len(entries) > proto.MaxEntriesPerMessage {
		return entries[:proto.MaxEntriesPerMessage]
	}
	return entries
}
