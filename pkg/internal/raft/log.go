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

// maxEntriesToApplySize caps the bytes entriesToApply returns; fixed and unrelated to Config.MaxApplyEntrySize.
const maxEntriesToApplySize uint64 = 64 * 1024 * 1024 // 64MB

// LogReader reads persisted entries, terms, snapshots and state from LogDB; implementations must be safe for concurrent use.
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
	// Snapshot returns the latest recorded snapshot metadata, or a zero Snapshot if none.
	Snapshot() proto.Snapshot
	// Compact removes entries up to and including the given index.
	Compact(index uint64) error
	// IndexRange returns the first and last available entry indices.
	// Returns (0, 0) when no entries are available.
	IndexRange() (uint64, uint64)
}

// raftLog combines the in-memory log with LogDB; owned by a single goroutine.
type raftLog struct {
	logdb     LogReader
	inmem     inMemory
	committed uint64
	processed uint64 // committed entries already returned for apply

	// confChangeIndex is a conservative upper bound on a config-change index not yet applied; seeded to the recovered last index.
	confChangeIndex uint64

	// pendingAdds records, by entry index, every replica identity (or removal) a config entry in this log admits; the receive gate's pending configuration.
	pendingAdds map[uint64][]pendingAdd
	// pendingVersion counts every mutation of pendingAdds, so a publisher can
	// tell whether a new snapshot is needed without comparing maps.
	pendingVersion uint64
	// pendingSeeded records that seedPendingAdds has classified the recovered
	// log once; see PublishPendingConfig.
	pendingSeeded bool

	// recoveryClamp records the clamp newRaftLog applied to a recovered commit index; check it with Applied().
	recoveryClamp RecoveryClamp
}

// RecoveryClamp describes newRaftLog clamping a recovered HardState.Commit
// down to the last index the recovered log actually holds. The zero value
// means no clamp was applied.
type RecoveryClamp struct {
	// RecoveredCommit is the commit index the state record claimed.
	RecoveredCommit uint64
	// RecoveredTerm is the term the state record claimed.
	RecoveredTerm uint64
	// LastIndex is the last index the recovered log holds, and the value
	// committed was clamped to.
	LastIndex uint64
	// SnapshotIndex is the recovered snapshot's index, 0 when there is none.
	SnapshotIndex uint64
}

// Applied reports whether a clamp was applied.
func (c RecoveryClamp) Applied() bool {
	return c.RecoveredCommit > c.LastIndex
}

// EmptyLog reports total loss: a state record but no entries or snapshot; the replica re-learns everything and it is logged at ERROR.
func (c RecoveryClamp) EmptyLog() bool {
	return c.Applied() && c.LastIndex == 0
}

// pendingAdd is one (replica, identity) a config-carrying entry admits — or,
// with remove set, one replica a RemoveNode entry removes (identity empty:
// the identity it held is whatever the configuration bound below the entry).
type pendingAdd struct {
	replicaID uint64
	identity  string
	remove    bool
}

// pendingView is the pending configuration above an applied one: identities pending adds commit and replicas pending removals remove.
type pendingView struct {
	adds    map[uint64]string
	removed map[uint64]string
}

// noteConfigChanges raises confChangeIndex over config-carrying entries in the batch and records their admitted identities; undecodable payloads are skipped.
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
			// Only an identity held by an active role admits anyone.
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

// dropPendingAbove forgets pending adds recorded from entries above index, which are being truncated.
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

// pendingIdentities resolves the pending changes above applied, in log order, to one verdict per replica; only committed removals count.
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

// pendingSeedBudget bounds the committed-tail bytes seedPendingAdds reads at start; a variable only so tests can lower it.
var pendingSeedBudget uint64 = 64 << 20

// seedPendingAdds classifies the recovered log above the folded configuration once, so the receive gate admits after restart what it did before.
func (l *raftLog) seedPendingAdds(applied uint64, logger *slog.Logger) {
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
			logger.Warn("the recovered committed tail exceeds the pending-config seed budget; "+
				"a config change above the budget admits its member only once the replay folds it",
				"from", next,
				"committed", l.committed,
				"budget_bytes", pendingSeedBudget)
			break
		}
		entries, err := l.entries(next, min(l.committed, last)+1, pendingSeedBudget-read)
		if err != nil || len(entries) == 0 {
			logger.Warn("could not classify the recovered committed tail for pending config changes",
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
			logger.Warn("could not classify the recovered uncommitted tail for pending config changes",
				"from", next,
				"to", last,
				"error", err)
			return
		}
		l.noteConfigChanges(entries)
		next = entries[len(entries)-1].Index + 1
	}
}

// newRaftLog creates a raftLog over logdb: inmem starts after the LogDB last index, committed comes from hard state, processed from the snapshot.
func newRaftLog(logdb LogReader) *raftLog {
	state, _ := logdb.NodeState()
	ss := logdb.Snapshot()
	firstIdx, lastIdx := logdb.IndexRange()

	// The inmem boundary is the LogDB last index, or the snapshot index when LogDB holds no entries, so the clamp below sees what the log actually holds.
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

	// A snapshot index is committed by construction, so it is a lower bound on committed.
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

	// Clamp committed down to the last index the log holds, so the replica never acknowledges entries it lacks.
	if last := l.lastIndex(); l.committed > last {
		// Recorded for the Host to log through Peer.RecoveryClamp.
		l.recoveryClamp = RecoveryClamp{
			RecoveredCommit: l.committed,
			RecoveredTerm:   state.Term,
			LastIndex:       last,
			SnapshotIndex:   ss.Index,
		}
		l.committed = last
	}

	// processed must never exceed committed on any recovery shape.
	if l.processed > l.committed {
		l.processed = l.committed
	}

	// Seed confChangeIndex conservatively over the recovered range.
	l.confChangeIndex = l.lastIndex()
	// The pending configuration is seeded by the first PublishPendingConfig; see seedPendingAdds.
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

// lastTerm returns the last entry's term and whether it could be determined; never fabricate term 0.
func (l *raftLog) lastTerm() (uint64, bool) {
	t, err := l.term(l.lastIndex())
	if err != nil {
		return 0, false
	}
	return t, true
}

// term returns the term at index from inmem, then LogDB, translating LogDB errors; the LogDB fallback is bounded by lastIndex().
func (l *raftLog) term(index uint64) (uint64, error) {
	// Check in-memory first.
	if t, ok := l.inmem.term(index); ok {
		return t, nil
	}
	// Nothing above the end of the log has a term; report unavailable so the leader backtracks.
	if index > l.lastIndex() {
		return 0, logdb.ErrUnavailable
	}
	// Fall back to LogDB.
	t, err := l.logdb.Term(index)
	if err != nil {
		return 0, err
	}
	return t, nil
}

// entries returns log entries in the range [low, high) up to maxSize
// bytes total. Combines entries from LogDB and in-memory storage.
func (l *raftLog) entries(low, high, maxSize uint64) ([]proto.Entry, error) {
	if low > high {
		return nil, logdb.ErrUnavailable
	}
	if low == high {
		return nil, nil
	}
	last := l.lastIndex()
	if low > last+1 {
		return nil, logdb.ErrUnavailable
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
			return nil, err
		}
		// Copy out of GetRange's reuse buffer: the entries may be serialized later.
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

// firstIndexOfTerm returns the first index with term by binary search, or 0; used for fast backtracking (Raft §5.3).
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

// lastIndexOfTerm returns the last index with term by binary search, or 0; used for fast backtracking (Raft §5.3).
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

// isUpToDate reports whether (lastIndex, lastTerm) is at least as up to date as this log, and whether that could be determined.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) (upToDate, readable bool) {
	ourLastTerm, ok := l.lastTerm()
	if !ok {
		// Our own last term is unreadable, so we cannot grant.
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

// tryAppend appends ents after prevLogIndex/prevLogTerm if the log matches, returning the last new index; a compacted or unavailable conflict lookup returns an error.
func (l *raftLog) tryAppend(prevLogIndex, prevLogTerm uint64, ents []proto.Entry) (lastIndex uint64, ok bool, truncated bool, err error) {
	if !l.matchTerm(prevLogIndex, prevLogTerm) {
		return 0, false, false, nil
	}
	// Reject non-contiguous batches before mutating the log.
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
	// A conflict at or below lastIndex is an overwrite and needs a membership rebuild; beyond it is a plain extension.
	isOverwrite := conflictIdx <= l.lastIndex()
	if isOverwrite {
		// Entries from conflictIdx on are gone; their replacements re-record what they admit.
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
	if errors.Is(err, logdb.ErrCompacted) {
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

// savedIndex returns the highest index durably persisted to LogDB.
func (l *raftLog) savedIndex() uint64 {
	return l.inmem.savedTo
}

// entriesToApply returns committed, unprocessed entries up to maxEntriesToApplySize in a pooled slice; release it with proto.PutEntrySlice.
func (l *raftLog) entriesToApply() ([]proto.Entry, *[]proto.Entry, error) {
	return l.entriesToApplyUpTo(l.committed)
}

// entriesToApplyUpTo is entriesToApply limited to indexes at or below ceiling.
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
	// Copy into a pooled slice the caller owns.
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

// pendingSnapshot returns the received, not yet persisted snapshot from inmem, if any.
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

// getConflictIndex returns the first index where ents conflict with the log, or 0; committed entries are skipped.
//
//nolint:unparam // Error is always nil but signature kept for consistency with tryAppend
func (l *raftLog) getConflictIndex(entries []proto.Entry) (uint64, error) {
	for i := range entries {
		// Committed entries cannot conflict.
		if entries[i].Index <= l.committed {
			continue
		}
		t, err := l.term(entries[i].Index)
		if err != nil {
			if errors.Is(err, logdb.ErrCompacted) {
				// Entry was compacted away — it was committed and
				// persisted, so it cannot conflict. Skip it.
				continue
			}
			// logdb.ErrUnavailable: entry is beyond our log. This is the
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

// commitUpdate applies a persistence notification, updating savedTo and the applied state.
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

// limitEntryCount caps an outbound message at proto.MaxEntriesPerMessage entries; the rest go in the next pipelined message.
func limitEntryCount(entries []proto.Entry) []proto.Entry {
	if len(entries) > proto.MaxEntriesPerMessage {
		return entries[:proto.MaxEntriesPerMessage]
	}
	return entries
}
