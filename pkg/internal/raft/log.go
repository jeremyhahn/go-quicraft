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

	"github.com/jeremyhahn/go-quicraft/pkg/internal/invariant"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// maxEntriesToApplySize is the maximum total size of entries returned
// by entriesToApply. This prevents unbounded memory usage during apply.
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

	// Use LogDB's last index for inmem boundary, matching dragonboat.
	// When no entries exist (lastIdx==0), fall back to state.Commit.
	inmemStart := lastIdx
	if inmemStart == 0 {
		inmemStart = state.Commit
	}

	// processed = snapshot index, or firstIndex-1 if no snapshot.
	// This ensures all entries from the snapshot (or log start) to
	// committed are re-applied on restart.
	processed := ss.Index
	if processed == 0 && firstIdx > 0 {
		processed = firstIdx - 1
	}

	l := &raftLog{
		logdb:     logdb,
		inmem:     newInMemory(inmemStart),
		committed: state.Commit,
		processed: processed,
	}
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

// lastTerm returns the term of the last log entry.
func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		return 0
	}
	return t
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
func (l *raftLog) term(index uint64) (uint64, error) {
	// Check in-memory first.
	if t, ok := l.inmem.term(index); ok {
		return t, nil
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

// isUpToDate returns true if the given (lastIndex, lastTerm) is at least
// as up-to-date as this log. The Raft paper defines "up-to-date" as:
// the candidate's last log entry has a higher term, or same term with
// equal or higher index.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	ourLastTerm := l.lastTerm()
	if lastTerm > ourLastTerm {
		return true
	}
	if lastTerm == ourLastTerm {
		return lastIndex >= l.lastIndex()
	}
	return false
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
	// Append from the conflict point onward.
	offset := conflictIdx - ents[0].Index
	l.append(ents[offset:])
	lastNew := ents[len(ents)-1].Index
	return lastNew, true, isOverwrite, nil
}

// append directly appends entries to the log via the in-memory layer.
// Returns the last index after appending.
//
//nolint:unparam // Return value used in tests for convenience
func (l *raftLog) append(entries []proto.Entry) uint64 {
	if len(entries) == 0 {
		return l.lastIndex()
	}
	invariant.Assert(entries[0].Index > l.committed,
		"cannot append entries at or below committed index")
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

// entriesToApply returns committed but not yet processed entries,
// limited to maxEntriesToApplySize bytes. Entries are copied into a
// pooled slice to reduce GC pressure on the apply hot path. The
// returned pooled pointer must be passed to proto.PutEntrySlice after
// the entries are consumed.
func (l *raftLog) entriesToApply() ([]proto.Entry, *[]proto.Entry, error) {
	if !l.hasEntriesToApply() {
		return nil, nil, nil
	}
	low := l.firstNotAppliedIndex()
	high := l.committed + 1
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
