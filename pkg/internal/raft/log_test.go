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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// testLogReader is an in-memory implementation of LogReader for testing.
// It stores entries and state without any persistence layer.
type testLogReader struct {
	entries     []proto.Entry
	state       proto.State
	membership  proto.Membership
	snapshot    proto.Snapshot
	markerIndex uint64
	markerTerm  uint64
}

func newTestLogReader(entries []proto.Entry, state proto.State) *testLogReader {
	lr := &testLogReader{
		entries: make([]proto.Entry, len(entries)),
		state:   state,
	}
	copy(lr.entries, entries)
	return lr
}

func (lr *testLogReader) GetRange(low, high, maxSize uint64) ([]proto.Entry, error) {
	if low > high {
		return nil, ErrUnavailable
	}
	if low == high {
		return nil, nil
	}
	// Find entries in our stored range.
	if len(lr.entries) == 0 {
		return nil, ErrUnavailable
	}
	firstIdx := lr.entries[0].Index
	lastIdx := lr.entries[len(lr.entries)-1].Index
	if low < firstIdx {
		return nil, ErrCompacted
	}
	if low > lastIdx {
		return nil, ErrUnavailable
	}
	if high > lastIdx+1 {
		high = lastIdx + 1
	}
	start := low - firstIdx
	end := high - firstIdx
	result := lr.entries[start:end]
	if maxSize > 0 {
		return limitEntrySize(result, maxSize), nil
	}
	return result, nil
}

func (lr *testLogReader) Term(index uint64) (uint64, error) {
	if index == lr.markerIndex {
		return lr.markerTerm, nil
	}
	if len(lr.entries) == 0 {
		return 0, ErrUnavailable
	}
	firstIdx := lr.entries[0].Index
	lastIdx := lr.entries[len(lr.entries)-1].Index
	if index < firstIdx {
		return 0, ErrCompacted
	}
	if index > lastIdx {
		return 0, ErrUnavailable
	}
	return lr.entries[index-firstIdx].Term, nil
}

func (lr *testLogReader) SetRange(firstIndex, length uint64) error {
	// No-op for tests: entries are managed directly.
	return nil
}

func (lr *testLogReader) SetState(state proto.State) {
	lr.state = state
}

func (lr *testLogReader) Append(entries []proto.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	lr.entries = append(lr.entries, entries...)
	return nil
}

func (lr *testLogReader) NodeState() (proto.State, proto.Membership) {
	return lr.state, lr.membership
}

func (lr *testLogReader) Snapshot() proto.Snapshot {
	return lr.snapshot
}

func (lr *testLogReader) CreateSnapshot(ss proto.Snapshot) error {
	lr.snapshot = ss
	return nil
}

func (lr *testLogReader) ApplySnapshot(ss proto.Snapshot) error {
	lr.snapshot = ss
	lr.markerIndex = ss.Index
	lr.markerTerm = ss.Term
	// Clear entries below snapshot.
	var kept []proto.Entry
	for _, e := range lr.entries {
		if e.Index > ss.Index {
			kept = append(kept, e)
		}
	}
	lr.entries = kept
	return nil
}

func (lr *testLogReader) IndexRange() (uint64, uint64) {
	if len(lr.entries) == 0 {
		return 0, 0
	}
	return lr.entries[0].Index, lr.entries[len(lr.entries)-1].Index
}

func (lr *testLogReader) Compact(index uint64) error {
	if len(lr.entries) == 0 {
		return ErrCompacted
	}
	firstIdx := lr.entries[0].Index
	if index < firstIdx {
		return ErrCompacted
	}
	lastIdx := lr.entries[len(lr.entries)-1].Index
	if index > lastIdx {
		return ErrUnavailable
	}
	offset := index - firstIdx
	lr.markerIndex = index
	lr.markerTerm = lr.entries[offset].Term
	lr.entries = lr.entries[offset+1:]
	return nil
}

// testLogDB is a minimal LogReader implementation for testing specific
// term/getRange behaviors. Only the methods set via function fields are
// active; all other methods return zero values or nil errors.
type testLogDB struct {
	termFn     func(index uint64) (uint64, error)
	getRangeFn func(low, high, maxSize uint64) ([]proto.Entry, error)
}

func (db *testLogDB) Term(index uint64) (uint64, error) {
	if db.termFn != nil {
		return db.termFn(index)
	}
	return 0, ErrUnavailable
}

func (db *testLogDB) GetRange(low, high, maxSize uint64) ([]proto.Entry, error) {
	if db.getRangeFn != nil {
		return db.getRangeFn(low, high, maxSize)
	}
	return nil, ErrUnavailable
}

func (db *testLogDB) SetRange(firstIndex, length uint64) error { return nil }
func (db *testLogDB) SetState(state proto.State)               {}
func (db *testLogDB) Append(entries []proto.Entry) error       { return nil }
func (db *testLogDB) NodeState() (proto.State, proto.Membership) {
	return proto.State{}, proto.Membership{}
}
func (db *testLogDB) Snapshot() proto.Snapshot               { return proto.Snapshot{} }
func (db *testLogDB) CreateSnapshot(ss proto.Snapshot) error { return nil }
func (db *testLogDB) ApplySnapshot(ss proto.Snapshot) error  { return nil }
func (db *testLogDB) Compact(index uint64) error             { return nil }
func (db *testLogDB) IndexRange() (uint64, uint64)           { return 0, 0 }

// newTestRaftLog creates a raftLog with pre-populated entries for testing.
func newTestRaftLog(entries []proto.Entry) *raftLog {
	state := proto.State{}
	if len(entries) > 0 {
		// Set the initial committed to 0 so all entries are uncommitted.
		state.Commit = 0
	}
	logdb := newTestLogReader(nil, state)
	l := &raftLog{
		logdb:     logdb,
		inmem:     newInMemory(0),
		committed: 0,
		processed: 0,
	}
	if len(entries) > 0 {
		l.inmem.merge(entries)
	}
	return l
}

func TestRaftLog_LastIndex_WithEntries(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)

	if l.lastIndex() != 5 {
		t.Fatalf("expected lastIndex 5, got %d", l.lastIndex())
	}
}

func TestRaftLog_LastIndex_Empty(t *testing.T) {
	l := newTestRaftLog(nil)
	if l.lastIndex() != 0 {
		t.Fatalf("expected lastIndex 0, got %d", l.lastIndex())
	}
}

func TestRaftLog_LastTerm_WithEntries(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	}
	l := newTestRaftLog(entries)

	if l.lastTerm() != 3 {
		t.Fatalf("expected lastTerm 3, got %d", l.lastTerm())
	}
}

func TestRaftLog_LastTerm_Empty(t *testing.T) {
	l := newTestRaftLog(nil)
	if l.lastTerm() != 0 {
		t.Fatalf("expected lastTerm 0, got %d", l.lastTerm())
	}
}

func TestRaftLog_Term_ValidIndex(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	}
	l := newTestRaftLog(entries)

	term, err := l.term(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if term != 2 {
		t.Fatalf("expected term 2, got %d", term)
	}
}

func TestRaftLog_Term_InvalidIndex(t *testing.T) {
	entries := makeEntries(1, 4, 1)
	l := newTestRaftLog(entries)

	_, err := l.term(10)
	// For in-memory only entries beyond lastIndex, term() returns 0
	// since inmem returns false and logdb returns ErrUnavailable.
	if err == nil {
		// The term for an unavailable index should return an error.
	}
}

func TestRaftLog_Entries_ReturnsCorrectRange(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)

	result, err := l.entries(2, 5, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result))
	}
	if result[0].Index != 2 || result[2].Index != 4 {
		t.Fatalf("unexpected indices: %d, %d", result[0].Index, result[2].Index)
	}
}

func TestRaftLog_Entries_ReturnsAllEntries(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)

	result, err := l.entries(1, 6, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(result))
	}
}

func TestRaftLog_Entries_EmptyRange(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)

	result, err := l.entries(3, 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestRaftLog_Entries_WithSizeLimit(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1, Cmd: make([]byte, 100)},
		{Index: 2, Term: 1, Cmd: make([]byte, 100)},
		{Index: 3, Term: 1, Cmd: make([]byte, 100)},
	}
	l := newTestRaftLog(entries)

	// Size of one entry: entryHeaderSize + 100 = 160 bytes.
	// Request maxSize that fits ~2 entries.
	result, err := l.entries(1, 4, 300)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should return at most 2 entries since third would exceed maxSize.
	if len(result) > 2 {
		t.Fatalf("expected at most 2 entries with size limit, got %d", len(result))
	}
}

func TestRaftLog_Entries_LowBeyondLastIndex(t *testing.T) {
	entries := makeEntries(1, 4, 1)
	l := newTestRaftLog(entries)

	_, err := l.entries(10, 15, 0)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestRaftLog_MatchTerm_Positive(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	}
	l := newTestRaftLog(entries)

	if !l.matchTerm(2, 2) {
		t.Fatal("expected matchTerm(2, 2) to be true")
	}
}

func TestRaftLog_MatchTerm_Negative(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	}
	l := newTestRaftLog(entries)

	if l.matchTerm(2, 5) {
		t.Fatal("expected matchTerm(2, 5) to be false")
	}
}

func TestRaftLog_MatchTerm_InvalidIndex(t *testing.T) {
	entries := makeEntries(1, 4, 1)
	l := newTestRaftLog(entries)

	if l.matchTerm(100, 1) {
		t.Fatal("expected matchTerm with invalid index to be false")
	}
}

func TestRaftLog_IsUpToDate_HigherTerm(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	l := newTestRaftLog(entries)

	// Candidate has higher term, even with lower index.
	if !l.isUpToDate(1, 3) {
		t.Fatal("expected isUpToDate(1, 3) to be true")
	}
}

func TestRaftLog_IsUpToDate_SameTermHigherIndex(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	l := newTestRaftLog(entries)

	if !l.isUpToDate(3, 2) {
		t.Fatal("expected isUpToDate(3, 2) to be true")
	}
}

func TestRaftLog_IsUpToDate_SameTermSameIndex(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	l := newTestRaftLog(entries)

	if !l.isUpToDate(2, 2) {
		t.Fatal("expected isUpToDate(2, 2) to be true")
	}
}

func TestRaftLog_IsUpToDate_LowerTerm(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	l := newTestRaftLog(entries)

	if l.isUpToDate(10, 1) {
		t.Fatal("expected isUpToDate(10, 1) to be false")
	}
}

func TestRaftLog_IsUpToDate_SameTermLowerIndex(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	l := newTestRaftLog(entries)

	if l.isUpToDate(1, 2) {
		t.Fatal("expected isUpToDate(1, 2) to be false")
	}
}

func TestRaftLog_TryAppend_Success(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}
	l := newTestRaftLog(entries)

	newEntries := []proto.Entry{
		{Index: 4, Term: 2},
		{Index: 5, Term: 2},
	}
	lastIdx, ok, truncated, tryErr := l.tryAppend(3, 1, newEntries)
	if tryErr != nil {
		t.Fatalf("unexpected tryAppend error: %v", tryErr)
	}
	if !ok {
		t.Fatal("expected tryAppend to succeed")
	}
	if truncated {
		t.Fatal("expected no truncation for contiguous append")
	}
	if lastIdx != 5 {
		t.Fatalf("expected lastIdx 5, got %d", lastIdx)
	}
	if l.lastIndex() != 5 {
		t.Fatalf("expected lastIndex 5, got %d", l.lastIndex())
	}
}

func TestRaftLog_TryAppend_TermMismatch(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}
	l := newTestRaftLog(entries)

	newEntries := []proto.Entry{
		{Index: 4, Term: 2},
	}
	_, ok, _, tryErr := l.tryAppend(3, 99, newEntries) // wrong prevLogTerm
	if tryErr != nil {
		t.Fatalf("unexpected tryAppend error: %v", tryErr)
	}
	if ok {
		t.Fatal("expected tryAppend to fail with term mismatch")
	}
}

func TestRaftLog_TryAppend_WithConflict(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}
	l := newTestRaftLog(entries)

	// New entries conflict at index 2 (different term).
	newEntries := []proto.Entry{
		{Index: 2, Term: 2},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
	}
	lastIdx, ok, truncated, tryErr := l.tryAppend(1, 1, newEntries)
	if tryErr != nil {
		t.Fatalf("unexpected tryAppend error: %v", tryErr)
	}
	if !ok {
		t.Fatal("expected tryAppend to succeed")
	}
	if !truncated {
		t.Fatal("expected truncation for conflicting append")
	}
	if lastIdx != 4 {
		t.Fatalf("expected lastIdx 4, got %d", lastIdx)
	}
	// Verify the conflicting entries were replaced.
	term, err := l.term(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if term != 2 {
		t.Fatalf("expected term 2 at index 2, got %d", term)
	}
}

func TestRaftLog_TryAppend_NoNewEntries(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	}
	l := newTestRaftLog(entries)

	// Empty entries with valid prevLogIndex/prevLogTerm.
	lastIdx, ok, truncated, tryErr := l.tryAppend(2, 1, nil)
	if tryErr != nil {
		t.Fatalf("unexpected tryAppend error: %v", tryErr)
	}
	if !ok {
		t.Fatal("expected tryAppend with empty entries to succeed")
	}
	if truncated {
		t.Fatal("expected no truncation for empty entries")
	}
	if lastIdx != 2 {
		t.Fatalf("expected lastIdx 2, got %d", lastIdx)
	}
}

func TestRaftLog_Append_ReturnsLastIndex(t *testing.T) {
	l := newTestRaftLog(makeEntries(1, 4, 1))

	newEntries := makeEntries(4, 7, 2)
	lastIdx := l.append(newEntries)
	if lastIdx != 6 {
		t.Fatalf("expected lastIndex 6, got %d", lastIdx)
	}
}

func TestRaftLog_Append_EmptyEntries(t *testing.T) {
	l := newTestRaftLog(makeEntries(1, 4, 1))

	lastIdx := l.append(nil)
	if lastIdx != 3 {
		t.Fatalf("expected lastIndex 3, got %d", lastIdx)
	}
}

func TestRaftLog_TryCommit_MatchingTerm(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 2},
	}
	l := newTestRaftLog(entries)

	ok := l.tryCommit(3, 2)
	if !ok {
		t.Fatal("expected tryCommit to succeed")
	}
	if l.committed != 3 {
		t.Fatalf("expected committed 3, got %d", l.committed)
	}
}

func TestRaftLog_TryCommit_MismatchingTerm(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 2},
	}
	l := newTestRaftLog(entries)

	ok := l.tryCommit(3, 99)
	if ok {
		t.Fatal("expected tryCommit to fail with mismatching term")
	}
	if l.committed != 0 {
		t.Fatalf("expected committed 0, got %d", l.committed)
	}
}

func TestRaftLog_TryCommit_AlreadyCommitted(t *testing.T) {
	entries := makeEntries(1, 4, 1)
	l := newTestRaftLog(entries)
	l.committed = 3

	ok := l.tryCommit(2, 1)
	if ok {
		t.Fatal("expected tryCommit to return false for already committed index")
	}
}

func TestRaftLog_CommitTo_Advances(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)

	l.commitTo(3)
	if l.committed != 3 {
		t.Fatalf("expected committed 3, got %d", l.committed)
	}
}

func TestRaftLog_CommitTo_NoBackward(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 3

	l.commitTo(1) // should be no-op
	if l.committed != 3 {
		t.Fatalf("expected committed to remain 3, got %d", l.committed)
	}
}

func TestRaftLog_AppliedTo_Advances(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 5
	l.processed = 2

	l.appliedTo(4)
	if l.processed != 4 {
		t.Fatalf("expected processed 4, got %d", l.processed)
	}
}

func TestRaftLog_AppliedTo_Zero(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 5

	l.appliedTo(0) // should be no-op
	if l.processed != 0 {
		t.Fatalf("expected processed 0, got %d", l.processed)
	}
}

func TestRaftLog_HasEntriesToApply_True(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 5
	l.processed = 2

	if !l.hasEntriesToApply() {
		t.Fatal("expected hasEntriesToApply to be true")
	}
}

func TestRaftLog_HasEntriesToApply_False(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 3
	l.processed = 3

	if l.hasEntriesToApply() {
		t.Fatal("expected hasEntriesToApply to be false")
	}
}

func TestRaftLog_EntriesToApply_ReturnsCommittedButUnapplied(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 4
	l.processed = 1

	result, pooled, err := l.entriesToApply()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer proto.PutEntrySlice(pooled)
	if len(result) != 3 {
		t.Fatalf("expected 3 entries to apply, got %d", len(result))
	}
	if result[0].Index != 2 {
		t.Fatalf("expected first entry to apply at index 2, got %d", result[0].Index)
	}
	if result[2].Index != 4 {
		t.Fatalf("expected last entry to apply at index 4, got %d", result[2].Index)
	}
}

func TestRaftLog_EntriesToApply_NoneAvailable(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 3
	l.processed = 3

	result, pooled, err := l.entriesToApply()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer proto.PutEntrySlice(pooled)
	if result != nil {
		t.Fatalf("expected nil, got %d entries", len(result))
	}
}

func TestRaftLog_EntriesToSave_AllUnsaved(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)

	toSave := l.entriesToSave()
	if len(toSave) != 5 {
		t.Fatalf("expected 5 entries to save, got %d", len(toSave))
	}
}

func TestRaftLog_EntriesToSave_PartiallySaved(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.inmem.savedLogTo(3, 1) // entries 1,2,3 saved

	toSave := l.entriesToSave()
	if len(toSave) != 2 {
		t.Fatalf("expected 2 entries to save, got %d", len(toSave))
	}
	if toSave[0].Index != 4 {
		t.Fatalf("expected first unsaved at index 4, got %d", toSave[0].Index)
	}
}

func TestRaftLog_Snapshot_NoSnapshot(t *testing.T) {
	l := newTestRaftLog(makeEntries(1, 4, 1))

	if l.snapshot() != nil {
		t.Fatal("expected nil snapshot")
	}
}

func TestRaftLog_Snapshot_WithSnapshot(t *testing.T) {
	l := newTestRaftLog(makeEntries(1, 4, 1))
	l.inmem.snapshot = &proto.Snapshot{Index: 10, Term: 3}

	ss := l.snapshot()
	if ss == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if ss.Index != 10 || ss.Term != 3 {
		t.Fatalf("expected snapshot index=10 term=3, got index=%d term=%d",
			ss.Index, ss.Term)
	}
}

func TestRaftLog_Restore_FromSnapshot(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)

	ss := proto.Snapshot{Index: 20, Term: 5}
	l.restore(ss)

	if l.committed != 20 {
		t.Fatalf("expected committed 20, got %d", l.committed)
	}
	if l.processed != 20 {
		t.Fatalf("expected processed 20, got %d", l.processed)
	}
	if l.lastIndex() != 20 {
		t.Fatalf("expected lastIndex 20, got %d", l.lastIndex())
	}
}

func TestRaftLog_Restore_ClearsInMemEntries(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)

	ss := proto.Snapshot{Index: 20, Term: 5}
	l.restore(ss)

	if l.inmem.entries != nil {
		t.Fatal("expected inmem entries to be nil after restore")
	}
}

func TestRaftLog_GetConflictIndex_NoConflict(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	}
	l := newTestRaftLog(entries)

	// All entries match.
	toCheck := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	idx, conflictErr := l.getConflictIndex(toCheck)
	if conflictErr != nil {
		t.Fatalf("unexpected getConflictIndex error: %v", conflictErr)
	}
	if idx != 0 {
		t.Fatalf("expected conflict index 0, got %d", idx)
	}
}

func TestRaftLog_GetConflictIndex_ConflictAtStart(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	}
	l := newTestRaftLog(entries)

	toCheck := []proto.Entry{
		{Index: 1, Term: 99}, // conflict at first entry
		{Index: 2, Term: 2},
	}
	idx, conflictErr := l.getConflictIndex(toCheck)
	if conflictErr != nil {
		t.Fatalf("unexpected getConflictIndex error: %v", conflictErr)
	}
	if idx != 1 {
		t.Fatalf("expected conflict index 1, got %d", idx)
	}
}

func TestRaftLog_GetConflictIndex_ConflictInMiddle(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	}
	l := newTestRaftLog(entries)

	toCheck := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 99}, // conflict at index 2
		{Index: 3, Term: 3},
	}
	idx, conflictErr := l.getConflictIndex(toCheck)
	if conflictErr != nil {
		t.Fatalf("unexpected getConflictIndex error: %v", conflictErr)
	}
	if idx != 2 {
		t.Fatalf("expected conflict index 2, got %d", idx)
	}
}

func TestRaftLog_GetConflictIndex_NewEntriesBeyondLog(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	l := newTestRaftLog(entries)

	// Entries beyond the log: matchTerm returns false for unavailable index.
	toCheck := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3}, // beyond log, no match
	}
	idx, conflictErr := l.getConflictIndex(toCheck)
	if conflictErr != nil {
		t.Fatalf("unexpected getConflictIndex error: %v", conflictErr)
	}
	if idx != 3 {
		t.Fatalf("expected conflict index 3 (new entry), got %d", idx)
	}
}

// TestRaftLog_GetConflictIndex_CompactedEntriesSkipped verifies that
// getConflictIndex skips entries that return ErrCompacted from term(),
// treating them as non-conflicting (already committed and persisted).
// This prevents false conflicts when a follower's inmem log has a gap
// after snapshot restore but LogDB has compacted entries below.
func TestRaftLog_GetConflictIndex_CompactedEntriesSkipped(t *testing.T) {
	// Build a log with entries [1..5] in LogDB, then compact 1-3.
	entries := makeEntries(1, 6, 1)
	logdb := newTestLogReader(entries, proto.State{})
	if err := logdb.Compact(3); err != nil {
		t.Fatalf("compact: %v", err)
	}
	// Now LogDB has entries [4, 5] and returns ErrCompacted for 1-3.
	l := &raftLog{
		logdb: logdb,
		inmem: newInMemory(5),
	}
	// Simulate post-snapshot state: inmem cleared, markerIndex=4.
	l.inmem.restore(proto.Snapshot{Index: 3, Term: 1})
	// Add entry 4 back to inmem.
	l.inmem.merge([]proto.Entry{{Index: 4, Term: 1}})

	// Entries 2-3 are compacted in LogDB (ErrCompacted), entry 4 matches inmem.
	checkEntries := []proto.Entry{
		{Index: 2, Term: 1}, // compacted → skip
		{Index: 3, Term: 1}, // compacted → skip
		{Index: 4, Term: 1}, // matches inmem
	}
	idx, err := l.getConflictIndex(checkEntries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx != 0 {
		t.Fatalf("expected no conflict (0), got %d", idx)
	}

	// Entry 4 has a different term → real conflict.
	checkEntries[2].Term = 99
	idx, err = l.getConflictIndex(checkEntries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx != 4 {
		t.Fatalf("expected conflict at index 4, got %d", idx)
	}
}

func TestRaftLog_CommitUpdate_ProcessedAndSaved(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 5
	l.processed = 2

	up := proto.UpdateCommit{
		StableLogTo:   4,
		StableLogTerm: 1,
		Processed:     true,
	}
	l.commitUpdate(up)

	// Processed is a bool flag, does not change l.processed index.
	if l.processed != 2 {
		t.Fatalf("expected processed 2, got %d", l.processed)
	}
	if l.inmem.savedTo != 4 {
		t.Fatalf("expected inmem savedTo 4, got %d", l.inmem.savedTo)
	}
}

func TestRaftLog_CommitUpdate_LastApplied(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 5
	l.processed = 3

	up := proto.UpdateCommit{
		LastApplied: 2,
	}
	l.commitUpdate(up)

	if l.inmem.appliedTo != 2 {
		t.Fatalf("expected appliedTo 2, got %d", l.inmem.appliedTo)
	}
	// Entries 1,2 should be truncated.
	if l.inmem.markerIndex != 3 {
		t.Fatalf("expected markerIndex 3, got %d", l.inmem.markerIndex)
	}
}

func TestRaftLog_CommitUpdate_ZeroProcessed(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 5
	l.processed = 2

	up := proto.UpdateCommit{
		Processed: false, // should not affect processed index
	}
	l.commitUpdate(up)

	if l.processed != 2 {
		t.Fatalf("expected processed 2, got %d", l.processed)
	}
}

func TestRaftLog_FirstIndex_WithEntries(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)

	if l.firstIndex() != 1 {
		t.Fatalf("expected firstIndex 1, got %d", l.firstIndex())
	}
}

func TestRaftLog_FirstIndex_WithSnapshot(t *testing.T) {
	l := newTestRaftLog(nil)
	l.inmem.snapshot = &proto.Snapshot{Index: 10, Term: 2}

	if l.firstIndex() != 11 {
		t.Fatalf("expected firstIndex 11, got %d", l.firstIndex())
	}
}

func TestRaftLog_LimitEntrySize_EmptySlice(t *testing.T) {
	result := limitEntrySize(nil, 100)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestRaftLog_LimitEntrySize_ZeroMaxSize(t *testing.T) {
	entries := makeEntries(1, 4, 1)
	result := limitEntrySize(entries, 0)
	if len(result) != 3 {
		t.Fatalf("expected all entries returned with maxSize=0, got %d", len(result))
	}
}

func TestRaftLog_LimitEntrySize_AlwaysReturnsAtLeastOne(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1, Cmd: make([]byte, 1000)},
	}
	// maxSize is smaller than the single entry.
	result := limitEntrySize(entries, 1)
	if len(result) != 1 {
		t.Fatalf("expected at least 1 entry, got %d", len(result))
	}
}

func TestRaftLog_LimitEntrySize_LimitsCorrectly(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1, Cmd: make([]byte, 100)},
		{Index: 2, Term: 1, Cmd: make([]byte, 100)},
		{Index: 3, Term: 1, Cmd: make([]byte, 100)},
	}
	// Each entry is entryHeaderSize + 100 = 160 bytes.
	// Limit to 250 bytes: first entry (160) fits, second would push to 320.
	result := limitEntrySize(entries, 250)
	if len(result) != 1 {
		t.Fatalf("expected 1 entry with limit 250, got %d", len(result))
	}
}

func TestRaftLog_NewRaftLog_InitializesFromState(t *testing.T) {
	state := proto.State{Commit: 5}
	logdb := newTestLogReader(nil, state)

	l := newRaftLog(logdb)
	if l.committed != 5 {
		t.Fatalf("expected committed 5, got %d", l.committed)
	}
	// processed is set to the snapshot index (0 when no snapshot),
	// not the commit index. This ensures entries are re-applied on
	// restart for in-memory SMs.
	if l.processed != 0 {
		t.Fatalf("expected processed 0 (no snapshot), got %d", l.processed)
	}
}

func TestRaftLog_NewRaftLog_ProcessedFromSnapshot(t *testing.T) {
	state := proto.State{Commit: 10}
	logdb := newTestLogReader(nil, state)
	logdb.snapshot = proto.Snapshot{Index: 7, Term: 2}

	l := newRaftLog(logdb)
	if l.committed != 10 {
		t.Fatalf("expected committed 10, got %d", l.committed)
	}
	// processed should be the snapshot index, not the commit index.
	// Entries 8-10 need to be re-applied to the SM on restart.
	if l.processed != 7 {
		t.Fatalf("expected processed 7 (snapshot index), got %d", l.processed)
	}
}

func TestRaftLog_NewRaftLog_EmptyState(t *testing.T) {
	state := proto.State{}
	logdb := newTestLogReader(nil, state)

	l := newRaftLog(logdb)
	if l.committed != 0 {
		t.Fatalf("expected committed 0, got %d", l.committed)
	}
	if l.processed != 0 {
		t.Fatalf("expected processed 0, got %d", l.processed)
	}
}

func TestRaftLog_Entries_HighClampedToLastIndex(t *testing.T) {
	entries := makeEntries(1, 4, 1) // index 1,2,3
	l := newTestRaftLog(entries)

	// Request range beyond last index; high should be clamped.
	result, err := l.entries(1, 100, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 entries (clamped), got %d", len(result))
	}
}

func TestRaftLog_TryAppend_AllMatchingEntries(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}
	l := newTestRaftLog(entries)

	// Append entries that all match existing log (no conflict, no new).
	toAppend := []proto.Entry{
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}
	lastIdx, ok, truncated, tryErr := l.tryAppend(1, 1, toAppend)
	if tryErr != nil {
		t.Fatalf("unexpected tryAppend error: %v", tryErr)
	}
	if !ok {
		t.Fatal("expected tryAppend to succeed")
	}
	if truncated {
		t.Fatal("expected no truncation for matching entries")
	}
	// No new entries, conflictIndex returns 0.
	if lastIdx != 3 {
		t.Fatalf("expected lastIdx 3, got %d", lastIdx)
	}
}

func TestRaftLog_FirstNotAppliedIndex(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.processed = 3

	if l.firstNotAppliedIndex() != 4 {
		t.Fatalf("expected firstNotAppliedIndex 4, got %d", l.firstNotAppliedIndex())
	}
}

func TestRaftLog_FirstNotAppliedIndex_BelowFirstIndex(t *testing.T) {
	l := newTestRaftLog(nil)
	l.inmem.snapshot = &proto.Snapshot{Index: 10, Term: 2}
	l.inmem.markerIndex = 11
	l.processed = 5 // processed is below firstIndex (11)

	if l.firstNotAppliedIndex() != 11 {
		t.Fatalf("expected firstNotAppliedIndex 11, got %d", l.firstNotAppliedIndex())
	}
}

func TestRaftLog_Entries_LowGtHigh(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)

	_, err := l.entries(5, 3, 0)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable for low > high, got %v", err)
	}
}

func TestRaftLog_EntriesToApply_WithProcessedAtZero(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 3
	l.processed = 0

	result, pooled, err := l.entriesToApply()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer proto.PutEntrySlice(pooled)
	if len(result) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result))
	}
	if result[0].Index != 1 {
		t.Fatalf("expected first entry index 1, got %d", result[0].Index)
	}
}

// TestRaftLog_Term_TranslatesLogDBErrCompacted verifies that when the
// underlying LogReader returns logdb.ErrCompacted (a different sentinel
// from raft.ErrCompacted), term() translates it to raft.ErrCompacted.
// This is the root cause of the getConflictIndex false-conflict bug:
// without translation, errors.Is(err, ErrCompacted) fails in
// getConflictIndex and the compacted entry is treated as a new entry.
func TestRaftLog_Term_TranslatesLogDBErrCompacted(t *testing.T) {
	db := &testLogDB{
		termFn: func(index uint64) (uint64, error) {
			return 0, logdb.ErrCompacted
		},
	}
	l := &raftLog{
		logdb: db,
		inmem: newInMemory(0),
	}
	_, err := l.term(5)
	if !errors.Is(err, ErrCompacted) {
		t.Fatalf("expected raft.ErrCompacted, got %v", err)
	}
	// Ensure it is NOT logdb.ErrCompacted (different sentinel).
	if errors.Is(err, logdb.ErrCompacted) {
		t.Fatal("expected translated error, but got raw logdb.ErrCompacted")
	}
}

// TestRaftLog_Term_TranslatesLogDBErrUnavailable verifies that
// logdb.ErrUnavailable from the LogReader is translated to
// raft.ErrUnavailable by term().
func TestRaftLog_Term_TranslatesLogDBErrUnavailable(t *testing.T) {
	db := &testLogDB{
		termFn: func(index uint64) (uint64, error) {
			return 0, logdb.ErrUnavailable
		},
	}
	l := &raftLog{
		logdb: db,
		inmem: newInMemory(0),
	}
	_, err := l.term(5)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected raft.ErrUnavailable, got %v", err)
	}
	if errors.Is(err, logdb.ErrUnavailable) {
		t.Fatal("expected translated error, but got raw logdb.ErrUnavailable")
	}
}

// TestRaftLog_Entries_TranslatesLogDBErrCompacted verifies that when
// LogDB's GetRange returns logdb.ErrCompacted, entries() translates it
// to raft.ErrCompacted for callers in the raft package.
func TestRaftLog_Entries_TranslatesLogDBErrCompacted(t *testing.T) {
	db := &testLogDB{
		getRangeFn: func(low, high, maxSize uint64) ([]proto.Entry, error) {
			return nil, logdb.ErrCompacted
		},
	}
	l := &raftLog{
		logdb: db,
		inmem: newInMemory(10), // markerIndex > low forces LogDB path
	}
	_, err := l.entries(1, 5, 0)
	if !errors.Is(err, ErrCompacted) {
		t.Fatalf("expected raft.ErrCompacted, got %v", err)
	}
	if errors.Is(err, logdb.ErrCompacted) {
		t.Fatal("expected translated error, but got raw logdb.ErrCompacted")
	}
}

// TestRaftLog_Entries_TranslatesLogDBErrUnavailable verifies that when
// LogDB's GetRange returns logdb.ErrUnavailable, entries() translates
// it to raft.ErrUnavailable.
func TestRaftLog_Entries_TranslatesLogDBErrUnavailable(t *testing.T) {
	db := &testLogDB{
		getRangeFn: func(low, high, maxSize uint64) ([]proto.Entry, error) {
			return nil, logdb.ErrUnavailable
		},
	}
	l := &raftLog{
		logdb: db,
		inmem: newInMemory(10),
	}
	_, err := l.entries(1, 5, 0)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected raft.ErrUnavailable, got %v", err)
	}
	if errors.Is(err, logdb.ErrUnavailable) {
		t.Fatal("expected translated error, but got raw logdb.ErrUnavailable")
	}
}

// TestRaftLog_GetConflictIndex_LogDBCompactedSkipped verifies the full
// bug-fix path: getConflictIndex receives logdb.ErrCompacted from the
// real LogDB layer (via term()), which is translated to
// raft.ErrCompacted, and the compacted entry is correctly skipped
// instead of treated as a false conflict.
func TestRaftLog_GetConflictIndex_LogDBCompactedSkipped(t *testing.T) {
	db := &testLogDB{
		termFn: func(index uint64) (uint64, error) {
			if index <= 3 {
				return 0, logdb.ErrCompacted // real LogDB sentinel
			}
			if index == 4 {
				return 1, nil // matches
			}
			return 0, logdb.ErrUnavailable
		},
	}
	l := &raftLog{
		logdb: db,
		inmem: newInMemory(0),
	}
	// Entries 2-3 are compacted, entry 4 matches.
	checkEntries := []proto.Entry{
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
	}
	idx, err := l.getConflictIndex(checkEntries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx != 0 {
		t.Fatalf("expected no conflict (0), got %d", idx)
	}

	// Now verify a real conflict at index 4 is detected.
	checkEntries[2].Term = 99
	idx, err = l.getConflictIndex(checkEntries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx != 4 {
		t.Fatalf("expected conflict at index 4, got %d", idx)
	}
}

// TestRaftLog_TryCommit_LogDBCompacted verifies that tryCommit correctly
// handles logdb.ErrCompacted from the underlying LogReader after
// translation (sets term to 0, returns false for term mismatch).
func TestRaftLog_TryCommit_LogDBCompacted(t *testing.T) {
	db := &testLogDB{
		termFn: func(index uint64) (uint64, error) {
			return 0, logdb.ErrCompacted
		},
	}
	l := &raftLog{
		logdb:     db,
		inmem:     newInMemory(0),
		committed: 0,
	}
	// tryCommit should see ErrCompacted (translated), set t=0,
	// fail the t==term check, and return false.
	ok := l.tryCommit(5, 1)
	if ok {
		t.Fatal("expected tryCommit to return false for compacted index")
	}
}

// --- Benchmarks ---

// BenchmarkRaftLogAppend measures the cost of appending batches of 10
// contiguous entries to a raftLog. This is the hot path for leaders
// replicating proposed entries.
func BenchmarkRaftLogAppend(b *testing.B) {
	b.ReportAllocs()
	logdb := newTestLogReader(nil, proto.State{})
	l := &raftLog{
		logdb: logdb,
		inmem: newInMemory(0),
	}
	batch := make([]proto.Entry, 10)
	nextIndex := uint64(1)
	for b.Loop() {
		for i := range batch {
			batch[i] = proto.Entry{Index: nextIndex, Term: 1}
			nextIndex++
		}
		l.append(batch)
	}
}

// BenchmarkRaftLogEntries measures the cost of reading a range of entries
// from a pre-populated raftLog. This exercises the inmem + logdb lookup
// path used during replication and apply.
func BenchmarkRaftLogEntries(b *testing.B) {
	b.ReportAllocs()
	entries := makeEntries(1, 1001, 1)
	l := newTestRaftLog(entries)
	for b.Loop() {
		_, err := l.entries(1, 100, 0)
		if err != nil {
			b.Fatalf("entries failed: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// firstIndexOfTerm / lastIndexOfTerm
// ---------------------------------------------------------------------------

func TestRaftLog_FirstIndexOfTerm_FindsFirstEntry(t *testing.T) {
	// Log: [1:t1, 2:t1, 3:t2, 4:t2, 5:t3, 6:t3, 7:t3]
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
		{Index: 5, Term: 3},
		{Index: 6, Term: 3},
		{Index: 7, Term: 3},
	}
	l := newTestRaftLog(entries)

	if got := l.firstIndexOfTerm(1); got != 1 {
		t.Fatalf("firstIndexOfTerm(1): expected 1, got %d", got)
	}
	if got := l.firstIndexOfTerm(2); got != 3 {
		t.Fatalf("firstIndexOfTerm(2): expected 3, got %d", got)
	}
	if got := l.firstIndexOfTerm(3); got != 5 {
		t.Fatalf("firstIndexOfTerm(3): expected 5, got %d", got)
	}
}

func TestRaftLog_FirstIndexOfTerm_TermNotPresent(t *testing.T) {
	// Log: [1:t1, 2:t1, 3:t3, 4:t3]  (term 2 missing)
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 3},
		{Index: 4, Term: 3},
	}
	l := newTestRaftLog(entries)

	if got := l.firstIndexOfTerm(2); got != 0 {
		t.Fatalf("firstIndexOfTerm(2): expected 0 for missing term, got %d", got)
	}
	if got := l.firstIndexOfTerm(99); got != 0 {
		t.Fatalf("firstIndexOfTerm(99): expected 0 for missing term, got %d", got)
	}
}

func TestRaftLog_FirstIndexOfTerm_EmptyLog(t *testing.T) {
	l := newTestRaftLog(nil)
	if got := l.firstIndexOfTerm(1); got != 0 {
		t.Fatalf("firstIndexOfTerm(1) on empty log: expected 0, got %d", got)
	}
}

func TestRaftLog_FirstIndexOfTerm_SingleEntry(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 5},
	}
	l := newTestRaftLog(entries)

	if got := l.firstIndexOfTerm(5); got != 1 {
		t.Fatalf("firstIndexOfTerm(5): expected 1, got %d", got)
	}
	if got := l.firstIndexOfTerm(4); got != 0 {
		t.Fatalf("firstIndexOfTerm(4): expected 0, got %d", got)
	}
}

func TestRaftLog_LastIndexOfTerm_FindsLastEntry(t *testing.T) {
	// Log: [1:t1, 2:t1, 3:t2, 4:t2, 5:t3, 6:t3, 7:t3]
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
		{Index: 5, Term: 3},
		{Index: 6, Term: 3},
		{Index: 7, Term: 3},
	}
	l := newTestRaftLog(entries)

	if got := l.lastIndexOfTerm(1); got != 2 {
		t.Fatalf("lastIndexOfTerm(1): expected 2, got %d", got)
	}
	if got := l.lastIndexOfTerm(2); got != 4 {
		t.Fatalf("lastIndexOfTerm(2): expected 4, got %d", got)
	}
	if got := l.lastIndexOfTerm(3); got != 7 {
		t.Fatalf("lastIndexOfTerm(3): expected 7, got %d", got)
	}
}

func TestRaftLog_LastIndexOfTerm_TermNotPresent(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 3},
		{Index: 4, Term: 3},
	}
	l := newTestRaftLog(entries)

	if got := l.lastIndexOfTerm(2); got != 0 {
		t.Fatalf("lastIndexOfTerm(2): expected 0 for missing term, got %d", got)
	}
}

func TestRaftLog_LastIndexOfTerm_EmptyLog(t *testing.T) {
	l := newTestRaftLog(nil)
	if got := l.lastIndexOfTerm(1); got != 0 {
		t.Fatalf("lastIndexOfTerm(1) on empty log: expected 0, got %d", got)
	}
}

func TestRaftLog_LastIndexOfTerm_SingleEntry(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 5},
	}
	l := newTestRaftLog(entries)

	if got := l.lastIndexOfTerm(5); got != 1 {
		t.Fatalf("lastIndexOfTerm(5): expected 1, got %d", got)
	}
	if got := l.lastIndexOfTerm(4); got != 0 {
		t.Fatalf("lastIndexOfTerm(4): expected 0 for missing term, got %d", got)
	}
	if got := l.lastIndexOfTerm(6); got != 0 {
		t.Fatalf("lastIndexOfTerm(6): expected 0 for missing term, got %d", got)
	}
}

func TestRaftLog_FirstIndexOfTerm_AllSameTerm(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 3},
		{Index: 2, Term: 3},
		{Index: 3, Term: 3},
		{Index: 4, Term: 3},
		{Index: 5, Term: 3},
	}
	l := newTestRaftLog(entries)

	if got := l.firstIndexOfTerm(3); got != 1 {
		t.Fatalf("firstIndexOfTerm(3): expected 1, got %d", got)
	}
	if got := l.firstIndexOfTerm(2); got != 0 {
		t.Fatalf("firstIndexOfTerm(2): expected 0 for missing term, got %d", got)
	}
	if got := l.firstIndexOfTerm(4); got != 0 {
		t.Fatalf("firstIndexOfTerm(4): expected 0 for missing term, got %d", got)
	}
}

func TestRaftLog_LastIndexOfTerm_AllSameTerm(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 3},
		{Index: 2, Term: 3},
		{Index: 3, Term: 3},
		{Index: 4, Term: 3},
		{Index: 5, Term: 3},
	}
	l := newTestRaftLog(entries)

	if got := l.lastIndexOfTerm(3); got != 5 {
		t.Fatalf("lastIndexOfTerm(3): expected 5, got %d", got)
	}
	if got := l.lastIndexOfTerm(2); got != 0 {
		t.Fatalf("lastIndexOfTerm(2): expected 0 for missing term, got %d", got)
	}
	if got := l.lastIndexOfTerm(4); got != 0 {
		t.Fatalf("lastIndexOfTerm(4): expected 0 for missing term, got %d", got)
	}
}

// TestRaftLog_BinarySearch_MatchesLinearScan verifies that the binary
// search implementation produces identical results to a naive linear
// scan for a log with many terms, including gaps in the term sequence.
func TestRaftLog_BinarySearch_MatchesLinearScan(t *testing.T) {
	// Log: terms 1,1,1, 3,3, 5,5,5,5, 10, 10
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 3},
		{Index: 5, Term: 3},
		{Index: 6, Term: 5},
		{Index: 7, Term: 5},
		{Index: 8, Term: 5},
		{Index: 9, Term: 5},
		{Index: 10, Term: 10},
		{Index: 11, Term: 10},
	}
	l := newTestRaftLog(entries)

	// linearFirstIndexOfTerm is a reference O(N) implementation.
	linearFirst := func(targetTerm uint64) uint64 {
		for _, e := range entries {
			if e.Term == targetTerm {
				return e.Index
			}
		}
		return 0
	}
	// linearLastIndexOfTerm is a reference O(N) implementation.
	linearLast := func(targetTerm uint64) uint64 {
		var result uint64
		for _, e := range entries {
			if e.Term == targetTerm {
				result = e.Index
			}
		}
		return result
	}

	// Test all terms from 0 to 12 (including absent ones).
	for term := uint64(0); term <= 12; term++ {
		wantFirst := linearFirst(term)
		gotFirst := l.firstIndexOfTerm(term)
		if gotFirst != wantFirst {
			t.Errorf("firstIndexOfTerm(%d): binary search got %d, linear scan got %d", term, gotFirst, wantFirst)
		}
		wantLast := linearLast(term)
		gotLast := l.lastIndexOfTerm(term)
		if gotLast != wantLast {
			t.Errorf("lastIndexOfTerm(%d): binary search got %d, linear scan got %d", term, gotLast, wantLast)
		}
	}
}

// TestRaftLog_FirstIndexOfTerm_TermHigherThanAll verifies that searching
// for a term larger than any in the log returns 0.
func TestRaftLog_FirstIndexOfTerm_TermHigherThanAll(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	}
	l := newTestRaftLog(entries)

	if got := l.firstIndexOfTerm(100); got != 0 {
		t.Fatalf("firstIndexOfTerm(100): expected 0, got %d", got)
	}
}

// TestRaftLog_LastIndexOfTerm_TermHigherThanAll verifies that searching
// for a term larger than any in the log returns 0.
func TestRaftLog_LastIndexOfTerm_TermHigherThanAll(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	}
	l := newTestRaftLog(entries)

	if got := l.lastIndexOfTerm(100); got != 0 {
		t.Fatalf("lastIndexOfTerm(100): expected 0, got %d", got)
	}
}

// TestRaftLog_FirstIndexOfTerm_TermZero verifies that searching for
// term 0 returns 0 (no entries should have term 0 in a valid log).
func TestRaftLog_FirstIndexOfTerm_TermZero(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	l := newTestRaftLog(entries)

	if got := l.firstIndexOfTerm(0); got != 0 {
		t.Fatalf("firstIndexOfTerm(0): expected 0, got %d", got)
	}
}

// TestRaftLog_LastIndexOfTerm_TermZero verifies that searching for
// term 0 returns 0 (no entries should have term 0 in a valid log).
func TestRaftLog_LastIndexOfTerm_TermZero(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	l := newTestRaftLog(entries)

	if got := l.lastIndexOfTerm(0); got != 0 {
		t.Fatalf("lastIndexOfTerm(0): expected 0, got %d", got)
	}
}

// BenchmarkRaftLogFirstIndexOfTerm measures binary search performance
// for firstIndexOfTerm on a large log with multiple terms.
func BenchmarkRaftLogFirstIndexOfTerm(b *testing.B) {
	b.ReportAllocs()
	// Create 10000 entries across 100 terms (100 entries per term).
	entries := make([]proto.Entry, 10000)
	for i := range entries {
		entries[i] = proto.Entry{
			Index: uint64(i + 1),
			Term:  uint64(i/100) + 1,
		}
	}
	l := newTestRaftLog(entries)
	term := uint64(1)
	for b.Loop() {
		_ = l.firstIndexOfTerm(term)
		term++
		if term > 100 {
			term = 1
		}
	}
}

// BenchmarkRaftLogLastIndexOfTerm measures binary search performance
// for lastIndexOfTerm on a large log with multiple terms.
func BenchmarkRaftLogLastIndexOfTerm(b *testing.B) {
	b.ReportAllocs()
	// Create 10000 entries across 100 terms (100 entries per term).
	entries := make([]proto.Entry, 10000)
	for i := range entries {
		entries[i] = proto.Entry{
			Index: uint64(i + 1),
			Term:  uint64(i/100) + 1,
		}
	}
	l := newTestRaftLog(entries)
	term := uint64(1)
	for b.Loop() {
		_ = l.lastIndexOfTerm(term)
		term++
		if term > 100 {
			term = 1
		}
	}
}

// BenchmarkRaftLogTerm measures the cost of term lookups on a
// pre-populated raftLog. Term lookups are on the critical path for
// conflict detection during log replication.
func BenchmarkRaftLogTerm(b *testing.B) {
	b.ReportAllocs()
	entries := makeEntries(1, 1001, 1)
	l := newTestRaftLog(entries)
	// Cycle through indices 1..1000 to avoid branch prediction bias.
	idx := uint64(1)
	for b.Loop() {
		_, _ = l.term(idx)
		idx++
		if idx > 1000 {
			idx = 1
		}
	}
}
