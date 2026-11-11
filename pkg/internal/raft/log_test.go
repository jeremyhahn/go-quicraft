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
		return nil, logdb.ErrUnavailable
	}
	if low == high {
		return nil, nil
	}
	// Find entries in our stored range.
	if len(lr.entries) == 0 {
		return nil, logdb.ErrUnavailable
	}
	firstIdx := lr.entries[0].Index
	lastIdx := lr.entries[len(lr.entries)-1].Index
	if low < firstIdx {
		return nil, logdb.ErrCompacted
	}
	if low > lastIdx {
		return nil, logdb.ErrUnavailable
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
		return 0, logdb.ErrUnavailable
	}
	firstIdx := lr.entries[0].Index
	lastIdx := lr.entries[len(lr.entries)-1].Index
	if index < firstIdx {
		return 0, logdb.ErrCompacted
	}
	if index > lastIdx {
		return 0, logdb.ErrUnavailable
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
		return logdb.ErrCompacted
	}
	firstIdx := lr.entries[0].Index
	if index < firstIdx {
		return logdb.ErrCompacted
	}
	lastIdx := lr.entries[len(lr.entries)-1].Index
	if index > lastIdx {
		return logdb.ErrUnavailable
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
//
// A stub that answers Term or GetRange for an index MUST also declare a range
// covering it via indexRangeFn. raftLog.term bounds its LogDB fallback by
// lastIndex(), which reads IndexRange, so a stub reporting the empty range
// (0, 0) while serving terms for indices 1..N describes a LogReader that cannot
// exist -- and the resulting test proves nothing about the fallback it means to
// exercise.
type testLogDB struct {
	termFn       func(index uint64) (uint64, error)
	getRangeFn   func(low, high, maxSize uint64) ([]proto.Entry, error)
	indexRangeFn func() (uint64, uint64)
}

func (db *testLogDB) Term(index uint64) (uint64, error) {
	if db.termFn != nil {
		return db.termFn(index)
	}
	return 0, logdb.ErrUnavailable
}

func (db *testLogDB) GetRange(low, high, maxSize uint64) ([]proto.Entry, error) {
	if db.getRangeFn != nil {
		return db.getRangeFn(low, high, maxSize)
	}
	return nil, logdb.ErrUnavailable
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
func (db *testLogDB) IndexRange() (uint64, uint64) {
	if db.indexRangeFn != nil {
		return db.indexRangeFn()
	}
	return 0, 0
}

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

	if term, ok := l.lastTerm(); !ok || term != 3 {
		t.Fatalf("lastTerm() = %d, %v; want 3, true", term, ok)
	}
}

func TestRaftLog_LastTerm_Empty(t *testing.T) {
	l := newTestRaftLog(nil)
	if term, ok := l.lastTerm(); !ok || term != 0 {
		t.Fatalf("lastTerm() = %d, %v; want 0, true (an EMPTY log has a "+
			"known last term of 0; only an unreadable one is !ok)", term, ok)
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

	// For an index beyond lastIndex the in-memory log reports a miss and the
	// LogDB reports logdb.ErrUnavailable, so term() must surface that error rather
	// than hand back a zero term a caller could mistake for a real one.
	term, err := l.term(10)
	if !errors.Is(err, logdb.ErrUnavailable) {
		t.Fatalf("term(10) error = %v, want %v", err, logdb.ErrUnavailable)
	}
	if term != 0 {
		t.Fatalf("term(10) = %d alongside its error, want 0", term)
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

	// Size of one entry: entryHeaderSize + 100 = 168 bytes.
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
	if !errors.Is(err, logdb.ErrUnavailable) {
		t.Fatalf("expected logdb.ErrUnavailable, got %v", err)
	}
}

func TestRaftLog_MatchTerm(t *testing.T) {
	l := newTestRaftLog([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	})
	tests := []struct {
		name        string
		index, term uint64
		want        bool
	}{
		{"Positive", 2, 2, true},
		{"Negative", 2, 5, false},
		{"InvalidIndex", 100, 1, false},
	}
	for _, tc := range tests {
		if got := l.matchTerm(tc.index, tc.term); got != tc.want {
			t.Errorf("%s: matchTerm(%d, %d) = %v, want %v",
				tc.name, tc.index, tc.term, got, tc.want)
		}
	}
}

func TestRaftLog_IsUpToDate(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	tests := []struct {
		name        string
		index, term uint64
		want        bool
	}{
		// A higher candidate term wins even with a lower index.
		{"HigherTerm", 1, 3, true},
		{"SameTermHigherIndex", 3, 2, true},
		{"SameTermSameIndex", 2, 2, true},
		{"LowerTerm", 10, 1, false},
		{"SameTermLowerIndex", 1, 2, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := newTestRaftLog(entries)
			upToDate, readable := l.isUpToDate(tc.index, tc.term)
			if !readable {
				t.Fatal("expected our own last term to be readable")
			}
			if upToDate != tc.want {
				t.Fatalf("isUpToDate(%d, %d) = %v, want %v",
					tc.index, tc.term, upToDate, tc.want)
			}
		})
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

func TestRaftLog_GetConflictIndex(t *testing.T) {
	tests := []struct {
		name    string
		log     []proto.Entry
		toCheck []proto.Entry
		want    uint64
	}{
		{
			name:    "NoConflict",
			log:     []proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}},
			toCheck: []proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			want:    0,
		},
		{
			name:    "ConflictAtStart",
			log:     []proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}},
			toCheck: []proto.Entry{{Index: 1, Term: 99}, {Index: 2, Term: 2}},
			want:    1,
		},
		{
			name:    "ConflictInMiddle",
			log:     []proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}},
			toCheck: []proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 99}, {Index: 3, Term: 3}},
			want:    2,
		},
		{
			// matchTerm is false for an index beyond the log, so the first
			// new entry is reported as the conflict.
			name:    "NewEntriesBeyondLog",
			log:     []proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			toCheck: []proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}},
			want:    3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := newTestRaftLog(tc.log)
			idx, conflictErr := l.getConflictIndex(tc.toCheck)
			if conflictErr != nil {
				t.Fatalf("unexpected getConflictIndex error: %v", conflictErr)
			}
			if idx != tc.want {
				t.Fatalf("expected conflict index %d, got %d", tc.want, idx)
			}
		})
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
	// Each entry is entryHeaderSize + 100 = 168 bytes.
	// Limit to 250 bytes: first entry (168) fits, second would push to 336.
	result := limitEntrySize(entries, 250)
	if len(result) != 1 {
		t.Fatalf("expected 1 entry with limit 250, got %d", len(result))
	}
}

func TestRaftLog_NewRaftLog_InitializesFromState(t *testing.T) {
	// The log must actually HOLD what the hard state claims. A Commit of 5
	// over an empty log is a corrupt recovery, and newRaftLog clamps it (see
	// TestNewRaftLog_TotalEntryLoss_ClampsCommittedToZero), so the fixture
	// carries the entries the state record refers to.
	state := proto.State{Term: 1, Commit: 5}
	logdb := newTestLogReader([]proto.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		{Index: 4, Term: 1}, {Index: 5, Term: 1},
	}, state)

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
	state := proto.State{Term: 2, Commit: 10}
	// Entries 8..10 are the committed tail above the snapshot; without them
	// on disk a Commit of 10 is not backed by anything and gets clamped.
	logdb := newTestLogReader([]proto.Entry{
		{Index: 8, Term: 2}, {Index: 9, Term: 2}, {Index: 10, Term: 2},
	}, state)
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

// TestRaftLog_NewRaftLog_SnapshotWithoutTail_ClampsToSnapshot covers the
// same recovery with the committed tail above the snapshot LOST. The
// snapshot index is the only thing the replica can prove it holds, so
// committed must come down to it rather than stay at the recovered 10.
func TestRaftLog_NewRaftLog_SnapshotWithoutTail_ClampsToSnapshot(t *testing.T) {
	logdb := newTestLogReader(nil, proto.State{Term: 2, Commit: 10})
	logdb.snapshot = proto.Snapshot{Index: 7, Term: 2}

	l := newRaftLog(logdb)
	if l.committed != 7 {
		t.Fatalf("committed = %d with only a snapshot at index 7 and no "+
			"entries above it: indices 8..10 are held by nothing, so "+
			"acknowledging them is a phantom match", l.committed)
	}
	if l.processed != 7 {
		t.Fatalf("expected processed 7 (snapshot index), got %d", l.processed)
	}
}

// TestRaftLog_NewRaftLog_ClampsUpToSnapshotIndex protects the OTHER
// direction of the commit reconciliation: a snapshot's index is COMMITTED by
// construction (a snapshot is only ever taken over committed state), so it is
// a lower bound on committed no matter what HardState survived.
//
// Without the clamp UP, restoring from a snapshot whose HardState.Commit was
// lost or lagging comes up with processed (= snapshot index) ABOVE committed.
// commitUpdate asserts LastApplied <= committed and LastApplied <= processed,
// so the very first commit cycle trips the invariant and takes the commit
// worker — and with it the shard — down.
func TestRaftLog_NewRaftLog_ClampsUpToSnapshotIndex(t *testing.T) {
	// HardState lags the snapshot: Commit=3, snapshot at 7.
	logdb := newTestLogReader(nil, proto.State{Term: 2, Commit: 3})
	logdb.snapshot = proto.Snapshot{Index: 7, Term: 2}

	l := newRaftLog(logdb)
	if l.committed != 7 {
		t.Fatalf("committed = %d with a snapshot at index 7: a snapshot is "+
			"only ever taken over COMMITTED state, so its index is a lower "+
			"bound on committed regardless of the recovered HardState",
			l.committed)
	}
	if l.processed > l.committed {
		t.Fatalf("processed = %d exceeds committed = %d: commitUpdate "+
			"asserts LastApplied <= committed on the first commit and would "+
			"take the commit worker down", l.processed, l.committed)
	}

	// The invariant commitUpdate enforces must actually hold end to end.
	l.commitUpdate(proto.UpdateCommit{Processed: true, LastApplied: 7})
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

// TestRaftLog_ErrorsAreLogDBSentinels verifies that every compacted and
// unavailable error the raft log produces -- whether it originates in the
// LogReader or in the raft log's own bounds checks -- is logdb.ErrCompacted or
// logdb.ErrUnavailable. One value means one condition everywhere: an embedder
// that finds one inside ShardFailedInfo.Err or a CommitError matches it with
// the public logdb name, and no raft-side copy of either sentinel exists to
// diverge from it.
func TestRaftLog_ErrorsAreLogDBSentinels(t *testing.T) {
	tests := []struct {
		name string
		call func() error
		want error
	}{
		{
			name: "term from LogReader compacted",
			call: func() error {
				l := &raftLog{
					logdb: &testLogDB{
						termFn: func(uint64) (uint64, error) { return 0, logdb.ErrCompacted },
						// Index 5 is compacted, so the log starts above it.
						indexRangeFn: func() (uint64, uint64) { return 6, 10 },
					},
					inmem: newInMemory(0),
				}
				_, err := l.term(5)
				return err
			},
			want: logdb.ErrCompacted,
		},
		{
			name: "term from LogReader unavailable",
			call: func() error {
				l := &raftLog{
					logdb: &testLogDB{
						termFn: func(uint64) (uint64, error) { return 0, logdb.ErrUnavailable },
						// Index 5 is inside the declared range, so the error
						// comes from the LogReader, not term()'s own bound.
						indexRangeFn: func() (uint64, uint64) { return 1, 10 },
					},
					inmem: newInMemory(0),
				}
				_, err := l.term(5)
				return err
			},
			want: logdb.ErrUnavailable,
		},
		{
			name: "term past the end of the log",
			call: func() error {
				_, err := newTestRaftLog(makeEntries(1, 6, 1)).term(10)
				return err
			},
			want: logdb.ErrUnavailable,
		},
		{
			name: "entries from LogReader compacted",
			call: func() error {
				l := &raftLog{
					logdb: &testLogDB{
						getRangeFn: func(uint64, uint64, uint64) ([]proto.Entry, error) {
							return nil, logdb.ErrCompacted
						},
					},
					inmem: newInMemory(10), // markerIndex > low forces the LogDB path
				}
				_, err := l.entries(1, 5, 0)
				return err
			},
			want: logdb.ErrCompacted,
		},
		{
			name: "entries from LogReader unavailable",
			call: func() error {
				l := &raftLog{
					logdb: &testLogDB{
						getRangeFn: func(uint64, uint64, uint64) ([]proto.Entry, error) {
							return nil, logdb.ErrUnavailable
						},
					},
					inmem: newInMemory(10),
				}
				_, err := l.entries(1, 5, 0)
				return err
			},
			want: logdb.ErrUnavailable,
		},
		{
			name: "entries low greater than high",
			call: func() error {
				_, err := newTestRaftLog(makeEntries(1, 6, 1)).entries(5, 3, 0)
				return err
			},
			want: logdb.ErrUnavailable,
		},
		{
			name: "entries past the end of the log",
			call: func() error {
				_, err := newTestRaftLog(makeEntries(1, 6, 1)).entries(10, 12, 0)
				return err
			},
			want: logdb.ErrUnavailable,
		},
		{
			name: "in-memory entries below the marker",
			call: func() error {
				im := newInMemory(5)
				_, err := im.getEntries(3, 6)
				return err
			},
			want: logdb.ErrCompacted,
		},
		{
			name: "in-memory entries above the upper bound",
			call: func() error {
				im := newInMemory(5)
				_, err := im.getEntries(6, 8)
				return err
			},
			want: logdb.ErrUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestRaftLog_GetConflictIndex_LogDBCompactedSkipped verifies the full
// bug-fix path: getConflictIndex receives logdb.ErrCompacted from the
// real LogDB layer (via term()), and the compacted entry is correctly
// skipped instead of treated as a false conflict.
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
		// Entries 1..3 are compacted and 4 is the last one the log holds.
		indexRangeFn: func() (uint64, uint64) { return 4, 4 },
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
		// Index 5 is compacted, so the log starts above it. Without a declared
		// range term() reports logdb.ErrUnavailable from its lastIndex bound and
		// tryCommit takes the plain-error path instead of the compacted one
		// this test is about.
		indexRangeFn: func() (uint64, uint64) { return 6, 10 },
	}
	l := &raftLog{
		logdb:     db,
		inmem:     newInMemory(0),
		committed: 0,
	}
	// tryCommit should see logdb.ErrCompacted (translated), set t=0,
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
