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

// Log edge case tests ported from dragonboat. These test entry retrieval
// from in-memory and LogDB stores, spanning retrieval, post-snapshot
// state, and uncommitted entry tracking.

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestLog_GetEntryFromInMem verifies that entries can be retrieved from
// the in-memory portion of the log when they have not yet been persisted.
func TestLog_GetEntryFromInMem(t *testing.T) {
	// Create raftLog with empty LogDB and entries only in inmem.
	l := newTestRaftLog(nil)
	entries := []proto.Entry{
		{Index: 1, Term: 1, Cmd: []byte("a")},
		{Index: 2, Term: 1, Cmd: []byte("b")},
		{Index: 3, Term: 2, Cmd: []byte("c")},
	}
	l.inmem.merge(entries)

	// All entries should come from inmem.
	result, err := l.entries(1, 4, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result))
	}
	for i, e := range result {
		if e.Index != entries[i].Index {
			t.Fatalf("entry %d: expected index %d, got %d", i, entries[i].Index, e.Index)
		}
		if e.Term != entries[i].Term {
			t.Fatalf("entry %d: expected term %d, got %d", i, entries[i].Term, e.Term)
		}
	}
}

// TestLog_GetEntryFromInMem_TermLookup verifies that term() returns
// the correct term for entries only in inmem.
func TestLog_GetEntryFromInMem_TermLookup(t *testing.T) {
	l := newTestRaftLog(nil)
	l.inmem.merge([]proto.Entry{
		{Index: 1, Term: 3},
		{Index: 2, Term: 5},
	})

	term, err := l.term(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if term != 3 {
		t.Fatalf("expected term 3, got %d", term)
	}

	term, err = l.term(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if term != 5 {
		t.Fatalf("expected term 5, got %d", term)
	}
}

// TestLog_GetEntryFromLogDB verifies that entries can be retrieved from
// the persistent LogDB when they are not in inmem.
func TestLog_GetEntryFromLogDB(t *testing.T) {
	// Create LogDB with entries, and raftLog with inmem starting after.
	dbEntries := []proto.Entry{
		{Index: 1, Term: 1, Cmd: []byte("x")},
		{Index: 2, Term: 1, Cmd: []byte("y")},
		{Index: 3, Term: 2, Cmd: []byte("z")},
	}
	logdb := newTestLogReader(dbEntries, proto.State{})
	l := &raftLog{
		logdb:     logdb,
		inmem:     newInMemory(3), // markerIndex=4, no inmem entries
		committed: 0,
		processed: 0,
	}

	// All entries should come from LogDB.
	result, err := l.entries(1, 4, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result))
	}
	for i, e := range result {
		if e.Index != dbEntries[i].Index {
			t.Fatalf("entry %d: expected index %d, got %d", i, dbEntries[i].Index, e.Index)
		}
	}
}

// TestLog_GetEntryFromLogDB_TermLookup verifies that term() falls back
// to LogDB when the entry is not in inmem.
func TestLog_GetEntryFromLogDB_TermLookup(t *testing.T) {
	dbEntries := []proto.Entry{
		{Index: 1, Term: 4},
		{Index: 2, Term: 7},
	}
	logdb := newTestLogReader(dbEntries, proto.State{})
	l := &raftLog{
		logdb: logdb,
		inmem: newInMemory(2), // markerIndex=3
	}

	term, err := l.term(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if term != 4 {
		t.Fatalf("expected term 4, got %d", term)
	}

	term, err = l.term(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if term != 7 {
		t.Fatalf("expected term 7, got %d", term)
	}
}

// TestLog_GetEntrySpanningBothStores verifies that entry retrieval
// correctly spans both LogDB (for persisted entries) and in-memory
// (for recent entries).
func TestLog_GetEntrySpanningBothStores(t *testing.T) {
	// LogDB has entries 1-3.
	dbEntries := []proto.Entry{
		{Index: 1, Term: 1, Cmd: []byte("a")},
		{Index: 2, Term: 1, Cmd: []byte("b")},
		{Index: 3, Term: 1, Cmd: []byte("c")},
	}
	logdb := newTestLogReader(dbEntries, proto.State{})

	// inmem has entries 4-5 (markerIndex=4).
	l := &raftLog{
		logdb:     logdb,
		inmem:     newInMemory(3), // markerIndex=4
		committed: 0,
		processed: 0,
	}
	l.inmem.merge([]proto.Entry{
		{Index: 4, Term: 2, Cmd: []byte("d")},
		{Index: 5, Term: 2, Cmd: []byte("e")},
	})

	// Retrieve entries spanning both stores: 2-5.
	result, err := l.entries(2, 6, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(result))
	}

	expectedIndices := []uint64{2, 3, 4, 5}
	expectedTerms := []uint64{1, 1, 2, 2}
	for i, e := range result {
		if e.Index != expectedIndices[i] {
			t.Fatalf("entry %d: expected index %d, got %d", i, expectedIndices[i], e.Index)
		}
		if e.Term != expectedTerms[i] {
			t.Fatalf("entry %d: expected term %d, got %d", i, expectedTerms[i], e.Term)
		}
	}
}

// TestLog_PostSnapshotRestoreState verifies that after restoring from a
// snapshot, firstIndex, lastIndex, committed, and processed are correct.
func TestLog_PostSnapshotRestoreState(t *testing.T) {
	entries := makeEntries(1, 6, 1) // indices 1-5
	l := newTestRaftLog(entries)
	l.committed = 3
	l.processed = 2

	ss := proto.Snapshot{Index: 20, Term: 5}
	l.restore(ss)

	// After restore, committed and processed should be at snapshot index.
	if l.committed != 20 {
		t.Fatalf("expected committed=20, got %d", l.committed)
	}
	if l.processed != 20 {
		t.Fatalf("expected processed=20, got %d", l.processed)
	}
	// lastIndex should be the snapshot index.
	if l.lastIndex() != 20 {
		t.Fatalf("expected lastIndex=20, got %d", l.lastIndex())
	}
	// firstIndex should be snapshot+1 since snapshot occupies that spot.
	if l.firstIndex() != 21 {
		t.Fatalf("expected firstIndex=21, got %d", l.firstIndex())
	}
	// No entries to apply since committed == processed.
	if l.hasEntriesToApply() {
		t.Fatal("expected no entries to apply after restore")
	}
}

// TestLog_UncommittedEntries verifies that only uncommitted entries
// (those beyond the committed index) can be identified.
func TestLog_UncommittedEntries(t *testing.T) {
	entries := makeEntries(1, 6, 1) // indices 1-5
	l := newTestRaftLog(entries)
	l.committed = 3

	// Entries 4 and 5 are uncommitted.
	uncommitted, err := l.entries(l.committed+1, l.lastIndex()+1, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(uncommitted) != 2 {
		t.Fatalf("expected 2 uncommitted entries, got %d", len(uncommitted))
	}
	if uncommitted[0].Index != 4 {
		t.Fatalf("expected first uncommitted at index 4, got %d", uncommitted[0].Index)
	}
	if uncommitted[1].Index != 5 {
		t.Fatalf("expected second uncommitted at index 5, got %d", uncommitted[1].Index)
	}
}

// TestLog_UncommittedEntries_AllCommitted verifies that when all entries
// are committed, the uncommitted range is empty.
func TestLog_UncommittedEntries_AllCommitted(t *testing.T) {
	entries := makeEntries(1, 4, 1) // indices 1-3
	l := newTestRaftLog(entries)
	l.committed = 3

	// No uncommitted entries.
	result, err := l.entries(l.committed+1, l.lastIndex()+1, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 && result != nil {
		t.Fatalf("expected 0 uncommitted entries, got %d", len(result))
	}
}

// TestLog_CommitUpdateAdvancesApplied verifies that appliedTo correctly
// tracks the applied index after commit updates.
func TestLog_CommitUpdateAdvancesApplied(t *testing.T) {
	entries := makeEntries(1, 6, 1) // indices 1-5
	l := newTestRaftLog(entries)
	l.committed = 5
	l.processed = 0

	// Apply entries 1-3.
	l.appliedTo(3)
	if l.processed != 3 {
		t.Fatalf("expected processed=3, got %d", l.processed)
	}

	// Verify hasEntriesToApply is still true (entries 4-5 remain).
	if !l.hasEntriesToApply() {
		t.Fatal("expected entries to apply (4-5 remain)")
	}

	// firstNotAppliedIndex should be 4.
	if l.firstNotAppliedIndex() != 4 {
		t.Fatalf("expected firstNotAppliedIndex=4, got %d", l.firstNotAppliedIndex())
	}

	// Apply the rest.
	l.appliedTo(5)
	if l.processed != 5 {
		t.Fatalf("expected processed=5, got %d", l.processed)
	}
	if l.hasEntriesToApply() {
		t.Fatal("expected no entries to apply after applying all")
	}
}

// TestLog_CommitUpdateAdvancesApplied_ZeroIsNoop verifies that
// appliedTo(0) is a no-op.
func TestLog_CommitUpdateAdvancesApplied_ZeroIsNoop(t *testing.T) {
	entries := makeEntries(1, 4, 1)
	l := newTestRaftLog(entries)
	l.committed = 3
	l.processed = 1

	l.appliedTo(0)
	if l.processed != 1 {
		t.Fatalf("expected processed unchanged at 1, got %d", l.processed)
	}
}

// TestLog_CommitUpdateAdvancesApplied_PanicOnBackward verifies that
// moving the applied index backward triggers an invariant assertion.
func TestLog_CommitUpdateAdvancesApplied_PanicOnBackward(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 5
	l.processed = 3

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic when moving applied backward")
		}
	}()
	l.appliedTo(2) // backward from 3
}

// TestLog_CommitUpdateAdvancesApplied_PanicBeyondCommitted verifies
// that applying beyond committed triggers an invariant assertion.
func TestLog_CommitUpdateAdvancesApplied_PanicBeyondCommitted(t *testing.T) {
	entries := makeEntries(1, 6, 1)
	l := newTestRaftLog(entries)
	l.committed = 3
	l.processed = 1

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic when applying beyond committed")
		}
	}()
	l.appliedTo(4) // beyond committed=3
}

// TestLog_FirstIndexOfTerm verifies binary search for term boundaries.
func TestLog_FirstIndexOfTerm(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
		{Index: 5, Term: 3},
	}
	l := newTestRaftLog(entries)

	tests := []struct {
		term     uint64
		expected uint64
	}{
		{term: 1, expected: 1},
		{term: 2, expected: 3},
		{term: 3, expected: 5},
		{term: 4, expected: 0}, // no entries at term 4
		{term: 0, expected: 0}, // no entries at term 0
	}

	for _, tt := range tests {
		result := l.firstIndexOfTerm(tt.term)
		if result != tt.expected {
			t.Fatalf("firstIndexOfTerm(%d): expected %d, got %d",
				tt.term, tt.expected, result)
		}
	}
}

// TestLog_LastIndexOfTerm verifies binary search for last entry of a term.
func TestLog_LastIndexOfTerm(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
		{Index: 5, Term: 3},
	}
	l := newTestRaftLog(entries)

	tests := []struct {
		term     uint64
		expected uint64
	}{
		{term: 1, expected: 2},
		{term: 2, expected: 4},
		{term: 3, expected: 5},
		{term: 4, expected: 0}, // no entries at term 4
	}

	for _, tt := range tests {
		result := l.lastIndexOfTerm(tt.term)
		if result != tt.expected {
			t.Fatalf("lastIndexOfTerm(%d): expected %d, got %d",
				tt.term, tt.expected, result)
		}
	}
}

// TestLog_LimitEntrySize verifies the entry size limiter.
func TestLog_LimitEntrySize(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1, Cmd: make([]byte, 100)},
		{Index: 2, Term: 1, Cmd: make([]byte, 100)},
		{Index: 3, Term: 1, Cmd: make([]byte, 100)},
	}

	t.Run("zero_maxSize_returns_all", func(t *testing.T) {
		result := limitEntrySize(entries, 0)
		if len(result) != 3 {
			t.Fatalf("expected 3 entries with maxSize=0, got %d", len(result))
		}
	})

	t.Run("always_returns_at_least_one", func(t *testing.T) {
		result := limitEntrySize(entries, 1)
		if len(result) < 1 {
			t.Fatal("expected at least 1 entry")
		}
	})

	t.Run("empty_slice_returns_empty", func(t *testing.T) {
		result := limitEntrySize(nil, 100)
		if result != nil {
			t.Fatalf("expected nil, got %v", result)
		}
	})
}
