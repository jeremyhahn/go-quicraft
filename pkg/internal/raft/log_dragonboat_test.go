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

// Log edge case tests ported from dragonboat. These test entry and term
// retrieval from LogDB, post-snapshot state, and applied-index tracking.

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

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
