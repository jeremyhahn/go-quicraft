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

// Regression tests for the memdb medium-severity findings: snapshot
// metadata persisted through SaveState (M2), deep-copied Cmd payloads (M3),
// and gap detection in EntryRange/IterateEntries (LogDB contract).

package memdb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestSaveStatePersistsSnapshotMetadata verifies that an Update carrying a
// snapshot persists the full metadata, not only the membership: a
// follower's InstallSnapshot lands via SaveState, and GetSnapshot must see
// it afterwards instead of ErrNoSnapshot.
func TestSaveStatePersistsSnapshotMetadata(t *testing.T) {
	db := New()
	snap := logdb.Snapshot{
		Index:    10,
		Term:     3,
		Filepath: "snap-10.dat",
		FileSize: 4096,
		Membership: logdb.Membership{
			ConfigChangeID: 7,
			Addresses:      map[uint64]string{1: "a1"},
		},
	}
	if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, Snapshot: snap}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot after SaveState: %v", err)
	}
	if got.Index != 10 || got.Term != 3 || got.Filepath != "snap-10.dat" || got.FileSize != 4096 {
		t.Fatalf("GetSnapshot = %+v, want the SaveState snapshot", got)
	}
}

// TestSaveStateIgnoresStaleSnapshot verifies the staleness check mirrors
// SaveSnapshot/waldb: an older snapshot in a later Update must not regress
// the stored snapshot or its membership.
func TestSaveStateIgnoresStaleSnapshot(t *testing.T) {
	db := New()
	newer := logdb.Snapshot{
		Index:      10,
		Term:       3,
		Membership: logdb.Membership{ConfigChangeID: 7},
	}
	stale := logdb.Snapshot{
		Index:      5,
		Term:       2,
		Membership: logdb.Membership{ConfigChangeID: 3},
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		State:    logdb.State{Term: 1, Commit: 1},
		Snapshot: newer,
	}}); err != nil {
		t.Fatalf("SaveState newer: %v", err)
	}
	if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, Snapshot: stale}}); err != nil {
		t.Fatalf("SaveState stale: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Index != 10 {
		t.Fatalf("GetSnapshot.Index = %d, want 10 (stale snapshot must not regress)", got.Index)
	}
	_, membership, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if membership.ConfigChangeID != 7 {
		t.Fatalf("membership.ConfigChangeID = %d, want 7 (stale snapshot must not regress membership)",
			membership.ConfigChangeID)
	}
}

// TestSaveStateDeepCopiesCmd verifies the stored log never aliases the
// caller's Cmd buffers: Host.Propose builds entries over application-owned
// buffers, and reuse after SaveState must not mutate the persisted log
// (replica divergence). Both merge paths are covered: the first append to
// an empty node and a follow-up append to existing entries.
func TestSaveStateDeepCopiesCmd(t *testing.T) {
	db := New()

	buf1 := []byte("first-payload")
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 1, Term: 1, Cmd: buf1}},
	}}); err != nil {
		t.Fatalf("SaveState 1: %v", err)
	}

	buf2 := []byte("second-payload")
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 2, Term: 1, Cmd: buf2}},
	}}); err != nil {
		t.Fatalf("SaveState 2: %v", err)
	}

	// Caller reuses its buffers.
	copy(buf1, "XXXXX")
	copy(buf2, "YYYYY")

	got, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 3, 0)
	if err != nil {
		t.Fatalf("IterateEntries: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if !bytes.Equal(got[0].Cmd, []byte("first-payload")) {
		t.Fatalf("entry 1 Cmd = %q, want %q (caller buffer aliased)", got[0].Cmd, "first-payload")
	}
	if !bytes.Equal(got[1].Cmd, []byte("second-payload")) {
		t.Fatalf("entry 2 Cmd = %q, want %q (caller buffer aliased)", got[1].Cmd, "second-payload")
	}
}

// TestEntryRangeRejectsGap verifies EntryRange reports ErrLogGapDetected
// when the stored entries are not contiguous, instead of a bare count that
// would hide the hole from LogReader.SetRange.
func TestEntryRangeRejectsGap(t *testing.T) {
	db := New()
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	// Non-contiguous append creates a hole (a persistence-pipeline bug).
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 10, Term: 1}},
	}}); err != nil {
		t.Fatalf("SaveState gap: %v", err)
	}

	if _, _, err := db.EntryRange(1, 1); !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("EntryRange = %v, want ErrLogGapDetected", err)
	}
}

// TestIterateEntriesRejectsInteriorHole verifies a hole inside the
// requested range fails with ErrLogGapDetected instead of returning fewer
// entries with a nil error.
func TestIterateEntriesRejectsInteriorHole(t *testing.T) {
	db := New()
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 10, Term: 1}},
	}}); err != nil {
		t.Fatalf("SaveState gap: %v", err)
	}

	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 11, 0); !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("IterateEntries = %v, want ErrLogGapDetected", err)
	}
}

// TestIterateEntriesReportsCompactedAfterFullCompaction is the regression test
// for the missing persistent compaction floor. RemoveEntriesTo sliced the
// entries and set them to nil when empty, recording no floor, so IterateEntries
// fell through to the empty-slice branch and returned ErrUnavailable. waldb
// keeps a durable compactedTo and checks it FIRST, returning ErrCompacted even
// with an empty index. The divergence matters to raft: ErrCompacted switches
// the leader to InstallSnapshot, ErrUnavailable makes it retry forever.
func TestIterateEntriesReportsCompactedAfterFullCompaction(t *testing.T) {
	db := New()
	defer db.Close()

	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{
			{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	// Full compaction: every entry is removed and the slice goes nil.
	if err := db.RemoveEntriesTo(1, 1, 3); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	for _, low := range []uint64{1, 2, 3} {
		if _, _, err := db.IterateEntries(nil, 0, 1, 1, low, low+1, 0); !errors.Is(err, logdb.ErrCompacted) {
			t.Errorf("IterateEntries(low=%d) error = %v, want ErrCompacted", low, err)
		}
	}
	// Above the floor there is simply nothing yet.
	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 4, 5, 0); !errors.Is(err, logdb.ErrUnavailable) {
		t.Errorf("IterateEntries(low=4) error = %v, want ErrUnavailable", err)
	}
}

// TestIterateEntriesReportsCompactedAfterPartialCompaction covers the
// floor check when entries remain, and that the floor never goes backwards.
func TestIterateEntriesReportsCompactedAfterPartialCompaction(t *testing.T) {
	db := New()
	defer db.Close()

	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{
			{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1},
		},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.RemoveEntriesTo(1, 1, 2); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	// A lower follow-up must not lower the floor.
	if err := db.RemoveEntriesTo(1, 1, 1); err != nil {
		t.Fatalf("RemoveEntriesTo(1): %v", err)
	}

	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 2, 4, 0); !errors.Is(err, logdb.ErrCompacted) {
		t.Errorf("IterateEntries(low=2) error = %v, want ErrCompacted", err)
	}
	got, _, err := db.IterateEntries(nil, 0, 1, 1, 3, 5, 0)
	if err != nil {
		t.Fatalf("IterateEntries(low=3): %v", err)
	}
	if len(got) != 2 || got[0].Index != 3 {
		t.Fatalf("IterateEntries(low=3) = %+v, want entries 3 and 4", got)
	}
}

// TestSaveStateDeepCopiesSnapshotMembership verifies the four membership maps
// carried by an Update's snapshot are cloned, matching the no-aliasing
// contract already honoured for Entry.Cmd. Retaining the caller's maps would
// let a later mutation rewrite the persisted membership.
func TestSaveStateDeepCopiesSnapshotMembership(t *testing.T) {
	db := New()
	defer db.Close()

	m := logdb.Membership{
		ConfigChangeID: 7,
		Addresses:      map[uint64]string{1: "a1"},
		Observers:      map[uint64]string{2: "o2"},
		Witnesses:      map[uint64]string{3: "w3"},
		Removed:        map[uint64]bool{4: true},
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		State:    logdb.State{Term: 2, Vote: 1, Commit: 5},
		Snapshot: logdb.Snapshot{Index: 5, Term: 2, Membership: m},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	// Mutate the caller's maps after the call.
	m.Addresses[1] = "MUTATED"
	m.Addresses[99] = "injected"
	m.Observers[2] = "MUTATED"
	m.Witnesses[3] = "MUTATED"
	m.Removed[4] = false
	m.Removed[98] = true

	_, stored, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if stored.Addresses[1] != "a1" || len(stored.Addresses) != 1 {
		t.Errorf("Addresses aliased the caller's map: %v", stored.Addresses)
	}
	if stored.Observers[2] != "o2" {
		t.Errorf("Observers aliased the caller's map: %v", stored.Observers)
	}
	if stored.Witnesses[3] != "w3" {
		t.Errorf("Witnesses aliased the caller's map: %v", stored.Witnesses)
	}
	if !stored.Removed[4] || len(stored.Removed) != 1 {
		t.Errorf("Removed aliased the caller's map: %v", stored.Removed)
	}

	ss, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if ss.Membership.Addresses[1] != "a1" {
		t.Errorf("snapshot membership aliased the caller's map: %v", ss.Membership.Addresses)
	}
}

// TestCopyMembershipPreservesNilMaps asserts a round-trip does not manufacture
// empty maps where the caller supplied none.
func TestCopyMembershipPreservesNilMaps(t *testing.T) {
	got := copyMembership(logdb.Membership{ConfigChangeID: 3})
	if got.ConfigChangeID != 3 {
		t.Fatalf("ConfigChangeID = %d, want 3", got.ConfigChangeID)
	}
	if got.Addresses != nil || got.Observers != nil || got.Witnesses != nil || got.Removed != nil {
		t.Fatalf("nil maps were replaced by empty maps: %+v", got)
	}
}

// TestIterateEntriesReportsHeadGap is the M1 regression test. A requested low
// below the first stored entry with NO compaction floor is a head GAP, not a
// compaction: the LogDB contract (and waldb) require ErrLogGapDetected there.
// Reporting ErrCompacted told raft the range was legitimately truncated and
// hid a missing prefix.
func TestIterateEntriesReportsHeadGap(t *testing.T) {
	db := New()
	defer db.Close()

	// Entries start at 5 with no RemoveEntriesTo call, so compactedTo is 0.
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{
			{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1},
		},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 3, 8, 0); !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("IterateEntries(low=3) = %v, want ErrLogGapDetected", err)
	}

	// Once a floor exists it wins: below the floor the range really is compacted.
	if err := db.RemoveEntriesTo(1, 1, 4); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 3, 8, 0); !errors.Is(err, logdb.ErrCompacted) {
		t.Fatalf("IterateEntries(low=3) below the floor = %v, want ErrCompacted", err)
	}
}

// TestSaveSnapshotUpdatesMembership is the M2 regression test. SaveSnapshot
// stored the snapshot but left nd.membership untouched and kept the caller's
// maps, so ReadState reported an EMPTY membership where waldb reports the
// snapshot's real one, and a later caller mutation rewrote the stored copy.
func TestSaveSnapshotUpdatesMembership(t *testing.T) {
	db := New()
	defer db.Close()

	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		State: logdb.State{Term: 2, Vote: 1, Commit: 5},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	m := logdb.Membership{
		ConfigChangeID: 9,
		Addresses:      map[uint64]string{1: "a1"},
		Observers:      map[uint64]string{2: "o2"},
		Witnesses:      map[uint64]string{3: "w3"},
		Removed:        map[uint64]bool{4: true},
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 5, Term: 2, Membership: m}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Mutate the caller's maps after the call.
	m.Addresses[1] = "MUTATED"
	m.Removed[4] = false

	_, stored, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if stored.ConfigChangeID != 9 {
		t.Fatalf("ReadState membership = %+v, want the snapshot's membership", stored)
	}
	if stored.Addresses[1] != "a1" || len(stored.Addresses) != 1 {
		t.Errorf("Addresses aliased the caller's map: %v", stored.Addresses)
	}
	if !stored.Removed[4] {
		t.Errorf("Removed aliased the caller's map: %v", stored.Removed)
	}

	ss, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if ss.Membership.Addresses[1] != "a1" {
		t.Errorf("snapshot membership aliased the caller's map: %v", ss.Membership.Addresses)
	}
}
