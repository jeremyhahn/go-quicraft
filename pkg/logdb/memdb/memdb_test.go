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

package memdb

import (
	"errors"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// --- Constructor and Name ---

func TestNewReturnsNonNil(t *testing.T) {
	db := New()
	if db == nil {
		t.Fatal("New() returned nil")
	}
}

func TestNameReturnsMemory(t *testing.T) {
	db := New()
	if got := db.Name(); got != "memory" {
		t.Errorf("Name() = %q, want %q", got, "memory")
	}
}

// --- Close ---

func TestCloseReturnsNilError(t *testing.T) {
	db := New()
	if err := db.Close(); err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	db := New()
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("second Close() error: %v", err)
	}
}

func TestOperationsAfterCloseReturnErrClosed(t *testing.T) {
	db := New()
	db.Close()

	tests := []struct {
		name string
		fn   func() error
	}{
		{"SaveState", func() error { return db.SaveState(nil) }},
		{"IterateEntries", func() error {
			_, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 0)
			return err
		}},
		{"ReadState", func() error {
			_, _, err := db.ReadState(1, 1)
			return err
		}},
		{"RemoveEntriesTo", func() error { return db.RemoveEntriesTo(1, 1, 5) }},
		{"Compact", func() error { return db.Compact(1, 1) }},
		{"SaveSnapshot", func() error { return db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 1}) }},
		{"GetSnapshot", func() error {
			_, err := db.GetSnapshot(1, 1)
			return err
		}},
		{"RemoveNodeData", func() error { return db.RemoveNodeData(1, 1) }},
		{"ListNodeInfo", func() error {
			_, err := db.ListNodeInfo()
			return err
		}},
		{"SaveBootstrap", func() error {
			return db.SaveBootstrap(1, 1, logdb.Bootstrap{})
		}},
		{"GetBootstrap", func() error {
			_, _, err := db.GetBootstrap(1, 1)
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if !errors.Is(err, logdb.ErrClosed) {
				t.Errorf("%s after Close: got %v, want ErrClosed", tt.name, err)
			}
		})
	}
}

// --- SaveState and ReadState ---

func TestSaveStateAndReadState(t *testing.T) {
	db := New()

	state := logdb.State{Term: 5, Vote: 2, Commit: 100}
	updates := []logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: state},
	}

	if err := db.SaveState(updates); err != nil {
		t.Fatalf("SaveState() error: %v", err)
	}

	got, _, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState() error: %v", err)
	}
	if got != state {
		t.Errorf("ReadState() = %+v, want %+v", got, state)
	}
}

func TestReadStateReturnsErrNoStateForUnknownNode(t *testing.T) {
	db := New()
	_, _, err := db.ReadState(99, 99)
	if !errors.Is(err, logdb.ErrNoState) {
		t.Errorf("ReadState() for unknown node: got %v, want ErrNoState", err)
	}
}

func TestReadStateReturnsErrNoStateWhenNoStateSaved(t *testing.T) {
	db := New()
	// Save only entries, no state.
	updates := []logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{{Index: 1, Term: 1}}},
	}
	db.SaveState(updates)

	_, _, err := db.ReadState(1, 1)
	if !errors.Is(err, logdb.ErrNoState) {
		t.Errorf("ReadState() with no state saved: got %v, want ErrNoState", err)
	}
}

func TestSaveStateWithNilUpdates(t *testing.T) {
	db := New()
	if err := db.SaveState(nil); err != nil {
		t.Errorf("SaveState(nil) error: %v", err)
	}
}

func TestSaveStateWithEmptyUpdates(t *testing.T) {
	db := New()
	if err := db.SaveState([]logdb.Update{}); err != nil {
		t.Errorf("SaveState(empty) error: %v", err)
	}
}

func TestSaveStateOverwritesPreviousState(t *testing.T) {
	db := New()

	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1, Vote: 1, Commit: 10}},
	})

	newState := logdb.State{Term: 2, Vote: 3, Commit: 50}
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: newState},
	})

	got, _, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState() error: %v", err)
	}
	if got != newState {
		t.Errorf("ReadState() = %+v, want %+v", got, newState)
	}
}

func TestSaveStatePersistsMembershipFromSnapshot(t *testing.T) {
	db := New()
	membership := logdb.Membership{
		ConfigChangeID: 7,
		Addresses:      map[uint64]string{1: "addr1", 2: "addr2"},
	}
	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			State:     logdb.State{Term: 1, Vote: 1, Commit: 10},
			Snapshot:  logdb.Snapshot{Index: 10, Term: 1, Membership: membership},
		},
	}

	db.SaveState(updates)

	_, gotMembership, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState() error: %v", err)
	}
	if gotMembership.ConfigChangeID != 7 {
		t.Errorf("Membership.ConfigChangeID = %d, want 7", gotMembership.ConfigChangeID)
	}
	if len(gotMembership.Addresses) != 2 {
		t.Errorf("Membership.Addresses len = %d, want 2", len(gotMembership.Addresses))
	}
}

// --- SaveState with Entries ---

func TestSaveStateWithEntries(t *testing.T) {
	db := New()
	entries := []logdb.Entry{
		{Index: 1, Term: 1, Cmd: []byte("cmd1")},
		{Index: 2, Term: 1, Cmd: []byte("cmd2")},
		{Index: 3, Term: 1, Cmd: []byte("cmd3")},
	}

	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: entries},
	})

	got, size, err := db.IterateEntries(nil, 0, 1, 1, 1, 4, 0)
	if err != nil {
		t.Fatalf("IterateEntries() error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("IterateEntries() returned %d entries, want 3", len(got))
	}
	if size == 0 {
		t.Error("IterateEntries() returned zero size")
	}
	for i, e := range got {
		if e.Index != entries[i].Index {
			t.Errorf("entry[%d].Index = %d, want %d", i, e.Index, entries[i].Index)
		}
	}
}

func TestSaveStateEntryOverwrite(t *testing.T) {
	db := New()

	// Save initial entries.
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: []byte("old1")},
			{Index: 2, Term: 1, Cmd: []byte("old2")},
			{Index: 3, Term: 1, Cmd: []byte("old3")},
		}},
	})

	// Overwrite from index 2 onward (truncate-and-append).
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 2, Term: 2, Cmd: []byte("new2")},
			{Index: 3, Term: 2, Cmd: []byte("new3")},
		}},
	})

	got, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 4, 0)
	if err != nil {
		t.Fatalf("IterateEntries() error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("IterateEntries() returned %d entries, want 3", len(got))
	}
	if string(got[0].Cmd) != "old1" {
		t.Errorf("entry[0].Cmd = %q, want %q", got[0].Cmd, "old1")
	}
	if got[1].Term != 2 || string(got[1].Cmd) != "new2" {
		t.Errorf("entry[1] = {Term:%d Cmd:%q}, want {Term:2 Cmd:new2}", got[1].Term, got[1].Cmd)
	}
	if got[2].Term != 2 || string(got[2].Cmd) != "new3" {
		t.Errorf("entry[2] = {Term:%d Cmd:%q}, want {Term:2 Cmd:new3}", got[2].Term, got[2].Cmd)
	}
}

func TestSaveStateMultipleNodesIsolated(t *testing.T) {
	db := New()

	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: []byte("shard1")},
		}},
		{ShardID: 2, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: []byte("shard2")},
		}},
	})

	// Verify isolation.
	got1, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 0)
	if err != nil {
		t.Fatalf("IterateEntries shard 1: %v", err)
	}
	got2, _, err := db.IterateEntries(nil, 0, 2, 1, 1, 2, 0)
	if err != nil {
		t.Fatalf("IterateEntries shard 2: %v", err)
	}

	if string(got1[0].Cmd) != "shard1" {
		t.Errorf("shard 1 entry = %q, want %q", got1[0].Cmd, "shard1")
	}
	if string(got2[0].Cmd) != "shard2" {
		t.Errorf("shard 2 entry = %q, want %q", got2[0].Cmd, "shard2")
	}
}

// --- IterateEntries ---

func TestIterateEntriesEmptyRange(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1},
		}},
	})

	// low == high should return nothing.
	got, size, err := db.IterateEntries(nil, 0, 1, 1, 5, 5, 0)
	if err != nil {
		t.Fatalf("IterateEntries() error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("IterateEntries() returned %d entries for empty range, want 0", len(got))
	}
	if size != 0 {
		t.Errorf("IterateEntries() size = %d, want 0", size)
	}
}

func TestIterateEntriesUnknownNode(t *testing.T) {
	db := New()
	_, _, err := db.IterateEntries(nil, 0, 99, 99, 1, 5, 0)
	if !errors.Is(err, logdb.ErrUnavailable) {
		t.Errorf("IterateEntries() unknown node: got %v, want ErrUnavailable", err)
	}
}

func TestIterateEntriesCompactedRange(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 5, Term: 1},
			{Index: 6, Term: 1},
			{Index: 7, Term: 1},
		}},
	})

	// A range entirely below the stored entries holds nothing and no
	// compaction floor was ever recorded, so the range is unavailable — not
	// compacted. (ErrCompacted here would diverge from waldb and tell raft the
	// prefix had been legitimately truncated.)
	_, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 3, 0)
	if !errors.Is(err, logdb.ErrUnavailable) {
		t.Errorf("IterateEntries() range below the log: got %v, want ErrUnavailable", err)
	}

	// Once a floor exists, the same range is genuinely compacted.
	if err := db.RemoveEntriesTo(1, 1, 4); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 3, 0); !errors.Is(err, logdb.ErrCompacted) {
		t.Errorf("IterateEntries() below the floor: got %v, want ErrCompacted", err)
	}
}

func TestIterateEntriesMaxSizeLimit(t *testing.T) {
	db := New()
	entries := make([]logdb.Entry, 10)
	for i := range entries {
		entries[i] = logdb.Entry{
			Index: uint64(i + 1),
			Term:  1,
			Cmd:   make([]byte, 100),
		}
	}
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: entries},
	})

	// Derived from logdb.EntryOverheadBytes rather than a literal: the budget
	// is defined in terms of that constant, so a layout change must move the
	// expectation with it instead of failing here as a stale number.
	perEntry := uint64(logdb.EntryOverheadBytes + 100)
	budget := 2*perEntry + perEntry/2 // room for 2, not 3

	// Request all 10 entries but limit the budget to two entries' worth.
	got, size, err := db.IterateEntries(nil, 0, 1, 1, 1, 11, budget)
	if err != nil {
		t.Fatalf("IterateEntries() error: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("IterateEntries() with maxSize=%d: got %d entries, want 2", budget, len(got))
	}
	if want := 2 * perEntry; size != want {
		t.Errorf("IterateEntries() size = %d, want %d", size, want)
	}
}

func TestIterateEntriesMaxSizeZeroIsUnlimited(t *testing.T) {
	db := New()
	entries := make([]logdb.Entry, 5)
	for i := range entries {
		entries[i] = logdb.Entry{Index: uint64(i + 1), Term: 1, Cmd: []byte("data")}
	}
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: entries},
	})

	got, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("IterateEntries() error: %v", err)
	}
	if len(got) != 5 {
		t.Errorf("IterateEntries() with maxSize=0: got %d entries, want 5", len(got))
	}
}

func TestIterateEntriesAlwaysReturnsAtLeastOneEntry(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: make([]byte, 1000)},
		}},
	})

	// maxSize = 1 byte, but we should still get the first entry.
	got, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 1)
	if err != nil {
		t.Fatalf("IterateEntries() error: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("IterateEntries() with tiny maxSize: got %d entries, want 1", len(got))
	}
}

func TestIterateEntriesAppendsToExistingSlice(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: []byte("a")},
			{Index: 2, Term: 1, Cmd: []byte("b")},
		}},
	})

	existing := []logdb.Entry{{Index: 99, Term: 99}}
	got, _, err := db.IterateEntries(existing, 0, 1, 1, 1, 3, 0)
	if err != nil {
		t.Fatalf("IterateEntries() error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("IterateEntries() returned %d entries, want 3 (1 existing + 2 new)", len(got))
	}
	if got[0].Index != 99 {
		t.Errorf("existing entry preserved: got Index=%d, want 99", got[0].Index)
	}
}

func TestIterateEntriesAccumulatesSize(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: []byte("hello")},
		}},
	})

	_, size, err := db.IterateEntries(nil, 100, 1, 1, 1, 2, 0)
	if err != nil {
		t.Fatalf("IterateEntries() error: %v", err)
	}
	// Initial size 100 + one entry's overhead + len("hello").
	if want := uint64(100 + logdb.EntryOverheadBytes + 5); size != want {
		t.Errorf("IterateEntries() size = %d, want %d", size, want)
	}
}

func TestIterateEntriesNodeWithNoEntries(t *testing.T) {
	db := New()
	// Create node via state save but with no entries.
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1}},
	})

	_, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 0)
	if !errors.Is(err, logdb.ErrUnavailable) {
		t.Errorf("IterateEntries() empty node: got %v, want ErrUnavailable", err)
	}
}

// --- Snapshot ---

func TestSaveAndGetSnapshot(t *testing.T) {
	db := New()
	snap := logdb.Snapshot{
		Index:    100,
		Term:     5,
		Filepath: "/data/snap-100",
		FileSize: 4096,
		Membership: logdb.Membership{
			Addresses: map[uint64]string{1: "addr1"},
		},
	}

	if err := db.SaveSnapshot(1, 1, snap); err != nil {
		t.Fatalf("SaveSnapshot() error: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot() error: %v", err)
	}
	if got.Index != snap.Index || got.Term != snap.Term {
		t.Errorf("GetSnapshot() = {Index:%d Term:%d}, want {Index:%d Term:%d}",
			got.Index, got.Term, snap.Index, snap.Term)
	}
	if got.Filepath != snap.Filepath {
		t.Errorf("GetSnapshot().Filepath = %q, want %q", got.Filepath, snap.Filepath)
	}
}

func TestGetSnapshotReturnsErrNoSnapshotForUnknownNode(t *testing.T) {
	db := New()
	_, err := db.GetSnapshot(99, 99)
	if !errors.Is(err, logdb.ErrNoSnapshot) {
		t.Errorf("GetSnapshot() unknown node: got %v, want ErrNoSnapshot", err)
	}
}

func TestGetSnapshotReturnsErrNoSnapshotWhenNoneSaved(t *testing.T) {
	db := New()
	// Create node with state but no snapshot.
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1}},
	})

	_, err := db.GetSnapshot(1, 1)
	if !errors.Is(err, logdb.ErrNoSnapshot) {
		t.Errorf("GetSnapshot() no snapshot: got %v, want ErrNoSnapshot", err)
	}
}

func TestSaveSnapshotRejectsOlderSnapshot(t *testing.T) {
	db := New()
	db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 100, Term: 5})

	err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 50, Term: 3})
	if !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
		t.Errorf("SaveSnapshot() older snapshot: got %v, want ErrSnapshotOutOfDate", err)
	}
}

func TestSaveSnapshotRejectsEqualIndexSnapshot(t *testing.T) {
	db := New()
	db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 100, Term: 5})

	err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 100, Term: 5})
	if !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
		t.Errorf("SaveSnapshot() equal index: got %v, want ErrSnapshotOutOfDate", err)
	}
}

func TestSaveSnapshotAcceptsNewerSnapshot(t *testing.T) {
	db := New()
	db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 100, Term: 5})

	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 200, Term: 6}); err != nil {
		t.Errorf("SaveSnapshot() newer snapshot: got %v, want nil", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot() error: %v", err)
	}
	if got.Index != 200 {
		t.Errorf("GetSnapshot().Index = %d, want 200", got.Index)
	}
}

// --- Bootstrap ---

func TestSaveAndGetBootstrap(t *testing.T) {
	db := New()
	boot := logdb.Bootstrap{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2"},
		Join:      true,
		Type:      3,
	}

	if err := db.SaveBootstrap(1, 1, boot); err != nil {
		t.Fatalf("SaveBootstrap() error: %v", err)
	}

	got, found, err := db.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap() error: %v", err)
	}
	if !found {
		t.Fatal("GetBootstrap() found = false, want true")
	}
	if !got.Join || got.Type != 3 || len(got.Addresses) != 2 {
		t.Errorf("GetBootstrap() = %+v, want %+v", got, boot)
	}
}

func TestGetBootstrapNotFoundForUnknownNode(t *testing.T) {
	db := New()
	_, found, err := db.GetBootstrap(99, 99)
	if err != nil {
		t.Fatalf("GetBootstrap() error: %v", err)
	}
	if found {
		t.Error("GetBootstrap() found = true for unknown node, want false")
	}
}

func TestGetBootstrapNotFoundWhenNoneSaved(t *testing.T) {
	db := New()
	// Create node with state but no bootstrap.
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1}},
	})

	_, found, err := db.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap() error: %v", err)
	}
	if found {
		t.Error("GetBootstrap() found = true when none saved, want false")
	}
}

func TestSaveBootstrapOverwritesPrevious(t *testing.T) {
	db := New()
	db.SaveBootstrap(1, 1, logdb.Bootstrap{
		Addresses: map[uint64]string{1: "old"},
		Type:      1,
	})

	newBoot := logdb.Bootstrap{
		Addresses: map[uint64]string{2: "new"},
		Type:      2,
		Join:      true,
	}
	db.SaveBootstrap(1, 1, newBoot)

	got, found, err := db.GetBootstrap(1, 1)
	if err != nil || !found {
		t.Fatalf("GetBootstrap() error=%v found=%v", err, found)
	}
	if got.Type != 2 || !got.Join {
		t.Errorf("GetBootstrap() = %+v, want %+v", got, newBoot)
	}
}

// --- EntryRange ---

func TestEntryRangeReturnsCorrectRange(t *testing.T) {
	db := New()
	entries := []logdb.Entry{
		{Index: 5, Term: 1},
		{Index: 6, Term: 1},
		{Index: 7, Term: 2},
	}
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: entries},
	})

	first, length, err := db.EntryRange(1, 1)
	if err != nil {
		t.Fatalf("EntryRange() error: %v", err)
	}
	if first != 5 {
		t.Errorf("EntryRange() firstIndex = %d, want 5", first)
	}
	if length != 3 {
		t.Errorf("EntryRange() length = %d, want 3", length)
	}
}

func TestEntryRangeUnknownNodeReturnsZero(t *testing.T) {
	db := New()
	first, length, err := db.EntryRange(99, 99)
	if err != nil {
		t.Fatalf("EntryRange() error: %v", err)
	}
	if first != 0 || length != 0 {
		t.Errorf("EntryRange() = (%d, %d), want (0, 0)", first, length)
	}
}

func TestEntryRangeEmptyNodeReturnsZero(t *testing.T) {
	db := New()
	// Save state without entries to create the node.
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1}},
	})

	first, length, err := db.EntryRange(1, 1)
	if err != nil {
		t.Fatalf("EntryRange() error: %v", err)
	}
	if first != 0 || length != 0 {
		t.Errorf("EntryRange() = (%d, %d), want (0, 0)", first, length)
	}
}

// TestEntryRangeAfterCompaction checks the range AND that the range agrees with
// what the log will actually serve. Reporting the right numbers is only half of
// it: EntryRange is what a recovering LogReader initialises its bounds from, so
// a range that names entries the store then refuses is worse than a wrong
// count — the caller reads a gap where it was told there was a log.
func TestEntryRangeAfterCompaction(t *testing.T) {
	db := New()
	entries := make([]logdb.Entry, 10)
	for i := range entries {
		entries[i] = logdb.Entry{Index: uint64(i + 1), Term: 1, Cmd: []byte("c")}
	}
	if err := db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: entries},
	}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	first, length, err := db.EntryRange(1, 1)
	if err != nil {
		t.Fatalf("EntryRange() error: %v", err)
	}
	if first != 6 {
		t.Errorf("EntryRange() firstIndex = %d, want 6", first)
	}
	if length != 5 {
		t.Errorf("EntryRange() length = %d, want 5", length)
	}

	// The advertised range must be readable in full.
	got, _, err := db.IterateEntries(nil, 0, 1, 1, first, first+length, 0)
	if err != nil {
		t.Fatalf("IterateEntries over the range EntryRange just advertised "+
			"([%d,%d)): %v", first, first+length, err)
	}
	if uint64(len(got)) != length {
		t.Fatalf("EntryRange advertised %d entries but only %d are readable",
			length, len(got))
	}
	for i, e := range got {
		if want := first + uint64(i); e.Index != want {
			t.Fatalf("entry %d of the advertised range has index %d, want %d",
				i, e.Index, want)
		}
	}
	// And the compacted prefix must read as compacted, not as merely absent:
	// raft falls back to InstallSnapshot on ErrCompacted and retries forever
	// on ErrUnavailable.
	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 6, 0); !errors.Is(err, logdb.ErrCompacted) {
		t.Fatalf("reading the compacted prefix = %v, want ErrCompacted", err)
	}
}

func TestEntryRangeOnClosedDBReturnsError(t *testing.T) {
	db := New()
	db.Close()
	_, _, err := db.EntryRange(1, 1)
	if !errors.Is(err, logdb.ErrClosed) {
		t.Errorf("EntryRange() on closed DB: got %v, want ErrClosed", err)
	}
}

// --- RemoveEntriesTo ---

func TestRemoveEntriesTo(t *testing.T) {
	db := New()
	entries := make([]logdb.Entry, 10)
	for i := range entries {
		entries[i] = logdb.Entry{Index: uint64(i + 1), Term: 1, Cmd: []byte("data")}
	}
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: entries},
	})

	if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
		t.Fatalf("RemoveEntriesTo() error: %v", err)
	}

	// Entries 1-5 should be compacted.
	_, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 6, 0)
	if !errors.Is(err, logdb.ErrCompacted) {
		t.Errorf("after RemoveEntriesTo(5), IterateEntries(1,6): got %v, want ErrCompacted", err)
	}

	// Entries 6-10 should still be available.
	got, _, err := db.IterateEntries(nil, 0, 1, 1, 6, 11, 0)
	if err != nil {
		t.Fatalf("IterateEntries(6,11) error: %v", err)
	}
	if len(got) != 5 {
		t.Errorf("IterateEntries(6,11) returned %d entries, want 5", len(got))
	}
	if got[0].Index != 6 {
		t.Errorf("first remaining entry Index = %d, want 6", got[0].Index)
	}
}

func TestRemoveEntriesToUnknownNodeIsNoOp(t *testing.T) {
	db := New()
	if err := db.RemoveEntriesTo(99, 99, 5); err != nil {
		t.Errorf("RemoveEntriesTo() unknown node: got %v, want nil", err)
	}
}

func TestRemoveEntriesToAllEntries(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
		}},
	})

	if err := db.RemoveEntriesTo(1, 1, 2); err != nil {
		t.Fatalf("RemoveEntriesTo() error: %v", err)
	}

	_, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 3, 0)
	if !errors.Is(err, logdb.ErrUnavailable) && !errors.Is(err, logdb.ErrCompacted) {
		t.Errorf("after removing all entries: got %v, want ErrUnavailable or ErrCompacted", err)
	}
}

func TestRemoveEntriesToBeyondLastEntry(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
		}},
	})

	// Remove up to index 100 (beyond last entry 2).
	if err := db.RemoveEntriesTo(1, 1, 100); err != nil {
		t.Fatalf("RemoveEntriesTo() beyond last: error %v", err)
	}
}

func TestRemoveEntriesToIndexZeroIsNoOp(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1},
		}},
	})

	if err := db.RemoveEntriesTo(1, 1, 0); err != nil {
		t.Fatalf("RemoveEntriesTo(0) error: %v", err)
	}

	// Entry should still exist.
	got, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 0)
	if err != nil {
		t.Fatalf("IterateEntries() error: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("entry count = %d, want 1", len(got))
	}
}

// --- Compact ---

// TestCompactIsNoOp covers one narrow contract point: Compact on a node this
// store has never heard of succeeds rather than erroring. That it does not
// DISTURB a populated node is a cross-backend property and is asserted in
// TestConformanceCompactPreservesEverythingAboveTheFloor; asserting it here as
// well would only duplicate it. Keeping the scope explicit matters because this
// test alone does not catch a Compact rewritten to delete the node's entries,
// hard state and snapshot.
func TestCompactIsNoOp(t *testing.T) {
	db := New()
	if err := db.Compact(1, 1); err != nil {
		t.Errorf("Compact() on an unknown node = %v, want nil", err)
	}
}

func TestCompactOnClosedDBReturnsErrClosed(t *testing.T) {
	db := New()
	db.Close()
	if err := db.Compact(1, 1); !errors.Is(err, logdb.ErrClosed) {
		t.Errorf("Compact() after close: got %v, want ErrClosed", err)
	}
}

// --- RemoveNodeData ---

func TestRemoveNodeData(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1,
			State:   logdb.State{Term: 1},
			Entries: []logdb.Entry{{Index: 1, Term: 1}},
		},
	})
	db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 1, Term: 1})
	db.SaveBootstrap(1, 1, logdb.Bootstrap{Addresses: map[uint64]string{1: "a"}})

	if err := db.RemoveNodeData(1, 1); err != nil {
		t.Fatalf("RemoveNodeData() error: %v", err)
	}

	// All queries should now fail with appropriate errors.
	_, _, err := db.ReadState(1, 1)
	if !errors.Is(err, logdb.ErrNoState) {
		t.Errorf("ReadState after remove: got %v, want ErrNoState", err)
	}

	_, err = db.GetSnapshot(1, 1)
	if !errors.Is(err, logdb.ErrNoSnapshot) {
		t.Errorf("GetSnapshot after remove: got %v, want ErrNoSnapshot", err)
	}

	_, found, err := db.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap after remove: error %v", err)
	}
	if found {
		t.Error("GetBootstrap after remove: found = true, want false")
	}
}

func TestRemoveNodeDataUnknownNodeIsNoOp(t *testing.T) {
	db := New()
	if err := db.RemoveNodeData(99, 99); err != nil {
		t.Errorf("RemoveNodeData() unknown node: got %v, want nil", err)
	}
}

// --- ListNodeInfo ---

func TestListNodeInfoEmpty(t *testing.T) {
	db := New()
	nodes, err := db.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo() error: %v", err)
	}
	if len(nodes) != 0 {
		t.Errorf("ListNodeInfo() returned %d nodes, want 0", len(nodes))
	}
}

func TestListNodeInfoMultipleNodes(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1}},
		{ShardID: 1, ReplicaID: 2, State: logdb.State{Term: 1}},
		{ShardID: 2, ReplicaID: 1, State: logdb.State{Term: 1}},
	})

	nodes, err := db.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo() error: %v", err)
	}
	if len(nodes) != 3 {
		t.Errorf("ListNodeInfo() returned %d nodes, want 3", len(nodes))
	}

	// Build a set for verification (map iteration order is non-deterministic).
	seen := make(map[logdb.NodeInfo]bool)
	for _, ni := range nodes {
		seen[ni] = true
	}

	expected := []logdb.NodeInfo{
		{ShardID: 1, ReplicaID: 1},
		{ShardID: 1, ReplicaID: 2},
		{ShardID: 2, ReplicaID: 1},
	}
	for _, want := range expected {
		if !seen[want] {
			t.Errorf("ListNodeInfo() missing node %+v", want)
		}
	}
}

func TestListNodeInfoAfterRemoveNodeData(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1}},
		{ShardID: 2, ReplicaID: 1, State: logdb.State{Term: 1}},
	})

	db.RemoveNodeData(1, 1)

	nodes, err := db.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo() error: %v", err)
	}
	if len(nodes) != 1 {
		t.Errorf("ListNodeInfo() returned %d nodes, want 1", len(nodes))
	}
	if nodes[0].ShardID != 2 || nodes[0].ReplicaID != 1 {
		t.Errorf("remaining node = %+v, want {ShardID:2 ReplicaID:1}", nodes[0])
	}
}

// --- Concurrency ---

func TestConcurrentSaveAndRead(t *testing.T) {
	db := New()
	const numWorkers = 10
	const numOps = 100

	var wg sync.WaitGroup
	wg.Add(numWorkers * 2)

	// Writers.
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			shardID := uint64(workerID)
			for i := 0; i < numOps; i++ {
				db.SaveState([]logdb.Update{
					{
						ShardID:   shardID,
						ReplicaID: 1,
						State:     logdb.State{Term: uint64(i + 1)},
						Entries: []logdb.Entry{
							{Index: uint64(i + 1), Term: uint64(i + 1), Cmd: []byte("cmd")},
						},
					},
				})
			}
		}(w)
	}

	// Readers.
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			shardID := uint64(workerID)
			for i := 0; i < numOps; i++ {
				db.ReadState(shardID, 1)
				db.IterateEntries(nil, 0, shardID, 1, 1, uint64(i+2), 0)
			}
		}(w)
	}

	wg.Wait()

	// Verify final state for each worker.
	for w := 0; w < numWorkers; w++ {
		state, _, err := db.ReadState(uint64(w), 1)
		if err != nil {
			t.Errorf("worker %d ReadState error: %v", w, err)
			continue
		}
		if state.Term != numOps {
			t.Errorf("worker %d final term = %d, want %d", w, state.Term, numOps)
		}
	}
}

func TestConcurrentSnapshotOperations(t *testing.T) {
	db := New()
	const numWriters = 5
	const numOps = 50

	var wg sync.WaitGroup
	wg.Add(numWriters)

	for w := 0; w < numWriters; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				snap := logdb.Snapshot{
					Index:    uint64((workerID * numOps) + i + 1),
					Term:     uint64(i + 1),
					Filepath: "/data/snap",
				}
				// Ignore ErrSnapshotOutOfDate from concurrent writers.
				err := db.SaveSnapshot(1, 1, snap)
				if err != nil && !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
					t.Errorf("SaveSnapshot error: %v", err)
				}
			}
		}(w)
	}

	wg.Wait()

	// A snapshot should exist.
	snap, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot() error: %v", err)
	}
	if snap.Index == 0 {
		t.Error("GetSnapshot().Index = 0, expected non-zero after concurrent writes")
	}
}

func TestConcurrentBootstrapOperations(t *testing.T) {
	db := New()
	const numGoroutines = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			db.SaveBootstrap(uint64(id), 1, logdb.Bootstrap{
				Addresses: map[uint64]string{1: "addr"},
				Type:      uint64(id),
			})
		}(g)

		go func(id int) {
			defer wg.Done()
			db.GetBootstrap(uint64(id), 1)
		}(g)
	}

	wg.Wait()
}

func TestConcurrentListNodeInfo(t *testing.T) {
	db := New()
	const numGoroutines = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			db.SaveState([]logdb.Update{
				{ShardID: uint64(id), ReplicaID: 1, State: logdb.State{Term: 1}},
			})
		}(g)

		go func() {
			defer wg.Done()
			db.ListNodeInfo()
		}()
	}

	wg.Wait()
}

// --- mergeEntries ---

func TestMergeEntriesEmptyExisting(t *testing.T) {
	incoming := []logdb.Entry{
		{Index: 1, Term: 1, Cmd: []byte("a")},
		{Index: 2, Term: 1, Cmd: []byte("b")},
	}

	got := mergeEntries(nil, incoming)
	if len(got) != 2 {
		t.Fatalf("mergeEntries(nil, ...) returned %d entries, want 2", len(got))
	}
	if got[0].Index != 1 || got[1].Index != 2 {
		t.Errorf("mergeEntries result = %+v, want indices [1, 2]", got)
	}
}

func TestMergeEntriesContiguous(t *testing.T) {
	existing := []logdb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	}
	incoming := []logdb.Entry{
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
	}

	got := mergeEntries(existing, incoming)
	if len(got) != 4 {
		t.Fatalf("mergeEntries contiguous: got %d entries, want 4", len(got))
	}
	for i, e := range got {
		if e.Index != uint64(i+1) {
			t.Errorf("entry[%d].Index = %d, want %d", i, e.Index, i+1)
		}
	}
}

func TestMergeEntriesOverlapping(t *testing.T) {
	existing := []logdb.Entry{
		{Index: 1, Term: 1, Cmd: []byte("old1")},
		{Index: 2, Term: 1, Cmd: []byte("old2")},
		{Index: 3, Term: 1, Cmd: []byte("old3")},
	}
	incoming := []logdb.Entry{
		{Index: 2, Term: 2, Cmd: []byte("new2")},
		{Index: 3, Term: 2, Cmd: []byte("new3")},
		{Index: 4, Term: 2, Cmd: []byte("new4")},
	}

	got := mergeEntries(existing, incoming)
	if len(got) != 4 {
		t.Fatalf("mergeEntries overlapping: got %d entries, want 4", len(got))
	}
	if string(got[0].Cmd) != "old1" {
		t.Errorf("entry[0].Cmd = %q, want %q", got[0].Cmd, "old1")
	}
	if string(got[1].Cmd) != "new2" {
		t.Errorf("entry[1].Cmd = %q, want %q", got[1].Cmd, "new2")
	}
	if string(got[3].Cmd) != "new4" {
		t.Errorf("entry[3].Cmd = %q, want %q", got[3].Cmd, "new4")
	}
}

func TestMergeEntriesCompleteOverwrite(t *testing.T) {
	existing := []logdb.Entry{
		{Index: 1, Term: 1, Cmd: []byte("old")},
	}
	incoming := []logdb.Entry{
		{Index: 1, Term: 2, Cmd: []byte("new")},
	}

	got := mergeEntries(existing, incoming)
	if len(got) != 1 {
		t.Fatalf("mergeEntries complete overwrite: got %d entries, want 1", len(got))
	}
	if got[0].Term != 2 || string(got[0].Cmd) != "new" {
		t.Errorf("entry = %+v, want Term=2 Cmd=new", got[0])
	}
}

// --- Interface compliance ---

func TestInterfaceCompliance(t *testing.T) {
	var db logdb.LogDB = New()
	if db.Name() != "memory" {
		t.Errorf("Name() via interface = %q, want %q", db.Name(), "memory")
	}
}

// --- Edge cases ---

func TestSaveStateZeroStateIsIgnored(t *testing.T) {
	db := New()
	// A zero State should not mark hasState as true.
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{}},
	})

	_, _, err := db.ReadState(1, 1)
	if !errors.Is(err, logdb.ErrNoState) {
		t.Errorf("ReadState after zero-state save: got %v, want ErrNoState", err)
	}
}

func TestSaveStateWithEntriesOnlyCreatesNode(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1},
		}},
	})

	nodes, err := db.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo() error: %v", err)
	}
	if len(nodes) != 1 {
		t.Errorf("ListNodeInfo() = %d nodes, want 1", len(nodes))
	}
}

func TestRemoveEntriesToEmptyNodeIsNoOp(t *testing.T) {
	db := New()
	// Create node with state only.
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1}},
	})

	if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
		t.Errorf("RemoveEntriesTo on empty node: got %v, want nil", err)
	}
}

func TestMaxUint64NodeKeys(t *testing.T) {
	db := New()
	maxID := ^uint64(0)

	db.SaveState([]logdb.Update{
		{ShardID: maxID, ReplicaID: maxID, State: logdb.State{Term: 42}},
	})

	state, _, err := db.ReadState(maxID, maxID)
	if err != nil {
		t.Fatalf("ReadState max IDs: error %v", err)
	}
	if state.Term != 42 {
		t.Errorf("state.Term = %d, want 42", state.Term)
	}
}

func TestIterateEntriesPartialRange(t *testing.T) {
	db := New()
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: []byte("a")},
			{Index: 2, Term: 1, Cmd: []byte("b")},
			{Index: 3, Term: 1, Cmd: []byte("c")},
			{Index: 4, Term: 1, Cmd: []byte("d")},
			{Index: 5, Term: 1, Cmd: []byte("e")},
		}},
	})

	// Read a subset [2, 4).
	got, _, err := db.IterateEntries(nil, 0, 1, 1, 2, 4, 0)
	if err != nil {
		t.Fatalf("IterateEntries(2,4) error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if got[0].Index != 2 || got[1].Index != 3 {
		t.Errorf("entries = [%d, %d], want [2, 3]", got[0].Index, got[1].Index)
	}
}

func TestEntrySizeCalculation(t *testing.T) {
	db := New()
	cmd := make([]byte, 24)
	db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: cmd},
			{Index: 2, Term: 1, Cmd: cmd},
			{Index: 3, Term: 1, Cmd: cmd},
		}},
	})

	_, size, err := db.IterateEntries(nil, 0, 1, 1, 1, 4, 0)
	if err != nil {
		t.Fatalf("IterateEntries() error: %v", err)
	}
	if want := uint64(3 * (logdb.EntryOverheadBytes + 24)); size != want {
		t.Errorf("size = %d, want %d", size, want)
	}
}

func TestRemoveNodeDataThenRecreateSameNodeIsRefused(t *testing.T) {
	db := New()

	// Create and remove a node: its replica ID is retired, and every writer
	// refuses it from then on, with nothing stored.
	if err := db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1}},
	}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.RemoveNodeData(1, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}

	writers := map[string]func() error{
		"SaveState": func() error {
			return db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 2}}})
		},
		"SaveSnapshot":  func() error { return db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 5, Term: 1}) },
		"SaveBootstrap": func() error { return db.SaveBootstrap(1, 1, logdb.Bootstrap{Type: 1}) },
	}
	for name, write := range writers {
		if err := write(); !errors.Is(err, logdb.ErrReplicaRetired) {
			t.Fatalf("%s for a removed node = %v, want logdb.ErrReplicaRetired", name, err)
		}
	}
	if _, _, err := db.ReadState(1, 1); !errors.Is(err, logdb.ErrNoState) {
		t.Fatalf("ReadState after refused writes = %v, want ErrNoState", err)
	}
}

// TestRemoveEntriesToReleasesMemory asserts what its name claims: that
// compaction actually RELEASES the compacted prefix, rather than re-slicing
// past it and leaving the whole original array — every dropped Cmd payload
// included — reachable from the surviving slice.
//
// Asserting only that the remaining entries read back is not a test of that. It
// passes identically for `nd.entries = nd.entries[cutIdx:]`, which releases
// nothing at all and turns a compacted node into a permanent hold on the log it
// was compacting away.
//
// The release is proven by aliasing rather than by measuring the heap: the old
// backing array is overwritten after the compaction, and a slice that merely
// re-sliced it would then serve the overwritten bytes. A GC-and-measure test
// would be nondeterministic; this one cannot be.
func TestRemoveEntriesToReleasesMemory(t *testing.T) {
	db := New()
	entries := make([]logdb.Entry, 1000)
	for i := range entries {
		entries[i] = logdb.Entry{Index: uint64(i + 1), Term: 1, Cmd: make([]byte, 1024)}
	}
	if err := db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, Entries: entries},
	}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	nd := db.getNode(1, 1)
	if nd == nil {
		t.Fatal("node missing after SaveState")
	}
	// A full view of the pre-compaction backing array, retained across the
	// compaction so it can be poisoned afterwards.
	nd.mu.RLock()
	old := nd.entries[:cap(nd.entries)]
	nd.mu.RUnlock()

	// Remove first 500.
	if err := db.RemoveEntriesTo(1, 1, 500); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	// The surviving slice must not carry the dropped prefix's capacity with it.
	nd.mu.RLock()
	live, capacity := len(nd.entries), cap(nd.entries)
	nd.mu.RUnlock()
	if live != 500 {
		t.Fatalf("live entries = %d, want 500", live)
	}
	if capacity != live {
		t.Errorf("compacted slice has cap %d for len %d: it is a re-slice of the "+
			"original array, so the 500 compacted entries and every Cmd payload "+
			"they hold are still reachable and can never be collected",
			capacity, live)
	}

	// Poison the pre-compaction array. A compacted slice that shares it now
	// serves zeroed entries; one that copied is untouched.
	clear(old)

	got, _, err := db.IterateEntries(nil, 0, 1, 1, 501, 1001, 0)
	if err != nil {
		t.Fatalf("IterateEntries after compaction: error %v (the surviving entries "+
			"still alias the pre-compaction backing array, so nothing was released)", err)
	}
	if len(got) != 500 {
		t.Fatalf("remaining entries = %d, want 500", len(got))
	}
	for i, e := range got {
		if want := uint64(501 + i); e.Index != want {
			t.Fatalf("entry %d has index %d, want %d: the surviving entries alias "+
				"the pre-compaction array, which is exactly the memory the "+
				"compaction was supposed to release", i, e.Index, want)
		}
		if len(e.Cmd) != 1024 {
			t.Fatalf("entry %d lost its payload (len %d): the surviving entries "+
				"alias the pre-compaction array", e.Index, len(e.Cmd))
		}
	}
}

func TestMergeEntriesDoesNotMutateIncoming(t *testing.T) {
	existing := []logdb.Entry{
		{Index: 1, Term: 1, Cmd: []byte("a")},
		{Index: 2, Term: 1, Cmd: []byte("b")},
	}

	incoming := []logdb.Entry{
		{Index: 2, Term: 2, Cmd: []byte("c")},
	}
	incomingCopy := make([]logdb.Entry, len(incoming))
	copy(incomingCopy, incoming)

	result := mergeEntries(existing, incoming)

	// Incoming slice must not be mutated.
	for i := range incoming {
		if incoming[i].Index != incomingCopy[i].Index || incoming[i].Term != incomingCopy[i].Term {
			t.Errorf("mergeEntries mutated incoming[%d]: got %+v, want %+v",
				i, incoming[i], incomingCopy[i])
		}
	}

	// Result must contain the correct merged entries.
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	if result[0].Index != 1 || result[0].Term != 1 {
		t.Errorf("result[0]: got Index=%d Term=%d, want Index=1 Term=1",
			result[0].Index, result[0].Term)
	}
	if result[1].Index != 2 || result[1].Term != 2 {
		t.Errorf("result[1]: got Index=%d Term=%d, want Index=2 Term=2",
			result[1].Index, result[1].Term)
	}
}

// TestMemDB_NodeRemovedRetiresTheReplica verifies RemoveNodeData retires the
// replica ID: NodeRemoved reports it from then on, and only for that pair.
func TestMemDB_NodeRemovedRetiresTheReplica(t *testing.T) {
	m := New()
	if err := m.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1}}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if removed, err := m.NodeRemoved(1, 1); err != nil || removed {
		t.Fatalf("NodeRemoved before removal = (%v, %v), want (false, nil)", removed, err)
	}
	if err := m.RemoveNodeData(1, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}
	if removed, err := m.NodeRemoved(1, 1); err != nil || !removed {
		t.Fatalf("NodeRemoved after removal = (%v, %v), want (true, nil)", removed, err)
	}
	if removed, _ := m.NodeRemoved(1, 2); removed {
		t.Fatal("NodeRemoved reported a replica that was never removed")
	}
}

// TestRemoveEntriesToRefusesARetiredNode verifies RemoveEntriesTo refuses a
// node RemoveNodeData retired, as every other writer does, rather than
// reporting success for a node that no longer exists.
func TestRemoveEntriesToRefusesARetiredNode(t *testing.T) {
	db := New()
	if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.RemoveNodeData(1, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}
	if err := db.RemoveEntriesTo(1, 1, 1); !errors.Is(err, logdb.ErrReplicaRetired) {
		t.Fatalf("RemoveEntriesTo on a retired node = %v, want logdb.ErrReplicaRetired", err)
	}
}
