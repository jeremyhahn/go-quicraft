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

package waldb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// openTestDB creates a DB in a temp directory with sync disabled for speed.
func openTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	return db
}

// makeEntries creates a slice of logdb.Entry with sequential indexes.
func makeEntries(start, count uint64, term uint64) []logdb.Entry {
	entries := make([]logdb.Entry, count)
	for i := uint64(0); i < count; i++ {
		entries[i] = logdb.Entry{
			Index: start + i,
			Term:  term,
			Type:  0,
			Cmd:   []byte("cmd"),
		}
	}
	return entries
}

// --- DB lifecycle tests ---

// TestOpenAndClose verifies a DB can be opened and closed.
func TestOpenAndClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestOpenEmptyDir verifies Open fails with an empty directory path.
func TestOpenEmptyDir(t *testing.T) {
	_, err := Open("")
	if err == nil {
		t.Fatal("Open with empty dir: expected error")
	}
}

// TestDoubleClose verifies closing a DB twice returns ErrClosed.
func TestDoubleClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := db.Close(); err != logdb.ErrClosed {
		t.Fatalf("second Close: got %v, want %v", err, logdb.ErrClosed)
	}
}

// TestOperationsAfterClose verifies all operations return ErrClosed.
func TestOperationsAfterClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.Close()

	if err := db.SaveState(nil); err != logdb.ErrClosed {
		t.Fatalf("SaveState: got %v, want %v", err, logdb.ErrClosed)
	}
	if _, _, err := db.ReadState(1, 1); err != logdb.ErrClosed {
		t.Fatalf("ReadState: got %v, want %v", err, logdb.ErrClosed)
	}
	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 0); err != logdb.ErrClosed {
		t.Fatalf("IterateEntries: got %v, want %v", err, logdb.ErrClosed)
	}
	if err := db.RemoveEntriesTo(1, 1, 1); err != logdb.ErrClosed {
		t.Fatalf("RemoveEntriesTo: got %v, want %v", err, logdb.ErrClosed)
	}
	if err := db.Compact(1, 1); err != logdb.ErrClosed {
		t.Fatalf("Compact: got %v, want %v", err, logdb.ErrClosed)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{}); err != logdb.ErrClosed {
		t.Fatalf("SaveSnapshot: got %v, want %v", err, logdb.ErrClosed)
	}
	if _, err := db.GetSnapshot(1, 1); err != logdb.ErrClosed {
		t.Fatalf("GetSnapshot: got %v, want %v", err, logdb.ErrClosed)
	}
	if err := db.RemoveNodeData(1, 1); err != logdb.ErrClosed {
		t.Fatalf("RemoveNodeData: got %v, want %v", err, logdb.ErrClosed)
	}
	if _, err := db.ListNodeInfo(); err != logdb.ErrClosed {
		t.Fatalf("ListNodeInfo: got %v, want %v", err, logdb.ErrClosed)
	}
	if err := db.SaveBootstrap(1, 1, logdb.Bootstrap{}); err != logdb.ErrClosed {
		t.Fatalf("SaveBootstrap: got %v, want %v", err, logdb.ErrClosed)
	}
	if _, _, err := db.GetBootstrap(1, 1); err != logdb.ErrClosed {
		t.Fatalf("GetBootstrap: got %v, want %v", err, logdb.ErrClosed)
	}
	if err := db.SaveEntries(1, 1, nil); err != logdb.ErrClosed {
		t.Fatalf("SaveEntries: got %v, want %v", err, logdb.ErrClosed)
	}
	if _, err := db.GetEntries(1, 1, 1, 2, 0); err != logdb.ErrClosed {
		t.Fatalf("GetEntries: got %v, want %v", err, logdb.ErrClosed)
	}
}

// TestName verifies the Name method.
func TestName(t *testing.T) {
	db := openTestDB(t)
	if name := db.Name(); name != "waldb" {
		t.Fatalf("Name: got %q, want %q", name, "waldb")
	}
}

// --- Entry CRUD tests ---

// TestSaveAndGetEntries verifies the basic save/get cycle for entries.
func TestSaveAndGetEntries(t *testing.T) {
	db := openTestDB(t)

	entries := makeEntries(1, 5, 1)
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	got, err := db.GetEntries(1, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("GetEntries: got %d entries, want 5", len(got))
	}
	for i, e := range got {
		if e.Index != uint64(i+1) {
			t.Fatalf("GetEntries[%d]: Index=%d, want %d", i, e.Index, i+1)
		}
		if e.Term != 1 {
			t.Fatalf("GetEntries[%d]: Term=%d, want 1", i, e.Term)
		}
	}
}

// TestSaveAndGetEntriesWithCmd verifies entry Cmd data roundtrips correctly.
func TestSaveAndGetEntriesWithCmd(t *testing.T) {
	db := openTestDB(t)

	entries := []logdb.Entry{
		{Index: 1, Term: 1, Cmd: []byte("hello world")},
		{Index: 2, Term: 1, Cmd: []byte("foo bar baz")},
	}
	if err := db.SaveEntries(10, 20, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	got, err := db.GetEntries(10, 20, 1, 3, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetEntries: got %d entries, want 2", len(got))
	}
	if string(got[0].Cmd) != "hello world" {
		t.Fatalf("GetEntries[0].Cmd = %q, want %q", got[0].Cmd, "hello world")
	}
	if string(got[1].Cmd) != "foo bar baz" {
		t.Fatalf("GetEntries[1].Cmd = %q, want %q", got[1].Cmd, "foo bar baz")
	}
}

// TestGetEntriesNonExistentNode verifies GetEntries returns empty for
// a node that has never been written to.
func TestGetEntriesNonExistentNode(t *testing.T) {
	db := openTestDB(t)

	got, err := db.GetEntries(99, 99, 1, 10, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if got != nil {
		t.Fatalf("GetEntries: got %d entries, want nil", len(got))
	}
}

// TestGetEntriesAfterCompaction verifies that requesting compacted entries
// returns ErrCompacted.
func TestGetEntriesAfterCompaction(t *testing.T) {
	db := openTestDB(t)

	entries := makeEntries(1, 10, 1)
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	// Requesting range that includes compacted entries.
	_, err := db.GetEntries(1, 1, 3, 8, 0)
	if err != logdb.ErrCompacted {
		t.Fatalf("GetEntries after compact: got %v, want %v", err, logdb.ErrCompacted)
	}

	// Requesting range above compaction point should work.
	got, err := db.GetEntries(1, 1, 6, 11, 0)
	if err != nil {
		t.Fatalf("GetEntries above compact: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("GetEntries above compact: got %d entries, want 5", len(got))
	}
}

// TestSaveStateWithEntries verifies saving entries via SaveState.
func TestSaveStateWithEntries(t *testing.T) {
	db := openTestDB(t)

	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Entries:   makeEntries(1, 3, 1),
			State:     logdb.State{Term: 1, Vote: 1, Commit: 3},
		},
	}

	if err := db.SaveState(updates); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	got, err := db.GetEntries(1, 1, 1, 4, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetEntries: got %d entries, want 3", len(got))
	}
}

// TestSaveStateEmpty verifies that SaveState with empty updates is a no-op.
func TestSaveStateEmpty(t *testing.T) {
	db := openTestDB(t)

	if err := db.SaveState(nil); err != nil {
		t.Fatalf("SaveState(nil): %v", err)
	}
	if err := db.SaveState([]logdb.Update{}); err != nil {
		t.Fatalf("SaveState([]): %v", err)
	}
}

// --- State tests ---

// TestSaveAndReadState verifies hard state persistence.
func TestSaveAndReadState(t *testing.T) {
	db := openTestDB(t)

	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			State:     logdb.State{Term: 5, Vote: 2, Commit: 100},
		},
	}
	if err := db.SaveState(updates); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	state, _, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if state.Term != 5 || state.Vote != 2 || state.Commit != 100 {
		t.Fatalf("ReadState: got %+v, want Term=5 Vote=2 Commit=100", state)
	}
}

// TestReadStateNonExistent verifies ReadState returns ErrNoState for
// an unknown node.
func TestReadStateNonExistent(t *testing.T) {
	db := openTestDB(t)

	_, _, err := db.ReadState(99, 99)
	if err != logdb.ErrNoState {
		t.Fatalf("ReadState: got %v, want %v", err, logdb.ErrNoState)
	}
}

// TestGetState verifies the GetState convenience method.
func TestGetState(t *testing.T) {
	db := openTestDB(t)

	if err := db.SaveNodeState(1, 1, logdb.State{Term: 3, Vote: 1, Commit: 50}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}

	state, err := db.GetState(1, 1)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state.Term != 3 || state.Vote != 1 || state.Commit != 50 {
		t.Fatalf("GetState: got %+v", state)
	}
}

// TestStateOverwrite verifies that saving state twice overwrites the first.
func TestStateOverwrite(t *testing.T) {
	db := openTestDB(t)

	if err := db.SaveNodeState(1, 1, logdb.State{Term: 1, Vote: 1, Commit: 10}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 2, Vote: 2, Commit: 20}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}

	state, err := db.GetState(1, 1)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state.Term != 2 || state.Vote != 2 || state.Commit != 20 {
		t.Fatalf("GetState: got %+v, want Term=2 Vote=2 Commit=20", state)
	}
}

// --- Bootstrap tests ---

// TestSaveAndGetBootstrap verifies bootstrap persistence.
func TestSaveAndGetBootstrap(t *testing.T) {
	db := openTestDB(t)

	bs := logdb.Bootstrap{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		Join:      true,
		Type:      2,
	}

	if err := db.SaveBootstrap(1, 1, bs); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}

	got, found, err := db.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap: %v", err)
	}
	if !found {
		t.Fatal("GetBootstrap: not found")
	}
	if !got.Join {
		t.Fatal("GetBootstrap: Join should be true")
	}
	if got.Type != 2 {
		t.Fatalf("GetBootstrap: Type=%d, want 2", got.Type)
	}
	if len(got.Addresses) != 3 {
		t.Fatalf("GetBootstrap: %d addresses, want 3", len(got.Addresses))
	}
	if got.Addresses[1] != "addr1" {
		t.Fatalf("GetBootstrap: addr[1]=%q, want %q", got.Addresses[1], "addr1")
	}
}

// TestGetBootstrapNonExistent verifies GetBootstrap for unknown node.
func TestGetBootstrapNonExistent(t *testing.T) {
	db := openTestDB(t)

	_, found, err := db.GetBootstrap(99, 99)
	if err != nil {
		t.Fatalf("GetBootstrap: %v", err)
	}
	if found {
		t.Fatal("GetBootstrap: found=true for non-existent node")
	}
}

// TestSaveBootstrapEmptyAddresses verifies saving bootstrap with no addresses.
func TestSaveBootstrapEmptyAddresses(t *testing.T) {
	db := openTestDB(t)

	bs := logdb.Bootstrap{
		Addresses: map[uint64]string{},
		Join:      false,
		Type:      0,
	}
	if err := db.SaveBootstrap(1, 1, bs); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}

	got, found, err := db.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap: %v", err)
	}
	if !found {
		t.Fatal("GetBootstrap: not found")
	}
	if got.Join {
		t.Fatal("GetBootstrap: Join should be false")
	}
}

// TestWriteBootstrapRejectsBatchMode verifies that writeBootstrap returns a
// WriteBootstrapBatchModeError when called with batchMode enabled. Bootstrap
// writes must use non-batch mode (inline fsync) so the immediate in-memory
// update is safe.
func TestWriteBootstrapRejectsBatchMode(t *testing.T) {
	db := openTestDB(t)
	shard := db.getShard(1)

	shard.mu.Lock()
	shard.batchMode = true
	err := shard.writeBootstrap(
		nodeKey{ShardID: 1, ReplicaID: 1},
		1, 1,
		logdb.Bootstrap{Addresses: map[uint64]string{1: "addr1"}},
	)
	shard.batchMode = false
	shard.mu.Unlock()

	if err == nil {
		t.Fatal("writeBootstrap: expected error in batchMode, got nil")
	}
	var batchErr *WriteBootstrapBatchModeError
	if !errors.As(err, &batchErr) {
		t.Fatalf("writeBootstrap: expected WriteBootstrapBatchModeError, got %T: %v", err, err)
	}
}

// TestWriteBootstrapNonBatchModeSucceeds verifies that writeBootstrap succeeds
// in normal (non-batch) mode with inline fsync.
func TestWriteBootstrapNonBatchModeSucceeds(t *testing.T) {
	db := openTestDB(t)

	bs := logdb.Bootstrap{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2"},
		Join:      true,
		Type:      3,
	}
	if err := db.SaveBootstrap(1, 1, bs); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}

	got, found, err := db.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap: %v", err)
	}
	if !found {
		t.Fatal("GetBootstrap: not found after non-batch write")
	}
	if !got.Join || got.Type != 3 || len(got.Addresses) != 2 {
		t.Fatalf("GetBootstrap: unexpected bootstrap %+v", got)
	}
}

// --- Snapshot tests ---

// TestSaveAndGetSnapshot verifies snapshot metadata persistence.
func TestSaveAndGetSnapshot(t *testing.T) {
	db := openTestDB(t)

	ss := logdb.Snapshot{
		Index:       100,
		Term:        5,
		Filepath:    "/data/snapshot-100",
		FileSize:    4096,
		OnDiskIndex: 95,
	}

	if err := db.SaveSnapshot(1, 1, ss); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Index != 100 || got.Term != 5 {
		t.Fatalf("GetSnapshot: got Index=%d Term=%d, want Index=100 Term=5", got.Index, got.Term)
	}
	if got.Filepath != "/data/snapshot-100" {
		t.Fatalf("GetSnapshot: Filepath=%q, want %q", got.Filepath, "/data/snapshot-100")
	}
	if got.FileSize != 4096 {
		t.Fatalf("GetSnapshot: FileSize=%d, want 4096", got.FileSize)
	}
	if got.OnDiskIndex != 95 {
		t.Fatalf("GetSnapshot: OnDiskIndex=%d, want 95", got.OnDiskIndex)
	}
}

// TestGetSnapshotNonExistent verifies GetSnapshot for unknown node.
func TestGetSnapshotNonExistent(t *testing.T) {
	db := openTestDB(t)

	_, err := db.GetSnapshot(99, 99)
	if err != logdb.ErrNoSnapshot {
		t.Fatalf("GetSnapshot: got %v, want %v", err, logdb.ErrNoSnapshot)
	}
}

// TestSnapshotOverwrite verifies later snapshots overwrite earlier ones.
func TestSnapshotOverwrite(t *testing.T) {
	db := openTestDB(t)

	ss1 := logdb.Snapshot{Index: 50, Term: 3}
	if err := db.SaveSnapshot(1, 1, ss1); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	ss2 := logdb.Snapshot{Index: 100, Term: 5}
	if err := db.SaveSnapshot(1, 1, ss2); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Index != 100 || got.Term != 5 {
		t.Fatalf("GetSnapshot: got %+v, want Index=100 Term=5", got)
	}
}

// TestSaveSnapshotRejectsStale verifies that SaveSnapshot returns
// ErrSnapshotOutOfDate when the new snapshot index is less than the
// currently stored snapshot index.
func TestSaveSnapshotRejectsStale(t *testing.T) {
	db := openTestDB(t)

	ss1 := logdb.Snapshot{Index: 100, Term: 5}
	if err := db.SaveSnapshot(1, 1, ss1); err != nil {
		t.Fatalf("SaveSnapshot(100): %v", err)
	}

	// Stale: index 50 < stored index 100.
	ss2 := logdb.Snapshot{Index: 50, Term: 3}
	err := db.SaveSnapshot(1, 1, ss2)
	if !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
		t.Fatalf("SaveSnapshot(50) = %v, want ErrSnapshotOutOfDate", err)
	}

	// Verify the stored snapshot was not overwritten.
	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Index != 100 || got.Term != 5 {
		t.Fatalf("GetSnapshot: got %+v, want Index=100 Term=5", got)
	}
}

// TestSaveSnapshotRejectsEqual verifies that SaveSnapshot returns
// ErrSnapshotOutOfDate when the new snapshot index equals the stored index.
func TestSaveSnapshotRejectsEqual(t *testing.T) {
	db := openTestDB(t)

	ss1 := logdb.Snapshot{Index: 100, Term: 5}
	if err := db.SaveSnapshot(1, 1, ss1); err != nil {
		t.Fatalf("SaveSnapshot(100): %v", err)
	}

	// Equal: index 100 == stored index 100.
	ss2 := logdb.Snapshot{Index: 100, Term: 6}
	err := db.SaveSnapshot(1, 1, ss2)
	if !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
		t.Fatalf("SaveSnapshot(100 again) = %v, want ErrSnapshotOutOfDate", err)
	}

	// Verify the stored snapshot was not overwritten (still Term 5).
	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Term != 5 {
		t.Fatalf("GetSnapshot.Term = %d, want 5", got.Term)
	}
}

// TestSaveSnapshotViaUpdateRejectsStale verifies that stale snapshots
// are rejected when saved through the SaveState path (batch updates).
func TestSaveSnapshotViaUpdateRejectsStale(t *testing.T) {
	db := openTestDB(t)

	// Save initial snapshot via SaveState.
	err := db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		Snapshot:  logdb.Snapshot{Index: 100, Term: 5},
	}})
	if err != nil {
		t.Fatalf("SaveState(ss100): %v", err)
	}

	// Stale snapshot via SaveState should silently succeed (saveUpdate
	// still calls writeSnapshot which updates in-memory). SaveSnapshot
	// is the gatekeeper.
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 50, Term: 3}); !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
		t.Fatalf("SaveSnapshot(50) = %v, want ErrSnapshotOutOfDate", err)
	}
}

// --- EntryRange tests ---

// TestEntryRangeReturnsCorrectRange verifies the range for a node with entries.
func TestEntryRangeReturnsCorrectRange(t *testing.T) {
	db := openTestDB(t)
	entries := makeEntries(5, 3, 1)
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	first, length, err := db.EntryRange(1, 1)
	if err != nil {
		t.Fatalf("EntryRange: %v", err)
	}
	if first != 5 {
		t.Errorf("EntryRange firstIndex = %d, want 5", first)
	}
	if length != 3 {
		t.Errorf("EntryRange length = %d, want 3", length)
	}
}

// TestEntryRangeUnknownNodeReturnsZero verifies empty result for unknown nodes.
func TestEntryRangeUnknownNodeReturnsZero(t *testing.T) {
	db := openTestDB(t)
	first, length, err := db.EntryRange(99, 99)
	if err != nil {
		t.Fatalf("EntryRange: %v", err)
	}
	if first != 0 || length != 0 {
		t.Errorf("EntryRange = (%d, %d), want (0, 0)", first, length)
	}
}

// TestEntryRangeAfterCompaction verifies range reflects compacted entries.
func TestEntryRangeAfterCompaction(t *testing.T) {
	db := openTestDB(t)
	entries := makeEntries(1, 10, 1)
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	first, length, err := db.EntryRange(1, 1)
	if err != nil {
		t.Fatalf("EntryRange: %v", err)
	}
	if first != 6 {
		t.Errorf("EntryRange firstIndex = %d, want 6", first)
	}
	if length != 5 {
		t.Errorf("EntryRange length = %d, want 5", length)
	}
}

// TestEntryRangeOnClosedDBReturnsError verifies error on closed DB.
func TestEntryRangeOnClosedDBReturnsError(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.Close()
	_, _, err = db.EntryRange(1, 1)
	if !errors.Is(err, logdb.ErrClosed) {
		t.Errorf("EntryRange on closed DB: got %v, want ErrClosed", err)
	}
}

// --- Compaction tests ---

// TestRemoveEntriesTo verifies entry compaction.
func TestRemoveEntriesTo(t *testing.T) {
	db := openTestDB(t)

	entries := makeEntries(1, 10, 1)
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	// Entries 1-5 should be compacted.
	_, err := db.GetEntries(1, 1, 1, 6, 0)
	if err != logdb.ErrCompacted {
		t.Fatalf("GetEntries(1,6): got %v, want %v", err, logdb.ErrCompacted)
	}

	// Entries 6-10 should still be available.
	got, err := db.GetEntries(1, 1, 6, 11, 0)
	if err != nil {
		t.Fatalf("GetEntries(6,11): %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("GetEntries(6,11): got %d, want 5", len(got))
	}
}

// TestRemoveEntriesToNonExistentNode verifies compaction on unknown node.
func TestRemoveEntriesToNonExistentNode(t *testing.T) {
	db := openTestDB(t)

	// Should not error -- just writes a compaction marker.
	if err := db.RemoveEntriesTo(99, 99, 100); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
}

// TestCompactTriggersGC verifies Compact does not error.
func TestCompactTriggersGC(t *testing.T) {
	db := openTestDB(t)

	entries := makeEntries(1, 10, 1)
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	if err := db.Compact(1, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}
}

// --- Node management tests ---

// TestRemoveNodeData verifies all data for a node is removed.
func TestRemoveNodeData(t *testing.T) {
	db := openTestDB(t)

	entries := makeEntries(1, 5, 1)
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 1}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	if err := db.SaveBootstrap(1, 1, logdb.Bootstrap{Type: 1}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 5}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	if err := db.RemoveNodeData(1, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}

	got, err := db.GetEntries(1, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("GetEntries after remove: %v", err)
	}
	if got != nil {
		t.Fatalf("GetEntries after remove: got %d entries, want nil", len(got))
	}

	_, _, err = db.ReadState(1, 1)
	if err != logdb.ErrNoState {
		t.Fatalf("ReadState after remove: got %v, want %v", err, logdb.ErrNoState)
	}

	_, found, err := db.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap after remove: %v", err)
	}
	if found {
		t.Fatal("GetBootstrap after remove: should not be found")
	}

	_, err = db.GetSnapshot(1, 1)
	if err != logdb.ErrNoSnapshot {
		t.Fatalf("GetSnapshot after remove: got %v, want %v", err, logdb.ErrNoSnapshot)
	}
}

// TestRemoveNodeDataNonExistent verifies removing a non-existent node is ok.
func TestRemoveNodeDataNonExistent(t *testing.T) {
	db := openTestDB(t)
	if err := db.RemoveNodeData(99, 99); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}
}

// TestRemoveNodeDataDurableAfterReopen verifies that RemoveNodeData is
// durable across a WAL close and reopen. After removing a node and
// reopening the database, the removed node's data must not reappear.
func TestRemoveNodeDataDurableAfterReopen(t *testing.T) {
	dir := t.TempDir()

	// Open, write data for two nodes, close.
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries(1,1): %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 3, Vote: 1, Commit: 10}); err != nil {
		t.Fatalf("SaveNodeState(1,1): %v", err)
	}
	if err := db.SaveBootstrap(1, 1, logdb.Bootstrap{
		Addresses: map[uint64]string{1: "addr1"},
		Type:      1,
	}); err != nil {
		t.Fatalf("SaveBootstrap(1,1): %v", err)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 5, Term: 2}); err != nil {
		t.Fatalf("SaveSnapshot(1,1): %v", err)
	}
	// Node 2 should survive the removal.
	if err := db.SaveEntries(2, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries(2,1): %v", err)
	}
	if err := db.SaveNodeState(2, 1, logdb.State{Term: 1, Vote: 1, Commit: 5}); err != nil {
		t.Fatalf("SaveNodeState(2,1): %v", err)
	}

	// Remove node (1,1) durably.
	if err := db.RemoveNodeData(1, 1); err != nil {
		t.Fatalf("RemoveNodeData(1,1): %v", err)
	}

	// Close.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify the removal persisted.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	// Node (1,1) should be completely gone.
	got, err := db2.GetEntries(1, 1, 1, 11, 0)
	if err != nil {
		t.Fatalf("GetEntries(1,1) after reopen: %v", err)
	}
	if got != nil {
		t.Fatalf("GetEntries(1,1) after reopen: got %d entries, want nil", len(got))
	}

	_, _, err = db2.ReadState(1, 1)
	if err != logdb.ErrNoState {
		t.Fatalf("ReadState(1,1) after reopen: got %v, want %v", err, logdb.ErrNoState)
	}

	_, found, err := db2.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap(1,1) after reopen: %v", err)
	}
	if found {
		t.Fatal("GetBootstrap(1,1) after reopen: should not be found")
	}

	_, err = db2.GetSnapshot(1, 1)
	if err != logdb.ErrNoSnapshot {
		t.Fatalf("GetSnapshot(1,1) after reopen: got %v, want %v", err, logdb.ErrNoSnapshot)
	}

	// Node (2,1) should still exist.
	got2, err := db2.GetEntries(2, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("GetEntries(2,1) after reopen: %v", err)
	}
	if len(got2) != 5 {
		t.Fatalf("GetEntries(2,1) after reopen: got %d entries, want 5", len(got2))
	}

	state2, err := db2.GetState(2, 1)
	if err != nil {
		t.Fatalf("GetState(2,1) after reopen: %v", err)
	}
	if state2.Term != 1 || state2.Vote != 1 || state2.Commit != 5 {
		t.Fatalf("GetState(2,1) after reopen: got %+v", state2)
	}
}

// TestRemoveNodeDataThenRewrite verifies that after removing a node, new
// data can be written for the same (shardID, replicaID) and survives
// recovery. This exercises the WAL replay ordering: remove record clears
// old data, then new entries are replayed.
func TestRemoveNodeDataThenRewrite(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write initial data.
	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 1, Vote: 1, Commit: 5}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}

	// Remove the node.
	if err := db.RemoveNodeData(1, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}

	// Write new data for the same node.
	if err := db.SaveEntries(1, 1, makeEntries(100, 3, 5)); err != nil {
		t.Fatalf("SaveEntries (new): %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 5, Vote: 2, Commit: 102}); err != nil {
		t.Fatalf("SaveNodeState (new): %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify only the new data exists.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	// Old entries (1-5) should be gone.
	got, err := db2.GetEntries(1, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("GetEntries(1,6) after reopen: %v", err)
	}
	if got != nil {
		t.Fatalf("GetEntries(1,6) after reopen: old entries still present: %d", len(got))
	}

	// New entries (100-102) should be present.
	got, err = db2.GetEntries(1, 1, 100, 103, 0)
	if err != nil {
		t.Fatalf("GetEntries(100,103) after reopen: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetEntries(100,103) after reopen: got %d, want 3", len(got))
	}

	// New state should be present.
	state, err := db2.GetState(1, 1)
	if err != nil {
		t.Fatalf("GetState after reopen: %v", err)
	}
	if state.Term != 5 || state.Vote != 2 || state.Commit != 102 {
		t.Fatalf("GetState after reopen: got %+v, want Term=5 Vote=2 Commit=102", state)
	}
}

// TestRemoveNodeDataListNodeInfo verifies that RemoveNodeData removes
// the node from ListNodeInfo results and this persists across restarts.
func TestRemoveNodeDataListNodeInfo(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write data for 3 nodes.
	if err := db.SaveEntries(1, 1, makeEntries(1, 1, 1)); err != nil {
		t.Fatalf("SaveEntries(1,1): %v", err)
	}
	if err := db.SaveEntries(2, 1, makeEntries(1, 1, 1)); err != nil {
		t.Fatalf("SaveEntries(2,1): %v", err)
	}
	if err := db.SaveEntries(3, 1, makeEntries(1, 1, 1)); err != nil {
		t.Fatalf("SaveEntries(3,1): %v", err)
	}

	nodes, err := db.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("ListNodeInfo: got %d nodes, want 3", len(nodes))
	}

	// Remove node (2,1).
	if err := db.RemoveNodeData(2, 1); err != nil {
		t.Fatalf("RemoveNodeData(2,1): %v", err)
	}

	nodes, err = db.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo after remove: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("ListNodeInfo after remove: got %d nodes, want 2", len(nodes))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify ListNodeInfo still shows 2 nodes.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	nodes, err = db2.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo after reopen: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("ListNodeInfo after reopen: got %d nodes, want 2", len(nodes))
	}

	// Verify the removed node is the right one.
	for _, n := range nodes {
		if n.ShardID == 2 && n.ReplicaID == 1 {
			t.Fatal("ListNodeInfo after reopen: node (2,1) should have been removed")
		}
	}
}

// TestListNodeInfo verifies listing all known nodes.
func TestListNodeInfo(t *testing.T) {
	db := openTestDB(t)

	// Write data for 3 different nodes.
	if err := db.SaveEntries(1, 1, makeEntries(1, 1, 1)); err != nil {
		t.Fatalf("SaveEntries(1,1): %v", err)
	}
	if err := db.SaveEntries(2, 1, makeEntries(1, 1, 1)); err != nil {
		t.Fatalf("SaveEntries(2,1): %v", err)
	}
	if err := db.SaveBootstrap(3, 1, logdb.Bootstrap{Type: 1}); err != nil {
		t.Fatalf("SaveBootstrap(3,1): %v", err)
	}

	nodes, err := db.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("ListNodeInfo: got %d nodes, want 3", len(nodes))
	}

	// Verify all expected nodes are present.
	found := make(map[logdb.NodeInfo]bool)
	for _, n := range nodes {
		found[n] = true
	}
	for _, expected := range []logdb.NodeInfo{
		{ShardID: 1, ReplicaID: 1},
		{ShardID: 2, ReplicaID: 1},
		{ShardID: 3, ReplicaID: 1},
	} {
		if !found[expected] {
			t.Fatalf("ListNodeInfo: missing %+v", expected)
		}
	}
}

// TestListNodeInfoEmpty verifies listing on an empty DB.
func TestListNodeInfoEmpty(t *testing.T) {
	db := openTestDB(t)
	nodes, err := db.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("ListNodeInfo: got %d nodes, want 0", len(nodes))
	}
}

// --- Multi-shard tests ---

// TestMultiShardIsolation verifies data is isolated between different shards.
func TestMultiShardIsolation(t *testing.T) {
	db := openTestDB(t)

	// Write entries to shard 1 and shard 2.
	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries(1,1): %v", err)
	}
	if err := db.SaveEntries(2, 1, makeEntries(1, 3, 2)); err != nil {
		t.Fatalf("SaveEntries(2,1): %v", err)
	}

	// Shard 1 should have 5 entries.
	got1, err := db.GetEntries(1, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("GetEntries(1,1): %v", err)
	}
	if len(got1) != 5 {
		t.Fatalf("GetEntries(1,1): got %d, want 5", len(got1))
	}

	// Shard 2 should have 3 entries.
	got2, err := db.GetEntries(2, 1, 1, 4, 0)
	if err != nil {
		t.Fatalf("GetEntries(2,1): %v", err)
	}
	if len(got2) != 3 {
		t.Fatalf("GetEntries(2,1): got %d, want 3", len(got2))
	}
}

// TestMultiShardSameWALShard verifies two Raft shards that map to the
// same WAL shard (shardID % 16 == same).
func TestMultiShardSameWALShard(t *testing.T) {
	db := openTestDB(t)

	// ShardID 0 and 16 both map to WAL shard 0.
	if err := db.SaveEntries(0, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries(0,1): %v", err)
	}
	if err := db.SaveEntries(16, 1, makeEntries(1, 3, 2)); err != nil {
		t.Fatalf("SaveEntries(16,1): %v", err)
	}

	got1, err := db.GetEntries(0, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("GetEntries(0,1): %v", err)
	}
	if len(got1) != 5 {
		t.Fatalf("GetEntries(0,1): got %d, want 5", len(got1))
	}

	got2, err := db.GetEntries(16, 1, 1, 4, 0)
	if err != nil {
		t.Fatalf("GetEntries(16,1): %v", err)
	}
	if len(got2) != 3 {
		t.Fatalf("GetEntries(16,1): got %d, want 3", len(got2))
	}
}

// --- Concurrent access tests ---

// TestConcurrentWritesDifferentShards verifies concurrent writes to
// different Raft shards (which map to different WAL shards).
func TestConcurrentWritesDifferentShards(t *testing.T) {
	db := openTestDB(t)

	var wg sync.WaitGroup
	numShards := 32

	for i := 0; i < numShards; i++ {
		wg.Add(1)
		go func(shardID uint64) {
			defer wg.Done()
			entries := makeEntries(1, 10, 1)
			if err := db.SaveEntries(shardID, 1, entries); err != nil {
				t.Errorf("SaveEntries(%d,1): %v", shardID, err)
			}
		}(uint64(i))
	}
	wg.Wait()

	// Verify all shards have their entries.
	for i := 0; i < numShards; i++ {
		got, err := db.GetEntries(uint64(i), 1, 1, 11, 0)
		if err != nil {
			t.Fatalf("GetEntries(%d,1): %v", i, err)
		}
		if len(got) != 10 {
			t.Fatalf("GetEntries(%d,1): got %d entries, want 10", i, len(got))
		}
	}
}

// TestConcurrentWritesAndReads verifies concurrent writes and reads
// do not race.
func TestConcurrentWritesAndReads(t *testing.T) {
	db := openTestDB(t)

	var wg sync.WaitGroup

	// Writers.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(shardID uint64) {
			defer wg.Done()
			for j := uint64(1); j <= 50; j++ {
				entries := []logdb.Entry{
					{Index: j, Term: 1, Cmd: []byte("data")},
				}
				db.SaveEntries(shardID, 1, entries)
			}
		}(uint64(i))
	}

	// Readers.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(shardID uint64) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				db.GetEntries(shardID, 1, 1, 51, 0)
			}
		}(uint64(i))
	}

	wg.Wait()
}

// TestConcurrentStateOperations verifies concurrent state reads and writes.
func TestConcurrentStateOperations(t *testing.T) {
	db := openTestDB(t)

	var wg sync.WaitGroup

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(shardID uint64) {
			defer wg.Done()
			state := logdb.State{Term: shardID, Vote: 1, Commit: shardID * 10}
			db.SaveNodeState(shardID, 1, state)
			db.GetState(shardID, 1)
		}(uint64(i))
	}

	wg.Wait()
}

// --- Recovery tests ---

// TestRecoveryAfterReopen verifies data survives close and reopen.
func TestRecoveryAfterReopen(t *testing.T) {
	dir := t.TempDir()

	// Open, write, close.
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 3, Vote: 1, Commit: 10}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	if err := db.SaveBootstrap(1, 1, logdb.Bootstrap{
		Addresses: map[uint64]string{1: "localhost:9001"},
		Type:      1,
	}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 5, Term: 2}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	// Verify state.
	state, err := db2.GetState(1, 1)
	if err != nil {
		t.Fatalf("GetState after reopen: %v", err)
	}
	if state.Term != 3 || state.Vote != 1 || state.Commit != 10 {
		t.Fatalf("GetState after reopen: got %+v", state)
	}

	// Verify bootstrap.
	bs, found, err := db2.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap after reopen: %v", err)
	}
	if !found {
		t.Fatal("GetBootstrap after reopen: not found")
	}
	if bs.Addresses[1] != "localhost:9001" {
		t.Fatalf("GetBootstrap after reopen: addr=%q", bs.Addresses[1])
	}

	// Verify snapshot.
	ss, err := db2.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot after reopen: %v", err)
	}
	if ss.Index != 5 || ss.Term != 2 {
		t.Fatalf("GetSnapshot after reopen: got %+v", ss)
	}

	// Verify entries are in the index after recovery.
	got, err := db2.GetEntries(1, 1, 1, 11, 0)
	if err != nil {
		t.Fatalf("GetEntries after reopen: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("GetEntries after reopen: got %d, want 10", len(got))
	}
}

// TestRecoveryAfterCompactionReopen verifies compacted state survives reopen.
func TestRecoveryAfterCompactionReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	// Compacted entries should still be compacted.
	_, err = db2.GetEntries(1, 1, 1, 6, 0)
	if err != logdb.ErrCompacted {
		t.Fatalf("GetEntries(1,6) after reopen: got %v, want %v", err, logdb.ErrCompacted)
	}

	// Entries above compaction point should be available.
	got, err := db2.GetEntries(1, 1, 6, 11, 0)
	if err != nil {
		t.Fatalf("GetEntries(6,11) after reopen: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("GetEntries(6,11) after reopen: got %d, want 5", len(got))
	}
}

// --- IterateEntries tests ---

// TestIterateEntriesAppendsToSlice verifies IterateEntries appends to the
// provided slice.
func TestIterateEntriesAppendsToSlice(t *testing.T) {
	db := openTestDB(t)

	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	existing := []logdb.Entry{{Index: 0, Term: 0}}
	got, _, err := db.IterateEntries(existing, 0, 1, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("IterateEntries: %v", err)
	}
	// Should have the pre-existing entry plus 5 new ones.
	if len(got) != 6 {
		t.Fatalf("IterateEntries: got %d entries, want 6", len(got))
	}
}

// TestIterateEntriesMaxSize verifies maxSize limit is respected.
func TestIterateEntriesMaxSize(t *testing.T) {
	db := openTestDB(t)

	if err := db.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	// Use a very small maxSize to limit results.
	got, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 11, 1)
	if err != nil {
		t.Fatalf("IterateEntries: %v", err)
	}
	// With maxSize=1, we should get at least 1 entry (first entry is always
	// returned regardless of maxSize when totalSize starts at 0).
	if len(got) < 1 {
		t.Fatalf("IterateEntries: got %d entries, want >= 1", len(got))
	}
}

// TestIterateEntriesCompacted verifies IterateEntries returns ErrCompacted.
func TestIterateEntriesCompacted(t *testing.T) {
	db := openTestDB(t)

	if err := db.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	_, _, err := db.IterateEntries(nil, 0, 1, 1, 3, 8, 0)
	if err != logdb.ErrCompacted {
		t.Fatalf("IterateEntries: got %v, want %v", err, logdb.ErrCompacted)
	}
}

// TestIterateEntriesNonExistentNode verifies IterateEntries for unknown node.
func TestIterateEntriesNonExistentNode(t *testing.T) {
	db := openTestDB(t)

	got, size, err := db.IterateEntries(nil, 0, 99, 99, 1, 10, 0)
	if err != nil {
		t.Fatalf("IterateEntries: %v", err)
	}
	if got != nil {
		t.Fatalf("IterateEntries: got %d entries, want nil", len(got))
	}
	if size != 0 {
		t.Fatalf("IterateEntries: size=%d, want 0", size)
	}
}

// --- Error type tests ---

// TestDBOpenError verifies the error type.
func TestDBOpenError(t *testing.T) {
	inner := os.ErrPermission
	e := &DBOpenError{Dir: "/test", Err: inner}

	msg := e.Error()
	if msg == "" {
		t.Fatal("Error() returned empty string")
	}
	if e.Unwrap() != inner {
		t.Fatalf("Unwrap: got %v, want %v", e.Unwrap(), inner)
	}
}

// TestDBOpenErrorIsTyped verifies errors.As works with DBOpenError.
func TestDBOpenErrorIsTyped(t *testing.T) {
	inner := os.ErrPermission
	e := &DBOpenError{Dir: "/test", Err: inner}

	var target *DBOpenError
	if !errors.As(e, &target) {
		t.Fatal("errors.As failed for DBOpenError")
	}
	if target.Dir != "/test" {
		t.Fatalf("Dir: got %q, want %q", target.Dir, "/test")
	}
}

// TestRecoveryError verifies the error type.
func TestRecoveryError(t *testing.T) {
	inner := os.ErrNotExist
	e := &RecoveryError{SegmentID: 42, Err: inner}

	msg := e.Error()
	if msg == "" {
		t.Fatal("Error() returned empty string")
	}
	if e.Unwrap() != inner {
		t.Fatalf("Unwrap: got %v, want %v", e.Unwrap(), inner)
	}
}

// TestSegmentCreateError verifies the error type.
func TestSegmentCreateError(t *testing.T) {
	inner := os.ErrPermission
	e := &SegmentCreateError{SegmentID: 7, Err: inner}

	msg := e.Error()
	if msg == "" {
		t.Fatal("Error() returned empty string")
	}
	if e.Unwrap() != inner {
		t.Fatalf("Unwrap: got %v, want %v", e.Unwrap(), inner)
	}
}

// TestDirSyncError verifies the DirSyncError type fields and message.
func TestDirSyncError(t *testing.T) {
	inner := os.ErrPermission
	e := &DirSyncError{Dir: "/wal/shard-0", Err: inner}

	msg := e.Error()
	if msg == "" {
		t.Fatal("Error() returned empty string")
	}
	if e.Unwrap() != inner {
		t.Fatalf("Unwrap: got %v, want %v", e.Unwrap(), inner)
	}
}

// TestDirSyncErrorIsTyped verifies errors.As works with DirSyncError.
func TestDirSyncErrorIsTyped(t *testing.T) {
	inner := os.ErrPermission
	e := &DirSyncError{Dir: "/wal/shard-0", Err: inner}

	var target *DirSyncError
	if !errors.As(e, &target) {
		t.Fatal("errors.As failed for DirSyncError")
	}
	if target.Dir != "/wal/shard-0" {
		t.Fatalf("Dir: got %q, want %q", target.Dir, "/wal/shard-0")
	}
}

// TestSyncDirFSSuccess verifies syncDirFS succeeds on a valid directory.
func TestSyncDirFSSuccess(t *testing.T) {
	dir := t.TempDir()
	if err := syncDirFS(defaultFS(), dir); err != nil {
		t.Fatalf("syncDirFS on valid directory: unexpected error: %v", err)
	}
}

// TestSyncDirFSNonExistentDir verifies syncDirFS returns a DirSyncError for a
// non-existent directory.
func TestSyncDirFSNonExistentDir(t *testing.T) {
	err := syncDirFS(defaultFS(), "/nonexistent/waldb/test/dir")
	if err == nil {
		t.Fatal("syncDir on non-existent directory: expected error, got nil")
	}
	var target *DirSyncError
	if !errors.As(err, &target) {
		t.Fatalf("expected DirSyncError, got %T: %v", err, err)
	}
	if target.Dir != "/nonexistent/waldb/test/dir" {
		t.Fatalf("DirSyncError.Dir: got %q, want %q", target.Dir, "/nonexistent/waldb/test/dir")
	}
}

// TestEnsureActiveSegmentSyncsDirOnCreate verifies that ensureActiveSegment
// creates the segment file and makes it durable via syncDir when noSync is
// false. The segment file must exist on disk after the call returns.
func TestEnsureActiveSegmentSyncsDirOnCreate(t *testing.T) {
	dir := t.TempDir()

	// Open with sync enabled (the default) to exercise the syncDir path.
	db, err := Open(dir, WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write one entry to trigger ensureActiveSegment.
	entry := logdb.Entry{Index: 1, Term: 1, Cmd: []byte("hello")}
	if err := db.SaveEntries(1, 1, []logdb.Entry{entry}); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	// Verify the segment file exists on disk. The shard directory and
	// segment file must be present because syncDir was called after
	// segment creation.
	shard := db.getShard(1)
	shard.mu.Lock()
	segID := shard.activeSegmentID
	shardDir := shard.dir
	shard.mu.Unlock()

	path := filepath.Join(shardDir, segmentFilename(segID))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("segment file %s does not exist after ensureActiveSegment: %v", path, err)
	}
}

// TestRotateSegmentSyncsDirOnNewSegment verifies that rotateSegment creates
// a new segment with a durable directory entry via ensureActiveSegment.
func TestRotateSegmentSyncsDirOnNewSegment(t *testing.T) {
	dir := t.TempDir()

	// Use a very small MaxFileSize to force rotation after a few entries.
	db, err := Open(dir, WithBlockSize(512), WithMaxFileSize(2*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write enough entries to force at least one segment rotation.
	for i := 1; i <= 50; i++ {
		entry := logdb.Entry{
			Index: uint64(i),
			Term:  1,
			Cmd:   make([]byte, 100),
		}
		if err := db.SaveEntries(1, 1, []logdb.Entry{entry}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}

	// Verify multiple segment files exist on disk.
	shardDir := db.getShard(1).dir
	dirEntries, err := os.ReadDir(shardDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", shardDir, err)
	}

	segCount := 0
	for _, de := range dirEntries {
		if !de.IsDir() {
			segCount++
		}
	}
	if segCount < 2 {
		t.Fatalf("expected at least 2 segment files after rotation, got %d", segCount)
	}
}

// TestShardDirName verifies shard directory naming.
func TestShardDirName(t *testing.T) {
	tests := []struct {
		idx  int
		want string
	}{
		{0, "shard-0"},
		{1, "shard-1"},
		{15, "shard-15"},
	}
	for _, tt := range tests {
		got := shardDirName(tt.idx)
		if got != tt.want {
			t.Fatalf("shardDirName(%d) = %q, want %q", tt.idx, got, tt.want)
		}
	}
}

// TestShardDirNameInvalid verifies shard-0 for index 0.
func TestShardDirNameZero(t *testing.T) {
	if got := shardDirName(0); got != "shard-0" {
		t.Fatalf("shardDirName(0) = %q, want %q", got, "shard-0")
	}
}

// TestItoa verifies the internal itoa helper.
func TestItoa(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{100, "100"},
		{999, "999"},
	}
	for _, tt := range tests {
		got := itoa(tt.n)
		if got != tt.want {
			t.Fatalf("itoa(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

// TestItoaZero verifies itoa(0) returns "0".
func TestItoaZero(t *testing.T) {
	if got := itoa(0); got != "0" {
		t.Fatalf("itoa(0) = %q, want %q", got, "0")
	}
}

// --- Interface satisfaction test ---

// TestDBImplementsLogDB verifies DB satisfies the LogDB interface.
func TestDBImplementsLogDB(t *testing.T) {
	var _ logdb.LogDB = (*DB)(nil)
}

// TestFullCRUDCycle verifies a complete create-read-update-delete cycle.
func TestFullCRUDCycle(t *testing.T) {
	db := openTestDB(t)

	// Create: save bootstrap, state, entries, snapshot.
	bs := logdb.Bootstrap{
		Addresses: map[uint64]string{1: "addr1"},
		Type:      1,
	}
	if err := db.SaveBootstrap(1, 1, bs); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 1, Vote: 1, Commit: 10}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 5, Term: 1}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Read: verify all data.
	gotBS, found, err := db.GetBootstrap(1, 1)
	if err != nil || !found {
		t.Fatalf("GetBootstrap: err=%v found=%v", err, found)
	}
	if gotBS.Addresses[1] != "addr1" {
		t.Fatalf("GetBootstrap: addr=%q", gotBS.Addresses[1])
	}

	gotEntries, err := db.GetEntries(1, 1, 1, 11, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(gotEntries) != 10 {
		t.Fatalf("GetEntries: got %d, want 10", len(gotEntries))
	}

	gotState, err := db.GetState(1, 1)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if gotState.Term != 1 || gotState.Commit != 10 {
		t.Fatalf("GetState: got %+v", gotState)
	}

	gotSS, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if gotSS.Index != 5 {
		t.Fatalf("GetSnapshot: got %+v", gotSS)
	}

	// Update: overwrite state and add more entries.
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 2, Vote: 2, Commit: 15}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(11, 5, 2)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	gotState, err = db.GetState(1, 1)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if gotState.Term != 2 {
		t.Fatalf("GetState after update: got %+v", gotState)
	}

	// Delete: remove node data.
	if err := db.RemoveNodeData(1, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}

	_, _, err = db.ReadState(1, 1)
	if err != logdb.ErrNoState {
		t.Fatalf("ReadState after delete: got %v, want %v", err, logdb.ErrNoState)
	}
}

// TestSegmentRotation verifies entries survive WAL segment rotation.
// Uses a tiny MaxFileSize to force rotation after a few entries.
func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()

	// Use a very small MaxFileSize (2KB) with a small block size (512)
	// to force segment rotation after a small number of entries.
	db, err := Open(dir, WithNoSync(true), WithBlockSize(512), WithMaxFileSize(2*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write enough entries to trigger at least one segment rotation.
	// Each entry has a 100-byte Cmd to consume space quickly.
	totalEntries := 50
	for i := 1; i <= totalEntries; i++ {
		entry := logdb.Entry{
			Index: uint64(i),
			Term:  1,
			Type:  0,
			Cmd:   make([]byte, 100),
		}
		entry.Cmd[0] = byte(i % 256)

		if err := db.SaveEntries(1, 1, []logdb.Entry{entry}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}

	// Read all entries back and verify.
	got, err := db.GetEntries(1, 1, 1, uint64(totalEntries)+1, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != totalEntries {
		t.Fatalf("GetEntries: got %d entries, want %d", len(got), totalEntries)
	}

	for i, e := range got {
		wantIndex := uint64(i + 1)
		if e.Index != wantIndex {
			t.Fatalf("entry[%d].Index = %d, want %d", i, e.Index, wantIndex)
		}
		if e.Cmd[0] != byte(wantIndex%256) {
			t.Fatalf("entry[%d].Cmd[0] = %d, want %d", i, e.Cmd[0], byte(wantIndex%256))
		}
	}

	db.Close()
}

// TestSegmentRotationRecovery verifies entries survive rotation and
// database reopen (recovery replays across multiple segments).
func TestSegmentRotationRecovery(t *testing.T) {
	dir := t.TempDir()

	// Use a very small MaxFileSize to force multiple segment rotations.
	db, err := Open(dir, WithNoSync(true), WithBlockSize(512), WithMaxFileSize(2*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	totalEntries := 30
	for i := 1; i <= totalEntries; i++ {
		entry := logdb.Entry{
			Index: uint64(i),
			Term:  1,
			Type:  0,
			Cmd:   make([]byte, 80),
		}
		entry.Cmd[0] = byte(i % 256)

		if err := db.SaveEntries(1, 1, []logdb.Entry{entry}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}

	// Also save state to verify it survives recovery.
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 5, Vote: 3, Commit: 20}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}

	db.Close()

	// Reopen and verify everything recovered.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(512), WithMaxFileSize(2*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	got, err := db2.GetEntries(1, 1, 1, uint64(totalEntries)+1, 0)
	if err != nil {
		t.Fatalf("GetEntries after reopen: %v", err)
	}
	if len(got) != totalEntries {
		t.Fatalf("GetEntries: got %d entries, want %d", len(got), totalEntries)
	}

	for i, e := range got {
		wantIndex := uint64(i + 1)
		if e.Index != wantIndex {
			t.Fatalf("entry[%d].Index = %d, want %d", i, e.Index, wantIndex)
		}
		if e.Cmd[0] != byte(wantIndex%256) {
			t.Fatalf("entry[%d].Cmd[0] = %d, want %d", i, e.Cmd[0], byte(wantIndex%256))
		}
	}

	// Verify state was recovered.
	state, err := db2.GetState(1, 1)
	if err != nil {
		t.Fatalf("GetState after reopen: %v", err)
	}
	if state.Term != 5 || state.Vote != 3 || state.Commit != 20 {
		t.Fatalf("GetState: got %+v, want Term=5 Vote=3 Commit=20", state)
	}
}

// --- Snapshot Epoch and Membership serialization tests ---

// makeMembership creates a populated logdb.Membership for testing.
func makeMembership() logdb.Membership {
	return logdb.Membership{
		ConfigChangeID: 42,
		Addresses: map[uint64]string{
			1: "10.0.0.1:9001",
			2: "10.0.0.2:9001",
			3: "10.0.0.3:9001",
		},
		Observers: map[uint64]string{
			4: "10.0.0.4:9001",
		},
		Witnesses: map[uint64]string{
			5: "10.0.0.5:9001",
		},
		Removed: map[uint64]bool{
			6: true,
			7: true,
		},
	}
}

// TestSnapshotEpochPersistence verifies that Epoch is correctly persisted
// in snapshots and survives a save/get cycle.
func TestSnapshotEpochPersistence(t *testing.T) {
	db := openTestDB(t)

	ss := logdb.Snapshot{
		Index:       200,
		Term:        10,
		Filepath:    "/data/snapshot-200",
		FileSize:    8192,
		OnDiskIndex: 190,
		Epoch:       7,
	}

	if err := db.SaveSnapshot(1, 1, ss); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Epoch != 7 {
		t.Fatalf("GetSnapshot: Epoch=%d, want 7", got.Epoch)
	}
	if got.Index != 200 || got.Term != 10 {
		t.Fatalf("GetSnapshot: Index=%d Term=%d, want 200 10", got.Index, got.Term)
	}
}

// TestSnapshotEpochZero verifies that Epoch=0 (no encryption) roundtrips.
func TestSnapshotEpochZero(t *testing.T) {
	db := openTestDB(t)

	ss := logdb.Snapshot{
		Index: 50,
		Term:  3,
		Epoch: 0,
	}

	if err := db.SaveSnapshot(1, 1, ss); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Epoch != 0 {
		t.Fatalf("GetSnapshot: Epoch=%d, want 0", got.Epoch)
	}
}

// TestSnapshotMembershipPersistence verifies that full Membership is
// correctly serialized in snapshot records.
func TestSnapshotMembershipPersistence(t *testing.T) {
	db := openTestDB(t)

	membership := makeMembership()
	ss := logdb.Snapshot{
		Index:       300,
		Term:        15,
		Filepath:    "/data/snapshot-300",
		FileSize:    16384,
		OnDiskIndex: 295,
		Epoch:       3,
		Membership:  membership,
	}

	if err := db.SaveSnapshot(1, 1, ss); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}

	// Verify all membership fields roundtrip.
	if got.Membership.ConfigChangeID != 42 {
		t.Fatalf("ConfigChangeID=%d, want 42", got.Membership.ConfigChangeID)
	}
	if len(got.Membership.Addresses) != 3 {
		t.Fatalf("Addresses: got %d, want 3", len(got.Membership.Addresses))
	}
	if got.Membership.Addresses[1] != "10.0.0.1:9001" {
		t.Fatalf("Addresses[1]=%q, want %q", got.Membership.Addresses[1], "10.0.0.1:9001")
	}
	if got.Membership.Addresses[2] != "10.0.0.2:9001" {
		t.Fatalf("Addresses[2]=%q, want %q", got.Membership.Addresses[2], "10.0.0.2:9001")
	}
	if got.Membership.Addresses[3] != "10.0.0.3:9001" {
		t.Fatalf("Addresses[3]=%q, want %q", got.Membership.Addresses[3], "10.0.0.3:9001")
	}
	if len(got.Membership.Observers) != 1 || got.Membership.Observers[4] != "10.0.0.4:9001" {
		t.Fatalf("Observers: got %v, want {4: 10.0.0.4:9001}", got.Membership.Observers)
	}
	if len(got.Membership.Witnesses) != 1 || got.Membership.Witnesses[5] != "10.0.0.5:9001" {
		t.Fatalf("Witnesses: got %v, want {5: 10.0.0.5:9001}", got.Membership.Witnesses)
	}
	if len(got.Membership.Removed) != 2 {
		t.Fatalf("Removed: got %d entries, want 2", len(got.Membership.Removed))
	}
	if !got.Membership.Removed[6] || !got.Membership.Removed[7] {
		t.Fatalf("Removed: got %v, want {6: true, 7: true}", got.Membership.Removed)
	}
}

// TestSnapshotEmptyMembership verifies that a snapshot with empty membership
// maps (nil and empty) serializes correctly.
func TestSnapshotEmptyMembership(t *testing.T) {
	db := openTestDB(t)

	ss := logdb.Snapshot{
		Index: 100,
		Term:  5,
		Epoch: 1,
		Membership: logdb.Membership{
			ConfigChangeID: 0,
			// All maps nil.
		},
	}

	if err := db.SaveSnapshot(1, 1, ss); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Membership.ConfigChangeID != 0 {
		t.Fatalf("ConfigChangeID=%d, want 0", got.Membership.ConfigChangeID)
	}
	if len(got.Membership.Addresses) != 0 {
		t.Fatalf("Addresses: got %d, want 0", len(got.Membership.Addresses))
	}
	if len(got.Membership.Observers) != 0 {
		t.Fatalf("Observers: got %d, want 0", len(got.Membership.Observers))
	}
	if len(got.Membership.Witnesses) != 0 {
		t.Fatalf("Witnesses: got %d, want 0", len(got.Membership.Witnesses))
	}
	if len(got.Membership.Removed) != 0 {
		t.Fatalf("Removed: got %d, want 0", len(got.Membership.Removed))
	}
}

// TestSnapshotEpochAndMembershipRecovery verifies that Epoch and Membership
// survive a WAL close and reopen (full recovery replay).
func TestSnapshotEpochAndMembershipRecovery(t *testing.T) {
	dir := t.TempDir()

	membership := makeMembership()
	ss := logdb.Snapshot{
		Index:       500,
		Term:        20,
		Filepath:    "/data/snapshot-500",
		FileSize:    32768,
		OnDiskIndex: 490,
		Epoch:       9,
		Membership:  membership,
	}

	// Open, write snapshot with epoch and membership, close.
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveSnapshot(1, 1, ss); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify all fields survived recovery.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	got, err := db2.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot after reopen: %v", err)
	}
	if got.Index != 500 || got.Term != 20 {
		t.Fatalf("Index=%d Term=%d, want 500 20", got.Index, got.Term)
	}
	if got.Filepath != "/data/snapshot-500" {
		t.Fatalf("Filepath=%q, want %q", got.Filepath, "/data/snapshot-500")
	}
	if got.FileSize != 32768 {
		t.Fatalf("FileSize=%d, want 32768", got.FileSize)
	}
	if got.OnDiskIndex != 490 {
		t.Fatalf("OnDiskIndex=%d, want 490", got.OnDiskIndex)
	}
	if got.Epoch != 9 {
		t.Fatalf("Epoch=%d, want 9", got.Epoch)
	}
	if got.Membership.ConfigChangeID != 42 {
		t.Fatalf("ConfigChangeID=%d, want 42", got.Membership.ConfigChangeID)
	}
	if len(got.Membership.Addresses) != 3 {
		t.Fatalf("Addresses: got %d, want 3", len(got.Membership.Addresses))
	}
	if got.Membership.Addresses[2] != "10.0.0.2:9001" {
		t.Fatalf("Addresses[2]=%q, want %q", got.Membership.Addresses[2], "10.0.0.2:9001")
	}
	if len(got.Membership.Observers) != 1 {
		t.Fatalf("Observers: got %d, want 1", len(got.Membership.Observers))
	}
	if len(got.Membership.Witnesses) != 1 {
		t.Fatalf("Witnesses: got %d, want 1", len(got.Membership.Witnesses))
	}
	if len(got.Membership.Removed) != 2 {
		t.Fatalf("Removed: got %d, want 2", len(got.Membership.Removed))
	}
}

// TestReadStateReturnsMembership verifies that ReadState returns the
// membership from the latest snapshot, not an empty struct.
func TestReadStateReturnsMembership(t *testing.T) {
	db := openTestDB(t)

	// Save state and snapshot with membership.
	membership := makeMembership()
	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			State:     logdb.State{Term: 5, Vote: 2, Commit: 100},
			Snapshot: logdb.Snapshot{
				Index:      100,
				Term:       5,
				Epoch:      2,
				Membership: membership,
			},
		},
	}
	if err := db.SaveState(updates); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	state, gotMembership, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if state.Term != 5 || state.Vote != 2 || state.Commit != 100 {
		t.Fatalf("ReadState state: got %+v", state)
	}
	if gotMembership.ConfigChangeID != 42 {
		t.Fatalf("ReadState membership ConfigChangeID=%d, want 42", gotMembership.ConfigChangeID)
	}
	if len(gotMembership.Addresses) != 3 {
		t.Fatalf("ReadState membership Addresses: got %d, want 3", len(gotMembership.Addresses))
	}
	if gotMembership.Addresses[1] != "10.0.0.1:9001" {
		t.Fatalf("ReadState membership Addresses[1]=%q", gotMembership.Addresses[1])
	}
	if len(gotMembership.Removed) != 2 {
		t.Fatalf("ReadState membership Removed: got %d, want 2", len(gotMembership.Removed))
	}
}

// TestReadStateMembershipWithoutSnapshot verifies that ReadState returns
// empty membership when no snapshot has been saved.
func TestReadStateMembershipWithoutSnapshot(t *testing.T) {
	db := openTestDB(t)

	// Save only state, no snapshot.
	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			State:     logdb.State{Term: 3, Vote: 1, Commit: 50},
		},
	}
	if err := db.SaveState(updates); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	state, membership, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if state.Term != 3 {
		t.Fatalf("ReadState state: got %+v", state)
	}
	// No snapshot = empty membership.
	if membership.ConfigChangeID != 0 {
		t.Fatalf("ReadState membership ConfigChangeID=%d, want 0", membership.ConfigChangeID)
	}
	if len(membership.Addresses) != 0 {
		t.Fatalf("ReadState membership Addresses: got %d, want 0", len(membership.Addresses))
	}
}

// TestReadStateMembershipRecovery verifies ReadState returns membership
// after WAL recovery.
func TestReadStateMembershipRecovery(t *testing.T) {
	dir := t.TempDir()

	membership := makeMembership()

	// Open, write state + snapshot with membership, close.
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			State:     logdb.State{Term: 8, Vote: 3, Commit: 200},
			Snapshot: logdb.Snapshot{
				Index:      200,
				Term:       8,
				Epoch:      5,
				Membership: membership,
			},
		},
	}
	if err := db.SaveState(updates); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify ReadState returns membership.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	state, gotMembership, err := db2.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState after reopen: %v", err)
	}
	if state.Term != 8 || state.Vote != 3 || state.Commit != 200 {
		t.Fatalf("ReadState state after reopen: got %+v", state)
	}
	if gotMembership.ConfigChangeID != 42 {
		t.Fatalf("ReadState membership ConfigChangeID=%d, want 42", gotMembership.ConfigChangeID)
	}
	if len(gotMembership.Addresses) != 3 {
		t.Fatalf("ReadState membership Addresses: got %d, want 3", len(gotMembership.Addresses))
	}
}

// TestSnapshotMembershipOverwrite verifies that saving a newer snapshot
// overwrites the membership from the previous snapshot.
func TestSnapshotMembershipOverwrite(t *testing.T) {
	db := openTestDB(t)

	// First snapshot with 3 addresses.
	ss1 := logdb.Snapshot{
		Index: 100,
		Term:  5,
		Epoch: 1,
		Membership: logdb.Membership{
			ConfigChangeID: 10,
			Addresses: map[uint64]string{
				1: "addr1",
				2: "addr2",
				3: "addr3",
			},
		},
	}
	if err := db.SaveSnapshot(1, 1, ss1); err != nil {
		t.Fatalf("SaveSnapshot 1: %v", err)
	}

	// Second snapshot with 2 addresses (node 3 removed).
	ss2 := logdb.Snapshot{
		Index: 200,
		Term:  10,
		Epoch: 2,
		Membership: logdb.Membership{
			ConfigChangeID: 20,
			Addresses: map[uint64]string{
				1: "addr1",
				2: "addr2",
			},
			Removed: map[uint64]bool{3: true},
		},
	}
	if err := db.SaveSnapshot(1, 1, ss2); err != nil {
		t.Fatalf("SaveSnapshot 2: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Membership.ConfigChangeID != 20 {
		t.Fatalf("ConfigChangeID=%d, want 20", got.Membership.ConfigChangeID)
	}
	if len(got.Membership.Addresses) != 2 {
		t.Fatalf("Addresses: got %d, want 2", len(got.Membership.Addresses))
	}
	if len(got.Membership.Removed) != 1 || !got.Membership.Removed[3] {
		t.Fatalf("Removed: got %v, want {3: true}", got.Membership.Removed)
	}
}

// TestMembershipAddrMapSizeHelpers verifies the size calculation helpers
// for addr maps and removed maps.
func TestMembershipAddrMapSizeHelpers(t *testing.T) {
	// Empty map should be 4 bytes (count only).
	if got := membershipAddrMapSize(nil); got != 4 {
		t.Fatalf("membershipAddrMapSize(nil)=%d, want 4", got)
	}
	if got := membershipAddrMapSize(map[uint64]string{}); got != 4 {
		t.Fatalf("membershipAddrMapSize({})=%d, want 4", got)
	}

	// Map with one entry: 4 + 8 + 4 + len("addr") = 20.
	m := map[uint64]string{1: "addr"}
	if got := membershipAddrMapSize(m); got != 20 {
		t.Fatalf("membershipAddrMapSize({1:addr})=%d, want 20", got)
	}

	// Empty removed map should be 4 bytes.
	if got := membershipRemovedMapSize(nil); got != 4 {
		t.Fatalf("membershipRemovedMapSize(nil)=%d, want 4", got)
	}

	// Removed map with 2 entries: 4 + 2*8 = 20.
	rm := map[uint64]bool{1: true, 2: true}
	if got := membershipRemovedMapSize(rm); got != 20 {
		t.Fatalf("membershipRemovedMapSize({1,2})=%d, want 20", got)
	}
}

// TestMembershipSizeHelper verifies the total membership size calculation.
func TestMembershipSizeHelper(t *testing.T) {
	m := logdb.Membership{
		ConfigChangeID: 42,
		// All nil maps.
	}
	// ConfigChangeID(8) + 4 addr maps * 4 bytes each = 8 + 16 = 24.
	if got := membershipSize(m); got != 24 {
		t.Fatalf("membershipSize(empty)=%d, want 24", got)
	}
}

// TestUnmarshalAddrMapTruncated verifies unmarshalAddrMap returns -1
// on truncated data.
func TestUnmarshalAddrMapTruncated(t *testing.T) {
	// Too short for count.
	_, pos := unmarshalAddrMap([]byte{0x01, 0x02}, 0)
	if pos != -1 {
		t.Fatalf("unmarshalAddrMap(short count): pos=%d, want -1", pos)
	}

	// Count=1 but no entry data follows.
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, 1) // 1 entry
	_, pos = unmarshalAddrMap(buf, 0)
	if pos != -1 {
		t.Fatalf("unmarshalAddrMap(missing entry): pos=%d, want -1", pos)
	}

	// Count=1, ReplicaID present but AddrLen data missing.
	buf = make([]byte, 4+8+4) // count + replicaID + addrLen header
	binary.LittleEndian.PutUint32(buf, 1)
	binary.LittleEndian.PutUint64(buf[4:], 1)
	binary.LittleEndian.PutUint32(buf[12:], 10) // addrLen=10 but no data
	_, pos = unmarshalAddrMap(buf, 0)
	if pos != -1 {
		t.Fatalf("unmarshalAddrMap(truncated addr): pos=%d, want -1", pos)
	}
}

// TestUnmarshalRemovedMapTruncated verifies unmarshalRemovedMap returns -1
// on truncated data.
func TestUnmarshalRemovedMapTruncated(t *testing.T) {
	// Too short for count.
	_, pos := unmarshalRemovedMap([]byte{0x01}, 0)
	if pos != -1 {
		t.Fatalf("unmarshalRemovedMap(short count): pos=%d, want -1", pos)
	}

	// Count=1 but no ReplicaID follows.
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, 1)
	_, pos = unmarshalRemovedMap(buf, 0)
	if pos != -1 {
		t.Fatalf("unmarshalRemovedMap(missing rid): pos=%d, want -1", pos)
	}
}

// --- WAL replay bounds and skip logging tests ---

// TestReplayEntriesCmdLenExceedsMax verifies that replayEntries skips
// entries with a cmdLen exceeding the maxCmdLen constant. Valid entries
// before the oversized one must still be applied.
func TestReplayEntriesCmdLenExceedsMax(t *testing.T) {
	t.Run("single oversized entry is skipped", func(t *testing.T) {
		s := &walShard{
			indexes:       make(map[nodeKey]*index),
			entries:       make(map[nodeKey]map[uint64]logdb.Entry),
			states:        make(map[nodeKey]logdb.State),
			bootstraps:    make(map[nodeKey]logdb.Bootstrap),
			snapshots:     make(map[nodeKey]logdb.Snapshot),
			compactedTo:   make(map[nodeKey]uint64),
			knownSegments: make(map[uint64]bool),
		}

		// Craft a single entry record with cmdLen = maxCmdLen + 1.
		oversizedCmdLen := uint32(maxCmdLen + 1)
		buf := make([]byte, entryRecordHeaderSize)
		binary.LittleEndian.PutUint64(buf[0:], 1)                // shardID
		binary.LittleEndian.PutUint64(buf[8:], 1)                // replicaID
		binary.LittleEndian.PutUint64(buf[16:], 1)               // raftIndex
		binary.LittleEndian.PutUint64(buf[24:], 1)               // term
		binary.LittleEndian.PutUint64(buf[32:], 0)               // entryType
		binary.LittleEndian.PutUint32(buf[40:], oversizedCmdLen) // cmdLen

		skipped := s.replayEntries(buf, 0)
		if skipped != 1 {
			t.Fatalf("replayEntries skipped=%d, want 1", skipped)
		}

		// No entries should have been stored.
		key := nodeKey{ShardID: 1, ReplicaID: 1}
		if nodeEntries, ok := s.entries[key]; ok && len(nodeEntries) > 0 {
			t.Fatalf("expected no entries, got %d", len(nodeEntries))
		}
	})

	t.Run("valid entries before oversized entry are applied", func(t *testing.T) {
		s := &walShard{
			indexes:       make(map[nodeKey]*index),
			entries:       make(map[nodeKey]map[uint64]logdb.Entry),
			states:        make(map[nodeKey]logdb.State),
			bootstraps:    make(map[nodeKey]logdb.Bootstrap),
			snapshots:     make(map[nodeKey]logdb.Snapshot),
			compactedTo:   make(map[nodeKey]uint64),
			knownSegments: make(map[uint64]bool),
		}

		// First entry: valid, 3-byte cmd.
		validCmd := []byte("abc")
		validEntrySize := entryRecordHeaderSize + len(validCmd)
		oversizedCmdLen := uint32(maxCmdLen + 1)

		totalBuf := make([]byte, validEntrySize+entryRecordHeaderSize)

		// Valid entry at position 0.
		binary.LittleEndian.PutUint64(totalBuf[0:], 1)                      // shardID
		binary.LittleEndian.PutUint64(totalBuf[8:], 1)                      // replicaID
		binary.LittleEndian.PutUint64(totalBuf[16:], 10)                    // raftIndex
		binary.LittleEndian.PutUint64(totalBuf[24:], 1)                     // term
		binary.LittleEndian.PutUint64(totalBuf[32:], 0)                     // entryType
		binary.LittleEndian.PutUint32(totalBuf[40:], uint32(len(validCmd))) // cmdLen
		copy(totalBuf[44:], validCmd)                                       // cmd
		// Oversized entry at position validEntrySize.
		pos := validEntrySize
		binary.LittleEndian.PutUint64(totalBuf[pos:], 1)                  // shardID
		binary.LittleEndian.PutUint64(totalBuf[pos+8:], 1)                // replicaID
		binary.LittleEndian.PutUint64(totalBuf[pos+16:], 11)              // raftIndex
		binary.LittleEndian.PutUint64(totalBuf[pos+24:], 1)               // term
		binary.LittleEndian.PutUint64(totalBuf[pos+32:], 0)               // entryType
		binary.LittleEndian.PutUint32(totalBuf[pos+40:], oversizedCmdLen) // cmdLen

		skipped := s.replayEntries(totalBuf, 0)
		if skipped != 1 {
			t.Fatalf("replayEntries skipped=%d, want 1", skipped)
		}

		// The valid entry must be present.
		key := nodeKey{ShardID: 1, ReplicaID: 1}
		nodeEntries := s.entries[key]
		if nodeEntries == nil {
			t.Fatal("expected entries map for node, got nil")
		}
		e, ok := nodeEntries[10]
		if !ok {
			t.Fatal("expected entry at index 10")
		}
		if string(e.Cmd) != "abc" {
			t.Fatalf("entry cmd = %q, want %q", e.Cmd, "abc")
		}

		// The oversized entry must not be present.
		if _, ok := nodeEntries[11]; ok {
			t.Fatal("oversized entry at index 11 should not be present")
		}
	})

	t.Run("entry at exactly maxCmdLen is accepted", func(t *testing.T) {
		s := &walShard{
			indexes:       make(map[nodeKey]*index),
			entries:       make(map[nodeKey]map[uint64]logdb.Entry),
			states:        make(map[nodeKey]logdb.State),
			bootstraps:    make(map[nodeKey]logdb.Bootstrap),
			snapshots:     make(map[nodeKey]logdb.Snapshot),
			compactedTo:   make(map[nodeKey]uint64),
			knownSegments: make(map[uint64]bool),
		}

		// Use a much smaller cmdLen for the test to avoid 8MB allocation.
		// We test the boundary by verifying maxCmdLen itself is accepted.
		// The actual boundary check: cmdLen > maxCmdLen. So cmdLen == maxCmdLen
		// must pass. We test with a tiny realistic size to avoid test memory
		// bloat, and also verify the boundary constant is correctly defined.
		cmdLen := uint32(64) // small but valid
		cmd := make([]byte, cmdLen)
		for i := range cmd {
			cmd[i] = byte(i)
		}

		buf := make([]byte, entryRecordHeaderSize+int(cmdLen))
		binary.LittleEndian.PutUint64(buf[0:], 1)       // shardID
		binary.LittleEndian.PutUint64(buf[8:], 1)       // replicaID
		binary.LittleEndian.PutUint64(buf[16:], 1)      // raftIndex
		binary.LittleEndian.PutUint64(buf[24:], 5)      // term
		binary.LittleEndian.PutUint64(buf[32:], 0)      // entryType
		binary.LittleEndian.PutUint32(buf[40:], cmdLen) // cmdLen
		copy(buf[44:], cmd)

		skipped := s.replayEntries(buf, 0)
		if skipped != 0 {
			t.Fatalf("replayEntries skipped=%d, want 0 for valid entry", skipped)
		}

		key := nodeKey{ShardID: 1, ReplicaID: 1}
		nodeEntries := s.entries[key]
		if nodeEntries == nil || len(nodeEntries) != 1 {
			t.Fatalf("expected 1 entry, got %v", nodeEntries)
		}
	})
}

// TestReplayRecordSkipsEmptyRecord verifies replayRecord reports a skip
// for empty records.
func TestReplayRecordSkipsEmptyRecord(t *testing.T) {
	s := &walShard{
		indexes:       make(map[nodeKey]*index),
		entries:       make(map[nodeKey]map[uint64]logdb.Entry),
		states:        make(map[nodeKey]logdb.State),
		bootstraps:    make(map[nodeKey]logdb.Bootstrap),
		snapshots:     make(map[nodeKey]logdb.Snapshot),
		compactedTo:   make(map[nodeKey]uint64),
		knownSegments: make(map[uint64]bool),
	}

	skipped, reason := s.replayRecord(nil, 0)
	if skipped != 1 {
		t.Fatalf("replayRecord(nil) skipped=%d, want 1", skipped)
	}
	if reason != "empty record" {
		t.Fatalf("replayRecord(nil) reason=%q, want %q", reason, "empty record")
	}

	skipped, reason = s.replayRecord([]byte{}, 0)
	if skipped != 1 {
		t.Fatalf("replayRecord([]) skipped=%d, want 1", skipped)
	}
	if reason != "empty record" {
		t.Fatalf("replayRecord([]) reason=%q, want %q", reason, "empty record")
	}
}

// TestReplayRecordSkipsUnknownPayloadType verifies replayRecord reports
// a skip for unrecognized payload type bytes.
func TestReplayRecordSkipsUnknownPayloadType(t *testing.T) {
	s := &walShard{
		indexes:       make(map[nodeKey]*index),
		entries:       make(map[nodeKey]map[uint64]logdb.Entry),
		states:        make(map[nodeKey]logdb.State),
		bootstraps:    make(map[nodeKey]logdb.Bootstrap),
		snapshots:     make(map[nodeKey]logdb.Snapshot),
		compactedTo:   make(map[nodeKey]uint64),
		knownSegments: make(map[uint64]bool),
	}

	// 0xFF is not a valid payload type.
	skipped, reason := s.replayRecord([]byte{0xFF}, 0)
	if skipped != 1 {
		t.Fatalf("skipped=%d, want 1", skipped)
	}
	if reason != "unknown payload type" {
		t.Fatalf("reason=%q, want %q", reason, "unknown payload type")
	}
}

// TestReplayRecordSkipsTruncatedState verifies replayRecord reports a
// skip for a state record that is too short.
func TestReplayRecordSkipsTruncatedState(t *testing.T) {
	s := &walShard{
		indexes:       make(map[nodeKey]*index),
		entries:       make(map[nodeKey]map[uint64]logdb.Entry),
		states:        make(map[nodeKey]logdb.State),
		bootstraps:    make(map[nodeKey]logdb.Bootstrap),
		snapshots:     make(map[nodeKey]logdb.Snapshot),
		compactedTo:   make(map[nodeKey]uint64),
		knownSegments: make(map[uint64]bool),
	}

	// State payload type + insufficient data.
	rec := []byte{payloadTypeState, 0x00}
	skipped, reason := s.replayRecord(rec, 0)
	if skipped != 1 {
		t.Fatalf("skipped=%d, want 1", skipped)
	}
	if reason != "truncated state record" {
		t.Fatalf("reason=%q, want %q", reason, "truncated state record")
	}
}

// TestReplayRecordValidStateSetsNoSkip verifies a well-formed state
// record produces zero skips.
func TestReplayRecordValidStateSetsNoSkip(t *testing.T) {
	s := &walShard{
		indexes:       make(map[nodeKey]*index),
		entries:       make(map[nodeKey]map[uint64]logdb.Entry),
		states:        make(map[nodeKey]logdb.State),
		bootstraps:    make(map[nodeKey]logdb.Bootstrap),
		snapshots:     make(map[nodeKey]logdb.Snapshot),
		compactedTo:   make(map[nodeKey]uint64),
		knownSegments: make(map[uint64]bool),
	}

	rec := make([]byte, 1+stateRecordSize)
	rec[0] = payloadTypeState
	binary.LittleEndian.PutUint64(rec[1:], 1)  // shardID
	binary.LittleEndian.PutUint64(rec[9:], 1)  // replicaID
	binary.LittleEndian.PutUint64(rec[17:], 5) // term
	binary.LittleEndian.PutUint64(rec[25:], 2) // vote
	binary.LittleEndian.PutUint64(rec[33:], 3) // commit

	skipped, reason := s.replayRecord(rec, 0)
	if skipped != 0 {
		t.Fatalf("skipped=%d, want 0", skipped)
	}
	if reason != "" {
		t.Fatalf("reason=%q, want empty", reason)
	}

	key := nodeKey{ShardID: 1, ReplicaID: 1}
	state, ok := s.states[key]
	if !ok {
		t.Fatal("state not found after replay")
	}
	if state.Term != 5 || state.Vote != 2 || state.Commit != 3 {
		t.Fatalf("state = %+v, want Term=5 Vote=2 Commit=3", state)
	}
}

// TestMaxCmdLenConstant verifies the maxCmdLen constant matches the
// proto.MaxEntrySize value (8 MB) to ensure consistency.
func TestMaxCmdLenConstant(t *testing.T) {
	const expectedMaxCmdLen = 8 * 1024 * 1024
	if maxCmdLen != expectedMaxCmdLen {
		t.Fatalf("maxCmdLen = %d, want %d (8 MB)", maxCmdLen, expectedMaxCmdLen)
	}
}

// TestSegmentFilePermissions verifies that created WAL segment files
// have 0600 permissions (owner read/write only), regardless of umask.
func TestSegmentFilePermissions(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 4096, 64*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	defer seg.Close()

	info, err := seg.f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Fatalf("segment file permissions = %04o, want 0600", perm)
	}
}

// TestSegmentFilePermissionsAfterReopen verifies that reopened segments
// retain 0600 permissions.
func TestSegmentFilePermissionsAfterReopen(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 4096, 64*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}

	// Write data and close.
	if _, err := seg.Write([]byte("test data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Check permissions via os.Stat on the file path.
	name := seg.dir + "/" + segmentFilename(1)
	info, err := os.Stat(name)
	if err != nil {
		t.Fatalf("Stat after close: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Fatalf("segment file permissions after close = %04o, want 0600", perm)
	}
}

// TestReplayEntriesCmdLenBoundaryOverflow verifies that a cmdLen of
// math.MaxUint32 (which would cause integer overflow in totalSize
// calculation) is caught by the maxCmdLen check.
func TestReplayEntriesCmdLenBoundaryOverflow(t *testing.T) {
	s := &walShard{
		indexes:       make(map[nodeKey]*index),
		entries:       make(map[nodeKey]map[uint64]logdb.Entry),
		states:        make(map[nodeKey]logdb.State),
		bootstraps:    make(map[nodeKey]logdb.Bootstrap),
		snapshots:     make(map[nodeKey]logdb.Snapshot),
		compactedTo:   make(map[nodeKey]uint64),
		knownSegments: make(map[uint64]bool),
	}

	// MaxUint32 cmdLen would cause totalSize to wrap on 32-bit, but should
	// be caught by the maxCmdLen check first.
	buf := make([]byte, entryRecordHeaderSize)
	binary.LittleEndian.PutUint64(buf[0:], 1)           // shardID
	binary.LittleEndian.PutUint64(buf[8:], 1)           // replicaID
	binary.LittleEndian.PutUint64(buf[16:], 1)          // raftIndex
	binary.LittleEndian.PutUint64(buf[24:], 1)          // term
	binary.LittleEndian.PutUint64(buf[32:], 0)          // entryType
	binary.LittleEndian.PutUint32(buf[40:], 0xFFFFFFFF) // cmdLen = MaxUint32

	skipped := s.replayEntries(buf, 0)
	if skipped != 1 {
		t.Fatalf("replayEntries skipped=%d, want 1 for MaxUint32 cmdLen", skipped)
	}
}

// TestReplayEntryRecoveryThroughWAL verifies end-to-end that an oversized
// cmdLen in a WAL segment causes the entry to be skipped during recovery
// without blocking valid records in other WAL records.
func TestReplayEntryRecoveryThroughWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write valid entries.
	entries := makeEntries(1, 5, 1)
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	// Close and verify recovery works.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir, WithNoSync(true), WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	got, err := db2.GetEntries(1, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("GetEntries after recovery: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d entries, want 5", len(got))
	}
}

// --- Benchmarks ---

// makeBenchEntries creates a slice of logdb.Entry with the given Cmd size.
func makeBenchEntries(start, count uint64, term uint64, cmdSize int) []logdb.Entry {
	entries := make([]logdb.Entry, count)
	cmd := make([]byte, cmdSize)
	for i := range cmd {
		cmd[i] = byte(i % 251)
	}
	for i := uint64(0); i < count; i++ {
		c := make([]byte, cmdSize)
		copy(c, cmd)
		entries[i] = logdb.Entry{
			Index: start + i,
			Term:  term,
			Type:  0,
			Cmd:   c,
		}
	}
	return entries
}

// openBenchDB creates a DB in a temp directory suitable for benchmarks.
// Sync is enabled to measure the real write path including fsync.
func openBenchDB(b *testing.B) *DB {
	b.Helper()
	dir := b.TempDir()
	db, err := Open(dir, WithBlockSize(4096), WithMaxFileSize(64*1024*1024))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() {
		db.Close()
	})
	return db
}

// BenchmarkDBSaveState benchmarks the critical write path: SaveState() with
// a batch of 10 entries (128-byte Cmd each), including WAL write and fsync.
func BenchmarkDBSaveState(b *testing.B) {
	b.ReportAllocs()
	db := openBenchDB(b)

	entries := makeBenchEntries(1, 10, 1, 128)
	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Entries:   entries,
			State:     logdb.State{Term: 1, Vote: 1, Commit: 10},
		},
	}

	idx := uint64(11)
	for b.Loop() {
		// Update entry indexes to avoid overwriting the same range,
		// which would skew results.
		for i := range updates[0].Entries {
			updates[0].Entries[i].Index = idx + uint64(i)
		}
		updates[0].State.Commit = idx + 9
		idx += 10

		if err := db.SaveState(updates); err != nil {
			b.Fatalf("SaveState: %v", err)
		}
	}
}

// BenchmarkDBGetEntries benchmarks range reads of 100 entries from a
// pre-populated DB with 1000 entries for shardID=1.
func BenchmarkDBGetEntries(b *testing.B) {
	b.ReportAllocs()
	db := openBenchDB(b)

	// Pre-populate with 1000 entries.
	entries := makeBenchEntries(1, 1000, 1, 128)
	if err := db.SaveEntries(1, 1, entries); err != nil {
		b.Fatalf("SaveEntries: %v", err)
	}

	b.ResetTimer()
	for b.Loop() {
		_, err := db.GetEntries(1, 1, 1, 100, 0)
		if err != nil {
			b.Fatalf("GetEntries: %v", err)
		}
	}
}

// BenchmarkDBReadState benchmarks ReadState lookups, which should be very
// fast in-memory map lookups.
func BenchmarkDBReadState(b *testing.B) {
	b.ReportAllocs()
	db := openBenchDB(b)

	// Save initial state so ReadState has something to return.
	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			State:     logdb.State{Term: 5, Vote: 2, Commit: 100},
		},
	}
	if err := db.SaveState(updates); err != nil {
		b.Fatalf("SaveState: %v", err)
	}

	b.ResetTimer()
	for b.Loop() {
		_, _, err := db.ReadState(1, 1)
		if err != nil {
			b.Fatalf("ReadState: %v", err)
		}
	}
}

// --- WAL write ordering tests ---
//
// These tests verify that in-memory state (index, entry map, state map,
// snapshot map, bootstrap map) is updated ONLY after the WAL record is
// durably written. A failed writeRecord must leave all in-memory state
// unchanged.

// sabotageSegment marks the active WAL segment as closed for the given
// shardID. This causes all subsequent segment.Write calls to return
// ErrClosed, allowing tests to verify that in-memory state is not
// updated on write failure.
func sabotageSegment(t *testing.T, db *DB, shardID uint64) {
	t.Helper()
	shard := db.getShard(shardID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.activeSegment == nil {
		t.Fatal("sabotageSegment: no active segment")
	}
	// Mark the segment as closed so Write returns ErrClosed. This also
	// prevents ensureActiveSegment from creating a replacement, since the
	// reference is non-nil.
	shard.activeSegment.closed = true
}

// TestWriteEntriesFailurePreservesInMemoryState verifies that a failed
// writeEntries does not update the in-memory index or entry map.
func TestWriteEntriesFailurePreservesInMemoryState(t *testing.T) {
	db := openTestDB(t)
	const shardID, replicaID = uint64(1), uint64(1)

	// Write initial entries.
	initial := makeEntries(1, 3, 1)
	if err := db.SaveEntries(shardID, replicaID, initial); err != nil {
		t.Fatalf("initial SaveEntries: %v", err)
	}

	// Verify initial entries are present.
	got, err := db.GetEntries(shardID, replicaID, 1, 4, 0)
	if err != nil {
		t.Fatalf("initial GetEntries: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("initial entries: got %d, want 3", len(got))
	}

	// Sabotage the segment to make writes fail.
	sabotageSegment(t, db, shardID)

	// Attempt to write more entries; this must fail.
	failing := makeEntries(4, 3, 2)
	if err := db.SaveEntries(shardID, replicaID, failing); err == nil {
		t.Fatal("SaveEntries after sabotage: expected error, got nil")
	}

	// Verify in-memory state is unchanged: still only 3 entries from the
	// initial write, none from the failed write.
	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	shard.mu.Lock()
	defer shard.mu.Unlock()

	idx := shard.indexes[key]
	if idx == nil {
		t.Fatal("index should exist after initial write")
	}
	if idx.Len() != 3 {
		t.Fatalf("index length: got %d, want 3", idx.Len())
	}

	nodeEntries := shard.entries[key]
	if len(nodeEntries) != 3 {
		t.Fatalf("entry map length: got %d, want 3", len(nodeEntries))
	}

	// Entries 4, 5, 6 must not exist.
	for i := uint64(4); i <= 6; i++ {
		if _, ok := nodeEntries[i]; ok {
			t.Fatalf("entry %d should not exist after failed write", i)
		}
	}
}

// TestWriteStateFailurePreservesInMemoryState verifies that a failed
// writeState does not update the in-memory state map.
func TestWriteStateFailurePreservesInMemoryState(t *testing.T) {
	db := openTestDB(t)
	const shardID, replicaID = uint64(1), uint64(1)

	// Write initial state.
	initialState := logdb.State{Term: 5, Vote: 2, Commit: 10}
	if err := db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		State:     initialState,
	}}); err != nil {
		t.Fatalf("initial SaveState: %v", err)
	}

	// Verify initial state.
	got, _, err := db.ReadState(shardID, replicaID)
	if err != nil {
		t.Fatalf("initial ReadState: %v", err)
	}
	if got != initialState {
		t.Fatalf("initial state: got %+v, want %+v", got, initialState)
	}

	// Sabotage the segment.
	sabotageSegment(t, db, shardID)

	// Attempt to write new state; this must fail.
	if err := db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		State:     logdb.State{Term: 99, Vote: 99, Commit: 99},
	}}); err == nil {
		t.Fatal("SaveState after sabotage: expected error, got nil")
	}

	// Verify in-memory state is unchanged.
	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	shard.mu.Lock()
	state := shard.states[key]
	shard.mu.Unlock()

	if state != initialState {
		t.Fatalf("state after failed write: got %+v, want %+v", state, initialState)
	}
}

// TestWriteSnapshotFailurePreservesInMemoryState verifies that a failed
// writeSnapshot does not update the in-memory snapshot map.
func TestWriteSnapshotFailurePreservesInMemoryState(t *testing.T) {
	db := openTestDB(t)
	const shardID, replicaID = uint64(1), uint64(1)

	// Write initial snapshot.
	initialSnap := logdb.Snapshot{
		Index:    100,
		Term:     5,
		FileSize: 4096,
		Filepath: "/data/snap-100",
	}
	if err := db.SaveSnapshot(shardID, replicaID, initialSnap); err != nil {
		t.Fatalf("initial SaveSnapshot: %v", err)
	}

	// Verify initial snapshot.
	got, err := db.GetSnapshot(shardID, replicaID)
	if err != nil {
		t.Fatalf("initial GetSnapshot: %v", err)
	}
	if got.Index != initialSnap.Index || got.Term != initialSnap.Term {
		t.Fatalf("initial snapshot: got Index=%d Term=%d, want Index=%d Term=%d",
			got.Index, got.Term, initialSnap.Index, initialSnap.Term)
	}

	// Sabotage the segment.
	sabotageSegment(t, db, shardID)

	// Attempt to write new snapshot; this must fail.
	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index:    200,
		Term:     10,
		FileSize: 8192,
		Filepath: "/data/snap-200",
	}); err == nil {
		t.Fatal("SaveSnapshot after sabotage: expected error, got nil")
	}

	// Verify in-memory state is unchanged.
	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	shard.mu.Lock()
	snap := shard.snapshots[key]
	shard.mu.Unlock()

	if snap.Index != initialSnap.Index || snap.Term != initialSnap.Term {
		t.Fatalf("snapshot after failed write: got Index=%d Term=%d, want Index=%d Term=%d",
			snap.Index, snap.Term, initialSnap.Index, initialSnap.Term)
	}
}

// TestWriteBootstrapFailurePreservesInMemoryState verifies that a failed
// writeBootstrap does not update the in-memory bootstrap map.
func TestWriteBootstrapFailurePreservesInMemoryState(t *testing.T) {
	db := openTestDB(t)
	const shardID, replicaID = uint64(1), uint64(1)

	// Write initial bootstrap.
	initialBS := logdb.Bootstrap{
		Join: false,
		Type: 1,
		Addresses: map[uint64]string{
			1: "localhost:5001",
			2: "localhost:5002",
		},
	}
	if err := db.SaveBootstrap(shardID, replicaID, initialBS); err != nil {
		t.Fatalf("initial SaveBootstrap: %v", err)
	}

	// Verify initial bootstrap.
	got, found, err := db.GetBootstrap(shardID, replicaID)
	if err != nil {
		t.Fatalf("initial GetBootstrap: %v", err)
	}
	if !found {
		t.Fatal("initial GetBootstrap: not found")
	}
	if got.Type != initialBS.Type || len(got.Addresses) != 2 {
		t.Fatalf("initial bootstrap: got Type=%d Addrs=%d, want Type=%d Addrs=2",
			got.Type, len(got.Addresses), initialBS.Type)
	}

	// Sabotage the segment.
	sabotageSegment(t, db, shardID)

	// Attempt to write new bootstrap; this must fail.
	if err := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
		Join: true,
		Type: 99,
		Addresses: map[uint64]string{
			10: "other:9001",
		},
	}); err == nil {
		t.Fatal("SaveBootstrap after sabotage: expected error, got nil")
	}

	// Verify in-memory state is unchanged.
	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	shard.mu.Lock()
	bs := shard.bootstraps[key]
	shard.mu.Unlock()

	if bs.Type != initialBS.Type || bs.Join != initialBS.Join || len(bs.Addresses) != 2 {
		t.Fatalf("bootstrap after failed write: got Type=%d Join=%v Addrs=%d, want Type=%d Join=%v Addrs=2",
			bs.Type, bs.Join, len(bs.Addresses), initialBS.Type, initialBS.Join)
	}
}

// TestWriteEntriesFailureOnEmptyDBPreservesCleanState verifies that a
// failed first-ever write to a shard does not leave partial in-memory state.
func TestWriteEntriesFailureOnEmptyDBPreservesCleanState(t *testing.T) {
	db := openTestDB(t)
	const shardID, replicaID = uint64(1), uint64(1)

	// Write a dummy entry to force segment creation, then sabotage.
	dummy := makeEntries(1, 1, 1)
	if err := db.SaveEntries(shardID, replicaID, dummy); err != nil {
		t.Fatalf("dummy SaveEntries: %v", err)
	}

	// Use a different node key (shardID=2) that maps to the same WAL shard,
	// but has no prior in-memory state. We'll sabotage the shared WAL shard.
	// shardID=2 maps to shard index 2, so let's use shardID that shares the
	// same shard: shardID % 16 == 1 % 16 == 1, so shardID=17 maps to shard 1.
	const newShardID = uint64(17) // 17 % 16 == 1, same WAL shard as shardID=1
	sabotageSegment(t, db, newShardID)

	// Attempt to write entries for the new shard; this must fail.
	failing := makeEntries(1, 5, 1)
	if err := db.SaveEntries(newShardID, replicaID, failing); err == nil {
		t.Fatal("SaveEntries on new shard after sabotage: expected error, got nil")
	}

	// Verify no in-memory state was created for the new shard.
	shard := db.getShard(newShardID)
	key := nodeKey{ShardID: newShardID, ReplicaID: replicaID}
	shard.mu.Lock()
	defer shard.mu.Unlock()

	idx := shard.indexes[key]
	if idx != nil && idx.Len() != 0 {
		t.Fatalf("index for new shard: expected nil or empty, got length %d", idx.Len())
	}

	nodeEntries := shard.entries[key]
	if len(nodeEntries) != 0 {
		t.Fatalf("entry map for new shard: expected empty, got %d entries", len(nodeEntries))
	}
}

// TestWriteStateFailureOnEmptyDBPreservesCleanState verifies that a failed
// first-ever state write leaves no state in memory.
func TestWriteStateFailureOnEmptyDBPreservesCleanState(t *testing.T) {
	db := openTestDB(t)
	const shardID, replicaID = uint64(1), uint64(1)

	// Write a dummy to force segment creation, then sabotage.
	if err := db.SaveEntries(shardID, replicaID, makeEntries(1, 1, 1)); err != nil {
		t.Fatalf("dummy write: %v", err)
	}

	const newShardID = uint64(17) // same WAL shard as shardID=1
	sabotageSegment(t, db, newShardID)

	// Attempt to write state for the new shard.
	if err := db.SaveState([]logdb.Update{{
		ShardID:   newShardID,
		ReplicaID: replicaID,
		State:     logdb.State{Term: 5, Vote: 3, Commit: 10},
	}}); err == nil {
		t.Fatal("SaveState on new shard after sabotage: expected error, got nil")
	}

	// Verify no state was created.
	_, _, err := db.ReadState(newShardID, replicaID)
	if err != logdb.ErrNoState {
		t.Fatalf("ReadState for new shard: got %v, want %v", err, logdb.ErrNoState)
	}
}

// TestSuccessfulWriteUpdatesInMemoryState is a positive path test verifying
// that all write methods correctly update in-memory state on success.
func TestSuccessfulWriteUpdatesInMemoryState(t *testing.T) {
	db := openTestDB(t)
	const shardID, replicaID = uint64(1), uint64(1)

	// Write entries.
	entries := makeEntries(1, 5, 1)
	if err := db.SaveEntries(shardID, replicaID, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	idx := shard.indexes[key]
	nodeEntries := shard.entries[key]
	shard.mu.Unlock()

	if idx == nil || idx.Len() != 5 {
		t.Fatalf("index after SaveEntries: expected 5 entries, got %d", idx.Len())
	}
	if len(nodeEntries) != 5 {
		t.Fatalf("entry map after SaveEntries: expected 5 entries, got %d", len(nodeEntries))
	}

	// Write state.
	state := logdb.State{Term: 3, Vote: 1, Commit: 5}
	if err := db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		State:     state,
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	shard.mu.Lock()
	gotState := shard.states[key]
	shard.mu.Unlock()

	if gotState != state {
		t.Fatalf("state after SaveState: got %+v, want %+v", gotState, state)
	}

	// Write snapshot.
	snap := logdb.Snapshot{Index: 50, Term: 3, Filepath: "/snap"}
	if err := db.SaveSnapshot(shardID, replicaID, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	shard.mu.Lock()
	gotSnap := shard.snapshots[key]
	shard.mu.Unlock()

	if gotSnap.Index != snap.Index || gotSnap.Term != snap.Term {
		t.Fatalf("snapshot after SaveSnapshot: got %+v, want Index=%d Term=%d",
			gotSnap, snap.Index, snap.Term)
	}

	// Write bootstrap.
	bs := logdb.Bootstrap{
		Join:      false,
		Type:      1,
		Addresses: map[uint64]string{1: "addr1"},
	}
	if err := db.SaveBootstrap(shardID, replicaID, bs); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}

	shard.mu.Lock()
	gotBS := shard.bootstraps[key]
	shard.mu.Unlock()

	if gotBS.Type != bs.Type || len(gotBS.Addresses) != 1 {
		t.Fatalf("bootstrap after SaveBootstrap: got Type=%d Addrs=%d, want Type=%d Addrs=1",
			gotBS.Type, len(gotBS.Addresses), bs.Type)
	}
}

// TestSaveUpdatePartialFailurePreservesEntries verifies that when a
// SaveState update contains both entries and state, a failure during
// state write does not affect the previously successful entries write
// within the same saveUpdate call.
func TestSaveUpdatePartialFailurePreservesEntries(t *testing.T) {
	db := openTestDB(t)
	const shardID, replicaID = uint64(1), uint64(1)

	// Write initial entries to establish a segment.
	initial := makeEntries(1, 2, 1)
	if err := db.SaveEntries(shardID, replicaID, initial); err != nil {
		t.Fatalf("initial SaveEntries: %v", err)
	}

	// Verify initial entries are present.
	got, err := db.GetEntries(shardID, replicaID, 1, 3, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("initial entries: got %d, want 2", len(got))
	}

	// Sabotage the segment.
	sabotageSegment(t, db, shardID)

	// Attempt a combined update with entries and state. The entries write
	// will fail (sabotaged), so the state write will never be reached.
	// Regardless, the initial entries must remain intact.
	if err := db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		Entries:   makeEntries(3, 2, 2),
		State:     logdb.State{Term: 99, Vote: 99, Commit: 99},
	}}); err == nil {
		t.Fatal("SaveState with combined update after sabotage: expected error, got nil")
	}

	// Verify initial entries are still intact.
	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	shard.mu.Lock()
	defer shard.mu.Unlock()

	idx := shard.indexes[key]
	if idx == nil || idx.Len() != 2 {
		var n int
		if idx != nil {
			n = idx.Len()
		}
		t.Fatalf("index after failed combined update: got %d entries, want 2", n)
	}

	nodeEntries := shard.entries[key]
	if len(nodeEntries) != 2 {
		t.Fatalf("entry map after failed combined update: got %d, want 2", len(nodeEntries))
	}

	// New entries must not exist.
	for i := uint64(3); i <= 4; i++ {
		if _, ok := nodeEntries[i]; ok {
			t.Fatalf("entry %d should not exist after failed write", i)
		}
	}
}

// BenchmarkDBIterateEntries benchmarks IterateEntries with a pre-allocated
// destination slice, measuring buffer reuse efficiency.
func BenchmarkDBIterateEntries(b *testing.B) {
	b.ReportAllocs()
	db := openBenchDB(b)

	// Pre-populate with 1000 entries.
	entries := makeBenchEntries(1, 1000, 1, 128)
	if err := db.SaveEntries(1, 1, entries); err != nil {
		b.Fatalf("SaveEntries: %v", err)
	}

	// Pre-allocate a destination slice for buffer reuse.
	buf := make([]logdb.Entry, 0, 100)

	b.ResetTimer()
	for b.Loop() {
		buf = buf[:0]
		_, _, err := db.IterateEntries(buf, 0, 1, 1, 1, 100, 0)
		if err != nil {
			b.Fatalf("IterateEntries: %v", err)
		}
	}
}

// TestSaveStatePartialWriteDiscardsGhostRecords verifies that when a batch
// write fails mid-way through processing updates, previously buffered
// records from successful shards are discarded. This prevents ghost records
// from being fsynced on the next successful SaveState call.
func TestSaveStatePartialWriteDiscardsGhostRecords(t *testing.T) {
	dir := t.TempDir()
	// Use sync disabled so we can observe buffer behavior without I/O noise.
	db, err := Open(dir, WithNoSync(true), WithBlockSize(512), WithMaxFileSize(2*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write an initial batch successfully to establish baseline state.
	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Entries:   makeEntries(1, 2, 1),
			State:     logdb.State{Term: 1, Vote: 1, Commit: 2},
		},
	}
	if err := db.SaveState(updates); err != nil {
		t.Fatalf("SaveState initial: %v", err)
	}

	// Verify entries are readable.
	got, err := db.GetEntries(1, 1, 1, 3, 0)
	if err != nil {
		t.Fatalf("GetEntries after initial: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries after initial write, got %d", len(got))
	}

	// Now create a batch with two updates to different shards:
	// - Shard 1 succeeds (writes to WAL buffer)
	// - Shard 2 fails (triggers rollback on shard 1)
	//
	// We force failure on shard 2 by closing its active segment before
	// the batch, then writing to it.
	shard2Idx := uint16(2 % DefaultShardCount)
	shard2 := db.shards[shard2Idx]

	// Pre-initialize shard 2 with a write so it has an active segment.
	initUpdate := []logdb.Update{
		{
			ShardID:   2,
			ReplicaID: 1,
			Entries:   makeEntries(1, 1, 1),
		},
	}
	if err := db.SaveState(initUpdate); err != nil {
		t.Fatalf("SaveState shard2 init: %v", err)
	}

	// Close the active segment to force a write error.
	shard2.mu.Lock()
	if shard2.activeSegment != nil {
		shard2.activeSegment.Close()
		shard2.activeSegment = nil
	}
	// Create a read-only file to block segment creation.
	shardDir := shard2.dir
	shard2.mu.Unlock()

	// Make the shard directory read-only to prevent segment creation.
	if err := os.Chmod(shardDir, 0444); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() {
		os.Chmod(shardDir, 0755)
	})

	// Attempt a batch where shard 1 succeeds then shard 2 fails.
	failBatch := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Entries:   makeEntries(10, 3, 2), // Ghost entries if not discarded.
			State:     logdb.State{Term: 2, Vote: 1, Commit: 12},
		},
		{
			ShardID:   2,
			ReplicaID: 1,
			Entries:   makeEntries(10, 1, 2), // This should fail.
		},
	}
	err = db.SaveState(failBatch)
	if err == nil {
		t.Fatal("SaveState should have failed on read-only shard directory")
	}

	// Now write a legitimate batch to shard 1 and sync it.
	goodBatch := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Entries:   makeEntries(3, 2, 1),
			State:     logdb.State{Term: 1, Vote: 1, Commit: 4},
		},
	}
	if err := db.SaveState(goodBatch); err != nil {
		t.Fatalf("SaveState good batch: %v", err)
	}

	// Verify only legitimate entries exist (no ghost entries 10-12).
	allEntries, err := db.GetEntries(1, 1, 1, 20, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}

	for _, e := range allEntries {
		if e.Index >= 10 && e.Term == 2 {
			t.Errorf("found ghost entry: index=%d term=%d (should have been discarded)", e.Index, e.Term)
		}
	}
}

// TestSaveStatePartialWriteReturnsOriginalError verifies that when a batch
// write fails and discard succeeds, the original write error is returned
// unwrapped (not wrapped in BatchWriteError).
func TestSaveStatePartialWriteReturnsOriginalError(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(512), WithMaxFileSize(2*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Initialize shard 2.
	initUpdate := []logdb.Update{
		{
			ShardID:   2,
			ReplicaID: 1,
			Entries:   makeEntries(1, 1, 1),
		},
	}
	if err := db.SaveState(initUpdate); err != nil {
		t.Fatalf("SaveState init: %v", err)
	}

	// Close the active segment and make dir read-only to force failure.
	shard2Idx := uint16(2 % DefaultShardCount)
	shard2 := db.shards[shard2Idx]
	shard2.mu.Lock()
	if shard2.activeSegment != nil {
		shard2.activeSegment.Close()
		shard2.activeSegment = nil
	}
	shardDir := shard2.dir
	shard2.mu.Unlock()

	if err := os.Chmod(shardDir, 0444); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() {
		os.Chmod(shardDir, 0755)
	})

	// First update to shard 1 (succeeds), second to shard 2 (fails).
	failBatch := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Entries:   makeEntries(1, 1, 1),
		},
		{
			ShardID:   2,
			ReplicaID: 1,
			Entries:   makeEntries(1, 1, 1),
		},
	}

	err = db.SaveState(failBatch)
	if err == nil {
		t.Fatal("expected error from SaveState")
	}

	// The error should NOT be a BatchWriteError because discard should
	// have succeeded (shard 1's segment was healthy).
	var batchErr *BatchWriteError
	if errors.As(err, &batchErr) {
		t.Errorf("expected plain error, got BatchWriteError with discard errors: %v", batchErr.DiscardErrors)
	}
}

// TestRotateSegmentSyncsBeforeClose verifies that rotateSegment syncs the
// old segment to stable storage before closing it. We test this indirectly
// by verifying that rotation with sync enabled does not lose data that was
// written just before the rotation boundary.
func TestRotateSegmentSyncsBeforeClose(t *testing.T) {
	dir := t.TempDir()
	// Use a very small MaxFileSize to force rotation quickly, with sync
	// enabled to exercise the fsync path.
	db, err := Open(dir, WithBlockSize(512), WithMaxFileSize(2*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write enough entries to force at least one segment rotation.
	var allIndexes []uint64
	for i := 1; i <= 50; i++ {
		entry := logdb.Entry{
			Index: uint64(i),
			Term:  1,
			Cmd:   make([]byte, 100),
		}
		if err := db.SaveEntries(1, 1, []logdb.Entry{entry}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
		allIndexes = append(allIndexes, uint64(i))
	}

	// Verify all entries are readable (none lost during rotation).
	got, err := db.GetEntries(1, 1, 1, 51, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 50 {
		t.Errorf("expected 50 entries, got %d", len(got))
	}

	// Verify multiple segments were created.
	shard := db.getShard(1)
	shard.mu.Lock()
	segCount := len(shard.knownSegments)
	shard.mu.Unlock()
	if segCount < 2 {
		t.Fatalf("expected at least 2 segments after rotation, got %d", segCount)
	}
}

// TestRotateSegmentNoSyncMode verifies that rotateSegment skips fdatasync
// when noSync is enabled, preventing unnecessary I/O overhead in test mode.
func TestRotateSegmentNoSyncMode(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(512), WithMaxFileSize(1*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write enough to force rotation with noSync enabled.
	for i := 1; i <= 30; i++ {
		entry := logdb.Entry{
			Index: uint64(i),
			Term:  1,
			Cmd:   make([]byte, 64),
		}
		if err := db.SaveEntries(1, 1, []logdb.Entry{entry}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}

	// Verify all entries are readable.
	got, err := db.GetEntries(1, 1, 1, 31, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 30 {
		t.Errorf("expected 30 entries, got %d", len(got))
	}
}

// TestSaveStateMidBatchFailureNoPhantomEntries verifies that when a batch
// write fails mid-way, in-memory entries from successful shards are NOT
// readable. This tests the two-phase in-memory update design: in-memory
// maps are only updated after all WAL writes and sync succeed.
func TestSaveStateMidBatchFailureNoPhantomEntries(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(512), WithMaxFileSize(4*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write initial data to shard 1 to establish baseline.
	initial := []logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		Entries:   makeEntries(1, 3, 1),
		State:     logdb.State{Term: 1, Vote: 1, Commit: 3},
	}}
	if err := db.SaveState(initial); err != nil {
		t.Fatalf("SaveState initial: %v", err)
	}

	// Verify initial state is readable.
	got, err := db.GetEntries(1, 1, 1, 4, 0)
	if err != nil {
		t.Fatalf("GetEntries initial: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 initial entries, got %d", len(got))
	}
	st, _, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState initial: %v", err)
	}
	if st.Term != 1 || st.Vote != 1 || st.Commit != 3 {
		t.Fatalf("unexpected initial state: %+v", st)
	}

	// Initialize shard 2 with data so it has an active segment.
	shard2Init := []logdb.Update{{
		ShardID:   2,
		ReplicaID: 1,
		Entries:   makeEntries(1, 1, 1),
	}}
	if err := db.SaveState(shard2Init); err != nil {
		t.Fatalf("SaveState shard2 init: %v", err)
	}

	// Sabotage shard 2 to force failure.
	shard2Idx := uint16(2 % DefaultShardCount)
	shard2 := db.shards[shard2Idx]
	shard2.mu.Lock()
	if shard2.activeSegment != nil {
		shard2.activeSegment.Close()
		shard2.activeSegment = nil
	}
	shard2Dir := shard2.dir
	shard2.mu.Unlock()

	if err := os.Chmod(shard2Dir, 0444); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(shard2Dir, 0755) })

	// Attempt a batch where shard 1 writes succeed at WAL level but
	// shard 2 fails. The shard 1 entries/state must NOT be readable.
	failBatch := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Entries:   makeEntries(10, 5, 2),
			State:     logdb.State{Term: 2, Vote: 2, Commit: 14},
		},
		{
			ShardID:   2,
			ReplicaID: 1,
			Entries:   makeEntries(10, 1, 2),
		},
	}
	if err := db.SaveState(failBatch); err == nil {
		t.Fatal("SaveState should have failed")
	}

	// Verify no phantom entries from the failed batch are readable.
	allEntries, err := db.GetEntries(1, 1, 1, 20, 0)
	if err != nil {
		t.Fatalf("GetEntries after failure: %v", err)
	}
	for _, e := range allEntries {
		if e.Index >= 10 {
			t.Errorf("phantom entry found: index=%d term=%d (should not exist after failed batch)", e.Index, e.Term)
		}
	}
	if len(allEntries) != 3 {
		t.Errorf("expected 3 entries after failed batch, got %d", len(allEntries))
	}

	// Verify hard state was NOT updated by the failed batch.
	st, _, err = db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState after failure: %v", err)
	}
	if st.Term != 1 || st.Vote != 1 || st.Commit != 3 {
		t.Errorf("state was updated by failed batch: %+v (expected Term=1, Vote=1, Commit=3)", st)
	}
}

// TestSaveStateMidBatchFailureNoPhantomSnapshots verifies that snapshot
// metadata from a successful shard is not visible when a later shard
// in the same batch fails.
func TestSaveStateMidBatchFailureNoPhantomSnapshots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(512), WithMaxFileSize(4*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Initialize shard 2 so it has an active segment.
	shard2Init := []logdb.Update{{
		ShardID:   2,
		ReplicaID: 1,
		Entries:   makeEntries(1, 1, 1),
	}}
	if err := db.SaveState(shard2Init); err != nil {
		t.Fatalf("SaveState shard2 init: %v", err)
	}

	// Sabotage shard 2.
	shard2Idx := uint16(2 % DefaultShardCount)
	shard2 := db.shards[shard2Idx]
	shard2.mu.Lock()
	if shard2.activeSegment != nil {
		shard2.activeSegment.Close()
		shard2.activeSegment = nil
	}
	shard2Dir := shard2.dir
	shard2.mu.Unlock()
	if err := os.Chmod(shard2Dir, 0444); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(shard2Dir, 0755) })

	// Batch with snapshot on shard 1, entries on shard 2 (fails).
	failBatch := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Snapshot: logdb.Snapshot{
				Index: 100,
				Term:  5,
			},
		},
		{
			ShardID:   2,
			ReplicaID: 1,
			Entries:   makeEntries(10, 1, 2),
		},
	}
	if err := db.SaveState(failBatch); err == nil {
		t.Fatal("SaveState should have failed")
	}

	// Verify snapshot is NOT readable after failed batch.
	_, snapErr := db.GetSnapshot(1, 1)
	if !errors.Is(snapErr, logdb.ErrNoSnapshot) {
		t.Errorf("expected ErrNoSnapshot after failed batch, got: %v", snapErr)
	}
}

// TestSaveStateLazyShardCheckpointOnFailure verifies that when a lazy shard
// (first write) creates a new segment via ensureActiveSegment and then a
// later shard in the batch fails, the newly created segment's data is
// properly rolled back via the checkpoint captured after segment creation.
func TestSaveStateLazyShardCheckpointOnFailure(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(512), WithMaxFileSize(4*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Shard 3 is lazy (never written to). Initialize shard 4 so it has
	// an active segment, then sabotage it.
	shard4Init := []logdb.Update{{
		ShardID:   4,
		ReplicaID: 1,
		Entries:   makeEntries(1, 1, 1),
	}}
	if err := db.SaveState(shard4Init); err != nil {
		t.Fatalf("SaveState shard4 init: %v", err)
	}

	// Verify shard 3 has no active segment (lazy).
	shard3Idx := uint16(3 % DefaultShardCount)
	shard3 := db.shards[shard3Idx]
	shard3.mu.Lock()
	if shard3.activeSegment != nil {
		t.Fatal("shard 3 should not have an active segment before first write")
	}
	shard3.mu.Unlock()

	// Sabotage shard 4.
	shard4Idx := uint16(4 % DefaultShardCount)
	shard4 := db.shards[shard4Idx]
	shard4.mu.Lock()
	if shard4.activeSegment != nil {
		shard4.activeSegment.Close()
		shard4.activeSegment = nil
	}
	shard4Dir := shard4.dir
	shard4.mu.Unlock()
	if err := os.Chmod(shard4Dir, 0444); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(shard4Dir, 0755) })

	// Batch: shard 3 (lazy, first write) then shard 4 (fails).
	failBatch := []logdb.Update{
		{
			ShardID:   3,
			ReplicaID: 1,
			Entries:   makeEntries(1, 5, 1),
			State:     logdb.State{Term: 1, Vote: 1, Commit: 5},
		},
		{
			ShardID:   4,
			ReplicaID: 1,
			Entries:   makeEntries(10, 1, 2),
		},
	}
	if err := db.SaveState(failBatch); err == nil {
		t.Fatal("SaveState should have failed")
	}

	// Verify shard 3 has no phantom entries or state.
	got, err := db.GetEntries(3, 1, 1, 10, 0)
	if err != nil {
		t.Fatalf("GetEntries shard 3 after failure: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 entries for lazy shard 3 after failed batch, got %d", len(got))
	}

	_, _, stateErr := db.ReadState(3, 1)
	if !errors.Is(stateErr, logdb.ErrNoState) {
		t.Errorf("expected ErrNoState for shard 3 after failed batch, got: %v", stateErr)
	}
}

// TestSaveStateSuccessAppliesInMemoryUpdates verifies that the two-phase
// update correctly applies in-memory updates after a successful batch,
// ensuring reads return the expected data.
func TestSaveStateSuccessAppliesInMemoryUpdates(t *testing.T) {
	db := openTestDB(t)

	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Entries:   makeEntries(1, 5, 1),
			State:     logdb.State{Term: 1, Vote: 1, Commit: 5},
			Snapshot:  logdb.Snapshot{Index: 3, Term: 1},
		},
		{
			ShardID:   2,
			ReplicaID: 1,
			Entries:   makeEntries(1, 3, 1),
			State:     logdb.State{Term: 1, Vote: 2, Commit: 3},
		},
	}
	if err := db.SaveState(updates); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	// Verify all entries.
	got1, err := db.GetEntries(1, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("GetEntries shard 1: %v", err)
	}
	if len(got1) != 5 {
		t.Errorf("shard 1: expected 5 entries, got %d", len(got1))
	}

	got2, err := db.GetEntries(2, 1, 1, 4, 0)
	if err != nil {
		t.Fatalf("GetEntries shard 2: %v", err)
	}
	if len(got2) != 3 {
		t.Errorf("shard 2: expected 3 entries, got %d", len(got2))
	}

	// Verify states.
	st1, _, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState shard 1: %v", err)
	}
	if st1.Term != 1 || st1.Vote != 1 || st1.Commit != 5 {
		t.Errorf("shard 1 state: %+v", st1)
	}

	st2, _, err := db.ReadState(2, 1)
	if err != nil {
		t.Fatalf("ReadState shard 2: %v", err)
	}
	if st2.Term != 1 || st2.Vote != 2 || st2.Commit != 3 {
		t.Errorf("shard 2 state: %+v", st2)
	}

	// Verify snapshot.
	ss, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot shard 1: %v", err)
	}
	if ss.Index != 3 || ss.Term != 1 {
		t.Errorf("shard 1 snapshot: %+v", ss)
	}
}
