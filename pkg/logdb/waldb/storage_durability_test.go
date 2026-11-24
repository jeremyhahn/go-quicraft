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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestOpen_StatFailureMustNotSkipRecovery is a regression test. The
// shard-directory probe treated EVERY Stat error as "directory absent", so an
// EACCES/EIO/ELOOP (or a stale NFS handle) skipped recovery: the shard came up
// with no known segments and nextSegmentID 0, and the first write recreated
// 000000.log with O_TRUNC, physically erasing the recovered node's term, vote
// and entries.
func TestOpen_StatFailureMustNotSkipRecovery(t *testing.T) {
	dir := t.TempDir()
	ffs := &faultFS{FS: defaultFS()}

	db, err := Open(dir, WithFS(ffs), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const shardID, replicaID = uint64(1), uint64(1)
	want := logdb.State{Term: 7, Vote: 3, Commit: 2}
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID,
		State:   want,
		Entries: makeEntries(1, 2, 7),
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with Stat failing for this node's shard directory only.
	simulated := errors.New("simulated EACCES on shard directory stat")
	ffs.arm(func(f *faultFS) {
		f.failNames = map[string]bool{shardDirName(int(shardID % DefaultShardCount)): true}
		f.failStat = simulated
	})
	db2, err := Open(dir, WithFS(ffs), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err == nil {
		_ = db2.Close()
		t.Fatal("Open succeeded despite an undiagnosable Stat failure on an " +
			"existing shard directory; recovery was skipped and the next write " +
			"would O_TRUNC the surviving segment")
	}
	if !errors.Is(err, simulated) {
		t.Fatalf("Open error = %v, want the injected Stat error in the chain", err)
	}

	// With the fault cleared the shard must still recover intact.
	ffs.arm(func(f *faultFS) { f.failStat = nil })
	db3, err := Open(dir, WithFS(ffs), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("reopen after clearing the fault: %v", err)
	}
	defer db3.Close()

	got, _, err := db3.ReadState(shardID, replicaID)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if got != want {
		t.Fatalf("recovered state = %+v, want %+v", got, want)
	}
	entries, err := db3.GetEntries(shardID, replicaID, 1, 3, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("recovered %d entries, want 2", len(entries))
	}
}

// TestOpen_SyncsWholeCreatedDirectoryChain is a regression test. Open
// fsynced only the WAL root's immediate parent, so when MkdirAll created
// several levels the intermediate directory entries were never durable: a
// crash after an ACKED SaveState lost the entire tree and the node came back
// with no term and no vote — a double vote in the acked term.
func TestOpen_SyncsWholeCreatedDirectoryChain(t *testing.T) {
	const mount = "/mnt/crashfs-chain"
	// Only the mount point is durable; every level below it is created by Open.
	cfs := newCrashFS(mount)
	dir := filepath.Join(mount, "node", "data", "wal")

	db, err := Open(dir, WithFS(cfs), WithBlockSize(crashDurBlockSize),
		WithMaxFileSize(crashDurMaxFileSize))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const shardID, replicaID = uint64(1), uint64(1)
	want := logdb.State{Term: 7, Vote: 3, Commit: 2}
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID,
		State:   want,
		Entries: makeEntries(1, 2, 7),
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	cfs.Crash()

	db2, err := Open(dir, WithFS(cfs), WithBlockSize(crashDurBlockSize),
		WithMaxFileSize(crashDurMaxFileSize))
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer db2.Close()

	got, _, err := db2.ReadState(shardID, replicaID)
	if err != nil {
		t.Fatalf("ReadState after crash: %v (the acked hard state was lost "+
			"with an ancestor directory whose entry was never fsynced)", err)
	}
	if got != want {
		t.Fatalf("recovered state = %+v, want %+v", got, want)
	}
}

// TestRemoveEntriesTo_FloorIsMonotonic is a regression test. The
// compaction floor was assigned unconditionally, so a lower follow-up call
// (reachable through the caller-overridable snapshot overhead) lowered a floor
// whose entries were already gone. Reads between the old and new floor then
// saw a hard log hole (ErrLogGapDetected) instead of ErrCompacted, and the
// regressed floor was written to the WAL so it survived restart.
func TestRemoveEntriesTo_FloorIsMonotonic(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const shardID, replicaID = uint64(1), uint64(1)
	if err := db.SaveEntries(shardID, replicaID, makeEntries(1, 20, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.RemoveEntriesTo(shardID, replicaID, 15); err != nil {
		t.Fatalf("RemoveEntriesTo(15): %v", err)
	}
	if err := db.RemoveEntriesTo(shardID, replicaID, 5); err != nil {
		t.Fatalf("RemoveEntriesTo(5): %v", err)
	}

	floor, err := db.CompactedTo(shardID, replicaID)
	if err != nil {
		t.Fatalf("CompactedTo: %v", err)
	}
	if floor != 15 {
		t.Fatalf("CompactedTo = %d, want 15 (the floor must never go backwards)", floor)
	}
	if _, _, err := db.IterateEntries(nil, 0, shardID, replicaID, 10, 21, 0); !errors.Is(err, logdb.ErrCompacted) {
		t.Fatalf("IterateEntries(low=10) = %v, want ErrCompacted; a regressed "+
			"floor makes raft see a log hole instead of falling back to InstallSnapshot", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	floor, err = db2.CompactedTo(shardID, replicaID)
	if err != nil {
		t.Fatalf("CompactedTo after reopen: %v", err)
	}
	if floor != 15 {
		t.Fatalf("CompactedTo after reopen = %d, want 15 (a regressed floor was persisted)", floor)
	}
	if _, _, err := db2.IterateEntries(nil, 0, shardID, replicaID, 10, 21, 0); !errors.Is(err, logdb.ErrCompacted) {
		t.Fatalf("IterateEntries(low=10) after reopen = %v, want ErrCompacted", err)
	}
}

// failingCloseFile wraps a File and fails Close, modelling a close(2) that
// reports a deferred writeback error. It mirrors failingSyncFile.
type failingCloseFile struct {
	File
	closeErr error
}

func (f *failingCloseFile) Close() error { return f.closeErr }

// TestRollbackShard_RotationCloseFailureStillRollsBack is a regression
// test. When a mid-batch ErrSegmentFull rotation fsynced the checkpoint
// segment (making THIS batch's earlier records durable) and then failed to
// close it, rotateSegment dropped the segment handle. rollbackShard saw
// activeSegment == nil and returned nil without truncating anything, so the
// failed batch's fsynced records survived and replayed as committed data the
// caller was told had FAILED.
func TestRollbackShard_RotationCloseFailureStillRollsBack(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(8*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Shard IDs 1 and 17 hash to the same WAL shard, so one batch writes both
	// updates into the same segment file.
	const nodeA, nodeB, replicaID = uint64(1), uint64(17), uint64(1)
	if err := db.SaveEntries(nodeA, replicaID, []logdb.Entry{
		{Index: 1, Term: 1, Cmd: make([]byte, 4000)},
	}); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	simulated := errors.New("simulated EIO on segment close")
	shard := db.getShard(nodeA)
	shard.mu.Lock()
	shard.activeSegment.f = &failingCloseFile{File: shard.activeSegment.f, closeErr: simulated}
	shard.mu.Unlock()

	// Update A lands a small record; update B overflows the segment, so the
	// rotation fsyncs A's record and then fails to close the segment.
	err = db.SaveState([]logdb.Update{
		{ShardID: nodeA, ReplicaID: replicaID, Entries: makeEntries(2, 1, 1)},
		{ShardID: nodeB, ReplicaID: replicaID, Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: make([]byte, 4000)},
		}},
	})
	if err == nil {
		t.Fatal("SaveState: expected failure when the rotation's segment close fails")
	}
	_ = db.Close()

	db2, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(8*1024))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	if _, _, err := db2.IterateEntries(nil, 0, nodeA, replicaID, 2, 3, 0); err == nil {
		t.Fatal("entry 2 from the FAILED batch survived the rollback: rollbackShard " +
			"no-oped because the rotation left activeSegment nil, so the fsynced " +
			"ghost record replayed as committed data")
	}
	if _, _, err := db2.IterateEntries(nil, 0, nodeB, replicaID, 1, 2, 0); err == nil {
		t.Fatal("node B's entry from the FAILED batch survived the rollback")
	}
	// The pre-batch prefix must still be there.
	got, err := db2.GetEntries(nodeA, replicaID, 1, 2, 0)
	if err != nil {
		t.Fatalf("GetEntries(1): %v (rollback must not erase the pre-batch prefix)", err)
	}
	if len(got) != 1 || got[0].Index != 1 {
		t.Fatalf("recovered %+v, want exactly entry 1", got)
	}
}

// TestSaveState_ZeroIndexSnapshotIsNotPersisted is a regression test.
// waldb treated a snapshot as present when Index OR Term was non-zero, so a
// zero-index snapshot was written, reported by GetSnapshot as a snapshot at
// index 0, and pinned its segment against garbage collection forever. memdb
// keys presence on Index > 0; both must agree.
func TestSaveState_ZeroIndexSnapshotIsNotPersisted(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const shardID, replicaID = uint64(1), uint64(1)
	if err := db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		State:     logdb.State{Term: 5, Vote: 1, Commit: 0},
		Snapshot:  logdb.Snapshot{Index: 0, Term: 5},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	if _, err := db.GetSnapshot(shardID, replicaID); !errors.Is(err, logdb.ErrNoSnapshot) {
		t.Fatalf("GetSnapshot = %v, want ErrNoSnapshot; a zero-index snapshot "+
			"must not count as present", err)
	}

	shard := db.getShard(shardID)
	shard.mu.Lock()
	pinned := shard.metaSegments[nodeKey{ShardID: shardID, ReplicaID: replicaID}].hasSnapshot
	shard.mu.Unlock()
	if pinned {
		t.Fatal("a zero-index snapshot pinned its segment against garbage collection")
	}
}

// TestSaveSnapshot_RejectsOversizedMembership is a regression test. WAL
// replay rejects any membership map declaring more than maxMembershipMapEntries
// entries as corruption, but the write path accepted one: the record was
// fsynced and acked, and the node was then permanently unopenable.
func TestSaveSnapshot_RejectsOversizedMembership(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	addrs := make(map[uint64]string, maxMembershipMapEntries+1)
	for i := 0; i <= maxMembershipMapEntries; i++ {
		addrs[uint64(i)] = "a"
	}
	err = db.SaveSnapshot(1, 1, logdb.Snapshot{
		Index: 1, Term: 1,
		Membership: logdb.Membership{Addresses: addrs},
	})
	if !errors.Is(err, ErrMembershipTooLarge) {
		t.Fatalf("SaveSnapshot with %d addresses = %v, want ErrMembershipTooLarge; "+
			"replay rejects the record fatally, so accepting it durably wedges the node",
			len(addrs), err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024*1024))
	if err != nil {
		t.Fatalf("reopen: %v (an oversized membership must never reach the WAL)", err)
	}
	defer db2.Close()
}

// TestParseSegmentFilename_RequiresCanonicalForm is a regression test.
// Arbitrary-length digit strings parsed as valid segment IDs (and wrapped
// silently on uint64 overflow), so a stray file could be adopted as a segment
// or make an unrelated ID look live.
func TestParseSegmentFilename_RequiresCanonicalForm(t *testing.T) {
	// The canonical-width, short, seven-digit-padded and plain uint64-overflow
	// names are cases of TestParseSegmentFilename (compaction_test.go). The
	// cases here are the ones only this regression reaches: an ID that has
	// outgrown the canonical width, padding far beyond it, and an overflow
	// that a wrapping parser reads back as a small live ID.
	const sevenDigits = "1000000.log"
	if got, ok := parseSegmentFilename(sevenDigits); !ok || got != 1000000 {
		t.Errorf("parseSegmentFilename(%q) = %d, %v; want 1000000, true", sevenDigits, got, ok)
	}

	invalid := []string{
		"00000000000000000001.log",  // padded beyond the canonical width
		"184467440737095516151.log", // overflows uint64 (a wrapping parser reads 1)
	}
	for _, name := range invalid {
		if got, ok := parseSegmentFilename(name); ok {
			t.Errorf("parseSegmentFilename(%q) = %d, true; want rejected", name, got)
		}
	}
}

// TestDiscardPending_ReReservesSpace is a regression test. The
// non-rotation rollback path truncated the segment back to the checkpoint,
// dropping the whole fallocate reservation, and never re-reserved it — every
// subsequent write paid a file extension and the ENOSPC pre-reservation the
// design relies on was gone.
func TestDiscardPending_ReReservesSpace(t *testing.T) {
	dir := t.TempDir()
	const maxFileSize = 64 * 1024
	seg, _, err := createSegment(defaultFS(), dir, 0, 1024, maxFileSize)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	defer seg.Close()

	if _, err := seg.Write(make([]byte, 4096)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := seg.DiscardPending(0, 0, 0); err != nil {
		t.Fatalf("DiscardPending: %v", err)
	}

	fi, err := defaultFS().Stat(filepath.Join(dir, segmentFilename(0)))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != maxFileSize {
		t.Fatalf("segment size after DiscardPending = %d, want %d "+
			"(the fallocate reservation was dropped and never re-taken)", fi.Size(), maxFileSize)
	}
}

// failingWriteAtFile wraps a File and fails WriteAt after failAfter successful
// calls, so a record that spans several blocks fails midway with its earlier
// blocks already on disk. It mirrors failingSyncFile.
type failingWriteAtFile struct {
	File
	writeErr  error
	failAfter int
	calls     int
}

func (f *failingWriteAtFile) WriteAt(p []byte, off int64) (int, error) {
	f.calls++
	if f.calls > f.failAfter {
		return 0, f.writeErr
	}
	return f.File.WriteAt(p, off)
}

// TestSaveSnapshot_RollsBackPartialRecord is a regression test.
// SaveSnapshot captured a checkpoint and never used it, so a record that
// failed midway left its already-flushed blocks in the segment even though the
// caller was told the write failed.
func TestSaveSnapshot_RollsBackPartialRecord(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(1024*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const shardID, replicaID = uint64(1), uint64(1)
	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{Index: 1, Term: 1}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	shard := db.getShard(shardID)
	shard.mu.Lock()
	sizeBefore := shard.activeSegment.Size()
	simulated := errors.New("simulated EIO on block flush")
	shard.activeSegment.f = &failingWriteAtFile{
		File: shard.activeSegment.f, writeErr: simulated, failAfter: 1,
	}
	shard.mu.Unlock()

	// A membership large enough to span several blocks, so the first block
	// seal succeeds and the next one fails.
	addrs := make(map[uint64]string, 200)
	for i := 0; i < 200; i++ {
		addrs[uint64(i)] = "replica-address-0000000000000000000000000000"
	}
	err = db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index: 2, Term: 1,
		Membership: logdb.Membership{Addresses: addrs},
	})
	if !errors.Is(err, simulated) {
		t.Fatalf("SaveSnapshot = %v, want the injected write error", err)
	}

	shard.mu.Lock()
	sizeAfter := shard.activeSegment.Size()
	shard.mu.Unlock()
	if sizeAfter != sizeBefore {
		t.Fatalf("segment size %d after the failed write, want %d: the partial "+
			"record was left behind instead of being rolled back", sizeAfter, sizeBefore)
	}

	if ss, err := db.GetSnapshot(shardID, replicaID); err != nil || ss.Index != 1 {
		t.Fatalf("GetSnapshot = %+v, %v; want the pre-failure snapshot at index 1", ss, err)
	}
}

// TestSaveBootstrap_RollsBackPartialRecord is the SaveBootstrap counterpart of
// TestSaveSnapshot_RollsBackPartialRecord.
//
// SaveBootstrap was the ONE standalone writer with no checkpoint at all —
// SaveSnapshot, RemoveEntriesTo and RemoveNodeData all take one. A record that
// failed midway therefore left its already-flushed blocks in the segment while
// the caller was told the write failed, and nothing ever removed them. The next
// successful write appends directly on top, so what recovery finds is a torn
// record with valid data above it in the middle of a live segment: the shape it
// must fail closed on. One failed SaveBootstrap could cost the whole WAL shard
// on the next restart.
func TestSaveBootstrap_RollsBackPartialRecord(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(1024*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const shardID, replicaID = uint64(1), uint64(1)
	if err := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
		Addresses: map[uint64]string{1: "node1:9000"},
		Type:      3,
	}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}

	shard := db.getShard(shardID)
	shard.mu.Lock()
	sizeBefore := shard.activeSegment.Size()
	simulated := errors.New("simulated EIO on block flush")
	shard.activeSegment.f = &failingWriteAtFile{
		File: shard.activeSegment.f, writeErr: simulated, failAfter: 1,
	}
	shard.mu.Unlock()

	// An address map large enough to span several blocks, so the first block
	// seal succeeds and the next one fails.
	addrs := make(map[uint64]string, 200)
	for i := 0; i < 200; i++ {
		addrs[uint64(i)] = "replica-address-0000000000000000000000000000"
	}
	err = db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{Addresses: addrs, Type: 3})
	if !errors.Is(err, simulated) {
		t.Fatalf("SaveBootstrap = %v, want the injected write error", err)
	}

	shard.mu.Lock()
	sizeAfter := shard.activeSegment.Size()
	shard.mu.Unlock()
	if sizeAfter != sizeBefore {
		t.Fatalf("segment size %d after the failed write, want %d: the partial "+
			"bootstrap record was left behind instead of being rolled back, and the "+
			"next successful write will bury it under valid data",
			sizeAfter, sizeBefore)
	}

	bs, ok, err := db.GetBootstrap(shardID, replicaID)
	if err != nil || !ok {
		t.Fatalf("GetBootstrap = ok %v err %v; want the pre-failure record", ok, err)
	}
	if len(bs.Addresses) != 1 || bs.Addresses[1] != "node1:9000" {
		t.Fatalf("GetBootstrap addresses = %v, want the pre-failure record", bs.Addresses)
	}

	// Restore a working file, write on top, and prove recovery reads the whole
	// segment back cleanly — the rolled-back bytes must not be sitting under it.
	shard.mu.Lock()
	shard.activeSegment.f = shard.activeSegment.f.(*failingWriteAtFile).File
	shard.mu.Unlock()

	if err := db.SaveEntries(shardID, replicaID, makeEntries(1, 2, 1)); err != nil {
		t.Fatalf("SaveEntries after the rolled-back bootstrap: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(1024*1024))
	if err != nil {
		t.Fatalf("reopen after a rolled-back bootstrap write: %v (the partial "+
			"record was buried under valid data and recovery had to fail closed)", err)
	}
	defer db2.Close()
	got, err := db2.GetEntries(shardID, replicaID, 1, 3, 0)
	if err != nil {
		t.Fatalf("GetEntries after reopen: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("recovered %d entries, want 2", len(got))
	}
}

// TestSaveSnapshot_RejectsZeroIndex pins the fresh-node hole in the staleness
// rule on the DIRECT snapshot path.
//
// TestSaveState_ZeroIndexSnapshotIsNotPersisted covers the batch path, which
// has always required Index > 0. SaveSnapshot only compared against a STORED
// snapshot, so on a node that had none an index-0 snapshot went straight
// through: it wrote a WAL record, made GetSnapshot report a snapshot existing
// at no index, and pinned the segment it landed in against garbage collection
// until a real snapshot replaced it.
func TestSaveSnapshot_RejectsZeroIndex(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const shardID, replicaID = uint64(1), uint64(1)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{Index: 0, Term: 5}); !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
		t.Fatalf("SaveSnapshot(Index=0) on a fresh node = %v, want ErrSnapshotOutOfDate", err)
	}
	if _, err := db.GetSnapshot(shardID, replicaID); !errors.Is(err, logdb.ErrNoSnapshot) {
		t.Fatalf("GetSnapshot after a rejected index-0 snapshot = %v, want "+
			"ErrNoSnapshot: a phantom snapshot at index 0 was published, and a "+
			"caller reads that as a recovery point this replica does not have", err)
	}

	// No record was written, so no segment is pinned by a snapshot that does
	// not exist.
	shard := db.getShard(shardID)
	shard.dataMu.RLock()
	pinned := shard.metaSegments[key].hasSnapshot
	shard.dataMu.RUnlock()
	if pinned {
		t.Fatal("an index-0 snapshot pinned a segment against garbage collection: " +
			"the pin outlives every entry in that segment and is only released by " +
			"a real snapshot")
	}

	// A real snapshot still lands, and index 0 stays rejected afterwards.
	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{Index: 10, Term: 5}); err != nil {
		t.Fatalf("SaveSnapshot(Index=10): %v", err)
	}
	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{Index: 0}); !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
		t.Fatalf("SaveSnapshot(Index=0) over a stored snapshot = %v, want ErrSnapshotOutOfDate", err)
	}
}

// TestOpen_FailedOpenDoesNotAccumulateEmptySegments covers closeAfterFailedOpen,
// which had no test of any kind: deleting its removal, and deleting the
// emptiness guard that protects it, both left the whole suite green.
//
// Every Open creates a fresh active segment for each shard whose directory
// already exists. When a LATER shard then fails the open, the segments the
// earlier shards just created are brand-new empty files nobody will ever read.
// Working through a fail-closed condition means opening repeatedly, so without
// the cleanup each attempt leaves one dead file per shard behind — and those
// files are not inert: recover() advances nextSegmentID past every segment it
// observes, so they permanently inflate the ID space of a database that is
// already in trouble.
//
// The directory fsync at the end of that cleanup is deliberately NOT asserted
// here. It is best-effort by construction — its failure is logged at Debug and
// swallowed — and correctly so: the file it makes durable is EMPTY, the open
// failed, and a crash that resurrects the dirent costs one stray empty segment
// and nothing else. Pinning it would test that a call happens, not that
// anything is true.
func TestOpen_FailedOpenDoesNotAccumulateEmptySegments(t *testing.T) {
	dir := t.TempDir()
	// Shard IDs 1 and 2 hash to WAL shards 1 and 2, and Open walks shards in
	// ascending index order, so shard 1 is opened (and creates its new segment)
	// before shard 2 fails.
	const first, second, replicaID = uint64(1), uint64(2), uint64(1)

	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveEntries(first, replicaID, makeEntries(1, 2, 1)); err != nil {
		t.Fatalf("SaveEntries(first): %v", err)
	}
	if err := db.SaveEntries(second, replicaID, makeEntries(1, 2, 1)); err != nil {
		t.Fatalf("SaveEntries(second): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	firstDir := filepath.Join(dir, shardDirName(int(first%DefaultShardCount)))
	secondDir := filepath.Join(dir, shardDirName(int(second%DefaultShardCount)))

	// Sabotage the SECOND shard's next segment: a directory at the path
	// createSegment will open with O_RDWR is EISDIR for every euid, root
	// included.
	blocked := filepath.Join(secondDir, segmentFilename(1))
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatalf("sabotage: %v", err)
	}

	countSegments := func(d string) int {
		t.Helper()
		des, err := os.ReadDir(d)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", d, err)
		}
		n := 0
		for _, de := range des {
			if de.IsDir() {
				continue
			}
			if _, ok := parseSegmentFilename(de.Name()); ok {
				n++
			}
		}
		return n
	}

	if got := countSegments(firstDir); got != 1 {
		t.Fatalf("fixture: shard %s holds %d segments before the failed opens, want 1",
			firstDir, got)
	}

	// Repeatedly fail to open, exactly as an operator working through a
	// fail-closed condition would.
	const attempts = 4
	for i := 0; i < attempts; i++ {
		bad, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
		if err == nil {
			bad.Close()
			t.Fatalf("attempt %d: Open succeeded despite the sabotaged segment path", i)
		}
		if got := countSegments(firstDir); got != 1 {
			t.Fatalf("after %d failed open(s) the earlier shard holds %d segment "+
				"files, want 1: each failed open left behind the empty segment it "+
				"created, and recover() will advance nextSegmentID past every one "+
				"of them on the next successful open", i+1, got)
		}
	}

	// The populated segment must be untouched — the cleanup removes only the
	// empty file this open created, never one holding records.
	if err := os.Remove(blocked); err != nil {
		t.Fatalf("un-sabotage: %v", err)
	}
	good, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open after removing the sabotage: %v", err)
	}
	defer good.Close()
	for _, sid := range []uint64{first, second} {
		got, err := good.GetEntries(sid, replicaID, 1, 3, 0)
		if err != nil {
			t.Fatalf("GetEntries(shard %d): %v", sid, err)
		}
		if len(got) != 2 {
			t.Fatalf("shard %d recovered %d entries after %d failed opens, want 2: "+
				"the failed-open cleanup deleted a segment that held records",
				sid, len(got), attempts)
		}
	}
}

// TestRotateSegment_ClosingSyncFailurePoisons pins the fail-stop on the
// closing fdatasync segment.Close issues: it is what puts the segment's last
// sync mark on the medium, and after a failed fdatasync nothing about the
// file can be established, so rotateSegment poisons the DB on it exactly as
// it does for a failed Sync. The first Sync of the rotation (the one that
// acknowledges the records and appends the mark) is allowed through; the
// closing one fails.
func TestRotateSegment_ClosingSyncFailurePoisons(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(8*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const nodeA, replicaID = uint64(1), uint64(1)
	if err := db.SaveEntries(nodeA, replicaID, []logdb.Entry{
		{Index: 1, Term: 1, Cmd: make([]byte, 4000)},
	}); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	simulated := errors.New("simulated EIO on the closing fdatasync")
	shard := db.getShard(nodeA)
	shard.mu.Lock()
	shard.activeSegment.f = &failingSyncFile{File: shard.activeSegment.f, syncErr: simulated, failAfter: 1}
	shard.mu.Unlock()

	// Overflow the segment: rotation's Sync succeeds, Close's fdatasync fails.
	err = db.SaveEntries(nodeA, replicaID, []logdb.Entry{
		{Index: 2, Term: 1, Cmd: make([]byte, 4000)},
	})
	if err == nil {
		t.Fatal("SaveEntries: expected failure when the rotation's closing fdatasync fails")
	}
	if !errors.Is(err, errSegmentCloseSync) || !errors.Is(err, simulated) {
		t.Fatalf("SaveEntries error = %v, want errSegmentCloseSync wrapping the injected failure", err)
	}
	if !db.poisoned.Load() {
		t.Fatal("DB not poisoned after the closing fdatasync failed; a Sync failure by another name must fail-stop")
	}
	if err := db.SaveEntries(nodeA, replicaID, makeEntries(3, 1, 1)); !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("SaveEntries after poison = %v, want ErrDBPoisoned", err)
	}
}
