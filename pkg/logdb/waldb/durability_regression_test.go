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
	"bytes"
	"errors"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// faultFS wraps an FS and injects errors into Remove, OpenFile and/or Stat
// for files whose base name is in failNames (or for all files when failAll is
// set). Arm/disarm at any point via the mutex-protected fields.
type faultFS struct {
	FS
	mu           sync.Mutex
	failAll      bool
	failNames    map[string]bool
	failRemove   error
	failOpenFile error
	// failStat injects an error that is NOT fs.ErrNotExist, the case a
	// presence probe must never read as absence.
	failStat error
}

func (f *faultFS) matches(name string) bool {
	return f.failAll || f.failNames[filepath.Base(name)]
}

func (f *faultFS) Remove(name string) error {
	f.mu.Lock()
	injected := f.failRemove
	match := f.matches(name)
	f.mu.Unlock()
	if injected != nil && match {
		return injected
	}
	return f.FS.Remove(name)
}

func (f *faultFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	f.mu.Lock()
	injected := f.failOpenFile
	match := f.matches(name)
	f.mu.Unlock()
	if injected != nil && match {
		return nil, injected
	}
	return f.FS.OpenFile(name, flag, perm)
}

func (f *faultFS) Stat(name string) (fs.FileInfo, error) {
	f.mu.Lock()
	injected := f.failStat
	match := f.matches(name)
	f.mu.Unlock()
	if injected != nil && match {
		return nil, injected
	}
	return f.FS.Stat(name)
}

// arm sets the fault configuration under the mutex.
func (f *faultFS) arm(configure func(*faultFS)) {
	f.mu.Lock()
	configure(f)
	f.mu.Unlock()
}

// rotateShard forces a segment rotation on the WAL shard serving shardID.
func rotateShard(t *testing.T, db *DB, shardID uint64) {
	t.Helper()
	shard := db.getShard(shardID)
	shard.mu.Lock()
	err := shard.rotateSegment()
	shard.mu.Unlock()
	if err != nil {
		t.Fatalf("rotateSegment: %v", err)
	}
}

// segmentPath returns the path of a segment file in the WAL shard directory
// serving shardID.
func segmentPath(db *DB, shardID, segID uint64) string {
	return filepath.Join(db.getShard(shardID).dir, segmentFilename(segID))
}

// TestRemoveNodeData_GCFailureKeepsEpochFloor is the H1 regression test for
// the RemoveNodeData path. When garbage collection fails, the segments it
// intended to delete remain on disk, so the minWriteEpoch floor must NOT rise
// past an epoch those surviving segments still need — otherwise the engine
// purges the DEK and the next restart dies with a fatal WALDecryptError.
func TestRemoveNodeData_GCFailureKeepsEpochFloor(t *testing.T) {
	barrier := newTestBarrier(t)
	ffs := &faultFS{FS: defaultFS()}
	dir := t.TempDir()
	db, err := Open(dir,
		WithFS(ffs),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Shard IDs 1 and 17 hash to the same WAL shard, so node A's and node
	// B's records share segment files.
	const nodeA, nodeB = uint64(1), uint64(17)

	// Node A writes at epoch 1; its records land in segment 0.
	if err := db.SaveEntries(nodeA, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries A: %v", err)
	}
	rotateShard(t, db, nodeA)

	// Rotate the barrier key so node B writes at epoch 2 into segment 1.
	if _, err := barrier.Rotate(); err != nil {
		t.Fatalf("barrier.Rotate: %v", err)
	}
	if err := db.SaveEntries(nodeB, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries B: %v", err)
	}

	if epoch, err := db.MinLiveEpoch(); err != nil || epoch != 1 {
		t.Fatalf("MinLiveEpoch before removal = %d, %v; want 1, nil", epoch, err)
	}

	// Every Remove fails: GC cannot delete segment 0, which still holds
	// node A's epoch-1 records on disk.
	ffs.arm(func(f *faultFS) {
		f.failAll = true
		f.failRemove = errors.New("simulated EIO on unlink")
	})
	if err := db.RemoveNodeData(nodeA, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}

	// Segment 0 must still be on disk.
	if _, err := ffs.Stat(segmentPath(db, nodeA, 0)); err != nil {
		t.Fatalf("segment 0 unexpectedly gone: %v", err)
	}

	// The epoch floor must still cover the surviving on-disk segment.
	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("MinLiveEpoch after failed GC = %d, want 1 "+
			"(floor rose past an epoch still needed by an on-disk segment)", epoch)
	}
}

// TestCompact_PartialGCFailureKeepsEpochFloor is the H1 regression test for
// the Compact path. GC deletes files one at a time; when it aborts partway
// with deleted > 0, the not-yet-deleted segments survive on disk and the
// epoch floor must NOT rise past their epochs.
func TestCompact_PartialGCFailureKeepsEpochFloor(t *testing.T) {
	barrier := newTestBarrier(t)
	ffs := &faultFS{FS: defaultFS()}
	dir := t.TempDir()
	db, err := Open(dir,
		WithFS(ffs),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const nodeA, nodeB = uint64(1), uint64(17)

	// Node A fills segments 0 and 1 at epoch 1.
	if err := db.SaveEntries(nodeA, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries A seg0: %v", err)
	}
	rotateShard(t, db, nodeA)
	if err := db.SaveEntries(nodeA, 1, makeEntries(11, 10, 1)); err != nil {
		t.Fatalf("SaveEntries A seg1: %v", err)
	}
	rotateShard(t, db, nodeA)

	// Node B writes at epoch 2 into segment 2 (the active segment).
	if _, err := barrier.Rotate(); err != nil {
		t.Fatalf("barrier.Rotate: %v", err)
	}
	if err := db.SaveEntries(nodeB, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries B: %v", err)
	}

	// Compact node A's entries away so segments 0 and 1 become dead.
	if err := db.RemoveEntriesTo(nodeA, 1, 20); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	// GC visits files in ascending name order: segment 0 deletes fine,
	// segment 1 fails — a partial GC with deleted > 0.
	ffs.arm(func(f *faultFS) {
		f.failNames = map[string]bool{segmentFilename(1): true}
		f.failRemove = errors.New("simulated EIO on unlink")
	})
	compactErr := db.Compact(nodeA, 0)
	if compactErr == nil {
		t.Fatal("Compact: expected GC delete error, got nil")
	}
	var delErr *CompactionDeleteError
	if !errors.As(compactErr, &delErr) {
		t.Fatalf("Compact error = %v, want CompactionDeleteError", compactErr)
	}

	// Segment 1 survived on disk with epoch-1 records; segment 0 is gone.
	if _, err := ffs.Stat(segmentPath(db, nodeA, 1)); err != nil {
		t.Fatalf("segment 1 unexpectedly gone: %v", err)
	}
	if _, err := ffs.Stat(segmentPath(db, nodeA, 0)); err == nil {
		t.Fatal("segment 0 should have been deleted by GC")
	}

	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("MinLiveEpoch after partial GC = %d, want 1 "+
			"(floor rose past an epoch still needed by an on-disk segment)", epoch)
	}
}

// TestRemoveNodeData_TombstonePinnedAgainstGC is the H2 regression test. The
// RemoveNode tombstone's segment must be pinned against garbage collection:
// WAL shards are shared by many Raft groups, so OLDER segments holding the
// removed node's records can stay alive (pinned by other nodes) after the
// tombstone's own segment becomes otherwise dead. If GC deletes the tombstone
// while those older segments survive, replay resurrects the removed replica —
// a membership-safety violation.
func TestRemoveNodeData_TombstonePinnedAgainstGC(t *testing.T) {
	dir := t.TempDir()
	open := func() *DB {
		db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}

	const nodeA, nodeB = uint64(1), uint64(17)
	keyA := nodeKey{ShardID: nodeA, ReplicaID: 1}

	db := open()

	// Segment 0: node A's records plus node B's records. Node B's entries
	// keep segment 0 alive after node A is removed.
	if err := db.SaveEntries(nodeA, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries A: %v", err)
	}
	if err := db.SaveEntries(nodeB, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries B seg0: %v", err)
	}
	rotateShard(t, db, nodeA)

	// Segment 1: the RemoveNode tombstone for node A.
	if err := db.RemoveNodeData(nodeA, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}
	rotateShard(t, db, nodeA)

	// Segment 2: more node B records, advancing the commit high-water mark
	// past the tombstone's segment so GC would consider it.
	if err := db.SaveEntries(nodeB, 1, makeEntries(4, 3, 1)); err != nil {
		t.Fatalf("SaveEntries B seg2: %v", err)
	}

	// GC: segment 0 is alive via node B, segment 2 is alive and active.
	// Segment 1 holds only the tombstone — it must survive via the pin.
	if err := db.Compact(nodeB, 0); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if _, err := defaultFS().Stat(segmentPath(db, nodeA, 1)); err != nil {
		t.Fatalf("tombstone segment deleted by GC: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After reopen, node A must stay removed even though segment 0 still
	// holds its pre-removal records.
	db2 := open()
	defer db2.Close()

	infos, err := db2.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo: %v", err)
	}
	var sawA, sawB bool
	for _, info := range infos {
		if info.ShardID == nodeA && info.ReplicaID == 1 {
			sawA = true
		}
		if info.ShardID == nodeB && info.ReplicaID == 1 {
			sawB = true
		}
	}
	if sawA {
		t.Fatal("removed node A resurrected after reopen (tombstone lost to GC)")
	}
	if !sawB {
		t.Fatal("surviving node B missing after reopen")
	}

	// The tombstone pin must be restored by recovery so a post-recovery GC
	// cannot reclaim it either.
	shard := db2.getShard(nodeA)
	shard.mu.Lock()
	m, ok := shard.metaSegments[keyA]
	pinned := ok && m.hasRemove
	shard.mu.Unlock()
	if !pinned {
		t.Fatal("tombstone segment not re-pinned after recovery")
	}
}

// writeMultiBlockSegment populates segment 0 of the WAL shard serving shardID
// with enough records to span several blocks, so a mid-file block can be
// zeroed to simulate a write hole.
func writeMultiBlockSegment(t *testing.T, db *DB, shardID uint64) {
	t.Helper()
	for i := uint64(0); i < 15; i++ {
		e := logdb.Entry{
			Index: i + 1,
			Term:  1,
			Cmd:   bytes.Repeat([]byte{byte(i + 1)}, 200),
		}
		if err := db.SaveEntries(shardID, 1, []logdb.Entry{e}); err != nil {
			t.Fatalf("SaveEntries %d: %v", i, err)
		}
	}
}

// corruptSegmentBlock zeroes one whole block of a segment file, simulating a
// write hole that halts replay at that segment.
func corruptSegmentBlock(t *testing.T, path string, blockSize int) {
	t.Helper()
	rw, err := defaultFS().OpenFile(path, 2 /* os.O_RDWR */, 0600)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	if _, err := rw.WriteAt(make([]byte, blockSize), int64(blockSize)); err != nil {
		t.Fatalf("corrupting write: %v", err)
	}
	if err := rw.Sync(); err != nil {
		t.Fatalf("sync corruption: %v", err)
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestRecovery_OrphanRemoveFailureTruncatesAndAdvancesIDs is the H3
// regression test. When recovery truncates at a corrupt segment and the
// removal of a higher orphan segment fails, the orphan must be truncated to
// zero length (so it replays to nothing), and nextSegmentID must be advanced
// past every observed segment so new records can never reuse an orphan's ID
// range — a lingering orphan ordered after fresh segments would replay LAST
// and regress HardState term/vote.
func TestRecovery_OrphanRemoveFailureTruncatesAndAdvancesIDs(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeMultiBlockSegment(t, db, shardID)
	rotateShard(t, db, shardID)
	if err := db.SaveNodeState(shardID, 1, logdb.State{Term: 7, Vote: 2, Commit: 1}); err != nil {
		t.Fatalf("SaveNodeState seg1: %v", err)
	}
	rotateShard(t, db, shardID)
	if err := db.SaveNodeState(shardID, 1, logdb.State{Term: 9, Vote: 3, Commit: 1}); err != nil {
		t.Fatalf("SaveNodeState seg2: %v", err)
	}
	seg1Path := segmentPath(db, shardID, 1)
	seg2Path := segmentPath(db, shardID, 2)
	seg0Path := segmentPath(db, shardID, 0)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt segment 0 mid-file: recovery truncates there and segments 1-2
	// become unreachable orphans.
	corruptSegmentBlock(t, seg0Path, blockSize)

	// Orphan removal fails; the truncate-to-zero fallback must succeed.
	ffs := &faultFS{FS: defaultFS()}
	ffs.arm(func(f *faultFS) {
		f.failNames = map[string]bool{
			segmentFilename(1): true,
			segmentFilename(2): true,
		}
		f.failRemove = errors.New("simulated EBUSY on unlink")
	})
	db2, err := Open(dir, WithFS(ffs), WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open after corruption with failing Remove: %v", err)
	}
	defer db2.Close()

	// The orphans must be empty on disk so they can never replay.
	for _, p := range []string{seg1Path, seg2Path} {
		fi, err := defaultFS().Stat(p)
		if err != nil {
			t.Fatalf("Stat %s: %v", p, err)
		}
		if fi.Size() != 0 {
			t.Fatalf("orphan %s not truncated: size=%d, want 0", p, fi.Size())
		}
	}

	// The orphaned HardState must not have been replayed.
	if _, err := db2.GetState(shardID, 1); err == nil {
		st, _ := db2.GetState(shardID, 1)
		if st.Term != 0 {
			t.Fatalf("orphaned HardState replayed: %+v", st)
		}
	}

	// New segments must be ordered after every observed segment ID, so even
	// a resurrected orphan could never replay after fresh records.
	shard := db2.getShard(shardID)
	shard.mu.Lock()
	nextID := shard.nextSegmentID
	activeID := shard.activeSegmentID
	shard.mu.Unlock()
	if activeID <= 2 {
		t.Fatalf("active segment ID = %d, want > 2 (must not reuse orphan ID range)", activeID)
	}
	if nextID <= 3 {
		t.Fatalf("nextSegmentID = %d, want > 3", nextID)
	}
}

// TestRecovery_OrphanDiscardTotalFailureIsFatal verifies the H3 fail-stop
// rule: when an orphan segment can be neither removed nor truncated to zero,
// recovery must fail with a RecoveryError instead of leaving an intact orphan
// whose stale records would replay on a later restart.
func TestRecovery_OrphanDiscardTotalFailureIsFatal(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeMultiBlockSegment(t, db, shardID)
	rotateShard(t, db, shardID)
	if err := db.SaveNodeState(shardID, 1, logdb.State{Term: 7, Vote: 2, Commit: 1}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	seg0Path := segmentPath(db, shardID, 0)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	corruptSegmentBlock(t, seg0Path, blockSize)

	ffs := &faultFS{FS: defaultFS()}
	ffs.arm(func(f *faultFS) {
		f.failNames = map[string]bool{segmentFilename(1): true}
		f.failRemove = errors.New("simulated EBUSY on unlink")
		f.failOpenFile = errors.New("simulated EIO on open")
	})
	_, err = Open(dir, WithFS(ffs), WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err == nil {
		t.Fatal("Open: expected fatal RecoveryError when orphan discard fails entirely")
	}
	var recErr *RecoveryError
	if !errors.As(err, &recErr) {
		t.Fatalf("Open error = %v, want RecoveryError", err)
	}
	if recErr.SegmentID != 1 {
		t.Fatalf("RecoveryError.SegmentID = %d, want 1", recErr.SegmentID)
	}
}

// dirSyncFailFS fails Sync on the file handle opened for one specific
// directory path, simulating a directory-fsync failure while file I/O keeps
// working.
type dirSyncFailFS struct {
	FS
	mu      sync.Mutex
	failDir string
	syncErr error
}

func (f *dirSyncFailFS) Open(name string) (File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	fail := f.failDir != "" && name == f.failDir
	syncErr := f.syncErr
	f.mu.Unlock()
	if fail {
		return &failingSyncFile{File: file, syncErr: syncErr}, nil
	}
	return file, nil
}

// TestRollback_DirSyncFailurePoisonsDB is the H4 regression test. When a
// mid-batch rotation rollback deletes/truncates segments but the directory
// fsync fails, the rollback cannot be proven durable: a crash could resurrect
// a deleted segment's directory entry and recovery would replay CRC-valid
// records from a batch that was reported failed. The failure must fail-stop
// the DB (poison + ErrDBPoisoned), exactly like a data-fsync failure.
func TestRollback_DirSyncFailurePoisonsDB(t *testing.T) {
	dir := t.TempDir()
	ffs := &dirSyncFailFS{FS: defaultFS()}
	db, err := Open(dir, WithFS(ffs), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const shardID = uint64(1)
	if err := db.SaveEntries(shardID, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	shard := db.getShard(shardID)
	shard.mu.Lock()

	// Capture a pre-batch checkpoint against the current active segment,
	// then rotate — reproducing a batch whose write rotated segments before
	// failing, which forces rollbackShard down the multi-segment path.
	if err := shard.activeSegment.flushBlock(); err != nil {
		shard.mu.Unlock()
		t.Fatalf("flushBlock: %v", err)
	}
	cp := shardCheckpoint{
		fileOffset:  shard.activeSegment.fileOffset,
		blockOffset: shard.activeSegment.blockOffset,
		seg:         shard.activeSegment,
		segID:       shard.activeSegmentID,
		valid:       true,
	}
	if err := shard.rotateSegment(); err != nil {
		shard.mu.Unlock()
		t.Fatalf("rotateSegment: %v", err)
	}

	// Fail the rollback's directory fsync only.
	simulated := errors.New("simulated EIO on directory fsync")
	ffs.mu.Lock()
	ffs.failDir = shard.dir
	ffs.syncErr = simulated
	ffs.mu.Unlock()

	rbErr := rollbackShard(shard, &cp)
	shard.mu.Unlock()

	if rbErr == nil {
		t.Fatal("rollbackShard: expected error when directory fsync fails")
	}
	if !errors.Is(rbErr, ErrDBPoisoned) {
		t.Fatalf("rollbackShard error = %v, want ErrDBPoisoned in chain", rbErr)
	}
	if !errors.Is(rbErr, simulated) {
		t.Fatalf("rollbackShard error = %v, want underlying cause in chain", rbErr)
	}
	if !db.poisoned.Load() {
		t.Fatal("DB must be poisoned after rollback directory-fsync failure")
	}

	// The fail-stop must surface through SaveState's error chain: a
	// BatchWriteError carrying this rollback failure exposes ErrDBPoisoned
	// via errors.Is.
	batchErr := &BatchWriteError{
		WriteErr:      errors.New("original write failure"),
		DiscardErrors: []error{rbErr},
	}
	if !errors.Is(batchErr, ErrDBPoisoned) {
		t.Fatal("BatchWriteError must expose ErrDBPoisoned from discard errors")
	}

	// Every subsequent write is refused.
	ffs.mu.Lock()
	ffs.failDir = ""
	ffs.mu.Unlock()
	if err := db.SaveEntries(shardID, 1, makeEntries(4, 1, 1)); !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("SaveEntries after poison = %v, want ErrDBPoisoned", err)
	}
}

// TestRecovery_CorruptSegmentRepairedSoLaterWritesSurvive is the regression
// test for the recurring truncation-recovery data loss. Before the repair,
// recover() reported errRecoveryTruncated and discarded the segments ABOVE the
// corrupt one but never truncated the corrupt segment itself, so its torn tail
// was frozen on disk forever: restart 1 recovers the prefix and the node runs
// for hours writing new segments whose entries are acked to Raft as committed;
// restart 2 re-detects the same corruption and DELETES EVERY SEGMENT WRITTEN
// SINCE RESTART 1 — committed entries, hard state (term/vote regression, i.e. a
// double vote), snapshot metadata and compaction markers — recurring forever.
func TestRecovery_CorruptSegmentRepairedSoLaterWritesSurvive(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeMultiBlockSegment(t, db, shardID)
	seg0Path := segmentPath(db, shardID, 0)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sizeBefore := statSize(t, seg0Path)
	corruptSegmentBlock(t, seg0Path, blockSize)

	// Restart 1: recovery halts at the corrupt block, replaying the clean
	// prefix, and must repair the segment on disk.
	db2, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open after corruption: %v", err)
	}
	recovered, _, err := db2.EntryRange(shardID, 1)
	if err != nil {
		t.Fatalf("EntryRange after restart 1: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("EntryRange first index = %d, want 1", recovered)
	}

	// The node now runs and acknowledges new writes as committed.
	if err := db2.SaveNodeState(shardID, 1, logdb.State{Term: 11, Vote: 4, Commit: 3}); err != nil {
		t.Fatalf("SaveNodeState after restart 1: %v", err)
	}
	if err := db2.SaveSnapshot(shardID, 1, logdb.Snapshot{Index: 3, Term: 11}); err != nil {
		t.Fatalf("SaveSnapshot after restart 1: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close after restart 1: %v", err)
	}

	// Restart 2: the repaired segment must replay cleanly, so nothing written
	// since restart 1 is discarded.
	db3, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open after restart 1: %v", err)
	}
	defer db3.Close()

	st, _, err := db3.ReadState(shardID, 1)
	if err != nil {
		t.Fatalf("ReadState after restart 2: %v (everything written since restart 1 was discarded)", err)
	}
	if st.Term != 11 || st.Vote != 4 {
		t.Fatalf("hard state lost on restart 2: got %+v, want Term 11 Vote 4", st)
	}
	ss, err := db3.GetSnapshot(shardID, 1)
	if err != nil {
		t.Fatalf("GetSnapshot after restart 2: %v", err)
	}
	if ss.Index != 3 {
		t.Fatalf("snapshot metadata lost on restart 2: got %+v", ss)
	}

	// Restart 3 must be a plain no-op recovery too.
	if err := db3.SaveNodeState(shardID, 1, logdb.State{Term: 13, Vote: 5, Commit: 3}); err != nil {
		t.Fatalf("SaveNodeState after restart 2: %v", err)
	}
	if err := db3.Close(); err != nil {
		t.Fatalf("Close after restart 2: %v", err)
	}
	db4, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open after restart 2: %v", err)
	}
	defer db4.Close()
	st, _, err = db4.ReadState(shardID, 1)
	if err != nil {
		t.Fatalf("ReadState after restart 3: %v", err)
	}
	if st.Term != 13 || st.Vote != 5 {
		t.Fatalf("hard state lost on restart 3: got %+v, want Term 13 Vote 5", st)
	}

	// The corrupt tail is physically gone, which is what stops the loop.
	if sizeAfter := statSize(t, seg0Path); sizeAfter >= sizeBefore {
		t.Fatalf("corrupt segment not truncated: size %d -> %d", sizeBefore, sizeAfter)
	}
}

// TestRecovery_CorruptSegmentTruncateFailureIsFatal verifies the repair is
// fail-stop: if the corrupt segment cannot be truncated, Open must fail rather
// than proceed to discard the higher segments (which would then be deleted
// again on every subsequent restart).
func TestRecovery_CorruptSegmentTruncateFailureIsFatal(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeMultiBlockSegment(t, db, shardID)
	rotateShard(t, db, shardID)
	if err := db.SaveNodeState(shardID, 1, logdb.State{Term: 7, Vote: 2, Commit: 1}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	seg0Path := segmentPath(db, shardID, 0)
	seg1Path := segmentPath(db, shardID, 1)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	corruptSegmentBlock(t, seg0Path, blockSize)
	seg1SizeBefore := statSize(t, seg1Path)

	ffs := &faultFS{FS: defaultFS()}
	ffs.arm(func(f *faultFS) {
		f.failNames = map[string]bool{segmentFilename(0): true}
		f.failOpenFile = errors.New("simulated EIO on reopen for truncation")
	})
	_, err = Open(dir, WithFS(ffs), WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err == nil {
		t.Fatal("Open: expected fatal RecoveryError when the corrupt segment cannot be repaired")
	}
	var recErr *RecoveryError
	if !errors.As(err, &recErr) {
		t.Fatalf("Open error = %v, want RecoveryError", err)
	}
	if recErr.SegmentID != 0 {
		t.Fatalf("RecoveryError.SegmentID = %d, want 0", recErr.SegmentID)
	}
	// The higher segment must be untouched: the discard never ran.
	if got := statSize(t, seg1Path); got != seg1SizeBefore {
		t.Fatalf("segment 1 size = %d, want %d (discard must not run before a failed repair)", got, seg1SizeBefore)
	}
}

// statSize returns a file's size, failing the test on error.
func statSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := defaultFS().Stat(path)
	if err != nil {
		t.Fatalf("Stat %s: %v", path, err)
	}
	return fi.Size()
}

// segSyncFailFS fails Sync on files opened through OpenFile whose base name
// matches, while leaving read-only Open (used for directory fsyncs) intact.
type segSyncFailFS struct {
	FS
	mu       sync.Mutex
	failName string
	syncErr  error
}

func (f *segSyncFailFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	file, err := f.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	fail := f.failName != "" && filepath.Base(name) == f.failName
	syncErr := f.syncErr
	f.mu.Unlock()
	if fail {
		return &failingSyncFile{File: file, syncErr: syncErr}, nil
	}
	return file, nil
}

// TestRollback_FileSyncFailurePoisonsDB is the regression test for the
// rollback FILE fsync (as opposed to the directory fsync, covered above).
// truncateClosedSegment and segment.DiscardPending returned a bare
// SegmentDiscardError, which flowed up as a BatchWriteError that did NOT
// satisfy errors.Is(err, ErrDBPoisoned). The DB stayed writable, the next
// batch appended past an un-durable truncation, and a crash resurrected
// rolled-back records that recovery replays as valid entries the caller was
// told had FAILED.
func TestRollback_FileSyncFailurePoisonsDB(t *testing.T) {
	dir := t.TempDir()
	ffs := &segSyncFailFS{FS: defaultFS()}
	db, err := Open(dir, WithFS(ffs), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const shardID = uint64(1)
	if err := db.SaveEntries(shardID, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	shard := db.getShard(shardID)
	shard.mu.Lock()
	if err := shard.activeSegment.flushBlock(); err != nil {
		shard.mu.Unlock()
		t.Fatalf("flushBlock: %v", err)
	}
	cp := shardCheckpoint{
		fileOffset:  shard.activeSegment.fileOffset,
		blockOffset: shard.activeSegment.blockOffset,
		seg:         shard.activeSegment,
		segID:       shard.activeSegmentID,
		valid:       true,
	}
	checkpointSeg := shard.activeSegmentID
	if err := shard.rotateSegment(); err != nil {
		shard.mu.Unlock()
		t.Fatalf("rotateSegment: %v", err)
	}

	// Fail the fsync of the truncation that rolls the checkpoint segment back.
	simulated := errors.New("simulated EIO on rollback truncation fsync")
	ffs.mu.Lock()
	ffs.failName = segmentFilename(checkpointSeg)
	ffs.syncErr = simulated
	ffs.mu.Unlock()

	rbErr := rollbackShard(shard, &cp)
	shard.mu.Unlock()

	if rbErr == nil {
		t.Fatal("rollbackShard: expected error when the truncation fsync fails")
	}
	if !errors.Is(rbErr, ErrDBPoisoned) {
		t.Fatalf("rollbackShard error = %v, want ErrDBPoisoned in chain", rbErr)
	}
	if !errors.Is(rbErr, simulated) {
		t.Fatalf("rollbackShard error = %v, want underlying cause in chain", rbErr)
	}
	if !db.poisoned.Load() {
		t.Fatal("DB must be poisoned after a rollback truncation fsync failure")
	}

	batchErr := &BatchWriteError{WriteErr: errors.New("original write failure"), DiscardErrors: []error{rbErr}}
	if !errors.Is(batchErr, ErrDBPoisoned) {
		t.Fatal("BatchWriteError must expose ErrDBPoisoned from discard errors")
	}

	ffs.mu.Lock()
	ffs.failName = ""
	ffs.mu.Unlock()
	if err := db.SaveEntries(shardID, 1, makeEntries(4, 1, 1)); !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("SaveEntries after poison = %v, want ErrDBPoisoned", err)
	}
}

// TestCompact_DirSyncFailureKeepsEpochFloor is the regression test for the
// post-GC ordering bug: the epoch-floor recompute ran BEFORE the directory
// fsync. If the fsync fails (or the process crashes first) the unlink is not
// durable, but MinLiveEpoch already reports the raised floor, the snapshot
// pool purges the old DEK, and on restart the resurrected segment makes replay
// die with a fatal WALDecryptError — the node cannot start at all.
func TestCompact_DirSyncFailureKeepsEpochFloor(t *testing.T) {
	barrier := newTestBarrier(t)
	ffs := &dirSyncFailFS{FS: defaultFS()}
	dir := t.TempDir()
	db, err := Open(dir,
		WithFS(ffs),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const nodeA, nodeB = uint64(1), uint64(17)

	// Node A fills segments 0 and 1 at epoch 1.
	if err := db.SaveEntries(nodeA, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries A seg0: %v", err)
	}
	rotateShard(t, db, nodeA)
	if err := db.SaveEntries(nodeA, 1, makeEntries(11, 10, 1)); err != nil {
		t.Fatalf("SaveEntries A seg1: %v", err)
	}
	rotateShard(t, db, nodeA)

	// Node B writes at epoch 2 into the active segment.
	if _, err := barrier.Rotate(); err != nil {
		t.Fatalf("barrier.Rotate: %v", err)
	}
	if err := db.SaveEntries(nodeB, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries B: %v", err)
	}
	if err := db.RemoveEntriesTo(nodeA, 1, 20); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if epoch, err := db.MinLiveEpoch(); err != nil || epoch != 1 {
		t.Fatalf("MinLiveEpoch before Compact = %d, %v; want 1, nil", epoch, err)
	}

	// GC succeeds but the directory fsync fails: the deletions are not proven
	// durable, so the floor must NOT rise.
	shardDir := db.getShard(nodeA).dir
	ffs.mu.Lock()
	ffs.failDir = shardDir
	ffs.syncErr = errors.New("simulated EIO on directory fsync")
	ffs.mu.Unlock()

	if err := db.Compact(nodeA, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("MinLiveEpoch after unproven deletion = %d, want 1 "+
			"(the floor must not rise on a deletion a crash can undo)", epoch)
	}
}

// TestRemoveNodeData_DirSyncFailureKeepsEpochFloor is the RemoveNodeData
// counterpart of TestCompact_DirSyncFailureKeepsEpochFloor.
func TestRemoveNodeData_DirSyncFailureKeepsEpochFloor(t *testing.T) {
	barrier := newTestBarrier(t)
	ffs := &dirSyncFailFS{FS: defaultFS()}
	dir := t.TempDir()
	db, err := Open(dir,
		WithFS(ffs),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const nodeA, nodeB = uint64(1), uint64(17)

	if err := db.SaveEntries(nodeA, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries A: %v", err)
	}
	rotateShard(t, db, nodeA)
	if _, err := barrier.Rotate(); err != nil {
		t.Fatalf("barrier.Rotate: %v", err)
	}
	if err := db.SaveEntries(nodeB, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries B: %v", err)
	}
	if epoch, err := db.MinLiveEpoch(); err != nil || epoch != 1 {
		t.Fatalf("MinLiveEpoch before removal = %d, %v; want 1, nil", epoch, err)
	}

	shardDir := db.getShard(nodeA).dir
	ffs.mu.Lock()
	ffs.failDir = shardDir
	ffs.syncErr = errors.New("simulated EIO on directory fsync")
	ffs.mu.Unlock()

	if err := db.RemoveNodeData(nodeA, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}

	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("MinLiveEpoch after unproven deletion = %d, want 1 "+
			"(the floor must not rise on a deletion a crash can undo)", epoch)
	}
}
