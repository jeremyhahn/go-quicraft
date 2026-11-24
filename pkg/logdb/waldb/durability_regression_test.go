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
	"context"
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

// TestRemoveNodeData_GCFailureKeepsEpochFloor is a regression test for
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
	if _, err := barrier.Rotate(context.Background()); err != nil {
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

// TestCompact_PartialGCFailureKeepsEpochFloor is a regression test for
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
	if _, err := barrier.Rotate(context.Background()); err != nil {
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

// TestRemoveNodeData_StaysRemovedWhileOlderSegmentsSurvive: the retired-replica registry keeps a removed node removed when shared segments survive replay.
func TestRemoveNodeData_StaysRemovedWhileOlderSegmentsSurvive(t *testing.T) {
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

	// Segment 1: node A alone.
	if err := db.SaveEntries(nodeA, 1, makeEntries(6, 5, 1)); err != nil {
		t.Fatalf("SaveEntries A seg1: %v", err)
	}
	rotateShard(t, db, nodeA)

	if err := db.RemoveNodeData(nodeA, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}
	// Segment 2: more node B records, advancing the commit high-water mark.
	if err := db.SaveEntries(nodeB, 1, makeEntries(4, 3, 1)); err != nil {
		t.Fatalf("SaveEntries B seg2: %v", err)
	}
	if err := db.Compact(nodeB, 0); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Segment 0 survives (node B). Segment 1 held only node A and nothing
	// pins it for the removal any more: it is reclaimed.
	if _, err := defaultFS().Stat(segmentPath(db, nodeA, 0)); err != nil {
		t.Fatalf("segment 0, still used by node B, was deleted: %v", err)
	}
	if _, err := defaultFS().Stat(segmentPath(db, nodeA, 1)); err == nil {
		t.Fatal("segment 1, holding only the removed node, was kept")
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
		t.Fatal("removed node A resurrected after reopen from a surviving older segment")
	}
	if !sawB {
		t.Fatal("surviving node B missing after reopen")
	}
	shard := db2.getShard(nodeA)
	shard.mu.Lock()
	_, pinned := shard.metaSegments[keyA]
	shard.mu.Unlock()
	if pinned {
		t.Fatal("recovery left segment pins for a removed node")
	}
	if removed, err := db2.NodeRemoved(nodeA, 1); err != nil || !removed {
		t.Fatalf("NodeRemoved after reopen = (%v, %v), want (true, nil)", removed, err)
	}
}

// writeMultiBlockSegment fills segment 0 of shardID's WAL shard across several blocks, one fsynced generation per record: damage recovery must refuse.
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

// writeSingleGenerationSegment fills segment 0 of shardID's WAL shard with several blocks in one durability generation, an in-flight batch that may legally be truncated; remove its sync mark with dropSyncMark.
func writeSingleGenerationSegment(t *testing.T, db *DB, shardID uint64) {
	t.Helper()
	updates := make([]logdb.Update, 0, 15)
	for i := uint64(0); i < 15; i++ {
		updates = append(updates, logdb.Update{
			ShardID:   shardID,
			ReplicaID: i + 1,
			Entries: []logdb.Entry{{
				Index: 1,
				Term:  1,
				Cmd:   bytes.Repeat([]byte{byte(i + 1)}, 200),
			}},
		})
	}
	if err := db.SaveState(updates); err != nil {
		t.Fatalf("SaveState (single generation): %v", err)
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

// TestRecovery_NonFinalCorruptionDoesNotMutateDisk: fail-stop recovery on a non-final segment changes nothing on disk.
func TestRecovery_NonFinalCorruptionDoesNotMutateDisk(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeSingleGenerationSegment(t, db, shardID)
	rotateShard(t, db, shardID)
	if err := db.SaveNodeState(shardID, 1, logdb.State{Term: 7, Vote: 2, Commit: 1}); err != nil {
		t.Fatalf("SaveNodeState seg1: %v", err)
	}
	rotateShard(t, db, shardID)
	if err := db.SaveNodeState(shardID, 1, logdb.State{Term: 9, Vote: 3, Commit: 1}); err != nil {
		t.Fatalf("SaveNodeState seg2: %v", err)
	}
	seg0Path := segmentPath(db, shardID, 0)
	seg1Path := segmentPath(db, shardID, 1)
	seg2Path := segmentPath(db, shardID, 2)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt segment 0 mid-file: segments 1-2 above hold committed state.
	corruptSegmentBlock(t, seg0Path, blockSize)

	// Sizes after corruption; fail-stop recovery must leave them unchanged.
	sizeBefore := func(p string) int64 {
		fi, err := defaultFS().Stat(p)
		if err != nil {
			t.Fatalf("Stat %s: %v", p, err)
		}
		return fi.Size()
	}
	want0, want1, want2 := sizeBefore(seg0Path), sizeBefore(seg1Path), sizeBefore(seg2Path)

	_, err = Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err == nil {
		t.Fatal("Open: expected fatal RecoveryError on non-final segment corruption")
	}
	if !errors.Is(err, ErrNonFinalSegmentCorruption) {
		t.Fatalf("Open error = %v, want ErrNonFinalSegmentCorruption", err)
	}
	// The error is a RecoveryError naming the corrupt non-final segment, not
	// one of the intact segments above it.
	var recErr *RecoveryError
	if !errors.As(err, &recErr) {
		t.Fatalf("Open error = %v, want RecoveryError", err)
	}
	if recErr.SegmentID != 0 {
		t.Fatalf("RecoveryError.SegmentID = %d, want 0 (the corrupt non-final segment)", recErr.SegmentID)
	}

	// Nothing on disk may have been truncated or discarded.
	if got := sizeBefore(seg0Path); got != want0 {
		t.Fatalf("corrupt segment 0 was mutated: size=%d, want %d (must not be prefix-truncated)", got, want0)
	}
	for _, tc := range []struct {
		path string
		want int64
	}{{seg1Path, want1}, {seg2Path, want2}} {
		fi, err := defaultFS().Stat(tc.path)
		if err != nil {
			t.Fatalf("committed segment %s was discarded or unreadable: %v", tc.path, err)
		}
		if fi.Size() != tc.want {
			t.Fatalf("committed segment %s was mutated: size=%d, want %d", tc.path, fi.Size(), tc.want)
		}
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

// TestRollback_DirSyncFailurePoisonsDB: a failed directory fsync during rollback poisons the DB (ErrDBPoisoned).
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

// TestRecovery_CorruptSegmentRepairedSoLaterWritesSurvive: recovery truncates the corrupt segment itself, so a later restart does not discard newer committed segments.
func TestRecovery_CorruptSegmentRepairedSoLaterWritesSurvive(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeSingleGenerationSegment(t, db, shardID)
	seg0Path := segmentPath(db, shardID, 0)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dropSyncMark(t, seg0Path, blockSize)

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

// TestRecovery_CorruptSegmentTruncateFailureIsFatal: if the corrupt final segment cannot be truncated, Open fails.
func TestRecovery_CorruptSegmentTruncateFailureIsFatal(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeSingleGenerationSegment(t, db, shardID)
	seg0Path := segmentPath(db, shardID, 0)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dropSyncMark(t, seg0Path, blockSize)

	corruptSegmentBlock(t, seg0Path, blockSize)
	sizeBefore := statSize(t, seg0Path)

	truncErr := errors.New("simulated EIO on reopen for truncation")
	ffs := &faultFS{FS: defaultFS()}
	ffs.arm(func(f *faultFS) {
		f.failNames = map[string]bool{segmentFilename(0): true}
		f.failOpenFile = truncErr
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
	// The failure that aborted the open must be the truncation, not something
	// that happened to fail earlier and looked the same.
	if !errors.Is(err, truncErr) {
		t.Fatalf("Open error = %v, want the injected truncation failure in the "+
			"chain (otherwise the test never reached the repair path)", err)
	}
	// The corrupt segment must be byte-length identical: the truncation is the
	// step that failed, and a partially applied one would leave a cut this
	// recovery never proved durable.
	if got := statSize(t, seg0Path); got != sizeBefore {
		t.Fatalf("segment 0 size = %d, want %d (the failed truncation must not have applied)",
			got, sizeBefore)
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

// TestRollback_FileSyncFailurePoisonsDB: a failed file fsync during rollback satisfies errors.Is(err, ErrDBPoisoned) and stops writes.
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

// TestCompact_DirSyncFailureKeepsEpochFloor: the epoch floor rises only after the post-GC directory fsync succeeds.
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
	if _, err := barrier.Rotate(context.Background()); err != nil {
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
	// durable, so the floor must NOT rise. The compaction floors are published
	// first, so the injected failure hits the post-GC directory sync rather
	// than the floor publication that precedes every unlink.
	shard := db.getShard(nodeA)
	shard.mu.Lock()
	pubErr := shard.publishCompactionFloorsLocked()
	shard.mu.Unlock()
	if pubErr != nil {
		t.Fatalf("publishCompactionFloorsLocked: %v", pubErr)
	}
	shardDir := shard.dir
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
	if _, err := barrier.Rotate(context.Background()); err != nil {
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

// TestRollback_DeletesHigherSegmentsBeforeCuttingTheCheckpoint: rollback removes segments high to low, so every crash state is a valid prefix.
func TestRollback_DeletesHigherSegmentsBeforeCuttingTheCheckpoint(t *testing.T) {
	dir := t.TempDir()
	ffs := &faultFS{FS: defaultFS()}
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

	// Checkpoint the pre-batch durable boundary of segment 0.
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

	// Batch data past the checkpoint, in the checkpoint segment and then in two
	// more segments, exactly as a batch that hit ErrSegmentFull twice would
	// leave behind.
	batch := bytes.Repeat([]byte{0xa5}, 300)
	writeBatchBytes := func(label string) {
		if _, err := shard.activeSegment.Write(batch); err != nil {
			shard.mu.Unlock()
			t.Fatalf("Write (%s): %v", label, err)
		}
		if err := shard.activeSegment.Sync(); err != nil {
			shard.mu.Unlock()
			t.Fatalf("Sync (%s): %v", label, err)
		}
	}
	writeBatchBytes("checkpoint segment")
	if err := shard.rotateSegment(); err != nil {
		shard.mu.Unlock()
		t.Fatalf("rotateSegment 1: %v", err)
	}
	intermediateID := shard.activeSegmentID
	writeBatchBytes("intermediate segment")
	if err := shard.rotateSegment(); err != nil {
		shard.mu.Unlock()
		t.Fatalf("rotateSegment 2: %v", err)
	}
	activeID := shard.activeSegmentID
	writeBatchBytes("active segment")

	seg0Path := filepath.Join(shard.dir, segmentFilename(cp.segID))
	seg0Size := statSize(t, seg0Path)
	if seg0Size <= cp.fileOffset+int64(cp.blockOffset) {
		shard.mu.Unlock()
		t.Fatalf("fixture: segment %d is %d bytes, not past the checkpoint end %d",
			cp.segID, seg0Size, cp.fileOffset+int64(cp.blockOffset))
	}

	// Fail only the reopen of the checkpoint segment, which is the truncation.
	truncErr := errors.New("simulated EIO reopening the checkpoint segment")
	ffs.arm(func(f *faultFS) {
		f.failNames = map[string]bool{segmentFilename(cp.segID): true}
		f.failOpenFile = truncErr
	})

	rbErr := rollbackShard(shard, &cp)
	shard.mu.Unlock()

	if rbErr == nil {
		t.Fatal("rollbackShard: expected an error when the checkpoint truncation fails")
	}
	if !errors.Is(rbErr, truncErr) {
		t.Fatalf("rollbackShard error = %v, want the injected truncation failure", rbErr)
	}
	if !errors.Is(rbErr, ErrDBPoisoned) {
		t.Fatalf("rollbackShard error = %v, want ErrDBPoisoned in the chain", rbErr)
	}

	// The intermediate segment must already be gone.
	if _, err := defaultFS().Stat(filepath.Join(shard.dir, segmentFilename(intermediateID))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat(intermediate segment %d) = %v, want fs.ErrNotExist: the "+
			"segments above the checkpoint must be discarded BEFORE the checkpoint "+
			"segment is cut, or a crash between the two replays the checkpoint "+
			"segment in full and then this one, resurrecting the failed batch",
			intermediateID, err)
	}

	// The active segment must already replay to nothing.
	activeRecs := reopenReadOnly(t, shard.dir, activeID, 1024)
	if len(activeRecs) != 0 {
		t.Fatalf("active segment %d still replays %d records; it must be emptied "+
			"before the checkpoint segment is cut", activeID, len(activeRecs))
	}

	// And the checkpoint segment is untouched: its truncation is the step that
	// failed, so the on-disk state is still the longer — but valid — prefix.
	if got := statSize(t, seg0Path); got != seg0Size {
		t.Fatalf("checkpoint segment %d size = %d, want %d (the failed truncation "+
			"must not have applied)", cp.segID, got, seg0Size)
	}
}
