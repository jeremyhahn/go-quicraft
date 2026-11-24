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

// corruptWALForRepair builds a WAL whose final segment has decodable records
// after a corruption — the fail-closed condition an ordinary power loss can
// produce — and returns the directory plus the SegmentCorruptionError Open
// refuses it with.
func corruptWALForRepair(t *testing.T, blockSize int) (string, *SegmentCorruptionError) {
	t.Helper()
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeMultiBlockSegment(t, db, shardID)
	if err := db.SaveNodeState(shardID, 1, logdb.State{Term: 7, Vote: 3, Commit: 15}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	seg0Path := segmentPath(db, shardID, 0)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	flipByteAt(t, seg0Path, int64(blockSize)+100)

	db, err = Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err == nil {
		db.Close()
		t.Fatal("fixture: Open accepted a mid-segment corruption")
	}
	var corrupt *SegmentCorruptionError
	if !errors.As(err, &corrupt) {
		t.Fatalf("fixture: Open error = %v, want a *SegmentCorruptionError", err)
	}
	return dir, corrupt
}

// TestRepair_MakesAFailClosedWALOpenableAgain is the availability half of the
// mid-segment fail-stop.
//
// Refusing to open is the right call — truncating past records that reached the
// medium would destroy acknowledged data — but it cannot be the ONLY outcome.
// The condition is reachable from an ordinary power loss (a multi-block batch
// issues one WriteAt per block before a single fdatasync, and writeback can
// persist any subset of those pages), and "restore from peers" is no answer at
// all for a single-node deployment. Without an explicit exit the node is simply
// dead, permanently, with its data sitting right there on disk.
func TestRepair_MakesAFailClosedWALOpenableAgain(t *testing.T) {
	const blockSize = 1024
	dir, corrupt := corruptWALForRepair(t, blockSize)

	res, err := Repair(dir, RepairTarget{
		ShardIndex: corrupt.ShardIndex,
		SegmentID:  corrupt.SegmentID,
		TruncateAt: corrupt.CleanEnd,
	}, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if res.RecordsKept == 0 {
		t.Fatal("Repair kept no records; the clean prefix should have survived")
	}
	if res.BytesDiscarded <= 0 {
		t.Fatalf("Repair discarded %d bytes, want the corrupt tail gone", res.BytesDiscarded)
	}

	// The whole point: the node boots.
	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open after Repair: %v", err)
	}
	defer db.Close()

	// It boots on the clean prefix, and the cost is explicit: the hard state
	// was written above the corruption, so the repair destroyed it. This is
	// asserted rather than glossed over — a repair that appeared to lose
	// nothing would mean the truncation had not actually happened, and an
	// operator has to understand that this node's newest writes are gone.
	if _, _, err := db.ReadState(1, 1); !errors.Is(err, logdb.ErrNoState) {
		t.Fatalf("ReadState after Repair = %v, want logdb.ErrNoState: the hard "+
			"state sat above the truncation point and must not have survived it", err)
	}

	// Repair is idempotent in the sense that matters: a second run finds
	// nothing to do rather than cutting deeper.
	if _, err := Repair(dir, RepairTarget{
		ShardIndex: corrupt.ShardIndex,
		SegmentID:  corrupt.SegmentID,
		TruncateAt: corrupt.CleanEnd,
	}, WithBlockSize(blockSize), WithMaxFileSize(64*1024)); !errors.Is(err, ErrRepairNoCorruption) {
		t.Fatalf("second Repair = %v, want ErrRepairNoCorruption", err)
	}
}

// TestRepair_RefusesAnOffsetItDidNotComputeIsTheInterlock pins the guard that
// makes the API safe to hand an operator: TruncateAt is a CONFIRMATION of the
// offset the WAL named, never a free parameter.
//
// A repair that accepted any offset would let a mistyped or stale value cut the
// segment at an arbitrary byte — leaving a half record at the end, and if the
// cut landed above the real clean end, silently readmitting the corrupt region
// on the next open.
func TestRepair_RefusesAnOffsetItDidNotCompute(t *testing.T) {
	const blockSize = 1024
	dir, corrupt := corruptWALForRepair(t, blockSize)
	segPath := walSegmentPath(dir, corrupt.ShardIndex, corrupt.SegmentID)
	sizeBefore := statSize(t, segPath)

	for _, offset := range []int64{corrupt.CleanEnd + 1, corrupt.CleanEnd - 1, 0, sizeBefore} {
		_, err := Repair(dir, RepairTarget{
			ShardIndex: corrupt.ShardIndex,
			SegmentID:  corrupt.SegmentID,
			TruncateAt: offset,
		}, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
		if !errors.Is(err, ErrRepairOffsetMismatch) {
			t.Fatalf("Repair at offset %d = %v, want ErrRepairOffsetMismatch", offset, err)
		}
		var mismatch *RepairOffsetMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("Repair error = %v, want a *RepairOffsetMismatchError naming both offsets", err)
		}
		if mismatch.CleanEnd != corrupt.CleanEnd || mismatch.Requested != offset {
			t.Fatalf("mismatch = requested %d clean %d, want %d and %d",
				mismatch.Requested, mismatch.CleanEnd, offset, corrupt.CleanEnd)
		}
	}

	if size := statSize(t, segPath); size != sizeBefore {
		t.Fatalf("a refused repair still touched the segment: %d -> %d", sizeBefore, size)
	}
}

// TestRepair_RefusesAHealthySegment covers the other way an operator can arrive
// with a wrong target: a segment that is not corrupt at all, because a previous
// repair already ran or the error was copied from another node. Truncating it
// would destroy data for no reason whatsoever.
func TestRepair_RefusesAHealthySegment(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()
	seedWAL(t, dir, blockSize)

	shardIdx := int(1 % DefaultShardCount)
	segPath := walSegmentPath(dir, shardIdx, 0)
	sizeBefore := statSize(t, segPath)

	_, err := Repair(dir, RepairTarget{ShardIndex: shardIdx, SegmentID: 0, TruncateAt: 0},
		WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if !errors.Is(err, ErrRepairNoCorruption) {
		t.Fatalf("Repair of a healthy segment = %v, want ErrRepairNoCorruption", err)
	}
	if size := statSize(t, segPath); size != sizeBefore {
		t.Fatalf("a refused repair truncated a healthy segment: %d -> %d", sizeBefore, size)
	}
}

// TestRepair_RejectsAnOutOfRangeShard pins the cheapest guard: a shard index
// that does not exist must not be turned into a path.
func TestRepair_RejectsAnOutOfRangeShard(t *testing.T) {
	dir := t.TempDir()
	for _, idx := range []int{-1, DefaultShardCount, DefaultShardCount + 5} {
		if _, err := Repair(dir, RepairTarget{ShardIndex: idx}); !errors.Is(err, ErrRepairShardOutOfRange) {
			t.Fatalf("Repair with shard %d = %v, want ErrRepairShardOutOfRange", idx, err)
		}
	}
}

// TestRepair_DeletesSegmentsAboveTheTruncation pins the prefix-validity rule.
// Everything above a truncation point is unreachable whatever it holds, and an
// intact orphan left behind replays its stale hard state on the next restart —
// a term/vote regression, i.e. a second vote in one term.
func TestRepair_DeletesSegmentsAboveTheTruncation(t *testing.T) {
	const blockSize = 1024
	dir, corrupt := corruptWALForRepair(t, blockSize)

	// Plant a higher-numbered segment in the same shard.
	shardDir := walShardDirByIndex(dir, corrupt.ShardIndex)
	orphanID := corrupt.SegmentID + 7
	writeSegmentRecords(t, shardDir, orphanID, blockSize, 4, 100)

	res, err := Repair(dir, RepairTarget{
		ShardIndex: corrupt.ShardIndex,
		SegmentID:  corrupt.SegmentID,
		TruncateAt: corrupt.CleanEnd,
	}, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if len(res.SegmentsDeleted) != 1 || res.SegmentsDeleted[0] != orphanID {
		t.Fatalf("Repair deleted %v, want [%d]", res.SegmentsDeleted, orphanID)
	}
	if _, err := os.Stat(walSegmentPath(dir, corrupt.ShardIndex, orphanID)); !os.IsNotExist(err) {
		t.Fatalf("orphan segment above the truncation survived: %v", err)
	}
}

// walShardDirByIndex returns the directory of the shard at the given INDEX
// (walShardDir takes a Raft shard ID and hashes it).
func walShardDirByIndex(root string, idx int) string {
	return filepath.Join(root, shardDirName(idx))
}

// walSegmentPath returns the path of one segment inside one shard directory.
func walSegmentPath(root string, shardIdx int, segID uint64) string {
	return filepath.Join(walShardDirByIndex(root, shardIdx), segmentFilename(segID))
}

// TestRepair_RefusesTheWrongBlockSize pins the geometry guard on the repair
// path. A repair scans on the configured grid and then truncates at what it
// finds, so running it under the wrong block size would compute a meaningless
// offset for the one operation whose whole job is to cut at that offset.
func TestRepair_RefusesTheWrongBlockSize(t *testing.T) {
	const blockSize = 1024
	dir, corrupt := corruptWALForRepair(t, blockSize)
	segPath := walSegmentPath(dir, corrupt.ShardIndex, corrupt.SegmentID)
	sizeBefore := statSize(t, segPath)

	_, err := Repair(dir, RepairTarget{
		ShardIndex: corrupt.ShardIndex,
		SegmentID:  corrupt.SegmentID,
		TruncateAt: corrupt.CleanEnd,
	}, WithBlockSize(2048), WithMaxFileSize(64*1024))
	if !errors.Is(err, ErrWALGeometryMismatch) {
		t.Fatalf("Repair with a mismatched block size = %v, want ErrWALGeometryMismatch", err)
	}
	if size := statSize(t, segPath); size != sizeBefore {
		t.Fatalf("a refused repair truncated the segment: %d -> %d", sizeBefore, size)
	}
}

// TestRepair_DirSyncFailureIsReported is the regression test for the dropped
// error in discardSegmentsAbove.
//
// The directory fsync that makes the orphan unlinks durable must not be
// logged and swallowed. Inside recover() a swallowed failure would be masked by
// a second, fatal directory fsync a few lines later; Repair has no such
// backstop. It would therefore return a RepairResult naming the segments it
// had deleted while the deletions might never reach the medium — and a crash
// would then resurrect segments holding CRC-valid records ABOVE the offset the
// operator authorized truncating at, whose stale HardState replays and lets
// the node vote twice in one term. That is precisely what the
// directory fsync exists to prevent, reached through the one code path an
// operator drives by hand.
func TestRepair_DirSyncFailureIsReported(t *testing.T) {
	const blockSize = 1024
	dir, corrupt := corruptWALForRepair(t, blockSize)

	shardDir := walShardDirByIndex(dir, corrupt.ShardIndex)
	orphanID := corrupt.SegmentID + 7
	writeSegmentRecords(t, shardDir, orphanID, blockSize, 4, 100)
	segPath := walSegmentPath(dir, corrupt.ShardIndex, corrupt.SegmentID)
	sizeBefore := statSize(t, segPath)

	syncErr := errors.New("simulated EIO on directory fsync")
	ffs := &dirSyncFailFS{FS: defaultFS(), failDir: shardDir, syncErr: syncErr}

	res, err := Repair(dir, RepairTarget{
		ShardIndex: corrupt.ShardIndex,
		SegmentID:  corrupt.SegmentID,
		TruncateAt: corrupt.CleanEnd,
	}, WithFS(ffs), WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err == nil {
		t.Fatalf("Repair returned %+v and no error, but the fsync proving the "+
			"deletion of segment %d durable FAILED: a crash now resurrects a segment "+
			"holding CRC-valid records above the authorized truncation point, and "+
			"its stale HardState replays as a second vote in the same term",
			res, orphanID)
	}
	if !errors.Is(err, syncErr) {
		t.Fatalf("Repair error = %v, want the directory fsync failure in the chain", err)
	}
	var dirErr *DirSyncError
	if !errors.As(err, &dirErr) {
		t.Fatalf("Repair error = %v, want a *DirSyncError", err)
	}

	// And the segment must NOT have been cut: the repair runs high to low, so a
	// failure to make the deletions durable stops it before the truncation.
	// Cutting first and then failing would leave a segment ending at a clean
	// record boundary with the orphan still readable above it — the exact
	// resurrection this fails closed to avoid.
	if got := statSize(t, segPath); got != sizeBefore {
		t.Fatalf("segment %d was truncated (%d -> %d) even though the discard "+
			"could not be proven durable", corrupt.SegmentID, sizeBefore, got)
	}
}

// TestRepair_DeletesOrphansBeforeCuttingTheSegment pins the repair ORDER.
//
// Truncating the named segment first leaves it ending cleanly at a record
// boundary — nothing left for recovery to detect — while the segments above it
// still hold CRC-valid records. A crash there produces a recovery that replays
// the truncated segment in full and then every segment above it, readmitting
// exactly the records the operator authorized destroying, with a hole where the
// cut was. Deleting high to low means every intermediate crash state is a valid
// prefix: the named segment is still corrupt, so a re-run finds the same clean
// end and does the same thing.
//
// The order is observed through the ONE point where the two steps can be told
// apart: a truncation that cannot even open the segment file. The discard must
// already have happened.
func TestRepair_DeletesOrphansBeforeCuttingTheSegment(t *testing.T) {
	const blockSize = 1024
	dir, corrupt := corruptWALForRepair(t, blockSize)

	shardDir := walShardDirByIndex(dir, corrupt.ShardIndex)
	orphanID := corrupt.SegmentID + 7
	writeSegmentRecords(t, shardDir, orphanID, blockSize, 4, 100)
	segPath := walSegmentPath(dir, corrupt.ShardIndex, corrupt.SegmentID)
	sizeBefore := statSize(t, segPath)

	openErr := errors.New("simulated EIO reopening the segment for truncation")
	ffs := &faultFS{FS: defaultFS()}
	ffs.arm(func(f *faultFS) {
		f.failNames = map[string]bool{segmentFilename(corrupt.SegmentID): true}
		f.failOpenFile = openErr
	})

	if _, err := Repair(dir, RepairTarget{
		ShardIndex: corrupt.ShardIndex,
		SegmentID:  corrupt.SegmentID,
		TruncateAt: corrupt.CleanEnd,
	}, WithFS(ffs), WithBlockSize(blockSize), WithMaxFileSize(64*1024)); !errors.Is(err, openErr) {
		t.Fatalf("Repair error = %v, want the injected truncation failure", err)
	}

	if got := statSize(t, segPath); got != sizeBefore {
		t.Fatalf("the failed truncation still changed segment %d (%d -> %d)",
			corrupt.SegmentID, sizeBefore, got)
	}
	if _, err := os.Stat(walSegmentPath(dir, corrupt.ShardIndex, orphanID)); !os.IsNotExist(err) {
		t.Fatalf("Stat(orphan %d) = %v, want not-exist: the segments above the "+
			"truncation point must be gone BEFORE the named segment is cut, so no "+
			"crash state leaves a clean-ending segment with CRC-valid records "+
			"stranded above it", orphanID, err)
	}
}
