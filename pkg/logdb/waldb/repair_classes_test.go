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
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// Repair is documented as the exit from every class of corruption recovery
// refuses to open over, not only ErrMidSegmentCorruption (repair_test.go).
// Each test here builds the fixture an existing recovery test uses for one of
// the other three classes and walks it through the same three steps:
//
//   - the failed Open names the class and carries a *SegmentCorruptionError
//     locating the damage;
//   - a Repair at any offset other than the clean end the error reported is
//     refused, and touches nothing on disk;
//   - a Repair at that clean end succeeds, the next Open succeeds, and the
//     entries written below the cut read back intact.

// repairClassCase is one refusal class staged on disk and ready to repair.
type repairClassCase struct {
	dir        string
	fsys       FS
	opts       []Option
	sentinel   error
	shardIndex int
	segmentID  uint64
	// readBack checks the reopened DB holds the entries below the cut.
	readBack func(ctx context.Context, t *testing.T, db *DB)
}

// snapshotWALDir returns every regular file under dir, keyed by its path
// relative to dir, with its full contents.
func snapshotWALDir(ctx context.Context, t *testing.T, fsys FS, dir string) map[string][]byte {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	out := make(map[string][]byte)
	var walk func(rel string)
	walk = func(rel string) {
		entries, err := fsys.ReadDir(filepath.Join(dir, rel))
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", rel, err)
		}
		for _, e := range entries {
			name := filepath.Join(rel, e.Name())
			if e.IsDir() {
				walk(name)
				continue
			}
			f, err := fsys.Open(filepath.Join(dir, name))
			if err != nil {
				t.Fatalf("Open(%s): %v", name, err)
			}
			fi, err := f.Stat()
			if err != nil {
				f.Close()
				t.Fatalf("Stat(%s): %v", name, err)
			}
			buf := make([]byte, fi.Size())
			if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
				f.Close()
				t.Fatalf("ReadAt(%s): %v", name, err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("Close(%s): %v", name, err)
			}
			out[name] = buf
		}
	}
	walk("")
	return out
}

// assertSameWALDir fails when before and after differ in any file's presence
// or contents.
func assertSameWALDir(ctx context.Context, t *testing.T, before, after map[string][]byte) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("compare: %v", err)
	}
	for name, want := range before {
		got, ok := after[name]
		if !ok {
			t.Fatalf("refused Repair removed %s", name)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("refused Repair changed %s (size %d -> %d)", name, len(want), len(got))
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			t.Fatalf("refused Repair created %s", name)
		}
	}
}

// runRepairClassCase drives one refusal class through Open, a refused Repair,
// an accepted Repair and a reopen.
func runRepairClassCase(ctx context.Context, t *testing.T, c repairClassCase) {
	t.Helper()

	// (a) The open fails closed on this class, and says where.
	db, err := Open(c.dir, c.opts...)
	if err == nil {
		db.Close()
		t.Fatalf("Open succeeded; want %v", c.sentinel)
	}
	if !errors.Is(err, c.sentinel) {
		t.Fatalf("Open error = %v, want %v", err, c.sentinel)
	}
	var sce *SegmentCorruptionError
	if !errors.As(err, &sce) {
		t.Fatalf("Open error = %v, want a *SegmentCorruptionError", err)
	}
	if sce.ShardIndex != c.shardIndex || sce.SegmentID != c.segmentID {
		t.Fatalf("SegmentCorruptionError names shard %d segment %d, want shard %d segment %d",
			sce.ShardIndex, sce.SegmentID, c.shardIndex, c.segmentID)
	}
	if sce.CleanEnd <= 0 {
		t.Fatalf("SegmentCorruptionError.CleanEnd = %d, want a positive clean prefix", sce.CleanEnd)
	}

	// (c) Any offset but the reported clean end is refused, with nothing touched.
	before := snapshotWALDir(ctx, t, c.fsys, c.dir)
	_, err = Repair(c.dir, RepairTarget{
		ShardIndex: sce.ShardIndex,
		SegmentID:  sce.SegmentID,
		TruncateAt: sce.CleanEnd + 1,
	}, c.opts...)
	if !errors.Is(err, ErrRepairOffsetMismatch) {
		t.Fatalf("Repair at CleanEnd+1 = %v, want ErrRepairOffsetMismatch", err)
	}
	var mismatch *RepairOffsetMismatchError
	if !errors.As(err, &mismatch) || mismatch.CleanEnd != sce.CleanEnd {
		t.Fatalf("Repair at CleanEnd+1 = %v, want a RepairOffsetMismatchError naming clean end %d",
			err, sce.CleanEnd)
	}
	assertSameWALDir(ctx, t, before, snapshotWALDir(ctx, t, c.fsys, c.dir))

	// (b) The reported clean end is accepted, and the node boots on the prefix.
	res, err := Repair(c.dir, RepairTarget{
		ShardIndex: sce.ShardIndex,
		SegmentID:  sce.SegmentID,
		TruncateAt: sce.CleanEnd,
	}, c.opts...)
	if err != nil {
		t.Fatalf("Repair at CleanEnd %d: %v", sce.CleanEnd, err)
	}
	if res.RecordsKept == 0 {
		t.Fatal("Repair kept no records; the clean prefix should have survived")
	}
	db, err = Open(c.dir, c.opts...)
	if err != nil {
		t.Fatalf("Open after Repair: %v", err)
	}
	defer db.Close()
	c.readBack(ctx, t, db)
}

// readEntryPrefix returns how many of the indexes first..last read back with
// the command want(index) before the first that does not, and fails if any
// index above that point reads back at all: a repair keeps a prefix, never a
// prefix with a resurrected record above a hole.
func readEntryPrefix(ctx context.Context, t *testing.T, db *DB, shardID, replicaID, first, last uint64,
	want func(index uint64) []byte) uint64 {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("read back: %v", err)
	}
	var kept uint64
	missing := false
	for idx := first; idx <= last; idx++ {
		got, err := db.GetEntries(shardID, replicaID, idx, idx+1, 1<<30)
		present := err == nil && len(got) == 1 && got[0].Index == idx
		if present && !bytes.Equal(got[0].Cmd, want(idx)) {
			t.Fatalf("entry %d/%d/%d read back with a different command", shardID, replicaID, idx)
		}
		switch {
		case present && missing:
			t.Fatalf("entry %d/%d/%d survived above a missing entry", shardID, replicaID, idx)
		case present:
			kept++
		default:
			missing = true
		}
	}
	return kept
}

func TestRepairEveryRefusalClass_NonFinalSegmentCorruption(t *testing.T) {
	ctx := context.Background()
	const (
		blockSize = 1024
		shardID   = uint64(1)
	)
	dir := t.TempDir()
	opts := []Option{WithBlockSize(blockSize), WithMaxFileSize(64 * 1024)}

	db, err := Open(dir, opts...)
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
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	corruptSegmentBlock(t, seg0Path, blockSize)

	runRepairClassCase(ctx, t, repairClassCase{
		dir:        dir,
		fsys:       defaultFS(),
		opts:       opts,
		sentinel:   ErrNonFinalSegmentCorruption,
		shardIndex: int(shardID % DefaultShardCount),
		segmentID:  0,
		readBack: func(ctx context.Context, t *testing.T, db *DB) {
			// writeSingleGenerationSegment gave replica r one entry at index 1
			// whose command is 200 bytes of r. The replicas written below the
			// zeroed block survive; the rest went with it.
			var kept uint64
			missing := false
			for r := uint64(1); r <= 15; r++ {
				n := readEntryPrefix(ctx, t, db, shardID, r, 1, 1, func(uint64) []byte {
					return bytes.Repeat([]byte{byte(r)}, 200)
				})
				switch {
				case n == 1 && missing:
					t.Fatalf("replica %d's entry survived above a missing one", r)
				case n == 1:
					kept++
				default:
					missing = true
				}
			}
			if kept == 0 || kept == 15 {
				t.Fatalf("%d of 15 entries read back; want the ones below the zeroed block, "+
					"and not the ones in it", kept)
			}
			// The hard state lived in the segments above the cut, which the
			// repair deleted.
			if _, _, err := db.ReadState(shardID, 1); !errors.Is(err, logdb.ErrNoState) {
				t.Fatalf("ReadState after Repair = %v, want logdb.ErrNoState", err)
			}
		},
	})
}

func TestRepairEveryRefusalClass_AcknowledgedDataLost(t *testing.T) {
	ctx := context.Background()
	const (
		root      = "/mnt/repair-acked-damage"
		blockSize = 1024
		maxFile   = 1 << 20
		shardID   = 1
		replicaID = 7
	)
	dir := filepath.Join(root, "wal")
	cfs := newCrashFS(root)
	opts := []Option{WithFS(cfs), WithBlockSize(blockSize), WithMaxFileSize(maxFile)}

	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	save := func(db *DB, from, count uint64) error {
		entries := make([]logdb.Entry, 0, count)
		for i := uint64(0); i < count; i++ {
			entries = append(entries, crashDurEntry(shardID, from+i, 1))
		}
		return db.SaveState([]logdb.Update{{
			ShardID:   shardID,
			ReplicaID: replicaID,
			State:     logdb.State{Term: 1, Vote: 2, Commit: from + count - 1},
			Entries:   entries,
		}})
	}
	// The fixture of TestWALCrashDurability_DamageBelowAcknowledgedEndFailsClosed:
	// acknowledged generations, then an in-flight batch cut at its fdatasync
	// with every page retained, then damage just below the acknowledged end.
	var acked uint64
	for b := 0; b < 6; b++ {
		if err := save(db, acked+1, 2); err != nil {
			t.Fatalf("acked batch %d: %v", b, err)
		}
		acked += 2
	}
	segPath := onlySegmentPath(t, cfs, filepath.Join(dir, shardDirName(int(shardID%DefaultShardCount))))
	var once sync.Once
	cfs.setHook(func(op crashOp, path string) {
		if op != crashOpSync || path != segPath {
			return
		}
		once.Do(func() {
			cfs.setWriteback(blockSize, func(string, int) bool { return true })
			cfs.Crash()
		})
	})
	if err := save(db, acked+1, 8); err == nil {
		t.Fatal("the batch whose fdatasync was interrupted returned success")
	}
	cfs.setHook(nil)

	image := readWholeFile(t, cfs, segPath)
	ackedEnd := int64(-1)
	for off := int64(0); off+recordHeaderSize <= int64(len(image)); off++ {
		_, _, seq, genStart, _, derr := decodeRecord(image[off:])
		if derr != nil {
			continue
		}
		if int64(genStart) > 0 && int64(genStart) <= off && seq > 0 {
			ackedEnd = int64(genStart)
		}
	}
	if ackedEnd <= int64(recordHeaderSize) {
		t.Fatalf("could not locate the in-flight generation's start in the image (%d)", ackedEnd)
	}
	invertFile(t, cfs, segPath, ackedEnd-8, 8)

	runRepairClassCase(ctx, t, repairClassCase{
		dir:        dir,
		fsys:       cfs,
		opts:       opts,
		sentinel:   ErrAcknowledgedDataLost,
		shardIndex: int(shardID % DefaultShardCount),
		segmentID:  0,
		readBack: func(ctx context.Context, t *testing.T, db *DB) {
			kept := readEntryPrefix(ctx, t, db, shardID, replicaID, 1, acked+8, func(idx uint64) []byte {
				return crashDurEntry(shardID, idx, 1).Cmd
			})
			// The cut keeps the acknowledged records below the damage and
			// nothing of the in-flight batch above it.
			if kept == 0 || kept > acked {
				t.Fatalf("%d entries read back; want the acknowledged ones below the "+
					"damage (at most %d, more than none)", kept, acked)
			}
		},
	})
}

func TestRepairEveryRefusalClass_RecordSequenceRegression(t *testing.T) {
	ctx := context.Background()
	// The fixture of TestRecovery_SequenceRegressionFailsOpenWithItsOwnError:
	// a page lost inside the final unsynced batch, and a well-formed record
	// carrying generation 0 planted above it.
	fx := writeSeqFixture(t)
	losePage(t, fx.segment, fx.batchAt+300)
	at, span := firstDecodableAt(t, fx.segment, fx.batchAt+300+seqTestPageSize)
	stale := make([]byte, span)
	if _, err := encodeRecord(stale, RecordFull, 0, 0, make([]byte, span-recordHeaderSize)); err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	f, err := os.OpenFile(fx.segment, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.WriteAt(stale, at); err != nil {
		t.Fatalf("write stale record: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	runRepairClassCase(ctx, t, repairClassCase{
		dir:        fx.dir,
		fsys:       defaultFS(),
		opts:       []Option{WithBlockSize(seqTestBlockSize), WithMaxFileSize(1024 * 1024)},
		sentinel:   ErrRecordSequenceRegression,
		shardIndex: 1,
		segmentID:  0,
		readBack: func(ctx context.Context, t *testing.T, db *DB) {
			// The acknowledged prefix — eight entries and the hard state after
			// them — lies wholly below the lost page.
			prefixCmd := make([]byte, 700)
			for i := range prefixCmd {
				prefixCmd[i] = byte(i % 97)
			}
			kept := readEntryPrefix(ctx, t, db, 1, 1, 1, 8, func(uint64) []byte { return prefixCmd })
			if kept != 8 {
				t.Fatalf("%d of the 8 acknowledged prefix entries read back, want all 8", kept)
			}
			st, _, err := db.ReadState(1, 1)
			if err != nil {
				t.Fatalf("ReadState after Repair: %v", err)
			}
			if st.Term != 9 || st.Vote != 4 || st.Commit < 8 {
				t.Fatalf("ReadState after Repair = %+v, want term 9 vote 4 commit >= 8", st)
			}
		},
	})
}
