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
	iofs "io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// segmentSizes maps every segment file under a WAL root to its size, so a test
// can assert that a refused open left the directory byte-for-byte alone.
func segmentSizes(t *testing.T, dir string) map[string]int64 {
	t.Helper()
	sizes := make(map[string]int64)
	for i := 0; i < DefaultShardCount; i++ {
		shardDir := filepath.Join(dir, shardDirName(i))
		entries, err := os.ReadDir(shardDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("read shard dir: %v", err)
		}
		for _, de := range entries {
			if _, ok := parseSegmentFilename(de.Name()); !ok {
				continue
			}
			info, err := de.Info()
			if err != nil {
				t.Fatalf("stat segment: %v", err)
			}
			sizes[filepath.Join(shardDir, de.Name())] = info.Size()
		}
	}
	return sizes
}

// sameSizes compares two segmentSizes maps.
func sameSizes(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// writeTestWALFormat stamps the format marker the way a first Open does, for
// tests that assemble a WAL directory by hand instead of opening one. Without it
// Open refuses the directory as unmarked (ErrWALFormatMissing) before replaying
// anything, and every assertion about replay would be measuring the wrong error.
func writeTestWALFormat(t *testing.T, dir string, blockSize int) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := writeWALFormat(defaultFS(), dir, blockSize, true); err != nil {
		t.Fatalf("writeWALFormat: %v", err)
	}
}

// seedWAL writes one hard state and enough entries to cross a block boundary, then closes the DB, so a wrong block size would misparse it.
func seedWAL(t *testing.T, dir string, blockSize int) {
	t.Helper()
	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 4, Vote: 2, Commit: 1}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	// One SaveEntries per call so the records land as separate WAL records and
	// the shard grows past blockSize.
	for i := uint64(0); i < 40; i++ {
		if err := db.SaveEntries(1, 1, makeEntries(1+i*3, 3, 4)); err != nil {
			t.Fatalf("SaveEntries: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !walShardExceedsBlock(t, dir, 1, blockSize) {
		t.Fatalf("fixture: seeded shard did not cross a %d-byte block boundary", blockSize)
	}
}

// seedWALSingleBlock writes a WAL small enough that no shard reaches a block
// boundary — the directory whose geometry could not be inferred from its
// contents back when inference was how the marker got written.
func seedWALSingleBlock(t *testing.T, dir string, blockSize int) {
	t.Helper()
	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 4, Vote: 2, Commit: 1}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if walShardExceedsBlock(t, dir, 1, blockSize) {
		t.Fatalf("fixture: seeded shard crossed a %d-byte block boundary", blockSize)
	}
}

// walShardExceedsBlock reports whether the shard holding shardID has a segment
// file longer than one block.
func walShardExceedsBlock(t *testing.T, dir string, shardID uint64, blockSize int) bool {
	t.Helper()
	shardDir := filepath.Join(dir, shardDirName(int(shardID%DefaultShardCount)))
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		t.Fatalf("read shard dir: %v", err)
	}
	for _, de := range entries {
		if _, ok := parseSegmentFilename(de.Name()); !ok {
			continue
		}
		info, err := de.Info()
		if err != nil {
			t.Fatalf("stat segment: %v", err)
		}
		if info.Size() > int64(blockSize) {
			return true
		}
	}
	return false
}

// TestOpen_BlockSizeMismatchIsRefused: a changed WithBlockSize on a populated directory fails Open with the real cause.
func TestOpen_BlockSizeMismatchIsRefused(t *testing.T) {
	dir := t.TempDir()
	seedWAL(t, dir, 1024)

	db, err := Open(dir, WithBlockSize(2048), WithMaxFileSize(64*1024))
	if err == nil {
		db.Close()
		t.Fatal("Open accepted a block size the directory was not written with; " +
			"every block boundary is misparsed from here on and recovery treats " +
			"the whole log as corrupt")
	}
	if !errors.Is(err, ErrWALGeometryMismatch) {
		t.Fatalf("Open error = %v, want ErrWALGeometryMismatch", err)
	}
	var mismatch *WALGeometryMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Open error = %v, want a *WALGeometryMismatchError naming both sizes", err)
	}
	if mismatch.Recorded != 1024 || mismatch.Configured != 2048 {
		t.Fatalf("mismatch = recorded %d configured %d, want 1024 and 2048",
			mismatch.Recorded, mismatch.Configured)
	}

	// The original geometry still opens, and the data is intact.
	db, err = Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open with the recorded block size: %v", err)
	}
	defer db.Close()
	st, _, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if st.Term != 4 || st.Vote != 2 {
		t.Fatalf("state = %+v, want Term 4 Vote 2", st)
	}
}

// TestOpen_UnmarkedPopulatedDirectoryIsRefused: a populated directory with no format marker is refused with a named error.
func TestOpen_UnmarkedPopulatedDirectoryIsRefused(t *testing.T) {
	dir := t.TempDir()
	seedWAL(t, dir, 1024)

	marker := filepath.Join(dir, walFormatFilename)
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("marker not written for a new directory: %v", err)
	}
	sizesBefore := segmentSizes(t, dir)

	// Simulate a directory written before the marker existed.
	if err := os.Remove(marker); err != nil {
		t.Fatalf("remove marker: %v", err)
	}

	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err == nil {
		db.Close()
		t.Fatal("Open accepted a populated WAL directory that records no on-disk " +
			"format: its records are framed for a parser this build does not have, " +
			"so recovery reads the whole log as corrupt and the torn-tail repair " +
			"truncates it away")
	}
	if !errors.Is(err, ErrWALFormatMissing) {
		t.Fatalf("Open error = %v, want ErrWALFormatMissing", err)
	}

	// A refused open leaves the directory untouched.
	if got := segmentSizes(t, dir); !sameSizes(got, sizesBefore) {
		t.Fatalf("a refused open mutated the segments on disk: %v -> %v",
			sizesBefore, got)
	}
}

// TestOpen_MalformedMarkerIsRefused verifies a marker that exists but cannot be
// parsed fails the open. A marker we cannot read is a geometry we cannot
// verify, and proceeding would restore exactly the unchecked assumption the
// marker exists to remove.
func TestOpen_MalformedMarkerIsRefused(t *testing.T) {
	dir := t.TempDir()
	seedWAL(t, dir, 1024)

	marker := filepath.Join(dir, walFormatFilename)
	if err := os.WriteFile(marker, []byte("not a wal format"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err == nil {
		db.Close()
		t.Fatal("Open accepted an unparseable geometry marker")
	}
	if !errors.Is(err, ErrWALFormatMalformed) {
		t.Fatalf("Open error = %v, want ErrWALFormatMalformed", err)
	}

	// A truncated marker is refused for the same reason.
	if err := os.WriteFile(marker, []byte(walFormatMagic), 0o600); err != nil {
		t.Fatalf("write short marker: %v", err)
	}
	if db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024)); !errors.Is(err, ErrWALFormatMalformed) {
		if err == nil {
			db.Close()
		}
		t.Fatalf("Open with a truncated marker = %v, want ErrWALFormatMalformed", err)
	}
}

// TestOpen_ZeroMarkerOnAPopulatedDirectoryIsRefused: a zero-length or all-zero marker reads as absent, so a populated directory is refused and an empty one adopted.
func TestOpen_ZeroMarkerOnAPopulatedDirectoryIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, marker string)
	}{
		{
			name: "zero length",
			write: func(t *testing.T, marker string) {
				if err := os.Truncate(marker, 0); err != nil {
					t.Fatalf("truncate marker: %v", err)
				}
			},
		},
		{
			name: "all zero bytes",
			write: func(t *testing.T, marker string) {
				if err := os.WriteFile(marker, make([]byte, walFormatSize), 0o600); err != nil {
					t.Fatalf("write zeroed marker: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			seedWAL(t, dir, 1024)
			marker := filepath.Join(dir, walFormatFilename)
			tc.write(t, marker)

			// The residue is reported as an absent marker, not as damage.
			if _, present, err := readWALFormat(defaultFS(), dir); err != nil || present {
				t.Fatalf("readWALFormat = (present %v, err %v), want (false, nil): a "+
					"stamp that never delivered its content is an absent geometry, not "+
					"a malformed one", present, err)
			}

			db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
			if err == nil {
				db.Close()
				t.Fatal("Open adopted an empty marker over a directory full of " +
					"segments, so it assumed a record framing it has no evidence for; " +
					"if the assumption is wrong every chunk fails CRC and the log is " +
					"truncated away as a torn tail")
			}
			if !errors.Is(err, ErrWALFormatMissing) {
				t.Fatalf("Open error = %v, want ErrWALFormatMissing", err)
			}

			// The same residue over an empty directory is adopted and re-stamped.
			empty := t.TempDir()
			if err := os.MkdirAll(empty, 0o700); err != nil {
				t.Fatalf("MkdirAll: %v", err)
			}
			tc.write(t, mustCreate(t, filepath.Join(empty, walFormatFilename)))
			db, err = Open(empty, WithBlockSize(1024), WithMaxFileSize(64*1024))
			if err != nil {
				t.Fatalf("Open of an EMPTY directory with the same marker residue "+
					"must succeed by re-stamping it: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if blockSize, present, err := readWALFormat(defaultFS(), empty); err != nil ||
				!present || blockSize != 1024 {
				t.Fatalf("marker after adoption = (%d, %v, %v), want (1024, true, nil)",
					blockSize, present, err)
			}
		})
	}
}

// mustCreate creates an empty file and returns its path.
func mustCreate(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
	return path
}

// TestOpen_SingleBlockUnmarkedDirectoryIsAlsoRefused: an unmarked directory whose segments fit one block is refused like any other.
func TestOpen_SingleBlockUnmarkedDirectoryIsAlsoRefused(t *testing.T) {
	dir := t.TempDir()
	seedWALSingleBlock(t, dir, 1024)

	if err := os.Remove(filepath.Join(dir, walFormatFilename)); err != nil {
		t.Fatalf("remove marker: %v", err)
	}

	// Reopen under a different block size; the refusal must come from the missing marker.
	db, err := Open(dir, WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err == nil {
		db.Close()
		t.Fatal("Open accepted an unmarked directory whose block size cannot be " +
			"inferred from its contents")
	}
	if !errors.Is(err, ErrWALFormatMissing) {
		t.Fatalf("Open error = %v, want ErrWALFormatMissing", err)
	}
	if _, present, err := readWALFormat(defaultFS(), dir); err != nil || present {
		t.Fatalf("a refused open stamped a marker anyway: (present %v, err %v)", present, err)
	}
}

// TestOpen_EmptyDirectoryIsStampedImmediately covers the other side of the
// evidence rule: a directory with no segments at all has nothing for a wrong
// value to misparse, so the configured geometry IS the geometry and is recorded
// at once rather than waiting for the WAL to grow.
func TestOpen_EmptyDirectoryIsStampedImmediately(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(2048), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	blockSize, present, err := readWALFormat(defaultFS(), dir)
	if err != nil {
		t.Fatalf("readWALFormat: %v", err)
	}
	if !present || blockSize != 2048 {
		t.Fatalf("marker for a fresh directory = (%d, %v), want (2048, true)",
			blockSize, present)
	}
}

// markerRecordingFS records the sequence of operations the marker publication
// performs, so a test can assert the durability steps actually happen rather
// than assuming they do because the code reads as if they do.
type markerRecordingFS struct {
	FS
	mu  sync.Mutex
	ops []string
	// failWriteOn, when non-empty, makes Write fail for the file whose base
	// name matches, modelling ENOSPC part-way through a stamp.
	failWriteOn string
}

func newMarkerRecordingFS() *markerRecordingFS {
	return &markerRecordingFS{FS: defaultFS()}
}

func (f *markerRecordingFS) record(op string) {
	f.mu.Lock()
	f.ops = append(f.ops, op)
	f.mu.Unlock()
}

func (f *markerRecordingFS) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ops...)
}

func (f *markerRecordingFS) OpenFile(name string, flag int, perm iofs.FileMode) (File, error) {
	// Creations are recorded too, so tests can order the marker before the first segment (TestOpen_MarkerWrittenBeforeAnySegmentExists).
	if flag&os.O_CREATE != 0 {
		f.record("create " + filepath.Base(name))
	}
	file, err := f.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &markerRecordingFile{File: file, name: filepath.Base(name), fs: f}, nil
}

func (f *markerRecordingFS) Open(name string) (File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return &markerRecordingFile{File: file, name: filepath.Base(name), fs: f}, nil
}

func (f *markerRecordingFS) Rename(oldpath, newpath string) error {
	f.record("rename " + filepath.Base(oldpath) + " -> " + filepath.Base(newpath))
	return f.FS.Rename(oldpath, newpath)
}

func (f *markerRecordingFS) Remove(name string) error {
	f.record("remove " + filepath.Base(name))
	return f.FS.Remove(name)
}

type markerRecordingFile struct {
	File
	name string
	fs   *markerRecordingFS
}

func (f *markerRecordingFile) Write(p []byte) (int, error) {
	f.fs.record("write " + f.name)
	if f.fs.failWriteOn != "" && f.fs.failWriteOn == f.name {
		return 0, errors.New("simulated ENOSPC during marker write")
	}
	return f.File.Write(p)
}

func (f *markerRecordingFile) Sync() error {
	f.fs.record("sync " + f.name)
	return f.File.Sync()
}

func (f *markerRecordingFile) SyncMetadata() error {
	f.fs.record("syncmeta " + f.name)
	if sm, ok := f.File.(FileMetadataSyncer); ok {
		return sm.SyncMetadata()
	}
	return f.File.Sync()
}

// TestWriteWALFormat_PublishesByRenameAndSyncs: write and fsync a temp file, rename it into place, fsync the directory last.
func TestWriteWALFormat_PublishesByRenameAndSyncs(t *testing.T) {
	dir := t.TempDir()
	ffs := newMarkerRecordingFS()

	if err := writeWALFormat(ffs, dir, 4096, false); err != nil {
		t.Fatalf("writeWALFormat: %v", err)
	}

	ops := ffs.recorded()
	want := []string{
		"create " + walFormatTempFilename,
		"write " + walFormatTempFilename,
		"sync " + walFormatTempFilename,
		"rename " + walFormatTempFilename + " -> " + walFormatFilename,
		"syncmeta " + filepath.Base(dir),
	}
	if len(ops) != len(want) {
		t.Fatalf("marker publication performed %v, want exactly %v", ops, want)
	}
	for i := range want {
		if ops[i] != want[i] {
			t.Fatalf("marker publication step %d = %q, want %q (full sequence %v)",
				i, ops[i], want[i], ops)
		}
	}

	// The published marker is the real one, and no temp file survives.
	if blockSize, present, err := readWALFormat(defaultFS(), dir); err != nil ||
		!present || blockSize != 4096 {
		t.Fatalf("published marker = (%d, %v, %v), want (4096, true, nil)",
			blockSize, present, err)
	}
	if _, err := os.Stat(filepath.Join(dir, walFormatTempFilename)); !os.IsNotExist(err) {
		t.Fatalf("temporary marker survived publication: %v", err)
	}
}

// TestWriteWALFormat_FailedWriteLeavesNothingBehind: a stamp failing on ENOSPC or EIO leaves the directory as it was.
func TestWriteWALFormat_FailedWriteLeavesNothingBehind(t *testing.T) {
	dir := t.TempDir()
	ffs := newMarkerRecordingFS()
	ffs.failWriteOn = walFormatTempFilename

	if err := writeWALFormat(ffs, dir, 4096, false); err == nil {
		t.Fatal("writeWALFormat reported success despite a failing write")
	}

	if _, err := os.Stat(filepath.Join(dir, walFormatFilename)); !os.IsNotExist(err) {
		t.Fatalf("a failed stamp published a marker: %v; every subsequent Open "+
			"would fail on it", err)
	}
	if _, err := os.Stat(filepath.Join(dir, walFormatTempFilename)); !os.IsNotExist(err) {
		t.Fatalf("a failed stamp left its temporary file behind: %v", err)
	}

	// A directory a failed stamp touched is still a directory that opens.
	blockSize, present, err := readWALFormat(defaultFS(), dir)
	if err != nil {
		t.Fatalf("readWALFormat after a failed stamp: %v", err)
	}
	if present || blockSize != 0 {
		t.Fatalf("readWALFormat after a failed stamp = (%d, %v), want absent",
			blockSize, present)
	}
}

// TestOpen_FailedStampFailsTheOpen: a marker that cannot be written fails Open and leaves the directory empty.
func TestOpen_FailedStampFailsTheOpen(t *testing.T) {
	dir := t.TempDir()
	ffs := newMarkerRecordingFS()
	ffs.failWriteOn = walFormatTempFilename

	db, err := Open(dir, WithFS(ffs), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err == nil {
		db.Close()
		t.Fatal("Open succeeded although the format marker could not be written; " +
			"the segments it goes on to create have no recorded framing, and the " +
			"next open refuses the directory for good")
	}

	// Nothing may have been created: the directory must be reusable once the
	// filesystem problem is fixed, not half-populated with unmarked segments.
	if n := countSegmentFiles(t, dir); n != 0 {
		t.Fatalf("a failed stamp left %d segment file(s) behind; those bytes now "+
			"record no on-disk format and make the directory permanently unopenable", n)
	}
	if _, present, rerr := readWALFormat(defaultFS(), dir); rerr != nil || present {
		t.Fatalf("marker after a failed stamp = (present %v, err %v), want (false, nil)",
			present, rerr)
	}

	// With the filesystem healthy the same directory opens and stamps normally.
	db, err = Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open after the stamp failure was resolved: %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 7, Vote: 3, Commit: 1}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err = Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	st, _, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if st.Term != 7 || st.Vote != 3 {
		t.Fatalf("state = %+v, want Term 7 Vote 3", st)
	}
}

// countSegmentFiles totals the segment files across every shard directory.
func countSegmentFiles(t *testing.T, dir string) int {
	t.Helper()
	total := 0
	for i := 0; i < DefaultShardCount; i++ {
		entries, err := os.ReadDir(filepath.Join(dir, shardDirName(i)))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("read shard dir: %v", err)
		}
		for _, de := range entries {
			if _, ok := parseSegmentFilename(de.Name()); ok {
				total++
			}
		}
	}
	return total
}

// TestOpen_FailedOpensDoNotAccumulateEmptySegments: a failed open removes the empty active segments it created.
func TestOpen_FailedOpensDoNotAccumulateEmptySegments(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()

	// Seed several shards so a failure in a later one leaves earlier ones
	// already opened, each with a freshly created active segment.
	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for shardID := uint64(0); shardID <= 4; shardID++ {
		if err := db.SaveNodeState(shardID, 1, logdb.State{Term: 4, Vote: 2, Commit: 1}); err != nil {
			t.Fatalf("SaveNodeState(%d): %v", shardID, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	before := countSegmentFiles(t, dir)

	// Fail the open of shard 4, after shards 0..3 have recovered and each
	// created an active segment.
	for i := 0; i < 5; i++ {
		ff := &shardOpenFailFS{FS: defaultFS(), failDir: shardDirName(4)}
		ff.armed.Store(true)
		db, err := Open(dir, WithFS(ff), WithBlockSize(blockSize), WithMaxFileSize(64*1024))
		if err == nil {
			db.Close()
			t.Fatal("fixture: Open succeeded despite a failing shard")
		}
	}

	if after := countSegmentFiles(t, dir); after != before {
		t.Fatalf("5 failed opens left %d extra segment files behind (%d -> %d); each "+
			"failed open leaks one empty segment per already-recovered shard",
			after-before, before, after)
	}

	// The directory still opens, with its data intact.
	db, err = Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open after failed opens: %v", err)
	}
	defer db.Close()
	if st, _, err := db.ReadState(3, 1); err != nil || st.Term != 4 {
		t.Fatalf("ReadState = (%+v, %v), want Term 4", st, err)
	}
}

// TestOpen_NoSyncDirectoryIsStamped covers the geometry gap a lazily created
// root would leave open. Under noSync the WAL root is not created at Open, so
// without creating it for the stamp the directory would spend its entire first
// life without a marker — exactly the window in which a changed block size produces
// the misparse the marker exists to name.
func TestOpen_NoSyncDirectoryIsStamped(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")

	db, err := Open(dir, WithNoSync(true), WithBlockSize(2048), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	blockSize, present, err := readWALFormat(defaultFS(), dir)
	if err != nil {
		t.Fatalf("readWALFormat: %v", err)
	}
	if !present || blockSize != 2048 {
		t.Fatalf("marker for a fresh noSync directory = (%d, %v), want (2048, true)",
			blockSize, present)
	}

	// Shard directories stay lazy: only the root was created.
	if _, err := os.Stat(filepath.Join(dir, shardDirName(0))); !os.IsNotExist(err) {
		t.Fatalf("stamping the geometry created a shard directory: %v", err)
	}

	// And the guard is live from the first open onward.
	if db, err := Open(dir, WithNoSync(true), WithBlockSize(4096)); !errors.Is(err, ErrWALGeometryMismatch) {
		if err == nil {
			db.Close()
		}
		t.Fatalf("Open with a changed block size = %v, want ErrWALGeometryMismatch", err)
	}
}
