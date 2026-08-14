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
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// syncAccountingFS delegates every call to the REAL production filesystem and
// records which paths were flushed, how, and in what order.
//
// It exists because "the WAL flushed before it acknowledged" and "the WAL
// flushed the DIRECTORY with the call that carries metadata" are the two
// durability facts nothing else in this package can observe. crashFS models
// the consequences of a missing flush but is entirely in memory, and the
// error-injection doubles can only intercept a call that FAILS — a flush that
// was never issued produces no failing call to intercept.
//
// Sync and SyncMetadata are counted separately and never conflated. On Linux
// File.Sync is fdatasync, which is specified to skip metadata the data does
// not need; a directory's metadata IS its content, so a directory flushed with
// Sync is not durable on every filesystem even though it happens to be on
// ext4. That difference is invisible in the return value and visible here.
//
// What it cannot observe is whether osFile.Sync issues a syscall at all: it
// counts calls at the File interface, one level ABOVE the implementation, so
// an osFile.Sync stubbed to return nil still increments these counters. That
// gap is closed by TestOSFileSyncIssuesRealSyscall in fs_sync_test.go, and the
// two together are what make a deleted fsync detectable.
type syncAccountingFS struct {
	FS

	mu sync.Mutex
	// seq is a monotonic operation number, used to order a directory flush
	// against the rename it is supposed to make durable.
	seq int
	// dataSync counts File.Sync calls per path.
	dataSync map[string]int
	// metaSync counts File.SyncMetadata calls per path.
	metaSync map[string]int
	// lastMetaSync is the operation number of the most recent SyncMetadata per
	// path.
	lastMetaSync map[string]int
	// renames records the operation number of each successful rename, keyed by
	// destination path.
	renames map[string]int
}

// errNoMetadataSync reports a delegate that cannot distinguish a metadata sync.
// The production osFile implements FileMetadataSyncer, so a delegate without it
// is a broken test setup, not a case worth modelling: silently accepting it
// would let syncDirFS fall back to Sync and the assertions below would then be
// about the fallback rather than about production.
var errNoMetadataSync = errors.New("waldb: syncAccountingFS delegate does not implement FileMetadataSyncer")

// errNoPreAllocate reports a delegate that cannot pre-allocate. Same rationale
// as errNoMetadataSync: the production osFile can, and hiding the capability
// would send segment creation down a path production never takes.
var errNoPreAllocate = errors.New("waldb: syncAccountingFS delegate does not implement FilePreAllocator")

func newSyncAccountingFS() *syncAccountingFS {
	return &syncAccountingFS{
		FS:           defaultFS(),
		dataSync:     make(map[string]int),
		metaSync:     make(map[string]int),
		lastMetaSync: make(map[string]int),
		renames:      make(map[string]int),
	}
}

// Open implements FS.
func (f *syncAccountingFS) Open(name string) (File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return f.wrap(file, name)
}

// OpenFile implements FS.
func (f *syncAccountingFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	file, err := f.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return f.wrap(file, name)
}

// Rename implements FS, recording when the rename happened so a later
// directory flush can be proven to follow it.
func (f *syncAccountingFS) Rename(oldpath, newpath string) error {
	if err := f.FS.Rename(oldpath, newpath); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	f.renames[newpath] = f.seq
	return nil
}

// wrap returns an accounting handle, refusing a delegate that does not carry
// the optional capabilities the production file has.
func (f *syncAccountingFS) wrap(file File, name string) (File, error) {
	if _, ok := file.(FileMetadataSyncer); !ok {
		return nil, errNoMetadataSync
	}
	if _, ok := file.(FilePreAllocator); !ok {
		return nil, errNoPreAllocate
	}
	return &syncAccountingFile{File: file, name: name, fs: f}, nil
}

// syncCount returns how many times path was flushed with Sync (fdatasync).
func (f *syncAccountingFS) syncCount(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dataSync[path]
}

// metaSyncCount returns how many times path was flushed with SyncMetadata.
func (f *syncAccountingFS) metaSyncCount(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.metaSync[path]
}

// metaSyncFollowedRename reports whether path was metadata-synced AFTER
// something was renamed onto renamed, and whether the rename happened at all.
func (f *syncAccountingFS) metaSyncFollowedRename(dir, renamed string) (renamed_ bool, synced bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	at, ok := f.renames[renamed]
	if !ok {
		return false, false
	}
	return true, f.lastMetaSync[dir] > at
}

// syncAccountingFile counts the flushes of one open handle. It carries both
// optional capabilities because wrap refuses any delegate that does not.
type syncAccountingFile struct {
	File
	name string
	fs   *syncAccountingFS
}

// Sync implements File.
func (f *syncAccountingFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	f.fs.seq++
	f.fs.dataSync[f.name]++
	return nil
}

// SyncMetadata implements FileMetadataSyncer.
func (f *syncAccountingFile) SyncMetadata() error {
	if err := f.File.(FileMetadataSyncer).SyncMetadata(); err != nil {
		return err
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	f.fs.seq++
	f.fs.metaSync[f.name]++
	f.fs.lastMetaSync[f.name] = f.fs.seq
	return nil
}

// PreAllocate implements FilePreAllocator.
func (f *syncAccountingFile) PreAllocate(size int64) error {
	return f.File.(FilePreAllocator).PreAllocate(size)
}

// activeSegmentFile returns the highest-numbered segment file in a shard
// directory, which is the one a write just landed in.
func activeSegmentFile(t *testing.T, shardDir string) string {
	t.Helper()
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		t.Fatalf("read shard dir %s: %v", shardDir, err)
	}
	var ids []uint64
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		if id, ok := parseSegmentFilename(de.Name()); ok {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		t.Fatalf("shard dir %s holds no segment file", shardDir)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	return filepath.Join(shardDir, segmentFilename(ids[0]))
}

// TestSaveEntries_SyncsTheSegmentBeforeAcknowledging asserts the flush that
// makes an acknowledgement true actually happens, and happens on the file the
// record was written to.
//
// SaveEntries returning nil is a promise that the entries survive a power
// loss. Nothing else in this package checks that the promise is backed by a
// flush of the RIGHT file: the crash model can only show the consequence of a
// missing flush in a simulated filesystem, and the error-injection suites can
// only intercept flushes that are attempted.
func TestSaveEntries_SyncsTheSegmentBeforeAcknowledging(t *testing.T) {
	const (
		shardID   = uint64(5)
		replicaID = uint64(1)
	)

	dir := filepath.Join(t.TempDir(), "wal")
	ffs := newSyncAccountingFS()

	// Real fsyncs: WithNoSync would legitimately remove every flush and make
	// the assertion below a statement about the option, not about durability.
	db, err := Open(dir, WithFS(ffs), WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.SaveEntries(shardID, replicaID, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	shardDir := filepath.Join(dir, shardDirName(int(shardID%DefaultShardCount)))
	segment := activeSegmentFile(t, shardDir)

	if got := ffs.syncCount(segment); got == 0 {
		t.Fatalf("SaveEntries acknowledged %s without ever flushing it; the entries it "+
			"promised are durable live only in the page cache", segment)
	}
}

// TestOpen_MetadataSyncsTheDirectoryTheFormatMarkerWasRenamedInto asserts the
// second half of the atomic-publication sequence.
//
// The WAL format marker is published by rename precisely so a reader can never
// see a half-written marker, and a rename is a DIRECTORY metadata change: it
// is durable only once the directory holding it is flushed with the call that
// carries metadata. fdatasync is specified to skip metadata the file's data
// does not need, so flushing the directory with Sync leaves the publication
// unproven on any filesystem that takes that liberty — and the return value is
// identical either way, which is exactly why this has to be asserted on the
// call and not on the outcome.
func TestOpen_MetadataSyncsTheDirectoryTheFormatMarkerWasRenamedInto(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	ffs := newSyncAccountingFS()

	db, err := Open(dir, WithFS(ffs), WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	marker := filepath.Join(dir, walFormatFilename)
	renamed, syncedAfter := ffs.metaSyncFollowedRename(dir, marker)
	if !renamed {
		t.Fatalf("the format marker was never renamed into %s; it was published by some "+
			"other construction, which can expose a partially written marker", dir)
	}
	if !syncedAfter {
		t.Fatalf("%s was not metadata-synced after the marker was renamed into it "+
			"(SyncMetadata count %d); the publication is not durable",
			dir, ffs.metaSyncCount(dir))
	}
	if got := ffs.syncCount(dir); got != 0 {
		t.Fatalf("the WAL directory %s was flushed with Sync %d times; a directory must be "+
			"flushed with SyncMetadata, because fdatasync may skip exactly the dirent "+
			"changes the flush exists to persist", dir, got)
	}
}

// TestEnsureActiveSegment_FsyncsShardDirParent is the M1 regression test. When a
// shard directory is lazily created on first write, its PARENT (the WAL root)
// must be fsynced so the shard directory's own entry is durable — otherwise a
// crash on a non-journaled filesystem can lose the entire shard directory even
// though SaveState fsynced the segment and returned success.
//
// The flush must be a METADATA sync: a dirent create is metadata, and it is
// what fdatasync is permitted to leave behind.
func TestEnsureActiveSegment_FsyncsShardDirParent(t *testing.T) {
	dir := t.TempDir() + "/wal"
	ffs := newSyncAccountingFS()

	// Real fsync (no WithNoSync) so the durability fsyncs actually run.
	db, err := Open(dir, WithFS(ffs), WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// First write to shard 5 lazily creates <dir>/shard-5 and must fsync its
	// parent, <dir> (the WAL root).
	if err := db.SaveEntries(5, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	if ffs.metaSyncCount(dir) == 0 {
		t.Fatalf("WAL root %q was never metadata-synced; the shard directory entry is not durable", dir)
	}
	if got := ffs.syncCount(dir); got != 0 {
		t.Fatalf("WAL root %q was flushed with Sync %d times instead of SyncMetadata", dir, got)
	}
}
