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
	"sync"
	"testing"
)

// dirSyncTrackingFS records the paths whose file handle is fsynced, so a test
// can assert the parent-of-new-directory fsync that POSIX durability requires.
type dirSyncTrackingFS struct {
	FS
	mu     sync.Mutex
	synced map[string]int
}

func newDirSyncTrackingFS() *dirSyncTrackingFS {
	return &dirSyncTrackingFS{FS: defaultFS(), synced: make(map[string]int)}
}

func (f *dirSyncTrackingFS) Open(name string) (File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return &syncTrackingFile{File: file, name: name, fs: f}, nil
}

func (f *dirSyncTrackingFS) syncCount(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.synced[path]
}

type syncTrackingFile struct {
	File
	name string
	fs   *dirSyncTrackingFS
}

func (f *syncTrackingFile) Sync() error {
	f.fs.mu.Lock()
	f.fs.synced[f.name]++
	f.fs.mu.Unlock()
	return f.File.Sync()
}

// TestEnsureActiveSegment_FsyncsShardDirParent is the M1 regression test. When a
// shard directory is lazily created on first write, its PARENT (the WAL root)
// must be fsynced so the shard directory's own entry is durable — otherwise a
// crash on a non-journaled filesystem can lose the entire shard directory even
// though SaveState fsynced the segment and returned success.
func TestEnsureActiveSegment_FsyncsShardDirParent(t *testing.T) {
	dir := t.TempDir() + "/wal"
	ffs := newDirSyncTrackingFS()

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

	if ffs.syncCount(dir) == 0 {
		t.Fatalf("WAL root %q was never fsynced; the shard directory entry is not durable", dir)
	}
}
