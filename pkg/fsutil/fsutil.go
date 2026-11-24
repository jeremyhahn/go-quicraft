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

package fsutil

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Dir is the handle SyncDirWith needs from an opened directory.
type Dir interface {
	// Sync flushes the directory to stable storage.
	Sync() error
	// Close releases the directory handle.
	Close() error
}

// MetadataSyncer is a Dir whose Sync omits metadata; SyncDirWith uses SyncMetadata when present.
type MetadataSyncer interface {
	// SyncMetadata flushes the directory, metadata included, to stable
	// storage.
	SyncMetadata() error
}

// FS is the filesystem surface MkdirAllSyncedWith needs to find the deepest
// existing ancestor and create the tree.
type FS interface {
	// Stat returns the file info of the named file, as os.Stat does.
	Stat(name string) (fs.FileInfo, error)
	// MkdirAll creates the directory path and any missing parents with
	// the given permissions, as os.MkdirAll does.
	MkdirAll(path string, perm fs.FileMode) error
}

// osFS implements FS with the os package.
type osFS struct{}

// Stat returns the FileInfo for the named file.
func (osFS) Stat(name string) (fs.FileInfo, error) { return os.Stat(name) }

// MkdirAll creates path and every missing parent directory.
func (osFS) MkdirAll(path string, perm fs.FileMode) error { return os.MkdirAll(path, perm) }

// SyncDir fsyncs dir so changes to its entries (file creates, deletes,
// renames) are durable. Without it, a crash on Linux ext4 can lose a new
// file's directory entry even though the file's own data was fsynced.
func SyncDir(dir string) error {
	return SyncDirWith(os.Open, dir)
}

// SyncDirWith opens, flushes and closes dir; the flush error takes precedence over the close error.
func SyncDirWith[D Dir](open func(string) (D, error), dir string) error {
	d, err := open(dir)
	if err != nil {
		return err
	}
	var syncErr error
	if ms, ok := any(d).(MetadataSyncer); ok {
		syncErr = ms.SyncMetadata()
	} else {
		syncErr = d.Sync()
	}
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// DeepestExistingAncestor returns the deepest existing ancestor of path; record it before MkdirAll and pass it to SyncDirChain.
func DeepestExistingAncestor(path string) string {
	return DeepestExistingAncestorWith(os.Stat, path)
}

// DeepestExistingAncestorWith walks with the supplied stat; only a nil error counts as present.
func DeepestExistingAncestorWith(stat func(string) (fs.FileInfo, error), path string) string {
	dir := path
	for {
		if _, err := stat(dir); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return dir
		}
		dir = parent
	}
}

// SyncDirChain fsyncs leaf and each ancestor up to stopAt; with no stopAt, or leaf outside it, only leaf.
func SyncDirChain(leaf, stopAt string) error {
	return SyncDirChainWith(SyncDir, leaf, stopAt)
}

// SyncDirChainWith walks with syncDir and stops at the first error; containment uses filepath.Rel.
func SyncDirChainWith(syncDir func(string) error, leaf, stopAt string) error {
	if stopAt == "" {
		return syncDir(filepath.Clean(leaf))
	}
	leaf = filepath.Clean(leaf)
	stopAt = filepath.Clean(stopAt)
	rel, err := filepath.Rel(stopAt, leaf)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		// Not under stopAt (or the two are not comparable, e.g. one absolute
		// and one relative): sync the leaf alone, which is all that is known
		// to exist along a chain this function cannot walk.
		return syncDir(leaf)
	}
	for dir := leaf; ; dir = filepath.Dir(dir) {
		if err := syncDir(dir); err != nil {
			return err
		}
		if dir == stopAt {
			return nil
		}
	}
}

// MkdirAllSynced creates path with perm and fsyncs every created directory and the topmost one's parent.
func MkdirAllSynced(path string, perm fs.FileMode) error {
	return MkdirAllSyncedWith(osFS{}, SyncDir, path, perm)
}

// MkdirAllSyncedWith performs MkdirAllSynced through fsys, fsyncing each
// directory with syncDir. The deepest existing ancestor is recorded before the
// MkdirAll, so the chain sync covers exactly the directories it created.
func MkdirAllSyncedWith(fsys FS, syncDir func(string) error, path string, perm fs.FileMode) error {
	stopAt := DeepestExistingAncestorWith(fsys.Stat, path)
	if err := fsys.MkdirAll(path, perm); err != nil {
		return err
	}
	return SyncDirChainWith(syncDir, path, stopAt)
}
