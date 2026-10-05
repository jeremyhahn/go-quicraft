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
	Sync() error
	Close() error
}

// MetadataSyncer is implemented by a Dir whose Sync deliberately omits
// metadata, for example one that uses fdatasync on Linux. A directory's
// content IS metadata (the entries created, removed and renamed in it), so
// SyncDirWith uses SyncMetadata when a handle provides it and Sync only when
// Sync already covers metadata, as it does for *os.File.
type MetadataSyncer interface {
	SyncMetadata() error
}

// FS is the filesystem surface MkdirAllSyncedWith needs to find the deepest
// existing ancestor and create the tree.
type FS interface {
	Stat(name string) (fs.FileInfo, error)
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

// SyncDirWith opens dir with open, flushes it and closes it. It flushes with
// SyncMetadata when the handle implements MetadataSyncer, and with Sync
// otherwise. The handle is closed even when the flush fails, and the flush
// error takes precedence over the close error. Errors are returned unwrapped.
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

// DeepestExistingAncestor returns the deepest ancestor of path that already
// exists, or path itself when it exists. Record it BEFORE an os.MkdirAll(path)
// and pass it as the stopAt of the SyncDirChain that follows: the chain then
// covers every directory the MkdirAll creates, plus the existing ancestor that
// holds the entry for the first of them, and nothing above that.
func DeepestExistingAncestor(path string) string {
	return DeepestExistingAncestorWith(os.Stat, path)
}

// DeepestExistingAncestorWith performs DeepestExistingAncestor's walk with the
// supplied stat in place of os.Stat.
//
// Only a nil error from stat counts as present. Any other error, including
// one other than fs.ErrNotExist, leaves that level's existence unknown, so the
// walk continues upward. That makes the chain sync cover more directories,
// never fewer: fsyncing a directory that already existed costs one extra
// fsync, while skipping one that MkdirAll created loses its entry on a crash.
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

// SyncDirChain fsyncs leaf and each ancestor directory up to and including
// stopAt. Call it after os.MkdirAll(leaf): each directory that call may have
// created needs its parent's entry made durable, so the whole chain from the
// deepest new directory to the pre-existing ancestor must be synced. When
// stopAt is empty, or leaf is not under stopAt, only leaf is synced.
func SyncDirChain(leaf, stopAt string) error {
	return SyncDirChainWith(SyncDir, leaf, stopAt)
}

// SyncDirChainWith performs SyncDirChain's walk, fsyncing each directory with
// syncDir in place of SyncDir. It stops at the first error and returns it.
//
// Containment is decided with filepath.Rel, NOT a string prefix. The prefix
// form fails for exactly the two values DeepestExistingAncestor returns when
// nothing above the leaf already exists: stopAt "/" builds the prefix "//" and
// stopAt "." builds "./", and a cleaned leaf starts with neither. Both quietly
// fell back to fsyncing only the leaf, skipping every ancestor MkdirAll had
// just created. Rel handles both roots and reports a leaf outside stopAt as a
// ".." prefix.
//
// The emptiness test runs BEFORE Clean because filepath.Clean("") is ".", which
// is a legitimate stopAt (the working directory) rather than "no stop point".
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

// MkdirAllSynced creates path and any missing parents with perm, then fsyncs
// each directory it created and the pre-existing parent of the topmost one, so
// every new directory entry is durable. For an existing path it fsyncs path
// alone.
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
