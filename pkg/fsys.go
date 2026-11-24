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

package quicraft

import (
	"io/fs"
	"os"

	"github.com/jeremyhahn/go-quicraft/pkg/fsutil"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
)

// snapshotFile is the open-handle surface of the snapshot persistence paths, aliased from the engine.
type snapshotFile = engine.SnapshotFile

// snapshotFS is the filesystem surface of the snapshot persistence paths; tests inject errors and power loss. It is separate from waldb.FS by design.
type snapshotFS interface {
	OpenFile(name string, flag int, perm fs.FileMode) (snapshotFile, error)
	Open(name string) (snapshotFile, error)
	CreateTemp(dir, pattern string) (snapshotFile, error)
	MkdirAll(path string, perm fs.FileMode) error
	MkdirTemp(dir, pattern string) (string, error)
	Rename(oldpath, newpath string) error
	Remove(name string) error
	RemoveAll(path string) error
	Stat(name string) (fs.FileInfo, error)
}

// osSnapshotFS implements snapshotFS against the real filesystem. Its zero
// value is ready for use and is the default for every Host.
type osSnapshotFS struct{}

// OpenFile opens the named file with the given flags and permissions.
func (osSnapshotFS) OpenFile(name string, flag int, perm fs.FileMode) (snapshotFile, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Open opens the named file for reading.
func (osSnapshotFS) Open(name string) (snapshotFile, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// CreateTemp creates a new temporary file in dir from pattern.
func (osSnapshotFS) CreateTemp(dir, pattern string) (snapshotFile, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// MkdirAll creates path and every missing parent directory.
func (osSnapshotFS) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(path, perm)
}

// MkdirTemp creates a new, uniquely named directory in dir and returns its path; it never returns an existing directory.
func (osSnapshotFS) MkdirTemp(dir, pattern string) (string, error) {
	// The directories made here are staging directories that are published
	// by rename, so they get the snapshot tree's mode, not MkdirTemp's 0700.
	return engine.MkdirStageDir(dir, pattern)
}

// Rename renames oldpath to newpath.
func (osSnapshotFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// Remove removes the named file.
func (osSnapshotFS) Remove(name string) error { return os.Remove(name) }

// RemoveAll removes path and everything it contains.
func (osSnapshotFS) RemoveAll(path string) error { return os.RemoveAll(path) }

// Stat returns the FileInfo for the named file.
func (osSnapshotFS) Stat(name string) (fs.FileInfo, error) { return os.Stat(name) }

// snapshotFsys returns the filesystem the Host's snapshot paths use. A Host
// with no injected filesystem — including a zero-value one built in a test —
// runs on the real one.
func (h *Host) snapshotFsys() snapshotFS {
	if h == nil || h.fsys == nil {
		return osSnapshotFS{}
	}
	return h.fsys
}

// snapshotFsys returns the filesystem the sender's snapshot paths use.
func (s *transportSender) snapshotFsys() snapshotFS {
	if s.fsys == nil {
		return osSnapshotFS{}
	}
	return s.fsys
}

// syncDir fsyncs a directory through fsys so its entry changes (file creates,
// renames, removals) become durable.
func syncDir(fsys snapshotFS, dir string) error {
	return fsutil.SyncDirWith(fsys.Open, dir)
}

// syncDirChain fsyncs leaf and every ancestor up to and including stopAt
// through fsys. stopAt is the fsutil.DeepestExistingAncestorWith(fsys.Stat, ...)
// recorded before the MkdirAll that created leaf.
func syncDirChain(fsys snapshotFS, leaf, stopAt string) error {
	return fsutil.SyncDirChainWith(func(dir string) error {
		return syncDir(fsys, dir)
	}, leaf, stopAt)
}
