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

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
)

// snapshotFile is the open-handle surface the snapshot persistence paths use.
// Seek exists for the frame-epoch prescan in ReencryptSnapshotFile, Stat for
// the send path's declared-size check, and Name to recover a CreateTemp path.
//
// It is the engine's surface, aliased rather than restated, so snapshotFS
// satisfies engine.ScratchFS and the host's recovery and send create their
// anonymous decrypt scratch through the one engine.CreateAnonymousScratch.
type snapshotFile = engine.SnapshotFile

// snapshotFS abstracts exactly the filesystem operations the snapshot
// persistence paths perform: receive/persist, save, export, import,
// re-encrypt, and decrypt-for-send. Production always runs on osSnapshotFS,
// which delegates to the os package; tests substitute an implementation that
// injects errors and models power loss, so the crash-safety sequence
// (temp -> fsync -> rename -> directory fsync) is verified rather than
// assumed.
//
// This is deliberately a SECOND, separate abstraction from waldb.FS and must
// not be unified with it: waldb.FS belongs to one storage backend and carries
// operations that backend needs (ReadDir, PreAllocate) while lacking the ones
// here (CreateTemp, Rename, RemoveAll), and the public host layer must not
// take a dependency on a specific logdb backend's internals. Two narrow
// consumer-defined interfaces for two different consumers is the intent.
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

// MkdirTemp creates a new directory in dir whose name is guaranteed not to
// collide with any existing one, and returns its path.
//
// The GUARANTEE is the point: unlike MkdirAll, which succeeds on a directory
// that already exists, this fails rather than hand back a directory holding
// someone else's data. Every path that writes a snapshot artifact staged for a
// rename depends on it, because such a path is entitled to delete what it
// created on failure.
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
	d, err := fsys.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// syncDirChain fsyncs leaf and every ancestor up to and including stopAt
// through fsys, applying engine.SyncDirChain's walk to the injected
// filesystem.
func syncDirChain(fsys snapshotFS, leaf, stopAt string) error {
	return engine.SyncDirChainWith(func(dir string) error {
		return syncDir(fsys, dir)
	}, leaf, stopAt)
}

// deepestExistingAncestor returns the deepest ancestor of path that already
// exists on fsys (path itself when it exists). Recorded BEFORE a MkdirAll, it
// is the stopAt the following syncDirChain must use: the fsync has to cover
// every directory the MkdirAll creates and no directory above them. It applies
// engine.DeepestExistingAncestor's walk to the injected filesystem.
func deepestExistingAncestor(fsys snapshotFS, path string) string {
	return engine.DeepestExistingAncestorWith(fsys.Stat, path)
}

// mkdirAllSynced creates dir and any missing parents, then fsyncs the whole
// created chain so the new directory entries are durable. A directory that
// holds durably-written files is itself only durable once its parent's entry
// is fsynced; without this a crash can lose the directory the WAL lives in.
func mkdirAllSynced(dir string) error {
	fsys := osSnapshotFS{}
	syncRoot := deepestExistingAncestor(fsys, dir)
	if err := fsys.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	return syncDirChain(fsys, dir, syncRoot)
}
