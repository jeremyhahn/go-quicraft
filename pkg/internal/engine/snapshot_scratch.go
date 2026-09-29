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

package engine

import (
	"io/fs"
	"log/slog"
	"os"
)

// SnapshotFile is the open-handle surface the snapshot scratch paths use. It
// is the host's snapshot file surface too (the host aliases it), which is what
// lets one anonymous-scratch implementation serve the engine's recovery and
// the host's recovery and send.
type SnapshotFile interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Seek(offset int64, whence int) (int64, error)
	Stat() (fs.FileInfo, error)
	Name() string
	Sync() error
	Close() error
}

// ScratchFS is the pair of filesystem operations an anonymous scratch file
// needs. The host's snapshot filesystem satisfies it, so its fault-injecting
// test filesystems reach the scratch path unchanged.
type ScratchFS interface {
	CreateTemp(dir, pattern string) (SnapshotFile, error)
	Remove(name string) error
}

// osScratchFS implements ScratchFS against the real filesystem. Its zero value
// is ready for use and is the default for every snapshot pool.
type osScratchFS struct{}

// CreateTemp creates a new temporary file in dir from pattern.
func (osScratchFS) CreateTemp(dir, pattern string) (SnapshotFile, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Remove removes the named file.
func (osScratchFS) Remove(name string) error {
	return os.Remove(name)
}

// CreateAnonymousScratch creates a scratch file in dir from pattern and
// unlinks its name at once, returning the open handle. Plaintext decrypted
// into it therefore never has a name: a crash or power loss cannot leave it
// readable on disk, the snapshot GC cannot remove it from under a reader, and
// the caller's Close is the whole cleanup.
//
// It is the single implementation of that sequence, shared by the engine's
// snapshot recovery, the host's startup recovery and the host's transport
// send. The caller must write nothing into the file before this returns — the
// unlink is what makes it safe to fill.
//
// A failed unlink fails the call with a *ScratchUnlinkError
// (ErrScratchNotUnlinked) before a byte is written. A scratch is never handed
// out with a name: plaintext decrypted into it would be a complete, readable
// copy of the state machine that a SIGKILL or power loss before its Close
// leaves at rest. The handle is closed and the name removed once more, best
// effort; a name that survives that too is still EMPTY, and is counted on
// StaleDecryptSweepFailures and left to the decrypt-scratch sweep of the
// replica directory (SweepStaleDecryptTmpTree). dir must therefore be a
// directory that sweep covers — the replica's own snapshot directory
// (ReplicaSnapshotDir). The recovery or send that asked for the scratch fails
// with the error and is retried later.
func CreateAnonymousScratch(fsys ScratchFS, dir, pattern string) (SnapshotFile, error) {
	f, err := fsys.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	name := f.Name()
	rmErr := fsys.Remove(name)
	if rmErr == nil || os.IsNotExist(rmErr) {
		return f, nil
	}
	slog.Warn("snapshot: decrypt scratch file could not be unlinked; refusing to decrypt into it",
		"path", name,
		"error", rmErr,
	)
	if closeErr := f.Close(); closeErr != nil {
		slog.Debug("snapshot: decrypt scratch close failed after a refused unlink",
			"path", name,
			"error", closeErr,
		)
	}
	if retryErr := fsys.Remove(name); retryErr != nil && !os.IsNotExist(retryErr) {
		NoteScratchNotReclaimed(name, retryErr)
	}
	return nil, &ScratchUnlinkError{Path: name, Err: rmErr}
}
