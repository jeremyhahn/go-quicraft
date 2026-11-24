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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrDirLocked is returned, wrapped with the lock file's path, when a
// directory is already held by another opener: another process, or another
// open in this one. Two writers over one data directory interleave their
// records and corrupt both, so the second is refused rather than queued.
var ErrDirLocked = errors.New("fsutil: directory is locked by another opener")

// LockFilePerms is the mode a lock file is created with.
const LockFilePerms = 0o600

// DirLock is an exclusive lock on a directory, held through a lock file
// inside it until Close. The lock is advisory and taken on an open file
// description (flock on unix, LockFileEx on Windows), so it is refused to a
// second open in the same process as well as to another process, and the
// kernel drops it when the holder dies: a crash never leaves a stale lock.
//
// The lock file is never removed. Removing it on Close would let an opener
// that opened the old name lock an unlinked inode while a third creates and
// locks a new one, and both would believe they hold the directory.
type DirLock struct {
	path string
	once sync.Once
	f    *os.File
	err  error
}

// LockDir takes the exclusive lock on dir through the file name inside it,
// creating the file (mode LockFilePerms) when absent and fsyncing dir so the
// created entry is durable. dir must exist. A directory already held fails
// immediately with an error wrapping ErrDirLocked and naming the path; it
// never waits.
func LockDir(dir, name string) (*DirLock, error) {
	path := filepath.Join(dir, name)
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, LockFilePerms)
	if err != nil {
		return nil, fmt.Errorf("fsutil: open lock file %s: %w", path, err)
	}
	if err := lockFile(f); err != nil {
		closeErr := f.Close()
		if errors.Is(err, ErrDirLocked) {
			return nil, fmt.Errorf("%w: %s", ErrDirLocked, path)
		}
		return nil, errors.Join(fmt.Errorf("fsutil: lock %s: %w", path, err), closeErr)
	}
	if created {
		if err := SyncDir(dir); err != nil {
			return nil, errors.Join(fmt.Errorf("fsutil: sync %s after creating its lock file: %w", dir, err), f.Close())
		}
	}
	return &DirLock{path: path, f: f}, nil
}

// Path returns the lock file's path.
func (l *DirLock) Path() string { return l.path }

// Close releases the lock. It is idempotent and safe on a nil lock, so every
// exit path of an opener can release without tracking whether it already did.
func (l *DirLock) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		// Closing the descriptor releases the lock with it.
		l.err = l.f.Close()
	})
	return l.err
}
