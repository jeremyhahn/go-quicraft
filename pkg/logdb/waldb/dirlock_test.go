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

//go:build unix || windows

package waldb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/fsutil"
)

// TestOpen_SecondOpenerRefused verifies a WAL directory has one writer: a
// second Open while the first is live fails with fsutil.ErrDirLocked, and
// succeeds once the first is closed.
func TestOpen_SecondOpenerRefused(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := Open(dir); !errors.Is(err, fsutil.ErrDirLocked) {
		db.Close()
		t.Fatalf("second Open = %v, want fsutil.ErrDirLocked", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestOpen_FailedOpenReleasesLock verifies an Open that fails after taking
// the lock releases it, so the directory is not held by a DB that does not
// exist.
func TestOpen_FailedOpenReleasesLock(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	registry := filepath.Join(dir, retiredFilename)
	if err := os.WriteFile(registry, []byte("not a registry"), 0o600); err != nil {
		t.Fatalf("write registry: %v", err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrRetiredRegistryMalformed) {
		t.Fatalf("Open over a malformed registry = %v, want ErrRetiredRegistryMalformed", err)
	}
	l, err := fsutil.LockDir(dir, walLockFilename)
	if err != nil {
		t.Fatalf("the failed Open left the directory locked: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestOpen_RefusedOpenerLeavesFormatUntouched verifies the lock is taken
// before the format marker is read or recorded. An opener refused on an
// empty directory, which another opener holds and has not yet stamped, must
// write no marker: recording one before the lock check would let it overwrite
// the holder's marker, with its own block size, under the holder's segments.
func TestOpen_RefusedOpenerLeavesFormatUntouched(t *testing.T) {
	dir := t.TempDir()
	held, err := fsutil.LockDir(dir, walLockFilename)
	if err != nil {
		t.Fatalf("LockDir: %v", err)
	}
	defer held.Close()
	if db, err := Open(dir); !errors.Is(err, fsutil.ErrDirLocked) {
		if db != nil {
			db.Close()
		}
		t.Fatalf("Open of a held directory = %v, want fsutil.ErrDirLocked", err)
	}
	if _, err := os.Stat(filepath.Join(dir, walFormatFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the refused Open touched the format marker: stat = %v, want not exist", err)
	}
}

// TestOpen_GeometryRefusalReleasesLock verifies an Open refused for a block
// size the directory was not written with, a check made under the lock,
// releases the lock.
func TestOpen_GeometryRefusalReleasesLock(t *testing.T) {
	const otherBlockSize = 4096
	if DefaultOptions.BlockSize == otherBlockSize {
		t.Fatalf("otherBlockSize must differ from the default %d", DefaultOptions.BlockSize)
	}
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var geoErr *WALGeometryMismatchError
	if _, err := Open(dir, WithBlockSize(otherBlockSize)); !errors.As(err, &geoErr) {
		t.Fatalf("Open with another block size = %v, want a *WALGeometryMismatchError", err)
	}
	l, err := fsutil.LockDir(dir, walLockFilename)
	if err != nil {
		t.Fatalf("the refused Open left the directory locked: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
