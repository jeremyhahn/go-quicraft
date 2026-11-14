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

package fsutil

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestLockFile_PersistsAfterClose pins the type doc: Close releases the lock
// but never removes the lock file.
func TestLockFile_PersistsAfterClose(t *testing.T) {
	dir := t.TempDir()
	l, err := LockDir(dir, lockName)
	if err != nil {
		t.Fatalf("LockDir: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(l.Path()); err != nil {
		t.Fatalf("stat lock file after Close: %v, want it kept", err)
	}
}

// TestLockFile_RelockSameFile checks that a relock in the same process after
// Close succeeds on the same file, and that the relock is again exclusive.
func TestLockFile_RelockSameFile(t *testing.T) {
	dir := t.TempDir()
	first, err := LockDir(dir, lockName)
	if err != nil {
		t.Fatalf("first LockDir: %v", err)
	}
	// Take identity from the lock handle, not the path: on Windows a
	// path-based FileInfo resolves identity lazily, so comparing two
	// os.Stat results after Close would check the second file against itself.
	fi1, err := first.f.Stat()
	if err != nil {
		t.Fatalf("stat while first lock held: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	second, err := LockDir(dir, lockName)
	if err != nil {
		t.Fatalf("relock after Close: %v, want nil", err)
	}
	defer second.Close()
	fi2, err := second.f.Stat()
	if err != nil {
		t.Fatalf("stat while second lock held: %v", err)
	}
	if !os.SameFile(fi1, fi2) {
		t.Errorf("relock locked a different file than the first lock")
	}

	third, err := LockDir(dir, lockName)
	if err == nil {
		_ = third.Close()
		t.Fatal("third LockDir while the relock is held succeeded, want ErrDirLocked")
	}
	if !errors.Is(err, ErrDirLocked) {
		t.Fatalf("third LockDir error = %v, want ErrDirLocked", err)
	}
}

// TestLockFile_KeepsExistingContentAndMode checks that LockDir opens an
// existing lock file without truncating it or changing its mode.
func TestLockFile_KeepsExistingContentAndMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, lockName)
	if err := os.WriteFile(path, []byte("x"), 0o640); err != nil {
		t.Fatalf("pre-create lock file: %v", err)
	}
	// os.WriteFile applies the umask; set the mode explicitly.
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatalf("chmod pre-created lock file: %v", err)
	}
	l, err := LockDir(dir, lockName)
	if err != nil {
		t.Fatalf("LockDir: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Read only after Close: LockFileEx blocks reads of a locked file on Windows.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read lock file: %v", err)
	}
	if string(got) != "x" {
		t.Errorf("lock file content = %q, want %q", got, "x")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat lock file after Close: %v", err)
	}
	if os.PathSeparator == '/' {
		if perm := after.Mode().Perm(); perm != 0o640 {
			t.Errorf("lock file mode = %04o, want 0640", perm)
		}
	}
}
