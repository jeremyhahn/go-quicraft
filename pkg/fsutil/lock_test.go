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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	lockChildEnv = "FSUTIL_LOCK_CHILD_DIR"
	lockName     = "LOCK"
)

// TestMain lets the test binary act as the second opener of
// TestLockDir_RefusedToAnotherProcess: re-executed with lockChildEnv set, it
// tries the lock once and reports the outcome through its exit status.
func TestMain(m *testing.M) {
	if dir := os.Getenv(lockChildEnv); dir != "" {
		l, err := LockDir(dir, lockName)
		switch {
		case errors.Is(err, ErrDirLocked):
			os.Exit(3)
		case err != nil:
			os.Exit(4)
		}
		_ = l.Close()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestLockDir_RefusedInProcess(t *testing.T) {
	dir := t.TempDir()
	l, err := LockDir(dir, lockName)
	if err != nil {
		t.Fatalf("first LockDir: %v", err)
	}
	defer l.Close()

	_, err = LockDir(dir, lockName)
	if !errors.Is(err, ErrDirLocked) {
		t.Fatalf("second LockDir = %v, want ErrDirLocked", err)
	}
	path := filepath.Join(dir, lockName)
	if !strings.Contains(err.Error(), path) {
		t.Errorf("error %q does not name the lock file %s", err, path)
	}
	if l.Path() != path {
		t.Errorf("Path() = %q, want %q", l.Path(), path)
	}
}

func TestLockDir_RefusedToAnotherProcess(t *testing.T) {
	dir := t.TempDir()
	l, err := LockDir(dir, lockName)
	if err != nil {
		t.Fatalf("LockDir: %v", err)
	}
	child := func() int {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), lockChildEnv+"="+dir)
		err := cmd.Run()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		if err != nil {
			t.Fatalf("running the child: %v", err)
		}
		return 0
	}
	if code := child(); code != 3 {
		t.Fatalf("child opener exit status = %d while the lock is held, want 3 (ErrDirLocked)", code)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if code := child(); code != 0 {
		t.Fatalf("child opener exit status = %d after Close, want 0", code)
	}
}

func TestDirLock_CloseIsIdempotent(t *testing.T) {
	l, err := LockDir(t.TempDir(), lockName)
	if err != nil {
		t.Fatalf("LockDir: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	var nilLock *DirLock
	if err := nilLock.Close(); err != nil {
		t.Fatalf("Close on a nil lock: %v", err)
	}
}

func TestLockDir_CreatesFileWithPerms(t *testing.T) {
	dir := t.TempDir()
	l, err := LockDir(dir, lockName)
	if err != nil {
		t.Fatalf("LockDir: %v", err)
	}
	defer l.Close()
	info, err := os.Stat(filepath.Join(dir, lockName))
	if err != nil {
		t.Fatalf("stat lock file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != LockFilePerms && os.PathSeparator == '/' {
		t.Errorf("lock file mode = %04o, want %04o", perm, LockFilePerms)
	}
}

func TestLockDir_MissingDir(t *testing.T) {
	_, err := LockDir(filepath.Join(t.TempDir(), "absent"), lockName)
	if err == nil || errors.Is(err, ErrDirLocked) {
		t.Fatalf("LockDir on a missing directory = %v, want an open error", err)
	}
}
