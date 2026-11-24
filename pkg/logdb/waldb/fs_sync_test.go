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

//go:build unix

package waldb

import (
	"errors"
	"os"
	"runtime"
	"syscall"
	"testing"
)

// The last unobservable step in the durability chain.
//
// Every other durability test in this package watches the File interface: the
// crash model advances its durable image on Sync, and syncAccountingFS counts
// the Sync and SyncMetadata calls per path. All of them sit ABOVE osFile, so
// all of them stay green if osFile.Sync and osFile.SyncMetadata are replaced
// with `return nil` — the calls still happen, they just stop reaching the
// disk. That single edit deletes the durability of the entire WAL and, before
// these tests existed, the whole repository still passed.
//
// A userspace test cannot see an fdatasync(2) succeed. It can see one FAIL:
// the syscall is defined only for file descriptors with something to flush, so
// on a pipe it returns EINVAL. A Sync that returns nil there did not issue the
// syscall, which is precisely the stub. The assertion is therefore about the
// error a real syscall produces, not about a side effect, and it is exact —
// EINVAL from a pipe has no other source.

// TestOSFileSyncIssuesRealSyscall fails if osFile.Sync stops calling
// fdatasync/fsync on the underlying descriptor.
func TestOSFileSyncIssuesRealSyscall(t *testing.T) {
	r, w := mustPipe(t)
	defer closeBoth(t, r, w)

	err := (&osFile{f: w}).Sync()
	if err == nil {
		t.Fatal("osFile.Sync returned nil on a pipe: no fdatasync(2) was issued, so nothing " +
			"this WAL acknowledges is durable")
	}
	assertEINVAL(t, "osFile.Sync", err)
}

// TestOSFileSyncMetadataIssuesRealSyscall fails if osFile.SyncMetadata stops
// calling fsync on the underlying descriptor. It is the call every DIRECTORY
// flush goes through, so a stub here silently un-publishes every rename,
// create and unlink the WAL depends on.
func TestOSFileSyncMetadataIssuesRealSyscall(t *testing.T) {
	r, w := mustPipe(t)
	defer closeBoth(t, r, w)

	err := (&osFile{f: w}).SyncMetadata()
	if err == nil {
		t.Fatal("osFile.SyncMetadata returned nil on a pipe: no fsync(2) was issued, so no " +
			"directory entry this WAL creates or removes is durable")
	}
	assertEINVAL(t, "osFile.SyncMetadata", err)
}

// mustPipe returns a pipe pair, failing the test if one cannot be created.
func mustPipe(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	return r, w
}

// closeBoth closes both ends of a pipe.
func closeBoth(t *testing.T, r, w *os.File) {
	t.Helper()
	if err := r.Close(); err != nil {
		t.Errorf("close pipe read end: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("close pipe write end: %v", err)
	}
}

// assertEINVAL checks the error is the one the kernel returns for a flush of a
// descriptor that cannot be flushed. Linux is exact about it; other unices
// report their own errno for the same refusal, so there the non-nil error
// above is the whole assertion.
func assertEINVAL(t *testing.T, who string, err error) {
	t.Helper()
	if runtime.GOOS != "linux" {
		return
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("%s on a pipe returned %v, want EINVAL: the error did not come from the "+
			"flush syscall, so the syscall may not have been issued", who, err)
	}
}
