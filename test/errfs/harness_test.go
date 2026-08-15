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

// Tests for the fault-injection harness itself. They carry no build tag: a
// broken harness makes every test that depends on it vacuously green, so its
// own guarantees are checked on every ordinary `go test ./...` rather than
// behind the tag that hides the suite it serves.
package errfs_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/test/errfs"
)

// errHarness is the fault every test in this file injects.
var errHarness = errors.New("errfs_test: harness fault")

// TestOnlyOpsRejectsUnknownOperation is the guard against the quietest failure
// this package can have. A misspelled operation name matches nothing, so the
// injection never fires, the WAL never fails, and every assertion the test
// makes about the fault it "injected" passes for the wrong reason.
func TestOnlyOpsRejectsUnknownOperation(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("OnlyOps accepted an unknown operation name: a typo silently disarms the injection")
		}
		unknown, ok := r.(*errfs.UnknownOpError)
		if !ok {
			t.Fatalf("OnlyOps panicked with %T (%v), want *errfs.UnknownOpError", r, r)
		}
		if unknown.Name != "Fsync" {
			t.Fatalf("UnknownOpError.Name=%q, want %q", unknown.Name, "Fsync")
		}
	}()
	errfs.New(waldb.DefaultFS(), 1, errHarness).OnlyOps("Fsync")
}

// TestOnlyOpsAcceptsEveryOperationConstant keeps the validation set and the
// operations the harness actually counts from drifting apart: a constant the
// validator rejects is a filter no test can express.
func TestOnlyOpsAcceptsEveryOperationConstant(t *testing.T) {
	ops := []string{
		errfs.OpOpenFile, errfs.OpOpen, errfs.OpMkdirAll, errfs.OpReadDir,
		errfs.OpStat, errfs.OpRemove, errfs.OpRename, errfs.OpWrite,
		errfs.OpWriteAt, errfs.OpRead, errfs.OpReadAt, errfs.OpFileStat,
		errfs.OpTruncate, errfs.OpSeek, errfs.OpSync, errfs.OpSyncMetadata,
		errfs.OpClose, errfs.OpPreAllocate,
	}
	for _, op := range ops {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("OnlyOps(%q) panicked: %v", op, r)
				}
			}()
			errfs.New(waldb.DefaultFS(), 1, errHarness).OnlyOps(op)
		}()
	}
}

// TestTimesBudgetControlsRepeatedInjection covers the property the
// retry-exhaustion and rollback-after-failure paths depend on: the SECOND
// fault. One-shot injection heals the filesystem the instant the first fault
// lands, so any code that runs because of it runs against a healthy disk.
func TestTimesBudgetControlsRepeatedInjection(t *testing.T) {
	cases := []struct {
		name  string
		times int64
		want  int
	}{
		{name: "default one-shot", times: 1, want: 1},
		{name: "budget of three", times: 3, want: 3},
		{name: "unlimited", times: 0, want: 5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			efs := errfs.New(waldb.DefaultFS(), 1, errHarness).
				OnlyOps(errfs.OpStat).
				Times(tc.times)

			got := 0
			for i := 0; i < 5; i++ {
				if _, err := efs.Stat(t.TempDir()); err != nil {
					if !errors.Is(err, errHarness) {
						t.Fatalf("Stat returned an unexpected error: %v", err)
					}
					got++
				}
			}
			if got != tc.want {
				t.Fatalf("fault fired %d times over 5 eligible operations, want %d", got, tc.want)
			}
			if efs.Fires() != int64(tc.want) {
				t.Fatalf("Fires()=%d, want %d", efs.Fires(), tc.want)
			}
		})
	}
}

// TestWrappedFileMirrorsDelegateCapabilities is the regression test for the
// silent degradation the harness used to force on production.
//
// waldb selects the directory-metadata-sync and pre-allocation paths by type
// assertion. A wrapper that answers assertions its delegate cannot honour
// sends production down a path the real filesystem does not have, and one that
// answers none of them sends every directory flush through fdatasync — which
// is exactly the call FileMetadataSyncer exists to keep directories away from,
// and which no assertion downstream can detect because the return value is the
// same.
func TestWrappedFileMirrorsDelegateCapabilities(t *testing.T) {
	t.Run("real filesystem keeps both capabilities", func(t *testing.T) {
		efs := errfs.New(waldb.DefaultFS(), 0, errHarness)
		path := filepath.Join(t.TempDir(), "seg.log")
		f, err := efs.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		defer f.Close()

		if _, ok := f.(waldb.FileMetadataSyncer); !ok {
			t.Fatal("wrapped production file does not implement FileMetadataSyncer: " +
				"syncDirFS would silently degrade every directory flush to fdatasync")
		}
		if _, ok := f.(waldb.FilePreAllocator); !ok {
			t.Fatal("wrapped production file does not implement FilePreAllocator: " +
				"segment creation would take the no-capability path production never takes")
		}
	})

	t.Run("capability-free delegate gains nothing", func(t *testing.T) {
		efs := errfs.New(plainFS{}, 0, errHarness)
		f, err := efs.Open("anything")
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if _, ok := f.(waldb.FileMetadataSyncer); ok {
			t.Fatal("wrapper implements FileMetadataSyncer over a delegate that does not: " +
				"production would call a capability the filesystem beneath it lacks")
		}
		if _, ok := f.(waldb.FilePreAllocator); ok {
			t.Fatal("wrapper implements FilePreAllocator over a delegate that does not")
		}
	})
}

// TestSyncMetadataIsCountedApartFromSync verifies the two flushes are distinct
// operations to the harness, so a filter can arm a directory flush without
// arming every data flush in the WAL.
func TestSyncMetadataIsCountedApartFromSync(t *testing.T) {
	efs := errfs.New(waldb.DefaultFS(), 1, errHarness).OnlyOps(errfs.OpSyncMetadata)
	path := filepath.Join(t.TempDir(), "seg.log")
	f, err := efs.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	if err := f.Sync(); err != nil {
		t.Fatalf("Sync fired a fault armed for SyncMetadata only: %v", err)
	}

	ms, ok := f.(waldb.FileMetadataSyncer)
	if !ok {
		t.Fatal("wrapped production file does not implement FileMetadataSyncer")
	}
	if err := ms.SyncMetadata(); !errors.Is(err, errHarness) {
		t.Fatalf("SyncMetadata returned %v, want the injected fault: the operation cannot be targeted", err)
	}
}

// plainFS is a filesystem whose files carry no optional capability. Its
// operations are never performed; only the type of what Open returns matters.
type plainFS struct{}

func (plainFS) OpenFile(string, int, fs.FileMode) (waldb.File, error) { return plainFile{}, nil }
func (plainFS) Open(string) (waldb.File, error)                       { return plainFile{}, nil }
func (plainFS) MkdirAll(string, fs.FileMode) error                    { return nil }
func (plainFS) ReadDir(string) ([]fs.DirEntry, error)                 { return nil, nil }
func (plainFS) Stat(string) (fs.FileInfo, error)                      { return nil, nil }
func (plainFS) Remove(string) error                                   { return nil }
func (plainFS) Rename(string, string) error                           { return nil }

// plainFile implements waldb.File and nothing else.
type plainFile struct{}

func (plainFile) Write([]byte) (int, error)          { return 0, nil }
func (plainFile) WriteAt([]byte, int64) (int, error) { return 0, nil }
func (plainFile) Read([]byte) (int, error)           { return 0, nil }
func (plainFile) ReadAt([]byte, int64) (int, error)  { return 0, nil }
func (plainFile) Stat() (fs.FileInfo, error)         { return nil, nil }
func (plainFile) Truncate(int64) error               { return nil }
func (plainFile) Seek(int64, int) (int64, error)     { return 0, nil }
func (plainFile) Sync() error                        { return nil }
func (plainFile) Close() error                       { return nil }
