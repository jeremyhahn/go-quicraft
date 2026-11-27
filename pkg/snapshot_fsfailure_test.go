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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// Sentinels for the individual filesystem failures injected below. Each is a
// distinct value so a test can prove the error the caller sees is the one that
// actually occurred rather than a generic wrapper.
var (
	errTestCreateTempFailed = errors.New("test fs: create temp refused")
	errTestSyncFailed       = errors.New("test fs: sync refused")
	errTestCloseFailed      = errors.New("test fs: close refused")
	errTestOpenFailed       = errors.New("test fs: open refused")
)

// faultySnapshotFS is the real filesystem with one selectable operation
// failing. It models the failure modes the snapshot persistence paths must
// survive without losing the canonical artifact: a full volume (CreateTemp),
// an I/O error at flush (Sync), and a deferred write-back error (Close).
type faultySnapshotFS struct {
	osSnapshotFS
	failOpen        bool
	failCreateTemp  bool
	failSync        bool
	failCloseSource bool
	failCloseTemp   bool
	failRemove      bool
}

func (f faultySnapshotFS) Open(name string) (snapshotFile, error) {
	if f.failOpen {
		return nil, errTestOpenFailed
	}
	inner, err := f.osSnapshotFS.Open(name)
	if err != nil {
		return nil, err
	}
	return &faultySnapshotFile{snapshotFile: inner, failClose: f.failCloseSource}, nil
}

func (f faultySnapshotFS) CreateTemp(dir, pattern string) (snapshotFile, error) {
	if f.failCreateTemp {
		return nil, errTestCreateTempFailed
	}
	inner, err := f.osSnapshotFS.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	return &faultySnapshotFile{snapshotFile: inner, failSync: f.failSync, failClose: f.failCloseTemp}, nil
}

func (f faultySnapshotFS) Remove(name string) error {
	if f.failRemove {
		return errTestRemoveFailed
	}
	return f.osSnapshotFS.Remove(name)
}

// faultySnapshotFile fails Sync and/or Close while still performing the real
// I/O, so the file on disk reflects what production would have written.
type faultySnapshotFile struct {
	snapshotFile
	failSync  bool
	failClose bool
}

func (f *faultySnapshotFile) Sync() error {
	if f.failSync {
		return errTestSyncFailed
	}
	return f.snapshotFile.Sync()
}

func (f *faultySnapshotFile) Close() error {
	// Always release the real descriptor; only the reported result is faked,
	// so a failure injection never leaks a handle into the rest of the run.
	err := f.snapshotFile.Close()
	if f.failClose {
		return errTestCloseFailed
	}
	return err
}

var (
	_ snapshotFS   = faultySnapshotFS{}
	_ snapshotFile = (*faultySnapshotFile)(nil)
)

// ---------------------------------------------------------------------------
// decryptSnapshotToAnonymousScratch
// ---------------------------------------------------------------------------

// TestDecryptSnapshotToAnonymousScratch_FilesystemFailures verifies that every
// filesystem failure on StartShard's recovery decrypt fails CLOSED: the caller
// is told which operation failed, no scratch name survives, and the canonical
// encrypted snapshot is never modified — that untouched ciphertext is what
// makes the recovery retryable once the fault clears.
func TestDecryptSnapshotToAnonymousScratch_FilesystemFailures(t *testing.T) {
	tests := []struct {
		name    string
		fsys    faultySnapshotFS
		wantErr error
	}{
		{
			name:    "temp file cannot be created",
			fsys:    faultySnapshotFS{failCreateTemp: true},
			wantErr: errTestCreateTempFailed,
		},
		{
			name:    "source cannot be opened",
			fsys:    faultySnapshotFS{failOpen: true},
			wantErr: errTestOpenFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "snapshot.dat")
			b := createTestBarrier(t)
			writeEncryptedSnapshotFile(t, b, path, []byte("snapshot payload"))

			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read snapshot: %v", err)
			}

			f, err := decryptSnapshotToAnonymousScratch(tt.fsys, dir, path, testSnapShardID, testSnapReplicaID, testSnapIndex, b.CurrentEpoch(), b)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("decryptSnapshotToAnonymousScratch error = %v, want %v", err, tt.wantErr)
			}
			if f != nil {
				t.Fatal("decryptSnapshotToAnonymousScratch returned a file with an error")
			}

			matches, globErr := filepath.Glob(filepath.Join(dir, recoverDecryptTempPattern))
			if globErr != nil {
				t.Fatalf("glob: %v", globErr)
			}
			if len(matches) > 0 {
				t.Fatalf("failed decrypt leaked scratch files: %v", matches)
			}

			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("re-read snapshot: %v", err)
			}
			if !bytes.Equal(original, after) {
				t.Fatal("failed decrypt modified the canonical encrypted snapshot")
			}
		})
	}
}

// TestDecryptSnapshotToAnonymousScratch_FailedUnlinkFailsClosed verifies that
// a scratch whose immediate unlink is refused fails StartShard's recovery
// decrypt before a byte of plaintext is written: no file is handed back, the
// name that survives the best-effort removal is EMPTY and counted as
// unreclaimed, and the canonical encrypted snapshot is untouched, so the
// recovery can be retried.
func TestDecryptSnapshotToAnonymousScratch_FailedUnlinkFailsClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")
	b := createTestBarrier(t)
	plaintext := []byte("snapshot payload that must never have a name")
	writeEncryptedSnapshotFile(t, b, path, plaintext)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}

	before := engine.StaleDecryptSweepFailures()
	f, err := decryptSnapshotToAnonymousScratch(faultySnapshotFS{failRemove: true}, dir, path,
		testSnapShardID, testSnapReplicaID, testSnapIndex, b.CurrentEpoch(), b)
	if f != nil {
		_ = f.Close()
		t.Fatal("decryptSnapshotToAnonymousScratch decrypted into a scratch whose unlink was refused")
	}
	if !errors.Is(err, engine.ErrScratchNotUnlinked) || !errors.Is(err, errTestRemoveFailed) {
		t.Fatalf("decryptSnapshotToAnonymousScratch = %v, want ErrScratchNotUnlinked wrapping the refused unlink", err)
	}
	left, globErr := filepath.Glob(filepath.Join(dir, recoverDecryptTempPattern))
	if globErr != nil {
		t.Fatalf("glob: %v", globErr)
	}
	if len(left) != 1 {
		t.Fatalf("found %d scratch names, want the 1 whose removal was refused", len(left))
	}
	if data, readErr := os.ReadFile(left[0]); readErr != nil || len(data) != 0 {
		t.Fatalf("the surviving scratch name must be empty, got %d bytes (err=%v)", len(data), readErr)
	}
	if engine.StaleDecryptSweepFailures() == before {
		t.Fatal("a scratch name that survived the best-effort removal was not counted")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("re-read snapshot: %v", err)
	}
	if !bytes.Equal(original, after) {
		t.Fatal("the failed decrypt modified the canonical encrypted snapshot")
	}
}

// writeExportTestSnapshot creates a real snapshot directory under the test's
// temp dir holding data as snapshot.dat with its metadata stamped, and
// returns the directory.
func writeExportTestSnapshot(t *testing.T, name string, data []byte) string {
	t.Helper()
	snapDir := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(snapDir, 0o750); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, "snapshot.dat"), data, 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	stampTestSnapshotMeta(t, snapDir, 1, 1)
	return snapDir
}

// newExportTestHost starts shard 1 replica 1 on a host using db and the
// given snapshot filesystem.
func newExportTestHost(t *testing.T, db *mockLogDB, fsys snapshotFS) *Host {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(),
		WithoutEncryption(), withSnapshotFS(fsys))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	return h
}

// TestExportSnapshot_OpenErrorIsNotSnapshotNotFound checks that an open
// failure other than "does not exist" is returned wrapped and is not
// reported as ErrSnapshotNotFound.
func TestExportSnapshot_OpenErrorIsNotSnapshotNotFound(t *testing.T) {
	db := newMockLogDB()
	h := newExportTestHost(t, db, faultySnapshotFS{failOpen: true})
	snapDir := writeExportTestSnapshot(t, "snap-open-fail", []byte("data"))
	db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 500, Filepath: snapDir})

	var buf bytes.Buffer
	err := h.ExportSnapshot(context.Background(), 1, &buf)
	if !errors.Is(err, errTestOpenFailed) {
		t.Fatalf("ExportSnapshot error = %v, want errTestOpenFailed", err)
	}
	if errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("ExportSnapshot error = %v, must not be ErrSnapshotNotFound", err)
	}
}

// gcRaceSnapshotFS reports the stale snapshot.dat as missing on Open, first
// running onStale (which may record a newer snapshot, as GC racing a new
// snapshot would).
type gcRaceSnapshotFS struct {
	osSnapshotFS
	stalePath string
	onStale   func()
}

func (g gcRaceSnapshotFS) Open(name string) (snapshotFile, error) {
	if name == g.stalePath {
		if g.onStale != nil {
			g.onStale()
		}
		return nil, fmt.Errorf("open %s: %w", name, fs.ErrNotExist)
	}
	return g.osSnapshotFS.Open(name)
}

// TestExportSnapshot_GCRaceRetriesLookupOnce checks that when the recorded
// snapshot file was removed and a newer snapshot is now recorded, the export
// retries once against the newer record; and that when the record has not
// changed, the export reports ErrSnapshotNotFound.
func TestExportSnapshot_GCRaceRetriesLookupOnce(t *testing.T) {
	t.Run("newer snapshot recorded", func(t *testing.T) {
		db := newMockLogDB()
		staleDir := writeExportTestSnapshot(t, "snap-stale", []byte("stale"))
		freshData := []byte("fresh snapshot data")
		freshDir := writeExportTestSnapshot(t, "snap-fresh", freshData)
		h := newExportTestHost(t, db, gcRaceSnapshotFS{
			stalePath: filepath.Join(staleDir, "snapshot.dat"),
			onStale: func() {
				db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 600, Filepath: freshDir})
			},
		})
		db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 500, Filepath: staleDir})

		var buf bytes.Buffer
		if err := h.ExportSnapshot(context.Background(), 1, &buf); err != nil {
			t.Fatalf("ExportSnapshot failed: %v", err)
		}
		hdr := stripArtifactTrailer(t, &buf)
		if hdr.Index != 600 {
			t.Errorf("artifact Index = %d, want 600", hdr.Index)
		}
		if !bytes.Equal(buf.Bytes(), freshData) {
			t.Errorf("exported data = %q, want %q", buf.Bytes(), freshData)
		}
	})

	t.Run("record unchanged", func(t *testing.T) {
		db := newMockLogDB()
		staleDir := writeExportTestSnapshot(t, "snap-stale", []byte("stale"))
		h := newExportTestHost(t, db, gcRaceSnapshotFS{
			stalePath: filepath.Join(staleDir, "snapshot.dat"),
		})
		db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 500, Filepath: staleDir})

		var buf bytes.Buffer
		err := h.ExportSnapshot(context.Background(), 1, &buf)
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("ExportSnapshot error = %v, want ErrSnapshotNotFound", err)
		}
	})
}

// ---------------------------------------------------------------------------
// writeImportedSnapshotData
// ---------------------------------------------------------------------------

// importWriteFaultFS is the real filesystem whose snapshot data file fails
// Sync or Close, the durability steps after an import's payload is written.
type importWriteFaultFS struct {
	osSnapshotFS
	failSync  bool
	failClose bool
}

func (f importWriteFaultFS) OpenFile(name string, flag int, perm fs.FileMode) (snapshotFile, error) {
	inner, err := f.osSnapshotFS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultySnapshotFile{snapshotFile: inner, failSync: f.failSync, failClose: f.failClose}, nil
}

var _ snapshotFS = importWriteFaultFS{}
