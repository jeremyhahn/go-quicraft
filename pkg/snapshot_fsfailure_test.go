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
	"errors"
	"os"
	"path/filepath"
	"testing"
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
// decryptSnapshotToTemp
// ---------------------------------------------------------------------------

// TestDecryptSnapshotToTemp_FilesystemFailures verifies that every filesystem
// failure on the decrypt-for-send path fails CLOSED: the caller is told which
// operation failed, no scratch file survives, and the canonical encrypted
// snapshot is never modified — that untouched ciphertext is what makes the
// transfer retryable once the fault clears.
func TestDecryptSnapshotToTemp_FilesystemFailures(t *testing.T) {
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
			name:    "temp file cannot be flushed",
			fsys:    faultySnapshotFS{failSync: true},
			wantErr: errTestSyncFailed,
		},
		{
			name:    "temp file cannot be closed",
			fsys:    faultySnapshotFS{failCloseTemp: true},
			wantErr: errTestCloseFailed,
		},
		{
			name:    "source cannot be closed",
			fsys:    faultySnapshotFS{failCloseSource: true},
			wantErr: errTestCloseFailed,
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

			tmpPath, size, err := decryptSnapshotToTemp(tt.fsys, path, sendDecryptTempPattern, b.CurrentEpoch(), b)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("decryptSnapshotToTemp error = %v, want %v", err, tt.wantErr)
			}
			if tmpPath != "" || size != 0 {
				t.Fatalf("decryptSnapshotToTemp returned (%q, %d) with an error", tmpPath, size)
			}

			matches, globErr := filepath.Glob(filepath.Join(dir, sendDecryptTempPattern))
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

// TestDecryptSnapshotToTemp_ScratchRemovalFailureStillReportsError verifies
// that when the cleanup of a scratch file ALSO fails, the original decrypt
// error is still what the caller sees. Masking it with the cleanup failure
// would misreport why the transfer was refused.
func TestDecryptSnapshotToTemp_ScratchRemovalFailureStillReportsError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")
	b := createTestBarrier(t)
	writeEncryptedSnapshotFile(t, b, path, []byte("snapshot payload"))

	fsys := faultySnapshotFS{failSync: true, failRemove: true}
	tmpPath, size, err := decryptSnapshotToTemp(fsys, path, sendDecryptTempPattern, b.CurrentEpoch(), b)
	if !errors.Is(err, errTestSyncFailed) {
		t.Fatalf("decryptSnapshotToTemp error = %v, want the sync failure", err)
	}
	if tmpPath != "" || size != 0 {
		t.Fatalf("decryptSnapshotToTemp returned (%q, %d) with an error", tmpPath, size)
	}
}

// ---------------------------------------------------------------------------
// reencryptReceivedSnapshot
// ---------------------------------------------------------------------------

// TestReencryptReceivedSnapshot_FilesystemFailures verifies that the receive
// path's in-place re-encryption is crash-safe under filesystem faults: on any
// failure the caller gets the error, no epoch is reported, and the received
// plaintext file is left exactly as it was, so the recovery path still finds a
// consistent artifact.
func TestReencryptReceivedSnapshot_FilesystemFailures(t *testing.T) {
	tests := []struct {
		name    string
		fsys    faultySnapshotFS
		wantErr error
	}{
		{
			name:    "source cannot be opened",
			fsys:    faultySnapshotFS{failOpen: true},
			wantErr: errTestOpenFailed,
		},
		{
			name:    "temp file cannot be created",
			fsys:    faultySnapshotFS{failCreateTemp: true},
			wantErr: errTestCreateTempFailed,
		},
		{
			name:    "temp file cannot be flushed",
			fsys:    faultySnapshotFS{failSync: true},
			wantErr: errTestSyncFailed,
		},
		{
			name:    "temp file cannot be closed",
			fsys:    faultySnapshotFS{failCloseTemp: true},
			wantErr: errTestCloseFailed,
		},
		{
			name:    "source cannot be closed",
			fsys:    faultySnapshotFS{failCloseSource: true},
			wantErr: errTestCloseFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "snapshot.dat")
			plaintext := []byte("received plaintext snapshot")
			if err := os.WriteFile(path, plaintext, 0o600); err != nil {
				t.Fatalf("write snapshot: %v", err)
			}
			b := createTestBarrier(t)

			epoch, err := reencryptReceivedSnapshot(tt.fsys, path, b)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("reencryptReceivedSnapshot error = %v, want %v", err, tt.wantErr)
			}
			if epoch != 0 {
				t.Fatalf("epoch = %d, want 0 on failure", epoch)
			}

			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read back: %v", err)
			}
			if !bytes.Equal(got, plaintext) {
				t.Fatalf("failed re-encryption replaced snapshot.dat: %q", got)
			}
			matches, globErr := filepath.Glob(filepath.Join(dir, ".recv-encrypt-*.tmp"))
			if globErr != nil {
				t.Fatalf("glob: %v", globErr)
			}
			if len(matches) > 0 {
				t.Fatalf("failed re-encryption leaked scratch files: %v", matches)
			}
		})
	}
}
