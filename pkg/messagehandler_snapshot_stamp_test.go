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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// readFailingSnapshotFS is the real filesystem whose opened files fail every
// Read: the artifact exists and opens, but its bytes cannot be hashed.
type readFailingSnapshotFS struct{ osSnapshotFS }

func (f readFailingSnapshotFS) Open(name string) (snapshotFile, error) {
	inner, err := f.osSnapshotFS.Open(name)
	if err != nil {
		return nil, err
	}
	return readFailingSnapshotFile{inner}, nil
}

type readFailingSnapshotFile struct{ snapshotFile }

func (readFailingSnapshotFile) Read([]byte) (int, error) { return 0, errTestReadFailed }

var _ snapshotFS = readFailingSnapshotFS{}

// TestWriteReceivedSnapshotMetadata_HashFailureWritesNoRecord is the
// regression guard for the receive and import paths' copy of the save path's
// defect: a failure to hash snapshot.dat was logged and the record written
// anyway. The encoder always emits the integrity fields, so that record
// claimed DataLen 0 and DataHash 0 as fact, the caller went on to publish and
// record the snapshot, and every later recovery refused it. The write must now
// fail before any record exists, so the caller discards its unpublished stage.
func TestWriteReceivedSnapshotMetadata_HashFailureWritesNoRecord(t *testing.T) {
	cases := []struct {
		name    string
		fsys    snapshotFS
		setup   func(t *testing.T, dir string)
		wantErr error
	}{
		{
			name:  "artifact_missing",
			fsys:  osSnapshotFS{},
			setup: func(*testing.T, string) {},
		},
		{
			name: "artifact_open_fails",
			fsys: faultySnapshotFS{failOpen: true},
			setup: func(t *testing.T, dir string) {
				writeTestArtifact(t, dir)
			},
			wantErr: errTestOpenFailed,
		},
		{
			name: "artifact_read_fails",
			fsys: readFailingSnapshotFS{},
			setup: func(t *testing.T, dir string) {
				writeTestArtifact(t, dir)
			},
			wantErr: errTestReadFailed,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.setup(t, dir)
			err := writeReceivedSnapshotMetadata(tc.fsys, dir, 42, 7, 10, 3, 0, config.NoCompression)
			if err == nil {
				t.Fatal("writeReceivedSnapshotMetadata succeeded without a hash of the " +
					"artifact: the record claims a zero hash every recovery refuses")
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if _, statErr := os.Stat(filepath.Join(dir, "snapshot.meta")); !os.IsNotExist(statErr) {
				t.Fatalf("a record was written despite the hash failure (stat: %v)", statErr)
			}
		})
	}
}

// TestWriteReceivedSnapshotMetadata_StampsAVerifiableRecord is the other
// half: a hashable artifact gets a record every verifier accepts.
func TestWriteReceivedSnapshotMetadata_StampsAVerifiableRecord(t *testing.T) {
	dir := t.TempDir()
	writeTestArtifact(t, dir)
	if err := writeReceivedSnapshotMetadata(osSnapshotFS{}, dir, 42, 7, 10, 3, 0,
		config.NoCompression); err != nil {
		t.Fatalf("writeReceivedSnapshotMetadata: %v", err)
	}
	if err := engine.VerifySnapshotArtifact(dir, filepath.Join(dir, "snapshot.dat")); err != nil {
		t.Fatalf("the stamped record does not verify its own artifact: %v", err)
	}
}

// TestStartShard_RefusesARecordedSnapshotWithoutMetadata drives the fail-closed
// integrity check through the startup recovery path: an artifact that is
// present but whose snapshot.meta is gone must fail StartShard rather than be
// recovered unchecked.
func TestStartShard_RefusesARecordedSnapshotWithoutMetadata(t *testing.T) {
	db := newMockLogDB()
	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	snapDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(snapDir, "snapshot.dat"), []byte{0, 0, 0, 0}, 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 42, Term: 1, Filepath: snapDir}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc,
		testShardConfig(1, 1))
	var hie *HostInitError
	if !errors.As(startErr, &hie) || hie.Field != "snapshot_integrity" {
		t.Fatalf("StartShard = %v, want HostInitError{Field: snapshot_integrity}", startErr)
	}
	if !errors.Is(startErr, engine.ErrSnapshotCorrupt) {
		t.Fatalf("StartShard = %v, want it to match engine.ErrSnapshotCorrupt", startErr)
	}
}

// writeTestArtifact writes a small snapshot.dat into dir.
func writeTestArtifact(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "snapshot.dat"), []byte("received artifact"), 0o600); err != nil {
		t.Fatalf("write snapshot.dat: %v", err)
	}
}
