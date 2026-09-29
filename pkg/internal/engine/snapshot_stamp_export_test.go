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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// errInjectedStampHash is the read failure the stamping tests inject.
var errInjectedStampHash = errors.New("injected snapshot hash read failure")

// failStampHash makes every snapshot.meta WRITER's hash fail until the
// returned restore runs (or the test ends). The verifiers are unaffected: they
// hash through hashSnapshotData directly.
func failStampHash(t *testing.T) (restore func()) {
	t.Helper()
	orig := stampSnapshotHash
	stampSnapshotHash = func(string) (uint64, uint64, error) {
		return 0, 0, errInjectedStampHash
	}
	restored := false
	restore = func() {
		if !restored {
			stampSnapshotHash = orig
			restored = true
		}
	}
	t.Cleanup(restore)
	return restore
}

// TestSaveSnapshot_HashFailureRecordsAndCompactsNothing is the regression
// guard for a save that published an artifact it knew it could not vouch for.
//
// writeSnapshotMetadata treated a failure to hash snapshot.dat as non-fatal
// and wrote the record "without the fields" — but the encoder always emits
// them, so it recorded DataLen 0 and DataHash 0 as fact. The save then
// published the directory, recorded it in LogDB and compacted the log it
// covers, and every later recovery refused the artifact on the hash mismatch:
// a single-replica shard was left with neither a usable snapshot nor the log
// to rebuild one. The failure must land BEFORE the publish, leaving nothing
// recorded and nothing compacted, and the next save must succeed.
func TestSaveSnapshot_HashFailureRecordsAndCompactsNothing(t *testing.T) {
	f := newResaveFixture(t, 200)
	f.node.logReader = advancingReader(t, f.req.shardID, f.req.replicaID, f.ldb, 200)

	restore := failStampHash(t)
	index, compaction, err := f.pool.saveSnapshotCompacting(f.req)
	restore()
	if !errors.Is(err, errInjectedStampHash) {
		t.Fatalf("saveSnapshot with an unhashable artifact = (%d, %v), want the "+
			"hash failure: a record stamped with a zero hash is refused by every "+
			"later recovery", index, err)
	}
	if index != 0 || compaction != (LogCompaction{}) {
		t.Fatalf("failed save reported index %d, compaction %+v; want neither", index, compaction)
	}
	if rec, getErr := f.ldb.GetSnapshot(f.req.shardID, f.req.replicaID); !errors.Is(getErr, logdb.ErrNoSnapshot) {
		t.Fatalf("the failed save recorded a snapshot in LogDB: %+v (err %v)", rec, getErr)
	}
	f.ldb.mu.Lock()
	removedTo := f.ldb.removedTo
	f.ldb.mu.Unlock()
	if removedTo != 0 {
		t.Fatalf("the failed save compacted the log to %d: the entries it covers "+
			"are gone and no recoverable snapshot replaces them", removedTo)
	}
	if got := f.node.lastSnapshotIndex.Load(); got != 0 {
		t.Fatalf("lastSnapshotIndex = %d after a failed save, want 0", got)
	}
	// Nothing published, and the stage reclaimed.
	entries, readErr := os.ReadDir(filepath.Dir(f.saveDirFor(200)))
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatalf("read replica snapshot dir: %v", readErr)
	}
	for _, e := range entries {
		t.Errorf("the failed save left %q behind", e.Name())
	}

	// The next save retries cleanly and produces an artifact recovery accepts.
	index, compaction, err = f.pool.saveSnapshotCompacting(f.req)
	if err != nil {
		t.Fatalf("retry save: %v", err)
	}
	if index != 200 {
		t.Fatalf("retry save index = %d, want 200", index)
	}
	if want := (LogCompaction{Index: 190, Reader: true, LogDB: true}); compaction != want {
		t.Fatalf("retry compaction = %+v, want %+v", compaction, want)
	}
	rec, err := f.ldb.GetSnapshot(f.req.shardID, f.req.replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot after retry: %v", err)
	}
	if err := VerifySnapshotArtifact(rec.Filepath, filepath.Join(rec.Filepath, snapshotDataFile)); err != nil {
		t.Fatalf("the retried save's artifact fails verification: %v", err)
	}
}

// TestWriteSnapshotMetadata_HashFailureWritesNoRecord pins the helper's own
// contract: a hash failure returns the error and leaves no snapshot.meta.
func TestWriteSnapshotMetadata_HashFailureWritesNoRecord(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, snapshotDataFile), []byte("artifact"), 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	failStampHash(t)
	err := writeSnapshotMetadata(dir, 10, 2, 1, 1, 0, config.NoCompression)
	if !errors.Is(err, errInjectedStampHash) {
		t.Fatalf("writeSnapshotMetadata = %v, want the hash failure", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, snapshotMetadataFile)); !os.IsNotExist(statErr) {
		t.Fatalf("a record was written despite the hash failure (stat: %v)", statErr)
	}
}

// ---------------------------------------------------------------------------
// SnapshotOption.ExportPath: the exported pair must be complete and durable
// ---------------------------------------------------------------------------

// TestExportSnapshotToPath_RefusesMissingSourceMetadata is the regression
// guard for an export that reported success without its record. The source
// snapshot.meta was read as optional, so an export whose record could not be
// read produced a snapshot.dat alone — a directory in no documented format,
// which no verifier can vouch for.
func TestExportSnapshotToPath_RefusesMissingSourceMetadata(t *testing.T) {
	saveDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(saveDir, snapshotDataFile), []byte("data"), 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	exportDir := filepath.Join(t.TempDir(), "export-dest")

	err := exportSnapshotToPath(saveDir, exportDir)
	var exportErr *SnapshotExportError
	if !errors.As(err, &exportErr) {
		t.Fatalf("export without source metadata = %v, want *SnapshotExportError", err)
	}
	if _, statErr := os.Stat(filepath.Join(exportDir, snapshotDataFile)); !os.IsNotExist(statErr) {
		t.Fatalf("the failed export left a snapshot.dat behind (stat: %v)", statErr)
	}
}

// TestWriteSnapshotExport_MetadataWriteFailureFailsExport is the regression
// guard for the metadata write whose failure was only logged: the export
// returned success with no snapshot.meta beside the data. The failure is
// injected by occupying the record's name with a non-empty directory, which
// no write or rename can replace — as root too.
func TestWriteSnapshotExport_MetadataWriteFailureFailsExport(t *testing.T) {
	src := filepath.Join(t.TempDir(), snapshotDataFile)
	if err := os.WriteFile(src, []byte("data"), 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	exportDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(exportDir, snapshotMetadataFile, "occupied"), 0o750); err != nil {
		t.Fatalf("block metadata path: %v", err)
	}

	err := writeSnapshotExport(src, exportDir,
		MarshalSnapshotMeta(SnapshotMeta{Index: 10, Term: 2, ShardID: 1, ReplicaID: 1}))
	var exportErr *SnapshotExportError
	if !errors.As(err, &exportErr) {
		t.Fatalf("export whose metadata could not be written = %v, want "+
			"*SnapshotExportError", err)
	}
	// No staged temp survives the failure.
	entries, readErr := os.ReadDir(exportDir)
	if readErr != nil {
		t.Fatalf("read export dir: %v", readErr)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".snapshot-export-") {
			t.Errorf("the failed export left staged temp %q behind", e.Name())
		}
	}
}

// TestWriteSnapshotExport_RefusesEmptyMetadata pins that the record is not
// optional: a caller with no record to write fails the export instead of
// producing a snapshot.dat alone.
func TestWriteSnapshotExport_RefusesEmptyMetadata(t *testing.T) {
	src := filepath.Join(t.TempDir(), snapshotDataFile)
	if err := os.WriteFile(src, []byte("data"), 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	exportDir := filepath.Join(t.TempDir(), "export-dest")
	err := writeSnapshotExport(src, exportDir, nil)
	if !errors.Is(err, ErrSnapshotMetaCorrupt) {
		t.Fatalf("export with no metadata = %v, want ErrSnapshotMetaCorrupt", err)
	}
	if _, statErr := os.Stat(filepath.Join(exportDir, snapshotDataFile)); !os.IsNotExist(statErr) {
		t.Fatalf("the refused export wrote a snapshot.dat (stat: %v)", statErr)
	}
}

// TestSaveSnapshot_EncryptedExportCarriesAVerifiablePair covers the decrypted
// export: its snapshot.meta is re-marshalled with Epoch 0, and it used to be
// marshalled from the save's metadata WITHOUT the integrity fields, so the
// exported plaintext carried a zero DataLen/DataHash that every verifier
// refuses. The fields must be stamped over the bytes the export holds.
func TestSaveSnapshot_EncryptedExportCarriesAVerifiablePair(t *testing.T) {
	f := newResaveFixture(t, 200)
	setTestShardBarrier(f.pool, newTenantBarrier(1, 4))
	exportDir := t.TempDir()
	f.req.exportPath = exportDir

	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("encrypted save with export: %v", err)
	}
	rec, err := f.ldb.GetSnapshot(f.req.shardID, f.req.replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if rec.Epoch == 0 {
		t.Fatal("test premise broken: the canonical artifact was not encrypted")
	}
	metaBytes, err := os.ReadFile(filepath.Join(exportDir, snapshotMetadataFile))
	if err != nil {
		t.Fatalf("read exported meta: %v", err)
	}
	meta, err := ParseSnapshotMeta(metaBytes)
	if err != nil {
		t.Fatalf("parse exported meta: %v", err)
	}
	if meta.Epoch != 0 {
		t.Fatalf("exported meta Epoch = %d, want 0 for a plaintext export", meta.Epoch)
	}
	if err := VerifySnapshotArtifact(exportDir, filepath.Join(exportDir, snapshotDataFile)); err != nil {
		t.Fatalf("the decrypted export fails verification against its own record: %v", err)
	}
}

// TestSaveSnapshot_ExportMetadataFailureFailsSave drives the same failure
// through SnapshotOption.ExportPath: the save must fail, and — because the
// export runs before the publish — record and compact nothing. Once the path
// is writable the retried save exports a pair that verifies.
func TestSaveSnapshot_ExportMetadataFailureFailsSave(t *testing.T) {
	f := newResaveFixture(t, 200)
	f.node.logReader = advancingReader(t, f.req.shardID, f.req.replicaID, f.ldb, 200)
	exportDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(exportDir, snapshotMetadataFile, "occupied"), 0o750); err != nil {
		t.Fatalf("block metadata path: %v", err)
	}
	f.req.exportPath = exportDir

	if _, err := f.pool.saveSnapshot(f.req); err == nil {
		t.Fatal("saveSnapshot reported success for an export without its metadata")
	}
	if _, getErr := f.ldb.GetSnapshot(f.req.shardID, f.req.replicaID); !errors.Is(getErr, logdb.ErrNoSnapshot) {
		t.Fatalf("the failed save recorded a snapshot in LogDB (err %v)", getErr)
	}
	f.ldb.mu.Lock()
	removedTo := f.ldb.removedTo
	f.ldb.mu.Unlock()
	if removedTo != 0 {
		t.Fatalf("the failed save compacted the log to %d", removedTo)
	}

	if err := os.RemoveAll(filepath.Join(exportDir, snapshotMetadataFile)); err != nil {
		t.Fatalf("unblock: %v", err)
	}
	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("retry save with export: %v", err)
	}
	if err := VerifySnapshotArtifact(exportDir, filepath.Join(exportDir, snapshotDataFile)); err != nil {
		t.Fatalf("the exported pair fails verification: %v", err)
	}
}
