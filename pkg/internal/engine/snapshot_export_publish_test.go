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
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// errInjectedExport is the failure the export publication tests inject.
var errInjectedExport = errors.New("test: export step refused")

// withExportRename replaces the export's rename for the rest of the test.
func withExportRename(t *testing.T, rename func(oldpath, newpath string) error) {
	t.Helper()
	orig := exportRename
	exportRename = rename
	t.Cleanup(func() { exportRename = orig })
}

// withExportSyncDir replaces the export's directory fsync for the rest of the
// test.
func withExportSyncDir(t *testing.T, syncDir func(string) error) {
	t.Helper()
	orig := exportSyncDir
	exportSyncDir = syncDir
	t.Cleanup(func() { exportSyncDir = orig })
}

// exportPairSource writes data as a snapshot.dat source and returns its path
// with the record that vouches for it.
func exportPairSource(t *testing.T, data []byte) (string, []byte) {
	t.Helper()
	src := filepath.Join(t.TempDir(), snapshotDataFile)
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatalf("write export source: %v", err)
	}
	sum, size, err := hashSnapshotData(src)
	if err != nil {
		t.Fatalf("hash export source: %v", err)
	}
	return src, MarshalSnapshotMeta(SnapshotMeta{
		Index: 10, Term: 2, ShardID: 1, ReplicaID: 1, DataLen: size, DataHash: sum,
	})
}

// readExportPair returns the export's snapshot.dat and snapshot.meta.
func readExportPair(t *testing.T, exportPath string) ([]byte, []byte) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(exportPath, snapshotDataFile))
	if err != nil {
		t.Fatalf("read exported data: %v", err)
	}
	meta, err := os.ReadFile(filepath.Join(exportPath, snapshotMetadataFile))
	if err != nil {
		t.Fatalf("read exported meta: %v", err)
	}
	return data, meta
}

// assertOnlyExportPair fails when anything but the pair — a staged temp or a
// backup directory — is left in the export directory.
func assertOnlyExportPair(t *testing.T, exportPath string) {
	t.Helper()
	entries, err := os.ReadDir(exportPath)
	if err != nil {
		t.Fatalf("read export dir: %v", err)
	}
	for _, e := range entries {
		if e.Name() != snapshotDataFile && e.Name() != snapshotMetadataFile {
			t.Errorf("the export left %q behind", e.Name())
		}
	}
}

// TestWriteSnapshotExport_FsyncsEveryCreatedDirectory: an export into a
// directory the export itself creates must fsync every directory MkdirAll
// created, and the pre-existing ancestor's entry for the first of them. The
// export directory's own fsync makes the two renames durable, but not the
// directory: a power loss could lose it, and with it an export reported as
// written. Nothing above the pre-existing ancestor is touched.
func TestWriteSnapshotExport_FsyncsEveryCreatedDirectory(t *testing.T) {
	base := t.TempDir()
	exportPath := filepath.Join(base, "a", "b", "c")
	var mu sync.Mutex
	synced := map[string]bool{}
	withExportSyncDir(t, func(dir string) error {
		mu.Lock()
		synced[filepath.Clean(dir)] = true
		mu.Unlock()
		return SyncDir(dir)
	})

	src, meta := exportPairSource(t, []byte("a fresh export"))
	if err := writeSnapshotExport(src, exportPath, meta); err != nil {
		t.Fatalf("writeSnapshotExport: %v", err)
	}
	for _, dir := range []string{
		exportPath,
		filepath.Join(base, "a", "b"),
		filepath.Join(base, "a"),
		base,
	} {
		if !synced[dir] {
			t.Errorf("the export did not fsync %s, so a power loss can lose the export directory", dir)
		}
	}
	if synced[filepath.Dir(base)] {
		t.Errorf("the export fsynced %s, above the deepest directory that already existed", filepath.Dir(base))
	}
	if err := VerifySnapshotArtifact(exportPath, filepath.Join(exportPath, snapshotDataFile)); err != nil {
		t.Fatalf("the export fails verification: %v", err)
	}
}

// TestWriteSnapshotExport_FailedPublishKeepsThePreviousPair: a publication
// that fails over an existing export must leave the previous pair exactly as
// it was — complete, consistent and verifiable. Renaming snapshot.dat over
// the old one and then failing on snapshot.meta left the new data beside the
// old record: the previous export destroyed, and nothing published in its
// place. Each step after the previous pair is moved aside is failed in turn.
func TestWriteSnapshotExport_FailedPublishKeepsThePreviousPair(t *testing.T) {
	isMetaPublish := func(oldpath, newpath string) bool {
		return strings.HasPrefix(filepath.Base(oldpath), ".snapshot-export-") &&
			strings.HasSuffix(oldpath, ".tmp") &&
			filepath.Base(newpath) == snapshotMetadataFile
	}
	tests := []struct {
		name   string
		inject func(t *testing.T, exportPath string)
	}{
		{
			name: "metadata rename fails",
			inject: func(t *testing.T, _ string) {
				withExportRename(t, func(oldpath, newpath string) error {
					if isMetaPublish(oldpath, newpath) {
						return errInjectedExport
					}
					return os.Rename(oldpath, newpath)
				})
			},
		},
		{
			name: "final directory fsync fails",
			inject: func(t *testing.T, exportPath string) {
				// The export directory is fsynced once after the move aside
				// and once after the publish; the second is refused.
				calls := 0
				withExportSyncDir(t, func(dir string) error {
					if filepath.Clean(dir) == exportPath {
						calls++
						if calls == 2 {
							return errInjectedExport
						}
					}
					return SyncDir(dir)
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exportPath := filepath.Join(t.TempDir(), "export")
			oldSrc, oldMeta := exportPairSource(t, []byte("the previous export's state machine"))
			if err := writeSnapshotExport(oldSrc, exportPath, oldMeta); err != nil {
				t.Fatalf("previous export: %v", err)
			}
			wantData, wantMeta := readExportPair(t, exportPath)

			tt.inject(t, exportPath)
			newSrc, newMeta := exportPairSource(t, []byte("a newer export, whose publication fails"))
			err := writeSnapshotExport(newSrc, exportPath, newMeta)
			var exportErr *SnapshotExportError
			if !errors.As(err, &exportErr) || !errors.Is(err, errInjectedExport) {
				t.Fatalf("failed publication = %v, want *SnapshotExportError wrapping the injected failure", err)
			}

			gotData, gotMeta := readExportPair(t, exportPath)
			if !bytes.Equal(gotData, wantData) {
				t.Fatalf("the previous export's snapshot.dat was replaced: %q, want %q", gotData, wantData)
			}
			if !bytes.Equal(gotMeta, wantMeta) {
				t.Fatal("the previous export's snapshot.meta was replaced")
			}
			if err := VerifySnapshotArtifact(exportPath, filepath.Join(exportPath, snapshotDataFile)); err != nil {
				t.Fatalf("the previous export no longer verifies: %v", err)
			}
			assertOnlyExportPair(t, exportPath)
		})
	}
}

// TestWriteSnapshotExport_FailedFreshPublishLeavesNothing: with no previous
// export, a failed publication removes the half it published, so the directory
// holds no record-less snapshot.dat.
func TestWriteSnapshotExport_FailedFreshPublishLeavesNothing(t *testing.T) {
	exportPath := filepath.Join(t.TempDir(), "export")
	withExportRename(t, func(oldpath, newpath string) error {
		if filepath.Base(newpath) == snapshotMetadataFile {
			return errInjectedExport
		}
		return os.Rename(oldpath, newpath)
	})
	src, meta := exportPairSource(t, []byte("a fresh export whose record cannot be published"))
	if err := writeSnapshotExport(src, exportPath, meta); !errors.Is(err, errInjectedExport) {
		t.Fatalf("failed publication = %v, want the injected failure", err)
	}
	entries, err := os.ReadDir(exportPath)
	if err != nil {
		t.Fatalf("read export dir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("the failed fresh export left %v behind", names)
	}
}

// TestWriteSnapshotExport_ReplacesThePreviousPair: a successful publication
// over an existing export leaves exactly the new pair, verifiable, and no
// backup of the old one.
func TestWriteSnapshotExport_ReplacesThePreviousPair(t *testing.T) {
	exportPath := filepath.Join(t.TempDir(), "export")
	oldSrc, oldMeta := exportPairSource(t, []byte("the previous export"))
	if err := writeSnapshotExport(oldSrc, exportPath, oldMeta); err != nil {
		t.Fatalf("previous export: %v", err)
	}
	newData := []byte("the newer export, published over it")
	newSrc, newMeta := exportPairSource(t, newData)
	if err := writeSnapshotExport(newSrc, exportPath, newMeta); err != nil {
		t.Fatalf("newer export: %v", err)
	}
	gotData, gotMeta := readExportPair(t, exportPath)
	if !bytes.Equal(gotData, newData) || !bytes.Equal(gotMeta, newMeta) {
		t.Fatal("the export directory does not hold the newer pair")
	}
	if err := VerifySnapshotArtifact(exportPath, filepath.Join(exportPath, snapshotDataFile)); err != nil {
		t.Fatalf("the newer export fails verification: %v", err)
	}
	assertOnlyExportPair(t, exportPath)
}

// TestWriteSnapshotExport_SweepsACrashedExportsLeftovers: an export that
// crashes after moving the previous pair aside — or while its temps are
// staged — leaves a backup directory or a temp file that its own cleanup
// never got to run on. They hold an obsolete snapshot, plaintext when the
// export decrypted it, and nothing else knows their names. The next export
// that publishes successfully removes every one of them.
func TestWriteSnapshotExport_SweepsACrashedExportsLeftovers(t *testing.T) {
	exportPath := filepath.Join(t.TempDir(), "export")
	if err := os.MkdirAll(exportPath, 0o750); err != nil {
		t.Fatalf("mkdir export: %v", err)
	}
	// What two crashed exports left: a backup holding a previous pair, and a
	// staged temp.
	orphanBackup, err := os.MkdirTemp(exportPath, exportBackupPattern)
	if err != nil {
		t.Fatalf("stage orphan backup: %v", err)
	}
	for _, name := range []string{snapshotDataFile, snapshotMetadataFile} {
		if err := os.WriteFile(filepath.Join(orphanBackup, name), []byte("obsolete plaintext"), 0o600); err != nil {
			t.Fatalf("stage orphan %s: %v", name, err)
		}
	}
	orphanTemp, err := os.CreateTemp(exportPath, exportTempPattern)
	if err != nil {
		t.Fatalf("stage orphan temp: %v", err)
	}
	if err := orphanTemp.Close(); err != nil {
		t.Fatalf("close orphan temp: %v", err)
	}

	// The sweep's removals must be made durable: some fsync of the export
	// directory has to happen once the leftovers are gone, not only the
	// publication's own, which runs while they are still there.
	var mu sync.Mutex
	sweptDurably := false
	withExportSyncDir(t, func(dir string) error {
		if dir == exportPath {
			_, backupErr := os.Stat(orphanBackup)
			_, tempErr := os.Stat(orphanTemp.Name())
			if os.IsNotExist(backupErr) && os.IsNotExist(tempErr) {
				mu.Lock()
				sweptDurably = true
				mu.Unlock()
			}
		}
		return SyncDir(dir)
	})

	src, meta := exportPairSource(t, []byte("the export that succeeds"))
	if err := writeSnapshotExport(src, exportPath, meta); err != nil {
		t.Fatalf("export: %v", err)
	}
	assertOnlyExportPair(t, exportPath)
	if !sweptDurably {
		t.Fatal("the export directory was not fsynced after the leftovers were removed")
	}
}

// TestWriteSnapshotExport_FailedPublishSweepsNothing: the sweep runs only
// once a new pair is durable. A failed export must leave a crashed export's
// backup alone — until something supersedes it, it may be the only copy of
// the previous pair an operator can restore.
func TestWriteSnapshotExport_FailedPublishSweepsNothing(t *testing.T) {
	exportPath := filepath.Join(t.TempDir(), "export")
	if err := os.MkdirAll(exportPath, 0o750); err != nil {
		t.Fatalf("mkdir export: %v", err)
	}
	orphanBackup, err := os.MkdirTemp(exportPath, exportBackupPattern)
	if err != nil {
		t.Fatalf("stage orphan backup: %v", err)
	}
	withExportSyncDir(t, func(string) error { return errInjectedExport })

	src, meta := exportPairSource(t, []byte("an export that fails"))
	if err := writeSnapshotExport(src, exportPath, meta); !errors.Is(err, errInjectedExport) {
		t.Fatalf("export = %v, want the injected failure", err)
	}
	if _, err := os.Stat(orphanBackup); err != nil {
		t.Fatalf("a failed export removed a crashed export's backup: %v", err)
	}
}
