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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
)

// ---------------------------------------------------------------------------
// A killed import must not leave a PERMANENT plaintext state machine
// ---------------------------------------------------------------------------

// TestStartShard_ReclaimsCrashOrphanedImportStaging is the regression guard for
// the import staging directory's plaintext window.
//
// ImportSnapshot writes the artifact PLAINTEXT (ExportSnapshot decrypts before
// writing, so an artifact never carries ciphertext) and re-encrypts it under
// this host's barrier immediately afterwards. A process killed between the two
// leaves the COMPLETE PLAINTEXT STATE MACHINE in the staging directory.
//
// Only a dedicated sweep reclaims it. Every other sweep and every scan on the
// host enumerates through engine.listSnapshotDirs, which skips any name without
// the "snapshot-" prefix — the property that deliberately hides a half-written
// import from snapshot GC, from the recovery fallback and from the DEK
// purge-floor scan. Without the sweep the staging name would convert a
// plaintext exposure that GC would eventually have reclaimed (a partial write under an index-named directory is
// deleted once maxSnapshotsRetained newer snapshots exist) into a PERMANENT one
// on a barrier-configured host.
//
// The assertion is on the CONTENT: the orphan's plaintext must not exist
// anywhere under the shard's snapshot tree afterwards. "The sweeper ran" is not
// the property; "the plaintext is gone" is.
func TestStartShard_ReclaimsCrashOrphanedImportStaging(t *testing.T) {
	// A barrier is configured, so everything the shard keeps at rest is
	// ciphertext and any occurrence of this marker is a real exposure.
	f := newLiveImportFixture(t, true)

	if err := f.importArtifact(t, 900, []byte("LIVE-SNAPSHOT")); err != nil {
		t.Fatalf("ImportSnapshot at 900 failed: %v", err)
	}
	live := f.capture(t)
	if live.record.Epoch == 0 {
		t.Fatal("the import recorded Epoch 0 although a barrier is configured; this " +
			"test cannot observe the plaintext exposure it is guarding")
	}

	replicaBase := filepath.Dir(live.record.Filepath)
	plaintext := []byte("PLAINTEXT-STATE-MACHINE-FROM-A-KILLED-IMPORT")

	// The tree a SIGKILL mid-import left behind when the import wrote its
	// plaintext before encrypting it: a staging directory named from the same
	// pattern the import creates, holding the artifact in the clear. The
	// import now seals as it writes, but the sweep must still reclaim such a
	// directory — and a plaintext deployment's import legitimately holds one.
	orphan := filepath.Join(replicaBase,
		strings.Replace(engine.ImportStageDirPattern, "*", "3517658391", 1))
	if err := os.MkdirAll(orphan, 0o750); err != nil {
		t.Fatalf("seeding the orphaned staging directory: %v", err)
	}
	orphanData := filepath.Join(orphan, "snapshot.dat")
	if err := os.WriteFile(orphanData, plaintext, 0o600); err != nil {
		t.Fatalf("seeding the orphaned plaintext artifact: %v", err)
	}

	failuresBefore := engine.StaleDecryptSweepFailures()

	// The restart after the kill.
	assertShardRecoversPayload(t, f, []byte("LIVE-SNAPSHOT"))

	if _, err := os.Stat(orphan); err == nil {
		t.Errorf("the orphaned staging directory %s survived the restart; nothing else "+
			"on the host enumerates that name, so the plaintext state machine is on "+
			"disk for good", orphan)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", orphan, err)
	}

	// The plaintext itself, not just its directory entry, must be gone from the
	// whole shard tree.
	assertPlaintextAbsent(t, f.base, plaintext)

	// The sweep is scoped: the live snapshot the shard just recovered from is
	// untouched, byte for byte.
	assertUnchanged(t, live, f.capture(t))

	if got := engine.StaleDecryptSweepFailures(); got != failuresBefore {
		t.Errorf("StaleDecryptSweepFailures %d -> %d: the sweep could not reclaim or "+
			"even inspect a path, which is the condition that counter exists to raise",
			failuresBefore, got)
	}
}

// TestStartShard_ReclaimsOrphanedStagingWithoutASnapshotRecord covers the
// disaster-recovery shape the sweep exists for: the import died on its FIRST
// attempt, so the shard has no recorded snapshot at all and recovery never
// runs. The plaintext orphan must still be reclaimed, on the start itself.
func TestStartShard_ReclaimsOrphanedStagingWithoutASnapshotRecord(t *testing.T) {
	f := newLiveImportFixture(t, true)

	replicaBase := engine.SnapshotSaveDir(f.base, f.shardID, f.replicaID, 1)
	replicaBase = filepath.Dir(replicaBase)
	if err := os.MkdirAll(replicaBase, 0o750); err != nil {
		t.Fatalf("creating the replica base directory: %v", err)
	}
	plaintext := []byte("PLAINTEXT-FROM-A-FIRST-IMPORT-THAT-NEVER-FINISHED")
	orphan := filepath.Join(replicaBase,
		strings.Replace(engine.ImportStageDirPattern, "*", "0084419912", 1))
	if err := os.MkdirAll(orphan, 0o750); err != nil {
		t.Fatalf("seeding the orphaned staging directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "snapshot.dat"), plaintext, 0o600); err != nil {
		t.Fatalf("seeding the orphaned plaintext artifact: %v", err)
	}

	scfg := testShardConfig(f.shardID, f.replicaID)
	if err := f.host.StartShard(map[uint64]string{f.replicaID: "127.0.0.1:5000"},
		false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	t.Cleanup(func() { _ = f.host.StopShard(f.shardID) })

	if _, err := os.Stat(orphan); err == nil {
		t.Errorf("the orphaned staging directory %s survived a start with no snapshot "+
			"record; the sweep must not depend on snapshot recovery running", orphan)
	}
	assertPlaintextAbsent(t, f.base, plaintext)
}

// assertPlaintextAbsent walks the shard's whole snapshot tree and fails if any
// file contains want. The tree is walked rather than one path checked so a
// staging directory that was merely RENAMED out of the way is still caught.
func assertPlaintextAbsent(t *testing.T, root string, want []byte) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, readErr := os.ReadFile(path) // #nosec G304 -- test-owned temp tree
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(data, want) {
			t.Errorf("%s still holds the killed import's PLAINTEXT state machine", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
}
