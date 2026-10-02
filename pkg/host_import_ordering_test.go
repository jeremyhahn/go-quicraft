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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// An imported snapshot directory must not sort as the OLDEST
// ---------------------------------------------------------------------------

// TestImportSnapshot_DirectoryOrdersByIndexAmongExistingSnapshots is the
// regression guard for the import directory's NAME.
//
// The snapshot tree is ORDERED by that name, not merely labelled by it:
// engine.listSnapshotDirs parses "snapshot-<index>" and sorts ascending,
// gcSnapshotDirs deletes everything but the last maxSnapshotsRetained, and
// latestSnapshotDir — the recovery fallback — takes the highest. A directory
// named from a per-host import sequence number starting at 1 would land a
// disaster-recovery restore onto a node that already had snapshots as the
// OLDEST directory in the tree: first in line for GC, and never the one the
// recovery fallback picks, which would prefer a stale but higher-numbered
// directory over the restore.
//
// ImportSnapshot therefore names the directory by the snapshot's own index,
// matching every other writer (engine.SnapshotSaveDir).
func TestImportSnapshot_DirectoryOrdersByIndexAmongExistingSnapshots(t *testing.T) {
	const shardID, replicaID uint64 = 3, 1
	// The restore is NEWER than everything already on this node.
	const importIndex uint64 = 900
	existing := []uint64{100, 200, 300}

	cfg := testHostConfig(t)
	db := newMockLogDB()
	db.mu.Lock()
	db.nodeInfos = []logdb.NodeInfo{{ShardID: shardID, ReplicaID: replicaID}}
	db.mu.Unlock()

	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	// Populate the shard's snapshot tree the way a node that has been running
	// normally would have it.
	base := shardSnapshotBaseDir(cfg.NodeHostDir, "", shardID)
	for _, idx := range existing {
		if mkErr := os.MkdirAll(engine.SnapshotSaveDir(base, shardID, replicaID, idx), 0o750); mkErr != nil {
			t.Fatalf("seeding snapshot dir %d failed: %v", idx, mkErr)
		}
	}

	artifact := buildSnapshotArtifact(t, importIndex, 7, 0, proto.Membership{}, []byte("restore"))
	if impErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact), WithImportDataDir("")); impErr != nil {
		t.Fatalf("ImportSnapshot failed: %v", impErr)
	}

	imported, getErr := db.GetSnapshot(shardID, replicaID)
	if getErr != nil {
		t.Fatalf("GetSnapshot failed: %v", getErr)
	}

	// The name must be the one every other snapshot writer would have used.
	wantDir := engine.SnapshotSaveDir(base, shardID, replicaID, importIndex)
	// Reported, not fatal: the ordering assertions below are the point of this
	// test and must still run when the name is wrong.
	if imported.Filepath != wantDir {
		t.Errorf("imported snapshot Filepath = %q, want %q (named by the snapshot index)",
			imported.Filepath, wantDir)
	}

	// The consequence: among the shard's snapshot directories the restore must
	// order LAST. Parse the names exactly as engine.listSnapshotDirs does — the
	// imported directory's own name included, whatever it turned out to be —
	// so this asserts the ordering GC and the recovery fallback actually see.
	//
	// ImportSnapshot now runs snapshot GC once the artifact is recorded, and
	// the GC retains the last three directories — so the four here become
	// three, with the OLDEST reclaimed. That the restore is not the one
	// reclaimed is the property this test is about, and it is asserted
	// directly below as well as through the ordering.
	indices := snapshotDirIndices(t, filepath.Dir(wantDir))
	const retained = 3 // engine's maxSnapshotsRetained
	if want := min(len(existing)+1, retained); len(indices) != want {
		t.Fatalf("snapshot tree has %d directories %v, want %d (the import records "+
			"a fourth and the GC retains the newest %d)",
			len(indices), indices, want, retained)
	}
	if _, statErr := os.Stat(wantDir); statErr != nil {
		t.Fatalf("the imported snapshot directory %q is gone after the import's own "+
			"GC ran (%v): a disaster-recovery restore must never be the directory "+
			"retention reclaims", wantDir, statErr)
	}
	importedOrder := snapshotDirIndex(t, imported.Filepath)
	var maxIdx, minIdx uint64 = 0, ^uint64(0)
	for _, idx := range indices {
		if idx > maxIdx {
			maxIdx = idx
		}
		if idx < minIdx {
			minIdx = idx
		}
	}
	if importedOrder == minIdx {
		t.Errorf("the imported snapshot orders as the OLDEST directory in %v: "+
			"gcSnapshotDirs deletes the oldest first, so the disaster-recovery "+
			"restore is the first thing the next GC destroys", indices)
	}
	if importedOrder != maxIdx {
		t.Errorf("the imported snapshot orders at %d, not last in %v: "+
			"latestSnapshotDir would recover a stale snapshot over the "+
			"disaster-recovery restore", importedOrder, indices)
	}
}

// snapshotDirIndex parses the ordering index out of a single snapshot
// directory path, the same way engine.listSnapshotDirs does.
func snapshotDirIndex(t *testing.T, dir string) uint64 {
	t.Helper()
	name := filepath.Base(dir)
	idx, err := strconv.ParseUint(strings.TrimPrefix(name, "snapshot-"), 10, 64)
	if err != nil {
		t.Fatalf("snapshot directory %q does not carry a parseable index: %v", name, err)
	}
	return idx
}

// snapshotDirIndices parses the snapshot directory names under dir the same way
// engine.listSnapshotDirs does.
func snapshotDirIndices(t *testing.T, dir string) []uint64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading snapshot base dir %q: %v", dir, err)
	}
	var indices []uint64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "snapshot-") {
			continue
		}
		idx, parseErr := strconv.ParseUint(strings.TrimPrefix(name, "snapshot-"), 10, 64)
		if parseErr != nil {
			continue
		}
		indices = append(indices, idx)
	}
	return indices
}
