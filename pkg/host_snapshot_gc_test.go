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
	"strconv"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// The snapshot receive path creates one directory per InstallSnapshot and, until
// it called engine.GCSnapshotDirs, reclaimed none of them: gcSnapshotDirs had a
// single caller, the engine's own saveSnapshot, and a shard configured with
// SnapshotEntries == 0 — a supported setting with NO default, so the
// out-of-the-box state — never runs saveSnapshot at all.
//
// A follower that repeatedly falls behind the leader's compaction floor
// therefore accumulated one full state-machine image per catch-up, forever. The
// disk cost is the visible half; the other half is that snapshotFloorScan folds
// EVERY retained directory of EVERY loaded node into the host-wide DEK purge
// floor, so one such shard's stale directories stop a co-hosted shard from ever
// purging an epoch key and old key material is never zeroed.

// TestSnapshotReceive_ReclaimsSupersededSnapshotDirectories drives four
// consecutive snapshot receives — the shape of a follower that keeps falling
// behind — and requires the tree to settle at the retention limit instead of
// growing without bound.
func TestSnapshotReceive_ReclaimsSupersededSnapshotDirectories(t *testing.T) {
	const shardID, replicaID, fromID = uint64(77), uint64(1), uint64(2)
	// engine's maxSnapshotsRetained. Kept as a local so the assertion reads
	// against a number rather than against the implementation.
	const retained = 3

	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	eng := engine.NewEngine(cfg, ldb, &noopSender{}, nil, nil, nil)
	eng.Start()
	t.Cleanup(eng.Stop)

	// No h.fsys override: the GC works on the real filesystem, so the fixture
	// has to as well.
	h := &Host{cfg: cfg, logdb: ldb, registry: registry.NewRegistry()}
	handler := &hostMessageHandler{engine: noRecoveryDeliverer{eng}, host: h}
	authorizeSnapshotPath(t, eng, h, shardID, replicaID, fromID)

	node := eng.GetNode(shardID)
	if node == nil {
		t.Fatal("engine node not loaded")
	}

	payload := bytes.Repeat([]byte("state-machine-image"), 8)
	indices := []uint64{100, 200, 300, 400}
	for i, index := range indices {
		chunks := []proto.SnapshotChunk{{
			ShardID:    shardID,
			ReplicaID:  replicaID,
			From:       fromID,
			Index:      index,
			Term:       uint64(i + 1),
			ChunkID:    0,
			ChunkCount: 1,
			ChunkSize:  uint64(len(payload)),
			FileSize:   uint64(len(payload)),
			Data:       payload,
			Membership: proto.Membership{Addresses: map[uint64]string{
				replicaID: "127.0.0.1:6001",
				fromID:    "127.0.0.1:6002",
			}},
		}}
		if err := handler.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
			t.Fatalf("HandleSnapshot at index %d: %v", index, err)
		}
	}

	base := filepath.Dir(node.SnapshotSaveDir(indices[0]))
	got := receivedSnapshotIndices(t, base)
	if len(got) != retained {
		t.Fatalf("the shard retains %d snapshot directories %v after %d receives, "+
			"want %d. Received snapshot directories are reclaimed by nothing else: "+
			"a shard with SnapshotEntries == 0 never runs the engine's own save "+
			"path, so every catch-up leaves another full state-machine image on "+
			"disk and pins the host-wide DEK purge floor",
			len(got), got, len(indices), retained)
	}
	// Retention keeps the NEWEST, and the newest is the one LogDB references.
	for _, index := range indices[len(indices)-retained:] {
		if !containsIndex(got, index) {
			t.Fatalf("snapshot %d is missing from the retained set %v; retention "+
				"must reclaim the oldest, never the most recent", index, got)
		}
	}
	if containsIndex(got, indices[0]) {
		t.Fatalf("the oldest snapshot %d survived in %v: nothing reclaimed it",
			indices[0], got)
	}
	snap, err := ldb.GetSnapshot(shardID, replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if _, statErr := os.Stat(snap.Filepath); statErr != nil {
		t.Fatalf("the snapshot LogDB references (%q) is gone after the GC ran (%v); "+
			"the shard cannot start", snap.Filepath, statErr)
	}
}

// receivedSnapshotIndices lists the snapshot directory indices under base, the
// way engine.listSnapshotDirs parses them.
func receivedSnapshotIndices(t *testing.T, base string) []uint64 {
	t.Helper()
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatalf("reading the snapshot base dir %q: %v", base, err)
	}
	var indices []uint64
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "snapshot-") {
			continue
		}
		idx, parseErr := strconv.ParseUint(strings.TrimPrefix(entry.Name(), "snapshot-"), 10, 64)
		if parseErr != nil {
			continue
		}
		indices = append(indices, idx)
	}
	return indices
}

func containsIndex(indices []uint64, want uint64) bool {
	for _, idx := range indices {
		if idx == want {
			return true
		}
	}
	return false
}
