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
	"os"
	"path/filepath"
	"testing"
)

// TestGCSnapshotDirs_ReclaimsReceivePathDirectories covers the GC entry point used by the receive path and ImportSnapshot.
func TestGCSnapshotDirs_ReclaimsReceivePathDirectories(t *testing.T) {
	const shardID, replicaID = 7, 3
	base := t.TempDir()

	// Simulate the receive path: several catch-ups, each writing its own
	// snapshot directory with no save ever running.
	indexes := []uint64{10, 20, 30, 40, 50}
	for _, idx := range indexes {
		dir := SnapshotSaveDir(base, shardID, replicaID, idx)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("MkdirAll(%s): %v", dir, err)
		}
		if err := os.WriteFile(filepath.Join(dir, snapshotDataFile), []byte("state"), 0o600); err != nil {
			t.Fatalf("write artifact: %v", err)
		}
	}

	if err := GCSnapshotDirs(base, shardID, replicaID, ""); err != nil {
		t.Fatalf("GCSnapshotDirs: %v", err)
	}

	dirs, err := listSnapshotDirs(base, shardID, replicaID)
	if err != nil {
		t.Fatalf("listSnapshotDirs: %v", err)
	}
	if len(dirs) != maxSnapshotsRetained {
		t.Fatalf("retained %d snapshot directories, want %d: an unreclaimed receive "+
			"path accumulates one state-machine image per catch-up and pins the "+
			"host-wide DEK purge floor", len(dirs), maxSnapshotsRetained)
	}
	want := indexes[len(indexes)-maxSnapshotsRetained:]
	for i, d := range dirs {
		if d.index != want[i] {
			t.Fatalf("retained index %d at position %d, want %d: retention must keep "+
				"the NEWEST snapshots", d.index, i, want[i])
		}
	}

	// Idempotent: a second call with nothing to reclaim must be a no-op.
	if err := GCSnapshotDirs(base, shardID, replicaID, ""); err != nil {
		t.Fatalf("second GCSnapshotDirs: %v", err)
	}
	dirs, err = listSnapshotDirs(base, shardID, replicaID)
	if err != nil {
		t.Fatalf("listSnapshotDirs: %v", err)
	}
	if len(dirs) != maxSnapshotsRetained {
		t.Fatalf("a repeat GC changed the retained set to %d directories", len(dirs))
	}
}

// TestGCSnapshotDirs_UnderRetentionKeepsEverything verifies the entry point does
// not evict when the shard is still below the retention limit — the state a
// freshly caught-up follower is in.
func TestGCSnapshotDirs_UnderRetentionKeepsEverything(t *testing.T) {
	const shardID, replicaID = 8, 1
	base := t.TempDir()

	for _, idx := range []uint64{1, 2} {
		dir := SnapshotSaveDir(base, shardID, replicaID, idx)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
	}
	if err := GCSnapshotDirs(base, shardID, replicaID, ""); err != nil {
		t.Fatalf("GCSnapshotDirs: %v", err)
	}
	dirs, err := listSnapshotDirs(base, shardID, replicaID)
	if err != nil {
		t.Fatalf("listSnapshotDirs: %v", err)
	}
	if len(dirs) != 2 {
		t.Fatalf("GC removed a directory while under the retention limit: %d left", len(dirs))
	}
}
