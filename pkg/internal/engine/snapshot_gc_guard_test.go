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

// TestGCSnapshotDirs_RefusesToDeleteTheRecordedDirectory pins the guard
// the save path has always had (now in publishStagedSave) and GC did not.
//
// GC selects victims by INDEX ORDER alone. Higher-indexed directories that are
// unusable — partial receives left by a crash, for instance — therefore push
// the directory LogDB actually records into the removal window, and deleting
// it leaves the durable record pointing at nothing while every surviving
// directory is a partial.
func TestGCSnapshotDirs_RefusesToDeleteTheRecordedDirectory(t *testing.T) {
	base := t.TempDir()
	const shardID, replicaID = 1, 1

	// Enough directories that GC wants to remove the lowest ones.
	var dirs []string
	for _, idx := range []uint64{4200, 5000, 6000, 7000} {
		d := snapshotSaveDir(base, shardID, replicaID, idx)
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
		dirs = append(dirs, d)
	}
	recorded := filepath.Clean(dirs[0]) // the LOWEST index — first to be culled

	if err := gcSnapshotDirs(base, shardID, replicaID, recorded); err != nil {
		t.Fatalf("gcSnapshotDirs: %v", err)
	}

	if _, err := os.Stat(recorded); err != nil {
		t.Fatalf("GC deleted the directory LogDB records (%s): the durable "+
			"snapshot record now points at nothing, and every surviving "+
			"directory here is a higher-indexed partial", recorded)
	}
}
