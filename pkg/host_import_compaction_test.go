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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestImportSnapshot_RemovesEntriesAtOrBelowSnapshotIndex is the regression
// guard for the offline disaster-recovery log-gap bug.
//
// The offline restore flow bootstraps a shard (StartShard) before importing, so
// the shard can be imported into at all — ImportSnapshot refuses an unrecorded
// shard. That bootstrap writes an initial configuration entry at index 1.
// Importing an artifact whose snapshot index is higher records the snapshot,
// and without advancing the floor would leave the index-1 entry orphaned below
// it: because the default CompactionOverhead (5000) dwarfs a freshly restored
// node's entry count, the running snapshotter's compaction never fires to clean
// it up. The shard would then append post-restore writes, and on the next
// restart log recovery would rebuild the entry index as {1, ...tail} whose span
// exceeds the entry count — reported as ErrLogGapDetected — and host init would
// fail closed. The restored node would be unrecoverable without wiping and
// restoring again.
//
// ImportSnapshot must therefore advance the compaction floor to the imported
// snapshot index, exactly as a normal InstallSnapshot does, removing every entry
// at or below it.
func TestImportSnapshot_RemovesEntriesAtOrBelowSnapshotIndex(t *testing.T) {
	f := newLiveImportFixture(t, false)

	// Simulate the bootstrap entry StartShard writes before the import.
	if err := f.db.SaveState([]logdb.Update{{
		ShardID:   f.shardID,
		ReplicaID: f.replicaID,
		Entries:   []logdb.Entry{{Index: 1, Term: 1}},
	}}); err != nil {
		t.Fatalf("seed bootstrap entry: %v", err)
	}
	if first, count, err := f.db.EntryRange(f.shardID, f.replicaID); err != nil || first != 1 || count != 1 {
		t.Fatalf("pre-import EntryRange = (%d, %d, %v), want (1, 1, nil)", first, count, err)
	}

	// Import an artifact whose snapshot index sits well above the orphan entry.
	const snapIndex uint64 = 10
	if err := f.importArtifact(t, snapIndex, []byte("restored-state")); err != nil {
		t.Fatalf("importArtifact at index %d: %v", snapIndex, err)
	}

	// The import must have removed every entry at or below the snapshot index.
	first, count, err := f.db.EntryRange(f.shardID, f.replicaID)
	if err != nil {
		t.Fatalf("post-import EntryRange: %v", err)
	}
	if count != 0 {
		t.Fatalf("post-import EntryRange = (first=%d, count=%d), want no entries: the "+
			"pre-snapshot entry at index 1 survived below the imported snapshot", first, count)
	}

	// End-to-end guard: a post-restore write must leave a CONTIGUOUS log. With
	// the orphan still present the log is {1, 11}, which EntryRange reports as
	// ErrLogGapDetected — precisely the failure that wedges the next restart.
	if err := f.db.SaveState([]logdb.Update{{
		ShardID:   f.shardID,
		ReplicaID: f.replicaID,
		Entries:   []logdb.Entry{{Index: snapIndex + 1, Term: 1}},
	}}); err != nil {
		t.Fatalf("post-restore write: %v", err)
	}
	if _, _, err := f.db.EntryRange(f.shardID, f.replicaID); err != nil {
		t.Fatalf("post-restore EntryRange returned %v; the imported snapshot left the log "+
			"non-contiguous, so the restored node fails recovery on its next restart", err)
	}
}
