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

package waldb

import "testing"

// TestRemoveEntriesTo_PinsCompactionMarkerSegment is the L1 regression test. A
// compaction marker is not tracked by any entry index, so its segment must be
// pinned in metaSegments (at runtime AND restored on recovery) — otherwise GC
// can reclaim the segment once its entries are compacted away, and compactedTo
// resets to 0 on the next restart (GetEntries then returns empty instead of
// ErrCompacted for compacted indices).
func TestRemoveEntriesTo_PinsCompactionMarkerSegment(t *testing.T) {
	dir := t.TempDir()
	open := func() *DB {
		db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}

	const shardID, replicaID = uint64(1), uint64(1)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	db := open()
	if err := db.SaveEntries(shardID, replicaID, makeEntries(1, 20, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.RemoveEntriesTo(shardID, replicaID, 10); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	// Runtime: the compaction marker's segment must be pinned.
	shard := db.getShard(shardID)
	shard.mu.Lock()
	m, ok := shard.metaSegments[key]
	runtimePinned := ok && m.hasCompact
	shard.mu.Unlock()
	if !runtimePinned {
		t.Fatal("compaction marker segment not pinned after RemoveEntriesTo")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Recovery: compactedTo restored AND the marker's segment re-pinned.
	db2 := open()
	defer db2.Close()
	shard2 := db2.getShard(shardID)
	shard2.mu.Lock()
	gotCompactedTo := shard2.compactedTo[key]
	m2, ok2 := shard2.metaSegments[key]
	recoveryPinned := ok2 && m2.hasCompact
	shard2.mu.Unlock()

	if gotCompactedTo != 10 {
		t.Fatalf("compactedTo after recovery = %d, want 10", gotCompactedTo)
	}
	if !recoveryPinned {
		t.Fatal("compaction marker segment not re-pinned after recovery (GC could reset compactedTo)")
	}
}
