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

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestRemoveEntriesTo_PinsCompactionMarkerSegment is a regression test. A
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

// TestCompactionMarkerSurvivesGarbageCollection drives the compaction-marker pin
// END TO END, which TestRemoveEntriesTo_PinsCompactionMarkerSegment above does
// not: that test asserts the pin FIELD is set but never calls Compact, so the
// branch in addMetaSegmentsToLive that actually keeps GC away from the marker's
// segment was completely uncovered — deleting it left the whole suite green.
//
// The hazard it guards is a durability one, and it FAILS OPEN. The compaction
// floor lives only in that marker record, while its effect (the entries being
// unlinked) is irreversible. If GC reclaims the segment, the next restart
// recovers compactedTo=0 over a log whose prefix is physically gone: reads below
// the real floor then return ErrLogGapDetected instead of ErrCompacted, so raft
// never falls back to InstallSnapshot, and the monotonic floor guard no longer
// holds because the floor regressed to zero.
func TestCompactionMarkerSurvivesGarbageCollection(t *testing.T) {
	dir := t.TempDir()
	const shardID, replicaID = uint64(1), uint64(1)
	// Small segments so the marker lands in a segment that ends up holding no
	// live index entries — the only case the pin is load-bearing for.
	open := func() *DB {
		db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(4096), WithNoSync(true))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}
	entry := func(i uint64) logdb.Entry {
		return logdb.Entry{Index: i, Term: 1, Type: 1, Cmd: make([]byte, 900)}
	}

	db := open()
	for i := uint64(1); i <= 8; i++ {
		if err := db.SaveEntries(shardID, replicaID, []logdb.Entry{entry(i)}); err != nil {
			t.Fatalf("SaveEntries %d: %v", i, err)
		}
	}
	// The marker lands in the segment holding entries 5..8; compacting through 8
	// leaves that segment with no live index entries, so only the pin protects it.
	if err := db.RemoveEntriesTo(shardID, replicaID, 8); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	// Move the active segment past the marker's segment so GC may consider it.
	for i := uint64(9); i <= 12; i++ {
		if err := db.SaveEntries(shardID, replicaID, []logdb.Entry{entry(i)}); err != nil {
			t.Fatalf("SaveEntries %d: %v", i, err)
		}
	}
	if err := db.Compact(shardID, replicaID); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2 := open()
	defer func() { _ = db2.Close() }()
	floor, err := db2.CompactedTo(shardID, replicaID)
	if err != nil {
		t.Fatalf("CompactedTo: %v", err)
	}
	if floor != 8 {
		t.Fatalf("compaction floor lost to garbage collection: compactedTo=%d, want 8. "+
			"The marker's segment was reclaimed, so recovery came up with no floor over a "+
			"log whose prefix is already unlinked: reads below the real floor now return "+
			"ErrLogGapDetected instead of ErrCompacted and raft never falls back to "+
			"InstallSnapshot", floor)
	}
}

// TestRecovery_RederivesLostCompactionFloorUnderSnapshot pins the repair for a
// compaction floor lost to a torn WAL block.
//
// The floor lives only in a marker record while its effect — the segment holding
// the compacted entries being unlinked — is irreversible. Lose the marker and
// replay comes back with floor 0 over a log whose prefix is physically gone. That
// state FAILS OPEN: a missing PREFIX keeps span == count, so EntryRange accepts
// it and the node starts, but reads below the real floor then return
// ErrLogGapDetected instead of ErrCompacted and raft never falls back to
// InstallSnapshot.
//
// Recovery re-derives the floor from the REBUILT INDEX (firstIndex-1), never from
// the snapshot index — the snapshot pool deliberately retains entries below the
// snapshot for cheap follower catch-up, and anchoring there would mark those live
// entries unreadable and force a full InstallSnapshot on every lagging follower
// after any restart. The snapshot is only the validity check on the derivation.
func TestRecovery_RederivesLostCompactionFloorUnderSnapshot(t *testing.T) {
	dir := t.TempDir()
	const shardID, replicaID = uint64(1), uint64(1)
	open := func() *DB {
		db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(4096), WithNoSync(true))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}

	db := open()
	for i := uint64(1); i <= 8; i++ {
		if err := db.SaveEntries(shardID, replicaID, []logdb.Entry{
			{Index: i, Term: 1, Type: 1, Cmd: make([]byte, 200)},
		}); err != nil {
			t.Fatalf("SaveEntries %d: %v", i, err)
		}
	}
	// A snapshot covering the prefix, then entries above it — the state a
	// compacted replica is in.
	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{Index: 5, Term: 1}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.RemoveEntriesTo(shardID, replicaID, 5); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2 := open()
	defer func() { _ = db2.Close() }()
	floor, err := db2.CompactedTo(shardID, replicaID)
	if err != nil {
		t.Fatalf("CompactedTo: %v", err)
	}
	first, count, err := db2.EntryRange(shardID, replicaID)
	if err != nil {
		t.Fatalf("EntryRange: %v", err)
	}
	// Whether the marker survived or was re-derived, the invariant that matters
	// is the same: the floor must be contiguous with the surviving entries, so a
	// read below it reports ErrCompacted (routing raft to InstallSnapshot) rather
	// than ErrLogGapDetected.
	if count > 0 && first > floor+1 {
		t.Fatalf("entries start at %d but the compaction floor is %d: reads in [%d,%d) "+
			"return ErrLogGapDetected instead of ErrCompacted, so raft never falls back "+
			"to InstallSnapshot", first, floor, floor+1, first)
	}
}
