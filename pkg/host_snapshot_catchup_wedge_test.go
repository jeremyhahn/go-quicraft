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
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
)

// TestSnapshotCatchUp_LeavesNoHoleInTheEntryIndex is the regression guard for a
// permanent, unrecoverable wedge reachable through ENTIRELY NORMAL OPERATION —
// no crash, no corruption, no operator error:
//
//	leader compacts -> a follower falls behind -> the leader sends
//	InstallSnapshot -> the follower records it and resumes replication ABOVE it
//	-> the follower restarts.
//
// The receive path recorded the snapshot but never advanced the log compaction
// floor, so the follower kept its old entries BELOW the snapshot while appending
// above it. Its durable entry index became {1..k} U {N+1..M} — a hole. EntryRange
// checks the stored count against the first..last span and correctly reports
// ErrLogGapDetected; recoverLogReader turns that into a StartShard failure, so
// the replica can NEVER be started again. There is no self-heal (SnapshotEntries
// has no default, so this replica never compacts on its own) and no repair path
// (RequestCompaction on an unloaded shard only runs segment GC).
//
// The fix advances the floor on receive, exactly as the local save path and
// ImportSnapshot do. This test drives the LogDB shape directly so it pins the
// durable-state invariant rather than one code path's plumbing.
func TestSnapshotCatchUp_LeavesNoHoleInTheEntryIndex(t *testing.T) {
	const shardID, replicaID = uint64(1), uint64(1)
	const snapshotIndex = uint64(6000)
	db := memdb.New()

	// The follower has fallen behind, holding entries 1..5.
	stale := make([]logdb.Entry, 0, 5)
	for i := uint64(1); i <= 5; i++ {
		stale = append(stale, logdb.Entry{Index: i, Term: 1, Cmd: []byte("old")})
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID,
		State:   logdb.State{Term: 1, Commit: 5},
		Entries: stale,
	}}); err != nil {
		t.Fatalf("seeding the lagging follower: %v", err)
	}

	// It catches up via InstallSnapshot: the snapshot is recorded AND the log
	// floor is advanced past it (what the receive path must do).
	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index: snapshotIndex, Term: 2,
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.RemoveEntriesTo(shardID, replicaID, snapshotIndex); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	// Replication resumes above the snapshot.
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID,
		State: logdb.State{Term: 2, Commit: snapshotIndex + 2},
		Entries: []logdb.Entry{
			{Index: snapshotIndex + 1, Term: 2, Cmd: []byte("new")},
			{Index: snapshotIndex + 2, Term: 2, Cmd: []byte("new")},
		},
	}}); err != nil {
		t.Fatalf("post-snapshot replication: %v", err)
	}

	// The restart. EntryRange is what recoverLogReader consults, and a holed
	// index makes StartShard fail permanently.
	first, count, err := db.EntryRange(shardID, replicaID)
	if errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("the entry index is HOLED after a snapshot catch-up "+
			"(first=%d count=%d): recoverLogReader fails and this replica can never "+
			"be started again — no self-heal, no repair path", first, count)
	}
	if err != nil {
		t.Fatalf("EntryRange after snapshot catch-up: %v", err)
	}
	// The surviving range must start above the snapshot: entries at or below it
	// are subsumed and must not linger.
	if count > 0 && first <= snapshotIndex {
		t.Fatalf("entries at or below the snapshot index survived: first=%d count=%d, "+
			"want a range starting above %d", first, count, snapshotIndex)
	}
}

// TestRecoverLogReader_RepairsHoleBelowRecordedSnapshot pins the self-heal that
// rescues replicas ALREADY wedged in the field.
//
// A replica whose log floor was never advanced past a snapshot it recorded holds
// {1..k} U {N+1..M}. EntryRange reports ErrLogGapDetected and StartShard fails
// forever — nothing compacts it and RequestCompaction cannot run on an unloaded
// shard. Two ways in: the receive path before it advanced the floor, and a crash
// inside ImportSnapshot between recording the snapshot and advancing the floor,
// a window that still exists by construction. A write-path fix alone cannot help
// a replica that is already in this state, so recovery repairs it: the snapshot
// subsumes the stale prefix, so removing it loses nothing.
func TestRecoverLogReader_RepairsHoleBelowRecordedSnapshot(t *testing.T) {
	const shardID, replicaID = uint64(1), uint64(1)
	const snapshotIndex = uint64(6000)
	db := memdb.New()

	// The wedged shape, written directly: stale prefix, a recorded snapshot far
	// above it, and post-snapshot entries.
	stale := make([]logdb.Entry, 0, 5)
	for i := uint64(1); i <= 5; i++ {
		stale = append(stale, logdb.Entry{Index: i, Term: 1, Cmd: []byte("old")})
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID,
		State:   logdb.State{Term: 1, Commit: 5},
		Entries: stale,
	}}); err != nil {
		t.Fatalf("seed stale prefix: %v", err)
	}
	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{Index: snapshotIndex, Term: 2}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID,
		State: logdb.State{Term: 2, Commit: snapshotIndex + 2},
		Entries: []logdb.Entry{
			{Index: snapshotIndex + 1, Term: 2, Cmd: []byte("new")},
			{Index: snapshotIndex + 2, Term: 2, Cmd: []byte("new")},
		},
	}}); err != nil {
		t.Fatalf("seed post-snapshot entries: %v", err)
	}
	if _, _, err := db.EntryRange(shardID, replicaID); !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("fixture: expected a holed index (ErrLogGapDetected), got %v", err)
	}

	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	lr := logdb.NewLogReader(shardID, replicaID, db)
	if _, err := h.recoverLogReader(lr, shardID, replicaID); err != nil {
		t.Fatalf("recovery did NOT repair a hole a recorded snapshot fully subsumes: %v; "+
			"this replica stays permanently unstartable with no repair path", err)
	}
	first, _ := lr.IndexRange()
	if first <= snapshotIndex {
		t.Fatalf("recovered range still starts at %d, at or below the snapshot index %d: "+
			"the subsumed prefix was not removed", first, snapshotIndex)
	}
}

// TestRecoverLogReader_FailsClosedOnGapWithNoSnapshot is the other half: a hole
// with NO snapshot to subsume it is real data loss and must still fail closed.
// Repairing it would silently discard entries nothing has captured.
func TestRecoverLogReader_FailsClosedOnGapWithNoSnapshot(t *testing.T) {
	const shardID, replicaID = uint64(1), uint64(1)
	db := memdb.New()

	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID,
		State:   logdb.State{Term: 1, Commit: 2},
		Entries: []logdb.Entry{{Index: 1, Term: 1, Cmd: []byte("a")}, {Index: 2, Term: 1, Cmd: []byte("a")}},
	}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// A hole with nothing recorded to cover it.
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID,
		State:   logdb.State{Term: 1, Commit: 900},
		Entries: []logdb.Entry{{Index: 900, Term: 1, Cmd: []byte("b")}},
	}}); err != nil {
		t.Fatalf("seed gap: %v", err)
	}

	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	lr := logdb.NewLogReader(shardID, replicaID, db)
	if _, err := h.recoverLogReader(lr, shardID, replicaID); err == nil {
		t.Fatal("recovery ACCEPTED a log hole with no snapshot covering it: real data " +
			"loss was silently repaired away instead of failing closed")
	}
}
