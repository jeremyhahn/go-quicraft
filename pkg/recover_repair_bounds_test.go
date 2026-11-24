// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
)

// TestRecoverLogReader_RepairNeverAcceptsAGapTheSnapshotDoesNotCover checks recovery fails closed when the snapshot is below the hole.
func TestRecoverLogReader_RepairNeverAcceptsAGapTheSnapshotDoesNotCover(t *testing.T) {
	const shardID, replicaID = uint64(1), uint64(1)
	const snapshotIndex = uint64(100)
	db := memdb.New()

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
	// The snapshot covers 1..100; nothing covers 101..4999.
	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{Index: snapshotIndex, Term: 2}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID,
		State: logdb.State{Term: 2, Commit: 5002},
		Entries: []logdb.Entry{
			{Index: 5000, Term: 2, Cmd: []byte("new")},
			{Index: 5001, Term: 2, Cmd: []byte("new")},
			{Index: 5002, Term: 2, Cmd: []byte("new")},
		},
	}}); err != nil {
		t.Fatalf("seed far entries: %v", err)
	}
	if _, _, err := db.EntryRange(shardID, replicaID); !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("fixture: expected a holed index, got %v", err)
	}

	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer func() { _ = h.Close() }()

	lr := logdb.NewLogReader(shardID, replicaID, db)
	if _, err := h.recoverLogReader(lr, shardID, replicaID); err == nil {
		t.Fatal("recovery ACCEPTED a hole the recorded snapshot does not cover: " +
			"indices 101..4999 exist nowhere, yet the replica came up with the " +
			"compaction marker at 100 and its first entry at 5000")
	}
	// The repair DID run — it removed the prefix the snapshot subsumes — so the
	// fail-closed above comes from SetRange, not from the repair declining to
	// fire. Anything that weakens SetRange's gap check reopens this.
	first, _, rangeErr := db.EntryRange(shardID, replicaID)
	if rangeErr != nil {
		t.Fatalf("post-repair EntryRange: %v", rangeErr)
	}
	if first != 5000 {
		t.Fatalf("fixture drift: expected the repair to have removed the subsumed "+
			"prefix leaving first=5000, got first=%d", first)
	}
}
