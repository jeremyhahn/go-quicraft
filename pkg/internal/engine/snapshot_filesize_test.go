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
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// recoverFixture builds a shard tree with one recorded snapshot whose data
// file holds payload, and returns the pool, the request and the LogDB so a
// test can perturb the record or the file.
type recoverFixture struct {
	pool     *snapshotPool
	req      snapshotRequest
	ldb      *snapshotTestLogDB
	dataPath string
	ss       *snapshotTestSnapshotter
}

func newRecoverFixture(t *testing.T, payload []byte, recordedSize uint64) *recoverFixture {
	t.Helper()
	const shardID, replicaID, index, term = uint64(21), uint64(1), uint64(100), uint64(3)

	baseDir := t.TempDir()
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })

	dir := snapshotSaveDir(baseDir, shardID, replicaID, index)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dataPath := filepath.Join(dir, snapshotDataFile)
	if err := os.WriteFile(dataPath, payload, 0o600); err != nil {
		t.Fatalf("write snapshot data: %v", err)
	}
	if err := writeTestSnapshotMetadata(dir, index, term, shardID, replicaID, 0,
		config.NoCompression); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	ldb := newSnapshotTestLogDB()
	if err := ldb.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index:    index,
		Term:     term,
		Filepath: dir,
		FileSize: recordedSize,
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	pool := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
	ss := newSnapshotTestSnapshotter(0)
	node := NewNode(nil, nil, nil, cfg, ss, baseDir, nil)

	return &recoverFixture{
		pool:     pool,
		req:      snapshotRequest{shardID: shardID, replicaID: replicaID, node: node, save: false},
		ldb:      ldb,
		dataPath: dataPath,
		ss:       ss,
	}
}

// snapshotPayload builds a snapshot.dat body the test snapshotter accepts: the
// recovered index as a little-endian uint64, padded out to n bytes.
func snapshotPayload(index uint64, n int) []byte {
	buf := make([]byte, n)
	binary.LittleEndian.PutUint64(buf[:8], index)
	return buf
}

// TestRecoverSnapshot_RefusesATruncatedArtifact is the regression guard for a
// FileSize that was written by every save and read by no load.
//
// A snapshot.dat truncated after it was recorded — a volume that filled between
// the write and a later flush, a partial restore, a torn copy — was recovered
// as if complete. The outcome is not an error but a SHORTER state machine: the
// replica comes up at the snapshot's index holding a prefix of its state,
// silently diverged from every peer at the same log position with every Raft
// invariant intact.
func TestRecoverSnapshot_RefusesATruncatedArtifact(t *testing.T) {
	const full = 4096
	f := newRecoverFixture(t, snapshotPayload(100, full), full)

	// Truncate the artifact AFTER it was recorded, and re-stamp snapshot.meta
	// over the truncated bytes: the file and its record replaced TOGETHER — a
	// partial restore that brought both from the wrong place — so the meta
	// hash agrees with the file and FileSize, which lives in LogDB, is the
	// only check that can see the difference. The record still claims the full
	// size; the file no longer has it.
	if err := os.Truncate(f.dataPath, full/2); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := writeSnapshotMetadata(filepath.Dir(f.dataPath), 100, 3, 21, 1, 0,
		config.NoCompression); err != nil {
		t.Fatalf("re-stamp metadata: %v", err)
	}

	err := f.pool.recoverSnapshot(f.req)
	if err == nil {
		t.Fatal("recoverSnapshot accepted a TRUNCATED snapshot: the state machine " +
			"is recovered from a prefix of its own state and the replica serves " +
			"reads from it")
	}
	if !errors.Is(err, ErrSnapshotSizeMismatch) {
		t.Fatalf("recoverSnapshot = %v, want ErrSnapshotSizeMismatch", err)
	}
	var mismatch *SnapshotSizeMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %T, want *SnapshotSizeMismatchError with both sizes", err)
	}
	if mismatch.Recorded != full || mismatch.OnDisk != full/2 {
		t.Errorf("mismatch = %+v, want Recorded %d and OnDisk %d",
			mismatch, full, full/2)
	}
	if f.ss.LastApplied() != 0 {
		t.Errorf("the state machine was recovered to %d despite the refusal; the "+
			"check must run BEFORE anything reads the artifact", f.ss.LastApplied())
	}
}

// TestRecoverSnapshot_UnrecordedSizeIsNotChecked pins the upgrade path: a
// record written before FileSize was populated carries zero, which claims
// nothing. Refusing those would make a new binary unable to recover the
// snapshots it inherited — a worse failure than not checking.
func TestRecoverSnapshot_UnrecordedSizeIsNotChecked(t *testing.T) {
	const full = 4096
	f := newRecoverFixture(t, snapshotPayload(100, full), 0)

	if err := f.pool.recoverSnapshot(f.req); err != nil {
		t.Fatalf("recoverSnapshot with FileSize 0 failed: %v", err)
	}
	if f.ss.LastApplied() != 100 {
		t.Fatalf("recovered lastApplied = %d, want 100", f.ss.LastApplied())
	}
}

// TestVerifySnapshotFileSize covers the cases of the helper's contract that
// no recovery test reaches: a file longer than its record, and the
// missing-file case the recovery paths rely on it to report. A matching, an
// unrecorded and a short size are asserted through recoverSnapshot.
func TestVerifySnapshotFileSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, snapshotDataFile)
	if err := os.WriteFile(path, make([]byte, 64), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := VerifySnapshotFileSize(path, 63); !errors.Is(err, ErrSnapshotSizeMismatch) {
		t.Errorf("a long file reported %v, want ErrSnapshotSizeMismatch", err)
	}
	if err := VerifySnapshotFileSize(filepath.Join(dir, "absent.dat"), 64); err == nil {
		t.Error("a missing artifact reported no error")
	}
}

// ---------------------------------------------------------------------------
// Snapshot membership must be the one in effect AT the snapshot's index
// ---------------------------------------------------------------------------

// newSaveFixtureWithMembership builds a save-side fixture whose peer publishes
// a membership carrying configChangeID, and whose state machine is at
// lastApplied. The two are independent on purpose: the snapshot's index comes
// from the APPLY frontier and its membership from the COMMIT frontier, and this
// is what lets a test drive them apart the way a config change committed but
// not yet applied does in production.
func newSaveFixtureWithMembership(
	t *testing.T, lastApplied, configChangeID uint64,
) (*snapshotPool, snapshotRequest, *snapshotTestSnapshotter) {
	t.Helper()
	const shardID, replicaID = uint64(31), uint64(1)

	ldb := newTestLogDB()
	lr := logdb.NewLogReader(shardID, replicaID, ldb)
	if err := lr.ApplySnapshot(proto.Snapshot{
		Index: 0,
		Term:  0,
		Membership: proto.Membership{
			ConfigChangeID: configChangeID,
			Addresses: map[uint64]string{
				1: "localhost:10001",
				2: "localhost:10002",
				3: "localhost:10003",
			},
		},
	}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	if got := peer.GetMembership().ConfigChangeID; got != configChangeID {
		t.Fatalf("setup: peer publishes ConfigChangeID %d, want %d; the fixture "+
			"cannot drive the two frontiers apart", got, configChangeID)
	}

	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })
	pool := newSnapshotPool(2, newSnapshotTestLogDB(), nil, nil, nil, stopC, nil)
	ss := newSnapshotTestSnapshotter(lastApplied)
	node := NewNode(peer, nil, lr, cfg, ss, t.TempDir(), nil)

	return pool, snapshotRequest{
		shardID: shardID, replicaID: replicaID, node: node, save: true,
	}, ss
}

// TestSaveSnapshot_RefusesAMembershipAheadOfTheSnapshotIndex is the regression
// guard for the two-frontier mismatch.
//
// The snapshot's index is the APPLY frontier; its membership is folded at
// COMMIT time, and commit runs ahead of apply — necessarily so here, because
// the saving gate this path holds stops applies while the step worker keeps
// committing. A config change committed at C above the snapshot's index was
// therefore recorded into a snapshot taken below it, so the artifact claimed a
// configuration that did not exist at its own index: a follower installing it
// adopts membership as of C, sets its fold marker to the snapshot's index, and
// re-folds every config change in between — while computing quorums, until it
// catches up, from a voting set the log at that index does not justify.
func TestSaveSnapshot_RefusesAMembershipAheadOfTheSnapshotIndex(t *testing.T) {
	pool, req, _ := newSaveFixtureWithMembership(t, 100, 500)

	index, err := pool.saveSnapshot(req)
	if err == nil {
		t.Fatalf("saveSnapshot recorded index %d with a membership whose last "+
			"config change is at index 500: the snapshot claims a configuration "+
			"that did not exist at its own index", index)
	}
	if !errors.Is(err, ErrSnapshotMembershipAhead) {
		t.Fatalf("saveSnapshot = %v, want ErrSnapshotMembershipAhead", err)
	}
	var ahead *SnapshotMembershipAheadError
	if !errors.As(err, &ahead) {
		t.Fatalf("error = %T, want *SnapshotMembershipAheadError", err)
	}
	if ahead.SnapshotIndex != 100 || ahead.ConfigChangeID != 500 {
		t.Errorf("error = %+v, want SnapshotIndex 100 and ConfigChangeID 500", ahead)
	}
	if !ahead.Retryable() {
		t.Error("the condition clears as soon as apply catches up; it must be retryable")
	}
	if index != 0 {
		t.Errorf("saveSnapshot returned index %d alongside the refusal", index)
	}
}

// TestSaveSnapshot_AcceptsAMembershipAtOrBelowTheSnapshotIndex is the other
// half. Anything applied at or below the snapshot's index was committed — and
// therefore folded — first, so a ConfigChangeID at or below it IS the
// membership in effect there and must not be refused. Without this the guard
// above would pass against a saveSnapshot that refused every snapshot.
func TestSaveSnapshot_AcceptsAMembershipAtOrBelowTheSnapshotIndex(t *testing.T) {
	for _, configChangeID := range []uint64{0, 50, 100} {
		pool, req, _ := newSaveFixtureWithMembership(t, 100, configChangeID)
		index, err := pool.saveSnapshot(req)
		if err != nil {
			t.Fatalf("saveSnapshot with ConfigChangeID %d failed: %v", configChangeID, err)
		}
		if index != 100 {
			t.Fatalf("saveSnapshot with ConfigChangeID %d recorded index %d, want 100",
				configChangeID, index)
		}
	}
}
