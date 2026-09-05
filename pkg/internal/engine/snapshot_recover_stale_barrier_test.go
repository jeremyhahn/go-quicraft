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
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestSnapshotPool_StaleRecovery_BarrierArtifact_DeclinedBeforeDecrypt settles
// the question #126 posed about the barrier snapshot tests: is the stale
// refusal on the ENCRYPTED recovery path a different error, or wrapped such
// that the engine's decline branch is bypassed?
//
// It is neither. recoveryBlockedByLog runs BEFORE the snapshotter is touched
// and before the decrypt-to-scratch step, so an encrypted artifact and a
// plaintext one produce the SAME *SnapshotRecoveryStaleError from the same
// line, unwrapped. The barrier changes nothing about the refusal, which is why
// #113's decline covers it — and why the surviving report in #126 had to be
// somewhere other than the engine (it is: hostEngineCallback.OnSnapshotCompleted,
// see TestOnSnapshotCompleted_DeclinedStaleRecoveryIsNotAShardFailure).
//
// The positive control in the second half is what makes the first half mean
// anything: the very same artifact, offered to a state machine that is
// genuinely behind it, decrypts and restores. So the refusal is about the
// index, not about a fixture the barrier path could not read.
func TestSnapshotPool_StaleRecovery_BarrierArtifact_DeclinedBeforeDecrypt(t *testing.T) {
	const (
		shardID       = uint64(10)
		replicaID     = uint64(1)
		barrierEpoch  = uint64(5)
		snapshotIndex = uint64(100)
		snapshotTerm  = uint64(4)
	)

	// Deferred calls run LIFO, so wg.Wait is registered FIRST in order to run
	// LAST -- after close(stopC). The pool workers only return once stopC is
	// closed, so the other order deadlocks the test.
	var wg sync.WaitGroup
	defer wg.Wait()
	stopC := make(chan struct{})
	defer close(stopC)

	tmpDir := t.TempDir()
	ldb := newSnapshotTestLogDB()
	barrier := newTestBarrier(barrierEpoch)
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)
	p.start(&wg)

	// ---------------------------------------------------------------------
	// A real barrier-encrypted artifact, produced by the production save path
	// rather than hand-written, so the recovery under test reads exactly what
	// a running replica would have on disk.
	// ---------------------------------------------------------------------
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
	saveSM := newSnapshotTestSnapshotter(snapshotIndex)
	saveNode := NewNode(nil, nil, nil, cfg, saveSM, tmpDir, nil)
	if err := p.requestSnapshot(snapshotRequest{
		shardID: shardID, replicaID: replicaID, node: saveNode, save: true,
	}); err != nil {
		t.Fatalf("save request failed: %v", err)
	}
	res := awaitSnapshotResult(t, p, "save")
	if res.err != nil {
		t.Fatalf("save failed: %v", res.err)
	}
	if res.index != snapshotIndex {
		t.Fatalf("saved index = %d, want %d", res.index, snapshotIndex)
	}

	// The artifact is genuinely encrypted at rest: the LogDB record carries the
	// barrier epoch, which is what makes recovery take the decrypt path at all.
	// A test that asserted the refusal against a plaintext artifact would be
	// testing the plaintext path under a barrier-shaped name.
	rec, err := ldb.GetSnapshot(shardID, replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if rec.Epoch != barrierEpoch {
		t.Fatalf("logdb snapshot epoch = %d, want %d: the artifact is not "+
			"encrypted at rest, so this is not the barrier recovery path", rec.Epoch, barrierEpoch)
	}
	dataPath := filepath.Join(snapshotSaveDir(tmpDir, shardID, replicaID, snapshotIndex), snapshotDataFile)
	ciphertext, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("read snapshot artifact: %v", err)
	}
	var plaintext [8]byte
	binary.LittleEndian.PutUint64(plaintext[:], snapshotIndex)
	if len(ciphertext) == len(plaintext) {
		t.Fatalf("snapshot artifact is %d bytes, the same as the plaintext: "+
			"the barrier did not encrypt it", len(ciphertext))
	}

	// ---------------------------------------------------------------------
	// The stale offer: a replica already past the snapshot is asked to recover
	// from it. This is the shape a leader's InstallSnapshot takes after a
	// rejection walk-down drives remote.next below the follower's real
	// progress -- raft rejects the snapshot itself, but the host queues the
	// state-machine recovery on DELIVERY, so the pool still sees the request.
	// ---------------------------------------------------------------------
	const appliedAhead = snapshotIndex + 50
	recoverSM := newSnapshotTestSnapshotter(0)
	recoverSM.state.Store(appliedAhead)
	node, peer := newRecoveryTestNode(t, shardID, replicaID, recoverSM)
	node.SeedLastApplied(appliedAhead, snapshotTerm+1)
	installSnapshotIntoLog(t, node, peer, appliedAhead, snapshotTerm+1)

	if err := p.requestSnapshot(snapshotRequest{
		shardID: shardID, replicaID: replicaID, node: node,
	}); err != nil {
		t.Fatalf("recovery request failed: %v", err)
	}
	res = awaitSnapshotResult(t, p, "recovery")
	var stale *SnapshotRecoveryStaleError
	if !errors.As(res.err, &stale) {
		t.Fatalf("barrier recovery error = %T (%v), want *SnapshotRecoveryStaleError.\n"+
			"If this is a wrapped or differently-typed error, the engine's decline "+
			"branch is bypassed and every stale InstallSnapshot on the encrypted "+
			"path kills the shard (#126 candidate B)", res.err, res.err)
	}
	if stale.SnapshotIndex != snapshotIndex || stale.LastApplied != appliedAhead {
		t.Errorf("stale error = %+v, want SnapshotIndex %d / LastApplied %d",
			stale, snapshotIndex, appliedAhead)
	}

	// The engine declines rather than failing the shard: the refusal was
	// produced before the recovering gate was raised and before
	// RecoverFromSnapshot ran, and the state machine already holds everything
	// the snapshot represents.
	if node.IsFailed() {
		t.Error("a stale recovery of a barrier-encrypted snapshot failed the shard; " +
			"the state machine was never touched and is already past the snapshot")
	}
	if node.IsRecovering() {
		t.Error("the recovering gate is still raised after a declined barrier recovery")
	}
	if got := recoverSM.state.Load(); got != appliedAhead {
		t.Errorf("state machine state = %d, want %d (untouched)", got, appliedAhead)
	}
	if got := node.LastApplied(); got != appliedAhead {
		t.Errorf("node.LastApplied = %d, want %d (untouched)", got, appliedAhead)
	}

	// Declining must not have decrypted anything. The canonical artifact is
	// still ciphertext, and no plaintext scratch sibling was left behind --
	// the refusal happens before the decrypt step, not after it.
	after, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("re-read snapshot artifact: %v", err)
	}
	if string(after) != string(ciphertext) {
		t.Error("the canonical snapshot artifact changed during a declined recovery")
	}
	assertNoPlaintextScratch(t, filepath.Dir(dataPath), plaintext[:])

	// ---------------------------------------------------------------------
	// Positive control: the SAME artifact, offered to a state machine that is
	// genuinely behind it, decrypts and restores. Without this the assertions
	// above would also pass against a barrier that could not read its own
	// output.
	// ---------------------------------------------------------------------
	behindSM := newSnapshotTestSnapshotter(0)
	behindNode, behindPeer := newRecoveryTestNode(t, shardID, replicaID, behindSM)
	installSnapshotIntoLog(t, behindNode, behindPeer, snapshotIndex, snapshotTerm)

	if err := p.requestSnapshot(snapshotRequest{
		shardID: shardID, replicaID: replicaID, node: behindNode,
	}); err != nil {
		t.Fatalf("control recovery request failed: %v", err)
	}
	res = awaitSnapshotResult(t, p, "control recovery")
	if res.err != nil {
		t.Fatalf("control recovery failed: %v -- the encrypted artifact is not "+
			"recoverable at all, so the refusal above proved nothing", res.err)
	}
	if got := behindSM.state.Load(); got != snapshotIndex {
		t.Errorf("control state machine state = %d, want %d: the barrier artifact "+
			"did not round-trip", got, snapshotIndex)
	}
}

// assertNoPlaintextScratch fails if any file in dir other than the canonical
// data file holds the snapshot's plaintext. The recovery path decrypts to a
// throwaway SIBLING file, so a decrypt that ran despite the refusal leaves its
// evidence next to the artifact.
func assertNoPlaintextScratch(t *testing.T, dir string, plaintext []byte) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read snapshot dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == snapshotDataFile {
			continue
		}
		body, readErr := os.ReadFile(filepath.Join(dir, e.Name()))
		if readErr != nil {
			continue
		}
		if len(body) == len(plaintext) && string(body) == string(plaintext) {
			t.Errorf("a declined recovery left decrypted plaintext at %s", e.Name())
		}
	}
}
