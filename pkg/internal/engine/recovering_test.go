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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// applyBatchParked reports whether a shard apply goroutine is asleep in the
// recovery gate's wait loop, the only time.Sleep in runShardApply. The
// dispatcher spawns that goroutine, so all goroutines are scanned.
func applyBatchParked() bool {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	for _, g := range strings.Split(string(buf), "\n\n") {
		// Line 1 is the innermost frame, line 3 its caller.
		lines := strings.Split(g, "\n")
		if len(lines) > 3 && strings.HasPrefix(lines[1], "time.Sleep(") &&
			strings.Contains(lines[3], "runShardApply") {
			return true
		}
	}
	return false
}

// TestRecovering_ClearedAfterRecoveryCompletes verifies that a recovery
// request through the snapshot pool clears the recovering and snapshotting
// flags when it completes, wakes the owning step worker through the work
// signal, and fires the completion callback.
func TestRecovering_ClearedAfterRecoveryCompletes(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	cb := newTestCallback()

	p := newSnapshotPool(1, nil, nil, cb, ws, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()

	// Create a node without a snapshotter -- recoverSnapshot returns
	// nil immediately (no-op for witness nodes), but the recovering
	// flag lifecycle is still exercised.
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if node.IsRecovering() {
		t.Fatal("node should not be recovering initially")
	}

	// Submit a recovery request (save=false).
	err := p.requestSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      false,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	// completedC is written AFTER the worker clears the recovering and
	// snapshotting flags and notifies the work signal. Polling the flags
	// directly would race: both are false before the worker picks the
	// request up.
	awaitSnapshotResult(t, p, "recovery")

	if node.IsRecovering() {
		t.Fatal("recovering flag should be cleared after recovery completes")
	}
	if node.snapshotting.Load() {
		t.Fatal("snapshotting flag should be cleared after recovery completes")
	}

	// The step worker that owns shard 1 (1 % 1 = worker 0) is woken at
	// once to re-process the node instead of waiting for the next tick.
	if !ws.HasWork(0) {
		t.Fatal("expected work signal to be notified after recovery")
	}

	// The completion callback fires after the result is published.
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for snapshot completion callback", func() bool {
		return cb.snapshotCallCount.Load() != 0
	})

	close(stopC)
	wg.Wait()
}

// TestRecovering_EntriesAppliedAfterRecovery verifies that a batch
// queued during recovery is held by the apply worker and applied once
// the recovering flag clears, with entries at or below the recovered
// snapshot index filtered out and entries above it applied.
func TestRecovering_EntriesAppliedAfterRecovery(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	ws := NewWorkSignal(1)
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, ws, stopC, nil)

	goodSM := newTestSM()
	rsmSM := newTestApplier(goodSM)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, rsmSM, nil, cfg, nil, "", nil)

	// Start the apply worker.
	go aw.run()

	// Phase 1: recovering=true; a batch delivered now must be held, not
	// applied and not discarded.
	node.SetRecovering(true)
	node.commitPending.Store(true)
	item1 := applyPool.Get().(*applyItem)
	item1.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 10, Term: 1, Cmd: []byte("covered-by-snapshot")},
			{Index: 11, Term: 1, Cmd: []byte("beyond-snapshot")},
		},
	}
	item1.node = node
	applyC <- item1

	// The batch stays parked while recovery is in progress: wait until a
	// shard apply goroutine sleeps in the recovery gate's wait loop.
	waitFor(t, 5*time.Second, time.Millisecond, "batch never parked on the recovery gate", applyBatchParked)
	if !node.commitPending.Load() {
		t.Fatal("expected commitPending to stay set while the batch waits on the recovery gate")
	}
	if node.lastApplied.Load() != 0 {
		t.Fatalf("expected lastApplied=0 during recovery, got %d", node.lastApplied.Load())
	}
	if goodSM.applyCount.Load() != 0 {
		t.Fatalf("expected 0 SM.Apply calls during recovery, got %d", goodSM.applyCount.Load())
	}

	// Phase 2: recovery completes at snapshot index 10 (recoverSnapshot
	// stores the snapshot index into node.lastApplied before clearing the
	// flag). The parked batch resumes: entry 10 is covered by the
	// snapshot and filtered; entry 11 applies.
	node.lastApplied.Store(10)
	node.SetRecovering(false)

	// Wait for entries to be applied.
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for entries to be applied (phase 2)", func() bool {
		return node.lastApplied.Load() >= 11
	})

	// Verify entries were applied.
	if got := node.lastApplied.Load(); got != 11 {
		t.Fatalf("expected lastApplied=11 after recovery, got %d", got)
	}
	if goodSM.applyCount.Load() != 1 {
		t.Fatalf("expected 1 SM.Apply call after recovery, got %d", goodSM.applyCount.Load())
	}

	// Verify callback was called for the applied entries.
	if cb.appliedCount.Load() != 1 {
		t.Fatalf("expected 1 OnApplied call, got %d", cb.appliedCount.Load())
	}

	close(stopC)
}

// TestRecovering_StepWorkerSuppressesOnlyCommittedEntries pins the
// recovering contract: the step worker keeps the node in the Raft protocol
// (it still drains the inbox, produces updates, and persists hard state so
// ordered responses can be sent) and suppresses EXACTLY ONE thing — the
// extraction of committed entries, the only part of an update that reaches
// the state machine the snapshot pool is replacing.
//
// Skipping the node entirely would make it invisible to consensus for the
// whole unbounded duration of user RecoverFromSnapshot code: two replicas
// recovering at once would dissolve a five-replica shard's quorum with every
// node still up.
func TestRecovering_StepWorkerSuppressesOnlyCommittedEntries(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	cb := newTestCallback()
	stopC := make(chan struct{})
	defer close(stopC)

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	// Single-node cluster: a proposal becomes committable after one full
	// step + commit cycle, giving the second cycle real committed entries
	// to extract.
	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)
	tickPeerToLeader(peer)

	// Cycle 1 (not recovering): propose an entry and persist it.
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("before-recovery")}})
	ws.Notify(1)
	sw.processReady()

	var first *commitItem
	select {
	case first = <-commitC:
	default:
		t.Fatal("expected a commit item for the proposed entry")
	}
	if len(first.update.EntriesToSave) == 0 {
		t.Fatal("expected entries to save in the first update")
	}
	// Stand in for the commit worker: persisting the entry makes it
	// committable, so the peer now holds committed entries to apply.
	if err := peer.Commit(first.update); err != nil {
		t.Fatalf("peer.Commit failed: %v", err)
	}
	node.commitPending.Store(false)

	// Cycle 2 (recovering): the peer HAS committed entries, but they must
	// stay in the log while the state machine is being replaced.
	node.SetRecovering(true)
	if !peer.HasUpdate(node.lastApplied.Load()) {
		t.Fatal("expected a pending update before the recovering cycle")
	}
	ws.Notify(1)
	sw.processReady()

	select {
	case item := <-commitC:
		if len(item.update.CommittedEntries) != 0 {
			t.Fatalf("recovering node extracted %d committed entries; they must stay in the log",
				len(item.update.CommittedEntries))
		}
	default:
		// Nothing left to persist in this cycle is legitimate; the
		// liveness half is asserted next.
	}
	node.commitPending.Store(false)

	// Protocol liveness: a message delivered while recovering is stepped,
	// not dropped. A higher-term heartbeat must move the peer's term.
	priorTerm := peer.Term()
	if !node.Deliver(proto.Message{
		Type:    proto.Heartbeat,
		ShardID: 1,
		From:    2,
		To:      1,
		Term:    priorTerm + 5,
	}) {
		t.Fatal("failed to deliver heartbeat to node inbox")
	}
	ws.Notify(1)
	sw.processReady()
	if got := peer.Term(); got <= priorTerm {
		t.Fatalf("recovering node did not step the delivered heartbeat: term %d -> %d", priorTerm, got)
	}
	node.commitPending.Store(false)

	// Cycle 3: recovery finished. The committed entries held back above are
	// re-offered now that the state machine is available again.
	node.SetRecovering(false)
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if len(item.update.CommittedEntries) == 0 {
			t.Fatal("expected the held-back committed entries after recovery cleared")
		}
	default:
		t.Fatal("expected a commit item after recovery cleared")
	}
}

// TestRecovering_FlagOnlySetsForRecoveryNotSave verifies that the
// recovering flag is ONLY set for recovery requests (save=false),
// not for save requests (save=true). Snapshot save does not conflict
// with the apply pipeline because it reads from the SM under
// commitPending protection, whereas recovery replaces SM state.
func TestRecovering_FlagOnlySetsForRecoveryNotSave(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)

	p := newSnapshotPool(1, nil, nil, nil, ws, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()

	// Create a node without a snapshotter. For save requests, this
	// will return early with an error (no snapshotter), but the
	// recovering flag behavior is what we are testing.
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// Submit a SAVE request (not recovery).
	err := p.requestSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	// Wait for the worker to finish the request. Polling snapshotting would
	// pass before the worker picked the request up: it starts false.
	awaitSnapshotResult(t, p, "save")

	// The recovering flag should NEVER have been set for a save request.
	if node.IsRecovering() {
		t.Fatal("recovering flag should not be set for snapshot save requests")
	}

	close(stopC)
	wg.Wait()
}

// TestRecovering_ApplyWorkerDoesNotParkEmptyBatch verifies that a batch
// with NO committed entries is released immediately instead of waiting on
// the recovering gate. An empty batch never touches the state machine, so
// parking it buys nothing and costs everything: commitPending stays set for
// the entire unbounded duration of user RecoverFromSnapshot code, which in
// memory mode (where every update reaches the apply worker, including a
// bare hard-state change) silences the shard's ordered responses — vote
// grants included — for exactly as long as the recovery runs.
func TestRecovering_ApplyWorkerDoesNotParkEmptyBatch(t *testing.T) {
	applyC := make(chan *applyItem, 8)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	ws := NewWorkSignal(1)
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, ws, stopC, nil)

	goodSM := newTestSM()
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, newTestApplier(goodSM), nil, cfg, nil, "", nil)
	node.SetRecovering(true)

	go aw.run()

	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{ShardID: 1, ReplicaID: 1, State: proto.State{Term: 7}}
	item.node = node
	applyC <- item

	waitFor(t, 5*time.Second, time.Millisecond, "commitPending still set: an empty batch was parked on the recovering gate", func() bool {
		return !node.commitPending.Load()
	})

	if goodSM.applyCount.Load() != 0 {
		t.Fatalf("expected 0 SM.Apply calls for an empty batch, got %d", goodSM.applyCount.Load())
	}
	if node.lastApplied.Load() != 0 {
		t.Fatalf("expected lastApplied to stay 0, got %d", node.lastApplied.Load())
	}
}

// TestRecovering_CommitWorkerSkipsPiggyback verifies that the commit
// worker does not extract piggyback committed entries for a recovering
// node. Extracting them advances raft's processed marker and hands the
// batch to an apply worker that must park it on the recovering gate,
// holding commitPending — and with it every ordered response, vote grants
// included — for the whole recovery. The entries stay in the log and are
// re-offered once the gate clears.
func TestRecovering_CommitWorkerSkipsPiggyback(t *testing.T) {
	ldb := newTestLogDB()
	sender := newTestSender()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	ws := NewWorkSignal(64)
	cb := newTestCallback()
	stopC := make(chan struct{})
	defer close(stopC)

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, ws, cb, stopC, nil)
	cw.noRetryDelay = true

	peer, lr, update, _ := setupPiggybackPeer(t, 1, 1)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	node.commitPending.Store(true)
	node.SetRecovering(true)

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	cw.processBatch()

	select {
	case ai := <-applyC:
		t.Fatalf("recovering node forwarded %d piggyback entries to the apply worker",
			len(ai.update.CommittedEntries))
	default:
	}
	if node.commitPending.Load() {
		t.Fatal("commitPending must be cleared so the recovering node keeps producing updates")
	}
	// The entries were NOT consumed: they are still offered once the gate
	// clears.
	node.SetRecovering(false)
	pb, err := peer.GetPiggybackUpdate(node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetPiggybackUpdate failed: %v", err)
	}
	if len(pb.CommittedEntries) == 0 {
		t.Fatal("committed entries were consumed during recovery; they must stay in the log")
	}
}
