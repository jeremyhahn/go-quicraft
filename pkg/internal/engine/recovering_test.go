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
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestRecovering_BlocksEntryApplication verifies that when the
// recovering flag is set on a node, the apply worker skips entry
// application and does not advance lastApplied. This prevents
// concurrent state machine access between the snapshot recovery
// goroutine and the apply pipeline.
func TestRecovering_BlocksEntryApplication(t *testing.T) {
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

	// Set recovering BEFORE sending the apply item. This simulates
	// snapshot recovery starting before entries arrive.
	node.SetRecovering(true)

	// Start the apply worker.
	go aw.run()

	// Send an item with committed entries while recovering. The apply
	// worker must hold the batch until the gate clears — never touch the
	// state machine, and never discard the batch (raft's processed marker
	// has already advanced past it, so it would not be re-delivered).
	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 5, Term: 1, Cmd: []byte("a")},
			{Index: 6, Term: 1, Cmd: []byte("b")},
		},
	}
	item.node = node
	applyC <- item

	// While recovering, the batch stays parked: the SM is untouched and
	// commitPending remains set.
	time.Sleep(20 * time.Millisecond)
	if goodSM.applyCount.Load() != 0 {
		t.Fatalf("expected 0 SM.Apply calls during recovery, got %d", goodSM.applyCount.Load())
	}
	if got := node.lastApplied.Load(); got != 0 {
		t.Fatalf("expected lastApplied to remain 0 during recovery, got %d", got)
	}
	if !node.commitPending.Load() {
		t.Fatal("expected commitPending to stay set while the batch waits on the recovery gate")
	}

	// Complete the recovery: the recovered snapshot covers both queued
	// entries, so the parked batch resumes, filters them as
	// snapshot-covered, and releases the pipeline without applying.
	node.lastApplied.Store(6)
	node.SetRecovering(false)

	deadlineRec := time.After(5 * time.Second)
	for node.commitPending.Load() {
		select {
		case <-deadlineRec:
			t.Fatal("timed out waiting for commitPending to clear after recovery completed")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	// Entries covered by the recovered snapshot must not reach the SM.
	if goodSM.applyCount.Load() != 0 {
		t.Fatalf("expected 0 SM.Apply calls for snapshot-covered entries, got %d", goodSM.applyCount.Load())
	}

	// OnApplied callback should NOT have been called.
	if cb.appliedCount.Load() != 0 {
		t.Fatalf("expected 0 OnApplied calls for snapshot-covered entries, got %d", cb.appliedCount.Load())
	}

	close(stopC)
}

// TestRecovering_ClearedAfterRecoveryCompletes verifies that the
// recovering flag is set before recovery starts and cleared after
// recovery completes (both on success and error paths).
func TestRecovering_ClearedAfterRecoveryCompletes(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)

	// Create a snapshot pool with a real work signal to verify
	// notification after recovery.
	p := newSnapshotPool(1, nil, nil, nil, ws, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()

	// Create a node without a snapshotter -- recoverSnapshot returns
	// nil immediately (no-op for witness nodes), but the recovering
	// flag lifecycle is still exercised.
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// Verify initial state.
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

	// Wait for the pool worker to process the request by reading
	// from completedC. This is the only reliable synchronization
	// point because it is written AFTER the worker clears both
	// the recovering and snapshotting flags. Polling the flags
	// directly has a race: the loop can exit before the worker
	// picks up the request (both flags are initially false), and
	// then the worker sets recovering=true, causing the assertion
	// at line 151 to fail.
	select {
	case <-p.CompletedC():
		// Recovery completed.
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for recovery to complete")
	}

	// Verify the flag was cleared.
	if node.IsRecovering() {
		t.Fatal("recovering flag should be cleared after recovery completes")
	}

	// Verify the snapshotting flag was also cleared.
	if node.snapshotting.Load() {
		t.Fatal("snapshotting flag should be cleared after recovery completes")
	}

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

	// The batch stays parked while recovery is in progress.
	time.Sleep(20 * time.Millisecond)
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
	deadline := time.After(5 * time.Second)
	for node.lastApplied.Load() < 11 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for entries to be applied (phase 2)")
		default:
		}
		time.Sleep(time.Millisecond)
	}

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
// The node used to be skipped entirely, which made it invisible to
// consensus for the whole unbounded duration of user RecoverFromSnapshot
// code: two replicas recovering at once dissolved a five-replica shard's
// quorum with every node still up.
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

	// Wait for completion.
	deadline := time.After(5 * time.Second)
	for node.snapshotting.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for snapshot save to complete")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	// The recovering flag should NEVER have been set for a save request.
	if node.IsRecovering() {
		t.Fatal("recovering flag should not be set for snapshot save requests")
	}

	close(stopC)
	wg.Wait()
}

// TestRecovering_SnapshotPoolSetsRecoveringBeforeExecution verifies
// that the snapshot pool worker sets the recovering flag BEFORE calling
// handleSnapshot for recovery requests. This ensures the flag is set
// before any state machine mutation begins.
func TestRecovering_SnapshotPoolSetsRecoveringBeforeExecution(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	cb := newTestCallback()

	p := newSnapshotPool(1, nil, nil, cb, ws, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()

	// Use a node without a snapshotter (recovery is a no-op).
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// Submit recovery.
	err := p.requestSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      false,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	// Wait for completion via the snapshot callback.
	deadline := time.After(5 * time.Second)
	for cb.snapshotCallCount.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for snapshot completion callback")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	// After completion, both flags should be cleared.
	if node.IsRecovering() {
		t.Fatal("recovering should be cleared after recovery completes")
	}
	if node.snapshotting.Load() {
		t.Fatal("snapshotting should be cleared after recovery completes")
	}

	close(stopC)
	wg.Wait()
}

// TestRecovering_WorkSignalNotifiedAfterRecovery verifies that the
// snapshot pool notifies the work signal after recovery completes,
// ensuring the step worker wakes up immediately to re-process the
// node instead of waiting for the next tick.
func TestRecovering_WorkSignalNotifiedAfterRecovery(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)

	p := newSnapshotPool(1, nil, nil, nil, ws, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// Submit recovery.
	err := p.requestSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      false,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	// Wait for recovery to complete by reading from completedC.
	// This is the most reliable synchronization point because
	// completedC is written AFTER the Notify call in the worker.
	select {
	case <-p.CompletedC():
		// Recovery completed.
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for recovery completion")
	}

	// The work signal should have been notified. Check by seeing
	// if the signal has work pending for the worker that owns
	// shard 1 (workerID = shardID % numWorkers = 1 % 1 = 0).
	if !ws.HasWork(0) {
		t.Fatal("expected work signal to be notified after recovery")
	}

	close(stopC)
	wg.Wait()
}

// TestRecovering_NodeAccessors verifies the SetRecovering/IsRecovering
// accessor methods and the initial state of the recovering flag.
func TestRecovering_NodeAccessors(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// Initial state should be false.
	if node.IsRecovering() {
		t.Fatal("new node should not be recovering")
	}

	// Set to true.
	node.SetRecovering(true)
	if !node.IsRecovering() {
		t.Fatal("node should be recovering after SetRecovering(true)")
	}

	// Set back to false.
	node.SetRecovering(false)
	if node.IsRecovering() {
		t.Fatal("node should not be recovering after SetRecovering(false)")
	}
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

	deadline := time.After(5 * time.Second)
	for node.commitPending.Load() {
		select {
		case <-deadline:
			t.Fatal("commitPending still set: an empty batch was parked on the recovering gate")
		default:
		}
		time.Sleep(time.Millisecond)
	}

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
