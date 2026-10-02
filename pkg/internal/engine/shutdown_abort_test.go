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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// commitTestRig bundles the channels a commitWorker needs so the shutdown
// tests below share one construction path.
type commitTestRig struct {
	cw        *commitWorker
	ldb       *testLogDB
	cb        *testCallback
	applyC    chan *applyItem
	applyPool *sync.Pool
	stopC     chan struct{}
}

// newCommitTestRig builds a commitWorker whose applyC has the given capacity.
// A zero capacity makes every send to the apply worker block, which is how the
// shutdown-abort path is reached deterministically.
func newCommitTestRig(applyCap int) *commitTestRig {
	ldb := newTestLogDB()
	cb := newTestCallback()
	applyC := make(chan *applyItem, applyCap)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	cw := newCommitWorker(0, ldb, nil, make(chan *commitItem, 16), applyC,
		applyPool, NewWorkSignal(1), cb, stopC, nil)
	return &commitTestRig{
		cw:        cw,
		ldb:       ldb,
		cb:        cb,
		applyC:    applyC,
		applyPool: applyPool,
		stopC:     stopC,
	}
}

// TestCommitWorker_SaveStateRetryCancelledOnShutdown pins the durability
// contract of the retry backoff: when shutdown fires WHILE the worker is
// sleeping between SaveState attempts, the batch was extracted from raft
// (prevState already advanced) but never persisted. Returning silently would
// leave every node in the batch with commitPending=true forever and every
// caller blocked to its context deadline. The worker must instead fail the
// batch closed — mark each node failed, clear commitPending, and complete the
// pending proposals with a SaveStateError.
//
// This also exercises the real timer path in retryWait (noRetryDelay stays
// false): stopC is closed from the retry hook before the wait begins, so the
// select resolves on stopC deterministically without depending on timing.
func TestCommitWorker_SaveStateRetryCancelledOnShutdown(t *testing.T) {
	rig := newCommitTestRig(16)
	diskErr := errors.New("wal fsync failed")
	rig.ldb.setSaveError(diskErr)

	// Close stopC before the first backoff wait so retryWait's select can only
	// resolve on stopC. saveStateBackoff[0] is milliseconds, so the timer case
	// is not ready; this is a deterministic choice, not a race.
	var hookCalls int
	rig.cw.retryHookForTest = func(attempt int) {
		hookCalls++
		if attempt == 0 {
			close(rig.stopC)
		}
	}

	cfg := config.Config{ShardID: 7, ReplicaID: 3}
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)
	node.commitPending.Store(true)

	rig.cw.batch = append(rig.cw.batch[:0], proto.Update{
		ShardID:       7,
		ReplicaID:     3,
		EntriesToSave: []proto.Entry{{Term: 1, Index: 1, Cmd: []byte("c")}},
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 1, Cmd: []byte("c")},
		},
	})
	rig.cw.batchNodes = append(rig.cw.batchNodes[:0], node)

	rig.cw.processBatch()

	// The retry loop must abort at the FIRST backoff, not burn the whole
	// budget after shutdown was signalled.
	if hookCalls != 1 {
		t.Fatalf("retryWait entered %d times, want 1: the retry loop must abort on shutdown", hookCalls)
	}
	if got := rig.ldb.saveCount.Load(); got != 1 {
		t.Fatalf("SaveState called %d times, want 1: retries must stop once stopC closed", got)
	}

	// Fail-closed: the node must not be left mid-commit.
	if node.commitPending.Load() {
		t.Fatal("commitPending still set after a cancelled SaveState retry: the shard is wedged")
	}
	if !node.IsFailed() {
		t.Fatal("node not marked failed after an unpersisted batch: raft state diverged from stable storage")
	}

	// Callers must see the error rather than block to their deadline.
	calls := rig.cb.getProposalFailedCalls()
	if len(calls) != 1 {
		t.Fatalf("OnProposalFailed calls = %d, want 1", len(calls))
	}
	if calls[0].shardID != 7 {
		t.Errorf("OnProposalFailed shard = %d, want 7", calls[0].shardID)
	}
	var saveErr *SaveStateError
	if !errors.As(calls[0].err, &saveErr) {
		t.Fatalf("OnProposalFailed err = %T (%v), want *SaveStateError", calls[0].err, calls[0].err)
	}
	if !errors.Is(calls[0].err, diskErr) {
		t.Errorf("SaveStateError does not wrap the underlying disk error")
	}
	if saveErr.ShardID != 7 || saveErr.ReplicaID != 3 {
		t.Errorf("SaveStateError = %+v, want shard 7 / replica 3", saveErr)
	}

	// The batch must be cleared so the next cycle cannot re-process it.
	for i := range rig.cw.batch {
		if rig.cw.batch[i].ShardID != 0 || len(rig.cw.batch[i].CommittedEntries) != 0 {
			t.Fatalf("batch slot %d not cleared after fail-closed abort: %+v", i, rig.cw.batch[i])
		}
	}
}

// TestCommitWorker_RetryWaitCompletesWithoutShutdown is the counterpart: with
// stopC open the backoff elapses and retryWait reports "not cancelled", so the
// retry budget is actually spent before the batch is failed. Without this the
// cancelled=true assertion above could pass for the wrong reason.
func TestCommitWorker_RetryWaitCompletesWithoutShutdown(t *testing.T) {
	rig := newCommitTestRig(16)
	if cancelled := rig.cw.retryWait(0); cancelled {
		t.Fatal("retryWait reported cancelled with stopC open")
	}
	close(rig.stopC)
	if cancelled := rig.cw.retryWait(0); !cancelled {
		t.Fatal("retryWait did not report cancelled with stopC closed")
	}
}

// TestCommitWorker_ApplyHandoffAbortedOnShutdown covers the last-resort branch
// in processBatch: applyC is full AND stopC is closed (apply worker panicked
// into its restart delay while the engine tears down). The worker must give up
// on the handoff rather than block forever, and while doing so it must release
// the pooled committed-entries slice and the pooled applyItem — otherwise every
// such shutdown permanently drains proto's entry-slice pool and leaks the item.
func TestCommitWorker_ApplyHandoffAbortedOnShutdown(t *testing.T) {
	// Unbuffered applyC with no reader: both the non-blocking and the blocking
	// send are unsatisfiable, so the select must resolve on stopC.
	rig := newCommitTestRig(0)
	close(rig.stopC)

	peer, lr := newTestPeer(t, 1, 1)
	tickPeerToLeader(peer)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)

	update := committedUpdateFromPeer(t, peer)
	node.commitPending.Store(true)
	rig.cw.batch = append(rig.cw.batch[:0], update)
	rig.cw.batchNodes = append(rig.cw.batchNodes[:0], node)

	done := make(chan struct{})
	go func() {
		rig.cw.processBatch()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("processBatch blocked on the apply handoff after shutdown; it must abort via stopC")
	}

	// Nothing may be left queued for an apply worker that will never run.
	select {
	case item := <-rig.applyC:
		t.Fatalf("apply item delivered after shutdown: %+v", item.update)
	default:
	}

	// The item must have been returned to the pool in a reset state.
	pooled, ok := rig.applyPool.Get().(*applyItem)
	if !ok {
		t.Fatal("apply pool did not yield an *applyItem")
	}
	if pooled.node != nil || pooled.update.ShardID != 0 || len(pooled.update.CommittedEntries) != 0 {
		t.Fatalf("apply item was not reset before being pooled: %+v", pooled)
	}

	// The batch must be cleared so a later cycle cannot double-commit it.
	for i := range rig.cw.batch {
		if rig.cw.batch[i].ShardID != 0 || len(rig.cw.batch[i].CommittedEntries) != 0 {
			t.Fatalf("batch slot %d not cleared after the aborted handoff: %+v", i, rig.cw.batch[i])
		}
	}
}

// committedUpdateFromPeer drives a single-node peer far enough to produce an
// update carrying CommittedEntries, which is what processBatch needs to reach
// the apply-handoff branch.
func committedUpdateFromPeer(t *testing.T, peer *raft.Peer) proto.Update {
	t.Helper()
	// Drain and commit the election (no-op) update first.
	for range 4 {
		if !peer.HasUpdate(0) {
			break
		}
		u, err := peer.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("GetUpdate: %v", err)
		}
		if len(u.CommittedEntries) > 0 {
			return u
		}
		if err := peer.Commit(u); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	if err := peer.Handle(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal,
		ShardID: 1,
		From:    1,
		Entries: []proto.Entry{{Cmd: []byte("payload")}},
	}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	for range 8 {
		if !peer.HasUpdate(0) {
			t.Fatal("peer produced no update after propose")
		}
		u, err := peer.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("GetUpdate: %v", err)
		}
		if len(u.CommittedEntries) > 0 {
			return u
		}
		if err := peer.Commit(u); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	t.Fatal("peer never produced an update with committed entries")
	return proto.Update{}
}

// TestSnapshotPool_Recovery_AbortsWhenNodeStopsMidDrain pins the
// state-machine-safety guard in handleRequest. Engine.Stop closes a node's
// stopC BEFORE the apply workers drain, so a recovery dequeued at that moment
// can find an apply still inside SM.Apply. RecoverFromSnapshot must NOT run in
// that window — it would replace state under a concurrent apply and corrupt
// SM/session state. The recovery has to abort cleanly and leave no flag set.
func TestSnapshotPool_Recovery_AbortsWhenNodeStopsMidDrain(t *testing.T) {
	poolStopC := make(chan struct{})
	defer close(poolStopC)
	ldb := newSnapshotTestLogDB()
	ws := NewWorkSignal(1)
	cb := newTestCallback()
	p := newSnapshotPool(1, ldb, nil, cb, ws, poolStopC, nil)

	seedRecoverableSnapshot(t, ldb, 1, 1, 100, 7, 100)

	ss := newSignallingSnapshotter(0)
	node, peer := newRecoveryTestNode(t, 1, 1, ss)
	// The raft log already carries the snapshot, so awaitRecoverableLog is
	// satisfied and the abort below can only come from the drain guard.
	installSnapshotIntoLog(t, node, peer, 100, 7)

	// An apply is in flight and the node is stopping: exactly the Engine.Stop
	// ordering the guard exists for.
	node.applyInFlight.Add(1)
	node.Stop()

	// Mirror requestSnapshot's bookkeeping so the abort's cleanup is observable.
	p.inFlight.Store(uint64(1), struct{}{})
	node.snapshotting.Store(true)

	p.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node, save: false})

	select {
	case <-ss.started:
		t.Fatal("RecoverFromSnapshot ran while an apply was in flight on a stopping node")
	default:
	}
	if got := ss.state.Load(); got != 0 {
		t.Fatalf("state machine was recovered to %d; it must be untouched", got)
	}
	if node.IsRecovering() {
		t.Fatal("recovering gate left raised after the aborted recovery: the shard is wedged")
	}
	if node.snapshotting.Load() {
		t.Fatal("snapshotting flag left set after the aborted recovery")
	}
	if _, stillInFlight := p.inFlight.Load(uint64(1)); stillInFlight {
		t.Fatal("in-flight marker left set: no further snapshot request for this shard would be accepted")
	}
	// The abort is not a completion: no result and no completion callback.
	select {
	case res := <-p.CompletedC():
		t.Fatalf("aborted recovery reported a completion result: %+v", res)
	default:
	}
	if got := cb.getSnapshotCalls(); len(got) != 0 {
		t.Fatalf("OnSnapshotCompleted fired %d times for an aborted recovery, want 0: %+v", len(got), got)
	}
}

// TestEngine_Stop_SurvivesMalformedNodeEntry covers the defensive type-assertion
// branch in Engine.Stop. A malformed allNodes entry must be skipped and logged,
// never panic: a panic inside Stop would abort shutdown with WAL/apply tiers
// still running.
func TestEngine_Stop_SurvivesMalformedNodeEntry(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)
	e.Start()

	nodeCfg := config.Config{ShardID: 3, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	e.LoadNode(node)

	// A value of the wrong type: the range callback must skip it.
	e.allNodes.Store(uint64(999), "not-a-node")

	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Engine.Stop did not complete with a malformed allNodes entry present")
	}

	// The well-formed node must still have been stopped despite the bad entry.
	select {
	case <-node.StopC():
	default:
		t.Fatal("Stop skipped the valid node after encountering a malformed entry")
	}
}

// TestCommitWorker_SaveStateFailure_FailsUncommittedProposals pins the case the
// batch-abandon path must not drop: a proposal that has been accepted
// into a batch but has NOT yet committed.
//
// On a leader this is where every client proposal first appears — in
// EntriesToSave, with CommittedEntries empty until a later round. Reporting only
// CommittedEntries meant the most common failure shape (SaveState fails on the
// very batch carrying a fresh proposal) completed nothing, so the caller blocked
// to its context deadline against a node that had already been marked
// permanently failed and could never complete it.
func TestCommitWorker_SaveStateFailure_FailsUncommittedProposals(t *testing.T) {
	rig := newCommitTestRig(16)
	rig.ldb.setSaveError(errors.New("wal fsync failed"))
	rig.cw.retryHookForTest = func(attempt int) {
		if attempt == 0 {
			close(rig.stopC)
		}
	}

	cfg := config.Config{ShardID: 7, ReplicaID: 3}
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)
	node.commitPending.Store(true)

	// An in-flight proposal: appended, not yet committed.
	rig.cw.batch = append(rig.cw.batch[:0], proto.Update{
		ShardID:       7,
		ReplicaID:     3,
		EntriesToSave: []proto.Entry{{Term: 1, Index: 4, Key: 0xBEEF, Cmd: []byte("p")}},
	})
	rig.cw.batchNodes = append(rig.cw.batchNodes[:0], node)

	rig.cw.processBatch()

	calls := rig.cb.getProposalFailedCalls()
	if len(calls) != 1 {
		t.Fatalf("OnProposalFailed calls = %d, want 1: the uncommitted proposal was "+
			"never completed, so its caller blocks to its deadline on a node that is "+
			"already permanently failed", len(calls))
	}
	var reported bool
	for _, e := range calls[0].entries {
		if e.Key == 0xBEEF {
			reported = true
		}
	}
	if !reported {
		t.Fatalf("the uncommitted proposal (key 0xBEEF) was not among the failed "+
			"entries: %+v", calls[0].entries)
	}
	var saveErr *SaveStateError
	if !errors.As(calls[0].err, &saveErr) {
		t.Fatalf("OnProposalFailed err = %v, want *SaveStateError", calls[0].err)
	}
}

// TestFailBatchProposals_UnionsWithoutDoubleReporting pins the callback contract
// on the overlap. A shard that commits in the same round it appends (a
// single-node shard always does) carries the same entry in both sets. Callback
// is an exported interface whose contract does not promise idempotence, so the
// entry must be reported exactly once, in exactly one call.
func TestFailBatchProposals_UnionsWithoutDoubleReporting(t *testing.T) {
	cb := newTestCallback()
	shared := proto.Entry{Term: 1, Index: 4, Key: 0xAAAA}
	committedOnly := proto.Entry{Term: 1, Index: 3, Key: 0xBBBB}

	failBatchProposals(cb, proto.Update{
		ShardID:          7,
		EntriesToSave:    []proto.Entry{shared},
		CommittedEntries: []proto.Entry{committedOnly, shared},
	}, errors.New("boom"))

	calls := cb.getProposalFailedCalls()
	if len(calls) != 1 {
		t.Fatalf("OnProposalFailed calls = %d, want exactly 1", len(calls))
	}
	counts := map[uint64]int{}
	for _, e := range calls[0].entries {
		counts[e.Key]++
	}
	if counts[0xAAAA] != 1 {
		t.Errorf("entry present in BOTH sets reported %d times, want 1", counts[0xAAAA])
	}
	if counts[0xBBBB] != 1 {
		t.Errorf("committed-only entry reported %d times, want 1", counts[0xBBBB])
	}
}
