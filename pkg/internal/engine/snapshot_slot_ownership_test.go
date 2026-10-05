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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// newSlotTestNode builds a node for shard 1 replica 1 over snapshotter ss.
func newSlotTestNode(t *testing.T, ss Snapshotter) *Node {
	t.Helper()
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	return NewNode(nil, nil, nil, cfg, ss, t.TempDir(), nil)
}

// TestRequestSnapshot_StoppedIncarnationSlotIsTakenOver: the per-shard
// in-flight slot is owned by node incarnation. A request a stopped incarnation
// abandoned must not refuse the next incarnation of the same shard, while a
// live holder — the same node, or another node not stopped — still refuses.
func TestRequestSnapshot_StoppedIncarnationSlotIsTakenOver(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	pool := newSnapshotPool(1, nil, nil, newTestCallback(), nil, stopC, nil)

	old := newSlotTestNode(t, newSnapshotTestSnapshotter(0))
	successor := newSlotTestNode(t, newSnapshotTestSnapshotter(0))
	oldReq := snapshotRequest{shardID: 1, replicaID: 1, node: old, save: true}
	newReq := snapshotRequest{shardID: 1, replicaID: 1, node: successor, save: true}

	if !pool.claimInFlight(oldReq) {
		t.Fatal("the first claim of an empty slot was refused")
	}
	if pool.claimInFlight(oldReq) {
		t.Fatal("a second claim by the holding node was admitted")
	}
	if pool.claimInFlight(newReq) {
		t.Fatal("a claim was admitted over a LIVE incarnation's in-flight request")
	}

	old.Stop()
	if !pool.claimInFlight(newReq) {
		t.Fatal("the next incarnation was refused because of a request its stopped " +
			"predecessor abandoned")
	}
	// The abandoned request finishing late releases only its own claim.
	pool.releaseInFlight(oldReq)
	if owner, held := pool.inFlight.Load(uint64(1)); !held || owner != successor {
		t.Fatalf("after the old incarnation's release the slot is (%v, held=%v), "+
			"want the successor's claim", owner, held)
	}
	pool.releaseInFlight(newReq)
	if _, held := pool.inFlight.Load(uint64(1)); held {
		t.Fatal("the successor's own release left the slot held")
	}
}

// TestHandleRequest_AbandonedRequestDoesNotClearSuccessorSlot: a request whose
// node stopped is abandoned by its worker, which releases the shard's slot as
// it exits. By then the next incarnation may hold that slot, and clearing it
// would admit a second concurrent snapshot of the shard.
func TestHandleRequest_AbandonedRequestDoesNotClearSuccessorSlot(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	cb := newTestCallback()
	pool := newSnapshotPool(1, nil, nil, cb, nil, stopC, nil)

	old := newSlotTestNode(t, newSnapshotTestSnapshotter(0))
	successor := newSlotTestNode(t, newSnapshotTestSnapshotter(0))
	old.snapshotting.Store(true)
	old.Stop()
	successor.snapshotting.Store(true)
	pool.inFlight.Store(uint64(1), successor)

	pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: old, save: true, requestKey: 7})

	if owner, held := pool.inFlight.Load(uint64(1)); !held || owner != successor {
		t.Fatalf("the abandoned request left the slot (%v, held=%v), want the successor's claim",
			owner, held)
	}
	if !successor.snapshotting.Load() {
		t.Fatal("the abandoned request cleared the successor's snapshotting flag")
	}
	if old.snapshotting.Load() {
		t.Fatal("the abandoned request did not release its own node's snapshotting flag")
	}
	if n := cb.snapshotCallCount.Load(); n != 0 {
		t.Fatalf("an abandoned request reported OnSnapshotCompleted %d times", n)
	}
	if len(cb.requestCalls) != 0 {
		t.Fatal("an abandoned request resolved its requester; teardown owns that")
	}
}

// TestHandleRequest_SavePanicResolvesRequesterAsFailedSave: a SaveSnapshot that
// panics is a failed save, not a failed shard. Its requester is resolved after
// the slots are released, with a save SnapshotError wrapping a
// SnapshotSavePanicError that does not match ErrShardFailed, and the save is
// still reported rejected.
func TestHandleRequest_SavePanicResolvesRequesterAsFailedSave(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	stopC := make(chan struct{})
	defer close(stopC)
	node := newSlotTestNode(t, &panickingSnapshotter{})
	pool := newSnapshotPool(1, nil, nil, nil, nil, stopC, nil)
	probe := &requestProbeCallback{pool: pool, node: node, shardID: 1}
	pool.setCallback(probe)

	node.snapshotting.Store(true)
	pool.inFlight.Store(uint64(1), node)

	pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node, save: true, requestKey: 0xfeed})

	if len(probe.requestCalls) != 1 {
		t.Fatalf("OnSnapshotRequestCompleted called %d times for a panicked save, want 1",
			len(probe.requestCalls))
	}
	got := probe.requestCalls[0]
	if got.requestKey != 0xfeed || got.index != 0 {
		t.Fatalf("resolved (key=%#x, index=%d), want (0xfeed, 0)", got.requestKey, got.index)
	}
	var snapErr *SnapshotError
	if !errors.As(got.err, &snapErr) || !snapErr.Save {
		t.Fatalf("resolved with %v (%T), want a save *SnapshotError", got.err, got.err)
	}
	var panicErr *SnapshotSavePanicError
	if !errors.As(got.err, &panicErr) || panicErr.Value != "snapshotter save panic" {
		t.Fatalf("resolved with %v, want it to wrap the *SnapshotSavePanicError", got.err)
	}
	if errors.Is(got.err, ErrShardFailed) {
		t.Fatal("a panicked save matches ErrShardFailed; the shard was not failed")
	}
	if probe.inFlightHeld || probe.nodeHeld {
		t.Fatalf("the requester was resolved with slots held (inFlight=%v, node=%v)",
			probe.inFlightHeld, probe.nodeHeld)
	}
	if n := probe.snapshotRejected.Load(); n != 1 {
		t.Fatalf("OnSnapshotRejected fired %d times, want 1", n)
	}
	if n := probe.snapshotCallCount.Load(); n != 0 {
		t.Fatalf("OnSnapshotCompleted fired %d times for a save that never completed", n)
	}
	if node.IsFailed() {
		t.Fatal("a panicked save failed the shard")
	}
}
