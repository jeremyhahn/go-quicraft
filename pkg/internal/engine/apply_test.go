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
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// panicApplier is an Applier that panics during Apply.
type panicApplier struct {
	panicValue interface{}
}

func (a *panicApplier) Apply(_ []proto.Entry, _ []sm.Result) error {
	panic(a.panicValue)
}

// TestApplyWorker_RecoverFromSMPanic verifies that when a user state machine
// panics during Apply, the apply worker recovers without crashing the host.
// The shard is marked as failed and the error is reported through handleError.
func TestApplyWorker_RecoverFromSMPanic(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	ws := NewWorkSignal(1)
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, ws, stopC, nil)

	// Create a node with a panicking state machine.
	panicker := &panicApplier{panicValue: "boom"}
	var errorReported atomic.Value
	listener := &config.EventListener{
		OnShardFailed: func(info config.ShardFailedInfo) {
			errorReported.Store(info.Err)
		},
	}
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, panicker, nil, cfg, nil, "", listener)

	// Start the apply worker.
	go aw.run()

	// Send an item with committed entries that will cause a panic.
	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("trigger-panic")},
		},
	}
	item.node = node
	applyC <- item

	// Wait for the apply worker to process the item. Poll for
	// errorReported (set by handleError) rather than node.failed,
	// because failed is stored BEFORE handleError runs. Polling
	// on the later signal avoids the race where the test reads
	// errorReported before the apply goroutine has called handleError.
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for panic recovery", func() bool {
		return errorReported.Load() != nil
	})

	// Verify the shard is marked as failed.
	if !node.failed.Load() {
		t.Fatal("expected node to be marked as failed after panic")
	}

	// Verify the error was reported through the listener.
	reportedErr := errorReported.Load()
	if reportedErr == nil {
		t.Fatal("expected error to be reported through OnShardFailed listener")
	}
	var smPanic *SMPanicError
	if !errors.As(reportedErr.(error), &smPanic) {
		t.Fatalf("expected SMPanicError, got %T: %v", reportedErr, reportedErr)
	}
	if smPanic.ShardID != 1 {
		t.Fatalf("expected ShardID 1, got %d", smPanic.ShardID)
	}
	if smPanic.ReplicaID != 1 {
		t.Fatalf("expected ReplicaID 1, got %d", smPanic.ReplicaID)
	}
	if smPanic.Value != "boom" {
		t.Fatalf("expected panic value 'boom', got %v", smPanic.Value)
	}
	if len(smPanic.Stack) == 0 {
		t.Fatal("expected non-empty stack trace")
	}

	// Verify errors.Is matches ErrShardFailed.
	if !errors.Is(smPanic, ErrShardFailed) {
		t.Fatal("expected SMPanicError to match ErrShardFailed via errors.Is")
	}

	// Verify commitPending was cleared (the deferred cleanup ran).
	if node.commitPending.Load() {
		t.Fatal("expected commitPending to be cleared after panic")
	}

	// Stop the worker cleanly.
	close(stopC)
}

// TestApplyWorker_RecoverFromSMPanic_ShardContinuesAfterPanic verifies
// that other shards continue processing after one shard's SM panics.
func TestApplyWorker_RecoverFromSMPanic_ShardContinuesAfterPanic(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	ws := NewWorkSignal(2)
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, ws, stopC, nil)

	// Shard 1: panicking state machine.
	panicker := &panicApplier{panicValue: "shard1-panic"}
	cfg1 := config.Config{ShardID: 1, ReplicaID: 1}
	cfg1.SetDefaults()
	node1 := NewNode(nil, panicker, nil, cfg1, nil, "", nil)

	// Shard 2: working state machine.
	goodSM := newTestSM()
	goodApplier := newTestApplier(goodSM)
	cfg2 := config.Config{ShardID: 2, ReplicaID: 1}
	cfg2.SetDefaults()
	node2 := NewNode(nil, goodApplier, nil, cfg2, nil, "", nil)

	go aw.run()

	// Send the panicking shard first.
	node1.commitPending.Store(true)
	item1 := applyPool.Get().(*applyItem)
	item1.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("panic")},
		},
	}
	item1.node = node1
	applyC <- item1

	// Wait for shard 1 to be marked as failed.
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for shard 1 to fail", node1.failed.Load)

	// Send work to shard 2 - it should still be processed.
	node2.commitPending.Store(true)
	item2 := applyPool.Get().(*applyItem)
	item2.update = proto.Update{
		ShardID:   2,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("should-succeed")},
		},
	}
	item2.node = node2
	applyC <- item2

	// Wait for callback to fire for shard 2.
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for shard 2 to apply", func() bool {
		for _, c := range cb.getAppliedCalls() {
			if c.shardID == 2 {
				return true
			}
		}
		return false
	})

	// Shard 2 should NOT be failed.
	if node2.failed.Load() {
		t.Fatal("shard 2 should not be failed")
	}

	// Shard 1 should be failed.
	if !node1.failed.Load() {
		t.Fatal("shard 1 should be failed")
	}

	close(stopC)
}

// TestApplyWorker_DispatchSkipsUnloadedShard verifies that when a shard
// has been unloaded via requestUnload, subsequent applyItems for that
// shard are discarded by dispatch() instead of spawning a new per-shard
// goroutine. This prevents the goroutine leak described in the bug where
// the commit worker enqueues an item after the unload is processed.
func TestApplyWorker_DispatchSkipsUnloadedShard(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	ws := NewWorkSignal(1)
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, ws, stopC, nil)

	// Create a working state machine and node for shard 10.
	goodSM := newTestSM()
	goodApplier := newTestApplier(goodSM)
	cfg := config.Config{ShardID: 10, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, goodApplier, nil, cfg, nil, "", nil)

	// Start the dispatcher.
	go aw.run()

	// Send one item so a per-shard goroutine is created for shard 10.
	node.commitPending.Store(true)
	item1 := applyPool.Get().(*applyItem)
	item1.update = proto.Update{
		ShardID:   10,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("first")},
		},
	}
	item1.node = node
	applyC <- item1

	// Wait for the apply callback to confirm shard 10 processed.
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for shard 10 initial apply", func() bool {
		for _, c := range cb.getAppliedCalls() {
			if c.shardID == 10 {
				return true
			}
		}
		return false
	})

	// Unload shard 10. The dispatcher processes the request via
	// removeShardChan, closing the per-shard channel and adding shard 10
	// to the unloaded set; the returned done channel closes only after
	// the per-shard goroutine has exited, so the unload is fully
	// processed before the stale item below is sent.
	<-aw.requestUnload(10)

	// Send a stale item for shard 10 after unload.
	node.commitPending.Store(true)
	item2 := applyPool.Get().(*applyItem)
	item2.update = proto.Update{
		ShardID:   10,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 2, Term: 1, Cmd: []byte("stale-after-unload")},
		},
	}
	item2.node = node
	applyC <- item2

	// Wait for commitPending to be cleared by the dispatch discard path.
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for commitPending to be cleared on discarded item", func() bool {
		return !node.commitPending.Load()
	})

	// Verify the second item was NOT applied (no second callback for shard 10).
	appliedCalls := cb.getAppliedCalls()
	count := 0
	for _, c := range appliedCalls {
		if c.shardID == 10 {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 apply callback for shard 10, got %d", count)
	}

	// Stop cleanly and verify no goroutine leak: wg.Wait in run()
	// should complete promptly since no leaked goroutine exists.
	close(stopC)
}

// TestApplyWorker_DispatchSkipsUnloadedShard_NoNode verifies that
// dispatch correctly handles a nil node on a stale applyItem for an
// unloaded shard. This covers the defensive nil check on item.node.
func TestApplyWorker_DispatchSkipsUnloadedShard_NoNode(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	ws := NewWorkSignal(1)
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, ws, stopC, nil)

	go aw.run()

	// Unload shard 99 before any item arrives. This adds it to the
	// unloaded set even though no per-shard channel exists; done closes
	// immediately since there is no goroutine to join.
	<-aw.requestUnload(99)

	// Send an item with nil node for shard 99.
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   99,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("orphaned")},
		},
	}
	item.node = nil
	applyC <- item

	// Wait for the applyC to be drained (item consumed by dispatcher).
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for dispatcher to drain applyC", func() bool {
		return len(applyC) == 0
	})

	// Stop cleanly. If dispatch panicked or leaked a goroutine, this
	// would hang or the test would have already failed.
	close(stopC)
}

// TestApplyWorker_UnloadThenReload verifies that after a shard is
// unloaded and then re-loaded via requestReload, the shard processes
// items normally again and the unloaded map entry is cleaned up.
func TestApplyWorker_UnloadThenReload(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	ws := NewWorkSignal(1)
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, ws, stopC, nil)

	goodSM := newTestSM()
	goodApplier := newTestApplier(goodSM)
	cfg := config.Config{ShardID: 5, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, goodApplier, nil, cfg, nil, "", nil)

	go aw.run()

	// Step 1: Apply an item so a per-shard goroutine exists.
	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   5,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("before-unload")},
		},
	}
	item.node = node
	applyC <- item

	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for initial apply", func() bool {
		for _, c := range cb.getAppliedCalls() {
			if c.shardID == 5 {
				return true
			}
		}
		return false
	})

	// Step 2: Unload shard 5. The returned done channel closes once the
	// unload is fully processed (per-shard goroutine exited), so the next
	// item is guaranteed to hit the unloaded guard.
	<-aw.requestUnload(5)

	// Step 3: Verify items are discarded while unloaded.
	node.commitPending.Store(true)
	item = applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   5,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 2, Term: 1, Cmd: []byte("while-unloaded")},
		},
	}
	item.node = node
	applyC <- item

	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for discarded item", func() bool {
		return !node.commitPending.Load()
	})

	// Only the initial apply should have a callback.
	appliedCalls := cb.getAppliedCalls()
	count := 0
	for _, c := range appliedCalls {
		if c.shardID == 5 {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected 1 apply callback for shard 5 before reload, got %d", count)
	}

	// Step 4: Re-load shard 5 (clears unloaded entry). In production,
	// LoadNode creates a fresh Node, so we do the same to avoid sharing
	// the old node's non-atomic fields with the old goroutine (which
	// may still be draining).
	// The reload rendezvouses with the dispatcher, which applies it before
	// it can pick up the item queued below.
	aw.requestReload(5)

	goodSM2 := newTestSM()
	goodApplier2 := newTestApplier(goodSM2)
	cfg2 := config.Config{ShardID: 5, ReplicaID: 1}
	cfg2.SetDefaults()
	node2 := NewNode(nil, goodApplier2, nil, cfg2, nil, "", nil)
	// Host.StartShard seeds the new incarnation's applied marker from raft's
	// processed marker before LoadNode. Index 2 was dropped by the unloaded
	// guard above and re-offered to this incarnation as part of its recovered
	// frontier, so the next batch legitimately starts at 3.
	node2.SeedLastApplied(2, 1)

	// Step 5: Send a new item. It should be applied normally.
	node2.commitPending.Store(true)
	item = applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   5,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 3, Term: 1, Cmd: []byte("after-reload")},
		},
	}
	item.node = node2
	applyC <- item

	// Should now have 2 callbacks: initial + post-reload.
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for post-reload apply", func() bool {
		postReloadCount := 0
		for _, c := range cb.getAppliedCalls() {
			if c.shardID == 5 {
				postReloadCount++
			}
		}
		return postReloadCount >= 2
	})

	close(stopC)
}

// TestApplyWorker_UnloadedMapCleanup verifies that the unloaded map
// does not grow without bound. After unload/reload cycles, the map
// should not retain entries for re-loaded shards.
func TestApplyWorker_UnloadedMapCleanup(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	ws := NewWorkSignal(1)
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, ws, stopC, nil)

	go aw.run()

	// Simulate high shard churn: unload then reload 100 shards. Each
	// unload is synchronous (done closes when processing completes).
	for i := uint64(1); i <= 100; i++ {
		<-aw.requestUnload(i)
	}

	// Each reload rendezvouses with the dispatcher, so all 100 are applied
	// before the items queued below can be dispatched.
	for i := uint64(1); i <= 100; i++ {
		aw.requestReload(i)
	}

	// Verify the unloaded map is empty by sending items for all 100
	// shards and confirming none are discarded (they create per-shard
	// goroutines). We verify indirectly: if any shard were still in
	// unloaded, its item would be discarded and commitPending would
	// be cleared without creating a shardChan entry.
	goodSM := newTestSM()
	goodApplier := newTestApplier(goodSM)

	for i := uint64(1); i <= 100; i++ {
		cfg := config.Config{ShardID: i, ReplicaID: 1}
		cfg.SetDefaults()
		node := NewNode(nil, goodApplier, nil, cfg, nil, "", nil)
		node.commitPending.Store(true)

		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:   i,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{Index: 1, Term: 1, Cmd: []byte("after-churn")},
			},
		}
		item.node = node
		applyC <- item
	}

	// Wait for all 100 items to be applied (confirming none were discarded).
	waitFor(t, 10*time.Second, time.Millisecond, "timed out: not all 100 items applied", func() bool {
		return goodSM.applyCount.Load() >= 100
	})

	close(stopC)
	// wg.Wait in run() should complete promptly if no goroutine leaked.
}

// TestSMPanicError_ErrorMessage verifies the error message format.
func TestSMPanicError_ErrorMessage(t *testing.T) {
	err := &SMPanicError{
		ShardID:   42,
		ReplicaID: 7,
		Value:     "test panic",
		Stack:     []byte("goroutine 1\nstack trace"),
	}
	expected := "engine: state machine panic for shard 42 replica 7: test panic"
	if err.Error() != expected {
		t.Fatalf("expected %q, got %q", expected, err.Error())
	}
}

// TestSMPanicError_Is_MatchesSentinel verifies errors.Is matches ErrShardFailed.
func TestSMPanicError_Is_MatchesSentinel(t *testing.T) {
	err := &SMPanicError{
		ShardID:   1,
		ReplicaID: 1,
		Value:     "panic",
	}
	if !errors.Is(err, ErrShardFailed) {
		t.Fatal("errors.Is(SMPanicError, ErrShardFailed) returned false")
	}
}

// TestApplyWorker_ReloadServicedBeforeQueuedItem verifies that a re-loaded
// shard's first committed batch is applied rather than dropped. LoadNode
// requests the reload before publishing the node; the request rendezvouses
// with the dispatcher, which therefore clears the unloaded mark before it can
// pick up the shard's next committed batch. A dropped batch is permanent:
// Peer.Commit has already advanced the processed marker, so it is never
// re-offered.
func TestApplyWorker_ReloadServicedBeforeQueuedItem(t *testing.T) {
	applyC := make(chan *applyItem, 4)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	cfg := config.Config{ShardID: 7, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, newTestApplier(testSm), nil, cfg, nil, "", nil)

	// The shard was unloaded, then re-loaded, and the new incarnation's first
	// committed batch follows immediately.
	aw.unloaded.add(7)
	go aw.run()
	aw.requestReload(7)

	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:          7,
		ReplicaID:        1,
		CommittedEntries: []proto.Entry{{Index: 1, Term: 1, Cmd: []byte("after-reload")}},
	}
	item.node = node
	applyC <- item

	// commitPending is the worker's per-item completion mark: the per-shard
	// goroutine clears it in a defer that runs AFTER SM.Apply has returned
	// and lastApplied has been stored, and the unloaded-discard path clears
	// it too, so the wait ends promptly whichever way the item went and
	// every read below is pinned. testSM.Update bumps applyCount on ENTRY,
	// so waiting on that alone observes the batch mid-apply, before the
	// worker has advanced lastApplied.
	waitFor(t, 5*time.Second, time.Millisecond,
		"committed batch of the re-loaded shard was never processed",
		func() bool { return !node.commitPending.Load() })

	if node.IsFailed() {
		t.Fatal("node must not be failed")
	}
	// The ordering guarantee: had the item been dispatched before the reload
	// was serviced, the dispatcher would have dropped it as "unloaded" and
	// the state machine would never have seen it.
	if got := testSm.applyCount.Load(); got != 1 {
		t.Fatalf("applyCount = %d, want 1: the batch was dropped as unloaded, "+
			"so the reload was not serviced before it", got)
	}
	if got := node.lastApplied.Load(); got != 1 {
		t.Fatalf("lastApplied = %d, want 1", got)
	}
}

// TestApplyWorker_MembershipOrderIsDeterministic verifies that the dispatcher
// applies unload and reload requests in the order they were made.
//
// While the two travelled on separate channels (an unbuffered unloadC and a
// buffered reloadC) the dispatcher's select picked between them uniformly at
// random whenever both were ready. A LoadNode(S) immediately followed by a
// StopShard(S) could therefore be applied in reverse, leaving S OUT of the
// unloaded set after it had been unloaded: a stale applyItem would then spawn
// a per-shard goroutine that nothing ever closes and apply committed entries
// into a state machine StopShard has already closed. The reverse inversion
// leaves a re-loaded shard marked unloaded, so its batches are dropped.
//
// The loop runs the race many times because a two-channel dispatcher picks
// the wrong order roughly half the time.
func TestApplyWorker_MembershipOrderIsDeterministic(t *testing.T) {
	const iterations = 500
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}

	// runCase starts a dispatcher, lets ops stage its membership requests,
	// then stops the dispatcher and reports whether shard 4 ended up
	// unloaded. The set is read only after the dispatcher goroutine exits.
	runCase := func(ops func(aw *applyWorker)) bool {
		applyC := make(chan *applyItem, 1)
		var wg sync.WaitGroup
		stopC := make(chan struct{})
		aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)
		runDone := make(chan struct{})
		go func() {
			aw.run()
			close(runDone)
		}()
		ops(aw)
		close(stopC)
		<-runDone
		wg.Wait()
		return aw.unloaded.has(4)
	}

	for i := range iterations {
		// StartShard(4) then StopShard(4): the shard must end up UNLOADED.
		unloaded := runCase(func(aw *applyWorker) {
			aw.requestReload(4)
			<-aw.requestUnload(4)
		})
		if !unloaded {
			t.Fatalf("iteration %d: shard 4 is loaded after reload-then-unload; "+
				"the unload was applied before the reload it followed", i)
		}

		// StopShard(4) then StartShard(4): the shard must end up LOADED.
		unloaded = runCase(func(aw *applyWorker) {
			<-aw.requestUnload(4)
			aw.requestReload(4)
		})
		if unloaded {
			t.Fatalf("iteration %d: shard 4 is unloaded after unload-then-reload; "+
				"the reload was applied before the unload it followed", i)
		}
	}
}

// TestApplyWorker_GapDetection_FailsShard verifies that a batch which does not
// continue at lastApplied+1 fails the shard instead of being applied. The
// missing range can never be re-offered, so applying over the hole would leave
// this state machine silently diverged from its peers.
func TestApplyWorker_GapDetection_FailsShard(t *testing.T) {
	applyC := make(chan *applyItem, 4)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, newTestApplier(testSm), nil, cfg, nil, "", nil)
	node.SeedLastApplied(5, 1)

	go aw.run()

	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:          1,
		ReplicaID:        1,
		CommittedEntries: []proto.Entry{{Index: 8, Term: 1, Cmd: []byte("gap")}},
	}
	item.node = node
	applyC <- item

	// failApply reports the batch through OnProposalFailed BEFORE it marks
	// the node failed, so waiting on the callback count alone reads
	// IsFailed before the worker has set it. commitPending is cleared by
	// the per-shard goroutine's defer after failApply has returned, which
	// pins every read below.
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for the gapped batch to be processed",
		func() bool { return !node.commitPending.Load() })

	if !node.IsFailed() {
		t.Fatal("node must be marked failed on a committed-entry gap")
	}
	if testSm.applyCount.Load() != 0 {
		t.Fatal("entries after a gap must not be applied")
	}
	if got := node.lastApplied.Load(); got != 5 {
		t.Fatalf("lastApplied must not advance past the gap, got %d", got)
	}
	if got := cb.proposalFailedCount.Load(); got != 1 {
		t.Fatalf("OnProposalFailed calls = %d, want 1", got)
	}
	failed := cb.getProposalFailedCalls()
	if !errors.Is(failed[0].err, ErrApplyGap) {
		t.Fatalf("expected ErrApplyGap, got %v", failed[0].err)
	}
}

// TestApplyWorker_GapDetection_FailsShardWhenNothingApplied verifies that the
// gap check also covers a shard whose applied marker is still zero.
//
// A fresh shard is seeded with SeedLastApplied(0, 0) and raft's processed
// marker is 0 with it, so raft offers its first batch at index 1. A first
// batch that starts anywhere above 1 therefore means an earlier batch was
// dropped by one of the three drop paths (the commit worker's unloaded skip,
// the apply dispatcher's unloaded/shutdown drop, a failed Peer.Commit), each
// of which relies on this check as its safety net. Peer.Commit has already
// advanced the processed marker past the missing range, so it can never be
// re-offered: applying over the hole would leave the replica acking the leader
// and serving reads from a state machine missing committed entries — a state
// machine safety violation with no error and no failed shard.
func TestApplyWorker_GapDetection_FailsShardWhenNothingApplied(t *testing.T) {
	applyC := make(chan *applyItem, 4)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, newTestApplier(testSm), nil, cfg, nil, "", nil)
	// A fresh shard: nothing recovered, nothing applied.
	node.SeedLastApplied(0, 0)

	go aw.run()

	// Entries 1..7 were dropped; the pipeline offers 8..9 next.
	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 8, Term: 1, Cmd: []byte("gap")},
			{Index: 9, Term: 1, Cmd: []byte("gap")},
		},
	}
	item.node = node
	applyC <- item

	// See TestApplyWorker_GapDetection_FailsShard: the deferred commitPending
	// clear is the only mark that follows failed.Store.
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for the gapped batch to be processed",
		func() bool { return !node.commitPending.Load() })

	if !node.IsFailed() {
		t.Fatal("node must be marked failed on a committed-entry gap")
	}
	if got := cb.proposalFailedCount.Load(); got != 1 {
		t.Fatalf("OnProposalFailed calls = %d, want 1", got)
	}
	if got := testSm.applyCount.Load(); got != 0 {
		t.Fatalf("entries after a gap must not be applied, applyCount = %d", got)
	}
	if got := cb.appliedCount.Load(); got != 0 {
		t.Fatalf("OnApplied must not fire for a gapped batch, got %d calls", got)
	}
	if got := node.lastApplied.Load(); got != 0 {
		t.Fatalf("lastApplied must not advance past the gap, got %d", got)
	}
	failed := cb.getProposalFailedCalls()
	if !errors.Is(failed[0].err, ErrApplyGap) {
		t.Fatalf("expected ErrApplyGap, got %v", failed[0].err)
	}
}
