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
	node := NewNode(nil, panicker, nil, cfg, 100, nil, "", listener)

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
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for panic recovery")
		default:
		}
		if errorReported.Load() != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}

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
	node1 := NewNode(nil, panicker, nil, cfg1, 100, nil, "", nil)

	// Shard 2: working state machine.
	goodSM := newTestSM()
	goodApplier := newTestApplier(goodSM)
	cfg2 := config.Config{ShardID: 2, ReplicaID: 1}
	cfg2.SetDefaults()
	node2 := NewNode(nil, goodApplier, nil, cfg2, 100, nil, "", nil)

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
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for shard 1 to fail")
		default:
		}
		if node1.failed.Load() {
			break
		}
		time.Sleep(time.Millisecond)
	}

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
	deadline = time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for shard 2 to apply")
		default:
		}
		calls := cb.getAppliedCalls()
		for _, c := range calls {
			if c.shardID == 2 {
				goto done
			}
		}
		time.Sleep(time.Millisecond)
	}
done:

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
	node := NewNode(nil, goodApplier, nil, cfg, 100, nil, "", nil)

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
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for shard 10 initial apply")
		default:
		}
		calls := cb.getAppliedCalls()
		for _, c := range calls {
			if c.shardID == 10 {
				goto applied
			}
		}
		time.Sleep(time.Millisecond)
	}
applied:

	// Unload shard 10. This sends on unloadC, which the dispatcher
	// will process via removeShardChan, closing the per-shard channel
	// and adding shard 10 to the unloaded set.
	aw.requestUnload(10)

	// Wait for the dispatcher to drain unloadC, confirming the unload
	// has been processed before we send the stale item. Otherwise the
	// item could arrive before the unload and be applied instead of discarded.
	deadlineUnload := time.After(5 * time.Second)
	for len(aw.unloadC) > 0 {
		select {
		case <-deadlineUnload:
			t.Fatal("timed out waiting for unloadC to drain")
		default:
		}
		time.Sleep(time.Millisecond)
	}

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
	deadline = time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for commitPending to be cleared on discarded item")
		default:
		}
		if !node.commitPending.Load() {
			break
		}
		time.Sleep(time.Millisecond)
	}

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
	// unloaded set even though no per-shard channel exists.
	aw.requestUnload(99)

	// Wait for the dispatcher to process the unload before sending the
	// item. Poll until unloadC is empty.
	deadline99 := time.After(5 * time.Second)
	for len(aw.unloadC) > 0 {
		select {
		case <-deadline99:
			t.Fatal("timed out waiting for unloadC to drain")
		default:
		}
		time.Sleep(time.Millisecond)
	}

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
	deadlineItem := time.After(5 * time.Second)
	for len(applyC) > 0 {
		select {
		case <-deadlineItem:
			t.Fatal("timed out waiting for dispatcher to drain applyC")
		default:
		}
		time.Sleep(time.Millisecond)
	}

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
	node := NewNode(nil, goodApplier, nil, cfg, 100, nil, "", nil)

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

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for initial apply")
		default:
		}
		calls := cb.getAppliedCalls()
		for _, c := range calls {
			if c.shardID == 5 {
				goto initialApplied
			}
		}
		time.Sleep(time.Millisecond)
	}
initialApplied:

	// Step 2: Unload shard 5. Wait for the dispatcher to drain unloadC,
	// which confirms the unload is processed before we send the next item.
	aw.requestUnload(5)
	deadline2 := time.After(5 * time.Second)
	for len(aw.unloadC) > 0 {
		select {
		case <-deadline2:
			t.Fatal("timed out waiting for unloadC to be drained")
		default:
		}
		time.Sleep(time.Millisecond)
	}

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

	deadline = time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for discarded item")
		default:
		}
		if !node.commitPending.Load() {
			break
		}
		time.Sleep(time.Millisecond)
	}

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
	aw.requestReload(5)
	deadline3 := time.After(5 * time.Second)
	for len(aw.reloadC) > 0 {
		select {
		case <-deadline3:
			t.Fatal("timed out waiting for reloadC to be drained")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	goodSM2 := newTestSM()
	goodApplier2 := newTestApplier(goodSM2)
	cfg2 := config.Config{ShardID: 5, ReplicaID: 1}
	cfg2.SetDefaults()
	node2 := NewNode(nil, goodApplier2, nil, cfg2, 100, nil, "", nil)

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

	deadline = time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for post-reload apply")
		default:
		}
		calls := cb.getAppliedCalls()
		postReloadCount := 0
		for _, c := range calls {
			if c.shardID == 5 {
				postReloadCount++
			}
		}
		// Should now have 2 callbacks: initial + post-reload.
		if postReloadCount >= 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}

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

	// Simulate high shard churn: unload then reload 100 shards.
	// Wait for each batch to be fully drained before proceeding.
	for i := uint64(1); i <= 100; i++ {
		aw.requestUnload(i)
	}
	deadlineU := time.After(5 * time.Second)
	for len(aw.unloadC) > 0 {
		select {
		case <-deadlineU:
			t.Fatal("timed out waiting for unloadC to be drained")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	for i := uint64(1); i <= 100; i++ {
		aw.requestReload(i)
	}
	deadlineR := time.After(5 * time.Second)
	for len(aw.reloadC) > 0 {
		select {
		case <-deadlineR:
			t.Fatal("timed out waiting for reloadC to be drained")
		default:
		}
		time.Sleep(time.Millisecond)
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
		node := NewNode(nil, goodApplier, nil, cfg, 100, nil, "", nil)
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
	deadlineAll := time.After(10 * time.Second)
	for goodSM.applyCount.Load() < 100 {
		select {
		case <-deadlineAll:
			t.Fatalf("timed out: only %d/100 items applied", goodSM.applyCount.Load())
		default:
		}
		time.Sleep(time.Millisecond)
	}

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

// TestSMPanicError_Is_DoesNotMatchOther verifies errors.Is does not
// match unrelated sentinels.
func TestSMPanicError_Is_DoesNotMatchOther(t *testing.T) {
	err := &SMPanicError{
		ShardID:   1,
		ReplicaID: 1,
		Value:     "panic",
	}
	if errors.Is(err, ErrStopped) {
		t.Fatal("errors.Is(SMPanicError, ErrStopped) should return false")
	}
}
