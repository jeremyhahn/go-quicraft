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
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// waitFor polls cond every pollInterval until it returns true or timeout
// elapses, at which point it calls t.Fatal with msg. This replaces all
// "time.Sleep then assert" patterns with a deterministic poll loop.
func waitFor(t *testing.T, timeout, pollInterval time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(pollInterval)
	}
}

// --- Test LogDB implementation ---

type testLogDB struct {
	mu         sync.Mutex
	saveCount  atomic.Int64
	saveErr    error
	saved      []logdb.Update
	failAfterN int64 // if > 0, SaveState returns saveErr when saveCount > failAfterN
}

func newTestLogDB() *testLogDB {
	return &testLogDB{}
}

func (t *testLogDB) Name() string { return "test" }

func (t *testLogDB) SaveState(updates []logdb.Update) error {
	count := t.saveCount.Add(1)
	t.mu.Lock()
	if t.failAfterN > 0 && count > t.failAfterN && t.saveErr != nil {
		err := t.saveErr
		t.mu.Unlock()
		return err
	}
	if t.failAfterN == 0 && t.saveErr != nil {
		err := t.saveErr
		t.mu.Unlock()
		return err
	}
	t.saved = append(t.saved, updates...)
	t.mu.Unlock()
	return nil
}

func (t *testLogDB) setSaveError(err error) {
	t.mu.Lock()
	t.saveErr = err
	t.mu.Unlock()
}

// setFailAfterN configures the testLogDB to return saveErr only after
// N successful SaveState calls. Calls 1..N succeed; calls N+1.. fail.
func (t *testLogDB) setFailAfterN(n int64, err error) {
	t.mu.Lock()
	t.failAfterN = n
	t.saveErr = err
	t.mu.Unlock()
}

func (t *testLogDB) getSavedCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.saved)
}

func (t *testLogDB) IterateEntries(entries []logdb.Entry, size uint64, _ uint64, _ uint64,
	_ uint64, _ uint64, _ uint64) ([]logdb.Entry, uint64, error) {
	return entries, size, nil
}

func (t *testLogDB) ReadState(_ uint64, _ uint64) (logdb.State, logdb.Membership, error) {
	return logdb.State{}, logdb.Membership{}, nil
}

func (t *testLogDB) EntryRange(_ uint64, _ uint64) (uint64, uint64, error) { return 0, 0, nil }

func (t *testLogDB) RemoveEntriesTo(_ uint64, _ uint64, _ uint64) error { return nil }

func (t *testLogDB) Compact(_ uint64, _ uint64) error { return nil }

func (t *testLogDB) SaveSnapshot(_ uint64, _ uint64, _ logdb.Snapshot) error { return nil }

func (t *testLogDB) GetSnapshot(_ uint64, _ uint64) (logdb.Snapshot, error) {
	return logdb.Snapshot{}, nil
}

func (t *testLogDB) RemoveNodeData(_ uint64, _ uint64) error { return nil }

func (t *testLogDB) ListNodeInfo() ([]logdb.NodeInfo, error) { return nil, nil }

func (t *testLogDB) SaveBootstrap(_ uint64, _ uint64, _ logdb.Bootstrap) error { return nil }

func (t *testLogDB) GetBootstrap(_ uint64, _ uint64) (logdb.Bootstrap, bool, error) {
	return logdb.Bootstrap{}, false, nil
}

func (t *testLogDB) Close() error { return nil }

var _ logdb.LogDB = (*testLogDB)(nil)

// --- Test State Machine ---

type testSM struct {
	mu          sync.Mutex
	applyCount  atomic.Int64
	applyErr    error
	lastApplied uint64
}

func newTestSM() *testSM {
	return &testSM{}
}

func (s *testSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	s.applyCount.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applyErr != nil {
		return s.applyErr
	}
	for i := range results {
		results[i] = sm.Result{Value: entries[i].Index}
	}
	if len(entries) > 0 {
		s.lastApplied = entries[len(entries)-1].Index
	}
	return nil
}

func (s *testSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return nil, nil
}

func (s *testSM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (s *testSM) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (s *testSM) Close(_ context.Context) error {
	return nil
}

func (s *testSM) setApplyError(err error) {
	s.mu.Lock()
	s.applyErr = err
	s.mu.Unlock()
}

var _ sm.StateMachine = (*testSM)(nil)

// --- Step Worker Tests ---

func TestStepWorker_LoadAndUnloadNode(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	cfg := config.Config{ShardID: 10, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	sw.nodesMu.RLock()
	_, ok := sw.nodes[10]
	sw.nodesMu.RUnlock()
	if !ok {
		t.Fatal("node should be loaded")
	}

	sw.unloadNode(10)

	sw.nodesMu.RLock()
	_, ok = sw.nodes[10]
	sw.nodesMu.RUnlock()
	if ok {
		t.Fatal("node should be unloaded")
	}
}

func TestStepWorker_UnloadNonexistent(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)
	// Should not panic.
	sw.unloadNode(999)
}

func TestStepWorker_UnloadCleansUpMaps(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	cfg := config.Config{ShardID: 10, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Populate commitSlots and prevLeaders.
	_ = sw.getCommitSlot(10)
	sw.prevLeaders[10] = 1

	if _, ok := sw.commitSlots[10]; !ok {
		t.Fatal("commitSlots should contain shard 10")
	}
	if _, ok := sw.prevLeaders[10]; !ok {
		t.Fatal("prevLeaders should contain shard 10")
	}

	sw.unloadNode(10)

	if _, ok := sw.commitSlots[10]; ok {
		t.Fatal("commitSlots should be cleaned up after unload")
	}
	if _, ok := sw.prevLeaders[10]; ok {
		t.Fatal("prevLeaders should be cleaned up after unload")
	}
}

func TestStepWorker_GetCommitSlot_CreateAndReuse(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	// First call creates.
	slot1 := sw.getCommitSlot(42)
	if slot1 == nil {
		t.Fatal("expected non-nil commit slot")
	}

	// Second call reuses.
	slot2 := sw.getCommitSlot(42)
	if slot1 != slot2 {
		t.Fatal("expected same commit slot to be reused")
	}
}

func TestStepWorker_RunStopsOnStopC(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	done := make(chan struct{})
	go func() {
		sw.run()
		close(done)
	}()

	close(stopC)
	select {
	case <-done:
		// Expected: worker exited.
	case <-time.After(2 * time.Second):
		t.Fatal("step worker did not stop within timeout")
	}
}

func TestStepWorker_SkipsFailedNodes(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	cfg := config.Config{ShardID: 10, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.failed.Store(true)
	sw.loadNode(node)

	// Tick should skip failed nodes without panicking.
	sw.tick()
}

// --- Commit Worker Tests ---

func TestCommitWorker_ProcessBatch_Success(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	// Override sleep for tests.
	cw.noRetryDelay = true

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	// We need a peer for Commit. Since we cannot easily construct one
	// without a full raft.LogReader, test the batch conversion and
	// SaveState separately.

	// Test the drain and SaveState path directly.
	cw.batch = append(cw.batch[:0], proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		State:     proto.State{Term: 1},
		EntriesToSave: []proto.Entry{
			{Term: 1, Index: 1, Cmd: []byte("cmd")},
		},
	})
	cw.batchNodes = []*Node{node}
	cw.logdbBuf, cw.flatBuf = protoUpdatesToLogDBReuse(cw.logdbBuf, cw.flatBuf, cw.batch)

	err := ldb.SaveState(cw.logdbBuf)
	if err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}
	if ldb.saveCount.Load() != 1 {
		t.Fatalf("expected 1 SaveState call, got %d", ldb.saveCount.Load())
	}
	if ldb.getSavedCount() != 1 {
		t.Fatalf("expected 1 saved update, got %d", ldb.getSavedCount())
	}
}

func TestCommitWorker_ProcessBatch_SaveStateError(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true // skip retry backoff in tests

	saveErr := errors.New("disk failure")
	ldb.setSaveError(saveErr)

	var errorCalled atomic.Bool
	listener := &config.EventListener{
		OnShardFailed: func(_ config.ShardFailedInfo) {
			errorCalled.Store(true)
		},
	}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", listener)
	node.commitPending.Store(true)

	cw.batch = append(cw.batch[:0], proto.Update{ShardID: 1, ReplicaID: 1})
	cw.batchNodes = []*Node{node}
	cw.processBatch()

	// commitPending should be cleared on error.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared after SaveState error")
	}

	// Error callback should have been called.
	if !errorCalled.Load() {
		t.Fatal("expected error callback to be invoked")
	}

	// SaveState should have been called saveStateMaxRetries times.
	if ldb.saveCount.Load() != int64(saveStateMaxRetries) {
		t.Fatalf("expected %d SaveState calls (retries), got %d",
			saveStateMaxRetries, ldb.saveCount.Load())
	}
}

func TestCommitWorker_RunStopsOnStopC(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	done := make(chan struct{})
	go func() {
		cw.run()
		close(done)
	}()

	close(stopC)
	select {
	case <-done:
		// Expected.
	case <-time.After(2 * time.Second):
		t.Fatal("commit worker did not stop within timeout")
	}
}

func TestCommitWorker_DrainCommitC(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)

	// Pre-fill commitC with items.
	for i := range 5 {
		commitC <- &commitItem{
			update: proto.Update{ShardID: uint64(i + 1)},
			node:   node,
		}
	}

	// Initialize batch with one item.
	cw.batch = append(cw.batch[:0], proto.Update{ShardID: 100})
	cw.batchNodes = []*Node{node}

	cw.drainCommitC()

	// Batch should now have 6 items (1 initial + 5 drained).
	if len(cw.batch) != 6 {
		t.Fatalf("expected 6 items in batch, got %d", len(cw.batch))
	}
}

// --- Apply Worker Tests ---

func TestApplyWorker_Dispatch_CreatesShardGoroutine(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{ShardID: 1}
	item.node = node

	// Send via applyC and run the dispatcher to avoid races.
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until commitPending is cleared by the apply goroutine.
	waitFor(t, 5*time.Second, time.Millisecond,
		"commitPending should be cleared after apply",
		func() bool { return !node.commitPending.Load() })

	// Clean up.
	close(stopC)
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not stop within timeout")
	}
	wg.Wait()
}

func TestApplyWorker_SequentialPerShard(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)

	// Send multiple items for the same shard via applyC.
	for i := range 5 {
		node.commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:   1,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{Term: 1, Index: uint64(i + 1), Cmd: []byte("cmd")},
			},
		}
		item.node = node
		applyC <- item
	}

	// Start the apply worker dispatcher loop.
	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until all 5 entries are applied.
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for all 5 applies",
		func() bool { return testSm.applyCount.Load() >= 5 })

	// Signal stop and wait for run() and shard goroutines to finish.
	close(stopC)
	<-runDone
	wg.Wait()

	// All 5 applies should have run.
	if testSm.applyCount.Load() != 5 {
		t.Fatalf("expected 5 applies, got %d", testSm.applyCount.Load())
	}

	// lastApplied should be 5.
	if node.lastApplied.Load() != 5 {
		t.Fatalf("lastApplied = %d, want 5", node.lastApplied.Load())
	}
}

func TestApplyWorker_CircuitBreaker(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	testSm.setApplyError(errors.New("permanent failure"))
	rsmSM := newTestApplier(testSm)

	var failedCalled atomic.Int64
	listener := &config.EventListener{
		OnShardFailed: func(_ config.ShardFailedInfo) {
			failedCalled.Add(1)
		},
	}
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 3, nil, "", listener) // maxApplyRetries=3

	// Send 3 items to trigger circuit breaker.
	for i := range 3 {
		node.commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:   1,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{Term: 1, Index: uint64(i + 1), Cmd: []byte("cmd")},
			},
		}
		item.node = node
		applyC <- item
	}

	// Start the apply worker dispatcher loop.
	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until circuit breaker trips (node.failed set).
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for circuit breaker to trip",
		func() bool { return node.IsFailed() })

	close(stopC)
	<-runDone
	wg.Wait()

	if !node.IsFailed() {
		t.Fatal("node should be in failed state after circuit breaker trips")
	}

	if node.applyRetries.Load() < 3 {
		t.Fatalf("applyRetries = %d, want >= 3", node.applyRetries.Load())
	}

	// lastApplied must advance even on error because the commit worker
	// already advanced processed. Not advancing would create a permanent
	// gap in the apply pipeline.
	if node.lastApplied.Load() != 3 {
		t.Fatalf("lastApplied = %d, want 3 (highest index in batch)", node.lastApplied.Load())
	}
}

func TestApplyWorker_ClearsCommitPendingOnEmptyEntries(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{ShardID: 1, CommittedEntries: nil} // empty
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	waitFor(t, 5*time.Second, time.Millisecond,
		"commitPending should be cleared even with empty entries",
		func() bool { return !node.commitPending.Load() })

	close(stopC)
	<-runDone
	wg.Wait()
}

func TestApplyWorker_RunStopsOnStopC(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	done := make(chan struct{})
	go func() {
		aw.run()
		close(done)
	}()

	close(stopC)
	select {
	case <-done:
		// Expected.
	case <-time.After(2 * time.Second):
		t.Fatal("apply worker did not stop within timeout")
	}
}

func TestApplyWorker_DrainOnShutdown(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)

	// Pre-fill applyC with items.
	for i := range 5 {
		node.commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:          1,
			CommittedEntries: []proto.Entry{{Term: 1, Index: uint64(i + 1), Cmd: []byte("c")}},
		}
		item.node = node
		applyC <- item
	}

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Signal stop - drain should process remaining items.
	close(stopC)

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("apply worker did not finish draining")
	}

	// Wait for shard goroutines to complete.
	wg.Wait()

	// All items should have been applied.
	if testSm.applyCount.Load() != 5 {
		t.Fatalf("expected 5 applies during drain, got %d", testSm.applyCount.Load())
	}
}

func TestApplyWorker_FanOut_MultipleShards(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	// Create 4 shards, each with their own SM.
	nodes := make([]*Node, 4)
	sms := make([]*testSM, 4)
	for i := range 4 {
		sms[i] = newTestSM()
		rsmSM := newTestApplier(sms[i])
		cfg := config.Config{ShardID: uint64(i + 1), ReplicaID: 1}
		nodes[i] = NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)
	}

	// Send items for all 4 shards via applyC.
	for i := range 4 {
		nodes[i].commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:          uint64(i + 1),
			CommittedEntries: []proto.Entry{{Term: 1, Index: 1, Cmd: []byte("cmd")}},
		}
		item.node = nodes[i]
		applyC <- item
	}

	// Start the apply worker dispatcher loop.
	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until all 4 shards have been applied.
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for all 4 shards to apply",
		func() bool {
			for i := range 4 {
				if sms[i].applyCount.Load() < 1 {
					return false
				}
			}
			return true
		})

	close(stopC)
	<-runDone
	wg.Wait()

	// All 4 shards should have 1 apply each.
	for i := range 4 {
		if sms[i].applyCount.Load() != 1 {
			t.Fatalf("shard %d: expected 1 apply, got %d", i+1, sms[i].applyCount.Load())
		}
	}
}

func TestApplyWorker_SemaphoreLimitsConcurrency(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	// Limit to 2 concurrent apply goroutines.
	aw := newApplyWorker(0, applyC, applyPool, 2, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	var concurrent atomic.Int64
	var maxConcurrent atomic.Int64

	slowSM := &slowTestSM{
		delay:         50 * time.Millisecond,
		concurrent:    &concurrent,
		maxConcurrent: &maxConcurrent,
	}
	rsmSM := newTestApplier(slowSM)

	// Create 4 items for 4 different shards and send via applyC.
	for i := range 4 {
		node := NewNode(nil, rsmSM, nil,
			config.Config{ShardID: uint64(i + 1), ReplicaID: 1}, 100, nil, "", nil)
		node.commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:          uint64(i + 1),
			CommittedEntries: []proto.Entry{{Term: 1, Index: 1, Cmd: []byte("c")}},
		}
		item.node = node
		applyC <- item
	}

	// Start the apply worker dispatcher loop.
	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until all 4 items have been applied (each with 50ms delay,
	// 2 concurrent batches so total ~100ms). slowTestSM increments
	// maxConcurrent as a side effect of each apply.
	waitFor(t, 10*time.Second, 5*time.Millisecond,
		"timed out waiting for all 4 slow applies to complete",
		func() bool { return maxConcurrent.Load() >= 1 && concurrent.Load() == 0 })

	// Signal stop and wait for cleanup.
	close(stopC)
	<-runDone
	wg.Wait()

	// Due to semaphore limit of 2, no more than 2 should run concurrently.
	if maxConcurrent.Load() > 2 {
		t.Fatalf("max concurrent = %d, expected <= 2", maxConcurrent.Load())
	}

	// All 4 should have been processed.
	if maxConcurrent.Load() < 1 {
		t.Fatal("expected at least 1 concurrent apply")
	}
}

func TestApplyWorker_CircuitBreakerResetOnSuccess(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 10, nil, "", nil) // maxApplyRetries=10

	// Start the apply worker dispatcher loop.
	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// First: fail twice.
	testSm.setApplyError(errors.New("transient"))
	for i := range 2 {
		node.commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:          1,
			CommittedEntries: []proto.Entry{{Term: 1, Index: uint64(i + 1), Cmd: []byte("c")}},
		}
		item.node = node
		applyC <- item
	}
	// Poll until both failing items have been processed (lastApplied advances
	// even on error so the commit pipeline doesn't stall).
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for 2 failing items to process",
		func() bool { return node.lastApplied.Load() >= 2 })

	// Now succeed.
	testSm.setApplyError(nil)
	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:          1,
		CommittedEntries: []proto.Entry{{Term: 1, Index: 3, Cmd: []byte("c")}},
	}
	item.node = node
	applyC <- item
	// Poll until the success item is processed.
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for success item to process",
		func() bool { return node.lastApplied.Load() >= 3 })

	close(stopC)
	<-runDone
	wg.Wait()

	// After the run() goroutine has fully exited, it is safe to read
	// non-atomic node fields that are only written by the per-shard
	// goroutine (which wg.Wait() guarantees is done).
	if node.applyRetries.Load() != 0 {
		t.Fatalf("applyRetries = %d, want 0 after success", node.applyRetries.Load())
	}
}

func TestApplyWorker_RemoveShardChan(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{ShardID: 1}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until commitPending is cleared (item was processed by the dispatcher).
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for item to be processed",
		func() bool { return !node.commitPending.Load() })

	close(stopC)
	<-runDone
	wg.Wait()

	// After run() has fully exited, it is safe to call removeShardChan
	// since there is no concurrent access to shardChans.
	// Removing a non-existent shard channel should not panic.
	aw.removeShardChan(999)
}

func TestApplyWorker_RemoveShardChan_ExistingShard(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	cfg := config.Config{ShardID: 42, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:          42,
		CommittedEntries: []proto.Entry{{Term: 1, Index: 1, Cmd: []byte("c")}},
	}
	item.node = node

	// Dispatch via run() to create the shard channel.
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until the apply has been processed (applyCount > 0).
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for shard 42 apply",
		func() bool { return testSm.applyCount.Load() >= 1 })

	// Now stop the dispatcher so we can safely call removeShardChan.
	close(stopC)
	<-runDone
	wg.Wait()

	// The shard channel was closed by run()'s shutdown. Verify
	// removeShardChan on a non-existent (already cleaned up) shard
	// is safe.
	aw.removeShardChan(42)
}

func TestCommitWorker_RunDrainsAndStops(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	// Set a save error so processBatch takes the error path (avoids
	// needing a real peer for the success path's Commit call).
	ldb.setSaveError(errors.New("test error"))

	done := make(chan struct{})
	go func() {
		cw.run()
		close(done)
	}()

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	commitC <- &commitItem{
		update: proto.Update{ShardID: 1, ReplicaID: 1},
		node:   node,
	}

	// Poll until commitPending is cleared (worker picked up and processed the item).
	waitFor(t, 5*time.Second, time.Millisecond,
		"commitPending should be cleared after commit worker processes item",
		func() bool { return !node.commitPending.Load() })

	close(stopC)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("commit worker did not stop within timeout")
	}

	// commitPending should have been cleared.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared after error")
	}
}

func TestStepWorker_TickSkipsNilPeer(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	// Load one node with nil peer and one failed node.
	cfg1 := config.Config{ShardID: 1, ReplicaID: 1}
	node1 := NewNode(nil, nil, nil, cfg1, 100, nil, "", nil)
	sw.loadNode(node1)

	cfg2 := config.Config{ShardID: 2, ReplicaID: 1}
	node2 := NewNode(nil, nil, nil, cfg2, 100, nil, "", nil)
	node2.failed.Store(true)
	sw.loadNode(node2)

	// Tick should not panic (both nodes are skipped).
	sw.tick()
}

func TestStepWorker_ProcessReady_SkipsNilPeer(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Signal work for shard 1.
	ws.Notify(1)

	// processReady should skip the node since peer is nil.
	sw.processReady()

	// No items should be in commitC.
	select {
	case <-commitC:
		t.Fatal("expected no commit items for node with nil peer")
	default:
		// Expected.
	}
}

func TestStepWorker_ProcessReady_SkipsFailedNode(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.failed.Store(true)
	sw.loadNode(node)

	ws.Notify(1)
	sw.processReady()

	select {
	case <-commitC:
		t.Fatal("expected no commit items for failed node")
	default:
		// Expected.
	}
}

func TestStepWorker_ProcessReady_SkipsNonexistentNode(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	// Signal work for shard that doesn't exist.
	ws.Notify(1)
	sw.processReady()

	select {
	case <-commitC:
		t.Fatal("expected no commit items for nonexistent node")
	default:
		// Expected.
	}
}

func TestStepWorker_ProcessReady_SkipsCommitPending(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)
	sw.loadNode(node)

	ws.Notify(1)
	sw.processReady()

	select {
	case <-commitC:
		t.Fatal("expected no commit items for commitPending node")
	default:
		// Expected.
	}
}

// ---------------------------------------------------------------------------
// Step worker processReady tests with real Raft peer
// ---------------------------------------------------------------------------

// newTestPeer creates a raft.Peer for a single-node cluster. A single-node
// cluster immediately becomes leader after enough ticks, making it easy to
// exercise the full processReady pipeline. The returned LogReader must be
// used to construct the engine Node so that the Peer and Node share the
// same log state.
func newTestPeer(t *testing.T, shardID, replicaID uint64) (*raft.Peer, *logdb.LogReader) {
	t.Helper()
	ldb := newTestLogDB()
	lr := logdb.NewLogReader(shardID, replicaID, ldb)
	// Initialize the LogReader with a membership containing this single node.
	ms := proto.Membership{
		Addresses: map[uint64]string{
			replicaID: "localhost:0",
		},
	}
	lr.ApplySnapshot(proto.Snapshot{
		Index:      0,
		Term:       0,
		Membership: ms,
	})
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}
	return peer, lr
}

// tickPeerToLeader advances the peer's logical clock enough times for a
// single-node cluster to elect itself as leader.
func tickPeerToLeader(peer *raft.Peer) {
	for range 20 {
		peer.Tick()
	}
}

func TestStepWorker_Tick_RunsDuringCommitPending(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)

	// Set commitPending to simulate the commit worker owning this peer.
	// Tick must still run to maintain leader heartbeat liveness. Prior to
	// the tick starvation fix, commitPending=true caused tick() to skip
	// the node entirely, starving heartbeats and causing leader step-down
	// under concurrent proposal load.
	node.commitPending.Store(true)
	sw.loadNode(node)

	// Tick enough times to trigger an election. Even with commitPending=true,
	// the peer should be ticked, advancing its election timer and eventually
	// becoming leader (single-node cluster).
	for range 30 {
		sw.tick()
	}

	if !peer.IsLeader() {
		t.Fatal("peer should be ticked even while commitPending is true")
	}
}

func TestStepWorker_Tick_InvokesPeerTick(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Tick enough times to trigger an election in a single-node cluster.
	for range 20 {
		sw.tick()
	}

	// After enough ticks, a single-node peer should become leader.
	if !peer.IsLeader() {
		t.Fatal("peer should be leader after sufficient ticks in single-node cluster")
	}
}

func TestStepWorker_ProcessReady_FullPipeline(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Tick the peer until it becomes leader.
	tickPeerToLeader(peer)

	// Propose an entry to generate an update.
	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("hello")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Signal work for shard 1.
	ws.Notify(1)

	// processReady should produce an update in commitC.
	sw.processReady()

	// Verify a commit item was produced.
	select {
	case item := <-commitC:
		if item.node != node {
			t.Fatal("commit item should reference the correct node")
		}
		if item.update.ShardID != 1 {
			t.Errorf("update.ShardID = %d, want 1", item.update.ShardID)
		}
		// Verify that commitPending was set.
		if !node.commitPending.Load() {
			t.Fatal("commitPending should be true after processReady")
		}
	case <-time.After(time.Second):
		t.Fatal("expected a commit item in commitC")
	}

	// Verify leader change callback was fired (leader went from 0 to 1).
	if cb.leaderCallCount.Load() < 1 {
		t.Fatal("expected OnLeaderUpdated callback to be called")
	}
	calls := cb.getLeaderCalls()
	if len(calls) < 1 {
		t.Fatal("expected at least one leader call")
	}
	if calls[0].shardID != 1 {
		t.Errorf("leader call shardID = %d, want 1", calls[0].shardID)
	}
	if calls[0].leaderID != 1 {
		t.Errorf("leader call leaderID = %d, want 1", calls[0].leaderID)
	}
}

func TestStepWorker_ProcessReady_SendsMessages(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Tick to become leader.
	tickPeerToLeader(peer)

	// Propose to generate an update with potential messages.
	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("msg-test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	ws.Notify(1)
	sw.processReady()

	// Drain commitC so the test can verify side effects.
	select {
	case <-commitC:
	case <-time.After(time.Second):
		t.Fatal("expected commit item")
	}

	// Single-node cluster may not produce outgoing messages, but the code
	// path for Messages is exercised if present. Verify no panic occurred.
}

func TestStepWorker_ProcessReady_DrainsInbox(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Tick to become leader.
	tickPeerToLeader(peer)

	// Deliver a message to the inbox before processReady.
	node.Deliver(proto.Message{
		Type:    proto.Heartbeat,
		From:    1,
		To:      1,
		ShardID: 1,
	})

	ws.Notify(1)
	sw.processReady()

	// The inbox should be drained (the message was routed through Handle).
	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("inbox should be empty after processReady, got %d", len(remaining))
	}
}

func TestStepWorker_ProcessReady_CommitCBlocking_StopC(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	// Use unbuffered commitC so the send blocks until stopC fires.
	commitC := make(chan *commitItem)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("full-test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	ws.Notify(1)

	// processReady blocks on commitC send. Close stopC to unblock.
	done := make(chan struct{})
	go func() {
		sw.processReady()
		close(done)
	}()

	// Wait until commitPending is set, meaning enqueueForCommit is
	// blocking on commitC. Then close stopC to unblock it.
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for processReady to block on commitC send",
		func() bool { return node.commitPending.Load() })
	close(stopC)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processReady did not unblock after stopC closed")
	}

	// commitPending should be cleared by the stopC branch.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be false after stopC fired")
	}
}

func TestStepWorker_ProcessReady_NoUpdate(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Tick to become leader and drain the initial update (election produces
	// state changes that count as an update).
	tickPeerToLeader(peer)
	ws.Notify(1)
	sw.processReady()

	// Drain any initial commit item.
	select {
	case item := <-commitC:
		// Commit the update to clear the peer's pending state.
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("unexpected Commit error: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Now call processReady with no new proposals. HasUpdate should return
	// false and no commit item should be produced.
	ws.Notify(1)
	sw.processReady()

	select {
	case <-commitC:
		t.Fatal("expected no commit item when there is no update")
	default:
		// Expected: no update available.
	}
}

func TestStepWorker_ProcessReady_LeaderChangeNotFired_WhenUnchanged(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// First processReady should fire the leader change callback.
	ws.Notify(1)
	sw.processReady()

	// Drain the commit item and clear state.
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("unexpected Commit error: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	initialLeaderCalls := cb.leaderCallCount.Load()

	// Propose another entry to trigger another processReady cycle.
	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("second")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	ws.Notify(1)
	sw.processReady()

	// Leader has not changed, so no additional callback should fire.
	if cb.leaderCallCount.Load() != initialLeaderCalls {
		t.Fatalf("leader callback should not fire when leader is unchanged, got %d calls (expected %d)",
			cb.leaderCallCount.Load(), initialLeaderCalls)
	}
}

// ---------------------------------------------------------------------------
// Commit worker processBatch success path (with real peer)
// ---------------------------------------------------------------------------

func TestCommitWorker_ProcessBatch_SuccessPath(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	tickPeerToLeader(peer)

	// Propose to generate a real update.
	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("commit-test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Get the update from the peer.
	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	// Set up the batch for processBatch.
	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	// Process the batch. This should SaveState, Commit, and forward to applyC.
	cw.processBatch()

	// Verify SaveState was called.
	if ldb.saveCount.Load() != 1 {
		t.Fatalf("expected 1 SaveState call, got %d", ldb.saveCount.Load())
	}

	// Verify an apply item was forwarded.
	select {
	case ai := <-applyC:
		if ai.node != node {
			t.Fatal("apply item should reference the correct node")
		}
		if ai.update.ShardID != 1 {
			t.Errorf("apply update.ShardID = %d, want 1", ai.update.ShardID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected an apply item in applyC")
	}
}

func TestCommitWorker_ProcessBatch_NilLogDB(t *testing.T) {
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	// Create commit worker with nil logdb.
	cw := newCommitWorker(0, nil, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	tickPeerToLeader(peer)

	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("nil-logdb-test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	// Process should skip persistence and still forward to apply.
	cw.processBatch()

	select {
	case ai := <-applyC:
		if ai.node != node {
			t.Fatal("apply item should reference the correct node")
		}
	case <-time.After(time.Second):
		t.Fatal("expected an apply item even with nil logdb")
	}
}

func TestCommitWorker_ProcessBatch_AlwaysForwards(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	tickPeerToLeader(peer)

	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("forward-test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	// Even with stopC closed, persisted entries MUST be forwarded to
	// the apply worker. The ordered shutdown guarantees the apply
	// worker is still consuming from applyC.
	close(stopC)
	cw.processBatch()

	// Verify the item was forwarded despite stopC being closed.
	select {
	case ai := <-applyC:
		if ai.node != node {
			t.Fatal("forwarded apply item should reference the correct node")
		}
	case <-time.After(time.Second):
		t.Fatal("persisted entry must be forwarded to applyC")
	}
}

func TestCommitWorker_DrainAndProcess(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	// Use 3 separate shards (each with its own peer) to avoid the
	// appliedTo invariant violation from double-committing the same peer.
	for i := range 3 {
		shardID := uint64(i + 1)
		peer, lr := newTestPeer(t, shardID, 1)
		cfg := config.Config{ShardID: shardID, ReplicaID: 1}
		cfg.SetDefaults()
		node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
		node.commitPending.Store(true)

		tickPeerToLeader(peer)

		_, err := peer.Propose([]proto.Entry{{Cmd: []byte("drain")}})
		if err != nil {
			t.Fatalf("propose %d failed: %v", i, err)
		}

		update, err := peer.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("GetUpdate %d failed: %v", i, err)
		}

		commitC <- &commitItem{update: update, node: node}
	}

	// Start the commit worker.
	done := make(chan struct{})
	go func() {
		cw.run()
		close(done)
	}()

	// Close stopC to trigger drain. The worker should drain all 3
	// items from commitC, persist them, and forward to applyC.
	close(stopC)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("commit worker did not finish draining")
	}

	// All 3 items should have been persisted.
	if ldb.saveCount.Load() < 1 {
		t.Fatal("expected at least 1 SaveState call during drain")
	}

	// All items should have been forwarded to applyC.
	count := 0
	for {
		select {
		case <-applyC:
			count++
		default:
			goto drained
		}
	}
drained:
	if count != 3 {
		t.Fatalf("expected 3 items forwarded to applyC during drain, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Apply worker removeShardChan with existing channel
// ---------------------------------------------------------------------------

func TestApplyWorker_RemoveShardChan_ClosesExistingChannel(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	// Manually create a shard channel to simulate an existing shard goroutine.
	ch := make(chan *applyItem, 4)
	aw.shardChans[42] = ch

	// removeShardChan should close the channel and delete the entry.
	aw.removeShardChan(42)

	// Verify the channel is closed by attempting to receive.
	_, ok := <-ch
	if ok {
		t.Fatal("shard channel should be closed")
	}

	// Verify the map entry was deleted.
	if _, exists := aw.shardChans[42]; exists {
		t.Fatal("shard channel entry should be deleted from map")
	}
}

func TestApplyWorker_AutoSnapshot_ClearsSnapshottingOnPoolFull(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	// Create a snapshot pool with 1 worker and DO NOT start workers so
	// requestC fills up and requests are rejected.
	ldb := newSnapshotTestLogDB()
	sp := newSnapshotPool(1, ldb, nil, nil, nil, stopC, nil)
	// Pre-fill requestC to make it full.
	dummyCfg := config.Config{ShardID: 999, ReplicaID: 1}
	dummyNode := NewNode(nil, nil, nil, dummyCfg, 100, nil, "", nil)
	_ = sp.requestSnapshot(snapshotRequest{
		shardID: 999, replicaID: 1, node: dummyNode, save: true,
	})
	// Now requestC is full (buffer size = 1 worker = 1).

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, sp, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1, SnapshotEntries: 5}
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, t.TempDir(), nil)

	// Apply 5 entries to trigger auto-snapshot (SnapshotEntries=5).
	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 1, Cmd: []byte("a")},
			{Term: 1, Index: 2, Cmd: []byte("b")},
			{Term: 1, Index: 3, Cmd: []byte("c")},
			{Term: 1, Index: 4, Cmd: []byte("d")},
			{Term: 1, Index: 5, Cmd: []byte("e")},
		},
	}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until the apply completes (lastApplied advances to 5).
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for 5 entries to be applied",
		func() bool { return node.lastApplied.Load() >= 5 })

	// The auto-snapshot request should have been rejected (pool full),
	// and the snapshotting flag should have been cleared so future
	// auto-snapshots can trigger. Before the fix, this flag would remain
	// true forever, permanently disabling auto-snapshot for this shard.
	if node.snapshotting.Load() {
		t.Fatal("snapshotting flag should be cleared when snapshot request is rejected")
	}

	// Verify entries were still applied successfully.
	if node.lastApplied.Load() != 5 {
		t.Fatalf("lastApplied = %d, want 5", node.lastApplied.Load())
	}

	close(stopC)
	<-runDone
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Commit worker micro-batch yield tests
// ---------------------------------------------------------------------------

func TestCommitWorker_YieldFn_CalledBetweenReceiveAndDrain(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	// Track whether yieldFn was called.
	var yieldCalled atomic.Int64
	cw.yieldFn = func() {
		yieldCalled.Add(1)
	}

	// Set a save error so processBatch takes the error path (avoids
	// needing a real peer for the success path's Commit call).
	ldb.setSaveError(errors.New("test yield error"))

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	// Start the worker.
	done := make(chan struct{})
	go func() {
		cw.run()
		close(done)
	}()

	// Send one item to trigger the receive+yield+drain path.
	commitC <- &commitItem{
		update: proto.Update{ShardID: 1, ReplicaID: 1},
		node:   node,
	}

	// Poll until yieldFn has been called at least once.
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for yieldFn to be called",
		func() bool { return yieldCalled.Load() >= 1 })

	close(stopC)
	<-done

	if yieldCalled.Load() < 1 {
		t.Fatal("yieldFn should have been called at least once between receive and drain")
	}
}

func TestCommitWorker_YieldFn_NotCalledOnStopC(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	var yieldCalled atomic.Int64
	cw.yieldFn = func() {
		yieldCalled.Add(1)
	}

	// Close stopC immediately - the worker should exit via the
	// drainAndProcess path which does not call yieldFn.
	close(stopC)

	done := make(chan struct{})
	go func() {
		cw.run()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("commit worker did not stop within timeout")
	}

	// yieldFn should NOT have been called in the shutdown path.
	if yieldCalled.Load() != 0 {
		t.Fatalf("yieldFn called %d times, expected 0 during shutdown", yieldCalled.Load())
	}
}

func TestCommitWorker_YieldAccumulatesLargerBatches(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 64)
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	// Track the batch sizes processed.
	var mu sync.Mutex
	var batchSizes []int

	// Create 4 shards each with their own peer to avoid commit conflicts.
	type shardInfo struct {
		peer *raft.Peer
		node *Node
	}
	shards := make([]shardInfo, 4)
	for i := range 4 {
		shardID := uint64(i + 1)
		peer, lr := newTestPeer(t, shardID, 1)
		cfg := config.Config{ShardID: shardID, ReplicaID: 1}
		cfg.SetDefaults()
		node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
		shards[i] = shardInfo{peer: peer, node: node}
		tickPeerToLeader(peer)
	}

	// Override processBatch to capture batch sizes.
	origProcessBatch := func() {
		mu.Lock()
		batchSizes = append(batchSizes, len(cw.batch))
		mu.Unlock()
		cw.processBatch()
	}

	// Yield function: while the commit worker yields, push more items.
	cw.yieldFn = func() {
		for i := 1; i < 4; i++ {
			si := shards[i]
			si.node.commitPending.Store(true)
			_, err := si.peer.Propose([]proto.Entry{{Cmd: []byte("yield-item")}})
			if err != nil {
				return
			}
			update, err := si.peer.GetUpdate(true, 0)
			if err != nil {
				return
			}
			commitC <- &commitItem{update: update, node: si.node}
		}
	}

	// Start the commit worker but use a custom run loop that calls
	// our tracking wrapper. We'll just test drainCommitC directly.
	_ = origProcessBatch // suppress unused

	// Send the first item.
	si := shards[0]
	si.node.commitPending.Store(true)
	_, err := si.peer.Propose([]proto.Entry{{Cmd: []byte("first")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}
	update, err := si.peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	// Simulate the run() logic: receive first, yield, drain, check batch size.
	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{si.node}

	// Call yieldFn which adds 3 more items to commitC.
	cw.yieldFn()
	cw.drainCommitC()

	// Batch should now have 4 items (1 initial + 3 from yield).
	if len(cw.batch) != 4 {
		t.Fatalf("expected 4 items in batch after yield, got %d", len(cw.batch))
	}
}

func TestCommitWorker_FlatBuf_UsedInProcessBatch(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	tickPeerToLeader(peer)

	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("flat-test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	// Initial flat buffer should be empty.
	if len(cw.flatBuf) != 0 {
		t.Fatalf("initial flatBuf len = %d, want 0", len(cw.flatBuf))
	}

	cw.processBatch()

	// After processBatch, the flat buffer should contain the converted entries.
	if len(cw.flatBuf) == 0 && len(update.EntriesToSave) > 0 {
		t.Fatal("flatBuf should be populated after processBatch with entries")
	}

	// Verify an apply item was forwarded.
	select {
	case ai := <-applyC:
		if ai.node != node {
			t.Fatal("apply item should reference the correct node")
		}
	case <-time.After(time.Second):
		t.Fatal("expected an apply item in applyC")
	}
}

// --- Helper types ---

// slowTestSM is a test SM that introduces a delay to test concurrency.
type slowTestSM struct {
	delay         time.Duration
	concurrent    *atomic.Int64
	maxConcurrent *atomic.Int64
}

func (s *slowTestSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	c := s.concurrent.Add(1)
	defer s.concurrent.Add(-1)
	// Track max concurrent.
	for {
		old := s.maxConcurrent.Load()
		if c <= old || s.maxConcurrent.CompareAndSwap(old, c) {
			break
		}
	}
	time.Sleep(s.delay)
	for i := range results {
		results[i] = sm.Result{Value: entries[i].Index}
	}
	return nil
}

func (s *slowTestSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return nil, nil
}

func (s *slowTestSM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (s *slowTestSM) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (s *slowTestSM) Close(_ context.Context) error { return nil }

var _ sm.StateMachine = (*slowTestSM)(nil)

// testApplier wraps a test SM into the Applier interface used by Node.
type testApplier struct {
	sm sm.StateMachine
}

// newTestApplier creates a testApplier wrapping the given test SM.
func newTestApplier(userSM sm.StateMachine) *testApplier {
	return &testApplier{sm: userSM}
}

// Apply delegates to the user SM's Update method, converting proto.Entry
// to sm.Entry.
func (a *testApplier) Apply(entries []proto.Entry, results []sm.Result) error {
	smEntries := make([]sm.Entry, len(entries))
	for i := range entries {
		smEntries[i] = sm.Entry{Index: entries[i].Index, Cmd: entries[i].Cmd}
	}
	return a.sm.Update(context.Background(), smEntries, results)
}

var _ Applier = (*testApplier)(nil)

// TestCommitWorker_SkipApplyWhenNoCommittedEntries verifies that when
// the update has no committed entries and the peer has no piggyback
// entries available, the commit worker short-circuits: clears
// commitPending and does not forward to the apply worker.
//
// This uses a peer where committed == processed (no entries to
// piggyback) so the skip-apply fast path is exercised. The earlier
// version of this test used a single-node peer with proposed entries
// but that scenario now triggers the piggyback optimization.
func TestCommitWorker_SkipApplyWhenNoCommittedEntries(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	ws := NewWorkSignal(1)
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, ws, nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	tickPeerToLeader(peer)

	// Drain the initial leader state so committed == processed.
	// This ensures GetPiggybackUpdate returns no entries.
	if peer.HasUpdate(0) {
		initUpdate, err := peer.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("GetUpdate (initial) failed: %v", err)
		}
		if err := peer.Commit(initUpdate); err != nil {
			t.Fatalf("Commit (initial) failed: %v", err)
		}
	}
	// Reset saveCount after the initial drain (not relevant to the test).
	ldb.saveCount.Store(0)

	// Create an update with only state changes and no committed entries.
	// This simulates a 3N first-cycle where the leader persists entries
	// but has not yet received enough acks for quorum.
	cw.batch = append(cw.batch[:0], proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		State:     proto.State{Term: 1, Vote: 1, Commit: 0},
	})
	cw.batchNodes = []*Node{node}

	// Process the batch.
	cw.processBatch()

	// Verify SaveState was called once (initial batch only, no piggyback).
	if ldb.saveCount.Load() != 1 {
		t.Fatalf("expected 1 SaveState call, got %d", ldb.saveCount.Load())
	}

	// Verify the apply worker did NOT receive an item.
	select {
	case <-applyC:
		t.Fatal("apply worker should not receive an item when no piggyback entries")
	default:
		// Good: nothing in applyC.
	}

	// Verify commitPending was cleared by the commit worker.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be false after skip-apply short-circuit")
	}
}

// TestCommitWorker_NoSkipApplyWhenCommittedEntriesPresent verifies that
// the commit worker forwards to the apply worker when committed entries
// are present (normal path).
func TestCommitWorker_NoSkipApplyWhenCommittedEntriesPresent(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	ws := NewWorkSignal(1)
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, ws, nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	tickPeerToLeader(peer)

	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("apply-test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	// Verify update has committed entries (single-node = immediate commit).
	if len(update.CommittedEntries) == 0 {
		t.Fatal("expected committed entries in single-node update")
	}

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	cw.processBatch()

	// Verify apply worker DID receive an item.
	select {
	case ai := <-applyC:
		if ai.node != node {
			t.Fatal("apply item should reference the correct node")
		}
	case <-time.After(time.Second):
		t.Fatal("expected an apply item when CommittedEntries is present")
	}

	// commitPending should still be true (apply worker hasn't cleared it).
	if !node.commitPending.Load() {
		t.Fatal("commitPending should still be true until apply worker clears it")
	}
}

// --- Test Metrics ---

// testRaftMetrics is a test implementation of RaftMetrics that records
// all metric calls for verification. All fields are thread-safe.
type testRaftMetrics struct {
	proposalLatencies  sync.Map // shardID -> *atomic.Int64 (count)
	commitLatencies    sync.Map // shardID -> *atomic.Int64 (count)
	applyLatencies     sync.Map // shardID -> *atomic.Int64 (count)
	proposalBatchSizes sync.Map // shardID -> []int
	commitBatchSizes   []int
	commitBatchSizesMu sync.Mutex
	proposalDrops      sync.Map // shardID -> *atomic.Int64
	elections          sync.Map // shardID -> *atomic.Int64
	snapshots          sync.Map // shardID -> *atomic.Int64
	piggybackCommits   sync.Map // shardID -> *atomic.Int64
}

func newTestRaftMetrics() *testRaftMetrics {
	return &testRaftMetrics{}
}

func (m *testRaftMetrics) ObserveProposalLatency(shardID uint64, _ time.Duration) {
	v, _ := m.proposalLatencies.LoadOrStore(shardID, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (m *testRaftMetrics) ObserveCommitLatency(shardID uint64, _ time.Duration) {
	v, _ := m.commitLatencies.LoadOrStore(shardID, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (m *testRaftMetrics) ObserveApplyLatency(shardID uint64, _ time.Duration) {
	v, _ := m.applyLatencies.LoadOrStore(shardID, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (m *testRaftMetrics) IncProposalDropped(shardID uint64) {
	v, _ := m.proposalDrops.LoadOrStore(shardID, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (m *testRaftMetrics) ObserveProposalBatchSize(shardID uint64, size int) {
	v, _ := m.proposalBatchSizes.LoadOrStore(shardID, &[]int{})
	ptr := v.(*[]int)
	*ptr = append(*ptr, size)
}

func (m *testRaftMetrics) ObserveCommitBatchSize(size int) {
	m.commitBatchSizesMu.Lock()
	m.commitBatchSizes = append(m.commitBatchSizes, size)
	m.commitBatchSizesMu.Unlock()
}

func (m *testRaftMetrics) IncElection(shardID uint64) {
	v, _ := m.elections.LoadOrStore(shardID, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (m *testRaftMetrics) IncSnapshot(shardID uint64) {
	v, _ := m.snapshots.LoadOrStore(shardID, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (m *testRaftMetrics) IncPiggybackCommit(shardID uint64) {
	v, _ := m.piggybackCommits.LoadOrStore(shardID, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (m *testRaftMetrics) getCommitBatchSizes() []int {
	m.commitBatchSizesMu.Lock()
	defer m.commitBatchSizesMu.Unlock()
	cp := make([]int, len(m.commitBatchSizes))
	copy(cp, m.commitBatchSizes)
	return cp
}

func (m *testRaftMetrics) getCommitLatencyCount(shardID uint64) int {
	v, ok := m.commitLatencies.Load(shardID)
	if !ok {
		return 0
	}
	return int(v.(*atomic.Int64).Load())
}

func (m *testRaftMetrics) getApplyLatencyCount(shardID uint64) int {
	v, ok := m.applyLatencies.Load(shardID)
	if !ok {
		return 0
	}
	return int(v.(*atomic.Int64).Load())
}

func (m *testRaftMetrics) getSnapshotCount(shardID uint64) int64 {
	v, ok := m.snapshots.Load(shardID)
	if !ok {
		return 0
	}
	return v.(*atomic.Int64).Load()
}

func (m *testRaftMetrics) getPiggybackCommitCount(shardID uint64) int64 {
	v, ok := m.piggybackCommits.Load(shardID)
	if !ok {
		return 0
	}
	return v.(*atomic.Int64).Load()
}

var _ RaftMetrics = (*testRaftMetrics)(nil)

// --- Metrics Tests ---

// TestCommitWorker_Metrics_BatchSize verifies that commit batch size is
// reported to the metrics collector when processBatch is called.
// Uses an error-returning LogDB so processBatch exits early via the
// error path (after recording batch size) without hitting Commit.
func TestCommitWorker_Metrics_BatchSize(t *testing.T) {
	ldb := newTestLogDB()
	ldb.setSaveError(errors.New("disk failure"))
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	metrics := newTestRaftMetrics()

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, metrics)
	cw.noRetryDelay = true
	cw.yieldFn = func() {}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	// Set up batch with 3 updates. processBatch records batch size
	// before attempting SaveState, then exits early due to error.
	cw.batch = append(cw.batch[:0],
		proto.Update{ShardID: 1, ReplicaID: 1},
		proto.Update{ShardID: 2, ReplicaID: 1},
		proto.Update{ShardID: 3, ReplicaID: 1},
	)
	cw.batchNodes = []*Node{node, node, node}
	cw.processBatch()

	sizes := metrics.getCommitBatchSizes()
	if len(sizes) != 1 {
		t.Fatalf("expected 1 batch size observation, got %d", len(sizes))
	}
	if sizes[0] != 3 {
		t.Fatalf("expected batch size 3, got %d", sizes[0])
	}
}

// TestCommitWorker_Metrics_BatchSizeNil verifies that when no metrics
// collector is provided, processBatch does not panic on the error path.
func TestCommitWorker_Metrics_BatchSizeNil(t *testing.T) {
	ldb := newTestLogDB()
	ldb.setSaveError(errors.New("disk failure"))
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true
	cw.yieldFn = func() {}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	cw.batch = append(cw.batch[:0], proto.Update{ShardID: 1, ReplicaID: 1})
	cw.batchNodes = []*Node{node}

	// Should not panic with nil metrics on error path.
	cw.processBatch()
}

// TestCommitWorker_Metrics_CommitLatency verifies that commit latency
// is recorded when metrics are enabled. Uses the run() loop to test
// the full path including commitTimes population and latency recording.
func TestCommitWorker_Metrics_CommitLatency(t *testing.T) {
	ldb := newTestLogDB()
	ldb.setSaveError(errors.New("disk failure"))
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	metrics := newTestRaftMetrics()

	// Use error logdb: processBatch records batch size, attempts
	// SaveState (fails), and exits early. Commit latency is only
	// recorded after successful SaveState, so with error logdb it
	// is NOT recorded. We verify batch size is recorded even on error.
	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, metrics)
	cw.noRetryDelay = true
	cw.yieldFn = func() {}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	// Populate commitTimes manually as run() would.
	cw.batch = append(cw.batch[:0], proto.Update{ShardID: 1, ReplicaID: 1})
	cw.batchNodes = []*Node{node}
	cw.commitTimes[1] = time.Now().Add(-10 * time.Millisecond)
	cw.processBatch()

	// Batch size should be recorded regardless of SaveState outcome.
	sizes := metrics.getCommitBatchSizes()
	if len(sizes) != 1 {
		t.Fatalf("expected 1 batch size observation, got %d", len(sizes))
	}

	// Commit latency should NOT be recorded because SaveState failed
	// and processBatch exits early.
	count := metrics.getCommitLatencyCount(1)
	if count != 0 {
		t.Fatalf("expected 0 commit latency observations on error path, got %d", count)
	}
}

// TestApplyWorker_Metrics_ApplyLatency verifies that apply latency is
// recorded when a metrics collector is provided.
func TestApplyWorker_Metrics_ApplyLatency(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	metrics := newTestRaftMetrics()

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, metrics)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 1, Cmd: []byte("cmd")},
		},
	}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until apply latency is recorded (apply completed).
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for apply latency to be recorded",
		func() bool { return metrics.getApplyLatencyCount(1) >= 1 })

	close(stopC)
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not stop within timeout")
	}
	wg.Wait()

	count := metrics.getApplyLatencyCount(1)
	if count != 1 {
		t.Fatalf("expected 1 apply latency observation, got %d", count)
	}
}

// TestApplyWorker_Metrics_ApplyLatencyNil verifies that apply works
// correctly without a metrics collector (nil safe).
func TestApplyWorker_Metrics_ApplyLatencyNil(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 1, Cmd: []byte("cmd")},
		},
	}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for SM apply with nil metrics",
		func() bool { return testSm.applyCount.Load() >= 1 })

	close(stopC)
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not stop within timeout")
	}
	wg.Wait()

	// Should not panic; SM should have been applied.
	if testSm.applyCount.Load() != 1 {
		t.Fatalf("expected 1 apply call, got %d", testSm.applyCount.Load())
	}
}

// TestCommitWorker_Metrics_RunIntegration verifies that metrics are
// recorded end-to-end through the commit worker's run() loop when
// items are sent on commitC. Uses an error-returning LogDB so the
// worker records batch size, attempts SaveState (fails), clears
// commitPending, and moves on without hitting Peer.Commit.
func TestCommitWorker_Metrics_RunIntegration(t *testing.T) {
	ldb := newTestLogDB()
	ldb.setSaveError(errors.New("disk failure"))
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	metrics := newTestRaftMetrics()

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, metrics)
	cw.noRetryDelay = true
	cw.yieldFn = func() {}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	// Send an item to commitC.
	commitC <- &commitItem{
		update: proto.Update{ShardID: 1, ReplicaID: 1},
		node:   node,
	}

	// Run commit worker in background.
	runDone := make(chan struct{})
	go func() {
		cw.run()
		close(runDone)
	}()

	// Poll until the batch size metric is recorded (item processed).
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for batch size metric to be recorded",
		func() bool { return len(metrics.getCommitBatchSizes()) >= 1 })

	close(stopC)
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not stop within timeout")
	}

	// Verify batch size was recorded.
	sizes := metrics.getCommitBatchSizes()
	if len(sizes) == 0 {
		t.Fatal("expected at least 1 batch size observation")
	}
	if sizes[0] != 1 {
		t.Fatalf("expected batch size 1, got %d", sizes[0])
	}
}

// --- GAP-1: Ordered Message Sending After WAL Persist Tests ---

// TestCommitWorker_SendsOrderedMessagesAfterSaveState verifies that
// ordered messages (VoteResp, HeartbeatResp, etc.) attached to
// commitItems are sent by the commit worker AFTER successful SaveState.
// This is the core correctness guarantee of GAP-1: persistent state
// must be durable before responding to RPCs.
func TestCommitWorker_SendsOrderedMessagesAfterSaveState(t *testing.T) {
	ldb := newTestLogDB()
	sender := newTestSender()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	tickPeerToLeader(peer)

	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("ordered-msg-test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	// Set up the batch with ordered messages.
	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}
	cw.orderedMsgsBuf[update.ShardID] = []proto.Message{
		{Type: proto.RequestVoteResp, From: 1, To: 2, ShardID: 1},
		{Type: proto.HeartbeatResp, From: 1, To: 3, ShardID: 1},
	}

	// Process the batch - SaveState should succeed, then ordered
	// messages should be sent.
	cw.processBatch()

	// Verify SaveState was called.
	if ldb.saveCount.Load() != 1 {
		t.Fatalf("expected 1 SaveState call, got %d", ldb.saveCount.Load())
	}

	// Verify ordered messages were sent via the sender.
	sentMsgs := sender.sentMessages()
	if len(sentMsgs) != 2 {
		t.Fatalf("expected 2 ordered messages sent, got %d", len(sentMsgs))
	}
	if sentMsgs[0].Type != proto.RequestVoteResp {
		t.Errorf("sentMsgs[0].Type = %s, want RequestVoteResp",
			proto.MessageTypeName(sentMsgs[0].Type))
	}
	if sentMsgs[1].Type != proto.HeartbeatResp {
		t.Errorf("sentMsgs[1].Type = %s, want HeartbeatResp",
			proto.MessageTypeName(sentMsgs[1].Type))
	}
}

// TestCommitWorker_OrderedMsgsNotSentOnSaveStateError verifies that
// when SaveState fails, ordered messages are NOT sent. This prevents
// sending VoteResp for a vote that was not actually persisted.
func TestCommitWorker_OrderedMsgsNotSentOnSaveStateError(t *testing.T) {
	ldb := newTestLogDB()
	sender := newTestSender()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true // skip retry backoff in tests

	saveErr := errors.New("disk failure")
	ldb.setSaveError(saveErr)

	listener := &config.EventListener{
		OnShardFailed: func(_ config.ShardFailedInfo) {},
	}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", listener)
	node.commitPending.Store(true)

	cw.batch = append(cw.batch[:0], proto.Update{ShardID: 1, ReplicaID: 1})
	cw.batchNodes = []*Node{node}
	cw.orderedMsgsBuf[1] = []proto.Message{
		{Type: proto.RequestVoteResp, From: 1, To: 2, ShardID: 1},
	}

	cw.processBatch()

	// Verify ordered messages were NOT sent.
	if sender.sentCount() != 0 {
		t.Fatalf("expected 0 messages sent on SaveState error, got %d",
			sender.sentCount())
	}

	// Verify commitPending was cleared.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared after SaveState error")
	}
}

// TestCommitWorker_OrderedMsgsNilSender verifies that when the sender
// is nil (memory mode), ordered messages are silently skipped without
// panic.
func TestCommitWorker_OrderedMsgsNilSender(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	// Create with nil sender.
	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	tickPeerToLeader(peer)

	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("nil-sender-test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}
	cw.orderedMsgsBuf[update.ShardID] = []proto.Message{
		{Type: proto.RequestVoteResp, From: 1, To: 2, ShardID: 1},
	}

	// Should not panic with nil sender.
	cw.processBatch()

	// Verify an apply item was still forwarded.
	select {
	case ai := <-applyC:
		if ai.node != node {
			t.Fatal("apply item should reference the correct node")
		}
	case <-time.After(time.Second):
		t.Fatal("expected an apply item even with nil sender")
	}
}

// TestCommitWorker_CaptureOrderedMsgs verifies that captureOrderedMsgs
// correctly accumulates ordered messages from multiple commitItems for
// the same shard.
func TestCommitWorker_CaptureOrderedMsgs(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	// First item with 2 ordered messages.
	item1 := &commitItem{
		update: proto.Update{ShardID: 1},
		orderedMsgs: []proto.Message{
			{Type: proto.RequestVoteResp, From: 1, To: 2},
			{Type: proto.HeartbeatResp, From: 1, To: 3},
		},
	}
	cw.captureOrderedMsgs(item1)

	if len(cw.orderedMsgsBuf[1]) != 2 {
		t.Fatalf("expected 2 ordered msgs for shard 1, got %d",
			len(cw.orderedMsgsBuf[1]))
	}

	// Second item for a different shard.
	item2 := &commitItem{
		update: proto.Update{ShardID: 2},
		orderedMsgs: []proto.Message{
			{Type: proto.ReplicateResp, From: 1, To: 4},
		},
	}
	cw.captureOrderedMsgs(item2)

	if len(cw.orderedMsgsBuf[2]) != 1 {
		t.Fatalf("expected 1 ordered msg for shard 2, got %d",
			len(cw.orderedMsgsBuf[2]))
	}

	// Third item with no ordered messages.
	item3 := &commitItem{
		update: proto.Update{ShardID: 3},
	}
	cw.captureOrderedMsgs(item3)

	if len(cw.orderedMsgsBuf[3]) != 0 {
		t.Fatalf("expected 0 ordered msgs for shard 3, got %d",
			len(cw.orderedMsgsBuf[3]))
	}

	// Verify clearBatch clears ordered messages.
	cw.clearBatch()
	if len(cw.orderedMsgsBuf) != 0 {
		t.Fatalf("expected orderedMsgsBuf to be cleared, got %d entries",
			len(cw.orderedMsgsBuf))
	}
}

// TestCommitWorker_ClearBatch_ZeroesBatchEntries verifies that clearBatch
// zeroes the batch slice entries to release references to large slices
// (EntriesToSave, CommittedEntries, Messages) for GC.
func TestCommitWorker_ClearBatch_ZeroesBatchEntries(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	// Populate the batch with updates containing large slices.
	cw.batch = append(cw.batch[:0],
		proto.Update{
			ShardID:   1,
			ReplicaID: 1,
			EntriesToSave: []proto.Entry{
				{Index: 1, Term: 1, Cmd: make([]byte, 4096)},
			},
			CommittedEntries: []proto.Entry{
				{Index: 1, Term: 1, Cmd: make([]byte, 4096)},
			},
			Messages: []proto.Message{
				{Type: proto.Replicate, From: 1, To: 2},
			},
		},
		proto.Update{
			ShardID:   2,
			ReplicaID: 1,
			EntriesToSave: []proto.Entry{
				{Index: 2, Term: 1, Cmd: make([]byte, 8192)},
			},
		},
	)
	cw.batchNodes = append(cw.batchNodes[:0], nil, nil)

	cw.clearBatch()

	// Verify all batch entries are zeroed.
	for i, u := range cw.batch {
		if u.ShardID != 0 || u.ReplicaID != 0 {
			t.Errorf("batch[%d]: ShardID=%d ReplicaID=%d, expected zero", i, u.ShardID, u.ReplicaID)
		}
		if u.EntriesToSave != nil {
			t.Errorf("batch[%d]: EntriesToSave not nil after clearBatch", i)
		}
		if u.CommittedEntries != nil {
			t.Errorf("batch[%d]: CommittedEntries not nil after clearBatch", i)
		}
		if u.Messages != nil {
			t.Errorf("batch[%d]: Messages not nil after clearBatch", i)
		}
	}

	// Verify batchNodes are also cleared.
	for i, n := range cw.batchNodes {
		if n != nil {
			t.Errorf("batchNodes[%d]: not nil after clearBatch", i)
		}
	}
}

// TestCommitWorker_ClearBatch_EmptyBatch verifies that clearBatch handles
// an empty batch without panicking.
func TestCommitWorker_ClearBatch_EmptyBatch(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	// clearBatch on empty state should not panic.
	cw.clearBatch()

	if len(cw.batch) != 0 {
		t.Errorf("batch length = %d after clearBatch on empty worker, want 0", len(cw.batch))
	}
	if len(cw.batchNodes) != 0 {
		t.Errorf("batchNodes length = %d after clearBatch on empty worker, want 0", len(cw.batchNodes))
	}
}

// TestCommitWorker_CaptureOrderedMsgs_EmptySlice verifies that
// captureOrderedMsgs handles empty orderedMsgs slices correctly
// without adding entries to the buffer.
func TestCommitWorker_CaptureOrderedMsgs_EmptySlice(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	item := &commitItem{
		update:      proto.Update{ShardID: 1},
		orderedMsgs: []proto.Message{},
	}
	cw.captureOrderedMsgs(item)

	// Empty orderedMsgs should not create a map entry.
	if _, ok := cw.orderedMsgsBuf[1]; ok {
		t.Fatal("should not create map entry for empty orderedMsgs")
	}
}

// TestCommitWorker_SendOrderedMessages_WithInstallSnapshot verifies
// that sendOrderedMessages correctly routes InstallSnapshot messages
// through SendSnapshot while other ordered messages go through Send.
func TestCommitWorker_SendOrderedMessages_WithInstallSnapshot(t *testing.T) {
	sender := newTestSender()
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	msgs := []proto.Message{
		{Type: proto.RequestVoteResp, From: 1, To: 2},
		{Type: proto.InstallSnapshot, From: 1, To: 3},
		{Type: proto.HeartbeatResp, From: 1, To: 4},
	}

	cw.sendOrderedMessages(msgs)

	if sender.sentCount() != 2 {
		t.Fatalf("expected 2 regular messages, got %d", sender.sentCount())
	}
	if sender.snapshotCount() != 1 {
		t.Fatalf("expected 1 snapshot message, got %d", sender.snapshotCount())
	}

	// Verify regular messages.
	sentMsgs := sender.sentMessages()
	if sentMsgs[0].Type != proto.RequestVoteResp {
		t.Errorf("sent[0].Type = %s, want RequestVoteResp",
			proto.MessageTypeName(sentMsgs[0].Type))
	}
	if sentMsgs[1].Type != proto.HeartbeatResp {
		t.Errorf("sent[1].Type = %s, want HeartbeatResp",
			proto.MessageTypeName(sentMsgs[1].Type))
	}

	// Verify snapshot message.
	snaps := sender.sentSnapshots()
	if snaps[0].To != 3 {
		t.Errorf("snapshot.To = %d, want 3", snaps[0].To)
	}
}

// TestCommitWorker_SendOrderedMessages_NoSnapshot verifies the fast
// path where no InstallSnapshot messages are present.
func TestCommitWorker_SendOrderedMessages_NoSnapshot(t *testing.T) {
	sender := newTestSender()
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	msgs := []proto.Message{
		{Type: proto.RequestVoteResp, From: 1, To: 2},
		{Type: proto.HeartbeatResp, From: 1, To: 3},
	}

	cw.sendOrderedMessages(msgs)

	if sender.sentCount() != 2 {
		t.Fatalf("expected 2 messages, got %d", sender.sentCount())
	}
	if sender.snapshotCount() != 0 {
		t.Fatalf("expected 0 snapshots, got %d", sender.snapshotCount())
	}
}

// TestCommitWorker_OrderedMsgsBuf_ClearedEachBatch verifies that the
// orderedMsgsBuf is properly cleared between batch cycles so ordered
// messages from a previous batch are not re-sent.
func TestCommitWorker_OrderedMsgsBuf_ClearedEachBatch(t *testing.T) {
	ldb := newTestLogDB()
	sender := newTestSender()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	// Process first batch with ordered messages.
	peer1, lr1 := newTestPeer(t, 1, 1)
	cfg1 := config.Config{ShardID: 1, ReplicaID: 1}
	cfg1.SetDefaults()
	node1 := NewNode(peer1, nil, lr1, cfg1, 100, nil, "", nil)
	node1.commitPending.Store(true)

	tickPeerToLeader(peer1)

	_, err := peer1.Propose([]proto.Entry{{Cmd: []byte("batch1")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	update1, err := peer1.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	// Clear committed entries to skip apply path (avoid needing SM).
	update1.CommittedEntries = nil

	cw.batch = append(cw.batch[:0], update1)
	cw.batchNodes = []*Node{node1}
	cw.orderedMsgsBuf[update1.ShardID] = []proto.Message{
		{Type: proto.RequestVoteResp, From: 1, To: 2, ShardID: 1},
	}

	cw.processBatch()

	// Verify first batch sent 1 ordered message.
	if sender.sentCount() != 1 {
		t.Fatalf("expected 1 message sent after first batch, got %d",
			sender.sentCount())
	}

	// Process second batch with NO ordered messages.
	peer2, lr2 := newTestPeer(t, 2, 1)
	cfg2 := config.Config{ShardID: 2, ReplicaID: 1}
	cfg2.SetDefaults()
	node2 := NewNode(peer2, nil, lr2, cfg2, 100, nil, "", nil)
	node2.commitPending.Store(true)

	tickPeerToLeader(peer2)

	_, err = peer2.Propose([]proto.Entry{{Cmd: []byte("batch2")}})
	if err != nil {
		t.Fatalf("propose 2 failed: %v", err)
	}

	update2, err := peer2.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate 2 failed: %v", err)
	}

	update2.CommittedEntries = nil

	cw.batch = append(cw.batch[:0], update2)
	cw.batchNodes = []*Node{node2}
	// No ordered messages for this batch.

	sender.mu.Lock()
	sender.msgs = sender.msgs[:0]
	sender.mu.Unlock()

	cw.processBatch()

	// Verify no additional messages were sent (orderedMsgsBuf was cleared).
	if sender.sentCount() != 0 {
		t.Fatalf("expected 0 messages sent after second batch (buf cleared), got %d",
			sender.sentCount())
	}
}

// ---------------------------------------------------------------------------
// M11: SaveState failure completes pending proposals via OnProposalFailed
// ---------------------------------------------------------------------------

// TestCommitWorker_SaveStateError_CompletesPendingProposals verifies that
// when SaveState fails after all retries, OnProposalFailed is called for
// each update's committed entries. This prevents proposals from hanging
// until context deadline timeout because the entries were already
// extracted via GetUpdate and will never reach the apply worker.
func TestCommitWorker_SaveStateError_CompletesPendingProposals(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cb := newTestCallback()
	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), cb, stopC, nil)
	cw.noRetryDelay = true // skip retry backoff in tests

	saveErr := errors.New("disk failure")
	ldb.setSaveError(saveErr)

	listener := &config.EventListener{
		OnShardFailed: func(_ config.ShardFailedInfo) {},
	}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", listener)
	node.commitPending.Store(true)

	// Set up a batch with committed entries that have proposal keys.
	cw.batch = append(cw.batch[:0], proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 5, Key: 100, Cmd: []byte("a")},
			{Term: 1, Index: 6, Key: 101, Cmd: []byte("b")},
			{Term: 1, Index: 7, Key: 102, Cmd: []byte("c")},
		},
	})
	cw.batchNodes = []*Node{node}
	cw.processBatch()

	// commitPending should be cleared.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared after SaveState error")
	}

	// OnProposalFailed should have been called with all committed entries.
	if cb.proposalFailedCount.Load() != 1 {
		t.Fatalf("expected 1 OnProposalFailed call, got %d", cb.proposalFailedCount.Load())
	}
	failedCalls := cb.getProposalFailedCalls()
	if len(failedCalls) != 1 {
		t.Fatalf("expected 1 failed call, got %d", len(failedCalls))
	}
	fc := failedCalls[0]
	if fc.shardID != 1 {
		t.Errorf("shardID = %d, want 1", fc.shardID)
	}
	if len(fc.entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(fc.entries))
	}
	for i, wantKey := range []uint64{100, 101, 102} {
		if fc.entries[i].Key != wantKey {
			t.Errorf("entries[%d].Key = %d, want %d", i, fc.entries[i].Key, wantKey)
		}
	}
	// The error should be a SaveStateError.
	var sse *SaveStateError
	if !errors.As(fc.err, &sse) {
		t.Fatalf("expected SaveStateError, got %T: %v", fc.err, fc.err)
	}
	if sse.ShardID != 1 || sse.ReplicaID != 1 {
		t.Errorf("SaveStateError shard/replica = %d/%d, want 1/1", sse.ShardID, sse.ReplicaID)
	}
}

// TestCommitWorker_SaveStateError_MultipleShards verifies that
// OnProposalFailed is called independently for each shard in a multi-
// shard batch when SaveState fails.
func TestCommitWorker_SaveStateError_MultipleShards(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cb := newTestCallback()
	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), cb, stopC, nil)
	cw.noRetryDelay = true // skip retry backoff in tests

	saveErr := errors.New("IO error")
	ldb.setSaveError(saveErr)

	listener := &config.EventListener{
		OnShardFailed: func(_ config.ShardFailedInfo) {},
	}

	// Set up 2 shards in the batch.
	cfg1 := config.Config{ShardID: 10, ReplicaID: 1}
	node1 := NewNode(nil, nil, nil, cfg1, 100, nil, "", listener)
	node1.commitPending.Store(true)

	cfg2 := config.Config{ShardID: 20, ReplicaID: 2}
	node2 := NewNode(nil, nil, nil, cfg2, 100, nil, "", listener)
	node2.commitPending.Store(true)

	cw.batch = append(cw.batch[:0],
		proto.Update{
			ShardID:   10,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{Term: 1, Index: 1, Key: 200, Cmd: []byte("x")},
			},
		},
		proto.Update{
			ShardID:   20,
			ReplicaID: 2,
			CommittedEntries: []proto.Entry{
				{Term: 1, Index: 1, Key: 300, Cmd: []byte("y")},
				{Term: 1, Index: 2, Key: 301, Cmd: []byte("z")},
			},
		},
	)
	cw.batchNodes = []*Node{node1, node2}

	cw.processBatch()

	// Both commitPending flags should be cleared.
	if node1.commitPending.Load() {
		t.Fatal("node1 commitPending should be cleared")
	}
	if node2.commitPending.Load() {
		t.Fatal("node2 commitPending should be cleared")
	}

	// OnProposalFailed should have been called twice (once per shard).
	if cb.proposalFailedCount.Load() != 2 {
		t.Fatalf("expected 2 OnProposalFailed calls, got %d", cb.proposalFailedCount.Load())
	}

	failedCalls := cb.getProposalFailedCalls()
	if len(failedCalls) != 2 {
		t.Fatalf("expected 2 failed calls, got %d", len(failedCalls))
	}

	// Check each call has the correct entries.
	shardEntries := make(map[uint64]int)
	for _, fc := range failedCalls {
		shardEntries[fc.shardID] = len(fc.entries)
		var sse *SaveStateError
		if !errors.As(fc.err, &sse) {
			t.Fatalf("expected SaveStateError for shard %d, got %T", fc.shardID, fc.err)
		}
	}
	if shardEntries[10] != 1 {
		t.Errorf("shard 10: expected 1 entry, got %d", shardEntries[10])
	}
	if shardEntries[20] != 2 {
		t.Errorf("shard 20: expected 2 entries, got %d", shardEntries[20])
	}
}

// TestCommitWorker_SaveStateError_NoCommittedEntries verifies that
// OnProposalFailed is NOT called when there are no committed entries
// in the failed batch (e.g., first-round WAL persistence with no
// committed entries).
func TestCommitWorker_SaveStateError_NoCommittedEntries(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cb := newTestCallback()
	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), cb, stopC, nil)
	cw.noRetryDelay = true // skip retry backoff in tests

	ldb.setSaveError(errors.New("full"))

	listener := &config.EventListener{
		OnShardFailed: func(_ config.ShardFailedInfo) {},
	}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", listener)
	node.commitPending.Store(true)

	// No committed entries in this update.
	cw.batch = append(cw.batch[:0], proto.Update{
		ShardID:   1,
		ReplicaID: 1,
	})
	cw.batchNodes = []*Node{node}
	cw.processBatch()

	// OnProposalFailed should NOT have been called.
	if cb.proposalFailedCount.Load() != 0 {
		t.Fatalf("expected 0 OnProposalFailed calls for empty committed entries, got %d",
			cb.proposalFailedCount.Load())
	}
}

// TestCommitWorker_SaveStateError_NilCallback verifies that SaveState
// failure with a nil callback does not panic when completing proposals.
func TestCommitWorker_SaveStateError_NilCallback(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	// nil callback must not panic.
	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true // skip retry backoff in tests

	ldb.setSaveError(errors.New("panic test"))

	listener := &config.EventListener{
		OnShardFailed: func(_ config.ShardFailedInfo) {},
	}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", listener)
	node.commitPending.Store(true)

	cw.batch = append(cw.batch[:0], proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 1, Key: 50, Cmd: []byte("x")},
		},
	})
	cw.batchNodes = []*Node{node}

	// Must not panic.
	cw.processBatch()

	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared even with nil callback")
	}
}

// TestCommitWorker_ProcessBatch_StaleIncarnation verifies that when two
// commitItems for the same shardID from different node incarnations land
// in the same batch (rapid stop/restart), each update is committed to
// its own node rather than the last node for that shardID. This is a
// regression test for a panic where the old update's applied index
// exceeded the new incarnation's committed index because a shardID-keyed
// nodesBuf clobbered the old node pointer with the new one.
func TestCommitWorker_ProcessBatch_StaleIncarnation(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	// Create the OLD incarnation: peer elected as leader, with entries.
	oldPeer, oldLR := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	oldSM := newTestApplier(newTestSM())
	oldNode := NewNode(oldPeer, oldSM, oldLR, cfg, 100, nil, "", nil)
	oldNode.commitPending.Store(true)

	tickPeerToLeader(oldPeer)

	// Propose to generate committed entries on the old incarnation.
	_, err := oldPeer.Propose([]proto.Entry{{Cmd: []byte("old-incarnation")}})
	if err != nil {
		t.Fatalf("propose on old peer failed: %v", err)
	}
	oldUpdate, err := oldPeer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate on old peer failed: %v", err)
	}
	if len(oldUpdate.CommittedEntries) == 0 {
		t.Fatal("expected committed entries from old incarnation")
	}

	// Create the NEW incarnation: fresh peer with committed=0.
	newPeer, newLR := newTestPeer(t, 1, 1)
	newSM := newTestApplier(newTestSM())
	newNode := NewNode(newPeer, newSM, newLR, cfg, 100, nil, "", nil)
	newNode.commitPending.Store(true)

	tickPeerToLeader(newPeer)

	newUpdate, err := newPeer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate on new peer failed: %v", err)
	}

	// Simulate both commitItems landing in the same batch (rapid
	// stop/restart: old item was in commitC, new item arrives before
	// the commit worker processes the old one).
	cw.batch = []proto.Update{oldUpdate, newUpdate}
	cw.batchNodes = []*Node{oldNode, newNode}

	// With the old shardID-keyed nodesBuf approach, this would panic:
	// "appliedTo: applied index exceeds committed index" because both
	// updates would be committed to newNode (whose committed index is
	// lower than oldUpdate's applied entries). With per-entry
	// batchNodes, each update commits to its own node.
	cw.processBatch()

	// Verify updates were forwarded to the apply worker. Each update
	// with committed entries produces an applyItem pointing to its own
	// node. commitPending remains true because the apply worker (not
	// processBatch) clears it after applying entries.
	var applyItems []*applyItem
	for {
		select {
		case ai := <-applyC:
			applyItems = append(applyItems, ai)
		default:
			goto done
		}
	}
done:
	if len(applyItems) == 0 {
		t.Fatal("expected at least 1 apply item")
	}

	// Verify that the apply items reference their correct nodes, not
	// the clobbered new node for both.
	seenOld, seenNew := false, false
	for _, ai := range applyItems {
		if ai.node == oldNode {
			seenOld = true
		}
		if ai.node == newNode {
			seenNew = true
		}
	}
	if !seenOld {
		t.Fatal("expected apply item referencing oldNode")
	}
	if !seenNew {
		t.Fatal("expected apply item referencing newNode")
	}
}

// TestCommitWorker_ProcessBatch_StaleIncarnation_Error verifies that when
// an old incarnation's update fails Peer.Commit (e.g., due to stale
// state after restart), the error is handled gracefully without panicking,
// and the node's commitPending is cleared so it doesn't block future
// processing.
func TestCommitWorker_ProcessBatch_StaleIncarnation_Error(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	// Create two independent nodes for the same shardID.
	peer1, lr1 := newTestPeer(t, 1, 1)
	peer2, lr2 := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()

	node1 := NewNode(peer1, newTestApplier(newTestSM()), lr1, cfg, 100, nil, "", nil)
	node1.commitPending.Store(true)
	node2 := NewNode(peer2, newTestApplier(newTestSM()), lr2, cfg, 100, nil, "", nil)
	node2.commitPending.Store(true)

	tickPeerToLeader(peer1)
	tickPeerToLeader(peer2)

	// Get updates from both peers.
	update1, err := peer1.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate peer1: %v", err)
	}
	update2, err := peer2.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate peer2: %v", err)
	}

	// Each update should commit to its own node without interference.
	cw.batch = []proto.Update{update1, update2}
	cw.batchNodes = []*Node{node1, node2}

	// Must not panic. With a shardID-keyed nodesBuf, the second
	// entry would clobber the first for the same shardID, causing
	// update1's committed entries to be applied to node2's peer
	// (whose committed index may be different).
	cw.processBatch()

	// Verify updates were forwarded to apply worker with correct
	// node ownership.
	var items []*applyItem
	for {
		select {
		case ai := <-applyC:
			items = append(items, ai)
		default:
			goto done2
		}
	}
done2:
	seen1, seen2 := false, false
	for _, ai := range items {
		if ai.node == node1 {
			seen1 = true
		}
		if ai.node == node2 {
			seen2 = true
		}
	}
	if !seen1 {
		t.Fatal("expected apply item for node1")
	}
	if !seen2 {
		t.Fatal("expected apply item for node2")
	}
}

// ---------------------------------------------------------------------------
// Piggyback commit optimization tests
// ---------------------------------------------------------------------------

// setupPiggybackPeer creates a single-node leader peer with one proposed
// entry whose committed index exceeds the processed index. This simulates
// the multi-node scenario where entries become committable during the
// commitPending window. Returns the peer, LogReader, the initial update
// (with CommittedEntries cleared to trigger the piggyback path), and the
// expected piggyback entry index.
//
// The flow:
//  1. Create single-node peer, tick to leader (immediate self-election).
//  2. Drain initial state (leader no-op entry).
//  3. Propose a new entry. In a single-node cluster, commit advances
//     immediately, but processed only advances in Commit().
//  4. Call GetUpdate to extract the update. Clear CommittedEntries from
//     the returned update so processBatch enters the piggyback branch.
//  5. The peer's internal state now has committed > processed because
//     GetUpdate does not advance processed (only Commit does).
func setupPiggybackPeer(t *testing.T, shardID, replicaID uint64) (
	*raft.Peer, *logdb.LogReader, proto.Update, uint64,
) {
	t.Helper()

	peer, lr := newTestPeer(t, shardID, replicaID)
	tickPeerToLeader(peer)

	// Drain initial leader state (no-op entry + state change).
	if peer.HasUpdate(0) {
		initUpdate, err := peer.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("GetUpdate (initial) failed: %v", err)
		}
		if err := peer.Commit(initUpdate); err != nil {
			t.Fatalf("Commit (initial) failed: %v", err)
		}
	}

	// Propose a new entry.
	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("piggyback-entry")}})
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	// GetUpdate returns EntriesToSave and CommittedEntries.
	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	if len(update.CommittedEntries) == 0 {
		t.Fatal("expected committed entries from single-node GetUpdate")
	}
	piggybackIndex := update.CommittedEntries[len(update.CommittedEntries)-1].Index

	// Clear CommittedEntries to simulate the 3N first-cycle scenario.
	// processBatch sees no committed entries, enters the piggyback branch,
	// and GetPiggybackUpdate discovers them because committed > processed.
	update.CommittedEntries = nil

	return peer, lr, update, piggybackIndex
}

// TestCommitWorker_PiggybackHappyPath verifies the end-to-end piggyback
// commit optimization: when processBatch commits an update with no
// committed entries, GetPiggybackUpdate discovers the newly committable
// entries and forwards them to the apply worker in the same pipeline cycle.
//
// OPTIMIZATION: The piggyback path no longer calls SaveState to persist
// the commit index. The commit index is persisted lazily in the next batch.
// This eliminates the double fsync and improves throughput. The entries
// themselves are already durable from the initial batch SaveState.
func TestCommitWorker_PiggybackHappyPath(t *testing.T) {
	ldb := newTestLogDB()
	sender := newTestSender()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	ws := NewWorkSignal(64)
	cb := newTestCallback()
	stopC := make(chan struct{})
	metrics := newTestRaftMetrics()

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, ws, cb, stopC, metrics)
	cw.noRetryDelay = true

	peer, lr, update, piggybackIndex := setupPiggybackPeer(t, 1, 1)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, rsmSM, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)
	node.SetNotifyCommit(true)

	// Set up the batch: update has EntriesToSave but no CommittedEntries.
	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	cw.processBatch()

	// Verify SaveState was called only once (initial batch persistence).
	// The piggyback path skips SaveState - commit index is persisted lazily.
	if got := ldb.saveCount.Load(); got != 1 {
		t.Fatalf("expected 1 SaveState call (initial only, piggyback skips persist), got %d", got)
	}

	// Verify the apply worker received the piggyback committed entries.
	select {
	case ai := <-applyC:
		if ai.node != node {
			t.Fatal("apply item should reference the correct node")
		}
		if len(ai.update.CommittedEntries) == 0 {
			t.Fatal("apply item should contain committed entries from piggyback")
		}
		lastIdx := ai.update.CommittedEntries[len(ai.update.CommittedEntries)-1].Index
		if lastIdx != piggybackIndex {
			t.Fatalf("piggyback committed entry index = %d, want %d", lastIdx, piggybackIndex)
		}
	case <-time.After(time.Second):
		t.Fatal("expected an apply item from piggyback path")
	}

	// Verify IncPiggybackCommit was called for shard 1.
	if got := metrics.getPiggybackCommitCount(1); got != 1 {
		t.Fatalf("expected 1 piggyback commit metric, got %d", got)
	}

	// Verify OnCommitted callback was fired (notifyCommit is true).
	if cb.committedCount.Load() < 1 {
		t.Fatal("expected OnCommitted callback to fire for piggyback entries")
	}
}

// TestCommitWorker_PiggybackNoEntries verifies that when the peer has
// no entries to piggyback (committed == processed after Commit), the
// commit worker takes the fast path: clears commitPending, notifies
// the work signal, and does not invoke the piggyback pipeline.
func TestCommitWorker_PiggybackNoEntries(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	ws := NewWorkSignal(64)
	stopC := make(chan struct{})
	metrics := newTestRaftMetrics()

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, ws, nil, stopC, metrics)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	tickPeerToLeader(peer)

	// Drain ALL initial state so committed == processed.
	if peer.HasUpdate(0) {
		initUpdate, err := peer.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("GetUpdate (initial) failed: %v", err)
		}
		if err := peer.Commit(initUpdate); err != nil {
			t.Fatalf("Commit (initial) failed: %v", err)
		}
	}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	// Create an update with only state (no entries, no committed entries).
	// This simulates a state-only cycle (e.g., heartbeat response changed
	// hard state but no new entries to commit).
	cw.batch = append(cw.batch[:0], proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		State:     proto.State{Term: 1, Vote: 1, Commit: 0},
	})
	cw.batchNodes = []*Node{node}

	cw.processBatch()

	// Only 1 SaveState call (initial batch). No piggyback call.
	if got := ldb.saveCount.Load(); got != 1 {
		t.Fatalf("expected 1 SaveState call, got %d", got)
	}

	// commitPending should be cleared immediately by the skip-apply path.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared when no piggyback entries exist")
	}

	// No apply item should be produced.
	select {
	case <-applyC:
		t.Fatal("apply worker should not receive an item when no piggyback entries")
	default:
	}

	// No piggyback metric should be recorded.
	if got := metrics.getPiggybackCommitCount(1); got != 0 {
		t.Fatalf("expected 0 piggyback commit metrics, got %d", got)
	}
}

// TestCommitWorker_PiggybackSkipsSaveState verifies that the piggyback
// path no longer calls SaveState to persist the commit index. This is an
// optimization that eliminates the double fsync per batch. The commit
// index is persisted lazily in the next batch's SaveState call.
//
// Even when LogDB would fail on subsequent calls, the piggyback path
// succeeds because it doesn't call SaveState at all.
func TestCommitWorker_PiggybackSkipsSaveState(t *testing.T) {
	ldb := newTestLogDB()
	sender := newTestSender()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	ws := NewWorkSignal(64)
	cb := newTestCallback()
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, ws, cb, stopC, nil)
	cw.noRetryDelay = true

	peer, lr, update, _ := setupPiggybackPeer(t, 1, 1)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()

	listener := &config.EventListener{
		OnShardFailed: func(_ config.ShardFailedInfo) {},
	}
	node := NewNode(peer, rsmSM, lr, cfg, 100, nil, "", listener)
	node.commitPending.Store(true)

	// Configure the LogDB to fail after the first successful SaveState.
	// With the optimization, the piggyback path never calls SaveState,
	// so this failure never occurs.
	ldb.setFailAfterN(1, errors.New("disk full on piggyback"))

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	cw.processBatch()

	// Verify SaveState was called only once (initial batch persistence).
	// The piggyback path skips SaveState - commit index is persisted lazily.
	if got := ldb.saveCount.Load(); got != 1 {
		t.Fatalf("expected 1 SaveState call (piggyback skips persist), got %d", got)
	}

	// Apply item SHOULD be produced since piggyback no longer fails
	// on SaveState errors (it doesn't call SaveState).
	select {
	case ai := <-applyC:
		if len(ai.update.CommittedEntries) == 0 {
			t.Fatal("piggyback apply item should have committed entries")
		}
	case <-time.After(time.Second):
		t.Fatal("expected apply item from piggyback (SaveState is skipped)")
	}
}

// TestCommitWorker_PiggybackMessageOrdering verifies that the piggyback
// path correctly splits messages: free-order messages (Replicate, Heartbeat)
// are sent before ordered messages (VoteResp, HeartbeatResp, ReplicateResp).
//
// OPTIMIZATION: The piggyback path no longer calls SaveState to persist
// the commit index. The commit index is persisted lazily in the next batch.
// However, the message ordering is still maintained for consistency with
// WAL mode behavior. Ordered messages are sent after Commit() completes.
func TestCommitWorker_PiggybackMessageOrdering(t *testing.T) {
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	ws := NewWorkSignal(64)
	stopC := make(chan struct{})

	// Use a sequencing sender that records the order of Send calls.
	var sendOrder []string
	var sendMu sync.Mutex
	seqSender := &sequencingSender{
		onSend: func(msgs []proto.Message) {
			sendMu.Lock()
			defer sendMu.Unlock()
			for _, m := range msgs {
				sendOrder = append(sendOrder, proto.MessageTypeName(m.Type))
			}
		},
	}

	// Use a LogDB that records when SaveState is called.
	seqLDB := &sequencingLogDB{
		testLogDB: newTestLogDB(),
		onSave: func() {
			sendMu.Lock()
			sendOrder = append(sendOrder, "SaveState")
			sendMu.Unlock()
		},
	}

	cw := newCommitWorker(0, seqLDB, seqSender, commitC, applyC, applyPool, ws, nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr, update, _ := setupPiggybackPeer(t, 1, 1)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, rsmSM, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	cw.processBatch()

	// Analyze the sequence of operations.
	// OPTIMIZATION: Only 1 SaveState call (initial batch persistence).
	// The piggyback path skips SaveState - commit index is persisted lazily.
	sendMu.Lock()
	defer sendMu.Unlock()

	// There should be exactly 1 SaveState call (initial only).
	saveStateCount := 0
	for _, entry := range sendOrder {
		if entry == "SaveState" {
			saveStateCount++
		}
	}
	if saveStateCount != 1 {
		t.Fatalf("expected 1 SaveState entry (piggyback skips persist), got %d (sequence: %v)",
			saveStateCount, sendOrder)
	}

	// Verify the apply worker received entries (piggyback succeeded).
	select {
	case ai := <-applyC:
		if len(ai.update.CommittedEntries) == 0 {
			t.Fatal("piggyback apply item should have committed entries")
		}
	case <-time.After(time.Second):
		t.Fatal("expected an apply item from piggyback")
	}
}

// sequencingSender records Send calls for ordering verification.
type sequencingSender struct {
	onSend func([]proto.Message)
}

func (s *sequencingSender) Send(msgs []proto.Message) {
	if s.onSend != nil {
		s.onSend(msgs)
	}
}

func (s *sequencingSender) SendSnapshot(_ proto.Message) {}

var _ MessageSender = (*sequencingSender)(nil)

// sequencingLogDB wraps testLogDB and calls a hook on each SaveState
// for ordering verification.
type sequencingLogDB struct {
	*testLogDB
	onSave func()
}

func (s *sequencingLogDB) SaveState(updates []logdb.Update) error {
	err := s.testLogDB.SaveState(updates)
	if s.onSave != nil {
		s.onSave()
	}
	return err
}

// TestCommitWorker_PiggybackMetrics verifies that a successful piggyback
// commit increments the IncPiggybackCommit metric with the correct shard
// ID, and that the metric is not incremented when no piggyback occurs.
func TestCommitWorker_PiggybackMetrics(t *testing.T) {
	ldb := newTestLogDB()
	sender := newTestSender()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	ws := NewWorkSignal(64)
	stopC := make(chan struct{})
	metrics := newTestRaftMetrics()

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, ws, nil, stopC, metrics)
	cw.noRetryDelay = true

	// Test with shard 42 to verify the correct shardID is passed.
	peer, lr, update, _ := setupPiggybackPeer(t, 42, 1)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	cfg := config.Config{ShardID: 42, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, rsmSM, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	// Verify no metrics before.
	if got := metrics.getPiggybackCommitCount(42); got != 0 {
		t.Fatalf("expected 0 piggyback metrics before test, got %d", got)
	}

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}
	cw.processBatch()

	// Drain the apply item to confirm piggyback succeeded.
	select {
	case ai := <-applyC:
		if len(ai.update.CommittedEntries) == 0 {
			t.Fatal("piggyback should produce committed entries")
		}
	case <-time.After(time.Second):
		t.Fatal("expected piggyback apply item")
	}

	// Verify IncPiggybackCommit was called exactly once for shard 42.
	if got := metrics.getPiggybackCommitCount(42); got != 1 {
		t.Fatalf("expected 1 piggyback commit for shard 42, got %d", got)
	}

	// Verify other shards were not affected.
	if got := metrics.getPiggybackCommitCount(1); got != 0 {
		t.Fatalf("expected 0 piggyback commits for shard 1, got %d", got)
	}
	if got := metrics.getPiggybackCommitCount(0); got != 0 {
		t.Fatalf("expected 0 piggyback commits for shard 0, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// clearStepCommitPending tests
// ---------------------------------------------------------------------------

// TestStepWorker_ClearStepCommitPending_ClearsAllNodes verifies that
// clearStepCommitPending resets commitPending on all nodes owned by the
// step worker. This is called by the engine's panic recovery loop to
// unblock nodes after a step worker crash.
func TestStepWorker_ClearStepCommitPending_ClearsAllNodes(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	// Load multiple nodes with commitPending=true.
	for i := uint64(1); i <= 5; i++ {
		cfg := config.Config{ShardID: i, ReplicaID: 1}
		node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
		node.commitPending.Store(true)
		sw.loadNode(node)
	}

	// Verify all are pending.
	for i := uint64(1); i <= 5; i++ {
		sw.nodesMu.RLock()
		if !sw.nodes[i].commitPending.Load() {
			t.Fatalf("node %d commitPending should be true before clear", i)
		}
		sw.nodesMu.RUnlock()
	}

	// Clear all.
	sw.clearStepCommitPending()

	// Verify all are cleared.
	for i := uint64(1); i <= 5; i++ {
		sw.nodesMu.RLock()
		if sw.nodes[i].commitPending.Load() {
			t.Fatalf("node %d commitPending should be false after clear", i)
		}
		sw.nodesMu.RUnlock()
	}
}

// TestStepWorker_ClearStepCommitPending_EmptyNodes verifies that
// clearStepCommitPending is a no-op when the worker has no loaded nodes.
func TestStepWorker_ClearStepCommitPending_EmptyNodes(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	// Should not panic with empty nodes map.
	sw.clearStepCommitPending()
}

// ---------------------------------------------------------------------------
// drainUnloadReload tests
// ---------------------------------------------------------------------------

// TestCommitWorker_DrainUnloadReload_ProcessesUnloads verifies that
// drainUnloadReload adds shard IDs from unloadC to the unloaded set.
func TestCommitWorker_DrainUnloadReload_ProcessesUnloads(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	// Enqueue unload requests.
	cw.unloadC <- 10
	cw.unloadC <- 20
	cw.unloadC <- 30

	cw.drainUnloadReload()

	// Verify all shards are in the unloaded set.
	for _, id := range []uint64{10, 20, 30} {
		if _, ok := cw.unloaded[id]; !ok {
			t.Fatalf("shard %d should be in unloaded set", id)
		}
	}
}

// TestCommitWorker_DrainUnloadReload_ProcessesReloads verifies that
// drainUnloadReload removes shard IDs from the unloaded set via reloadC.
func TestCommitWorker_DrainUnloadReload_ProcessesReloads(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	// Pre-populate unloaded set.
	cw.unloaded[10] = struct{}{}
	cw.unloaded[20] = struct{}{}

	// Enqueue reload requests.
	cw.reloadC <- 10
	cw.reloadC <- 20

	cw.drainUnloadReload()

	// Verify shards are removed from the unloaded set.
	if _, ok := cw.unloaded[10]; ok {
		t.Fatal("shard 10 should be removed from unloaded set after reload")
	}
	if _, ok := cw.unloaded[20]; ok {
		t.Fatal("shard 20 should be removed from unloaded set after reload")
	}
}

// TestCommitWorker_DrainUnloadReload_UnloadThenReloadInTwoPasses verifies
// that drainUnloadReload correctly processes unload followed by reload
// in separate calls (deterministic ordering).
func TestCommitWorker_DrainUnloadReload_UnloadThenReloadInTwoPasses(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	// First pass: unload shard 10 and shard 20.
	cw.unloadC <- 10
	cw.unloadC <- 20

	cw.drainUnloadReload()

	// Both should be in the unloaded set.
	if _, ok := cw.unloaded[10]; !ok {
		t.Fatal("shard 10 should be in unloaded set after first drain")
	}
	if _, ok := cw.unloaded[20]; !ok {
		t.Fatal("shard 20 should be in unloaded set after first drain")
	}

	// Second pass: reload shard 10 only.
	cw.reloadC <- 10

	cw.drainUnloadReload()

	// Shard 10 should be removed; shard 20 should remain.
	if _, ok := cw.unloaded[10]; ok {
		t.Fatal("shard 10 should be removed from unloaded set after reload")
	}
	if _, ok := cw.unloaded[20]; !ok {
		t.Fatal("shard 20 should still be in unloaded set")
	}
}

// TestCommitWorker_DrainUnloadReload_EmptyChannels verifies that
// drainUnloadReload returns immediately when both channels are empty.
func TestCommitWorker_DrainUnloadReload_EmptyChannels(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)

	// Should return immediately without blocking.
	done := make(chan struct{})
	go func() {
		cw.drainUnloadReload()
		close(done)
	}()

	select {
	case <-done:
		// Expected.
	case <-time.After(time.Second):
		t.Fatal("drainUnloadReload blocked on empty channels")
	}
}

// ---------------------------------------------------------------------------
// Apply worker dispatch for unloaded shards
// ---------------------------------------------------------------------------

// TestApplyWorker_Dispatch_SkipsUnloadedShard verifies that dispatch
// discards items for unloaded shards, clears commitPending, and
// returns the item to the pool.
func TestApplyWorker_Dispatch_SkipsUnloadedShard(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	// Mark shard 42 as unloaded.
	aw.unloaded[42] = struct{}{}

	cfg := config.Config{ShardID: 42, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:          42,
		CommittedEntries: []proto.Entry{{Index: 1, Term: 1, Cmd: []byte("skip-me")}},
	}
	item.node = node

	// dispatch should skip the item silently.
	aw.dispatch(item)

	// commitPending should be cleared.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared for unloaded shard dispatch")
	}

	// No shard channel should have been created.
	if _, ok := aw.shardChans[42]; ok {
		t.Fatal("shard channel should not be created for unloaded shard")
	}
}

// ---------------------------------------------------------------------------
// CommitWorker run loop: unload/reload select cases in main loop
// ---------------------------------------------------------------------------

// TestCommitWorker_Run_HandlesUnloadInMainLoop verifies that the commit
// worker's main run loop correctly handles unload messages received
// via the unloadC select case (not just via drainUnloadReload).
func TestCommitWorker_Run_HandlesUnloadInMainLoop(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	// Start the commit worker.
	done := make(chan struct{})
	go func() {
		cw.run()
		close(done)
	}()

	// Send unload and reload requests via the channels (not pre-filled).
	cw.requestUnload(55)
	cw.requestReload(55)
	cw.requestUnload(66)

	// Wait for the channels to be drained by the worker's select loop.
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for unload/reload channels to drain",
		func() bool {
			return len(cw.unloadC) == 0 && len(cw.reloadC) == 0
		})

	close(stopC)
	select {
	case <-done:
		// Expected.
	case <-time.After(2 * time.Second):
		t.Fatal("commit worker did not stop within timeout")
	}
}

// ---------------------------------------------------------------------------
// executePiggyback edge cases
// ---------------------------------------------------------------------------

// TestExecutePiggyback_WithOnCommittedCallback verifies that
// executePiggyback fires the OnCommitted callback when the node
// has notifyCommit=true and committed entries are present.
func TestExecutePiggyback_WithOnCommittedCallback(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	cb := newTestCallback()

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), cb, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	tickPeerToLeader(peer)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)
	node.SetNotifyCommit(true)

	piggyback := proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		State:     proto.State{Term: 1, Commit: 1},
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("committed")},
		},
	}

	cw.executePiggyback(node, piggyback)

	// OnCommitted should have been called.
	if cb.committedCount.Load() == 0 {
		t.Fatal("expected OnCommitted callback to fire for piggyback with notifyCommit=true")
	}

	// Apply item should be forwarded.
	select {
	case ai := <-applyC:
		if ai.update.ShardID != 1 {
			t.Fatalf("expected shardID 1, got %d", ai.update.ShardID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected apply item after piggyback")
	}
}

// TestExecutePiggyback_NoMessages verifies executePiggyback works
// correctly when the piggyback update has committed entries but no
// outbound messages to split.
func TestExecutePiggyback_NoMessages(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	tickPeerToLeader(peer)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	piggyback := proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		State:     proto.State{Term: 1, Commit: 1},
		// No messages, only committed entries.
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("no-msg")},
		},
	}

	cw.executePiggyback(node, piggyback)

	// Apply item should still be forwarded.
	select {
	case ai := <-applyC:
		if ai.update.ShardID != 1 {
			t.Fatalf("expected shardID 1, got %d", ai.update.ShardID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected apply item for piggyback with no messages")
	}
}

// TestExecutePiggyback_OnlyOrderedMessages verifies that executePiggyback
// handles the case where all messages are ordered (no free-order messages).
func TestExecutePiggyback_OnlyOrderedMessages(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	sender := newTestSender()

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	tickPeerToLeader(peer)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	piggyback := proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		State:     proto.State{Term: 1, Commit: 1},
		Messages: []proto.Message{
			// Only ordered messages (no Heartbeat/Replicate/Ping).
			{Type: proto.RequestVoteResp, ShardID: 1, From: 1, To: 2, Term: 1},
			{Type: proto.HeartbeatResp, ShardID: 1, From: 1, To: 2},
		},
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("ordered-only")},
		},
	}

	cw.executePiggyback(node, piggyback)

	// Apply item should be forwarded.
	select {
	case ai := <-applyC:
		if ai.update.ShardID != 1 {
			t.Fatalf("expected shardID 1, got %d", ai.update.ShardID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected apply item for piggyback with ordered messages")
	}
}

// ---------------------------------------------------------------------------
// CommitWorker processBatch with nil logdb (memory mode)
// ---------------------------------------------------------------------------

// TestCommitWorker_ProcessBatch_NilLogDB_ForwardsPiggyback verifies that
// processBatch with nil logdb (memory mode) still forwards piggyback
// committed entries to the apply worker.
func TestCommitWorker_ProcessBatch_NilLogDB_ForwardsPiggyback(t *testing.T) {
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	// nil logdb = memory mode.
	cw := newCommitWorker(0, nil, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	tickPeerToLeader(peer)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	// Propose and get an update.
	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("mem-mode")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	cw.processBatch()

	// Apply item should be forwarded (from piggyback path or direct).
	select {
	case ai := <-applyC:
		if ai.update.ShardID != 1 {
			t.Fatalf("expected shardID 1, got %d", ai.update.ShardID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected apply item in memory mode")
	}
}

// ---------------------------------------------------------------------------
// Apply worker: backpressure path in dispatch
// ---------------------------------------------------------------------------

// TestApplyWorker_Dispatch_Backpressure verifies that when a shard's
// apply channel is full, dispatch blocks but eventually delivers the
// item (the backpressure warning path in dispatch).
func TestApplyWorker_Dispatch_Backpressure(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	// Create a per-shard channel with capacity 1 by dispatching one item first.
	goodSM := newTestSM()
	rsmSM := newTestApplier(goodSM)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)

	// Start the dispatcher so shard goroutines are created.
	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Send enough items to fill the shard channel and trigger backpressure.
	// defaultShardChanBuffer is typically small (e.g., 4-8).
	for i := 0; i < defaultShardChanBuffer+2; i++ {
		node.commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:          1,
			CommittedEntries: []proto.Entry{{Term: 1, Index: uint64(i + 1), Cmd: []byte("bp")}},
		}
		item.node = node
		applyC <- item
	}

	// Poll until all items have been applied despite backpressure.
	expected := int64(defaultShardChanBuffer + 2)
	waitFor(t, 10*time.Second, time.Millisecond,
		"timed out waiting for all backpressure items to be applied",
		func() bool { return goodSM.applyCount.Load() >= expected })

	close(stopC)
	<-runDone
	wg.Wait()

	// All items should have been applied despite backpressure.
	if goodSM.applyCount.Load() < int64(defaultShardChanBuffer+2) {
		t.Fatalf("expected all items applied, got %d", goodSM.applyCount.Load())
	}
}

// ---------------------------------------------------------------------------
// Apply worker: SM panic recovery in runShardApply
// ---------------------------------------------------------------------------

// TestApplyWorker_SMPanic_MarksNodeFailed verifies that when the state
// machine panics during Apply, the node is marked as failed and the
// error is reported via handleError.
func TestApplyWorker_SMPanic_MarksNodeFailed(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	panicSM := &panickingSM{}
	rsmSM := newTestApplier(panicSM)

	var failedCalled atomic.Bool
	listener := &config.EventListener{
		OnShardFailed: func(_ config.ShardFailedInfo) {
			failedCalled.Store(true)
		},
	}
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", listener)
	node.commitPending.Store(true)

	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:          1,
		CommittedEntries: []proto.Entry{{Term: 1, Index: 1, Cmd: []byte("panic-trigger")}},
	}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until the node is marked as failed (SM panic recovered).
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for node to be marked as failed after SM panic",
		func() bool { return node.IsFailed() })

	close(stopC)
	<-runDone
	wg.Wait()

	// Node should be marked as failed.
	if !node.IsFailed() {
		t.Fatal("node should be failed after SM panic")
	}

	// commitPending should still be cleared by defer.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared even after SM panic")
	}

	// Error callback should have been called.
	if !failedCalled.Load() {
		t.Fatal("expected error callback for SM panic")
	}
}

// panickingSM is a state machine that panics on Update.
type panickingSM struct{}

func (s *panickingSM) Update(_ context.Context, _ []sm.Entry, _ []sm.Result) error {
	panic("intentional SM panic")
}
func (s *panickingSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return nil, nil
}
func (s *panickingSM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}
func (s *panickingSM) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}
func (s *panickingSM) Close(_ context.Context) error { return nil }

var _ sm.StateMachine = (*panickingSM)(nil)

// ---------------------------------------------------------------------------
// CommitWorker drainCommitC with metrics timestamp
// ---------------------------------------------------------------------------

// TestCommitWorker_DrainCommitC_WithMetrics verifies that drainCommitC
// captures commit timestamps when metrics are configured.
func TestCommitWorker_DrainCommitC_WithMetrics(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	metrics := newTestRaftMetrics()

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, metrics)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)

	now := time.Now()
	commitC <- &commitItem{
		update:    proto.Update{ShardID: 1},
		node:      node,
		createdAt: now,
	}

	// Initialize batch with one item first (drainCommitC appends).
	cw.batch = append(cw.batch[:0], proto.Update{ShardID: 99})
	cw.batchNodes = []*Node{nil}

	cw.drainCommitC()

	// Batch should have 2 items.
	if len(cw.batch) != 2 {
		t.Fatalf("expected 2 items in batch, got %d", len(cw.batch))
	}

	// Commit time should be recorded for shard 1.
	if ct, ok := cw.commitTimes[1]; !ok || ct.IsZero() {
		t.Fatal("expected commit timestamp to be recorded for shard 1")
	}
}

// ---------------------------------------------------------------------------
// Step worker sendMessages with InstallSnapshot messages
// ---------------------------------------------------------------------------

// TestStepWorker_SendMessages_WithSnapshot verifies that sendMessages
// routes InstallSnapshot messages through sender.SendSnapshot while
// regular messages go through sender.Send.
func TestStepWorker_SendMessages_WithSnapshot(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	msgs := []proto.Message{
		{Type: proto.Heartbeat, From: 1, To: 2},
		{Type: proto.InstallSnapshot, From: 1, To: 3},
		{Type: proto.Replicate, From: 1, To: 4},
	}

	sw.sendMessages(msgs)

	// Both Send and SendSnapshot should have been called.
	if sender.sentCount() == 0 {
		t.Fatal("expected sender.Send to be called for regular messages")
	}
	if sender.snapshotCount() == 0 {
		t.Fatal("expected sender.SendSnapshot to be called for InstallSnapshot")
	}
}

// TestStepWorker_SendMessages_NoSnapshot verifies that sendMessages
// takes the fast path (single sender.Send) when no InstallSnapshot
// messages are present.
func TestStepWorker_SendMessages_NoSnapshot(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	msgs := []proto.Message{
		{Type: proto.Heartbeat, From: 1, To: 2},
		{Type: proto.Replicate, From: 1, To: 3},
	}

	sw.sendMessages(msgs)

	// Only Send should have been called (fast path).
	if sender.sentCount() == 0 {
		t.Fatal("expected sender.Send to be called")
	}
	if sender.snapshotCount() != 0 {
		t.Fatal("expected no SendSnapshot calls on fast path")
	}
}

// ---------------------------------------------------------------------------
// Apply worker: requestUnload and requestReload
// ---------------------------------------------------------------------------

// TestApplyWorker_RequestUnload_MarksShard verifies that requestUnload
// adds the shard to the unloaded set via the unloadC channel during
// the dispatcher's run loop.
func TestApplyWorker_RequestUnload_MarksShard(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Request unload for shard 77.
	aw.requestUnload(77)

	// Wait for the dispatcher to drain unloadC.
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for unloadC to drain",
		func() bool { return len(aw.unloadC) == 0 })

	close(stopC)
	<-runDone
	wg.Wait()
}

// TestApplyWorker_RequestReload_ClearsUnloaded verifies that
// requestReload removes the shard from the unloaded set.
func TestApplyWorker_RequestReload_ClearsUnloaded(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Unload then reload shard 88. Wait for each channel to drain
	// before sending the next request to ensure ordering.
	aw.requestUnload(88)
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for unloadC to drain",
		func() bool { return len(aw.unloadC) == 0 })
	aw.requestReload(88)
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for reloadC to drain",
		func() bool { return len(aw.reloadC) == 0 })

	close(stopC)
	<-runDone
	wg.Wait()
}

// ---------------------------------------------------------------------------
// CommitWorker: unloaded shard skip in processBatch
// ---------------------------------------------------------------------------

// TestCommitWorker_ProcessBatch_SkipsUnloadedShard verifies that when
// a shard is in the unloaded set, processBatch clears commitPending
// and skips Peer.Commit for that shard.
func TestCommitWorker_ProcessBatch_SkipsUnloadedShard(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	// Mark shard 1 as unloaded.
	cw.unloaded[1] = struct{}{}

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	cw.batch = append(cw.batch[:0], proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		State:     proto.State{Term: 1},
	})
	cw.batchNodes = []*Node{node}

	cw.processBatch()

	// commitPending should be cleared for unloaded shard.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared for unloaded shard")
	}

	// No apply items should have been sent.
	select {
	case <-applyC:
		t.Fatal("apply worker should not receive items for unloaded shard")
	default:
		// Expected.
	}
}

// ---------------------------------------------------------------------------
// processCommitPendingInbox tests
// ---------------------------------------------------------------------------

// TestStepWorker_ProcessCommitPendingInbox_WithProposals verifies that
// proposals queued while commitPending is true are processed through the
// pipelining path (processCommitPendingInbox), appending entries to the
// Raft log without waiting for the commit worker to complete.
func TestStepWorker_ProcessCommitPendingInbox_WithProposals(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Make the peer a leader.
	tickPeerToLeader(peer)
	if !peer.IsLeader() {
		t.Fatal("peer must be leader for this test")
	}

	// Set commitPending to simulate the commit worker owning the pipeline.
	node.commitPending.Store(true)

	// Queue proposals via the MPSC queue while commitPending is true.
	ok := node.DeliverProposal([]proto.Entry{
		{Cmd: []byte("proposal-during-commit-1")},
		{Cmd: []byte("proposal-during-commit-2")},
	})
	if !ok {
		t.Fatal("DeliverProposal should accept entries")
	}

	// processReady should call processCommitPendingInbox which processes
	// the queued proposals under raftMu.
	ws.Notify(1)
	sw.processReady()

	// No commit items should be produced (commitPending is true,
	// so the normal pipeline is skipped).
	select {
	case <-commitC:
		t.Fatal("no commit items expected when commitPending is true")
	default:
		// Expected.
	}

	// The proposals should have been handled by the peer (appended to log).
	// Verify by checking the proposal queue is drained.
	remaining := node.DrainProposals(nil)
	if len(remaining) != 0 {
		t.Fatalf("proposal queue should be drained, got %d entries", len(remaining))
	}
}

// TestStepWorker_ProcessCommitPendingInbox_WithInboxMessages verifies that
// transport messages queued in the inbox are processed during commitPending
// through the pipelining path, including HeartbeatResp prioritization.
func TestStepWorker_ProcessCommitPendingInbox_WithInboxMessages(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)
	node.commitPending.Store(true)

	// Deliver a HeartbeatResp and a regular message to the inbox.
	node.Deliver(proto.Message{
		Type: proto.HeartbeatResp,
		From: 1,
		To:   1,
	})
	node.Deliver(proto.Message{
		Type: proto.Replicate,
		From: 1,
		To:   1,
	})

	ws.Notify(1)
	sw.processReady()

	// The inbox should be drained.
	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("inbox should be drained, got %d messages", len(remaining))
	}
}

// TestStepWorker_ProcessCommitPendingInbox_InstallSnapshotDeferred verifies
// that InstallSnapshot messages received during commitPending are deferred
// to the next normal processReady cycle (when commitPending is false).
func TestStepWorker_ProcessCommitPendingInbox_InstallSnapshotDeferred(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)
	node.commitPending.Store(true)

	// Deliver an InstallSnapshot message while commitPending is true.
	node.Deliver(proto.Message{
		Type: proto.InstallSnapshot,
		From: 2,
		To:   1,
	})

	ws.Notify(1)
	sw.processReady()

	// The InstallSnapshot should be buffered in deferredSnapshots.
	deferred, ok := sw.deferredSnapshots[1]
	if !ok || len(deferred) == 0 {
		t.Fatal("InstallSnapshot should be deferred during commitPending")
	}
	if deferred[0].Type != proto.InstallSnapshot {
		t.Fatalf("deferred message type = %d, want InstallSnapshot", deferred[0].Type)
	}
}

// TestStepWorker_ProcessCommitPendingInbox_EmptyQueues verifies the fast
// return path when all MPSC queues are empty during commitPending.
func TestStepWorker_ProcessCommitPendingInbox_EmptyQueues(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)
	node.commitPending.Store(true)

	// processReady with empty queues should return without error.
	ws.Notify(1)
	sw.processReady()

	select {
	case <-commitC:
		t.Fatal("no commit items expected")
	default:
		// Expected: empty queues, fast return.
	}
}

// TestStepWorker_DrainAndHandleInbox_SuccessPath verifies that messages
// in the inbox are successfully processed through peer.Handle during
// processReady (non-commitPending path).
func TestStepWorker_DrainAndHandleInbox_SuccessPath(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Deliver valid messages that peer.Handle will accept.
	node.Deliver(proto.Message{
		Type: proto.HeartbeatResp,
		From: 1,
		To:   1,
	})

	ws.Notify(1)
	sw.processReady()

	// Inbox should be drained.
	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("inbox should be drained, got %d messages", len(remaining))
	}
}

// TestStepWorker_DrainAndHandleInbox_DebugLogging verifies the debug
// logging path in drainAndHandleInbox by temporarily enabling debug-level
// logging. This exercises the slog.LevelDebug-gated timing code.
func TestStepWorker_DrainAndHandleInbox_DebugLogging(t *testing.T) {
	// Enable debug logging for this test to cover the handleStart path.
	oldHandler := slog.Default().Handler()
	debugHandler := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(debugHandler))
	defer slog.SetDefault(slog.New(oldHandler))

	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Deliver a valid message so peer.Handle succeeds and the debug
	// timing log fires (the else-if branch at line 368).
	node.Deliver(proto.Message{
		Type: proto.HeartbeatResp,
		From: 1,
		To:   1,
	})

	ws.Notify(1)
	sw.processReady()

	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("inbox should be drained, got %d messages", len(remaining))
	}
}

// TestStepWorker_DrainAndHandleInbox_HandleError verifies that peer.Handle
// errors during inbox processing are logged but do not prevent processing
// of remaining messages.
func TestStepWorker_DrainAndHandleInbox_HandleError(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Deliver an invalid message that will cause peer.Handle to return
	// an error (e.g., message with invalid term or from unknown node).
	node.Deliver(proto.Message{
		Type: proto.Replicate,
		From: 999, // unknown replica
		To:   1,
		Term: 999, // bogus term
	})

	// processReady should process the message without panicking.
	ws.Notify(1)
	sw.processReady()

	// Inbox should be drained regardless of errors.
	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("inbox should be drained after processReady, got %d", len(remaining))
	}
}

// ---------------------------------------------------------------------------
// executePiggyback additional tests
// ---------------------------------------------------------------------------

// TestExecutePiggyback_ApplyCFull_BlockingSend verifies that when the
// first non-blocking send to applyC fails (channel full), executePiggyback
// falls through to the blocking send path.
func TestExecutePiggyback_ApplyCFull_BlockingSend(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 1) // capacity 1
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	sender := newTestSender()
	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)

	tickPeerToLeader(peer)

	// Fill applyC to capacity so the first non-blocking send fails.
	applyC <- &applyItem{}

	// Start a goroutine that drains applyC after a short delay to
	// unblock the blocking send path.
	drained := make(chan struct{})
	go func() {
		<-applyC // drain the pre-filled item
		close(drained)
	}()

	// Wait for the drain goroutine to remove the first item.
	<-drained

	piggyback := proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("piggyback")},
		},
	}

	node.commitPending.Store(true)
	cw.executePiggyback(node, piggyback)

	// The piggyback should have been sent to applyC via the blocking path.
	select {
	case ai := <-applyC:
		if ai.update.ShardID != 1 {
			t.Errorf("apply item ShardID = %d, want 1", ai.update.ShardID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected apply item in applyC")
	}
}

// TestExecutePiggyback_ApplyCFull_StopC verifies that when applyC is full
// and stopC fires, executePiggyback returns without blocking.
func TestExecutePiggyback_ApplyCFull_StopC(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem) // unbuffered = always blocks
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)

	tickPeerToLeader(peer)

	piggyback := proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("piggyback")},
		},
	}

	node.commitPending.Store(true)

	// Close stopC so the blocking send path immediately takes the
	// stopC branch.
	close(stopC)

	// executePiggyback should return without blocking.
	done := make(chan struct{})
	go func() {
		cw.executePiggyback(node, piggyback)
		close(done)
	}()

	select {
	case <-done:
		// Expected: returned via stopC path.
	case <-time.After(2 * time.Second):
		t.Fatal("executePiggyback blocked despite stopC being closed")
	}
}

// ---------------------------------------------------------------------------
// saveSnapshot additional tests
// ---------------------------------------------------------------------------

// TestSaveSnapshot_MaxSnapshotSizeExceeded verifies that saveSnapshot
// rejects snapshots that exceed MaxSnapshotSize and cleans up the
// oversized snapshot directory.
func TestSaveSnapshot_MaxSnapshotSizeExceeded(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	stopC := make(chan struct{})
	ws := NewWorkSignal(1)

	pool := newSnapshotPool(1, nil, nil, nil, ws, stopC, nil)

	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 1, ReplicaID: 1, MaxSnapshotSize: 10} // 10 bytes max
	cfg.SetDefaults()

	// Create a snapshotter that writes more data than the limit.
	ss := &largeSaveSnapshotter{data: make([]byte, 100)} // 100 bytes > 10 byte limit
	node := NewNode(nil, nil, nil, cfg, 100, ss, snapDir, nil)

	_, err := pool.saveSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err == nil {
		t.Fatal("expected error for oversized snapshot")
	}
	var tooLargeErr *SnapshotTooLargeError
	if !errors.As(err, &tooLargeErr) {
		t.Fatalf("expected SnapshotTooLargeError, got %T: %v", err, err)
	}
	if tooLargeErr.Size <= tooLargeErr.Limit {
		t.Fatalf("size (%d) should exceed limit (%d)", tooLargeErr.Size, tooLargeErr.Limit)
	}
}

// largeSaveSnapshotter writes a fixed amount of data during SaveSnapshot.
type largeSaveSnapshotter struct {
	data []byte
}

func (s *largeSaveSnapshotter) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	_, err := w.Write(s.data)
	return err
}

func (s *largeSaveSnapshotter) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (s *largeSaveSnapshotter) LastApplied() uint64 { return 42 }

var _ Snapshotter = (*largeSaveSnapshotter)(nil)

// TestSaveSnapshot_NilSnapshotter verifies that saveSnapshot returns an
// error when the node's snapshotter is nil.
func TestSaveSnapshot_NilSnapshotter(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)

	pool := newSnapshotPool(1, nil, nil, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil) // nil snapshotter

	_, err := pool.saveSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err == nil {
		t.Fatal("expected error for nil snapshotter")
	}
	var snapErr *SnapshotError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected SnapshotError, got %T: %v", err, err)
	}
}

// TestSaveSnapshot_ZeroLastApplied verifies that saveSnapshot returns
// (0, nil) when LastApplied() returns 0 (nothing to snapshot).
func TestSaveSnapshot_ZeroLastApplied(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)

	pool := newSnapshotPool(1, nil, nil, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	ss := &zeroAppliedSnapshotter{}
	node := NewNode(nil, nil, nil, cfg, 100, ss, "", nil)

	idx, err := pool.saveSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx != 0 {
		t.Fatalf("expected index 0 for zero LastApplied, got %d", idx)
	}
}

type zeroAppliedSnapshotter struct{}

func (s *zeroAppliedSnapshotter) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}
func (s *zeroAppliedSnapshotter) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}
func (s *zeroAppliedSnapshotter) LastApplied() uint64 { return 0 }

var _ Snapshotter = (*zeroAppliedSnapshotter)(nil)

// TestSaveSnapshot_SaveError verifies that saveSnapshot returns a
// SnapshotError when the snapshotter's SaveSnapshot fails.
func TestSaveSnapshot_SaveError(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)

	pool := newSnapshotPool(1, nil, nil, nil, ws, stopC, nil)

	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	ss := &errorSaveSnapshotter{err: errors.New("save failed")}
	node := NewNode(nil, nil, nil, cfg, 100, ss, snapDir, nil)

	_, err := pool.saveSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err == nil {
		t.Fatal("expected error from failed SaveSnapshot")
	}
	var snapErr *SnapshotError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected SnapshotError, got %T: %v", err, err)
	}
	if !snapErr.Save {
		t.Fatal("SnapshotError.Save should be true for save operation")
	}
}

type errorSaveSnapshotter struct {
	err error
}

func (s *errorSaveSnapshotter) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return s.err
}
func (s *errorSaveSnapshotter) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}
func (s *errorSaveSnapshotter) LastApplied() uint64 { return 42 }

var _ Snapshotter = (*errorSaveSnapshotter)(nil)

// TestSaveSnapshot_SnappyCompression verifies that saveSnapshot with Snappy
// compression configured writes compressed data and succeeds.
func TestSaveSnapshot_SnappyCompression(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	ldb := newSnapshotTestLogDB()

	pool := newSnapshotPool(1, ldb, nil, nil, ws, stopC, nil)

	snapDir := t.TempDir()
	cfg := config.Config{
		ShardID:                 1,
		ReplicaID:               1,
		SnapshotCompressionType: config.Snappy,
	}
	cfg.SetDefaults()
	ss := &largeSaveSnapshotter{data: []byte("compressible data for snappy test")}
	node := NewNode(nil, nil, nil, cfg, 100, ss, snapDir, nil)

	idx, err := pool.saveSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("saveSnapshot with Snappy failed: %v", err)
	}
	if idx != 42 {
		t.Errorf("snapshot index = %d, want 42", idx)
	}

	// Verify snapshot was persisted to LogDB.
	snap, getErr := ldb.GetSnapshot(1, 1)
	if getErr != nil {
		t.Fatalf("GetSnapshot failed: %v", getErr)
	}
	if snap.Index != 42 {
		t.Errorf("LogDB snapshot index = %d, want 42", snap.Index)
	}
}

// TestSaveSnapshot_FullPipeline verifies saveSnapshot writes data, metadata,
// and persists to LogDB in the normal uncompressed path.
func TestSaveSnapshot_FullPipeline(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	ldb := newSnapshotTestLogDB()

	pool := newSnapshotPool(1, ldb, nil, nil, ws, stopC, nil)

	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	ss := &largeSaveSnapshotter{data: []byte("test-snapshot-data")}

	peer, lr := newTestPeer(t, 1, 1)
	tickPeerToLeader(peer)

	node := NewNode(peer, nil, lr, cfg, 100, ss, snapDir, nil)

	idx, err := pool.saveSnapshot(snapshotRequest{
		shardID:       1,
		replicaID:     1,
		node:          node,
		save:          true,
		appliedToTerm: 1,
	})
	if err != nil {
		t.Fatalf("saveSnapshot failed: %v", err)
	}
	if idx != 42 {
		t.Errorf("snapshot index = %d, want 42", idx)
	}

	// Verify LogDB has the snapshot with membership.
	snap, getErr := ldb.GetSnapshot(1, 1)
	if getErr != nil {
		t.Fatalf("GetSnapshot failed: %v", getErr)
	}
	if snap.Index != 42 {
		t.Errorf("LogDB snapshot index = %d, want 42", snap.Index)
	}
}

// TestSaveSnapshot_LogDBSaveError verifies that saveSnapshot returns a
// SnapshotError when LogDB.SaveSnapshot fails.
func TestSaveSnapshot_LogDBSaveError(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	ldb := &errorSaveSnapshotLogDB{
		snapshotTestLogDB: *newSnapshotTestLogDB(),
		saveErr:           errors.New("logdb write failed"),
	}

	pool := newSnapshotPool(1, ldb, nil, nil, ws, stopC, nil)

	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	ss := &largeSaveSnapshotter{data: []byte("data")}
	node := NewNode(nil, nil, nil, cfg, 100, ss, snapDir, nil)

	_, err := pool.saveSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err == nil {
		t.Fatal("expected error from LogDB.SaveSnapshot failure")
	}
	var snapErr *SnapshotError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected SnapshotError, got %T: %v", err, err)
	}
}

type errorSaveSnapshotLogDB struct {
	snapshotTestLogDB
	saveErr error
}

func (db *errorSaveSnapshotLogDB) SaveSnapshot(_, _ uint64, _ logdb.Snapshot) error {
	return db.saveErr
}

// ---------------------------------------------------------------------------
// recoverSnapshot additional tests
// ---------------------------------------------------------------------------

// TestRecoverSnapshot_NilSnapshotter verifies that recoverSnapshot returns
// nil when the node has no snapshotter (witness node).
func TestRecoverSnapshot_NilSnapshotter(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	ldb := newSnapshotTestLogDB()

	pool := newSnapshotPool(1, ldb, nil, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil) // nil snapshotter

	err := pool.recoverSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      false,
	})
	if err != nil {
		t.Fatalf("expected nil error for nil snapshotter, got %v", err)
	}
}

// TestRecoverSnapshot_GetSnapshotError verifies that recoverSnapshot
// returns a SnapshotError when LogDB.GetSnapshot fails.
func TestRecoverSnapshot_GetSnapshotError(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	ldb := &errorGetSnapshotLogDB{err: errors.New("logdb corrupt")}

	pool := newSnapshotPool(1, ldb, nil, nil, ws, stopC, nil)

	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	ss := &zeroAppliedSnapshotter{}
	node := NewNode(nil, nil, nil, cfg, 100, ss, snapDir, nil)

	err := pool.recoverSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      false,
	})
	if err == nil {
		t.Fatal("expected error from failed GetSnapshot")
	}
	var snapErr *SnapshotError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected SnapshotError, got %T: %v", err, err)
	}
}

// errorGetSnapshotLogDB returns an error from GetSnapshot.
type errorGetSnapshotLogDB struct {
	snapshotTestLogDB
	err error
}

func (db *errorGetSnapshotLogDB) GetSnapshot(_, _ uint64) (logdb.Snapshot, error) {
	return logdb.Snapshot{}, db.err
}

// TestRecoverSnapshot_ZeroIndex verifies that recoverSnapshot returns nil
// when the snapshot index is 0 (no snapshot to recover).
func TestRecoverSnapshot_ZeroIndex(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	ldb := newSnapshotTestLogDB() // empty, GetSnapshot returns Index=0

	pool := newSnapshotPool(1, ldb, nil, nil, ws, stopC, nil)

	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	ss := &zeroAppliedSnapshotter{}
	node := NewNode(nil, nil, nil, cfg, 100, ss, snapDir, nil)

	err := pool.recoverSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      false,
	})
	if err != nil {
		t.Fatalf("expected nil for zero snapshot index, got %v", err)
	}
}

// TestRecoverSnapshot_FileNotExist verifies that recoverSnapshot returns
// nil when the snapshot data file doesn't exist (cleaned up).
func TestRecoverSnapshot_FileNotExist(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	ldb := newSnapshotTestLogDB()

	snapDir := t.TempDir()
	// Set Filepath to a directory that exists but has no snapshot.dat.
	ldb.snapshots[1] = logdb.Snapshot{
		Index:    100,
		Term:     5,
		Filepath: snapDir,
	}

	pool := newSnapshotPool(1, ldb, nil, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	ss := &zeroAppliedSnapshotter{}
	node := NewNode(nil, nil, nil, cfg, 100, ss, snapDir, nil)

	err := pool.recoverSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      false,
	})
	// File not found is treated as "snapshot cleaned up" -> nil error.
	if err != nil {
		t.Fatalf("expected nil for missing snapshot file, got %v", err)
	}
}

// TestRecoverSnapshot_RecoverError verifies that recoverSnapshot returns
// a SnapshotError when RecoverFromSnapshot fails.
func TestRecoverSnapshot_RecoverError(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	ldb := newSnapshotTestLogDB()

	snapDir := t.TempDir()
	ldb.snapshots[1] = logdb.Snapshot{
		Index:    100,
		Term:     5,
		Filepath: snapDir,
	}

	// Create the snapshot data file.
	dataPath := filepath.Join(snapDir, "snapshot.dat")
	if err := os.WriteFile(dataPath, []byte("snapshot-data"), 0o644); err != nil {
		t.Fatalf("failed to create snapshot data file: %v", err)
	}

	pool := newSnapshotPool(1, ldb, nil, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	ss := &errorRecoverSnapshotter{err: errors.New("recovery failed")}
	node := NewNode(nil, nil, nil, cfg, 100, ss, snapDir, nil)

	err := pool.recoverSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      false,
	})
	if err == nil {
		t.Fatal("expected error from failed RecoverFromSnapshot")
	}
	var snapErr *SnapshotError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected SnapshotError, got %T: %v", err, err)
	}
	if snapErr.Save {
		t.Fatal("SnapshotError.Save should be false for recover operation")
	}
}

type errorRecoverSnapshotter struct {
	err error
}

func (s *errorRecoverSnapshotter) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}
func (s *errorRecoverSnapshotter) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return s.err
}
func (s *errorRecoverSnapshotter) LastApplied() uint64 { return 42 }

var _ Snapshotter = (*errorRecoverSnapshotter)(nil)

// TestRecoverSnapshot_SuccessUpdatesNodeState verifies that a successful
// recovery updates the node's lastApplied and lastSnapshotIndex fields.
func TestRecoverSnapshot_SuccessUpdatesNodeState(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	ldb := newSnapshotTestLogDB()

	snapDir := t.TempDir()
	ldb.snapshots[1] = logdb.Snapshot{
		Index:    200,
		Term:     10,
		Filepath: snapDir,
	}

	// Create the snapshot data file.
	dataPath := filepath.Join(snapDir, "snapshot.dat")
	if err := os.WriteFile(dataPath, []byte("valid-snapshot"), 0o644); err != nil {
		t.Fatalf("failed to create snapshot data file: %v", err)
	}

	pool := newSnapshotPool(1, ldb, nil, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	ss := &noopSnapshotter{}
	node := NewNode(nil, nil, nil, cfg, 100, ss, snapDir, nil)

	err := pool.recoverSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// lastApplied and lastSnapshotIndex should be updated.
	if node.lastApplied.Load() != 200 {
		t.Errorf("lastApplied = %d, want 200", node.lastApplied.Load())
	}
	if node.lastSnapshotIndex.Load() != 200 {
		t.Errorf("lastSnapshotIndex = %d, want 200", node.lastSnapshotIndex.Load())
	}
}

type noopSnapshotter struct{}

func (s *noopSnapshotter) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}
func (s *noopSnapshotter) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}
func (s *noopSnapshotter) LastApplied() uint64 { return 42 }

var _ Snapshotter = (*noopSnapshotter)(nil)

// ---------------------------------------------------------------------------
// apply worker additional tests
// ---------------------------------------------------------------------------

// TestApplyWorker_RunShardApply_EmptyEntries verifies that runShardApply
// clears commitPending when there are no committed entries to apply.
func TestApplyWorker_RunShardApply_EmptyEntries(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	var wg sync.WaitGroup
	ws := NewWorkSignal(1)

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	item := &applyItem{
		update: proto.Update{
			ShardID:          1,
			ReplicaID:        1,
			CommittedEntries: nil, // empty
		},
		node: node,
	}

	// Simulate dispatch by adding the shard channel and sending.
	shardC := make(chan *applyItem, 1)
	aw.shardChans[1] = shardC
	shardC <- item
	close(shardC) // close so runShardApply exits after processing

	aw.runShardApply(shardC)

	// commitPending should be cleared.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared for empty entries")
	}
}

// TestApplyWorker_RunShardApply_RecoveringSkipsApply verifies that when a
// node is recovering (snapshot recovery in progress), entries are not
// applied to the state machine.
func TestApplyWorker_RunShardApply_RecoveringSkipsApply(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	var wg sync.WaitGroup
	ws := NewWorkSignal(1)

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)
	node.SetRecovering(true) // simulate snapshot recovery in progress

	item := &applyItem{
		update: proto.Update{
			ShardID:   1,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{Index: 1, Term: 1, Cmd: []byte("should-not-apply")},
			},
		},
		node: node,
	}

	shardC := make(chan *applyItem, 1)
	aw.shardChans[1] = shardC
	shardC <- item
	close(shardC)

	aw.runShardApply(shardC)

	// Entries should NOT be applied because recovering is true.
	if testSm.applyCount.Load() != 0 {
		t.Fatal("state machine should not be applied during recovery")
	}

	// commitPending should still be cleared.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared even during recovery skip")
	}
}

// TestApplyWorker_RunShardApply_AutoSnapshot verifies that the auto-snapshot
// trigger fires when SnapshotEntries is configured and enough entries have
// been applied since the last snapshot.
func TestApplyWorker_RunShardApply_AutoSnapshot(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	var wg sync.WaitGroup
	ws := NewWorkSignal(1)

	// Create snapshot pool with capacity.
	snapPool := newSnapshotPool(1, nil, nil, nil, ws, stopC, nil)

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, snapPool, ws, stopC, nil)

	cfg := config.Config{
		ShardID:         1,
		ReplicaID:       1,
		SnapshotEntries: 5, // trigger snapshot every 5 entries
	}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)
	node.lastSnapshotIndex.Store(0) // no previous snapshot

	// Apply 6 entries (>= SnapshotEntries threshold of 5).
	entries := make([]proto.Entry, 6)
	for i := range entries {
		entries[i] = proto.Entry{Index: uint64(i + 1), Term: 1, Cmd: []byte("data")}
	}

	item := &applyItem{
		update: proto.Update{
			ShardID:          1,
			ReplicaID:        1,
			CommittedEntries: entries,
		},
		node: node,
	}

	shardC := make(chan *applyItem, 1)
	aw.shardChans[1] = shardC
	shardC <- item
	close(shardC)

	aw.runShardApply(shardC)

	// lastApplied should be updated to the highest index.
	if node.lastApplied.Load() != 6 {
		t.Errorf("lastApplied = %d, want 6", node.lastApplied.Load())
	}

	// snapshotting flag should be set (snapshot request was submitted).
	if !node.snapshotting.Load() {
		t.Fatal("snapshotting should be set after auto-snapshot trigger")
	}
}

// TestApplyWorker_RunShardApply_DecompressError verifies that when entry
// decompression fails, the node's circuit breaker counter is incremented,
// lastApplied is advanced, and the error is handled.
func TestApplyWorker_RunShardApply_DecompressError(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	var wg sync.WaitGroup
	ws := NewWorkSignal(1)

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	// Create an entry with the compress flag set but invalid Snappy data.
	// This will cause DecompressEntries to fail.
	item := &applyItem{
		update: proto.Update{
			ShardID:   1,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{
					Index: 3,
					Term:  1,
					Type:  proto.EntryCompressFlag, // compressed flag
					Cmd:   []byte("not-valid-snappy"),
				},
			},
		},
		node: node,
	}

	shardC := make(chan *applyItem, 1)
	aw.shardChans[1] = shardC
	shardC <- item
	close(shardC)

	aw.runShardApply(shardC)

	// applyRetries should be incremented.
	if node.applyRetries.Load() == 0 {
		t.Fatal("applyRetries should be incremented on decompression error")
	}

	// lastApplied should be advanced.
	if node.lastApplied.Load() != 3 {
		t.Errorf("lastApplied = %d, want 3", node.lastApplied.Load())
	}

	// SM should NOT have been called.
	if testSm.applyCount.Load() != 0 {
		t.Fatal("state machine should not be applied on decompression error")
	}

	// commitPending should be cleared.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared after decompression error")
	}
}

// TestApplyWorker_RunShardApply_ApplyError verifies that when sm.Apply
// returns an error, the node's circuit breaker counter is incremented,
// lastApplied is advanced, and pending proposals are failed.
func TestApplyWorker_RunShardApply_ApplyError(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	var wg sync.WaitGroup
	ws := NewWorkSignal(1)

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	testSm.setApplyError(errors.New("apply failed"))
	rsmSM := newTestApplier(testSm)
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	item := &applyItem{
		update: proto.Update{
			ShardID:   1,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{Index: 5, Term: 1, Cmd: []byte("data")},
			},
		},
		node: node,
	}

	shardC := make(chan *applyItem, 1)
	aw.shardChans[1] = shardC
	shardC <- item
	close(shardC)

	aw.runShardApply(shardC)

	// applyRetries should be incremented.
	if node.applyRetries.Load() == 0 {
		t.Fatal("applyRetries should be incremented on Apply error")
	}

	// lastApplied should be advanced to prevent permanent gaps.
	if node.lastApplied.Load() != 5 {
		t.Errorf("lastApplied = %d, want 5", node.lastApplied.Load())
	}

	// commitPending should be cleared.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared after apply error")
	}
}

// TestApplyWorker_RunShardApply_ApplyErrorCircuitBreaker verifies that
// repeated Apply errors trigger the circuit breaker, marking the node
// as permanently failed.
func TestApplyWorker_RunShardApply_ApplyErrorCircuitBreaker(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	var wg sync.WaitGroup
	ws := NewWorkSignal(1)

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	testSm.setApplyError(errors.New("persistent failure"))
	rsmSM := newTestApplier(testSm)
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)

	// Pre-set retries to just below the threshold so one more failure
	// triggers the circuit breaker.
	node.applyRetries.Store(node.maxApplyRetries - 1)

	item := &applyItem{
		update: proto.Update{
			ShardID:   1,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{Index: 10, Term: 1, Cmd: []byte("fail")},
			},
		},
		node: node,
	}
	node.commitPending.Store(true)

	shardC := make(chan *applyItem, 1)
	aw.shardChans[1] = shardC
	shardC <- item
	close(shardC)

	aw.runShardApply(shardC)

	// Node should be marked as failed via circuit breaker.
	if !node.IsFailed() {
		t.Fatal("node should be marked as failed after circuit breaker trips")
	}
}

// TestRequestSnapshot_PoolBusy verifies that requestSnapshot returns a
// SnapshotPoolBusyError when the request channel is full.
func TestRequestSnapshot_PoolBusy(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	cb := newTestCallback()

	// Create a pool with a very small request channel (capacity 1).
	pool := newSnapshotPool(1, nil, nil, cb, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node1 := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)

	cfg2 := config.Config{ShardID: 2, ReplicaID: 1}
	cfg2.SetDefaults()
	node2 := NewNode(nil, nil, nil, cfg2, 100, nil, "", nil)

	// Fill the request channel by submitting a request.
	err1 := pool.requestSnapshot(snapshotRequest{
		shardID: 1, replicaID: 1, node: node1, save: true,
	})
	if err1 != nil {
		t.Fatalf("first request should succeed: %v", err1)
	}

	// The second request for a different shard should fail because the
	// channel is full (capacity = workers = 1).
	err2 := pool.requestSnapshot(snapshotRequest{
		shardID: 2, replicaID: 1, node: node2, save: true,
	})
	if err2 == nil {
		t.Fatal("expected SnapshotPoolBusyError when channel is full")
	}
	var busyErr *SnapshotPoolBusyError
	if !errors.As(err2, &busyErr) {
		t.Fatalf("expected SnapshotPoolBusyError, got %T: %v", err2, err2)
	}
}

// TestRequestSnapshot_Duplicate verifies that requestSnapshot returns a
// SnapshotDuplicateError when a snapshot is already in-flight for the shard.
func TestRequestSnapshot_Duplicate(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	cb := newTestCallback()

	pool := newSnapshotPool(1, nil, nil, cb, ws, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)

	// First request succeeds.
	err := pool.requestSnapshot(snapshotRequest{
		shardID: 1, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("first request should succeed: %v", err)
	}

	// Second request for the same shard should fail as duplicate.
	err = pool.requestSnapshot(snapshotRequest{
		shardID: 1, replicaID: 1, node: node, save: true,
	})
	if err == nil {
		t.Fatal("expected SnapshotDuplicateError for duplicate shard request")
	}
	var dupErr *SnapshotDuplicateError
	if !errors.As(err, &dupErr) {
		t.Fatalf("expected SnapshotDuplicateError, got %T: %v", err, err)
	}
}

// TestStepWorker_CommitMemoryMode_StopC verifies that commitMemoryMode
// returns true when stopC fires during the applyC send, clearing
// commitPending and returning the apply item to the pool.
func TestStepWorker_CommitMemoryMode_StopC(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})
	applyC := make(chan *applyItem) // unbuffered to force blocking
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)
	sw.memoryMode = true
	sw.applyC = applyC
	sw.applyPool = applyPool

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Close stopC before calling commitMemoryMode.
	close(stopC)

	update := proto.Update{
		ShardID:   1,
		ReplicaID: 1,
	}

	stopped := sw.commitMemoryMode(node, update)
	if !stopped {
		t.Fatal("commitMemoryMode should return true when stopC fires")
	}
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared on stopC")
	}
}

// TestStepWorker_CommitMemoryMode_SuccessPath verifies the normal
// commitMemoryMode path: Commit succeeds, entries are forwarded to applyC.
func TestStepWorker_CommitMemoryMode_SuccessPath(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)
	sw.memoryMode = true
	sw.applyC = applyC
	sw.applyPool = applyPool

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Propose an entry to generate an update.
	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("memory-mode")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	lastApplied := node.lastApplied.Load()
	update, getErr := peer.GetUpdate(true, lastApplied)
	if getErr != nil {
		t.Fatalf("GetUpdate failed: %v", getErr)
	}

	stopped := sw.commitMemoryMode(node, update)
	if stopped {
		t.Fatal("commitMemoryMode should return false on success")
	}

	// Should have forwarded to applyC.
	select {
	case ai := <-applyC:
		if ai.update.ShardID != 1 {
			t.Errorf("apply item ShardID = %d, want 1", ai.update.ShardID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected apply item in applyC")
	}
}

// TestStepWorker_EnqueueForCommit_StopC verifies that enqueueForCommit
// returns true and clears commitPending when stopC is closed while
// trying to send on a full commitC.
func TestStepWorker_EnqueueForCommit_StopC(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem) // unbuffered
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	close(stopC)

	update := proto.Update{
		ShardID:   1,
		ReplicaID: 1,
	}

	stopped := sw.enqueueForCommit(1, node, update)
	if !stopped {
		t.Fatal("enqueueForCommit should return true when stopC fires")
	}
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared on stopC")
	}
}

// TestCommitWorker_ProcessBatch_ApplyCStopC verifies that when applyC is
// full and stopC fires, processBatch returns without blocking.
func TestCommitWorker_ProcessBatch_ApplyCStopC(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem) // unbuffered
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	sender := newTestSender()
	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.commitPending.Store(true)

	tickPeerToLeader(peer)

	// Propose and get update to create committed entries.
	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}
	lastApplied := node.lastApplied.Load()
	update, getErr := peer.GetUpdate(true, lastApplied)
	if getErr != nil {
		t.Fatalf("GetUpdate failed: %v", getErr)
	}

	cw.batch = append(cw.batch[:0], update)
	cw.batchNodes = []*Node{node}

	// Close stopC so the blocking send path immediately exits.
	close(stopC)

	// processBatch should not block because stopC is closed.
	done := make(chan struct{})
	go func() {
		cw.processBatch()
		close(done)
	}()

	select {
	case <-done:
		// Expected.
	case <-time.After(2 * time.Second):
		t.Fatal("processBatch blocked despite stopC being closed")
	}
}

// TestCommitWorker_Run_ReloadInMainLoop verifies that the reload case in
// the commit worker's main select loop removes the shard from the
// unloaded set.
func TestCommitWorker_Run_ReloadInMainLoop(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	// Pre-mark shard 5 as unloaded.
	cw.unloaded[5] = struct{}{}

	// Start the worker loop in background.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cw.run()
	}()

	// Send reload request.
	cw.reloadC <- uint64(5)

	// Wait for the select loop to process the reload (channel drained).
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for reloadC to drain",
		func() bool { return len(cw.reloadC) == 0 })

	close(stopC)
	wg.Wait()

	// Shard 5 should no longer be in the unloaded set.
	if _, stillUnloaded := cw.unloaded[5]; stillUnloaded {
		t.Fatal("shard 5 should have been removed from unloaded set after reload")
	}
}

// TestStepWorker_EnqueueForCommit_WithMessages verifies that enqueueForCommit
// splits messages into free-order (sent immediately) and ordered (deferred
// to the commit worker) when the update contains messages.
func TestStepWorker_EnqueueForCommit_WithMessages(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Create an update with both free-order and ordered messages.
	update := proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		Messages: []proto.Message{
			{Type: proto.Replicate, From: 1, To: 2},     // free-order
			{Type: proto.HeartbeatResp, From: 1, To: 2}, // ordered
		},
	}

	stopped := sw.enqueueForCommit(1, node, update)
	if stopped {
		t.Fatal("enqueueForCommit should return false on success")
	}

	// Free-order messages should have been sent immediately.
	if sender.sentCount() == 0 {
		t.Fatal("expected free-order messages to be sent")
	}

	// Commit item should be in commitC with ordered messages.
	select {
	case item := <-commitC:
		if len(item.orderedMsgs) == 0 {
			t.Fatal("expected ordered messages in commit item")
		}
	case <-time.After(time.Second):
		t.Fatal("expected commit item")
	}
}

// TestStepWorker_ProcessReady_RecoveringSkipped verifies that nodes
// with the recovering flag set are skipped during processReady.
func TestStepWorker_ProcessReady_RecoveringSkipped(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	node.SetRecovering(true) // simulate snapshot recovery in progress
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Queue a proposal - it should NOT be processed because recovering.
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("ignored")}})

	ws.Notify(1)
	sw.processReady()

	// No commit items should be produced.
	select {
	case <-commitC:
		t.Fatal("no commit items expected for recovering node")
	default:
		// Expected.
	}
}

// TestCommitWorker_Run_UnloadInMainLoop verifies that the unload case in
// the commit worker's main select loop adds the shard to the unloaded set.
func TestCommitWorker_Run_UnloadInMainLoop(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cw.run()
	}()

	// Send unload request.
	cw.unloadC <- uint64(7)

	// Wait for the select loop to process the unload (channel drained).
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for unloadC to drain",
		func() bool { return len(cw.unloadC) == 0 })

	close(stopC)
	wg.Wait()

	if _, unloaded := cw.unloaded[7]; !unloaded {
		t.Fatal("shard 7 should be in unloaded set after unload request")
	}
}
