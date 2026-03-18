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
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// testCallback is a test implementation of Callback that records
// all calls for verification.
type testCallback struct {
	mu                   sync.Mutex
	appliedCalls         []appliedCall
	committedCalls       []committedCall
	leaderCalls          []leaderCall
	readyToReadKeys      []readyToReadCall
	appliedCount         atomic.Int64
	committedCount       atomic.Int64
	leaderCallCount      atomic.Int64
	readyCount           atomic.Int64
	snapshotCalls        []snapshotCall
	snapshotCallCount    atomic.Int64
	campaignCalls        []campaignCall
	campaignCount        atomic.Int64
	campaignSkippedCalls []campaignCall
	campaignSkippedCount atomic.Int64
	readIndexBatchCalls  []readIndexBatchCall
	batchCount           atomic.Int64
	proposalFailedCalls  []proposalFailedCall
	proposalFailedCount  atomic.Int64
	readIndexFailedCalls []readIndexFailedCall
	readIndexFailedCount atomic.Int64
}

type readIndexBatchCall struct {
	shardID   uint64
	batchKey  uint64
	aliasKeys []uint64
}

type readyToReadCall struct {
	shardID     uint64
	readyToRead []proto.ReadyToRead
}

type appliedCall struct {
	shardID uint64
	entries []proto.Entry
	results []sm.Result
}

type committedCall struct {
	shardID uint64
	entries []proto.Entry
}

type leaderCall struct {
	shardID  uint64
	leaderID uint64
	term     uint64
}

type snapshotCall struct {
	shardID uint64
	index   uint64
	err     error
}

type campaignCall struct {
	shardID   uint64
	replicaID uint64
	term      uint64
}

type proposalFailedCall struct {
	shardID uint64
	entries []proto.Entry
	err     error
}

type readIndexFailedCall struct {
	shardID uint64
	keys    []uint64
	err     error
}

func newTestCallback() *testCallback {
	return &testCallback{}
}

func (c *testCallback) OnApplied(shardID uint64, entries []proto.Entry, results []sm.Result) {
	c.appliedCount.Add(1)
	entriesCopy := make([]proto.Entry, len(entries))
	copy(entriesCopy, entries)
	resultsCopy := make([]sm.Result, len(results))
	copy(resultsCopy, results)
	c.mu.Lock()
	c.appliedCalls = append(c.appliedCalls, appliedCall{
		shardID: shardID,
		entries: entriesCopy,
		results: resultsCopy,
	})
	c.mu.Unlock()
}

func (c *testCallback) OnCommitted(shardID uint64, entries []proto.Entry) {
	c.committedCount.Add(1)
	entriesCopy := make([]proto.Entry, len(entries))
	copy(entriesCopy, entries)
	c.mu.Lock()
	c.committedCalls = append(c.committedCalls, committedCall{
		shardID: shardID,
		entries: entriesCopy,
	})
	c.mu.Unlock()
}

func (c *testCallback) OnLeaderUpdated(shardID uint64, leaderID uint64, term uint64) {
	c.leaderCallCount.Add(1)
	c.mu.Lock()
	c.leaderCalls = append(c.leaderCalls, leaderCall{
		shardID:  shardID,
		leaderID: leaderID,
		term:     term,
	})
	c.mu.Unlock()
}

func (c *testCallback) OnReadyToRead(shardID uint64, readyToRead []proto.ReadyToRead) {
	c.readyCount.Add(1)
	cp := make([]proto.ReadyToRead, len(readyToRead))
	copy(cp, readyToRead)
	c.mu.Lock()
	c.readyToReadKeys = append(c.readyToReadKeys, readyToReadCall{
		shardID:     shardID,
		readyToRead: cp,
	})
	c.mu.Unlock()
}

func (c *testCallback) OnSnapshotCompleted(shardID uint64, index uint64, err error) {
	c.snapshotCallCount.Add(1)
	c.mu.Lock()
	c.snapshotCalls = append(c.snapshotCalls, snapshotCall{
		shardID: shardID,
		index:   index,
		err:     err,
	})
	c.mu.Unlock()
}

func (c *testCallback) OnCampaignLaunched(shardID, replicaID, term uint64) {
	c.campaignCount.Add(1)
	c.mu.Lock()
	c.campaignCalls = append(c.campaignCalls, campaignCall{
		shardID:   shardID,
		replicaID: replicaID,
		term:      term,
	})
	c.mu.Unlock()
}

func (c *testCallback) getCampaignCalls() []campaignCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]campaignCall, len(c.campaignCalls))
	copy(result, c.campaignCalls)
	return result
}

func (c *testCallback) OnCampaignSkipped(shardID, replicaID, term uint64) {
	c.campaignSkippedCount.Add(1)
	c.mu.Lock()
	c.campaignSkippedCalls = append(c.campaignSkippedCalls, campaignCall{
		shardID:   shardID,
		replicaID: replicaID,
		term:      term,
	})
	c.mu.Unlock()
}

func (c *testCallback) getCampaignSkippedCalls() []campaignCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]campaignCall, len(c.campaignSkippedCalls))
	copy(result, c.campaignSkippedCalls)
	return result
}

func (c *testCallback) OnSnapshotRejected(_, _ uint64) {}

func (c *testCallback) OnReadIndexBatched(shardID uint64, batchKey uint64, aliasKeys []uint64) {
	c.batchCount.Add(1)
	cp := make([]uint64, len(aliasKeys))
	copy(cp, aliasKeys)
	c.mu.Lock()
	c.readIndexBatchCalls = append(c.readIndexBatchCalls, readIndexBatchCall{
		shardID:   shardID,
		batchKey:  batchKey,
		aliasKeys: cp,
	})
	c.mu.Unlock()
}

func (c *testCallback) OnProposalFailed(shardID uint64, entries []proto.Entry, err error) {
	c.proposalFailedCount.Add(1)
	entriesCopy := make([]proto.Entry, len(entries))
	copy(entriesCopy, entries)
	c.mu.Lock()
	c.proposalFailedCalls = append(c.proposalFailedCalls, proposalFailedCall{
		shardID: shardID,
		entries: entriesCopy,
		err:     err,
	})
	c.mu.Unlock()
}

func (c *testCallback) OnReadIndexFailed(shardID uint64, keys []uint64, err error) {
	c.readIndexFailedCount.Add(1)
	keysCopy := make([]uint64, len(keys))
	copy(keysCopy, keys)
	c.mu.Lock()
	c.readIndexFailedCalls = append(c.readIndexFailedCalls, readIndexFailedCall{
		shardID: shardID,
		keys:    keysCopy,
		err:     err,
	})
	c.mu.Unlock()
}

func (c *testCallback) getProposalFailedCalls() []proposalFailedCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]proposalFailedCall, len(c.proposalFailedCalls))
	copy(result, c.proposalFailedCalls)
	return result
}

func (c *testCallback) getReadIndexBatchCalls() []readIndexBatchCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]readIndexBatchCall, len(c.readIndexBatchCalls))
	copy(result, c.readIndexBatchCalls)
	return result
}

func (c *testCallback) getReadIndexFailedCalls() []readIndexFailedCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]readIndexFailedCall, len(c.readIndexFailedCalls))
	copy(result, c.readIndexFailedCalls)
	return result
}

func (c *testCallback) getSnapshotCalls() []snapshotCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]snapshotCall, len(c.snapshotCalls))
	copy(result, c.snapshotCalls)
	return result
}

func (c *testCallback) getReadyToReadCalls() []readyToReadCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]readyToReadCall, len(c.readyToReadKeys))
	copy(result, c.readyToReadKeys)
	return result
}

func (c *testCallback) getAppliedCalls() []appliedCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]appliedCall, len(c.appliedCalls))
	copy(result, c.appliedCalls)
	return result
}

func (c *testCallback) getCommittedCalls() []committedCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]committedCall, len(c.committedCalls))
	copy(result, c.committedCalls)
	return result
}

func (c *testCallback) getLeaderCalls() []leaderCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]leaderCall, len(c.leaderCalls))
	copy(result, c.leaderCalls)
	return result
}

var _ Callback = (*testCallback)(nil)

// ---------------------------------------------------------------------------
// Apply worker callback tests
// ---------------------------------------------------------------------------

func TestApplyWorker_CallbackFiresOnSuccessfulApply(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)

	// Send an item with committed entries.
	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 1, Cmd: []byte("cmd1")},
			{Term: 1, Index: 2, Cmd: []byte("cmd2")},
		},
	}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until the callback fires (apply completed).
	deadlineCB := time.After(5 * time.Second)
	for cb.appliedCount.Load() == 0 {
		select {
		case <-deadlineCB:
			t.Fatal("timed out waiting for OnApplied callback")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	close(stopC)
	<-runDone
	wg.Wait()

	// Verify callback was called.
	if cb.appliedCount.Load() != 1 {
		t.Fatalf("expected 1 OnApplied call, got %d", cb.appliedCount.Load())
	}

	calls := cb.getAppliedCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 applied call, got %d", len(calls))
	}
	if calls[0].shardID != 1 {
		t.Errorf("shardID = %d, want 1", calls[0].shardID)
	}
	if len(calls[0].entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(calls[0].entries))
	}
	if calls[0].entries[0].Index != 1 {
		t.Errorf("entries[0].Index = %d, want 1", calls[0].entries[0].Index)
	}
	if calls[0].entries[1].Index != 2 {
		t.Errorf("entries[1].Index = %d, want 2", calls[0].entries[1].Index)
	}
	if len(calls[0].results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(calls[0].results))
	}
	// testSM sets Value = entry.Index.
	if calls[0].results[0].Value != 1 {
		t.Errorf("results[0].Value = %d, want 1", calls[0].results[0].Value)
	}
	if calls[0].results[1].Value != 2 {
		t.Errorf("results[1].Value = %d, want 2", calls[0].results[1].Value)
	}
}

func TestApplyWorker_OnAppliedNotCalledOnApplyError(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	testSm.setApplyError(&ApplyError{ShardID: 1, ReplicaID: 1, Err: ErrStopped})
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)

	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 1, Cmd: []byte("cmd1")},
		},
	}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until OnProposalFailed fires (apply error path completed).
	deadlineErr := time.After(5 * time.Second)
	for cb.proposalFailedCount.Load() == 0 {
		select {
		case <-deadlineErr:
			t.Fatal("timed out waiting for OnProposalFailed callback")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	close(stopC)
	<-runDone
	wg.Wait()

	// OnApplied should NOT have been called due to apply error.
	if cb.appliedCount.Load() != 0 {
		t.Fatalf("expected 0 OnApplied calls, got %d", cb.appliedCount.Load())
	}
	// OnProposalFailed MUST be called so callers see an immediate error
	// instead of waiting for a context deadline timeout.
	if cb.proposalFailedCount.Load() != 1 {
		t.Fatalf("expected 1 OnProposalFailed call, got %d", cb.proposalFailedCount.Load())
	}
	failedCalls := cb.getProposalFailedCalls()
	if failedCalls[0].shardID != 1 {
		t.Errorf("OnProposalFailed shardID = %d, want 1", failedCalls[0].shardID)
	}
	var applyErr *ApplyError
	if !errors.As(failedCalls[0].err, &applyErr) {
		t.Fatalf("expected ApplyError, got %T: %v", failedCalls[0].err, failedCalls[0].err)
	}
}

func TestApplyWorker_NilCallbackDoesNotPanic(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	// nil callback should not panic.
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
			{Term: 1, Index: 1, Cmd: []byte("cmd1")},
		},
	}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until the SM apply completes (no panic occurred).
	deadlineNil := time.After(5 * time.Second)
	for testSm.applyCount.Load() == 0 {
		select {
		case <-deadlineNil:
			t.Fatal("timed out waiting for SM apply with nil callback")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	close(stopC)
	<-runDone
	wg.Wait()

	// Should have applied without panic.
	if testSm.applyCount.Load() != 1 {
		t.Fatalf("expected 1 apply, got %d", testSm.applyCount.Load())
	}
}

func TestApplyWorker_CallbackMultipleShards(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	// Create 3 shards with their own SMs.
	for shardID := uint64(1); shardID <= 3; shardID++ {
		testSm := newTestSM()
		rsmSM := newTestApplier(testSm)
		cfg := config.Config{ShardID: shardID, ReplicaID: 1}
		node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)

		node.commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:   shardID,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{Term: 1, Index: 1, Cmd: []byte("cmd")},
			},
		}
		item.node = node
		applyC <- item
	}

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Poll until all 3 shards have fired callbacks.
	deadlineMulti := time.After(5 * time.Second)
	for cb.appliedCount.Load() < 3 {
		select {
		case <-deadlineMulti:
			t.Fatalf("timed out waiting for 3 OnApplied callbacks, got %d", cb.appliedCount.Load())
		default:
		}
		time.Sleep(time.Millisecond)
	}

	close(stopC)
	<-runDone
	wg.Wait()

	// Each shard should have triggered one callback.
	if cb.appliedCount.Load() != 3 {
		t.Fatalf("expected 3 OnApplied calls, got %d", cb.appliedCount.Load())
	}

	calls := cb.getAppliedCalls()
	shardsSeen := make(map[uint64]bool)
	for _, call := range calls {
		shardsSeen[call.shardID] = true
	}
	for shardID := uint64(1); shardID <= 3; shardID++ {
		if !shardsSeen[shardID] {
			t.Errorf("missing OnApplied call for shard %d", shardID)
		}
	}
}

// ---------------------------------------------------------------------------
// Engine SetCallback tests
// ---------------------------------------------------------------------------

func TestEngine_SetCallback(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	cb := newTestCallback()
	e.SetCallback(cb)

	// Verify callback is set on the engine.
	if e.callback != cb {
		t.Fatal("callback not set on engine")
	}

	// Verify callback is propagated to step workers.
	for i := range e.NumWorkers() {
		sw := e.StepWorker(i)
		if sw.callback != cb {
			t.Errorf("step worker %d callback not set", i)
		}
	}

	// Verify callback is propagated to commit workers.
	for i := range e.NumWorkers() {
		cw := e.CommitWorker(i)
		if cw.callback != cb {
			t.Errorf("commit worker %d callback not set", i)
		}
	}

	// Verify callback is propagated to apply workers.
	for i := range e.NumWorkers() {
		aw := e.ApplyWorker(i)
		if aw.callback != cb {
			t.Errorf("apply worker %d callback not set", i)
		}
	}
}

func TestEngine_SetSender(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender1 := newTestSender()

	e := NewEngine(cfg, ldb, sender1, nil, nil, nil)

	sender2 := newTestSender()
	e.SetSender(sender2)

	if e.sender != sender2 {
		t.Fatal("sender not updated on engine")
	}

	for i := range e.NumWorkers() {
		sw := e.StepWorker(i)
		if sw.sender != sender2 {
			t.Errorf("step worker %d sender not updated", i)
		}
	}
}

func TestEngine_SetSender_PanicsAfterStart(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()
	defer e.Stop()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from SetSender after Start, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if msg != "engine: SetSender called after Start" {
			t.Fatalf("unexpected panic message: %s", msg)
		}
	}()

	e.SetSender(newTestSender())
}

func TestEngine_SetCallback_PanicsAfterStart(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()
	defer e.Stop()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from SetCallback after Start, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if msg != "engine: SetCallback called after Start" {
			t.Fatalf("unexpected panic message: %s", msg)
		}
	}()

	e.SetCallback(newTestCallback())
}

// ---------------------------------------------------------------------------
// Step worker leader change detection tests
// ---------------------------------------------------------------------------

func TestStepWorker_PrevLeadersInitialized(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	cb := newTestCallback()
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	if sw.prevLeaders == nil {
		t.Fatal("prevLeaders should be initialized")
	}
	if len(sw.prevLeaders) != 0 {
		t.Fatalf("prevLeaders should be empty, got %d entries", len(sw.prevLeaders))
	}
}

func TestStepWorker_CallbackFieldSet(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	cb := newTestCallback()
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	if sw.callback != cb {
		t.Fatal("callback not set on step worker")
	}
}

// ---------------------------------------------------------------------------
// Step worker campaign detection tests
// ---------------------------------------------------------------------------

// newMultiNodeTestPeer creates a raft.Peer configured for a 3-node cluster.
// This forces the peer to broadcast RequestPreVote/RequestVote messages when
// the election timeout fires, unlike a single-node cluster which self-elects
// silently.
func newMultiNodeTestPeer(t *testing.T, shardID, replicaID uint64) (*raft.Peer, *logdb.LogReader) {
	t.Helper()
	ldb := newTestLogDB()
	lr := logdb.NewLogReader(shardID, replicaID, ldb)
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: "localhost:10001",
			2: "localhost:10002",
			3: "localhost:10003",
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
		t.Fatalf("failed to create multi-node peer: %v", err)
	}
	return peer, lr
}

func TestStepWorker_OnCampaignLaunched_FiredOnElection(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	cb := newTestCallback()
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	// Use a 3-node cluster so the peer broadcasts vote messages.
	peer, lr := newMultiNodeTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Tick enough times to trigger the election timeout. A 3-node cluster
	// peer will send RequestPreVote messages to the other two members.
	for range 30 {
		sw.tick()
	}

	// processReady should detect the campaign and fire the callback.
	ws.Notify(1)
	sw.processReady()

	// Drain the commit item produced by the election state change.
	select {
	case <-commitC:
	default:
	}

	// Verify OnCampaignLaunched was called at least once.
	if cb.campaignCount.Load() < 1 {
		t.Fatal("expected OnCampaignLaunched callback to be fired during election")
	}
	calls := cb.getCampaignCalls()
	if len(calls) < 1 {
		t.Fatal("expected at least one campaign call")
	}
	if calls[0].shardID != 1 {
		t.Errorf("campaign call shardID = %d, want 1", calls[0].shardID)
	}
	if calls[0].replicaID != 1 {
		t.Errorf("campaign call replicaID = %d, want 1", calls[0].replicaID)
	}
	if calls[0].term == 0 {
		t.Error("campaign call term should be non-zero")
	}
}

func TestStepWorker_OnCampaignLaunched_NotFiredWithoutElection(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	cb := newTestCallback()
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	// Use a single-node cluster. The node self-elects without sending
	// any RequestVote or RequestPreVote messages, so the campaign
	// callback should NOT fire.
	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Tick to become leader (single-node self-elects).
	tickPeerToLeader(peer)

	// processReady should produce an update but without vote messages.
	ws.Notify(1)
	sw.processReady()

	// Drain commit item.
	select {
	case <-commitC:
	default:
	}

	// No campaign callback should have been fired.
	if cb.campaignCount.Load() != 0 {
		t.Fatalf("expected 0 OnCampaignLaunched calls for single-node cluster, got %d",
			cb.campaignCount.Load())
	}
}

func TestStepWorker_OnCampaignLaunched_NilCallback(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	// nil callback should not panic during campaign detection.
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newMultiNodeTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Tick to trigger election.
	for range 30 {
		sw.tick()
	}

	// processReady with nil callback should not panic.
	ws.Notify(1)
	sw.processReady()

	// Drain commit item.
	select {
	case <-commitC:
	default:
	}
}

// ---------------------------------------------------------------------------
// Apply failure: pending proposals completed with error (not timeout)
// ---------------------------------------------------------------------------

// TestApplyWorker_ApplyError_PendingProposalsFailedImmediately verifies
// that when SM.Apply returns an error, pending proposals are completed
// via OnProposalFailed with an ApplyError instead of timing out.
func TestApplyWorker_ApplyError_PendingProposalsFailedImmediately(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	testSm.setApplyError(errors.New("disk full"))
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 42, ReplicaID: 7}
	cfg.SetDefaults()
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)

	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   42,
		ReplicaID: 7,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 10, Key: 100, Cmd: []byte("a")},
			{Term: 1, Index: 11, Key: 101, Cmd: []byte("b")},
			{Term: 1, Index: 12, Key: 102, Cmd: []byte("c")},
		},
	}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Wait for the proposal failed callback.
	deadline := time.After(5 * time.Second)
	for cb.proposalFailedCount.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for OnProposalFailed callback")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	close(stopC)
	<-runDone
	wg.Wait()

	// OnApplied must not be called on error.
	if cb.appliedCount.Load() != 0 {
		t.Fatalf("expected 0 OnApplied calls, got %d", cb.appliedCount.Load())
	}

	// Exactly one OnProposalFailed call with all 3 entries.
	failedCalls := cb.getProposalFailedCalls()
	if len(failedCalls) != 1 {
		t.Fatalf("expected 1 OnProposalFailed call, got %d", len(failedCalls))
	}
	fc := failedCalls[0]
	if fc.shardID != 42 {
		t.Errorf("shardID = %d, want 42", fc.shardID)
	}
	if len(fc.entries) != 3 {
		t.Fatalf("expected 3 entries in failed call, got %d", len(fc.entries))
	}
	for i, wantKey := range []uint64{100, 101, 102} {
		if fc.entries[i].Key != wantKey {
			t.Errorf("entries[%d].Key = %d, want %d", i, fc.entries[i].Key, wantKey)
		}
	}
	var applyErr *ApplyError
	if !errors.As(fc.err, &applyErr) {
		t.Fatalf("expected ApplyError, got %T: %v", fc.err, fc.err)
	}
	if applyErr.ShardID != 42 || applyErr.ReplicaID != 7 {
		t.Errorf("ApplyError shard/replica = %d/%d, want 42/7",
			applyErr.ShardID, applyErr.ReplicaID)
	}
}

// TestApplyWorker_ApplyError_LastAppliedAdvances verifies that
// lastApplied is advanced even when SM.Apply returns an error. This
// is critical because the commit worker has already advanced processed;
// not advancing lastApplied would create a permanent gap.
func TestApplyWorker_ApplyError_LastAppliedAdvances(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	testSm.setApplyError(errors.New("transient failure"))
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)

	// Verify initial lastApplied is zero.
	if node.lastApplied.Load() != 0 {
		t.Fatalf("expected initial lastApplied 0, got %d", node.lastApplied.Load())
	}

	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 5, Cmd: []byte("x")},
			{Term: 1, Index: 6, Cmd: []byte("y")},
			{Term: 1, Index: 7, Cmd: []byte("z")},
		},
	}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Wait for lastApplied to advance.
	deadline := time.After(5 * time.Second)
	for node.lastApplied.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for lastApplied to advance")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	close(stopC)
	<-runDone
	wg.Wait()

	// lastApplied must be the highest index in the batch.
	if got := node.lastApplied.Load(); got != 7 {
		t.Fatalf("lastApplied = %d, want 7", got)
	}
}

// TestApplyWorker_ApplyError_NilCallbackDoesNotPanic verifies that
// a nil callback does not cause a panic when apply fails.
func TestApplyWorker_ApplyError_NilCallbackDoesNotPanic(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	// nil callback -- must not panic on the error path.
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	testSm.setApplyError(errors.New("boom"))
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, rsmSM, nil, cfg, 100, nil, "", nil)

	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 3, Cmd: []byte("fail")},
		},
	}
	item.node = node
	applyC <- item

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Wait for lastApplied to advance (confirms the error path ran).
	deadline := time.After(5 * time.Second)
	for node.lastApplied.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for lastApplied to advance with nil callback")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	close(stopC)
	<-runDone
	wg.Wait()

	// lastApplied must still be advanced despite nil callback.
	if got := node.lastApplied.Load(); got != 3 {
		t.Fatalf("lastApplied = %d, want 3", got)
	}
}

// TestApplyWorker_ApplyError_CircuitBreakerStillTrips verifies that
// the circuit breaker continues to work correctly after the fix.
// After maxApplyRetries consecutive failures, the shard is marked
// as failed and a CircuitBreakerError is reported.
func TestApplyWorker_ApplyError_CircuitBreakerStillTrips(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	testSm.setApplyError(errors.New("persistent failure"))
	rsmSM := newTestApplier(testSm)

	// maxApplyRetries = 3 so the circuit breaker trips on the 3rd failure.
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	// Use a buffered channel to collect errors reported through the
	// listener. atomic.Value panics when storing different concrete
	// types (*ApplyError vs *CircuitBreakerError).
	errorsCh := make(chan error, 8)
	listener := &config.EventListener{
		OnShardFailed: func(info config.ShardFailedInfo) {
			errorsCh <- info.Err
		},
	}
	node := NewNode(nil, rsmSM, nil, cfg, 3, nil, "", listener)

	go aw.run()

	// Send 3 items to trip the circuit breaker.
	for i := uint64(1); i <= 3; i++ {
		node.commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:   1,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{Term: 1, Index: i, Cmd: []byte("fail")},
			},
		}
		item.node = node
		applyC <- item

		// Wait for this item to complete before sending the next.
		deadline := time.After(5 * time.Second)
		for node.lastApplied.Load() < i {
			select {
			case <-deadline:
				t.Fatalf("timed out waiting for item %d to process", i)
			default:
			}
			time.Sleep(time.Millisecond)
		}
	}

	// Verify circuit breaker tripped.
	if !node.failed.Load() {
		t.Fatal("expected node to be marked as failed after circuit breaker trip")
	}

	// Drain the error channel and find the CircuitBreakerError.
	var foundCBErr bool
	for {
		select {
		case reported := <-errorsCh:
			var cbErr *CircuitBreakerError
			if errors.As(reported, &cbErr) {
				foundCBErr = true
			}
		default:
			goto doneErrors
		}
	}
doneErrors:
	if !foundCBErr {
		t.Fatal("expected CircuitBreakerError to be reported through listener")
	}

	// Verify OnProposalFailed was called for each batch.
	if cb.proposalFailedCount.Load() != 3 {
		t.Fatalf("expected 3 OnProposalFailed calls, got %d", cb.proposalFailedCount.Load())
	}

	close(stopC)
}
