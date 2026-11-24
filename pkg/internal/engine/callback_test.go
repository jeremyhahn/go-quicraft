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
	mu                  sync.Mutex
	shardFailedCalls    []shardFailedCall
	shardFailedCount    atomic.Int64
	appliedCalls        []appliedCall
	committedCalls      []committedCall
	leaderCalls         []leaderCall
	readyToReadKeys     []readyToReadCall
	appliedCount        atomic.Int64
	committedCount      atomic.Int64
	leaderCallCount     atomic.Int64
	leaderEvents        []leaderEvent
	leaderReadyCount    atomic.Int64
	readyCount          atomic.Int64
	snapshotCalls       []snapshotCall
	compactionCalls     []compactionCall
	requestCalls        []requestCall
	snapshotCallCount   atomic.Int64
	campaignCalls       []campaignCall
	campaignCount       atomic.Int64
	readIndexBatchCalls []readIndexBatchCall
	batchCount          atomic.Int64
	proposalFailedCalls []proposalFailedCall
	proposalFailedCount atomic.Int64
	// proposalRejectedCalls records OnProposalRejected: proposals raft refused
	// before appending them.
	proposalRejectedCalls []proposalFailedCall
	proposalRejectedCount atomic.Int64
	readIndexFailedCalls  []readIndexFailedCall
	readIndexFailedCount  atomic.Int64
	snapshotRejected      atomic.Int64
	rejectedCauses        []error // guarded by mu

	// panicOnReadyToRead makes OnReadyToRead panic, standing in for a bug in
	// the engine-to-Host plumbing that runs inline on the step worker. Set
	// before the worker starts; never mutated concurrently.
	panicOnReadyToRead bool
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

// leaderEvent is one OnLeaderUpdated (ready false) or OnLeaderReady (ready
// true, with the reporting replica) in delivery order.
type leaderEvent struct {
	leaderCall
	ready     bool
	replicaID uint64
}

type snapshotCall struct {
	shardID uint64
	index   uint64
	op      SnapshotOperation
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

type shardFailedCall struct {
	shardID uint64
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
	call := leaderCall{
		shardID:  shardID,
		leaderID: leaderID,
		term:     term,
	}
	c.leaderCalls = append(c.leaderCalls, call)
	c.leaderEvents = append(c.leaderEvents, leaderEvent{leaderCall: call})
	c.mu.Unlock()
}

func (c *testCallback) OnLeaderReady(shardID, replicaID, leaderID, term uint64) {
	c.leaderReadyCount.Add(1)
	c.mu.Lock()
	c.leaderEvents = append(c.leaderEvents, leaderEvent{
		leaderCall: leaderCall{shardID: shardID, leaderID: leaderID, term: term},
		ready:      true,
		replicaID:  replicaID,
	})
	c.mu.Unlock()
}

// getLeaderEvents returns OnLeaderUpdated and OnLeaderReady calls in the one
// order they were delivered.
func (c *testCallback) getLeaderEvents() []leaderEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]leaderEvent, len(c.leaderEvents))
	copy(result, c.leaderEvents)
	return result
}

func (c *testCallback) OnReadyToRead(shardID uint64, readyToRead []proto.ReadyToRead) {
	c.readyCount.Add(1)
	if c.panicOnReadyToRead {
		panic("test: OnReadyToRead panicked")
	}
	cp := make([]proto.ReadyToRead, len(readyToRead))
	copy(cp, readyToRead)
	c.mu.Lock()
	c.readyToReadKeys = append(c.readyToReadKeys, readyToReadCall{
		shardID:     shardID,
		readyToRead: cp,
	})
	c.mu.Unlock()
}

func (c *testCallback) OnSnapshotCompleted(shardID uint64, index uint64, op SnapshotOperation, err error) {
	c.snapshotCallCount.Add(1)
	c.mu.Lock()
	c.snapshotCalls = append(c.snapshotCalls, snapshotCall{
		shardID: shardID,
		index:   index,
		op:      op,
		err:     err,
	})
	c.mu.Unlock()
}

// requestCall records one OnSnapshotRequestCompleted delivery.
type requestCall struct {
	shardID    uint64
	requestKey uint64
	index      uint64
	err        error
}

func (c *testCallback) OnSnapshotRequestCompleted(shardID, requestKey, index uint64, err error) {
	c.mu.Lock()
	c.requestCalls = append(c.requestCalls, requestCall{
		shardID:    shardID,
		requestKey: requestKey,
		index:      index,
		err:        err,
	})
	c.mu.Unlock()
}

// compactionCall records one OnLogCompacted delivery.
type compactionCall struct {
	shardID    uint64
	replicaID  uint64
	compaction LogCompaction
}

func (c *testCallback) OnLogCompacted(shardID, replicaID uint64, compaction LogCompaction) {
	c.mu.Lock()
	c.compactionCalls = append(c.compactionCalls, compactionCall{
		shardID:    shardID,
		replicaID:  replicaID,
		compaction: compaction,
	})
	c.mu.Unlock()
}

// getCompactionCalls returns a copy of every OnLogCompacted delivery so far.
func (c *testCallback) getCompactionCalls() []compactionCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]compactionCall(nil), c.compactionCalls...)
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

func (c *testCallback) OnSnapshotRejected(_, _ uint64, err error) {
	c.mu.Lock()
	c.rejectedCauses = append(c.rejectedCauses, err)
	c.mu.Unlock()
	c.snapshotRejected.Add(1)
}

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

func (c *testCallback) OnProposalRejected(shardID uint64, entries []proto.Entry, err error) {
	c.proposalRejectedCount.Add(1)
	entriesCopy := make([]proto.Entry, len(entries))
	copy(entriesCopy, entries)
	c.mu.Lock()
	c.proposalRejectedCalls = append(c.proposalRejectedCalls, proposalFailedCall{
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

func (c *testCallback) OnShardFailed(shardID uint64, err error) {
	c.shardFailedCount.Add(1)
	c.mu.Lock()
	c.shardFailedCalls = append(c.shardFailedCalls, shardFailedCall{shardID: shardID, err: err})
	c.mu.Unlock()
}

func (c *testCallback) getShardFailedCalls() []shardFailedCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]shardFailedCall, len(c.shardFailedCalls))
	copy(out, c.shardFailedCalls)
	return out
}

func (c *testCallback) getProposalFailedCalls() []proposalFailedCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]proposalFailedCall, len(c.proposalFailedCalls))
	copy(result, c.proposalFailedCalls)
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

func (c *testCallback) getAppliedCalls() []appliedCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]appliedCall, len(c.appliedCalls))
	copy(result, c.appliedCalls)
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

	// Three shards, each with its own SM and a two-entry batch: every shard
	// fires exactly one OnApplied call carrying its own entries and results.
	const shards = 3
	for shardID := uint64(1); shardID <= shards; shardID++ {
		rsmSM := newTestApplier(newTestSM())
		cfg := config.Config{ShardID: shardID, ReplicaID: 1}
		node := NewNode(nil, rsmSM, nil, cfg, nil, "", nil)

		node.commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:   shardID,
			ReplicaID: 1,
			CommittedEntries: []proto.Entry{
				{Term: 1, Index: 1, Cmd: []byte("cmd1")},
				{Term: 1, Index: 2, Cmd: []byte("cmd2")},
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

	// Poll until every shard's callback fires (apply completed).
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for OnApplied callbacks", func() bool {
		return cb.appliedCount.Load() >= shards
	})

	close(stopC)
	<-runDone
	wg.Wait()

	if cb.appliedCount.Load() != shards {
		t.Fatalf("expected %d OnApplied calls, got %d", shards, cb.appliedCount.Load())
	}

	shardsSeen := make(map[uint64]bool)
	for _, call := range cb.getAppliedCalls() {
		if shardsSeen[call.shardID] {
			t.Errorf("shard %d: more than one OnApplied call", call.shardID)
		}
		shardsSeen[call.shardID] = true
		if len(call.entries) != 2 || len(call.results) != 2 {
			t.Fatalf("shard %d: %d entries, %d results, want 2 and 2",
				call.shardID, len(call.entries), len(call.results))
		}
		for i := range 2 {
			want := uint64(i + 1)
			if call.entries[i].Index != want {
				t.Errorf("shard %d: entries[%d].Index = %d, want %d", call.shardID, i, call.entries[i].Index, want)
			}
			// testSM sets Value = entry.Index.
			if call.results[i].Value != want {
				t.Errorf("shard %d: results[%d].Value = %d, want %d", call.shardID, i, call.results[i].Value, want)
			}
		}
	}
	for shardID := uint64(1); shardID <= shards; shardID++ {
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
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
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
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
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
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
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
	node := NewNode(nil, rsmSM, nil, cfg, nil, "", nil)
	// Seed the applied frontier so the batch below continues at
	// lastApplied+1; otherwise the apply gap check rejects it before
	// SM.Apply is ever called and this test would pass vacuously.
	node.SeedLastApplied(9, 1)

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
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for OnProposalFailed callback", func() bool {
		return cb.proposalFailedCount.Load() != 0
	})

	close(stopC)
	<-runDone
	wg.Wait()

	// Fail-fast: lastApplied stays at the seeded frontier. Advancing it
	// while skipping the batch would diverge the SM from its peers.
	if got := node.lastApplied.Load(); got != 9 {
		t.Fatalf("lastApplied = %d, want 9 (fail-fast does not advance)", got)
	}

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
	// The failure must be the SM.Apply error, not a gap rejection.
	if applyErr.Err == nil || applyErr.Err.Error() != "disk full" {
		t.Fatalf("ApplyError.Err = %v, want the SM.Apply error", applyErr.Err)
	}

	// The halt is announced ONCE, with the same error, and only after the
	// batch's own proposals were completed. OnShardFailed is what
	// lets the host fail the reads the batch does not account for; if it
	// fired before OnProposalFailed, the host would drain pendingReads
	// while the proposals that caused the halt were still unresolved.
	shardFailed := cb.getShardFailedCalls()
	if len(shardFailed) != 1 {
		t.Fatalf("expected exactly 1 OnShardFailed call, got %d", len(shardFailed))
	}
	if shardFailed[0].shardID != 42 {
		t.Errorf("OnShardFailed shardID = %d, want 42", shardFailed[0].shardID)
	}
	if !errors.Is(shardFailed[0].err, fc.err) && shardFailed[0].err != fc.err {
		t.Errorf("OnShardFailed err = %v, want the same *ApplyError the batch was "+
			"failed with (%v)", shardFailed[0].err, fc.err)
	}
	if cb.shardFailedCount.Load() != 1 {
		t.Errorf("OnShardFailed fired %d times for one halt, want 1", cb.shardFailedCount.Load())
	}
}

// TestApplyWorker_ApplyError_NilCallbackDoesNotPanic verifies that
// a nil callback does not cause a panic when apply fails. Under fail-fast
// semantics the shard is marked failed and lastApplied is NOT advanced.
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
	node := NewNode(nil, rsmSM, nil, cfg, nil, "", nil)
	// Seed the applied frontier so the batch below continues at
	// lastApplied+1; otherwise the apply gap check rejects it before
	// SM.Apply is ever called and this test would pass vacuously.
	node.SeedLastApplied(2, 1)

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

	// Wait for the shard to be marked failed (confirms the error path ran).
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for shard to be marked failed with nil callback", node.failed.Load)

	close(stopC)
	<-runDone
	wg.Wait()

	// Fail-fast: lastApplied must NOT advance on apply error. Advancing it
	// while skipping the batch would diverge the SM from its peers.
	if got := node.lastApplied.Load(); got != 2 {
		t.Fatalf("lastApplied = %d, want 2 (fail-fast does not advance)", got)
	}
}

// TestApplyWorker_ApplyError_FailsFast verifies that an SM.Apply error is
// terminal: the shard is marked failed on the FIRST error, an ApplyError
// is reported through the listener, OnProposalFailed is invoked, and
// lastApplied is not advanced.
func TestApplyWorker_ApplyError_FailsFast(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	testSm := newTestSM()
	testSm.setApplyError(errors.New("persistent failure"))
	rsmSM := newTestApplier(testSm)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	errorsCh := make(chan error, 8)
	listener := &config.EventListener{
		OnShardFailed: func(info config.ShardFailedInfo) {
			errorsCh <- info.Err
		},
	}
	node := NewNode(nil, rsmSM, nil, cfg, nil, "", listener)

	go aw.run()

	// A single failing item must trip fail-fast immediately.
	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 1, Index: 1, Cmd: []byte("fail")},
		},
	}
	item.node = node
	applyC <- item

	// Wait for the shard to be marked failed.
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for shard to fail", node.failed.Load)

	// lastApplied must not advance under fail-fast.
	if got := node.lastApplied.Load(); got != 0 {
		t.Fatalf("lastApplied = %d, want 0 (fail-fast does not advance)", got)
	}

	// An ApplyError must be reported through the listener.
	var foundApplyErr bool
	deadline := time.After(2 * time.Second)
	for !foundApplyErr {
		select {
		case reported := <-errorsCh:
			var applyErr *ApplyError
			if errors.As(reported, &applyErr) {
				foundApplyErr = true
			}
		case <-deadline:
			t.Fatal("expected ApplyError to be reported through listener")
		}
	}

	// OnProposalFailed must be invoked exactly once for the failed batch.
	if cb.proposalFailedCount.Load() != 1 {
		t.Fatalf("expected 1 OnProposalFailed call, got %d", cb.proposalFailedCount.Load())
	}

	close(stopC)
}
