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
)

// unloadOnLeaderCallback is an engine Callback that reacts to a leadership
// change by STOPPING the shard, which is what an operator handler naturally
// does. It records how long the reaction took and signals completion.
type unloadOnLeaderCallback struct {
	testCallback
	engine  *Engine
	once    sync.Once
	done    chan struct{}
	unloads atomic.Int64
}

func (c *unloadOnLeaderCallback) OnLeaderUpdated(shardID, leaderID, term uint64) {
	c.testCallback.OnLeaderUpdated(shardID, leaderID, term)
	c.once.Do(func() {
		c.engine.UnloadNode(shardID)
		c.unloads.Add(1)
		close(c.done)
	})
}

// TestEngine_ListenerStopsShardFromLeaderCallback_DoesNotDeadlock is the
// regression test for the step-worker callback reentrancy deadlock.
//
// Every callback the step worker fired ran synchronously on the step-worker
// goroutine WHILE it held stepWorker.nodesMu.RLock — tick() and processReady()
// hold it for their whole body. Engine.UnloadNode (Host.StopShard) re-enters
// nodesMu.Lock() on that same goroutine via stepWorker.unloadNode, and Go's
// RWMutex is not reentrant, so this was a PERMANENT deadlock rather than a
// stall: every shard mapped to shardID % numWorkers stopped ticking (no
// heartbeats, no elections, no inbox drain) and a later Engine.Stop never
// returned from stepWg.Wait().
//
// The fix delivers user-visible notifications on the listener dispatcher, a
// goroutine that holds no engine lock. The test therefore asserts two things
// with hard deadlines — the callback's StopShard completes, and Engine.Stop
// afterwards returns — because a test that hangs forever is not a passing test.
func TestEngine_ListenerStopsShardFromLeaderCallback_DoesNotDeadlock(t *testing.T) {
	const shardID, replicaID = 1, 1

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	cb := &unloadOnLeaderCallback{engine: e, done: make(chan struct{})}
	e.SetCallback(cb)

	// Build the peer and carry it to leadership BEFORE the engine starts, so the
	// test goroutine is the peer's only toucher while it is being driven.
	peer, lr := newTestVoterPeer(t, shardID, replicaID, 3)
	driveToLeader(t, peer, replicaID, 2)

	nodeCfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	nodeCfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, nodeCfg, nil, "", nil)
	e.LoadNode(node)

	e.Start()

	// The step worker's first cycle sees leader != prevLeader and publishes,
	// which drives the callback into UnloadNode.
	select {
	case <-cb.done:
	case <-time.After(15 * time.Second):
		t.Fatal("StopShard from an OnLeaderUpdated listener never completed: the " +
			"callback ran on the step-worker goroutine while it held nodesMu.RLock, " +
			"and UnloadNode's nodesMu.Lock() cannot be re-entered")
	}

	stopped := make(chan struct{})
	go func() {
		e.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(15 * time.Second):
		t.Fatal("Engine.Stop did not return: the step worker is wedged in " +
			"nodesMu.Lock() and stepWg.Wait() can never complete")
	}

	if got := cb.unloads.Load(); got != 1 {
		t.Fatalf("expected exactly one UnloadNode from the listener, got %d", got)
	}
	if e.GetNode(shardID) != nil {
		t.Fatal("the shard should no longer be loaded after the listener stopped it")
	}
}

// TestNode_HandleError_DoesNotRunListenerOnCallerGoroutine covers the other half
// of the same defect: EventListener.OnShardFailed is fired from node.handleError,
// which the step worker reaches under nodesMu.RLock (a GetUpdate failure) and a
// per-shard apply goroutine reaches on the very goroutine a synchronous
// UnloadNode joins on. "React to a failed shard by stopping it" is the obvious
// handler, so running it on the producing goroutine is the same deadlock.
//
// The observable contract is that handleError RETURNS while the listener is
// still executing, i.e. the listener does not run on the caller's goroutine.
func TestNode_HandleError_DoesNotRunListenerOnCallerGoroutine(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)
	e.Start()
	defer e.Stop()

	entered := make(chan struct{})
	release := make(chan struct{})
	listener := &config.EventListener{
		OnShardFailed: func(config.ShardFailedInfo) {
			close(entered)
			<-release
		},
	}

	nodeCfg := config.Config{ShardID: 3, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "", listener)
	e.LoadNode(node)

	returned := make(chan struct{})
	go func() {
		node.handleError(errors.New("shard is toast"))
		close(returned)
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("OnShardFailed was never delivered")
	}

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("handleError blocked until the listener returned: the listener is " +
			"running on the producing worker's goroutine, so a listener that calls " +
			"StopShard deadlocks against it")
	}
	close(release)
}

// TestListenerDispatcher_PreservesProductionOrder pins the ordering guarantee the
// dispatcher promises: one goroutine consuming a FIFO queue, so notifications
// arrive in the order the engine produced them — a total order, and therefore
// per-shard order too. Consumers that watch a shard go leader -> follower ->
// leader depend on it.
func TestListenerDispatcher_PreservesProductionOrder(t *testing.T) {
	d := newListenerDispatcher()
	var wg sync.WaitGroup
	d.start(&wg)

	const n = 500
	var mu sync.Mutex
	got := make([]int, 0, n)
	for i := range n {
		if !d.dispatch(func() {
			mu.Lock()
			got = append(got, i)
			mu.Unlock()
		}) {
			t.Fatalf("dispatch %d refused: the queue (%d) should not fill here", i, listenerQueueSize)
		}
	}
	d.stop()
	wg.Wait()

	if len(got) != n {
		t.Fatalf("delivered %d of %d notifications; stop() must drain what the "+
			"workers queued before they exited", len(got), n)
	}
	for i := range n {
		if got[i] != i {
			t.Fatalf("notification %d delivered out of order (got %d): FIFO production "+
				"order is the guarantee consumers rely on", i, got[i])
		}
	}
}

// blockDispatcher parks the dispatcher goroutine inside a notification and
// returns the channel that releases it, only once it is provably parked. The
// wait is what makes "fill the queue to capacity" deterministic: without it the
// blocker may still be sitting IN the queue when the fill starts, so the fill
// overflows by one and then frees a slot when the consumer finally picks the
// blocker up.
func blockDispatcher(t *testing.T, d *listenerDispatcher) (release chan struct{}, parked chan struct{}) {
	t.Helper()
	release = make(chan struct{})
	parked = make(chan struct{})
	if !d.dispatch(func() {
		close(parked)
		<-release
	}) {
		t.Fatal("first dispatch refused on an empty queue")
	}
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("dispatcher goroutine never picked up the first notification")
	}
	return release, parked
}

// TestListenerDispatcher_FullQueueRefusesWithoutBlocking pins the queue-full
// policy. dispatch must NEVER block the producing worker: a worker parked on a
// full queue still holds nodesMu.RLock, so a listener calling StopShard from the
// dispatcher goroutine would wait on nodesMu.Lock() behind that parked reader
// and neither would move again — the deadlock the dispatcher exists to remove.
// Refusal must be reported to the caller and counted, never silent.
func TestListenerDispatcher_FullQueueRefusesWithoutBlocking(t *testing.T) {
	d := newListenerDispatcher()
	var wg sync.WaitGroup
	d.start(&wg)

	// Park the consumer so nothing drains, and WAIT until it is actually parked
	// before filling: the dispatch below only leaves the queue exactly full if
	// the blocker has already been dequeued.
	block, _ := blockDispatcher(t, d)
	for i := range listenerQueueSize {
		if !d.dispatch(func() {}) {
			t.Fatalf("dispatch %d refused before the queue reached capacity %d",
				i, listenerQueueSize)
		}
	}

	refused := make(chan bool, 1)
	go func() {
		refused <- d.dispatch(func() {})
	}()
	select {
	case ok := <-refused:
		if ok {
			t.Fatal("dispatch accepted a notification on a full queue")
		}
	case <-time.After(5 * time.Second):
		close(block)
		t.Fatal("dispatch BLOCKED on a full queue; the producing worker holds " +
			"nodesMu.RLock and must never park here")
	}
	if d.droppedCount() == 0 {
		t.Fatal("a refused notification must be counted, not dropped silently")
	}

	close(block)
	d.stop()
	wg.Wait()
}

// TestStepWorker_LeaderChangeIsRepublishedWhenQueueIsFull pins the one
// queue-full policy that is NOT drop. OnLeaderUpdated maintains the Host's
// isLeader mirror — the flag clients route writes by — so losing one leaves
// ShardInfo permanently wrong. publishLeaderChange therefore commits its
// prevLeaders bookkeeping only when the dispatch is accepted, which makes a
// refused publish retry on the next tick instead of vanishing.
func TestStepWorker_LeaderChangeIsRepublishedWhenQueueIsFull(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	d := newListenerDispatcher()
	var wg sync.WaitGroup
	d.start(&wg)
	sw.listeners = d

	block, _ := blockDispatcher(t, d)
	for i := range listenerQueueSize {
		if !d.dispatch(func() {}) {
			t.Fatalf("dispatch %d refused before the queue reached capacity %d",
				i, listenerQueueSize)
		}
	}

	peer, lr := newTestVoterPeer(t, 1, 1, 3)
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	nodeCfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, nodeCfg, nil, "", nil)
	sw.loadNode(node)
	driveToLeader(t, peer, 1, 2)

	// Refused: the queue is full. The bookkeeping must NOT record the leader.
	sw.publishLeaderChange(1, node)
	if _, recorded := sw.prevLeaders[1]; recorded {
		t.Fatal("a refused leader publish recorded prevLeaders anyway; the change " +
			"would never be re-published and the host's isLeader mirror would stay stale")
	}

	// Drain the queue and retry: the change must still be published.
	close(block)
	waitFor(t, 10*time.Second, time.Millisecond, "listener queue never drained", func() bool {
		return len(d.q) == 0
	})
	sw.publishLeaderChange(1, node)
	d.stop()
	wg.Wait()

	if got := lastLeader(cb); got != 1 {
		t.Fatalf("leader change was lost rather than re-published: lastLeader=%d", got)
	}
}

// compile-time check that the test callback embedding stays a full Callback.
var _ Callback = (*unloadOnLeaderCallback)(nil)
