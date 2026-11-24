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

// unloadOnLeaderCallback stops the shard on a leadership change and records how long that took.
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

// TestEngine_ListenerStopsShardFromLeaderCallback_DoesNotDeadlock verifies a listener can stop its shard from a leader callback.
func TestEngine_ListenerStopsShardFromLeaderCallback_DoesNotDeadlock(t *testing.T) {
	const shardID, replicaID = 1, 1

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	cb := &unloadOnLeaderCallback{engine: e, done: make(chan struct{})}
	e.SetCallback(cb)

	// Drive the peer to leadership before the engine starts.
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

// TestNode_HandleError_DoesNotRunListenerOnCallerGoroutine verifies OnShardFailed runs on the dispatcher, not the caller.
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

// TestListenerDispatcher_PreservesProductionOrder verifies notifications arrive in production order.
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

// blockDispatcher parks the dispatcher in a notification and returns the channel that releases it.
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

// TestListenerDispatcher_FullQueueRefusesWithoutBlocking verifies dispatch never blocks on a full queue.
func TestListenerDispatcher_FullQueueRefusesWithoutBlocking(t *testing.T) {
	d := newListenerDispatcher()
	var wg sync.WaitGroup
	d.start(&wg)

	// Park the consumer before filling the queue.
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

// TestStepWorker_LeaderChangeIsRepublishedWhenQueueIsFull verifies a dropped leader change is republished.
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
