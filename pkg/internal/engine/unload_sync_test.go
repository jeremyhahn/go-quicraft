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
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// slowUnloadApplier signals when Apply starts, then holds the apply until
// release is closed, so UnloadNode is called mid-Apply, and records completion.
type slowUnloadApplier struct {
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
	completed atomic.Bool
}

func (s *slowUnloadApplier) Apply(entries []proto.Entry, results []sm.Result) error {
	s.startOnce.Do(func() { close(s.started) })
	<-s.release
	for i := range entries {
		results[i] = sm.Result{}
	}
	s.completed.Store(true)
	return nil
}

// TestEngine_UnloadNode_WaitsForInFlightApply is a regression test:
// Engine.UnloadNode must be synchronous end-to-end. A slow SM.Apply that is
// in flight when UnloadNode is called must COMPLETE before UnloadNode
// returns — otherwise the caller (Host.StopShard) closes the state machine
// while Apply is still executing user code on it (use-after-close).
func TestEngine_UnloadNode_WaitsForInFlightApply(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	e := NewEngine(cfg, nil, newTestSender(), nil, nil, nil)
	e.Start()
	defer e.Stop()

	applier := &slowUnloadApplier{started: make(chan struct{}), release: make(chan struct{})}
	// Release the applier on every exit path. Deferred after e.Stop, so it
	// runs first: an early t.Fatal must not leave Apply blocked while Stop
	// joins the apply goroutines.
	var releaseOnce sync.Once
	releaseApplier := func() { releaseOnce.Do(func() { close(applier.release) }) }
	defer releaseApplier()
	nodeCfg := config.Config{ShardID: 7, ReplicaID: 1}
	nodeCfg.SetDefaults()
	node := NewNode(nil, applier, nil, nodeCfg, nil, "", nil)
	e.LoadNode(node)

	// Feed a committed batch directly to the apply worker's channel so the
	// per-shard apply goroutine enters the slow SM.Apply.
	node.commitPending.Store(true)
	item := e.applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:          7,
		ReplicaID:        1,
		CommittedEntries: []proto.Entry{{Index: 1, Term: 1, Cmd: []byte("x")}},
	}
	item.node = node
	workerID := int(node.shardID % uint64(e.NumWorkers()))
	e.applyCs[workerID] <- item

	select {
	case <-applier.started:
	case <-time.After(5 * time.Second):
		t.Fatal("SM.Apply did not start")
	}

	// Apply is now in flight. UnloadNode must block until it completes.
	unloadDone := make(chan struct{})
	go func() {
		e.UnloadNode(7)
		close(unloadDone)
	}()
	waitFor(t, 5*time.Second, time.Millisecond, "UnloadNode never marked the node unloaded", node.unloaded.Load)
	select {
	case <-unloadDone:
		t.Fatal("UnloadNode returned while SM.Apply was still blocked")
	default:
	}
	releaseApplier()
	select {
	case <-unloadDone:
	case <-time.After(5 * time.Second):
		t.Fatal("UnloadNode did not return after SM.Apply completed")
	}

	if !applier.completed.Load() {
		t.Fatal("UnloadNode returned while SM.Apply was still in flight: " +
			"closing the state machine now would race the apply (use-after-close)")
	}
	if e.GetNode(7) != nil {
		t.Fatal("node still loaded after UnloadNode")
	}
}

// TestEngine_UnloadNode_ReleasesGateParkedApply verifies that the synchronous
// UnloadNode cannot deadlock behind an apply goroutine parked on the
// recovering/saving gate: UnloadNode calls node.Stop(), and the closed stopC
// releases the parked apply (which abandons its batch — safe, the entries are
// durable in the WAL) so the per-shard goroutine can exit and the unload
// completes.
func TestEngine_UnloadNode_ReleasesGateParkedApply(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	e := NewEngine(cfg, nil, newTestSender(), nil, nil, nil)
	e.Start()
	defer e.Stop()

	applier := &slowUnloadApplier{started: make(chan struct{})}
	nodeCfg := config.Config{ShardID: 8, ReplicaID: 1}
	nodeCfg.SetDefaults()
	node := NewNode(nil, applier, nil, nodeCfg, nil, "", nil)
	e.LoadNode(node)

	// Hold the saving gate so the apply goroutine parks BEFORE touching the
	// state machine, then deliver a batch.
	node.SetSaving(true)
	node.commitPending.Store(true)
	item := e.applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:          8,
		ReplicaID:        1,
		CommittedEntries: []proto.Entry{{Index: 1, Term: 1, Cmd: []byte("x")}},
	}
	item.node = node
	workerID := int(node.shardID % uint64(e.NumWorkers()))
	e.applyCs[workerID] <- item

	// Wait until the dispatcher has routed the item toward the per-shard
	// goroutine, which then parks on the held saving gate (applyInFlight
	// oscillates during the gate's sleep/recheck loop, so it cannot be
	// polled reliably). Observing the exact park is not required: with the
	// gate held, the ONLY way the unload below can complete is UnloadNode's
	// node.Stop() releasing the gate wait via stopC — whether the item is
	// mid-park or still buffered in the per-shard channel when the channel
	// closes.
	waitFor(t, 5*time.Second, time.Millisecond, "dispatcher did not consume the apply item",
		func() bool { return len(e.applyCs[workerID]) == 0 })

	// UnloadNode must complete despite the parked apply: node.Stop() releases
	// the gate wait via stopC.
	unloadDone := make(chan struct{})
	go func() {
		e.UnloadNode(8)
		close(unloadDone)
	}()

	select {
	case <-unloadDone:
	case <-time.After(5 * time.Second):
		t.Fatal("UnloadNode deadlocked behind an apply goroutine parked on the saving gate")
	}
	if e.GetNode(8) != nil {
		t.Fatal("node still loaded after UnloadNode")
	}
}
