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
	"io"
	"runtime"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// closeDetectingSnapshotter stands in for a user state machine the host closes
// once UnloadNode returns. RecoverFromSnapshot announces itself on entered and
// then waits for release; whatever it finds when it resumes is recorded, so a
// restore that runs on a closed state machine is caught.
type closeDetectingSnapshotter struct {
	*snapshotTestSnapshotter
	entered        chan struct{}
	enteredOnce    sync.Once
	release        chan struct{}
	recoveries     atomic.Int64
	closed         atomic.Bool
	usedAfterClose atomic.Bool
}

func newCloseDetectingSnapshotter() *closeDetectingSnapshotter {
	return &closeDetectingSnapshotter{
		snapshotTestSnapshotter: newSnapshotTestSnapshotter(0),
		entered:                 make(chan struct{}),
		release:                 make(chan struct{}),
	}
}

func (s *closeDetectingSnapshotter) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopC <-chan struct{}) error {
	s.recoveries.Add(1)
	s.enteredOnce.Do(func() { close(s.entered) })
	<-s.release
	if s.closed.Load() {
		s.usedAfterClose.Store(true)
	}
	return s.snapshotTestSnapshotter.RecoverFromSnapshot(ctx, r, stopC)
}

// newLeaseFenceFixture is a node holding entries for an adopted snapshot at
// redriveSnapshotIndex, its close-detecting state machine, and a running pool.
func newLeaseFenceFixture(t *testing.T) (*snapshotPool, *Node, *closeDetectingSnapshotter, *testCallback) {
	t.Helper()
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	t.Cleanup(wg.Wait)
	t.Cleanup(func() { close(stopC) })

	ldb := newSnapshotTestLogDB()
	seedRecoverableSnapshot(t, ldb, 1, 1, redriveSnapshotIndex, 7, redriveSnapshotIndex)
	ss := newCloseDetectingSnapshotter()
	node, peer := newRecoveryTestNode(t, 1, 1, ss)
	installSnapshotIntoLog(t, node, peer, redriveSnapshotIndex, 7)
	cb := newTestCallback()
	pool := newSnapshotPool(1, ldb, nil, cb, NewWorkSignal(1), stopC, nil)
	pool.start(&wg)
	return pool, node, ss, cb
}

// TestSnapshotRecovery_UnloadWaitsForTheRestore: UnloadNode's write-lease
// fence must not pass while RecoverFromSnapshot is running, or the host closes
// the state machine the restore is still writing. The recovery holds the lease
// from before the recovering gate is raised until it has settled.
func TestSnapshotRecovery_UnloadWaitsForTheRestore(t *testing.T) {
	pool, node, ss, cb := newLeaseFenceFixture(t)
	if !node.TryStartSnapshot() {
		t.Fatal("snapshot slot busy")
	}
	if err := pool.requestRecovery(1, 1, node); err != nil {
		t.Fatalf("requestRecovery: %v", err)
	}
	select {
	case <-ss.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("RecoverFromSnapshot never ran")
	}

	// UnloadNode: stop the node, then wait out the write leases. If no lease
	// is held the fence passes at once and the host closes the state machine.
	node.Stop()
	fenced := node.leaseMu.TryLock()
	if fenced {
		ss.closed.Store(true)
		node.leaseMu.Unlock()
	}
	close(ss.release)
	waitRecoveryIdle(t, node)
	node.awaitWriteLeases()
	ss.closed.Store(true)

	if fenced || ss.usedAfterClose.Load() {
		t.Fatal("UnloadNode's write-lease fence passed while RecoverFromSnapshot was " +
			"running: the host closed the state machine under the restore")
	}
	// The node stopped under the restore: the recovery is abandoned, not a
	// failed shard.
	if got := cb.shardFailedCount.Load(); got != 0 {
		t.Fatalf("OnShardFailed fired %d times for a recovery its unload stopped, want 0", got)
	}
}

// TestSnapshotRecovery_StoppedNodeNeverRestores: a recovery that reaches the
// pool for a node that has already stopped — a re-drive or a receive racing
// StopShard — must not touch the state machine at all.
func TestSnapshotRecovery_StoppedNodeNeverRestores(t *testing.T) {
	pool, node, ss, cb := newLeaseFenceFixture(t)
	close(ss.release)
	node.Stop()
	if !node.TryStartSnapshot() {
		t.Fatal("snapshot slot busy")
	}
	if err := pool.requestRecovery(1, 1, node); err != nil {
		t.Fatalf("requestRecovery: %v", err)
	}
	waitRecoveryIdle(t, node)
	if got := ss.recoveries.Load(); got != 0 {
		t.Fatalf("RecoverFromSnapshot ran %d times on a stopped node, want 0", got)
	}
	if got := cb.shardFailedCount.Load(); got != 0 {
		t.Fatalf("OnShardFailed fired %d times for a stopped node, want 0", got)
	}
}

// TestNode_WaitForApplyDrain_StoppedWithNothingInFlight: the drain reports a
// stopped node as stopped even when no apply is in flight.
func TestNode_WaitForApplyDrain_StoppedWithNothingInFlight(t *testing.T) {
	node := NewNode(nil, nil, nil, config.Config{ShardID: 1, ReplicaID: 1}, nil, "", nil)
	if !node.WaitForApplyDrain(node.StopC()) {
		t.Fatal("a running node with nothing in flight did not drain")
	}
	node.Stop()
	if node.WaitForApplyDrain(node.StopC()) {
		t.Fatal("WaitForApplyDrain returned true for a stopped node with nothing in flight")
	}
}

// goexitRecoverySnapshotter calls runtime.Goexit from RecoverFromSnapshot.
type goexitRecoverySnapshotter struct {
	*snapshotTestSnapshotter
}

func (s *goexitRecoverySnapshotter) RecoverFromSnapshot(context.Context, io.Reader, <-chan struct{}) error {
	runtime.Goexit()
	return nil
}

// TestSnapshotPool_GoexitInARestoreFailsTheShardAndTheWorkerSurvives: a
// RecoverFromSnapshot that calls runtime.Goexit cannot be recovered where the
// restore runs, so whether the restore began is unknown and the state machine
// may be half-restored: the shard is failed, and the request's slots are
// released. The pool worker is restarted, so a later request is still served.
func TestSnapshotPool_GoexitInARestoreFailsTheShardAndTheWorkerSurvives(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	t.Cleanup(wg.Wait)
	t.Cleanup(func() { close(stopC) })

	ldb := newSnapshotTestLogDB()
	seedRecoverableSnapshot(t, ldb, 1, 1, redriveSnapshotIndex, 7, redriveSnapshotIndex)
	node, peer := newRecoveryTestNode(t, 1, 1, &goexitRecoverySnapshotter{newSnapshotTestSnapshotter(0)})
	installSnapshotIntoLog(t, node, peer, redriveSnapshotIndex, 7)
	cb := newTestCallback()
	pool := newSnapshotPool(1, ldb, nil, cb, NewWorkSignal(1), stopC, nil)
	pool.start(&wg)

	if !node.TryStartSnapshot() {
		t.Fatal("snapshot slot busy")
	}
	if err := pool.requestRecovery(1, 1, node); err != nil {
		t.Fatalf("requestRecovery: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for cb.shardFailedCount.Load() == 0 || node.snapshotting.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("after a Goexit in the restore: failed=%d snapshotting=%v, want the shard "+
				"failed and the slot released", cb.shardFailedCount.Load(), node.snapshotting.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A second request, for a shard with no snapshot, is still served.
	other, _ := newRecoveryTestNode(t, 2, 1, newSnapshotTestSnapshotter(0))
	if !other.TryStartSnapshot() {
		t.Fatal("snapshot slot busy")
	}
	if err := pool.requestRecovery(2, 1, other); err != nil {
		t.Fatalf("requestRecovery: %v", err)
	}
	select {
	case res := <-pool.CompletedC():
		if res.shardID != 2 {
			t.Fatalf("completed shard %d, want 2", res.shardID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the snapshot pool worker did not survive the Goexit")
	}
}

// goexitRejectCallback calls runtime.Goexit from every OnSnapshotRejected.
type goexitRejectCallback struct {
	*testCallback
}

func (c *goexitRejectCallback) OnSnapshotRejected(shardID, replicaID uint64, err error) {
	c.testCallback.OnSnapshotRejected(shardID, replicaID, err)
	runtime.Goexit()
}

// TestSnapshotPool_DrainOnStopSurvivesGoexitingCallbacks: the pool's
// shutdown drain releases every request still queued and reports each one
// rejected, and the report runs user code. A Goexit there ended the drain
// after the first request and left the rest holding their shard's snapshot
// slot for good.
func TestSnapshotPool_DrainOnStopSurvivesGoexitingCallbacks(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	cb := &goexitRejectCallback{testCallback: newTestCallback()}
	pool := newSnapshotPool(1, newSnapshotTestLogDB(), nil, cb, NewWorkSignal(1), make(chan struct{}), nil)
	pool.requestC = make(chan snapshotRequest, 8) // room for every request: no worker runs
	var nodes []*Node
	for shard := uint64(1); shard <= 3; shard++ {
		node, _ := newRecoveryTestNode(t, shard, 1, newSnapshotTestSnapshotter(0))
		if !node.TryStartSnapshot() {
			t.Fatal("snapshot slot busy")
		}
		if err := pool.requestRecovery(shard, 1, node); err != nil {
			t.Fatalf("requestRecovery: %v", err)
		}
		nodes = append(nodes, node)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.drainRequests()
	}()
	<-done
	for i, node := range nodes {
		if node.snapshotting.Load() {
			t.Fatalf("request %d still holds its shard's snapshot slot after the drain", i+1)
		}
	}
	if got := cb.snapshotRejected.Load(); got != 3 {
		t.Fatalf("OnSnapshotRejected fired %d times, want 3", got)
	}
}
