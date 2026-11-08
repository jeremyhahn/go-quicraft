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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// gatedFailingLogDB fails the snapshot-record read a recovery makes under the
// recovering gate — once per recovery attempt — for the first failures
// attempts, and counts every attempt.
type gatedFailingLogDB struct {
	*snapshotTestLogDB
	node     *Node
	failures int64
	// panics makes a failing attempt panic instead of returning an error.
	panics   bool
	attempts atomic.Int64
}

func (db *gatedFailingLogDB) GetSnapshot(shardID, replicaID uint64) (logdb.Snapshot, error) {
	if db.node.IsRecovering() {
		if n := db.attempts.Add(1); n <= db.failures {
			if db.panics {
				panic("logdb panic under the recovering gate")
			}
			return logdb.Snapshot{}, errSnapshotRecordRead
		}
	}
	return db.snapshotTestLogDB.GetSnapshot(shardID, replicaID)
}

// redriveFixture is a node whose raft log adopted a snapshot at index 100
// that its state machine has not restored, so it is holding entries, plus a
// running snapshot pool and a step worker wired to it.
type redriveFixture struct {
	pool *snapshotPool
	sw   *stepWorker
	node *Node
	db   *gatedFailingLogDB
	cb   *testCallback
}

const redriveSnapshotIndex = uint64(100)

func newRedriveFixture(t *testing.T, failures int64) *redriveFixture {
	t.Helper()
	return newRedriveFixtureWith(t, &gatedFailingLogDB{failures: failures})
}

func newRedriveFixtureWith(t *testing.T, db *gatedFailingLogDB) *redriveFixture {
	t.Helper()
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)

	var wg sync.WaitGroup
	stopC := make(chan struct{})
	// LIFO: close stopC first, then wait for the pool workers.
	t.Cleanup(wg.Wait)
	t.Cleanup(func() { close(stopC) })

	base := newSnapshotTestLogDB()
	seedRecoverableSnapshot(t, base, 1, 1, redriveSnapshotIndex, 7, redriveSnapshotIndex)
	node, peer := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))
	installSnapshotIntoLog(t, node, peer, redriveSnapshotIndex, 7)
	if !node.snapshotAdoptionPending() {
		t.Fatal("fixture: the node is not holding entries for the adopted snapshot")
	}

	db.snapshotTestLogDB, db.node = base, node
	f := &redriveFixture{node: node, db: db, cb: newTestCallback()}
	f.pool = newSnapshotPool(1, f.db, nil, f.cb, NewWorkSignal(1), stopC, nil)
	f.pool.start(&wg)
	f.sw = newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), nil, f.cb, make(chan struct{}), nil)
	f.sw.snapshots = f.pool

	// The recovery the snapshot receive queues.
	if !node.TryStartSnapshot() {
		t.Fatal("fixture: snapshot slot busy")
	}
	if err := f.pool.requestRecovery(1, 1, node); err != nil {
		t.Fatalf("requestRecovery: %v", err)
	}
	return f
}

// drive runs the step worker's re-drive check every few milliseconds, as its
// cycles would, until done reports true or the deadline passes.
func (f *redriveFixture) drive(t *testing.T, deadline time.Duration, done func() bool) {
	t.Helper()
	defer func() {
		if t.Failed() {
			t.Logf("attempts=%d rejected=%d failed=%v lastApplied=%d",
				f.db.attempts.Load(), f.cb.snapshotRejected.Load(), f.node.IsFailed(), f.node.LastApplied())
		}
	}()
	waitFor(t, deadline, 5*time.Millisecond, "re-drive condition not reached", func() bool {
		if done() {
			return true
		}
		f.sw.redriveRecovery(f.node)
		return false
	})
}

// TestRecoveryRedrive_TransientFailureSelfHeals: a recovery rejected before
// the restore by a failure that clears on its own is re-driven by the engine
// with no outside help, and succeeds without the shard ever latching.
func TestRecoveryRedrive_TransientFailureSelfHeals(t *testing.T) {
	f := newRedriveFixture(t, 2)
	f.drive(t, 10*time.Second, func() bool {
		return f.node.LastApplied() == redriveSnapshotIndex || f.node.IsFailed()
	})
	waitRecoveryIdle(t, f.node)

	if f.node.IsFailed() {
		t.Fatal("a transient pre-restore failure latched the shard")
	}
	if got := f.db.attempts.Load(); got != 3 {
		t.Fatalf("recovery attempts = %d, want 3 (two rejected, one restored)", got)
	}
	if got := f.cb.snapshotRejected.Load(); got != 2 {
		t.Fatalf("OnSnapshotRejected fired %d times, want 2", got)
	}
	if got := f.cb.shardFailedCount.Load(); got != 0 {
		t.Fatalf("OnShardFailed fired %d times, want 0", got)
	}
	f.cb.mu.Lock()
	causes := append([]error(nil), f.cb.rejectedCauses...)
	f.cb.mu.Unlock()
	for i, cause := range causes {
		if !errors.Is(cause, errSnapshotRecordRead) {
			t.Fatalf("rejection %d cause = %v, want the LogDB read failure", i, cause)
		}
	}
	f.node.redrive.mu.Lock()
	armed, rejected := f.node.redrive.armed.Load(), f.node.redrive.rejected
	f.node.redrive.mu.Unlock()
	if armed || rejected != 0 {
		t.Fatalf("re-drive state after a successful recovery: armed=%v rejected=%d, want cleared",
			armed, rejected)
	}
}

// TestRecoveryRedrive_DeterministicFailureLatchesAfterLastAttempt: a failure
// that never clears is re-driven until maxRecoveryAttempts attempts have been
// rejected, then the shard is failed once, with an error that names the
// attempts and carries the last cause, and nothing is re-driven after that.
func TestRecoveryRedrive_DeterministicFailureLatchesAfterLastAttempt(t *testing.T) {
	f := newRedriveFixture(t, 1<<30)
	f.drive(t, 15*time.Second, f.node.IsFailed)
	waitRecoveryIdle(t, f.node)

	// Nothing more is re-driven once the shard has failed.
	for range 20 {
		f.sw.redriveRecovery(f.node)
		// Kept: spaced step-worker cycles are the window in which no re-drive may start.
		time.Sleep(10 * time.Millisecond)
	}
	if got := f.db.attempts.Load(); got != maxRecoveryAttempts {
		t.Fatalf("recovery attempts = %d, want exactly %d", got, maxRecoveryAttempts)
	}
	if got := f.cb.snapshotRejected.Load(); got != maxRecoveryAttempts-1 {
		t.Fatalf("OnSnapshotRejected fired %d times, want %d (the last attempt fails the shard instead)",
			got, maxRecoveryAttempts-1)
	}
	if got := f.cb.shardFailedCount.Load(); got != 1 {
		t.Fatalf("OnShardFailed fired %d times, want exactly 1", got)
	}
	f.cb.mu.Lock()
	failErr := f.cb.shardFailedCalls[0].err
	f.cb.mu.Unlock()
	var exhausted *SnapshotRecoveryExhaustedError
	if !errors.As(failErr, &exhausted) {
		t.Fatalf("OnShardFailed error = %T (%v), want *SnapshotRecoveryExhaustedError", failErr, failErr)
	}
	if exhausted.Attempts != maxRecoveryAttempts || exhausted.SnapshotIndex != redriveSnapshotIndex {
		t.Fatalf("exhausted error = %+v, want %d attempts at index %d",
			exhausted, maxRecoveryAttempts, redriveSnapshotIndex)
	}
	if !errors.Is(failErr, errSnapshotRecordRead) {
		t.Fatalf("OnShardFailed error %v does not carry the last cause", failErr)
	}
}

// TestRecoveryRedrive_PreRestorePanicTakesTheSamePath: a recovery that panics
// before the restore is re-driven and, once the attempts are exhausted, fails
// the shard exactly as one rejected with an error does, carrying the panic as
// the last cause.
func TestRecoveryRedrive_PreRestorePanicTakesTheSamePath(t *testing.T) {
	f := newRedriveFixtureWith(t, &gatedFailingLogDB{failures: 1 << 30, panics: true})
	f.drive(t, 15*time.Second, f.node.IsFailed)
	waitRecoveryIdle(t, f.node)

	if got := f.db.attempts.Load(); got != maxRecoveryAttempts {
		t.Fatalf("recovery attempts = %d, want exactly %d", got, maxRecoveryAttempts)
	}
	if got := f.cb.snapshotRejected.Load(); got != maxRecoveryAttempts-1 {
		t.Fatalf("OnSnapshotRejected fired %d times, want %d", got, maxRecoveryAttempts-1)
	}
	if got := f.cb.shardFailedCount.Load(); got != 1 {
		t.Fatalf("OnShardFailed fired %d times, want exactly 1", got)
	}
	f.cb.mu.Lock()
	failErr := f.cb.shardFailedCalls[0].err
	f.cb.mu.Unlock()
	var panicErr *SnapshotRecoveryPanicError
	if !errors.As(failErr, &panicErr) {
		t.Fatalf("OnShardFailed error = %v, want the last attempt's *SnapshotRecoveryPanicError", failErr)
	}
}

// TestRecoveryRedrive_StopDuringBackoffLeaksNothing: a node that stops while a
// re-drive is owed is never re-driven, and the re-drive holds no goroutine or
// timer of its own: the step worker's cycles are its only clock.
func TestRecoveryRedrive_StopDuringBackoffLeaksNothing(t *testing.T) {
	f := newRedriveFixture(t, 1<<30)
	f.drive(t, 10*time.Second, func() bool { return f.cb.snapshotRejected.Load() == 1 })
	waitRecoveryIdle(t, f.node)
	if !f.node.redrive.pending() {
		t.Fatal("fixture: no re-drive owed after the first rejection")
	}
	before := runtime.NumGoroutine()

	f.node.Stop()
	// Well past the first backoff.
	deadline := time.Now().Add(3 * recoveryRedriveBaseBackoff)
	for time.Now().Before(deadline) {
		f.sw.redriveRecovery(f.node)
		// Kept: the backoff must elapse in real time; the cycles through it are the test.
		time.Sleep(5 * time.Millisecond)
	}

	if got := f.db.attempts.Load(); got != 1 {
		t.Fatalf("recovery attempts after the node stopped = %d, want 1", got)
	}
	if !f.node.redrive.pending() {
		t.Fatal("a stopped node's owed re-drive was queued")
	}
	if f.node.snapshotting.Load() {
		t.Fatal("a stopped node's snapshot slot was claimed by a re-drive")
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines grew from %d to %d across a stopped node's backoff", before, after)
	}
}

// TestRecoveryRedrive_CountSpansTheHoldingEpisode: attempts count across the
// whole continuous episode of holding entries, whatever snapshot each attempt
// was made against, so a persistent pre-restore fault cannot be kept alive by
// a stream of newer snapshots. Only reset (a completed recovery, or a node no
// longer holding entries) starts again. A restarted shard is a new Node, whose
// zero-value state counts from nothing.
func TestRecoveryRedrive_CountSpansTheHoldingEpisode(t *testing.T) {
	var r recoveryRedrive
	now := time.Now()
	for i := 1; i < maxRecoveryAttempts; i++ {
		if got, exhausted := r.reject(now); got != i || exhausted {
			t.Fatalf("rejection %d = (%d, %v), want (%d, false)", i, got, exhausted, i)
		}
	}
	if got, exhausted := r.reject(now); got != maxRecoveryAttempts || !exhausted {
		t.Fatalf("rejection %d = (%d, %v), want (%d, true)",
			maxRecoveryAttempts, got, exhausted, maxRecoveryAttempts)
	}
	r.reset()
	if got, exhausted := r.reject(now); got != 1 || exhausted {
		t.Fatalf("first rejection after reset = (%d, %v), want (1, false)", got, exhausted)
	}
	var fresh recoveryRedrive
	if fresh.pending() {
		t.Fatal("a new node's re-drive state owes a re-drive")
	}
}

// TestRecoveryRedrive_NewerSnapshotsDoNotResetTheBudget: a replica whose
// pre-restore fault persists while newer snapshots keep being adopted is
// failed after maxRecoveryAttempts rejections in all, and the error reports
// the latest snapshot index attempted.
func TestRecoveryRedrive_NewerSnapshotsDoNotResetTheBudget(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	node, peer := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))
	cb := newTestCallback()
	p := &snapshotPool{callback: cb}
	req := snapshotRequest{shardID: 1, replicaID: 1, node: node}
	var index uint64
	for i := 1; i <= maxRecoveryAttempts; i++ {
		index = redriveSnapshotIndex * uint64(i)
		installSnapshotIntoLog(t, node, peer, index, 7)
		exhausted := p.settleRejectedRecovery(req, errSnapshotRecordRead)
		if want := i == maxRecoveryAttempts; exhausted != want {
			t.Fatalf("rejection %d at snapshot %d: exhausted = %v, want %v", i, index, exhausted, want)
		}
	}
	if !node.IsFailed() || cb.shardFailedCount.Load() != 1 {
		t.Fatalf("failed = %v, OnShardFailed = %d; want the shard failed once",
			node.IsFailed(), cb.shardFailedCount.Load())
	}
	cb.mu.Lock()
	failErr := cb.shardFailedCalls[0].err
	cb.mu.Unlock()
	var ex *SnapshotRecoveryExhaustedError
	if !errors.As(failErr, &ex) {
		t.Fatalf("OnShardFailed error = %T, want *SnapshotRecoveryExhaustedError", failErr)
	}
	if ex.Attempts != maxRecoveryAttempts || ex.SnapshotIndex != index {
		t.Fatalf("exhausted error = %+v, want %d attempts, latest index %d", ex, maxRecoveryAttempts, index)
	}
}

// TestRecoveryRedrive_PoolBusyDoesNotStorm: a re-drive the pool cannot admit
// (its request queue is full) is retried after a backoff, consumes no attempt,
// and publishes nothing: no OnSnapshotRejected per scheduling retry, however
// many step cycles pass.
func TestRecoveryRedrive_PoolBusyDoesNotStorm(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	node, peer := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))
	installSnapshotIntoLog(t, node, peer, redriveSnapshotIndex, 7)
	cb := newTestCallback()
	// No workers: the one-slot request queue, once filled, stays full.
	pool := newSnapshotPool(1, newSnapshotTestLogDB(), nil, cb, NewWorkSignal(1), make(chan struct{}), nil)
	pool.requestC = make(chan snapshotRequest, 1)
	pool.requestC <- snapshotRequest{shardID: 99, replicaID: 1}
	sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), nil, cb, make(chan struct{}), nil)
	sw.snapshots = pool

	start := time.Now()
	node.redrive.reject(start.Add(-time.Hour))
	if !node.redrive.due(start) {
		t.Fatal("fixture: re-drive not due")
	}
	for range 10000 {
		sw.redriveRecovery(node)
	}

	if got := cb.snapshotRejected.Load(); got != 0 {
		t.Fatalf("OnSnapshotRejected fired %d times for pool-busy re-drive retries, want 0", got)
	}
	node.redrive.mu.Lock()
	rejected, armed, dueAt := node.redrive.rejected, node.redrive.armed.Load(), node.redrive.dueAt
	node.redrive.mu.Unlock()
	if rejected != 1 {
		t.Fatalf("rejected = %d after pool-busy retries, want 1 (admission failures consume no attempt)", rejected)
	}
	if !armed {
		t.Fatal("a re-drive the pool could not admit was dropped")
	}
	if !dueAt.After(start) {
		t.Fatalf("re-drive due at %v, not backed off past %v", dueAt, start)
	}
	if node.snapshotting.Load() {
		t.Fatal("a refused re-drive kept the snapshot slot")
	}
}

// rejectingCallback runs onRejected inside OnSnapshotRejected, on whatever
// goroutine reports the rejection.
type rejectingCallback struct {
	*testCallback
	onRejected func()
}

func (c *rejectingCallback) OnSnapshotRejected(shardID, replicaID uint64, err error) {
	c.testCallback.OnSnapshotRejected(shardID, replicaID, err)
	c.onRejected()
}

// TestRecoveryRedrive_FastRejectionIsNotDisarmed: a re-drive's attempt is
// claimed before the request is published, so a rejection that lands after the
// publish but before the submitting step worker resumes arms the next re-drive,
// and the submitter cannot wipe it. Were the claim made after the publish, the
// node would hold entries with no re-drive owed, forever.
//
// The interleaving is made deterministic through submit's own shutdown drain:
// with the pool stopped, the submitter itself pulls the request it just
// published back out and reports it rejected, between the publish and its
// return, and the report records the rejection the worker would have.
func TestRecoveryRedrive_FastRejectionIsNotDisarmed(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	node, peer := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))
	installSnapshotIntoLog(t, node, peer, redriveSnapshotIndex, 7)
	cb := &rejectingCallback{testCallback: newTestCallback()}
	cb.onRejected = func() { node.redrive.reject(time.Now()) }
	poolStopC := make(chan struct{})
	close(poolStopC)
	pool := newSnapshotPool(1, newSnapshotTestLogDB(), nil, cb, NewWorkSignal(1), poolStopC, nil)
	sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), nil, cb, make(chan struct{}), nil)
	sw.snapshots = pool

	node.redrive.reject(time.Now().Add(-time.Hour))
	sw.redriveRecovery(node)

	if got := cb.snapshotRejected.Load(); got != 1 {
		t.Fatalf("fixture: the published request was rejected %d times, want 1", got)
	}
	if !node.redrive.pending() {
		t.Fatal("a rejection that landed after the re-drive was published re-armed it, " +
			"and the submitter then disarmed it: the node holds entries with no re-drive owed")
	}
}

// TestRecoveryRedriveBackoff pins the schedule: doubling from the base, capped.
func TestRecoveryRedriveBackoff(t *testing.T) {
	want := []time.Duration{
		100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
		800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond,
		5 * time.Second, 5 * time.Second,
	}
	for i, w := range want {
		if got := recoveryRedriveBackoff(i + 1); got != w {
			t.Fatalf("backoff after %d rejections = %v, want %v", i+1, got, w)
		}
	}
	r := recoveryRedrive{}
	now := time.Now()
	r.reject(now)
	if r.due(now.Add(recoveryRedriveBaseBackoff - time.Millisecond)) {
		t.Fatal("re-drive due before its backoff elapsed")
	}
	if !r.due(now.Add(recoveryRedriveBaseBackoff)) {
		t.Fatal("re-drive not due once its backoff elapsed")
	}
}

// waitRecoveryIdle waits until the node's snapshot slot is free, so a
// recovery's settle (which reports before releasing the slot) has finished.
func waitRecoveryIdle(t *testing.T, node *Node) {
	t.Helper()
	waitFor(t, 5*time.Second, time.Millisecond, "the recovery never released the snapshot slot", func() bool {
		return !node.snapshotting.Load() && !node.IsRecovering()
	})
}

// BenchmarkStepWorker_RedriveRecoveryIdle measures the re-drive check the step
// worker makes for every node on every cycle, in the common case: nothing is
// owed. It must stay a single atomic load.
func BenchmarkStepWorker_RedriveRecoveryIdle(b *testing.B) {
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	defer wg.Wait()
	defer close(stopC)
	pool := newSnapshotPool(1, newSnapshotTestLogDB(), nil, newTestCallback(), NewWorkSignal(1), stopC, nil)
	pool.start(&wg)
	sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), nil, newTestCallback(), make(chan struct{}), nil)
	sw.snapshots = pool
	node := &Node{shardID: 1, replicaID: 1}
	b.ReportAllocs()
	for b.Loop() {
		sw.redriveRecovery(node)
	}
}
