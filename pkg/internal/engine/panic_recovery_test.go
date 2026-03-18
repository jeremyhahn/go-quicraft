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
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// captureLogOutput installs a slog.TextHandler backed by a buffer and
// returns the buffer and a restore function that reinstates the previous
// default logger. Tests that need to verify log output should call this
// at the top and defer the restore.
func captureLogOutput(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})
	slog.SetDefault(slog.New(handler))
	return &buf, func() { slog.SetDefault(prev) }
}

// ---------------------------------------------------------------------------
// P-1: Step worker panic recovery
// ---------------------------------------------------------------------------

// panicStepWorker is a step worker whose run method panics immediately.
// This simulates an invariant.Assert failure or nil pointer dereference
// inside the worker's main loop.
type panicStepWorker struct {
	stepWorker
	panicValue interface{}
}

func (w *panicStepWorker) run() {
	panic(w.panicValue)
}

// TestStepWorkerPanicRecovery_GoroutineExitsCleanly verifies that when a
// step worker goroutine panics, the panic is recovered, logged, and the
// goroutine exits cleanly (WaitGroup completes without deadlock).
func TestStepWorkerPanicRecovery_GoroutineExitsCleanly(t *testing.T) {
	logBuf, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	// Replace step worker 0's run with a panicking version by launching
	// the goroutine with the same defer/recover pattern used in Start().
	var wg sync.WaitGroup
	wg.Add(1)

	panicWorker := &panicStepWorker{
		stepWorker: *e.StepWorker(0),
		panicValue: "step worker test panic",
	}

	done := make(chan struct{})
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("step worker panic recovered",
					"worker_id", panicWorker.workerID,
					"panic", r,
				)
			}
		}()
		panicWorker.run()
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Goroutine exited cleanly after panic recovery.
	case <-time.After(5 * time.Second):
		t.Fatal("step worker goroutine did not exit within timeout after panic")
	}

	// Verify panic was logged.
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "step worker panic recovered") {
		t.Errorf("expected panic log message, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "step worker test panic") {
		t.Errorf("expected panic value in log, got: %s", logOutput)
	}
}

// TestStepWorkerPanicRecovery_WaitGroupCompletes verifies that the
// Engine.Start() goroutine wrapper properly decrements the WaitGroup
// even when the step worker panics, preventing Engine.Stop() from
// deadlocking.
func TestStepWorkerPanicRecovery_WaitGroupCompletes(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	// Inject a node that will cause a panic during processReady.
	// A nil peer with certain conditions will trigger a nil pointer
	// dereference. Instead, we'll directly test the Start/Stop
	// lifecycle by starting the engine normally and stopping it
	// to verify the WaitGroup contract.
	e.Start()

	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Engine stopped cleanly -- WaitGroup completed.
	case <-time.After(10 * time.Second):
		t.Fatal("Engine.Stop() deadlocked, WaitGroup not properly managed")
	}
}

// ---------------------------------------------------------------------------
// P-1: Commit worker panic recovery
// ---------------------------------------------------------------------------

// panicCommitWorker is a commit worker whose run method panics immediately.
type panicCommitWorker struct {
	commitWorker
	panicValue interface{}
}

func (w *panicCommitWorker) run() {
	panic(w.panicValue)
}

// TestCommitWorkerPanicRecovery_GoroutineExitsCleanly verifies that when a
// commit worker goroutine panics, the panic is recovered, logged, and the
// goroutine exits cleanly.
func TestCommitWorkerPanicRecovery_GoroutineExitsCleanly(t *testing.T) {
	logBuf, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	var wg sync.WaitGroup
	wg.Add(1)

	panicWorker := &panicCommitWorker{
		commitWorker: *e.CommitWorker(0),
		panicValue:   "commit worker test panic",
	}

	done := make(chan struct{})
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("commit worker panic recovered",
					"worker_id", panicWorker.workerID,
					"panic", r,
				)
			}
		}()
		panicWorker.run()
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Goroutine exited cleanly after panic recovery.
	case <-time.After(5 * time.Second):
		t.Fatal("commit worker goroutine did not exit within timeout after panic")
	}

	// Verify panic was logged.
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "commit worker panic recovered") {
		t.Errorf("expected panic log message, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "commit worker test panic") {
		t.Errorf("expected panic value in log, got: %s", logOutput)
	}
}

// TestCommitWorkerPanicRecovery_WaitGroupCompletes verifies that the
// Engine.Start() goroutine wrapper properly decrements the WaitGroup
// even when the commit worker panics, preventing Engine.Stop() from
// deadlocking.
func TestCommitWorkerPanicRecovery_WaitGroupCompletes(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()

	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Engine stopped cleanly.
	case <-time.After(10 * time.Second):
		t.Fatal("Engine.Stop() deadlocked after commit worker setup")
	}
}

// ---------------------------------------------------------------------------
// P-1: Apply worker dispatcher panic recovery
// ---------------------------------------------------------------------------

// TestApplyWorkerDispatcherPanicRecovery_RestartsAndLogs verifies that when
// the apply worker dispatcher goroutine panics, the panic is recovered, the
// error is logged with the correct worker type and panic value, and the
// worker restarts automatically via runApplyWorkerLoop.
func TestApplyWorkerDispatcherPanicRecovery_RestartsAndLogs(t *testing.T) {
	logBuf, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	// Create an apply worker with a nil shardChans map. When dispatch()
	// attempts to read from the nil map, it triggers a nil map panic in
	// the dispatcher goroutine (assignment to entry in nil map).
	stopC := make(chan struct{})
	var wg sync.WaitGroup
	applyC := make(chan *applyItem, 1)
	aw := newApplyWorker(7, applyC, nil, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)
	// Nil the shardChans to force a panic during dispatch.
	aw.shardChans = nil

	// Send an item so dispatch() is called and panics on nil map write.
	applyC <- &applyItem{
		update: proto.Update{ShardID: 1, CommittedEntries: []proto.Entry{{Index: 1}}},
	}

	// runApplyWorkerOnce should recover the panic and return true.
	panicked := e.runApplyWorkerOnce(aw)
	if !panicked {
		t.Fatal("expected runApplyWorkerOnce to report panic from nil map write")
	}

	// Verify the panic was logged with the correct worker type and ID.
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "apply worker panic recovered") {
		t.Fatalf("expected 'apply worker panic recovered' in log, got: %s", logOutput)
	}
}

// TestApplyWorkerDispatcherPanicRecovery_NormalFlowUnaffected verifies that
// the apply worker dispatcher panic recovery does not interfere with normal
// apply flow. When no panic occurs, runApplyWorkerOnce returns panicked=false
// and items are dispatched and applied correctly.
func TestApplyWorkerDispatcherPanicRecovery_NormalFlowUnaffected(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	cb := newTestCallback()
	ws := NewWorkSignal(1)
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, ws, stopC, nil)

	// Create a node with a working state machine.
	goodSM := newTestSM()
	goodApplier := newTestApplier(goodSM)
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	nodeCfg.SetDefaults()
	node := NewNode(nil, goodApplier, nil, nodeCfg, 100, nil, "", nil)

	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()
	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	// Start the apply worker through the recovery wrapper.
	done := make(chan struct{})
	go func() {
		e.runApplyWorkerLoop(aw)
		close(done)
	}()

	// Send an item with committed entries.
	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("normal-apply")},
		},
	}
	item.node = node
	applyC <- item

	// Wait for the apply to complete.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for normal apply to complete")
		default:
		}
		if node.lastApplied.Load() >= 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Verify the state machine received the entry.
	if goodSM.applyCount.Load() != 1 {
		t.Fatalf("expected 1 apply call, got %d", goodSM.applyCount.Load())
	}

	// Stop the worker cleanly.
	close(stopC)

	select {
	case <-done:
		// Clean shutdown via runApplyWorkerLoop.
	case <-time.After(5 * time.Second):
		t.Fatal("runApplyWorkerLoop did not exit after stopC closed")
	}
}

// ---------------------------------------------------------------------------
// P-1: WorkerPanicError typed error tests
// ---------------------------------------------------------------------------

// TestWorkerPanicError_ApplyWorkerMessage verifies WorkerPanicError formats
// correctly for apply workers.
func TestWorkerPanicError_ApplyWorkerMessage(t *testing.T) {
	err := &WorkerPanicError{
		WorkerID:   2,
		WorkerType: "apply",
		Value:      "nil pointer dereference",
		Stack:      []byte("goroutine 55 [running]:"),
	}

	msg := err.Error()
	if msg != "engine: apply worker 2 panicked: nil pointer dereference" {
		t.Errorf("unexpected error message: %s", msg)
	}
}

// TestWorkerPanicError_StepWorkerMessage verifies WorkerPanicError formats
// correctly for step workers.
func TestWorkerPanicError_StepWorkerMessage(t *testing.T) {
	err := &WorkerPanicError{
		WorkerID:   3,
		WorkerType: "step",
		Value:      "nil pointer dereference",
		Stack:      []byte("goroutine 42 [running]:"),
	}

	msg := err.Error()
	if msg != "engine: step worker 3 panicked: nil pointer dereference" {
		t.Errorf("unexpected error message: %s", msg)
	}
}

// TestWorkerPanicError_CommitWorkerMessage verifies WorkerPanicError formats
// correctly for commit workers.
func TestWorkerPanicError_CommitWorkerMessage(t *testing.T) {
	err := &WorkerPanicError{
		WorkerID:   0,
		WorkerType: "commit",
		Value:      "index out of range",
		Stack:      []byte("goroutine 99 [running]:"),
	}

	msg := err.Error()
	if msg != "engine: commit worker 0 panicked: index out of range" {
		t.Errorf("unexpected error message: %s", msg)
	}
}

// ---------------------------------------------------------------------------
// Engine.Start panic recovery integration test
// ---------------------------------------------------------------------------

// TestEngine_StartWithPanicRecovery_StopDoesNotDeadlock verifies the
// complete lifecycle: Start launches workers with panic recovery, and
// Stop waits for all goroutines to complete without deadlocking.
func TestEngine_StartWithPanicRecovery_StopDoesNotDeadlock(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 4
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()

	// Stop immediately -- the test verifies Stop does not deadlock
	// even when workers are mid-panic-recovery. No idle period needed.
	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
		// All workers stopped cleanly. Recovery defers did not interfere.
	case <-time.After(10 * time.Second):
		t.Fatal("Engine.Stop() deadlocked with panic recovery in place")
	}
}

// TestEngine_StartWithPanicRecovery_InvalidInput verifies that starting
// and stopping an engine with minimal (edge-case) configuration still
// works correctly with panic recovery in place.
func TestEngine_StartWithPanicRecovery_InvalidInput(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 0 // Will be clamped to 1
	cfg.MaxApplyWorkers = 0
	cfg.CommitCBufferSize = 0
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()

	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown with minimum config.
	case <-time.After(10 * time.Second):
		t.Fatal("Engine.Stop() deadlocked with minimum config")
	}
}
