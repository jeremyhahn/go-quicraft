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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

func newTestHostConfig() config.HostConfig {
	cfg := config.HostConfig{
		WALDir:               "/tmp/test-wal",
		NodeHostDir:          "/tmp/test-node",
		ListenAddress:        "127.0.0.1:0",
		RTTMillisecond:       200,
		NumWorkers:           2,
		MaxApplyWorkers:      2,
		CommitCBufferSize:    16,
		ShutdownTimeout:      5 * time.Second,
		MaxTotalInMemLogSize: 4 * 1024 * 1024 * 1024,
		DeploymentID:         1,
	}
	cfg.TransportConfig.SetDefaults()
	return cfg
}

func TestNewEngine_CreatesWorkers(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	if e.NumWorkers() != 2 {
		t.Fatalf("expected 2 workers, got %d", e.NumWorkers())
	}
	if e.StepWorker(0) == nil || e.StepWorker(1) == nil {
		t.Fatal("step workers should not be nil")
	}
	if e.CommitWorker(0) == nil || e.CommitWorker(1) == nil {
		t.Fatal("commit workers should not be nil")
	}
	if e.ApplyWorker(0) == nil || e.ApplyWorker(1) == nil {
		t.Fatal("apply workers should not be nil")
	}
	if e.SnapshotPool() == nil {
		t.Fatal("snapshot pool should not be nil")
	}
	if e.WorkSignal() == nil {
		t.Fatal("work signal should not be nil")
	}
}

func TestNewEngine_MinimumValues(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.NumWorkers = 0
	cfg.MaxApplyWorkers = 0
	cfg.CommitCBufferSize = 0
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	if e.NumWorkers() < 1 {
		t.Fatalf("expected at least 1 worker, got %d", e.NumWorkers())
	}
}

func TestEngine_UnloadNode(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	nodeCfg := config.Config{ShardID: 10, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	e.LoadNode(node)

	e.UnloadNode(10)

	workerID := int(10 % uint64(e.NumWorkers()))
	sw := e.StepWorker(workerID)
	if _, ok := sw.nodes[10]; ok {
		t.Fatal("node should be unloaded")
	}
	if got := e.GetNode(10); got != nil {
		t.Fatal("GetNode should return nil after unload")
	}
}

func TestEngine_UnloadNonexistentNode(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	// Should not panic.
	e.UnloadNode(999)
}

func TestEngine_NotifyWork_CorrectWorker(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.NumWorkers = 4
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	// Shard 7 should go to worker 7 % 4 = 3.
	e.NotifyWork(7)

	// Drain notification from the correct worker.
	ws := e.WorkSignal()
	select {
	case <-ws.WaitC(3):
		got := ws.Swap(3)
		if _, ok := got[7]; !ok {
			t.Fatal("expected shard 7 in worker 3 ready map")
		}
	case <-time.After(time.Second):
		t.Fatal("worker 3 should have received notification")
	}
}

func TestEngine_OrderedShutdown(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()

	// Load a node.
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	e.LoadNode(node)

	// Notify work and proceed directly to shutdown -- the shutdown
	// itself exercises the ordered drain path.
	e.NotifyWork(1)

	// Stop with ordered shutdown.
	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Expected: clean shutdown.
	case <-time.After(10 * time.Second):
		t.Fatal("ordered shutdown did not complete within timeout")
	}
}

func TestEngine_StopCallsNodeStop(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()

	// Load multiple nodes.
	nodes := make([]*Node, 3)
	for i := range nodes {
		nodeCfg := config.Config{ShardID: uint64(i + 1), ReplicaID: 1}
		nodes[i] = NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
		e.LoadNode(nodes[i])
	}

	// Verify nodes are not stopped yet.
	for i, n := range nodes {
		select {
		case <-n.StopC():
			t.Fatalf("node %d should not be stopped before Engine.Stop()", i+1)
		default:
		}
	}

	e.Stop()

	// After Engine.Stop(), all node stopC channels should be closed.
	for i, n := range nodes {
		select {
		case <-n.StopC():
			// Expected: node stopped.
		default:
			t.Fatalf("node %d stopC should be closed after Engine.Stop()", i+1)
		}
	}
}

func TestEngine_WorkerPartitioning(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.NumWorkers = 4
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	// Verify fixed-hash partitioning: shardID % numWorkers.
	tests := []struct {
		shardID  uint64
		workerID int
	}{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 0},
		{5, 1},
		{100, 0}, // 100 % 4 = 0
		{101, 1}, // 101 % 4 = 1
	}

	for _, tt := range tests {
		nodeCfg := config.Config{ShardID: tt.shardID, ReplicaID: 1}
		node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
		e.LoadNode(node)

		sw := e.StepWorker(tt.workerID)
		if _, ok := sw.nodes[tt.shardID]; !ok {
			t.Errorf("shard %d should be in worker %d", tt.shardID, tt.workerID)
		}
	}
}

// ---------------------------------------------------------------------------
// DeliverMessage and GetNode
// ---------------------------------------------------------------------------

func TestEngine_GetNode_Loaded(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	nodeCfg := config.Config{ShardID: 42, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	e.LoadNode(n)

	got := e.GetNode(42)
	if got == nil {
		t.Fatal("GetNode should return loaded node")
	}
	if got.ShardID() != 42 {
		t.Errorf("ShardID = %d, want 42", got.ShardID())
	}
}

func TestEngine_DeliverMessage_NotLoaded(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	msg := proto.Message{Type: proto.Heartbeat, From: 2, To: 1}
	ok := e.DeliverMessage(999, msg)
	if ok {
		t.Fatal("DeliverMessage should return false for unloaded shard")
	}
}

func TestEngine_DeliverMessage_Loaded(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	e.LoadNode(n)

	msg := proto.Message{Type: proto.Heartbeat, From: 2, To: 1}
	ok := e.DeliverMessage(1, msg)
	if !ok {
		t.Fatal("DeliverMessage should return true for loaded shard")
	}

	// Verify message is in the node's inbox.
	buf := n.DrainInbox(nil)
	if len(buf) != 1 {
		t.Fatalf("expected 1 message in inbox, got %d", len(buf))
	}
	if buf[0].Type != proto.Heartbeat {
		t.Errorf("message type = %d, want %d", buf[0].Type, proto.Heartbeat)
	}
}

func TestEngine_DeliverMessage_InboxFull(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	e.LoadNode(n)

	// Fill the inbox.
	msg := proto.Message{Type: proto.Heartbeat, From: 2, To: 1}
	for i := 0; i < defaultInboxQueueSize; i++ {
		if !e.DeliverMessage(1, msg) {
			t.Fatalf("DeliverMessage should succeed for message %d", i)
		}
	}

	// The next message should fail (inbox full).
	ok := e.DeliverMessage(1, msg)
	if ok {
		t.Fatal("DeliverMessage should return false when inbox is full")
	}
}

// ---------------------------------------------------------------------------
// Node Deliver and DrainInbox
// ---------------------------------------------------------------------------

func TestNode_DrainInbox_Multiple(t *testing.T) {
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)

	for i := 0; i < 5; i++ {
		n.Deliver(proto.Message{Type: proto.Propose, Origin: proto.OriginLocal, From: uint64(i + 1)})
	}

	// A caller-supplied buffer is reused: its capacity is preserved.
	buf := n.DrainInbox(make([]proto.Message, 0, 16))
	if cap(buf) < 16 {
		t.Errorf("buffer capacity = %d, want >= 16", cap(buf))
	}
	if len(buf) != 5 {
		t.Fatalf("DrainInbox should return 5 messages, got %d", len(buf))
	}
	for i := range buf {
		if buf[i].From != uint64(i+1) {
			t.Errorf("message[%d].From = %d, want %d", i, buf[i].From, i+1)
		}
	}

	// Second drain should be empty.
	buf = n.DrainInbox(buf)
	if len(buf) != 0 {
		t.Fatalf("second DrainInbox should return empty, got %d", len(buf))
	}
}

// ---------------------------------------------------------------------------
// GlobalRateLimiter
// ---------------------------------------------------------------------------

func TestEngine_GlobalRateLimiter(t *testing.T) {
	tests := []struct {
		name    string
		size    uint64
		wantNil bool
	}{
		{"enabled when MaxTotalInMemLogSize > 0", 4 * 1024 * 1024 * 1024, false},
		{"disabled when MaxTotalInMemLogSize is 0", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newTestHostConfig()
			cfg.MaxTotalInMemLogSize = tt.size
			e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)
			if got := e.GlobalRateLimiter() == nil; got != tt.wantNil {
				t.Fatalf("GlobalRateLimiter() == nil is %v, want %v", got, tt.wantNil)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// RequestSnapshot
// ---------------------------------------------------------------------------

func TestEngine_RequestSnapshot_WithPeer(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()
	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()
	defer e.Stop()

	peer, lr := newTestPeer(t, 1, 1)
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	nodeCfg.SetDefaults()
	node := NewNode(peer, nil, lr, nodeCfg, nil, "/tmp/snap", nil)
	e.LoadNode(node)

	err := e.RequestSnapshot(1, 1, node, SnapshotRequestOption{})
	if err != nil {
		t.Fatalf("RequestSnapshot should succeed: %v", err)
	}

	// Wait for snapshot pool to process (it will fail because no snapshotter,
	// but the request should be accepted without error).
	awaitSnapshotResult(t, e.SnapshotPool(), "snapshot")
}

// heldSnapshotter is a Snapshotter that parks the pool worker inside
// LastApplied until the test releases it. saveSnapshot consults LastApplied
// before it touches the barrier or the filesystem, so parking there pins the
// request in flight without creating any state to clean up. Returning 0 after
// the release makes the save a no-op ("nothing applied yet").
type heldSnapshotter struct {
	release <-chan struct{}
}

func (s *heldSnapshotter) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (s *heldSnapshotter) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (s *heldSnapshotter) LastApplied() uint64 {
	<-s.release
	return 0
}

var _ Snapshotter = (*heldSnapshotter)(nil)

func TestEngine_RequestSnapshot_DuplicateRejected(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()
	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()
	defer e.Stop()

	// The dedup this test asserts holds only WHILE the first request is in
	// flight: the pool worker deletes the in-flight marker as its last act.
	// With a nil snapshotter the worker rejects the request immediately, so on
	// a loaded host it could finish before the second call and the second
	// request would legitimately be accepted — the flake. Park the worker
	// inside the snapshotter instead: the marker cannot be released until this
	// test says so, and the assertion no longer depends on scheduling.
	release := make(chan struct{})
	defer close(release)

	peer, lr := newTestPeer(t, 1, 1)
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	nodeCfg.SetDefaults()
	node := NewNode(peer, nil, lr, nodeCfg, &heldSnapshotter{release: release}, t.TempDir(), nil)
	e.LoadNode(node)

	// First request should succeed.
	err := e.RequestSnapshot(1, 1, node, SnapshotRequestOption{})
	if err != nil {
		t.Fatalf("first RequestSnapshot should succeed: %v", err)
	}

	// Second request for same shard should be rejected as duplicate.
	err = e.RequestSnapshot(1, 1, node, SnapshotRequestOption{})
	if err == nil {
		t.Fatal("duplicate RequestSnapshot should return an error")
	}
	var dupErr *SnapshotDuplicateError
	if !errors.As(err, &dupErr) {
		t.Fatalf("expected SnapshotDuplicateError, got %T: %v", err, err)
	}
	if dupErr.ShardID != 1 {
		t.Fatalf("SnapshotDuplicateError.ShardID = %d, want 1", dupErr.ShardID)
	}
}

func TestEngine_RequestSnapshot_NilPeer(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()
	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()
	defer e.Stop()

	nodeCfg := config.Config{ShardID: 2, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "/tmp/snap", nil)
	e.LoadNode(node)

	// When peer is nil, RequestSnapshot should still submit (term=0).
	err := e.RequestSnapshot(2, 1, node, SnapshotRequestOption{})
	if err != nil {
		t.Fatalf("RequestSnapshot with nil peer should succeed: %v", err)
	}

	awaitSnapshotResult(t, e.SnapshotPool(), "snapshot")
}

// ---------------------------------------------------------------------------
// RequestSnapshotRecovery
// ---------------------------------------------------------------------------

func TestEngine_RequestSnapshotRecovery_Success(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()
	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()
	defer e.Stop()

	nodeCfg := config.Config{ShardID: 3, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "/tmp/snap", nil)
	e.LoadNode(node)

	err := e.RequestSnapshotRecovery(3, 1, node)
	if err != nil {
		t.Fatalf("RequestSnapshotRecovery should succeed: %v", err)
	}

	awaitSnapshotResult(t, e.SnapshotPool(), "snapshot")
}

// ---------------------------------------------------------------------------
// step-worker panic recovery and clearBatchCommitPending
// ---------------------------------------------------------------------------

// The two tests that lived here, TestEngine_DrainCommitC and
// TestEngine_DrainCommitC_NotifiesWorkSignal, asserted that a step-worker panic
// DRAINS commitC and clears commitPending on every item it wins.
//
// That behaviour was the defect, not the contract. commitC is the channel the
// LIVE commit worker consumes, and it holds up to CommitCBufferSize (64) items
// belonging to OTHER shards, so a single panic on one shard permanently failed
// up to 64 unrelated healthy shards — and which ones was a race against the
// consumer. Engine.drainCommitC is gone; see the rationale on the recovery in
// runStepWorkerOnce for why the slot-reuse hazard it claimed to prevent is
// unreachable.
//
// These tests are deleted rather than inverted because an inverted version must
// drive a REAL panic through runStepWorkerOnce to mean anything, and asserting
// on a hand-rolled stand-in is how a test ends up proving only itself. The
// behavioural replacement — an unrelated shard's queued item survives a genuine
// step-worker panic — belongs with the recovery change.

func TestCommitWorker_ClearBatchCommitPending(t *testing.T) {
	t.Run("clears commitPending for the batch only", func(t *testing.T) {
		ws := NewWorkSignal(1)
		cw := newCommitWorker(0, newTestLogDB(), nil, make(chan *commitItem, 1),
			make(chan *applyItem, 1), &sync.Pool{New: func() interface{} { return &applyItem{} }},
			ws, nil, make(chan struct{}), nil)

		inBatch := make([]*Node, 3)
		for i := range inBatch {
			nodeCfg := config.Config{ShardID: uint64(i + 1), ReplicaID: 1}
			inBatch[i] = NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
			inBatch[i].commitPending.Store(true)
			cw.batch = append(cw.batch, proto.Update{ShardID: nodeCfg.ShardID})
			cw.batchNodes = append(cw.batchNodes, inBatch[i])
		}
		outside := NewNode(nil, nil, nil, config.Config{ShardID: 9, ReplicaID: 1}, nil, "", nil)
		outside.commitPending.Store(true)

		cw.clearBatchCommitPending(ws)

		for i, n := range inBatch {
			if n.commitPending.Load() {
				t.Errorf("batch node %d should have commitPending cleared", i+1)
			}
			if !ws.HasWork(0) {
				t.Error("expected the step worker to be notified")
			}
		}
		if !outside.commitPending.Load() {
			t.Error("a node outside the batch must keep its commitPending")
		}
	})

	t.Run("tolerates nil batch node slots", func(t *testing.T) {
		cw := newCommitWorker(0, newTestLogDB(), nil, make(chan *commitItem, 1),
			make(chan *applyItem, 1), &sync.Pool{New: func() interface{} { return &applyItem{} }},
			NewWorkSignal(1), nil, make(chan struct{}), nil)
		cw.batchNodes = append(cw.batchNodes, nil)
		cw.clearBatchCommitPending(nil)
	})
}

// ---------------------------------------------------------------------------
// Double stop safety
// ---------------------------------------------------------------------------

func TestEngine_RequestSnapshotRecovery_DuplicateRejected(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()
	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	// Do NOT start the engine. Without workers draining requestC, the
	// first request stays in-flight so the second call deterministically
	// hits the per-shard dedup guard.
	defer e.Stop()

	nodeCfg := config.Config{ShardID: 4, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "/tmp/snap", nil)
	e.LoadNode(node)

	err := e.RequestSnapshotRecovery(4, 1, node)
	if err != nil {
		t.Fatalf("first RequestSnapshotRecovery should succeed: %v", err)
	}

	err = e.RequestSnapshotRecovery(4, 1, node)
	if err == nil {
		t.Fatal("duplicate RequestSnapshotRecovery should return an error")
	}
	var dupErr *SnapshotDuplicateError
	if !errors.As(err, &dupErr) {
		t.Fatalf("expected SnapshotDuplicateError, got %T: %v", err, err)
	}
}

// ---------------------------------------------------------------------------
// Worker loop panic recovery tests
// ---------------------------------------------------------------------------

// TestEngine_RunStepWorkerLoop_NormalShutdown verifies that
// runStepWorkerLoop exits cleanly when stopC is closed.
func TestEngine_RunStepWorkerLoop_NormalShutdown(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	sw := e.StepWorker(0)

	done := make(chan struct{})
	go func() {
		e.runStepWorkerLoop(sw)
		close(done)
	}()

	close(sw.stopC)
	select {
	case <-done:
		// Expected: clean exit.
	case <-time.After(5 * time.Second):
		t.Fatal("runStepWorkerLoop did not exit after stopC closed")
	}
}

// TestEngine_RunCommitWorkerLoop_NormalShutdown verifies that
// runCommitWorkerLoop exits cleanly when stopC is closed.
func TestEngine_RunCommitWorkerLoop_NormalShutdown(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	cw := e.CommitWorker(0)

	done := make(chan struct{})
	go func() {
		e.runCommitWorkerLoop(cw)
		close(done)
	}()

	close(cw.stopC)
	select {
	case <-done:
		// Expected: clean exit.
	case <-time.After(5 * time.Second):
		t.Fatal("runCommitWorkerLoop did not exit after stopC closed")
	}
}

// ---------------------------------------------------------------------------
// Panic recovery path tests using panicking commit workers
// ---------------------------------------------------------------------------

// TestEngine_RunStepWorkerOnce_PanicPath verifies that when a step
// worker panics, runStepWorkerOnce recovers and returns panicked=true.
// Also verifies clearStepCommitPending is called. We inject a panic
// by nil-ing the workSignal field, so run() panics on WaitC.
func TestEngine_RunStepWorkerOnce_PanicPath(t *testing.T) {
	logBuf, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	sw := e.StepWorker(0)

	// Load a node that was mid hand-off to the commit pipeline (commitPending
	// set, recorded as enqueuingNode) when the panic hit. This is the ONE node
	// clearStepCommitPending unwedges after a step-worker panic.
	nodeCfg := config.Config{ShardID: 0, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	node.commitPending.Store(true)
	sw.loadNode(node)
	sw.enqueuingNode = node

	// Nil the workSignal to force a panic on WaitC.
	sw.workSignal = nil

	panicked := e.runStepWorkerOnce(sw)
	if !panicked {
		t.Fatal("expected runStepWorkerOnce to report panic from nil workSignal")
	}

	// Verify panic was logged.
	logOutput := logBuf.Await("step worker panic recovered")
	if !contains(logOutput, "step worker panic recovered") {
		t.Fatalf("expected step worker panic log, got: %s", logOutput)
	}

	// Verify clearStepCommitPending cleared the enqueuing node.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared by clearStepCommitPending")
	}
}

// TestEngine_RunCommitWorkerLoop_PanicAndShutdown verifies that
// runCommitWorkerLoop handles the panic->stopC->exit sequence:
// the worker panics, the loop checks stopC and exits cleanly.
func TestEngine_RunCommitWorkerLoop_PanicAndShutdown(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	// Create a fresh commit worker with its own stopC.
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	// Make retryHookForTest panic to trigger the panic recovery.
	cw.retryHookForTest = func(_ int) {
		panic("loop panic test")
	}
	ldb.setSaveError(errors.New("disk error"))

	nodeCfg := config.Config{ShardID: 0, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	e.LoadNode(node)

	// Send a commit item that will trigger SaveState error -> retryWait -> retryHookForTest panic.
	commitC <- &commitItem{
		update: proto.Update{ShardID: 0, ReplicaID: 1, State: proto.State{Term: 1}},
		node:   node,
	}

	// Close stopC so the loop exits after the panic recovery check.
	close(stopC)

	done := make(chan struct{})
	go func() {
		e.runCommitWorkerLoop(cw)
		close(done)
	}()

	select {
	case <-done:
		// Expected: panic recovered, stopC closed, loop exits.
	case <-time.After(5 * time.Second):
		t.Fatal("runCommitWorkerLoop did not exit after panic + stopC close")
	}
}

// TestEngine_RunStepWorkerLoop_PanicAndShutdown verifies that
// runStepWorkerLoop handles the panic->stopC->exit sequence.
func TestEngine_RunStepWorkerLoop_PanicAndShutdown(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()

	e := NewEngine(cfg, ldb, newTestSender(), nil, nil, nil)
	sw := e.StepWorker(0)

	// Nil the workSignal to force a panic on WaitC.
	sw.workSignal = nil

	// Close stopC so the loop exits after the panic recovery check.
	close(sw.stopC)

	done := make(chan struct{})
	go func() {
		e.runStepWorkerLoop(sw)
		close(done)
	}()

	select {
	case <-done:
		// Expected: panic recovered, stopC closed, loop exits.
	case <-time.After(5 * time.Second):
		t.Fatal("runStepWorkerLoop did not exit after panic + stopC close")
	}
}

// goexitOnceCallback calls runtime.Goexit from the first OnReadyToRead, on the
// step worker goroutine that delivers it.
type goexitOnceCallback struct {
	*testCallback
	goexited atomic.Bool
}

func (c *goexitOnceCallback) OnReadyToRead(shardID uint64, readyToRead []proto.ReadyToRead) {
	c.testCallback.OnReadyToRead(shardID, readyToRead)
	if c.goexited.CompareAndSwap(false, true) {
		runtime.Goexit()
	}
}

// TestEngine_StepWorkerSurvivesGoexitInACallback: recover returns nil for
// runtime.Goexit, so a callback that Goexits on a worker ended the worker's
// goroutine and its panic-restart loop with it, and every shard the worker
// owned stopped making progress for good. The supervisor restarts the loop on
// a fresh goroutine, and the worker keeps processing.
func TestEngine_StepWorkerSurvivesGoexitInACallback(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()
	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	cb := &goexitOnceCallback{testCallback: newTestCallback()}
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, cb, nil)
	e.Start()
	defer e.Stop()

	peer, lr := newTestPeer(t, 1, 1)
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	nodeCfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, nodeCfg, nil, "", nil)
	e.LoadNode(node)

	key := uint64(1000)
	// Each poll that finds fewer than two confirmations issues another read.
	waitFor(t, 15*time.Second, 10*time.Millisecond, "no second ReadIndex confirmation after "+
		"the callback Goexited: the step worker did not survive it",
		func() bool {
			if cb.readyCount.Load() >= 2 {
				return true
			}
			key++
			node.DeliverReadIndex(key)
			e.NotifyWork(1)
			return false
		})
	if !cb.goexited.Load() {
		t.Fatal("fixture: the callback never Goexited")
	}
}

// goexitApplier calls runtime.Goexit from its first Apply.
type goexitApplier struct{ calls atomic.Int64 }

func (a *goexitApplier) Apply(entries []proto.Entry, results []sm.Result) error {
	if a.calls.Add(1) == 1 {
		runtime.Goexit()
	}
	for i := range entries {
		results[i] = sm.Result{}
	}
	return nil
}

// TestEngine_ShardApplyGoexitFailsTheShard: a state machine whose Apply calls
// runtime.Goexit leaves the batch's outcome unknown, exactly as a panic does,
// so the shard is failed with an *SMPanicError and the batch's proposals are
// failed. Before, recover saw nothing, the batch vanished and the shard's
// apply goroutine was gone. The goroutine is restarted, so the unload's join
// still completes.
func TestEngine_ShardApplyGoexitFailsTheShard(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()
	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	cb := newTestCallback()
	e := NewEngine(cfg, nil, newTestSender(), nil, cb, nil)
	e.Start()
	defer e.Stop()

	nodeCfg := config.Config{ShardID: 7, ReplicaID: 1}
	nodeCfg.SetDefaults()
	node := NewNode(nil, &goexitApplier{}, nil, nodeCfg, nil, "", nil)
	e.LoadNode(node)
	node.commitPending.Store(true)
	item := e.applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID:          7,
		ReplicaID:        1,
		CommittedEntries: []proto.Entry{{Index: 1, Term: 1, Key: 9, Cmd: []byte("x")}},
	}
	item.node = node
	e.applyCs[int(node.shardID%uint64(e.NumWorkers()))] <- item

	waitFor(t, 5*time.Second, 5*time.Millisecond, "a Goexit in SM.Apply never failed the shard",
		func() bool { return cb.shardFailedCount.Load() != 0 })
	cb.mu.Lock()
	failErr := cb.shardFailedCalls[0].err
	cb.mu.Unlock()
	var smPanic *SMPanicError
	if !errors.As(failErr, &smPanic) || smPanic.Value != supervise.ErrGoexit {
		t.Fatalf("shard failure = %v, want an *SMPanicError carrying supervise.ErrGoexit", failErr)
	}
	if cb.proposalFailedCount.Load() == 0 {
		t.Fatal("the batch's proposals were not failed")
	}
	unloaded := make(chan struct{})
	go func() {
		e.UnloadNode(7)
		close(unloaded)
	}()
	select {
	case <-unloaded:
	case <-time.After(5 * time.Second):
		t.Fatal("UnloadNode never joined the shard's apply goroutine")
	}
}

// TestListenerDispatcher_SurvivesGoexitInAListener: a listener that calls
// runtime.Goexit ended the dispatcher goroutine, and every later notification
// sat in the queue undelivered. The dispatcher is restarted.
func TestListenerDispatcher_SurvivesGoexitInAListener(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()
	var wg sync.WaitGroup
	d := newListenerDispatcher()
	d.start(&wg)
	defer func() {
		d.stop()
		wg.Wait()
	}()
	d.dispatch(func() { runtime.Goexit() })
	delivered := make(chan struct{})
	d.dispatch(func() { close(delivered) })
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("a notification queued after a listener Goexited was never delivered")
	}
}

// TestListenerDispatcher_StopSurvivesGoexitsInItsFinalDrain: once stop has
// begun, a listener that Goexits sends the dispatcher down its stopped path,
// whose drain runs user code too. A second Goexit there skipped the close of
// doneC, and Engine.Stop — and with it Host.Close — waited forever.
func TestListenerDispatcher_StopSurvivesGoexitsInItsFinalDrain(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()
	var wg sync.WaitGroup
	d := newListenerDispatcher()
	d.start(&wg)
	gate := make(chan struct{})
	var delivered atomic.Bool
	d.dispatch(func() {
		<-gate
		runtime.Goexit()
	})
	d.dispatch(func() { runtime.Goexit() })
	d.dispatch(func() { runtime.Goexit() })
	d.dispatch(func() { delivered.Store(true) })

	stopped := make(chan struct{})
	go func() {
		d.stop()
		wg.Wait()
		close(stopped)
	}()
	// stop has closed stopC once running is false; only then release the
	// first listener, so its Goexit lands on the stopped path.
	waitFor(t, 5*time.Second, time.Millisecond, "stop never cleared running",
		func() bool { return !d.running.Load() })
	close(gate)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stop never returned after listeners Goexited in the final drain")
	}
	if !delivered.Load() {
		t.Fatal("a notification queued behind the Goexiting listeners was never delivered")
	}
}

// hostileHandler is a slog handler — user-configurable through slog.SetDefault
// — that runs act on every record: a panic or runtime.Goexit.
type hostileHandler struct{ act func() }

func (h hostileHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h hostileHandler) Handle(context.Context, slog.Record) error {
	h.act()
	return nil
}
func (h hostileHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h hostileHandler) WithGroup(string) slog.Handler      { return h }

// installHostileLogger makes the default slog handler run act until the test
// ends.
func installHostileLogger(t *testing.T, act func()) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(hostileHandler{act: act}))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

// hostileExits are the two ways a slog handler can leave its caller.
var hostileExits = map[string]func(){
	"panic":  func() { panic("slog handler panicked") },
	"Goexit": runtime.Goexit,
}

// TestEngine_CommitRecoverySettlesBeforeLogging: a worker's recovery must
// settle everything structural before it runs any user code, logging
// included. A commit worker that logged first and whose slog handler then
// Goexited skipped its settlement: the batch's hard-state claim, commitPending
// and buffered ordered messages survived, and the next batch's save could
// release a vote response for a vote never persisted. A handler panic escaped
// the recovery altogether.
func TestEngine_CommitRecoverySettlesBeforeLogging(t *testing.T) {
	for exitName, exit := range hostileExits {
		t.Run(exitName, func(t *testing.T) {
			cfg := newTestHostConfig()
			cfg.NumWorkers = 1
			ldb := newTestLogDB()
			e := NewEngine(cfg, ldb, newTestSender(), nil, nil, nil)
			cw := e.CommitWorker(0)
			peer, lr := newTestVoterPeer(t, 1, 1, 3)
			nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
			nodeCfg.SetDefaults()
			node := NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
			if err := peer.Handle(proto.Message{Type: proto.RequestVote, From: 2, To: 1, Term: 5}); err != nil {
				t.Fatalf("Handle(RequestVote): %v", err)
			}
			update, err := peer.GetUpdate(true, 0)
			if err != nil || update.ReadOnly {
				t.Fatalf("fixture: %v", err)
			}
			node.commitPending.Store(true)
			cw.commitC <- &commitItem{node: node, update: update}
			ldb.setSaveError(errors.New("disk error"))
			cw.retryHookForTest = func(int) { panic("injected commit worker panic") }
			installHostileLogger(t, exit)

			runToAbnormalEnd(func() { e.runCommitWorkerOnce(cw) })

			if node.commitPending.Load() {
				t.Fatal("the recovery's settlement was skipped: commitPending still set")
			}
			next, err := peer.GetUpdate(true, 0)
			if err != nil {
				t.Fatalf("GetUpdate: %v", err)
			}
			if next.ReadOnly {
				t.Fatal("the recovery's settlement was skipped: the hard-state claim stands")
			}
		})
	}
}

// TestEngine_RunApplyWorkerLoop_PanicAndShutdown verifies that
// runApplyWorkerLoop handles the panic->stopC->exit sequence.
func TestEngine_RunApplyWorkerLoop_PanicAndShutdown(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)
	// Nil the shardChans to force a panic during dispatch.
	aw.shardChans = nil

	// Send an item that triggers nil map panic.
	applyC <- &applyItem{
		update: proto.Update{ShardID: 1, CommittedEntries: []proto.Entry{{Index: 1}}},
	}

	// Close stopC so the loop exits after recovery.
	close(stopC)

	done := make(chan struct{})
	go func() {
		e.runApplyWorkerLoop(aw)
		close(done)
	}()

	select {
	case <-done:
		// Expected: panic recovered, stopC closed, loop exits.
	case <-time.After(5 * time.Second):
		t.Fatal("runApplyWorkerLoop did not exit after panic + stopC close")
	}
}

// ---------------------------------------------------------------------------
// Type assertion failure paths for DeliverMessage and GetNode
// ---------------------------------------------------------------------------

// TestEngine_DeliverMessage_TypeAssertionFailure verifies that when a
// non-*Node value is stored in allNodes, DeliverMessage returns false
// and logs a TypeAssertionError.
func TestEngine_DeliverMessage_TypeAssertionFailure(t *testing.T) {
	logBuf, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	// Directly store a non-*Node value in allNodes to trigger the
	// type assertion failure path.
	e.allNodes.Store(uint64(42), "not-a-node")

	msg := proto.Message{Type: proto.Heartbeat, From: 2, To: 1}
	ok := e.DeliverMessage(42, msg)
	if ok {
		t.Fatal("DeliverMessage should return false on type assertion failure")
	}

	logOutput := logBuf.String()
	if !contains(logOutput, "node lookup type assertion failed") {
		t.Fatalf("expected type assertion error log, got: %s", logOutput)
	}
	// The log must still identify WHICH entry point hit it: DeliverMessage and
	// DeliverLocal share the lookup, and a report that cannot tell them apart
	// cannot tell a wire delivery from a local injection.
	if !contains(logOutput, "DeliverMessage node lookup") {
		t.Fatalf("expected the DeliverMessage caller context in the log, got: %s", logOutput)
	}
}

// TestEngine_DeliverLocal_TypeAssertionFailure verifies the local-injection
// entry point reports the same failure under its own name.
func TestEngine_DeliverLocal_TypeAssertionFailure(t *testing.T) {
	logBuf, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)
	e.allNodes.Store(uint64(42), "not-a-node")

	if e.DeliverLocal(42, proto.Message{Type: proto.SnapshotStatus, From: 2, To: 1}) {
		t.Fatal("DeliverLocal should return false on type assertion failure")
	}
	if !contains(logBuf.String(), "DeliverLocal node lookup") {
		t.Fatalf("expected the DeliverLocal caller context in the log, got: %s", logBuf.String())
	}
}

// TestEngine_GetNode_TypeAssertionFailure verifies that when a
// non-*Node value is stored in allNodes, GetNode returns nil and logs
// a TypeAssertionError.
func TestEngine_GetNode_TypeAssertionFailure(t *testing.T) {
	logBuf, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	// Store a wrong type.
	e.allNodes.Store(uint64(99), 12345)

	got := e.GetNode(99)
	if got != nil {
		t.Fatal("GetNode should return nil on type assertion failure")
	}

	logOutput := logBuf.String()
	if !contains(logOutput, "GetNode type assertion failed") {
		t.Fatalf("expected type assertion error log, got: %s", logOutput)
	}
}

// ---------------------------------------------------------------------------
// commit-worker panic recovery: commitPending ownership
// ---------------------------------------------------------------------------

// TestEngine_CommitWorkerPanic_ClearsOnlyBatchCommitPending verifies that a
// panicking commit worker unwedges EXACTLY the nodes whose updates died with
// its batch, and leaves every other node's commitPending alone.
//
// The step worker keeps running through a commit-worker panic. It sets
// commitPending BEFORE its commitC send, so a blanket clear over every node
// the worker owns steals ownership from an enqueue that is still in flight:
// the next processReady calls GetUpdate again while raft's processed marker
// has not advanced, extracting the SAME committed entries twice (double
// Peer.Commit, double apply, double ReleaseCommittedEntries on one pooled
// slice) and re-enqueueing the SAME pre-allocated *commitItem pointer.
func TestEngine_CommitWorkerPanic_ClearsOnlyBatchCommitPending(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	e := NewEngine(cfg, ldb, newTestSender(), nil, nil, nil)
	cw := e.CommitWorker(0)

	// batched is the node whose update is in the batch when the panic hits.
	batched := NewNode(nil, nil, nil, config.Config{ShardID: 1, ReplicaID: 1}, nil, "", nil)
	batched.commitPending.Store(true)
	e.LoadNode(batched)

	// enqueueing stands in for a node the step worker is mid-enqueue for:
	// commitPending is set, but its item has not reached commitC yet, so it
	// is in neither the channel nor the batch.
	enqueueing := NewNode(nil, nil, nil, config.Config{ShardID: 2, ReplicaID: 1}, nil, "", nil)
	enqueueing.commitPending.Store(true)
	e.LoadNode(enqueueing)

	// Drive a batch containing only `batched` and panic inside processBatch
	// (SaveState error -> retryWait -> hook).
	ldb.setSaveError(errors.New("disk error"))
	cw.retryHookForTest = func(_ int) { panic("injected commit worker panic") }
	cw.commitC <- &commitItem{
		update: proto.Update{ShardID: 1, ReplicaID: 1, State: proto.State{Term: 1}},
		node:   batched,
	}

	if panicked := e.runCommitWorkerOnce(cw); !panicked {
		t.Fatal("expected runCommitWorkerOnce to report panic")
	}

	if batched.commitPending.Load() {
		t.Fatal("batched node stayed wedged: its update died with the panicking batch")
	}
	if !enqueueing.commitPending.Load() {
		t.Fatal("commitPending was stolen from a node whose enqueue is still in flight")
	}
}

// ---------------------------------------------------------------------------
// SetSender and SetCallback after Start panics
// ---------------------------------------------------------------------------

// TestEngine_RunCommitWorkerOnce_PanicDrainsAndClearsBatch verifies the
// commit-worker panic recovery contract:
//
//   - commitC is drained BEFORE commitPending is cleared. The step worker is
//     still running; once commitPending is false it reuses the very
//     *commitItem still queued in commitC, so clearing first could enqueue
//     the same pointer twice (double Peer.Commit, double apply, double
//     release of the same pooled entry slice).
//   - the batch buffers are cleared, so the restarted worker cannot send this
//     batch's ordered messages after an unrelated later batch's fsync.
func TestEngine_RunCommitWorkerOnce_PanicDrainsAndClearsBatch(t *testing.T) {
	logBuf, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()

	e := NewEngine(cfg, ldb, newTestSender(), nil, nil, nil)
	cw := e.CommitWorker(0)
	cw.retryHookForTest = func(_ int) { panic("injected commit worker panic") }
	ldb.setSaveError(errors.New("disk error"))

	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	node.commitPending.Store(true)
	e.LoadNode(node)

	// A queued item that must be drained by the recovery.
	stale := NewNode(nil, nil, nil, config.Config{ShardID: 3, ReplicaID: 1}, nil, "", nil)
	stale.commitPending.Store(true)
	e.LoadNode(stale)
	cw.commitC <- &commitItem{update: proto.Update{ShardID: 3, ReplicaID: 1}, node: stale}

	// The item that panics, carrying an ordered message that must never be
	// sent by a later batch.
	cw.commitC <- &commitItem{
		update:      proto.Update{ShardID: 1, ReplicaID: 1, State: proto.State{Term: 1}},
		node:        node,
		orderedMsgs: []proto.Message{{Type: proto.RequestVoteResp, ShardID: 1, From: 1, To: 2}},
	}

	if !e.runCommitWorkerOnce(cw) {
		t.Fatal("expected runCommitWorkerOnce to report a panic")
	}

	if len(cw.commitC) != 0 {
		t.Fatalf("commitC must be drained by the recovery, %d items left", len(cw.commitC))
	}
	if len(cw.orderedMsgsBuf) != 0 {
		t.Fatalf("orderedMsgsBuf must be cleared by the recovery, got %d shards", len(cw.orderedMsgsBuf))
	}
	if len(cw.commitTimes) != 0 {
		t.Fatalf("commitTimes must be cleared by the recovery, got %d entries", len(cw.commitTimes))
	}
	for i := range cw.batch {
		if cw.batch[i].ShardID != 0 || len(cw.batch[i].CommittedEntries) != 0 {
			t.Fatalf("batch slot %d not cleared: %+v", i, cw.batch[i])
		}
	}
	for i := range cw.batchNodes {
		if cw.batchNodes[i] != nil {
			t.Fatalf("batchNodes slot %d not cleared", i)
		}
	}
	if node.commitPending.Load() || stale.commitPending.Load() {
		t.Fatal("commitPending must be cleared on every node owned by the worker")
	}
	// A commit-worker panic must not FAIL the batch's shards. The batch's
	// entries were never persisted, but nothing was extracted past stable
	// storage either — the restarted worker re-derives them.
	if node.IsFailed() || stale.IsFailed() {
		t.Fatal("a commit worker panic must unwedge its batch's shards, not fail them")
	}
	if logOutput := logBuf.Await("commit worker panic recovered"); !contains(logOutput, "commit worker panic recovered") {
		t.Fatalf("expected panic log message, got: %s", logOutput)
	}
}
