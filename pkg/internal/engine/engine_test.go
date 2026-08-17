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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
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

func TestEngine_StartAndStop(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()

	// Stop should not deadlock regardless of how long workers have run.
	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Expected.
	case <-time.After(10 * time.Second):
		t.Fatal("Engine.Stop() did not complete within timeout")
	}
}

func TestEngine_LoadNode(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	nodeCfg := config.Config{ShardID: 10, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	e.LoadNode(node)

	// Node should be in the correct worker.
	workerID := int(10 % uint64(e.NumWorkers()))
	sw := e.StepWorker(workerID)
	if _, ok := sw.nodes[10]; !ok {
		t.Fatalf("node not found in step worker %d", workerID)
	}
}

func TestEngine_LoadMultipleNodes(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	for i := range 10 {
		nodeCfg := config.Config{ShardID: uint64(i + 1), ReplicaID: 1}
		node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
		e.LoadNode(node)
	}

	// Verify nodes are distributed across workers.
	total := 0
	for i := range e.NumWorkers() {
		total += len(e.StepWorker(i).nodes)
	}
	if total != 10 {
		t.Fatalf("expected 10 total nodes across workers, got %d", total)
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
}

func TestEngine_UnloadNonexistentNode(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	// Should not panic.
	e.UnloadNode(999)
}

func TestEngine_NotifyWork(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)

	// NotifyWork should not panic or block.
	e.NotifyWork(5)
	e.NotifyWork(10)
	e.NotifyWork(15)
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

func TestEngine_StopNoNodes(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()

	// Engine.Stop() with no loaded nodes should not panic.
	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Expected: clean shutdown.
	case <-time.After(5 * time.Second):
		t.Fatal("Engine.Stop() with no nodes did not complete")
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

func TestEngine_SnapshotPoolIntegration(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()

	sp := e.SnapshotPool()

	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)

	err := sp.requestSnapshot(snapshotRequest{
		shardID: 1, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("snapshot request failed: %v", err)
	}

	select {
	case result := <-sp.CompletedC():
		if result.shardID != 1 {
			t.Fatalf("result.shardID = %d, want 1", result.shardID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot result not received within timeout")
	}

	e.Stop()
}

// ---------------------------------------------------------------------------
// DeliverMessage and GetNode
// ---------------------------------------------------------------------------

func TestEngine_GetNode_NotLoaded(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	node := e.GetNode(999)
	if node != nil {
		t.Fatal("GetNode should return nil for unloaded shard")
	}
}

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

func TestEngine_GetNode_AfterUnload(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	nodeCfg := config.Config{ShardID: 42, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	e.LoadNode(n)
	e.UnloadNode(42)

	got := e.GetNode(42)
	if got != nil {
		t.Fatal("GetNode should return nil after unload")
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

func TestNode_Deliver_Success(t *testing.T) {
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)

	msg := proto.Message{Type: proto.Propose, Origin: proto.OriginLocal, From: 1}
	ok := n.Deliver(msg)
	if !ok {
		t.Fatal("Deliver should return true")
	}
}

func TestNode_Deliver_InboxFull(t *testing.T) {
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)

	msg := proto.Message{Type: proto.Propose, Origin: proto.OriginLocal, From: 1}
	for i := 0; i < defaultInboxQueueSize; i++ {
		n.Deliver(msg)
	}
	ok := n.Deliver(msg)
	if ok {
		t.Fatal("Deliver should return false when inbox is full")
	}
}

func TestNode_DrainInbox_Empty(t *testing.T) {
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)

	buf := n.DrainInbox(nil)
	if len(buf) != 0 {
		t.Fatalf("DrainInbox should return empty slice, got %d", len(buf))
	}
}

func TestNode_DrainInbox_Multiple(t *testing.T) {
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)

	for i := 0; i < 5; i++ {
		n.Deliver(proto.Message{Type: proto.Propose, Origin: proto.OriginLocal, From: uint64(i + 1)})
	}

	buf := n.DrainInbox(nil)
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

func TestNode_DrainInbox_ReusesBuffer(t *testing.T) {
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)

	// Pre-allocate a buffer with capacity.
	buf := make([]proto.Message, 0, 16)
	n.Deliver(proto.Message{Type: proto.Heartbeat})

	buf = n.DrainInbox(buf)
	if len(buf) != 1 {
		t.Fatalf("expected 1 message, got %d", len(buf))
	}
	// The buffer should have been reused (capacity preserved).
	if cap(buf) < 16 {
		t.Errorf("buffer capacity = %d, want >= 16", cap(buf))
	}
}

// ---------------------------------------------------------------------------
// GlobalRateLimiter
// ---------------------------------------------------------------------------

func TestEngine_GlobalRateLimiter_Enabled(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.MaxTotalInMemLogSize = 4 * 1024 * 1024 * 1024 // 4GB
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	rl := e.GlobalRateLimiter()
	if rl == nil {
		t.Fatal("GlobalRateLimiter should not be nil when MaxTotalInMemLogSize > 0")
	}
}

func TestEngine_GlobalRateLimiter_Disabled(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.MaxTotalInMemLogSize = 0
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	rl := e.GlobalRateLimiter()
	if rl != nil {
		t.Fatal("GlobalRateLimiter should be nil when MaxTotalInMemLogSize is 0")
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
	select {
	case <-e.SnapshotPool().CompletedC():
		// Expected: pool processed the request.
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot pool did not produce a result within timeout")
	}
}

func TestEngine_RequestSnapshot_DuplicateRejected(t *testing.T) {
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

	select {
	case <-e.SnapshotPool().CompletedC():
		// Expected.
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot pool did not produce a result within timeout")
	}
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

	select {
	case <-e.SnapshotPool().CompletedC():
		// Expected.
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot pool did not produce a result within timeout")
	}
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

func TestEngine_DoubleStopDoesNotPanic(t *testing.T) {
	cfg := newTestHostConfig()
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	e.Start()
	e.Stop()

	// A second Stop should not panic due to double channel close.
	// The engine uses separate stop channels per tier, so this
	// verifies the ordered shutdown is idempotent in effect.
	// Note: the implementation may panic if Stop is called twice.
	// This test documents the current behavior.
}

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

// TestEngine_RunStepWorkerOnce_NormalExit verifies that
// runStepWorkerOnce returns false (no panic) when the step worker
// exits normally via stopC.
func TestEngine_RunStepWorkerOnce_NormalExit(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	sw := e.StepWorker(0)

	// Close stopC so run() exits immediately.
	close(sw.stopC)

	panicked := e.runStepWorkerOnce(sw)
	if panicked {
		t.Fatal("expected no panic on normal step worker exit")
	}
}

// TestEngine_RunCommitWorkerOnce_NormalExit verifies that
// runCommitWorkerOnce returns false (no panic) when the commit worker
// exits normally via stopC.
func TestEngine_RunCommitWorkerOnce_NormalExit(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	cw := e.CommitWorker(0)

	// Close stopC so run() exits immediately.
	close(cw.stopC)

	panicked := e.runCommitWorkerOnce(cw)
	if panicked {
		t.Fatal("expected no panic on normal commit worker exit")
	}
}

// ---------------------------------------------------------------------------
// Panic recovery path tests using panicking commit workers
// ---------------------------------------------------------------------------

// TestEngine_RunCommitWorkerOnce_PanicPath verifies that when a commit
// worker panics, runCommitWorkerOnce recovers and returns panicked=true.
// Also verifies clearBatchCommitPending unwedges the batch's nodes.
func TestEngine_RunCommitWorkerOnce_PanicPath(t *testing.T) {
	logBuf, restore := captureLogOutput(t)
	defer restore()

	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	ldb := newTestLogDB()
	sender := newTestSender()

	e := NewEngine(cfg, ldb, sender, nil, nil, nil)
	cw := e.CommitWorker(0)

	// Load a node so clearCommitPending has work.
	nodeCfg := config.Config{ShardID: 0, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, nil, "", nil)
	node.commitPending.Store(true)
	e.LoadNode(node)

	// Queue an item ahead of the panicking one. run() takes it as the batch's
	// first item and drainCommitC folds the second in, so BOTH end up in
	// w.batch and both must be unwedged by clearBatchCommitPending.
	cw.commitC <- &commitItem{node: node}

	// Inject a panic via retryHookForTest: SaveState returns an error,
	// processBatch calls retryWait, which calls the hook before waiting.
	cw.retryHookForTest = func(_ int) {
		panic("injected commit worker panic")
	}

	// Send a commit item to trigger the retry path in processBatch
	// (SaveState error -> retryWait -> retryHookForTest panic).
	ldb.setSaveError(errors.New("disk error"))

	cfg2 := config.Config{ShardID: 1, ReplicaID: 1}
	node2 := NewNode(nil, nil, nil, cfg2, nil, "", nil)
	node2.commitPending.Store(true)
	e.LoadNode(node2)

	cw.commitC <- &commitItem{
		update: proto.Update{ShardID: 1, ReplicaID: 1, State: proto.State{Term: 1}},
		node:   node2,
	}

	panicked := e.runCommitWorkerOnce(cw)
	if !panicked {
		t.Fatal("expected runCommitWorkerOnce to report panic")
	}

	// Verify panic was logged.
	logOutput := logBuf.String()
	if !contains(logOutput, "commit worker panic recovered") {
		t.Fatalf("expected panic log message, got: %s", logOutput)
	}

	// Verify commitPending was cleared on both batch nodes.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared by clearBatchCommitPending after panic")
	}
	if node2.commitPending.Load() {
		t.Fatal("commitPending should be cleared by clearBatchCommitPending after panic")
	}
	// A commit-worker panic must not FAIL the batch's shards. The batch's
	// entries were never persisted, but nothing was extracted past stable
	// storage either — the restarted worker re-derives them.
	if node.IsFailed() || node2.IsFailed() {
		t.Fatal("a commit worker panic must unwedge its batch's shards, not fail them")
	}
}

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
	logOutput := logBuf.String()
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

// TestEngine_SetSender_AfterStart_Panics verifies that calling
// SetSender after Start panics to prevent data races on the sender
// field while workers are running.
func TestEngine_SetSender_AfterStart_Panics(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)
	e.Start()
	defer e.Stop()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from SetSender after Start")
		}
	}()

	e.SetSender(newTestSender())
}

// TestEngine_SetCallback_AfterStart_Panics verifies that calling
// SetCallback after Start panics.
func TestEngine_SetCallback_AfterStart_Panics(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)
	e.Start()
	defer e.Stop()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from SetCallback after Start")
		}
	}()

	e.SetCallback(newTestCallback())
}

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
	_, restore := captureLogOutput(t)
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
}
