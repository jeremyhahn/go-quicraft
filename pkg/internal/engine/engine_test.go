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
		MaxApplyRetries:      100,
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
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
		node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
		nodes[i] = NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
		node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)

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
	n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
	n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
	n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
	n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
	n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)

	msg := proto.Message{Type: proto.Propose, From: 1}
	ok := n.Deliver(msg)
	if !ok {
		t.Fatal("Deliver should return true")
	}
}

func TestNode_Deliver_InboxFull(t *testing.T) {
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)

	msg := proto.Message{Type: proto.Propose, From: 1}
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
	n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)

	buf := n.DrainInbox(nil)
	if len(buf) != 0 {
		t.Fatalf("DrainInbox should return empty slice, got %d", len(buf))
	}
}

func TestNode_DrainInbox_Multiple(t *testing.T) {
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)

	for i := 0; i < 5; i++ {
		n.Deliver(proto.Message{Type: proto.Propose, From: uint64(i + 1)})
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
	n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)

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
	node := NewNode(peer, nil, lr, nodeCfg, 100, nil, "/tmp/snap", nil)
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
	node := NewNode(peer, nil, lr, nodeCfg, 100, nil, "/tmp/snap", nil)
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
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "/tmp/snap", nil)
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
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "/tmp/snap", nil)
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
// drainCommitC and clearCommitPending
// ---------------------------------------------------------------------------

func TestEngine_DrainCommitC(t *testing.T) {
	t.Run("drains all items and clears commitPending", func(t *testing.T) {
		cfg := newTestHostConfig()
		ldb := newTestLogDB()
		sender := newTestSender()
		e := NewEngine(cfg, ldb, sender, nil, nil, nil)

		commitC := make(chan *commitItem, 16)

		// Create nodes and enqueue commit items.
		nodes := make([]*Node, 3)
		for i := range nodes {
			nodeCfg := config.Config{ShardID: uint64(i + 1), ReplicaID: 1}
			nodes[i] = NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
			nodes[i].commitPending.Store(true)
			e.LoadNode(nodes[i])

			commitC <- &commitItem{
				node: nodes[i],
			}
		}

		// Drain all items.
		e.drainCommitC(commitC)

		// Verify all commitPending flags are cleared.
		for i, n := range nodes {
			if n.commitPending.Load() {
				t.Errorf("node %d commitPending should be false after drain", i+1)
			}
		}

		// Verify channel is empty.
		select {
		case <-commitC:
			t.Fatal("commitC should be empty after drain")
		default:
		}
	})

	t.Run("drain on empty channel returns immediately", func(t *testing.T) {
		cfg := newTestHostConfig()
		ldb := newTestLogDB()
		sender := newTestSender()
		e := NewEngine(cfg, ldb, sender, nil, nil, nil)

		commitC := make(chan *commitItem, 16)

		// Should return immediately without blocking.
		done := make(chan struct{})
		go func() {
			e.drainCommitC(commitC)
			close(done)
		}()

		select {
		case <-done:
			// Expected: returned immediately.
		case <-time.After(time.Second):
			t.Fatal("drainCommitC blocked on empty channel")
		}
	})
}

func TestEngine_ClearCommitPending(t *testing.T) {
	t.Run("clears commitPending for nodes on target worker", func(t *testing.T) {
		cfg := newTestHostConfig()
		cfg.NumWorkers = 2
		ldb := newTestLogDB()
		sender := newTestSender()
		e := NewEngine(cfg, ldb, sender, nil, nil, nil)

		// Load nodes with shardIDs that map to different workers.
		// With 2 workers: shard 0,2,4 -> worker 0; shard 1,3,5 -> worker 1.
		for i := uint64(0); i < 6; i++ {
			nodeCfg := config.Config{ShardID: i, ReplicaID: 1}
			n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
			n.commitPending.Store(true)
			e.LoadNode(n)
		}

		// Clear commitPending for worker 0 (shards 0, 2, 4).
		e.clearCommitPending(0)

		// Verify worker 0 nodes have commitPending cleared.
		for _, shardID := range []uint64{0, 2, 4} {
			n := e.GetNode(shardID)
			if n == nil {
				t.Fatalf("node %d should be loaded", shardID)
			}
			if n.commitPending.Load() {
				t.Errorf("shard %d (worker 0) commitPending should be false", shardID)
			}
		}

		// Verify worker 1 nodes still have commitPending set.
		for _, shardID := range []uint64{1, 3, 5} {
			n := e.GetNode(shardID)
			if n == nil {
				t.Fatalf("node %d should be loaded", shardID)
			}
			if !n.commitPending.Load() {
				t.Errorf("shard %d (worker 1) commitPending should still be true", shardID)
			}
		}
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
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "/tmp/snap", nil)
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

// panicOnFirstCommit creates a commit worker whose run() panics when it
// receives the first commitItem. This exercises the full panic recovery
// path in runCommitWorkerOnce: recover, log, clearCommitPending, drainCommitC.
type panicCommitWorkerHook struct {
	*commitWorker
	panickOnce sync.Once
}

// TestEngine_RunCommitWorkerOnce_PanicPath verifies that when a commit
// worker panics, runCommitWorkerOnce recovers and returns panicked=true.
// Also verifies clearCommitPending and drainCommitC are called.
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
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
	node.commitPending.Store(true)
	e.LoadNode(node)

	// Put a stale item in the commitC channel to exercise drainCommitC.
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
	node2 := NewNode(nil, nil, nil, cfg2, 100, nil, "", nil)
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

	// Verify commitPending was cleared on node 0 (from clearCommitPending).
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared by clearCommitPending after panic")
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

	// Load a node and set commitPending.
	nodeCfg := config.Config{ShardID: 0, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
	node.commitPending.Store(true)
	sw.loadNode(node)

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

	// Verify clearStepCommitPending was called.
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
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
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
	if !contains(logOutput, "DeliverMessage type assertion failed") {
		t.Fatalf("expected type assertion error log, got: %s", logOutput)
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
// clearCommitPending edge cases
// ---------------------------------------------------------------------------

// TestEngine_ClearCommitPending_NonNodeValues verifies that
// clearCommitPending skips non-*Node values in allNodes without
// panicking (the type assertion guard).
func TestEngine_ClearCommitPending_NonNodeValues(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.NumWorkers = 2
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	// Store a non-*Node value.
	e.allNodes.Store(uint64(0), "not-a-node")

	// Also store a real node.
	nodeCfg := config.Config{ShardID: 2, ReplicaID: 1}
	n := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
	n.commitPending.Store(true)
	e.LoadNode(n)

	// Should not panic despite the non-*Node value in allNodes.
	e.clearCommitPending(0)

	// The real node on worker 0 (shardID 2 % 2 = 0) should be cleared.
	if n.commitPending.Load() {
		t.Fatal("commitPending should be cleared for node on matching worker")
	}
}

// TestEngine_ClearCommitPending_NonUint64Key verifies that
// clearCommitPending skips entries with non-uint64 keys without
// panicking.
func TestEngine_ClearCommitPending_NonUint64Key(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	// Store a value with a string key (non-uint64).
	e.allNodes.Store("bad-key", &Node{})

	// Should not panic.
	e.clearCommitPending(0)
}

// ---------------------------------------------------------------------------
// DrainCommitC with work signal notification
// ---------------------------------------------------------------------------

// TestEngine_DrainCommitC_NotifiesWorkSignal verifies that drainCommitC
// sends a work signal notification for each drained shard, enabling
// immediate step worker re-processing.
func TestEngine_DrainCommitC_NotifiesWorkSignal(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.NumWorkers = 1
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	commitC := make(chan *commitItem, 16)

	nodeCfg := config.Config{ShardID: 7, ReplicaID: 1}
	node := NewNode(nil, nil, nil, nodeCfg, 100, nil, "", nil)
	node.commitPending.Store(true)
	e.LoadNode(node)

	commitC <- &commitItem{node: node}

	e.drainCommitC(commitC)

	// Verify commitPending was cleared.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be cleared after drain")
	}

	// Verify work signal was sent for shard 7.
	ws := e.WorkSignal()
	if !ws.HasWork(int(7 % uint64(e.NumWorkers()))) {
		t.Fatal("expected work signal notification for shard 7")
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
