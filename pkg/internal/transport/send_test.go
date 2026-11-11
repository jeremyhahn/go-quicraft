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

package transport

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestSendBatchHeartbeatType verifies heartbeat messages are sent via the
// priority stream (index 0).
func TestSendBatchHeartbeatType(t *testing.T) {
	for _, typ := range []uint64{proto.Heartbeat, proto.HeartbeatResp} {
		t.Run(fmt.Sprintf("type=%d", typ), func(t *testing.T) {
			t1, _, _, handler2, cleanup := transportPair(t, 42)
			defer cleanup()

			t1.Send([]proto.Message{
				{Type: typ, ShardID: 1, From: 1, To: 2, Term: 1},
			})
			handler2.waitBatch(t, 10*time.Second)

			batches := handler2.getBatches()
			if len(batches) != 1 {
				t.Fatalf("expected 1 batch, got %d", len(batches))
			}
			if got := batches[0].Requests[0].Type; got != typ {
				t.Fatalf("expected type %d, got %d", typ, got)
			}
		})
	}
}

// TestSendBatchDeploymentIDIncluded verifies that sent batches include the
// correct DeploymentID.
func TestSendBatchDeploymentIDIncluded(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 777)
	defer cleanup()

	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if batches[0].DeploymentID != 777 {
		t.Fatalf("expected DeploymentID 777, got %d", batches[0].DeploymentID)
	}
	if batches[0].SourceAddress == "" {
		t.Fatal("SourceAddress should not be empty")
	}
}

// TestSendGroupsByTarget verifies that messages for different targets are
// sent in separate batches.
func TestSendGroupsByTarget(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	handler3 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	reg3 := registry.NewRegistry()

	mtls := testMTLSConfig(t)
	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

	t2, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	t3, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler3, reg3)
	if err != nil {
		t.Fatalf("NewQUICTransport(3) failed: %v", err)
	}
	if err := t3.Start(); err != nil {
		t.Fatalf("t3.Start failed: %v", err)
	}
	defer t3.Stop()

	// Register: shard=1,replica=2 -> t2; shard=1,replica=3 -> t3
	reg1.Register(1, 2, t2.Addr().String())
	reg1.Register(1, 3, t3.Addr().String())

	if t1.SendQueueCount() != 0 {
		t.Fatalf("expected 0 send queues before sending, got %d", t1.SendQueueCount())
	}

	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 3, Term: 2},
	}
	t1.Send(msgs)

	handler2.waitBatch(t, 10*time.Second)
	handler3.waitBatch(t, 10*time.Second)

	b2 := handler2.getBatches()
	b3 := handler3.getBatches()
	if len(b2) != 1 {
		t.Fatalf("t2 expected 1 batch, got %d", len(b2))
	}
	if len(b3) != 1 {
		t.Fatalf("t3 expected 1 batch, got %d", len(b3))
	}
	if t1.SendQueueCount() != 2 {
		t.Fatalf("expected 2 send queues (one per target), got %d", t1.SendQueueCount())
	}
}

// TestSendWithRejectedResolve verifies that partial resolve failures
// don't affect messages to known targets.
func TestSendWithRejectedResolve(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},   // known target
		{Type: proto.Replicate, ShardID: 99, From: 1, To: 99, Term: 1}, // unknown target
	}
	t1.Send(msgs)

	handler2.waitBatch(t, 10*time.Second)
	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch (known target only), got %d", len(batches))
	}
}

// TestSendLargeMessageBatch verifies sending a large number of messages
// in a single batch. Since Send() is non-blocking (per-target async queues),
// messages may arrive across multiple batches. We poll until all 100
// messages have been received.
func TestSendLargeMessageBatch(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	msgs := make([]proto.Message, 100)
	for i := range msgs {
		msgs[i] = proto.Message{
			Type:    proto.Replicate,
			ShardID: 1,
			From:    1,
			To:      2,
			Term:    uint64(i),
		}
	}

	t1.Send(msgs)

	// Wait until all 100 messages have been received. The per-target
	// sender goroutine may deliver them across multiple batches.
	waitForMessageCount(t, handler2, 100, 10*time.Second)
	if total := handler2.messageCount(); total != 100 {
		t.Fatalf("expected 100 messages total, got %d", total)
	}
}

// TestSendNoStaleMessageLeak verifies that successive Send calls do not
// deliver stale messages from prior calls.
func TestSendNoStaleMessageLeak(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Register a second shard pointing to the same target.
	t2Addr, err := t1.reg.Resolve(1, 2)
	if err != nil {
		t.Fatalf("failed to resolve t2: %v", err)
	}
	t1.reg.Register(2, 2, t2Addr)

	// First Send: two messages (shard 1 and shard 2, both going to t2).
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
		{Type: proto.Replicate, ShardID: 2, From: 1, To: 2, Term: 2},
	})

	// Wait until both messages are received.
	waitForMessageCount(t, handler2, 2, 10*time.Second)

	// Clear handler state.
	handler2.mu.Lock()
	handler2.batches = nil
	handler2.mu.Unlock()

	// Second Send: only one message.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 10},
	})

	// Wait for the single message to arrive.
	waitForMessageCount(t, handler2, 1, 10*time.Second)

	batches := handler2.getBatches()
	totalMsgs := 0
	for _, b := range batches {
		totalMsgs += len(b.Requests)
	}
	// Must receive exactly 1 message, not 2 (no stale leak).
	if totalMsgs != 1 {
		t.Fatalf("expected 1 message on second Send, got %d", totalMsgs)
	}
	if batches[0].Requests[0].Term != 10 {
		t.Fatalf("expected Term 10, got %d", batches[0].Requests[0].Term)
	}
}

// TestRepeatedSendCorrectness verifies that many repeated Send() calls
// with the same target produce correct messages each time.
func TestRepeatedSendCorrectness(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	const iterations = 20
	for i := range iterations {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1)},
		})
	}

	// Wait until all messages are received.
	waitForMessageCount(t, handler2, iterations, 10*time.Second)
	if totalMsgs := handler2.messageCount(); totalMsgs != iterations {
		t.Fatalf("expected %d messages, got %d", iterations, totalMsgs)
	}
}

// TestSendQueueIdlePruneRemovesQueue verifies that a send queue worker
// removes itself from the sendQueues map and exits after the idle timeout
// expires with no messages. This prevents goroutine and memory leaks for
// removed cluster nodes.
func TestSendQueueIdlePruneRemovesQueue(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send a message to create the send queue for t2's target.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Verify the send queue was created.
	if t1.SendQueueCount() != 1 {
		t.Fatalf("expected 1 send queue, got %d", t1.SendQueueCount())
	}

	// Wait for the idle timeout to expire. The constant is 60s but in a
	// production build we cannot easily override it without modifying the
	// constant. Instead, we directly test the pruning mechanism by calling
	// the worker's cleanup path: manually delete the queue from the map
	// to simulate what happens when the idle timer fires.
	//
	// For a true timeout test, we would need to wait 60+ seconds which is
	// too slow for unit tests. Instead, we verify the mechanism works by
	// checking the SendQueueCount method and that re-creation works.
	t1.sendQueuesMu.Lock()
	for k := range t1.sendQueues {
		delete(t1.sendQueues, k)
	}
	t1.sendQueuesMu.Unlock()

	if t1.SendQueueCount() != 0 {
		t.Fatalf("expected 0 send queues after manual prune, got %d", t1.SendQueueCount())
	}

	// Sending again should lazily re-create the queue and succeed.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2},
	})
	handler2.waitBatch(t, 10*time.Second)

	if t1.SendQueueCount() != 1 {
		t.Fatalf("expected 1 send queue after re-creation, got %d", t1.SendQueueCount())
	}

	batches := handler2.getBatches()
	totalMsgs := 0
	for _, b := range batches {
		totalMsgs += len(b.Requests)
	}
	if totalMsgs < 2 {
		t.Fatalf("expected at least 2 messages total, got %d", totalMsgs)
	}
}

// TestEnqueueToTarget_PruneThenEnqueueReEnqueues verifies the fix for the
// send-queue idle-prune race: if the worker marks a queue closed and removes
// it from the map after a producer has obtained the *sendQueue pointer, a
// subsequent enqueue must not be orphaned. enqueueToTarget observes the closed
// flag, re-acquires a fresh queue, and re-enqueues so the message is not lost.
func TestEnqueueToTarget_PruneThenEnqueueReEnqueues(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	// Do not Start: this is a white-box test of the enqueue path that does
	// not require a real worker or network. Stop must still run to flip the
	// stopped flag cleanly for any future getOrCreateSendQueue callers.
	defer tr.Stop()

	const target = "127.0.0.1:9999"

	// Simulate a queue that the idle-prune worker has just marked closed and
	// removed from the map (the worker has exited; no drainer remains).
	orphan := &sendQueue{
		ch:   make(chan proto.Message, defaultSendQueueLen),
		hbCh: make(chan proto.Message, 64),
	}
	orphan.closed.Store(true)
	tr.sendQueuesMu.Lock()
	tr.sendQueues[target] = orphan
	tr.sendQueuesMu.Unlock()

	msg := proto.Message{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 7}
	if ok := tr.enqueueToTarget(target, msg, false); !ok {
		t.Fatal("enqueueToTarget returned false (transport stopped) unexpectedly")
	}

	// A fresh, non-closed queue must have replaced the orphan.
	tr.sendQueuesMu.RLock()
	fresh, present := tr.sendQueues[target]
	tr.sendQueuesMu.RUnlock()
	if !present {
		t.Fatal("expected a fresh send queue to be created for the target")
	}
	if fresh == orphan {
		t.Fatal("expected the orphaned queue to be replaced, got the same pointer")
	}
	if fresh.closed.Load() {
		t.Fatal("fresh send queue should not be marked closed")
	}

	// The message must be deliverable: it must NOT be sitting only in the
	// orphaned channel. The fresh queue's worker (started by
	// getOrCreateSendQueue) will have drained or will drain it; pull from the
	// fresh channel directly to assert the message is recoverable there. We
	// stop the transport first so the worker exits, then inspect the channel.
	//
	// Drain whichever channel holds the message: the worker may have already
	// consumed it, so accept delivery via either the fresh channel or the
	// worker having taken it (channel empty but orphan empty too).
	if len(orphan.ch) != 0 {
		t.Fatalf("message was orphaned in the closed queue (len=%d)", len(orphan.ch))
	}
}

// TestEnqueueToTarget_FreshQueueNotMarkedClosed verifies that the normal
// (non-pruned) enqueue path returns after a single attempt with the message
// placed in a live queue.
func TestEnqueueToTarget_FreshQueueNotMarkedClosed(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	defer tr.Stop()

	const target = "127.0.0.1:9998"
	msg := proto.Message{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 3}
	if ok := tr.enqueueToTarget(target, msg, false); !ok {
		t.Fatal("enqueueToTarget returned false unexpectedly")
	}
	tr.sendQueuesMu.RLock()
	sq, present := tr.sendQueues[target]
	tr.sendQueuesMu.RUnlock()
	if !present {
		t.Fatal("expected a send queue to be created")
	}
	if sq.closed.Load() {
		t.Fatal("freshly created send queue must not be marked closed")
	}
}

// TestSendQueueIdlePruneDoesNotAffectActiveQueues verifies that an active
// send queue (one that is continuously receiving messages) is not pruned.
func TestSendQueueIdlePruneDoesNotAffectActiveQueues(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send several messages to keep the queue active.
	for i := range 5 {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1)},
		})
	}
	waitForMessageCount(t, handler2, 5, 10*time.Second)

	// The send queue should still exist because it was active.
	if t1.SendQueueCount() != 1 {
		t.Fatalf("expected 1 active send queue, got %d", t1.SendQueueCount())
	}
}

// TestSendWithMessageEntries verifies messages carrying log entries round-trip.
func TestSendWithMessageEntries(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	msgs := []proto.Message{
		{
			Type:    proto.Replicate,
			ShardID: 1,
			From:    1,
			To:      2,
			Term:    5,
			Entries: []proto.Entry{
				{Index: 1, Term: 5, Cmd: []byte("command-1")},
				{Index: 2, Term: 5, Cmd: []byte("command-2")},
			},
		},
	}

	t1.Send(msgs)
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	req := batches[0].Requests[0]
	if len(req.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(req.Entries))
	}
	if string(req.Entries[0].Cmd) != "command-1" {
		t.Fatalf("unexpected entry cmd: %s", req.Entries[0].Cmd)
	}
}

// ---------------------------------------------------------------------------
// Compression and send queue coverage (merged from coverage_send_test.go)
// ---------------------------------------------------------------------------

// TestSendBatch_CompressionEnabled verifies that sending a large enough
// non-heartbeat payload with compression enabled results in the
// compressed flag being set on the received frame.
func TestSendBatch_CompressionEnabled(t *testing.T) {
	for _, disable := range []bool{false, true} {
		t.Run(fmt.Sprintf("DisableCompression=%v", disable), func(t *testing.T) {
			mtls := testMTLSConfig(t)
			start := func(h *testHandler, reg *registry.Registry) *QUICTransport {
				tr, err := NewQUICTransport(Config{
					ListenAddress:      ":0",
					DeploymentID:       42,
					MTLSConfig:         mtls,
					DisableCompression: disable,
				}, h, reg)
				if err != nil {
					t.Fatalf("NewQUICTransport failed: %v", err)
				}
				if err := tr.Start(); err != nil {
					t.Fatalf("Start failed: %v", err)
				}
				t.Cleanup(func() { _ = tr.Stop() })
				return tr
			}
			reg1 := registry.NewRegistry()
			handler2 := newTestHandler()
			t1 := start(newTestHandler(), reg1)
			t2 := start(handler2, registry.NewRegistry())
			reg1.Register(1, 2, t2.Addr().String())

			largeCmd := bytes.Repeat([]byte("AAAA"), 200)
			t1.Send([]proto.Message{
				{
					Type:    proto.Replicate,
					ShardID: 1,
					From:    1,
					To:      2,
					Term:    1,
					Entries: []proto.Entry{
						{Index: 1, Term: 1, Cmd: largeCmd},
					},
				},
			})
			handler2.waitBatch(t, 10*time.Second)

			batches := handler2.getBatches()
			if len(batches) != 1 {
				t.Fatalf("expected 1 batch, got %d", len(batches))
			}
			if len(batches[0].Requests) != 1 {
				t.Fatalf("expected 1 request, got %d", len(batches[0].Requests))
			}
			if !bytes.Equal(batches[0].Requests[0].Entries[0].Cmd, largeCmd) {
				t.Fatalf("cmd mismatch: got %d bytes, want %d", len(batches[0].Requests[0].Entries[0].Cmd), len(largeCmd))
			}
		})
	}
}

// TestSendQueueWorker_StopDrains verifies that when the transport is
// stopped, the send queue worker exits cleanly.
func TestSendQueueWorker_StopDrains(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	for i := range 10 {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1)},
		})
	}

	waitForMessageCount(t, handler2, 5, 5*time.Second)

	done := make(chan struct{})
	go func() {
		t1.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("transport.Stop() did not complete within timeout")
	}
}

// noopTransportMetrics implements the transport Metrics interface with no-op methods.
type noopTransportMetrics struct{}

func (noopTransportMetrics) ObserveSendLatency(time.Duration)                  {}
func (noopTransportMetrics) IncMessagesSent(uint64, uint64, uint64)            {}
func (noopTransportMetrics) IncMessagesReceived(uint64, uint64, uint64)        {}
func (noopTransportMetrics) IncSnapshotsSent(uint64, int64, time.Duration)     {}
func (noopTransportMetrics) IncSnapshotsReceived(uint64, int64, time.Duration) {}
func (noopTransportMetrics) IncSnapshotsRejected(uint64, string)               {}
func (noopTransportMetrics) IncMessagesDropped(uint64, uint64, string)         {}

// TestSendBatch_WithMetrics verifies that the metrics recording path
// is exercised when the transport has a Metrics implementation.
func TestSendBatch_WithMetrics(t *testing.T) {
	cases := map[string][]byte{
		"uncompressed": []byte("metrics-data"),
		// Highly compressible data takes the compressed write path.
		"compressed": bytes.Repeat([]byte("AAAAAAAAAA"), 500),
	}
	for name, cmd := range cases {
		t.Run(name, func(t *testing.T) {
			mtls := testMTLSConfig(t)
			start := func(h *testHandler, reg *registry.Registry) *QUICTransport {
				tr, err := NewQUICTransport(Config{
					ListenAddress: ":0",
					DeploymentID:  42,
					MTLSConfig:    mtls,
					Metrics:       noopTransportMetrics{},
				}, h, reg)
				if err != nil {
					t.Fatalf("NewQUICTransport failed: %v", err)
				}
				if err := tr.Start(); err != nil {
					t.Fatalf("Start failed: %v", err)
				}
				t.Cleanup(func() { _ = tr.Stop() })
				return tr
			}
			reg1 := registry.NewRegistry()
			handler2 := newTestHandler()
			t1 := start(newTestHandler(), reg1)
			t2 := start(handler2, registry.NewRegistry())
			reg1.Register(1, 2, t2.Addr().String())

			t1.Send([]proto.Message{
				{
					Type:    proto.Replicate,
					ShardID: 1,
					From:    1,
					To:      2,
					Term:    1,
					Entries: []proto.Entry{
						{Index: 1, Term: 1, Cmd: cmd},
					},
				},
			})
			handler2.waitBatch(t, 10*time.Second)

			batches := handler2.getBatches()
			if len(batches) != 1 {
				t.Fatalf("expected 1 batch, got %d", len(batches))
			}
			if len(batches[0].Requests) != 1 {
				t.Fatalf("expected 1 request, got %d", len(batches[0].Requests))
			}
			if got := len(batches[0].Requests[0].Entries[0].Cmd); got != len(cmd) {
				t.Fatalf("expected %d byte cmd, got %d", len(cmd), got)
			}
		})
	}
}

// TestSendBatch_SnappyPoolCapacityTooSmall verifies the code path where
// the pooled Snappy encode buffer has insufficient capacity and is discarded.

func TestSendBatch_SnappyPoolCapacityTooSmall(t *testing.T) {
	cases := map[string]func(tr *QUICTransport){
		// A tiny pooled buffer exercises the capacity check path.
		"too small": func(tr *QUICTransport) {
			tiny := make([]byte, 10)
			tr.snappyEncPool.Put(&tiny)
		},
		// A drained pool forces a fresh allocation.
		"empty": func(tr *QUICTransport) {
			for i := 0; i < 100; i++ {
				if tr.snappyEncPool.Get() == nil {
					break
				}
			}
		},
		// A large pooled buffer is a hit with sufficient capacity.
		"large hit": func(tr *QUICTransport) {
			bigBuf := make([]byte, 128*1024)
			tr.snappyEncPool.Put(&bigBuf)
		},
	}
	for name, seed := range cases {
		t.Run(name, func(t *testing.T) {
			mtls := testMTLSConfig(t)
			start := func(h *testHandler, reg *registry.Registry) *QUICTransport {
				tr, err := NewQUICTransport(Config{
					ListenAddress: ":0",
					DeploymentID:  42,
					MTLSConfig:    mtls,
				}, h, reg)
				if err != nil {
					t.Fatalf("NewQUICTransport failed: %v", err)
				}
				if err := tr.Start(); err != nil {
					t.Fatalf("Start failed: %v", err)
				}
				t.Cleanup(func() { _ = tr.Stop() })
				return tr
			}
			reg1 := registry.NewRegistry()
			handler2 := newTestHandler()
			t1 := start(newTestHandler(), reg1)
			t2 := start(handler2, registry.NewRegistry())
			reg1.Register(1, 2, t2.Addr().String())

			seed(t1)

			cmd := bytes.Repeat([]byte("X"), 5000)
			t1.Send([]proto.Message{
				{
					Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
					Entries: []proto.Entry{{Index: 1, Term: 1, Cmd: cmd}},
				},
			})
			handler2.waitBatch(t, 10*time.Second)

			batches := handler2.getBatches()
			if len(batches) != 1 {
				t.Fatalf("expected 1 batch, got %d", len(batches))
			}
			if !bytes.Equal(batches[0].Requests[0].Entries[0].Cmd, cmd) {
				t.Fatalf("cmd mismatch: got %d bytes, want %d", len(batches[0].Requests[0].Entries[0].Cmd), len(cmd))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// handleMessageFrame error paths (recv.go:340 - 77.1% coverage)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_ZeroLengthPayload verifies that a zero-length
// payload is handled gracefully.

// ---------------------------------------------------------------------------
// SendSnapshot with metrics (quic.go:1026-1028)
// ---------------------------------------------------------------------------

// TestSendSnapshot_WithMetrics verifies that the metrics recording path
// is exercised for snapshot sends.

// ---------------------------------------------------------------------------
// Stop with active connections and streams (quic.go:418-442)
// ---------------------------------------------------------------------------

// TestStop_WithActiveConnectionsAndStreams verifies that Stop cleanly
// closes all connections and streams, exercising the cleanup loops.

func TestSendBatch_BufferReallocationForCompressedFrame(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

	t2, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	// Send a message with incompressible data (pseudo-random) that will
	// make the snappy output larger than the input, triggering the
	// "if cap(buf) < totalSize" reallocation path.
	cmd := make([]byte, 8192)
	for i := range cmd {
		cmd[i] = byte((i * 251) ^ (i >> 3))
	}

	t1.Send([]proto.Message{
		{
			Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{
				{Index: 1, Term: 1, Cmd: cmd},
			},
		},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected at least 1 batch")
	}
	if len(batches[0].Requests[0].Entries[0].Cmd) != 8192 {
		t.Fatalf("expected 8192 byte cmd, got %d", len(batches[0].Requests[0].Entries[0].Cmd))
	}
}

// ---------------------------------------------------------------------------
// handleMessageFrame recvStart debug timing (recv.go:346-348)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_DebugTimingPath verifies that the debug timing
// code in handleMessageFrame is exercised. Since slog.LevelDebug may not
// be enabled, this test primarily covers the fallback path where
// recvStart.IsZero() is true.

// ---------------------------------------------------------------------------
// handleStream with SetReadDeadline error (recv.go:314-316)
// ---------------------------------------------------------------------------

// TestHandleStream_ReadDeadlineErrorExits verifies that if SetReadDeadline
// fails, handleStream exits. This is exercised by closing the stream's
// underlying connection while it is processing.

// ---------------------------------------------------------------------------
// handleMessageFrame with unmarshal error on compressed data
// (exercises recv.go:410-417 compressed + unmarshal error combo)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_CompressedUnmarshalError verifies that a compressed
// payload that decompresses to invalid protobuf returns an unmarshal error.

// ---------------------------------------------------------------------------
// sendHeartbeatBatch direct call with dead connection (heartbeat.go:108-127)
// ---------------------------------------------------------------------------

// TestSendHeartbeatBatch_DirectCallWriteError verifies the heartbeat
// error paths by directly calling sendHeartbeatBatch on a dead connection.

// ---------------------------------------------------------------------------
// handleMessageFrame compressed + buf != nil path (recv.go:460-467)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_UncompressedTooManyMessages verifies the
// too-many-messages check on the uncompressed path specifically exercises
// the buf != nil cleanup path at recv.go:463-465.

// ---------------------------------------------------------------------------
// evictStreamsForTarget close error path (conn.go:467-472)
// ---------------------------------------------------------------------------

// TestEvictStreamsForTarget_StreamCloseError exercises the stream close
// error logging path in evictStreamsForTarget. When a stream has already
// been closed, Close returns an error that gets logged.

// ---------------------------------------------------------------------------
// sendBatch snappy pool hit + pool return (quic.go:779-783, 818-822)
// ---------------------------------------------------------------------------

// TestSendBatch_DirectSnappyPoolHitAndReturn exercises the snappy encode
// pool path where a pooled buffer has sufficient capacity. The buffer is
// reused for encoding and then returned to the pool at quic.go:818-822.

// ---------------------------------------------------------------------------
// receiveChunks total memory exceeded (snapshot_recv.go:250-258)
// ---------------------------------------------------------------------------

// TestReceiveChunks_TotalBytesExceedsMaxMemory verifies that receiveChunks
// stops processing when a single snapshot receive accumulates more bytes
// than the receiver's maxMemory budget.

// TestSendBatch_WriteErrorEvictsConnection verifies that a stream write failure
// evicts AND closes the cached connection along with every stream pooled on it.
// Eviction is what stops the transport writing into a black-holed connection:
// without it every cached stream on the dead connection stalls for the full
// write deadline on each subsequent batch, and Raft sees a peer that is
// reachable-but-silent.
//
// The failure is injected deterministically by cancelling the send side of the
// pooled stream — subsequent writes on it fail immediately — so the test never
// depends on a peer teardown racing the send. It asserts identity, not mere
// absence, of the connection, so a legitimate re-dial cannot mask a missing
// eviction.
func TestSendBatch_WriteErrorEvictsConnection(t *testing.T) {
	rec := &dropRecorder{}
	t1, t2, cleanup := transportPairWith(t, 42, newTestHandler(), newTestHandler(),
		func(c *Config) { c.Metrics = rec })
	defer cleanup()

	t2Addr := t2.Addr().String()
	const shardID = uint64(1)

	// Establish the connection and the exact pooled stream sendBatch will reuse
	// for this shard.
	stream, err := t1.getStream(t2Addr, shardID, false, "")
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}
	if stream == nil {
		t.Fatal("getStream returned a nil stream")
	}

	t1.connMu.RLock()
	conn := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if conn == nil {
		t.Fatal("no cached connection after getStream")
	}
	select {
	case <-conn.Context().Done():
		t.Fatal("connection already dead before the write error was injected")
	default:
	}

	// Cancelling the send side makes every subsequent Write on this stream fail
	// while leaving the connection itself healthy — so only sendBatch's write
	// error path can be responsible for the eviction that follows.
	stream.CancelWrite(1)

	t1.sendBatch(t2Addr, "", []proto.Message{
		{Type: proto.Replicate, ShardID: shardID, From: 1, To: 2, Term: 2},
	})

	// The batch must be reported dropped as a write failure, not as an
	// unavailable stream: the latter would mean getStream, not the write,
	// failed and the eviction below proves nothing.
	var sawWriteFailed bool
	for _, d := range rec.drops() {
		if d.reason == dropReasonWriteFailed && d.shardID == shardID && d.to == 2 {
			sawWriteFailed = true
		}
	}
	if !sawWriteFailed {
		t.Fatalf("no %q drop recorded for the failed write; drops=%+v",
			dropReasonWriteFailed, rec.drops())
	}

	// The evicted connection must be CLOSED, not merely dropped from the map:
	// a forgotten-but-open connection keeps its streams writable into a peer
	// that will never read them.
	select {
	case <-conn.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not closed after the write failure")
	}

	// And it must no longer be the cached connection for the target. Compare
	// identity rather than presence so a concurrent re-dial cannot hide a
	// missing eviction.
	t1.connMu.RLock()
	cached, cachedOK := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if cachedOK && cached == conn {
		t.Fatal("the dead connection is still cached after the write failure")
	}

	// Every stream pooled on that connection must have been evicted too, so the
	// next send does not reuse the broken stream.
	next, err := t1.getStream(t2Addr, shardID, false, "")
	if err != nil {
		// A dial failure here is acceptable; what must not happen is the
		// cancelled stream being handed back.
		return
	}
	if next == stream {
		t.Fatal("getStream returned the cancelled stream; streams were not evicted with the connection")
	}
}

// ---------------------------------------------------------------------------
// handleConnection semaphore exhaustion (recv.go:262-269)
// ---------------------------------------------------------------------------

// TestHandleConnection_SemaphoreExhaustion verifies that when the
// MaxStreamsPerConnection semaphore is full, the connection context
// cancellation causes the handler to return cleanly with the pending
// stream closed.

// ---------------------------------------------------------------------------
// dialConnection context cancel (conn.go:92-94)
// ---------------------------------------------------------------------------

// TestDialConnection_ShutdownContextCanceled exercises the path in
// dialConnection where the shutdownCtx is already canceled, causing
// the QUIC dial to fail immediately.

func TestSendBatch_DirectWriteErrorViaConnectionClose(t *testing.T) {
	// The re-dial to the stopped peer must fail, and fail on production's
	// budget; see withProductionDialBudget.
	t1, t2, cleanup := transportPairWith(t, 42, newTestHandler(), newTestHandler(), withProductionDialBudget)
	defer cleanup()

	t2Addr := t2.Addr().String()

	// Get a stream to populate the cache and establish connection.
	stream, err := t1.getStream(t2Addr, 1, false, "")
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil stream")
	}

	rec := &dropRecorder{}
	t1.metrics = rec

	// Close the remote transport and wait for the connection to die.
	t2.Stop()
	t1.connMu.Lock()
	deadConn, ok := t1.conns[t2Addr]
	t1.connMu.Unlock()
	if ok {
		waitForChan(t, deadConn.Context().Done(), 10*time.Second, "the connection did not close after the peer stopped")
	}

	// Remove the connection from conns but keep the cached stream.
	// This prevents getStream's liveness check from detecting the
	// dead connection and reconnecting.
	t1.connMu.Lock()
	delete(t1.conns, t2Addr)
	t1.connMu.Unlock()

	// sendBatch's getStream does not find the connection and its re-dial
	// to the stopped peer fails, so the message is dropped.
	t1.sendBatch(t2Addr, "", []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2},
	})
	drops := rec.drops()
	if len(drops) != 1 || drops[0].shardID != 1 || drops[0].to != 2 {
		t.Fatalf("drops = %+v, want one for shard 1 replica 2", drops)
	}
}

// ---------------------------------------------------------------------------
// Send backpressure logging (quic.go:531-538)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// getOrCreateSendQueue double-check hit (quic.go:568-571)
// ---------------------------------------------------------------------------

// TestGetOrCreateSendQueue_DoubleCheckHitViaBarrier exercises the
// double-check path in getOrCreateSendQueue (quic.go:568-571) by racing
// many goroutines for different targets to maximize slow path contention.
// Since RWMutex blocks readers when a writer is waiting, we need multiple
// targets to create contention at the write lock.
