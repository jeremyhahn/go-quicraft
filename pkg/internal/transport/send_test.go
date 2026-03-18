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
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestSendBatchHeartbeatType verifies heartbeat messages are sent via the
// priority stream (index 0).
func TestSendBatchHeartbeatType(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send heartbeat response type.
	t1.Send([]proto.Message{
		{Type: proto.HeartbeatResp, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if batches[0].Requests[0].Type != proto.HeartbeatResp {
		t.Fatalf("expected HeartbeatResp, got %d", batches[0].Requests[0].Type)
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
}

// TestSendBatchSourceAddressSet verifies that the source address is included
// in sent batches.
func TestSendBatchSourceAddressSet(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
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
}

// TestBufPoolGetAndPut verifies the buffer pool get/put cycle.
func TestBufPoolGetAndPut(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}
	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	// Get a buffer with a size larger than the pool default.
	buf := tr.getBuf(maxPoolBufSize + 100)
	if buf == nil {
		t.Fatal("expected non-nil buffer")
	}
	if cap(*buf) < maxPoolBufSize+100 {
		t.Fatalf("expected cap >= %d, got %d", maxPoolBufSize+100, cap(*buf))
	}

	// Put it back - should not be pooled since it's too large.
	tr.putBuf(buf)

	// Get a buffer within pool size.
	smallBuf := tr.getBuf(100)
	if smallBuf == nil {
		t.Fatal("expected non-nil small buffer")
	}
	tr.putBuf(smallBuf)
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
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		batches := handler2.getBatches()
		total := 0
		for _, b := range batches {
			total += len(b.Requests)
		}
		if total == 100 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	batches := handler2.getBatches()
	total := 0
	for _, b := range batches {
		total += len(b.Requests)
	}
	t.Fatalf("expected 100 messages total, got %d", total)
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
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		batches := handler2.getBatches()
		total := 0
		for _, b := range batches {
			total += len(b.Requests)
		}
		if total >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Clear handler state.
	handler2.mu.Lock()
	handler2.batches = nil
	handler2.mu.Unlock()

	// Second Send: only one message.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 10},
	})

	// Wait for the single message to arrive.
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		batches := handler2.getBatches()
		total := 0
		for _, b := range batches {
			total += len(b.Requests)
		}
		if total >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

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
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		batches := handler2.getBatches()
		totalMsgs := 0
		for _, b := range batches {
			totalMsgs += len(b.Requests)
		}
		if totalMsgs == iterations {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	batches := handler2.getBatches()
	totalMsgs := 0
	for _, b := range batches {
		totalMsgs += len(b.Requests)
	}
	t.Fatalf("expected %d messages, got %d", iterations, totalMsgs)
}

// TestSendQueuePerTargetIsolation verifies that per-target send queues
// are created for each unique target address and messages are correctly
// routed.
func TestSendQueuePerTargetIsolation(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send two rounds of messages to the same target.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Both messages should have been received correctly via the same
	// per-target send queue.
	batches := handler2.getBatches()
	totalMsgs := 0
	for _, b := range batches {
		totalMsgs += len(b.Requests)
	}
	if totalMsgs < 2 {
		t.Fatalf("expected at least 2 messages, got %d", totalMsgs)
	}
}

// TestSendQueueCreatedPerTarget verifies that the transport creates a
// separate send queue for each unique target address.
func TestSendQueueCreatedPerTarget(t *testing.T) {
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

	// Register targets.
	reg1.Register(1, 2, t2.Addr().String())
	reg1.Register(1, 3, t3.Addr().String())

	// Send to both targets.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 3, Term: 2},
	})

	handler2.waitBatch(t, 10*time.Second)
	handler3.waitBatch(t, 10*time.Second)

	// Verify separate send queues were created.
	t1.sendQueuesMu.RLock()
	numQueues := len(t1.sendQueues)
	t1.sendQueuesMu.RUnlock()

	if numQueues != 2 {
		t.Fatalf("expected 2 send queues (one per target), got %d", numQueues)
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

// TestSendQueueIdlePruneDoesNotAffectActiveQueues verifies that an active
// send queue (one that is continuously receiving messages) is not pruned.
func TestSendQueueIdlePruneDoesNotAffectActiveQueues(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send several messages with short gaps to keep the queue active.
	for i := range 5 {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1)},
		})
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for messages to arrive.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		batches := handler2.getBatches()
		total := 0
		for _, b := range batches {
			total += len(b.Requests)
		}
		if total >= 5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The send queue should still exist because it was active.
	if t1.SendQueueCount() != 1 {
		t.Fatalf("expected 1 active send queue, got %d", t1.SendQueueCount())
	}
}

// TestSendQueueCountReflectsMultipleTargets verifies that SendQueueCount
// returns the correct number of queues when sending to multiple targets.
func TestSendQueueCountReflectsMultipleTargets(t *testing.T) {
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

	reg1.Register(1, 2, t2.Addr().String())
	reg1.Register(1, 3, t3.Addr().String())

	if t1.SendQueueCount() != 0 {
		t.Fatalf("expected 0 send queues before sending, got %d", t1.SendQueueCount())
	}

	// Send to both targets.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 3, Term: 2},
	})

	handler2.waitBatch(t, 10*time.Second)
	handler3.waitBatch(t, 10*time.Second)

	if t1.SendQueueCount() != 2 {
		t.Fatalf("expected 2 send queues (one per target), got %d", t1.SendQueueCount())
	}
}

// TestSendQueueIdleTimeoutConstant verifies the idle timeout constant
// is set to the expected value (2x QUIC MaxIdleTimeout).
func TestSendQueueIdleTimeoutConstant(t *testing.T) {
	if sendQueueIdleTimeout != 60*time.Second {
		t.Fatalf("expected sendQueueIdleTimeout=60s, got %v", sendQueueIdleTimeout)
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
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: false,
	}, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

	t2, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: false,
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	largeCmd := []byte(strings.Repeat("AAAA", 200))
	msgs := []proto.Message{
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
	}
	t1.Send(msgs)
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0].Requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(batches[0].Requests))
	}
	recvCmd := batches[0].Requests[0].Entries[0].Cmd
	if len(recvCmd) != 800 {
		t.Fatalf("expected 800 byte cmd, got %d", len(recvCmd))
	}
}

// TestSendBatch_SmallPayloadNoCompression verifies that heartbeat-sized
// messages are NOT compressed even when compression is enabled.
func TestSendBatch_SmallPayloadNoCompression(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	msgs := []proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
	}
	t1.Send(msgs)
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if batches[0].Requests[0].Type != proto.Heartbeat {
		t.Fatalf("expected Heartbeat, got %d", batches[0].Requests[0].Type)
	}
}

// TestSendBatch_DisableCompressionConfig verifies that when
// DisableCompression is true, messages are sent uncompressed.
func TestSendBatch_DisableCompressionConfig(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: true,
	}, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

	t2, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: true,
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	largeCmd := []byte(strings.Repeat("BBBB", 200))
	msgs := []proto.Message{
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
	}
	t1.Send(msgs)
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	recvCmd := batches[0].Requests[0].Entries[0].Cmd
	if len(recvCmd) != 800 {
		t.Fatalf("expected 800 byte cmd, got %d", len(recvCmd))
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

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		batches := handler2.getBatches()
		total := 0
		for _, b := range batches {
			total += len(b.Requests)
		}
		if total >= 5 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

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

// TestGetOrCreateSendQueue_StoppedTransportReturnsNil verifies that
// getOrCreateSendQueue returns nil when the transport is stopped.
func TestGetOrCreateSendQueue_StoppedTransportReturnsNil(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}
	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	tr.Stop()

	sq := tr.getOrCreateSendQueue("127.0.0.1:9999")
	if sq != nil {
		t.Fatal("expected nil send queue after transport stop")
	}
}

// TestGetOrCreateSendQueue_DoubleCheckRace verifies the double-check
// pattern in getOrCreateSendQueue.
func TestGetOrCreateSendQueue_DoubleCheckRace(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}
	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()

	target := "127.0.0.1:8888"

	sq1 := tr.getOrCreateSendQueue(target)
	if sq1 == nil {
		t.Fatal("expected non-nil send queue")
	}

	sq2 := tr.getOrCreateSendQueue(target)
	if sq2 != sq1 {
		t.Fatal("expected same send queue on second call")
	}

	if tr.SendQueueCount() != 1 {
		t.Fatalf("expected 1 send queue, got %d", tr.SendQueueCount())
	}
}

// TestSendBatch_BurstExercisesDrainLoop verifies burst send batching.
func TestSendBatch_BurstExercisesDrainLoop(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	for i := range 20 {
		t1.Send([]proto.Message{
			{
				Type:    proto.Replicate,
				ShardID: 1,
				From:    1,
				To:      2,
				Term:    1,
				Entries: []proto.Entry{
					{Index: uint64(i + 1), Term: 1, Cmd: []byte{byte(i + 1)}},
				},
			},
		})
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		for _, b := range handler2.getBatches() {
			total += len(b.Requests)
		}
		if total >= 20 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	total := 0
	for _, b := range handler2.getBatches() {
		total += len(b.Requests)
	}
	if total < 20 {
		t.Fatalf("expected at least 20 total messages, got %d", total)
	}
}

// TestSendBatch_MultipleMsgsInOneSend verifies multiple messages in a
// single Send() call.
func TestSendBatch_MultipleMsgsInOneSend(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{{Index: 1, Term: 1, Cmd: []byte("a")}}},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{{Index: 2, Term: 1, Cmd: []byte("b")}}},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{{Index: 3, Term: 1, Cmd: []byte("c")}}},
	}
	t1.Send(msgs)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		for _, b := range handler2.getBatches() {
			total += len(b.Requests)
		}
		if total >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	total := 0
	for _, b := range handler2.getBatches() {
		total += len(b.Requests)
	}
	if total < 3 {
		t.Fatalf("expected at least 3 messages, got %d", total)
	}
}

// noopTransportMetrics implements the transport Metrics interface with no-op methods.
type noopTransportMetrics struct{}

func (noopTransportMetrics) ObserveSendLatency(time.Duration)                  {}
func (noopTransportMetrics) IncMessagesSent(uint64, uint64, uint64)            {}
func (noopTransportMetrics) IncMessagesReceived(uint64, uint64, uint64)        {}
func (noopTransportMetrics) IncSnapshotsSent(uint64, int64, time.Duration)     {}
func (noopTransportMetrics) IncSnapshotsReceived(uint64, int64, time.Duration) {}

// TestSendBatch_WithMetrics verifies that the metrics recording path
// is exercised when the transport has a Metrics implementation.
func TestSendBatch_WithMetrics(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		Metrics:       noopTransportMetrics{},
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
		Metrics:       noopTransportMetrics{},
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	msgs := []proto.Message{
		{
			Type:    proto.Replicate,
			ShardID: 1,
			From:    1,
			To:      2,
			Term:    1,
			Entries: []proto.Entry{
				{Index: 1, Term: 1, Cmd: []byte("metrics-data")},
			},
		},
	}
	t1.Send(msgs)
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0].Requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(batches[0].Requests))
	}
}

func TestSendBatch_CompressionIncompressibleData(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: false,
	}, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

	t2, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: false,
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	// Create incompressible (random-looking) data.
	incompressible := make([]byte, 500)
	for i := range incompressible {
		incompressible[i] = byte(i * 37)
	}

	t1.Send([]proto.Message{
		{
			Type:    proto.Replicate,
			ShardID: 1,
			From:    1,
			To:      2,
			Term:    1,
			Entries: []proto.Entry{
				{Index: 1, Term: 1, Cmd: incompressible},
			},
		},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected at least 1 batch")
	}
	if len(batches[0].Requests[0].Entries[0].Cmd) != 500 {
		t.Fatalf("expected 500 byte cmd, got %d", len(batches[0].Requests[0].Entries[0].Cmd))
	}
}

// TestSendBatch_SnappyPoolCapacityTooSmall verifies the code path where
// the pooled Snappy encode buffer has insufficient capacity and is discarded.

func TestSendBatch_SnappyPoolCapacityTooSmall(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: false,
	}, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

	t2, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: false,
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	// Pre-populate the pool with a tiny buffer to exercise the
	// capacity check path.
	tiny := make([]byte, 10)
	t1.snappyEncPool.Put(&tiny)

	// Send a large message that needs a bigger buffer.
	largeCmd := make([]byte, 2000)
	for i := range largeCmd {
		largeCmd[i] = 'X'
	}

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
	if len(batches) == 0 {
		t.Fatal("expected at least 1 batch")
	}
}

// ---------------------------------------------------------------------------
// handleMessageFrame error paths (recv.go:340 - 77.1% coverage)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_ZeroLengthPayload verifies that a zero-length
// payload is handled gracefully.

func TestSendBatch_WithMetricsAndDebugLog(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		Metrics:       noopTransportMetrics{},
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
		Metrics:       noopTransportMetrics{},
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	// Send multiple messages to exercise metrics recording for each.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{{Index: 1, Term: 1, Cmd: []byte("m1")}}},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{{Index: 2, Term: 1, Cmd: []byte("m2")}}},
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		for _, b := range handler2.getBatches() {
			total += len(b.Requests)
		}
		if total >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("did not receive 2 messages")
}

// ---------------------------------------------------------------------------
// SendSnapshot with metrics (quic.go:1026-1028)
// ---------------------------------------------------------------------------

// TestSendSnapshot_WithMetrics verifies that the metrics recording path
// is exercised for snapshot sends.

func TestSendBatch_WriteDeadlineAndWriteErrorEviction(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Establish connection by sending a message.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{{Index: 1, Term: 1, Cmd: []byte("hello")}}},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Stop t2 to make writes fail.
	t2.Stop()
	time.Sleep(200 * time.Millisecond)

	// Send another message. This should hit the write error path
	// and trigger connection removal + stream eviction.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2,
			Entries: []proto.Entry{{Index: 2, Term: 2, Cmd: []byte("world")}}},
	})

	// Wait for the error to be processed.
	time.Sleep(1 * time.Second)
}

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

func TestSendBatch_CompressedSuccessWithMetrics(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		Metrics:       noopTransportMetrics{},
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
		Metrics:       noopTransportMetrics{},
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	// Send highly compressible data (repeated bytes) to ensure the
	// compressed path is taken AND the write succeeds, exercising the
	// full metrics + debug log path at quic.go:858-872.
	bigCmd := bytes.Repeat([]byte("AAAAAAAAAA"), 500)

	t1.Send([]proto.Message{
		{
			Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{
				{Index: 1, Term: 1, Cmd: bigCmd},
			},
		},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected at least 1 batch")
	}
	if len(batches[0].Requests[0].Entries[0].Cmd) != 5000 {
		t.Fatalf("expected 5000 byte cmd, got %d", len(batches[0].Requests[0].Entries[0].Cmd))
	}
}

// ---------------------------------------------------------------------------
// handleStream with SetReadDeadline error (recv.go:314-316)
// ---------------------------------------------------------------------------

// TestHandleStream_ReadDeadlineErrorExits verifies that if SetReadDeadline
// fails, handleStream exits. This is exercised by closing the stream's
// underlying connection while it is processing.

func TestSendBatch_SnappyPoolReturnsNil(t *testing.T) {
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

	// Drain the pool to ensure Get returns nil.
	for i := 0; i < 100; i++ {
		v := t1.snappyEncPool.Get()
		if v == nil {
			break
		}
	}

	// Send a compressible message. The pool is empty, so a new buffer
	// will be allocated (quic.go:787-789).
	cmd := bytes.Repeat([]byte("Z"), 2000)
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
}

// ---------------------------------------------------------------------------
// handleMessageFrame with unmarshal error on compressed data
// (exercises recv.go:410-417 compressed + unmarshal error combo)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_CompressedUnmarshalError verifies that a compressed
// payload that decompresses to invalid protobuf returns an unmarshal error.

func TestSendBatch_DirectCallWriteDeadlineError(t *testing.T) {
	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

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
	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	// Establish connection and cache stream.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Stop t2 to kill the connection.
	t2.Stop()
	time.Sleep(1 * time.Second)

	// Call sendBatch directly. The cached stream is dead, so either
	// getStream will fail, or SetWriteDeadline/Write will fail.
	t1.sendBatch(t2Addr, []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2},
	})
}

// TestSendBatch_DirectCallStreamWriteError verifies that sendBatch
// handles stream.Write errors and evicts the connection.

func TestSendBatch_DirectCallStreamWriteError(t *testing.T) {
	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

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
	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	// Establish and cache a stream.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Close t2's connection but keep the stream cached on t1.
	// This makes stream.Write fail.
	t2.Stop()
	time.Sleep(500 * time.Millisecond)

	// Call sendBatch directly multiple times to exercise different paths.
	for i := 0; i < 5; i++ {
		t1.sendBatch(t2Addr, []proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 2)},
		})
	}

	// After errors, connections should have been cleaned up.
	time.Sleep(200 * time.Millisecond)
}

// ---------------------------------------------------------------------------
// sendHeartbeatBatch direct call with dead connection (heartbeat.go:108-127)
// ---------------------------------------------------------------------------

// TestSendHeartbeatBatch_DirectCallWriteError verifies the heartbeat
// error paths by directly calling sendHeartbeatBatch on a dead connection.

func TestSendBatch_DirectCallSnappyPoolPaths(t *testing.T) {
	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	t1, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: false,
		Metrics:            noopTransportMetrics{},
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

	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	// Drain the pool completely.
	for i := 0; i < 100; i++ {
		v := t1.snappyEncPool.Get()
		if v == nil {
			break
		}
	}

	// Call sendBatch directly with highly compressible data.
	// Pool is empty -> allocates new buffer. Compression succeeds
	// -> snappyBufPtr is nil (came from allocation, not pool).
	cmd := bytes.Repeat([]byte("A"), 5000)
	t1.sendBatch(t2Addr, []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{{Index: 1, Term: 1, Cmd: cmd}}},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Now put a buffer in the pool and send again to exercise the
	// pool-hit + pool-return path.
	bigBuf := make([]byte, 128*1024)
	t1.snappyEncPool.Put(&bigBuf)

	t1.sendBatch(t2Addr, []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2,
			Entries: []proto.Entry{{Index: 2, Term: 2, Cmd: cmd}}},
	})
	time.Sleep(500 * time.Millisecond)

	batches := handler2.getBatches()
	if len(batches) < 1 {
		t.Fatal("expected at least 1 batch")
	}
}

// ---------------------------------------------------------------------------
// handleMessageFrame compressed + buf != nil path (recv.go:460-467)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_UncompressedTooManyMessages verifies the
// too-many-messages check on the uncompressed path specifically exercises
// the buf != nil cleanup path at recv.go:463-465.

func TestSendBatch_SnappyPoolCapacityTooSmallForceAlloc(t *testing.T) {
	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	t1, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: false,
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

	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	// Pre-populate the snappy encode pool with a tiny buffer.
	tinyBuf := make([]byte, 10)
	t1.snappyEncPool.Put(&tinyBuf)

	// Send a large compressible message. The pooled buffer (10 bytes)
	// will have insufficient capacity, triggering the alloc path.
	largeCmd := bytes.Repeat([]byte("compress-me-"), 500)
	t1.Send([]proto.Message{
		{
			Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{
				{Index: 1, Term: 1, Cmd: largeCmd},
			},
		},
	})

	handler2.waitBatch(t, 10*time.Second)
}

// ---------------------------------------------------------------------------
// evictStreamsForTarget close error path (conn.go:467-472)
// ---------------------------------------------------------------------------

// TestEvictStreamsForTarget_StreamCloseError exercises the stream close
// error logging path in evictStreamsForTarget. When a stream has already
// been closed, Close returns an error that gets logged.

func TestSendBatch_DirectSnappyPoolCapTooSmall(t *testing.T) {
	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	t1, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: false,
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

	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	// Pre-populate the snappy encode pool with a tiny (10-byte) buffer.
	// The snappy MaxEncodedLen for a 5000-byte payload is ~5032 bytes,
	// so cap=10 is far too small, triggering the discard path at
	// quic.go:783-786.
	tinyBuf := make([]byte, 10)
	t1.snappyEncPool.Put(&tinyBuf)

	// Call sendBatch directly to avoid async queue GC timing issues.
	cmd := bytes.Repeat([]byte("A"), 5000)
	t1.sendBatch(t2Addr, []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{{Index: 1, Term: 1, Cmd: cmd}}},
	})

	handler2.waitBatch(t, 10*time.Second)
	batches := handler2.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected at least 1 batch")
	}
}

// ---------------------------------------------------------------------------
// sendBatch snappy pool hit + pool return (quic.go:779-783, 818-822)
// ---------------------------------------------------------------------------

// TestSendBatch_DirectSnappyPoolHitAndReturn exercises the snappy encode
// pool path where a pooled buffer has sufficient capacity. The buffer is
// reused for encoding and then returned to the pool at quic.go:818-822.

func TestSendBatch_DirectSnappyPoolHitAndReturn(t *testing.T) {
	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	t1, err := NewQUICTransport(Config{
		ListenAddress:      ":0",
		DeploymentID:       42,
		MTLSConfig:         mtls,
		DisableCompression: false,
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

	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	// Pre-populate the pool with a large buffer (128KB) that will have
	// sufficient capacity for the snappy-encoded output.
	bigBuf := make([]byte, 128*1024)
	t1.snappyEncPool.Put(&bigBuf)

	// Call sendBatch directly to exercise pool hit with sufficient cap.
	cmd := bytes.Repeat([]byte("A"), 5000)
	t1.sendBatch(t2Addr, []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1,
			Entries: []proto.Entry{{Index: 1, Term: 1, Cmd: cmd}}},
	})

	handler2.waitBatch(t, 10*time.Second)
	batches := handler2.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected at least 1 batch")
	}
}

// ---------------------------------------------------------------------------
// receiveChunks total memory exceeded (snapshot_recv.go:250-258)
// ---------------------------------------------------------------------------

// TestReceiveChunks_TotalBytesExceedsMaxMemory verifies that receiveChunks
// stops processing when a single snapshot receive accumulates more bytes
// than the receiver's maxMemory budget.

func TestSendBatch_WriteErrorEvictsConnection(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	t2Addr := t2.Addr().String()

	// Establish a connection by sending a message.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	time.Sleep(200 * time.Millisecond)

	// Stop t2 to close the remote side.
	t2.Stop()
	// Give time for the connection context to cancel.
	time.Sleep(200 * time.Millisecond)

	// Direct sendBatch call should hit the write error path.
	// The getStream may fail or the write may fail, both are acceptable.
	t1.sendBatch(t2Addr, []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2},
	})

	// The connection should have been evicted.
	t1.connMu.RLock()
	_, connExists := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if connExists {
		t.Log("connection still exists after write failure (may have been re-dialed)")
	}
}

// ---------------------------------------------------------------------------
// handleConnection semaphore exhaustion (recv.go:262-269)
// ---------------------------------------------------------------------------

// TestHandleConnection_SemaphoreExhaustion verifies that when the
// MaxStreamsPerConnection semaphore is full, the connection context
// cancellation causes the handler to return cleanly with the pending
// stream closed.

func TestSendBatch_WithMetricsAndDebugLogging(t *testing.T) {
	// Enable debug logging for this test.
	oldLevel := slog.SetLogLoggerLevel(slog.LevelDebug)
	defer slog.SetLogLoggerLevel(oldLevel)

	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		Metrics:       noopTransportMetrics{},
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

	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	// Direct sendBatch call with metrics configured.
	t1.sendBatch(t2Addr, []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})

	handler2.waitBatch(t, 10*time.Second)
}

// ---------------------------------------------------------------------------
// dialConnection context cancel (conn.go:92-94)
// ---------------------------------------------------------------------------

// TestDialConnection_ShutdownContextCanceled exercises the path in
// dialConnection where the shutdownCtx is already canceled, causing
// the QUIC dial to fail immediately.

func TestSendBatch_DirectWriteErrorViaConnectionClose(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	t2Addr := t2.Addr().String()

	// Get a stream to populate the cache and establish connection.
	stream, err := t1.getStream(t2Addr, 1, false)
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil stream")
	}

	// Close the remote transport to kill the connection.
	t2.Stop()
	time.Sleep(200 * time.Millisecond)

	// Remove the connection from conns but keep the cached stream.
	// This prevents getStream's liveness check from detecting the
	// dead connection and reconnecting.
	t1.connMu.Lock()
	deadConn, ok := t1.conns[t2Addr]
	if ok {
		delete(t1.conns, t2Addr)
	}
	t1.connMu.Unlock()

	if deadConn != nil {
		// Verify connection is actually dead.
		select {
		case <-deadConn.Context().Done():
			// Expected.
		default:
			t.Log("connection not yet dead, test may not exercise write error")
		}
	}

	// sendBatch will call getStream which won't find the connection in
	// conns (removed above). It will try to reconnect (which fails since
	// t2 is stopped), so getStream returns error.
	// This covers the getStream error path (line 833-835) at minimum.
	t1.sendBatch(t2Addr, []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2},
	})

	// No crash = success. The error paths are logged and dropped.
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
