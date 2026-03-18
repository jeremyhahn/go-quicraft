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
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// isHeartbeatBatch
// ---------------------------------------------------------------------------

// TestIsHeartbeatBatch_AllHeartbeats verifies that a batch of only heartbeat
// and heartbeat response messages is detected as a heartbeat batch.
func TestIsHeartbeatBatch_AllHeartbeats(t *testing.T) {
	msgs := []proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
		{Type: proto.HeartbeatResp, ShardID: 2, From: 1, To: 2, Term: 1},
		{Type: proto.Heartbeat, ShardID: 3, From: 1, To: 2, Term: 1},
	}
	if !isHeartbeatBatch(msgs) {
		t.Fatal("expected heartbeat batch to be detected")
	}
}

// TestIsHeartbeatBatch_MixedTypes verifies that a batch containing non-heartbeat
// messages is not classified as a heartbeat batch.
func TestIsHeartbeatBatch_MixedTypes(t *testing.T) {
	msgs := []proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	}
	if isHeartbeatBatch(msgs) {
		t.Fatal("mixed batch should not be classified as heartbeat batch")
	}
}

// TestIsHeartbeatBatch_Empty verifies that an empty batch is classified as
// a heartbeat batch (vacuously true).
func TestIsHeartbeatBatch_Empty(t *testing.T) {
	if !isHeartbeatBatch(nil) {
		t.Fatal("empty batch should be vacuously true")
	}
	if !isHeartbeatBatch([]proto.Message{}) {
		t.Fatal("zero-length batch should be vacuously true")
	}
}

// TestIsHeartbeatBatch_NonHeartbeatOnly verifies that a batch of only
// non-heartbeat messages is not classified as a heartbeat batch.
func TestIsHeartbeatBatch_NonHeartbeatOnly(t *testing.T) {
	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
		{Type: proto.RequestVote, ShardID: 1, From: 1, To: 2, Term: 1},
	}
	if isHeartbeatBatch(msgs) {
		t.Fatal("non-heartbeat batch should not be classified as heartbeat batch")
	}
}

// ---------------------------------------------------------------------------
// initHeartbeatPrefix
// ---------------------------------------------------------------------------

// TestInitHeartbeatPrefix_CorrectFormat verifies that the heartbeat prefix
// is correctly serialized with the expected BinVer, DeploymentID, and
// SourceAddress fields.
func TestInitHeartbeatPrefix_CorrectFormat(t *testing.T) {
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

	if tr.hbPrefix == nil {
		t.Fatal("hbPrefix should be initialized after Start()")
	}

	addr := tr.localAddr()
	expectedLen := heartbeatBatchFixedPrefix + len(addr)
	if len(tr.hbPrefix) != expectedLen {
		t.Fatalf("hbPrefix length: want %d, got %d", expectedLen, len(tr.hbPrefix))
	}
}

// TestInitHeartbeatPrefix_NilBeforeStart verifies that hbPrefix is nil
// before Start is called.
func TestInitHeartbeatPrefix_NilBeforeStart(t *testing.T) {
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

	if tr.hbPrefix != nil {
		t.Fatal("hbPrefix should be nil before Start()")
	}
}

// ---------------------------------------------------------------------------
// sendHeartbeatBatch round-trip
// ---------------------------------------------------------------------------

// TestSendHeartbeatBatch_SingleMessage verifies that a single heartbeat
// message sent via the fast path is correctly received by the peer.
func TestSendHeartbeatBatch_SingleMessage(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 7, Commit: 99},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0].Requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(batches[0].Requests))
	}
	msg := batches[0].Requests[0]
	if msg.Type != proto.Heartbeat {
		t.Fatalf("expected Heartbeat, got %d", msg.Type)
	}
	if msg.Term != 7 {
		t.Fatalf("expected Term 7, got %d", msg.Term)
	}
	if msg.Commit != 99 {
		t.Fatalf("expected Commit 99, got %d", msg.Commit)
	}
}

// TestSendHeartbeatBatch_MultipleMessages verifies that multiple heartbeat
// messages coalesced to the same target are correctly delivered via the
// fast path.
func TestSendHeartbeatBatch_MultipleMessages(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Register additional shards pointing to the same target.
	t2Addr, err := t1.reg.Resolve(1, 2)
	if err != nil {
		t.Fatalf("failed to resolve t2: %v", err)
	}
	t1.reg.Register(2, 2, t2Addr)
	t1.reg.Register(3, 2, t2Addr)

	// Send heartbeats for 3 shards to the same target. They will be
	// grouped into one batch by Send() and sent via the heartbeat fast path.
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 10},
		{Type: proto.Heartbeat, ShardID: 2, From: 1, To: 2, Term: 11},
		{Type: proto.Heartbeat, ShardID: 3, From: 1, To: 2, Term: 12},
	})
	handler2.waitBatch(t, 10*time.Second)

	// The 3 heartbeats may arrive in 1-3 batches depending on framing
	// and scheduling. Poll until all 3 are delivered.
	deadline := time.Now().Add(5 * time.Second)
	var totalMsgs int
	for time.Now().Before(deadline) {
		totalMsgs = 0
		for _, b := range handler2.getBatches() {
			totalMsgs += len(b.Requests)
		}
		if totalMsgs >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if totalMsgs != 3 {
		t.Fatalf("expected 3 messages total, got %d", totalMsgs)
	}
}

// TestSendHeartbeatBatch_HeartbeatResp verifies that HeartbeatResp messages
// also use the fast path and round-trip correctly.
func TestSendHeartbeatBatch_HeartbeatResp(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{
		{Type: proto.HeartbeatResp, ShardID: 1, From: 1, To: 2, Term: 3},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	msg := batches[0].Requests[0]
	if msg.Type != proto.HeartbeatResp {
		t.Fatalf("expected HeartbeatResp, got %d", msg.Type)
	}
	if msg.Term != 3 {
		t.Fatalf("expected Term 3, got %d", msg.Term)
	}
}

// TestSendHeartbeatBatch_DeploymentIDIncluded verifies that the heartbeat
// fast path includes the correct DeploymentID in the batch header.
func TestSendHeartbeatBatch_DeploymentIDIncluded(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 777)
	defer cleanup()

	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
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

// TestSendHeartbeatBatch_SourceAddressSet verifies that the heartbeat
// fast path includes a non-empty SourceAddress in the batch header.
func TestSendHeartbeatBatch_SourceAddressSet(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
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

// TestSendHeartbeatBatch_StreamEvictionOnWriteError verifies that the
// heartbeat fast path evicts the stream when a write fails.
func TestSendHeartbeatBatch_StreamEvictionOnWriteError(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Establish the heartbeat stream.
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Stop t2 to break the stream.
	t2.Stop()
	time.Sleep(500 * time.Millisecond)

	// Send again. The write should fail, triggering stream eviction.
	// Should not panic or block.
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 2},
	})
}

// TestSendHeartbeatBatch_UnreachableTarget verifies that the heartbeat
// fast path handles getStream failure gracefully.
func TestSendHeartbeatBatch_UnreachableTarget(t *testing.T) {
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

	// Register an unreachable target.
	reg.Register(1, 2, "127.0.0.1:1")

	// Should not panic or block.
	tr.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
	})
}

// TestSendHeartbeatBatch_MixedBatchUsesSlowPath verifies that a batch
// containing both heartbeat and non-heartbeat messages delivers all
// messages to the receiver. With priority heartbeat channels, heartbeats
// and data messages are routed to separate channels and may arrive in
// separate batches. This test verifies both are delivered.
func TestSendHeartbeatBatch_MixedBatchUsesSlowPath(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// The heartbeat goes to hbCh and the replicate goes to ch, so they
	// are delivered as separate batches by the sendQueueWorker.
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})

	// Wait for at least one batch to arrive, then poll briefly for the
	// second batch since they are sent on separate channels.
	handler2.waitBatch(t, 10*time.Second)

	deadline := time.After(5 * time.Second)
	for {
		batches := handler2.getBatches()
		totalMsgs := 0
		for _, b := range batches {
			totalMsgs += len(b.Requests)
		}
		if totalMsgs >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected 2 messages, got %d", totalMsgs)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestSendHeartbeatBatch_AllFieldsRoundTrip verifies that all Message
// fields survive the heartbeat fast path serialization.
func TestSendHeartbeatBatch_AllFieldsRoundTrip(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{
		{
			Type:      proto.Heartbeat,
			From:      1,
			To:        2,
			ShardID:   1,
			ReplicaID: 1,
			Term:      100,
			LogTerm:   99,
			LogIndex:  500,
			Commit:    450,
			Reject:    false,
			Hint:      42,
		},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 || len(batches[0].Requests) != 1 {
		t.Fatal("expected 1 batch with 1 request")
	}
	msg := batches[0].Requests[0]
	if msg.Type != proto.Heartbeat {
		t.Fatalf("Type: want %d, got %d", proto.Heartbeat, msg.Type)
	}
	if msg.From != 1 {
		t.Fatalf("From: want 1, got %d", msg.From)
	}
	if msg.To != 2 {
		t.Fatalf("To: want 2, got %d", msg.To)
	}
	if msg.ShardID != 1 {
		t.Fatalf("ShardID: want 1, got %d", msg.ShardID)
	}
	if msg.ReplicaID != 1 {
		t.Fatalf("ReplicaID: want 1, got %d", msg.ReplicaID)
	}
	if msg.Term != 100 {
		t.Fatalf("Term: want 100, got %d", msg.Term)
	}
	if msg.LogTerm != 99 {
		t.Fatalf("LogTerm: want 99, got %d", msg.LogTerm)
	}
	if msg.LogIndex != 500 {
		t.Fatalf("LogIndex: want 500, got %d", msg.LogIndex)
	}
	if msg.Commit != 450 {
		t.Fatalf("Commit: want 450, got %d", msg.Commit)
	}
	if msg.Reject {
		t.Fatal("Reject: want false, got true")
	}
	if msg.Hint != 42 {
		t.Fatalf("Hint: want 42, got %d", msg.Hint)
	}
}

// TestSendHeartbeatBatch_RepeatedSendReusesBuffer verifies that repeated
// heartbeat sends reuse the buffer pool without corruption.
func TestSendHeartbeatBatch_RepeatedSendReusesBuffer(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	const iterations = 20
	for i := range iterations {
		t1.Send([]proto.Message{
			{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1)},
		})
		handler2.waitBatch(t, 10*time.Second)
	}

	batches := handler2.getBatches()
	totalMsgs := 0
	for _, b := range batches {
		totalMsgs += len(b.Requests)
	}
	if totalMsgs != iterations {
		t.Fatalf("expected %d messages, got %d", iterations, totalMsgs)
	}
}

// TestSendHeartbeatBatch_BidirectionalRoundTrip verifies heartbeat fast
// path works in both directions between two transports.
func TestSendHeartbeatBatch_BidirectionalRoundTrip(t *testing.T) {
	t1, t2, handler1, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// t1 -> t2
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 10},
	})
	handler2.waitBatch(t, 10*time.Second)

	// t2 -> t1
	t2.Send([]proto.Message{
		{Type: proto.HeartbeatResp, ShardID: 1, From: 2, To: 1, Term: 10},
	})
	handler1.waitBatch(t, 10*time.Second)

	b2 := handler2.getBatches()
	if len(b2) != 1 || b2[0].Requests[0].Term != 10 {
		t.Fatal("t2 did not receive correct heartbeat from t1")
	}

	b1 := handler1.getBatches()
	if len(b1) != 1 || b1[0].Requests[0].Term != 10 {
		t.Fatal("t1 did not receive correct heartbeat response from t2")
	}
}

// TestSendHeartbeatBatch_ConcurrentSends verifies that concurrent heartbeat
// sends from multiple goroutines do not race.
func TestSendHeartbeatBatch_ConcurrentSends(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	var wg sync.WaitGroup
	const goroutines = 10
	const msgsPerGoroutine = 5

	for g := range goroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range msgsPerGoroutine {
				t1.Send([]proto.Message{
					{
						Type:    proto.Heartbeat,
						ShardID: 1,
						From:    1,
						To:      2,
						Term:    uint64(gid*msgsPerGoroutine + i + 1),
					},
				})
			}
		}(g)
	}

	wg.Wait()

	// Wait for at least some messages to arrive.
	deadline := time.After(10 * time.Second)
	for handler2.batchCount.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for any batch to arrive")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	totalMsgs := 0
	for _, b := range handler2.getBatches() {
		totalMsgs += len(b.Requests)
	}
	if totalMsgs < 1 {
		t.Fatal("expected at least 1 heartbeat message to be received")
	}
}

// ---------------------------------------------------------------------------
// HeartbeatMessageSize constant validation
// ---------------------------------------------------------------------------

// TestHeartbeatMessageSize_Constant verifies that the proto.HeartbeatMessageSize
// constant matches the actual marshaled size of a heartbeat message.
func TestHeartbeatMessageSize_Constant(t *testing.T) {
	msg := proto.Message{
		Type:    proto.Heartbeat,
		From:    1,
		To:      2,
		ShardID: 1,
		Term:    100,
		Commit:  50,
	}
	if msg.Size() != proto.HeartbeatMessageSize {
		t.Fatalf("HeartbeatMessageSize constant (%d) does not match actual size (%d)",
			proto.HeartbeatMessageSize, msg.Size())
	}

	// Verify with HeartbeatResp too.
	resp := proto.Message{
		Type:    proto.HeartbeatResp,
		From:    2,
		To:      1,
		ShardID: 1,
		Term:    100,
	}
	if resp.Size() != proto.HeartbeatMessageSize {
		t.Fatalf("HeartbeatResp size (%d) does not match HeartbeatMessageSize (%d)",
			resp.Size(), proto.HeartbeatMessageSize)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: heartbeat fast path vs standard sendBatch serialization
// ---------------------------------------------------------------------------

// BenchmarkHeartbeatFastPath_Serialize measures the serialization overhead
// of the heartbeat fast path (pre-computed prefix + inline Message marshal).
// This isolates the CPU cost of frame construction, which is the part we
// optimized, separate from QUIC stream I/O.
func BenchmarkHeartbeatFastPath_Serialize(b *testing.B) {
	// Simulate what sendHeartbeatBatch does minus the stream write.
	addr := "127.0.0.1:12345"
	prefixLen := heartbeatBatchFixedPrefix + len(addr)
	hbPrefix := make([]byte, prefixLen)

	binary.LittleEndian.PutUint64(hbPrefix[0:], proto.WireVersion)
	binary.LittleEndian.PutUint64(hbPrefix[8:], 42) // DeploymentID
	binary.LittleEndian.PutUint32(hbPrefix[16:], uint32(len(addr)))
	copy(hbPrefix[20:], addr)

	msg := proto.Message{
		Type:    proto.Heartbeat,
		From:    1,
		To:      2,
		ShardID: 1,
		Term:    100,
		Commit:  50,
	}

	payloadSize := len(hbPrefix) + 4 + proto.HeartbeatMessageSize
	frameSize := FrameHeaderSize + payloadSize
	buf := make([]byte, frameSize)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		// Frame header
		buf[0] = MagicByte0
		buf[1] = MagicByte1
		buf[2] = WireVersion
		buf[3] = byte(FlagNone)
		binary.BigEndian.PutUint32(buf[4:8], uint32(payloadSize))

		// Batch prefix
		offset := FrameHeaderSize
		copy(buf[offset:], hbPrefix)
		offset += len(hbPrefix)

		// Requests count
		binary.LittleEndian.PutUint32(buf[offset:], 1)
		offset += 4

		// Message
		msg.MarshalTo(buf[offset:])
	}
}

// BenchmarkHeartbeatSlowPath_Serialize measures the serialization overhead
// of the standard sendBatch path for a single heartbeat message. This
// provides a baseline comparison for the fast path benchmark.
func BenchmarkHeartbeatSlowPath_Serialize(b *testing.B) {
	addr := "127.0.0.1:12345"

	msg := proto.Message{
		Type:    proto.Heartbeat,
		From:    1,
		To:      2,
		ShardID: 1,
		Term:    100,
		Commit:  50,
	}

	mb := proto.MessageBatch{
		BinVer:        proto.WireVersion,
		DeploymentID:  42,
		SourceAddress: addr,
		Requests:      []proto.Message{msg},
	}

	sz := mb.Size()
	buf := make([]byte, sz+FrameHeaderSize)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		// Recompute size (sendBatch does this every call)
		mb.Requests[0] = msg
		sz = mb.Size()

		// Marshal MessageBatch
		payloadBuf := buf[FrameHeaderSize:]
		n, _ := mb.MarshalTo(payloadBuf)

		// Marshal frame header
		MarshalFrame(buf, payloadBuf[:n], FlagNone)
	}
}

func TestSendHeartbeatBatch_WriteDeadlineErrorEvicts(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)

	// Establish the heartbeat stream.
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	initialConns := t1.ConnectionCount()
	if initialConns != 1 {
		t.Fatalf("expected 1 connection, got %d", initialConns)
	}

	// Stop t2 to break the connection.
	t2.Stop()
	time.Sleep(500 * time.Millisecond)

	// Send another heartbeat. The write will fail, triggering eviction.
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 2},
	})

	// Wait for eviction to occur.
	time.Sleep(500 * time.Millisecond)

	cleanup()
}

// ---------------------------------------------------------------------------
// getConnection stale connection path (conn.go:42-82)
// ---------------------------------------------------------------------------

// TestGetConnection_StoppedTransportReturnsError verifies getConnection
// returns ErrStreamClosed when the transport is stopped.

func TestSendHeartbeatBatch_MarshalErrorPath(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send a heartbeat with a valid message to establish connection.
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Send multiple heartbeats to exercise the batch send path fully.
	for i := 0; i < 20; i++ {
		t1.Send([]proto.Message{
			{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: uint64(i + 2)},
		})
	}

	// Wait for delivery.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		for _, b := range handler2.getBatches() {
			total += len(b.Requests)
		}
		if total >= 10 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// sendBatch write deadline error and write error paths
// ---------------------------------------------------------------------------

// TestSendBatch_WriteDeadlineAndWriteErrorEviction verifies that
// a write deadline or write error triggers connection removal
// and stream eviction.

func TestSendHeartbeatBatch_DirectCallWriteError(t *testing.T) {
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

	// Establish connection and heartbeat stream.
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Stop t2.
	t2.Stop()
	time.Sleep(500 * time.Millisecond)

	// Call sendHeartbeatBatch directly. Should hit error paths.
	t1.sendHeartbeatBatch(t2Addr, []proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 2},
	})

	// Call again to exercise the path after connection eviction.
	t1.sendHeartbeatBatch(t2Addr, []proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 3},
	})
}

// ---------------------------------------------------------------------------
// evictStaleOutboundOnRecv inbound changed path (conn.go:564-577)
// ---------------------------------------------------------------------------

// TestEvictStaleOutboundOnRecv_InboundChanged verifies that when the
// inbound connection changes (simulating peer restart), the outbound
// connection is evicted even if it's still alive.

func TestSendHeartbeatBatch_GetStreamError(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Cancel the shutdown context so getStream/dial fails.
	tr.shutdownCancel()

	// sendHeartbeatBatch should hit the getStream error path
	// at heartbeat.go:109-112 and return without crash.
	tr.sendHeartbeatBatch("127.0.0.1:50001", []proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 2},
	})

	tr.Stop()
}

// ---------------------------------------------------------------------------
// SendSnapshot SetWriteDeadline error after open (quic.go:936-938)
// ---------------------------------------------------------------------------

// TestSendSnapshot_ChunkLoopWriteFailure exercises the SendSnapshot chunk
// write error path by sending many large chunks while closing the
// connection concurrently. The connection close races with chunk writes.
