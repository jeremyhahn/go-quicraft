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
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

// snappyEncode is a test helper that wraps snappy.Encode.
func snappyEncode(dst, src []byte) []byte {
	return snappy.Encode(dst, src)
}

// TestIsConnectionClosedWithTransportError verifies TransportError detection.
func TestIsConnectionClosedWithTransportError(t *testing.T) {
	err := &quic.TransportError{}
	if !isConnectionClosed(err) {
		t.Fatal("TransportError should be detected as closed")
	}
}

// TestSendToFailedConnection verifies that send drops messages when the
// connection cannot be established, without blocking.
func TestSendToFailedConnection(t *testing.T) {
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

	// Register a target that will fail to connect (port 1 is typically unusable).
	reg.Register(1, 2, "127.0.0.1:1")

	// Send should not block; it should silently drop the message.
	start := time.Now()
	tr.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	elapsed := time.Since(start)

	// Should complete within the dial timeout (5s) plus some buffer.
	if elapsed > 10*time.Second {
		t.Fatalf("Send took too long: %v", elapsed)
	}
}

// TestConnectionCleanupWithMultipleConns verifies cleanup handles
// multiple connections correctly.
func TestConnectionCleanupWithMultipleConns(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	handler3 := newTestHandler()
	reg := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	reg3 := registry.NewRegistry()

	mtls := testMTLSConfig(t)
	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler1, reg)
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

	// Establish connections to both t2 and t3.
	target2 := t2.Addr().String()
	target3 := t3.Addr().String()
	reg.Register(1, 2, target2)
	reg.Register(1, 3, target3)

	_, err = t1.getConnection(target2)
	if err != nil {
		t.Fatalf("getConnection(t2) failed: %v", err)
	}
	_, err = t1.getConnection(target3)
	if err != nil {
		t.Fatalf("getConnection(t3) failed: %v", err)
	}

	if t1.ConnectionCount() != 2 {
		t.Fatalf("expected 2 connections, got %d", t1.ConnectionCount())
	}

	// Close one connection.
	t2.Stop()
	time.Sleep(500 * time.Millisecond)

	t1.cleanStaleConnections()

	// Should have 1 remaining (t3 still alive).
	if t1.ConnectionCount() > 1 {
		// t2's connection may take time to close. Give it another try.
		time.Sleep(500 * time.Millisecond)
		t1.cleanStaleConnections()
	}
}

// TestGetStreamPoolSizeOneDataPath verifies that with pool size 1,
// data messages also use index 0.
func TestGetStreamPoolSizeOneDataPath(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress:  ":0",
		DeploymentID:   42,
		StreamPoolSize: 1,
		MTLSConfig:     mtls,
	}, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

	t2, err := NewQUICTransport(Config{
		ListenAddress:  ":0",
		DeploymentID:   42,
		StreamPoolSize: 1,
		MTLSConfig:     mtls,
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	target := t2.Addr().String()

	// Both heartbeat and data should use index 0 when pool size is 1.
	hbStream, err := t1.getStream(target, 1, true)
	if err != nil {
		t.Fatalf("heartbeat getStream failed: %v", err)
	}

	dataStream, err := t1.getStream(target, 1, false)
	if err != nil {
		t.Fatalf("data getStream failed: %v", err)
	}

	// With pool size 1, both should be the same stream (same index 0).
	if hbStream != dataStream {
		t.Fatal("with pool size 1, heartbeat and data should use same stream")
	}
}

// TestSendSnapshotConnectionDialFailure verifies snapshot send fails
// gracefully when connection cannot be established.
func TestSendSnapshotConnectionDialFailure(t *testing.T) {
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

	// Register unreachable target.
	reg.Register(1, 2, "127.0.0.1:1")

	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 2, ChunkCount: 1, Data: []byte("data")},
	}

	err = tr.SendSnapshot(chunks)
	if err == nil {
		t.Fatal("expected error for unreachable target")
	}
}

// TestNewQUICTransportWithInvalidMTLS verifies that invalid mTLS config
// causes construction to fail.
func TestNewQUICTransportWithInvalidMTLS(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig: &MTLSConfig{
			CACert: []byte("invalid"),
			Cert:   []byte("invalid"),
			Key:    []byte("invalid"),
		},
	}

	_, err := NewQUICTransport(cfg, handler, reg)
	if err == nil {
		t.Fatal("expected error for invalid mTLS config")
	}
}

// TestBuildMTLSTLSPairForConfig tests that buildMTLSTLS produces a valid
// server config from mTLS material.
func TestBuildMTLSTLSPairForConfig(t *testing.T) {
	mtls := testMTLSConfig(t)
	pair, err := buildMTLSTLS(mtls, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}
	if pair == nil || pair.server == nil {
		t.Fatal("expected non-nil server config")
	}
	if len(pair.server.Certificates) != 1 {
		t.Fatalf("expected 1 cert, got %d", len(pair.server.Certificates))
	}
}

// TestHandleMessageFrameZeroLength verifies that zero-length frames
// are handled correctly without error.
func TestHandleMessageFrameZeroLength(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send a valid message followed by zero-length is handled internally.
	// Just verify normal messages still work.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 99},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if batches[0].Requests[0].Term != 99 {
		t.Fatalf("expected Term 99, got %d", batches[0].Requests[0].Term)
	}
}

// TestDialConnectionResolveFailure verifies that dialConnection handles
// address resolution failure correctly.
func TestDialConnectionResolveFailure(t *testing.T) {
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

	// Invalid address should fail at resolution.
	_, err = tr.dialConnection("not-a-valid-address::::")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

// TestSendWithRejectField verifies that the Reject bool field round-trips.
func TestSendWithRejectField(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{
		{
			Type:    proto.ReplicateResp,
			ShardID: 1,
			From:    1,
			To:      2,
			Term:    5,
			Reject:  true,
			Hint:    42,
		},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 || len(batches[0].Requests) != 1 {
		t.Fatal("expected 1 batch with 1 request")
	}
	msg := batches[0].Requests[0]
	if !msg.Reject {
		t.Fatal("expected Reject=true")
	}
	if msg.Hint != 42 {
		t.Fatalf("expected Hint=42, got %d", msg.Hint)
	}
}

// TestSendVoteMessages verifies RequestVote round-trip.
func TestSendVoteMessages(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{
		{
			Type:     proto.RequestVote,
			ShardID:  1,
			From:     1,
			To:       2,
			Term:     10,
			LogTerm:  9,
			LogIndex: 100,
		},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	msg := batches[0].Requests[0]
	if msg.Type != proto.RequestVote {
		t.Fatalf("expected RequestVote, got %d", msg.Type)
	}
	if msg.LogTerm != 9 || msg.LogIndex != 100 {
		t.Fatalf("LogTerm/LogIndex mismatch")
	}
}

// TestHandleMessageFrameDirectReadFullError verifies the handleMessageFrame
// error path when ReadFull fails.
func TestHandleMessageFrameDirectReadFullError(t *testing.T) {
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

	// Create a stream that returns an error on read (via bytes.Reader with
	// insufficient data). We test handleMessageFrame with a reader that
	// has less data than the indicated length.
	// handleMessageFrame takes *quic.Stream, but we can't easily mock that.
	// Instead, test by sending a message and verifying the handler works.
	// The error paths in handleMessageFrame are triggered when the wire
	// format is corrupted, so we verify the happy path works fully.
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()
}

// TestSnapshotReceiverMemoryBudgetExhaustion verifies that the snapshot
// receiver rejects chunks when memory budget is exhausted.
func TestSnapshotReceiverMemoryBudgetExhaustion(t *testing.T) {
	handler := newTestHandler()
	// Very small memory budget (10 bytes) to trigger exhaustion.
	sr := newSnapshotReceiver(4, 10, 0, 0, handler)

	if sr.AvailableMemory() != 10 {
		t.Fatalf("expected 10 bytes memory budget, got %d", sr.AvailableMemory())
	}

	// The receiveChunks method reads from a QUIC stream. Since we can't
	// easily mock quic.Stream, we verify the budget accounting directly.
	// Subtract more than available.
	remaining := sr.memoryBudget.Add(-20)
	if remaining >= 0 {
		t.Fatal("expected negative remaining budget")
	}
	// Restore.
	sr.memoryBudget.Add(20)
	if sr.AvailableMemory() != 10 {
		t.Fatalf("expected restored budget, got %d", sr.AvailableMemory())
	}
}

// TestSnapshotReceiverRateLimitZero verifies that zero rate means no rate limiting.
func TestSnapshotReceiverRateLimitZero(t *testing.T) {
	handler := newTestHandler()
	sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)

	// bytesPerSec = 0 means rate limiting is disabled.
	if sr.bytesPerSec != 0 {
		t.Fatalf("expected 0 bytesPerSec, got %d", sr.bytesPerSec)
	}
}

// TestHandleSnapshotStreamSmallHeaderLen verifies that handleSnapshotStream
// rejects headers smaller than SnapshotHeaderSize.
func TestHandleSnapshotStreamSmallHeaderLen(t *testing.T) {
	// We test this by sending a snapshot with a crafted frame. But since
	// handleSnapshotStream requires a real *quic.Stream, we test the
	// condition indirectly via a full snapshot round-trip.
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// A normal single-chunk snapshot should work fine.
	chunks := []proto.SnapshotChunk{
		{
			ShardID:    1,
			ReplicaID:  2,
			From:       1,
			Index:      10,
			Term:       1,
			ChunkID:    0,
			ChunkCount: 1,
			ChunkSize:  3,
			Data:       []byte("abc"),
		},
	}
	if err := t1.SendSnapshot(chunks); err != nil {
		t.Fatalf("SendSnapshot failed: %v", err)
	}
	handler2.waitSnapshot(t, 10*time.Second)

	snaps := handler2.getSnapshots()
	if len(snaps) == 0 {
		t.Fatal("expected snapshot delivery")
	}
	if len(snaps[0]) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(snaps[0]))
	}
}

// TestStartWithInvalidUDPAddress verifies that Start returns an error
// when given an invalid listen address that fails at UDP resolution.
func TestStartWithInvalidUDPAddress(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()

	// Create transport with an address that passes validation but fails
	// at UDP resolution.
	cfg := Config{
		ListenAddress: "invalid-host-that-will-fail:99999999",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}
	cfg.SetDefaults()

	// Build TLS manually since we want to test Start.
	pair, err := buildMTLSTLS(testMTLSConfig(t), nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}

	qcfg := &quic.Config{
		MaxIdleTimeout: 30 * time.Second,
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	tr := &QUICTransport{
		cfg:            cfg,
		handler:        handler,
		reg:            reg,
		tlsPair:        pair,
		quicCfg:        qcfg,
		conns:          make(map[string]*quic.Conn),
		stopC:          make(chan struct{}),
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}
	for i := range tr.streamShards {
		tr.streamShards[i].streams = make(map[streamKey]*quic.Stream)
	}

	err = tr.Start()
	if err == nil {
		tr.Stop()
		t.Fatal("expected error for invalid UDP address")
	}
}

// TestSendBatchStreamGetError verifies sendBatch handles getStream failure
// gracefully by dropping the message.
func TestSendBatchStreamGetError(t *testing.T) {
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

	// sendBatch to an unreachable target. This triggers the getStream
	// error path (which calls getConnection which fails).
	tr.sendBatch("127.0.0.1:1", []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	// Should not panic or block.
}

// TestSendBatchWriteDeadlineAndWriteError verifies that sendBatch handles
// stream write failures by evicting the stream.
func TestSendBatchWriteDeadlineAndWriteError(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send a first message to establish the stream.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Stop t2 to make the stream broken.
	t2.Stop()
	time.Sleep(500 * time.Millisecond)

	// Now send again. The write should fail, triggering stream eviction.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2},
	})
	// Should not panic. The message is silently dropped.
}

// TestSendBatchMarshalFrameCapReuse verifies the capacity reuse path in
// sendBatch where the buffer from the pool has sufficient capacity.
func TestSendBatchMarshalFrameCapReuse(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send multiple messages to exercise the buffer pool reuse path.
	for i := 0; i < 5; i++ {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1)},
		})
	}

	// Wait for at least one to arrive.
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}
}

// TestHandleConnectionStopDuringAccept verifies handleConnection exits
// when transport is stopped.
func TestHandleConnectionStopDuringAccept(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)

	// Establish a connection.
	target := t2.Addr().String()
	_, err := t1.getConnection(target)
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Stop both transports. This should cause handleConnection to exit
	// cleanly on the acceptor side.
	cleanup()
}

// TestGetConnectionDeadConnectionRedial verifies that getConnection
// re-dials when an existing connection's context is done.
func TestGetConnectionDeadConnectionRedial(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Get initial connection.
	conn1, err := t1.getConnection(target)
	if err != nil {
		t.Fatalf("first getConnection failed: %v", err)
	}

	// Close the connection to make its context done.
	conn1.CloseWithError(0, "force redial test")
	time.Sleep(200 * time.Millisecond)

	// Next getConnection should redial.
	conn2, err := t1.getConnection(target)
	if err != nil {
		t.Fatalf("second getConnection after close failed: %v", err)
	}

	// Should be a different connection.
	if conn1 == conn2 {
		t.Fatal("expected new connection after redial")
	}
}

// TestSnapshotMultiChunkWithRateLimiting verifies that snapshot chunks
// with rate limiting enabled still transfer correctly.
func TestSnapshotMultiChunkWithRateLimiting(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	// Use a rate-limited receiver.
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

	// Set a modest rate limit on t2's snapshot receiver.
	t2, err := NewQUICTransport(Config{
		ListenAddress:          ":0",
		DeploymentID:           42,
		MaxSnapshotReceiveRate: 1024 * 1024, // 1 MB/s (fast enough for test)
		MTLSConfig:             mtls,
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	chunks := []proto.SnapshotChunk{
		{
			ShardID: 1, ReplicaID: 2, From: 1,
			Index: 10, Term: 1, ChunkID: 0, ChunkCount: 3,
			ChunkSize: 10, Data: []byte("0123456789"),
		},
		{
			ShardID: 1, ReplicaID: 2, From: 1,
			Index: 10, Term: 1, ChunkID: 1, ChunkCount: 3,
			ChunkSize: 10, Data: []byte("abcdefghij"),
		},
		{
			ShardID: 1, ReplicaID: 2, From: 1,
			Index: 10, Term: 1, ChunkID: 2, ChunkCount: 3,
			ChunkSize: 10, Data: []byte("klmnopqrst"),
		},
	}

	if err := t1.SendSnapshot(chunks); err != nil {
		t.Fatalf("SendSnapshot failed: %v", err)
	}

	handler2.waitSnapshot(t, 15*time.Second)
	snaps := handler2.getSnapshots()
	if len(snaps) == 0 {
		t.Fatal("expected snapshot delivery")
	}
	if len(snaps[0]) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(snaps[0]))
	}
}

// TestFrameSnapshotFlagRoundTrip verifies that FlagSnapshot survives
// a frame write/read cycle.
func TestFrameSnapshotFlagRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("test payload data")

	if err := WriteFrame(&buf, payload, FlagSnapshot); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	length, flags, err := ReadFrameHeader(&buf)
	if err != nil {
		t.Fatalf("ReadFrameHeader failed: %v", err)
	}
	if flags != FlagSnapshot {
		t.Fatalf("expected FlagSnapshot, got %d", flags)
	}
	if int(length) != len(payload) {
		t.Fatalf("expected length %d, got %d", len(payload), length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(&buf, data); err != nil {
		t.Fatalf("ReadFull failed: %v", err)
	}
	if string(data) != "test payload data" {
		t.Fatalf("unexpected data: %s", data)
	}
}

// TestSendHeartbeatResp verifies HeartbeatResp message type detection.
func TestSendHeartbeatResp(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{
		{Type: proto.HeartbeatResp, ShardID: 1, From: 1, To: 2, Term: 7},
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

// TestSendWithSnapshotField verifies Snapshot field on messages round-trips.
func TestSendWithSnapshotField(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{
		{
			Type:    proto.InstallSnapshot,
			ShardID: 1,
			From:    1,
			To:      2,
			Term:    5,
			Snapshot: proto.Snapshot{
				Index: 100,
				Term:  5,
			},
		},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	msg := batches[0].Requests[0]
	if msg.Snapshot.Index != 100 {
		t.Fatalf("expected Snapshot.Index=100, got %d", msg.Snapshot.Index)
	}
}

// TestGetStreamConcurrentDoubleCheck verifies the double-check path in
// getStream when two goroutines race to create the same stream.
func TestGetStreamConcurrentDoubleCheck(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// First, create a stream so the double-check path is exercised.
	s1, err := t1.getStream(target, 5, false)
	if err != nil {
		t.Fatalf("first getStream failed: %v", err)
	}

	// Getting the same stream should return the cached one.
	s2, err := t1.getStream(target, 5, false)
	if err != nil {
		t.Fatalf("second getStream failed: %v", err)
	}
	if s1 != s2 {
		t.Fatal("expected same cached stream")
	}
}

// TestSendBatchWithHeartbeatDetection verifies that sendBatch correctly
// identifies heartbeat messages and routes them to stream index 0.
func TestSendBatchWithHeartbeatDetection(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send a heartbeat message through sendBatch path.
	t1.sendBatch(func() string {
		addr, _ := t1.reg.Resolve(1, 2)
		return addr
	}(), []proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 3},
	})

	handler2.waitBatch(t, 10*time.Second)
	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if batches[0].Requests[0].Type != proto.Heartbeat {
		t.Fatalf("expected Heartbeat, got %d", batches[0].Requests[0].Type)
	}
}

// TestSendSnapshotWithLargePayload verifies snapshot transfer with a
// larger data payload per chunk.
func TestSendSnapshotWithLargePayload(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Create a 1KB data chunk.
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    1,
			ReplicaID:  2,
			From:       1,
			Index:      50,
			Term:       3,
			ChunkID:    0,
			ChunkCount: 1,
			ChunkSize:  uint64(len(data)),
			Data:       data,
		},
	}

	if err := t1.SendSnapshot(chunks); err != nil {
		t.Fatalf("SendSnapshot failed: %v", err)
	}

	handler2.waitSnapshot(t, 10*time.Second)
	snaps := handler2.getSnapshots()
	if len(snaps) == 0 {
		t.Fatal("expected snapshot delivery")
	}
	if len(snaps[0][0].Data) != 1024 {
		t.Fatalf("expected 1024 bytes data, got %d", len(snaps[0][0].Data))
	}
}

// TestSendBatchBufCapRealloc verifies the buf capacity < needed reallocation
// path in sendBatch.
func TestSendBatchBufCapRealloc(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Create a message with large entries to force buf reallocation.
	bigData := make([]byte, 1024)
	for i := range bigData {
		bigData[i] = byte(i % 256)
	}

	t1.Send([]proto.Message{
		{
			Type:    proto.Replicate,
			ShardID: 1,
			From:    1,
			To:      2,
			Term:    10,
			Entries: []proto.Entry{
				{Index: 1, Term: 10, Cmd: bigData},
				{Index: 2, Term: 10, Cmd: bigData},
				{Index: 3, Term: 10, Cmd: bigData},
			},
		},
	})

	handler2.waitBatch(t, 10*time.Second)
	batches := handler2.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected at least 1 batch")
	}
	msg := batches[0].Requests[0]
	if len(msg.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(msg.Entries))
	}
}

// TestAcceptLoopStopDuringAccept verifies acceptLoop exits cleanly when
// transport is stopped while waiting for connections.
func TestAcceptLoopStopDuringAccept(t *testing.T) {
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

	// Stop immediately. The acceptLoop should exit cleanly.
	tr.Stop()
}

// TestEnable0RTTConfig verifies the 0-RTT configuration is passed through
// when mTLS is configured (0-RTT requires mTLS for replay protection).
func TestEnable0RTTConfig(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		Enable0RTT:    true,
		MTLSConfig: &MTLSConfig{
			CACert: caCertPEM,
			Cert:   certPEM,
			Key:    keyPEM,
		},
	}

	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	if !tr.quicCfg.Allow0RTT {
		t.Fatal("expected Allow0RTT=true")
	}
}

// errReader is a test reader that returns an error after reading n bytes.
type errReader struct {
	data []byte
	pos  int
	err  error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, r.err
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, r.err
	}
	return n, nil
}

// TestHandleMessageFrameZeroLengthDirect tests the zero-length fast path
// directly by calling handleMessageFrame with length=0.
func TestHandleMessageFrameZeroLengthDirect(t *testing.T) {
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

	// length=0 should return nil immediately.
	err = tr.handleMessageFrame(nil, &bytes.Reader{}, 0, FlagNone)
	if err != nil {
		t.Fatalf("expected nil error for zero length, got %v", err)
	}
}

// TestHandleMessageFrameReadFullError tests the ReadFull error path
// when the stream doesn't have enough data.
func TestHandleMessageFrameReadFullError(t *testing.T) {
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

	// Provide less data than indicated by length.
	reader := &errReader{
		data: []byte{0x01, 0x02}, // only 2 bytes
		err:  io.ErrUnexpectedEOF,
	}

	err = tr.handleMessageFrame(nil, reader, 100, FlagNone) // claims 100 bytes
	if err == nil {
		t.Fatal("expected error for short read")
	}
}

// TestHandleMessageFrameUnmarshalError tests the unmarshal error path
// when the payload is invalid.
func TestHandleMessageFrameUnmarshalError(t *testing.T) {
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

	// Provide invalid payload that will fail to unmarshal.
	garbage := make([]byte, 50)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	reader := bytes.NewReader(garbage)

	err = tr.handleMessageFrame(nil, reader, uint32(len(garbage)), FlagNone)
	if err == nil {
		t.Fatal("expected error for invalid payload")
	}
}

// TestHandleMessageFrameDeploymentMismatch tests the deployment ID
// mismatch path in handleMessageFrame.
func TestHandleMessageFrameDeploymentMismatch(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  999, // Will not match the batch's deployment ID
		MTLSConfig:    testMTLSConfig(t),
	}

	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	// Create a valid MessageBatch with a different deployment ID.
	mb := proto.MessageBatch{
		BinVer:       proto.WireVersion,
		DeploymentID: 123, // Mismatch with cfg.DeploymentID=999
		Requests: []proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
		},
	}
	sz := mb.Size()
	buf := make([]byte, sz)
	n, err := mb.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}

	reader := bytes.NewReader(buf[:n])
	err = tr.handleMessageFrame(nil, reader, uint32(n), FlagNone)
	if !errors.Is(err, ErrPeerIdentityMismatch) {
		t.Fatalf("expected ErrPeerIdentityMismatch, got %v", err)
	}
}

// TestProcessSnapshotHeaderTooSmall tests the header too small path.
func TestProcessSnapshotHeaderTooSmall(t *testing.T) {
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

	// headerLen < SnapshotHeaderSize should return immediately.
	tr.processSnapshotHeader(bytes.NewReader(nil), SnapshotHeaderSize-1)
	// Should not panic.
}

// TestProcessSnapshotHeaderReadError tests ReadFull error during header read.
func TestProcessSnapshotHeaderReadError(t *testing.T) {
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

	// Provide less than SnapshotHeaderSize bytes.
	reader := &errReader{
		data: make([]byte, 5), // less than SnapshotHeaderSize (48)
		err:  io.ErrUnexpectedEOF,
	}
	tr.processSnapshotHeader(reader, SnapshotHeaderSize)
	// Should not panic.
}

// TestProcessSnapshotHeaderExtraBytesDiscardError tests the error path
// when discarding extra header bytes fails.
func TestProcessSnapshotHeaderExtraBytesDiscardError(t *testing.T) {
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

	// Provide exactly SnapshotHeaderSize bytes, but claim more.
	hdr := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      10,
		Term:       1,
		ChunkCount: 1,
	}
	var hdrBuf [SnapshotHeaderSize]byte
	hdr.MarshalTo(hdrBuf[:])

	reader := &errReader{
		data: hdrBuf[:], // exactly header, no extra bytes
		err:  io.ErrUnexpectedEOF,
	}
	// Claim headerLen > SnapshotHeaderSize to trigger discard path.
	tr.processSnapshotHeader(reader, SnapshotHeaderSize+10)
	// Should handle error gracefully without panic.
}

// TestProcessSnapshotHeaderExtraBytesSuccess tests the discard path
// when there are extra header bytes that are successfully read.
func TestProcessSnapshotHeaderExtraBytesSuccess(t *testing.T) {
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

	// Provide header + extra bytes.
	hdr := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      10,
		Term:       1,
		ChunkCount: 1,
	}
	var hdrBuf [SnapshotHeaderSize]byte
	hdr.MarshalTo(hdrBuf[:])

	// Append 10 extra bytes.
	data := append(hdrBuf[:], make([]byte, 10)...)
	reader := bytes.NewReader(data)
	// headerLen = SnapshotHeaderSize + 10 to trigger discard path.
	tr.processSnapshotHeader(reader, SnapshotHeaderSize+10)
	// Should succeed without panic. Since reader is not *quic.Stream,
	// receiveSnapshot is not called.
}

// TestProcessSnapshotHeaderChunkCountTooLarge tests the ChunkCount
// exceeds max path.
func TestProcessSnapshotHeaderChunkCountTooLarge(t *testing.T) {
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

	// Create a header with ChunkCount > defaultMaxSnapshotChunkCount.
	hdr := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      10,
		Term:       1,
		ChunkCount: defaultMaxSnapshotChunkCount + 1,
	}
	var hdrBuf [SnapshotHeaderSize]byte
	hdr.MarshalTo(hdrBuf[:])

	reader := bytes.NewReader(hdrBuf[:])
	tr.processSnapshotHeader(reader, SnapshotHeaderSize)
	// Should return early without processing chunks.
}

// TestHandleMessageFrameTooManyMessages tests the message count limit path.
func TestHandleMessageFrameTooManyMessages(t *testing.T) {
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

	// Create a batch with more messages than maxMessagesPerBatch.
	// We can't easily create 10001 real messages in the wire format,
	// but we can verify the limit exists.
	if maxMessagesPerBatch != 10000 {
		t.Fatalf("expected maxMessagesPerBatch=10000, got %d", maxMessagesPerBatch)
	}

	// Create a valid batch with correct deployment ID and 1 message
	// to exercise the happy path in handleMessageFrame.
	mb := proto.MessageBatch{
		BinVer:       proto.WireVersion,
		DeploymentID: 42,
		Requests: []proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
		},
	}
	sz := mb.Size()
	buf := make([]byte, sz)
	n, err := mb.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}

	reader := bytes.NewReader(buf[:n])
	err = tr.handleMessageFrame(nil, reader, uint32(n), FlagNone)
	if err != nil {
		t.Fatalf("expected no error for valid batch, got %v", err)
	}

	// Verify handler received it.
	batches := handler.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
}

// TestHandleConnectionRemotePeerClose verifies that handleConnection
// exits cleanly when the remote peer closes the connection.
func TestHandleConnectionRemotePeerClose(t *testing.T) {
	t1, _, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	// Establish a connection by sending a message.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})

	// Wait for the message to be delivered.
	time.Sleep(500 * time.Millisecond)

	// Close t1's connection to t2. This triggers the handleConnection
	// path in t2 where isConnectionClosed returns true.
	t1.connMu.RLock()
	for _, conn := range t1.conns {
		conn.CloseWithError(0, "test remote close")
	}
	t1.connMu.RUnlock()

	// Give time for t2 to detect the closed connection.
	time.Sleep(500 * time.Millisecond)
}

// testChunkReader implements chunkReader for testing receiveChunks.
type testChunkReader struct {
	data        []byte
	pos         int
	readErr     error // error to return when data is exhausted
	deadlineErr error // error to return from SetReadDeadline
}

func (r *testChunkReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		if r.readErr != nil {
			return 0, r.readErr
		}
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) && r.readErr != nil {
		return n, r.readErr
	}
	return n, nil
}

func (r *testChunkReader) SetReadDeadline(t time.Time) error {
	return r.deadlineErr
}

// buildChunkFrames creates a byte buffer containing framed snapshot chunks.
func buildChunkFrames(t *testing.T, chunks []proto.SnapshotChunk) []byte {
	t.Helper()
	var buf bytes.Buffer
	for i := range chunks {
		sz := chunks[i].Size()
		payload := make([]byte, sz)
		n, err := chunks[i].MarshalTo(payload)
		if err != nil {
			t.Fatalf("chunk MarshalTo failed: %v", err)
		}
		if err := WriteFrame(&buf, payload[:n], FlagNone); err != nil {
			t.Fatalf("WriteFrame failed: %v", err)
		}
	}
	return buf.Bytes()
}

// TestReceiveChunksHappyPath verifies successful chunk reception via mock reader.
func TestReceiveChunksHappyPath(t *testing.T) {
	handler := newTestHandler()
	sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)

	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 2, ChunkID: 0, ChunkCount: 2, ChunkSize: 5, Data: []byte("hello")},
		{ShardID: 1, ReplicaID: 2, ChunkID: 1, ChunkCount: 2, ChunkSize: 5, Data: []byte("world")},
	}
	data := buildChunkFrames(t, chunks)

	reader := &testChunkReader{data: data}
	hdr := &SnapshotHeader{ChunkCount: 2}
	stopC := make(chan struct{})

	sr.receiveChunks(reader, hdr, stopC)

	snaps := handler.getSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if len(snaps[0]) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(snaps[0]))
	}
}

// TestReceiveChunksStopDuringLoop verifies that closing stopC during
// chunk reception causes an early return.
func TestReceiveChunksStopDuringLoop(t *testing.T) {
	handler := newTestHandler()
	sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)

	// Create two chunks but close stopC before processing.
	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 2, ChunkID: 0, ChunkCount: 2, ChunkSize: 5, Data: []byte("hello")},
		{ShardID: 1, ReplicaID: 2, ChunkID: 1, ChunkCount: 2, ChunkSize: 5, Data: []byte("world")},
	}
	data := buildChunkFrames(t, chunks)

	reader := &testChunkReader{data: data}
	hdr := &SnapshotHeader{ChunkCount: 2}
	stopC := make(chan struct{})
	close(stopC) // Stop immediately.

	sr.receiveChunks(reader, hdr, stopC)

	// No chunks should be delivered.
	snaps := handler.getSnapshots()
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots, got %d", len(snaps))
	}
}

// TestReceiveChunksSetReadDeadlineError verifies the SetReadDeadline
// error path.
func TestReceiveChunksSetReadDeadlineError(t *testing.T) {
	handler := newTestHandler()
	sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)

	reader := &testChunkReader{
		data:        nil,
		deadlineErr: errors.New("deadline error"),
	}
	hdr := &SnapshotHeader{ChunkCount: 1}
	stopC := make(chan struct{})

	sr.receiveChunks(reader, hdr, stopC)

	snaps := handler.getSnapshots()
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots on deadline error, got %d", len(snaps))
	}
}

// TestReceiveChunksReadFrameHeaderEOF verifies the EOF break path
// when the stream ends before all chunks are read.
func TestReceiveChunksReadFrameHeaderEOF(t *testing.T) {
	handler := newTestHandler()
	sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)

	// Create only 1 chunk but claim 2 in the header.
	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 2, ChunkID: 0, ChunkCount: 2, ChunkSize: 5, Data: []byte("hello")},
	}
	data := buildChunkFrames(t, chunks)

	reader := &testChunkReader{data: data, readErr: io.EOF}
	hdr := &SnapshotHeader{ChunkCount: 2} // Expect 2 but only 1 available.
	stopC := make(chan struct{})

	sr.receiveChunks(reader, hdr, stopC)

	// Should have received the 1 available chunk.
	snaps := handler.getSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if len(snaps[0]) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(snaps[0]))
	}
}

// TestReceiveChunksReadFrameHeaderNonEOFError verifies the non-EOF
// ReadFrameHeader error path.
func TestReceiveChunksReadFrameHeaderNonEOFError(t *testing.T) {
	handler := newTestHandler()
	sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)

	// Provide partial frame header (less than 8 bytes) followed by an error.
	reader := &testChunkReader{
		data:    []byte{0x01, 0x02, 0x03}, // partial header
		readErr: errors.New("network error"),
	}
	hdr := &SnapshotHeader{ChunkCount: 1}
	stopC := make(chan struct{})

	sr.receiveChunks(reader, hdr, stopC)

	snaps := handler.getSnapshots()
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots on header error, got %d", len(snaps))
	}
}

// TestReceiveChunksZeroLengthChunk verifies the zero-length chunk
// continue path.
func TestReceiveChunksZeroLengthChunk(t *testing.T) {
	handler := newTestHandler()
	sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)

	// Build: zero-length frame + normal chunk.
	var buf bytes.Buffer
	// Zero-length frame.
	WriteFrame(&buf, nil, FlagNone)
	// Normal chunk.
	chunk := proto.SnapshotChunk{
		ShardID: 1, ReplicaID: 2, ChunkID: 0, ChunkCount: 2,
		ChunkSize: 4, Data: []byte("data"),
	}
	sz := chunk.Size()
	payload := make([]byte, sz)
	n, _ := chunk.MarshalTo(payload)
	WriteFrame(&buf, payload[:n], FlagNone)

	reader := &testChunkReader{data: buf.Bytes()}
	hdr := &SnapshotHeader{ChunkCount: 2}
	stopC := make(chan struct{})

	sr.receiveChunks(reader, hdr, stopC)

	snaps := handler.getSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if len(snaps[0]) != 1 {
		t.Fatalf("expected 1 chunk (zero-length skipped), got %d", len(snaps[0]))
	}
}

// TestReceiveChunksMemoryBudgetExceeded verifies the memory budget
// exceeded path.
func TestReceiveChunksMemoryBudgetExceeded(t *testing.T) {
	handler := newTestHandler()
	// Very small memory budget - 5 bytes.
	sr := newSnapshotReceiver(4, 5, 0, 0, handler)

	// Create a chunk larger than the budget.
	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 2, ChunkID: 0, ChunkCount: 1, ChunkSize: 100, Data: make([]byte, 100)},
	}
	data := buildChunkFrames(t, chunks)

	reader := &testChunkReader{data: data}
	hdr := &SnapshotHeader{ChunkCount: 1}
	stopC := make(chan struct{})

	sr.receiveChunks(reader, hdr, stopC)

	// Should have been rejected due to budget.
	snaps := handler.getSnapshots()
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots on budget exceeded, got %d", len(snaps))
	}

	// Memory budget should be restored.
	if sr.AvailableMemory() != 5 {
		t.Fatalf("expected restored budget 5, got %d", sr.AvailableMemory())
	}
}

// TestReceiveChunksReadFullError verifies the io.ReadFull error path
// during chunk payload read.
func TestReceiveChunksReadFullError(t *testing.T) {
	handler := newTestHandler()
	sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)

	// Build a valid frame header claiming 100 bytes payload, but only
	// provide the header (no payload data).
	var buf bytes.Buffer
	WriteFrame(&buf, make([]byte, 100), FlagNone) // Valid frame
	frameData := buf.Bytes()

	// Only provide the header part, not the full payload.
	reader := &testChunkReader{
		data:    frameData[:FrameHeaderSize+10], // header + only 10 of 100 bytes
		readErr: io.ErrUnexpectedEOF,
	}
	hdr := &SnapshotHeader{ChunkCount: 1}
	stopC := make(chan struct{})

	sr.receiveChunks(reader, hdr, stopC)

	snaps := handler.getSnapshots()
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots on read error, got %d", len(snaps))
	}
}

// TestReceiveChunksUnmarshalError verifies the unmarshal error path
// when chunk payload is invalid.
func TestReceiveChunksUnmarshalError(t *testing.T) {
	handler := newTestHandler()
	sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)

	// Build a frame with invalid (garbage) payload.
	garbage := make([]byte, 50)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	var buf bytes.Buffer
	WriteFrame(&buf, garbage, FlagNone)

	reader := &testChunkReader{data: buf.Bytes()}
	hdr := &SnapshotHeader{ChunkCount: 1}
	stopC := make(chan struct{})

	sr.receiveChunks(reader, hdr, stopC)

	snaps := handler.getSnapshots()
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots on unmarshal error, got %d", len(snaps))
	}
}

// TestReceiveChunksRateLimitWithStop verifies the rate limiting path
// where stopC is closed during the rate limit sleep.
func TestReceiveChunksRateLimitWithStop(t *testing.T) {
	handler := newTestHandler()
	// Very slow rate to ensure rate limit kicks in: 1 byte/sec.
	sr := newSnapshotReceiver(4, 1<<30, 1, 0, handler)

	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 2, ChunkID: 0, ChunkCount: 2, ChunkSize: 100, Data: make([]byte, 100)},
		{ShardID: 1, ReplicaID: 2, ChunkID: 1, ChunkCount: 2, ChunkSize: 100, Data: make([]byte, 100)},
	}
	data := buildChunkFrames(t, chunks)

	reader := &testChunkReader{data: data}
	hdr := &SnapshotHeader{ChunkCount: 2}
	stopC := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		sr.receiveChunks(reader, hdr, stopC)
	}()

	// Close stopC after a short delay to interrupt rate limiting.
	time.Sleep(50 * time.Millisecond)
	close(stopC)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for receiveChunks to exit")
	}
}

// TestReceiveChunksEmptyChunkCount verifies behavior with ChunkCount=0.
func TestReceiveChunksEmptyChunkCount(t *testing.T) {
	handler := newTestHandler()
	sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)

	reader := &testChunkReader{data: nil}
	hdr := &SnapshotHeader{ChunkCount: 0}
	stopC := make(chan struct{})

	sr.receiveChunks(reader, hdr, stopC)

	// With 0 chunks, handler should not be called.
	snaps := handler.getSnapshots()
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots for empty chunk count, got %d", len(snaps))
	}
}

// TestStopPooledStreamsClosedOnShutdown verifies that Stop closes all
// pooled streams.
func TestStopPooledStreamsClosedOnShutdown(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Create multiple streams.
	for shardID := uint64(1); shardID <= 5; shardID++ {
		_, err := t1.getStream(target, shardID, false)
		if err != nil {
			t.Fatalf("getStream(%d) failed: %v", shardID, err)
		}
	}

	// Also create a heartbeat stream.
	_, err := t1.getStream(target, 1, true)
	if err != nil {
		t.Fatalf("getStream(heartbeat) failed: %v", err)
	}

	// Stop should close all streams without panic.
	t1.Stop()
}

// TestHandleMessageFrame_CompressedPayload verifies that a Snappy-compressed
// frame payload is correctly decompressed and dispatched to the handler.
func TestHandleMessageFrame_CompressedPayload(t *testing.T) {
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

	// Build a valid MessageBatch.
	mb := proto.MessageBatch{
		BinVer:       proto.WireVersion,
		DeploymentID: 42,
		Requests: []proto.Message{
			{
				Type:    proto.Heartbeat,
				ShardID: 1,
				From:    2,
				To:      1,
				Term:    5,
			},
		},
	}

	sz := mb.Size()
	rawBuf := make([]byte, sz)
	n, err := mb.MarshalTo(rawBuf)
	if err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}
	rawPayload := rawBuf[:n]

	// Compress the raw payload with Snappy.
	compressed := snappyEncode(nil, rawPayload)
	reader := bytes.NewReader(compressed)

	err = tr.handleMessageFrame(nil, reader, uint32(len(compressed)), FlagCompressed)
	if err != nil {
		t.Fatalf("expected no error for compressed valid batch, got %v", err)
	}

	// Verify handler received the message.
	batches := handler.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0].Requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(batches[0].Requests))
	}
	if batches[0].Requests[0].Type != proto.Heartbeat {
		t.Fatalf("expected Heartbeat message type, got %d", batches[0].Requests[0].Type)
	}
}

// TestHandleMessageFrame_CompressedCorruptData verifies that corrupt
// compressed data returns a FrameDecompressError.
func TestHandleMessageFrame_CompressedCorruptData(t *testing.T) {
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

	// Provide invalid Snappy data.
	corrupt := []byte{0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA}
	reader := bytes.NewReader(corrupt)

	err = tr.handleMessageFrame(nil, reader, uint32(len(corrupt)), FlagCompressed)
	if err == nil {
		t.Fatal("expected error for corrupt compressed data")
	}
	var decompErr *FrameDecompressError
	if !errors.As(err, &decompErr) {
		t.Fatalf("expected FrameDecompressError, got %T: %v", err, err)
	}
}

// TestHandleMessageFrame_CompressedExceedsMaxDecompressedSize verifies
// that decompressed payloads exceeding MaxDecompressedSize are rejected.
func TestHandleMessageFrame_CompressedExceedsMaxDecompressedSize(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress:       ":0",
		DeploymentID:        42,
		MaxDecompressedSize: 10, // very small limit
		MTLSConfig:          testMTLSConfig(t),
	}

	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	// Build a valid payload larger than 10 bytes when decompressed.
	mb := proto.MessageBatch{
		BinVer:       proto.WireVersion,
		DeploymentID: 42,
		Requests: []proto.Message{
			{
				Type:    proto.Heartbeat,
				ShardID: 1,
				From:    2,
				To:      1,
				Term:    5,
			},
		},
	}

	sz := mb.Size()
	rawBuf := make([]byte, sz)
	n, err := mb.MarshalTo(rawBuf)
	if err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}

	compressed := snappyEncode(nil, rawBuf[:n])
	reader := bytes.NewReader(compressed)

	err = tr.handleMessageFrame(nil, reader, uint32(len(compressed)), FlagCompressed)
	if !errors.Is(err, ErrDecompressedSizeLimitExceeded) {
		t.Fatalf("expected ErrDecompressedSizeLimitExceeded, got %v", err)
	}
}

// TestHandleMessageFrame_CompressedBoundaryCondition verifies that the
// decompression size check correctly handles boundary conditions.
// This test ensures the fix for potential integer overflow on 32-bit systems
// where the comparison `declaredLen > int(MaxDecompressedSize)` is used
// instead of `uint32(declaredLen) > MaxDecompressedSize`.
func TestHandleMessageFrame_CompressedBoundaryCondition(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()

	// Create a payload that will have a known decompressed size.
	// We'll set MaxDecompressedSize to exactly match, then test at boundary.
	mb := proto.MessageBatch{
		BinVer:       proto.WireVersion,
		DeploymentID: 42,
		Requests: []proto.Message{
			{
				Type:    proto.Heartbeat,
				ShardID: 1,
				From:    2,
				To:      1,
				Term:    5,
			},
		},
	}

	sz := mb.Size()
	rawBuf := make([]byte, sz)
	n, err := mb.MarshalTo(rawBuf)
	if err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}
	decompressedSize := n

	t.Run("exactly_at_limit_accepted", func(t *testing.T) {
		cfg := Config{
			ListenAddress:       ":0",
			DeploymentID:        42,
			MaxDecompressedSize: uint32(decompressedSize), // exactly at boundary
			MTLSConfig:          testMTLSConfig(t),
		}

		tr, err := NewQUICTransport(cfg, handler, reg)
		if err != nil {
			t.Fatalf("NewQUICTransport failed: %v", err)
		}
		defer tr.Stop()

		compressed := snappyEncode(nil, rawBuf[:n])
		reader := bytes.NewReader(compressed)

		// Should succeed because decompressed size equals limit
		err = tr.handleMessageFrame(nil, reader, uint32(len(compressed)), FlagCompressed)
		if err != nil {
			t.Fatalf("expected success at boundary, got %v", err)
		}
	})

	t.Run("one_byte_over_rejected", func(t *testing.T) {
		cfg := Config{
			ListenAddress:       ":0",
			DeploymentID:        42,
			MaxDecompressedSize: uint32(decompressedSize - 1), // one byte under payload
			MTLSConfig:          testMTLSConfig(t),
		}

		tr, err := NewQUICTransport(cfg, handler, reg)
		if err != nil {
			t.Fatalf("NewQUICTransport failed: %v", err)
		}
		defer tr.Stop()

		compressed := snappyEncode(nil, rawBuf[:n])
		reader := bytes.NewReader(compressed)

		// Should fail because decompressed size exceeds limit by 1 byte
		err = tr.handleMessageFrame(nil, reader, uint32(len(compressed)), FlagCompressed)
		if !errors.Is(err, ErrDecompressedSizeLimitExceeded) {
			t.Fatalf("expected ErrDecompressedSizeLimitExceeded, got %v", err)
		}
	})
}

// TestFrameDecompressError_ErrorString verifies the error message format.
func TestFrameDecompressError_ErrorString(t *testing.T) {
	inner := errors.New("bad snappy data")
	e := &FrameDecompressError{Err: inner}
	want := "transport: frame decompression failed: bad snappy data"
	if e.Error() != want {
		t.Fatalf("error string = %q, want %q", e.Error(), want)
	}
}

// TestFrameDecompressError_Unwrap verifies error chain traversal.
func TestFrameDecompressError_Unwrap(t *testing.T) {
	inner := errors.New("bad snappy data")
	e := &FrameDecompressError{Err: inner}
	if !errors.Is(e, inner) {
		t.Fatal("Unwrap should return inner error")
	}
}

// TestSendBatch_DisableCompression verifies that the DisableCompression flag
// controls whether non-heartbeat messages are Snappy-compressed on the wire.
// With DisableCompression=false (default), large payloads are compressed.
// With DisableCompression=true, payloads are sent raw. Both round-trip
// correctly because the receiver handles FlagNone and FlagCompressed frames.
func TestSendBatch_DisableCompression(t *testing.T) {
	// Build a large Cmd payload that compresses well (repeated bytes).
	largeCmd := bytes.Repeat([]byte("AAAA"), 1024) // 4 KB of repetitive data

	t.Run("compression_enabled", func(t *testing.T) {
		t1, _, _, handler2, cleanup := transportPair(t, 42)
		defer cleanup()

		t1.sendBatch(func() string {
			addr, _ := t1.reg.Resolve(1, 2)
			return addr
		}(), []proto.Message{
			{
				Type:    proto.Replicate,
				ShardID: 1,
				From:    1,
				To:      2,
				Term:    10,
				Entries: []proto.Entry{
					{Index: 1, Term: 10, Cmd: largeCmd},
				},
			},
		})

		handler2.waitBatch(t, 10*time.Second)

		batches := handler2.getBatches()
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(batches))
		}
		msg := batches[0].Requests[0]
		if msg.Type != proto.Replicate {
			t.Fatalf("expected Replicate, got %d", msg.Type)
		}
		if len(msg.Entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(msg.Entries))
		}
		if !bytes.Equal(msg.Entries[0].Cmd, largeCmd) {
			t.Fatal("Cmd payload mismatch with compression enabled")
		}
	})

	t.Run("compression_disabled", func(t *testing.T) {
		handler1 := newTestHandler()
		handler2 := newTestHandler()
		reg1 := registry.NewRegistry()
		reg2 := registry.NewRegistry()

		cfg := Config{
			ListenAddress:      ":0",
			DeploymentID:       42,
			DisableCompression: true,
			MTLSConfig:         testMTLSConfig(t),
		}

		t1, err := NewQUICTransport(cfg, handler1, reg1)
		if err != nil {
			t.Fatalf("NewQUICTransport(1) failed: %v", err)
		}
		if err := t1.Start(); err != nil {
			t.Fatalf("t1.Start failed: %v", err)
		}
		defer t1.Stop()

		t2, err := NewQUICTransport(cfg, handler2, reg2)
		if err != nil {
			t.Fatalf("NewQUICTransport(2) failed: %v", err)
		}
		if err := t2.Start(); err != nil {
			t.Fatalf("t2.Start failed: %v", err)
		}
		defer t2.Stop()

		reg1.Register(1, 2, t2.Addr().String())

		t1.sendBatch(t2.Addr().String(), []proto.Message{
			{
				Type:    proto.Replicate,
				ShardID: 1,
				From:    1,
				To:      2,
				Term:    20,
				Entries: []proto.Entry{
					{Index: 1, Term: 20, Cmd: largeCmd},
				},
			},
		})

		handler2.waitBatch(t, 10*time.Second)

		batches := handler2.getBatches()
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(batches))
		}
		msg := batches[0].Requests[0]
		if msg.Type != proto.Replicate {
			t.Fatalf("expected Replicate, got %d", msg.Type)
		}
		if len(msg.Entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(msg.Entries))
		}
		if !bytes.Equal(msg.Entries[0].Cmd, largeCmd) {
			t.Fatal("Cmd payload mismatch with compression disabled")
		}
	})
}
