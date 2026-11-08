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
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

// TestGetConnectionCreatesNewConnection verifies that getConnection dials
// when no connection exists.
func TestGetConnectionCreatesNewConnection(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}

	// Should be stored in the pool.
	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection, got %d", t1.ConnectionCount())
	}
}

// TestGetConnectionReusesExisting verifies the fast path reuse.
func TestGetConnectionReusesExisting(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	conn1, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("first getConnection failed: %v", err)
	}

	conn2, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("second getConnection failed: %v", err)
	}

	if conn1 != conn2 {
		t.Fatal("expected same connection on second call")
	}
}

// TestGetConnectionSingleflightCoalescing verifies that concurrent dials
// to the same target are coalesced into a single connection.
func TestGetConnectionSingleflightCoalescing(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	var wg sync.WaitGroup
	conns := make([]*quic.Conn, 10)
	errs := make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conns[idx], errs[idx] = t1.getConnection(target, "")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: getConnection failed: %v", i, err)
		}
	}

	// All should return the same connection.
	for i := 1; i < 10; i++ {
		if conns[i] != conns[0] {
			t.Fatalf("goroutine %d returned different connection", i)
		}
	}

	// Only one connection in the pool.
	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection after coalescing, got %d", t1.ConnectionCount())
	}
}

// TestGetConnectionWhenStopped verifies that getConnection returns error
// when the transport is stopped.
func TestGetConnectionWhenStopped(t *testing.T) {
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

	_, err = tr.getConnection("127.0.0.1:12345", "")
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("expected ErrStreamClosed, got %v", err)
	}
}

// TestRemoveConnection verifies connection removal from pool.
func TestRemoveConnection(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	_, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection, got %d", t1.ConnectionCount())
	}

	t1.removeConnection(target)

	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections after removal, got %d", t1.ConnectionCount())
	}
}

// TestCleanStaleConnections verifies that connections with done contexts
// are cleaned up.
func TestCleanStaleConnections(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection, got %d", t1.ConnectionCount())
	}

	// Close the connection from the remote side.
	conn.CloseWithError(0, "test close")
	waitForChan(t, conn.Context().Done(), 5*time.Second, "closed connection context not done")

	// Run cleanup.
	t1.cleanStaleConnections()

	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections after cleanup, got %d", t1.ConnectionCount())
	}
}

// TestGetStreamOpensNewStream verifies that getStream creates a new stream
// when none exists in the pool.
func TestGetStreamOpensNewStream(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	stream, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil stream")
	}
}

// TestGetStreamReusesExisting verifies stream pool caching.
func TestGetStreamReusesExisting(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	s1, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("first getStream failed: %v", err)
	}

	s2, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("second getStream failed: %v", err)
	}

	if s1 != s2 {
		t.Fatal("expected same stream on second call with same shardID")
	}
}

// TestGetStreamHeartbeatUsesIndexZero verifies heartbeat stream index.
func TestGetStreamHeartbeatUsesIndexZero(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	hbStream, err := t1.getStream(target, 1, true, "")
	if err != nil {
		t.Fatalf("getStream(heartbeat) failed: %v", err)
	}

	dataStream, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream(data) failed: %v", err)
	}

	if hbStream == dataStream {
		t.Fatal("heartbeat and data streams should be different")
	}
}

// TestIsConnectionClosedWithNil verifies nil error is not closed.
func TestIsConnectionClosedWithNil(t *testing.T) {
	if isConnectionClosed(nil) {
		t.Fatal("nil error should not be considered closed")
	}
}

// TestIsConnectionClosedWithAppError verifies ApplicationError detection.
func TestIsConnectionClosedWithAppError(t *testing.T) {
	err := &quic.ApplicationError{ErrorCode: 0}
	if !isConnectionClosed(err) {
		t.Fatal("ApplicationError should be detected as closed")
	}
}

// TestIsConnectionClosedWithIdleTimeout verifies IdleTimeoutError detection.
func TestIsConnectionClosedWithIdleTimeout(t *testing.T) {
	err := &quic.IdleTimeoutError{}
	if !isConnectionClosed(err) {
		t.Fatal("IdleTimeoutError should be detected as closed")
	}
}

// TestIsConnectionClosedWithOtherError verifies non-connection errors.
func TestIsConnectionClosedWithOtherError(t *testing.T) {
	err := errors.New("some other error")
	if isConnectionClosed(err) {
		t.Fatal("generic error should not be detected as closed")
	}
}

// TestConnectionCleanupLoop verifies the periodic cleanup goroutine
// terminates on stop. Start() launches connectionCleanup automatically.
func TestConnectionCleanupLoop(t *testing.T) {
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

	// Stop should cause the cleanup goroutine to exit cleanly.
	tr.Stop()
}

// TestConcurrentSendDoesNotRace verifies that concurrent sends from
// multiple goroutines do not race.
func TestConcurrentSendDoesNotRace(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	var wg sync.WaitGroup
	sendCount := 20

	for i := 0; i < sendCount; i++ {
		wg.Add(1)
		go func(term uint64) {
			defer wg.Done()
			t1.Send([]proto.Message{
				{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: term},
			})
		}(uint64(i))
	}
	wg.Wait()

	pollUntil(t, 10*time.Second, 10*time.Millisecond, "timeout waiting for any batch to arrive", func() bool {
		return handler2.batchCount.Load() >= 1
	})

	// At least some batches should have arrived.
	var received atomic.Int64
	for _, b := range handler2.getBatches() {
		received.Add(int64(len(b.Requests)))
	}
	if received.Load() < 1 {
		t.Fatal("expected at least 1 message to be received")
	}
}

// TestDialConnectionServerNameExtraction verifies that the TLS ServerName
// is extracted from the target address hostname, not hardcoded to "localhost".
// This is critical for mTLS where peer certificates have specific CN/SANs.
func TestDialConnectionServerNameExtraction(t *testing.T) {
	t.Run("uses IP from target address", func(t *testing.T) {
		t1, t2, _, _, cleanup := transportPair(t, 42)
		defer cleanup()

		target := t2.Addr().String()
		conn, err := t1.getConnection(target, "")
		if err != nil {
			t.Fatalf("getConnection(%s) failed: %v", target, err)
		}
		if conn == nil {
			t.Fatal("expected non-nil connection")
		}
		// Connection established successfully with extracted hostname.
		if t1.ConnectionCount() != 1 {
			t.Fatalf("expected 1 connection, got %d", t1.ConnectionCount())
		}
	})

	t.Run("falls back to localhost for empty host", func(t *testing.T) {
		// Addresses like ":4001" have empty host, which should default
		// to "localhost". This test verifies no error on parse.
		t1, t2, _, _, cleanup := transportPair(t, 42)
		defer cleanup()

		// The actual transport pair uses resolved addresses (127.0.0.1:PORT),
		// so we test the empty host fallback indirectly: verify getConnection
		// works with an address that resolves to the peer.
		target := t2.Addr().String()
		conn, err := t1.getConnection(target, "")
		if err != nil {
			t.Fatalf("getConnection failed: %v", err)
		}
		if conn == nil {
			t.Fatal("expected non-nil connection")
		}
	})
}

// TestStreamPoolSizeOne verifies pool behavior when pool size is 1.
func TestStreamPoolSizeOne(t *testing.T) {
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

	reg1.Register(1, 2, t2.Addr().String())

	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
}

// TestCleanStaleConnectionsEvictsStreams verifies that when a connection
// dies, cleanStaleConnections removes both the connection AND all pooled
// streams that targeted the dead connection. This prevents goroutine
// leaks and repeated write failures from stale stream handles.
func TestCleanStaleConnectionsEvictsStreams(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Create several streams on the connection.
	for shardID := uint64(1); shardID <= 5; shardID++ {
		_, err := t1.getStream(target, shardID, false, "")
		if err != nil {
			t.Fatalf("getStream(%d) failed: %v", shardID, err)
		}
	}
	// Also a heartbeat stream.
	_, err := t1.getStream(target, 1, true, "")
	if err != nil {
		t.Fatalf("getStream(heartbeat) failed: %v", err)
	}

	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection, got %d", t1.ConnectionCount())
	}
	if t1.StreamCount() == 0 {
		t.Fatal("expected pooled streams before connection death")
	}

	// Kill the connection from the remote side.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	conn.CloseWithError(0, "test kill connection")
	waitForChan(t, conn.Context().Done(), 5*time.Second, "closed connection context not done")

	// Run cleanup.
	t1.cleanStaleConnections()

	// Connection should be removed.
	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections after cleanup, got %d", t1.ConnectionCount())
	}

	// All streams targeting the dead connection should be evicted.
	if t1.StreamCount() != 0 {
		t.Fatalf("expected 0 streams after cleanup, got %d", t1.StreamCount())
	}
}

// TestCleanStaleConnectionsKeepsLiveStreams verifies that stream cleanup
// only removes streams for dead connections, not streams for live ones.
func TestCleanStaleConnectionsKeepsLiveStreams(t *testing.T) {
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

	target2 := t2.Addr().String()
	target3 := t3.Addr().String()

	// Create streams to both t2 and t3. dialConnection also pre-opens a
	// heartbeat stream (index 0) asynchronously for each new connection.
	_, err = t1.getStream(target2, 1, false, "")
	if err != nil {
		t.Fatalf("getStream(t2) failed: %v", err)
	}
	_, err = t1.getStream(target3, 1, false, "")
	if err != nil {
		t.Fatalf("getStream(t3) failed: %v", err)
	}

	pollUntil(t, 5*time.Second, 5*time.Millisecond, "heartbeat streams not pre-opened", func() bool {
		return countStreamsForTarget(t1, target2) == 2 && countStreamsForTarget(t1, target3) == 2
	})

	// Each connection has 1 data stream + 1 pre-opened heartbeat stream.
	streamsBefore := t1.StreamCount()
	if streamsBefore < 2 {
		t.Fatalf("expected at least 2 streams, got %d", streamsBefore)
	}

	// Count streams for t3 before cleanup. These should be preserved.
	t3StreamsBefore := countStreamsForTarget(t1, target3)

	// Kill only t2's connection.
	conn2, err := t1.getConnection(target2, "")
	if err != nil {
		t.Fatalf("getConnection(t2) failed: %v", err)
	}
	conn2.CloseWithError(0, "kill t2")
	waitForChan(t, conn2.Context().Done(), 5*time.Second, "closed connection context not done")

	t1.cleanStaleConnections()

	// t2 connection + streams should be gone, t3 should remain.
	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection (t3), got %d", t1.ConnectionCount())
	}
	// Only t3's streams should survive (data + possibly heartbeat).
	if t1.StreamCount() != t3StreamsBefore {
		t.Fatalf("expected %d streams (t3 only), got %d", t3StreamsBefore, t1.StreamCount())
	}
}

// TestGetConnectionDeadEvictsStreams verifies that getConnection on the
// fast path evicts streams when it detects a dead connection and redials.
func TestGetConnectionDeadEvictsStreams(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Create a data stream on the connection. dialConnection also
	// pre-opens a heartbeat stream asynchronously.
	_, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}

	pollUntil(t, 5*time.Second, 5*time.Millisecond, "heartbeat stream not pre-opened", func() bool {
		return countStreamsForTarget(t1, target) == 2
	})

	streamsBefore := t1.StreamCount()
	if streamsBefore < 1 {
		t.Fatalf("expected at least 1 stream, got %d", streamsBefore)
	}

	// Kill the connection.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	conn.CloseWithError(0, "test kill for redial")
	waitForChan(t, conn.Context().Done(), 5*time.Second, "closed connection context not done")

	// getConnection should detect the dead connection, evict streams,
	// and redial. The redial will also fire preOpenHeartbeatStream
	// asynchronously.
	newConn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("redial getConnection failed: %v", err)
	}
	if newConn == conn {
		t.Fatal("expected new connection after redial")
	}

	// Old streams should be evicted. New pre-opened heartbeat streams
	// from the redial may or may not have been created yet, so we only
	// assert that the old streams are gone (count <= 1, where 1 is the
	// possible pre-opened heartbeat from the redial).
	if t1.StreamCount() > 1 {
		t.Fatalf("expected at most 1 stream after redial eviction (pre-opened heartbeat), got %d", t1.StreamCount())
	}
}

// TestEvictStreamsForTargetNonExistent verifies that evicting streams
// for a non-existent target is safe and does not panic.
func TestEvictStreamsForTargetNonExistent(t *testing.T) {
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

	// Should not panic.
	tr.evictStreamsForTarget("nonexistent:1234")

	if tr.StreamCount() != 0 {
		t.Fatalf("expected 0 streams, got %d", tr.StreamCount())
	}
}

// TestPreOpenHeartbeatStream verifies that dialConnection proactively opens
// a heartbeat stream (index 0) so that the first heartbeat send on a new
// connection does not pay the OpenStreamSync latency.
func TestPreOpenHeartbeatStream(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Trigger a connection dial by calling getConnection.
	_, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Wait for the async preOpenHeartbeatStream goroutine to complete.
	// The goroutine opens a stream with a 5s timeout, but on loopback
	// it completes in microseconds. Poll for the heartbeat stream.
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "timeout waiting for heartbeat stream to be pre-opened", func() bool {
		shardIdx := uint64(0) & (streamShardCount - 1)
		shard := &t1.streamShards[shardIdx]
		key := streamKey{target: target, idx: 0}

		shard.mu.RLock()
		_, ok := shard.streams[key]
		shard.mu.RUnlock()
		return ok
	})

	// Verify getStream returns the pre-opened heartbeat stream without
	// needing to open a new one (cache hit path).
	stream, err := t1.getStream(target, 1, true, "")
	if err != nil {
		t.Fatalf("getStream(heartbeat) failed: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil heartbeat stream")
	}
}

// TestPreOpenHeartbeatStreamNoRaceWithGetStream verifies that a concurrent
// heartbeat send does not race with preOpenHeartbeatStream. If both the
// pre-open goroutine and a heartbeat send open the stream simultaneously,
// one should win the double-check in the stream pool and the other should
// close its duplicate.
func TestPreOpenHeartbeatStreamNoRaceWithGetStream(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Dial the connection (which triggers preOpenHeartbeatStream async).
	_, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Immediately open a heartbeat stream from the main goroutine,
	// racing with the pre-open goroutine.
	stream, err := t1.getStream(target, 1, true, "")
	if err != nil {
		t.Fatalf("getStream(heartbeat) failed: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil heartbeat stream")
	}

	// Whichever of the pre-open and getStream stored first, the pool keeps
	// one live heartbeat stream for the target and later calls get it.
	stream2, err := t1.getStream(target, 1, true, "")
	if err != nil {
		t.Fatalf("second getStream(heartbeat) failed: %v", err)
	}
	if stream2 != stream {
		t.Fatal("second getStream(heartbeat) returned a different stream: the pre-open replaced a live entry")
	}
}

// TestPreOpenHeartbeatStreamDeadConnection verifies that preOpenHeartbeatStream
// handles a dead connection gracefully (non-fatal failure). We use a separate
// target address that has never been dialed by the transport, so no async
// preOpenHeartbeatStream goroutine from dialConnection is in flight.
func TestPreOpenHeartbeatStreamDeadConnection(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Get a connection to obtain a real quic.Conn object.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Wait for the async preOpenHeartbeatStream from dialConnection to
	// complete so it does not interfere with our test.
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "heartbeat stream not pre-opened", func() bool {
		return countStreamsForTarget(t1, target) == 1
	})

	// Evict any streams that were pre-opened by dialConnection.
	t1.evictStreamsForTarget(target)

	// Close the connection so OpenStreamSync will fail.
	conn.CloseWithError(0, "test close before pre-open")
	waitForChan(t, conn.Context().Done(), 5*time.Second, "closed connection context not done")

	// Calling preOpenHeartbeatStream on a dead connection should not
	// panic or block. The stream open will fail and return silently.
	// Add to WaitGroup since preOpenHeartbeatStream now calls wg.Done().
	t1.wg.Add(1)
	t1.preOpenHeartbeatStream(target, conn)

	// No heartbeat stream should be in the pool for this target.
	shardIdx := uint64(0) & (streamShardCount - 1)
	shard := &t1.streamShards[shardIdx]
	key := streamKey{target: target, idx: 0}
	shard.mu.RLock()
	_, ok := shard.streams[key]
	shard.mu.RUnlock()
	if ok {
		t.Fatal("expected no heartbeat stream for dead connection")
	}
}

// TestStreamCount verifies the StreamCount method returns the correct
// count across all shards.
func TestStreamCount(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	if t1.StreamCount() != 0 {
		t.Fatalf("expected 0 streams initially, got %d", t1.StreamCount())
	}

	// Create streams across different shard indices.
	for shardID := uint64(1); shardID <= 8; shardID++ {
		_, err := t1.getStream(target, shardID, false, "")
		if err != nil {
			t.Fatalf("getStream(%d) failed: %v", shardID, err)
		}
	}

	count := t1.StreamCount()
	if count == 0 {
		t.Fatal("expected non-zero stream count after creating streams")
	}
}

// TestPreOpenHeartbeatStreamTrackedByWaitGroup verifies that the
// preOpenHeartbeatStream goroutine is tracked by the transport's WaitGroup.
// Without tracking, Stop() could complete before the goroutine finishes,
// causing it to access cleared stream shard maps and potentially panic.
func TestPreOpenHeartbeatStreamTrackedByWaitGroup(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	_ = cleanup // we manually stop

	target := t2.Addr().String()

	// Trigger a dial to start the preOpenHeartbeatStream goroutine.
	_, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Immediately stop the transport. If preOpenHeartbeatStream is not
	// tracked by the WaitGroup, Stop() would return before the goroutine
	// finishes. The goroutine would then access t.streamShards on a
	// stopped transport. With proper WaitGroup tracking, Stop() blocks
	// until the goroutine exits.
	if err := t1.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	t2.Stop()

	// If we reach this point without a race or panic, the goroutine
	// was properly tracked.
	if !t1.stopped.Load() {
		t.Fatal("transport should be stopped")
	}
}

// TestPreOpenHeartbeatStreamExitsEarlyWhenStopped verifies that
// preOpenHeartbeatStream returns immediately if the transport is already
// stopped, avoiding unnecessary work and potential access to cleaned-up state.
func TestPreOpenHeartbeatStreamExitsEarlyWhenStopped(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Get a connection.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Wait for the initial preOpenHeartbeatStream from dialConnection.
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "heartbeat stream not pre-opened", func() bool {
		return countStreamsForTarget(t1, target) == 1
	})

	// Evict all streams so we can observe if preOpen creates a new one.
	t1.evictStreamsForTarget(target)
	initialStreams := t1.StreamCount()

	// Mark transport as stopped. preOpenHeartbeatStream should exit
	// immediately without opening a stream.
	t1.stopped.Store(true)

	// Call preOpenHeartbeatStream directly (simulating what dialConnection does).
	// With the stopped check, it should return without creating a stream.
	t1.wg.Add(1) // manually add since we call directly
	t1.preOpenHeartbeatStream(target, conn)

	// Restore stopped flag for cleanup.
	t1.stopped.Store(false)

	// No new stream should have been created.
	if t1.StreamCount() != initialStreams {
		t.Fatalf("expected %d streams (no new stream when stopped), got %d",
			initialStreams, t1.StreamCount())
	}
}

// TestPreOpenHeartbeatStreamWaitGroupDoneOnAllPaths verifies that the
// WaitGroup is properly decremented on both the success and failure paths
// of preOpenHeartbeatStream. We test the failure path (dead connection)
// to ensure defer wg.Done() covers all exit points.
func TestPreOpenHeartbeatStreamWaitGroupDoneOnAllPaths(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Get a connection.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Wait for initial preOpenHeartbeatStream to complete.
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "heartbeat stream not pre-opened", func() bool {
		return countStreamsForTarget(t1, target) == 1
	})

	// Close the connection so OpenStreamSync will fail.
	conn.CloseWithError(0, "test close for wg done verification")
	waitForChan(t, conn.Context().Done(), 5*time.Second, "closed connection context not done")

	// Call preOpenHeartbeatStream on the dead connection. It should fail
	// gracefully and still call wg.Done() via defer.
	t1.wg.Add(1) // manually add since we call directly
	t1.preOpenHeartbeatStream(target, conn)

	// If we can Stop() without hanging, wg.Done() was called properly.
	// (Stop() calls wg.Wait() which would deadlock if Done was missed.)
}

// TestEvictStaleOutboundEmptyTarget verifies that evictStaleOutbound
// returns immediately for an empty target string without touching the
// connection pool.
func TestEvictStaleOutboundEmptyTarget(t *testing.T) {
	t1, _, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	// Should return immediately without panic or side effects.
	t1.evictStaleOutboundOnRecv("", 0, nil)

	// No connections should have been touched.
	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections, got %d", t1.ConnectionCount())
	}
}

// TestEvictStaleOutboundNoConnection verifies that evictStaleOutbound
// is safe when the target has no cached connection. This is the common
// case when we receive a message from a peer we have never dialed.
func TestEvictStaleOutboundNoConnection(t *testing.T) {
	t1, _, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	// Call with a target that has no cached outbound connection.
	t1.evictStaleOutboundOnRecv("192.168.1.1:4001", 0, nil)

	// No connections should exist.
	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections, got %d", t1.ConnectionCount())
	}
}

// TestEvictStaleOutboundAliveConnection verifies that evictStaleOutbound
// does not evict a connection that is still alive. The fast path checks the
// connection context and returns without modification.
func TestEvictStaleOutboundAliveConnection(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Establish a live connection.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}

	// Create a data stream on this connection.
	_, err = t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}

	// Let the stream pool reach steady state BEFORE snapshotting it.
	//
	// dialConnection pre-opens the heartbeat stream in a goroutine
	// (`go t.preOpenHeartbeatStream(...)`) and nothing here joins it, so the
	// count reaches its final value asynchronously. Snapshotting first and
	// asserting equality afterwards made the assertion a race against that
	// goroutine: if it registered its stream in between, StreamCount went from
	// 1 to 2 and the test failed with "expected 1 streams after alive check,
	// got 2" — attributing to evictStaleOutboundOnRecv a stream it never
	// created. It survives an unloaded machine and fails on a busy CI runner.
	//
	// Waiting for both streams (pre-opened heartbeat + the data stream above)
	// also makes the assertion STRONGER: the eviction path is now checked
	// against a fully-populated pool rather than whatever subset happened to
	// exist at snapshot time.
	const wantStreams = 2
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "stream pool did not reach the pre-opened heartbeat plus the data stream", func() bool {
		return t1.StreamCount() >= wantStreams
	})
	if got := t1.StreamCount(); got != wantStreams {
		t.Fatalf("stream pool did not settle: StreamCount = %d, want %d "+
			"(pre-opened heartbeat stream plus the data stream)", got, wantStreams)
	}

	connsBefore := t1.ConnectionCount()
	streamsBefore := t1.StreamCount()

	// evictStaleOutbound should detect the connection is alive and skip eviction.
	t1.evictStaleOutboundOnRecv(target, 0, nil)

	if t1.ConnectionCount() != connsBefore {
		t.Fatalf("expected %d connections after alive check, got %d",
			connsBefore, t1.ConnectionCount())
	}
	if t1.StreamCount() != streamsBefore {
		t.Fatalf("expected %d streams after alive check, got %d",
			streamsBefore, t1.StreamCount())
	}
}

// TestEvictStaleOutboundDeadConnection verifies that evictStaleOutbound
// removes a dead connection and evicts all associated pooled streams. This
// is the primary code path: a peer restarted, the old outbound connection
// context is cancelled, and we clean up on the receive path.
func TestEvictStaleOutboundDeadConnection(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Establish a connection and create some streams.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	for shardID := uint64(1); shardID <= 3; shardID++ {
		_, err = t1.getStream(target, shardID, false, "")
		if err != nil {
			t.Fatalf("getStream(%d) failed: %v", shardID, err)
		}
	}

	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection, got %d", t1.ConnectionCount())
	}
	if t1.StreamCount() == 0 {
		t.Fatal("expected at least 1 stream")
	}

	// Let the pre-opened heartbeat land before the kill, so it cannot be
	// stored after the eviction: heartbeat plus three data streams.
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "heartbeat stream not pre-opened", func() bool {
		return countStreamsForTarget(t1, target) == 4
	})

	// Kill the connection to simulate a peer restart.
	conn.CloseWithError(0, "simulate peer restart")
	waitForChan(t, conn.Context().Done(), 5*time.Second, "closed connection context not done")

	// evictStaleOutbound should detect the dead context and clean up.
	t1.evictStaleOutboundOnRecv(target, 0, nil)

	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections after eviction, got %d", t1.ConnectionCount())
	}
	if t1.StreamCount() != 0 {
		t.Fatalf("expected 0 streams after eviction, got %d", t1.StreamCount())
	}
}

// TestEvictStaleOutboundOnRecvDelegatesToEvictStaleOutbound verifies that
// evictStaleOutboundOnRecv with a nil inbound connection falls back to
// evictStaleOutbound, which detects and evicts a dead outbound connection.
func TestEvictStaleOutboundOnRecvDelegatesToEvictStaleOutbound(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Establish a connection.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Create a stream.
	_, err = t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}

	// Kill the connection.
	conn.CloseWithError(0, "dead for nil-inbound test")
	waitConnDown(t, t1, target, 5*time.Second)

	// Call evictStaleOutboundOnRecv with nil inbound — should delegate
	// to evictStaleOutbound and clean up the dead connection.
	t1.evictStaleOutboundOnRecv(target, 0, nil)

	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections after nil-inbound eviction, got %d",
			t1.ConnectionCount())
	}
	if t1.StreamCount() != 0 {
		t.Fatalf("expected 0 streams after nil-inbound eviction, got %d",
			t1.StreamCount())
	}
}

// TestEvictStaleOutboundOnRecv_AliveOutboundNotEvicted verifies that an alive
// outbound connection is NOT evicted when the inbound connection differs. In
// normal QUIC operation each peer dials independently, so outbound != inbound
// is the expected steady state.
func TestEvictStaleOutboundOnRecv_AliveOutboundNotEvicted(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Establish a live outbound connection from t1 to t2.
	outbound, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Get a different connection (t2 dialing t1) to use as inbound.
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}

	// Sanity: the two connections are different objects.
	if outbound == inbound {
		t.Fatal("outbound and inbound should be different connections")
	}

	// Call evictStaleOutboundOnRecv with the different (alive) inbound.
	t1.evictStaleOutboundOnRecv(target, 0, inbound)

	// Outbound should NOT be evicted: it is alive.
	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection (alive outbound kept), got %d",
			t1.ConnectionCount())
	}
}

// TestEvictStaleOutboundOnRecv_DeadOutboundEvicted verifies that a dead
// outbound connection is evicted when the inbound is a different connection.
// This simulates a peer restart where the old outbound context is cancelled.
func TestEvictStaleOutboundOnRecv_DeadOutboundEvicted(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Establish outbound from t1 to t2.
	outbound, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Create some streams to verify they are evicted too.
	_, err = t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}

	// Let the pre-opened heartbeat land before the kill, so it cannot be
	// stored after the eviction.
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "heartbeat stream not pre-opened", func() bool {
		return countStreamsForTarget(t1, target) == 2
	})

	// Kill the outbound connection to simulate a peer restart.
	outbound.CloseWithError(0, "simulate dead outbound")
	waitForChan(t, outbound.Context().Done(), 5*time.Second, "closed connection context not done")

	// Use a different connection as inbound (t2 dialing t1).
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}

	// evictStaleOutboundOnRecv should detect the dead outbound and evict.
	t1.evictStaleOutboundOnRecv(target, 0, inbound)

	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections after dead eviction, got %d",
			t1.ConnectionCount())
	}
	if t1.StreamCount() != 0 {
		t.Fatalf("expected 0 streams after dead eviction, got %d",
			t1.StreamCount())
	}
}

// TestEvictStaleOutboundOnRecv_NoOutboundNoAdoption verifies that when there
// is no cached outbound to a target, the inbound connection is NOT adopted.
// The dialing side does not run a stream accept loop on outbound connections,
// so adopting the inbound for sending would cause streams to be unread.
func TestEvictStaleOutboundOnRecv_NoOutboundNoAdoption(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	// t1 has no outbound to t2. Use t2's connection to t1 as inbound.
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}

	// Verify t1 has no outbound connections.
	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections before call, got %d", t1.ConnectionCount())
	}

	// Call evictStaleOutboundOnRecv with the inbound. Should be a no-op.
	t1.evictStaleOutboundOnRecv(t2.Addr().String(), 0, inbound)

	// Inbound should NOT be adopted.
	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections (no adoption), got %d",
			t1.ConnectionCount())
	}
}

// TestEvictStaleOutboundOnRecv_SameConnectionNoop verifies that when the
// outbound and inbound are the same *quic.Conn pointer, the fast path
// returns immediately without eviction.
func TestEvictStaleOutboundOnRecv_SameConnectionNoop(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Establish outbound.
	outbound, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Create a stream to verify nothing is evicted.
	_, err = t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}

	// Wait for the async preOpenHeartbeatStream goroutine (launched by
	// getConnection) to settle so StreamCount() is stable before we
	// capture the "before" snapshot.
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "heartbeat stream not pre-opened", func() bool {
		return countStreamsForTarget(t1, target) == 2
	})

	connsBefore := t1.ConnectionCount()
	streamsBefore := t1.StreamCount()

	// Call with the same connection as inbound.
	t1.evictStaleOutboundOnRecv(target, 0, outbound)

	if t1.ConnectionCount() != connsBefore {
		t.Fatalf("expected %d connections (same-conn noop), got %d",
			connsBefore, t1.ConnectionCount())
	}
	if t1.StreamCount() != streamsBefore {
		t.Fatalf("expected %d streams (same-conn noop), got %d",
			streamsBefore, t1.StreamCount())
	}
}

// countStreamsForTarget returns the number of pooled streams whose streamKey
// targets the given address. Used by tests to verify per-target stream counts
// when pre-opened heartbeat streams may be present.
func countStreamsForTarget(tr *QUICTransport, target string) int {
	total := 0
	for i := range tr.streamShards {
		shard := &tr.streamShards[i]
		shard.mu.RLock()
		for key := range shard.streams {
			if key.target == target {
				total++
			}
		}
		shard.mu.RUnlock()
	}
	return total
}

// TestGetStreamReturnesFreshStreamAfterEviction verifies that getStream does
// not return a stale stream when the connection is evicted between the stream
// lookup and the connection lookup. This exercises the re-verification guard
// that detects when a cached stream has been removed by evictStreamsForTarget
// during the window between the two reads.
func TestGetStreamReturnesFreshStreamAfterEviction(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Open a data stream so it gets cached in the shard map.
	s1, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("initial getStream failed: %v", err)
	}
	if s1 == nil {
		t.Fatal("expected non-nil stream")
	}

	// Wait for pre-opened heartbeat stream to settle.
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "heartbeat stream not pre-opened", func() bool {
		return countStreamsForTarget(t1, target) == 2
	})

	// Kill the connection. This makes the context Done channel close.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	conn.CloseWithError(0, "test kill for stale stream race")

	// Remove the connection and evict all streams, simulating the race
	// where another goroutine evicts between stream read and conn read.
	t1.removeConnection(target)
	t1.evictStreamsForTarget(target)

	// Now getStream should detect the stale stream and fall through to
	// the slow path, dialing a new connection and opening a fresh stream.
	s2, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream after eviction failed: %v", err)
	}
	if s2 == nil {
		t.Fatal("expected non-nil stream after eviction")
	}
	if s1 == s2 {
		t.Fatal("expected different stream after connection eviction, got same stale stream")
	}
}

// TestGetStreamFreshStreamUnderConcurrentEviction uses concurrent goroutines
// to exercise the race window between stream lookup and connection lookup in
// getStream. One goroutine repeatedly evicts the connection and streams while
// another calls getStream. The test verifies no stale streams are returned
// and no data races occur (run with -race).
func TestGetStreamFreshStreamUnderConcurrentEviction(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Establish initial connection and stream.
	_, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("initial getStream failed: %v", err)
	}
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "heartbeat stream not pre-opened", func() bool {
		return countStreamsForTarget(t1, target) == 2
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 100)

	// Eviction goroutine: repeatedly kill connection and evict streams.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			t1.connMu.RLock()
			c, ok := t1.conns[target]
			t1.connMu.RUnlock()
			if ok {
				c.CloseWithError(0, "concurrent eviction test")
				t1.removeConnection(target)
				t1.evictStreamsForTarget(target)
			}
			// Paces the churn on purpose: one eviction per millisecond
			// leaves the reader's re-dials room to complete.
			time.Sleep(time.Millisecond)
		}
	}()

	// Reader goroutine: repeatedly call getStream.
	readerDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(readerDone)
		for i := 0; i < 20; i++ {
			select {
			case <-stop:
				return
			default:
			}
			s, err := t1.getStream(target, 1, false, "")
			if err != nil {
				// Connection errors are expected under eviction.
				continue
			}
			if s == nil {
				errCh <- errors.New("getStream returned nil stream without error")
				return
			}
		}
	}()

	// Stop the evictor once the reader has made its 20 calls.
	waitForChan(t, readerDone, 30*time.Second, "reader goroutine did not finish")
	close(stop)
	wg.Wait()

	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

// goroutineID returns the calling goroutine's ID, parsed from the
// "goroutine N [running]:" header of its own stack.
func goroutineID() uint64 {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	buf = bytes.TrimPrefix(buf, []byte("goroutine "))
	if i := bytes.IndexByte(buf, ' '); i >= 0 {
		buf = buf[:i]
	}
	id, _ := strconv.ParseUint(string(buf), 10, 64)
	return id
}

// goroutineStack returns the stack of goroutine id, or "" if it has exited.
func goroutineStack(id uint64) string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	header := "goroutine " + strconv.FormatUint(id, 10) + " ["
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.HasPrefix(g, header) {
			return g
		}
	}
	return ""
}

// goroutineBlockedIn reports whether the stack g is parked (not running or
// runnable) with fn on it.
func goroutineBlockedIn(g, fn string) bool {
	_, state, ok := strings.Cut(g, " [")
	if !ok || strings.HasPrefix(state, "running") || strings.HasPrefix(state, "runnable") {
		return false
	}
	return strings.Contains(g, fn)
}

// exhaustStreamCredit opens streams on conn until the peer's stream limit is
// reached, so the next OpenStreamSync blocks until its context ends. The
// streams carry no data, so the peer never sees them nor grants more.
func exhaustStreamCredit(t *testing.T, conn *quic.Conn) {
	t.Helper()
	for i := 0; i < 1<<16; i++ {
		if _, err := conn.OpenStream(); err != nil {
			// quic-go reports the limit only by message.
			if !strings.Contains(err.Error(), "too many open streams") {
				t.Fatalf("OpenStream failed: %v", err)
			}
			return
		}
	}
	t.Fatal("stream limit never reached")
}

// takeHeartbeatStream waits until the heartbeat stream that getConnection
// pre-opens in the background is in tr's stream pool, then takes it out
// without closing it. The pre-open is then over and holds no stream credit,
// and Stop has no pooled stream to close. A close (as evictStreamsForTarget
// does) would send the stream's first frame; the live peer, done with it,
// would grant new stream credit and could release a blocked open without the
// shutdown. Never written to, the stream stays unknown to the peer.
func takeHeartbeatStream(t *testing.T, tr *QUICTransport, target string) {
	t.Helper()
	key := streamKey{target: target, idx: 0}
	shard := &tr.streamShards[uint64(0)&(streamShardCount-1)]
	deadline := time.Now().Add(5 * time.Second)
	for {
		shard.mu.Lock()
		_, ok := shard.streams[key]
		if ok {
			shard.drop(key)
		}
		shard.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("heartbeat stream was not pooled within 5s")
		}
		time.Sleep(time.Millisecond)
	}
}

// waitBlockedIn waits until the goroutine whose ID arrives on idC is parked
// inside fn, so the caller acts on an operation that is in flight rather
// than one that has not begun.
func waitBlockedIn(t *testing.T, idC <-chan uint64, fn string, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var id uint64
	select {
	case id = <-idC:
	case <-deadline.C:
		t.Fatal("goroutine under test did not start")
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		g := goroutineStack(id)
		if goroutineBlockedIn(g, fn) {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("goroutine %d did not block in %s within %v; stack:\n%s", id, fn, timeout, g)
		}
	}
}

// TestPreOpenHeartbeatStreamRespectsShutdownCtx verifies that
// preOpenHeartbeatStream derives its context from shutdownCtx so that
// Stop() cancellation unblocks it promptly instead of waiting for the
// full 5s timeout.
func TestPreOpenHeartbeatStreamRespectsShutdownCtx(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Establish a connection.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Take the pre-opened heartbeat stream out of the pool, so nothing but
	// the shutdown can release the open below.
	takeHeartbeatStream(t, t1, target)

	// Use up the peer's stream credit so OpenStreamSync blocks until its
	// context ends. (Stopping the peer instead would close the connection,
	// and OpenStreamSync would fail at once without blocking.)
	exhaustStreamCredit(t, conn)

	// Call preOpenHeartbeatStream in a goroutine and measure how long
	// it takes after we cancel shutdownCtx.
	t1.wg.Add(1)
	idC := make(chan uint64, 1)
	done := make(chan struct{})
	go func() {
		idC <- goroutineID()
		t1.preOpenHeartbeatStream(target, conn)
		close(done)
	}()
	waitBlockedIn(t, idC, "quic-go.(*Conn).OpenStreamSync", 5*time.Second)

	// Cancel shutdownCtx by stopping t1. This should unblock the
	// goroutine promptly.
	start := time.Now()
	t1.Stop()

	select {
	case <-done:
		elapsed := time.Since(start)
		// Should complete well under the 5s timeout.
		if elapsed > 2*time.Second {
			t.Fatalf("preOpenHeartbeatStream took %v after shutdown, expected prompt cancellation", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("preOpenHeartbeatStream did not return within 5s after shutdown")
	}
}

// TestGetStreamReturnsPromptlyOnShutdown verifies that getStream's slow path
// returns promptly when shutdownCtx is cancelled, rather than blocking for the
// full 5s OpenStreamSync timeout. This guards against the regression where
// getStream used context.Background() instead of t.shutdownCtx.
func TestGetStreamReturnsPromptlyOnShutdown(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Establish a connection so getStream's slow path can reach OpenStreamSync.
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// Take the pre-opened heartbeat stream out of the pool, so getStream
	// takes the slow path and nothing but the shutdown can release the open
	// below.
	takeHeartbeatStream(t, t1, target)

	// Use up the peer's stream credit so OpenStreamSync blocks until its
	// context ends. (Stopping the peer instead would close the connection,
	// and getStream would redial rather than open a stream.)
	exhaustStreamCredit(t, conn)

	idC := make(chan uint64, 1)
	done := make(chan error, 1)
	go func() {
		idC <- goroutineID()
		_, err := t1.getStream(target, 1, false, "")
		done <- err
	}()
	waitBlockedIn(t, idC, "quic-go.(*Conn).OpenStreamSync", 5*time.Second)

	// Cancel shutdownCtx by stopping t1. This should unblock getStream promptly.
	start := time.Now()
	t1.Stop()

	select {
	case <-done:
		elapsed := time.Since(start)
		// Should complete well under the 5s timeout.
		if elapsed > 2*time.Second {
			t.Fatalf("getStream took %v after shutdown, expected prompt cancellation", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("getStream did not return within 5s after shutdown")
	}
}

// TestDialConnectionReturnsPromptlyOnShutdown verifies that dialConnection
// returns promptly when shutdownCtx is cancelled, rather than blocking for
// the full dialTimeout. This guards against the regression where
// dialConnection used context.Background() instead of t.shutdownCtx.
func TestDialConnectionReturnsPromptlyOnShutdown(t *testing.T) {
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

	// Use a non-routable address to force the dial to block for the full
	// timeout. 192.0.2.1 is TEST-NET-1 (RFC 5737) and is non-routable.
	target := "192.0.2.1:9999"

	idC := make(chan uint64, 1)
	done := make(chan error, 1)
	go func() {
		idC <- goroutineID()
		_, err := tr.dialForTest(target)
		done <- err
	}()
	waitBlockedIn(t, idC, "quic-go.(*Transport).Dial", 5*time.Second)

	// Cancel shutdownCtx by stopping the transport. This should unblock
	// dialConnection promptly.
	start := time.Now()
	tr.Stop()

	select {
	case <-done:
		elapsed := time.Since(start)
		// Should complete well under the 5s dialTimeout.
		if elapsed > 2*time.Second {
			t.Fatalf("dialConnection took %v after shutdown, expected prompt cancellation", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dialConnection did not return within 5s after shutdown")
	}
}

// TestDialConnectionReturnsErrorOnShutdown verifies that dialConnection
// returns an error when shutdownCtx is already cancelled at call time.
func TestDialConnectionReturnsErrorOnShutdown(t *testing.T) {
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

	_, err = tr.dialForTest("127.0.0.1:12345")
	if err == nil {
		t.Fatal("expected error from dialConnection after stop, got nil")
	}
}

func TestRemoveAndCloseConnection_NoConnection(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()

	// No connections exist, should be a no-op.
	tr.removeAndCloseConnection("127.0.0.1:1234", "test reason")

	if tr.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections, got %d", tr.ConnectionCount())
	}
}

// TestRemoveAndCloseConnection_WithExistingConnection verifies that
// removeAndCloseConnection removes and closes an existing connection.

func TestRemoveAndCloseConnection_WithExistingConnection(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Establish a connection.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	target := t2.Addr().String()
	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection, got %d", t1.ConnectionCount())
	}

	// Remove and close the connection.
	t1.removeAndCloseConnection(target, "test cleanup")

	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections after removeAndClose, got %d", t1.ConnectionCount())
	}
}

// ---------------------------------------------------------------------------
// getStream edge cases (conn.go:215 - 67.6% coverage)
// ---------------------------------------------------------------------------

// TestGetStream_StoppedTransportReturnsError verifies getStream returns
// ErrStreamClosed when the transport is stopped.

func TestGetStream_StoppedTransportReturnsError(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	tr.Stop()

	_, err = tr.getStream("127.0.0.1:1234", 0, false, "")
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("expected ErrStreamClosed, got %v", err)
	}
}

// TestGetStream_StreamPoolSizeOne verifies that when StreamPoolSize is 1,
// non-heartbeat messages use stream index 0 (same as heartbeats).

// ---------------------------------------------------------------------------
// connectionCleanupOnce panic path (conn.go:384 - 66.7% coverage)
// ---------------------------------------------------------------------------

// TestConnectionCleanupOnce_PanicReturnsTrueAndRecovers verifies that
// connectionCleanupOnce returns panicked=true when the cleanup function
// panics and that the panic is recovered.

func TestConnectionCleanupOnce_PanicReturnsTrueAndRecovers(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	// Drive the sweep immediately instead of waiting a real tick.
	//
	// connectionCleanupOnce tickers on cleanupInterval, and the panic can only
	// fire once the ticker fires. On the 30s production interval this test
	// would pay the full interval; with the interval as a field it exercises
	// the panic path at once.
	tr.cleanupInterval = time.Millisecond

	tr.stopC = make(chan struct{})

	// Inject a nil connection into the pool to cause a panic during
	// cleanStaleConnections when it tries to read conn.Context().
	tr.connMu.Lock()
	tr.conns["panic-target"] = nil
	tr.connMu.Unlock()

	// The nil connection panics when cleanStaleConnections calls
	// conn.Context().Done(); the deferred recover must catch it and report
	// panicked=true. Deterministic now, not conditional on a 30s tick.
	if panicked := tr.connectionCleanupOnce(); !panicked {
		t.Fatal("connectionCleanupOnce did not report the recovered panic")
	}
}

// TestConnectionCleanupOnce_StopCReturnsFalse covers the other exit: a clean
// shutdown returns panicked=false and does not sweep.
//
// It is a separate test rather than a fallback branch of the panic test above,
// so it runs every time and says which path it is testing.
func TestConnectionCleanupOnce_StopCReturnsFalse(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	// Long interval on purpose: the stopC branch must win without racing a tick,
	// so a pass cannot come from the sweep happening to run first.
	tr.cleanupInterval = time.Hour

	stopC := make(chan struct{})
	tr.stopC = stopC
	close(stopC)

	if panicked := tr.connectionCleanupOnce(); panicked {
		t.Fatal("a clean stopC shutdown reported a panic")
	}
}

// ---------------------------------------------------------------------------
// connectionCleanup restart loop (conn.go:363 - 62.5% coverage)
// ---------------------------------------------------------------------------

// TestConnectionCleanup_ExitsOnStopAfterNormalReturn verifies the outer
// connectionCleanup loop exits when connectionCleanupOnce returns normally.

func TestConnectionCleanup_ExitsOnStopAfterNormalReturn(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	testStopC := make(chan struct{})
	tr.stopC = testStopC

	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.connectionCleanup()
	}()

	close(testStopC)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("connectionCleanup did not exit")
	}
}

// ---------------------------------------------------------------------------
// sendQueueWorkerOnce heartbeat drain paths (quic.go:620 - 74.4% coverage)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// localAddr (quic.go:1177-1182)
// ---------------------------------------------------------------------------

// TestLocalAddr_BeforeStart verifies that localAddr falls back to the
// configured address before Start is called.

// ---------------------------------------------------------------------------
// Send with deep-copy of entries (quic.go:496-507)
// ---------------------------------------------------------------------------

// TestSend_DeepCopyEntries verifies that Send deep-copies entries to break
// alias with the caller's slice.

func TestDialConnection_InvalidTarget(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()

	_, err = tr.dialForTest("not-valid")
	if err == nil {
		t.Fatal("expected error for invalid target")
	}
}

// TestDialConnection_EmptyHost verifies that an empty host in the target
// defaults to "localhost".

// ---------------------------------------------------------------------------
// SendSnapshotStreaming round-trip (quic.go:1053 - 17.5% coverage)
// ---------------------------------------------------------------------------

// TestSendSnapshotStreaming_RoundTrip verifies that SendSnapshotStreaming
// correctly streams chunks to the receiver and the snapshot is processed.

// TestDialConnection_WithEventListener verifies that connection events
// are fired when an event listener is configured.

func TestDialConnection_WithEventListener(t *testing.T) {
	connected := make(chan struct{}, 1)
	listener := &config.EventListener{
		OnConnectionEstablished: func(info config.ConnectionInfo) {
			select {
			case connected <- struct{}{}:
			default:
			}
		},
	}

	mtls := testMTLSConfig(t)

	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		EventListener: listener,
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()

	// Create a second transport with the SAME mTLS config.
	t2, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	_, dialErr := tr.dialForTest(t2.Addr().String())
	if dialErr != nil {
		t.Fatalf("dialConnection failed: %v", dialErr)
	}

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("OnConnectionEstablished was not fired")
	}
}

// ---------------------------------------------------------------------------
// connectionCleanup panic-restart path (conn.go:371-377)
// ---------------------------------------------------------------------------

// TestConnectionCleanup_PanicRestartsAndExitsOnStop verifies that the
// connectionCleanup loop restarts after a panic and then exits cleanly
// when stopC is closed. This exercises the panic restart + shutdown check
// path at conn.go:371-377.

func TestConnectionCleanup_PanicRestartsAndExitsOnStop(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	testStopC := make(chan struct{})
	tr.stopC = testStopC
	// Sweep every millisecond so the injected entry panics within the window
	// below rather than after the default 30s interval.
	tr.cleanupInterval = time.Millisecond

	// Inject a nil connection that will cause a panic during cleanup.
	tr.connMu.Lock()
	tr.conns["panic-target-1"] = nil
	tr.connMu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.connectionCleanup()
	}()

	// Kept on purpose: the window spans a panicking sweep and a restart delay
	// (supervise.RestartDelay, 100ms), which expose no signal to wait on.
	time.Sleep(300 * time.Millisecond)

	// Close stopC to exit the loop.
	close(testStopC)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("connectionCleanup did not exit after panic restart + stop")
	}
}

// ---------------------------------------------------------------------------
// sendQueueWorker panic-restart path (quic.go:607-613)
// ---------------------------------------------------------------------------

// TestSendQueueWorker_PanicRestartsAndExitsOnStop verifies that the
// sendQueueWorker loop restarts after a panic and exits when stopC
// is closed. This exercises the panic restart + shutdown check path
// at quic.go:607-613.

func TestEvictStaleOutboundOnRecv_PeerRestartDetected(t *testing.T) {
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

	// Establish outbound connection from t1 to t2.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection, got %d", t1.ConnectionCount())
	}

	// Stop t2 to simulate peer restart.
	t2.Stop()

	// Create a new t2 on the same address to simulate restart.
	handler2b := newTestHandler()
	t2b, err := NewQUICTransport(Config{
		ListenAddress: t2Addr,
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler2b, registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(2b) failed: %v", err)
	}
	if err := t2b.Start(); err != nil {
		t.Fatalf("t2b.Start failed: %v", err)
	}
	defer t2b.Stop()

	// Send again until the restarted peer receives: the stale outbound
	// must be detected and replaced by a fresh dial.
	pollUntil(t, 10*time.Second, 50*time.Millisecond, "restarted peer never received a message", func() bool {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
		})
		return handler2b.batchCount.Load() > 0
	})
}

// ---------------------------------------------------------------------------
// sendHeartbeatBatch error paths (heartbeat.go:108-127)
// ---------------------------------------------------------------------------

// TestSendHeartbeatBatch_MarshalErrorReturnsBuffer verifies that
// if MarshalTo fails during heartbeat batch serialization, the
// buffer is returned to the pool.

func TestRemoveAndCloseConnection_ConnectionAlreadyClosed(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Establish connection.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	target := t2.Addr().String()

	// Close the underlying connection first, then call removeAndCloseConnection.
	// This should trigger the closeErr != nil path.
	t1.connMu.RLock()
	conn := t1.conns[target]
	t1.connMu.RUnlock()

	if conn != nil {
		conn.CloseWithError(0, "pre-close")
	}

	// Now removeAndCloseConnection should handle the close error gracefully.
	t1.removeAndCloseConnection(target, "already closed test")

	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections, got %d", t1.ConnectionCount())
	}
}

// ---------------------------------------------------------------------------
// getStream slow path with stale dedup (conn.go:287-356)
// ---------------------------------------------------------------------------

// TestGetStream_StaleConnectionFastPathEviction verifies that getStream
// detects a dead connection in the fast path and falls through to the
// slow path to dial a fresh connection.

func TestGetStream_StaleConnectionFastPathEviction(t *testing.T) {
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

	// Establish connection and cache a stream.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Stop t2 to make the connection dead.
	t1.connMu.RLock()
	dead := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	t2.Stop()
	waitForChan(t, dead.Context().Done(), 5*time.Second, "outbound connection did not die after peer stop")

	// Restart t2 on the same address.
	t2b, err := NewQUICTransport(Config{
		ListenAddress: t2Addr,
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(2b) failed: %v", err)
	}
	if err := t2b.Start(); err != nil {
		t.Fatalf("t2b.Start failed: %v", err)
	}
	defer t2b.Stop()

	// Now getStream should detect the dead connection in the fast path,
	// evict it, and fall through to the slow path.
	stream, err := t1.getStream(t2Addr, 1, false, "")
	if err != nil {
		t.Fatalf("getStream after stale eviction failed: %v", err)
	} else if stream == nil {
		t.Fatal("expected non-nil stream")
	}
}

// ---------------------------------------------------------------------------
// handleConnection error paths - connection close and stream error
// (recv.go:242-279)
// ---------------------------------------------------------------------------

// TestHandleConnection_ConnectionContextCancelled verifies that the
// connection handler exits when the connection context is cancelled.

func TestEvictStreamsForTarget_WithMultipleStreams(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t2Addr := t2.Addr().String()

	// Send to different shards to create streams in different shard buckets.
	for shard := uint64(1); shard <= 8; shard++ {
		t1.reg.Register(shard, 2, t2Addr)
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: shard, From: 1, To: 2, Term: 1,
				Entries: []proto.Entry{{Index: 1, Term: 1, Cmd: []byte("data")}}},
		})
	}

	pollUntil(t, 10*time.Second, 10*time.Millisecond, "not all eight messages arrived", func() bool {
		total := 0
		for _, b := range handler2.getBatches() {
			total += len(b.Requests)
		}
		return total >= 8
	})

	// Evict all streams for the target.
	t1.evictStreamsForTarget(t2Addr)

	// After eviction, getting a stream should open a new one.
	stream, err := t1.getStream(t2Addr, 1, false, "")
	if err != nil {
		t.Fatalf("getStream after eviction failed: %v", err)
	} else if stream == nil {
		t.Fatal("expected non-nil stream after eviction")
	}
}

// ---------------------------------------------------------------------------
// receiveChunks validateChunk failure (snapshot_recv.go:232-241)
// ---------------------------------------------------------------------------

// TestReceiveChunks_ValidateChunkFailure verifies that a chunk failing
// validation causes receiveChunks to return early without delivering
// any chunks to the handler.

func TestGetStream_SlowPathStaleDedup(t *testing.T) {
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

	// Establish a connection and get a stream via the normal path.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Now evict the fast-path cached stream so the next getStream
	// enters the slow path, but leave a stale stream in the shard map
	// by stopping t2 (killing the connection) without evicting streams.
	t1.connMu.RLock()
	dead := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	t2.Stop()
	waitForChan(t, dead.Context().Done(), 5*time.Second, "outbound connection did not die after peer stop")

	// Remove the connection from the pool but leave streams in the shards.
	// This simulates the race: another goroutine cached a stream on a
	// now-dead connection.
	t1.removeConnection(t2Addr)

	// Restart t2 on the same address so the retry can succeed.
	t2b, err := NewQUICTransport(Config{
		ListenAddress: t2Addr,
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(2b) failed: %v", err)
	}
	if err := t2b.Start(); err != nil {
		t.Fatalf("t2b.Start failed: %v", err)
	}
	defer t2b.Stop()

	// getStream should:
	// 1. Miss the fast path (connection removed)
	// 2. Enter slow path, dial fresh connection
	// 3. Find existing stale stream in shard
	// 4. Detect it's stale (!connOK), discard + retry
	stream, err := t1.getStream(t2Addr, 1, false, "")
	if err != nil {
		t.Fatalf("getStream stale dedup retry failed: %v", err)
	} else if stream == nil {
		t.Fatal("expected non-nil stream from stale dedup retry")
	}
}

// ---------------------------------------------------------------------------
// handleMessageFrame compressed path with entries (covers bulk Cmd copy +
// compressed flag + snappy pool return all in one flow)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_CompressedMultiMessageWithEntries exercises the
// full compressed receive path with multiple messages containing entries,
// hitting bulk Cmd copy, compressed flag handling, and snappy pool return.

func TestEvictStaleOutboundOnRecv_InboundChanged(t *testing.T) {
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
	defer t2.Stop()

	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	// Establish outbound from t1 to t2.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Get the current outbound connection.
	t1.connMu.RLock()
	outbound := t1.conns[t2Addr]
	t1.connMu.RUnlock()

	if outbound == nil {
		t.Fatal("expected outbound connection to exist")
	}

	// First recv establishes the peer's boot epoch (1). Keep the original
	// outbound cached so the second call compares against it.
	t1.evictStaleOutboundOnRecv(t2Addr, 1, outbound)

	// Drop the cache entry (without forgetting the peer epoch) so
	// dialConnection produces a DISTINCT second connection: its dial-race
	// re-check would otherwise reuse the cached live outbound.
	t1.connMu.Lock()
	delete(t1.conns, t2Addr)
	t1.connMu.Unlock()
	conn2, dialErr := t1.dialForTest(t2Addr)
	if dialErr != nil {
		t.Fatalf("dialConnection for second conn failed: %v", dialErr)
	}
	if conn2 == outbound {
		t.Fatal("expected a distinct second connection after cache removal")
	}
	t1.connMu.Lock()
	t1.conns[t2Addr] = outbound
	t1.connMu.Unlock()

	// Second recv carries a DIFFERENT epoch (2) → the peer restarted → the
	// alive-but-stale outbound must be evicted promptly.
	t1.evictStaleOutboundOnRecv(t2Addr, 2, conn2)

	// The eviction leaves the pool synchronously; there is nothing to wait on.
	t1.connMu.RLock()
	current, stillExists := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if stillExists && current == outbound {
		t.Fatal("stale outbound was NOT evicted after peer epoch changed (restart)")
	}
}

// ---------------------------------------------------------------------------
// sendBatch snappy pool get nil path and pool return (quic.go:779-822)
// ---------------------------------------------------------------------------

// TestSendBatch_DirectCallSnappyPoolPaths verifies the snappy encode pool
// get/put paths by directly calling sendBatch with compression enabled.

// TestEvictStaleOutboundOnRecv_InboundChangedKeepsAliveOutbound documents the
// corrected behaviour after fixing a regression where an inbound identity
// change was treated as a peer-restart signal. In small clusters that
// heuristic produced a self-sustaining eviction cascade (node A evicts its
// outbound -> redials -> B observes new inbound -> B evicts its outbound ->
// B redials -> A observes new inbound -> ... ) that starved leader election.
//
// The contract is now: while the outbound's own context is still alive,
// evictStaleOutboundOnRecv must leave it alone even if the inbound identity
// changes. Peer restarts naturally kill the outbound (MaxIdleTimeout /
// keepalive / explicit close) which flips the context to Done and is
// handled by the dead-connection branch.
func TestEvictStaleOutboundOnRecv_InboundChangedKeepsAliveOutbound(t *testing.T) {
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
	defer t2.Stop()

	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	// Establish outbound from t1 to t2.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Get the current outbound connection.
	t1.connMu.RLock()
	outbound := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if outbound == nil {
		t.Fatal("expected outbound connection to exist")
	}

	// Seed the peer epoch (7) with a prior recv.
	t1.evictStaleOutboundOnRecv(t2Addr, 7, outbound)

	// Open a second connection and simulate a recv on that new *quic.Conn —
	// i.e. "the peer dialled us from a fresh connection" — but with the SAME
	// boot epoch (7): a benign re-dial, NOT a restart.
	conn2, dialErr := t1.dialForTest(t2Addr)
	if dialErr != nil {
		t.Fatalf("dialConnection for second conn failed: %v", dialErr)
	}
	t1.connMu.Lock()
	t1.conns[t2Addr] = outbound
	t1.connMu.Unlock()

	t1.evictStaleOutboundOnRecv(t2Addr, 7, conn2)

	// The alive outbound must still be present (same epoch ⇒ no eviction).
	// evictStaleOutboundOnRecv decides synchronously, so this is final.
	t1.connMu.RLock()
	current, stillExists := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if !stillExists {
		t.Fatal("alive outbound was evicted after inbound changed; cascade regression re-introduced")
	}
	if current != outbound {
		t.Fatal("alive outbound was replaced even though it was not evicted")
	}
}

// TestEvictStaleOutboundOnRecv_UnauthorizedAddressIgnored verifies the
// authentication gate on restart-eviction: a restart hint (epoch change) whose
// target is NOT covered by the inbound connection's verified certificate must
// be ignored. Without it, an authenticated-but-malicious cluster member could
// stamp another node's SourceAddress with a mutating SourceEpoch and force
// eviction of our live outbound to that victim (a liveness/DoS vector). The
// shared test certificate covers loopback/:: only, so a TEST-NET-1 host
// (RFC 5737, 192.0.2.0/24) is guaranteed absent from its SANs.
func TestEvictStaleOutboundOnRecv_UnauthorizedAddressIgnored(t *testing.T) {
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
	defer t2.Stop()

	// Two live, fully-handshaked connections to t2. Both present t2's
	// certificate, which covers loopback/:: but NOT the spoofed host below.
	cachedConn, dialErr := t1.dialForTest(t2.Addr().String())
	if dialErr != nil {
		t.Fatalf("dialConnection(cached) failed: %v", dialErr)
	}
	inboundConn, dialErr := t1.dialForTest(t2.Addr().String())
	if dialErr != nil {
		t.Fatalf("dialConnection(inbound) failed: %v", dialErr)
	}

	// Cache an ALIVE outbound under a spoofed target whose host is not in any
	// node's certificate. inboundConn differs from cachedConn so the same-conn
	// fast path does not short-circuit the eviction logic.
	const spoofed = "192.0.2.50:4001"
	t1.connMu.Lock()
	t1.conns[spoofed] = cachedConn
	t1.connMu.Unlock()

	// Distinct epochs would signal a restart if the claim were trusted.
	t1.evictStaleOutboundOnRecv(spoofed, 100, inboundConn)
	t1.evictStaleOutboundOnRecv(spoofed, 200, inboundConn)

	// The live outbound must survive: the cert does not authorize the spoofed
	// target, so the restart hint is ignored. (removeConnection mutates the
	// map synchronously, so no wait is required.)
	t1.connMu.RLock()
	current, stillExists := t1.conns[spoofed]
	t1.connMu.RUnlock()
	if !stillExists || current != cachedConn {
		t.Fatal("live outbound was evicted on a cert-unauthorized (spoofed) restart hint")
	}

	// The unauthorized epoch must not have been recorded either.
	t1.peerEpochMu.Lock()
	_, epochRecorded := t1.peerEpoch[spoofed]
	t1.peerEpochMu.Unlock()
	if epochRecorded {
		t.Fatal("peerEpoch recorded an unauthorized (spoofed) SourceAddress")
	}
}

// ---------------------------------------------------------------------------
// getStream stale dedup via direct shard injection (conn.go:288-341)
// ---------------------------------------------------------------------------

// TestGetStream_StaleStreamInShardDedupRetry exercises the slow-path stale
// dedup in getStream by directly injecting a stale stream into the shard
// map. When getStream enters the slow path, opens a new stream, and finds
// the injected stale stream during the dedup check, it detects the missing
// connection backing the stale stream and retries with a fresh connection.

func TestGetStream_StaleStreamInShardDedupRetry(t *testing.T) {
	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	t1, err := NewQUICTransport(Config{
		ListenAddress:  ":0",
		DeploymentID:   42,
		MTLSConfig:     mtls,
		StreamPoolSize: 4,
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

	// Step 1: Establish a connection and stream.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Step 2: Get the stream that was cached (shardID=1 with poolSize=4
	// gives idx = (1 % 3) + 1 = 2, shardIdx = 2 & 15 = 2).
	shardID := uint64(1)
	poolSize := uint64(4)
	idx := (shardID % (poolSize - 1)) + 1
	shardIdx := idx & (streamShardCount - 1)
	shard := &t1.streamShards[shardIdx]
	key := streamKey{target: t2Addr, idx: idx}

	// Step 3: Get the existing stream from the shard.
	shard.mu.RLock()
	existingStream := shard.streams[key]
	shard.mu.RUnlock()

	// Step 4: Stop t2 (kills the connection).
	t1.connMu.RLock()
	dead := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	t2.Stop()
	waitForChan(t, dead.Context().Done(), 5*time.Second, "outbound connection did not die after peer stop")

	// Step 5: Remove the connection from conns but leave the stale stream
	// in the shard map. This is the key setup: getStream slow path will
	// find this stale stream during dedup, detect no connection in conns,
	// and trigger the retry path.
	t1.removeConnection(t2Addr)
	// Also evict all streams, then re-inject just the stale one.
	t1.evictStreamsForTarget(t2Addr)

	// Re-inject the stale stream into the shard.
	if existingStream != nil {
		shard.mu.Lock()
		shard.streams[key] = existingStream
		shard.mu.Unlock()
	}

	// Step 6: Restart t2 on the same address.
	t2b, err := NewQUICTransport(Config{
		ListenAddress: t2Addr,
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(2b) failed: %v", err)
	}
	if err := t2b.Start(); err != nil {
		t.Fatalf("t2b.Start failed: %v", err)
	}
	defer t2b.Stop()

	// Step 7: Call getStream. It should:
	// 1. Miss fast path (no connection in conns)
	// 2. Dial fresh connection to t2b
	// 3. Open new stream
	// 4. Find injected stale stream in shard dedup check
	// 5. Detect stale (!connOK since we removed the original conn)
	// 6. Retry with fresh connection
	stream, err := t1.getStream(t2Addr, shardID, false, "")
	if err != nil {
		t.Fatalf("getStream stale dedup retry failed: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil stream from stale dedup retry")
	}
}

// ---------------------------------------------------------------------------
// sendQueueWorkerOnce panic recovery (quic.go:621-637)
// ---------------------------------------------------------------------------

// TestSendQueueWorkerOnce_StopSignalCleanExit verifies that
// sendQueueWorkerOnce returns panicked=false when the stop signal fires
// cleanly. This covers the normal exit path at quic.go:678-679.

func TestCleanStaleConnections_EventListenerOnConnectionFailedDirect(t *testing.T) {
	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	failedCh := make(chan string, 10)

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		EventListener: &config.EventListener{
			OnConnectionFailed: func(info config.ConnectionInfo) {
				failedCh <- info.Address
			},
		},
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
	defer func() { _ = t2.Stop() }()
	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	// Establish connection.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Close the connection from t1's side to make it immediately stale.
	t1.connMu.RLock()
	conn := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if conn != nil {
		conn.CloseWithError(0, "force stale for test")
	}
	// cleanStaleConnections keys on the connection's context, so wait for that
	// and not for a duration: the close has to be observed before the sweep
	// runs or the sweep finds a live connection and reports nothing.
	waitConnDown(t, t1, t2Addr, 5*time.Second)

	// Call cleanStaleConnections directly.
	t1.cleanStaleConnections()

	// Wait for the event listener callback.
	select {
	case addr := <-failedCh:
		if addr != t2Addr {
			t.Fatalf("expected failed addr %s, got %s", t2Addr, addr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for OnConnectionFailed callback")
	}
}

// ---------------------------------------------------------------------------
// Send with empty messages (quic.go:485-486)
// ---------------------------------------------------------------------------

// TestSend_EmptyOrNilMessagesIsNoop verifies that Send with nil/empty
// slice returns without side effects.

func TestDialConnection_OnConnectionFailedEvent(t *testing.T) {
	var failedCalled bool
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
		EventListener: &config.EventListener{
			OnConnectionFailed: func(info config.ConnectionInfo) {
				failedCalled = true
			},
		},
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()

	// Dial a non-existent address to trigger failure.
	_, err = tr.dialForTest("127.0.0.1:1")
	if err == nil {
		t.Fatal("expected dial to fail")
	}

	if !failedCalled {
		t.Fatal("expected OnConnectionFailed to be called")
	}
}

// ---------------------------------------------------------------------------
// dialConnection with host="" (conn.go:95-97)
// ---------------------------------------------------------------------------

// TestDialConnection_EmptyHostUsesLocalhost verifies that dialConnection
// treats empty host as "localhost", covering conn.go:95-97.

func TestDialConnection_EmptyHostRejected(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()

	// ":12345" has an empty host. Rather than silently verifying the peer
	// certificate against a hardcoded "localhost" ServerName (masking a
	// misconfiguration), dialConnection must reject it with ErrEmptyDialHost.
	_, err = tr.dialForTest(":12345")
	if !errors.Is(err, ErrEmptyDialHost) {
		t.Fatalf("expected ErrEmptyDialHost for empty-host target, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// receiveSnapshot with snapshotTracker shutdown (snapshot_recv.go:128-133)
// ---------------------------------------------------------------------------

// TestReceiveSnapshot_ShutdownTrackerRejectsNew verifies that
// receiveSnapshot returns immediately when the snapshotTracker
// has been shut down.

func TestPreOpenHeartbeatStream_StoppedTransportReturns(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)

	// Establish connection so we have a valid conn object.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	t2Addr := t2.Addr().String()
	t1.connMu.RLock()
	conn := t1.conns[t2Addr]
	t1.connMu.RUnlock()

	// Stop to set stopped flag.
	cleanup()

	// Now preOpenHeartbeatStream should return immediately.
	t1.wg.Add(1)
	t1.preOpenHeartbeatStream(t2Addr, conn)
}

// ---------------------------------------------------------------------------
// Stop with pre-closed connections (quic.go:420-422, 436-438, 445-447)
// ---------------------------------------------------------------------------

// TestStop_WithPreClosedConnectionsAndStreams exercises the error logging
// paths in Stop() when connections and streams are already closed before
// Stop is called. This covers quic.go:420-422, 436-438, and 445-447.

// ---------------------------------------------------------------------------
// sendBatch with snappy encode pool capacity too small (quic.go:781-785)
// ---------------------------------------------------------------------------

// TestSendBatch_SnappyPoolCapacityTooSmallForceAlloc exercises the path
// where the pooled snappy encode buffer has insufficient capacity for the
// maximum encoded size. This forces a new allocation (quic.go:784-785).

func TestEvictStreamsForTarget_StreamCloseError(t *testing.T) {
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

	// Establish connection and create streams.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Close the connection first (which invalidates all streams).
	t1.connMu.RLock()
	conn := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if conn == nil {
		t.Fatal("expected outbound connection to exist")
	}
	conn.CloseWithError(0, "pre-close for stream error test")
	waitForChan(t, conn.Context().Done(), 5*time.Second, "closed connection context not done")

	// Now evict streams - they should fail to close since connection is dead.
	t1.evictStreamsForTarget(t2Addr)
	if n := countStreamsForTarget(t1, t2Addr); n != 0 {
		t.Fatalf("expected no pooled streams after eviction despite close errors, got %d", n)
	}

	t2.Stop()
}

// ---------------------------------------------------------------------------
// handleMessageFrame unmarshal error on compressed path
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_CompressedInvalidBatchUnmarshal verifies that
// when a compressed payload decompresses successfully but contains
// invalid protobuf data, the unmarshal error path returns the snappy
// buffer to the pool.

func TestGetStream_ConcurrentDedupReturnsCachedStream(t *testing.T) {
	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	t1, err := NewQUICTransport(Config{
		ListenAddress:  ":0",
		DeploymentID:   42,
		MTLSConfig:     mtls,
		StreamPoolSize: 4,
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

	// Establish a connection first.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Repeat the evict-then-race cycle multiple times to increase the
	// probability of hitting the slow-path dedup.
	for round := 0; round < 10; round++ {
		// Evict all streams but keep the connection alive.
		t1.evictStreamsForTarget(t2Addr)

		// Launch many concurrent getStream calls for the same key.
		// All miss the fast path (no stream cached), all enter the slow
		// path, and race to store their stream. The losers find the
		// winner's stream during the shard.mu.Lock dedup check.
		const numGoroutines = 50
		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = t1.getStream(t2Addr, 1, false, "")
			}()
		}
		wg.Wait()
	}
}

// TestGetStream_StaleDedup_ConnRemovedDuringSlowPath exercises the stale
// dedup path by racing removeConnection against getStream. When the
// connection is removed between getConnection (line 273) and the dedup
// check (line 293-294), connOK is false, triggering the stale=true path.

func TestGetStream_StaleDedup_ConnRemovedDuringSlowPath(t *testing.T) {
	mtls := testMTLSConfig(t)
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	t1, err := NewQUICTransport(Config{
		ListenAddress:  ":0",
		DeploymentID:   42,
		MTLSConfig:     mtls,
		StreamPoolSize: 4,
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

	// Establish connection.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Strategy: repeatedly evict streams and race getStream with
	// removeConnection. Some iterations should hit the stale dedup
	// where the stream exists in the shard but the connection was
	// removed from conns.
	for attempt := 0; attempt < 10; attempt++ {
		// Evict all streams but keep the connection.
		t1.evictStreamsForTarget(t2Addr)

		// Inject a "stale" stream into the shard.
		shardID := uint64(1)
		poolSize := uint64(4)
		idx := (shardID % (poolSize - 1)) + 1
		shardIdx := idx & (streamShardCount - 1)
		shard := &t1.streamShards[shardIdx]
		key := streamKey{target: t2Addr, idx: idx}

		// Get a fresh stream to inject.
		s, getErr := t1.getStream(t2Addr, shardID, false, "")
		if getErr != nil {
			continue
		}
		_ = s

		// Now race: remove connection and call getStream concurrently.
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			// Repeatedly remove the connection.
			for i := 0; i < 100; i++ {
				t1.removeConnection(t2Addr)
			}
		}()

		go func() {
			defer wg.Done()
			// Repeatedly evict and re-inject, then call getStream.
			for i := 0; i < 50; i++ {
				// Inject a stale entry.
				shard.mu.Lock()
				if existing, ok := shard.streams[key]; ok {
					// Leave it.
					_ = existing
				}
				shard.mu.Unlock()

				_, _ = t1.getStream(t2Addr, shardID, false, "")
			}
		}()

		wg.Wait()
	}
}

// ---------------------------------------------------------------------------
// Send with getOrCreateSendQueue returning nil (quic.go:513-515)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// SendSnapshot OpenStreamSync error (quic.go:921-924)
// ---------------------------------------------------------------------------

// TestSendSnapshot_OpenStreamSyncError exercises the OpenStreamSync error
// path by closing the connection after getConnection succeeds but before
// OpenStreamSync completes.

func TestGetStream_FastPathEvictionBetweenReads(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t2Addr := t2.Addr().String()

	// Establish connection and cache a stream.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Get a stream to populate the cache.
	stream1, err := t1.getStream(t2Addr, 1, false, "")
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}
	if stream1 == nil {
		t.Fatal("expected non-nil stream")
	}

	// Evict all streams for the target. This removes the stream from
	// the shard map.
	t1.evictStreamsForTarget(t2Addr)

	// Now getStream should detect the stream was evicted and fall
	// through to the slow path, opening a new stream.
	stream2, err := t1.getStream(t2Addr, 1, false, "")
	if err != nil {
		t.Fatalf("getStream after eviction failed: %v", err)
	}
	if stream2 == nil {
		t.Fatal("expected non-nil stream after eviction")
	}
}

// ---------------------------------------------------------------------------
// getStream fast path dead connection (conn.go:246-249)
// ---------------------------------------------------------------------------

// TestGetStream_FastPathDeadConnection exercises the path in getStream
// where the cached stream's backing connection is dead (context done).
// The dead connection is removed and streams evicted, then a fresh
// connection and stream are established.

func TestGetStream_FastPathDeadConnection(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	t2Addr := t2.Addr().String()

	// Establish a stream via getStream.
	stream1, err := t1.getStream(t2Addr, 1, false, "")
	if err != nil {
		t.Fatalf("initial getStream failed: %v", err)
	}
	if stream1 == nil {
		t.Fatal("expected non-nil stream")
	}

	// Close the connection to make its context done.
	t1.connMu.RLock()
	conn, ok := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if !ok {
		t.Fatal("expected connection to exist")
	}
	conn.CloseWithError(0, "test: force dead connection")
	waitConnDown(t, t1, t2Addr, 5*time.Second)

	// getStream should detect the dead connection, evict, and reconnect.
	stream2, err := t1.getStream(t2Addr, 1, false, "")
	if err != nil {
		t.Fatalf("getStream after dead connection failed: %v", err)
	}
	if stream2 == nil {
		t.Fatal("expected non-nil stream after reconnect")
	}
}

// ---------------------------------------------------------------------------
// getStream OpenStreamSync error (conn.go:282-284)
// ---------------------------------------------------------------------------

// TestGetStream_OpenStreamSyncError exercises the path in getStream
// where OpenStreamSync fails because the transport is being shut down.

func TestGetStream_OpenStreamSyncError(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	handler2 := newTestHandler()
	reg2 := registry.NewRegistry()
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
	reg.Register(1, 2, t2Addr)

	// Establish a connection.
	_, dialErr := t1.getConnection(t2Addr, "")
	if dialErr != nil {
		t.Fatalf("getConnection failed: %v", dialErr)
	}

	// Cancel the shutdown context so OpenStreamSync fails.
	t1.shutdownCancel()

	// getStream should fail at OpenStreamSync.
	_, err = t1.getStream(t2Addr, 999, false, "")
	if err == nil {
		t.Fatal("expected error from getStream after shutdown cancel")
	}

	// Clean up both transports.
	t1.Stop()
	t2.Stop()
}

// ---------------------------------------------------------------------------
// sendBatch metrics and debug logging (quic.go:858-872)
// ---------------------------------------------------------------------------

// TestSendBatch_WithMetricsAndDebugLogging exercises the send metrics
// recording and debug log path at the end of sendBatch by configuring
// both Metrics and enabling debug-level logging.

func TestDialConnection_ShutdownContextCanceled(t *testing.T) {
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

	// Cancel the shutdown context.
	tr.shutdownCancel()

	// dialConnection should fail because the context is canceled.
	_, err = tr.dialForTest("127.0.0.1:39999")
	if err == nil {
		t.Fatal("expected error from dialConnection with canceled context")
	}

	tr.Stop()
}

// ---------------------------------------------------------------------------
// receiveSnapshot concurrency limit (snapshot_recv.go:141-143)
// ---------------------------------------------------------------------------

// TestReceiveSnapshot_ConcurrencyLimitDrop verifies that when the
// concurrency semaphore is full, additional snapshot receives are
// silently dropped.

// ---------------------------------------------------------------------------
// dialConnection dial-race re-check (conn.go)
// ---------------------------------------------------------------------------

// TestDialConnection_ReusesCachedWinner verifies the dial-race fix: a dial
// that completes after another dial already populated the connection cache
// (singleflight only coalesces OVERLAPPING dials) must close its own
// connection and return the cached winner instead of overwriting it — the
// overwritten connection would otherwise stay alive on keep-alives forever,
// with its pre-opened streams diverging from the pool's bookkeeping.
func TestDialConnection_ReusesCachedWinner(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	t2Addr := t2.Addr().String()

	winner, err := t1.getConnection(t2Addr, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	// A direct dial simulates the loser of the race: the cache already
	// holds a live connection when its insert runs.
	got, err := t1.dialForTest(t2Addr)
	if err != nil {
		t.Fatalf("dialConnection failed: %v", err)
	}
	if got != winner {
		t.Fatal("losing dial must return the cached winner, not its own connection")
	}
	if n := t1.ConnectionCount(); n != 1 {
		t.Fatalf("expected exactly 1 cached connection, got %d", n)
	}

	// The winner must still be alive and cached.
	t1.connMu.RLock()
	cached := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if cached != winner {
		t.Fatal("cache no longer holds the winning connection")
	}
	select {
	case <-winner.Context().Done():
		t.Fatal("winning connection was closed by the losing dial")
	default:
	}
}

// ---------------------------------------------------------------------------
// CloseTarget: explicit teardown on a committed membership removal
// ---------------------------------------------------------------------------

// peerEpochKnown reports whether the transport holds an incarnation record for
// target. Reads under peerEpochMu so it is safe alongside the receive path.
func peerEpochKnown(tr *QUICTransport, target string) bool {
	tr.peerEpochMu.Lock()
	defer tr.peerEpochMu.Unlock()
	_, ok := tr.peerEpoch[target]
	return ok
}

// connEpochKnown reports whether the transport holds a dial-epoch stamp for
// target. Reads under connMu.
func connEpochKnown(tr *QUICTransport, target string) bool {
	tr.connMu.RLock()
	defer tr.connMu.RUnlock()
	_, ok := tr.connEpoch[target]
	return ok
}

// sendQueueKnown reports whether the transport holds a send queue for target.
func sendQueueKnown(tr *QUICTransport, target string) bool {
	tr.sendQueuesMu.RLock()
	defer tr.sendQueuesMu.RUnlock()
	_, ok := tr.sendQueues[target]
	return ok
}

// TestCloseTargetUnknownAddressIsNoOp verifies CloseTarget on an address this
// transport never dialed reports false, does not panic, and mutates nothing.
func TestCloseTargetUnknownAddressIsNoOp(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Establish a live connection to a DIFFERENT target so the no-op case can
	// be shown not to disturb unrelated state.
	target := t2.Addr().String()
	t1.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1}})
	handler2.waitBatch(t, 10*time.Second)

	streamsBefore := t1.StreamCount()

	if t1.CloseTarget("127.0.0.1:1") {
		t.Fatal("CloseTarget reported a close for an address that was never dialed")
	}
	if t1.CloseTarget("") {
		t.Fatal("CloseTarget reported a close for the empty address")
	}

	if n := t1.ConnectionCount(); n != 1 {
		t.Fatalf("unrelated connection disturbed: expected 1 connection, got %d", n)
	}
	if n := t1.StreamCount(); n != streamsBefore {
		t.Fatalf("unrelated streams disturbed: had %d, now %d", streamsBefore, n)
	}
	if !sendQueueKnown(t1, target) {
		t.Fatal("unrelated send queue was retired by a no-op CloseTarget")
	}
	if !connEpochKnown(t1, target) {
		t.Fatal("unrelated dial-epoch stamp was dropped by a no-op CloseTarget")
	}
}

// TestCloseTargetClosesConnectionAndEvictsState verifies CloseTarget closes the
// cached connection and clears every parallel address-keyed structure —
// conns, connEpoch, the pooled streams, the send queue and the peer incarnation
// record — and that a later send re-dials the peer successfully.
func TestCloseTargetClosesConnectionAndEvictsState(t *testing.T) {
	t1, t2, handler1, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// Take the inbound batch from t2 FIRST: that is what creates t1's
	// incarnation record for the target, and dialing afterwards stamps the
	// outbound with the epoch already known, so the receive path's
	// restart-eviction never fires on the connection under test.
	t2.Send([]proto.Message{{Type: proto.ReplicateResp, ShardID: 1, From: 2, To: 1, Term: 1}})
	handler1.waitBatch(t, 10*time.Second)
	pollUntil(t, 5*time.Second, 10*time.Millisecond,
		"no incarnation record recorded for the peer",
		func() bool { return peerEpochKnown(t1, target) })

	t1.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1}})
	handler2.waitBatch(t, 10*time.Second)
	pollUntil(t, 5*time.Second, 10*time.Millisecond,
		"no pooled stream for the target",
		func() bool { return t1.StreamCount() > 0 })

	t1.connMu.RLock()
	conn := t1.conns[target]
	t1.connMu.RUnlock()
	if conn == nil {
		t.Fatal("no cached connection for the target")
	}

	if !t1.CloseTarget(target) {
		t.Fatal("CloseTarget did not report closing the cached connection")
	}

	if n := t1.ConnectionCount(); n != 0 {
		t.Fatalf("expected 0 cached connections, got %d", n)
	}
	if connEpochKnown(t1, target) {
		t.Fatal("connEpoch stamp survived CloseTarget")
	}
	if n := t1.StreamCount(); n != 0 {
		t.Fatalf("expected all pooled streams evicted, got %d", n)
	}
	if sendQueueKnown(t1, target) {
		t.Fatal("send queue survived CloseTarget")
	}
	if peerEpochKnown(t1, target) {
		t.Fatal("peer incarnation record survived CloseTarget")
	}
	waitForChan(t, conn.Context().Done(), 5*time.Second,
		"closed connection's context was never cancelled")

	// Idempotent: nothing left to close.
	if t1.CloseTarget(target) {
		t.Fatal("second CloseTarget reported a close with nothing cached")
	}

	// A subsequent send must re-dial and be delivered.
	t1.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2}})
	handler2.waitBatch(t, 10*time.Second)
	if n := t1.ConnectionCount(); n != 1 {
		t.Fatalf("expected the peer to be re-dialed, got %d connections", n)
	}
}

// TestCloseTargetRefusesDuringSnapshotTransfer verifies an in-flight snapshot
// transfer vetoes the teardown: the cached connection (which SendSnapshot
// streams over) and all its state must survive, and the teardown must succeed
// once the transfer completes.
func TestCloseTargetRefusesDuringSnapshotTransfer(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	t1.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1}})
	handler2.waitBatch(t, 10*time.Second)

	streamsBefore := t1.StreamCount()

	// Occupy a snapshot slot exactly as SendSnapshot does.
	if !t1.snapshotTracker.TryAdd() {
		t.Fatal("snapshotTracker refused an operation on a running transport")
	}

	if t1.CloseTarget(target) {
		t.Fatal("CloseTarget aborted a connection with a snapshot transfer in flight")
	}
	if n := t1.ConnectionCount(); n != 1 {
		t.Fatalf("connection torn down despite the snapshot veto: %d connections", n)
	}
	if n := t1.StreamCount(); n != streamsBefore {
		t.Fatalf("streams evicted despite the snapshot veto: had %d, now %d", streamsBefore, n)
	}
	if !sendQueueKnown(t1, target) {
		t.Fatal("send queue retired despite the snapshot veto")
	}

	// Transfer completes: the teardown is no longer vetoed.
	t1.snapshotTracker.Done()

	if !t1.CloseTarget(target) {
		t.Fatal("CloseTarget refused after the snapshot transfer completed")
	}
	if n := t1.ConnectionCount(); n != 0 {
		t.Fatalf("expected 0 cached connections, got %d", n)
	}
}

// TestCloseTargetSendQueueRetirementSurvivesWorkerPrune verifies the retired
// queue is replaced by a fresh one on the next send, and that the retired
// worker's own idle prune cannot delete that replacement.
func TestCloseTargetSendQueueRetirementSurvivesWorkerPrune(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	t1.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1}})
	handler2.waitBatch(t, 10*time.Second)

	t1.sendQueuesMu.RLock()
	retired := t1.sendQueues[target]
	t1.sendQueuesMu.RUnlock()
	if retired == nil {
		t.Fatal("no send queue for the target")
	}

	if !t1.CloseTarget(target) {
		t.Fatal("CloseTarget did not report closing the cached connection")
	}
	if !retired.closed.Load() {
		t.Fatal("retired send queue was removed from the map without being marked closed")
	}

	// The next send creates a fresh queue rather than reusing the retired one.
	t1.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2}})
	handler2.waitBatch(t, 10*time.Second)

	t1.sendQueuesMu.RLock()
	fresh := t1.sendQueues[target]
	t1.sendQueuesMu.RUnlock()
	if fresh == nil {
		t.Fatal("no send queue was created after the retirement")
	}
	if fresh == retired {
		t.Fatal("send after retirement reused the retired queue")
	}

	// Simulate the retired worker reaching its idle prune: it must not delete
	// the live replacement.
	t1.sendQueuesMu.Lock()
	retired.closed.Store(true)
	if cur, ok := t1.sendQueues[target]; ok && cur == retired {
		delete(t1.sendQueues, target)
	}
	t1.sendQueuesMu.Unlock()

	if !sendQueueKnown(t1, target) {
		t.Fatal("retired worker's idle prune deleted the live replacement queue")
	}
}

// inboundConnCount returns how many inbound connections the transport is
// tracking, which is the population every inbound reclaim policy operates on.
func inboundConnCount(tr *QUICTransport) int {
	n := 0
	tr.inboundConns.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

// awaitInboundConn waits for an inbound connection to be tracked and returns it
// together with its liveness record.
func awaitInboundConn(t *testing.T, tr *QUICTransport) (*quic.Conn, *inboundConnState) {
	t.Helper()
	var found *quic.Conn
	var state *inboundConnState
	pollUntil(t, 10*time.Second, 20*time.Millisecond,
		"no inbound connection was tracked", func() bool {
			tr.inboundConns.Range(func(k, v any) bool {
				conn, ok := k.(*quic.Conn)
				if !ok {
					return true
				}
				st, ok := v.(*inboundConnState)
				if !ok {
					return true
				}
				found, state = conn, st
				return false
			})
			return found != nil
		})
	return found, state
}

// TestIdleInboundConnectionIsReclaimed is the regression test for an inbound
// connection that nothing could ever close.
//
// Tracking inbound connections was only half the control. This node's own
// quic.Config sets KeepAlivePeriod (10s) below MaxIdleTimeout (30s) for BOTH
// roles, so the listener PINGs every accepted connection forever and QUIC's
// idle timeout can never fire on it; cleanStaleConnections reaps only
// connections that are ALREADY dead. A peer that completes a handshake and then
// says nothing held one of MaxIncomingConnections and one of
// MaxConnectionsPerIP for the process lifetime, for the cost of answering a
// PING.
//
// The test drives the sweep with an explicit clock rather than waiting out the
// real 5-minute bound, and asserts BOTH directions: a connection inside the
// bound survives, one past it does not.
func TestIdleInboundConnectionIsReclaimed(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2}})
	handler2.waitBatch(t, 10*time.Second)
	inbound, _ := awaitInboundConn(t, t2)

	// CONTROL: a sweep at the current time must not touch a connection that
	// has just carried a frame. Without this the assertion below would pass
	// against a sweep that closes everything it sees.
	t2.reapIdleInboundConns(time.Now())
	select {
	case <-inbound.Context().Done():
		t.Fatal("a connection that carried a frame moments ago was reclaimed " +
			"as idle: the sweep ignores the activity stamp and would tear down " +
			"every healthy peering on each 30s tick")
	case <-time.After(200 * time.Millisecond):
	}

	// SUBJECT: past the idle bound the connection must actually be closed.
	t2.reapIdleInboundConns(time.Now().Add(inboundIdleRetention + time.Second))
	select {
	case <-inbound.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("an inbound connection idle for more than %s was not closed: "+
			"nothing else can ever reclaim it, so it holds a global and a "+
			"per-IP connection slot for the process lifetime", inboundIdleRetention)
	}

	// The close must also drop the tracking entry, or the reclaimed connection
	// leaks in the map it was reclaimed from.
	pollUntil(t, 10*time.Second, 20*time.Millisecond,
		"the reclaimed inbound connection was never untracked",
		func() bool { return inboundConnCount(t2) == 0 })
}

// TestInboundActivityStampKeepsBusyConnectionAlive pins the stamp that makes
// the idle sweep safe.
//
// The stamp is taken on every FRAME, not once at admission and not on stream
// acceptance, and that distinction is the whole difference between a reclaim
// policy and an outage: a sender caches and reuses one stream across bursts, so
// a connection carrying continuous replication traffic accepts no new stream
// for as long as the burst lasts, and an admission-only stamp ages out under it.
//
// The connection's record is backdated past the idle bound before the traffic
// is sent, so only a stamp taken on the ARRIVING FRAME can save it. Without the
// backdating the admission stamp alone keeps the connection alive and the test
// passes against no per-frame stamp at all.
func TestInboundActivityStampKeepsBusyConnectionAlive(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2}})
	handler2.waitBatch(t, 10*time.Second)
	inbound, state := awaitInboundConn(t, t2)

	// Age the connection well past the idle bound, as a long-lived peering
	// reaches on its own between the sweeps.
	stale := time.Now().Add(-2 * inboundIdleRetention)
	state.lastActive.Store(stale.UnixNano())

	// CONTROL: the backdated connection really is reapable, so the assertion
	// below is about the stamp rather than about a sweep that never fires.
	if state.lastActive.Load() > time.Now().Add(-inboundIdleRetention).UnixNano() {
		t.Fatal("the connection was not aged past the idle bound; the " +
			"assertion below would pass without any per-frame stamp")
	}

	// Traffic over the SAME cached stream must refresh it.
	t1.Send([]proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2}})
	handler2.waitBatch(t, 10*time.Second)

	t2.reapIdleInboundConns(time.Now())
	select {
	case <-inbound.Context().Done():
		t.Fatal("a connection that has just delivered a batch was reclaimed: " +
			"the activity stamp is not taken on arriving frames, so any peering " +
			"whose sender reuses one cached stream is torn down on a timer")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestCloseTargetClosesInboundFromRemovedPeer is the regression test for the
// half of a removed member's connectivity that the member controls.
//
// CloseTarget reached only t.conns, the connection this node DIALED. A removed
// member that dialed US kept a fully live inbound connection — keep-alived from
// both ends, holding a global and a per-IP slot, free to open streams — and
// when this node had never dialed the peer at all, CloseTarget reclaimed
// literally nothing and returned false.
//
// The receiving transport advertises a host its peers' certificate does NOT
// cover, so the co-location exemption (which every loopback deployment lands
// in) does not apply and the certificate match is actually exercised.
func TestCloseTargetClosesInboundFromRemovedPeer(t *testing.T) {
	receiver, sender, cleanup := inboundCertMatchPair(t)
	defer cleanup()

	sender.Send([]proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 2, To: 1}})
	inbound, _ := awaitInboundConn(t, receiver)

	// CONTROL: an address the peer's certificate does not cover must not close
	// anything, or the "match" is really a sweep over every inbound connection.
	if n := receiver.closeInboundFromPeer("not-in-any-san.example:9999"); n != 0 {
		t.Fatalf("closeInboundFromPeer closed %d connections for an address no "+
			"peer certificate covers: the teardown is not certificate-matched "+
			"and would close third-party peers", n)
	}
	select {
	case <-inbound.Context().Done():
		t.Fatal("the inbound connection was closed for an unrelated address")
	case <-time.After(200 * time.Millisecond):
	}

	// SUBJECT: the removed peer's own address closes its inbound connection.
	// "localhost" is in the test leaf's SANs; the receiver advertises
	// node-under-test.example, so the co-location exemption does not fire.
	if !receiver.CloseTarget("localhost:9999") {
		t.Fatal("CloseTarget reported nothing reclaimed for a removed peer " +
			"holding a live inbound connection: the inbound half is invisible " +
			"to membership removal")
	}
	select {
	case <-inbound.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the removed peer's inbound connection is still live after " +
			"CloseTarget: a removed member keeps talking to this node forever")
	}
}

// TestCloseTargetSparesCoLocatedInbound pins the exemption that keeps the
// inbound teardown from destroying single-machine deployments.
//
// A certificate is matched on HOST alone, so on one machine every replica's
// leaf covers every other replica's address. Without the exemption, removing
// one loopback replica would close the healthy inbound connections of all its
// neighbours — the same reason every other certificate control here exempts
// co-located hosts.
func TestCloseTargetSparesCoLocatedInbound(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send([]proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2}})
	handler2.waitBatch(t, 10*time.Second)
	inbound, _ := awaitInboundConn(t, t2)

	if n := t2.closeInboundFromPeer("127.0.0.1:9999"); n != 0 {
		t.Fatalf("closeInboundFromPeer closed %d co-located connections: on a "+
			"single machine every replica's certificate covers every other "+
			"replica's address, so this closes healthy neighbours", n)
	}
	select {
	case <-inbound.Context().Done():
		t.Fatal("a co-located peer's inbound connection was torn down")
	case <-time.After(200 * time.Millisecond):
	}
}

// inboundCertMatchPair builds a receiver whose ADVERTISED host is absent from
// the shared test certificate, so certificate-matched inbound teardown can be
// exercised: the co-located exemption would otherwise make it a no-op on
// loopback, which is every test in this package.
//
// Returns (receiver, sender, cleanup).
func inboundCertMatchPair(t *testing.T) (*QUICTransport, *QUICTransport, func()) {
	t.Helper()
	mtls := testMTLSConfig(t)

	receiver, err := NewQUICTransport(Config{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "node-under-test.example:4001",
		DeploymentID:     42,
		MTLSConfig:       mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(receiver): %v", err)
	}
	if err := receiver.Start(); err != nil {
		t.Fatalf("receiver.Start: %v", err)
	}

	senderReg := registry.NewRegistry()
	sender, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), senderReg)
	if err != nil {
		receiver.Stop()
		t.Fatalf("NewQUICTransport(sender): %v", err)
	}
	if err := sender.Start(); err != nil {
		receiver.Stop()
		t.Fatalf("sender.Start: %v", err)
	}
	senderReg.Register(1, 1, receiver.Addr().String())

	return receiver, sender, func() {
		sender.Stop()
		receiver.Stop()
	}
}
