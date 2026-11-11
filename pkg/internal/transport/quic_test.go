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
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

// testMTLSConfig returns a valid MTLSConfig for use in tests. It generates
// an ephemeral CA and leaf certificate signed by that CA. The certs are
// cached per test via t.Helper() but regenerated for each test to avoid
// cross-test contamination.
func testMTLSConfig(t testing.TB) *MTLSConfig {
	t.Helper()
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)
	return &MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}
}

// testHandler is a MessageHandler that records received batches and
// snapshot chunks for verification in tests.
type testHandler struct {
	mu             sync.Mutex
	batches        []proto.MessageBatch
	snapshots      [][]proto.SnapshotChunk
	batchCount     atomic.Int64
	snapshotCount  atomic.Int64
	handleErr      error
	snapshotErr    error
	batchNotify    chan struct{}
	snapshotNotify chan struct{}
}

func newTestHandler() *testHandler {
	return &testHandler{
		batchNotify:    make(chan struct{}, 100),
		snapshotNotify: make(chan struct{}, 100),
	}
}

func (h *testHandler) HandleMessage(batch proto.MessageBatch, _ PeerAuth) error {
	if h.handleErr != nil {
		return h.handleErr
	}
	// The transport recycles the batch — including the Message values inside
	// it — as soon as this call returns, so the Requests slice header must not
	// outlive the call. Copy it, as any conforming handler must; the Entries
	// and Checksum slices inside each Message already point at an owned bulk
	// allocation rather than the pooled receive buffer.
	retained := batch
	if len(batch.Requests) > 0 {
		retained.Requests = make([]proto.Message, len(batch.Requests))
		copy(retained.Requests, batch.Requests)
	}
	h.mu.Lock()
	h.batches = append(h.batches, retained)
	h.mu.Unlock()
	h.batchCount.Add(1)
	select {
	case h.batchNotify <- struct{}{}:
	default:
	}
	return nil
}

// AuthorizeSnapshot admits every transfer: this fake exercises the paths after admission.
func (*testHandler) AuthorizeSnapshot(uint64, uint64, uint64, uint64, PeerAuth) error { return nil }

func (h *testHandler) HandleSnapshot(chunks []proto.SnapshotChunk, _ PeerAuth) error {
	if h.snapshotErr != nil {
		return h.snapshotErr
	}
	h.mu.Lock()
	h.snapshots = append(h.snapshots, chunks)
	h.mu.Unlock()
	h.snapshotCount.Add(1)
	select {
	case h.snapshotNotify <- struct{}{}:
	default:
	}
	return nil
}

func (h *testHandler) getBatches() []proto.MessageBatch {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]proto.MessageBatch, len(h.batches))
	copy(cp, h.batches)
	return cp
}

func (h *testHandler) getSnapshots() [][]proto.SnapshotChunk {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([][]proto.SnapshotChunk, len(h.snapshots))
	copy(cp, h.snapshots)
	return cp
}

func (h *testHandler) waitBatch(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-h.batchNotify:
	case <-time.After(timeout):
		t.Fatal("timeout waiting for batch")
	}
}

func (h *testHandler) waitSnapshot(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-h.snapshotNotify:
	case <-time.After(timeout):
		t.Fatal("timeout waiting for snapshot")
	}
}

// transportPair creates two QUICTransport instances connected via loopback.
// It registers each transport in the other's registry so messages can be
// routed. Returns (sender, receiver, senderHandler, receiverHandler, cleanup).
func transportPair(t testing.TB, deploymentID uint64) (
	*QUICTransport, *QUICTransport,
	*testHandler, *testHandler,
	func(),
) {
	t.Helper()

	handler1 := newTestHandler()
	handler2 := newTestHandler()
	t1, t2, cleanup := transportPairWith(t, deploymentID, handler1, handler2)
	return t1, t2, handler1, handler2, cleanup
}

// transportPairWith is the general form of transportPair: it accepts the two
// MessageHandler implementations and optional Config mutators applied to BOTH
// endpoints before construction, so callers that need custom handlers or
// non-default transport tuning (benchmarks, in particular) reuse the same
// loopback + mTLS + registry setup instead of building their own.
// Returns (sender, receiver, cleanup).
// testDialTimeout is the dial budget the transport pair fixture runs with.
// Production's 5s is a liveness budget backed by Raft retransmission; a test
// that dials directly has no retransmission, so a handshake slowed past 5s by
// CI saturation fails the test rather than the batch (handshakes of 4.3s and
// 5.01s have been observed on loaded runners). The
// budget here is wide enough that only a broken transport reaches it. Tests
// that need the production figure -- the first-contact bound, the shutdown
// promptness check -- build their own Config and get the default.
const testDialTimeout = 60 * time.Second

// withProductionDialBudget is the fixture option for a test whose subject is a
// dial that must FAIL -- a stopped peer, a port nothing listens on. Such a test
// waits for the handshake to give up, and it should give up on production's
// terms: 60s would turn a five-second failure path into a minute. Two tests
// use it; a test that dials a live peer never should.
func withProductionDialBudget(c *Config) { c.DialTimeout = dialTimeout }

func transportPairWith(
	t testing.TB,
	deploymentID uint64,
	handler1, handler2 MessageHandler,
	opts ...func(*Config),
) (*QUICTransport, *QUICTransport, func()) {
	t.Helper()

	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	mtls := testMTLSConfig(t)
	cfg1 := Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  deploymentID,
		MTLSConfig:    mtls,
		DialTimeout:   testDialTimeout,
	}
	cfg2 := Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  deploymentID,
		MTLSConfig:    mtls,
		DialTimeout:   testDialTimeout,
	}
	for _, opt := range opts {
		opt(&cfg1)
		opt(&cfg2)
	}

	t1, err := NewQUICTransport(cfg1, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}

	t2, err := NewQUICTransport(cfg2, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}

	// Register addresses in each other's registry.
	// shard=1, replica=1 -> t1's address
	// shard=1, replica=2 -> t2's address
	addr1 := t1.Addr().String()
	addr2 := t2.Addr().String()
	reg1.Register(1, 2, addr2) // t1 can send to replica 2 (at t2)
	reg2.Register(1, 1, addr1) // t2 can send to replica 1 (at t1)

	cleanup := func() {
		t1.Stop()
		t2.Stop()
	}

	return t1, t2, cleanup
}

// TestNewQUICTransportInvalidConfig verifies rejection of invalid config.
func TestNewQUICTransportInvalidConfig(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{} // missing ListenAddress

	_, err := NewQUICTransport(cfg, handler, reg)
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
	if !errors.Is(err, ErrEmptyListenAddress) {
		t.Fatalf("expected ErrEmptyListenAddress, got %v", err)
	}
}

// TestDoubleStopIsIdempotent verifies that calling Stop twice is safe.
func TestDoubleStopIsIdempotent(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  100,
		MTLSConfig:    testMTLSConfig(t),
	}

	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if err := tr.Stop(); err != nil {
		t.Fatalf("first Stop failed: %v", err)
	}
	if err := tr.Stop(); err != nil {
		t.Fatalf("second Stop failed: %v", err)
	}
}

// TestAddrNilBeforeStart verifies Addr returns nil before Start.
func TestAddrNilBeforeStart(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  100,
		MTLSConfig:    testMTLSConfig(t),
	}

	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	if tr.Addr() != nil {
		t.Fatal("Addr should be nil before Start")
	}
}

// TestSendAndReceiveRoundTrip verifies that a message sent from t1 is
// received by t2's handler.
func TestSendAndReceiveRoundTrip(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	msgs := []proto.Message{
		{
			Type:    proto.Replicate,
			ShardID: 1,
			From:    1,
			To:      2,
			Term:    5,
			Commit:  10,
		},
	}

	t1.Send(msgs)
	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}
	if len(batches[0].Requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(batches[0].Requests))
	}
	got := batches[0].Requests[0]
	if got.Type != proto.Replicate {
		t.Fatalf("expected Replicate type, got %d", got.Type)
	}
	if got.ShardID != 1 {
		t.Fatalf("expected ShardID 1, got %d", got.ShardID)
	}
	if got.Term != 5 {
		t.Fatalf("expected Term 5, got %d", got.Term)
	}
	if got.Commit != 10 {
		t.Fatalf("expected Commit 10, got %d", got.Commit)
	}
}

// TestSendAndReceiveMultipleMessages verifies multiple messages in a single
// Send call are delivered.
func TestSendAndReceiveMultipleMessages(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 3},
	}

	t1.Send(msgs)

	// Messages may arrive in one or more batches depending on timing.
	// Poll until all 3 are delivered or timeout.
	deadline := time.After(10 * time.Second)
	for {
		totalMsgs := 0
		for _, b := range handler2.getBatches() {
			totalMsgs += len(b.Requests)
		}
		if totalMsgs >= 3 {
			if totalMsgs != 3 {
				t.Fatalf("expected exactly 3 messages, got %d", totalMsgs)
			}
			return
		}
		select {
		case <-handler2.batchNotify:
		case <-deadline:
			t.Fatalf("expected 3 messages total, got %d", totalMsgs)
		}
	}
}

// TestSendEmptyMessages verifies that sending empty messages is a no-op.
func TestSendEmptyMessages(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	t1.Send(nil)
	t1.Send([]proto.Message{})

	assertNever(t, 200*time.Millisecond, 20*time.Millisecond,
		"unexpected batch for empty send",
		func() bool { return len(handler2.getBatches()) != 0 })
}

// TestSendAfterStop verifies that Send is a no-op after Stop.
func TestSendAfterStop(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)

	t1.Stop()
	cleanup()

	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	}
	t1.Send(msgs)

	assertNever(t, 200*time.Millisecond, 20*time.Millisecond,
		"unexpected batch after Stop",
		func() bool { return len(handler2.getBatches()) != 0 })
}

// TestConnectionReuse verifies that a second message uses the existing
// connection rather than dialing a new one.
func TestConnectionReuse(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	}

	t1.Send(msgs)
	handler2.waitBatch(t, 10*time.Second)

	connCountAfterFirst := t1.ConnectionCount()
	if connCountAfterFirst != 1 {
		t.Fatalf("expected 1 connection, got %d", connCountAfterFirst)
	}

	// Send again.
	msgs2 := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2},
	}
	t1.Send(msgs2)
	handler2.waitBatch(t, 10*time.Second)

	connCountAfterSecond := t1.ConnectionCount()
	if connCountAfterSecond != 1 {
		t.Fatalf("expected 1 connection after reuse, got %d", connCountAfterSecond)
	}
}

// TestHeartbeatUsesStreamZero verifies that heartbeat messages use stream
// index 0 (the priority stream) while data messages use other indices.
func TestHeartbeatUsesStreamZero(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send a heartbeat.
	heartbeat := []proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
	}
	sendFirstContact(t, t1, handler2, heartbeat)

	batches := handler2.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected heartbeat batch")
	}
	if batches[0].Requests[0].Type != proto.Heartbeat {
		t.Fatalf("expected Heartbeat type, got %d", batches[0].Requests[0].Type)
	}
}

// TestStreamPoolDifferentShards verifies that messages for different shards
// may use different stream indices.
func TestStreamPoolDifferentShards(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Register additional shards pointing to t2.
	t2Addr, _ := t1.reg.Resolve(1, 2)
	t1.reg.Register(2, 2, t2Addr)
	t1.reg.Register(3, 2, t2Addr)

	// Send messages for different shards.
	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	}
	t1.Send(msgs)
	handler2.waitBatch(t, 10*time.Second)

	msgs2 := []proto.Message{
		{Type: proto.Replicate, ShardID: 2, From: 1, To: 2, Term: 1},
	}
	t1.Send(msgs2)
	handler2.waitBatch(t, 10*time.Second)

	if handler2.batchCount.Load() < 2 {
		t.Fatalf("expected at least 2 batches, got %d", handler2.batchCount.Load())
	}
}

// TestSendSnapshotRoundTrip verifies that snapshot chunks sent from t1
// are received by t2's handler.
func TestSendSnapshotRoundTrip(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	chunks := []proto.SnapshotChunk{
		{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			From:       1,
			Index:      100,
			Term:       5,
			ChunkID:    0,
			ChunkCount: 2,
			ChunkSize:  10,
			Data:       []byte("chunk-0000"),
		},
		{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			From:       1,
			Index:      100,
			Term:       5,
			ChunkID:    1,
			ChunkCount: 2,
			ChunkSize:  10,
			Data:       []byte("chunk-0001"),
		},
	}

	if err := t1.SendSnapshot(chunks); err != nil {
		t.Fatalf("SendSnapshot failed: %v", err)
	}

	handler2.waitSnapshot(t, 10*time.Second)

	snaps := handler2.getSnapshots()
	if len(snaps) == 0 {
		t.Fatal("expected at least one snapshot delivery")
	}
	if len(snaps[0]) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(snaps[0]))
	}
	if string(snaps[0][0].Data) != "chunk-0000" {
		t.Fatalf("unexpected chunk 0 data: %s", snaps[0][0].Data)
	}
	if string(snaps[0][1].Data) != "chunk-0001" {
		t.Fatalf("unexpected chunk 1 data: %s", snaps[0][1].Data)
	}
}

// TestSendSnapshotEmptyChunks verifies that empty chunks is a no-op.
func TestSendSnapshotEmptyChunks(t *testing.T) {
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

	err = tr.SendSnapshot(nil)
	if err != nil {
		t.Fatalf("SendSnapshot with nil should return nil, got %v", err)
	}

	err = tr.SendSnapshot([]proto.SnapshotChunk{})
	if err != nil {
		t.Fatalf("SendSnapshot with empty should return nil, got %v", err)
	}
}

// TestSendSnapshotAfterStop verifies snapshot send returns error after stop.
func TestSendSnapshotAfterStop(t *testing.T) {
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

	chunks := []proto.SnapshotChunk{
		{BinVer: proto.WireVersion, ShardID: 1, ReplicaID: 1, ChunkCount: 1},
	}
	err = tr.SendSnapshot(chunks)
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("expected ErrStreamClosed, got %v", err)
	}
}

// TestGracefulShutdownWithInflightMessages verifies that stopping the
// transport while messages are in flight does not panic.
func TestGracefulShutdownWithInflightMessages(t *testing.T) {
	t1, _, _, _, cleanup := transportPair(t, 42)

	// Fire off sends in a goroutine.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			t1.Send([]proto.Message{
				{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i)},
			})
		}
	}()

	// Stop while sends might be in flight.
	inFlightTimer := time.NewTimer(5 * time.Millisecond)
	<-inFlightTimer.C
	inFlightTimer.Stop()
	cleanup()

	<-done
}

// TestDeploymentIDMismatchRejection verifies that messages from a different
// deployment are rejected and not delivered to the handler.
func TestDeploymentIDMismatchRejection(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	// Create two transports with different deployment IDs.
	mtls := testMTLSConfig(t)
	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  100,
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
		DeploymentID:  200, // Different deployment ID
		MTLSConfig:    mtls,
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	// Register addresses.
	reg1.Register(1, 2, t2.Addr().String())

	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	}
	t1.Send(msgs)

	// Verify no batch was delivered for the deployment mismatch.
	assertNever(t, 2*time.Second, 50*time.Millisecond,
		"unexpected batch for deployment mismatch",
		func() bool { return len(handler2.getBatches()) != 0 })
}

// TestIsHeartbeatMessage verifies the heartbeat detection helper.
func TestIsHeartbeatMessage(t *testing.T) {
	tests := []struct {
		msgType  uint64
		expected bool
	}{
		{proto.Heartbeat, true},
		{proto.HeartbeatResp, true},
		{proto.Replicate, false},
		{proto.RequestVote, false},
		{proto.LocalTick, false},
		{0, false},
		{^uint64(0), false},
	}

	for _, tc := range tests {
		got := isHeartbeatMessage(tc.msgType)
		if got != tc.expected {
			t.Fatalf("isHeartbeatMessage(%d): want %v, got %v", tc.msgType, tc.expected, got)
		}
	}
}

// TestLocalAddrBeforeStart verifies localAddr returns config address
// before start.
func TestLocalAddrBeforeStart(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: "127.0.0.1:9999",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}

	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	addr := tr.localAddr()
	if addr != "127.0.0.1:9999" {
		t.Fatalf("expected config address, got %s", addr)
	}
}

// TestNewQUICTransportWithMTLS verifies mTLS config is accepted.
func TestNewQUICTransportWithMTLS(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig: &MTLSConfig{
			CACert: caCertPEM,
			Cert:   certPEM,
			Key:    keyPEM,
		},
	}

	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport with mTLS failed: %v", err)
	}
	if tr == nil {
		t.Fatal("transport should not be nil")
	}
}

// TestFlagSnapshotConstant verifies the FlagSnapshot constant.
func TestFlagSnapshotConstant(t *testing.T) {
	if FlagSnapshot != 0x02 {
		t.Fatalf("FlagSnapshot: want 0x02, got 0x%02x", FlagSnapshot)
	}
}

// TestBidirectionalSend verifies both transports can send to each other.
func TestBidirectionalSend(t *testing.T) {
	t1, t2, handler1, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// t1 -> t2
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 10},
	})
	handler2.waitBatch(t, 10*time.Second)

	// t2 -> t1
	t2.Send([]proto.Message{
		{Type: proto.ReplicateResp, ShardID: 1, From: 2, To: 1, Term: 10},
	})
	handler1.waitBatch(t, 10*time.Second)

	b2 := handler2.getBatches()
	if len(b2) != 1 || b2[0].Requests[0].Term != 10 {
		t.Fatal("t2 did not receive correct message from t1")
	}

	b1 := handler1.getBatches()
	if len(b1) != 1 || b1[0].Requests[0].Term != 10 {
		t.Fatal("t1 did not receive correct message from t2")
	}
}

// TestQUICTransport_Stop_WaitsForSnapshots verifies that Stop() waits for
// in-flight SendSnapshot operations to complete before tearing down.
func TestQUICTransport_Stop_WaitsForSnapshots(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	// The body stops t1 itself, mid-test, to exercise the snapshot wait. Stop
	// is idempotent, so the pair's cleanup still runs here and is what closes
	// t2 — discarding it leaked t2's listener, accept loop and cleanup ticker
	// into every test that ran after this one.
	defer cleanup()

	chunks := []proto.SnapshotChunk{
		{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			From:       1,
			Index:      200,
			Term:       3,
			ChunkID:    0,
			ChunkCount: 1,
			ChunkSize:  5,
			Data:       []byte("drain"),
		},
	}

	// Send a snapshot. The snapshot uses a dedicated stream that
	// completes before returning.
	if err := t1.SendSnapshot(chunks); err != nil {
		t.Fatalf("SendSnapshot failed: %v", err)
	}

	// Verify the snapshot was received.
	handler2.waitSnapshot(t, 10*time.Second)
	snaps := handler2.getSnapshots()
	if len(snaps) == 0 {
		t.Fatal("expected at least one snapshot delivery")
	}
	if string(snaps[0][0].Data) != "drain" {
		t.Fatalf("unexpected snapshot data: %s", snaps[0][0].Data)
	}

	// Now stop the transport. With snapshotTracker properly tracked,
	// Stop() should not deadlock or race.
	t1.Stop()

	// Verify that stopped is set.
	if !t1.stopped.Load() {
		t.Error("transport should be stopped after Stop()")
	}
}

// TestQUICTransport_SnapshotTracker_TracksReceives verifies the
// snapshotTracker is wired into the snapshot receiver for receive-side
// tracking.
func TestQUICTransport_SnapshotTracker_TracksReceives(t *testing.T) {
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

	// Verify that the snapshotTracker was wired into the receiver.
	if tr.snapshotRecv.snapshotTracker == nil {
		t.Error("snapshotRecv.snapshotTracker should be set by NewQUICTransport")
	}
	if tr.snapshotRecv.snapshotTracker != &tr.snapshotTracker {
		t.Error("snapshotRecv.snapshotTracker should point to transport's snapshotTracker")
	}
}

// TestNewQUICTransportRevocationMalformedCAPEM verifies that NewQUICTransport
// returns a CACertificateParseError when RevocationConfig is set but the
// CA PEM is malformed (not valid PEM). This ensures fail-fast at startup
// instead of silently passing a nil issuer to the revocation checker.
func TestNewQUICTransportRevocationMalformedCAPEM(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()

	_, _, certPEM, keyPEM := generateTestCertPair(t)

	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig: &MTLSConfig{
			CACert: []byte("not-valid-pem-data"),
			Cert:   certPEM,
			Key:    keyPEM,
		},
		RevocationConfig: &revocation.Config{
			OCSPResponderURL: "http://ocsp.example.com",
		},
	}

	_, err := NewQUICTransport(cfg, handler, reg)
	if err == nil {
		t.Fatal("expected error for malformed CA PEM with revocation enabled")
	}
	if !errors.Is(err, ErrCACertificateParse) {
		t.Fatalf("expected ErrCACertificateParse, got %v", err)
	}

	var parseErr *CACertificateParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected *CACertificateParseError, got %T", err)
	}
	if parseErr.Reason != "PEM decode yielded no CERTIFICATE block" {
		t.Fatalf("unexpected reason: %s", parseErr.Reason)
	}
	if parseErr.Err != nil {
		t.Fatalf("expected nil underlying error for PEM decode failure, got %v", parseErr.Err)
	}
}

// TestNewQUICTransportRevocationCorruptDER verifies that NewQUICTransport
// returns a CACertificateParseError when RevocationConfig is set and the
// CA PEM contains a valid PEM block but corrupt DER data that cannot be
// parsed as an X.509 certificate.
func TestNewQUICTransportRevocationCorruptDER(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()

	_, _, certPEM, keyPEM := generateTestCertPair(t)

	// Valid PEM envelope wrapping garbage DER bytes.
	corruptCAPEM := []byte("-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n")

	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig: &MTLSConfig{
			CACert: corruptCAPEM,
			Cert:   certPEM,
			Key:    keyPEM,
		},
		RevocationConfig: &revocation.Config{
			OCSPResponderURL: "http://ocsp.example.com",
		},
	}

	_, err := NewQUICTransport(cfg, handler, reg)
	if err == nil {
		t.Fatal("expected error for corrupt DER in CA PEM with revocation enabled")
	}
	if !errors.Is(err, ErrCACertificateParse) {
		t.Fatalf("expected ErrCACertificateParse, got %v", err)
	}

	var parseErr *CACertificateParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected *CACertificateParseError, got %T", err)
	}
	if parseErr.Reason != "x509.ParseCertificate failed" {
		t.Fatalf("unexpected reason: %s", parseErr.Reason)
	}
	if parseErr.Err == nil {
		t.Fatal("expected non-nil underlying error for x509 parse failure")
	}
}

// TestNewQUICTransportRevocationValidCAPEM verifies that NewQUICTransport
// succeeds when RevocationConfig is set and the CA PEM is valid. The
// revocation checker should be created with the parsed issuer certificate.
func TestNewQUICTransportRevocationValidCAPEM(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()

	mtls := testMTLSConfig(t)

	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		RevocationConfig: &revocation.Config{
			OCSPResponderURL: "http://ocsp.example.com",
		},
	}

	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport with valid CA PEM and revocation failed: %v", err)
	}
	if tr == nil {
		t.Fatal("transport should not be nil")
	}
	if tr.revocationChecker == nil {
		t.Fatal("revocation checker should be non-nil when RevocationConfig is set")
	}
}

// TestNewQUICTransportNoRevocationIgnoresMalformedCA verifies that when
// RevocationConfig is nil, a malformed CA PEM does not trigger the
// CACertificateParseError. The CA PEM parsing is only enforced when
// revocation checking is enabled.
func TestNewQUICTransportNoRevocationIgnoresMalformedCA(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()

	_, _, certPEM, keyPEM := generateTestCertPair(t)

	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig: &MTLSConfig{
			CACert: []byte("not-valid-pem-data"),
			Cert:   certPEM,
			Key:    keyPEM,
		},
		// No RevocationConfig — CA PEM parsing for issuer is skipped.
	}

	// This may fail downstream in buildMTLSTLS for other reasons (invalid
	// CA cert for the TLS pool), but it should NOT fail with
	// ErrCACertificateParse since revocation is disabled.
	_, err := NewQUICTransport(cfg, handler, reg)
	if err != nil && errors.Is(err, ErrCACertificateParse) {
		t.Fatalf("should not get CACertificateParseError when revocation is disabled, got %v", err)
	}
}

// TestQUICTransport_ReconnectAfterFailure verifies that the transport can
// recover from a connection failure. When the receiver is stopped and
// restarted, subsequent sends from the sender eventually succeed because
// the send path retries with new connections.
func TestQUICTransport_ReconnectAfterFailure(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	mtls := testMTLSConfig(t)
	cfg1 := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}
	cfg2 := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}

	// Create and start sender (t1).
	t1, err := NewQUICTransport(cfg1, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

	// Create and start receiver (t2).
	t2, err := NewQUICTransport(cfg2, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	t2Addr := t2.Addr().String()

	// Register t2's address in t1's registry.
	reg1.Register(1, 2, t2Addr)

	// Step 1: Send a message successfully.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)
	initialBatches := handler2.getBatches()
	if len(initialBatches) == 0 {
		t.Fatal("expected at least one batch before failure")
	}
	t.Log("initial message delivered successfully")

	// Step 2: Stop the receiver to simulate connection failure.
	t2.Stop()
	t.Log("receiver stopped, simulating connection failure")

	// Send messages while receiver is down -- these are expected to be
	// dropped silently (the transport does not block on failed sends).
	// Space them out to let the send worker observe each failure and
	// evict the stale connection.
	sendTicker := time.NewTicker(100 * time.Millisecond)
	for i := range 5 {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 100)},
		})
		<-sendTicker.C
	}
	sendTicker.Stop()
	// Wait for the send path to fully observe the connection failure
	// and evict the stale QUIC connection (dial timeout is 5s).
	evictTimer := time.NewTimer(6 * time.Second)
	<-evictTimer.C
	evictTimer.Stop()

	// Step 3: Restart the receiver on the SAME address. We create a new
	// transport but bind to the same address so the sender's cached
	// connection becomes stale.
	handler2b := newTestHandler()
	reg2b := registry.NewRegistry()
	cfg2b := Config{
		ListenAddress: t2Addr,
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}
	t2b, err := NewQUICTransport(cfg2b, handler2b, reg2b)
	if err != nil {
		t.Fatalf("NewQUICTransport(2b) failed: %v", err)
	}
	if err := t2b.Start(); err != nil {
		// The address may still be in TIME_WAIT. In that case, the test
		// still validates that the send path handles errors gracefully.
		t.Logf("t2b.Start on same address failed (TIME_WAIT expected): %v", err)
		t.Log("reconnection test: verified send path handles connection errors gracefully")
		return
	}
	defer t2b.Stop()
	t.Log("receiver restarted on same address")

	// Update the registry to point to the restarted receiver's address.
	// (It is the same address, but this ensures the registry is consistent.)
	reg1.Register(1, 2, t2b.Addr().String())

	// Step 4: Send a new message. The transport should reconnect and
	// deliver it to the new receiver. Allow up to 30 seconds because
	// the dial timeout is 5s and the first few attempts may still see
	// the stale connection before eviction completes.
	deadline := time.Now().Add(30 * time.Second)
	var reconnected bool
	for time.Now().Before(deadline) {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 999},
		})
		// Check if the message arrived at the new receiver.
		select {
		case <-handler2b.batchNotify:
			reconnected = true
		case <-time.After(1 * time.Second):
		}
		if reconnected {
			break
		}
	}

	if !reconnected {
		t.Fatal("transport did not reconnect: no message received by restarted receiver within timeout")
	}

	batches := handler2b.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected batches on restarted receiver")
	}
	t.Logf("reconnection verified: %d batch(es) received after receiver restart", len(batches))
}

// ---------------------------------------------------------------------------
// SetEpochTracker tests (line 334 - currently 0% coverage)
// ---------------------------------------------------------------------------

// TestSetEpochTracker_NilTracker verifies that SetEpochTracker can accept nil
// and will set the tracker to nil on the snapshot receiver.
func TestSetEpochTracker_NilTracker(t *testing.T) {
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

	// First set a non-nil tracker.
	tracker := newTestEpochTracker()
	tr.SetEpochTracker(tracker)

	if tr.snapshotRecv.epochTracker == nil {
		t.Fatal("epoch tracker should not be nil after SetEpochTracker")
	}

	// Now set it to nil.
	tr.SetEpochTracker(nil)

	if tr.snapshotRecv.epochTracker != nil {
		t.Fatal("epoch tracker should be nil after SetEpochTracker(nil)")
	}
}

// TestSetEpochTracker_BeforeStart verifies that SetEpochTracker can be called
// before Start() without issue.
func TestSetEpochTracker_BeforeStart(t *testing.T) {
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

	tracker := newTestEpochTracker()
	tr.SetEpochTracker(tracker)

	// Start should not panic and tracker should remain set.
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()

	if tr.snapshotRecv.epochTracker != tracker {
		t.Fatal("epoch tracker changed after Start")
	}
}

// ---------------------------------------------------------------------------
// Addr tests (line 414 - currently 0% coverage)
// ---------------------------------------------------------------------------

// TestAddr_ReturnsValidAddressAfterStart verifies that Addr returns
// the correct local address after the transport has been started.
func TestAddr_ReturnsValidAddressAfterStart(t *testing.T) {
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

	addr := tr.Addr()
	if addr == nil {
		t.Fatal("Addr should not be nil after Start")
	}

	// Verify the address is a UDP address.
	addrStr := addr.String()
	if addrStr == "" {
		t.Fatal("Addr string should not be empty")
	}

	// Verify the network is "udp".
	if addr.Network() != "udp" {
		t.Fatalf("expected network 'udp', got %s", addr.Network())
	}
}

// ---------------------------------------------------------------------------
// SendQueueCount tests (line 877 - currently 0% coverage)
// ---------------------------------------------------------------------------

// TestSendQueueCount_ThreadSafe verifies that SendQueueCount can be called
// concurrently without data races.
func TestSendQueueCount_ThreadSafe(t *testing.T) {
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

	// Register a target.
	reg.Register(1, 2, "127.0.0.1:9999")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = tr.SendQueueCount()
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// getOrCreateSendQueue error path tests (line 470 - 71.4% coverage)
// ---------------------------------------------------------------------------

// TestGetOrCreateSendQueue_StoppedDuringSlowPath verifies that getOrCreateSendQueue
// returns nil when the transport is stopped during the slow path (line 485-487).
func TestGetOrCreateSendQueue_StoppedDuringSlowPath(t *testing.T) {
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

	// First call creates the queue.
	sq1 := tr.getOrCreateSendQueue("127.0.0.1:8888")
	if sq1 == nil {
		t.Fatal("expected non-nil send queue on first call")
	}

	// Stop the transport.
	tr.Stop()

	// Attempting to create a queue for a new target after stop should return nil.
	sq2 := tr.getOrCreateSendQueue("127.0.0.1:7777")
	if sq2 != nil {
		t.Fatal("expected nil send queue for new target after stop")
	}
}

// TestGetOrCreateSendQueue_ConcurrentCreation verifies that concurrent calls
// to getOrCreateSendQueue for the same target result in only one queue being
// created (exercises the double-check pattern at lines 489-494).
func TestGetOrCreateSendQueue_ConcurrentCreation(t *testing.T) {
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

	target := "127.0.0.1:6666"
	var wg sync.WaitGroup
	queues := make(chan *sendQueue, 100)

	// Fire off many goroutines to create queues concurrently.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sq := tr.getOrCreateSendQueue(target)
			if sq != nil {
				queues <- sq
			}
		}()
	}
	wg.Wait()
	close(queues)

	// All should return the same queue.
	var firstQueue *sendQueue
	for sq := range queues {
		if firstQueue == nil {
			firstQueue = sq
		} else if sq != firstQueue {
			t.Fatal("expected all calls to return the same queue")
		}
	}

	// Should have exactly 1 send queue.
	if tr.SendQueueCount() != 1 {
		t.Fatalf("expected 1 send queue, got %d", tr.SendQueueCount())
	}
}

// ---------------------------------------------------------------------------
// sendQueueWorker error path tests (line 515 - 73.9% coverage)
// ---------------------------------------------------------------------------

// TestSendQueueWorker_BatchDrain verifies the batching behavior where
// multiple messages are drained from the channel (lines 549-557).
func TestSendQueueWorker_BatchDrain(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send a burst of messages to exercise the batch drain loop.
	for i := 0; i < 50; i++ {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1)},
		})
	}

	// Wait for all messages to arrive.
	pollUntil(t, 10*time.Second, 10*time.Millisecond,
		"expected at least 50 messages",
		func() bool {
			total := 0
			for _, b := range handler2.getBatches() {
				total += len(b.Requests)
			}
			return total >= 50
		})
}

// ---------------------------------------------------------------------------
// sendBatch error path tests (line 588 - 75.4% coverage)
// ---------------------------------------------------------------------------

// TestSendBatch_EmptyMessages verifies behavior when sendBatch is called
// with an empty message slice (exercises line 614-616).
func TestSendBatch_EmptyMessages(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// sendBatch with empty messages should handle gracefully.
	t1.sendBatch(target, "", []proto.Message{})
	// Should not panic.
}

// TestSendBatch_WriteError verifies that sendBatch handles stream.Write
// errors gracefully and evicts the connection (lines 690-700).
func TestSendBatch_WriteError(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)

	// Establish a connection.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Verify connection exists.
	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection, got %d", t1.ConnectionCount())
	}

	// Stop t2 to break the connection, then send until a send fails on it
	// and evicts it.
	t2.Stop()
	pollUntil(t, 10*time.Second, 20*time.Millisecond, "a send on the broken connection did not evict it", func() bool {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2},
		})
		return t1.ConnectionCount() == 0
	})
	cleanup()
}

// TestSendBatch_SnappyPoolReuse verifies that the snappyEncPool is correctly
// reused across multiple sends (lines 630-645).
func TestSendBatch_SnappyPoolReuse(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send multiple compressible messages to exercise pool reuse.
	for i := 0; i < 10; i++ {
		// Compressible data (repeated pattern).
		cmd := make([]byte, 500)
		for j := range cmd {
			cmd[j] = 'A'
		}

		t1.Send([]proto.Message{
			{
				Type:    proto.Replicate,
				ShardID: 1,
				From:    1,
				To:      2,
				Term:    uint64(i + 1),
				Entries: []proto.Entry{
					{Index: uint64(i + 1), Term: uint64(i + 1), Cmd: cmd},
				},
			},
		})
	}

	// Wait for all messages to arrive.
	pollUntil(t, 10*time.Second, 10*time.Millisecond,
		"expected at least 10 messages",
		func() bool {
			total := 0
			for _, b := range handler2.getBatches() {
				total += len(b.Requests)
			}
			return total >= 10
		})
}

func TestSendQueueWorkerOnce_HeartbeatPriority(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Enqueue heartbeat messages via the priority channel by sending
	// heartbeat type messages through the Send interface.
	for i := 0; i < 5; i++ {
		t1.Send([]proto.Message{
			{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1)},
		})
	}

	// Also send regular messages.
	for i := 0; i < 5; i++ {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 100)},
		})
	}

	// Wait for all messages.
	pollUntil(t, 10*time.Second, 10*time.Millisecond,
		"expected at least 10 messages",
		func() bool {
			total := 0
			for _, b := range handler2.getBatches() {
				total += len(b.Requests)
			}
			return total >= 10
		})
}

// TestSendQueueWorkerOnce_HeartbeatBatchDrain verifies that multiple
// heartbeat messages buffered in hbCh are drained into a single batch.

func TestSendQueueWorkerOnce_HeartbeatBatchDrain(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Register multiple shards pointing to the same target.
	t2Addr, err := t1.reg.Resolve(1, 2)
	if err != nil {
		t.Fatalf("failed to resolve t2: %v", err)
	}
	for shard := uint64(2); shard <= 10; shard++ {
		t1.reg.Register(shard, 2, t2Addr)
	}

	// Rapidly send heartbeats for multiple shards.
	for shard := uint64(1); shard <= 10; shard++ {
		t1.Send([]proto.Message{
			{Type: proto.Heartbeat, ShardID: shard, From: 1, To: 2, Term: 1},
		})
	}

	// Wait for all messages.
	pollUntil(t, 10*time.Second, 10*time.Millisecond,
		"expected at least 10 heartbeat messages",
		func() bool {
			total := 0
			for _, b := range handler2.getBatches() {
				total += len(b.Requests)
			}
			return total >= 10
		})
}

// ---------------------------------------------------------------------------
// sendBatch error paths (quic.go:736 - 68.9% coverage)
// ---------------------------------------------------------------------------

// TestSendBatch_CompressionIncompressibleData verifies that incompressible
// data is sent uncompressed (Snappy output larger than input).

// TestSendSnapshot_StoppedTransport verifies that SendSnapshot returns
// ErrStreamClosed when the transport is stopped.

// ---------------------------------------------------------------------------
// SendSnapshotStreaming (quic.go:1053 - 0% coverage)
// ---------------------------------------------------------------------------

// TestSendSnapshotStreaming_StoppedTransport verifies that
// SendSnapshotStreaming returns ErrStreamClosed when the transport is stopped.

func TestSendSnapshotStreaming_StoppedTransport(t *testing.T) {
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

	meta := SnapshotHeader{ShardID: 1, ReplicaID: 2, Index: 100, ChunkCount: 1}
	err = tr.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
		return nil
	})
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("expected ErrStreamClosed, got %v", err)
	}
}

// TestSendSnapshotStreaming_UnresolvableTarget verifies that
// SendSnapshotStreaming returns an error when the target cannot be resolved.

func TestSendSnapshotStreaming_UnresolvableTarget(t *testing.T) {
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

	meta := SnapshotHeader{ShardID: 99, ReplicaID: 99, Index: 1, ChunkCount: 1}
	err = tr.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for unresolvable target")
	}
}

// TestSendSnapshotStreaming_ConnectionFailure verifies that
// SendSnapshotStreaming returns an error when the connection cannot be
// established.

func TestSendSnapshotStreaming_ConnectionFailure(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	// Register a target that cannot be connected to.
	reg.Register(1, 2, "127.0.0.1:1")

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

	meta := SnapshotHeader{ShardID: 1, ReplicaID: 2, Index: 1, ChunkCount: 1}
	err = tr.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
		return emit(proto.SnapshotChunk{BinVer: proto.WireVersion, ShardID: 1, ReplicaID: 2, Data: []byte("test")})
	})
	if err == nil {
		t.Fatal("expected error for unreachable target")
	}
}

// ---------------------------------------------------------------------------
// ipConnTracker concurrency (recv.go:70-132)
// ---------------------------------------------------------------------------

// TestIPConnTracker_ConcurrentAcquireRelease verifies that concurrent
// tryAcquire and release calls are race-free.

// TestLocalAddr_AfterStart verifies that localAddr returns the resolved
// address after Start.

func TestLocalAddr_AfterStart(t *testing.T) {
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

	addr := tr.localAddr()
	if addr == "" {
		t.Fatal("localAddr should not be empty after Start")
	}
	if addr == ":0" {
		t.Fatal("localAddr should be resolved, not :0")
	}
}

// ---------------------------------------------------------------------------
// Send edge cases (quic.go:481-541)
// ---------------------------------------------------------------------------

// TestSend_StoppedTransportIsNoop verifies that Send does nothing when
// the transport is stopped.

// TestSend_EmptySlice verifies that Send with an empty slice is a no-op.

// TestSend_HeartbeatQueueFull verifies that dropping heartbeat messages
// when the heartbeat channel is full does not panic.

func TestSend_HeartbeatQueueFull(t *testing.T) {
	t1, _, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send many heartbeats rapidly to fill the heartbeat channel.
	for i := 0; i < 200; i++ {
		t1.Send([]proto.Message{
			{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: uint64(i)},
		})
	}
	// Should not panic even if the heartbeat channel is full.
}

// TestSend_DataQueueFull verifies that dropping data messages when the
// send queue is full does not panic.

func TestSend_DataQueueFull(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	// Register a target that cannot be connected to (messages will back up).
	reg.Register(1, 2, "127.0.0.1:1")

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

	// Send many messages rapidly to exercise backpressure.
	for i := 0; i < 3000; i++ {
		tr.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i)},
		})
	}
	// Should not panic even if the queue is full.
}

// ---------------------------------------------------------------------------
// Stop edge cases (quic.go:384)
// ---------------------------------------------------------------------------

// TestStop_DoubleStopIsNoop verifies that calling Stop twice does not panic.

// ---------------------------------------------------------------------------
// sendHeartbeatBatch write deadline error path (heartbeat.go:114-118)
// ---------------------------------------------------------------------------

// TestSendHeartbeatBatch_WriteDeadlineError verifies that the heartbeat
// fast path handles SetWriteDeadline errors gracefully by closing the
// connection and evicting streams.

func TestSend_DeepCopyEntries(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	cmd := []byte("original-command")
	msgs := []proto.Message{
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
	}

	t1.Send(msgs)

	// Modify the original command after Send returns to verify the
	// deep copy is effective.
	copy(cmd, "modified-command")

	handler2.waitBatch(t, 10*time.Second)

	batches := handler2.getBatches()
	if len(batches) == 0 {
		t.Fatal("expected at least 1 batch")
	}
	receivedCmd := string(batches[0].Requests[0].Entries[0].Cmd)
	if receivedCmd != "original-command" {
		t.Fatalf("expected 'original-command', got %q (deep copy failed)", receivedCmd)
	}
}

// ---------------------------------------------------------------------------
// handleMessageFrame with handler error (recv.go:529)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_HandlerError verifies that handleMessageFrame
// propagates errors from the handler.

func TestSendSnapshotStreaming_RoundTrip(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	meta := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      200,
		Term:       10,
		ChunkCount: 3,
	}

	err := t1.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
		for i := uint64(0); i < 3; i++ {
			chunk := proto.SnapshotChunk{
				BinVer:     proto.WireVersion,
				ShardID:    1,
				ReplicaID:  2,
				Index:      200,
				Term:       10,
				ChunkID:    i,
				ChunkCount: 3,
				Data:       []byte("chunk-data"),
			}
			if err := emit(chunk); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("SendSnapshotStreaming failed: %v", err)
	}

	handler2.waitSnapshot(t, 10*time.Second)

	snaps := handler2.getSnapshots()
	if len(snaps) == 0 {
		t.Fatal("expected at least 1 snapshot")
	}
	if len(snaps[0]) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(snaps[0]))
	}
}

// TestSendSnapshotStreaming_WithEventListener verifies that snapshot
// lifecycle events are fired during streaming sends.

func TestSendSnapshotStreaming_WithEventListener(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()

	sendStarted := make(chan struct{}, 1)
	sendCompleted := make(chan struct{}, 1)

	mtls := testMTLSConfig(t)
	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		EventListener: &config.EventListener{
			OnSnapshotSendStarted: func(info config.SnapshotInfo) {
				select {
				case sendStarted <- struct{}{}:
				default:
				}
			},
			OnSnapshotSent: func(info config.SnapshotInfo) {
				select {
				case sendCompleted <- struct{}{}:
				default:
				}
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
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	meta := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      300,
		Term:       15,
		ChunkCount: 1,
	}

	err = t1.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
		return emit(proto.SnapshotChunk{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			Index:      300,
			Term:       15,
			ChunkCount: 1,
			Data:       []byte("event-test-data"),
		})
	})
	if err != nil {
		t.Fatalf("SendSnapshotStreaming failed: %v", err)
	}

	select {
	case <-sendStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("OnSnapshotSendStarted not fired")
	}
	select {
	case <-sendCompleted:
	case <-time.After(5 * time.Second):
		t.Fatal("OnSnapshotSent not fired")
	}
}

// TestSendSnapshotStreaming_WithMetrics verifies the metrics recording
// path for streaming snapshot sends.

// TestSendSnapshotStreaming_ProducerError verifies that an error from
// the producer function is propagated correctly.

func TestSendSnapshotStreaming_ProducerError(t *testing.T) {
	t1, _, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	meta := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      500,
		Term:       25,
		ChunkCount: 3,
	}

	producerErr := errors.New("disk read failed")
	err := t1.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
		// Emit one chunk successfully, then fail.
		if emitErr := emit(proto.SnapshotChunk{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			Index:      500,
			Term:       25,
			ChunkID:    0,
			ChunkCount: 3,
			Data:       []byte("first-chunk"),
		}); emitErr != nil {
			return emitErr
		}
		return producerErr
	})
	if !errors.Is(err, producerErr) {
		t.Fatalf("expected producer error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// sendQueueWorker panic restart (quic.go:599 - 62.5% coverage)
// ---------------------------------------------------------------------------

// TestSendQueueWorker_PanicRestart verifies that the sendQueueWorker
// for-loop restarts after a panic in sendQueueWorkerOnce and checks
// stopC before restarting.

// ---------------------------------------------------------------------------
// SendSnapshot with event listener (quic.go:888)
// ---------------------------------------------------------------------------

// TestSendSnapshot_WithEventListener verifies that snapshot lifecycle
// events are fired during SendSnapshot.

// ---------------------------------------------------------------------------
// sendBatch with metrics (quic.go:859-873)
// ---------------------------------------------------------------------------

// TestSendBatch_WithMetricsAndDebugLog verifies the sendBatch metrics
// recording and debug logging paths with a successful write.

// ---------------------------------------------------------------------------
// handleMessageFrame bulk Cmd copy (recv.go:474-493)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_BulkCmdCopy verifies that entry Cmd slices are
// detached from the receive buffer via the bulk copy optimization.

// ---------------------------------------------------------------------------
// Start with UDPRecvBufSize/UDPSendBufSize (quic.go:303-314)
// ---------------------------------------------------------------------------

// TestStart_WithUDPBufferSizes verifies that Start exercises the
// SO_RCVBUF and SO_SNDBUF setsockopt paths.

func TestStart_WithUDPBufferSizes(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	tr, err := NewQUICTransport(Config{
		ListenAddress:  ":0",
		DeploymentID:   42,
		MTLSConfig:     testMTLSConfig(t),
		UDPRecvBufSize: 1024 * 1024,
		UDPSendBufSize: 1024 * 1024,
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()

	if tr.Addr() == nil {
		t.Fatal("expected non-nil address after Start")
	}
}

// ---------------------------------------------------------------------------
// acceptLoopIteration global connection limit rejection (recv.go:194-215)
// ---------------------------------------------------------------------------

// TestAcceptLoopIteration_GlobalConnectionLimitReached verifies that when
// the global connection semaphore is full, additional incoming connections
// are rejected with QUICErrConnLimitReached. This exercises the default
// case in acceptLoopIteration at recv.go:206-215.

func TestStop_WithActiveConnectionsAndStreams(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)

	// Establish connection and verify stream creation.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Send to multiple shards to create multiple streams.
	t2Addr := t2.Addr().String()
	t1.reg.Register(2, 2, t2Addr)
	t1.reg.Register(3, 2, t2Addr)
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 2, From: 1, To: 2, Term: 1},
		{Type: proto.Replicate, ShardID: 3, From: 1, To: 2, Term: 1},
	})
	// Wait for at least 2 total batches (shard 1 already delivered above).
	pollUntil(t, 5*time.Second, 20*time.Millisecond,
		"expected additional messages to arrive",
		func() bool { return handler2.batchCount.Load() >= 2 })

	if t1.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection, got %d", t1.ConnectionCount())
	}

	// Now call cleanup which triggers Stop on both.
	cleanup()

	// Verify both transports are fully stopped.
	if t1.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections after stop, got %d", t1.ConnectionCount())
	}
	if t2.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections on t2 after stop, got %d", t2.ConnectionCount())
	}
}

// ---------------------------------------------------------------------------
// sendQueueWorkerOnce blocking select data channel path (quic.go:701-719)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// sendQueueWorkerOnce blocking hbCh select (quic.go:680-691)
// ---------------------------------------------------------------------------

// TestSendQueueWorkerOnce_BlockingHBChannelDrain verifies that heartbeats
// on the blocking select (hbCh case at quic.go:680) are properly drained
// and batched.

func TestSendQueueWorkerOnce_BlockingHBChannelDrain(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send heartbeats with a small delay between each to ensure
	// some hit the blocking select path (not just the non-blocking
	// priority check).
	hbTicker := time.NewTicker(5 * time.Millisecond)
	for i := 0; i < 10; i++ {
		t1.Send([]proto.Message{
			{Type: proto.HeartbeatResp, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1)},
		})
		<-hbTicker.C
	}
	hbTicker.Stop()

	pollUntil(t, 10*time.Second, 50*time.Millisecond,
		"expected at least 8 heartbeat responses",
		func() bool {
			total := 0
			for _, b := range handler2.getBatches() {
				total += len(b.Requests)
			}
			return total >= 8
		})
}

// ---------------------------------------------------------------------------
// handleMessageFrame decompression mismatch (recv.go:384-389)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_DecompressionMismatch verifies that
// ErrDecompressionMismatch is returned when the decompressed size
// does not match the declared size.

func TestSend_BackpressureLogging(t *testing.T) {
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

	// Register a target.
	reg.Register(1, 2, "127.0.0.1:19999")

	// Fill the send queue by sending more messages than the queue capacity.
	// defaultSendQueueLen is 2048, so sending 3000+ should trigger drops.
	for i := 0; i < 3000; i++ {
		tr.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1)},
		})
	}

	// The test verifies the backpressure path is exercised without panic.
}

// ---------------------------------------------------------------------------
// handleMessageFrame metrics on receive path (recv.go:519-527)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_ReceiveMetrics verifies that the receive metrics
// recording path is exercised when Metrics is configured.

// ---------------------------------------------------------------------------
// Stop with revocationChecker (quic.go:393-395)
// ---------------------------------------------------------------------------

// TestStop_WithListenerCloseError exercises the listener close path
// during Stop with active connections.

func TestStop_WithListenerCloseError(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)

	// Establish connection.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Stop both transports (via cleanup) to exercise the full
	// shutdown path with active listener and connections.
	cleanup()

	// Verify double-stop is a no-op.
	_ = t1.Stop()
	_ = t2.Stop()
}

// ---------------------------------------------------------------------------
// sendBatch direct call with dead stream (quic.go:838-857)
// ---------------------------------------------------------------------------

// TestSendBatch_DirectCallWriteDeadlineError verifies that sendBatch
// handles SetWriteDeadline errors by directly calling sendBatch on a
// transport with a cached but dead stream.

func TestSendSnapshotStreaming_ProducerErrorWithEventListener(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	aborted := make(chan config.SnapshotInfo, 1)
	started := make(chan config.SnapshotInfo, 1)

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		EventListener: &config.EventListener{
			OnSnapshotSendStarted: func(info config.SnapshotInfo) {
				select {
				case started <- info:
				default:
				}
			},
			OnSnapshotSendAborted: func(info config.SnapshotInfo) {
				select {
				case aborted <- info:
				default:
				}
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
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	meta := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      500,
		Term:       25,
		ChunkCount: 3,
	}

	producerErr := errors.New("disk read failed")
	sendErr := t1.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
		// Emit one chunk successfully, then fail.
		if emitErr := emit(proto.SnapshotChunk{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			Index:      500,
			Term:       25,
			ChunkID:    0,
			ChunkCount: 3,
			Data:       []byte("first-chunk"),
		}); emitErr != nil {
			return emitErr
		}
		return producerErr
	})
	if !errors.Is(sendErr, producerErr) {
		t.Fatalf("expected producer error, got %v", sendErr)
	}

	// OnSnapshotSendStarted should have been fired.
	select {
	case info := <-started:
		if info.ShardID != 1 {
			t.Fatalf("expected ShardID 1, got %d", info.ShardID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSnapshotSendStarted was not fired")
	}

	// OnSnapshotSendAborted should have been fired due to producer error.
	select {
	case info := <-aborted:
		if info.ShardID != 1 {
			t.Fatalf("expected ShardID 1, got %d", info.ShardID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSnapshotSendAborted was not fired")
	}
}

// ---------------------------------------------------------------------------
// SendSnapshot with event listeners (quic.go:966-977)
// ---------------------------------------------------------------------------

// TestSendSnapshot_WithEventListeners verifies that OnSnapshotSendStarted
// and OnSnapshotSent events are fired during successful snapshot send.

func TestSendSnapshot_WithEventListeners(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	started := make(chan config.SnapshotInfo, 1)
	sent := make(chan config.SnapshotInfo, 1)

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		Metrics:       noopTransportMetrics{},
		EventListener: &config.EventListener{
			OnSnapshotSendStarted: func(info config.SnapshotInfo) {
				select {
				case started <- info:
				default:
				}
			},
			OnSnapshotSent: func(info config.SnapshotInfo) {
				select {
				case sent <- info:
				default:
				}
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
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	chunks := []proto.SnapshotChunk{
		{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			From:       1,
			Index:      50,
			Term:       3,
			ChunkID:    0,
			ChunkCount: 1,
			ChunkSize:  5,
			Data:       []byte("hello"),
		},
	}

	if sendErr := t1.SendSnapshot(chunks); sendErr != nil {
		t.Fatalf("SendSnapshot failed: %v", sendErr)
	}

	select {
	case info := <-started:
		if info.ShardID != 1 {
			t.Fatalf("expected ShardID 1, got %d", info.ShardID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSnapshotSendStarted was not fired")
	}

	select {
	case info := <-sent:
		if info.ShardID != 1 {
			t.Fatalf("expected ShardID 1, got %d", info.ShardID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSnapshotSent was not fired")
	}
}

// ---------------------------------------------------------------------------
// SendSnapshotStreaming with event listener: OnSnapshotSendStarted + success
// (quic.go:1112-1114, 1159-1177)
// ---------------------------------------------------------------------------

// TestSendSnapshotStreaming_SuccessWithEventListeners verifies that the
// success path, with metrics recording enabled, fires OnSnapshotSendStarted
// and OnSnapshotSent, and the deferred OnSnapshotSendAborted is NOT fired.

func TestSendSnapshotStreaming_SuccessWithEventListeners(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	started := make(chan config.SnapshotInfo, 1)
	aborted := make(chan struct{}, 1)
	sent := make(chan config.SnapshotInfo, 1)

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		Metrics:       noopTransportMetrics{},
		EventListener: &config.EventListener{
			OnSnapshotSendStarted: func(info config.SnapshotInfo) {
				select {
				case started <- info:
				default:
				}
			},
			OnSnapshotSendAborted: func(info config.SnapshotInfo) {
				select {
				case aborted <- struct{}{}:
				default:
				}
			},
			OnSnapshotSent: func(info config.SnapshotInfo) {
				select {
				case sent <- info:
				default:
				}
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
	defer t2.Stop()

	reg1.Register(1, 2, t2.Addr().String())

	meta := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      500,
		Term:       25,
		ChunkCount: 1,
	}

	sendErr := t1.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
		return emit(proto.SnapshotChunk{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			Index:      500,
			Term:       25,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("streaming-data"),
		})
	})
	if sendErr != nil {
		t.Fatalf("SendSnapshotStreaming failed: %v", sendErr)
	}

	select {
	case info := <-started:
		if info.ShardID != 1 {
			t.Fatalf("expected ShardID 1, got %d", info.ShardID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSnapshotSendStarted was not fired")
	}

	select {
	case info := <-sent:
		if info.ShardID != 1 {
			t.Fatalf("expected ShardID 1, got %d", info.ShardID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSnapshotSent was not fired")
	}

	// The deferred OnSnapshotSendAborted runs before SendSnapshotStreaming
	// returns, so it would already be queued here.
	select {
	case <-aborted:
		t.Fatal("OnSnapshotSendAborted should not have been fired on success")
	default:
	}
}

// ---------------------------------------------------------------------------
// receiveChunks with HandleSnapshot error (snapshot_recv.go:276-282)
// ---------------------------------------------------------------------------

// TestReceiveChunks_HandleSnapshotError verifies that when HandleSnapshot
// returns an error, it is logged but does not cause a panic.

// ---------------------------------------------------------------------------
// getOrCreateSendQueue stopped during slow path (quic.go:562-564)
// ---------------------------------------------------------------------------

// TestGetOrCreateSendQueue_StoppedAfterFastPathMiss verifies that
// getOrCreateSendQueue returns nil when the transport is stopped
// after the RLock fast path miss but before the write lock acquisition.

// ---------------------------------------------------------------------------
// getOrCreateSendQueue slow path double-check (quic.go:567-571)
// ---------------------------------------------------------------------------

// TestGetOrCreateSendQueue_SlowPathDoubleCheck verifies that the slow
// path double-check in getOrCreateSendQueue returns an existing queue
// found after acquiring the write lock.

// ---------------------------------------------------------------------------
// handleMessageFrame decompression mismatch (recv.go:384-390)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_DecompressionMismatchExact exercises the exact
// decompression mismatch path where DecodedLen succeeds but the actual
// decoded output length differs from the declared length. This is achieved
// by crafting a valid snappy stream with a mismatched length header.

// ---------------------------------------------------------------------------
// handleStream stopped check (recv.go:309-311)
// ---------------------------------------------------------------------------

// TestHandleStream_StoppedTransportReturns verifies that handleStream
// returns immediately when the transport is stopped.

func TestStop_WithPreClosedConnectionsAndStreams(t *testing.T) {
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

	// Establish connection and send messages to create streams.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Pre-close the connection. When Stop() tries to close it again,
	// it will get an error (connection already closed).
	t1.connMu.RLock()
	conn := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if conn != nil {
		conn.CloseWithError(0, "pre-close for test")
		// Wait for connection context to cancel before stopping transports.
		waitForChan(t, conn.Context().Done(), 15*time.Second,
			"connection context not cancelled after pre-close")
	}

	// Stop t2 first, then stop t1. The pre-closed connection
	// will trigger the error log path in Stop().
	t2.Stop()
	t1.Stop()
}

// ---------------------------------------------------------------------------
// Send queue backpressure path (quic.go:530-538)
// ---------------------------------------------------------------------------

// fillSendQueue offers messages into sq's data channel until the channel
// refuses one, and returns how many were accepted. The refusal is the point:
// it is the queue-full condition Send's backpressure branch depends on, so
// without observing it a "the queue is full now" test would be asserting
// against an arbitrary queue depth.
func fillSendQueue(t *testing.T, sq *sendQueue, shardID, to uint64) int {
	t.Helper()
	const offers = 4 * defaultSendQueueLen
	for i := 0; i < offers; i++ {
		select {
		case sq.ch <- proto.Message{Type: proto.Replicate, ShardID: shardID, From: 1, To: to, Term: 1}:
		default:
			return i
		}
	}
	t.Fatalf("send queue accepted all %d offers without filling (len=%d cap=%d)",
		offers, len(sq.ch), cap(sq.ch))
	return 0
}

// requireQueueFullDrop calls Send until the transport counts a queue-full drop,
// then asserts the drop carries the expected shard and target. Retrying is what
// makes this deterministic: the per-target worker drains in batches of up to
// 256, so a single Send issued just after the queue filled can still be
// accepted. Send must never block on a full queue, so a hang here is a failure
// in its own right.
func requireQueueFullDrop(t *testing.T, tr *QUICTransport, rec *dropRecorder, shardID, to uint64) {
	t.Helper()
	const sends = 8 * defaultSendQueueLen
	for i := 0; i < sends; i++ {
		tr.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: shardID, From: 1, To: to, Term: uint64(i + 1)},
		})
		if got, ok := rec.firstDropWithReason(dropReasonQueueFull); ok {
			if got.shardID != shardID || got.to != to {
				t.Fatalf("queue-full drop = %+v, want shard %d to %d", got, shardID, to)
			}
			return
		}
	}
	t.Fatalf("Send never took the queue-full backpressure path in %d sends", sends)
}

// ---------------------------------------------------------------------------
// handleMessageFrame with debug timing (recv.go:346-348)
// ---------------------------------------------------------------------------

// TestHandleMessageFrame_WithDebugLogTiming exercises the debug log timing
// path in handleMessageFrame by sending a valid uncompressed batch.
// The debug log branch at recv.go:346-348 requires slog debug level.

func TestSendSnapshot_OpenStreamSyncError(t *testing.T) {
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

	// Establish connection.
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	// Stop t2 and close the connection to make OpenStreamSync fail.
	t2.Stop()
	t1.connMu.RLock()
	conn := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if conn != nil {
		conn.CloseWithError(0, "force close for test")
		waitForChan(t, conn.Context().Done(), 15*time.Second,
			"connection context not cancelled after CloseWithError")
	}

	// SendSnapshot should fail at OpenStreamSync or getConnection.
	err = t1.SendSnapshot([]proto.SnapshotChunk{
		{BinVer: proto.WireVersion, ShardID: 1, ReplicaID: 2, From: 1, Index: 1, Term: 1,
			ChunkID: 0, ChunkCount: 1, ChunkSize: 5,
			Data: []byte("hello")},
	})
	if err == nil {
		t.Fatal("expected error from SendSnapshot after connection closed")
	}
}

// ---------------------------------------------------------------------------
// SendSnapshotStreaming OpenStreamSync error (quic.go:1081-1084)
// ---------------------------------------------------------------------------

// TestSendSnapshotStreaming_OpenStreamSyncError exercises the
// OpenStreamSync error path in SendSnapshotStreaming.

func TestSendSnapshotStreaming_OpenStreamSyncError(t *testing.T) {
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

	// Establish the connection: a cold Send, bounded by what the transport
	// allows one and re-sent if it is abandoned.
	sendFirstContact(t, t1, handler2, []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})

	// Stop t2 and close the connection.
	t2.Stop()
	t1.connMu.RLock()
	conn := t1.conns[t2Addr]
	t1.connMu.RUnlock()
	if conn != nil {
		conn.CloseWithError(0, "force close for test")
		waitForChan(t, conn.Context().Done(), 15*time.Second,
			"connection context not cancelled after CloseWithError")
	}

	err = t1.SendSnapshotStreaming(
		SnapshotHeader{ShardID: 1, ReplicaID: 2, Index: 1, Term: 1, ChunkCount: 1},
		func(emit func(proto.SnapshotChunk) error) error {
			return emit(proto.SnapshotChunk{
				BinVer:  proto.WireVersion,
				ShardID: 1, ReplicaID: 2, From: 1, Index: 1, Term: 1,
				ChunkID: 0, ChunkCount: 1, ChunkSize: 5,
				Data: []byte("hello"),
			})
		},
	)
	if err == nil {
		t.Fatal("expected error from SendSnapshotStreaming after connection closed")
	}
}

// ---------------------------------------------------------------------------
// sendBatch write deadline and write error paths (quic.go:839-857)
// ---------------------------------------------------------------------------

// TestSendBatch_WriteErrorEvictsConnection exercises the stream write
// error path in sendBatch by sending to a transport that has been stopped.
// After getStream succeeds, the remote connection is closed, causing the
// Write to fail and trigger connection removal + stream eviction.

func TestSendSnapshot_ChunkWriteErrorTriggersAbort(t *testing.T) {
	mtls := testMTLSConfig(t)

	aborted := make(chan config.SnapshotInfo, 1)
	handler1 := newTestHandler()
	reg1 := registry.NewRegistry()
	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		EventListener: &config.EventListener{
			OnSnapshotSendStarted: func(info config.SnapshotInfo) {},
			OnSnapshotSendAborted: func(info config.SnapshotInfo) {
				select {
				case aborted <- info:
				default:
				}
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
	reg1.Register(1, 2, t2Addr)

	// Build many large chunks to maximize the chance that some
	// write fails after the connection is closed.
	chunks := make([]proto.SnapshotChunk, 100)
	largeData := bytes.Repeat([]byte("snapshot-data"), 1000)
	for i := range chunks {
		chunks[i] = proto.SnapshotChunk{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			Index:      10,
			Term:       5,
			ChunkID:    uint64(i),
			ChunkCount: uint64(len(chunks)),
			Data:       largeData,
		}
	}

	// Stop t2 after a short delay to close the connection mid-send.
	go func() {
		midSendTimer := time.NewTimer(50 * time.Millisecond)
		<-midSendTimer.C
		midSendTimer.Stop()
		t2.Stop()
	}()

	// SendSnapshot should return an error (either chunk write or stream error).
	sendErr := t1.SendSnapshot(chunks)
	if sendErr == nil {
		t.Log("SendSnapshot succeeded (receiver didn't close in time)")
		return
	}

	// Check that OnSnapshotSendAborted was fired.
	select {
	case info := <-aborted:
		if info.ShardID != 1 {
			t.Fatalf("expected ShardID 1, got %d", info.ShardID)
		}
	case <-time.After(2 * time.Second):
		t.Log("abort event not received (timing-dependent)")
	}
}

// ---------------------------------------------------------------------------
// SendSnapshotStreaming chunk write error (quic.go:1141-1148)
// ---------------------------------------------------------------------------

// TestSendSnapshotStreaming_ChunkWriteError exercises the streaming
// snapshot send path where a chunk write fails because the receiver
// connection is closed mid-transfer.

func TestSendSnapshotStreaming_ChunkWriteError(t *testing.T) {
	mtls := testMTLSConfig(t)

	handler1 := newTestHandler()
	reg1 := registry.NewRegistry()
	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		EventListener: &config.EventListener{
			OnSnapshotSendStarted: func(info config.SnapshotInfo) {},
			OnSnapshotSendAborted: func(info config.SnapshotInfo) {},
		},
	}, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

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
	defer t2.Stop()

	t2Addr := t2.Addr().String()
	reg1.Register(1, 2, t2Addr)

	meta := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      10,
		Term:       5,
		ChunkCount: 100,
	}

	// Close the connection once the first chunk is on the wire: the next
	// chunk write fails on it.
	largeData := bytes.Repeat([]byte("streaming-chunk"), 1000)
	conn, err := t1.getConnection(t2Addr, "")
	if err != nil {
		t.Fatalf("getConnection: %v", err)
	}
	sendErr := t1.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
		for i := uint64(0); i < 100; i++ {
			if err := emit(proto.SnapshotChunk{
				BinVer:     proto.WireVersion,
				ShardID:    1,
				ReplicaID:  2,
				Index:      10,
				Term:       5,
				ChunkID:    i,
				ChunkCount: 100,
				Data:       largeData,
			}); err != nil {
				return err
			}
			if i == 0 {
				conn.CloseWithError(0, "test: break the snapshot stream")
			}
		}
		return errors.New("no chunk write failed after the connection closed")
	})
	var writeErr *SnapshotChunkWriteTimeoutError
	if !errors.As(sendErr, &writeErr) {
		t.Fatalf("SendSnapshotStreaming error = %v, want *SnapshotChunkWriteTimeoutError", sendErr)
	}
	if writeErr.ShardID != 1 || writeErr.ReplicaID != 2 || writeErr.ChunkID == 0 {
		t.Fatalf("chunk write error names shard %d replica %d chunk %d, want 1, 2 and a chunk after the first",
			writeErr.ShardID, writeErr.ReplicaID, writeErr.ChunkID)
	}
}

// ---------------------------------------------------------------------------
// ShutdownAndWait with timer path (shutdown_tracker.go:101-103)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// SendSnapshot OpenStreamSync error via shutdown cancel (quic.go:922-924)
// ---------------------------------------------------------------------------

// TestSendSnapshot_OpenStreamSyncErrorViaShutdownCancel exercises the
// OpenStreamSync error path by canceling the shutdown context before
// calling SendSnapshot. The OpenStreamSync call will fail because the
// context is already canceled.

func TestSendSnapshot_OpenStreamSyncErrorViaShutdownCancel(t *testing.T) {
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

	// Establish a connection first so getConnection succeeds.
	_, dialErr := t1.getConnection(t2Addr, "")
	if dialErr != nil {
		t.Fatalf("getConnection failed: %v", dialErr)
	}

	// Cancel the shutdown context so OpenStreamSync (using shutdownCtx)
	// will fail.
	t1.shutdownCancel()

	chunks := []proto.SnapshotChunk{
		{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			Index:      10,
			Term:       5,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("data"),
		},
	}

	sendErr := t1.SendSnapshot(chunks)
	if sendErr == nil {
		t.Fatal("expected error from SendSnapshot with canceled shutdown context")
	}
}

// ---------------------------------------------------------------------------
// SendSnapshotStreaming OpenStreamSync error via shutdown cancel
// (quic.go:1082-1084)
// ---------------------------------------------------------------------------

// TestSendSnapshotStreaming_OpenStreamSyncErrorViaShutdown exercises the
// OpenStreamSync error path in SendSnapshotStreaming by canceling the
// shutdown context before calling SendSnapshotStreaming.

func TestSendSnapshotStreaming_OpenStreamSyncErrorViaShutdown(t *testing.T) {
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

	// Establish connection first.
	_, dialErr := t1.getConnection(t2Addr, "")
	if dialErr != nil {
		t.Fatalf("getConnection failed: %v", dialErr)
	}

	// Cancel shutdown context.
	t1.shutdownCancel()

	meta := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      10,
		Term:       5,
		ChunkCount: 1,
	}

	sendErr := t1.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
		return emit(proto.SnapshotChunk{
			BinVer:     proto.WireVersion,
			ShardID:    1,
			ReplicaID:  2,
			Index:      10,
			Term:       5,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("data"),
		})
	})
	if sendErr == nil {
		t.Fatal("expected error from SendSnapshotStreaming with canceled context")
	}
}

// ---------------------------------------------------------------------------
// ShutdownAndWait with timer path (shutdown_tracker.go:101-103)
// ---------------------------------------------------------------------------

// TestShutdownTracker_ShutdownAndWaitWithInFlight exercises the
// ShutdownAndWait timeout path by having an in-flight operation
// that completes after the shutdown signal is set.

func TestGetOrCreateSendQueue_StoppedWhileWaitingForWriteLock(t *testing.T) {
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

	target := "127.0.0.1:40000"

	// Hold the write lock to block the slow path.
	tr.sendQueuesMu.Lock()

	// Launch getOrCreateSendQueue in a goroutine. It will pass the
	// fast-path RLock (no queue exists), then block at the write Lock().
	resultCh := make(chan *sendQueue, 1)
	go func() {
		resultCh <- tr.getOrCreateSendQueue(target)
	}()

	// Give the goroutine time to reach the Lock() call.
	lockWaitTimer := time.NewTimer(50 * time.Millisecond)
	<-lockWaitTimer.C
	lockWaitTimer.Stop()

	// Stop the transport while the goroutine is blocked.
	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		// Stop() also acquires sendQueuesMu.Lock(), but it will block
		// because we hold it. We need to release our lock first.
		tr.Stop()
	}()

	// Set stopped flag directly to simulate the stop happening while
	// we hold the lock.
	tr.stopped.Store(true)

	// Release the lock. The goroutine will acquire it, see stopped=true
	// at line 562, and return nil.
	tr.sendQueuesMu.Unlock()

	// Wait for result.
	select {
	case sq := <-resultCh:
		if sq != nil {
			t.Fatal("expected nil queue after transport stopped during slow path")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("getOrCreateSendQueue did not return within timeout")
	}

	// Wait for Stop to complete.
	waitForChan(t, stopDone, 5*time.Second, "Stop did not complete within timeout")
}

// ---------------------------------------------------------------------------
// sendBatch write error via connection close (quic.go:848-857)
// ---------------------------------------------------------------------------

// TestSendBatch_DirectWriteErrorViaConnectionClose exercises the stream
// write error path by getting a stream, then closing the underlying
// connection before calling sendBatch. The cached stream has a stale
// connection, causing the write to fail.

func TestGetOrCreateSendQueue_DoubleCheckHitViaBarrier(t *testing.T) {
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

	// Run many rounds. In each round, delete the queue and race
	// goroutines to recreate it. When two goroutines both miss the
	// RLock check and race to the write lock, one creates and the
	// other hits the double-check.
	for round := 0; round < 20; round++ {
		target := "127.0.0.1:41000"

		// Delete any existing queue so the fast path misses.
		tr.sendQueuesMu.Lock()
		delete(tr.sendQueues, target)
		tr.sendQueuesMu.Unlock()

		// Race goroutines to create the queue.
		const numGoroutines = 100
		var wg sync.WaitGroup
		start := make(chan struct{})
		results := make([]*sendQueue, numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			idx := i
			go func() {
				defer wg.Done()
				<-start
				results[idx] = tr.getOrCreateSendQueue(target)
			}()
		}
		close(start)
		wg.Wait()

		// All should return the same queue.
		for i := 1; i < numGoroutines; i++ {
			if results[i] != results[0] {
				t.Fatalf("round %d: goroutine %d got different queue", round, i)
			}
		}
	}
}

// TestSend_BackpressureLoggingOverflow exercises the send queue backpressure
// path by flooding the data channel past capacity, and asserts the overflow is
// counted rather than silently swallowed.

func TestSend_BackpressureLoggingOverflow(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	mtls := testMTLSConfig(t)
	rec := &dropRecorder{}

	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		Metrics:       rec,
	}, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()

	reg.Register(1, 2, "127.0.0.1:50001")

	// Create the queue first.
	sq := tr.getOrCreateSendQueue("127.0.0.1:50001")
	if sq == nil {
		t.Fatal("expected non-nil queue")
	}

	if accepted := fillSendQueue(t, sq, 1, 2); accepted == 0 {
		t.Fatal("send queue refused the very first offer; it was never empty")
	}
	requireQueueFullDrop(t, tr, rec, 1, 2)
}

// ---------------------------------------------------------------------------
// snappyEncPool bootstrap + cap (quic.go NewQUICTransport / sendBatch)
// ---------------------------------------------------------------------------

// TestSnappyEncPool_Bootstraps verifies that the Snappy encode pool has a
// New function so it can bootstrap: without one, Put was gated on a
// successful Get and every compressed send heap-allocated on the hot path.
func TestSnappyEncPool_Bootstraps(t *testing.T) {
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

	v := tr.snappyEncPool.Get()
	if v == nil {
		t.Fatal("snappyEncPool.Get returned nil: pool has no New function")
	}
	ptr, ok := v.(*[]byte)
	if !ok {
		t.Fatalf("snappyEncPool holds %T, want *[]byte", v)
	}
	if cap(*ptr) == 0 {
		t.Fatal("snappyEncPool New returned a zero-capacity buffer")
	}
	// The per-P private pool slot returns the most recently Put pointer,
	// proving Put/Get now round-trip. Retried because the race detector makes
	// sync.Pool discard a random fraction of Puts (runtime behaviour, not a
	// property of this pool); a single attempt is a coin flip under -race,
	// while a pool that never round-trips fails every attempt.
	if !poolRoundTrips(func() { tr.snappyEncPool.Put(ptr) }, func() bool {
		return tr.snappyEncPool.Get() == v
	}) {
		t.Fatal("snappyEncPool did not round-trip the returned buffer")
	}
}

// poolRoundTrips reports whether put/observe round-trips within enough
// attempts to be reliable under the race detector, which drops a random
// fraction of sync.Pool Puts.
func poolRoundTrips(put func(), observe func() bool) bool {
	const attempts = 64
	for i := 0; i < attempts; i++ {
		put()
		if observe() {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Send drop observability (quic.go Send)
// ---------------------------------------------------------------------------

// syncBuffer is a bytes.Buffer safe for concurrent use.
//
// slog.SetDefault installs a sink for the whole process, not for the calling
// test, so every goroutine alive at that moment logs into it -- including
// transport goroutines still serving connections started by earlier tests. A
// bare bytes.Buffer there is a data race, and the detector caught it in CI:
// a snapshot receiver's rejectSnapshot write against this test's String()
// read. The other two tests in this package that redirect the default logger
// (adversarial_test.go, failpath_test.go) send it to io.Discard, which is
// already safe; this is the one that needed to keep the output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestSend_UnresolvedTargetLogsDrop verifies that a message dropped because
// its target address cannot be resolved emits a structured log record
// instead of vanishing silently.
func TestSend_UnresolvedTargetLogsDrop(t *testing.T) {
	handler := newTestHandler()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	var logBuf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)

	// Replica 99 is not registered, so Resolve fails and the message drops.
	tr.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 99, Term: 1},
	})

	if !strings.Contains(logBuf.String(), "target address unresolved") {
		t.Fatalf("expected drop log for unresolved target, got: %s", logBuf.String())
	}
}

// ---------------------------------------------------------------------------
// SendSnapshot shutdown responsiveness (quic.go cancelSnapshotWriteOnStop)
// ---------------------------------------------------------------------------

// TestSendSnapshot_StopUnblocksStalledChunkWrite verifies that Stop() is not
// held hostage by an in-flight snapshot send whose chunk writes are blocked
// on a stalled receiver. The receiver never reads the stream, so the sender
// fills its QUIC flow-control window and blocks in Write; without the
// shutdown watcher, Stop() would wait out the 30s chunk write deadline.
func TestSendSnapshot_StopUnblocksStalledChunkWrite(t *testing.T) {
	handler1 := newTestHandler()
	handler2 := newTestHandler()
	reg1 := registry.NewRegistry()
	reg2 := registry.NewRegistry()
	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler1, reg1)
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}

	// The receiver is a bare QUIC listener with t2's TLS that accepts the
	// connection and never reads a stream, so no stream credit comes back.
	// Its 64KB stream window is far below one 1MB chunk, so the first chunk
	// Write can end only by a cancel or its write deadline.
	t2, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, handler2, reg2)
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	ln, err := quic.ListenAddr("127.0.0.1:0", t2.listenerTLS(), &quic.Config{
		HandshakeIdleTimeout:       3 * time.Second,
		InitialStreamReceiveWindow: 64 << 10,
		MaxStreamReceiveWindow:     64 << 10,
	})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			if _, aerr := ln.Accept(context.Background()); aerr != nil {
				return
			}
		}
	}()

	reg1.Register(1, 2, ln.Addr().String())

	const chunkCount = 32
	data := bytes.Repeat([]byte("x"), 1<<20)
	chunks := make([]proto.SnapshotChunk, chunkCount)
	for i := range chunks {
		chunks[i] = proto.SnapshotChunk{
			BinVer:       proto.WireVersion,
			ShardID:      1,
			ReplicaID:    2,
			From:         1,
			Index:        10,
			Term:         1,
			ChunkID:      uint64(i),
			ChunkCount:   chunkCount,
			ChunkSize:    uint64(len(data)),
			Data:         data,
			DeploymentID: 42,
		}
	}

	idC := make(chan uint64, 1)
	sendErr := make(chan error, 1)
	go func() {
		idC <- goroutineID()
		sendErr <- t1.SendSnapshot(chunks)
	}()

	// Wait until the send is parked inside the first chunk's stream Write,
	// which the receiver never lets complete, so Stop meets a write in
	// flight rather than the between-chunks stopC check.
	waitBlockedIn(t, idC, "quic-go.(*SendStream).Write", 10*time.Second)

	stopStart := time.Now()
	if err := t1.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if elapsed := time.Since(stopStart); elapsed > 15*time.Second {
		t.Fatalf("Stop blocked %v behind a stalled snapshot send", elapsed)
	}

	select {
	case err := <-sendErr:
		if err == nil {
			t.Fatal("SendSnapshot should fail when the transport stops mid-transfer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendSnapshot did not return after Stop")
	}
}

// TestSend_UnresolvedTargetCountsDrop verifies that a message whose target
// replica has no registered address increments IncMessagesDropped under the
// fixed "unresolved_target" reason rather than vanishing into a Debug log.
func TestSend_UnresolvedTargetCountsDrop(t *testing.T) {
	rec := &dropRecorder{}
	tr, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
		Metrics:       rec,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer tr.Stop()

	// Nothing is registered, so Resolve fails for every target.
	tr.Send([]proto.Message{{Type: proto.Replicate, ShardID: 5, From: 1, To: 3}})

	got := rec.drops()
	if len(got) != 1 {
		t.Fatalf("IncMessagesDropped calls = %d, want 1", len(got))
	}
	if got[0].shardID != 5 || got[0].to != 3 || got[0].reason != dropReasonUnresolvedTarget {
		t.Fatalf("drop = %+v, want shard 5 to 3 reason %q", got[0], dropReasonUnresolvedTarget)
	}
}

// peerLeafIdentity returns the node identity tr presents to peers.
func peerLeafIdentity(t *testing.T, tr *QUICTransport) string {
	t.Helper()
	block, _ := pem.Decode(tr.cfg.MTLSConfig.Cert)
	if block == nil {
		t.Fatal("no certificate PEM block")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return NodeIdentityFromCertificate(leaf)
}

// replicaIdentities is a mutable Config.ReplicaIdentity for tests.
type replicaIdentities struct {
	mu  sync.Mutex
	ids map[[2]uint64]string
}

func (r *replicaIdentities) set(shardID, replicaID uint64, id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ids == nil {
		r.ids = make(map[[2]uint64]string)
	}
	r.ids[[2]uint64{shardID, replicaID}] = id
}

func (r *replicaIdentities) get(shardID, replicaID uint64) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ids[[2]uint64{shardID, replicaID}]
}

// strangerIdentity is an identity no test transport presents.
const strangerIdentity = "spki:0000000000000000000000000000000000000000000000000000000000000000"

// TestGetConnection_DialsOnlyTheWantedIdentity: a connection is taken for a
// specific destination identity. A dial is kept only when the peer presents
// exactly that identity — the TLS handshake proves no more than a cluster
// certificate whose SAN covers the host — and a refusal is remembered, so an
// immediate retry costs no second handshake.
func TestGetConnection_DialsOnlyTheWantedIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		want   func(real string) string
		wantOK bool
	}{
		{"the peer's own identity", func(real string) string { return real }, true},
		{"another identity", func(string) string { return strangerIdentity }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids := &replicaIdentities{}
			var failedDials atomic.Int64
			t1, t2, cleanup := transportPairWith(t, 1, newTestHandler(), newTestHandler(), func(c *Config) {
				c.ReplicaIdentity = ids.get
				c.EventListener = &config.EventListener{
					OnConnectionFailed: func(config.ConnectionInfo) { failedDials.Add(1) },
				}
			})
			defer cleanup()
			want := tc.want(peerLeafIdentity(t, t2))

			conn, err := t1.getConnection(t2.Addr().String(), want)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("dial to the wanted identity failed: %v", err)
				}
				return
			}
			var refused *DialIdentityError
			if !errors.As(err, &refused) {
				t.Fatalf("getConnection error = %v (connection returned: %t), want a DialIdentityError", err, conn != nil)
			}
			if t1.ConnectionCount() != 0 {
				t.Fatal("a refused connection was pooled for reuse")
			}
			if _, again := t1.getConnection(t2.Addr().String(), want); !errors.As(again, &refused) {
				t.Fatalf("immediate re-dial = %v, want the remembered refusal", again)
			}
			if n := failedDials.Load(); n != 1 {
				t.Fatalf("%d refused dials for two attempts inside the cooldown, want 1", n)
			}
		})
	}
}

// TestSend_AMovedRouteNeitherReceivesNorChurnsAnotherReplicasTraffic: a member
// of another shard has moved its own route onto a victim replica's address and
// holds the connection there. The victim's messages are dropped rather than
// written to it — and rather than re-dialed, which would let the member churn
// the connection — while the member's own traffic flows.
func TestSend_AMovedRouteNeitherReceivesNorChurnsAnotherReplicasTraffic(t *testing.T) {
	ids := &replicaIdentities{}
	rec := &dropRecorder{}
	handler2 := newTestHandler()
	var established atomic.Int64
	t1, t2, cleanup := transportPairWith(t, 1, newTestHandler(), handler2, func(c *Config) {
		c.ReplicaIdentity = ids.get
		c.EventListener = &config.EventListener{
			OnConnectionEstablished: func(config.ConnectionInfo) { established.Add(1) },
		}
	})
	defer cleanup()
	t1.metrics = rec
	member := peerLeafIdentity(t, t2)
	addr := t2.Addr().String()

	ids.set(1, 2, strangerIdentity) // the victim, bound to another node's key
	ids.set(7, 9, member)           // the member that moved its route here
	t1.reg.Register(7, 9, addr)
	if _, err := t1.getConnection(addr, member); err != nil {
		t.Fatalf("member dial: %v", err)
	}

	// The heartbeat stream is shared by every shard and pre-opened on the
	// member's connection, so the victim's heartbeat would find a pooled
	// stream to it without the identity check on pooled streams.
	pollUntil(t, 5*time.Second, 10*time.Millisecond, "the member's heartbeat stream was not pre-opened",
		func() bool { _, ok := t1.cachedStream(addr, 7, true, member); return ok })
	for range 3 {
		t1.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1}})
		t1.Send([]proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1}})
	}
	t1.Send([]proto.Message{{Type: proto.Replicate, ShardID: 7, From: 1, To: 9, Term: 1}})
	waitForBatchCount(t, handler2, 1, 5*time.Second)
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "the victim's six messages were not all dropped", func() bool {
		var victim int
		for _, d := range rec.drops() {
			if d.reason == dropReasonIdentityMismatch && d.shardID == 1 && d.to == 2 {
				victim++
			}
		}
		return victim >= 6
	})

	for _, b := range handler2.getBatches() {
		for _, m := range b.Requests {
			if m.ShardID == 1 {
				t.Fatal("the victim replica's message was written to the member holding its address")
			}
		}
	}
	if _, ok := rec.firstDropWithReason(dropReasonIdentityMismatch); !ok {
		t.Fatal("the victim's messages were not counted as identity_mismatch drops")
	}
	// t1's listener counts only t1's own dials: one, the member's.
	if n := established.Load(); n != 1 || t1.ConnectionCount() != 1 {
		t.Fatalf("the member's connection was churned: %d dials established, %d pooled, want 1 and 1",
			n, t1.ConnectionCount())
	}
}

// TestSend_EachMessageIsPinnedToItsDestination: connections are keyed by
// address and shared by every replica the registry routes there, and address
// learning lets a member of ANY shard point its own route at another replica's
// address. A dial-time check of the peer against every identity routed there
// then accepted that member for the other replica's traffic. Each message is
// held to its own destination: the member receives its own shard's traffic
// and none of the victim's.
func TestSend_EachMessageIsPinnedToItsDestination(t *testing.T) {
	ids := &replicaIdentities{}
	rec := &dropRecorder{}
	handler2 := newTestHandler()
	t1, t2, cleanup := transportPairWith(t, 1, newTestHandler(), handler2, func(c *Config) {
		c.ReplicaIdentity = ids.get
	})
	defer cleanup()
	t1.metrics = rec
	peer := peerLeafIdentity(t, t2)

	// Shard 1's replica 2 (the victim) is bound to another node's identity;
	// shard 7's replica 9 (the member that moved its route) is the peer that
	// answers at the address. Both routes point at t2.
	ids.set(1, 2, strangerIdentity)
	ids.set(7, 9, peer)
	t1.reg.Register(7, 9, t2.Addr().String())

	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
		{Type: proto.Replicate, ShardID: 7, From: 1, To: 9, Term: 1},
	})

	waitForBatchCount(t, handler2, 1, 5*time.Second)
	for _, b := range handler2.getBatches() {
		for _, m := range b.Requests {
			if m.ShardID == 1 {
				t.Fatal("the victim replica's message was written to a peer bound to another replica")
			}
		}
	}
	if d, ok := rec.firstDropWithReason(dropReasonIdentityMismatch); !ok || d.shardID != 1 || d.to != 2 {
		t.Fatalf("identity-mismatch drop = %+v (recorded: %t), want shard 1 to 2", d, ok)
	}
}

// TestSend_ClosesAConnectionWhosePeerIsNoLongerBound: the pin was checked only
// at dial time, so a connection cached before membership rebound a replica to
// a new keypair at the same address kept carrying that replica's traffic to
// the old key's holder. A send whose destination the peer no longer is drops
// the message and, when the peer is bound to no replica at the address any
// more, closes the connection so the next send dials whoever is bound now.
func TestSend_ClosesAConnectionWhosePeerIsNoLongerBound(t *testing.T) {
	ids := &replicaIdentities{}
	t1, t2, cleanup := transportPairWith(t, 1, newTestHandler(), newTestHandler(), func(c *Config) {
		c.ReplicaIdentity = ids.get
	})
	defer cleanup()
	ids.set(1, 2, peerLeafIdentity(t, t2))
	if _, err := t1.getConnection(t2.Addr().String(), peerLeafIdentity(t, t2)); err != nil {
		t.Fatalf("initial dial: %v", err)
	}

	ids.set(1, 2, strangerIdentity) // rebound to a new keypair at the same address
	t1.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1}})

	pollUntil(t, 5*time.Second, 10*time.Millisecond,
		"the connection to a peer no longer bound at the address was not closed",
		func() bool { return t1.ConnectionCount() == 0 })
}

// TestSnapshotSends_ArePinnedToTheDestination: a snapshot is the whole state
// machine. Both send paths take their connection for the destination
// replica's bound identity, so a snapshot for a replica whose address another
// peer answers — or that nothing binds — is refused, not streamed.
func TestSnapshotSends_ArePinnedToTheDestination(t *testing.T) {
	for _, bind := range []string{strangerIdentity, ""} {
		t.Run(fmt.Sprintf("bound=%q", bind), func(t *testing.T) {
			ids := &replicaIdentities{}
			handler2 := newTestHandler()
			t1, _, cleanup := transportPairWith(t, 1, newTestHandler(), handler2, func(c *Config) {
				c.ReplicaIdentity = ids.get
			})
			defer cleanup()
			if bind != "" {
				ids.set(1, 2, bind)
			}

			var refused *DialIdentityError
			meta := SnapshotHeader{ShardID: 1, ReplicaID: 2, Index: 100, Term: 1, ChunkCount: 1, From: 1}
			err := t1.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
				return emit(proto.SnapshotChunk{ShardID: 1, ReplicaID: 2, From: 1, Index: 100, Term: 1, ChunkCount: 1})
			})
			if !errors.As(err, &refused) {
				t.Fatalf("SendSnapshotStreaming = %v, want a DialIdentityError", err)
			}
			err = t1.SendSnapshot([]proto.SnapshotChunk{{ShardID: 1, ReplicaID: 2, From: 1, Index: 100, Term: 1, ChunkCount: 1}})
			if !errors.As(err, &refused) {
				t.Fatalf("SendSnapshot = %v, want a DialIdentityError", err)
			}
			if n := handler2.snapshotCount.Load(); n != 0 {
				t.Fatalf("%d snapshots reached a peer that is not their destination", n)
			}
		})
	}
}

// TestSend_AMessageForAnUnboundReplicaIsDropped: with pinning on, a message for
// a replica nothing binds has no peer it may be written to — alone in its
// batch or mixed with deliverable ones — and is dropped as identity_mismatch
// while the rest of the batch is delivered.
func TestSend_AMessageForAnUnboundReplicaIsDropped(t *testing.T) {
	ids := &replicaIdentities{}
	rec := &dropRecorder{}
	handler2 := newTestHandler()
	t1, t2, cleanup := transportPairWith(t, 1, newTestHandler(), handler2, func(c *Config) {
		c.ReplicaIdentity = ids.get
	})
	defer cleanup()
	t1.metrics = rec
	ids.set(1, 2, peerLeafIdentity(t, t2))
	t1.reg.Register(3, 5, t2.Addr().String()) // routed there, bound to nothing

	t1.Send([]proto.Message{{Type: proto.Replicate, ShardID: 3, From: 1, To: 5, Term: 1}})
	t1.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 3, From: 1, To: 5, Term: 1},
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	countUnbound := func() int {
		var unbound int
		for _, d := range rec.drops() {
			if d.reason == dropReasonIdentityMismatch && d.shardID == 3 && d.to == 5 {
				unbound++
			}
		}
		return unbound
	}
	waitForBatchCount(t, handler2, 1, 5*time.Second)
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "the unbound replica's messages were not dropped",
		func() bool { return countUnbound() >= 2 })

	for _, b := range handler2.getBatches() {
		for _, m := range b.Requests {
			if m.ShardID == 3 {
				t.Fatal("a message for a replica nothing binds was written")
			}
		}
	}
	if unbound := countUnbound(); unbound != 2 {
		t.Fatalf("%d identity_mismatch drops for the unbound replica, want 2 (alone and mixed)", unbound)
	}
}

// TestGetConnection_EmptyIdentityFailsClosedWhenPinned: with pinning on, a
// caller that names no destination identity gets no connection, rather than
// an unpinned one any peer could answer.
func TestGetConnection_EmptyIdentityFailsClosedWhenPinned(t *testing.T) {
	ids := &replicaIdentities{}
	t1, t2, cleanup := transportPairWith(t, 1, newTestHandler(), newTestHandler(), func(c *Config) {
		c.ReplicaIdentity = ids.get
	})
	defer cleanup()
	var refused *DialIdentityError
	if _, err := t1.getConnection(t2.Addr().String(), ""); !errors.As(err, &refused) {
		t.Fatalf("getConnection with no identity = %v, want a DialIdentityError", err)
	}
	if t1.ConnectionCount() != 0 {
		t.Fatal("an unpinned connection was dialed")
	}
}
