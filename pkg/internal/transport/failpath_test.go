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
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

// withDebugLogging routes slog to a Debug-level discard handler for the
// duration of the test, so the drop paths that gate their diagnostics on
// debugEnabled() execute the gated branch. The previous logger is restored.
func withDebugLogging(t *testing.T) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

// newDropTransport builds a started transport whose metrics recorder captures
// every per-message drop, so tests can assert the exact reason bucket a drop
// path attributes. opts mutate the config before construction.
func newDropTransport(t *testing.T, opts ...func(*Config)) (*QUICTransport, *dropRecorder) {
	t.Helper()
	rec := &dropRecorder{}
	cfg := Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
		Metrics:       rec,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	tr, err := NewQUICTransport(cfg, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() { _ = tr.Stop() })
	return tr, rec
}

// ---------------------------------------------------------------------------
// Heartbeat fast-path drop accounting
// ---------------------------------------------------------------------------

// TestSendHeartbeatBatch_MarshalFailureDropsBatch covers the defensive guard in
// the heartbeat fast path. The frame buffer is sized for FIXED
// HeartbeatMessageSize records, so a heartbeat carrying a variable-length
// payload that slipped past isHeartbeatBatch must be dropped loudly and
// accounted, never marshalled into an undersized slot (which would corrupt the
// frame for every OTHER heartbeat in the same batch).
func TestSendHeartbeatBatch_MarshalFailureDropsBatch(t *testing.T) {
	tr, rec := newDropTransport(t)

	// A Heartbeat carrying entries does not fit a fixed-size slot.
	msgs := []proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2},
		{
			Type: proto.Heartbeat, ShardID: 1, From: 1, To: 3,
			Entries: []proto.Entry{{Index: 1, Term: 1, Cmd: bytes.Repeat([]byte("x"), 512)}},
		},
	}
	tr.sendHeartbeatBatch("127.0.0.1:1", msgs)

	drops := rec.drops()
	if len(drops) != len(msgs) {
		t.Fatalf("recorded %d drops, want one per message (%d): the counter must measure messages, not batches",
			len(drops), len(msgs))
	}
	for _, d := range drops {
		if d.reason != dropReasonMarshalFailed {
			t.Fatalf("drop reason = %q, want %q", d.reason, dropReasonMarshalFailed)
		}
	}
	if drops[0].to != 2 || drops[1].to != 3 {
		t.Fatalf("drops attributed to targets (%d, %d), want (2, 3)", drops[0].to, drops[1].to)
	}
}

// TestSendHeartbeatBatch_StreamUnavailableDropsBatch covers the unreachable-peer
// path with Debug logging on, so the gated diagnostic branch executes. Raft
// retransmits on the next tick, but the drop must still be attributed.
func TestSendHeartbeatBatch_StreamUnavailableDropsBatch(t *testing.T) {
	withDebugLogging(t)
	tr, rec := newDropTransport(t)

	msgs := []proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2}}
	// A syntactically valid but unresolvable target: the dial fails, so no
	// stream can be opened.
	tr.sendHeartbeatBatch("no-such-host.invalid:1234", msgs)

	drops := rec.drops()
	if len(drops) != 1 {
		t.Fatalf("recorded %d drops, want 1", len(drops))
	}
	if drops[0].reason != dropReasonStreamUnavailable {
		t.Fatalf("drop reason = %q, want %q", drops[0].reason, dropReasonStreamUnavailable)
	}
}

// TestSendHeartbeatBatch_WriteFailureEvictsConnection pins the recovery
// behaviour after a heartbeat write fails on a dead connection: the batch is
// accounted as a write failure AND the connection plus every stream cached on
// it is evicted. Without the eviction, each remaining cached stream would hang
// for the full write deadline on the next send, compounding latency across
// every message type until the connection finally aged out.
func TestSendHeartbeatBatch_WriteFailureEvictsConnection(t *testing.T) {
	withDebugLogging(t)
	rec := &dropRecorder{}
	t1, t2, cleanup := transportPairWith(t, 42, newTestHandler(), newTestHandler(),
		func(c *Config) { c.Metrics = rec })
	defer cleanup()

	target := t2.Addr().String()
	// Cache a heartbeat stream (index 0) plus a data stream on this connection.
	hbStream, err := t1.getStream(target, 1, true)
	if err != nil {
		t.Fatalf("getStream(heartbeat) failed: %v", err)
	}
	if _, err := t1.getStream(target, 1, false); err != nil {
		t.Fatalf("getStream(data) failed: %v", err)
	}
	if got := t1.StreamCount(); got < 2 {
		t.Fatalf("StreamCount = %d, want at least 2; the eviction assertion would be vacuous", got)
	}

	// Cancel the heartbeat stream's send side while leaving the connection
	// alive, so getStream returns this cached stream and the write on it fails
	// immediately and deterministically.
	hbStream.CancelWrite(1)

	msgs := []proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2},
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2},
	}
	t1.sendHeartbeatBatch(target, msgs)

	drops := rec.drops()
	if len(drops) != len(msgs) {
		t.Fatalf("recorded %d drops, want %d (one per message)", len(drops), len(msgs))
	}
	for _, d := range drops {
		if d.reason != dropReasonWriteFailed {
			t.Fatalf("drop reason = %q, want %q", d.reason, dropReasonWriteFailed)
		}
	}
	if got := t1.StreamCount(); got != 0 {
		t.Fatalf("StreamCount = %d after a heartbeat write failure, want 0: "+
			"stale streams on the dead connection were not evicted", got)
	}
	t1.connMu.RLock()
	_, cached := t1.conns[target]
	t1.connMu.RUnlock()
	if cached {
		t.Fatal("dead connection still cached after a heartbeat write failure")
	}
}

// ---------------------------------------------------------------------------
// sendBatch drop accounting
// ---------------------------------------------------------------------------

// TestSendBatch_StreamUnavailableDropsBatch covers the general send path's
// unreachable-peer branch (Debug gate included).
func TestSendBatch_StreamUnavailableDropsBatch(t *testing.T) {
	withDebugLogging(t)
	tr, rec := newDropTransport(t)

	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 4, From: 1, To: 2, Term: 3},
		{Type: proto.Replicate, ShardID: 4, From: 1, To: 5, Term: 3},
	}
	tr.sendBatch("no-such-host.invalid:1234", msgs)

	drops := rec.drops()
	if len(drops) != len(msgs) {
		t.Fatalf("recorded %d drops, want %d (one per message)", len(drops), len(msgs))
	}
	for _, d := range drops {
		if d.reason != dropReasonStreamUnavailable {
			t.Fatalf("drop reason = %q, want %q", d.reason, dropReasonStreamUnavailable)
		}
		if d.shardID != 4 {
			t.Fatalf("drop shard = %d, want 4", d.shardID)
		}
	}
}

// TestSendBatch_OversizedFrameDropsBatch covers the ErrPayloadTooLarge branch.
// This is a producer defect with a silent-quorum-degradation failure mode:
// raft sizes replication batches against MaxApplyEntrySize, and if that exceeds
// the frame cap the message is dropped WITHOUT advancing the follower's next
// index, so raft rebuilds the identical oversized message forever and the
// follower can never catch up. The drop must at minimum be attributed so the
// condition is observable rather than silent.
//
// The message is refused by the pre-write sendability check
// (dropReasonUnsendable), not by MarshalFrame: an unacceptable frame must
// never reach the wire, because the peer's rejection of it is indistinguishable
// at the sender from a broken connection.
func TestSendBatch_OversizedFrameDropsBatch(t *testing.T) {
	tr, rec := newDropTransport(t)

	// Incompressible payload just past MaxFrameSize, so Snappy cannot shrink
	// it back under the cap and MarshalFrame must reject it.
	cmd := make([]byte, int(MaxFrameSize)+64*1024)
	if _, err := rand.Read(cmd); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}
	msgs := []proto.Message{{
		Type:    proto.Replicate,
		ShardID: 9,
		From:    1,
		To:      2,
		Term:    4,
		Entries: []proto.Entry{{Index: 1, Term: 4, Cmd: cmd}},
	}}

	tr.sendBatch("127.0.0.1:1", msgs)

	drops := rec.drops()
	if len(drops) != 1 {
		t.Fatalf("recorded %d drops, want 1", len(drops))
	}
	if drops[0].reason != dropReasonUnsendable {
		t.Fatalf("drop reason = %q, want %q", drops[0].reason, dropReasonUnsendable)
	}
	if drops[0].shardID != 9 || drops[0].to != 2 {
		t.Fatalf("drop attributed to shard %d / replica %d, want 9 / 2", drops[0].shardID, drops[0].to)
	}

	// The oversized batch must not have been written anywhere: no connection
	// was even dialled for it.
	if got := tr.ConnectionCount(); got != 0 {
		t.Fatalf("ConnectionCount = %d; an oversized batch must be rejected before dialling", got)
	}
}

// ---------------------------------------------------------------------------
// Snapshot metadata ownership
// ---------------------------------------------------------------------------

// TestCopySnapshotRefs_DetachesEveryReference pins the ownership contract of
// the asynchronous send path: SendSnapshot hands snapshot metadata to a
// background marshal, so every reference-typed field must be copied. If any
// field still aliased the caller's memory, a caller mutating its membership maps
// after the call would race the marshal and could put a DIFFERENT membership on
// the wire than the one raft committed.
//
// The two AUTHORIZATION maps are covered explicitly. Identities was missed when
// copySnapshotRefs was written and RemovedIdentities arrived later; a snapshot
// whose identity map is mutated between the enqueue and the marshal ships a
// configuration that authorizes a different set of replicas than the caller
// submitted, which is a strictly worse outcome than a stale address.
func TestCopySnapshotRefs_DetachesEveryReference(t *testing.T) {
	orig := proto.Snapshot{
		Index: 7,
		Term:  2,
		Membership: proto.Membership{
			Addresses:         map[uint64]string{1: "a:1"},
			Observers:         map[uint64]string{2: "b:2"},
			Witnesses:         map[uint64]string{3: "c:3"},
			Removed:           map[uint64]bool{4: true},
			Identities:        map[uint64]string{1: "spki:aa"},
			RemovedIdentities: map[uint64]string{4: "spki:dd"},
		},
	}
	copied := orig
	copySnapshotRefs(&copied)

	// Mutate every source reference; the copy must be unaffected.
	orig.Membership.Addresses[1] = "mutated"
	orig.Membership.Observers[2] = "mutated"
	orig.Membership.Witnesses[3] = "mutated"
	orig.Membership.Removed[4] = false
	orig.Membership.Identities[1] = "spki:mutated"
	orig.Membership.RemovedIdentities[4] = "spki:mutated"

	if copied.Membership.Addresses[1] != "a:1" {
		t.Error("Membership.Addresses still aliases the caller's map")
	}
	if copied.Membership.Observers[2] != "b:2" {
		t.Error("Membership.Observers still aliases the caller's map")
	}
	if copied.Membership.Witnesses[3] != "c:3" {
		t.Error("Membership.Witnesses still aliases the caller's map")
	}
	if !copied.Membership.Removed[4] {
		t.Error("Membership.Removed still aliases the caller's map")
	}
	if copied.Membership.Identities[1] != "spki:aa" {
		t.Error("Membership.Identities still aliases the caller's map: a mutation " +
			"after the enqueue would put a different authorization set on the wire")
	}
	if copied.Membership.RemovedIdentities[4] != "spki:dd" {
		t.Error("Membership.RemovedIdentities still aliases the caller's map: a " +
			"mutation after the enqueue would put a different removal deny-list " +
			"on the wire")
	}
}

// TestCopySnapshotRefs_PreservesNilDistinction pins the wire-format detail that
// a nil map and an empty map are NOT interchangeable: the encoder distinguishes
// them, so materialising empty maps here would change the bytes on the wire.
func TestCopySnapshotRefs_PreservesNilDistinction(t *testing.T) {
	s := proto.Snapshot{Index: 1}
	copySnapshotRefs(&s)

	if s.Membership.Addresses != nil {
		t.Error("nil Addresses map was materialised into a non-nil map")
	}
	if s.Membership.Observers != nil {
		t.Error("nil Observers map was materialised into a non-nil map")
	}
	if s.Membership.Witnesses != nil {
		t.Error("nil Witnesses map was materialised into a non-nil map")
	}
	if s.Membership.Removed != nil {
		t.Error("nil Removed map was materialised into a non-nil map")
	}

	// An empty (non-nil) map must survive as empty and non-nil.
	s2 := proto.Snapshot{Membership: proto.Membership{Addresses: map[uint64]string{}}}
	copySnapshotRefs(&s2)
	if s2.Membership.Addresses == nil {
		t.Error("empty Addresses map was collapsed to nil")
	}
	if len(s2.Membership.Addresses) != 0 {
		t.Error("empty Addresses map gained entries")
	}
}

// ---------------------------------------------------------------------------
// Fail-closed connection and authorization guards
// ---------------------------------------------------------------------------

// TestDialConnection_RefusesInsertAfterStop covers the shutdown race guard: a
// dial that completes after Stop has flipped the stopped flag must close the
// fresh connection and refuse, rather than inserting it into a pool that Stop
// has already drained (which leaks a keep-alive'd connection past shutdown).
func TestDialConnection_RefusesInsertAfterStop(t *testing.T) {
	t1, t2, cleanup := transportPairWith(t, 42, newTestHandler(), newTestHandler())
	defer cleanup()

	target := t2.Addr().String()
	// Flip the flag Stop sets, without cancelling shutdownCtx, so the dial
	// itself succeeds and the post-dial guard is the branch under test.
	t1.stopped.Store(true)

	conn, err := t1.dialConnection(target)
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("dialConnection after stop = (%v, %v), want ErrStreamClosed", conn, err)
	}
	if conn != nil {
		t.Fatal("dialConnection returned a connection after stop")
	}
	if got := t1.ConnectionCount(); got != 0 {
		t.Fatalf("ConnectionCount = %d; a post-stop dial must not be inserted into the pool", got)
	}

	t1.stopped.Store(false)
}

// TestInboundAuthorizedFor_FailsClosed pins the authorization gate that decides
// whether an in-band, untrusted SourceAddress may mutate connection state. Every
// case where the claim cannot be tied to the peer's verified certificate must
// return false: a permissive answer would let an authenticated peer evict
// another node's outbound connection by claiming its address.
func TestInboundAuthorizedFor_FailsClosed(t *testing.T) {
	t1, conn := liveConn(t)

	if t1.inboundAuthorizedFor(nil, "127.0.0.1:1234") {
		t.Error("nil connection (handshake incomplete) must not authorize any address")
	}
	// No port: SplitHostPort fails and the whole target is used as the host,
	// which the peer certificate does not cover.
	if t1.inboundAuthorizedFor(conn, "not-in-the-cert") {
		t.Error("an address outside the peer certificate must not be authorized")
	}
	// Empty host.
	if t1.inboundAuthorizedFor(conn, ":1234") {
		t.Error("an empty host must not be authorized")
	}
	if t1.inboundAuthorizedFor(conn, "") {
		t.Error("an empty target must not be authorized")
	}
}

// ---------------------------------------------------------------------------
// Configuration and advertisement
// ---------------------------------------------------------------------------

// TestConfigValidate_RejectsZeroMessageReceiveMemory pins the fail-closed
// validation of the aggregate receive budget. A zero budget would disable the
// OOM guard entirely rather than meaning "unlimited".
func TestConfigValidate_RejectsZeroMessageReceiveMemory(t *testing.T) {
	cfg := Config{ListenAddress: "127.0.0.1:0", MTLSConfig: testMTLSConfig(t)}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaulted config must validate, got %v", err)
	}
	cfg.MaxMessageReceiveMemory = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidMaxMessageReceiveMemory) {
		t.Fatalf("Validate with zero MaxMessageReceiveMemory = %v, want ErrInvalidMaxMessageReceiveMemory", err)
	}
	cfg.MaxMessageReceiveMemory = -1
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidMaxMessageReceiveMemory) {
		t.Fatalf("Validate with negative MaxMessageReceiveMemory = %v, want ErrInvalidMaxMessageReceiveMemory", err)
	}
}

// TestLocalAddr_AdvertiseAddressWins pins the NAT/container advertisement rule:
// a configured AdvertiseAddress takes priority over the OS-reported socket
// address, which for a wildcard bind would otherwise put 0.0.0.0 on the wire as
// this node's reachable address.
func TestLocalAddr_AdvertiseAddressWins(t *testing.T) {
	tr, _ := newDropTransport(t, func(c *Config) { c.AdvertiseAddress = "raft.example:9999" })
	if got := tr.localAddr(); got != "raft.example:9999" {
		t.Fatalf("localAddr = %q, want the configured AdvertiseAddress", got)
	}
	if got := tr.Addr().String(); got == "raft.example:9999" {
		t.Fatal("test setup: the bound socket address must differ from the advertised one")
	}
}

// TestSendBatch_WriteFailureEvictsConnection is the general-path counterpart to
// the heartbeat write-failure test: a failed write must both account the drop
// and evict the connection with every stream cached on it. Leaving the dead
// streams cached makes every subsequent send of ANY message type hang for the
// full write deadline before failing.
func TestSendBatch_WriteFailureEvictsConnection(t *testing.T) {
	withDebugLogging(t)
	rec := &dropRecorder{}
	t1, t2, cleanup := transportPairWith(t, 42, newTestHandler(), newTestHandler(),
		func(c *Config) { c.Metrics = rec })
	defer cleanup()

	target := t2.Addr().String()
	dataStream, err := t1.getStream(target, 4, false)
	if err != nil {
		t.Fatalf("getStream failed: %v", err)
	}
	if _, err := t1.getStream(target, 4, true); err != nil {
		t.Fatalf("getStream(heartbeat) failed: %v", err)
	}
	if got := t1.StreamCount(); got < 2 {
		t.Fatalf("StreamCount = %d, want at least 2", got)
	}

	// Cancel only this stream's send side: the connection stays alive, so
	// getStream returns the cached stream and the write fails deterministically.
	dataStream.CancelWrite(1)

	msgs := []proto.Message{
		{Type: proto.Replicate, ShardID: 4, From: 1, To: 2, Term: 3},
		{Type: proto.Replicate, ShardID: 4, From: 1, To: 2, Term: 3},
	}
	t1.sendBatch(target, msgs)

	drops := rec.drops()
	if len(drops) != len(msgs) {
		t.Fatalf("recorded %d drops, want %d (one per message)", len(drops), len(msgs))
	}
	for _, d := range drops {
		if d.reason != dropReasonWriteFailed {
			t.Fatalf("drop reason = %q, want %q", d.reason, dropReasonWriteFailed)
		}
	}
	if got := t1.StreamCount(); got != 0 {
		t.Fatalf("StreamCount = %d after a write failure, want 0: stale streams were not evicted", got)
	}
	t1.connMu.RLock()
	_, cached := t1.conns[target]
	t1.connMu.RUnlock()
	if cached {
		t.Fatal("dead connection still cached after a stream write failure")
	}
}

// TestMsgRecvBudget_PerConnectionCompressedDeltaRejected covers the
// per-connection sub-budget applied to the DECOMPRESSION delta. The aggregate
// check alone is not enough: one peer with several compressed frames in flight
// could pin the whole host budget and force every other peer's frames into the
// exhaustion path. The rejection must also be exactly balanced — a leaked
// reservation would permanently shrink that connection's budget.
func TestMsgRecvBudget_PerConnectionCompressedDeltaRejected(t *testing.T) {
	tr, conn := liveConn(t)

	// Leave only a small slice of this connection's sub-budget available, as
	// concurrent in-flight frames from the same peer would.
	const headroom = int64(16 * 1024)
	preReserve := tr.maxConnMsgRecv - headroom
	if preReserve <= 0 {
		t.Fatalf("test setup: per-connection budget %d is too small", tr.maxConnMsgRecv)
	}
	if !tr.reserveConnMsgRecvBudget(conn, preReserve) {
		t.Fatal("test setup: pre-reservation of the per-connection budget failed")
	}

	// 64KB of zeros: the compressed frame fits in the remaining headroom, but
	// the declared decompressed size does not.
	raw := make([]byte, 64*1024)
	compressed := snappyEncode(nil, raw)
	if int64(len(compressed)) >= headroom {
		t.Fatalf("test setup: compressed length %d must fit the headroom %d", len(compressed), headroom)
	}

	aggregateBefore := tr.msgRecvBudget.Load()
	err := tr.handleMessageFrame(conn, bytes.NewReader(compressed), uint32(len(compressed)), FlagCompressed)
	if !errors.Is(err, ErrMessageRecvMemoryBudgetExceeded) {
		t.Fatalf("handleMessageFrame = %v, want ErrMessageRecvMemoryBudgetExceeded", err)
	}

	if got := tr.msgRecvBudget.Load(); got != aggregateBefore {
		t.Fatalf("aggregate budget = %d after rejection, want %d (reserve/release imbalance)", got, aggregateBefore)
	}
	// The sub-budget is keyed by the peer's authenticated IDENTITY, not by the
	// connection: keying on the connection let one peer open
	// MaxConnectionsPerIP connections and hold that many separate sub-budgets,
	// which is exactly the cap this reservation exists to impose.
	v, ok := tr.connMsgBudget.Load(msgBudgetKey(conn))
	if !ok {
		t.Fatal("per-peer budget entry disappeared")
	}
	if got := v.(*peerMsgBudget).reserved.Load(); got != preReserve {
		t.Fatalf("per-peer reservation = %d after rejection, want %d (the rejected delta leaked)",
			got, preReserve)
	}

	// With the headroom returned, the same frame is accepted: the rejection was
	// budget pressure, not a permanent refusal.
	tr.releaseConnMsgRecvBudget(conn, preReserve)
	if err := tr.handleMessageFrame(conn, bytes.NewReader(compressed), uint32(len(compressed)), FlagCompressed); err == nil {
		t.Log("frame accepted once the per-connection budget was released")
	} else if errors.Is(err, ErrMessageRecvMemoryBudgetExceeded) {
		t.Fatal("frame still rejected for budget after the reservation was released")
	}
}

// TestIsProtocolViolation_DecoderSentinels pins the classification of the proto
// decoder's malformed-batch sentinels as protocol violations. They are what a
// peer speaking a broken or hostile wire format actually produces; classifying
// them as ordinary I/O would tear streams down with no counter and no warning,
// which is indistinguishable from normal connection churn.
func TestIsProtocolViolation_DecoderSentinels(t *testing.T) {
	violations := []error{
		proto.ErrTooManyMessages,
		proto.ErrBufferTooSmall,
		proto.ErrInvalidEntryType,
		proto.ErrInvalidMessageType,
		proto.ErrDuplicateMapKey,
		proto.ErrUnsupportedVersion,
	}
	for _, err := range violations {
		if !isProtocolViolation(err) {
			t.Errorf("isProtocolViolation(%v) = false, want true", err)
		}
		// Wrapped form must classify identically: the receive path sees these
		// through the decoder's error chain.
		if !isProtocolViolation(&FrameDecompressError{Err: err}) {
			t.Errorf("isProtocolViolation(wrapped %v) = false, want true", err)
		}
	}

	// A transient I/O error is NOT a violation: counting it would make the
	// violation counter useless for detecting an actually-broken peer.
	if isProtocolViolation(io.EOF) {
		t.Error("isProtocolViolation(io.EOF) = true, want false")
	}
	if isProtocolViolation(nil) {
		t.Error("isProtocolViolation(nil) = true, want false")
	}
}

// TestSnapshotReceiver_PerPeerSlotFloor pins the per-peer concurrency floor. The
// per-peer cap is half the global budget, but it must never round down to zero:
// a zero cap would make acquirePeerSlot reject every transfer and no snapshot
// could ever be received.
func TestSnapshotReceiver_PerPeerSlotFloor(t *testing.T) {
	sr := newSnapshotReceiver(1, 1<<20, 1<<20, 42, newTestHandler())
	if sr.maxPerPeer < 1 {
		t.Fatalf("maxPerPeer = %d, want at least 1", sr.maxPerPeer)
	}
	if !sr.acquirePeerSlot("peer-a:1") {
		t.Fatal("a receiver with maxConcurrent=1 must still admit one transfer per peer")
	}
	if sr.acquirePeerSlot("peer-a:1") {
		t.Fatal("the per-peer cap was not enforced for a second concurrent transfer")
	}
	sr.releasePeerSlot("peer-a:1")
	if !sr.acquirePeerSlot("peer-a:1") {
		t.Fatal("the peer slot was not returned by releasePeerSlot")
	}
}

// TestSnapshotReceiver_ReleaseUnknownPeerIsNoop pins the map hygiene of the
// per-peer slot table. A release for a peer with no reservation must not create
// or decrement an entry: a negative count would permanently under-report the
// peer's usage and let it exceed its concurrency cap.
func TestSnapshotReceiver_ReleaseUnknownPeerIsNoop(t *testing.T) {
	sr := newSnapshotReceiver(4, 1<<20, 1<<20, 42, newTestHandler())

	sr.releasePeerSlot("never-seen:1")
	sr.peerMu.Lock()
	_, present := sr.peerUse["never-seen:1"]
	entries := len(sr.peerUse)
	sr.peerMu.Unlock()
	if present {
		t.Fatal("releasing an unknown peer created a slot entry")
	}
	if entries != 0 {
		t.Fatalf("peerSlots has %d entries after releasing an unknown peer, want 0", entries)
	}

	// An empty peer address (unit-test/no-connection path) is not accounted at
	// all, in either direction.
	if !sr.acquirePeerSlot("") {
		t.Fatal("an empty peer address must not be rate-limited")
	}
	sr.releasePeerSlot("")
	sr.peerMu.Lock()
	entries = len(sr.peerUse)
	sr.peerMu.Unlock()
	if entries != 0 {
		t.Fatalf("peerSlots has %d entries after an unaccounted peer, want 0", entries)
	}
}

// TestParseCACertificates_SkipsNonCertificateBlocks pins the bundle scan's
// tolerance for non-certificate PEM blocks. Operators routinely concatenate
// keys, CRLs or DH parameters into a bundle; treating such a block as fatal
// (or, worse, as an issuer) would either refuse a valid bundle at startup or
// wire revocation to a bogus issuer, which under enforcement is a total
// handshake outage.
func TestParseCACertificates_SkipsNonCertificateBlocks(t *testing.T) {
	caPEM, caKeyPEM, _, _ := generateTestCertPair(t)

	// A key block ahead of the CA certificate must be skipped, not counted.
	bundle := append(append([]byte{}, caKeyPEM...), caPEM...)
	issuers, err := parseCACertificates(bundle)
	if err != nil {
		t.Fatalf("parseCACertificates on a bundle with a key block failed: %v", err)
	}
	if len(issuers) != 1 {
		t.Fatalf("parsed %d issuers, want 1 (the key block must not become an issuer)", len(issuers))
	}
	if !issuers[0].IsCA {
		t.Fatal("the parsed issuer is not a CA certificate")
	}

	// A bundle with ONLY non-certificate blocks still fails closed: there is no
	// issuer to build an OCSP request against or verify a CRL with.
	if _, err := parseCACertificates(caKeyPEM); !errors.Is(err, ErrCACertificateParse) {
		t.Fatalf("parseCACertificates on a key-only bundle = %v, want ErrCACertificateParse", err)
	}
}

// TestAcceptLoop_GlobalConnectionLimitRejects pins the global inbound
// connection bound. Once MaxIncomingConnections handlers are live, a further
// inbound connection must be refused with QUICErrConnLimitReached rather than
// queued: unbounded accepts are the memory-amplification path the per-connection
// receive budgets are sized against.
func TestAcceptLoop_GlobalConnectionLimitRejects(t *testing.T) {
	rec := &dropRecorder{}
	// One shared CA: all three endpoints must authenticate to each other.
	mtls := testMTLSConfig(t)
	sharedMTLS := func(c *Config) { c.MTLSConfig = mtls }
	receiverHandler := newTestHandler()
	receiver, err := NewQUICTransport(Config{
		ListenAddress:          "127.0.0.1:0",
		DeploymentID:           42,
		MTLSConfig:             mtls,
		MaxIncomingConnections: 1,
		Metrics:                rec,
	}, receiverHandler, registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(receiver) failed: %v", err)
	}
	if err := receiver.Start(); err != nil {
		t.Fatalf("receiver.Start failed: %v", err)
	}
	defer func() { _ = receiver.Stop() }()

	target := receiver.Addr().String()

	// First sender: occupy the single inbound slot. Waiting for a delivered
	// batch proves the handler goroutine is running and holds the semaphore,
	// so the second connection's rejection is not a race.
	first, _ := newDropTransport(t, sharedMTLS)
	firstConn, err := first.getConnection(target)
	if err != nil {
		t.Fatalf("first getConnection failed: %v", err)
	}
	first.sendBatch(target, []proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2}})
	receiverHandler.waitBatch(t, 10*time.Second)
	select {
	case <-firstConn.Context().Done():
		t.Fatal("the first connection was closed; it must hold the single inbound slot")
	default:
	}

	// Second sender: over the limit, so the receiver must close it.
	second, _ := newDropTransport(t, sharedMTLS)
	secondConn, err := second.getConnection(target)
	if err != nil {
		t.Fatalf("second getConnection failed: %v", err)
	}
	select {
	case <-secondConn.Context().Done():
	case <-time.After(10 * time.Second):
		t.Fatal("a connection past MaxIncomingConnections was accepted and kept open")
	}

	var appErr *quic.ApplicationError
	if !errors.As(context.Cause(secondConn.Context()), &appErr) {
		t.Fatalf("second connection closed with %v, want a QUIC application error",
			context.Cause(secondConn.Context()))
	}
	if appErr.ErrorCode != QUICErrConnLimitReached {
		t.Fatalf("second connection closed with code %d, want QUICErrConnLimitReached (%d)",
			appErr.ErrorCode, QUICErrConnLimitReached)
	}

	// The first connection must be untouched: the limit rejects the newcomer,
	// it does not evict an established peer.
	select {
	case <-firstConn.Context().Done():
		t.Fatal("the established connection was closed when a new one hit the limit")
	default:
	}
}
