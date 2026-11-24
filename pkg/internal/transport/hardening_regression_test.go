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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"testing"
	"time"
	"unsafe"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

// ---------------------------------------------------------------------------
// Inbound snapshot memory accounting (snapshot_recv.go)
// ---------------------------------------------------------------------------

// minimalChunk builds the smallest well-formed chunk the receiver accepts for
// the given header: one data byte, so it is never a zero-length frame and the
// flood cap never fires.
func minimalChunk(id, count uint64, hdr *SnapshotHeader) proto.SnapshotChunk {
	return proto.SnapshotChunk{
		BinVer:     proto.WireVersion,
		ShardID:    hdr.ShardID,
		ReplicaID:  hdr.ReplicaID,
		Index:      hdr.Index,
		Term:       hdr.Term,
		Epoch:      hdr.Epoch,
		ChunkID:    id,
		ChunkCount: count,
		ChunkSize:  1,
		Data:       []byte{0x01},
	}
}

// TestSnapshotBudgetChargesRetainedChunkHeaders pins that the inbound snapshot
// memory budget accounts for what the receiver actually HOLDS. receiveChunks
// accumulates every accepted chunk in a slice for the whole transfer, so a
// peer sending many minimum-size chunks retains a proto.SnapshotChunk header
// (280 bytes here) per 177-byte frame. Charging the frame length alone
// under-counts residency ~2.6x and lets concurrent transfers hold multiples of
// the configured budget.
func TestSnapshotBudgetChargesRetainedChunkHeaders(t *testing.T) {
	hdr := &SnapshotHeader{ShardID: 1, ReplicaID: 2, Index: 50, Term: 3, ChunkCount: 10}

	chunks := make([]proto.SnapshotChunk, hdr.ChunkCount)
	for i := range chunks {
		chunks[i] = minimalChunk(uint64(i), hdr.ChunkCount, hdr)
	}
	frameLen := int64(chunks[0].Size())
	structBytes := int64(unsafe.Sizeof(proto.SnapshotChunk{}))

	// A budget that comfortably fits every chunk's WIRE bytes but not the
	// headers retained alongside them. Correct accounting must refuse the
	// transfer; frame-length-only accounting completes it while holding
	// ChunkCount*structBytes of unaccounted memory.
	budget := int64(hdr.ChunkCount)*frameLen + structBytes
	if budget >= int64(hdr.ChunkCount)*(frameLen+structBytes) {
		t.Fatalf("test budget %d does not distinguish the two accountings", budget)
	}

	handler := newTestHandler()
	sr := newSnapshotReceiver(4, budget, 0, 0, handler)
	rec := &dropRecorder{}
	sr.metrics = rec

	reader := &testChunkReader{data: buildChunkFrames(t, chunks)}
	sr.receiveChunks(reader, hdr, "", PeerAuth{}, make(chan struct{}))

	if handler.snapshotCount.Load() != 0 {
		t.Fatalf("transfer of %d minimal chunks needs %d bytes resident against a %d byte budget and must be refused",
			hdr.ChunkCount, int64(hdr.ChunkCount)*(frameLen+structBytes), budget)
	}
	rejects := rec.rejects()
	if len(rejects) != 1 || rejects[0].reason != string(snapshotRejectMemory) {
		t.Fatalf("rejections = %+v, want one %q", rejects, snapshotRejectMemory)
	}
	if got := sr.AvailableMemory(); got != budget {
		t.Fatalf("budget after a refused transfer = %d, want the full %d restored", got, budget)
	}
}

// TestSnapshotBudgetRestoredAfterCompleteTransfer pins that the corrected
// accounting still balances exactly on the success path.
func TestSnapshotBudgetRestoredAfterCompleteTransfer(t *testing.T) {
	hdr := &SnapshotHeader{ShardID: 1, ReplicaID: 2, Index: 50, Term: 3, ChunkCount: 4}
	chunks := make([]proto.SnapshotChunk, hdr.ChunkCount)
	for i := range chunks {
		chunks[i] = minimalChunk(uint64(i), hdr.ChunkCount, hdr)
	}

	handler := newTestHandler()
	const budget = int64(1) << 20
	sr := newSnapshotReceiver(4, budget, 0, 0, handler)

	reader := &testChunkReader{data: buildChunkFrames(t, chunks)}
	sr.receiveChunks(reader, hdr, "", PeerAuth{}, make(chan struct{}))

	if handler.snapshotCount.Load() != 1 {
		t.Fatalf("complete transfer not delivered: snapshots = %d", handler.snapshotCount.Load())
	}
	if got := sr.AvailableMemory(); got != budget {
		t.Fatalf("budget after a complete transfer = %d, want %d", got, budget)
	}
}

// TestMaxAdmissibleChunkCountBoundedByBudget pins that the declared ChunkCount
// ceiling follows the memory budget instead of a fixed 1<<20. The fixed
// ceiling is not a memory bound: a peer declaring it and sending minimum-size
// chunks commits the receiver to ChunkCount * minSnapshotChunkResidentBytes.
func TestMaxAdmissibleChunkCountBoundedByBudget(t *testing.T) {
	small := newSnapshotReceiver(4, 100*minSnapshotChunkResidentBytes, 0, 0, newTestHandler())
	if got := small.maxAdmissibleChunkCount(); got != 100 {
		t.Fatalf("chunk-count ceiling for a 100-chunk budget = %d, want 100", got)
	}

	large := newSnapshotReceiver(4, int64(defaultMaxSnapshotChunkCount)*minSnapshotChunkResidentBytes*2, 0, 0, newTestHandler())
	if got := large.maxAdmissibleChunkCount(); got != defaultMaxSnapshotChunkCount {
		t.Fatalf("chunk-count ceiling for a large budget = %d, want the protocol maximum %d",
			got, defaultMaxSnapshotChunkCount)
	}
}

// TestProcessSnapshotHeaderRejectsChunkCountBeyondBudget pins the admission
// check: a header declaring more chunks than the budget can hold is refused at
// the header, before any memory is committed.
func TestProcessSnapshotHeaderRejectsChunkCountBeyondBudget(t *testing.T) {
	handler := newTestHandler()
	rec := &dropRecorder{}
	tr, err := NewQUICTransport(Config{
		ListenAddress:            ":0",
		DeploymentID:             42,
		MTLSConfig:               testMTLSConfig(t),
		MaxSnapshotReceiveMemory: 64 * minSnapshotChunkResidentBytes,
		Metrics:                  rec,
	}, handler, registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	tr.snapshotRecv.metrics = rec

	hdr := SnapshotHeader{ShardID: 7, ReplicaID: 2, Index: 100, Term: 5, ChunkCount: 65}
	if hdr.ChunkCount >= defaultMaxSnapshotChunkCount {
		t.Fatal("the declared count must be inside the fixed ceiling so only the budget clamp can reject it")
	}
	var hdrBuf [SnapshotHeaderSize]byte
	if _, marshalErr := hdr.MarshalTo(hdrBuf[:]); marshalErr != nil {
		t.Fatalf("header MarshalTo: %v", marshalErr)
	}

	tr.processSnapshotHeader(nil, bytes.NewReader(hdrBuf[:]), SnapshotHeaderSize)

	rejects := rec.rejects()
	// Counted under shard 0: the header was rejected before its sender was
	// authorized, so the shard it names is unverified.
	if len(rejects) != 1 || rejects[0].reason != string(snapshotRejectMalformed) || rejects[0].shardID != 0 {
		t.Fatalf("rejections = %+v, want one %q, unattributed (shard 0)", rejects, snapshotRejectMalformed)
	}
}

// ---------------------------------------------------------------------------
// TLS revocation wiring (tls.go VerifyConnection)
// ---------------------------------------------------------------------------

// leafFromPEM parses the first certificate in a PEM bundle.
func leafFromPEM(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("no PEM block in certificate")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return leaf
}

// newRevocationTLS builds the transport's TLS pair wired to a checker over the
// same CA bundle, and returns the peer leaf the tests present.
func newRevocationTLS(t *testing.T, cfg revocation.Config) (*tlsPair, *revocation.Checker, *x509.Certificate) {
	t.Helper()
	mtls := testMTLSConfig(t)
	issuers, err := parseCACertificates(mtls.CACert)
	if err != nil {
		t.Fatalf("parseCACertificates: %v", err)
	}
	checker := revocation.NewChecker(cfg, issuers...)
	pair, err := buildMTLSTLS(mtls, checker)
	if err != nil {
		t.Fatalf("buildMTLSTLS: %v", err)
	}
	return pair, checker, leafFromPEM(t, mtls.Cert)
}

// TestVerifyConnectionRejectsRevokedPeer pins the wiring the whole revocation
// subsystem exists for: a checker verdict of "revoked" must fail the TLS
// handshake on BOTH roles, and fire the OnCertRevoked callback. Without this
// the checker can be perfectly correct while the connection is still admitted.
func TestVerifyConnectionRejectsRevokedPeer(t *testing.T) {
	var notified []string
	pair, checker, leaf := newRevocationTLS(t, revocation.Config{
		Mode:          revocation.ModeCRL,
		OnCertRevoked: func(serial string) { notified = append(notified, serial) },
	})
	serial := leaf.SerialNumber.Text(16)
	checker.AddRevokedSerial(serial)

	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
	for name, cfg := range map[string]*tls.Config{"server": pair.server, "client": pair.client} {
		if cfg.VerifyConnection == nil {
			t.Fatalf("%s config has no VerifyConnection: revocation is not wired into the handshake", name)
		}
		err := cfg.VerifyConnection(state)
		if !errors.Is(err, revocation.ErrCertificateRevoked) {
			t.Fatalf("%s VerifyConnection = %v, want ErrCertificateRevoked", name, err)
		}
	}
	if len(notified) != 2 || notified[0] != serial {
		t.Fatalf("OnCertRevoked notifications = %v, want the serial %q once per role", notified, serial)
	}
}

// TestVerifyConnectionPropagatesCheckerError pins that an "unknown status"
// verdict under enforcement also fails the handshake, rather than being
// swallowed into an admission.
func TestVerifyConnectionPropagatesCheckerError(t *testing.T) {
	pair, _, leaf := newRevocationTLS(t, revocation.Config{
		Mode:     revocation.ModeCRL,
		CRLPaths: []string{"/nonexistent/never-loaded.crl"},
	})

	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
	if err := pair.server.VerifyConnection(state); !errors.Is(err, revocation.ErrCRLStale) {
		t.Fatalf("VerifyConnection = %v, want the enforced ErrCRLStale rejection", err)
	}
}

// ---------------------------------------------------------------------------
// Receive-buffer detach of snapshot metadata (recv.go handleMessageFrame)
// ---------------------------------------------------------------------------

// TestSnapshotMetadataDetachedFromReceiveBuffer pins that nothing on a decoded
// Snapshot still points into the POOLED receive buffer once the frame has been
// dispatched. The buffer is recycled immediately, so a surviving alias is read
// by the step worker after the buffer has been handed to another frame — and a
// Snapshot is the worst possible place for that, because raft adopts its
// Membership wholesale.
//
// Filepath and every membership map are copied by their own decoders
// (getString, getMap) and must stay that way. A future "zero-copy Filepath" or
// a membership map aliased into the buffer would reintroduce exactly this class
// of bug, and this is what catches it.
func TestSnapshotMetadataDetachedFromReceiveBuffer(t *testing.T) {
	handler := newTestHandler()
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, handler, registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}

	const wantPath = "/var/lib/quicraft/shard-1/snapshot-77"
	batch := proto.MessageBatch{
		BinVer:       proto.WireVersion,
		DeploymentID: 42,
		Requests: []proto.Message{{
			Type:    proto.InstallSnapshot,
			ShardID: 1,
			From:    2,
			To:      1,
			Term:    9,
			Snapshot: proto.Snapshot{
				ShardID:  1,
				Index:    77,
				Term:     9,
				Filepath: wantPath,
				Membership: proto.Membership{
					ConfigChangeID:    4,
					Addresses:         map[uint64]string{1: "node-1:5000", 2: "node-2:5000"},
					Removed:           map[uint64]bool{3: true},
					Identities:        map[uint64]string{1: "spki:aaaaaaaa"},
					RemovedIdentities: map[uint64]string{3: "spki:cccccccc"},
				},
			},
		}},
	}
	payload := make([]byte, batch.Size())
	n, err := batch.MarshalTo(payload)
	if err != nil {
		t.Fatalf("batch MarshalTo: %v", err)
	}
	payload = payload[:n]

	if err := tr.handleMessageFrame(nil, bytes.NewReader(payload), uint32(len(payload)), FlagNone); err != nil {
		t.Fatalf("handleMessageFrame: %v", err)
	}

	// Recycle the pooled receive buffer through a second frame carrying
	// different bytes. A surviving alias would now read this frame's content.
	poison := make([]byte, len(payload))
	copy(poison, payload)
	for i := range poison {
		poison[i] ^= 0xFF
	}
	_ = tr.handleMessageFrame(nil, bytes.NewReader(poison), uint32(len(poison)), FlagNone)

	batches := handler.getBatches()
	if len(batches) == 0 {
		t.Fatal("no batch delivered")
	}
	got := batches[0].Requests[0].Snapshot
	if got.Filepath != wantPath {
		t.Fatalf("Snapshot.Filepath = %q, want %q: it still aliased the recycled receive buffer",
			got.Filepath, wantPath)
	}
	if got.Membership.Addresses[1] != "node-1:5000" || got.Membership.Addresses[2] != "node-2:5000" {
		t.Fatalf("Snapshot.Membership.Addresses = %v: it still aliased the recycled receive buffer",
			got.Membership.Addresses)
	}
	if !got.Membership.Removed[3] {
		t.Fatalf("Snapshot.Membership.Removed = %v: it still aliased the recycled receive buffer",
			got.Membership.Removed)
	}
	if got.Membership.Identities[1] != "spki:aaaaaaaa" {
		t.Fatalf("Snapshot.Membership.Identities = %v: it still aliased the recycled "+
			"receive buffer, so the receive-side quorum gate would authorize against "+
			"another frame's bytes", got.Membership.Identities)
	}
	if got.Membership.RemovedIdentities[3] != "spki:cccccccc" {
		t.Fatalf("Snapshot.Membership.RemovedIdentities = %v: it still aliased the "+
			"recycled receive buffer, so the removal deny-list would be evaluated "+
			"against another frame's bytes", got.Membership.RemovedIdentities)
	}
}

// ---------------------------------------------------------------------------
// Per-connection message-receive sub-budget (recv.go / conn.go)
// ---------------------------------------------------------------------------

// TestPerConnectionRecvBudgetRejectsAndDrains pins the per-connection
// sub-budget. It exists so one peer cannot occupy the whole aggregate budget
// and push every other peer's frames into the exhaustion path — and every
// existing test passes a nil connection, which disables the feature outright.
func TestPerConnectionRecvBudgetRejectsAndDrains(t *testing.T) {
	tr, conn := liveConn(t)

	if tr.maxConnMsgRecv <= 0 {
		t.Fatal("per-connection budget is disabled; the sub-budget cannot be exercised")
	}

	// Occupy the connection's whole sub-budget, as concurrent in-flight frames
	// on the same connection would.
	if !tr.reserveConnMsgRecvBudget(conn, tr.maxConnMsgRecv) {
		t.Fatal("initial reservation of the full sub-budget must succeed")
	}
	if tr.reserveConnMsgRecvBudget(conn, 1) {
		t.Fatal("a reservation past the per-connection ceiling must be refused")
	}

	// A frame arriving on that connection is refused, and its body is drained
	// so the stream is left at a clean frame boundary for the next frame.
	payload := bytes.Repeat([]byte{0x7E}, 512)
	trailer := []byte("NEXTFRAME")
	stream := bytes.NewReader(append(append([]byte{}, payload...), trailer...))

	err := tr.handleMessageFrame(conn, stream, uint32(len(payload)), FlagNone)
	if !errors.Is(err, ErrMessageRecvMemoryBudgetExceeded) {
		t.Fatalf("handleMessageFrame = %v, want ErrMessageRecvMemoryBudgetExceeded", err)
	}
	rest, _ := io.ReadAll(stream)
	if !bytes.Equal(rest, trailer) {
		t.Fatalf("stream left at %q, want the next frame boundary %q", rest, trailer)
	}

	// Releasing restores capacity; a different connection was never charged.
	tr.releaseConnMsgRecvBudget(conn, tr.maxConnMsgRecv)
	if !tr.reserveConnMsgRecvBudget(conn, tr.maxConnMsgRecv) {
		t.Fatal("released sub-budget was not returned")
	}
	tr.releaseConnMsgRecvBudget(conn, tr.maxConnMsgRecv)
}

// ---------------------------------------------------------------------------
// Frame drain / protocol violation classification (recv.go)
// ---------------------------------------------------------------------------

// TestDropFrameIncompleteDrainIsNotRetryable pins that a frame body which could
// not be fully discarded is reported as a distinct, non-retryable error. The
// budget error is retryable — handleStream continues on the same stream — and
// continuing on a stream desynchronised mid-frame parses the next "header" out
// of undelivered payload.
func TestDropFrameIncompleteDrainIsNotRetryable(t *testing.T) {
	tr := &QUICTransport{}

	short := bytes.NewReader(bytes.Repeat([]byte{0x11}, 8))
	err := tr.dropFrame(short, 64, ErrMessageRecvMemoryBudgetExceeded)
	if err == nil {
		t.Fatal("an incomplete drain must report an error")
	}
	if errors.Is(err, ErrMessageRecvMemoryBudgetExceeded) {
		t.Fatal("an incomplete drain must NOT report the retryable budget error")
	}
	if !errors.Is(err, ErrFrameDrainIncomplete) {
		t.Fatalf("dropFrame = %v, want ErrFrameDrainIncomplete", err)
	}
	var drainErr *FrameDrainError
	if !errors.As(err, &drainErr) || drainErr.Drained != 8 || drainErr.Declared != 64 {
		t.Fatalf("drain error = %+v, want 8 of 64 bytes drained", drainErr)
	}

	// A complete drain leaves the stream at a clean boundary, so the caller's
	// retryable error stands.
	full := bytes.NewReader(bytes.Repeat([]byte{0x11}, 64))
	if err := tr.dropFrame(full, 64, ErrMessageRecvMemoryBudgetExceeded); !errors.Is(err, ErrMessageRecvMemoryBudgetExceeded) {
		t.Fatalf("dropFrame after a complete drain = %v, want the retryable budget error", err)
	}
}

// TestIsProtocolViolationCoversDecoderSentinels pins that the errors a
// malformed batch ACTUALLY returns are classified as protocol violations.
// Without them such frames tear streams down with no counter and no warning,
// indistinguishable from ordinary connection churn.
func TestIsProtocolViolationCoversDecoderSentinels(t *testing.T) {
	for _, err := range []error{
		proto.ErrBufferTooSmall,
		proto.ErrInvalidEntryType,
		proto.ErrInvalidMessageType,
		proto.ErrDuplicateMapKey,
		proto.ErrUnsupportedVersion,
		proto.ErrTooManyMessages,
	} {
		if !isProtocolViolation(err) {
			t.Fatalf("isProtocolViolation(%v) = false, want true", err)
		}
	}
	if isProtocolViolation(io.EOF) {
		t.Fatal("an ordinary end-of-stream is not a protocol violation")
	}
}

// TestProtocolViolationRateLimitIsPerTransport pins that the log rate limiter
// is instance state. A package global lets one transport's violation flood
// suppress the FIRST diagnostic every other transport would emit, and
// in-process multi-node deployments run many transports at once.
func TestProtocolViolationRateLimitIsPerTransport(t *testing.T) {
	rec1, rec2 := &dropRecorder{}, &dropRecorder{}
	t1 := &QUICTransport{metrics: rec1}
	t2 := &QUICTransport{metrics: rec2}

	t1.recordProtocolViolation("first", nil, ErrInvalidMagic)
	if t1.lastProtocolViolationLog.Load() == 0 {
		t.Fatal("the first violation on t1 must be logged")
	}
	t2.recordProtocolViolation("first", nil, ErrInvalidMagic)
	if t2.lastProtocolViolationLog.Load() == 0 {
		t.Fatal("t1's rate limit must not suppress t2's first diagnostic")
	}
	if len(rec1.drops()) != 1 || len(rec2.drops()) != 1 {
		t.Fatalf("drop counters = %d/%d, want 1 each", len(rec1.drops()), len(rec2.drops()))
	}
}

// ---------------------------------------------------------------------------
// Send-path fixes
// ---------------------------------------------------------------------------

// TestIsHeartbeatBatchRejectsSnapshotCarrier pins that a heartbeat carrying a
// snapshot falls through to the general send path. The fast path marshals into
// fixed HeartbeatMessageSize slots, so routing it there overflows the buffer
// and drops the WHOLE batch — a liveness event, not a dropped byte.
func TestIsHeartbeatBatchRejectsSnapshotCarrier(t *testing.T) {
	withSnapshot := []proto.Message{{
		Type:     proto.Heartbeat,
		ShardID:  1,
		Snapshot: proto.Snapshot{ShardID: 1, Index: 5, Term: 2},
	}}
	if isHeartbeatBatch(withSnapshot) {
		t.Fatal("a heartbeat carrying a snapshot must not take the fixed-size fast path")
	}

	// The message really is oversized for the fast path's fixed slot.
	if got := withSnapshot[0].Size(); got <= proto.HeartbeatMessageSize {
		t.Fatalf("snapshot-carrying heartbeat size %d does not exceed the fixed slot %d",
			got, proto.HeartbeatMessageSize)
	}

	plain := []proto.Message{{Type: proto.Heartbeat, ShardID: 1}}
	if !isHeartbeatBatch(plain) {
		t.Fatal("a plain heartbeat must still take the fast path")
	}
}

// TestGetBufReturnsUndersizedBufferToPool pins that growing past a pooled
// buffer's capacity returns that buffer instead of dropping it, as getRecvBuf
// already does. Dropping it discards a live pooled buffer on every oversized
// send and steadily drains the pool.
func TestGetBufReturnsUndersizedBufferToPool(t *testing.T) {
	tr := &QUICTransport{}
	tr.bufPool.New = func() any {
		buf := make([]byte, 0, 8)
		return &buf
	}

	marker := make([]byte, 0, 16)

	// Each attempt seeds the pool with the 16-byte buffer, forces the grow
	// path, and checks the undersized buffer came back. Retried because the
	// race detector makes sync.Pool discard a random fraction of Puts; a
	// getBuf that DROPS the buffer instead of returning it fails every attempt.
	recovered := poolRoundTrips(func() { tr.putBuf(&marker) }, func() bool {
		grown := tr.getBuf(1024)
		if cap(*grown) < 1024 {
			t.Fatalf("getBuf returned capacity %d, want >= 1024", cap(*grown))
		}
		return cap(*tr.getBuf(1)) == 16
	})
	if !recovered {
		t.Fatal("getBuf dropped the pooled buffer it had to grow past instead of returning it")
	}
}

// ---------------------------------------------------------------------------
// QUIC-level receive window (config.go)
// ---------------------------------------------------------------------------

// TestConnectionReceiveWindowDerivedFromMessageBudget pins that QUIC's
// per-connection buffering follows the documented application budget instead
// of a fixed value multiplied by MaxIncomingConnections into several GB the
// budget does not cover.
func TestConnectionReceiveWindowDerivedFromMessageBudget(t *testing.T) {
	// Large budget, few connections: capped at the ceiling.
	if got := connectionReceiveWindow(1<<40, 1); got != maxConnectionReceiveWindowCeiling {
		t.Fatalf("window = %d, want the ceiling %d", got, maxConnectionReceiveWindowCeiling)
	}
	// Small budget spread over many connections: floored at one stream window.
	if got := connectionReceiveWindow(1<<20, 256); got != maxStreamReceiveWindow {
		t.Fatalf("window = %d, want the stream-window floor %d", got, maxStreamReceiveWindow)
	}
	// In between: the derived share.
	if got := connectionReceiveWindow(160<<20, 10); got != 16<<20 {
		t.Fatalf("window = %d, want the derived 16 MB share", got)
	}
	// Degenerate inputs never produce a zero window.
	if got := connectionReceiveWindow(0, 0); got != maxStreamReceiveWindow {
		t.Fatalf("window = %d, want the floor for degenerate input", got)
	}

	// It is actually wired into the QUIC config.
	tr, err := NewQUICTransport(Config{
		ListenAddress:           ":0",
		DeploymentID:            42,
		MTLSConfig:              testMTLSConfig(t),
		MaxMessageReceiveMemory: 160 << 20,
		MaxIncomingConnections:  10,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	if got := tr.quicCfg.MaxConnectionReceiveWindow; got != 16<<20 {
		t.Fatalf("quic MaxConnectionReceiveWindow = %d, want the derived 16 MB", got)
	}
}

// ---------------------------------------------------------------------------
// 0-RTT snapshot rejection (recv.go processSnapshotHeader)
// ---------------------------------------------------------------------------

// TestSnapshotOver0RTTRejected pins the replay policy end to end: 0-RTT data is
// replayable per RFC 9001 §8, and the snapshot path persists to disk and feeds
// InstallSnapshot into raft, so unlike an idempotent Raft RPC it must not run
// on early data. The check is defense-in-depth (0-RTT is default-off), which is
// exactly why it needs a test — nothing else would notice it regressing.
func TestSnapshotOver0RTTRejected(t *testing.T) {
	mtls := testMTLSConfig(t)
	rec := &dropRecorder{}
	receiver, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		Enable0RTT:    true,
		Metrics:       rec,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	if err := receiver.Start(); err != nil {
		t.Fatalf("receiver.Start: %v", err)
	}
	defer receiver.Stop()
	receiver.snapshotRecv.metrics = rec

	pair, err := buildMTLSTLS(mtls, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS: %v", err)
	}
	addr := receiver.Addr().String()
	qcfg := &quic.Config{
		MaxIdleTimeout:       10 * time.Second,
		HandshakeIdleTimeout: 5 * time.Second,
		Allow0RTT:            true,
	}

	// First connection: completes a full handshake so the server issues a
	// session ticket the client caches. The ticket arrives shortly AFTER the
	// handshake, so the connection is held open until the client cache holds
	// it (keyed by the SNI DialAddrEarly derives from the address).
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	warm, err := quic.DialAddrEarly(ctx, addr, pair.client, qcfg)
	if err != nil {
		t.Fatalf("warm-up dial: %v", err)
	}
	<-warm.HandshakeComplete()
	pollUntil(t, 5*time.Second, 5*time.Millisecond, "no session ticket cached after the warm-up handshake",
		func() bool {
			_, ok := pair.client.ClientSessionCache.Get("127.0.0.1")
			return ok
		})
	_ = warm.CloseWithError(0, "warm-up done")

	// Resumed connection: the snapshot header is written BEFORE the handshake
	// completes, so it travels as replayable early data.
	early, err := quic.DialAddrEarly(ctx, addr, pair.client, qcfg)
	if err != nil {
		t.Fatalf("early dial: %v", err)
	}
	defer func() { _ = early.CloseWithError(0, "done") }()

	stream, err := early.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}
	hdr := SnapshotHeader{ShardID: 3, ReplicaID: 2, Index: 100, Term: 5, ChunkCount: 1}
	var hdrBuf [SnapshotHeaderSize]byte
	if _, marshalErr := hdr.MarshalTo(hdrBuf[:]); marshalErr != nil {
		t.Fatalf("header MarshalTo: %v", marshalErr)
	}
	if writeErr := WriteFrame(stream, hdrBuf[:], FlagSnapshot); writeErr != nil {
		t.Fatalf("WriteFrame: %v", writeErr)
	}

	<-early.HandshakeComplete()
	if !early.ConnectionState().Used0RTT {
		t.Fatal("resumption did not use 0-RTT; the replay-policy path cannot be exercised")
	}

	pollUntil(t, 10*time.Second, 20*time.Millisecond,
		"snapshot arriving over 0-RTT was not rejected under the replay policy",
		func() bool {
			for _, r := range rec.rejects() {
				// Counted unattributed: the transfer is refused before its
				// sender was authorized, so the shard it names is unverified.
				if r.reason == string(snapshotRejectPolicy) && r.shardID == 0 {
					return true
				}
			}
			return false
		})
}
