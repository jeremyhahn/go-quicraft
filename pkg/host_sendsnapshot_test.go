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

package quicraft

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Test harness: a real (unstarted) QUIC transport
// ---------------------------------------------------------------------------

// testMTLSConfig generates a throwaway self-signed certificate that serves as
// both the node leaf and the trusted CA bundle. NewQUICTransport requires a
// usable mTLS pair even when the transport is never bound to a socket.
func testMTLSConfig(t *testing.T) *transport.MTLSConfig {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "quicraft-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return &transport.MTLSConfig{CACert: certPEM, Cert: certPEM, Key: keyPEM}
}

// recordingTransportMetrics records the transport metric calls the send path
// makes, so a test can prove a message actually reached the transport.
type recordingTransportMetrics struct {
	mu      sync.Mutex
	dropped []droppedMessage
}

type droppedMessage struct {
	shardID uint64
	to      uint64
	reason  string
}

func (m *recordingTransportMetrics) ObserveSendLatency(time.Duration)           {}
func (m *recordingTransportMetrics) IncMessagesSent(uint64, uint64, uint64)     {}
func (m *recordingTransportMetrics) IncMessagesReceived(uint64, uint64, uint64) {}
func (m *recordingTransportMetrics) IncSnapshotsSent(uint64, int64, time.Duration) {
}
func (m *recordingTransportMetrics) IncSnapshotsReceived(uint64, int64, time.Duration) {
}
func (m *recordingTransportMetrics) IncSnapshotsRejected(uint64, string) {}

func (m *recordingTransportMetrics) IncMessagesDropped(shardID, to uint64, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropped = append(m.dropped, droppedMessage{shardID: shardID, to: to, reason: reason})
}

func (m *recordingTransportMetrics) drops() []droppedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]droppedMessage, len(m.dropped))
	copy(out, m.dropped)
	return out
}

var _ transport.Metrics = (*recordingTransportMetrics)(nil)

// newTestTransport builds a real QUICTransport that is never bound to a
// socket. Its registry is empty, so every Resolve fails and both Send and
// SendSnapshotStreaming take their target-unresolved paths deterministically —
// no network, no peer, no timing dependency.
func newTestTransport(t *testing.T, metrics transport.Metrics) *transport.QUICTransport {
	t.Helper()
	cfg := transport.Config{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		DeploymentID:     1,
		MTLSConfig:       testMTLSConfig(t),
		Metrics:          metrics,
	}
	tr, err := transport.NewQUICTransport(cfg, nil, registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	t.Cleanup(func() {
		if stopErr := tr.Stop(); stopErr != nil {
			t.Errorf("transport.Stop: %v", stopErr)
		}
	})
	return tr
}

// snapshotSendFixture wires a transportSender to a real transport and a real
// engine holding one loaded node, so SnapshotStatus notifications can be
// observed on that node's inbox.
type snapshotSendFixture struct {
	sender  *transportSender
	node    *engine.Node
	metrics *recordingTransportMetrics
}

func newSnapshotSendFixture(t *testing.T, barrier Barrier) *snapshotSendFixture {
	t.Helper()
	hcfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	hcfg.SetDefaults()

	eng := engine.NewEngine(hcfg, nil, &noopSender{}, nil, nil, nil)
	eng.Start()
	t.Cleanup(eng.Stop)

	node := newMinimalEngineNode(t, 1, 1)
	eng.LoadNode(node)
	t.Cleanup(func() { eng.UnloadNode(1) })

	metrics := &recordingTransportMetrics{}
	return &snapshotSendFixture{
		sender: &transportSender{
			transport: newTestTransport(t, metrics),
			engine:    eng,
			barrier:   barrier,
			logger:    slog.Default(),
		},
		node:    node,
		metrics: metrics,
	}
}

// expectSnapshotStatus drains the node inbox and asserts exactly one
// SnapshotStatus carrying the expected rejection verdict and transfer
// sequence. The transfer sequence is what disambiguates repeated sends of the
// same snapshot index, so a status that loses it is worse than no status.
func (f *snapshotSendFixture) expectSnapshotStatus(t *testing.T, wantReject bool, wantSeq uint64) {
	t.Helper()
	var buf []proto.Message
	buf = f.node.DrainInbox(buf)
	if len(buf) != 1 {
		t.Fatalf("inbox has %d messages, want exactly 1 SnapshotStatus", len(buf))
	}
	msg := buf[0]
	if msg.Type != proto.SnapshotStatus {
		t.Fatalf("message type = %v, want SnapshotStatus", msg.Type)
	}
	if msg.Reject != wantReject {
		t.Fatalf("SnapshotStatus.Reject = %v, want %v", msg.Reject, wantReject)
	}
	if msg.Hint != wantSeq {
		t.Fatalf("SnapshotStatus.Hint = %d, want transfer seq %d", msg.Hint, wantSeq)
	}
}

// testInstallSnapshotMsg builds an InstallSnapshot message for the given file.
func testInstallSnapshotMsg(t *testing.T, path string, epoch, transferSeq uint64) proto.Message {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat snapshot file: %v", err)
	}
	return proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
		Hint:    transferSeq,
		Snapshot: proto.Snapshot{
			Index:    100,
			Term:     5,
			Filepath: path,
			FileSize: uint64(info.Size()),
			Epoch:    epoch,
		},
	}
}

// assertNoTempFiles fails when a decrypt-for-send scratch file was left in dir.
func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, sendDecryptTempPattern))
	if err != nil {
		t.Fatalf("glob temp files: %v", err)
	}
	if len(matches) > 0 {
		t.Fatalf("decrypt-for-send temp files leaked: %v", matches)
	}
}

// ---------------------------------------------------------------------------
// transportSender.Send
// ---------------------------------------------------------------------------

// TestTransportSender_SendDelegatesToTransport verifies that the engine-facing
// Send actually reaches the QUIC transport: with an empty registry the target
// is unresolvable, and the transport records the drop with the shard and
// replica of the message it was handed.
func TestTransportSender_SendDelegatesToTransport(t *testing.T) {
	metrics := &recordingTransportMetrics{}
	ts := &transportSender{
		transport: newTestTransport(t, metrics),
		logger:    slog.Default(),
	}

	ts.Send([]proto.Message{
		{Type: proto.Replicate, ShardID: 7, From: 1, To: 9},
	})

	drops := metrics.drops()
	if len(drops) != 1 {
		t.Fatalf("transport recorded %d drops, want 1 (message never reached the transport)", len(drops))
	}
	if drops[0].shardID != 7 || drops[0].to != 9 {
		t.Fatalf("drop = shard %d to %d, want shard 7 to 9", drops[0].shardID, drops[0].to)
	}
	if drops[0].reason != "unresolved_target" {
		t.Fatalf("drop reason = %q, want %q", drops[0].reason, "unresolved_target")
	}
}

// ---------------------------------------------------------------------------
// transportSender.barrierFor
// ---------------------------------------------------------------------------

// TestTransportSender_BarrierFor verifies snapshot decrypt-for-send barrier
// routing: a mapped shard uses its own barrier, an unmapped shard falls back
// to the host-wide barrier, and with no resolver every shard uses the
// host-wide barrier. Routing a shard to the wrong barrier would ship a
// snapshot decrypted with the wrong key.
func TestTransportSender_BarrierFor(t *testing.T) {
	hostWide := &mockBarrier{}
	perShard := &mockBarrier{}

	withResolver := &transportSender{
		barrier: hostWide,
		shardBarriers: func(shardID uint64) Barrier {
			if shardID == 42 {
				return perShard
			}
			return nil
		},
	}
	if got := withResolver.barrierFor(42); got != Barrier(perShard) {
		t.Fatalf("barrierFor(42) = %p, want the per-shard barrier %p", got, perShard)
	}
	if got := withResolver.barrierFor(43); got != Barrier(hostWide) {
		t.Fatalf("barrierFor(43) = %p, want the host-wide barrier %p", got, hostWide)
	}

	noResolver := &transportSender{barrier: hostWide}
	if got := noResolver.barrierFor(42); got != Barrier(hostWide) {
		t.Fatalf("barrierFor(42) without resolver = %p, want the host-wide barrier %p", got, hostWide)
	}
}

// ---------------------------------------------------------------------------
// doSendSnapshot
// ---------------------------------------------------------------------------

// TestDoSendSnapshot_PlaintextSendFailureRejects verifies that when the
// transport cannot deliver the snapshot the leader is told the transfer was
// REJECTED, carrying the transfer sequence of this attempt. Without the
// rejection the follower stays pinned in snapshot state forever.
func TestDoSendSnapshot_PlaintextSendFailureRejects(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")
	if err := os.WriteFile(path, []byte("plaintext snapshot payload"), 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	f := newSnapshotSendFixture(t, nil)
	f.sender.doSendSnapshot(testInstallSnapshotMsg(t, path, 0, 77))

	f.expectSnapshotStatus(t, true, 77)
	assertNoTempFiles(t, dir)

	// The canonical snapshot file must survive a failed send so the leader
	// can retry from it.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot file must survive a failed send: %v", err)
	}
}

// TestDoSendSnapshot_EncryptedRefusedWithoutBarrier verifies the fail-closed
// rule: an at-rest-encrypted snapshot is never shipped when the barrier that
// sealed it is unavailable (absent or sealed). Shipping ciphertext the peer
// cannot decrypt would wedge the follower, so the send must be refused and
// the leader told it was rejected.
func TestDoSendSnapshot_EncryptedRefusedWithoutBarrier(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")
	if err := os.WriteFile(path, []byte("ciphertext"), 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	t.Run("no barrier configured", func(t *testing.T) {
		f := newSnapshotSendFixture(t, nil)
		f.sender.doSendSnapshot(testInstallSnapshotMsg(t, path, 3, 11))
		f.expectSnapshotStatus(t, true, 11)
		assertNoTempFiles(t, dir)
	})

	t.Run("barrier sealed", func(t *testing.T) {
		sealed := &mockBarrier{}
		if err := sealed.Seal(); err != nil {
			t.Fatalf("seal barrier: %v", err)
		}
		f := newSnapshotSendFixture(t, sealed)
		f.sender.doSendSnapshot(testInstallSnapshotMsg(t, path, 3, 12))
		f.expectSnapshotStatus(t, true, 12)
		assertNoTempFiles(t, dir)
	})
}

// TestDoSendSnapshot_DecryptFailureRejects verifies that an undecryptable
// snapshot file is reported as a rejected transfer rather than being shipped
// as-is, and that the failed decrypt leaves no scratch file behind.
func TestDoSendSnapshot_DecryptFailureRejects(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")

	// A valid frame length followed by bytes that are not a valid AEAD frame.
	var frameLen [4]byte
	binary.LittleEndian.PutUint32(frameLen[:], 64)
	corrupt := append(frameLen[:], make([]byte, 64)...)
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	b := createTestBarrier(t)
	f := newSnapshotSendFixture(t, b)
	f.sender.doSendSnapshot(testInstallSnapshotMsg(t, path, b.CurrentEpoch(), 21))

	f.expectSnapshotStatus(t, true, 21)
	assertNoTempFiles(t, dir)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("encrypted snapshot must survive a failed decrypt: %v", err)
	}
}

// TestDoSendSnapshot_DecryptedTempFileRemoved verifies the decrypt-for-send
// path end to end: an encrypted snapshot is decrypted to a scratch file, and
// that scratch file is removed once the send finishes — here by failing at the
// transport, which is the path most likely to leak it. The canonical
// ciphertext must be left untouched so the transfer stays retryable.
func TestDoSendSnapshot_DecryptedTempFileRemoved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")

	b := createTestBarrier(t)
	plaintext := []byte("encrypted-at-rest snapshot payload")
	writeEncryptedSnapshotFile(t, b, path, plaintext)

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}

	f := newSnapshotSendFixture(t, b)
	msg := testInstallSnapshotMsg(t, path, b.CurrentEpoch(), 33)
	f.sender.doSendSnapshot(msg)

	f.expectSnapshotStatus(t, true, 33)
	assertNoTempFiles(t, dir)

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("re-read snapshot: %v", err)
	}
	if string(before) != string(after) {
		t.Fatal("decrypt-for-send modified the canonical encrypted snapshot")
	}
}

// TestSendSnapshot_AsyncRejectsAndDrains verifies the exported async entry
// point: SendSnapshot runs the transfer on its own goroutine and Wait blocks
// until it finishes, at which point the rejection has been delivered.
func TestSendSnapshot_AsyncRejectsAndDrains(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")
	if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	f := newSnapshotSendFixture(t, nil)
	f.sender.SendSnapshot(testInstallSnapshotMsg(t, path, 0, 44))
	f.sender.Wait()

	f.expectSnapshotStatus(t, true, 44)
}

// errTestRemoveFailed models a scratch file that cannot be reclaimed.
var errTestRemoveFailed = errors.New("test fs: remove refused")

// removeFailFS is the real filesystem with a Remove that always fails, so the
// decrypt-for-send cleanup path can be observed failing.
type removeFailFS struct{ osSnapshotFS }

func (removeFailFS) Remove(string) error { return errTestRemoveFailed }

// TestDoSendSnapshot_TempFileRemovalFailureIsNotFatal verifies that a scratch
// file that cannot be reclaimed does not derail the transfer: the leader still
// receives its SnapshotStatus. A cleanup error must be logged, never
// propagated as a send failure or a panic.
func TestDoSendSnapshot_TempFileRemovalFailureIsNotFatal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")

	b := createTestBarrier(t)
	writeEncryptedSnapshotFile(t, b, path, []byte("payload that decrypts cleanly"))

	f := newSnapshotSendFixture(t, b)
	f.sender.fsys = removeFailFS{}
	f.sender.doSendSnapshot(testInstallSnapshotMsg(t, path, b.CurrentEpoch(), 55))

	f.expectSnapshotStatus(t, true, 55)

	// The scratch file is still there precisely because Remove failed; the
	// point is that the transfer completed its notification regardless.
	matches, err := filepath.Glob(filepath.Join(dir, sendDecryptTempPattern))
	if err != nil {
		t.Fatalf("glob temp files: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("found %d scratch files, want the 1 whose removal was refused", len(matches))
	}
}
