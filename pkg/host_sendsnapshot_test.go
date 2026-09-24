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
	"io"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	eng, node := newLoadedEngine(t, 1, 1)

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

// newLoadedEngine starts an engine holding one loaded node for shardID.
func newLoadedEngine(t *testing.T, shardID, replicaID uint64) (*engine.Engine, *engine.Node) {
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

	node := newMinimalEngineNode(t, shardID, replicaID)
	// StartShard creates the replica's snapshot directory, where the send
	// creates its scratch.
	if err := os.MkdirAll(node.ReplicaSnapshotDir(), 0o750); err != nil {
		t.Fatalf("mkdir replica snapshot dir: %v", err)
	}
	eng.LoadNode(node)
	t.Cleanup(func() { eng.UnloadNode(shardID) })
	return eng, node
}

// newDecryptSender returns a transportSender for the decrypt-for-send path
// alone: its engine holds a loaded node for (shardID, replicaID), from which
// the send resolves its scratch directory, and it has no transport.
func newDecryptSender(t *testing.T, b Barrier, shardID, replicaID uint64) *transportSender {
	t.Helper()
	eng, _ := newLoadedEngine(t, shardID, replicaID)
	return &transportSender{engine: eng, barrier: b, logger: slog.Default()}
}

// sendSnapshotSync runs one transfer the way SendSnapshot does — the artifact
// opened first, then handed to doSendSnapshot — but on the caller's goroutine.
func sendSnapshotSync(s *transportSender, msg proto.Message) {
	var src snapshotFile
	var openErr error
	if msg.Snapshot.Filepath != "" {
		src, openErr = s.snapshotFsys().Open(msg.Snapshot.Filepath)
	}
	if src != nil {
		defer func() { _ = src.Close() }()
	}
	s.doSendSnapshot(msg, src, openErr)
}

// decryptForSendToPath runs the send's handle-based decrypt on msg's artifact
// and copies the plaintext into a named file under t.TempDir(), returning its
// path and the plaintext size, so a test can inspect or re-stream it.
func decryptForSendToPath(t *testing.T, s *transportSender, msg proto.Message) (string, uint64, error) {
	t.Helper()
	src, err := s.snapshotFsys().Open(msg.Snapshot.Filepath)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = src.Close() }()
	dec, size, err := s.decryptSnapshotForSend(src, msg, s.barrier)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = dec.Close() }()
	plain, err := io.ReadAll(dec)
	if err != nil {
		return "", 0, err
	}
	path := filepath.Join(t.TempDir(), "decrypted.dat")
	if err := os.WriteFile(path, plain, 0o600); err != nil {
		return "", 0, err
	}
	return path, size, nil
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

// assertNoTempFiles fails when a decrypt-for-send scratch file was left in the
// artifact's directory dir or in the node's replica snapshot directory, where
// the send creates it.
func (f *snapshotSendFixture) assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	for _, d := range []string{dir, f.node.ReplicaSnapshotDir()} {
		matches, err := filepath.Glob(filepath.Join(d, sendDecryptTempPattern))
		if err != nil {
			t.Fatalf("glob temp files: %v", err)
		}
		if len(matches) > 0 {
			t.Fatalf("decrypt-for-send temp files leaked: %v", matches)
		}
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
	sendSnapshotSync(f.sender, testInstallSnapshotMsg(t, path, 0, 77))

	f.expectSnapshotStatus(t, true, 77)
	f.assertNoTempFiles(t, dir)

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
		sendSnapshotSync(f.sender, testInstallSnapshotMsg(t, path, 3, 11))
		f.expectSnapshotStatus(t, true, 11)
		f.assertNoTempFiles(t, dir)
	})

	t.Run("barrier sealed", func(t *testing.T) {
		sealed := &mockBarrier{}
		if err := sealed.Seal(); err != nil {
			t.Fatalf("seal barrier: %v", err)
		}
		f := newSnapshotSendFixture(t, sealed)
		sendSnapshotSync(f.sender, testInstallSnapshotMsg(t, path, 3, 12))
		f.expectSnapshotStatus(t, true, 12)
		f.assertNoTempFiles(t, dir)
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
	sendSnapshotSync(f.sender, testInstallSnapshotMsg(t, path, b.CurrentEpoch(), 21))

	f.expectSnapshotStatus(t, true, 21)
	f.assertNoTempFiles(t, dir)
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
	sendSnapshotSync(f.sender, msg)

	f.expectSnapshotStatus(t, true, 33)
	f.assertNoTempFiles(t, dir)

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
	writeEncryptedSnapshotFileFor(t, b, path, []byte("payload that decrypts cleanly"), 1, 1, 100)

	f := newSnapshotSendFixture(t, b)
	f.sender.fsys = removeFailFS{}
	before := engine.StaleDecryptSweepFailures()
	// The artifact's own identity, so the decrypt succeeds and the scratch is
	// handed to the send — whose cleanup retries the unlink.
	msg := testInstallSnapshotMsg(t, path, b.CurrentEpoch(), 55)
	msg.ReplicaID = 1
	sendSnapshotSync(f.sender, msg)

	f.expectSnapshotStatus(t, true, 55)

	// The scratch file is still there precisely because Remove failed — at
	// creation and again at close; the point is that the transfer completed
	// its notification regardless, and that the unreclaimed plaintext is
	// counted. It is created in the node's replica snapshot directory, which
	// the stale-scratch sweep covers.
	matches, err := filepath.Glob(filepath.Join(f.node.ReplicaSnapshotDir(), sendDecryptTempPattern))
	if err != nil {
		t.Fatalf("glob temp files: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("found %d scratch files, want the 1 whose removal was refused", len(matches))
	}
	// The artifact sits outside the node's tree, as a record may name it; the
	// scratch must not have followed it there.
	if beside, _ := filepath.Glob(filepath.Join(filepath.Dir(dir), sendDecryptTempPattern)); len(beside) != 0 {
		t.Fatalf("scratch created beside the artifact, outside the replica's swept tree: %v", beside)
	}
	if engine.StaleDecryptSweepFailures() == before {
		t.Fatal("an unreclaimed plaintext scratch file was not counted")
	}
}

// TestDecryptSnapshotForSend_UnloadedShardFailsClosed: the send resolves its
// scratch directory from the loaded node, never from the artifact's path, so
// a shard that is no longer loaded — or is loaded as a different replica — is
// refused before anything is created.
func TestDecryptSnapshotForSend_UnloadedShardFailsClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")

	b := createTestBarrier(t)
	writeEncryptedSnapshotFile(t, b, path, []byte("payload that decrypts cleanly"))
	sender := newDecryptSender(t, b, testSnapShardID, testSnapReplicaID)

	src, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = src.Close() }()

	for _, tc := range []struct {
		name               string
		shardID, replicaID uint64
	}{
		{"shard not loaded", testSnapShardID + 1, testSnapReplicaID},
		// A send of a stopped incarnation's artifact still running after
		// the shard came back as a different replica.
		{"shard loaded as another replica", testSnapShardID, testSnapReplicaID + 1},
	} {
		msg := testInstallSnapshotMsg(t, path, b.CurrentEpoch(), 58)
		msg.ShardID, msg.ReplicaID = tc.shardID, tc.replicaID
		dec, _, err := sender.decryptSnapshotForSend(src, msg, b)
		if dec != nil {
			_ = dec.Close()
		}
		if !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("%s: decrypt = %v, want ErrShardNotFound", tc.name, err)
		}
	}
	for _, d := range []string{dir, filepath.Dir(dir)} {
		if left, _ := filepath.Glob(filepath.Join(d, sendDecryptTempPattern)); len(left) != 0 {
			t.Fatalf("a refused send created scratch files: %v", left)
		}
	}
}

// TestDecryptSnapshotForSend_DoesNotRecreateARemovedReplicaDir: StartShard
// creates the replica's snapshot directory and no scratch writer does, so a
// late send whose tree was removed under it fails rather than resurrecting
// the directory of a replica that is gone.
func TestDecryptSnapshotForSend_DoesNotRecreateARemovedReplicaDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")

	b := createTestBarrier(t)
	writeEncryptedSnapshotFile(t, b, path, []byte("payload that decrypts cleanly"))
	eng, node := newLoadedEngine(t, testSnapShardID, testSnapReplicaID)
	sender := &transportSender{engine: eng, barrier: b, logger: slog.Default()}
	if err := os.RemoveAll(node.ReplicaSnapshotDir()); err != nil {
		t.Fatalf("remove replica dir: %v", err)
	}

	src, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = src.Close() }()

	msg := testInstallSnapshotMsg(t, path, b.CurrentEpoch(), 59)
	msg.ShardID, msg.ReplicaID, msg.Snapshot.Index = testSnapShardID, testSnapReplicaID, testSnapIndex
	dec, _, err := sender.decryptSnapshotForSend(src, msg, b)
	if dec != nil {
		_ = dec.Close()
	}
	if err == nil {
		t.Fatal("a send whose replica directory was removed succeeded")
	}
	if _, statErr := os.Stat(node.ReplicaSnapshotDir()); !os.IsNotExist(statErr) {
		t.Fatalf("the send recreated the removed replica directory (stat err=%v)", statErr)
	}
}

// removeFailOnceFS refuses the first Remove only.
type removeFailOnceFS struct {
	osSnapshotFS
	failed *atomic.Bool
}

func (fs removeFailOnceFS) Remove(name string) error {
	if fs.failed.CompareAndSwap(false, true) {
		return errTestRemoveFailed
	}
	return fs.osSnapshotFS.Remove(name)
}

// TestDoSendSnapshot_FailedUnlinkIsRetriedAtClose: when the scratch file's
// immediate unlink fails, the plaintext it is then filled with must not keep
// its name for the life of the process — a leader rarely runs the recovery
// that would sweep it. Closing the scratch retries the unlink.
func TestDoSendSnapshot_FailedUnlinkIsRetriedAtClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")

	b := createTestBarrier(t)
	writeEncryptedSnapshotFileFor(t, b, path, []byte("payload that decrypts cleanly"), 1, 1, 100)

	f := newSnapshotSendFixture(t, b)
	f.sender.fsys = removeFailOnceFS{failed: new(atomic.Bool)}
	before := engine.StaleDecryptSweepFailures()
	// The artifact's own identity, so the decrypt succeeds and the scratch is
	// handed to the send — whose cleanup, not the decrypt's, closes it.
	msg := testInstallSnapshotMsg(t, path, b.CurrentEpoch(), 56)
	msg.ReplicaID = 1
	sendSnapshotSync(f.sender, msg)

	f.expectSnapshotStatus(t, true, 56)
	f.assertNoTempFiles(t, dir)
	if got := engine.StaleDecryptSweepFailures(); got != before {
		t.Fatalf("a reclaimed scratch file was counted as unreclaimed: %d -> %d", before, got)
	}
}

// panicWriteFile panics on Write and records its Close.
type panicWriteFile struct {
	snapshotFile
	closed *atomic.Bool
}

func (panicWriteFile) Write([]byte) (int, error) { panic("injected: scratch write") }

func (f panicWriteFile) Close() error {
	f.closed.Store(true)
	return f.snapshotFile.Close()
}

// panicScratchFS hands out scratch files that panic when written.
type panicScratchFS struct {
	osSnapshotFS
	closed *atomic.Bool
}

func (fs panicScratchFS) CreateTemp(dir, pattern string) (snapshotFile, error) {
	f, err := fs.osSnapshotFS.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	return panicWriteFile{snapshotFile: f, closed: fs.closed}, nil
}

// TestDecryptSnapshotForSend_PanicClosesTheScratch: a panic inside the
// decrypt — a custom barrier or filesystem — is recovered by SendSnapshot,
// and the scratch descriptor must not leak with it. Automatic snapshot
// retries would otherwise repeat the leak until descriptors run out.
func TestDecryptSnapshotForSend_PanicClosesTheScratch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")

	b := createTestBarrier(t)
	writeEncryptedSnapshotFile(t, b, path, []byte("payload that decrypts cleanly"))

	sender := newDecryptSender(t, b, testSnapShardID, testSnapReplicaID)
	closed := new(atomic.Bool)
	sender.fsys = panicScratchFS{closed: closed}

	src, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = src.Close() }()

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the injected scratch-write panic did not propagate")
			}
		}()
		msg := testInstallSnapshotMsg(t, path, b.CurrentEpoch(), 57)
		// The frames' positional context, so the decrypt reaches the write.
		msg.ShardID, msg.ReplicaID, msg.Snapshot.Index = testSnapShardID, testSnapReplicaID, testSnapIndex
		_, _, _ = sender.decryptSnapshotForSend(src, msg, b)
	}()
	if !closed.Load() {
		t.Fatal("a panic inside the decrypt leaked the scratch descriptor")
	}
}
