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
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// collectSnapshotChunks is a test helper that collects all snapshot chunks
// into a slice. Production code uses forEachSnapshotChunk directly to avoid
// holding all chunk data in memory simultaneously.
func collectSnapshotChunks(msg proto.Message) ([]proto.SnapshotChunk, error) {
	snap := &msg.Snapshot
	fileSize := snap.FileSize
	var chunkCount uint64
	if snap.Filepath == "" || fileSize == 0 {
		chunkCount = 1
	} else {
		chunkCount = (fileSize-1)/snapshotChunkSize + 1
	}

	chunks := make([]proto.SnapshotChunk, 0, chunkCount)
	err := forEachSnapshotChunk(osSnapshotFS{}, msg, func(chunk proto.SnapshotChunk) error {
		chunks = append(chunks, chunk)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return chunks, nil
}

// newMinimalEngineNode creates a minimal engine.Node for tests that only need
// inbox delivery. Passes nil for peer, sm, logReader, snapshotter, and listener
// since the test only exercises message delivery.
//
// The snapshot root is t.TempDir(), never "". An empty root makes
// Node.SnapshotSaveDir return the RELATIVE path "shard-<N>/replica-<M>/
// snapshot-<index>", so anything that touches it writes into the package
// source directory and leaves committed-looking artifacts behind.
func newMinimalEngineNode(t *testing.T, shardID, replicaID uint64) *engine.Node {
	t.Helper()
	cfg := config.Config{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
	return engine.NewNode(nil, nil, nil, cfg, nil, t.TempDir(), nil)
}

// TestSplitSnapshotMessage_EmptySnapshot verifies that an InstallSnapshot
// message with no filepath or filesize produces a single metadata-only chunk.
func TestSplitSnapshotMessage_EmptySnapshot(t *testing.T) {
	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
		Snapshot: proto.Snapshot{
			Index:    100,
			Term:     5,
			Filepath: "",
			FileSize: 0,
			Membership: proto.Membership{
				ConfigChangeID: 10,
				Addresses: map[uint64]string{
					1: "127.0.0.1:5001",
				},
			},
		},
	}

	chunks, err := collectSnapshotChunks(msg)
	if err != nil {
		t.Fatalf("collectSnapshotChunks returned error: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]
	if c.ShardID != 1 {
		t.Errorf("ShardID = %d, want 1", c.ShardID)
	}
	if c.ReplicaID != 2 {
		t.Errorf("ReplicaID = %d, want 2", c.ReplicaID)
	}
	if c.From != 1 {
		t.Errorf("From = %d, want 1", c.From)
	}
	if c.Index != 100 {
		t.Errorf("Index = %d, want 100", c.Index)
	}
	if c.Term != 5 {
		t.Errorf("Term = %d, want 5", c.Term)
	}
	if c.ChunkCount != 1 {
		t.Errorf("ChunkCount = %d, want 1", c.ChunkCount)
	}
	if c.ChunkSize != 0 {
		t.Errorf("ChunkSize = %d, want 0", c.ChunkSize)
	}
	if len(c.Data) != 0 {
		t.Errorf("Data = %d bytes, want none", len(c.Data))
	}
	if c.FileSize != 0 {
		t.Errorf("FileSize = %d, want 0", c.FileSize)
	}
	if c.BinVer != proto.WireVersion {
		t.Errorf("BinVer = %d, want %d", c.BinVer, proto.WireVersion)
	}
	if c.Membership.ConfigChangeID != 10 {
		t.Errorf("Membership.ConfigChangeID = %d, want 10", c.Membership.ConfigChangeID)
	}
}

// TestSplitSnapshotMessage_SmallFile verifies splitting a file that fits
// in a single chunk (smaller than snapshotChunkSize).
func TestSplitSnapshotMessage_SmallFile(t *testing.T) {
	tmpDir := t.TempDir()
	snapPath := filepath.Join(tmpDir, "snapshot.dat")
	data := []byte("small snapshot data for testing")
	if err := os.WriteFile(snapPath, data, 0o600); err != nil {
		t.Fatalf("failed to write snapshot file: %v", err)
	}
	stampTestSnapshotMeta(t, filepath.Dir(snapPath), 1, 1)

	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 10,
		From:    1,
		To:      3,
		Snapshot: proto.Snapshot{
			Index:    500,
			Term:     12,
			Filepath: snapPath,
			FileSize: uint64(len(data)),
		},
	}

	chunks, err := collectSnapshotChunks(msg)
	if err != nil {
		t.Fatalf("collectSnapshotChunks returned error: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]
	if c.ShardID != 10 {
		t.Errorf("ShardID = %d, want 10", c.ShardID)
	}
	if c.ReplicaID != 3 {
		t.Errorf("ReplicaID = %d, want 3", c.ReplicaID)
	}
	if c.From != 1 {
		t.Errorf("From = %d, want 1", c.From)
	}
	if c.Index != 500 {
		t.Errorf("Index = %d, want 500", c.Index)
	}
	if c.ChunkID != 0 {
		t.Errorf("ChunkID = %d, want 0", c.ChunkID)
	}
	if c.ChunkCount != 1 {
		t.Errorf("ChunkCount = %d, want 1", c.ChunkCount)
	}
	if c.ChunkSize != uint64(len(data)) {
		t.Errorf("ChunkSize = %d, want %d", c.ChunkSize, len(data))
	}
	if c.FileSize != uint64(len(data)) {
		t.Errorf("FileSize = %d, want %d", c.FileSize, len(data))
	}
	if !bytes.Equal(c.Data, data) {
		t.Errorf("Data mismatch: got %q, want %q", c.Data, data)
	}
}

// TestSplitSnapshotMessage_MultiChunk verifies that a file larger than
// snapshotChunkSize is correctly split into multiple chunks.
func TestSplitSnapshotMessage_MultiChunk(t *testing.T) {
	tmpDir := t.TempDir()
	snapPath := filepath.Join(tmpDir, "snapshot.dat")

	// Create data slightly larger than 2 chunks (2*snapshotChunkSize + extra).
	totalSize := snapshotChunkSize*2 + 1024
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := os.WriteFile(snapPath, data, 0o600); err != nil {
		t.Fatalf("failed to write snapshot file: %v", err)
	}
	stampTestSnapshotMeta(t, filepath.Dir(snapPath), 1, 1)

	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
		Snapshot: proto.Snapshot{
			Index:    1000,
			Term:     15,
			Filepath: snapPath,
			FileSize: uint64(totalSize),
			Membership: proto.Membership{
				ConfigChangeID: 99,
				Addresses: map[uint64]string{
					1: "127.0.0.1:5001",
					2: "127.0.0.1:5002",
				},
			},
			OnDiskIndex: 42,
		},
	}

	chunks, err := collectSnapshotChunks(msg)
	if err != nil {
		t.Fatalf("collectSnapshotChunks returned error: %v", err)
	}

	expectedChunks := uint64(3) // 2MB + 2MB + 1KB
	if uint64(len(chunks)) != expectedChunks {
		t.Fatalf("expected %d chunks, got %d", expectedChunks, len(chunks))
	}

	// Verify each chunk's metadata.
	for i, c := range chunks {
		if c.ChunkID != uint64(i) {
			t.Errorf("chunk[%d].ChunkID = %d, want %d", i, c.ChunkID, i)
		}
		if c.ChunkCount != expectedChunks {
			t.Errorf("chunk[%d].ChunkCount = %d, want %d", i, c.ChunkCount, expectedChunks)
		}
		if c.FileChunkID != uint64(i) {
			t.Errorf("chunk[%d].FileChunkID = %d, want %d", i, c.FileChunkID, i)
		}
		if c.FileChunkCount != expectedChunks {
			t.Errorf("chunk[%d].FileChunkCount = %d, want %d", i, c.FileChunkCount, expectedChunks)
		}
		if c.FileSize != uint64(totalSize) {
			t.Errorf("chunk[%d].FileSize = %d, want %d", i, c.FileSize, totalSize)
		}
		if c.ShardID != 1 {
			t.Errorf("chunk[%d].ShardID = %d, want 1", i, c.ShardID)
		}
		if c.Index != 1000 {
			t.Errorf("chunk[%d].Index = %d, want 1000", i, c.Index)
		}
		if c.BinVer != proto.WireVersion {
			t.Errorf("chunk[%d].BinVer = %d, want %d", i, c.BinVer, proto.WireVersion)
		}
		if c.OnDiskIndex != 42 {
			t.Errorf("chunk[%d].OnDiskIndex = %d, want 42", i, c.OnDiskIndex)
		}
	}

	// Verify the first two chunks are exactly snapshotChunkSize.
	if chunks[0].ChunkSize != uint64(snapshotChunkSize) {
		t.Errorf("chunk[0].ChunkSize = %d, want %d", chunks[0].ChunkSize, snapshotChunkSize)
	}
	if chunks[1].ChunkSize != uint64(snapshotChunkSize) {
		t.Errorf("chunk[1].ChunkSize = %d, want %d", chunks[1].ChunkSize, snapshotChunkSize)
	}
	// Last chunk is the remainder.
	if chunks[2].ChunkSize != 1024 {
		t.Errorf("chunk[2].ChunkSize = %d, want 1024", chunks[2].ChunkSize)
	}

	// Reassemble and verify the full data.
	var reassembled []byte
	for _, c := range chunks {
		reassembled = append(reassembled, c.Data...)
	}
	if !bytes.Equal(reassembled, data) {
		t.Fatal("reassembled data does not match original")
	}
}

// TestSplitSnapshotMessage_FileNotFound verifies that a missing snapshot file
// returns a SnapshotSendError.
func TestSplitSnapshotMessage_FileNotFound(t *testing.T) {
	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
		Snapshot: proto.Snapshot{
			Index:    100,
			Term:     5,
			Filepath: "/nonexistent/path/snapshot.dat",
			FileSize: 1024,
		},
	}

	_, err := collectSnapshotChunks(msg)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}

	var sendErr *SnapshotSendError
	if !errors.As(err, &sendErr) {
		t.Fatalf("expected *SnapshotSendError, got %T: %v", err, err)
	}
	if sendErr.ShardID != 1 {
		t.Errorf("ShardID = %d, want 1", sendErr.ShardID)
	}
	if sendErr.ReplicaID != 2 {
		t.Errorf("ReplicaID = %d, want 2", sendErr.ReplicaID)
	}
	if sendErr.Op != "read" {
		t.Errorf("Op = %q, want %q", sendErr.Op, "read")
	}
}

// TestSplitSnapshotMessage_SizeMismatch verifies that a size mismatch between
// the declared FileSize and actual file size returns ErrSnapshotSizeMismatch.
func TestSplitSnapshotMessage_SizeMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	snapPath := filepath.Join(tmpDir, "snapshot.dat")
	data := []byte("actual data is shorter")
	if err := os.WriteFile(snapPath, data, 0o600); err != nil {
		t.Fatalf("failed to write snapshot file: %v", err)
	}

	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
		Snapshot: proto.Snapshot{
			Index:    100,
			Term:     5,
			Filepath: snapPath,
			FileSize: uint64(len(data) + 100), // Declared size larger than actual.
		},
	}

	_, err := collectSnapshotChunks(msg)
	if err == nil {
		t.Fatal("expected error for size mismatch, got nil")
	}

	var sendErr *SnapshotSendError
	if !errors.As(err, &sendErr) {
		t.Fatalf("expected *SnapshotSendError, got %T: %v", err, err)
	}
	if !errors.Is(sendErr.Err, ErrSnapshotSizeMismatch) {
		t.Errorf("expected ErrSnapshotSizeMismatch in chain, got: %v", sendErr.Err)
	}
}

// TestSplitSnapshotMessage_ExactChunkBoundary verifies correct behavior when
// the file size is exactly a multiple of the chunk size.
func TestSplitSnapshotMessage_ExactChunkBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	snapPath := filepath.Join(tmpDir, "snapshot.dat")

	// Exactly 2 chunks worth of data.
	totalSize := snapshotChunkSize * 2
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 251) // prime modulus for varied data
	}
	if err := os.WriteFile(snapPath, data, 0o600); err != nil {
		t.Fatalf("failed to write snapshot file: %v", err)
	}
	stampTestSnapshotMeta(t, filepath.Dir(snapPath), 1, 1)

	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
		Snapshot: proto.Snapshot{
			Index:    200,
			Term:     8,
			Filepath: snapPath,
			FileSize: uint64(totalSize),
		},
	}

	chunks, err := collectSnapshotChunks(msg)
	if err != nil {
		t.Fatalf("collectSnapshotChunks returned error: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}

	// Both chunks should be exactly snapshotChunkSize.
	for i, c := range chunks {
		if c.ChunkSize != uint64(snapshotChunkSize) {
			t.Errorf("chunk[%d].ChunkSize = %d, want %d", i, c.ChunkSize, snapshotChunkSize)
		}
		if len(c.Data) != snapshotChunkSize {
			t.Errorf("chunk[%d].Data len = %d, want %d", i, len(c.Data), snapshotChunkSize)
		}
	}

	// Verify data integrity.
	var reassembled []byte
	for _, c := range chunks {
		reassembled = append(reassembled, c.Data...)
	}
	if !bytes.Equal(reassembled, data) {
		t.Fatal("reassembled data does not match original")
	}
}

// TestTransportSender_NotifySnapshotStatus verifies that the transportSender
// delivers SnapshotStatus messages, rejected and accepted, back to the raft
// layer via the engine, and drops one for a shard the host does not run.
func TestTransportSender_NotifySnapshotStatus(t *testing.T) {
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	sender := &noopSender{}
	eng := engine.NewEngine(cfg, nil, sender, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	node := newMinimalEngineNode(t, 1, 1)
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	ts := &transportSender{
		engine: eng,
		logger: slog.Default(),
	}

	// A status for a shard this host does not run is dropped (DeliverMessage
	// returns false): no panic, and nothing reaches the loaded shard.
	ts.notifySnapshotStatus(99, 1, 2, 10, true)
	if buf := node.DrainInbox(nil); len(buf) != 0 {
		t.Fatalf("status for an unknown shard reached shard 1: %d message(s)", len(buf))
	}

	for _, rejected := range []bool{true, false} {
		ts.notifySnapshotStatus(1, 1, 2, 10, rejected)

		buf := node.DrainInbox(nil)
		if len(buf) != 1 {
			t.Fatalf("rejected=%v: expected 1 message in inbox, got %d", rejected, len(buf))
		}
		m := buf[0]
		if m.Type != proto.SnapshotStatus {
			t.Errorf("rejected=%v: Type = %v, want SnapshotStatus", rejected, m.Type)
		}
		if m.ShardID != 1 {
			t.Errorf("rejected=%v: ShardID = %d, want 1", rejected, m.ShardID)
		}
		if m.From != 2 {
			t.Errorf("rejected=%v: From = %d, want 2 (target replica)", rejected, m.From)
		}
		if m.To != 1 {
			t.Errorf("rejected=%v: To = %d, want 1 (local replica)", rejected, m.To)
		}
		if m.Reject != rejected {
			t.Errorf("Reject = %v, want %v", m.Reject, rejected)
		}
	}
}

// TestSnapshotSendError_Format verifies the error message format.
func TestSnapshotSendError_Format(t *testing.T) {
	inner := errors.New("file not found")
	err := &SnapshotSendError{
		ShardID:   1,
		ReplicaID: 2,
		Op:        "read",
		Err:       inner,
	}

	expected := "quicraft: snapshot send read failed for shard 1 replica 2: file not found"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	if err.Unwrap() != inner {
		t.Error("Unwrap() should return inner error")
	}
}

// TestDecryptSnapshotForSend_RoundTrip verifies that doSendSnapshot decrypts
// an encrypted snapshot file before sending, producing plaintext chunks that
// a receiver with a different barrier key can read without decryption.
func TestDecryptSnapshotForSend_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a barrier and encrypt a snapshot file using the same frame
	// format as snapshot_pool.encryptSnapshotFile: [4-byte LE len][data]...
	b := createTestBarrier(t)

	plaintext := []byte("snapshot data for decryptSnapshotForSend test")
	encPath := filepath.Join(tmpDir, "snapshot.dat")
	writeEncryptedSnapshotFile(t, b, encPath, plaintext)

	// Get the encrypted file size.
	info, err := os.Stat(encPath)
	if err != nil {
		t.Fatalf("stat encrypted file: %v", err)
	}

	ts := newDecryptSender(t, b, testSnapShardID, testSnapReplicaID)

	// The identity must match the one the frames were sealed under: the send
	// path reproduces the positional context from (ShardID, ReplicaID, Index).
	msg := proto.Message{
		ShardID:   testSnapShardID,
		ReplicaID: testSnapReplicaID,
		Snapshot: proto.Snapshot{
			Index:    testSnapIndex,
			Filepath: encPath,
			FileSize: uint64(info.Size()),
			Epoch:    b.CurrentEpoch(),
		},
	}

	decPath, decSize, err := decryptForSendToPath(t, ts, msg)
	if err != nil {
		t.Fatalf("decryptSnapshotForSend: %v", err)
	}
	defer func() { _ = os.Remove(decPath) }()

	if decSize != uint64(len(plaintext)) {
		t.Errorf("decrypted size = %d, want %d", decSize, len(plaintext))
	}

	got, err := os.ReadFile(decPath)
	if err != nil {
		t.Fatalf("read decrypted file: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Errorf("decrypted data mismatch: got %q, want %q", got, plaintext)
	}
}

// TestSendSnapshot_SurvivesItsDirectoryBeingRemoved: a send that read its
// artifact by PATH — the decrypt, then every chunk — could have the directory
// removed underneath it by a newer snapshot's GC, or the next incarnation's, or
// an offline import's. The send opens the artifact before the
// hand-off and reads only through handles, decrypting into an anonymous
// scratch file, so removing the directory after the open changes nothing.
func TestSendSnapshot_SurvivesItsDirectoryBeingRemoved(t *testing.T) {
	newArtifact := func(t *testing.T) (string, string) {
		t.Helper()
		base := t.TempDir()
		dir := filepath.Join(base, "snapshot-00000000000000000100")
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		return dir, filepath.Join(dir, "snapshot.dat")
	}

	t.Run("plaintext stream", func(t *testing.T) {
		dir, path := newArtifact(t)
		payload := bytes.Repeat([]byte("state machine "), 1000)
		writeRecordedSnapshot(t, dir, payload)
		// The integrity record is read at open, so the removal below cannot
		// take it from under the stream either.
		src, err := openVerifiedSnapshotFile(osSnapshotFS{}, path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = src.Close() }()
		if err := os.RemoveAll(dir); err != nil { // the GC runs
			t.Fatalf("remove: %v", err)
		}

		msg := proto.Message{ShardID: 1, To: 2, Snapshot: proto.Snapshot{
			Index: 100, Filepath: path, FileSize: uint64(len(payload)),
		}}
		var got []byte
		if err := streamSnapshotChunks(src, msg, func(c proto.SnapshotChunk) error {
			got = append(got, c.Data...)
			return nil
		}, src.Verify); err != nil {
			t.Fatalf("streaming after the directory was removed: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("streamed %d bytes, want the %d-byte artifact", len(got), len(payload))
		}
	})

	t.Run("encrypted decrypt", func(t *testing.T) {
		dir, path := newArtifact(t)
		b := createTestBarrier(t)
		plaintext := []byte("encrypted state machine that must still decrypt")
		writeEncryptedSnapshotFile(t, b, path, plaintext)
		src, err := os.Open(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = src.Close() }()
		if err := os.RemoveAll(dir); err != nil { // the GC runs
			t.Fatalf("remove: %v", err)
		}

		ts := newDecryptSender(t, b, testSnapShardID, testSnapReplicaID)
		msg := proto.Message{ShardID: testSnapShardID, ReplicaID: testSnapReplicaID, Snapshot: proto.Snapshot{
			Index: testSnapIndex, Filepath: path, Epoch: b.CurrentEpoch(),
		}}
		dec, size, err := ts.decryptSnapshotForSend(src, msg, b)
		if err != nil {
			t.Fatalf("decrypting after the directory was removed: %v", err)
		}
		defer func() { _ = dec.Close() }()
		got, err := io.ReadAll(dec)
		if err != nil {
			t.Fatalf("read decrypted: %v", err)
		}
		if size != uint64(len(plaintext)) || !bytes.Equal(got, plaintext) {
			t.Fatalf("decrypted %d bytes %q, want %q", size, got, plaintext)
		}
		// The scratch file is anonymous: nothing named is left beside the base.
		if left, _ := filepath.Glob(filepath.Join(filepath.Dir(dir), "*")); len(left) != 0 {
			t.Fatalf("the decrypt left named files behind: %v", left)
		}
	})
}

// TestSendSnapshot_WitnessInAnEncryptedClusterIsMetadataOnly drives the real
// SendSnapshot — its goroutine, its source handling, its panic recovery — with
// a witness's snapshot: no data file, but the encrypted cluster's epoch on the
// record. It must not reach the decrypt with no source, which would create a
// scratch file in the process's working directory and panic on a nil read,
// leaking that descriptor on every send the leader retried. It is metadata only, sent as
// its single empty chunk at epoch 0; nothing is opened or created.
func TestSendSnapshot_WitnessInAnEncryptedClusterIsMetadataOnly(t *testing.T) {
	b := createTestBarrier(t)
	f := newSnapshotSendFixture(t, b)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	before, _ := filepath.Glob(filepath.Join(cwd, sendDecryptTempPattern))

	var logs bytes.Buffer
	f.sender.logger = slog.New(slog.NewTextHandler(&logs, nil))

	// A witness's snapshot: no data file, the cluster's epoch on the record.
	msg := proto.Message{
		Type: proto.InstallSnapshot, ShardID: 1, From: 1, To: 2, Hint: 91,
		Snapshot: proto.Snapshot{Index: 100, Term: 5, Epoch: b.CurrentEpoch()},
	}
	f.sender.SendSnapshot(msg)
	f.sender.snapshotWg.Wait()

	// The fixture's transport cannot deliver, so the leader is told the
	// transfer was rejected — by the transport, not by a recovered panic.
	f.expectSnapshotStatus(t, true, 91)
	if strings.Contains(logs.String(), "panic recovered") {
		t.Fatalf("the witness send panicked:\n%s", logs.String())
	}
	after, _ := filepath.Glob(filepath.Join(cwd, sendDecryptTempPattern))
	if len(after) != len(before) {
		t.Fatalf("a witness send created decrypt scratch files in the working directory: %v", after)
	}
}

// createTestBarrier creates a real seal.Barrier initialized with fast
// Argon2id parameters for testing. A no-op StatePersister is wired in
// because Rotate and PurgeEpochsBefore fail closed with ErrNoStatePersister
// without one: an unpersisted rotation would lose the new epoch key on
// restart, so the barrier refuses it.
func createTestBarrier(t *testing.T) *seal.Barrier {
	t.Helper()
	_ = seal.AllowWeakKDFParams()
	cfg := seal.DefaultBarrierConfig(1)
	cfg.StatePersister = func(*seal.SealedRootKey) error { return nil }
	b := seal.MustNewBarrier(cfg)
	s, err := newTestSealStrategy([]byte("test-snapshot-sender"))
	if err != nil {
		t.Fatalf("newTestSealStrategy: %v", err)
	}
	if err := b.Initialize(context.Background(), s, seal.Credentials{}); err != nil {
		t.Fatalf("barrier.Initialize: %v", err)
	}
	return b
}

// writeEncryptedSnapshotFile encrypts plaintext in the snapshot pool's frame
// format: [4-byte LE length][encrypted chunk]... and writes it to path, using
// the shared test artifact identity.
func writeEncryptedSnapshotFile(t *testing.T, b Barrier, path string, plaintext []byte) {
	t.Helper()
	writeEncryptedSnapshotFileFor(t, b, path, plaintext,
		testSnapShardID, testSnapReplicaID, testSnapIndex)
}

// writeEncryptedSnapshotFileFor is writeEncryptedSnapshotFile under an explicit
// artifact identity, for tests whose artifact is later read back through a
// production path that derives the identity from the shard/replica/index it
// actually holds. It seals through the production encoder, so the fixture is
// the format the decrypt side reads, terminal frame included, rather than a
// copy of it. The epochs the seal pinned are released once the file is
// written, as a caller does once its recorded epoch is durable.
func writeEncryptedSnapshotFileFor(t *testing.T, b Barrier, path string, plaintext []byte,
	shardID, replicaID, index uint64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create encrypted file: %v", err)
	}
	defer func() { _ = f.Close() }()
	_, pinned, err := encryptSnapshotFrames(b, shardID, replicaID, index, bytes.NewReader(plaintext), f)
	if err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}
	for _, e := range pinned {
		b.DeregisterInFlightEpoch(e)
	}
}

// TestSnapshotReceiveError_Format verifies the error message format.
func TestSnapshotReceiveError_Format(t *testing.T) {
	inner := errors.New("disk full")
	err := &SnapshotReceiveError{
		ShardID:   3,
		ReplicaID: 4,
		Op:        "write",
		Err:       inner,
	}

	expected := "quicraft: snapshot receive write failed for shard 3 replica 4: disk full"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	if err.Unwrap() != inner {
		t.Error("Unwrap() should return inner error")
	}
}

// TestForEachSnapshotChunk_CallbackErrorStopsIteration verifies that when the
// emit callback returns an error, forEachSnapshotChunk stops reading and
// returns that error immediately.
func TestForEachSnapshotChunk_CallbackErrorStopsIteration(t *testing.T) {
	tmpDir := t.TempDir()
	snapPath := filepath.Join(tmpDir, "snapshot.dat")

	// Create data spanning 3 chunks.
	totalSize := snapshotChunkSize*2 + 1024
	data := make([]byte, totalSize)
	if err := os.WriteFile(snapPath, data, 0o600); err != nil {
		t.Fatalf("failed to write snapshot file: %v", err)
	}

	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
		Snapshot: proto.Snapshot{
			Index:    100,
			Term:     5,
			Filepath: snapPath,
			FileSize: uint64(totalSize),
		},
	}

	callbackErr := errors.New("simulated send failure")
	var callCount int
	err := forEachSnapshotChunk(osSnapshotFS{}, msg, func(chunk proto.SnapshotChunk) error {
		callCount++
		if callCount == 2 {
			return callbackErr
		}
		return nil
	})

	if err != callbackErr {
		t.Fatalf("expected callback error, got: %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 callbacks before error, got %d", callCount)
	}
}
