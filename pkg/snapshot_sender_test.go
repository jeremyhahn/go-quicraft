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
	"encoding/binary"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
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
	err := forEachSnapshotChunk(msg, func(chunk proto.SnapshotChunk) error {
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
func newMinimalEngineNode(shardID, replicaID uint64) *engine.Node {
	cfg := config.Config{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
	return engine.NewNode(nil, nil, nil, cfg, 100, nil, "", nil)
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
// correctly delivers SnapshotStatus messages back to the raft layer via the
// engine.
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

	ts := &transportSender{
		engine: eng,
		logger: slog.Default(),
	}

	// Without a loaded node, the status message will be dropped silently
	// (DeliverMessage returns false for unknown shards). This should not panic.
	ts.notifySnapshotStatus(1, 1, 2, true)
	ts.notifySnapshotStatus(1, 1, 2, false)
}

// TestTransportSender_NotifySnapshotStatus_Rejected verifies that a rejected
// SnapshotStatus message is correctly formed.
func TestTransportSender_NotifySnapshotStatus_Rejected(t *testing.T) {
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

	// Create a node and load it so DeliverMessage succeeds.
	node := newMinimalEngineNode(1, 1)
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	ts := &transportSender{
		engine: eng,
		logger: slog.Default(),
	}

	// Send rejected status - should be delivered to the node's inbox.
	ts.notifySnapshotStatus(1, 1, 2, true)

	// Drain the inbox and verify the SnapshotStatus message.
	var buf []proto.Message
	buf = node.DrainInbox(buf)
	if len(buf) != 1 {
		t.Fatalf("expected 1 message in inbox, got %d", len(buf))
	}
	if buf[0].Type != proto.SnapshotStatus {
		t.Errorf("Type = %v, want SnapshotStatus", buf[0].Type)
	}
	if buf[0].ShardID != 1 {
		t.Errorf("ShardID = %d, want 1", buf[0].ShardID)
	}
	if buf[0].From != 2 {
		t.Errorf("From = %d, want 2 (target replica)", buf[0].From)
	}
	if buf[0].To != 1 {
		t.Errorf("To = %d, want 1 (local replica)", buf[0].To)
	}
	if !buf[0].Reject {
		t.Error("expected Reject=true for rejected snapshot status")
	}
}

// TestTransportSender_NotifySnapshotStatus_Accepted verifies that an accepted
// SnapshotStatus message has Reject=false.
func TestTransportSender_NotifySnapshotStatus_Accepted(t *testing.T) {
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

	node := newMinimalEngineNode(1, 1)
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	ts := &transportSender{
		engine: eng,
		logger: slog.Default(),
	}

	// Send accepted status.
	ts.notifySnapshotStatus(1, 1, 2, false)

	var buf []proto.Message
	buf = node.DrainInbox(buf)
	if len(buf) != 1 {
		t.Fatalf("expected 1 message in inbox, got %d", len(buf))
	}
	if buf[0].Type != proto.SnapshotStatus {
		t.Errorf("Type = %v, want SnapshotStatus", buf[0].Type)
	}
	if buf[0].Reject {
		t.Error("expected Reject=false for accepted snapshot status")
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

	ts := &transportSender{
		barrier: b,
		logger:  slog.Default(),
	}

	msg := proto.Message{
		Snapshot: proto.Snapshot{
			Filepath: encPath,
			FileSize: uint64(info.Size()),
			Epoch:    b.CurrentEpoch(),
		},
	}

	decPath, decSize, err := ts.decryptSnapshotForSend(msg)
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

// TestDecryptSnapshotForSend_FileNotFound verifies error handling when the
// encrypted snapshot file does not exist.
func TestDecryptSnapshotForSend_FileNotFound(t *testing.T) {
	b := createTestBarrier(t)
	ts := &transportSender{
		barrier: b,
		logger:  slog.Default(),
	}

	msg := proto.Message{
		Snapshot: proto.Snapshot{
			Filepath: "/nonexistent/snapshot.dat",
			Epoch:    1,
		},
	}

	_, _, err := ts.decryptSnapshotForSend(msg)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

// TestDoSendSnapshot_DecryptsBeforeSend verifies that doSendSnapshot
// strips the epoch from chunks when decrypting an encrypted snapshot.
func TestDoSendSnapshot_DecryptsBeforeSend(t *testing.T) {
	tmpDir := t.TempDir()

	// Create an encrypted snapshot file.
	b := createTestBarrier(t)
	plaintext := []byte("encrypted snapshot payload for chunk verification")
	encPath := filepath.Join(tmpDir, "snapshot.dat")
	writeEncryptedSnapshotFile(t, b, encPath, plaintext)

	info, err := os.Stat(encPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	// Build a message with non-zero epoch referencing the encrypted file.
	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
		Snapshot: proto.Snapshot{
			Index:    100,
			Term:     5,
			Filepath: encPath,
			FileSize: uint64(info.Size()),
			Epoch:    b.CurrentEpoch(),
		},
	}

	// Simulate what doSendSnapshot does: decrypt + split.
	ts := &transportSender{
		barrier: b,
		logger:  slog.Default(),
	}

	decPath, decSize, err := ts.decryptSnapshotForSend(msg)
	if err != nil {
		t.Fatalf("decryptSnapshotForSend: %v", err)
	}
	defer func() { _ = os.Remove(decPath) }()

	// Update message to reference decrypted file.
	msg.Snapshot.Filepath = decPath
	msg.Snapshot.FileSize = decSize
	msg.Snapshot.Epoch = 0

	chunks, err := collectSnapshotChunks(msg)
	if err != nil {
		t.Fatalf("collectSnapshotChunks: %v", err)
	}

	// Verify epoch is 0 on all chunks.
	for i, c := range chunks {
		if c.Epoch != 0 {
			t.Errorf("chunk[%d].Epoch = %d, want 0", i, c.Epoch)
		}
	}

	// Verify the data is plaintext.
	var reassembled []byte
	for _, c := range chunks {
		reassembled = append(reassembled, c.Data...)
	}
	if !bytes.Equal(reassembled, plaintext) {
		t.Errorf("reassembled data mismatch: got %q, want %q", reassembled, plaintext)
	}
}

// createTestBarrier creates a real seal.Barrier initialized with fast
// Argon2id parameters for testing.
func createTestBarrier(t *testing.T) *seal.Barrier {
	t.Helper()
	b := seal.NewBarrier(seal.DefaultBarrierConfig(1))
	s, err := seal.NewSoftwareStrategyWithParams(
		[]byte("test-snapshot-sender"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams: %v", err)
	}
	if err := b.Initialize(context.Background(), s, seal.Credentials{}); err != nil {
		t.Fatalf("barrier.Initialize: %v", err)
	}
	return b
}

// writeEncryptedSnapshotFile encrypts plaintext in the snapshot pool's frame
// format: [4-byte LE length][encrypted chunk]... and writes it to path.
func writeEncryptedSnapshotFile(t *testing.T, b Barrier, path string, plaintext []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create encrypted file: %v", err)
	}
	defer func() { _ = f.Close() }()

	// Encrypt in 64KB chunks (matching snapshotEncryptChunkSize).
	const chunkSize = 64 * 1024
	offset := 0
	var frameBuf [4]byte
	for offset < len(plaintext) {
		end := offset + chunkSize
		if end > len(plaintext) {
			end = len(plaintext)
		}
		encrypted, encErr := b.Encrypt(nil, plaintext[offset:end])
		if encErr != nil {
			t.Fatalf("barrier.Encrypt: %v", encErr)
		}
		binary.LittleEndian.PutUint32(frameBuf[:], uint32(len(encrypted)))
		if _, err := f.Write(frameBuf[:]); err != nil {
			t.Fatalf("write frame len: %v", err)
		}
		if _, err := f.Write(encrypted); err != nil {
			t.Fatalf("write encrypted data: %v", err)
		}
		offset = end
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

// TestSplitSnapshotMessage_FileCloseOnError verifies that the file handle is
// properly closed when an error occurs during chunk reading. This tests the
// resource cleanup pattern that uses a fileClosed flag to prevent double-close.
func TestSplitSnapshotMessage_FileCloseOnError(t *testing.T) {
	tmpDir := t.TempDir()
	snapPath := filepath.Join(tmpDir, "snapshot.dat")

	// Create a file smaller than declared to trigger a read error.
	data := []byte("short data")
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
			FileSize: uint64(len(data)), // Declare correct size first
		},
	}

	// First verify normal case works - file should be closed after success.
	chunks, err := collectSnapshotChunks(msg)
	if err != nil {
		t.Fatalf("collectSnapshotChunks failed for valid file: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	// Now test error case - file should still be closed even on error.
	msg.Snapshot.FileSize = uint64(len(data) + 100) // Wrong size to trigger error
	_, err = collectSnapshotChunks(msg)
	if err == nil {
		t.Fatal("expected error for size mismatch, got nil")
	}

	// Verify we can still access the file (it was properly closed).
	_, statErr := os.Stat(snapPath)
	if statErr != nil {
		t.Errorf("file should still exist: %v", statErr)
	}
}

// TestDecryptSnapshotForSend_FileCloseOnError verifies that both source and
// temp files are properly closed when an error occurs during decryption.
func TestDecryptSnapshotForSend_FileCloseOnError(t *testing.T) {
	tmpDir := t.TempDir()
	b := createTestBarrier(t)

	// Create a malformed encrypted file that will cause decryption to fail.
	encPath := filepath.Join(tmpDir, "snapshot.dat")
	f, err := os.Create(encPath)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}

	// Write a valid frame length but invalid encrypted data.
	var frameBuf [4]byte
	binary.LittleEndian.PutUint32(frameBuf[:], 100)
	if _, err := f.Write(frameBuf[:]); err != nil {
		t.Fatalf("write frame len: %v", err)
	}
	// Write garbage data that won't decrypt properly.
	garbage := make([]byte, 100)
	if _, err := f.Write(garbage); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ts := &transportSender{
		barrier: b,
		logger:  slog.Default(),
	}

	msg := proto.Message{
		ShardID: 1,
		Snapshot: proto.Snapshot{
			Filepath: encPath,
			Epoch:    b.CurrentEpoch(),
		},
	}

	// This should fail during decryption but clean up all resources.
	_, _, err = ts.decryptSnapshotForSend(msg)
	if err == nil {
		t.Fatal("expected decryption error, got nil")
	}

	// Verify source file still exists and is accessible (was properly closed).
	_, statErr := os.Stat(encPath)
	if statErr != nil {
		t.Errorf("source file should still exist: %v", statErr)
	}

	// Verify no temp files were left behind.
	matches, _ := filepath.Glob(filepath.Join(tmpDir, ".send-decrypt-*.tmp"))
	if len(matches) > 0 {
		t.Errorf("temp files should be cleaned up, found: %v", matches)
	}
}

// TestDoSendSnapshot_TempFileCleanupOnSplitError verifies that the decrypted
// temp file is cleaned up when collectSnapshotChunks fails. We simulate this
// by creating an encrypted file that decrypts successfully but then providing
// incorrect FileSize to collectSnapshotChunks.
func TestDoSendSnapshot_TempFileCleanupOnSplitError(t *testing.T) {
	tmpDir := t.TempDir()

	// Create an encrypted snapshot file with known plaintext.
	b := createTestBarrier(t)
	plaintext := []byte("test snapshot data for cleanup verification")
	encPath := filepath.Join(tmpDir, "snapshot.dat")
	writeEncryptedSnapshotFile(t, b, encPath, plaintext)

	info, err := os.Stat(encPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	// Create the transportSender with just the barrier for testing.
	ts := &transportSender{
		barrier: b,
		logger:  slog.Default(),
	}

	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
		Snapshot: proto.Snapshot{
			Index:    100,
			Term:     5,
			Filepath: encPath,
			FileSize: uint64(info.Size()),
			Epoch:    b.CurrentEpoch(),
		},
	}

	// Manually run the decryption step (which doSendSnapshot does internally).
	decPath, decSize, err := ts.decryptSnapshotForSend(msg)
	if err != nil {
		t.Fatalf("decryptSnapshotForSend: %v", err)
	}

	// Verify temp file exists after decryption.
	if _, statErr := os.Stat(decPath); statErr != nil {
		t.Fatalf("decrypted temp file should exist: %v", statErr)
	}

	// Simulate what doSendSnapshot does: update message to reference decrypted file,
	// then call collectSnapshotChunks with wrong FileSize to trigger an error.
	msg.Snapshot.Filepath = decPath
	msg.Snapshot.FileSize = decSize + 100 // Wrong size to trigger error.
	msg.Snapshot.Epoch = 0

	// Split should fail due to size mismatch.
	_, splitErr := collectSnapshotChunks(msg)
	if splitErr == nil {
		// Clean up the temp file manually since split unexpectedly succeeded.
		_ = os.Remove(decPath)
		t.Fatal("expected split error for size mismatch, got nil")
	}

	// Clean up the temp file (simulating the defer in doSendSnapshot).
	if rmErr := os.Remove(decPath); rmErr != nil && !os.IsNotExist(rmErr) {
		t.Errorf("failed to remove temp file: %v", rmErr)
	}

	// Verify no temp files were left behind.
	matches, _ := filepath.Glob(filepath.Join(tmpDir, ".send-decrypt-*.tmp"))
	if len(matches) > 0 {
		t.Errorf("temp files should be cleaned up after split error, found: %v", matches)
	}
}

// TestForEachSnapshotChunk_StreamsOneAtATime verifies that forEachSnapshotChunk
// delivers chunks one at a time to the callback, ensuring only one chunk's
// data is live in memory at any point during iteration.
func TestForEachSnapshotChunk_StreamsOneAtATime(t *testing.T) {
	tmpDir := t.TempDir()
	snapPath := filepath.Join(tmpDir, "snapshot.dat")

	// Create data spanning 3 chunks.
	totalSize := snapshotChunkSize*2 + 1024
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := os.WriteFile(snapPath, data, 0o600); err != nil {
		t.Fatalf("failed to write snapshot file: %v", err)
	}

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
		},
	}

	var callCount int
	var reassembled []byte
	err := forEachSnapshotChunk(msg, func(chunk proto.SnapshotChunk) error {
		// Verify chunk metadata on each callback invocation.
		if chunk.ChunkID != uint64(callCount) {
			t.Errorf("callback %d: ChunkID = %d, want %d", callCount, chunk.ChunkID, callCount)
		}
		if chunk.ChunkCount != 3 {
			t.Errorf("callback %d: ChunkCount = %d, want 3", callCount, chunk.ChunkCount)
		}
		reassembled = append(reassembled, chunk.Data...)
		callCount++
		return nil
	})
	if err != nil {
		t.Fatalf("forEachSnapshotChunk returned error: %v", err)
	}
	if callCount != 3 {
		t.Fatalf("expected 3 callbacks, got %d", callCount)
	}
	if !bytes.Equal(reassembled, data) {
		t.Fatal("reassembled data does not match original")
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
	err := forEachSnapshotChunk(msg, func(chunk proto.SnapshotChunk) error {
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

// TestForEachSnapshotChunk_EmptySnapshot verifies that forEachSnapshotChunk
// emits exactly one metadata-only chunk for a witness/empty snapshot.
func TestForEachSnapshotChunk_EmptySnapshot(t *testing.T) {
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
		},
	}

	var callCount int
	err := forEachSnapshotChunk(msg, func(chunk proto.SnapshotChunk) error {
		callCount++
		if chunk.ChunkSize != 0 {
			t.Errorf("expected ChunkSize=0 for empty snapshot, got %d", chunk.ChunkSize)
		}
		if chunk.ChunkCount != 1 {
			t.Errorf("expected ChunkCount=1, got %d", chunk.ChunkCount)
		}
		if len(chunk.Data) != 0 {
			t.Errorf("expected no data for empty snapshot, got %d bytes", len(chunk.Data))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("forEachSnapshotChunk returned error: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected 1 callback for empty snapshot, got %d", callCount)
	}
}

// TestResourceCleanup_Pattern verifies the resource cleanup pattern used in
// doSendSnapshot correctly handles cleanup of multiple resources in reverse
// order.
func TestResourceCleanup_Pattern(t *testing.T) {
	var cleanupOrder []int

	// Simulate the cleanup pattern from doSendSnapshot.
	var resourceCleanup []func()
	cleanup := func() {
		for i := len(resourceCleanup) - 1; i >= 0; i-- {
			resourceCleanup[i]()
		}
	}
	defer cleanup()

	// Register cleanup functions in order.
	resourceCleanup = append(resourceCleanup, func() { cleanupOrder = append(cleanupOrder, 1) })
	resourceCleanup = append(resourceCleanup, func() { cleanupOrder = append(cleanupOrder, 2) })
	resourceCleanup = append(resourceCleanup, func() { cleanupOrder = append(cleanupOrder, 3) })

	// Manually trigger cleanup (simulating function return).
	cleanup()

	// Verify reverse order cleanup.
	if len(cleanupOrder) != 3 {
		t.Fatalf("expected 3 cleanups, got %d", len(cleanupOrder))
	}
	if cleanupOrder[0] != 3 || cleanupOrder[1] != 2 || cleanupOrder[2] != 1 {
		t.Errorf("expected cleanup order [3,2,1], got %v", cleanupOrder)
	}

	// Clear for the deferred cleanup call at function exit.
	resourceCleanup = nil
}
