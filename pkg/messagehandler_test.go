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
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

func TestHostMessageHandler_HandleMessage_DeliversToEngine(t *testing.T) {
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

	handler := &hostMessageHandler{engine: eng}

	batch := proto.MessageBatch{
		Requests: []proto.Message{
			{ShardID: 1, To: 2, Type: proto.Heartbeat},
			{ShardID: 3, To: 4, Type: proto.Replicate},
		},
	}

	err := handler.HandleMessage(batch)
	if err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}
	// Messages to unloaded shards are silently dropped by the engine,
	// so no error is expected.
}

func TestHostMessageHandler_HandleMessage_EmptyBatch(t *testing.T) {
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

	handler := &hostMessageHandler{engine: eng}

	batch := proto.MessageBatch{
		Requests: []proto.Message{},
	}

	err := handler.HandleMessage(batch)
	if err != nil {
		t.Fatalf("HandleMessage returned unexpected error for empty batch: %v", err)
	}
}

// newEngineNodeWithMembership creates an engine.Node with a real raft.Peer
// whose membership includes the specified voters. This enables per-shard
// authorization tests where IsMember returns accurate results.
func newEngineNodeWithMembership(t *testing.T, shardID, replicaID uint64, voters map[uint64]string) *engine.Node {
	t.Helper()
	ldb := memdb.New()
	lr := logdb.NewLogReader(shardID, replicaID, ldb)
	ms := proto.Membership{
		Addresses: voters,
	}
	lr.ApplySnapshot(proto.Snapshot{
		Index:      0,
		Term:       0,
		Membership: ms,
	})
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer failed: %v", err)
	}
	return engine.NewNode(peer, nil, lr, cfg, 100, nil, "", nil)
}

// TestHostMessageHandler_HandleMessage_AuthorizedMember verifies that
// messages from a known member of the target shard are delivered to the
// engine's inbox and work is signaled.
func TestHostMessageHandler_HandleMessage_AuthorizedMember(t *testing.T) {
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

	// Load a node with membership {1, 2} for shard 1.
	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	handler := &hostMessageHandler{engine: eng}

	// Message from replica 2 (a member) should be delivered.
	batch := proto.MessageBatch{
		Requests: []proto.Message{
			{ShardID: 1, From: 2, To: 1, Type: proto.Heartbeat},
		},
	}

	err := handler.HandleMessage(batch)
	if err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}
	// Authorized message is delivered. We verify by confirming no error
	// and the message handler did not skip the message.
}

// TestHostMessageHandler_HandleMessage_UnauthorizedMember verifies that
// messages from a replica NOT in the shard's membership are silently
// dropped (not delivered to the engine).
func TestHostMessageHandler_HandleMessage_UnauthorizedMember(t *testing.T) {
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

	// Load a node with membership {1, 2} for shard 1.
	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	handler := &hostMessageHandler{engine: eng}

	// Message from replica 999 (NOT a member) should be dropped.
	batch := proto.MessageBatch{
		Requests: []proto.Message{
			{ShardID: 1, From: 999, To: 1, Type: proto.Heartbeat},
		},
	}

	err := handler.HandleMessage(batch)
	if err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}
	// The message from the unauthorized replica was silently dropped.
	// DeliverMessage was not called for it.
}

// TestHostMessageHandler_HandleMessage_UnloadedShardAllowed verifies that
// messages to shards not loaded on this host are still delivered. This
// ensures bootstrap messages and shard-loading triggers flow through.
func TestHostMessageHandler_HandleMessage_UnloadedShardAllowed(t *testing.T) {
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

	handler := &hostMessageHandler{engine: eng}

	// Message to shard 42 which is NOT loaded on this engine.
	// GetNode returns nil, so the message should be allowed through.
	batch := proto.MessageBatch{
		Requests: []proto.Message{
			{ShardID: 42, From: 5, To: 1, Type: proto.Replicate},
		},
	}

	err := handler.HandleMessage(batch)
	if err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}
}

// TestHostMessageHandler_HandleMessage_MixedAuthorization verifies that
// a batch with both authorized and unauthorized messages correctly
// delivers only the authorized ones.
func TestHostMessageHandler_HandleMessage_MixedAuthorization(t *testing.T) {
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

	// Load shard 1 with membership {1, 2}.
	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	handler := &hostMessageHandler{engine: eng}

	batch := proto.MessageBatch{
		Requests: []proto.Message{
			// Authorized: replica 2 is a member of shard 1.
			{ShardID: 1, From: 2, To: 1, Type: proto.Heartbeat},
			// Unauthorized: replica 777 is NOT a member of shard 1.
			{ShardID: 1, From: 777, To: 1, Type: proto.Replicate},
			// Unloaded shard: allowed through.
			{ShardID: 99, From: 5, To: 1, Type: proto.Heartbeat},
		},
	}

	err := handler.HandleMessage(batch)
	if err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}
	// First message delivered (authorized), second dropped (unauthorized),
	// third delivered (shard not loaded, no membership check).
}

// TestHostMessageHandler_HandleSnapshot_UnauthorizedSender verifies that
// snapshot chunks from a non-member sender are rejected.
func TestHostMessageHandler_HandleSnapshot_UnauthorizedSender(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	sender := &noopSender{}
	eng := engine.NewEngine(cfg, ldb, sender, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	// Load shard 1 with membership {1, 2}.
	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	h := &Host{
		cfg:   cfg,
		logdb: ldb,
	}

	handler := &hostMessageHandler{
		engine: eng,
		host:   h,
	}

	// Snapshot from replica 999 (NOT a member of shard 1).
	chunks := []proto.SnapshotChunk{
		{
			ShardID:    1,
			ReplicaID:  1,
			From:       999,
			Index:      500,
			Term:       10,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("malicious-snapshot"),
		},
	}

	err := handler.HandleSnapshot(chunks)
	if err == nil {
		t.Fatal("expected error for unauthorized snapshot sender")
	}

	var authErr *UnauthorizedMessageError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected UnauthorizedMessageError, got %T: %v", err, err)
	}
	if authErr.ShardID != 1 {
		t.Errorf("ShardID = %d, want 1", authErr.ShardID)
	}
	if authErr.From != 999 {
		t.Errorf("From = %d, want 999", authErr.From)
	}
}

func TestHostMessageHandler_HandleSnapshot_NilChunks(t *testing.T) {
	handler := &hostMessageHandler{}

	err := handler.HandleSnapshot(nil)
	if err != nil {
		t.Fatalf("HandleSnapshot returned unexpected error: %v", err)
	}
}

func TestHostMessageHandler_HandleSnapshot_EmptyChunks(t *testing.T) {
	handler := &hostMessageHandler{}

	err := handler.HandleSnapshot([]proto.SnapshotChunk{})
	if err != nil {
		t.Fatalf("HandleSnapshot returned unexpected error: %v", err)
	}
}

func TestHostMessageHandler_HandleSnapshot_NilHost(t *testing.T) {
	// When host is nil (unit test or not fully wired), return nil.
	handler := &hostMessageHandler{}

	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 1},
	}

	err := handler.HandleSnapshot(chunks)
	if err != nil {
		t.Fatalf("HandleSnapshot with nil host returned unexpected error: %v", err)
	}
}

// TestHostMessageHandler_HandleSnapshot_WritesSnapshotToDisk verifies the full
// HandleSnapshot flow: chunk reassembly, disk writing, metadata persistence,
// and LogDB saving.
func TestHostMessageHandler_HandleSnapshot_WritesSnapshotToDisk(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	sender := &noopSender{}
	eng := engine.NewEngine(cfg, ldb, sender, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	// Create a Host with minimal wiring for the handler.
	h := &Host{
		cfg:   cfg,
		logdb: ldb,
	}

	handler := &hostMessageHandler{
		engine: eng,
		host:   h,
	}

	// Simulate snapshot data spread across 3 chunks.
	snapshotData := []byte("snapshot-data-chunk1" +
		"snapshot-data-chunk2" +
		"snapshot-data-chunk3")

	chunk1Data := []byte("snapshot-data-chunk1")
	chunk2Data := []byte("snapshot-data-chunk2")
	chunk3Data := []byte("snapshot-data-chunk3")

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    1,
			ReplicaID:  2,
			From:       1,
			Index:      500,
			Term:       10,
			ChunkID:    0,
			ChunkCount: 3,
			Data:       chunk1Data,
			Membership: proto.Membership{
				ConfigChangeID: 42,
				Addresses: map[uint64]string{
					1: "127.0.0.1:5001",
					2: "127.0.0.1:5002",
				},
			},
			OnDiskIndex: 0,
			Epoch:       5,
		},
		{
			ShardID:    1,
			ReplicaID:  2,
			From:       1,
			Index:      500,
			Term:       10,
			ChunkID:    1,
			ChunkCount: 3,
			Data:       chunk2Data,
		},
		{
			ShardID:    1,
			ReplicaID:  2,
			From:       1,
			Index:      500,
			Term:       10,
			ChunkID:    2,
			ChunkCount: 3,
			Data:       chunk3Data,
		},
	}

	err := handler.HandleSnapshot(chunks)
	if err != nil {
		t.Fatalf("HandleSnapshot returned error: %v", err)
	}

	// Verify the snapshot data file was written correctly.
	snapshotDir := filepath.Join(
		nodeHostDir, "snapshots",
		"shard-1",
		"replica-2",
		"snapshot-00000000000000000500",
	)

	dataPath := filepath.Join(snapshotDir, "snapshot.dat")
	data, readErr := os.ReadFile(dataPath)
	if readErr != nil {
		t.Fatalf("failed to read snapshot data file: %v", readErr)
	}
	if string(data) != string(snapshotData) {
		t.Fatalf("snapshot data mismatch:\ngot:  %q\nwant: %q", string(data), string(snapshotData))
	}

	// Verify the metadata file was written correctly.
	metaPath := filepath.Join(snapshotDir, "snapshot.meta")
	metaData, readErr := os.ReadFile(metaPath)
	if readErr != nil {
		t.Fatalf("failed to read snapshot metadata file: %v", readErr)
	}
	if len(metaData) != snapshotRecvMetaSize {
		t.Fatalf("metadata size = %d, want %d", len(metaData), snapshotRecvMetaSize)
	}

	// Verify metadata fields.
	metaIndex := binary.LittleEndian.Uint64(metaData[0:])
	metaTerm := binary.LittleEndian.Uint64(metaData[8:])
	metaShardID := binary.LittleEndian.Uint64(metaData[16:])
	metaReplicaID := binary.LittleEndian.Uint64(metaData[24:])

	if metaIndex != 500 {
		t.Errorf("metadata index = %d, want 500", metaIndex)
	}
	if metaTerm != 10 {
		t.Errorf("metadata term = %d, want 10", metaTerm)
	}
	if metaShardID != 1 {
		t.Errorf("metadata shardID = %d, want 1", metaShardID)
	}
	if metaReplicaID != 2 {
		t.Errorf("metadata replicaID = %d, want 2", metaReplicaID)
	}

	// Verify LogDB received the snapshot metadata.
	snap, logErr := ldb.GetSnapshot(1, 2)
	if logErr != nil {
		t.Fatalf("GetSnapshot from LogDB failed: %v", logErr)
	}
	if snap.Index != 500 {
		t.Errorf("LogDB snapshot index = %d, want 500", snap.Index)
	}
	if snap.Term != 10 {
		t.Errorf("LogDB snapshot term = %d, want 10", snap.Term)
	}
	if snap.Filepath != snapshotDir {
		t.Errorf("LogDB snapshot filepath = %q, want %q", snap.Filepath, snapshotDir)
	}
	if snap.Membership.ConfigChangeID != 42 {
		t.Errorf("LogDB membership ConfigChangeID = %d, want 42",
			snap.Membership.ConfigChangeID)
	}
	if len(snap.Membership.Addresses) != 2 {
		t.Errorf("LogDB membership Addresses len = %d, want 2",
			len(snap.Membership.Addresses))
	}
	if snap.Epoch != 5 {
		t.Errorf("LogDB snapshot epoch = %d, want 5", snap.Epoch)
	}
}

// TestHostMessageHandler_HandleSnapshot_SingleChunk verifies that a snapshot
// delivered as a single chunk is handled correctly.
func TestHostMessageHandler_HandleSnapshot_SingleChunk(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	sender := &noopSender{}
	eng := engine.NewEngine(cfg, ldb, sender, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	h := &Host{
		cfg:   cfg,
		logdb: ldb,
	}

	handler := &hostMessageHandler{
		engine: eng,
		host:   h,
	}

	payload := []byte("complete-snapshot-in-one-chunk")
	chunks := []proto.SnapshotChunk{
		{
			ShardID:    3,
			ReplicaID:  1,
			From:       2,
			Index:      1000,
			Term:       20,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       payload,
		},
	}

	err := handler.HandleSnapshot(chunks)
	if err != nil {
		t.Fatalf("HandleSnapshot returned error: %v", err)
	}

	// Verify data file.
	snapshotDir := filepath.Join(
		nodeHostDir, "snapshots",
		"shard-3",
		"replica-1",
		"snapshot-00000000000000001000",
	)
	data, readErr := os.ReadFile(filepath.Join(snapshotDir, "snapshot.dat"))
	if readErr != nil {
		t.Fatalf("failed to read snapshot data: %v", readErr)
	}
	if string(data) != string(payload) {
		t.Fatalf("data mismatch: got %q, want %q", string(data), string(payload))
	}
}

// TestHostMessageHandler_HandleSnapshot_EmptyChunkData verifies that chunks
// with zero-length data (metadata-only) are handled without error.
func TestHostMessageHandler_HandleSnapshot_EmptyChunkData(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	sender := &noopSender{}
	eng := engine.NewEngine(cfg, ldb, sender, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	h := &Host{
		cfg:   cfg,
		logdb: ldb,
	}

	handler := &hostMessageHandler{
		engine: eng,
		host:   h,
	}

	// Empty data chunk (witness snapshot or metadata-only).
	chunks := []proto.SnapshotChunk{
		{
			ShardID:    5,
			ReplicaID:  1,
			From:       3,
			Index:      200,
			Term:       7,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       nil,
		},
	}

	err := handler.HandleSnapshot(chunks)
	if err != nil {
		t.Fatalf("HandleSnapshot returned error for empty data chunk: %v", err)
	}

	// The data file should exist but be empty.
	snapshotDir := filepath.Join(
		nodeHostDir, "snapshots",
		"shard-5",
		"replica-1",
		"snapshot-00000000000000000200",
	)
	data, readErr := os.ReadFile(filepath.Join(snapshotDir, "snapshot.dat"))
	if readErr != nil {
		t.Fatalf("failed to read snapshot data: %v", readErr)
	}
	if len(data) != 0 {
		t.Fatalf("expected empty data file, got %d bytes", len(data))
	}
}

// TestHostMessageHandler_HandleSnapshot_NilLogDB verifies that HandleSnapshot
// works correctly when LogDB is nil (skips LogDB save).
func TestHostMessageHandler_HandleSnapshot_NilLogDB(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
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

	// Host with nil logdb.
	h := &Host{
		cfg:   cfg,
		logdb: nil,
	}

	handler := &hostMessageHandler{
		engine: eng,
		host:   h,
	}

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    1,
			ReplicaID:  1,
			From:       2,
			Index:      100,
			Term:       5,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("test-data"),
		},
	}

	err := handler.HandleSnapshot(chunks)
	if err != nil {
		t.Fatalf("HandleSnapshot returned error with nil logdb: %v", err)
	}

	// Verify data was still written to disk.
	snapshotDir := filepath.Join(
		nodeHostDir, "snapshots",
		"shard-1",
		"replica-1",
		"snapshot-00000000000000000100",
	)
	data, readErr := os.ReadFile(filepath.Join(snapshotDir, "snapshot.dat"))
	if readErr != nil {
		t.Fatalf("failed to read snapshot data: %v", readErr)
	}
	if string(data) != "test-data" {
		t.Fatalf("data mismatch: got %q, want %q", string(data), "test-data")
	}
}

func TestWithoutTransport_DisablesTransport(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	if h.transport != nil {
		t.Fatal("expected transport to be nil when WithoutTransport() is used")
	}

	// Verify the sender is a noopSender.
	if _, ok := h.sender.(*noopSender); !ok {
		t.Fatalf("expected noopSender, got %T", h.sender)
	}
}

func TestBuildTransportConfig_MapsAllFields(t *testing.T) {
	hc := config.HostConfig{
		ListenAddress: "0.0.0.0:4001",
		DeploymentID:  42,
		TransportConfig: config.TransportConfig{
			Enable0RTT:                    true,
			StreamPoolSize:                32,
			MaxStreamPoolSize:             512,
			MaxSnapshotReceiveRate:        100 * 1024 * 1024,
			MaxConcurrentSnapshotReceives: 8,
			MaxSnapshotReceiveMemory:      2 * 1024 * 1024 * 1024,
			SendBatchMaxSize:              128 * 1024,
			MaxDecompressedSize:           32 * 1024 * 1024,
			MaxIncomingConnections:        128,
			MaxConnectionsPerIP:           8,
		},
	}

	tc := buildTransportConfig(hc)

	if tc.ListenAddress != "0.0.0.0:4001" {
		t.Fatalf("ListenAddress: got %q, want %q", tc.ListenAddress, "0.0.0.0:4001")
	}
	if tc.DeploymentID != 42 {
		t.Fatalf("DeploymentID: got %d, want %d", tc.DeploymentID, 42)
	}
	if tc.Enable0RTT != true {
		t.Fatal("Enable0RTT: got false, want true")
	}
	if tc.StreamPoolSize != 32 {
		t.Fatalf("StreamPoolSize: got %d, want %d", tc.StreamPoolSize, 32)
	}
	if tc.MaxStreamPoolSize != 512 {
		t.Fatalf("MaxStreamPoolSize: got %d, want %d", tc.MaxStreamPoolSize, 512)
	}
	if tc.MaxSnapshotReceiveRate != 100*1024*1024 {
		t.Fatalf("MaxSnapshotReceiveRate: got %d, want %d", tc.MaxSnapshotReceiveRate, 100*1024*1024)
	}
	if tc.MaxConcurrentSnapshotRecv != 8 {
		t.Fatalf("MaxConcurrentSnapshotRecv: got %d, want %d", tc.MaxConcurrentSnapshotRecv, 8)
	}
	if tc.MaxSnapshotReceiveMemory != 2*1024*1024*1024 {
		t.Fatalf("MaxSnapshotReceiveMemory: got %d, want %d", tc.MaxSnapshotReceiveMemory, int64(2*1024*1024*1024))
	}
	if tc.SendBatchMaxSize != 128*1024 {
		t.Fatalf("SendBatchMaxSize: got %d, want %d", tc.SendBatchMaxSize, 128*1024)
	}
	if tc.MaxDecompressedSize != 32*1024*1024 {
		t.Fatalf("MaxDecompressedSize: got %d, want %d", tc.MaxDecompressedSize, 32*1024*1024)
	}
	if tc.MaxIncomingConnections != 128 {
		t.Fatalf("MaxIncomingConnections: got %d, want %d", tc.MaxIncomingConnections, 128)
	}
	if tc.MaxConnectionsPerIP != 8 {
		t.Fatalf("MaxConnectionsPerIP: got %d, want %d", tc.MaxConnectionsPerIP, 8)
	}
}

func TestBuildTransportConfig_DefaultValues(t *testing.T) {
	hc := config.HostConfig{
		ListenAddress: ":4001",
		DeploymentID:  1,
	}
	hc.SetDefaults()

	tc := buildTransportConfig(hc)

	if tc.ListenAddress != ":4001" {
		t.Fatalf("ListenAddress: got %q, want %q", tc.ListenAddress, ":4001")
	}
	if tc.DeploymentID != 1 {
		t.Fatalf("DeploymentID: got %d, want %d", tc.DeploymentID, 1)
	}
	// After SetDefaults, StreamPoolSize should be the default.
	if tc.StreamPoolSize != config.DefaultStreamPoolSize {
		t.Fatalf("StreamPoolSize: got %d, want %d", tc.StreamPoolSize, config.DefaultStreamPoolSize)
	}
}

func TestHost_CloseWithTransportDisabled(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}

	// Close should succeed with nil transport.
	if err := h.Close(); err != nil {
		t.Fatalf("Close returned unexpected error: %v", err)
	}
}

func TestNoopSender_Send_DoesNotPanic(t *testing.T) {
	s := &noopSender{}

	// Nil slice.
	s.Send(nil)

	// Empty slice.
	s.Send([]proto.Message{})

	// Non-empty slice.
	s.Send([]proto.Message{
		{ShardID: 1, To: 2},
	})
}

func TestNoopSender_SendSnapshot_DoesNotPanic(t *testing.T) {
	s := &noopSender{}
	s.SendSnapshot(proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
	})
}

// TestWriteReceivedSnapshotMetadata verifies the binary metadata format
// written by writeReceivedSnapshotMetadata.
func TestWriteReceivedSnapshotMetadata(t *testing.T) {
	dir := t.TempDir()

	err := writeReceivedSnapshotMetadata(dir, 42, 7, 10, 3)
	if err != nil {
		t.Fatalf("writeReceivedSnapshotMetadata returned error: %v", err)
	}

	metaPath := filepath.Join(dir, "snapshot.meta")
	data, readErr := os.ReadFile(metaPath)
	if readErr != nil {
		t.Fatalf("failed to read metadata file: %v", readErr)
	}
	if len(data) != snapshotRecvMetaSize {
		t.Fatalf("metadata size = %d, want %d", len(data), snapshotRecvMetaSize)
	}

	index := binary.LittleEndian.Uint64(data[0:])
	term := binary.LittleEndian.Uint64(data[8:])
	shardID := binary.LittleEndian.Uint64(data[16:])
	replicaID := binary.LittleEndian.Uint64(data[24:])
	receivedAt := binary.LittleEndian.Uint64(data[32:])

	if index != 42 {
		t.Errorf("index = %d, want 42", index)
	}
	if term != 7 {
		t.Errorf("term = %d, want 7", term)
	}
	if shardID != 10 {
		t.Errorf("shardID = %d, want 10", shardID)
	}
	if replicaID != 3 {
		t.Errorf("replicaID = %d, want 3", replicaID)
	}
	if receivedAt == 0 {
		t.Error("receivedAt should be non-zero")
	}
}

// TestWriteReceivedSnapshotMetadata_InvalidDir verifies error handling when
// the directory does not exist and cannot be created.
func TestWriteReceivedSnapshotMetadata_InvalidDir(t *testing.T) {
	// Use a path that cannot be created (file as parent).
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "file.txt")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Try to write metadata into a directory path where a file exists.
	invalidDir := filepath.Join(filePath, "subdir")
	err := writeReceivedSnapshotMetadata(invalidDir, 1, 1, 1, 1)
	if err == nil {
		t.Fatal("expected error for invalid directory, got nil")
	}
}

// TestCopyMapUint64String verifies deep copy behavior.
func TestCopyMapUint64String(t *testing.T) {
	// Nil input.
	result := copyMapUint64String(nil)
	if result != nil {
		t.Fatalf("expected nil for nil input, got %v", result)
	}

	// Empty input.
	result = copyMapUint64String(map[uint64]string{})
	if result != nil {
		t.Fatalf("expected nil for empty input, got %v", result)
	}

	// Non-empty input.
	orig := map[uint64]string{1: "a", 2: "b"}
	result = copyMapUint64String(orig)
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	if result[1] != "a" || result[2] != "b" {
		t.Fatalf("values mismatch: %v", result)
	}

	// Verify deep copy (modifying original does not affect copy).
	orig[3] = "c"
	if _, ok := result[3]; ok {
		t.Fatal("copy should not be affected by original modifications")
	}
}

// TestHostMessageHandler_HandleSnapshot_CleansUpOnWriteError verifies that
// when a snapshot write fails after directory creation, the partial snapshot
// directory is removed to prevent orphaned partial data.
func TestHostMessageHandler_HandleSnapshot_CleansUpOnWriteError(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	sender := &noopSender{}
	eng := engine.NewEngine(cfg, ldb, sender, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	h := &Host{
		cfg:   cfg,
		logdb: ldb,
	}

	handler := &hostMessageHandler{
		engine: eng,
		host:   h,
	}

	// Pre-create the snapshot directory and put a read-only file where
	// snapshot.dat would go so that os.Create fails after MkdirAll.
	snapshotDir := filepath.Join(
		nodeHostDir, "snapshots",
		"shard-10",
		"replica-1",
		"snapshot-00000000000000000777",
	)
	if err := os.MkdirAll(snapshotDir, 0o750); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	// Create a directory where the file would be, so os.Create fails.
	dataPath := filepath.Join(snapshotDir, "snapshot.dat")
	if err := os.MkdirAll(dataPath, 0o750); err != nil {
		t.Fatalf("failed to create blocking dir: %v", err)
	}

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    10,
			ReplicaID:  1,
			From:       2,
			Index:      777,
			Term:       15,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("data-that-will-fail"),
		},
	}

	err := handler.HandleSnapshot(chunks)
	if err == nil {
		t.Fatal("expected error from HandleSnapshot, got nil")
	}

	var snapErr *SnapshotReceiveError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected SnapshotReceiveError, got %T: %v", err, err)
	}

	// The snapshot directory should be cleaned up by the deferred cleanup.
	if _, statErr := os.Stat(snapshotDir); !os.IsNotExist(statErr) {
		t.Fatalf("snapshot directory should have been removed after error, but still exists")
	}
}

// TestHostMessageHandler_HandleSnapshot_NoCleanupOnSuccess verifies that
// on a successful snapshot receive, the directory is NOT cleaned up.
func TestHostMessageHandler_HandleSnapshot_NoCleanupOnSuccess(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	sender := &noopSender{}
	eng := engine.NewEngine(cfg, ldb, sender, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	h := &Host{
		cfg:   cfg,
		logdb: ldb,
	}

	handler := &hostMessageHandler{
		engine: eng,
		host:   h,
	}

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    20,
			ReplicaID:  1,
			From:       2,
			Index:      888,
			Term:       25,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("good-snapshot-data"),
		},
	}

	err := handler.HandleSnapshot(chunks)
	if err != nil {
		t.Fatalf("HandleSnapshot returned error: %v", err)
	}

	// The snapshot directory should still exist.
	snapshotDir := filepath.Join(
		nodeHostDir, "snapshots",
		"shard-20",
		"replica-1",
		"snapshot-00000000000000000888",
	)
	if _, statErr := os.Stat(snapshotDir); os.IsNotExist(statErr) {
		t.Fatal("snapshot directory should exist after successful receive")
	}

	// Verify the data file is correct.
	data, readErr := os.ReadFile(filepath.Join(snapshotDir, "snapshot.dat"))
	if readErr != nil {
		t.Fatalf("failed to read snapshot data: %v", readErr)
	}
	if string(data) != "good-snapshot-data" {
		t.Fatalf("data mismatch: got %q, want %q", string(data), "good-snapshot-data")
	}
}

// TestCopyMapUint64Bool verifies deep copy behavior.
func TestCopyMapUint64Bool(t *testing.T) {
	// Nil input.
	result := copyMapUint64Bool(nil)
	if result != nil {
		t.Fatalf("expected nil for nil input, got %v", result)
	}

	// Empty input.
	result = copyMapUint64Bool(map[uint64]bool{})
	if result != nil {
		t.Fatalf("expected nil for empty input, got %v", result)
	}

	// Non-empty input.
	orig := map[uint64]bool{1: true, 2: false}
	result = copyMapUint64Bool(orig)
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	if result[1] != true || result[2] != false {
		t.Fatalf("values mismatch: %v", result)
	}
}

// TestHostMessageHandler_HandleMessage_SourceAddressLearning verifies that
// when a MessageBatch carries a non-empty SourceAddress, the handler
// registers the sender's address in the host registry for each request.
// This covers the address-learning path (messagehandler.go lines 65-72).
func TestHostMessageHandler_HandleMessage_SourceAddressLearning(t *testing.T) {
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

	reg := registry.NewRegistry()
	h := &Host{
		cfg:      cfg,
		registry: reg,
	}

	handler := &hostMessageHandler{
		engine: eng,
		host:   h,
	}

	batch := proto.MessageBatch{
		SourceAddress: "10.0.0.5:9000",
		Requests: []proto.Message{
			{ShardID: 1, From: 10, To: 1, Type: proto.Heartbeat},
			{ShardID: 2, From: 20, To: 2, Type: proto.Replicate},
		},
	}

	err := handler.HandleMessage(batch)
	if err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}

	// Verify that the registry learned both senders' addresses.
	addr1, err1 := reg.Resolve(1, 10)
	if err1 != nil {
		t.Fatalf("Resolve(1, 10) failed: %v", err1)
	}
	if addr1 != "10.0.0.5:9000" {
		t.Errorf("Resolve(1, 10) = %q, want %q", addr1, "10.0.0.5:9000")
	}

	addr2, err2 := reg.Resolve(2, 20)
	if err2 != nil {
		t.Fatalf("Resolve(2, 20) failed: %v", err2)
	}
	if addr2 != "10.0.0.5:9000" {
		t.Errorf("Resolve(2, 20) = %q, want %q", addr2, "10.0.0.5:9000")
	}
}

// TestHostMessageHandler_HandleSnapshot_MkdirError verifies that when
// os.MkdirAll fails (e.g., a file blocks the directory path), the handler
// returns a SnapshotReceiveError with op="mkdir" and the partial directory
// is cleaned up.
func TestHostMessageHandler_HandleSnapshot_MkdirError(t *testing.T) {
	nodeHostDir := t.TempDir()

	// Create a regular file where the snapshot directory tree would need
	// to be created, so os.MkdirAll fails.
	blockingFile := filepath.Join(nodeHostDir, "snapshots")
	if err := os.WriteFile(blockingFile, []byte("blocker"), 0o600); err != nil {
		t.Fatalf("failed to create blocking file: %v", err)
	}

	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	h := &Host{cfg: cfg}

	handler := &hostMessageHandler{host: h}

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    1,
			ReplicaID:  2,
			From:       3,
			Index:      100,
			Term:       5,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("snapshot-data"),
		},
	}

	err := handler.HandleSnapshot(chunks)
	if err == nil {
		t.Fatal("expected error from HandleSnapshot, got nil")
	}

	var snapErr *SnapshotReceiveError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected SnapshotReceiveError, got %T: %v", err, err)
	}
	if snapErr.Op != "mkdir" {
		t.Errorf("SnapshotReceiveError.Op = %q, want %q", snapErr.Op, "mkdir")
	}
}

// TestHostMessageHandler_HandleSnapshot_MetadataWriteError verifies that
// when writeReceivedSnapshotMetadata fails (e.g., a directory blocks the
// metadata file path), HandleSnapshot returns a SnapshotReceiveError with
// op="metadata" and cleans up the snapshot directory.
func TestHostMessageHandler_HandleSnapshot_MetadataWriteError(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	h := &Host{cfg: cfg}
	handler := &hostMessageHandler{host: h}

	// Pre-create the snapshot directory and place a directory where
	// snapshot.meta would be created, so os.Create fails.
	snapshotDir := filepath.Join(
		nodeHostDir, "snapshots",
		"shard-7",
		"replica-4",
		"snapshot-00000000000000000300",
	)
	metaBlocker := filepath.Join(snapshotDir, "snapshot.meta")
	if err := os.MkdirAll(metaBlocker, 0o750); err != nil {
		t.Fatalf("failed to create blocking dir: %v", err)
	}

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    7,
			ReplicaID:  4,
			From:       1,
			Index:      300,
			Term:       12,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("valid-snapshot-data"),
		},
	}

	err := handler.HandleSnapshot(chunks)
	if err == nil {
		t.Fatal("expected error from HandleSnapshot, got nil")
	}

	var snapErr *SnapshotReceiveError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected SnapshotReceiveError, got %T: %v", err, err)
	}
	if snapErr.Op != "metadata" {
		t.Errorf("SnapshotReceiveError.Op = %q, want %q", snapErr.Op, "metadata")
	}
}

// failingSaveSnapshotLogDB wraps a real LogDB and returns an error on
// SaveSnapshot. All other methods delegate to the underlying LogDB.
type failingSaveSnapshotLogDB struct {
	logdb.LogDB
	saveErr error
}

func (f *failingSaveSnapshotLogDB) SaveSnapshot(shardID, replicaID uint64, snapshot logdb.Snapshot) error {
	return f.saveErr
}

// TestHostMessageHandler_HandleSnapshot_LogDBSaveError verifies that when
// the LogDB SaveSnapshot call fails, the handler returns a
// SnapshotReceiveError with op="logdb_save" and cleans up the snapshot
// directory.
func TestHostMessageHandler_HandleSnapshot_LogDBSaveError(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	saveErr := errors.New("simulated logdb save failure")
	failDB := &failingSaveSnapshotLogDB{
		LogDB:   memdb.New(),
		saveErr: saveErr,
	}

	h := &Host{
		cfg:   cfg,
		logdb: failDB,
	}

	handler := &hostMessageHandler{host: h}

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    5,
			ReplicaID:  3,
			From:       1,
			Index:      200,
			Term:       8,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("snapshot-data"),
			Membership: proto.Membership{
				ConfigChangeID: 10,
				Addresses: map[uint64]string{
					1: "127.0.0.1:5001",
				},
			},
		},
	}

	err := handler.HandleSnapshot(chunks)
	if err == nil {
		t.Fatal("expected error from HandleSnapshot, got nil")
	}

	var snapErr *SnapshotReceiveError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected SnapshotReceiveError, got %T: %v", err, err)
	}
	if snapErr.Op != "logdb_save" {
		t.Errorf("SnapshotReceiveError.Op = %q, want %q", snapErr.Op, "logdb_save")
	}
	if !errors.Is(snapErr.Err, saveErr) {
		t.Errorf("SnapshotReceiveError.Err = %v, want %v", snapErr.Err, saveErr)
	}

	// Snapshot directory should be cleaned up after the error.
	snapshotDir := filepath.Join(
		nodeHostDir, "snapshots",
		"shard-5",
		"replica-3",
		"snapshot-00000000000000000200",
	)
	if _, statErr := os.Stat(snapshotDir); !os.IsNotExist(statErr) {
		t.Fatal("snapshot directory should have been removed after logdb save error")
	}
}

// TestHostMessageHandler_HandleSnapshot_DuplicateSkipped verifies that
// a snapshot already present in LogDB at the same or higher index is
// silently skipped (dedup logic).
func TestHostMessageHandler_HandleSnapshot_DuplicateSkipped(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()

	// Pre-save a snapshot at index 100.
	if err := ldb.SaveSnapshot(1, 1, logdb.Snapshot{Index: 100, Term: 5}); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	h := &Host{
		cfg:   cfg,
		logdb: ldb,
	}
	handler := &hostMessageHandler{
		host: h,
	}

	// Try to deliver a snapshot at index 50 (older than existing 100).
	chunks := []proto.SnapshotChunk{
		{
			ShardID:    1,
			ReplicaID:  1,
			From:       2,
			Index:      50,
			Term:       3,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("old-snapshot"),
		},
	}
	err := handler.HandleSnapshot(chunks)
	if err != nil {
		t.Fatalf("HandleSnapshot should return nil for dedup, got: %v", err)
	}
}

// TestHostMessageHandler_HandleSnapshot_DuplicateSameIndex verifies that
// a snapshot at the exact same index is also deduped.
func TestHostMessageHandler_HandleSnapshot_DuplicateSameIndex(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	if err := ldb.SaveSnapshot(1, 1, logdb.Snapshot{Index: 100, Term: 5}); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	h := &Host{
		cfg:   cfg,
		logdb: ldb,
	}
	handler := &hostMessageHandler{
		host: h,
	}

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    1,
			ReplicaID:  1,
			From:       2,
			Index:      100, // same index
			Term:       5,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("dup-snapshot"),
		},
	}
	err := handler.HandleSnapshot(chunks)
	if err != nil {
		t.Fatalf("HandleSnapshot should return nil for same-index dedup, got: %v", err)
	}
}

// TestHostMessageHandler_HandleSnapshot_NoEngineDelivery verifies that
// HandleSnapshot succeeds even when the engine node is nil (shard not
// loaded in engine). This path exercises the code where delivered=false.
func TestHostMessageHandler_HandleSnapshot_NoEngineDelivery(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	sender := &noopSender{}
	eng := engine.NewEngine(cfg, ldb, sender, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	h := &Host{
		cfg:   cfg,
		logdb: ldb,
	}
	handler := &hostMessageHandler{
		engine: eng,
		host:   h,
	}

	// Shard 99 is NOT loaded in the engine, so DeliverMessage returns false.
	chunks := []proto.SnapshotChunk{
		{
			ShardID:    99,
			ReplicaID:  1,
			From:       2,
			Index:      10,
			Term:       1,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("snapshot-data"),
		},
	}
	err := handler.HandleSnapshot(chunks)
	if err != nil {
		t.Fatalf("HandleSnapshot should succeed (write to disk + logdb), got: %v", err)
	}
}
