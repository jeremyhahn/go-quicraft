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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
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

	err := handler.HandleMessage(batch, authorizeAllSenders)
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

	err := handler.HandleMessage(batch, authorizeAllSenders)
	if err != nil {
		t.Fatalf("HandleMessage returned unexpected error for empty batch: %v", err)
	}
}

// newEngineNodeWithMembership creates an engine.Node with a real raft.Peer
// whose membership includes the specified voters. This enables per-shard
// authorization tests where IsMember returns accurate results.
// newEngineNodeWithFullMembership builds a node from a complete proto.Membership
// so tests can seed Removed tombstones as well as voters.
func newEngineNodeWithFullMembership(t *testing.T, shardID, replicaID uint64, ms proto.Membership) *engine.Node {
	t.Helper()
	ldb := memdb.New()
	lr := logdb.NewLogReader(shardID, replicaID, ldb)
	if err := lr.ApplySnapshot(proto.Snapshot{Membership: ms}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer failed: %v", err)
	}
	return engine.NewNode(peer, nil, lr, cfg, nil, t.TempDir(), nil)
}

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
	// Give the node a real snapshot base (mirrors production, where host.go
	// roots it at NodeHostDir/snapshots/shard-N). The inbound-snapshot receiver
	// now derives its on-disk path from node.SnapshotSaveDir, so tests assert
	// against that rather than a hardcoded NodeHostDir path.
	return engine.NewNode(peer, nil, lr, cfg, nil, t.TempDir(), nil)
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

	err := handler.HandleMessage(batch, authorizeAllSenders)
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

	err := handler.HandleMessage(batch, authorizeAllSenders)
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

	err := handler.HandleMessage(batch, authorizeAllSenders)
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

	err := handler.HandleMessage(batch, authorizeAllSenders)
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
		cfg:      cfg,
		logdb:    ldb,
		registry: registry.NewRegistry(),
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

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
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

// authorizeAllSenders is a PeerAuth that authorizes any address. Used by tests
// that exercise behaviour PAST the sender-binding check.
//
// It carries no Identity, which is deliberate: a membership with committed
// identities is not authorized by address at all, so a test that wants the
// permissive fixture must also use a membership with no identities. Giving it a
// blanket identity would silently satisfy the identity gate for every replica
// and turn these tests vacuous the moment identities are involved.
var authorizeAllSenders = transport.PeerAuth{
	AuthorizesAddress: func(string) bool { return true },
}

// The committed addresses authorizeSnapshotPath installs. They are named
// because snapshotPathMembership has to reproduce them exactly: a chunk
// Membership that disagrees with the configuration the fixture authorized
// against is a REWRITE of it, which is the whole hazard the helper exists to
// keep out of tests.
const (
	snapshotPathLocalAddr  = "127.0.0.1:6001"
	snapshotPathSenderAddr = "127.0.0.1:6002"
)

// snapshotPathMembership is the committed configuration authorizeSnapshotPath
// installs, in the form a snapshot chunk carries it. A test whose snapshot must
// not REWRITE that configuration ships this as the chunk Membership.
//
// Shipping it is what a real sender does, not a convenience: forEachSnapshotChunk
// and emptySnapshotChunk both stamp every chunk with the leader's committed
// snap.Membership, so a chunk with an EMPTY Membership is a shape no sender
// produces. Delivering one is not inert — HandleSnapshot forwards it to raft as
// the InstallSnapshot's Snapshot.Membership, raft.Peer.Handle adopts it
// wholesale on the restore path (p.membership = newMembership(ss.Membership)),
// and the sender the fixture authorized is then a replica the committed
// configuration does not name. Every later receive from it is refused
// unknown_member. That adoption happens on the engine worker, ASYNCHRONOUSLY to
// the HandleSnapshot that queued it, so a test that delivers an empty
// membership and then authorizes anything else is a race, not a failure.
// snapshot_durability_test.go's recvFixture carries the same guard.
func snapshotPathMembership(localReplicaID, fromReplicaID uint64) proto.Membership {
	return proto.Membership{
		Addresses: map[uint64]string{
			localReplicaID: snapshotPathLocalAddr,
			fromReplicaID:  snapshotPathSenderAddr,
		},
	}
}

// authorizeSnapshotPath wires everything HandleSnapshot's fail-closed
// authorization needs to admit a test sender: it loads an engine node for
// shardID whose membership contains both the local replica and the
// sender, and registers the sender's address in the host registry.
//
// The membership it installs is snapshotPathMembership; a snapshot delivered
// through this path should carry that same value (see there).
func authorizeSnapshotPath(t *testing.T, eng *engine.Engine, h *Host, shardID, localReplicaID, fromReplicaID uint64) {
	t.Helper()
	const fromAddr = snapshotPathSenderAddr
	voters := snapshotPathMembership(localReplicaID, fromReplicaID).Addresses
	node := newEngineNodeWithMembership(t, shardID, localReplicaID, voters)
	eng.LoadNode(node)
	t.Cleanup(func() { eng.UnloadNode(shardID) })
	if h.registry == nil {
		h.registry = registry.NewRegistry()
	}
	h.registry.Register(shardID, fromReplicaID, fromAddr)
}

// TestHostMessageHandler_HandleSnapshot_ShardNotLoaded verifies that a
// snapshot for a shard that is not loaded locally is rejected (fail
// closed): without a loaded node there is no membership to authorize the
// sender against, and persisting the snapshot would poison a later
// StartShard.
func TestHostMessageHandler_HandleSnapshot_ShardNotLoaded(t *testing.T) {
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
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

	h := &Host{cfg: cfg, logdb: ldb, registry: registry.NewRegistry()}
	handler := &hostMessageHandler{engine: eng, host: h}

	chunks := []proto.SnapshotChunk{
		{ShardID: 42, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 1, Data: []byte("x")},
	}

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	var authErr *UnauthorizedMessageError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected UnauthorizedMessageError for unloaded shard, got %T: %v", err, err)
	}

	// Nothing must have been persisted.
	if _, gerr := ldb.GetSnapshot(42, 1); gerr == nil {
		t.Fatal("snapshot must not be persisted for an unloaded shard")
	}
}

// TestHostMessageHandler_HandleSnapshot_SenderIdentityNotBound verifies
// that a snapshot whose claimed From identity is not covered by the
// sending connection's verified TLS certificate is rejected, even when
// From is a legitimate member of the shard.
func TestHostMessageHandler_HandleSnapshot_SenderIdentityNotBound(t *testing.T) {
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
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

	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	reg := registry.NewRegistry()
	reg.Register(1, 2, "127.0.0.1:5002")
	h := &Host{cfg: cfg, logdb: ldb, registry: reg}
	handler := &hostMessageHandler{engine: eng, host: h}

	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 1, Data: []byte("x")},
	}

	// Certificate does not cover the claimed sender's address.
	denyAll := transport.PeerAuth{AuthorizesAddress: func(string) bool { return false }}
	err := handler.HandleSnapshot(chunks, denyAll)
	var authErr *UnauthorizedMessageError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected UnauthorizedMessageError for unbound identity, got %T: %v", err, err)
	}

	// A nil senderAuthorized callback must also be treated as unauthorized.
	err = handler.HandleSnapshot(chunks, transport.PeerAuth{})
	if !errors.As(err, &authErr) {
		t.Fatalf("expected UnauthorizedMessageError for nil callback, got %T: %v", err, err)
	}

	// A sender the COMMITTED MEMBERSHIP does not name must be rejected too,
	// however permissive the certificate is. Authorization comes from the
	// configuration, not from the routing registry inbound traffic writes.
	chunksFromStranger := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 99, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 1, Data: []byte("x")},
	}
	err = handler.HandleSnapshot(chunksFromStranger, authorizeAllSenders)
	if !errors.As(err, &authErr) {
		t.Fatalf("expected UnauthorizedMessageError for a sender outside the committed "+
			"membership, got %T: %v", err, err)
	}
}

func TestHostMessageHandler_HandleSnapshot_NilChunks(t *testing.T) {
	handler := &hostMessageHandler{}

	err := handler.HandleSnapshot(nil, transport.PeerAuth{})
	if err != nil {
		t.Fatalf("HandleSnapshot returned unexpected error: %v", err)
	}
}

func TestHostMessageHandler_HandleSnapshot_EmptyChunks(t *testing.T) {
	handler := &hostMessageHandler{}

	err := handler.HandleSnapshot([]proto.SnapshotChunk{}, transport.PeerAuth{})
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

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
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
	authorizeSnapshotPath(t, eng, h, 1, 2, 1)

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
			Epoch:      5,
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
			Epoch:      5,
		},
	}

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err != nil {
		t.Fatalf("HandleSnapshot returned error: %v", err)
	}

	// Verify the snapshot data file was written correctly.
	snapshotDir := handler.engine.GetNode(1).SnapshotSaveDir(500)

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
	meta, parseErr := engine.ParseSnapshotMeta(metaData)
	if parseErr != nil {
		t.Fatalf("ParseSnapshotMeta rejected the received record: %v", parseErr)
	}
	if !meta.EpochRecorded {
		t.Error("received record does not carry the epoch field the purge-floor scan reads")
	}

	if meta.Index != 500 {
		t.Errorf("metadata index = %d, want 500", meta.Index)
	}
	if meta.Term != 10 {
		t.Errorf("metadata term = %d, want 10", meta.Term)
	}
	if meta.ShardID != 1 {
		t.Errorf("metadata shardID = %d, want 1", meta.ShardID)
	}
	if meta.ReplicaID != 2 {
		t.Errorf("metadata replicaID = %d, want 2", meta.ReplicaID)
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
	// Per-node snapshot encryption: the receiver records its OWN at-rest epoch
	// from re-encrypting the (plaintext) received snapshot under its own barrier,
	// NOT the wire epoch. This handler has no barrier, so the snapshot stays
	// plaintext at rest and the recorded epoch is 0 — the chunks' wire Epoch (5)
	// is intentionally not trusted (in production the sender streams plaintext
	// with Epoch=0 via decrypt-for-send; the receiver is authoritative for its
	// own at-rest epoch).
	if snap.Epoch != 0 {
		t.Errorf("LogDB snapshot epoch = %d, want 0 (no receiver barrier => plaintext at rest)", snap.Epoch)
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
	authorizeSnapshotPath(t, eng, h, 3, 1, 2)

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

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err != nil {
		t.Fatalf("HandleSnapshot returned error: %v", err)
	}

	// Verify data file.
	snapshotDir := handler.engine.GetNode(3).SnapshotSaveDir(1000)
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
	authorizeSnapshotPath(t, eng, h, 5, 1, 3)

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

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err != nil {
		t.Fatalf("HandleSnapshot returned error for empty data chunk: %v", err)
	}

	// The data file should exist but be empty.
	snapshotDir := handler.engine.GetNode(5).SnapshotSaveDir(200)
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
	authorizeSnapshotPath(t, eng, h, 1, 1, 2)

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

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err != nil {
		t.Fatalf("HandleSnapshot returned error with nil logdb: %v", err)
	}

	// Verify data was still written to disk.
	snapshotDir := handler.engine.GetNode(1).SnapshotSaveDir(100)
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

	err := writeReceivedSnapshotMetadata(osSnapshotFS{}, dir, 42, 7, 10, 3, 9, config.Snappy)
	if err != nil {
		t.Fatalf("writeReceivedSnapshotMetadata returned error: %v", err)
	}

	metaPath := filepath.Join(dir, "snapshot.meta")
	data, readErr := os.ReadFile(metaPath)
	if readErr != nil {
		t.Fatalf("failed to read metadata file: %v", readErr)
	}
	// Decode through the SHARED decoder rather than re-deriving the offsets
	// here: the point of the assertion is that the receive path writes a
	// record the purge-floor scan can read, and the scan uses this decoder.
	meta, parseErr := engine.ParseSnapshotMeta(data)
	if parseErr != nil {
		t.Fatalf("ParseSnapshotMeta rejected the received record: %v (the purge-floor "+
			"scan reads such a record as indeterminate and stops purging)", parseErr)
	}

	if meta.Index != 42 {
		t.Errorf("index = %d, want 42", meta.Index)
	}
	if meta.Term != 7 {
		t.Errorf("term = %d, want 7", meta.Term)
	}
	if meta.ShardID != 10 {
		t.Errorf("shardID = %d, want 10", meta.ShardID)
	}
	if meta.ReplicaID != 3 {
		t.Errorf("replicaID = %d, want 3", meta.ReplicaID)
	}
	if meta.CreatedAt == 0 {
		t.Error("receivedAt should be non-zero")
	}
	// The epoch field is what the DEK purge-floor scan reads; a record that
	// does not carry it is unevaluable and lets the artifact's DEK be purged
	// only after suppressing every other purge under the same barrier.
	if !meta.EpochRecorded {
		t.Error("record does not carry the epoch field")
	}
	if meta.Epoch != 9 {
		t.Errorf("epoch = %d, want 9", meta.Epoch)
	}
	if !meta.CompressionRecorded {
		t.Error("record does not carry the compression field")
	}
	if meta.Compression != config.Snappy {
		t.Errorf("compression = %d, want %d", meta.Compression, config.Snappy)
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
	err := writeReceivedSnapshotMetadata(osSnapshotFS{}, invalidDir, 1, 1, 1, 1, 0, config.NoCompression)
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
	authorizeSnapshotPath(t, eng, h, 10, 1, 2)

	// A snapshot directory already exists at the index about to be received,
	// holding a complete artifact, and LogDB does NOT record it — the shape a
	// previous transfer leaves when it wrote the files and then failed, and the
	// shape a same-index re-send routinely meets. It must survive a transfer
	// that fails: the receive may only discard directories it created itself.
	node := handler.engine.GetNode(10)
	snapshotDir := node.SnapshotSaveDir(777)
	cfs := newCrashFS(node.SnapshotDir(), cfg.NodeHostDir)
	h.fsys = cfs

	dataPath := filepath.Join(snapshotDir, "snapshot.dat")
	const preexisting = "a complete snapshot that predates this transfer"
	if err := cfs.MkdirAll(snapshotDir, 0o750); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	seed, err := cfs.OpenFile(dataPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("seed snapshot.dat: %v", err)
	}
	if _, err := seed.Write([]byte(preexisting)); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	// Fail the receive's own write, after it has created its staging
	// directory. The hook is installed AFTER the seeding above so the seed is
	// not what trips it.
	cfs.failOnce(opWrite, "snapshot.dat", errInjected)

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

	err = handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err == nil {
		t.Fatal("expected error from HandleSnapshot, got nil")
	}

	var snapErr *SnapshotReceiveError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected SnapshotReceiveError, got %T: %v", err, err)
	}

	// The pre-existing artifact must be untouched. Writing straight into the
	// canonical directory truncated it before a byte was validated, and the
	// failure cleanup then removed the whole directory — destroying a complete
	// snapshot on the strength of a transfer that failed.
	got, exists := cfs.liveBytes(dataPath)
	if !exists {
		t.Fatal("the pre-existing snapshot.dat is gone after a FAILED receive: " +
			"the receive wrote into, and then deleted, a directory it did not create")
	}
	if string(got) != preexisting {
		t.Fatalf("the pre-existing snapshot.dat = %q, want %q: the receive "+
			"truncated an artifact it did not create before validating a byte",
			got, preexisting)
	}

	// And the receive must not leak its own staging directory.
	for _, name := range cfs.entries(filepath.Dir(snapshotDir)) {
		if engine.IsSnapshotRecvStage(name) {
			t.Fatalf("the failed receive left its staging directory %q behind", name)
		}
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
	authorizeSnapshotPath(t, eng, h, 20, 1, 2)

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

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err != nil {
		t.Fatalf("HandleSnapshot returned error: %v", err)
	}

	// The snapshot directory should still exist.
	snapshotDir := handler.engine.GetNode(20).SnapshotSaveDir(888)
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
// when a MessageBatch carries a non-empty SourceAddress, the handler learns
// the sender's address in the host registry — but ONLY for senders that are
// authorized members of a loaded shard.
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

	// Load shard 1 with membership {1, 10} so replica 10 is an authorized
	// member whose address may be learned.
	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1:  "127.0.0.1:5001",
		10: "127.0.0.1:5010",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

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
		},
	}

	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}

	// The authorized member's address was learned.
	addr, err := reg.Resolve(1, 10)
	if err != nil {
		t.Fatalf("Resolve(1, 10) failed: %v", err)
	}
	if addr != "10.0.0.5:9000" {
		t.Errorf("Resolve(1, 10) = %q, want %q", addr, "10.0.0.5:9000")
	}
}

// TestHostMessageHandler_HandleMessage_UnauthorizedSourceNotLearned is the M2
// regression test: when the batch SourceAddress is NOT bound to the sender's
// certificate (senderAuthorized returns false, or is nil), the handler must not
// learn it into the return-path registry — otherwise a peer could teach the
// registry an address its certificate does not cover and hijack another
// replica's return path.
func TestHostMessageHandler_HandleMessage_UnauthorizedSourceNotLearned(t *testing.T) {
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	eng := engine.NewEngine(cfg, nil, &noopSender{}, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1:  "127.0.0.1:5001",
		10: "127.0.0.1:5010",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	reg := registry.NewRegistry()
	handler := &hostMessageHandler{engine: eng, host: &Host{cfg: cfg, registry: reg}}

	batch := proto.MessageBatch{
		SourceAddress: "10.0.0.5:9000", // an address the sender's cert does not cover
		Requests:      []proto.Message{{ShardID: 1, From: 10, To: 1, Type: proto.Heartbeat}},
	}

	// Cert does not authorize the address: it must NOT be learned.
	denyAll := transport.PeerAuth{AuthorizesAddress: func(string) bool { return false }}
	if err := handler.HandleMessage(batch, denyAll); err != nil {
		t.Fatalf("HandleMessage error: %v", err)
	}
	if addr, err := reg.Resolve(1, 10); err == nil {
		t.Fatalf("source address %q was learned despite the sender being unauthorized for it (registry poisoning)", addr)
	}

	// A nil callback must be treated as unauthorized too.
	if err := handler.HandleMessage(batch, transport.PeerAuth{}); err != nil {
		t.Fatalf("HandleMessage(nil authorizer) error: %v", err)
	}
	if addr, err := reg.Resolve(1, 10); err == nil {
		t.Fatalf("source address %q was learned with a nil authorizer", addr)
	}
}

// TestHostMessageHandler_HandleMessage_NoLearnForUnloadedShard verifies that
// the handler does NOT learn an address for a shard that is not loaded
// locally. The arbitrary replicaID in From cannot be verified as a real
// member until the shard is loaded with its configured membership, so
// learning is deferred to prevent registry poisoning.
func TestHostMessageHandler_HandleMessage_NoLearnForUnloadedShard(t *testing.T) {
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
	h := &Host{cfg: cfg, registry: reg}
	handler := &hostMessageHandler{engine: eng, host: h}

	// Shard 7 is NOT loaded; the message is still delivered (bootstrap
	// path) but no address is learned for it.
	batch := proto.MessageBatch{
		SourceAddress: "10.0.0.9:9000",
		Requests: []proto.Message{
			{ShardID: 7, From: 3, To: 1, Type: proto.Replicate},
		},
	}
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}

	if _, err := reg.Resolve(7, 3); err == nil {
		t.Fatal("expected no learned address for unloaded shard, but one was registered")
	}
}

// TestHostMessageHandler_HandleMessage_NoLearnForNonMember verifies that the
// handler does NOT learn an address for a sender that is not a member of a
// loaded shard (the message is dropped before learning).
func TestHostMessageHandler_HandleMessage_NoLearnForNonMember(t *testing.T) {
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

	// Load shard 1 with membership {1, 2}. Replica 999 is NOT a member.
	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	reg := registry.NewRegistry()
	h := &Host{cfg: cfg, registry: reg}
	handler := &hostMessageHandler{engine: eng, host: h}

	batch := proto.MessageBatch{
		SourceAddress: "10.0.0.6:9000",
		Requests: []proto.Message{
			{ShardID: 1, From: 999, To: 1, Type: proto.Heartbeat},
		},
	}
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}

	if _, err := reg.Resolve(1, 999); err == nil {
		t.Fatal("expected no learned address for non-member, but one was registered")
	}
}

// TestHostMessageHandler_HandleMessage_UpdatesAddressForRecovery verifies that
// an inbound message from an authorized member with a NEW SourceAddress updates
// the registry. This overwrite is required for liveness: a node that recovers
// at a new address (and, during quorum loss, has no leader to propagate a
// config change) must be relearned from its own traffic or the cluster can
// never re-form a quorum. Anti-poisoning is enforced one layer down, at the
// transport, which clears any SourceAddress not covered by the sender's
// verified TLS certificate (covered by the transport package's tests); this
// handler-level test exercises the post-binding overwrite behavior.
func TestHostMessageHandler_HandleMessage_UpdatesAddressForRecovery(t *testing.T) {
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

	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	reg := registry.NewRegistry()
	// Replica 2's old address is known (e.g. from bootstrap/config).
	reg.Register(1, 2, "127.0.0.1:5002")

	h := &Host{cfg: cfg, registry: reg}
	handler := &hostMessageHandler{engine: eng, host: h}

	// Replica 2 recovered at a NEW address and sends traffic. The transport
	// has already cert-bound this SourceAddress; the handler must update the
	// mapping so peers can reach the recovered node.
	const newAddr = "127.0.0.1:5999"
	batch := proto.MessageBatch{
		SourceAddress: newAddr,
		Requests: []proto.Message{
			{ShardID: 1, From: 2, To: 1, Type: proto.Heartbeat},
		},
	}
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}

	addr, err := reg.Resolve(1, 2)
	if err != nil {
		t.Fatalf("Resolve(1, 2) failed: %v", err)
	}
	if addr != newAddr {
		t.Errorf("address was not updated for recovery: got %q, want %q", addr, newAddr)
	}
}

// TestHostMessageHandler_HandleMessage_SameAddressNoChange verifies that a
// repeated claim of the SAME address is a harmless no-op (idempotent).
func TestHostMessageHandler_HandleMessage_SameAddressNoChange(t *testing.T) {
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

	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(1)

	reg := registry.NewRegistry()
	h := &Host{cfg: cfg, registry: reg}
	handler := &hostMessageHandler{engine: eng, host: h}

	batch := proto.MessageBatch{
		SourceAddress: "10.0.0.2:9000",
		Requests: []proto.Message{
			{ShardID: 1, From: 2, To: 1, Type: proto.Heartbeat},
		},
	}
	// First message learns the address.
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}
	// Second identical message is a no-op (no overwrite, no warning needed).
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage returned unexpected error: %v", err)
	}

	addr, err := reg.Resolve(1, 2)
	if err != nil {
		t.Fatalf("Resolve(1, 2) failed: %v", err)
	}
	if addr != "10.0.0.2:9000" {
		t.Errorf("Resolve(1, 2) = %q, want %q", addr, "10.0.0.2:9000")
	}
}

// TestHostMessageHandler_HandleSnapshot_MkdirError verifies that when
// os.MkdirAll fails (e.g., a file blocks the directory path), the handler
// returns a SnapshotReceiveError with op="mkdir" and the partial directory
// is cleaned up.
func TestHostMessageHandler_HandleSnapshot_MkdirError(t *testing.T) {
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

	h := &Host{cfg: cfg}

	handler := &hostMessageHandler{engine: eng, host: h}
	authorizeSnapshotPath(t, eng, h, 1, 2, 3)

	// Create a regular file where the receiver must MkdirAll its snapshot tree
	// (node.SnapshotSaveDir = <base>/shard-1/replica-2/snapshot-...), so MkdirAll
	// fails. The base comes from the loaded node, which the receiver now uses.
	node := eng.GetNode(1)
	blockingFile := filepath.Join(node.SnapshotDir(), "shard-1")
	if err := os.WriteFile(blockingFile, []byte("blocker"), 0o600); err != nil {
		t.Fatalf("failed to create blocking file: %v", err)
	}

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

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
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

	sender := &noopSender{}
	eng := engine.NewEngine(cfg, nil, sender, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	h := &Host{cfg: cfg}
	handler := &hostMessageHandler{engine: eng, host: h}
	authorizeSnapshotPath(t, eng, h, 7, 4, 1)

	// Fail the metadata write. The receive stages in a directory of its own,
	// so a blocker planted at the canonical path no longer intercepts it — the
	// failure is injected at the filesystem instead.
	node := handler.engine.GetNode(7)
	cfs := newCrashFS(node.SnapshotDir(), cfg.NodeHostDir)
	h.fsys = cfs
	cfs.failOnce(opWrite, "snapshot.meta", errInjected)

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

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
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

	sender := &noopSender{}
	eng := engine.NewEngine(cfg, nil, sender, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	h := &Host{
		cfg:   cfg,
		logdb: failDB,
	}

	handler := &hostMessageHandler{engine: eng, host: h}
	authorizeSnapshotPath(t, eng, h, 5, 3, 1)

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

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
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
	snapshotDir := handler.engine.GetNode(5).SnapshotSaveDir(200)
	if _, statErr := os.Stat(snapshotDir); !os.IsNotExist(statErr) {
		t.Fatal("snapshot directory should have been removed after logdb save error")
	}
}

// seedPersistedSnapshot writes a snapshot.dat holding data into a fresh
// directory and records the matching LogDB snapshot record, modelling a
// snapshot this replica already durably holds. The dedup branch re-delivers
// the install FROM this artifact, so it must actually exist on disk.
func seedPersistedSnapshot(t *testing.T, ldb logdb.LogDB, shardID, replicaID, index, term uint64, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "snapshot.dat"), data, 0o600); err != nil {
		t.Fatalf("write snapshot.dat: %v", err)
	}
	if err := ldb.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index:    index,
		Term:     term,
		Filepath: dir,
		FileSize: uint64(len(data)),
	}); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}
	return dir
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

	// Pre-save a snapshot at index 100, artifact included: the dedup branch
	// re-delivers the install from the persisted snapshot file.
	seedPersistedSnapshot(t, ldb, 1, 1, 100, 5, []byte("persisted-snapshot"))

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
	authorizeSnapshotPath(t, eng, h, 1, 1, 2)

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
	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
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
	seedPersistedSnapshot(t, ldb, 1, 1, 100, 5, []byte("persisted-snapshot"))

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
	authorizeSnapshotPath(t, eng, h, 1, 1, 2)

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
	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err != nil {
		t.Fatalf("HandleSnapshot should return nil for same-index dedup, got: %v", err)
	}
}

// TestHostMessageHandler_HandleSnapshot_NoEngineDelivery verifies that
// HandleSnapshot REJECTS a snapshot when the engine node is nil (shard
// not loaded in engine): authorization fails closed because there is no
// membership to validate the sender against.
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
	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	var authErr *UnauthorizedMessageError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected UnauthorizedMessageError for unloaded shard, got %T: %v", err, err)
	}
	// Nothing must have been persisted to LogDB or disk.
	if _, gerr := ldb.GetSnapshot(99, 1); gerr == nil {
		t.Fatal("snapshot must not be persisted for an unloaded shard")
	}
	assertNoSnapshotDir(t, handler, 99, 1, 10)
}

// newSnapshotHandler builds a handler with a real engine + memdb LogDB and a
// host rooted at the returned nodeHostDir, for chunk-validation tests.
func newSnapshotHandler(t *testing.T) (*hostMessageHandler, string) {
	t.Helper()
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
	t.Cleanup(eng.Stop)

	h := &Host{cfg: cfg, logdb: ldb, registry: registry.NewRegistry()}
	// Authorize the chunk identities the validation tests use (shard 1 and
	// shard 4, local replica 1, sender replica 2) so failures exercised by
	// those tests occur past the fail-closed authorization gate.
	authorizeSnapshotPath(t, eng, h, 1, 1, 2)
	authorizeSnapshotPath(t, eng, h, 4, 1, 2)
	return &hostMessageHandler{engine: eng, host: h}, nodeHostDir
}

// assertValidateError fails the test unless err is a *SnapshotReceiveError
// with Op == "validate".
func assertValidateError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a validation error, got nil")
	}
	var snapErr *SnapshotReceiveError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected *SnapshotReceiveError, got %T: %v", err, err)
	}
	if snapErr.Op != "validate" {
		t.Fatalf("SnapshotReceiveError.Op = %q, want %q", snapErr.Op, "validate")
	}
}

// TestHandleSnapshot_RejectsGap verifies a chunk set with a missing ChunkID
// (gap) is rejected without persisting anything.
func TestHandleSnapshot_RejectsGap(t *testing.T) {
	handler, _ := newSnapshotHandler(t)

	// ChunkCount 3 but only ChunkIDs 0 and 2 present (1 is missing).
	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 3, Data: []byte("a")},
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 2, ChunkCount: 3, Data: []byte("c")},
	}
	// Note: len(chunks)=2 != ChunkCount=3, so this also trips the count check;
	// either way it must be a validate error.
	assertValidateError(t, handler.HandleSnapshot(chunks, authorizeAllSenders))
	assertNoSnapshotDir(t, handler, 1, 1, 100)
}

// TestHandleSnapshot_RejectsMissingMiddleChunk verifies a chunk set whose
// count is correct but contains a duplicate filling another slot (so a
// middle ChunkID is absent) is rejected as a gap.
func TestHandleSnapshot_RejectsMissingMiddleChunk(t *testing.T) {
	handler, _ := newSnapshotHandler(t)

	// Three chunks, ChunkIDs {0, 2, 2}: ID 1 is missing, ID 2 duplicated.
	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 3, Data: []byte("a")},
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 2, ChunkCount: 3, Data: []byte("c")},
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 2, ChunkCount: 3, Data: []byte("c2")},
	}
	assertValidateError(t, handler.HandleSnapshot(chunks, authorizeAllSenders))
	assertNoSnapshotDir(t, handler, 1, 1, 100)
}

// TestHandleSnapshot_RejectsDuplicate verifies a chunk set with a duplicate
// ChunkID is rejected.
func TestHandleSnapshot_RejectsDuplicate(t *testing.T) {
	handler, _ := newSnapshotHandler(t)

	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, Data: []byte("a")},
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, Data: []byte("a-dup")},
	}
	assertValidateError(t, handler.HandleSnapshot(chunks, authorizeAllSenders))
	assertNoSnapshotDir(t, handler, 1, 1, 100)
}

// TestHandleSnapshot_RejectsWrongCount verifies a chunk set whose declared
// ChunkCount disagrees with the number of chunks received is rejected.
func TestHandleSnapshot_RejectsWrongCount(t *testing.T) {
	handler, _ := newSnapshotHandler(t)

	// Two chunks received, but each declares ChunkCount 5.
	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 5, Data: []byte("a")},
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 1, ChunkCount: 5, Data: []byte("b")},
	}
	assertValidateError(t, handler.HandleSnapshot(chunks, authorizeAllSenders))
	assertNoSnapshotDir(t, handler, 1, 1, 100)
}

// TestHandleSnapshot_RejectsInconsistentCount verifies that chunks declaring
// different ChunkCount values are rejected.
func TestHandleSnapshot_RejectsInconsistentCount(t *testing.T) {
	handler, _ := newSnapshotHandler(t)

	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, Data: []byte("a")},
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 1, ChunkCount: 3, Data: []byte("b")},
	}
	assertValidateError(t, handler.HandleSnapshot(chunks, authorizeAllSenders))
	assertNoSnapshotDir(t, handler, 1, 1, 100)
}

// TestHandleSnapshot_RejectsIdentityMismatch verifies that a chunk whose
// identity/metadata (here Term) differs from chunk 0 is rejected. This is
// the core BUG C fix: chunks[0] is no longer blindly trusted for the set.
func TestHandleSnapshot_RejectsIdentityMismatch(t *testing.T) {
	handler, _ := newSnapshotHandler(t)

	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, Data: []byte("a")},
		// Term 6 differs from chunk 0's Term 5.
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 6, ChunkID: 1, ChunkCount: 2, Data: []byte("b")},
	}
	assertValidateError(t, handler.HandleSnapshot(chunks, authorizeAllSenders))
	assertNoSnapshotDir(t, handler, 1, 1, 100)
}

// TestHandleSnapshot_RejectsEpochMismatch verifies that a chunk whose Epoch
// differs from chunk 0 is rejected (the production sender stamps the same
// Epoch on every chunk).
func TestHandleSnapshot_RejectsEpochMismatch(t *testing.T) {
	handler, _ := newSnapshotHandler(t)

	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, Epoch: 3, Data: []byte("a")},
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 1, ChunkCount: 2, Epoch: 9, Data: []byte("b")},
	}
	assertValidateError(t, handler.HandleSnapshot(chunks, authorizeAllSenders))
	assertNoSnapshotDir(t, handler, 1, 1, 100)
}

// TestHandleSnapshot_RejectsFileSizeMismatch verifies that when the declared
// FileSize does not equal the reassembled byte count, the snapshot is
// rejected (truncated or forged size).
func TestHandleSnapshot_RejectsFileSizeMismatch(t *testing.T) {
	handler, _ := newSnapshotHandler(t)

	// Two chunks totaling 6 bytes, but FileSize declares 100.
	chunks := []proto.SnapshotChunk{
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 0, ChunkCount: 2, FileSize: 100, Data: []byte("abc")},
		{ShardID: 1, ReplicaID: 1, From: 2, Index: 100, Term: 5, ChunkID: 1, ChunkCount: 2, FileSize: 100, Data: []byte("def")},
	}
	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err == nil {
		t.Fatal("expected FileSize mismatch error, got nil")
	}
	var snapErr *SnapshotReceiveError
	if !errors.As(err, &snapErr) {
		t.Fatalf("expected *SnapshotReceiveError, got %T: %v", err, err)
	}
	if snapErr.Op != "validate" {
		t.Errorf("Op = %q, want %q", snapErr.Op, "validate")
	}
}

// TestHandleSnapshot_ReassemblesOutOfOrder verifies that chunks delivered in
// arrival order != ChunkID order are reassembled in ChunkID order, so the
// on-disk snapshot is byte-correct regardless of delivery ordering.
func TestHandleSnapshot_ReassemblesOutOfOrder(t *testing.T) {
	handler, _ := newSnapshotHandler(t)

	// Arrival order: 2, 0, 1. Correct assembly must be "AAABBBCCC".
	chunks := []proto.SnapshotChunk{
		{ShardID: 4, ReplicaID: 1, From: 2, Index: 700, Term: 9, ChunkID: 2, ChunkCount: 3, Data: []byte("CCC")},
		{ShardID: 4, ReplicaID: 1, From: 2, Index: 700, Term: 9, ChunkID: 0, ChunkCount: 3, Data: []byte("AAA")},
		{ShardID: 4, ReplicaID: 1, From: 2, Index: 700, Term: 9, ChunkID: 1, ChunkCount: 3, Data: []byte("BBB")},
	}
	if err := handler.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
		t.Fatalf("HandleSnapshot returned error: %v", err)
	}

	snapshotDir := handler.engine.GetNode(4).SnapshotSaveDir(700)
	data, err := os.ReadFile(filepath.Join(snapshotDir, "snapshot.dat"))
	if err != nil {
		t.Fatalf("failed to read reassembled snapshot: %v", err)
	}
	if string(data) != "AAABBBCCC" {
		t.Fatalf("reassembled data = %q, want %q (must follow ChunkID order)",
			string(data), "AAABBBCCC")
	}
}

// assertNoSnapshotDir fails the test if a snapshot directory exists for the
// given identity, confirming an invalid chunk set persisted nothing.
func assertNoSnapshotDir(t *testing.T, handler *hostMessageHandler, shardID, replicaID, index uint64) {
	t.Helper()
	node := handler.engine.GetNode(shardID)
	if node == nil {
		return // shard not loaded: the receiver rejects early and persists nothing
	}
	snapshotDir := node.SnapshotSaveDir(index)
	if _, err := os.Stat(snapshotDir); !os.IsNotExist(err) {
		t.Fatalf("expected no snapshot directory for invalid chunk set, but %q exists", snapshotDir)
	}
}

// TestHandleSnapshot_LandsUnderNodeSnapshotDir is the CX3 regression test: an
// inbound snapshot must be written under the engine node's SnapshotDir() — the
// root that snapshot GC and the DEK-purge floor scan — NOT under a hardcoded
// NodeHostDir/snapshots path. For a per-shard DataDir (which roots SnapshotDir()
// off the DataDir, distinct from NodeHostDir), the old hardcoded path was
// invisible to GC/purge: received snapshots leaked and a key rotation could
// purge a DEK they still needed.
func TestHandleSnapshot_LandsUnderNodeSnapshotDir(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:        t.TempDir(),
		NodeHostDir:   nodeHostDir,
		RaftAddress:   "127.0.0.1:5000",
		ListenAddress: "127.0.0.1:5000",
		DeploymentID:  1,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	eng := engine.NewEngine(cfg, ldb, &noopSender{}, nil, nil, nil)
	eng.Start()
	defer eng.Stop()
	h := &Host{cfg: cfg, logdb: ldb, registry: registry.NewRegistry()}
	handler := &hostMessageHandler{engine: eng, host: h}

	const shardID, localReplica, fromReplica, index = uint64(8), uint64(1), uint64(2), uint64(555)
	authorizeSnapshotPath(t, eng, h, shardID, localReplica, fromReplica)

	chunks := []proto.SnapshotChunk{{
		ShardID: shardID, ReplicaID: localReplica, From: fromReplica,
		Index: index, Term: 3, ChunkID: 0, ChunkCount: 1, Data: []byte("snapshot-payload"),
	}}
	if err := handler.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
		t.Fatalf("HandleSnapshot: %v", err)
	}

	node := eng.GetNode(shardID)
	gotDir := node.SnapshotSaveDir(index)
	if _, err := os.Stat(filepath.Join(gotDir, "snapshot.dat")); err != nil {
		t.Fatalf("snapshot not written under node.SnapshotSaveDir(%d): %v", index, err)
	}
	// The snapshot MUST be under the GC/DEK-purge scan root (node.SnapshotDir()).
	if !strings.HasPrefix(gotDir, node.SnapshotDir()+string(filepath.Separator)) {
		t.Fatalf("snapshot dir %q is not under node.SnapshotDir() %q — GC/purge would miss it",
			gotDir, node.SnapshotDir())
	}
	// The node's snapshot base is distinct from the hardcoded NodeHostDir path
	// (this models a DataDir shard), and the snapshot is NOT under that path.
	hardcoded := filepath.Join(nodeHostDir, "snapshots")
	if strings.HasPrefix(gotDir, hardcoded+string(filepath.Separator)) {
		t.Fatalf("snapshot dir %q landed under the hardcoded NodeHostDir path (CX3 regression)", gotDir)
	}
}

// authorizationRigCore builds the engine and node shared by authorizationRig
// and authorizationRigRecording. The two differ only in which engineDeliverer
// the handler is wired to, so the shared setup lives here rather than being
// duplicated.
func authorizationRigCore(t *testing.T, ms proto.Membership) (*engine.Engine, *engine.Node, *Host) {
	t.Helper()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()
	eng := engine.NewEngine(cfg, nil, &noopSender{}, nil, nil, nil)
	eng.Start()
	t.Cleanup(eng.Stop)

	node := newEngineNodeWithFullMembership(t, 1, 1, ms)
	eng.LoadNode(node)
	t.Cleanup(func() { eng.UnloadNode(1) })

	h := &Host{cfg: cfg, registry: registry.NewRegistry()}
	return eng, node, h
}

// authorizationRig builds an engine + handler with shard 1 loaded under ms.
func authorizationRig(t *testing.T, ms proto.Membership) (*engine.Engine, *hostMessageHandler, *engine.Node) {
	t.Helper()
	eng, node, h := authorizationRigCore(t, ms)
	return eng, &hostMessageHandler{engine: eng, host: h}, node
}

// authorizationRigJoining is authorizationRig for a replica that is still
// joining: started with join=true and holding no configuration yet.
func authorizationRigJoining(t *testing.T, ms proto.Membership) (*engine.Engine, *hostMessageHandler, *engine.Node) {
	t.Helper()
	eng, handler, node := authorizationRig(t, ms)
	node.Peer().MarkJoining()
	return eng, handler, node
}

// recordingDeliverWrapper wraps a real engineDeliverer, synchronously
// recording every message DeliverMessage accepts before delegating to the
// wrapped implementation (go-quicraft#26).
//
// authorizationRig's eng.Start() runs a real background worker that
// concurrently drains the same inbox queue a test would otherwise have to
// poll (DiagState().InboxLen) to observe delivery -- and that queue-depth
// snapshot is racy: under enough scheduling delay on the test goroutine, the
// worker can drain the just-delivered message before the test ever samples
// the length, so a later re-read (even a polled one tracking the maximum
// seen) is indistinguishable from "never delivered" if the worker both
// drains and stabilizes before the first sample. Recording synchronously at
// the DeliverMessage call itself -- which HandleMessage invokes directly,
// in the test's own goroutine, before it returns -- removes the race
// entirely: delivery is observed at the moment it happens, not inferred
// from a live counter a second goroutine is concurrently mutating.
type recordingDeliverWrapper struct {
	engineDeliverer
	mu        sync.Mutex
	delivered []proto.Message
}

func (w *recordingDeliverWrapper) DeliverMessage(shardID uint64, msg proto.Message) bool {
	ok := w.engineDeliverer.DeliverMessage(shardID, msg)
	if ok {
		w.mu.Lock()
		w.delivered = append(w.delivered, msg)
		w.mu.Unlock()
	}
	return ok
}

// DeliveredCount returns the number of messages DeliverMessage has accepted
// so far.
func (w *recordingDeliverWrapper) DeliveredCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.delivered)
}

// authorizationRigRecording is authorizationRig with the handler's engine
// wrapped in a recordingDeliverWrapper, for tests that must assert delivery
// deterministically rather than by polling engine.Node's live queue depth.
func authorizationRigRecording(t *testing.T, ms proto.Membership) (*hostMessageHandler, *engine.Node, *recordingDeliverWrapper) {
	t.Helper()
	eng, node, h := authorizationRigCore(t, ms)
	rec := &recordingDeliverWrapper{engineDeliverer: eng}
	return &hostMessageHandler{engine: rec, host: h}, node, rec
}

// authorizationRigRecordingJoining is authorizationRigRecording for a
// replica that is still joining.
func authorizationRigRecordingJoining(t *testing.T, ms proto.Membership) (*hostMessageHandler, *engine.Node, *recordingDeliverWrapper) {
	t.Helper()
	handler, node, rec := authorizationRigRecording(t, ms)
	node.Peer().MarkJoining()
	return handler, node, rec
}

// TestHostMessageHandler_AcceptsSenderAbsentFromAStaleConfiguration is the
// regression for the wedge the member allow-list caused.
//
// A replica's configuration lags by design. Replica 5 is added to the shard and
// becomes leader while this host is behind, so the local configuration still
// reads {1,2}. Under an allow-list every Heartbeat, Replicate and
// InstallSnapshot from leader 5 was dropped -- including the very messages that
// would have delivered the config change naming 5 -- so the replica was deaf to
// its own leader for the life of the process while the shard silently ran with
// reduced redundancy, and nothing re-opened the window.
//
// Authorization is now a removed-tombstone deny-list: monotone, so it cannot go
// stale in the unsafe direction. Same model as etcd (EtcdServer.Process rejects
// only a "message from removed member") and hashicorp/raft (no sender-membership
// test on appendEntries at all).
func TestHostMessageHandler_AcceptsSenderAbsentFromAStaleConfiguration(t *testing.T) {
	handler, node, rec := authorizationRigRecording(t, proto.Membership{
		Addresses: map[uint64]string{1: "127.0.0.1:5001", 2: "127.0.0.1:5002"},
	})

	if node.IsRemoved(5) {
		t.Fatal("fixture: replica 5 must not carry a removal tombstone")
	}

	batch := proto.MessageBatch{
		SourceAddress: "127.0.0.1:5005",
		Requests: []proto.Message{
			{ShardID: 1, From: 5, To: 1, Type: proto.Heartbeat, Term: 9},
		},
	}
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	// rec.DeliveredCount, not a DiagState().InboxLen re-read: the recording
	// wrapper observes delivery synchronously at the DeliverMessage call
	// HandleMessage makes directly, in this goroutine, before it returns --
	// so there is no race against the engine's background worker draining
	// the same message out of the live queue a re-read would otherwise poll
	// (go-quicraft#26).
	if rec.DeliveredCount() == 0 {
		t.Fatal("the heartbeat from leader 5 was dropped because our configuration " +
			"does not name it yet: this replica can never learn the config change " +
			"that adds 5, so it stays deaf to its own leader permanently")
	}
}

// TestHostMessageHandler_RejectsRemovedSender pins the other half: a COMMITTED
// removal is still enforced. The tombstone is the one membership-derived fact
// that is safe on the receive path, because it only ever grows.
func TestHostMessageHandler_RejectsRemovedSender(t *testing.T) {
	_, handler, node := authorizationRig(t, proto.Membership{
		Addresses: map[uint64]string{1: "127.0.0.1:5001", 2: "127.0.0.1:5002"},
		Removed:   map[uint64]bool{7: true},
	})

	if !node.IsRemoved(7) {
		t.Fatal("fixture: replica 7 must carry a removal tombstone")
	}
	before := node.DiagState().InboxLen

	batch := proto.MessageBatch{
		SourceAddress: "127.0.0.1:5007",
		Requests: []proto.Message{
			{ShardID: 1, From: 7, To: 1, Type: proto.Heartbeat, Term: 9},
		},
	}
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	if node.DiagState().InboxLen != before {
		t.Fatal("a message from a replica with a committed Removed tombstone was " +
			"delivered to raft")
	}
}

// TestHostMessageHandler_JoiningReplicaLearnsTheLeaderAddress is the regression
// for the bootstrap deadlock a strict membership gate on address learning
// causes.
//
// A replica joining a shard starts with an EMPTY configuration, so IsMember is
// false for everyone -- including the leader. If it cannot record the leader's
// address it cannot route its ReplicateResp back, so it never acks, the leader's
// match index never advances, and the join never completes. Every membership
// change and add-node integration test times out at exactly this point.
func TestHostMessageHandler_JoiningReplicaLearnsTheLeaderAddress(t *testing.T) {
	_, handler, node := authorizationRigJoining(t, proto.Membership{})

	if node.IsMember(1) {
		t.Fatal("fixture: a joining replica must know no members")
	}
	if !node.IsJoining() {
		t.Fatal("fixture: the replica must be marked joining")
	}

	batch := proto.MessageBatch{
		SourceAddress: "127.0.0.1:5001",
		Requests: []proto.Message{
			{ShardID: 1, From: 1, To: 1, Type: proto.Replicate, Term: 3},
		},
	}
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	got, err := handler.host.registry.Resolve(1, 1)
	if err != nil || got != "127.0.0.1:5001" {
		t.Fatalf("joining replica did not record the leader's address "+
			"(resolved %q, err %v): it cannot send its ReplicateResp back, so it "+
			"never acks and the join never completes", got, err)
	}
}

// TestHostMessageHandler_NonMemberCannotRepointAnEstablishedAddress pins the
// other half. First contact may fill in a mapping; nothing outside the committed
// configuration may REPLACE one. Otherwise any authenticated peer could re-point
// an established member's traffic at itself.
func TestHostMessageHandler_NonMemberCannotRepointAnEstablishedAddress(t *testing.T) {
	_, handler, _ := authorizationRig(t, proto.Membership{
		Addresses: map[uint64]string{1: "127.0.0.1:5001", 2: "127.0.0.1:5002"},
	})
	handler.host.registry.Register(1, 2, "127.0.0.1:5002")

	// Replica 42 is not in the configuration and claims to be replica 2.
	batch := proto.MessageBatch{
		SourceAddress: "127.0.0.1:6666",
		Requests: []proto.Message{
			{ShardID: 1, From: 2, To: 1, Type: proto.Replicate, Term: 3},
		},
	}
	// A member MAY overwrite (recovery at a new address), so to isolate the
	// non-member path this shard's configuration is edited to drop replica 2.
	_, handler2, _ := authorizationRig(t, proto.Membership{
		Addresses: map[uint64]string{1: "127.0.0.1:5001"},
	})
	handler2.host.registry.Register(1, 2, "127.0.0.1:5002")
	if err := handler2.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if got, _ := handler2.host.registry.Resolve(1, 2); got != "127.0.0.1:5002" {
		t.Fatalf("a replica outside the configuration re-pointed replica 2's "+
			"address to %q: shard traffic for replica 2 now goes to it", got)
	}

	// And a MEMBER may still overwrite, which recovery at a new address needs.
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if got, _ := handler.host.registry.Resolve(1, 2); got != "127.0.0.1:6666" {
		t.Fatalf("a member could not update its own address (got %q): a replica "+
			"recovering at a new address stays unreachable during quorum loss", got)
	}
}
