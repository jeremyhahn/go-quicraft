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
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// hostMessageHandler routes inbound Raft messages from the transport
// layer to the engine for processing. It implements the
// transport.MessageHandler interface.
type hostMessageHandler struct {
	engine *engine.Engine
	host   *Host
}

// HandleMessage delivers each message in the batch to the engine's
// per-shard inbox and signals the step worker that work is available.
//
// Per-shard authorization is enforced: when a shard is loaded, the
// sender's replicaID (msg.From) is checked against the shard's
// membership. Messages from non-members are silently dropped with a
// warning log. This prevents compromised nodes from injecting messages
// into shards they do not belong to.
//
// Messages to unloaded shards are delivered without membership checks.
// This allows bootstrap messages and messages that trigger shard loading
// to flow through. The shard's raft state machine provides its own
// term-based and log-based rejection for unauthorized messages once
// the shard is loaded.
func (h *hostMessageHandler) HandleMessage(batch proto.MessageBatch) error {
	// Auto-register sender address for return path. When a node joins
	// a cluster via StartShard(join=true), its registry is empty — it
	// has no knowledge of other nodes' addresses. Without address
	// learning, the joining node cannot send responses (ReplicateResp,
	// HeartbeatResp, VoteResp) because the transport cannot resolve the
	// destination. By learning addresses from incoming messages, the
	// joining node bootstraps its registry naturally from traffic.
	//
	// This is safe because:
	// - The transport layer already validated mTLS and deployment ID
	// - Registry.Register is idempotent (overwrites silently)
	// - sync.Map.Swap is lock-free on the read path
	if batch.SourceAddress != "" {
		for i := range batch.Requests {
			h.host.registry.Register(
				batch.Requests[i].ShardID,
				batch.Requests[i].From,
				batch.SourceAddress,
			)
		}
	}

	for i := range batch.Requests {
		msg := &batch.Requests[i]

		// Per-shard authorization: verify the sender is a member of
		// the target shard. When node is nil (shard not loaded locally),
		// the message is allowed through because it may be a bootstrap
		// or shard-loading trigger message.
		node := h.engine.GetNode(msg.ShardID)
		if node != nil && !node.IsMember(msg.From) {
			slog.Warn("rejected unauthorized message",
				"shard", msg.ShardID,
				"from", msg.From,
				"type", proto.MessageTypeName(msg.Type),
			)
			continue
		}

		h.engine.DeliverMessage(msg.ShardID, *msg)
		h.engine.NotifyWork(msg.ShardID)
	}
	return nil
}

// HandleSnapshot processes received snapshot chunks from the transport
// layer. It reassembles the snapshot data, writes it to disk, saves
// metadata to LogDB, delivers an InstallSnapshot to the local raft
// layer (so it updates log state and membership), and triggers
// snapshot recovery on the state machine.
//
// The sender side (transportSender.doSendSnapshot) delivers a
// SnapshotStatus to the leader's raft layer when the transfer
// completes so the leader transitions the remote out of snapshot state.
//
// This method is called by the transport's snapshotReceiver after all
// chunks for a snapshot have been received and validated.
//
//nolint:gocyclo // snapshot receive handles validation, directory creation, assembly, and cleanup
func (h *hostMessageHandler) HandleSnapshot(chunks []proto.SnapshotChunk) error {
	if len(chunks) == 0 {
		return nil
	}

	// Guard against nil host. This can happen in unit tests or when the
	// handler is not fully wired.
	if h.host == nil {
		return nil
	}

	first := &chunks[0]
	shardID := first.ShardID
	replicaID := first.ReplicaID
	fromReplicaID := first.From
	snapshotIndex := first.Index
	snapshotTerm := first.Term

	// Dedup: skip if LogDB already has a snapshot at this index or later.
	// Without this check, a duplicate snapshot delivery can overwrite the
	// on-disk snapshot file while an async recovery is reading it, then
	// delete the directory on the "out of date" error path.
	if h.host.logdb != nil {
		existing, err := h.host.logdb.GetSnapshot(shardID, replicaID)
		if err == nil && existing.Index >= snapshotIndex {
			slog.Debug("snapshot already applied, skipping duplicate",
				"shard", shardID,
				"replica", replicaID,
				"existing_index", existing.Index,
				"received_index", snapshotIndex,
			)
			return nil
		}
	}

	// Per-shard authorization: verify the snapshot sender is a member of
	// the target shard. Snapshots are only sent by the leader, which must
	// be a member. A compromised node could attempt to send a crafted
	// snapshot to corrupt a shard it does not belong to.
	if h.engine != nil {
		node := h.engine.GetNode(shardID)
		if node != nil && !node.IsMember(fromReplicaID) {
			slog.Warn("rejected unauthorized snapshot",
				"shard", shardID,
				"from", fromReplicaID,
				"index", snapshotIndex,
			)
			return &UnauthorizedMessageError{
				ShardID: shardID,
				From:    fromReplicaID,
				MsgType: proto.InstallSnapshot,
			}
		}
	}

	// Reassemble the snapshot data from chunks.
	totalSize := uint64(0)
	for i := range chunks {
		totalSize += uint64(len(chunks[i].Data))
	}

	// Determine the snapshot directory. Use the host's configured snapshot
	// directory structure: <NodeHostDir>/snapshots/shard-<N>/replica-<N>/snapshot-<index>
	snapshotDir := filepath.Join(
		h.host.cfg.NodeHostDir, "snapshots",
		fmt.Sprintf("shard-%d", shardID),
		fmt.Sprintf("replica-%d", replicaID),
		fmt.Sprintf("snapshot-%020d", snapshotIndex),
	)

	if err := os.MkdirAll(snapshotDir, 0o750); err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "mkdir",
			Err:       err,
		}
	}

	// Clean up partial snapshot directory on error. The committed flag
	// is set at the end of the successful path so that only failed
	// attempts leave orphaned directories.
	var committed bool
	defer func() {
		if !committed {
			if err := os.RemoveAll(snapshotDir); err != nil {
				slog.Debug("snapshot directory cleanup failed",
					"shard", shardID,
					"replica", replicaID,
					"dir", snapshotDir,
					"error", err)
			}
		}
	}()

	// Write the reassembled snapshot data to disk.
	dataPath := filepath.Join(snapshotDir, "snapshot.dat")
	f, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "create",
			Err:       err,
		}
	}

	for i := range chunks {
		if len(chunks[i].Data) > 0 {
			if _, writeErr := f.Write(chunks[i].Data); writeErr != nil {
				if closeErr := f.Close(); closeErr != nil {
					slog.Debug("snapshot file close failed after write error",
						"shard", shardID,
						"replica", replicaID,
						"error", closeErr)
				}
				return &SnapshotReceiveError{
					ShardID:   shardID,
					ReplicaID: replicaID,
					Op:        "write",
					Err:       writeErr,
				}
			}
		}
	}

	if syncErr := f.Sync(); syncErr != nil {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot file close failed after sync error",
				"shard", shardID,
				"replica", replicaID,
				"error", closeErr)
		}
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "sync",
			Err:       syncErr,
		}
	}
	if closeErr := f.Close(); closeErr != nil {
		slog.Debug("snapshot file close failed",
			"shard", shardID,
			"replica", replicaID,
			"error", closeErr)
	}

	// Write snapshot metadata file.
	if err := writeReceivedSnapshotMetadata(snapshotDir, snapshotIndex, snapshotTerm, shardID, replicaID); err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "metadata",
			Err:       err,
		}
	}

	// Save snapshot metadata to LogDB so the node can recover from it.
	if h.host.logdb != nil {
		membership := logdb.Membership{}
		if len(first.Membership.Addresses) > 0 || first.Membership.ConfigChangeID > 0 {
			membership = logdb.Membership{
				ConfigChangeID: first.Membership.ConfigChangeID,
				Addresses:      copyMapUint64String(first.Membership.Addresses),
				Observers:      copyMapUint64String(first.Membership.Observers),
				Witnesses:      copyMapUint64String(first.Membership.Witnesses),
				Removed:        copyMapUint64Bool(first.Membership.Removed),
			}
		}
		logdbSnap := logdb.Snapshot{
			Index:       snapshotIndex,
			Term:        snapshotTerm,
			Membership:  membership,
			Filepath:    snapshotDir,
			OnDiskIndex: first.OnDiskIndex,
			Epoch:       first.Epoch,
		}
		if saveErr := h.host.logdb.SaveSnapshot(shardID, replicaID, logdbSnap); saveErr != nil {
			return &SnapshotReceiveError{
				ShardID:   shardID,
				ReplicaID: replicaID,
				Op:        "logdb_save",
				Err:       saveErr,
			}
		}
	}

	// Deliver the InstallSnapshot to the local raft layer so it updates
	// its log state (committed index, applied index, membership). Without
	// this, the raft layer would still think it's behind and never accept
	// new Replicate messages that follow the snapshot.
	installMsg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: shardID,
		From:    fromReplicaID,
		To:      replicaID,
		Snapshot: proto.Snapshot{
			ShardID:    shardID,
			ReplicaID:  replicaID,
			Index:      snapshotIndex,
			Term:       snapshotTerm,
			Filepath:   filepath.Join(snapshotDir, "snapshot.dat"),
			FileSize:   totalSize,
			Membership: first.Membership,
			Epoch:      first.Epoch,
		},
	}
	delivered := h.engine.DeliverMessage(shardID, installMsg)
	if delivered {
		h.engine.NotifyWork(shardID)
	}

	// Deliver a SnapshotReceived message to the local raft peer so
	// it rebuilds its remote tracking maps from the snapshot's
	// membership. Without this, the raft layer may have stale remote
	// state after restoring from the snapshot.
	recvMsg := proto.Message{
		Type:    proto.SnapshotReceived,
		ShardID: shardID,
		From:    fromReplicaID,
		To:      replicaID,
		Snapshot: proto.Snapshot{
			ShardID:    shardID,
			ReplicaID:  replicaID,
			Index:      snapshotIndex,
			Term:       snapshotTerm,
			Membership: first.Membership,
		},
	}
	if h.engine.DeliverMessage(shardID, recvMsg) {
		h.engine.NotifyWork(shardID)
	}

	// Trigger snapshot recovery on the engine node. The engine's
	// snapshot pool handles the actual SM.RecoverFromSnapshot call.
	engNode := h.engine.GetNode(shardID)
	if engNode != nil {
		started := engNode.TryStartSnapshot()
		if started {
			if err := h.engine.RequestSnapshotRecovery(shardID, replicaID, engNode); err != nil {
				engNode.ClearSnapshotting()
				slog.Warn("snapshot recovery request failed",
					"shard", shardID,
					"replica", replicaID,
					"error", err,
				)
			}
		}
	}

	// Note: The sender side (transportSender.doSendSnapshot) already delivers
	// a SnapshotStatus message to the leader's raft layer when the snapshot
	// transfer completes. We do NOT deliver SnapshotStatus here because this
	// handler runs on the RECEIVER host, and the SnapshotStatus must reach
	// the LEADER (on the sender host) to transition the remote out of
	// snapshot state.

	committed = true
	return nil
}

// snapshotRecvMetaSize is the metadata file size for received snapshots.
// Format: Index(8) | Term(8) | ShardID(8) | ReplicaID(8) | ReceivedAt(8)
const snapshotRecvMetaSize = 5 * 8

// writeReceivedSnapshotMetadata writes a binary metadata file for a
// received snapshot.
func writeReceivedSnapshotMetadata(dir string, index, term, shardID, replicaID uint64) error {
	metaPath := filepath.Join(dir, "snapshot.meta")
	f, err := os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot metadata file close failed",
				"shard", shardID,
				"replica", replicaID,
				"path", metaPath,
				"error", closeErr)
		}
	}()

	var buf [snapshotRecvMetaSize]byte
	binary.LittleEndian.PutUint64(buf[0:], index)
	binary.LittleEndian.PutUint64(buf[8:], term)
	binary.LittleEndian.PutUint64(buf[16:], shardID)
	binary.LittleEndian.PutUint64(buf[24:], replicaID)
	binary.LittleEndian.PutUint64(buf[32:], uint64(time.Now().Unix()))

	if _, writeErr := f.Write(buf[:]); writeErr != nil {
		return writeErr
	}
	return f.Sync()
}

// copyMapUint64String deep-copies a map[uint64]string.
func copyMapUint64String(m map[uint64]string) map[uint64]string {
	if len(m) == 0 {
		return nil
	}
	cp := make(map[uint64]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// copyMapUint64Bool deep-copies a map[uint64]bool.
func copyMapUint64Bool(m map[uint64]bool) map[uint64]bool {
	if len(m) == 0 {
		return nil
	}
	cp := make(map[uint64]bool, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
