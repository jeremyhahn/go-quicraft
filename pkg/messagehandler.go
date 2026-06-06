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
//
// Address learning (return path) is defended against poisoning by an
// authenticated-but-malicious member through two layers:
//
//   - Authorization first: an inbound message's SourceAddress is only
//     learned for the (shardID, From) pair AFTER the per-shard membership
//     check passes. A sender that is not a member of a loaded shard never
//     gets an address recorded for any replicaID in that shard. Unloaded
//     shards do not learn addresses at all; authoritative addresses arrive
//     when the shard is loaded with its configured/bootstrap membership.
//   - Transport cert-binding: by the time a batch reaches this handler the
//     transport has already cleared any SourceAddress not covered by the
//     sender's verified TLS certificate (see handleMessageFrame), so a
//     learned address is always one the sender's certificate authorizes.
//
// The mapping IS updated (overwritten) for an authorized member — this is
// required so a node that recovers at a new address becomes reachable again,
// including during quorum loss when there is no leader to propagate the new
// address via a committed config change (see learnSourceAddress). The residual
// — a member asserting another replicaID's From together with its own
// cert-valid address — is a bounded, self-healing insider redirect (the victim
// and committed-membership config changes re-register the authoritative
// address); closing it fully would require replicaID-bound certificates.
func (h *hostMessageHandler) HandleMessage(batch proto.MessageBatch) error {
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

		// Learn the sender's address for the return path, but only for
		// senders that ARE authorized members of a loaded shard, and only
		// when doing so would not overwrite a differing existing mapping.
		// Unloaded shards (node == nil) do not learn an address: the
		// arbitrary replicaID in From cannot be verified as a real member
		// until the shard is loaded with its configured membership.
		if node != nil {
			h.learnSourceAddress(msg.ShardID, msg.From, batch.SourceAddress)
		}

		h.engine.DeliverMessage(msg.ShardID, *msg)
		h.engine.NotifyWork(msg.ShardID)
	}
	return nil
}

// learnSourceAddress records (and updates) the (shardID, replicaID) -> address
// return-path mapping for an authorized member.
//
// Overwrite is REQUIRED for liveness: a node that crashes and recovers at a
// NEW address (ephemeral ports, container reschedule) must become reachable
// again. When recovery happens during quorum loss there is no leader to
// propagate the new address via a committed config change, so peers can only
// relearn it from this node's own inbound traffic — e.g. its RequestVote must
// teach voters where to send VoteResp, or the cluster can never re-elect a
// leader. A no-overwrite policy here strands a recovered-at-new-address node
// and prevents quorum recovery.
//
// This is not an open injection vector: the caller only reaches here for a
// sender that is an authorized member of a loaded shard (IsMember), and the
// transport has already bound `address` to the sender's verified TLS
// certificate (handleMessageFrame clears any SourceAddress not covered by the
// peer cert's SANs). A member can therefore only set an address its own
// certificate authorizes. The residual — a member asserting another
// replicaID's From together with its own cert-valid address — is bounded by
// mTLS membership and would require replicaID-bound certificates to close
// fully; committed-membership config changes remain the authoritative source
// and overwrite the registry directly.
func (h *hostMessageHandler) learnSourceAddress(shardID, replicaID uint64, address string) {
	if address == "" || h.host == nil || h.host.registry == nil {
		return
	}
	h.host.registry.Register(shardID, replicaID, address)
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

	// Validate the chunk set BEFORE trusting chunks[0] for identity and
	// metadata. A reordered, gapped, duplicated, truncated, or
	// inconsistent chunk set must never be reassembled or persisted: doing
	// so would corrupt the on-disk snapshot and inject a bad
	// InstallSnapshot into raft. ordered holds the chunks indexed by
	// ChunkID so reassembly is independent of arrival order.
	ordered, err := validateSnapshotChunks(chunks)
	if err != nil {
		return err
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

	// Reassemble the snapshot data from chunks in ChunkID order.
	totalSize := uint64(0)
	for i := range ordered {
		totalSize += uint64(len(ordered[i].Data))
	}

	// If a declared total file size is present, the reassembled byte count
	// must match it exactly. A mismatch means a chunk was truncated or the
	// declared size was forged; either way the snapshot is unsafe to use.
	if first.FileSize != 0 && first.FileSize != totalSize {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "validate",
			Err: fmt.Errorf("reassembled size %d does not match declared FileSize %d",
				totalSize, first.FileSize),
		}
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

	for i := range ordered {
		if len(ordered[i].Data) > 0 {
			if _, writeErr := f.Write(ordered[i].Data); writeErr != nil {
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

// validateSnapshotChunks verifies that a received chunk set forms a
// complete, consistent, correctly-ordered snapshot before any of it is
// reassembled or persisted. The transport delivers chunks in arrival
// order, which a reordering or malicious peer can perturb, so identity
// and sequencing must be validated rather than trusted.
//
// The checks enforced are:
//   - Every chunk carries the same identity and metadata as chunks[0]:
//     ShardID, ReplicaID, From, Index, Term, Epoch, and OnDiskIndex.
//   - Every chunk's ChunkCount is identical and equals len(chunks).
//   - The set of ChunkIDs is exactly {0, 1, ..., N-1} with no gaps and
//     no duplicates.
//
// On success it returns the chunks indexed by ChunkID (ordered[i] is the
// chunk with ChunkID == i) so the caller reassembles data in sequence
// order, not arrival order. On any inconsistency it returns a
// *SnapshotReceiveError with Op == "validate" and no ordered slice.
func validateSnapshotChunks(chunks []proto.SnapshotChunk) ([]*proto.SnapshotChunk, error) {
	first := &chunks[0]
	n := uint64(len(chunks))

	validateErr := func(format string, args ...any) ([]*proto.SnapshotChunk, error) {
		return nil, &SnapshotReceiveError{
			ShardID:   first.ShardID,
			ReplicaID: first.ReplicaID,
			Op:        "validate",
			Err:       fmt.Errorf(format, args...),
		}
	}

	ordered := make([]*proto.SnapshotChunk, n)
	for i := range chunks {
		c := &chunks[i]

		if c.ShardID != first.ShardID ||
			c.ReplicaID != first.ReplicaID ||
			c.From != first.From ||
			c.Index != first.Index ||
			c.Term != first.Term ||
			c.Epoch != first.Epoch ||
			c.OnDiskIndex != first.OnDiskIndex {
			return validateErr(
				"chunk %d identity mismatch with chunk 0 "+
					"(shard %d/%d replica %d/%d from %d/%d index %d/%d term %d/%d epoch %d/%d ondisk %d/%d)",
				c.ChunkID,
				c.ShardID, first.ShardID,
				c.ReplicaID, first.ReplicaID,
				c.From, first.From,
				c.Index, first.Index,
				c.Term, first.Term,
				c.Epoch, first.Epoch,
				c.OnDiskIndex, first.OnDiskIndex,
			)
		}

		if c.ChunkCount != n {
			return validateErr(
				"chunk %d declares ChunkCount %d but %d chunks were received",
				c.ChunkID, c.ChunkCount, n)
		}

		if c.ChunkID >= n {
			return validateErr(
				"chunk ID %d out of range for %d chunks", c.ChunkID, n)
		}
		if ordered[c.ChunkID] != nil {
			return validateErr("duplicate chunk ID %d", c.ChunkID)
		}
		ordered[c.ChunkID] = c
	}

	// Every slot must be filled; a nil slot means a missing ChunkID (gap).
	for id := range ordered {
		if ordered[id] == nil {
			return validateErr("missing chunk ID %d of %d", id, n)
		}
	}

	return ordered, nil
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
