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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// hostMessageHandler routes inbound Raft messages from the transport
// layer to the engine for processing. It implements the
// transport.MessageHandler interface.
// engineDeliverer is the narrow engine surface the host message handler depends
// on. Depending on this interface rather than the concrete *engine.Engine keeps
// the inbound message/snapshot routing unit-testable with a capturing fake
// (e.g. to assert the at-rest epoch threaded into a delivered InstallSnapshot).
// *engine.Engine satisfies it.
type engineDeliverer interface {
	DeliverMessage(shardID uint64, msg proto.Message) bool
	DeliverLocal(shardID uint64, msg proto.Message) bool
	NotifyWork(shardID uint64)
	GetNode(shardID uint64) *engine.Node
	RequestSnapshotRecovery(shardID, replicaID uint64, node *engine.Node) error
}

type hostMessageHandler struct {
	engine engineDeliverer
	host   *Host

	// inflightSnapshotRecv serializes concurrent HandleSnapshot calls for the
	// same (shard, replica, index). The snapshot directory is deterministic
	// per index and the dedup guard only consults LogDB — which is written
	// LAST — so without this claim two concurrent receives of the same index
	// would both O_TRUNC and interleave writes into the same snapshot.dat,
	// and the loser's cleanup would RemoveAll a directory the winner may
	// already have recorded in LogDB. Keys are claimed with LoadOrStore at
	// entry and released at exit; the duplicate takes the same early-return
	// path as the LogDB dedup.
	inflightSnapshotRecv sync.Map
}

// snapshotRecvKey identifies one in-flight snapshot receive by target
// replica and snapshot index.
type snapshotRecvKey struct {
	shardID   uint64
	replicaID uint64
	index     uint64
}

// snapshotCtrlDeliverer is the minimal engine surface needed to deliver
// one-shot snapshot control messages with retry. It deliberately names
// DeliverLocal and not DeliverMessage: these messages are built in-process
// and carry no term, so they must reach the inbox stamped OriginLocal —
// the stamp raft.step's term-gate exemption is keyed on.
type snapshotCtrlDeliverer interface {
	DeliverLocal(shardID uint64, msg proto.Message) bool
	NotifyWork(shardID uint64)
}

const (
	// snapshotCtrlDeliverAttempts bounds delivery retries for one-shot
	// snapshot control messages.
	snapshotCtrlDeliverAttempts = 5
	// snapshotCtrlDeliverBackoff is the pause between delivery retries.
	snapshotCtrlDeliverBackoff = 100 * time.Millisecond
)

// deliverSnapshotControl delivers a one-shot snapshot control message
// (SnapshotStatus, or a re-delivered InstallSnapshot) to the shard inbox,
// retrying briefly when the inbox is transiently full. Unlike regular raft
// traffic, these messages have no protocol-level retransmission: dropping a
// status leaves the leader's remote paused in snapshot state with no ack
// countdown armed, and dropping an install leaves the follower's raft log
// behind a snapshot it already persisted. Callers run on snapshot/transport
// goroutines, never on a step worker, so the bounded sleep is safe. Returns
// false when delivery still failed after all attempts (shard unloaded, or a
// persistently saturated inbox).
func deliverSnapshotControl(eng snapshotCtrlDeliverer, shardID uint64, msg proto.Message) bool {
	for attempt := 0; ; attempt++ {
		if eng.DeliverLocal(shardID, msg) {
			eng.NotifyWork(shardID)
			return true
		}
		if attempt >= snapshotCtrlDeliverAttempts-1 {
			return false
		}
		time.Sleep(snapshotCtrlDeliverBackoff)
	}
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
func (h *hostMessageHandler) HandleMessage(batch proto.MessageBatch, senderAuthorized func(address string) bool) error {
	// A nil callback is treated as unauthorized: never learn an address that
	// cannot be bound to the sender's certificate.
	if senderAuthorized == nil {
		senderAuthorized = func(string) bool { return false }
	}
	for i := range batch.Requests {
		msg := &batch.Requests[i]

		// Per-shard authorization: refuse senders this shard has COMMITTED a
		// removal for. This is deliberately a deny-list, not a member
		// allow-list — a replica's configuration lags by design, so
		// allow-listing makes a stale replica drop the Replicate, Heartbeat and
		// InstallSnapshot from a leader added while it was behind, permanently
		// wedging itself out of the shard. The removed set is monotone and is
		// the only membership-derived fact that is safe to enforce here; see
		// raft.Peer.IsRemoved. Authentication proper is the transport's mTLS
		// node certificate plus the deployment ID.
		//
		// When node is nil (shard not loaded locally) the message is allowed
		// through because it may be a bootstrap or shard-loading trigger.
		node := h.engine.GetNode(msg.ShardID)
		if node != nil && node.IsRemoved(msg.From) {
			slog.Warn("rejected message from removed replica",
				"shard", msg.ShardID,
				"from", msg.From,
				"type", proto.MessageTypeName(msg.Type),
			)
			if h.host != nil && h.host.metrics != nil {
				h.host.metrics.IncUnauthorizedMessageDropped(msg.ShardID, msg.From)
			}
			continue
		}

		// Destination check: a message addressed to a different replica of
		// this shard must be dropped, not stepped into the local raft state.
		// Neither engine.DeliverMessage nor raft.step compares msg.To against
		// the local replica ID, so misrouted or stale traffic (e.g. a
		// replica ID reused after removal, or a transport-level routing
		// error) would otherwise be processed as if it were addressed here.
		// To == 0 is allowed: broadcast/local control messages leave it unset.
		if node != nil && msg.To != 0 && msg.To != node.ReplicaID() {
			slog.Warn("rejected misrouted message",
				"shard", msg.ShardID,
				"from", msg.From,
				"to", msg.To,
				"local_replica", node.ReplicaID(),
				"type", proto.MessageTypeName(msg.Type),
			)
			if h.host != nil && h.host.metrics != nil {
				h.host.metrics.IncUnauthorizedMessageDropped(msg.ShardID, msg.From)
			}
			continue
		}

		// Learn the sender's address for the return path, but only for
		// senders that ARE authorized members of a loaded shard, AND only
		// when the SourceAddress is bound to the sender's verified certificate.
		// Binding it here at the handler — rather than trusting the transport
		// to have cleared a non-cert-valid SourceAddress — makes the handler
		// the authoritative enforcement point, so a transport regression cannot
		// let a peer teach the registry an address its cert does not cover.
		// Unloaded shards (node == nil) do not learn an address: the arbitrary
		// replicaID in From cannot be verified as a real member until the shard
		// is loaded with its configured membership.
		//
		// Learning is gated on IsMember, NOT on the deny-list that gates message
		// ACCEPTANCE above. The two are deliberately different predicates:
		// accepting a message is a read that raft's own term and log rules then
		// validate, and it must tolerate a stale configuration or a lagging
		// replica wedges itself. Learning an address MUTATES this host's routing
		// state, so it takes the strict test — only a replica the committed
		// configuration actually names may rewrite where this host sends. A
		// sender we have never heard of has nothing to route to anyway.
		if node != nil && node.IsMember(msg.From) &&
			batch.SourceAddress != "" && senderAuthorized(batch.SourceAddress) {
			h.learnSourceAddress(msg.ShardID, msg.From, batch.SourceAddress)
		}

		// DeliverMessage (not DeliverLocal) stamps the message OriginRemote.
		// The shard inbox is shared with the in-process injectors — the
		// snapshot receive path's InstallSnapshot, the transport's
		// SnapshotStatus, the host's LeaderTransfer — and raft.step exempts
		// exactly those types from its term gate. The stamp is what keeps the
		// exemption unreachable from here: without it a peer could name one of
		// those types in a MessageBatch and be stepped as if this host had
		// injected it (a term-less InstallSnapshot then rewrites membership).
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
func (h *hostMessageHandler) HandleSnapshot(chunks []proto.SnapshotChunk, senderAuthorized func(address string) bool) error {
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

	// Per-shard authorization, fail closed. All chunk fields (including
	// From) travel in-band and are NOT bound to the sender's mTLS identity
	// by the transport framing, so three independent checks are required
	// before anything is buffered or persisted:
	//
	//  1. The target shard must be loaded locally. Without a loaded node
	//     there is no membership to authorize against; persisting the
	//     snapshot anyway would let any authenticated peer poison the
	//     LogDB state consumed by a later StartShard.
	//  2. The claimed sender must be a member of the shard (snapshots are
	//     only ever sent by the leader, which is always a member).
	//  3. The claimed From identity must be bound to the verified TLS
	//     certificate of the connection the snapshot arrived on: the
	//     registered address for (shardID, From) must be covered by the
	//     peer certificate (same mechanism as the SourceAddress binding
	//     in the message path). This stops an authenticated-but-
	//     compromised node from forging another replica's identity.
	reject := func(reason string) error {
		slog.Warn("rejected unauthorized snapshot",
			"shard", shardID,
			"from", fromReplicaID,
			"index", snapshotIndex,
			"reason", reason,
		)
		if h.host.metrics != nil {
			h.host.metrics.IncUnauthorizedSnapshotDropped(shardID, fromReplicaID)
		}
		return &UnauthorizedMessageError{
			ShardID: shardID,
			From:    fromReplicaID,
			MsgType: proto.InstallSnapshot,
		}
	}
	if h.engine == nil {
		return reject("no engine")
	}
	node := h.engine.GetNode(shardID)
	if node == nil {
		return reject("shard not loaded")
	}
	// Destination check, mirroring the msg.To check in HandleMessage. The
	// target ReplicaID travels in-band and is otherwise trusted verbatim,
	// yet everything downstream keys off it: a foreign value splits the
	// snapshotRecvKey so the inflight serialization guard is bypassed while
	// snapshotDir still uses the LOCAL replica ID (two concurrent receives
	// O_TRUNC and interleave into the same snapshot.dat), defeats the
	// LogDB dedup lookup, writes a durable LogDB record under a replica ID
	// this host does not own, and reaches raft through DeliverMessage,
	// which performs no destination check of its own.
	if replicaID != node.ReplicaID() {
		return reject("snapshot addressed to a different replica")
	}
	// Deny-list, not an allow-list: see raft.Peer.IsRemoved. A replica that has
	// not yet applied the config change adding the sender must still be able to
	// receive the snapshot that brings it up to date.
	if node.IsRemoved(fromReplicaID) {
		return reject("sender removed from the shard")
	}
	if h.host.registry == nil {
		return reject("no registry to resolve sender address")
	}
	senderAddr, err := h.host.registry.Resolve(shardID, fromReplicaID)
	if err != nil || senderAddr == "" {
		// A shard started with join=true and empty initial members registers
		// no membership and no addresses: the leader's bootstrap snapshot is
		// the only thing that can deliver them, yet it is the very message
		// being authorized. Take the sender's address from the membership the
		// snapshot itself carries and require the connection's VERIFIED peer
		// certificate to cover it — the authorization is still bound to the
		// authenticated QUIC peer, only the source of the address changes. A
		// non-joining shard keeps the registry requirement unconditionally.
		claimed := first.Membership.Addresses[fromReplicaID]
		if !h.host.isShardJoining(shardID) || claimed == "" {
			return reject("sender address unknown")
		}
		senderAddr = claimed
	}
	if senderAuthorized == nil || !senderAuthorized(senderAddr) {
		return reject("sender identity not covered by peer certificate")
	}

	// Serialize concurrent receives of the same snapshot. snapshotDir is
	// deterministic per index and the LogDB dedup below only sees COMPLETED
	// receives (LogDB is written last), so two concurrent deliveries of the
	// same index would otherwise both O_TRUNC and interleave writes into the
	// same snapshot.dat, and the loser's !committed cleanup would RemoveAll a
	// directory the winner may already have recorded in LogDB. The duplicate
	// takes the same early-return path as the dedup: drop it and rely on the
	// leader's ack-timeout re-send, which lands after the winner finishes and
	// is then answered from LogDB by the dedup branch.
	recvKey := snapshotRecvKey{shardID: shardID, replicaID: replicaID, index: snapshotIndex}
	if _, inflight := h.inflightSnapshotRecv.LoadOrStore(recvKey, struct{}{}); inflight {
		slog.Debug("concurrent receive of same snapshot in flight, dropping duplicate",
			"shard", shardID,
			"replica", replicaID,
			"index", snapshotIndex,
		)
		return nil
	}
	defer h.inflightSnapshotRecv.Delete(recvKey)

	// Dedup: skip persistence if LogDB already has a snapshot at this index
	// or later. Without this check, a duplicate snapshot delivery can
	// overwrite the on-disk snapshot file while an async recovery is reading
	// it, then delete the directory on the "out of date" error path.
	//
	// Persistence is skipped, but the InstallSnapshot raft message is
	// RE-DELIVERED from the persisted snapshot: the original delivery is
	// best-effort and can be dropped on a full inbox, in which case the
	// follower's raft log stays behind a snapshot it durably holds. The
	// leader's retransmitted snapshot lands here — swallowing it would
	// neuter retransmission and livelock the catch-up (the leader re-streams,
	// this dedup discards, forever). handleSnapshot is idempotent, so a
	// duplicate install for an already-applied snapshot just re-sends the
	// ReplicateResp ack the leader is waiting for.
	if h.host.logdb != nil {
		existing, err := h.host.logdb.GetSnapshot(shardID, replicaID)
		// Fail closed when the lookup itself fails. Only ErrNoSnapshot means
		// "nothing recorded"; any other error leaves it UNKNOWN whether LogDB
		// already durably references this index. The snapshot directory is a
		// pure function of the index and the failure path below removes it
		// wholesale, so proceeding on an unknown would let a failed receive
		// delete a committed snapshot. The leader re-sends after the ack
		// timeout, and the dedup answers it once LogDB reads again.
		if err != nil && !errors.Is(err, logdb.ErrNoSnapshot) {
			return &SnapshotReceiveError{
				ShardID:   shardID,
				ReplicaID: replicaID,
				Op:        "logdb_read",
				Err:       err,
			}
		}
		if err == nil && existing.Index >= snapshotIndex {
			slog.Debug("snapshot already persisted, re-delivering install without re-writing",
				"shard", shardID,
				"replica", replicaID,
				"existing_index", existing.Index,
				"received_index", snapshotIndex,
			)
			// Measure the artifact instead of trusting existing.FileSize, and
			// point at snapshot.dat rather than at the directory LogDB stores.
			// The re-delivered install reaches the LogReader, and this replica
			// streams FROM that record once it becomes leader: a (non-empty
			// Filepath, zero FileSize) pair there makes it ship a metadata-only
			// snapshot, erasing the receiving follower's state. If the artifact
			// cannot be measured its size is unknown, so refuse to re-deliver
			// and let the leader re-send.
			existingDataPath := filepath.Join(existing.Filepath, "snapshot.dat")
			fi, statErr := h.host.snapshotFsys().Stat(existingDataPath)
			if statErr != nil {
				return &SnapshotReceiveError{
					ShardID:   shardID,
					ReplicaID: replicaID,
					Op:        "stat_persisted",
					Err:       statErr,
				}
			}
			installMsg := proto.Message{
				Type:    proto.InstallSnapshot,
				ShardID: shardID,
				From:    fromReplicaID,
				To:      replicaID,
				Snapshot: proto.Snapshot{
					ShardID:    shardID,
					ReplicaID:  replicaID,
					Index:      existing.Index,
					Term:       existing.Term,
					Filepath:   existingDataPath,
					FileSize:   uint64(fi.Size()),
					Membership: logdbMembershipToProto(existing.Membership),
					Epoch:      existing.Epoch,
				},
			}
			if !deliverSnapshotControl(h.engine, shardID, installMsg) {
				slog.Warn("failed to re-deliver InstallSnapshot for persisted snapshot",
					"shard", shardID,
					"replica", replicaID,
					"index", existing.Index,
				)
			}
			// Re-arm state-machine recovery when the persisted snapshot
			// is ahead of the applied state. This makes the leader's
			// ack-timeout re-send self-healing for the case where the
			// original delivery persisted and installed the snapshot but
			// recovery could not be queued: without this, the dedup path
			// would swallow every retransmission and the state machine
			// would stay behind the snapshot forever.
			if existing.Index > node.LastApplied() {
				if err := h.requestRecoveryWithRetry(shardID, replicaID, node); err != nil {
					return err
				}
			}
			return nil
		}
	}

	// Resolve the receiver's at-rest barrier BEFORE any directory, data file or
	// LogDB record exists. The wire bytes are plaintext (the sender decrypts
	// before streaming), so every way of not having a usable barrier persists
	// them as a plaintext state machine at rest:
	//
	//   - CONFIGURED but SEALED: encryption is temporarily unavailable for this
	//     shard, NOT a plaintext deployment.
	//   - CONFIGURED per-shard but UNRESOLVED with no host-wide fallback: this
	//     tenant alone would be downgraded, invisibly.
	//
	// A shard with NO barrier configured at all is unaffected: it resolves to
	// (nil, nil) and stays plaintext as configured. reencryptReceivedSnapshot
	// re-checks the seal below and covers one that lands mid-receive.
	recvBarrier, barrierErr := h.host.snapshotBarrierForShard(shardID)
	if barrierErr != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "reencrypt",
			Err:       barrierErr,
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

	// Determine the snapshot directory via the engine node, so a received
	// snapshot lands in the SAME location the snapshot pool uses for saving,
	// garbage collection, and DEK-purge-floor scans. Using a hardcoded
	// <NodeHostDir>/snapshots/... path here instead would, for a per-shard
	// DataDir (which roots SnapshotDir() off the DataDir), write received
	// snapshots where GC and key-rotation purge never look — leaking them and
	// letting a purge remove a DEK the snapshot still needs.
	snapshotDir := node.SnapshotSaveDir(snapshotIndex)

	fsys := h.host.snapshotFsys()

	if err := fsys.MkdirAll(snapshotDir, 0o750); err != nil {
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
			if err := fsys.RemoveAll(snapshotDir); err != nil {
				slog.Debug("snapshot directory cleanup failed",
					"shard", shardID,
					"replica", replicaID,
					"dir", snapshotDir,
					"error", err)
			}
		}
	}()

	// Classify the artifact from the reassembled PLAINTEXT bytes, before a
	// single byte is written. The compression is a property of what the LEADER
	// wrote — the wire carries no compression field, and the receiver's own
	// SnapshotCompressionType describes nothing about it — so recovery must take
	// it from the artifact. Reading it out of the chunks rather than back off
	// the file is what lets the write below be encrypted from the start.
	recvCompression := classifySnapshotCompression(snapshotChunkPrefix(ordered))

	// Write the reassembled snapshot data, ENCRYPTED UNDER THE RECEIVER'S OWN
	// BARRIER AS IT IS WRITTEN.
	//
	// The sender decrypts before streaming (doSendSnapshot sets Epoch=0), so the
	// bytes in hand are plaintext and this is what closes the follower-side
	// at-rest gap — with no shared cross-node key, since the receiver records
	// and later recovers under its own epoch.
	//
	// It encrypts INLINE, straight from the in-memory chunks, rather than
	// writing the plaintext to snapshot.dat and re-encrypting it afterwards.
	// The chunks are already fully reassembled in memory (the transport's
	// receive budget bounds them), so streaming them through the barrier costs
	// nothing and removes the window the two-step form could not close: a
	// process killed between the plaintext fsync and the re-encryption left a
	// COMPLETE PLAINTEXT STATE MACHINE durably under the canonical
	// snapshot-<index> name. The deferred RemoveAll below covers error returns,
	// not a kill, and snapshot GC only reclaims a directory once
	// maxSnapshotsRetained NEWER snapshots exist — on a follower that catches up
	// once and never snapshots again, never.
	//
	// A nil barrier is the configured plaintext deployment and is copied
	// through unchanged (recvEpoch stays 0). A barrier sealed between the
	// resolution above and here fails inside EncryptWithEpoch, so the failure
	// still arrives with no plaintext artifact in existence.
	dataPath := filepath.Join(snapshotDir, "snapshot.dat")
	f, err := fsys.OpenFile(dataPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "create",
			Err:       err,
		}
	}

	var recvEpoch uint64
	var writeErr error
	plain := &snapshotChunkReader{chunks: ordered}
	if recvBarrier != nil {
		// Pin the at-rest epoch across the write, the metadata file and the
		// LogDB record — the whole window in which this artifact's epoch is
		// durable but INVISIBLE to the purge-floor scan, which reads it out of
		// snapshot.meta.
		//
		// The transport pins hdr.Epoch on the way in, but that protects nothing
		// here: on the receive path hdr.Epoch is ALWAYS 0 because the sender
		// decrypts before streaming. The epoch this node actually seals under is
		// chosen right here, and a Rotate plus a concurrent purgeOldEpochs in
		// this window would drop its DEK while SaveSnapshot goes on to record
		// Epoch: E — durably naming ciphertext nothing can ever open.
		//
		// encryptSnapshotFrames pins EVERY epoch it seals a frame under — before
		// the seal (so the current epoch cannot be purged between the read and the
		// first frame) and after (so an intermediate epoch a mid-write rotation
		// creates is protected too) — and returns the set. Hold those pins until
		// this handler returns, which is past the SaveSnapshot that records the
		// epoch, so a concurrent rotate+purge cannot orphan any frame's DEK.
		var recvPins []uint64
		recvEpoch, recvPins, writeErr = encryptSnapshotFrames(recvBarrier, plain, f)
		defer func() {
			for _, e := range recvPins {
				recvBarrier.DeregisterInFlightEpoch(e)
			}
		}()
	} else {
		_, writeErr = io.Copy(f, plain)
	}
	if writeErr != nil {
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

	// Measure the artifact AFTER re-encryption: FileSize must always mean
	// "size of the file at Filepath", never the plaintext wire size. A zero
	// FileSize on a record with a non-empty Filepath makes this replica
	// stream an EMPTY snapshot once it becomes leader, silently erasing the
	// receiving follower's state.
	onDiskInfo, statErr := fsys.Stat(dataPath)
	if statErr != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "stat",
			Err:       statErr,
		}
	}
	onDiskSize := uint64(onDiskInfo.Size())

	// Write snapshot metadata file. It records the RECEIVER's own at-rest epoch
	// and the classified compression, so the directory describes itself even
	// without its LogDB record — and so the DEK purge floor, which is computed
	// by scanning these directories, sees the epoch this artifact needs.
	if err := writeReceivedSnapshotMetadata(fsys, snapshotDir, snapshotIndex, snapshotTerm,
		shardID, replicaID, recvEpoch, recvCompression); err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "metadata",
			Err:       err,
		}
	}

	// Make the directory entries durable BEFORE recording the snapshot in
	// LogDB: the data and metadata files are fsynced, but the entries
	// created by MkdirAll and the file creates are not durable until each
	// directory in the chain is fsynced. Otherwise a crash could leave
	// LogDB's durably-recorded snapshot pointing at a directory that no
	// longer exists.
	// Sync the directory chain up to the node's snapshot base (matching the
	// snapshot pool's save path). The base — NOT NodeHostDir — is the correct
	// root: for a DataDir shard NodeHostDir is not even an ancestor of snapshotDir.
	if err := syncDirChain(fsys, snapshotDir, node.SnapshotDir()); err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "sync_dir",
			Err:       err,
		}
	}

	// Save snapshot metadata to LogDB so the node can recover from it.
	if h.host.logdb != nil {
		// Carry the full membership unconditionally, including the
		// replicated witness policy: gating on Addresses/ConfigChangeID
		// would discard a policy-only membership, and dropping the policy
		// fields would persist this snapshot with a zeroed witness quorum
		// that disagrees with every peer after a restart.
		membership := logdb.Membership{
			ConfigChangeID:             first.Membership.ConfigChangeID,
			MaxWitnesses:               first.Membership.MaxWitnesses,
			RequireFullReplicaMajority: first.Membership.RequireFullReplicaMajority,
			Addresses:                  copyMapUint64String(first.Membership.Addresses),
			Observers:                  copyMapUint64String(first.Membership.Observers),
			Witnesses:                  copyMapUint64String(first.Membership.Witnesses),
			Removed:                    copyMapUint64Bool(first.Membership.Removed),
		}
		logdbSnap := logdb.Snapshot{
			Index:       snapshotIndex,
			Term:        snapshotTerm,
			Membership:  membership,
			Filepath:    snapshotDir,
			FileSize:    onDiskSize,
			OnDiskIndex: first.OnDiskIndex,
			// Record the RECEIVER's at-rest epoch (from re-encryption above), not
			// the wire epoch (first.Epoch is 0 — the sender streamed plaintext).
			// recoverSnapshot decrypts under this node's barrier at this epoch.
			Epoch: recvEpoch,
			// Record the compression of the bytes actually on disk, classified
			// above, so recoverSnapshot builds its reader chain from the
			// artifact rather than from this node's own config.
			Compression: recvCompression,
		}
		if saveErr := h.host.logdb.SaveSnapshot(shardID, replicaID, logdbSnap); saveErr != nil {
			return &SnapshotReceiveError{
				ShardID:   shardID,
				ReplicaID: replicaID,
				Op:        "logdb_save",
				Err:       saveErr,
			}
		}
		// Advance the log compaction floor to the snapshot index, exactly as the
		// local save path (engine snapshotPool) and ImportSnapshot do.
		//
		// Without this, a follower that fell behind and caught up via
		// InstallSnapshot keeps its OLD entries below the snapshot and then
		// resumes replication above it, so its durable entry index becomes
		// {1..k} U {N+1..M}: a HOLE. EntryRange checks the stored count against
		// the first..last span, correctly reports ErrLogGapDetected, and
		// recoverLogReader turns that into a StartShard failure — so the replica
		// is permanently unstartable after the next restart. There is no
		// self-heal (SnapshotEntries has no default, so this replica never
		// compacts on its own) and no repair path (RequestCompaction on an
		// unloaded shard only runs segment GC). This is reachable through
		// entirely normal operation: leader compacts, follower lags, follower
		// catches up, follower restarts.
		//
		// The snapshot subsumes every index at or below it, so removing them
		// loses nothing. It is done AFTER the snapshot record is durable: a crash
		// in between leaves the pre-existing shape (recoverable by the next
		// successful receive or a local snapshot), whereas removing first would
		// discard entries with no snapshot recorded to cover them.
		// NON-FATAL, and the placement is the reason: this runs inside the block
		// that precedes committed = true, so returning an error here would send
		// the deferred rollback through RemoveAll on the very directory the
		// SaveSnapshot one line above already durably recorded. That destroys a
		// committed snapshot: the shard then cannot start (its LogDB record points
		// at a deleted directory) and every leader re-send is refused by the dedup
		// branch — a permanent wedge, the exact failure this floor advance exists
		// to prevent.
		//
		// Failing to advance the floor is recoverable and self-healing: the
		// snapshot is durably recorded, so the hole left below it is precisely the
		// shape recoverLogReader repairs on the next start, and the next
		// successful receive or local snapshot advances the floor anyway.
		if rmErr := h.host.logdb.RemoveEntriesTo(shardID, replicaID, snapshotIndex); rmErr != nil {
			slog.Warn("snapshot recorded but the log floor advance failed; the entry "+
				"index is left holed below the snapshot and will be repaired on the next "+
				"recovery",
				"shard", shardID,
				"replica", replicaID,
				"snapshot_index", snapshotIndex,
				"error", rmErr)
		}
	}

	// The snapshot file is fsynced and the LogDB record is written, so the
	// on-disk artifacts must survive from here on regardless of what happens
	// to the raft install or the state-machine recovery below: LogDB now
	// points at this directory, and the dedup branch above answers the
	// leader's re-send from it.
	committed = true

	if h.host.isShardJoining(shardID) {
		// The bootstrap snapshot carries the full membership, so the join
		// window closes here: record the certificate-verified sender address
		// so the leader's retransmissions — and every later snapshot for this
		// shard — authorize against the registry like any other.
		h.host.registry.Register(shardID, fromReplicaID, senderAddr)
		h.host.clearShardJoining(shardID)
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
			ShardID:   shardID,
			ReplicaID: replicaID,
			Index:     snapshotIndex,
			Term:      snapshotTerm,
			Filepath:  dataPath,
			// The ON-DISK size, not totalSize (the plaintext wire size): the
			// artifact at Filepath is re-encrypted, and FileSize must describe
			// the file the send path will read.
			FileSize:   onDiskSize,
			Membership: first.Membership,
			// Deliver the RECEIVER's at-rest epoch (from reencryptReceivedSnapshot
			// above, persisted to LogDB), NOT the wire epoch (first.Epoch is 0 —
			// the sender streamed plaintext). Peer.Commit applies this snapshot to
			// the LogReader; using first.Epoch would make the in-memory raft layer
			// advertise the encrypted on-disk snapshot as plaintext until restart,
			// so if this node became leader doSendSnapshot would skip decryption
			// and stream ciphertext that followers cannot recover.
			Epoch: recvEpoch,
		},
	}
	if !deliverSnapshotControl(h.engine, shardID, installMsg) {
		// Fatal for this transfer. Without the install, raft never advances
		// its committed index to the snapshot index, so queuing state-machine
		// recovery would only produce a SnapshotRecoveryBehindError and the
		// node markers would stay behind. Reporting success here would stop
		// the leader from re-sending, leaving the follower permanently
		// unrecovered; report failure instead so the leader retries and the
		// dedup branch above re-delivers the install from LogDB.
		slog.Error("failed to deliver InstallSnapshot to raft",
			"shard", shardID,
			"replica", replicaID,
			"index", snapshotIndex,
		)
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "deliver_install",
			Err:       ErrSystemBusy,
		}
	}

	// Trigger snapshot recovery on the engine node. The engine's snapshot
	// pool handles the actual SM.RecoverFromSnapshot call. This must not
	// be best-effort: raft now treats every entry at or below the snapshot
	// index as applied, so if recovery is never queued the state machine
	// silently misses the snapshot contents and diverges from its peers.
	// A concurrent auto-save briefly holds the snapshotting slot, so retry
	// until the slot frees; if the retry budget is exhausted, report the
	// transfer failed so the leader re-sends and the dedup branch above
	// re-arms recovery from the persisted snapshot.
	engNode := h.engine.GetNode(shardID)
	if engNode != nil {
		if err := h.requestRecoveryWithRetry(shardID, replicaID, engNode); err != nil {
			return err
		}
	}

	// Note: The sender side (transportSender.doSendSnapshot) already delivers
	// a SnapshotStatus message to the leader's raft layer when the snapshot
	// transfer completes. We do NOT deliver SnapshotStatus here because this
	// handler runs on the RECEIVER host, and the SnapshotStatus must reach
	// the LEADER (on the sender host) to transition the remote out of
	// snapshot state.

	return nil
}

// requestRecoveryWithRetry queues state-machine recovery for a persisted,
// raft-installed snapshot, retrying while a concurrent snapshot save
// holds the node's snapshotting slot or the snapshot pool is briefly
// full. Recovery is mandatory once InstallSnapshot has been delivered
// (see HandleSnapshot); a request that cannot be queued within the retry
// budget is a snapshot receive failure, not a warning.
func (h *hostMessageHandler) requestRecoveryWithRetry(shardID, replicaID uint64, engNode *engine.Node) error {
	const (
		attempts = 100
		backoff  = 50 * time.Millisecond
	)
	for i := 0; i < attempts; i++ {
		if engNode.TryStartSnapshot() {
			err := h.engine.RequestSnapshotRecovery(shardID, replicaID, engNode)
			if err == nil {
				return nil
			}
			// Pool briefly full: release the slot and retry.
			engNode.ClearSnapshotting()
			slog.Warn("snapshot recovery request failed, retrying",
				"shard", shardID,
				"replica", replicaID,
				"attempt", i+1,
				"error", err,
			)
		}
		time.Sleep(backoff)
	}
	return &SnapshotReceiveError{
		ShardID:   shardID,
		ReplicaID: replicaID,
		Op:        "recover",
		Err:       ErrSnapshotInProgress,
	}
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

// snappyStreamIdentifier is the mandatory first block of the Snappy framing
// format: chunk type 0xff (stream identifier), a 3-byte length of 6, and the
// payload "sNaPpY". Every non-empty stream snappy.NewBufferedWriter produces
// begins with these bytes, and snappy.NewReader rejects any stream that does
// not.
var snappyStreamIdentifier = [snappyStreamIdentifierSize]byte{
	0xff, 0x06, 0x00, 0x00, 's', 'N', 'a', 'P', 'p', 'Y',
}

// snappyStreamIdentifierSize is the length of the Snappy stream identifier
// block: a 4-byte chunk header plus the 6-byte "sNaPpY" payload.
const snappyStreamIdentifierSize = 4 + 6

// classifySnapshotCompression classifies a snapshot artifact from its leading
// bytes.
//
// The snapshot wire protocol carries no compression field, and the receiver's
// own Config.SnapshotCompressionType says nothing about how the LEADER wrote
// the bytes it just streamed, so the artifact itself is the only available
// source. The Snappy stream identifier is a required, fixed prefix of every
// non-empty Snappy stream: bytes carrying it can only be read through a Snappy
// reader, and bytes without it only raw.
//
// It takes the reassembled chunks in MEMORY rather than a path because the
// artifact the receive path writes is encrypted from its first byte -- there is
// no plaintext file left on disk to read the identifier back out of.
//
// head may be shorter than the identifier (a small or empty artifact); a prefix
// that cannot contain the identifier is NoCompression, which is also the right
// answer for an empty artifact since both reader chains yield EOF on zero bytes.
func classifySnapshotCompression(head []byte) config.CompressionType {
	if len(head) >= snappyStreamIdentifierSize &&
		bytes.Equal(head[:snappyStreamIdentifierSize], snappyStreamIdentifier[:]) {
		return config.Snappy
	}
	return config.NoCompression
}

// snapshotChunkPrefix returns the first snappyStreamIdentifierSize bytes of the
// reassembled snapshot, gathered across chunk boundaries. The identifier is 10
// bytes and a chunk is megabytes, so the loop virtually always ends in the
// first chunk — but a sender is free to split anywhere, and a classifier that
// only looked at chunks[0] would silently misclassify a Snappy artifact whose
// first chunk is shorter than the identifier.
func snapshotChunkPrefix(chunks []*proto.SnapshotChunk) []byte {
	head := make([]byte, 0, snappyStreamIdentifierSize)
	for i := range chunks {
		need := snappyStreamIdentifierSize - len(head)
		if need <= 0 {
			break
		}
		data := chunks[i].Data
		if len(data) > need {
			data = data[:need]
		}
		head = append(head, data...)
	}
	return head
}

// snapshotChunkReader streams the payloads of reassembled snapshot chunks as a
// single io.Reader, without concatenating them into a second full-size buffer.
// It is what lets the receive path encrypt straight from the chunks it already
// holds instead of writing a plaintext artifact and re-encrypting it.
type snapshotChunkReader struct {
	chunks []*proto.SnapshotChunk
	chunk  int // index of the chunk being read
	off    int // read offset within that chunk
}

// Read fills p from the remaining chunk payloads, returning io.EOF once every
// chunk is exhausted. Empty chunks are skipped rather than reported as a
// zero-byte read, since a zero-byte read with a nil error stalls callers that
// treat it as no-progress.
func (r *snapshotChunkReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n := 0
	for n < len(p) && r.chunk < len(r.chunks) {
		data := r.chunks[r.chunk].Data
		if r.off >= len(data) {
			r.chunk++
			r.off = 0
			continue
		}
		copied := copy(p[n:], data[r.off:])
		r.off += copied
		n += copied
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

// writeReceivedSnapshotMetadata writes the metadata record beside a snapshot
// artifact this host did not produce itself (received from a peer or imported
// from an operator artifact).
//
// The record is encoded by engine.MarshalSnapshotMeta — the SAME encoder the
// snapshot pool uses for locally saved snapshots. Sharing it is mandatory, not
// tidiness: the DEK purge floor is computed by SCANNING snapshot directories
// and reading the Epoch field back (engine.readSnapshotEpoch). A record this
// path writes in a layout of its own reads as corrupt or as epoch-less, and an
// epoch-less directory suppresses key purging for the whole barrier until it
// rolls off. That is exactly what a divergent 5-field layout here did.
//
// epoch is the at-rest encryption epoch of the artifact AS STORED (0 when it is
// plaintext) and compression describes the bytes on disk; both must be the
// values actually applied, never this host's configured defaults, because
// recovery and the DEK purge floor read them back as fact.
func writeReceivedSnapshotMetadata(fsys snapshotFS, dir string, index, term, shardID, replicaID, epoch uint64,
	compression config.CompressionType) error {
	metaPath := filepath.Join(dir, "snapshot.meta")
	f, err := fsys.OpenFile(metaPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
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

	buf := engine.MarshalSnapshotMeta(engine.SnapshotMeta{
		Index:       index,
		Term:        term,
		ShardID:     shardID,
		ReplicaID:   replicaID,
		Epoch:       epoch,
		Compression: compression,
	})
	if _, writeErr := f.Write(buf); writeErr != nil {
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
