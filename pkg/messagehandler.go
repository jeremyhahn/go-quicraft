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
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// engineDeliverer is the engine surface hostMessageHandler, which routes inbound Raft messages to the engine, depends on; *engine.Engine satisfies it.
type engineDeliverer interface {
	DeliverMessage(shardID uint64, msg proto.Message) bool
	DeliverLocal(shardID uint64, msg proto.Message) bool
	DeliverLocalTo(node *engine.Node, msg proto.Message) bool
	NotifyWork(shardID uint64)
	GetNode(shardID uint64) *engine.Node
	RequestSnapshotRecovery(shardID, replicaID uint64, node *engine.Node) error
}

type hostMessageHandler struct {
	engine engineDeliverer
	host   *Host

	// dropLogStamp and dropsSinceLog rate-limit the per-message
	// authorization warnings (see warnDrop).
	dropLogStamp  atomic.Int64
	dropsSinceLog atomic.Int64

	// inflightSnapshotRecv serializes concurrent HandleSnapshot calls for one (shard, replica, index); a duplicate returns early.
	inflightSnapshotRecv sync.Map
}

// snapshotRecvKey identifies one in-flight snapshot receive by target
// replica and snapshot index.
type snapshotRecvKey struct {
	shardID   uint64
	replicaID uint64
	index     uint64
}

// snapshotCtrlDeliverer delivers in-process snapshot control messages via DeliverLocal, so they are stamped OriginLocal.
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

// deliverSnapshotControl delivers a SnapshotStatus or re-delivered InstallSnapshot, retrying briefly on a full inbox; it reports false if every attempt failed.
func deliverSnapshotControl(eng snapshotCtrlDeliverer, shardID uint64, msg proto.Message) bool {
	return deliverWithRetry(
		func() bool { return eng.DeliverLocal(shardID, msg) },
		func() { eng.NotifyWork(shardID) },
	)
}

// deliverWithRetry tries delivery up to snapshotCtrlDeliverAttempts, spaced by snapshotCtrlDeliverBackoff, and wakes the worker on success.
func deliverWithRetry(deliver func() bool, notify func()) bool {
	for attempt := 0; ; attempt++ {
		if deliver() {
			notify()
			return true
		}
		if attempt >= snapshotCtrlDeliverAttempts-1 {
			return false
		}
		time.Sleep(snapshotCtrlDeliverBackoff)
	}
}

// HandleMessage delivers each authorized message (see authorizeInbound) to its shard's inbox and signals the step worker; messages for unloaded shards are dropped.
func (h *hostMessageHandler) HandleMessage(batch proto.MessageBatch, peer transport.PeerAuth) error {
	// What each sender advertises is recorded only by a build that has a
	// newer version to switch to; one (shard, sender) pair per run.
	trackWire := h.host != nil && h.host.wireSwitchEnabled()
	var lastShard, lastFrom uint64
	noted := false
	for i := range batch.Requests {
		msg := &batch.Requests[i]

		if !h.authorizeInbound(msg, batch.SourceAddress, peer) {
			continue
		}
		if trackWire && (!noted || msg.ShardID != lastShard || msg.From != lastFrom) {
			noted, lastShard, lastFrom = true, msg.ShardID, msg.From
			h.host.notePeerWireVersion(msg.ShardID, msg.From, batch.MaxBinVer)
		}

		// Leader-executed requests pass the same gate, then go to the host router, not the inbox.
		if msg.Type >= proto.LeaderExec {
			if receive, ok := leaderExecReceivers[msg.Type]; ok {
				if h.host != nil && h.host.leaderExec != nil {
					receive(h.host, msg)
				}
				continue
			}
		}

		// DeliverMessage stamps OriginRemote, keeping raft's local-injection exemptions unreachable from the wire.
		h.engine.DeliverMessage(msg.ShardID, *msg)
		h.engine.NotifyWork(msg.ShardID)
	}
	return nil
}

// authorizeInbound reports whether a message may reach raft, after the removal deny-list, destination and committed sender-binding gates, then learns its address; drops are logged and counted.
func (h *hostMessageHandler) authorizeInbound(msg *proto.Message, sourceAddress string,
	peer transport.PeerAuth) bool {

	// Refuse senders with a committed removal, by claimed msg.From and by authenticated identity; see raft.Peer.IsRemoved and IsRemovedIdentity.
	node := h.engine.GetNode(msg.ShardID)
	if node != nil && node.IsRemoved(msg.From) {
		h.warnDrop("rejected message from removed replica",
			"shard", msg.ShardID,
			"from", msg.From,
			"type", proto.MessageTypeName(msg.Type),
		)
		h.countUnauthorizedDrop(msg.ShardID, msg.From, dropRemovedReplica)
		return false
	}
	if node != nil && node.IsRemovedIdentity(peer.Identity) {
		h.warnDrop("rejected message from removed replica identity",
			"shard", msg.ShardID,
			"claimed_from", msg.From,
			"identity", peer.Identity,
			"type", proto.MessageTypeName(msg.Type),
		)
		h.countUnauthorizedDrop(msg.ShardID, msg.From, dropRemovedIdentity)
		return false
	}
	// Refuse a sender removed by a committed but unfolded RemoveNode, counted with the committed tombstones.
	if node != nil && node.PendingRemoval(msg.From, peer.Identity) {
		h.warnDrop("rejected message from a replica a pending config change removes",
			"shard", msg.ShardID,
			"claimed_from", msg.From,
			"identity", peer.Identity,
			"type", proto.MessageTypeName(msg.Type),
		)
		h.countUnauthorizedDrop(msg.ShardID, msg.From, dropRemovedReplica)
		return false
	}

	// Drop a message addressed to another replica of this shard; To == 0 is allowed.
	if node != nil && msg.To != 0 && msg.To != node.ReplicaID() {
		h.warnDrop("rejected misrouted message",
			"shard", msg.ShardID,
			"from", msg.From,
			"to", msg.To,
			"local_replica", node.ReplicaID(),
			"type", proto.MessageTypeName(msg.Type),
		)
		h.countUnauthorizedDrop(msg.ShardID, msg.From, dropMisrouted)
		return false
	}

	// A shard not loaded here drops the message quietly.
	if node == nil {
		slog.Debug("dropped message for a shard that is not loaded",
			"shard", msg.ShardID,
			"from", msg.From,
			"type", proto.MessageTypeName(msg.Type),
		)
		return false
	}

	// Bind msg.From to the authenticated identity for every message type via senderBinding (committed, pending, then operator anchor); failures are counted as unknown_member, identity_mismatch or missing_identity.
	committed, reason := h.senderBinding(node, msg.ShardID, msg.From, peer)
	if reason != "" {
		h.warnDrop("rejected message: "+unauthorizedDropDescriptions[reason],
			"shard", msg.ShardID,
			"from", msg.From,
			"committed", committed,
			"source", sourceAddress,
			"type", proto.MessageTypeName(msg.Type),
			"reason", reason,
		)
		h.countUnauthorizedDrop(msg.ShardID, msg.From, reason)
		return false
	}

	// The pre-learn routing mapping, used only by learnSourceAddress.
	h.learnFromSource(node, msg, h.registeredAddress(msg.ShardID, msg.From), sourceAddress, peer)
	return true
}

// committedSenderBinding returns the committed identity for replicaID and the drop reason, empty when the connection's identity matches; no identity or no node is refused.
func committedSenderBinding(node *engine.Node, replicaID uint64, peer transport.PeerAuth) (string, string) {
	if node == nil {
		return "", dropUnknownMember
	}
	binding, ok := node.MemberBinding(replicaID)
	if !ok || binding.Address == "" {
		return "", dropUnknownMember
	}
	if binding.Identity == "" {
		return "", dropMissingIdentity
	}
	// Both values are public, so no constant-time comparison.
	if peer.Identity != binding.Identity {
		return binding.Identity, dropIdentityMismatch
	}
	return binding.Identity, ""
}

// senderBinding reports whether the sender is the replica it claims, by identity from the committed configuration, the pending configuration, or the operator anchor, returning the judged value and the drop reason.
func (h *hostMessageHandler) senderBinding(node *engine.Node, shardID, from uint64,
	peer transport.PeerAuth) (string, string) {

	committed, reason := committedSenderBinding(node, from, peer)
	if reason != dropUnknownMember {
		return committed, reason
	}
	if node == nil {
		return "", dropUnknownMember
	}
	// Neither pending nor anchor may admit a keypair the committed configuration binds to another replica.
	if holder, ok := node.IdentityHolder(peer.Identity); ok && holder != from {
		return "", dropIdentityMismatch
	}
	if identity, ok := node.PendingMemberIdentity(from); ok {
		if peer.Identity != identity {
			return identity, dropIdentityMismatch
		}
		return identity, ""
	}
	if h.host != nil {
		if anchor, ok := h.host.anchorIdentity(shardID, from); ok {
			if peer.Identity != anchor {
				return anchor, dropIdentityMismatch
			}
			return anchor, ""
		}
	}
	return "", dropUnknownMember
}

// replicaIdentity returns the node identity senderBinding binds to replicaID, or "".
func (h *hostMessageHandler) replicaIdentity(shardID, replicaID uint64) string {
	if h.host == nil || h.engine == nil {
		return ""
	}
	node := h.engine.GetNode(shardID)
	if node == nil {
		return ""
	}
	return h.boundIdentity(node, shardID, replicaID)
}

// boundIdentity returns the identity senderBinding binds replicaID of shardID to, or "".
func (h *hostMessageHandler) boundIdentity(node *engine.Node, shardID, replicaID uint64) string {
	if binding, ok := node.MemberBinding(replicaID); ok && binding.Address != "" {
		return binding.Identity
	}
	if identity, ok := node.PendingMemberIdentity(replicaID); ok {
		return identity
	}
	if anchor, ok := h.host.anchorIdentity(shardID, replicaID); ok {
		return anchor
	}
	return ""
}

// registeredAddress returns the routing registry's address for (shardID, replicaID), or ""; it is not an authorization source.
func (h *hostMessageHandler) registeredAddress(shardID, replicaID uint64) string {
	if h.host == nil || h.host.registry == nil {
		return ""
	}
	addr, err := h.host.registry.Resolve(shardID, replicaID)
	if err != nil {
		return ""
	}
	return addr
}

// Drop reasons for countUnauthorizedDrop, a closed set of metric labels; address_mismatch and shared_address are retired.
const (
	dropRemovedReplica = "removed_replica"
	// dropRemovedIdentity is a removed identity presenting another replica's ID, unlike the usually benign dropRemovedReplica.
	dropRemovedIdentity  = "removed_identity"
	dropMisrouted        = "misrouted"
	dropUnknownMember    = "unknown_member"
	dropIdentityMismatch = "identity_mismatch"
	dropMissingIdentity  = "missing_identity"
)

// unauthorizedDropDescriptions gives each reason the sentence an operator reads
// in the log. Keeping them beside the label values is what stops the two from
// describing different things as the code moves.
var unauthorizedDropDescriptions = map[string]string{
	dropRemovedReplica:   "the shard has committed a removal for this replica",
	dropRemovedIdentity:  "the connection's authenticated node identity is one the shard has committed a removal for, whatever replica ID it claims",
	dropMisrouted:        "addressed to a different replica of this shard",
	dropUnknownMember:    "no configuration this replica holds names the sender (not committed, not pending in its own log, not in Config.MemberIdentities); if this persists for a leader, this replica missed the config change that added it — restart it with a Config.MemberIdentities that names the leader",
	dropIdentityMismatch: "the connection's authenticated node identity is not the one committed membership assigns this replica",
	dropMissingIdentity:  "committed membership records no node identity for this replica; a member is authorized by its identity and by nothing else",
}

// dropLogInterval bounds how often a per-message authorization drop is
// logged. The drop counters record every one.
const dropLogInterval = 10 * time.Second

// warnDrop logs an authorization drop, rate-limited, with the count it stands for.
func (h *hostMessageHandler) warnDrop(msg string, args ...any) {
	n := h.dropsSinceLog.Add(1)
	if !transport.LogSampled(&h.dropLogStamp, dropLogInterval) {
		return
	}
	h.dropsSinceLog.Add(-n)
	slog.Warn(msg, append(args, "drops_since_last_log", n)...)
}

// countUnauthorizedDrop counts a refused message with its logged reason when metrics are wired.
func (h *hostMessageHandler) countUnauthorizedDrop(shardID, from uint64, reason string) {
	if h.host != nil && h.host.metrics != nil {
		h.host.metrics.IncUnauthorizedMessageDropped(shardID, from, reason)
	}
}

// learnFromSource records an authorized sender's certificate-bound address for routing; a committed, identity-proved member may overwrite its mapping, any other bound sender may only fill an empty one.
func (h *hostMessageHandler) learnFromSource(node *engine.Node, msg *proto.Message,
	registered, sourceAddress string, peer transport.PeerAuth) {

	if node == nil || sourceAddress == "" || !peer.Authorizes(sourceAddress) {
		return
	}
	h.learnSourceAddress(msg.ShardID, msg.From, registered, sourceAddress,
		routingOverwriteAuthorized(node, msg.From, peer))
}

// routingOverwriteAuthorized reports whether the connection's identity equals the committed identity for replicaID.
func routingOverwriteAuthorized(node *engine.Node, replicaID uint64, peer transport.PeerAuth) bool {
	binding, ok := node.MemberBinding(replicaID)
	if !ok || binding.Identity == "" {
		return false
	}
	return peer.Identity == binding.Identity
}

// learnSourceAddress records or, for a committed member, updates the (shardID, replicaID) routing address; registered is the earlier mapping, advisory only.
func (h *hostMessageHandler) learnSourceAddress(shardID, replicaID uint64, registered, address string, mayOverwrite bool) {
	if address == "" || h.host == nil || h.host.registry == nil {
		return
	}
	// Skip the store when the mapping is already current.
	if registered == address {
		return
	}
	// Publish under peer admission via publishLearnedAddress; RegisterIfAbsent makes fill-but-never-replace atomic.
	if !mayOverwrite {
		h.host.publishLearnedAddress(shardID, replicaID, address, func() {
			if existing, stored := h.host.registry.RegisterIfAbsent(shardID, replicaID, address); !stored && existing != address {
				slog.Warn("ignoring address change from a replica outside the configuration",
					"shard", shardID,
					"replica", replicaID,
					"registered", existing,
					"offered", address,
				)
			}
		})
		return
	}
	// Log replacement of an established mapping.
	h.host.publishLearnedAddress(shardID, replicaID, address, func() {
		if registered != "" {
			slog.Warn("replacing the registered address of a configured replica",
				"shard", shardID,
				"replica", replicaID,
				"registered", registered,
				"offered", address,
			)
		}
		h.host.registry.Register(shardID, replicaID, address)
	})
}

// admitSnapshotSender runs every snapshot sender check and returns the loaded node; it runs from the header and again from the chunks.
func (h *hostMessageHandler) admitSnapshotSender(shardID, replicaID, fromReplicaID, snapshotIndex uint64, peer transport.PeerAuth) (*engine.Node, error) {
	// The shard must be loaded, the snapshot addressed to this replica, and From bound by senderBinding, never the routing registry.

	// Count against shard 0 until a loaded shard is found, so senders cannot mint metric series.
	var countedShard uint64
	reject := func(reason string) (*engine.Node, error) {
		return nil, h.refuseUnauthorizedSnapshot(countedShard, shardID, fromReplicaID, snapshotIndex, reason)
	}
	if h.engine == nil {
		return reject("no engine")
	}
	node := h.engine.GetNode(shardID)
	if node == nil {
		// No loaded node: fail closed as an authorization failure.
		return reject("shard not loaded")
	}
	countedShard = shardID
	// Drop a snapshot addressed to another replica.
	if replicaID != node.ReplicaID() {
		return reject("snapshot addressed to a different replica")
	}
	// Deny-list by claimed ID and by authenticated identity; see raft.Peer.IsRemoved.
	if node.IsRemoved(fromReplicaID) {
		return reject("sender removed from the shard")
	}
	if node.IsRemovedIdentity(peer.Identity) {
		return reject("the connection's authenticated node identity is one the shard removed")
	}
	if node.PendingRemoval(fromReplicaID, peer.Identity) {
		return reject("a committed config change this replica has not yet applied removes the sender")
	}
	if h.host.registry == nil {
		return reject("no registry to record the sender address")
	}
	if _, reason := h.senderBinding(node, shardID, fromReplicaID, peer); reason != "" {
		return reject(reason)
	}
	// Failed or restarting shard states are reported as such, after authorization and before staging.
	notAccepting := func(cause error) (*engine.Node, error) {
		slog.Debug("snapshot refused: shard is not accepting installs",
			"shard", shardID,
			"from", fromReplicaID,
			"index", snapshotIndex,
			"error", cause,
		)
		return nil, &SnapshotReceiveError{ShardID: shardID, ReplicaID: replicaID, Op: "admit", Err: cause}
	}
	// A failed shard steps nothing more, so its install would never get a
	// verdict; refuse before staging anything rather than hold a transport
	// receive slot and its memory budget for nothing.
	if node.IsFailed() {
		return notAccepting(ErrShardFailed)
	}
	// A stopped incarnation adopts nothing; wait for the next one.
	if node.IsStopped() {
		return notAccepting(ErrShardNotReady)
	}
	// A paused replica receives no snapshot; HandleSnapshot registers with the pause.
	if node.IsPaused() {
		return notAccepting(ErrShardPaused)
	}
	return node, nil
}

// refuseUnauthorizedSnapshot logs and counts a snapshot refused for failing
// authorization, and returns the error the transfer is refused with.
// countedShard is the metric label: 0 until the shard is known to be loaded.
func (h *hostMessageHandler) refuseUnauthorizedSnapshot(countedShard, shardID, fromReplicaID, snapshotIndex uint64, reason string) error {
	slog.Warn("rejected unauthorized snapshot",
		"shard", shardID,
		"from", fromReplicaID,
		"index", snapshotIndex,
		"reason", reason,
	)
	if h.host.metrics != nil {
		h.host.metrics.IncUnauthorizedSnapshotDropped(countedShard, fromReplicaID)
	}
	return &UnauthorizedMessageError{
		ShardID: shardID,
		From:    fromReplicaID,
		MsgType: proto.InstallSnapshot,
	}
}

// AuthorizeSnapshot implements transport.MessageHandler: a snapshot's sender
// is admitted from the stream header, before the transport takes any receive
// slot or memory for the transfer.
func (h *hostMessageHandler) AuthorizeSnapshot(shardID, replicaID, from, index uint64, peer transport.PeerAuth) error {
	if h.host == nil {
		return nil // as HandleSnapshot: a handler not wired to a host persists nothing
	}
	_, err := h.admitSnapshotSender(shardID, replicaID, from, index, peer)
	var notAccepting *SnapshotReceiveError
	if errors.As(err, &notAccepting) {
		// A failed or stopping shard: an ordinary state, which the transport
		// reports quietly and apart from unauthorized transfers.
		return fmt.Errorf("%w: %w", transport.ErrSnapshotNotAccepting, err)
	}
	return err
}

// HandleSnapshot reassembles received chunks, persists the snapshot and its LogDB record, delivers InstallSnapshot to raft and triggers state-machine recovery.
//
//nolint:gocyclo // snapshot receive handles validation, directory creation, assembly, and cleanup
func (h *hostMessageHandler) HandleSnapshot(chunks []proto.SnapshotChunk, peer transport.PeerAuth) error {
	if len(chunks) == 0 {
		return nil
	}

	// Guard against nil host. This can happen in unit tests or when the
	// handler is not fully wired.
	if h.host == nil {
		return nil
	}

	// Validate the chunk set before trusting chunks[0]; ordered holds chunks by ChunkID.
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

	// Per-shard authorization, fail closed — the gates AuthorizeSnapshot
	// applied from the stream header before the transport took any receive
	// resource, re-checked from the chunks before anything is persisted.
	node, err := h.admitSnapshotSender(shardID, replicaID, fromReplicaID, snapshotIndex, peer)
	if err != nil {
		return err
	}
	// Registered with the pause before anything is staged: a PauseShard
	// that set the flag first refuses this receive, one that sets it later
	// waits until the receive has returned.
	if !node.BeginSnapshotTransfer() {
		return &SnapshotReceiveError{ShardID: shardID, ReplicaID: replicaID, Op: "admit", Err: ErrShardPaused}
	}
	defer node.EndSnapshotTransfer()
	// Refuse a snapshot whose binding for this replica is not the identity this host presents.
	if own := h.host.NodeIdentity(); own != "" {
		if committed := first.Membership.Identities[replicaID]; committed != "" && committed != own {
			return h.refuseUnauthorizedSnapshot(shardID, shardID, fromReplicaID, snapshotIndex, fmt.Sprintf("the snapshot's membership binds this replica to identity %s "+
				"but this host presents %s; every peer would refuse it — remove and re-add the "+
				"replica with the identity it presents", committed, own))
		}
	}

	// Serialize concurrent receives of one snapshot index; a duplicate is dropped and the leader re-sends.
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

	// Dedup: if LogDB already has a snapshot at this index or later, skip persistence and re-deliver InstallSnapshot from the persisted one.
	if h.host.logdb != nil {
		existing, err := h.host.logdb.GetSnapshot(shardID, replicaID)
		// Fail closed when the lookup fails with anything but ErrNoSnapshot.
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
			// Measure snapshot.dat rather than trusting existing.FileSize; refuse to re-deliver if it cannot be measured.
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
					// Carry the artifact's full description, including Compression.
					OnDiskIndex: existing.OnDiskIndex,
					Compression: uint64(existing.Compression),
				},
			}
			if !deliverSnapshotControl(h.engine, shardID, installMsg) {
				slog.Warn("failed to re-deliver InstallSnapshot for persisted snapshot",
					"shard", shardID,
					"replica", replicaID,
					"index", existing.Index,
				)
			}
			// Re-arm state-machine recovery when the persisted snapshot is ahead of the applied state.
			if existing.Index > node.LastApplied() {
				if err := h.requestRecoveryWithRetry(shardID, replicaID, node); err != nil {
					return err
				}
			}
			return nil
		}
	}

	// Resolve the at-rest barrier before writing anything; a configured barrier that is sealed or unresolved fails rather than writing plaintext.
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
			Err:       &SnapshotSizeMismatchError{Declared: first.FileSize, Actual: totalSize},
		}
	}

	// Take the snapshot directory from the engine node so it matches the snapshot pool's location.
	snapshotDir := node.SnapshotSaveDir(snapshotIndex)

	fsys := h.host.snapshotFsys()

	// Write into a staging directory owned by this receive and publish with one rename, as Host.ImportSnapshot does.
	stageParent := filepath.Dir(snapshotDir)
	if err := fsys.MkdirAll(stageParent, 0o750); err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "mkdir",
			Err:       err,
		}
	}
	stageDir, err := fsys.MkdirTemp(stageParent, engine.RecvStageDirPattern)
	if err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "stage",
			Err:       err,
		}
	}

	// ownedDir is the directory this receive may discard on failure; committed is set once the artifact is recorded.
	ownedDir := stageDir
	var committed bool
	// retain keeps an unrecorded artifact raft may have adopted, as an orphan for GC.
	var retain bool
	defer func() {
		if committed || retain {
			return
		}
		// Discard the published directory under the write lease; on a stopped node keep it as an orphan.
		if ownedDir == snapshotDir {
			if !node.AcquireWriteLease() {
				slog.Info("snapshot directory kept: the shard stopped before the "+
					"failed receive could discard it",
					"shard", shardID,
					"replica", replicaID,
					"dir", ownedDir)
				return
			}
			defer node.ReleaseWriteLease()
			// Check the record under the snapshot-directory lock; a local save may have recorded this index.
			node.LockSnapshotDir()
			defer node.UnlockSnapshotDir()
			if h.host.snapshotRecordMayName(shardID, replicaID, ownedDir) {
				slog.Info("snapshot directory kept: LogDB records it (or cannot say)",
					"shard", shardID,
					"replica", replicaID,
					"dir", ownedDir)
				return
			}
		}
		if err := fsys.RemoveAll(ownedDir); err != nil {
			slog.Debug("snapshot directory cleanup failed",
				"shard", shardID,
				"replica", replicaID,
				"dir", ownedDir,
				"error", err)
		}
	}()

	// Classify compression from the reassembled plaintext before writing.
	recvCompression := classifySnapshotCompression(snapshotChunkPrefix(ordered))

	// Write the snapshot encrypted inline under this receiver's barrier, so no plaintext artifact ever exists; a nil barrier writes plaintext.
	stageDataPath := filepath.Join(stageDir, "snapshot.dat")
	f, err := fsys.OpenFile(stageDataPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
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
		// Hold the pins on every epoch encryptSnapshotFrames sealed under until the handler returns.
		var recvPins []uint64
		// The frames are bound to (shardID, replicaID, snapshotIndex) plus their
		// ordinal, the same identity recoverSnapshot reads them back under.
		recvEpoch, recvPins, writeErr = encryptSnapshotFrames(
			recvBarrier, shardID, replicaID, snapshotIndex, plain, f)
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
	// Return the Close error; a late write failure must stop the snapshot being recorded.
	if closeErr := f.Close(); closeErr != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "close",
			Err:       closeErr,
		}
	}

	// FileSize is the size of the file at Filepath, measured after encryption.
	onDiskInfo, statErr := fsys.Stat(stageDataPath)
	if statErr != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "stat",
			Err:       statErr,
		}
	}
	onDiskSize := uint64(onDiskInfo.Size())

	// Write the metadata file with this receiver's epoch and the classified compression.
	if err := writeReceivedSnapshotMetadata(fsys, stageDir, snapshotIndex, snapshotTerm,
		shardID, replicaID, recvEpoch, recvCompression); err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "metadata",
			Err:       err,
		}
	}

	// Publish under this incarnation's write lease.
	if !node.AcquireWriteLease() {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "publish",
			Err:       ErrShardNotReady,
		}
	}
	if publishErr := func() error {
		defer node.ReleaseWriteLease()
		// Under the snapshot-directory lock too: the recorded-destination
		// check below and the rename must not interleave with a local save's
		// publish and record (see engine.Node.LockSnapshotDir).
		node.LockSnapshotDir()
		defer node.UnlockSnapshotDir()
		// Publish by rename; an existing destination is never replaced if it might be recorded or being read.
		if h.host.snapshotRecordMayName(shardID, replicaID, snapshotDir) || node.IsSnapshotDirPinned(snapshotDir) {
			return &SnapshotReceiveError{
				ShardID:   shardID,
				ReplicaID: replicaID,
				Op:        "publish",
				Err:       ErrSnapshotDestinationRecorded,
			}
		}
		// os.Rename onto a non-empty directory fails, so an orphaned destination is
		// cleared first. It is an orphan by the argument above: no LogDB record
		// names it.
		if _, statErr := fsys.Stat(snapshotDir); statErr == nil {
			slog.Warn("snapshot receive is replacing an unrecorded snapshot directory "+
				"left by an earlier failed transfer",
				"shard", shardID,
				"replica", replicaID,
				"dir", snapshotDir,
			)
			if rmErr := fsys.RemoveAll(snapshotDir); rmErr != nil {
				return &SnapshotReceiveError{
					ShardID:   shardID,
					ReplicaID: replicaID,
					Op:        "publish",
					Err:       rmErr,
				}
			}
		}
		if renameErr := fsys.Rename(stageDir, snapshotDir); renameErr != nil {
			return &SnapshotReceiveError{
				ShardID:   shardID,
				ReplicaID: replicaID,
				Op:        "publish",
				Err:       renameErr,
			}
		}
		// Claim the index until this receive settles: until raft's verdict
		// the directory is unrecorded, and a local save at the same index
		// would otherwise take it for an orphan and replace it.
		node.ClaimSnapshotDir(snapshotIndex)
		return nil
	}(); publishErr != nil {
		return publishErr
	}
	// Released when the receive returns; taken without a lease.
	defer func() {
		node.LockSnapshotDir()
		node.ReleaseSnapshotDir(snapshotIndex)
		node.UnlockSnapshotDir()
	}()
	// The canonical directory is now this receive's to discard on failure.
	ownedDir = snapshotDir
	dataPath := filepath.Join(snapshotDir, "snapshot.dat")

	// Fsync the directory chain up to the snapshot base before recording in LogDB.
	if err := syncDirChain(fsys, snapshotDir, node.SnapshotDir()); err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "sync_dir",
			Err:       err,
		}
	}

	joining := h.host.isShardJoining(shardID)
	if joining {
		// Register the leader's committed address for routing only, under peer admission; authorization uses the installed membership.
		if senderAddr := first.Membership.Addresses[fromReplicaID]; senderAddr != "" {
			h.host.publishCommittedAddress(shardID, fromReplicaID, senderAddr)
		}
	}

	// Deliver InstallSnapshot to raft so it updates log state and membership.
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
			// The on-disk size, not the plaintext wire size.
			FileSize:   onDiskSize,
			Membership: first.Membership,
			// This receiver's at-rest epoch, not the wire epoch (always 0).
			Epoch: recvEpoch,
			// The classified compression and on-disk cursor as persisted.
			Compression: uint64(recvCompression),
			OnDiskIndex: first.OnDiskIndex,
		},
	}
	// Record, compact and recover only after raft adopts the install, on the node it was delivered to.
	verdictC := node.AwaitInstallVerdict(snapshotIndex)
	if !deliverWithRetry(
		func() bool { return h.engine.DeliverLocalTo(node, installMsg) },
		func() { h.engine.NotifyWork(shardID) },
	) {
		node.CancelInstallVerdict(snapshotIndex)
		// Fatal for this transfer: nothing is recorded, so the leader's
		// re-send is received afresh.
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
	var verdict engine.InstallVerdict
	select {
	case verdict = <-verdictC:
	case <-node.StopC():
		verdict = engine.InstallVerdict{Outcome: engine.InstallAbandoned}
	}
	switch verdict.Outcome {
	case engine.InstallAdopted:
	case engine.InstallDeclined:
		// Raft is already at or past this snapshot; the unrecorded artifact
		// is discarded by the deferred rollback.
		slog.Info("snapshot receive declined: raft is already at or past it",
			"shard", shardID,
			"replica", replicaID,
			"index", snapshotIndex,
		)
		return nil
	case engine.InstallRefused:
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "install",
			Err:       verdict.Err,
		}
	case engine.InstallSuperseded:
		// Dropped before it was stepped — evicted for a newer install, or
		// lost to a recovered step-worker panic: raft never saw it, so the
		// unrecorded artifact is discarded.
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "install",
			Err:       ErrSystemBusy,
		}
	default: // InstallAbandoned: the shard stopped or failed, or its step panicked
		// No verdict: keep the artifact, since raft may have adopted it.
		retain = true
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "install",
			Err:       ErrSystemBusy,
		}
	}
	// Adopted: record under the adopting incarnation's write lease; a stopped node counts as abandoned.
	if !node.AcquireWriteLease() {
		retain = true
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "install",
			Err:       ErrSystemBusy,
		}
	}
	// From this point the artifact is raft's, and no failure below may send
	// the rollback through RemoveAll on it.
	committed = true

	if recordErr := func() error {
		defer node.ReleaseWriteLease()
		node.LockSnapshotDir()
		defer node.UnlockSnapshotDir()
		// Save snapshot metadata to LogDB so the node can recover from it.
		if h.host.logdb != nil {
			// Carry the full membership, including the witness policy, identities and session policy.
			membership := logdb.Membership{
				ConfigChangeID:             first.Membership.ConfigChangeID,
				MaxWitnesses:               first.Membership.MaxWitnesses,
				RequireFullReplicaMajority: first.Membership.RequireFullReplicaMajority,
				SessionExpiryEntries:       first.Membership.SessionExpiryEntries,
				MaxTotalSessions:           first.Membership.MaxTotalSessions,
				MaxResponsesPerSession:     first.Membership.MaxResponsesPerSession,
				MaxSessionsPerOwner:        first.Membership.MaxSessionsPerOwner,
				WireVersion:                first.Membership.WireVersion,
				Addresses:                  copyMapUint64String(first.Membership.Addresses),
				Observers:                  copyMapUint64String(first.Membership.Observers),
				Witnesses:                  copyMapUint64String(first.Membership.Witnesses),
				Removed:                    copyMapUint64Bool(first.Membership.Removed),
				Identities:                 copyMapUint64String(first.Membership.Identities),
				RemovedIdentities:          copyMapUint64String(first.Membership.RemovedIdentities),
			}
			logdbSnap := logdb.Snapshot{
				Index:       snapshotIndex,
				Term:        snapshotTerm,
				Membership:  membership,
				Filepath:    snapshotDir,
				FileSize:    onDiskSize,
				OnDiskIndex: first.OnDiskIndex,
				// This receiver's at-rest epoch, not the wire epoch.
				Epoch: recvEpoch,
				// Record the compression of the bytes actually on disk, classified
				// above, so recoverSnapshot builds its reader chain from the
				// artifact rather than from this node's own config.
				Compression: recvCompression,
			}
			// A record at this index or later is this snapshot; any other failure fails the shard.
			if saveErr := h.host.logdb.SaveSnapshot(shardID, replicaID, logdbSnap); saveErr != nil &&
				!errors.Is(saveErr, logdb.ErrSnapshotOutOfDate) {
				h.host.engine.FailShard(node, saveErr)
				return &SnapshotReceiveError{
					ShardID:   shardID,
					ReplicaID: replicaID,
					Op:        "logdb_save",
					Err:       saveErr,
				}
			}
			// Advance the log compaction floor to the snapshot index after the record is durable; failure is non-fatal and repaired on next start.
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

		// Reclaim superseded snapshot directories now; failure is non-fatal.
		h.host.gcShardSnapshots(node.SnapshotDir(), shardID, replicaID, node.GCSparedSnapshotDirs()...)
		// Inside the lease with the record: a join window is keyed by shard ID,
		// and once this incarnation stops, the window may already belong to
		// the next one.
		if joining {
			h.host.clearShardJoining(shardID)
		}
		return nil
	}(); recordErr != nil {
		return recordErr
	}

	// Queue state-machine recovery on the adopting node, retrying; if it cannot be queued, fail the transfer so the leader re-sends.
	if err := h.requestRecoveryWithRetry(shardID, replicaID, node); err != nil {
		return err
	}

	// SnapshotStatus is delivered by the sender side, not here.

	return nil
}

// requestRecoveryWithRetry queues recovery for an installed snapshot, retrying while the slot or pool is busy; exhausting the budget is a receive failure.
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

// validateSnapshotChunks checks the chunks share identity and metadata and have ChunkIDs exactly 0..N-1, returning them ordered by ChunkID or a *SnapshotReceiveError with Op "validate".
func validateSnapshotChunks(chunks []proto.SnapshotChunk) ([]*proto.SnapshotChunk, error) {
	first := &chunks[0]
	n := uint64(len(chunks))

	validateErr := func(chunkID uint64, reason string) ([]*proto.SnapshotChunk, error) {
		return nil, &SnapshotReceiveError{
			ShardID:   first.ShardID,
			ReplicaID: first.ReplicaID,
			Op:        "validate",
			Err:       &SnapshotChunkError{ChunkID: chunkID, ChunkCount: n, Reason: reason},
		}
	}

	// Check chunk 0's position once, wrapping proto.ErrLogIndexOutOfRange or ErrTermOutOfRange.
	if err := (&proto.Snapshot{Index: first.Index, Term: first.Term}).CheckBounds(); err != nil {
		return nil, &SnapshotReceiveError{
			ShardID:   first.ShardID,
			ReplicaID: first.ReplicaID,
			Op:        "validate",
			Err:       err,
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
			return validateErr(c.ChunkID, fmt.Sprintf(
				"identity mismatch with chunk 0 "+
					"(shard %d/%d replica %d/%d from %d/%d index %d/%d term %d/%d epoch %d/%d ondisk %d/%d)",
				c.ShardID, first.ShardID,
				c.ReplicaID, first.ReplicaID,
				c.From, first.From,
				c.Index, first.Index,
				c.Term, first.Term,
				c.Epoch, first.Epoch,
				c.OnDiskIndex, first.OnDiskIndex,
			))
		}

		if c.ChunkCount != n {
			return validateErr(c.ChunkID, fmt.Sprintf(
				"declares ChunkCount %d but %d chunks were received", c.ChunkCount, n))
		}

		if c.ChunkID >= n {
			return validateErr(c.ChunkID, "chunk ID out of range")
		}
		if ordered[c.ChunkID] != nil {
			return validateErr(c.ChunkID, "duplicate chunk ID")
		}
		ordered[c.ChunkID] = c
	}

	// Every slot must be filled; a nil slot means a missing ChunkID (gap).
	for id := range ordered {
		if ordered[id] == nil {
			return validateErr(uint64(id), "missing chunk ID")
		}
	}

	return ordered, nil
}

// snappyStreamIdentifier is the mandatory first block of a Snappy framed stream.
var snappyStreamIdentifier = [snappyStreamIdentifierSize]byte{
	0xff, 0x06, 0x00, 0x00, 's', 'N', 'a', 'P', 'p', 'Y',
}

// snappyStreamIdentifierSize is the length of the Snappy stream identifier
// block: a 4-byte chunk header plus the 6-byte "sNaPpY" payload.
const snappyStreamIdentifierSize = 4 + 6

// classifySnapshotCompression returns Snappy if head starts with the stream identifier, else NoCompression.
func classifySnapshotCompression(head []byte) config.CompressionType {
	if len(head) >= snappyStreamIdentifierSize &&
		bytes.Equal(head[:snappyStreamIdentifierSize], snappyStreamIdentifier[:]) {
		return config.Snappy
	}
	return config.NoCompression
}

// snapshotChunkPrefix returns the first snappyStreamIdentifierSize bytes across chunk boundaries.
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

// snapshotChunkReader reads the chunk payloads as one stream without concatenating them.
type snapshotChunkReader struct {
	chunks []*proto.SnapshotChunk
	chunk  int // index of the chunk being read
	off    int // read offset within that chunk
}

// Read fills p from the remaining chunks, skipping empty ones, and returns io.EOF at the end.
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

// writeReceivedSnapshotMetadata writes the engine.MarshalSnapshotMeta record beside a received or imported artifact, with its stored epoch and compression.
func writeReceivedSnapshotMetadata(fsys snapshotFS, dir string, index, term, shardID, replicaID, epoch uint64,
	compression config.CompressionType) error {
	// Stamp integrity fields over the stored data file; a hash failure fails the write.
	dataFile, err := fsys.Open(filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		return err
	}
	dataHash, dataLen, hashErr := engine.HashSnapshotReader(dataFile)
	// A read-only handle's Close error cannot indicate lost data, so it does
	// not override the hash's own outcome.
	_ = dataFile.Close()
	if hashErr != nil {
		return hashErr
	}

	metaPath := filepath.Join(dir, "snapshot.meta")
	f, err := fsys.OpenFile(metaPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	buf := engine.MarshalSnapshotMeta(engine.SnapshotMeta{
		Index:       index,
		Term:        term,
		ShardID:     shardID,
		ReplicaID:   replicaID,
		Epoch:       epoch,
		Compression: compression,
		DataLen:     dataLen,
		DataHash:    dataHash,
	})
	// Write, sync, then close, reporting the first error.
	_, writeErr := f.Write(buf)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
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
