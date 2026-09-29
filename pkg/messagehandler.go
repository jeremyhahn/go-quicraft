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
	return deliverWithRetry(
		func() bool { return eng.DeliverLocal(shardID, msg) },
		func() { eng.NotifyWork(shardID) },
	)
}

// deliverWithRetry makes one delivery attempt per snapshotCtrlDeliverBackoff,
// up to snapshotCtrlDeliverAttempts, and wakes the shard's worker on success.
// It is the one retry loop for snapshot control messages, whichever way they
// are addressed.
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

// HandleMessage delivers each message in the batch to the engine's
// per-shard inbox and signals the step worker that work is available.
//
// The per-message authorization decision — deny-list, destination, sender
// binding, and the routing-address learning that follows it — lives in
// authorizeInbound. This is the loop and the delivery.
//
// Every message is bound to a replica the receiver can vouch for — by the
// committed configuration, the pending configuration in its own log, or the
// operator anchor — before it is delivered (senderBinding); there are no
// type-based exemptions. A message for a shard that is not loaded here is
// dropped: there is nothing to bind it to and nothing to deliver it into.
func (h *hostMessageHandler) HandleMessage(batch proto.MessageBatch, peer transport.PeerAuth) error {
	for i := range batch.Requests {
		msg := &batch.Requests[i]

		if !h.authorizeInbound(msg, batch.SourceAddress, peer) {
			continue
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

// authorizeInbound decides whether one inbound message may reach raft, and
// performs the address learning that must follow that decision. It reports true
// when the message may be delivered; every false return has already logged the
// reason and counted the drop.
//
// Three gates run:
//
//  1. Removal deny-list: a sender this shard has COMMITTED a removal for.
//  2. Destination: a message addressed to a different replica of this shard.
//  3. Sender binding: a QUORUM-BEARING message must come from the peer this
//     shard's COMMITTED MEMBERSHIP names as msg.From.
//
// Gate 3 deliberately does not consult the address registry. The registry is a
// mutable routing cache that inbound traffic itself writes, so anything an
// inbound message can teach it, an inbound message can forge — an earlier
// version of this binding did read it, and a message could re-point the entry
// and then be checked against what it had just written. The committed
// membership is the one address source a peer cannot influence without going
// through Raft consensus. Authorization reads it; routing continues to use the
// registry, and the two are now independent.
func (h *hostMessageHandler) authorizeInbound(msg *proto.Message, sourceAddress string,
	peer transport.PeerAuth) bool {

	// Per-shard authorization: refuse senders this shard has COMMITTED a
	// removal for. This is deliberately a deny-list, not a member allow-list —
	// a replica's configuration lags by design, so allow-listing makes a stale
	// replica drop the Replicate, Heartbeat and InstallSnapshot from a leader
	// added while it was behind, permanently wedging itself out of the shard.
	// The removed set is monotone and is the only membership-derived fact that
	// is safe to enforce here; see raft.Peer.IsRemoved. Authentication proper is
	// the transport's mTLS node certificate plus the deployment ID.
	//
	// It is asked TWICE, of two different facts, because one of them the sender
	// chooses and the other it cannot:
	//
	//  1. The CLAIMED msg.From. Cheap, and it still catches the honest case — a
	//     removed replica that has not yet learned of its own removal keeps
	//     addressing the shard under its real ID.
	//  2. The AUTHENTICATED peer.Identity. This is the one an attacker cannot
	//     step around. A removed replica still holds its private key, so it
	//     authenticates normally, and under the claimed-From check alone it
	//     evaded its own tombstone by simply naming a different replica ID.
	//     Closing that needed the removed member's identity RETAINED as
	//     replicated state rather than deleted with the member — which is what
	//     proto.Membership.RemovedIdentities now is, persisted alongside every
	//     other membership fact so it survives a snapshot restore and a log
	//     replay identically.
	//
	// Both are monotone deny-lists over committed state, so neither can wrongly
	// refuse a legitimate peer; see raft.Peer.IsRemoved and IsRemovedIdentity.
	//
	// A member the configuration holds no identity for cannot be evaluated by
	// check 2 and is refused by the sender binding below (dropMissingIdentity);
	// admission (config.Config.RequireMemberIdentities) is what keeps such a
	// member out of the configuration in the first place.
	//
	// A nil node (shard not loaded locally) passes the two tombstone checks
	// and is then dropped below: there is nothing to bind the sender to.
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
	// The PENDING tombstone: a RemoveNode this replica holds, committed but
	// above its applied configuration. It will be the committed tombstone the
	// moment it is folded, and a replica that restarted with it unfolded must
	// not admit the removed member meanwhile, by claimed ID or by keypair,
	// through the committed binding, a pending add below the removal, or the
	// anchor. Counted with the committed tombstones: same fact, one index
	// earlier. (An uncommitted removal is not consulted — see
	// raft.Peer.PendingRemoval.)
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

	// Destination check: a message addressed to a different replica of this
	// shard must be dropped, not stepped into the local raft state. Neither
	// engine.DeliverMessage nor raft.step compares msg.To against the local
	// replica ID, so misrouted or stale traffic (e.g. a replica ID reused after
	// removal, or a transport-level routing error) would otherwise be processed
	// as if it were addressed here. To == 0 is allowed: broadcast/local control
	// messages leave it unset.
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

	// A shard that is not loaded here has no configuration to bind a sender
	// to and no inbox to deliver into: the engine drops the message anyway.
	// Refused quietly — it is not an authorization failure, the shard is
	// simply not here.
	if node == nil {
		slog.Debug("dropped message for a shard that is not loaded",
			"shard", msg.ShardID,
			"from", msg.From,
			"type", proto.MessageTypeName(msg.Type),
		)
		return false
	}

	// Bind msg.From — the replica ID raft counts toward election and commit
	// quorum, installs as leader, appends proposals for, and answers votes to
	// — to the authenticated peer, for EVERY message type.
	//
	// From travels in-band and is otherwise trusted verbatim. mTLS authenticates
	// the HOST, not the replica ID, and neither gate above binds the two: the
	// deny-list inspects the CLAIMED From, so it does not even stop a replica
	// this shard has already REMOVED from asserting a live member's ID. Without
	// this check any host holding a valid cluster certificate can open a normal
	// mTLS connection and send ReplicateResp{From: 2}, ReplicateResp{From: 3},
	// ... Each is accepted and counted, so the leader advances commitIndex over
	// entries no follower holds — a State Machine Safety violation that loses
	// committed data at the next leader change — a forged RequestVoteResp wins
	// an election from a minority, a Heartbeat at a higher term installs the
	// sender as leader and steps the real one down (repeated, it holds the
	// shard leaderless), a RequestVote at a higher term forces an election, and
	// a forwarded Propose has the leader append a command a stranger wrote.
	//
	// The rule: a message is accepted only when the peer that sent it IS the
	// replica msg.From names, and there are exactly three ways to be that
	// replica (senderBinding decides, in this order):
	//
	//   1. The COMMITTED configuration names msg.From with a node identity,
	//      and the connection's authenticated identity is that identity.
	//   2. A config-carrying entry in THIS replica's own log — appended but not
	//      yet applied — admits msg.From with an identity, and the connection
	//      presents it. This is the lagging-replica case: a follower that holds
	//      the AddNode for the new leader but has not committed it must still
	//      admit that leader's Replicate and Heartbeat, or it can never catch
	//      up. It is safe because the entry reached this log through a sender
	//      bound by this same rule, so the pending configuration is as
	//      trustworthy as the committed one, and because it binds the IDENTITY
	//      the entry carries rather than a bare replica ID.
	//   3. The operator anchor — the Config.MemberIdentities the shard was
	//      started with — names msg.From with the identity the connection
	//      presents. A joiner has no configuration at all, so the anchor is
	//      the only thing that can say who may bring it one; an established
	//      replica that missed the AddNode for the leader now serving its
	//      shard is vouched for the same way, by a restart with a current
	//      map, rather than by a wipe.
	//
	// There used to be type-based exemptions — leader-originated pushes and
	// vote requests from a replica ID the configuration did not name were
	// admitted, so a lagging replica could be brought up to date — and they
	// were what let any certificate holder move a shard's term, install itself
	// as leader on followers, and have proposals appended (#197 B). The pending
	// configuration replaces them with a fact this replica holds itself.
	//
	// What this costs: a replica that never received the config change adding
	// the new leader — it was down when the change committed and the old
	// leader is gone — refuses that leader until a replica its configuration
	// DOES name leads, or it is restarted with a Config.MemberIdentities that
	// names the new leader (source 3). The shard itself has quorum without it
	// (the change committed on a majority); it is that replica's own liveness,
	// it is logged as unknown_member for as long as it lasts, and it was the
	// price of closing a hole that cost every shard its availability to any
	// certificate holder.
	//
	// It is deliberately NOT `committed == batch.SourceAddress`. SourceAddress
	// is the peer's own ADVERTISED address, and the certificate is matched on
	// the identity, not the address; a member that moves keeps its identity.
	//
	// The ways this can fail are logged and counted SEPARATELY (see the
	// drop-reason constants), because they mean different things: the ordinary
	// config-change window (unknown_member), a forged or stale identity
	// (identity_mismatch), and a configuration written without identities
	// (missing_identity). Reporting them as one number leaves an operator unable
	// to tell a rolling membership change from an attack.
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

	// The PRE-LEARN routing mapping, read before learning can mutate it, so
	// learnSourceAddress can tell a no-op from a genuine replacement. It is NOT
	// an authorization input — see the gate above.
	h.learnFromSource(node, msg, h.registeredAddress(msg.ShardID, msg.From), sourceAddress, peer)
	return true
}

// committedSenderBinding decides whether the peer that sent this message is the
// replica the shard's COMMITTED MEMBERSHIP calls replicaID.
//
// It is THE sender-binding predicate, and it is deliberately the only one:
// HandleMessage and HandleSnapshot both call it, so the two paths cannot drift
// apart again. They did — the message path was moved off the mutable address
// registry while the snapshot path was left resolving the same registry, which
// turned a poisoned routing entry into snapshot install rights and therefore
// into a rewrite of the very membership this predicate reads.
//
// There is one binding, and the COMMITTED CONFIGURATION decides it, never the
// sender: when the configuration records a node identity for replicaID, the
// connection's AUTHENTICATED identity must equal it, and that is the ONLY check
// performed. The address is routing metadata and is not an authorization input.
// This closes all three holes the address scheme had: a promiscuous certificate
// proves nothing about a public key it does not hold; two replicas at one
// address still have two keypairs; and — the liveness win — a member that
// recovers at a NEW address keeps its identity, so its quorum-bearing responses
// keep being counted with no config change at all. The address gate dropped
// them until a config change committed, which during quorum loss is never.
//
// A member the configuration records NO identity for is refused
// (dropMissingIdentity). There used to be a per-replica fallback to the old
// chain, replica ID -> committed address -> certificate, kept for shards
// bootstrapped before identities existed; admission now requires the identity
// on every host with a transport (config.Config.RequireMemberIdentities), so
// the only way to reach this branch is a configuration written by a build that
// did not, and the answer to that is wipe and re-bootstrap, not a weaker gate.
//
// A nil node — the shard is not loaded here — is always unbound. There is no
// committed membership to authorize against, and a quorum outcome can only ever
// answer a request a loaded shard sent, so there is nothing to let through.
//
// It returns the committed identity it judged (empty when the configuration
// holds none) and the REASON to refuse, empty when the sender is bound.
// Returning the reason rather than a bool is what keeps the log line, the
// metric label and the decision in one place for both callers; they used to be
// assembled separately at each drop site, and that is how the snapshot path
// came to be judged on weaker evidence than the message path.
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
	// Constant-time comparison is deliberately NOT used. Both values are
	// public: the committed one is replicated to every member and the
	// presented one is the hash of a public key the peer just transmitted in
	// its certificate. There is no secret to leak by timing, and a timing-safe
	// compare here would imply one exists.
	if peer.Identity != binding.Identity {
		return binding.Identity, dropIdentityMismatch
	}
	return binding.Identity, ""
}

// senderBinding decides whether the peer that sent a message or a snapshot IS
// the replica it claims to be, and reports the committed value it judged and
// the reason to refuse (empty when the sender is bound). It is THE sender
// predicate: HandleMessage and HandleSnapshot both call it, so the two paths
// cannot drift apart. They did once — the message path was moved off the
// mutable address registry while the snapshot path was left resolving it,
// which turned a poisoned routing entry into snapshot install rights.
//
// Three sources can vouch for a sender, consulted in this order and each
// binding the connection's authenticated IDENTITY, never an address:
//
//  1. The committed configuration (committedSenderBinding).
//  2. The pending configuration: a config-carrying entry in this replica's own
//     log, above the applied configuration, that admits the sender with an
//     identity (engine.Node.PendingMemberIdentity). See authorizeInbound for
//     why that is sound.
//  3. The operator anchor: the Config.MemberIdentities the shard was started
//     with, each entry bound to an identity (Host.anchorIdentity). A joiner
//     holds no configuration, so nothing else can say who may hand it one —
//     including who may send the bootstrap snapshot, the very message that
//     used to authorize itself by the membership it carried (#197 A). An
//     established replica that was down while a member was added and elected
//     holds neither a committed nor a pending entry for that leader, and the
//     anchor is how its operator vouches for it without wiping the replica.
//     The committed configuration must hold NO entry for the sender to reach
//     here: a named sender is judged on that entry and never falls through.
//
// A sender none of them names is refused (dropUnknownMember); a sender one of
// them names on a connection carrying a different identity is refused as
// identity_mismatch.
func (h *hostMessageHandler) senderBinding(node *engine.Node, shardID, from uint64,
	peer transport.PeerAuth) (string, string) {

	committed, reason := committedSenderBinding(node, from, peer)
	if reason != dropUnknownMember {
		return committed, reason
	}
	if node == nil {
		return "", dropUnknownMember
	}
	// The pending configuration and the anchor name senders the committed
	// configuration does not know. Neither may admit a keypair the committed
	// configuration binds to ANOTHER replica — that member would be sending
	// as a second ID — and the committed configuration is asked at the
	// moment of the question, so a member added after the last snapshot is
	// covered where a start-time check of the anchor could not see it.
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

// replicaIdentity implements transport.Config.ReplicaIdentity: the node
// identity this host's view of the shard binds to replicaID — the one
// senderBinding would accept FROM it — so outbound traffic is held to the same
// identity an inbound sender is. "" when the shard is not loaded here or
// nothing binds the replica, which the transport treats as no peer at all.
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

// boundIdentity returns the node identity senderBinding binds replicaID of
// shardID to, from the same sources in the same order: the committed
// membership when it names the replica (whose missing identity binds
// nothing), else a pending config change in the shard's own log, else the
// operator anchor. "" when none binds it.
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

// registeredAddress returns the address the routing registry currently holds
// for (shardID, replicaID), or "" when there is none or no registry is wired.
//
// This is the mutable routing cache, not an authorization source; see
// authorizeInbound.
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

// Drop reasons for countUnauthorizedDrop. They are metric LABEL values, so the
// set is closed and none of them is derived from anything a sender supplies.
//
// The two quorum-gate reasons are split because they mean opposite things to an
// operator. dropUnknownMember is the benign config-change window: msg.From is a
// replica this host's committed configuration does not name YET, which is the
// normal state of a replica between a config change committing elsewhere and
// replicating here — it rises and falls on its own. dropIdentityMismatch is a
// sender the configuration DOES name, arriving on a connection whose
// authenticated identity is not the one committed for it: a forged From, or a
// stale identity after a key rotation. One aggregate counter makes an attack
// indistinguishable from a rolling membership change.
//
// address_mismatch and shared_address were the address fallback's two
// failures. The fallback is gone (#197); the labels are not reallocated.
const (
	dropRemovedReplica = "removed_replica"
	// dropRemovedIdentity is kept apart from dropRemovedReplica because the two
	// mean very different things. dropRemovedReplica is usually benign: a
	// removed replica that has not yet learned of its removal, still addressing
	// the shard under its own ID. dropRemovedIdentity is a connection whose
	// AUTHENTICATED identity this shard removed, presenting some OTHER replica's
	// ID — there is no accident that produces that shape, so it is the label an
	// operator should alert on.
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

// warnDrop logs a per-message authorization drop, rate-limited. A batch
// carries up to proto.MaxMessagesPerBatch messages, so one frame from a peer
// the shard does not admit would otherwise write that many lines — each a
// synchronous write that serialises every other log call in the process (see
// transport.LogSampled). The sampled line reports how many drops it stands
// for; countUnauthorizedDrop still counts each one.
func (h *hostMessageHandler) warnDrop(msg string, args ...any) {
	n := h.dropsSinceLog.Add(1)
	if !transport.LogSampled(&h.dropLogStamp, dropLogInterval) {
		return
	}
	h.dropsSinceLog.Add(-n)
	slog.Warn(msg, append(args, "drops_since_last_log", n)...)
}

// countUnauthorizedDrop records a message this handler refused, when metrics
// are wired. Every drop site in the authorization path goes through it, with
// the same reason it logged, so the counters cannot silently diverge from the
// logs.
func (h *hostMessageHandler) countUnauthorizedDrop(shardID, from uint64, reason string) {
	if h.host != nil && h.host.metrics != nil {
		h.host.metrics.IncUnauthorizedMessageDropped(shardID, from, reason)
	}
}

// learnFromSource records the sender's address for the return path, for senders
// that ARE authorized members of a loaded shard, and only when sourceAddress is
// bound to the sender's verified certificate. Binding it here at the handler —
// rather than trusting the transport to have cleared a non-cert-valid
// SourceAddress — makes the handler the authoritative enforcement point, so a
// transport regression cannot let a peer teach the registry an address its cert
// does not cover. Unloaded shards (node == nil) do not learn an address: the
// arbitrary replicaID in From cannot be verified as a real member until the
// shard is loaded with its configured membership.
//
// What it writes is ROUTING state only. authorizeInbound binds quorum-bearing
// messages to the committed membership, never to this mapping, so a poisoned
// entry cannot admit a message — it can only misdirect what this host SENDS,
// which is a liveness problem (the real member is black-holed until a config
// change or its own later traffic corrects the entry), not an authorization
// bypass. That separation is deliberate: routing must stay learnable so a peer
// that moved is reachable again, and authorization must not be learnable at all.
//
// Learning still MUTATES this host's routing state, so it is gated more tightly
// than message acceptance — but not on membership alone, which would break the
// case the whole deny-list exists to serve. A replica joining a shard starts
// with an EMPTY configuration: it does not yet know the leader is a member, and
// if it cannot record the leader's address it cannot send its ReplicateResp
// back, so it never acks, the leader never advances its match index, and the
// join never completes.
//
// The rule that satisfies both:
//
//   - A replica the committed configuration NAMES, on a connection that has
//     PROVED it is that replica, may register and OVERWRITE its address. The
//     overwrite is required so a member that recovers at a new address becomes
//     reachable again, including during quorum loss when no leader can propagate
//     a config change — and an identity survives the move, so the proof is
//     available exactly when the overwrite is needed. See
//     routingOverwriteAuthorized.
//   - Any other sender that reached here — one the pending configuration or
//     the operator anchor vouched for, since senderBinding admits nothing
//     else — may FILL a mapping that does not exist yet, never replace one.
//     This is what lets a joiner record the leader it must ack, and what lets
//     the lagging or restarted replica the pending configuration and the
//     anchor exist for reply to the leader they admit: a leader admitted
//     inbound whose responses cannot be routed back never sees its match
//     index advance, so the replica can never commit the entry that would
//     name it, and the admission bought nothing.
//
// Only bound senders reach this function, so unbounded registry growth from
// invented replica IDs is not a concern here: every mapping written is for a
// replica some configuration this replica holds — committed, pending, or the
// anchor — has named.
func (h *hostMessageHandler) learnFromSource(node *engine.Node, msg *proto.Message,
	registered, sourceAddress string, peer transport.PeerAuth) {

	if node == nil || sourceAddress == "" || !peer.Authorizes(sourceAddress) {
		return
	}
	h.learnSourceAddress(msg.ShardID, msg.From, registered, sourceAddress,
		routingOverwriteAuthorized(node, msg.From, peer))
}

// routingOverwriteAuthorized reports whether the connection a message arrived on
// has proved it IS the replica this shard's COMMITTED configuration calls
// replicaID, to the standard required to REPLACE that replica's routing entry.
//
// The overwrite used to be keyed on node.IsMember(msg.From) — the CLAIMED id —
// which made it satisfiable by naming any real member. That was described as a
// black-hole corrected by the next committed config change, and the description
// understated it: the attacker holds a CA-valid certificate, so this host's
// outbound dial to the re-pointed entry SUCCEEDS, and it then receives the
// victim's share of this shard's Replicate traffic — committed log entries
// streamed to a replica the configuration never named. That is exfiltration
// from a shard the sender is not a member of, not a liveness dent.
//
// The test is therefore the same evidence the receive gate uses: the
// committed IDENTITY must equal the connection's authenticated one. This costs
// the recovered-at-a-new-address case nothing, because the identity is a
// property of the keypair and not of where the replica is running. A member
// the configuration holds NO identity for may not have its route replaced by
// anyone: the pre-identity rule let any certificate holder naming a member
// re-point that member's traffic at itself (#197), and admission now requires
// the identity, so such a member only exists in a configuration written by a
// build that did not.
func routingOverwriteAuthorized(node *engine.Node, replicaID uint64, peer transport.PeerAuth) bool {
	binding, ok := node.MemberBinding(replicaID)
	if !ok || binding.Identity == "" {
		return false
	}
	return peer.Identity == binding.Identity
}

// learnSourceAddress records (and, for a replica the committed configuration
// names, updates) the (shardID, replicaID) -> address ROUTING mapping.
//
// Overwrite exists for liveness: a node that crashes and recovers at a NEW
// address (ephemeral ports, container reschedule) must become reachable again.
// When recovery happens during quorum loss there is no leader to propagate the
// new address via a committed config change, so peers can only relearn it from
// this node's own inbound traffic — e.g. its RequestVote must teach voters where
// to send VoteResp. A no-overwrite policy here strands a recovered-at-new-address
// node for every message this host tries to send it.
//
// It authorizes nothing. Quorum-bearing messages are bound to the COMMITTED
// MEMBERSHIP (see authorizeInbound), which no inbound message can write, so the
// worst an authenticated peer achieves by claiming another replicaID's From here
// is to re-point that member's ROUTING at itself — a black-hole, corrected by the
// next committed config change or by the victim's own later traffic, and never a
// path to having a forged response tallied. A member that recovers at a new
// address is served in full: its responses are counted as soon as it is
// reachable, because a sender is bound by the node identity its certificate
// carries, not by the address membership records.
//
// Callers reach here only for a sender that is a member of a loaded shard
// (IsMember) or for a shard this replica is still joining, and the transport has
// already bound `address` to the sender's verified TLS certificate
// (handleMessageFrame clears any SourceAddress not covered by the peer cert's
// SANs), which learnFromSource re-checks. A peer can therefore only ever offer an
// address its own certificate authorizes.
//
// registered is the mapping authorizeInbound resolved for (shardID, replicaID)
// before this call, empty when there was none. It is an advisory snapshot used
// to skip a redundant store and to log a genuine replacement; the no-overwrite
// invariant below never depends on it.
func (h *hostMessageHandler) learnSourceAddress(shardID, replicaID uint64, registered, address string, mayOverwrite bool) {
	if address == "" || h.host == nil || h.host.registry == nil {
		return
	}
	// A sender the committed configuration does not name may establish a
	// mapping, but must never REPLACE one. That keeps first contact working for
	// a joining replica (which knows no members yet, and must record the
	// leader's address to ack at all) while denying any authenticated peer the
	// ability to re-point an established member's traffic at itself.
	//
	// RegisterIfAbsent, not Resolve-then-Register: as two operations the gap
	// between them lets two concurrent receives both observe no mapping and both
	// store, so the invariant above holds only when the receives are serialized.
	// LoadOrStore makes "establish but never replace" actually atomic — which is
	// why the `registered` snapshot is not consulted on this path.
	if !mayOverwrite {
		if existing, stored := h.host.registry.RegisterIfAbsent(shardID, replicaID, address); !stored && existing != address {
			slog.Warn("ignoring address change from a replica outside the configuration",
				"shard", shardID,
				"replica", replicaID,
				"registered", existing,
				"offered", address,
			)
		}
		return
	}
	// Steady state: the mapping already says what this message would write.
	// Skipping the store keeps every inbound message from a settled member off
	// sync.Map's write path, which is what the registry's read-biased design is
	// for. A concurrent writer that changed the entry since the snapshot was
	// taken is authoritative (it is a committed config change), so leaving its
	// value in place is the correct outcome, not a lost update.
	if registered == address {
		return
	}
	// Replacing an ESTABLISHED mapping re-points every future send for this
	// replica, so it is either a genuine recovery at a new address or a peer
	// black-holing that member. Both are rare and neither is visible anywhere
	// else, so log it: it no longer affects authorization, but it is the signal
	// an operator needs when a member goes silent for no apparent reason.
	if registered != "" {
		slog.Warn("replacing the registered address of a configured replica",
			"shard", shardID,
			"replica", replicaID,
			"registered", registered,
			"offered", address,
		)
	}
	h.host.registry.Register(shardID, replicaID, address)
}

// admitSnapshotSender applies every check a snapshot's sender must pass before
// anything of the transfer is kept, and returns the shard's loaded node. It
// runs twice for every transfer: from the stream header, through
// AuthorizeSnapshot, before the transport takes any receive slot or memory;
// and from the chunks — which the transport has bound to that header — in
// HandleSnapshot, before anything is persisted.
func (h *hostMessageHandler) admitSnapshotSender(shardID, replicaID, fromReplicaID, snapshotIndex uint64, peer transport.PeerAuth) (*engine.Node, error) {
	// Per-shard authorization, fail closed. The header and chunk fields
	// (including From) travel in-band and are NOT bound to the sender's mTLS
	// identity by the transport framing, so three independent checks are
	// required:
	//
	//  1. The target shard must be loaded locally. Without a loaded node
	//     there is no membership to authorize against; persisting the
	//     snapshot anyway would let any authenticated peer poison the
	//     LogDB state consumed by a later StartShard.
	//  2. The snapshot must be addressed to THIS replica.
	//  3. The claimed From must be bound to the verified TLS certificate of
	//     the connection the snapshot arrived on, through the node identity
	//     the shard's COMMITTED MEMBERSHIP assigns it — the identical predicate
	//     the message path applies, senderBinding.
	//
	// Gate 3 is the whole reason this is not a routing-registry lookup. A
	// snapshot REPLACES this replica's membership (raft.Peer.Handle adopts
	// Snapshot.Membership, and deliverSnapshotControl injects it OriginLocal so
	// the wire-origin refusal does not apply), which makes the snapshot channel
	// the one inbound path that can rewrite the authorization source itself.
	// Resolving the sender from the mutable registry made that a two-step
	// escalation: a peer sent an exempt message claiming the leader's From,
	// address learning re-pointed registry[shard, leader] at the peer, and the
	// snapshot that followed authorized against the entry the peer had just
	// written — replacing the state machine AND every committed address the
	// message-path gate reads. Committed membership is not writable that way.

	// The shard the drop is counted against. It stays 0 — unattributed —
	// until a loaded shard is found: before that the ID is whatever the
	// sender wrote, and counting it as a label would let any certificate
	// holder mint unbounded metric series by cycling shard IDs.
	var countedShard uint64
	reject := func(reason string) (*engine.Node, error) {
		return nil, h.refuseUnauthorizedSnapshot(countedShard, shardID, fromReplicaID, snapshotIndex, reason)
	}
	if h.engine == nil {
		return reject("no engine")
	}
	node := h.engine.GetNode(shardID)
	if node == nil {
		// Counted as an authorization failure even though a leader can hit it
		// in the gap between StopShard and StartShard: with no node loaded
		// there is no committed membership to authenticate the sender
		// against, so an unauthenticated probe and a legitimate retransmit
		// are indistinguishable here, and the gate fails closed.
		return reject("shard not loaded")
	}
	countedShard = shardID
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
	//
	// Asked of both facts, exactly as authorizeInbound asks it: the CLAIMED
	// sender ID, and the AUTHENTICATED identity that a removed replica cannot
	// change by renaming itself. It matters more here than anywhere else — a
	// snapshot REPLACES this replica's state machine and its committed
	// membership, so a removed replica admitted through the claimed-ID gap would
	// be rewriting the very configuration that records its removal.
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
	// The two refusals below are ordinary shard states, not authorization
	// failures: a leader retransmits to a failed or restarting replica as a
	// matter of course, so they are reported as such rather than through
	// reject's security log, metric and error. They come AFTER every
	// authorization gate above, so an unauthorized sender is still counted as
	// one whatever state the shard is in, and BEFORE anything is staged.
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
	// A stopped incarnation likewise adopts nothing more — and it may still
	// have an adopted-but-unrecorded install of its own in the commit
	// pipeline, whose retained directory this receive's publish step would
	// reclaim as an orphan moments before the record naming it lands. The
	// receive waits for the incarnation that follows: UnloadNode does not
	// return until the stopped one can no longer write LogDB.
	if node.IsStopped() {
		return notAccepting(ErrShardNotReady)
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
func (h *hostMessageHandler) HandleSnapshot(chunks []proto.SnapshotChunk, peer transport.PeerAuth) error {
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

	// Per-shard authorization, fail closed — the gates AuthorizeSnapshot
	// applied from the stream header before the transport took any receive
	// resource, re-checked from the chunks before anything is persisted.
	node, err := h.admitSnapshotSender(shardID, replicaID, fromReplicaID, snapshotIndex, peer)
	if err != nil {
		return err
	}
	// The snapshot REPLACES this replica's committed membership, and for a
	// joiner it is the only one it will ever hold. A binding in it for THIS
	// replica that is not the identity this host presents — the AddNode that
	// admitted the joiner carried a mistyped identity — would be adopted
	// silently, and every peer would then refuse the joiner's acks for the
	// life of the shard with nothing local saying why. It is the one binding
	// the host can check itself, so it is checked before anything is
	// persisted, as StartShard and applyMembershipRouting check it on their
	// paths. The remedy is the same as theirs: fix the keypair, or remove and
	// re-add the replica with the right identity.
	if own := h.host.NodeIdentity(); own != "" {
		if committed := first.Membership.Identities[replicaID]; committed != "" && committed != own {
			return h.refuseUnauthorizedSnapshot(shardID, shardID, fromReplicaID, snapshotIndex, fmt.Sprintf("the snapshot's membership binds this replica to identity %s "+
				"but this host presents %s; every peer would refuse it — remove and re-add the "+
				"replica with the identity it presents", committed, own))
		}
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
					// Carry the artifact's full description, not just its
					// position. The re-delivered install reaches the LogReader,
					// and the SEND path reads the LogReader's copy when this
					// replica ships the snapshot on: a Compression this message
					// drops becomes a follower recording NoCompression for a
					// snappy artifact, which its recovery then feeds to the
					// state machine as garbage.
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
	// (nil, nil) and stays plaintext as configured. The streamed encryption
	// below re-checks the seal and covers one that lands mid-receive.
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

	// Determine the snapshot directory via the engine node, so a received
	// snapshot lands in the SAME location the snapshot pool uses for saving,
	// garbage collection, and DEK-purge-floor scans. Using a hardcoded
	// <NodeHostDir>/snapshots/... path here instead would, for a per-shard
	// DataDir (which roots SnapshotDir() off the DataDir), write received
	// snapshots where GC and key-rotation purge never look — leaking them and
	// letting a purge remove a DEK the snapshot still needs.
	snapshotDir := node.SnapshotSaveDir(snapshotIndex)

	fsys := h.host.snapshotFsys()

	// STAGE, then publish with one rename. The receive must never write into
	// the canonical snapshot-<index> directory directly.
	//
	// Same-index re-sends are routine and supported — msg.Hint carries a
	// transfer sequence precisely to disambiguate them, and the dedup branch
	// above re-delivers from LogDB rather than refusing — so this code is
	// reached with a snapshot-<index> directory already on disk whenever LogDB
	// and the tree disagree about it: a previous receive that wrote the files
	// and then failed before SaveSnapshot, a crash whose cleanup never ran, or
	// a local save that created the directory but has not recorded it yet.
	// Opening snapshot.dat there with O_TRUNC destroyed that artifact before a
	// single received byte had been authenticated, and the failure path then
	// called RemoveAll on a directory this receive had not created — deleting a
	// complete snapshot on the strength of a transfer that failed.
	//
	// Staging removes both. The directory below is created by THIS receive, so
	// it is the only thing the failure path may remove, and the canonical name
	// appears only when a fully written, fsynced artifact is renamed onto it.
	// This is the discipline Host.ImportSnapshot already uses; see
	// engine.RecvStageDirPrefix for why the receive gets its own staged name
	// rather than the import one.
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

	// ownedDir is the directory this receive may discard on failure. It is the
	// staging directory until the publishing rename, and the canonical
	// directory afterwards — at which point ownership HAS been proved, because
	// the rename moved a directory this receive created onto it. It is never a
	// directory this receive merely found. committed is set once the artifact
	// is durably recorded, after which nothing here may remove anything.
	ownedDir := stageDir
	var committed bool
	// retain keeps an UNRECORDED artifact that raft may nonetheless have
	// adopted: a verdict of InstallAbandoned does not prove the install was
	// never stepped. It is left as an orphan for the publish step and the GC.
	var retain bool
	defer func() {
		if committed || retain {
			return
		}
		// Discarding the PUBLISHED directory is a write on the incarnation's
		// behalf, like the publish itself, and takes its write lease. A node
		// stopped since the publish may have an adopted install of its own
		// in flight in its commit pipeline whose record names this very path
		// (this receive replaced that artifact when it published); deleting
		// it now would leave the record naming nothing. So on a stopped node
		// the directory is kept, an orphan the next incarnation's receive or
		// GC reclaims once UnloadNode has settled the record either way.
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
			// And under the snapshot-directory lock, checking the record
			// first: a local save may have published and recorded this very
			// index while the receive awaited raft's verdict, and what sits
			// here is then the recorded artifact, not this receive's.
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
	// The Close is part of the durability chain, not cleanup, so its error is
	// RETURNED. Sync flushes the data this process wrote; Close is what surfaces
	// a deferred write failure the file system reported late (a full or quota-
	// exceeded volume, a network file system's delayed error), and on some
	// platforms it is the only place that error is ever delivered. Logging it at
	// Debug and continuing meant the very next steps — SaveSnapshot recording
	// this artifact as this replica's durable snapshot, and the RemoveEntriesTo
	// that DELETES the log entries the snapshot subsumes — ran over a snapshot
	// file that may be short. The entries are then gone and the snapshot cannot
	// replace them. Returning here lands on the deferred RemoveAll (committed is
	// still false), so the partial directory is torn down and the leader re-sends
	// after its ack timeout. This is the same sync-then-close idiom syncDir and
	// snapshot_reencrypt.go use.
	if closeErr := f.Close(); closeErr != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "close",
			Err:       closeErr,
		}
	}

	// Measure the artifact AFTER re-encryption: FileSize must always mean
	// "size of the file at Filepath", never the plaintext wire size. A zero
	// FileSize on a record with a non-empty Filepath makes this replica
	// stream an EMPTY snapshot once it becomes leader, silently erasing the
	// receiving follower's state.
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

	// Write snapshot metadata file. It records the RECEIVER's own at-rest epoch
	// and the classified compression, so the directory describes itself even
	// without its LogDB record — and so the DEK purge floor, which is computed
	// by scanning these directories, sees the epoch this artifact needs.
	if err := writeReceivedSnapshotMetadata(fsys, stageDir, snapshotIndex, snapshotTerm,
		shardID, replicaID, recvEpoch, recvCompression); err != nil {
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "metadata",
			Err:       err,
		}
	}

	// Publish under this incarnation's write lease. A stopped incarnation may
	// still have an adopted-but-unrecorded install of its own at this index in
	// the commit pipeline, retained at this very path; reclaiming it as an
	// orphan below while that record lands would leave LogDB naming a
	// directory this receive replaced — or, once this receive is declined,
	// deleted. The lease is refused once the node stops, and UnloadNode waits
	// for a lease already held, so the publish finishes before any new
	// incarnation can exist.
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
		// Publish. The rename is what makes the canonical name appear, and it
		// appears already complete: data written, fsynced and closed, metadata
		// beside it.
		//
		// A destination that already exists here is provably NOT the snapshot LogDB
		// records. The dedup branch above returned for every existing.Index >=
		// snapshotIndex, so the recorded snapshot — if there is one — is at a LOWER
		// index and therefore under a DIFFERENT directory name. The guard is kept
		// anyway and fails closed, because it also covers the paths that reach here
		// without having consulted LogDB at all: a nil LogDB, or a read that
		// returned ErrNoSnapshot while the tree still holds the artifact. Deleting
		// the shard's live snapshot would leave LogDB pointing at nothing, which
		// makes the shard unstartable and makes every leader re-send hit the dedup
		// branch — a permanent wedge.
		// An UNREADABLE record fails closed as well: it might name this very
		// directory, which a local save can have published and recorded since
		// the dedup lookup above.
		// Nor one a recovery is reading: with no recorded path it falls back to
		// the newest directory on disk, which can be unrecorded.
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
	// Released when the receive returns — recorded, discarded or retained by
	// then. The lock is taken without a lease: this changes no directory and
	// no record, so an unload need not wait for it, and it waits for nothing
	// but the lock itself.
	defer func() {
		node.LockSnapshotDir()
		node.ReleaseSnapshotDir(snapshotIndex)
		node.UnlockSnapshotDir()
	}()
	// Ownership has transferred: the canonical directory IS the directory this
	// receive created, so the failure path may now discard it — and must, since
	// the staging name no longer exists.
	ownedDir = snapshotDir
	dataPath := filepath.Join(snapshotDir, "snapshot.dat")

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

	joining := h.host.isShardJoining(shardID)
	if joining {
		// The bootstrap snapshot carries the full membership, so the join
		// window closes once raft adopts it (below) and cannot reopen for the
		// life of the process. The Register is ROUTING only — it is how this replica reaches the
		// leader before any config change of its own commits, and the address
		// comes from the membership of a snapshot whose sender the join anchor
		// has just authenticated. It is expressly NOT what authorizes the
		// leader's retransmissions: those, like every later snapshot for this
		// shard, go through the committed membership the snapshot below is
		// about to install. Authorization stopped reading the registry
		// precisely because this write exists.
		if senderAddr := first.Membership.Addresses[fromReplicaID]; senderAddr != "" {
			h.host.registry.Register(shardID, fromReplicaID, senderAddr)
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
			// Deliver the RECEIVER's at-rest epoch (from the encryption above,
			// persisted to LogDB), NOT the wire epoch (first.Epoch is 0 —
			// the sender streamed plaintext). Peer.Commit applies this snapshot to
			// the LogReader; using first.Epoch would make the in-memory raft layer
			// advertise the encrypted on-disk snapshot as plaintext until restart,
			// so if this node became leader doSendSnapshot would skip decryption
			// and stream ciphertext that followers cannot recover.
			Epoch: recvEpoch,
			// The compression CLASSIFIED from the received bytes, and the
			// sender's on-disk cursor, both as persisted to LogDB just above —
			// so the LogReader's copy and the durable record describe the same
			// artifact.
			Compression: uint64(recvCompression),
			OnDiskIndex: first.OnDiskIndex,
		},
	}
	// Two-phase from here (whole-project review, M1). Nothing above made this
	// snapshot durable beyond its own directory: the LogDB record, the log
	// floor, reclaiming older directories and the state-machine recovery all
	// wait for raft's verdict on the install. Raft can still refuse an install
	// that passed every check above — its sender deposed since the stream
	// began, a position or membership out of bounds — or decline one it is
	// already past. Recording and recovering on DELIVERY left a refused
	// snapshot durably recorded, the log floor raised under it and a recovery
	// queued that the log could never catch up to, failing the shard.
	// Delivered to exactly the node the verdict is awaited on: by shard ID a
	// StopShard/StartShard during this receive would hand the install to the
	// NEW node, which adopts it while this receive waits on the old one.
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
		// No verdict will come, and that does NOT prove the install was never
		// stepped — a step in progress when the node stopped may have adopted
		// it, and raft's own record would then name this directory. So the
		// artifact is kept, never discarded: unrecorded, it is an orphan the
		// publish step and the GC reclaim; recorded, the shard recovers from
		// it on its next start.
		retain = true
		return &SnapshotReceiveError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Op:        "install",
			Err:       ErrSystemBusy,
		}
	}
	// Adopted. Record it under the adopting incarnation's write lease: the
	// LogDB record, the log floor and the GC below must not land after
	// UnloadNode has returned and a new incarnation has loaded LogDB — its
	// raft never adopted this snapshot, and its log reader would have entries
	// removed underneath it. A node stopped since its verdict is treated as
	// abandoned: its own commit pipeline records the adoption or drops it,
	// and either way the artifact is kept.
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
			// Carry the full membership unconditionally, including the
			// replicated witness policy, the committed node identities and the
			// replicated session policy: gating on Addresses/ConfigChangeID
			// would discard a policy-only membership, and dropping any of the
			// replicated fields would persist this snapshot with state that
			// disagrees with every peer after a restart — a zeroed witness
			// quorum, an identity-free configuration authorized by address
			// alone, or an unset session policy whose apply path falls back to
			// this node's own Config.
			//
			// This is the ONLY membership a replica bootstrapped by an
			// InstallSnapshot has: its log has been compacted past the
			// EntryInitialMembership that would otherwise carry these.
			membership := logdb.Membership{
				ConfigChangeID:             first.Membership.ConfigChangeID,
				MaxWitnesses:               first.Membership.MaxWitnesses,
				RequireFullReplicaMajority: first.Membership.RequireFullReplicaMajority,
				SessionExpiryEntries:       first.Membership.SessionExpiryEntries,
				MaxTotalSessions:           first.Membership.MaxTotalSessions,
				MaxSessionsPerClient:       first.Membership.MaxSessionsPerClient,
				MaxSessionsPerOwner:        first.Membership.MaxSessionsPerOwner,
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
				// Record the RECEIVER's at-rest epoch (from re-encryption above), not
				// the wire epoch (first.Epoch is 0 — the sender streamed plaintext).
				// recoverSnapshot decrypts under this node's barrier at this epoch.
				Epoch: recvEpoch,
				// Record the compression of the bytes actually on disk, classified
				// above, so recoverSnapshot builds its reader chain from the
				// artifact rather than from this node's own config.
				Compression: recvCompression,
			}
			// Raft has adopted this snapshot. On its restore branch the commit
			// worker records the same snapshot from raft's Update, and may have
			// done so already: a record at this index or later is this snapshot
			// recorded, not a failure. Any other failure cannot be answered by
			// rolling back — raft now depends on this artifact — so it fails the
			// shard, as a failed SaveState does; a restart replays the log, whose
			// entries below the snapshot are still in place.
			if saveErr := h.host.logdb.SaveSnapshot(shardID, replicaID, logdbSnap); saveErr != nil &&
				!errors.Is(saveErr, logdb.ErrSnapshotOutOfDate) {
				node.Fail(saveErr)
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

		// Reclaim the shard's superseded snapshot directories now that this one is
		// durably recorded, so retention is computed against the set INCLUDING it.
		//
		// The receive path creates one directory per InstallSnapshot and, until
		// this call, reclaimed none of them: saveSnapshot was the only caller of
		// the GC, and a shard with SnapshotEntries == 0 — a supported setting with
		// no default, so the out-of-the-box state — never runs saveSnapshot at all.
		// A follower that repeatedly fell behind the leader's compaction floor
		// therefore accumulated one full state-machine image per catch-up forever,
		// and every retained directory folds into the host-wide DEK purge floor, so
		// those images also stopped co-hosted shards from ever purging an epoch key.
		//
		// NON-FATAL for the same reason the log-floor advance above is: the
		// snapshot is recorded and the transfer succeeded. Failing to reclaim disk
		// costs space, and the next successful receive or local save runs the GC
		// again.
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

	// Trigger snapshot recovery on the engine node. The engine's snapshot
	// pool handles the actual SM.RecoverFromSnapshot call. This must not
	// be best-effort: raft now treats every entry at or below the snapshot
	// index as applied, so if recovery is never queued the state machine
	// silently misses the snapshot contents and diverges from its peers.
	// A concurrent auto-save briefly holds the snapshotting slot, so retry
	// until the slot frees; if the retry budget is exhausted, report the
	// transfer failed so the leader re-sends and the dedup branch above
	// re-arms recovery from the persisted snapshot.
	//
	// On the node that ADOPTED the install, not whichever node holds the shard
	// now: after a StopShard/StartShard in between, the new node's raft never
	// adopted this snapshot, so a recovery queued on it waits for a log that
	// will not reach the index and fails the shard. A recovery on an adopting
	// node that has since stopped is abandoned by the snapshot pool without
	// failing or reporting anything, and the next start recovers from the
	// record.
	if err := h.requestRecoveryWithRetry(shardID, replicaID, node); err != nil {
		return err
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

	validateErr := func(chunkID uint64, reason string) ([]*proto.SnapshotChunk, error) {
		return nil, &SnapshotReceiveError{
			ShardID:   first.ShardID,
			ReplicaID: first.ReplicaID,
			Op:        "validate",
			Err:       &SnapshotChunkError{ChunkID: chunkID, ChunkCount: n, Reason: reason},
		}
	}

	// The position every chunk must agree on is checked ONCE, on chunk 0,
	// before the agreement loop: a set that agrees on a position the protocol
	// cannot do arithmetic on is refused as one defect, not as the same defect
	// once per chunk. It is the sentinel the bound is defined with, wrapped in
	// the validate error like every other refusal here, so errors.Is finds
	// proto.ErrLogIndexOutOfRange / proto.ErrTermOutOfRange one level down.
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
	// Stamp the integrity fields over the data file AS STORED. Both callers of
	// this function — the streamed receive path and Host.ImportSnapshot — have
	// already written and fsynced snapshot.dat by the time they get here, so
	// the bytes hashed are the bytes every later consumer reads.
	//
	// A hash failure FAILS the write, before the record is created. The
	// encoder always emits the integrity fields, so there is no record
	// "without" them: a zero hash would be stamped as fact, and every later
	// recovery would refuse the artifact after it had been recorded in LogDB
	// and the log below it compacted. Both callers write into an unpublished
	// staging directory and discard it on error, so the failure leaves no
	// recorded artifact; the leader re-sends, or the operator re-imports.
	//
	// The artifact is read through fsys, like every other step here, so the
	// hash is taken over the file this path actually wrote.
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
	// Write, sync, THEN close — and report whichever failed first, with the
	// close error reported when nothing before it failed. This is a write path:
	// its Close can be the only place a late write error is delivered, and this
	// record is what the DEK purge floor scans to learn the epoch the artifact
	// needs, so a silently truncated one suppresses key purging for the whole
	// barrier. The old deferred Close discarded that error by construction. The
	// close still runs on every path, exactly as the defer arranged; the ordering
	// is what changed. Mirrors fsys.go syncDir and snapshot_reencrypt.go.
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
