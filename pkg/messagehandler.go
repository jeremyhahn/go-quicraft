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

// nonQuorumBearingMessages is the allow-list of message types whose acceptance
// cannot hand a sender a QUORUM OUTCOME it did not earn. Every type NOT listed
// is gated, so a message type added later is bound to committed membership by
// default instead of silently escaping the sender binding HandleMessage applies.
//
// Membership is decided by what a single accepted message BUYS, not by
// direction. Each exemption below names the raft handler that proves it:
//
//   - Replicate (raft.handleAppend) and Heartbeat (raft.handleHeartbeat) are
//     leader-to-follower pushes. Both install a leader and can advance this
//     replica's commit index, which is why they are the most dangerous
//     exemptions on the list — but they are also the messages a lagging replica
//     needs from a leader its configuration does not name yet, and gating them
//     outright is the member allow-list that wedged replicas out of their own
//     shard. They are exempt from THIS gate and bound by the weaker
//     leader-originated one instead; see leaderOriginatedMessages.
//   - RequestVote and RequestPreVote (raft.handleVoteRequest) are
//     candidate-to-voter REQUESTS. They are answered, never tallied; the grants
//     they induce are the responses, and those ARE gated. Exempt so a member
//     that recovered at a new address can still campaign during quorum loss,
//     when no config change can commit its new address.
//   - Quiesce (raft.handleQuiesceMessage) only changes tick intervals.
//   - Propose (stepLeader/stepFollower Propose case) and ReadIndex
//     (raft.handleLeaderReadIndex / handleFollowerReadIndex) are
//     follower-to-leader forwards of client work; RateLimit
//     (raft.handleLeaderRateLimit) is advisory follower state.
//
// Everything else is gated. The four tallied response types are the reason the
// gate exists: ReplicateResp and HeartbeatResp advance the match index and the
// ReadIndex ack count, RequestVoteResp and RequestPreVoteResp feed the vote
// tally. ReadIndexResp and TimeoutNow are gated for the same reason even though
// nothing counts them:
//
//   - ReadIndexResp (raft.handleReadIndexResp) is the WIRE ENCODING of a
//     completed heartbeat-quorum round. One accepted message resolves a pending
//     linearizable read at an index of the sender's choosing, and the
//     correlation key is a plain monotonic counter, so a forged response is a
//     stale read with no quorum ever run. It only ever answers a request this
//     replica addressed to a leader it took from the configuration, so binding
//     it to committed membership costs no first-contact liveness.
//   - TimeoutNow (raft.handleTimeoutNow) is the campaign entry point that
//     bypasses both PreVote and the CheckQuorum leader lease. It is
//     leader-originated by construction, so committed-membership binding is
//     exactly right, and repeated forged copies would otherwise hold a shard in
//     a permanent election storm.
//
// So is every process-local control type (InstallSnapshot, SnapshotStatus,
// Unreachable, LeaderTransfer), which a peer must never be able to send at all
// — raft refuses them from the wire, and gating them here refuses them one
// layer earlier.
var nonQuorumBearingMessages = map[uint64]struct{}{
	proto.Replicate:      {},
	proto.Heartbeat:      {},
	proto.RequestVote:    {},
	proto.RequestPreVote: {},
	proto.Quiesce:        {},
	proto.Propose:        {},
	proto.ReadIndex:      {},
	proto.RateLimit:      {},
}

// isQuorumBearing reports whether accepting msgType can hand the sender a
// quorum outcome — a tallied vote or ack, or the wire form of one — and
// therefore whether HandleMessage must bind msg.From to the authenticated peer
// before delivering it.
//
// Unknown and future types answer true. Failing closed costs at most a dropped
// message on a path raft ignores anyway; failing open costs a forged quorum.
func isQuorumBearing(msgType uint64) bool {
	_, exempt := nonQuorumBearingMessages[msgType]
	return !exempt
}

// leaderOriginatedMessages is the set of types only an ESTABLISHED LEADER ever
// sends. They are not tallied, so they are not quorum-bearing — but they are the
// two messages that install a leader and advance a follower's commit index, and
// leaving them with no sender binding at all made msg.From a value the sender
// simply chose.
//
// The residual that used to be recorded in authorizeInbound argued the exemption
// was bounded because impersonating a leader means beating raft's term and
// log-matching rules. That covers Replicate, which has a log-match bar. It does
// NOT cover Heartbeat, which carries no prevLogIndex, no prevLogTerm and no
// entries, and yet set r.leaderID and committed up to a Commit index the sender
// named. One frame from any holder of a CA-valid certificate — a node the shard
// had already REMOVED included, because the deny-list is evaluated on the
// CLAIMED From — committed a follower's minority-replicated tail, which a
// legitimate leader elected from the majority then truncates. That is a State
// Machine Safety violation, not a liveness inconvenience.
//
// So they are bound, but at a DIFFERENT strictness than the quorum-bearing
// types, and the difference is one specific verdict:
//
//   - The configuration NAMES msg.From: the full committedSenderBinding
//     applies — identity where one is committed, address otherwise. An
//     identity/address mismatch, a shared address, a missing identity under the
//     strict posture: all refused. This is the whole forgery surface, because
//     impersonating a member is what a forger needs to do. It costs no liveness
//     at all: a replica whose configuration names the sender already holds the
//     evidence, and a member that moved keeps its identity.
//
//   - The configuration does NOT name msg.From (dropUnknownMember): ADMITTED.
//     This is the lagging-replica case and the reason the exemption existed —
//     a follower legitimately has not yet applied the config change that added
//     the current leader, and refusing here is precisely the member allow-list
//     that round 10 turned into a permanently leaderless shard. What survives is
//     bounded on the raft side rather than here: raft.handleHeartbeat refuses a
//     From that is not the leader it already recognises for the term, and
//     advances the commit index only to an index whose entry carries the CURRENT
//     term — a fact this replica proved for itself when handleAppend log-matched
//     the entry in. An unnamed sender can therefore still cost a term bump and an
//     election delay, which is exactly what the RequestVote exemption below
//     already concedes to any certificate holder and cannot be closed without
//     giving up first contact.
//
// InstallSnapshot is leader-originated too and is NOT here: it is process-local
// on this path (the snapshot receiver injects it) and HandleSnapshot binds its
// sender through committedSenderBinding with no unknown-member allowance beyond
// the documented join window.
var leaderOriginatedMessages = map[uint64]struct{}{
	proto.Replicate: {},
	proto.Heartbeat: {},
}

// isLeaderOriginated reports whether msgType is one only an established leader
// sends, and therefore whether HandleMessage must bind msg.From to the
// authenticated peer whenever committed membership names msg.From at all.
func isLeaderOriginated(msgType uint64) bool {
	_, ok := leaderOriginatedMessages[msgType]
	return ok
}

// HandleMessage delivers each message in the batch to the engine's
// per-shard inbox and signals the step worker that work is available.
//
// The per-message authorization decision — deny-list, destination, sender
// binding, and the routing-address learning that follows it — lives in
// authorizeInbound. This is the loop and the delivery.
//
// Messages to unloaded shards are delivered without membership checks. This
// allows bootstrap messages and messages that trigger shard loading to flow
// through. The shard's raft state machine provides its own term-based and
// log-based rejection for unauthorized messages once the shard is loaded. A
// QUORUM-BEARING message is the exception and is refused outright when the shard
// is not loaded: it is only ever an answer to a request this replica sent, which
// a shard with no loaded node cannot have sent, and there is no committed
// membership to authorize it against.
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
	// RESIDUAL: on a shard whose members carry NO committed identities there is
	// nothing for check 2 to consult, and check 1 is evadable exactly as before.
	// No replicated fact exists to close it there — the configuration has nothing
	// to compare an authenticated connection against — so the remedy is
	// operational: commit identities (Config.MemberIdentities,
	// RequestAddNodeWithIdentity) and, once migrated,
	// TransportConfig.RequireMemberIdentity.
	//
	// When node is nil (shard not loaded locally) the message is allowed through
	// because it may be a bootstrap or shard-loading trigger.
	node := h.engine.GetNode(msg.ShardID)
	if node != nil && node.IsRemoved(msg.From) {
		slog.Warn("rejected message from removed replica",
			"shard", msg.ShardID,
			"from", msg.From,
			"type", proto.MessageTypeName(msg.Type),
		)
		h.countUnauthorizedDrop(msg.ShardID, msg.From, dropRemovedReplica)
		return false
	}
	if node != nil && node.IsRemovedIdentity(peer.Identity) {
		slog.Warn("rejected message from removed replica identity",
			"shard", msg.ShardID,
			"claimed_from", msg.From,
			"identity", peer.Identity,
			"type", proto.MessageTypeName(msg.Type),
		)
		h.countUnauthorizedDrop(msg.ShardID, msg.From, dropRemovedIdentity)
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
		slog.Warn("rejected misrouted message",
			"shard", msg.ShardID,
			"from", msg.From,
			"to", msg.To,
			"local_replica", node.ReplicaID(),
			"type", proto.MessageTypeName(msg.Type),
		)
		h.countUnauthorizedDrop(msg.ShardID, msg.From, dropMisrouted)
		return false
	}

	// Bind msg.From — the replica ID raft counts toward election and commit
	// quorum — to the authenticated peer, for the message types raft TALLIES.
	//
	// From travels in-band and is otherwise trusted verbatim. mTLS authenticates
	// the HOST (the node certificate's SANs cover its address), not the replica
	// ID, and neither gate above binds the two: the deny-list inspects the
	// CLAIMED From, so it does not even stop a replica this shard has already
	// REMOVED from asserting a live member's ID. Without this check any host
	// holding a valid cluster certificate can open a normal mTLS connection and
	// send ReplicateResp{From: 2}, ReplicateResp{From: 3}, ... Each is accepted
	// and counted, so the leader advances commitIndex over entries no follower
	// holds — a State Machine Safety violation that loses committed data at the
	// next leader change — and a forged RequestVoteResp wins an election from a
	// minority.
	//
	// The rule: a quorum-bearing message is accepted only when the peer that
	// sent it is the replica the shard's COMMITTED MEMBERSHIP calls msg.From.
	// committedSenderBinding decides that, and the evidence it uses depends on
	// what the configuration recorded:
	//
	//   - A member with a committed NODE IDENTITY is bound to that identity —
	//     the SHA-256 of its leaf's public key, proved by the mTLS handshake.
	//     Nothing else is checked; the address is routing metadata.
	//   - A member with none falls back to the older chain, replica ID ->
	//     committed address -> certificate. config.TransportConfig's
	//     RequireMemberIdentity refuses that fallback outright for operators
	//     who have finished migrating.
	//
	// Every link but the last is consensus state either way. A replica the
	// configuration does not name at all is a drop, not a first contact: a
	// quorum outcome only makes sense as the answer to a request this replica
	// sent, and it addresses those from the configuration.
	//
	// It is deliberately NOT `committed == batch.SourceAddress`. SourceAddress
	// is the peer's own ADVERTISED address (AdvertiseAddress, else the bound
	// local address, else ListenAddress) while the committed address is what the
	// operator handed StartShard or RequestAddNode — two independently sourced
	// strings for one host, and a certificate legitimately carries several SANs,
	// so node5.example.com:5001 and 10.0.0.5:5001 can both be cert-valid and
	// unequal. String equality there drops every quorum-bearing message from
	// that member forever and presents as a network partition over a healthy
	// network. It also buys nothing: SourceAddress is a value the SENDER
	// chooses, so a forger simply claims the victim's address.
	//
	// LIVENESS: for a member with a committed identity there is NO address
	// coupling left, and the cost this gate used to carry is gone. A replica
	// that recovers at a new address — an ephemeral port, a rescheduled
	// container, a machine replaced — keeps its keypair, so its identity is
	// unchanged and its quorum-bearing responses keep being counted with no
	// config change at all. That matters most exactly when it is hardest: during
	// quorum loss no config change CAN commit, and under the address-only gate a
	// shard with a member back at a new address did not self-heal. For a member
	// with NO committed identity the old cost still applies, and it is one more
	// reason to commit identities.
	//
	// RESIDUAL, stated without overstatement. Binding to a committed identity
	// removes address-based From forgery, including by a wildcard-certificate
	// holder. It does NOT make committed membership unreachable from the wire.
	// What remains, after the leader-originated binding closed the largest hole:
	//
	//   - Replicate and Heartbeat no longer accept a From the configuration
	//     names but the connection cannot prove (see leaderOriginatedMessages),
	//     so leader IMPERSONATION is closed on a shard whose members carry
	//     committed identities. What is still admitted is a leader-originated
	//     push from a replica ID the configuration does not name AT ALL, which
	//     is the lagging-replica case the deny-list model exists to serve. Such
	//     a sender can still make this replica adopt a higher term and name it
	//     leader for one election timeout — an availability cost, not a safety
	//     one: raft.handleHeartbeat will not advance the commit index onto a log
	//     tail from an older term, and raft.handleAppend still has to satisfy
	//     log matching. The same term-bump is reachable through the RequestVote
	//     exemption, which cannot be closed without breaking campaigning during
	//     quorum loss, so this costs nothing that is not already conceded.
	//   - The removal deny-list is now evaluated on the AUTHENTICATED identity as
	//     well as the claimed From (see the two gates above), so a removed
	//     replica can no longer evade its tombstone by naming a different ID —
	//     the case that used to slip through was a leader-originated push under
	//     an ID the configuration does not name at all, which this class
	//     deliberately admits. What remains is the ADDRESS-ONLY shard: with no
	//     committed identities there is nothing for that gate to consult, and the
	//     claimed-From evasion is exactly as it was. No replicated fact can close
	//     it there; the remedy is to commit identities.
	//   - InstallSnapshot carries a full Membership that replaces the
	//     configuration wholesale. HandleSnapshot binds its sender through the
	//     SAME predicate as this one, so it is no longer the one-step rewrite it
	//     was; the join-window exception in snapshotSenderAddress is the
	//     remaining unauthenticated first contact, and it is address-based
	//     because a joining replica has no committed state to check against.
	//
	// The ways this can fail are logged and counted SEPARATELY (see the
	// drop-reason constants), because they mean different things: the ordinary
	// config-change window (unknown_member), a forged or stale identity
	// (identity_mismatch), an un-migrated member under the strict mode
	// (missing_identity), and the two address-path failures. Reporting them as
	// one number leaves an operator unable to tell a rolling membership change
	// from an attack.
	//
	// CONSIDERED AND REJECTED: a grace path admitting a msg.From present in the
	// PENDING (proposed but uncommitted) configuration, to shorten the
	// unknown_member window. Uncommitted entries are exactly the state that can
	// still be truncated away by a new leader, and they arrive by replication
	// from a peer — so authorizing against them reintroduces "the sender can
	// influence what authorizes it", which is the hole this gate closed. The
	// window is bounded by one round of replication and costs liveness only for
	// messages that raft would tally; waiting for the commit is the point.
	//
	// The leader-originated class (Replicate, Heartbeat) runs the SAME predicate
	// with exactly one verdict downgraded: dropUnknownMember is admitted, because
	// a replica whose configuration lags cannot yet name the leader that would
	// bring it up to date, and refusing there is the wedge round 10 produced.
	// Every other verdict — a configured replica the connection cannot prove it
	// is — is refused identically. See leaderOriginatedMessages.
	gated, leaderOriginated := isQuorumBearing(msg.Type), isLeaderOriginated(msg.Type)
	if gated || leaderOriginated {
		committed, reason := committedSenderBinding(node, msg.From, peer, h.requireMemberIdentity())
		// Written as "refuse, EXCEPT the one downgraded verdict" because that is
		// exactly what the comment above states and what the security argument
		// rests on: leader-originated traffic admits dropUnknownMember and
		// nothing else. De Morgan's equivalent, !leaderOriginated || reason !=
		// dropUnknownMember, reads as two independent permissions and hides that
		// the exception is a single named verdict.
		//nolint:staticcheck // QF1001: the negated form mirrors the documented single-verdict downgrade
		if reason != "" && !(leaderOriginated && reason == dropUnknownMember) {
			class := "quorum-bearing"
			if leaderOriginated {
				class = "leader-originated"
			}
			slog.Warn("rejected "+class+" message: "+unauthorizedDropDescriptions[reason],
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
	}

	// The PRE-LEARN routing mapping, read before learning can mutate it, so
	// learnSourceAddress can tell a no-op from a genuine replacement. It is NOT
	// an authorization input — see the gate above.
	h.learnFromSource(node, msg, h.registeredAddress(msg.ShardID, msg.From), sourceAddress, peer)
	return true
}

// requireMemberIdentity reports the host's configured posture for a member with
// no committed node identity: true refuses its quorum-bearing traffic outright,
// false falls back to the committed-address check. See
// config.TransportConfig.RequireMemberIdentity for what each costs.
//
// A handler with no host wired (unit-test paths) reports false, matching the
// zero value of the config field: the strict mode is an explicit operator
// decision, and inventing it here would make a fake host stricter than a real
// one configured identically.
func (h *hostMessageHandler) requireMemberIdentity() bool {
	if h.host == nil {
		return false
	}
	return h.host.cfg.TransportConfig.RequireMemberIdentity
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
// There are two answers it can give, and which one applies is decided by the
// COMMITTED CONFIGURATION, never by the sender:
//
//  1. IDENTITY (the durable binding). When the configuration records a node
//     identity for replicaID, the connection's AUTHENTICATED identity must equal
//     it, and that is the ONLY check performed. The address becomes routing
//     metadata and stops being an authorization input entirely. This closes all
//     three holes the address scheme had: a promiscuous certificate proves
//     nothing about a public key it does not hold; two replicas at one address
//     still have two keypairs; and — the liveness win — a member that recovers
//     at a NEW address keeps its identity, so its quorum-bearing responses keep
//     being counted with no config change at all. The address gate dropped them
//     until a config change committed, which during quorum loss is never.
//
//  2. ADDRESS (the compatibility binding). When the configuration records NO
//     identity — a shard bootstrapped before identities existed, or by an
//     operator who supplied none — the old chain applies: replica ID ->
//     committed address -> certificate. It is weaker in exactly the ways
//     described above, which is why RequireMemberIdentity exists to refuse it.
//
// The fallback is chosen per REPLICA, not per shard or per host, so committing
// an identity for one member hardens that member immediately without waiting
// for the rest.
//
// A nil node — the shard is not loaded here — is always unbound. There is no
// committed membership to authorize against, and a quorum outcome can only ever
// answer a request a loaded shard sent, so there is nothing to let through.
//
// An address the configuration does not hold EXCLUSIVELY is unbound on the
// address path, and reported as such. A certificate is matched on the address
// alone, so two active replicas at one address collapse that chain: the single
// host behind it authenticates as both. Validation refuses to create that
// configuration (raft.membership.addressHolder), but a shard seeded through
// StartShard or restored from an older snapshot can already be in it, so the
// receive path fails closed rather than assuming validation ran. The identity
// path does not consult exclusivity at all — it is not matched on an address,
// and duplicate identities are refused at config-change time
// (raft.ErrDuplicateMemberIdentity).
//
// It returns the committed value it judged (the identity when it judged one,
// otherwise the address; empty when there is neither) and the REASON to refuse,
// empty when the sender is bound. Returning the reason rather than a bool is
// what keeps the log line, the metric label and the decision in one place for
// both callers; they used to be assembled separately at each drop site, and
// that is how the snapshot path came to be judged on weaker evidence than the
// message path.
func committedSenderBinding(node *engine.Node, replicaID uint64,
	peer transport.PeerAuth, requireIdentity bool) (string, string) {

	if node == nil {
		return "", dropUnknownMember
	}
	binding, ok := node.MemberBinding(replicaID)
	if !ok || binding.Address == "" {
		return "", dropUnknownMember
	}
	if binding.Identity != "" {
		// Constant-time comparison is deliberately NOT used. Both values are
		// public: the committed one is replicated to every member and the
		// presented one is the hash of a public key the peer just transmitted
		// in its certificate. There is no secret to leak by timing, and a
		// timing-safe compare here would imply one exists.
		if peer.Identity != binding.Identity {
			return binding.Identity, dropIdentityMismatch
		}
		return binding.Identity, ""
	}
	if requireIdentity {
		return "", dropMissingIdentity
	}
	if !binding.Exclusive {
		return binding.Address, dropSharedAddress
	}
	if !peer.Authorizes(binding.Address) {
		return binding.Address, dropAddressMismatch
	}
	return binding.Address, ""
}

// snapshotSenderAddress resolves the address an inbound snapshot's claimed
// sender must be authenticated as, and reports the reason to reject when it
// cannot be. An empty reason means the returned address is authorized.
//
// The primary path is committedSenderAddress — the SAME predicate the message
// path applies, so a snapshot cannot be admitted on weaker evidence than a
// HeartbeatResp. That matters more here than anywhere else: the snapshot
// replaces both the state machine and the committed membership the message-path
// gate reads.
//
// The join-window exception is the only registry-free, membership-free path,
// and it is deliberately narrow. A shard started with join=true holds no
// configuration at all: the leader's bootstrap snapshot is the only thing that
// can deliver one, yet it is the very message being authorized. The address
// then comes from the membership the snapshot itself carries and must still be
// covered by the connection's VERIFIED peer certificate — the binding stays
// anchored to the authenticated QUIC peer, only the source of the address
// changes. Three conditions fence it in:
//
//   - The committed membership must hold NO address for the sender. A sender it
//     DOES name is judged on that address alone and never falls through here,
//     so the exception cannot be used to speak for a configured replica.
//   - The host must still be inside the join window, which closes for the life
//     of the process on the first accepted snapshot.
//   - Raft must independently agree the local configuration is incomplete
//     (Peer.IsJoining). The host flag and the raft flag are set together at
//     StartShard, and raft clears its own as soon as the configuration names
//     the leader — at which point the primary path succeeds and the exception
//     is not needed.
//
// What this does NOT do is stop the exception from adopting a membership: a
// snapshot accepted here is delivered OriginLocal and raft.Peer.Handle adopts
// its configuration unconditionally for a joining replica. Any host holding a
// valid cluster certificate can therefore be the one that bootstraps a joining
// shard, once, before any legitimate leader reaches it. Nothing available at
// this point can distinguish them: the replica has no committed state, and the
// operator supplied no expected-leader address to StartShard(join=true). The
// closures are an operator-supplied join anchor or replica-ID-bound
// certificates; both are out of scope here.
func (h *hostMessageHandler) snapshotSenderAddress(node *engine.Node, shardID, fromReplicaID uint64,
	claimedMembership proto.Membership, peer transport.PeerAuth) (string, string) {

	committed, reason := committedSenderBinding(node, fromReplicaID, peer, h.requireMemberIdentity())
	if reason == "" {
		return committed, ""
	}
	// Only "the configuration does not name this sender" falls through to the
	// join window. A sender it DOES name has been judged on its committed
	// address and is refused on that answer, so the exception can never be used
	// to speak for a configured replica or to sidestep a shared address.
	if reason != dropUnknownMember {
		return "", reason
	}
	claimed := claimedMembership.Addresses[fromReplicaID]
	if claimed == "" || !h.host.isShardJoining(shardID) || !node.IsJoining() {
		return "", dropUnknownMember
	}
	if !peer.Authorizes(claimed) {
		return "", dropAddressMismatch
	}
	return claimed, ""
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
// replicating here — it rises and falls on its own. dropAddressMismatch is a
// sender the configuration DOES name, arriving behind a certificate that does
// not cover its committed address: either a forged From, or a member whose
// address the operator changed without a config change. One aggregate counter
// makes an attack indistinguishable from a rolling membership change.
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
	dropAddressMismatch  = "address_mismatch"
	dropSharedAddress    = "shared_address"
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
	dropUnknownMember:    "the committed membership does not name this replica",
	dropAddressMismatch:  "the peer certificate does not cover this replica's committed address",
	dropSharedAddress:    "another active member claims the same committed address, so the certificate cannot tell them apart",
	dropIdentityMismatch: "the connection's authenticated node identity is not the one committed membership assigns this replica",
	dropMissingIdentity:  "committed membership records no node identity for this replica and RequireMemberIdentity is set",
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
//   - Otherwise an address is learned only while THIS replica's own
//     configuration is incomplete (it is still joining), and even then only to
//     fill a mapping that does not exist yet.
//
// The second clause is deliberately about the LOCAL replica, not the sender:
// the legitimate first-contact case is "I do not know who the members are yet",
// not "the sender claims to be someone". That keeps an established replica from
// recording addresses for arbitrary replica IDs a peer invents — unbounded
// registry growth — while still letting a joiner record the leader it must ack.
func (h *hostMessageHandler) learnFromSource(node *engine.Node, msg *proto.Message,
	registered, sourceAddress string, peer transport.PeerAuth) {

	if node == nil || sourceAddress == "" || !peer.Authorizes(sourceAddress) {
		return
	}
	if !node.IsMember(msg.From) && !node.IsJoining() {
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
// The test is therefore the same evidence the receive gate uses, minus the
// address fallback:
//
//   - A committed IDENTITY must equal the connection's authenticated one. This
//     is strictly stronger than the old rule and costs the recovered-at-a-new-
//     address case nothing, because the identity is a property of the keypair
//     and not of where the replica is running.
//   - NO committed identity leaves the pre-identity behaviour in place. The
//     alternative — matching the certificate against the COMMITTED address —
//     would refuse exactly the member that moved, and with it the RequestVote
//     that has to teach voters where to send the grant during quorum loss. The
//     residual is the old one (an address-only shard's routing is re-pointable
//     by any certificate holder naming a member) and its closure is committing
//     identities, which is also what config.TransportConfig.RequireMemberIdentity
//     exists to push.
func routingOverwriteAuthorized(node *engine.Node, replicaID uint64, peer transport.PeerAuth) bool {
	binding, ok := node.MemberBinding(replicaID)
	if !ok {
		return false
	}
	if binding.Identity == "" {
		return true
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
// path to having a forged response tallied. Note the recovered-at-new-address
// case this overwrite serves is now only half-served: the peer becomes reachable
// again, but its own quorum-bearing responses stay uncounted until a config
// change commits the new address.
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

	// Per-shard authorization, fail closed. All chunk fields (including
	// From) travel in-band and are NOT bound to the sender's mTLS identity
	// by the transport framing, so three independent checks are required
	// before anything is buffered or persisted:
	//
	//  1. The target shard must be loaded locally. Without a loaded node
	//     there is no membership to authorize against; persisting the
	//     snapshot anyway would let any authenticated peer poison the
	//     LogDB state consumed by a later StartShard.
	//  2. The snapshot must be addressed to THIS replica.
	//  3. The claimed From must be bound to the verified TLS certificate of
	//     the connection the snapshot arrived on, through the address the
	//     shard's COMMITTED MEMBERSHIP assigns it — the identical predicate
	//     the message path applies, committedSenderAddress.
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
	if h.host.registry == nil {
		return reject("no registry to record the sender address")
	}
	senderAddr, reason := h.snapshotSenderAddress(node, shardID, fromReplicaID, first.Membership, peer)
	if reason != "" {
		return reject(reason)
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
	if gcErr := engine.GCSnapshotDirs(node.SnapshotDir(), shardID, replicaID); gcErr != nil {
		slog.Warn("snapshot recorded but reclaiming superseded snapshot directories failed",
			"shard", shardID,
			"replica", replicaID,
			"snapshot_index", snapshotIndex,
			"error", gcErr)
	}

	if h.host.isShardJoining(shardID) {
		// The bootstrap snapshot carries the full membership, so the join
		// window closes here and cannot reopen for the life of the process.
		// The Register is ROUTING only — it is how this replica reaches the
		// leader before any config change of its own commits. It is expressly
		// NOT what authorizes the leader's retransmissions: those, like every
		// later snapshot for this shard, go through the committed membership
		// the snapshot below is about to install. Authorization stopped
		// reading the registry precisely because this write exists.
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

	buf := engine.MarshalSnapshotMeta(engine.SnapshotMeta{
		Index:       index,
		Term:        term,
		ShardID:     shardID,
		ReplicaID:   replicaID,
		Epoch:       epoch,
		Compression: compression,
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
