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

package raft

import (
	"errors"
	"fmt"
)

// Sentinel errors for the raft package. These are pre-allocated and
// compared with errors.Is for zero-allocation on the hot path.
var (
	// ErrCompacted is returned when requested entries have been removed
	// by log compaction and are no longer available.
	ErrCompacted = errors.New("raft: entry compacted")

	// ErrUnavailable is returned when requested entries are not yet
	// available in the log (index beyond last entry).
	ErrUnavailable = errors.New("raft: entry unavailable")

	// ErrTransferInProgress is returned when a leadership transfer
	// is already in progress.
	ErrTransferInProgress = errors.New("raft: leadership transfer in progress")

	// ErrNodeNotFound is returned when the target node is not in the
	// current cluster membership.
	ErrNodeNotFound = errors.New("raft: target node not found in membership")

	// ErrEntropyUnavailable is returned when crypto/rand fails to
	// provide entropy for election jitter seeding.
	ErrEntropyUnavailable = errors.New("raft: crypto/rand entropy unavailable")

	// ErrRemoveLastVoter is returned when attempting to remove the
	// last voting member from the cluster.
	ErrRemoveLastVoter = errors.New("raft: cannot remove last voting member")

	// ErrNodeAlreadyExists is returned when attempting to add a node
	// that already exists in the membership.
	ErrNodeAlreadyExists = errors.New("raft: node already exists in membership")

	// ErrNodeRemoved is returned when attempting to add a node that
	// has been previously removed.
	ErrNodeRemoved = errors.New("raft: node was previously removed")

	// ErrInvalidConfigChangeType is returned when a config change has
	// an unrecognized type.
	ErrInvalidConfigChangeType = errors.New("raft: unrecognized config change type")

	// ErrEmptyAddress is returned when a config change that requires
	// an address has an empty address.
	ErrEmptyAddress = errors.New("raft: address required for add operations")

	// ErrZeroReplicaID is returned when a config change targets
	// replica ID zero.
	ErrZeroReplicaID = errors.New("raft: replica ID must be non-zero")

	// ErrTransferTargetNotVoter is returned when a leadership transfer
	// targets a witness or observer (non-voting member). Only full
	// voting replicas are eligible transfer targets.
	ErrTransferTargetNotVoter = errors.New("raft: transfer target is not a voting member")

	// ErrReadIndexNotReady is returned when a ReadIndex request is
	// received before the leader has committed an entry in its current
	// term. Per PhD thesis 6.4, the leader must commit its no-op
	// entry before serving linearizable reads.
	ErrReadIndexNotReady = errors.New("raft: read index not ready, leader has not committed entry in current term")

	// ErrPendingConfigChange is returned when a config change is
	// proposed while another config change entry is still pending
	// (not yet committed). Per PhD thesis 4.1, config changes must
	// be serialized.
	ErrPendingConfigChange = errors.New("raft: pending config change not yet committed")

	// ErrDecompressionBomb is returned when a decompressed entry payload
	// exceeds the configured maximum size, preventing zip-bomb attacks.
	ErrDecompressionBomb = errors.New("raft: decompressed size exceeds limit")

	// ErrInflightsFull is returned when attempting to add to a full
	// inflights buffer. Callers should check full() before calling
	// add(), so this error indicates a programming error in the
	// caller's flow control logic.
	ErrInflightsFull = errors.New("raft: inflights buffer is full")

	// ErrTooManyVotingMembers is returned when the cluster has more
	// than 64 voting members. The ReadIndex ackBits bitfield is a
	// uint64, so bit positions 0..63 are the maximum. Exceeding this
	// limit would cause bit position aliasing, breaking quorum checks.
	ErrTooManyVotingMembers = errors.New("raft: cluster exceeds 64 voting members, ReadIndex quorum tracking requires ackBits to fit in uint64")

	// ErrTooManyWitnesses is returned when adding a witness would exceed the
	// shard's configured WitnessConfig.MaxWitnesses cap.
	ErrTooManyWitnesses = errors.New("raft: witness count would exceed configured MaxWitnesses")

	// ErrBreaksFullReplicaMajority is returned when a config change — adding
	// a witness or removing a full replica — would leave the full
	// (data-bearing) replicas unable to form a quorum on their own, violating
	// WitnessConfig.RequireFullReplicaMajority. Such a membership lets commits
	// rest on witness acks with the payload on fewer full replicas, so it is
	// rejected unless the policy is explicitly disabled.
	ErrBreaksFullReplicaMajority = errors.New("raft: config change would leave full replicas unable to form a quorum (set WitnessConfig.RequireFullReplicaMajority=false to allow)")

	// ErrReadIndexOverloaded is returned when the pendingReadIndex
	// queue reaches maxPendingReadIndex capacity. This occurs when
	// heartbeat responses are lost or delayed under sustained
	// ReadIndex load, preventing unbounded memory growth.
	ErrReadIndexOverloaded = errors.New("raft: read index overloaded, too many pending requests awaiting quorum confirmation")

	// ErrRemoveLastFullReplica is returned when attempting to remove
	// the last full (non-witness) voting replica. Witnesses cannot
	// become leaders, so at least one full replica must remain.
	ErrRemoveLastFullReplica = errors.New("raft: cannot remove last full replica, witnesses cannot become leader")

	// ErrGappedEntryBatch is returned when a Replicate message carries an
	// entry batch that is not contiguous with the declared prevLogIndex or
	// contains internal index gaps. Accepting such a batch would corrupt
	// the in-memory log long before persistence-time gap detection fires.
	ErrGappedEntryBatch = errors.New("raft: non-contiguous entry batch in append")

	// ErrZeroTermMessage is returned when a protocol message that is only
	// ever produced by raft.send() arrives with Term == 0. Term 0 is the
	// local-injection escape hatch (the engine and the message handler
	// build Propose/ReadIndex/InstallSnapshot/SnapshotStatus/Unreachable/
	// LeaderTransfer without a term); on a wire message it would bypass
	// every term comparison in step().
	ErrZeroTermMessage = errors.New("raft: protocol message carries no term")

	// ErrTermCeilingExceeded is returned when a wire message names a term in
	// the reserved band at the top of the uint64 range. Adopting one would let
	// becomeCandidate's increment wrap the term to 0 — a regression that lets
	// this replica vote a second time in a term it has already voted in — or
	// strand it at a term it can never advance past. See maxAdoptableTerm.
	ErrTermCeilingExceeded = errors.New("raft: protocol message term is in the reserved ceiling range")

	// ErrWireLocalMessage is returned when a message type that is only ever
	// injected in-process (InstallSnapshot, SnapshotStatus, Unreachable,
	// LeaderTransfer) arrives stamped proto.OriginRemote, i.e. it was decoded
	// from the network. None of them is constrained by a term, a log match or
	// a quorum, so an authenticated peer that could inject one would be able
	// to replace this replica's membership, install a leader, or fabricate a
	// snapshot transfer outcome.
	ErrWireLocalMessage = errors.New("raft: process-local message type received from the wire")

	// ErrWitnessRoleMismatch is returned when this replica's LOCALLY
	// configured witness role contradicts the REPLICATED membership: the
	// configuration lists it as a witness while it was started with
	// IsWitness=false, or the reverse. The two roles run different data
	// paths — a leader strips entry payloads for witnesses — so a replica
	// on the wrong side of that split holds a state machine that never saw
	// the entry data, while every local gate keyed on the config believes
	// it is a full replica. Elections and reads are refused until the two
	// agree.
	ErrWitnessRoleMismatch = errors.New("raft: local witness role contradicts the replicated membership")

	// ErrJoiningNodeCannotCampaign is returned when a TimeoutNow reaches a
	// node that is still joining. Its membership is incomplete until a
	// snapshot delivers the bootstrap confState, so an election it wins
	// would be decided by a quorum of one.
	ErrJoiningNodeCannotCampaign = errors.New("raft: joining node cannot campaign with incomplete membership")

	// ErrLeaseVoteHoldDown is returned when a TimeoutNow reaches a replica
	// that a leader may still be counting toward a read lease: one inside its
	// post-restart vote hold-down, or one whose vote promise from a recent
	// heartbeat ack is still outstanding and was not made to the leader asking
	// for the transfer. A campaign opened now (a self-vote) could elect a
	// second leader inside that lease. The transfer can be retried once the
	// window has passed.
	ErrLeaseVoteHoldDown = errors.New("raft: vote withheld while a read lease may still count this replica")

	// ErrStaleConfigCannotCampaign is returned when a TimeoutNow reaches a
	// replica whose committed configuration tail could not be folded into its
	// membership. Membership is config-in-log: a restart recovers a BASE
	// configuration and learns every later change by re-applying committed
	// entries, so until that tail is folded the replica's voting set is a
	// superseded one and any quorum computed over it is the wrong denominator.
	// The condition is transient — the fold is retried on the next election
	// timeout — and it only arises when the local log cannot be read.
	ErrStaleConfigCannotCampaign = errors.New("raft: configuration tail unresolved, cannot campaign on a stale voting set")

	// ErrUnreadableLogCannotCampaign is returned when a TimeoutNow reaches a
	// replica whose OWN last log term cannot be read. campaign() refuses to
	// build a vote request in that state — advertising LogTerm 0 is rejected by
	// every peer holding a non-empty log — so a campaign started here would send
	// nothing at all. becomeCandidate runs FIRST though, and it increments the
	// term, so the round costs a term with no vote request behind it. That term
	// is not inert: send() stamps r.term on every reply this replica makes, so
	// it reaches the live leader through ordinary Replicate/Heartbeat rejections
	// and forces a step-down — one replica with a transient storage fault
	// deposing a healthy leader while emitting no election traffic of its own.
	// startElection refuses this; TimeoutNow is the OTHER campaign entry point
	// and needs the same bar.
	//
	// The condition is transient: the transfer target is retried by the leader's
	// transfer timeout, and an ordinary election timeout campaigns normally once
	// the read succeeds.
	ErrUnreadableLogCannotCampaign = errors.New("raft: local last log term unreadable, cannot campaign")

	// ErrDuplicateMemberAddress is returned when a config change would add a
	// replica at an address another ACTIVE member of the shard already holds.
	// Routing is per address: a replica is dialled at its committed address and
	// routes are learned, replaced and evicted by address, so two replica IDs at
	// one address would share one route and the transport's address-scoped
	// checks could not tell them apart. See membership.addressHolder.
	ErrDuplicateMemberAddress = errors.New("raft: address already held by another member of this shard")

	// ErrDuplicateMemberIdentity is returned when a config change would bind a
	// node identity another ACTIVE member of the shard already holds.
	//
	// The identity IS the authorization key the receive-side gate compares
	// against, so one identity naming two replica IDs collapses the gate the
	// same way one address naming two collapsed the address gate: the single
	// keypair behind it authenticates as both, and holds two votes and two
	// match indices in a quorum that believes they are independent. Unlike
	// addresses there is no co-location exception — two replicas on one machine
	// still need distinct keypairs. See membership.identityHolder.
	ErrDuplicateMemberIdentity = errors.New("raft: node identity already held by another member of this shard")

	// ErrMemberIdentityRequired is returned at propose time when a config
	// change would admit a member that carries no node identity into a shard
	// whose local policy requires one (config.Config.RequireMemberIdentities,
	// which the Host sets on every shard behind a transport). The receive-side
	// gate authorizes a member by its committed identity and by nothing else,
	// so an identity-less member would be one whose every message is dropped:
	// a voter counted in every quorum that can never be heard from.
	ErrMemberIdentityRequired = errors.New("raft: a member must be admitted with its node identity")

	// ErrRemovedMemberIdentity is returned when a config change would bind a
	// node identity this shard has already committed a REMOVAL for, under any
	// replica ID.
	//
	// The removal tombstone is on the IDENTITY, not on the replica ID it
	// happened to be bound to, and nothing evicts it (see
	// proto.Membership.RemovedIdentities). So a keypair whose replica was
	// removed is banned from this shard permanently: the receive-side deny-list
	// refuses its traffic whatever ID it presents. Letting such an add COMMIT
	// would produce a member every replica silently black-holes — a shard with a
	// voter that can never be reached, counted in every quorum. Refusing it at
	// propose time is the same fail-fast the duplicate checks make, and it is
	// validate-only for the same reason: refusing an already-committed entry on
	// the apply path would diverge replicas.
	//
	// The remedy is a NEW keypair. Recycling a decommissioned machine's ADDRESS
	// into a fresh replica ID stays legal (removed replicas do not count in
	// addressHolder); recycling its PRIVATE KEY does not, and that asymmetry is
	// deliberate — reusing a removed replica's key is precisely the capability
	// the tombstone exists to revoke.
	ErrRemovedMemberIdentity = errors.New("raft: node identity belongs to a replica this shard has removed")

	// ErrReadIndexRespNotFromLeader is returned when a ReadIndexResp arrives
	// from a replica that is not this shard's current leader. The message is
	// the wire form of a completed heartbeat-quorum round and its correlation
	// key is a monotonic counter, so accepting one from anywhere else resolves
	// a pending linearizable read at an index the sender chose, with no quorum
	// behind it. A follower only ever forwards ReadIndex to r.leaderID, so a
	// response from anyone else answers nothing this replica asked.
	ErrReadIndexRespNotFromLeader = errors.New("raft: read index response from a replica that is not the leader")

	// ErrReservedEntryType is returned when a Propose carries an entry whose
	// masked type is not one a client may propose. Only EntryNormal,
	// EntryConfigChange, and EntryEncodedEntry originate from legitimate
	// Host-side construction. EntryInitialMembership and EntryMetadata are
	// produced only internally (bootstrap seeding and the leader's witness
	// metadata path); a follower-forwarded wire Propose carrying one would
	// otherwise wedge membership changes (an injected EntryInitialMembership
	// keeps hasConfigChangeToApply true) or land a payload-less metadata entry
	// in the durable log. The batch is rejected before any state mutation.
	ErrReservedEntryType = errors.New("raft: proposed entry carries a reserved entry type")

	// ErrHeartbeatNotFromLeader is returned when a Heartbeat names a sender
	// that is not the leader this replica already recognises for the current
	// term. A Heartbeat carries NO log-matching proof yet installs a leader and
	// advances the commit index, so it is the one leader-to-follower push whose
	// authority rests entirely on the sender's identity. A replica that has
	// already accepted a leader for this term has no reason to accept a second
	// one at the same term — Election Safety says there is only one — so the
	// second is either a stale duplicate or a forgery, and either way it must
	// not move the commit index.
	ErrHeartbeatNotFromLeader = errors.New("raft: heartbeat from a replica that is not this shard's leader")

	// ErrSnapshotNotFromLeader is returned when an InstallSnapshot names a
	// sender that is not the leader this replica already recognises for the
	// current term.
	//
	// InstallSnapshot is the weakest-proof push in the protocol: it carries no
	// prevLogIndex/prevLogTerm, no entries and no log-matching test of any kind,
	// yet its non-matching branch REPLACES this replica's entire log and rebuilds
	// the voting set from the snapshot's membership. A Heartbeat, which can at
	// least only move the commit index, is already gated on sender identity for
	// exactly this reason (ErrHeartbeatNotFromLeader); the strictly more powerful
	// message was not.
	//
	// Election Safety guarantees one leader per term, so a second claimant at the
	// current term is a stale duplicate or a forgery. step() has already installed
	// msg.From as leader for a HIGHER term before the handler runs, and a LOWER
	// term never reaches it, so this gate concerns the current term only. A
	// replica with no leader yet (leaderID == noLeader) accepts, which keeps
	// first contact after an election and the candidate step-down path working.
	ErrSnapshotNotFromLeader = errors.New("raft: install snapshot from a replica that is not this shard's leader")

	// ErrReadIndexContextNotIssued is returned when a HeartbeatResp echoes a
	// ReadIndex correlation context greater than any this leader has issued.
	//
	// ReadIndex confirms leadership by counting heartbeat responses that echo the
	// context stamped on the confirming heartbeat round: a response carrying ctx X
	// proves the peer still regarded this replica as leader AFTER the read's index
	// was recorded, because the heartbeat bearing X was sent after it. The
	// confirmation rule is "ack every pending read whose ctx is at or below the
	// echoed one", so an echoed context ABOVE the counter — a value no heartbeat
	// ever carried — confirms every pending read at once from a single response,
	// including reads the leader has not yet broadcast a confirming heartbeat for.
	// Combined with the leader's own self-ack that is a quorum in a three-voter
	// shard, and the read is answered at a committed index no round ever
	// validated: a stale or deposed leader can then serve a linearizable read.
	ErrReadIndexContextNotIssued = errors.New("raft: heartbeat response echoes a read index context this leader never issued")

	// ErrConfigChangeUnmarshal is returned when a proposed EntryConfigChange
	// carries a payload that does not decode. It is a REJECTION, never a
	// pass-through: an undecodable payload cannot be validated, so admitting it
	// would put an entry into the log that no propose-time gate has judged,
	// while the apply path deliberately never re-validates (a committed change
	// must replay identically on every replica). This is a propose-time refusal
	// only; the commit path handles an undecodable committed entry separately.
	ErrConfigChangeUnmarshal = errors.New("raft: proposed config change payload does not decode")

	// ErrCorruptConfigEntry is returned when a COMMITTED config-carrying entry
	// fails to decode on the apply path. Recovery already treats these bytes as
	// fatal (ReconstructMembership), and the runtime must reach the same
	// decision: a committed entry is authoritative replicated state, so one this
	// replica cannot read leaves it computing quorum over a configuration that
	// differs from every peer's, silently and durably (the divergent membership
	// is persisted with the next snapshot). Faulting the shard is the only
	// outcome that does not manufacture a split configuration.
	ErrCorruptConfigEntry = errors.New("raft: committed config entry does not decode")
)

// DecompressError wraps an underlying Snappy decode failure with context
// about the decompression operation.
type DecompressError struct {
	Err error
}

// Error returns a human-readable description of the decompression failure.
func (e *DecompressError) Error() string {
	return fmt.Sprintf("raft: decompression failed: %v", e.Err)
}

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *DecompressError) Unwrap() error {
	return e.Err
}

// NotLeaderError is returned when a proposal or read index request is made
// on a non-leader node. LeaderID and LeaderAddress (if known) allow the
// caller to redirect the request to the current leader without an extra
// round-trip.
type NotLeaderError struct {
	LeaderID      uint64
	LeaderAddress string
}

// Error returns a human-readable description of the not-leader condition.
func (e *NotLeaderError) Error() string {
	if e.LeaderID == 0 {
		return "raft: not leader, no leader currently known"
	}
	return fmt.Sprintf("raft: not leader, current leader: %d (%s)",
		e.LeaderID, e.LeaderAddress)
}
