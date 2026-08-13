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

	// ErrStaleConfigCannotCampaign is returned when a TimeoutNow reaches a
	// replica whose committed configuration tail could not be folded into its
	// membership. Membership is config-in-log: a restart recovers a BASE
	// configuration and learns every later change by re-applying committed
	// entries, so until that tail is folded the replica's voting set is a
	// superseded one and any quorum computed over it is the wrong denominator.
	// The condition is transient — the fold is retried on the next election
	// timeout — and it only arises when the local log cannot be read.
	ErrStaleConfigCannotCampaign = errors.New("raft: configuration tail unresolved, cannot campaign on a stale voting set")

	// ErrDuplicateMemberAddress is returned when a config change would add a
	// replica at an address another ACTIVE member of the shard already holds.
	// The inbound authorization chain is replica ID -> committed address ->
	// certificate, so two replicas at one address let the single host behind it
	// speak as both: two votes and two match indices in a quorum that believes
	// they are independent. See membership.addressHolder.
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
