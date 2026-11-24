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

// Sentinel errors for the raft package, compared with errors.Is; compaction and unavailability use logdb.ErrCompacted and logdb.ErrUnavailable.
var (
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

	// ErrNodeRemoved is returned when adding a node that was removed.
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

	// ErrReadIndexNotReady is returned for a ReadIndex before the leader committed an entry in its term (thesis 6.4).
	ErrReadIndexNotReady = errors.New("raft: read index not ready, leader has not committed entry in current term")

	// ErrPendingConfigChange is returned when a config change is proposed while another is uncommitted (thesis 4.1).
	ErrPendingConfigChange = errors.New("raft: pending config change not yet committed")

	// ErrDecompressionBomb is returned when a decompressed payload exceeds the maximum size; quicraft re-exports it.
	ErrDecompressionBomb = errors.New("quicraft: decompressed size exceeds limit")

	// ErrInflightsFull is returned when adding to a full inflights buffer, a caller flow-control bug.
	ErrInflightsFull = errors.New("raft: inflights buffer is full")

	// ErrTooManyVotingMembers is returned above 64 voting members, the width of the ReadIndex ackBits bitfield.
	ErrTooManyVotingMembers = errors.New("raft: cluster exceeds 64 voting members, ReadIndex quorum tracking requires ackBits to fit in uint64")

	// ErrTooManyWitnesses is returned when adding a witness would exceed the
	// shard's configured WitnessConfig.MaxWitnesses cap.
	ErrTooManyWitnesses = errors.New("raft: witness count would exceed configured MaxWitnesses")

	// ErrBreaksFullReplicaMajority is returned when a change would leave full replicas unable to form a quorum alone (WitnessConfig.RequireFullReplicaMajority).
	ErrBreaksFullReplicaMajority = errors.New("raft: config change would leave full replicas unable to form a quorum (set WitnessConfig.RequireFullReplicaMajority=false to allow)")

	// ErrReadIndexOverloaded is returned when pendingReadIndex reaches maxPendingReadIndex.
	ErrReadIndexOverloaded = errors.New("raft: read index overloaded, too many pending requests awaiting quorum confirmation")

	// ErrRemoveLastFullReplica is returned when attempting to remove
	// the last full (non-witness) voting replica. Witnesses cannot
	// become leaders, so at least one full replica must remain.
	ErrRemoveLastFullReplica = errors.New("raft: cannot remove last full replica, witnesses cannot become leader")

	// ErrGappedEntryBatch is returned when a Replicate batch is not contiguous with prevLogIndex or has internal gaps.
	ErrGappedEntryBatch = errors.New("raft: non-contiguous entry batch in append")

	// ErrZeroTermMessage is returned when a wire message that raft.send always stamps arrives with Term 0, which is reserved for local injection.
	ErrZeroTermMessage = errors.New("raft: protocol message carries no term")

	// ErrTermCeilingExceeded is returned for a wire term at or above maxAdoptableTerm.
	ErrTermCeilingExceeded = errors.New("raft: protocol message term is in the reserved ceiling range")

	// ErrWireLocalMessage is returned when an in-process-only message type (InstallSnapshot, SnapshotStatus, Unreachable, LeaderTransfer) arrives with proto.OriginRemote.
	ErrWireLocalMessage = errors.New("raft: process-local message type received from the wire")

	// ErrWitnessRoleMismatch is returned while the local witness role contradicts the replicated membership; elections and reads are refused until they agree.
	ErrWitnessRoleMismatch = errors.New("raft: local witness role contradicts the replicated membership")

	// ErrJoiningNodeCannotCampaign is returned when TimeoutNow reaches a node that is still joining.
	ErrJoiningNodeCannotCampaign = errors.New("raft: joining node cannot campaign with incomplete membership")

	// ErrLeaseVoteHoldDown is returned when TimeoutNow reaches a replica a leader may still count toward a read lease; retry after the window.
	ErrLeaseVoteHoldDown = errors.New("raft: vote withheld while a read lease may still count this replica")

	// ErrStaleConfigCannotCampaign is returned when TimeoutNow reaches a replica whose committed configuration tail is not yet folded into membership.
	ErrStaleConfigCannotCampaign = errors.New("raft: configuration tail unresolved, cannot campaign on a stale voting set")

	// ErrUnreadableLogCannotCampaign is returned when TimeoutNow reaches a replica that cannot read its own last log term; campaigning would only burn a term.
	ErrUnreadableLogCannotCampaign = errors.New("raft: local last log term unreadable, cannot campaign")

	// ErrDuplicateMemberAddress is returned when a change adds a replica at an address another active member holds.
	ErrDuplicateMemberAddress = errors.New("raft: address already held by another member of this shard")

	// ErrDuplicateMemberIdentity is returned when a change binds a node identity another active member holds.
	ErrDuplicateMemberIdentity = errors.New("raft: node identity already held by another member of this shard")

	// ErrMemberIdentityRequired is returned when a change admits a member without a node identity into a shard that requires one.
	ErrMemberIdentityRequired = errors.New("raft: a member must be admitted with its node identity")

	// ErrRemovedMemberIdentity is returned when a change binds a node identity this shard has removed; a removed replica rejoins only with a new keypair.
	ErrRemovedMemberIdentity = errors.New("raft: node identity belongs to a replica this shard has removed")

	// ErrReadIndexRespNotFromLeader is returned for a ReadIndexResp from a replica other than the current leader.
	ErrReadIndexRespNotFromLeader = errors.New("raft: read index response from a replica that is not the leader")

	// ErrReservedEntryType is returned when a Propose carries an entry type clients may not propose; the batch is rejected before any state change.
	ErrReservedEntryType = errors.New("raft: proposed entry carries a reserved entry type")

	// ErrHeartbeatNotFromLeader is returned for a Heartbeat from someone other than the leader already recognised for the term.
	ErrHeartbeatNotFromLeader = errors.New("raft: heartbeat from a replica that is not this shard's leader")

	// ErrSnapshotNotFromLeader is returned for an InstallSnapshot from someone other than the leader already recognised for the current term.
	ErrSnapshotNotFromLeader = errors.New("raft: install snapshot from a replica that is not this shard's leader")

	// ErrReadIndexContextNotIssued is returned when a HeartbeatResp echoes a ReadIndex context above any this leader issued.
	ErrReadIndexContextNotIssued = errors.New("raft: heartbeat response echoes a read index context this leader never issued")

	// ErrConfigChangeUnmarshal is returned at propose time when an EntryConfigChange payload does not decode.
	ErrConfigChangeUnmarshal = errors.New("raft: proposed config change payload does not decode")

	// ErrCorruptConfigEntry is returned when a committed config entry fails to decode on apply; the shard is faulted.
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

// NotLeaderError is returned for a proposal or read index on a non-leader; LeaderID and LeaderAddress allow a redirect.
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
