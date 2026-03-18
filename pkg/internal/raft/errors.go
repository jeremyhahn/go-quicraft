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

	// ErrReadIndexOverloaded is returned when the pendingReadIndex
	// queue reaches maxPendingReadIndex capacity. This occurs when
	// heartbeat responses are lost or delayed under sustained
	// ReadIndex load, preventing unbounded memory growth.
	ErrReadIndexOverloaded = errors.New("raft: read index overloaded, too many pending requests awaiting quorum confirmation")

	// ErrRemoveLastFullReplica is returned when attempting to remove
	// the last full (non-witness) voting replica. Witnesses cannot
	// become leaders, so at least one full replica must remain.
	ErrRemoveLastFullReplica = errors.New("raft: cannot remove last full replica, witnesses cannot become leader")
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
