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

package engine

import (
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// Callback receives notifications from engine workers about
// applied entries, leadership changes, and confirmed read indices.
//
// IMPORTANT: All Callback methods are invoked synchronously from engine
// worker goroutines (step workers, apply workers, snapshot pool workers).
// Implementations MUST be non-blocking. Blocking a callback stalls the
// entire worker, preventing progress for all shards assigned to that
// worker. If an implementation must acquire locks, it should use
// fine-grained per-shard locks (not a global lock) and hold them for
// the minimum duration necessary.
//
// Implementations must be safe for concurrent use from multiple
// worker goroutines.
type Callback interface {
	// OnApplied is called by the apply worker after entries are
	// successfully applied to the state machine. entries and results
	// are parallel slices of equal length. MUST be non-blocking.
	OnApplied(shardID uint64, entries []proto.Entry, results []sm.Result)

	// OnCommitted is called by the commit worker after entries are
	// persisted to the WAL but before state machine apply. Only
	// invoked when NotifyCommit is enabled on the host. Used to
	// deliver early WAL-durable notifications to proposal callers.
	// MUST be non-blocking.
	OnCommitted(shardID uint64, entries []proto.Entry)

	// OnLeaderUpdated is called by the step worker when a leader
	// change is detected for a shard. MUST be non-blocking.
	OnLeaderUpdated(shardID uint64, leaderID uint64, term uint64)

	// OnReadyToRead is called by the step worker when read index
	// requests have been confirmed by the leader via heartbeat quorum.
	// Each ReadyToRead carries a Key for correlation and the committed
	// Index at which the read is linearizable. MUST be non-blocking.
	OnReadyToRead(shardID uint64, readyToRead []proto.ReadyToRead)

	// OnSnapshotCompleted is called by the snapshot pool worker after a
	// snapshot save or recover operation completes. The index is the
	// snapshot log index on success, or 0 on error. MUST be non-blocking.
	OnSnapshotCompleted(shardID uint64, index uint64, err error)

	// OnCampaignLaunched is called by the step worker when the raft state
	// machine starts an election (pre-vote or real vote). This is a COLD
	// path -- elections are seconds apart. MUST be non-blocking.
	OnCampaignLaunched(shardID, replicaID, term uint64)

	// OnCampaignSkipped is called by the step worker when an election is
	// skipped because uncommitted config change entries exist between
	// committed and lastIndex. Elections during membership transitions
	// can violate the overlap guarantee. This is a COLD path.
	// MUST be non-blocking.
	OnCampaignSkipped(shardID, replicaID, term uint64)

	// OnSnapshotRejected is called when a snapshot request is rejected by
	// the snapshot pool (duplicate in-flight or pool full).
	// MUST be non-blocking.
	OnSnapshotRejected(shardID, replicaID uint64)

	// OnReadIndexBatched registers alias keys that should complete when the
	// batchKey's ReadIndex is confirmed. This enables multiple concurrent
	// ReadIndex callers to share a single raft ReadIndex operation and a
	// single heartbeat quorum round, reducing per-ReadIndex overhead from
	// O(N) heartbeat rounds to O(1). MUST be non-blocking.
	//
	// The step worker calls this BEFORE submitting the ReadIndex to raft
	// via Peer.Handle. This ordering ensures aliases are registered before
	// the raft layer can produce a readState (which triggers OnReadyToRead).
	// In single-node clusters, Handle(ReadIndex) resolves immediately;
	// registering aliases after Handle would create a race where
	// OnReadyToRead completes and deletes the batchKey before aliases
	// are attached, orphaning alias callers until context deadline.
	OnReadIndexBatched(shardID uint64, batchKey uint64, aliasKeys []uint64)

	// OnProposalFailed is called by the step worker when a batched proposal
	// is rejected by the Raft peer (e.g., NotLeaderError when the leader is
	// unknown, or ErrTransferInProgress). The entries slice contains the
	// failed proposals whose pendingProposals entries should be completed
	// with the given error. MUST be non-blocking.
	OnProposalFailed(shardID uint64, entries []proto.Entry, err error)

	// OnReadIndexFailed is called by the step worker when a batched ReadIndex
	// request fails to be submitted to the Raft peer. The keys slice contains
	// all ReadIndex keys in the batch (batch key + alias keys) whose
	// pendingReads entries should be completed with the given error. This
	// prevents orphaned pending reads that would otherwise block until
	// context deadline timeout. MUST be non-blocking.
	OnReadIndexFailed(shardID uint64, keys []uint64, err error)
}
