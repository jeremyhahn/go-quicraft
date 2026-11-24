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

// Callback receives engine worker notifications about applied entries, leadership, confirmed reads and snapshots; implementations must not block.
type Callback interface {
	// OnApplied is called by the apply worker after entries are
	// successfully applied to the state machine. entries and results
	// are parallel slices of equal length. MUST be non-blocking.
	OnApplied(shardID uint64, entries []proto.Entry, results []sm.Result)

	// OnCommitted is called after entries are persisted to the WAL and before apply, when NotifyCommit is enabled.
	OnCommitted(shardID uint64, entries []proto.Entry)

	// OnLeaderUpdated is called on a leader change, delivered by the listener dispatcher, with the (leaderID, term) at detection.
	OnLeaderUpdated(shardID uint64, leaderID uint64, term uint64)

	// OnLeaderReady is called when the current term's leader, as replicaID knows it, has committed an entry of that term.
	OnLeaderReady(shardID, replicaID, leaderID, term uint64)

	// OnReadyToRead is called when read index requests are confirmed by a heartbeat quorum.
	OnReadyToRead(shardID uint64, readyToRead []proto.ReadyToRead)

	// OnSnapshotCompleted is called after a snapshot save or recover completes; op says which.
	OnSnapshotCompleted(shardID uint64, index uint64, op SnapshotOperation, err error)

	// OnSnapshotRequestCompleted is called after OnSnapshotCompleted for a save with a non-zero RequestKey, with that key and the outcome.
	OnSnapshotRequestCompleted(shardID, requestKey, index uint64, err error)

	// OnLogCompacted is called right after OnSnapshotCompleted when a save compacted the log.
	OnLogCompacted(shardID, replicaID uint64, compaction LogCompaction)

	// OnCampaignLaunched is called when raft starts an election, delivered by the listener dispatcher.
	OnCampaignLaunched(shardID, replicaID, term uint64)

	// OnSnapshotRejected is called when the snapshot pool refuses or abandons a request, or rejects a failed recovery.
	OnSnapshotRejected(shardID, replicaID uint64, err error)

	// OnReadIndexBatched registers alias keys that complete with batchKey's ReadIndex; called before the batch is submitted.
	OnReadIndexBatched(shardID uint64, batchKey uint64, aliasKeys []uint64)

	// OnProposalRejected is called when the peer refuses a proposal batch before appending any of it.
	OnProposalRejected(shardID uint64, entries []proto.Entry, err error)

	// OnProposalFailed is called when proposals already handed to raft fail; their outcome is unknown.
	OnProposalFailed(shardID uint64, entries []proto.Entry, err error)

	// OnReadIndexFailed is called when a batched ReadIndex fails to submit; keys holds the batch and alias keys.
	OnReadIndexFailed(shardID uint64, keys []uint64, err error)

	// OnShardFailed is called when a shard's failure latch trips.
	OnShardFailed(shardID uint64, err error)
}

// SnapshotOperation identifies the snapshot pool operation a completion
// reports. The zero value is deliberately not an operation, so a completion
// that forgot to say which cannot pass for either.
type SnapshotOperation uint8

const (
	// SnapshotSave captured the state machine to a snapshot.
	SnapshotSave SnapshotOperation = iota + 1
	// SnapshotRecover restored the state machine from a snapshot.
	SnapshotRecover
)

// LogCompaction reports what a snapshot save's log compaction actually did.
type LogCompaction struct {
	// Index is the log index compaction ran through: the snapshot index less
	// CompactionOverhead. It is non-zero only when Reader is true.
	Index uint64
	// Reader reports that this save advanced the log reader's compaction marker to Index.
	Reader bool
	// LogDB reports that the LogDB also removed and compacted the entries
	// through Index. It is false when either step failed; the failure is
	// logged and the next snapshot retries.
	LogDB bool
}
