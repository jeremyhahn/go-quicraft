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
// # Callback guidelines
//
// This is the contract config.HostConfig's EventListener documentation refers
// to. It applies transitively: a Host implementation of this interface that
// fans a method out to a user EventListener inherits the method's rules.
//
// Every method MUST be non-blocking, and there are two reasons rather than one:
//
//  1. Throughput. A method invoked inline stalls its worker, and a worker owns
//     a whole partition of shards (shardID % numWorkers), so a slow
//     implementation stops unrelated shards from ticking, committing or
//     applying. If an implementation must lock, it should use fine-grained
//     per-shard locks, never a global one.
//  2. Reentrancy. Methods invoked INLINE run on a worker goroutine that already
//     holds engine state — the step worker holds stepWorker.nodesMu.RLock for
//     the whole of tick()/processReady(); a per-shard apply goroutine is what a
//     synchronous Engine.UnloadNode joins on. Calling back into a synchronous
//     Host API that unloads a shard (Host.StopShard) from one of those is a
//     PERMANENT deadlock, not a stall: sync.RWMutex is not reentrant, and a
//     goroutine cannot join itself.
//
// Which methods are inline is therefore part of the contract:
//
//   - Delivered by the listener dispatcher, on a goroutine holding no engine
//     lock, in FIFO production order: OnLeaderUpdated, OnCampaignLaunched, and
//     the node-level EventListener.OnShardFailed. These may call any Host API,
//     including StopShard. They are NOT ordered relative to the inline methods.
//   - Invoked INLINE on the producing worker: OnApplied, OnCommitted,
//     OnReadyToRead, OnProposalFailed, OnReadIndexBatched, OnReadIndexFailed,
//     OnShardFailed,
//     OnSnapshotCompleted, OnSnapshotRequestCompleted, OnSnapshotRejected,
//     and OnCampaignSkipped were it
//     ever invoked (it is not — see its declaration). These resolve
//     registered client requests and carry happens-before requirements with
//     raft.Peer.Handle (see OnReadIndexBatched), so they cannot be queued behind
//     user code. Implementations must not call back into a synchronous Host API
//     from them, and must not fan them out to user code without dispatching
//     that fan-out themselves.
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

	// OnLeaderUpdated is called when the step worker detects a leader change
	// for a shard, and is DELIVERED BY THE LISTENER DISPATCHER rather than on
	// the step worker. The (leaderID, term) pair is the state at detection
	// time. Deliveries are never dropped: a refused dispatch leaves the step
	// worker's published-leader bookkeeping uncommitted, so the change is
	// re-published on the next tick. MUST be non-blocking.
	OnLeaderUpdated(shardID uint64, leaderID uint64, term uint64)

	// OnLeaderReady is called when the step worker observes that the leader of
	// the current term, as replica replicaID knows it, has committed an entry
	// of that term: on the leader itself, from that moment a ReadIndex is no
	// longer refused with ErrReadIndexNotReady for the term; on a follower,
	// a ReadIndex it forwards to that leader is not either. See
	// raft.Peer.LeaderTermReady.
	//
	// Delivered by the listener dispatcher, ordered with OnLeaderUpdated: it
	// follows the OnLeaderUpdated(leaderID, term) that announced the same pair,
	// with no other OnLeaderUpdated for the shard in between, so the next
	// OnLeaderUpdated for the shard is the one that ends the readiness. It is
	// published at most once per announced (leaderID, term) and per loaded
	// replica, and like OnLeaderUpdated it is retried, never dropped, when the
	// queue is full. MUST be non-blocking.
	OnLeaderReady(shardID, replicaID, leaderID, term uint64)

	// OnReadyToRead is called by the step worker when read index
	// requests have been confirmed by the leader via heartbeat quorum.
	// Each ReadyToRead carries a Key for correlation and the committed
	// Index at which the read is linearizable. MUST be non-blocking.
	OnReadyToRead(shardID uint64, readyToRead []proto.ReadyToRead)

	// OnSnapshotCompleted is called by the snapshot pool worker after a
	// snapshot save or recover operation completes; op says which. For a save
	// the index is the snapshot log index, or 0 when the save wrote no
	// snapshot (nothing applied yet, or no progress since the last one) or
	// failed. For a recovery it is always 0. It is delivered while the shard's
	// snapshot slots are still held, so a shard's completions arrive in the
	// order its snapshots ran. It does NOT resolve the requester; see
	// OnSnapshotRequestCompleted. MUST be non-blocking.
	OnSnapshotCompleted(shardID uint64, index uint64, op SnapshotOperation, err error)

	// OnSnapshotRequestCompleted is called by the snapshot pool worker after
	// OnSnapshotCompleted (and OnLogCompacted) for a save that carried a
	// non-zero SnapshotRequestOption.RequestKey, with that key and the same
	// index and error. It is delivered AFTER the shard's snapshot slots are
	// released, so a requester resolved here can issue its next request at
	// once and have it admitted. It resolves only the request with that key,
	// so a request admitted after the release is never handed this result.
	// MUST be non-blocking.
	OnSnapshotRequestCompleted(shardID, requestKey, index uint64, err error)

	// OnLogCompacted is called by the snapshot pool worker, immediately after
	// OnSnapshotCompleted, when a snapshot save compacted the log. It is NOT
	// called when the save compacted nothing — the snapshot index was within
	// CompactionOverhead, or the log reader's marker could not advance — so a
	// listener is never told about a compaction that did not happen. MUST be
	// non-blocking.
	OnLogCompacted(shardID, replicaID uint64, compaction LogCompaction)

	// OnCampaignLaunched is called when the raft state machine starts an
	// election (pre-vote or real vote), and is DELIVERED BY THE LISTENER
	// DISPATCHER rather than on the step worker. This is a COLD path --
	// elections are seconds apart -- so a full notification queue drops it
	// (counted and logged at ERROR) rather than reporting the election at the
	// wrong time later. The election metric is incremented inline regardless,
	// so the counter stays exact. MUST be non-blocking.
	OnCampaignLaunched(shardID, replicaID, term uint64)

	// OnCampaignSkipped is RESERVED and is NEVER INVOKED. No code in this
	// package calls it, so an implementation of it receives nothing:
	// `git grep -n OnCampaignSkipped -- pkg/` finds only this declaration,
	// the Host forwarder and test doubles.
	//
	// It was intended to report a refused election. Every campaign-refusal
	// branch lives in pkg/internal/raft (startElection, handleTimeoutNow:
	// selfRemoved, roleMismatch, joining, an unresolved stale config tail,
	// an unreadable own-log term); that package holds no reference to this
	// Callback and has no path to reach it, so all of those log and return
	// silently. Wiring the hook needs a raft-to-engine notification path
	// that does not exist today.
	//
	// It stays on the interface so that path, if built, has a destination.
	// Do NOT build monitoring on it: the conditions it would report are
	// exactly the ones that leave a shard leaderless while quorum looks
	// intact, and silence here is indistinguishable from health. Use the
	// leader ID or shard health instead. Were it ever called it would be a
	// COLD path and MUST be non-blocking.
	OnCampaignSkipped(shardID, replicaID, term uint64)

	// OnSnapshotRejected is called when the snapshot pool refuses a request
	// (duplicate in-flight or pool full), abandons an accepted one (the pool
	// stopping, a save that panicked), or rejects a recovery that failed
	// before the state machine restore began. err is the cause.
	// MUST be non-blocking.
	OnSnapshotRejected(shardID, replicaID uint64, err error)

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

	// OnProposalRejected is called by the step worker when the Raft peer
	// refuses a batch of proposals BEFORE appending any of it to the log
	// (e.g. NotLeaderError when the leader is unknown, ErrTransferInProgress,
	// ErrPendingConfigChange): raft validates the whole batch before it
	// mutates anything, so none of these entries can ever commit. The
	// refusal is definite. The entries slice contains the proposals whose
	// pendingProposals entries should be completed with the given error.
	// MUST be non-blocking.
	OnProposalRejected(shardID uint64, entries []proto.Entry, err error)

	// OnProposalFailed is called when proposals that were already handed to
	// raft fail: a SaveState failure (entries the leader could not persist,
	// which it may already have replicated — Replicate is a free-order
	// message, sent before the local persist), a Peer.Commit or apply
	// failure of committed entries, or a committed batch discarded unapplied.
	// Every such entry may still commit and apply, here after a reload or on
	// the rest of the shard, so the outcome reported to its caller is
	// ambiguous. The entries slice contains the proposals whose
	// pendingProposals entries should be completed with the given error.
	// MUST be non-blocking.
	OnProposalFailed(shardID uint64, entries []proto.Entry, err error)

	// OnReadIndexFailed is called by the step worker when a batched ReadIndex
	// request fails to be submitted to the Raft peer. The keys slice contains
	// all ReadIndex keys in the batch (batch key + alias keys) whose
	// pendingReads entries should be completed with the given error. This
	// prevents orphaned pending reads that would otherwise block until
	// context deadline timeout. MUST be non-blocking.
	OnReadIndexFailed(shardID uint64, keys []uint64, err error)

	// OnShardFailed is called at the moment a shard's failure latch trips —
	// an SM.Apply error, a state-machine panic, an apply dispatcher panic, a
	// commit or save-state failure, a step-worker GetUpdate error, or a
	// snapshot recovery that failed or panicked — with the error that
	// tripped it. It fires once per halt, after OnProposalFailed has
	// completed the batch that caused it, when there was one.
	//
	// It exists for the requests the batch does not account for. A pending
	// linearizable read completes only when lastApplied reaches its read
	// index, and a halted shard never applies again, so without this every
	// read registered before the halt would block until its deadline and then
	// report ErrTimeout — pointing the caller at consensus, the network or a
	// lost leader, none of which was the cause. MUST be non-blocking.
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
	// Reader reports that THIS save advanced the log reader's compaction
	// marker to Index, so raft no longer reads the entries at or below it. A
	// marker an earlier snapshot, receive or import had already taken past
	// Index is not a compaction by this save, and is not reported.
	Reader bool
	// LogDB reports that the LogDB also removed and compacted the entries
	// through Index. It is false when either step failed; the failure is
	// logged and the next snapshot retries.
	LogDB bool
}
