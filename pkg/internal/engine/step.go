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
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// commitItem carries a proto.Update and the owning Node from the step
// worker to the commit worker. The step worker uses per-shard pre-allocated
// slots to avoid heap allocation on the hot path.
//
// orderedMsgs holds messages that must be sent AFTER WAL persistence
// completes. Per Raft PhD thesis section 10.2.1, only free-order
// messages (Replicate, Ping, Heartbeat, Quiesce) may be sent before
// SaveState. All other messages (VoteResp, HeartbeatResp, etc.) are
// deferred here and sent by the commit worker after successful persist.
type commitItem struct {
	update      proto.Update
	node        *Node
	createdAt   time.Time       // for commit latency measurement
	orderedMsgs []proto.Message // messages to send AFTER WAL persist
}

// stepWorker processes Raft ticks and incoming messages, producing
// Updates that flow through the commit pipeline. Each step worker owns
// a fixed partition of shards (shardID % numWorkers == workerID).
//
// The nodes map is protected by nodesMu to support safe concurrent
// access from LoadNode/UnloadNode (caller goroutine) and the worker's
// run loop (tick/processReady). Reads use RLock and writes use Lock.
//
// When memoryMode is true (LogDB is nil), the step worker inlines
// Peer.Commit and forwards directly to applyC, bypassing the commit
// worker entirely. This eliminates one goroutine hop and two channel
// operations per update, reducing the pipeline from 3 hops (step →
// commit → apply) to 2 hops (step → apply).
//
// Message sending via sender.Send() is non-blocking. The transport
// layer provides per-target async send queues (buffered channels with
// dedicated sender goroutines), so the step worker never blocks on
// QUIC operations. This prevents heartbeat starvation and cascading
// multi-node stalls that would occur if transport sends blocked the
// tick/processReady loop.
type stepWorker struct {
	workerID          int
	tickInterval      time.Duration
	nodesMu           sync.RWMutex
	nodes             map[uint64]*Node
	sender            MessageSender
	workSignal        *WorkSignal
	commitSlots       map[uint64]*commitItem
	commitC           chan *commitItem
	callback          Callback
	prevLeaders       map[uint64]uint64 // shardID -> last known leaderID
	stopC             chan struct{}
	inboxBuf          []proto.Message            // reusable buffer for draining node inboxes
	proposeBuf        []proto.Entry              // reusable buffer for batching inbox proposals
	readKeyBuf        []uint64                   // reusable buffer for draining ReadIndex keys
	heartbeatRespBuf  []proto.Message            // reusable buffer for HeartbeatResp separation
	otherMsgBuf       []proto.Message            // reusable buffer for non-HeartbeatResp messages
	deferredSnapshots map[uint64][]proto.Message // per-shard deferred InstallSnapshot messages

	// freeOrderBuf and orderedMsgBuf are pre-allocated buffers for
	// splitting messages by persistence ordering in enqueueForCommit
	// and commitMemoryMode. Reused across calls to avoid the
	// msgs[:0:0] allocation pattern on every update.
	freeOrderBuf  []proto.Message
	orderedMsgBuf []proto.Message

	// Memory-mode fast path fields. When memoryMode is true, the
	// step worker calls Peer.Commit inline and forwards to applyC
	// directly, skipping the commit worker goroutine.
	memoryMode bool
	applyC     chan *applyItem
	applyPool  *sync.Pool

	// metrics collects raft-level metrics. Nil when disabled.
	metrics RaftMetrics
}

// newStepWorker creates a step worker for the given partition.
// tickInterval controls how often the worker advances the Raft logical
// clock for all loaded nodes. It should match the HostConfig.RTTMillisecond
// value so that election and heartbeat timeouts are measured in real
// round-trip time units rather than an arbitrary fixed period.
func newStepWorker(
	workerID int,
	tickInterval time.Duration,
	sender MessageSender,
	workSignal *WorkSignal,
	commitC chan *commitItem,
	callback Callback,
	stopC chan struct{},
	metrics RaftMetrics,
) *stepWorker {
	return &stepWorker{
		workerID:          workerID,
		tickInterval:      tickInterval,
		nodes:             make(map[uint64]*Node),
		sender:            sender,
		workSignal:        workSignal,
		commitSlots:       make(map[uint64]*commitItem),
		commitC:           commitC,
		callback:          callback,
		prevLeaders:       make(map[uint64]uint64),
		stopC:             stopC,
		inboxBuf:          make([]proto.Message, 0, 64),
		proposeBuf:        make([]proto.Entry, 0, 64),
		readKeyBuf:        make([]uint64, 0, 64),
		freeOrderBuf:      make([]proto.Message, 0, 16),
		orderedMsgBuf:     make([]proto.Message, 0, 16),
		deferredSnapshots: make(map[uint64][]proto.Message),
		metrics:           metrics,
	}
}

// run is the main loop for the step worker. It blocks until either a
// work notification, a tick, or a stop signal arrives. After waking,
// it non-blocking drains the ticker to ensure tick() runs at least
// once per processing cycle. This prevents tick starvation under
// heavy notification load: without the drain, the Go select statement
// picks between WaitC and ticker.C with equal probability, which
// under sustained proposal pressure causes the ticker case to be
// skipped for 10+ ms. That starves heartbeats, triggers leader
// checkQuorum failures, and causes multi-node proposal stalls.
//
// The tickInterval corresponds to one RTT, so Raft election and
// heartbeat timeouts (measured in ticks) map to real network latency.
//
// Message sending via sender.Send() is non-blocking because the
// transport provides per-target async send queues. This ensures ticks
// and inbox processing are never blocked by slow QUIC operations.
func (w *stepWorker) run() {
	ticker := time.NewTicker(w.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopC:
			return
		case <-w.workSignal.WaitC(w.workerID):
			w.workSignal.ClearNotified(w.workerID)
			w.drainTick(ticker)
			w.processReady()
		case <-ticker.C:
			w.tick()
			w.processReady()
		}
	}
}

// drainTick non-blocking consumes a pending tick if available. This
// ensures tick() runs at least once per processReady cycle even when
// the work notification path is taken. Without this, sustained
// proposal/transport load can starve the ticker case in the select
// statement, delaying heartbeats and causing leader step-down via
// checkQuorum.
func (w *stepWorker) drainTick(ticker *time.Ticker) {
	select {
	case <-ticker.C:
		w.tick()
	default:
	}
}

// tick advances the Raft logical clock for all loaded nodes.
//
// Tick runs unconditionally regardless of commitPending state. This is
// critical for leader liveness: the leader must always advance its
// heartbeat timer and send heartbeats to followers, even while WAL
// fsync and state machine apply are in progress. Without this, the
// commit+apply cycle (which can take 200-500us under load) starves
// heartbeat delivery, causing followers to timeout and trigger
// elections, which in turn causes the leader to fail checkQuorum and
// step down.
//
// The raftMu per-node mutex serializes Tick() with Commit() to prevent
// concurrent access to raft state fields. The mutex contention is
// minimal: Tick() is ~100ns (counter increment, conditional heartbeat
// broadcast) and Commit() is ~200ns (marker updates, inmem truncation),
// with both running at most once per RTT interval.
func (w *stepWorker) tick() {
	w.nodesMu.RLock()
	defer w.nodesMu.RUnlock()
	for _, node := range w.nodes {
		if node.IsFailed() || node.peer == nil {
			continue
		}
		node.raftMu.Lock()
		node.peer.Tick()
		// When commitPending is true, processReady skips this node.
		// But Tick may have produced heartbeat messages that must be
		// sent immediately to maintain leader liveness. Drain and
		// send only free-order messages inline. Tick during
		// commitPending produces only Heartbeat messages (free-order)
		// since the inbox is not processed. Filter defensively to
		// ensure ordered messages (e.g., VoteResp) are never sent
		// before WAL persist completes.
		if node.commitPending.Load() {
			if msgs := node.peer.DrainFreeOrderMessages(); len(msgs) > 0 {
				w.sendMessages(msgs)
			}
		}
		node.raftMu.Unlock()
	}
}

// processReady processes all loaded nodes, checking for pending work.
// Unlike the ready set (which only tracks external notifications from
// proposals and transport messages), this iterates ALL nodes because
// tick() can produce internal state changes (elections, heartbeats)
// that are invisible to the WorkSignal. Shards with commitPending=true
// skip inbox drain, HasUpdate, and GetUpdate to avoid extracting a
// second update while the previous one is still being committed/applied.
// Note: tick() runs unconditionally for all nodes (including
// commitPending ones) to maintain leader heartbeat liveness; only the
// full processReady work is deferred.
func (w *stepWorker) processReady() {
	// Swap the ready set to clear it. We iterate all loaded nodes
	// below rather than just the notified ones, because tick-driven
	// state changes (election timeouts, heartbeat intervals) are not
	// signaled through WorkSignal.
	_ = w.workSignal.Swap(w.workerID)
	w.nodesMu.RLock()
	defer w.nodesMu.RUnlock()
	for shardID, node := range w.nodes {
		if node.IsFailed() {
			continue
		}
		// When recovering is true, a snapshot recovery is in progress
		// on the snapshot pool goroutine. The state machine is being
		// replaced by RecoverFromSnapshot. Skip all pipeline operations
		// (inbox drain, GetUpdate, commit) for this node to prevent
		// concurrent state machine access. The snapshot pool will clear
		// the flag and notify the work signal when recovery completes.
		if node.IsRecovering() {
			continue
		}
		// When commitPending is true, the commit/apply pipeline owns the
		// Peer for write operations. However, incoming transport messages
		// (HeartbeatResp, ReadIndexResp) can satisfy pending ReadIndex
		// quorum checks. We drain the inbox and process messages under
		// raftMu (which serializes with Commit) to avoid delaying
		// ReadIndex completion by an entire commit+apply cycle (200us-1ms+).
		// Only ReadOnly updates (no entries, no state change, no snapshot)
		// are handled here; anything requiring the commit pipeline is
		// deferred to the next normal processReady cycle.
		if node.commitPending.Load() {
			if node.peer != nil {
				w.processCommitPendingInbox(shardID, node)
			}
			continue
		}
		// Drain proposals from the MPSC queue and transport messages from
		// the channel, then route them through the Raft peer before
		// checking for updates. Proposals are separated into proposalQ
		// (written by Host.Propose goroutines) and transport messages
		// remain in inboxC (written by transport delivery goroutines).
		if node.peer != nil {
			w.drainAndHandleProposals(shardID, node)
			// Update the per-shard rate limiter cache immediately after
			// proposals are appended to the in-memory log. On single-node
			// clusters, entries are committed and compacted in the same
			// processReady iteration, so the tick-based evaluation in
			// tickLeader() would always see an empty log. Evaluating here
			// captures the peak in-memory size before commit/apply drains it.
			node.peer.RateLimited()
			w.drainAndHandleReadIndex(shardID, node)
			w.redeliverDeferredSnapshots(shardID, node)
			w.drainAndHandleInbox(shardID, node)
			node.peer.FlushReadIndex()
		}
		lastApplied := node.lastApplied.Load()
		hasUpdate := node.peer != nil && node.peer.HasUpdate(lastApplied)
		if !hasUpdate {
			continue
		}
		update, err := node.peer.GetUpdate(true, lastApplied)
		if err != nil {
			node.handleError(err)
			continue
		}
		// Detect leader change, ReadyToRead, and campaign launch.
		w.processCallbacks(shardID, node, update)
		// Read-only updates (e.g., ReadIndex confirmations) carry no
		// entries, state changes, or snapshots. The ReadyToRead callback
		// above already delivered the results. Skip the commit/apply
		// pipeline to avoid unnecessary LogDB writes, goroutine hops,
		// and commitPending blocking. This reduces ReadIndex latency
		// from ~300us (3 goroutine hops) to ~5us (inline callback).
		if update.ReadOnly {
			if len(update.Messages) > 0 {
				w.sendMessages(update.Messages)
			}
			continue
		}
		// Memory-mode fast path: when no LogDB is configured, inline
		// Peer.Commit and forward directly to applyC. This eliminates
		// the commit worker goroutine hop (commitC send → commit worker
		// receive → Peer.Commit → applyC send), reducing latency by
		// ~1-2µs per update. No raftMu needed: both Tick and Commit
		// execute in this same goroutine (step worker), so they are
		// naturally serialized.
		if w.memoryMode {
			if w.commitMemoryMode(node, update) {
				return
			}
			continue
		}
		// WAL-mode pipeline: split messages by persistence ordering
		// and queue for the commit worker.
		if w.enqueueForCommit(shardID, node, update) {
			return
		}
	}
}

// redeliverDeferredSnapshots re-injects deferred InstallSnapshot messages
// from a previous commitPending cycle. These were buffered in
// deferredSnapshots instead of re-delivered to the inbox to prevent
// CPU-burning drain-redeliver loops during commitPending.
func (w *stepWorker) redeliverDeferredSnapshots(shardID uint64, node *Node) {
	deferred, ok := w.deferredSnapshots[shardID]
	if !ok || len(deferred) == 0 {
		return
	}
	for _, msg := range deferred {
		node.Deliver(msg)
	}
	w.deferredSnapshots[shardID] = deferred[:0]
}

// drainAndHandleInbox drains transport messages (Replicate, Heartbeat,
// ReadIndex, etc.) from inboxQ and handles them individually through
// the Raft peer.
func (w *stepWorker) drainAndHandleInbox(shardID uint64, node *Node) {
	w.inboxBuf = node.DrainInbox(w.inboxBuf)
	for i := range w.inboxBuf {
		var handleStart time.Time
		if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
			handleStart = time.Now()
		}
		if err := node.peer.Handle(w.inboxBuf[i]); err != nil {
			slog.Warn("peer.Handle failed",
				"shard_id", shardID,
				"replica_id", node.ReplicaID(),
				"msg_type", w.inboxBuf[i].Type,
				"msg_from", w.inboxBuf[i].From,
				"error", err,
			)
		} else if !handleStart.IsZero() {
			slog.Debug("message processed",
				"type", proto.MessageTypeName(w.inboxBuf[i].Type),
				"from", w.inboxBuf[i].From,
				"to", w.inboxBuf[i].To,
				"shard", shardID,
				"processing_ns", time.Since(handleStart).Nanoseconds(),
			)
		}
	}
}

// enqueueForCommit splits update messages by persistence ordering and
// queues the update for the commit worker. Per Raft spec (PhD thesis
// 10.2.1), free-order messages (Replicate, Ping, Heartbeat, Quiesce)
// are sent immediately to overlap QUIC transport with WAL fsync.
// Ordered messages (VoteResp, HeartbeatResp, etc.) are deferred to the
// commit worker and sent only after SaveState completes. Returns true
// if the step worker should exit (stopC fired).
func (w *stepWorker) enqueueForCommit(shardID uint64, node *Node, update proto.Update) bool {
	node.commitPending.Store(true)
	item := w.getCommitSlot(shardID)
	item.update = update
	item.node = node
	if w.metrics != nil {
		item.createdAt = time.Now()
	}

	// Split messages: free-order sent now, ordered deferred. Reuse
	// pre-allocated buffers to avoid the msgs[:0:0] allocation pattern.
	item.orderedMsgs = item.orderedMsgs[:0]
	if len(update.Messages) > 0 {
		w.freeOrderBuf = w.freeOrderBuf[:0]
		for i := range update.Messages {
			if proto.IsFreeOrderMessage(update.Messages[i].Type) {
				w.freeOrderBuf = append(w.freeOrderBuf, update.Messages[i])
			} else {
				item.orderedMsgs = append(item.orderedMsgs, update.Messages[i])
			}
		}
		if len(w.freeOrderBuf) > 0 {
			w.sendMessages(w.freeOrderBuf)
		}
	}

	select {
	case w.commitC <- item:
		return false
	case <-w.stopC:
		node.commitPending.Store(false)
		return true
	}
}

// drainAndHandleProposals drains batched proposals from the MPSC queue and
// routes them through the Raft peer as a single batched Propose message.
// Multiple concurrent Host.Propose calls accumulate entries in proposalQ.
// We swap once and handle them as a single batched Propose message, producing
// one log append and one broadcastAppend instead of N separate ones.
func (w *stepWorker) drainAndHandleProposals(shardID uint64, node *Node) {
	w.proposeBuf = node.DrainProposals(w.proposeBuf)
	if len(w.proposeBuf) > 0 {
		if w.metrics != nil {
			w.metrics.ObserveProposalBatchSize(shardID, len(w.proposeBuf))
		}
		slog.Debug("proposal batch processing",
			"shard", shardID,
			"replica", node.ReplicaID(),
			"batch_size", len(w.proposeBuf),
		)
		if err := node.peer.Handle(proto.Message{
			Type:    proto.Propose,
			Entries: w.proposeBuf,
		}); err != nil {
			slog.Warn("batched proposal failed",
				"shard_id", shardID,
				"replica_id", node.ReplicaID(),
				"num_entries", len(w.proposeBuf),
				"error", err,
			)
			// Notify the host so it can complete pending proposals
			// with the error instead of leaving callers blocked
			// until context deadline timeout.
			if w.callback != nil {
				w.callback.OnProposalFailed(shardID, w.proposeBuf, err)
			}
		}
	}
}

// drainAndHandleReadIndex drains batched ReadIndex requests from the MPSC
// queue and submits a single ReadIndex message using the first key as the
// batch key. Multiple concurrent Host.ReadIndex goroutines accumulate
// readKey values in readIndexQ. We drain once and submit a single ReadIndex
// message. All other keys in the batch are registered as aliases that
// complete when the batch key's ReadIndex is confirmed. This coalesces N
// ReadIndex callers into 1 raft ReadIndex operation + 1 heartbeat quorum
// round.
func (w *stepWorker) drainAndHandleReadIndex(shardID uint64, node *Node) {
	w.readKeyBuf = node.DrainReadIndex(w.readKeyBuf)
	if len(w.readKeyBuf) > 0 {
		batchKey := w.readKeyBuf[0]
		// Register batch aliases BEFORE submitting the ReadIndex
		// to raft. In single-node clusters, Handle(ReadIndex)
		// resolves immediately and produces a readState that
		// GetUpdate returns via ReadyToRead. If aliases were
		// registered after Handle, OnReadyToRead could fire
		// (via GetUpdate below) and complete the batchKey before
		// the aliases are registered, leaving alias callers
		// blocked until context deadline.
		if len(w.readKeyBuf) > 1 && w.callback != nil {
			w.callback.OnReadIndexBatched(shardID, batchKey, w.readKeyBuf[1:])
		}
		if err := node.peer.Handle(proto.Message{
			Type: proto.ReadIndex,
			From: node.ReplicaID(),
			Hint: batchKey,
		}); err != nil {
			slog.Warn("batched ReadIndex failed",
				"shard_id", shardID,
				"replica_id", node.ReplicaID(),
				"num_requests", len(w.readKeyBuf),
				"error", err,
			)
			// Complete all pending reads (batch key + aliases) with the
			// error. Without this, OnReadIndexBatched already registered
			// aliases, but no ReadyToRead callback will ever fire because
			// the ReadIndex message was never delivered to raft. The
			// pending RequestState objects would hang until context
			// deadline timeout.
			if w.callback != nil {
				w.callback.OnReadIndexFailed(shardID, w.readKeyBuf, err)
			}
		}
	}
}

// processCallbacks detects leader changes, delivers ReadyToRead notifications,
// and detects campaign launches. All notifications are delivered through the
// callback interface.
func (w *stepWorker) processCallbacks(shardID uint64, node *Node, update proto.Update) {
	// Detect leader change and notify callback.
	if w.callback != nil {
		currentLeader := node.peer.LeaderID()
		prevLeader := w.prevLeaders[shardID]
		if currentLeader != prevLeader {
			w.prevLeaders[shardID] = currentLeader
			w.callback.OnLeaderUpdated(shardID, currentLeader, update.State.Term)
		}
		// Notify confirmed read index requests.
		if len(update.ReadyToRead) > 0 {
			w.callback.OnReadyToRead(shardID, update.ReadyToRead)
		}
		// Detect campaign launch. Outbound RequestVote or
		// RequestPreVote messages indicate this node started an
		// election. The term is taken from the vote message itself
		// rather than update.State.Term because PreVote does not
		// increment the persistent term. This is a cold path
		// (elections are seconds apart) so the linear scan is
		// negligible.
		for i := range update.Messages {
			if update.Messages[i].Type == proto.RequestVote ||
				update.Messages[i].Type == proto.RequestPreVote {
				w.callback.OnCampaignLaunched(
					shardID, node.ReplicaID(), update.Messages[i].Term)
				if w.metrics != nil {
					w.metrics.IncElection(shardID)
				}
				break
			}
		}
	}
}

// commitMemoryMode handles the memory-mode fast path for a single update.
// It splits messages by ordering requirement (matching WAL mode's
// discipline), calls Peer.Commit inline, and forwards the update to
// applyC. Returns true if the caller (processReady) should return (stop
// signal received); returns false to continue the loop.
//
// Message ordering: free-order messages (Replicate, Heartbeat, Ping,
// Quiesce) are sent before Commit to overlap network I/O with Commit
// processing. Ordered messages (VoteResp, HeartbeatResp, etc.) are
// sent after Commit completes. Although memory mode has no WAL
// persistence and therefore no crash-recovery concern, applying the
// same ordering discipline as WAL mode ensures consistent behavior
// across modes and prevents sending RPC responses (e.g., VoteResp)
// before the corresponding state change (vote grant) is committed in
// memory. On Commit error, ordered messages are NOT sent because the
// state change they depend on was not applied.
func (w *stepWorker) commitMemoryMode(node *Node, update proto.Update) bool {
	// Split messages: send free-order before Commit, ordered after.
	// Reuse pre-allocated buffers to avoid allocation on every update.
	w.freeOrderBuf = w.freeOrderBuf[:0]
	w.orderedMsgBuf = w.orderedMsgBuf[:0]
	if len(update.Messages) > 0 {
		for i := range update.Messages {
			if proto.IsFreeOrderMessage(update.Messages[i].Type) {
				w.freeOrderBuf = append(w.freeOrderBuf, update.Messages[i])
			} else {
				w.orderedMsgBuf = append(w.orderedMsgBuf, update.Messages[i])
			}
		}
		if len(w.freeOrderBuf) > 0 {
			w.sendMessages(w.freeOrderBuf)
		}
	}
	if err := node.peer.Commit(update); err != nil {
		node.handleError(&CommitError{
			ShardID:   update.ShardID,
			ReplicaID: update.ReplicaID,
			Err:       err,
		})
		return false
	}
	// Send ordered messages after successful Commit.
	if len(w.orderedMsgBuf) > 0 {
		w.sendMessages(w.orderedMsgBuf)
	}
	node.commitPending.Store(true)
	ai := w.applyPool.Get().(*applyItem)
	ai.update = update
	ai.node = node
	select {
	case w.applyC <- ai:
		// Successfully queued for apply.
		return false
	case <-w.stopC:
		node.commitPending.Store(false)
		w.applyPool.Put(ai)
		return true
	}
}

// processCommitPendingInbox drains and processes inbox messages for a node
// whose commitPending is true. This allows HeartbeatResp and ReadIndexResp
// messages to satisfy pending ReadIndex quorum checks without waiting for
// the commit+apply pipeline to finish, which can take 200us-1ms+.
//
// ReadIndex Prioritization: HeartbeatResp messages (which confirm ReadIndex
// quorum) are processed FIRST, and ReadIndex completions are delivered
// IMMEDIATELY before processing new proposals. This ensures read latency
// is not blocked by proposal processing time. The processing order is:
//
//  1. New ReadIndex requests (register + submit to raft)
//  2. HeartbeatResp messages (may confirm pending ReadIndex)
//  3. Extract and deliver ReadIndex completions (outside lock)
//  4. Proposals (pipelining)
//  5. Other inbox messages
//
// InstallSnapshot messages are explicitly filtered out and re-delivered to
// the inbox. handleSnapshot triggers log.restore() which resets committed,
// processed, and entries. If the apply worker has in-flight entries from
// the current commit cycle, processing InstallSnapshot here would cause
// lastApplied to regress when the apply worker finishes its stale entries.
// InstallSnapshot is deferred to the normal processReady path which only
// runs when commitPending is false and the pipeline is fully drained.
//
// All inbox processing is done under raftMu to serialize with the commit
// worker's Peer.Commit() call. The critical section is kept minimal by
// extracting ReadIndex completions early and delivering callbacks outside
// the lock.
//
// Unlike processReady's normal path, this method uses DrainReadStatesOnly
// instead of the full HasUpdate/GetUpdate cycle. DrainReadStatesOnly extracts
// only readStates without touching entries, hard state, snapshots, or
// prevState. This avoids conflicts with the in-flight commit pipeline which
// owns those fields.
//
// Proposal Pipelining: Proposals ARE drained during commitPending to enable
// parallel pipeline throughput. While the previous update is being committed
// (WAL fsync + apply), new proposals are drained and appended to the Raft log.
// When commitPending clears, the next GetUpdate immediately captures all
// accumulated entries without waiting for another step cycle.
//
// This converts the serial pipeline (drain -> commit -> drain -> commit) into a
// parallel pipeline (drain while commit -> ready for next update). The raftMu
// lock serializes Handle(Propose) with Commit() to prevent data races.
//
// Performance impact: In 5N concurrent propose benchmarks, this eliminates
// the ~5-10ms blocking window where proposals queue in the MPSC but cannot
// progress, improving throughput from ~250 ops/s to ~1200+ ops/s. ReadIndex
// prioritization reduces read latency by ~4.5x under concurrent write load.
//
//nolint:gocyclo // pipeline optimization: processes proposals, heartbeats, reads, and snapshots during commit
func (w *stepWorker) processCommitPendingInbox(shardID uint64, node *Node) {
	// Drain all queued operations from MPSC queues. The actual processing
	// happens under raftMu to serialize with the commit worker.
	w.proposeBuf = node.DrainProposals(w.proposeBuf)
	w.readKeyBuf = node.DrainReadIndex(w.readKeyBuf)
	w.inboxBuf = node.DrainInbox(w.inboxBuf)
	if len(w.inboxBuf) == 0 && len(w.readKeyBuf) == 0 && len(w.proposeBuf) == 0 {
		return
	}

	// Filter out InstallSnapshot messages. These trigger log.restore()
	// which resets committed/processed/entries, conflicting with the
	// in-flight commit/apply pipeline. If the apply worker has entries
	// being applied and handleSnapshot runs concurrently, lastApplied
	// can regress (e.g., snapshot sets state to index 100, then apply
	// worker finishes old entries and stores lastApplied=7). Defer
	// InstallSnapshot to the normal processReady path which only runs
	// when commitPending is false and the pipeline is fully drained.
	//
	// InstallSnapshot messages are buffered in deferredSnapshots
	// (per-shard on the step worker) instead of re-delivered to the
	// inbox. Re-delivery to the inbox caused a CPU-burning loop: each
	// processReady cycle drained the same message and put it back.
	// The deferredSnapshots buffer is re-injected into the inbox when
	// commitPending clears and the normal processReady path runs.
	hasDeferred := false
	for i := range w.inboxBuf {
		if w.inboxBuf[i].Type == proto.InstallSnapshot {
			hasDeferred = true
			break
		}
	}
	if hasDeferred {
		filtered := w.inboxBuf[:0:0]
		for i := range w.inboxBuf {
			if w.inboxBuf[i].Type == proto.InstallSnapshot {
				// Buffer InstallSnapshot messages locally instead of
				// re-delivering them to the inbox. Re-delivery causes
				// an infinite drain-redeliver loop: each processReady
				// cycle drains the same message and puts it back,
				// burning CPU until commitPending clears. Buffering
				// here ensures the messages are processed exactly once
				// when commitPending transitions to false.
				w.deferredSnapshots[shardID] = append(
					w.deferredSnapshots[shardID], w.inboxBuf[i])
			} else {
				filtered = append(filtered, w.inboxBuf[i])
			}
		}
		w.inboxBuf = filtered
		if len(w.inboxBuf) == 0 && len(w.readKeyBuf) == 0 && len(w.proposeBuf) == 0 {
			return
		}
	}

	// Separate HeartbeatResp messages from other inbox messages for
	// priority processing. HeartbeatResp confirms ReadIndex quorum and
	// should be processed before proposals to minimize read latency.
	heartbeatResps := w.heartbeatRespBuf[:0]
	otherMsgs := w.otherMsgBuf[:0]
	for i := range w.inboxBuf {
		if w.inboxBuf[i].Type == proto.HeartbeatResp {
			heartbeatResps = append(heartbeatResps, w.inboxBuf[i])
		} else {
			otherMsgs = append(otherMsgs, w.inboxBuf[i])
		}
	}
	w.heartbeatRespBuf = heartbeatResps
	w.otherMsgBuf = otherMsgs

	// Phase 1: Process ReadIndex requests and HeartbeatResp under lock,
	// then immediately extract and deliver ReadIndex completions.
	// This minimizes read latency by not waiting for proposal processing.
	node.raftMu.Lock()

	// Process batched ReadIndex keys first. Same coalescing logic as
	// the normal processReady path: aliases are registered via
	// OnReadIndexBatched BEFORE submitting the batch key to raft.
	if len(w.readKeyBuf) > 0 {
		batchKey := w.readKeyBuf[0]
		// Register batch aliases BEFORE submitting the ReadIndex
		// to raft. Same rationale as the normal processReady path:
		// in single-node clusters, Handle(ReadIndex) resolves
		// immediately and DrainReadStatesOnly below returns the result.
		// If aliases were registered after Handle, OnReadyToRead
		// could fire before aliases exist, orphaning alias callers.
		if len(w.readKeyBuf) > 1 && w.callback != nil {
			w.callback.OnReadIndexBatched(shardID, batchKey, w.readKeyBuf[1:])
		}
		if err := node.peer.Handle(proto.Message{
			Type: proto.ReadIndex,
			From: node.ReplicaID(),
			Hint: batchKey,
		}); err != nil {
			slog.Warn("batched ReadIndex failed (commitPending)",
				"shard_id", shardID,
				"replica_id", node.ReplicaID(),
				"num_requests", len(w.readKeyBuf),
				"error", err,
			)
			// Complete all pending reads with the error. Same rationale
			// as the normal path: aliases are already registered but no
			// ReadyToRead will fire.
			if w.callback != nil {
				w.callback.OnReadIndexFailed(shardID, w.readKeyBuf, err)
			}
		}
	}

	// Process HeartbeatResp messages which may confirm pending ReadIndex.
	// These are processed before proposals to prioritize read completion.
	for i := range heartbeatResps {
		if err := node.peer.Handle(heartbeatResps[i]); err != nil {
			slog.Warn("peer.Handle failed (commitPending HeartbeatResp)",
				"shard_id", shardID,
				"replica_id", node.ReplicaID(),
				"msg_from", heartbeatResps[i].From,
				"error", err,
			)
		}
	}

	// Flush deferred ReadIndex heartbeat broadcasts that may have been
	// generated by the ReadIndex requests above.
	node.peer.FlushReadIndex()

	// Extract ReadIndex completions immediately after processing
	// HeartbeatResp. This allows us to deliver read results before
	// processing proposals, reducing read latency.
	readStates := node.peer.DrainReadStatesOnly()

	node.raftMu.Unlock()

	// Deliver ReadIndex completion callbacks immediately, before
	// processing proposals. This prioritizes read latency.
	if w.callback != nil && len(readStates) > 0 {
		w.callback.OnReadyToRead(shardID, readStates)
	}

	// Phase 2: Process proposals and remaining inbox messages.
	// These can proceed at lower priority since reads are done.
	if len(w.proposeBuf) == 0 && len(otherMsgs) == 0 {
		return
	}

	node.raftMu.Lock()

	// Process batched proposals. Unlike the normal processReady path which
	// skips proposal drain during commitPending, we handle proposals here
	// to enable pipelining. The proposals are appended to the Raft log and
	// will be included in the next update after commitPending clears.
	// This reduces latency by eliminating the blocking window where
	// proposals sit in the MPSC queue waiting for commit to complete.
	if len(w.proposeBuf) > 0 {
		if w.metrics != nil {
			w.metrics.ObserveProposalBatchSize(shardID, len(w.proposeBuf))
		}
		if err := node.peer.Handle(proto.Message{
			Type:    proto.Propose,
			Entries: w.proposeBuf,
		}); err != nil {
			slog.Warn("batched proposal failed (commitPending)",
				"shard_id", shardID,
				"replica_id", node.ReplicaID(),
				"num_entries", len(w.proposeBuf),
				"error", err,
			)
			// Notify the host so it can complete pending proposals
			// with the error instead of leaving callers blocked.
			if w.callback != nil {
				w.callback.OnProposalFailed(shardID, w.proposeBuf, err)
			}
		}
		// Update rate limiter cache after proposals are appended.
		node.peer.RateLimited()
	}

	// Process remaining inbox messages (non-HeartbeatResp).
	for i := range otherMsgs {
		if err := node.peer.Handle(otherMsgs[i]); err != nil {
			slog.Warn("peer.Handle failed (commitPending)",
				"shard_id", shardID,
				"replica_id", node.ReplicaID(),
				"msg_type", otherMsgs[i].Type,
				"msg_from", otherMsgs[i].From,
				"error", err,
			)
		}
	}

	// Drain free-order messages (Replicate, Heartbeat, Ping, Quiesce)
	// generated by proposal handling and inbox processing. These messages
	// are safe to send before WAL persistence per PhD thesis 10.2.1.
	// Sending Replicate messages immediately enables pipelining: followers
	// receive entries and send acks while the leader's WAL fsync is still
	// in progress. This reduces commit latency by overlapping leader
	// persistence with network round-trip.
	//
	// Ordered messages (VoteResp, HeartbeatResp, ReplicateResp) remain in
	// the peer's buffer and will be sent after WAL persistence completes
	// in the next GetUpdate cycle. DrainFreeOrderMessages filters them
	// out automatically.
	freeOrderMsgs := node.peer.DrainFreeOrderMessages()

	// Check for any additional ReadIndex completions that may have been
	// generated by processing other inbox messages (e.g., ReadIndexResp
	// from forwarded ReadIndex requests).
	additionalReadStates := node.peer.DrainReadStatesOnly()

	node.raftMu.Unlock()

	// Send free-order messages outside the lock.
	if len(freeOrderMsgs) > 0 {
		w.sendMessages(freeOrderMsgs)
	}

	// Deliver any additional ReadIndex completions from phase 2.
	if w.callback != nil && len(additionalReadStates) > 0 {
		w.callback.OnReadyToRead(shardID, additionalReadStates)
	}
}

// sendMessages delivers outbound Raft messages via the transport layer.
// Regular messages (Replicate, Heartbeat, etc.) are sent through
// sender.Send(), which is non-blocking because the transport uses
// per-target async send queues. InstallSnapshot messages are routed
// through sender.SendSnapshot() for chunked snapshot streaming.
//
// This method does NOT block the step worker's processReady loop.
// The transport's Send() enqueues messages into per-target buffered
// channels (capacity 2048) and returns immediately. Actual QUIC I/O
// happens in background sender goroutines managed by the transport.
func (w *stepWorker) sendMessages(msgs []proto.Message) {
	// Fast path: scan for InstallSnapshot messages. In the common case
	// (no snapshots in flight), all messages go through Send() without
	// allocation.
	hasSnapshot := false
	for i := range msgs {
		if msgs[i].Type == proto.InstallSnapshot {
			hasSnapshot = true
			break
		}
	}
	if !hasSnapshot {
		w.sender.Send(msgs)
		return
	}
	// Slow path: separate snapshot messages from regular messages.
	// Build a filtered slice from the original without allocation
	// when possible.
	regular := msgs[:0:0] // new slice header, no allocation
	for i := range msgs {
		if msgs[i].Type == proto.InstallSnapshot {
			w.sender.SendSnapshot(msgs[i])
		} else {
			regular = append(regular, msgs[i])
		}
	}
	if len(regular) > 0 {
		w.sender.Send(regular)
	}
}

// clearStepCommitPending clears the commitPending flag on all nodes owned
// by this step worker. Called by the engine's restart loop after a step
// worker panic to unblock nodes that may have had commitPending set by
// processReady before the panic occurred. Without this, nodes would
// remain stuck with commitPending=true indefinitely, preventing the
// restarted worker from producing new updates for those shards.
func (w *stepWorker) clearStepCommitPending() {
	w.nodesMu.RLock()
	defer w.nodesMu.RUnlock()
	for _, node := range w.nodes {
		node.commitPending.Store(false)
	}
}

// getCommitSlot returns the pre-allocated commitItem for the given shard,
// creating one if it does not exist. Pre-allocation avoids heap allocation
// on the hot path since we send pointers through the channel.
func (w *stepWorker) getCommitSlot(shardID uint64) *commitItem {
	slot, ok := w.commitSlots[shardID]
	if !ok {
		slot = &commitItem{}
		w.commitSlots[shardID] = slot
	}
	return slot
}

// loadNode adds a node to this worker's partition.
// Safe for concurrent use; called from the Host goroutine while the
// step worker's run loop may be iterating nodes.
//
// The nodes map is protected by nodesMu. The commitSlots and
// prevLeaders maps are accessed by the step worker goroutine and
// cleaned up in unloadNode (which holds the write lock, ensuring
// mutual exclusion with the step worker).
func (w *stepWorker) loadNode(node *Node) {
	w.nodesMu.Lock()
	w.nodes[node.shardID] = node
	w.nodesMu.Unlock()
}

// unloadNode removes a node from this worker's partition and cleans up
// per-shard maps to prevent memory leaks across load/unload cycles.
// Safe for concurrent use; called from the Host goroutine while the
// step worker's run loop may be iterating nodes.
//
// commitSlots and prevLeaders are safe to clean up here because the
// write lock on nodesMu guarantees the step worker is not executing
// tick() or processReady() (both require RLock). The step worker is
// either blocked in the select loop or waiting for the read lock.
func (w *stepWorker) unloadNode(shardID uint64) {
	w.nodesMu.Lock()
	delete(w.nodes, shardID)
	delete(w.commitSlots, shardID)
	delete(w.prevLeaders, shardID)
	delete(w.deferredSnapshots, shardID)
	w.nodesMu.Unlock()
}
