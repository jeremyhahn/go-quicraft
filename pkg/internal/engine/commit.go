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
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// saveStateMaxRetries is the maximum number of retry attempts for
// LogDB.SaveState before giving up.
const saveStateMaxRetries = 5

// saveStateBackoff defines the exponential backoff schedule for
// SaveState retries. Total wait: ~1.76s (10ms + 50ms + 200ms + 500ms + 1s).
var saveStateBackoff = [saveStateMaxRetries]time.Duration{
	10 * time.Millisecond,
	50 * time.Millisecond,
	200 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
}

// applyItem carries a proto.Update and the owning Node from the commit
// worker to the apply worker. Items are pooled via sync.Pool to avoid
// allocation on the hot path.
type applyItem struct {
	update proto.Update
	node   *Node
}

// commitWorker writes Updates to LogDB using a drain-then-batch pattern
// for maximum write throughput. Multiple updates arriving while the
// worker is fsync'ing are batched into a single SaveState call.
//
// After receiving the first item from commitC, the worker yields via
// runtime.Gosched() to allow other goroutines (step workers) to
// produce additional items before draining. This micro-batch
// accumulation window increases average batch size under load without
// adding latency when the system is idle (Gosched returns immediately
// when no other goroutines are runnable).
//
// After successful SaveState, the commit worker sends ordered messages
// (VoteResp, HeartbeatResp, etc.) that were deferred by the step
// worker. Per Raft PhD thesis 10.2.1, these messages must not be sent
// until the corresponding state change is durable on stable storage.
type commitWorker struct {
	workerID   int
	logdb      logdb.LogDB
	sender     MessageSender
	commitC    chan *commitItem
	batch      []proto.Update
	batchNodes []*Node // parallel slice: batchNodes[i] owns batch[i]
	logdbBuf   []logdb.Update
	flatBuf    []logdb.Entry // single flat buffer for all entries across the batch
	applyC     chan *applyItem
	applyPool  *sync.Pool
	workSignal *WorkSignal
	callback   Callback
	stopC      chan struct{}

	// orderedMsgsBuf accumulates ordered messages from commitItems
	// during batch draining. These are messages that must be sent
	// AFTER SaveState completes (e.g., VoteResp, HeartbeatResp).
	// Keyed by shardID to support per-shard send ordering. Cleared
	// each batch cycle.
	orderedMsgsBuf map[uint64][]proto.Message

	// freeOrderBuf and orderedMsgSplitBuf are pre-allocated buffers
	// for splitting messages in executePiggyback. Reused across calls
	// to avoid the allocation from the msgs[:0:0] pattern.
	freeOrderBuf       []proto.Message
	orderedMsgSplitBuf []proto.Message

	// noRetryDelay skips the timer wait between SaveState retry attempts.
	// Set to true in tests to avoid real delays when SaveState errors are
	// injected. Has no effect in production (always false).
	noRetryDelay bool

	// retryHookForTest is called before each retry wait in tests only.
	// It may panic to exercise the panic-recovery path in processBatch.
	// Nil in production.
	retryHookForTest func(attempt int)

	// yieldFn is the yield function called between the first receive
	// and drain to accumulate larger batches. Defaults to
	// runtime.Gosched; overridden in tests.
	yieldFn func()

	// metrics collects raft-level metrics. Nil when disabled.
	metrics RaftMetrics

	// commitTimes maps shardID to the createdAt timestamp of the
	// commitItem for commit latency measurement. Cleared each batch.
	commitTimes map[uint64]time.Time

	// unloaded tracks shard IDs that have been unloaded via
	// UnloadNode. The commit worker skips Peer.Commit for shards
	// in this set, mirroring the apply worker's unloaded guard.
	// Without this, an in-flight commitItem could call Peer.Commit
	// on a node that has already been torn down.
	unloaded map[uint64]struct{}

	// unloadC receives shard IDs to add to the unloaded set.
	unloadC chan uint64

	// reloadC receives shard IDs to remove from the unloaded set.
	reloadC chan uint64
}

// newCommitWorker creates a commit worker. The sender parameter is used
// to deliver ordered messages (VoteResp, HeartbeatResp, etc.) after
// successful WAL persistence. It may be nil when running in memory mode
// (where the commit worker is bypassed entirely).
func newCommitWorker(
	workerID int,
	ldb logdb.LogDB,
	sender MessageSender,
	commitC chan *commitItem,
	applyC chan *applyItem,
	applyPool *sync.Pool,
	workSignal *WorkSignal,
	callback Callback,
	stopC chan struct{},
	metrics RaftMetrics,
) *commitWorker {
	return &commitWorker{
		workerID:           workerID,
		logdb:              ldb,
		sender:             sender,
		commitC:            commitC,
		batch:              make([]proto.Update, 0, 64),
		batchNodes:         make([]*Node, 0, 64),
		logdbBuf:           make([]logdb.Update, 0, 64),
		flatBuf:            make([]logdb.Entry, 0, 256),
		orderedMsgsBuf:     make(map[uint64][]proto.Message),
		freeOrderBuf:       make([]proto.Message, 0, 16),
		orderedMsgSplitBuf: make([]proto.Message, 0, 16),
		applyC:             applyC,
		applyPool:          applyPool,
		workSignal:         workSignal,
		callback:           callback,
		stopC:              stopC,
		yieldFn:            runtime.Gosched,
		metrics:            metrics,
		commitTimes:        make(map[uint64]time.Time),
		unloaded:           make(map[uint64]struct{}),
		unloadC:            make(chan uint64, 64),
		reloadC:            make(chan uint64, 64),
	}
}

// run is the main loop for the commit worker. It blocks until the first
// commitItem arrives, then yields via runtime.Gosched() to let other
// step workers produce additional items before draining. This micro-
// batch accumulation window increases batch size under concurrent load
// without adding latency when the system is idle. On shutdown, it
// drains all remaining items from commitC to ensure committed entries
// are persisted and forwarded to the apply worker.
func (w *commitWorker) run() {
	for {
		// Drain unload/reload requests before blocking on commitC.
		w.drainUnloadReload()
		select {
		case <-w.stopC:
			w.drainAndProcess()
			return
		case shardID := <-w.unloadC:
			w.unloaded[shardID] = struct{}{}
			continue
		case shardID := <-w.reloadC:
			delete(w.unloaded, shardID)
			continue
		case item := <-w.commitC:
			w.batch = append(w.batch[:0], item.update)
			w.batchNodes = append(w.batchNodes[:0], item.node)
			if w.metrics != nil {
				w.commitTimes[item.update.ShardID] = item.createdAt
			}
			w.captureOrderedMsgs(item)
			// Yield to let other step workers produce items before
			// draining. Under load this increases batch size; when
			// idle Gosched returns immediately with zero overhead.
			w.yieldFn()
			w.drainCommitC()
			w.processBatch()
		}
	}
}

// drainAndProcess non-blocking drains all remaining items from commitC
// and processes them as final batches. Called during shutdown to ensure
// all committed entries produced by step workers (which stopped first
// in the ordered shutdown) are persisted and forwarded to the apply
// worker (which is still running at this point).
func (w *commitWorker) drainAndProcess() {
	for {
		select {
		case item := <-w.commitC:
			w.batch = append(w.batch[:0], item.update)
			w.batchNodes = append(w.batchNodes[:0], item.node)
			if w.metrics != nil {
				w.commitTimes[item.update.ShardID] = item.createdAt
			}
			w.captureOrderedMsgs(item)
			w.drainCommitC()
			w.processBatch()
		default:
			return
		}
	}
}

// drainCommitC non-blocking drains all immediately available items from
// commitC into the batch. This maximizes batch size when multiple
// updates arrive while the worker is processing.
func (w *commitWorker) drainCommitC() {
	for {
		select {
		case item := <-w.commitC:
			w.batch = append(w.batch, item.update)
			w.batchNodes = append(w.batchNodes, item.node)
			if w.metrics != nil {
				w.commitTimes[item.update.ShardID] = item.createdAt
			}
			w.captureOrderedMsgs(item)
		default:
			return
		}
	}
}

// captureOrderedMsgs copies ordered messages from a commitItem into
// the per-shard orderedMsgsBuf. Called during batch accumulation in
// run(), drainCommitC(), and drainAndProcess().
func (w *commitWorker) captureOrderedMsgs(item *commitItem) {
	if len(item.orderedMsgs) > 0 {
		shardID := item.update.ShardID
		w.orderedMsgsBuf[shardID] = append(w.orderedMsgsBuf[shardID], item.orderedMsgs...)
	}
}

// sendOrderedMessages delivers ordered messages via the transport layer.
// Ordered messages include VoteResp, HeartbeatResp, ReplicateResp, etc.
// that must not be sent until the corresponding state is durable on
// stable storage. This method reuses the same InstallSnapshot routing
// logic as the step worker's sendMessages.
func (w *commitWorker) sendOrderedMessages(msgs []proto.Message) {
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
	regular := w.freeOrderBuf[:0]
	for i := range msgs {
		if msgs[i].Type == proto.InstallSnapshot {
			w.sender.SendSnapshot(msgs[i])
		} else {
			regular = append(regular, msgs[i])
		}
	}
	w.freeOrderBuf = regular
	if len(regular) > 0 {
		w.sender.Send(regular)
	}
}

// retryWait waits for the SaveState retry backoff delay for the given
// attempt index, or returns early if the stop channel is closed.
// Returns true if the stop channel was closed (caller should return),
// false if the delay elapsed normally.
//
// In tests, noRetryDelay skips the timer entirely for fast-path
// coverage. retryHookForTest, when non-nil, is called before waiting
// to allow tests to inject panics and exercise panic recovery.
func (w *commitWorker) retryWait(attempt int) (cancelled bool) {
	if w.retryHookForTest != nil {
		w.retryHookForTest(attempt)
	}
	if w.noRetryDelay {
		return false
	}
	t := time.NewTimer(saveStateBackoff[attempt])
	select {
	case <-w.stopC:
		t.Stop()
		return true
	case <-t.C:
		return false
	}
}

// processBatch converts the batch to logdb.Update format, calls
// SaveState with retry logic, and forwards successful updates to the
// apply worker.
//
//nolint:gocyclo // batch processing covers save, send, apply, and error recovery paths
func (w *commitWorker) processBatch() {
	if w.metrics != nil {
		w.metrics.ObserveCommitBatchSize(len(w.batch))
	}

	// Only convert and persist when LogDB is configured. In memory
	// mode (logdb == nil), skip the conversion entirely to avoid
	// allocating logdb.Entry slices and copying fields that will
	// never be persisted.
	var saveErr error
	if w.logdb != nil {
		w.logdbBuf, w.flatBuf = protoUpdatesToLogDBReuse(w.logdbBuf, w.flatBuf, w.batch)
		for attempt := range saveStateMaxRetries {
			saveErr = w.logdb.SaveState(w.logdbBuf)
			if saveErr == nil {
				break
			}
			if attempt < saveStateMaxRetries-1 {
				if cancelled := w.retryWait(attempt); cancelled {
					return
				}
			}
		}
	}

	if saveErr != nil {
		// Clear commitPending for all shards in the batch to prevent
		// them from being stuck forever. Also fail pending proposals so
		// callers see an immediate error instead of blocking until
		// context deadline timeout. The committed entries from the
		// failed batch were already extracted via GetUpdate and will
		// never reach the apply worker.
		//
		// Mark each node as failed so the step worker skips it on
		// subsequent ticks. The node's internal Raft state has diverged
		// from stable storage (entries were extracted via GetUpdate,
		// advancing prevState, but never persisted). Allowing the node
		// to continue would violate Raft's durability invariant: the
		// node could acknowledge entries to the leader that were never
		// written to WAL, causing silent data loss on crash.
		// Circuit breaker: mark nodes as permanently failed. There is
		// intentionally no automatic reset for SaveState failures.
		// The node's internal Raft state has diverged from stable
		// storage, making automatic recovery unsafe. The recovery
		// path is to unload and reload the shard, which reconstructs
		// the Raft state from the last consistent WAL checkpoint.
		// This conservative approach prevents silent data loss that
		// could occur if the node continued operating with stale
		// persisted state.
		for i, u := range w.batch {
			node := w.batchNodes[i]
			node.failed.Store(true)
			node.commitPending.Store(false)
			saveStateErr := &SaveStateError{
				ShardID:   u.ShardID,
				ReplicaID: u.ReplicaID,
				Err:       saveErr,
			}
			node.handleError(saveStateErr)
			if w.callback != nil && len(u.CommittedEntries) > 0 {
				w.callback.OnProposalFailed(u.ShardID, u.CommittedEntries, saveStateErr)
			}
		}
		w.clearBatch()
		return
	}

	// Record commit latency for each shard in the batch. The latency
	// spans from step worker Update production to SaveState completion.
	if w.metrics != nil {
		now := time.Now()
		for shardID, ts := range w.commitTimes {
			if !ts.IsZero() {
				w.metrics.ObserveCommitLatency(shardID, now.Sub(ts))
			}
		}
	}

	// Send ordered messages now that state is persisted to stable
	// storage. These include VoteResp, HeartbeatResp, ReplicateResp,
	// etc. that must not be sent until the corresponding state change
	// (vote grant, term update, log append acknowledgment) is durable.
	// Per Raft PhD thesis section 10.2.1, sending these before persist
	// can cause split-brain (e.g., vote grant then crash then re-vote).
	if w.sender != nil {
		for _, msgs := range w.orderedMsgsBuf {
			if len(msgs) > 0 {
				w.sendOrderedMessages(msgs)
			}
		}
	}

	// WAL-durable notification: after successful SaveState, signal
	// committed proposals before forwarding to the apply worker. Only
	// entries from this batch that were proposed locally (Key > 0) are
	// notified. This fires per-update so the callback receives entries
	// scoped to a single shard.
	if w.callback != nil {
		for i, u := range w.batch {
			node := w.batchNodes[i]
			if node.notifyCommit && len(u.EntriesToSave) > 0 {
				w.callback.OnCommitted(u.ShardID, u.EntriesToSave)
			}
		}
	}

	// Commit peer state and forward to apply workers. The raftMu lock
	// serializes Commit with the step worker's Tick() call, which now
	// runs unconditionally (even when commitPending=true) to maintain
	// leader heartbeat liveness. The lock is per-node and sub-microsecond.
	//
	// Each update is committed to its owning node (batchNodes[i]),
	// NOT resolved by shardID. This is critical for correctness
	// during rapid shard stop/restart: the batch may contain updates
	// from both the old and new shard incarnation for the same
	// shardID. Using a shardID-keyed map would clobber the old
	// node pointer with the new one, causing the old update's
	// committed entries to be applied to the new Peer (whose
	// committed index may be lower), violating the appliedTo
	// invariant.
	for i, u := range w.batch {
		node := w.batchNodes[i]

		// Skip Peer.Commit for shards that have been unloaded.
		// An in-flight commitItem may have been enqueued before the
		// unload was processed. Calling Peer.Commit on a torn-down
		// node would panic or corrupt state.
		if _, unloaded := w.unloaded[u.ShardID]; unloaded {
			slog.Warn("commit skipped for unloaded shard",
				"worker", w.workerID,
				"shard", u.ShardID,
			)
			u.ReleaseCommittedEntries()
			node.commitPending.Store(false)
			continue
		}

		node.raftMu.Lock()
		err := node.peer.Commit(u)

		if err != nil {
			node.raftMu.Unlock()
			slog.Warn("commit Peer.Commit failed",
				"shard", u.ShardID, "replica", u.ReplicaID, "error", err)
			node.commitPending.Store(false)
			node.handleError(&CommitError{
				ShardID:   u.ShardID,
				ReplicaID: u.ReplicaID,
				Err:       err,
			})
			continue
		}

		if len(u.CommittedEntries) == 0 {
			// Piggyback: check if entries became committable during the
			// commitPending window (processCommitPendingInbox advanced the
			// commit index after handling follower ReplicateResp).
			piggyback, pbErr := node.peer.GetPiggybackUpdate(node.lastApplied.Load())
			node.raftMu.Unlock()

			if pbErr != nil || len(piggyback.CommittedEntries) == 0 {
				// No piggyback available — original skip-apply fast path.
				if pbErr != nil {
					slog.Warn("piggyback GetPiggybackUpdate failed",
						"shard", u.ShardID, "replica", u.ReplicaID, "error", pbErr)
				}
				node.commitPending.Store(false)
				w.workSignal.Notify(node.shardID)
				continue
			}

			// Piggyback path: persist updated state, send messages, apply.
			w.executePiggyback(node, piggyback)
			continue
		}

		node.raftMu.Unlock()

		ai := w.applyPool.Get().(*applyItem)
		ai.update = u
		ai.node = node
		// Send to the apply worker with a stopC guard to prevent
		// deadlock when the apply worker has panicked and is in its
		// 100ms restart delay window. The non-blocking attempt first
		// ensures committed entries are always delivered when the
		// channel has capacity (which is the common case during
		// ordered shutdown, since the apply worker is still running).
		// Only if the channel is full AND stopC is closed do we give
		// up — this can only happen during a simultaneous shutdown
		// and apply worker panic, which is an edge case where the
		// engine is already tearing down.
		select {
		case w.applyC <- ai:
		default:
			select {
			case w.applyC <- ai:
			case <-w.stopC:
				slog.Warn("commit worker shutting down, committed item not delivered to apply worker",
					"shard", ai.update.ShardID)
				w.clearBatch()
				return
			}
		}
	}
	w.clearBatch()
}

// clearBatch clears the batch node pointers, update values, commit times,
// and ordered messages buffer for reuse across batch cycles.
func (w *commitWorker) clearBatch() {
	// Zero batch entries to release references to large slices
	// (EntriesToSave, CommittedEntries, Messages) that would otherwise
	// be retained until the next batch cycle overwrites them.
	var zeroUpdate proto.Update
	for i := range w.batch {
		w.batch[i] = zeroUpdate
	}
	// Clear node pointers to avoid retaining references to unloaded
	// nodes across batch cycles.
	for i := range w.batchNodes {
		w.batchNodes[i] = nil
	}
	clear(w.commitTimes)
	clear(w.orderedMsgsBuf)
}

// drainUnloadReload non-blocking drains all pending unload and reload
// requests from their respective channels. Called at the top of each
// run loop iteration to process requests that arrived while the worker
// was busy with SaveState or message sending.
func (w *commitWorker) drainUnloadReload() {
	for {
		select {
		case shardID := <-w.unloadC:
			w.unloaded[shardID] = struct{}{}
		case shardID := <-w.reloadC:
			delete(w.unloaded, shardID)
		default:
			return
		}
	}
}

// requestUnload sends a shard removal request to the commit worker via
// the unloadC channel. Safe for concurrent use by the Host goroutine.
func (w *commitWorker) requestUnload(shardID uint64) {
	select {
	case w.unloadC <- shardID:
	case <-w.stopC:
	}
}

// requestReload clears a shard from the unloaded set, allowing the
// commit worker to process Peer.Commit for the shard again. Safe for
// concurrent use by the Host goroutine.
func (w *commitWorker) requestReload(shardID uint64) {
	select {
	case w.reloadC <- shardID:
	case <-w.stopC:
	}
}

// executePiggyback sends messages with proper free/ordered split, commits
// the piggyback to advance the processed marker, and forwards committed
// entries to the apply worker. Eliminates the second pipeline cycle for
// multi-node proposals. Called with raftMu already released.
//
// Commit Index Persistence: The piggyback commit index is NOT persisted
// to WAL here. This eliminates the second fsync that was previously
// required per batch. The commit index will be persisted in the next
// batch's SaveState call. This is safe because:
//
//  1. Raft safety only requires Term and Vote to be durable before
//     responding to vote requests (PhD thesis 3.6.1). The commit index
//     is a performance optimization, not a safety requirement.
//
//  2. On recovery, the commit index is recomputed from the durable log
//     entries and follower acknowledgments. The leader recalculates the
//     commit point by checking which entries have been replicated to a
//     majority.
//
//  3. The entries themselves are already durable from the first fsync
//     in processBatch(). Only the commit index metadata was being
//     redundantly persisted here.
//
// On crash-recovery between piggyback apply and the next SaveState, the
// node may re-apply entries between the persisted and actual commit index.
// This is safe because state machine Apply is idempotent: each entry is
// identified by its unique log index, and the session mechanism (client
// sequence tracking) prevents duplicate side-effects from re-applied
// proposals.
//
// This optimization reduces fsync operations by ~50% for batches that
// trigger piggyback commits, improving throughput significantly on
// workloads with concurrent multi-node proposals.
func (w *commitWorker) executePiggyback(node *Node, piggyback proto.Update) {
	// Split messages: free-order sent immediately, ordered after commit.
	// Reuse pre-allocated buffers to avoid the msgs[:0:0] allocation
	// pattern on every piggyback call.
	w.freeOrderBuf = w.freeOrderBuf[:0]
	w.orderedMsgSplitBuf = w.orderedMsgSplitBuf[:0]
	if len(piggyback.Messages) > 0 {
		for j := range piggyback.Messages {
			if proto.IsFreeOrderMessage(piggyback.Messages[j].Type) {
				w.freeOrderBuf = append(w.freeOrderBuf, piggyback.Messages[j])
			} else {
				w.orderedMsgSplitBuf = append(w.orderedMsgSplitBuf, piggyback.Messages[j])
			}
		}
		if len(w.freeOrderBuf) > 0 && w.sender != nil {
			w.sender.Send(w.freeOrderBuf)
		}
	}

	// Skip commit index persistence - it will be included in the next
	// batch's SaveState. The entries are already durable from the first
	// fsync, and the commit index can be recomputed on recovery.

	// Send ordered messages. Although we skipped WAL persistence for the
	// commit index, the original entries are already durable, so it's
	// safe to send responses.
	if w.sender != nil && len(w.orderedMsgSplitBuf) > 0 {
		w.sendOrderedMessages(w.orderedMsgSplitBuf)
	}

	// OnCommitted callback (WAL-durable notification).
	if w.callback != nil && node.notifyCommit && len(piggyback.CommittedEntries) > 0 {
		w.callback.OnCommitted(piggyback.ShardID, piggyback.CommittedEntries)
	}

	// Commit piggyback to advance processed markers.
	node.raftMu.Lock()
	pbCommitErr := node.peer.Commit(piggyback)
	node.raftMu.Unlock()

	if pbCommitErr != nil {
		slog.Warn("piggyback Peer.Commit failed",
			"shard", piggyback.ShardID, "replica", piggyback.ReplicaID, "error", pbCommitErr)
		piggyback.ReleaseCommittedEntries()
		node.commitPending.Store(false)
		node.handleError(&CommitError{
			ShardID:   piggyback.ShardID,
			ReplicaID: piggyback.ReplicaID,
			Err:       pbCommitErr,
		})
		return
	}

	if w.metrics != nil {
		w.metrics.IncPiggybackCommit(piggyback.ShardID)
	}

	// Forward to apply worker (commitPending cleared by apply worker).
	// Non-blocking attempt first to ensure delivery when the channel
	// has capacity (common case). Falls back to stopC-guarded blocking
	// send to prevent deadlock during apply worker panic recovery.
	ai := w.applyPool.Get().(*applyItem)
	ai.update = piggyback
	ai.node = node
	select {
	case w.applyC <- ai:
	default:
		select {
		case w.applyC <- ai:
		case <-w.stopC:
			slog.Warn("commit worker shutting down, piggyback item not delivered to apply worker",
				"shard", piggyback.ShardID)
			w.clearBatch()
			return
		}
	}
}
