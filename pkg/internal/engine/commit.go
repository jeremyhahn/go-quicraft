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
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// saveStateMaxRetries is the maximum number of retry attempts for
// LogDB.SaveState before giving up.
const saveStateMaxRetries = 5

// saveStateBackoff defines the exponential backoff schedule between SaveState
// attempts. There are saveStateMaxRetries (5) attempts and therefore at most
// saveStateMaxRetries-1 (4) inter-attempt waits — the schedule is sized to that,
// so no element is dead. Total pre-failure grace: ~760ms (10ms + 50ms + 200ms +
// 500ms). retryWait indexes it by attempt (0..3); the final attempt is not
// followed by a wait.
var saveStateBackoff = [saveStateMaxRetries - 1]time.Duration{
	10 * time.Millisecond,
	50 * time.Millisecond,
	200 * time.Millisecond,
	500 * time.Millisecond,
}

// applyItem carries a proto.Update and the owning Node from the commit
// worker to the apply worker. Items are pooled via sync.Pool to avoid
// allocation on the hot path.
type applyItem struct {
	update proto.Update
	node   *Node
}

// shardMembership is a single ordered load/unload notification for the
// commit worker. unload marks the shard retired (Peer.Commit is skipped
// for it); clearing unload re-admits it.
//
// Both directions travel on ONE channel because select chooses uniformly
// at random among ready cases: with a separate unloadC and reloadC, an
// UnloadNode followed by a LoadNode for the same shard could be applied
// in reverse, leaving the shard permanently in the unloaded set. The
// commit worker would then skip Peer.Commit forever, so the restarted
// replica would persist and vote (even win elections) while applying
// nothing. A single channel makes the sequence FIFO by construction.
//
// done, when set, is closed once the commit worker goroutine has applied the
// change. The goroutine applies membership only BETWEEN batches, so a closed
// done also proves that no batch in progress when it was sent is still running.
type shardMembership struct {
	shardID uint64
	unload  bool
	done    chan struct{}
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

	// batchUnpersisted reports that the current batch holds updates whose
	// SaveState has not completed — from the moment the batch is gathered
	// until SaveState succeeds or failBatchSaveState settles it. Its updates
	// are owed a durable hard-state write GetUpdate already claimed (it
	// advanced prevState at extraction), so an abnormal exit in that window
	// gives the claim back (recoverAbnormalExit), as the step worker's
	// discardExtractedUpdate does. Their entries were never marked stable, so
	// the next GetUpdate re-derives them.
	batchUnpersisted bool

	// handoffNode and handoffUpdate are the update whose Peer.Commit has
	// advanced raft's processed marker but whose committed entries have not
	// yet reached the apply worker. They exist nowhere else in that window,
	// and user code — the OnCommitted callback, a logger — runs inside it, so
	// an abnormal exit fails the shard (recoverAbnormalExit). Written and
	// read only on the commit-worker goroutine.
	handoffNode   *Node
	handoffUpdate proto.Update
	logdbBuf      []logdb.Update
	flatBuf       []logdb.Entry // single flat buffer for all entries across the batch
	applyC        chan *applyItem
	applyPool     *sync.Pool
	workSignal    *WorkSignal
	callback      Callback
	stopC         chan struct{}

	// applyStopC is the APPLY tier's stop channel (applyStopC in the engine),
	// distinct from this worker's own stopC (commitStopC). The forward to the
	// apply worker gives up only when the apply tier is actually gone: during
	// ordered shutdown commitStopC closes in Stop step 3 while the apply tier
	// stays live until step 4, so guarding the forward on stopC would abandon a
	// WAL-persisted, Commit-advanced item on a coin-flip select. Nil in unit
	// tests, where the forward falls back to stopC (see forwardGiveUpC).
	applyStopC chan struct{}

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
	unloaded *unloadedSet

	// membershipC receives ordered unload/reload notifications. See
	// shardMembership for why both directions share one channel.
	membershipC chan shardMembership
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
		unloaded:           newUnloadedSet(),
		membershipC:        make(chan shardMembership, 128),
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
		case m := <-w.membershipC:
			w.applyMembership(m)
			continue
		case item := <-w.commitC:
			w.batch = w.batch[:0]
			w.batchNodes = w.batchNodes[:0]
			w.batchUnpersisted = true
			w.admit(item)
			// Yield to let other step workers produce items before
			// draining. Under load this increases batch size; when
			// idle Gosched returns immediately with zero overhead.
			w.yieldFn()
			w.drainCommitC()
			if len(w.batch) > 0 {
				w.processBatch()
			}
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
			w.batch = w.batch[:0]
			w.batchNodes = w.batchNodes[:0]
			w.batchUnpersisted = true
			w.admit(item)
			w.drainCommitC()
			if len(w.batch) > 0 {
				w.processBatch()
			}
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
			w.admit(item)
		default:
			return
		}
	}
}

// admit adds one commitItem to the batch, or drops it when it belongs to an
// unloaded shard incarnation.
//
// Dropping is what keeps an unloaded incarnation from writing LogDB after
// UnloadNode returns. The unload fence (requestUnload) proves only that no
// SaveState of the stopped incarnation is running or will run in the current
// batch; an item still queued in commitC is admitted later, and by then a new incarnation of the same shard
// may have loaded. Its update was never made durable, so dropping it is
// exactly a crash before the fsync: its ordered messages — the ones that
// promise durability, such as a vote grant — are dropped with it and never
// sent, and the free-order ones already sent promise nothing. The check is at
// admission rather than beside the post-SaveState guards because ordered
// messages are gathered per shard ID here, where an old and a new
// incarnation's would otherwise merge.
//
// The test is Node.isUnloaded, not the node's stopC: Engine.Stop stops every
// node long before the commit tier exits, and no incarnation can follow a
// shutdown, so the shutdown drain keeps persisting.
func (w *commitWorker) admit(item *commitItem) {
	if item.node.isUnloaded() {
		slog.Warn("update dropped for unloaded shard incarnation",
			"worker", w.workerID,
			"shard", item.update.ShardID,
		)
		item.update.ReleaseCommittedEntries()
		item.node.commitPending.Store(false)
		return
	}
	w.batch = append(w.batch, item.update)
	w.batchNodes = append(w.batchNodes, item.node)
	if w.metrics != nil {
		w.commitTimes[item.update.ShardID] = item.createdAt
	}
	w.captureOrderedMsgs(item)
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

// forwardGiveUpC returns the channel whose close means "stop trying to forward
// committed items to the apply tier". In production this is applyStopC: the apply
// tier is live until Stop step 4, so a full applyC during ordered shutdown (step
// 3) must be waited out, not abandoned. In unit tests applyStopC is nil, so it
// falls back to the worker's own stopC, preserving the original single-channel
// behavior.
func (w *commitWorker) forwardGiveUpC() <-chan struct{} {
	if w.applyStopC != nil {
		return w.applyStopC
	}
	return w.stopC
}

// forwardToApply hands ai to the apply worker. It returns false, having
// delivered nothing, when it gave up because the apply tier is stopping.
//
// The common case is a non-blocking send. While the send blocks — the apply
// dispatcher backpressured on some shard's full per-shard channel — the
// worker keeps applying unload/reload requests. An unload waits on this
// worker (requestUnload is a fence), and StopShard of one shard must not wait
// out another shard's busy state machine: the dispatcher answers its own
// membership requests while backpressured, and this send is where the
// commit tier would otherwise stall them. Applying membership mid-batch is
// safe. The batch's SaveState, and any piggyback SaveState of the item being
// forwarded, have already completed, and every later item in the batch
// re-checks the unloaded set and its node's unloaded mark before it is
// committed or persisted.
func (w *commitWorker) forwardToApply(ai *applyItem) bool {
	select {
	case w.applyC <- ai:
		return true
	default:
	}
	giveUp := w.forwardGiveUpC()
	for {
		select {
		case w.applyC <- ai:
			return true
		case m := <-w.membershipC:
			w.applyMembership(m)
		case <-giveUp:
			return false
		}
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
func (w *commitWorker) processBatch() {
	// Observability runs on every exit, but only once the batch is settled —
	// failed, abandoned or handed on — never inside a window in which an
	// abnormal exit in user metrics code could lose state.
	batchSize := len(w.batch)
	var persistedAt time.Time
	observe := func() {
		if w.metrics == nil {
			return
		}
		w.metrics.ObserveCommitBatchSize(batchSize)
		if persistedAt.IsZero() {
			return
		}
		for shardID, ts := range w.commitTimes {
			if !ts.IsZero() {
				w.metrics.ObserveCommitLatency(shardID, persistedAt.Sub(ts))
			}
		}
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
					// Shutdown fired during the retry backoff. The batch
					// was never persisted, so its committed entries will
					// never reach the apply worker. Clear commitPending
					// and fail pending proposals for every node in the
					// batch before returning, mirroring the saveErr
					// cleanup path below. Without this, callers block
					// until their context deadline and the nodes are left
					// stuck with commitPending=true.
					w.failBatchSaveState(saveErr)
					observe()
					return
				}
			}
		}
	}

	if saveErr != nil {
		w.failBatchSaveState(saveErr)
		observe()
		return
	}
	w.batchUnpersisted = false
	// The latency spans from step worker Update production to SaveState
	// completion; it is reported only once the batch is settled (observe).
	persistedAt = time.Now()

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
		stop := w.commitBatchItem(w.batchNodes[i], u)
		// The item reached its terminal hand-off — the apply worker owns it,
		// or it was failed, skipped or released — so it is retired from the
		// batch: a later abnormal exit must not touch its node, whose
		// commitPending may already belong to the apply worker.
		w.batchNodes[i] = nil
		if stop {
			w.abandonBatchTail(i + 1)
			observe()
			w.clearBatch()
			return
		}
	}
	observe()
	w.clearBatch()
}

// commitBatchItem commits one persisted batch update to its owning node and
// hands its committed entries on, reporting stop when the worker is shutting
// down and the rest of the batch must be abandoned. It runs every terminal
// outcome for the item itself, so the caller can retire it.
//
// Each update is committed to its owning node (batchNodes[i]), NOT resolved by
// shardID. This is critical for correctness during rapid shard stop/restart:
// the batch may contain updates from both the old and new shard incarnation
// for the same shardID. Using a shardID-keyed map would clobber the old node
// pointer with the new one, causing the old update's committed entries to be
// applied to the new Peer (whose committed index may be lower), violating the
// appliedTo invariant.
func (w *commitWorker) commitBatchItem(node *Node, u proto.Update) (stop bool) {
	// Skip Peer.Commit for shards that have been unloaded. An in-flight
	// commitItem may have been enqueued before the unload was processed.
	// Calling Peer.Commit on a torn-down node would panic or corrupt state.
	if w.unloaded.has(u.ShardID) {
		slog.Warn("commit skipped for unloaded shard",
			"worker", w.workerID,
			"shard", u.ShardID,
		)
		u.ReleaseCommittedEntries()
		node.commitPending.Store(false)
		return false
	}

	// Incarnation guard, mirroring the apply dispatcher. The unloaded set is
	// shardID-keyed and cannot distinguish a stale old-incarnation item from
	// a fresh one after a rapid StopShard->StartShard. The node's own stopC
	// can: an item whose node UnloadNode has begun tearing down belongs to a
	// torn-down incarnation, and Commit on its peer would race the teardown.
	// A node stopped only by Engine.Stop is not unloaded, so the shutdown
	// drain keeps committing (see Node.isUnloaded).
	if node.isUnloaded() {
		slog.Warn("commit skipped for stopped shard incarnation",
			"worker", w.workerID,
			"shard", u.ShardID,
		)
		u.ReleaseCommittedEntries()
		node.commitPending.Store(false)
		return false
	}

	var piggyback proto.Update
	status, err := commitUpdateLocked(node, &u, &piggyback)
	switch status {
	case commitFailed:
		failCommit(node, w.callback, u, err)
		return false
	case commitRecovering:
		// A snapshot recovery owns the state machine, so nothing was
		// extracted for the apply worker (see commitUpdateLocked). Clear
		// commitPending and re-notify: the entries stay in the log and are
		// re-offered once the gate clears.
		node.commitPending.Store(false)
		w.workSignal.Notify(node.shardID)
		return false
	case commitPiggyback:
		if err != nil || len(piggyback.CommittedEntries) == 0 {
			// No piggyback available — original skip-apply fast path.
			if err != nil {
				notePiggybackFailure(u.ShardID, u.ReplicaID, err)
			}
			node.commitPending.Store(false)
			w.workSignal.Notify(node.shardID)
			return false
		}
		// Piggyback path: persist updated state, send messages, apply. It
		// takes raftMu itself, in short bursts, and must therefore run with
		// the lock released — which is why commitUpdateLocked returns the
		// piggyback instead of executing it. On shutdown mid-delivery it
		// returns false and the rest of the batch is abandoned.
		if !w.executePiggyback(node, piggyback) {
			node.commitPending.Store(false)
			return true
		}
		return false
	}

	// commitApply: the update carries committed entries. The forward below is
	// a BLOCKING channel send to the apply worker and must never run under
	// node.raftMu — the apply worker's own progress depends on the commit and
	// step workers being able to take it.
	w.handoffNode, w.handoffUpdate = node, u
	ai := w.applyPool.Get().(*applyItem)
	ai.update = u
	ai.node = node
	// Only if the channel is full AND the apply tier is stopping do we give up
	// — an edge case where the engine is already tearing down (see
	// forwardToApply).
	forwarded := w.forwardToApply(ai)
	w.handoffNode, w.handoffUpdate = nil, proto.Update{}
	if !forwarded {
		slog.Warn("commit worker shutting down, committed item not delivered to apply worker",
			"shard", ai.update.ShardID)
		ai.update.ReleaseCommittedEntries()
		releaseApplyItem(w.applyPool, ai)
		node.commitPending.Store(false)
		return true
	}
	return false
}

// commitStatus classifies what commitUpdateLocked did, telling processBatch
// which of its four continuations to run once node.raftMu has been released.
type commitStatus uint8

const (
	// commitApply means the update committed and carries entries the caller
	// must forward to the apply worker.
	commitApply commitStatus = iota
	// commitFailed means Peer.Commit returned an error, carried in the
	// accompanying error value. Nothing was extracted.
	commitFailed
	// commitRecovering means the update committed with no entries and the
	// state machine is not ready to receive any: either a snapshot recovery
	// owns it, or a received snapshot is installed in the log and not yet
	// adopted by it. No piggyback was attempted.
	commitRecovering
	// commitPiggyback means the update committed with no entries and a
	// piggyback update was requested. The piggyback out-parameter holds it and
	// the accompanying error is GetPiggybackUpdate's, which may be nil with an
	// empty piggyback (the common no-op case).
	commitPiggyback
)

// commitUpdateLocked commits one update to its owning node's Peer with
// node.raftMu held, and — when the update committed nothing to apply — asks the
// peer for a piggyback update while still under the same lock, writing it to
// the piggyback out-parameter. raftMu serializes all of this with the step
// worker's Tick(), which runs unconditionally even while commitPending is set.
//
// Everything else processBatch does with the outcome stays in the caller,
// outside the lock, and that placement is load-bearing rather than tidiness:
//
//   - failCommit enters the host callback and fails every pending proposal for
//     the shard, which is unbounded user work.
//   - executePiggyback fsyncs, sends messages and forwards to the apply worker,
//     and takes raftMu itself in short bursts — running it here would
//     self-deadlock on a non-reentrant sync.Mutex.
//   - the applyC forward is a BLOCKING channel send; holding raftMu across it
//     would let a full apply queue stall the step worker's Tick and, with it,
//     leader heartbeats for every shard this worker owns.
//
// The lock is released by a defer, so a panic inside Peer.Commit (a corrupt
// log, a torn-down peer) cannot leak node.raftMu. A leaked lock would be fatal:
// the commit worker recovers and restarts, the restarted worker blocks forever
// on Lock(), and every shard it owns stops committing, taking Host.Close (which
// joins the same worker) with it.
//
// The update is taken by pointer purely to avoid copying a proto.Update on the
// commit hot path; it is neither retained nor mutated here.
func commitUpdateLocked(node *Node, u *proto.Update, piggyback *proto.Update) (commitStatus, error) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()

	if err := node.peer.Commit(*u); err != nil {
		return commitFailed, err
	}

	if len(u.CommittedEntries) == 0 {
		// A snapshot recovery owns the state machine: extracting
		// committed entries here would advance raft's processed marker
		// and hand the batch to an apply worker that must park it on the
		// recovering gate, holding commitPending — and with it every
		// ordered response, including vote grants — for the whole
		// unbounded recovery. The step worker suppresses the same
		// extraction (GetUpdate moreEntriesToApply=false); entries stay
		// in the log and are re-offered once the gate clears.
		//
		// snapshotAdoptionPending covers the window BEFORE that gate goes
		// up. THIS is the update that carries a received snapshot — it has
		// no committed entries of its own, so it lands here — and the
		// Peer.Commit above has just handed that snapshot to the LogReader.
		// The state machine adopts it only when the snapshot pool seeds
		// lastApplied, so a piggyback extracted now reaches past the
		// un-adopted snapshot index and produces exactly the gapped batch
		// the step worker was stopped from producing.
		if node.IsRecovering() || node.snapshotAdoptionPending() {
			return commitRecovering, nil
		}
		// Piggyback: check if entries became committable during the
		// commitPending window (processCommitPendingInbox advanced the
		// commit index after handling follower ReplicateResp).
		pb, err := node.peer.GetPiggybackUpdate(node.lastApplied.Load())
		*piggyback = pb
		return commitPiggyback, err
	}

	return commitApply, nil
}

// abandonBatchTail releases the batch items from index start onwards, which
// processBatch's shutdown early-returns would otherwise leave behind. Those
// items were persisted by SaveState but never reached Peer.Commit, so their
// nodes still hold commitPending: without this the step worker never produces
// another update for them and Engine.Stop returns with the shards wedged —
// permanently, since the flag survives into the next batch cycle if the
// worker keeps running.
//
// Dropping the entries is safe on exactly this path: nothing advanced raft's
// processed marker for them (Peer.Commit never ran), they are durable in the
// WAL, and the engine is shutting down — restart re-derives the apply
// frontier from LogDB, the same rationale as the apply worker's shutdown
// drop. Their pooled committed-entry slices are returned so a shutdown does
// not drain proto's entry pool.
func (w *commitWorker) abandonBatchTail(start int) {
	for i := start; i < len(w.batch); i++ {
		node := w.batchNodes[i]
		if node == nil {
			continue
		}
		slog.Warn("commit worker shutting down, batch item abandoned",
			"worker", w.workerID, "shard", w.batch[i].ShardID)
		w.batch[i].ReleaseCommittedEntries()
		node.commitPending.Store(false)
		if w.workSignal != nil {
			w.workSignal.Notify(node.shardID)
		}
	}
}

// piggybackFailures counts GetPiggybackUpdate errors process-wide. A failure
// here is not terminal for the shard — the commit worker falls back to the
// skip-apply path and the entries are re-offered on a later cycle — but it
// PERMANENTLY degrades the multi-node fast path for as long as the underlying
// condition persists, turning every two-phase proposal back into a full extra
// pipeline cycle. A repeated Warn is not something an operator can alert on, so
// the condition is counted as well and the log record is rate-limited.
//
// It is a package counter rather than a RaftMetrics method because RaftMetrics
// is implemented outside this package; adding a method to that interface is a
// change to its implementor, not to the engine.
var piggybackFailures atomic.Uint64

// piggybackReportInterval bounds how often the degradation is logged. The
// condition is persistent (a broken LogReader read path does not heal between
// commits) while the call site runs on every commit cycle.
const piggybackReportInterval = time.Minute

// lastPiggybackReport is the unix-nano time the degradation warning was last
// emitted, process-wide.
var lastPiggybackReport atomic.Int64

// PiggybackFailures returns the process-wide count of GetPiggybackUpdate
// failures. Exposed so the host layer can surface the fast path's health.
func PiggybackFailures() uint64 {
	return piggybackFailures.Load()
}

// notePiggybackFailure records a GetPiggybackUpdate failure and emits a
// rate-limited warning carrying the running total. The compare-and-swap makes
// concurrent commit workers yield exactly one record per interval.
func notePiggybackFailure(shardID, replicaID uint64, err error) {
	total := piggybackFailures.Add(1)
	now := time.Now().UnixNano()
	prev := lastPiggybackReport.Load()
	if prev != 0 && now-prev < int64(piggybackReportInterval) {
		return
	}
	if !lastPiggybackReport.CompareAndSwap(prev, now) {
		return
	}
	slog.Warn("piggyback GetPiggybackUpdate failed; commit fast path degraded",
		"shard", shardID, "replica", replicaID,
		"total_failures", total, "error", err)
}

// failBatchProposals completes every pending proposal carried by a batch that
// is being abandoned, so callers get an immediate error instead of blocking to
// their context deadline.
//
// BOTH entry sets have to be reported, because the proposals waiting on them
// are resolved at different stages and the abandoned batch strands both:
//
//   - EntriesToSave are freshly proposed entries. On a leader this is where a
//     client's proposal FIRST appears; it is not yet committed, so it is absent
//     from CommittedEntries. Reporting only committed entries therefore left the
//     common case — a proposal accepted into a batch that then failed to persist
//     — with no completion at all.
//   - CommittedEntries are resolved on apply, which will never run for this
//     batch.
//
// The node is marked permanently failed by both callers, so no later batch can
// complete either set.
//
// The two sets overlap whenever a shard commits in the same round it appends (a
// single-node shard always does), so they are unioned into ONE call rather than
// reported separately. Callback is an exported interface: reporting the same
// entry twice would silently require every external implementation to be
// idempotent, which its contract does not promise. Entries are deduplicated on
// (Index, Term), which identifies an entry uniquely within a single update.
// The allocation is confined to this terminal path — the node is already failed.
func failBatchProposals(callback Callback, u proto.Update, err error) {
	if callback == nil {
		return
	}
	switch {
	case len(u.EntriesToSave) == 0 && len(u.CommittedEntries) == 0:
		return
	case len(u.EntriesToSave) == 0:
		callback.OnProposalFailed(u.ShardID, u.CommittedEntries, err)
		return
	case len(u.CommittedEntries) == 0:
		callback.OnProposalFailed(u.ShardID, u.EntriesToSave, err)
		return
	}

	type entryID struct{ index, term uint64 }
	all := make([]proto.Entry, 0, len(u.EntriesToSave)+len(u.CommittedEntries))
	all = append(all, u.EntriesToSave...)
	seen := make(map[entryID]struct{}, len(all))
	for i := range all {
		seen[entryID{all[i].Index, all[i].Term}] = struct{}{}
	}
	for i := range u.CommittedEntries {
		id := entryID{u.CommittedEntries[i].Index, u.CommittedEntries[i].Term}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		all = append(all, u.CommittedEntries[i])
	}
	callback.OnProposalFailed(u.ShardID, all, err)
}

// failBatchSaveState handles a terminal SaveState failure (either the
// retry budget was exhausted or shutdown fired during the retry backoff)
// for the current batch. The batch's committed entries were already
// extracted via GetUpdate but never persisted, so they will never reach
// the apply worker.
//
// For every node in the batch it clears commitPending (so the node is not
// stuck forever), marks the node failed, reports a SaveStateError, and
// fails the batch's pending proposals so callers see an immediate error
// instead of blocking until their context deadline. Finally it clears the
// batch buffers for reuse.
//
// Nodes are marked as permanently failed with no automatic reset: their
// internal Raft state has diverged from stable storage (entries were
// extracted, advancing prevState, but never written to WAL). Allowing
// such a node to continue would violate Raft's durability invariant — it
// could acknowledge entries to the leader that were never persisted,
// causing silent data loss on crash. Recovery is to unload and reload the
// shard, reconstructing Raft state from the last consistent WAL
// checkpoint.
func (w *commitWorker) failBatchSaveState(saveErr error) {
	for i, u := range w.batch {
		node := w.batchNodes[i]
		saveStateErr := &SaveStateError{
			ShardID:   u.ShardID,
			ReplicaID: u.ReplicaID,
			Err:       saveErr,
		}
		failShard(node, w.callback, saveStateErr, func() {
			failBatchProposals(w.callback, u, saveStateErr)
		})
		// The latch is tripped before commitPending is cleared, so the step
		// worker never steps the node again.
		node.commitPending.Store(false)
		// Return the pooled committed-entries slice after the callback (which
		// must not retain them). clearBatch below only zeroes the slot, so
		// without this every failed batch permanently removes a slice from
		// proto's entry-slice pool and degrades the apply hot path.
		u.ReleaseCommittedEntries()
	}
	w.clearBatch()
}

// failCommit handles a terminal Peer.Commit failure for one update.
//
// Peer.Commit advances the raft log's processed marker (and applies committed
// config changes) BEFORE its fallible LogReader steps (ApplySnapshot, Append).
// Once it returns an error, entriesToApply can never re-offer this batch, so
// the state machine is permanently missing committed entries. A shard that
// kept running would keep acking the leader and keep serving reads from that
// divergent state machine — a state-machine-safety violation. It is therefore
// treated exactly like a terminal SaveState failure: the node is marked failed
// (no automatic reset; recovery is unload/reload or snapshot recovery),
// commitPending is cleared, the error is reported, and pending proposals for
// the batch are failed so callers see an error instead of blocking to their
// deadline.
func failCommit(node *Node, callback Callback, u proto.Update, err error) {
	commitErr := &CommitError{
		ShardID:   u.ShardID,
		ReplicaID: u.ReplicaID,
		Err:       err,
	}
	slog.Warn("commit Peer.Commit failed, shard marked failed",
		"shard", u.ShardID, "replica", u.ReplicaID, "error", err)
	failShard(node, callback, commitErr, func() {
		failBatchProposals(callback, u, commitErr)
	})
	// The latch is tripped before commitPending is cleared, so the step
	// worker never steps the node again.
	node.commitPending.Store(false)
	// Return the pooled slice only after the callback, which must not
	// retain the entries beyond the call.
	u.ReleaseCommittedEntries()
}

// clearBatchCommitPending releases the pipeline ownership of every node in
// the current batch and wakes its step worker. It is called ONLY from the
// commit worker's panic recovery, where the batch's updates die with the
// panic and would otherwise leave their nodes wedged with commitPending set
// forever.
//
// It is scoped to the batch on purpose: the batch's commitItems have
// already been consumed from commitC and copied into w.batch, so their
// per-shard slots are free and a fresh GetUpdate for them is correct. Nodes
// OUTSIDE the batch either have no update in flight or have one the step
// worker is still enqueueing, and clearing those would let the step worker
// re-extract committed entries raft has not marked processed yet.
//
// Must run on the (unwinding) commit worker goroutine: nothing else owns
// the batch buffers at that point.
func (w *commitWorker) clearBatchCommitPending(workSignal *WorkSignal) {
	for i := range w.batchNodes {
		node := w.batchNodes[i]
		if node == nil {
			continue
		}
		node.commitPending.Store(false)
		if workSignal != nil {
			workSignal.Notify(node.shardID)
		}
	}
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

// recoverAbnormalExit settles the batch a panic or runtime.Goexit cut short,
// before the worker is restarted. cause is the *WorkerPanicError the
// recovery built. It runs on the unwinding worker goroutine, which owns the
// batch.
func (w *commitWorker) recoverAbnormalExit(cause error, workSignal *WorkSignal) {
	// A batch whose SaveState had not completed: GetUpdate already claimed its
	// hard state durable, so the claim is given back, and the next GetUpdate
	// carries the hard state through SaveState again instead of classifying
	// the update ReadOnly — a vote or term that never reached disk is never
	// treated as persisted. Its entries were never marked stable and are
	// re-derived. The shards keep running, as the batch's commitPending is
	// cleared below.
	if w.batchUnpersisted {
		w.batchUnpersisted = false
		for _, node := range w.batchNodes {
			invalidatePersistedStateLocked(node)
		}
	}
	// An update whose Peer.Commit advanced raft's processed marker without
	// its committed entries reaching the apply worker: they can never be
	// re-offered, so the shard is failed and their proposals with it
	// (maybe-applied).
	if node := w.handoffNode; node != nil {
		u := w.handoffUpdate
		w.handoffNode, w.handoffUpdate = nil, proto.Update{}
		err := &CommitError{ShardID: u.ShardID, ReplicaID: u.ReplicaID, Err: cause}
		failShard(node, w.callback, err, func() { failBatchProposals(w.callback, u, err) })
		node.commitPending.Store(false)
		u.ReleaseCommittedEntries()
	}
	// commitC is deliberately NOT drained here, for the same reason as the
	// step-worker recovery: a shard has at most ONE item in flight
	// (commitPending gates the step worker from producing a second), so no
	// shard can have both an item in w.batch and an item in commitC. Clearing
	// a batch node's commitPending therefore cannot hand the step worker a
	// slot whose pointer is still queued, and the items that ARE queued
	// belong to other shards, which the restarted worker processes normally.
	// Draining them instead failed those unrelated healthy shards permanently.
	//
	// Unwedge exactly the nodes whose updates died with the batch. A blanket
	// clear over every node this worker owns would also clear nodes the STEP
	// worker is mid-enqueueForCommit for: that worker sets commitPending
	// before its commitC send, so clearing it lets the very next processReady
	// call GetUpdate again while the first update is still in flight. Raft's
	// processed marker has not advanced yet, so the same committed entries are
	// extracted TWICE (double Peer.Commit, double apply, double
	// ReleaseCommittedEntries on one pooled slice), and getCommitSlot hands
	// back the SAME *commitItem that is still queued — one pointer enqueued
	// twice. Items in the batch have already been consumed from commitC and
	// copied into w.batch, so clearing them is safe.
	w.clearBatchCommitPending(workSignal)
	// The exit may have unwound out of processBatch before clearBatch ran,
	// leaving orderedMsgsBuf holding this batch's VoteResp/HeartbeatResp and
	// batch/commitTimes holding its updates. The restarted worker would append
	// the NEXT batch's messages to those survivors and send them after that
	// unrelated batch's SaveState — sending a vote grant whose HardState was
	// never persisted (Raft PhD 10.2.1). Safe to touch here: the worker
	// goroutine is unwinding, so nothing else owns the batch.
	w.clearBatch()
}

// invalidatePersistedStateLocked gives back the hard-state durability claim
// GetUpdate took for node's extracted update, with node.raftMu held: the
// commit worker reaches the Peer only under it. A node without a Peer has no
// claim to give back.
func invalidatePersistedStateLocked(node *Node) {
	if node == nil || node.peer == nil {
		return
	}
	node.raftMu.Lock()
	defer node.raftMu.Unlock()
	node.peer.InvalidatePersistedState()
}

// applyMembership folds one ordered unload/reload notification into the
// unloaded set. Must run on the commit worker goroutine (single-owner
// invariant on unloadedSet).
func (w *commitWorker) applyMembership(m shardMembership) {
	if m.unload {
		w.unloaded.add(m.shardID)
	} else {
		w.unloaded.remove(m.shardID)
	}
	if m.done != nil {
		close(m.done)
	}
}

// drainUnloadReload non-blocking drains all pending unload and reload
// requests. Called at the top of each run loop iteration to process
// requests that arrived while the worker was busy with SaveState or
// message sending. Requests are applied in the order they were sent
// because they share a single channel.
func (w *commitWorker) drainUnloadReload() {
	for {
		select {
		case m := <-w.membershipC:
			w.applyMembership(m)
		default:
			return
		}
	}
}

// requestUnload sends a shard removal request to the commit worker via
// the membership channel and returns a channel closed once the worker has
// applied it. Safe for concurrent use by the Host goroutine.
//
// The returned channel is what makes an unload a fence rather than a hint.
// The worker applies membership between batches, and while a forward to the
// apply tier is blocked (forwardToApply) — never while a SaveState is running.
// Once it closes: no SaveState of the stopped incarnation is running; every
// item of it still ahead in the current batch is skipped, because processBatch
// re-checks the unloaded set and the node's unloaded mark before it commits or
// persists an item; and every later batch drops its update at admission (see
// admit). Any new LogDB write added to the batch loop must sit behind those
// re-checks, or the fence no longer holds. Without the
// fence, an old incarnation's HardState, entries or adopted snapshot could be
// written AFTER a new incarnation of the same shard had loaded LogDB.
//
// Wait on it with awaitMembership, which also gives up when the worker stops.
func (w *commitWorker) requestUnload(shardID uint64) <-chan struct{} {
	done := make(chan struct{})
	select {
	case w.membershipC <- shardMembership{shardID: shardID, unload: true, done: done}:
	case <-w.stopC:
		close(done)
	}
	return done
}

// awaitMembership waits until done closes or the worker is stopped. A stopped
// worker is draining its final batches for an engine shutdown, which no new
// incarnation can race.
func (w *commitWorker) awaitMembership(done <-chan struct{}) {
	select {
	case <-done:
	case <-w.stopC:
	}
}

// requestReload clears a shard from the unloaded set, allowing the
// commit worker to process Peer.Commit for the shard again. It is
// ordered behind any earlier requestUnload for the same shard because
// both travel on the membership channel. Safe for concurrent use by the
// Host goroutine.
func (w *commitWorker) requestReload(shardID uint64) {
	select {
	case w.membershipC <- shardMembership{shardID: shardID, unload: false}:
	case <-w.stopC:
	}
}

// executePiggyback sends messages with proper free/ordered split, persists the
// piggybacked commit index, advances the peer, sends ordered responses, and
// forwards committed entries to the apply worker. Eliminates the second pipeline
// cycle for multi-node proposals. Called with raftMu already released.
//
// Commit-index durability: this path DOES persist the advanced commit index via
// SaveState before applying or sending ordered responses (see below). An earlier
// version skipped that fsync as an optimization, but that could leave an on-disk
// state machine whose durable lastApplied exceeded the recovered HardState.Commit
// (violating lastApplied <= committed). The state-only SaveState does not truncate
// entries, so the cost is one fsync without touching the durable log.
//
// What it does NOT persist is entries — the logdb.Update below has no Entries
// field, by design. That is why GetPiggybackUpdate withholds ordered messages
// whenever the peer has unsaved entries: an ordered message reaching this
// function is one whose durability requirement this state-only fsync can
// actually satisfy. Do not "optimize" that withholding away without giving this
// path an entry write, or the shard resumes acknowledging RAM-only log indices.
//
// It returns false only when the worker is shutting down (stopC fired mid-
// delivery); in that case the caller must stop processing the batch (the shared
// batch buffers are no longer safe to iterate). It returns true for normal
// completion AND for per-node errors (the caller continues with the next item).
func (w *commitWorker) executePiggyback(node *Node, piggyback proto.Update) bool {
	// Split messages: free-order may go immediately (they do not depend on
	// commit-index durability); ordered responses go AFTER the commit index
	// is durable. Reuse pre-allocated buffers.
	//
	// The ordered half is empty by construction whenever the peer still holds
	// unsaved entries — GetPiggybackUpdate leaves those messages in r.msgs
	// because this function fsyncs hard state only. The split is still needed
	// for the case it does receive: ordered messages backed purely by the
	// term/vote/commit written below.
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

	// Persist the advanced commit index (hard state) BEFORE applying or
	// sending ordered responses. The entries this piggyback APPLIES are
	// already fsynced — GetPiggybackUpdate clamps them to the peer's durable
	// marker — but the commit-index advance was not part of any earlier
	// SaveState. Skipping it would let a crash leave an on-disk state machine
	// whose durable lastApplied exceeds the recovered HardState.Commit — violating lastApplied <= committed and risking
	// re-apply. piggyback.State is that same clamped hard state, so persisting
	// it never advertises a commit past the durable log.
	if w.logdb != nil {
		stateUpdate := []logdb.Update{{
			ShardID:   piggyback.ShardID,
			ReplicaID: piggyback.ReplicaID,
			State:     protoStateToLogDB(&piggyback.State),
		}}
		// Retry with the same backoff schedule as the batch path — a transient
		// fsync hiccup should not give up on the first attempt.
		var saveErr error
		for attempt := range saveStateMaxRetries {
			saveErr = w.logdb.SaveState(stateUpdate)
			if saveErr == nil {
				break
			}
			if attempt < saveStateMaxRetries-1 {
				if cancelled := w.retryWait(attempt); cancelled {
					break
				}
			}
		}
		if saveErr != nil {
			// Terminal for this cycle, but SELF-HEALING — NOT a shard failure.
			// The piggyback's entries were already fsynced by the batch that
			// produced them; only this cycle's hard-state advance failed to
			// persist. Commit has not run yet (it is below), so the processed
			// marker has not moved and the entries are re-offered on a later
			// cycle. Do NOT fail the shard (no handleError/OnShardFailed for a
			// transient disk hiccup): release the un-applied entries, clear
			// commitPending, and wake the step worker to retry. The error is
			// typed *SaveStateError because this WAS a SaveState failure, not a
			// Commit failure.
			//
			// Self-healing is a property of the RAFT STATE too, but only
			// because of the two steps below; it was NOT free.
			//
			//  1. prevState is untouched — NotePiggybackPersisted runs only
			//     after a successful save. hasStateChanged() therefore still
			//     reports this term/vote/commit as unpersisted, so the next
			//     update is not ReadOnly and carries the hard state back
			//     through SaveState. Advancing prevState here (the old
			//     behavior, done inside GetPiggybackUpdate) made a vote granted
			//     in this window invisible, and a later ReadOnly update could
			//     then transmit its RequestVoteResp with no WAL write at all.
			//  2. The ordered messages this cycle drained are handed back to
			//     the peer instead of being dropped. They were withheld
			//     precisely because they depend on this failed write; the next
			//     cycle that persists releases them. Dropping a ReplicateResp
			//     only costs a retransmit, but dropping a VoteResp stalls an
			//     election for a full timeout.
			slog.Warn("piggyback commit-index persist failed after retries",
				"shard", piggyback.ShardID, "replica", piggyback.ReplicaID,
				"error", &SaveStateError{
					ShardID:   piggyback.ShardID,
					ReplicaID: piggyback.ReplicaID,
					Err:       saveErr,
				})
			if len(w.orderedMsgSplitBuf) > 0 {
				requeueMessagesLocked(node, w.orderedMsgSplitBuf)
			}
			piggyback.ReleaseCommittedEntries()
			node.commitPending.Store(false)
			if w.workSignal != nil {
				w.workSignal.Notify(node.shardID)
			}
			return true
		}
	}

	// Record the hard state as durable and advance the processed markers
	// (in-memory). NotePiggybackPersisted is what GetPiggybackUpdate stopped
	// doing: prevState may only claim a state that has actually reached stable
	// storage, and reaching this line is the proof. It takes the Update's
	// State — the commit-clamped one that was written — not the peer's current
	// hard state, which the step worker may already have moved on.
	pbCommitErr := commitPiggybackLocked(node, &piggyback)

	if pbCommitErr != nil {
		failCommit(node, w.callback, piggyback, pbCommitErr)
		return true
	}
	// From here until the apply worker has the committed entries they exist
	// nowhere else: an abnormal exit fails the shard (recoverAbnormalExit).
	w.handoffNode, w.handoffUpdate = node, piggyback

	// Now that the commit index is durable, it is safe to send ordered
	// responses and fire the WAL-durable commit notification.
	if w.sender != nil && len(w.orderedMsgSplitBuf) > 0 {
		w.sendOrderedMessages(w.orderedMsgSplitBuf)
	}
	if w.callback != nil && node.notifyCommit && len(piggyback.CommittedEntries) > 0 {
		w.callback.OnCommitted(piggyback.ShardID, piggyback.CommittedEntries)
	}

	// Forward to apply worker (commitPending cleared by apply worker).
	ai := w.applyPool.Get().(*applyItem)
	ai.update = piggyback
	ai.node = node
	forwarded := w.forwardToApply(ai)
	w.handoffNode, w.handoffUpdate = nil, proto.Update{}
	// Observability after the hand-off: user code that exits abnormally
	// here loses nothing.
	if forwarded && w.metrics != nil {
		w.metrics.IncPiggybackCommit(piggyback.ShardID)
	}
	if !forwarded {
		slog.Warn("commit worker shutting down, piggyback item not delivered to apply worker",
			"shard", piggyback.ShardID)
		ai.update.ReleaseCommittedEntries()
		releaseApplyItem(w.applyPool, ai)
		// Signal the caller to stop: it must NOT keep iterating the
		// shared batch (clearBatch will run in the caller).
		return false
	}
	return true
}

// requeueMessagesLocked hands ordered messages back to the peer with
// node.raftMu held, so the requeue cannot race the step worker's Tick.
//
// It exists as a function so the unlock is deferred. RequeueMessages appends to
// the peer's message buffer, and a panic there (an oversized buffer, a
// torn-down peer) on the SaveState-failure path would strand node.raftMu at
// precisely the worst moment: the shard is already retrying, and the restarted
// commit worker would block on Lock() forever instead of draining the retry.
func requeueMessagesLocked(node *Node, msgs []proto.Message) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()
	node.peer.RequeueMessages(msgs)
}

// commitPiggybackLocked records the piggyback's hard state as durable and
// commits it to the peer with node.raftMu held, returning Peer.Commit's error.
//
// The two calls belong to one critical section: NotePiggybackPersisted may only
// claim a state that reached stable storage, and Commit advances the processed
// markers derived from it, so the step worker must not observe the pair
// half-applied. Everything after it in executePiggyback — the ordered message
// send, the OnCommitted callback, the metrics update and the BLOCKING applyC
// forward — runs with the lock released, exactly as the explicit unlock left
// it: the applyC send can block until the apply worker drains, and raftMu held
// across it would stall the step worker's Tick and the leader heartbeats it
// drives for every shard this worker owns.
//
// The piggyback is taken by pointer to avoid copying a proto.Update on the
// commit hot path; it is neither retained nor mutated here.
func commitPiggybackLocked(node *Node, piggyback *proto.Update) error {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()
	node.peer.NotePiggybackPersisted(piggyback.State)
	return node.peer.Commit(*piggyback)
}
