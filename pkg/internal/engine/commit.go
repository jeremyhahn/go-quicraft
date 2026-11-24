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

// saveStateBackoff is the wait between the saveStateMaxRetries SaveState attempts, about 760ms in total.
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

// shardMembership is one ordered load/unload notification for the commit worker; one channel keeps them FIFO, and done closes once it is applied between batches.
type shardMembership struct {
	shardID uint64
	unload  bool
	done    chan struct{}
}

// commitWorker batches Updates into SaveState calls and sends the deferred ordered messages only after they are durable (Raft thesis 10.2.1).
type commitWorker struct {
	workerID   int
	logdb      logdb.LogDB
	sender     MessageSender
	commitC    chan *commitItem
	batch      []proto.Update
	batchNodes []*Node // parallel slice: batchNodes[i] owns batch[i]

	// batchUnpersisted reports the batch holds updates whose SaveState has not completed; an abnormal exit then gives their claim back.
	batchUnpersisted bool

	// handoffNode and handoffUpdate hold an update whose Peer.Commit ran but whose entries have not reached apply; an abnormal exit fails the shard. Commit-worker goroutine only.
	handoffNode   *Node
	handoffUpdate proto.Update
	logdbBuf      []logdb.Update
	flatBuf       []logdb.Entry // single flat buffer for all entries across the batch
	applyC        chan *applyItem
	applyPool     *sync.Pool
	workSignal    *WorkSignal
	callback      Callback
	stopC         chan struct{}

	// applyStopC is the apply tier's stop channel; forwards give up only when it closes (nil in unit tests, see forwardGiveUpC).
	applyStopC chan struct{}

	// orderedMsgsBuf holds, per shard, the messages to send after SaveState completes; cleared each batch.
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

	// unloaded holds shard IDs removed by UnloadNode; Peer.Commit is skipped for them.
	unloaded *unloadedSet

	// membershipC receives ordered unload/reload notifications. See
	// shardMembership for why both directions share one channel.
	membershipC chan shardMembership

	// sealHold, held and sealWakeC park shards whose log write the sealed barrier refused; run joins every waiter before returning. saveErrs is holdSealedBatch's per-update result.
	sealHold    SealHold
	held        []*heldShard
	sealWakeC   chan struct{}
	sealWaiters sync.WaitGroup
	saveErrs    []error
}

// newCommitWorker creates a commit worker; sender delivers ordered messages after WAL persistence and may be nil in memory mode.
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
		sealWakeC:          make(chan struct{}, 1),
	}
}

// run is the commit worker loop: it waits for an item, yields to gather a batch, processes it, and drains commitC on shutdown.
func (w *commitWorker) run() {
	for {
		// Drain unload/reload requests before blocking on commitC.
		w.drainUnloadReload()
		select {
		case <-w.stopC:
			w.drainAndProcess()
			// Every waiter selects on stopC, so this returns promptly and leaves no node marked sealHeld after shutdown.
			w.sealWaiters.Wait()
			w.releaseWoken()
			return
		case m := <-w.membershipC:
			w.applyMembership(m)
			continue
		case <-w.sealWakeC:
			w.releaseWoken()
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

// drainAndProcess drains commitC without blocking and processes the remaining items at shutdown.
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

// admit adds item to the batch, or drops it when its node is unloaded (Node.isUnloaded), so an unloaded incarnation never writes LogDB after UnloadNode returns.
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

// sendOrderedMessages sends the messages that must follow durable state, with the step worker's InstallSnapshot routing.
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

// forwardGiveUpC returns applyStopC, or stopC when applyStopC is nil (unit tests).
func (w *commitWorker) forwardGiveUpC() <-chan struct{} {
	if w.applyStopC != nil {
		return w.applyStopC
	}
	return w.stopC
}

// forwardToApply hands ai to the apply worker, applying membership requests while blocked; false means it gave up because the apply tier is stopping.
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

// retryWait waits for attempt's backoff and returns true if stopC closed; noRetryDelay and retryHookForTest are test hooks.
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

	// Convert and persist only when LogDB is configured.
	var saveErr error
	if w.logdb != nil {
		w.logdbBuf, w.flatBuf = protoUpdatesToLogDBReuse(w.logdbBuf, w.flatBuf, w.batch)
		for attempt := range saveStateMaxRetries {
			saveErr = w.logdb.SaveState(w.logdbBuf)
			if saveErr == nil {
				break
			}
			// A sealed barrier will not unseal within the backoff; the
			// hold below settles the batch instead.
			if w.isSealedSaveErr(saveErr) {
				break
			}
			if attempt < saveStateMaxRetries-1 {
				if cancelled := w.retryWait(attempt); cancelled {
					// Shutdown during backoff: the batch was never persisted, so clear commitPending and fail its proposals.
					w.failBatchSaveState(saveErr)
					observe()
					return
				}
			}
		}
	}

	if saveErr != nil {
		// A sealed refusal parks the refused shards and lets every update
		// that persisted on its own carry on below (holdSealedBatch); any
		// other failure fails the batch.
		if !w.isSealedSaveErr(saveErr) {
			w.failBatchSaveState(saveErr)
			observe()
			return
		}
		if !w.holdSealedBatch() {
			w.batchUnpersisted = false
			observe()
			return
		}
	}
	w.batchUnpersisted = false
	// The latency spans from step worker Update production to SaveState
	// completion; it is reported only once the batch is settled (observe).
	persistedAt = time.Now()

	// Send ordered messages only now that state is durable (Raft thesis 10.2.1).
	if w.sender != nil {
		for _, msgs := range w.orderedMsgsBuf {
			if len(msgs) > 0 {
				w.sendOrderedMessages(msgs)
			}
		}
	}

	// Notify locally proposed entries of this batch that they are WAL-durable, per update.
	if w.callback != nil {
		for i, u := range w.batch {
			node := w.batchNodes[i]
			if node.notifyCommit && len(u.EntriesToSave) > 0 {
				w.callback.OnCommitted(u.ShardID, u.EntriesToSave)
			}
		}
	}

	// Commit each update to its own node (batchNodes[i]), not by shardID, under raftMu, and forward to apply.
	for i, u := range w.batch {
		stop := w.commitBatchItem(w.batchNodes[i], u)
		// The item reached its terminal hand-off, so retire it from the batch.
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

// commitBatchItem commits one persisted update to its own node and hands its entries on; stop reports the worker is shutting down.
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

	// Incarnation guard: skip an item whose node UnloadNode is tearing down (Node.isUnloaded).
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
		// A snapshot recovery owns the state machine: clear commitPending and re-notify; entries are re-offered later.
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
		// Piggyback path, run with raftMu released; false on shutdown abandons the rest of the batch.
		if !w.executePiggyback(node, piggyback) {
			node.commitPending.Store(false)
			return true
		}
		return false
	}

	// commitApply: the update carries committed entries; the blocking forward to apply must not run under raftMu.
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
	// commitRecovering: no entries, and the state machine cannot take any yet (recovery or an un-adopted snapshot).
	commitRecovering
	// commitPiggyback: no entries and a piggyback update was requested; the error is GetPiggybackUpdate's.
	commitPiggyback
)

// commitUpdateLocked commits update to its node's Peer under raftMu (released by defer) and may fetch a piggyback; failure handling, piggyback execution and the apply forward stay outside the lock.
func commitUpdateLocked(node *Node, u *proto.Update, piggyback *proto.Update) (commitStatus, error) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()

	if err := node.peer.Commit(*u); err != nil {
		return commitFailed, err
	}

	if len(u.CommittedEntries) == 0 {
		// No extraction while a snapshot recovery owns the state machine or a received snapshot awaits adoption.
		if node.IsRecovering() || node.snapshotAdoptionPending() {
			return commitRecovering, nil
		}
		// Piggyback: check if entries became committable during the
		// commitPending window (processCommitPendingInbox advanced the
		// commit index after handling follower ReplicateResp).
		pb, err := node.peer.GetPiggybackUpdate(node.lastApplied.Load())
		*piggyback = pb
		if err == nil && len(pb.CommittedEntries) > 0 {
			// Publish the commit markers before the piggyback entries reach apply.
			node.publishLogMarkers()
		}
		return commitPiggyback, err
	}

	return commitApply, nil
}

// abandonBatchTail releases items from start onwards at shutdown: clears commitPending and returns their pooled entry slices; restart re-derives them from LogDB.
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

// piggybackFailures counts GetPiggybackUpdate errors process-wide; the log record is rate-limited.
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

// failBatchProposals fails every pending proposal in an abandoned batch, the union of EntriesToSave and CommittedEntries deduplicated on (Index, Term).
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

// failBatchSaveState handles a terminal SaveState failure: clears commitPending, marks each node permanently failed, reports SaveStateError and fails the batch's proposals.
func (w *commitWorker) failBatchSaveState(saveErr error) {
	for i, u := range w.batch {
		w.failSaveState(w.batchNodes[i], u, saveErr)
	}
	w.clearBatch()
}

// failSaveState fails one update's shard on a SaveState error.
func (w *commitWorker) failSaveState(node *Node, u proto.Update, saveErr error) {
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
	// Return the pooled committed-entries slice after the callback.
	u.ReleaseCommittedEntries()
}

// failCommit treats a Peer.Commit failure as terminal: the node is marked failed, commitPending cleared, the error reported and the batch's proposals failed.
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

// clearBatchCommitPending clears commitPending for the batch's nodes only and wakes their step workers; panic recovery only, on the commit worker goroutine.
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

// recoverAbnormalExit settles the batch a panic or Goexit cut short, on the unwinding worker goroutine.
func (w *commitWorker) recoverAbnormalExit(cause error, workSignal *WorkSignal) {
	// SaveState had not completed: give back the hard-state durability claim so the next GetUpdate persists it again.
	if w.batchUnpersisted {
		w.batchUnpersisted = false
		for _, node := range w.batchNodes {
			invalidatePersistedStateLocked(node)
		}
	}
	// Peer.Commit ran but the entries never reached apply: fail the shard and its proposals (maybe-applied).
	if node := w.handoffNode; node != nil {
		u := w.handoffUpdate
		w.handoffNode, w.handoffUpdate = nil, proto.Update{}
		err := &CommitError{ShardID: u.ShardID, ReplicaID: u.ReplicaID, Err: cause}
		failShard(node, w.callback, err, func() { failBatchProposals(w.callback, u, err) })
		node.commitPending.Store(false)
		u.ReleaseCommittedEntries()
	}
	// Do not drain commitC (its items belong to other shards); clear commitPending only for the batch's own nodes.
	w.clearBatchCommitPending(workSignal)
	// Clear the batch buffers so the restarted worker never sends this batch's ordered messages after another batch's SaveState.
	w.clearBatch()
}

// invalidatePersistedStateLocked gives back GetUpdate's hard-state durability claim for node, with raftMu held.
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
		w.releaseUnloaded(m.shardID)
	} else {
		w.unloaded.remove(m.shardID)
	}
	if m.done != nil {
		close(m.done)
	}
}

// drainUnloadReload applies pending unload and reload requests in send order without blocking.
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

// requestUnload asks the commit worker to unload a shard and returns a channel closed once applied, after which no LogDB write of that incarnation can happen. Wait with awaitMembership.
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

// requestReload readmits a shard, ordered behind any earlier requestUnload.
func (w *commitWorker) requestReload(shardID uint64) {
	select {
	case w.membershipC <- shardMembership{shardID: shardID, unload: false}:
	case <-w.stopC:
	}
}

// executePiggyback sends free messages, persists the commit index (hard state only, no entries), sends ordered responses and forwards entries to apply; false means shutdown.
func (w *commitWorker) executePiggyback(node *Node, piggyback proto.Update) bool {
	// Free-order messages go now; ordered ones after the hard state is durable.
	w.freeOrderBuf = w.freeOrderBuf[:0]
	w.orderedMsgSplitBuf = w.orderedMsgSplitBuf[:0]
	if len(piggyback.Messages) > 0 {
		for j := range piggyback.Messages {
			if proto.IsFreeOrder(&piggyback.Messages[j]) {
				w.freeOrderBuf = append(w.freeOrderBuf, piggyback.Messages[j])
			} else {
				w.orderedMsgSplitBuf = append(w.orderedMsgSplitBuf, piggyback.Messages[j])
			}
		}
		if len(w.freeOrderBuf) > 0 && w.sender != nil {
			w.sender.Send(w.freeOrderBuf)
		}
	}

	// Persist the clamped commit index before applying or sending ordered responses, keeping lastApplied <= committed.
	if w.logdb != nil {
		saveErr := w.savePiggybackState(&piggyback)
		if saveErr != nil && w.isSealedSaveErr(saveErr) {
			// Sealed: hold the shard until unseal, requeueing the ordered messages with the rewind.
			w.holdSealed(node, piggyback, w.orderedMsgSplitBuf)
			return true
		}
		if saveErr != nil {
			// Self-healing, not a shard failure: release the entries, clear commitPending, requeue the ordered messages and wake the step worker; prevState stays unpersisted so the next update carries the hard state again.
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

	// Record the written, commit-clamped hard state as durable and advance the processed markers.
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

// savePiggybackState persists the piggyback's hard state with the batch backoff, never retrying a sealed-barrier refusal.
func (w *commitWorker) savePiggybackState(piggyback *proto.Update) error {
	stateUpdate := []logdb.Update{{
		ShardID:   piggyback.ShardID,
		ReplicaID: piggyback.ReplicaID,
		State:     protoStateToLogDB(&piggyback.State),
	}}
	var saveErr error
	for attempt := range saveStateMaxRetries {
		saveErr = w.logdb.SaveState(stateUpdate)
		if saveErr == nil || w.isSealedSaveErr(saveErr) {
			break
		}
		if attempt < saveStateMaxRetries-1 {
			if cancelled := w.retryWait(attempt); cancelled {
				break
			}
		}
	}
	return saveErr
}

// requeueMessagesLocked hands ordered messages back to the peer under raftMu, unlocked by defer.
func requeueMessagesLocked(node *Node, msgs []proto.Message) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()
	node.peer.RequeueMessages(msgs)
}

// commitPiggybackLocked marks the piggyback hard state durable and commits it under raftMu as one critical section; everything after runs unlocked.
func commitPiggybackLocked(node *Node, piggyback *proto.Update) error {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()
	node.peer.NotePiggybackPersisted(piggyback.State)
	return node.peer.Commit(*piggyback)
}
