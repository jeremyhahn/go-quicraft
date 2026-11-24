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
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// commitItem carries a proto.Update and its Node from step to commit worker; orderedMsgs are sent only after the WAL persists (Raft thesis 10.2.1).
type commitItem struct {
	update      proto.Update
	node        *Node
	createdAt   time.Time       // for commit latency measurement
	orderedMsgs []proto.Message // messages to send AFTER WAL persist
}

// stepWorker ticks and steps messages for its shard partition, producing Updates for the commit pipeline; sends are non-blocking.
type stepWorker struct {
	workerID     int
	tickInterval time.Duration
	nodesMu      sync.RWMutex
	nodes        map[uint64]*Node
	sender       MessageSender
	workSignal   *WorkSignal
	commitSlots  map[uint64]*commitItem
	commitC      chan *commitItem
	callback     Callback
	prevLeaders  map[uint64]uint64 // shardID -> last published leaderID
	// prevLeaderTerms holds the term published with each leader, so a re-election at a higher term still fires OnLeaderUpdated.
	prevLeaderTerms map[uint64]uint64
	// prevReadyTerms records the term whose OnLeaderReady was published for the current pair; cleared on any leader change.
	prevReadyTerms map[uint64]uint64
	stopC          chan struct{}

	// listeners runs user notifications on a goroutine holding no engine lock, since a listener calling Host.StopShard would deadlock on nodesMu.
	listeners *listenerDispatcher

	// snapshots is the engine's snapshot pool, through which redriveRecovery
	// re-queues a rejected snapshot recovery. Nil in tests that build a step
	// worker alone, which then re-drives nothing.
	snapshots *snapshotPool

	// enqueuingNode is the node mid hand-off to the commit pipeline, the one a step-worker panic recovery must unwedge; step-worker goroutine only.
	enqueuingNode *Node

	// inFlight is what this worker took from a node and has not handed on; recoverAbnormalExit settles it (settleInFlight).
	inFlight stepInFlight

	// memCommitted and memCommittedUpdate are a memory-mode commit not yet on applyC; recoverAbnormalExit fails the shard.
	memCommitted       *Node
	memCommittedUpdate proto.Update

	// extractedNode is the node whose update GetUpdate extracted but no path has persisted yet, a wider window than enqueuingNode.
	extractedNode *Node

	// inboxNode and inboxStep record which drained inbox message raft is stepping; see answerDroppedInstalls.
	inboxNode *Node
	inboxStep int
	// inboxCommittedBefore is raft's committed index just before the local
	// install at inboxStep was stepped, so a panic can still tell adoption.
	inboxCommittedBefore uint64

	inboxBuf   []proto.Message // reusable buffer for draining node inboxes
	proposeBuf []proto.Entry   // reusable buffer for batching inbox proposals
	// commitProposeBuf is a separate buffer for the commit-pending drain, so a forwarded Propose never aliases the queue's live buffer.
	commitProposeBuf []proto.Entry
	// commitProposeBatch is the proposal-queue batch id of commitProposeBuf,
	// for HandOffProposals.
	commitProposeBatch uint64
	readKeyBuf         []uint64                   // reusable buffer for draining ReadIndex keys
	heartbeatRespBuf   []proto.Message            // reusable buffer for HeartbeatResp separation
	otherMsgBuf        []proto.Message            // reusable buffer for non-HeartbeatResp messages
	deferredSnapshots  map[uint64][]proto.Message // per-shard deferred InstallSnapshot messages

	// freeOrderBuf and orderedMsgBuf are reused buffers for splitting messages by persistence ordering.
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

// newStepWorker creates a step worker for the given partition, ticking every tickInterval (HostConfig.RTTMillisecond).
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
		prevLeaderTerms:   make(map[uint64]uint64),
		prevReadyTerms:    make(map[uint64]uint64),
		stopC:             stopC,
		inboxBuf:          make([]proto.Message, 0, 64),
		proposeBuf:        make([]proto.Entry, 0, 64),
		commitProposeBuf:  make([]proto.Entry, 0, 64),
		readKeyBuf:        make([]uint64, 0, 64),
		freeOrderBuf:      make([]proto.Message, 0, 16),
		orderedMsgBuf:     make([]proto.Message, 0, 16),
		deferredSnapshots: make(map[uint64][]proto.Message),
		metrics:           metrics,
	}
}

// run is the step worker's main loop; after each wake it drains a pending tick so heartbeats are not starved by work notifications.
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

// drainTick consumes a pending tick without blocking, so tick() runs at least once per cycle.
func (w *stepWorker) drainTick(ticker *time.Ticker) {
	select {
	case <-ticker.C:
		w.tick()
	default:
	}
}

// tick advances the Raft clock of every loaded node except sealHeld or paused ones, including commitPending nodes for leader liveness.
func (w *stepWorker) tick() {
	w.nodesMu.RLock()
	defer w.nodesMu.RUnlock()
	for shardID, node := range w.nodes {
		// A sealHeld or paused shard is not ticked.
		if node.IsFailed() || node.sealHeld.Load() || node.paused.Load() || node.peer == nil {
			continue
		}
		w.stepInboxBeforeTick(shardID, node)
		campaignTerm, campaignLaunched := w.tickNodeLocked(node)
		// Publish here: a CheckQuorum step-down inside Tick produces no update, so no other publish site sees it.
		if campaignLaunched {
			w.fireCampaign(shardID, node, campaignTerm)
		}
		w.publishLeaderChange(shardID, node)
	}
}

// stepInboxBeforeTick steps messages already in the inbox before the tick, so CheckQuorum never ignores a reply that already arrived (Raft thesis 6.2).
func (w *stepWorker) stepInboxBeforeTick(shardID uint64, node *Node) {
	// Skip the drain when the inbox is empty, which is one atomic load.
	if node.inboxQ.Empty() {
		return
	}
	if node.commitPending.Load() {
		w.processCommitPendingInbox(shardID, node)
		return
	}
	w.drainAndHandleInbox(shardID, node, node.IsRecovering())
}

func (w *stepWorker) tickNodeLocked(node *Node) (campaignTerm uint64, campaignLaunched bool) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()
	node.peer.Tick()
	// While commitPending, send only free-order messages Tick produced; ordered ones wait for the WAL.
	if node.commitPending.Load() {
		if msgs := node.peer.DrainFreeOrderMessages(); len(msgs) > 0 {
			// Detect a pre-vote campaign under the lock; the callback fires after return.
			campaignTerm, campaignLaunched = scanPreVoteCampaign(msgs)
			w.sendMessages(msgs)
		}
	}
	// Publish log markers and the diagnostic snapshot under raftMu on every tick.
	node.publishLogMarkers()
	node.publishDiagLocked()
	return campaignTerm, campaignLaunched
}

// processReady handles every loaded node, since tick-driven changes are not signalled; commitPending nodes skip extraction and sealHeld nodes step nothing.
func (w *stepWorker) processReady() {
	// Clear the ready set; every loaded node is iterated below.
	_ = w.workSignal.Swap(w.workerID)
	w.nodesMu.RLock()
	defer w.nodesMu.RUnlock()
	for shardID, node := range w.nodes {
		if node.IsFailed() {
			continue
		}
		// A paused shard steps nothing and its queues stay queued.
		if node.paused.Load() {
			w.discardPaused(shardID, node)
			continue
		}
		// During recovery the node stays in the protocol: only committed-entry extraction and InstallSnapshot are suppressed, re-reading the flag at each use.
		// A commitPending node steps its inbox under raftMu for ReadOnly updates only; a sealHeld node steps nothing.
		if node.commitPending.Load() {
			if node.peer != nil && !node.sealHeld.Load() {
				w.processCommitPendingInbox(shardID, node)
			}
			continue
		}
		// Drain proposals and transport messages, then route them through the peer.
		if node.peer != nil {
			// Client work stays queued during recovery so the queues apply backpressure; Raft traffic keeps flowing.
			if !node.IsRecovering() {
				w.drainAndHandleProposals(shardID, node)
				// Update the rate limiter right after appending, before commit drains the in-memory log.
				node.peer.RateLimited()
				w.drainAndHandleReadIndex(shardID, node)
				// Deferred snapshots are re-injected only once the pipeline can
				// accept them: not while a recovery is installing one.
				w.redeliverDeferredSnapshots(shardID, node)
			}
			w.drainAndHandleInbox(shardID, node, node.IsRecovering())
			node.peer.FlushReadIndex()
			// After the inbox, so an install it just adopted is seen.
			w.redriveRecovery(node)
		}
		// Skip a shard without GetUpdate while the output channel is full; this worker is its only producer, so a free slot guarantees a non-blocking send.
		outLen, outCap := len(w.commitC), cap(w.commitC)
		if w.memoryMode {
			outLen, outCap = len(w.applyC), cap(w.applyC)
		}
		if outCap > 0 && outLen == outCap {
			// Flush free-order messages before skipping, reporting any pre-vote campaign; ordered messages wait for durability.
			if node.peer != nil {
				if msgs := node.peer.DrainFreeOrderMessages(); len(msgs) > 0 {
					w.sendMessages(msgs)
					w.reportFreeOrderCampaign(shardID, node, msgs)
				}
			}
			continue
		}
		lastApplied := node.lastApplied.Load()
		hasUpdate := node.peer != nil && node.peer.HasUpdate(lastApplied)
		if !hasUpdate {
			continue
		}
		// Extract no committed entries while a recovery owns the state machine or a just-installed snapshot awaits adoption.
		holdEntries := node.IsRecovering() || node.snapshotAdoptionPending()
		update, err := node.peer.GetUpdate(!holdEntries, lastApplied)
		if err != nil {
			// GetUpdate fails only when committed entries cannot be read; fail the shard closed.
			slog.Error("raft GetUpdate failed; failing the shard",
				"shard_id", shardID, "replica_id", node.replicaID, "error", err)
			failShard(node, w.callback, err, nil)
			continue
		}
		// Publish log markers for FollowerRead and the snapshot pool.
		node.publishLogMarkers()
		// Until the update is persisted, discardExtractedUpdate can take back GetUpdate's durability claim.
		w.extractedNode = node
		// The read states GetUpdate took from the Peer exist nowhere else
		// until OnReadyToRead delivers them (processCallbacks clears this).
		if len(update.ReadyToRead) > 0 {
			w.inFlight = stepInFlight{node: node, readStates: update.ReadyToRead}
		}
		// Detect leader change, ReadyToRead, and campaign launch.
		w.processCallbacks(shardID, node, update)
		// Read-only updates skip the commit/apply pipeline.
		if update.ReadOnly {
			// A ReadOnly update carries no hard state to persist, so there is
			// nothing left owed once its messages are away.
			w.extractedNode = nil
			if len(update.Messages) > 0 {
				w.sendMessages(update.Messages)
			}
			continue
		}
		// Memory mode: commit inline and forward to applyC; Tick and Commit share this goroutine.
		if w.memoryMode {
			if w.commitMemoryMode(node, update) {
				return
			}
			// Commit succeeded, so the state this update carried is as durable
			// as memory mode ever makes it.
			w.extractedNode = nil
			continue
		}
		// WAL-mode pipeline: split messages by persistence ordering
		// and queue for the commit worker.
		if w.enqueueForCommit(shardID, node, update) {
			return
		}
		// The commit worker owns the update and its SaveState from here.
		w.extractedNode = nil
	}
}

// redeliverDeferredSnapshots re-injects InstallSnapshot messages deferred during commitPending.
func (w *stepWorker) redeliverDeferredSnapshots(shardID uint64, node *Node) {
	deferred, ok := w.deferredSnapshots[shardID]
	if !ok || len(deferred) == 0 {
		return
	}
	// Drop only delivered messages; keep those the full inbox refused.
	kept := deferred[:0]
	for _, msg := range deferred {
		if !node.redeliver(msg) {
			kept = append(kept, msg)
		}
	}
	// Zero the tail so retained references to dropped messages are released.
	for i := len(kept); i < len(deferred); i++ {
		deferred[i] = proto.Message{}
	}
	w.deferredSnapshots[shardID] = kept
}

// discardPaused empties a paused node's inbox, answering local InstallSnapshots InstallSuperseded.
func (w *stepWorker) discardPaused(shardID uint64, node *Node) {
	w.inboxBuf = node.DrainInbox(w.inboxBuf)
	for i := range w.inboxBuf {
		if msg := &w.inboxBuf[i]; msg.Type == proto.InstallSnapshot && msg.Origin == proto.OriginLocal {
			node.signalInstallVerdict(msg.Snapshot.Index, InstallVerdict{Outcome: InstallSuperseded})
		}
	}
	if deferred, ok := w.deferredSnapshots[shardID]; ok {
		for i := range deferred {
			if deferred[i].Origin == proto.OriginLocal {
				node.signalInstallVerdict(deferred[i].Snapshot.Index, InstallVerdict{Outcome: InstallSuperseded})
			}
		}
		delete(w.deferredSnapshots, shardID)
	}
}

// deferSnapshot buffers the highest-index local InstallSnapshot per shard until commitPending clears, answering evicted ones InstallSuperseded.
func (w *stepWorker) deferSnapshot(node *Node, shardID uint64, msg proto.Message) {
	buf := w.deferredSnapshots[shardID]
	if len(buf) == 0 {
		w.deferredSnapshots[shardID] = append(buf[:0], msg)
		return
	}
	switch {
	case msg.Snapshot.Index > buf[0].Snapshot.Index:
		node.signalInstallVerdict(buf[0].Snapshot.Index, InstallVerdict{Outcome: InstallSuperseded})
		buf[0] = msg
	case msg.Snapshot.Index == buf[0].Snapshot.Index:
		buf[0] = msg // the same install again; its awaited verdict is still pending
	default:
		node.signalInstallVerdict(msg.Snapshot.Index, InstallVerdict{Outcome: InstallSuperseded})
	}
	w.deferredSnapshots[shardID] = buf[:1]
}

// filterSnapshots moves InstallSnapshot messages into the deferred buffer and returns the rest; use the returned slice.
func (w *stepWorker) filterSnapshots(shardID uint64, node *Node, msgs []proto.Message) []proto.Message {
	hasSnapshot := false
	for i := range msgs {
		if msgs[i].Type == proto.InstallSnapshot {
			hasSnapshot = true
			break
		}
	}
	if !hasSnapshot {
		return msgs
	}
	// Buffer InstallSnapshot locally; redelivering to the inbox loops.
	filtered := msgs[:0:0]
	for i := range msgs {
		if msgs[i].Type == proto.InstallSnapshot {
			// Drop wire-origin InstallSnapshot: only a local one is legitimate, and a forged index would evict it.
			if msgs[i].Origin != proto.OriginLocal {
				slog.Warn("dropping wire-origin InstallSnapshot",
					"shard_id", shardID,
					"snapshot_index", msgs[i].Snapshot.Index,
					"from", msgs[i].From,
				)
				continue
			}
			w.deferSnapshot(node, shardID, msgs[i])
		} else {
			filtered = append(filtered, msgs[i])
		}
	}
	return filtered
}

// drainAndHandleInbox steps transport messages through the peer, deferring InstallSnapshot while recovering.
func (w *stepWorker) drainAndHandleInbox(shardID uint64, node *Node, recovering bool) {
	w.inboxBuf = node.DrainInbox(w.inboxBuf)
	if recovering {
		w.inboxBuf = w.filterSnapshots(shardID, node, w.inboxBuf)
	}
	// Cleared explicitly after the loop, never by a defer: a deferred clear
	// would run during a panic's unwind, before the recovery that reads it.
	w.inboxNode = node
	for i := range w.inboxBuf {
		w.inboxStep = i
		var handleStart time.Time
		if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
			handleStart = time.Now()
		}
		msg := &w.inboxBuf[i]
		localInstall := msg.Type == proto.InstallSnapshot && msg.Origin == proto.OriginLocal
		if localInstall && node.IsStopped() {
			// Being unloaded: a stopped node adopts nothing. Its waiting
			// receive was already answered InstallAbandoned by Stop.
			continue
		}
		if localInstall {
			// Poisoned first, so a panic inside LogMarkers itself cannot be
			// classified against a previous install's reading.
			w.inboxCommittedBefore = math.MaxUint64
			w.inboxCommittedBefore, _ = node.peer.LogMarkers()
		}
		err := node.peer.Handle(*msg)
		if localInstall {
			node.signalInstallVerdict(msg.Snapshot.Index, installVerdictFor(node, msg.Snapshot.Index, w.inboxCommittedBefore, err))
		}
		if err != nil {
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
	w.inboxNode = nil
	if node.peer == nil {
		return
	}
	if keys := node.peer.DrainReadRejects(); len(keys) > 0 {
		w.inFlight = stepInFlight{node: node, readKeys: keys}
		w.failRejectedReads(shardID, keys)
		w.inFlight = stepInFlight{}
	}
}

// failRejectedReads fails the forwarded reads the leader refused with
// ErrReadIndexNotReady. It must run outside node.raftMu: OnReadIndexFailed is
// a host callback. The host fails each key's batch aliases with it.
func (w *stepWorker) failRejectedReads(shardID uint64, keys []uint64) {
	if w.callback == nil {
		return
	}
	slog.Debug("forwarded read index refused by the leader",
		"shard_id", shardID,
		"num_batches", len(keys),
	)
	w.callback.OnReadIndexFailed(shardID, keys, raft.ErrReadIndexNotReady)
}

// answerDroppedInstalls answers local InstallSnapshots a panic dropped from the inbox batch; must not panic itself.
func (w *stepWorker) answerDroppedInstalls(cause error) {
	node := w.inboxNode
	if node == nil {
		return
	}
	w.inboxNode = nil
	defer func() {
		if r := recover(); r != nil {
			shardID := node.shardID
			supervise.LogContained(func() {
				slog.Error("answering dropped snapshot installs panicked",
					"shard_id", shardID, "panic", fmt.Sprint(r))
			})
		}
	}()
	if w.inboxStep >= len(w.inboxBuf) {
		return
	}
	for i := w.inboxStep + 1; i < len(w.inboxBuf); i++ {
		if msg := &w.inboxBuf[i]; msg.Type == proto.InstallSnapshot && msg.Origin == proto.OriginLocal {
			node.signalInstallVerdict(msg.Snapshot.Index, InstallVerdict{Outcome: InstallSuperseded})
		}
	}
	inHand := &w.inboxBuf[w.inboxStep]
	if inHand.Type != proto.InstallSnapshot || inHand.Origin != proto.OriginLocal {
		return
	}
	index := inHand.Snapshot.Index
	// Already answered or stopped: leave it.
	if !node.awaitingInstall(index) || node.IsStopped() {
		return
	}
	if w.inboxCommittedBefore < index && committedReached(node, index) {
		node.signalInstallVerdict(index, InstallVerdict{Outcome: InstallAdopted})
		return
	}
	// Fail under the node's write lease, only tried, so no later incarnation receives the report.
	if !node.TryAcquireWriteLease() {
		return
	}
	defer node.ReleaseWriteLease()
	failShard(node, w.callback, cause, nil)
	shardID, replicaID := node.shardID, node.replicaID
	supervise.LogContained(func() {
		slog.Error("step worker panicked inside a snapshot install; failed the shard",
			"shard_id", shardID, "replica_id", replicaID,
			"snapshot_index", index, "error", cause)
	})
}

// committedReached reports whether raft's committed index is at or past index,
// and false if reading it panics: it runs inside a panic recovery.
func committedReached(node *Node, index uint64) (reached bool) {
	defer func() {
		if recover() != nil {
			reached = false
		}
	}()
	committed, _ := node.peer.LogMarkers()
	return committed >= index
}

// installVerdictFor classifies a stepped local InstallSnapshot as adopted, declined or refused.
func installVerdictFor(node *Node, index, committedBefore uint64, err error) InstallVerdict {
	if err != nil {
		return InstallVerdict{Outcome: InstallRefused, Err: err}
	}
	if committedAfter, _ := node.peer.LogMarkers(); committedBefore < index && committedAfter >= index {
		return InstallVerdict{Outcome: InstallAdopted}
	}
	return InstallVerdict{Outcome: InstallDeclined}
}

// enqueueForCommit sends free-order messages now and queues the update with ordered ones for the commit worker (Raft thesis 10.2.1); true means stop.
func (w *stepWorker) enqueueForCommit(shardID uint64, node *Node, update proto.Update) bool {
	// Record the hand-off before raising commitPending.
	w.enqueuingNode = node
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
			if proto.IsFreeOrder(&update.Messages[i]) {
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
		w.enqueuingNode = nil
		return false
	case <-w.stopC:
		node.commitPending.Store(false)
		w.enqueuingNode = nil
		return true
	}
}

// drainAndHandleProposals routes queued proposals through the peer as one batched Propose.
func (w *stepWorker) drainAndHandleProposals(shardID uint64, node *Node) {
	batch, id := node.DrainProposalBatch(w.proposeBuf)
	w.proposeBuf = batch
	if len(batch) == 0 {
		return
	}
	// From here until Peer.Handle has the batch it exists nowhere else; it is
	// tracked so an abnormal exit settles it (settleInFlight).
	w.inFlight = stepInFlight{node: node, proposals: batch}
	// Declared handed to raft only as Peer.Handle is entered. A closed queue
	// refuses: the node failed or stopped, and the host has already reported
	// these proposals never taken, so they are refused definitely here too.
	if !node.HandOffProposals(id) {
		w.inFlight = stepInFlight{}
		if w.callback != nil {
			w.callback.OnProposalRejected(shardID, batch, handOffRefusal(node))
		}
		return
	}
	w.inFlight.handed = true
	err := node.peer.Handle(proto.Message{
		Type:    proto.Propose,
		Entries: batch,
		Origin:  proto.OriginLocal,
	})
	if err != nil {
		// Raft refused the whole batch; notify the host to fail the pending proposals.
		w.inFlight.handed = false
		slog.Warn("batched proposal failed",
			"shard_id", shardID,
			"replica_id", node.ReplicaID(),
			"num_entries", len(batch),
			"error", err,
		)
		if w.callback != nil {
			w.callback.OnProposalRejected(shardID, batch, err)
		}
	}
	w.inFlight = stepInFlight{}
	// Observability last, outside the window: user code that exits
	// abnormally here loses nothing.
	if w.metrics != nil {
		w.metrics.ObserveProposalBatchSize(shardID, len(batch))
	}
	slog.Debug("proposal batch processed",
		"shard", shardID,
		"replica", node.ReplicaID(),
		"batch_size", len(batch),
	)
}

// drainAndHandleReadIndex submits one ReadIndex for the queued keys, registering the rest as aliases of the first.
func (w *stepWorker) drainAndHandleReadIndex(shardID uint64, node *Node) {
	w.readKeyBuf = node.DrainReadIndex(w.readKeyBuf)
	if len(w.readKeyBuf) > 0 {
		// The drained keys exist nowhere else until Peer.Handle has them;
		// they are tracked so an abnormal exit fails them (settleInFlight).
		w.inFlight = stepInFlight{node: node, readKeys: w.readKeyBuf}
		batchKey := w.readKeyBuf[0]
		// Register aliases before submitting; a single-node read resolves immediately.
		if len(w.readKeyBuf) > 1 && w.callback != nil {
			w.callback.OnReadIndexBatched(shardID, batchKey, w.readKeyBuf[1:])
		}
		err := node.peer.Handle(proto.Message{
			Type:   proto.ReadIndex,
			From:   node.ReplicaID(),
			Hint:   batchKey,
			Origin: proto.OriginLocal,
		})
		if err != nil {
			slog.Warn("batched ReadIndex failed",
				"shard_id", shardID,
				"replica_id", node.ReplicaID(),
				"num_requests", len(w.readKeyBuf),
				"error", err,
			)
			// Fail every pending read in the batch; raft never received it.
			if w.callback != nil {
				w.callback.OnReadIndexFailed(shardID, w.readKeyBuf, err)
			}
		}
		w.inFlight = stepInFlight{}
	}
}

// publishLeaderChange publishes the (leader, term) pair and read readiness through the listener dispatcher; it is the only writer of ns.isLeader.
func (w *stepWorker) publishLeaderChange(shardID uint64, node *Node) {
	if w.callback == nil || node.peer == nil {
		return
	}
	// One consistent triple: the readiness published below belongs to exactly
	// the (leader, term) pair published first.
	currentLeader, term, readReady := node.peer.LeaderTermReady()
	if !w.publishLeaderUpdate(shardID, currentLeader, term) {
		return
	}
	if readReady {
		w.publishLeaderReady(shardID, node, currentLeader, term)
	}
}

// publishLeaderUpdate publishes OnLeaderUpdated for a changed (leader, term) pair and reports whether the published pair is now current.
func (w *stepWorker) publishLeaderUpdate(shardID, currentLeader, term uint64) bool {
	// Compare the pair, so a re-election at a higher term is published.
	if currentLeader == w.prevLeaders[shardID] && term == w.prevLeaderTerms[shardID] {
		return true
	}
	// A term change with no leader before or after is campaign churn: record it without publishing.
	if currentLeader == 0 && w.prevLeaders[shardID] == 0 {
		w.prevLeaderTerms[shardID] = term
		return false
	}
	// Record the pair only when the dispatcher accepts it, so a full queue retries next tick.
	cb := w.callback
	if !w.listeners.dispatch(func() {
		cb.OnLeaderUpdated(shardID, currentLeader, term)
	}) {
		slog.Warn("leader update notification deferred: listener queue full",
			"shard_id", shardID,
			"leader_id", currentLeader,
			"term", term,
		)
		return false
	}
	w.prevLeaders[shardID] = currentLeader
	w.prevLeaderTerms[shardID] = term
	// Every published leader change ends the readiness announced for the
	// previous pair; the new pair's readiness is announced afresh.
	delete(w.prevReadyTerms, shardID)
	return true
}

// publishLeaderReady publishes OnLeaderReady once for the published pair, retried like a leader change.
func (w *stepWorker) publishLeaderReady(shardID uint64, node *Node, leaderID, term uint64) {
	if w.prevReadyTerms[shardID] == term {
		return
	}
	cb := w.callback
	replicaID := node.replicaID
	if !w.listeners.dispatch(func() {
		cb.OnLeaderReady(shardID, replicaID, leaderID, term)
	}) {
		slog.Warn("leader ready notification deferred: listener queue full",
			"shard_id", shardID,
			"leader_id", leaderID,
			"term", term,
		)
		return
	}
	w.prevReadyTerms[shardID] = term
}

// processCallbacks publishes leader changes and campaigns through the dispatcher and delivers OnReadyToRead inline; in-flight read states are tracked.
func (w *stepWorker) processCallbacks(shardID uint64, node *Node, update proto.Update) {
	// Detect leader change and notify callback.
	if w.callback != nil {
		w.publishLeaderChange(shardID, node)
		// Notify confirmed read index requests.
		if len(update.ReadyToRead) > 0 {
			w.callback.OnReadyToRead(shardID, update.ReadyToRead)
		}
	}
	// Delivered (or no callback to deliver to): nothing is in flight.
	w.inFlight = stepInFlight{}
	if w.callback != nil {
		// An outbound RequestVote or RequestPreVote marks a campaign; take the term from the message.
		for i := range update.Messages {
			if update.Messages[i].Type == proto.RequestVote ||
				update.Messages[i].Type == proto.RequestPreVote {
				w.fireCampaign(shardID, node, update.Messages[i].Term)
				break
			}
		}
	}
}

// scanPreVoteCampaign returns the term of the first RequestPreVote in msgs and
// whether one was found. RequestVote is ordered and never travels as a
// free-order message, so only the pre-vote needs detecting here.
func scanPreVoteCampaign(msgs []proto.Message) (term uint64, launched bool) {
	for i := range msgs {
		if msgs[i].Type == proto.RequestPreVote {
			return msgs[i].Term, true
		}
	}
	return 0, false
}

// fireCampaign dispatches OnCampaignLaunched (dropped when the queue is full) and increments the election metric inline.
func (w *stepWorker) fireCampaign(shardID uint64, node *Node, term uint64) {
	if w.callback == nil {
		return
	}
	cb := w.callback
	replicaID := node.ReplicaID()
	if !w.listeners.dispatch(func() {
		cb.OnCampaignLaunched(shardID, replicaID, term)
	}) {
		slog.Error("campaign notification dropped: listener queue full",
			"shard_id", shardID,
			"replica_id", replicaID,
			"term", term,
			"dropped_total", w.listeners.droppedCount(),
		)
	}
	if w.metrics != nil {
		w.metrics.IncElection(shardID)
	}
}

// reportFreeOrderCampaign reports a RequestPreVote drained outside GetUpdate; call outside raftMu.
func (w *stepWorker) reportFreeOrderCampaign(shardID uint64, node *Node, msgs []proto.Message) {
	if w.callback == nil {
		return
	}
	if term, launched := scanPreVoteCampaign(msgs); launched {
		w.fireCampaign(shardID, node, term)
	}
}

// commitMemoryMode sends free-order messages, commits inline, sends ordered messages and forwards to applyC; true means stop.
func (w *stepWorker) commitMemoryMode(node *Node, update proto.Update) bool {
	// Split messages: send free-order before Commit, ordered after.
	// Reuse pre-allocated buffers to avoid allocation on every update.
	w.freeOrderBuf = w.freeOrderBuf[:0]
	w.orderedMsgBuf = w.orderedMsgBuf[:0]
	if len(update.Messages) > 0 {
		for i := range update.Messages {
			if proto.IsFreeOrder(&update.Messages[i]) {
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
		// Terminal: Peer.Commit already advanced the processed marker, so
		// these committed entries can never be re-offered. See failCommit.
		failCommit(node, w.callback, update, err)
		return false
	}
	// From here until applyC has the entries they exist nowhere else: an
	// abnormal exit fails the shard (settleMemCommitted).
	w.memCommitted, w.memCommittedUpdate = node, update
	// Send ordered messages after successful Commit.
	if len(w.orderedMsgBuf) > 0 {
		w.sendMessages(w.orderedMsgBuf)
	}
	// Record the hand-off before raising commitPending.
	w.enqueuingNode = node
	node.commitPending.Store(true)
	ai := w.applyPool.Get().(*applyItem)
	ai.update = update
	ai.node = node
	// Try the hand-off first so a stop never drops a committed update that had a free slot.
	select {
	case w.applyC <- ai:
		w.enqueuingNode = nil
		w.memCommitted, w.memCommittedUpdate = nil, proto.Update{}
		return false
	default:
	}
	select {
	case w.applyC <- ai:
		// Successfully queued for apply.
		w.enqueuingNode = nil
		w.memCommitted, w.memCommittedUpdate = nil, proto.Update{}
		return false
	case <-w.stopC:
		// Shutdown drop: the entries are released here, not by a recovery.
		w.memCommitted, w.memCommittedUpdate = nil, proto.Update{}
		node.commitPending.Store(false)
		// Release the pooled committed-entries slice before the item.
		ai.update.ReleaseCommittedEntries()
		releaseApplyItem(w.applyPool, ai)
		w.enqueuingNode = nil
		return true
	}
}

// processCommitPendingInbox steps inbox messages, ReadIndex first, then proposals, under raftMu while commitPending, deferring InstallSnapshot.
func (w *stepWorker) processCommitPendingInbox(shardID uint64, node *Node) {
	// Drain queued work; client work stays queued during recovery while Raft traffic flows.
	if !node.IsRecovering() {
		w.commitProposeBuf, w.commitProposeBatch = node.DrainProposalBatch(w.commitProposeBuf)
		w.readKeyBuf = node.DrainReadIndex(w.readKeyBuf)
	} else {
		w.commitProposeBuf = w.commitProposeBuf[:0]
		w.readKeyBuf = w.readKeyBuf[:0]
	}
	// Track drained proposals and keys until Peer.Handle has them (settleInFlight).
	w.inFlight = stepInFlight{node: node, proposals: w.commitProposeBuf, readKeys: w.readKeyBuf}
	w.inboxBuf = node.DrainInbox(w.inboxBuf)
	if len(w.inboxBuf) == 0 && len(w.readKeyBuf) == 0 && len(w.commitProposeBuf) == 0 {
		return
	}

	// Defer InstallSnapshot to the normal path: a restore during commitPending would race the apply pipeline and regress lastApplied.
	w.inboxBuf = w.filterSnapshots(shardID, node, w.inboxBuf)
	if len(w.inboxBuf) == 0 && len(w.readKeyBuf) == 0 && len(w.commitProposeBuf) == 0 {
		return
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

	// Register aliases before submitting the ReadIndex and outside raftMu, since OnReadIndexBatched is a host callback.
	var batchKey uint64
	haveReadKeys := len(w.readKeyBuf) > 0
	if haveReadKeys {
		batchKey = w.readKeyBuf[0]
		if len(w.readKeyBuf) > 1 && w.callback != nil {
			w.callback.OnReadIndexBatched(shardID, batchKey, w.readKeyBuf[1:])
		}
	}

	// Phase 1: Process ReadIndex requests and HeartbeatResp under lock,
	// then immediately extract and deliver ReadIndex completions.
	// This minimizes read latency by not waiting for proposal processing.
	phase1FreeOrder, readStates, readIndexErr := w.commitPendingPhase1Locked(
		shardID, node, batchKey, haveReadKeys, heartbeatResps)

	// Fail every pending read with the submit error, outside raftMu.
	if readIndexErr != nil && w.callback != nil {
		w.callback.OnReadIndexFailed(shardID, w.readKeyBuf, readIndexErr)
	}
	// Raft has the keys or they failed; the read states taken in phase 1 stay in flight until delivered.
	w.inFlight.readKeys = nil
	w.inFlight.readStates = readStates

	// Publish outside raftMu, before the early return, so both exits publish.
	w.publishLeaderChange(shardID, node)

	// Send Phase 1 free-order messages outside the lock. A pre-vote campaign
	// among them is reported so a campaign launched under commitPending stays
	// visible to OnCampaignLaunched/IncElection.
	if len(phase1FreeOrder) > 0 {
		w.sendMessages(phase1FreeOrder)
		w.reportFreeOrderCampaign(shardID, node, phase1FreeOrder)
	}

	// Deliver ReadIndex completion callbacks immediately, before
	// processing proposals. This prioritizes read latency.
	if w.callback != nil && len(readStates) > 0 {
		w.callback.OnReadyToRead(shardID, readStates)
	}
	w.inFlight.readStates = nil

	// Phase 2: Process proposals and remaining inbox messages.
	// These can proceed at lower priority since reads are done.
	if len(w.commitProposeBuf) == 0 && len(otherMsgs) == 0 {
		w.inFlight = stepInFlight{}
		return
	}

	w.runCommitPendingPhase2(shardID, node, otherMsgs)
}

// runCommitPendingPhase2 hands proposals and the remaining inbox to raft under raftMu, then settles, sends and publishes outside the lock.
func (w *stepWorker) runCommitPendingPhase2(shardID uint64, node *Node, otherMsgs []proto.Message) {
	freeOrderMsgs, additionalReadStates, rejectedReads, proposeErr := w.commitPendingPhase2Locked(shardID, node, otherMsgs)

	// Raft or the closed queue refused the batch definitely; notify the host first.
	if proposeErr != nil && w.callback != nil {
		w.callback.OnProposalRejected(shardID, w.commitProposeBuf, proposeErr)
	}
	// The proposals are raft's, or refused; the further read states and the
	// refused reads are in flight until delivered below.
	w.inFlight = stepInFlight{node: node, readStates: additionalReadStates, readKeys: rejectedReads}

	// Send free-order messages outside the lock. A pre-vote campaign among them
	// is reported so it stays visible to OnCampaignLaunched/IncElection.
	if len(freeOrderMsgs) > 0 {
		w.sendMessages(freeOrderMsgs)
		w.reportFreeOrderCampaign(shardID, node, freeOrderMsgs)
	}

	// Deliver any additional ReadIndex completions from phase 2.
	if w.callback != nil && len(additionalReadStates) > 0 {
		w.callback.OnReadyToRead(shardID, additionalReadStates)
	}
	if len(rejectedReads) > 0 {
		w.failRejectedReads(shardID, rejectedReads)
	}
	w.inFlight = stepInFlight{}

	// Publish again: phase 2 steps RequestVoteResp and can make this node leader.
	w.publishLeaderChange(shardID, node)

	// Observability last, outside every window above.
	if w.metrics != nil && len(w.commitProposeBuf) > 0 {
		w.metrics.ObserveProposalBatchSize(shardID, len(w.commitProposeBuf))
	}
}

// commitPendingPhase1Locked steps the ReadIndex batch and HeartbeatResps under raftMu, returning messages, completions and the submit error for the caller to handle outside the lock.
func (w *stepWorker) commitPendingPhase1Locked(
	shardID uint64,
	node *Node,
	batchKey uint64,
	haveReadKeys bool,
	heartbeatResps []proto.Message,
) (freeOrder []proto.Message, readStates []proto.ReadyToRead, readIndexErr error) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()

	// Process batched ReadIndex keys first. Same coalescing logic as
	// the normal processReady path. readIndexErr is captured here and the
	// completion callback fires after the return below.
	if haveReadKeys {
		if err := node.peer.Handle(proto.Message{
			Type:   proto.ReadIndex,
			From:   node.ReplicaID(),
			Hint:   batchKey,
			Origin: proto.OriginLocal,
		}); err != nil {
			slog.Warn("batched ReadIndex failed (commitPending)",
				"shard_id", shardID,
				"replica_id", node.ReplicaID(),
				"num_requests", len(w.readKeyBuf),
				"error", err,
			)
			readIndexErr = err
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

	// Drain free-order messages now, so ReadIndex heartbeats are not held until the next commit cycle.
	freeOrder = node.peer.DrainFreeOrderMessages()

	// Extract ReadIndex completions immediately after processing
	// HeartbeatResp. This allows us to deliver read results before
	// processing proposals, reducing read latency.
	readStates = node.peer.DrainReadStatesOnly()

	return freeOrder, readStates, readIndexErr
}

// commitPendingPhase2Locked appends proposals and steps the remaining inbox under raftMu, returning messages, completions and the propose error for the caller.
func (w *stepWorker) commitPendingPhase2Locked(
	shardID uint64,
	node *Node,
	otherMsgs []proto.Message,
) (freeOrder []proto.Message, readStates []proto.ReadyToRead, rejectedReads []uint64, proposeErr error) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()

	// Append proposals during commitPending so the next update carries them.
	if len(w.commitProposeBuf) > 0 {
		// Declared handed to raft only as Peer.Handle is entered; a closed
		// queue (the node failed or stopped) refuses, and the batch is
		// refused definitely by the caller.
		if !node.HandOffProposals(w.commitProposeBatch) {
			proposeErr = handOffRefusal(node)
		} else {
			w.inFlight.handed = true
			if err := node.peer.Handle(proto.Message{
				Type:    proto.Propose,
				Entries: w.commitProposeBuf,
				Origin:  proto.OriginLocal,
			}); err != nil {
				// Raft validates the whole batch before appending any of
				// it, so the refusal is definite.
				w.inFlight.handed = false
				slog.Warn("batched proposal failed (commitPending)",
					"shard_id", shardID,
					"replica_id", node.ReplicaID(),
					"num_entries", len(w.commitProposeBuf),
					"error", err,
				)
				proposeErr = err
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

	// Flush deferred ReadIndex heartbeat broadcasts, as phase 1 does.
	node.peer.FlushReadIndex()

	// Send free-order messages before the WAL write (Raft thesis 10.2.1); ordered ones wait in the peer.
	freeOrder = node.peer.DrainFreeOrderMessages()

	// Check for any additional ReadIndex completions that may have been
	// generated by processing other inbox messages (e.g., ReadIndexResp
	// from forwarded ReadIndex requests).
	readStates = node.peer.DrainReadStatesOnly()
	// Forwarded reads the leader refused (a ReadIndexResp with Reject set);
	// the caller fails them outside the lock.
	rejectedReads = node.peer.DrainReadRejects()

	return freeOrder, readStates, rejectedReads, proposeErr
}

// sendMessages hands messages to the non-blocking transport, InstallSnapshot through SendSnapshot.
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

// stepInFlight is what a step worker holds between taking it out of a node and handing it on; see stepWorker.inFlight.
type stepInFlight struct {
	node *Node
	// proposals is the batch taken from the proposal queue; handed reports
	// whether Peer.Handle was entered with it (HandOffProposals succeeded).
	proposals []proto.Entry
	handed    bool
	// readKeys are ReadIndex keys drained and not yet given to Peer.Handle.
	readKeys []uint64
	// readStates were taken from the Peer and not yet delivered.
	readStates []proto.ReadyToRead
}

// handOffRefusal is the definite refusal for a batch whose hand-off a closed queue refused: ErrShardFailed if the shard halted, else ErrStopped.
func handOffRefusal(node *Node) error {
	if node.IsFailed() {
		return ErrShardFailed
	}
	return ErrStopped
}

// settleInFlight resolves what an abnormal exit left in w.inFlight: an unhandled proposal batch is refused, a handled one fails maybe-applied, and reads fail.
func (w *stepWorker) settleInFlight(cause error) {
	f := w.inFlight
	w.inFlight = stepInFlight{}
	if f.node == nil || w.callback == nil {
		return
	}
	shardID := f.node.shardID
	if len(f.proposals) > 0 {
		if f.handed {
			w.callback.OnProposalFailed(shardID, f.proposals, cause)
		} else {
			w.callback.OnProposalRejected(shardID, f.proposals, ErrDropped)
		}
	}
	keys := make([]uint64, 0, len(f.readKeys)+len(f.readStates))
	keys = append(keys, f.readKeys...)
	for i := range f.readStates {
		keys = append(keys, f.readStates[i].Key)
	}
	if len(keys) > 0 {
		w.callback.OnReadIndexFailed(shardID, keys, ErrDropped)
	}
}

// settleMemCommitted fails the shard whose memory-mode commit never reached applyC; its proposals fail maybe-applied.
func (w *stepWorker) settleMemCommitted(cause error) {
	node, u := w.memCommitted, w.memCommittedUpdate
	w.memCommitted, w.memCommittedUpdate = nil, proto.Update{}
	if node == nil {
		return
	}
	err := &CommitError{ShardID: u.ShardID, ReplicaID: u.ReplicaID, Err: cause}
	failShard(node, w.callback, err, func() {
		if w.callback != nil && len(u.CommittedEntries) > 0 {
			w.callback.OnProposalFailed(u.ShardID, u.CommittedEntries, err)
		}
	})
	u.ReleaseCommittedEntries()
}

func (w *stepWorker) recoverAbnormalExit(cause error) {
	// Whatever was taken out of a node and not yet handed on is resolved
	// first, so nothing the exit interrupted is lost silently.
	w.settleMemCommitted(cause)
	w.settleInFlight(cause)
	// The ONE node that was mid hand-off (enqueueForCommit or
	// commitMemoryMode): its item never reached commitC/applyC.
	w.clearStepCommitPending()
	// Withdraw the durability claim of an extracted update the exit destroyed.
	w.discardExtractedUpdate()
	// A receive waiting on a local install the exit dropped from the inbox is
	// answered rather than left waiting for the shard to stop.
	w.answerDroppedInstalls(cause)
}

func (w *stepWorker) clearStepCommitPending() {
	node := w.enqueuingNode
	if node == nil {
		return
	}
	w.enqueuingNode = nil
	node.commitPending.Store(false)
	if w.workSignal != nil {
		w.workSignal.Notify(node.shardID)
	}
}

// discardExtractedUpdate withdraws GetUpdate's durability claim for an update a panic destroyed, so the hard state is still written.
func (w *stepWorker) discardExtractedUpdate() {
	node := w.extractedNode
	if node == nil {
		return
	}
	w.extractedNode = nil
	node.peer.InvalidatePersistedState()
	if w.workSignal != nil {
		w.workSignal.Notify(node.shardID)
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

// loadNode adds a node to this worker's partition; safe for concurrent use.
func (w *stepWorker) loadNode(node *Node) {
	w.nodesMu.Lock()
	w.nodes[node.shardID] = node
	w.nodesMu.Unlock()
}

// unloadNode removes a node and its per-shard state; nodesMu's write lock excludes tick and processReady.
func (w *stepWorker) unloadNode(shardID uint64) {
	w.nodesMu.Lock()
	delete(w.nodes, shardID)
	delete(w.commitSlots, shardID)
	delete(w.prevLeaders, shardID)
	delete(w.prevLeaderTerms, shardID)
	delete(w.prevReadyTerms, shardID)
	delete(w.deferredSnapshots, shardID)
	w.nodesMu.Unlock()
}
