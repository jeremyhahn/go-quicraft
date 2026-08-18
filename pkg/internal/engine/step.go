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
	// prevLeaderTerms mirrors prevLeaders with the term published alongside each
	// leader. publishLeaderChange compares the (leader, term) PAIR so a
	// same-leader re-election at a higher term still fires OnLeaderUpdated;
	// keying on the leader ID alone would silently swallow it. Kept as a
	// separate map (not a struct value on prevLeaders) so the two stay
	// individually addressable.
	prevLeaderTerms map[uint64]uint64
	stopC           chan struct{}

	// listeners carries user-visible notifications off this goroutine. The step
	// worker fires them while holding nodesMu.RLock, and a listener that calls
	// back into Host.StopShard re-enters nodesMu.Lock() on this same goroutine —
	// a permanent deadlock, because sync.RWMutex is not reentrant. Handing the
	// notification to the dispatcher instead means the user's code runs on a
	// goroutine that holds no engine lock. See listenerDispatcher.
	listeners *listenerDispatcher

	// enqueuingNode records the node currently being handed off to the commit
	// pipeline (mid enqueueForCommit or commitMemoryMode): its commitPending is
	// set but its item has not yet reached commitC/applyC, so nothing else owns
	// it. Set before commitPending.Store(true) and cleared after the channel
	// send. On a step-worker panic this is the ONE node the recovery must
	// unwedge — an item already IN commitC is owned by the commit worker, which
	// consumes and commits it normally after the restart, and an item already
	// handed off is owned by the commit/apply worker. Written and read only on
	// the step-worker goroutine (recovery runs on it during the panic unwind),
	// so no synchronization is required.
	enqueuingNode *Node

	// extractedNode records the node whose update has been pulled out of the
	// Peer by GetUpdate but has not yet reached a path that persists it. It is
	// a strictly wider window than enqueuingNode, which only opens once the
	// hand-off itself begins: a panic in processCallbacks — the inline
	// OnReadyToRead in particular — unwinds before enqueuingNode is ever set,
	// and that is precisely the window in which GetUpdate has already advanced
	// prevState. Same goroutine discipline as enqueuingNode.
	extractedNode *Node
	inboxBuf      []proto.Message // reusable buffer for draining node inboxes
	proposeBuf    []proto.Entry   // reusable buffer for batching inbox proposals
	// commitProposeBuf is a SEPARATE reusable buffer for the commit-pending
	// drain. It must not share proposeBuf: within one iteration the step worker
	// drains proposals into proposeBuf and steps a Propose message that ALIASES
	// that buffer (on a follower the message is forwarded, so the outbound copy
	// happens later, in QUICTransport.Send). Handing the SAME buffer back to
	// EntryQueue.Swap in the commit-pending drain makes it the queue's live write
	// buffer again, so client Add calls overwrite entry structs the transport is
	// still reading — a data race that corrupts forwarded proposals. Using a
	// distinct buffer defers recycling to the NEXT iteration's first drain, by
	// which point sendMessages has already copied.
	commitProposeBuf  []proto.Entry
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
		prevLeaderTerms:   make(map[uint64]uint64),
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
	for shardID, node := range w.nodes {
		if node.IsFailed() || node.peer == nil {
			continue
		}
		campaignTerm, campaignLaunched := w.tickNodeLocked(node)
		// A CheckQuorum step-down happens entirely inside Tick(): tickLeader
		// calls becomeFollower(r.term, noLeader), which changes neither term,
		// vote nor commit index and emits no message. hasStateChanged() is
		// therefore false and HasUpdate() false, so processReady skips the
		// shard and NEITHER of the two other publish sites can ever run. The
		// node kept reporting itself leader through Host.GetShardInfo() —
		// the flag clients route writes by — until it campaigned roughly an
		// election timeout later. Publishing here is the only way this
		// transition is ever observed. Runs outside raftMu (LeaderID/Term are
		// atomics) so the callback never executes under the node lock.
		if campaignLaunched {
			w.fireCampaign(shardID, node, campaignTerm)
		}
		w.publishLeaderChange(shardID, node)
	}
}

// tickNodeLocked advances one node's Raft clock and publishes its markers with
// node.raftMu held for the whole call, returning whether Tick launched a
// pre-vote campaign (and at which term) so the caller can report it.
//
// The lock covers exactly Tick, the commitPending free-order drain/send, and
// the two publications — nothing else. It is a separate method so the unlock
// can be deferred: a panic anywhere in this region (Peer.Tick stepping a
// corrupt log, a metrics collector called from sendMessages) would otherwise
// leak node.raftMu permanently, and the step worker's panic handler restarts
// the worker straight back into a Lock() that can never be taken — wedging
// every shard this worker owns plus Host.Close, which joins the same worker.
//
// The campaign report and the leadership publish deliberately stay in the
// caller, OUTSIDE this method: both run arbitrary user listener code, and the
// commit worker's Lock() for every shard this worker owns would queue behind
// it for however long that code takes.
func (w *stepWorker) tickNodeLocked(node *Node) (campaignTerm uint64, campaignLaunched bool) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()
	node.peer.Tick()
	// When commitPending is true, processReady skips this node.
	// But Tick may have produced heartbeat messages that must be
	// sent immediately to maintain leader liveness. Drain and
	// send only free-order messages inline. Tick during
	// commitPending produces Heartbeat (leader liveness) and, on an
	// election timeout, RequestPreVote — both free-order. Filter
	// defensively to ensure ordered messages (e.g., VoteResp) are never
	// sent before WAL persist completes.
	if node.commitPending.Load() {
		if msgs := node.peer.DrainFreeOrderMessages(); len(msgs) > 0 {
			// Detect a pre-vote campaign under the lock (pure reads); the
			// observability callback fires after the return below. Without
			// this, a campaign launched from tick() while commitPending is
			// set never reaches OnCampaignLaunched/IncElection because it
			// bypasses processCallbacks entirely.
			campaignTerm, campaignLaunched = scanPreVoteCampaign(msgs)
			w.sendMessages(msgs)
		}
	}
	// Publish the log markers and the diagnostic snapshot under
	// raftMu — the only race-free point to read Peer.r from
	// (excludes the commit worker; sequential with the step worker's
	// own Handle). The markers are published on EVERY tick, not just
	// when an update is produced, so a snapshot pool waiting for the
	// raft log to install a snapshot keeps seeing fresh values even
	// on a node whose processReady work is otherwise idle. Two atomic
	// stores; the diagnostic snapshot below stays a no-op unless a
	// diagnostic accessor has been called.
	node.publishLogMarkers()
	node.publishDiagLocked()
	return campaignTerm, campaignLaunched
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
		// When recovering is true, a snapshot recovery is in progress on
		// the snapshot pool goroutine and the state machine is being
		// replaced by RecoverFromSnapshot — arbitrary user code with no
		// time bound. The node nevertheless STAYS IN THE PROTOCOL: nothing
		// in Handle(RequestVote)/Handle(Heartbeat), nor in the WAL persist
		// that lets their (ordered) responses be sent, touches the state
		// machine. Only the extraction of committed entries feeds the apply
		// worker, so that is the single thing suppressed here — GetUpdate
		// is called with moreEntriesToApply=false — plus InstallSnapshot,
		// which would move the raft log's snapshot floor out from under the
		// recovery in progress (see drainAndHandleInbox).
		//
		// Skipping the node outright (the previous behavior) made it
		// invisible to consensus for the whole recovery: it stepped no
		// RequestVote, emitted no RequestVoteResp, and answered no
		// Heartbeat, while Node.Deliver DISCARDED inbound messages once the
		// inbox filled. Two replicas recovering at once therefore dissolved
		// a five-replica shard's quorum with every node still up.
		// Read at each use below rather than once here. The snapshot pool
		// goroutine can RAISE this flag at any point during this iteration, and
		// a value captured before that is stale exactly when it matters: the
		// InstallSnapshot suppression in drainAndHandleInbox would then move the
		// raft log's snapshot floor out from under a recovery that has already
		// begun. The three decisions below guard independent actions and carry
		// no invariant that they agree with each other, so each takes the
		// freshest value. (The apply worker re-checks the gate itself, which is
		// what keeps state-machine access safe regardless.)
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
			// Client work — proposals and ReadIndex requests — stays queued
			// while a recovery owns the state machine. Neither can produce a
			// result before the gate clears (the apply pipeline is stopped and
			// lastApplied does not move), so accepting them buys nothing;
			// leaving them queued lets the queues apply their own backpressure
			// (ErrSystemBusy), which is the honest answer from a replica that
			// cannot serve them yet. Raft protocol traffic below is a different
			// matter and MUST keep flowing.
			if !node.IsRecovering() {
				w.drainAndHandleProposals(shardID, node)
				// Update the per-shard rate limiter cache immediately after
				// proposals are appended to the in-memory log. On single-node
				// clusters, entries are committed and compacted in the same
				// processReady iteration, so the tick-based evaluation in
				// tickLeader() would always see an empty log. Evaluating here
				// captures the peak in-memory size before commit/apply drains it.
				node.peer.RateLimited()
				w.drainAndHandleReadIndex(shardID, node)
				// Deferred snapshots are re-injected only once the pipeline can
				// accept them: not while a recovery is installing one.
				w.redeliverDeferredSnapshots(shardID, node)
			}
			w.drainAndHandleInbox(shardID, node, node.IsRecovering())
			node.peer.FlushReadIndex()
		}
		// Output-channel backpressure guard. This worker is the ONLY producer
		// for its output channel — commitC in WAL mode, applyC in memory mode —
		// so a free slot observed here cannot be taken by anyone else and the
		// enqueue below is guaranteed not to block. When the channel is full,
		// skip the shard WITHOUT calling GetUpdate: nothing has been extracted
		// from the peer, so nothing is lost and the shard is simply retried.
		// Blocking on the send instead would park this goroutine while holding
		// nodesMu.RLock, stopping tick() for EVERY shard this worker owns
		// (missed heartbeats, spurious elections on unrelated shards) and
		// stalling UnloadNode. Memory mode has the same hazard: commitMemoryMode
		// blocks on the applyC send under the same lock. Liveness: the pipeline
		// notifies the work signal as it clears commitPending, and processReady
		// also re-runs on every tick. An unbuffered output channel is a test-only
		// rendezvous configuration (NewEngine always allocates a buffer), so the
		// guard does not apply.
		outLen, outCap := len(w.commitC), cap(w.commitC)
		if w.memoryMode {
			outLen, outCap = len(w.applyC), cap(w.applyC)
		}
		if outCap > 0 && outLen == outCap {
			// Flush free-order messages before skipping. GetUpdate is the
			// only other drain of p.r.msgs on this path, and commitPending
			// is false here so tick()'s drain does not run either — so
			// heartbeats, Replicate, and the Ping/Quiesce control traffic
			// produced by the inbox processing above would sit unsent for as
			// long as the downstream worker stays behind. A SaveState retry
			// backoff would therefore silence every shard this worker owns and
			// trigger elections against a live leader. Only free-order messages
			// may go: everything else (VoteResp, HeartbeatResp) still has to
			// wait for its state to be durable (Raft PhD 10.2.1). A pre-vote
			// campaign among them is reported so it stays visible to
			// OnCampaignLaunched/IncElection.
			if node.peer != nil {
				if msgs := node.peer.DrainFreeOrderMessages(); len(msgs) > 0 {
					w.reportFreeOrderCampaign(shardID, node, msgs)
					w.sendMessages(msgs)
				}
			}
			continue
		}
		lastApplied := node.lastApplied.Load()
		hasUpdate := node.peer != nil && node.peer.HasUpdate(lastApplied)
		if !hasUpdate {
			continue
		}
		// moreEntriesToApply is false while a snapshot recovery owns the
		// state machine: committed entries stay in the raft log (the
		// processed marker does not advance, so they are re-offered once
		// the gate clears) instead of entering the commit/apply pipeline.
		// Everything else in the update — hard state, entries to save,
		// messages — is protocol-level and must keep flowing.
		//
		// snapshotAdoptionPending covers the window BEFORE that gate goes up:
		// the inbox drain above may have just installed a received snapshot,
		// which moves the log's processed marker to the snapshot index while
		// the state machine is still at its old lastApplied. Extracting there
		// hands the apply worker a batch starting above lastApplied+1 —
		// ErrApplyGap, and a replica that never applies again the moment it
		// finishes joining. The snapshot pool cannot raise the recovering gate
		// until it observes this install, so only a check on the owner's own
		// goroutine closes it.
		holdEntries := node.IsRecovering() || node.snapshotAdoptionPending()
		update, err := node.peer.GetUpdate(!holdEntries, lastApplied)
		if err != nil {
			node.handleError(err)
			continue
		}
		// Propagate the log markers so FollowerRead can wait for
		// lastApplied to catch up without a ReadIndex round-trip, and so
		// the snapshot pool observes an InstallSnapshot the moment this
		// cycle installs it rather than a tick later. The step worker
		// holds exclusive access to the Peer here, so reading them is
		// safe without additional locking.
		node.publishLogMarkers()
		// From here until the update reaches its persistence path, this node
		// is owed a hard-state write that GetUpdate has already optimistically
		// marked as done (it advanced prevState at extraction). If the
		// goroutine unwinds in between, discardExtractedUpdate takes that
		// claim back. See Peer.InvalidatePersistedState.
		w.extractedNode = node
		// Detect leader change, ReadyToRead, and campaign launch.
		w.processCallbacks(shardID, node, update)
		// Read-only updates (e.g., ReadIndex confirmations) carry no
		// entries, state changes, or snapshots. The ReadyToRead callback
		// above already delivered the results. Skip the commit/apply
		// pipeline to avoid unnecessary LogDB writes, goroutine hops,
		// and commitPending blocking. This reduces ReadIndex latency
		// from ~300us (3 goroutine hops) to ~5us (inline callback).
		if update.ReadOnly {
			// A ReadOnly update carries no hard state to persist, so there is
			// nothing left owed once its messages are away.
			w.extractedNode = nil
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

// redeliverDeferredSnapshots re-injects deferred InstallSnapshot messages
// from a previous commitPending cycle. These were buffered in
// deferredSnapshots instead of re-delivered to the inbox to prevent
// CPU-burning drain-redeliver loops during commitPending.
func (w *stepWorker) redeliverDeferredSnapshots(shardID uint64, node *Node) {
	deferred, ok := w.deferredSnapshots[shardID]
	if !ok || len(deferred) == 0 {
		return
	}
	// Only drop messages that were successfully delivered. Deliver returns
	// false when the inbox queue is full; those messages must be retained
	// for a later retry rather than discarded, otherwise a transient full
	// inbox would permanently drop a deferred InstallSnapshot. Filter in
	// place by compacting the undelivered messages to the front.
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

// deferSnapshot buffers an InstallSnapshot message for redelivery once
// commitPending clears, keeping only the HIGHEST-index snapshot per shard.
// A leader retransmits InstallSnapshot every retry interval while it waits
// for an ack, and each message carries its own membership maps; appending
// them all would grow the buffer without bound for as long as the shard stays
// in commitPending. An older snapshot is strictly superseded by a newer one,
// so keeping the latest loses nothing.
//
// It only ever compares LOCAL-origin installs: filterSnapshots drops wire-origin
// InstallSnapshot before it reaches here, so a forged high index on the wire can
// never evict the legitimate local install from this highest-index-wins buffer.
func (w *stepWorker) deferSnapshot(shardID uint64, msg proto.Message) {
	buf := w.deferredSnapshots[shardID]
	if len(buf) == 0 {
		w.deferredSnapshots[shardID] = append(buf[:0], msg)
		return
	}
	if msg.Snapshot.Index >= buf[0].Snapshot.Index {
		buf[0] = msg
	}
	w.deferredSnapshots[shardID] = buf[:1]
}

// filterSnapshots moves every InstallSnapshot message in msgs into the
// shard's deferred-snapshot buffer and returns the remaining messages.
// The input slice is filtered in place into a fresh header, so the caller
// must use the returned slice. When msgs holds no snapshot it is returned
// unchanged and nothing is allocated.
func (w *stepWorker) filterSnapshots(shardID uint64, msgs []proto.Message) []proto.Message {
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
	// Buffer InstallSnapshot messages locally instead of re-delivering
	// them to the inbox. Re-delivery causes an infinite drain-redeliver
	// loop: each processReady cycle drains the same message and puts it
	// back, burning CPU until the gate clears. Buffering here ensures the
	// messages are processed exactly once when it does.
	filtered := msgs[:0:0]
	for i := range msgs {
		if msgs[i].Type == proto.InstallSnapshot {
			// Only a process-local InstallSnapshot is legitimate here: the
			// snapshot receive path builds it after verifying and persisting the
			// artifact and injects it via DeliverLocal. A wire-origin one is
			// refused by raft.step (ErrWireLocalMessage) regardless, so deferring
			// it buys nothing — and it is actively harmful, because deferSnapshot
			// keeps the HIGHEST index and a forged Snapshot.Index (e.g. 1<<60)
			// would evict the real local install, wedging the follower behind the
			// compaction floor. Drop wire-origin snapshots outright.
			if msgs[i].Origin != proto.OriginLocal {
				slog.Warn("dropping wire-origin InstallSnapshot",
					"shard_id", shardID,
					"snapshot_index", msgs[i].Snapshot.Index,
					"from", msgs[i].From,
				)
				continue
			}
			w.deferSnapshot(shardID, msgs[i])
		} else {
			filtered = append(filtered, msgs[i])
		}
	}
	return filtered
}

// drainAndHandleInbox drains transport messages (Replicate, Heartbeat,
// ReadIndex, etc.) from inboxQ and handles them individually through
// the Raft peer.
//
// While recovering is true, InstallSnapshot messages are deferred instead
// of handled: handleSnapshot restores the raft log onto a NEWER snapshot
// index while the pool is still installing an older one into the state
// machine, which would leave lastApplied permanently below the log's
// snapshot floor and fail the shard with an apply gap. Every other message
// type is pure Raft protocol and is safe to step during recovery.
func (w *stepWorker) drainAndHandleInbox(shardID uint64, node *Node, recovering bool) {
	w.inboxBuf = node.DrainInbox(w.inboxBuf)
	if recovering {
		w.inboxBuf = w.filterSnapshots(shardID, w.inboxBuf)
	}
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
	// Record the hand-off in progress BEFORE raising commitPending, so a panic
	// anywhere below leaves the recovery a way to unwedge exactly this node.
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
		w.enqueuingNode = nil
		return false
	case <-w.stopC:
		node.commitPending.Store(false)
		w.enqueuingNode = nil
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
			Origin:  proto.OriginLocal,
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
			Type:   proto.ReadIndex,
			From:   node.ReplicaID(),
			Hint:   batchKey,
			Origin: proto.OriginLocal,
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

// publishLeaderChange fires OnLeaderUpdated when this shard's leader differs
// from the last value published, and is the ONLY way ns.isLeader — and
// therefore Host.GetShardInfo().IsLeader — ever changes.
//
// It must be reachable from every path that can change leadership, not just
// the one that produces a proto.Update. A node can win an election entirely
// inside processCommitPendingInbox (a RequestVoteResp there drives
// handleVoteResp -> becomeLeader), and a CheckQuorum step-down changes no
// hard-state field at all, so neither produces an update and neither used to
// publish anything. The result was a real Raft leader that the public API
// reported as a follower, and a stepped-down node that kept reporting itself
// leader — clients route writes by exactly this flag.
// The leader and term are read as a PAIR (Peer.LeaderAndTerm). All three publish
// sites run outside raftMu, so reading the two mirrors separately could report a
// combination that never co-existed — telling a listener a replica led in a term
// it never led.
//
// The callback itself is DELIVERED BY THE LISTENER DISPATCHER, not on this
// goroutine: tick() and processReady() hold nodesMu.RLock across this call, and
// a listener that reacts to a leadership change by stopping the shard would
// re-enter nodesMu.Lock() on this very goroutine. The (leader, term) pair is
// captured here, so what the listener eventually sees is the state at detection
// time, not a later re-read.
func (w *stepWorker) publishLeaderChange(shardID uint64, node *Node) {
	if w.callback == nil || node.peer == nil {
		return
	}
	currentLeader, term := node.peer.LeaderAndTerm()
	// Compare the (leader, term) PAIR: a same-leader re-election at a higher
	// term (leader unchanged, term advanced) is a real leadership event that
	// keying on the leader ID alone would silently swallow.
	if currentLeader == w.prevLeaders[shardID] && term == w.prevLeaderTerms[shardID] {
		return
	}
	// A term that advances while NO leader is known in either the previous or
	// the current state is campaign churn, not a leadership event: a replica
	// that cannot win keeps incrementing its term with leaderID still 0. It
	// must not be published — OnLeaderUpdated overwrites the host's cached
	// leader (clobbering a leader learned out-of-band with 0) and completes
	// pending proposals with NotLeaderError on a leadership loss that did not
	// happen. Record the term so the next genuine change is still detected.
	if currentLeader == 0 && w.prevLeaders[shardID] == 0 {
		w.prevLeaderTerms[shardID] = term
		return
	}
	// Hand the notification to the listener dispatcher and commit the
	// bookkeeping ONLY if it was accepted. OnLeaderUpdated maintains the Host's
	// isLeader mirror, so a dropped one would leave GetShardInfo permanently
	// wrong; recording prevLeaders only on acceptance makes a refused dispatch
	// (queue full) re-publish on the next tick instead of losing the event.
	// This is why the queue-full policy for leadership is retry, not drop.
	cb := w.callback
	if !w.listeners.dispatch(func() {
		cb.OnLeaderUpdated(shardID, currentLeader, term)
	}) {
		slog.Warn("leader update notification deferred: listener queue full",
			"shard_id", shardID,
			"leader_id", currentLeader,
			"term", term,
		)
		return
	}
	w.prevLeaders[shardID] = currentLeader
	w.prevLeaderTerms[shardID] = term
}

// processCallbacks detects leader changes, delivers ReadyToRead notifications,
// and detects campaign launches. All notifications are delivered through the
// callback interface.
//
// Leader changes and campaign launches go through the listener dispatcher (they
// reach user code); OnReadyToRead is invoked INLINE because it is engine-to-Host
// plumbing that completes registered ReadIndex requests, and queueing it behind
// user code would delay every linearizable read by the depth of that queue.
//
// A panic in the inline OnReadyToRead still loses the Update that GetUpdate
// already extracted, with the node neither failed nor notified. That is largely
// self-healing — update.State carries the full current HardState rather than a
// diff, so the next cycle re-derives it — except for the readStates, which
// GetUpdate drained out of the peer and which are gone: those ReadIndex callers
// wait for the host's sweeper. It is left as-is deliberately: the remaining
// panic surface here is engine-owned plumbing rather than user code (the user
// callbacks that used to run here now run on the dispatcher, under its own
// recover), so a panic in it is an engine bug that the step worker's
// restart-on-panic must surface, not an expected condition to be absorbed.
func (w *stepWorker) processCallbacks(shardID uint64, node *Node, update proto.Update) {
	// Detect leader change and notify callback.
	if w.callback != nil {
		w.publishLeaderChange(shardID, node)
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

// fireCampaign delivers the campaign-launch observability callbacks. The
// listener notification goes through the dispatcher — OnCampaignLaunched runs
// arbitrary user listener code and this is called from tick(), which holds
// nodesMu.RLock — while the metric is incremented inline (IncElection is engine
// code with no reentrancy hazard, and keeping it here keeps the counter exact
// even when the notification queue is full).
//
// Queue-full policy for campaigns is DROP: dispatch counts and reports it, and
// the event is pure observability of a cold path, so re-publishing it later
// would report an election at the wrong time rather than at the right one.
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

// reportFreeOrderCampaign fires the campaign-launch callbacks when a
// RequestPreVote is present among free-order messages drained OUTSIDE the normal
// GetUpdate/processCallbacks path (the commitPending drains and the commitC-full
// skip). PreVote is free-order, so a campaign launched under commitPending
// reaches the wire through these drains; without this scan it would be invisible
// to OnCampaignLaunched/IncElection. Safe only when called outside raftMu.
func (w *stepWorker) reportFreeOrderCampaign(shardID uint64, node *Node, msgs []proto.Message) {
	if w.callback == nil {
		return
	}
	if term, launched := scanPreVoteCampaign(msgs); launched {
		w.fireCampaign(shardID, node, term)
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
		// Terminal: Peer.Commit already advanced the processed marker, so
		// these committed entries can never be re-offered. See failCommit.
		failCommit(node, w.callback, update, err)
		return false
	}
	// Send ordered messages after successful Commit.
	if len(w.orderedMsgBuf) > 0 {
		w.sendMessages(w.orderedMsgBuf)
	}
	// Record the hand-off in progress BEFORE raising commitPending so a panic
	// before the applyC send leaves the recovery a way to unwedge this node.
	w.enqueuingNode = node
	node.commitPending.Store(true)
	ai := w.applyPool.Get().(*applyItem)
	ai.update = update
	ai.node = node
	select {
	case w.applyC <- ai:
		// Successfully queued for apply.
		w.enqueuingNode = nil
		return false
	case <-w.stopC:
		node.commitPending.Store(false)
		// Return the pooled committed-entries slice BEFORE the item:
		// releaseApplyItem clears the update with a plain struct assignment, so
		// dropping the item without this call leaks the slice out of the pool.
		// Every other drop site pairs the two (commit.go, applyWorker).
		ai.update.ReleaseCommittedEntries()
		releaseApplyItem(w.applyPool, ai)
		w.enqueuingNode = nil
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
// The gocyclo suppression that sat here is gone: extracting the two locked
// phases into commitPendingPhase1Locked and commitPendingPhase2Locked took this
// function back under the limit on its own merits.
func (w *stepWorker) processCommitPendingInbox(shardID uint64, node *Node) {
	// Drain all queued operations from MPSC queues. The actual processing
	// happens under raftMu to serialize with the commit worker.
	//
	// Client work — proposals and ReadIndex — stays QUEUED while a recovery
	// owns the state machine, matching processReady's gate. This path is not a
	// corner case during recovery, it is the steady state: the apply worker
	// parks on the recovery gate WITHOUT clearing commitPending, so every cycle
	// comes through here. Draining anyway defeated the documented backpressure
	// entirely — neither a proposal nor a read can produce a result before the
	// gate clears (the apply pipeline is stopped and lastApplied does not move),
	// so accepting them buys nothing and denies the queues the chance to answer
	// ErrSystemBusy, which is the honest reply from a replica that cannot serve
	// them yet. Raft protocol traffic (the inbox) MUST keep flowing regardless.
	if !node.IsRecovering() {
		w.commitProposeBuf = node.DrainProposals(w.commitProposeBuf)
		w.readKeyBuf = node.DrainReadIndex(w.readKeyBuf)
	} else {
		w.commitProposeBuf = w.commitProposeBuf[:0]
		w.readKeyBuf = w.readKeyBuf[:0]
	}
	w.inboxBuf = node.DrainInbox(w.inboxBuf)
	if len(w.inboxBuf) == 0 && len(w.readKeyBuf) == 0 && len(w.commitProposeBuf) == 0 {
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
	// inbox. The buffer is re-injected into the inbox when commitPending
	// clears and the normal processReady path runs.
	w.inboxBuf = w.filterSnapshots(shardID, w.inboxBuf)
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

	// Register batch aliases BEFORE taking raftMu, and therefore before
	// submitting the ReadIndex to raft. The happens-before this ordering exists
	// for is unchanged (and strengthened): in single-node clusters
	// Handle(ReadIndex) resolves immediately and DrainReadStatesOnly below
	// returns the result, so aliases registered after Handle could be orphaned
	// by an OnReadyToRead that fired first. Running it OUTSIDE the lock is what
	// this hoist buys — OnReadIndexBatched is a host callback and the rule this
	// method already states for publishLeaderChange applies verbatim: a callback
	// under node.raftMu stalls the commit worker's Lock for every shard that
	// worker owns, for however long the callback takes.
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

	// Complete all pending reads with the submit error, OUTSIDE raftMu. Same
	// rationale as the normal path: aliases are already registered but no
	// ReadyToRead will fire, so without this the callers block to their context
	// deadline. Fired before the readStates delivery below to preserve the
	// order the locked version had.
	if readIndexErr != nil && w.callback != nil {
		w.callback.OnReadIndexFailed(shardID, w.readKeyBuf, readIndexErr)
	}

	// Publish OUTSIDE raftMu. OnLeaderUpdated runs arbitrary user listener code
	// (and the Host's failPendingRequests); running it under the lock stalls the
	// commit worker's node.raftMu.Lock() for every shard that worker owns, for
	// however long the user's listener takes. LeaderID/Term are atomics, so
	// nothing here needs the lock. Placed before the early return below so both
	// exits from this function publish.
	w.publishLeaderChange(shardID, node)

	// Send Phase 1 free-order messages outside the lock. A pre-vote campaign
	// among them is reported so a campaign launched under commitPending stays
	// visible to OnCampaignLaunched/IncElection.
	if len(phase1FreeOrder) > 0 {
		w.reportFreeOrderCampaign(shardID, node, phase1FreeOrder)
		w.sendMessages(phase1FreeOrder)
	}

	// Deliver ReadIndex completion callbacks immediately, before
	// processing proposals. This prioritizes read latency.
	if w.callback != nil && len(readStates) > 0 {
		w.callback.OnReadyToRead(shardID, readStates)
	}

	// Phase 2: Process proposals and remaining inbox messages.
	// These can proceed at lower priority since reads are done.
	if len(w.commitProposeBuf) == 0 && len(otherMsgs) == 0 {
		return
	}

	freeOrderMsgs, additionalReadStates, proposeErr := w.commitPendingPhase2Locked(shardID, node, otherMsgs)

	// Notify the host so it can complete pending proposals with the error
	// instead of leaving callers blocked. Fired first among the post-unlock
	// callbacks, preserving the order the locked version had.
	if proposeErr != nil && w.callback != nil {
		w.callback.OnProposalFailed(shardID, w.commitProposeBuf, proposeErr)
	}

	// Send free-order messages outside the lock. A pre-vote campaign among them
	// is reported so it stays visible to OnCampaignLaunched/IncElection.
	if len(freeOrderMsgs) > 0 {
		w.reportFreeOrderCampaign(shardID, node, freeOrderMsgs)
		w.sendMessages(freeOrderMsgs)
	}

	// Deliver any additional ReadIndex completions from phase 2.
	if w.callback != nil && len(additionalReadStates) > 0 {
		w.callback.OnReadyToRead(shardID, additionalReadStates)
	}

	// Publish again after Phase 2, still outside the lock. This is the phase
	// that can actually make this node a leader: RequestVoteResp is not a
	// HeartbeatResp, so it is sorted into otherMsgs and stepped HERE — after the
	// Phase 1 publish has already run. A node that won its election on a vote
	// response delivered while commitPending was set would otherwise be reported
	// as a follower by GetShardInfo until some later cycle happened to publish.
	w.publishLeaderChange(shardID, node)
}

// commitPendingPhase1Locked steps the batched ReadIndex and the HeartbeatResp
// messages into the peer with node.raftMu held, and returns the free-order
// messages and the ReadIndex completions the caller must deliver. A non-nil
// error is the ReadIndex submit failure; it is returned rather than reported
// here so OnReadIndexFailed fires outside the lock.
//
// Everything the caller does with these return values — the failure callback,
// the leadership publish, the message send, OnReadyToRead — MUST stay outside
// this method. Each of them enters host or user listener code of unbounded
// duration, and the commit worker's node.raftMu.Lock() for every shard this
// worker owns queues behind whatever runs under the lock.
//
// The method exists so the unlock can be deferred while still ending exactly
// where the explicit unlock did. A panic inside peer.Handle would otherwise
// leak node.raftMu: the step worker's recover restarts the worker, the restart
// blocks on Lock() forever, and every shard on this worker plus Host.Close
// (which joins the worker) hangs with no diagnostic beyond a stuck process.
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

	// Drain the free-order messages (e.g. ReadIndex-confirmation heartbeats)
	// generated by the ReadIndex/HeartbeatResp processing above so they are
	// sent NOW, even when Phase 2 early-returns because there are no proposals
	// or other inbox messages. Without this, those heartbeats stayed buffered
	// until the next commit/apply cycle, adding up to a full cycle of read
	// latency under write load — defeating this method's purpose. Phase 2
	// drains again for any messages it generates; DrainFreeOrderMessages only
	// returns what is currently buffered, so there is no double-send.
	//
	// A CheckQuorum step-down driven by the HeartbeatResps stepped above
	// produces no proto.Update at all, so without publishing it the Host would
	// keep serving a stale IsLeader through GetShardInfo — the flag clients
	// route writes by. The publish itself happens in the caller, after the
	// deferred unlock.
	freeOrder = node.peer.DrainFreeOrderMessages()

	// Extract ReadIndex completions immediately after processing
	// HeartbeatResp. This allows us to deliver read results before
	// processing proposals, reducing read latency.
	readStates = node.peer.DrainReadStatesOnly()

	return freeOrder, readStates, readIndexErr
}

// commitPendingPhase2Locked appends the batched proposals and steps the
// remaining inbox messages into the peer with node.raftMu held, returning the
// free-order messages to send, any further ReadIndex completions, and the
// proposal submit error.
//
// proposeErr is returned rather than acted on here for a concrete reason:
// peer.Handle(Propose) on a leaderless follower returns NotLeaderError, which
// makes this path hot rather than exotic, and the host's completion of every
// stranded proposal is unbounded work to hold node.raftMu across. The same
// applies to the message send and the leadership publish the caller performs
// afterwards; none of them may move into this method.
//
// The locked region is a method so the unlock can be deferred without
// extending the hold: a panic in peer.Handle, or in the metrics collector the
// proposal path calls, would otherwise strand node.raftMu and deadlock the
// restarted step worker against every shard it owns.
func (w *stepWorker) commitPendingPhase2Locked(
	shardID uint64,
	node *Node,
	otherMsgs []proto.Message,
) (freeOrder []proto.Message, readStates []proto.ReadyToRead, proposeErr error) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()

	// Process batched proposals. Unlike the normal processReady path which
	// skips proposal drain during commitPending, we handle proposals here
	// to enable pipelining. The proposals are appended to the Raft log and
	// will be included in the next update after commitPending clears.
	// This reduces latency by eliminating the blocking window where
	// proposals sit in the MPSC queue waiting for commit to complete.
	if len(w.commitProposeBuf) > 0 {
		if w.metrics != nil {
			w.metrics.ObserveProposalBatchSize(shardID, len(w.commitProposeBuf))
		}
		if err := node.peer.Handle(proto.Message{
			Type:    proto.Propose,
			Entries: w.commitProposeBuf,
			Origin:  proto.OriginLocal,
		}); err != nil {
			slog.Warn("batched proposal failed (commitPending)",
				"shard_id", shardID,
				"replica_id", node.ReplicaID(),
				"num_entries", len(w.commitProposeBuf),
				"error", err,
			)
			proposeErr = err
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

	// Flush deferred ReadIndex heartbeat broadcasts generated by the otherMsgs
	// stepped above (e.g. a follower-forwarded ReadIndex parks its heartbeat
	// broadcast until flushed). Mirrors the phase 1 flush: without it those
	// heartbeats sit buffered a full commit/apply cycle, adding that latency to
	// the reads they confirm.
	node.peer.FlushReadIndex()

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
	freeOrder = node.peer.DrainFreeOrderMessages()

	// Check for any additional ReadIndex completions that may have been
	// generated by processing other inbox messages (e.g., ReadIndexResp
	// from forwarded ReadIndex requests).
	readStates = node.peer.DrainReadStatesOnly()

	return freeOrder, readStates, proposeErr
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

// clearStepCommitPending unwedges the ONE node that was mid hand-off to the
// commit pipeline (enqueueForCommit or commitMemoryMode) when a step-worker
// panic hit: its commitPending is set but its item never reached commitC/applyC,
// so nothing else will ever clear it. Called by the engine's restart loop; it is
// the ONLY thing that recovery does to the pipeline, because items already in
// commitC are owned by the commit worker and are committed normally after the
// restart.
//
// It is deliberately NOT a blanket clear over every owned node. A node whose
// item is in the commit worker's in-flight batch legitimately holds
// commitPending across SaveState; clearing it there would let the restarted step
// worker GetUpdate again while the first update is still in flight (double
// extraction, double apply, double ReleaseCommittedEntries).
//
// Runs on the step-worker goroutine during the panic unwind, the same goroutine
// that writes enqueuingNode, so no locking is required.
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

// discardExtractedUpdate withdraws the durability claim GetUpdate made on
// behalf of an update that the panic just destroyed.
//
// GetUpdate advances prevState when it hands an update out, not when the engine
// persists it. If the update never reaches SaveState, prevState is left
// asserting that a term and vote are on disk when they are not, and because the
// ReadOnly classification is computed from exactly that diff, the NEXT update
// is marked ReadOnly and skips the WAL as well. The hard state then never gets
// written at all: the node answers RequestVote from memory, crashes, comes back
// with no record of the grant, and can grant the same term to a second
// candidate.
//
// The lost readStates are not recoverable here — they were drained out of the
// Peer by the same GetUpdate — and those ReadIndex callers are completed by the
// host's sweeper. Durability of the hard state is the part that cannot be left
// to heal on its own.
//
// Runs on the step-worker goroutine during the panic unwind, which is the same
// goroutine that holds exclusive Peer access, so no locking is required.
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
	delete(w.prevLeaderTerms, shardID)
	delete(w.deferredSnapshots, shardID)
	w.nodesMu.Unlock()
}
