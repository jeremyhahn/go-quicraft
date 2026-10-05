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
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/server"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// RaftMetrics is the metrics interface consumed by the engine for proposal,
// commit, apply, election, and snapshot measurements. When nil, all metric
// collection is skipped (zero overhead). The concrete implementation lives
// in the public quicraft package to avoid an import cycle.
type RaftMetrics interface {
	ObserveProposalLatency(shardID uint64, d time.Duration)
	ObserveCommitLatency(shardID uint64, d time.Duration)
	ObserveApplyLatency(shardID uint64, d time.Duration)
	IncProposalDropped(shardID uint64)
	ObserveProposalBatchSize(shardID uint64, size int)
	ObserveCommitBatchSize(size int)
	IncElection(shardID uint64)
	IncSnapshot(shardID uint64)
	IncPiggybackCommit(shardID uint64)
}

// Engine is the top-level coordinator that manages the worker pool
// driving Raft shards through their lifecycle. It creates and manages
// step, commit, apply, and snapshot workers.
//
// Shards are partitioned across workers by fixed hash: shardID % numWorkers.
// This ensures no contention between workers for the same shard.
type Engine struct {
	cfg           config.HostConfig
	logdb         logdb.LogDB
	sender        MessageSender
	callback      Callback
	stepWorkers   []*stepWorker
	commitWorkers []*commitWorker
	applyWorkers  []*applyWorker
	snapshots     *snapshotPool
	workSignal    *WorkSignal
	applyPool     *sync.Pool
	stepWg        sync.WaitGroup
	commitWg      sync.WaitGroup
	applyWg       sync.WaitGroup
	snapshotWg    sync.WaitGroup
	listenerWg    sync.WaitGroup

	// listeners delivers user-visible notifications (leadership changes,
	// campaign launches, shard failures) on a dedicated goroutine instead of on
	// the worker that produced them, so a listener may call back into the Host
	// — including the synchronous StopShard — without deadlocking against the
	// engine lock its producing worker holds. See listenerDispatcher.
	listeners *listenerDispatcher

	// allNodes is a concurrent-safe map of shardID -> *Node, enabling
	// lock-free message delivery from the transport layer without step
	// worker coordination. Uses sync.Map for O(1) lock-free reads on
	// the DeliverMessage hot path.
	allNodes sync.Map

	// globalRL is the global (cross-shard) in-memory rate limiter.
	// Nil when global rate limiting is disabled.
	globalRL *server.RateLimiter

	// tickInterval is the real duration of one Raft tick: the period of
	// every step worker's ticker.
	tickInterval time.Duration

	// metrics collects raft-level metrics (proposal, commit, apply,
	// election, snapshot). Nil when metrics are disabled.
	metrics RaftMetrics

	// Per-tier stop channels for ordered shutdown.
	stepStopC     chan struct{}
	commitStopC   chan struct{}
	applyStopC    chan struct{}
	snapshotStopC chan struct{}

	// commitC and applyC are the inter-worker channels, one per worker.
	commitCs []chan *commitItem
	applyCs  []chan *applyItem

	// started tracks whether Start() has been called. Used to guard
	// against configuration methods (SetSender, SetCallback) being
	// called after workers are running, which would be a data race.
	started atomic.Bool

	// stopOnce makes Stop idempotent, matching Node.Stop. Every stop channel
	// below is closed unconditionally, so a second Stop panicked on "close of
	// closed channel" — and Stop is reachable twice through Host.Close, which
	// documents itself as idempotent, and through a test or supervisor that
	// stops the engine directly before closing the host.
	stopOnce sync.Once
}

// NewEngine creates an Engine with the configured number of workers.
// Workers are not started until Start() is called. The metrics parameter
// may be nil to disable metric collection.
func NewEngine(cfg config.HostConfig, ldb logdb.LogDB, sender MessageSender, barrier SnapshotBarrier, callback Callback, metrics RaftMetrics) *Engine {
	numWorkers := int(cfg.NumWorkers)
	if numWorkers < 1 {
		numWorkers = 1
	}
	maxApplyWorkers := cfg.MaxApplyWorkers
	if maxApplyWorkers < 1 {
		maxApplyWorkers = 1
	}
	commitBufSize := cfg.CommitCBufferSize
	if commitBufSize < 1 {
		commitBufSize = 64
	}
	applyBufSize := commitBufSize * 4
	if applyBufSize < 256 {
		applyBufSize = 256
	}

	var globalRL *server.RateLimiter
	if cfg.MaxTotalInMemLogSize > 0 {
		globalRL = server.NewRateLimiter(cfg.MaxTotalInMemLogSize)
	}

	// Compute tick interval from RTTMillisecond. Each Raft tick
	// corresponds to one RTT, so election and heartbeat timeouts
	// (expressed in ticks) map to real network latency. Default to
	// 1ms if RTTMillisecond is not set (pre-Validate config).
	rttMs := cfg.RTTMillisecond
	if rttMs == 0 {
		rttMs = 1
	}
	tickInterval := time.Duration(rttMs) * time.Millisecond

	snapshotWorkers := numWorkers / 2
	if snapshotWorkers < 2 {
		snapshotWorkers = 2
	}

	stepStopC := make(chan struct{})
	commitStopC := make(chan struct{})
	applyStopC := make(chan struct{})
	snapshotStopC := make(chan struct{})

	workSignal := NewWorkSignal(numWorkers)

	applyPool := &sync.Pool{
		New: func() interface{} {
			return &applyItem{}
		},
	}

	// Create per-worker channels.
	commitCs := make([]chan *commitItem, numWorkers)
	applyCs := make([]chan *applyItem, numWorkers)
	for i := range numWorkers {
		commitCs[i] = make(chan *commitItem, commitBufSize)
		applyCs[i] = make(chan *applyItem, applyBufSize)
	}

	// Create snapshot pool before workers so apply workers can reference it.
	snapshots := newSnapshotPool(snapshotWorkers, ldb, barrier, callback, workSignal, snapshotStopC, metrics)

	listeners := newListenerDispatcher()

	// Create workers.
	stepWorkers := make([]*stepWorker, numWorkers)
	commitWorkers := make([]*commitWorker, numWorkers)
	applyWorkers := make([]*applyWorker, numWorkers)

	// When LogDB is nil (memory mode), step workers inline Peer.Commit
	// and forward directly to applyC, bypassing the commit worker
	// goroutine. This reduces the pipeline from 3 hops to 2 hops.
	memoryMode := ldb == nil

	for i := range numWorkers {
		sw := newStepWorker(i, tickInterval, sender, workSignal, commitCs[i], callback, stepStopC, metrics)
		sw.listeners = listeners
		sw.snapshots = snapshots
		if memoryMode {
			sw.memoryMode = true
			sw.applyC = applyCs[i]
			sw.applyPool = applyPool
		}
		stepWorkers[i] = sw
		commitWorkers[i] = newCommitWorker(i, ldb, sender, commitCs[i], applyCs[i], applyPool, workSignal, callback, commitStopC, metrics)
		// The commit worker forwards committed items to the apply tier. During
		// ordered shutdown its own stopC (commitStopC) closes in Stop step 3
		// while the apply tier stays live until step 4, so the forward's
		// give-up guard must watch applyStopC — not commitStopC — to avoid
		// abandoning a WAL-persisted, Commit-advanced item on a coin flip.
		commitWorkers[i].applyStopC = applyStopC
		applyWorkers[i] = newApplyWorker(i, applyCs[i], applyPool, maxApplyWorkers, &sync.WaitGroup{}, callback, snapshots, workSignal, applyStopC, metrics)
	}

	e := &Engine{
		cfg:           cfg,
		logdb:         ldb,
		sender:        sender,
		callback:      callback,
		stepWorkers:   stepWorkers,
		commitWorkers: commitWorkers,
		applyWorkers:  applyWorkers,
		snapshots:     snapshots,
		workSignal:    workSignal,
		applyPool:     applyPool,
		globalRL:      globalRL,
		tickInterval:  tickInterval,
		stepStopC:     stepStopC,
		commitStopC:   commitStopC,
		applyStopC:    applyStopC,
		snapshotStopC: snapshotStopC,
		commitCs:      commitCs,
		applyCs:       applyCs,
		metrics:       metrics,
		listeners:     listeners,
	}

	// Wire the snapshot pool's node enumerator so epoch purging can compute
	// a GLOBAL safe-purge floor across every shard sharing the process-wide
	// barrier. Without this, a single shard's purge could delete a DEK still
	// needed by another shard's retained snapshot.
	snapshots.nodes = e.loadedNodes

	return e
}

// loadedNodes returns a snapshot of every node currently loaded in the
// engine. Used by the snapshot pool to scan all shards' snapshot
// directories when computing the global safe-purge floor for barrier
// epoch DEKs.
func (e *Engine) loadedNodes() []*Node {
	var nodes []*Node
	e.allNodes.Range(func(_, val any) bool {
		if node, ok := val.(*Node); ok {
			nodes = append(nodes, node)
		}
		return true
	})
	return nodes
}

// SetShardBarriers installs a per-shard barrier resolver on the snapshot
// pool, enabling per-tenant WAL/snapshot encryption where each Raft shard can
// be sealed under its OWN barrier. It is purely additive: when never called
// (the default), the snapshot pool uses the single process-wide barrier passed
// to NewEngine and behaves byte-for-byte as before. A nil return from fn for a
// given shard falls back to that process-wide barrier.
//
// Must be called during Host construction, before Start() launches the
// snapshot workers, so the resolver is visible to every snapshot operation.
func (e *Engine) SetShardBarriers(fn func(shardID uint64) SnapshotBarrier) {
	e.snapshots.shardBarriers = fn
}

// GlobalSafePurgeFloor returns the minimum barrier epoch still required by
// any retained encrypted snapshot (across all shards) or any live WAL
// record. DEKs for epochs strictly below this floor can be safely purged.
//
// The second return value is false when the floor cannot be determined
// (errors scanning snapshot dirs, MinLiveEpoch failure) or when nothing
// encrypted is retained. Callers MUST NOT purge any epoch when ok is
// false: purging on uncertainty risks bricking encrypted data.
func (e *Engine) GlobalSafePurgeFloor() (uint64, bool) {
	return e.snapshots.globalSafePurgeFloor()
}

// Start launches all worker goroutines. Each worker tier is tracked by
// its own WaitGroup so Stop() can perform ordered tier-by-tier shutdown.
func (e *Engine) Start() {
	e.started.Store(true)

	// Start the listener dispatcher FIRST. Every worker tier below can produce
	// a notification the instant it starts, and until the dispatcher goroutine
	// is running those notifications are invoked inline on the producing
	// worker — the very reentrancy hazard the dispatcher exists to remove.
	e.listeners.start(&e.listenerWg)

	// Start snapshot pool workers.
	e.snapshots.start(&e.snapshotWg)

	// Start apply workers with restart-on-panic. If an apply worker's
	// dispatcher goroutine panics (during dispatch() or drain()), all
	// shards assigned to that worker would stop making progress without
	// restart logic. The restart loop re-enters the dispatcher after a
	// brief delay to avoid tight panic loops.
	//
	// Every tier runs under supervise.Supervise, which also restarts a worker
	// whose goroutine a runtime.Goexit ended (recover cannot catch one).
	for _, aw := range e.applyWorkers {
		// The stopped path runs the dispatcher's stop path, user code
		// included, so it is resilient (supervise.RunResilient).
		supervise.Supervise(&e.applyWg, aw.stopC,
			func() { e.runApplyWorkerLoop(aw) },
			func() { supervise.RunResilient(func() { e.finishStoppedApplyWorker(aw) }) })
	}

	// Start commit workers with restart-on-panic. If a commit worker
	// panics, all shards assigned to it would stall permanently without
	// restart logic. The restart loop clears commitPending on all nodes
	// owned by this worker to unblock them, then restarts after a brief
	// delay to avoid tight panic loops.
	for _, cw := range e.commitWorkers {
		// A worker that exited abnormally once the engine is stopping still
		// owes its shutdown drain: persisting and forwarding what is queued
		// in commitC. It runs resiliently, each run under the same recovery
		// as the worker's own (runCommitWorkerOnce returns true after a
		// recovered panic, and its recovery settles a Goexit before the
		// helper exits).
		supervise.Supervise(&e.commitWg, cw.stopC,
			func() { e.runCommitWorkerLoop(cw) },
			func() {
				supervise.RunResilient(func() {
					for e.runCommitWorkerOnce(cw) {
					}
				})
			})
	}

	// Start step workers with restart-on-panic. On panic the recovery clears
	// commitPending for the single node that was mid-enqueue, then restarts
	// after a brief delay.
	for _, sw := range e.stepWorkers {
		supervise.Supervise(&e.stepWg, sw.stopC, func() { e.runStepWorkerLoop(sw) }, nil)
	}
}

// LoadNode adds a node to the appropriate worker partition and the
// concurrent-safe allNodes map. The node is assigned to
// stepWorker[shardID % numWorkers].
func (e *Engine) LoadNode(node *Node) {
	workerID := int(node.shardID % uint64(len(e.stepWorkers)))

	// Clear the shard from the commit and apply workers' unloaded sets
	// BEFORE publishing the node to the step worker. This is a no-op if the
	// shard was never unloaded. Ordering matters on StopShard/StartShard of
	// the same shardID: publishing first lets the new incarnation's first
	// update reach applyC while the shard is still marked unloaded, and the
	// apply dispatcher would drop that batch permanently (Peer.Commit has
	// already advanced the processed marker, so it is never re-offered).
	// Enqueueing the reload first makes it strictly precede any applyItem the
	// new incarnation can produce; the dispatcher consumes reload requests
	// before dispatching (see applyWorker.run).
	//
	// The apply-side reload rendezvouses with the dispatcher goroutine (it
	// shares the unbuffered membership channel with requestUnload so unload
	// and reload can never be applied out of order), so it is skipped before
	// Start for the same reason UnloadNode skips the apply-side unload: with
	// no dispatcher running nothing can have been unloaded, so the unloaded
	// set is provably empty and the reload would have nothing to clear.
	e.commitWorkers[workerID].requestReload(node.shardID)
	if e.started.Load() {
		e.applyWorkers[workerID].requestReload(node.shardID)
	}

	// Route this node's EventListener notifications (OnShardFailed) through the
	// engine's dispatcher rather than the worker goroutine that detects the
	// failure. handleError is reachable from the step worker under
	// nodesMu.RLock and from a per-shard apply goroutine that a synchronous
	// UnloadNode joins on, so a listener that reacts by stopping the shard
	// deadlocks against its own producer without this.
	node.listeners = e.listeners

	e.stepWorkers[workerID].loadNode(node)

	e.allNodes.Store(node.shardID, node)
}

// UnloadNode removes a node from its worker partition and the
// concurrent-safe allNodes map. It is SYNCHRONOUS end-to-end: on return,
// the step worker no longer owns the shard, the node is stopped (stopC
// closed, cancelling in-flight snapshot-pool operations and releasing any
// apply goroutine parked on the recovering/saving gate), and the shard's
// per-shard apply goroutine has fully exited — so no SM.Apply is or will
// be in flight. Callers (Host.StopShard) rely on this guarantee to close
// the state machine immediately afterwards without racing an apply.
//
// UnloadNode must NOT be called from an engine worker goroutine. There are TWO
// distinct self-deadlocks, not one, and neither is recoverable:
//
//   - The STEP worker holds stepWorker.nodesMu.RLock for the whole of tick()
//     and processReady(). unloadNode below takes nodesMu.Lock(), and Go's
//     RWMutex is not reentrant, so a call made from that goroutine never
//     returns. Every shard the worker owns (shardID % numWorkers) then stops
//     ticking — no heartbeats, no elections, no inbox drain — and Engine.Stop
//     blocks forever in stepWg.Wait().
//   - A per-shard APPLY goroutine is what the requestUnload wait below joins
//     on, so a call made from that goroutine waits for itself to return.
//
// The engine removes the reachable instances of this rather than relying on the
// contract: every user-visible notification it fires (OnShardFailed, and the
// leadership/campaign callbacks the step worker produces) is delivered by the
// listener dispatcher, a goroutine that holds no engine lock and is joined
// last, so a listener may call StopShard from any of them. Callbacks that
// remain synchronous are engine-to-Host plumbing (OnApplied, OnCommitted,
// OnReadyToRead, OnProposalFailed, OnReadIndexBatched, OnReadIndexFailed); a
// Host implementation that fans any of those out to user code must dispatch
// that fan-out itself, or the hazard returns through it.
func (e *Engine) UnloadNode(shardID uint64) {
	workerID := int(shardID % uint64(len(e.stepWorkers)))

	// Stop the node FIRST, before any worker-side unload. Closing node.stopC
	// releases an apply goroutine blocked on the recovering/saving gate,
	// aborts snapshot save/recover operations that select on it, and releases
	// a step worker parked in a commitC/applyC send for this shard. Taking
	// the step worker's nodesMu write lock first would instead make this call
	// wait behind that parked send, and the apply-side wait below would stall
	// behind the gated apply.
	node := e.GetNode(shardID)
	if node != nil {
		// Marked before it stops, so no tier can see the stop without the
		// reason: the pipeline's incarnation guards drop an unloaded
		// incarnation's work, and keep an engine shutdown's.
		node.unloaded.Store(true)
		node.Stop()
		// A snapshot receive writing on this incarnation's behalf — its
		// publish, or the record of an install raft adopted — finishes before
		// a new incarnation can load LogDB and the snapshot tree.
		node.awaitWriteLeases()
	}

	e.stepWorkers[workerID].unloadNode(shardID)

	// Signal the commit worker to skip Peer.Commit for this shard, and WAIT
	// until it has taken the change: from then on no SaveState for this
	// incarnation is running or can start (see commitWorker.requestUnload).
	// A StartShard that follows therefore reads a LogDB the stopped
	// incarnation can no longer write to — an old HardState landing after the
	// new one loaded could otherwise grant a second vote in the same term.
	// Before Start there is no commit goroutine and nothing in flight.
	unloaded := e.commitWorkers[workerID].requestUnload(shardID)
	if e.started.Load() {
		e.commitWorkers[workerID].awaitMembership(unloaded)
	}

	// Unload the apply side and WAIT for completion. The request is
	// processed by the dispatcher goroutine (preserving the single-owner
	// invariant on shardChans); done closes only after the per-shard
	// channel is closed AND the per-shard apply goroutine has exited. The
	// wait is bounded by the current SM.Apply batch — deliberately no
	// timeout, because abandoning the wait would reintroduce the
	// apply-racing-Close use-after-close this exists to prevent. During
	// engine shutdown the stopping dispatcher owns the join, and the wait
	// ends when it has joined every per-shard goroutine — never sooner, or a
	// StopShard racing Host.Close would close a state machine the shutdown
	// drain is still applying to. Before Start there is no dispatcher goroutine
	// to rendezvous with and nothing can be in flight, so the apply-side
	// unload is skipped entirely.
	if e.started.Load() {
		<-e.applyWorkers[workerID].requestUnload(shardID)
	}

	// Return this shard's share of the SHARED host-wide in-memory rate limiter.
	// Each shard tracks its own contribution so it can apply deltas without
	// disturbing the others, and that contribution lives on the raft log and
	// dies with the Peer — so without this, a stopped shard's bytes stay counted
	// against a shard that no longer exists and every StartShard/StopShard cycle
	// leaks more, until the accumulated orphans cross MaxTotalInMemLogSize and
	// every propose on the host fails ErrSystemBusy with no memory held.
	//
	// It happens HERE, last, and under raftMu — not in stepWorker.unloadNode.
	// That runs before the commit worker has been told to skip Peer.Commit, so
	// an in-flight Commit could still be inside appliedTo -> subGlobalSize,
	// mutating the very same accounting: a data race, and one that also corrupts
	// the shared counter. By this point the step worker has dropped the node,
	// the commit worker skips it, and the apply goroutine has exited; raftMu
	// then serialises against any Commit still unwinding.
	if node != nil && node.peer != nil {
		releaseGlobalRateLimitLocked(node)
	}

	e.allNodes.Delete(shardID)
}

// releaseGlobalRateLimitLocked returns the shard's share of the shared host-wide
// in-memory rate limiter with node.raftMu held, so the accounting cannot race a
// Peer.Commit still unwinding inside appliedTo -> subGlobalSize.
//
// It is a function rather than an inline lock/unlock pair purely so the unlock
// is deferred. A panic inside ReleaseGlobalRateLimit (a torn-down peer, a
// corrupt limiter) would otherwise leak node.raftMu forever, and the next
// worker to take it — the step or commit worker after its recover-and-restart —
// blocks with no diagnostic. The deferred scope covers exactly the one call the
// explicit unlock covered: the allNodes.Delete that follows in the caller must
// stay outside, and no lock is needed for it.
func releaseGlobalRateLimitLocked(node *Node) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()
	node.peer.ReleaseGlobalRateLimit()
}

// DeliverMessage routes a WIRE-RECEIVED Raft message to the target shard's
// node inbox for processing by the step worker. Returns true if the
// message was delivered, false if the shard is not loaded or the
// inbox is full.
//
// This is the transport's entry point and the message is stamped
// OriginRemote (see Node.Deliver). Locally injected control messages must
// use DeliverLocal.
//
// Safe for concurrent use by multiple goroutines (transport layer).
func (e *Engine) DeliverMessage(shardID uint64, msg proto.Message) bool {
	node := e.lookupNode(shardID, "DeliverMessage")
	if node == nil {
		return false
	}
	return node.Deliver(msg)
}

// DeliverLocal routes a message injected by this process to the target
// shard's node inbox, stamping it OriginLocal. Returns true if the message
// was delivered, false if the shard is not loaded or the inbox is full.
//
// Safe for concurrent use by multiple goroutines.
func (e *Engine) DeliverLocal(shardID uint64, msg proto.Message) bool {
	node := e.lookupNode(shardID, "DeliverLocal")
	if node == nil {
		return false
	}
	return node.DeliverLocal(msg)
}

// DeliverLocalTo injects a process-local message into exactly node, rather
// than whatever node currently holds its shard. The snapshot receive path uses
// it for an install whose verdict it awaits on that node: delivering by shard
// ID lets a StopShard/StartShard during the receive hand the install to the
// NEW node, which adopts it while the receive, waiting on the old one, is told
// it was abandoned. A stopped node accepts nothing.
func (e *Engine) DeliverLocalTo(node *Node, msg proto.Message) bool {
	if node == nil || node.IsStopped() {
		return false
	}
	return node.DeliverLocal(msg)
}

// lookupNode resolves a loaded shard's node, reporting a type-assertion
// failure against the named caller.
func (e *Engine) lookupNode(shardID uint64, context string) *Node {
	val, ok := e.allNodes.Load(shardID)
	if !ok {
		return nil
	}
	node, nodeOK := val.(*Node)
	if !nodeOK {
		slog.Error("node lookup type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      context + " node lookup",
				ExpectedType: "*Node",
				ActualValue:  val,
			})
		return nil
	}
	return node
}

// GetNode returns the engine Node for the given shardID, or nil if
// not loaded. Safe for concurrent use.
func (e *Engine) GetNode(shardID uint64) *Node {
	val, ok := e.allNodes.Load(shardID)
	if !ok {
		return nil
	}
	node, nodeOK := val.(*Node)
	if !nodeOK {
		slog.Error("GetNode type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "GetNode node lookup",
				ExpectedType: "*Node",
				ActualValue:  val,
			})
		return nil
	}
	return node
}

// NotifyWork signals that a shard has pending work. The notification
// is routed to the appropriate step worker via WorkSignal.
func (e *Engine) NotifyWork(shardID uint64) {
	e.workSignal.Notify(shardID)
}

// Stop performs an ordered shutdown of the engine. Each worker tier is
// stopped and fully drained before the next tier is signaled, ensuring
// committed entries flow through the entire pipeline before shutdown:
//
//  1. Stop all loaded nodes (closes stopC, unblocking snapshot operations)
//  2. Stop step workers, wait until all exit (no new commits produced)
//  3. Stop commit workers, wait until all exit (drains commitC, forwards to applyC)
//  4. Stop apply workers, wait until all exit (drains applyC, applies to SM)
//  5. Stop snapshot pool workers, wait until all exit
//  6. Stop the listener dispatcher, draining queued notifications
//
// This sequential close-and-wait pattern guarantees that no in-flight
// items are lost between tiers.
//
// Stop is idempotent, like Node.Stop: a second call is a no-op, and a
// concurrent one blocks until the first has finished the whole ordered
// shutdown. Callers may therefore stop the engine defensively.
func (e *Engine) Stop() {
	e.stopOnce.Do(e.stop)
}

// stop performs the ordered shutdown exactly once; see Stop.
func (e *Engine) stop() {
	// 1. Stop all loaded nodes. This closes each node's stopC channel,
	// which unblocks any in-flight snapshot save/recover operations
	// that select on node.stopC. Without this, snapshot operations
	// would block indefinitely during shutdown.
	e.allNodes.Range(func(_, val any) bool {
		node, ok := val.(*Node)
		if !ok {
			slog.Error("Stop: allNodes value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "Stop allNodes range",
					ExpectedType: "*Node",
					ActualValue:  val,
				})
			return true // skip malformed entry
		}
		node.Stop()
		return true
	})

	// 2. Stop step workers first. After this returns, no new Updates
	// will be produced and no new items will enter commitC.
	close(e.stepStopC)
	e.stepWg.Wait()

	// 3. Stop commit workers. They drain remaining commitC items,
	// persist to LogDB, and forward to applyC before exiting.
	close(e.commitStopC)
	e.commitWg.Wait()

	// 4. Stop apply workers. They drain remaining applyC items and
	// apply to state machines before exiting.
	close(e.applyStopC)
	e.applyWg.Wait()

	// 5. Stop snapshot workers.
	close(e.snapshotStopC)
	e.snapshotWg.Wait()

	// 6. Stop the listener dispatcher LAST, once every tier that can produce a
	// notification has exited. It drains what they queued before returning, so
	// a leadership loss or shard failure observed during teardown still reaches
	// the listener. A listener that blocks here blocks Stop, exactly as it
	// would have blocked its producing worker before: the non-blocking contract
	// is not weakened by the dispatcher, only relocated to a goroutine that
	// holds no engine lock.
	e.listeners.stop()
	e.listenerWg.Wait()
}

// runApplyWorkerLoop runs an apply worker dispatcher with automatic restart
// on panic. If the dispatcher panics (during dispatch() or drain()), the
// loop logs the panic with a full stack trace and restarts after a brief
// delay. The loop exits cleanly when the apply stop channel is closed.
//
// A shutdown that arrives while a panicked dispatcher is down does not end the
// loop directly: the dispatcher runs ONE more time, and its own stop path
// drains applyC and joins every per-shard goroutine (shutdownIfStopping, which
// run defers, so it runs even if that last run panics too). Returning straight
// away skipped the join — leaving per-shard goroutines running into Close and
// joinedC open, so an UnloadNode waiting on it hung forever.
func (e *Engine) runApplyWorkerLoop(aw *applyWorker) {
	for e.runApplyWorkerOnce(aw) {
		if !supervise.AwaitRestart(aw.stopC) {
			e.finishStoppedApplyWorker(aw)
			return
		}
	}
}

// finishStoppedApplyWorker runs a panicked dispatcher's stop path once more —
// drain applyC, join every per-shard goroutine, close joinedC — unless the
// panicked run already joined. That happens when stopC closed while the panic
// was unwinding: the run's deferred shutdownIfStopping joined and closed
// joinedC, and running the dispatcher again could start a per-shard goroutine
// after joinedC had promised there were none. In that case applyC is NOT
// drained: its committed batches are left unapplied, as a crash leaves them,
// and are applied from the WAL on the next start.
func (e *Engine) finishStoppedApplyWorker(aw *applyWorker) {
	select {
	case <-aw.joinedC:
		return
	default:
	}
	e.runApplyWorkerOnce(aw)
}

// runApplyWorkerOnce runs the apply worker dispatcher and recovers from panics.
// Returns true if the worker panicked, false if it returned normally.
func (e *Engine) runApplyWorkerOnce(aw *applyWorker) (panicked bool) {
	normalReturn := false
	defer func() {
		if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
			panicked = true
			// The dispatcher panicked holding an applyItem it had received from
			// applyC but not yet handed to a per-shard goroutine. That item's
			// commitPending is set: dropping it silently would wedge the shard
			// forever (no apply completes to clear the flag, so the step worker
			// never produces another update). Discard it via the fail-the-node
			// idiom — release its pooled entries, mark the shard failed, and
			// report a typed error — because the committed batch is lost and the
			// state machine would otherwise silently miss the range.
			settleWorkerExit("apply", aw.workerID, r, func(error) {
				if item := aw.inFlight.Swap(nil); item != nil {
					aw.failInFlightItem(item)
				}
			})
		}
	}()
	aw.run()
	normalReturn = true
	return false
}

// settleWorkerExit is the one shape of every worker recovery: build the
// *WorkerPanicError for the exit (no user code: the panic value is stored, not
// formatted), run settle with it — everything structural — and only then
// report the exit, through supervise.LogContained, so a slog handler or the panic value's
// own methods can neither skip the settlement nor escape the recovery.
func settleWorkerExit(workerType string, workerID int, r any, settle func(cause error)) {
	stack := debug.Stack()
	panicErr := &WorkerPanicError{
		WorkerID:   workerID,
		WorkerType: workerType,
		Value:      r,
		Stack:      stack,
	}
	settle(panicErr)
	supervise.LogContained(func() {
		slog.Error(workerType+" worker panic recovered, restarting",
			"worker_id", workerID,
			"panic", fmt.Sprint(r),
			"stack", string(stack),
			"error", panicErr,
		)
	})
}

// runCommitWorkerLoop runs a commit worker with automatic restart on
// panic. If the worker panics, the loop logs the panic with a full stack
// trace, unblocks the nodes whose updates died with the panicking batch,
// and restarts the worker after a brief delay. The loop exits cleanly when
// the commit stop channel is closed.
func (e *Engine) runCommitWorkerLoop(cw *commitWorker) {
	for e.runCommitWorkerOnce(cw) {
		if !supervise.AwaitRestart(cw.stopC) {
			return
		}
	}
}

// runCommitWorkerOnce runs the commit worker and recovers from panics.
// Returns true if the worker panicked, false if it returned normally.
func (e *Engine) runCommitWorkerOnce(cw *commitWorker) (panicked bool) {
	normalReturn := false
	defer func() {
		if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
			panicked = true
			settleWorkerExit("commit", cw.workerID, r, func(cause error) {
				cw.recoverAbnormalExit(cause, e.workSignal)
			})
		}
	}()
	cw.run()
	normalReturn = true
	return false
}

// runStepWorkerLoop runs a step worker with automatic restart on panic.
// If the worker panics, the loop logs the panic with a full stack trace and
// clears commitPending for the one node that was mid-enqueue, then restarts the
// worker after a brief delay. Items already queued in commitC are left for the
// commit worker, which owns them. The loop exits cleanly when the step stop
// channel is closed.
func (e *Engine) runStepWorkerLoop(sw *stepWorker) {
	for e.runStepWorkerOnce(sw) {
		if !supervise.AwaitRestart(sw.stopC) {
			return
		}
	}
}

// runStepWorkerOnce runs the step worker and recovers from panics.
// Returns true if the worker panicked, false if it returned normally.
func (e *Engine) runStepWorkerOnce(sw *stepWorker) (panicked bool) {
	normalReturn := false
	defer func() {
		if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
			panicked = true
			settleWorkerExit("step", sw.workerID, r, sw.recoverAbnormalExit)
		}
	}()
	sw.run()
	normalReturn = true
	return false
}

// ListenerNotificationsDropped returns the number of user-visible notifications
// the engine refused because the listener queue was full. A non-zero value means
// a registered EventListener is violating the non-blocking contract: campaign
// and shard-failure notifications were logged at ERROR instead of delivered.
// Leadership changes are never counted here — they are re-published on the next
// tick instead of dropped.
func (e *Engine) ListenerNotificationsDropped() uint64 {
	return e.listeners.droppedCount()
}

// DispatchListener hands fn — a fan-out to user EventListener code — to the
// listener dispatcher, which runs it on a goroutine holding no engine lock, in
// FIFO order with the engine's own notifications. It is how a Callback
// implementation meets the contract for its INLINE methods (see Callback):
// user code reached from one of them could otherwise call a synchronous Host
// API such as StopShard on a goroutine the unload has to wait for. It reports
// false, counted in ListenerNotificationsDropped, when the queue is full; the
// notification is then dropped, as the engine drops its own. Once the engine
// has stopped, fn runs inline, since no worker remains to deadlock with.
func (e *Engine) DispatchListener(fn func()) bool {
	return e.listeners.dispatch(fn)
}

// WorkSignal returns the work signal for external notification.
func (e *Engine) WorkSignal() *WorkSignal {
	return e.workSignal
}

// NumWorkers returns the number of workers per tier.
func (e *Engine) NumWorkers() int {
	return len(e.stepWorkers)
}

// StepWorker returns the step worker for the given index. Exposed for
// testing only.
func (e *Engine) StepWorker(idx int) *stepWorker {
	return e.stepWorkers[idx]
}

// CommitWorker returns the commit worker for the given index. Exposed
// for testing only.
func (e *Engine) CommitWorker(idx int) *commitWorker {
	return e.commitWorkers[idx]
}

// ApplyWorker returns the apply worker for the given index. Exposed
// for testing only.
func (e *Engine) ApplyWorker(idx int) *applyWorker {
	return e.applyWorkers[idx]
}

// SnapshotPool returns the snapshot pool. Exposed for testing only.
func (e *Engine) SnapshotPool() *snapshotPool {
	return e.snapshots
}

// GlobalRateLimiter returns the global in-memory rate limiter.
// Returns nil when global rate limiting is disabled.
func (e *Engine) GlobalRateLimiter() *server.RateLimiter {
	return e.globalRL
}

// TickInterval returns the real duration of one Raft tick, the period at
// which the step workers tick every loaded peer. A peer needs it to measure
// its read lease in real time (raft.Peer.SetTickInterval).
func (e *Engine) TickInterval() time.Duration {
	return e.tickInterval
}

// SnapshotRequestOption holds per-request snapshot options that are passed
// from the Host layer to the engine. These fields mirror the public
// SnapshotOption type but live in the engine package to avoid import cycles.
type SnapshotRequestOption struct {
	// ExportPath is the directory to export the snapshot to. Empty means
	// no export.
	ExportPath string

	// CompactionOverhead overrides the shard-level compaction overhead.
	// Only used when OverrideCompaction is true.
	CompactionOverhead uint64

	// OverrideCompaction controls whether CompactionOverhead is used.
	OverrideCompaction bool

	// Exported marks this snapshot as an export operation on the metadata.
	Exported bool

	// RequestKey identifies the requester waiting on this snapshot. It is
	// handed back through Callback.OnSnapshotRequestCompleted, after the
	// shard's snapshot slots are released, and must be non-zero for the
	// requester to be resolved.
	RequestKey uint64
}

// RequestSnapshot submits a snapshot save request for the given shard
// to the engine's snapshot pool. Returns a SnapshotDuplicateError if
// a snapshot is already in-flight for the shard, or a
// SnapshotPoolBusyError if the request channel is full.
//
// appliedToTerm is carried in the request only as a fallback term source; the
// snapshot pool records the term from the engine's apply-tracked
// (lastApplied, lastAppliedTerm) pair captured while applies are quiesced. The
// read of Peer.AppliedToTerm() is race-free because inMemory.appliedToTerm is
// atomic (the step worker's lock-free restore() writes it).
//
// The opt parameter carries per-request snapshot options. A zero-value
// SnapshotRequestOption uses shard-level defaults.
func (e *Engine) RequestSnapshot(shardID, replicaID uint64, node *Node, opt SnapshotRequestOption) error {
	var term uint64
	if peer := node.Peer(); peer != nil {
		term = peer.AppliedToTerm()
	}
	return e.snapshots.requestSnapshot(snapshotRequest{
		shardID:            shardID,
		replicaID:          replicaID,
		node:               node,
		appliedToTerm:      term,
		save:               true,
		exportPath:         opt.ExportPath,
		compactionOverhead: opt.CompactionOverhead,
		overrideCompaction: opt.OverrideCompaction,
		exported:           opt.Exported,
		requestKey:         opt.RequestKey,
	})
}

// RequestSnapshotRecovery submits a snapshot recovery request for the
// given shard to the engine's snapshot pool. This is called after a
// remote snapshot has been received and persisted to disk and LogDB.
// The snapshot pool worker will read the snapshot from disk and apply
// it to the state machine via Snapshotter.RecoverFromSnapshot.
func (e *Engine) RequestSnapshotRecovery(shardID, replicaID uint64, node *Node) error {
	return e.snapshots.requestRecovery(shardID, replicaID, node)
}

// FailShard trips node's shard-failure latch for a fault detected outside the
// engine's own workers, and reports it exactly as the engine reports its own:
// EventListener.OnShardFailed and Callback.OnShardFailed, once, from whichever
// failure wins the latch. The shard stays offline until it is reloaded; there
// is no automatic reset, deliberately. It reports whether this call tripped
// the latch.
//
// It is how the host fails a shard: a snapshot receive whose adopted snapshot
// cannot be recorded, and the routing mirror of the committed membership fold,
// which decodes the same entries a second time — runtime failing open where
// recovery fails closed, over the identical bytes, would leave a replica
// serving with a routing table that disagrees with the membership raft holds.
// Going through the one latch-and-report path is what keeps a failure that
// races an engine-side one from winning the latch without the engine callback,
// which would leave the host's pending reads undrained.
//
// Idempotent, and safe from any goroutine.
func (e *Engine) FailShard(node *Node, err error) bool {
	return failShard(node, e.callback, err, nil)
}

// SetSender replaces the message sender on the engine, all step
// workers, and all commit workers. This is used during Host
// initialization when the transport is created after the engine (to
// break a circular dependency). Must be called before Start or while
// the engine is quiescent.
func (e *Engine) SetSender(sender MessageSender) {
	if e.started.Load() {
		panic("engine: SetSender called after Start")
	}
	e.sender = sender
	for _, sw := range e.stepWorkers {
		sw.sender = sender
	}
	for _, cw := range e.commitWorkers {
		cw.sender = sender
	}
}

// SetCallback replaces the engine callback on the engine and all step,
// commit, apply, and snapshot workers. Must be called before Start or
// while the engine is quiescent.
func (e *Engine) SetCallback(callback Callback) {
	if e.started.Load() {
		panic("engine: SetCallback called after Start")
	}
	e.callback = callback
	for _, sw := range e.stepWorkers {
		sw.callback = callback
	}
	for _, cw := range e.commitWorkers {
		cw.callback = callback
	}
	for _, aw := range e.applyWorkers {
		aw.callback = callback
	}
	e.snapshots.setCallback(callback)
}
