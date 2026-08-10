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
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/server"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// errCommitDrainedAfterPanic marks a commit batch whose HardState was extracted
// by GetUpdate (advancing Peer.prevState) but never persisted, because a worker
// panicked before SaveState ran. The batch is discarded during panic recovery
// and its owning node is failed: the node's in-memory raft state has advanced
// past stable storage, so letting it keep running could re-send an unpersisted
// vote (a double vote) or ack entries that were never durable.
var errCommitDrainedAfterPanic = errors.New(
	"engine: commit batch discarded during worker panic recovery before SaveState")

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

	// allNodes is a concurrent-safe map of shardID -> *Node, enabling
	// lock-free message delivery from the transport layer without step
	// worker coordination. Uses sync.Map for O(1) lock-free reads on
	// the DeliverMessage hot path.
	allNodes sync.Map

	// globalRL is the global (cross-shard) in-memory rate limiter.
	// Nil when global rate limiting is disabled.
	globalRL *server.RateLimiter

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
		stepStopC:     stepStopC,
		commitStopC:   commitStopC,
		applyStopC:    applyStopC,
		snapshotStopC: snapshotStopC,
		commitCs:      commitCs,
		applyCs:       applyCs,
		metrics:       metrics,
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

// SetImportLocker installs the host's per-shard import try-lock on the snapshot
// pool. fn must take the lock without waiting and report whether it did,
// returning a release func when it did.
//
// Snapshot recovery reclaims orphaned import staging directories, which hold an
// imported state machine in PLAINTEXT and are indistinguishable by name from a
// live one. The lock is the only thing that tells them apart, so without this
// the recovery sweep can delete the staging directory of an import that is
// still writing. When never called, the pool assumes no host owns it and sweeps
// unconditionally.
//
// Must be called during Host construction, before Start() launches the snapshot
// workers, so the lock is visible to every recovery.
func (e *Engine) SetImportLocker(fn func(shardID uint64) (func(), bool)) {
	e.snapshots.tryImportLock = fn
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

	// Start snapshot pool workers.
	e.snapshots.start(&e.snapshotWg)

	// Start apply workers with restart-on-panic. If an apply worker's
	// dispatcher goroutine panics (during dispatch() or drain()), all
	// shards assigned to that worker would stop making progress without
	// restart logic. The restart loop re-enters the dispatcher after a
	// brief delay to avoid tight panic loops.
	for _, aw := range e.applyWorkers {
		e.applyWg.Add(1)
		go func() {
			defer e.applyWg.Done()
			e.runApplyWorkerLoop(aw)
		}()
	}

	// Start commit workers with restart-on-panic. If a commit worker
	// panics, all shards assigned to it would stall permanently without
	// restart logic. The restart loop clears commitPending on all nodes
	// owned by this worker to unblock them, then restarts after a brief
	// delay to avoid tight panic loops.
	for _, cw := range e.commitWorkers {
		e.commitWg.Add(1)
		go func() {
			defer e.commitWg.Done()
			e.runCommitWorkerLoop(cw)
		}()
	}

	// Start step workers with restart-on-panic. On panic the recovery drains
	// commitC (failing those nodes) and clears commitPending for the single
	// node that was mid-enqueue, then restarts after a brief delay.
	for _, sw := range e.stepWorkers {
		e.stepWg.Add(1)
		go func() {
			defer e.stepWg.Done()
			e.runStepWorkerLoop(sw)
		}()
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
// UnloadNode must NOT be called from an engine worker goroutine: waiting
// for the apply goroutine to exit from a callback that runs ON that
// goroutine would self-deadlock. All production callers run on Host/user
// goroutines, and the EventListener contract already forbids blocking
// calls from engine-worker callbacks.
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
		node.Stop()
	}

	e.stepWorkers[workerID].unloadNode(shardID)

	// Signal the commit worker to skip Peer.Commit for this shard.
	// This prevents in-flight commitItems from calling Commit on a
	// torn-down node.
	e.commitWorkers[workerID].requestUnload(shardID)

	// Unload the apply side and WAIT for completion. The request is
	// processed by the dispatcher goroutine (preserving the single-owner
	// invariant on shardChans); done closes only after the per-shard
	// channel is closed AND the per-shard apply goroutine has exited. The
	// wait is bounded by the current SM.Apply batch — deliberately no
	// timeout, because abandoning the wait would reintroduce the
	// apply-racing-Close use-after-close this exists to prevent. During
	// engine shutdown the request is acked immediately and Engine.Stop
	// owns the join instead. Before Start there is no dispatcher goroutine
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
		node.raftMu.Lock()
		node.peer.ReleaseGlobalRateLimit()
		node.raftMu.Unlock()
	}

	e.allNodes.Delete(shardID)
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
//
// This sequential close-and-wait pattern guarantees that no in-flight
// items are lost between tiers.
func (e *Engine) Stop() {
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
}

// workerPanicRestartDelay is the delay before restarting a panicked
// worker. This prevents tight panic-restart loops from consuming CPU
// and flooding logs when a worker hits a persistent panic trigger.
const workerPanicRestartDelay = 100 * time.Millisecond

// runApplyWorkerLoop runs an apply worker dispatcher with automatic restart
// on panic. If the dispatcher panics (during dispatch() or drain()), the
// loop logs the panic with a full stack trace and restarts after a brief
// delay. The loop exits cleanly when the apply stop channel is closed.
func (e *Engine) runApplyWorkerLoop(aw *applyWorker) {
	for {
		panicked := e.runApplyWorkerOnce(aw)
		if !panicked {
			return
		}
		// Check for shutdown before restarting.
		select {
		case <-aw.stopC:
			return
		default:
		}
		t := time.NewTimer(workerPanicRestartDelay)
		select {
		case <-aw.stopC:
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// runApplyWorkerOnce runs the apply worker dispatcher and recovers from panics.
// Returns true if the worker panicked, false if it returned normally.
func (e *Engine) runApplyWorkerOnce(aw *applyWorker) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			stack := debug.Stack()
			panicErr := &WorkerPanicError{
				WorkerID:   aw.workerID,
				WorkerType: "apply",
				Value:      r,
				Stack:      stack,
			}
			slog.Error("apply worker panic recovered, restarting",
				"worker_id", aw.workerID,
				"panic", fmt.Sprint(r),
				"stack", string(stack),
				"error", panicErr,
			)
			// The dispatcher panicked holding an applyItem it had received from
			// applyC but not yet handed to a per-shard goroutine. That item's
			// commitPending is set: dropping it silently would wedge the shard
			// forever (no apply completes to clear the flag, so the step worker
			// never produces another update). Discard it via the fail-the-node
			// idiom — release its pooled entries, mark the shard failed, and
			// report a typed error — because the committed batch is lost and the
			// state machine would otherwise silently miss the range.
			if item := aw.inFlight.Swap(nil); item != nil {
				aw.failInFlightItem(item)
			}
		}
	}()
	aw.run()
	return false
}

// runCommitWorkerLoop runs a commit worker with automatic restart on
// panic. If the worker panics, the loop logs the panic with a full stack
// trace, unblocks the nodes whose updates died with the panicking batch,
// and restarts the worker after a brief delay. The loop exits cleanly when
// the commit stop channel is closed.
func (e *Engine) runCommitWorkerLoop(cw *commitWorker) {
	for {
		panicked := e.runCommitWorkerOnce(cw)
		if !panicked {
			return
		}
		// Check for shutdown before restarting.
		select {
		case <-cw.stopC:
			return
		default:
		}
		t := time.NewTimer(workerPanicRestartDelay)
		select {
		case <-cw.stopC:
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// runCommitWorkerOnce runs the commit worker and recovers from panics.
// Returns true if the worker panicked, false if it returned normally.
func (e *Engine) runCommitWorkerOnce(cw *commitWorker) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			stack := debug.Stack()
			panicErr := &WorkerPanicError{
				WorkerID:   cw.workerID,
				WorkerType: "commit",
				Value:      r,
				Stack:      stack,
			}
			slog.Error("commit worker panic recovered, restarting",
				"worker_id", cw.workerID,
				"panic", fmt.Sprint(r),
				"stack", string(stack),
				"error", panicErr,
			)
			// Drain stale commitItems already queued in commitC BEFORE
			// clearing commitPending — the same ordering the step-worker
			// recovery uses, and for the same reason. The step worker is
			// still running: the instant commitPending is false it may call
			// getCommitSlot(shardID), which returns the SAME *commitItem
			// pointer that is still queued here, overwrite it, and enqueue it
			// again. commitC would then hold one pointer twice, so the same
			// batch could be committed and applied twice and its pooled
			// committed-entries slice returned to proto's pool twice — handing
			// one backing array to two shards.
			e.drainCommitC(cw.commitC)
			// Unwedge exactly the nodes whose updates died with the batch.
			// A blanket clear over every node this worker owns would also
			// clear nodes the STEP worker is mid-enqueueForCommit for: that
			// worker sets commitPending before its commitC send, so clearing
			// it lets the very next processReady call GetUpdate again while
			// the first update is still in flight. Raft's processed marker
			// has not advanced yet, so the same committed entries are
			// extracted TWICE (double Peer.Commit, double apply, double
			// ReleaseCommittedEntries on one pooled slice), and
			// getCommitSlot hands back the SAME *commitItem that is still
			// queued — one pointer enqueued twice. Items in the batch have
			// already been consumed from commitC and copied into w.batch, so
			// clearing them is safe; items still in commitC were cleared by
			// the drain above.
			cw.clearBatchCommitPending(e.workSignal)
			// The panic may have unwound out of processBatch before
			// clearBatch ran, leaving orderedMsgsBuf holding this batch's
			// VoteResp/HeartbeatResp and batch/commitTimes holding its
			// updates. The restarted worker would append the NEXT batch's
			// messages to those survivors and send them after that unrelated
			// batch's SaveState — sending a vote grant whose HardState was
			// never persisted (Raft PhD 10.2.1). Safe to touch here: the
			// worker goroutine is unwinding, so nothing else owns the batch.
			cw.clearBatch()
		}
	}()
	cw.run()
	return false
}

// drainCommitC non-blocking drains all items from the given commitC channel,
// FAILING each drained node. Called during panic recovery to discard stale
// in-flight commitItems that were enqueued before the panic. Draining an item
// BEFORE its node's commitPending is touched is what keeps the shard's
// pre-allocated commitItem slot from being reused with a stale pointer: the
// step worker can only reuse the slot once the item is out of the channel.
//
// Each drained item's HardState was extracted by GetUpdate (advancing
// Peer.prevState) but never persisted by SaveState — the batch died with the
// panic. Merely clearing commitPending and continuing would let the ReadOnly
// fast path re-send an unpersisted VoteResp, granting a second vote in the same
// term (two leaders). So the node is failed exactly as a terminal SaveState
// failure is: its in-memory raft state has diverged from stable storage and it
// must not keep voting or acking.
func (e *Engine) drainCommitC(commitC chan *commitItem) {
	for {
		select {
		case item := <-commitC:
			node := item.node
			u := item.update
			saveStateErr := &SaveStateError{
				ShardID:   u.ShardID,
				ReplicaID: u.ReplicaID,
				Err:       errCommitDrainedAfterPanic,
			}
			node.failed.Store(true)
			node.commitPending.Store(false)
			node.handleError(saveStateErr)
			// Complete pending proposals so callers see an error instead of
			// blocking to their context deadline against a now-failed node.
			failBatchProposals(e.callback, u, saveStateErr)
			// Return the pooled committed-entries slice AFTER the callback (which
			// must not retain it); otherwise every panic permanently drains
			// proto's entry-slice pool.
			u.ReleaseCommittedEntries()
			// Wake the step worker so it re-processes the shard promptly. The
			// node is failed, so the worker skips it — the notify only avoids a
			// one-tick delay before that skip is observed.
			e.workSignal.Notify(node.shardID)
		default:
			return
		}
	}
}

// runStepWorkerLoop runs a step worker with automatic restart on panic.
// If the worker panics, the loop logs the panic with a full stack trace,
// drains commitC (failing those nodes) and clears commitPending for the one
// node that was mid-enqueue, and restarts the worker after a brief delay. The
// loop exits cleanly when the step stop channel is closed.
func (e *Engine) runStepWorkerLoop(sw *stepWorker) {
	for {
		panicked := e.runStepWorkerOnce(sw)
		if !panicked {
			return
		}
		// Check for shutdown before restarting.
		select {
		case <-sw.stopC:
			return
		default:
		}
		t := time.NewTimer(workerPanicRestartDelay)
		select {
		case <-sw.stopC:
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// runStepWorkerOnce runs the step worker and recovers from panics.
// Returns true if the worker panicked, false if it returned normally.
func (e *Engine) runStepWorkerOnce(sw *stepWorker) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			stack := debug.Stack()
			panicErr := &WorkerPanicError{
				WorkerID:   sw.workerID,
				WorkerType: "step",
				Value:      r,
				Stack:      stack,
			}
			slog.Error("step worker panic recovered, restarting",
				"worker_id", sw.workerID,
				"panic", fmt.Sprint(r),
				"stack", string(stack),
				"error", panicErr,
			)
			// Drain stale commitItems already queued in commitC and fail their
			// nodes. The step worker's per-shard commitItem slots
			// (getCommitSlot) are reused with commitPending as the ownership
			// flag: leaving a queued item would let the restarted worker
			// overwrite that slot and enqueue the SAME pointer again, so the
			// commit worker would process it twice — double Peer.Commit, double
			// apply, and a double ReleaseCommittedEntries on the same pooled
			// slice. The commit worker may concurrently consume from commitC,
			// but a channel receive is exclusive, so each item is either drained
			// here or processed normally — never both.
			e.drainCommitC(sw.commitC)
			// Clear commitPending for the ONE node that was mid-enqueueForCommit
			// (or mid-commitMemoryMode) when the panic hit — its item never
			// reached commitC/applyC, so nothing else owns it. A blanket clear
			// over every owned node would race the commit worker's in-flight
			// batch, which legitimately holds commitPending across SaveState,
			// causing a double GetUpdate extraction and double apply.
			sw.clearStepCommitPending()
		}
	}()
	sw.run()
	return false
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
	})
}

// RequestSnapshotRecovery submits a snapshot recovery request for the
// given shard to the engine's snapshot pool. This is called after a
// remote snapshot has been received and persisted to disk and LogDB.
// The snapshot pool worker will read the snapshot from disk and apply
// it to the state machine via Snapshotter.RecoverFromSnapshot.
func (e *Engine) RequestSnapshotRecovery(shardID, replicaID uint64, node *Node) error {
	return e.snapshots.requestSnapshot(snapshotRequest{
		shardID:   shardID,
		replicaID: replicaID,
		node:      node,
		save:      false,
	})
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
