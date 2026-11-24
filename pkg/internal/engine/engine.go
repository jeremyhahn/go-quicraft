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

// RaftMetrics is the engine's metrics interface; nil disables collection.
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

// Engine runs the step, commit, apply and snapshot workers; shards map to workers by shardID % numWorkers.
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

	// listeners delivers user-visible notifications on a goroutine holding no engine lock, so a listener may call StopShard.
	listeners *listenerDispatcher

	// allNodes maps shardID to *Node for lock-free message delivery.
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

	// stopOnce makes Stop idempotent.
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

	// One tick per RTTMillisecond; 1ms if unset.
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
		// The forward's give-up guard watches applyStopC, which closes after commitStopC.
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

	// The node enumerator lets epoch purging compute a global safe-purge floor across shards.
	snapshots.nodes = e.loadedNodes

	return e
}

// loadedNodes returns every loaded node, for the global safe-purge floor scan.
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

// SetShardBarriers installs a per-shard barrier resolver; nil falls back to the process-wide barrier. Call before Start.
func (e *Engine) SetShardBarriers(fn func(shardID uint64) SnapshotBarrier) {
	e.snapshots.shardBarriers = fn
}

// SetPlaintextHostRoute declares the host route plaintext, so an unresolved shard takes it instead of failing; call before Start.
func (e *Engine) SetPlaintextHostRoute() {
	e.snapshots.plaintextHostRoute = true
}

// GlobalSafePurgeFloor returns the lowest epoch any retained snapshot or live WAL record needs; when ok is false, purge nothing.
func (e *Engine) GlobalSafePurgeFloor() (uint64, bool) {
	return e.snapshots.globalSafePurgeFloor()
}

// Start launches all worker goroutines. Each worker tier is tracked by
// its own WaitGroup so Stop() can perform ordered tier-by-tier shutdown.
func (e *Engine) Start() {
	e.started.Store(true)

	// Start the listener dispatcher first, so no notification runs inline on a worker.
	e.listeners.start(&e.listenerWg)

	// Start snapshot pool workers.
	e.snapshots.start(&e.snapshotWg)

	// Apply workers restart on panic or Goexit under supervise.Supervise.
	for _, aw := range e.applyWorkers {
		// The stopped path runs the dispatcher's stop path, user code
		// included, so it is resilient (supervise.RunResilient).
		supervise.Supervise(&e.applyWg, aw.stopC,
			func() { e.runApplyWorkerLoop(aw) },
			func() { supervise.RunResilient(func() { e.finishStoppedApplyWorker(aw) }) })
	}

	// Commit workers restart on panic, clearing commitPending on their nodes first.
	for _, cw := range e.commitWorkers {
		// A worker that exited abnormally while stopping still runs its shutdown drain, under the same recovery.
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

	// Clear the unloaded marks before publishing the node, so the new incarnation's first batch is not dropped; skipped before Start.
	e.commitWorkers[workerID].requestReload(node.shardID)
	if e.started.Load() {
		e.applyWorkers[workerID].requestReload(node.shardID)
	}

	// Route OnShardFailed through the dispatcher so a listener stopping the shard cannot deadlock.
	node.listeners = e.listeners

	e.stepWorkers[workerID].loadNode(node)

	e.allNodes.Store(node.shardID, node)
}

// UnloadNode removes and stops a node and waits until its apply goroutine has exited; never call it from an engine worker goroutine.
func (e *Engine) UnloadNode(shardID uint64) {
	workerID := int(shardID % uint64(len(e.stepWorkers)))

	// Stop the node first: closing stopC releases gated applies, snapshot operations and parked sends.
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

	// Wait until the commit worker skips this shard, so no stale SaveState can land after a restart.
	unloaded := e.commitWorkers[workerID].requestUnload(shardID)
	if e.started.Load() {
		e.commitWorkers[workerID].awaitMembership(unloaded)
	}

	// Unload the apply side and wait, without timeout, until the per-shard goroutine has exited; skipped before Start.
	if e.started.Load() {
		<-e.applyWorkers[workerID].requestUnload(shardID)
	}

	// Return this shard's share of the host-wide in-memory rate limiter, last and under raftMu.
	if node != nil && node.peer != nil {
		releaseGlobalRateLimitLocked(node)
	}

	e.allNodes.Delete(shardID)
}

// releaseGlobalRateLimitLocked releases the shard's rate-limit share with node.raftMu held, unlocking by defer.
func releaseGlobalRateLimitLocked(node *Node) {
	node.raftMu.Lock()
	defer node.raftMu.Unlock()
	node.peer.ReleaseGlobalRateLimit()
}

// DeliverMessage routes a wire-received message, stamped OriginRemote, to the shard's inbox; false if not loaded or full.
func (e *Engine) DeliverMessage(shardID uint64, msg proto.Message) bool {
	node := e.lookupNode(shardID, "DeliverMessage")
	if node == nil {
		return false
	}
	return node.Deliver(msg)
}

// DeliverLocal routes a process-local message, stamped OriginLocal, to the shard's inbox; false if not loaded or full.
func (e *Engine) DeliverLocal(shardID uint64, msg proto.Message) bool {
	node := e.lookupNode(shardID, "DeliverLocal")
	if node == nil {
		return false
	}
	return node.DeliverLocal(msg)
}

// DeliverLocalTo injects a process-local message into exactly node; a stopped node accepts nothing.
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

// Stop shuts down in order (nodes, step, commit, apply, snapshot, listener), draining each tier; it is idempotent.
func (e *Engine) Stop() {
	e.stopOnce.Do(e.stop)
}

// stop performs the ordered shutdown exactly once; see Stop.
func (e *Engine) stop() {
	// 1. Stop all loaded nodes, unblocking snapshot operations.
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

	// 6. Stop the listener dispatcher last, delivering what the tiers queued.
	e.listeners.stop()
	e.listenerWg.Wait()
}

// runApplyWorkerLoop restarts the apply dispatcher after a panic; on stop it runs the stop path once more to drain and join.
func (e *Engine) runApplyWorkerLoop(aw *applyWorker) {
	for e.runApplyWorkerOnce(aw) {
		if !supervise.AwaitRestart(aw.stopC) {
			e.finishStoppedApplyWorker(aw)
			return
		}
	}
}

// finishStoppedApplyWorker runs the stop path again unless the panicked run already joined.
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
			// Fail the node for the item the panicked dispatcher held, so its commitPending cannot wedge the shard.
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

// settleWorkerExit builds the *WorkerPanicError, runs settle with it, then logs the exit.
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

// runCommitWorkerLoop restarts the commit worker after a panic, unblocking the nodes of the lost batch.
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

// runStepWorkerLoop restarts the step worker after a panic, clearing commitPending for the node mid-enqueue.
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

// ListenerNotificationsDropped returns how many notifications were refused because the listener queue was full.
func (e *Engine) ListenerNotificationsDropped() uint64 {
	return e.listeners.droppedCount()
}

// DispatchListener runs fn on the listener dispatcher in FIFO order; false when the queue is full, inline once stopped.
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

	// RequestKey identifies the waiting requester; must be non-zero.
	RequestKey uint64
}

// RequestSnapshot submits a snapshot save for the shard; SnapshotDuplicateError or SnapshotPoolBusyError if refused.
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

// RequestSnapshotRecovery submits recovery of a received, persisted snapshot to the snapshot pool.
func (e *Engine) RequestSnapshotRecovery(shardID, replicaID uint64, node *Node) error {
	return e.snapshots.requestRecovery(shardID, replicaID, node)
}

// FailShard trips node's failure latch and reports it once, as the engine reports its own; it returns whether this call tripped it.
func (e *Engine) FailShard(node *Node, err error) bool {
	return failShard(node, e.callback, err, nil)
}

// SetSender replaces the sender on the engine and its workers; call before Start or while quiescent.
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
