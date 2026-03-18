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
		applyWorkers[i] = newApplyWorker(i, applyCs[i], applyPool, maxApplyWorkers, &sync.WaitGroup{}, callback, snapshots, workSignal, applyStopC, metrics)
	}

	return &Engine{
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

	// Start step workers with restart-on-panic. Same restart logic as
	// commit workers: on panic, clear commitPending on all owned nodes
	// to unblock them, then restart after a brief delay.
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
	e.stepWorkers[workerID].loadNode(node)

	// Clear the shard from the commit and apply workers' unloaded
	// sets so that committed entries for this shard are processed
	// normally. This is a no-op if the shard was never unloaded.
	e.commitWorkers[workerID].requestReload(node.shardID)
	e.applyWorkers[workerID].requestReload(node.shardID)

	e.allNodes.Store(node.shardID, node)
}

// UnloadNode removes a node from its worker partition and the
// concurrent-safe allNodes map. It also signals the apply worker to
// close the per-shard goroutine, preventing goroutine leaks.
func (e *Engine) UnloadNode(shardID uint64) {
	workerID := int(shardID % uint64(len(e.stepWorkers)))
	e.stepWorkers[workerID].unloadNode(shardID)

	// Signal the commit worker to skip Peer.Commit for this shard.
	// This prevents in-flight commitItems from calling Commit on a
	// torn-down node.
	e.commitWorkers[workerID].requestUnload(shardID)

	// Signal the apply worker to close the per-shard channel and
	// goroutine. This is sent through a channel so the actual cleanup
	// happens in the dispatcher goroutine's select loop, maintaining
	// the single-owner invariant on shardChans.
	e.applyWorkers[workerID].requestUnload(shardID)

	e.allNodes.Delete(shardID)
}

// DeliverMessage routes a Raft message to the target shard's node
// inbox for processing by the step worker. Returns true if the
// message was delivered, false if the shard is not loaded or the
// inbox is full.
//
// Safe for concurrent use by multiple goroutines (transport layer).
func (e *Engine) DeliverMessage(shardID uint64, msg proto.Message) bool {
	val, ok := e.allNodes.Load(shardID)
	if !ok {
		return false
	}
	node, nodeOK := val.(*Node)
	if !nodeOK {
		slog.Error("DeliverMessage type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "DeliverMessage node lookup",
				ExpectedType: "*Node",
				ActualValue:  val,
			})
		return false
	}
	return node.Deliver(msg)
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
		}
	}()
	aw.run()
	return false
}

// runCommitWorkerLoop runs a commit worker with automatic restart on
// panic. If the worker panics, the loop logs the panic with a full
// stack trace, clears commitPending on all nodes assigned to this
// worker to unblock them, and restarts the worker after a brief delay.
// The loop exits cleanly when the commit stop channel is closed.
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
			e.clearCommitPending(cw.workerID)
			// Drain stale commitItems that may already be in-flight
			// in the commitC channel. Without this, the restarted
			// worker would process a stale item whose node already
			// had commitPending cleared above, violating the
			// single-owner invariant on the Peer.
			e.drainCommitC(cw.commitC)
		}
	}()
	cw.run()
	return false
}

// drainCommitC non-blocking drains all items from the given commitC
// channel, clearing commitPending on each drained node. Called during
// panic recovery to discard stale in-flight commitItems that were
// enqueued before the panic. Without this drain, the restarted worker
// would pick up a stale item whose node already had commitPending
// cleared by clearCommitPending, causing a double-process race.
func (e *Engine) drainCommitC(commitC chan *commitItem) {
	for {
		select {
		case item := <-commitC:
			item.node.commitPending.Store(false)
			// Wake the step worker so it immediately re-processes
			// this shard. Without this notification, the step worker
			// would wait for the next tick interval before seeing
			// that commitPending was cleared, adding unnecessary
			// latency to the pipeline recovery after a panic.
			e.workSignal.Notify(item.node.shardID)
		default:
			return
		}
	}
}

// clearCommitPending iterates all nodes in the engine and clears the
// commitPending flag on nodes assigned to the given worker ID. This
// unblocks nodes that were waiting for the commit worker to process
// their updates when the worker panicked. Without this, nodes would
// remain stuck with commitPending=true indefinitely, preventing the
// step worker from producing new updates for those shards.
func (e *Engine) clearCommitPending(workerID int) {
	numWorkers := len(e.stepWorkers)
	e.allNodes.Range(func(key, val any) bool {
		node, ok := val.(*Node)
		if !ok {
			return true
		}
		shardID, shardOK := key.(uint64)
		if !shardOK {
			return true
		}
		if int(shardID%uint64(numWorkers)) == workerID {
			node.commitPending.Store(false)
		}
		return true
	})
}

// runStepWorkerLoop runs a step worker with automatic restart on panic.
// If the worker panics, the loop logs the panic with a full stack
// trace, clears commitPending on all nodes owned by this worker to
// unblock them, and restarts the worker after a brief delay. The loop
// exits cleanly when the step stop channel is closed.
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
// The appliedToTerm is captured under raftMu to prevent a data race
// with the step worker, which writes inMemory.appliedToTerm during
// Peer.Commit(). The snapshot pool worker receives the captured value
// in the request instead of reading it from the Peer directly.
//
// The opt parameter carries per-request snapshot options. A zero-value
// SnapshotRequestOption uses shard-level defaults.
func (e *Engine) RequestSnapshot(shardID, replicaID uint64, node *Node, opt SnapshotRequestOption) error {
	var term uint64
	if peer := node.Peer(); peer != nil {
		node.raftMu.Lock()
		term = peer.AppliedToTerm()
		node.raftMu.Unlock()
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
