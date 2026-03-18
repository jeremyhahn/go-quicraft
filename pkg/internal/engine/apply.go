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
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// defaultShardChanBuffer is the per-shard channel buffer size for the
// apply worker's fan-out pattern. Sized to absorb commit pipeline
// bursts (piggyback commits, batched SaveState flushes) without
// causing head-of-line blocking in the dispatcher. A slow shard
// filling its buffer blocks only the dispatcher's send to that shard,
// but with a larger buffer the window for backpressure is reduced.
const defaultShardChanBuffer = 64

// applyWorker applies committed entries to state machines using a
// fan-out pattern. A dispatcher goroutine reads from applyC and routes
// items to per-shard goroutines. A semaphore limits concurrent applies
// to maxApplyWorkers. Each shard's goroutine processes items sequentially
// to preserve Raft ordering guarantees.
//
// The semaphore is acquired per-Apply call (not per goroutine) so that
// idle shard goroutines do not hold semaphore slots. This prevents
// deadlocks when more shards exist than the semaphore capacity.
type applyWorker struct {
	workerID        int
	applyC          chan *applyItem
	applyPool       *sync.Pool
	sem             chan struct{}
	shardChans      map[uint64]chan *applyItem
	unloaded        map[uint64]struct{} // shards that have been unloaded; dispatch skips these
	wg              *sync.WaitGroup
	maxApplyWorkers int
	callback        Callback
	snapshotPool    *snapshotPool
	workSignal      *WorkSignal
	unloadC         chan uint64 // shard IDs to unload from the dispatcher goroutine
	reloadC         chan uint64 // shard IDs to re-load (clear from unloaded set)
	stopC           chan struct{}

	// metrics collects raft-level metrics. Nil when disabled.
	metrics RaftMetrics
}

// newApplyWorker creates an apply worker with the given concurrency limit.
// The snapshotPool parameter may be nil to disable auto-snapshot triggering.
func newApplyWorker(
	workerID int,
	applyC chan *applyItem,
	applyPool *sync.Pool,
	maxApplyWorkers int,
	wg *sync.WaitGroup,
	callback Callback,
	snapshotPool *snapshotPool,
	workSignal *WorkSignal,
	stopC chan struct{},
	metrics RaftMetrics,
) *applyWorker {
	return &applyWorker{
		workerID:        workerID,
		applyC:          applyC,
		applyPool:       applyPool,
		sem:             make(chan struct{}, maxApplyWorkers),
		shardChans:      make(map[uint64]chan *applyItem),
		unloaded:        make(map[uint64]struct{}),
		wg:              wg,
		maxApplyWorkers: maxApplyWorkers,
		callback:        callback,
		snapshotPool:    snapshotPool,
		workSignal:      workSignal,
		unloadC:         make(chan uint64, 64),
		reloadC:         make(chan uint64, 64),
		stopC:           stopC,
		metrics:         metrics,
	}
}

// run is the dispatcher loop. It reads items from applyC and routes
// each item to the owning per-shard goroutine, spawning one on demand.
// It also listens on unloadC for shard removal requests from the Host
// goroutine, ensuring per-shard goroutines are cleaned up when nodes
// are unloaded.
func (w *applyWorker) run() {
	for {
		select {
		case <-w.stopC:
			// Drain applyC before exiting. Committed entries must be
			// applied to deliver results and advance lastApplied.
			w.drain()
			// Close all per-shard channels so their goroutines exit.
			for shardID, ch := range w.shardChans {
				close(ch)
				delete(w.shardChans, shardID)
			}
			// Wait for all per-shard goroutines to finish their
			// current Apply call before returning. Without this,
			// Engine.Stop() could return while SM.Apply() is still
			// executing, causing use-after-close on the state machine.
			w.wg.Wait()
			return
		case shardID := <-w.unloadC:
			w.removeShardChan(shardID)
		case shardID := <-w.reloadC:
			delete(w.unloaded, shardID)
		case item := <-w.applyC:
			w.dispatch(item)
		}
	}
}

// dispatch routes an applyItem to the per-shard goroutine, spawning
// one if the shard has no running goroutine. Items for shards that
// have been unloaded are discarded to prevent goroutine leaks: the
// commit worker may have enqueued an item before the unload was
// processed by the dispatcher, and creating a new per-shard channel
// for an unloaded shard would spawn a goroutine that can never exit.
func (w *applyWorker) dispatch(item *applyItem) {
	shardID := item.update.ShardID

	if _, unloaded := w.unloaded[shardID]; unloaded {
		slog.Warn("apply dispatch skipped for unloaded shard",
			"worker", w.workerID,
			"shard", shardID,
		)
		// Release the item back to the pool and clear commitPending
		// so the pipeline does not stall waiting for this item.
		item.update.ReleaseCommittedEntries()
		if item.node != nil {
			item.node.commitPending.Store(false)
		}
		w.applyPool.Put(item)
		return
	}

	ch, ok := w.shardChans[shardID]
	if !ok {
		ch = make(chan *applyItem, defaultShardChanBuffer)
		w.shardChans[shardID] = ch
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.runShardApply(ch)
		}()
	}
	// Send to the per-shard channel with backpressure monitoring.
	// If the shard's channel is full (slow state machine Apply),
	// log the backpressure event before blocking. This makes
	// head-of-line blocking visible in logs without dropping
	// committed entries, which would violate Raft safety.
	select {
	case ch <- item:
	default:
		slog.Warn("apply dispatcher backpressure: shard channel full, blocking",
			"worker", w.workerID,
			"shard", shardID,
			"channel_cap", cap(ch),
		)
		ch <- item
	}
}

// drain non-blocking drains all items from applyC and dispatches them
// to per-shard goroutines.
func (w *applyWorker) drain() {
	for {
		select {
		case item := <-w.applyC:
			w.dispatch(item)
		default:
			return
		}
	}
}

// runShardApply processes items for a single shard sequentially.
// Sequential ordering is required by Raft: committed entries must be
// applied in log index order to maintain state machine correctness.
//
// The semaphore is acquired before each Apply call and released after,
// so idle goroutines do not hold semaphore slots.
func (w *applyWorker) runShardApply(ch <-chan *applyItem) {
	var results []sm.Result
	for item := range ch {
		node := item.node
		entries := item.update.CommittedEntries
		func() {
			defer func() {
				// Return pooled committed entries slice before releasing
				// the applyItem. The entries are no longer referenced
				// after apply and callback delivery complete.
				item.update.ReleaseCommittedEntries()
				// Release ownership back to step worker. Uses defer to
				// ensure commitPending is always cleared, even on panic.
				node.commitPending.Store(false)
				// Wake the step worker so it immediately re-processes
				// this shard. Without this notification, the step worker
				// would wait for the next tick, adding up to one tick
				// interval of latency to the commit/apply pipeline.
				w.workSignal.Notify(node.shardID)
				w.applyPool.Put(item)
			}()

			// Recover from panics in user state machine code (Apply,
			// decompression, etc). A panicking SM must not crash the
			// entire host process. The shard is marked as failed via
			// the circuit breaker so it becomes unavailable, but all
			// other shards continue operating normally.
			defer func() {
				if r := recover(); r != nil {
					stack := debug.Stack()
					slog.Error("state machine panic recovered",
						"shard", node.shardID,
						"replica", node.replicaID,
						"panic", fmt.Sprint(r),
						"stack", string(stack),
					)
					node.failed.Store(true)
					node.handleError(&SMPanicError{
						ShardID:   node.shardID,
						ReplicaID: node.replicaID,
						Value:     r,
						Stack:     stack,
					})
				}
			}()

			// If snapshot recovery is in progress, skip applying entries
			// to avoid concurrent state machine access between the
			// recovery goroutine and this apply goroutine. Since
			// lastApplied is NOT advanced, the step worker will
			// re-deliver these entries on the next cycle after
			// recovery completes and the recovering flag is cleared.
			if node.IsRecovering() {
				return
			}

			if len(entries) == 0 {
				return
			}
			// Grow pre-allocated results slice if needed.
			if cap(results) < len(entries) {
				results = make([]sm.Result, len(entries))
			}
			results = results[:len(entries)]

			// Decompress entry payloads before applying to the state
			// machine. The maxSize limit prevents decompression bomb
			// attacks from malicious entries in the log.
			maxDecompressSize := int(node.cfg.MaxEntrySize)
			if err := raft.DecompressEntries(entries, maxDecompressSize); err != nil {
				retries := node.applyRetries.Add(1)
				applyErr := &ApplyError{
					ShardID:   node.shardID,
					ReplicaID: node.replicaID,
					Err:       err,
				}
				// Fail pending proposals immediately so callers see
				// the apply error instead of waiting for a timeout.
				// The entries are consumed from the pipeline (the
				// commit worker already advanced processed), so they
				// will not be re-applied. We must advance lastApplied
				// to avoid a permanent gap.
				if w.callback != nil {
					w.callback.OnProposalFailed(node.shardID, entries, applyErr)
				}
				lastIdx := entries[len(entries)-1].Index
				node.lastApplied.Store(lastIdx)
				if retries >= node.maxApplyRetries {
					node.failed.Store(true)
					node.handleError(&CircuitBreakerError{
						ShardID:          node.shardID,
						ReplicaID:        node.replicaID,
						ConsecutiveFails: retries,
						LastErr:          err,
					})
				} else {
					node.handleError(applyErr)
				}
				return
			}

			// Acquire semaphore to limit concurrent Apply calls.
			w.sem <- struct{}{}
			applyStart := time.Now()
			err := node.sm.Apply(entries, results)
			applyElapsed := time.Since(applyStart)
			<-w.sem // release semaphore
			if w.metrics != nil {
				w.metrics.ObserveApplyLatency(node.shardID, applyElapsed)
			}
			slog.Debug("state machine apply completed",
				"shard", node.shardID,
				"replica", node.replicaID,
				"entries", len(entries),
				"apply_ns", applyElapsed.Nanoseconds(),
			)

			if err != nil {
				retries := node.applyRetries.Add(1)
				applyErr := &ApplyError{
					ShardID:   node.shardID,
					ReplicaID: node.replicaID,
					Err:       err,
				}
				// Fail pending proposals immediately so callers see
				// the apply error instead of waiting for a timeout.
				// The entries are consumed from the pipeline (the
				// commit worker already advanced processed), so they
				// will not be re-applied. We must advance lastApplied
				// to avoid a permanent gap.
				if w.callback != nil {
					w.callback.OnProposalFailed(node.shardID, entries, applyErr)
				}
				lastIdx := entries[len(entries)-1].Index
				node.lastApplied.Store(lastIdx)
				if retries >= node.maxApplyRetries {
					node.failed.Store(true)
					node.handleError(&CircuitBreakerError{
						ShardID:          node.shardID,
						ReplicaID:        node.replicaID,
						ConsecutiveFails: retries,
						LastErr:          err,
					})
				} else {
					node.handleError(applyErr)
				}
				return
			}
			// Notify callback before resetting circuit breaker state.
			// This allows the Host to complete pending proposals with
			// the apply results before any other state changes.
			if w.callback != nil {
				w.callback.OnApplied(node.shardID, entries, results[:len(entries)])
			}
			// Reset circuit breaker on success.
			node.applyRetries.Store(0)
			// Update lastApplied to the highest index in the batch.
			lastIdx := entries[len(entries)-1].Index
			node.lastApplied.Store(lastIdx)

			// Auto-snapshot trigger: if SnapshotEntries is configured and
			// enough entries have been applied since the last snapshot,
			// submit a snapshot request to the pool.
			if w.snapshotPool != nil && node.cfg.SnapshotEntries > 0 {
				lastSnap := node.lastSnapshotIndex.Load()
				if lastIdx-lastSnap >= node.cfg.SnapshotEntries {
					if node.snapshotting.CompareAndSwap(false, true) {
						// Capture appliedToTerm before dispatching to the
						// pool. commitPending is still true here (cleared
						// in the deferred function above), so the step
						// worker cannot be writing inMemory.appliedToTerm
						// concurrently. Reading it here is race-free.
						var term uint64
						if peer := node.Peer(); peer != nil {
							term = peer.AppliedToTerm()
						}
						if err := w.snapshotPool.requestSnapshot(snapshotRequest{
							shardID:       node.shardID,
							replicaID:     node.replicaID,
							node:          node,
							appliedToTerm: term,
							save:          true,
						}); err != nil {
							// Request rejected (pool full or duplicate).
							// Clear the flag so future triggers can retry.
							node.snapshotting.Store(false)
						}
					}
				}
			}
		}()
	}
}

// requestUnload sends a shard removal request to the dispatcher goroutine
// via the unloadC channel. This is safe for concurrent use by the Host
// goroutine because the actual map mutation (removeShardChan) happens in
// the dispatcher goroutine's select loop, preserving the single-owner
// invariant on shardChans.
func (w *applyWorker) requestUnload(shardID uint64) {
	select {
	case w.unloadC <- shardID:
	case <-w.stopC:
	}
}

// requestReload clears a shard from the unloaded set, allowing dispatch
// to create a new per-shard goroutine when the next applyItem arrives.
// This must be called when a previously unloaded shard is re-loaded to
// prevent the unloaded map from growing without bound and to allow the
// re-loaded shard to process committed entries.
//
// Safe for concurrent use: the actual map deletion happens in the
// dispatcher goroutine's select loop.
func (w *applyWorker) requestReload(shardID uint64) {
	select {
	case w.reloadC <- shardID:
	case <-w.stopC:
	}
}

// removeShardChan removes and closes the per-shard channel and adds the
// shard to the unloaded set. Must only be called from the dispatcher
// goroutine to maintain the single-owner invariant on shardChans.
//
// The unloaded set prevents dispatch() from re-creating a per-shard
// channel for this shard if a stale applyItem arrives from the commit
// worker after the unload has been processed. Without this guard, the
// new goroutine would leak because no subsequent removeShardChan call
// would ever close its channel.
func (w *applyWorker) removeShardChan(shardID uint64) {
	w.unloaded[shardID] = struct{}{}
	ch, ok := w.shardChans[shardID]
	if ok {
		close(ch)
		delete(w.shardChans, shardID)
	}
}
