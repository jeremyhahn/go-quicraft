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

	"github.com/golang/snappy"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// errApplyDispatcherPanic marks a batch lost to a dispatcher panic; the shard is failed.
var errApplyDispatcherPanic = errors.New(
	"engine: apply dispatcher panicked, committed batch discarded")

// errApplyItemDiscarded marks a batch dropped without applying (shard unloaded or engine stopping); its proposals complete with it.
var errApplyItemDiscarded = errors.New(
	"engine: committed batch discarded before apply (shard unloaded or engine stopping)")

// defaultShardChanBuffer is the per-shard apply channel buffer; commitPending keeps at most one item in flight per shard.
const defaultShardChanBuffer = 64

// applyMembershipRequest asks the dispatcher to unload or reload a shard; one FIFO channel keeps the order, and an unload's done closes after its goroutine exits.
type applyMembershipRequest struct {
	shardID uint64
	unload  bool
	done    chan struct{}
}

// maxUnloadedShards bounds the retired shard IDs the commit and apply workers remember.
const maxUnloadedShards = 1024

// unloadedSet is a bounded FIFO set of unloaded shard IDs, owned by one goroutine.
type unloadedSet struct {
	m     map[uint64]struct{}
	order []uint64
}

// newUnloadedSet creates an empty bounded set.
func newUnloadedSet() *unloadedSet {
	return &unloadedSet{m: make(map[uint64]struct{})}
}

// add marks a shard as unloaded, evicting the oldest entry when full.
func (s *unloadedSet) add(shardID uint64) {
	if _, ok := s.m[shardID]; ok {
		return
	}
	if len(s.order) >= maxUnloadedShards {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.m, oldest)
	}
	s.m[shardID] = struct{}{}
	s.order = append(s.order, shardID)
}

// remove clears a shard from the set, called when it is loaded again.
func (s *unloadedSet) remove(shardID uint64) {
	if _, ok := s.m[shardID]; !ok {
		return
	}
	delete(s.m, shardID)
	for i := range s.order {
		if s.order[i] == shardID {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// has reports whether the shard is currently marked unloaded.
func (s *unloadedSet) has(shardID uint64) bool {
	_, ok := s.m[shardID]
	return ok
}

// releaseApplyItem clears an applyItem's references, so a pooled item keeps no node alive, and returns it to the pool.
func releaseApplyItem(pool *sync.Pool, item *applyItem) {
	item.node = nil
	item.update = proto.Update{}
	pool.Put(item)
}

// applyWorker fans committed entries out to per-shard goroutines that apply in order; a per-Apply semaphore caps concurrency at maxApplyWorkers.
type applyWorker struct {
	workerID        int
	applyC          chan *applyItem
	applyPool       *sync.Pool
	sem             chan struct{}
	shardChans      map[uint64]chan *applyItem
	shardExits      map[uint64]chan struct{} // per-shard goroutine exit signals, closed on runShardApply return
	unloaded        *unloadedSet             // shards that have been unloaded; dispatch skips these
	wg              *sync.WaitGroup
	maxApplyWorkers int
	callback        Callback
	snapshotPool    *snapshotPool
	workSignal      *WorkSignal
	membershipC     chan applyMembershipRequest // ordered unload/reload requests handled by the dispatcher
	stopC           chan struct{}

	// joinedC closes once a stopping dispatcher has joined every per-shard goroutine; an unload during stop waits on it.
	joinedC  chan struct{}
	joinOnce sync.Once

	// inFlight is the item received but not yet handed off; a dispatcher panic discards it and fails the shard. Dispatcher-goroutine only.
	inFlight atomic.Pointer[applyItem]

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
		shardExits:      make(map[uint64]chan struct{}),
		unloaded:        newUnloadedSet(),
		wg:              wg,
		maxApplyWorkers: maxApplyWorkers,
		callback:        callback,
		snapshotPool:    snapshotPool,
		workSignal:      workSignal,
		// membershipC is unbuffered so an unload or reload always rendezvouses with the dispatcher, in order.
		membershipC: make(chan applyMembershipRequest),
		stopC:       stopC,
		joinedC:     make(chan struct{}),
		metrics:     metrics,
	}
}

// run is the dispatcher loop: it routes applyC items to per-shard goroutines and serves unload/reload requests.
func (w *applyWorker) run() {
	// Join the per-shard goroutines on every exit while stopping, so Stop never returns with an Apply in flight.
	defer w.shutdownIfStopping()
	for {
		select {
		case <-w.stopC:
			// Drain applyC before exiting. Committed entries must be
			// applied to deliver results and advance lastApplied.
			w.drain()
			return
		case req := <-w.membershipC:
			w.handleMembership(req)
		case item := <-w.applyC:
			// Track the item so a dispatcher panic before hand-off does not
			// strand it (commitPending set forever, shard wedged).
			w.inFlight.Store(item)
			// Serve membership requests first so a reloaded shard's first batch is not dropped as unloaded.
			w.drainUnloadReload()
			w.dispatch(item)
			w.inFlight.Store(nil)
		}
	}
}

// applyShutdownWarnAfter is how long the shutdown wait may run before it is reported.
const applyShutdownWarnAfter = 2 * time.Second

// shutdownIfStopping closes every per-shard channel, waits for the per-shard goroutines and acks unload requests that raced shutdown; a no-op unless stopC is closed.
func (w *applyWorker) shutdownIfStopping() {
	select {
	case <-w.stopC:
	default:
		return
	}
	closing := make([]uint64, 0, len(w.shardChans))
	for shardID, ch := range w.shardChans {
		close(ch)
		delete(w.shardChans, shardID)
		closing = append(closing, shardID)
	}
	// The wait is unbounded (a goroutine may be inside a user Apply); report it only once it has proven slow.
	waited := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(applyShutdownWarnAfter):
		slog.Warn("apply worker still waiting for in-flight SM.Apply calls; "+
			"Close blocks until every user state machine returns, and "+
			"ShutdownTimeout does not bound this wait",
			"worker", w.workerID,
			"shards", closing,
			"waited", applyShutdownWarnAfter,
		)
		<-waited
	}
	// All per-shard goroutines have exited, so closing done keeps the
	// synchronous-unload contract truthful; without the ack a concurrent
	// UnloadNode would block forever on its done channel.
	w.joinOnce.Do(func() { close(w.joinedC) })
	w.ackPendingUnloads()
}

// handleMembership applies one ordered unload/reload request. Must run on
// the dispatcher goroutine (single-owner invariant on shardChans/shardExits
// and the unloaded set).
func (w *applyWorker) handleMembership(req applyMembershipRequest) {
	if !req.unload {
		w.unloaded.remove(req.shardID)
		return
	}
	w.handleUnload(req)
}

// handleUnload tears down a shard's apply resources and acks the requester
// once the per-shard goroutine has exited. Must run on the dispatcher
// goroutine (single-owner invariant on shardChans/shardExits).
func (w *applyWorker) handleUnload(req applyMembershipRequest) {
	exited := w.removeShardChan(req.shardID)
	if exited == nil {
		// No per-shard goroutine was ever spawned: nothing to join.
		close(req.done)
		return
	}
	// Join the per-shard goroutine from a helper so a slow SM.Apply on the
	// unloading shard cannot head-of-line block the dispatcher (other shards
	// keep applying).
	go func(exited <-chan struct{}, done chan struct{}) {
		<-exited
		close(done)
	}(exited, req.done)
}

// drainUnloadReload non-blocking drains pending unload and reload requests.
// Called before dispatching so shard membership changes are always observed
// ahead of the items they govern, and in the order they were requested.
func (w *applyWorker) drainUnloadReload() {
	for {
		select {
		case req := <-w.membershipC:
			w.handleMembership(req)
		default:
			return
		}
	}
}

// dispatch routes an applyItem to its shard's goroutine, spawning one if needed; items for unloaded shards are discarded.
func (w *applyWorker) dispatch(item *applyItem) {
	shardID := item.update.ShardID

	if w.unloaded.has(shardID) {
		slog.Warn("apply dispatch skipped for unloaded shard",
			"worker", w.workerID,
			"shard", shardID,
		)
		// Release the item back to the pool and clear commitPending
		// so the pipeline does not stall waiting for this item.
		w.discardItem(item)
		return
	}

	// Incarnation guard: drop an item whose node is being unloaded, so it never reaches a closed or newer state machine.
	if item.node != nil && item.node.isUnloaded() {
		slog.Warn("apply dispatch skipped for stopped shard incarnation",
			"worker", w.workerID,
			"shard", shardID,
		)
		w.discardItem(item)
		return
	}

	ch, ok := w.shardChans[shardID]
	if !ok {
		ch = make(chan *applyItem, defaultShardChanBuffer)
		w.shardChans[shardID] = ch
		// exited is closed when the per-shard goroutine returns; the
		// unload path joins on it so UnloadNode can guarantee no
		// SM.Apply is in flight after the unload completes.
		exited := make(chan struct{})
		w.shardExits[shardID] = exited
		// Supervised so a runtime.Goexit in user code restarts the goroutine; exited closes only on the final exit.
		closeExited := sync.OnceFunc(func() { close(exited) })
		supervise.Supervise(w.wg, w.stopC,
			func() {
				w.runShardApply(ch)
				closeExited()
			},
			closeExited)
	}
	// Log backpressure before blocking on a full shard channel; committed entries are never dropped.
	select {
	case ch <- item:
		return
	default:
	}
	slog.Warn("apply dispatcher backpressure: shard channel full, blocking",
		"worker", w.workerID,
		"shard", shardID,
		"channel_cap", cap(ch),
	)
	// Block until the shard drains while still serving membership requests and noticing the shard goroutine's exit.
	exited := w.shardExits[shardID]
	for {
		select {
		case ch <- item:
			return
		case req := <-w.membershipC:
			w.handleMembership(req)
			if _, live := w.shardChans[shardID]; !live {
				// This shard was just unloaded: removeShardChan closed ch, so
				// retrying the send would panic, and the entries can never be
				// applied. Drop the item exactly as the unloaded guard does.
				slog.Warn("apply dispatch skipped for shard unloaded while backpressured",
					"worker", w.workerID, "shard", shardID)
				w.discardItem(item)
				return
			}
		case <-exited:
			slog.Warn("apply dispatch skipped: the shard's apply goroutine has exited",
				"worker", w.workerID, "shard", shardID)
			// Mirror the unloaded-shard cleanup: release committed entries,
			// clear commitPending, and return the item to the pool so we
			// don't leak pooled state or wedge the node.
			w.discardItem(item)
			return
		}
	}
}

// failInFlightItem fails the batch the dispatcher held when it panicked and marks the shard failed; called only from panic recovery.
func (w *applyWorker) failInFlightItem(item *applyItem) {
	node := item.node
	if node != nil {
		applyErr := &ApplyError{
			ShardID:   node.shardID,
			ReplicaID: node.replicaID,
			Err:       errApplyDispatcherPanic,
		}
		// The batch's proposals are failed before the pooled entries are
		// released below.
		failShard(node, w.callback, applyErr, func() {
			if w.callback != nil {
				w.callback.OnProposalFailed(node.shardID, item.update.CommittedEntries, applyErr)
			}
		})
		node.commitPending.Store(false)
	}
	item.update.ReleaseCommittedEntries()
	// applyPool is always set in production; guard only so the panic-recovery
	// path stays safe for unit tests that construct a worker without a pool.
	if w.applyPool != nil {
		releaseApplyItem(w.applyPool, item)
	}
}

// discardItem completes a never-applied batch's proposals with a typed error, clears commitPending and releases the item.
func (w *applyWorker) discardItem(item *applyItem) {
	node := item.node
	// Resolve the proposals before releasing the pooled entries.
	if node != nil && w.callback != nil {
		w.callback.OnProposalFailed(node.shardID, item.update.CommittedEntries,
			&ApplyError{
				ShardID:   node.shardID,
				ReplicaID: node.replicaID,
				Err:       errApplyItemDiscarded,
			})
	}
	item.update.ReleaseCommittedEntries()
	if node != nil {
		node.commitPending.Store(false)
		// No step-worker wakeup: every caller is terminal for the shard; clearing commitPending is what matters.
	}
	releaseApplyItem(w.applyPool, item)
}

// drain non-blocking drains all items from applyC and dispatches them
// to per-shard goroutines.
func (w *applyWorker) drain() {
	for {
		w.drainUnloadReload()
		select {
		case item := <-w.applyC:
			w.inFlight.Store(item)
			w.dispatch(item)
			w.inFlight.Store(nil)
		default:
			return
		}
	}
}

// runShardApply applies one shard's items in log order, holding the semaphore only around each Apply.
//
//nolint:gocyclo // the per-shard apply loop: recovering/saving gates, the applyInFlight handshake, gap detection, snapshot coverage skipping and the auto-snapshot trigger are one ordered sequence of side effects; extracting any stage reorders them relative to the gates that guard them
func (w *applyWorker) runShardApply(ch <-chan *applyItem) {
	var results []sm.Result
	for item := range ch {
		node := item.node
		entries := item.update.CommittedEntries
		func() {
			// Mark the apply in flight before the recovering gate, so snapshot recovery can wait for it to drain.
			node.applyInFlight.Add(1)
			defer node.applyInFlight.Add(-1)
			defer func() {
				// Return the pooled entries before releasing the applyItem.
				item.update.ReleaseCommittedEntries()
				// Release ownership back to step worker. Uses defer to
				// ensure commitPending is always cleared, even on panic.
				node.commitPending.Store(false)
				// Wake the step worker so it re-processes this shard without waiting a tick.
				w.workSignal.Notify(node.shardID)
				releaseApplyItem(w.applyPool, item)
			}()

			// A panic or Goexit in user state machine code fails this shard, not the process.
			normalReturn := false
			defer func() {
				if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
					stack := debug.Stack()
					// Settled first (below); the diagnostic is emitted last and
					// contained, so neither a slog handler nor the panic value's own
					// methods can skip the settlement.
					defer supervise.LogContained(func() {
						slog.Error("state machine panic recovered",
							"shard", node.shardID,
							"replica", node.replicaID,
							"panic", fmt.Sprint(r),
							"stack", string(stack),
						)
					})
					panicErr := &SMPanicError{
						ShardID:   node.shardID,
						ReplicaID: node.replicaID,
						Value:     r,
						Stack:     stack,
					}
					// Complete the batch's proposals before the outer defer releases the entries.
					failShard(node, w.callback, panicErr, func() {
						if w.callback != nil {
							w.callback.OnProposalFailed(node.shardID, entries, panicErr)
						}
					})
				}
			}()

			func() {
				// An empty batch never touches the state machine, so it skips the recovering/saving gate.
				if len(entries) == 0 {
					return
				}

				// Wait for a snapshot recovery, save capture or pause to clear rather than dropping the batch; the Add(-1)/recheck/Add(+1) handshake pairs with WaitForApplyDrain.
				for node.paused.Load() || node.IsRecovering() || node.IsSaving() {
					node.applyInFlight.Add(-1)
					stopped := false
					if node.paused.Load() {
						stopped = !node.awaitResume()
					} else {
						select {
						case <-node.StopC():
							stopped = true
						default:
							time.Sleep(50 * time.Microsecond)
						}
					}
					node.applyInFlight.Add(1)
					if stopped {
						// Shard is stopping. The entries are durable in the
						// WAL; restart re-derives the apply frontier from
						// LogDB state, so abandoning them here is safe.
						return
					}
				}

				// Skip entries a completed recovery's snapshot already covers.
				la := node.lastApplied.Load()
				if len(entries) > 0 && entries[0].Index <= la {
					skip := 0
					for skip < len(entries) && entries[skip].Index <= la {
						skip++
					}
					entries = entries[skip:]
				}

				if len(entries) == 0 {
					return
				}

				// A batch must start at lastApplied+1 (la == 0 included); a gap means entries were lost, so fail the shard.
				if entries[0].Index != la+1 {
					slog.Error("committed entry gap before apply",
						"shard", node.shardID,
						"replica", node.replicaID,
						"last_applied", la,
						"batch_first_index", entries[0].Index,
					)
					w.failApply(node, entries, ErrApplyGap)
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
				err := checkDecompressedSize(entries, maxDecompressSize)
				if err == nil {
					err = raft.DecompressEntries(entries, maxDecompressSize)
				}
				if err != nil {
					// A decompression error fails the shard without advancing lastApplied.
					w.failApply(node, entries, err)
					return
				}

				// applyElapsed is reported once the batch is applied and
				// recorded (below), outside the window in which the state
				// machine has applied it but lastApplied does not show it.
				var applyElapsed time.Duration
				// Release the semaphore by defer so a panicking Apply cannot leak a slot.
				func() {
					w.sem <- struct{}{}
					defer func() { <-w.sem }()
					applyStart := time.Now()
					err = node.sm.Apply(entries, results)
					applyElapsed = time.Since(applyStart)
					slog.Debug("state machine apply completed",
						"shard", node.shardID,
						"replica", node.replicaID,
						"entries", len(entries),
						"apply_ns", applyElapsed.Nanoseconds(),
					)
				}()

				if err != nil {
					// Fail-fast: an SM.Apply error is terminal. See the
					// decompression path above for the safety rationale.
					w.failApply(node, entries, err)
					return
				}
				// Notify callback before resetting circuit breaker state.
				// This allows the Host to complete pending proposals with
				// the apply results before any other state changes.
				if w.callback != nil {
					w.callback.OnApplied(node.shardID, entries, results[:len(entries)])
				}
				// Store lastApplied with its term so the snapshot pool reads a consistent pair.
				lastEntry := &entries[len(entries)-1]
				lastIdx := lastEntry.Index
				node.lastApplied.Store(lastIdx)
				node.lastAppliedTerm.Store(lastEntry.Term)
				if w.metrics != nil {
					w.metrics.ObserveApplyLatency(node.shardID, applyElapsed)
				}

				// Auto-snapshot trigger: if SnapshotEntries is configured and
				// enough entries have been applied since the last snapshot,
				// submit a snapshot request to the pool.
				if w.snapshotPool != nil && node.cfg.SnapshotEntries > 0 {
					lastSnap := node.lastSnapshotIndex.Load()
					if lastIdx-lastSnap >= node.cfg.SnapshotEntries {
						if node.snapshotting.CompareAndSwap(false, true) {
							// commitPending is still set, so reading appliedToTerm here is race-free.
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
			normalReturn = true
		}()
	}
}

// checkDecompressedSize rejects an oversized declared length before allocating; maxSize <= 0 disables the limit.
func checkDecompressedSize(entries []proto.Entry, maxSize int) error {
	if maxSize <= 0 {
		return nil
	}
	for i := range entries {
		if entries[i].Type&proto.EntryCompressFlag == 0 {
			continue
		}
		n, err := snappy.DecodedLen(entries[i].Cmd)
		if err != nil {
			return &raft.DecompressError{Err: err}
		}
		if n > maxSize {
			return raft.ErrDecompressionBomb
		}
	}
	return nil
}

// failApply fails the batch's proposals and the shard without advancing lastApplied; the shard stays offline until reload or snapshot recovery.
func (w *applyWorker) failApply(node *Node, entries []proto.Entry, err error) {
	applyErr := &ApplyError{
		ShardID:   node.shardID,
		ReplicaID: node.replicaID,
		Err:       err,
	}
	failShard(node, w.callback, applyErr, func() {
		if w.callback != nil {
			w.callback.OnProposalFailed(node.shardID, entries, applyErr)
		}
	})
}

// requestUnload asks the dispatcher to unload a shard and returns a channel closed when the unload is complete (joinedC while stopping).
func (w *applyWorker) requestUnload(shardID uint64) <-chan struct{} {
	done := make(chan struct{})
	select {
	case w.membershipC <- applyMembershipRequest{shardID: shardID, unload: true, done: done}:
	case <-w.stopC:
		// The engine is stopping, and its drain owns the per-shard goroutines.
		// Acknowledging now let a StopShard racing Host.Close close the state
		// machine while that drain was still inside its Apply.
		return w.joinedC
	}
	return done
}

// ackPendingUnloads closes the done channel of unload requests left on membershipC at shutdown and discards reloads.
func (w *applyWorker) ackPendingUnloads() {
	for {
		select {
		case req := <-w.membershipC:
			if req.done != nil {
				close(req.done)
			}
		default:
			return
		}
	}
}

// requestReload clears a shard from the unloaded set through the dispatcher, ordered with unloads; called only after Engine.Start.
func (w *applyWorker) requestReload(shardID uint64) {
	select {
	case w.membershipC <- applyMembershipRequest{shardID: shardID}:
	case <-w.stopC:
	}
}

// removeShardChan closes and removes the shard's channel, marks it unloaded and returns its goroutine's exit channel; dispatcher-goroutine only.
func (w *applyWorker) removeShardChan(shardID uint64) <-chan struct{} {
	w.unloaded.add(shardID)
	ch, ok := w.shardChans[shardID]
	if ok {
		close(ch)
		delete(w.shardChans, shardID)
	}
	exited := w.shardExits[shardID]
	delete(w.shardExits, shardID)
	return exited
}
