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

	"github.com/golang/snappy"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// defaultShardChanBuffer is the per-shard channel buffer size for the
// apply worker's fan-out pattern. Sized to absorb commit pipeline
// bursts (piggyback commits, batched SaveState flushes) without
// causing head-of-line blocking in the dispatcher. A slow shard
// filling its buffer blocks only the dispatcher's send to that shard,
// but with a larger buffer the window for backpressure is reduced.
//
// A shard has at most ONE applyItem in flight at a time (commitPending gates
// the step worker from producing a second update until the first has been
// applied), so in practice the per-shard channel never fills and the
// dispatcher never parks in a per-shard send. The blocking send in dispatch
// nonetheless services membershipC, so even if that invariant is ever
// weakened the unbuffered unload/reload rendezvous stays reachable.
const defaultShardChanBuffer = 64

// applyMembershipRequest asks the apply worker's dispatcher goroutine to
// tear down (unload) or re-admit (reload) a shard's apply resources.
//
// For an unload, done is closed only after the per-shard channel has been
// closed AND the per-shard apply goroutine (if one exists) has fully exited,
// so the requester can treat the unload as synchronous: once done is closed,
// no SM.Apply for the shard is or will be in flight. Reload requests carry a
// nil done — nothing waits on them.
//
// Unloads and reloads share ONE channel because select chooses uniformly at
// random among ready cases. With a separate unloadC and reloadC the two could
// be applied in reverse order, leaving a shard OUT of the unloaded set after
// it was unloaded; a stale applyItem would then spawn a per-shard goroutine
// nothing ever closes and apply entries into a state machine StopShard has
// already closed. A single channel makes the sequence FIFO by construction.
type applyMembershipRequest struct {
	shardID uint64
	unload  bool
	done    chan struct{}
}

// maxUnloadedShards bounds the number of retired shard IDs the commit and
// apply workers remember. The set exists to reject items that were already
// in flight when a shard was unloaded; that window closes as soon as the
// pipeline drains, so only recent unloads matter. Without a bound the set
// would grow for the process lifetime under shard churn, since an entry is
// removed only when the SAME shard ID is loaded again.
const maxUnloadedShards = 1024

// unloadedSet is a bounded FIFO set of unloaded shard IDs. It is owned by a
// single worker goroutine and is not safe for concurrent use. Adding beyond
// maxUnloadedShards retires the oldest entry.
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

// releaseApplyItem clears an applyItem's references and returns it to the
// pool. Clearing is required: a pooled item that still points at a Node keeps
// that node's Peer, LogReader, and state machine reachable long after the
// shard was unloaded, and a retained proto.Update keeps its entry slices
// alive.
func releaseApplyItem(pool *sync.Pool, item *applyItem) {
	item.node = nil
	item.update = proto.Update{}
	pool.Put(item)
}

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
	shardExits      map[uint64]chan struct{} // per-shard goroutine exit signals, closed on runShardApply return
	unloaded        *unloadedSet             // shards that have been unloaded; dispatch skips these
	wg              *sync.WaitGroup
	maxApplyWorkers int
	callback        Callback
	snapshotPool    *snapshotPool
	workSignal      *WorkSignal
	membershipC     chan applyMembershipRequest // ordered unload/reload requests handled by the dispatcher
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
		shardExits:      make(map[uint64]chan struct{}),
		unloaded:        newUnloadedSet(),
		wg:              wg,
		maxApplyWorkers: maxApplyWorkers,
		callback:        callback,
		snapshotPool:    snapshotPool,
		workSignal:      workSignal,
		// membershipC is deliberately unbuffered: requestUnload must rendezvous
		// with the dispatcher (or observe stopC) so an unload request can
		// never be stranded in a buffer after the dispatcher has exited,
		// which would leave the synchronous UnloadNode waiter blocked forever.
		// Reloads ride the same channel to keep the two ordered, so they
		// rendezvous too — see requestReload.
		membershipC: make(chan applyMembershipRequest),
		stopC:       stopC,
		metrics:     metrics,
	}
}

// run is the dispatcher loop. It reads items from applyC and routes
// each item to the owning per-shard goroutine, spawning one on demand.
// It also listens on membershipC for shard unload/reload requests from the
// Host goroutine, ensuring per-shard goroutines are cleaned up when nodes
// are unloaded.
func (w *applyWorker) run() {
	// Join the per-shard goroutines on EVERY exit path taken while the
	// engine is stopping, including a panic unwinding out of the dispatcher.
	// Engine.Stop must never return with an SM.Apply in flight: Host.Close
	// closes the state machines immediately afterwards.
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
			// Unload/reload requests are ordered ahead of any applyItem a
			// re-loaded shard can produce (LoadNode enqueues the reload
			// before publishing the node), but select picks uniformly at
			// random among ready cases. Service them first so a re-loaded
			// shard's first committed batch is not dropped as "unloaded".
			w.drainUnloadReload()
			w.dispatch(item)
		}
	}
}

// shutdownIfStopping closes every per-shard channel and waits for the
// per-shard apply goroutines to exit, then acks unload requests that raced
// shutdown. It is a no-op unless stopC is closed, so a panic-restart of the
// dispatcher keeps the per-shard channels — and the committed items buffered
// in them — intact. It is idempotent: shardChans is emptied as it goes.
func (w *applyWorker) shutdownIfStopping() {
	select {
	case <-w.stopC:
	default:
		return
	}
	for shardID, ch := range w.shardChans {
		close(ch)
		delete(w.shardChans, shardID)
	}
	w.wg.Wait()
	// All per-shard goroutines have exited, so closing done keeps the
	// synchronous-unload contract truthful; without the ack a concurrent
	// UnloadNode would block forever on its done channel.
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

// dispatch routes an applyItem to the per-shard goroutine, spawning
// one if the shard has no running goroutine. Items for shards that
// have been unloaded are discarded to prevent goroutine leaks: the
// commit worker may have enqueued an item before the unload was
// processed by the dispatcher, and creating a new per-shard channel
// for an unloaded shard would spawn a goroutine that can never exit.
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

	ch, ok := w.shardChans[shardID]
	if !ok {
		ch = make(chan *applyItem, defaultShardChanBuffer)
		w.shardChans[shardID] = ch
		// exited is closed when the per-shard goroutine returns; the
		// unload path joins on it so UnloadNode can guarantee no
		// SM.Apply is in flight after the unload completes.
		exited := make(chan struct{})
		w.shardExits[shardID] = exited
		w.wg.Add(1)
		go func() {
			defer close(exited)
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
		return
	default:
	}
	slog.Warn("apply dispatcher backpressure: shard channel full, blocking",
		"worker", w.workerID,
		"shard", shardID,
		"channel_cap", cap(ch),
	)
	// Block until the shard drains, BUT remain responsive to shutdown and to
	// membership requests: without the stopC guard a stalled/stopped shard
	// goroutine whose channel is full would wedge the dispatcher forever,
	// preventing the engine from tearing down; without the membershipC case
	// it would also stall UnloadNode and LoadNode for EVERY shard on this
	// worker, since both rendezvous with this goroutine. Committed entries
	// are still never dropped in normal operation — the abandon branches
	// below run only when the engine is stopping or this very shard was
	// unloaded underneath us.
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
		case <-w.stopC:
			slog.Warn("apply dispatcher shutting down; committed item not delivered",
				"worker", w.workerID, "shard", shardID)
			// Mirror the unloaded-shard cleanup: release committed entries,
			// clear commitPending, and return the item to the pool so we
			// don't leak pooled state or wedge the node on shutdown.
			w.discardItem(item)
			return
		}
	}
}

// discardItem releases a committed batch that will never be applied. It
// returns the pooled committed-entries slice, clears commitPending so the
// shard's pipeline does not stall waiting for an apply that will not happen,
// and returns the applyItem to the pool.
func (w *applyWorker) discardItem(item *applyItem) {
	item.update.ReleaseCommittedEntries()
	if item.node != nil {
		item.node.commitPending.Store(false)
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
//
//nolint:gocyclo // the per-shard apply loop: recovering/saving gates, the applyInFlight handshake, gap detection, snapshot coverage skipping and the auto-snapshot trigger are one ordered sequence of side effects; extracting any stage reorders them relative to the gates that guard them
func (w *applyWorker) runShardApply(ch <-chan *applyItem) {
	var results []sm.Result
	for item := range ch {
		node := item.node
		entries := item.update.CommittedEntries
		func() {
			// Mark an apply in flight BEFORE the recovering gate below. Paired
			// with the snapshot worker's set-recovering-then-drain, this lets
			// recovery prove no apply is mid-SM.Apply before it replaces SM
			// state. Registered first so it decrements LAST (after all work).
			node.applyInFlight.Add(1)
			defer node.applyInFlight.Add(-1)
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
				releaseApplyItem(w.applyPool, item)
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

			// An empty batch never touches the state machine, so it must
			// NOT wait on the recovering/saving gate below: parking it
			// would hold commitPending — and with it the shard's whole
			// pipeline — for the entire, unbounded duration of user
			// RecoverFromSnapshot/SaveSnapshot code, for a batch that has
			// nothing to apply. Memory mode forwards such batches (a hard
			// state change with no committed entries) on every update.
			if len(entries) == 0 {
				return
			}

			// If a snapshot recovery OR a snapshot-save capture is in
			// progress, WAIT for the gate to clear rather than dropping the
			// batch. Raft's processed marker already advanced past these
			// entries in Peer.Commit, so a dropped batch is never
			// re-delivered and the state machine would silently diverge.
			// The gate exists to avoid concurrent state machine access
			// between the snapshot-pool goroutine and this apply goroutine:
			// recovery replaces SM state; the save capture reads shared
			// in-memory state (the session manager, which is not safe for
			// concurrent use, and a regular in-memory SM).
			//
			// The Add(-1)/recheck/Add(+1) handshake pairs with the snapshot
			// worker's set-flag-then-drain protocol: either this apply's
			// in-flight registration is visible to WaitForApplyDrain, or
			// the flag is visible here and we back off, releasing the slot
			// so the drain can complete.
			for node.IsRecovering() || node.IsSaving() {
				node.applyInFlight.Add(-1)
				stopped := false
				select {
				case <-node.StopC():
					stopped = true
				default:
					time.Sleep(50 * time.Microsecond)
				}
				node.applyInFlight.Add(1)
				if stopped {
					// Shard is stopping. The entries are durable in the
					// WAL; restart re-derives the apply frontier from
					// LogDB state, so abandoning them here is safe.
					return
				}
			}

			// A completed recovery replaced SM state with a snapshot that
			// may already cover a prefix (or all) of this batch. Skip
			// covered entries: they are represented in the recovered state
			// and re-applying them would double-apply. The save gate never
			// moves lastApplied, so this filter is a no-op for it — and in
			// normal operation entries[0].Index == lastApplied+1.
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

			// The batch must continue exactly where the last one stopped.
			// A higher first index means an earlier batch was dropped (an
			// unloaded-shard skip, a shutdown drop, a failed Peer.Commit).
			// The missing range can never be re-offered, so applying this
			// batch would silently skip committed entries and keep the shard
			// serving reads from a divergent state machine. Fail the shard
			// instead.
			//
			// The check covers la == 0 as well. A zero applied marker is not
			// an "unknown frontier" that has to be trusted: node.lastApplied
			// is seeded to raft's processed marker before the node enters the
			// pipeline (Host.StartShard: SetProcessedIndex then
			// SeedLastApplied) and to the snapshot index by snapshot recovery,
			// so la == 0 means raft.processed == 0 and raft offers batches
			// starting at index 1. A first batch above index 1 with la == 0
			// therefore means entries were lost — exactly the drop this guard
			// exists to catch, and the only window the three drop paths
			// (unloaded skip, shutdown drop, failed Peer.Commit) were not
			// covered in.
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
				// Fail-fast: a decompression error means these committed
				// entries cannot be applied. Skipping them while advancing
				// lastApplied would silently diverge this state machine
				// from its peers, violating Raft's state-machine safety
				// property. Instead, fail pending proposals, mark the shard
				// failed (lastApplied is NOT advanced), and take it offline
				// until reload/snapshot recovery — mirroring the SM-panic
				// recovery path above.
				w.failApply(node, entries, err)
				return
			}

			// Acquire the semaphore to limit concurrent Apply calls and
			// release it via defer relative to the Apply call, so a
			// panicking user SM.Apply (recovered above) cannot leak the
			// slot. Leaking enough slots across distinct panicking shards
			// would deadlock all applies.
			func() {
				w.sem <- struct{}{}
				defer func() { <-w.sem }()
				applyStart := time.Now()
				err = node.sm.Apply(entries, results)
				applyElapsed := time.Since(applyStart)
				if w.metrics != nil {
					w.metrics.ObserveApplyLatency(node.shardID, applyElapsed)
				}
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
			// Update lastApplied (and its term) to the highest index in the
			// batch. The term is stored together with the index so the snapshot
			// pool can read a consistent (index, term) pair for the snapshot
			// point while applies are quiesced.
			lastEntry := &entries[len(entries)-1]
			lastIdx := lastEntry.Index
			node.lastApplied.Store(lastIdx)
			node.lastAppliedTerm.Store(lastEntry.Term)

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

// checkDecompressedSize rejects decompression bombs BEFORE any output buffer
// is allocated. raft.DecompressEntry enforces the limit only after
// snappy.Decode has already allocated the declared decoded length, so a
// crafted 4-byte header claiming gigabytes would be allocated first and
// rejected second. snappy.DecodedLen reads that declared length from the
// frame header without allocating. maxSize <= 0 disables the limit, matching
// raft.DecompressEntry.
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

// failApply handles a terminal apply or decompression error for a shard
// using fail-fast semantics. It fails the batch's pending proposals so
// callers observe the error immediately, marks the shard as failed, and
// reports the error through the node's error handler. lastApplied is
// deliberately NOT advanced: advancing it while skipping the batch would
// diverge this state machine from its peers, breaking Raft's
// state-machine safety property. The shard stays offline until it is
// reloaded or recovered from a snapshot.
func (w *applyWorker) failApply(node *Node, entries []proto.Entry, err error) {
	applyErr := &ApplyError{
		ShardID:   node.shardID,
		ReplicaID: node.replicaID,
		Err:       err,
	}
	if w.callback != nil {
		w.callback.OnProposalFailed(node.shardID, entries, applyErr)
	}
	node.failed.Store(true)
	node.handleError(applyErr)
}

// requestUnload sends a shard removal request to the dispatcher goroutine
// via the membership channel and returns a channel that is closed once the
// unload is COMPLETE: the per-shard channel closed and the per-shard apply
// goroutine (if any) fully exited. This is safe for concurrent use by the
// Host goroutine because the actual map mutation (removeShardChan) happens
// in the dispatcher goroutine's select loop, preserving the single-owner
// invariant on shardChans.
//
// When the engine is stopping (stopC closed), done is closed immediately:
// Engine.Stop owns the join in that case — it waits for the apply workers,
// which in turn wait for every per-shard goroutine, before returning.
func (w *applyWorker) requestUnload(shardID uint64) <-chan struct{} {
	done := make(chan struct{})
	select {
	case w.membershipC <- applyMembershipRequest{shardID: shardID, unload: true, done: done}:
	case <-w.stopC:
		close(done)
	}
	return done
}

// ackPendingUnloads closes the done channel of every unload request still
// pending on membershipC. Called by the dispatcher after wg.Wait() during
// shutdown, when all per-shard goroutines have already exited, so closing
// done keeps the synchronous-unload contract truthful. Reload requests
// parked on the same channel are discarded: the dispatcher is gone, so
// there is no unloaded set left to maintain.
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

// requestReload clears a shard from the unloaded set, allowing dispatch
// to create a new per-shard goroutine when the next applyItem arrives.
// This must be called when a previously unloaded shard is re-loaded to
// prevent the unloaded map from growing without bound and to allow the
// re-loaded shard to process committed entries.
//
// It rendezvouses with the dispatcher (or observes stopC), because the
// request shares the unbuffered membership channel with requestUnload so
// the two are applied in the order they were made. The wait is bounded by
// the dispatcher's liveness only: every dispatcher path either selects on
// membershipC or on stopC (dispatch's blocking per-shard send services
// membershipC for exactly this reason), so a slow state machine cannot
// stall a reload. Engine.LoadNode calls it only after Engine.Start, when a
// dispatcher goroutine exists to answer.
//
// Safe for concurrent use: the actual map deletion happens in the
// dispatcher goroutine's select loop.
func (w *applyWorker) requestReload(shardID uint64) {
	select {
	case w.membershipC <- applyMembershipRequest{shardID: shardID}:
	case <-w.stopC:
	}
}

// removeShardChan removes and closes the per-shard channel, adds the
// shard to the unloaded set, and returns the per-shard goroutine's exit
// channel (nil when no goroutine was ever spawned for the shard). Must
// only be called from the dispatcher goroutine to maintain the
// single-owner invariant on shardChans/shardExits.
//
// The unloaded set prevents dispatch() from re-creating a per-shard
// channel for this shard if a stale applyItem arrives from the commit
// worker after the unload has been processed. Without this guard, the
// new goroutine would leak because no subsequent removeShardChan call
// would ever close its channel.
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
