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
	"time"
)

// pausePollInterval paces PauseNode's wait for the shard's in-flight work to
// settle. A pause is an operator action, rare and not latency-critical.
const pausePollInterval = time.Millisecond

// IsPaused reports whether the replica is paused (Engine.PauseNode).
func (n *Node) IsPaused() bool {
	return n.paused.Load()
}

// pause marks the node paused. Pausing a paused node is a no-op: its
// resumeC, which a parked apply may be waiting on, is left untouched.
func (n *Node) pause() {
	n.pauseMu.Lock()
	defer n.pauseMu.Unlock()
	if n.paused.Load() {
		return
	}
	n.resumeC = make(chan struct{})
	n.paused.Store(true)
}

// resume clears the pause and releases a parked apply. It reports false if
// the node was not paused.
func (n *Node) resume() bool {
	n.pauseMu.Lock()
	defer n.pauseMu.Unlock()
	if !n.paused.Load() {
		return false
	}
	n.paused.Store(false)
	close(n.resumeC)
	n.resumeC = nil
	return true
}

// awaitResume parks the calling apply goroutine until the node is resumed.
// It reports false if the node stopped first. The caller has released its
// applyInFlight registration, so a pause or a recovery waiting on the drain
// is not held by the parked batch.
func (n *Node) awaitResume() bool {
	n.pauseMu.Lock()
	ch := n.resumeC
	n.pauseMu.Unlock()
	if ch == nil {
		return true
	}
	n.applyParked.Store(true)
	defer n.applyParked.Store(false)
	select {
	case <-ch:
		return true
	case <-n.stopC:
		return false
	}
}

// BeginSnapshotTransfer registers a snapshot send or receive of this shard
// that the host is about to run. It reports false — and registers nothing —
// if the replica is paused: a paused replica neither sends nor receives a
// snapshot. The registration comes before the check, so a PauseNode that
// set the flag first sees the transfer and waits for EndSnapshotTransfer.
func (n *Node) BeginSnapshotTransfer() bool {
	n.snapshotTransfers.Add(1)
	if n.paused.Load() {
		n.snapshotTransfers.Add(-1)
		return false
	}
	return true
}

// EndSnapshotTransfer ends a transfer BeginSnapshotTransfer admitted.
func (n *Node) EndSnapshotTransfer() {
	n.snapshotTransfers.Add(-1)
}

// pauseSettled reports whether no log, apply or snapshot work of the paused
// node is in flight: no pipeline item is being persisted (one that reached
// the apply gate is parked, one refused by a sealed barrier is held), no
// apply is running, and no snapshot save, recovery or transfer is running or
// queued.
func (n *Node) pauseSettled(pool *snapshotPool) bool {
	if n.commitPending.Load() && !n.applyParked.Load() && !n.sealHeld.Load() {
		return false
	}
	if n.applyInFlight.Load() != 0 || n.recovering.Load() || n.saving.Load() {
		return false
	}
	if n.snapshotTransfers.Load() != 0 {
		return false
	}
	return pool == nil || !pool.holdsSlot(n)
}

// holdsSlot reports whether a snapshot request of node is queued or running.
func (p *snapshotPool) holdsSlot(node *Node) bool {
	v, ok := p.inFlight.Load(node.shardID)
	if !ok {
		return false
	}
	owner, _ := v.(*Node)
	return owner == node
}

// refusePaused settles a snapshot request taken off the queue for a paused
// shard without running it. A recovery is re-armed on the redrive without
// counting an attempt, so a pause never exhausts recovery into a shard
// failure and the recovery runs after the resume. An automatic save is
// dropped and re-requested by ResumeNode; a requested one is answered
// ErrShardPaused.
func (p *snapshotPool) refusePaused(req snapshotRequest) {
	switch {
	case !req.save:
		req.node.redrive.deferred(time.Now())
	case req.requestKey == 0:
		req.node.pausedSaveDropped.Store(true)
	}
	p.releaseInFlight(req)
	req.node.snapshotting.Store(false)
	if p.callback != nil && req.save && req.requestKey != 0 {
		p.callback.OnSnapshotRequestCompleted(req.shardID, req.requestKey, 0, ErrShardPaused)
	}
}

// stepWorkerFor returns the step worker that owns shardID.
func (e *Engine) stepWorkerFor(shardID uint64) *stepWorker {
	return e.stepWorkers[int(shardID%uint64(len(e.stepWorkers)))]
}

// PauseNode stops node without unloading it. Once paused the replica
// neither ticks nor steps a message — it does not campaign, vote, replicate
// or acknowledge, and inbound messages are discarded — no committed entry
// reaches the state machine (a batch already handed to the apply goroutine
// parks before it), and no snapshot is taken, recovered, sent or received.
// Its log, state machine, membership and persisted raft state are untouched.
//
// PauseNode returns nil once none of that work is in flight, so nothing of
// the shard writes to the log or the snapshot store until ResumeNode. If
// ctx ends first it returns ctx's error and the node STAYS paused: the
// in-flight work settles on its own, and a later PauseNode waits for it
// again. Pausing a paused node waits the same way and is otherwise a no-op.
// A failed node is marked paused and returns at once.
//
// PauseNode is BeginPause followed by AwaitPause.
func (e *Engine) PauseNode(ctx context.Context, node *Node) error {
	e.BeginPause(node)
	return e.AwaitPause(ctx, node)
}

// BeginPause marks node paused and returns without waiting: from here on
// the step worker skips it and no new log, apply or snapshot work of the
// shard starts. A caller that serializes pause against ResumeNode holds its
// lock across this call only, not across AwaitPause.
func (e *Engine) BeginPause(node *Node) {
	node.pause()
}

// AwaitPause waits until none of the paused node's work is in flight, as
// PauseNode does. It returns nil at once if the node is no longer paused,
// resumed by ResumeNode while it waited.
func (e *Engine) AwaitPause(ctx context.Context, node *Node) error {
	if node.IsFailed() {
		return nil
	}
	// The step worker may be mid-iteration over this node, having read the
	// flag before it was set: wait for that iteration to end, so no update
	// is extracted or enqueued after the settled check below.
	if err := e.stepWorkerFor(node.shardID).awaitIteration(ctx); err != nil {
		return err
	}
	for {
		if !node.IsPaused() || node.pauseSettled(e.snapshots) || node.IsFailed() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-node.stopC:
			return ErrStopped
		case <-time.After(pausePollInterval):
		}
	}
}

// ResumeNode undoes PauseNode. The replica rejoins as a follower at its
// current term with a fresh election timeout (raft.Peer.RejoinAfterPause),
// the parked apply continues with the batches held, in log order, and an
// automatic snapshot the pause refused is requested again. A recovery the
// pause refused is re-driven by the step worker. Resuming a node that is not
// paused is a no-op.
func (e *Engine) ResumeNode(node *Node) {
	if !node.IsPaused() {
		return
	}
	w := e.stepWorkerFor(node.shardID)
	// Excluding the step worker and the commit worker's Peer.Commit while
	// raft is moved: the step worker skips a paused node, and a pause whose
	// ctx expired may not have waited for the iteration in progress.
	w.nodesMu.Lock()
	var term uint64
	if node.peer != nil && !node.IsFailed() {
		node.raftMu.Lock()
		node.peer.RejoinAfterPause()
		term = node.peer.AppliedToTerm()
		node.raftMu.Unlock()
	}
	resumed := node.resume()
	w.nodesMu.Unlock()
	if !resumed {
		return
	}
	if node.pausedSaveDropped.Swap(false) {
		e.requestDueSave(node, term)
	}
	e.workSignal.Notify(node.shardID)
}

// requestDueSave re-requests the automatic snapshot a pause refused, if it
// is still due, exactly as the apply goroutine's trigger would.
func (e *Engine) requestDueSave(node *Node, appliedToTerm uint64) {
	if e.snapshots == nil || node.cfg.SnapshotEntries == 0 || node.IsFailed() {
		return
	}
	if node.lastApplied.Load()-node.lastSnapshotIndex.Load() < node.cfg.SnapshotEntries {
		return
	}
	if !node.snapshotting.CompareAndSwap(false, true) {
		return
	}
	if err := e.snapshots.requestSnapshot(snapshotRequest{
		shardID:       node.shardID,
		replicaID:     node.replicaID,
		node:          node,
		appliedToTerm: appliedToTerm,
		save:          true,
	}); err != nil {
		node.snapshotting.Store(false)
	}
}

// awaitIteration waits until any step-worker iteration in progress has
// ended, bounded by ctx. Every iteration runs under nodesMu's read lock, so
// acquiring the write lock once is the barrier. On ctx expiry the acquiring
// goroutine finishes on its own when the iteration does.
func (w *stepWorker) awaitIteration(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.nodesMu.Lock()
		w.nodesMu.Unlock() //nolint:staticcheck // an empty critical section is the barrier
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
