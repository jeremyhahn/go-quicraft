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
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// SealHold lets the commit worker PARK a shard whose log write was refused
// because the at-rest barrier is sealed, instead of failing it (#296). A
// sealed barrier is an operator state, not a disk fault: the WAL refused the
// record before writing it and rolled the batch back to its checkpoint, so
// nothing reached disk and the write can be retried, unchanged, once the
// barrier unseals.
//
// The engine does not know which errors mean "sealed", nor where a shard's
// barrier lives; the host does, and supplies both. A zero SealHold (the
// default) keeps the old behaviour: every SaveState failure fails the shard.
type SealHold struct {
	// IsSealed reports that err is EXACTLY the barrier's sealed refusal of a
	// write that left the log as it was before the batch. It must report
	// false for anything that also carries a disk fault — a failed fsync, a
	// poisoned log, a rollback that did not complete — since those are
	// permanent and still fail the shard.
	//
	// A classifier that accepts a batch's sealed refusal MUST accept every
	// single-update sealed refusal the same LogDB produces for that batch's
	// members: holdSealedBatch re-writes each member alone through saveOne,
	// which gives an error IsSealed rejects the full retry budget and then
	// fails the shard. A mismatch would spend backoffs on the commit worker
	// and latch a shard that unsealing would have released.
	IsSealed func(err error) bool

	// UnsealedC returns a channel closed when shardID's barrier is next
	// unsealed (already closed if it is unsealed now). Nil, or a nil return,
	// falls back to retrying every sealHoldPollInterval.
	UnsealedC func(shardID uint64) <-chan struct{}
}

// sealHoldPollInterval is how often a parked shard retries when its barrier
// offers no unseal notification. Each retry is one refused write.
const sealHoldPollInterval = time.Second

// heldShard is one parked shard on a commit worker. woken is set by its
// waiter goroutine; the worker's run loop then releases it.
type heldShard struct {
	node  *Node
	woken atomic.Bool
}

// SetSealHold installs the sealed-write hold on every commit worker. Like
// SetShardBarriers it must be called before Start.
func (e *Engine) SetSealHold(h SealHold) {
	for _, w := range e.commitWorkers {
		w.sealHold = h
	}
}

// isSealedSaveErr reports that a SaveState failure is the barrier's sealed
// refusal and takes the hold path rather than failShard.
func (w *commitWorker) isSealedSaveErr(err error) bool {
	return w.sealHold.IsSealed != nil && w.sealHold.IsSealed(err)
}

// holdSealedBatch settles a batch whose SaveState the sealed barrier refused,
// and reports whether it kept any update that is now durable and must carry
// on through the normal send/commit path.
//
// One SaveState covers every shard in the batch, and a refused batch is
// rolled back whole, so a refusal says only that SOME shard's barrier is
// sealed. With one barrier per host every shard is, but a multiplexing LogDB
// gives shards their own barriers, and one sealed shard must not stall the
// others on the worker. The batch is therefore re-written one update at a
// time: the updates that persist go on, the ones refused as sealed are
// parked, and any other failure fails that shard alone, as before.
func (w *commitWorker) holdSealedBatch() bool {
	if len(w.batch) == 1 {
		node, u := w.batchNodes[0], w.batch[0]
		w.batchNodes[0] = nil
		w.holdSealed(node, u, w.takeOrderedMsgs(u.ShardID))
		w.clearBatch()
		w.batch, w.batchNodes = w.batch[:0], w.batchNodes[:0]
		return false
	}
	// Write every update first, then settle the refused ones against the
	// intact batch (takeOrderedMsgs reads it), then compact.
	w.saveErrs = w.saveErrs[:0]
	for i := range w.batch {
		w.saveErrs = append(w.saveErrs, w.saveOne(w.logdbBuf[i:i+1]))
	}
	for i, err := range w.saveErrs {
		if err == nil {
			continue
		}
		node, u := w.batchNodes[i], w.batch[i]
		w.batchNodes[i] = nil
		// Either way the shard's ordered messages leave the batch: they
		// are owed to a write that did not happen.
		ordered := w.takeOrderedMsgs(u.ShardID)
		if w.isSealedSaveErr(err) {
			w.holdSealed(node, u, ordered)
		} else {
			w.failSaveState(node, u, err)
		}
	}
	keep := 0
	var zeroUpdate proto.Update
	for i := range w.batch {
		if w.saveErrs[i] == nil {
			w.batch[keep], w.batchNodes[keep] = w.batch[i], w.batchNodes[i]
			keep++
		}
	}
	for i := keep; i < len(w.batch); i++ {
		w.batch[i], w.batchNodes[i] = zeroUpdate, nil
	}
	clear(w.saveErrs)
	w.batch, w.batchNodes = w.batch[:keep], w.batchNodes[:keep]
	return keep > 0
}

// saveOne persists a single update with the batch path's retry schedule. A
// sealed refusal is not retried here: it is settled by the hold.
func (w *commitWorker) saveOne(u []logdb.Update) error {
	var err error
	for attempt := range saveStateMaxRetries {
		if err = w.logdb.SaveState(u); err == nil || w.isSealedSaveErr(err) {
			return err
		}
		if attempt < saveStateMaxRetries-1 {
			if cancelled := w.retryWait(attempt); cancelled {
				return err
			}
		}
	}
	return err
}

// takeOrderedMsgs removes and returns the ordered messages the batch holds
// for shardID. They are owed to state the refused write carried, so they
// must leave the batch before any other update's send. orderedMsgsBuf is
// keyed by shardID; when two incarnations of the shard share the batch their
// messages cannot be told apart, so they are dropped rather than requeued to
// either — a lost message costs a retransmit or an election timeout, never
// safety.
func (w *commitWorker) takeOrderedMsgs(shardID uint64) []proto.Message {
	msgs := w.orderedMsgsBuf[shardID]
	delete(w.orderedMsgsBuf, shardID)
	delete(w.commitTimes, shardID)
	n := 0
	for i := range w.batch {
		if w.batch[i].ShardID == shardID {
			n++
		}
	}
	if n > 1 {
		return nil
	}
	return msgs
}

// holdSealed parks node: its update u was refused by the sealed barrier, so
// it neither reached disk nor may be sent, acked or applied.
//
// Raft's view is put back exactly as GetUpdate found it — the hard state is
// marked unpersisted again, the ordered messages owed to it are requeued,
// and the entries, never marked stable, stay in the unstable log — and the
// shard is frozen (sealHeld) with commitPending still set, so the step
// worker neither ticks it, steps its inbox nor extracts another update.
// When the barrier unseals, releaseHeld clears both and the step worker
// re-derives the same update — same entries, same indexes and terms, since
// nothing moved raft in between — and writes it.
//
// The leader lease is invalidated: a frozen replica sends no heartbeats and
// may be deposed while it waits, so on release it must earn a fresh quorum
// of acks before serving a LeaseRead again.
//
// Proposals already in raft's log are neither failed nor acknowledged: they
// complete when the retried write commits, or their callers' deadlines
// expire.
func (w *commitWorker) holdSealed(node *Node, u proto.Update, ordered []proto.Message) {
	u.ReleaseCommittedEntries()
	if node.isUnloaded() {
		// A torn-down incarnation is dropped, as admit drops one.
		node.commitPending.Store(false)
		return
	}
	// Frozen before raft is rewound, so no tick lands on the rewound state.
	node.sealHeld.Store(true)
	if node.peer != nil {
		node.raftMu.Lock()
		node.peer.InvalidatePersistedState()
		node.peer.RequeueMessages(ordered)
		node.peer.InvalidateLease()
		node.raftMu.Unlock()
	}
	slog.Warn("log write refused: barrier sealed; shard held until unseal",
		"worker", w.workerID, "shard", u.ShardID, "replica", u.ReplicaID)
	h := &heldShard{node: node}
	w.held = append(w.held, h)
	var unsealC <-chan struct{}
	if w.sealHold.UnsealedC != nil {
		unsealC = w.sealHold.UnsealedC(node.shardID)
	}
	w.sealWaiters.Go(func() { w.awaitUnseal(h, unsealC) })
}

// awaitUnseal waits for the held shard's barrier to unseal, for the shard to
// stop, or for the worker to stop, then wakes the run loop. It never touches
// the node: the run loop owns the release.
//
// Engine stop leaks nothing: every waiter returns on stopC, and run joins them
// (sealWaiters) and then releases every held shard before the commit tier
// reports done.
func (w *commitWorker) awaitUnseal(h *heldShard, unsealC <-chan struct{}) {
	var poll <-chan time.Time
	if unsealC == nil {
		t := time.NewTimer(sealHoldPollInterval)
		defer t.Stop()
		poll = t.C
	}
	select {
	case <-unsealC:
	case <-poll:
	case <-h.node.StopC():
	case <-w.stopC:
	}
	h.woken.Store(true)
	// A send dropped because a wake is already pending loses nothing:
	// releaseWoken drains every woken entry in w.held, not one per wake.
	select {
	case w.sealWakeC <- struct{}{}:
	default:
	}
}

// releaseWoken releases every held shard whose waiter has fired.
func (w *commitWorker) releaseWoken() {
	keep := w.held[:0]
	for _, h := range w.held {
		if h.woken.Load() {
			releaseHeld(h.node, w.workSignal)
		} else {
			keep = append(keep, h)
		}
	}
	clear(w.held[len(keep):])
	w.held = keep
}

// releaseUnloaded releases every held incarnation of shardID. Called when the
// shard is unloaded, so UnloadNode's membership wait never returns with the
// shard still held.
func (w *commitWorker) releaseUnloaded(shardID uint64) {
	keep := w.held[:0]
	for _, h := range w.held {
		if h.node.shardID == shardID {
			releaseHeld(h.node, w.workSignal)
		} else {
			keep = append(keep, h)
		}
	}
	clear(w.held[len(keep):])
	w.held = keep
}

// releaseHeld unfreezes a held node and hands it back to the step worker,
// which re-derives the refused update and writes it again. sealHeld is
// cleared first so the step worker never sees commitPending clear on a
// frozen node.
func releaseHeld(node *Node, workSignal *WorkSignal) {
	node.sealHeld.Store(false)
	node.commitPending.Store(false)
	if workSignal != nil {
		workSignal.Notify(node.shardID)
	}
}
