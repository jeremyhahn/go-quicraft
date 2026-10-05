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
	"sync"
	"sync/atomic"
	"time"
)

// A runtime snapshot recovery rejected before the state machine restore began
// leaves the replica holding every committed entry above the snapshot its Raft
// log adopted (Node.snapshotAdoptionPending): it stays in the protocol, can
// vote and even lead, but applies nothing. Nothing outside the engine re-queues
// that recovery — the leader saw the snapshot acknowledged — so the engine
// re-drives it, and bounds the re-drive: after maxRecoveryAttempts consecutive
// rejections over one continuous episode of holding entries the shard is
// failed through the ordinary latch, so a replica cannot hold entries, and as
// leader commit without applying, for longer than the backoff schedule plus the
// attempts themselves. The count spans the episode whatever snapshot each
// attempt was made against: a newer snapshot adopted mid-episode does not grant
// fresh attempts, or a persistent fault plus a stream of newer snapshots would
// hold entries indefinitely.
const (
	// maxRecoveryAttempts counts every attempt in one holding episode, the
	// recovery the snapshot receive queued included. The last rejection fails
	// the shard instead of re-driving.
	maxRecoveryAttempts = 5
	// recoveryRedriveBaseBackoff is the wait before the first re-drive; each
	// later one doubles it, up to recoveryRedriveMaxBackoff.
	recoveryRedriveBaseBackoff = 100 * time.Millisecond
	recoveryRedriveMaxBackoff  = 5 * time.Second
)

// recoveryRedriveBackoff returns the wait before re-driving a recovery whose
// rejected attempts so far number rejected (1 or more).
func recoveryRedriveBackoff(rejected int) time.Duration {
	backoff := recoveryRedriveBaseBackoff
	for i := 1; i < rejected && backoff < recoveryRedriveMaxBackoff; i++ {
		backoff *= 2
	}
	return min(backoff, recoveryRedriveMaxBackoff)
}

// recoveryRedrive is a node's re-drive state for rejected snapshot recoveries.
// The snapshot pool records each rejection (reject) and clears the state on a
// recovery that completes, or once the node is no longer holding entries
// (reset); the step worker, which alone can tell whether the node is still
// holding entries, claims the attempt and re-queues the recovery once the
// backoff has elapsed (due, claim), and re-arms after another backoff when the
// pool cannot admit it (deferred).
// It lives on the Node, so a restarted shard — a new Node — starts with none.
// The zero value is ready to use.
type recoveryRedrive struct {
	mu sync.Mutex
	// rejected counts the attempts rejected in the current holding episode,
	// whatever snapshot index each was made against.
	rejected int
	// armed is set while a re-drive is owed, and dueAt is when it may run.
	// armed is written under mu but read without it by pending, which the
	// step worker calls for every node on every cycle.
	armed atomic.Bool
	dueAt time.Time
}

// reject records a rejected attempt and reports how many have been rejected in
// the current holding episode. While that is below maxRecoveryAttempts it arms
// a re-drive after the backoff; at the limit it reports exhausted and arms
// nothing, and the caller fails the shard.
func (r *recoveryRedrive) reject(now time.Time) (rejected int, exhausted bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rejected++
	if r.rejected >= maxRecoveryAttempts {
		r.armed.Store(false)
		return r.rejected, true
	}
	r.dueAt = now.Add(recoveryRedriveBackoff(r.rejected))
	r.armed.Store(true)
	return r.rejected, false
}

// due reports whether a re-drive is owed and its backoff has elapsed.
func (r *recoveryRedrive) due(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.armed.Load() && !now.Before(r.dueAt)
}

// pending reports whether a re-drive is owed, elapsed or not. It takes no
// lock, so the step worker's per-cycle check costs one atomic load.
func (r *recoveryRedrive) pending() bool {
	return r.armed.Load()
}

// deferred re-arms a claimed re-drive the snapshot pool could not admit (its
// request queue was full), after the backoff the current count calls for. The
// refused admission was not an attempt — nothing ran — so it consumes none,
// and retrying it on a later step cycle instead of every one keeps a pool that
// stays busy from being polled at the step worker's rate.
func (r *recoveryRedrive) deferred(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dueAt = now.Add(recoveryRedriveBackoff(max(r.rejected, 1)))
	r.armed.Store(true)
}

// claim disarms the owed re-drive for the attempt about to be queued. It MUST
// run before the request is published to the pool: from the publish on, a
// worker may run and reject the attempt at once, and reject re-arms the next
// re-drive — a disarm after the publish could land on that newer state and
// leave the node holding entries with nothing owed. The count is kept: the
// queued attempt's own outcome decides what happens next, and an admission
// the pool refuses re-arms through deferred.
func (r *recoveryRedrive) claim() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armed.Store(false)
}

// reset forgets every rejected attempt: a recovery completed, or the node is
// no longer holding entries for one.
func (r *recoveryRedrive) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rejected, r.dueAt = 0, time.Time{}
	r.armed.Store(false)
}

// adoptedSnapshotIndex returns the index of the snapshot a node's Raft log has
// adopted ahead of its state machine, and whether it is holding entries for
// one at all. It reads the published log markers, so it is safe from the
// snapshot pool's goroutine: while entries are held the processed marker sits
// at the adopted snapshot's index, because extraction is what advances it.
func adoptedSnapshotIndex(node *Node) (uint64, bool) {
	if node.Peer() == nil {
		return 0, false
	}
	_, processed := node.LogMarkers()
	return processed, processed > node.LastApplied()
}

// settleRejectedRecovery records a recovery rejected before the state machine
// restore began with cause, and reports whether the rejection exhausted the
// attempts, in which case it has failed the shard with a
// *SnapshotRecoveryExhaustedError wrapping cause and naming the snapshot the
// last attempt was made against, and the caller must not also report the
// rejection. A node no longer holding entries for an adopted
// snapshot owes no recovery, so its state is cleared and nothing is re-driven.
// The caller holds the node's write lease, so a failure cannot land on a later
// incarnation of the shard.
func (p *snapshotPool) settleRejectedRecovery(req snapshotRequest, cause error) (exhausted bool) {
	index, holding := adoptedSnapshotIndex(req.node)
	if !holding {
		req.node.redrive.reset()
		return false
	}
	rejected, exhausted := req.node.redrive.reject(time.Now())
	if !exhausted {
		return false
	}
	err := &SnapshotRecoveryExhaustedError{
		ShardID:       req.shardID,
		ReplicaID:     req.replicaID,
		SnapshotIndex: index,
		Attempts:      rejected,
		Err:           cause,
	}
	failShard(req.node, p.callback, err, nil)
	return true
}

// redriveRecovery re-queues a rejected snapshot recovery for node once its
// backoff has elapsed, through the same request path a snapshot receive uses.
// It runs on the step worker with exclusive Peer access, which is what lets it
// read snapshotAdoptionPending: a node that is no longer holding entries owes
// no recovery and has its re-drive state cleared. A node whose snapshot slot
// is busy (a save, or a recovery already queued) is retried on a later cycle.
// A re-drive the pool cannot admit (its request queue is full) is re-armed
// after a backoff, consumes no attempt and reports nothing: it is scheduling,
// not an attempt, and publishing a rejection for each retry would flood the
// listener queue a terminal event must get through. Each real attempt is
// reported by its own outcome. It starts no goroutine and no timer: the step
// worker's own cycles, which run on every tick, are the clock, so a node that
// stops simply stops being visited.
func (w *stepWorker) redriveRecovery(node *Node) {
	if w.snapshots == nil || !node.redrive.pending() {
		return
	}
	if !node.snapshotAdoptionPending() {
		node.redrive.reset()
		return
	}
	if node.IsStopped() || node.IsRecovering() || !node.redrive.due(time.Now()) {
		return
	}
	if !node.TryStartSnapshot() {
		return
	}
	// Claimed before the publish; see claim.
	node.redrive.claim()
	if err := w.snapshots.submit(snapshotRequest{
		shardID:   node.shardID,
		replicaID: node.replicaID,
		node:      node,
	}); err != nil {
		node.ClearSnapshotting()
		node.redrive.deferred(time.Now())
	}
}
