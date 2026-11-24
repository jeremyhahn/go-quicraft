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

// The engine re-drives a runtime snapshot recovery rejected before restore began, failing the shard after maxRecoveryAttempts rejections in one holding episode.
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

// recoveryRedrive is a node's re-drive state for rejected snapshot recoveries: the pool records rejections, the step worker claims and re-queues. The zero value is ready to use.
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

// reject records a rejected attempt and returns the episode's count; at maxRecoveryAttempts it reports exhausted and arms nothing.
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

// deferred re-arms a claimed re-drive the pool could not admit, without consuming an attempt.
func (r *recoveryRedrive) deferred(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dueAt = now.Add(recoveryRedriveBackoff(max(r.rejected, 1)))
	r.armed.Store(true)
}

// claim disarms the owed re-drive; it must run before the request is published to the pool.
func (r *recoveryRedrive) claim() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armed.Store(false)
}

// reset forgets every rejected attempt.
func (r *recoveryRedrive) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rejected, r.dueAt = 0, time.Time{}
	r.armed.Store(false)
}

// adoptedSnapshotIndex returns the snapshot index the log adopted ahead of the state machine, and whether entries are held for it.
func adoptedSnapshotIndex(node *Node) (uint64, bool) {
	if node.Peer() == nil {
		return 0, false
	}
	_, processed := node.LogMarkers()
	return processed, processed > node.LastApplied()
}

// settleRejectedRecovery records a rejected recovery and reports exhaustion, having failed the shard with *SnapshotRecoveryExhaustedError; the caller holds the write lease.
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

// redriveRecovery re-queues a rejected recovery after its backoff, on the step worker; a busy slot or full pool queue is retried later without consuming an attempt.
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
