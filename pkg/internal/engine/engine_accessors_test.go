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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestEngine_GlobalSafePurgeFloor_SpansEveryLoadedShard pins the DEK-purge
// safety contract at the Engine boundary. The barrier is shared by all shards,
// so the floor must be the minimum over EVERY loaded shard's retained
// snapshots. If the node enumerator missed a shard, the caller would purge a
// DEK that shard's retained snapshot still needs and brick it.
func TestEngine_GlobalSafePurgeFloor_SpansEveryLoadedShard(t *testing.T) {
	base := t.TempDir()
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), newTestBarrier(9), nil, nil)

	// Nothing retained yet: the fail-safe contract says "do not purge".
	if floor, ok := e.GlobalSafePurgeFloor(); ok {
		t.Fatalf("GlobalSafePurgeFloor with no loaded shards = (%d, true), want ok=false", floor)
	}

	e.LoadNode(NewNode(nil, nil, nil,
		config.Config{ShardID: 1, ReplicaID: 1}, 0, nil, base, nil))
	e.LoadNode(NewNode(nil, nil, nil,
		config.Config{ShardID: 2, ReplicaID: 1}, 0, nil, base, nil))

	// Shard 2 holds the older epoch. A per-shard (rather than global) floor
	// would report 7 and purge epoch 4, destroying shard 2's snapshot.
	writeShardSnapshot(t, base, 1, 1, 100, 7)
	writeShardSnapshot(t, base, 2, 1, 100, 4)

	floor, ok := e.GlobalSafePurgeFloor()
	if !ok {
		t.Fatal("GlobalSafePurgeFloor reported ok=false with encrypted snapshots retained")
	}
	if floor != 4 {
		t.Fatalf("GlobalSafePurgeFloor = %d, want 4 (the minimum epoch across ALL loaded shards)", floor)
	}
}

// TestEngine_SetShardBarriers_RoutesPerShard pins the per-tenant barrier
// resolver contract: a shard with its own barrier is sealed under that barrier,
// and a nil resolver result falls back to the process-wide barrier rather than
// leaving the shard unencrypted.
func TestEngine_SetShardBarriers_RoutesPerShard(t *testing.T) {
	cfg := newTestHostConfig()
	global := newTestBarrier(5)
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), global, nil, nil)

	// Default (resolver never installed): every shard uses the process-wide
	// barrier, byte-for-byte the single-barrier behavior.
	if got := e.SnapshotPool().barrierForShard(42); got != SnapshotBarrier(global) {
		t.Fatalf("barrierForShard(42) = %v before SetShardBarriers, want the process-wide barrier", got)
	}

	tenant := newTestBarrier(11)
	e.SetShardBarriers(func(shardID uint64) SnapshotBarrier {
		if shardID == 1 {
			return tenant
		}
		return nil
	})

	if got := e.SnapshotPool().barrierForShard(1); got != SnapshotBarrier(tenant) {
		t.Fatalf("barrierForShard(1) = %v, want the tenant barrier", got)
	}
	if got := e.SnapshotPool().barrierForShard(2); got != SnapshotBarrier(global) {
		t.Fatalf("barrierForShard(2) = %v, want fallback to the process-wide barrier", got)
	}
}

// TestNode_SnapshotSaveDir_MatchesPoolLayout pins the shared path construction.
// An inbound-snapshot receiver writes to Node.SnapshotSaveDir; if that layout
// ever diverged from the pool's, received snapshots would land where GC and the
// DEK-purge-floor scan never look — they would leak, and a key rotation could
// purge a DEK such a snapshot still needs.
func TestNode_SnapshotSaveDir_MatchesPoolLayout(t *testing.T) {
	base := t.TempDir()
	node := NewNode(nil, nil, nil,
		config.Config{ShardID: 4, ReplicaID: 6}, 0, nil, base, nil)

	got := node.SnapshotSaveDir(77)
	want := snapshotSaveDir(base, 4, 6, 77)
	if got != want {
		t.Fatalf("Node.SnapshotSaveDir(77) = %q, want %q (the pool's own layout)", got, want)
	}

	// The purge-floor scan must actually find what was written there.
	writeShardSnapshot(t, base, 4, 6, 77, 3)
	dirs, err := listSnapshotDirs(node.SnapshotDir(), node.ShardID(), node.ReplicaID())
	if err != nil {
		t.Fatalf("listSnapshotDirs: %v", err)
	}
	if len(dirs) != 1 || dirs[0].path != want {
		t.Fatalf("purge-floor scan found %+v, want exactly %q", dirs, want)
	}
}

// TestUnloadedSet_EvictsOldestWhenFull pins the bound on the retired-shard set.
// Without the FIFO eviction the set grows for the process lifetime under shard
// churn (an entry is removed only when the SAME shard ID is loaded again).
func TestUnloadedSet_EvictsOldestWhenFull(t *testing.T) {
	s := newUnloadedSet()
	for i := range uint64(maxUnloadedShards) {
		s.add(i)
	}
	if len(s.m) != maxUnloadedShards || len(s.order) != maxUnloadedShards {
		t.Fatalf("set size = (%d, %d), want %d", len(s.m), len(s.order), maxUnloadedShards)
	}
	if !s.has(0) {
		t.Fatal("shard 0 missing before the set overflowed")
	}

	// One past the bound: the OLDEST entry must be retired, not the newest.
	s.add(uint64(maxUnloadedShards))
	if len(s.m) != maxUnloadedShards || len(s.order) != maxUnloadedShards {
		t.Fatalf("set grew past the bound: sizes = (%d, %d)", len(s.m), len(s.order))
	}
	if s.has(0) {
		t.Fatal("oldest entry (shard 0) was not evicted when the set overflowed")
	}
	if !s.has(1) {
		t.Fatal("second-oldest entry was evicted; eviction must be FIFO")
	}
	if !s.has(uint64(maxUnloadedShards)) {
		t.Fatal("newly added shard is missing after eviction")
	}

	// Re-adding an existing member is a no-op and must not evict anything.
	s.add(uint64(maxUnloadedShards))
	if len(s.order) != maxUnloadedShards {
		t.Fatalf("duplicate add changed the order list length to %d", len(s.order))
	}
	if !s.has(1) {
		t.Fatal("duplicate add evicted an entry")
	}
}

// TestStepWorker_DrainTick_ConsumesPendingTick pins the tick anti-starvation
// path. Under sustained proposal/transport load the work-signal case wins the
// select every time, so drainTick is the ONLY thing that keeps the raft logical
// clock advancing; without it the leader misses heartbeats and steps down via
// checkQuorum. The ticker channel is driven explicitly so the assertion does
// not depend on wall-clock timing.
func TestStepWorker_DrainTick_ConsumesPendingTick(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	stopC := make(chan struct{})
	defer close(stopC)
	sw := newStepWorker(0, time.Millisecond, sender, ws, make(chan *commitItem, 16), nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	sw.loadNode(NewNode(peer, nil, lr, cfg, 100, nil, "", nil))

	tickC := make(chan time.Time, 1)
	ticker := &time.Ticker{C: tickC}

	// No pending tick: drainTick must be a no-op, so the clock does not move.
	for range 40 {
		sw.drainTick(ticker)
	}
	if peer.IsLeader() {
		t.Fatal("drainTick advanced the raft clock with no pending tick")
	}

	// With a tick pending on every call the clock advances and the single-node
	// shard elects itself, proving the tick was actually delivered.
	for range 40 {
		tickC <- time.Now()
		sw.drainTick(ticker)
		if len(tickC) != 0 {
			t.Fatal("drainTick left a pending tick unconsumed")
		}
	}
	if !peer.IsLeader() {
		t.Fatal("drainTick did not advance the raft clock: heartbeat starvation is unguarded")
	}
}
