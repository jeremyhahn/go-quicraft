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
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
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
		config.Config{ShardID: 1, ReplicaID: 1}, nil, base, nil))
	e.LoadNode(NewNode(nil, nil, nil,
		config.Config{ShardID: 2, ReplicaID: 1}, nil, base, nil))

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
	got, err := e.SnapshotPool().barrierForShard(42)
	if err != nil {
		t.Fatalf("barrierForShard(42) before SetShardBarriers: %v", err)
	}
	if got != SnapshotBarrier(global) {
		t.Fatalf("barrierForShard(42) = %v before SetShardBarriers, want the process-wide barrier", got)
	}

	tenant := newTestBarrier(11)
	e.SetShardBarriers(func(shardID uint64) SnapshotBarrier {
		if shardID == 1 {
			return tenant
		}
		return nil
	})

	got, err = e.SnapshotPool().barrierForShard(1)
	if err != nil {
		t.Fatalf("barrierForShard(1): %v", err)
	}
	if got != SnapshotBarrier(tenant) {
		t.Fatalf("barrierForShard(1) = %v, want the tenant barrier", got)
	}
	got, err = e.SnapshotPool().barrierForShard(2)
	if err != nil {
		t.Fatalf("barrierForShard(2): %v", err)
	}
	if got != SnapshotBarrier(global) {
		t.Fatalf("barrierForShard(2) = %v, want fallback to the process-wide barrier", got)
	}
}

// TestEngine_ShardBarrierUnresolvedWithoutGlobalFailsClosed pins the fail-closed
// half of the resolver contract: with per-shard barriers configured and NO
// process-wide barrier, a shard the resolver does not know is refused rather
// than written in plaintext.
//
// The asymmetry this removes is the whole point. A SEALED barrier already fails
// closed on every path; an ABSENT one failing OPEN would make the one tenant
// whose shard the resolver missed — an async-provisioned tenant, a remapped
// shard ID, any map miss — the only shard on the host written in cleartext, while
// every other tenant's snapshot directory still showed encryption working.
func TestEngine_ShardBarrierUnresolvedWithoutGlobalFailsClosed(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	tenant := newTestBarrier(3)
	e.SetShardBarriers(func(shardID uint64) SnapshotBarrier {
		if shardID == 1 {
			return tenant
		}
		return nil
	})

	got, err := e.SnapshotPool().barrierForShard(1)
	if err != nil {
		t.Fatalf("barrierForShard(1): %v", err)
	}
	if got != SnapshotBarrier(tenant) {
		t.Fatalf("barrierForShard(1) = %v, want the tenant barrier", got)
	}

	got, err = e.SnapshotPool().barrierForShard(2)
	if !errors.Is(err, ErrShardBarrierUnresolved) {
		t.Fatalf("barrierForShard(2) = (%v, %v), want ErrShardBarrierUnresolved: "+
			"an unresolved shard with no process-wide barrier must not be written in plaintext",
			got, err)
	}
	if got != nil {
		t.Fatalf("barrierForShard(2) returned barrier %v alongside its error; "+
			"a caller that ignores the error must not receive a usable-looking nil-safe value", got)
	}
}

// TestSaveSnapshot_UnresolvedShardBarrierWritesNothing asserts the CONSEQUENCE
// of the refusal above: no snapshot directory, no artifact, and no LogDB record
// are produced for a shard whose barrier does not resolve.
func TestSaveSnapshot_UnresolvedShardBarrierWritesNothing(t *testing.T) {
	base := t.TempDir()
	stopC := make(chan struct{})
	ldb := newTestLogDB()
	pool := newSnapshotPool(1, ldb, nil, nil, NewWorkSignal(1), stopC, nil)
	pool.shardBarriers = func(uint64) SnapshotBarrier { return nil }

	cfg := config.Config{ShardID: 9, ReplicaID: 1}
	cfg.SetDefaults()
	// The state machine holds a recognisable secret, so a leaked artifact is
	// identifiable as such rather than merely present.
	secret := []byte("PLAINTEXT-USER-COMMAND-9")
	node := NewNode(nil, nil, nil, cfg, &largeSaveSnapshotter{data: secret}, base, nil)

	_, err := pool.saveSnapshot(snapshotRequest{
		shardID:   9,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if !errors.Is(err, ErrShardBarrierUnresolved) {
		t.Fatalf("saveSnapshot with an unresolved shard barrier = %v, want ErrShardBarrierUnresolved", err)
	}

	// The consequence: nothing on disk. A refusal that still wrote the artifact
	// would leave exactly the plaintext state machine it exists to prevent.
	if found := grepTreeForBytes(t, base, secret); found != "" {
		t.Fatalf("refused save wrote the plaintext state machine to %s", found)
	}
	shardBase := snapshotBaseDir(base, 9, 1)
	if entries, readErr := os.ReadDir(shardBase); readErr == nil && len(entries) > 0 {
		t.Fatalf("refused save left %d entries under %s; want none", len(entries), shardBase)
	}
	if snap, snapErr := ldb.GetSnapshot(9, 1); snapErr == nil && snap.Index != 0 {
		t.Fatalf("refused save recorded a LogDB snapshot at index %d", snap.Index)
	}
}

// grepTreeForBytes returns the path of the first file under root whose contents
// contain want, or "" when no file does. It is how the barrier tests assert the
// CONSEQUENCE — bytes on disk — rather than merely that a call returned an
// error.
func grepTreeForBytes(t *testing.T, root string, want []byte) string {
	t.Helper()
	var hit string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		// Every error here is fatal to the assertion, never a reason to move
		// on: this helper backs a claim that a secret is NOWHERE under root,
		// and a file the test could not read is a file it could not clear.
		// Swallowing the error would turn an I/O or permissions problem into a
		// silent pass of an at-rest-secrecy test.
		if err != nil {
			return err
		}
		if d.IsDir() || hit != "" {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(data, want) {
			hit = path
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return hit
}

// TestNode_SnapshotSaveDir_MatchesPoolLayout pins the shared path construction.
// An inbound-snapshot receiver writes to Node.SnapshotSaveDir; if that layout
// ever diverged from the pool's, received snapshots would land where GC and the
// DEK-purge-floor scan never look — they would leak, and a key rotation could
// purge a DEK such a snapshot still needs.
func TestNode_SnapshotSaveDir_MatchesPoolLayout(t *testing.T) {
	base := t.TempDir()
	node := NewNode(nil, nil, nil,
		config.Config{ShardID: 4, ReplicaID: 6}, nil, base, nil)

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
	sw.loadNode(NewNode(peer, nil, lr, cfg, nil, "", nil))

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
