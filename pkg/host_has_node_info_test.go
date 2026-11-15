package quicraft

import (
	"context"
	"testing"
	"time"
)

// hasNodeInfoPair names a shard/replica pair and the HasNodeInfo result a
// check expects for it.
type hasNodeInfoPair struct {
	shardID, replicaID uint64
	want               bool
}

// assertHasNodeInfo checks HasNodeInfo for every pair, naming the step.
func assertHasNodeInfo(t *testing.T, h *Host, step string, pairs ...hasNodeInfoPair) {
	t.Helper()
	for _, p := range pairs {
		if got := h.HasNodeInfo(p.shardID, p.replicaID); got != p.want {
			t.Errorf("%s: HasNodeInfo(%d, %d) = %v, want %v",
				step, p.shardID, p.replicaID, got, p.want)
		}
	}
}

// startAndCommit starts a single-replica shard, waits for its leader and
// commits one proposal, all within the deadline of ctx.
func startAndCommit(ctx context.Context, t *testing.T, h *Host, shardID, replicaID uint64) {
	t.Helper()
	members := map[uint64]string{replicaID: "127.0.0.1:5000"}
	create := func(_, _ uint64) interface{} { return &countingIdentitySM{} }
	if err := h.StartShard(members, false, create, testShardConfig(shardID, replicaID)); err != nil {
		t.Fatalf("StartShard(%d, %d): %v", shardID, replicaID, err)
	}
	waitForLeader(t, h, shardID)
	if _, err := h.SyncPropose(ctx, shardID, []byte("entry")); err != nil {
		t.Fatalf("SyncPropose on shard %d: %v", shardID, err)
	}
}

// TestHost_HasNodeInfo_BootstrapRestartAndRemoveData asserts that HasNodeInfo
// is true for a replica this Host bootstrapped, stays true across a restart
// on the same directories before the shard is started again, is false for
// any other shard/replica pair, reports each shard independently, and is
// false after RemoveData of the pair, including after a further restart.
func TestHost_HasNodeInfo_BootstrapRestartAndRemoveData(t *testing.T) {
	const deploymentID uint64 = 952
	walDir, nodeDir := t.TempDir(), t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := newDirLockTestHost(t, walDir, nodeDir, deploymentID)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			mustCloseHost(t, h)
		}
	}()

	// (a) The bootstrapped pair is reported; no other pair is.
	startAndCommit(ctx, t, h, 1, 1)
	assertHasNodeInfo(t, h, "after StartShard(1, 1)",
		hasNodeInfoPair{1, 1, true},
		hasNodeInfoPair{1, 2, false},
		hasNodeInfoPair{2, 1, false},
	)

	// (d) A second shard is reported independently of the first.
	startAndCommit(ctx, t, h, 2, 1)
	assertHasNodeInfo(t, h, "after StartShard(2, 1)",
		hasNodeInfoPair{1, 1, true},
		hasNodeInfoPair{2, 1, true},
		hasNodeInfoPair{2, 2, false},
		hasNodeInfoPair{3, 1, false},
	)

	// (b) After Close and a new Host on the same directories and identity,
	// the pairs are reported before any StartShard.
	mustCloseHost(t, h)
	closed = true
	assertHasNodeInfo(t, h, "on the closed Host", hasNodeInfoPair{1, 1, false})

	h, err = newDirLockTestHost(t, walDir, nodeDir, deploymentID)
	if err != nil {
		t.Fatalf("NewHost after restart: %v", err)
	}
	closed = false
	assertHasNodeInfo(t, h, "after restart, before StartShard",
		hasNodeInfoPair{1, 1, true},
		hasNodeInfoPair{2, 1, true},
		hasNodeInfoPair{1, 2, false},
	)

	// (c) After StopShard and SyncRemoveData the pair is no longer reported;
	// the other shard is unaffected.
	startAndCommit(ctx, t, h, 1, 1)
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard(1): %v", err)
	}
	if err := h.SyncRemoveData(ctx, 1, 1); err != nil {
		t.Fatalf("SyncRemoveData(1, 1): %v", err)
	}
	assertHasNodeInfo(t, h, "after SyncRemoveData(1, 1)",
		hasNodeInfoPair{1, 1, false},
		hasNodeInfoPair{2, 1, true},
	)

	// The removal persists across a further restart.
	mustCloseHost(t, h)
	closed = true
	h, err = newDirLockTestHost(t, walDir, nodeDir, deploymentID)
	if err != nil {
		t.Fatalf("NewHost after removal and restart: %v", err)
	}
	closed = false
	assertHasNodeInfo(t, h, "after removal and restart",
		hasNodeInfoPair{1, 1, false},
		hasNodeInfoPair{2, 1, true},
	)
}
