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

//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestIntegration_DB_SyncIOAPIs verifies that the sync propose/read
// round-trip works through the adapter. Matches dragonboat's
// TestNodeHostSyncIOAPIs.
func TestIntegration_DB_SyncIOAPIs(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session := dbClient.NewNoOPSession(1, nil)

	// Propose.
	cmd := encodeKVPut("sync-key", "sync-value")
	result, err := leaderHost.SyncPropose(ctx, session, cmd)
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}
	if result.Value != uint64(len("sync-value")) {
		t.Errorf("result.Value = %d, want %d", result.Value, len("sync-value"))
	}

	// Read.
	readResult, readErr := leaderHost.SyncRead(ctx, 1, encodeKVGet("sync-key"))
	if readErr != nil {
		t.Fatalf("SyncRead failed: %v", readErr)
	}
	val, ok := readResult.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", readResult)
	}
	if val != "sync-value" {
		t.Fatalf("SyncRead = %q, want %q", val, "sync-value")
	}
	t.Logf("sync IO round-trip verified: propose + read")
}

// TestIntegration_DB_ConcurrentSMLookup verifies that a concurrent
// state machine correctly handles concurrent reads during writes.
// Matches dragonboat's TestConcurrentStateMachineLookup.
func TestIntegration_DB_ConcurrentSMLookup(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	concTrackers := cluster.startConcurrentShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("concurrent SM leader at node[%d]", leaderIdx)

	leaderHost := cluster.hosts[leaderIdx]
	session := dbClient.NewNoOPSession(1, nil)

	// Warmup entry.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err := leaderHost.SyncPropose(ctx, session, encodeKVPut("warmup", "ok"))
	if err != nil {
		t.Fatalf("warmup failed: %v", err)
	}
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || concTrackers[i] == nil {
			continue
		}
		sm := concTrackers[i].Get(1, uint64(i+1))
		if sm == nil {
			continue
		}
		waitForCondition(t, defaultTimeout, fmt.Sprintf("node[%d] warmup", i), func() bool {
			_, ok := sm.Get("warmup")
			return ok
		})
	}

	// Propose 10 entries.
	for i := range 10 {
		key := fmt.Sprintf("conc-lookup-%03d", i)
		value := fmt.Sprintf("val-%03d", i)
		cmd := encodeKVPut(key, value)
		propCtx, propCancel := context.WithTimeout(context.Background(), defaultTimeout)
		_, propErr := leaderHost.SyncPropose(propCtx, session, cmd)
		propCancel()
		if propErr != nil {
			t.Fatalf("SyncPropose(%q) failed: %v", key, propErr)
		}
	}

	// Verify via SyncRead (exercises the Lookup path on concurrent SM).
	readCtx, readCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer readCancel()
	result, readErr := leaderHost.SyncRead(readCtx, 1, encodeKVGet("conc-lookup-005"))
	if readErr != nil {
		t.Fatalf("SyncRead failed: %v", readErr)
	}
	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", result)
	}
	if val != "val-005" {
		t.Fatalf("SyncRead = %q, want %q", val, "val-005")
	}
	t.Logf("concurrent SM lookup verified")
}

// TestIntegration_DB_LeaderInfoIsReported verifies that GetNodeHostInfo
// correctly reports the leader for each shard. Matches dragonboat's
// TestLeaderInfoIsReported.
func TestIntegration_DB_LeaderInfoIsReported(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]
	info := leaderHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})

	if len(info.ShardInfoList) == 0 {
		t.Fatal("ShardInfoList should not be empty")
	}

	foundShard := false
	for _, si := range info.ShardInfoList {
		if si.ShardID == 1 {
			foundShard = true
			if si.LeaderID == 0 {
				t.Error("LeaderID should be non-zero for an active shard")
			}
			if !si.IsLeader {
				t.Error("IsLeader should be true on the leader node")
			}
			t.Logf("shard 1 info: LeaderID=%d, IsLeader=%v, Term=%d",
				si.LeaderID, si.IsLeader, si.Term)
			break
		}
	}
	if !foundShard {
		t.Fatal("shard 1 not found in ShardInfoList")
	}
}

// TestIntegration_DB_DroppedRequestsOnInvalidShard verifies that
// proposals and reads on shards that aren't ready return errors.
// Matches dragonboat's TestDroppedRequestsAreReported.
func TestIntegration_DB_DroppedRequestsOnInvalidShard(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Propose on shard that doesn't exist.
	session := dbClient.NewNoOPSession(999, nil)
	_, propErr := nh.SyncPropose(ctx, session, encodeKVPut("k", "v"))
	if propErr == nil {
		t.Fatal("SyncPropose on non-existent shard should fail")
	}
	t.Logf("SyncPropose on non-existent shard: %v", propErr)

	// Read on shard that doesn't exist.
	_, readErr := nh.SyncRead(ctx, 999, encodeKVGet("k"))
	if readErr == nil {
		t.Fatal("SyncRead on non-existent shard should fail")
	}
	t.Logf("SyncRead on non-existent shard: %v", readErr)
}

// TestIntegration_DB_TimeoutCanBeReturned verifies that operations
// with a very short timeout return a timeout error. Matches
// dragonboat's TestTimeoutCanBeReturned.
func TestIntegration_DB_TimeoutCanBeReturned(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[cluster.findLeader(1)]

	// Use an already-expired context.
	expiredCtx, expiredCancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	// Ensure the context is truly expired before proceeding.
	expireTimer := time.NewTimer(1 * time.Millisecond)
	<-expireTimer.C
	expireTimer.Stop()
	defer expiredCancel()

	session := dbClient.NewNoOPSession(1, nil)
	_, propErr := leaderHost.SyncPropose(expiredCtx, session, encodeKVPut("k", "v"))
	if propErr == nil {
		t.Fatal("SyncPropose with expired context should return error")
	}
	t.Logf("SyncPropose with expired context returned: %v", propErr)
}

// TestIntegration_DB_RegisterSessionTwice verifies ProposeSession behavior
// when called on an already-registered session.
//
// In quicraft, after SyncGetSession the session's SeriesID is
// SeriesIDFirstProposal (normal proposal range), not SeriesIDForRegister.
// The adapter's ProposeSession routes based on SeriesID:
//   - SeriesIDForRegister  -> register a new session (new client ID)
//   - Any other value      -> close/unregister the session
//
// To simulate "register twice", we manually prepare the session for
// registration. Since quicraft's GetNewSession always creates a new
// client ID, ProposeSession creates a distinct session rather than
// re-registering the same one. This is correct behavior -- the adapter
// cannot re-register an existing client ID through the public API.
//
// Matches dragonboat's TestRegisterASessionTwiceWillBeReported in spirit:
// both verify that ProposeSession on an already-registered session does
// not produce undefined behavior.
func TestIntegration_DB_RegisterSessionTwice(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Register a tracked session.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	session, err := leaderHost.SyncGetSession(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetSession failed: %v", err)
	}
	originalClientID := session.ClientID()
	t.Logf("original session clientID=%d seriesID=%d", session.ClientID(), session.SeriesID())

	// Manually prepare the session for registration so ProposeSession
	// takes the register path. After SyncGetSession the SeriesID is in
	// the normal proposal range; PrepareForRegister sets it to
	// SeriesIDForRegister.
	session.Inner().PrepareForRegister()

	// ProposeSession with SeriesIDForRegister calls GetNewSession under
	// the hood, which creates a brand-new session with a different client
	// ID. It then overwrites the adapter session in place.
	rs, propErr := leaderHost.ProposeSession(session, defaultTimeout)
	if propErr != nil {
		// A direct error is acceptable -- some implementations may reject
		// the operation eagerly.
		t.Logf("ProposeSession returned error: %v", propErr)
		return
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Rejected() {
			// Rejection is acceptable if the engine detects the duplicate.
			t.Logf("ProposeSession for already-registered session was rejected")
			return
		}
		if !result.Completed() {
			t.Fatalf("unexpected result state: completed=%v rejected=%v",
				result.Completed(), result.Rejected())
		}

		// Registration succeeded -- verify it created a new session with
		// a different client ID, not a re-registration of the original.
		newClientID := session.ClientID()
		t.Logf("new session clientID=%d (original=%d)", newClientID, originalClientID)
		if newClientID == originalClientID {
			t.Fatal("ProposeSession should have created a new session with a different client ID")
		}
	case <-ctx.Done():
		t.Fatal("ProposeSession timed out waiting for result")
	}
}

// TestIntegration_DB_GetNodeHostRegistry verifies that
// GetNodeHostRegistry returns nil/false since quicraft doesn't support gossip.
func TestIntegration_DB_GetNodeHostRegistry(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	registry, ok := nh.GetNodeHostRegistry()
	if ok {
		t.Fatal("GetNodeHostRegistry should return false (gossip not supported)")
	}
	if registry != nil {
		t.Fatal("GetNodeHostRegistry should return nil registry")
	}
	t.Logf("GetNodeHostRegistry correctly returns nil, false")
}

// TestIntegration_DB_GetNodeUser verifies GetNodeUser returns a
// functional INodeUser for proposal and read operations.
func TestIntegration_DB_GetNodeUser(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// GetNodeUser on non-existent shard should fail.
	_, userErr := nh.GetNodeUser(99999)
	if userErr == nil {
		t.Fatal("GetNodeUser on non-existent shard should return error")
	}
	t.Logf("GetNodeUser on non-existent shard: %v", userErr)

	// Start a single-node shard.
	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica failed: %v", startErr)
	}

	// Wait for leader election.
	waitForCondition(t, leaderElectionTimeout, "single-node leader", func() bool {
		_, _, valid, lErr := nh.GetLeaderID(1)
		return lErr == nil && valid
	})

	// GetNodeUser should succeed now.
	user, userErr2 := nh.GetNodeUser(1)
	if userErr2 != nil {
		t.Fatalf("GetNodeUser failed: %v", userErr2)
	}
	if user.ShardID() != 1 {
		t.Fatalf("user.ShardID() = %d, want 1", user.ShardID())
	}
	t.Logf("GetNodeUser succeeded: ShardID=%d, ReplicaID=%d",
		user.ShardID(), user.ReplicaID())
}

// TestIntegration_DB_SnapshotRecoveryAfterRestart verifies that a node
// that restarts after a snapshot was taken recovers its state correctly.
// This exercises the full snapshot lifecycle: propose, snapshot, stop,
// restart, verify data intact.
func TestIntegration_DB_SnapshotRecoveryAfterRestart(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 15, 5)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose enough entries to trigger auto-snapshots.
	expected := proposeNEntries(t, cluster, trackers, 1, 30, "snap-recover")
	verifyAllData(t, cluster, trackers, 1, expected)

	// Stop a follower.
	followerIdx := -1
	for i := range cluster.hosts {
		if !cluster.stopped[i].Load() && i != leaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	followerHost := cluster.hosts[followerIdx]
	if err := followerHost.StopShard(1); err != nil {
		t.Fatalf("StopShard on follower failed: %v", err)
	}
	t.Logf("stopped shard on follower node[%d]", followerIdx)

	// Restart the follower's shard.
	replicaID := uint64(followerIdx + 1)
	createFn, newTracker := dbKVCreateFunc()
	cfg := snapshotCfg(1, replicaID)
	if err := followerHost.StartReplica(nil, true, createFn, cfg); err != nil {
		t.Fatalf("StartReplica (rejoin) failed: %v", err)
	}
	trackers[followerIdx] = newTracker

	// Verify the restarted follower catches up via snapshot recovery.
	sm := newTracker.Get(1, replicaID)
	if sm == nil {
		t.Fatal("restarted follower has no state machine")
	}
	for key, val := range expected {
		waitForCondition(t, defaultTimeout, "follower recovers: "+key, func() bool {
			got, ok := sm.Get(key)
			return ok && got == val
		})
	}
	t.Logf("follower recovered all %d entries after snapshot-based restart", len(expected))
}

// TestIntegration_DB_SnapshotIndex_ZeroOnRegularResult verifies that
// SnapshotIndex() returns 0 on a regular (non-snapshot) request result.
func TestIntegration_DB_SnapshotIndex_ZeroOnRegularResult(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	leaderHost := cluster.hosts[leaderIdx]
	session := dbClient.NewNoOPSession(1, nil)

	key := "snap-idx-key"
	value := "snap-idx-value"
	cmd := encodeKVPut(key, value)

	rs, err := leaderHost.Propose(session, cmd, defaultTimeout)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Fatalf("Propose did not complete: timeout=%v, rejected=%v",
				result.Timeout(), result.Rejected())
		}
		if result.SnapshotIndex() != 0 {
			t.Fatalf("SnapshotIndex() = %d, want 0 for regular proposal result",
				result.SnapshotIndex())
		}
		smResult := result.GetResult()
		if smResult.Value != uint64(len(value)) {
			t.Fatalf("GetResult().Value = %d, want %d", smResult.Value, len(value))
		}
		t.Logf("regular proposal result: SnapshotIndex=0, Value=%d", smResult.Value)
	case <-time.After(defaultTimeout):
		t.Fatal("Propose timed out waiting for ResultC")
	}
	rs.Release()
}
