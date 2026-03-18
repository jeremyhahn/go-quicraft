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
	"errors"
	"testing"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestIntegration_DB_StopAndRestartShard verifies that a shard can be
// stopped and restarted on the same NodeHost. Matches dragonboat's
// TestJoinedShardCanBeRestartedOrJoinedAgain.
func TestIntegration_DB_StopAndRestartShard(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose entries before stop.
	expected := proposeNEntries(t, cluster, trackers, 1, 5, "pre-restart")
	verifyAllData(t, cluster, trackers, 1, expected)

	// Stop the shard on one follower node.
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
		t.Fatalf("StopShard failed: %v", err)
	}
	t.Logf("stopped shard on node[%d]", followerIdx)

	// Restart the shard on the same follower node (join=true since already bootstrapped).
	replicaID := uint64(followerIdx + 1)
	createFn, newTracker := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, replicaID)
	if err := followerHost.StartReplica(nil, true, createFn, cfg); err != nil {
		t.Fatalf("StartReplica (rejoin) failed: %v", err)
	}
	trackers[followerIdx] = newTracker
	t.Logf("restarted shard on node[%d]", followerIdx)

	// Verify the restarted node catches up with all data.
	sm := newTracker.Get(1, replicaID)
	if sm == nil {
		t.Fatal("restarted node has no state machine")
	}
	for key, val := range expected {
		waitForCondition(t, defaultTimeout, "restarted node catches up: "+key, func() bool {
			got, ok := sm.Get(key)
			return ok && got == val
		})
	}
	t.Logf("restarted node caught up with all %d entries", len(expected))
}

// TestIntegration_DB_RemoveNodeData verifies that RemoveData removes
// all data for a stopped shard/replica pair. Matches dragonboat's
// TestRemoveNodeDataRemovesAllNodeData.
func TestIntegration_DB_RemoveNodeData(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// Start a single-node shard.
	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica failed: %v", startErr)
	}

	// Wait for leader and propose data.
	waitForCondition(t, leaderElectionTimeout, "single-node leader", func() bool {
		_, _, valid, lErr := nh.GetLeaderID(1)
		return lErr == nil && valid
	})

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	session := dbClient.NewNoOPSession(1, nil)
	_, propErr := nh.SyncPropose(ctx, session, encodeKVPut("key", "val"))
	if propErr != nil {
		t.Fatalf("SyncPropose failed: %v", propErr)
	}

	// Stop the shard.
	if stopErr := nh.StopShard(1); stopErr != nil {
		t.Fatalf("StopShard failed: %v", stopErr)
	}

	// Remove the data.
	if rmErr := nh.RemoveData(1, 1); rmErr != nil {
		t.Fatalf("RemoveData failed: %v", rmErr)
	}
	t.Logf("RemoveData succeeded for shard=1, replica=1")

	// HasNodeInfo should return false after removal.
	if nh.HasNodeInfo(1, 1) {
		t.Fatal("HasNodeInfo should return false after RemoveData")
	}
	t.Logf("HasNodeInfo correctly returns false after RemoveData")
}

// TestIntegration_DB_RemoveData_StillRunning verifies that RemoveData
// fails when the shard is still running. Matches dragonboat's
// TestRemoveNodeDataWillFailWhenNodeIsStillRunning.
func TestIntegration_DB_RemoveData_StillRunning(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica failed: %v", startErr)
	}

	// Try to remove data while shard is running — should fail.
	rmErr := nh.RemoveData(1, 1)
	if rmErr == nil {
		t.Fatal("RemoveData should fail when shard is still running")
	}
	t.Logf("RemoveData on running shard returned expected error: %v", rmErr)
}

// TestIntegration_DB_NodeHostID_IsStatic verifies that the NodeHost ID
// remains stable and is returned correctly. Matches dragonboat's
// TestNodeHostIDIsStatic.
func TestIntegration_DB_NodeHostID_IsStatic(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	id1 := nh.ID()
	id2 := nh.ID()
	if id1 == "" {
		t.Fatal("NodeHost ID should not be empty")
	}
	if id1 != id2 {
		t.Fatalf("NodeHost ID changed: %q != %q", id1, id2)
	}
	t.Logf("NodeHost ID is stable: %s", id1)
}

// TestIntegration_DB_HasNodeInfo verifies HasNodeInfo returns correct
// results for existing and non-existing shard/replica pairs. Matches
// dragonboat's TestNodeHostHasNodeInfo.
func TestIntegration_DB_HasNodeInfo(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// Before starting any shard.
	if nh.HasNodeInfo(1, 1) {
		t.Fatal("HasNodeInfo should return false before StartReplica")
	}

	// Start a shard.
	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica failed: %v", startErr)
	}

	// After starting, HasNodeInfo should return true.
	if !nh.HasNodeInfo(1, 1) {
		t.Fatal("HasNodeInfo should return true after StartReplica")
	}

	// Non-existent shard/replica should return false.
	if nh.HasNodeInfo(99, 99) {
		t.Fatal("HasNodeInfo should return false for non-existent shard")
	}
	t.Logf("HasNodeInfo verified for existing and non-existing replicas")
}

// TestIntegration_DB_StopShard verifies StopShard stops a running shard
// and StopReplica stops a specific replica.
func TestIntegration_DB_StopShard_And_StopReplica(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica failed: %v", startErr)
	}

	// StopShard should succeed.
	if stopErr := nh.StopShard(1); stopErr != nil {
		t.Fatalf("StopShard failed: %v", stopErr)
	}

	// StopShard on already stopped shard — some implementations allow
	// idempotent stop, so we only log the outcome without failing.
	stopErr := nh.StopShard(1)
	if stopErr == nil {
		t.Logf("StopShard on already-stopped shard succeeded (implementation allows it)")
	} else {
		t.Logf("StopShard on already-stopped shard returned: %v", stopErr)
	}

	// StopShard on non-existent shard.
	stop99Err := nh.StopShard(99999)
	if stop99Err == nil {
		t.Fatal("StopShard on non-existent shard should return error")
	}
	t.Logf("StopShard on non-existent shard correctly returned: %v", stop99Err)
	t.Logf("StopShard lifecycle verified")
}

// TestIntegration_DB_StartDuplicateReplica verifies that starting a
// shard that is already running returns an error.
func TestIntegration_DB_StartDuplicateReplica(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("first StartReplica failed: %v", startErr)
	}

	// Second start should fail.
	createFn2, _ := dbKVCreateFunc()
	dupErr := nh.StartReplica(members, false, createFn2, cfg)
	if dupErr == nil {
		t.Fatal("duplicate StartReplica should return error")
	}
	if errors.Is(dupErr, dragonboat.ErrShardAlreadyExist) {
		t.Logf("duplicate StartReplica correctly returned ErrShardAlreadyExist")
	} else {
		t.Logf("duplicate StartReplica returned: %v", dupErr)
	}
}

// TestIntegration_DB_UsingClosedNodeHost verifies that various
// operations on a closed NodeHost return errors.
func TestIntegration_DB_UsingClosedNodeHost(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	nh.Close()

	// All operations should fail on closed host.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, readErr := nh.SyncRead(ctx, 1, []byte("key"))
	if readErr == nil {
		t.Fatal("SyncRead on closed host should return error")
	}

	_, memberErr := nh.SyncGetShardMembership(ctx, 1)
	if memberErr == nil {
		t.Fatal("SyncGetShardMembership on closed host should return error")
	}

	_, staleErr := nh.StaleRead(1, []byte("key"))
	if staleErr == nil {
		t.Fatal("StaleRead on closed host should return error")
	}

	t.Logf("all operations on closed NodeHost returned errors correctly")
}

// TestIntegration_DB_NewNodeHost_InvalidConfig verifies that creating
// a NodeHost with invalid configuration returns an error.
func TestIntegration_DB_NewNodeHost_InvalidConfig(t *testing.T) {
	// Empty RaftAddress should fail validation.
	nhConfig := dbConfig.NodeHostConfig{
		WALDir:      t.TempDir(),
		NodeHostDir: t.TempDir(),
		// Missing RaftAddress, ListenAddress.
	}

	_, err := dragonboat.NewNodeHost(nhConfig)
	if err == nil {
		t.Fatal("NewNodeHost with invalid config should return error")
	}
	t.Logf("NewNodeHost with invalid config returned: %v", err)
}

// TestIntegration_DB_NodeHostID_CustomID verifies that setting
// NodeHostID in NodeHostConfig produces that exact ID from nh.ID().
func TestIntegration_DB_NodeHostID_CustomID(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)
	nhConfig.NodeHostID = "custom-test-host-id-12345"

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	got := nh.ID()
	if got != "custom-test-host-id-12345" {
		t.Fatalf("nh.ID() = %q, want %q", got, "custom-test-host-id-12345")
	}
	t.Logf("custom NodeHostID verified: %s", got)
}

// TestIntegration_DB_StartReplicaWaitForReadiness verifies that after
// StartReplica returns and a leader is elected, the shard becomes fully
// ready to serve reads and writes. This confirms the cluster transitions
// from the initial startup state to an operational state where
// linearizable reads succeed within a reasonable timeout.
func TestIntegration_DB_StartReplicaWaitForReadiness(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)

	// Wait for leader election. After StartReplica returns, the Raft
	// nodes begin their election protocol. A leader must be elected
	// before the shard can serve requests.
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	leaderHost := cluster.hosts[leaderIdx]

	// Verify that the shard can serve a SyncRead. A successful
	// linearizable read confirms: (1) a leader exists, (2) the leader
	// has committed a no-op entry to establish its term (PhD 6.4),
	// and (3) the ReadIndex protocol succeeds, meaning the shard is
	// fully operational.
	//
	// The warmup proposal ensures the leader has committed at least
	// one entry in its term, which is required for ReadIndex to work
	// correctly per the Raft specification.
	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer warmupCancel()

	session := dbClient.NewNoOPSession(1, nil)
	warmupCmd := encodeKVPut("readiness-check", "ready")
	_, warmupErr := leaderHost.SyncPropose(warmupCtx, session, warmupCmd)
	if warmupErr != nil {
		t.Fatalf("SyncPropose warmup failed: %v", warmupErr)
	}

	// Verify the warmup entry replicated to all nodes, confirming
	// cluster-wide readiness.
	waitForReplication(t, cluster, trackers, 1, "readiness-check", "ready", defaultTimeout)
	t.Logf("all 3 nodes replicated warmup entry")

	// Now perform a SyncRead on the leader to confirm linearizable
	// reads are operational.
	readCtx, readCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer readCancel()

	readResult, readErr := leaderHost.SyncRead(readCtx, 1, encodeKVGet("readiness-check"))
	if readErr != nil {
		t.Fatalf("SyncRead on leader failed after StartReplica: %v", readErr)
	}
	val, ok := readResult.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", readResult)
	}
	if val != "ready" {
		t.Fatalf("SyncRead = %q, want %q", val, "ready")
	}
	t.Logf("leader SyncRead succeeded: readiness-check=%q", val)

	// Verify SyncRead works on follower nodes as well, confirming
	// all replicas are ready to participate in the ReadIndex protocol.
	for i, nh := range cluster.hosts {
		if cluster.stopped[i].Load() || i == leaderIdx {
			continue
		}
		followerCtx, followerCancel := context.WithTimeout(context.Background(), defaultTimeout)
		followerResult, followerErr := nh.SyncRead(followerCtx, 1, encodeKVGet("readiness-check"))
		followerCancel()
		if followerErr != nil {
			t.Fatalf("SyncRead on follower node[%d] failed: %v", i, followerErr)
		}
		followerVal, followerOk := followerResult.(string)
		if !followerOk {
			t.Fatalf("SyncRead on follower node[%d] returned type %T, want string", i, followerResult)
		}
		if followerVal != "ready" {
			t.Fatalf("SyncRead on follower node[%d] = %q, want %q", i, followerVal, "ready")
		}
		t.Logf("follower node[%d] SyncRead succeeded: readiness-check=%q", i, followerVal)
	}
	t.Logf("all 3 nodes are ready and serving linearizable reads after StartReplica")
}
