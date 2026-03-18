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

package host_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestHostAPI_SyncPropose_ThreeNode proposes to the leader and verifies
// the value is replicated to all nodes.
func TestHostAPI_SyncPropose_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	result, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut("sp-key", "sp-value"))
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}
	if result.Value == 0 {
		t.Error("SyncPropose result.Value is 0")
	}

	waitForReplication(t, cluster, trackers, 1, "sp-key", "sp-value", defaultTimeout)
}

// TestHostAPI_SyncRead_ThreeNode writes via leader then reads back with
// linearizable SyncRead.
func TestHostAPI_SyncRead_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut("sr-key", "sr-value"))
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "sr-key applied on leader", func() bool {
		_, ok := leaderSM.Get("sr-key")
		return ok
	})

	result, err := leaderHost.SyncRead(ctx, 1, "sr-key")
	if err != nil {
		t.Fatalf("SyncRead failed: %v", err)
	}
	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned unexpected type: %T", result)
	}
	if val != "sr-value" {
		t.Errorf("SyncRead = %q, want %q", val, "sr-value")
	}
}

// TestHostAPI_StaleRead_FromFollower writes on leader, stale-reads from follower.
func TestHostAPI_StaleRead_FromFollower(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut("stale-key", "stale-val"))
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}

	followerIdx := cluster.findFollower(1)
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	followerSM := trackers[followerIdx].Get(1, uint64(followerIdx+1))
	waitForCondition(t, defaultTimeout, "follower replicates stale-key", func() bool {
		_, ok := followerSM.Get("stale-key")
		return ok
	})

	result, err := cluster.hosts[followerIdx].StaleRead(ctx, 1, "stale-key")
	if err != nil {
		t.Fatalf("StaleRead failed: %v", err)
	}
	if val, ok := result.(string); !ok || val != "stale-val" {
		t.Errorf("StaleRead = %v, want 'stale-val'", result)
	}
}

// TestHostAPI_QueryLocalNode_ThreeNode does a local query after replication.
func TestHostAPI_QueryLocalNode_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut("local-key", "local-val"))
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "local-key applied", func() bool {
		_, ok := leaderSM.Get("local-key")
		return ok
	})

	result, err := leaderHost.QueryLocalNode(ctx, 1, "local-key")
	if err != nil {
		t.Fatalf("QueryLocalNode failed: %v", err)
	}
	if val, ok := result.(string); !ok || val != "local-val" {
		t.Errorf("QueryLocalNode = %v, want 'local-val'", result)
	}
}

// TestHostAPI_Session_Lifecycle_ThreeNode creates a session on leader, uses it, closes it.
func TestHostAPI_Session_Lifecycle_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session, err := leaderHost.GetNewSession(ctx, 1)
	if err != nil {
		t.Fatalf("GetNewSession failed: %v", err)
	}
	if session.ClientID() == 0 {
		t.Error("session ClientID is 0")
	}

	rs, err := leaderHost.CloseSession(ctx, 1, session)
	if err != nil {
		t.Fatalf("CloseSession failed: %v", err)
	}
	rs.Release()
}

// TestHostAPI_Snapshot_LeaderRequest requests a snapshot on the leader.
func TestHostAPI_Snapshot_LeaderRequest(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write some data first.
	for i := 0; i < 5; i++ {
		_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut(
			fmt.Sprintf("snap-key-%d", i),
			fmt.Sprintf("snap-val-%d", i),
		))
		if err != nil {
			t.Fatalf("SyncPropose[%d] failed: %v", i, err)
		}
	}

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "5 entries applied", func() bool {
		return leaderSM.Len() >= 5
	})

	rs, err := leaderHost.RequestSnapshot(ctx, 1, quicraft.SnapshotOption{})
	if err != nil {
		t.Fatalf("RequestSnapshot failed: %v", err)
	}
	if rs == nil {
		t.Fatal("RequestSnapshot returned nil")
	}
	rs.Release()
}

// TestHostAPI_ReadIndex_ThreeNode verifies ReadIndex on leader.
func TestHostAPI_ReadIndex_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	rs, err := leaderHost.ReadIndex(ctx, 1)
	if err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}
	if rs == nil {
		t.Fatal("ReadIndex returned nil")
	}
	rs.Release()
}

// TestHostAPI_LeaderTransfer_ThreeNode requests leadership transfer to a
// specific target and verifies the request completes without error.
func TestHostAPI_LeaderTransfer_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	// Wait for a follower to discover the leader before transferring.
	targetIdx := cluster.waitForFollower(1, leaderElectionTimeout)
	targetReplicaID := uint64(targetIdx + 1)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	if err := leaderHost.RequestLeaderTransfer(ctx, 1, targetReplicaID); err != nil {
		t.Fatalf("RequestLeaderTransfer failed: %v", err)
	}

	// Verify the request was accepted (fire-and-forget). The transfer
	// is asynchronous; we verify the API call succeeds without error.
	// Also verify the original leader is still queryable.
	info, err := leaderHost.GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo after transfer request failed: %v", err)
	}
	if info.ShardID != 1 {
		t.Errorf("ShardID = %d, want 1", info.ShardID)
	}
}

// TestHostAPI_GetShardInfo_AllNodes verifies all nodes return correct shard info.
func TestHostAPI_GetShardInfo_AllNodes(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Wait for all nodes to learn the term (followers may lag behind the
	// leader's heartbeat). This avoids races where a follower still reports
	// Term=0 right after the leader is elected.
	for i, h := range cluster.hosts {
		idx := i
		host := h
		waitForCondition(t, leaderElectionTimeout,
			fmt.Sprintf("node[%d] learns term", idx),
			func() bool {
				info, err := host.GetShardInfo(1)
				return err == nil && info.Term > 0
			},
		)
	}

	for i, h := range cluster.hosts {
		info, err := h.GetShardInfo(1)
		if err != nil {
			t.Fatalf("node[%d] GetShardInfo failed: %v", i, err)
		}
		if info.ShardID != 1 {
			t.Errorf("node[%d] ShardID = %d, want 1", i, info.ShardID)
		}
		expectedReplicaID := uint64(i + 1)
		if info.ReplicaID != expectedReplicaID {
			t.Errorf("node[%d] ReplicaID = %d, want %d", i, info.ReplicaID, expectedReplicaID)
		}
		if info.Term == 0 {
			t.Errorf("node[%d] Term is 0", i)
		}
	}
}

// TestHostAPI_GetNodeHostInfo_AllNodes verifies all nodes return host info.
func TestHostAPI_GetNodeHostInfo_AllNodes(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	for i, h := range cluster.hosts {
		info := h.GetNodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] GetNodeHostInfo returned nil", i)
		}
		shardCount := len(info.ShardInfoList)
		if shardCount != 1 {
			t.Errorf("node[%d] ShardCount = %d, want 1", i, shardCount)
		}
		if info.RaftAddress != cluster.addrs[i] {
			t.Errorf("node[%d] RaftAddress = %q, want %q", i, info.RaftAddress, cluster.addrs[i])
		}
	}
}

// TestHostAPI_MultipleShards_ThreeNode runs 3 independent shards on the cluster.
func TestHostAPI_MultipleShards_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	// Start 3 shards and collect trackers per shard. After each shard
	// elects a leader, perform a warmup proposal to ensure all QUIC
	// connections are fully established before starting the next shard.
	shardTrackers := make(map[uint64][]*smTracker)
	for shardID := uint64(1); shardID <= 3; shardID++ {
		trackers := cluster.startShard(shardID)
		shardTrackers[shardID] = trackers
		leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

		// Warmup: propose and verify replication to establish QUIC
		// connections between all nodes for this shard.
		warmupCtx, warmupCancel := context.WithTimeout(context.Background(), defaultTimeout)
		warmupKey := fmt.Sprintf("warmup-shard%d", shardID)
		_, err := cluster.hosts[leaderIdx].SyncPropose(warmupCtx, shardID,
			encodeKVPut(warmupKey, "warmup"))
		warmupCancel()
		if err != nil {
			t.Fatalf("warmup SyncPropose shard %d failed: %v", shardID, err)
		}
		waitForReplication(t, cluster, trackers, shardID, warmupKey, "warmup", defaultTimeout)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write to each shard via its leader.
	for shardID := uint64(1); shardID <= 3; shardID++ {
		leaderIdx := cluster.findLeader(shardID)
		if leaderIdx < 0 {
			t.Fatalf("no leader for shard %d", shardID)
		}
		key := fmt.Sprintf("shard%d-key", shardID)
		val := fmt.Sprintf("shard%d-val", shardID)
		_, err := cluster.hosts[leaderIdx].SyncPropose(ctx, shardID, encodeKVPut(key, val))
		if err != nil {
			t.Fatalf("SyncPropose shard %d failed: %v", shardID, err)
		}
	}

	// Verify host info shows all 3 shards.
	for i, h := range cluster.hosts {
		info := h.GetNodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] GetNodeHostInfo returned nil", i)
		}
		shardCount := len(info.ShardInfoList)
		if shardCount != 3 {
			t.Errorf("node[%d] ShardCount = %d, want 3", i, shardCount)
		}
	}
}

// TestHostAPI_ConcurrentProposals_ThreeNode sends proposals from 5 goroutines.
func TestHostAPI_ConcurrentProposals_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	numGoroutines := 5
	proposalsPerGoroutine := 20

	var wg sync.WaitGroup
	var successCount atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for p := 0; p < proposalsPerGoroutine; p++ {
				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				key := fmt.Sprintf("conc-g%d-p%d", gID, p)
				val := fmt.Sprintf("v-%d-%d", gID, p)
				_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut(key, val))
				cancel()
				if err != nil {
					continue
				}
				successCount.Add(1)
			}
		}(g)
	}

	wg.Wait()
	t.Logf("concurrent proposals: %d/%d succeeded", successCount.Load(), numGoroutines*proposalsPerGoroutine)

	if successCount.Load() == 0 {
		t.Fatal("no proposals succeeded")
	}

	// Verify replication to all followers.
	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "leader SM apply all", func() bool {
		return int64(leaderSM.Len()) >= successCount.Load()
	})

	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		followerSM := trackers[i].Get(1, uint64(i+1))
		if followerSM == nil {
			continue
		}
		waitForCondition(t, defaultTimeout*2,
			fmt.Sprintf("follower[%d] replication", i),
			func() bool {
				return int64(followerSM.Len()) >= successCount.Load()
			},
		)
	}
}

// TestHostAPI_ProposeToFollower_Forwarded verifies that proposing to a
// follower succeeds via automatic leader forwarding.
func TestHostAPI_ProposeToFollower_Forwarded(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Wait for a follower that knows the leader so it can forward proposals.
	followerIdx := cluster.waitForFollower(1, leaderElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Propose via follower — this should succeed via leader forwarding.
	_, err := cluster.hosts[followerIdx].SyncPropose(ctx, 1, encodeKVPut("fwd-key", "fwd-val"))
	if err != nil {
		t.Fatalf("SyncPropose via follower failed: %v", err)
	}

	// Verify the value was committed and replicated.
	waitForReplication(t, cluster, trackers, 1, "fwd-key", "fwd-val", defaultTimeout)
}

// TestHostAPI_LargeValue_Replication replicates a 64KB value to all nodes.
func TestHostAPI_LargeValue_Replication(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Create a 64KB value.
	largeVal := strings.Repeat("X", 64*1024)
	_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut("large-key", largeVal))
	if err != nil {
		t.Fatalf("SyncPropose large value failed: %v", err)
	}

	// Verify replication.
	for i := range cluster.hosts {
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			continue
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] replicates large value", i),
			func() bool {
				val, ok := nodeSM.Get("large-key")
				return ok && len(val) == 64*1024
			},
		)
	}
}

// ---------------------------------------------------------------------------
// 5-Node Cluster Tests
// ---------------------------------------------------------------------------

// TestHostAPI_SyncPropose_FiveNode proposes to the leader of a 5-node cluster
// and verifies the value is replicated to all nodes.
func TestHostAPI_SyncPropose_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	result, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut("sp5-key", "sp5-value"))
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}
	if result.Value == 0 {
		t.Error("SyncPropose result.Value is 0")
	}

	waitForReplication(t, cluster, trackers, 1, "sp5-key", "sp5-value", defaultTimeout)
}

// TestHostAPI_SyncRead_FiveNode writes via leader then reads back with
// linearizable SyncRead on a 5-node cluster.
func TestHostAPI_SyncRead_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut("sr5-key", "sr5-value"))
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "sr5-key applied on leader", func() bool {
		_, ok := leaderSM.Get("sr5-key")
		return ok
	})

	result, err := leaderHost.SyncRead(ctx, 1, "sr5-key")
	if err != nil {
		t.Fatalf("SyncRead failed: %v", err)
	}
	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned unexpected type: %T", result)
	}
	if val != "sr5-value" {
		t.Errorf("SyncRead = %q, want %q", val, "sr5-value")
	}
}

// TestHostAPI_StaleRead_FiveNode writes on leader, stale-reads from a follower
// in a 5-node cluster.
func TestHostAPI_StaleRead_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut("stale5-key", "stale5-val"))
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}

	followerIdx := cluster.findFollower(1)
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	followerSM := trackers[followerIdx].Get(1, uint64(followerIdx+1))
	waitForCondition(t, defaultTimeout, "follower replicates stale5-key", func() bool {
		_, ok := followerSM.Get("stale5-key")
		return ok
	})

	result, err := cluster.hosts[followerIdx].StaleRead(ctx, 1, "stale5-key")
	if err != nil {
		t.Fatalf("StaleRead failed: %v", err)
	}
	if val, ok := result.(string); !ok || val != "stale5-val" {
		t.Errorf("StaleRead = %v, want 'stale5-val'", result)
	}
}

// TestHostAPI_QueryLocalNode_FiveNode does a local query on the leader after
// replication in a 5-node cluster.
func TestHostAPI_QueryLocalNode_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut("local5-key", "local5-val"))
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "local5-key applied", func() bool {
		_, ok := leaderSM.Get("local5-key")
		return ok
	})

	result, err := leaderHost.QueryLocalNode(ctx, 1, "local5-key")
	if err != nil {
		t.Fatalf("QueryLocalNode failed: %v", err)
	}
	if val, ok := result.(string); !ok || val != "local5-val" {
		t.Errorf("QueryLocalNode = %v, want 'local5-val'", result)
	}
}

// TestHostAPI_Session_Lifecycle_FiveNode creates a session on the leader,
// uses it, and closes it in a 5-node cluster.
func TestHostAPI_Session_Lifecycle_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session, err := leaderHost.GetNewSession(ctx, 1)
	if err != nil {
		t.Fatalf("GetNewSession failed: %v", err)
	}
	if session.ClientID() == 0 {
		t.Error("session ClientID is 0")
	}

	rs, err := leaderHost.CloseSession(ctx, 1, session)
	if err != nil {
		t.Fatalf("CloseSession failed: %v", err)
	}
	rs.Release()
}

// TestHostAPI_Snapshot_FiveNode requests a snapshot on the leader after
// writing data in a 5-node cluster.
func TestHostAPI_Snapshot_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write data so the snapshot has content.
	for i := 0; i < 5; i++ {
		_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut(
			fmt.Sprintf("snap5-key-%d", i),
			fmt.Sprintf("snap5-val-%d", i),
		))
		if err != nil {
			t.Fatalf("SyncPropose[%d] failed: %v", i, err)
		}
	}

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "5 entries applied", func() bool {
		return leaderSM.Len() >= 5
	})

	rs, err := leaderHost.RequestSnapshot(ctx, 1, quicraft.SnapshotOption{})
	if err != nil {
		t.Fatalf("RequestSnapshot failed: %v", err)
	}
	if rs == nil {
		t.Fatal("RequestSnapshot returned nil")
	}
	rs.Release()
}

// TestHostAPI_ReadIndex_FiveNode verifies ReadIndex on the leader of a
// 5-node cluster.
func TestHostAPI_ReadIndex_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	rs, err := leaderHost.ReadIndex(ctx, 1)
	if err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}
	if rs == nil {
		t.Fatal("ReadIndex returned nil")
	}
	rs.Release()
}

// TestHostAPI_LeaderTransfer_FiveNode requests leadership transfer to a
// specific follower in a 5-node cluster and verifies the request completes.
func TestHostAPI_LeaderTransfer_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	// Wait for a follower to discover the leader before transferring.
	targetIdx := cluster.waitForFollower(1, leaderElectionTimeout)
	targetReplicaID := uint64(targetIdx + 1)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	if err := leaderHost.RequestLeaderTransfer(ctx, 1, targetReplicaID); err != nil {
		t.Fatalf("RequestLeaderTransfer failed: %v", err)
	}

	// Verify the request was accepted. Leadership transfer is asynchronous.
	info, err := leaderHost.GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo after transfer request failed: %v", err)
	}
	if info.ShardID != 1 {
		t.Errorf("ShardID = %d, want 1", info.ShardID)
	}
}

// TestHostAPI_GetShardInfo_FiveNode verifies all 5 nodes return correct
// shard info.
func TestHostAPI_GetShardInfo_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Wait for all nodes to learn the term (followers may lag behind the
	// leader's heartbeat). This avoids races where a follower still reports
	// Term=0 right after the leader is elected.
	for i, h := range cluster.hosts {
		idx := i
		host := h
		waitForCondition(t, leaderElectionTimeout,
			fmt.Sprintf("node[%d] learns term", idx),
			func() bool {
				info, err := host.GetShardInfo(1)
				return err == nil && info.Term > 0
			},
		)
	}

	for i, h := range cluster.hosts {
		info, err := h.GetShardInfo(1)
		if err != nil {
			t.Fatalf("node[%d] GetShardInfo failed: %v", i, err)
		}
		if info.ShardID != 1 {
			t.Errorf("node[%d] ShardID = %d, want 1", i, info.ShardID)
		}
		expectedReplicaID := uint64(i + 1)
		if info.ReplicaID != expectedReplicaID {
			t.Errorf("node[%d] ReplicaID = %d, want %d", i, info.ReplicaID, expectedReplicaID)
		}
		if info.Term == 0 {
			t.Errorf("node[%d] Term is 0", i)
		}
	}

	// Wait for exactly one leader. During election transitions, multiple
	// nodes may momentarily report IsLeader before convergence.
	waitForCondition(t, leaderElectionTimeout, "exactly 1 leader", func() bool {
		count := 0
		for _, h := range cluster.hosts {
			info, err := h.GetShardInfo(1)
			if err != nil {
				return false
			}
			if info.IsLeader {
				count++
			}
		}
		return count == 1
	})
}

// TestHostAPI_GetNodeHostInfo_FiveNode verifies all 5 nodes return host info.
func TestHostAPI_GetNodeHostInfo_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	for i, h := range cluster.hosts {
		info := h.GetNodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] GetNodeHostInfo returned nil", i)
		}
		shardCount := len(info.ShardInfoList)
		if shardCount != 1 {
			t.Errorf("node[%d] ShardCount = %d, want 1", i, shardCount)
		}
		if info.RaftAddress != cluster.addrs[i] {
			t.Errorf("node[%d] RaftAddress = %q, want %q", i, info.RaftAddress, cluster.addrs[i])
		}
	}
}

// TestHostAPI_MultipleShards_FiveNode runs 3 independent shards on the 5-node
// cluster and verifies each shard has an independent leader and can replicate.
func TestHostAPI_MultipleShards_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	// Start 3 shards and collect trackers per shard. After each shard
	// elects a leader, perform a warmup proposal to ensure all QUIC
	// connections are fully established before starting the next shard.
	shardTrackers := make(map[uint64][]*smTracker)
	for shardID := uint64(1); shardID <= 3; shardID++ {
		trackers := cluster.startShard(shardID)
		shardTrackers[shardID] = trackers
		leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

		// Warmup: propose and verify replication to establish QUIC
		// connections between all 5 nodes for this shard.
		warmupCtx, warmupCancel := context.WithTimeout(context.Background(), defaultTimeout)
		warmupKey := fmt.Sprintf("warmup-shard%d", shardID)
		_, err := cluster.hosts[leaderIdx].SyncPropose(warmupCtx, shardID,
			encodeKVPut(warmupKey, "warmup"))
		warmupCancel()
		if err != nil {
			t.Fatalf("warmup SyncPropose shard %d failed: %v", shardID, err)
		}
		waitForReplication(t, cluster, trackers, shardID, warmupKey, "warmup", defaultTimeout)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write to each shard via its leader.
	for shardID := uint64(1); shardID <= 3; shardID++ {
		leaderIdx := cluster.findLeader(shardID)
		if leaderIdx < 0 {
			t.Fatalf("no leader for shard %d", shardID)
		}
		key := fmt.Sprintf("ms5-shard%d-key", shardID)
		val := fmt.Sprintf("ms5-shard%d-val", shardID)
		_, err := cluster.hosts[leaderIdx].SyncPropose(ctx, shardID, encodeKVPut(key, val))
		if err != nil {
			t.Fatalf("SyncPropose shard %d failed: %v", shardID, err)
		}
	}

	// Verify replication for each shard.
	for shardID := uint64(1); shardID <= 3; shardID++ {
		key := fmt.Sprintf("ms5-shard%d-key", shardID)
		val := fmt.Sprintf("ms5-shard%d-val", shardID)
		waitForReplication(t, cluster, shardTrackers[shardID], shardID, key, val, defaultTimeout)
	}

	// Verify host info shows all 3 shards.
	for i, h := range cluster.hosts {
		info := h.GetNodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] GetNodeHostInfo returned nil", i)
		}
		shardCount := len(info.ShardInfoList)
		if shardCount != 3 {
			t.Errorf("node[%d] ShardCount = %d, want 3", i, shardCount)
		}
	}
}

// TestHostAPI_ConcurrentProposals_FiveNode sends proposals from 5 goroutines
// (20 each) to the leader of a 5-node cluster and verifies replication.
func TestHostAPI_ConcurrentProposals_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	numGoroutines := 5
	proposalsPerGoroutine := 20

	var wg sync.WaitGroup
	var successCount atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for p := 0; p < proposalsPerGoroutine; p++ {
				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				key := fmt.Sprintf("conc5-g%d-p%d", gID, p)
				val := fmt.Sprintf("v5-%d-%d", gID, p)
				_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut(key, val))
				cancel()
				if err != nil {
					continue
				}
				successCount.Add(1)
			}
		}(g)
	}

	wg.Wait()
	t.Logf("concurrent proposals (5-node): %d/%d succeeded", successCount.Load(), numGoroutines*proposalsPerGoroutine)

	if successCount.Load() == 0 {
		t.Fatal("no proposals succeeded")
	}

	// Verify replication to all nodes.
	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "leader SM apply all", func() bool {
		return int64(leaderSM.Len()) >= successCount.Load()
	})

	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		followerSM := trackers[i].Get(1, uint64(i+1))
		if followerSM == nil {
			continue
		}
		waitForCondition(t, defaultTimeout*2,
			fmt.Sprintf("follower[%d] replication (5-node)", i),
			func() bool {
				return int64(followerSM.Len()) >= successCount.Load()
			},
		)
	}
}

// TestHostAPI_ProposeToFollower_FiveNode verifies that proposing to a follower
// in a 5-node cluster succeeds via automatic leader forwarding.
func TestHostAPI_ProposeToFollower_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Wait for a follower that knows the leader so it can forward proposals.
	followerIdx := cluster.waitForFollower(1, leaderElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Propose via follower — this should succeed via leader forwarding.
	_, err := cluster.hosts[followerIdx].SyncPropose(ctx, 1, encodeKVPut("fwd5-key", "fwd5-val"))
	if err != nil {
		t.Fatalf("SyncPropose via follower failed: %v", err)
	}

	// Verify the value was committed and replicated.
	waitForReplication(t, cluster, trackers, 1, "fwd5-key", "fwd5-val", defaultTimeout)
}

// TestHostAPI_LargeValue_FiveNode replicates a 64KB value to all 5 nodes.
func TestHostAPI_LargeValue_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Create a 64KB value.
	largeVal := strings.Repeat("Y", 64*1024)
	_, err := leaderHost.SyncPropose(ctx, 1, encodeKVPut("large5-key", largeVal))
	if err != nil {
		t.Fatalf("SyncPropose large value failed: %v", err)
	}

	// Verify replication to all 5 nodes.
	for i := range cluster.hosts {
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			continue
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] replicates large value (5-node)", i),
			func() bool {
				val, ok := nodeSM.Get("large5-key")
				return ok && len(val) == 64*1024
			},
		)
	}
}
