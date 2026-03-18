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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestHostAPI_MembershipChange_AddAndRemoveNode verifies that a 4th node
// can be dynamically added to a 3-node cluster via RequestAddNode, receives
// all replicated data (including data written before it joined), accepts
// new writes, and then is cleanly removed via RequestDeleteNode while the
// remaining 3-node cluster continues operating.
//
// The test exercises:
// - Dynamic membership addition via Raft config change
// - Snapshot or log catch-up to the new node
// - Continued replication with the expanded 4-node cluster
// - Dynamic membership removal via Raft config change
// - Cluster stability after returning to 3 nodes
// - Linearizable reads (SyncRead) for data verification
func TestHostAPI_MembershipChange_AddAndRemoveNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Phase 1: Write initial data to the 3-node cluster and verify
	// replication across all nodes.
	initialData := proposeNEntries(t, cluster, trackers, shardID, 10, "pre-add")
	verifyAllData(t, cluster, trackers, shardID, initialData)
	t.Logf("phase 1 complete: 10 entries replicated to all 3 nodes")

	// Phase 2: Add a 4th node to the cluster. The addNodeToCluster helper
	// creates a new Host, requests the membership change from the leader,
	// and starts the shard in join mode.
	const newReplicaID uint64 = 4
	result := addNodeToCluster(t, cluster, shardID, newReplicaID, testShardConfig)
	t.Logf("phase 2: added node[%d] (replica=%d) at %s", result.idx, newReplicaID, result.addr)

	// Wait for the cluster to recognize 4 voting members.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 4, membershipTimeout)
	t.Logf("phase 2: membership size confirmed as 4")

	// Verify the new node receives all pre-existing data via log replay
	// or snapshot catch-up.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, initialData, membershipTimeout)
	t.Logf("phase 2 complete: node[%d] caught up on all %d pre-existing entries", result.idx, len(initialData))

	// Phase 3: Write additional data with the 4-node cluster and verify
	// the new node receives it.
	postAddData := proposeNEntries(t, cluster, trackers, shardID, 10, "post-add")
	allData := make(map[string]string, len(initialData)+len(postAddData))
	for k, v := range initialData {
		allData[k] = v
	}
	for k, v := range postAddData {
		allData[k] = v
	}

	// Verify the 4th node has all data (initial + post-add).
	waitForNodeData(t, result.tracker, shardID, newReplicaID, allData, membershipTimeout)
	t.Logf("phase 3 complete: node[%d] received 10 post-add entries, total=%d", result.idx, len(allData))

	// Phase 4: Remove the 4th node from the cluster.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := leaderHost.SyncRequestDeleteNode(ctx, shardID, newReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", newReplicaID, err)
	}
	t.Logf("phase 4: requested removal of replica=%d", newReplicaID)

	// Wait for the leader to reflect 3 voting members.
	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 3, membershipTimeout)
	t.Logf("phase 4: membership size confirmed as 3")

	// Stop the removed node so it does not interfere with further
	// operations (it is no longer part of the cluster).
	cluster.stopNode(result.idx)

	// Phase 5: Write more data to the 3-node cluster and verify it
	// still functions correctly after the membership change.
	postRemoveData := proposeNEntries(t, cluster, trackers, shardID, 5, "post-remove")
	for k, v := range postRemoveData {
		allData[k] = v
	}

	// Verify all data (pre-add + post-add + post-remove) via SyncRead
	// on each remaining live node.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		host := cluster.hosts[i]
		for key, expectedVal := range allData {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] SyncRead key=%q", i, key),
				func() bool {
					rCtx, rCancel := context.WithTimeout(context.Background(), defaultTimeout)
					defer rCancel()
					got, err := host.SyncRead(rCtx, shardID, key)
					if err != nil {
						return false
					}
					val, ok := got.(string)
					return ok && val == expectedVal
				},
			)
		}
	}

	t.Logf("phase 5 complete: all %d entries verified via SyncRead on 3 remaining nodes", len(allData))
}

// TestHostAPI_MembershipChange_AddNodeWhileWriting verifies that adding a
// node while continuous writes are in progress does not lose data and the
// new node eventually catches up to a consistent state.
//
// The test exercises:
// - Concurrent proposals during a membership change
// - New node catch-up under write load
// - Data consistency after the membership change settles
func TestHostAPI_MembershipChange_AddNodeWhileWriting(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Write initial data to establish baseline.
	initialData := proposeNEntries(t, cluster, trackers, shardID, 5, "baseline")
	verifyAllData(t, cluster, trackers, shardID, initialData)

	// Write additional entries before adding the node.
	preAddData := proposeNEntries(t, cluster, trackers, shardID, 15, "pre-add-load")
	allData := make(map[string]string, len(initialData)+len(preAddData))
	for k, v := range initialData {
		allData[k] = v
	}
	for k, v := range preAddData {
		allData[k] = v
	}
	t.Logf("wrote 20 entries before adding node")

	// Add the 4th node.
	const newReplicaID uint64 = 4
	result := addNodeToCluster(t, cluster, shardID, newReplicaID, testShardConfig)
	t.Logf("added node[%d] (replica=%d) at %s", result.idx, newReplicaID, result.addr)

	// Wait for the 4-member cluster to stabilize.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 4, membershipTimeout)

	// Write more data after the node was added.
	postAddData := proposeNEntries(t, cluster, trackers, shardID, 10, "post-add-load")
	for k, v := range postAddData {
		allData[k] = v
	}

	// Verify the new node has caught up to all data.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, allData, membershipTimeout)

	// Verify consistency via SyncRead on the leader.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	for key, expectedVal := range allData {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		got, err := leaderHost.SyncRead(ctx, shardID, key)
		cancel()
		if err != nil {
			t.Fatalf("SyncRead(%q) failed: %v", key, err)
		}
		val, ok := got.(string)
		if !ok || val != expectedVal {
			t.Fatalf("SyncRead(%q) = %q, want %q", key, val, expectedVal)
		}
	}

	t.Logf("all %d entries verified via SyncRead after membership addition under load", len(allData))
}

// TestHostAPI_MembershipChange_RemoveFollower verifies that removing a
// follower from a 3-node cluster results in a healthy 2-node cluster
// that continues to accept writes and serve linearizable reads.
//
// The test exercises:
// - Follower removal via SyncRequestDeleteNode
// - Quorum maintenance with 2 nodes
// - Continued write and read operations after removal
func TestHostAPI_MembershipChange_RemoveFollower(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Write initial data.
	initialData := proposeNEntries(t, cluster, trackers, shardID, 5, "pre-remove")
	verifyAllData(t, cluster, trackers, shardID, initialData)

	// Find a follower to remove.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatal("no follower found for removal")
	}
	followerReplicaID := uint64(followerIdx + 1)
	t.Logf("removing follower node[%d] (replica=%d)", followerIdx, followerReplicaID)

	// Request the removal.
	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := leaderHost.SyncRequestDeleteNode(ctx, shardID, followerReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", followerReplicaID, err)
	}

	// Wait for membership to reflect 2 nodes.
	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 2, membershipTimeout)
	t.Logf("membership size confirmed as 2 after removing node[%d]", followerIdx)

	// Mark the removed node as stopped so helpers skip it.
	cluster.stopNode(followerIdx)

	// Write more data to the 2-node cluster.
	postRemoveData := proposeNEntries(t, cluster, trackers, shardID, 5, "post-remove")

	// Verify all data via SyncRead on each remaining live node.
	allData := make(map[string]string, len(initialData)+len(postRemoveData))
	for k, v := range initialData {
		allData[k] = v
	}
	for k, v := range postRemoveData {
		allData[k] = v
	}

	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		host := cluster.hosts[i]
		for key, expectedVal := range allData {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] SyncRead key=%q after follower removal", i, key),
				func() bool {
					rCtx, rCancel := context.WithTimeout(context.Background(), defaultTimeout)
					defer rCancel()
					got, err := host.SyncRead(rCtx, shardID, key)
					if err != nil {
						return false
					}
					val, ok := got.(string)
					return ok && val == expectedVal
				},
			)
		}
	}

	t.Logf("all %d entries verified on 2-node cluster after follower removal", len(allData))
}

// TestHostAPI_MembershipChange_AddRemoveWithSnapshot verifies that a
// newly added node can catch up via snapshot when the log has been
// compacted. Uses aggressive snapshot settings (SnapshotEntries=100,
// CompactionOverhead=10) to force compaction before the new node joins.
//
// The test exercises:
// - Log compaction via automatic snapshots
// - New node catch-up via snapshot transfer (not log replay)
// - Correct data after snapshot-based recovery
func TestHostAPI_MembershipChange_AddRemoveWithSnapshot(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	// Use aggressive snapshot settings to force compaction.
	snapshotCfg := func(sid, rid uint64) config.Config {
		return testShardConfigWithSnapshot(sid, rid, 100, 10)
	}

	trackers := cluster.startShardWithConfig(shardID, snapshotCfg)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d] with SnapshotEntries=100, CompactionOverhead=10", leaderIdx)

	// Write enough entries to trigger at least one snapshot and compaction.
	// With SnapshotEntries=100, we need > 100 entries to trigger a snapshot.
	initialData := proposeNEntries(t, cluster, trackers, shardID, 150, "snap-data")
	verifyAllData(t, cluster, trackers, shardID, initialData)
	t.Logf("wrote 150 entries to trigger snapshots and compaction")

	// Add a 4th node. Because the log has been compacted, the new node
	// must catch up via snapshot transfer.
	const newReplicaID uint64 = 4
	result := addNodeToCluster(t, cluster, shardID, newReplicaID, snapshotCfg)
	t.Logf("added node[%d] (replica=%d) at %s", result.idx, newReplicaID, result.addr)

	// Wait for 4-member membership.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 4, membershipTimeout)

	// Verify the new node has all data (received via snapshot).
	waitForNodeData(t, result.tracker, shardID, newReplicaID, initialData, membershipTimeout)
	t.Logf("node[%d] caught up on all 150 entries via snapshot", result.idx)

	// Write additional entries to verify the expanded cluster works.
	postAddData := proposeNEntries(t, cluster, trackers, shardID, 10, "post-snap-add")
	allData := make(map[string]string, len(initialData)+len(postAddData))
	for k, v := range initialData {
		allData[k] = v
	}
	for k, v := range postAddData {
		allData[k] = v
	}
	waitForNodeData(t, result.tracker, shardID, newReplicaID, allData, membershipTimeout)

	// Remove the 4th node and verify the 3-node cluster continues.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	rmCtx, rmCancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer rmCancel()
	if err := cluster.hosts[leaderIdx].SyncRequestDeleteNode(rmCtx, shardID, newReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", newReplicaID, err)
	}
	cluster.stopNode(result.idx)

	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 3, membershipTimeout)

	// Final write to confirm the 3-node cluster is healthy.
	proposeAndWait(t, cluster, trackers, shardID, "final-check", "ok")

	t.Logf("membership change with snapshot catch-up complete: all data verified")
}
