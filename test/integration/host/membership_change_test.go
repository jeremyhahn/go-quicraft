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
	"github.com/jeremyhahn/go-quicraft/test/membership"
)

// TestHostAPI_MembershipChange_AddAndRemoveNode adds a fourth node, checks it has all data and takes writes, then removes it while the other three keep working.
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

	// The leader is the replica that proposed the change, so asking only the
	// leader whether it happened is close to asking nothing. Assert that every
	// live replica, the joiner included, ended up on the same configuration.
	waitForMembershipConverged(t, cluster, shardID,
		[]uint64{1, 2, 3, newReplicaID}, nil, membershipTimeout)
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

	// Phase 4: Remove the 4th node from the cluster, through whichever node
	// leads at each attempt (test/membership).
	membership.DeleteNode(t, cluster.leaderHost, shardID, newReplicaID, membershipTimeout)
	t.Logf("phase 4: requested removal of replica=%d", newReplicaID)

	// Wait for the leader to reflect 3 voting members.
	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 3, membershipTimeout)
	waitForMembershipConverged(t, cluster, shardID, []uint64{1, 2, 3},
		[]uint64{newReplicaID}, membershipTimeout)
	t.Logf("phase 4: membership size confirmed as 3")

	// Stop the removed node.
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
					//quicraft:direct-read every live replica IS the subject: a follower's SyncRead must forward its ReadIndex to the leader
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

// TestHostAPI_MembershipChange_AddRemoveWithSnapshot forces compaction (SnapshotEntries=100, CompactionOverhead=10) so a new node catches up by snapshot.
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

	// A snapshot-caught-up joiner rebuilt its configuration from the snapshot; it must match everyone else's.
	waitForMembershipConverged(t, cluster, shardID,
		[]uint64{1, 2, 3, newReplicaID}, nil, membershipTimeout)

	// Verify the new node has all data (received via snapshot).
	waitForNodeData(t, result.tracker, shardID, newReplicaID, initialData, membershipTimeout)

	// The key set alone would be identical under plain log replay. The joiner
	// starts empty, so a restore from a snapshot can only have come from an
	// InstallSnapshot streamed by the leader — which is the claim in this
	// test's name and doc comment.
	joinerSM := result.tracker.Get(shardID, newReplicaID)
	if joinerSM == nil {
		t.Fatalf("node[%d] (replica %d) has no state machine after catch-up", result.idx, newReplicaID)
	}
	if joinerSM.Recoveries() == 0 {
		t.Fatalf("node[%d] (replica %d) never restored from a snapshot: it caught up "+
			"by log replay, so this test did not exercise snapshot catch-up",
			result.idx, newReplicaID)
	}
	t.Logf("node[%d] caught up on all 150 entries via snapshot (%d snapshot restores)",
		result.idx, joinerSM.Recoveries())

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
	membership.DeleteNode(t, cluster.leaderHost, shardID, newReplicaID, membershipTimeout)
	cluster.stopNode(result.idx)

	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 3, membershipTimeout)
	waitForMembershipConverged(t, cluster, shardID, []uint64{1, 2, 3},
		[]uint64{newReplicaID}, membershipTimeout)

	// Final write to confirm the 3-node cluster is healthy.
	proposeAndWait(t, cluster, trackers, shardID, "final-check", "ok")

	t.Logf("membership change with snapshot catch-up complete: all data verified")
}

// TestHostAPI_MembershipChange_RemoveFollowerFromTwoVoters: removing B from a two-voter shard commits through B's own ack, and the survivor serves alone.
func TestHostAPI_MembershipChange_RemoveFollowerFromTwoVoters(t *testing.T) {
	cluster := newTestCluster(t, 2)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1
	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	initialData := proposeNEntries(t, cluster, trackers, shardID, 3, "two-voters")
	verifyAllData(t, cluster, trackers, shardID, initialData)

	followerIdx := 1 - leaderIdx
	followerReplicaID := uint64(followerIdx + 1)
	membership.DeleteNode(t, cluster.leaderHost, shardID, followerReplicaID, membershipTimeout)

	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 1, membershipTimeout)
	cluster.stopNode(followerIdx)

	// The lone survivor leads and serves.
	postRemoveData := proposeNEntries(t, cluster, trackers, shardID, 3, "lone-voter")
	for key, expectedVal := range postRemoveData {
		waitForCondition(t, membershipTimeout, fmt.Sprintf("survivor SyncRead key=%q", key), func() bool {
			rCtx, rCancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer rCancel()
			//quicraft:direct-read the lone survivor IS the subject
			got, err := cluster.hosts[leaderIdx].SyncRead(rCtx, shardID, key)
			if err != nil {
				return false
			}
			val, ok := got.(string)
			return ok && val == expectedVal
		})
	}
}
