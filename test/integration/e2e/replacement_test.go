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

package e2e_test

import (
	"fmt"
	"testing"
)

// TestE2E_ReplaceFollower_ThreeNode verifies that a stopped follower in a
// 3-node cluster can be removed via SyncRequestRemoveNode, replaced with a
// new node via SyncRequestAddNode, and that the replacement catches up on
// all existing data and accepts new proposals.
func TestE2E_ReplaceFollower_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Propose 10 entries and wait for full replication across all 3 nodes.
	expected := proposeNEntries(t, cluster, trackers, shardID, 10, "repl3n")
	verifyAllData(t, cluster, trackers, shardID, expected)
	t.Logf("10 entries proposed and replicated to all 3 nodes")

	// Identify a follower to replace.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatal("no follower found for replacement")
	}
	removedReplicaID := uint64(followerIdx + 1)
	t.Logf("replacing follower: node[%d] replicaID=%d", followerIdx, removedReplicaID)

	// Stop the follower.
	cluster.stopNode(followerIdx)

	// Ensure the remaining 2 nodes still have a leader (quorum = 2 of 3).
	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("leader after follower stop: node[%d]", leaderIdx)

	// Remove the stopped follower from the Raft membership.

	syncDeleteNode(t, cluster, shardID, removedReplicaID, membershipTimeout)
	t.Logf("removed replicaID=%d from membership", removedReplicaID)

	// Add a replacement node with a new replicaID (4).
	const replacementReplicaID uint64 = 4
	result := addNodeToCluster(t, cluster, shardID, replacementReplicaID, testShardConfig)
	t.Logf("added replacement node[%d] replicaID=%d at %s", result.idx, replacementReplicaID, result.addr)

	// Wait for the replacement to catch up on all pre-existing data.
	waitForNodeData(t, result.tracker, shardID, replacementReplicaID, expected, membershipTimeout)
	t.Logf("replacement node caught up on all 10 pre-existing entries")

	// Propose 5 more entries through the leader.
	moreExpected := proposeNEntries(t, cluster, trackers, shardID, 5, "repl3n-post")

	// Merge all expected data.
	allExpected := make(map[string]string, len(expected)+len(moreExpected))
	for k, v := range expected {
		allExpected[k] = v
	}
	for k, v := range moreExpected {
		allExpected[k] = v
	}

	// Verify all 15 entries on the two original survivors and the replacement.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		// The replacement node's replicaID does not follow the i+1
		// convention; use the tracker directly for the replacement.
		if i == result.idx {
			waitForNodeData(t, result.tracker, shardID, replacementReplicaID, allExpected, membershipTimeout)
			continue
		}
		nodeSM := trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] state machine not found", i)
		}
		for key, val := range allExpected {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] has key=%q", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	// Assert: the removed replica appears in Membership.Removed. Resolve the
	// leader now rather than reusing one captured before the membership change,
	// which a re-election in between would have deposed.
	membership, err := cluster.hosts[cluster.waitForClusterLeader(shardID, leaderElectionTimeout)].ShardMembership(shardID)
	if err != nil {
		t.Fatalf("ShardMembership failed: %v", err)
	}
	if _, removed := membership.Removed[removedReplicaID]; !removed {
		t.Fatalf("replicaID=%d not found in Membership.Removed: %v", removedReplicaID, membership.Removed)
	}

	// Assert: the replacement replica is in Membership.Nodes.
	if _, present := membership.Nodes[replacementReplicaID]; !present {
		t.Fatalf("replacement replicaID=%d not found in Membership.Nodes: %v", replacementReplicaID, membership.Nodes)
	}

	t.Logf("3-node replacement complete: removed=%d, replacement=%d, total entries=%d",
		removedReplicaID, replacementReplicaID, len(allExpected))
}
