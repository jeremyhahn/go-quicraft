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

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestE2E_AddRemove_RoundTrip verifies a complete membership change cycle:
// add a node (3->4), verify data, remove the added node (4->3), and verify
// the cluster remains stable and serves all data correctly.
func TestE2E_AddRemove_RoundTrip(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Phase 1: Propose 5 entries with the initial 3-node cluster.
	phase1Expected := proposeNEntries(t, cluster, trackers, shardID, 5, "phase1")
	verifyAllData(t, cluster, trackers, shardID, phase1Expected)
	t.Logf("phase 1: 5 entries replicated on 3 nodes")

	// Phase 2: Add a 4th node, propose 5 more, verify on all 4.
	const addedReplicaID uint64 = 4
	result := addNodeToCluster(t, cluster, shardID, addedReplicaID, testShardConfig)
	t.Logf("phase 2: added node[%d] as replica %d", result.idx, addedReplicaID)

	// Wait for the new node to catch up on phase 1 data.
	waitForNodeData(t, result.tracker, shardID, addedReplicaID, phase1Expected, membershipTimeout)

	phase2Expected := proposeNEntries(t, cluster, trackers, shardID, 5, "phase2")
	allExpected := make(map[string]string, len(phase1Expected)+len(phase2Expected))
	for k, v := range phase1Expected {
		allExpected[k] = v
	}
	for k, v := range phase2Expected {
		allExpected[k] = v
	}

	// Verify all 10 entries on original 3 nodes.
	verifyAllData(t, cluster, trackers, shardID, allExpected)

	// Verify the added node also has all 10 entries.
	waitForNodeData(t, result.tracker, shardID, addedReplicaID, allExpected, membershipTimeout)
	t.Logf("phase 2: all 10 entries verified on 4 nodes")

	// Verify membership is 4 before removal, on every replica rather than only
	// on the leader.
	//
	// Querying the leader alone is not enough. A joining node does not learn
	// the full membership only from a snapshot: initial membership is a
	// committed EntryInitialMembership at log index 1, and a joiner replays that entry like any other and ends up
	// with the same configuration as everyone else. Asserting it here is what
	// keeps that guarantee honest — if a joiner ever again comes up knowing
	// only its own AddNode, this fails instead of being documented around.
	leaderIdx = cluster.findLeader(shardID)
	if leaderIdx < 0 {
		t.Fatalf("no leader found for membership verification")
	}
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 4, membershipTimeout)
	waitForMembershipConverged(t, cluster, shardID,
		[]uint64{1, 2, 3, addedReplicaID}, nil, membershipTimeout)

	// Phase 3: Remove the added node (4->3), propose 5 more, verify on remaining 3.
	leaderIdx = cluster.findLeader(shardID)
	if leaderIdx < 0 {
		t.Fatalf("no leader found for removal")
	}

	syncDeleteNode(t, cluster, shardID, addedReplicaID, membershipTimeout)

	cluster.stopNode(result.idx)
	t.Logf("phase 3: removed and stopped node[%d] (replica %d)", result.idx, addedReplicaID)

	// Wait for membership to shrink to 3.
	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 3, membershipTimeout)
	waitForMembershipConverged(t, cluster, shardID, []uint64{1, 2, 3},
		[]uint64{addedReplicaID}, membershipTimeout)

	phase3Expected := proposeNEntries(t, cluster, trackers, shardID, 5, "phase3")
	for k, v := range phase3Expected {
		allExpected[k] = v
	}

	// Verify all 15 entries on the remaining 3 nodes.
	verifyAllData(t, cluster, trackers, shardID, allExpected)

	// Final stability check: verify exact membership.
	m, err := cluster.hosts[leaderIdx].ShardMembership(shardID)
	if err != nil {
		t.Fatalf("ShardMembership failed: %v", err)
	}
	if len(m.Nodes) != 3 {
		t.Errorf("expected 3 voting nodes after round trip, got %d: %v", len(m.Nodes), m.Nodes)
	}
	if _, removed := m.Removed[addedReplicaID]; !removed {
		t.Errorf("replicaID %d not found in Removed set after round trip", addedReplicaID)
	}

	t.Logf("round trip complete: 3->4->3 with all %d entries intact", len(allExpected))
}

// TestE2E_RemoveNode_ThenPropose verifies that a 3-node cluster can remove
// a follower and continue accepting proposals with the remaining 2-node
// quorum. This exercises the Raft single-configuration-change safety
// property per PhD Section 4.1: after removing one node from a 3-node
// cluster, quorum drops from 2 to 2 (since the membership is now 2 nodes).
func TestE2E_RemoveNode_ThenPropose(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup: propose 1 entry after election (PhD 6.4 requires a committed
	// entry at the current term before serving reads or processing config
	// changes reliably).
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")
	waitForReplication(t, cluster, trackers, shardID, "warmup", "ok", defaultTimeout)
	t.Logf("warmup entry replicated to all 3 nodes")

	// Find a follower and remove it.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatalf("no follower found for removal")
	}
	removedReplicaID := uint64(followerIdx + 1)
	t.Logf("removing follower node[%d] replicaID=%d", followerIdx, removedReplicaID)

	syncDeleteNode(t, cluster, shardID, removedReplicaID, membershipTimeout)

	cluster.stopNode(followerIdx)
	t.Logf("stopped removed node[%d]", followerIdx)

	// Wait for the 2-node cluster to stabilize with a leader.
	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 2, membershipTimeout)

	// Both survivors must agree that the shard is now two voters. This is the
	// case where a divergent follower is most dangerous: with a 3-voter
	// configuration still in its head, a survivor sizes quorum at 2 out of 3
	// and would count a vote from a replica that no longer exists.
	var survivors []uint64
	for i := range cluster.hosts {
		if replicaID := uint64(i + 1); replicaID != removedReplicaID {
			survivors = append(survivors, replicaID)
		}
	}
	waitForMembershipConverged(t, cluster, shardID, survivors,
		[]uint64{removedReplicaID}, membershipTimeout)
	t.Logf("cluster stabilized with 2 nodes, leader at node[%d]", leaderIdx)

	// Propose 10 entries on the 2-node cluster (quorum=2).
	expected := proposeNEntries(t, cluster, trackers, shardID, 10, "post-remove")
	expected["warmup"] = "ok"

	// Verify all entries on the 2 remaining nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] has no state machine", i)
		}
		for key, val := range expected {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] has key=%q", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	// Verify membership state.
	m, err := cluster.hosts[leaderIdx].ShardMembership(shardID)
	if err != nil {
		t.Fatalf("ShardMembership failed: %v", err)
	}
	if len(m.Nodes) != 2 {
		t.Errorf("expected 2 voting nodes, got %d: %v", len(m.Nodes), m.Nodes)
	}
	if _, removed := m.Removed[removedReplicaID]; !removed {
		t.Errorf("replicaID %d not found in Membership.Removed: %v", removedReplicaID, m.Removed)
	}

	t.Logf("2-node cluster accepted all %d entries after removal of replica %d", len(expected), removedReplicaID)
}

// Compile-time assertion: ensure testShardConfig matches the signature
// expected by addNodeToCluster's cfgFn parameter.
var _ func(uint64, uint64) config.Config = testShardConfig
