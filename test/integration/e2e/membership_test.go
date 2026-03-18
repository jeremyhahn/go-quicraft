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
	"context"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestE2E_AddNode_ThreeToFour verifies that a 4th node can be dynamically
// added to a running 3-node cluster, catches up on existing data via
// snapshot or log replication, and participates in subsequent proposals.
func TestE2E_AddNode_ThreeToFour(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Propose 10 entries before adding the new node.
	expected := proposeNEntries(t, cluster, trackers, shardID, 10, "add3to4")
	t.Logf("proposed 10 entries, verifying replication on all 3 nodes")
	verifyAllData(t, cluster, trackers, shardID, expected)

	// Add a 4th node with replicaID=4.
	const newReplicaID uint64 = 4
	result := addNodeToCluster(t, cluster, shardID, newReplicaID, testShardConfig)
	t.Logf("added node[%d] at %s as replica %d", result.idx, result.addr, newReplicaID)

	// Wait for the new node to catch up on all 10 existing entries.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, expected, membershipTimeout)
	t.Logf("node[%d] caught up on all 10 entries", result.idx)

	// Propose 1 more entry and verify replication to all 4 nodes.
	proposeAndWait(t, cluster, trackers, shardID, "add3to4-extra", "extra-value")
	expected["add3to4-extra"] = "extra-value"

	// The new node's tracker is separate from the cluster.trackers slice used
	// by waitForReplication. Wait for the new node explicitly, then verify
	// the original 3 nodes via the standard helper.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, expected, membershipTimeout)
	waitForReplication(t, cluster, trackers, shardID, "add3to4-extra", "extra-value", defaultTimeout)

	// Verify membership size is 4 from the leader's perspective.
	leaderIdx = cluster.findLeader(shardID)
	if leaderIdx < 0 {
		t.Fatalf("no leader found after adding node")
	}
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 4, membershipTimeout)

	m, err := cluster.hosts[leaderIdx].GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership failed: %v", err)
	}
	if len(m.Nodes) != 4 {
		t.Errorf("expected 4 voting nodes, got %d: %v", len(m.Nodes), m.Nodes)
	}

	// Verify the new node has all 11 entries.
	newNodeSM := result.tracker.Get(shardID, newReplicaID)
	if newNodeSM == nil {
		t.Fatalf("new node state machine not registered")
	}
	if newNodeSM.Len() < len(expected) {
		t.Errorf("new node has %d entries, expected at least %d", newNodeSM.Len(), len(expected))
	}

	t.Logf("membership size confirmed: %d nodes, all %d entries replicated", len(m.Nodes), len(expected))
}

// TestE2E_RemoveNode_FourToThree verifies that a follower can be removed
// from a 4-node cluster via SyncRequestDeleteNode, the cluster shrinks
// to 3 nodes, and subsequent proposals succeed on the remaining nodes.
func TestE2E_RemoveNode_FourToThree(t *testing.T) {
	cluster := newTestCluster(t, 4)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Propose 5 entries and verify full replication.
	expected := proposeNEntries(t, cluster, trackers, shardID, 5, "rm4to3")
	verifyAllData(t, cluster, trackers, shardID, expected)
	t.Logf("5 entries replicated to all 4 nodes")

	// Find a follower to remove.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatalf("no follower found for removal")
	}
	removedReplicaID := uint64(followerIdx + 1)
	t.Logf("removing follower node[%d] replicaID=%d", followerIdx, removedReplicaID)

	// Request node removal through the leader.
	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := cluster.hosts[leaderIdx].SyncRequestDeleteNode(ctx, shardID, removedReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", removedReplicaID, err)
	}

	// Stop the removed node.
	cluster.stopNode(followerIdx)
	t.Logf("stopped removed node[%d]", followerIdx)

	// Wait for membership to reflect 3 active nodes.
	// Find a live node to query membership from.
	var liveIdx int
	for i := range cluster.hosts {
		if !cluster.stopped[i].Load() {
			liveIdx = i
			break
		}
	}
	waitForMembershipSize(t, cluster.hosts[liveIdx], shardID, 3, membershipTimeout)

	// Propose 5 more entries with a different prefix and verify on remaining 3.
	postRemoveExpected := proposeNEntries(t, cluster, trackers, shardID, 5, "rm4to3-post")
	verifyAllData(t, cluster, trackers, shardID, postRemoveExpected)
	t.Logf("5 post-removal entries replicated to remaining 3 nodes")

	// Verify the removed replica ID appears in Membership.Removed.
	m, err := cluster.hosts[liveIdx].GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership failed: %v", err)
	}
	if _, removed := m.Removed[removedReplicaID]; !removed {
		t.Errorf("replicaID %d not found in Membership.Removed: %v", removedReplicaID, m.Removed)
	}
	if _, stillActive := m.Nodes[removedReplicaID]; stillActive {
		t.Errorf("replicaID %d still in Membership.Nodes after removal", removedReplicaID)
	}

	t.Logf("cluster shrunk to %d nodes, removed replica %d confirmed in Removed set", len(m.Nodes), removedReplicaID)
}

// TestE2E_AddNode_FiveToSix verifies that a 5-node cluster can scale to
// 6 nodes. The new node catches up on 20 pre-existing entries and
// participates in replicating 5 additional entries.
func TestE2E_AddNode_FiveToSix(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Propose 20 entries and verify replication on all 5 nodes.
	expected := proposeNEntries(t, cluster, trackers, shardID, 20, "add5to6")
	verifyAllData(t, cluster, trackers, shardID, expected)
	t.Logf("20 entries replicated to all 5 nodes")

	// Add a 6th node with replicaID=6.
	const newReplicaID uint64 = 6
	result := addNodeToCluster(t, cluster, shardID, newReplicaID, testShardConfig)
	t.Logf("added node[%d] at %s as replica %d", result.idx, result.addr, newReplicaID)

	// Wait for the new node to catch up on all 20 entries.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, expected, membershipTimeout)
	t.Logf("node[%d] caught up on all 20 entries", result.idx)

	// Propose 5 more entries and verify all 25 on all 6 nodes.
	postExpected := proposeNEntries(t, cluster, trackers, shardID, 5, "add5to6-post")
	for k, v := range postExpected {
		expected[k] = v
	}

	// Verify the original 5 nodes have all 25 entries.
	verifyAllData(t, cluster, trackers, shardID, expected)

	// Verify the new node also has all 25 entries.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, expected, membershipTimeout)

	// Verify membership size is 6.
	leaderIdx = cluster.findLeader(shardID)
	if leaderIdx < 0 {
		t.Fatalf("no leader found after adding node")
	}
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 6, membershipTimeout)

	m, err := cluster.hosts[leaderIdx].GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership failed: %v", err)
	}
	if len(m.Nodes) != 6 {
		t.Errorf("expected 6 voting nodes, got %d: %v", len(m.Nodes), m.Nodes)
	}

	t.Logf("cluster expanded to %d nodes with all %d entries replicated", len(m.Nodes), len(expected))
}

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

	// Verify membership is 4 before removal. Query the leader (an existing
	// node) because the joining node only learns full membership via snapshot.
	// Without log compaction triggering a snapshot, the joining node's
	// membership only contains config change entries from the log (its own
	// AddNode), not the bootstrap members that were set via SetMembership.
	// This is consistent with TestE2E_AddNode_ThreeToFour and
	// TestE2E_AddNode_FiveToSix which also query the leader.
	leaderIdx = cluster.findLeader(shardID)
	if leaderIdx < 0 {
		t.Fatalf("no leader found for membership verification")
	}
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 4, membershipTimeout)

	// Phase 3: Remove the added node (4->3), propose 5 more, verify on remaining 3.
	leaderIdx = cluster.findLeader(shardID)
	if leaderIdx < 0 {
		t.Fatalf("no leader found for removal")
	}

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := cluster.hosts[leaderIdx].SyncRequestDeleteNode(ctx, shardID, addedReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", addedReplicaID, err)
	}

	cluster.stopNode(result.idx)
	t.Logf("phase 3: removed and stopped node[%d] (replica %d)", result.idx, addedReplicaID)

	// Wait for membership to shrink to 3.
	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 3, membershipTimeout)

	phase3Expected := proposeNEntries(t, cluster, trackers, shardID, 5, "phase3")
	for k, v := range phase3Expected {
		allExpected[k] = v
	}

	// Verify all 15 entries on the remaining 3 nodes.
	verifyAllData(t, cluster, trackers, shardID, allExpected)

	// Final stability check: verify exact membership.
	m, err := cluster.hosts[leaderIdx].GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership failed: %v", err)
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

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := cluster.hosts[leaderIdx].SyncRequestDeleteNode(ctx, shardID, removedReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", removedReplicaID, err)
	}

	cluster.stopNode(followerIdx)
	t.Logf("stopped removed node[%d]", followerIdx)

	// Wait for the 2-node cluster to stabilize with a leader.
	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 2, membershipTimeout)
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
	m, err := cluster.hosts[leaderIdx].GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership failed: %v", err)
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
