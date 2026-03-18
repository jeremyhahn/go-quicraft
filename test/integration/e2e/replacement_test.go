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
)

// TestE2E_ReplaceFollower_ThreeNode verifies that a stopped follower in a
// 3-node cluster can be removed via SyncRequestDeleteNode, replaced with a
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
	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	leaderHost := cluster.hosts[leaderIdx]
	if err := leaderHost.SyncRequestDeleteNode(ctx, shardID, removedReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", removedReplicaID, err)
	}
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

	// Assert: the removed replica appears in Membership.Removed.
	membership, err := leaderHost.GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership failed: %v", err)
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

// TestE2E_ReplaceFollower_FiveNode verifies follower replacement at 5-node
// scale. A follower is stopped, removed, replaced with a new node, and the
// cluster continues operating with all data intact.
func TestE2E_ReplaceFollower_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Propose 10 entries and replicate to all 5 nodes.
	expected := proposeNEntries(t, cluster, trackers, shardID, 10, "repl5n")
	verifyAllData(t, cluster, trackers, shardID, expected)
	t.Logf("10 entries proposed and replicated to all 5 nodes")

	// Identify a follower to replace.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatal("no follower found for replacement")
	}
	removedReplicaID := uint64(followerIdx + 1)
	t.Logf("replacing follower: node[%d] replicaID=%d", followerIdx, removedReplicaID)

	// Stop and remove the follower.
	cluster.stopNode(followerIdx)

	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := leaderHost.SyncRequestDeleteNode(ctx, shardID, removedReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", removedReplicaID, err)
	}
	t.Logf("removed replicaID=%d from membership", removedReplicaID)

	// Add replacement node with replicaID=6.
	const replacementReplicaID uint64 = 6
	result := addNodeToCluster(t, cluster, shardID, replacementReplicaID, testShardConfig)
	t.Logf("added replacement node[%d] replicaID=%d at %s", result.idx, replacementReplicaID, result.addr)

	// Wait for catch-up.
	waitForNodeData(t, result.tracker, shardID, replacementReplicaID, expected, membershipTimeout)
	t.Logf("replacement node caught up on all 10 pre-existing entries")

	// Propose 5 more entries.
	moreExpected := proposeNEntries(t, cluster, trackers, shardID, 5, "repl5n-post")

	// Merge all expected data.
	allExpected := make(map[string]string, len(expected)+len(moreExpected))
	for k, v := range expected {
		allExpected[k] = v
	}
	for k, v := range moreExpected {
		allExpected[k] = v
	}

	// Verify all 15 entries on all 5 live nodes (4 original survivors + replacement).
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		if i == result.idx {
			waitForNodeData(t, result.tracker, shardID, replacementReplicaID, allExpected, membershipTimeout)
			continue
		}
		replicaID := uint64(i + 1)
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

	// Assert: membership is correct.
	membership, err := leaderHost.GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership failed: %v", err)
	}
	if _, removed := membership.Removed[removedReplicaID]; !removed {
		t.Fatalf("replicaID=%d not found in Membership.Removed: %v", removedReplicaID, membership.Removed)
	}
	if _, present := membership.Nodes[replacementReplicaID]; !present {
		t.Fatalf("replacement replicaID=%d not found in Membership.Nodes: %v", replacementReplicaID, membership.Nodes)
	}
	// Cluster should have 5 voting members (4 originals + 1 replacement).
	if len(membership.Nodes) != 5 {
		t.Fatalf("expected 5 voting members, got %d: %v", len(membership.Nodes), membership.Nodes)
	}

	t.Logf("5-node replacement complete: removed=%d, replacement=%d, total entries=%d",
		removedReplicaID, replacementReplicaID, len(allExpected))
}

// TestE2E_ReplaceNode_DataIntegrity verifies zero data loss during node
// replacement by writing 150 entries before the replacement and confirming
// every single entry is present on the replacement node after catch-up.
func TestE2E_ReplaceNode_DataIntegrity(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Propose a substantial number of entries to stress snapshot/log transfer.
	expected := proposeNEntries(t, cluster, trackers, shardID, 150, "integrity")
	t.Logf("150 entries proposed")

	// Wait for full replication before starting the replacement.
	verifyAllData(t, cluster, trackers, shardID, expected)
	t.Logf("150 entries replicated to all 3 nodes")

	// Stop a follower, remove it, and add a replacement.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatal("no follower found for replacement")
	}
	removedReplicaID := uint64(followerIdx + 1)
	t.Logf("replacing follower: node[%d] replicaID=%d", followerIdx, removedReplicaID)

	cluster.stopNode(followerIdx)

	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := leaderHost.SyncRequestDeleteNode(ctx, shardID, removedReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", removedReplicaID, err)
	}

	const replacementReplicaID uint64 = 4
	result := addNodeToCluster(t, cluster, shardID, replacementReplicaID, testShardConfig)
	t.Logf("added replacement node[%d] replicaID=%d", result.idx, replacementReplicaID)

	// Wait for the replacement to receive ALL 150 entries.
	waitForNodeData(t, result.tracker, shardID, replacementReplicaID, expected, membershipTimeout)
	t.Logf("replacement node received all 150 entries")

	// Exhaustive verification: confirm every key individually to assert
	// zero data loss on the replacement node.
	replacementSM := result.tracker.Get(shardID, replacementReplicaID)
	if replacementSM == nil {
		t.Fatal("replacement state machine not found after waitForNodeData")
	}
	missingKeys := make([]string, 0)
	mismatchKeys := make([]string, 0)
	for key, val := range expected {
		got, ok := replacementSM.Get(key)
		if !ok {
			missingKeys = append(missingKeys, key)
			continue
		}
		if got != val {
			mismatchKeys = append(mismatchKeys, fmt.Sprintf("%s: got=%q want=%q", key, got, val))
		}
	}
	if len(missingKeys) > 0 {
		t.Fatalf("replacement node missing %d/%d keys: first 5: %v",
			len(missingKeys), len(expected), missingKeys[:min(5, len(missingKeys))])
	}
	if len(mismatchKeys) > 0 {
		t.Fatalf("replacement node has %d mismatched values: first 5: %v",
			len(mismatchKeys), mismatchKeys[:min(5, len(mismatchKeys))])
	}

	t.Logf("data integrity verified: all 150 entries present on replacement node with zero data loss")
}

// TestE2E_ReplaceNode_ContinuedWrites verifies that a cluster continues
// accepting writes after a node replacement. Phase 1 writes 50 entries
// before the replacement, phase 2 writes 50 entries after, and all 100
// entries are verified on both the replacement and the original survivors.
func TestE2E_ReplaceNode_ContinuedWrites(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Phase 1: propose 50 entries before replacement.
	phase1Expected := proposeNEntries(t, cluster, trackers, shardID, 50, "phase1")
	verifyAllData(t, cluster, trackers, shardID, phase1Expected)
	t.Logf("phase 1 complete: 50 entries replicated")

	// Stop a follower, remove it, and add replacement.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatal("no follower found for replacement")
	}
	removedReplicaID := uint64(followerIdx + 1)
	t.Logf("replacing follower: node[%d] replicaID=%d", followerIdx, removedReplicaID)

	cluster.stopNode(followerIdx)

	leaderIdx = cluster.waitForClusterLeader(shardID, failoverTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := leaderHost.SyncRequestDeleteNode(ctx, shardID, removedReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", removedReplicaID, err)
	}

	const replacementReplicaID uint64 = 4
	result := addNodeToCluster(t, cluster, shardID, replacementReplicaID, testShardConfig)
	t.Logf("added replacement node[%d] replicaID=%d", result.idx, replacementReplicaID)

	// Wait for the replacement to catch up on phase 1 data.
	waitForNodeData(t, result.tracker, shardID, replacementReplicaID, phase1Expected, membershipTimeout)
	t.Logf("replacement node caught up on phase 1 data")

	// Phase 2: propose 50 more entries after replacement.
	phase2Expected := proposeNEntries(t, cluster, trackers, shardID, 50, "phase2")
	t.Logf("phase 2 complete: 50 more entries proposed")

	// Merge both phases into the complete expected data set.
	allExpected := make(map[string]string, len(phase1Expected)+len(phase2Expected))
	for k, v := range phase1Expected {
		allExpected[k] = v
	}
	for k, v := range phase2Expected {
		allExpected[k] = v
	}

	// Verify all 100 entries on the replacement node.
	waitForNodeData(t, result.tracker, shardID, replacementReplicaID, allExpected, membershipTimeout)
	t.Logf("replacement node has all 100 entries")

	// Verify all 100 entries on the original surviving nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || i == result.idx {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] state machine not found", i)
		}
		for key, val := range allExpected {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("survivor node[%d] has key=%q", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
		t.Logf("survivor node[%d] verified with all 100 entries", i)
	}

	t.Logf("continued writes after replacement verified: 100 total entries across all live nodes")
}
