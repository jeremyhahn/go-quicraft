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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestE2E_SnapshotCatchUp_NewNode verifies that a new node joining a
// 3-node cluster after log compaction receives a snapshot transfer and
// catches up with the full state. With SnapshotEntries=20 and
// CompactionOverhead=5, the log is compacted after 20 entries leaving
// only ~5 trailing entries. A new node joining after 50 entries cannot
// receive the compacted log entries and must catch up via snapshot.
func TestE2E_SnapshotCatchUp_NewNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) config.Config {
		return testShardConfigWithSnapshot(shardID, replicaID, 20, 5)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected, proposing 50 entries to trigger snapshot and compaction")

	// Propose 50 entries. Snapshots trigger at entries 20 and 40,
	// compaction removes entries below ~35 (snapshot index - overhead).
	expected := proposeNEntries(t, cluster, trackers, 1, 50, "snap")
	t.Logf("proposed 50 entries, verifying replication on original 3 nodes")

	// Verify all 3 original nodes have the complete dataset.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all 50 entries replicated, adding new node (replicaID=4)")

	// Add a 4th node. The early log entries have been compacted, so the
	// new node must receive a snapshot transfer to catch up.
	result := addNodeToCluster(t, cluster, 1, 4, snapshotCfg)

	// Wait for the new node's membership to be recognized.
	waitForMembershipSize(t, result.host, 1, 4, membershipTimeout)

	// Verify all 50 entries are present on the new node via snapshot catch-up.
	waitForNodeData(t, result.tracker, 1, 4, expected, membershipTimeout)
	t.Logf("new node (replicaID=4) caught up with all 50 entries via snapshot")

	// Propose 10 more entries and verify the new node receives them
	// through normal log replication (post-snapshot).
	moreExpected := proposeNEntries(t, cluster, trackers, 1, 10, "post-snap")
	t.Logf("proposed 10 additional entries after node join")

	// Merge expected maps for full verification.
	for k, v := range moreExpected {
		expected[k] = v
	}

	// Verify all 60 entries are present on the new node.
	waitForNodeData(t, result.tracker, 1, 4, expected, membershipTimeout)

	// Verify all 60 entries on the original 3 nodes as well.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all 60 entries verified on all 4 nodes (snapshot catch-up + normal replication)")
}

// TestE2E_SnapshotCatchUp_DataIntegrity verifies that data transferred
// via snapshot to a new node is byte-for-byte identical to the leader's
// state machine. This ensures no data corruption or loss during the
// snapshot save, transfer, and recovery pipeline.
func TestE2E_SnapshotCatchUp_DataIntegrity(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) config.Config {
		return testShardConfigWithSnapshot(shardID, replicaID, 15, 5)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d], proposing 40 entries with varied keys", leaderIdx)

	// Propose 40 entries with the "integrity" prefix. Snapshots trigger
	// at entries 15 and 30, compaction removes entries below ~25.
	expected := proposeNEntries(t, cluster, trackers, 1, 40, "integrity")
	t.Logf("proposed 40 entries, verifying replication on original 3 nodes")

	// Verify replication before adding the new node.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all 40 entries replicated, adding new node (replicaID=4)")

	// Add a 4th node that must catch up via snapshot.
	result := addNodeToCluster(t, cluster, 1, 4, snapshotCfg)

	// Wait for the new node to receive and apply the snapshot.
	waitForNodeData(t, result.tracker, 1, 4, expected, membershipTimeout)
	t.Logf("new node caught up, comparing state machine snapshots for data integrity")

	// Re-find the leader since it may have changed during membership change.
	leaderIdx = cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderReplicaID := uint64(leaderIdx + 1)

	// Get the leader's state machine snapshot.
	leaderSM := trackers[leaderIdx].Get(1, leaderReplicaID)
	if leaderSM == nil {
		t.Fatalf("leader state machine not found at node[%d] replicaID=%d", leaderIdx, leaderReplicaID)
	}
	leaderData := leaderSM.Snapshot()

	// Get the new node's state machine snapshot.
	newNodeSM := result.tracker.Get(1, 4)
	if newNodeSM == nil {
		t.Fatal("new node state machine not found for replicaID=4")
	}
	newNodeData := newNodeSM.Snapshot()

	// Compare: every key in the leader must exist in the new node with
	// the same value.
	if len(leaderData) != len(newNodeData) {
		t.Fatalf("state machine size mismatch: leader has %d entries, new node has %d entries",
			len(leaderData), len(newNodeData))
	}

	for key, leaderVal := range leaderData {
		newNodeVal, ok := newNodeData[key]
		if !ok {
			t.Fatalf("new node missing key %q that exists on leader", key)
		}
		if leaderVal != newNodeVal {
			t.Fatalf("data mismatch for key %q: leader=%q, new node=%q", key, leaderVal, newNodeVal)
		}
	}

	// Reverse check: every key in the new node must exist in the leader.
	for key := range newNodeData {
		if _, ok := leaderData[key]; !ok {
			t.Fatalf("new node has extra key %q not present on leader", key)
		}
	}

	t.Logf("data integrity verified: %d entries match between leader (node[%d]) and new node (replicaID=4)",
		len(leaderData), leaderIdx)
}

// TestE2E_SnapshotCatchUp_FiveNode verifies snapshot-based catch-up in a
// 5-node cluster. After proposing 60 entries (triggering snapshots at 20,
// 40, and 60 with compaction), a 6th node joins and must receive a
// snapshot to obtain the compacted history. Additional entries are then
// proposed and verified on all 6 nodes.
func TestE2E_SnapshotCatchUp_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) config.Config {
		return testShardConfigWithSnapshot(shardID, replicaID, 20, 5)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("5-node cluster leader elected, proposing 60 entries")

	// Propose 60 entries. Snapshots trigger at entries 20, 40, and 60,
	// compaction removes entries below ~55 (snapshot index - overhead).
	expected := proposeNEntries(t, cluster, trackers, 1, 60, "snap5n")
	t.Logf("proposed 60 entries, verifying replication on all 5 nodes")

	// Verify all 5 original nodes have the complete dataset.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all 60 entries replicated across 5 nodes, adding 6th node (replicaID=6)")

	// Add a 6th node. The compacted log forces snapshot-based catch-up.
	result := addNodeToCluster(t, cluster, 1, 6, snapshotCfg)

	// Wait for the new node's membership to be recognized.
	waitForMembershipSize(t, result.host, 1, 6, membershipTimeout)

	// Verify all 60 entries are present on the 6th node.
	waitForNodeData(t, result.tracker, 1, 6, expected, membershipTimeout)
	t.Logf("6th node caught up with all 60 entries via snapshot")

	// Propose 10 more entries and verify normal replication to all 6 nodes.
	moreExpected := proposeNEntries(t, cluster, trackers, 1, 10, "post-snap5n")
	t.Logf("proposed 10 additional entries after 6th node join")

	// Merge expected maps.
	for k, v := range moreExpected {
		expected[k] = v
	}

	// Verify all 70 entries on the 6th node.
	waitForNodeData(t, result.tracker, 1, 6, expected, membershipTimeout)

	// Verify all 70 entries on all original 5 nodes.
	verifyAllData(t, cluster, trackers, 1, expected)

	// Final sanity check: verify the entry count on every node.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		var replicaID uint64
		if i < 5 {
			replicaID = uint64(i + 1)
		} else {
			replicaID = 6
		}

		var tracker *smTracker
		if i < len(trackers) {
			tracker = trackers[i]
		} else {
			tracker = result.tracker
		}

		nodeSM := tracker.Get(1, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] replicaID=%d has no state machine", i, replicaID)
		}
		entryCount := nodeSM.Len()
		if entryCount != len(expected) {
			t.Fatalf("node[%d] replicaID=%d has %d entries, want %d",
				i, replicaID, entryCount, len(expected))
		}
	}

	t.Logf("all 70 entries verified on all 6 nodes (%d entries each)", len(expected))
}
