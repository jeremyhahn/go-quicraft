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

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestE2E_Barrier_ReplaceNode verifies that a follower in a 3-node
// encrypted barrier cluster can be stopped, removed from the membership,
// and replaced by a new node with an independent barrier. The replacement
// node must catch up on all previously committed data via encrypted
// replication and then continue accepting new proposals.
func TestE2E_Barrier_ReplaceNode(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Propose 20 entries through the leader and wait for full replication.
	expected := proposeNEntries(t, cluster.testCluster, trackers, shardID, 20, "brepl")
	verifyAllData(t, cluster.testCluster, trackers, shardID, expected)
	t.Logf("20 entries proposed and verified on all 3 barrier nodes")

	// Identify a follower to replace.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatal("no follower found for replacement")
	}
	removedReplicaID := uint64(followerIdx + 1)
	t.Logf("replacing follower node[%d] replicaID=%d", followerIdx, removedReplicaID)

	// Stop the follower and remove it from the Raft membership.
	cluster.stopNode(followerIdx)

	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := leaderHost.SyncRequestDeleteNode(ctx, shardID, removedReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", removedReplicaID, err)
	}
	t.Logf("removed replica %d from membership", removedReplicaID)

	// Create a replacement node with an independent barrier.
	b := newBarrierForNode(t)
	result := addNodeToCluster(t, cluster.testCluster, shardID, 4, testShardConfig, quicraft.WithBarrier(b))
	t.Logf("replacement node[%d] added at %s as replicaID=4", result.idx, result.addr)

	// Wait for the replacement node to catch up on all 20 entries.
	waitForNodeData(t, result.tracker, shardID, 4, expected, membershipTimeout)
	t.Logf("replacement node caught up on all 20 entries via encrypted replication")

	// Propose 10 more entries and verify on all 3 live nodes.
	moreExpected := proposeNEntries(t, cluster.testCluster, trackers, shardID, 10, "brepl-post")
	for k, v := range expected {
		moreExpected[k] = v
	}

	// Verify the full 30 entries on all live nodes including the replacement.
	verifyAllData(t, cluster.testCluster, trackers, shardID, moreExpected)
	t.Logf("all 30 entries verified on 3 live barrier nodes (encrypted replication + catch-up)")
}

// TestE2E_Barrier_SnapshotCatchUp verifies that a new node added to an
// encrypted barrier cluster catches up via snapshot transfer when the
// log has been compacted. With snapshotEntries=20 and 50 proposed entries,
// the log will have been compacted, forcing snapshot-based recovery over
// mTLS QUIC for the new joiner.
func TestE2E_Barrier_SnapshotCatchUp(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	// Use aggressive snapshot settings to force compaction.
	snapshotCfg := func(shardID, replicaID uint64) config.Config {
		return testShardConfigWithSnapshot(shardID, replicaID, 20, 5)
	}
	trackers := cluster.startShardWithConfig(shardID, snapshotCfg)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Propose 50 entries to force log compaction (snapshotEntries=20).
	expected := proposeNEntries(t, cluster.testCluster, trackers, shardID, 50, "bsnap")
	verifyAllData(t, cluster.testCluster, trackers, shardID, expected)
	t.Logf("50 entries proposed and verified; log compaction should have occurred")

	// Add a new node with an independent barrier. Because the log has been
	// compacted, the new node must catch up via encrypted snapshot transfer.
	b := newBarrierForNode(t)
	result := addNodeToCluster(t, cluster.testCluster, shardID, 4, snapshotCfg, quicraft.WithBarrier(b))
	t.Logf("new node[%d] added at %s as replicaID=4 (snapshot catch-up)", result.idx, result.addr)

	// Wait for the new node to receive all 50 entries via snapshot.
	waitForNodeData(t, result.tracker, shardID, 4, expected, membershipTimeout)
	t.Logf("new node caught up on all 50 entries via encrypted snapshot transfer")

	// Propose 10 more entries and verify all 60 on all 4 nodes.
	moreExpected := proposeNEntries(t, cluster.testCluster, trackers, shardID, 10, "bsnap-post")
	allExpected := make(map[string]string, len(expected)+len(moreExpected))
	for k, v := range expected {
		allExpected[k] = v
	}
	for k, v := range moreExpected {
		allExpected[k] = v
	}

	verifyAllData(t, cluster.testCluster, trackers, shardID, allExpected)
	t.Logf("all 60 entries verified on 4 barrier nodes (snapshot-based catch-up)")
}

// TestE2E_Barrier_KeyRotation_ReplaceNode verifies that a barrier cluster
// survives key rotation (epoch change) and that a replacement node with a
// fresh barrier can recover all data spanning multiple encryption epochs.
// The test writes data under epoch 1, rotates all barriers to epoch 2,
// writes more data, then replaces a follower and verifies the replacement
// has all cross-epoch data.
func TestE2E_Barrier_KeyRotation_ReplaceNode(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	// Use snapshot config to exercise compaction across epoch boundaries.
	snapshotCfg := func(shardID, replicaID uint64) config.Config {
		return testShardConfigWithSnapshot(shardID, replicaID, 20, 5)
	}
	trackers := cluster.startShardWithConfig(shardID, snapshotCfg)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Phase 1: Propose 15 entries under epoch 1.
	epoch1Expected := proposeNEntries(t, cluster.testCluster, trackers, shardID, 15, "epoch1")
	verifyAllData(t, cluster.testCluster, trackers, shardID, epoch1Expected)
	t.Logf("epoch 1: 15 entries proposed and verified")

	// Rotate all barriers to epoch 2.
	for i, b := range cluster.barriers {
		if cluster.stopped[i].Load() {
			continue
		}
		newEpoch, err := b.Rotate()
		if err != nil {
			t.Fatalf("barrier.Rotate[%d]: %v", i, err)
		}
		t.Logf("node[%d] rotated to epoch %d", i, newEpoch)
	}

	// Phase 2: Propose 15 entries under epoch 2.
	epoch2Expected := proposeNEntries(t, cluster.testCluster, trackers, shardID, 15, "epoch2")
	t.Logf("epoch 2: 15 entries proposed")

	// Merge both epochs into a single expected map.
	allExpected := make(map[string]string, len(epoch1Expected)+len(epoch2Expected))
	for k, v := range epoch1Expected {
		allExpected[k] = v
	}
	for k, v := range epoch2Expected {
		allExpected[k] = v
	}

	// Verify all 30 entries on all 3 nodes.
	verifyAllData(t, cluster.testCluster, trackers, shardID, allExpected)
	t.Logf("all 30 cross-epoch entries verified on 3 nodes")

	// Replace a follower.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatal("no follower found for replacement")
	}
	removedReplicaID := uint64(followerIdx + 1)
	t.Logf("replacing follower node[%d] replicaID=%d", followerIdx, removedReplicaID)

	cluster.stopNode(followerIdx)

	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := leaderHost.SyncRequestDeleteNode(ctx, shardID, removedReplicaID, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", removedReplicaID, err)
	}
	t.Logf("removed replica %d from membership", removedReplicaID)

	// Add replacement node with a fresh, independently initialized barrier.
	b := newBarrierForNode(t)
	result := addNodeToCluster(t, cluster.testCluster, shardID, 4, snapshotCfg, quicraft.WithBarrier(b))
	t.Logf("replacement node[%d] added at %s as replicaID=4", result.idx, result.addr)

	// Verify replacement has all 30 cross-epoch entries.
	waitForNodeData(t, result.tracker, shardID, 4, allExpected, membershipTimeout)
	t.Logf("replacement node caught up on all 30 cross-epoch entries")

	// Propose 10 more entries under epoch 2 and verify all 40 on live nodes.
	postExpected := proposeNEntries(t, cluster.testCluster, trackers, shardID, 10, "epoch2-post")
	finalExpected := make(map[string]string, len(allExpected)+len(postExpected))
	for k, v := range allExpected {
		finalExpected[k] = v
	}
	for k, v := range postExpected {
		finalExpected[k] = v
	}

	// Verify all 40 entries across 3 live barrier nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		var tracker *smTracker
		if i < len(trackers) {
			tracker = trackers[i]
		}
		if tracker == nil {
			// Use the replacement tracker for the dynamically added node.
			if i == result.idx {
				tracker = result.tracker
				replicaID = 4
			} else {
				continue
			}
		}
		for key, val := range finalExpected {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] has key=%q after key rotation replacement", i, key),
				func() bool {
					got, ok := tracker.Get(shardID, replicaID).Get(key)
					return ok && got == val
				},
			)
		}
	}
	t.Logf("all 40 entries verified on 3 live nodes (cross-epoch data intact, replacement fully caught up)")
}
