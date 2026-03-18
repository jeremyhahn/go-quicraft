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
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestE2E_Partition_MinorityCantElectLeader verifies that a minority
// partition (2 out of 5 nodes) cannot elect a leader. In a 5-node
// cluster, quorum requires 3 nodes. We simulate the partition by
// stopping the majority (3 nodes), leaving only 2 nodes alive, and
// verify that those 2 nodes cannot establish leadership.
func TestE2E_Partition_MinorityCantElectLeader(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up: ensure all QUIC connections are established and
	// heartbeats are flowing between all 5 nodes.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	t.Logf("initial leader: node[%d]", leaderIdx)

	// Identify the minority partition: pick 2 non-leader followers.
	// These will be the only surviving nodes, simulating a minority
	// partition that has been isolated from the majority.
	minorityNodes := make([]int, 0, 2)
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		minorityNodes = append(minorityNodes, i)
		if len(minorityNodes) == 2 {
			break
		}
	}

	// Stop the majority partition (3 nodes including the leader).
	// This leaves only the 2 minority nodes alive.
	for i := range cluster.hosts {
		isMinority := false
		for _, mi := range minorityNodes {
			if i == mi {
				isMinority = true
				break
			}
		}
		if !isMinority {
			t.Logf("stopping node[%d] (majority partition)", i)
			cluster.stopNode(i)
		}
	}

	if cluster.liveNodes() != 2 {
		t.Fatalf("expected 2 live nodes (minority partition), got %d", cluster.liveNodes())
	}

	// The minority partition (2 nodes) must NOT be able to elect a leader.
	// Wait a generous amount of time to ensure no leader emerges. With
	// RTT=50ms and ElectionRTT=10, election timeout is ~500ms. We wait
	// for several election cycles to confirm no leader is elected.
	noLeaderDuration := 3 * time.Second
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	noLeaderTimer := time.NewTimer(noLeaderDuration)
	defer noLeaderTimer.Stop()
noLeaderLoop:
	for {
		idx := cluster.findLeader(1)
		if idx >= 0 {
			t.Fatalf("minority partition elected a leader at node[%d]; "+
				"split-brain detected", idx)
		}
		select {
		case <-noLeaderTimer.C:
			break noLeaderLoop
		case <-ticker.C:
		}
	}

	t.Logf("minority partition (nodes %v) correctly cannot elect a leader", minorityNodes)
}

// TestE2E_Partition_MajorityCommitsDuringPartition verifies that the
// majority partition in a 5-node cluster can continue to accept and
// commit proposals while the minority partition is isolated. We simulate
// the partition by stopping 2 follower nodes, leaving the leader and
// 2 followers (majority of 3) alive.
func TestE2E_Partition_MajorityCommitsDuringPartition(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up: ensure all QUIC connections are established.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Write data before partition for later consistency verification.
	proposeAndWait(t, cluster, trackers, 1, "pre-partition", "before")
	waitForReplication(t, cluster, trackers, 1, "pre-partition", "before", defaultTimeout)

	t.Logf("initial leader: node[%d], pre-partition data replicated", leaderIdx)

	// Create partition: stop 2 followers (minority), keep leader + 2
	// followers alive (majority of 3).
	stoppedCount := 0
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		if stoppedCount < 2 {
			t.Logf("stopping node[%d] (minority partition)", i)
			cluster.stopNode(i)
			stoppedCount++
		}
	}

	if cluster.liveNodes() != 3 {
		t.Fatalf("expected 3 live nodes (majority partition), got %d", cluster.liveNodes())
	}

	// Wait for leadership to stabilize after partition.
	activeLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("active leader during partition: node[%d]", activeLeaderIdx)

	// The majority partition should still accept proposals.
	for i := range 5 {
		key := fmt.Sprintf("during-partition-%03d", i)
		value := fmt.Sprintf("val-partition-%03d", i)
		proposeAndWait(t, cluster, trackers, 1, key, value)
	}

	// Verify all data (pre-partition + during-partition) is present on
	// all live nodes in the majority partition.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			continue
		}

		// Check pre-partition data.
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] has pre-partition data", i),
			func() bool {
				val, ok := nodeSM.Get("pre-partition")
				return ok && val == "before"
			},
		)

		// Check during-partition data.
		for j := range 5 {
			key := fmt.Sprintf("during-partition-%03d", j)
			expectedVal := fmt.Sprintf("val-partition-%03d", j)
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("node[%d] has key=%q", i, key),
				func() bool {
					val, ok := nodeSM.Get(key)
					return ok && val == expectedVal
				},
			)
		}
	}

	t.Logf("majority partition (3 nodes) committed 5 entries during partition")
}

// TestE2E_Partition_HealAndReconverge verifies the full partition
// lifecycle: form cluster, write data, create partition, write more
// data through majority, then heal the partition and verify all nodes
// reconverge with consistent state.
//
// Since the e2e test cluster uses ephemeral temp directories (t.TempDir),
// stopped nodes cannot be restarted with their WAL state. Instead, we
// simulate partition healing by adding new nodes via membership changes
// to replace the partitioned nodes. This validates that Raft's membership
// change and snapshot transfer mechanisms correctly bring new nodes up
// to date after a partition event.
func TestE2E_Partition_HealAndReconverge(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up and write pre-partition data.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	expected := make(map[string]string)
	expected["warmup"] = "ok"

	for i := range 3 {
		key := fmt.Sprintf("pre-partition-%03d", i)
		value := fmt.Sprintf("pre-val-%03d", i)
		proposeAndWait(t, cluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Wait for full replication before partition.
	for key, val := range expected {
		waitForReplication(t, cluster, trackers, 1, key, val, defaultTimeout)
	}

	t.Logf("pre-partition data replicated to all 5 nodes, leader is node[%d]", leaderIdx)

	// Create partition: stop 2 followers (minority).
	stoppedNodes := make([]int, 0, 2)
	for i := range cluster.hosts {
		if i == leaderIdx || len(stoppedNodes) >= 2 {
			continue
		}
		t.Logf("partitioning: stopping node[%d]", i)
		cluster.stopNode(i)
		stoppedNodes = append(stoppedNodes, i)
	}

	if cluster.liveNodes() != 3 {
		t.Fatalf("expected 3 live nodes during partition, got %d", cluster.liveNodes())
	}

	// Wait for leadership to stabilize and write data during partition.
	activeLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("active leader during partition: node[%d]", activeLeaderIdx)

	for i := range 3 {
		key := fmt.Sprintf("during-partition-%03d", i)
		value := fmt.Sprintf("during-val-%03d", i)
		proposeAndWait(t, cluster, trackers, 1, key, value)
		expected[key] = value
	}

	t.Logf("wrote 3 entries during partition with %d live nodes", cluster.liveNodes())

	// Heal partition: remove the stopped nodes from membership and add
	// replacement nodes. This simulates the partition healing process
	// where isolated nodes rejoin (or are replaced) and catch up via
	// Raft log replication or snapshot transfer.
	leaderHost := cluster.hosts[activeLeaderIdx]

	for idx, stoppedIdx := range stoppedNodes {
		oldReplicaID := uint64(stoppedIdx + 1)

		// Remove the stopped node from membership.
		ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
		if err := leaderHost.SyncRequestDeleteNode(ctx, 1, oldReplicaID, 0); err != nil {
			cancel()
			t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", oldReplicaID, err)
		}
		cancel()

		t.Logf("removed stopped node[%d] (replica=%d) from membership", stoppedIdx, oldReplicaID)

		// Add a replacement node. Use replica IDs starting at 10 to
		// avoid collision with original replica IDs.
		newReplicaID := uint64(10 + idx)
		cfgFn := func(shardID, replicaID uint64) config.Config {
			return testShardConfig(shardID, replicaID)
		}

		result := addNodeToCluster(t, cluster, 1, newReplicaID, cfgFn)
		t.Logf("added replacement node[%d] (replica=%d) at %s",
			result.idx, newReplicaID, result.addr)
	}

	// Wait for the replacement nodes to catch up with all data.
	// Verify every node in the reconverged cluster has all expected data.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}

		replicaID := uint64(i + 1)
		// For replacement nodes (indices >= 5), look up the correct
		// replica ID from the addNodeToCluster sequence.
		if i >= 5 {
			replicaID = uint64(10 + (i - 5))
		}

		tracker := cluster.trackers[i]
		if tracker == nil {
			// Original trackers may be nil for stopped nodes; skip.
			continue
		}

		nodeSM := tracker.Get(1, replicaID)
		if nodeSM == nil {
			// SM may not be registered yet; wait for it.
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] replica=%d SM registered", i, replicaID),
				func() bool {
					nodeSM = tracker.Get(1, replicaID)
					return nodeSM != nil
				},
			)
		}

		for key, val := range expected {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] has key=%q after reconvergence", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	// Write post-heal data to confirm the reconverged cluster is fully
	// operational.
	proposeAndWait(t, cluster, trackers, 1, "post-heal", "reconverged")
	expected["post-heal"] = "reconverged"

	// Verify post-heal data on all live nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}

		replicaID := uint64(i + 1)
		if i >= 5 {
			replicaID = uint64(10 + (i - 5))
		}

		tracker := cluster.trackers[i]
		if tracker == nil {
			continue
		}

		nodeSM := tracker.Get(1, replicaID)
		if nodeSM == nil {
			continue
		}

		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] has post-heal data", i),
			func() bool {
				val, ok := nodeSM.Get("post-heal")
				return ok && val == "reconverged"
			},
		)
	}

	t.Logf("partition healed: all %d live nodes reconverged with %d entries",
		cluster.liveNodes(), len(expected))
}
