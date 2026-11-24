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

	for idx, stoppedIdx := range stoppedNodes {
		oldReplicaID := uint64(stoppedIdx + 1)

		// Remove the stopped node from membership.
		syncDeleteNode(t, cluster, 1, oldReplicaID, membershipTimeout)

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
	// Verify every node in the reconverged cluster has all expected data, and
	// count them: a node that is live but contributes no assertion is the
	// failure this loop exists to report, not a node to pass over.
	liveCount := 0
	verified := 0
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		liveCount++

		// Ask the host which replica it runs. An index-derived form ("i+1,
		// except >=5 which is 10+(i-5)") would restate addNodeToCluster's
		// argument list as a rule and go stale the moment the sequence
		// changed. See replicaIDAt.
		replicaID, ok := cluster.replicaIDAt(1, i)
		if !ok {
			t.Fatalf("node[%d] is live but reports no replica for shard 1", i)
		}

		tracker := cluster.trackers[i]
		if tracker == nil {
			// A live node with no tracker means startShard/addNodeToCluster
			// never registered one for it, which is a harness or startup
			// failure — not a reason to leave the node unchecked.
			t.Fatalf("node[%d] (replica=%d) is live but has no state machine tracker", i, replicaID)
		}

		nodeSM := tracker.Get(1, replicaID)
		if nodeSM == nil {
			// SM may not be registered yet; wait for it. Failing to register
			// is a startup failure, so the wait fatals rather than skipping.
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
		verified++
	}
	if verified < liveCount {
		t.Fatalf("reconvergence check verified %d nodes but %d are live", verified, liveCount)
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

		// See replicaIDAt: the host's own answer, not i+1. This loop
		// tolerates a missing tracker and a missing state machine below, so
		// it tolerates a host with no replica here for the same reason.
		replicaID, ok := cluster.replicaIDAt(1, i)
		if !ok {
			continue
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
