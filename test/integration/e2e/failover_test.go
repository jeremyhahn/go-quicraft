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

// TestE2E_Failover_ProposalAfterFailover verifies that the new leader
// accepts proposals after the original leader has been stopped.
func TestE2E_Failover_ProposalAfterFailover(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	oldLeaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Write initial data.
	proposeAndWait(t, cluster, trackers, 1, "before-failover", "initial")
	waitForReplication(t, cluster, trackers, 1, "before-failover", "initial", defaultTimeout)

	// Stop the leader.
	t.Logf("stopping leader node[%d]", oldLeaderIdx)
	cluster.stopNode(oldLeaderIdx)

	// Wait for new leader.
	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("new leader at node[%d]", newLeaderIdx)

	// Propose through the new leader.
	// The property is that the CLUSTER still accepts writes after the
	// failure, not that newLeaderIdx specifically does. Resolve at the point of use.
	proposeToLeader(t, cluster, 1, encodeKVPut("after-failover", "recovered"), defaultTimeout)

	// Verify new data is replicated to all surviving nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		sm := trackers[i].Get(1, replicaID)
		if sm == nil {
			continue
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] replicates after-failover", i),
			func() bool {
				val, ok := sm.Get("after-failover")
				return ok && val == "recovered"
			},
		)
	}

	// Verify the pre-failover data is also still present.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		sm := trackers[i].Get(1, replicaID)
		if sm == nil {
			t.Fatalf("node[%d] has no state machine", i)
		}
		val, ok := sm.Get("before-failover")
		if !ok {
			t.Fatalf("node[%d] lost pre-failover data", i)
		}
		if val != "initial" {
			t.Fatalf("node[%d] 'before-failover' = %q, want %q", i, val, "initial")
		}
	}
}

// ---------------------------------------------------------------------------
// 5-Node Failover Tests
// ---------------------------------------------------------------------------

// TestE2E_FiveNode_Failover_ProposalAfterFailover verifies that the new
// leader accepts proposals after the original leader has been stopped
// in a 5-node cluster.
func TestE2E_FiveNode_Failover_ProposalAfterFailover(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	oldLeaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Write initial data and ensure full replication (warmup).
	proposeAndWait(t, cluster, trackers, 1, "f5-before-failover", "initial")
	waitForReplication(t, cluster, trackers, 1, "f5-before-failover", "initial", defaultTimeout)

	// Stop the leader.
	t.Logf("stopping leader node[%d]", oldLeaderIdx)
	cluster.stopNode(oldLeaderIdx)

	// Wait for new leader.
	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("new leader at node[%d]", newLeaderIdx)

	// Propose through the new leader.
	// The property is that the CLUSTER still accepts writes after the
	// failure, not that newLeaderIdx specifically does. Resolve at the point of use.
	proposeToLeader(t, cluster, 1, encodeKVPut("f5-after-failover", "recovered"), defaultTimeout)

	// Verify new data is replicated to all surviving nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			continue
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] replicates f5-after-failover", i),
			func() bool {
				val, ok := nodeSM.Get("f5-after-failover")
				return ok && val == "recovered"
			},
		)
	}

	// Verify the pre-failover data is also still present.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] has no state machine", i)
		}
		val, ok := nodeSM.Get("f5-before-failover")
		if !ok {
			t.Fatalf("node[%d] lost pre-failover data", i)
		}
		if val != "initial" {
			t.Fatalf("node[%d] 'f5-before-failover' = %q, want %q", i, val, "initial")
		}
	}
}
