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

// TestE2E_Failover_LeaderKill_NewLeaderElected verifies that when the
// current leader is stopped, the remaining nodes elect a new leader.
func TestE2E_Failover_LeaderKill_NewLeaderElected(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	oldLeaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up: propose a value to ensure all QUIC connections are
	// established and heartbeats are flowing between nodes.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	oldLeaderReplicaID := uint64(oldLeaderIdx + 1)
	t.Logf("initial leader: node[%d] replicaID=%d", oldLeaderIdx, oldLeaderReplicaID)

	// Stop the leader.
	cluster.stopNode(oldLeaderIdx)

	if cluster.liveNodes() != 2 {
		t.Fatalf("expected 2 live nodes after stopping leader, got %d", cluster.liveNodes())
	}

	// Wait for a new leader to be elected from the remaining nodes.
	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)

	if newLeaderIdx == oldLeaderIdx {
		t.Fatal("new leader should not be the stopped node")
	}
	if cluster.stopped[newLeaderIdx].Load() {
		t.Fatal("new leader index points to a stopped node")
	}

	t.Logf("new leader elected: node[%d] after stopping node[%d]", newLeaderIdx, oldLeaderIdx)
}

// TestE2E_Failover_DataSurvivesLeaderChange verifies that data written
// before a leader failover is still accessible on the surviving nodes.
func TestE2E_Failover_DataSurvivesLeaderChange(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Write data through the initial leader.
	proposeAndWait(t, cluster, trackers, 1, "survive-key", "survive-value")
	waitForReplication(t, cluster, trackers, 1, "survive-key", "survive-value", defaultTimeout)

	t.Logf("data written and replicated, stopping leader node[%d]", leaderIdx)

	// Stop the leader.
	cluster.stopNode(leaderIdx)

	// Wait for new leader.
	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("new leader elected at node[%d]", newLeaderIdx)

	// Verify data is still accessible on surviving nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		sm := trackers[i].Get(1, replicaID)
		if sm == nil {
			t.Fatalf("node[%d] has no state machine", i)
		}
		val, ok := sm.Get("survive-key")
		if !ok {
			t.Fatalf("node[%d] lost key 'survive-key' after failover", i)
		}
		if val != "survive-value" {
			t.Fatalf("node[%d] 'survive-key' = %q, want %q", i, val, "survive-value")
		}
	}
}

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
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("after-failover", "recovered")
	rs, err := cluster.hosts[newLeaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose after failover failed: %v", err)
	}
	_, err = rs.Result()
	if err != nil {
		t.Fatalf("Result after failover failed: %v", err)
	}
	rs.Release()

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
			continue
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

// TestE2E_Failover_MinorityPartition verifies that stopping 1 node in a
// 3-node cluster (minority failure) allows the cluster to continue
// accepting proposals.
func TestE2E_Failover_MinorityPartition(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up: ensure all QUIC connections are established before
	// stopping a node.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Find a follower and stop it.
	followerIdx := cluster.findFollower(1)
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	t.Logf("stopping follower node[%d], leader is node[%d]", followerIdx, leaderIdx)
	cluster.stopNode(followerIdx)

	if cluster.liveNodes() != 2 {
		t.Fatalf("expected 2 live nodes, got %d", cluster.liveNodes())
	}

	// The leader should still be alive. If the leader was the stopped node
	// this would have been caught by findFollower. Wait for leadership to
	// stabilize (leader may or may not change).
	activeLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("active leader after minority failure: node[%d]", activeLeaderIdx)

	// Propose through the active leader.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("minority-key", "minority-value")
	rs, err := cluster.hosts[activeLeaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose with minority failure failed: %v", err)
	}
	_, err = rs.Result()
	if err != nil {
		t.Fatalf("Result with minority failure failed: %v", err)
	}
	rs.Release()

	// Verify data on surviving nodes.
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
			fmt.Sprintf("node[%d] replicates minority-key", i),
			func() bool {
				val, ok := sm.Get("minority-key")
				return ok && val == "minority-value"
			},
		)
	}
}

// ---------------------------------------------------------------------------
// 5-Node Failover Tests
// ---------------------------------------------------------------------------

// TestE2E_FiveNode_Failover_LeaderKill_NewLeaderElected verifies that when
// the leader is stopped in a 5-node cluster, the remaining 4 nodes elect
// a new leader. With quorum=3, 4 surviving nodes have ample majority.
func TestE2E_FiveNode_Failover_LeaderKill_NewLeaderElected(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	oldLeaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up: propose a value to ensure all QUIC connections are
	// established and heartbeats are flowing between all 5 nodes.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	oldLeaderReplicaID := uint64(oldLeaderIdx + 1)
	t.Logf("initial leader: node[%d] replicaID=%d", oldLeaderIdx, oldLeaderReplicaID)

	// Stop the leader.
	cluster.stopNode(oldLeaderIdx)

	if cluster.liveNodes() != 4 {
		t.Fatalf("expected 4 live nodes after stopping leader, got %d", cluster.liveNodes())
	}

	// Wait for a new leader to be elected from the remaining 4 nodes.
	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)

	if newLeaderIdx == oldLeaderIdx {
		t.Fatal("new leader should not be the stopped node")
	}
	if cluster.stopped[newLeaderIdx].Load() {
		t.Fatal("new leader index points to a stopped node")
	}

	t.Logf("new leader elected: node[%d] after stopping node[%d]", newLeaderIdx, oldLeaderIdx)
}

// TestE2E_FiveNode_Failover_DataSurvivesLeaderChange verifies that data
// written before a leader failover is still accessible on the 4 surviving
// nodes in a 5-node cluster.
func TestE2E_FiveNode_Failover_DataSurvivesLeaderChange(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Write data through the initial leader and replicate to all nodes.
	proposeAndWait(t, cluster, trackers, 1, "f5-survive-key", "f5-survive-value")
	waitForReplication(t, cluster, trackers, 1, "f5-survive-key", "f5-survive-value", defaultTimeout)

	t.Logf("data written and replicated to all 5 nodes, stopping leader node[%d]", leaderIdx)

	// Stop the leader.
	cluster.stopNode(leaderIdx)

	// Wait for new leader.
	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("new leader elected at node[%d]", newLeaderIdx)

	// Verify data is still accessible on all 4 surviving nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] has no state machine", i)
		}
		val, ok := nodeSM.Get("f5-survive-key")
		if !ok {
			t.Fatalf("node[%d] lost key 'f5-survive-key' after failover", i)
		}
		if val != "f5-survive-value" {
			t.Fatalf("node[%d] 'f5-survive-key' = %q, want %q", i, val, "f5-survive-value")
		}
	}
}

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
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("f5-after-failover", "recovered")
	rs, err := cluster.hosts[newLeaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose after failover failed: %v", err)
	}
	_, err = rs.Result()
	if err != nil {
		t.Fatalf("Result after failover failed: %v", err)
	}
	rs.Release()

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
			continue
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

// TestE2E_FiveNode_Failover_MinorityLoss_OneFailed verifies that a 5-node
// cluster continues operating after 1 follower is stopped. With 4 out of
// 5 nodes running, quorum (3) is maintained.
func TestE2E_FiveNode_Failover_MinorityLoss_OneFailed(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up: ensure all QUIC connections are established.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Find a follower and stop it.
	followerIdx := cluster.findFollower(1)
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	t.Logf("stopping follower node[%d], leader is node[%d]", followerIdx, leaderIdx)
	cluster.stopNode(followerIdx)

	if cluster.liveNodes() != 4 {
		t.Fatalf("expected 4 live nodes, got %d", cluster.liveNodes())
	}

	// Leadership should stabilize (may or may not change).
	activeLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("active leader after 1-node failure: node[%d]", activeLeaderIdx)

	// Propose through the active leader.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("f5-one-down-key", "f5-one-down-value")
	rs, err := cluster.hosts[activeLeaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose with 1 node down failed: %v", err)
	}
	_, err = rs.Result()
	if err != nil {
		t.Fatalf("Result with 1 node down failed: %v", err)
	}
	rs.Release()

	// Verify data on all 4 surviving nodes.
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
			fmt.Sprintf("node[%d] replicates f5-one-down-key", i),
			func() bool {
				val, ok := nodeSM.Get("f5-one-down-key")
				return ok && val == "f5-one-down-value"
			},
		)
	}
}

// TestE2E_FiveNode_Failover_MinorityLoss_TwoFailed verifies that a 5-node
// cluster continues operating after 2 followers are stopped. With 3 out of
// 5 nodes running, the cluster is at exact quorum (3) and should still
// accept proposals.
func TestE2E_FiveNode_Failover_MinorityLoss_TwoFailed(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up: ensure all QUIC connections are established.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Find and stop the first follower.
	follower1Idx := cluster.findFollower(1)
	if follower1Idx < 0 {
		t.Fatal("no first follower found")
	}

	t.Logf("stopping first follower node[%d], leader is node[%d]", follower1Idx, leaderIdx)
	cluster.stopNode(follower1Idx)

	if cluster.liveNodes() != 4 {
		t.Fatalf("expected 4 live nodes after first stop, got %d", cluster.liveNodes())
	}

	// Wait for leadership to stabilize after first failure.
	activeLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)

	// Find and stop the second follower.
	follower2Idx := -1
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || i == activeLeaderIdx {
			continue
		}
		follower2Idx = i
		break
	}
	if follower2Idx < 0 {
		t.Fatal("no second follower found")
	}

	t.Logf("stopping second follower node[%d], active leader is node[%d]", follower2Idx, activeLeaderIdx)
	cluster.stopNode(follower2Idx)

	if cluster.liveNodes() != 3 {
		t.Fatalf("expected 3 live nodes after two stops, got %d", cluster.liveNodes())
	}

	// Wait for leadership to stabilize. With exactly quorum (3 of 5),
	// the cluster must still elect/maintain a leader.
	finalLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("leader after 2-node failure: node[%d]", finalLeaderIdx)

	// Propose through the leader at exact quorum.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("f5-two-down-key", "f5-two-down-value")
	rs, err := cluster.hosts[finalLeaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose with 2 nodes down (exact quorum) failed: %v", err)
	}
	_, err = rs.Result()
	if err != nil {
		t.Fatalf("Result with 2 nodes down (exact quorum) failed: %v", err)
	}
	rs.Release()

	// Verify data on all 3 surviving nodes.
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
			fmt.Sprintf("node[%d] replicates f5-two-down-key", i),
			func() bool {
				val, ok := nodeSM.Get("f5-two-down-key")
				return ok && val == "f5-two-down-value"
			},
		)
	}

	t.Logf("5-node cluster survived 2 follower failures (3/5 = exact quorum)")
}
