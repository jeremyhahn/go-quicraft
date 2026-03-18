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

package host_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestHostAPI_RollingRestart_ThreeNode verifies that a 3-node cluster
// survives a rolling restart where each node is stopped and restarted
// sequentially. After all restarts complete, all data written before and
// during the rolling restart is verified across all nodes.
//
// The test exercises:
// - WAL recovery after clean shutdown
// - Leader re-election when the leader is restarted
// - State machine recovery from persisted log/snapshot
// - Cluster stability throughout the rolling restart sequence
func TestHostAPI_RollingRestart_ThreeNode(t *testing.T) {
	cluster, nodeConfigs := newTestClusterWithRestart(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	// Use snapshot-enabled config so in-memory SMs can recover state
	// after restart. Without snapshots, raft considers all committed
	// entries as already processed and won't replay them to a fresh SM.
	shardCfgFn := func(sid, rid uint64) config.Config {
		return testShardConfigWithSnapshot(sid, rid, 10, 5)
	}

	trackers := cluster.startShardWithConfig(shardID, shardCfgFn)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Propose initial data before any restarts.
	expected := proposeNEntries(t, cluster, trackers, shardID, 10, "pre-restart")
	verifyAllData(t, cluster, trackers, shardID, expected)
	t.Logf("10 entries replicated to all 3 nodes before rolling restart")

	// Rolling restart: stop and restart each node sequentially.
	// The cluster must maintain quorum (2 of 3) at all times.
	for nodeIdx := range 3 {
		t.Logf("rolling restart: stopping node[%d]", nodeIdx)

		restartNode(t, cluster, nodeIdx, nodeConfigs[nodeIdx], shardID, shardCfgFn)
		t.Logf("rolling restart: node[%d] restarted", nodeIdx)

		// Wait for a leader to be available after the restart.
		newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
		t.Logf("rolling restart: leader is node[%d] after restarting node[%d]", newLeaderIdx, nodeIdx)

		// Propose an entry after each restart to confirm the cluster is
		// operational and accepting writes.
		key := fmt.Sprintf("during-restart-%d", nodeIdx)
		value := fmt.Sprintf("val-restart-%d", nodeIdx)
		proposeAndWait(t, cluster, cluster.trackers, shardID, key, value)
		expected[key] = value
		t.Logf("rolling restart: proposed and applied key=%q after restarting node[%d]", key, nodeIdx)
	}

	// After all 3 restarts, propose additional entries.
	postExpected := proposeNEntries(t, cluster, trackers, shardID, 5, "post-restart")
	for k, v := range postExpected {
		expected[k] = v
	}

	// Verify all data (pre-restart + during-restart + post-restart) is
	// present on every node. The restarted nodes must have recovered all
	// previously persisted data from WAL.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			t.Fatalf("node[%d] should not be stopped after rolling restart", i)
		}
		replicaID := uint64(i + 1)
		nodeSM := cluster.trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] state machine not found after rolling restart", i)
		}
		for key, val := range expected {
			waitForCondition(t, failoverTimeout,
				fmt.Sprintf("node[%d] has key=%q after rolling restart", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	t.Logf("rolling restart complete: all %d entries verified across all 3 nodes", len(expected))
}

// TestHostAPI_RollingRestart_LeaderFirst verifies the rolling restart
// sequence when the leader node is restarted first. This is the most
// disruptive scenario because it forces an immediate leader election.
func TestHostAPI_RollingRestart_LeaderFirst(t *testing.T) {
	cluster, nodeConfigs := newTestClusterWithRestart(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	// Use snapshot-enabled config so in-memory SMs can recover state
	// after restart. Without snapshots, raft considers all committed
	// entries as already processed and won't replay them to a fresh SM.
	shardCfgFn := func(sid, rid uint64) config.Config {
		return testShardConfigWithSnapshot(sid, rid, 10, 5)
	}

	trackers := cluster.startShardWithConfig(shardID, shardCfgFn)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Propose initial data.
	expected := proposeNEntries(t, cluster, trackers, shardID, 15, "leader-first")
	verifyAllData(t, cluster, trackers, shardID, expected)
	t.Logf("15 entries replicated before leader-first rolling restart")

	// Build restart order: leader first, then followers.
	restartOrder := make([]int, 0, 3)
	restartOrder = append(restartOrder, leaderIdx)
	for i := range 3 {
		if i != leaderIdx {
			restartOrder = append(restartOrder, i)
		}
	}

	for _, nodeIdx := range restartOrder {
		t.Logf("restarting node[%d] (leader=%v)", nodeIdx, nodeIdx == leaderIdx)

		restartNode(t, cluster, nodeIdx, nodeConfigs[nodeIdx], shardID, shardCfgFn)

		newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
		t.Logf("leader is node[%d] after restarting node[%d]", newLeaderIdx, nodeIdx)

		// Confirm writes work after each restart. Use cluster.trackers
		// (not the local trackers variable) because restartNode replaces
		// cluster.trackers[idx] with a fresh smTracker for each restarted
		// node. The original trackers slice still points to pre-restart
		// state machines that are closed and will never receive new entries.
		key := fmt.Sprintf("lf-restart-%d", nodeIdx)
		value := fmt.Sprintf("lf-val-%d", nodeIdx)
		proposeAndWait(t, cluster, cluster.trackers, shardID, key, value)
		expected[key] = value
	}

	// Wait for a stable leader before final verification so that
	// replication can proceed to all nodes.
	cluster.waitForClusterLeader(shardID, failoverTimeout)

	// Final verification: every node has all data.
	verifyAllData(t, cluster, cluster.trackers, shardID, expected)

	t.Logf("leader-first rolling restart complete: all %d entries verified", len(expected))
}

// TestHostAPI_RollingRestart_WritesDuringRestart verifies that concurrent
// writes submitted while a node is being restarted are not lost. The
// remaining 2 nodes maintain quorum and accept proposals while the third
// node is down.
func TestHostAPI_RollingRestart_WritesDuringRestart(t *testing.T) {
	cluster, nodeConfigs := newTestClusterWithRestart(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	// Use snapshot-enabled config so in-memory SMs can recover state
	// after restart. Without snapshots, raft considers all committed
	// entries as already processed and won't replay them to a fresh SM.
	shardCfgFn := func(sid, rid uint64) config.Config {
		return testShardConfigWithSnapshot(sid, rid, 10, 5)
	}

	trackers := cluster.startShardWithConfig(shardID, shardCfgFn)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// Warmup to establish QUIC connections.
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")

	expected := map[string]string{"warmup": "ok"}

	// Stop a follower (not the leader) to avoid triggering re-election.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}
	t.Logf("stopping follower node[%d] while proposing entries", followerIdx)
	cluster.stopNode(followerIdx)

	// Propose entries while the follower is down.
	for i := range 10 {
		key := fmt.Sprintf("during-down-%03d", i)
		value := fmt.Sprintf("val-during-%03d", i)

		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		leaderIdx := cluster.findLeader(shardID)
		if leaderIdx < 0 {
			cancel()
			t.Fatal("no leader available while follower is down")
		}
		cmd := encodeKVPut(key, value)
		rs, err := cluster.hosts[leaderIdx].Propose(ctx, shardID, cmd)
		if err != nil {
			cancel()
			t.Fatalf("Propose(%q) while follower down failed: %v", key, err)
		}
		rs.Release()
		cancel()
		expected[key] = value
	}
	t.Logf("proposed 10 entries while node[%d] was down", followerIdx)

	// Restart the stopped follower.
	restartNode(t, cluster, followerIdx, nodeConfigs[followerIdx], shardID, shardCfgFn)
	t.Logf("restarted node[%d]", followerIdx)

	// Wait for the restarted node to catch up on all entries.
	cluster.waitForClusterLeader(shardID, failoverTimeout)

	replicaID := uint64(followerIdx + 1)
	restartedSM := cluster.trackers[followerIdx].Get(shardID, replicaID)
	if restartedSM == nil {
		// SM may take a moment to register after restart.
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] SM registered after restart", followerIdx),
			func() bool {
				restartedSM = cluster.trackers[followerIdx].Get(shardID, replicaID)
				return restartedSM != nil
			},
		)
	}

	for key, val := range expected {
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("restarted node[%d] has key=%q", followerIdx, key),
			func() bool {
				got, ok := restartedSM.Get(key)
				return ok && got == val
			},
		)
	}

	t.Logf("restarted node[%d] caught up on all %d entries", followerIdx, len(expected))
}
