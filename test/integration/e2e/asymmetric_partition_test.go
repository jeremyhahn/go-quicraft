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
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// Asymmetric partition tests, simulated by stopping nodes since loopback QUIC has no per-link filtering.

// TestE2E_AsymmetricPartition_LeaderIsolatedFromMinority checks a 3-node leader cut off from one follower keeps quorum and serves proposals, then all converge.
func TestE2E_AsymmetricPartition_LeaderIsolatedFromMinority(t *testing.T) {
	const shardID uint64 = 1

	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// Warm up: ensure all QUIC connections are established and
	// heartbeats are flowing between all 3 nodes.
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")
	waitForReplication(t, cluster, trackers, shardID, "warmup", "ok", defaultTimeout)

	// Write pre-partition data so we can verify it survives.
	expected := map[string]string{"warmup": "ok"}
	for i := range 3 {
		key := fmt.Sprintf("pre-asym-%03d", i)
		value := fmt.Sprintf("pre-val-%03d", i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}
	for key, val := range expected {
		waitForReplication(t, cluster, trackers, shardID, key, val, defaultTimeout)
	}

	t.Logf("pre-partition data replicated to all 3 nodes, leader is node[%d]", leaderIdx)

	// Identify the follower to partition (isolate from leader).
	// We pick one follower; the other stays connected to the leader.
	partitionedIdx := -1
	connectedFollowerIdx := -1
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		if partitionedIdx < 0 {
			partitionedIdx = i
		} else {
			connectedFollowerIdx = i
		}
	}

	t.Logf("partitioning follower node[%d] from leader node[%d]; "+
		"follower node[%d] stays connected", partitionedIdx, leaderIdx, connectedFollowerIdx)

	// Simulate asymmetric partition: stop the minority follower.
	cluster.stopNode(partitionedIdx)

	if cluster.liveNodes() != 2 {
		t.Fatalf("expected 2 live nodes during partition, got %d", cluster.liveNodes())
	}

	// The leader should retain leadership since it still has quorum
	// (itself + 1 connected follower = 2 of 3).
	activeLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("active leader during asymmetric partition: node[%d]", activeLeaderIdx)

	// Proposals must still commit through the majority partition.
	for i := range 5 {
		key := fmt.Sprintf("during-asym-%03d", i)
		value := fmt.Sprintf("asym-val-%03d", i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}

	// Verify all data on the 2 live nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			continue
		}
		for key, val := range expected {
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("node[%d] has key=%q during partition", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	t.Logf("majority partition (leader + 1 follower) committed %d entries", len(expected))

	// Heal partition: remove stopped node and add a replacement.
	partitionedReplicaID := uint64(partitionedIdx + 1)

	syncDeleteNode(t, cluster, shardID, partitionedReplicaID, membershipTimeout)

	t.Logf("removed partitioned node[%d] (replica=%d) from membership",
		partitionedIdx, partitionedReplicaID)

	newReplicaID := uint64(10)
	cfgFn := func(sid, rid uint64) config.Config {
		return testShardConfig(sid, rid)
	}
	result := addNodeToCluster(t, cluster, shardID, newReplicaID, cfgFn)
	t.Logf("added replacement node[%d] (replica=%d) at %s",
		result.idx, newReplicaID, result.addr)

	// Write post-heal data to confirm full cluster operation.
	proposeAndWait(t, cluster, trackers, shardID, "post-heal-asym", "healed")
	expected["post-heal-asym"] = "healed"

	// Verify all live nodes converge with the complete dataset.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		// See replicaIDAt: the host's own answer, not i+1. A live host
		// that reports no replica for this shard never started it or has
		// already lost it, which this loop fails on a line below when the SM
		// lookup comes back nil -- so it fails here too, rather than becoming
		// a node quietly dropped from a convergence check.
		replicaID, ok := cluster.replicaIDAt(shardID, i)
		if !ok {
			t.Fatalf("node[%d] is live but reports no replica for shard %d", i, shardID)
		}

		tracker := cluster.trackers[i]
		if tracker == nil {
			continue
		}

		nodeSM := tracker.Get(shardID, replicaID)
		if nodeSM == nil {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] replica=%d SM registered", i, replicaID),
				func() bool {
					nodeSM = tracker.Get(shardID, replicaID)
					return nodeSM != nil
				},
			)
		}

		for key, val := range expected {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] has key=%q after heal", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	t.Logf("asymmetric partition healed: all %d live nodes converged with %d entries",
		cluster.liveNodes(), len(expected))
}

// TestE2E_AsymmetricPartition_LeaderIsolatedFromMajority checks a 5-node majority elects a new leader when the old one and one follower are stopped, then all converge.
func TestE2E_AsymmetricPartition_LeaderIsolatedFromMajority(t *testing.T) {
	const shardID uint64 = 1

	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(shardID)
	oldLeaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// Warm up and write pre-partition data.
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")
	waitForReplication(t, cluster, trackers, shardID, "warmup", "ok", defaultTimeout)

	expected := map[string]string{"warmup": "ok"}
	for i := range 3 {
		key := fmt.Sprintf("pre-major-%03d", i)
		value := fmt.Sprintf("pre-val-%03d", i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}
	for key, val := range expected {
		waitForReplication(t, cluster, trackers, shardID, key, val, defaultTimeout)
	}

	// Record the old leader's term before partition.
	oldLeaderInfo, err := cluster.hosts[oldLeaderIdx].ShardInfo(shardID)
	if err != nil {
		t.Fatalf("ShardInfo for old leader failed: %v", err)
	}
	oldLeaderTerm := oldLeaderInfo.Term
	t.Logf("old leader node[%d] at term=%d", oldLeaderIdx, oldLeaderTerm)

	// Identify one additional follower to partition alongside the leader,
	// leaving 3 nodes as the connected majority.
	additionalPartitionIdx := -1
	for i := range cluster.hosts {
		if i == oldLeaderIdx {
			continue
		}
		additionalPartitionIdx = i
		break
	}

	t.Logf("partitioning old leader node[%d] and follower node[%d]; "+
		"remaining 3 nodes form majority", oldLeaderIdx, additionalPartitionIdx)

	// Stop the old leader and one follower to simulate leader isolation
	// from the majority.
	cluster.stopNode(oldLeaderIdx)
	cluster.stopNode(additionalPartitionIdx)

	if cluster.liveNodes() != 3 {
		t.Fatalf("expected 3 live nodes (majority), got %d", cluster.liveNodes())
	}

	// The majority (3 nodes) must elect a new leader.
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)

	if newLeaderIdx == oldLeaderIdx {
		t.Fatal("new leader must not be the partitioned old leader")
	}
	if cluster.stopped[newLeaderIdx].Load() {
		t.Fatal("new leader index points to a stopped node")
	}

	t.Logf("new leader elected: node[%d]", newLeaderIdx)

	// Verify the new leader's term has advanced beyond the old leader's term.
	// A new election always increments the term.
	waitForCondition(t, failoverTimeout,
		"new leader term advances beyond old leader term",
		func() bool {
			info, infoErr := cluster.hosts[newLeaderIdx].ShardInfo(shardID)
			if infoErr != nil {
				return false
			}
			return info.Term > oldLeaderTerm
		},
	)

	newLeaderInfo, err := cluster.hosts[newLeaderIdx].ShardInfo(shardID)
	if err != nil {
		t.Fatalf("ShardInfo for new leader failed: %v", err)
	}
	t.Logf("new leader node[%d] at term=%d (old term=%d)",
		newLeaderIdx, newLeaderInfo.Term, oldLeaderTerm)

	// The new leader must accept proposals.
	for i := range 5 {
		key := fmt.Sprintf("during-major-%03d", i)
		value := fmt.Sprintf("major-val-%03d", i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}

	// Verify all data on the 3 live nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			continue
		}
		for key, val := range expected {
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("node[%d] has key=%q", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	t.Logf("majority partition committed %d entries under new leader", len(expected))

	// Heal partition: remove stopped nodes from membership and add
	// replacements. This simulates the old leader stepping down and
	// rejoining as a follower (via replacement).
	stoppedNodes := []int{oldLeaderIdx, additionalPartitionIdx}

	for idx, stoppedIdx := range stoppedNodes {
		oldReplicaID := uint64(stoppedIdx + 1)

		syncDeleteNode(t, cluster, shardID, oldReplicaID, membershipTimeout)

		t.Logf("removed stopped node[%d] (replica=%d) from membership",
			stoppedIdx, oldReplicaID)

		newReplicaID := uint64(10 + idx)
		cfgFn := func(sid, rid uint64) config.Config {
			return testShardConfig(sid, rid)
		}
		result := addNodeToCluster(t, cluster, shardID, newReplicaID, cfgFn)
		t.Logf("added replacement node[%d] (replica=%d) at %s",
			result.idx, newReplicaID, result.addr)
	}

	// Write post-heal data.
	proposeAndWait(t, cluster, trackers, shardID, "post-major-heal", "converged")
	expected["post-major-heal"] = "converged"

	// Verify all live nodes converge.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}

		// See replicaIDAt: the host's own answer, not i+1. A live host
		// that reports no replica for this shard never started it or has
		// already lost it, which this loop fails on a line below when the SM
		// lookup comes back nil -- so it fails here too, rather than becoming
		// a node quietly dropped from a convergence check.
		replicaID, ok := cluster.replicaIDAt(shardID, i)
		if !ok {
			t.Fatalf("node[%d] is live but reports no replica for shard %d", i, shardID)
		}

		tracker := cluster.trackers[i]
		if tracker == nil {
			continue
		}

		nodeSM := tracker.Get(shardID, replicaID)
		if nodeSM == nil {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] replica=%d SM registered", i, replicaID),
				func() bool {
					nodeSM = tracker.Get(shardID, replicaID)
					return nodeSM != nil
				},
			)
		}

		for key, val := range expected {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] has key=%q after major heal", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	// Verify that all replacement nodes observe the advanced term.
	for i := 5; i < len(cluster.hosts); i++ {
		if cluster.stopped[i].Load() {
			continue
		}
		waitForCondition(t, membershipTimeout,
			fmt.Sprintf("replacement node[%d] sees advanced term", i),
			func() bool {
				info, infoErr := cluster.hosts[i].ShardInfo(shardID)
				if infoErr != nil {
					return false
				}
				return info.Term >= newLeaderInfo.Term
			},
		)
	}

	t.Logf("leader-majority partition healed: all %d live nodes converged with %d entries",
		cluster.liveNodes(), len(expected))
}

// TestE2E_AsymmetricPartition_FollowerIsolated checks the majority serves proposals while one follower is stopped and its replacement catches up.
func TestE2E_AsymmetricPartition_FollowerIsolated(t *testing.T) {
	const shardID uint64 = 1

	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// Warm up and write pre-partition data.
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")
	waitForReplication(t, cluster, trackers, shardID, "warmup", "ok", defaultTimeout)

	expected := map[string]string{"warmup": "ok"}
	for i := range 5 {
		key := fmt.Sprintf("pre-follower-%03d", i)
		value := fmt.Sprintf("pre-fval-%03d", i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}
	for key, val := range expected {
		waitForReplication(t, cluster, trackers, shardID, key, val, defaultTimeout)
	}

	t.Logf("pre-partition data replicated to all 5 nodes, leader is node[%d]", leaderIdx)

	// Isolate one follower.
	isolatedIdx := cluster.findFollower(shardID)
	if isolatedIdx < 0 {
		t.Fatal("no follower found to isolate")
	}
	isolatedReplicaID := uint64(isolatedIdx + 1)

	t.Logf("isolating follower node[%d] (replica=%d)", isolatedIdx, isolatedReplicaID)
	cluster.stopNode(isolatedIdx)

	if cluster.liveNodes() != 4 {
		t.Fatalf("expected 4 live nodes during partition, got %d", cluster.liveNodes())
	}

	// Leadership should be retained (4 of 5 nodes still connected).
	activeLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("active leader during follower isolation: node[%d]", activeLeaderIdx)

	// The majority continues committing proposals that the isolated
	// follower will miss.
	for i := range 10 {
		key := fmt.Sprintf("during-follower-%03d", i)
		value := fmt.Sprintf("fval-%03d", i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}

	// Verify all data on the 4 live nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			continue
		}
		for key, val := range expected {
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("node[%d] has key=%q during follower isolation", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	t.Logf("majority (4 nodes) committed %d entries while follower node[%d] is isolated",
		len(expected), isolatedIdx)

	// Heal: remove the isolated follower and add a replacement that
	// must catch up via log replication or snapshot transfer.

	syncDeleteNode(t, cluster, shardID, isolatedReplicaID, membershipTimeout)

	t.Logf("removed isolated follower node[%d] (replica=%d) from membership",
		isolatedIdx, isolatedReplicaID)

	newReplicaID := uint64(10)
	cfgFn := func(sid, rid uint64) config.Config {
		return testShardConfig(sid, rid)
	}
	result := addNodeToCluster(t, cluster, shardID, newReplicaID, cfgFn)
	t.Logf("added replacement node[%d] (replica=%d) at %s",
		result.idx, newReplicaID, result.addr)

	// Write post-heal data.
	proposeAndWait(t, cluster, trackers, shardID, "post-follower-heal", "caught-up")
	expected["post-follower-heal"] = "caught-up"

	// Verify the replacement node catches up with all data (pre-partition,
	// during-partition, and post-heal).
	waitForNodeData(t, result.tracker, shardID, newReplicaID, expected, membershipTimeout)

	// Verify all live nodes have consistent state.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}

		// See replicaIDAt: the host's own answer, not i+1. A live host
		// that reports no replica for this shard never started it or has
		// already lost it, which this loop fails on a line below when the SM
		// lookup comes back nil -- so it fails here too, rather than becoming
		// a node quietly dropped from a convergence check.
		replicaID, ok := cluster.replicaIDAt(shardID, i)
		if !ok {
			t.Fatalf("node[%d] is live but reports no replica for shard %d", i, shardID)
		}

		tracker := cluster.trackers[i]
		if tracker == nil {
			continue
		}

		nodeSM := tracker.Get(shardID, replicaID)
		if nodeSM == nil {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] replica=%d SM registered", i, replicaID),
				func() bool {
					nodeSM = tracker.Get(shardID, replicaID)
					return nodeSM != nil
				},
			)
		}

		for key, val := range expected {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] has key=%q after follower heal", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	t.Logf("follower isolation healed: all %d live nodes converged with %d entries",
		cluster.liveNodes(), len(expected))
}

// TestE2E_AsymmetricPartition_FollowerIsolated_SnapshotCatchup checks the replacement catches up by snapshot after compaction during the partition.
func TestE2E_AsymmetricPartition_FollowerIsolated_SnapshotCatchup(t *testing.T) {
	const shardID uint64 = 1

	cluster := newTestCluster(t, 5)
	defer cluster.close()

	// Use aggressive snapshot settings to force compaction during partition.
	cfgFn := func(sid, rid uint64) config.Config {
		return testShardConfigWithSnapshot(sid, rid, 5, 2)
	}
	trackers := cluster.startShardWithConfig(shardID, cfgFn)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// Warm up.
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")
	waitForReplication(t, cluster, trackers, shardID, "warmup", "ok", defaultTimeout)

	expected := map[string]string{"warmup": "ok"}

	t.Logf("cluster ready with snapshot settings, leader is node[%d]", leaderIdx)

	// Isolate one follower.
	isolatedIdx := cluster.findFollower(shardID)
	if isolatedIdx < 0 {
		t.Fatal("no follower found to isolate")
	}
	isolatedReplicaID := uint64(isolatedIdx + 1)

	t.Logf("isolating follower node[%d] (replica=%d) for snapshot catchup test",
		isolatedIdx, isolatedReplicaID)
	cluster.stopNode(isolatedIdx)

	// Write enough entries to trigger multiple snapshot cycles.
	// With SnapshotEntries=5, we need more than 5 entries to trigger
	// compaction. Write 20 entries to ensure the log is compacted
	// well past the isolated follower's last known index.
	activeLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("active leader during isolation: node[%d]", activeLeaderIdx)

	// Capture LastApplied before the writes.
	initInfo, initErr := cluster.hosts[activeLeaderIdx].ShardInfo(shardID)
	initApplied := uint64(0)
	if initErr == nil {
		initApplied = initInfo.LastApplied
	}

	for i := range 20 {
		key := fmt.Sprintf("snap-entry-%03d", i)
		value := fmt.Sprintf("snap-val-%03d", i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}

	// Wait for the entries to be applied on the leader. Since
	// proposeAndWait blocks until SM apply, LastApplied should
	// already be past initApplied+20.
	waitForCondition(t, 10*time.Second,
		"entries applied on leader after isolation writes",
		func() bool {
			info, err := cluster.hosts[activeLeaderIdx].ShardInfo(shardID)
			return err == nil && info.LastApplied >= initApplied+20
		},
	)
	// Applied is not compacted: the snapshot those applies trigger, and the
	// compaction behind it, finish asynchronously. The isolated follower is
	// stopped, so only the live voters are awaited.
	waitForLogCompacted(t, cluster, shardID, membershipTimeout)

	t.Logf("wrote 20 entries during isolation and the log is compacted")

	// Heal: replace the isolated follower.

	syncDeleteNode(t, cluster, shardID, isolatedReplicaID, membershipTimeout)

	newReplicaID := uint64(10)
	result := addNodeToCluster(t, cluster, shardID, newReplicaID, cfgFn)
	t.Logf("added replacement node[%d] (replica=%d) for snapshot catchup",
		result.idx, newReplicaID)

	// The replacement catches up by snapshot; verify its data.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, expected, membershipTimeout)

	// ...and verify the recovery path itself. This test exists to exercise
	// "snapshot-based recovery path rather than log replication", and the key
	// set cannot tell the two apart: both leave the same data behind. The
	// state machine's own RecoverFromSnapshot count can.
	assertCaughtUpViaSnapshot(t, result.tracker, shardID, newReplicaID, membershipTimeout)
	assertSnapshotTaken(t, cluster, activeLeaderIdx)

	// Write one more entry to confirm the cluster is fully operational
	// after snapshot-based recovery.
	proposeAndWait(t, cluster, trackers, shardID, "post-snap-heal", "snapshot-ok")
	expected["post-snap-heal"] = "snapshot-ok"

	// Verify the replacement also receives the post-heal entry.
	waitForNodeData(t, result.tracker, shardID, newReplicaID,
		map[string]string{"post-snap-heal": "snapshot-ok"}, membershipTimeout)

	t.Logf("follower snapshot catchup verified: replacement node[%d] has all %d entries",
		result.idx, len(expected))
}
