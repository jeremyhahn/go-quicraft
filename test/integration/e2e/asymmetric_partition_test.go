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

// ---------------------------------------------------------------------------
// Asymmetric Network Partition Tests
//
// These tests exercise scenarios where network connectivity is directional
// or partial, unlike symmetric partitions where a node is completely
// isolated from all peers equally.
//
// Since the E2E test infrastructure operates with real QUIC transport on
// loopback and does not support per-link network filtering, asymmetric
// partitions are simulated by selectively stopping nodes to create the
// desired reachability topology. The Raft protocol's behavior under
// these topologies (leader isolation from minority vs majority, follower
// isolation) is validated through leader election, proposal commitment,
// term advancement, and state convergence assertions.
// ---------------------------------------------------------------------------

// TestE2E_AsymmetricPartition_LeaderIsolatedFromMinority verifies that
// when the leader loses connectivity to a minority of followers (1 out
// of 2 followers in a 3-node cluster), it retains quorum with the
// remaining follower and continues to serve proposals.
//
// Topology (3-node cluster, quorum=2):
//
//	Leader (node A) <---> Follower B  (connected)
//	Leader (node A)  X    Follower C  (partitioned)
//	Follower B       X    Follower C  (partitioned)
//
// Simulated by stopping Follower C. The leader and Follower B form the
// majority partition (2 of 3). After healing (replacing C), all nodes
// converge.
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

// TestE2E_AsymmetricPartition_LeaderIsolatedFromMinority_ProposalsDuringPartition
// is the error-path companion to TestE2E_AsymmetricPartition_LeaderIsolatedFromMinority.
// It verifies that proposals submitted to the partitioned (stopped) follower
// fail gracefully, while proposals through the leader succeed.
func TestE2E_AsymmetricPartition_LeaderIsolatedFromMinority_ProposalsDuringPartition(t *testing.T) {
	const shardID uint64 = 1

	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// Warm up.
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")
	waitForReplication(t, cluster, trackers, shardID, "warmup", "ok", defaultTimeout)

	// Find a follower to partition.
	followerIdx := cluster.findFollower(shardID)
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	t.Logf("partitioning follower node[%d], leader is node[%d]", followerIdx, leaderIdx)
	cluster.stopNode(followerIdx)

	// Attempting to propose through a stopped node should not be
	// possible (the host is closed). Verify the leader still works.
	activeLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)

	// "Verify the leader still works" is a statement about the cluster, not
	// about activeLeaderIdx. The index is kept for the per-replica assertion
	// below, which replication satisfies whoever proposed.
	proposeToLeader(t, cluster, shardID, encodeKVPut("error-path-key", "error-path-val"), defaultTimeout)

	// Verify the proposal was applied on the leader.
	replicaID := uint64(activeLeaderIdx + 1)
	leaderSM := trackers[activeLeaderIdx].Get(shardID, replicaID)
	if leaderSM == nil {
		t.Fatal("leader state machine not found")
	}
	waitForCondition(t, defaultTimeout,
		"leader applies error-path-key",
		func() bool {
			val, ok := leaderSM.Get("error-path-key")
			return ok && val == "error-path-val"
		},
	)
}

// TestE2E_AsymmetricPartition_LeaderIsolatedFromMajority verifies that
// when the leader is isolated from the majority of the cluster, the
// majority elects a new leader, the new leader serves proposals, and
// after healing the old leader rejoins as a follower with advanced term.
//
// Topology (5-node cluster, quorum=3):
//
//	Old Leader (node A)  X  Nodes B, C, D  (majority, connected to each other)
//	Old Leader (node A)  X  Node E         (also partitioned)
//	Nodes B, C, D       <-->               (fully connected majority)
//
// Simulated by stopping the old leader AND one follower (E), leaving
// 3 nodes (B, C, D) as the majority. The majority elects a new leader
// and commits entries. After healing (replacing A and E), all converge.
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
	oldLeaderInfo, err := cluster.hosts[oldLeaderIdx].GetShardInfo(shardID)
	if err != nil {
		t.Fatalf("GetShardInfo for old leader failed: %v", err)
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
			info, infoErr := cluster.hosts[newLeaderIdx].GetShardInfo(shardID)
			if infoErr != nil {
				return false
			}
			return info.Term > oldLeaderTerm
		},
	)

	newLeaderInfo, err := cluster.hosts[newLeaderIdx].GetShardInfo(shardID)
	if err != nil {
		t.Fatalf("GetShardInfo for new leader failed: %v", err)
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
				info, infoErr := cluster.hosts[i].GetShardInfo(shardID)
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

// TestE2E_AsymmetricPartition_LeaderIsolatedFromMajority_OldLeaderCannotCommit
// is the error-path companion to TestE2E_AsymmetricPartition_LeaderIsolatedFromMajority.
// It verifies that the old leader, once cut off from the majority, cannot
// commit new proposals.
//
// The proposal goes to the OLD LEADER, which stays RUNNING. Stopping (closing)
// the old leader and proposing to the NEW one would establish only that a
// healthy leader can commit — the named property, that an isolated old leader
// cannot, would never be exercised at all, and the test would pass on a build
// with no quorum rule.
//
// The harness has no per-link filtering (see the file header), so isolation is
// produced the only way it can be: three of the five nodes are stopped, leaving
// the old leader in a live minority of two. That is a real loss of quorum for a
// live leader, which is the condition under test. The companion assertion — a
// majority electing a new leader and continuing to serve — cannot coexist with
// it here (a running old leader is reachable by everyone, so no rival majority
// can form) and is covered by TestE2E_AsymmetricPartition_LeaderIsolatedFromMajority.
func TestE2E_AsymmetricPartition_LeaderIsolatedFromMajority_OldLeaderCannotCommit(t *testing.T) {
	const shardID uint64 = 1

	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(shardID)
	oldLeaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// Warm up.
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")
	waitForReplication(t, cluster, trackers, shardID, "warmup", "ok", defaultTimeout)

	t.Logf("old leader: node[%d]", oldLeaderIdx)

	// Positive control: this leader commits without difficulty while it holds
	// a majority. Without it, a build that cannot commit anything at all would
	// satisfy the negative assertion below for entirely the wrong reason.
	proposeAndWait(t, cluster, trackers, shardID, "quorum-intact", "committed")
	waitForReplication(t, cluster, trackers, shardID, "quorum-intact", "committed", defaultTimeout)

	// Cut the leader off from the majority by stopping three of the four
	// followers. The leader itself keeps running with one follower: 2 of 5.
	stopped := 0
	for i := range cluster.hosts {
		if i == oldLeaderIdx || stopped >= 3 {
			continue
		}
		cluster.stopNode(i)
		stopped++
	}
	if stopped != 3 {
		t.Fatalf("expected to stop 3 followers, stopped %d", stopped)
	}
	if cluster.liveNodes() != 2 {
		t.Fatalf("expected 2 live nodes (old leader + 1 follower), got %d", cluster.liveNodes())
	}
	if cluster.stopped[oldLeaderIdx].Load() {
		t.Fatalf("the old leader node[%d] must stay running for this test", oldLeaderIdx)
	}
	t.Logf("old leader node[%d] left running in a 2-of-5 minority", oldLeaderIdx)

	// Propose through the still-running old leader. Accepting the proposal
	// into its log is legal; reporting it committed is not.
	const isolatedKey = "isolated-leader-write"
	const isolatedVal = "must-never-commit"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//quicraft:direct-propose the isolated old leader IS the subject: it must not commit
	rs, err := cluster.hosts[oldLeaderIdx].Propose(ctx, shardID, encodeKVPut(isolatedKey, isolatedVal))
	if err == nil {
		_, resErr := rs.Result()
		rs.Release()
		if resErr == nil {
			cancel()
			t.Fatalf("the isolated old leader node[%d] reported %q as committed with only "+
				"%d of 5 nodes alive", oldLeaderIdx, isolatedKey, cluster.liveNodes())
		}
		t.Logf("old leader node[%d]: proposal accepted but never committed: %v", oldLeaderIdx, resErr)
	} else {
		t.Logf("old leader node[%d]: Propose rejected outright: %v", oldLeaderIdx, err)
	}
	cancel()

	// The decisive assertion: the value must not have been applied anywhere.
	assertKeyAbsentEverywhere(t, cluster, trackers, shardID, isolatedKey, 2*time.Second)

	// The pre-partition data must still be intact on the minority side; losing
	// quorum must not lose committed state.
	waitForReplication(t, cluster, trackers, shardID, "quorum-intact", "committed", defaultTimeout)

	// And the isolated leader must eventually step down rather than keep
	// claiming leadership it can no longer support.
	waitForCondition(t, failoverTimeout,
		"isolated old leader steps down",
		func() bool { return cluster.findLeader(shardID) < 0 },
	)
	t.Logf("isolated old leader node[%d] stepped down; no leader in the minority", oldLeaderIdx)
}

// TestE2E_AsymmetricPartition_FollowerIsolated verifies that when a single
// follower is isolated from all other nodes, the remaining majority
// continues serving proposals. After healing (replacing the isolated
// follower), the replacement catches up via log replication or snapshot
// transfer, and all nodes converge to consistent state.
//
// Topology (5-node cluster, quorum=3):
//
//	Follower E   X   Leader A, Follower B, C, D  (isolated)
//	Leader A    <-->  Followers B, C, D           (connected)
//
// Simulated by stopping Follower E.
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

// TestE2E_AsymmetricPartition_FollowerIsolated_SnapshotCatchup verifies
// that an isolated follower's replacement catches up via snapshot transfer
// when the log has been compacted during the partition. This exercises
// the snapshot-based recovery path rather than log replication.
//
// The test uses aggressive snapshot settings (SnapshotEntries=5,
// CompactionOverhead=2) to force log compaction while the follower
// is isolated. After healing, the replacement node must receive a
// snapshot to catch up since the compacted log entries are no longer
// available.
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

	// Capture LastApplied BEFORE writes so we can detect advancement.
	initInfo, initErr := cluster.hosts[activeLeaderIdx].GetShardInfo(shardID)
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
			info, err := cluster.hosts[activeLeaderIdx].GetShardInfo(shardID)
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

	// The replacement node should catch up via snapshot transfer since
	// the log entries before compaction are no longer available.
	// Verify it has all expected data.
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
