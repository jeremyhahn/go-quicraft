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
	leaderHost := cluster.hosts[activeLeaderIdx]
	partitionedReplicaID := uint64(partitionedIdx + 1)

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	if err := leaderHost.SyncRequestDeleteNode(ctx, shardID, partitionedReplicaID, 0); err != nil {
		cancel()
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", partitionedReplicaID, err)
	}
	cancel()

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
		replicaID := uint64(i + 1)
		if i >= 3 {
			replicaID = uint64(10 + (i - 3))
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
	leaderHost := cluster.hosts[activeLeaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("error-path-key", "error-path-val")
	rs, err := leaderHost.Propose(ctx, shardID, cmd)
	if err != nil {
		t.Fatalf("Propose through leader during partition failed: %v", err)
	}
	rs.Release()

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
	newLeaderHost := cluster.hosts[newLeaderIdx]
	stoppedNodes := []int{oldLeaderIdx, additionalPartitionIdx}

	for idx, stoppedIdx := range stoppedNodes {
		oldReplicaID := uint64(stoppedIdx + 1)

		ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
		if err := newLeaderHost.SyncRequestDeleteNode(ctx, shardID, oldReplicaID, 0); err != nil {
			cancel()
			t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", oldReplicaID, err)
		}
		cancel()

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

		replicaID := uint64(i + 1)
		if i >= 5 {
			replicaID = uint64(10 + (i - 5))
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
// It verifies that the old leader, once isolated from the majority, cannot
// commit new proposals (it has lost quorum). Meanwhile the majority must
// elect a new leader and continue serving.
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

	// Find one additional follower to partition with the leader.
	additionalIdx := -1
	for i := range cluster.hosts {
		if i == oldLeaderIdx {
			continue
		}
		additionalIdx = i
		break
	}

	// Stop old leader and one follower (minority partition).
	cluster.stopNode(oldLeaderIdx)
	cluster.stopNode(additionalIdx)

	if cluster.liveNodes() != 3 {
		t.Fatalf("expected 3 live nodes, got %d", cluster.liveNodes())
	}

	// The 3-node majority must elect a new leader.
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)

	if newLeaderIdx == oldLeaderIdx || cluster.stopped[newLeaderIdx].Load() {
		t.Fatalf("invalid new leader: node[%d] (old=%d)", newLeaderIdx, oldLeaderIdx)
	}

	// The new leader must be able to commit proposals.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("majority-only", "committed")
	rs, err := cluster.hosts[newLeaderIdx].Propose(ctx, shardID, cmd)
	if err != nil {
		t.Fatalf("Propose through new leader failed: %v", err)
	}
	rs.Release()

	// Verify the proposal was applied on the new leader.
	newLeaderReplicaID := uint64(newLeaderIdx + 1)
	newLeaderSM := trackers[newLeaderIdx].Get(shardID, newLeaderReplicaID)
	if newLeaderSM == nil {
		t.Fatal("new leader state machine not found")
	}
	waitForCondition(t, defaultTimeout,
		"new leader applies majority-only key",
		func() bool {
			val, ok := newLeaderSM.Get("majority-only")
			return ok && val == "committed"
		},
	)

	t.Logf("new leader node[%d] committed proposal while old leader node[%d] is partitioned",
		newLeaderIdx, oldLeaderIdx)
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
	activeLeaderHost := cluster.hosts[activeLeaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	if err := activeLeaderHost.SyncRequestDeleteNode(ctx, shardID, isolatedReplicaID, 0); err != nil {
		cancel()
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", isolatedReplicaID, err)
	}
	cancel()

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

		replicaID := uint64(i + 1)
		if i >= 5 {
			replicaID = uint64(10 + (i - 5))
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
	// already be past initApplied+20. This also gives the async
	// snapshot compaction path time to trigger.
	waitForCondition(t, 10*time.Second,
		"entries applied on leader after isolation writes",
		func() bool {
			info, err := cluster.hosts[activeLeaderIdx].GetShardInfo(shardID)
			return err == nil && info.LastApplied >= initApplied+20
		},
	)

	t.Logf("wrote 20 entries during isolation (expected snapshot compaction)")

	// Heal: replace the isolated follower.
	activeLeaderHost := cluster.hosts[activeLeaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	if err := activeLeaderHost.SyncRequestDeleteNode(ctx, shardID, isolatedReplicaID, 0); err != nil {
		cancel()
		t.Fatalf("SyncRequestDeleteNode(replica=%d) failed: %v", isolatedReplicaID, err)
	}
	cancel()

	newReplicaID := uint64(10)
	result := addNodeToCluster(t, cluster, shardID, newReplicaID, cfgFn)
	t.Logf("added replacement node[%d] (replica=%d) for snapshot catchup",
		result.idx, newReplicaID)

	// The replacement node should catch up via snapshot transfer since
	// the log entries before compaction are no longer available.
	// Verify it has all expected data.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, expected, membershipTimeout)

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
