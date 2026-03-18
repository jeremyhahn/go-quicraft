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

// TestE2E_ThreeNodeCluster_LeaderElection verifies that a 3-node cluster
// elects exactly one leader for a shard.
func TestE2E_ThreeNodeCluster_LeaderElection(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	if leaderIdx < 0 || leaderIdx >= 3 {
		t.Fatalf("expected leader index in [0,2], got %d", leaderIdx)
	}

	// Wait for exactly one leader. During election transitions, multiple
	// nodes may momentarily report IsLeader before convergence.
	waitForCondition(t, leaderElectionTimeout, "exactly 1 leader", func() bool {
		count := 0
		for i := range cluster.hosts {
			if cluster.stopped[i].Load() {
				continue
			}
			info, err := cluster.hosts[i].GetShardInfo(1)
			if err != nil {
				return false
			}
			if info.IsLeader {
				count++
			}
		}
		return count == 1
	})

	t.Logf("leader elected at node index %d", leaderIdx)
}

// TestE2E_ThreeNodeCluster_AllNodesAgreeOnLeader verifies that all nodes
// in a 3-node cluster converge on the same leader ID for a shard.
func TestE2E_ThreeNodeCluster_AllNodesAgreeOnLeader(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Wait for all nodes to converge on the same non-zero leader.
	// After one node becomes leader, followers need a heartbeat round
	// to learn the leader ID.
	var agreedLeader uint64
	waitForCondition(t, leaderElectionTimeout, "all nodes agree on leader", func() bool {
		var leaderID uint64
		for i, h := range cluster.hosts {
			if cluster.stopped[i].Load() {
				continue
			}
			info, err := h.GetShardInfo(1)
			if err != nil || info.LeaderID == 0 {
				return false
			}
			if leaderID == 0 {
				leaderID = info.LeaderID
			} else if info.LeaderID != leaderID {
				return false
			}
		}
		agreedLeader = leaderID
		return leaderID > 0
	})

	t.Logf("all 3 nodes agree on leader replica ID %d", agreedLeader)
}

// TestE2E_FiveNodeCluster_LeaderElection verifies that a 5-node cluster
// successfully elects a leader.
func TestE2E_FiveNodeCluster_LeaderElection(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	if leaderIdx < 0 || leaderIdx >= 5 {
		t.Fatalf("expected leader index in [0,4], got %d", leaderIdx)
	}

	// Wait for exactly one leader. During election transitions, multiple
	// nodes may momentarily report IsLeader before convergence.
	waitForCondition(t, leaderElectionTimeout, "exactly 1 leader in 5-node cluster", func() bool {
		count := 0
		for i := range cluster.hosts {
			if cluster.stopped[i].Load() {
				continue
			}
			info, err := cluster.hosts[i].GetShardInfo(1)
			if err != nil {
				return false
			}
			if info.IsLeader {
				count++
			}
		}
		return count == 1
	})

	t.Logf("5-node cluster elected leader at node index %d", leaderIdx)
}

// TestE2E_MultipleShards_IndependentLeaders verifies that multiple shards
// each elect their own leader independently within the same cluster.
func TestE2E_MultipleShards_IndependentLeaders(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	shardIDs := []uint64{1, 2, 3}
	allTrackers := make([][]*smTracker, len(shardIDs))

	for i, sid := range shardIDs {
		allTrackers[i] = cluster.startShard(sid)
	}

	// Wait for each shard to elect a leader.
	leaders := make(map[uint64]int, len(shardIDs))
	for _, sid := range shardIDs {
		leaderIdx := cluster.waitForClusterLeader(sid, leaderElectionTimeout)
		if leaderIdx < 0 {
			t.Fatalf("shard %d failed to elect a leader", sid)
		}
		leaders[sid] = leaderIdx
		t.Logf("shard %d elected leader at node index %d", sid, leaderIdx)
	}

	// Each shard must have a valid leader.
	for _, sid := range shardIDs {
		idx := leaders[sid]
		if idx < 0 || idx >= 3 {
			t.Fatalf("shard %d leader index %d out of range", sid, idx)
		}
	}

	t.Logf("all %d shards have independent leaders", len(shardIDs))
}

// TestE2E_FiveNodeCluster_AllNodesAgreeOnLeader verifies that all 5 nodes
// in a cluster converge on the same leader ID for a shard. With 5 nodes,
// the leader needs heartbeats to propagate to 4 followers.
func TestE2E_FiveNodeCluster_AllNodesAgreeOnLeader(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Wait for all 5 nodes to converge on the same non-zero leader.
	// After one node becomes leader, followers need a heartbeat round
	// to learn the leader ID. With 5 nodes and real QUIC transport,
	// this may take slightly longer than a 3-node cluster.
	var agreedLeader uint64
	waitForCondition(t, leaderElectionTimeout, "all 5 nodes agree on leader", func() bool {
		var leaderID uint64
		for i, h := range cluster.hosts {
			if cluster.stopped[i].Load() {
				continue
			}
			info, err := h.GetShardInfo(1)
			if err != nil || info.LeaderID == 0 {
				return false
			}
			if leaderID == 0 {
				leaderID = info.LeaderID
			} else if info.LeaderID != leaderID {
				return false
			}
		}
		agreedLeader = leaderID
		return leaderID > 0
	})

	t.Logf("all 5 nodes agree on leader replica ID %d", agreedLeader)
}

// TestE2E_FiveNodeCluster_MultipleShards verifies that 3 shards on a
// 5-node cluster each elect their own leader independently.
func TestE2E_FiveNodeCluster_MultipleShards(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	shardIDs := []uint64{1, 2, 3}
	allTrackers := make([][]*smTracker, len(shardIDs))

	for i, sid := range shardIDs {
		allTrackers[i] = cluster.startShard(sid)
	}

	// Wait for each shard to elect a leader.
	leaders := make(map[uint64]int, len(shardIDs))
	for _, sid := range shardIDs {
		leaderIdx := cluster.waitForClusterLeader(sid, leaderElectionTimeout)
		if leaderIdx < 0 {
			t.Fatalf("shard %d failed to elect a leader", sid)
		}
		leaders[sid] = leaderIdx
		t.Logf("shard %d elected leader at node index %d", sid, leaderIdx)
	}

	// Each shard must have a valid leader in range [0, 4].
	for _, sid := range shardIDs {
		idx := leaders[sid]
		if idx < 0 || idx >= 5 {
			t.Fatalf("shard %d leader index %d out of range [0,4]", sid, idx)
		}
	}

	// Wait for exactly one leader per shard. During election transitions,
	// multiple nodes may momentarily report IsLeader before the old leader
	// steps down. Poll until convergence.
	for _, sid := range shardIDs {
		sid := sid
		waitForCondition(t, leaderElectionTimeout,
			fmt.Sprintf("shard %d exactly 1 leader", sid),
			func() bool {
				leaderCount := 0
				for i := range cluster.hosts {
					if cluster.stopped[i].Load() {
						continue
					}
					info, err := cluster.hosts[i].GetShardInfo(sid)
					if err != nil {
						return false
					}
					if info.IsLeader {
						leaderCount++
					}
				}
				return leaderCount == 1
			},
		)
	}

	// Verify each shard can accept proposals independently.
	for s, sid := range shardIDs {
		trackers := allTrackers[s]

		key := fmt.Sprintf("shard%d-key", sid)
		value := fmt.Sprintf("shard%d-value", sid)
		proposeAndWait(t, cluster, trackers, sid, key, value)
		waitForReplication(t, cluster, trackers, sid, key, value, defaultTimeout)
	}

	t.Logf("all %d shards on 5-node cluster have independent leaders and accept proposals", len(shardIDs))
}
