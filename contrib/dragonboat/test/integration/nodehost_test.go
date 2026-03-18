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

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
)

// TestIntegration_DB_ProposeAndRead verifies that a 3-node cluster using the
// dragonboat adapter API can propose entries and serve linearizable reads
// via SyncRead.
func TestIntegration_DB_ProposeAndRead(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 10 entries through the dragonboat adapter.
	expected := proposeNEntries(t, cluster, trackers, 1, 10, "db-rw")

	// Verify all entries replicated to all 3 nodes.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all 10 entries replicated to 3 nodes via dragonboat adapter")

	// Perform a linearizable SyncRead from the leader.
	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	targetKey := "db-rw-005"
	expectedValue := expected[targetKey]
	result, err := leaderHost.SyncRead(ctx, 1, encodeKVGet(targetKey))
	if err != nil {
		t.Fatalf("SyncRead(%q) failed: %v", targetKey, err)
	}

	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", result)
	}
	if val != expectedValue {
		t.Fatalf("SyncRead(%q) = %q, want %q", targetKey, val, expectedValue)
	}
	t.Logf("SyncRead(%q) = %q (correct)", targetKey, val)
}

// TestIntegration_DB_Replication verifies that a 3-node cluster using the
// dragonboat adapter replicates 30 entries to all nodes with identical
// state machine data.
func TestIntegration_DB_Replication(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 30 entries.
	expected := proposeNEntries(t, cluster, trackers, 1, 30, "db-repl")

	// Verify all entries on all 3 nodes.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all 30 entries verified on 3 nodes via dragonboat adapter")

	// Compare state machine snapshots across all nodes.
	snapshots := make([]map[string]string, 0, 3)
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || i >= len(trackers) || trackers[i] == nil {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] has no state machine", i)
		}
		snap := nodeSM.Snapshot()
		snapshots = append(snapshots, snap)
	}

	if len(snapshots) < 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(snapshots))
	}

	reference := snapshots[0]
	for i := 1; i < len(snapshots); i++ {
		if len(snapshots[i]) != len(reference) {
			t.Fatalf("node[%d] has %d keys, node[0] has %d keys",
				i, len(snapshots[i]), len(reference))
		}
		for key, refVal := range reference {
			got, exists := snapshots[i][key]
			if !exists {
				t.Fatalf("node[%d] missing key %q present on node[0]", i, key)
			}
			if got != refVal {
				t.Fatalf("node[%d] key %q = %q, node[0] has %q", i, key, got, refVal)
			}
		}
	}
	t.Logf("all 3 nodes have identical state machine data (%d keys)", len(reference))
}

// TestIntegration_DB_AsyncPropose verifies the dragonboat async Propose +
// ResultC pattern works correctly through the adapter.
func TestIntegration_DB_AsyncPropose(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	leaderHost := cluster.hosts[leaderIdx]
	session := dbClient.NewNoOPSession(1, nil)

	// Propose 10 entries using the async Propose + ResultC pattern.
	const numEntries = 10
	expected := make(map[string]string, numEntries)
	for i := range numEntries {
		key := fmt.Sprintf("async-%03d", i)
		value := fmt.Sprintf("val-async-%03d", i)
		expected[key] = value

		cmd := encodeKVPut(key, value)
		rs, err := leaderHost.Propose(session, cmd, defaultTimeout)
		if err != nil {
			t.Fatalf("Propose(%q) failed: %v", key, err)
		}

		// Wait for the result via ResultC (dragonboat async pattern).
		select {
		case result := <-rs.ResultC():
			if !result.Completed() {
				t.Fatalf("Propose(%q) did not complete successfully", key)
			}
			smResult := result.GetResult()
			if smResult.Value != uint64(len(value)) {
				t.Errorf("Propose(%q) result value = %d, want %d",
					key, smResult.Value, len(value))
			}
		case <-time.After(defaultTimeout):
			t.Fatalf("Propose(%q) timed out waiting for ResultC", key)
		}
		rs.Release()
	}

	// Verify all entries replicated to all nodes.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all %d async entries verified on all nodes", numEntries)
}

// TestIntegration_DB_LeaderFailover verifies that a 3-node cluster using
// the dragonboat adapter correctly handles leader failure and elects a
// new leader that can serve continued proposals.
func TestIntegration_DB_LeaderFailover(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("initial leader at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose entries before failover.
	expected := proposeNEntries(t, cluster, trackers, 1, 10, "pre-failover")
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("10 pre-failover entries verified")

	// Kill the leader.
	oldLeaderIdx := cluster.findLeader(1)
	if oldLeaderIdx < 0 {
		t.Fatal("no leader found before failover")
	}
	t.Logf("stopping leader node[%d]", oldLeaderIdx)
	cluster.stopNode(oldLeaderIdx)

	// Wait for new leader election.
	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	if newLeaderIdx == oldLeaderIdx {
		t.Fatal("new leader must not be the stopped node")
	}
	t.Logf("new leader elected at node[%d]", newLeaderIdx)

	// Warmup after new election.
	proposeAndWait(t, cluster, trackers, 1, "post-warmup", "ok")

	// Propose entries after failover through the new leader.
	for i := range 10 {
		key := fmt.Sprintf("post-failover-%03d", i)
		value := fmt.Sprintf("val-post-failover-%03d", i)
		proposeAndWait(t, cluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Verify all entries on surviving nodes.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all 20 entries verified on surviving nodes after failover")
}

// TestIntegration_DB_Membership verifies that SyncGetShardMembership returns
// the correct cluster topology through the dragonboat adapter.
func TestIntegration_DB_Membership(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	membership, err := leaderHost.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}

	// Verify 3 voting members.
	if len(membership.Nodes) != 3 {
		t.Fatalf("expected 3 nodes in membership, got %d", len(membership.Nodes))
	}

	// Verify each replica maps to its expected address.
	for i, addr := range cluster.addrs {
		replicaID := uint64(i + 1)
		memberAddr, exists := membership.Nodes[replicaID]
		if !exists {
			t.Fatalf("replica %d not found in membership", replicaID)
		}
		if memberAddr != addr {
			t.Fatalf("replica %d address = %q, want %q", replicaID, memberAddr, addr)
		}
	}

	// Verify no non-voting or witness members.
	if len(membership.NonVotings) != 0 {
		t.Fatalf("expected 0 non-votings, got %d", len(membership.NonVotings))
	}
	if len(membership.Witnesses) != 0 {
		t.Fatalf("expected 0 witnesses, got %d", len(membership.Witnesses))
	}

	t.Logf("membership verified: %d voting nodes, ConfigChangeID=%d",
		len(membership.Nodes), membership.ConfigChangeID)
}

// TestIntegration_DB_StaleRead verifies that StaleRead returns data
// without consensus through the dragonboat adapter.
func TestIntegration_DB_StaleRead(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup and propose data.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	proposeAndWait(t, cluster, trackers, 1, "stale-key", "stale-value")
	waitForReplication(t, cluster, trackers, 1, "stale-key", "stale-value", defaultTimeout)

	// StaleRead from a follower node to exercise the non-consensus path.
	followerIdx := -1
	for i := range cluster.hosts {
		if !cluster.stopped[i].Load() && i != leaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	followerHost := cluster.hosts[followerIdx]

	// Wait for the data to be applied on the follower before issuing
	// StaleRead to avoid reading before replication arrives.
	replicaID := uint64(followerIdx + 1)
	followerSM := trackers[followerIdx].Get(1, replicaID)
	waitForCondition(t, defaultTimeout, "follower has stale-key", func() bool {
		_, ok := followerSM.Get("stale-key")
		return ok
	})

	result, err := followerHost.StaleRead(1, encodeKVGet("stale-key"))
	if err != nil {
		t.Fatalf("StaleRead failed: %v", err)
	}

	val, ok := result.(string)
	if !ok {
		t.Fatalf("StaleRead returned type %T, want string", result)
	}
	if val != "stale-value" {
		t.Fatalf("StaleRead = %q, want %q", val, "stale-value")
	}
	t.Logf("StaleRead from follower[%d] = %q (correct)", followerIdx, val)
}

// TestIntegration_DB_FiveNode verifies that a 5-node cluster using the
// dragonboat adapter handles proposals, replication, leader failover, and
// continued writes under a larger quorum (3 of 5).
func TestIntegration_DB_FiveNode(t *testing.T) {
	cluster := newDBTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("5-node cluster leader at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 20 entries.
	expected := proposeNEntries(t, cluster, trackers, 1, 20, "db5n")

	// Verify all 20 entries on all 5 nodes.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("20 entries verified on all 5 nodes")

	// Kill the leader.
	oldLeaderIdx := cluster.findLeader(1)
	if oldLeaderIdx < 0 {
		t.Fatal("no leader found before failover")
	}
	t.Logf("stopping leader node[%d]", oldLeaderIdx)
	cluster.stopNode(oldLeaderIdx)

	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	if newLeaderIdx == oldLeaderIdx {
		t.Fatal("new leader must not be the stopped node")
	}
	t.Logf("new leader elected at node[%d] after failover", newLeaderIdx)

	// Warmup after new election.
	proposeAndWait(t, cluster, trackers, 1, "post-warmup", "ok")

	// Propose 10 more entries through the new leader.
	for i := range 10 {
		key := fmt.Sprintf("db5n-post-%03d", i)
		value := fmt.Sprintf("val-db5n-post-%03d", i)
		proposeAndWait(t, cluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Verify all 30 entries on the 4 surviving nodes.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all 30 entries verified on 4 surviving nodes after failover")
}
