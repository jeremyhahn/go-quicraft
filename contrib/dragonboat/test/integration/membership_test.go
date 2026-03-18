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
	"testing"
	"time"
)

// TestIntegration_DB_AsyncRequestAddReplica verifies the async membership
// change pattern: RequestAddReplica returns a *RequestState, and the caller
// waits on ResultC for the config change to commit.
func TestIntegration_DB_AsyncRequestAddReplica(t *testing.T) {
	// Start a 3-node cluster with shard 1.
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Get current membership to obtain the ConfigChangeID.
	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	membership, err := leaderHost.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}
	configChangeID := membership.ConfigChangeID

	// Use async RequestAddReplica to add a replica ID 10 (non-existent
	// address, but the config change should still commit).
	rs, err := leaderHost.RequestAddReplica(1, 10, "127.0.0.1:59999", configChangeID, defaultTimeout)
	if err != nil {
		t.Fatalf("RequestAddReplica failed: %v", err)
	}
	defer rs.Release()

	// Wait for the config change to commit via ResultC.
	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Fatalf("RequestAddReplica did not complete: code=%v", result)
		}
		t.Logf("RequestAddReplica completed successfully")
	case <-time.After(defaultTimeout):
		t.Fatal("RequestAddReplica timed out waiting for ResultC")
	}

	// Verify the membership now includes replica 10.
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel2()
	updatedMembership, err := leaderHost.SyncGetShardMembership(ctx2, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership after add failed: %v", err)
	}

	if _, exists := updatedMembership.Nodes[10]; !exists {
		t.Fatalf("replica 10 not found in updated membership: %v", updatedMembership.Nodes)
	}
	t.Logf("membership updated: %d nodes, ConfigChangeID=%d",
		len(updatedMembership.Nodes), updatedMembership.ConfigChangeID)
}

// TestIntegration_DB_SyncMembership_Roundtrip verifies SyncGetShardMembership
// returns complete membership data including Nodes, NonVotings, Witnesses,
// and ConfigChangeID fields.
func TestIntegration_DB_SyncMembership_Roundtrip(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	membership, err := leaderHost.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}

	// Verify Nodes has exactly 3 voting members.
	if len(membership.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(membership.Nodes))
	}

	// Verify each replica has the correct address.
	for i, addr := range cluster.addrs {
		replicaID := uint64(i + 1)
		memberAddr, exists := membership.Nodes[replicaID]
		if !exists {
			t.Fatalf("replica %d not found in Nodes", replicaID)
		}
		if memberAddr != addr {
			t.Fatalf("replica %d address = %q, want %q", replicaID, memberAddr, addr)
		}
	}

	// Verify NonVotings is empty.
	if len(membership.NonVotings) != 0 {
		t.Fatalf("expected 0 non-votings, got %d", len(membership.NonVotings))
	}

	// Verify Witnesses is empty.
	if len(membership.Witnesses) != 0 {
		t.Fatalf("expected 0 witnesses, got %d", len(membership.Witnesses))
	}

	// ConfigChangeID is 0 for initial bootstrap membership (no config change
	// entries have been applied yet). After a membership change, it will be
	// set to the log index of that change.
	t.Logf("membership roundtrip verified: Nodes=%d, NonVotings=%d, Witnesses=%d, ConfigChangeID=%d",
		len(membership.Nodes), len(membership.NonVotings), len(membership.Witnesses), membership.ConfigChangeID)
}

// TestIntegration_DB_RequestLeaderTransfer verifies that RequestLeaderTransfer
// moves leadership to the specified follower, which can then be confirmed
// via GetLeaderID.
func TestIntegration_DB_RequestLeaderTransfer(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("initial leader at node[%d] (replicaID=%d)", leaderIdx, leaderIdx+1)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Pick a target follower that is not the current leader.
	targetIdx := -1
	for i := range cluster.hosts {
		if !cluster.stopped[i].Load() && i != leaderIdx {
			targetIdx = i
			break
		}
	}
	if targetIdx < 0 {
		t.Fatal("no follower found for transfer target")
	}
	targetReplicaID := uint64(targetIdx + 1)
	t.Logf("requesting transfer to node[%d] (replicaID=%d)", targetIdx, targetReplicaID)

	// Request leader transfer from the current leader.
	leaderHost := cluster.hosts[leaderIdx]
	if err := leaderHost.RequestLeaderTransfer(1, targetReplicaID); err != nil {
		t.Fatalf("RequestLeaderTransfer failed: %v", err)
	}

	// Wait for the target to become leader.
	waitForCondition(t, failoverTimeout, "target becomes leader", func() bool {
		for i, nh := range cluster.hosts {
			if cluster.stopped[i].Load() {
				continue
			}
			leaderID, _, valid, err := nh.GetLeaderID(1)
			if err != nil || !valid {
				continue
			}
			if leaderID == targetReplicaID {
				return true
			}
		}
		return false
	})

	// Verify via GetLeaderID from the target host.
	targetHost := cluster.hosts[targetIdx]
	leaderID, _, valid, err := targetHost.GetLeaderID(1)
	if err != nil {
		t.Fatalf("GetLeaderID failed: %v", err)
	}
	if !valid {
		t.Fatal("GetLeaderID returned valid=false after transfer")
	}
	if leaderID != targetReplicaID {
		t.Fatalf("leader after transfer = %d, want %d", leaderID, targetReplicaID)
	}
	t.Logf("leader transfer confirmed: new leader replicaID=%d", leaderID)
}
