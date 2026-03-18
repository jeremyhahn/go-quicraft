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
)

// TestIntegration_DB_SyncRead_FromFollower verifies that linearizable reads
// via SyncRead work correctly when issued from a follower node, not just
// the leader. Per Raft, linearizable reads require consensus confirmation
// but can be served from any node that completes the ReadIndex protocol.
func TestIntegration_DB_SyncRead_FromFollower(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose a key-value pair via the leader.
	proposeAndWait(t, cluster, trackers, 1, "follower-read-key", "follower-read-value")
	waitForReplication(t, cluster, trackers, 1, "follower-read-key", "follower-read-value", defaultTimeout)

	// Find a follower node.
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
	t.Logf("using follower node[%d] for SyncRead", followerIdx)

	// Issue a linearizable SyncRead from the follower.
	followerHost := cluster.hosts[followerIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	result, err := followerHost.SyncRead(ctx, 1, encodeKVGet("follower-read-key"))
	if err != nil {
		t.Fatalf("SyncRead from follower failed: %v", err)
	}

	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", result)
	}
	if val != "follower-read-value" {
		t.Fatalf("SyncRead from follower = %q, want %q", val, "follower-read-value")
	}
	t.Logf("SyncRead from follower[%d] = %q (correct)", followerIdx, val)

	// Also verify reading a different key from another follower (if available).
	proposeAndWait(t, cluster, trackers, 1, "follower-read-key2", "follower-read-value2")
	waitForReplication(t, cluster, trackers, 1, "follower-read-key2", "follower-read-value2", defaultTimeout)

	secondFollowerIdx := -1
	for i := range cluster.hosts {
		if !cluster.stopped[i].Load() && i != leaderIdx && i != followerIdx {
			secondFollowerIdx = i
			break
		}
	}
	if secondFollowerIdx >= 0 {
		secondFollowerHost := cluster.hosts[secondFollowerIdx]
		ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel2()

		result2, err2 := secondFollowerHost.SyncRead(ctx2, 1, encodeKVGet("follower-read-key2"))
		if err2 != nil {
			t.Fatalf("SyncRead from second follower failed: %v", err2)
		}
		val2, ok2 := result2.(string)
		if !ok2 {
			t.Fatalf("SyncRead returned type %T, want string", result2)
		}
		if val2 != "follower-read-value2" {
			t.Fatalf("SyncRead from second follower = %q, want %q", val2, "follower-read-value2")
		}
		t.Logf("SyncRead from second follower[%d] = %q (correct)", secondFollowerIdx, val2)
	}
}

// TestIntegration_DB_SyncRead_AfterLeaderChange verifies that linearizable
// reads return correct data after a leader change. Per Raft Section 8, a new
// leader must commit a no-op entry before serving reads to ensure its log is
// up to date. This test verifies that both old (pre-failover) and new
// (post-failover) entries are readable after a leader change.
func TestIntegration_DB_SyncRead_AfterLeaderChange(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("initial leader at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose entries before failover.
	proposeAndWait(t, cluster, trackers, 1, "pre-leader-change", "old-value")
	waitForReplication(t, cluster, trackers, 1, "pre-leader-change", "old-value", defaultTimeout)

	proposeAndWait(t, cluster, trackers, 1, "pre-leader-change-2", "old-value-2")
	waitForReplication(t, cluster, trackers, 1, "pre-leader-change-2", "old-value-2", defaultTimeout)
	t.Logf("pre-failover entries proposed and replicated")

	// Stop the leader to force a new election.
	oldLeaderIdx := leaderIdx
	cluster.stopNode(oldLeaderIdx)
	t.Logf("stopped leader node[%d]", oldLeaderIdx)

	// Wait for new leader election.
	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	if newLeaderIdx == oldLeaderIdx {
		t.Fatal("new leader must not be the stopped node")
	}
	t.Logf("new leader at node[%d]", newLeaderIdx)

	// Warmup after new election (no-op commit per Section 8).
	proposeAndWait(t, cluster, trackers, 1, "post-warmup", "ok")

	// Propose a new entry via the new leader.
	proposeAndWait(t, cluster, trackers, 1, "post-leader-change", "new-value")
	t.Logf("post-failover entry proposed")

	// Verify linearizable reads of old entries on the new leader.
	newLeaderHost := cluster.hosts[newLeaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	oldResult, oldErr := newLeaderHost.SyncRead(ctx, 1, encodeKVGet("pre-leader-change"))
	if oldErr != nil {
		t.Fatalf("SyncRead of old entry after leader change failed: %v", oldErr)
	}
	oldVal, ok := oldResult.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", oldResult)
	}
	if oldVal != "old-value" {
		t.Fatalf("SyncRead(pre-leader-change) = %q, want %q", oldVal, "old-value")
	}
	t.Logf("old entry readable after leader change: %q = %q", "pre-leader-change", oldVal)

	// Verify linearizable reads of old entry #2.
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel2()
	oldResult2, oldErr2 := newLeaderHost.SyncRead(ctx2, 1, encodeKVGet("pre-leader-change-2"))
	if oldErr2 != nil {
		t.Fatalf("SyncRead of old entry 2 after leader change failed: %v", oldErr2)
	}
	oldVal2, ok2 := oldResult2.(string)
	if !ok2 {
		t.Fatalf("SyncRead returned type %T, want string", oldResult2)
	}
	if oldVal2 != "old-value-2" {
		t.Fatalf("SyncRead(pre-leader-change-2) = %q, want %q", oldVal2, "old-value-2")
	}
	t.Logf("old entry 2 readable after leader change: %q = %q", "pre-leader-change-2", oldVal2)

	// Verify linearizable reads of the new entry.
	ctx3, cancel3 := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel3()
	newResult, newErr := newLeaderHost.SyncRead(ctx3, 1, encodeKVGet("post-leader-change"))
	if newErr != nil {
		t.Fatalf("SyncRead of new entry after leader change failed: %v", newErr)
	}
	newVal, ok3 := newResult.(string)
	if !ok3 {
		t.Fatalf("SyncRead returned type %T, want string", newResult)
	}
	if newVal != "new-value" {
		t.Fatalf("SyncRead(post-leader-change) = %q, want %q", newVal, "new-value")
	}
	t.Logf("new entry readable after leader change: %q = %q", "post-leader-change", newVal)

	// Verify a follower also serves correct reads after leader change.
	followerIdx := -1
	for i := range cluster.hosts {
		if !cluster.stopped[i].Load() && i != newLeaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx >= 0 {
		followerHost := cluster.hosts[followerIdx]
		ctx4, cancel4 := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel4()

		followerResult, followerErr := followerHost.SyncRead(ctx4, 1, encodeKVGet("post-leader-change"))
		if followerErr != nil {
			t.Fatalf("SyncRead from follower after leader change failed: %v", followerErr)
		}
		followerVal, ok4 := followerResult.(string)
		if !ok4 {
			t.Fatalf("SyncRead returned type %T, want string", followerResult)
		}
		if followerVal != "new-value" {
			t.Fatalf("follower SyncRead(post-leader-change) = %q, want %q", followerVal, "new-value")
		}
		t.Logf("follower[%d] also serves correct reads after leader change", followerIdx)
	}
}
