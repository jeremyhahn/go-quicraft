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

package raft_test

import (
	"context"
	"testing"
)

// TestIntegration_StaleRead_MultiNode_FromFollower verifies that data
// written via the leader can be stale-read from a follower after the
// entry has been replicated.
func TestIntegration_StaleRead_MultiNode_FromFollower(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write via leader.
	cmd := encodeKVPut("multi-stale-key", "multi-stale-value")
	rs, err := leaderHost.Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose to leader failed: %v", err)
	}
	rs.Release()

	// Find a follower node.
	followerIdx := -1
	for i := range cluster.hosts {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx < 0 {
		t.Fatal("no follower found in cluster")
	}

	// Wait for the follower's state machine to replicate the entry.
	followerSM := trackers[followerIdx].Get(1, uint64(followerIdx+1))
	if followerSM == nil {
		t.Fatalf("follower[%d] state machine not found", followerIdx)
	}
	waitForCondition(t, defaultTimeout, "follower replication for stale read", func() bool {
		_, ok := followerSM.Get("multi-stale-key")
		return ok
	})

	// Stale read from the follower.
	result, err := cluster.hosts[followerIdx].StaleRead(ctx, 1, "multi-stale-key")
	if err != nil {
		t.Fatalf("StaleRead from follower failed: %v", err)
	}
	val, ok := result.(string)
	if !ok {
		t.Fatalf("StaleRead returned unexpected type: %T", result)
	}
	if val != "multi-stale-value" {
		t.Errorf("StaleRead returned %q, want %q", val, "multi-stale-value")
	}
}
