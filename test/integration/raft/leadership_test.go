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
	"fmt"
	"testing"
)

// TestIntegration_GetLeaderID_MultiNode_AllAgree verifies that in a 3-node
// cluster, all nodes eventually agree on the same leader ID after election.
func TestIntegration_GetLeaderID_MultiNode_AllAgree(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Determine the expected leader ID from the node that reports itself
	// as leader.
	leaderHost := cluster.hosts[leaderIdx]
	leaderInfo, err := leaderHost.GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo on leader node failed: %v", err)
	}
	expectedLeaderID := leaderInfo.ReplicaID

	// Wait for every node to agree on the leader ID.
	for i, h := range cluster.hosts {
		waitForCondition(t, leaderElectionTimeout,
			fmt.Sprintf("node %d agrees on leader ID %d", i, expectedLeaderID),
			func() bool {
				lid, known, getErr := h.GetLeaderID(1)
				return getErr == nil && known && lid == expectedLeaderID
			},
		)
	}
}
