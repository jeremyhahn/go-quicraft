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
	"fmt"
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestIntegration_Snapshot_MultiNode_ConsistentState creates a 3-node
// cluster, writes entries to the leader, waits for replication, requests
// a snapshot on the leader, and verifies all nodes have the same number
// of entries in their state machines.
func TestIntegration_Snapshot_MultiNode_ConsistentState(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write entries via the leader.
	numEntries := 10
	for i := 0; i < numEntries; i++ {
		cmd := encodeKVPut(fmt.Sprintf("consistent-key-%d", i), fmt.Sprintf("consistent-val-%d", i))
		rs, propErr := leaderHost.Propose(ctx, 1, cmd)
		if propErr != nil {
			t.Fatalf("Propose[%d] to leader failed: %v", i, propErr)
		}
		rs.Release()
	}

	// Wait for replication to all nodes.
	lastKey := fmt.Sprintf("consistent-key-%d", numEntries-1)
	for i := range cluster.hosts {
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] state machine not found", i)
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] all entries replicated", i),
			func() bool {
				_, ok := nodeSM.Get(lastKey)
				return ok
			},
		)
	}

	// Request snapshot on the leader.
	rs, err := leaderHost.RequestSnapshot(ctx, 1, quicraft.SnapshotOption{})
	if err != nil {
		t.Fatalf("RequestSnapshot on leader failed: %v", err)
	}
	if rs == nil {
		t.Fatal("RequestSnapshot returned nil")
	}
	rs.Release()

	// Verify all nodes have the same number of entries.
	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	if leaderSM == nil {
		t.Fatal("leader state machine not found")
	}
	expectedLen := leaderSM.Len()

	for i := range cluster.hosts {
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] state machine not found", i)
		}
		actualLen := nodeSM.Len()
		if actualLen != expectedLen {
			t.Errorf("node[%d] SM has %d entries, leader has %d", i, actualLen, expectedLen)
		}
	}

	// Verify all entries are consistent across nodes.
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("consistent-key-%d", i)
		expectedVal := fmt.Sprintf("consistent-val-%d", i)

		for nodeIdx := range cluster.hosts {
			replicaID := uint64(nodeIdx + 1)
			nodeSM := trackers[nodeIdx].Get(1, replicaID)
			if nodeSM == nil {
				continue
			}
			val, ok := nodeSM.Get(key)
			if !ok {
				t.Errorf("node[%d]: key %q not found", nodeIdx, key)
				continue
			}
			if val != expectedVal {
				t.Errorf("node[%d]: key %q = %q, want %q", nodeIdx, key, val, expectedVal)
			}
		}
	}
}
