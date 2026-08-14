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

// TestIntegration_Snapshot_MultiNode_ConsistentState creates a 3-node
// cluster, writes entries to the leader, waits for replication, takes a
// snapshot on the leader, and verifies all nodes hold identical state.
//
// The snapshot itself is verified, not merely requested: RequestSnapshot is
// asynchronous, so the result is awaited and required to report a non-zero
// index that covers the applied state, and the durable snapshot.dat artifact
// is required to exist on disk. Without those checks the whole test reduces
// to "the nodes replicated some keys", which plain log replay satisfies
// equally and which no snapshot code path is needed for.
//
// Automatic snapshots cannot interfere: Config.SnapshotEntries has no default
// (SetDefaults never assigns it) and testShardConfig leaves it at 0, so the
// only snapshot in this test is the one requested below.
func TestIntegration_Snapshot_MultiNode_ConsistentState(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	// Write entries via the leader, awaiting apply on each.
	numEntries := 10
	for i := 0; i < numEntries; i++ {
		proposeCommitted(t, leaderHost, 1,
			fmt.Sprintf("consistent-key-%d", i), fmt.Sprintf("consistent-val-%d", i),
			defaultTimeout)
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

	// Take a snapshot on the leader and prove it happened: the result must
	// carry a non-zero index at or above what the leader had applied, and a
	// non-empty snapshot.dat must exist under the leader's NodeHostDir.
	snapshotIndex := awaitSnapshot(t, leaderHost, cluster.nodeHostDirs[leaderIdx],
		1, uint64(leaderIdx+1))

	// The snapshot must cover every entry written above. Each put consumes
	// one log index and the engine consumes more for the initial-membership
	// and no-op entries, so the applied index is always at least numEntries.
	if snapshotIndex < uint64(numEntries) {
		t.Fatalf("snapshot index %d is below the %d entries committed before it",
			snapshotIndex, numEntries)
	}

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
				t.Fatalf("node[%d] state machine not found", nodeIdx)
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
