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
)

// TestIntegration_DB_MultiShard verifies that multiple independent
// shards can run on the same cluster of NodeHosts. Each shard
// maintains its own state machine, and entries proposed to one shard
// must not appear in another shard.
func TestIntegration_DB_MultiShard(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	// Start shard 1 on all 3 nodes.
	trackers1 := cluster.startShard(1)
	leaderIdx1 := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("shard 1 leader elected at node[%d]", leaderIdx1)

	// Warmup shard 1.
	proposeAndWait(t, cluster, trackers1, 1, "warmup-s1", "ok")
	waitForReplication(t, cluster, trackers1, 1, "warmup-s1", "ok", defaultTimeout)

	// Start shard 2 on all 3 nodes. We need a separate set of trackers
	// because each shard has its own state machines.
	trackers2 := cluster.startShard(2)
	leaderIdx2 := cluster.waitForClusterLeader(2, leaderElectionTimeout)
	t.Logf("shard 2 leader elected at node[%d]", leaderIdx2)

	// Warmup shard 2.
	proposeAndWait(t, cluster, trackers2, 2, "warmup-s2", "ok")
	waitForReplication(t, cluster, trackers2, 2, "warmup-s2", "ok", defaultTimeout)

	// Propose entries to shard 1.
	const numEntries = 5
	shard1Expected := make(map[string]string, numEntries)
	for i := range numEntries {
		key := fmt.Sprintf("s1-key-%03d", i)
		value := fmt.Sprintf("s1-val-%03d", i)
		proposeAndWait(t, cluster, trackers1, 1, key, value)
		shard1Expected[key] = value
	}

	// Propose different entries to shard 2.
	shard2Expected := make(map[string]string, numEntries)
	for i := range numEntries {
		key := fmt.Sprintf("s2-key-%03d", i)
		value := fmt.Sprintf("s2-val-%03d", i)
		proposeAndWait(t, cluster, trackers2, 2, key, value)
		shard2Expected[key] = value
	}

	// Verify shard 1 data replicated to all nodes.
	verifyAllData(t, cluster, trackers1, 1, shard1Expected)
	t.Logf("shard 1: %d entries verified on all nodes", numEntries)

	// Verify shard 2 data replicated to all nodes.
	verifyAllData(t, cluster, trackers2, 2, shard2Expected)
	t.Logf("shard 2: %d entries verified on all nodes", numEntries)

	// Verify independence: shard 1 keys must NOT exist in shard 2, and vice versa.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)

		// Check shard 2 state machines do not contain shard 1 keys.
		if trackers2[i] != nil {
			sm2 := trackers2[i].Get(2, replicaID)
			if sm2 != nil {
				for key := range shard1Expected {
					val, exists := sm2.Get(key)
					if exists && val != "" {
						t.Fatalf("shard 2 node[%d] contains shard 1 key %q=%q; shards are not independent", i, key, val)
					}
				}
			}
		}

		// Check shard 1 state machines do not contain shard 2 keys.
		if trackers1[i] != nil {
			sm1 := trackers1[i].Get(1, replicaID)
			if sm1 != nil {
				for key := range shard2Expected {
					val, exists := sm1.Get(key)
					if exists && val != "" {
						t.Fatalf("shard 1 node[%d] contains shard 2 key %q=%q; shards are not independent", i, key, val)
					}
				}
			}
		}
	}
	t.Logf("shard independence verified: shard 1 and shard 2 data are completely isolated")

	// Cross-verify via SyncRead that each shard only sees its own data.
	for shardID, expected := range map[uint64]map[string]string{1: shard1Expected, 2: shard2Expected} {
		leaderIdx := cluster.findLeader(shardID)
		if leaderIdx < 0 {
			t.Fatalf("no leader for shard %d during cross-verify", shardID)
		}
		leaderHost := cluster.hosts[leaderIdx]
		for key, wantVal := range expected {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			result, readErr := leaderHost.SyncRead(ctx, shardID, encodeKVGet(key))
			cancel()
			if readErr != nil {
				t.Fatalf("SyncRead shard=%d key=%q failed: %v", shardID, key, readErr)
			}
			gotVal, ok := result.(string)
			if !ok {
				t.Fatalf("SyncRead shard=%d key=%q returned type %T, want string", shardID, key, result)
			}
			if gotVal != wantVal {
				t.Fatalf("SyncRead shard=%d key=%q = %q, want %q", shardID, key, gotVal, wantVal)
			}
		}
	}
	t.Logf("SyncRead cross-verification passed for both shards")
}
