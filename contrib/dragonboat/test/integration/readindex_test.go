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
	"testing"
	"time"
)

// TestIntegration_DB_ReadIndex_ReadLocalNode verifies the dragonboat two-step
// async linearizable read pattern: ReadIndex returns a RequestState that
// resolves when the local state machine catches up to the confirmed read
// index, then ReadLocalNode queries the local state machine.
func TestIntegration_DB_ReadIndex_ReadLocalNode(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose data to read back.
	proposeAndWait(t, cluster, trackers, 1, "readindex-key", "readindex-value")
	waitForReplication(t, cluster, trackers, 1, "readindex-key", "readindex-value", defaultTimeout)

	// Step 1: ReadIndex to get a RequestState.
	leaderHost := cluster.hosts[leaderIdx]
	rs, err := leaderHost.ReadIndex(1, defaultTimeout)
	if err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}
	defer rs.Release()

	// Step 2: Wait for ReadIndex to resolve via ResultC, then query locally.
	result, err := leaderHost.ReadLocalNode(rs, encodeKVGet("readindex-key"))
	if err != nil {
		t.Fatalf("ReadLocalNode failed: %v", err)
	}

	val, ok := result.(string)
	if !ok {
		t.Fatalf("ReadLocalNode returned type %T, want string", result)
	}
	if val != "readindex-value" {
		t.Fatalf("ReadLocalNode = %q, want %q", val, "readindex-value")
	}
	t.Logf("ReadIndex + ReadLocalNode = %q (correct)", val)
}

// TestIntegration_DB_ReadIndex_InvalidShard verifies that ReadIndex on a
// non-existent shard returns an error.
func TestIntegration_DB_ReadIndex_InvalidShard(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	// ReadIndex on shard 99999 which does not exist.
	leaderIdx := cluster.findLeader(1)
	if leaderIdx < 0 {
		t.Fatal("no leader found")
	}
	leaderHost := cluster.hosts[leaderIdx]

	_, err := leaderHost.ReadIndex(99999, 2*time.Second)
	if err == nil {
		t.Fatal("ReadIndex on non-existent shard should return error")
	}
	t.Logf("ReadIndex on invalid shard returned expected error: %v", err)
}
