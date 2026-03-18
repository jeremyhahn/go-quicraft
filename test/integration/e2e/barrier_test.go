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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestE2E_Barrier_ProposeAndRead verifies that a 3-node barrier-encrypted
// cluster can propose entries, replicate them, and serve linearizable reads.
// All barriers must remain unsealed throughout the test.
func TestE2E_Barrier_ProposeAndRead(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4) to commit a
	// no-op at the current term before reads can be linearized.
	proposeAndWait(t, cluster.testCluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 10 entries through the barrier-encrypted cluster.
	expected := proposeNEntries(t, cluster.testCluster, trackers, 1, 10, "barrier-rw")

	// Verify all entries replicated to all 3 nodes.
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("all 10 entries replicated to 3 barrier-encrypted nodes")

	// Perform a linearizable SyncRead from the leader for one of the keys.
	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	targetKey := "barrier-rw-005"
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

	// Assert all barriers remain unsealed (encryption active).
	for i, b := range cluster.barriers {
		if b.IsSealed() {
			t.Fatalf("barrier[%d] is sealed; expected unsealed for active encryption", i)
		}
	}
	t.Logf("all 3 barriers remain unsealed")
}

// TestE2E_Barrier_Replication verifies that a 3-node barrier-encrypted
// cluster replicates data identically across all nodes despite each node
// having an independent barrier with a distinct passphrase.
func TestE2E_Barrier_Replication(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster.testCluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 30 entries to exercise replication under encryption.
	expected := proposeNEntries(t, cluster.testCluster, trackers, 1, 30, "barrier-repl")

	// Verify all entries on all 3 nodes.
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("all 30 entries verified on 3 barrier-encrypted nodes")

	// Collect state machine snapshots from all live nodes and compare.
	// Despite independent barriers on each host, the decrypted state
	// machine data must be identical across all replicas.
	snapshots := make([]map[string]string, 0, 3)
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
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

	// Compare every snapshot against the first to ensure consistency.
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
	t.Logf("all 3 barrier-encrypted nodes have identical state machine data (%d keys)", len(reference))
}

// TestE2E_Barrier_SnapshotRestore verifies that automatic and explicit
// snapshots work correctly in a barrier-encrypted cluster. Data written
// before and after snapshots must be fully recoverable.
func TestE2E_Barrier_SnapshotRestore(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	// Configure aggressive snapshot settings: every 15 entries with
	// 5 entries of compaction overhead.
	snapshotCfg := func(shardID, replicaID uint64) config.Config {
		return testShardConfigWithSnapshot(shardID, replicaID, 15, 5)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster.testCluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 30 entries. With SnapshotEntries=15, this triggers at
	// least one automatic snapshot with barrier encryption.
	expected := proposeNEntries(t, cluster.testCluster, trackers, 1, 30, "barrier-snap")
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("30 entries proposed and verified, auto-snapshot should have triggered")

	// Request an explicit snapshot from the leader. This exercises the
	// full snapshot pipeline: save -> encrypt -> persist -> acknowledge.
	// Retry with backoff if an automatic snapshot is still in progress.
	leaderHost := cluster.hosts[leaderIdx]
	const maxSnapRetries = 10
	const snapRetryInterval = 500 * time.Millisecond
	var snapshotIndex uint64
	var snapshotSucceeded bool
	for attempt := range maxSnapRetries {
		snapCtx, snapCancel := context.WithTimeout(context.Background(), defaultTimeout)
		rs, err := leaderHost.RequestSnapshot(snapCtx, 1, quicraft.SnapshotOption{})
		snapCancel()
		if err == nil {
			result, resultErr := rs.Result()
			rs.Release()
			if resultErr != nil {
				t.Fatalf("snapshot Result() failed: %v", resultErr)
			}
			snapshotIndex = result.Value
			snapshotSucceeded = true
			t.Logf("explicit snapshot completed at index %d (attempt %d)", snapshotIndex, attempt+1)
			break
		}
		if errors.Is(err, quicraft.ErrSnapshotInProgress) {
			t.Logf("snapshot in progress, retrying (attempt %d/%d)", attempt+1, maxSnapRetries)
			retryTimer := time.NewTimer(snapRetryInterval)
			<-retryTimer.C
			retryTimer.Stop()
			continue
		}
		t.Fatalf("RequestSnapshot from leader failed: %v", err)
	}
	if !snapshotSucceeded {
		t.Fatalf("RequestSnapshot failed after %d retries: snapshot still in progress", maxSnapRetries)
	}

	// Propose 10 more entries after the snapshot. This ensures that
	// entries after the snapshot point are correctly applied on top
	// of any future snapshot restore.
	for i := range 10 {
		key := fmt.Sprintf("barrier-post-%03d", i)
		value := fmt.Sprintf("val-barrier-post-%03d", i)
		proposeAndWait(t, cluster.testCluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Verify all 40 entries (30 pre-snapshot + 10 post-snapshot)
	// are present and correct on all 3 nodes.
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("all 40 entries verified on 3 barrier-encrypted nodes after snapshot")
}

// TestE2E_Barrier_FiveNode verifies that a 5-node barrier-encrypted cluster
// handles proposals, replication, leader failover, and continued writes.
// This exercises the barrier under a larger quorum (3 of 5).
func TestE2E_Barrier_FiveNode(t *testing.T) {
	cluster := newBarrierTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("5-node barrier cluster leader at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster.testCluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 20 entries through the barrier-encrypted 5-node cluster.
	expected := proposeNEntries(t, cluster.testCluster, trackers, 1, 20, "barrier5n")

	// Verify all 20 entries on all 5 nodes.
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("20 entries verified on all 5 barrier-encrypted nodes")

	// Kill the leader and wait for a new leader to be elected.
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

	// Propose 10 more entries through the new leader.
	for i := range 10 {
		key := fmt.Sprintf("barrier5n-post-%03d", i)
		value := fmt.Sprintf("val-barrier5n-post-%03d", i)
		proposeAndWait(t, cluster.testCluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Verify all 30 entries on the 4 surviving nodes.
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("all 30 entries verified on 4 surviving barrier-encrypted nodes after failover")

	// Assert all barriers on surviving nodes remain unsealed.
	for i, b := range cluster.barriers {
		if cluster.stopped[i].Load() {
			continue
		}
		if b.IsSealed() {
			t.Fatalf("barrier[%d] is sealed on surviving node; expected unsealed", i)
		}
	}
	t.Logf("all surviving barriers remain unsealed after failover")
}
