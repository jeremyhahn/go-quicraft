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
	"errors"
	"fmt"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestIntegration_DB_Snapshot_RequestAndVerify verifies that the dragonboat
// adapter correctly handles explicit snapshot requests. A 3-node cluster
// proposes entries, requests a snapshot, proposes more entries, and
// verifies all data survives the snapshot boundary.
func TestIntegration_DB_Snapshot_RequestAndVerify(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	// Configure with snapshot support.
	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 100, 10)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 20 entries before snapshot.
	expected := proposeNEntries(t, cluster, trackers, 1, 20, "snap-pre")
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("20 pre-snapshot entries verified")

	// Request an explicit snapshot from the leader via the dragonboat
	// adapter's SyncRequestSnapshot.
	leaderHost := cluster.hosts[leaderIdx]
	const maxSnapRetries = 10
	const snapRetryInterval = 500 * time.Millisecond
	var snapshotIndex uint64
	var snapshotSucceeded bool

	for attempt := range maxSnapRetries {
		snapCtx, snapCancel := context.WithTimeout(context.Background(), defaultTimeout)
		idx, err := leaderHost.SyncRequestSnapshot(snapCtx, 1, dragonboat.DefaultSnapshotOption)
		snapCancel()
		if err == nil {
			snapshotIndex = idx
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
		t.Fatalf("SyncRequestSnapshot from leader failed: %v", err)
	}
	if !snapshotSucceeded {
		t.Fatalf("SyncRequestSnapshot failed after %d retries", maxSnapRetries)
	}

	// Propose 10 more entries after the snapshot.
	for i := range 10 {
		key := fmt.Sprintf("snap-post-%03d", i)
		value := fmt.Sprintf("val-snap-post-%03d", i)
		proposeAndWait(t, cluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Verify all 30 entries are present and correct on all nodes.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all 30 entries verified on 3 nodes after snapshot at index %d", snapshotIndex)
}

// TestIntegration_DB_Snapshot_AutoTrigger verifies that the dragonboat
// adapter correctly handles automatic snapshot triggering. With aggressive
// snapshot settings (SnapshotEntries=15), proposing enough entries
// triggers auto-snapshots, and all data remains consistent.
func TestIntegration_DB_Snapshot_AutoTrigger(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	// Configure aggressive snapshot: every 15 entries with 5 compaction
	// overhead to trigger auto-snapshots during the test.
	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 15, 5)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 50 entries. With SnapshotEntries=15, this triggers at
	// least 3 automatic snapshots.
	expected := proposeNEntries(t, cluster, trackers, 1, 50, "auto-snap")
	t.Logf("50 entries proposed, auto-snapshots should have triggered")

	// Verify all entries on all nodes despite multiple auto-snapshots.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("all 50 entries verified on 3 nodes after auto-snapshots")

	// Compare state machine data across all nodes for consistency.
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
		snapshots = append(snapshots, nodeSM.Snapshot())
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
				t.Fatalf("node[%d] missing key %q", i, key)
			}
			if got != refVal {
				t.Fatalf("node[%d] key %q = %q, want %q", i, key, got, refVal)
			}
		}
	}
	t.Logf("all 3 nodes have identical state (%d keys) after auto-snapshots", len(reference))
}

// TestIntegration_DB_AsyncRequestSnapshot verifies the async RequestSnapshot
// pattern where the caller receives a *RequestState and waits on ResultC
// for the snapshot to complete.
func TestIntegration_DB_AsyncRequestSnapshot(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 100, 10)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose entries to snapshot.
	proposeNEntries(t, cluster, trackers, 1, 20, "async-snap")

	// Request snapshot asynchronously via RequestSnapshot.
	leaderHost := cluster.hosts[leaderIdx]
	const maxRetries = 10
	const retryInterval = 500 * time.Millisecond
	var snapshotCompleted bool

	for attempt := range maxRetries {
		rs, err := leaderHost.RequestSnapshot(1, dragonboat.DefaultSnapshotOption, defaultTimeout)
		if err != nil {
			if errors.Is(err, quicraft.ErrSnapshotInProgress) {
				t.Logf("snapshot in progress, retrying (attempt %d/%d)", attempt+1, maxRetries)
				retryTimer := time.NewTimer(retryInterval)
				<-retryTimer.C
				retryTimer.Stop()
				continue
			}
			t.Fatalf("RequestSnapshot failed: %v", err)
		}

		// Wait on ResultC for the snapshot to complete.
		select {
		case result := <-rs.ResultC():
			if !result.Completed() {
				t.Fatalf("RequestSnapshot did not complete: timeout=%v, rejected=%v",
					result.Timeout(), result.Rejected())
			}
			snapshotIndex := result.SnapshotIndex()
			t.Logf("async snapshot completed at index %d (attempt %d)", snapshotIndex, attempt+1)
			snapshotCompleted = true
		case <-time.After(defaultTimeout):
			t.Fatalf("RequestSnapshot timed out waiting for ResultC")
		}
		rs.Release()
		break
	}

	if !snapshotCompleted {
		t.Fatalf("async snapshot failed after %d retries", maxRetries)
	}
}

// TestIntegration_DB_RequestCompaction verifies that RequestCompaction
// returns a SysOpState that signals completion via CompletedC. The
// adapter wraps quicraft's synchronous compaction in a pre-completed
// SysOpState.
func TestIntegration_DB_RequestCompaction(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 50, 5)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose entries to build up the log.
	proposeNEntries(t, cluster, trackers, 1, 30, "compact")

	// Request a snapshot first (compaction needs a snapshot reference).
	leaderHost := cluster.hosts[leaderIdx]
	snapCtx, snapCancel := context.WithTimeout(context.Background(), defaultTimeout)
	var snapshotDone bool
	for range 10 {
		_, snapErr := leaderHost.SyncRequestSnapshot(snapCtx, 1, dragonboat.DefaultSnapshotOption)
		if snapErr == nil {
			snapshotDone = true
			break
		}
		if errors.Is(snapErr, quicraft.ErrSnapshotInProgress) {
			retryTimer := time.NewTimer(500 * time.Millisecond)
			<-retryTimer.C
			retryTimer.Stop()
			continue
		}
		snapCancel()
		t.Fatalf("SyncRequestSnapshot failed: %v", snapErr)
	}
	snapCancel()
	if !snapshotDone {
		t.Fatal("failed to take snapshot before compaction")
	}

	// Request compaction on the leader's replica.
	leaderReplicaID := uint64(leaderIdx + 1)
	sysOp, err := leaderHost.RequestCompaction(1, leaderReplicaID)
	if err != nil {
		t.Fatalf("RequestCompaction failed: %v", err)
	}

	// Wait for completion via CompletedC.
	select {
	case <-sysOp.CompletedC():
		t.Logf("RequestCompaction completed via CompletedC")
	case <-time.After(defaultTimeout):
		t.Fatal("RequestCompaction timed out waiting for CompletedC")
	}
}
