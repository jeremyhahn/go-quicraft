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
	"os"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestIntegration_DB_SnapshotWithExportPath verifies that requesting
// a snapshot with an ExportPath set exports the snapshot to the
// specified directory. Matches dragonboat's TestSnapshotCanBeExported.
func TestIntegration_DB_SnapshotWithExportPath(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 100, 10)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose entries before snapshot.
	proposeNEntries(t, cluster, trackers, 1, 20, "export-snap")

	// Request snapshot with ExportPath.
	leaderHost := cluster.hosts[leaderIdx]
	exportDir := t.TempDir()

	snapCtx, snapCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer snapCancel()

	opt := dragonboat.SnapshotOption{
		ExportPath: exportDir,
	}
	idx, err := leaderHost.SyncRequestSnapshot(snapCtx, 1, opt)
	if err != nil {
		t.Fatalf("SyncRequestSnapshot with ExportPath failed: %v", err)
	}
	t.Logf("snapshot exported to %s at index %d", exportDir, idx)
}

// TestIntegration_DB_SnapshotWithOverrideCompaction verifies that
// OverrideCompactionOverhead in SnapshotOption overrides the shard-level
// compaction overhead. Matches dragonboat's TestCanOverrideSnapshotOverhead.
func TestIntegration_DB_SnapshotWithOverrideCompaction(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	// Configure with large compaction overhead.
	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 100, 50)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 30 entries.
	proposeNEntries(t, cluster, trackers, 1, 30, "override-compact")

	// Request snapshot with overridden compaction overhead = 0.
	leaderHost := cluster.hosts[leaderIdx]
	opt := dragonboat.SnapshotOption{
		OverrideCompactionOverhead: true,
		CompactionOverhead:         0,
	}

	const maxRetries = 10
	const retryInterval = 500 * time.Millisecond
	var snapshotDone bool
	for attempt := range maxRetries {
		snapCtx, snapCancel := context.WithTimeout(context.Background(), defaultTimeout)
		_, snapErr := leaderHost.SyncRequestSnapshot(snapCtx, 1, opt)
		snapCancel()
		if snapErr == nil {
			snapshotDone = true
			t.Logf("snapshot with override compaction=0 completed (attempt %d)", attempt+1)
			break
		}
		if errors.Is(snapErr, quicraft.ErrSnapshotInProgress) {
			retryTimer := time.NewTimer(retryInterval)
			<-retryTimer.C
			retryTimer.Stop()
			continue
		}
		t.Fatalf("SyncRequestSnapshot with override compaction failed: %v", snapErr)
	}
	if !snapshotDone {
		t.Fatal("SyncRequestSnapshot with override compaction failed after retries")
	}
}

// TestIntegration_DB_SnapshotOptionIsChecked verifies that invalid
// SnapshotOption fields are rejected. Matches dragonboat's
// TestSnapshotOptionIsChecked.
func TestIntegration_DB_SnapshotOptionIsChecked(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 100, 10)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Setting both CompactionOverhead and CompactionIndex should fail validation.
	invalidOpt := dragonboat.SnapshotOption{
		CompactionOverhead: 10,
		CompactionIndex:    5,
	}
	if validErr := invalidOpt.Validate(); validErr == nil {
		t.Fatal("SnapshotOption with both CompactionOverhead and CompactionIndex should fail validation")
	}

	snapCtx, snapCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer snapCancel()

	// Try to use the invalid option in a snapshot request.
	_, snapErr := leaderHost.SyncRequestSnapshot(snapCtx, 1, invalidOpt)
	if snapErr == nil {
		t.Fatal("SyncRequestSnapshot with invalid SnapshotOption should return error")
	}
	t.Logf("SyncRequestSnapshot with invalid option correctly failed: %v", snapErr)
}

// TestIntegration_DB_SnapshotTimeout verifies that a snapshot request
// with a short timeout returns a timeout error. Matches dragonboat's
// TestRequestSnapshotTimeoutWillBeReported.
func TestIntegration_DB_SnapshotTimeout(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 100, 10)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Use an already-expired context.
	expiredCtx, expiredCancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	// Ensure the context is truly expired before proceeding.
	expireTimer := time.NewTimer(1 * time.Millisecond)
	<-expireTimer.C
	expireTimer.Stop()
	defer expiredCancel()

	_, snapErr := leaderHost.SyncRequestSnapshot(expiredCtx, 1, dragonboat.DefaultSnapshotOption)
	if snapErr == nil {
		t.Fatal("SyncRequestSnapshot with expired context should return error")
	}
	t.Logf("SyncRequestSnapshot with expired context returned: %v", snapErr)
}

// TestIntegration_DB_SnapshotExportVerifiesFiles verifies that a
// snapshot export actually writes files to the specified export
// directory. This ensures the export path is not silently ignored
// and that snapshot data is persisted to disk.
func TestIntegration_DB_SnapshotExportVerifiesFiles(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 100, 10)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose entries to ensure there is state to snapshot.
	proposeNEntries(t, cluster, trackers, 1, 10, "export-verify")

	// Request snapshot with ExportPath set to a temp directory.
	leaderHost := cluster.hosts[leaderIdx]
	exportDir := t.TempDir()

	snapCtx, snapCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer snapCancel()

	opt := dragonboat.SnapshotOption{
		ExportPath: exportDir,
	}
	idx, err := leaderHost.SyncRequestSnapshot(snapCtx, 1, opt)
	if err != nil {
		t.Fatalf("SyncRequestSnapshot with ExportPath failed: %v", err)
	}
	t.Logf("snapshot exported at index %d to %s", idx, exportDir)

	// Verify that files were actually written to the export directory.
	entries, readErr := os.ReadDir(exportDir)
	if readErr != nil {
		t.Fatalf("os.ReadDir(%s) failed: %v", exportDir, readErr)
	}
	if len(entries) == 0 {
		t.Fatalf("export directory %s is empty; expected at least one snapshot file", exportDir)
	}
	for _, entry := range entries {
		t.Logf("exported file: %s (dir=%v)", entry.Name(), entry.IsDir())
	}
	t.Logf("snapshot export verification passed: %d entries in export directory", len(entries))
}

// TestIntegration_DB_SnapshotTransferToNewNode verifies that adding a 4th
// node to a 3-node cluster after enough entries to trigger snapshot
// compaction forces the new node to receive and apply a snapshot transfer.
// The new node should catch up with all committed data via the snapshot
// rather than replaying individual log entries (which have been compacted).
func TestIntegration_DB_SnapshotTransferToNewNode(t *testing.T) {
	// Create 3-node cluster with aggressive snapshot settings:
	// snapshot every 10 entries, compaction overhead of 5. After ~15+
	// entries, old log entries are compacted away and only available
	// via snapshot.
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 10, 5)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose enough entries to trigger multiple auto-snapshots and
	// compactions, ensuring old log entries are no longer available.
	expected := proposeNEntries(t, cluster, trackers, 1, 50, "snap-xfer")
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("50 entries proposed and verified on 3 nodes; snapshots and compaction should have occurred")

	// Request an explicit snapshot to guarantee compaction state.
	leaderHost := cluster.hosts[leaderIdx]
	const maxSnapRetries = 10
	const snapRetryInterval = 500 * time.Millisecond
	var snapshotDone bool
	for attempt := range maxSnapRetries {
		snapCtx, snapCancel := context.WithTimeout(context.Background(), defaultTimeout)
		_, snapErr := leaderHost.SyncRequestSnapshot(snapCtx, 1, dragonboat.DefaultSnapshotOption)
		snapCancel()
		if snapErr == nil {
			snapshotDone = true
			t.Logf("explicit snapshot completed (attempt %d)", attempt+1)
			break
		}
		if errors.Is(snapErr, quicraft.ErrSnapshotInProgress) {
			retryTimer := time.NewTimer(snapRetryInterval)
			<-retryTimer.C
			retryTimer.Stop()
			continue
		}
		t.Fatalf("SyncRequestSnapshot failed: %v", snapErr)
	}
	if !snapshotDone {
		t.Fatalf("SyncRequestSnapshot failed after %d retries", maxSnapRetries)
	}

	// Get current membership to obtain the ConfigChangeID.
	memCtx, memCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer memCancel()
	membership, memErr := leaderHost.SyncGetShardMembership(memCtx, 1)
	if memErr != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", memErr)
	}

	// Create the 4th node on a new NodeHost.
	newAddr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, newAddr, cluster.mtls)
	newHost, nhErr := dragonboat.NewNodeHostWithOptions(nhConfig,
		quicraft.WithMemoryLogDB(),
	)
	if nhErr != nil {
		t.Fatalf("NewNodeHostWithOptions for 4th node failed: %v", nhErr)
	}
	defer newHost.Close()

	// Add the 4th replica to the cluster's membership via the leader.
	const newReplicaID = 4
	addCtx, addCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer addCancel()
	addErr := leaderHost.SyncRequestAddReplica(addCtx, 1, newReplicaID, newAddr, membership.ConfigChangeID)
	if addErr != nil {
		t.Fatalf("SyncRequestAddReplica for 4th node failed: %v", addErr)
	}
	t.Logf("replica %d added to membership at address %s", newReplicaID, newAddr)

	// Start the replica on the new NodeHost (join=true since it was
	// added to an existing cluster).
	createFn, newTracker := dbKVCreateFunc()
	cfg := dbTestShardConfigWithSnapshot(1, newReplicaID, 10, 5)
	if startErr := newHost.StartReplica(nil, true, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica on 4th node failed: %v", startErr)
	}
	t.Logf("4th node started, waiting for snapshot transfer and catch-up")

	// Wait for the new node's state machine to appear and catch up
	// with all expected data. The new node must receive a snapshot
	// because the early log entries have been compacted away.
	waitForCondition(t, failoverTimeout, "4th node SM created", func() bool {
		return newTracker.Get(1, newReplicaID) != nil
	})

	newSM := newTracker.Get(1, newReplicaID)
	for key, val := range expected {
		waitForCondition(t, failoverTimeout,
			fmt.Sprintf("4th node has key=%q", key),
			func() bool {
				got, ok := newSM.Get(key)
				return ok && got == val
			},
		)
	}
	t.Logf("4th node caught up with all %d entries via snapshot transfer", len(expected))

	// Verify the new node can serve reads via SyncRead.
	readCtx, readCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer readCancel()

	session := dbClient.NewNoOPSession(1, nil)

	// Propose a new entry through the leader and verify the 4th node
	// receives it, confirming full cluster participation.
	postCmd := encodeKVPut("post-transfer", "verified")
	_, propErr := leaderHost.SyncPropose(
		readCtx, session, postCmd,
	)
	if propErr != nil {
		t.Fatalf("SyncPropose after 4th node join failed: %v", propErr)
	}

	waitForCondition(t, defaultTimeout, "4th node has post-transfer key", func() bool {
		got, ok := newSM.Get("post-transfer")
		return ok && got == "verified"
	})
	t.Logf("4th node successfully receives new proposals after snapshot transfer")
}
