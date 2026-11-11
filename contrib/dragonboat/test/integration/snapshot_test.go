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
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

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
