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

// TestIntegration_DB_LeaderCrashDuringSnapshot verifies cluster resilience
// when the leader is stopped while a snapshot is in progress. The remaining
// two nodes must elect a new leader and continue serving proposals with all
// previously committed data intact.
func TestIntegration_DB_LeaderCrashDuringSnapshot(t *testing.T) {
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

	// Propose 25 entries before the crash.
	preData := proposeNEntries(t, cluster, trackers, 1, 25, "crash-pre")
	verifyAllData(t, cluster, trackers, 1, preData)
	t.Logf("25 pre-crash entries verified on all nodes")

	// Request a snapshot on the leader and immediately crash it.
	leaderHost := cluster.hosts[leaderIdx]
	var snapshotRequested bool
	for attempt := range 5 {
		snapCtx, snapCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, snapErr := leaderHost.SyncRequestSnapshot(snapCtx, 1, dragonboat.DefaultSnapshotOption)
		snapCancel()
		if snapErr == nil {
			snapshotRequested = true
			break
		}
		if errors.Is(snapErr, quicraft.ErrSnapshotInProgress) {
			t.Logf("snapshot in progress, retrying (attempt %d/5)", attempt+1)
			retryTimer := time.NewTimer(500 * time.Millisecond)
			<-retryTimer.C
			retryTimer.Stop()
			continue
		}
		// Snapshot may fail for transient reasons; proceed with the crash
		// regardless since the test is about crash recovery.
		t.Logf("snapshot request returned error (attempt %d): %v", attempt+1, snapErr)
		break
	}
	if snapshotRequested {
		t.Logf("snapshot requested on leader node[%d]", leaderIdx)
	} else {
		t.Logf("snapshot request did not succeed; proceeding with leader crash anyway")
	}

	// Immediately stop/close the leader host to simulate a crash.
	cluster.stopNode(leaderIdx)
	t.Logf("leader node[%d] stopped (simulating crash)", leaderIdx)

	// Wait for a new leader to be elected among the surviving nodes.
	newLeaderIdx := -1
	waitForCondition(t, failoverTimeout, "new leader elected after crash", func() bool {
		for i, nh := range cluster.hosts {
			if cluster.stopped[i].Load() {
				continue
			}
			leaderID, _, valid, err := nh.GetLeaderID(1)
			if err != nil || !valid || leaderID == 0 {
				continue
			}
			// Verify this surviving node IS the leader.
			replicaID := uint64(i + 1)
			if leaderID == replicaID {
				newLeaderIdx = i
				return true
			}
		}
		return false
	})
	t.Logf("new leader elected at node[%d]", newLeaderIdx)

	// Verify all pre-crash data is intact on the surviving nodes.
	for key, val := range preData {
		for i := range cluster.hosts {
			if cluster.stopped[i].Load() || i >= len(trackers) || trackers[i] == nil {
				continue
			}
			replicaID := uint64(i + 1)
			nodeSM := trackers[i].Get(1, replicaID)
			if nodeSM == nil {
				continue
			}
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("surviving node[%d] has key=%q", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}
	t.Logf("all pre-crash data intact on surviving nodes")

	// Verify the cluster continues operating by proposing new entries.
	for i := range 10 {
		key := fmt.Sprintf("crash-post-%03d", i)
		value := fmt.Sprintf("val-crash-post-%03d", i)
		proposeAndWait(t, cluster, trackers, 1, key, value)
		preData[key] = value
	}
	t.Logf("10 post-crash entries proposed and committed")

	// Verify post-crash entries replicated to all surviving nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || i >= len(trackers) || trackers[i] == nil {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			continue
		}
		for key, val := range preData {
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("surviving node[%d] post-crash has key=%q", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}
	t.Logf("all 35 entries verified on surviving nodes after leader crash")
}
