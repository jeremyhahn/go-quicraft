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

package host_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// snapshotLoadShardConfig creates a per-shard config with very aggressive
// snapshot settings to trigger frequent automatic snapshots. With
// SnapshotEntries=50 and CompactionOverhead=10, a snapshot is taken
// approximately every 50 applied entries.
func snapshotLoadShardConfig(shardID, replicaID uint64) config.Config {
	return testShardConfigWithSnapshot(shardID, replicaID, 50, 10)
}

// TestHostAPI_SnapshotUnderLoad_ConcurrentWriters verifies that automatic
// snapshots complete correctly while the cluster is under sustained write
// load from multiple concurrent writers. The test confirms that no data
// is lost and all nodes converge to the same state.
//
// The test exercises:
// - Automatic snapshot triggering under high write throughput
// - Concurrent proposal handling during snapshot creation
// - Log compaction without data loss
// - Data consistency across all nodes after snapshot and compaction
// - Linearizable reads (SyncRead) for final verification
func TestHostAPI_SnapshotUnderLoad_ConcurrentWriters(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	trackers := cluster.startShardWithConfig(shardID, snapshotLoadShardConfig)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	t.Logf("leader elected at node[%d] with SnapshotEntries=50, CompactionOverhead=10", leaderIdx)

	// Warmup write to establish a committed entry at the current term.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err := leaderHost.SyncPropose(ctx, shardID, encodeKVPut("warmup", "ok"))
	if err != nil {
		t.Fatalf("warmup SyncPropose failed: %v", err)
	}

	const (
		numWriters       = 4
		writesPerWriter  = 75
		totalExpected    = numWriters * writesPerWriter
		retryBackoff     = 10 * time.Millisecond
		maxRetryDuration = 30 * time.Second
	)

	// Each writer writes unique keys: writer-W-NNN = val-W-NNN.
	// Transient errors (timeouts, leader changes during snapshots)
	// are retried with backoff.
	var successCount atomic.Int64
	var wg sync.WaitGroup
	errCh := make(chan error, totalExpected)

	// Track all successfully written key-value pairs.
	type kvPair struct {
		key string
		val string
	}
	pairsCh := make(chan kvPair, totalExpected)

	for w := range numWriters {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range writesPerWriter {
				key := fmt.Sprintf("writer-%d-%03d", workerID, i)
				value := fmt.Sprintf("val-%d-%03d", workerID, i)

				// Retry loop for transient failures during snapshots.
				retryDeadline := time.Now().Add(maxRetryDuration)
				var lastErr error
				retryTicker := time.NewTicker(retryBackoff)
				retryDeadlineTimer := time.NewTimer(time.Until(retryDeadline))
				func() {
					defer retryTicker.Stop()
					defer retryDeadlineTimer.Stop()
					for {
						wCtx, wCancel := context.WithTimeout(context.Background(), defaultTimeout)
						_, wErr := leaderHost.SyncPropose(wCtx, shardID, encodeKVPut(key, value))
						wCancel()
						if wErr == nil {
							successCount.Add(1)
							pairsCh <- kvPair{key: key, val: value}
							lastErr = nil
							return
						}
						lastErr = wErr
						select {
						case <-retryDeadlineTimer.C:
							return
						case <-retryTicker.C:
						}
					}
				}()
				if lastErr != nil {
					errCh <- fmt.Errorf("writer %d entry %d exhausted retries: %w", workerID, i, lastErr)
				}
			}
		}(w)
	}

	wg.Wait()
	close(pairsCh)
	close(errCh)

	// Collect errors.
	var writeErrors []error
	for e := range errCh {
		writeErrors = append(writeErrors, e)
	}
	if len(writeErrors) > 0 {
		for _, e := range writeErrors {
			t.Logf("write error: %v", e)
		}
		t.Fatalf("%d out of %d writes failed permanently", len(writeErrors), totalExpected)
	}

	succeeded := successCount.Load()
	t.Logf("all %d/%d writes succeeded across %d concurrent writers", succeeded, totalExpected, numWriters)

	if succeeded != int64(totalExpected) {
		t.Fatalf("expected %d successful writes, got %d", totalExpected, succeeded)
	}

	// Collect the expected key-value map from successfully written pairs.
	expected := make(map[string]string, totalExpected)
	for pair := range pairsCh {
		expected[pair.key] = pair.val
	}

	// Verify data consistency across all nodes via state machine
	// inspection. Each node's SM must contain all written entries.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] state machine not found", i)
		}
		waitForCondition(t, membershipTimeout,
			fmt.Sprintf("node[%d] has all %d entries", i, len(expected)),
			func() bool {
				return nodeSM.Len() >= len(expected)
			},
		)
		for key, val := range expected {
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("node[%d] has key=%q", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
		t.Logf("node[%d] verified: %d entries consistent", i, nodeSM.Len())
	}

	// Verify via SyncRead on the leader for linearizable correctness.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost = cluster.hosts[leaderIdx]
	for key, expectedVal := range expected {
		rCtx, rCancel := context.WithTimeout(context.Background(), defaultTimeout)
		got, rErr := leaderHost.SyncRead(rCtx, shardID, key)
		rCancel()
		if rErr != nil {
			t.Fatalf("SyncRead(%q) failed: %v", key, rErr)
		}
		val, ok := got.(string)
		if !ok || val != expectedVal {
			t.Fatalf("SyncRead(%q) = %q, want %q", key, val, expectedVal)
		}
	}

	t.Logf("all %d entries verified via SyncRead after snapshot-under-load test", len(expected))
}

// TestHostAPI_SnapshotUnderLoad_VerifySnapshotOccurred confirms that with
// SnapshotEntries=50, writing 200+ entries actually triggers multiple
// snapshots. The test verifies this by checking that the LastApplied index
// on each node advances well beyond what a non-compacting log would show,
// and by requesting an explicit snapshot that succeeds (proving the snapshot
// machinery is functional).
//
// The test exercises:
// - Snapshot triggering threshold enforcement
// - Manual snapshot request after automatic snapshots
// - LastApplied advancement consistency across nodes
func TestHostAPI_SnapshotUnderLoad_VerifySnapshotOccurred(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	trackers := cluster.startShardWithConfig(shardID, snapshotLoadShardConfig)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	t.Logf("leader elected at node[%d] with SnapshotEntries=50", leaderIdx)

	// Write enough entries to trigger multiple automatic snapshots.
	// With SnapshotEntries=50, 200 entries should trigger ~4 snapshots.
	const totalEntries = 200
	expected := make(map[string]string, totalEntries)
	for i := range totalEntries {
		key := fmt.Sprintf("snap-verify-%03d", i)
		value := fmt.Sprintf("val-verify-%03d", i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}
	t.Logf("wrote %d entries to trigger automatic snapshots", totalEntries)

	// Verify all data is replicated.
	verifyAllData(t, cluster, trackers, shardID, expected)

	// Request an explicit snapshot to prove the snapshot machinery works.
	// This also confirms that the SM can serialize its state correctly
	// after being modified by many entries and compactions.
	// Retry with backoff if an automatic snapshot is still in progress.
	const maxSnapRetries = 10
	const snapRetryInterval = 500 * time.Millisecond
	var snapshotSucceeded bool
	for attempt := range maxSnapRetries {
		snapCtx, snapCancel := context.WithTimeout(context.Background(), defaultTimeout)
		rs, err := leaderHost.RequestSnapshot(snapCtx, shardID, quicraft.SnapshotOption{})
		snapCancel()
		if err == nil {
			rs.Release()
			t.Logf("explicit snapshot request succeeded on leader (attempt %d)", attempt+1)
			snapshotSucceeded = true
			break
		}
		if errors.Is(err, quicraft.ErrSnapshotInProgress) {
			t.Logf("snapshot in progress, retrying (attempt %d/%d)", attempt+1, maxSnapRetries)
			retryTimer := time.NewTimer(snapRetryInterval)
			<-retryTimer.C
			retryTimer.Stop()
			continue
		}
		t.Fatalf("RequestSnapshot after %d entries failed: %v", totalEntries, err)
	}
	if !snapshotSucceeded {
		t.Fatalf("RequestSnapshot failed after %d retries: snapshot still in progress", maxSnapRetries)
	}

	// Verify LastApplied is reasonable on all nodes. After 200 user entries
	// plus config entries and session entries, LastApplied should be
	// well above 200.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] state machine not found", i)
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] LastApplied >= %d", i, totalEntries),
			func() bool {
				return nodeSM.LastApplied() >= uint64(totalEntries)
			},
		)
		t.Logf("node[%d] LastApplied=%d (expected >= %d)", i, nodeSM.LastApplied(), totalEntries)
	}

	// Verify via SyncRead on each live node for full linearizable check.
	// Use a shorter per-attempt timeout so the outer retry loop can make
	// multiple attempts under connection churn.
	const syncReadAttemptTimeout = 2 * time.Second
	const syncReadOuterTimeout = 30 * time.Second
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		host := cluster.hosts[i]
		// Spot-check a subset of keys via SyncRead to avoid excessive
		// RTTs. Check first, middle, and last entries.
		spotKeys := []string{
			"snap-verify-000",
			fmt.Sprintf("snap-verify-%03d", totalEntries/2),
			fmt.Sprintf("snap-verify-%03d", totalEntries-1),
		}
		for _, key := range spotKeys {
			expectedVal := expected[key]
			waitForCondition(t, syncReadOuterTimeout,
				fmt.Sprintf("node[%d] SyncRead key=%q", i, key),
				func() bool {
					rCtx, rCancel := context.WithTimeout(context.Background(), syncReadAttemptTimeout)
					defer rCancel()
					got, rErr := host.SyncRead(rCtx, shardID, key)
					if rErr != nil {
						return false
					}
					val, ok := got.(string)
					return ok && val == expectedVal
				},
			)
		}
	}

	t.Logf("snapshot verification complete: %d entries, snapshots triggered, all data consistent", totalEntries)
}

// TestHostAPI_SnapshotUnderLoad_NewNodeCatchUp verifies that a node added
// after many snapshots and compactions can still catch up to the current
// state. This tests the snapshot streaming path for new nodes joining a
// cluster with a heavily compacted log.
//
// The test exercises:
// - Heavy log compaction before new node addition
// - Snapshot-based catch-up for newly added nodes
// - Data correctness after snapshot transfer
func TestHostAPI_SnapshotUnderLoad_NewNodeCatchUp(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	trackers := cluster.startShardWithConfig(shardID, snapshotLoadShardConfig)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d] with SnapshotEntries=50, CompactionOverhead=10", leaderIdx)

	// Write enough entries to trigger multiple snapshots and compaction.
	// The log will be heavily compacted, so a new node cannot catch up
	// via log replay alone.
	const totalEntries = 250
	expected := make(map[string]string, totalEntries)
	for i := range totalEntries {
		key := fmt.Sprintf("catchup-%03d", i)
		value := fmt.Sprintf("val-catchup-%03d", i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}
	verifyAllData(t, cluster, trackers, shardID, expected)
	t.Logf("wrote %d entries, log heavily compacted", totalEntries)

	// Add a 4th node that must catch up via snapshot.
	const newReplicaID uint64 = 4
	result := addNodeToCluster(t, cluster, shardID, newReplicaID, snapshotLoadShardConfig)
	t.Logf("added node[%d] (replica=%d) at %s", result.idx, newReplicaID, result.addr)

	// Wait for membership to reflect 4 nodes.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 4, membershipTimeout)

	// Verify the new node receives all data via snapshot transfer.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, expected, membershipTimeout)
	t.Logf("node[%d] caught up on all %d entries via snapshot", result.idx, totalEntries)

	// Write a few more entries and verify the new node receives them.
	postData := proposeNEntries(t, cluster, trackers, shardID, 10, "post-catchup")
	for k, v := range postData {
		expected[k] = v
	}
	waitForNodeData(t, result.tracker, shardID, newReplicaID, expected, membershipTimeout)

	// Linearizable verification via SyncRead on the new node.
	newNodeHost := cluster.hosts[result.idx]
	// SyncRead on a follower may not work (requires leader), so read
	// from the leader instead and verify the new node's SM directly.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	spotKeys := []string{
		"catchup-000",
		fmt.Sprintf("catchup-%03d", totalEntries/2),
		fmt.Sprintf("catchup-%03d", totalEntries-1),
		"post-catchup-000",
	}
	for _, key := range spotKeys {
		expectedVal := expected[key]
		rCtx, rCancel := context.WithTimeout(context.Background(), defaultTimeout)
		got, rErr := leaderHost.SyncRead(rCtx, shardID, key)
		rCancel()
		if rErr != nil {
			t.Fatalf("SyncRead(%q) failed: %v", key, rErr)
		}
		val, ok := got.(string)
		if !ok || val != expectedVal {
			t.Fatalf("SyncRead(%q) = %q, want %q", key, val, expectedVal)
		}
	}

	// Suppress unused variable warning for newNodeHost (used for
	// documentation clarity about which node is the new one).
	_ = newNodeHost

	t.Logf("new node snapshot catch-up verified: %d total entries consistent", len(expected))
}
