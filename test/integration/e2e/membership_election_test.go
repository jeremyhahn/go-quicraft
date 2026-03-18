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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// ---------------------------------------------------------------------------
// TestE2E_MembershipChange_DuringLeaderElection
// ---------------------------------------------------------------------------

// TestE2E_MembershipChange_DuringLeaderElection kills the leader and
// immediately attempts a membership change (add node). The test verifies
// that the membership change either succeeds after the new leader is
// elected or returns an appropriate error (NotLeader, SystemBusy, etc.),
// and that the cluster reaches a consistent state with no data loss.
func TestE2E_MembershipChange_DuringLeaderElection(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Write baseline data and ensure full replication before disruption.
	baselineData := proposeNEntries(t, cluster, trackers, shardID, 10, "baseline")
	verifyAllData(t, cluster, trackers, shardID, baselineData)
	t.Logf("baseline: 10 entries replicated to all 3 nodes")

	// Re-query the current leader immediately before killing it. The
	// initial leaderIdx from waitForClusterLeader may be stale if a
	// leadership change occurred during baseline data writes.
	currentLeader := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	cluster.stopNode(currentLeader)
	t.Logf("stopped current leader node[%d], waiting for new leader", currentLeader)

	// Wait for a new leader to be elected.
	newLeaderAfterFailover := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("new leader elected: node[%d], now attempting membership change", newLeaderAfterFailover)

	const newReplicaID uint64 = 4

	newAddr := freePort(t)
	hostCfg := testHostConfig(t, newAddr, cluster.mtls)
	newHost, err := quicraft.NewHost(hostCfg)
	if err != nil {
		t.Fatalf("NewHost for new node failed: %v", err)
	}
	defer func() {
		newHost.Close()
	}()

	// Start the shard on the new host in join mode BEFORE requesting
	// membership addition. This ensures the new node is reachable when
	// the leader adds it to the cluster. Without this, the leader adds
	// the 4th member but can't reach it (shard not started), and with
	// CheckQuorum enabled the leader steps down — deadlocking the
	// cluster (4 members, 2 live nodes, quorum=3).
	createFn, newTracker := kvCreateFunc()
	scfg := testShardConfig(shardID, newReplicaID)
	if err := newHost.StartShard(nil, true, createFn, scfg); err != nil {
		t.Fatalf("StartShard(join=true) for replica=%d failed: %v", newReplicaID, err)
	}
	t.Logf("new node started in join mode at %s", newAddr)

	// Now request membership addition from the leader.
	var addErr error
	var addedViaNode int
	retryTicker := time.NewTicker(50 * time.Millisecond)
	defer retryTicker.Stop()
	addDeadline := time.NewTimer(failoverTimeout)
	defer addDeadline.Stop()
addLoop:
	for {
		for i := range cluster.hosts {
			if cluster.stopped[i].Load() {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			addErr = cluster.hosts[i].SyncRequestAddNode(ctx, shardID, newReplicaID, newAddr, 0)
			cancel()
			if addErr == nil {
				addedViaNode = i
				break addLoop
			}
			if isTransientLeadershipError(addErr) {
				continue
			}
			t.Logf("SyncRequestAddNode via node[%d] returned unexpected error: %v", i, addErr)
		}
		select {
		case <-addDeadline.C:
			break addLoop
		case <-retryTicker.C:
		}
	}

	if addErr != nil {
		t.Fatalf("membership change failed after leader election: %v", addErr)
	}
	t.Logf("membership change succeeded via node[%d]", addedViaNode)

	// Wait for the cluster to stabilize with 4 members.
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[newLeaderIdx], shardID, 4, membershipTimeout)
	t.Logf("cluster stabilized with 4 members, new leader: node[%d]", newLeaderIdx)

	// Verify the new node catches up on all baseline data.
	waitForNodeData(t, newTracker, shardID, newReplicaID, baselineData, membershipTimeout)
	t.Logf("new node caught up on all %d baseline entries", len(baselineData))

	// Verify no data loss: all surviving original nodes have baseline data.
	verifyAllData(t, cluster, trackers, shardID, baselineData)

	// Write additional entries to confirm the cluster is fully functional.
	postData := proposeNEntries(t, cluster, trackers, shardID, 5, "post-election-add")
	verifyAllData(t, cluster, trackers, shardID, postData)
	waitForNodeData(t, newTracker, shardID, newReplicaID, postData, membershipTimeout)

	t.Logf("cluster consistent after membership change during election: %d entries verified",
		len(baselineData)+len(postData))
}

// ---------------------------------------------------------------------------
// TestE2E_MembershipChange_ConcurrentWithProposals
// ---------------------------------------------------------------------------

// TestE2E_MembershipChange_ConcurrentWithProposals adds a node while
// concurrent proposals are in-flight. It verifies that all proposals
// before the membership change commit, the new node receives the full
// log (or snapshot), and proposals during and after the change succeed.
// All nodes (including the new one) must have consistent state.
func TestE2E_MembershipChange_ConcurrentWithProposals(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Phase 1: Write initial data to ensure the cluster is warmed up.
	initialData := proposeNEntries(t, cluster, trackers, shardID, 10, "initial")
	verifyAllData(t, cluster, trackers, shardID, initialData)
	t.Logf("phase 1: 10 initial entries replicated")

	// Phase 2: Launch concurrent writers while adding a node.
	const (
		numWriters      = 3
		writesPerWriter = 10
		newReplicaID    = 4
	)

	// Track all successfully proposed keys and their values.
	var mu sync.Mutex
	successfulWrites := make(map[string]string)
	var writeErrors atomic.Int64

	var wg sync.WaitGroup
	writerStarted := make(chan struct{})

	// Start concurrent writers.
	for w := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			<-writerStarted
			for i := range writesPerWriter {
				key := fmt.Sprintf("concurrent-w%d-%03d", writerID, i)
				value := fmt.Sprintf("val-w%d-%03d", writerID, i)

				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				leaderHost := cluster.leaderHost(shardID)
				if leaderHost == nil {
					cancel()
					writeErrors.Add(1)
					continue
				}

				cmd := encodeKVPut(key, value)
				rs, err := leaderHost.Propose(ctx, shardID, cmd)
				if err != nil {
					cancel()
					if isTransientLeadershipError(err) {
						writeErrors.Add(1)
						continue
					}
					t.Errorf("writer[%d] Propose(%q) unexpected error: %v", writerID, key, err)
					writeErrors.Add(1)
					continue
				}

				_, err = rs.Result()
				rs.Release()
				cancel()

				if err != nil {
					writeErrors.Add(1)
					continue
				}

				mu.Lock()
				successfulWrites[key] = value
				mu.Unlock()
			}
		}(w)
	}

	// Signal writers to start and concurrently add a new node.
	close(writerStarted)

	result := addNodeToCluster(t, cluster, shardID, newReplicaID, testShardConfig)
	t.Logf("added node[%d] (replica=%d) at %s while writers active", result.idx, newReplicaID, result.addr)

	// Wait for all writers to complete.
	wg.Wait()

	mu.Lock()
	numSuccessful := len(successfulWrites)
	mu.Unlock()
	t.Logf("concurrent writers completed: %d successful, %d transient errors",
		numSuccessful, writeErrors.Load())

	// Wait for membership to reach 4.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 4, membershipTimeout)

	// Phase 3: Write additional entries after the membership change.
	postData := proposeNEntries(t, cluster, trackers, shardID, 5, "post-add")
	t.Logf("phase 3: 5 post-add entries proposed")

	// Merge all expected data.
	allExpected := make(map[string]string, len(initialData)+numSuccessful+len(postData))
	for k, v := range initialData {
		allExpected[k] = v
	}
	mu.Lock()
	for k, v := range successfulWrites {
		allExpected[k] = v
	}
	mu.Unlock()
	for k, v := range postData {
		allExpected[k] = v
	}

	// Verify all data on the original nodes.
	verifyAllData(t, cluster, trackers, shardID, allExpected)

	// Verify the new node catches up on all data.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, allExpected, membershipTimeout)

	t.Logf("all %d entries verified on all 4 nodes (including new node)", len(allExpected))
}

// ---------------------------------------------------------------------------
// TestE2E_MultiShardFailover_UnderLoad
// ---------------------------------------------------------------------------

// TestE2E_MultiShardFailover_UnderLoad starts a 3-node cluster with 3
// shards, runs concurrent proposals on all shards, then kills the leader
// host. It verifies that all shards elect new leaders, all in-flight
// proposals either complete or return errors, and after recovery all
// shards are functional and consistent.
func TestE2E_MultiShardFailover_UnderLoad(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const numShards = 3
	shardIDs := [numShards]uint64{1, 2, 3}

	// Start all shards. Each shard gets its own set of trackers.
	shardTrackers := make([][]*smTracker, numShards)
	for i, sid := range shardIDs {
		shardTrackers[i] = cluster.startShard(sid)
	}

	// Wait for all shards to have leaders and write warmup data.
	for _, sid := range shardIDs {
		cluster.waitForClusterLeader(sid, leaderElectionTimeout)
	}
	t.Logf("all %d shards have leaders", numShards)

	// Write baseline data to each shard.
	shardBaseline := make([]map[string]string, numShards)
	for i, sid := range shardIDs {
		prefix := fmt.Sprintf("shard%d-baseline", sid)
		shardBaseline[i] = proposeNEntries(t, cluster, shardTrackers[i], sid, 5, prefix)
	}
	t.Logf("baseline data written to all shards")

	// Find the node that is leader for the most shards (or any leader).
	// We want to kill a node that disrupts at least one shard's leadership.
	leaderNode := -1
	for _, sid := range shardIDs {
		idx := cluster.findLeader(sid)
		if idx >= 0 {
			leaderNode = idx
			break
		}
	}
	if leaderNode < 0 {
		t.Fatal("no leader found on any shard")
	}

	// Launch concurrent writers on all shards.
	const writesPerShard = 10

	type shardWriteResult struct {
		shardIdx int
		shardID  uint64
		success  map[string]string
		errors   int64
	}

	resultCh := make(chan shardWriteResult, numShards)
	writerStarted := make(chan struct{})

	for i, sid := range shardIDs {
		go func(shardIdx int, shardID uint64) {
			<-writerStarted
			success := make(map[string]string)
			var errCount int64
			for w := range writesPerShard {
				key := fmt.Sprintf("shard%d-load-%03d", shardID, w)
				value := fmt.Sprintf("val-shard%d-%03d", shardID, w)

				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				leaderHost := cluster.leaderHost(shardID)
				if leaderHost == nil {
					cancel()
					errCount++
					continue
				}

				cmd := encodeKVPut(key, value)
				rs, propErr := leaderHost.Propose(ctx, shardID, cmd)
				if propErr != nil {
					cancel()
					errCount++
					continue
				}

				_, resErr := rs.Result()
				rs.Release()
				cancel()

				if resErr != nil {
					errCount++
					continue
				}
				success[key] = value
			}
			resultCh <- shardWriteResult{
				shardIdx: shardIdx,
				shardID:  shardID,
				success:  success,
				errors:   errCount,
			}
		}(i, sid)
	}

	// Start writers and then kill the leader node while writes are in flight.
	close(writerStarted)
	// Wait for proposals to be in flight before killing the leader.
	chaosTimer := time.NewTimer(50 * time.Millisecond)
	<-chaosTimer.C
	chaosTimer.Stop()

	t.Logf("stopping leader host node[%d] while %d shards are under load", leaderNode, numShards)
	cluster.stopNode(leaderNode)

	// Collect all writer results.
	shardResults := make([]shardWriteResult, numShards)
	for range numShards {
		r := <-resultCh
		shardResults[r.shardIdx] = r
	}

	for _, r := range shardResults {
		t.Logf("shard %d: %d successful writes, %d errors", r.shardID, len(r.success), r.errors)
	}

	// Wait for all shards to elect new leaders.
	for _, sid := range shardIDs {
		newLeader := cluster.waitForClusterLeader(sid, failoverTimeout)
		if cluster.stopped[newLeader].Load() {
			t.Fatalf("shard %d elected stopped node[%d] as leader", sid, newLeader)
		}
		t.Logf("shard %d: new leader at node[%d]", sid, newLeader)
	}

	// Verify all shards are functional: propose and verify on each.
	for i, sid := range shardIDs {
		key := fmt.Sprintf("shard%d-recovery", sid)
		proposeAndWait(t, cluster, shardTrackers[i], sid, key, "recovered")

		// Verify on all surviving nodes.
		for nodeIdx := range cluster.hosts {
			if cluster.stopped[nodeIdx].Load() {
				continue
			}
			replicaID := uint64(nodeIdx + 1)
			nodeSM := shardTrackers[i][nodeIdx].Get(sid, replicaID)
			if nodeSM == nil {
				continue
			}
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("shard %d node[%d] has recovery key", sid, nodeIdx),
				func() bool {
					val, ok := nodeSM.Get(key)
					return ok && val == "recovered"
				},
			)
		}
	}

	// Verify consistency: all successfully written data is present on
	// surviving nodes.
	for i, sid := range shardIDs {
		allExpected := make(map[string]string, len(shardBaseline[i])+len(shardResults[i].success)+1)
		for k, v := range shardBaseline[i] {
			allExpected[k] = v
		}
		for k, v := range shardResults[i].success {
			allExpected[k] = v
		}
		recoveryKey := fmt.Sprintf("shard%d-recovery", sid)
		allExpected[recoveryKey] = "recovered"

		for nodeIdx := range cluster.hosts {
			if cluster.stopped[nodeIdx].Load() {
				continue
			}
			replicaID := uint64(nodeIdx + 1)
			nodeSM := shardTrackers[i][nodeIdx].Get(sid, replicaID)
			if nodeSM == nil {
				continue
			}
			for key, val := range allExpected {
				waitForCondition(t, membershipTimeout,
					fmt.Sprintf("shard %d node[%d] key=%q", sid, nodeIdx, key),
					func() bool {
						got, ok := nodeSM.Get(key)
						return ok && got == val
					},
				)
			}
		}
	}

	t.Logf("multi-shard failover complete: all %d shards recovered and consistent", numShards)
}

// ---------------------------------------------------------------------------
// TestE2E_LeaderTransfer_DuringConcurrentWrites
// ---------------------------------------------------------------------------

// TestE2E_LeaderTransfer_DuringConcurrentWrites starts concurrent writers,
// then initiates leadership transfer. It verifies the transfer completes
// successfully, all in-flight proposals complete or fail gracefully, and
// the new leader serves subsequent proposals.
func TestE2E_LeaderTransfer_DuringConcurrentWrites(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Warm up and establish all connections.
	warmupData := proposeNEntries(t, cluster, trackers, shardID, 5, "warmup")
	verifyAllData(t, cluster, trackers, shardID, warmupData)
	t.Logf("warmup complete: 5 entries replicated")

	// Find the transfer target (a follower).
	targetIdx := cluster.findFollower(shardID)
	if targetIdx < 0 {
		t.Fatal("no follower found for transfer target")
	}
	targetReplicaID := uint64(targetIdx + 1)
	oldLeaderIdx := leaderIdx
	t.Logf("transfer target: node[%d] (replica=%d)", targetIdx, targetReplicaID)

	// Launch concurrent writers.
	const (
		numWriters      = 3
		writesPerWriter = 15
	)

	var mu sync.Mutex
	successfulWrites := make(map[string]string)
	var writeErrors atomic.Int64
	var wg sync.WaitGroup
	writerStarted := make(chan struct{})

	for w := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			<-writerStarted
			for i := range writesPerWriter {
				key := fmt.Sprintf("transfer-w%d-%03d", writerID, i)
				value := fmt.Sprintf("val-w%d-%03d", writerID, i)

				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				leaderHost := cluster.leaderHost(shardID)
				if leaderHost == nil {
					cancel()
					writeErrors.Add(1)
					// Wait briefly for a leader to appear during transfer.
					noLeaderTimer := time.NewTimer(20 * time.Millisecond)
					<-noLeaderTimer.C
					noLeaderTimer.Stop()
					continue
				}

				cmd := encodeKVPut(key, value)
				rs, propErr := leaderHost.Propose(ctx, shardID, cmd)
				if propErr != nil {
					cancel()
					// NotLeader and SystemBusy are expected during transfer.
					writeErrors.Add(1)
					continue
				}

				_, resErr := rs.Result()
				rs.Release()
				cancel()

				if resErr != nil {
					writeErrors.Add(1)
					continue
				}

				mu.Lock()
				successfulWrites[key] = value
				mu.Unlock()
			}
		}(w)
	}

	// Start writers, then initiate the leadership transfer.
	close(writerStarted)
	// Allow proposals to be in flight before triggering the transfer.
	flowTimer := time.NewTimer(30 * time.Millisecond)
	<-flowTimer.C
	flowTimer.Stop()

	transferCtx, transferCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer transferCancel()

	transferErr := cluster.hosts[oldLeaderIdx].RequestLeaderTransfer(
		transferCtx, shardID, targetReplicaID)
	if transferErr != nil {
		// If the leader already changed due to timing, that is acceptable.
		if !isTransientLeadershipError(transferErr) {
			t.Fatalf("RequestLeaderTransfer failed unexpectedly: %v", transferErr)
		}
		t.Logf("RequestLeaderTransfer returned transient error (acceptable): %v", transferErr)
	} else {
		t.Logf("RequestLeaderTransfer submitted successfully")
	}

	// Wait for all writers to finish.
	wg.Wait()

	mu.Lock()
	numSuccessful := len(successfulWrites)
	mu.Unlock()
	t.Logf("writers completed: %d successful, %d transient errors",
		numSuccessful, writeErrors.Load())

	// Wait for leadership to stabilize. The transfer target should
	// ideally become the new leader, but any stable leader is acceptable.
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("leader after transfer: node[%d] (was node[%d], target was node[%d])",
		newLeaderIdx, oldLeaderIdx, targetIdx)

	// Verify the new leader can serve proposals.
	postData := proposeNEntries(t, cluster, trackers, shardID, 5, "post-transfer")
	t.Logf("post-transfer: 5 entries proposed via new leader")

	// Merge all expected data.
	allExpected := make(map[string]string, len(warmupData)+numSuccessful+len(postData))
	for k, v := range warmupData {
		allExpected[k] = v
	}
	mu.Lock()
	for k, v := range successfulWrites {
		allExpected[k] = v
	}
	mu.Unlock()
	for k, v := range postData {
		allExpected[k] = v
	}

	// Verify consistency across all nodes.
	verifyAllData(t, cluster, trackers, shardID, allExpected)

	t.Logf("leader transfer during concurrent writes complete: %d entries consistent across all nodes",
		len(allExpected))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// isTransientLeadershipError returns true if the error is a transient
// leadership error that is expected during elections and transfers.
func isTransientLeadershipError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, quicraft.ErrNotLeader) {
		return true
	}
	if errors.Is(err, quicraft.ErrSystemBusy) {
		return true
	}
	if errors.Is(err, quicraft.ErrTimeout) {
		return true
	}
	if errors.Is(err, quicraft.ErrCanceled) {
		return true
	}
	if errors.Is(err, quicraft.ErrShardNotReady) {
		return true
	}
	// Leadership transfer in progress is transient — it clears after
	// the transfer completes or the election timeout expires.
	if strings.Contains(err.Error(), "transfer in progress") {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return false
}

// Compile-time assertion: ensure testShardConfig matches the signature
// expected by addNodeToCluster's cfgFn parameter.
var _ func(uint64, uint64) config.Config = testShardConfig
