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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestE2E_Replication_SingleEntry verifies that a single key-value entry
// proposed through the leader is replicated to all 3 nodes.
func TestE2E_Replication_SingleEntry(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("hello", "world")
	rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	result, err := rs.Result()
	if err != nil {
		t.Fatalf("Result failed: %v", err)
	}
	rs.Release()

	if result.Value != uint64(len("world")) {
		t.Errorf("result.Value = %d, want %d", result.Value, len("world"))
	}

	// Verify all nodes replicate the entry.
	waitForReplication(t, cluster, trackers, 1, "hello", "world", defaultTimeout)
}

// TestE2E_Replication_MultipleEntries verifies that 50 entries are
// replicated in order to all nodes.
func TestE2E_Replication_MultipleEntries(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	const numEntries = 50

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	for i := range numEntries {
		key := fmt.Sprintf("key-%03d", i)
		value := fmt.Sprintf("value-%03d", i)
		cmd := encodeKVPut(key, value)

		rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
		if err != nil {
			t.Fatalf("Propose[%d] failed: %v", i, err)
		}
		_, err = rs.Result()
		if err != nil {
			t.Fatalf("Result[%d] failed: %v", i, err)
		}
		rs.Release()
	}

	// Verify all entries on all nodes.
	for i := range numEntries {
		key := fmt.Sprintf("key-%03d", i)
		value := fmt.Sprintf("value-%03d", i)
		waitForReplication(t, cluster, trackers, 1, key, value, defaultTimeout)
	}
}

// TestE2E_Replication_ConcurrentProposals verifies that proposals from
// 5 concurrent goroutines are all replicated across the cluster.
func TestE2E_Replication_ConcurrentProposals(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	const (
		numWorkers       = 5
		entriesPerWorker = 10
	)

	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers*entriesPerWorker)

	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range entriesPerWorker {
				key := fmt.Sprintf("w%d-k%d", workerID, i)
				value := fmt.Sprintf("w%d-v%d", workerID, i)

				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				cmd := encodeKVPut(key, value)
				rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
				if err != nil {
					cancel()
					errCh <- fmt.Errorf("worker %d entry %d propose: %w", workerID, i, err)
					return
				}
				_, err = rs.Result()
				rs.Release()
				cancel()
				if err != nil {
					errCh <- fmt.Errorf("worker %d entry %d result: %w", workerID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent proposal error: %v", err)
	}

	// Verify all entries replicated to all nodes.
	for w := range numWorkers {
		for i := range entriesPerWorker {
			key := fmt.Sprintf("w%d-k%d", w, i)
			value := fmt.Sprintf("w%d-v%d", w, i)
			waitForReplication(t, cluster, trackers, 1, key, value, defaultTimeout)
		}
	}
}

// TestE2E_Replication_StaleReadFromFollower verifies that data written
// through the leader is eventually readable from a follower via StaleRead.
func TestE2E_Replication_StaleReadFromFollower(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Write through the leader.
	proposeAndWait(t, cluster, trackers, 1, "stale-key", "stale-value")

	// Wait for the follower to replicate.
	waitForReplication(t, cluster, trackers, 1, "stale-key", "stale-value", defaultTimeout)

	// Perform a stale read from a follower.
	followerIdx := cluster.findFollower(1)
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	result, err := cluster.hosts[followerIdx].StaleRead(ctx, 1, "stale-key")
	if err != nil {
		t.Fatalf("StaleRead from follower[%d] failed: %v", followerIdx, err)
	}

	val, ok := result.(string)
	if !ok {
		t.Fatalf("expected string result from StaleRead, got %T", result)
	}
	if val != "stale-value" {
		t.Errorf("StaleRead = %q, want %q", val, "stale-value")
	}

	// Also verify reading a non-existent key returns empty.
	result, err = cluster.hosts[followerIdx].StaleRead(ctx, 1, "nonexistent")
	if err != nil {
		t.Fatalf("StaleRead for nonexistent key failed: %v", err)
	}
	val, ok = result.(string)
	if !ok {
		t.Fatalf("expected string result, got %T", result)
	}
	if val != "" {
		t.Errorf("expected empty string for nonexistent key, got %q", val)
	}

	_ = leaderIdx // used implicitly by proposeAndWait
}

// TestE2E_Replication_LargeValue verifies that a 64KB value is correctly
// replicated across all nodes.
func TestE2E_Replication_LargeValue(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Create a 64KB value.
	largeValue := strings.Repeat("A", 64*1024)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("large-key", largeValue)
	rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose large value failed: %v", err)
	}
	_, err = rs.Result()
	if err != nil {
		t.Fatalf("Result for large value failed: %v", err)
	}
	rs.Release()

	// Verify replication on all nodes.
	waitForReplication(t, cluster, trackers, 1, "large-key", largeValue, defaultTimeout)

	// Double-check the value length on each node.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		sm := trackers[i].Get(1, replicaID)
		if sm == nil {
			continue
		}
		val, ok := sm.Get("large-key")
		if !ok {
			t.Fatalf("node[%d] missing large-key", i)
		}
		if len(val) != 64*1024 {
			t.Fatalf("node[%d] large-key length = %d, want %d", i, len(val), 64*1024)
		}
	}
}

// ---------------------------------------------------------------------------
// 5-Node Replication Tests
// ---------------------------------------------------------------------------

// TestE2E_FiveNode_Replication_SingleEntry verifies that a single key-value
// entry proposed through the leader is replicated to all 5 nodes.
func TestE2E_FiveNode_Replication_SingleEntry(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("five-hello", "five-world")
	rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	result, err := rs.Result()
	if err != nil {
		t.Fatalf("Result failed: %v", err)
	}
	rs.Release()

	if result.Value != uint64(len("five-world")) {
		t.Errorf("result.Value = %d, want %d", result.Value, len("five-world"))
	}

	// Verify all 5 nodes replicate the entry.
	waitForReplication(t, cluster, trackers, 1, "five-hello", "five-world", defaultTimeout)

	t.Logf("single entry replicated to all 5 nodes, leader at node[%d]", leaderIdx)
}

// TestE2E_FiveNode_Replication_MultipleEntries verifies that 50 entries
// are replicated in order to all 5 nodes.
func TestE2E_FiveNode_Replication_MultipleEntries(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	const numEntries = 50

	// Use a per-proposal context to avoid a single context timeout
	// covering all 50 sequential proposals under resource contention.
	// Retry on rate-limit errors with backoff for Docker CI load.
	for i := range numEntries {
		key := fmt.Sprintf("f5-key-%03d", i)
		value := fmt.Sprintf("f5-value-%03d", i)
		cmd := encodeKVPut(key, value)

		var proposed bool
		for attempt := range 5 {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
			if err != nil {
				cancel()
				if strings.Contains(err.Error(), "rate limit") || strings.Contains(err.Error(), "system busy") {
					backoffTimer := time.NewTimer(time.Duration(50*(attempt+1)) * time.Millisecond)
					<-backoffTimer.C
					backoffTimer.Stop()
					continue
				}
				t.Fatalf("Propose[%d] failed: %v", i, err)
			}
			_, err = rs.Result()
			rs.Release()
			cancel()
			if err != nil {
				t.Fatalf("Result[%d] failed: %v", i, err)
			}
			proposed = true
			break
		}
		if !proposed {
			t.Fatalf("Propose[%d] failed after 5 retries (rate limited)", i)
		}
	}

	// Wait for the last entry to replicate to all 5 nodes first. Since
	// Raft replication is ordered, once the last entry is present on a
	// node, all earlier entries are guaranteed to be there too. Use an
	// extended timeout for 5-node replication under Docker CI contention.
	lastKey := fmt.Sprintf("f5-key-%03d", numEntries-1)
	lastValue := fmt.Sprintf("f5-value-%03d", numEntries-1)
	waitForReplication(t, cluster, trackers, 1, lastKey, lastValue, defaultTimeout*3)

	// Verify all entries are present on all 5 nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			continue
		}
		for j := range numEntries {
			key := fmt.Sprintf("f5-key-%03d", j)
			expectedVal := fmt.Sprintf("f5-value-%03d", j)
			val, ok := nodeSM.Get(key)
			if !ok {
				t.Errorf("node[%d]: key %q not found", i, key)
				continue
			}
			if val != expectedVal {
				t.Errorf("node[%d]: key %q = %q, want %q", i, key, val, expectedVal)
			}
		}
	}

	t.Logf("50 entries replicated to all 5 nodes")
}

// TestE2E_FiveNode_Replication_ConcurrentProposals verifies that proposals
// from 5 concurrent workers (10 entries each) are all replicated across a
// 5-node cluster.
func TestE2E_FiveNode_Replication_ConcurrentProposals(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	const (
		numWorkers       = 5
		entriesPerWorker = 10
	)

	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers*entriesPerWorker)

	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range entriesPerWorker {
				key := fmt.Sprintf("f5w%d-k%d", workerID, i)
				value := fmt.Sprintf("f5w%d-v%d", workerID, i)

				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				cmd := encodeKVPut(key, value)
				rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
				if err != nil {
					cancel()
					errCh <- fmt.Errorf("worker %d entry %d propose: %w", workerID, i, err)
					return
				}
				_, err = rs.Result()
				rs.Release()
				cancel()
				if err != nil {
					errCh <- fmt.Errorf("worker %d entry %d result: %w", workerID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent proposal error: %v", err)
	}

	// Verify all entries replicated to all 5 nodes.
	for w := range numWorkers {
		for i := range entriesPerWorker {
			key := fmt.Sprintf("f5w%d-k%d", w, i)
			value := fmt.Sprintf("f5w%d-v%d", w, i)
			waitForReplication(t, cluster, trackers, 1, key, value, defaultTimeout)
		}
	}

	t.Logf("5 workers x 10 entries replicated across 5-node cluster, leader at node[%d]", leaderIdx)
}

// TestE2E_FiveNode_Replication_StaleReadFromFollower verifies that data
// written through the leader is eventually readable from a follower via
// StaleRead in a 5-node cluster.
func TestE2E_FiveNode_Replication_StaleReadFromFollower(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Write through the leader.
	proposeAndWait(t, cluster, trackers, 1, "f5-stale-key", "f5-stale-value")

	// Wait for all followers to replicate.
	waitForReplication(t, cluster, trackers, 1, "f5-stale-key", "f5-stale-value", defaultTimeout)

	// Perform a stale read from a follower.
	followerIdx := cluster.findFollower(1)
	if followerIdx < 0 {
		t.Fatal("no follower found in 5-node cluster")
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	result, err := cluster.hosts[followerIdx].StaleRead(ctx, 1, "f5-stale-key")
	if err != nil {
		t.Fatalf("StaleRead from follower[%d] failed: %v", followerIdx, err)
	}

	val, ok := result.(string)
	if !ok {
		t.Fatalf("expected string result from StaleRead, got %T", result)
	}
	if val != "f5-stale-value" {
		t.Errorf("StaleRead = %q, want %q", val, "f5-stale-value")
	}

	// Also verify reading a non-existent key returns empty.
	result, err = cluster.hosts[followerIdx].StaleRead(ctx, 1, "nonexistent-5n")
	if err != nil {
		t.Fatalf("StaleRead for nonexistent key failed: %v", err)
	}
	val, ok = result.(string)
	if !ok {
		t.Fatalf("expected string result, got %T", result)
	}
	if val != "" {
		t.Errorf("expected empty string for nonexistent key, got %q", val)
	}

	t.Logf("StaleRead from follower[%d] succeeded in 5-node cluster", followerIdx)
}

// TestE2E_FiveNode_Replication_LargeValue verifies that a 64KB value is
// correctly replicated across all 5 nodes.
func TestE2E_FiveNode_Replication_LargeValue(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Create a 64KB value.
	largeValue := strings.Repeat("B", 64*1024)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("f5-large-key", largeValue)
	rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose large value failed: %v", err)
	}
	_, err = rs.Result()
	if err != nil {
		t.Fatalf("Result for large value failed: %v", err)
	}
	rs.Release()

	// Verify replication on all 5 nodes.
	waitForReplication(t, cluster, trackers, 1, "f5-large-key", largeValue, defaultTimeout)

	// Double-check the value length on each node.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			continue
		}
		val, ok := nodeSM.Get("f5-large-key")
		if !ok {
			t.Fatalf("node[%d] missing f5-large-key", i)
		}
		if len(val) != 64*1024 {
			t.Fatalf("node[%d] f5-large-key length = %d, want %d", i, len(val), 64*1024)
		}
	}

	t.Logf("64KB value replicated to all 5 nodes")
}
