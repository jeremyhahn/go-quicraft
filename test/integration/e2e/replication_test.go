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
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestE2E_Replication_MultipleEntries verifies that 50 entries are
// replicated in order to all nodes, that each proposal returns the state
// machine's result, and that a 64 KB value replicates intact.
func TestE2E_Replication_MultipleEntries(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	const numEntries = 50

	for i := range numEntries {
		key := fmt.Sprintf("key-%03d", i)
		value := fmt.Sprintf("value-%03d", i)
		result := proposeToLeader(t, cluster, 1, encodeKVPut(key, value), defaultTimeout)
		if result.Value != uint64(len(value)) {
			t.Errorf("entry %d: result.Value = %d, want %d", i, result.Value, len(value))
		}
	}

	// Verify all entries on all nodes.
	for i := range numEntries {
		key := fmt.Sprintf("key-%03d", i)
		value := fmt.Sprintf("value-%03d", i)
		waitForReplication(t, cluster, trackers, 1, key, value, defaultTimeout)
	}

	// A 64 KB value replicates intact: waitForReplication compares the full
	// value on every live node.
	largeValue := strings.Repeat("A", 64*1024)
	proposeToLeader(t, cluster, 1, encodeKVPut("large-key", largeValue), defaultTimeout)
	waitForReplication(t, cluster, trackers, 1, "large-key", largeValue, defaultTimeout)
}

// TestE2E_Replication_ConcurrentProposals verifies that proposals from
// 5 concurrent goroutines are all replicated across the cluster.
func TestE2E_Replication_ConcurrentProposals(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	const (
		numWorkers       = 5
		entriesPerWorker = 10
	)

	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers*entriesPerWorker)

	// Every entry must still commit -- the verification below requires all of
	// them on every node. A leadership change during the run is retried
	// against the NEW leader instead of being reported as a failed proposal:
	// workers that pinned the leader index sampled before they started and
	// failed on ErrNotLeader would let an ordinary re-election fail the test
	// with a message that reads like a lost write.
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range entriesPerWorker {
				key := fmt.Sprintf("w%d-k%d", workerID, i)
				value := fmt.Sprintf("w%d-v%d", workerID, i)
				if err := proposeUntilCommitted(cluster, 1, key, value, membershipTimeout); err != nil {
					errCh <- fmt.Errorf("worker %d entry %d: %w", workerID, i, err)
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
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Write through the leader. proposeAndWait re-resolves the leader itself;
	// the index sampled above is deliberately not carried into the write.
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
}

// ---------------------------------------------------------------------------
// 5-Node Replication Tests
// ---------------------------------------------------------------------------

// TestE2E_FiveNode_Replication_MultipleEntries verifies that 50 entries
// are replicated in order to all 5 nodes.
func TestE2E_FiveNode_Replication_MultipleEntries(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	const numEntries = 50

	// Each proposal gets its own deadline so one context timeout does not cover
	// all 50 sequential proposals under resource contention. tryProposeToLeader
	// re-resolves the leader per attempt and absorbs the not-leader rejection;
	// the rate-limit backoff below stays this test's own, because a busy
	// pipeline is not a leadership problem.
	//
	// The predicate is the typed error rather than a substring of its message,
	// and it covers the commit half of the round trip as well as the
	// submission — the helper reports one error for both, and a rate limiter
	// that rejected at admission cannot be told apart here from one that did
	// not. That is marginally more permissive than failing outright on a busy
	// error raised after submission.
	for i := range numEntries {
		key := fmt.Sprintf("f5-key-%03d", i)
		value := fmt.Sprintf("f5-value-%03d", i)
		cmd := encodeKVPut(key, value)

		var proposed bool
		for attempt := range 5 {
			_, err := tryProposeToLeader(cluster, 1, cmd, defaultTimeout)
			if err != nil {
				if errors.Is(err, quicraft.ErrSystemBusy) || errors.Is(err, quicraft.ErrBusy) {
					backoffTimer := time.NewTimer(time.Duration(50*(attempt+1)) * time.Millisecond)
					<-backoffTimer.C
					backoffTimer.Stop()
					continue
				}
				t.Fatalf("Propose[%d] failed: %v", i, err)
			}
			proposed = true
			break
		}
		if !proposed {
			t.Fatalf("Propose[%d] never committed after 5 attempts under rate limiting", i)
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
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	const (
		numWorkers       = 5
		entriesPerWorker = 10
	)

	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers*entriesPerWorker)

	// See TestE2E_Replication_ConcurrentProposals: every entry must still
	// commit, but a leadership change is retried against the new leader rather
	// than failing the test.
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range entriesPerWorker {
				key := fmt.Sprintf("f5w%d-k%d", workerID, i)
				value := fmt.Sprintf("f5w%d-v%d", workerID, i)
				if err := proposeUntilCommitted(cluster, 1, key, value, membershipTimeout); err != nil {
					errCh <- fmt.Errorf("worker %d entry %d: %w", workerID, i, err)
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

	t.Logf("5 workers x 10 entries replicated across 5-node cluster")
}
