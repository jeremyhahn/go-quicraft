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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
)

// TestIntegration_DB_LinearizableRead_ImmediateAfterPropose proves that
// SyncRead returns freshly proposed data without an explicit replication
// wait. A broken implementation that serves stale reads from local state
// without performing the ReadIndex protocol would fail this test because
// the state machine may not yet have applied the entry at the time of
// the read call. The ReadIndex protocol forces the read to observe the
// committed entry by confirming leadership and waiting for the local
// state machine to catch up to the commit index.
//
// Reference: Raft Section 8 (linearizable reads via ReadIndex).
func TestIntegration_DB_LinearizableRead_ImmediateAfterPropose(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Propose a key-value pair through SyncPropose on the leader. SyncPropose
	// returns once the entry is committed (majority replicated), but the local
	// state machine application is asynchronous and may lag behind.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session := dbClient.NewNoOPSession(shardID, nil)
	cmd := encodeKVPut("lin-key", "lin-value")
	_, err := leaderHost.SyncPropose(ctx, session, cmd)
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}

	// IMMEDIATELY call SyncRead on the SAME leader without calling
	// waitForReplication. If the implementation correctly uses the ReadIndex
	// protocol, SyncRead will wait until the local state machine has applied
	// up to the read index before executing the lookup. A broken
	// implementation that reads directly from local state would return empty.
	readCtx, readCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer readCancel()

	result, err := leaderHost.SyncRead(readCtx, shardID, encodeKVGet("lin-key"))
	if err != nil {
		t.Fatalf("SyncRead immediately after propose failed: %v", err)
	}

	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", result)
	}
	if val != "lin-value" {
		t.Fatalf("SyncRead immediately after propose returned %q, want %q; "+
			"this indicates reads bypass the ReadIndex protocol and serve stale local state",
			val, "lin-value")
	}
	t.Logf("SyncRead immediately after propose returned correct value %q, "+
		"confirming ReadIndex protocol forces read to observe committed entry", val)
}

// TestIntegration_DB_LinearizableRead_FollowerWithoutExplicitWait proves that
// linearizable reads from followers go through the ReadIndex consensus
// protocol rather than reading from a potentially stale local cache. After
// proposing on the leader, we immediately read from a follower WITHOUT
// waiting for replication to complete. A correct implementation will either:
//   - (a) return the correct value (the ReadIndex protocol waited for the
//     follower's state machine to catch up), or
//   - (b) return an error (timeout, not-ready) proving it attempted consensus.
//
// It must NOT return an empty string with nil error, which would indicate
// stale local reads bypassing the ReadIndex protocol.
//
// Reference: Raft Section 8.
func TestIntegration_DB_LinearizableRead_FollowerWithoutExplicitWait(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")

	// Find a follower node.
	followerIdx := -1
	for i := range cluster.hosts {
		if !cluster.stopped[i].Load() && i != leaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx < 0 {
		t.Fatal("no follower found in the cluster")
	}
	t.Logf("using follower node[%d] for immediate read", followerIdx)

	// Propose a key-value pair on the leader.
	leaderHost := cluster.hosts[leaderIdx]
	proposeCtx, proposeCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer proposeCancel()

	session := dbClient.NewNoOPSession(shardID, nil)
	cmd := encodeKVPut("follower-lin-key", "follower-lin-value")
	_, err := leaderHost.SyncPropose(proposeCtx, session, cmd)
	if err != nil {
		t.Fatalf("SyncPropose on leader failed: %v", err)
	}

	// IMMEDIATELY read from the follower without waiting for replication.
	followerHost := cluster.hosts[followerIdx]
	readCtx, readCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer readCancel()

	result, err := followerHost.SyncRead(readCtx, shardID, encodeKVGet("follower-lin-key"))

	// A correct implementation must either return the right value (ReadIndex
	// waited for SM catch-up) or return an error (timeout / not-ready).
	// Returning empty string with nil error means the read bypassed consensus.
	if err != nil {
		// An error is acceptable: it proves the follower attempted the
		// ReadIndex protocol rather than serving stale local state.
		t.Logf("SyncRead from follower returned error (acceptable): %v", err)
		t.Logf("this confirms the follower attempted consensus rather than serving stale data")
		return
	}

	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", result)
	}

	if val == "" {
		t.Fatalf("SyncRead from follower returned empty string with nil error; " +
			"this indicates reads bypass the ReadIndex protocol and serve stale local state. " +
			"A correct implementation must either return the committed value or an error " +
			"indicating the read went through consensus")
	}

	if val != "follower-lin-value" {
		t.Fatalf("SyncRead from follower returned unexpected value %q, want %q",
			val, "follower-lin-value")
	}

	t.Logf("SyncRead from follower[%d] returned correct value %q immediately after propose, "+
		"confirming ReadIndex protocol waited for SM catch-up", followerIdx, val)
}

// TestIntegration_DB_LinearizableRead_AfterLeaderFailover_NoManualWarmup
// proves that the implementation commits a no-op entry at the start of a
// new term, as required by Raft Section 8 and the PhD dissertation 6.4.
// Without the no-op, a new leader cannot determine which entries from
// previous terms are committed and therefore cannot safely serve reads.
//
// After stopping the old leader and electing a new one, we issue a SyncRead
// WITHOUT proposing any warmup/no-op entry manually. If the implementation
// correctly commits a no-op at term start, the read succeeds. If it does
// not, the read times out because the leader cannot confirm its commit
// index.
func TestIntegration_DB_LinearizableRead_AfterLeaderFailover_NoManualWarmup(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader at node[%d]", leaderIdx)

	// Warmup entry for the first term so the leader can serve reads.
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")

	// Propose entries and wait for full replication so all nodes have the data.
	proposeAndWait(t, cluster, trackers, shardID, "failover-key", "failover-value")
	waitForReplication(t, cluster, trackers, shardID, "failover-key", "failover-value", defaultTimeout)
	t.Logf("pre-failover entry proposed and replicated")

	// Stop the leader to force a new election.
	oldLeaderIdx := leaderIdx
	cluster.stopNode(oldLeaderIdx)
	t.Logf("stopped leader node[%d]", oldLeaderIdx)

	// Wait for a new leader to be elected.
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	if newLeaderIdx == oldLeaderIdx {
		t.Fatal("new leader must not be the stopped node")
	}
	t.Logf("new leader at node[%d]", newLeaderIdx)

	// DO NOT propose any warmup entry. This is the point of the test: the
	// implementation should commit a no-op automatically at term start.

	// The no-op is appended immediately on becomeLeader but needs a round
	// of replication to commit. Retry SyncRead until the no-op commits
	// and ReadIndex succeeds, up to the failover timeout.
	newLeaderHost := cluster.hosts[newLeaderIdx]
	deadline := time.Now().Add(failoverTimeout)
	for {
		readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
		result, err := newLeaderHost.SyncRead(readCtx, shardID, encodeKVGet("failover-key"))
		readCancel()

		if err == nil {
			val, ok := result.(string)
			if !ok {
				t.Fatalf("SyncRead returned type %T, want string", result)
			}
			if val != "failover-value" {
				t.Fatalf("SyncRead after failover returned %q, want %q", val, "failover-value")
			}
			t.Logf("SyncRead after failover without manual warmup returned correct value %q, "+
				"confirming the implementation commits a no-op at term start (Section 8 / PhD 6.4)", val)
			return
		}

		// "read index not ready" means the no-op hasn't committed yet — retry.
		if strings.Contains(err.Error(), "read index not ready") && time.Now().Before(deadline) {
			retryTimer := time.NewTimer(50 * time.Millisecond)
			<-retryTimer.C
			retryTimer.Stop()
			continue
		}

		t.Fatalf("SyncRead after failover returned unexpected error: %v", err)
	}
}

// TestIntegration_DB_LinearizableRead_ConcurrentWritesAndReads proves that
// linearizable reads are properly ordered relative to concurrent writes.
// Specifically, reads must be monotonic: if a read observes value N for a
// key, all subsequent reads must observe a value >= N. A violation of this
// property (e.g., reading "5" then "3") indicates the reads are not going
// through the ReadIndex protocol and are instead serving stale local state.
//
// Reference: Raft Section 8 (linearizability definition).
func TestIntegration_DB_LinearizableRead_ConcurrentWritesAndReads(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Seed the initial value so reads never see an empty string.
	seedCtx, seedCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer seedCancel()
	session := dbClient.NewNoOPSession(shardID, nil)
	_, err := leaderHost.SyncPropose(seedCtx, session, encodeKVPut("counter", "0"))
	if err != nil {
		t.Fatalf("SyncPropose seed failed: %v", err)
	}

	// Wait for the seed to be readable via SyncRead before starting the
	// concurrent phase. This ensures the test does not fail due to the
	// initial entry not yet being applied.
	waitForCondition(t, defaultTimeout, "seed value readable via SyncRead", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		result, err := leaderHost.SyncRead(ctx, shardID, encodeKVGet("counter"))
		if err != nil {
			return false
		}
		val, ok := result.(string)
		return ok && val == "0"
	})

	const testDuration = 3 * time.Second

	var (
		writeCounter atomic.Int64
		writeErr     atomic.Value
		readErr      atomic.Value
		wg           sync.WaitGroup
		stopCh       = make(chan struct{})
	)

	// Writer goroutine: continuously proposes incrementing values for "counter".
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
			}

			next := writeCounter.Add(1)
			val := strconv.FormatInt(next, 10)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			session := dbClient.NewNoOPSession(shardID, nil)
			_, err := leaderHost.SyncPropose(ctx, session, encodeKVPut("counter", val))
			cancel()

			if err != nil {
				// Check if we were told to stop during the propose.
				select {
				case <-stopCh:
					return
				default:
				}
				writeErr.Store(fmt.Errorf("SyncPropose(%s) failed: %w", val, err))
				return
			}
		}
	}()

	// Reader goroutine: continuously reads "counter" and asserts monotonicity.
	wg.Add(1)
	go func() {
		defer wg.Done()

		var lastSeen int64
		var readCount int64

		for {
			select {
			case <-stopCh:
				t.Logf("reader completed %d reads, last seen value: %d", readCount, lastSeen)
				return
			default:
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			result, err := leaderHost.SyncRead(ctx, shardID, encodeKVGet("counter"))
			cancel()

			if err != nil {
				// Transient errors during concurrent writes are expected.
				// Only record persistent errors.
				select {
				case <-stopCh:
					return
				default:
				}
				continue
			}

			val, ok := result.(string)
			if !ok {
				readErr.Store(fmt.Errorf("SyncRead returned type %T, want string", result))
				return
			}

			if val == "" {
				readErr.Store(fmt.Errorf(
					"SyncRead returned empty string with nil error after %d reads (last seen: %d); "+
						"this indicates reads bypass the ReadIndex protocol",
					readCount, lastSeen))
				return
			}

			num, parseErr := strconv.ParseInt(val, 10, 64)
			if parseErr != nil {
				readErr.Store(fmt.Errorf("failed to parse counter value %q: %w", val, parseErr))
				return
			}

			if num < lastSeen {
				readErr.Store(fmt.Errorf(
					"linearizability violation: read %q (=%d) after previously reading %d; "+
						"reads must be monotonically non-decreasing",
					val, num, lastSeen))
				return
			}

			lastSeen = num
			readCount++
		}
	}()

	// Let the concurrent phase run for the test duration.
	testTimer := time.NewTimer(testDuration)
	<-testTimer.C
	testTimer.Stop()
	close(stopCh)
	wg.Wait()

	// Check for errors from both goroutines.
	if stored := writeErr.Load(); stored != nil {
		t.Fatalf("writer error: %v", stored)
	}
	if stored := readErr.Load(); stored != nil {
		t.Fatalf("reader error: %v", stored)
	}

	// Final assertion: at least some writes and reads completed.
	finalCount := writeCounter.Load()
	if finalCount == 0 {
		t.Fatal("writer goroutine did not complete any proposals")
	}
	t.Logf("concurrent test completed: %d writes proposed", finalCount)
}
