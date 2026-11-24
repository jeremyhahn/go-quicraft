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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
)

// TestIntegration_DB_LinearizableRead_ImmediateAfterPropose verifies SyncRead sees a just-proposed value through ReadIndex (Raft §8).
func TestIntegration_DB_LinearizableRead_ImmediateAfterPropose(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")

	// SyncPropose returns on commit while local apply may lag; read from the replica that accepted the entry.
	session := dbClient.NewNoOPSession(shardID, nil)
	cmd := encodeKVPut("lin-key", "lin-value")
	leaderHost := cluster.proposeToLeader(t, session, cmd, defaultTimeout).Host

	// Read immediately, with no replication wait: ReadIndex must wait for the local apply.
	readCtx, readCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer readCancel()

	//quicraft:direct-read the replica that ACCEPTED the entry is the subject: reading
	// immediately from that same host is what proves ReadIndex waits for
	// the local state machine instead of returning empty
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

// TestIntegration_DB_LinearizableRead_FollowerWithoutExplicitWait verifies a follower read returns the value or an error, never a stale empty value (Raft §8).
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

	// Propose on the current leader; the follower read is the assertion.
	session := dbClient.NewNoOPSession(shardID, nil)
	cmd := encodeKVPut("follower-lin-key", "follower-lin-value")
	cluster.proposeToLeader(t, session, cmd, defaultTimeout)

	// Read from the follower without waiting for replication.
	followerHost := cluster.hosts[followerIdx]
	readCtx, readCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer readCancel()

	//quicraft:direct-read the FOLLOWER is the subject: the assertion is that its ReadIndex
	// waits for state-machine catch-up rather than serving local state
	result, err := followerHost.SyncRead(readCtx, shardID, encodeKVGet("follower-lin-key"))

	// A correct implementation must either return the right value (ReadIndex
	// waited for SM catch-up) or return an error (timeout / not-ready).
	// Returning empty string with nil error means the read bypassed consensus.
	if err != nil {
		// An immediate error is acceptable, but the value must become readable within the timeout.
		t.Logf("SyncRead from follower returned error on the immediate read (acceptable): %v", err)
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("follower node[%d] eventually serves the committed value", followerIdx),
			func() bool {
				retryCtx, retryCancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer retryCancel()
				//quicraft:direct-read the same FOLLOWER is the subject: this retry must stay on the
				// replica whose immediate read was allowed to fail
				retryResult, retryErr := followerHost.SyncRead(retryCtx, shardID, encodeKVGet("follower-lin-key"))
				if retryErr != nil {
					return false
				}
				retryVal, isStr := retryResult.(string)
				return isStr && retryVal == "follower-lin-value"
			},
		)
		t.Logf("follower node[%d] served the committed value on retry, confirming the read "+
			"went through consensus rather than returning stale local state", followerIdx)
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

// TestIntegration_DB_LinearizableRead_AfterLeaderFailover_NoManualWarmup verifies a new leader commits a term-start no-op and serves reads (Raft §8, thesis §6.4).
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

	// Retry on dragonboat.IsTempError in fixed 2s slices until the new leader's no-op commits; the adapter reports it as ErrShardNotReady.
	newLeaderHost := cluster.hosts[newLeaderIdx]
	deadline := time.Now().Add(failoverTimeout)
	for {
		readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
		//quicraft:direct-read the newly elected leader IS the subject, and this
		// loop retries IsTempError on a fixed 2s slice, which driveToLeader does
		// not do -- see the block above this loop
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

		// A temporary error means the no-op has not committed yet — retry.
		if dragonboat.IsTempError(err) && time.Now().Before(deadline) {
			retryTimer := time.NewTimer(50 * time.Millisecond)
			<-retryTimer.C
			retryTimer.Stop()
			continue
		}

		t.Fatalf("SyncRead after failover returned unexpected error: %v", err)
	}
}

// TestIntegration_DB_LinearizableRead_ConcurrentWritesAndReads verifies reads are monotonic under concurrent writes (Raft §8).
func TestIntegration_DB_LinearizableRead_ConcurrentWritesAndReads(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")

	// Seed the initial value so reads never see an empty string.
	session := dbClient.NewNoOPSession(shardID, nil)
	cluster.proposeToLeader(t, session, encodeKVPut("counter", "0"), defaultTimeout)

	// Wait for the seed to be readable before starting the concurrent phase, so
	// the test does not fail on the initial entry not yet being applied.
	// tryReadFromLeader, not readFromLeader: a t.Fatal inside a
	// waitForCondition predicate would report the first not-yet-applied poll as
	// a failure instead of polling again.
	waitForCondition(t, defaultTimeout, "seed value readable", func() bool {
		result, err := cluster.tryReadFromLeader(shardID, encodeKVGet("counter"), 2*time.Second)
		if err != nil {
			return false
		}
		val, ok := result.(string)
		return ok && val == "0"
	})

	// minSuccessfulReads is the fewest successful reads the assertions need.
	const minSuccessfulReads = 50

	// The phase runs at least concurrentPhaseFloor and until it has its reads, failing only past concurrentPhaseBudget.
	const concurrentPhaseFloor = 3 * time.Second
	concurrentPhaseBudget := coldStartElectionRounds * electionRound

	// readRetryInterval throttles retries to one Raft tick, the finest step at which leadership can change.
	const readRetryInterval = raftTick

	var (
		writeCounter atomic.Int64
		readCount    atomic.Int64
		readErrCount atomic.Int64
		// readErrLedCount counts only the errors observed while the shard
		// HAD a leader. See the final assertions for why the distinction is
		// the whole assertion.
		readErrLedCount atomic.Int64
		lastSeenRead    atomic.Int64
		writeErr        atomic.Value
		readErr         atomic.Value
		wg              sync.WaitGroup
		stopCh          = make(chan struct{})
		// phaseDoneCh closes once the reader has its evidence or either goroutine has stored an error.
		phaseDoneCh   = make(chan struct{})
		phaseDoneOnce sync.Once
		phaseDone     = func() { phaseDoneOnce.Do(func() { close(phaseDoneCh) }) }
	)

	// Writer goroutine: continuously proposes incrementing values for "counter".
	wg.Add(1)
	go func() {
		defer wg.Done()
		// A writer that has stopped can produce no further evidence, so the
		// phase should not wait out its budget for evidence that is not coming.
		defer phaseDone()
		for {
			select {
			case <-stopCh:
				return
			default:
			}

			next := writeCounter.Add(1)
			val := strconv.FormatInt(next, 10)

			// tryProposeToLeader, not proposeToLeader: this runs off the test
			// goroutine and must not touch t. It re-resolves the leader on every
			// attempt, so a mid-run election is survived rather than reported as
			// a write failure.
			session := dbClient.NewNoOPSession(shardID, nil)
			_, err := cluster.tryProposeToLeader(session, encodeKVPut("counter", val), 5*time.Second)

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
		defer phaseDone()

		var lastSeen int64

		for {
			select {
			case <-stopCh:
				// The errored count is reported split, because the two halves
				// mean opposite things: an error taken while the shard had a
				// leader is a signal about the engine, and one taken during a
				// leaderless window is a signal about the host's scheduler.
				errs := readErrCount.Load()
				led := readErrLedCount.Load()
				t.Logf("reader completed %d successful reads (%d errored: %d with a leader, "+
					"%d during a leaderless window), last seen value: %d",
					readCount.Load(), errs, led, errs-led, lastSeen)
				return
			default:
			}

			// tryReadFromLeader absorbs not-leader refusals; it must not touch t from this goroutine.
			result, err := cluster.tryReadFromLeader(shardID, encodeKVGet("counter"), 5*time.Second)

			if err != nil {
				// Count errors, classified by whether the shard had a leader; findLeader is safe from this goroutine.
				readErrCount.Add(1)
				if cluster.findLeader(shardID) >= 0 {
					readErrLedCount.Add(1)
				}
				// Back off before retrying. A refusal costs microseconds; the
				// condition behind it clears no faster than a Raft tick.
				retry := time.NewTimer(readRetryInterval)
				select {
				case <-stopCh:
					retry.Stop()
					return
				case <-retry.C:
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
					readCount.Load(), lastSeen))
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
			lastSeenRead.Store(num)
			n := readCount.Add(1)

			// Signal once the evidence the final assertions need is in hand:
			// enough successful reads, and at least one of them past the seed
			// so "reads observed a concurrent write" can be concluded.
			if n >= minSuccessfulReads && lastSeen > 0 {
				phaseDone()
			}
		}
	}()

	// Run the concurrent phase for at least the floor, then on until the reader
	// has what the assertions need or the budget expires.
	phaseStart := time.Now()
	floor := time.NewTimer(concurrentPhaseFloor)
	<-floor.C
	floor.Stop()

	// concurrentPhaseFloor < concurrentPhaseBudget, so this is always positive.
	if remaining := concurrentPhaseBudget - time.Since(phaseStart); remaining > 0 {
		budget := time.NewTimer(remaining)
		select {
		case <-phaseDoneCh:
		case <-budget.C:
		}
		budget.Stop()
	}

	phaseElapsed := time.Since(phaseStart)
	close(stopCh)
	wg.Wait()
	t.Logf("concurrent phase took %v (floor %v, budget %v)",
		phaseElapsed.Round(time.Millisecond), concurrentPhaseFloor, concurrentPhaseBudget)

	// Check for errors from both goroutines.
	if stored := writeErr.Load(); stored != nil {
		t.Fatalf("writer error: %v", stored)
	}
	if stored := readErr.Load(); stored != nil {
		t.Fatalf("reader error: %v", stored)
	}

	// Final assertions. Each one exists because without it the test passes on
	// a run that did no useful work.
	finalCount := writeCounter.Load()
	if finalCount == 0 {
		t.Fatal("writer goroutine did not complete any proposals")
	}

	reads := readCount.Load()
	readFailures := readErrCount.Load()
	ledFailures := readErrLedCount.Load()
	if reads < minSuccessfulReads {
		t.Fatalf("reader completed only %d successful linearizable reads within %v (want at least %d); "+
			"%d reads returned an error, %d of them while the shard had a leader. The monotonicity and "+
			"non-empty checks only run on a read that returned, so a run this thin asserts nothing "+
			"about linearizability",
			reads, phaseElapsed.Round(time.Millisecond), minSuccessfulReads, readFailures, ledFailures)
	}

	// Only errors taken while the shard had a leader count against the failure rate; a leaderless refusal is correct.
	if ledFailures >= reads {
		t.Fatalf("linearizable reads failed more often than they succeeded WHILE THE SHARD HAD A LEADER: "+
			"%d such errors vs %d successes (%d errors total, so %d were taken during a leaderless "+
			"window and are not counted); a leader serving a settled shard must complete the "+
			"overwhelming majority of ReadIndex reads",
			ledFailures, reads, readFailures, readFailures-ledFailures)
	}

	// The reads must have observed a value written during the concurrent phase, not only the seed.
	highest := lastSeenRead.Load()
	if highest <= 0 {
		t.Fatalf("reads never observed a value written during the concurrent phase "+
			"(highest observed counter = %d after %d successful reads, %d writes proposed); "+
			"reads that never advance past the seed are not observing the committed log",
			highest, reads, finalCount)
	}

	// A final linearizable read after the writer has stopped must return the
	// settled counter: at least the highest value already observed, and never
	// a value the writer never proposed.
	finalResult := cluster.readFromLeader(t, shardID, encodeKVGet("counter"), defaultTimeout)
	finalVal, ok := finalResult.(string)
	if !ok {
		t.Fatalf("final SyncRead returned type %T, want string", finalResult)
	}
	finalNum, parseErr := strconv.ParseInt(finalVal, 10, 64)
	if parseErr != nil {
		t.Fatalf("final SyncRead returned unparseable counter %q: %v", finalVal, parseErr)
	}
	if finalNum < highest {
		t.Fatalf("final SyncRead returned %d, which is BEHIND the highest value already read (%d); "+
			"a linearizable read must never go backwards", finalNum, highest)
	}
	if finalNum > finalCount {
		t.Fatalf("final SyncRead returned %d but the writer only ever proposed up to %d; "+
			"the state machine holds a value that was never proposed", finalNum, finalCount)
	}

	t.Logf("concurrent test completed: %d writes proposed, %d successful reads (%d errored), "+
		"highest value read %d, settled value %d",
		finalCount, reads, readFailures, highest, finalNum)
}
