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

	// Propose a key-value pair through SyncPropose on the leader. SyncPropose
	// returns once the entry is committed (majority replicated), but the local
	// state machine application is asynchronous and may lag behind.
	//
	// The read below must hit the replica that ACCEPTED this entry -- that is the
	// node whose state machine may lag -- so it is taken from the proposal rather
	// than from leaderIdx, which was read before the warmup above.
	session := dbClient.NewNoOPSession(shardID, nil)
	cmd := encodeKVPut("lin-key", "lin-value")
	leaderHost := cluster.proposeToLeader(t, session, cmd, defaultTimeout).Host

	// IMMEDIATELY call SyncRead on the SAME leader without calling
	// waitForReplication. If the implementation correctly uses the ReadIndex
	// protocol, SyncRead will wait until the local state machine has applied
	// up to the read index before executing the lookup. A broken
	// implementation that reads directly from local state would return empty.
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

	// Propose a key-value pair on the leader. The FOLLOWER read below is the
	// assertion, so the proposal only has to reach whichever replica leads now.
	session := dbClient.NewNoOPSession(shardID, nil)
	cmd := encodeKVPut("follower-lin-key", "follower-lin-value")
	cluster.proposeToLeader(t, session, cmd, defaultTimeout)

	// IMMEDIATELY read from the follower without waiting for replication.
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
		// An error on the IMMEDIATE read is acceptable: it proves the follower
		// attempted the ReadIndex protocol rather than serving stale local
		// state. It is not, however, a licence to stop asserting — a follower
		// whose reads ALWAYS fail would otherwise pass this test. Require the
		// value to become readable within the timeout.
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
	//
	// The retry predicate is dragonboat.IsTempError, NOT a match on
	// "read index not ready". That string cannot reach this test: the
	// adapter's error table maps quicraft.ErrReadIndexNotReady onto
	// ErrShardNotReady (errors.go — dragonboat's documented "unknown leader"
	// condition), so through the compatibility API the not-yet-committed
	// no-op arrives as "quicraft: shard not ready". Matching the untranslated
	// string made the retry loop unreachable: the test passed only when the
	// FIRST SyncRead won the race with the no-op's replication round and
	// failed outright otherwise, ~1.5s into a 30s budget it never spent. That
	// is what made this suite load-sensitive.
	//
	// IsTempError keeps the assertion intact. If the no-op genuinely never
	// commits — the defect this test exists to catch — every attempt returns
	// a temporary error until the deadline expires and the test still fails,
	// it just spends its whole budget first instead of failing instantly.
	// This loop is the leader-resolving helper's job done differently ON
	// PURPOSE, and readFromLeader cannot do it.
	//
	// Two things here are not what tryReadFromLeader provides. Its retry
	// predicate is dragonboat.IsTempError, deliberately WIDER than
	// isNotLeaderRejection, because the condition being waited out is the
	// no-op's replication round rather than a leadership change. And each
	// attempt gets a FIXED 2s slice of the failover budget, where driveToLeader
	// hands an attempt everything left on the deadline -- one read that hangs
	// would then swallow the whole budget instead of being retried every 2s,
	// which is the load-sensitivity this loop was rewritten to remove.
	//
	// Re-resolving would also be beside the point: the assertion is that a
	// replica which has JUST become leader serves a read with no manual warmup,
	// so the newly elected leader is the subject.
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

	// minSuccessfulReads is the floor below which this test asserts nothing.
	// The reader's monotonicity and non-empty checks only execute on a read
	// that actually returned, so without a floor a run in which every single
	// SyncRead errored — the reader spinning on `continue` — reached the end of
	// the test green. 50 is a floor a working implementation clears by a wide
	// margin and a broken one cannot.
	const minSuccessfulReads = 50

	// concurrentPhaseFloor is how long the concurrent phase runs REGARDLESS of
	// how quickly the reader gets its 50 reads, and concurrentPhaseBudget is
	// the point past which it gives up waiting for them.
	//
	// Three seconds is not a DEADLINE: running the phase for exactly that long
	// and then asserting that at least 50 reads had landed inside it would be a
	// throughput assertion wearing a correctness assertion's clothes, and on a
	// saturated host it fails without anything being wrong. Measured on 24
	// cores at load average 170-205: the same reader that completes 700-1100
	// reads in three seconds on a quiet box completed 4, 6, 9 and 24 — with
	// ZERO errors. Every read succeeded; there were simply not many of them,
	// because each one took most of a second. Nothing about linearizability
	// was ever in question.
	//
	// So three seconds is the floor and the budget is the deadline. On a
	// quiet machine the phase is three seconds and roughly a thousand reads —
	// the monotonicity check is a sampling check and this
	// keeps its sample size. On a starved machine the phase runs on past three
	// seconds until it has the 50 reads its conclusions rest on, instead of
	// stopping on the clock and reporting the shortfall as a defect.
	//
	// The budget is the assertion's teeth: an
	// implementation that cannot produce 50 good reads and one observed write
	// in this long fails, and says so in terms of the shard rather than of the
	// clock. It is written in election rounds so that it tracks ElectionRTT
	// and RTTMillisecond (see helpers_test.go), and it is the cold-start
	// budget's length because the phase must be able to absorb one complete
	// leaderless window — the shard losing and re-electing a leader mid-phase
	// is normal under saturation — and still gather its evidence afterwards.
	//
	// The floor is shorter than the budget by construction; the wait below
	// relies on it.
	const concurrentPhaseFloor = 3 * time.Second
	concurrentPhaseBudget := coldStartElectionRounds * electionRound

	// readRetryInterval is how long the reader waits after a failed read.
	//
	// A ReadIndex refused because the shard has no leader is refused in
	// microseconds, so an un-throttled reader retries it tens of thousands of
	// times per second, so a count like "4252 errors against 85 successes" is
	// not a 50:1 failure rate, but a tight loop measuring how fast it can spin
	// through one leaderless window. One Raft tick is the
	// finest granularity at which the shard's leadership can actually change,
	// so retrying faster than that only burns a core the host has already run
	// out of and inflates a counter into a number that looks like a defect.
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
		// phaseDoneCh is closed when running the concurrent phase any longer
		// cannot change its outcome. That is either of:
		//
		//   - the reader has gathered what the final assertions need:
		//     minSuccessfulReads successful linearizable reads, at least one
		//     of which observed a value the writer produced during this phase.
		//     Tying the exit condition to the assertions themselves is what
		//     stops the phase ending just short of being able to conclude
		//     anything;
		//   - either goroutine has given up and stored an error. Waiting out
		//     the remaining budget would only delay reporting a linearizability
		//     violation that has already been found.
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

			// tryReadFromLeader, not a pinned SyncRead: polling the replica
			// that accepted the seed would turn every leadership move into a
			// run of counted errors that said nothing about
			// linearizability. It must be the Try form -- this is a
			// goroutine and must not touch t. The counters below stay
			// meaningful: what the helper absorbs is the not-leader refusal,
			// which was never evidence of a defect, and everything that IS
			// still reaches them.
			result, err := cluster.tryReadFromLeader(shardID, encodeKVGet("counter"), 5*time.Second)

			if err != nil {
				// Transient errors during concurrent writes are expected, but
				// they are COUNTED, and classified: an error taken while the
				// shard HAD a leader is the one the final assertion is about.
				// findLeader takes the cluster's read lock and never touches
				// t, so it is safe from this goroutine — tryProposeToLeader,
				// which the writer above calls, resolves through it too.
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

	// The failure-rate assertion counts only the errors taken while the shard
	// HAD a leader, and that restriction is the assertion rather than a
	// weakening of it.
	//
	// What it exists to catch is a leader that refuses reads it should serve:
	// ReadIndex requests dropped under load, ErrReadIndexNotReady mishandled,
	// a settled shard that will not answer. Every one of those shows up as an
	// error taken while a leader is sitting right there, and every one is
	// still caught.
	//
	// What it must not count is a read refused because the
	// shard genuinely had NO leader. Under CPU starvation a three-node shard
	// on this configuration does lose its leader — an election timeout is 500ms
	// of ticks, and a starved tick is late — and for the length of that window
	// the correct answer to a linearizable read is a refusal. handleFollowerReadIndex
	// returns NotLeaderError when r.leaderID is noLeader, quicraft's
	// ErrNotLeader, which this adapter folds into ErrShardNotReady. Counting
	// that against the engine asserts that Raft never holds an election, which
	// is not a property Raft has.
	//
	// Measured: of 6691 errors in one run, 6691 were taken with
	// findLeader() == -1 — no replica anywhere considered itself leader — and 0
	// with a leader present. Counting them all reads a leaderless window as an
	// engine defect.
	if ledFailures >= reads {
		t.Fatalf("linearizable reads failed more often than they succeeded WHILE THE SHARD HAD A LEADER: "+
			"%d such errors vs %d successes (%d errors total, so %d were taken during a leaderless "+
			"window and are not counted); a leader serving a settled shard must complete the "+
			"overwhelming majority of ReadIndex reads",
			ledFailures, reads, readFailures, readFailures-ledFailures)
	}

	// Value assertion, not merely an absence-of-error assertion: the reads must
	// have observed values the writer produced DURING the concurrent phase. The
	// seed is "0", so a highest-observed value of 0 means every read returned
	// the pre-phase state and never saw a concurrent write.
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
