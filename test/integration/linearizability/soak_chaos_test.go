//go:build integration

package linearizability_test

import (
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Soak knobs. The defaults keep the normal suite fast; a long campaign sets
// QUICRAFT_SOAK_DURATION (e.g. "10m"). The seed is always logged so any
// failure can be replayed exactly by setting QUICRAFT_SOAK_SEED.
const (
	defaultSoakDuration = 30 * time.Second
	soakDurationEnv     = "QUICRAFT_SOAK_DURATION"
	soakSeedEnv         = "QUICRAFT_SOAK_SEED"
)

func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	raw := os.Getenv(soakDurationEnv)
	if raw == "" {
		return defaultSoakDuration
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("%s=%q is not a duration: %v", soakDurationEnv, raw, err)
	}
	return d
}

func soakSeed(t *testing.T) int64 {
	t.Helper()
	raw := os.Getenv(soakSeedEnv)
	if raw == "" {
		// Time-based so repeated runs explore different fault orderings; the
		// value is logged, and replaying it is what makes a failure debuggable.
		return time.Now().UnixNano()
	}
	seed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q is not an integer: %v", soakSeedEnv, raw, err)
	}
	return seed
}

// TestSoak_ChaosUnderSustainedLoad runs a 5-node cluster under continuous
// client load while randomly killing and restarting nodes, then verifies that
// the recorded history is linearizable and that every surviving replica
// converged to the same state.
//
// The existing fault tests each script ONE scenario and run for a few seconds.
// This one is randomized and open-ended, which is what surfaces bugs that need
// an unplanned ORDERING of faults — a restart landing mid-snapshot, an
// election racing a catch-up — rather than any single fault in isolation.
//
// Quorum is deliberately never lost: with 5 voters at most 2 are down at once,
// so the cluster MUST keep committing. That makes loss of progress a failure
// signal in its own right, not just an absence of throughput. Availability is
// asserted, not assumed.
func TestSoak_ChaosUnderSustainedLoad(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 8
		// The fault schedule is randomized, so the share of operations that
		// end INDETERMINATE varies a lot between seeds. Those are what make
		// the porcupine search expensive — the model is nondeterministic, so
		// each one doubles the states its key's partition must explore — and a
		// seed that parks two nodes for a while can pile enough of them onto
		// one key to exhaust the whole check budget. Spreading the same
		// operations over more keys shortens each partition without dropping
		// any of them from the check. See TestSoak_RollingRestartUnderLoad.
		numKeys     = 64
		shardID     = 1
		maxDownNode = 2 // (5-1)/2 — quorum always survives
	)

	duration := soakDuration(t)
	seed := soakSeed(t)
	rng := rand.New(rand.NewSource(seed))
	t.Logf("soak: duration=%v seed=%d (replay with %s=%d)",
		duration, seed, soakSeedEnv, seed)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	// Kill-and-restart under sustained load is the workload most likely to
	// produce two leaders in one term, and this test had no check for it at
	// all: it stops and restarts nodes for the whole run while asserting only
	// throughput and linearizability. The guard samples every host, including
	// ones the harness believes are down, and fails the instant two replicas
	// claim the same term.
	startLeaderTermGuard(t, cluster, shardID)

	recorder := &opRecorder{}
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(t, cluster, shardID, clientID, numKeys, duration, recorder, done)
		}(i)
	}

	// Chaos loop. Runs alongside the clients and stops before they do, so the
	// cluster has a healthy window at the end in which to converge.
	chaosEnd := time.Now().Add(duration - 5*time.Second)
	// Above the ~5s client-deadline tail and a failover, far below anything
	// that would let a genuinely wedged cluster pass.
	maxStall := 3 * failoverTimeout

	var (
		down         = make(map[int]bool)
		faults       int
		progressAt   = recorder.completed()
		stalledSince time.Time
	)

	for time.Now().Before(chaosEnd) {
		// Pace faults so the cluster gets time to elect and catch up between
		// them; too fast and this measures nothing but election storms.
		sleep := time.Duration(1500+rng.Intn(2500)) * time.Millisecond
		select {
		case <-time.After(sleep):
		case <-done:
		}

		// Liveness gate. Quorum is never lost, so a LASTING absence of
		// progress is a real defect (a wedged leader, a stuck apply pipeline).
		// But a bounded gap is normal: Raft cannot commit during an election,
		// and a client whose request was in flight to the dying leader waits
		// out its own 5s deadline before retrying. Measuring elapsed time
		// rather than counting intervals keeps this from firing on a single
		// slow failover, while still catching a cluster that never comes back.
		now := recorder.completed()
		if now == progressAt {
			if stalledSince.IsZero() {
				stalledSince = time.Now()
			} else if stalled := time.Since(stalledSince); stalled > maxStall {
				close(done)
				wg.Wait()
				t.Fatalf("cluster made no progress for %v with quorum intact "+
					"(%d/%d nodes down, %d ops total): that outlasts both a "+
					"failover and a client deadline, so it is availability "+
					"loss rather than slowness", stalled, len(down), numNodes, now)
			}
		} else {
			stalledSince = time.Time{}
		}
		progressAt = now

		// Choose a fault: restart something that is down, or take another node
		// down while quorum still permits it.
		switch {
		case len(down) >= maxDownNode || (len(down) > 0 && rng.Intn(2) == 0):
			var idx int
			for i := range down {
				idx = i
				break
			}
			cluster.restartNode(idx, shardID)
			delete(down, idx)
			faults++
			t.Logf("chaos[%d]: restarted node[%d] (%d down)", faults, idx, len(down))
		default:
			idx := rng.Intn(numNodes)
			if down[idx] {
				continue
			}
			cluster.stopNode(idx)
			down[idx] = true
			faults++
			t.Logf("chaos[%d]: stopped node[%d] (%d down)", faults, idx, len(down))
		}

		// A leader must exist again promptly; otherwise the remaining nodes
		// cannot form quorum, which contradicts the invariant above.
		if !waitForLeaderOrDump(t, cluster, shardID, failoverTimeout, down) {
			close(done)
			wg.Wait()
			t.Fatalf("no leader elected within %v with %d/%d nodes down after "+
				"%d faults: quorum is intact, so this is an election failure",
				failoverTimeout, len(down), numNodes, faults)
		}
	}

	// Heal: bring everything back so the cluster can converge before the check.
	for idx := range down {
		cluster.restartNode(idx, shardID)
		t.Logf("heal: restarted node[%d]", idx)
	}
	cluster.waitForClusterLeader(shardID, failoverTimeout)

	wg.Wait()
	close(done)

	ops := recorder.completed()
	t.Logf("soak complete: %d faults injected, %d operations committed", faults, ops)
	if faults == 0 {
		t.Fatal("no faults were injected; the soak exercised nothing")
	}
	if ops == 0 {
		t.Fatal("no operations completed during the soak")
	}

	// Chaos deliberately removes up to two of five voters at a time, so only
	// a fraction of the nominal rate is expected; the floor still fails a run
	// whose throughput collapsed rather than merely dipped.
	checkLinearizability(t, recorder, floorFor(numClients, duration/4))
}

// TestSoak_RollingRestartUnderLoad restarts every node in sequence, one at a
// time, while clients keep writing — the shape of a production rolling upgrade.
//
// It is separate from the randomized soak because the ordering is the point:
// each node must rejoin and catch up BEFORE the next goes down, so any replica
// that fails to recover from its WAL shows up as a stall or a linearizability
// violation rather than being masked by a surviving majority.
func TestSoak_RollingRestartUnderLoad(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 6
		// A rolling restart kills the leader every round, so a large share of
		// this run's operations end INDETERMINATE (the client's request was in
		// flight to a host that went away), and every indeterminate return is
		// emitted at the end of the history, leaving its operation concurrent
		// with the rest of its key.
		//
		// Spreading the SAME operations over more keys shortens each partition
		// without dropping a single one from the check -- porcupine verifies
		// per-key partitions independently either way. Per-key contention depth
		// is not what this test is for; TestLinearizability_3Node_SingleKeyContention
		// and the chaos soak cover that.
		//
		// Measured: a legal history of 29347 operations over these 64 keys, 90
		// of them indeterminate, decides Ok in 48ms. Width is not what makes a
		// check slow here -- backtracking is -- so if this check ever exhausts
		// its budget, read the per-partition diagnosis before assuming the
		// history was merely too big.
		numKeys = 64
		shardID = 1
	)

	duration := soakDuration(t)
	t.Logf("rolling restart: duration=%v", duration)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	// Every round takes a node down and brings it back while clients write, so
	// this is a first-class opportunity for a restarted replica to claim a term
	// that is already led. Nothing in the test looked for that before.
	startLeaderTermGuard(t, cluster, shardID)

	recorder := &opRecorder{}
	done := make(chan struct{})

	clientsStartedAt := time.Now()
	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(t, cluster, shardID, clientID, numKeys, duration, recorder, done)
		}(i)
	}

	// A stall longer than this is a wedge, not an election. failoverTimeout
	// is the harness's own bound on how long a new leader may take.
	maxStall := 3 * failoverTimeout

	// workloadActive reports whether the clients are still submitting. The
	// catch-up wait below demands that the LEADER make progress, which only
	// means anything while somebody is writing: a round that starts near the
	// end of the run can outlive the clients, and then it is waiting for
	// progress that nothing is going to produce. Both the round loop and
	// waitForCatchUp consult this, so the absolute progress requirement stays
	// honest instead of being traded away for a lag window that a dead cluster
	// also satisfies.
	workloadEnds := clientsStartedAt.Add(duration)
	workloadActive := func() bool { return time.Now().Before(workloadEnds.Add(-2 * time.Second)) }

	deadline := time.Now().Add(duration - 5*time.Second)
	rounds := 0
	var stalledSince time.Time
	for round := 0; time.Now().Before(deadline) && workloadActive(); round++ {
		idx := round % numNodes
		before := recorder.completed()

		roundStart := time.Now()
		cluster.stopNode(idx)
		stoppedAt := time.Now()
		cluster.waitForClusterLeader(shardID, failoverTimeout)
		electedAt := time.Now()
		cluster.restartNode(idx, shardID)
		restartedAt := time.Now()
		leaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
		t.Logf("round %d node[%d]: stop=%v elect=%v restart=%v elect2=%v (leader=%d)",
			round, idx,
			stoppedAt.Sub(roundStart).Round(time.Millisecond),
			electedAt.Sub(stoppedAt).Round(time.Millisecond),
			restartedAt.Sub(electedAt).Round(time.Millisecond),
			time.Since(restartedAt).Round(time.Millisecond), leaderIdx)

		// Wait for the restarted replica to CATCH UP before taking the next
		// one down. A real rolling upgrade does the same, and skipping it
		// runs the cluster at the quorum edge with a lagging replica: with
		// one node down and one still replaying its WAL, every commit needs
		// all three remaining healthy nodes, so the cluster stalls for
		// reasons that have nothing to do with a defect.
		catchStart := time.Now()
		waitForCatchUp(t, cluster, shardID, idx, leaderIdx, failoverTimeout, workloadActive)
		t.Logf("round %d node[%d]: catchup=%v ops=%d", round, idx,
			time.Since(catchStart).Round(time.Millisecond), recorder.completed())
		rounds++

		// Track the longest window with no committed operation. A stall
		// bounded by an election is CORRECT -- Raft cannot commit without a
		// leader, so demanding progress in every single round would assert
		// more than the protocol promises. What must not happen is a stall
		// that outlasts failover: that is a wedged cluster, not an election.
		if after := recorder.completed(); after == before {
			if stalledSince.IsZero() {
				stalledSince = time.Now()
			} else if stalled := time.Since(stalledSince); stalled > maxStall {
				close(done)
				wg.Wait()
				t.Fatalf("no operations committed for %v across %d rolling "+
					"restarts (4 of 5 nodes up, quorum intact, last node[%d]): "+
					"that outlasts leader failover, so the cluster is wedged "+
					"rather than merely electing", stalled, rounds, idx)
			}
		} else {
			stalledSince = time.Time{}
		}
		time.Sleep(500 * time.Millisecond)
	}

	wg.Wait()
	close(done)

	ops := recorder.completed()
	t.Logf("rolling restart complete: %d node restarts, %d operations", rounds, ops)
	if rounds == 0 {
		t.Fatalf("no restart rounds completed in %v; increase %s", duration, soakDurationEnv)
	}
	checkLinearizability(t, recorder, floorFor(numClients, duration/4))
}

// waitForCatchUp blocks until the replica at idx has applied close to what the
// leader has AND the cluster has actually made forward progress, so a rolling
// restart proceeds only once the cluster is genuinely healthy again rather than
// merely having elected someone.
//
// minProgress is the absolute half of the predicate and it is not optional. The
// original condition was purely RELATIVE -- self + maxLag >= leader -- which a
// WEDGED leader satisfies trivially: if the leader stops committing, the
// restarted replica draws level with a stationary target and the harness reads
// a dead cluster as "caught up", then takes the next node down. Requiring the
// leader's applied index to have ADVANCED past where it was when the wait
// started makes a stall fail here instead of being laundered into the next
// round.
//
// workloadActive reports whether the clients are still submitting. The absolute
// requirement is conditional on it for a reason that is not a loophole: with no
// client writing, the leader's applied index is not SUPPOSED to move, and
// demanding that it does turns the end of the run into a guaranteed failure.
// When the workload has ended the predicate tightens instead of relaxing — the
// replica must reach the leader's index EXACTLY, with no lag window at all,
// which an idle healthy cluster satisfies immediately and a lagging one does
// not.
func waitForCatchUp(t *testing.T, cluster *testCluster, shardID uint64, idx, leaderIdx int,
	timeout time.Duration, workloadActive func() bool) {
	t.Helper()
	// A small lag is normal on a live cluster taking writes; require the
	// replica to be within a bounded window of the leader rather than exactly
	// equal, which a moving target would never satisfy.
	const maxLag = 50
	// How far the leader must move while this wait is in progress. Clients are
	// writing continuously and quorum is intact, so a healthy cluster clears
	// this in well under a second; a wedged one never does.
	const minProgress = 5

	start := time.Now()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	diag := time.NewTicker(500 * time.Millisecond)
	defer diag.Stop()

	// Baseline the leader's applied index, so "progress" is measured from the
	// moment the restarted replica rejoined rather than from zero.
	var baseline uint64
	var haveBaseline bool
	var lastSelf, lastLeader uint64

	caught := func() bool {
		cluster.hostsMu.RLock()
		defer cluster.hostsMu.RUnlock()
		if cluster.stopped[idx].Load() || cluster.stopped[leaderIdx].Load() {
			return false
		}
		self, err := cluster.hosts[idx].ShardInfo(shardID)
		if err != nil {
			return false
		}
		leader, err := cluster.hosts[leaderIdx].ShardInfo(shardID)
		if err != nil {
			return false
		}
		if !haveBaseline {
			baseline = leader.LastApplied
			haveBaseline = true
		}
		lastSelf, lastLeader = self.LastApplied, leader.LastApplied
		if !workloadActive() {
			// Nobody is writing any more, so the leader is not going to move.
			// Require exact catch-up rather than progress: strictly stronger
			// than the lag window, and achievable on an idle cluster.
			return self.LastApplied >= leader.LastApplied
		}
		if leader.LastApplied < baseline+minProgress {
			// The cluster has not committed anything since the restart. Being
			// level with a stationary leader is not being caught up.
			return false
		}
		return self.LastApplied+maxLag >= leader.LastApplied
	}

	for {
		if caught() {
			t.Logf("catchup[node=%d leader=%d] applied=%d leader=%d (advanced %d from %d) in %v",
				idx, leaderIdx, lastSelf, lastLeader, lastLeader-baseline, baseline,
				time.Since(start).Round(time.Millisecond))
			return
		}
		select {
		case <-deadline.C:
			t.Logf("catchup[node=%d leader=%d] TIMEOUT after %v; final state:", idx, leaderIdx, timeout)
			dumpClusterState(t, cluster, shardID)
			t.Fatalf("timed out after %v waiting for the restarted replica to catch up: "+
				"node[%d] applied=%d, leader node[%d] applied=%d (baseline %d, needed +%d "+
				"progress and lag <= %d)",
				timeout, idx, lastSelf, leaderIdx, lastLeader, baseline, minProgress, maxLag)
			return
		case <-diag.C:
			// Only start dumping once the wait is clearly abnormal, so a
			// healthy rolling restart stays quiet.
			if time.Since(start) > 2*time.Second {
				t.Logf("catchup[node=%d leader=%d] waiting %v:", idx, leaderIdx, time.Since(start).Round(100*time.Millisecond))
				dumpClusterState(t, cluster, shardID)
			}
		case <-ticker.C:
		}
	}
}

// dumpClusterState logs a one-line-per-node view of every live replica's raft
// and apply state. It is the instrumentation the catch-up assertion needs: the
// assertion only sees two LastApplied numbers, which cannot distinguish "the
// replica is not being sent entries" from "the replica has them and is not
// applying them".
func dumpClusterState(t *testing.T, cluster *testCluster, shardID uint64) {
	t.Helper()
	cluster.hostsMu.RLock()
	defer cluster.hostsMu.RUnlock()
	for i, h := range cluster.hosts {
		if cluster.stopped[i].Load() {
			t.Logf("  node[%d]: STOPPED", i)
			continue
		}
		info, err := h.ShardInfo(shardID)
		if err != nil {
			t.Logf("  node[%d]: ShardInfo: %v", i, err)
			continue
		}
		rs, rerr := h.DiagRaftState(shardID)
		gates, gerr := h.DiagShardGates(shardID)
		pipe, perr := h.DiagPipelineState(shardID)
		remotes, _ := h.DiagRemotes(shardID)
		t.Logf("  node[%d] applied=%d leader=%d term=%d isLeader=%v health=%v | raft%+v err=%v "+
			"gates%+v err=%v pipe%+v err=%v remotes=%s",
			i, info.LastApplied, info.LeaderID, info.Term, info.IsLeader, info.Health,
			rs, rerr, gates, gerr, pipe, perr, remotes.Text)
	}
}

// TestSoak_LeaderCloseDoesNotStallWrites measures the write outage caused by
// gracefully closing the LEADER while clients are writing, and guards against
// that outage regressing into a drain-length one.
//
// Measured at HEAD, recovery is BIMODAL: usually ~550-700ms, but roughly one
// run in three takes ~5s. Host.Close itself is fast (3-25ms), so the tail is
// not the drain — it tracks the client's own 5s request deadline, i.e. a
// caller that had a request in flight to the dying leader waits out its
// deadline before retrying against the new one. Worth understanding further;
// it is an availability tail, not a safety problem, and it is unchanged by
// leadership hand-off (measured both ways).
//
// The bound is therefore set above that observed tail and far below
// ShutdownTimeout: it catches a genuine regression to drain-length stalls
// without failing on the known tail.
func TestSoak_LeaderCloseDoesNotStallWrites(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 6
		numKeys    = 16
		shardID    = 1
		// Above the observed ~5s tail, far below ShutdownTimeout (30s).
		maxOutage = 12 * time.Second
		// How long the clients run against a stable leader before it is
		// closed.
		steadyState = 2 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	// The leader is closed mid-write and a new one takes over: exactly the
	// hand-off during which a stale leader that has not stepped down would
	// overlap the new one in the same term.
	startLeaderTermGuard(t, cluster, shardID)

	recorder := &opRecorder{}
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(t, cluster, shardID, clientID, numKeys, 20*time.Second, recorder, done)
		}(i)
	}

	// Let the cluster reach steady state so the measurement is not dominated
	// by warmup.
	time.Sleep(steadyState)
	if recorder.completed() == 0 {
		close(done)
		wg.Wait()
		t.Fatal("no operations committed before the leader was closed")
	}

	// Close the CURRENT leader. The index found before warmup goes stale if
	// leadership moved during the steady-state phase (a loaded host can miss
	// heartbeats), and closing a follower instead measures nothing (#309).
	//
	// Poll for it rather than looking once: the same loaded host can be
	// mid-election at this instant (a CheckQuorum stepdown, then a split vote
	// or two). That is a timing bound, not a fault, and the test needs a
	// leader to close, not one at an exact instant. The wait ends before the
	// close, so the outage measured below is unchanged.
	leaderWaitStart := time.Now()
	leaderDeadline := leaderWaitStart.Add(failoverTimeout)
	leaderIdx = cluster.findLeader(shardID)
	waitedForLeader := leaderIdx < 0
	for leaderIdx < 0 && time.Now().Before(leaderDeadline) {
		time.Sleep(10 * time.Millisecond)
		leaderIdx = cluster.findLeader(shardID)
	}
	if leaderIdx < 0 {
		close(done)
		wg.Wait()
		t.Fatalf("no leader within %v of the end of the steady-state phase", failoverTimeout)
	}
	if waitedForLeader {
		t.Logf("waited %v for a leader at the end of the steady-state phase",
			time.Since(leaderWaitStart))
	}

	// Close the leader and measure how long writes are unavailable.
	before := recorder.completed()
	closeStart := time.Now()
	cluster.stopNode(leaderIdx)
	closeElapsed := time.Since(closeStart)

	// Split the outage into "how long until a new leader exists" and "how
	// long after that until writes actually land". Those have different
	// causes -- a slow election vs. clients not retrying promptly -- and the
	// aggregate number cannot distinguish them.
	// electedAt is written by the election watcher and read by the log line
	// below while that goroutine may still be running, so it has to be atomic.
	//
	// The watcher polls findLeader itself instead of calling
	// waitForClusterLeader. waitForClusterLeader ends in t.Fatalf, and t.Fatalf
	// on a NON-TEST goroutine only runs runtime.Goexit on that goroutine: the
	// test keeps going, electedAt stays 0, and the failure is reported as the
	// nonsense "new leader after 0s" -- with a real risk of the deferred
	// failure landing after the test has finished. Errorf is the correct
	// primitive off the test goroutine (see helpers_test.go leaderTermGuard).
	var electedAt atomic.Int64
	electionWatcherDone := make(chan struct{})
	go func() {
		defer close(electionWatcherDone)
		es := time.Now()
		deadline := time.Now().Add(failoverTimeout)
		for time.Now().Before(deadline) {
			if cluster.findLeader(shardID) >= 0 {
				electedAt.Store(int64(time.Since(es)))
				return
			}
			select {
			case <-done:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
		t.Errorf("no leader was elected within %v of closing the leader", failoverTimeout)
	}()

	// Require SUSTAINED recovery, not a single completion. An operation that
	// was already in flight when the leader closed will finish moments later
	// and would otherwise be misread as "the cluster is healthy again",
	// hiding an outage entirely. Demand a run of new commits instead.
	const recoveredOps = 50
	outage := time.Duration(0)
	deadline := time.Now().Add(maxOutage + 10*time.Second)
	for time.Now().Before(deadline) {
		if recorder.completed() >= before+recoveredOps {
			outage = time.Since(closeStart)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(done)
	wg.Wait()
	// Join the watcher before reading electedAt so the log line reports a
	// settled value rather than racing the goroutine that fills it in.
	<-electionWatcherDone

	elected := time.Duration(electedAt.Load())
	if elected == 0 {
		t.Errorf("the election watcher never observed a new leader after the old one was closed")
	}

	t.Logf("leader close took %v; new leader after %v; %d sustained ops committed after %v (%d total)",
		closeElapsed, elected, recoveredOps, outage, recorder.completed())

	if outage == 0 {
		t.Fatalf("writes never resumed within %v of closing the leader", maxOutage+5*time.Second)
	}
	if outage > maxOutage {
		t.Errorf("writes were unavailable for %v after gracefully closing the "+
			"leader, exceeding %v: that is a drain-length stall, meaning "+
			"leadership was not handed off before draining", outage, maxOutage)
	}

	// The clients serve for the steady-state phase, then stop once
	// recoveredOps more operations land after the close, so the run is far
	// shorter than their 20s window. The floor covers the steady-state phase
	// only: the operations after the close are a fixed count, and their pace
	// is what the outage check above asserts. Scaling the floor to the 20s
	// window demanded writes the run never gives the clients time to make,
	// so a slow but healthy host failed it (#309).
	checkLinearizability(t, recorder, floorFor(numClients, steadyState))
}

// waitForLeaderOrDump waits for a leader and, on timeout, dumps every live
// replica's view so an election failure can be diagnosed from the log rather
// than guessed at. Returns false instead of calling Fatalf so the caller can
// shut the clients down cleanly first.
func waitForLeaderOrDump(t *testing.T, cluster *testCluster, shardID uint64,
	timeout time.Duration, down map[int]bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cluster.findLeader(shardID) >= 0 {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Logf("ELECTION FAILURE: no leader after %v; per-replica state follows", timeout)
	cluster.hostsMu.RLock()
	defer cluster.hostsMu.RUnlock()
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			t.Logf("  node[%d]: STOPPED", i)
			continue
		}
		info, err := cluster.hosts[i].ShardInfo(shardID)
		if err != nil {
			t.Logf("  node[%d]: ShardInfo error: %v", i, err)
			continue
		}
		t.Logf("  node[%d]: replica=%d term=%d leader=%d isLeader=%v "+
			"lastApplied=%d ccIndex=%d observer=%v witness=%v",
			i, info.ReplicaID, info.Term, info.LeaderID, info.IsLeader,
			info.LastApplied, info.ConfigChangeIndex, info.IsObserver, info.IsWitness)
	}
	return false
}

// Soak debugging: set QUICRAFT_SOAK_DEBUG=1 to surface the library's debug
// logs (election, vote grant/deny, snapshot recovery) for a failing seed.
// Off by default so a normal soak is not drowned in output.
func init() {
	if os.Getenv("QUICRAFT_SOAK_DEBUG") == "" {
		return
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
}
