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

// TestSoak_ChaosUnderSustainedLoad randomly kills and restarts nodes of a 5-node cluster under load (at most 2 down, so it must keep committing), then checks linearizability and convergence.
func TestSoak_ChaosUnderSustainedLoad(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 8
		// Spread operations over more keys so indeterminate operations do not exhaust one partition's check budget (see TestSoak_RollingRestartUnderLoad).
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

	// Fail the instant two replicas, down ones included, claim leadership in one term.
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

		// Liveness gate: fail on a lasting absence of progress, measured in elapsed time so one slow failover does not trip it.
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

// TestSoak_RollingRestartUnderLoad restarts every node in turn under load, each catching up before the next goes down, as in a rolling upgrade.
func TestSoak_RollingRestartUnderLoad(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 6
		// Spread operations over 64 keys to shorten each partition, since every round kills the leader and leaves many indeterminate operations; on a budget timeout read the per-partition diagnosis.
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

	// workloadActive reports whether clients are still submitting; the round loop and waitForCatchUp consult it.
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

		// Wait for the restarted replica to catch up before taking the next one down.
		catchStart := time.Now()
		waitForCatchUp(t, cluster, shardID, idx, leaderIdx, failoverTimeout, workloadActive)
		t.Logf("round %d node[%d]: catchup=%v ops=%d", round, idx,
			time.Since(catchStart).Round(time.Millisecond), recorder.completed())
		rounds++

		// Track the longest window without a commit; a stall longer than failover is a wedged cluster.
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
		// The pause between restart rounds is part of the soak's schedule,
		// not a wait for an effect: the catch-up wait above already ended.
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

// waitForCatchUp blocks until the replica at idx is within maxLag of the leader and the leader has advanced by minProgress; once workloadActive is false it requires the leader's exact index instead.
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

// TestSoak_LeaderCloseDoesNotStallWrites bounds the write outage from gracefully closing the leader: above the observed ~5s client-deadline tail, far below ShutdownTimeout.
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
	// stopClients ends the workload once. It is deferred so that a wait
	// below that fails with t.Fatal still stops the clients before the
	// cluster closes under them.
	stopClients := sync.OnceFunc(func() {
		close(done)
		wg.Wait()
	})
	defer stopClients()

	// Let the cluster reach steady state so the measurement is not dominated
	// by warmup. The interval is the phase itself (the floor below is sized
	// to it), not a wait for an effect.
	time.Sleep(steadyState)
	if recorder.completed() == 0 {
		stopClients()
		t.Fatal("no operations committed before the leader was closed")
	}

	// Close the current leader, polling for one since a loaded host may be mid-election.
	leaderIdx = cluster.findLeader(shardID)
	if leaderIdx < 0 {
		leaderWaitStart := time.Now()
		waitForCondition(t, failoverTimeout, "a leader at the end of the steady-state phase", func() bool {
			leaderIdx = cluster.findLeader(shardID)
			return leaderIdx >= 0
		})
		t.Logf("waited %v for a leader at the end of the steady-state phase",
			time.Since(leaderWaitStart))
	}

	// Close the leader and measure how long writes are unavailable.
	before := recorder.completed()
	closeStart := time.Now()
	cluster.stopNode(leaderIdx)
	closeElapsed := time.Since(closeStart)

	// Split the outage into election time and time to the first writes; electedAt is atomic, and the watcher polls findLeader and reports with Errorf off the test goroutine.
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
	// On a t.Fatal below, stop the clients (which closes done and so ends the
	// watcher) and join the watcher before the test returns.
	defer func() {
		stopClients()
		<-electionWatcherDone
	}()

	// Require a run of new commits, not one completion of an already in-flight operation.
	const recoveredOps = 50
	waitForCondition(t, maxOutage+10*time.Second,
		"a sustained run of commits after closing the leader", func() bool {
			return recorder.completed() >= before+recoveredOps
		})
	outage := time.Since(closeStart)

	stopClients()
	// Join the watcher before reading electedAt so the log line reports a
	// settled value rather than racing the goroutine that fills it in.
	<-electionWatcherDone

	elected := time.Duration(electedAt.Load())
	if elected == 0 {
		t.Errorf("the election watcher never observed a new leader after the old one was closed")
	}

	t.Logf("leader close took %v; new leader after %v; %d sustained ops committed after %v (%d total)",
		closeElapsed, elected, recoveredOps, outage, recorder.completed())

	if outage > maxOutage {
		t.Errorf("writes were unavailable for %v after gracefully closing the "+
			"leader, exceeding %v: that is a drain-length stall, meaning "+
			"leadership was not handed off before draining", outage, maxOutage)
	}

	// The floor covers the steady-state phase only; post-close operations are a fixed count checked by the outage bound.
	checkLinearizability(t, recorder, floorFor(numClients, steadyState))
}

// waitForLeaderOrDump waits for a leader and, on timeout, dumps every live
// replica's view so an election failure can be diagnosed from the log rather
// than guessed at. Returns false instead of calling Fatalf so the caller can
// shut the clients down cleanly first.
func waitForLeaderOrDump(t *testing.T, cluster *testCluster, shardID uint64,
	timeout time.Duration, down map[int]bool) bool {
	t.Helper()
	// A bounded poll that reports instead of failing: the caller stops its
	// workload before it fails the test, which waitForCondition cannot do.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	expired := time.After(timeout)
	for polling := true; polling; {
		if cluster.findLeader(shardID) >= 0 {
			return true
		}
		select {
		case <-expired:
			polling = false
		case <-ticker.C:
		}
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
