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
		numNodes    = 5
		numClients  = 8
		numKeys     = 32
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

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, duration, recorder, &totalOps, done)
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
		progressAt   = totalOps.Load()
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
		now := totalOps.Load()
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

	ops := totalOps.Load()
	t.Logf("soak complete: %d faults injected, %d operations committed", faults, ops)
	if faults == 0 {
		t.Fatal("no faults were injected; the soak exercised nothing")
	}
	if ops == 0 {
		t.Fatal("no operations completed during the soak")
	}

	checkLinearizability(t, recorder, ops)
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
		numKeys    = 16
		shardID    = 1
	)

	duration := soakDuration(t)
	t.Logf("rolling restart: duration=%v", duration)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, duration, recorder, &totalOps, done)
		}(i)
	}

	// A stall longer than this is a wedge, not an election. failoverTimeout
	// is the harness's own bound on how long a new leader may take.
	maxStall := 3 * failoverTimeout

	deadline := time.Now().Add(duration - 5*time.Second)
	rounds := 0
	var stalledSince time.Time
	for round := 0; time.Now().Before(deadline); round++ {
		idx := round % numNodes
		before := totalOps.Load()

		cluster.stopNode(idx)
		cluster.waitForClusterLeader(shardID, failoverTimeout)
		cluster.restartNode(idx, shardID)
		leaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)

		// Wait for the restarted replica to CATCH UP before taking the next
		// one down. A real rolling upgrade does the same, and skipping it
		// runs the cluster at the quorum edge with a lagging replica: with
		// one node down and one still replaying its WAL, every commit needs
		// all three remaining healthy nodes, so the cluster stalls for
		// reasons that have nothing to do with a defect.
		waitForCatchUp(t, cluster, shardID, idx, leaderIdx, failoverTimeout)
		rounds++

		// Track the longest window with no committed operation. A stall
		// bounded by an election is CORRECT -- Raft cannot commit without a
		// leader, so demanding progress in every single round would assert
		// more than the protocol promises. What must not happen is a stall
		// that outlasts failover: that is a wedged cluster, not an election.
		if after := totalOps.Load(); after == before {
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

	ops := totalOps.Load()
	t.Logf("rolling restart complete: %d node restarts, %d operations", rounds, ops)
	if rounds == 0 {
		t.Fatalf("no restart rounds completed in %v; increase %s", duration, soakDurationEnv)
	}
	checkLinearizability(t, recorder, ops)
}

// waitForCatchUp blocks until the replica at idx has applied close to what the
// leader has, so a rolling restart proceeds only once the cluster is genuinely
// healthy again rather than merely having elected someone.
func waitForCatchUp(t *testing.T, cluster *testCluster, shardID uint64, idx, leaderIdx int, timeout time.Duration) {
	t.Helper()
	// A small lag is normal on a live cluster taking writes; require the
	// replica to be within a bounded window of the leader rather than exactly
	// equal, which a moving target would never satisfy.
	const maxLag = 50
	waitForCondition(t, timeout, "restarted replica catches up", func() bool {
		cluster.hostsMu.RLock()
		defer cluster.hostsMu.RUnlock()
		if cluster.stopped[idx].Load() || cluster.stopped[leaderIdx].Load() {
			return false
		}
		self, err := cluster.hosts[idx].GetShardInfo(shardID)
		if err != nil {
			return false
		}
		leader, err := cluster.hosts[leaderIdx].GetShardInfo(shardID)
		if err != nil {
			return false
		}
		return self.LastApplied+maxLag >= leader.LastApplied
	})
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
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, 20*time.Second, recorder, &totalOps, done)
		}(i)
	}

	// Let the cluster reach steady state so the measurement is not dominated
	// by warmup.
	time.Sleep(2 * time.Second)
	if totalOps.Load() == 0 {
		close(done)
		wg.Wait()
		t.Fatal("no operations committed before the leader was closed")
	}

	// Close the leader and measure how long writes are unavailable.
	before := totalOps.Load()
	closeStart := time.Now()
	cluster.stopNode(leaderIdx)
	closeElapsed := time.Since(closeStart)

	// Split the outage into "how long until a new leader exists" and "how
	// long after that until writes actually land". Those have different
	// causes -- a slow election vs. clients not retrying promptly -- and the
	// aggregate number cannot distinguish them.
	// electedAt is written by the election watcher and read by the log line
	// below while that goroutine may still be running, so it has to be atomic.
	var electedAt atomic.Int64
	go func() {
		es := time.Now()
		cluster.waitForClusterLeader(shardID, failoverTimeout)
		electedAt.Store(int64(time.Since(es)))
	}()

	// Require SUSTAINED recovery, not a single completion. An operation that
	// was already in flight when the leader closed will finish moments later
	// and would otherwise be misread as "the cluster is healthy again",
	// hiding an outage entirely. Demand a run of new commits instead.
	const recoveredOps = 50
	outage := time.Duration(0)
	deadline := time.Now().Add(maxOutage + 10*time.Second)
	for time.Now().Before(deadline) {
		if totalOps.Load() >= before+recoveredOps {
			outage = time.Since(closeStart)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(done)
	wg.Wait()

	t.Logf("leader close took %v; new leader after %v; %d sustained ops committed after %v (%d total)",
		closeElapsed, time.Duration(electedAt.Load()), recoveredOps, outage, totalOps.Load())

	if outage == 0 {
		t.Fatalf("writes never resumed within %v of closing the leader", maxOutage+5*time.Second)
	}
	if outage > maxOutage {
		t.Errorf("writes were unavailable for %v after gracefully closing the "+
			"leader, exceeding %v: that is a drain-length stall, meaning "+
			"leadership was not handed off before draining", outage, maxOutage)
	}

	checkLinearizability(t, recorder, totalOps.Load())
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
		info, err := cluster.hosts[i].GetShardInfo(shardID)
		if err != nil {
			t.Logf("  node[%d]: GetShardInfo error: %v", i, err)
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
