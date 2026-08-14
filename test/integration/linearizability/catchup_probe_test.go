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

package linearizability_test

import (
	"sync"
	"testing"
	"time"
)

// TestRestartedReplicaResumesReplication asserts that a replica which restarts
// while the leader keeps writing is sent entries again, WITHOUT depending on a
// leader change to rescue it.
//
// This is the isolated form of the rolling-restart soak's "restarted replica
// catches up" assertion. The soak restarts a node every few seconds, so an
// election is usually in flight and a fresh leader rebuilds its per-peer
// replication state from scratch — which masks a leader that has stopped
// sending to one peer. Here the leadership deliberately stays put, so the
// leader's own recovery path is the only thing under test.
//
// The failure it guards against is a leader whose per-peer send window is full
// of messages that were delivered to the peer's PREVIOUS incarnation: those
// acks can never arrive, the window never drains, and the peer is starved
// indefinitely while heartbeats keep it looking healthy. The cluster still
// commits on the remaining majority, so nothing else reports a problem.
//
// Several restart cycles are needed: the first one or two find the leader's
// window for that peer already backed off (a rejection or an eviction moved it
// out of the pipelining state), which recovers on its own. The steady state,
// where the peer is dropped mid-pipeline, is what stalls.
func TestRestartedReplicaResumesReplication(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 6
		numKeys    = 16
		shardID    = 1
		victim     = 4
		buildUp    = 8 * time.Second
		downtime   = 2 * time.Second
		// Well above the sub-second catch-up a healthy leader achieves, and
		// below the soak's own 20s budget for the same condition.
		catchUpBudget = 15 * time.Second
		// After each catch-up the cluster is left alone so the leader returns
		// to steady-state pipelining before the next restart. Without it the
		// leader is still backed off from the previous cycle and re-sends
		// promptly, which is the case that never stalls.
		settle = 10 * time.Second
		cycles = 5
		// The same lag the soak tolerates: a live cluster taking writes is a
		// moving target, so exact equality would never hold.
		maxLag = 50
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	// The probe restarts one replica five times under sustained load and only
	// ever looks at the applied-index gap. A restarted replica that rejoined
	// still believing it leads the current term would satisfy that gap check
	// and go unnoticed, so the election-safety invariant is asserted alongside.
	startLeaderTermGuard(t, cluster, shardID)

	recorder := &opRecorder{}
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(t, cluster, shardID, clientID, numKeys, buildUp+cycles*(downtime+catchUpBudget+settle+5*time.Second),
				recorder, done)
		}(i)
	}
	defer func() {
		close(done)
		wg.Wait()
	}()

	// Let the leader reach steady-state pipelining before the first restart.
	time.Sleep(buildUp)

	for cycle := range cycles {
		cluster.stopNode(victim)
		time.Sleep(downtime)
		cluster.restartNode(victim, shardID)
		leaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
		if leaderIdx == victim {
			// The restarted node won the election; there is no starving
			// leader to test in this cycle.
			t.Logf("cycle %d: restarted node became leader, skipping", cycle)
			continue
		}

		// Arm the per-node diagnostics before the wait: the accessors publish
		// a snapshot only once asked, so a dump taken for the first time at
		// the moment of failure reports nothing but "warming up".
		cluster.hostsMu.RLock()
		for _, h := range cluster.hosts {
			_, _ = h.DiagRemotes(shardID)
			_, _, _, _, _, _, _ = h.DiagRaftState(shardID)
		}
		cluster.hostsMu.RUnlock()

		start := time.Now()
		ticker := time.NewTicker(20 * time.Millisecond)
		var caughtUp time.Duration
		var lastGap int64
		for time.Since(start) < catchUpBudget {
			<-ticker.C

			cluster.hostsMu.RLock()
			leaderHost := cluster.hosts[leaderIdx]
			victimHost := cluster.hosts[victim]
			stopped := cluster.stopped[leaderIdx].Load() || cluster.stopped[victim].Load()
			cluster.hostsMu.RUnlock()
			if stopped {
				continue
			}
			li, lerr := leaderHost.GetShardInfo(shardID)
			vi, verr := victimHost.GetShardInfo(shardID)
			if lerr != nil || verr != nil {
				continue
			}
			lastGap = int64(li.LastApplied) - int64(vi.LastApplied)
			if lastGap <= maxLag {
				caughtUp = time.Since(start)
				break
			}
		}
		ticker.Stop()

		if caughtUp == 0 {
			t.Logf("cycle %d: node[%d] still %d entries behind leader node[%d] "+
				"after %v; final state:", cycle, victim, lastGap, leaderIdx, catchUpBudget)
			dumpClusterState(t, cluster, shardID)
			t.Fatalf("restarted replica node[%d] was never sent entries again: "+
				"lag %d after %v with the leader alive and committing "+
				"(cycle %d, %d ops)", victim, lastGap, catchUpBudget, cycle, recorder.completed())
		}
		t.Logf("cycle %d: node[%d] caught up in %v (leader node[%d], %d ops)",
			cycle, victim, caughtUp.Round(time.Millisecond), leaderIdx, recorder.completed())
		time.Sleep(settle)
	}
}
