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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// pauseHold is how long a replica stays paused in these tests: long enough
// for several election timeouts and for the rest of the shard to commit.
const pauseHold = 2 * time.Second

// pauseScenario selects what runPausedReplica does around the pause.
type pauseScenario struct {
	leader    bool // pause the leader rather than a follower
	underLoad bool // keep proposing through a healthy replica while paused
	sealCycle bool // Seal() the paused replica's barrier, Unseal() before resume
	inFlight  bool // propose on the target just before pausing it
}

func (s pauseScenario) role() string {
	if s.leader {
		return "leader"
	}
	return "follower"
}

func TestE2E_PausedFollower_Idle(t *testing.T) {
	runPausedReplica(t, pauseScenario{})
}

func TestE2E_PausedLeader_Idle(t *testing.T) {
	runPausedReplica(t, pauseScenario{leader: true})
}

func TestE2E_PausedFollower_UnderLoad(t *testing.T) {
	runPausedReplica(t, pauseScenario{underLoad: true, inFlight: true})
}

func TestE2E_PausedLeader_UnderLoad(t *testing.T) {
	runPausedReplica(t, pauseScenario{leader: true, underLoad: true, inFlight: true})
}

// The runtime seal the pause exists for: pause, Seal(), Unseal(), resume,
// with a proposal in flight across the whole cycle.
func TestE2E_PausedFollower_SealCycle(t *testing.T) {
	runPausedReplica(t, pauseScenario{underLoad: true, sealCycle: true, inFlight: true})
}

func TestE2E_PausedLeader_SealCycle(t *testing.T) {
	runPausedReplica(t, pauseScenario{leader: true, underLoad: true, sealCycle: true, inFlight: true})
}

// pausedRefusals asserts that every client call on the paused replica is
// refused with a Temporary ErrShardPaused.
func pausedRefusals(t *testing.T, host *quicraft.Host, role string, peer uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := []struct {
		name string
		err  error
	}{}
	add := func(name string, err error) {
		calls = append(calls, struct {
			name string
			err  error
		}{name, err})
	}
	//quicraft:direct-propose the paused replica itself is the subject: it must refuse
	_, err := host.SyncPropose(ctx, 1, encodeKVPut("paused", "x"))
	add("SyncPropose", err)
	//quicraft:direct-read the paused replica itself is the subject: it must refuse
	_, err = host.SyncRead(ctx, 1, encodeKVGet("warmup"))
	add("SyncRead", err)
	//quicraft:direct-read the paused replica itself is the subject: it must refuse
	_, err = host.ReadIndex(ctx, 1)
	add("ReadIndex", err)
	//quicraft:direct-read the paused replica itself is the subject: it must refuse
	_, err = host.StaleRead(ctx, 1, encodeKVGet("warmup"))
	add("StaleRead", err)
	//quicraft:direct-read the paused replica itself is the subject: it must refuse
	_, err = host.FollowerRead(ctx, 1, encodeKVGet("warmup"))
	add("FollowerRead", err)
	_, err = host.GetNewSession(ctx, 1)
	add("GetNewSession", err)
	_, err = host.ExecuteOnLeader(ctx, 1, []byte("req"))
	add("ExecuteOnLeader", err)
	add("RequestLeaderTransfer", host.RequestLeaderTransfer(ctx, 1, peer))
	_, err = host.RequestSnapshot(ctx, 1, quicraft.SnapshotOption{})
	add("RequestSnapshot", err)
	for _, c := range calls {
		if !errors.Is(c.err, quicraft.ErrShardPaused) {
			t.Fatalf("%s on the paused %s: err = %v, want ErrShardPaused", c.name, role, c.err)
		}
		if !quicraft.Temporary(c.err) {
			t.Fatalf("%s on the paused %s: Temporary(%v) = false", c.name, role, c.err)
		}
	}
}

// healthyLeader reports the leader replica ID both healthy replicas agree on,
// or 0 while they do not.
func healthyLeader(cluster *testCluster, skip int) uint64 {
	var agreed uint64
	for i, h := range cluster.hosts {
		if i == skip {
			continue
		}
		id, ok, err := h.GetLeaderID(1)
		if err != nil || !ok || id == 0 {
			return 0
		}
		if agreed != 0 && agreed != id {
			return 0
		}
		agreed = id
	}
	return agreed
}

func assertNoEngineFailure(t *testing.T, what string) {
	t.Helper()
	engineFailures.mu.Lock()
	tripped := engineFailures.first
	engineFailures.mu.Unlock()
	if tripped != "" {
		t.Fatalf("%s: a shard failure latch tripped: %s", what, tripped)
	}
}

func runPausedReplica(t *testing.T, sc pauseScenario) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)
	proposeAndWait(t, cluster.testCluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "warmup", "ok", defaultTimeout)

	role := sc.role()
	target := cluster.findFollower(1)
	if sc.leader {
		target = cluster.findLeader(1)
	}
	if target < 0 {
		t.Fatalf("no %s to pause", role)
	}
	host := cluster.hosts[target]
	replicaID := uint64(target + 1)
	healthy := (target + 1) % len(cluster.hosts)
	peer := uint64(healthy + 1)
	machine := trackers[target].Get(1, replicaID)
	if machine == nil {
		t.Fatalf("node[%d] has no state machine", target)
	}

	// A proposal accepted just before the pause. It is never applied on the
	// paused replica while paused; it completes after the resume, ends at
	// its deadline, or reports the maybe-applied NotLeaderError.
	var inFlight *quicraft.RequestState
	if sc.inFlight {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		//quicraft:direct-propose the paused replica itself is the subject: its proposal spans the pause
		rs, err := host.Propose(ctx, 1, encodeKVPut("in-flight", "v"))
		if err != nil {
			t.Fatalf("Propose on the %s before the pause: %v", role, err)
		}
		defer rs.Release()
		inFlight = rs
	}

	pctx, pcancel := context.WithTimeout(context.Background(), defaultTimeout)
	if err := host.PauseShard(pctx, 1); err != nil {
		pcancel()
		t.Fatalf("PauseShard node[%d] (%s): %v", target, role, err)
	}
	pcancel()
	_, inFlightAppliedBeforePause := machine.Get("in-flight")
	appliedAtPause := machine.LastApplied()
	recoversAtPause := machine.RecoverCount()
	if info, err := host.GetShardInfo(1); err != nil || !info.Paused {
		t.Fatalf("GetShardInfo on the paused %s = %+v, %v; want Paused", role, info, err)
	}
	if sc.sealCycle {
		if err := cluster.barriers[target].Seal(); err != nil {
			t.Fatalf("Seal node[%d]: %v", target, err)
		}
	}
	t.Logf("paused node[%d] (%s) for %v, load=%t seal=%t", target, role, pauseHold, sc.underLoad, sc.sealCycle)

	stopLoad := make(chan struct{})
	var load sync.WaitGroup
	var committed atomic.Int64
	var lastLoadKey atomic.Value
	if sc.underLoad {
		load.Add(1)
		go func() {
			defer load.Done()
			for i := 0; ; i++ {
				select {
				case <-stopLoad:
					return
				default:
				}
				// Errors are expected while a paused leader's term runs out;
				// the cluster must still commit once a new leader is elected.
				key := fmt.Sprintf("load-%04d", i)
				if _, err := proposer.Try(proposer.Pinned(cluster.hosts[healthy]), 1,
					encodeKVPut(key, "v"), 500*time.Millisecond); err == nil {
					committed.Add(1)
					lastLoadKey.Store(key)
				}
			}
		}()
	}

	deadline := time.Now().Add(pauseHold)
	for time.Now().Before(deadline) {
		pausedRefusals(t, host, role, peer)
		if got := machine.LastApplied(); got != appliedAtPause {
			t.Fatalf("the paused %s applied index %d (was %d at the pause)", role, got, appliedAtPause)
		}
		if inFlight != nil && !inFlightAppliedBeforePause {
			select {
			case r := <-inFlight.ResultC():
				if r.Err == nil {
					t.Fatalf("in-flight proposal completed successfully while its replica was paused")
				}
				t.Logf("in-flight proposal ended while paused: %v", r.Err)
				inFlight = nil
			default:
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if sc.leader {
		// Leadership moved off the paused replica and the rest kept going.
		waitForCondition(t, failoverTimeout, "the healthy pair agrees on a new leader",
			func() bool {
				id := healthyLeader(cluster.testCluster, target)
				return id != 0 && id != replicaID
			})
	}
	if sc.underLoad {
		waitForCondition(t, failoverTimeout, "the cluster commits while a replica is paused",
			func() bool { return committed.Load() > 0 })
	}
	close(stopLoad)
	load.Wait()
	if got := machine.LastApplied(); got != appliedAtPause {
		t.Fatalf("the paused %s applied index %d (was %d at the pause)", role, got, appliedAtPause)
	}
	if got := machine.RecoverCount(); got != recoversAtPause {
		t.Fatalf("the paused %s recovered a snapshot while paused", role)
	}
	t.Logf("the cluster committed %d load writes while the %s was paused", committed.Load(), role)

	if sc.sealCycle {
		strategy, err := newTestSealStrategy([]byte(fmt.Sprintf("test-passphrase-%d", target)))
		if err != nil {
			t.Fatalf("newTestSealStrategy: %v", err)
		}
		if err := cluster.barriers[target].Unseal(context.Background(), strategy, seal.Credentials{}); err != nil {
			t.Fatalf("Unseal node[%d]: %v", target, err)
		}
	}
	assertNoEngineFailure(t, "while paused")

	rctx, rcancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer rcancel()
	if err := host.ResumeShard(rctx, 1); err != nil {
		t.Fatalf("ResumeShard node[%d]: %v", target, err)
	}
	if info, err := host.GetShardInfo(1); err != nil || info.Paused {
		t.Fatalf("GetShardInfo after ResumeShard = %+v, %v; want not Paused", info, err)
	}

	// Everything committed while paused reaches the resumed replica, in log
	// order (assertApplyOrder, armed by the cluster, checks every apply).
	if key, ok := lastLoadKey.Load().(string); ok {
		waitForReplication(t, cluster.testCluster, trackers, 1, key, "v", defaultTimeout)
	}
	if inFlight != nil {
		select {
		case r := <-inFlight.ResultC():
			switch {
			case r.Err == nil:
				waitForReplication(t, cluster.testCluster, trackers, 1, "in-flight", "v", defaultTimeout)
			case errors.Is(r.Err, quicraft.ErrTimeout):
			default:
				if _, ok := quicraft.NotLeader(r.Err); !ok {
					t.Fatalf("in-flight proposal ended with %v; want success, ErrTimeout or a NotLeaderError", r.Err)
				}
			}
			t.Logf("in-flight proposal across the pause: applied before the pause=%t err=%v",
				inFlightAppliedBeforePause, r.Err)
		case <-time.After(3 * time.Minute):
			t.Fatal("in-flight proposal never completed after the resume")
		}
	}
	proposeAndWait(t, cluster.testCluster, trackers, 1, "after-resume", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "after-resume", "ok", defaultTimeout)
	if _, err := proposer.TryWith(proposer.Pinned(host), 1,
		encodeKVPut("via-resumed", "ok"), defaultTimeout, quicraft.Temporary); err != nil {
		t.Fatalf("propose through the resumed %s: %v", role, err)
	}
	waitForReplication(t, cluster.testCluster, trackers, 1, "via-resumed", "ok", defaultTimeout)
	if got := proposer.Read(t, proposer.Pinned(host), 1, encodeKVGet("via-resumed"), defaultTimeout); got != "ok" {
		t.Fatalf("read through the resumed %s = %v, want %q", role, got, "ok")
	}
	assertNoEngineFailure(t, "after the resume")
}

// A follower paused while the leader compacts past it neither receives nor
// recovers a snapshot, and its recovery is never exhausted into a shard
// failure; after the resume it catches up from a snapshot.
func TestE2E_PausedFollower_SnapshotRecoveryAfterResume(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShardWithConfig(1, func(shardID, replicaID uint64) config.Config {
		return testShardConfigWithSnapshot(shardID, replicaID, 20, 5)
	})
	leader := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	proposeAndWait(t, cluster.testCluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "warmup", "ok", defaultTimeout)

	target := cluster.findFollower(1)
	if target < 0 {
		t.Fatal("no follower to pause")
	}
	host := cluster.hosts[target]
	machine := trackers[target].Get(1, uint64(target+1))
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	if err := host.PauseShard(ctx, 1); err != nil {
		t.Fatalf("PauseShard: %v", err)
	}
	appliedAtPause := machine.LastApplied()
	recoversAtPause := machine.RecoverCount()

	// Enough entries for the leader to snapshot and compact past the paused
	// follower, so it can only catch up from a snapshot.
	for i := range 120 {
		if _, err := proposer.TryWith(proposer.Pinned(cluster.hosts[leader]), 1,
			encodeKVPut(fmt.Sprintf("snap-%03d", i), "v"), defaultTimeout, quicraft.Temporary); err != nil {
			t.Fatalf("propose snap-%03d: %v", i, err)
		}
	}
	assertSnapshotTaken(t, cluster.testCluster, leader)
	// Taking the snapshot does not compact the log: compaction runs
	// asynchronously after the save. Until the leader's log starts past the
	// paused follower's next index, the follower can still catch up by replay.
	waitForCondition(t, defaultTimeout, "leader compacts past the paused follower", func() bool {
		lr, err := cluster.hosts[leader].GetLogRange(1)
		return err == nil && lr.FirstIndex > appliedAtPause+1
	})
	// Several heartbeat rounds in which the leader tries to send its snapshot.
	time.Sleep(pauseHold)

	if got := machine.LastApplied(); got != appliedAtPause {
		t.Fatalf("paused follower applied index %d (was %d)", got, appliedAtPause)
	}
	if got := machine.RecoverCount(); got != recoversAtPause {
		t.Fatal("paused follower recovered a snapshot while paused")
	}
	assertNoEngineFailure(t, "snapshot sends to a paused follower")

	if err := host.ResumeShard(ctx, 1); err != nil {
		t.Fatalf("ResumeShard: %v", err)
	}
	waitForReplication(t, cluster.testCluster, trackers, 1, "snap-119", "v", defaultTimeout)
	assertCaughtUpViaSnapshot(t, trackers[target], 1, uint64(target+1), defaultTimeout)
	assertNoEngineFailure(t, "after the resume")
}

// Every shard of these hosts runs on the one step, commit and apply worker
// (NumWorkers and MaxApplyWorkers are 1): pausing one shard on a replica
// leaves the other shard on the same worker committing and applying there.
func TestE2E_PausedShard_SharedWorkerUnaffected(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	trackers1 := cluster.startShard(1)
	trackers2 := cluster.startShard(2)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)
	cluster.waitForClusterLeader(2, leaderElectionTimeout)
	proposeAndWait(t, cluster.testCluster, trackers1, 1, "warmup", "ok")
	proposeAndWait(t, cluster.testCluster, trackers2, 2, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers1, 1, "warmup", "ok", defaultTimeout)
	waitForReplication(t, cluster.testCluster, trackers2, 2, "warmup", "ok", defaultTimeout)

	target := cluster.findFollower(1)
	if target < 0 {
		t.Fatal("no follower of shard 1 to pause")
	}
	host := cluster.hosts[target]
	paused := trackers1[target].Get(1, uint64(target+1))
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	if err := host.PauseShard(ctx, 1); err != nil {
		t.Fatalf("PauseShard: %v", err)
	}
	appliedAtPause := paused.LastApplied()

	for i := range 10 {
		key := fmt.Sprintf("other-%02d", i)
		proposeAndWait(t, cluster.testCluster, trackers2, 2, key, "v")
		waitForReplication(t, cluster.testCluster, trackers2, 2, key, "v", defaultTimeout)
	}
	//quicraft:direct-read shard 2 on the replica whose shard 1 is paused is the subject
	if _, err := host.StaleRead(ctx, 2, encodeKVGet("other-09")); err != nil {
		t.Fatalf("StaleRead of shard 2 on the replica with shard 1 paused: %v", err)
	}
	if info, err := host.GetShardInfo(2); err != nil || info.Paused {
		t.Fatalf("GetShardInfo(2) = %+v, %v; want not Paused", info, err)
	}
	if got := paused.LastApplied(); got != appliedAtPause {
		t.Fatalf("paused shard 1 applied index %d (was %d)", got, appliedAtPause)
	}

	if err := host.ResumeShard(ctx, 1); err != nil {
		t.Fatalf("ResumeShard: %v", err)
	}
	proposeAndWait(t, cluster.testCluster, trackers1, 1, "after-resume", "ok")
	waitForReplication(t, cluster.testCluster, trackers1, 1, "after-resume", "ok", defaultTimeout)
	assertNoEngineFailure(t, "shared worker")
}
