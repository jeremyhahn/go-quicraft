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

package raft_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Leadership stability, transfer, and step-down
// ---------------------------------------------------------------------------
//
// That three nodes agree on a leader ID after an election is the shallowest
// thing a leadership suite can assert. It says nothing about whether the leader STAYS the leader while it is healthy,
// whether leadership can be moved on request, or whether a replica that has
// lost leadership stops behaving like a leader — and each of those is a defect
// class with real consequences: a cluster that re-elects under its own write
// traffic makes no progress, a transfer that silently does nothing turns every
// rolling restart into an unplanned failover, and a replica that keeps
// answering as leader after stepping down is the front half of split brain.

// leaderView is one node's answer about who leads the shard and in what term.
// The pair is read through Host.LeaderIDAndTerm rather than through two
// separate calls, so it is a combination that actually held on that replica.
type leaderView struct {
	index    int
	leaderID uint64
	term     uint64
	known    bool
}

// observeLeaderViews samples every node in the cluster once.
func observeLeaderViews(c *testCluster, shardID uint64) []leaderView {
	views := make([]leaderView, 0, len(c.hosts))
	for i, h := range c.hosts {
		leaderID, term, known, err := h.LeaderIDAndTerm(shardID)
		if err != nil {
			continue
		}
		views = append(views, leaderView{index: i, leaderID: leaderID, term: term, known: known})
	}
	return views
}

// waitForUnanimousLeader waits until every node names the same leader in the
// same term, and returns that pair. A test that asserts stability has to start
// from a settled cluster, or it measures the tail of the bootstrap election
// instead of the property it is about.
func waitForUnanimousLeader(t *testing.T, c *testCluster, shardID uint64, timeout time.Duration) (leaderID, term uint64) {
	t.Helper()
	waitForCondition(t, timeout, "every node names the same leader in the same term", func() bool {
		views := observeLeaderViews(c, shardID)
		if len(views) != len(c.hosts) {
			return false
		}
		leaderID, term = views[0].leaderID, views[0].term
		if !views[0].known || leaderID == 0 {
			return false
		}
		for _, v := range views[1:] {
			if !v.known || v.leaderID != leaderID || v.term != term {
				return false
			}
		}
		return true
	})
	return leaderID, term
}

// TestIntegration_LeaderStaysLeaderUnderSustainedLoad asserts that a healthy
// leader is not superseded while it is doing its job.
//
// Nothing in this suite covered the negative: that no election happens. An
// election is not free — every replica stops serving for the duration, every
// in-flight proposal is refused or delayed, and the new leader has to commit a
// no-op before it can answer a read. A cluster that re-elects under its own
// write traffic is functionally down while looking perfectly healthy in any
// test that only ever waits for "a" leader to exist.
//
// The assertion is on the TERM, not just the leader ID. Term is the only
// direct evidence that no election was attempted: a shard can churn through
// elections and come back to the same replica, which a leader-ID-only check
// reads as stability. Every node is sampled, not just the leader, because a
// follower whose election timer fires bumps the term everywhere it can reach.
//
// The write floor at the end is what stops this from passing vacuously: with
// no traffic at all, a leader trivially remains leader, and this test would be
// asserting nothing about behaviour under load.
//
// The property has a precondition the test checks: Raft keeps
// a healthy leader only while heartbeats are delivered well inside the
// election timeout, which here is ElectionRTT (10) ticks of 50ms randomised
// up to twice that — 500ms to 1s. A follower cannot tell a leader whose
// process is not being scheduled from a dead one, so once the host freezes
// goroutines for longer than that, forgetting the leader and campaigning is
// the protocol working, not failing. Per-send and per-receive timestamps on a
// 24-core box at load ~200 show exactly this: in two losses the leader sent
// on every tick while nothing crossed to one follower
// for 0.6-1.3s; in the third the leader's own step worker was frozen
// mid-iteration for 1.6s, `runnable`, holding no lock, with no I/O in its
// path. Nothing in the engine stopped; the kernel stopped running it.
//
// The scheduling probe below measures that freeze directly. A follower that
// forgets the leader, a term that advances, or a write floor that is missed
// is a verdict on the engine unless a freeze of followerStallCeiling or more
// ended inside the electionRound before it (the whole window, for the
// floor); a forgotten leader after such a freeze is then pinned over one
// electionRound rather than read at an instant, because a leader that never
// stopped sending re-joins its follower at the same term as soon as delivery
// resumes. Two replicas leading one term is a safety violation and is never
// excused by the environment.
func TestIntegration_LeaderStaysLeaderUnderSustainedLoad(t *testing.T) {
	const (
		shardID     = uint64(1)
		numWriters  = 4
		loadWindow  = 4 * time.Second
		samplePause = 10 * time.Millisecond
		// writeFloor is deliberately well under the measured rate. The
		// linearizability suite records 5-19 committed writes per client-second
		// against this configuration; four writers over four seconds clear 20
		// with more than an order of magnitude of headroom, while a run that
		// stalled entirely cannot.
		writeFloor = 20
	)

	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(shardID)
	warmupIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// A committed entry in the current term (PhD 6.4) before anything is
	// measured, so the leader is fully established rather than mid-handover.
	proposeCommitted(t, cluster.hosts[warmupIdx], shardID, "stability-warmup", "ok", defaultTimeout)
	baselineLeader, baselineTerm := waitForUnanimousLeader(t, cluster, shardID, leaderElectionTimeout)
	t.Logf("settled: replica %d leads term %d", baselineLeader, baselineTerm)

	// The writers address the replica the baseline was actually taken from,
	// not the index sampled before the warmup. Those are the same node in the
	// common case and different whenever the warmup spanned an election — and
	// when they differ, every write below is refused by a healthy cluster and
	// the run fails the write floor while the term assertion, the real subject
	// of this test, never fires. Members are numbered replicaID = index+1.
	leaderIdx := int(baselineLeader) - 1
	if leaderIdx < 0 || leaderIdx >= len(cluster.hosts) {
		t.Fatalf("baseline leader replica %d has no host in a %d-node cluster",
			baselineLeader, len(cluster.hosts))
	}

	probe := startSchedulingProbe()
	defer probe.stop()

	stop := make(chan struct{})
	var committed atomic.Int64
	var wg sync.WaitGroup
	for w := range numWriters {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				//quicraft:direct-propose pinned to the baseline leader on purpose: the term assertion is the subject
				_, err := cluster.hosts[leaderIdx].SyncPropose(ctx, shardID,
					encodeKVPut(fmt.Sprintf("load-%d-%d", writer, n), "v"))
				cancel()
				if err == nil {
					committed.Add(1)
				}
			}
		}(w)
	}

	stopWriters := func() {
		close(stop)
		wg.Wait()
	}

	deadline := time.Now().Add(loadWindow)
	var samples, flickers int
	// forgottenSince is the instant a follower was first seen without a
	// leader after a qualifying stall, and zero while every node names the
	// baseline. See the default case below.
	var forgottenSince time.Time
	var forgottenStall time.Duration
	for time.Now().Before(deadline) {
		allBaseline := true
		for _, v := range observeLeaderViews(cluster, shardID) {
			samples++
			switch {
			case v.known && v.leaderID == baselineLeader && v.term == baselineTerm:
				continue
			case v.term != baselineTerm:
				stopWriters()
				failUnlessStalled(t, probe, time.Now(), electionRound, "node[%d] reports term %d, but the shard was in term %d "+
					"with replica %d leading and no fault was injected. The term "+
					"only advances when a replica campaigns: the cluster held an "+
					"election under nothing but its own write traffic.",
					v.index, v.term, baselineTerm, baselineLeader)
			case v.known && v.leaderID != baselineLeader:
				stopWriters()
				t.Fatalf("node[%d] reports replica %d as leader of term %d; "+
					"replica %d led that term. Two replicas cannot lead one term.",
					v.index, v.leaderID, v.term, baselineLeader)
			default:
				// A follower with no leader at the baseline term: its election
				// timer fired and it is a pre-candidate. That instant read is
				// a failure unless a stall that could have fired the timer
				// preceded it — and even then it is not yet a verdict: a
				// leader that never stopped sending re-joins the follower at
				// the SAME term as soon as delivery resumes, which a dead or
				// wedged leader cannot do. So the observation is pinned over
				// one electionRound: restored, and the run continues with the
				// flicker on record; not restored, or restored by an election,
				// and the run is inconclusive or failed above.
				allBaseline = false
				if !forgottenSince.IsZero() {
					if time.Since(forgottenSince) > electionRound {
						stopWriters()
						t.Skipf("INCONCLUSIVE, not a pass. node[%d] forgot replica %d "+
							"after a %v goroutine stall and was not re-joined to it "+
							"at term %d within %v. The stall exceeds the %v ceiling, "+
							"so the host, not the engine, is the first suspect; "+
							"re-run on a host that is not oversubscribed.",
							v.index, baselineLeader, forgottenStall, baselineTerm,
							electionRound, followerStallCeiling)
					}
					continue
				}
				now := time.Now()
				forgottenStall = stallExcusing(t, probe, now, electionRound, stopWriters,
					fmt.Sprintf("node[%d] lost track of the leader while replica %d was "+
						"healthy and leading term %d. A follower that forgets its "+
						"leader has stopped receiving heartbeats and is one election "+
						"timeout away from campaigning against a working leader.",
						v.index, baselineLeader, baselineTerm))
				forgottenSince = now
				flickers++
				t.Logf("node[%d] forgot replica %d after a %v goroutine stall (ceiling %v); "+
					"allowing %v for it to be re-joined at term %d",
					v.index, baselineLeader, forgottenStall, followerStallCeiling,
					electionRound, baselineTerm)
			}
		}
		if allBaseline && !forgottenSince.IsZero() {
			t.Logf("every node names replica %d at term %d again, %v after the stall",
				baselineLeader, baselineTerm, time.Since(forgottenSince))
			forgottenSince = time.Time{}
		}
		// Sampling interval across the load window: the window runs for
		// loadWindow by design, not until a condition.
		time.Sleep(samplePause)
	}

	stopWriters()

	if n := committed.Load(); n < writeFloor {
		failUnlessStalled(t, probe, time.Now(), loadWindow, "only %d writes committed in %v across %d writers (floor %d): "+
			"the leader was never actually put under load, so its stability "+
			"proves nothing", n, loadWindow, numWriters, writeFloor)
	}
	t.Logf("replica %d held term %d across %d observations while committing %d writes "+
		"(%d stall-excused flickers; longest goroutine freeze %v over %d probe samples, ceiling %v)",
		baselineLeader, baselineTerm, samples, committed.Load(), flickers,
		probe.longestFreeze(), probe.sampleCount(), followerStallCeiling)

	// The data written under load must be on the leader's state machine, so
	// that "stable" cannot mean "wedged but still heartbeating".
	assertSMHasExact(t, trackers[leaderIdx].Get(shardID, uint64(leaderIdx+1)),
		fmt.Sprintf("node[%d]", leaderIdx), "stability-warmup", "ok")
}

// electionTimeoutFloor is the SHORTEST election timeout a follower in this
// suite can draw: ElectionRTT ticks, before randomisation adds up to another
// ElectionRTT on top (see electionRound). A follower that hears nothing from
// its leader for this long may, correctly, forget it and campaign.
const electionTimeoutFloor = time.Duration(testElectionRTT) * raftTick

// followerStallCeiling is the longest a single goroutine wake-up may be seen
// to lag during the load window before a lost leader stops being evidence
// about the engine and becomes evidence about the host.
//
// It is HALF the floor, and the half is not headroom. A heartbeat needs one
// wake-up on the leader's side to be sent and one on the follower's side to
// be delivered, so two stalls of this size back to back — one per side of
// the wire, with every other goroutine running perfectly — are a whole
// election timeout of silence, and the follower's timer fires on a leader
// that never stopped sending. Stalls of 0.37s to 1.6s have been recorded on a
// loaded CI box against this 0.5s floor; the losses they produced were all of
// that shape.
//
// Derived, not chosen: it follows the shard configuration through
// electionTimeoutFloor, and TestBudgetsDescribeTheClusterTheyBudgetFor pins
// that configuration to the constants the derivation reads.
const followerStallCeiling = electionTimeoutFloor / 2

// schedulingProbe measures the one thing the stability assertion assumes and
// cannot control: that a goroutine in this process which is RUNNING keeps
// running. It is a goroutine that does nothing but timestamp its own
// progress; the longest gap between two consecutive timestamps is the
// longest this process had a running goroutine frozen, which is the
// situation the step worker that owns the shard is in when the kernel parks
// the thread underneath it mid-iteration.
//
// It spins on purpose, for the length of the load window, on one core. A
// probe that sleeps on a ticker measures something else — how long a
// wake-up waits for a P — and the first cut of this gate used one: it
// reported a worst lag of 188ms, under the ceiling, in a run where a
// follower had just lost a leader whose step worker had been frozen for
// longer than the election timeout with no lock held and no I/O in its
// path. A sleeping probe is never the goroutine that gets parked.
type schedulingProbe struct {
	stopC   chan struct{}
	doneC   chan struct{}
	maxGap  atomic.Int64
	samples atomic.Int64

	// stalls records every gap of followerStallCeiling or more, with the
	// instant the probe resumed, so a loss can be checked against the stalls
	// that PRECEDED it rather than against the worst moment of the whole
	// window. A stall after the loss cannot have caused it, and neither can
	// one that ended a full election timeout before.
	mu     sync.Mutex
	stalls []probeStall
}

// probeStall is one freeze of at least followerStallCeiling.
type probeStall struct {
	at  time.Time
	gap time.Duration
}

// startSchedulingProbe starts the probe; it runs until stop.
func startSchedulingProbe() *schedulingProbe {
	p := &schedulingProbe{
		stopC: make(chan struct{}),
		doneC: make(chan struct{}),
	}
	go p.run()
	return p
}

func (p *schedulingProbe) run() {
	defer close(p.doneC)
	last := time.Now()
	for {
		select {
		case <-p.stopC:
			return
		default:
		}
		now := time.Now()
		gap := now.Sub(last)
		if gap > time.Duration(p.maxGap.Load()) {
			p.maxGap.Store(int64(gap))
		}
		if gap >= followerStallCeiling {
			p.mu.Lock()
			p.stalls = append(p.stalls, probeStall{at: now, gap: gap})
			p.mu.Unlock()
		}
		p.samples.Add(1)
		last = now
	}
}

// stop ends sampling and waits for the probe goroutine to exit.
func (p *schedulingProbe) stop() {
	close(p.stopC)
	<-p.doneC
}

// longestFreeze is the longest gap between consecutive samples so far.
func (p *schedulingProbe) longestFreeze() time.Duration {
	return time.Duration(p.maxGap.Load())
}

// sampleCount is how many timestamps the probe has taken.
func (p *schedulingProbe) sampleCount() int64 {
	return p.samples.Load()
}

// worstStallBefore is the largest ceiling-exceeding freeze that ended within
// lookback of at, and zero if there was none. A freeze that ended after at is
// not counted.
func (p *schedulingProbe) worstStallBefore(at time.Time, lookback time.Duration) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	var worst time.Duration
	for _, st := range p.stalls {
		if st.at.After(at) || at.Sub(st.at) > lookback {
			continue
		}
		if st.gap > worst {
			worst = st.gap
		}
	}
	return worst
}

// stallExcusing returns the stall that could have produced the finding on
// its own, or fails the test with the finding if there was none.
//
// The stall has to be one that could have done it: it must have ended
// within lookback of the observation at observed. For a lost leader or an
// advanced term the lookback is electionRound, the longest a follower's
// randomised timer can run without leader traffic, because a stall that
// ended earlier than that had already been followed by a whole timeout's
// worth of heartbeats. A missed write floor looks back over the entire
// window, since a stall anywhere in it lowers throughput. A stall that
// merely happened somewhere in the window does not excuse a loss it could
// not have caused; the first cut of this gate did exactly that and pardoned
// a leader that had been deliberately closed.
//
// beforeFailing runs before t.Fatalf so the caller can stop its writers.
func stallExcusing(t *testing.T, probe *schedulingProbe, observed time.Time, lookback time.Duration, beforeFailing func(), finding string) time.Duration {
	t.Helper()
	stall := probe.worstStallBefore(observed, lookback)
	if stall < followerStallCeiling {
		if beforeFailing != nil {
			beforeFailing()
		}
		t.Fatalf("%s No running goroutine in this process was frozen for the %v "+
			"ceiling in the %v before the observation (longest freeze over the "+
			"whole window: %v), so the host ran this process well enough for "+
			"the loss to be the engine's doing.", finding, followerStallCeiling,
			lookback, probe.longestFreeze())
	}
	return stall
}

// failUnlessStalled fails the test with the finding unless the probe saw a
// stall that could have produced it (see stallExcusing), in which case the
// finding is still reported, in full, and the run is declared inconclusive
// rather than passed.
//
// Only outcomes an election timeout can produce go through here. A safety
// violation never does.
func failUnlessStalled(t *testing.T, probe *schedulingProbe, observed time.Time, lookback time.Duration, format string, args ...any) {
	t.Helper()
	finding := fmt.Sprintf(format, args...)
	stall := stallExcusing(t, probe, observed, lookback, nil, finding)
	t.Skipf("INCONCLUSIVE, not a pass. %s A running goroutine in this process was "+
		"frozen for %v within the %v before the observation (ceiling %v: half "+
		"the %v election-timeout floor of ElectionRTT %d x %v tick). Two such "+
		"stalls, one on each side of the wire, are a full election timeout of "+
		"silence from a leader that never stopped sending, so a follower's "+
		"timer firing here says nothing about the engine. Re-run on a host that "+
		"is not oversubscribed.", finding, stall, lookback, followerStallCeiling,
		electionTimeoutFloor, testElectionRTT, raftTick)
}

// requestTransferUntilLeader asks the current leader to hand leadership to
// targetReplica and waits for the target to actually hold it.
//
// The request is repeated rather than issued once. RequestLeaderTransfer is
// fire-and-forget by contract: it returns as soon as the message is delivered
// to the raft state machine, it is refused outright while another transfer is
// pending, and a transfer whose target is behind is completed only after the
// leader has caught it up. A single call followed by a wait would therefore be
// testing timing as much as the transfer itself. Retrying does not weaken the
// assertion — leadership must still end up on the requested replica, and the
// deadline is a hard failure.
func requestTransferUntilLeader(t *testing.T, c *testCluster, shardID, targetReplica uint64, timeout time.Duration) {
	t.Helper()

	targetIdx := int(targetReplica) - 1
	// The condition's description is fixed before the wait starts, so each new
	// RequestLeaderTransfer error is logged as it appears, for the failure.
	var lastErr error
	waitForCondition(t, timeout, fmt.Sprintf("leadership moves to replica %d. A transfer "+
		"that is accepted and then does nothing turns every planned handover — rolling "+
		"restart, drain, maintenance — into an unplanned failover that waits out an "+
		"election timeout", targetReplica), func() bool {
		info, err := c.hosts[targetIdx].ShardInfo(shardID)
		if err == nil && info.IsLeader {
			return true
		}
		if leaderIdx := c.findLeader(shardID); leaderIdx >= 0 {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			err = c.hosts[leaderIdx].RequestLeaderTransfer(ctx, shardID, targetReplica)
			cancel()
			if err != nil && (lastErr == nil || err.Error() != lastErr.Error()) {
				t.Logf("RequestLeaderTransfer(replica=%d): %v", targetReplica, err)
			}
			lastErr = err
		}
		return false
	})
}

// TestIntegration_LeadershipTransfer_MovesLeadershipToTheChosenReplica asserts
// that a requested handover completes, on the requested replica, and leaves a
// working cluster behind.
//
// A leadership transfer is the only mechanism by which leadership moves without
// an outage: the outgoing leader stops accepting proposals, catches the target
// up, and sends it TimeoutNow so it campaigns immediately instead of waiting
// out an election timeout. Every graceful operation in production depends on it
// — draining a node, rolling a release, moving load off a host about to be
// rebooted. Nothing in this suite exercised it at all, so a transfer that
// quietly did nothing would have been invisible: the cluster still has a
// leader, just not the one that was asked for.
func TestIntegration_LeadershipTransfer_MovesLeadershipToTheChosenReplica(t *testing.T) {
	const shardID = uint64(1)

	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(shardID)
	oldLeaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	proposeCommitted(t, cluster.hosts[oldLeaderIdx], shardID, "pre-transfer", "before", defaultTimeout)
	_, termBefore := waitForUnanimousLeader(t, cluster, shardID, leaderElectionTimeout)

	// Any replica that is not the incumbent will do; the first one is enough
	// and keeps the choice independent of which node won the bootstrap.
	targetIdx := (oldLeaderIdx + 1) % len(cluster.hosts)
	targetReplica := uint64(targetIdx + 1)
	t.Logf("transferring leadership from node[%d] to node[%d] (replica %d)",
		oldLeaderIdx, targetIdx, targetReplica)

	requestTransferUntilLeader(t, cluster, shardID, targetReplica, defaultTimeout)

	// The whole cluster must agree, not just the target. A target that believes
	// it leads while the other replicas still follow the old leader is a split
	// brain, not a transfer.
	leaderAfter, termAfter := waitForUnanimousLeader(t, cluster, shardID, leaderElectionTimeout)
	if leaderAfter != targetReplica {
		t.Fatalf("after the transfer the cluster agrees on replica %d, but "+
			"leadership was requested for replica %d", leaderAfter, targetReplica)
	}
	if termAfter <= termBefore {
		t.Fatalf("leadership moved to replica %d in term %d, which is not above "+
			"the outgoing leader's term %d. A new leader always serves a NEW "+
			"term; the same term with a different leader is an election-safety "+
			"violation.", targetReplica, termAfter, termBefore)
	}
	t.Logf("replica %d leads term %d (was term %d)", leaderAfter, termAfter, termBefore)

	// A transfer that completes but leaves the shard unable to commit is not a
	// successful transfer. Write through the new leader and read it back
	// linearizably, plus the entry written before the handover — the new leader
	// must hold everything the old one committed (Leader Completeness).
	proposeCommitted(t, cluster.hosts[targetIdx], shardID, "post-transfer", "after", defaultTimeout)
	syncReadExact(t, cluster.hosts[targetIdx], shardID, "pre-transfer", "before", defaultTimeout)
	syncReadExact(t, cluster.hosts[targetIdx], shardID, "post-transfer", "after", defaultTimeout)
	assertSMHasExact(t, trackers[targetIdx].Get(shardID, targetReplica),
		fmt.Sprintf("node[%d]", targetIdx), "pre-transfer", "before")
}

// TestIntegration_SteppedDownLeader_StopsActingAsLeader asserts that the
// replica that GAVE UP leadership stops behaving as leader.
//
// This is the other half of a handover and the dangerous half. A replica that
// still reports IsLeader after stepping down will be selected as leader by
// every client, dashboard and test helper that asks it; if it also still
// accepts leader-only operations, it is issuing decisions for a term that
// belongs to someone else. The suite had no assertion anywhere that leadership
// is ever RELINQUISHED — only that it is acquired.
//
// The stepped-down replica must remain a fully working follower: it knows who
// leads now, it holds the data, and it refuses only the operations that require
// leadership. "Broken" is not the same as "not leader", and asserting the first
// would let a regression that kills the replica outright pass.
func TestIntegration_SteppedDownLeader_StopsActingAsLeader(t *testing.T) {
	const shardID = uint64(1)

	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(shardID)
	oldLeaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	oldLeaderReplica := uint64(oldLeaderIdx + 1)
	proposeCommitted(t, cluster.hosts[oldLeaderIdx], shardID, "pre-stepdown", "value", defaultTimeout)

	targetIdx := (oldLeaderIdx + 1) % len(cluster.hosts)
	targetReplica := uint64(targetIdx + 1)
	requestTransferUntilLeader(t, cluster, shardID, targetReplica, defaultTimeout)
	waitForUnanimousLeader(t, cluster, shardID, leaderElectionTimeout)

	oldLeader := cluster.hosts[oldLeaderIdx]

	// It must no longer claim leadership.
	info, err := oldLeader.ShardInfo(shardID)
	if err != nil {
		t.Fatalf("ShardInfo on the stepped-down replica %d failed: %v", oldLeaderReplica, err)
	}
	if info.IsLeader {
		t.Fatalf("replica %d handed leadership to replica %d and still reports "+
			"itself as leader of shard %d (term %d). Every client that asks it "+
			"will be told to use it, and it will answer for a term another "+
			"replica now owns.", oldLeaderReplica, targetReplica, shardID, info.Term)
	}

	// It must name the replica that took over, and be at least as current.
	leaderID, known, err := oldLeader.LeaderID(shardID)
	if err != nil || !known {
		t.Fatalf("the stepped-down replica %d does not know who leads shard %d "+
			"(known=%v, err=%v); it stepped down without learning who to follow",
			oldLeaderReplica, shardID, known, err)
	}
	if leaderID != targetReplica {
		t.Fatalf("the stepped-down replica %d names replica %d as leader; "+
			"leadership went to replica %d", oldLeaderReplica, leaderID, targetReplica)
	}

	// Leader-only operations must be refused. This is the behavioural half of
	// "not leader": a replica that merely reports IsLeader=false while still
	// accepting leader-only work has not really stepped down.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	err = oldLeader.RequestLeaderTransfer(ctx, shardID, oldLeaderReplica)
	cancel()
	if err == nil {
		t.Fatalf("the stepped-down replica %d accepted a leadership transfer "+
			"request, an operation only the leader may perform. Leadership is "+
			"held by replica %d.", oldLeaderReplica, targetReplica)
	}
	t.Logf("stepped-down replica %d refused a leader-only operation: %v", oldLeaderReplica, err)

	// And it must still be a healthy follower: same data, still replicating.
	assertSMHasExact(t, trackers[oldLeaderIdx].Get(shardID, oldLeaderReplica),
		fmt.Sprintf("node[%d]", oldLeaderIdx), "pre-stepdown", "value")
	proposeCommitted(t, cluster.hosts[targetIdx], shardID, "post-stepdown", "replicated", defaultTimeout)
	waitForCondition(t, defaultTimeout,
		fmt.Sprintf("stepped-down replica %d keeps replicating", oldLeaderReplica),
		func() bool {
			machine := trackers[oldLeaderIdx].Get(shardID, oldLeaderReplica)
			if machine == nil {
				return false
			}
			v, ok := machine.Get("post-stepdown")
			return ok && v == "replicated"
		})
	t.Logf("stepped-down replica %d is still a working follower of replica %d",
		oldLeaderReplica, targetReplica)
}
