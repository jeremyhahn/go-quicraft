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
// A follower that forgets the leader, a term that advances or a missed write
// floor is a verdict on the engine and fails the test; two replicas leading
// one term is a safety violation and fails it too. The second subtest is the
// control that keeps the first honest: it runs the same observation loop and
// closes the leader's host partway through the window, and fails unless the
// loop sees the term advance.
func TestIntegration_LeaderStaysLeaderUnderSustainedLoad(t *testing.T) {
	t.Run("HoldsTermUnderLoad", testLeaderHoldsTermUnderLoad)
	t.Run("Control_ClosedLeaderAdvancesTerm", testClosedLeaderAdvancesTerm)
}

// settledShard is a three-node shard whose every node names the same leader
// in the same term, after an entry committed in that term.
type settledShard struct {
	cluster  *testCluster
	trackers []*smTracker
	// leaderIdx is the cluster.hosts index of leader. Members are numbered
	// replicaID = index+1.
	leaderIdx int
	leader    uint64
	term      uint64
}

// settleShard starts shardID on a new three-node cluster, closed when the test
// ends, and returns it once every node names the same leader in the same term.
func settleShard(t *testing.T, shardID uint64) settledShard {
	t.Helper()
	cluster := newTestCluster(t, 3)
	t.Cleanup(cluster.close)

	trackers := cluster.startShard(shardID)
	warmupIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// A committed entry in the current term (PhD 6.4) before anything is
	// measured, so the leader is fully established rather than mid-handover.
	proposeCommitted(t, cluster.hosts[warmupIdx], shardID, "stability-warmup", "ok", defaultTimeout)
	leader, term := waitForUnanimousLeader(t, cluster, shardID, leaderElectionTimeout)
	t.Logf("settled: replica %d leads term %d", leader, term)

	// The caller addresses the replica the baseline was actually taken from,
	// not the index sampled before the warmup. Those are the same node in the
	// common case and different whenever the warmup spanned an election.
	leaderIdx := int(leader) - 1
	if leaderIdx < 0 || leaderIdx >= len(cluster.hosts) {
		t.Fatalf("baseline leader replica %d has no host in a %d-node cluster",
			leader, len(cluster.hosts))
	}
	return settledShard{cluster: cluster, trackers: trackers, leaderIdx: leaderIdx, leader: leader, term: term}
}

// leadershipWatch is what watchLeadership saw: the first observation of each
// way a shard can depart from its baseline, nil when it never did.
type leadershipWatch struct {
	samples int
	// forgotten is a node naming no leader at the baseline term: its election
	// timer fired and it is a pre-candidate.
	forgotten *leaderView
	// termAdvanced is a node at a term other than the baseline: a replica
	// campaigned. termAdvancedAt is when, from the start of the watch.
	termAdvanced   *leaderView
	termAdvancedAt time.Duration
	// secondLeader is a node naming another replica as leader of the
	// baseline term: two leaders in one term.
	secondLeader *leaderView
}

// watchLeadership samples every node's view of shardID across window,
// against the baseline leader and term. It runs the whole window unless it
// sees a term change or a second leader, either of which ends it at once.
// tick, when non-nil, is called before each round of samples with the time
// elapsed since the watch began, so a caller can inject a fault at a known
// point of the window.
func watchLeadership(c *testCluster, shardID, baselineLeader, baselineTerm uint64,
	window time.Duration, tick func(elapsed time.Duration)) leadershipWatch {
	const samplePause = 10 * time.Millisecond
	var w leadershipWatch
	start := time.Now()
	for elapsed := time.Duration(0); elapsed < window; elapsed = time.Since(start) {
		if tick != nil {
			tick(elapsed)
		}
		for _, v := range observeLeaderViews(c, shardID) {
			w.samples++
			switch {
			case v.known && v.leaderID == baselineLeader && v.term == baselineTerm:
			case v.term != baselineTerm:
				w.termAdvanced = &v
				w.termAdvancedAt = time.Since(start)
				return w
			case v.known && v.leaderID != baselineLeader:
				w.secondLeader = &v
				return w
			default:
				if w.forgotten == nil {
					w.forgotten = &v
				}
			}
		}
		// Sampling interval across the window: the watch runs for window by
		// design, not until a condition.
		time.Sleep(samplePause)
	}
	return w
}

// testLeaderHoldsTermUnderLoad drives four writers at the leader for the
// load window and fails on any departure from the baseline leader and term,
// or on a write floor missed.
func testLeaderHoldsTermUnderLoad(t *testing.T) {
	const (
		shardID    = uint64(1)
		numWriters = 4
		loadWindow = 4 * time.Second
		// writeFloor is deliberately well under the measured rate: 5/4
		// committed writes per writer-second, where the linearizability suite
		// records 5-19 per client-second against this configuration. A run
		// that stalled entirely cannot clear it; a healthy one clears it by
		// four times or more.
		writeFloor = numWriters * int64(loadWindow/time.Second) * 5 / 4
	)

	s := settleShard(t, shardID)

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
				_, err := s.cluster.hosts[s.leaderIdx].SyncPropose(ctx, shardID,
					encodeKVPut(fmt.Sprintf("load-%d-%d", writer, n), "v"))
				cancel()
				if err == nil {
					committed.Add(1)
				}
			}
		}(w)
	}

	w := watchLeadership(s.cluster, shardID, s.leader, s.term, loadWindow, nil)
	close(stop)
	wg.Wait()

	if v := w.secondLeader; v != nil {
		t.Fatalf("node[%d] reports replica %d as leader of term %d; "+
			"replica %d led that term. Two replicas cannot lead one term.",
			v.index, v.leaderID, v.term, s.leader)
	}
	if v := w.termAdvanced; v != nil {
		t.Fatalf("node[%d] reports term %d %v into the load window, but the shard "+
			"was in term %d with replica %d leading and no fault was injected. "+
			"The term only advances when a replica campaigns: the cluster held "+
			"an election under nothing but its own write traffic.",
			v.index, v.term, w.termAdvancedAt, s.term, s.leader)
	}
	if v := w.forgotten; v != nil {
		t.Fatalf("node[%d] lost track of the leader while replica %d was healthy "+
			"and leading term %d. A follower that forgets its leader has stopped "+
			"receiving heartbeats and is one election timeout away from "+
			"campaigning against a working leader.", v.index, s.leader, s.term)
	}
	if n := committed.Load(); n < writeFloor {
		t.Fatalf("only %d writes committed in %v across %d writers (floor %d): "+
			"the leader was never actually put under load, so its stability "+
			"proves nothing", n, loadWindow, numWriters, writeFloor)
	}
	t.Logf("replica %d held term %d across %d observations while committing %d writes in %v",
		s.leader, s.term, w.samples, committed.Load(), loadWindow)

	// The data written under load must be on the leader's state machine, so
	// that "stable" cannot mean "wedged but still heartbeating".
	assertSMHasExact(t, s.trackers[s.leaderIdx].Get(shardID, uint64(s.leaderIdx+1)),
		fmt.Sprintf("node[%d]", s.leaderIdx), "stability-warmup", "ok")
}

// testClosedLeaderAdvancesTerm is the control for testLeaderHoldsTermUnderLoad:
// the same watch, with the leader's host closed partway through the window. A
// closed leader sends nothing, so the survivors' election timers fire and one
// of them campaigns; the watch must report that term change, and if it never
// does, the load subtest's "no term change" proves nothing.
func testClosedLeaderAdvancesTerm(t *testing.T) {
	const (
		shardID    = uint64(1)
		closeAfter = time.Second
	)
	// After the close, the survivors need up to one randomised election
	// timeout to stop expecting the leader (electionRound bounds it) and one
	// more round for the election that advances the term.
	controlWindow := closeAfter + 2*electionRound

	s := settleShard(t, shardID)

	closed := false
	w := watchLeadership(s.cluster, shardID, s.leader, s.term, controlWindow, func(elapsed time.Duration) {
		if !closed && elapsed >= closeAfter {
			closed = true
			s.cluster.hosts[s.leaderIdx].Close()
		}
	})

	if !closed {
		t.Fatalf("the %v window ended before the leader's host was closed at %v", controlWindow, closeAfter)
	}
	if v := w.secondLeader; v != nil {
		t.Fatalf("node[%d] reports replica %d as leader of term %d; "+
			"replica %d led that term. Two replicas cannot lead one term.",
			v.index, v.leaderID, v.term, s.leader)
	}
	if w.termAdvanced == nil {
		t.Fatalf("replica %d's host was closed %v into a %v window, yet no node "+
			"reported a term past %d: the watch the load subtest relies on did "+
			"not see an election a dead leader must cause, so it is vacuous",
			s.leader, closeAfter, controlWindow, s.term)
	}
	t.Logf("control: node[%d] reports term %d (baseline %d) %v into the window, "+
		"after replica %d's host was closed at %v",
		w.termAdvanced.index, w.termAdvanced.term, s.term, w.termAdvancedAt, s.leader, closeAfter)
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
