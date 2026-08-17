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

// TestIntegration_GetLeaderID_MultiNode_AllAgree verifies that in a 3-node
// cluster, all nodes eventually agree on the same leader ID after election.
func TestIntegration_GetLeaderID_MultiNode_AllAgree(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Determine the expected leader ID from the node that reports itself
	// as leader.
	leaderHost := cluster.hosts[leaderIdx]
	leaderInfo, err := leaderHost.GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo on leader node failed: %v", err)
	}
	expectedLeaderID := leaderInfo.ReplicaID

	// Wait for every node to agree on the leader ID.
	for i, h := range cluster.hosts {
		waitForCondition(t, leaderElectionTimeout,
			fmt.Sprintf("node %d agrees on leader ID %d", i, expectedLeaderID),
			func() bool {
				lid, known, getErr := h.GetLeaderID(1)
				return getErr == nil && known && lid == expectedLeaderID
			},
		)
	}
}

// ---------------------------------------------------------------------------
// Leadership stability, transfer, and step-down
// ---------------------------------------------------------------------------
//
// This file used to hold one test: that three nodes agree on a leader ID after
// an election. That is the shallowest thing a leadership suite can assert. It
// says nothing about whether the leader STAYS the leader while it is healthy,
// whether leadership can be moved on request, or whether a replica that has
// lost leadership stops behaving like a leader — and each of those is a defect
// class with real consequences: a cluster that re-elects under its own write
// traffic makes no progress, a transfer that silently does nothing turns every
// rolling restart into an unplanned failover, and a replica that keeps
// answering as leader after stepping down is the front half of split brain.

// leaderView is one node's answer about who leads the shard and in what term.
// The pair is read through Host.GetLeaderIDAndTerm rather than through two
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
		leaderID, term, known, err := h.GetLeaderIDAndTerm(shardID)
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
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// A committed entry in the current term (PhD 6.4) before anything is
	// measured, so the leader is fully established rather than mid-handover.
	proposeCommitted(t, cluster.hosts[leaderIdx], shardID, "stability-warmup", "ok", defaultTimeout)
	baselineLeader, baselineTerm := waitForUnanimousLeader(t, cluster, shardID, leaderElectionTimeout)
	t.Logf("settled: replica %d leads term %d", baselineLeader, baselineTerm)

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
				_, err := cluster.hosts[leaderIdx].SyncPropose(ctx, shardID,
					encodeKVPut(fmt.Sprintf("load-%d-%d", writer, n), "v"))
				cancel()
				if err == nil {
					committed.Add(1)
				}
			}
		}(w)
	}

	deadline := time.Now().Add(loadWindow)
	var samples int
	for time.Now().Before(deadline) {
		for _, v := range observeLeaderViews(cluster, shardID) {
			samples++
			switch {
			case !v.known || v.leaderID == 0:
				close(stop)
				wg.Wait()
				t.Fatalf("node[%d] lost track of the leader while replica %d was "+
					"healthy and leading term %d. A follower that forgets its "+
					"leader has stopped receiving heartbeats and is one election "+
					"timeout away from campaigning against a working leader.",
					v.index, baselineLeader, baselineTerm)
			case v.term != baselineTerm:
				close(stop)
				wg.Wait()
				t.Fatalf("node[%d] reports term %d, but the shard was in term %d "+
					"with replica %d leading and no fault was injected. The term "+
					"only advances when a replica campaigns: the cluster held an "+
					"election under nothing but its own write traffic.",
					v.index, v.term, baselineTerm, baselineLeader)
			case v.leaderID != baselineLeader:
				close(stop)
				wg.Wait()
				t.Fatalf("node[%d] reports replica %d as leader of term %d; "+
					"replica %d led that term. Two replicas cannot lead one term.",
					v.index, v.leaderID, v.term, baselineLeader)
			}
		}
		time.Sleep(samplePause)
	}

	close(stop)
	wg.Wait()

	if n := committed.Load(); n < writeFloor {
		t.Fatalf("only %d writes committed in %v across %d writers (floor %d): "+
			"the leader was never actually put under load, so its stability "+
			"proves nothing", n, loadWindow, numWriters, writeFloor)
	}
	t.Logf("replica %d held term %d across %d observations while committing %d writes",
		baselineLeader, baselineTerm, samples, committed.Load())

	// The data written under load must be on the leader's state machine, so
	// that "stable" cannot mean "wedged but still heartbeating".
	assertSMHasExact(t, trackers[leaderIdx].Get(shardID, uint64(leaderIdx+1)),
		fmt.Sprintf("node[%d]", leaderIdx), "stability-warmup", "ok")
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
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		info, err := c.hosts[targetIdx].GetShardInfo(shardID)
		if err == nil && info.IsLeader {
			return
		}
		if leaderIdx := c.findLeader(shardID); leaderIdx >= 0 {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			lastErr = c.hosts[leaderIdx].RequestLeaderTransfer(ctx, shardID, targetReplica)
			cancel()
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("leadership never moved to replica %d within %v (last RequestLeaderTransfer "+
		"error: %v). A transfer that is accepted and then does nothing turns every "+
		"planned handover — rolling restart, drain, maintenance — into an unplanned "+
		"failover that waits out an election timeout.",
		targetReplica, timeout, lastErr)
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
	info, err := oldLeader.GetShardInfo(shardID)
	if err != nil {
		t.Fatalf("GetShardInfo on the stepped-down replica %d failed: %v", oldLeaderReplica, err)
	}
	if info.IsLeader {
		t.Fatalf("replica %d handed leadership to replica %d and still reports "+
			"itself as leader of shard %d (term %d). Every client that asks it "+
			"will be told to use it, and it will answer for a term another "+
			"replica now owns.", oldLeaderReplica, targetReplica, shardID, info.Term)
	}

	// It must name the replica that took over, and be at least as current.
	leaderID, known, err := oldLeader.GetLeaderID(shardID)
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
