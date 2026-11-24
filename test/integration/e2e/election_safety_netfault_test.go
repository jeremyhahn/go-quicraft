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

//go:build integration && quicraft_netfault

// Election safety: a vote sink spends a term's votes it cannot win with, and a disruptor that never saw that term must not win it. Run with go test -tags 'integration quicraft_netfault' -run ElectionSafety ./test/integration/e2e/...
package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/netfault"
)

// Replica roles for TestElectionSafety_SpentTermCannotBeWonTwice. They are
// replica IDs, not cluster indices; replicaOf(idx) == idx+1 maps between them.
const (
	// esOutgoingLeader holds leadership until it hands it to the vote sink.
	esOutgoingLeader uint64 = 1
	// esVoteSink campaigns exactly once, collects the term's votes, and is
	// structurally unable to win with them.
	esVoteSink uint64 = 2
	// esVoterA and esVoterB are the replicas whose votes decide the term.
	// They are the ones the invariant is asserted about.
	esVoterA uint64 = 3
	esVoterB uint64 = 4
	// esDisruptor campaigns in the term the vote sink already spent.
	esDisruptor uint64 = 5
)

// Election timing, in RTT multiples. RTTMillisecond is 50 in testHostConfig,
// and a replica campaigns after a randomised electionTimeout in
// [ElectionRTT, 2*ElectionRTT) ticks.
const (
	// esSlowElectionRTT is 5-10s. Every replica except the disruptor uses it,
	// which is what makes the sequence deterministic: no other replica's
	// election timer can fire inside the window this test observes, so the
	// only campaigns that happen are the transfer-triggered one and the
	// disruptor's.
	esSlowElectionRTT uint64 = 100
	// esFastElectionRTT is 1-2s: the disruptor campaigns promptly once the
	// outgoing leader stops heartbeating, and retries promptly if refused.
	esFastElectionRTT uint64 = 20
)

// electionSafetyShardConfig gives each replica its role's election timing; only the disruptor runs without PreVote.
func electionSafetyShardConfig(shardID, replicaID uint64) config.Config {
	c := config.Config{
		ShardID:     shardID,
		ReplicaID:   replicaID,
		ElectionRTT: esSlowElectionRTT,
	}
	if replicaID == esDisruptor {
		c.ElectionRTT = esFastElectionRTT
		c.PreVote = config.BoolPtr(false)
	}
	c.SetDefaults()
	return c
}

// indexOf returns the cluster index of a replica ID. It is the inverse of
// replicaOf and exists so the role constants above can be used directly
// against helpers that take an index.
func indexOf(replicaID uint64) int { return int(replicaID) - 1 }

// termAt returns the term the node at idx currently reports for the shard.
// ok is false when the node cannot answer at all.
func termAt(c *testCluster, idx int, shardID uint64) (term uint64, isLeader, ok bool) {
	c.hostsMu.RLock()
	h := c.hosts[idx]
	c.hostsMu.RUnlock()
	info, err := h.ShardInfo(shardID)
	if err != nil {
		return 0, false, false
	}
	return info.Term, info.IsLeader, true
}

// transferLeadershipTo moves leadership to targetReplica and waits for the
// target to report it. It retries the request itself: RequestLeaderTransfer is
// fire-and-forget and is refused outright while another transfer is pending, so
// a single call is not a reliable way to ask for a specific leader.
func transferLeadershipTo(t *testing.T, cluster *testCluster, shardID, targetReplica uint64, timeout time.Duration) {
	t.Helper()

	targetIdx := indexOf(targetReplica)
	var lastErr error
	waitForConditionDiag(t, timeout,
		fmt.Sprintf("leadership moves to replica %d", targetReplica), func() bool {
			if isLeaderAt(cluster, targetIdx, shardID) {
				return true
			}
			if h := cluster.leaderHost(shardID); h != nil {
				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				lastErr = h.RequestLeaderTransfer(ctx, shardID, targetReplica)
				cancel()
			}
			return false
		}, func() string { return fmt.Sprintf("last transfer error: %v", lastErr) })
	t.Logf("replica %d holds leadership of shard %d", targetReplica, shardID)
}

// TestElectionSafety_SpentTermCannotBeWonTwice verifies no replica wins a term whose votes a quorum already granted to another candidate.
func TestElectionSafety_SpentTermCannotBeWonTwice(t *testing.T) {
	const shardID uint64 = 1

	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShardWithConfig(shardID, electionSafetyShardConfig)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// Pin leadership to a known replica. The bootstrap election is won by the
	// disruptor — it is the only replica with a short election timer — and the
	// roles below only hold if the outgoing leader is replica 1.
	transferLeadershipTo(t, cluster, shardID, esOutgoingLeader, failoverTimeout)

	// Every log must be identical before the faults go in. isUpToDate is
	// consulted on every vote, so a replica that is even one entry behind is
	// refused for a reason that has nothing to do with the invariant under
	// test — which would make this test pass against the mutation for the
	// wrong reason.
	proposeAndWait(t, cluster, trackers, shardID, "election-safety", "warmup")
	waitForReplication(t, cluster, trackers, shardID, "election-safety", "warmup", defaultTimeout)

	baseTerm, _, ok := termAt(cluster, indexOf(esOutgoingLeader), shardID)
	if !ok {
		t.Fatal("could not read the outgoing leader's term")
	}
	spentTerm := baseTerm + 1
	t.Logf("leader replica %d at term %d; the term under test is %d",
		esOutgoingLeader, baseTerm, spentTerm)

	// Fault 1: the vote sink can never be told that it won. Its vote requests
	// still arrive at the two vote-bearing replicas — this cut is one
	// direction only — but their grants are dropped, so it can reach at most
	// two of five and stays a candidate.
	netfault.BlockLink(esVoterA, esVoteSink)
	netfault.BlockLink(esVoterB, esVoteSink)

	// Fault 2: the disruptor and the vote sink cannot see each other at all,
	// so the disruptor never learns that term T+1 was ever started. That is
	// what makes its first campaign land on the spent term rather than a
	// fresh one.
	netfault.Partition([]uint64{esVoteSink}, []uint64{esDisruptor})

	requireFaultInjection(t, 4)

	// Hand leadership to the vote sink. A transfer campaigns immediately with
	// Hint == From, which CheckQuorum exempts from the higher-term-vote drop,
	// so the request is heard even though the outgoing leader's lease is still
	// fresh on every voter. This is the only way to make a specific replica
	// campaign at a specific moment without racing an election timer.
	leaderHost := cluster.leaderHost(shardID)
	if leaderHost == nil {
		t.Fatal("the shard lost its leader before the transfer could be requested")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	err := leaderHost.RequestLeaderTransfer(ctx, shardID, esVoteSink)
	cancel()
	if err != nil {
		t.Fatalf("RequestLeaderTransfer(replica=%d) failed: %v", esVoteSink, err)
	}

	// The sink must actually be campaigning at the term under test. If it is
	// not, no votes were spent and everything below would pass vacuously.
	waitForCondition(t, failoverTimeout,
		"the vote sink campaigns at the term under test", func() bool {
			term, isLeader, ok := termAt(cluster, indexOf(esVoteSink), shardID)
			return ok && term == spentTerm && !isLeader
		})

	// A quorum must have reached the spent term, which only the sink's RequestVote carries.
	for _, voter := range []uint64{esVoterA, esVoterB, esOutgoingLeader} {
		replica := voter
		waitForCondition(t, failoverTimeout,
			"a voter spends its vote for the term under test", func() bool {
				term, _, ok := termAt(cluster, indexOf(replica), shardID)
				return ok && term == spentTerm
			})
	}
	t.Logf("replicas %d, %d and %d have spent their term-%d votes on replica %d, "+
		"which cannot win with them", esVoterA, esVoterB, esOutgoingLeader, spentTerm, esVoteSink)

	// Watch until someone is elected: a leader at spentTerm is the violation, a higher term is correct.
	var electedTerm, electedReplica uint64
	waitForCondition(t, failoverTimeout,
		"a replica is elected in a term above the spent one (the shard should "+
			"recover by electing in the NEXT term, and a test that never sees "+
			"that has not exercised the refusal it asserts)", func() bool {
			for _, obs := range cluster.observeLeaders(shardID) {
				switch {
				case obs.term == spentTerm:
					t.Fatalf("node[%d] (replica %d) was elected leader in term %d, "+
						"a term in which replicas %d, %d and %d had already granted "+
						"their votes to replica %d. A voter granted a second vote in "+
						"one term: the election-safety property no longer holds and "+
						"two leaders can now write different entries at the same "+
						"(index, term).",
						obs.index, obs.replicaID, obs.term,
						esVoterA, esVoterB, esOutgoingLeader, esVoteSink)
				case obs.term > spentTerm:
					electedTerm, electedReplica = obs.term, obs.replicaID
				}
			}
			return electedTerm != 0
		})
	t.Logf("replica %d was elected in term %d — the term after the spent one, "+
		"which is the only correct outcome", electedReplica, electedTerm)

	// Heal and confirm the shard is genuinely healthy afterwards rather than
	// merely quiet: one leader, and a proposal that commits.
	cluster.healNetwork()
	waitForSingleLeader(t, cluster, shardID, failoverTimeout)
	if err := proposeUntilCommitted(cluster, shardID, "post-election-safety", "ok",
		defaultTimeout); err != nil {
		t.Fatalf("the shard could not commit after the partition healed: %v", err)
	}
}
