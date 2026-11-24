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

// One vote per term — the invariant no other test in the suite could fail on.
//
// # What this file exists to catch
//
// Raft's election-safety property (§5.2, §5.4) rests on a single rule: a
// replica grants at most ONE vote per term. Delete that rule — make
// handleVoteRequest's canVote ignore r.vote — and two candidates can each
// collect the same quorum's votes in the same term and both be elected. Every
// safety argument in the protocol collapses at that point, because two leaders
// in one term write different entries at the same (index, term) and Log
// Matching never reconciles them.
//
// That mutation was applied to pkg/internal/raft and the suite's own
// election-safety test — TestLinearizability_4Node_ElectionSafety — PASSED
// three times out of three, with a clean porcupine verdict on ~7000 operations
// each run. It is not a weak test; it is the wrong shape. It kills the leader
// and lets ONE clean election run, and one election never asks a voter for a
// second vote in a term it has already voted in. Nothing in the suite did.
//
// # Why the obvious construction does not work
//
// The natural test is "make two candidates race in one term and assert only
// one wins". It cannot be written reliably, for three separate reasons, and
// each of them is a deliberate defence in this codebase:
//
//   - The CheckQuorum guard (dropHighTermVote) silently discards a higher-term
//     RequestVote while the voter has heard from its leader within an election
//     timeout. A candidate can therefore only be HEARD after the voters' own
//     leases have expired — which is the same instant those voters become
//     willing to campaign themselves.
//   - PreVote refuses to advance a term at all unless the pre-candidate is
//     prospecting ABOVE the voter's term with no leader active, so a second
//     candidate learns the winner's term from the refusal and moves past it.
//   - becomeLeader appends a no-op. One round trip after the first candidate
//     wins, every voter's log is ahead of the loser's, and isUpToDate denies
//     the loser its vote on log freshness alone — which masks the missing
//     one-vote-per-term rule rather than enforcing it.
//
// Between them, the ONLY window where a same-term second vote can even be
// requested is the ~1 RTT between two candidates broadcasting. Racing on it
// produces a flaky test, and a flaky safety test is not a safety test.
//
// # The construction this file uses instead
//
// The observable is inverted. Rather than trying to catch two leaders in one
// term, the test SPENDS a term's votes on a candidate that is then prevented
// from ever winning, and asserts that nobody can subsequently win that term:
//
//  1. Five voters. Leadership is transferred to a designated replica so the
//     roles below are fixed rather than whatever the initial election produced.
//  2. The vote sink (replica 2) is made unable to win before it ever campaigns:
//     the two vote-bearing replicas (3 and 4) have their responses TO it
//     dropped, so their grants never arrive. It can reach at most itself plus
//     the outgoing leader — two of five.
//  3. Leadership is transferred TO the vote sink. A transfer campaigns
//     immediately with Hint == From, which is exactly the case CheckQuorum
//     exempts, so the vote request is heard by every voter even though the
//     leader's lease is fresh. Replicas 1, 3 and 4 adopt term T+1 and spend
//     their T+1 votes on the sink. The sink stays a candidate; nothing is
//     appended anywhere, so every log stays equal.
//  4. The disruptor (replica 5) is partitioned from the sink only, so it never
//     learns that T+1 happened. Its election timer — the only short one in the
//     shard — expires and it campaigns at T+1, the term whose votes are gone.
//
// Under correct code every voter refuses, the disruptor retries at T+2, and
// wins there: liveness is preserved and the test's final assertion proves it.
// With one-vote-per-term removed, the voters grant a second time and the
// disruptor is elected in a term a quorum had already voted away.
//
// Nothing here is timing-dependent. Every fault is installed before the
// election that matters, the only short election timer belongs to the
// disruptor, and the vote sink's own retry is an order of magnitude further
// out than the entire test.
//
// Run with:
//
//	go test -tags 'integration quicraft_netfault' -run ElectionSafety ./test/integration/e2e/...
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

// electionSafetyShardConfig gives every replica its role's election timing.
//
// The disruptor also runs with PreVote disabled, and ONLY the disruptor does.
// Pre-vote is a courtesy the candidate extends to the cluster — it asks whether
// it could win before burning a term — and it is not what enforces one vote per
// term; the voters enforce that, and here they all run the stock configuration.
// Leaving pre-vote on for the disruptor would mean it never reaches the real
// vote at the spent term at all: its pre-vote for T+1 is refused for being AT
// rather than above the voters' term, the refusal teaches it T+1, and its next
// campaign is at T+2 where the votes are unspent and granting them is correct.
// Pre-vote would hide the defect, not prevent it — and it is bypassed by design
// on the leadership-transfer path (see broadcastTransferVote), which is a
// supported production path.
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

// TestElectionSafety_SpentTermCannotBeWonTwice asserts the one-vote-per-term
// rule directly: once a quorum has granted its votes for a term, NOTHING may be
// elected in that term but the candidate those votes were granted to.
//
// The file comment explains the construction and why the obvious "race two
// candidates" test cannot be written reliably. In short: the vote sink spends
// the whole shard's term-T+1 votes and is structurally unable to win with them,
// then the disruptor campaigns in that same T+1. A correct voter refuses and
// the disruptor wins T+2 instead; a voter that has forgotten it already voted
// elects a second leader in a term the cluster had already decided.
//
// This is the test that kills the mutation the rest of the suite could not:
// removing the r.vote test from canVote in handleVoteRequest.
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

	// And a quorum must have HEARD it. A replica only reaches term T+1 here by
	// processing the sink's RequestVote — nothing else in the shard is at that
	// term — so this is the observable proof that their T+1 votes were asked
	// for. They grant, because their logs are identical to the sink's and they
	// have cast no other vote in T+1.
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

	// The disruptor now campaigns at spentTerm, because it never heard that
	// the term existed. Watch every replica until some replica is elected.
	//
	// A leader at spentTerm is the violation and is never waited out: it is
	// not a transient state that resolves, it is a decided term being decided
	// a second time. A leader at a HIGHER term is the correct outcome and ends
	// the test.
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
