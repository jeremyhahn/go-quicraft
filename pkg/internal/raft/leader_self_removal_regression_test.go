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

package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Removing the CURRENT LEADER.
//
// Raft §6: "the leader steps down once Cnew is committed if it is not part of
// the new configuration." Three production sites implement it and each one is
// load-bearing on its own:
//
//	Peer.checkLeaderSelfRemoval  the step-down itself, at commit time
//	raft.startElection           the selfRemoved bar on the timeout path
//	raft.handleTimeoutNow        the selfRemoved bar on the transfer path,
//	                             which bypasses startElection entirely
//
// Every multi-node membership test removes a FOLLOWER, so the leader case was
// exercised only by the single-Peer path. The failure it guards is a two-leader
// term: a removed replica's remotes are rebuilt from the surviving
// configuration, so it computes quorum over a voting set it is not in, counts
// its own vote, and can declare itself leader for a term the real configuration
// is simultaneously electing in.
//
// leaderPeerRemovingItself drives the whole production path — propose, replicate,
// commit — rather than poking membership directly, so the commit-time hook is
// the thing under test.
func leaderPeerRemovingItself(t *testing.T) *Peer {
	t.Helper()
	p := newTestMultiNodeLeaderPeer(t)
	if !p.IsLeader() {
		t.Fatal("precondition: replica 1 must start as leader")
	}

	if err := p.ProposeConfigChange(proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: p.replicaID,
	}); err != nil {
		t.Fatalf("self-removal proposal: %v", err)
	}
	// Replica 2's ack completes the durable quorum, so the removal commits.
	mustHandle(t, p, proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     p.r.term,
		LogIndex: p.r.log.lastIndex(),
	})
	runUpdateCycles(t, p)

	if p.membership.isMember(p.replicaID) {
		t.Fatal("precondition: the self-removal did not commit")
	}
	return p
}

// TestPeer_LeaderSelfRemoval_StepsDownOnCommit pins the step-down itself. It is
// the commit of Cnew that must demote the leader, before it can serve another
// read or replicate another entry from a configuration it has left.
func TestPeer_LeaderSelfRemoval_StepsDownOnCommit(t *testing.T) {
	p := leaderPeerRemovingItself(t)

	if p.IsLeader() {
		t.Fatal("the leader is STILL leader after its own removal committed: " +
			"Raft §6 requires it to step down once Cnew commits, and a leader " +
			"outside the configuration replicates to and computes quorum over a " +
			"voting set that no longer contains it")
	}
	if p.r.state != follower {
		t.Fatalf("state after self-removal = %s, want follower", p.r.state)
	}
	if p.LeaderID() != noLeader {
		t.Fatalf("leaderID after stepping down = %d, want noLeader: the removed "+
			"replica must not keep advertising itself as the leader", p.LeaderID())
	}
	if !p.r.selfRemoved {
		t.Fatal("selfRemoved was not synced from the committed membership")
	}
}

// TestPeer_LeaderSelfRemoval_CannotWinASubsequentElection is the safety half:
// after the step-down the replica must never come BACK as leader. It exercises
// both campaign entry points — the election timeout and TimeoutNow, which
// bypasses startElection — and then hands the replica a full set of vote grants
// to prove that even a cooperative (or malicious) peer cannot resurrect it.
func TestPeer_LeaderSelfRemoval_CannotWinASubsequentElection(t *testing.T) {
	p := leaderPeerRemovingItself(t)
	termAfterRemoval := p.r.term

	// 1. The election-timeout path. Ticking well past several timeouts must not
	//    start a campaign: a campaign increments the term, which is the
	//    observable signature of one.
	p.r.drainMessages()
	for range 4 * p.r.electionTimeout {
		p.Tick()
	}
	if p.r.term != termAfterRemoval {
		t.Fatalf("term advanced from %d to %d on the election timeout: a removed "+
			"replica campaigned for a configuration it is not in",
			termAfterRemoval, p.r.term)
	}
	if votes := collectMessages(p.r, proto.RequestVote); len(votes) != 0 {
		t.Fatalf("a removed replica sent %d RequestVote messages", len(votes))
	}
	if pre := collectMessages(p.r, proto.RequestPreVote); len(pre) != 0 {
		t.Fatalf("a removed replica sent %d RequestPreVote messages", len(pre))
	}

	// 2. The TimeoutNow path. A leadership transfer that was already in flight
	//    when the removal committed campaigns IMMEDIATELY and skips
	//    startElection, so the bar has to be repeated in handleTimeoutNow.
	p.r.drainMessages()
	mustHandle(t, p, proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: p.r.term,
	})
	if p.r.term != termAfterRemoval {
		t.Fatalf("term advanced from %d to %d on TimeoutNow: a removed replica "+
			"campaigned because it was told to, handing leadership to a replica "+
			"the configuration no longer contains",
			termAfterRemoval, p.r.term)
	}
	if p.r.state != follower {
		t.Fatalf("state after TimeoutNow = %s, want follower", p.r.state)
	}
	if votes := collectMessages(p.r, proto.RequestVote); len(votes) != 0 {
		t.Fatalf("a removed replica sent %d RequestVote messages after TimeoutNow",
			len(votes))
	}

	// 3. Vote grants from every surviving member, at the term a campaign would
	//    have used. Nothing may promote a replica that never legitimately
	//    became a candidate.
	for _, from := range []uint64{2, 3} {
		mustHandle(t, p, proto.Message{
			Type: proto.RequestVoteResp,
			From: from,
			Term: termAfterRemoval,
		})
	}
	if p.IsLeader() {
		t.Fatal("a removed replica became leader on unsolicited vote grants: it " +
			"computes quorum over the surviving configuration it is not part of, " +
			"so it can hold the same term as the configuration's real leader")
	}
	if p.r.state != follower {
		t.Fatalf("state after vote grants = %s, want follower", p.r.state)
	}
}

// TestPeer_RemovedFollower_ButNotTheLeader_KeepsCampaigning is the liveness
// control for the two tests above: the bars must key on THIS replica's removal,
// not on any committed RemoveNode. A leader that removes a FOLLOWER keeps its
// leadership, and a replica still in the configuration keeps campaigning — an
// over-broad bar would silently disable a healthy voter.
func TestPeer_RemovedFollower_ButNotTheLeader_KeepsCampaigning(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	if err := p.ProposeConfigChange(proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 3,
	}); err != nil {
		t.Fatalf("follower-removal proposal: %v", err)
	}
	mustHandle(t, p, proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     p.r.term,
		LogIndex: p.r.log.lastIndex(),
	})
	runUpdateCycles(t, p)

	if p.membership.isMember(3) {
		t.Fatal("precondition: the removal of follower 3 did not commit")
	}
	if !p.IsLeader() {
		t.Fatal("the leader stepped down after removing a DIFFERENT replica: the " +
			"step-down must be conditional on the leader's own membership")
	}
	if p.r.selfRemoved {
		t.Fatal("selfRemoved set after removing a different replica")
	}

	// And a replica that is still a member must still be able to campaign.
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	q := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	startTerm := q.r.term
	for range 4 * q.r.electionTimeout {
		q.Tick()
	}
	if q.r.term == startTerm {
		t.Fatal("a replica inside the configuration never campaigned: the " +
			"self-removal bar is disabling healthy voters")
	}
}
