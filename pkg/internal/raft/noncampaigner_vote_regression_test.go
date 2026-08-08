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

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// A non-campaigning replica that still counts toward quorum must forget a dead
// leader, or it stalls the election it is a required part of.
// ---------------------------------------------------------------------------
//
// startElection returns early for observers, witnesses, self-removed replicas,
// and role-mismatched replicas — none of them campaign. But tickFollower has
// already zeroed electionTick on the way in, and randomizedElectionTimeout is
// only redrawn on a role transition (which a non-campaigner never makes). So
// electionTick cycles 0..randomizedElectionTimeout forever, and if the branch
// leaves leaderID naming the dead leader, the shared leader-lease test
// (leaderID != noLeader && electionTick < electionTimeout) stays true for most
// of every cycle. handleVoteRequest then denies PreVotes and dropHighTermVote
// silently swallows higher-term RequestVotes.
//
// A witness's vote COUNTS toward the unified quorum, so this is not cosmetic:
// after the only remaining voter's peer dies, the shard needs the witness's
// vote to elect a new leader. This is the same defect the joining fix closed,
// on the other non-campaigning branches.

// newWitnessFollowingLeader builds replica 3 as a WITNESS of the shard whose
// voters are {1,2}, currently following leader 1 with its leader lease armed.
func newWitnessFollowingLeader(t *testing.T) *Peer {
	t.Helper()
	cfg := testConfigWithID(1, 3)
	cfg.IsWitness = true
	ms := proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2"},
		Witnesses: map[uint64]string{3: "addr3"},
	}
	p := newTestPeerWithLog(t, cfg, contiguousEntries(5),
		proto.State{Term: 1, Commit: 5}, ms)

	if err := p.Handle(proto.Message{
		Type:   proto.Heartbeat,
		From:   1,
		To:     3,
		Term:   1,
		Commit: 5,
	}); err != nil {
		t.Fatalf("heartbeat from leader: %v", err)
	}
	if p.LeaderID() != 1 {
		t.Fatalf("fixture: expected the witness to follow leader 1, got %d", p.LeaderID())
	}
	if !p.r.isWitness {
		t.Fatal("fixture: replica 3 must be a witness")
	}
	p.DrainMessages()
	return p
}

// TestWitness_GrantsHigherTermVoteAfterLeaderDies is the regression test: a
// witness must grant the surviving voter's vote once the leader is gone.
func TestWitness_GrantsHigherTermVoteAfterLeaderDies(t *testing.T) {
	p := newWitnessFollowingLeader(t)
	tickPastElectionTimeout(t, p)

	if err := p.Handle(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		To:       3,
		Term:     2,
		LogIndex: 5,
		LogTerm:  1,
	}); err != nil {
		t.Fatalf("RequestVote: %v", err)
	}

	msgs := p.DrainMessages()
	var resp *proto.Message
	for i := range msgs {
		if msgs[i].Type == proto.RequestVoteResp {
			resp = &msgs[i]
			break
		}
	}
	if resp == nil {
		t.Fatalf("the witness produced no RequestVoteResp.\n"+
			"Interleaving: witness 3 followed leader 1 (voters {1,2}, witness {3}). "+
			"Leader 1 died. Voter 2 campaigns at term 2 and needs the witness's vote "+
			"to reach the unified quorum of 2. Because startElection left leaderID=%d "+
			"while tickFollower keeps electionTick low (=%d, timeout=%d), the leader "+
			"lease never expires and dropHighTermVote swallowed the request. The shard "+
			"stays leaderless with a healthy voter and a healthy witness, forever.",
			p.r.leaderID, p.r.electionTick, p.r.electionTimeout)
	}
	if resp.Reject {
		t.Fatalf("witness DENIED voter 2's higher-term RequestVote "+
			"(electionTick=%d, electionTimeout=%d, leaderID=%d): the surviving voter "+
			"cannot elect a leader without the witness's vote",
			p.r.electionTick, p.r.electionTimeout, p.r.leaderID)
	}
	if p.r.vote != 2 {
		t.Fatalf("expected the witness to record its vote for replica 2, got %d", p.r.vote)
	}
}

// TestWitness_GrantsPreVoteAfterLeaderDies is the PreVote half.
func TestWitness_GrantsPreVoteAfterLeaderDies(t *testing.T) {
	p := newWitnessFollowingLeader(t)
	tickPastElectionTimeout(t, p)

	if err := p.Handle(proto.Message{
		Type:     proto.RequestPreVote,
		From:     2,
		To:       3,
		Term:     2,
		LogIndex: 5,
		LogTerm:  1,
	}); err != nil {
		t.Fatalf("RequestPreVote: %v", err)
	}

	msgs := p.DrainMessages()
	var resp *proto.Message
	for i := range msgs {
		if msgs[i].Type == proto.RequestPreVoteResp {
			resp = &msgs[i]
			break
		}
	}
	if resp == nil {
		t.Fatal("the witness produced no RequestPreVoteResp: leaderActive stayed " +
			"true because startElection left the dead leader named")
	}
	if resp.Reject {
		t.Fatalf("witness DENIED voter 2's RequestPreVote (electionTick=%d, "+
			"electionTimeout=%d, leaderID=%d): handleVoteRequest still computes "+
			"leaderActive=true after the leader is gone",
			p.r.electionTick, p.r.electionTimeout, p.r.leaderID)
	}
}

// TestWitness_StillDoesNotCampaign confirms the fix did not turn the witness
// into a candidate. A witness holds no entry payloads and must never lead.
func TestWitness_StillDoesNotCampaign(t *testing.T) {
	p := newWitnessFollowingLeader(t)
	termBefore := p.Term()

	tickPastElectionTimeout(t, p)

	if p.r.state != follower {
		t.Fatalf("witness left the follower state (%v): a witness must never campaign",
			p.r.state)
	}
	if p.Term() != termBefore {
		t.Fatalf("witness advanced its term from %d to %d: it campaigned",
			termBefore, p.Term())
	}
	if p.IsLeader() {
		t.Fatal("witness elected itself")
	}
}
