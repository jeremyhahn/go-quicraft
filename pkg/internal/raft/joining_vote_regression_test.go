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
// A joining node that caught up by pure LOG REPLAY must still grant votes
// ---------------------------------------------------------------------------
//
// Config.SnapshotEntries defaults to 0, so the leader never compacts and never
// sends a snapshot. The joining flag is cleared in exactly one place — the
// InstallSnapshot adopt branch in Peer.Handle — so a node started with
// join=true that catches up by replication alone stays joining for the whole
// process lifetime. That is the ORDINARY case, not an exotic one.
//
// These tests are driven through *Peer, not the raw *raft, because the freeze
// that caused the bug lived in Peer.Tick: the raft-level harnesses call
// r.tickFunc directly and never see it.

// newJoiningReplayCaughtUpPeer builds replica 3 of the {1,2,3} shard in exactly
// the state the bug needs: started with join=true, caught up by log replay, and
// currently following leader 1. No snapshot has ever been delivered, so the
// joining flag is still set.
//
// The seeded configuration names ONLY replica 3 — the replica applied the
// AddNode for itself, but the entries naming the rest of the shard were
// compacted away before it arrived and no snapshot has replaced them. That is
// the state in which a replica is still genuinely joining, and it is chosen
// deliberately over the two alternatives:
//
//   - The full {1,2,3} configuration is no longer compatible with joining at
//     all: knowing itself alongside another member ends the bypass (see
//     clearJoiningIfKnown), so a fixture asserting both would be asserting an
//     unreachable state.
//   - An EMPTY configuration would make the fixture VACUOUS. It sets
//     selfRemoved, so startElection returns at its first gate and
//     StillDoesNotCampaign below would pass without ever reaching the joining
//     gate it exists to test.
func newJoiningReplayCaughtUpPeer(t *testing.T) *Peer {
	t.Helper()
	p := newTestPeerWithLog(t, testConfigWithID(1, 3), contiguousEntries(5),
		proto.State{Term: 1, Commit: 5},
		proto.Membership{Addresses: map[uint64]string{3: "addr3"}})
	p.MarkJoining()

	// The leader contacts the joining node, which becomes its follower. This
	// is what arms the leader lease: r.leaderID now names replica 1.
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
		t.Fatalf("fixture: expected the joining node to follow leader 1, got %d",
			p.LeaderID())
	}
	if !p.joining.Load() {
		t.Fatal("fixture: joining flag must still be set (no snapshot was delivered)")
	}
	p.DrainMessages()
	return p
}

// tickPastElectionTimeout advances the peer's clock far enough that a healthy
// follower would have given up on the leader several times over. The joining
// node must reach the same conclusion — the leader is gone — without
// campaigning.
func tickPastElectionTimeout(t *testing.T, p *Peer) {
	t.Helper()
	// randomizedElectionTimeout is drawn from [electionTimeout,
	// 2*electionTimeout), so 4x the base timeout crosses it at least twice
	// whatever the draw was.
	for range 4 * p.r.electionTimeout {
		p.Tick()
	}
	p.DrainMessages()
}

// voteResponse returns the single vote response the peer produced, or fails
// with the interleaving that led to silence.
func voteResponse(t *testing.T, p *Peer, respType uint64, what string) proto.Message {
	t.Helper()
	msgs := p.DrainMessages()
	for i := range msgs {
		if msgs[i].Type == respType {
			return msgs[i]
		}
	}
	t.Fatalf("no %s was produced.\n"+
		"Interleaving: replica 3 was added to shard 1 with join=true and caught up "+
		"by pure log replay, so no snapshot was ever delivered and its joining flag "+
		"is still set. Leader 1 has now died. Replica 2 campaigns and needs replica "+
		"3's vote to reach a quorum of 2 out of 3. Because Peer.Tick refused to "+
		"advance the election clock while joining, r.electionTick is frozen at %d "+
		"while r.leaderID still names the dead leader %d, so the leader lease "+
		"(leaderID != 0 && electionTick < electionTimeout=%d) never expires. "+
		"raft.dropHighTermVote therefore discards %s with no reply at all. "+
		"Shard 1 stays leaderless with two of three voters healthy, forever.",
		what, p.r.electionTick, p.r.leaderID, p.r.electionTimeout, what)
	return proto.Message{}
}

// TestJoiningNode_CaughtUpByReplay_GrantsPreVoteAfterLeaderDies is the PreVote
// half of the availability regression.
func TestJoiningNode_CaughtUpByReplay_GrantsPreVoteAfterLeaderDies(t *testing.T) {
	p := newJoiningReplayCaughtUpPeer(t)
	tickPastElectionTimeout(t, p)

	// Replica 2 campaigns for term 2 with a log at least as current as ours.
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

	resp := voteResponse(t, p, proto.RequestPreVoteResp, "replica 2's RequestPreVote")
	if resp.Reject {
		t.Fatalf("replica 3 DENIED replica 2's RequestPreVote.\n"+
			"Interleaving: replica 3 joined shard 1 with join=true and caught up by "+
			"pure log replay, so no snapshot ever cleared its joining flag. Leader 1 "+
			"is dead and %d ticks have elapsed — more than two full election "+
			"timeouts (electionTimeout=%d) — yet electionTick=%d and leaderID=%d, so "+
			"handleVoteRequest still computes leaderActive=true and denies the "+
			"pre-vote. Replica 2 can never reach 2 of 3 votes and shard 1 stays "+
			"leaderless with a healthy quorum.",
			4*p.r.electionTimeout, p.r.electionTimeout, p.r.electionTick, p.r.leaderID)
	}
}

// TestJoiningNode_CaughtUpByReplay_GrantsHigherTermVoteAfterLeaderDies is the
// real-vote half: dropHighTermVote must no longer swallow the request.
func TestJoiningNode_CaughtUpByReplay_GrantsHigherTermVoteAfterLeaderDies(t *testing.T) {
	p := newJoiningReplayCaughtUpPeer(t)
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

	resp := voteResponse(t, p, proto.RequestVoteResp, "replica 2's RequestVote")
	if resp.Reject {
		t.Fatalf("replica 3 rejected replica 2's higher-term RequestVote "+
			"(electionTick=%d, electionTimeout=%d, leaderID=%d, vote=%d): the "+
			"surviving voters cannot elect a leader without it",
			p.r.electionTick, p.r.electionTimeout, p.r.leaderID, p.r.vote)
	}
	if p.r.vote != 2 {
		t.Fatalf("expected the vote to be recorded for replica 2, got %d", p.r.vote)
	}
}

// TestJoiningNode_CaughtUpByReplay_StillDoesNotCampaign pins the other half of
// the contract. Ticking the election clock must NOT turn the joining node into
// a candidate: its membership can be a one-member view, and a term it won that
// way would acknowledge writes the real cluster discards.
func TestJoiningNode_CaughtUpByReplay_StillDoesNotCampaign(t *testing.T) {
	p := newJoiningReplayCaughtUpPeer(t)
	termBefore := p.Term()

	tickPastElectionTimeout(t, p)

	if p.r.state != follower {
		t.Fatalf("joining node left the follower state (%v): a replica with an "+
			"incomplete membership must never campaign", p.r.state)
	}
	if p.Term() != termBefore {
		t.Fatalf("joining node advanced its term from %d to %d: it campaigned",
			termBefore, p.Term())
	}
	if p.IsLeader() {
		t.Fatal("joining node elected itself on an incomplete membership")
	}
	// And the clock genuinely ran, which is the whole point of the fix.
	if p.r.electionTick == 0 && p.r.leaderID != noLeader {
		t.Fatal("the joining node's election clock never advanced: the leader " +
			"lease can never expire and it will deny every vote")
	}
}

// TestJoiningNode_LocalTimeoutNowStillRefused keeps the raft-layer campaign gate
// honest. Peer.Handle refuses a wire TimeoutNow before it reaches step, so the
// raft gate is only reachable through a locally injected one — which is exactly
// why it must exist independently.
func TestJoiningNode_LocalTimeoutNowStillRefused(t *testing.T) {
	p := newJoiningReplayCaughtUpPeer(t)
	tickPastElectionTimeout(t, p)

	if err := p.r.handleTimeoutNow(proto.Message{
		Type: proto.TimeoutNow,
		From: 1,
		Term: 1,
	}); err != ErrJoiningNodeCannotCampaign {
		t.Fatalf("expected ErrJoiningNodeCannotCampaign from the raft layer, got %v", err)
	}
	if p.r.state != follower {
		t.Fatalf("joining node campaigned on TimeoutNow: state=%v", p.r.state)
	}
}
