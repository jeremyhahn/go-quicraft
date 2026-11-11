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

// Persisted-vote recovery.
//
// votedFor is one of the three fields Raft Figure 2 requires to be durable, and
// the ONLY reason it is durable is so a replica that restarts inside a term
// cannot vote a SECOND time in that term. Every link of the persistence chain
// was already asserted — hardState() emits the vote, LogDB stores it, ReadState
// returns it — but the single line where the recovered vote becomes a DECISION
// (newRaft's `r.vote = state.Vote`, which handleVoteRequest then reads) was not.
// Mutating that line to noNode left the whole suite green, which is exactly the
// textbook route to two leaders in one term:
//
//	replica 1 votes for 2 in term 5 and persists it -> 1 crashes and restarts,
//	forgetting the vote -> 3 campaigns in term 5 -> 1 grants -> both 2 and 3
//	can now assemble a majority for term 5.
//
// newRaft restores the vote and then calls becomeFollower(r.term, noLeader) with
// the SAME term it just restored, and becomeFollower clears the vote only on
// term ADVANCEMENT (Figure 2), so the vote survives. This mirrors dragonboat,
// where loadState sets r.vote = st.Vote and reset() clears it only when
// r.term != term.

// restartedPeerWithVote rebuilds a replica from durable state alone: term 5, a
// vote already cast for replica 2, and a three-entry committed log. It is the
// state a replica holds after it crashed between granting a vote and hearing
// the outcome.
func restartedPeerWithVote(t *testing.T, votedFor uint64) *Peer {
	t.Helper()
	entries := []proto.Entry{
		{Index: 1, Term: 5},
		{Index: 2, Term: 5},
		{Index: 3, Term: 5},
	}
	lr := newTestLogReader(entries, proto.State{Term: 5, Vote: votedFor, Commit: 3})
	lr.membership = threeNodeMembership()
	p, err := NewPeer(testConfig(), lr, nil)
	if err != nil {
		t.Fatalf("failed to restart peer from persisted state: %v", err)
	}
	return p
}

// TestPeer_RestartedReplicaDeniesSecondCandidateInSameTerm is the required
// double-vote test. A replica that voted for 2 in term 5 and restarted must
// REJECT replica 3's RequestVote for the same term 5, even though 3's log is
// perfectly up to date and no leader is known.
//
// The positive control below proves the rejection is the persisted vote talking
// and not a blanket refusal: a REPEAT request from replica 2 — the replica the
// recovered vote names — is granted, which is what makes a lost vote response
// recoverable.
func TestPeer_RestartedReplicaDeniesSecondCandidateInSameTerm(t *testing.T) {
	p := restartedPeerWithVote(t, 2)
	p.r.drainMessages()

	// Preconditions: no leader is known after a restart, so nothing but the
	// recovered vote can deny this request.
	if p.LeaderID() != noLeader {
		t.Fatalf("precondition: leaderID = %d, want noLeader after restart",
			p.LeaderID())
	}
	if p.r.state != follower {
		t.Fatalf("precondition: state = %s, want follower after restart", p.r.state)
	}

	// Replica 3 campaigns in the SAME term 5, with a log identical to ours.
	mustHandle(t, p, proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     5,
		LogIndex: 3,
		LogTerm:  5,
	})

	resps := collectMessages(p.r, proto.RequestVoteResp)
	if len(resps) != 1 {
		t.Fatalf("got %d RequestVoteResp for the second candidate, want exactly 1",
			len(resps))
	}
	if !resps[0].Reject {
		t.Fatal("a replica that restarted after voting for 2 in term 5 GRANTED a " +
			"second vote in term 5 to replica 3: the persisted votedFor was not " +
			"restored, so 2 and 3 can both assemble a majority for term 5 — two " +
			"leaders in one term")
	}
	if p.r.vote != 2 {
		t.Fatalf("vote after the denied request = %d, want 2 (unchanged)", p.r.vote)
	}

	// Positive control: the SAME term, the SAME log, but from the candidate the
	// recovered vote names. A repeat RequestVote must be granted, or a lost vote
	// response would strand an election that this replica already decided.
	p.r.drainMessages()
	mustHandle(t, p, proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     5,
		LogIndex: 3,
		LogTerm:  5,
	})
	repeats := collectMessages(p.r, proto.RequestVoteResp)
	if len(repeats) != 1 {
		t.Fatalf("got %d RequestVoteResp for the repeat request, want exactly 1",
			len(repeats))
	}
	if repeats[0].Reject {
		t.Fatal("the repeat RequestVote from replica 2 — the candidate the " +
			"persisted vote names — was REJECTED: a re-sent vote request must be " +
			"idempotent or a lost response stalls the election for a full timeout")
	}
}
