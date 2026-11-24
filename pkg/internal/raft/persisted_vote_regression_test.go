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

// Persisted-vote recovery: a restarted replica keeps its vote for the term, so it cannot vote twice in one term.

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

// TestPeer_RestartedReplicaDeniesSecondCandidateInSameTerm checks a restarted replica refuses another candidate in its voted term but re-grants the one it voted for.
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

	// Replica 3 campaigns in term 5 with an identical log.
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

	// Positive control: a repeat request from the voted-for candidate is granted.
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
