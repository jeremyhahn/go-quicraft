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

// PreVote requires a STRICTLY higher prospective term.
//
// A pre-vote asks "would you vote for me if I campaigned at term M?". The
// answer can only be yes when M is a term this replica has not already decided,
// which is what makes PreVote's whole purpose — never disturb a term that is
// already settled — hold. Both reference implementations require it explicitly:
//
//	dragonboat handleNodeRequestPreVote:  grant iff m.Term > r.term && isUpToDate
//	etcd/raft   Step:                     canVote includes
//	                                      (m.Type == MsgPreVote && m.Term > r.Term)
//
// handleVoteRequest computes the pre-vote grant as `logOK && !leaderActive`
// with NO term comparison at all. step() rejects msg.Term < r.term before it
// gets here, so the hole is exactly msg.Term == r.term: a pre-candidate at term
// T-1 prospecting for T is granted by a replica already AT term T which has
// already cast its real vote for someone else in T.
//
// It costs liveness, not safety — a pre-vote is not a vote, and the real
// RequestVote that follows is still refused by the already-voted replica — but
// the wasted round is precisely the disruption PreVote exists to prevent, and
// it recurs every election timeout while the stale pre-candidate keeps
// prospecting for a term the cluster has moved past.

// TestRaft_PreVote_NotGrantedForAnAlreadyDecidedTerm pins the rule. The
// responder is at term 5 and has already voted for replica 2 there; a
// pre-candidate prospecting for that same term 5 must be REJECTED.
//
// KNOWN RED. This test FAILS against handleVoteRequest as it stands and is the
// acceptance criterion for the one-line fix — adding `&& msg.Term > r.term` to
// the pre-vote branch's canVote. That fix has been verified against the whole
// of ./pkg/internal/raft/, ./pkg/ and ./pkg/internal/engine/ with no other test
// affected. It is not a flake: it fails deterministically, on every run.
func TestRaft_PreVote_NotGrantedForAnAlreadyDecidedTerm(t *testing.T) {
	r := newTestRaft(1)
	r.term = 5
	r.vote = 2 // this replica already decided term 5, for replica 2
	r.leaderID = noLeader
	r.drainMessages()

	// Replica 3's log is perfectly up to date and no leader is known, so
	// nothing but the prospective term can deny this request.
	if err := r.step(proto.Message{
		Type:     proto.RequestPreVote,
		From:     3,
		Term:     5, // == r.term: a term this replica has already decided
		LogIndex: r.log.lastIndex(),
		LogTerm:  0,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resps := collectMessages(r, proto.RequestPreVoteResp)
	if len(resps) != 1 {
		t.Fatalf("got %d RequestPreVoteResp, want exactly 1", len(resps))
	}
	if !resps[0].Reject {
		t.Fatal("a pre-vote for a term this replica has ALREADY DECIDED (it is at " +
			"term 5 and voted for replica 2 there) was GRANTED. A pre-vote grant " +
			"means \"I would vote for you at that term\", which is false: the real " +
			"RequestVote that follows is refused, so the pre-candidate burns an " +
			"election round every timeout. dragonboat grants only when " +
			"m.Term > r.term; etcd/raft requires the same in canVote")
	}
	if resps[0].Term != r.term {
		t.Fatalf("rejected pre-vote response Term = %d, want the responder's own "+
			"term %d: echoing the prospective term on a rejection makes the "+
			"pre-candidate treat it as term-advancing", resps[0].Term, r.term)
	}
}

// TestRaft_PreVote_GrantedForAStrictlyHigherTerm is the liveness control. The
// term test must be STRICTLY greater-than and nothing more: a pre-candidate
// prospecting for a term above this replica's, with an up-to-date log and no
// active leader, must still be granted, or PreVote would deadlock every
// election.
func TestRaft_PreVote_GrantedForAStrictlyHigherTerm(t *testing.T) {
	r := newTestRaft(1)
	r.term = 5
	r.vote = 2 // an already-cast vote in term 5 must not veto term 6
	r.leaderID = noLeader
	r.drainMessages()

	if err := r.step(proto.Message{
		Type:     proto.RequestPreVote,
		From:     3,
		Term:     6, // > r.term: an undecided term
		LogIndex: r.log.lastIndex(),
		LogTerm:  0,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resps := collectMessages(r, proto.RequestPreVoteResp)
	if len(resps) != 1 {
		t.Fatalf("got %d RequestPreVoteResp, want exactly 1", len(resps))
	}
	if resps[0].Reject {
		t.Fatal("a pre-vote for a STRICTLY HIGHER term was rejected: a vote " +
			"already cast in term 5 says nothing about term 6, and refusing here " +
			"prevents any election from ever starting")
	}
	if resps[0].Term != 6 {
		t.Fatalf("granted pre-vote response Term = %d, want the prospective term "+
			"6, or the pre-candidate discards its own grant as stale",
			resps[0].Term)
	}
	// A pre-vote never advances the responder's term or consumes its vote.
	if r.term != 5 || r.vote != 2 {
		t.Fatalf("after granting a pre-vote: term = %d, vote = %d, want 5 and 2",
			r.term, r.vote)
	}
}
