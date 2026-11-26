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
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// The Replicate handler validates the sender the same way the Heartbeat and
// InstallSnapshot handlers do: at the current term, only the leader this
// replica already recognises may drive its log.

// TestReplicate_SameTermNonLeaderIsRefused checks that a same-term Replicate
// from a replica other than the recognised leader is refused and changes
// nothing: not the leader, not the election timer, not the log.
func TestReplicate_SameTermNonLeaderIsRefused(t *testing.T) {
	quietLogs(t)

	r := followerHoldingMinorityTail(t, 5)
	if r.leaderID != 2 {
		t.Fatalf("fixture: leaderID = %d, want 2", r.leaderID)
	}
	r.electionTick = 3
	committed, lastIndex := r.log.committed, r.log.lastIndex()

	err := r.step(proto.Message{
		Type: proto.Replicate, From: 3, To: 1, Term: 5,
		LogIndex: 5, LogTerm: 5, Commit: 6,
		Entries: []proto.Entry{{Index: 6, Term: 5, Type: proto.EntryNormal}},
	})
	if !errors.Is(err, ErrReplicateNotFromLeader) {
		t.Fatalf("step returned %v, want ErrReplicateNotFromLeader", err)
	}
	if r.leaderID != 2 {
		t.Fatalf("leaderID = %d, want 2: a refused Replicate must not install "+
			"its sender as leader", r.leaderID)
	}
	if r.electionTick != 3 {
		t.Fatalf("electionTick = %d, want 3: a refused Replicate must not reset "+
			"the election timer", r.electionTick)
	}
	if r.log.committed != committed || r.log.lastIndex() != lastIndex {
		t.Fatalf("log moved: committed %d -> %d, lastIndex %d -> %d",
			committed, r.log.committed, lastIndex, r.log.lastIndex())
	}
	if got := collectMessages(r, proto.ReplicateResp); len(got) != 0 {
		t.Fatalf("ReplicateResp count = %d, want 0", len(got))
	}
}

// TestReplicate_RecognisedLeaderIsAccepted is the positive control: the
// recognised leader's Replicate appends, commits and is acknowledged.
func TestReplicate_RecognisedLeaderIsAccepted(t *testing.T) {
	quietLogs(t)

	r := followerHoldingMinorityTail(t, 5)
	r.electionTick = 3
	if err := r.step(proto.Message{
		Type: proto.Replicate, From: 2, To: 1, Term: 5,
		LogIndex: 5, LogTerm: 5, Commit: 6,
		Entries: []proto.Entry{{Index: 6, Term: 5, Type: proto.EntryNormal}},
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.log.lastIndex() != 6 || r.log.committed != 6 {
		t.Fatalf("lastIndex=%d committed=%d, want 6 and 6",
			r.log.lastIndex(), r.log.committed)
	}
	if r.electionTick != 0 {
		t.Fatalf("electionTick = %d, want 0", r.electionTick)
	}
	got := collectMessages(r, proto.ReplicateResp)
	if len(got) != 1 || got[0].To != 2 || got[0].Reject || got[0].LogIndex != 6 {
		t.Fatalf("ReplicateResp = %+v, want one ack to 2 at index 6", got)
	}
}

// TestReplicate_NoLeaderAcceptsFirstContact checks that a follower with no
// leader for the term accepts the first same-term Replicate and records its
// sender.
func TestReplicate_NoLeaderAcceptsFirstContact(t *testing.T) {
	quietLogs(t)

	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(7, noLeader)
	r.drainMessages()

	if err := r.step(proto.Message{
		Type: proto.Replicate, From: 3, To: 1, Term: 7,
		Entries: []proto.Entry{{Index: 1, Term: 7, Type: proto.EntryNormal}},
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.leaderID != 3 {
		t.Fatalf("leaderID = %d, want 3", r.leaderID)
	}
	if r.log.lastIndex() != 1 {
		t.Fatalf("lastIndex = %d, want 1", r.log.lastIndex())
	}
	if got := collectMessages(r, proto.ReplicateResp); len(got) != 1 || got[0].Reject {
		t.Fatalf("ReplicateResp = %+v, want one ack", got)
	}
}

// TestReplicate_HigherTermSenderIsAdopted checks that a Replicate at a higher
// term still goes through the normal step path: the follower adopts the term
// and the sender as leader.
func TestReplicate_HigherTermSenderIsAdopted(t *testing.T) {
	quietLogs(t)

	r := followerHoldingMinorityTail(t, 5)
	if err := r.step(proto.Message{
		Type: proto.Replicate, From: 3, To: 1, Term: 9,
		LogIndex: 5, LogTerm: 5,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.term != 9 || r.leaderID != 3 {
		t.Fatalf("term=%d leaderID=%d, want 9 and 3", r.term, r.leaderID)
	}
	if got := collectMessages(r, proto.ReplicateResp); len(got) != 1 || got[0].Reject {
		t.Fatalf("ReplicateResp = %+v, want one ack", got)
	}
}
