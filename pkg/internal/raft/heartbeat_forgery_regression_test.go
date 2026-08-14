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

// Forged-heartbeat commit advance
//
// handleHeartbeat used to be four lines with no proof of anything:
//
//	r.electionTick = 0
//	r.leaderID = msg.From
//	if msg.Commit > r.log.committed { commitTo(min(msg.Commit, lastIndex)) }
//
// A Heartbeat carries no prevLogIndex, no prevLogTerm and no entries, so nothing
// in it establishes that the sender's log matches this replica's — yet it
// committed straight up to lastIndex. The attack is one frame: a follower
// holding entries 501..1000 that replicated to a MINORITY receives
// Heartbeat{From: leader, Term: T+1, Commit: 1<<40}, step() adopts the higher
// term, and the whole uncommitted tail is committed and handed to the state
// machine. A legitimate leader elected from the majority that never saw those
// entries then truncates them, and this replica has applied — and will serve
// reads from — entries that exist in no log.
//
// Two gates close it, and both are exercised below: the sender must be the
// leader this replica already recognises for the term, and the advance goes
// through raftLog.tryCommit with the CURRENT term, so it can only reach an index
// whose entry this replica log-matched in itself.

// followerHoldingMinorityTail builds a follower at term T with committed=1 and
// an uncommitted tail 2..5 from term T. It is the shape the exploit needs: real
// entries this replica holds that a new leader may still truncate.
func followerHoldingMinorityTail(t *testing.T, term uint64) *raft {
	t.Helper()
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(term, 2)
	entries := make([]proto.Entry, 0, 5)
	for i := uint64(1); i <= 5; i++ {
		entries = append(entries, proto.Entry{Index: i, Term: term, Type: proto.EntryNormal})
	}
	r.log.append(entries)
	r.log.commitTo(1)
	if r.log.committed != 1 || r.log.lastIndex() != 5 {
		t.Fatalf("fixture: committed=%d lastIndex=%d, want 1 and 5",
			r.log.committed, r.log.lastIndex())
	}
	r.drainMessages()
	return r
}

// TestHeartbeat_ForgedHigherTermCannotCommitTheUncommittedTail is the attacker
// test for the term half of the gate. The forger names a term ABOVE this
// replica's, which step() adopts (that part is correct Raft — a higher term is
// authoritative), and a Commit index far past the log. The commit index must not
// move: every entry this replica holds is from the OLD term, which is precisely
// the tail a leader of the new term is free to truncate.
func TestHeartbeat_ForgedHigherTermCannotCommitTheUncommittedTail(t *testing.T) {
	quietLogs(t)

	r := followerHoldingMinorityTail(t, 5)
	before := r.log.committed

	if err := r.step(proto.Message{
		Type: proto.Heartbeat, From: 2, To: 1, Term: 6, Commit: 1 << 40,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}

	if r.log.committed != before {
		t.Fatalf("committed advanced %d -> %d on a heartbeat carrying no "+
			"log-matching proof, over entries from term %d that a leader of term "+
			"%d has never seen: those entries reach the state machine and are then "+
			"truncated away, which is a State Machine Safety violation",
			before, r.log.committed, 5, r.term)
	}
	if r.term != 6 {
		t.Fatalf("term = %d, want 6: adopting a higher term is correct Raft and "+
			"must not have been broken by the commit gate", r.term)
	}
}

// TestHeartbeat_SameTermNonLeaderIsRefused is the attacker test for the sender
// half. The replica already recognises replica 2 as the leader of term 5.
// Replica 3 sends a Heartbeat at the SAME term with a Commit index past the
// tail — so the current-term rule alone would happily commit it, because the
// tail IS from term 5. Only the sender check refuses this one.
func TestHeartbeat_SameTermNonLeaderIsRefused(t *testing.T) {
	quietLogs(t)

	r := followerHoldingMinorityTail(t, 5)
	if r.leaderID != 2 {
		t.Fatalf("fixture: leaderID = %d, want 2", r.leaderID)
	}
	before := r.log.committed

	err := r.step(proto.Message{
		Type: proto.Heartbeat, From: 3, To: 1, Term: 5, Commit: 1 << 40,
	})
	if !errors.Is(err, ErrHeartbeatNotFromLeader) {
		t.Fatalf("step returned %v, want ErrHeartbeatNotFromLeader", err)
	}
	if r.log.committed != before {
		t.Fatalf("committed advanced %d -> %d on a heartbeat from replica 3 while "+
			"replica 2 is this term's leader: Election Safety says there is one "+
			"leader per term, so the second claimant is a forgery or a stale "+
			"duplicate and must move nothing", before, r.log.committed)
	}
	if r.leaderID != 2 {
		t.Fatalf("leaderID = %d, want 2: a refused heartbeat must not install its "+
			"sender as leader", r.leaderID)
	}
}

// TestHeartbeat_LegitimateLeaderStillCommits is the POSITIVE CONTROL, and it is
// what makes the two tests above mean something. The recognised leader of the
// CURRENT term sends an ordinary heartbeat and the commit index must advance
// exactly as before — otherwise the gate has simply broken commit propagation
// and every shard would stall one heartbeat short forever.
func TestHeartbeat_LegitimateLeaderStillCommits(t *testing.T) {
	quietLogs(t)

	r := followerHoldingMinorityTail(t, 5)
	if err := r.step(proto.Message{
		Type: proto.Heartbeat, From: 2, To: 1, Term: 5, Commit: 4,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.log.committed != 4 {
		t.Fatalf("committed = %d, want 4: the recognised leader's heartbeat must "+
			"still advance the commit index, or commit propagation stalls",
			r.log.committed)
	}
	if r.electionTick != 0 {
		t.Fatalf("electionTick = %d, want 0: a legitimate heartbeat must still "+
			"reset the election timer", r.electionTick)
	}
	// And the echo the leader correlates ReadIndex against must still be sent.
	if got := collectMessages(r, proto.HeartbeatResp); len(got) != 1 {
		t.Fatalf("HeartbeatResp count = %d, want 1", len(got))
	}
}

// TestHeartbeat_FirstContactAfterElectionStillInstallsTheLeader is the second
// liveness control. A follower with NO leader for the term must accept the first
// heartbeat it sees — the sender gate is `leaderID != noLeader && From !=
// leaderID`, and getting that conjunct wrong would leave a freshly elected
// leader unable to establish itself on any follower.
func TestHeartbeat_FirstContactAfterElectionStillInstallsTheLeader(t *testing.T) {
	quietLogs(t)

	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(7, noLeader)
	r.drainMessages()

	if err := r.step(proto.Message{
		Type: proto.Heartbeat, From: 3, To: 1, Term: 7,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.leaderID != 3 {
		t.Fatalf("leaderID = %d, want 3: a follower with no leader for the term "+
			"must accept first contact, or a newly elected leader can never "+
			"establish itself", r.leaderID)
	}
}

// TestHeartbeat_HigherTermLeaderStillInstallsItself is the third liveness
// control: the sender gate must not fire when step() has ALREADY installed the
// sender as leader for a higher term. It reads r.leaderID after that transition,
// so the two must agree.
func TestHeartbeat_HigherTermLeaderStillInstallsItself(t *testing.T) {
	quietLogs(t)

	r := followerHoldingMinorityTail(t, 5)
	if err := r.step(proto.Message{
		Type: proto.Heartbeat, From: 3, To: 1, Term: 9,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.term != 9 || r.leaderID != 3 {
		t.Fatalf("term=%d leaderID=%d, want 9 and 3: a leader at a higher term is "+
			"authoritative and must still be adopted", r.term, r.leaderID)
	}
}
