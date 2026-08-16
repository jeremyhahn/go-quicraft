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

// InstallSnapshot from a replica that is not this term's leader
//
// handleSnapshot opened with the same two unconditional lines handleHeartbeat
// used to:
//
//	r.electionTick = 0
//	r.leaderID = msg.From
//
// The heartbeat path was gated on sender identity (ErrHeartbeatNotFromLeader)
// precisely because a Heartbeat proves nothing about the sender's log. An
// InstallSnapshot proves strictly less and does strictly more: no prevLogIndex,
// no prevLogTerm, no entries, and its non-matching branch REPLACES the entire
// log and rebuilds the voting set from a Membership the sender supplied. The
// gate was never generalised to it.
//
// Two separate capabilities fall out of that, and both are exercised below.
//
//  1. ELECTION SUPPRESSION, available even on the REJECT path. The two lines run
//     before the ss.Index <= committed rejection, so retransmitting one obsolete
//     snapshot holds a healthy follower's election timer at zero AND installs the
//     sender as its leaderID — which makes handleVoteRequest's leader-lease test
//     deny PreVotes and dropHighTermVote discard higher-term RequestVotes. A
//     replica that cannot be made to campaign or to vote is removed from every
//     quorum without ever being removed from the configuration.
//
//  2. LOG REPLACEMENT on the accept path, from a sender Election Safety says
//     cannot be this term's leader.

// followerWithEstablishedLeader builds a follower at term 5 that already
// recognises replica 2 as this term's leader and holds committed entries 1..5.
func followerWithEstablishedLeader(t *testing.T) *raft {
	t.Helper()
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(5, 2)
	entries := make([]proto.Entry, 0, 5)
	for i := uint64(1); i <= 5; i++ {
		entries = append(entries, proto.Entry{Index: i, Term: 5, Type: proto.EntryNormal})
	}
	r.log.append(entries)
	r.log.commitTo(5)
	if r.leaderID != 2 || r.log.committed != 5 {
		t.Fatalf("fixture: leaderID=%d committed=%d, want 2 and 5",
			r.leaderID, r.log.committed)
	}
	r.electionTick = 7
	r.drainMessages()
	return r
}

// TestInstallSnapshot_SameTermNonLeaderCannotReplaceTheLog is the attacker test
// for the accept path. Replica 3 sends a snapshot at index 40 — above the
// committed index, so it takes the non-matching branch and would restore the log
// wholesale — at the SAME term, while replica 2 is this term's leader.
//
// Election Safety says term 5 has exactly one leader, so replica 3 is either a
// deposed sender replaying an old transfer or a forgery. Either way its snapshot
// must move nothing: adopting it discards entries 1..5 that the real leader
// replicated and rebuilds the voting set from a membership replica 3 chose.
func TestInstallSnapshot_SameTermNonLeaderCannotReplaceTheLog(t *testing.T) {
	quietLogs(t)

	r := followerWithEstablishedLeader(t)
	committedBefore := r.log.committed
	lastBefore := r.log.lastIndex()

	err := r.step(proto.Message{
		Type: proto.InstallSnapshot, From: 3, To: 1, Term: 5, Origin: proto.OriginLocal,
		Snapshot: proto.Snapshot{
			Index: 40, Term: 5,
			Membership: proto.Membership{Addresses: map[uint64]string{3: "attacker"}},
		},
	})

	if r.log.committed != committedBefore || r.log.lastIndex() != lastBefore {
		t.Fatalf("log moved committed %d->%d lastIndex %d->%d on an InstallSnapshot "+
			"from replica 3 while replica 2 is term 5's leader: the snapshot carries "+
			"no log-matching proof of any kind and its non-matching branch replaces "+
			"the whole log, so a second claimant at one term must move nothing",
			committedBefore, r.log.committed, lastBefore, r.log.lastIndex())
	}
	if _, ok := r.remotes[3]; ok && len(r.remotes) == 1 {
		t.Fatalf("remotes rebuilt from the sender's membership (%v): the voting "+
			"set quorum() is computed over was redefined by a replica that is not "+
			"this term's leader", r.remotes)
	}
	if r.leaderID != 2 {
		t.Fatalf("leaderID = %d, want 2: a refused InstallSnapshot must not install "+
			"its sender as leader", r.leaderID)
	}
	if !errors.Is(err, ErrSnapshotNotFromLeader) {
		t.Fatalf("step returned %v, want ErrSnapshotNotFromLeader", err)
	}
}

// TestInstallSnapshot_RejectedSnapshotFromNonLeaderCannotSuppressTheElection is
// the attacker test for the capability that survives even when the snapshot is
// REJECTED. The snapshot is at index 3, at or below the committed index, so
// handleSnapshot's first branch throws it away — but the election timer reset and
// the leaderID install both happened above that branch.
//
// This is the cheaper and more durable half of the defect: the attacker needs no
// valid snapshot at all, just one stale artifact re-sent on a timer, and the
// victim is silently excluded from every subsequent election. Suppressing an
// election is not a lesser power than winning one.
func TestInstallSnapshot_RejectedSnapshotFromNonLeaderCannotSuppressTheElection(t *testing.T) {
	quietLogs(t)

	r := followerWithEstablishedLeader(t)
	tickBefore := r.electionTick

	err := r.step(proto.Message{
		Type: proto.InstallSnapshot, From: 3, To: 1, Term: 5, Origin: proto.OriginLocal,
		Snapshot: proto.Snapshot{Index: 3, Term: 5},
	})

	if r.electionTick != tickBefore {
		t.Fatalf("electionTick reset %d -> %d by a snapshot that was REJECTED as "+
			"behind the committed index, and sent by a replica that is not this "+
			"term's leader: re-sending one obsolete artifact on a timer holds the "+
			"election timer at zero forever and removes this voter from every "+
			"quorum without removing it from the configuration",
			tickBefore, r.electionTick)
	}
	if r.leaderID != 2 {
		t.Fatalf("leaderID = %d, want 2: installing the sender as leader makes "+
			"handleVoteRequest deny PreVotes and dropHighTermVote drop higher-term "+
			"RequestVotes on this replica's behalf", r.leaderID)
	}
	if !errors.Is(err, ErrSnapshotNotFromLeader) {
		t.Fatalf("step returned %v, want ErrSnapshotNotFromLeader", err)
	}
}

// TestInstallSnapshot_RecognisedLeaderStillInstalls is the primary liveness
// control. The follower's own leader sends a snapshot ahead of its committed
// index and the ordinary catch-up must work exactly as before, or every replica
// that falls behind a compacted log is stranded permanently.
func TestInstallSnapshot_RecognisedLeaderStillInstalls(t *testing.T) {
	quietLogs(t)

	r := followerWithEstablishedLeader(t)

	if err := r.step(proto.Message{
		Type: proto.InstallSnapshot, From: 2, To: 1, Term: 5, Origin: proto.OriginLocal,
		Snapshot: proto.Snapshot{
			Index: 40, Term: 5,
			Membership: proto.Membership{Addresses: map[uint64]string{
				1: "addr1", 2: "addr2", 3: "addr3",
			}},
		},
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.log.committed != 40 {
		t.Fatalf("committed = %d, want 40: the recognised leader's snapshot must "+
			"still install, or a follower behind a compacted log never catches up",
			r.log.committed)
	}
	if r.electionTick != 0 {
		t.Fatalf("electionTick = %d, want 0: a legitimate install must still reset "+
			"the election timer", r.electionTick)
	}
	if got := collectMessages(r, proto.ReplicateResp); len(got) != 1 || got[0].LogIndex != 40 {
		t.Fatalf("ReplicateResp = %+v, want exactly one acknowledging index 40", got)
	}
}

// TestInstallSnapshot_NoLeaderYetStillInstalls is the second liveness control:
// the gate is `leaderID != noLeader && From != leaderID`, and dropping the first
// conjunct would strand a restarted replica that has not yet heard from anyone.
func TestInstallSnapshot_NoLeaderYetStillInstalls(t *testing.T) {
	quietLogs(t)

	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(5, noLeader)
	r.drainMessages()

	if err := r.step(proto.Message{
		Type: proto.InstallSnapshot, From: 3, To: 1, Term: 5, Origin: proto.OriginLocal,
		Snapshot: proto.Snapshot{Index: 20, Term: 5},
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.leaderID != 3 || r.log.committed != 20 {
		t.Fatalf("leaderID=%d committed=%d, want 3 and 20: a replica with no leader "+
			"for the term must accept first contact", r.leaderID, r.log.committed)
	}
}

// TestInstallSnapshot_HigherTermSenderStillInstalls is the third liveness
// control, and it is the one the gate's placement depends on. step() installs a
// leader-originated sender at a HIGHER term via becomeFollower(msg.Term,
// msg.From) before the handler runs, so by the time the gate reads r.leaderID
// the new leader is already there. A gate that read the PRE-step leader would
// reject every legitimate snapshot from a newly elected leader.
func TestInstallSnapshot_HigherTermSenderStillInstalls(t *testing.T) {
	quietLogs(t)

	r := followerWithEstablishedLeader(t)

	if err := r.step(proto.Message{
		Type: proto.InstallSnapshot, From: 3, To: 1, Term: 9, Origin: proto.OriginLocal,
		Snapshot: proto.Snapshot{Index: 40, Term: 9},
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.term != 9 || r.leaderID != 3 || r.log.committed != 40 {
		t.Fatalf("term=%d leaderID=%d committed=%d, want 9, 3 and 40: a leader at a "+
			"higher term is authoritative and its snapshot must still install",
			r.term, r.leaderID, r.log.committed)
	}
}

// TestInstallSnapshot_CandidateStillInstalls is the fourth liveness control.
// stepCandidate calls stepDownToFollower(msg) before handleSnapshot, which sets
// leaderID = msg.From, so a candidate must still accept the install that ends
// its candidacy. This is the path a follower catching up by snapshot takes when
// its election timer fires mid-transfer.
func TestInstallSnapshot_CandidateStillInstalls(t *testing.T) {
	quietLogs(t)

	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(5, 2)
	r.becomeCandidate()
	r.drainMessages()
	if r.state != candidate || r.leaderID != noLeader {
		t.Fatalf("fixture: state=%v leaderID=%d, want candidate and noLeader",
			r.state, r.leaderID)
	}

	if err := r.step(proto.Message{
		Type: proto.InstallSnapshot, From: 2, To: 1, Term: r.term, Origin: proto.OriginLocal,
		Snapshot: proto.Snapshot{Index: 30, Term: r.term},
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.state != follower || r.log.committed != 30 {
		t.Fatalf("state=%v committed=%d, want follower and 30: a candidate must "+
			"still step down to and install from the term's real leader",
			r.state, r.log.committed)
	}
}
