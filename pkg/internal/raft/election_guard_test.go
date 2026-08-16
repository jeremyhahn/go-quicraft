package raft

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestStartElection_UnreadableLastTermDoesNotBurnTerm asserts that a replica
// whose own last log term cannot be read does not enter candidate state and
// does not advance its term.
//
// The guard originally lived inside campaign(), which runs AFTER
// becomeCandidate(). The term was therefore already incremented, leaderID
// cleared and timers reset by the time the guard fired, so the node sat as a
// candidate having sent zero RequestVotes. Because send() stamps r.term on
// every reply, that rising term reaches the live leader through ordinary
// Replicate/Heartbeat rejections and forces a step-down on each exchange --
// one replica with an unreadable log term escalating into cluster-wide leader
// churn while emitting no election traffic of its own.
func TestStartElection_UnreadableLastTermDoesNotBurnTerm(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfg(1, 1)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft failed: %v", err)
	}

	// Make lastIndex resolve to an index whose term cannot be read: the
	// in-memory marker sits above it and LogDB has nothing.
	r.log.inmem = newInMemory(5)
	if _, ok := r.log.lastTerm(); ok {
		t.Fatal("precondition failed: lastTerm is readable, test proves nothing")
	}

	termBefore := r.term
	r.startElection()

	if r.state != follower {
		t.Errorf("state = %v, want follower: a replica that cannot read its "+
			"own last log term must not campaign", r.state)
	}
	if r.term != termBefore {
		t.Errorf("term = %d, want %d: the term was burned on an election that "+
			"could never send a vote request, and send() stamps it onto every "+
			"reply, forcing the live leader to step down", r.term, termBefore)
	}
	if len(r.msgs) != 0 {
		t.Errorf("emitted %d messages, want 0", len(r.msgs))
	}
}

// TestStartElection_ReadableLastTermStillCampaigns is the liveness control:
// the guard must not suppress a legitimate election.
func TestStartElection_ReadableLastTermStillCampaigns(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfg(1, 1)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft failed: %v", err)
	}
	if _, ok := r.log.lastTerm(); !ok {
		t.Fatal("precondition failed: lastTerm should be readable on a fresh log")
	}

	r.startElection()

	if r.state == follower {
		t.Error("replica with a readable last term did not campaign")
	}
	var sawVote bool
	for i := range r.msgs {
		if r.msgs[i].Type == proto.RequestVote || r.msgs[i].Type == proto.RequestPreVote {
			sawVote = true
			break
		}
	}
	if !sawVote {
		t.Error("campaign emitted no vote request")
	}
}

// unreadableLastTermReplica builds a follower whose lastIndex resolves to an
// index whose term cannot be read: the in-memory marker sits above it and the
// test LogDB has nothing. It is the same fault the two startElection tests above
// use, factored out so the TimeoutNow entry point is exercised against the
// identical condition rather than an approximation of it.
func unreadableLastTermReplica(t *testing.T) *raft {
	t.Helper()
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	r, err := newRaft(raftTestCfg(1, 1), newTestLogReaderWithMembership(members))
	if err != nil {
		t.Fatalf("newRaft failed: %v", err)
	}
	r.log.inmem = newInMemory(5)
	if _, ok := r.log.lastTerm(); ok {
		t.Fatal("precondition failed: lastTerm is readable, test proves nothing")
	}
	r.drainMessages()
	return r
}

// TestTimeoutNow_UnreadableLastTermDoesNotBurnTerm is the same defect as
// TestStartElection_UnreadableLastTermDoesNotBurnTerm, at the OTHER campaign
// entry point. startElection got the guard; handleTimeoutNow did not, and it
// reaches becomeCandidate by a path that never passes through startElection.
//
// The cost is not hypothetical. becomeCandidate increments the term, then
// campaign() refuses to build the vote request (LogTerm 0 is rejected by every
// peer holding a non-empty log) and returns having sent nothing. The replica is
// left at T+1 with no election in progress, and because send() stamps r.term on
// every reply it makes, the burned term travels to the LIVE leader on the next
// ordinary Replicate/Heartbeat rejection and deposes it. A leadership transfer
// that could never have succeeded costs the shard its healthy leader.
func TestTimeoutNow_UnreadableLastTermDoesNotBurnTerm(t *testing.T) {
	quietLogs(t)

	r := unreadableLastTermReplica(t)
	r.becomeFollower(5, 2)
	r.drainMessages()
	termBefore := r.term

	err := r.step(proto.Message{Type: proto.TimeoutNow, From: 2, To: 1, Term: 5})

	// The observable damage is asserted BEFORE the error value. A test that
	// leads with the sentinel proves only that some branch returned it; these
	// four assertions are the property itself, and each one fails on its own
	// when the guard is removed.
	if r.term != termBefore {
		t.Fatalf("term = %d, want %d: TimeoutNow burned a term on an election that "+
			"could not send a single vote request, and send() stamps that term onto "+
			"every reply — the live leader steps down on the next exchange",
			r.term, termBefore)
	}
	if r.state != follower {
		t.Fatalf("state = %v, want follower: a replica that cannot read its own "+
			"last log term must not enter candidate state", r.state)
	}
	if got := collectMessages(r, proto.RequestVote); len(got) != 0 {
		t.Fatalf("emitted %d RequestVote messages, want 0: this is the campaign "+
			"that cannot happen, so the term spent on it buys nothing", len(got))
	}
	if r.leaderID != 2 {
		t.Fatalf("leaderID = %d, want 2: a refused TimeoutNow must not clear the "+
			"leader this replica still recognises", r.leaderID)
	}
	if !errors.Is(err, ErrUnreadableLogCannotCampaign) {
		t.Fatalf("step returned %v, want ErrUnreadableLogCannotCampaign: the "+
			"refusal has to be reportable, or the transfer source cannot tell a "+
			"declined transfer from an accepted one", err)
	}
}

// TestTimeoutNow_ReadableLastTermStillCampaigns is the liveness control. A
// leadership transfer to a healthy replica must still campaign immediately —
// that is the entire purpose of TimeoutNow, and a guard that fired here would
// break every transfer instead of just the doomed ones.
func TestTimeoutNow_ReadableLastTermStillCampaigns(t *testing.T) {
	quietLogs(t)

	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(5, 2)
	r.drainMessages()
	if _, ok := r.log.lastTerm(); !ok {
		t.Fatal("precondition failed: lastTerm should be readable on a fresh log")
	}

	if err := r.step(proto.Message{
		Type: proto.TimeoutNow, From: 2, To: 1, Term: 5,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if r.state != candidate {
		t.Fatalf("state = %v, want candidate: TimeoutNow on a healthy replica must "+
			"campaign immediately", r.state)
	}
	if r.term != 6 {
		t.Fatalf("term = %d, want 6", r.term)
	}
	votes := collectMessages(r, proto.RequestVote)
	if len(votes) != 2 {
		t.Fatalf("emitted %d RequestVote messages, want 2 (the other two voters)",
			len(votes))
	}
	// The transfer hint is what exempts these votes from the recipients'
	// CheckQuorum higher-term-vote guard; losing it would make the transfer
	// silently ineffective against a leader whose lease is still fresh.
	for i := range votes {
		if votes[i].Hint != r.replicaID {
			t.Fatalf("RequestVote to %d carries Hint %d, want %d (the transfer hint)",
				votes[i].To, votes[i].Hint, r.replicaID)
		}
	}
}
