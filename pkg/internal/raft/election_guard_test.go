package raft

import (
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
