package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestStepCandidate_LocalInstallSnapshotDoesNotRegressTerm asserts that a
// term-less local message cannot drive a candidate's term backwards.
//
// The snapshot receive path builds InstallSnapshot with Term 0 (it is injected
// in-process, so there is no term to carry), and becomeFollower assigned
// msg.Term unconditionally. A follower that is catching up by snapshot and
// whose election timer fires mid-transfer becomes a candidate at term N; the
// install then lands on that candidate and dropped it to term 0, clearing its
// vote. It would grant votes again in terms it had already voted in — two
// leaders in one term, an Election Safety violation.
func TestStepCandidate_LocalInstallSnapshotDoesNotRegressTerm(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfg(1, 1)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft failed: %v", err)
	}

	// Drive this replica to candidate at a real term with a recorded vote.
	r.becomeCandidate()
	r.term = 10
	r.vote = r.replicaID
	if r.state != candidate {
		t.Fatalf("precondition failed: state = %v, want candidate", r.state)
	}

	// The install arrives the way the receive path builds it: no term.
	if err := r.step(proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Origin:   proto.OriginLocal,
		Snapshot: proto.Snapshot{Index: 5, Term: 3},
	}); err != nil {
		t.Fatalf("local InstallSnapshot rejected: %v", err)
	}

	if r.term < 10 {
		t.Errorf("term regressed %d -> %d on a term-less local message: the "+
			"replica will grant votes again in terms it has already voted in",
			10, r.term)
	}
	if r.vote == noNode {
		t.Errorf("vote was cleared by a term-less local message; the replica "+
			"can now double-vote in term %d", r.term)
	}
}
