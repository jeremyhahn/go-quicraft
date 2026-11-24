package raft

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// candidateReadIndexCases enumerates every way a ReadIndex reaches a
// campaigning replica: from its own host (Origin local, no term) and forwarded
// by a follower that still believed this replica led (Origin remote, stamped
// with the forwarder's term). Both roles that share stepCandidate are covered,
// because becomePreCandidate and becomeCandidate install the same step
// function and a gap in it is a gap in both.
var candidateReadIndexCases = []struct {
	name    string
	build   func() *raft
	forward bool
}{
	{
		name: "candidate/local",
		build: func() *raft {
			r := newTestRaftPreVoteDisabled(1)
			r.becomeCandidate()
			return r
		},
	},
	{
		name: "candidate/forwarded",
		build: func() *raft {
			r := newTestRaftPreVoteDisabled(1)
			r.becomeCandidate()
			return r
		},
		forward: true,
	},
	{
		name:  "precandidate/local",
		build: newTestPreCandidate,
	},
	{
		name:    "precandidate/forwarded",
		build:   newTestPreCandidate,
		forward: true,
	},
}

// newTestPreCandidate builds a pre-candidate at a real term. becomePreCandidate
// leaves the term alone (PreVote never advances it), and a forwarded ReadIndex
// stamped with term 0 would be refused by step's no-term guard before the role
// dispatch this test is about.
func newTestPreCandidate() *raft {
	r := newTestRaft(1)
	r.becomeFollower(3, noLeader)
	r.becomePreCandidate()
	return r
}

func candidateReadIndexMessage(r *raft, forward bool) proto.Message {
	if forward {
		return proto.Message{
			Type:   proto.ReadIndex,
			From:   2,
			To:     r.replicaID,
			Term:   r.term,
			Hint:   42,
			Origin: proto.OriginRemote,
		}
	}
	return proto.Message{
		Type:   proto.ReadIndex,
		From:   r.replicaID,
		Hint:   42,
		Origin: proto.OriginLocal,
	}
}

// TestStepCandidate_ReadIndexIsRefusedNotLeader asserts that a ReadIndex
// arriving at a campaigning replica is refused with the same
// NotLeaderError{LeaderID: 0} a Propose gets, rather than falling to
// stepCandidate's default and being silently dropped.
//
// A dropped ReadIndex is never answered: nothing is queued in
// pendingReadIndex, nothing is forwarded, and no ReadyToRead is ever produced,
// so the host's pending read waits to the caller's deadline and SyncRead
// reports ErrTimeout for what is a not-leader condition. The follower path
// (handleFollowerReadIndex) already refuses this way when it knows no leader;
// a candidate by definition knows none and cannot supply a leader's commit
// index, so it must refuse the same way, for the same reason.
func TestStepCandidate_ReadIndexIsRefusedNotLeader(t *testing.T) {
	for _, tc := range candidateReadIndexCases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.build()
			stateBefore, termBefore := r.state, r.term
			msg := candidateReadIndexMessage(r, tc.forward)

			err := r.step(msg)

			var nle *NotLeaderError
			if !errors.As(err, &nle) {
				t.Fatalf("ReadIndex at a %v: got err = %v, want *NotLeaderError "+
					"(a nil here is the silent drop that hangs SyncRead to its deadline)",
					stateBefore, err)
			}
			if nle.LeaderID != 0 {
				t.Fatalf("NotLeaderError.LeaderID = %d, want 0: a campaigning replica knows no leader", nle.LeaderID)
			}

			// A refusal, never a forward or an answer.
			if n := len(r.msgs); n != 0 {
				t.Fatalf("candidate sent %d message(s) on a refused ReadIndex, want none: %+v", n, r.msgs)
			}
			if n := len(r.pendingReadIndex); n != 0 {
				t.Fatalf("candidate queued %d pending ReadIndex, want none", n)
			}
			if n := len(r.readStates); n != 0 {
				t.Fatalf("candidate produced %d ReadyToRead, want none", n)
			}
			// The refusal is inert with respect to the election.
			if r.state != stateBefore || r.term != termBefore {
				t.Fatalf("refusing a ReadIndex changed role/term: %v/%d -> %v/%d",
					stateBefore, termBefore, r.state, r.term)
			}
		})
	}
}

// TestStepCandidate_ReadIndexRefusalMatchesPropose pins the symmetry the fix
// restores: for a given campaigning replica, ReadIndex and Propose yield the
// same refusal.
func TestStepCandidate_ReadIndexRefusalMatchesPropose(t *testing.T) {
	for _, tc := range candidateReadIndexCases {
		if tc.forward {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			r := tc.build()

			readErr := r.step(candidateReadIndexMessage(r, false))
			proposeErr := r.step(proto.Message{
				Type:    proto.Propose,
				From:    r.replicaID,
				Origin:  proto.OriginLocal,
				Entries: []proto.Entry{{Cmd: []byte("x")}},
			})

			var readNLE, proposeNLE *NotLeaderError
			if !errors.As(readErr, &readNLE) || !errors.As(proposeErr, &proposeNLE) {
				t.Fatalf("read err = %v, propose err = %v; want *NotLeaderError from both", readErr, proposeErr)
			}
			if *readNLE != *proposeNLE {
				t.Fatalf("ReadIndex refusal %+v differs from Propose refusal %+v", *readNLE, *proposeNLE)
			}
		})
	}
}

// The step functions are the read hot path. These pin the per-call cost of the
// candidate dispatch for the two client request types, with the follower's
// leaderless refusal as the reference point; an extra case in a switch over a
// small constant set must not move them.

func benchmarkStep(b *testing.B, r *raft, msg proto.Message) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.step(msg)
	}
}

func BenchmarkStepCandidate_ReadIndex(b *testing.B) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()
	benchmarkStep(b, r, candidateReadIndexMessage(r, false))
}

func BenchmarkStepCandidate_Propose(b *testing.B) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()
	benchmarkStep(b, r, proto.Message{
		Type:    proto.Propose,
		From:    r.replicaID,
		Origin:  proto.OriginLocal,
		Entries: []proto.Entry{{Cmd: []byte("x")}},
	})
}

func BenchmarkStepPreCandidate_ReadIndex(b *testing.B) {
	r := newTestRaft(1)
	r.becomePreCandidate()
	benchmarkStep(b, r, candidateReadIndexMessage(r, false))
}

func BenchmarkStepFollower_ReadIndexNoLeader(b *testing.B) {
	r := newTestRaft(1)
	r.leaderID = noLeader
	benchmarkStep(b, r, candidateReadIndexMessage(r, false))
}
