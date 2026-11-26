package raft

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// timeoutNowSenderSnapshot is the election state a refused TimeoutNow must
// leave untouched.
type timeoutNowSenderSnapshot struct {
	state  roleState
	term   uint64
	vote   uint64
	leader uint64
}

func snapshotTimeoutNowSender(r *raft) timeoutNowSenderSnapshot {
	return timeoutNowSenderSnapshot{state: r.state, term: r.term, vote: r.vote, leader: r.leaderID}
}

// TestTimeoutNowSender_RemoteNonLeaderIsRefused checks that a follower only
// accepts a remote TimeoutNow from its current leader at its current term.
func TestTimeoutNowSender_RemoteNonLeaderIsRefused(t *testing.T) {
	cases := []struct {
		name string
		msg  proto.Message
		// viaStep routes the message through step; otherwise it goes to
		// handleTimeoutNow directly (step already drops a lower term).
		viaStep bool
	}{
		{
			name:    "other replica at the current term",
			msg:     proto.Message{Type: proto.TimeoutNow, From: 3, To: 1, ShardID: 1, Term: staleConfigTerm},
			viaStep: true,
		},
		{
			name: "leader at a lower term",
			msg:  proto.Message{Type: proto.TimeoutNow, From: staleConfigLeader, To: 1, ShardID: 1, Term: staleConfigTerm - 1},
		},
		{
			name: "leader without a term",
			msg:  proto.Message{Type: proto.TimeoutNow, From: staleConfigLeader, To: 1, ShardID: 1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			r := staleConfigFollower(ctx, t, nil)
			before := snapshotTimeoutNowSender(r)
			var err error
			if tc.viaStep {
				err = r.step(tc.msg)
			} else {
				err = r.handleTimeoutNow(tc.msg)
			}
			if !errors.Is(err, ErrTimeoutNowNotFromLeader) {
				t.Fatalf("TimeoutNow returned %v, want ErrTimeoutNowNotFromLeader", err)
			}
			if after := snapshotTimeoutNowSender(r); after != before {
				t.Fatalf("election state changed %+v -> %+v", before, after)
			}
			assertNoVoteRequest(ctx, t, r, tc.name)
		})
	}
}

// TestTimeoutNowSender_LeaderAtCurrentTermCampaigns checks that the leader's
// TimeoutNow at the current term still starts the election.
func TestTimeoutNowSender_LeaderAtCurrentTermCampaigns(t *testing.T) {
	ctx := t.Context()
	r := staleConfigFollower(ctx, t, nil)
	if err := r.step(staleConfigTimeoutNow()); err != nil {
		t.Fatalf("step(TimeoutNow) = %v, want nil", err)
	}
	if r.state != candidate || r.term != staleConfigTerm+1 || r.vote != r.replicaID {
		t.Fatalf("state=%v term=%d vote=%d, want candidate at %d voting for self",
			r.state, r.term, r.vote, staleConfigTerm+1)
	}
	if n := len(collectMessages(r, proto.RequestVote)); n != 2 {
		t.Fatalf("%d RequestVote sent, want 2", n)
	}
}

// TestTimeoutNowSender_LocalOriginCampaigns checks that a locally injected
// TimeoutNow is not subject to the sender check.
func TestTimeoutNowSender_LocalOriginCampaigns(t *testing.T) {
	ctx := t.Context()
	r := staleConfigFollower(ctx, t, nil)
	msg := proto.Message{Type: proto.TimeoutNow, To: 1, ShardID: 1, Origin: proto.OriginLocal}
	if err := r.handleTimeoutNow(msg); err != nil {
		t.Fatalf("handleTimeoutNow(local) = %v, want nil", err)
	}
	if r.state != candidate || r.term != staleConfigTerm+1 {
		t.Fatalf("state=%v term=%d, want candidate at %d", r.state, r.term, staleConfigTerm+1)
	}
	if n := len(collectMessages(r, proto.RequestVote)); n != 2 {
		t.Fatalf("%d RequestVote sent, want 2", n)
	}
}
