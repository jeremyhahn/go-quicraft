package raft

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// staleConfigTerm and staleConfigLeader are the term and leader the follower
// under test sits at before the TimeoutNow arrives.
const (
	staleConfigTerm   uint64 = 5
	staleConfigLeader uint64 = 2
)

// staleConfigFollower returns replica 1 of a three-member shard {1, 2, 3} as a
// follower of replica 2 at staleConfigTerm, with resolve installed as its
// resolveStaleConfig hook and its outbound queue drained.
func staleConfigFollower(ctx context.Context, t *testing.T, resolve func() bool) *raft {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before setup: %v", err)
	}
	quietLogs(t)
	r := newTestRaftWithMembers(1, map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"})
	r.becomeFollower(staleConfigTerm, staleConfigLeader)
	r.resolveStaleConfig = resolve
	r.drainMessages()
	if r.state != follower || r.term != staleConfigTerm || r.leaderID != staleConfigLeader {
		t.Fatalf("precondition: state=%v term=%d leader=%d, want follower at %d led by %d",
			r.state, r.term, r.leaderID, staleConfigTerm, staleConfigLeader)
	}
	if len(r.remotes) != 2 {
		t.Fatalf("precondition: %d remotes, want 2 (replicas 2 and 3)", len(r.remotes))
	}
	return r
}

// staleConfigTimeoutNow is the TimeoutNow the leader sends at the current term.
func staleConfigTimeoutNow() proto.Message {
	return proto.Message{
		Type: proto.TimeoutNow, From: staleConfigLeader, To: 1, ShardID: 1, Term: staleConfigTerm,
	}
}

// assertNoVoteRequest fails the test when r has a RequestVote or
// RequestPreVote queued.
func assertNoVoteRequest(ctx context.Context, t *testing.T, r *raft, when string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done: %v", err)
	}
	if n := len(collectMessages(r, proto.RequestVote)); n != 0 {
		t.Errorf("%s: %d RequestVote pending, want 0", when, n)
	}
	if n := len(collectMessages(r, proto.RequestPreVote)); n != 0 {
		t.Errorf("%s: %d RequestPreVote pending, want 0", when, n)
	}
}

// TestStaleConfigTimeoutNow_UnresolvedTailRefusesCampaign covers (a) and (b):
// a TimeoutNow on a replica whose committed configuration tail cannot be
// folded returns ErrStaleConfigCannotCampaign and changes nothing.
func TestStaleConfigTimeoutNow_UnresolvedTailRefusesCampaign(t *testing.T) {
	ctx := t.Context()
	calls := 0
	r := staleConfigFollower(ctx, t, func() bool { calls++; return false })
	voteBefore := r.vote

	err := r.step(staleConfigTimeoutNow())
	if !errors.Is(err, ErrStaleConfigCannotCampaign) {
		t.Fatalf("step(TimeoutNow) = %v, want ErrStaleConfigCannotCampaign", err)
	}
	if calls == 0 {
		t.Fatal("resolveStaleConfig was never consulted")
	}
	if r.state != follower {
		t.Errorf("state = %v, want follower", r.state)
	}
	if r.term != staleConfigTerm {
		t.Errorf("term = %d, want %d unchanged", r.term, staleConfigTerm)
	}
	if r.vote != voteBefore {
		t.Errorf("vote = %d, want %d unchanged", r.vote, voteBefore)
	}
	if r.leaderID != staleConfigLeader {
		t.Errorf("leaderID = %d, want %d unchanged", r.leaderID, staleConfigLeader)
	}
	assertNoVoteRequest(ctx, t, r, "after a refused TimeoutNow")
}

// TestStaleConfigTimeoutNow_ElectionTimeoutAlsoRefuses covers (c): the
// election-timeout path shares the gate, so ticking the same replica past its
// election timeout starts no election either.
func TestStaleConfigTimeoutNow_ElectionTimeoutAlsoRefuses(t *testing.T) {
	ctx := t.Context()
	calls := 0
	r := staleConfigFollower(ctx, t, func() bool { calls++; return false })
	if err := r.step(staleConfigTimeoutNow()); !errors.Is(err, ErrStaleConfigCannotCampaign) {
		t.Fatalf("step(TimeoutNow) = %v, want ErrStaleConfigCannotCampaign", err)
	}
	callsAfterTimeoutNow := calls

	// randomizedElectionTimeout is below 2*electionTimeout; three timeouts'
	// worth of ticks expires it at least once whatever the draw.
	for range 3 * r.electionTimeout {
		r.tickFunc(r)
	}
	if calls == callsAfterTimeoutNow {
		t.Fatal("the election timeout never reached the stale-config gate: the " +
			"ticks prove nothing")
	}
	if r.state != follower {
		t.Errorf("state = %v, want follower", r.state)
	}
	if r.term != staleConfigTerm {
		t.Errorf("term = %d, want %d unchanged", r.term, staleConfigTerm)
	}
	assertNoVoteRequest(ctx, t, r, "after the election timeout")
}

// TestStaleConfigTimeoutNow_ResolvedTailCampaigns covers (d) and (e): with the
// hook reporting true, or with no hook at all, the same TimeoutNow makes the
// replica a candidate that solicits votes from both peers.
func TestStaleConfigTimeoutNow_ResolvedTailCampaigns(t *testing.T) {
	calls := 0
	cases := []struct {
		name    string
		resolve func() bool
	}{
		{"hook reports true", func() bool { calls++; return true }},
		{"nil hook", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			calls = 0
			r := staleConfigFollower(ctx, t, tc.resolve)
			if err := r.step(staleConfigTimeoutNow()); err != nil {
				t.Fatalf("step(TimeoutNow) = %v, want nil", err)
			}
			if tc.resolve != nil && calls == 0 {
				t.Error("resolveStaleConfig was never consulted")
			}
			if r.state != candidate {
				t.Fatalf("state = %v, want candidate", r.state)
			}
			if r.term != staleConfigTerm+1 {
				t.Errorf("term = %d, want %d", r.term, staleConfigTerm+1)
			}
			if r.vote != r.replicaID {
				t.Errorf("vote = %d, want self (%d)", r.vote, r.replicaID)
			}
			votes := collectMessages(r, proto.RequestVote)
			to := make([]uint64, 0, len(votes))
			for i := range votes {
				to = append(to, votes[i].To)
				if votes[i].Term != staleConfigTerm+1 {
					t.Errorf("RequestVote to %d at term %d, want %d",
						votes[i].To, votes[i].Term, staleConfigTerm+1)
				}
			}
			sort.Slice(to, func(i, j int) bool { return to[i] < to[j] })
			if len(to) != 2 || to[0] != 2 || to[1] != 3 {
				t.Errorf("RequestVote sent to %v, want [2 3]", to)
			}
			if n := len(collectMessages(r, proto.RequestPreVote)); n != 0 {
				t.Errorf("%d RequestPreVote sent, want 0: TimeoutNow bypasses PreVote", n)
			}
		})
	}
}
