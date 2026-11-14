package raft

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// rejectFollower returns a follower Peer of a three-replica shard whose
// leader is replica 2, with no refused reads pending.
func rejectFollower(ctx context.Context, t *testing.T) *Peer {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before the peer was built: %v", err)
	}
	p := newTestPeer(t, testConfig(), proto.State{}, threeNodeMembership())
	p.r.becomeFollower(2, 2)
	if p.r.leaderID != 2 {
		t.Fatalf("leaderID = %d, want 2", p.r.leaderID)
	}
	return p
}

// stepReject hands the Peer a ReadIndexResp from replica from refusing the
// read keyed hint, and returns what Handle returned.
func stepReject(ctx context.Context, t *testing.T, p *Peer, from, hint uint64) error {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before the reject was stepped: %v", err)
	}
	return p.Handle(proto.Message{
		Type: proto.ReadIndexResp, From: from, To: p.replicaID, Term: p.r.term,
		Hint: hint, Reject: true,
	})
}

// A Peer that has had no read refused drains nil, not an empty slice.
func TestPeer_DrainReadRejects_NilWhenNoneRefused(t *testing.T) {
	p := rejectFollower(t.Context(), t)
	if got := p.DrainReadRejects(); got != nil {
		t.Fatalf("DrainReadRejects() = %#v, want nil", got)
	}
}

// Each refused read comes back once, in the order the refusals arrived.
func TestPeer_DrainReadRejects_EachOnceInArrivalOrder(t *testing.T) {
	ctx := t.Context()
	p := rejectFollower(ctx, t)
	for _, hint := range []uint64{7, 8, 9} {
		if err := stepReject(ctx, t, p, 2, hint); err != nil {
			t.Fatalf("reject %d from the leader: %v", hint, err)
		}
	}
	if got, want := p.DrainReadRejects(), []uint64{7, 8, 9}; !slices.Equal(got, want) {
		t.Fatalf("DrainReadRejects() = %v, want %v", got, want)
	}
	if got := p.DrainReadRejects(); got != nil {
		t.Fatalf("second DrainReadRejects() = %v, want nil", got)
	}
}

// A refusal from a replica that is not the leader fails no read.
func TestPeer_DrainReadRejects_SkipsRejectNotFromLeader(t *testing.T) {
	ctx := t.Context()
	p := rejectFollower(ctx, t)
	if err := stepReject(ctx, t, p, 3, 13); !errors.Is(err, ErrReadIndexRespNotFromLeader) {
		t.Fatalf("reject from replica 3: got %v, want ErrReadIndexRespNotFromLeader", err)
	}
	if err := stepReject(ctx, t, p, 2, 14); err != nil {
		t.Fatalf("reject from the leader: %v", err)
	}
	if got, want := p.DrainReadRejects(), []uint64{14}; !slices.Equal(got, want) {
		t.Fatalf("DrainReadRejects() = %v, want %v", got, want)
	}
}

// A full reject list refuses the next reject with ErrReadIndexOverloaded and
// keeps what it holds; a drain makes room again.
func TestPeer_DrainReadRejects_FullListIsOverloaded(t *testing.T) {
	ctx := t.Context()
	p := rejectFollower(ctx, t)
	for hint := uint64(1); hint <= maxReadStates; hint++ {
		if err := stepReject(ctx, t, p, 2, hint); err != nil {
			t.Fatalf("reject %d of %d: %v", hint, maxReadStates, err)
		}
	}
	if err := stepReject(ctx, t, p, 2, maxReadStates+1); !errors.Is(err, ErrReadIndexOverloaded) {
		t.Fatalf("reject past the cap: got %v, want ErrReadIndexOverloaded", err)
	}
	got := p.DrainReadRejects()
	if len(got) != maxReadStates {
		t.Fatalf("drained %d keys, want %d", len(got), maxReadStates)
	}
	if got[0] != 1 || got[len(got)-1] != maxReadStates {
		t.Fatalf("drained keys run %d..%d, want 1..%d", got[0], got[len(got)-1], maxReadStates)
	}
	if err := stepReject(ctx, t, p, 2, maxReadStates+2); err != nil {
		t.Fatalf("reject after a drain: %v", err)
	}
	if got, want := p.DrainReadRejects(), []uint64{maxReadStates + 2}; !slices.Equal(got, want) {
		t.Fatalf("DrainReadRejects() after the refill = %v, want %v", got, want)
	}
}
