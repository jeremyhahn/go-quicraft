package raft

import (
	"context"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// requeueTestMsg builds an outbound message whose Term tags it uniquely, so a
// test can tell two messages of the same type apart after reordering.
func requeueTestMsg(msgType uint64, to, tag uint64) proto.Message {
	return proto.Message{Type: msgType, To: to, Term: tag}
}

// assertRequeueOrder fails the test unless got holds exactly want's messages,
// by type and tag, in want's order.
func assertRequeueOrder(ctx context.Context, t *testing.T, what string, got, want []proto.Message) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("%s: context done: %v", what, err)
	}
	if len(got) != len(want) {
		t.Fatalf("%s: got %d messages, want %d: %+v", what, len(got), len(want), got)
	}
	for i := range want {
		if got[i].Type != want[i].Type || got[i].To != want[i].To || got[i].Term != want[i].Term {
			t.Fatalf("%s: message %d = {Type:%d To:%d Term:%d}, want {Type:%d To:%d Term:%d}",
				what, i, got[i].Type, got[i].To, got[i].Term,
				want[i].Type, want[i].To, want[i].Term)
		}
	}
}

// emptyRequeueRaft returns a follower raft with an empty outbound buffer.
func emptyRequeueRaft(ctx context.Context, t *testing.T) *raft {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done: %v", err)
	}
	r := newTestRaft(1)
	r.drainMessages()
	if len(r.msgs) != 0 || r.orderedHeld != 0 {
		t.Fatalf("setup: msgs=%d orderedHeld=%d, want empty", len(r.msgs), r.orderedHeld)
	}
	return r
}

// checkRequeueAheadOfNewer drains [VoteResp, ReplicateResp], queues a newer
// message, requeues the drained pair through requeue and checks that the
// next drainMessages returns the pair first, in order, then the newer one.
func checkRequeueAheadOfNewer(ctx context.Context, t *testing.T, r *raft, requeue func([]proto.Message)) {
	t.Helper()
	pair := []proto.Message{
		requeueTestMsg(proto.RequestVoteResp, 2, 101),
		requeueTestMsg(proto.ReplicateResp, 3, 102),
	}
	r.msgs = append(r.msgs, pair...)
	drained := append([]proto.Message(nil), r.drainMessages()...)
	assertRequeueOrder(ctx, t, "first drain", drained, pair)

	newer := requeueTestMsg(proto.ReplicateResp, 2, 103)
	r.msgs = append(r.msgs, newer)

	requeue(drained)
	if r.orderedHeld != 2 {
		t.Fatalf("orderedHeld after requeue = %d, want 2", r.orderedHeld)
	}

	got := r.drainMessages()
	assertRequeueOrder(ctx, t, "drain after requeue", got,
		[]proto.Message{pair[0], pair[1], newer})
	if r.orderedHeld != 0 {
		t.Fatalf("orderedHeld after drain = %d, want 0", r.orderedHeld)
	}
	if len(r.msgs) != 0 {
		t.Fatalf("msgs after drain = %d, want 0", len(r.msgs))
	}
}

// checkRequeueHeldThroughFreeDrain requeues [HeartbeatResp, Heartbeat],
// queues a newer HeartbeatResp, and checks that drainFreeOrderMessages(true)
// releases the Heartbeat and the newer HeartbeatResp but keeps the requeued
// HeartbeatResp, which the next drainMessages then returns.
func checkRequeueHeldThroughFreeDrain(ctx context.Context, t *testing.T, r *raft, requeue func([]proto.Message)) {
	t.Helper()
	heldResp := requeueTestMsg(proto.HeartbeatResp, 2, 201)
	hb := requeueTestMsg(proto.Heartbeat, 3, 202)
	requeue([]proto.Message{heldResp, hb})
	if r.orderedHeld != 2 {
		t.Fatalf("orderedHeld after requeue = %d, want 2", r.orderedHeld)
	}

	newerResp := requeueTestMsg(proto.HeartbeatResp, 3, 203)
	r.msgs = append(r.msgs, newerResp)

	free := append([]proto.Message(nil), r.drainFreeOrderMessages(true)...)
	assertRequeueOrder(ctx, t, "free-order drain", free,
		[]proto.Message{hb, newerResp})
	assertRequeueOrder(ctx, t, "kept after free-order drain", r.msgs,
		[]proto.Message{heldResp})
	if r.orderedHeld != 1 {
		t.Fatalf("orderedHeld after free-order drain = %d, want 1", r.orderedHeld)
	}

	got := r.drainMessages()
	assertRequeueOrder(ctx, t, "drain after free-order drain", got,
		[]proto.Message{heldResp})
	if r.orderedHeld != 0 {
		t.Fatalf("orderedHeld after drain = %d, want 0", r.orderedHeld)
	}
}

func TestRequeueMessages_PutsDrainedAheadOfNewer(t *testing.T) {
	ctx := context.Background()
	r := emptyRequeueRaft(ctx, t)
	checkRequeueAheadOfNewer(ctx, t, r, r.requeueMessages)
}

func TestRequeueMessages_NilAndEmptyAreNoOps(t *testing.T) {
	ctx := context.Background()
	r := emptyRequeueRaft(ctx, t)
	queued := []proto.Message{
		requeueTestMsg(proto.RequestVoteResp, 2, 301),
		requeueTestMsg(proto.Heartbeat, 3, 302),
	}
	r.msgs = append(r.msgs, queued...)
	r.orderedHeld = 1

	for _, in := range [][]proto.Message{nil, {}} {
		r.requeueMessages(in)
		assertRequeueOrder(ctx, t, "msgs after empty requeue", r.msgs, queued)
		if r.orderedHeld != 1 {
			t.Fatalf("orderedHeld after empty requeue = %d, want 1", r.orderedHeld)
		}
	}

	// An empty buffer stays empty and unheld too.
	r.drainMessages()
	r.requeueMessages(nil)
	r.requeueMessages([]proto.Message{})
	if len(r.msgs) != 0 || r.orderedHeld != 0 {
		t.Fatalf("empty buffer after empty requeue: msgs=%d orderedHeld=%d, want 0 and 0",
			len(r.msgs), r.orderedHeld)
	}
}

func TestRequeueMessages_FreeOrderDrainKeepsRequeuedHeartbeatResp(t *testing.T) {
	ctx := context.Background()
	r := emptyRequeueRaft(ctx, t)
	checkRequeueHeldThroughFreeDrain(ctx, t, r, r.requeueMessages)
}

func TestPeerRequeueMessages_MatchesRaftRequeue(t *testing.T) {
	ctx := context.Background()
	newPeer := func() *Peer {
		p := newTestPeer(t, testConfig(), proto.State{}, threeNodeMembership())
		p.r.drainMessages()
		if len(p.r.msgs) != 0 || p.r.orderedHeld != 0 {
			t.Fatalf("setup: msgs=%d orderedHeld=%d, want empty", len(p.r.msgs), p.r.orderedHeld)
		}
		return p
	}

	p := newPeer()
	checkRequeueAheadOfNewer(ctx, t, p.r, p.RequeueMessages)

	p = newPeer()
	p.RequeueMessages(nil)
	p.RequeueMessages([]proto.Message{})
	if len(p.r.msgs) != 0 || p.r.orderedHeld != 0 {
		t.Fatalf("peer after empty requeue: msgs=%d orderedHeld=%d, want 0 and 0",
			len(p.r.msgs), p.r.orderedHeld)
	}

	p = newPeer()
	checkRequeueHeldThroughFreeDrain(ctx, t, p.r, p.RequeueMessages)
}
