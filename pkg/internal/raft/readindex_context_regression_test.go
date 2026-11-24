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

// Unissued ReadIndex confirmation context
//
// ReadIndex without a lease confirms leadership by counting heartbeat responses
// that echo the correlation context stamped on the confirming heartbeat round. A
// response carrying ctx X is evidence of exactly one thing: the peer still
// regarded this replica as leader at a moment AFTER the read's index was
// recorded, because the heartbeat bearing X was sent after it. Everything the
// read's linearizability rests on is in that "after".
//
// handleHeartbeatResp took msg.Hint verbatim and acked every pending read with
// ctx <= it. The <= rule is correct for contexts this leader ISSUED — they are
// monotone within a tenure — but nothing bounded the value from above.
// r.readIndexCtx is the highest ever assigned (becomeLeader zeroes it,
// handleLeaderReadIndex is the only writer and only increments), so a response
// echoing anything above it corresponds to no heartbeat this leader ever sent,
// and under <= it acks EVERY pending read from one message — including reads
// whose confirming heartbeat has not been broadcast yet, since
// handleLeaderReadIndex defers the broadcast to flushReadIndex.
//
// With the leader's own pre-set self-ack, one such response is already a quorum
// of two in a three-voter shard. The read is then answered at a committed index
// no heartbeat round validated, which is precisely the guarantee ReadIndex
// exists to provide and the shape a stale leader serves a stale read through.

// leaderWithUnbroadcastRead builds a three-voter leader holding two pending
// ReadIndex requests: ctx 1, whose confirming heartbeat HAS been broadcast, and
// ctx 2, whose heartbeat has NOT (flushReadIndex has not run). The second is the
// one no honest response can possibly confirm yet.
func leaderWithUnbroadcastRead(t *testing.T) *raft {
	t.Helper()
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	if err := r.step(proto.Message{
		Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: 42,
	}); err != nil {
		t.Fatalf("first ReadIndex: %v", err)
	}
	r.flushReadIndex()
	r.drainMessages()

	if err := r.step(proto.Message{
		Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: 43,
	}); err != nil {
		t.Fatalf("second ReadIndex: %v", err)
	}

	if len(r.pendingReadIndex) != 2 || r.readIndexCtx != 2 {
		t.Fatalf("fixture: %d pending reads at ctx %d, want 2 and 2 — the slow "+
			"heartbeat-confirmation path is what this test is about, so a fixture "+
			"that took the lease or single-voter fast path proves nothing",
			len(r.pendingReadIndex), r.readIndexCtx)
	}
	if !r.readIndexBroadcastPending {
		t.Fatal("fixture: the second read's heartbeat is not pending, so ctx 2 is " +
			"not the unbroadcast context this test needs")
	}
	r.readStates = nil
	r.drainMessages()
	return r
}

// TestReadIndex_UnissuedContextCannotConfirmLeadership is the attacker test. One
// HeartbeatResp echoing a context far above anything this leader assigned would,
// under the bare <= rule, satisfy both pending reads at once — including the one
// whose confirming heartbeat has not left the leader.
func TestReadIndex_UnissuedContextCannotConfirmLeadership(t *testing.T) {
	quietLogs(t)

	r := leaderWithUnbroadcastRead(t)

	err := r.step(proto.Message{
		Type: proto.HeartbeatResp, From: 2, Term: r.term, Hint: 1 << 40,
	})

	if len(r.readStates) != 0 {
		t.Fatalf("%d read(s) resolved by a heartbeat response echoing context %d "+
			"when this leader has only ever issued up to %d: no heartbeat ever "+
			"carried that value, so the response proves nothing about leadership "+
			"at the moment either read's index was recorded, and the reads are "+
			"answered on a quorum that was never run — resolved states %+v",
			len(r.readStates), uint64(1)<<40, r.readIndexCtx, r.readStates)
	}
	if len(r.pendingReadIndex) != 2 {
		t.Fatalf("pending reads = %d, want 2: neither read may be retired by an "+
			"unissued context", len(r.pendingReadIndex))
	}
	for i := range r.pendingReadIndex {
		if r.pendingReadIndex[i].ackCount != 1 {
			t.Fatalf("pending read ctx %d has ackCount %d, want 1 (the leader's own "+
				"self-ack): the unissued context contributed a confirmation",
				r.pendingReadIndex[i].ctx, r.pendingReadIndex[i].ackCount)
		}
	}
	if !errors.Is(err, ErrReadIndexContextNotIssued) {
		t.Fatalf("step returned %v, want ErrReadIndexContextNotIssued", err)
	}
}

// TestReadIndex_IssuedContextStillConfirms is the primary liveness control, and
// it is what stops the guard from being a blanket refusal. The response echoes
// ctx 1 — a context this leader really did broadcast — so the FIRST read must
// resolve, while the second (ctx 2 > 1) correctly stays pending.
func TestReadIndex_IssuedContextStillConfirms(t *testing.T) {
	quietLogs(t)

	r := leaderWithUnbroadcastRead(t)

	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp, From: 2, Term: r.term, Hint: 1,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if len(r.readStates) != 1 || r.readStates[0].Key != 42 {
		t.Fatalf("readStates = %+v, want exactly the ctx-1 read (key 42): a "+
			"legitimately confirmed read must still resolve, or every linearizable "+
			"read on a lease-less shard hangs", r.readStates)
	}
	if len(r.pendingReadIndex) != 1 || r.pendingReadIndex[0].ctx != 2 {
		t.Fatalf("pending reads = %+v, want only ctx 2: the <= rule must still "+
			"retire exactly the reads at or below the echoed context",
			r.pendingReadIndex)
	}
}

// TestReadIndex_HighestIssuedContextIsAccepted pins the boundary. The guard is
// `msg.Hint > r.readIndexCtx`, and an off-by-one that refused the highest issued
// context would reject the response to the most recent heartbeat — the one that
// confirms every outstanding read — and stall reads under sustained load, where
// every heartbeat carries the newest context.
func TestReadIndex_HighestIssuedContextIsAccepted(t *testing.T) {
	quietLogs(t)

	r := leaderWithUnbroadcastRead(t)
	r.flushReadIndex()
	r.drainMessages()
	r.readStates = nil

	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp, From: 2, Term: r.term, Hint: r.readIndexCtx,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}
	if len(r.pendingReadIndex) != 0 || len(r.readStates) != 2 {
		t.Fatalf("pending=%d resolved=%d, want 0 and 2: the highest context this "+
			"leader issued is a context it issued, and must confirm every read at "+
			"or below it", len(r.pendingReadIndex), len(r.readStates))
	}
}

// TestReadIndex_RegularHeartbeatResponseIsUnaffected is the second liveness
// control. Ordinary heartbeats carry ctx 0, and their responses drive lease
// tracking, probe recovery and the stalled-window unblock in handleHeartbeatResp
// — all of which run before the ReadIndex block. Zero is at or below any issued
// context, so the guard must never see them.
func TestReadIndex_RegularHeartbeatResponseIsUnaffected(t *testing.T) {
	quietLogs(t)

	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()
	if r.readIndexCtx != 0 {
		t.Fatalf("fixture: readIndexCtx = %d, want 0 on a fresh tenure", r.readIndexCtx)
	}

	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp, From: 2, Term: r.term, Hint: 0,
	}); err != nil {
		t.Fatalf("step: %v: a regular heartbeat response must pass through even "+
			"before this leader has issued any ReadIndex context", err)
	}
}
