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

// ReadIndex data structure unit tests. These test the ReadIndex lifecycle:
// request addition, quorum confirmation, state clearing on role change,
// and input validation.

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestReadIndex_RequestCanBeAdded verifies that a ReadIndex request can
// be added and is tracked in pendingReadIndex until quorum is confirmed.
func TestReadIndex_RequestCanBeAdded(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Send a ReadIndex request.
	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42, // client key
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have a pending read index entry.
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read index, got %d", len(r.pendingReadIndex))
	}
	pending := r.pendingReadIndex[0]
	if pending.key != 42 {
		t.Fatalf("expected key=42, got %d", pending.key)
	}
	if pending.index != r.log.committed {
		t.Fatalf("expected index=%d, got %d", r.log.committed, pending.index)
	}
	// Self-ack should be pre-set.
	if pending.ackCount != 1 {
		t.Fatalf("expected ackCount=1 (self), got %d", pending.ackCount)
	}
}

// TestReadIndex_DuplicateContextRejected verifies that the same client key
// can be added multiple times (they get different ctx values). Each request
// is independent and tracked separately.
func TestReadIndex_DuplicateContextRejected(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Send two ReadIndex requests with the same key.
	err := r.step(proto.Message{Type: proto.ReadIndex, Hint: 42})
	if err != nil {
		t.Fatalf("unexpected error on first request: %v", err)
	}
	err = r.step(proto.Message{Type: proto.ReadIndex, Hint: 42})
	if err != nil {
		t.Fatalf("unexpected error on second request: %v", err)
	}

	// Both should be tracked as separate pending entries with different ctx.
	if len(r.pendingReadIndex) != 2 {
		t.Fatalf("expected 2 pending read index entries, got %d", len(r.pendingReadIndex))
	}
	if r.pendingReadIndex[0].ctx == r.pendingReadIndex[1].ctx {
		t.Fatal("expected different ctx values for duplicate key requests")
	}
	if r.pendingReadIndex[0].key != 42 || r.pendingReadIndex[1].key != 42 {
		t.Fatal("expected both entries to have key=42")
	}
}

// TestReadIndex_LeaderConfirmation verifies that a ReadIndex request is
// confirmed when the leader receives heartbeat responses from a quorum.
func TestReadIndex_LeaderConfirmation(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Submit a ReadIndex request.
	err := r.step(proto.Message{Type: proto.ReadIndex, Hint: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(r.pendingReadIndex))
	}

	// Flush to broadcast heartbeats with the ReadIndex ctx.
	r.flushReadIndex()
	msgs := r.drainMessages()

	// Find the heartbeat ctx.
	var hbCtx uint64
	for _, m := range msgs {
		if m.Type == proto.Heartbeat {
			hbCtx = m.Hint
			break
		}
	}
	if hbCtx == 0 {
		t.Fatal("expected heartbeat with non-zero ctx")
	}

	// Respond from peer 2 with matching ctx (self + peer 2 = quorum).
	err = r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The pending read should be resolved.
	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected 0 pending after quorum, got %d", len(r.pendingReadIndex))
	}

	// readStates should contain the result.
	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 100 {
		t.Fatalf("expected read state key=100, got %d", r.readStates[0].Key)
	}
}

// TestReadIndex_ResetOnStateChange verifies that pending ReadIndex state
// is cleared when the node transitions from leader to follower.
func TestReadIndex_ResetOnStateChange(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Add pending reads.
	err := r.step(proto.Message{Type: proto.ReadIndex, Hint: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = r.step(proto.Message{Type: proto.ReadIndex, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(r.pendingReadIndex) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(r.pendingReadIndex))
	}

	// Step down to follower (higher term message).
	r.becomeFollower(r.term+1, 2)

	// Pending reads should be cleared.
	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected 0 pending after role change, got %d", len(r.pendingReadIndex))
	}
	// Broadcast pending flag should be cleared.
	if r.readIndexBroadcastPending {
		t.Fatal("expected readIndexBroadcastPending cleared on role change")
	}
}

// TestReadIndex_InputIndexValidation verifies that ReadIndex is rejected
// when the leader has not yet committed an entry in its current term.
func TestReadIndex_InputIndexValidation(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	// Do NOT call commitNoOp -- the noop is not committed yet.
	r.drainMessages()

	err := r.step(proto.Message{Type: proto.ReadIndex, Hint: 1})
	if !errors.Is(err, ErrReadIndexNotReady) {
		t.Fatalf("expected ErrReadIndexNotReady before noop committed, got %v", err)
	}

	// Now commit the noop.
	commitNoOp(r)
	r.drainMessages()

	// ReadIndex should now work.
	err = r.step(proto.Message{Type: proto.ReadIndex, Hint: 2})
	if err != nil {
		t.Fatalf("expected ReadIndex to succeed after noop committed, got %v", err)
	}
}

// TestReadIndex_OverloadProtection verifies that the pendingReadIndex
// queue has a capacity limit to prevent unbounded memory growth.
func TestReadIndex_OverloadProtection(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Fill up to the limit.
	for i := 0; i < maxPendingReadIndex; i++ {
		err := r.step(proto.Message{Type: proto.ReadIndex, Hint: uint64(i)})
		if err != nil {
			t.Fatalf("unexpected error at request %d: %v", i, err)
		}
	}

	// The next request should be rejected.
	err := r.step(proto.Message{Type: proto.ReadIndex, Hint: 99999})
	if !errors.Is(err, ErrReadIndexOverloaded) {
		t.Fatalf("expected ErrReadIndexOverloaded, got %v", err)
	}
}

// TestReadIndex_ReadStatesOverloadProtection verifies that the readStates
// slice has a capacity limit.
func TestReadIndex_ReadStatesOverloadProtection(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Fill readStates to the limit.
	for i := 0; i < maxReadStates; i++ {
		r.readStates = append(r.readStates, proto.ReadyToRead{Key: uint64(i), Index: 1})
	}

	// addReadState should return error when full.
	err := r.addReadState(proto.ReadyToRead{Key: 99999, Index: 1})
	if !errors.Is(err, ErrReadIndexOverloaded) {
		t.Fatalf("expected ErrReadIndexOverloaded, got %v", err)
	}
}
