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
// request addition and quorum confirmation. Clearing on role change, the
// no-op readiness gate and the overload limits are tested through step in
// raft_test.go.

import (
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
		Type: proto.ReadIndex, Origin: proto.OriginLocal,
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
	err := r.step(proto.Message{Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: 42})
	if err != nil {
		t.Fatalf("unexpected error on first request: %v", err)
	}
	err = r.step(proto.Message{Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: 42})
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
	err := r.step(proto.Message{Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: 100})
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
