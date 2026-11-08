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
	"math"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// readIndexResps returns the ReadIndexResp messages r has queued.
func readIndexResps(r *raft) []proto.Message {
	var out []proto.Message
	for _, m := range r.msgs {
		if m.Type == proto.ReadIndexResp {
			out = append(out, m)
		}
	}
	return out
}

// requireReject asserts r answered the forwarded read key from replica to
// with exactly one reject, and served nothing.
func requireReject(t *testing.T, r *raft, to, key uint64) {
	t.Helper()
	resps := readIndexResps(r)
	if len(resps) != 1 {
		t.Fatalf("want exactly one ReadIndexResp, got %+v", resps)
	}
	m := resps[0]
	if !m.Reject || m.To != to || m.Hint != key {
		t.Fatalf("want a reject to %d for key %d, got %+v", to, key, m)
	}
	if m.LogIndex != math.MaxUint64 {
		t.Fatalf("a reject carries LogIndex %d, want MaxUint64: a receiver that "+
			"ignored Reject could serve it", m.LogIndex)
	}
	if len(r.readStates) != 0 || len(r.pendingReadIndex) != 0 {
		t.Fatalf("a refused read was queued: readStates=%+v pending=%d",
			r.readStates, len(r.pendingReadIndex))
	}
}

// TestForwardedRead_RejectIsFreeOrder: while the engine's commit pipeline is
// busy it transmits only what drainFreeOrderMessages returns, and an unready
// leader's pipeline is busy for exactly the window its refusals cover. The
// reject must come out of that drain, not wait in r.msgs for the next
// GetUpdate (#376).
func TestForwardedRead_RejectIsFreeOrder(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)
	if r.hasCommittedEntryAtCurrentTerm() {
		t.Fatal("fixture: the leader committed its term-opening entry")
	}
	r.msgs = r.msgs[:0]

	if err := r.step(proto.Message{Type: proto.ReadIndex, From: 2, Term: r.term, Hint: 7}); err != nil {
		t.Fatalf("an answered refusal is not a step error: %v", err)
	}
	var sent []proto.Message
	for _, m := range r.drainFreeOrderMessages(false) {
		if m.Type == proto.ReadIndexResp {
			sent = append(sent, m)
		}
	}
	if len(sent) != 1 || !sent[0].Reject || sent[0].To != 2 || sent[0].Hint != 7 {
		t.Fatalf("free-order drain returned ReadIndexResp %+v, want one reject to 2 for key 7", sent)
	}
	if left := readIndexResps(r); len(left) != 0 {
		t.Fatalf("the reject was also left in r.msgs: %+v", left)
	}
}

// TestForwardedRead_TransferringLeaderRejects covers the refusal a read-ready
// follower cannot predict from its own state: a leader in a leadership
// transfer refuses reads, and must say so to the follower.
func TestForwardedRead_TransferringLeaderRejects(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)
	commitNoOp(r)
	r.transferTarget = 3
	r.msgs = r.msgs[:0]

	if err := r.step(proto.Message{Type: proto.ReadIndex, From: 2, Term: r.term, Hint: 8}); err != nil {
		t.Fatalf("an answered refusal is not a step error: %v", err)
	}
	requireReject(t, r, 2, 8)
}

// TestForwardedRead_LocalRefusalStaysAnError: the leader's own read is
// refused through the step error, which the engine already routes to the
// caller; it sends nothing.
func TestForwardedRead_LocalRefusalStaysAnError(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)
	r.msgs = r.msgs[:0]

	err := r.step(proto.Message{Type: proto.ReadIndex, From: 1, Hint: 9, Origin: proto.OriginLocal})
	if !errors.Is(err, ErrReadIndexNotReady) {
		t.Fatalf("got %v, want ErrReadIndexNotReady", err)
	}
	if resps := readIndexResps(r); len(resps) != 0 {
		t.Fatalf("a local refusal sent %+v", resps)
	}
}

// TestForwardedRead_UnreadyFollowerRefusesLocally: a follower that has not
// seen an entry of the current term commit refuses its own read where the
// caller sees it, and still relays another replica's read to the leader,
// whose reject is the only way that replica can learn of a refusal.
func TestForwardedRead_UnreadyFollowerRefusesLocally(t *testing.T) {
	r := atTerm(newTestRaft(1), 4)
	r.leaderID = 2
	r.msgs = r.msgs[:0]

	err := r.step(proto.Message{Type: proto.ReadIndex, From: 1, Hint: 10, Origin: proto.OriginLocal})
	if !errors.Is(err, ErrReadIndexNotReady) {
		t.Fatalf("got %v, want ErrReadIndexNotReady", err)
	}
	if len(r.msgs) != 0 {
		t.Fatalf("a refused read was forwarded: %+v", r.msgs)
	}

	if err := r.step(proto.Message{Type: proto.ReadIndex, From: 3, Term: r.term, Hint: 11}); err != nil {
		t.Fatalf("a relayed read was refused: %v", err)
	}
	if len(r.msgs) != 1 || r.msgs[0].Type != proto.ReadIndex || r.msgs[0].To != 2 {
		t.Fatalf("the relayed read was not forwarded to the leader: %+v", r.msgs)
	}
}

// TestForwardedRead_RejectIsNeverServed: the follower turns the leader's
// reject into a refused key, never a read state, whatever LogIndex it
// carries, and accepts it only from its leader.
func TestForwardedRead_RejectIsNeverServed(t *testing.T) {
	for _, logIndex := range []uint64{0, 5, math.MaxUint64} {
		r := atTerm(newTestRaft(1), 4)
		r.leaderID = 2
		r.readStates = nil

		err := r.step(proto.Message{
			Type: proto.ReadIndexResp, From: 2, Term: r.term,
			Hint: 12, LogIndex: logIndex, Reject: true,
		})
		if err != nil {
			t.Fatalf("LogIndex %d: %v", logIndex, err)
		}
		if len(r.readStates) != 0 {
			t.Fatalf("LogIndex %d: a reject was served as %+v", logIndex, r.readStates)
		}
		if len(r.readRejects) != 1 || r.readRejects[0] != 12 {
			t.Fatalf("LogIndex %d: readRejects = %v, want [12]", logIndex, r.readRejects)
		}
	}

	r := atTerm(newTestRaft(1), 4)
	r.leaderID = 2
	err := r.step(proto.Message{
		Type: proto.ReadIndexResp, From: 3, Term: r.term, Hint: 13, Reject: true,
	})
	if !errors.Is(err, ErrReadIndexRespNotFromLeader) {
		t.Fatalf("a reject from a non-leader: got %v, want ErrReadIndexRespNotFromLeader", err)
	}
	if len(r.readRejects) != 0 {
		t.Fatalf("a reject from a non-leader failed a read: %v", r.readRejects)
	}
}
