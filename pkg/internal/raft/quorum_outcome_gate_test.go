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

// ---------------------------------------------------------------------------
// ReadIndexResp is the wire form of a quorum outcome
// ---------------------------------------------------------------------------

// TestRaft_ReadIndexResp_RefusedFromNonLeader is the raft half of the
// ReadIndexResp sender gate. A handleReadIndexResp that resolved a pending
// linearizable read from ANY sender — no check that the response came from the
// leader this replica actually asked, no check that a matching read existed —
// would be exploitable: the correlation key is a plain monotonic counter, so it
// is guessable, and one forged message would serve a read at an index of the sender's choosing with no
// heartbeat quorum ever run, which is a silent linearizability violation and
// the worst possible failure for a read the caller asked to be linearizable.
func TestRaft_ReadIndexResp_RefusedFromNonLeader(t *testing.T) {
	r := atTerm(newTestRaft(1), 4)
	r.leaderID = 2
	r.readStates = nil

	err := r.step(proto.Message{
		Type:     proto.ReadIndexResp,
		From:     3, // not the leader this replica forwarded its ReadIndex to
		Term:     r.term,
		Hint:     42,
		LogIndex: 10,
	})
	if !errors.Is(err, ErrReadIndexRespNotFromLeader) {
		t.Fatalf("step(ReadIndexResp from a non-leader) = %v, want ErrReadIndexRespNotFromLeader", err)
	}
	if len(r.readStates) != 0 {
		t.Fatalf("a forged ReadIndexResp produced %d read states: a pending linearizable "+
			"read was resolved at an index nobody confirmed", len(r.readStates))
	}
}

// TestRaft_ReadIndexResp_RefusedWithNoKnownLeader covers the other unbound
// case: a replica with no leader has no outstanding ReadIndex to answer, so
// every response is spurious. Without the explicit noLeader test a message
// carrying From == 0 would match r.leaderID and be accepted.
func TestRaft_ReadIndexResp_RefusedWithNoKnownLeader(t *testing.T) {
	r := atTerm(newTestRaft(1), 4)
	r.leaderID = noLeader
	r.readStates = nil

	err := r.step(proto.Message{
		Type:     proto.ReadIndexResp,
		From:     noLeader,
		Term:     r.term,
		Hint:     42,
		LogIndex: 10,
	})
	if !errors.Is(err, ErrReadIndexRespNotFromLeader) {
		t.Fatalf("step(ReadIndexResp with no known leader) = %v, want ErrReadIndexRespNotFromLeader", err)
	}
	if len(r.readStates) != 0 {
		t.Fatalf("a ReadIndexResp was accepted with no leader to have sent it (%d read states)",
			len(r.readStates))
	}
}

// TestRaft_ReadIndexResp_AcceptsAnIndexAheadOfTheLocalCommit pins the check
// that must NOT be added.
//
// The leader answers with ITS committed index, which is by construction at or
// ahead of the follower's — that gap is the entire point of ReadIndex, and the
// follower waits for lastApplied to reach the returned index before serving the
// read (hostEngineCallback.OnReadyToRead). Rejecting LogIndex > r.log.committed
// as "stale or forged" would therefore refuse the common case: every
// linearizable read taken while the follower is even one entry behind.
func TestRaft_ReadIndexResp_AcceptsAnIndexAheadOfTheLocalCommit(t *testing.T) {
	r := atTerm(newTestRaft(1), 4)
	r.leaderID = 2
	r.readStates = nil

	ahead := r.log.committed + 100
	if err := r.step(proto.Message{
		Type:     proto.ReadIndexResp,
		From:     2,
		Term:     r.term,
		Hint:     7,
		LogIndex: ahead,
	}); err != nil {
		t.Fatalf("step(ReadIndexResp from the leader): %v", err)
	}
	if len(r.readStates) != 1 {
		t.Fatalf("got %d read states, want 1: a leader read index ahead of this "+
			"follower's commit was refused, which is every read taken while the "+
			"follower lags", len(r.readStates))
	}
	if r.readStates[0].Index != ahead {
		t.Fatalf("read state index = %d, want %d", r.readStates[0].Index, ahead)
	}
}

// ---------------------------------------------------------------------------
// The leadership-transfer exemption from the CheckQuorum vote guard
// ---------------------------------------------------------------------------

// TestRaft_DropHighTermVote_TransferHintCappedAtNextTerm pins the bound on the
// transfer exemption.
//
// Hint == From is a value the SENDER chooses, and RequestVote is deliberately
// not bound to committed membership on the receive path (a member that
// recovered at a new address must still be able to campaign during quorum
// loss). Unbounded, the exemption let any certificate holder send
// RequestVote{Hint: From, Term: r.term + 1000}, walk past a live leader lease,
// and force every recipient to becomeFollower at an arbitrary term — a leader
// step-down on demand, repeatable for as long as the sender kept sending.
//
// r.term+1 is exactly what a real transferee campaigns at: it takes its term
// from the leader's own TimeoutNow and becomeCandidate adds one. A replica far
// enough behind for a bigger gap has not heard from the leader inside the
// election timeout, so the lease clause is already false and the vote is not
// dropped for an unrelated reason.
func TestRaft_DropHighTermVote_TransferHintCappedAtNextTerm(t *testing.T) {
	tests := []struct {
		name     string
		term     func(r *raft) uint64
		wantDrop bool
	}{
		{
			name:     "a two-term jump is not a transfer and is dropped",
			term:     func(r *raft) uint64 { return r.term + 2 },
			wantDrop: true,
		},
		{
			name:     "an arbitrary term jump is dropped",
			term:     func(r *raft) uint64 { return r.term + 1000 },
			wantDrop: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := followerWithKnownLeader(t, 5)
			msg := proto.Message{Type: proto.RequestVote, From: 3, Hint: 3, Term: tt.term(r)}
			if got := r.dropHighTermVote(msg); got != tt.wantDrop {
				t.Fatalf("dropHighTermVote(transfer-hinted vote at term %d, local term %d) = %v, want %v",
					msg.Term, r.term, got, tt.wantDrop)
			}
		})
	}
}

// TestRaft_DropHighTermVote_ForgedTransferCannotUnseatALiveLeader asserts the
// OBSERVABLE effect, not just the predicate: a follower with a fresh leader
// lease must not advance its term or forget its leader because of a
// transfer-hinted vote at an arbitrary term.
func TestRaft_DropHighTermVote_ForgedTransferCannotUnseatALiveLeader(t *testing.T) {
	r := followerWithKnownLeader(t, 5)
	termBefore, leaderBefore := r.term, r.leaderID

	if err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Hint:     3, // claims to be a leadership transfer
		Term:     r.term + 1000,
		LogIndex: r.log.lastIndex(),
		LogTerm:  mustLastTerm(t, r),
	}); err != nil {
		t.Fatalf("step: %v", err)
	}

	if r.term != termBefore {
		t.Fatalf("term advanced %d -> %d on a forged transfer vote: the live leader is "+
			"forced to step down on demand", termBefore, r.term)
	}
	if r.leaderID != leaderBefore {
		t.Fatalf("leaderID = %d, want %d: the follower forgot a leader it is still "+
			"hearing from", r.leaderID, leaderBefore)
	}
}

// ---------------------------------------------------------------------------
// One address may name only one member
// ---------------------------------------------------------------------------

// TestValidateConfigChange_RejectsDuplicateMemberAddress is quorum forgery with
// no forged message and no certificate abuse at all: add a SECOND replica ID at
// an address an existing member already holds and the single host behind it
// legitimately authenticates as both. It then sends ReplicateResp{From: 2} and
// ReplicateResp{From: 4}, both pass every inbound check, both advance a remote
// match index, and tryCommit counts them as two independent replicas. The same
// host also casts two votes.
func TestValidateConfigChange_RejectsDuplicateMemberAddress(t *testing.T) {
	const sharedAddr = "10.0.0.5:5001"
	base := proto.Membership{Addresses: map[uint64]string{
		1: "10.0.0.1:5001",
		2: sharedAddr,
	}}

	for _, tt := range []struct {
		name   string
		change proto.ConfigChange
	}{
		{"AddNode", proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: sharedAddr}},
		{"AddWitness", proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 4, Address: sharedAddr}},
		{"AddNonVoting", proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 4, Address: sharedAddr}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestPeer(t, testConfig(), proto.State{}, base)
			if err := p.ValidateConfigChange(tt.change); !errors.Is(err, ErrDuplicateMemberAddress) {
				t.Fatalf("ValidateConfigChange(%s at an address replica 2 already holds) = %v, "+
					"want ErrDuplicateMemberAddress: one host would hold two committed "+
					"identities and two quorum votes", tt.name, err)
			}
		})
	}
}

// TestValidateConfigChange_AllowsADepartedReplicasAddress pins the other side:
// a REMOVED replica's address is free to reuse. Its tombstone denies it
// regardless of who holds the address, and recycling a machine into a new
// replica ID is ordinary operations.
func TestValidateConfigChange_AllowsADepartedReplicasAddress(t *testing.T) {
	const recycledAddr = "10.0.0.5:5001"
	p := newTestPeer(t, testConfig(), proto.State{}, proto.Membership{
		Addresses: map[uint64]string{1: "10.0.0.1:5001"},
		Removed:   map[uint64]bool{2: true},
	})

	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: recycledAddr}
	if err := p.ValidateConfigChange(cc); err != nil {
		t.Fatalf("ValidateConfigChange(reuse of a removed replica's address) = %v, want nil: "+
			"a machine can never be recycled into a new replica ID", err)
	}
}

// TestMemberAddress_ReportsASharedAddressAsNonExclusive covers the receive-side
// fail-closed for a configuration that is ALREADY in the duplicate state —
// seeded through StartShard before that validation existed, or restored from an
// older snapshot's membership. Validation refuses to create it; the sender
// binding must not assume validation ran.
func TestMemberAddress_ReportsASharedAddressAsNonExclusive(t *testing.T) {
	const sharedAddr, ownAddr = "10.0.0.5:5001", "10.0.0.1:5001"
	p := newTestPeer(t, testConfig(), proto.State{}, proto.Membership{
		Addresses: map[uint64]string{1: ownAddr, 2: sharedAddr, 4: sharedAddr},
	})

	for _, replicaID := range []uint64{2, 4} {
		binding, ok := p.MemberBinding(replicaID)
		addr, exclusive := binding.Address, binding.Exclusive
		if !ok || addr != sharedAddr {
			t.Fatalf("MemberAddress(%d) = (%q, _, %v), want (%q, _, true)",
				replicaID, addr, ok, sharedAddr)
		}
		if exclusive {
			t.Fatalf("MemberAddress(%d) reports the shared address %q as exclusive: the "+
				"one host behind it is authorized to speak as both replicas",
				replicaID, sharedAddr)
		}
	}

	// The uninvolved member is unaffected — the failure is scoped to the shared
	// address, not to the whole configuration.
	if binding, ok := p.MemberBinding(1); !ok || !binding.Exclusive || binding.Address != ownAddr {
		t.Fatalf("MemberBinding(1) = (%q, exclusive %v, ok %v), want (%q, true, true)",
			binding.Address, binding.Exclusive, ok, ownAddr)
	}
}
