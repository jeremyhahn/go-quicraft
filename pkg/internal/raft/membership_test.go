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

func testMembership() *membership {
	return newMembership(proto.Membership{
		ConfigChangeID: 0,
		Addresses: map[uint64]string{
			1: "addr1",
			2: "addr2",
			3: "addr3",
		},
		Observers: map[uint64]string{},
		Witnesses: map[uint64]string{},
		Removed:   map[uint64]bool{},
	})
}

// --- newMembership ---

func TestMembership_NewMembership_CopiesMaps(t *testing.T) {
	src := proto.Membership{
		ConfigChangeID: 5,
		Addresses: map[uint64]string{
			1: "addr1",
		},
		Observers: map[uint64]string{
			10: "obs1",
		},
		Witnesses: map[uint64]string{
			20: "wit1",
		},
		Removed: map[uint64]bool{
			99: true,
		},
	}
	m := newMembership(src)

	// Mutating the source should not affect the membership.
	src.Addresses[1] = "mutated"
	if m.addresses[1] != "addr1" {
		t.Fatalf("expected addr1, got %s", m.addresses[1])
	}
	if m.configChangeID != 5 {
		t.Fatalf("expected configChangeID 5, got %d", m.configChangeID)
	}
	if !m.isObserver(10) {
		t.Fatal("expected replica 10 to be observer")
	}
	if !m.isWitness(20) {
		t.Fatal("expected replica 20 to be witness")
	}
	if !m.removed[99] {
		t.Fatal("expected replica 99 to be removed")
	}
}

func TestMembership_NewMembership_NilMaps(t *testing.T) {
	m := newMembership(proto.Membership{})

	if m.addresses != nil {
		t.Fatal("expected nil addresses")
	}
	if m.observers != nil {
		t.Fatal("expected nil observers")
	}
	if m.witnesses != nil {
		t.Fatal("expected nil witnesses")
	}
	if m.removed != nil {
		t.Fatal("expected nil removed")
	}
}

// --- numVotingMembers ---

func TestMembership_NumVotingMembers_VotersOnly(t *testing.T) {
	m := testMembership()
	if m.numVotingMembers() != 3 {
		t.Fatalf("expected 3 voting members, got %d", m.numVotingMembers())
	}
}

func TestMembership_NumVotingMembers_WithWitnesses(t *testing.T) {
	m := testMembership()
	m.witnesses = map[uint64]string{10: "wit1"}
	if m.numVotingMembers() != 4 {
		t.Fatalf("expected 4 voting members, got %d", m.numVotingMembers())
	}
}

// --- numDataBearingMembers ---

func TestMembership_NumDataBearingMembers_ExcludesWitnesses(t *testing.T) {
	m := testMembership()
	m.witnesses = map[uint64]string{10: "wit1"}
	if len(m.addresses) != 3 {
		t.Fatalf("expected 3 data-bearing members, got %d", len(m.addresses))
	}
}

func TestMembership_NumDataBearingMembers_VotersOnly(t *testing.T) {
	m := testMembership()
	if len(m.addresses) != 3 {
		t.Fatalf("expected 3 data-bearing members, got %d", len(m.addresses))
	}
}

// --- isVoter ---

func TestMembership_IsVoter_True(t *testing.T) {
	m := testMembership()
	if !m.isVoter(1) {
		t.Fatal("expected replica 1 to be voter")
	}
}

func TestMembership_IsVoter_False(t *testing.T) {
	m := testMembership()
	if m.isVoter(99) {
		t.Fatal("expected replica 99 to not be voter")
	}
}

// --- isObserver ---

func TestMembership_IsObserver_True(t *testing.T) {
	m := testMembership()
	m.observers = map[uint64]string{10: "obs1"}
	if !m.isObserver(10) {
		t.Fatal("expected replica 10 to be observer")
	}
}

func TestMembership_IsObserver_False(t *testing.T) {
	m := testMembership()
	if m.isObserver(1) {
		t.Fatal("expected replica 1 to not be observer")
	}
}

// --- isWitness ---

func TestMembership_IsWitness_True(t *testing.T) {
	m := testMembership()
	m.witnesses = map[uint64]string{20: "wit1"}
	if !m.isWitness(20) {
		t.Fatal("expected replica 20 to be witness")
	}
}

func TestMembership_IsWitness_False(t *testing.T) {
	m := testMembership()
	if m.isWitness(1) {
		t.Fatal("expected replica 1 to not be witness")
	}
}

// --- isMember ---

func TestMembership_IsMember_Voter(t *testing.T) {
	m := testMembership()
	if !m.isMember(1) {
		t.Fatal("expected replica 1 to be member")
	}
}

func TestMembership_IsMember_Observer(t *testing.T) {
	m := testMembership()
	m.observers = map[uint64]string{10: "obs1"}
	if !m.isMember(10) {
		t.Fatal("expected replica 10 to be member")
	}
}

func TestMembership_IsMember_Witness(t *testing.T) {
	m := testMembership()
	m.witnesses = map[uint64]string{20: "wit1"}
	if !m.isMember(20) {
		t.Fatal("expected replica 20 to be member")
	}
}

func TestMembership_IsMember_NotMember(t *testing.T) {
	m := testMembership()
	if m.isMember(99) {
		t.Fatal("expected replica 99 to not be member")
	}
}

// --- AddNode ---

func TestMembership_AddNode_Success(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.isVoter(4) {
		t.Fatal("expected replica 4 to be voter")
	}
	if m.addresses[4] != "addr4" {
		t.Fatalf("expected address addr4, got %s", m.addresses[4])
	}
	if m.configChangeID != 10 {
		t.Fatalf("expected configChangeID 10, got %d", m.configChangeID)
	}
}

// TestMembership_AddNode_AlreadyVoter pins the APPLY path's convergence on a
// committed AddNode whose target the membership already records as a voter.
//
// Refusing it was not a safe no-op: configChangeID stops advancing at the
// skipped entry, so this replica reports a stale ConfigChangeID as the CAS
// token for every later membership change while its peers have moved on.
// validate() still rejects the same change at propose time — see
// TestMembership_Validate_AddNodeAlreadyVoter.
func TestMembership_AddNode_AlreadyVoter(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 1,
		Address:   "addr1-new",
	}
	if err := m.apply(cc, 10); err != nil {
		t.Fatalf("apply(AddNode) over an existing voter: %v", err)
	}
	if m.addresses[1] != "addr1-new" {
		t.Fatalf("addresses[1] = %q, want the committed entry's address", m.addresses[1])
	}
	if m.configChangeID != 10 {
		t.Fatalf("configChangeID = %d, want 10; a skipped entry leaves this replica "+
			"reporting a CAS token its peers have moved past", m.configChangeID)
	}
}

func TestMembership_AddNode_EmptyAddress(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrEmptyAddress) {
		t.Fatalf("expected ErrEmptyAddress, got %v", err)
	}
}

func TestMembership_AddNode_PromotesObserver(t *testing.T) {
	m := testMembership()
	m.observers = map[uint64]string{10: "obs1"}
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 10,
		Address:   "obs1-promoted",
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.isVoter(10) {
		t.Fatal("expected replica 10 to be voter after promotion")
	}
	if m.isObserver(10) {
		t.Fatal("expected replica 10 to not be observer after promotion")
	}
	if m.addresses[10] != "obs1-promoted" {
		t.Fatalf("expected address obs1-promoted, got %s", m.addresses[10])
	}
}

func TestMembership_AddNode_RemovedNode(t *testing.T) {
	m := testMembership()
	m.removed = map[uint64]bool{99: true}
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 99,
		Address:   "addr99",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("expected ErrNodeRemoved, got %v", err)
	}
}

func TestMembership_AddNode_ZeroReplicaID(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 0,
		Address:   "addr0",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrZeroReplicaID) {
		t.Fatalf("expected ErrZeroReplicaID, got %v", err)
	}
}

func TestMembership_AddNode_AlreadyWitness(t *testing.T) {
	m := testMembership()
	m.witnesses = map[uint64]string{20: "wit1"}
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 20,
		Address:   "addr20",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
	}
}

// --- RemoveNode ---

func TestMembership_RemoveNode_Success(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 1,
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.isVoter(1) {
		t.Fatal("expected replica 1 to not be voter after removal")
	}
	if !m.removed[1] {
		t.Fatal("expected replica 1 to be in removed set")
	}
	if m.configChangeID != 10 {
		t.Fatalf("expected configChangeID 10, got %d", m.configChangeID)
	}
}

func TestMembership_RemoveNode_LastVoter(t *testing.T) {
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
	})
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 1,
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrRemoveLastFullReplica) {
		t.Fatalf("expected ErrRemoveLastFullReplica, got %v", err)
	}
}

func TestMembership_RemoveNode_NotFound(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 99,
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestMembership_RemoveNode_Observer(t *testing.T) {
	m := testMembership()
	m.observers = map[uint64]string{10: "obs1"}
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 10,
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.isObserver(10) {
		t.Fatal("expected replica 10 to not be observer after removal")
	}
	if !m.removed[10] {
		t.Fatal("expected replica 10 to be in removed set")
	}
}

func TestMembership_RemoveNode_Witness(t *testing.T) {
	m := testMembership()
	m.witnesses = map[uint64]string{20: "wit1"}
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 20,
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.isWitness(20) {
		t.Fatal("expected replica 20 to not be witness after removal")
	}
	if !m.removed[20] {
		t.Fatal("expected replica 20 to be in removed set")
	}
}

func TestMembership_RemoveNode_LastWitness_WithVoters(t *testing.T) {
	m := testMembership()
	m.witnesses = map[uint64]string{20: "wit1"}
	// Remove the witness. This is fine because there are still 3 voters.
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 20,
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMembership_RemoveNode_LastVotingWitness(t *testing.T) {
	// A cluster with only 1 witness and 0 voters: removing the witness
	// should fail because it is the last voting member.
	m := newMembership(proto.Membership{
		Witnesses: map[uint64]string{1: "wit1"},
	})
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 1,
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrRemoveLastVoter) {
		t.Fatalf("expected ErrRemoveLastVoter, got %v", err)
	}
}

// --- AddNonVoting ---

func TestMembership_AddNonVoting_Success(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 10,
		Address:   "obs1",
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.isObserver(10) {
		t.Fatal("expected replica 10 to be observer")
	}
}

// TestMembership_AddNonVoting_DemotesSeededVoter is the C1 unit-level
// regression. Host.StartShard seeds Membership.Addresses from the caller's
// members map on a restart that recovered no snapshot, so an observer the
// operator lists — as it must, join=false requires self in the map — is seeded
// as a full VOTER. The committed AddNonVoting that gave it its real role is
// still in the log and is re-applied on restart; refusing it because the target
// "already exists" left the observer a voter in its own membership durably,
// because nothing else repairs the seed.
func TestMembership_AddNonVoting_DemotesSeededVoter(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 1,
		Address:   "addr1",
	}
	if err := m.apply(cc, 10); err != nil {
		t.Fatalf("apply(AddNonVoting) over a seeded voter: %v", err)
	}
	if m.isVoter(1) {
		t.Fatalf("replica 1 is still a full voter after a committed AddNonVoting: %+v", m.addresses)
	}
	if !m.isObserver(1) {
		t.Fatalf("replica 1 is not an observer after a committed AddNonVoting: %+v", m.observers)
	}
	if m.configChangeID != 10 {
		t.Fatalf("configChangeID = %d, want 10", m.configChangeID)
	}
}

// TestMembership_AddNonVoting_AlreadyWitness pins the refusal that remains: a
// witness role can only have been set by an earlier COMMITTED AddWitness, so
// every replica evaluates this identically and refusing cannot diverge them.
func TestMembership_AddNonVoting_AlreadyWitness(t *testing.T) {
	m := testMembership()
	m.witnesses = map[uint64]string{20: "wit1"}
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 20,
		Address:   "obs20",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
	}
	if m.isObserver(20) {
		t.Fatal("a witness must not be demoted to observer by apply")
	}
}

func TestMembership_AddNonVoting_EmptyAddress(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 10,
		Address:   "",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrEmptyAddress) {
		t.Fatalf("expected ErrEmptyAddress, got %v", err)
	}
}

func TestMembership_AddNonVoting_RemovedNode(t *testing.T) {
	m := testMembership()
	m.removed = map[uint64]bool{99: true}
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 99,
		Address:   "obs99",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("expected ErrNodeRemoved, got %v", err)
	}
}

func TestMembership_AddNonVoting_NilObserversMap(t *testing.T) {
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
	})
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 10,
		Address:   "obs1",
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.isObserver(10) {
		t.Fatal("expected replica 10 to be observer")
	}
}

// --- AddWitness ---

func TestMembership_AddWitness_Success(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 20,
		Address:   "wit1",
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.isWitness(20) {
		t.Fatal("expected replica 20 to be witness")
	}
}

// TestMembership_AddWitness_DemotesSeededVoter is the C1/H1 unit-level
// regression for the witness half. A witness listed in the restart members map
// is seeded into Addresses — the full voter set — and the leader then selects
// sendAppend over sendWitnessAppend for it, replicating the complete command
// stream to a replica whose purpose is to hold none of it. The committed
// AddWitness in the log must put it back.
func TestMembership_AddWitness_DemotesSeededVoter(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 1,
		Address:   "addr1",
	}
	if err := m.apply(cc, 10); err != nil {
		t.Fatalf("apply(AddWitness) over a seeded voter: %v", err)
	}
	if m.isVoter(1) {
		t.Fatalf("replica 1 is still a full voter after a committed AddWitness: %+v", m.addresses)
	}
	if !m.isWitness(1) {
		t.Fatalf("replica 1 is not a witness after a committed AddWitness: %+v", m.witnesses)
	}
	if m.configChangeID != 10 {
		t.Fatalf("configChangeID = %d, want 10", m.configChangeID)
	}
}

// TestMembership_AddWitness_AlreadyObserver pins the refusal that remains: an
// observer role can only have been set by an earlier COMMITTED AddNonVoting,
// so every replica evaluates this identically and refusing cannot diverge them.
func TestMembership_AddWitness_AlreadyObserver(t *testing.T) {
	m := testMembership()
	m.observers = map[uint64]string{10: "obs1"}
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 10,
		Address:   "wit10",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
	}
	if m.isWitness(10) {
		t.Fatal("an observer must not be converted to a witness by apply")
	}
}

func TestMembership_AddWitness_EmptyAddress(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 20,
		Address:   "",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrEmptyAddress) {
		t.Fatalf("expected ErrEmptyAddress, got %v", err)
	}
}

func TestMembership_AddWitness_RemovedNode(t *testing.T) {
	m := testMembership()
	m.removed = map[uint64]bool{99: true}
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 99,
		Address:   "wit99",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("expected ErrNodeRemoved, got %v", err)
	}
}

func TestMembership_AddWitness_NilWitnessesMap(t *testing.T) {
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
	})
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 20,
		Address:   "wit1",
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.isWitness(20) {
		t.Fatal("expected replica 20 to be witness")
	}
}

// --- validate ---

func TestMembership_Validate_ValidAddNode(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := m.validate(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The original membership should not be mutated by validate.
	if m.isVoter(4) {
		t.Fatal("validate should not mutate the original membership")
	}
}

func TestMembership_Validate_InvalidZeroReplicaID(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 0,
		Address:   "addr0",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrZeroReplicaID) {
		t.Fatalf("expected ErrZeroReplicaID, got %v", err)
	}
}

func TestMembership_Validate_InvalidType(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      999,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrInvalidConfigChangeType) {
		t.Fatalf("expected ErrInvalidConfigChangeType, got %v", err)
	}
}

// --- validate/apply equivalence ---
// These tests verify that validate() returns the same accept/reject
// decisions as the old copy-then-apply approach for every config change
// type, and that validate() never mutates the membership.

func TestMembership_Validate_AddNodePromotion_NoMutation(t *testing.T) {
	m := testMembership()
	m.observers = map[uint64]string{10: "obs1"}
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 10,
		Address:   "obs1-promoted",
	}
	// Validate should succeed (observer promotion is allowed).
	err := m.validate(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Membership must be unchanged: observer still present, not promoted.
	if !m.isObserver(10) {
		t.Fatal("validate must not remove replica from observers")
	}
	if m.isVoter(10) {
		t.Fatal("validate must not add replica to addresses")
	}
}

func TestMembership_Validate_AddNodeAlreadyVoter(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 1,
		Address:   "addr1",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
	}
}

func TestMembership_Validate_AddNodeAlreadyWitness(t *testing.T) {
	m := testMembership()
	m.witnesses = map[uint64]string{20: "wit1"}
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 20,
		Address:   "addr20",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
	}
}

func TestMembership_Validate_AddNodeRemoved(t *testing.T) {
	m := testMembership()
	m.removed = map[uint64]bool{99: true}
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 99,
		Address:   "addr99",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("expected ErrNodeRemoved, got %v", err)
	}
}

func TestMembership_Validate_AddNodeEmptyAddress(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrEmptyAddress) {
		t.Fatalf("expected ErrEmptyAddress, got %v", err)
	}
}

func TestMembership_Validate_RemoveNodeSuccess_NoMutation(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 1,
	}
	err := m.validate(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Membership must be unchanged: node 1 still a voter, not removed.
	if !m.isVoter(1) {
		t.Fatal("validate must not remove replica from addresses")
	}
	if m.removed[1] {
		t.Fatal("validate must not add replica to removed set")
	}
}

func TestMembership_Validate_RemoveNodeLastVoter(t *testing.T) {
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
	})
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 1,
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrRemoveLastFullReplica) {
		t.Fatalf("expected ErrRemoveLastFullReplica, got %v", err)
	}
}

func TestMembership_Validate_RemoveNodeLastWitness(t *testing.T) {
	m := newMembership(proto.Membership{
		Witnesses: map[uint64]string{1: "wit1"},
	})
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 1,
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrRemoveLastVoter) {
		t.Fatalf("expected ErrRemoveLastVoter, got %v", err)
	}
}

func TestMembership_Validate_RemoveNodeNotFound(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 99,
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestMembership_Validate_RemoveObserver_NoMutation(t *testing.T) {
	m := testMembership()
	m.observers = map[uint64]string{10: "obs1"}
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 10,
	}
	err := m.validate(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.isObserver(10) {
		t.Fatal("validate must not remove observer")
	}
}

func TestMembership_Validate_AddNonVotingSuccess_NoMutation(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 10,
		Address:   "obs1",
	}
	err := m.validate(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.isObserver(10) {
		t.Fatal("validate must not add observer to membership")
	}
}

func TestMembership_Validate_AddNonVotingAlreadyMember(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 1,
		Address:   "addr1",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
	}
}

func TestMembership_Validate_AddNonVotingRemoved(t *testing.T) {
	m := testMembership()
	m.removed = map[uint64]bool{99: true}
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 99,
		Address:   "obs99",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("expected ErrNodeRemoved, got %v", err)
	}
}

func TestMembership_Validate_AddNonVotingEmptyAddress(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 10,
		Address:   "",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrEmptyAddress) {
		t.Fatalf("expected ErrEmptyAddress, got %v", err)
	}
}

func TestMembership_Validate_AddWitnessSuccess_NoMutation(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 20,
		Address:   "wit1",
	}
	err := m.validate(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.isWitness(20) {
		t.Fatal("validate must not add witness to membership")
	}
}

func TestMembership_Validate_AddWitnessAlreadyMember(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 1,
		Address:   "addr1",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
	}
}

func TestMembership_Validate_AddWitnessRemoved(t *testing.T) {
	m := testMembership()
	m.removed = map[uint64]bool{99: true}
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 99,
		Address:   "wit99",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("expected ErrNodeRemoved, got %v", err)
	}
}

func TestMembership_Validate_AddWitnessEmptyAddress(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 20,
		Address:   "",
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrEmptyAddress) {
		t.Fatalf("expected ErrEmptyAddress, got %v", err)
	}
}

func TestMembership_Validate_ConfigChangeIDUnchanged(t *testing.T) {
	m := testMembership()
	m.configChangeID = 42
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := m.validate(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.configChangeID != 42 {
		t.Fatalf("validate must not change configChangeID, got %d", m.configChangeID)
	}
}

// TestMembership_Validate_MatchesApply_AllTypes pins the relationship between
// the propose-time gate and the deterministic apply.
//
// The safety-critical direction is that apply() must NEVER refuse a change
// validate() accepted: apply runs on already-COMMITTED entries and nothing
// re-delivers one, so its refusal is final and leaves this replica holding a
// membership the rest of the shard does not. That is asserted for every case
// as a subset property, on top of the exact per-case expectation below.
//
// The converse does not hold, deliberately. validate() refuses an add whose
// target is already a member; apply() converges on the role the committed
// entry names, because the membership it applies to may be a provisional
// bootstrap-map seed rather than the product of committed entries alone (see
// the "Convergent application" comment in membership.go).
func TestMembership_Validate_MatchesApply_AllTypes(t *testing.T) {
	cases := []struct {
		name string
		cc   proto.ConfigChange
		// wantValidate/wantApply are the exact expected errors; nil means the
		// change is accepted.
		wantValidate error
		wantApply    error
	}{
		{"AddNode-new", proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "a4"}, nil, nil},
		// Converges: an already-listed voter is re-stated, not refused.
		{"AddNode-existing", proto.ConfigChange{Type: proto.AddNode, ReplicaID: 1, Address: "a1"}, ErrNodeAlreadyExists, nil},
		{"AddNode-empty-addr", proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: ""}, ErrEmptyAddress, ErrEmptyAddress},
		{"RemoveNode-exists", proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 1}, nil, nil},
		{"RemoveNode-missing", proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 99}, ErrNodeNotFound, ErrNodeNotFound},
		{"AddNonVoting-new", proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 10, Address: "o10"}, nil, nil},
		// Converges: a seeded voter is demoted to the observer role the
		// committed entry names.
		{"AddNonVoting-exists", proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 1, Address: "a1"}, ErrNodeAlreadyExists, nil},
		{"AddWitness-new", proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 20, Address: "w20"}, nil, nil},
		// Converges: a seeded voter is moved to the witness role.
		{"AddWitness-exists", proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 1, Address: "a1"}, ErrNodeAlreadyExists, nil},
		{"InvalidType", proto.ConfigChange{Type: 999, ReplicaID: 4, Address: "a4"}, ErrInvalidConfigChangeType, ErrInvalidConfigChangeType},
		{"ZeroReplica", proto.ConfigChange{Type: proto.AddNode, ReplicaID: 0, Address: "a0"}, ErrZeroReplicaID, ErrZeroReplicaID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := testMembership()
			validateErr := m.validate(tc.cc)
			if !errors.Is(validateErr, tc.wantValidate) {
				t.Fatalf("validate returned %v, want %v", validateErr, tc.wantValidate)
			}

			// Apply on a separate copy to get the "ground truth" error.
			mCopy := testMembership()
			applyErr := mCopy.apply(tc.cc, 100)
			if !errors.Is(applyErr, tc.wantApply) {
				t.Fatalf("apply returned %v, want %v", applyErr, tc.wantApply)
			}

			// The subset property: whatever apply refuses, validate must
			// refuse too, or a change can be proposed and committed that this
			// replica then declines to apply.
			if applyErr != nil && !errors.Is(validateErr, applyErr) {
				t.Fatalf("apply refused a change validate accepted: apply=%v validate=%v",
					applyErr, validateErr)
			}
		})
	}
}

// --- apply with invalid type ---

func TestMembership_Apply_InvalidType(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      999,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrInvalidConfigChangeType) {
		t.Fatalf("expected ErrInvalidConfigChangeType, got %v", err)
	}
}

func TestMembership_Apply_UpdatesConfigChangeID(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := m.apply(cc, 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.configChangeID != 42 {
		t.Fatalf("expected configChangeID 42, got %d", m.configChangeID)
	}
}

// --- copyStringMap / copyBoolMap ---

func TestMembership_CopyStringMap_Nil(t *testing.T) {
	result := copyStringMap(nil)
	if result != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestMembership_CopyStringMap_Isolation(t *testing.T) {
	src := map[uint64]string{1: "a", 2: "b"}
	dst := copyStringMap(src)
	dst[1] = "mutated"
	if src[1] != "a" {
		t.Fatal("copy should not affect source")
	}
}

func TestMembership_CopyBoolMap_Nil(t *testing.T) {
	result := copyBoolMap(nil)
	if result != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestMembership_CopyBoolMap_Isolation(t *testing.T) {
	src := map[uint64]bool{1: true, 2: false}
	dst := copyBoolMap(src)
	dst[1] = false
	if !src[1] {
		t.Fatal("copy should not affect source")
	}
}

// --- Sequence of operations ---

func TestMembership_AddThenRemove(t *testing.T) {
	m := testMembership()

	// Add a new node.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error adding node: %v", err)
	}
	if !m.isVoter(4) {
		t.Fatal("expected replica 4 to be voter")
	}

	// Remove the node.
	cc = proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 4,
	}
	err = m.apply(cc, 11)
	if err != nil {
		t.Fatalf("unexpected error removing node: %v", err)
	}
	if m.isVoter(4) {
		t.Fatal("expected replica 4 to not be voter after removal")
	}
	if !m.removed[4] {
		t.Fatal("expected replica 4 to be in removed set")
	}

	// Cannot re-add a removed node.
	cc = proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4-new",
	}
	err = m.apply(cc, 12)
	if !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("expected ErrNodeRemoved, got %v", err)
	}
}

func TestMembership_AddObserverThenPromote(t *testing.T) {
	m := testMembership()

	// Add observer.
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 10,
		Address:   "obs1",
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.isObserver(10) {
		t.Fatal("expected replica 10 to be observer")
	}

	// Promote to voter via AddNode.
	cc = proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 10,
		Address:   "obs1-promoted",
	}
	err = m.apply(cc, 11)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.isVoter(10) {
		t.Fatal("expected replica 10 to be voter after promotion")
	}
	if m.isObserver(10) {
		t.Fatal("expected replica 10 to not be observer after promotion")
	}
}

// --- isRemoved edge case ---

func TestMembership_IsRemoved_NotInMap(t *testing.T) {
	m := testMembership()
	if m.removed[999] {
		t.Fatal("expected non-existent replica to not be removed")
	}
}

func TestMembership_IsRemoved_NilMap(t *testing.T) {
	m := newMembership(proto.Membership{})
	if m.removed[1] {
		t.Fatal("expected isRemoved to be false with nil removed map")
	}
}

// --- RemoveNode with nil removed map ---

func TestMembership_RemoveNode_NilRemovedMap(t *testing.T) {
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
			2: "addr2",
		},
	})
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 1,
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.removed[1] {
		t.Fatal("expected replica 1 to be in removed set")
	}
}

// --- membershipSnapshot ---

func TestMembershipSnapshot_IsMember_Voter(t *testing.T) {
	m := testMembership()
	snap := m.snapshot()
	if !snap.isMember(1) {
		t.Fatal("expected replica 1 to be member in snapshot")
	}
	if !snap.isMember(2) {
		t.Fatal("expected replica 2 to be member in snapshot")
	}
}

func TestMembershipSnapshot_IsMember_Observer(t *testing.T) {
	m := testMembership()
	m.observers = map[uint64]string{10: "obs1"}
	snap := m.snapshot()
	if !snap.isMember(10) {
		t.Fatal("expected observer 10 to be member in snapshot")
	}
}

func TestMembershipSnapshot_IsMember_Witness(t *testing.T) {
	m := testMembership()
	m.witnesses = map[uint64]string{20: "wit1"}
	snap := m.snapshot()
	if !snap.isMember(20) {
		t.Fatal("expected witness 20 to be member in snapshot")
	}
}

func TestMembershipSnapshot_IsMember_NotMember(t *testing.T) {
	m := testMembership()
	snap := m.snapshot()
	if snap.isMember(999) {
		t.Fatal("expected replica 999 to not be member in snapshot")
	}
}

func TestMembershipSnapshot_ToProto_ReturnsDeepCopy(t *testing.T) {
	m := testMembership()
	snap := m.snapshot()
	p := snap.toProto()

	// Mutating the proto should not affect the snapshot.
	p.Addresses[1] = "mutated"
	if snap.addresses[1] != "addr1" {
		t.Fatalf("expected addr1 in snapshot, got %s", snap.addresses[1])
	}
}

func TestMembershipSnapshot_ToProto_PreservesConfigChangeID(t *testing.T) {
	m := testMembership()
	m.configChangeID = 42
	snap := m.snapshot()
	p := snap.toProto()
	if p.ConfigChangeID != 42 {
		t.Fatalf("expected ConfigChangeID 42, got %d", p.ConfigChangeID)
	}
}

func TestMembershipSnapshot_IsolatedFromMutableMembership(t *testing.T) {
	m := testMembership()
	snap := m.snapshot()

	// Mutate the original membership after snapshot.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Snapshot should not see the mutation.
	if snap.isMember(4) {
		t.Fatal("snapshot should not reflect mutations to the mutable membership")
	}
	// Original should see it.
	if !m.isMember(4) {
		t.Fatal("mutable membership should see the mutation")
	}
}

func TestMembershipSnapshot_NilMaps(t *testing.T) {
	m := newMembership(proto.Membership{})
	snap := m.snapshot()
	if snap.isMember(1) {
		t.Fatal("expected no members in snapshot from nil maps")
	}
	p := snap.toProto()
	if p.Addresses != nil {
		t.Fatal("expected nil Addresses in proto from nil-maps snapshot")
	}
}

func TestMembershipSnapshot_SuccessiveSnapshots(t *testing.T) {
	m := testMembership()
	snap1 := m.snapshot()

	// Apply a config change.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	snap2 := m.snapshot()

	// snap1 should not see replica 4.
	if snap1.isMember(4) {
		t.Fatal("snap1 should not see replica 4")
	}
	// snap2 should see replica 4.
	if !snap2.isMember(4) {
		t.Fatal("snap2 should see replica 4")
	}
}

// ---------------------------------------------------------------------------
// R2: Witness-only cluster prevention
// ---------------------------------------------------------------------------

func TestMembership_RemoveNode_LastFullReplicaWithWitnesses(t *testing.T) {
	// Cluster: 1 full replica + 2 witnesses. Removing the full replica
	// would create a witness-only cluster where no member can become
	// leader. Must return ErrRemoveLastFullReplica.
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
		Witnesses: map[uint64]string{10: "wit1", 11: "wit2"},
	})
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 1,
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrRemoveLastFullReplica) {
		t.Fatalf("expected ErrRemoveLastFullReplica, got %v", err)
	}
	// Verify the membership was not mutated.
	if !m.isVoter(1) {
		t.Fatal("expected replica 1 to still be voter after failed removal")
	}
}

func TestMembership_RemoveNode_WitnessWithFullReplicasSucceeds(t *testing.T) {
	// Cluster: 2 full replicas + 1 witness. Removing the witness
	// is safe because full replicas remain.
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2"},
		Witnesses: map[uint64]string{10: "wit1"},
	})
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 10,
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error removing witness: %v", err)
	}
	if m.isWitness(10) {
		t.Fatal("expected witness 10 to be removed")
	}
	if !m.removed[10] {
		t.Fatal("expected witness 10 to be in removed set")
	}
}

func TestMembership_RemoveNode_FullReplicaWhenMultipleExist(t *testing.T) {
	// Cluster: 3 full replicas + 1 witness. Removing one full replica
	// is safe because others remain.
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		Witnesses: map[uint64]string{10: "wit1"},
	})
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 1,
	}
	err := m.apply(cc, 10)
	if err != nil {
		t.Fatalf("unexpected error removing voter with remaining replicas: %v", err)
	}
	if m.isVoter(1) {
		t.Fatal("expected replica 1 to be removed")
	}
}

func TestMembership_Validate_RemoveLastFullReplicaWithWitnesses(t *testing.T) {
	// Validate path: same check as handleRemoveNode.
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
		Witnesses: map[uint64]string{10: "wit1"},
	})
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 1,
	}
	err := m.validate(cc)
	if !errors.Is(err, ErrRemoveLastFullReplica) {
		t.Fatalf("expected ErrRemoveLastFullReplica from validate, got %v", err)
	}
}

// TestMembership_VotingCapEnforced is the regression test for the voting cap:
// adding a voting member (full replica or witness) past maxVotingMembers must be
// rejected at PROPOSE/VALIDATE time (validateAddNode/validateAddWitness) so the
// over-cap change never enters the log — preventing the later leader-side
// invariant panic when the ReadIndex ackBits word overflows.
//
// Critically, the apply-path handlers (handleAddNode/handleAddWitness) must NOT
// re-check the cap: apply() runs on already-committed entries (replication,
// post-truncation rebuildMembershipFromLog), and rejecting a committed change
// would cause replicas to diverge from the agreed membership — a Raft safety
// violation. The validate gate is the single enforcement point.
func TestMembership_VotingCapEnforced(t *testing.T) {
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{},
		Observers: map[uint64]string{},
		Witnesses: map[uint64]string{},
		Removed:   map[uint64]bool{},
	})
	// Fill exactly to the cap with full voting members.
	for i := uint64(1); i <= maxVotingMembers; i++ {
		cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: i, Address: "addr"}
		if err := handleAddNode(m, cc); err != nil {
			t.Fatalf("add voter %d: unexpected error %v", i, err)
		}
	}
	if m.numVotingMembers() != maxVotingMembers {
		t.Fatalf("voting members = %d, want %d", m.numVotingMembers(), maxVotingMembers)
	}

	// The 65th voter must be rejected at validate (propose) time.
	over := proto.ConfigChange{Type: proto.AddNode, ReplicaID: maxVotingMembers + 1, Address: "addr"}
	if err := validateAddNode(m, over); !errors.Is(err, ErrTooManyVotingMembers) {
		t.Fatalf("validateAddNode over cap = %v, want ErrTooManyVotingMembers", err)
	}
	// The apply-path handler must accept it: a committed change is always
	// applied so replicas converge. (Rejecting here would diverge membership.)
	if err := handleAddNode(m, over); err != nil {
		t.Fatalf("handleAddNode over cap = %v, want nil (committed changes always apply)", err)
	}

	// A witness (also a voting member) must likewise be rejected at validate time,
	// but accepted by the apply-path handler.
	w := proto.ConfigChange{Type: proto.AddWitness, ReplicaID: maxVotingMembers + 2, Address: "addr"}
	if err := validateAddWitness(m, w); !errors.Is(err, ErrTooManyVotingMembers) {
		t.Fatalf("validateAddWitness over cap = %v, want ErrTooManyVotingMembers", err)
	}
	if err := handleAddWitness(m, w); err != nil {
		t.Fatalf("handleAddWitness over cap = %v, want nil (committed changes always apply)", err)
	}

	// Observers do not count toward the voting cap and must still be allowed.
	obs := proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: maxVotingMembers + 3, Address: "addr"}
	if err := handleAddNonVoting(m, obs); err != nil {
		t.Fatalf("handleAddNonVoting at voting cap: unexpected error %v", err)
	}
}

// --- Replicated witness policy (C1) ---

// membershipWith builds a membership with the given full-replica and witness
// counts and the given replicated policy for validator testing.
func membershipWith(fullReplicas, witnesses int, maxWitnesses uint64, requireFullReplicaMajority bool) *membership {
	addr := map[uint64]string{}
	for i := 1; i <= fullReplicas; i++ {
		addr[uint64(i)] = "addr"
	}
	wit := map[uint64]string{}
	for i := 0; i < witnesses; i++ {
		wit[uint64(100+i)] = "waddr"
	}
	return newMembership(proto.Membership{
		Addresses:                  addr,
		Witnesses:                  wit,
		MaxWitnesses:               maxWitnesses,
		RequireFullReplicaMajority: requireFullReplicaMajority,
	})
}

// TestMembership_WitnessPolicy_RequireFullReplicaMajority verifies that the
// default RequireFullReplicaMajority policy rejects a witness add that would
// leave the full replicas unable to form a quorum on their own, while allowing
// adds that preserve the full-replica majority.
func TestMembership_WitnessPolicy_RequireFullReplicaMajority(t *testing.T) {
	add := proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 9, Address: "w"}

	// 3 full + 1 witness -> after add 3 full + 2 witness = 5 voting, quorum 3,
	// full 3 >= 3: ALLOWED.
	if err := validateAddWitness(membershipWith(3, 1, 0, true), add); err != nil {
		t.Fatalf("3 full + 2nd witness should be allowed, got %v", err)
	}
	// 3 full + 2 witness -> after add = 6 voting, quorum 4, full 3 < 4: REJECT.
	if err := validateAddWitness(membershipWith(3, 2, 0, true), add); !errors.Is(err, ErrBreaksFullReplicaMajority) {
		t.Fatalf("3 full + 3rd witness should break full-replica majority, got %v", err)
	}
	// 2 full + 1 witness -> after add = 4 voting, quorum 3, full 2 < 3: REJECT.
	if err := validateAddWitness(membershipWith(2, 1, 0, true), add); !errors.Is(err, ErrBreaksFullReplicaMajority) {
		t.Fatalf("2 full + 2nd witness should break full-replica majority, got %v", err)
	}
	// 2 full + 0 witness -> after add = 3 voting, quorum 2, full 2 >= 2: ALLOWED
	// (a valid tiebreaker config where full replicas still form a quorum).
	if err := validateAddWitness(membershipWith(2, 0, 0, true), add); err != nil {
		t.Fatalf("2 full + 1st witness should be allowed, got %v", err)
	}
}

// TestMembership_WitnessPolicy_AllowWhenDisabled verifies that disabling
// RequireFullReplicaMajority permits witness-as-tiebreaker deployments that the
// default policy would reject (the quorums stay unified, so data is still safe).
func TestMembership_WitnessPolicy_AllowWhenDisabled(t *testing.T) {
	add := proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 9, Address: "w"}
	if err := validateAddWitness(membershipWith(2, 1, 0, false), add); err != nil {
		t.Fatalf("witness add should be allowed when RequireFullReplicaMajority is off, got %v", err)
	}
}

// TestMembership_WitnessPolicy_MaxWitnesses verifies the MaxWitnesses cap.
func TestMembership_WitnessPolicy_MaxWitnesses(t *testing.T) {
	add := proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 9, Address: "w"}

	// Cap of 2: adding a 2nd witness (1 existing) is allowed.
	if err := validateAddWitness(membershipWith(3, 1, 2, false), add); err != nil {
		t.Fatalf("2nd witness under cap 2 should be allowed, got %v", err)
	}
	// Cap of 2: adding a 3rd witness (2 existing) is rejected.
	if err := validateAddWitness(membershipWith(3, 2, 2, false), add); !errors.Is(err, ErrTooManyWitnesses) {
		t.Fatalf("3rd witness over cap 2 should be rejected, got %v", err)
	}
	// Cap of 0 means unbounded.
	if err := validateAddWitness(membershipWith(5, 4, 0, false), add); err != nil {
		t.Fatalf("unbounded cap should allow witness add, got %v", err)
	}
}

// TestMembership_WitnessPolicy_RemoveNodePreservesFullReplicaMajority verifies
// that RequireFullReplicaMajority also gates full-replica removal: removing a
// full replica must not leave the remaining full replicas unable to form a
// quorum over the voting set. Without this check, a remove could silently
// produce exactly the witness-heavy membership the policy rejects on add.
func TestMembership_WitnessPolicy_RemoveNodePreservesFullReplicaMajority(t *testing.T) {
	// 3 full + 2 witness (full 3 >= quorum 3): removing full replica 3 would
	// leave 2 full + 2 witness = 4 voting, quorum 3, full 2 < 3: REJECT.
	rmFull := proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 3}
	if err := validateRemoveNode(membershipWith(3, 2, 0, true), rmFull); !errors.Is(err, ErrBreaksFullReplicaMajority) {
		t.Fatalf("removing full replica from 3F+2W should break full-replica majority, got %v", err)
	}
	// Same removal is allowed when the policy is disabled.
	if err := validateRemoveNode(membershipWith(3, 2, 0, false), rmFull); err != nil {
		t.Fatalf("removal should be allowed with policy off, got %v", err)
	}
	// 4 full + 1 witness: removing a full replica leaves 3 full + 1 witness =
	// 4 voting, quorum 3, full 3 >= 3: ALLOWED.
	if err := validateRemoveNode(membershipWith(4, 1, 0, true), rmFull); err != nil {
		t.Fatalf("removal preserving full-replica majority should be allowed, got %v", err)
	}
	// Removing a WITNESS only improves the ratio: 3 full + 2 witness minus
	// witness 100 -> ALLOWED even under the policy.
	rmWitness := proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 100}
	if err := validateRemoveNode(membershipWith(3, 2, 0, true), rmWitness); err != nil {
		t.Fatalf("removing a witness should always be allowed by the policy, got %v", err)
	}
	// No witnesses present: policy imposes nothing on full-replica removal.
	if err := validateRemoveNode(membershipWith(3, 0, 0, true), rmFull); err != nil {
		t.Fatalf("removal with no witnesses should be allowed, got %v", err)
	}
}

// TestMembership_UpdateWitnessPolicy_ApplyAndValidate verifies the
// UpdateWitnessPolicy config change: apply replaces the replicated policy
// unconditionally (deterministic replay), while validate rejects a new
// policy the CURRENT membership does not satisfy.
func TestMembership_UpdateWitnessPolicy_ApplyAndValidate(t *testing.T) {
	// Apply: policy fields replaced, configChangeID advanced.
	m := membershipWith(3, 1, 0, false)
	cc := proto.ConfigChange{
		Type:                       proto.UpdateWitnessPolicy,
		MaxWitnesses:               2,
		RequireFullReplicaMajority: true,
	}
	if err := m.validate(cc); err != nil {
		t.Fatalf("consistent policy update should validate, got %v", err)
	}
	if err := m.apply(cc, 42); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if m.maxWitnesses != 2 || !m.requireFullReplicaMajority {
		t.Fatalf("policy not applied: max=%d rfrm=%v", m.maxWitnesses, m.requireFullReplicaMajority)
	}
	if m.configChangeID != 42 {
		t.Fatalf("expected configChangeID 42, got %d", m.configChangeID)
	}
	// The new policy now governs subsequent changes: a 3rd witness would
	// exceed... cap 2 with 1 existing allows one more, then majority rule
	// kicks in for 3F+2W -> 3rd witness (6 voting, quorum 4 > full 3).
	add := proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 9, Address: "w"}
	if err := m.validate(add); err != nil {
		t.Fatalf("2nd witness should be allowed under new policy, got %v", err)
	}

	// Validate: tightening the cap below the current witness count is
	// rejected — reshape first, then tighten.
	m2 := membershipWith(3, 2, 0, false)
	if err := m2.validate(proto.ConfigChange{
		Type:         proto.UpdateWitnessPolicy,
		MaxWitnesses: 1,
	}); !errors.Is(err, ErrTooManyWitnesses) {
		t.Fatalf("cap below current witness count must be rejected, got %v", err)
	}
	// Validate: enabling RequireFullReplicaMajority on a membership that
	// violates it is rejected (2 full + 2 witness: quorum 3 > full 2).
	m3 := membershipWith(2, 2, 0, false)
	if err := m3.validate(proto.ConfigChange{
		Type:                       proto.UpdateWitnessPolicy,
		RequireFullReplicaMajority: true,
	}); !errors.Is(err, ErrBreaksFullReplicaMajority) {
		t.Fatalf("enabling RFRM on violating membership must be rejected, got %v", err)
	}
	// Apply is unconditional even for a policy the membership violates
	// (committed replay must never diverge).
	if err := m3.apply(proto.ConfigChange{
		Type:                       proto.UpdateWitnessPolicy,
		RequireFullReplicaMajority: true,
	}, 50); err != nil {
		t.Fatalf("apply must be unconditional, got %v", err)
	}
	if !m3.requireFullReplicaMajority {
		t.Fatal("policy not applied on replay path")
	}
}

// TestMembership_PolicySurvivesSnapshotRoundTrip verifies the policy flows
// through snapshot() -> toProto() -> newMembership().
func TestMembership_PolicySurvivesSnapshotRoundTrip(t *testing.T) {
	m := membershipWith(3, 1, 5, true)
	restored := newMembership(m.snapshot().toProto())
	if restored.maxWitnesses != 5 || !restored.requireFullReplicaMajority {
		t.Fatalf("policy lost in round trip: max=%d rfrm=%v",
			restored.maxWitnesses, restored.requireFullReplicaMajority)
	}
}

// TestLeaderRejectsConfigChangeWhileAnEarlierOneIsUnapplied pins the
// one-config-change-at-a-time invariant (PhD thesis 4.1) against the way it was
// silently lost.
//
// Peer.Commit clears pendingConfigChange for every committed EntryConfigChange
// it applies, without asking whether a LATER config change is still unapplied --
// and a catching-up follower legitimately applies a batch containing several.
// handleLeaderPropose then re-derived the flag from the authoritative bound only
// when it was ALREADY true, so a wrongly-cleared flag was never repaired and the
// next config change was accepted while one was outstanding.
//
// Beyond overlapping configurations, the propose-time validators
// (ErrRemoveLastFullReplica, ErrRemoveLastVoter, ErrBreaksFullReplicaMajority)
// then run against a membership that excludes the pending change -- i.e. they
// vet the wrong configuration.
func TestLeaderRejectsConfigChangeWhileAnEarlierOneIsUnapplied(t *testing.T) {
	cfg := testConfig()
	p := newTestPeerWithLog(t, cfg, contiguousEntries(200),
		proto.State{Term: 1, Commit: 150}, threeNodeMembership())

	// The log holds an unapplied config change high in the tail (index 200) and
	// the apply frontier is far behind it -- the ordinary state of a replica
	// that just won an election while still catching up on apply.
	p.r.log.confChangeIndex = 200
	p.r.log.processed = 50

	p.r.becomeCandidate()
	p.r.becomeLeader()
	if !p.r.pendingConfigChange {
		t.Fatalf("fixture: becomeLeader must arm the flag (confChangeIndex=%d processed=%d)",
			p.r.log.confChangeIndex, p.r.log.processed)
	}

	// The apply pipeline now applies a batch [51..150] that contains an EARLIER
	// committed config change. This is what Peer.Commit does for each one.
	p.r.pendingConfigChange = false
	p.r.log.processed = 150

	// The authoritative bound still says a config change is outstanding.
	if !p.r.hasConfigChangeToApply() {
		t.Fatalf("fixture: a config change at 200 must still be unapplied "+
			"(confChangeIndex=%d processed=%d)",
			p.r.log.confChangeIndex, p.r.log.processed)
	}

	// The operator retries the membership change against the new leader.
	cc := proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 3}
	buf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	err := p.r.step(proto.Message{
		Type:    proto.Propose,
		From:    1,
		Origin:  proto.OriginLocal,
		Entries: []proto.Entry{{Type: proto.EntryConfigChange, Cmd: buf}},
	})

	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("leader ACCEPTED a second config change (err=%v) while the one at "+
			"index %d is still unapplied (processed=%d). The single-pending "+
			"invariant handleLeaderPropose documents is not enforced: Peer.Commit "+
			"cleared the flag on an EARLIER change and handleLeaderPropose only "+
			"re-derives it from hasConfigChangeToApply() when it is already true.",
			err, p.r.log.confChangeIndex, p.r.log.processed)
	}
}

// TestVoteDeniedToCandidateOutsideTheConfiguration pins the guard that
// compensates for the receive path no longer being an allow-list.
//
// Once any authenticated peer can reach raft, refusing the VOTE is what stops a
// replica that is not part of this shard from winning a term and becoming its
// leader. The configuration test belongs here and not on the receive path
// because denying a vote can only ever cost liveness, never safety, whereas
// dropping replication on a stale configuration wedges the replica permanently.
//
// hashicorp/raft places the identical test in requestVote/requestPreVote.
func TestVoteDeniedToCandidateOutsideTheConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name     string
		voteType uint64
		respType uint64
	}{
		{"RequestVote", proto.RequestVote, proto.RequestVoteResp},
		{"RequestPreVote", proto.RequestPreVote, proto.RequestPreVoteResp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPeer(t, testConfigWithID(1, 1), proto.State{Term: 5},
				proto.Membership{Addresses: map[uint64]string{
					1: "a", 2: "b", 3: "c",
				}})
			p.DrainMessages()

			// Replica 42 is in no role in this configuration.
			if err := p.Handle(proto.Message{
				Type: tc.voteType, From: 42, To: 1, ShardID: 1, Term: 6,
				LogIndex: 100, LogTerm: 5,
			}); err != nil {
				t.Fatalf("Handle: %v", err)
			}

			var replied bool
			for _, m := range p.DrainMessages() {
				if m.Type != tc.respType {
					continue
				}
				replied = true
				if !m.Reject {
					t.Fatalf("granted a vote to replica 42, which this configuration "+
						"does not contain in any role: an outsider that collects a "+
						"quorum becomes the leader of a shard it is not part of "+
						"(membership=%v)", p.GetMembership().Addresses)
				}
			}
			if !replied {
				t.Fatal("no vote response at all: the candidate cannot distinguish a " +
					"denial from a lost message and will retry indefinitely")
			}
		})
	}
}

// TestVoteGrantedWhenTheConfigurationIsIncomplete is the liveness half. A
// replica still joining holds a known-incomplete configuration -- it may name
// only itself, having applied its own AddNode without yet holding the entry
// naming the rest of the shard. Applying the guard in that state would make it
// deny every real member and strand a shard whose quorum needs its vote. An
// incomplete configuration is not evidence of non-membership.
func TestVoteGrantedWhenTheConfigurationIsIncomplete(t *testing.T) {
	p := newTestPeer(t, testConfigWithID(1, 6), proto.State{Term: 5},
		proto.Membership{Addresses: map[uint64]string{6: "self"}})
	p.MarkJoining()
	if !p.joining.Load() {
		t.Fatal("fixture: the replica must still be joining")
	}
	p.DrainMessages()

	if err := p.Handle(proto.Message{
		Type: proto.RequestVote, From: 2, To: 6, ShardID: 1, Term: 6,
		LogIndex: 100, LogTerm: 5,
	}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	for _, m := range p.DrainMessages() {
		if m.Type == proto.RequestVoteResp && m.Reject {
			t.Fatal("a joining replica denied a vote to a real member because its " +
				"own configuration is incomplete: the shard cannot elect a leader " +
				"if its quorum needs this replica's vote")
		}
	}
}
