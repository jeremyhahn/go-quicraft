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

func TestMembership_AddNode_AlreadyVoter(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 1,
		Address:   "addr1",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
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

func TestMembership_AddNonVoting_AlreadyMember(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 1,
		Address:   "addr1",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
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

func TestMembership_AddWitness_AlreadyMember(t *testing.T) {
	m := testMembership()
	cc := proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 1,
		Address:   "addr1",
	}
	err := m.apply(cc, 10)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
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

func TestMembership_Validate_MatchesApply_AllTypes(t *testing.T) {
	// Exhaustive equivalence test: for each config change type, validate()
	// must return the same error as applying to a fresh copy of the membership.
	cases := []struct {
		name string
		cc   proto.ConfigChange
	}{
		{"AddNode-new", proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "a4"}},
		{"AddNode-existing", proto.ConfigChange{Type: proto.AddNode, ReplicaID: 1, Address: "a1"}},
		{"AddNode-empty-addr", proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: ""}},
		{"RemoveNode-exists", proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 1}},
		{"RemoveNode-missing", proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 99}},
		{"AddNonVoting-new", proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 10, Address: "o10"}},
		{"AddNonVoting-exists", proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 1, Address: "a1"}},
		{"AddWitness-new", proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 20, Address: "w20"}},
		{"AddWitness-exists", proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 1, Address: "a1"}},
		{"InvalidType", proto.ConfigChange{Type: 999, ReplicaID: 4, Address: "a4"}},
		{"ZeroReplica", proto.ConfigChange{Type: proto.AddNode, ReplicaID: 0, Address: "a0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := testMembership()
			validateErr := m.validate(tc.cc)

			// Apply on a separate copy to get the "ground truth" error.
			mCopy := testMembership()
			applyErr := mCopy.apply(tc.cc, 100)

			if !errors.Is(validateErr, applyErr) {
				t.Fatalf("validate returned %v, apply returned %v", validateErr, applyErr)
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
