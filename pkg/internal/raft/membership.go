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
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// configChangeHandler is the function signature for config change handlers
// that both validate and mutate the membership.
type configChangeHandler func(m *membership, cc proto.ConfigChange) error

// configChangeHandlers maps config change types to their handler functions
// for O(1) dispatch. These handlers mutate the membership in place.
var configChangeHandlers = map[uint64]configChangeHandler{
	proto.AddNode:      handleAddNode,
	proto.RemoveNode:   handleRemoveNode,
	proto.AddNonVoting: handleAddNonVoting,
	proto.AddWitness:   handleAddWitness,
}

// configChangeValidator is the function signature for read-only config change
// validators. These check invariants without copying or mutating the membership.
type configChangeValidator func(m *membership, cc proto.ConfigChange) error

// configChangeValidators maps config change types to their read-only validation
// functions for O(1) dispatch. Each validator checks the same preconditions as
// the corresponding handler but performs no mutations.
var configChangeValidators = map[uint64]configChangeValidator{
	proto.AddNode:      validateAddNode,
	proto.RemoveNode:   validateRemoveNode,
	proto.AddNonVoting: validateAddNonVoting,
	proto.AddWitness:   validateAddWitness,
}

// membershipSnapshot is an immutable, concurrency-safe snapshot of the
// cluster membership. It is published via atomic.Pointer after every
// mutation to the mutable membership struct, enabling lock-free reads
// from the transport recv goroutine (IsMember) and host query methods
// (GetMembership). The maps are never modified after construction.
type membershipSnapshot struct {
	// addresses maps replica IDs to network addresses for full voting members.
	addresses map[uint64]string
	// observers maps replica IDs to network addresses for non-voting observers.
	observers map[uint64]string
	// witnesses maps replica IDs to network addresses for witness replicas.
	witnesses map[uint64]string
	// removed tracks replica IDs that have been removed from the cluster.
	removed map[uint64]bool
	// configChangeID is the log index of the most recently applied config change.
	configChangeID uint64
}

// isMember returns true if the replica is in any active role (voter,
// observer, or witness). Safe for concurrent reads because the snapshot
// maps are never modified after construction.
func (s *membershipSnapshot) isMember(replicaID uint64) bool {
	if _, ok := s.addresses[replicaID]; ok {
		return true
	}
	if _, ok := s.observers[replicaID]; ok {
		return true
	}
	_, ok := s.witnesses[replicaID]
	return ok
}

// toProto converts the snapshot to a proto.Membership value. The maps
// are deep-copied to prevent external mutation of the immutable snapshot.
func (s *membershipSnapshot) toProto() proto.Membership {
	return proto.Membership{
		ConfigChangeID: s.configChangeID,
		Addresses:      copyStringMap(s.addresses),
		Observers:      copyStringMap(s.observers),
		Witnesses:      copyStringMap(s.witnesses),
		Removed:        copyBoolMap(s.removed),
	}
}

// membership tracks the current cluster membership configuration. It maintains
// maps of voting members, observers, witnesses, and removed replicas. All
// mutations go through the Apply method which validates and dispatches config
// changes.
//
// The membership struct is owned by a single goroutine (the step worker) and
// requires no synchronization. After each mutation, the caller must publish
// an immutable snapshot via Peer.publishMembership() to make the updated
// state visible to concurrent readers.
type membership struct {
	// addresses maps replica IDs to network addresses for full voting members.
	addresses map[uint64]string
	// observers maps replica IDs to network addresses for non-voting observers.
	observers map[uint64]string
	// witnesses maps replica IDs to network addresses for witness replicas.
	witnesses map[uint64]string
	// removed tracks replica IDs that have been removed from the cluster.
	removed map[uint64]bool
	// configChangeID is the log index of the most recently applied config change.
	configChangeID uint64
}

// newMembership creates a membership from a proto.Membership. The maps are
// deep-copied to avoid aliasing the source.
func newMembership(m proto.Membership) *membership {
	return &membership{
		addresses:      copyStringMap(m.Addresses),
		observers:      copyStringMap(m.Observers),
		witnesses:      copyStringMap(m.Witnesses),
		removed:        copyBoolMap(m.Removed),
		configChangeID: m.ConfigChangeID,
	}
}

// snapshot creates an immutable membershipSnapshot from the current mutable
// state. All maps are deep-copied so the snapshot is safe for concurrent
// reads while the mutable membership continues to be modified by the step
// worker.
func (m *membership) snapshot() *membershipSnapshot {
	return &membershipSnapshot{
		addresses:      copyStringMap(m.addresses),
		observers:      copyStringMap(m.observers),
		witnesses:      copyStringMap(m.witnesses),
		removed:        copyBoolMap(m.removed),
		configChangeID: m.configChangeID,
	}
}

// numVotingMembers returns the total number of voting members including
// full replicas and witnesses. Observers are excluded because they do
// not participate in elections or quorum.
func (m *membership) numVotingMembers() int {
	return len(m.addresses) + len(m.witnesses)
}

// isVoter returns true if the replica is a full voting member.
func (m *membership) isVoter(replicaID uint64) bool {
	_, ok := m.addresses[replicaID]
	return ok
}

// isObserver returns true if the replica is a non-voting observer.
func (m *membership) isObserver(replicaID uint64) bool {
	_, ok := m.observers[replicaID]
	return ok
}

// isWitness returns true if the replica is a witness.
func (m *membership) isWitness(replicaID uint64) bool {
	_, ok := m.witnesses[replicaID]
	return ok
}

// isMember returns true if the replica is in any active role (voter,
// observer, or witness).
func (m *membership) isMember(replicaID uint64) bool {
	return m.isVoter(replicaID) || m.isObserver(replicaID) || m.isWitness(replicaID)
}

// validate checks that a config change is valid before application. It
// enforces safety rules such as preventing removal of the last voter and
// detecting duplicate or conflicting additions. This is a read-only
// operation that does not copy or mutate the membership.
func (m *membership) validate(cc proto.ConfigChange) error {
	if cc.ReplicaID == 0 {
		return ErrZeroReplicaID
	}
	validator, ok := configChangeValidators[cc.Type]
	if !ok {
		return ErrInvalidConfigChangeType
	}
	return validator(m, cc)
}

// apply applies a validated config change to the membership. The config
// change must have been validated with validate() first. This method
// modifies the membership in place.
func (m *membership) apply(cc proto.ConfigChange, logIndex uint64) error {
	if cc.ReplicaID == 0 {
		return ErrZeroReplicaID
	}
	handler, ok := configChangeHandlers[cc.Type]
	if !ok {
		return ErrInvalidConfigChangeType
	}
	err := handler(m, cc)
	if err != nil {
		return err
	}
	m.configChangeID = logIndex
	return nil
}

// handleAddNode adds a full voting member. If the replica is currently an
// observer, it is promoted to voter (address is updated). Returns an error
// if the replica already exists as a voter or witness, or was previously
// removed.
func handleAddNode(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	if m.isVoter(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	if m.isWitness(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	// Promote from observer if applicable.
	if m.isObserver(cc.ReplicaID) {
		delete(m.observers, cc.ReplicaID)
	}
	if m.addresses == nil {
		m.addresses = make(map[uint64]string)
	}
	m.addresses[cc.ReplicaID] = cc.Address
	return nil
}

// handleRemoveNode removes a member from any role (voter, observer, or
// witness). Returns an error if removing the last voting member or if the
// node is not found. It also prevents creating a witness-only cluster by
// checking that at least one full (non-witness) replica remains after removal.
func handleRemoveNode(m *membership, cc proto.ConfigChange) error {
	if !m.isMember(cc.ReplicaID) {
		return ErrNodeNotFound
	}
	// Prevent removing the last full (non-witness) replica. Witnesses cannot
	// become leaders, so at least one full replica must remain for the cluster
	// to be operable. Use len(m.addresses) to count only full replicas.
	if m.isVoter(cc.ReplicaID) && len(m.addresses) <= 1 {
		return ErrRemoveLastFullReplica
	}
	// Prevent removing the last voting member entirely (witness-only edge case
	// where no full replicas exist and the last witness is being removed).
	if m.isWitness(cc.ReplicaID) && m.numVotingMembers() <= 1 {
		return ErrRemoveLastVoter
	}
	delete(m.addresses, cc.ReplicaID)
	delete(m.observers, cc.ReplicaID)
	delete(m.witnesses, cc.ReplicaID)
	if m.removed == nil {
		m.removed = make(map[uint64]bool)
	}
	m.removed[cc.ReplicaID] = true
	return nil
}

// handleAddNonVoting adds a non-voting observer. Returns an error if the
// replica already exists in any role or was previously removed.
func handleAddNonVoting(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	if m.isMember(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	if m.observers == nil {
		m.observers = make(map[uint64]string)
	}
	m.observers[cc.ReplicaID] = cc.Address
	return nil
}

// handleAddWitness adds a witness member. Returns an error if the replica
// already exists in any role or was previously removed.
func handleAddWitness(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	if m.isMember(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	if m.witnesses == nil {
		m.witnesses = make(map[uint64]string)
	}
	m.witnesses[cc.ReplicaID] = cc.Address
	return nil
}

// validateAddNode checks whether adding a full voting member would succeed
// without mutating the membership. Returns the same errors as handleAddNode.
func validateAddNode(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	if m.isVoter(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	if m.isWitness(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	// Observer promotion is allowed, so no error for isObserver.
	return nil
}

// validateRemoveNode checks whether removing a member would succeed without
// mutating the membership. Returns the same errors as handleRemoveNode.
func validateRemoveNode(m *membership, cc proto.ConfigChange) error {
	if !m.isMember(cc.ReplicaID) {
		return ErrNodeNotFound
	}
	if m.isVoter(cc.ReplicaID) && len(m.addresses) <= 1 {
		return ErrRemoveLastFullReplica
	}
	if m.isWitness(cc.ReplicaID) && m.numVotingMembers() <= 1 {
		return ErrRemoveLastVoter
	}
	return nil
}

// validateAddNonVoting checks whether adding a non-voting observer would
// succeed without mutating the membership. Returns the same errors as
// handleAddNonVoting.
func validateAddNonVoting(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	if m.isMember(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	return nil
}

// validateAddWitness checks whether adding a witness would succeed without
// mutating the membership. Returns the same errors as handleAddWitness.
func validateAddWitness(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	if m.isMember(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	return nil
}

// copyStringMap returns a deep copy of a string map. Returns nil for nil
// input.
func copyStringMap(src map[uint64]string) map[uint64]string {
	if src == nil {
		return nil
	}
	dst := make(map[uint64]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// copyBoolMap returns a deep copy of a bool map. Returns nil for nil
// input.
func copyBoolMap(src map[uint64]bool) map[uint64]bool {
	if src == nil {
		return nil
	}
	dst := make(map[uint64]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
