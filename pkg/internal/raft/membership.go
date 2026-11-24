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
	"fmt"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/netaddr"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// configChangeHandler is the function signature for config change handlers
// that both validate and mutate the membership.
type configChangeHandler func(m *membership, cc proto.ConfigChange) error

// configChangeHandlers maps config change types to their handler functions
// for O(1) dispatch. These handlers mutate the membership in place.
var configChangeHandlers = map[uint64]configChangeHandler{
	proto.AddNode:             handleAddNode,
	proto.RemoveNode:          handleRemoveNode,
	proto.AddNonVoting:        handleAddNonVoting,
	proto.AddWitness:          handleAddWitness,
	proto.UpdateWitnessPolicy: handleUpdateWitnessPolicy,
	proto.UpdateWireVersion:   handleUpdateWireVersion,
}

// configChangeValidator is a read-only check run on the leader's propose path; apply-path refusals are deterministic across replicas.
type configChangeValidator func(m *membership, cc proto.ConfigChange) error

// configChangeValidators maps config change types to their read-only validation
// functions for O(1) dispatch. Each validator checks the same preconditions as
// the corresponding handler but performs no mutations.
var configChangeValidators = map[uint64]configChangeValidator{
	proto.AddNode:             validateAddNode,
	proto.RemoveNode:          validateRemoveNode,
	proto.AddNonVoting:        validateAddNonVoting,
	proto.AddWitness:          validateAddWitness,
	proto.UpdateWitnessPolicy: validateUpdateWitnessPolicy,
	proto.UpdateWireVersion:   validateUpdateWireVersion,
}

// membershipSnapshot is an immutable membership copy published via atomic.Pointer for lock-free reads.
type membershipSnapshot struct {
	// maxWitnesses is the shard's replicated witness cap (0 = no cap).
	maxWitnesses uint64
	// requireFullReplicaMajority is the shard's replicated
	// full-replica-majority witness policy.
	requireFullReplicaMajority bool
	// sessionPolicy is the shard's replicated session policy. It is carried
	// through the snapshot so the state machine can read it off the published
	// pointer from the apply goroutine; see proto.SessionPolicy.
	sessionPolicy proto.SessionPolicy
	// wireVersion is the shard's committed wire version (0 reads as
	// proto.PreviousWireVersion); see membership.wireVersion.
	wireVersion uint64
	// addresses maps replica IDs to network addresses for full voting members.
	addresses map[uint64]string
	// observers maps replica IDs to network addresses for non-voting observers.
	observers map[uint64]string
	// witnesses maps replica IDs to network addresses for witness replicas.
	witnesses map[uint64]string
	// removed tracks replica IDs that have been removed from the cluster.
	removed map[uint64]bool
	// identities maps replica IDs to their committed cryptographic node
	// identity. Sparse: see proto.Membership.Identities.
	identities map[uint64]string
	// removedIdentities maps removed replica IDs to their identities; see proto.Membership.RemovedIdentities.
	removedIdentities map[uint64]string
	// removedIdentitySet is removedIdentities keyed by identity, built once per membership change.
	removedIdentitySet map[string]struct{}
	// identityHolders maps each committed identity to its replica; see Peer.IdentityHolder.
	identityHolders map[string]uint64
	// configChangeID is the log index of the most recently applied config change.
	configChangeID uint64
	// sharedAddresses holds addresses claimed by more than one active member; nil when correctly configured.
	sharedAddresses map[string]struct{}
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

// isObserverOr reports whether replicaID is an observer, or hint when the configuration does not mention it.
func (s *membershipSnapshot) isObserverOr(replicaID uint64, hint bool) bool {
	if _, ok := s.observers[replicaID]; ok {
		return true
	}
	if s.isMember(replicaID) {
		return false
	}
	return hint
}

// memberAddress returns replicaID's address in any active role and whether it is present, reading the same maps as isMember.
func (s *membershipSnapshot) memberAddress(replicaID uint64) (string, bool) {
	if addr, ok := s.addresses[replicaID]; ok {
		return addr, true
	}
	if addr, ok := s.observers[replicaID]; ok {
		return addr, true
	}
	addr, ok := s.witnesses[replicaID]
	return addr, ok
}

// memberIdentity returns replicaID's committed node identity, or "".
func (s *membershipSnapshot) memberIdentity(replicaID uint64) string {
	return s.identities[replicaID]
}

// addressIsExclusive reports whether address, canonicalised, identifies exactly one active member.
func (s *membershipSnapshot) addressIsExclusive(address string) bool {
	if len(s.sharedAddresses) == 0 {
		return true
	}
	_, shared := s.sharedAddresses[netaddr.Canonical(address)]
	return !shared
}

// isRemoved reports whether the replica carries a Removed tombstone. Safe for
// concurrent reads because the snapshot maps are never modified after
// construction.
func (s *membershipSnapshot) isRemoved(replicaID uint64) bool {
	return s.removed[replicaID]
}

// isRemovedIdentity reports whether identity has a committed removal; the empty identity never does.
func (s *membershipSnapshot) isRemovedIdentity(identity string) bool {
	if identity == "" {
		return false
	}
	_, removed := s.removedIdentitySet[identity]
	return removed
}

// identityHolder reports the replica the committed configuration binds
// identity to, and whether there is one. Safe for concurrent reads because the
// snapshot maps are never modified after construction.
func (s *membershipSnapshot) identityHolder(identity string) (uint64, bool) {
	if identity == "" {
		return 0, false
	}
	id, ok := s.identityHolders[identity]
	return id, ok
}

// toProto converts the snapshot to a proto.Membership value. The maps
// are deep-copied to prevent external mutation of the immutable snapshot.
func (s *membershipSnapshot) toProto() proto.Membership {
	m := proto.Membership{
		ConfigChangeID:             s.configChangeID,
		MaxWitnesses:               s.maxWitnesses,
		RequireFullReplicaMajority: s.requireFullReplicaMajority,
		Addresses:                  copyStringMap(s.addresses),
		Observers:                  copyStringMap(s.observers),
		Witnesses:                  copyStringMap(s.witnesses),
		Removed:                    copyBoolMap(s.removed),
		Identities:                 copyStringMap(s.identities),
		RemovedIdentities:          copyStringMap(s.removedIdentities),
		WireVersion:                s.wireVersion,
	}
	// Copy the policy as one value so no field can be dropped.
	m.SetSessionPolicy(s.sessionPolicy)
	return m
}

// membership is the step worker's mutable configuration; after each mutation publish a snapshot via Peer.publishMembership.
type membership struct {
	// addresses maps replica IDs to network addresses for full voting members.
	addresses map[uint64]string
	// observers maps replica IDs to network addresses for non-voting observers.
	observers map[uint64]string
	// witnesses maps replica IDs to network addresses for witness replicas.
	witnesses map[uint64]string
	// removed tracks replica IDs that have been removed from the cluster.
	removed map[uint64]bool
	// identities maps replica IDs to committed node identities across every role; only committed config changes write it.
	identities map[uint64]string
	// removedIdentities maps removed replica IDs to their identities; written only by handleRemoveNode.
	removedIdentities map[uint64]string
	// configChangeID is the log index of the most recently applied config change.
	configChangeID uint64

	// maxWitnesses is the replicated witness cap (0 = none), changed only by a committed UpdateWitnessPolicy.
	maxWitnesses uint64
	// requireFullReplicaMajority is the shard's replicated
	// full-replica-majority witness policy. Same lifecycle as
	// maxWitnesses.
	requireFullReplicaMajority bool

	// sessionPolicy is the replicated session policy, set at bootstrap and never changed after.
	sessionPolicy proto.SessionPolicy
	// wireVersion is the committed wire version (0 = proto.PreviousWireVersion), moved only forward by UpdateWireVersion.
	wireVersion uint64
	// wire is the local set of readable wire versions; a committed version outside it is refused.
	wire proto.WireWindow
	// requireIdentities is local admission policy, checked only by validate and never replicated.
	requireIdentities bool
}

// newMembership creates a membership from a proto.Membership. The maps are
// deep-copied to avoid aliasing the source.
func newMembership(m proto.Membership) *membership {
	return &membership{
		addresses:                  copyStringMap(m.Addresses),
		observers:                  copyStringMap(m.Observers),
		witnesses:                  copyStringMap(m.Witnesses),
		removed:                    copyBoolMap(m.Removed),
		identities:                 copyStringMap(m.Identities),
		removedIdentities:          copyStringMap(m.RemovedIdentities),
		configChangeID:             m.ConfigChangeID,
		maxWitnesses:               m.MaxWitnesses,
		requireFullReplicaMajority: m.RequireFullReplicaMajority,
		sessionPolicy:              m.SessionPolicy(),
		wireVersion:                m.WireVersion,
		wire:                       proto.DefaultWireWindow(),
	}
}

// newMembershipWithPolicy is newMembership under a local identity-admission
// policy; see membership.requireIdentities.
func newMembershipWithPolicy(m proto.Membership, requireIdentities bool) *membership {
	mem := newMembership(m)
	mem.requireIdentities = requireIdentities
	return mem
}

// snapshot returns a deep-copied immutable membershipSnapshot.
func (m *membership) snapshot() *membershipSnapshot {
	return &membershipSnapshot{
		addresses:                  copyStringMap(m.addresses),
		observers:                  copyStringMap(m.observers),
		witnesses:                  copyStringMap(m.witnesses),
		removed:                    copyBoolMap(m.removed),
		identities:                 copyStringMap(m.identities),
		removedIdentities:          copyStringMap(m.removedIdentities),
		removedIdentitySet:         invertIdentities(m.removedIdentities),
		identityHolders:            identityHoldersOf(m.identities),
		configChangeID:             m.configChangeID,
		maxWitnesses:               m.maxWitnesses,
		requireFullReplicaMajority: m.requireFullReplicaMajority,
		sessionPolicy:              m.sessionPolicy,
		wireVersion:                m.wireVersion,
		sharedAddresses:            m.sharedAddresses(),
	}
}

// identityHoldersOf builds the identity-keyed holder index for a
// replica-ID-keyed identity map, or nil when it holds no identity. Empty
// identities are skipped for the reason invertIdentities skips them.
func identityHoldersOf(m map[uint64]string) map[string]uint64 {
	var holders map[string]uint64
	for id, identity := range m {
		if identity == "" {
			continue
		}
		if holders == nil {
			holders = make(map[string]uint64, len(m))
		}
		holders[identity] = id
	}
	return holders
}

// invertIdentities returns the identity-keyed set for an identity map, skipping empty identities, or nil when empty.
func invertIdentities(m map[uint64]string) map[string]struct{} {
	var set map[string]struct{}
	for _, identity := range m {
		if identity == "" {
			continue
		}
		if set == nil {
			set = make(map[string]struct{}, len(m))
		}
		set[identity] = struct{}{}
	}
	return set
}

// SessionPolicy returns the replicated session policy from the published snapshot; before publication it is the zero policy.
func (p *Peer) SessionPolicy() proto.SessionPolicy {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return proto.SessionPolicy{}
	}
	return snap.sessionPolicy
}

// sharedAddresses returns canonical addresses held by more than one active member, or nil.
func (m *membership) sharedAddresses() map[string]struct{} {
	var shared map[string]struct{}
	seen := make(map[string]uint64, len(m.addresses)+len(m.witnesses)+len(m.observers))
	for _, group := range []map[uint64]string{m.addresses, m.witnesses, m.observers} {
		for id, addr := range group {
			if addr == "" {
				continue
			}
			key := netaddr.Canonical(addr)
			if other, dup := seen[key]; dup && other != id {
				if shared == nil {
					shared = make(map[string]struct{}, 1)
				}
				shared[key] = struct{}{}
				continue
			}
			seen[key] = id
		}
	}
	return shared
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

// addressHolder returns the active replica holding address, compared in canonical form without DNS; removed replicas do not count.
func (m *membership) addressHolder(address string) (uint64, bool) {
	want := netaddr.Canonical(address)
	for _, group := range []map[uint64]string{m.addresses, m.witnesses, m.observers} {
		for id, addr := range group {
			if netaddr.Canonical(addr) == want {
				return id, true
			}
		}
	}
	return 0, false
}

// recordIdentity binds an add's committed identity to its replica; an add with no identity leaves the binding alone.
func (m *membership) recordIdentity(cc proto.ConfigChange) {
	if cc.NodeIdentity == "" {
		return
	}
	if m.identities == nil {
		m.identities = make(map[uint64]string)
	}
	m.identities[cc.ReplicaID] = cc.NodeIdentity
}

// identityHolder returns the active replica holding identity; removed replicas do not count.
func (m *membership) identityHolder(identity string) (uint64, bool) {
	if identity == "" {
		return 0, false
	}
	for id, known := range m.identities {
		if known == identity {
			return id, true
		}
	}
	return 0, false
}

// validateMemberIdentity refuses an identity bound to another active replica or tombstoned by a removal.
func validateMemberIdentity(m *membership, cc proto.ConfigChange) error {
	// With mandatory identities only a net-new binding without an identity is refused.
	if m.requireIdentities && cc.NodeIdentity == "" && m.identities[cc.ReplicaID] == "" {
		return ErrMemberIdentityRequired
	}
	if holder, dup := m.identityHolder(cc.NodeIdentity); dup && holder != cc.ReplicaID {
		return ErrDuplicateMemberIdentity
	}
	if cc.NodeIdentity != "" {
		for _, removed := range m.removedIdentities {
			if removed == cc.NodeIdentity {
				return ErrRemovedMemberIdentity
			}
		}
	}
	return nil
}

// hasMemberOtherThan reports whether an active member other than replicaID exists.
func (m *membership) hasMemberOtherThan(replicaID uint64) bool {
	for id := range m.addresses {
		if id != replicaID {
			return true
		}
	}
	for id := range m.observers {
		if id != replicaID {
			return true
		}
	}
	for id := range m.witnesses {
		if id != replicaID {
			return true
		}
	}
	return false
}

// isRemoved reports whether the replica has a Removed tombstone.
func (m *membership) isRemoved(replicaID uint64) bool {
	return m.removed[replicaID]
}

// validate checks a config change before application, including the witness policy, without mutating the membership.
func (m *membership) validate(cc proto.ConfigChange) error {
	// UpdateWitnessPolicy and UpdateWireVersion target the shard, not a
	// replica; every other change type must name a non-zero target replica.
	if cc.ReplicaID == 0 && !targetsShard(cc.Type) {
		return ErrZeroReplicaID
	}
	validator, ok := configChangeValidators[cc.Type]
	if !ok {
		return ErrInvalidConfigChangeType
	}
	return validator(m, cc)
}

// apply applies a committed config change in place, converging on the named role; refusals belong in validate.
func (m *membership) apply(cc proto.ConfigChange, logIndex uint64) error {
	// Skip a change at or below configChangeID: it is already applied.
	if logIndex <= m.configChangeID {
		return nil
	}
	if cc.ReplicaID == 0 && !targetsShard(cc.Type) {
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

// The add handlers converge on the role the committed entry names, because a membership seeded by StartShard may list non-voters as voters.

// handleAddNode adds or promotes a full voter, updating an existing voter's address; it refuses a witness or removed replica.
func handleAddNode(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	// A witness cannot be promoted to a full voter.
	if m.isWitness(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	// The voting-member cap is checked only in validateAddNode; promote from observer unconditionally.
	delete(m.observers, cc.ReplicaID)
	if m.addresses == nil {
		m.addresses = make(map[uint64]string)
	}
	m.addresses[cc.ReplicaID] = cc.Address
	m.recordIdentity(cc)
	return nil
}

// handleRemoveNode removes a member from any role, refusing to remove the last voter or last full replica, or an unknown node.
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
	// Move the identity to the tombstone, unconditionally, so the deny-list can match the authenticated identity.
	if identity := m.identities[cc.ReplicaID]; identity != "" {
		if m.removedIdentities == nil {
			m.removedIdentities = make(map[uint64]string)
		}
		m.removedIdentities[cc.ReplicaID] = identity
	}
	delete(m.identities, cc.ReplicaID)
	if m.removed == nil {
		m.removed = make(map[uint64]bool)
	}
	m.removed[cc.ReplicaID] = true
	return nil
}

// handleAddNonVoting adds an observer, demoting a seeded voter; it refuses a witness or removed replica.
func handleAddNonVoting(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	// Only a committed AddWitness makes a witness, identically on every replica, so refusing is safe.
	if m.isWitness(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	delete(m.addresses, cc.ReplicaID)
	if m.observers == nil {
		m.observers = make(map[uint64]string)
	}
	m.observers[cc.ReplicaID] = cc.Address
	m.recordIdentity(cc)
	return nil
}

// handleAddWitness adds a witness, demoting a seeded voter; it refuses an observer or removed replica.
func handleAddWitness(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	// Only a committed AddNonVoting makes an observer, identically on every replica, so refusing is safe.
	if m.isObserver(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	delete(m.addresses, cc.ReplicaID)
	// The voting-member cap, witnesses included, is checked only in validateAddWitness.
	if m.witnesses == nil {
		m.witnesses = make(map[uint64]string)
	}
	m.witnesses[cc.ReplicaID] = cc.Address
	m.recordIdentity(cc)
	return nil
}

// ErrStaleConfigChange is returned when a bound add names a configuration other than the current one.
var ErrStaleConfigChange = errors.New("raft: stale config change")

// checkConfigChangeBound refuses a bound add whose ConfigChangeID is not current; unbound adds are not checked.
func checkConfigChangeBound(m *membership, cc proto.ConfigChange) error {
	if cc.ConfigChangeID == 0 && !cc.MustMatchConfigChangeID {
		return nil
	}
	if cc.ConfigChangeID != m.configChangeID {
		return &StaleConfigChangeError{Expected: m.configChangeID, Got: cc.ConfigChangeID}
	}
	return nil
}

// StaleConfigChangeError is the add validators' refusal of a bound change:
// Got is the ConfigChangeID the change was checked against, Expected the
// membership's current one. It matches ErrStaleConfigChange with errors.Is.
type StaleConfigChangeError struct {
	Expected uint64
	Got      uint64
}

// Error returns a human-readable description.
func (e *StaleConfigChangeError) Error() string {
	return fmt.Sprintf("raft: stale config change: change checked membership %d, current is %d",
		e.Got, e.Expected)
}

// Is reports whether target is ErrStaleConfigChange.
func (e *StaleConfigChangeError) Is(target error) bool {
	return target == ErrStaleConfigChange
}

// validateAddNode checks whether adding a full voting member would succeed
// without mutating the membership. Returns the same errors as handleAddNode.
func validateAddNode(m *membership, cc proto.ConfigChange) error {
	if err := checkConfigChangeBound(m, cc); err != nil {
		return err
	}
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
	if m.numVotingMembers() >= maxVotingMembers {
		return ErrTooManyVotingMembers
	}
	// An address held by another active replica is refused; an observer promotion keeps its own address.
	if holder, dup := m.addressHolder(cc.Address); dup && holder != cc.ReplicaID {
		return ErrDuplicateMemberAddress
	}
	if err := validateMemberIdentity(m, cc); err != nil {
		return err
	}
	// Observer promotion is allowed, so no error for isObserver.
	return nil
}

// validateRemoveNode checks a removal without mutating, adding the RequireFullReplicaMajority check to handleRemoveNode's errors.
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
	// RequireFullReplicaMajority also gates removing a full replica.
	if m.requireFullReplicaMajority && len(m.witnesses) > 0 && m.isVoter(cc.ReplicaID) {
		fullReplicas := len(m.addresses) - 1
		if fullReplicas < quorumOf(fullReplicas+len(m.witnesses)) {
			return ErrBreaksFullReplicaMajority
		}
	}
	return nil
}

// validateAddNonVoting checks whether adding a non-voting observer would
// succeed without mutating the membership. Returns the same errors as
// handleAddNonVoting.
func validateAddNonVoting(m *membership, cc proto.ConfigChange) error {
	if err := checkConfigChangeBound(m, cc); err != nil {
		return err
	}
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	if m.isMember(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	if _, dup := m.addressHolder(cc.Address); dup {
		return ErrDuplicateMemberAddress
	}
	return validateMemberIdentity(m, cc)
}

// validateAddWitness checks a witness add without mutating, adding the replicated witness-policy checks.
func validateAddWitness(m *membership, cc proto.ConfigChange) error {
	if err := checkConfigChangeBound(m, cc); err != nil {
		return err
	}
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	if m.isMember(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	if m.numVotingMembers() >= maxVotingMembers {
		return ErrTooManyVotingMembers
	}
	if _, dup := m.addressHolder(cc.Address); dup {
		return ErrDuplicateMemberAddress
	}
	if err := validateMemberIdentity(m, cc); err != nil {
		return err
	}
	newWitnessCount := len(m.witnesses) + 1
	if m.maxWitnesses > 0 && uint64(newWitnessCount) > m.maxWitnesses {
		return ErrTooManyWitnesses
	}
	if m.requireFullReplicaMajority {
		// After the add the full replicas alone must still form a quorum of the voting set.
		fullReplicas := len(m.addresses)
		if fullReplicas < quorumOf(fullReplicas+newWitnessCount) {
			return ErrBreaksFullReplicaMajority
		}
	}
	return nil
}

// handleUpdateWitnessPolicy replaces the shard's replicated witness policy.
// Like every handler it is unconditional: an already-committed policy change
// must replay identically on every replica.
func handleUpdateWitnessPolicy(m *membership, cc proto.ConfigChange) error {
	m.maxWitnesses = cc.MaxWitnesses
	m.requireFullReplicaMajority = cc.RequireFullReplicaMajority
	return nil
}

// validateUpdateWitnessPolicy requires the current membership to satisfy the proposed policy.
func validateUpdateWitnessPolicy(m *membership, cc proto.ConfigChange) error {
	if cc.MaxWitnesses > 0 && uint64(len(m.witnesses)) > cc.MaxWitnesses {
		return ErrTooManyWitnesses
	}
	if cc.RequireFullReplicaMajority && len(m.witnesses) > 0 {
		fullReplicas := len(m.addresses)
		if fullReplicas < quorumOf(fullReplicas+len(m.witnesses)) {
			return ErrBreaksFullReplicaMajority
		}
	}
	return nil
}

// targetsShard reports whether a config change type changes shard-wide
// state rather than naming a target replica, so a zero ReplicaID is valid.
func targetsShard(t uint64) bool {
	return t == proto.UpdateWitnessPolicy || t == proto.UpdateWireVersion
}

// handleUpdateWireVersion moves the committed wire version forward; a lower or unreadable version returns proto.ErrUnsupportedVersion.
func handleUpdateWireVersion(m *membership, cc proto.ConfigChange) error {
	if err := m.wire.CheckCommit(m.wireVersion, cc.WireVersion); err != nil {
		return err
	}
	m.wireVersion = cc.WireVersion
	return nil
}

// validateUpdateWireVersion applies the apply-path rule and refuses a switch checked against another ConfigChangeID as pending.
func validateUpdateWireVersion(m *membership, cc proto.ConfigChange) error {
	if err := m.wire.CheckCommit(m.wireVersion, cc.WireVersion); err != nil {
		return err
	}
	if cc.ConfigChangeID != m.configChangeID {
		return fmt.Errorf("%w: wire-version switch checked membership %d, current is %d",
			ErrPendingConfigChange, cc.ConfigChangeID, m.configChangeID)
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
