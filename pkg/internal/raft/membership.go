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
	proto.AddNode:             handleAddNode,
	proto.RemoveNode:          handleRemoveNode,
	proto.AddNonVoting:        handleAddNonVoting,
	proto.AddWitness:          handleAddWitness,
	proto.UpdateWitnessPolicy: handleUpdateWitnessPolicy,
}

// configChangeValidator is the function signature for read-only config change
// validators. These check invariants without copying or mutating the
// membership, including the shard's replicated witness policy (carried on
// the membership itself). The deterministic apply path (configChangeHandlers)
// never validates, so an already-committed change always replays — mirroring
// how the 64-member cap is validate-only.
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
}

// membershipSnapshot is an immutable, concurrency-safe snapshot of the
// cluster membership. It is published via atomic.Pointer after every
// mutation to the mutable membership struct, enabling lock-free reads
// from the transport recv goroutine (IsMember) and host query methods
// (GetMembership). The maps are never modified after construction.
type membershipSnapshot struct {
	// maxWitnesses is the shard's replicated witness cap (0 = no cap).
	maxWitnesses uint64
	// requireFullReplicaMajority is the shard's replicated
	// full-replica-majority witness policy.
	requireFullReplicaMajority bool
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
		ConfigChangeID:             s.configChangeID,
		MaxWitnesses:               s.maxWitnesses,
		RequireFullReplicaMajority: s.requireFullReplicaMajority,
		Addresses:                  copyStringMap(s.addresses),
		Observers:                  copyStringMap(s.observers),
		Witnesses:                  copyStringMap(s.witnesses),
		Removed:                    copyBoolMap(s.removed),
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

	// maxWitnesses is the shard's replicated witness cap (0 = no cap).
	// Part of the replicated membership state: seeded at bootstrap from
	// config.WitnessConfig, changed only via a committed
	// UpdateWitnessPolicy config change, so every replica enforces the
	// same policy regardless of local configuration.
	maxWitnesses uint64
	// requireFullReplicaMajority is the shard's replicated
	// full-replica-majority witness policy. Same lifecycle as
	// maxWitnesses.
	requireFullReplicaMajority bool
}

// newMembership creates a membership from a proto.Membership. The maps are
// deep-copied to avoid aliasing the source.
func newMembership(m proto.Membership) *membership {
	return &membership{
		addresses:                  copyStringMap(m.Addresses),
		observers:                  copyStringMap(m.Observers),
		witnesses:                  copyStringMap(m.Witnesses),
		removed:                    copyBoolMap(m.Removed),
		configChangeID:             m.ConfigChangeID,
		maxWitnesses:               m.MaxWitnesses,
		requireFullReplicaMajority: m.RequireFullReplicaMajority,
	}
}

// snapshot creates an immutable membershipSnapshot from the current mutable
// state. All maps are deep-copied so the snapshot is safe for concurrent
// reads while the mutable membership continues to be modified by the step
// worker.
func (m *membership) snapshot() *membershipSnapshot {
	return &membershipSnapshot{
		addresses:                  copyStringMap(m.addresses),
		observers:                  copyStringMap(m.observers),
		witnesses:                  copyStringMap(m.witnesses),
		removed:                    copyBoolMap(m.removed),
		configChangeID:             m.configChangeID,
		maxWitnesses:               m.maxWitnesses,
		requireFullReplicaMajority: m.requireFullReplicaMajority,
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
// detecting duplicate or conflicting additions, plus the shard's replicated
// witness policy. This is a read-only operation that does not copy or mutate
// the membership.
func (m *membership) validate(cc proto.ConfigChange) error {
	// UpdateWitnessPolicy targets the shard, not a replica; every other
	// change type must name a non-zero target replica.
	if cc.ReplicaID == 0 && cc.Type != proto.UpdateWitnessPolicy {
		return ErrZeroReplicaID
	}
	validator, ok := configChangeValidators[cc.Type]
	if !ok {
		return ErrInvalidConfigChangeType
	}
	return validator(m, cc)
}

// apply applies a COMMITTED config change to the membership, modifying it in
// place. It is the deterministic replicated-state-machine transition, not a
// gate: every precondition that can legitimately refuse a change belongs in
// validate(), which runs on the leader BEFORE the entry is proposed.
//
// The handlers therefore CONVERGE on the role the committed entry names rather
// than refusing an entry whose target is already a member. A refusal here is
// not a safe no-op: it leaves this replica holding a membership the rest of
// the shard does not, and it does not advance configChangeID, so nothing later
// repairs it. See the "Convergent application" comment above the handlers.
func (m *membership) apply(cc proto.ConfigChange, logIndex uint64) error {
	// Replay guard: a config change at or below configChangeID has already
	// been applied (restart replay of committed entries on top of a
	// snapshot-seeded membership, or a follower re-receiving entries).
	// Re-applying would be wrong for non-idempotent changes — a replayed
	// UpdateWitnessPolicy would revert a newer policy — and would regress
	// configChangeID. Skipping is deterministic across replicas because
	// configChangeID is itself replicated state.
	if logIndex <= m.configChangeID {
		return nil
	}
	if cc.ReplicaID == 0 && cc.Type != proto.UpdateWitnessPolicy {
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

// Convergent application of committed adds
//
// The three add handlers below place the target in exactly the role the
// COMMITTED entry names, whatever role the membership currently records for
// it. They never refuse an entry because the target "already exists".
//
// This matters because a membership is not always built purely from committed
// entries. Host.StartShard seeds proto.Membership.Addresses from the caller's
// members map whenever recovery restored nothing — the ordinary state of a
// caught-up replica that has never snapshotted — and that map is the
// operator's list of replica IDs, with no record of which of them the shard
// has since made witnesses or observers. Every non-voter listed in it is
// therefore seeded as a full VOTER. The committed AddWitness / AddNonVoting
// entries that gave those replicas their real roles are still in the log and
// are re-applied on restart; refusing them (ErrNodeAlreadyExists) left the
// wrong roles in place permanently, because the seed is not repaired by
// anything else and configChangeID never advanced past the skipped entry.
//
// Converging is not a weakening of the checks. When the membership WAS built
// from committed entries only, validate() ran on the leader before the entry
// was proposed and guaranteed the target was not a member, so the extra
// deletes below are provably no-ops and the behaviour is byte-for-byte the
// old one. The rejections that remain all have a precondition that either
// holds identically on every replica or that the seed can only LOOSEN, never
// tighten, so none can make replicas diverge:
//
//   - the two add refusals (a Removed tombstone, and promoting a witness via
//     AddNode) — the seed only populates addresses, so it can manufacture
//     neither a tombstone nor a witness role; the precondition is therefore
//     identical to the committed-only membership.
//   - handleRemoveNode's three (ErrNodeNotFound, ErrRemoveLastFullReplica,
//     ErrRemoveLastVoter) — these read len(m.addresses)/isMember/isWitness,
//     and seeding every listed non-voter as a voter can only make isMember
//     more likely true, len(m.addresses) larger, and isWitness less likely
//     true. Each of those makes the refusal STRICTLY LESS likely to fire than
//     on the true membership, so a committed remove the true membership would
//     apply is never refused by the seeded one.

// handleAddNode adds a full voting member. An existing observer is promoted;
// an existing voter has its address updated (the committed entry is
// authoritative over a seeded placement). Returns an error if the replica is a
// witness or was previously removed.
func handleAddNode(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	// Promoting a witness to a full voter would make a replica that holds no
	// entry payloads answer reads out of a state machine that never saw them.
	// dragonboat panics here. The membership seed cannot produce this state
	// (it only ever populates addresses) and validateAddNode rejects the
	// proposal, so every replica evaluates this identically and refusing
	// cannot cause divergence.
	if m.isWitness(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	// NOTE: The voting-member cap is enforced ONLY at propose/validate time
	// (validateAddNode). It must NOT be re-checked here: apply() runs on
	// already-committed config-change entries (commit-time application in
	// Peer.Commit, restart replay), and rejecting a committed change would
	// make replicas diverge from the agreed membership — a Raft safety
	// violation.
	//
	// Promote from observer if applicable. Unconditional: deleting an absent
	// key is a no-op, and this is the only place a promotion can happen.
	delete(m.observers, cc.ReplicaID)
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

// handleAddNonVoting adds a non-voting observer, demoting a replica the
// membership currently records as a full voter (a bootstrap-map seed the
// committed entry overrides). Returns an error if the replica is a witness or
// was previously removed.
func handleAddNonVoting(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	// A witness can only have been made one by an earlier committed
	// AddWitness, which validateAddNonVoting would have refused to follow with
	// this change; the seed cannot produce it either. Identical on every
	// replica, so refusing cannot cause divergence.
	if m.isWitness(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	delete(m.addresses, cc.ReplicaID)
	if m.observers == nil {
		m.observers = make(map[uint64]string)
	}
	m.observers[cc.ReplicaID] = cc.Address
	return nil
}

// handleAddWitness adds a witness member, demoting a replica the membership
// currently records as a full voter (a bootstrap-map seed the committed entry
// overrides). Returns an error if the replica is an observer or was previously
// removed.
func handleAddWitness(m *membership, cc proto.ConfigChange) error {
	if cc.Address == "" {
		return ErrEmptyAddress
	}
	if m.removed[cc.ReplicaID] {
		return ErrNodeRemoved
	}
	// An observer can only have been made one by an earlier committed
	// AddNonVoting, which validateAddWitness would have refused to follow with
	// this change; the seed cannot produce it either. Identical on every
	// replica, so refusing cannot cause divergence.
	if m.isObserver(cc.ReplicaID) {
		return ErrNodeAlreadyExists
	}
	delete(m.addresses, cc.ReplicaID)
	// The voting-member cap (witnesses count toward it) is enforced ONLY at
	// propose/validate time (validateAddWitness). Re-checking here would let a
	// replica reject an already-committed change and diverge — see handleAddNode.
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
	if m.numVotingMembers() >= maxVotingMembers {
		return ErrTooManyVotingMembers
	}
	// Observer promotion is allowed, so no error for isObserver.
	return nil
}

// validateRemoveNode checks whether removing a member would succeed without
// mutating the membership. Returns the same errors as handleRemoveNode, plus
// the RequireFullReplicaMajority policy violation when removing a full
// replica would leave the remaining full replicas unable to form a quorum.
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
	// RequireFullReplicaMajority must also gate full-replica removal: without
	// this check, removing a full replica from a compliant membership (e.g.
	// {3 full + 2 witnesses}) silently produces exactly the witness-heavy
	// shape the policy exists to reject ({2 full + 2 witnesses}). The
	// witnesses > 0 conjunct is clarity-only: with zero witnesses the
	// arithmetic below can never fire (the last-full-replica check above
	// pre-empts fullReplicas == 0).
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
// mutating the membership. Returns the same errors as handleAddWitness, plus
// the replicated witness-policy violations (validate-only — never enforced
// on the apply path).
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
	if m.numVotingMembers() >= maxVotingMembers {
		return ErrTooManyVotingMembers
	}
	newWitnessCount := len(m.witnesses) + 1
	if m.maxWitnesses > 0 && uint64(newWitnessCount) > m.maxWitnesses {
		return ErrTooManyWitnesses
	}
	if m.requireFullReplicaMajority {
		// After the add, the full (data-bearing) replicas alone must still be
		// able to form a quorum over the voting set. This bounds membership
		// shape only; it does not constrain which nodes form any individual
		// commit quorum (see the config.WitnessConfig doc).
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

// validateUpdateWitnessPolicy checks that the proposed policy is satisfied
// by the CURRENT membership: a policy the shard already violates would be
// unenforceable doctrine (every subsequent witness change would be judged
// against a baseline the membership never met). Operators must reshape the
// membership first, then tighten the policy.
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
