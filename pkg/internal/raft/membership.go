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

// configChangeValidator is the function signature for read-only config change
// validators. These check invariants without copying or mutating the
// membership, including the shard's replicated witness policy (carried on
// the membership itself). They run only on the LEADER's propose path, where a
// refusal is safe because nothing has been committed yet.
//
// The apply path (configChangeHandlers) enforces its own, narrower set of
// preconditions and can also refuse — handleAddNode returns ErrEmptyAddress,
// ErrNodeRemoved and ErrNodeAlreadyExists; handleRemoveNode returns
// ErrNodeNotFound, ErrRemoveLastFullReplica and ErrRemoveLastVoter — and
// Peer.applyMembershipEntry then SKIPS the committed change and logs a warning.
// So an already-committed change does not unconditionally replay. Every such
// refusal is a pure function of the replicated membership and the entry, so it
// is identical on every replica and cannot diverge; that determinism, not an
// absence of checks, is what makes the apply path safe. The 64-member cap is
// the genuinely validate-only rule.
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
	// removedIdentities is the replicated tombstone map: removed replica ID to
	// the identity the configuration held for it. See
	// proto.Membership.RemovedIdentities, including why nothing is ever evicted
	// from it.
	removedIdentities map[uint64]string
	// removedIdentitySet is removedIdentities INVERTED — the same fact keyed by
	// the identity string.
	//
	// The receive path asks the question the other way round ("is the identity
	// on this connection one this shard removed?") and asks it once per inbound
	// message, so the index is built once per membership change rather than
	// scanned per message. Same trade as sharedAddresses, and safe for the same
	// reason: everything on a published snapshot is immutable after
	// construction.
	removedIdentitySet map[string]struct{}
	// identityHolders is identities INVERTED: committed identity to the
	// replica that holds it. The receive path asks it once per inbound
	// message a non-committed source (the pending configuration or the
	// operator anchor) would admit, to refuse a keypair the committed
	// configuration already binds to ANOTHER replica; see
	// Peer.IdentityHolder. Built once per membership change, like
	// removedIdentitySet.
	identityHolders map[string]uint64
	// configChangeID is the log index of the most recently applied config change.
	configChangeID uint64
	// sharedAddresses holds every address more than one ACTIVE member claims,
	// and is nil for a correctly configured shard. See membership.sharedAddresses.
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

// isObserverOr reports whether this configuration lists replicaID as a
// non-voting observer, and returns hint when it does not mention the replica in
// any role — the configuration makes no statement about it then, so the
// caller's own belief stands. It is the published-snapshot form of the rule
// raft.checkSelfRole applies to r.isObserver, so a reader on another goroutine
// sees the same effective role the step worker acts on. Safe for concurrent
// reads because the snapshot maps are never modified after construction.
func (s *membershipSnapshot) isObserverOr(replicaID uint64, hint bool) bool {
	if _, ok := s.observers[replicaID]; ok {
		return true
	}
	if s.isMember(replicaID) {
		return false
	}
	return hint
}

// memberAddress returns the address this configuration holds for replicaID in
// any active role, and whether the replica is present at all.
//
// It reads the SAME three maps as isMember, in the same order, so the two can
// never disagree: every replica isMember reports has an address here, and
// nothing else does. Safe for concurrent reads because the snapshot maps are
// never modified after construction.
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

// memberIdentity returns the committed node identity this configuration holds
// for replicaID, or "" when it holds none.
//
// Unlike the address, the identity is stored in ONE map covering every role, so
// a role change (observer promoted to voter, voter demoted to witness) carries
// it across without the handlers having to move it. Safe for concurrent reads
// because the snapshot maps are never modified after construction.
func (s *membershipSnapshot) memberIdentity(replicaID uint64) string {
	return s.identities[replicaID]
}

// addressIsExclusive reports whether address identifies exactly one active
// member of this configuration. Safe for concurrent reads because the snapshot
// maps are never modified after construction.
//
// The lookup key is canonicalised because sharedAddresses is keyed that way:
// comparing the raw spelling here would miss a duplicate recorded under a
// different but equivalent spelling, which is precisely the case the canonical
// form exists to catch.
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

// isRemovedIdentity reports whether identity is one this shard has committed a
// removal for, whatever replica ID the holder now claims.
//
// The empty identity is never removed. A connection with no authenticated
// identity (an address-only shard, or a peer whose certificate yields none) must
// fall through to the checks that CAN judge it rather than be matched against a
// tombstone it cannot be distinguished from — an empty-matches-empty rule would
// deny every such peer on any shard that had ever removed an identity-less
// member, which is a shard-wide outage, not a security improvement.
//
// Safe for concurrent reads because the snapshot maps are never modified after
// construction.
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
	// The policy moves as ONE value rather than field by field. This site
	// silently dropped a policy field once already: it listed the three that
	// existed when it was written, so a fourth was added to proto.SessionPolicy,
	// compiled cleanly here, and vanished on every path that reads membership
	// back out of the raft layer. SetSessionPolicy is total over the struct, so
	// the next field to be added cannot be forgotten in this direction.
	m.SetSessionPolicy(s.sessionPolicy)
	return m
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
	// identities maps replica IDs to their committed cryptographic node
	// identity — the value the receive-side gate compares the AUTHENTICATED
	// identity of a connection against. It is sparse (see
	// proto.Membership.Identities) and, unlike the address maps, it is a single
	// map spanning every role so a promotion or demotion carries the identity
	// across untouched.
	//
	// Every write to it originates in a COMMITTED config change or a committed
	// initial membership. No inbound message field ever reaches it — that is
	// the property the whole gate rests on, and the reason cc.NodeIdentity is
	// the only input the handlers read.
	identities map[uint64]string
	// removedIdentities maps every REMOVED replica ID to the identity the
	// configuration held for it when the removal was applied. It is what makes
	// the removal deny-list answerable against the AUTHENTICATED peer identity
	// instead of the claimed msg.From — see proto.Membership.RemovedIdentities
	// for the threat it closes, why nothing is ever evicted, and the address-only
	// residual.
	//
	// Written in exactly one place, handleRemoveNode, from a value that was
	// itself only ever written by a committed config change. No inbound message
	// field reaches it, which is the property the gate rests on.
	removedIdentities map[uint64]string
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

	// sessionPolicy is the shard's replicated session policy: the three
	// limits the deterministic apply path reads (see proto.SessionPolicy).
	// It is seeded at bootstrap from config.Config and, unlike the witness
	// policy, has no config change that alters it — nothing in this package
	// writes it after newMembership. That immutability is load-bearing: the
	// state machine reads the published value on the APPLY goroutine, which
	// trails commit, so a value that changed mid-log would be observed at a
	// replica-local point in the log. See rsm.StateMachine.sessionPolicyFn.
	sessionPolicy proto.SessionPolicy
	// wireVersion is the shard's committed wire version: the version every
	// replica sends at once the switch commits. Zero reads as
	// proto.PreviousWireVersion. Changed only by a committed
	// UpdateWireVersion config change, which moves it forward and never back,
	// so it is applied at one log index, replayed identically after a crash
	// and carried in snapshots like the witness policy.
	wireVersion uint64
	// wire is LOCAL: the wire versions this build reads. A committed
	// wireVersion outside it is refused with proto.ErrUnsupportedVersion,
	// because this replica could not speak to its peers at it. Like
	// requireIdentities it is not replicated and not carried in snapshots.
	wire proto.WireWindow
	// requireIdentities is LOCAL admission policy, not replicated state: when
	// set, validate refuses to admit a member that carries no node identity
	// (config.Config.RequireMemberIdentities). It is never consulted on the
	// apply path and is not carried into snapshots — a committed entry is
	// applied whatever it carries, so replicas never diverge on it.
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

// invertIdentities builds the identity-keyed lookup set for a replica-ID-keyed
// identity map, or nil when there is nothing to look up — which is every shard
// that has never removed an identity-bearing member, so the common case costs
// one nil check on the read path.
//
// Empty identities are skipped. They cannot be present in a map only
// handleRemoveNode writes (it records nothing for a member with no committed
// identity), but the set is an AUTHORIZATION index and an empty key in it would
// deny every connection whose identity could not be determined. Refusing to
// build one is cheaper than relying on the writer forever.
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

// SessionPolicy returns the shard's replicated session policy from the
// published membership snapshot. It is a pointer load and three field reads,
// safe from any goroutine, which is what lets the apply path consult it per
// batch (see rsm.StateMachine.refreshSessionPolicy).
//
// A peer whose membership has not been published yet — a replica started with
// join=true that has not replicated the shard's configuration — returns the
// zero policy, whose IsSet reports false. The state machine reads that as "no
// replicated policy known" and stays on its local configuration until one
// arrives, which is the only window in which local configuration can still
// drive apply.
func (p *Peer) SessionPolicy() proto.SessionPolicy {
	snap := p.membershipSnap.Load()
	if snap == nil {
		return proto.SessionPolicy{}
	}
	return snap.sessionPolicy
}

// sharedAddresses returns the set of addresses held by MORE THAN ONE active
// member, or nil when every address is exclusive — which is every correctly
// configured shard, so the common result is a nil map and the reader's lookup
// is free.
//
// It is computed once per membership change rather than scanned per inbound
// message because the inbound sender binding consults it on the hot path
// (ReplicateResp and HeartbeatResp arrive at replication rate), and because a
// scan there would be O(members) on messages that are otherwise O(1).
//
// validateAddNode and friends refuse to CREATE a duplicate, so a non-empty
// result means the configuration predates that validation, was seeded through
// StartShard, or arrived in a snapshot's membership. Authorization must still
// fail closed on it: the whole point is that the host behind a shared address
// cannot be told apart from the replica it claims to be.
//
// Keys are netaddr.Canonical, so two SPELLINGS of one endpoint collapse onto a
// single key and are recognised as the duplicate they are.
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

// addressHolder returns the replica an ACTIVE role already holds address for,
// and whether one does. Removed replicas do not count: their tombstone denies
// them regardless, so reusing a departed replica's address is legitimate — it
// is how a machine is recycled into a new replica ID.
//
// A configuration is not allowed to name one address twice, because the inbound
// authorization chain is replica ID -> committed address -> certificate. Two
// active replicas at ONE address collapse that chain: the single host behind
// the address is authorized to speak as BOTH, so it holds two votes and two
// match indices in a quorum that believes they are independent. That is quorum
// forgery reachable through the ordinary admin API, with no forged message and
// no certificate abuse anywhere.
//
// Comparison is on the whole address, not on the host, and that is deliberate:
// two replicas on one MACHINE at different ports are a supported deployment
// (see transport.isCoLocatedCertHost) and are separated by nothing else either,
// so refusing them would break single-machine clusters. Two replicas at the
// same host AND port are one endpoint and cannot be distinct replicas at all.
//
// Comparison is on the CANONICAL form (netaddr.Canonical), so two spellings of
// one endpoint — "NODE-B:5001" and "node-b:5001", "[::1]:5001" and
// "[0:0:0:0:0:0:0:1]:5001" — are recognised as the duplicate they are. The
// certificate match this chain ends in is done by x509.VerifyHostname, which
// already treats them as one host, so a raw string compare here would call two
// members distinct that the certificate cannot tell apart — the exact gap the
// duplicate check exists to close.
//
// It canonicalises SPELLING ONLY and never resolves DNS: this runs inside
// config-change validation, which is replicated logic that must produce the
// same answer on every replica and on every replay. A hostname and the IP it
// currently resolves to therefore remain distinct, and that residual is
// deliberate — see netaddr.
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

// recordIdentity binds the committed node identity an add carries to its target
// replica. An add that carries NO identity leaves any existing binding alone.
//
// "Leaves alone" rather than "clears" is the load-bearing choice. An add is
// replayed on restart and re-applied by followers, and Host.StartShard seeds a
// membership from the operator's members map; every one of those paths can
// produce an AddNode with an empty NodeIdentity for a replica whose identity
// WAS committed earlier. Clearing on empty would silently demote that replica
// back to address-only authorization — a downgrade an attacker could induce by
// getting any identity-less config change committed for the victim. Removing
// an identity therefore requires removing the member (handleRemoveNode).
//
// The value comes from cc.NodeIdentity and from nowhere else. cc is the
// COMMITTED config-change entry, so nothing an inbound message carries in its
// own header can reach this map — which is the property the receive-side gate
// depends on and the mistake the whole campaign exists to undo.
func (m *membership) recordIdentity(cc proto.ConfigChange) {
	if cc.NodeIdentity == "" {
		return
	}
	if m.identities == nil {
		m.identities = make(map[uint64]string)
	}
	m.identities[cc.ReplicaID] = cc.NodeIdentity
}

// identityHolder returns the replica an ACTIVE role already holds identity for,
// and whether one does.
//
// One identity naming two replica IDs is the same forgery as one address naming
// two: the identity IS the authorization key, so a single keypair authorized as
// both replicas holds two votes and two match indices in a quorum that believes
// they are independent. Unlike the address case there is no co-location
// exception — two replicas on one machine still need distinct keypairs, and
// giving them one is exactly the promiscuous-certificate shape identities exist
// to defeat.
//
// Removed replicas do not count: identities are deleted with the member, so a
// tombstoned ID holds none.
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

// validateMemberIdentity refuses a config change whose identity is already
// bound to a DIFFERENT active replica of this shard, or which this shard has
// committed a REMOVAL for under any replica ID. It is validate-only, like every
// other shape rule: refusing an already-committed entry on the apply path would
// make replicas diverge.
//
// The tombstone check is what keeps the deny-list from producing a silently
// unreachable member. Because the tombstone is on the identity and nothing
// evicts it, an add that reuses a removed replica's keypair would commit
// successfully and then have every one of its messages dropped by
// Peer.IsRemovedIdentity on every peer — a voter counted in every quorum that
// can never be heard from. See ErrRemovedMemberIdentity.
func validateMemberIdentity(m *membership, cc proto.ConfigChange) error {
	// Under the mandatory-identity policy a member enters the configuration
	// WITH its identity or not at all. An add that names a replica the
	// configuration already holds an identity for (an observer promotion, a
	// role change) may leave the field empty — recordIdentity leaves the
	// existing binding alone — so only a net-new binding is refused.
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

// hasMemberOtherThan reports whether the configuration names an active member
// that is not replicaID. It is the test for "this configuration can authorize
// somebody besides me": a membership consisting solely of this replica carries
// no information about who else may legitimately send to this shard, so it is
// not sufficient to end the joining window.
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

// isRemoved reports whether the replica carries a Removed tombstone — a
// committed RemoveNode was applied for it. A tombstone is durable evidence that
// this replica has learned the committed configuration (its own removal was
// agreed by the cluster), which is what makes it a valid terminal state for the
// joining window even though the replica is no longer a member.
func (m *membership) isRemoved(replicaID uint64) bool {
	return m.removed[replicaID]
}

// validate checks that a config change is valid before application. It
// enforces safety rules such as preventing removal of the last voter and
// detecting duplicate or conflicting additions, plus the shard's replicated
// witness policy. This is a read-only operation that does not copy or mutate
// the membership.
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
	m.recordIdentity(cc)
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
	// The identity MOVES to the tombstone; it is not dropped and it does not
	// stay where it was.
	//
	// Not left in identities, because that map is the ACTIVE binding: it is what
	// identityHolder scans to refuse a duplicate, so leaving a departed
	// replica's identity there would refuse the legitimate operation of
	// recycling a decommissioned machine into a NEW replica ID — the same
	// operation addressHolder deliberately permits by not counting removed
	// replicas.
	//
	// Not deleted either. Deleting it would leave NOTHING in replicated state to
	// evaluate the removal deny-list against, so the gate could only ever
	// consult the CLAIMED msg.From — and a removed replica still holds its
	// private key, so it authenticates perfectly well and would evade its own
	// tombstone by naming any other replica ID. Retaining it here is what lets the receive path ask the
	// question of the AUTHENTICATED identity instead.
	//
	// The record is UNCONDITIONAL and deterministic, like every other apply-path
	// effect: it is a pure function of (this membership, this committed entry),
	// so every replica computes the identical map and no replica can refuse an
	// entry its peers accepted. It is deliberately NOT gated on anything that
	// could be true on one replica and false on another.
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
	m.recordIdentity(cc)
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
	m.recordIdentity(cc)
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
	// An address already held by another ACTIVE replica would give one host two
	// committed identities in this shard; see addressHolder. Promotion of an
	// observer keeps its own address, so it is exempt.
	if holder, dup := m.addressHolder(cc.Address); dup && holder != cc.ReplicaID {
		return ErrDuplicateMemberAddress
	}
	if err := validateMemberIdentity(m, cc); err != nil {
		return err
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
	if _, dup := m.addressHolder(cc.Address); dup {
		return ErrDuplicateMemberAddress
	}
	return validateMemberIdentity(m, cc)
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

// targetsShard reports whether a config change type changes shard-wide
// state rather than naming a target replica, so a zero ReplicaID is valid.
func targetsShard(t uint64) bool {
	return t == proto.UpdateWitnessPolicy || t == proto.UpdateWireVersion
}

// handleUpdateWireVersion moves the shard's committed wire version forward.
// A value below the current one or outside this build's read window is
// refused with proto.ErrUnsupportedVersion: the replica cannot follow the
// shard, and the caller stops it rather than skip the entry.
func handleUpdateWireVersion(m *membership, cc proto.ConfigChange) error {
	if err := m.wire.CheckCommit(m.wireVersion, cc.WireVersion); err != nil {
		return err
	}
	m.wireVersion = cc.WireVersion
	return nil
}

// validateUpdateWireVersion checks a proposed switch against the same rule
// the apply path enforces, so a leader never commits one it cannot apply.
// That every member advertises the version is the proposer's check: it
// rests on the MaxBinVer the host has seen, which is not replicated state.
// The proposer therefore names the membership it checked by its
// ConfigChangeID, and a switch judged against any other membership is
// refused as pending: a member added since then was never asked what it
// reads. Nothing entered the log, so the proposer re-checks and retries.
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
