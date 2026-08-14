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
// A removal must RETAIN the departing member's identity
//
// handleRemoveNode used to delete it along with the member. That left nothing
// in replicated state for the receive-side deny-list to evaluate against the
// AUTHENTICATED peer identity, so the gate could only ever consult the CLAIMED
// msg.From — which a removed replica evades by naming a different replica ID,
// while still holding the private key that authenticates it perfectly well.
//
// The tests below pin the three properties that make the retained fact usable:
// it is recorded, it is deterministic (so replicas cannot diverge over it), and
// it SURVIVES a snapshot round trip like every other membership fact. The last
// one is not decoration: a tombstone that vanishes on restore is a tombstone
// that reopens the hole at the next restart.
// ---------------------------------------------------------------------------

const (
	testRemovedIdentity = "spki:" +
		"1111111111111111111111111111111111111111111111111111111111111111"
	testSurvivorIdentity = "spki:" +
		"2222222222222222222222222222222222222222222222222222222222222222"
)

// identityMembership is a three-voter shard where two members carry committed
// node identities.
func identityMembership() *membership {
	return newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		Identities: map[uint64]string{
			1: testRemovedIdentity,
			2: testSurvivorIdentity,
		},
	})
}

// TestRemoveNode_RetainsTheDepartingIdentityAsATombstone is the core
// regression. The identity must leave the ACTIVE map (so it no longer
// authorizes, and so recycling the machine into a new replica ID stays legal)
// and land in the tombstone map (so the deny-list can still recognise it).
func TestRemoveNode_RetainsTheDepartingIdentityAsATombstone(t *testing.T) {
	m := identityMembership()
	cc := proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 1}
	if err := m.apply(cc, 10); err != nil {
		t.Fatalf("apply(RemoveNode): %v", err)
	}

	if got, active := m.identities[1]; active {
		t.Fatalf("the removed replica's identity is still in the ACTIVE map as %q: "+
			"identityHolder scans that map, so leaving it there refuses the "+
			"legitimate operation of recycling a decommissioned machine into a new "+
			"replica ID", got)
	}
	if got := m.removedIdentities[1]; got != testRemovedIdentity {
		t.Fatalf("removedIdentities[1] = %q, want %q: without the retained "+
			"identity the removal deny-list can only consult the CLAIMED msg.From, "+
			"which the removed replica evades by naming any other replica ID",
			got, testRemovedIdentity)
	}
	if !m.removed[1] {
		t.Fatal("the removal tombstone itself was not recorded")
	}
	// The survivor is untouched.
	if m.identities[2] != testSurvivorIdentity {
		t.Fatalf("identities[2] = %q, want %q: removing one member must not "+
			"disturb another's binding", m.identities[2], testSurvivorIdentity)
	}
	if _, tombstoned := m.removedIdentities[2]; tombstoned {
		t.Fatal("a live member's identity was tombstoned")
	}
}

// TestRemoveNode_TombstoneLookupIsByIdentityNotByClaimedID pins what the
// receive path actually asks: the question is "is THIS identity removed", with
// no replica ID involved, because the replica ID is the field the attacker
// controls.
func TestRemoveNode_TombstoneLookupIsByIdentityNotByClaimedID(t *testing.T) {
	m := identityMembership()
	if err := m.apply(proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 1}, 10); err != nil {
		t.Fatalf("apply(RemoveNode): %v", err)
	}
	snap := m.snapshot()

	if !snap.isRemovedIdentity(testRemovedIdentity) {
		t.Fatal("the removed identity is not recognised by isRemovedIdentity, so a " +
			"connection presenting it is admitted under any replica ID it cares to claim")
	}
	if snap.isRemovedIdentity(testSurvivorIdentity) {
		t.Fatal("a LIVE member's identity is reported as removed: the shard would " +
			"refuse its own quorum traffic")
	}
	if snap.isRemovedIdentity("") {
		t.Fatal("the empty identity is reported as removed. A connection with no " +
			"authenticated identity must fall through to the gates that can judge " +
			"it; matching it here would deny every pre-identity peer on any shard " +
			"that has ever removed a member")
	}
	if snap.isRemovedIdentity("spki:unknown") {
		t.Fatal("an unrelated identity is reported as removed")
	}
}

// TestRemoveNode_IdentitylessMemberRecordsNoTombstone pins the address-only
// RESIDUAL as a property rather than a comment. A member with no committed
// identity leaves no tombstone identity behind, because there is nothing to
// retain — so on such a shard the removal deny-list remains what it always was,
// a check on the claimed msg.From, and no replicated fact can close it.
func TestRemoveNode_IdentitylessMemberRecordsNoTombstone(t *testing.T) {
	m := identityMembership()
	// Replica 3 was never given an identity.
	if err := m.apply(proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 3}, 10); err != nil {
		t.Fatalf("apply(RemoveNode): %v", err)
	}
	if !m.removed[3] {
		t.Fatal("the removal tombstone itself was not recorded")
	}
	if got, present := m.removedIdentities[3]; present {
		t.Fatalf("removedIdentities[3] = %q for a member that never had a committed "+
			"identity; an empty key in the lookup set would deny every connection "+
			"whose identity cannot be determined", got)
	}
	if m.snapshot().isRemovedIdentity("") {
		t.Fatal("removing an identity-less member made the empty identity denied")
	}
}

// TestRemoveNode_IsUnconditionalAndDeterministic pins the apply-path rule. The
// handlers do NOT re-validate committed changes, because refusing an entry the
// rest of the shard applied makes replicas diverge. The tombstone record must
// therefore be a pure function of (membership, entry) — the same on every
// replica, and the same on a replay.
func TestRemoveNode_IsUnconditionalAndDeterministic(t *testing.T) {
	cc := proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 1}

	a := identityMembership()
	if err := a.apply(cc, 10); err != nil {
		t.Fatalf("apply on replica A: %v", err)
	}
	b := identityMembership()
	if err := b.apply(cc, 10); err != nil {
		t.Fatalf("apply on replica B: %v", err)
	}
	if a.removedIdentities[1] != b.removedIdentities[1] {
		t.Fatalf("two replicas applying the SAME committed entry to the same "+
			"membership recorded different tombstones (%q vs %q): the record is not "+
			"deterministic and the configurations have diverged",
			a.removedIdentities[1], b.removedIdentities[1])
	}

	// A replay of the same committed entry is a no-op (the replay guard), and
	// must not lose the tombstone either.
	if err := a.apply(cc, 10); err != nil {
		t.Fatalf("replay of the committed entry: %v", err)
	}
	if a.removedIdentities[1] != testRemovedIdentity {
		t.Fatalf("replaying the removal lost the tombstone identity: %q",
			a.removedIdentities[1])
	}
}

// TestRemovedIdentities_SurviveASnapshotRoundTrip is the restore half, and it
// is the one that decides whether any of this is durable.
//
// The path exercised is the real one: mutable membership -> published snapshot
// -> proto.Membership -> marshaled bytes -> decoded proto.Membership -> a fresh
// membership built the way a snapshot adopt builds one (raft.Peer.Handle ->
// newMembership). A break anywhere in that chain leaves a restored replica
// enforcing an empty deny-list while every peer enforces a full one.
func TestRemovedIdentities_SurviveASnapshotRoundTrip(t *testing.T) {
	origin := identityMembership()
	if err := origin.apply(proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 1}, 10); err != nil {
		t.Fatalf("apply(RemoveNode): %v", err)
	}

	// Published snapshot -> proto, the shape a Snapshot carries on the wire and
	// a snapshot record carries at rest.
	ms := origin.snapshot().toProto()
	if ms.RemovedIdentities[1] != testRemovedIdentity {
		t.Fatalf("toProto dropped the tombstone identity: %+v", ms.RemovedIdentities)
	}

	// Through the actual encoder and decoder.
	buf := make([]byte, ms.Size())
	n, err := ms.MarshalTo(buf)
	if err != nil {
		t.Fatalf("Membership.MarshalTo: %v", err)
	}
	var decoded proto.Membership
	if _, err := decoded.UnmarshalFrom(buf[:n]); err != nil {
		t.Fatalf("Membership.UnmarshalFrom: %v", err)
	}
	if decoded.RemovedIdentities[1] != testRemovedIdentity {
		t.Fatalf("the marshal/unmarshal round trip lost the tombstone identity: "+
			"%+v. A replica restored from this snapshot would silently enforce an "+
			"empty removal deny-list while its peers enforce a full one",
			decoded.RemovedIdentities)
	}

	// And back into a membership, the way a snapshot adopt rebuilds one.
	restored := newMembership(decoded)
	if !restored.snapshot().isRemovedIdentity(testRemovedIdentity) {
		t.Fatal("a membership rebuilt from the restored snapshot does not recognise " +
			"the removed identity: the tombstone did not survive the restore")
	}
	if restored.identities[2] != testSurvivorIdentity {
		t.Fatalf("the restore lost the surviving member's active identity: %+v",
			restored.identities)
	}
	if _, active := restored.identities[1]; active {
		t.Fatal("the restore put the removed replica's identity back in the ACTIVE map")
	}
}

// TestValidateAdd_RefusesAReAddOfARemovedIdentity pins the ergonomic half of
// "never evict".
//
// The tombstone is on the IDENTITY, not on the replica ID it was bound to, and
// the deny-list refuses that keypair's traffic whatever ID it presents. So an
// add that reuses a removed replica's keypair under a FRESH replica ID would
// commit successfully and then be black-holed by every peer: a voter counted in
// every quorum that can never be heard from, which is worse than a refused
// config change. It must be refused at PROPOSE time, where a refusal is safe.
func TestValidateAdd_RefusesAReAddOfARemovedIdentity(t *testing.T) {
	m := identityMembership()
	if err := m.apply(proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 1}, 10); err != nil {
		t.Fatalf("apply(RemoveNode): %v", err)
	}

	// A brand-new replica ID reusing the removed replica's keypair.
	reAdd := proto.ConfigChange{
		Type:         proto.AddNode,
		ReplicaID:    77,
		Address:      "addr77",
		NodeIdentity: testRemovedIdentity,
	}
	if err := m.validate(reAdd); !errors.Is(err, ErrRemovedMemberIdentity) {
		t.Fatalf("validate(AddNode reusing a removed identity): got %v, want "+
			"ErrRemovedMemberIdentity. Committing it produces a member every "+
			"replica silently drops while counting it toward quorum", err)
	}

	// The same shape for the other two add types, which share the validator.
	for _, ccType := range []uint64{proto.AddNonVoting, proto.AddWitness} {
		cc := reAdd
		cc.Type = ccType
		if err := m.validate(cc); !errors.Is(err, ErrRemovedMemberIdentity) {
			t.Fatalf("validate(%s reusing a removed identity): got %v, want "+
				"ErrRemovedMemberIdentity", proto.ConfigChangeTypeName(ccType), err)
		}
	}

	// POSITIVE CONTROL: a fresh keypair at a fresh replica ID is fine, and so is
	// reusing the departed replica's ADDRESS. Recycling hardware stays legal;
	// only recycling the private key does not.
	fresh := proto.ConfigChange{
		Type:         proto.AddNode,
		ReplicaID:    77,
		Address:      "addr1", // the removed replica's old address
		NodeIdentity: "spki:" + "7777777777777777777777777777777777777777777777777777777777777777",
	}
	if err := m.validate(fresh); err != nil {
		t.Fatalf("validate(AddNode with a fresh keypair at the removed replica's "+
			"old address): %v; recycling a decommissioned machine must stay "+
			"possible, or the tombstone is an operational trap", err)
	}
}
