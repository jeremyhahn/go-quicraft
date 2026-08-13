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

// TestValidateConfigChange_RejectsSpellingVariantDuplicateAddress closes the
// gap the first duplicate-address check documented and left open.
//
// The comparison used to be a literal string compare, so "10.0.0.5:5001" and
// "10.0.0.05:5001" — or an uppercase hostname, or the long form of an IPv6
// literal — were two distinct members to the configuration validator and ONE
// host to x509.VerifyHostname, which is what the receive-side gate ultimately
// matches on. Two committed replica IDs the certificate cannot tell apart is
// exactly the forgery the duplicate check exists to prevent: the single host
// behind them casts two votes and advances two match indices.
//
// Each variant below is a DIFFERENT string from the committed one and the same
// endpoint after canonicalisation.
func TestValidateConfigChange_RejectsSpellingVariantDuplicateAddress(t *testing.T) {
	tests := []struct {
		name      string
		committed string
		variant   string
	}{
		{"uppercase hostname", "node-b.example.com:5001", "NODE-B.EXAMPLE.COM:5001"},
		{"absolute FQDN form", "node-b.example.com:5001", "node-b.example.com.:5001"},
		{"expanded IPv6 literal", "[::1]:5001", "[0:0:0:0:0:0:0:1]:5001"},
		{"IPv4-mapped IPv6 literal", "10.0.0.5:5001", "[::ffff:10.0.0.5]:5001"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.committed == tt.variant {
				t.Fatal("fixture: the variant must be a DIFFERENT string, or a literal " +
					"compare would catch it and the test proves nothing")
			}
			base := proto.Membership{Addresses: map[uint64]string{
				1: "10.0.0.1:5001",
				2: tt.committed,
			}}
			p := newTestPeer(t, testConfig(), proto.State{}, base)

			cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: tt.variant}
			if err := p.ValidateConfigChange(cc); !errors.Is(err, ErrDuplicateMemberAddress) {
				t.Fatalf("ValidateConfigChange(%q against committed %q) = %v, want "+
					"ErrDuplicateMemberAddress: the certificate match treats these as one "+
					"host, so two committed replica IDs at them is one host holding two votes",
					tt.variant, tt.committed, err)
			}
		})
	}

	// A genuinely different endpoint is still allowed. Without this the test
	// would pass against a validator that rejected every add.
	t.Run("distinct address still allowed", func(t *testing.T) {
		p := newTestPeer(t, testConfig(), proto.State{}, proto.Membership{
			Addresses: map[uint64]string{1: "10.0.0.1:5001", 2: "node-b.example.com:5001"},
		})
		cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "node-c.example.com:5001"}
		if err := p.ValidateConfigChange(cc); err != nil {
			t.Fatalf("ValidateConfigChange(distinct address) = %v, want nil", err)
		}
	})

	// A co-located replica — same host, DIFFERENT port — remains supported.
	// Canonicalisation must normalise the spelling, never collapse the port.
	t.Run("same host different port still allowed", func(t *testing.T) {
		p := newTestPeer(t, testConfig(), proto.State{}, proto.Membership{
			Addresses: map[uint64]string{1: "10.0.0.1:5001", 2: "node-b.example.com:5001"},
		})
		cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "NODE-B.example.com:5002"}
		if err := p.ValidateConfigChange(cc); err != nil {
			t.Fatalf("ValidateConfigChange(co-located replica on another port) = %v, want nil: "+
				"two replicas on one machine at different ports are a supported deployment", err)
		}
	})
}

// TestMemberBinding_ReportsASharedSpellingVariantAsNonExclusive is the
// receive-side half. Validation refuses to CREATE a spelling-variant duplicate,
// but a shard seeded through StartShard or restored from an older snapshot can
// already hold one, so the sender binding must fail closed on it rather than
// assume validation ran.
func TestMemberBinding_ReportsASharedSpellingVariantAsNonExclusive(t *testing.T) {
	const lower, upper = "node-b.example.com:5001", "NODE-B.EXAMPLE.COM:5001"
	p := newTestPeer(t, testConfig(), proto.State{}, proto.Membership{
		Addresses: map[uint64]string{1: "10.0.0.1:5001", 2: lower, 4: upper},
	})

	for _, replicaID := range []uint64{2, 4} {
		binding, ok := p.MemberBinding(replicaID)
		if !ok {
			t.Fatalf("MemberBinding(%d) reports the replica as absent", replicaID)
		}
		if binding.Exclusive {
			t.Fatalf("MemberBinding(%d) reports %q as exclusive although replica %d holds "+
				"the same endpoint under another spelling: the one host behind it is "+
				"authorized to speak as both", replicaID, binding.Address, 6-replicaID)
		}
	}

	if binding, ok := p.MemberBinding(1); !ok || !binding.Exclusive {
		t.Fatalf("MemberBinding(1) = (%+v, %v): the uninvolved member must be unaffected",
			binding, ok)
	}
}

// TestValidateConfigChange_RejectsDuplicateMemberIdentity is the identity
// analogue of the duplicate-address rule, and it is strictly more important:
// the identity IS the authorization key once committed, so one identity naming
// two replica IDs means one keypair holds two votes and two match indices in a
// quorum that believes they are independent — with no certificate abuse and no
// forged message anywhere.
//
// Unlike addresses there is no co-location exception. Two replicas on one
// machine still need distinct keypairs; sharing one is precisely the
// promiscuous shape identities exist to defeat.
func TestValidateConfigChange_RejectsDuplicateMemberIdentity(t *testing.T) {
	const sharedIdentity = "spki:aaaa"
	base := proto.Membership{
		Addresses:  map[uint64]string{1: "10.0.0.1:5001", 2: "10.0.0.2:5001"},
		Identities: map[uint64]string{2: sharedIdentity},
	}

	for _, tt := range []struct {
		name   string
		change proto.ConfigChange
	}{
		{"AddNode", proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4,
			Address: "10.0.0.4:5001", NodeIdentity: sharedIdentity}},
		{"AddWitness", proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 4,
			Address: "10.0.0.4:5001", NodeIdentity: sharedIdentity}},
		{"AddNonVoting", proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 4,
			Address: "10.0.0.4:5001", NodeIdentity: sharedIdentity}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestPeer(t, testConfig(), proto.State{}, base)
			if err := p.ValidateConfigChange(tt.change); !errors.Is(err, ErrDuplicateMemberIdentity) {
				t.Fatalf("ValidateConfigChange(%s reusing replica 2's identity) = %v, want "+
					"ErrDuplicateMemberIdentity: one keypair would be authorized as two "+
					"independent members of the quorum", tt.name, err)
			}
		})
	}

	// A DISTINCT identity at a distinct address is allowed. Without this the
	// test would pass against a validator that rejected every identity-bearing
	// add.
	t.Run("distinct identity allowed", func(t *testing.T) {
		p := newTestPeer(t, testConfig(), proto.State{}, base)
		cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4,
			Address: "10.0.0.4:5001", NodeIdentity: "spki:bbbb"}
		if err := p.ValidateConfigChange(cc); err != nil {
			t.Fatalf("ValidateConfigChange(distinct identity) = %v, want nil", err)
		}
	})

	// An add carrying NO identity is allowed: that is the pre-identity
	// behaviour, and refusing it would make an upgraded binary unable to serve
	// the API every existing caller uses.
	t.Run("empty identity allowed", func(t *testing.T) {
		p := newTestPeer(t, testConfig(), proto.State{}, base)
		cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "10.0.0.4:5001"}
		if err := p.ValidateConfigChange(cc); err != nil {
			t.Fatalf("ValidateConfigChange(no identity) = %v, want nil", err)
		}
	})
}

// TestMembershipApply_IdentityReachesTheBindingThroughConsensus pins the
// POPULATION path: an identity supplied on a config change must be visible to
// the receive-side gate once — and only once — that change is applied, and must
// survive the role changes an ordinary cluster performs.
//
// The "no identity leaves an existing one alone" case is the load-bearing one.
// An AddNode with an empty identity is produced by restart replay, by follower
// re-application, and by the StartShard members-map seed, so clearing on empty
// would silently downgrade a migrated replica back to address-only
// authorization — a downgrade an attacker could induce by getting any
// identity-less config change committed for the victim.
func TestMembershipApply_IdentityReachesTheBindingThroughConsensus(t *testing.T) {
	const identity = "spki:cccc"
	m := newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "10.0.0.1:5001"},
	})

	if got := m.snapshot().memberIdentity(4); got != "" {
		t.Fatalf("identity of an absent replica = %q, want empty", got)
	}

	// The committed add binds it.
	if err := m.apply(proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4,
		Address: "10.0.0.4:5001", NodeIdentity: identity}, 10); err != nil {
		t.Fatalf("apply(AddNode with identity): %v", err)
	}
	if got := m.snapshot().memberIdentity(4); got != identity {
		t.Fatalf("committed identity = %q, want %q: the config-change path is the only "+
			"way an identity can be established, so it must actually establish one",
			got, identity)
	}

	// A later identity-less add for the same replica must NOT clear it.
	if err := m.apply(proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4,
		Address: "10.0.0.44:5001"}, 11); err != nil {
		t.Fatalf("apply(AddNode without identity): %v", err)
	}
	if got := m.snapshot().memberIdentity(4); got != identity {
		t.Fatalf("identity after an identity-less re-add = %q, want %q: replay, follower "+
			"re-application and the StartShard seed all produce this entry, and clearing "+
			"on it silently downgrades the replica to address-only authorization",
			got, identity)
	}

	// A role change carries it across untouched.
	if err := m.apply(proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 4,
		Address: "10.0.0.44:5001"}, 12); err != nil {
		t.Fatalf("apply(AddWitness): %v", err)
	}
	if got := m.snapshot().memberIdentity(4); got != identity {
		t.Fatalf("identity after a demotion to witness = %q, want %q", got, identity)
	}

	// Removal takes it with the member: a tombstoned ID must hold no
	// authorization fact of any kind.
	if err := m.apply(proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 4}, 13); err != nil {
		t.Fatalf("apply(RemoveNode): %v", err)
	}
	if got := m.snapshot().memberIdentity(4); got != "" {
		t.Fatalf("identity after removal = %q, want empty", got)
	}
}
