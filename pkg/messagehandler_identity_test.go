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

package quicraft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// msg.From is bound to the peer's AUTHENTICATED NODE IDENTITY, not its address
//
// The address binding these tests supersede closed registry poisoning but left
// three holes, and each test below is one of them:
//
//  1. A promiscuous certificate (a wildcard SAN, or one certificate carrying
//     every node's SAN) covers EVERY member's committed address, so its holder
//     satisfied the address binding for any replica it chose to name.
//  2. Two replica IDs at one address gave a single host two quorum identities.
//  3. Address IS NOT IDENTITY: a member that recovered at a NEW address had its
//     quorum-bearing responses dropped until a config change committed, which
//     during quorum loss is never.
//
// The identity binding closes 1 and 2 outright and turns 3 from a liveness cost
// into a non-event. What every test here must assert is the DISTINGUISHING
// observable — which message was refused and with which drop reason — because a
// test that only asserts "something was dropped" passes against the address
// binding too, and is therefore worth nothing.
// ---------------------------------------------------------------------------

// identityRig is fromGateRig with a configurable RequireMemberIdentity posture.
// It returns the recording deliverer, the handler, and the metrics sink the
// drop reasons land in, because the reason — not the mere absence of a delivery
// — is what distinguishes this gate from the one it replaced.
func identityRig(t *testing.T, ms proto.Membership, requireIdentity bool) (
	*recordingDeliverer, *hostMessageHandler, *dropReasonMetrics) {
	t.Helper()

	node := newEngineNodeWithFullMembership(t, 1, 1, ms)
	eng := &recordingDeliverer{node: node}
	metrics := &dropReasonMetrics{}
	hostCfg := config.HostConfig{}
	hostCfg.TransportConfig.RequireMemberIdentity = requireIdentity
	host := &Host{
		cfg:      hostCfg,
		registry: registry.NewRegistry(),
		metrics:  metrics,
	}
	return eng, &hostMessageHandler{engine: eng, host: host}, metrics
}

// TestHandleMessage_PromiscuousCertificateCannotAssertAnotherMembersFrom is the
// central regression for Part A.
//
// The attacker is a GENUINE, authenticated member of the shard whose
// certificate is a cluster wildcard: it covers replica 2's committed address
// just as validly as its own. Under the committed-ADDRESS binding that is
// sufficient — the certificate check is `cert covers committed_address`, and it
// passes — so every forged ReplicateResp, HeartbeatResp and RequestVoteResp was
// counted. Under the identity binding the attacker presents ITS OWN keypair,
// which is not the one committed membership records for replica 2, and every
// one is refused with identity_mismatch.
//
// The fixture is the whole point: peer.AuthorizesAddress returns TRUE for the
// victim's address. A test that withheld that would prove nothing here — it
// would pass against the address binding as well.
func TestHandleMessage_PromiscuousCertificateCannotAssertAnotherMembersFrom(t *testing.T) {
	const (
		victimAddr    = "10.0.0.2:5001"
		attackerAddr  = "10.0.0.3:5001"
		victimID      = "spki:1111111111111111111111111111111111111111111111111111111111111111"
		attackerIDStr = "spki:3333333333333333333333333333333333333333333333333333333333333333"
	)
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: "10.0.0.1:5001",
			2: victimAddr,
			3: attackerAddr,
		},
		Identities: map[uint64]string{2: victimID, 3: attackerIDStr},
	}
	// A cluster wildcard certificate: it covers every member's address. Only
	// the keypair behind it is the attacker's own.
	wildcardPeer := certCoveringAs(attackerIDStr, "10.0.0.1:5001", victimAddr, attackerAddr)

	for _, msgType := range quorumBearingTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler, metrics := identityRig(t, ms, false)

			// Sanity: the fixture really is the hard case. If the certificate
			// did not cover the victim's address the address binding would
			// refuse this on its own and the test would prove nothing.
			if !wildcardPeer.Authorizes(victimAddr) {
				t.Fatal("fixture: the attacker's certificate must cover the victim's address")
			}

			batch := proto.MessageBatch{
				SourceAddress: attackerAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(batch, wildcardPeer); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a %s claiming replica 2's From reached raft behind a wildcard "+
					"certificate: the sender is authorized by an address its certificate "+
					"happens to cover, not by the keypair membership committed",
					proto.MessageTypeName(msgType))
			}
			if got := metrics.lastReason(); got != dropIdentityMismatch {
				t.Fatalf("drop reason = %q, want %q: the message must be refused BECAUSE "+
					"the authenticated identity is not the committed one, not for some "+
					"incidental reason that would also fire under the address binding",
					got, dropIdentityMismatch)
			}
		})
	}

	// The attacker speaking as ITSELF is admitted. Without this the test would
	// pass against a gate that refused everything.
	t.Run("own_identity_admitted", func(t *testing.T) {
		eng, handler, _ := identityRig(t, ms, false)
		batch := proto.MessageBatch{
			SourceAddress: attackerAddr,
			Requests: []proto.Message{
				{ShardID: 1, From: 3, To: 1, Type: proto.HeartbeatResp, Term: 5},
			},
		}
		if err := handler.HandleMessage(batch, wildcardPeer); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
		if len(eng.delivered) != 1 {
			t.Fatal("a member speaking under its OWN committed identity was refused")
		}
	})
}

// TestHandleMessage_IdentitySurvivesAnAddressChange is the liveness half, and
// it is the reason identity is the durable fix rather than merely a stronger
// one.
//
// Replica 2 crashes and comes back at a new address — an ephemeral port, a
// rescheduled container, a replaced machine. Its certificate covers the NEW
// address and not the committed one, so the committed-address binding drops
// every quorum-bearing message it sends until a config change carrying the new
// address commits. During quorum loss no config change CAN commit, so under the
// address binding the shard does not self-heal.
//
// The keypair did not change, so the identity did not change, and the traffic
// keeps flowing with no config change at all.
func TestHandleMessage_IdentitySurvivesAnAddressChange(t *testing.T) {
	const (
		committedAddr = "10.0.0.2:5001"
		recoveredAddr = "10.0.0.2:41999"
		victimID      = "spki:2222222222222222222222222222222222222222222222222222222222222222"
	)
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: "10.0.0.1:5001", 2: committedAddr},
		Identities: map[uint64]string{2: victimID},
	}
	// The recovered replica's certificate covers where it IS, not where the
	// configuration still thinks it is.
	recovered := certCoveringAs(victimID, recoveredAddr)

	for _, msgType := range quorumBearingTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler, metrics := identityRig(t, ms, false)

			if recovered.Authorizes(committedAddr) {
				t.Fatal("fixture: the recovered replica's certificate must NOT cover " +
					"its stale committed address, or the address binding would pass too")
			}

			batch := proto.MessageBatch{
				SourceAddress: recoveredAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(batch, recovered); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 1 {
				t.Fatalf("a %s from a member that recovered at a new address was dropped "+
					"(reason %q); its identity is unchanged, so its votes and acks must "+
					"keep counting without waiting for a config change that quorum loss "+
					"makes impossible", proto.MessageTypeName(msgType), metrics.lastReason())
			}
		})
	}
}

// TestHandleMessage_MissingIdentityFollowsTheConfiguredMode pins Part B: what
// happens to a replica the configuration records NO identity for.
//
// Both postures must be exercised, and both must be observable in the drop
// reason. Asserting only the strict case would leave the compatible default
// untested, and a default that silently refused everything would brick every
// existing cluster on upgrade.
func TestHandleMessage_MissingIdentityFollowsTheConfiguredMode(t *testing.T) {
	const legacyAddr = "10.0.0.2:5001"
	// No Identities map at all: the shape of every membership written before
	// identities existed, and of every shard whose operator supplied none.
	ms := proto.Membership{
		Addresses: map[uint64]string{1: "10.0.0.1:5001", 2: legacyAddr},
	}
	// The peer holds the right address and no identity the shard knows about,
	// which is exactly a correctly-behaving un-migrated member.
	legacyPeer := certCovering(legacyAddr)

	t.Run("compatible_default_falls_back_to_the_address", func(t *testing.T) {
		eng, handler, metrics := identityRig(t, ms, false)
		batch := proto.MessageBatch{
			SourceAddress: legacyAddr,
			Requests: []proto.Message{
				{ShardID: 1, From: 2, To: 1, Type: proto.ReplicateResp, Term: 5},
			},
		}
		if err := handler.HandleMessage(batch, legacyPeer); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
		if len(eng.delivered) != 1 {
			t.Fatalf("a member with no committed identity was refused under the "+
				"compatible default (reason %q): every existing cluster would lose "+
				"quorum on upgrade", metrics.lastReason())
		}
	})

	t.Run("strict_mode_refuses_it", func(t *testing.T) {
		eng, handler, metrics := identityRig(t, ms, true)
		batch := proto.MessageBatch{
			SourceAddress: legacyAddr,
			Requests: []proto.Message{
				{ShardID: 1, From: 2, To: 1, Type: proto.ReplicateResp, Term: 5},
			},
		}
		if err := handler.HandleMessage(batch, legacyPeer); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
		if len(eng.delivered) != 0 {
			t.Fatal("RequireMemberIdentity admitted a member with no committed identity " +
				"on the strength of its address alone")
		}
		if got := metrics.lastReason(); got != dropMissingIdentity {
			t.Fatalf("drop reason = %q, want %q: an operator must be able to tell "+
				"an un-migrated member from a forged one", got, dropMissingIdentity)
		}
	})

	t.Run("strict_mode_still_admits_a_migrated_member", func(t *testing.T) {
		const migratedID = "spki:4444444444444444444444444444444444444444444444444444444444444444"
		migrated := proto.Membership{
			Addresses:  map[uint64]string{1: "10.0.0.1:5001", 2: legacyAddr},
			Identities: map[uint64]string{2: migratedID},
		}
		eng, handler, metrics := identityRig(t, migrated, true)
		batch := proto.MessageBatch{
			SourceAddress: legacyAddr,
			Requests: []proto.Message{
				{ShardID: 1, From: 2, To: 1, Type: proto.ReplicateResp, Term: 5},
			},
		}
		if err := handler.HandleMessage(batch, certCoveringAs(migratedID, legacyAddr)); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
		if len(eng.delivered) != 1 {
			t.Fatalf("the strict mode refused a fully migrated member (reason %q)",
				metrics.lastReason())
		}
	})
}

// TestCommittedSenderBinding_IdentityIgnoresAddressEvidence pins the ranking
// directly: once an identity is committed, the address stops being an
// authorization input in BOTH directions. A matching identity is admitted even
// though the certificate covers nothing, and a mismatching one is refused even
// though the certificate covers the committed address perfectly.
//
// The second half is the one that matters. A gate that checked the identity AND
// the address would still be defeated by the promiscuous certificate whenever
// the identity happened to be absent from one side, and a gate that checked the
// address as a fallback AFTER an identity mismatch would be defeated outright.
func TestCommittedSenderBinding_IdentityIgnoresAddressEvidence(t *testing.T) {
	const addr = "10.0.0.2:5001"
	const committedID = "spki:5555555555555555555555555555555555555555555555555555555555555555"
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: "10.0.0.1:5001", 2: addr},
		Identities: map[uint64]string{2: committedID},
	}
	node := newEngineNodeWithFullMembership(t, 1, 1, ms)

	// Right keypair, certificate covers nothing at all.
	value, reason := committedSenderBinding(node, 2, certCoveringAs(committedID), false)
	if reason != "" {
		t.Fatalf("matching identity refused with reason %q (value %q): the address is "+
			"not an authorization input once an identity is committed", reason, value)
	}

	// Wrong keypair, certificate covers the committed address.
	value, reason = committedSenderBinding(node, 2, certCoveringAs("spki:deadbeef", addr), false)
	if reason != dropIdentityMismatch {
		t.Fatalf("mismatching identity behind a covering certificate: reason %q (value %q), "+
			"want %q — falling back to the address here would reopen the whole hole",
			reason, value, dropIdentityMismatch)
	}

	// No identity presented at all (a peer with no verified leaf).
	if _, reason = committedSenderBinding(node, 2, certCovering(addr), false); reason != dropIdentityMismatch {
		t.Fatalf("a peer presenting NO identity was judged %q, want %q", reason, dropIdentityMismatch)
	}
}

// dropReasonMetrics records the reason of the most recent unauthorized-message
// drop. It implements only the metrics surface the authorization path touches;
// everything else is a no-op.
type dropReasonMetrics struct {
	NoOpMetricsCollector
	reason string
}

func (m *dropReasonMetrics) IncUnauthorizedMessageDropped(_, _ uint64, reason string) {
	m.reason = reason
}

func (m *dropReasonMetrics) lastReason() string { return m.reason }
