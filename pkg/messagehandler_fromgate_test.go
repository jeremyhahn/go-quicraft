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
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// msg.From is bound to the authenticated sender: authorizeInbound requires the verified peer certificate to cover the address committed membership assigns to msg.From.

// certCovering models one connection's verified certificate: it covers that peer's own SANs only.
func certCovering(addresses ...string) transport.PeerAuth {
	covered := make(map[string]struct{}, len(addresses))
	for _, addr := range addresses {
		covered[addr] = struct{}{}
	}
	return transport.PeerAuth{
		AuthorizesAddress: func(addr string) bool {
			_, ok := covered[addr]
			return ok
		},
	}
}

// certCoveringAs is certCovering with an authenticated node identity; a promiscuous certificate is the victim's address with the attacker's identity.
func certCoveringAs(identity string, addresses ...string) transport.PeerAuth {
	peer := certCovering(addresses...)
	peer.Identity = identity
	return peer
}

// fromGateRig builds the smallest handler that exercises the binding: a fake
// deliverer that records what reached the raft inbox, a real committed
// membership (the authorization source), and a routing registry pre-seeded with
// the addresses in registered.
func fromGateRig(t *testing.T, ms proto.Membership, registered map[uint64]string) (
	*recordingDeliverer, *hostMessageHandler) {
	t.Helper()
	node := newEngineNodeWithFullMembership(t, 1, 1, ms)
	reg := registry.NewRegistry()
	for replicaID, addr := range registered {
		reg.Register(1, replicaID, addr)
	}
	eng := &recordingDeliverer{node: node}
	return eng, &hostMessageHandler{engine: eng, host: &Host{registry: reg}}
}

// quorumBearingTypes are the message types that hand the sender a quorum outcome (see TestIsQuorumBearing_EveryKnownMessageTypeIsClassified).
var quorumBearingTypes = []uint64{
	proto.ReplicateResp,
	proto.HeartbeatResp,
	proto.RequestVoteResp,
	proto.RequestPreVoteResp,
	proto.ReadIndexResp,
	proto.TimeoutNow,
}

// TestHandleMessage_AuthenticatedPeerCannotForgeAnotherMembersFrom: a member claiming another replica's ID is dropped before the inbox, for every quorum-bearing type.
func TestHandleMessage_AuthenticatedPeerCannotForgeAnotherMembersFrom(t *testing.T) {
	const victimAddr, attackerAddr = "127.0.0.1:5002", "127.0.0.1:5003"
	ms := proto.Membership{Addresses: map[uint64]string{
		1: "127.0.0.1:5001",
		2: victimAddr,
		3: attackerAddr,
	}}
	attackerCert := certCovering(attackerAddr)

	for _, msgType := range quorumBearingTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler := fromGateRig(t, ms, map[uint64]string{2: victimAddr, 3: attackerAddr})

			batch := proto.MessageBatch{
				SourceAddress: attackerAddr, // the attacker's own, cert-valid address
				Requests: []proto.Message{
					{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(batch, attackerCert); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a %s carrying another member's From reached raft",
					proto.MessageTypeName(msgType))
			}

			// The victim's return path must still point at the victim. If the
			// forged message re-pointed it, replica 2 is black-holed as well.
			if got, err := handler.host.registry.Resolve(1, 2); err != nil || got != victimAddr {
				t.Fatalf("replica 2's registered address is now %q (err %v), want %q: "+
					"its traffic is being routed to the forger", got, err, victimAddr)
			}
		})
	}
}

// TestHandleMessage_RemovedReplicaCannotBypassItsTombstoneWithAForgedFrom: the sender binding refuses a removed replica claiming a live member's ID.
func TestHandleMessage_RemovedReplicaCannotBypassItsTombstoneWithAForgedFrom(t *testing.T) {
	const victimAddr, removedAddr = "127.0.0.1:5002", "127.0.0.1:5009"
	ms := proto.Membership{
		Addresses: map[uint64]string{1: "127.0.0.1:5001", 2: victimAddr},
		Removed:   map[uint64]bool{9: true},
	}
	removedCert := certCovering(removedAddr)

	for _, msgType := range quorumBearingTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler := fromGateRig(t, ms, map[uint64]string{2: victimAddr})
			if !eng.node.IsRemoved(9) {
				t.Fatal("fixture: replica 9 must carry a removal tombstone")
			}

			// Claiming its own (removed) ID is refused by the deny-list.
			ownID := proto.MessageBatch{
				SourceAddress: removedAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 9, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(ownID, removedCert); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatal("the removal tombstone did not stop the removed replica")
			}

			// Claiming a live member's ID must be refused too.
			forged := proto.MessageBatch{
				SourceAddress: removedAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(forged, removedCert); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a removed replica bypassed its tombstone by sending %s under "+
					"a live member's From", proto.MessageTypeName(msgType))
			}
		})
	}
}

// TestHandleMessage_MemberAdvertisingAnEquivalentAddressIsNotPartitioned: a member whose advertised address differs in spelling from its committed one is still accepted.
func TestHandleMessage_MemberAdvertisingAnEquivalentAddressIsNotPartitioned(t *testing.T) {
	const committedAddr = "node5.example.com:5001"
	const advertisedAddr = "10.0.0.5:5001"
	const memberID = "spki:5555555555555555555555555555555555555555555555555555555555555555"
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: "127.0.0.1:5001", 5: committedAddr},
		Identities: map[uint64]string{5: memberID},
	}

	// One host, one keypair, one certificate, two SANs: both spellings verify,
	// and the identity is the same whichever the peer advertises.
	bothSpellings := certCoveringAs(memberID, committedAddr, advertisedAddr)

	for _, msgType := range quorumBearingTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler := fromGateRig(t, ms, nil)

			batch := proto.MessageBatch{
				SourceAddress: advertisedAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 5, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(batch, bothSpellings); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 1 {
				t.Fatalf("delivered %d %s messages from a member whose advertised address "+
					"(%q) is a different, equally cert-valid spelling of its committed "+
					"address (%q), want 1: the shard is partitioned over a healthy network",
					len(eng.delivered), proto.MessageTypeName(msgType), advertisedAddr, committedAddr)
			}
		})
	}
}

// TestHandleMessage_MemberAtANewAddressIsStillCounted: a member at a new address keeps its identity and is counted with no config change.
func TestHandleMessage_MemberAtANewAddressIsStillCounted(t *testing.T) {
	const committedAddr, movedAddr = "10.0.0.5:5001", "10.0.0.9:6001"
	const memberID = "spki:5555555555555555555555555555555555555555555555555555555555555555"
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: "127.0.0.1:5001", 5: committedAddr},
		Identities: map[uint64]string{5: memberID},
	}
	// The certificate covers only the NEW address; the keypair is the same.
	movedCert := certCoveringAs(memberID, movedAddr)

	eng, handler := fromGateRig(t, ms, nil)

	for _, msgType := range quorumBearingTypes {
		batch := proto.MessageBatch{
			SourceAddress: movedAddr,
			Requests: []proto.Message{
				{ShardID: 1, From: 5, To: 1, Type: msgType, Term: 5},
			},
		}
		if err := handler.HandleMessage(batch, movedCert); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
	}
	if len(eng.delivered) != len(quorumBearingTypes) {
		t.Fatalf("%d of %d quorum-bearing messages from a member at a new address were "+
			"delivered: the member kept its keypair, so nothing about it changed",
			len(eng.delivered), len(quorumBearingTypes))
	}
	if got, err := handler.host.registry.Resolve(1, 5); err != nil || got != movedAddr {
		t.Fatalf("routing for replica 5 = (%q, %v), want (%q, nil): the moved member is "+
			"counted but unreachable", got, err, movedAddr)
	}
}

// TestHandleMessage_JoiningReplicaStillLearnsAndAcks: a joiner with an empty configuration accepts the leader named by its join anchor.
func TestHandleMessage_JoiningReplicaStillLearnsAndAcks(t *testing.T) {
	const leaderAddr = "127.0.0.1:5001"
	const leaderID = "spki:1111111111111111111111111111111111111111111111111111111111111111"
	handler, node, rec := authorizationRigRecordingJoining(t, proto.Membership{})
	// The join anchor names the leader and its identity; nothing else can,
	// because the joiner holds no configuration.
	handler.host.markShardJoining(1)
	handler.host.setShardAnchor(1, map[uint64]string{1: leaderID})

	if node.IsMember(1) {
		t.Fatal("fixture: a joining replica must know no members")
	}
	if _, err := handler.host.registry.Resolve(1, 1); err == nil {
		t.Fatal("fixture: a joining replica must start with no registered addresses")
	}

	batch := proto.MessageBatch{
		SourceAddress: leaderAddr,
		Requests: []proto.Message{
			{ShardID: 1, From: 1, To: 1, Type: proto.Replicate, Term: 3},
		},
	}
	if err := handler.HandleMessage(batch, certCoveringAs(leaderID, leaderAddr)); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	// Delivery is checked with rec.DeliveredCount, observed synchronously, since the engine's worker may drain the inbox first.
	if rec.DeliveredCount() == 0 {
		t.Fatal("the leader's Replicate never reached the raft inbox: the joining " +
			"replica has nothing to append and nothing to ack, so the join never " +
			"completes no matter what the registry holds")
	}
	got, err := handler.host.registry.Resolve(1, 1)
	if err != nil || got != leaderAddr {
		t.Fatalf("joining replica did not record the leader's address (resolved %q, err %v): "+
			"it cannot route its ReplicateResp back, so it never acks", got, err)
	}
}

// TestHandleMessage_LearnCannotAuthorizeItsOwnMessage: the decision precedes address learning, so a refused message teaches the registry nothing.
func TestHandleMessage_LearnCannotAuthorizeItsOwnMessage(t *testing.T) {
	const attackerAddr = "127.0.0.1:5099"
	ms := proto.Membership{Addresses: map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
	}}
	attackerCert := certCovering(attackerAddr)

	for _, msgType := range quorumBearingTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			// The registry is empty: replica 2 has no mapping.
			eng, handler := fromGateRig(t, ms, nil)

			batch := proto.MessageBatch{
				SourceAddress: attackerAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(batch, attackerCert); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a %s with no pre-existing mapping was delivered: address "+
					"learning ran before the binding and authorized its own message",
					proto.MessageTypeName(msgType))
			}
			if got, err := handler.host.registry.Resolve(1, 2); err == nil {
				t.Fatalf("a dropped quorum-bearing message still taught the registry "+
					"%q for replica 2", got)
			}
		})
	}
}

// TestHandleMessage_RegistryPoisoningCannotUnlockTheQuorumGate: a poisoned routing registry buys nothing, since authorization reads committed membership.
func TestHandleMessage_RegistryPoisoningCannotUnlockTheQuorumGate(t *testing.T) {
	const victimAddr, attackerAddr = "127.0.0.1:5002", "127.0.0.1:5099"
	const victimID = "spki:2222222222222222222222222222222222222222222222222222222222222222"
	const attackerID = "spki:9999999999999999999999999999999999999999999999999999999999999999"
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: "127.0.0.1:5001", 2: victimAddr},
		Identities: map[uint64]string{2: victimID},
	}
	attackerCert := certCoveringAs(attackerID, attackerAddr)

	eng, handler := fromGateRig(t, ms, map[uint64]string{2: victimAddr})

	// Step 1: an exempt message claiming replica 2's From. It is delivered
	// (raft validates its term and it hands the sender no quorum outcome). With
	// the victim's identity committed the routing overwrite is refused, and
	// that is asserted so a regression in routingOverwriteAuthorized shows up
	// here rather than only in the exfiltration test.
	poison := proto.MessageBatch{
		SourceAddress: attackerAddr,
		Requests: []proto.Message{
			{ShardID: 1, From: 2, To: 1, Type: proto.RequestVote, Term: 7},
		},
	}
	if err := handler.HandleMessage(poison, attackerCert); err != nil {
		t.Fatalf("HandleMessage(poison): %v", err)
	}
	if got, err := handler.host.registry.Resolve(1, 2); err != nil || got != victimAddr {
		t.Fatalf("the registry was re-pointed to %q (err %v) by a peer that does not "+
			"hold replica 2's committed identity", got, err)
	}

	// Step 2: the quorum-bearing follow-up the poisoning was meant to unlock.
	delivered := len(eng.delivered)
	for _, msgType := range quorumBearingTypes {
		followUp := proto.MessageBatch{
			SourceAddress: attackerAddr,
			Requests: []proto.Message{
				{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 7},
			},
		}
		if err := handler.HandleMessage(followUp, attackerCert); err != nil {
			t.Fatalf("HandleMessage(%s): %v", proto.MessageTypeName(msgType), err)
		}
		if len(eng.delivered) != delivered {
			t.Fatalf("a %s was accepted after the registry was re-pointed: authorization "+
				"is reading the mutable routing cache instead of the committed membership",
				proto.MessageTypeName(msgType))
		}
	}

	// The committed membership is untouched by any of it.
	if b, ok := eng.node.MemberBinding(2); !ok || !b.Exclusive || b.Address != victimAddr {
		t.Fatalf("committed membership for replica 2 = (%q, exclusive %v, ok %v), want "+
			"(%q, true, true): inbound traffic reached the authorization source",
			b.Address, b.Exclusive, ok, victimAddr)
	}
}

// TestHandleMessage_QuorumBearingRefusedForUnloadedShard pins the fail-closed
// case. With no loaded node there is no committed membership to authorize
// against, and a quorum outcome can only answer a request a loaded shard sent —
// so unlike bootstrap traffic, it must not pass through.
func TestHandleMessage_QuorumBearingRefusedForUnloadedShard(t *testing.T) {
	reg := registry.NewRegistry()
	reg.Register(1, 2, "127.0.0.1:5002")
	eng := &recordingDeliverer{node: nil} // shard not loaded
	handler := &hostMessageHandler{engine: eng, host: &Host{registry: reg}}

	for _, msgType := range quorumBearingTypes {
		batch := proto.MessageBatch{
			SourceAddress: "127.0.0.1:5002",
			Requests: []proto.Message{
				{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 5},
			},
		}
		if err := handler.HandleMessage(batch, certCovering("127.0.0.1:5002")); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
	}
	if len(eng.delivered) != 0 {
		t.Fatalf("%d quorum-bearing messages were delivered for an unloaded shard, want 0",
			len(eng.delivered))
	}
}

// TestHandleSnapshot_RegistryPoisoningCannotAuthorizeASnapshot: a poisoned registry cannot authorize a snapshot; both paths resolve the sender through committedSenderAddress.
func TestHandleSnapshot_RegistryPoisoningCannotAuthorizeASnapshot(t *testing.T) {
	const leaderAddr, attackerAddr = "127.0.0.1:5001", "127.0.0.1:5099"
	const leaderID = "spki:1111111111111111111111111111111111111111111111111111111111111111"
	const attackerID = "spki:9999999999999999999999999999999999999999999999999999999999999999"
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: leaderAddr, 2: "127.0.0.1:5002"},
		Identities: map[uint64]string{1: leaderID},
	}
	attackerCert := certCoveringAs(attackerID, attackerAddr)

	eng, handler := fromGateRig(t, ms, map[uint64]string{1: leaderAddr})

	// Step 1: try to poison the routing entry for the leader. With the
	// leader's identity committed the overwrite is refused. Either
	// way the snapshot path below must not consult the registry, so the
	// registry is driven to whichever value it will take and the step-2
	// assertion stands on the committed membership alone.
	poison := proto.MessageBatch{
		SourceAddress: attackerAddr,
		Requests: []proto.Message{
			{ShardID: 1, From: 1, To: 1, Type: proto.RequestVote, Term: 9},
		},
	}
	if err := handler.HandleMessage(poison, attackerCert); err != nil {
		t.Fatalf("HandleMessage(poison): %v", err)
	}
	if got, err := handler.host.registry.Resolve(1, 1); err != nil || got != leaderAddr {
		t.Fatalf("the registry was re-pointed to %q (err %v) by a peer that does not "+
			"hold the leader's committed identity", got, err)
	}
	// Poison it directly, the way a registry write from any other source
	// would, so the snapshot step proves the path does not read it.
	handler.host.registry.Register(1, 1, attackerAddr)

	// Step 2: the snapshot the poisoning was meant to unlock, carrying a
	// membership that hands the attacker the shard.
	chunks := []proto.SnapshotChunk{{
		ShardID:    1,
		ReplicaID:  1,
		From:       1, // the leader's identity, which the attacker does not hold
		Index:      500,
		Term:       9,
		ChunkID:    0,
		ChunkCount: 1,
		Data:       []byte("attacker-chosen state machine"),
		Membership: proto.Membership{Addresses: map[uint64]string{
			1: attackerAddr,
			9: attackerAddr,
		}},
	}}

	err := handler.HandleSnapshot(chunks, attackerCert)
	if err == nil {
		t.Fatal("a snapshot claiming the leader's From was ACCEPTED after the routing " +
			"registry was re-pointed: the snapshot path is authorizing against a table " +
			"inbound traffic writes, and the adopted membership replaces the very " +
			"addresses the message-path gate reads")
	}
	var authErr *UnauthorizedMessageError
	if !errors.As(err, &authErr) {
		t.Fatalf("HandleSnapshot error = %T (%v), want *UnauthorizedMessageError", err, err)
	}

	// The committed membership — the authorization source for both paths — is
	// untouched, and the attacker is not in it.
	if b, ok := eng.node.MemberBinding(1); !ok || !b.Exclusive || b.Address != leaderAddr {
		t.Fatalf("committed address of replica 1 = (%q, exclusive %v, ok %v), want "+
			"(%q, true, true)", b.Address, b.Exclusive, ok, leaderAddr)
	}
	if _, ok := eng.node.MemberBinding(9); ok {
		t.Fatal("the snapshot's attacker-chosen membership was adopted")
	}
}

// TestHandleMessage_SharedCommittedAddressFailsClosed: one committed address shared by two active replicas is refused on receive.
func TestHandleMessage_SharedCommittedAddressFailsClosed(t *testing.T) {
	const sharedAddr = "10.0.0.5:5001"
	ms := proto.Membership{Addresses: map[uint64]string{
		1: "127.0.0.1:5001",
		2: sharedAddr,
		4: sharedAddr, // the same host, committed twice
	}}
	sharedCert := certCovering(sharedAddr)

	for _, from := range []uint64{2, 4} {
		eng, handler := fromGateRig(t, ms, nil)
		for _, msgType := range quorumBearingTypes {
			batch := proto.MessageBatch{
				SourceAddress: sharedAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: from, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(batch, sharedCert); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
		}
		if len(eng.delivered) != 0 {
			t.Fatalf("%d quorum-bearing messages claiming From=%d were accepted while two "+
				"active members share the committed address %q: one host is being counted "+
				"as two independent replicas", len(eng.delivered), from, sharedAddr)
		}
	}
}

// TestHandleMessage_EveryMessageTypeIsBoundToASender: across proto's whole type space, a sender no configuration or join anchor names is refused; a configured member is the positive control.
func TestHandleMessage_EveryMessageTypeIsBoundToASender(t *testing.T) {
	const memberAddr, strangerAddr = "127.0.0.1:5002", "127.0.0.1:5099"
	const memberID = "spki:2222222222222222222222222222222222222222222222222222222222222222"
	const strangerID = "spki:9999999999999999999999999999999999999999999999999999999999999999"
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: "127.0.0.1:5001", 2: memberAddr},
		Identities: map[uint64]string{1: fixtureIdentity, 2: memberID},
	}

	// proto assigns message types from a small dense range; scan well past it so
	// a newly added constant is covered here instead of slipping through, and
	// probe one value proto does not define at all.
	const scanLimit = 256
	types := []uint64{9999}
	for msgType := uint64(0); msgType < scanLimit; msgType++ {
		if proto.MessageTypeName(msgType) != fmt.Sprintf("Unknown(%d)", msgType) {
			types = append(types, msgType)
		}
	}
	if len(types) < 10 {
		t.Fatalf("scanned only %d message types; the type space has moved", len(types))
	}

	for _, msgType := range types {
		name := proto.MessageTypeName(msgType)
		t.Run(name, func(t *testing.T) {
			eng, handler := fromGateRig(t, ms, map[uint64]string{2: memberAddr})

			// A stranger naming a replica ID the configuration does not hold.
			stranger := proto.MessageBatch{
				SourceAddress: strangerAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 99, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(stranger, certCoveringAs(strangerID, strangerAddr)); err != nil {
				t.Fatalf("HandleMessage(stranger): %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a %s from a replica no configuration names was delivered", name)
			}
			// The same stranger claiming a member's ID.
			forged := stranger
			forged.Requests = []proto.Message{{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 5}}
			if err := handler.HandleMessage(forged, certCoveringAs(strangerID, strangerAddr)); err != nil {
				t.Fatalf("HandleMessage(forged): %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a %s claiming member 2's From was delivered from a connection "+
					"that does not hold member 2's identity", name)
			}
			// Positive control: the member itself.
			genuine := proto.MessageBatch{
				SourceAddress: memberAddr,
				Requests:      []proto.Message{{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 5}},
			}
			if err := handler.HandleMessage(genuine, certCoveringAs(memberID, memberAddr)); err != nil {
				t.Fatalf("HandleMessage(genuine): %v", err)
			}
			if _, hostTraffic := leaderExecReceivers[msgType]; hostTraffic {
				// Past the gate this type goes to the host's router, never to
				// raft; TestHandleMessage_LeaderExecTrafficIsBoundToASender
				// holds its positive control.
				if len(eng.delivered) != 0 {
					t.Fatalf("a %s reached raft: it is host traffic", name)
				}
				return
			}
			if len(eng.delivered) != 1 {
				t.Fatalf("a %s from a configured member on its own connection was "+
					"dropped (%d delivered): the gate refuses traffic it must not",
					name, len(eng.delivered))
			}
		})
	}
}

// TestRegisterIfAbsent_ConcurrentEstablishHasExactlyOneWinner: under -race, exactly one of N concurrent registrations for a key wins.
func TestRegisterIfAbsent_ConcurrentEstablishHasExactlyOneWinner(t *testing.T) {
	// One contended key is a single coin flip and passes against a
	// Resolve-then-Register roughly as often as not, which is worse than no test
	// at all. Every key below is an independent trial and they all start at
	// once, so the window is hit many times over.
	const keys, racersPerKey = 128, 8
	reg := registry.NewRegistry()

	type outcome struct {
		winners  int
		returned map[string]int
	}
	var (
		start    = make(chan struct{})
		wg       sync.WaitGroup
		mu       sync.Mutex
		outcomes = make(map[uint64]*outcome, keys)
	)
	for key := uint64(1); key <= keys; key++ {
		outcomes[key] = &outcome{returned: make(map[string]int)}
		for i := 0; i < racersPerKey; i++ {
			wg.Add(1)
			go func(key uint64, i int) {
				defer wg.Done()
				<-start
				existing, stored := reg.RegisterIfAbsent(1, key, fmt.Sprintf("10.0.0.%d:%d", i, key))
				mu.Lock()
				defer mu.Unlock()
				o := outcomes[key]
				if stored {
					o.winners++
				}
				o.returned[existing]++
			}(key, i)
		}
	}
	close(start)
	wg.Wait()

	for key, o := range outcomes {
		if o.winners != 1 {
			t.Fatalf("replica %d: %d callers were told they stored the mapping, want exactly "+
				"1: concurrent first contacts can replace each other's address", key, o.winners)
		}
		if len(o.returned) != 1 {
			t.Fatalf("replica %d: callers observed %d different surviving addresses %v, want "+
				"1: the entry was replaced after it was established", key, len(o.returned), o.returned)
		}
		surviving, err := reg.Resolve(1, key)
		if err != nil {
			t.Fatalf("replica %d: Resolve: %v", key, err)
		}
		if _, ok := o.returned[surviving]; !ok {
			t.Fatalf("replica %d: Resolve = %q but no caller was told that address survived %v",
				key, surviving, o.returned)
		}
	}
	if got := reg.Count(); got != keys {
		t.Fatalf("Count = %d, want %d: the entry count diverged from the map", got, keys)
	}
}
