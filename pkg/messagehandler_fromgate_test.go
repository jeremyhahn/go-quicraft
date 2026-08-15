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

// ---------------------------------------------------------------------------
// msg.From must be bound to the AUTHENTICATED sender before raft acts on it
//
// The gate these tests pin is authorizeInbound's sender binding for
// QUORUM-BEARING message types: the connection's verified peer certificate must
// cover the address the shard's COMMITTED MEMBERSHIP assigns to msg.From.
//
// Three earlier shapes of this check were wrong, and the tests below cover all
// of them. The first resolved the address registry AFTER learnSourceAddress had
// overwritten registry[shard, From] with the sender's own address, so it
// compared the attacker's address to itself. The second fixed that ordering but
// still read the registry, which inbound traffic writes — so a non-quorum-bearing
// message could re-point the entry and a follow-up would match it. The third
// read committed membership but compared it to batch.SourceAddress by string
// equality, which the sender chooses and which is independently sourced from the
// committed address, so it partitioned healthy members instead of stopping
// forgery.
// ---------------------------------------------------------------------------

// certCovering models the transport's per-connection certificate binding: the
// senderAuthorized closure passed to HandleMessage answers for ONE inbound
// connection's verified leaf certificate, so it covers that peer's own SANs and
// nothing else.
//
// This is the fixture detail that decides whether these tests prove anything. A
// closure that answers true for every address (authorizeAllSenders) models an
// attacker holding every node's private key, which is not the threat: an
// attacker holding a valid CLUSTER certificate has its OWN identity and is
// trying to speak as another replica.
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

// certCoveringAs is certCovering with an authenticated NODE IDENTITY attached:
// the peer this connection carries is provably the holder of that keypair, and
// its certificate additionally covers the listed addresses.
//
// The two are separate arguments because they are separate facts, and the whole
// point of the identity binding is that the second one stops mattering. A
// promiscuous certificate is modelled by passing the victim's address here
// while passing the ATTACKER's identity.
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

// quorumBearingTypes are the message types that hand the sender a quorum
// outcome. Each one is a complete forgery primitive on its own, so every test
// below drives all six. ReadIndexResp and TimeoutNow are here because accepting
// one is accepting the RESULT of a quorum round that was never run — see
// TestIsQuorumBearing_EveryKnownMessageTypeIsClassified.
var quorumBearingTypes = []uint64{
	proto.ReplicateResp,
	proto.HeartbeatResp,
	proto.RequestVoteResp,
	proto.RequestPreVoteResp,
	proto.ReadIndexResp,
	proto.TimeoutNow,
}

// TestHandleMessage_AuthenticatedPeerCannotForgeAnotherMembersFrom is the core
// regression. Replica 3 is a genuine, authenticated member of shard 1 speaking
// under its OWN certificate, and it claims to be replica 2. Nothing about the
// certificate is wrong; only the replica ID is. Every such message must be
// dropped before it reaches the inbox — one accepted ReplicateResp advances the
// leader's match index for a follower that never received the entries, one
// accepted RequestVoteResp is a vote from a replica that never granted it, one
// accepted ReadIndexResp resolves a linearizable read with no heartbeat quorum
// behind it, and one accepted TimeoutNow starts an election that bypasses both
// PreVote and the leader lease.
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

// TestHandleMessage_RemovedReplicaCannotBypassItsTombstoneWithAForgedFrom pins
// the deny-list's blind spot. IsRemoved inspects the CLAIMED From, so a replica
// the shard has committed a removal for simply stops claiming its own ID and
// asserts a live member's instead. The tombstone alone is no defence; the
// sender binding is what refuses it.
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

// TestHandleMessage_ConfiguredMemberQuorumTrafficStillFlows is the liveness
// half: a member speaking under its own certificate must still have every
// quorum-bearing message delivered. A gate that fails this stops the shard from
// committing or electing at all.
func TestHandleMessage_ConfiguredMemberQuorumTrafficStillFlows(t *testing.T) {
	const memberAddr = "127.0.0.1:5002"
	ms := proto.Membership{Addresses: map[uint64]string{1: "127.0.0.1:5001", 2: memberAddr}}

	for _, msgType := range quorumBearingTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler := fromGateRig(t, ms, map[uint64]string{2: memberAddr})

			batch := proto.MessageBatch{
				SourceAddress: memberAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(batch, certCovering(memberAddr)); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 1 {
				t.Fatalf("delivered %d %s messages from a configured member, want 1: "+
					"the shard can no longer reach quorum",
					len(eng.delivered), proto.MessageTypeName(msgType))
			}
		})
	}
}

// TestHandleMessage_MemberAdvertisingAnEquivalentAddressIsNotPartitioned is the
// regression for a LIVENESS defect the first version of this gate introduced.
//
// batch.SourceAddress is the peer's ADVERTISED address (AdvertiseAddress, else
// the bound local address, else ListenAddress); the committed address is what
// the operator handed StartShard or RequestAddNode. They are independently
// sourced strings for one host, and a certificate legitimately carries several
// SANs — so the DNS spelling and the IP spelling of one node can both be
// cert-valid and unequal.
//
// Comparing them by string equality dropped every quorum-bearing message from
// that member forever: the shard could not commit or elect, and it presented as
// a network partition over a completely healthy network. The old (forgeable)
// code self-healed this because address learning made the comparison reflexive.
// The binding is on the certificate, so both spellings work.
func TestHandleMessage_MemberAdvertisingAnEquivalentAddressIsNotPartitioned(t *testing.T) {
	const committedAddr = "node5.example.com:5001"
	const advertisedAddr = "10.0.0.5:5001"
	ms := proto.Membership{Addresses: map[uint64]string{1: "127.0.0.1:5001", 5: committedAddr}}

	// One host, one certificate, two SANs: both spellings verify.
	bothSpellings := certCovering(committedAddr, advertisedAddr)

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

// TestHandleMessage_MemberAtAnUncoveredAddressIsRefused pins the LIVENESS COST
// the binding accepts on purpose, so it is a deliberate decision rather than an
// accident someone silently "fixes" later.
//
// A member that comes back at an address its certificate does not cover — a
// genuinely new host, not a second spelling of the same one — has its
// quorum-bearing traffic refused until a config change carrying the new address
// COMMITS. During quorum loss no config change can commit, so such a shard does
// not self-heal and an operator has to intervene. Routing is unaffected: this
// host can still SEND to the moved peer.
func TestHandleMessage_MemberAtAnUncoveredAddressIsRefused(t *testing.T) {
	const committedAddr, movedAddr = "10.0.0.5:5001", "10.0.0.9:6001"
	ms := proto.Membership{Addresses: map[uint64]string{1: "127.0.0.1:5001", 5: committedAddr}}
	movedCert := certCovering(movedAddr)

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
	if len(eng.delivered) != 0 {
		t.Fatalf("%d quorum-bearing messages from a member at an address its certificate "+
			"does not cover were delivered, want 0", len(eng.delivered))
	}
	// A refused message teaches nothing — learning runs only after the gate, so
	// a dropped message can never establish the address that would admit it.
	if got, err := handler.host.registry.Resolve(1, 5); err == nil {
		t.Fatalf("a refused quorum-bearing message taught routing %q for replica 5", got)
	}

	// Routing DOES relearn from the member's exempt traffic — its RequestVote
	// when it campaigns — so this host can still reach the moved peer even
	// though it will not count what the peer says back. That is the split the
	// design rests on: routing is learnable, acceptance is not.
	campaign := proto.MessageBatch{
		SourceAddress: movedAddr,
		Requests: []proto.Message{
			{ShardID: 1, From: 5, To: 1, Type: proto.RequestVote, Term: 6},
		},
	}
	if err := handler.HandleMessage(campaign, movedCert); err != nil {
		t.Fatalf("HandleMessage(campaign): %v", err)
	}
	if got, err := handler.host.registry.Resolve(1, 5); err != nil || got != movedAddr {
		t.Fatalf("routing for replica 5 = (%q, %v), want (%q, nil): the moved member is "+
			"unreachable as well as uncounted", got, err, movedAddr)
	}
}

// TestHandleMessage_JoiningReplicaStillLearnsAndAcks is the other liveness
// half, and the reason the gate is scoped to quorum-bearing types only.
//
// A replica joining a shard starts with an EMPTY configuration and an empty
// registry: it knows no members, so a binding on the leader's Replicate would
// drop the very message the join depends on. Replicate carries a term raft
// validates and hands the sender no quorum outcome, so it stays on the
// pre-existing path — the joiner records the leader's address AND steps the
// message, which is what makes the ack possible.
func TestHandleMessage_JoiningReplicaStillLearnsAndAcks(t *testing.T) {
	const leaderAddr = "127.0.0.1:5001"
	_, handler, node := authorizationRigJoining(t, proto.Membership{})

	if node.IsMember(1) {
		t.Fatal("fixture: a joining replica must know no members")
	}
	if _, err := handler.host.registry.Resolve(1, 1); err == nil {
		t.Fatal("fixture: a joining replica must start with no registered addresses")
	}
	before := node.DiagState().InboxLen

	batch := proto.MessageBatch{
		SourceAddress: leaderAddr,
		Requests: []proto.Message{
			{ShardID: 1, From: 1, To: 1, Type: proto.Replicate, Term: 3},
		},
	}
	if err := handler.HandleMessage(batch, certCovering(leaderAddr)); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	// Delivery is the half that matters: an address learned for a message that
	// never reached raft buys the join nothing.
	if node.DiagState().InboxLen == before {
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

// TestHandleMessage_LearnCannotAuthorizeItsOwnMessage pins the single-message
// form of the escalation. A quorum-bearing message from an unknown source used
// to have its address learned first and then be checked against the entry it
// had just written. Neither half may happen now: the claimed From is a
// configured member, but the sender's certificate does not cover that member's
// committed address, so the message is refused — and because the decision
// precedes learning, it also teaches the routing registry nothing on its way
// out.
func TestHandleMessage_LearnCannotAuthorizeItsOwnMessage(t *testing.T) {
	const attackerAddr = "127.0.0.1:5099"
	ms := proto.Membership{Addresses: map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
	}}
	attackerCert := certCovering(attackerAddr)

	for _, msgType := range quorumBearingTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			// Registry is EMPTY: replica 2 has no mapping yet.
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

// TestHandleMessage_RegistryPoisoningCannotUnlockTheQuorumGate is the
// regression for the two-message escalation, and the reason authorization reads
// the COMMITTED MEMBERSHIP rather than the address registry.
//
// While the gate compared against the registry, the escalation was two messages
// long: send any exempt message claiming another member's From, and address
// learning re-points registry[shard, From] at you; the quorum-bearing follow-up
// then matches the entry you just wrote. The thing being checked was the thing
// inbound traffic writes.
//
// The registry is still learnable — routing has to be, so a peer that moved
// stays reachable — so this test lets the poisoning SUCCEED and asserts it buys
// nothing. Committed membership changes only through consensus.
func TestHandleMessage_RegistryPoisoningCannotUnlockTheQuorumGate(t *testing.T) {
	const victimAddr, attackerAddr = "127.0.0.1:5002", "127.0.0.1:5099"
	ms := proto.Membership{Addresses: map[uint64]string{
		1: "127.0.0.1:5001",
		2: victimAddr,
	}}
	attackerCert := certCovering(attackerAddr)

	eng, handler := fromGateRig(t, ms, map[uint64]string{2: victimAddr})

	// Step 1: an exempt message claiming replica 2's From. It is delivered
	// (raft validates its term and it hands the sender no quorum outcome) and it
	// DOES re-point the routing entry — asserted, so the test cannot pass
	// vacuously by the poisoning silently failing.
	poison := proto.MessageBatch{
		SourceAddress: attackerAddr,
		Requests: []proto.Message{
			{ShardID: 1, From: 2, To: 1, Type: proto.RequestVote, Term: 7},
		},
	}
	if err := handler.HandleMessage(poison, attackerCert); err != nil {
		t.Fatalf("HandleMessage(poison): %v", err)
	}
	if got, err := handler.host.registry.Resolve(1, 2); err != nil || got != attackerAddr {
		t.Fatalf("fixture: the registry was not poisoned (resolved %q, err %v), so this "+
			"test would pass without proving anything", got, err)
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

// TestHandleSnapshot_RegistryPoisoningCannotAuthorizeASnapshot is the
// regression for the escalation the message-path fix left open on the snapshot
// channel, and it drives BOTH paths because that is the shape of the attack.
//
// The snapshot path resolved its sender from the routing registry while the
// message path had already been moved onto committed membership. Two messages
// were then enough to replace a replica wholesale:
//
//  1. Send an exempt message (RequestVote) claiming the LEADER's From. Address
//     learning re-points registry[shard, leader] at the attacker.
//  2. Stream a snapshot with From = leader and a Membership of the attacker's
//     choosing. Resolve returned the attacker's own address, the attacker's own
//     certificate covers it, and the snapshot was accepted.
//
// What made it worse than a message forgery: the snapshot is delivered
// OriginLocal, so raft.Peer.Handle's wire-origin refusal does not apply and the
// carried Membership is ADOPTED. That replaces the state machine and rewrites
// the committed addresses the message-path gate reads — the attacker ends up
// having written the table that authorizes it.
//
// Both paths now resolve the sender through committedSenderAddress.
func TestHandleSnapshot_RegistryPoisoningCannotAuthorizeASnapshot(t *testing.T) {
	const leaderAddr, attackerAddr = "127.0.0.1:5001", "127.0.0.1:5099"
	ms := proto.Membership{Addresses: map[uint64]string{
		1: leaderAddr,
		2: "127.0.0.1:5002",
	}}
	attackerCert := certCovering(attackerAddr)

	eng, handler := fromGateRig(t, ms, map[uint64]string{1: leaderAddr})

	// Step 1: poison the routing entry for the leader. This SUCCEEDS — routing
	// stays learnable on purpose — and is asserted so the test cannot pass by
	// the poisoning silently failing.
	poison := proto.MessageBatch{
		SourceAddress: attackerAddr,
		Requests: []proto.Message{
			{ShardID: 1, From: 1, To: 1, Type: proto.RequestVote, Term: 9},
		},
	}
	if err := handler.HandleMessage(poison, attackerCert); err != nil {
		t.Fatalf("HandleMessage(poison): %v", err)
	}
	if got, err := handler.host.registry.Resolve(1, 1); err != nil || got != attackerAddr {
		t.Fatalf("fixture: the registry was not poisoned (resolved %q, err %v), so the "+
			"snapshot step below would prove nothing", got, err)
	}

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

// TestHandleMessage_SharedCommittedAddressFailsClosed pins the defence against
// a configuration that gives ONE address to two active replicas.
//
// Nothing is forged here and no certificate is abused: the host at that address
// legitimately authenticates as itself. But the authorization chain is replica
// ID -> committed address -> certificate, so that one host is authorized to
// speak as BOTH replicas — two votes in an election and two match indices in a
// commit quorum that believes they are independent. Config-change validation
// refuses to create this (see raft.membership.addressHolder), but a shard seeded
// through StartShard or restored from an older snapshot can already be in it, so
// the receive path must fail closed rather than assume validation ran.
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

// quorumBearingClassification is the EXHAUSTIVE expected classification of every
// message type proto defines. The table is driven off proto's own type space
// (see TestIsQuorumBearing_EveryKnownMessageTypeIsClassified), so a type added
// later fails this test until someone decides which side of the gate it is on —
// which is the only mechanism that stops a new response type from silently
// escaping the sender binding, as ReadIndexResp and TimeoutNow both did.
//
// Every EXEMPT entry names the raft handler that proves acceptance hands the
// sender no quorum outcome. "gated" needs no such proof: it is the default and
// the safe direction.
var quorumBearingClassification = map[uint64]struct {
	gated  bool
	reason string
}{
	// --- exempt: peer-to-peer traffic that buys the sender nothing tallied ---
	proto.Replicate: {false, "raft.handleAppend: appends and commits, never tallied. " +
		"Exempt from THIS gate so a replica whose configuration lags can still hear " +
		"the leader that would bring it up to date — and bound instead by the " +
		"leader-originated binding, which refuses a From the configuration NAMES " +
		"but the connection cannot prove (see leaderOriginatedMessages)"},
	proto.Heartbeat: {false, "raft.handleHeartbeat: advances commit, never tallied. " +
		"Exempt from this gate for the same lag reason as Replicate, and bound by " +
		"the leader-originated binding for the same forgery reason"},
	proto.RequestVote: {false, "raft.handleVoteRequest: answered, never tallied. Exempt so " +
		"a member that recovered at a new address can still campaign during quorum loss"},
	proto.RequestPreVote: {false, "raft.handleVoteRequest: answered, never tallied, and " +
		"PreVote does not advance terms"},
	proto.Quiesce:   {false, "raft.handleQuiesceMessage: changes tick intervals only"},
	proto.Propose:   {false, "stepLeader/stepFollower Propose: client work forwarded to the leader"},
	proto.ReadIndex: {false, "raft.handleLeaderReadIndex: a REQUEST for a quorum round, not its result"},
	proto.RateLimit: {false, "raft.handleLeaderRateLimit: advisory follower log-size feedback"},

	// --- gated: accepting one hands the sender a quorum outcome ---
	proto.ReplicateResp:      {true, "tallied: advances matchIndex and commitIndex"},
	proto.HeartbeatResp:      {true, "tallied: advances the ReadIndex ack count"},
	proto.RequestVoteResp:    {true, "tallied: feeds the vote count"},
	proto.RequestPreVoteResp: {true, "tallied: feeds the pre-vote count"},
	proto.ReadIndexResp:      {true, "the wire encoding of a COMPLETED heartbeat quorum round"},
	proto.TimeoutNow:         {true, "the campaign entry point that bypasses PreVote and CheckQuorum"},

	// --- gated: process-local types no peer may send at all ---
	proto.InstallSnapshot: {true, "process-local; raft.step refuses it from the wire"},
	proto.SnapshotStatus:  {true, "process-local; raft.step refuses it from the wire"},
	proto.Unreachable:     {true, "process-local; raft.step refuses it from the wire"},
	proto.LeaderTransfer:  {true, "process-local; raft.step refuses it from the wire"},

	// --- gated: local tick/event types that never travel ---
	proto.LocalTick:         {true, "local: engine tick"},
	proto.Election:          {true, "local: election trigger"},
	proto.LeaderHeartbeat:   {true, "local: heartbeat trigger"},
	proto.ConfigChangeEvent: {true, "local: config change event"},
	proto.NoOP:              {true, "local: no-op"},
	proto.Ping:              {true, "local: liveness probe"},
	proto.Pong:              {true, "local: liveness probe reply"},
	proto.CheckQuorum:       {true, "local: quorum check trigger"},
	proto.BatchedReadIndex:  {true, "local: batched read index trigger"},
	proto.SnapshotReceived:  {true, "local: snapshot receive notification"},
}

// TestIsQuorumBearing_EveryKnownMessageTypeIsClassified drives the
// classification off proto's OWN type space rather than off the handler's list,
// so it cannot pass by testing the author's answer against itself.
//
// This is the test that would have caught ReadIndexResp and TimeoutNow: both
// were on the exempt list with a one-line justification about DIRECTION
// ("leader-to-follower answers/commands"), and direction is not the property
// that matters. Requiring a named raft handler per exemption is what forces the
// question "what does accepting this actually buy the sender?".
func TestIsQuorumBearing_EveryKnownMessageTypeIsClassified(t *testing.T) {
	// proto assigns message types from a small dense range; scan well past it so
	// a newly added constant is discovered here instead of slipping through.
	const scanLimit = 256
	seen := 0
	for msgType := uint64(0); msgType < scanLimit; msgType++ {
		name := proto.MessageTypeName(msgType)
		if name == fmt.Sprintf("Unknown(%d)", msgType) {
			continue // not a type proto defines
		}
		seen++
		want, ok := quorumBearingClassification[msgType]
		if !ok {
			t.Errorf("%s (%d) is defined by proto but not classified here. Decide which "+
				"side of the sender binding it belongs on: if accepting one can hand the "+
				"sender a quorum outcome it must be GATED, and if not, name the raft "+
				"handler that proves it", name, msgType)
			continue
		}
		if got := isQuorumBearing(msgType); got != want.gated {
			t.Errorf("isQuorumBearing(%s) = %v, want %v — %s", name, got, want.gated, want.reason)
		}
	}
	if seen != len(quorumBearingClassification) {
		t.Errorf("classified %d types but proto defines %d in [0,%d): the table has an "+
			"entry for a type proto no longer has", len(quorumBearingClassification), seen, scanLimit)
	}

	// An unrecognised type must fail closed.
	if !isQuorumBearing(9999) {
		t.Error("an unknown message type was classified as non-quorum-bearing; a " +
			"response type added later would escape the sender binding")
	}
}

// TestRegisterIfAbsent_ConcurrentEstablishHasExactlyOneWinner covers the atomic
// primitive the no-overwrite learning path is built on.
//
// The invariant is "establish, never replace". As a Resolve followed by a
// Register it held only when receives were SERIALIZED — which is why a
// sequential test proves nothing about it: two concurrent receives for the same
// (shard, replica) both observe no entry and both store, so the loser's address
// silently replaces the winner's, the exact hijack the check exists to deny.
// Run under -race, N goroutines contend for one key and exactly one must win.
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

// TestRegisterIfAbsent_NeverReplacesAnEstablishedMapping is the sequential
// contract: the return value reports the RETAINED address and that this caller
// did not store it.
func TestRegisterIfAbsent_NeverReplacesAnEstablishedMapping(t *testing.T) {
	reg := registry.NewRegistry()

	existing, stored := reg.RegisterIfAbsent(1, 2, "127.0.0.1:5002")
	if !stored || existing != "127.0.0.1:5002" {
		t.Fatalf("RegisterIfAbsent on an empty entry = (%q, %v), want (%q, true)",
			existing, stored, "127.0.0.1:5002")
	}

	existing, stored = reg.RegisterIfAbsent(1, 2, "10.0.0.9:9999")
	if stored {
		t.Fatal("RegisterIfAbsent replaced an established mapping")
	}
	if existing != "127.0.0.1:5002" {
		t.Fatalf("RegisterIfAbsent returned %q, want the retained %q", existing, "127.0.0.1:5002")
	}
	if got, _ := reg.Resolve(1, 2); got != "127.0.0.1:5002" {
		t.Fatalf("Resolve = %q, want the retained %q", got, "127.0.0.1:5002")
	}
	if got := reg.Count(); got != 1 {
		t.Fatalf("Count = %d after a rejected register, want 1", got)
	}
}
