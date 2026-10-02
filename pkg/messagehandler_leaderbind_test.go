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

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Leader-originated messages must be bound to the authenticated sender
//
// Replicate and Heartbeat are not TALLIED, so they are not quorum-bearing — but
// leaving them with no sender binding would be the largest hole in the receive
// path. handleHeartbeat carries no prevLogIndex, no prevLogTerm and no entries,
// yet it sets r.leaderID = msg.From and commits up to a Commit index the sender
// names. Without a binding, any holder of a CA-valid certificate could send one
// frame claiming to be the leader and commit a follower's minority-replicated
// tail, which the real leader then truncates: a State Machine Safety
// violation.
//
// The binding is deliberately WEAKER than the quorum-bearing one in exactly one
// verdict — a sender the committed configuration does not name at all is still
// admitted, because a lagging replica has not applied the config change that
// added the current leader, and refusing there is the member allow-list that
// wedges replicas out of their own shard. Every test below covers
// both halves: the forgery is refused AND the lagging-replica first contact
// still works.
// ---------------------------------------------------------------------------

// leaderOriginatedTypes are the two message types only an established leader
// sends. Both install a leader; both can advance the commit index.
var leaderOriginatedTypes = []uint64{proto.Replicate, proto.Heartbeat}

// TestHandleMessage_LeaderOriginatedFromIsBoundToCommittedIdentity is the
// attacker test. Replica 3 is a genuine authenticated member of shard 1 holding
// its own valid certificate and its own committed identity. It claims to be
// replica 2 — the leader — and sends a Heartbeat and a Replicate. Nothing about
// the connection is forged; only msg.From is.
//
// The POSITIVE CONTROL in the same subtest is what makes the assertion mean
// something: the real replica 2, on a connection carrying replica 2's committed
// identity, sends the identical message and it must be DELIVERED.
func TestHandleMessage_LeaderOriginatedFromIsBoundToCommittedIdentity(t *testing.T) {
	const leaderAddr, attackerAddr = "127.0.0.1:5002", "127.0.0.1:5003"
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: "127.0.0.1:5001",
			2: leaderAddr,
			3: attackerAddr,
		},
		Identities: map[uint64]string{
			1: "identity-1",
			2: "identity-2",
			3: "identity-3",
		},
	}
	attackerCert := certCoveringAs("identity-3", attackerAddr)
	leaderCert := certCoveringAs("identity-2", leaderAddr)

	for _, msgType := range leaderOriginatedTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler := fromGateRig(t, ms, map[uint64]string{2: leaderAddr, 3: attackerAddr})

			forged := proto.MessageBatch{
				SourceAddress: attackerAddr, // its own, cert-valid address
				Requests: []proto.Message{{
					ShardID: 1, From: 2, To: 1, Type: msgType,
					Term: 9, Commit: 1 << 40,
				}},
			}
			if err := handler.HandleMessage(forged, attackerCert); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a %s claiming the leader's From reached raft from a "+
					"connection carrying a DIFFERENT committed identity: one such "+
					"frame installs a forged leader and commits this replica's "+
					"uncommitted tail", proto.MessageTypeName(msgType))
			}
			// The leader's return path must not have been re-pointed either.
			if got, err := handler.host.registry.Resolve(1, 2); err != nil || got != leaderAddr {
				t.Fatalf("replica 2's registered address is now %q (err %v), want %q",
					got, err, leaderAddr)
			}

			// POSITIVE CONTROL: the real leader, same message, must get through.
			legit := proto.MessageBatch{
				SourceAddress: leaderAddr,
				Requests: []proto.Message{{
					ShardID: 1, From: 2, To: 1, Type: msgType, Term: 9, Commit: 4,
				}},
			}
			if err := handler.HandleMessage(legit, leaderCert); err != nil {
				t.Fatalf("HandleMessage (control): %v", err)
			}
			if len(eng.delivered) != 1 {
				t.Fatalf("the REAL leader's %s was dropped (%d delivered): the "+
					"binding has become an allow-list and this replica is wedged "+
					"out of its own shard", proto.MessageTypeName(msgType),
					len(eng.delivered))
			}
		})
	}
}

// TestHandleMessage_LeaderOriginatedFromAPendingMemberStillReaches is the
// LIVENESS control, and the reason the binding consults the PENDING
// configuration as well as the committed one.
//
// This replica's committed configuration names only replicas 1 and 2. The shard
// has since added replica 4 and elected it. The AddNode that admits 4 reached
// this replica from leader 2 and sits in its log, uncommitted — the ordinary
// state of a lagging follower. Replica 4's Replicate and Heartbeat are the only
// things that can commit it. If they are refused because the COMMITTED
// configuration does not name the sender, the replica can never catch up: that
// is the member allow-list, which can leave a shard permanently leaderless. If
// they were admitted because NO configuration names the sender, any
// certificate holder could be replica 4. The pending
// entry, with the identity it carries, is the fact between the two.
func TestHandleMessage_LeaderOriginatedFromAPendingMemberStillReaches(t *testing.T) {
	const selfAddr, knownAddr, newLeaderAddr = "127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5004"
	const newLeaderID = "spki:4444444444444444444444444444444444444444444444444444444444444444"
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: selfAddr, 2: knownAddr},
		Identities: map[uint64]string{1: "identity-1", 2: "identity-2"},
	}
	newLeaderCert := certCoveringAs(newLeaderID, newLeaderAddr)

	for _, msgType := range leaderOriginatedTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler := fromGateRig(t, ms, map[uint64]string{2: knownAddr})
			if _, ok := eng.node.MemberBinding(4); ok {
				t.Fatal("fixture: the committed configuration must NOT name replica 4")
			}
			appendPendingAdd(t, eng.node, 2, 9, 4, newLeaderAddr, newLeaderID)
			if eng.node.IsMember(4) {
				t.Fatal("fixture: the AddNode must be pending, not applied")
			}

			batch := proto.MessageBatch{
				SourceAddress: newLeaderAddr,
				Requests: []proto.Message{{
					ShardID: 1, From: 4, To: 1, Type: msgType, Term: 9, Commit: 3,
				}},
			}
			if err := handler.HandleMessage(batch, newLeaderCert); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 1 {
				t.Fatalf("a %s from a leader this replica's own log admits but has not "+
					"committed was dropped (%d delivered): a lagging replica can no "+
					"longer be brought up to date, which is the wedge the pending "+
					"configuration exists to avoid", proto.MessageTypeName(msgType),
					len(eng.delivered))
			}
		})
	}
}

// TestHandleMessage_LeaderOriginatedIdentitylessMemberIsRefused covers a shard
// whose configuration names the leader WITHOUT an identity. There is no
// fallback to the committed address — a wildcard or shared-SAN certificate
// would satisfy it for every member at once. Admission requires the identity,
// and a leader-originated message from an identity-less member is refused
// whichever certificate carries it: a configuration in this shape was written
// by a build that did not require identities, and the remedy is re-bootstrap,
// not a weaker gate.
func TestHandleMessage_LeaderOriginatedIdentitylessMemberIsRefused(t *testing.T) {
	const leaderAddr, attackerAddr = "127.0.0.1:5002", "127.0.0.1:5003"
	ms := proto.Membership{Addresses: map[uint64]string{
		1: "127.0.0.1:5001",
		2: leaderAddr,
		3: attackerAddr,
	}}
	attackerCert := certCovering(attackerAddr)
	leaderCert := certCovering(leaderAddr)

	for _, msgType := range leaderOriginatedTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler := fromGateRig(t, ms, map[uint64]string{2: leaderAddr})

			if err := handler.HandleMessage(proto.MessageBatch{
				SourceAddress: attackerAddr,
				Requests: []proto.Message{{
					ShardID: 1, From: 2, To: 1, Type: msgType, Term: 9, Commit: 1 << 40,
				}},
			}, attackerCert); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a %s claiming the leader's From was accepted behind a "+
					"certificate that does not cover the leader's committed address",
					proto.MessageTypeName(msgType))
			}

			// The real leader's certificate covers its address and proves no
			// identity the configuration holds: refused too, for the reason
			// that is true.
			if err := handler.HandleMessage(proto.MessageBatch{
				SourceAddress: leaderAddr,
				Requests: []proto.Message{{
					ShardID: 1, From: 2, To: 1, Type: msgType, Term: 9, Commit: 4,
				}},
			}, leaderCert); err != nil {
				t.Fatalf("HandleMessage (identity-less leader): %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a %s from a member the configuration holds no identity for "+
					"was admitted on its address (%d delivered): that is the fallback a "+
					"promiscuous certificate satisfies for every member",
					proto.MessageTypeName(msgType), len(eng.delivered))
			}
		})
	}
}

// TestHandleMessage_RemovedReplicaCannotForgeALeaderPush is defect 3's residual,
// pinned to what actually remains. The deny-list is evaluated on the CLAIMED
// From, so a replica the shard removed evades it by naming a live member's ID.
// What it CANNOT do any more is have that message accepted: the leader-originated
// binding checks the authenticated identity, and a removal deletes the removed
// replica's identity, so it can never present the victim's.
func TestHandleMessage_RemovedReplicaCannotForgeALeaderPush(t *testing.T) {
	const leaderAddr, removedAddr = "127.0.0.1:5002", "127.0.0.1:5009"
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: "127.0.0.1:5001", 2: leaderAddr},
		Identities: map[uint64]string{1: "identity-1", 2: "identity-2"},
		Removed:    map[uint64]bool{9: true},
	}
	removedCert := certCoveringAs("identity-9", removedAddr)

	for _, msgType := range leaderOriginatedTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler := fromGateRig(t, ms, map[uint64]string{2: leaderAddr})
			if !eng.node.IsRemoved(9) {
				t.Fatal("fixture: replica 9 must carry a removal tombstone")
			}

			if err := handler.HandleMessage(proto.MessageBatch{
				SourceAddress: removedAddr,
				Requests: []proto.Message{{
					ShardID: 1, From: 2, To: 1, Type: msgType, Term: 9, Commit: 1 << 40,
				}},
			}, removedCert); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a removed replica evaded its tombstone by claiming the "+
					"leader's From and its %s was accepted",
					proto.MessageTypeName(msgType))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Routing overwrite must be keyed on the AUTHENTICATED identity (defect 3)
// ---------------------------------------------------------------------------

// TestLearnSourceAddress_OverwriteRequiresTheCommittedIdentity is the
// exfiltration test. If learnFromSource allowed an OVERWRITE whenever the
// CLAIMED msg.From named a member, any authenticated peer could re-point a
// member's routing entry at itself. That is not a black-hole corrected by the
// next config change; it is worse than that. The attacker holds
// a CA-valid certificate, so this host's outbound dial to the poisoned entry
// SUCCEEDS, and it then receives the victim's Replicate frames — committed log
// entries streamed to a replica the configuration never named.
//
// A message type with NO sender binding at all is used deliberately (RequestVote
// is exempt for a real liveness reason and cannot be gated), so the test proves
// the LEARNING gate, not the acceptance gate.
func TestLearnSourceAddress_OverwriteRequiresTheCommittedIdentity(t *testing.T) {
	const victimAddr, attackerAddr = "127.0.0.1:5002", "127.0.0.1:5003"
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: "127.0.0.1:5001",
			2: victimAddr,
			3: attackerAddr,
		},
		Identities: map[uint64]string{
			1: "identity-1",
			2: "identity-2",
			3: "identity-3",
		},
	}

	eng, handler := fromGateRig(t, ms, map[uint64]string{2: victimAddr, 3: attackerAddr})
	_ = eng

	// Replica 3, authenticated as itself, claims to be replica 2.
	if err := handler.HandleMessage(proto.MessageBatch{
		SourceAddress: attackerAddr,
		Requests: []proto.Message{{
			ShardID: 1, From: 2, To: 1, Type: proto.RequestVote, Term: 9,
		}},
	}, certCoveringAs("identity-3", attackerAddr)); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if got, err := handler.host.registry.Resolve(1, 2); err != nil || got != victimAddr {
		t.Fatalf("replica 2's routing entry is now %q (err %v), want %q: every "+
			"Replicate this host sends replica 2 — committed log entries — now "+
			"goes to a peer the configuration does not name as replica 2",
			got, err, victimAddr)
	}

	// POSITIVE CONTROL: replica 2 itself, recovered at a NEW address, must still
	// be able to re-point its own entry. Its identity is unchanged by the move,
	// which is exactly why the identity is the right key: this is the liveness
	// case the overwrite exists for, and it must survive during quorum loss when
	// no config change can commit the new address.
	const victimNewAddr = "127.0.0.1:6002"
	if err := handler.HandleMessage(proto.MessageBatch{
		SourceAddress: victimNewAddr,
		Requests: []proto.Message{{
			ShardID: 1, From: 2, To: 1, Type: proto.RequestVote, Term: 9,
		}},
	}, certCoveringAs("identity-2", victimNewAddr)); err != nil {
		t.Fatalf("HandleMessage (control): %v", err)
	}
	if got, err := handler.host.registry.Resolve(1, 2); err != nil || got != victimNewAddr {
		t.Fatalf("replica 2's routing entry is %q (err %v), want %q: a member "+
			"that recovered at a new address can no longer teach this host where "+
			"to send its vote response", got, err, victimNewAddr)
	}
}

// TestLearnSourceAddress_IdentitylessMemberCannotBeRepointed: on a shard whose
// configuration holds no identity for a member, letting ANY certificate holder
// naming that member re-point its routing entry would let it receive its share
// of the shard's Replicate traffic. The overwrite requires the committed
// identity;
// with none committed, nobody may replace the entry, and the member that moved
// is reached again through the config change that commits its identity and new
// address, which is the same remedy an identity-less shard needs anyway.
func TestLearnSourceAddress_IdentitylessMemberCannotBeRepointed(t *testing.T) {
	const victimAddr, victimNewAddr = "127.0.0.1:5002", "127.0.0.1:6002"
	ms := proto.Membership{Addresses: map[uint64]string{
		1: "127.0.0.1:5001",
		2: victimAddr,
	}}

	_, handler := fromGateRig(t, ms, map[uint64]string{2: victimAddr})
	if err := handler.HandleMessage(proto.MessageBatch{
		SourceAddress: victimNewAddr,
		Requests: []proto.Message{{
			ShardID: 1, From: 2, To: 1, Type: proto.RequestVote, Term: 9,
		}},
	}, certCovering(victimNewAddr)); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if got, err := handler.host.registry.Resolve(1, 2); err != nil || got != victimAddr {
		t.Fatalf("replica 2's routing entry is %q (err %v), want %q: a member the "+
			"configuration holds no identity for had its route re-pointed by a "+
			"certificate holder that proved nothing about it",
			got, err, victimAddr)
	}
}
