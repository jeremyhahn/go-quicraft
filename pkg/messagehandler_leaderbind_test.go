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
// Replicate and Heartbeat are not TALLIED, so they are not quorum-bearing — and
// they were therefore left with no sender binding at all. That was the largest
// remaining hole in the receive path. handleHeartbeat carries no prevLogIndex,
// no prevLogTerm and no entries, yet it set r.leaderID = msg.From and committed
// up to a Commit index the sender named. Any holder of a CA-valid certificate
// could send one frame claiming to be the leader and commit a follower's
// minority-replicated tail, which the real leader then truncates: a State
// Machine Safety violation.
//
// The binding is deliberately WEAKER than the quorum-bearing one in exactly one
// verdict — a sender the committed configuration does not name at all is still
// admitted, because a lagging replica has not applied the config change that
// added the current leader, and refusing there is the member allow-list that
// wedged replicas out of their own shard in round 10. Every test below covers
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

// TestHandleMessage_LeaderOriginatedFromAnUnnamedReplicaStillReaches is the
// LIVENESS control, and it is the reason the binding is not simply the
// quorum-bearing one.
//
// This replica's committed configuration names only replicas 1 and 2. The shard
// has since added replica 4 and elected it, but that config change has not
// replicated here yet — which is the ordinary state of a lagging follower.
// Replica 4's Replicate and Heartbeat are the only things that can repair it. If
// they are refused because the configuration does not name the sender, the
// replica can never catch up: that is the member allow-list, and round 10 turned
// it into a permanently leaderless shard.
func TestHandleMessage_LeaderOriginatedFromAnUnnamedReplicaStillReaches(t *testing.T) {
	const selfAddr, knownAddr, newLeaderAddr = "127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5004"
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: selfAddr, 2: knownAddr},
		Identities: map[uint64]string{1: "identity-1", 2: "identity-2"},
	}
	newLeaderCert := certCoveringAs("identity-4", newLeaderAddr)

	for _, msgType := range leaderOriginatedTypes {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler := fromGateRig(t, ms, map[uint64]string{2: knownAddr})
			if _, ok := eng.node.MemberBinding(4); ok {
				t.Fatal("fixture: the committed configuration must NOT name replica 4")
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
				t.Fatalf("a %s from a leader this replica's configuration does not "+
					"name yet was dropped (%d delivered): a lagging replica can no "+
					"longer be brought up to date, which is the wedge the deny-list "+
					"model exists to avoid", proto.MessageTypeName(msgType),
					len(eng.delivered))
			}
		})
	}
}

// TestHandleMessage_LeaderOriginatedAddressPathStillBinds covers a shard with
// NO committed identities — the pre-identity compatibility path. The binding
// falls back to the committed address, exactly as it does for quorum-bearing
// types, so a certificate that does not cover the leader's committed address
// cannot speak as the leader.
func TestHandleMessage_LeaderOriginatedAddressPathStillBinds(t *testing.T) {
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
					"certificate that does not cover the leader's COMMITTED address",
					proto.MessageTypeName(msgType))
			}

			// POSITIVE CONTROL.
			if err := handler.HandleMessage(proto.MessageBatch{
				SourceAddress: leaderAddr,
				Requests: []proto.Message{{
					ShardID: 1, From: 2, To: 1, Type: msgType, Term: 9, Commit: 4,
				}},
			}, leaderCert); err != nil {
				t.Fatalf("HandleMessage (control): %v", err)
			}
			if len(eng.delivered) != 1 {
				t.Fatalf("the REAL leader's %s was dropped on the address path "+
					"(%d delivered)", proto.MessageTypeName(msgType), len(eng.delivered))
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
// exfiltration test. learnFromSource used to allow an OVERWRITE whenever the
// CLAIMED msg.From named a member, so any authenticated peer re-pointed a
// member's routing entry at itself. That was written off as a black-hole
// corrected by the next config change; it is worse than that. The attacker holds
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

// TestLearnSourceAddress_AddressOnlyShardKeepsTheOldOverwriteRule is the
// compatibility control. A shard with no committed identities has nothing to key
// the overwrite on, and matching the certificate against the COMMITTED address
// would refuse exactly the member that moved — taking the RequestVote that has
// to teach voters where to send the grant with it. The pre-identity behaviour is
// therefore left in place, and this test says so out loud so the residual is not
// mistaken for an oversight.
func TestLearnSourceAddress_AddressOnlyShardKeepsTheOldOverwriteRule(t *testing.T) {
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
	if got, err := handler.host.registry.Resolve(1, 2); err != nil || got != victimNewAddr {
		t.Fatalf("replica 2's routing entry is %q (err %v), want %q: on a shard "+
			"with no committed identities the overwrite must keep working, or a "+
			"member that moved is unreachable for the life of the partition",
			got, err, victimNewAddr)
	}
}
