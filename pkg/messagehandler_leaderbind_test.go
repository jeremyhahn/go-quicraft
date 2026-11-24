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

// Leader-originated messages are bound to the authenticated sender's committed identity.

// leaderOriginatedTypes are the two message types only an established leader
// sends. Both install a leader; both can advance the commit index.
var leaderOriginatedTypes = []uint64{proto.Replicate, proto.Heartbeat}

// TestHandleMessage_LeaderOriginatedFromIsBoundToCommittedIdentity verifies a member cannot send leader messages as another replica.
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

			// Control: the real leader's message gets through.
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

// TestHandleMessage_LeaderOriginatedFromAPendingMemberStillReaches verifies a leader named only in the pending configuration still gets through.
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

// TestHandleMessage_LeaderOriginatedIdentitylessMemberIsRefused verifies a leader with no committed identity is refused.
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

// TestHandleMessage_RemovedReplicaCannotForgeALeaderPush verifies a removed replica claiming a live member's ID is refused.
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

// Routing overwrites are keyed on the authenticated identity.

// TestLearnSourceAddress_OverwriteRequiresTheCommittedIdentity verifies only the committed identity can re-point a member's address.
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

	// Control: replica 2 at a new address can re-point its own entry.
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

// TestLearnSourceAddress_IdentitylessMemberCannotBeRepointed verifies a member without an identity cannot be re-pointed.
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
