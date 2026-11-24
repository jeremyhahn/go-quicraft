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
	"strconv"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// A removed replica with its real certificate claiming an unnamed replica ID must be refused by its authenticated identity.

const (
	// removedIdentity is the identity the shard committed for replica 9 before
	// removing it. handleRemoveNode moves it into RemovedIdentities.
	removedIdentity = "spki:" + "9999999999999999999999999999999999999999999999999999999999999999"
	// liveIdentity belongs to replica 2, which is still a member.
	liveIdentity = "spki:" + "2222222222222222222222222222222222222222222222222222222222222222"
)

// tombstonedShardMembership has two live voters, one with an identity, and one removed replica whose identity is retained.
func tombstonedShardMembership() proto.Membership {
	return proto.Membership{
		Addresses: map[uint64]string{
			1: "127.0.0.1:5001",
			2: "127.0.0.1:5002",
		},
		Identities:        map[uint64]string{2: liveIdentity},
		Removed:           map[uint64]bool{9: true},
		RemovedIdentities: map[uint64]string{9: removedIdentity},
	}
}

// TestHandleMessage_RemovedIdentityCannotEvadeItsTombstoneByRenamingItself is
// the attacker test. Replica 9 was removed. It reconnects with its own,
// genuinely valid certificate — nothing about the TLS handshake is wrong — and
// claims a replica ID the configuration does not name, which is the one verdict
// the leader-originated class admits. Every such message must be dropped.
func TestHandleMessage_RemovedIdentityCannotEvadeItsTombstoneByRenamingItself(t *testing.T) {
	const removedAddr = "127.0.0.1:5009"
	// The attacker's real address and keypair.
	attacker := certCoveringAs(removedIdentity, removedAddr)

	// Every claimed ID it could pick. 9 is its own (the claimed-From check
	// catches that one); 2 is a live member (the sender binding catches that
	// one); 77 is named by nothing, which is the gap.
	for _, claimed := range []uint64{9, 2, 77} {
		for _, msgType := range []uint64{proto.Replicate, proto.Heartbeat, proto.HeartbeatResp} {
			t.Run(proto.MessageTypeName(msgType)+"/from-"+strconv.FormatUint(claimed, 10), func(t *testing.T) {
				eng, handler := fromGateRig(t, tombstonedShardMembership(),
					map[uint64]string{2: "127.0.0.1:5002"})
				if !eng.node.IsRemovedIdentity(removedIdentity) {
					t.Fatal("fixture: the shard must hold a tombstone for the attacker's identity")
				}

				batch := proto.MessageBatch{
					SourceAddress: removedAddr,
					Requests: []proto.Message{
						{ShardID: 1, From: claimed, To: 1, Type: msgType, Term: 99},
					},
				}
				if err := handler.HandleMessage(batch, attacker); err != nil {
					t.Fatalf("HandleMessage: %v", err)
				}
				if len(eng.delivered) != 0 {
					t.Fatalf("a replica the shard REMOVED delivered a %s by claiming "+
						"replica %d. Its certificate and its identity are its own and "+
						"genuine; only the replica ID is forged, and the deny-list "+
						"must be evaluated against the authenticated identity for that "+
						"to matter", proto.MessageTypeName(msgType), claimed)
				}
			})
		}
	}
}

// TestHandleMessage_LiveMemberIsStillAcceptedOnATombstonedShard checks a legitimate member is still delivered.
func TestHandleMessage_LiveMemberIsStillAcceptedOnATombstonedShard(t *testing.T) {
	const memberAddr = "127.0.0.1:5002"
	member := certCoveringAs(liveIdentity, memberAddr)

	for _, msgType := range []uint64{proto.Replicate, proto.Heartbeat, proto.HeartbeatResp} {
		t.Run(proto.MessageTypeName(msgType), func(t *testing.T) {
			eng, handler := fromGateRig(t, tombstonedShardMembership(),
				map[uint64]string{2: memberAddr})

			batch := proto.MessageBatch{
				SourceAddress: memberAddr,
				Requests: []proto.Message{
					{ShardID: 1, From: 2, To: 1, Type: msgType, Term: 5},
				},
			}
			if err := handler.HandleMessage(batch, member); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 1 {
				t.Fatalf("delivered %d %s messages from a live member of a shard that "+
					"holds a tombstone, want 1: the identity deny-list is refusing "+
					"traffic it must not, and the shard can no longer replicate",
					len(eng.delivered), proto.MessageTypeName(msgType))
			}
		})
	}
}

// TestHandleMessage_EmptyPeerIdentityIsNotMatchedAgainstTheTombstone checks an empty identity is refused as missing_identity, not as removed.
func TestHandleMessage_EmptyPeerIdentityIsNotMatchedAgainstTheTombstone(t *testing.T) {
	const memberAddr = "127.0.0.1:5002"
	ms := tombstonedShardMembership()
	// Replica 2 has no committed identity on this shard.
	delete(ms.Identities, 2)

	eng, handler, metrics := identityRig(t, ms)
	handler.host.registry.Register(1, 2, memberAddr)
	batch := proto.MessageBatch{
		SourceAddress: memberAddr,
		Requests: []proto.Message{
			{ShardID: 1, From: 2, To: 1, Type: proto.HeartbeatResp, Term: 5},
		},
	}
	// certCovering leaves PeerAuth.Identity empty, which is the whole point.
	if err := handler.HandleMessage(batch, certCovering(memberAddr)); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(eng.delivered) != 0 {
		t.Fatalf("delivered %d messages from an identity-less member: a member is "+
			"authorized by its identity and by nothing else", len(eng.delivered))
	}
	if got := metrics.lastReason(); got != dropMissingIdentity {
		t.Fatalf("drop reason = %q, want %q: an empty authenticated identity was "+
			"matched against a tombstone, so every identity-less peer on a shard "+
			"that has ever removed a member reads as a removed member sneaking back",
			got, dropMissingIdentity)
	}
}
