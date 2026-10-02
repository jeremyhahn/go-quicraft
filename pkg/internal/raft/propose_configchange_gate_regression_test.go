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

// The wire-forwarded Propose was the unvalidated path into the membership
//
// proto.Propose is exempt from the transport's sender binding — a follower
// legitimately forwards client work to the leader — so a Propose reaches
// handleLeaderPropose with msg.From whatever the sender chose, and r.send
// rewrites From to the forwarding replica's own ID on the way, so a forged one
// arrives wearing an honest follower's identity.
//
// handleLeaderPropose sanitised the entry TYPE and enforced the one-pending-
// config-change rule, then ran validateConfigChangePolicy: the voting-member cap
// and the witness policy. Nothing else. No duplicate identity, no duplicate
// address, no Removed tombstone, no empty address. And an undecodable payload
// logged a warning and returned nil, so it entered the log having been judged by
// nothing at all.
//
// Duplicate identity is the sharpest of these. committedSenderBinding
// authenticates purely on peer.Identity == binding.Identity, so two replica IDs
// bound to ONE identity are both satisfiable by one keypair: one physical node
// then holds two votes and two match indices in a quorum that believes they are
// independent, and quorum intersection — the property every Raft safety argument
// rests on — is gone.
//
// The fix is the leader's propose-time gate running the FULL membership
// validator, and nowhere else. It must NOT move to the apply path: handleAddNode
// deliberately does not re-validate, because refusing an already-committed
// config change makes replicas diverge.

// leaderPeerWithMembership returns a single-voter Peer that has been elected
// leader, seeded with ms. A single-voter shard elects itself, which keeps these
// tests about the propose gate rather than about elections.
func leaderPeerWithMembership(t *testing.T, ms proto.Membership) *Peer {
	t.Helper()
	p := newTestPeer(t, testConfigWithID(1, 1), proto.State{}, ms)
	electSingleNodeLeader(t, p)
	if !p.IsLeader() {
		t.Fatal("fixture: the peer must be leader to exercise handleLeaderPropose")
	}
	p.r.drainMessages()
	return p
}

// proposeConfigChangeOverTheWire steps a Propose the way a follower-forwarded
// one arrives: OriginRemote, carrying the leader's term. It is deliberately NOT
// Peer.ProposeConfigChange, which pre-validates — the whole point is the path
// that skips every Host-side check.
func proposeConfigChangeOverTheWire(t *testing.T, p *Peer, cc proto.ConfigChange) error {
	t.Helper()
	return p.r.step(proto.Message{
		Type: proto.Propose, From: 2, To: 1, Term: p.r.term,
		Origin: proto.OriginRemote,
		Entries: []proto.Entry{{
			Type: proto.EntryConfigChange,
			Cmd:  marshalConfigChange(t, cc),
		}},
	})
}

// TestWirePropose_DuplicateIdentityIsRefused is the quorum-forgery test. The
// configuration already binds "identity-1" to replica 1. A wire-forwarded
// Propose adds replica 7 with the SAME identity — so one keypair would
// authenticate as two independent members of the quorum.
func TestWirePropose_DuplicateIdentityIsRefused(t *testing.T) {
	quietLogs(t)

	p := leaderPeerWithMembership(t, proto.Membership{
		Addresses:  map[uint64]string{1: "addr1"},
		Identities: map[uint64]string{1: "identity-1"},
	})
	lastBefore := p.r.log.lastIndex()

	err := proposeConfigChangeOverTheWire(t, p, proto.ConfigChange{
		Type:         proto.AddNode,
		ReplicaID:    7,
		Address:      "addr7",
		NodeIdentity: "identity-1",
	})
	if !errors.Is(err, ErrDuplicateMemberIdentity) {
		t.Fatalf("wire Propose returned %v, want ErrDuplicateMemberIdentity: one "+
			"keypair authorized as two replica IDs holds two votes and two match "+
			"indices in a quorum that believes they are independent", err)
	}
	if p.r.log.lastIndex() != lastBefore {
		t.Fatalf("lastIndex advanced %d -> %d: the entry entered the log, and the "+
			"apply path never re-validates, so every replica applies it",
			lastBefore, p.r.log.lastIndex())
	}
	if p.r.pendingConfigChange {
		t.Fatal("a rejected config change left pendingConfigChange armed, which " +
			"wedges every later membership change on this leader")
	}
}

// TestWirePropose_DuplicateAddressIsRefused covers the address-path equivalent:
// a certificate is matched on the address alone, so two active replicas at one
// committed address let the single host behind it speak as both.
func TestWirePropose_DuplicateAddressIsRefused(t *testing.T) {
	quietLogs(t)

	p := leaderPeerWithMembership(t, proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
	})
	err := proposeConfigChangeOverTheWire(t, p, proto.ConfigChange{
		Type: proto.AddNode, ReplicaID: 7, Address: "addr1",
	})
	if !errors.Is(err, ErrDuplicateMemberAddress) {
		t.Fatalf("wire Propose returned %v, want ErrDuplicateMemberAddress", err)
	}
}

// TestWirePropose_RemovedReplicaCannotBeReAdded covers the tombstone. The
// Removed set is the per-shard DENY-list; a wire Propose that re-adds a
// tombstoned replica un-removes it on every replica, because the apply path
// converges on the committed entry.
func TestWirePropose_RemovedReplicaCannotBeReAdded(t *testing.T) {
	quietLogs(t)

	p := leaderPeerWithMembership(t, proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
		Removed:   map[uint64]bool{9: true},
	})
	err := proposeConfigChangeOverTheWire(t, p, proto.ConfigChange{
		Type: proto.AddNode, ReplicaID: 9, Address: "addr9",
	})
	if !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("wire Propose returned %v, want ErrNodeRemoved: a re-add of a "+
			"tombstoned replica reverses a committed removal", err)
	}
}

// TestWirePropose_EmptyAddressIsRefused covers the structural rule that produces
// an unroutable member: an AddNode with no address commits a voter nothing can
// reach, changing the quorum denominator with no way to satisfy it.
func TestWirePropose_EmptyAddressIsRefused(t *testing.T) {
	quietLogs(t)

	p := leaderPeerWithMembership(t, proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
	})
	err := proposeConfigChangeOverTheWire(t, p, proto.ConfigChange{
		Type: proto.AddNode, ReplicaID: 7,
	})
	if !errors.Is(err, ErrEmptyAddress) {
		t.Fatalf("wire Propose returned %v, want ErrEmptyAddress", err)
	}
}

// TestWirePropose_UndecodablePayloadIsRefused pins that the validator fails
// closed on an unmarshalable payload. Logging a warning and returning nil would
// append the entry having been judged by nothing: every rule above reads fields
// that failed to parse.
func TestWirePropose_UndecodablePayloadIsRefused(t *testing.T) {
	quietLogs(t)

	p := leaderPeerWithMembership(t, proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
	})
	lastBefore := p.r.log.lastIndex()

	err := p.r.step(proto.Message{
		Type: proto.Propose, From: 2, To: 1, Term: p.r.term,
		Origin: proto.OriginRemote,
		Entries: []proto.Entry{{
			Type: proto.EntryConfigChange,
			Cmd:  []byte("not a marshaled config change"),
		}},
	})
	if !errors.Is(err, ErrConfigChangeUnmarshal) {
		t.Fatalf("wire Propose returned %v, want ErrConfigChangeUnmarshal: a "+
			"payload nothing can decode is a payload no gate has judged", err)
	}
	if p.r.log.lastIndex() != lastBefore {
		t.Fatalf("lastIndex advanced %d -> %d on an undecodable config change",
			lastBefore, p.r.log.lastIndex())
	}
}

// TestWirePropose_LegitimateConfigChangeStillCommits is the POSITIVE CONTROL. A
// wire-forwarded Propose is how a client's config change reaches the leader when
// it contacted a follower, and it must keep working — otherwise the gate has
// removed the feature rather than secured it. This is also the answer to
// "should EntryConfigChange in a wire-origin Propose be refused outright?": no,
// because this is the legitimate path it would break.
func TestWirePropose_LegitimateConfigChangeStillCommits(t *testing.T) {
	quietLogs(t)

	p := leaderPeerWithMembership(t, proto.Membership{
		Addresses:  map[uint64]string{1: "addr1"},
		Identities: map[uint64]string{1: "identity-1"},
	})
	lastBefore := p.r.log.lastIndex()

	if err := proposeConfigChangeOverTheWire(t, p, proto.ConfigChange{
		Type:         proto.AddNode,
		ReplicaID:    7,
		Address:      "addr7",
		NodeIdentity: "identity-7",
	}); err != nil {
		t.Fatalf("a legitimate follower-forwarded config change was refused: %v. "+
			"A client that contacted a follower can no longer change membership",
			err)
	}
	if p.r.log.lastIndex() != lastBefore+1 {
		t.Fatalf("lastIndex = %d, want %d: the legitimate change did not reach "+
			"the log", p.r.log.lastIndex(), lastBefore+1)
	}
	if !p.r.pendingConfigChange {
		t.Fatal("pendingConfigChange was not armed for an accepted config change")
	}
}

// TestWirePropose_NormalEntriesAreUntouched is the second control: the gate must
// fire on config changes only. An ordinary data proposal carries arbitrary bytes
// by definition and must never be decoded as a ConfigChange.
func TestWirePropose_NormalEntriesAreUntouched(t *testing.T) {
	quietLogs(t)

	p := leaderPeerWithMembership(t, proto.Membership{
		Addresses: map[uint64]string{1: "addr1"},
	})
	lastBefore := p.r.log.lastIndex()

	if err := p.r.step(proto.Message{
		Type: proto.Propose, From: 2, To: 1, Term: p.r.term,
		Origin: proto.OriginRemote,
		Entries: []proto.Entry{{
			Type: proto.EntryNormal,
			Cmd:  []byte("arbitrary client bytes that decode as nothing"),
		}},
	}); err != nil {
		t.Fatalf("an ordinary data proposal was refused by the config-change "+
			"gate: %v", err)
	}
	if p.r.log.lastIndex() != lastBefore+1 {
		t.Fatalf("lastIndex = %d, want %d", p.r.log.lastIndex(), lastBefore+1)
	}
}

// TestApplyPath_StillAcceptsACommittedDuplicateIdentity is the divergence
// control, and it is the reason the fix lives at the propose gate and NOWHERE
// else. Once an entry is COMMITTED it is agreed replicated state: a replica that
// refuses it holds a membership the rest of the shard does not, and nothing
// later repairs it (configChangeID never advances past a skipped entry). The
// apply path must therefore stay convergent even for a change the propose gate
// would have rejected — an older leader, a rolling upgrade, or a log written
// before the gate existed can all produce one.
func TestApplyPath_StillAcceptsACommittedDuplicateIdentity(t *testing.T) {
	quietLogs(t)

	p := newTestPeer(t, testConfigWithID(1, 1), proto.State{}, proto.Membership{
		Addresses:  map[uint64]string{1: "addr1"},
		Identities: map[uint64]string{1: "identity-1"},
	})

	if err := p.processConfigChanges([]proto.Entry{{
		Index: 2, Term: 1, Type: proto.EntryConfigChange,
		Cmd: marshalConfigChange(t, proto.ConfigChange{
			Type:         proto.AddNode,
			ReplicaID:    7,
			Address:      "addr7",
			NodeIdentity: "identity-1",
		}),
	}}); err != nil {
		t.Fatalf("the apply path refused a COMMITTED config change: %v. Refusing "+
			"one leaves this replica holding a membership no peer shares, and "+
			"configChangeID never advances past it", err)
	}
	if !p.IsMember(7) {
		t.Fatal("a committed AddNode was not applied: the propose-time rule has " +
			"leaked onto the apply path, which is a divergence generator")
	}
}
