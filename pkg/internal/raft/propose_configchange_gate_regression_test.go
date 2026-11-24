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

// A wire-forwarded Propose must pass the full membership validator at the leader's propose gate; the apply path never re-validates.

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

// TestWirePropose_InvalidConfigChangeIsRefused verifies each invalid change is refused with the log and pendingConfigChange unchanged.
func TestWirePropose_InvalidConfigChangeIsRefused(t *testing.T) {
	quietLogs(t)

	cases := []struct {
		name       string
		membership proto.Membership
		change     proto.ConfigChange
		want       error
	}{
		{
			name: "duplicate_identity",
			membership: proto.Membership{
				Addresses:  map[uint64]string{1: "addr1"},
				Identities: map[uint64]string{1: "identity-1"},
			},
			change: proto.ConfigChange{
				Type: proto.AddNode, ReplicaID: 7, Address: "addr7", NodeIdentity: "identity-1",
			},
			want: ErrDuplicateMemberIdentity,
		},
		{
			name:       "duplicate_address",
			membership: proto.Membership{Addresses: map[uint64]string{1: "addr1"}},
			change:     proto.ConfigChange{Type: proto.AddNode, ReplicaID: 7, Address: "addr1"},
			want:       ErrDuplicateMemberAddress,
		},
		{
			name: "removed_replica",
			membership: proto.Membership{
				Addresses: map[uint64]string{1: "addr1"},
				Removed:   map[uint64]bool{9: true},
			},
			change: proto.ConfigChange{Type: proto.AddNode, ReplicaID: 9, Address: "addr9"},
			want:   ErrNodeRemoved,
		},
		{
			name:       "empty_address",
			membership: proto.Membership{Addresses: map[uint64]string{1: "addr1"}},
			change:     proto.ConfigChange{Type: proto.AddNode, ReplicaID: 7},
			want:       ErrEmptyAddress,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := leaderPeerWithMembership(t, tc.membership)
			lastBefore := p.r.log.lastIndex()

			err := proposeConfigChangeOverTheWire(t, p, tc.change)
			if !errors.Is(err, tc.want) {
				t.Fatalf("wire Propose returned %v, want %v", err, tc.want)
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
		})
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

// TestWirePropose_LegitimateConfigChangeStillCommits verifies a valid forwarded config change commits.
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

// TestApplyPath_StillAcceptsACommittedDuplicateIdentity verifies the apply path accepts a committed change the propose gate would refuse.
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
