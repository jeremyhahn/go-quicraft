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
	"bytes"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// The witness data path is driven by the replicated membership alone.

// TestWitnessDataPath_FollowsReplicatedMembership verifies the leader chooses the witness data path from Membership.Witnesses, not the target's IsWitness.
func TestWitnessDataPath_FollowsReplicatedMembership(t *testing.T) {
	payload := []byte("linearizable-state")

	tests := []struct {
		name string
		// membership places replica 2 in exactly one role.
		witnessRole bool
	}{
		{
			name:        "Membership.Witnesses gets metadata-only entries",
			witnessRole: true,
		},
		{
			name:        "Membership.Addresses gets full payloads",
			witnessRole: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := raftTestCfgPreVote(1, 1, false)
			cfg.CheckQuorum = config.BoolPtr(false)

			lr := newTestLogReader(nil, proto.State{})
			if tt.witnessRole {
				lr.membership = proto.Membership{
					Addresses: map[uint64]string{1: "addr1"},
					Witnesses: map[uint64]string{2: "addr2"},
				}
			} else {
				// The shape StartShard's bootstrap seed produced for a node
				// started with Config.IsWitness=true and listed in members.
				lr.membership = proto.Membership{
					Addresses: map[uint64]string{1: "addr1", 2: "addr2"},
				}
			}

			r, err := newRaft(cfg, lr)
			if err != nil {
				t.Fatalf("newRaft: %v", err)
			}
			electLeader(r)

			if err := r.step(proto.Message{
				Type:    proto.Propose,
				Origin:  proto.OriginLocal,
				Entries: []proto.Entry{{Cmd: payload}},
			}); err != nil {
				t.Fatalf("propose: %v", err)
			}
			proposed := r.log.lastIndex()
			r.drainMessages()

			// Un-pause replica 2 so the next broadcast actually carries the
			// entry rather than a probe.
			rm := r.getRemote(2)
			if rm == nil {
				t.Fatal("replica 2 is not tracked by the leader")
			}
			rm.probeSent = false
			r.broadcastAppend()

			var sent *proto.Entry
			for _, msg := range collectMessages(r, proto.Replicate) {
				if msg.To != 2 {
					continue
				}
				for i := range msg.Entries {
					if msg.Entries[i].Index == proposed {
						sent = &msg.Entries[i]
					}
				}
			}
			if sent == nil {
				t.Fatalf("no Replicate to replica 2 carried entry %d", proposed)
			}

			if tt.witnessRole {
				if sent.Type != proto.EntryMetadata {
					t.Errorf("entry type = %d, want EntryMetadata (%d)",
						sent.Type, proto.EntryMetadata)
				}
				if sent.Cmd != nil {
					t.Errorf("a witness received the entry payload %q; witnesses "+
						"receive metadata only", sent.Cmd)
				}
				return
			}
			if !bytes.Equal(sent.Cmd, payload) {
				t.Fatalf("entry Cmd = %q, want the full payload %q: a replica in "+
					"Membership.Addresses is a FULL replica on the data path no "+
					"matter what its own Config.IsWitness says", sent.Cmd, payload)
			}
		})
	}
}

// TestWitnessBootstrapShape_LatchesRoleMismatch verifies a witness listed as a full voter latches roleMismatch.
func TestWitnessBootstrapShape_LatchesRoleMismatch(t *testing.T) {
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.IsWitness = true

	lr := newTestLogReader(nil, proto.State{})
	// A bootstrap seed listing every member of the members map, this replica
	// included, in Addresses.
	lr.membership = proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
	}

	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	if !r.roleMismatch {
		t.Fatal("a replica started as a witness but seeded into the full voter " +
			"set did not latch roleMismatch")
	}

	for i := 0; i < 10*int(cfg.ElectionRTT); i++ {
		r.tickFunc(r)
	}
	if r.state != follower {
		t.Fatalf("state = %v, want follower: a witness seeded as a voter must "+
			"never campaign", r.state)
	}
}

// A restarted leader must not replicate full payloads to a seeded witness.

// TestWitnessDataPath_RestartSeedRepairedByCommittedConfigChange verifies replaying the committed AddWitness moves a seeded witness back to the metadata-only path.
func TestWitnessDataPath_RestartSeedRepairedByCommittedConfigChange(t *testing.T) {
	payload := []byte("full-entry-payload")

	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)

	lr := newTestLogReader(nil, proto.State{})
	// The members map lists witness 4 like a full replica.
	lr.membership = proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 4: "wit4"},
	}

	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	if !p.membership.isVoter(4) {
		t.Fatal("the fixture does not reproduce the seed: replica 4 must start " +
			"in the full voter set")
	}

	// The committed config change that gave replica 4 its role, replayed from
	// the log exactly as Peer.Commit replays it after a restart.
	cc := proto.ConfigChange{Type: proto.AddWitness, ReplicaID: 4, Address: "wit4"}
	ccCmd := make([]byte, cc.Size())
	if _, mErr := cc.MarshalTo(ccCmd); mErr != nil {
		t.Fatalf("marshalling the config change: %v", mErr)
	}
	p.processConfigChanges([]proto.Entry{
		{Index: 5, Term: 1, Type: proto.EntryConfigChange, Cmd: ccCmd},
	})

	if p.membership.isVoter(4) {
		t.Fatalf("replica 4 is still in the voting set after its committed "+
			"AddWitness replayed: %+v", p.membership.addresses)
	}
	if !p.membership.isWitness(4) {
		t.Fatalf("replica 4 is not a witness after its committed AddWitness "+
			"replayed: %+v", p.membership.witnesses)
	}
	if p.membership.configChangeID != 5 {
		t.Fatalf("configChangeID = %d, want 5", p.membership.configChangeID)
	}
	if _, ok := p.r.witnesses[4]; !ok {
		t.Fatal("r.witnesses does not track replica 4; broadcastAppend selects " +
			"sendWitnessAppend from this map and from nothing else")
	}
	if _, ok := p.r.remotes[4]; ok {
		t.Fatal("r.remotes still tracks replica 4 as a full replica")
	}

	// The data path itself: the leader must strip the payload for replica 4
	// and keep it for the full replica 2.
	r := p.r
	electLeader(r)
	if err := r.step(proto.Message{
		Type:    proto.Propose,
		Origin:  proto.OriginLocal,
		Entries: []proto.Entry{{Cmd: payload}},
	}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	proposed := r.log.lastIndex()
	r.drainMessages()
	for _, id := range []uint64{2, 4} {
		rm := r.getRemote(id)
		if rm == nil {
			t.Fatalf("replica %d is not tracked by the leader", id)
		}
		rm.probeSent = false
	}
	r.broadcastAppend()

	sent := make(map[uint64]*proto.Entry)
	for _, msg := range collectMessages(r, proto.Replicate) {
		for i := range msg.Entries {
			if msg.Entries[i].Index == proposed {
				sent[msg.To] = &msg.Entries[i]
			}
		}
	}
	if sent[4] == nil || sent[2] == nil {
		t.Fatalf("no Replicate carried entry %d to both replicas: %v", proposed, sent)
	}
	if sent[4].Cmd != nil {
		t.Errorf("the witness received the entry payload %q; a restarted leader "+
			"replicated the complete command stream to it", sent[4].Cmd)
	}
	if sent[4].Type != proto.EntryMetadata {
		t.Errorf("witness entry type = %d, want EntryMetadata (%d)",
			sent[4].Type, proto.EntryMetadata)
	}
	if !bytes.Equal(sent[2].Cmd, payload) {
		t.Errorf("the full replica received %q, want the full payload %q",
			sent[2].Cmd, payload)
	}
}
