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

// ---------------------------------------------------------------------------
// M2: the witness DATA PATH is driven by the replicated membership alone
// ---------------------------------------------------------------------------

// TestWitnessDataPath_FollowsReplicatedMembership pins the consequence that
// makes Host.StartShard refuse to bootstrap a witness through the initial
// members map (see pkg.TestStartShard_RejectsWitnessOrObserverInMembers).
//
// broadcastAppend picks sendWitnessAppend over sendAppend by looking up the
// target in r.witnesses, and r.witnesses is populated from
// Membership.Witnesses — from nothing else. The target's own Config.IsWitness
// is not an input and cannot be: the leader never sees it. So a replica the
// operator started with IsWitness=true, but which the members map seeded into
// Membership.Addresses, receives FULL entry payloads exactly like any other
// voter, while its local host gates keep refusing the reads a witness must not
// serve. It is a full voter wearing a witness label.
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

// TestWitnessBootstrapShape_LatchesRoleMismatch pins the other half of the same
// disagreement, seen from the witness itself: a replica started with
// Config.IsWitness=true whose replicated membership lists it among the full
// voters latches roleMismatch at construction and can then neither campaign nor
// serve reads for the life of the shard.
//
// That latch is a TRUE positive — the replica really is a full voter in the
// configuration the shard agreed on — so it must not be suppressed. The
// configuration has to be prevented from being created, which is what
// Host.StartShard now does.
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

// ---------------------------------------------------------------------------
// H1: a RESTARTED leader must not replicate full payloads to a seeded witness
// ---------------------------------------------------------------------------

// TestWitnessDataPath_RestartSeedRepairedByCommittedConfigChange is the H1
// regression, seen from the LEADER.
//
// Host.StartShard seeds proto.Membership.Addresses from the caller's members
// map whenever recovery restored no membership — the ordinary state of a
// replica that has never snapshotted. That map is the operator's list of
// replica IDs; it records no roles, so every witness and observer in it is
// seeded as a full VOTER. syncRaftRemotes then fills r.witnesses from
// membership.witnesses alone, so the witness is absent from it, and
// broadcastAppend selects sendAppend — the FULL command stream — for a replica
// whose entire purpose is to hold none of it. Omitting the witness from the
// map instead is not an alternative: it disappears from the leader's remotes
// altogether.
//
// The committed AddWitness entry is still in the log (config changes are
// compacted only behind a snapshot, and a snapshot would have restored the
// membership instead) and is re-applied on restart. This asserts it moves the
// witness OUT of the voting set and back onto the metadata-only data path.
func TestWitnessDataPath_RestartSeedRepairedByCommittedConfigChange(t *testing.T) {
	payload := []byte("full-entry-payload")

	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)

	lr := newTestLogReader(nil, proto.State{})
	// The restart seed: replica 4 is a WITNESS, but the members map the
	// operator passed lists it exactly like every full replica.
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
