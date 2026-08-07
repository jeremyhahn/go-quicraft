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
	// Exactly what StartShard's bootstrap seed used to write: every member of
	// the members map, this replica included, in Addresses.
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
