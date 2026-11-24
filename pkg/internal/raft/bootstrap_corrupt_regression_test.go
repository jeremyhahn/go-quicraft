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

// Corrupt and lossy index-1 entries.

// corruptBootstrapEntries returns index-1 EntryInitialMembership entries whose
// Cmd cannot be decoded: a wholly foreign payload, a torn (truncated) tail of a
// genuine one, and an empty one.
func corruptBootstrapEntries(t *testing.T) map[string]proto.Entry {
	t.Helper()
	valid, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
	})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	if len(valid.Cmd) < 4 {
		t.Fatalf("fixture: encoded membership is only %d bytes", len(valid.Cmd))
	}
	entry := func(cmd []byte) proto.Entry {
		return proto.Entry{
			Index: BootstrapMembershipIndex,
			Term:  bootstrapMembershipTerm,
			Type:  proto.EntryInitialMembership,
			Cmd:   cmd,
		}
	}
	return map[string]proto.Entry{
		"foreign payload": entry([]byte("this is not a marshaled membership")),
		"torn tail":       entry(valid.Cmd[:len(valid.Cmd)-3]),
		"empty cmd":       entry(nil),
	}
}

// TestReconstructMembership_CorruptBootstrapEntryFailsClosed verifies an undecodable bootstrap entry fails reconstruction.
func TestReconstructMembership_CorruptBootstrapEntryFailsClosed(t *testing.T) {
	quietLogs(t)
	for name, e := range corruptBootstrapEntries(t) {
		t.Run(name, func(t *testing.T) {
			ms, err := ReconstructMembership([]proto.Entry{e})
			if err == nil {
				t.Fatalf("reconstruction SUCCEEDED over an undecodable bootstrap "+
					"entry and returned membership %+v: an empty voting set was "+
					"adopted as the shard's configuration, and the caller re-seeds "+
					"from its local members map — dropping witnesses, observers, "+
					"Removed tombstones and the replicated witness policy", ms)
			}
			if len(ms.Addresses) != 0 || len(ms.Witnesses) != 0 || len(ms.Observers) != 0 {
				t.Fatalf("a failed reconstruction returned a non-empty membership "+
					"%+v alongside its error", ms)
			}
		})
	}
}

// TestReconstructMembership_PreservesEveryReplicatedRoleAndPolicy verifies reconstruction keeps every role, tombstone and policy.
func TestReconstructMembership_PreservesEveryReplicatedRoleAndPolicy(t *testing.T) {
	want := proto.Membership{
		Addresses:                  map[uint64]string{1: "addr1", 2: "addr2"},
		Observers:                  map[uint64]string{4: "addr4"},
		Witnesses:                  map[uint64]string{5: "addr5"},
		Removed:                    map[uint64]bool{9: true},
		Identities:                 map[uint64]string{1: "id1", 5: "id5"},
		MaxWitnesses:               3,
		RequireFullReplicaMajority: true,
	}
	want.SetSessionPolicy(proto.SessionPolicy{
		ExpiryEntries:          1234,
		MaxTotalSessions:       77,
		MaxResponsesPerSession: 5,
		MaxSessionsPerOwner:    9,
	})

	e, err := BuildInitialMembershipEntry(want)
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	got, err := ReconstructMembership([]proto.Entry{e})
	if err != nil {
		t.Fatalf("ReconstructMembership: %v", err)
	}

	if len(got.Addresses) != 2 || got.Addresses[1] != "addr1" || got.Addresses[2] != "addr2" {
		t.Fatalf("voters = %v, want %v", got.Addresses, want.Addresses)
	}
	if len(got.Observers) != 1 || got.Observers[4] != "addr4" {
		t.Fatalf("observers = %v, want %v: an observer lost by reconstruction is "+
			"re-seeded as a VOTER from the caller's map, changing the quorum "+
			"denominator this replica computes against its peers",
			got.Observers, want.Observers)
	}
	if len(got.Witnesses) != 1 || got.Witnesses[5] != "addr5" {
		t.Fatalf("witnesses = %v, want %v: witnesses count toward the unified "+
			"quorum, so losing one changes the majority this replica requires",
			got.Witnesses, want.Witnesses)
	}
	if !got.Removed[9] {
		t.Fatalf("Removed tombstones = %v, want replica 9 tombstoned: the "+
			"tombstone set is the per-shard DENY-list, and losing it lets a "+
			"removed replica re-join", got.Removed)
	}
	if got.Identities[1] != "id1" || got.Identities[5] != "id5" {
		t.Fatalf("identities = %v, want %v: the committed identity map is what "+
			"the receive-side gate authenticates connections against",
			got.Identities, want.Identities)
	}
	if got.MaxWitnesses != 3 || !got.RequireFullReplicaMajority {
		t.Fatalf("witness policy = {MaxWitnesses %d, RequireFullReplicaMajority "+
			"%v}, want {3, true}: the policy is REPLICATED state and reverting "+
			"it to local config makes this replica enforce a different quorum "+
			"rule than its peers", got.MaxWitnesses, got.RequireFullReplicaMajority)
	}
	if sp := got.SessionPolicy(); sp.ExpiryEntries != 1234 ||
		sp.MaxTotalSessions != 77 || sp.MaxResponsesPerSession != 5 ||
		sp.MaxSessionsPerOwner != 9 {
		t.Fatalf("session policy = %+v, want {1234, 77, 5, 9}: it is replicated "+
			"state, so a divergent value expires different sessions at the same "+
			"log index on different replicas", sp)
	}
}

// TestPeer_CorruptBootstrapEntryDoesNotWipeMembership verifies a runtime decode failure leaves membership intact.
func TestPeer_CorruptBootstrapEntryDoesNotWipeMembership(t *testing.T) {
	for name, e := range corruptBootstrapEntries(t) {
		t.Run(name, func(t *testing.T) {
			p := newTestPeer(t, testConfig(), proto.State{}, threeNodeMembership())
			if !p.membership.isMember(2) {
				t.Fatal("precondition: replica 2 must start as a member")
			}

			// t.Error, not t.Fatal: the membership assertions below are the
			// consequence worth reporting even when the return value already
			// gave the answer away.
			changed, err := p.applyMembershipEntry(e)
			if changed {
				t.Error("applyMembershipEntry reported a membership CHANGE for an " +
					"undecodable bootstrap entry")
			}
			// The decode failure must also be reported.
			if !errors.Is(err, ErrCorruptConfigEntry) {
				t.Errorf("applyMembershipEntry returned err = %v, want "+
					"ErrCorruptConfigEntry: an undecodable committed config entry "+
					"must fault the shard, the way recovery already does", err)
			}

			for _, id := range []uint64{1, 2, 3} {
				if !p.membership.isMember(id) {
					t.Fatalf("replica %d is no longer a member after an undecodable "+
						"bootstrap entry: the corrupt entry wiped the live "+
						"configuration, leaving the replica computing quorum over an "+
						"empty voting set", id)
				}
			}
		})
	}
}

// TestReconstructMembership_CorruptConfigChangeFailsClosed verifies an undecodable committed config change fails reconstruction.
func TestReconstructMembership_CorruptConfigChangeFailsClosed(t *testing.T) {
	quietLogs(t)

	bootstrap, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
	})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	valid := marshalConfigChange(t, proto.ConfigChange{
		Type: proto.AddNode, ReplicaID: 4, Address: "addr4",
	})
	if len(valid) < 4 {
		t.Fatalf("fixture: encoded config change is only %d bytes", len(valid))
	}

	corrupt := map[string][]byte{
		"foreign payload": []byte("this is not a marshaled config change"),
		"torn tail":       valid[:len(valid)-3],
	}
	for name, cmd := range corrupt {
		t.Run(name, func(t *testing.T) {
			ms, err := ReconstructMembership([]proto.Entry{
				bootstrap,
				{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: cmd},
			})
			if !errors.Is(err, ErrCorruptConfigEntry) {
				t.Fatalf("ReconstructMembership over an undecodable COMMITTED config "+
					"change returned (%+v, %v), want ErrCorruptConfigEntry. Skipping "+
					"the entry reconstructs a membership every peer that can read "+
					"those bytes disagrees with — a silent, durable split of the "+
					"voting set quorum() is computed over", ms, err)
			}
			if len(ms.Addresses) != 0 {
				t.Fatalf("a failed reconstruction returned a non-empty membership "+
					"%+v alongside its error: the caller cannot be left with a "+
					"partially folded configuration to adopt", ms)
			}
		})
	}
}

// TestReconstructMembership_ValidConfigChangeStillFolds verifies a valid committed AddNode still folds.
func TestReconstructMembership_ValidConfigChangeStillFolds(t *testing.T) {
	bootstrap, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
	})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}

	ms, err := ReconstructMembership([]proto.Entry{
		bootstrap,
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: marshalConfigChange(t,
			proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "addr4"})},
		// A zero-length payload stays a no-op on every replica.
		{Index: 3, Term: 1, Type: proto.EntryConfigChange},
		// And a non-config entry in the run is still skipped.
		{Index: 4, Term: 1, Type: proto.EntryNormal, Cmd: []byte("payload")},
	})
	if err != nil {
		t.Fatalf("ReconstructMembership over a VALID committed config change: %v", err)
	}
	if len(ms.Addresses) != 4 || ms.Addresses[4] != "addr4" {
		t.Fatalf("reconstructed voters = %v, want the 3 founders plus replica 4: "+
			"the fail-closed guard must not have stopped legitimate folding",
			ms.Addresses)
	}
}
