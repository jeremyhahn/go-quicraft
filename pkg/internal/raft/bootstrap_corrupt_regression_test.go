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
//
// The bootstrap entry at index 1 is the ONLY thing that makes membership a
// function of the durable log. Every consumer of it decodes a caller-supplied
// byte slice, and a Membership that fails to decode leaves the zero value —
// an EMPTY voting set — behind. Adopting that is not a degraded membership, it
// is a shard with no voters at all, and on the recovery path it is worse still:
// a silently empty reconstruction makes the caller re-seed from its local
// members map, dropping witnesses, observers, the Removed tombstone DENY-list
// and the replicated witness/session policy.
//
// So both decode sites must refuse rather than adopt: ReconstructMembership
// returns the error (recovery fails closed) and Peer.applyMembershipEntry
// reports "no change" and leaves the membership it already had intact.

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

// TestReconstructMembership_CorruptBootstrapEntryFailsClosed pins recovery-time
// reconstruction as fail-closed. A decode failure must surface as an error so
// StartShard aborts; swallowing it yields an EMPTY membership that the caller
// cannot distinguish from "this log has no bootstrap entry", and it then
// re-seeds the shard from its local members map.
func TestReconstructMembership_CorruptBootstrapEntryFailsClosed(t *testing.T) {
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

// TestReconstructMembership_ValidBootstrapEntryStillDecodes is the control for
// the test above: the refusal must key on the DECODE, not on the entry type, or
// config-in-log recovery would never work at all.
func TestReconstructMembership_ValidBootstrapEntryStillDecodes(t *testing.T) {
	e, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2"},
	})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	ms, err := ReconstructMembership([]proto.Entry{e})
	if err != nil {
		t.Fatalf("reconstruction of a VALID bootstrap entry failed: %v", err)
	}
	if len(ms.Addresses) != 2 || ms.Addresses[2] != "addr2" {
		t.Fatalf("reconstructed addresses = %v, want the 2 bootstrap voters",
			ms.Addresses)
	}
}

// TestReconstructMembership_PreservesEveryReplicatedRoleAndPolicy is the
// non-voter survival test. The founding entry carries the WHOLE Membership, so
// witnesses, observers, the Removed tombstone DENY-list, the committed member
// identities and the replicated witness/session policy all have to come back
// out of it. These are precisely the fields a re-seed from the caller's members
// map — which carries voter addresses and nothing else — silently drops.
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
		ExpiryEntries:        1234,
		MaxTotalSessions:     77,
		MaxSessionsPerClient: 5,
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
		sp.MaxTotalSessions != 77 || sp.MaxSessionsPerClient != 5 {
		t.Fatalf("session policy = %+v, want {1234, 77, 5}: it is replicated "+
			"state, so a divergent value expires different sessions at the same "+
			"log index on different replicas", sp)
	}
}

// TestPeer_CorruptBootstrapEntryDoesNotWipeMembership covers the RUNTIME decode
// site. applyMembershipEntry logs and continues on a decode failure — a
// committed entry is never re-delivered, so it cannot abort the scan — but
// "continue" must mean the membership is left ALONE. Adopting the zero
// Membership here would leave a live replica with no voters at all: it would
// compute quorum over an empty set, stop recognising its own peers, and
// (because membership is persisted alongside the next snapshot) make the wipe
// durable.
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
			// The decode failure must also be REPORTED, not merely survived.
			// Recovery treats these bytes as fatal (ReconstructMembership above);
			// the runtime swallowing them meant a replica ran on a configuration
			// its peers do not share and persisted it with the next snapshot.
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
