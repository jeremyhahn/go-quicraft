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

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestBuildInitialMembershipEntry_Deterministic is the load-bearing property:
// two replicas building the bootstrap entry from the SAME configuration must
// produce byte-identical entries, or the entry at index 1 differs between
// replicas and Log Matching is violated on first contact. The maps are
// populated in different literal orders to make map-iteration nondeterminism,
// if any leaked in, observable.
func TestBuildInitialMembershipEntry_Deterministic(t *testing.T) {
	a := proto.Membership{
		Addresses:                  map[uint64]string{3: "c", 1: "a", 2: "b"},
		Observers:                  map[uint64]string{5: "e", 4: "d"},
		Witnesses:                  map[uint64]string{7: "g", 6: "f"},
		MaxWitnesses:               2,
		RequireFullReplicaMajority: true,
	}
	b := proto.Membership{
		Addresses:                  map[uint64]string{1: "a", 2: "b", 3: "c"},
		Observers:                  map[uint64]string{4: "d", 5: "e"},
		Witnesses:                  map[uint64]string{6: "f", 7: "g"},
		MaxWitnesses:               2,
		RequireFullReplicaMajority: true,
	}

	ea, err := BuildInitialMembershipEntry(a)
	if err != nil {
		t.Fatalf("build a: %v", err)
	}
	eb, err := BuildInitialMembershipEntry(b)
	if err != nil {
		t.Fatalf("build b: %v", err)
	}

	if ea.Index != BootstrapMembershipIndex || ea.Term != bootstrapMembershipTerm {
		t.Fatalf("entry landed at index %d term %d, want index %d term %d",
			ea.Index, ea.Term, BootstrapMembershipIndex, bootstrapMembershipTerm)
	}
	if ea.Type&proto.EntryTypeMask != proto.EntryInitialMembership {
		t.Fatalf("entry type = %d, want EntryInitialMembership", ea.Type&proto.EntryTypeMask)
	}
	if !bytes.Equal(ea.Cmd, eb.Cmd) {
		t.Fatal("two replicas built DIFFERENT bootstrap entries from the same " +
			"configuration: the entry at index 1 would differ between replicas and " +
			"Log Matching is violated the moment they compare logs")
	}
}

// TestInitialMembershipRoundTrip confirms the entry decodes back to the
// configuration it was built from, with ConfigChangeID pinned to the entry's
// own index.
func TestInitialMembershipRoundTrip(t *testing.T) {
	src := proto.Membership{
		Addresses:                  map[uint64]string{1: "a", 2: "b", 3: "c"},
		Witnesses:                  map[uint64]string{4: "d"},
		MaxWitnesses:               1,
		RequireFullReplicaMajority: true,
	}
	e, err := BuildInitialMembershipEntry(src)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !e.IsInitialMembership() {
		t.Fatal("IsInitialMembership() is false for a bootstrap entry")
	}

	got, err := initialMembershipFromEntry(e)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ConfigChangeID != 0 {
		t.Fatalf("ConfigChangeID = %d, want 0 (the initial configuration predates "+
			"any config change; stamping it would break a compare-and-swap that read 0)",
			got.ConfigChangeID)
	}
	if len(got.Addresses) != 3 || got.Addresses[2] != "b" {
		t.Fatalf("voters not round-tripped: %v", got.Addresses)
	}
	if len(got.Witnesses) != 1 || got.Witnesses[4] != "d" {
		t.Fatalf("witnesses not round-tripped: %v", got.Witnesses)
	}
	if got.MaxWitnesses != 1 || !got.RequireFullReplicaMajority {
		t.Fatalf("witness policy not round-tripped: max=%d requireFull=%v",
			got.MaxWitnesses, got.RequireFullReplicaMajority)
	}
}

// TestJoiningClearsWhenBootstrapEntryApplied is the Stage-4 property: a joiner
// that started with empty membership ends its joining window — and its IsMember
// authorization bypass — the moment it applies the bootstrap membership entry
// the leader replicates to it. Before config-in-log the window could only be
// closed by a snapshot, so a joiner caught up by replication alone stayed in the
// bypass permanently.
func TestJoiningClearsWhenBootstrapEntryApplied(t *testing.T) {
	// Replica 4 joins a {1,2,3} cluster: it starts knowing no members.
	p := newTestPeer(t, testConfigWithID(1, 4), proto.State{}, proto.Membership{})
	p.MarkJoining()
	if !p.joining.Load() {
		t.Fatal("fixture: joining flag must be set")
	}
	// While joining, IsMember authorizes every replica so the joiner can accept
	// the leader's first replication.
	if !p.IsMember(999) {
		t.Fatal("fixture: a joining node must bypass IsMember")
	}

	// The leader replicates the committed bootstrap entry (index 1) carrying the
	// full initial configuration. Applying it at commit time is what
	// processConfigChanges does on the joiner.
	entry, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
	})
	if err != nil {
		t.Fatalf("build bootstrap entry: %v", err)
	}
	p.processConfigChanges([]proto.Entry{entry})

	if p.joining.Load() {
		t.Fatal("joining was NOT cleared after applying the bootstrap membership " +
			"entry: the IsMember bypass would persist for the life of the process, " +
			"exactly the permanent-bypass bug config-in-log removes")
	}
	if p.IsMember(999) {
		t.Fatal("IsMember still authorizes an unknown replica after the joining " +
			"window ended: per-shard authorization is still disabled")
	}
	for _, id := range []uint64{1, 2, 3} {
		if !p.IsMember(id) {
			t.Fatalf("member %d not recognized after reconstructing membership from "+
				"the bootstrap entry", id)
		}
	}
}
