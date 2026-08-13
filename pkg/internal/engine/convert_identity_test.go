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

package engine

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestProtoMembershipToLogDBCarriesIdentities pins the WRITE side of the
// snapshot-metadata round-trip.
//
// This converter is what a replica's own snapshot save goes through
// (snapshotPool.save) and what carries an Update's snapshot metadata into
// LogDB. Identities reach membership only through the log, so if this drops
// them the record on disk never had them and the recovery path has nothing to
// read back — the replica returns with an identity-free configuration and
// silently authorizes every member by address instead.
func TestProtoMembershipToLogDBCarriesIdentities(t *testing.T) {
	pm := proto.Membership{
		ConfigChangeID: 7,
		Addresses:      map[uint64]string{1: "addr1", 2: "addr2"},
		Observers:      map[uint64]string{3: "obs1"},
		Identities: map[uint64]string{
			1: "spki:1111111111111111111111111111111111111111111111111111111111111111",
			3: "spki:3333333333333333333333333333333333333333333333333333333333333333",
		},
	}

	lm := protoMembershipToLogDB(&pm)

	if len(lm.Identities) != 2 {
		t.Fatalf("Identities = %v, want 2 entries; a snapshot record written "+
			"without them cannot restore identity-based authorization on restart",
			lm.Identities)
	}
	for id, want := range pm.Identities {
		if lm.Identities[id] != want {
			t.Fatalf("Identities[%d] = %q, want %q", id, lm.Identities[id], want)
		}
	}

	// The map must not alias the caller's: the proto membership belongs to the
	// raft snapshot and keeps being read after the conversion.
	pm.Identities[1] = "spki:tampered"
	if lm.Identities[1] == "spki:tampered" {
		t.Fatal("protoMembershipToLogDB aliased the caller's Identities map")
	}
}

// TestProtoMembershipToLogDBWithoutIdentities pins that absence stays absence:
// an identity is only ever committed, never invented by a conversion.
func TestProtoMembershipToLogDBWithoutIdentities(t *testing.T) {
	pm := proto.Membership{
		ConfigChangeID: 2,
		Addresses:      map[uint64]string{1: "addr1"},
	}
	if lm := protoMembershipToLogDB(&pm); lm.Identities != nil {
		t.Fatalf("Identities = %v, want nil", lm.Identities)
	}
}

// TestProtoSnapshotToLogDBCarriesIdentities pins that the snapshot-level
// conversion — the one SaveState's Update path uses — does not lose the field
// its membership converter preserves.
func TestProtoSnapshotToLogDBCarriesIdentities(t *testing.T) {
	identity := "spki:2222222222222222222222222222222222222222222222222222222222222222"
	ps := proto.Snapshot{
		Index: 10,
		Term:  4,
		Membership: proto.Membership{
			Addresses:  map[uint64]string{2: "addr2"},
			Identities: map[uint64]string{2: identity},
		},
	}
	if got := protoSnapshotToLogDB(&ps).Membership.Identities[2]; got != identity {
		t.Fatalf("Identities[2] = %q, want %q", got, identity)
	}
}
