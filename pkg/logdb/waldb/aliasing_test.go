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

package waldb

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// snapshotWithMembership builds a snapshot whose membership populates every
// map, so an aliasing bug in any one of them is observable.
func snapshotWithMembership(index uint64) logdb.Snapshot {
	return logdb.Snapshot{
		Index:    index,
		Term:     1,
		Filepath: "/snap.dat",
		Membership: logdb.Membership{
			ConfigChangeID:    index,
			Addresses:         map[uint64]string{1: "a1", 2: "a2"},
			Observers:         map[uint64]string{3: "o3"},
			Witnesses:         map[uint64]string{4: "w4"},
			Removed:           map[uint64]bool{5: true},
			Identities:        map[uint64]string{1: "spki:one"},
			RemovedIdentities: map[uint64]string{5: "spki:five"},
		},
	}
}

// TestSaveSnapshot_DoesNotAliasCallerMaps pins the ownership contract on the
// WRITE side.
//
// waldb keeps an in-memory record of the latest snapshot per node and serves
// it from GetSnapshot. Staging the caller's value verbatim meant retaining the
// caller's maps, so the owning layer could mutate a record waldb had already
// reported as persisted — no write, no error, and the mutation visible to
// every subsequent reader.
//
// memdb has deep-copied since it was written. These are two implementations of
// one LogDB interface, so the asymmetry made code correct against the test
// backend wrong against the production one.
func TestSaveSnapshot_DoesNotAliasCallerMaps(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()

	ss := snapshotWithMembership(10)
	if err := db.SaveSnapshot(1, 1, ss); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Mutate every map the caller still holds.
	ss.Membership.Addresses[1] = "MUTATED"
	ss.Membership.Observers[3] = "MUTATED"
	ss.Membership.Witnesses[4] = "MUTATED"
	ss.Membership.Removed[5] = false
	ss.Membership.Identities[1] = "MUTATED"
	ss.Membership.RemovedIdentities[5] = "MUTATED"

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	m := got.Membership
	if m.Addresses[1] != "a1" || m.Observers[3] != "o3" || m.Witnesses[4] != "w4" ||
		!m.Removed[5] || m.Identities[1] != "spki:one" ||
		m.RemovedIdentities[5] != "spki:five" {
		t.Fatalf("the stored snapshot aliases the caller's maps: %+v.\n"+
			"A mutation by the owning layer rewrote a record waldb had "+
			"already reported as persisted — including the identity maps, "+
			"which are the authorization state a recovering replica has no "+
			"other source for", m)
	}
}

// TestGetSnapshot_DoesNotAliasStoredMaps pins the same contract on the READ
// side. A caller handed a value may reasonably mutate it; doing so must not
// rewrite the record waldb still serves to everyone else.
func TestGetSnapshot_DoesNotAliasStoredMaps(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()

	if err := db.SaveSnapshot(1, 1, snapshotWithMembership(10)); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	first, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	first.Membership.Addresses[1] = "MUTATED"
	first.Membership.RemovedIdentities[5] = "MUTATED"

	second, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot (second): %v", err)
	}
	if second.Membership.Addresses[1] != "a1" ||
		second.Membership.RemovedIdentities[5] != "spki:five" {
		t.Fatalf("GetSnapshot returned waldb's own maps: one caller's "+
			"mutation changed what every later reader sees: %+v",
			second.Membership)
	}
}

// TestCopyMembership_PreservesNilMaps pins that copying does not manufacture
// empty maps where the caller had none. The encoders distinguish nil from
// empty, so a round-trip that invented maps would change the persisted bytes.
func TestCopyMembership_PreservesNilMaps(t *testing.T) {
	got := copyMembership(logdb.Membership{ConfigChangeID: 3})
	if got.Addresses != nil || got.Observers != nil || got.Witnesses != nil ||
		got.Removed != nil || got.Identities != nil || got.RemovedIdentities != nil {
		t.Fatalf("copyMembership manufactured empty maps: %+v", got)
	}
	if got.ConfigChangeID != 3 {
		t.Fatalf("copyMembership dropped a scalar field: %+v", got)
	}
}
