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

package memdb

import (
	"context"
	"maps"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// readCopyFullMembership returns a membership with every one of the six maps
// filled, so a reader that hands back any one of them aliased is caught.
func readCopyFullMembership() logdb.Membership {
	return logdb.Membership{
		Addresses:         map[uint64]string{1: "a1"},
		Observers:         map[uint64]string{3: "o3"},
		Witnesses:         map[uint64]string{4: "w4"},
		Removed:           map[uint64]bool{5: true},
		Identities:        map[uint64]string{1: "spki:one"},
		RemovedIdentities: map[uint64]string{5: "spki:five"},
	}
}

// readCopySetup stores state and a snapshot carrying m for node (1, 1), so
// both ReadState and GetSnapshot return nil error.
func readCopySetup(_ context.Context, t *testing.T, m logdb.Membership) *MemDB {
	t.Helper()
	db := New()
	if err := db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 2, Commit: 10},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 10, Term: 2, Membership: m}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	return db
}

// TestGetSnapshotReadSideCopy: a write into the maps of one GetSnapshot
// result is never seen by the next GetSnapshot.
func TestGetSnapshotReadSideCopy(t *testing.T) {
	db := readCopySetup(context.Background(), t, readCopyFullMembership())

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	got.Membership.Addresses[1] = "MUTATED"
	got.Membership.Removed[5] = false
	got.Membership.Identities[1] = "MUTATED"

	again, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("second GetSnapshot: %v", err)
	}
	if v := again.Membership.Addresses[1]; v != "a1" {
		t.Errorf("Addresses[1] = %q, want %q", v, "a1")
	}
	if v := again.Membership.Removed[5]; !v {
		t.Errorf("Removed[5] = %v, want true", v)
	}
	if v := again.Membership.Identities[1]; v != "spki:one" {
		t.Errorf("Identities[1] = %q, want %q", v, "spki:one")
	}
}

// TestReadStateReadSideCopy: a write into the membership ReadState returns is
// never seen by the next ReadState, nor by GetSnapshot, which SaveSnapshot
// stores the same membership for.
func TestReadStateReadSideCopy(t *testing.T) {
	db := readCopySetup(context.Background(), t, readCopyFullMembership())

	_, m, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	m.RemovedIdentities[5] = "MUTATED"
	m.Observers[9] = "injected"

	_, again, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("second ReadState: %v", err)
	}
	snap, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	for name, got := range map[string]logdb.Membership{
		"ReadState":   again,
		"GetSnapshot": snap.Membership,
	} {
		if v := got.RemovedIdentities[5]; v != "spki:five" {
			t.Errorf("%s: RemovedIdentities[5] = %q, want %q", name, v, "spki:five")
		}
		if v, ok := got.Observers[9]; ok {
			t.Errorf("%s: Observers[9] = %q, want no key", name, v)
		}
	}
}

// TestGetBootstrapReadSideCopy: deleting from or adding to the Addresses of
// one GetBootstrap result is never seen by the next GetBootstrap.
func TestGetBootstrapReadSideCopy(t *testing.T) {
	db := New()
	if err := db.SaveBootstrap(1, 1, logdb.Bootstrap{
		Addresses: map[uint64]string{1: "a1", 2: "a2"},
		Type:      3,
	}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}

	b, ok, err := db.GetBootstrap(1, 1)
	if err != nil || !ok {
		t.Fatalf("GetBootstrap: ok=%v err=%v", ok, err)
	}
	delete(b.Addresses, 2)
	b.Addresses[7] = "x"

	again, ok, err := db.GetBootstrap(1, 1)
	if err != nil || !ok {
		t.Fatalf("second GetBootstrap: ok=%v err=%v", ok, err)
	}
	want := map[uint64]string{1: "a1", 2: "a2"}
	if !maps.Equal(again.Addresses, want) {
		t.Errorf("Addresses = %v, want %v", again.Addresses, want)
	}
}

// TestReadSideCopyPreservesNilMaps: the read-side copy keeps nil maps nil,
// the same rule as TestCopyMembershipPreservesNilMaps on the write side.
func TestReadSideCopyPreservesNilMaps(t *testing.T) {
	m := readCopyFullMembership()
	m.Witnesses = nil
	m.RemovedIdentities = nil
	db := readCopySetup(context.Background(), t, m)

	snap, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	_, rs, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	for name, got := range map[string]logdb.Membership{
		"GetSnapshot": snap.Membership,
		"ReadState":   rs,
	} {
		if got.Witnesses != nil {
			t.Errorf("%s: Witnesses = %v, want nil", name, got.Witnesses)
		}
		if got.RemovedIdentities != nil {
			t.Errorf("%s: RemovedIdentities = %v, want nil", name, got.RemovedIdentities)
		}
		if got.Addresses == nil {
			t.Errorf("%s: Addresses = nil, want the stored map", name)
		}
	}
}
