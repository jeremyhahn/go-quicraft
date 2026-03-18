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

package dragonboat

import (
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

func TestFromQuicraftMembership_AllFieldsMapped(t *testing.T) {
	qm := &quicraft.Membership{
		ConfigChangeID: 42,
		Nodes:          map[uint64]string{1: "addr1", 2: "addr2"},
		Observers:      map[uint64]string{3: "addr3"},
		Witnesses:      map[uint64]string{4: "addr4"},
		Removed:        map[uint64]struct{}{5: {}},
	}

	dm := fromQuicraftMembership(qm)

	if dm.ConfigChangeID != 42 {
		t.Errorf("ConfigChangeID = %d, want 42", dm.ConfigChangeID)
	}

	// Nodes
	if len(dm.Nodes) != 2 {
		t.Fatalf("Nodes length = %d, want 2", len(dm.Nodes))
	}
	if dm.Nodes[1] != "addr1" {
		t.Errorf("Nodes[1] = %q, want %q", dm.Nodes[1], "addr1")
	}
	if dm.Nodes[2] != "addr2" {
		t.Errorf("Nodes[2] = %q, want %q", dm.Nodes[2], "addr2")
	}

	// NonVotings (quicraft Observers)
	if len(dm.NonVotings) != 1 {
		t.Fatalf("NonVotings length = %d, want 1", len(dm.NonVotings))
	}
	if dm.NonVotings[3] != "addr3" {
		t.Errorf("NonVotings[3] = %q, want %q", dm.NonVotings[3], "addr3")
	}

	// Witnesses
	if len(dm.Witnesses) != 1 {
		t.Fatalf("Witnesses length = %d, want 1", len(dm.Witnesses))
	}
	if dm.Witnesses[4] != "addr4" {
		t.Errorf("Witnesses[4] = %q, want %q", dm.Witnesses[4], "addr4")
	}

	// Removed
	if len(dm.Removed) != 1 {
		t.Fatalf("Removed length = %d, want 1", len(dm.Removed))
	}
	if _, ok := dm.Removed[5]; !ok {
		t.Error("Removed should contain replica 5")
	}
}

func TestFromQuicraftMembership_NilMembership(t *testing.T) {
	dm := fromQuicraftMembership(nil)
	if dm != nil {
		t.Errorf("fromQuicraftMembership(nil) = %v, want nil", dm)
	}
}

func TestFromQuicraftMembership_EmptyMaps(t *testing.T) {
	qm := quicraft.NewMembership()
	dm := fromQuicraftMembership(qm)

	if dm == nil {
		t.Fatal("fromQuicraftMembership should not return nil for non-nil input")
	}
	if len(dm.Nodes) != 0 {
		t.Errorf("Nodes length = %d, want 0", len(dm.Nodes))
	}
	if len(dm.NonVotings) != 0 {
		t.Errorf("NonVotings length = %d, want 0", len(dm.NonVotings))
	}
	if len(dm.Witnesses) != 0 {
		t.Errorf("Witnesses length = %d, want 0", len(dm.Witnesses))
	}
	if len(dm.Removed) != 0 {
		t.Errorf("Removed length = %d, want 0", len(dm.Removed))
	}
}

func TestFromQuicraftMembership_IsolatesFromSource(t *testing.T) {
	qm := &quicraft.Membership{
		Nodes:     map[uint64]string{1: "addr1"},
		Observers: map[uint64]string{},
		Witnesses: map[uint64]string{},
		Removed:   map[uint64]struct{}{},
	}
	dm := fromQuicraftMembership(qm)

	// Modify the source after conversion.
	qm.Nodes[2] = "addr2"

	// The dragonboat membership should not reflect the change.
	if len(dm.Nodes) != 1 {
		t.Errorf("mutation of source affected result: Nodes length = %d, want 1", len(dm.Nodes))
	}
}
