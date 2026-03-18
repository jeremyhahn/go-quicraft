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

package quicraft

import "testing"

// ---------------------------------------------------------------------------
// NewMembership
// ---------------------------------------------------------------------------

func TestNewMembership_MapsInitialized(t *testing.T) {
	m := NewMembership()
	if m.Nodes == nil {
		t.Error("Nodes map is nil")
	}
	if m.Observers == nil {
		t.Error("Observers map is nil")
	}
	if m.Witnesses == nil {
		t.Error("Witnesses map is nil")
	}
	if m.Removed == nil {
		t.Error("Removed map is nil")
	}
}

func TestNewMembership_MapsEmpty(t *testing.T) {
	m := NewMembership()
	if len(m.Nodes) != 0 {
		t.Errorf("Nodes length = %d, want 0", len(m.Nodes))
	}
	if len(m.Observers) != 0 {
		t.Errorf("Observers length = %d, want 0", len(m.Observers))
	}
	if len(m.Witnesses) != 0 {
		t.Errorf("Witnesses length = %d, want 0", len(m.Witnesses))
	}
	if len(m.Removed) != 0 {
		t.Errorf("Removed length = %d, want 0", len(m.Removed))
	}
}

func TestNewMembership_ConfigChangeIDZero(t *testing.T) {
	m := NewMembership()
	if m.ConfigChangeID != 0 {
		t.Errorf("ConfigChangeID = %d, want 0", m.ConfigChangeID)
	}
}

// ---------------------------------------------------------------------------
// Field access and mutation
// ---------------------------------------------------------------------------

func TestMembership_AddNodes(t *testing.T) {
	m := NewMembership()
	m.Nodes[1] = "addr1:5000"
	m.Nodes[2] = "addr2:5000"

	if len(m.Nodes) != 2 {
		t.Errorf("Nodes length = %d, want 2", len(m.Nodes))
	}
	if m.Nodes[1] != "addr1:5000" {
		t.Errorf("Nodes[1] = %q, want %q", m.Nodes[1], "addr1:5000")
	}
	if m.Nodes[2] != "addr2:5000" {
		t.Errorf("Nodes[2] = %q, want %q", m.Nodes[2], "addr2:5000")
	}
}

func TestMembership_AddObservers(t *testing.T) {
	m := NewMembership()
	m.Observers[10] = "observer:6000"

	if len(m.Observers) != 1 {
		t.Errorf("Observers length = %d, want 1", len(m.Observers))
	}
	if m.Observers[10] != "observer:6000" {
		t.Errorf("Observers[10] = %q, want %q", m.Observers[10], "observer:6000")
	}
}

func TestMembership_AddWitnesses(t *testing.T) {
	m := NewMembership()
	m.Witnesses[20] = "witness:7000"

	if len(m.Witnesses) != 1 {
		t.Errorf("Witnesses length = %d, want 1", len(m.Witnesses))
	}
	if m.Witnesses[20] != "witness:7000" {
		t.Errorf("Witnesses[20] = %q, want %q", m.Witnesses[20], "witness:7000")
	}
}

func TestMembership_AddRemoved(t *testing.T) {
	m := NewMembership()
	m.Removed[5] = struct{}{}
	m.Removed[6] = struct{}{}

	if len(m.Removed) != 2 {
		t.Errorf("Removed length = %d, want 2", len(m.Removed))
	}
	if _, ok := m.Removed[5]; !ok {
		t.Error("Removed should contain 5")
	}
	if _, ok := m.Removed[6]; !ok {
		t.Error("Removed should contain 6")
	}
}

func TestMembership_ConfigChangeID(t *testing.T) {
	m := NewMembership()
	m.ConfigChangeID = 42

	if m.ConfigChangeID != 42 {
		t.Errorf("ConfigChangeID = %d, want 42", m.ConfigChangeID)
	}
}

// ---------------------------------------------------------------------------
// Direct construction (no NewMembership)
// ---------------------------------------------------------------------------

func TestMembership_DirectConstruction(t *testing.T) {
	m := &Membership{
		ConfigChangeID: 100,
		Nodes:          map[uint64]string{1: "a", 2: "b"},
		Observers:      map[uint64]string{3: "c"},
		Witnesses:      map[uint64]string{4: "d"},
		Removed:        map[uint64]struct{}{5: {}},
	}

	if m.ConfigChangeID != 100 {
		t.Errorf("ConfigChangeID = %d, want 100", m.ConfigChangeID)
	}
	if len(m.Nodes) != 2 {
		t.Errorf("Nodes length = %d, want 2", len(m.Nodes))
	}
	if len(m.Observers) != 1 {
		t.Errorf("Observers length = %d, want 1", len(m.Observers))
	}
	if len(m.Witnesses) != 1 {
		t.Errorf("Witnesses length = %d, want 1", len(m.Witnesses))
	}
	if len(m.Removed) != 1 {
		t.Errorf("Removed length = %d, want 1", len(m.Removed))
	}
}

func TestMembership_DirectConstruction_NilMaps(t *testing.T) {
	// Verify that accessing nil maps panics (motivating NewMembership).
	m := &Membership{}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when writing to nil map")
		}
	}()
	m.Nodes[1] = "should-panic"
}
