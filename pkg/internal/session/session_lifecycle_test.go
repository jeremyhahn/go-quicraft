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

package session

import (
	"bytes"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ===========================================================================
// SaveAndRestore: full serialization round-trip with response history
// ===========================================================================

// TestSession_RestoreReplacesExistingState verifies that UnmarshalFrom
// completely replaces the manager's existing sessions with the snapshot
// state, removing any pre-existing sessions.
func TestSession_RestoreReplacesExistingState(t *testing.T) {
	m1 := NewManager(100, 100, 0)
	if err := m1.Register(10, 0, 0); err != nil {
		t.Fatalf("Register(10): %v", err)
	}
	if err := m1.RecordResult(10, 1, 0, 100, sm.Result{Value: 100}); err != nil {
		t.Fatalf("RecordResult: %v", err)
	}

	// Marshal m1 state.
	var buf bytes.Buffer
	if err := m1.MarshalTo(&buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	// m2 has a completely different session.
	m2 := NewManager(100, 100, 0)
	if err := m2.Register(99, 0, 0); err != nil {
		t.Fatalf("Register(99): %v", err)
	}
	if err := m2.RecordResult(99, 1, 0, 100, sm.Result{Value: 999}); err != nil {
		t.Fatalf("RecordResult(99): %v", err)
	}

	// Restore m1 state into m2.
	if err := m2.UnmarshalFrom(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}

	// m2 should now have m1's state, not its own.
	if m2.IsRegistered(99) {
		t.Error("session 99 should be gone after restore")
	}
	if !m2.IsRegistered(10) {
		t.Error("session 10 should be present after restore")
	}
	if m2.Count() != 1 {
		t.Errorf("Count = %d, want 1", m2.Count())
	}

	// Verify cached result from m1 survived.
	result, isDup, err := m2.CheckDuplicate(10, 1, 200)
	if err != nil {
		t.Fatalf("CheckDuplicate: %v", err)
	}
	if !isDup {
		t.Fatal("series 1 should be duplicate after restore")
	}
	if result.Value != 100 {
		t.Errorf("Value = %d, want 100", result.Value)
	}
}
