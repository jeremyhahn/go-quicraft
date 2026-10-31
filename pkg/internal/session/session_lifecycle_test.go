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

// TestSession_SaveAndRestore verifies a full marshal/unmarshal round-trip
// preserving all session state including response history, respondedTo,
// and lastActiveIndex.
// TestSession_SaveAndRestore_EmptyManager verifies that an empty manager
// can be saved and restored correctly.
// ===========================================================================
// Response addition
// ===========================================================================

// TestSession_ResponseCanBeAdded verifies that individual responses can
// be added to a session and subsequently found via CheckDuplicate.
// TestSession_ResponseCanBeAdded_UnregisteredSessionReturnsError verifies
// that adding a response to an unregistered session returns ErrSessionNotFound.
// ===========================================================================
// Cached response clearing
// ===========================================================================

// TestSession_CachedResponseCleared verifies that advancing respondedTo
// clears cached responses with seriesID <= respondedTo.
// ===========================================================================
// Response lookup for deduplication
// ===========================================================================

// TestSession_ResponseLookup verifies that duplicate detection correctly
// returns cached results and identifies new vs. duplicate proposals.
// ===========================================================================
// Version validation on deserialization
// ===========================================================================

// TestSession_VersionValidation_TooManySessions verifies that deserializing
// snapshot data with numSessions exceeding maxTotal produces a BoundsError.
// TestSession_VersionValidation_TooManyResponses verifies that
// deserializing snapshot data with numResponses per session exceeding
// maxResponses produces a BoundsError.
// TestSession_VersionValidation_TruncatedData verifies that deserializing
// truncated snapshot data produces an UnmarshalError.
// ===========================================================================
// Save/restore with eviction ordering preserved
// ===========================================================================

// TestSession_SaveRestoreWithEvictionOrdering verifies that after
// advancing respondedTo (which evicts cached responses), the remaining
// responses survive a snapshot round-trip in the correct order.
// ===========================================================================
// Save/restore with large response data payloads
// ===========================================================================

// TestSession_SaveRestoreLargeResponseData verifies that sessions with
// large Data payloads survive a snapshot round-trip without truncation
// or corruption.
// ===========================================================================
// Unmarshal with truncated session body
// ===========================================================================

// TestSession_UnmarshalTruncatedSessionBody verifies that UnmarshalFrom
// returns an error when the session body is truncated after the header
// declares one session.
// ===========================================================================
// Unmarshal from completely empty reader
// ===========================================================================

// TestSession_UnmarshalFromEmptyReader verifies that UnmarshalFrom returns
// an UnmarshalError when given an empty reader (no header at all).
// ===========================================================================
// Unmarshal with response data exceeding maxResponseDataSize
// ===========================================================================

// TestSession_UnmarshalResponseDataExceedsMax verifies that UnmarshalFrom
// returns a BoundsError when a single response's data length exceeds the
// hardcoded maxResponseDataSize limit (64 MiB).
// ===========================================================================
// Save/restore replaces existing state completely
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
