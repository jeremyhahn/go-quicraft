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
	"encoding/binary"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ===========================================================================
// SaveAndRestore: full serialization round-trip with response history
// ===========================================================================

// TestSession_SaveAndRestore verifies a full marshal/unmarshal round-trip
// preserving all session state including response history, respondedTo,
// and lastActiveIndex.
func TestSession_SaveAndRestore(t *testing.T) {
	m := NewManager(100, 100)

	// Register sessions with varying state.
	if err := m.Register(10, 100); err != nil {
		t.Fatalf("Register(10): %v", err)
	}
	if err := m.Register(20, 200); err != nil {
		t.Fatalf("Register(20): %v", err)
	}
	if err := m.Register(30, 300); err != nil {
		t.Fatalf("Register(30): %v", err)
	}

	// Add responses with data.
	if err := m.RecordResult(10, 1, 0, 150, sm.Result{Value: 100, Data: []byte("alpha")}); err != nil {
		t.Fatalf("RecordResult(10,1): %v", err)
	}
	if err := m.RecordResult(10, 2, 0, 160, sm.Result{Value: 200, Data: []byte("beta")}); err != nil {
		t.Fatalf("RecordResult(10,2): %v", err)
	}
	if err := m.RecordResult(20, 1, 0, 250, sm.Result{Value: 300}); err != nil {
		t.Fatalf("RecordResult(20,1): %v", err)
	}

	// Advance respondedTo on session 10 (evicts series 1).
	if err := m.RecordResult(10, 3, 1, 170, sm.Result{Value: 400}); err != nil {
		t.Fatalf("RecordResult(10,3): %v", err)
	}

	// Marshal.
	var buf bytes.Buffer
	if err := m.MarshalTo(&buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("marshal produced empty output")
	}

	// Unmarshal into a new manager.
	restored := NewManager(100, 100)
	if err := restored.UnmarshalFrom(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}

	// Verify session count.
	if restored.Count() != 3 {
		t.Fatalf("restored count = %d, want 3", restored.Count())
	}

	// Verify session 10: respondedTo should be 1, series 1 evicted.
	_, isDup, err := restored.CheckDuplicate(10, 1, 500)
	if !errors.Is(err, ErrResponseEvicted) {
		t.Fatalf("session 10, series 1: expected ErrResponseEvicted, got %v", err)
	}
	if isDup {
		t.Fatal("evicted series should not be duplicate")
	}

	// Series 2 and 3 should still be cached.
	result, isDup, err := restored.CheckDuplicate(10, 2, 500)
	if err != nil {
		t.Fatalf("session 10, series 2: %v", err)
	}
	if !isDup {
		t.Fatal("series 2 should be duplicate")
	}
	if result.Value != 200 || string(result.Data) != "beta" {
		t.Fatalf("series 2: value=%d data=%q, want 200 beta", result.Value, result.Data)
	}

	result, isDup, err = restored.CheckDuplicate(10, 3, 500)
	if err != nil {
		t.Fatalf("session 10, series 3: %v", err)
	}
	if !isDup {
		t.Fatal("series 3 should be duplicate")
	}
	if result.Value != 400 {
		t.Fatalf("series 3: value=%d, want 400", result.Value)
	}

	// Verify session 20.
	result, isDup, err = restored.CheckDuplicate(20, 1, 500)
	if err != nil {
		t.Fatalf("session 20, series 1: %v", err)
	}
	if !isDup {
		t.Fatal("session 20 series 1 should be duplicate")
	}
	if result.Value != 300 {
		t.Fatalf("session 20 series 1: value=%d, want 300", result.Value)
	}

	// Verify session 30 has no responses.
	_, isDup, err = restored.CheckDuplicate(30, 1, 500)
	if err != nil {
		t.Fatalf("session 30, series 1: %v", err)
	}
	if isDup {
		t.Fatal("session 30 should have no duplicates")
	}
}

// TestSession_SaveAndRestore_EmptyManager verifies that an empty manager
// can be saved and restored correctly.
func TestSession_SaveAndRestore_EmptyManager(t *testing.T) {
	m := NewManager(100, 100)

	var buf bytes.Buffer
	if err := m.MarshalTo(&buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	restored := NewManager(100, 100)
	if err := restored.UnmarshalFrom(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}

	if restored.Count() != 0 {
		t.Fatalf("restored count = %d, want 0", restored.Count())
	}
}

// ===========================================================================
// Response addition
// ===========================================================================

// TestSession_ResponseCanBeAdded verifies that individual responses can
// be added to a session and subsequently found via CheckDuplicate.
func TestSession_ResponseCanBeAdded(t *testing.T) {
	m := NewManager(10, 10)
	if err := m.Register(1, 100); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Add response.
	if err := m.RecordResult(1, 5, 0, 200, sm.Result{Value: 42, Data: []byte("data")}); err != nil {
		t.Fatalf("RecordResult: %v", err)
	}

	// Verify it is found.
	result, isDup, err := m.CheckDuplicate(1, 5, 300)
	if err != nil {
		t.Fatalf("CheckDuplicate: %v", err)
	}
	if !isDup {
		t.Fatal("series 5 should be duplicate")
	}
	if result.Value != 42 {
		t.Errorf("Value = %d, want 42", result.Value)
	}
	if string(result.Data) != "data" {
		t.Errorf("Data = %q, want %q", result.Data, "data")
	}
}

// TestSession_ResponseCanBeAdded_UnregisteredSessionReturnsError verifies
// that adding a response to an unregistered session returns ErrSessionNotFound.
func TestSession_ResponseCanBeAdded_UnregisteredSessionReturnsError(t *testing.T) {
	m := NewManager(10, 10)

	err := m.RecordResult(999, 1, 0, 100, sm.Result{Value: 1})
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}
}

// ===========================================================================
// Cached response clearing
// ===========================================================================

// TestSession_CachedResponseCleared verifies that advancing respondedTo
// clears cached responses with seriesID <= respondedTo.
func TestSession_CachedResponseCleared(t *testing.T) {
	m := NewManager(10, 10)
	if err := m.Register(1, 100); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Add responses for series 1, 2, 3.
	for series := uint64(1); series <= 3; series++ {
		if err := m.RecordResult(1, series, 0, 100+series, sm.Result{Value: series * 10}); err != nil {
			t.Fatalf("RecordResult(%d): %v", series, err)
		}
	}

	// Verify all are present.
	for series := uint64(1); series <= 3; series++ {
		_, isDup, err := m.CheckDuplicate(1, series, 200)
		if err != nil {
			t.Fatalf("CheckDuplicate(%d): %v", series, err)
		}
		if !isDup {
			t.Fatalf("series %d should be duplicate", series)
		}
	}

	// Advance respondedTo to 2 (should clear series 1 and 2).
	if err := m.RecordResult(1, 4, 2, 300, sm.Result{Value: 40}); err != nil {
		t.Fatalf("RecordResult(4): %v", err)
	}

	// Series 1 and 2 should be evicted.
	for _, series := range []uint64{1, 2} {
		_, _, err := m.CheckDuplicate(1, series, 400)
		if !errors.Is(err, ErrResponseEvicted) {
			t.Fatalf("series %d: expected ErrResponseEvicted, got %v", series, err)
		}
	}

	// Series 3 should still be present.
	result, isDup, err := m.CheckDuplicate(1, 3, 400)
	if err != nil {
		t.Fatalf("series 3: %v", err)
	}
	if !isDup {
		t.Fatal("series 3 should still be duplicate")
	}
	if result.Value != 30 {
		t.Fatalf("series 3 value = %d, want 30", result.Value)
	}
}

// ===========================================================================
// Response lookup for deduplication
// ===========================================================================

// TestSession_ResponseLookup verifies that duplicate detection correctly
// returns cached results and identifies new vs. duplicate proposals.
func TestSession_ResponseLookup(t *testing.T) {
	m := NewManager(10, 10)
	if err := m.Register(1, 100); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// New proposal should not be duplicate.
	_, isDup, err := m.CheckDuplicate(1, 1, 200)
	if err != nil {
		t.Fatalf("CheckDuplicate (new): %v", err)
	}
	if isDup {
		t.Fatal("new proposal should not be duplicate")
	}

	// Record result.
	if err := m.RecordResult(1, 1, 0, 200, sm.Result{Value: 42, Data: []byte("result")}); err != nil {
		t.Fatalf("RecordResult: %v", err)
	}

	// Same series should now be duplicate.
	result, isDup, err := m.CheckDuplicate(1, 1, 300)
	if err != nil {
		t.Fatalf("CheckDuplicate (dup): %v", err)
	}
	if !isDup {
		t.Fatal("recorded proposal should be duplicate")
	}
	if result.Value != 42 {
		t.Errorf("cached Value = %d, want 42", result.Value)
	}
	if string(result.Data) != "result" {
		t.Errorf("cached Data = %q, want %q", result.Data, "result")
	}

	// Different series should not be duplicate.
	_, isDup, err = m.CheckDuplicate(1, 2, 300)
	if err != nil {
		t.Fatalf("CheckDuplicate (different series): %v", err)
	}
	if isDup {
		t.Fatal("different series should not be duplicate")
	}
}

// ===========================================================================
// Version validation on deserialization
// ===========================================================================

// TestSession_VersionValidation_TooManySessions verifies that deserializing
// snapshot data with numSessions exceeding maxTotal produces a BoundsError.
func TestSession_VersionValidation_TooManySessions(t *testing.T) {
	m := NewManager(2, 10) // maxTotal = 2

	// Craft a snapshot with numSessions = 100 (exceeds limit of 2).
	var buf bytes.Buffer
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], 100)
	buf.Write(header[:])

	err := m.UnmarshalFrom(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatal("expected error for numSessions exceeding maxTotal")
	}

	var boundsErr *BoundsError
	if !errors.As(err, &boundsErr) {
		t.Fatalf("expected BoundsError, got %T: %v", err, err)
	}
	if boundsErr.Field != "numSessions" {
		t.Errorf("field = %q, want %q", boundsErr.Field, "numSessions")
	}
	if boundsErr.Value != 100 {
		t.Errorf("value = %d, want 100", boundsErr.Value)
	}
	if boundsErr.Limit != 2 {
		t.Errorf("limit = %d, want 2", boundsErr.Limit)
	}

	// Also verify errors.Is with ErrSnapshotCorrupted.
	if !errors.Is(err, ErrSnapshotCorrupted) {
		t.Error("BoundsError should match ErrSnapshotCorrupted via errors.Is")
	}
}

// TestSession_VersionValidation_TooManyResponses verifies that
// deserializing snapshot data with numResponses per session exceeding
// maxResponses produces a BoundsError.
func TestSession_VersionValidation_TooManyResponses(t *testing.T) {
	m := NewManager(100, 2) // maxResponses = 2

	// Craft a valid snapshot with 1 session that has 50 responses.
	var buf bytes.Buffer

	// numSessions = 1
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], 1)
	buf.Write(header[:])

	// clientID = 1
	var u64 [8]byte
	binary.LittleEndian.PutUint64(u64[:], 1)
	buf.Write(u64[:])

	// respondedTo = 0
	binary.LittleEndian.PutUint64(u64[:], 0)
	buf.Write(u64[:])

	// numResponses = 50 (exceeds limit of 2)
	binary.LittleEndian.PutUint32(header[:], 50)
	buf.Write(header[:])

	err := m.UnmarshalFrom(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatal("expected error for numResponses exceeding maxResponses")
	}

	var boundsErr *BoundsError
	if !errors.As(err, &boundsErr) {
		t.Fatalf("expected BoundsError, got %T: %v", err, err)
	}
	if boundsErr.Field != "numResponses" {
		t.Errorf("field = %q, want %q", boundsErr.Field, "numResponses")
	}
}

// TestSession_VersionValidation_TruncatedData verifies that deserializing
// truncated snapshot data produces an UnmarshalError.
func TestSession_VersionValidation_TruncatedData(t *testing.T) {
	m := NewManager(100, 100)

	// Only 2 bytes of a 4-byte header.
	err := m.UnmarshalFrom(bytes.NewReader([]byte{0x01, 0x00}))
	if err == nil {
		t.Fatal("expected error for truncated data")
	}

	var unmarshalErr *UnmarshalError
	if !errors.As(err, &unmarshalErr) {
		t.Fatalf("expected UnmarshalError, got %T: %v", err, err)
	}
}

// ===========================================================================
// Save/restore with eviction ordering preserved
// ===========================================================================

// TestSession_SaveRestoreWithEvictionOrdering verifies that after
// advancing respondedTo (which evicts cached responses), the remaining
// responses survive a snapshot round-trip in the correct order.
func TestSession_SaveRestoreWithEvictionOrdering(t *testing.T) {
	m := NewManager(100, 100)

	if err := m.Register(10, 0); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Record series 1 through 5.
	for sid := uint64(1); sid <= 5; sid++ {
		if err := m.RecordResult(10, sid, 0, sid*10, sm.Result{Value: sid * 100}); err != nil {
			t.Fatalf("RecordResult(%d): %v", sid, err)
		}
	}

	// Advance respondedTo to 3, evicting series 1, 2, 3.
	if err := m.RecordResult(10, 6, 3, 60, sm.Result{Value: 600}); err != nil {
		t.Fatalf("RecordResult(6): %v", err)
	}

	// Marshal and unmarshal.
	var buf bytes.Buffer
	if err := m.MarshalTo(&buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	restored := NewManager(100, 100)
	if err := restored.UnmarshalFrom(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}

	// Series 1, 2, 3 should be evicted (respondedTo=3).
	for sid := uint64(1); sid <= 3; sid++ {
		_, _, err := restored.CheckDuplicate(10, sid, 1000)
		if !errors.Is(err, ErrResponseEvicted) {
			t.Errorf("series %d: expected ErrResponseEvicted, got %v", sid, err)
		}
	}

	// Series 4, 5, 6 should survive the snapshot.
	for _, sid := range []uint64{4, 5, 6} {
		result, isDup, err := restored.CheckDuplicate(10, sid, 1000)
		if err != nil {
			t.Fatalf("series %d: %v", sid, err)
		}
		if !isDup {
			t.Errorf("series %d should be duplicate after restore", sid)
		}
		if result.Value != sid*100 {
			t.Errorf("series %d: value=%d, want %d", sid, result.Value, sid*100)
		}
	}
}

// ===========================================================================
// Save/restore with large response data payloads
// ===========================================================================

// TestSession_SaveRestoreLargeResponseData verifies that sessions with
// large Data payloads survive a snapshot round-trip without truncation
// or corruption.
func TestSession_SaveRestoreLargeResponseData(t *testing.T) {
	m := NewManager(100, 100)

	if err := m.Register(10, 0); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Create a 1 KiB data payload.
	largeData := make([]byte, 1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	if err := m.RecordResult(10, 1, 0, 100, sm.Result{Value: 42, Data: largeData}); err != nil {
		t.Fatalf("RecordResult: %v", err)
	}

	var buf bytes.Buffer
	if err := m.MarshalTo(&buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	restored := NewManager(100, 100)
	if err := restored.UnmarshalFrom(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}

	result, isDup, err := restored.CheckDuplicate(10, 1, 200)
	if err != nil {
		t.Fatalf("CheckDuplicate: %v", err)
	}
	if !isDup {
		t.Fatal("series 1 should be duplicate after restore")
	}
	if result.Value != 42 {
		t.Errorf("Value = %d, want 42", result.Value)
	}
	if len(result.Data) != 1024 {
		t.Fatalf("Data len = %d, want 1024", len(result.Data))
	}
	for i, b := range result.Data {
		if b != byte(i%256) {
			t.Fatalf("Data[%d] = %d, want %d", i, b, byte(i%256))
			break
		}
	}
}

// ===========================================================================
// Unmarshal with truncated session body
// ===========================================================================

// TestSession_UnmarshalTruncatedSessionBody verifies that UnmarshalFrom
// returns an error when the session body is truncated after the header
// declares one session.
func TestSession_UnmarshalTruncatedSessionBody(t *testing.T) {
	m := NewManager(100, 100)

	// Craft valid header: numSessions = 1.
	var buf bytes.Buffer
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], 1)
	buf.Write(header[:])

	// Write only clientID (8 bytes) but no respondedTo or rest.
	var partial [8]byte
	binary.LittleEndian.PutUint64(partial[:], 42)
	buf.Write(partial[:])

	err := m.UnmarshalFrom(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatal("expected error for truncated session body")
	}

	var unmarshalErr *UnmarshalError
	if !errors.As(err, &unmarshalErr) {
		t.Fatalf("expected UnmarshalError, got %T: %v", err, err)
	}
}

// ===========================================================================
// Unmarshal from completely empty reader
// ===========================================================================

// TestSession_UnmarshalFromEmptyReader verifies that UnmarshalFrom returns
// an UnmarshalError when given an empty reader (no header at all).
func TestSession_UnmarshalFromEmptyReader(t *testing.T) {
	m := NewManager(100, 100)

	err := m.UnmarshalFrom(bytes.NewReader(nil))
	if err == nil {
		t.Fatal("expected error for empty reader")
	}

	var unmarshalErr *UnmarshalError
	if !errors.As(err, &unmarshalErr) {
		t.Fatalf("expected UnmarshalError, got %T: %v", err, err)
	}
}

// ===========================================================================
// Unmarshal with response data exceeding maxResponseDataSize
// ===========================================================================

// TestSession_UnmarshalResponseDataExceedsMax verifies that UnmarshalFrom
// returns a BoundsError when a single response's data length exceeds the
// hardcoded maxResponseDataSize limit (64 MiB).
func TestSession_UnmarshalResponseDataExceedsMax(t *testing.T) {
	m := NewManager(100, 100)

	var buf bytes.Buffer
	var b4 [4]byte
	var b8 [8]byte

	// numSessions = 1
	binary.LittleEndian.PutUint32(b4[:], 1)
	buf.Write(b4[:])

	// clientID = 1
	binary.LittleEndian.PutUint64(b8[:], 1)
	buf.Write(b8[:])

	// respondedTo = 0
	binary.LittleEndian.PutUint64(b8[:], 0)
	buf.Write(b8[:])

	// numResponses = 1
	binary.LittleEndian.PutUint32(b4[:], 1)
	buf.Write(b4[:])

	// seriesID = 1
	binary.LittleEndian.PutUint64(b8[:], 1)
	buf.Write(b8[:])

	// result value = 42
	binary.LittleEndian.PutUint64(b8[:], 42)
	buf.Write(b8[:])

	// dataLen = maxResponseDataSize + 1 (exceeds 64 MiB limit)
	binary.LittleEndian.PutUint32(b4[:], uint32(maxResponseDataSize+1))
	buf.Write(b4[:])

	err := m.UnmarshalFrom(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatal("expected error for response data exceeding max size")
	}

	var boundsErr *BoundsError
	if !errors.As(err, &boundsErr) {
		t.Fatalf("expected BoundsError, got %T: %v", err, err)
	}
	if boundsErr.Field != "responseDataLen" {
		t.Errorf("field = %q, want %q", boundsErr.Field, "responseDataLen")
	}
	if !errors.Is(err, ErrSnapshotCorrupted) {
		t.Error("BoundsError should match ErrSnapshotCorrupted via errors.Is")
	}
}

// ===========================================================================
// Save/restore replaces existing state completely
// ===========================================================================

// TestSession_RestoreReplacesExistingState verifies that UnmarshalFrom
// completely replaces the manager's existing sessions with the snapshot
// state, removing any pre-existing sessions.
func TestSession_RestoreReplacesExistingState(t *testing.T) {
	m1 := NewManager(100, 100)
	if err := m1.Register(10, 0); err != nil {
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
	m2 := NewManager(100, 100)
	if err := m2.Register(99, 0); err != nil {
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
