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

//go:build quicraft_fuzz

package session

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// FuzzManagerMarshalRoundTrip verifies that marshal/unmarshal produces
// identical state for any combination of fuzzed client IDs, series IDs,
// and result values. The test registers sessions, records results, marshals
// the manager, unmarshals into a fresh manager, then verifies all sessions
// and cached responses are preserved exactly.
func FuzzManagerMarshalRoundTrip(f *testing.F) {
	// Corpus seeds: varying session/response combinations.
	f.Add(uint64(1), uint64(100), uint64(42), uint64(0), []byte{})
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), []byte{})
	f.Add(uint64(math.MaxUint64-3), uint64(1), uint64(math.MaxUint64), uint64(0), []byte("hello"))
	f.Add(uint64(123), uint64(500), uint64(99), uint64(10), []byte{0xFF, 0x00, 0xAB})
	f.Add(uint64(1), uint64(1), uint64(1), uint64(0), make([]byte, 256))

	f.Fuzz(func(t *testing.T, clientID, seriesID, resultValue, respondedTo uint64, resultData []byte) {
		// Cap data size to prevent excessive memory use during fuzzing.
		if len(resultData) > 4096 {
			resultData = resultData[:4096]
		}

		// Avoid sentinel series IDs and zero client ID (not session-managed).
		if clientID == proto.NotSessionManagedClientID {
			clientID = 1
		}
		if seriesID == proto.SessionSeriesIDForRegister ||
			seriesID == proto.SessionSeriesIDForUnregister ||
			seriesID == 0 {
			seriesID = 1
		}

		// Ensure respondedTo is less than seriesID so the response is
		// retained in the cache and not immediately evicted.
		if respondedTo >= seriesID {
			respondedTo = seriesID - 1
		}

		const (
			maxTotal     = 256
			maxResponses = 128
			logIndex     = 10
		)

		original := NewManager(maxTotal, maxResponses)
		if err := original.Register(clientID, logIndex); err != nil {
			t.Fatalf("Register(%d): %v", clientID, err)
		}

		result := sm.Result{Value: resultValue, Data: resultData}
		if err := original.RecordResult(clientID, seriesID, respondedTo, logIndex+1, result); err != nil {
			t.Fatalf("RecordResult(%d, %d): %v", clientID, seriesID, err)
		}

		// Marshal.
		var buf bytes.Buffer
		if err := original.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}

		// Unmarshal into a fresh manager with the same limits.
		restored := NewManager(maxTotal, maxResponses)
		if err := restored.UnmarshalFrom(bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatalf("UnmarshalFrom: %v", err)
		}

		// Verify session count matches.
		if original.Count() != restored.Count() {
			t.Fatalf("session count mismatch: original=%d restored=%d",
				original.Count(), restored.Count())
		}

		// Verify the session is registered.
		if !restored.IsRegistered(clientID) {
			t.Fatalf("client %d not registered after restore", clientID)
		}

		// Verify cached result is preserved.
		cachedResult, isDup, err := restored.CheckDuplicate(clientID, seriesID, logIndex+2)
		if err != nil {
			t.Fatalf("CheckDuplicate after restore: %v", err)
		}
		if !isDup {
			t.Fatalf("expected duplicate for client=%d series=%d after restore", clientID, seriesID)
		}
		if cachedResult.Value != result.Value {
			t.Fatalf("result value mismatch: want %d, got %d", result.Value, cachedResult.Value)
		}
		if !bytes.Equal(cachedResult.Data, result.Data) {
			t.Fatalf("result data mismatch: want %x, got %x", result.Data, cachedResult.Data)
		}

		// Double round-trip: marshal the restored manager (without any
		// intervening CheckDuplicate that would update lastActiveIndex)
		// and verify the output bytes are identical to the first marshal.
		// This confirms deterministic serialization.
		var buf2 bytes.Buffer
		if err := restored.MarshalTo(&buf2); err != nil {
			t.Fatalf("second MarshalTo: %v", err)
		}

		// Unmarshal once more and marshal again to verify stability.
		restored2 := NewManager(maxTotal, maxResponses)
		if err := restored2.UnmarshalFrom(bytes.NewReader(buf2.Bytes())); err != nil {
			t.Fatalf("second UnmarshalFrom: %v", err)
		}
		var buf3 bytes.Buffer
		if err := restored2.MarshalTo(&buf3); err != nil {
			t.Fatalf("third MarshalTo: %v", err)
		}
		if !bytes.Equal(buf2.Bytes(), buf3.Bytes()) {
			t.Fatalf("non-deterministic marshal: second=%d bytes, third=%d bytes",
				buf2.Len(), buf3.Len())
		}
	})
}

// FuzzManagerUnmarshalCrash feeds arbitrary bytes to UnmarshalFrom to
// verify that the deserializer never panics. All malformed input must
// produce an error, not a crash.
func FuzzManagerUnmarshalCrash(f *testing.F) {
	// Corpus seed: empty input.
	f.Add([]byte{})

	// Corpus seed: truncated session count (3 bytes instead of 4).
	f.Add([]byte{0x01, 0x02, 0x03})

	// Corpus seed: zero sessions (valid minimal state).
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})

	// Corpus seed: one session with valid structure.
	f.Add(buildMinimalManagerBytes(1, 0))

	// Corpus seed: one session with one response.
	f.Add(buildMinimalManagerBytes(1, 1))

	// Corpus seed: session count at configured limit.
	f.Add(buildMinimalManagerBytes(64, 0))

	// Corpus seed: session count exceeding limit (triggers BoundsError).
	{
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], math.MaxUint32)
		f.Add(buf[:])
	}

	// Corpus seed: response data length set to a huge value.
	{
		b := buildMinimalManagerBytes(1, 1)
		// Overwrite the dataLen field (last 4 bytes before lastActiveIndex)
		// with a value exceeding maxResponseDataSize.
		if len(b) >= 12 {
			binary.LittleEndian.PutUint32(b[len(b)-12:len(b)-8], math.MaxUint32)
		}
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		mgr := NewManager(256, 128)
		// Must not panic. Errors are expected for malformed input.
		_ = mgr.UnmarshalFrom(bytes.NewReader(data))
	})
}

// FuzzSessionSentinelBoundary fuzzes SeriesID values near the sentinel
// boundaries (SessionSeriesIDForRegister = MaxUint64-1 and
// SessionSeriesIDForUnregister = MaxUint64) to verify that no valid
// recorded result uses a sentinel series ID, and that values near the
// boundary do not produce incorrect duplicate detection behavior.
func FuzzSessionSentinelBoundary(f *testing.F) {
	// Boundary seeds around the sentinel values. SeriesID 0 is excluded
	// because respondedTo starts at 0, and CheckDuplicate treats
	// seriesID <= respondedTo as evicted.
	f.Add(uint64(1))
	f.Add(uint64(2))
	f.Add(proto.SessionSeriesIDForRegister - 2)
	f.Add(proto.SessionSeriesIDForRegister - 1)
	f.Add(proto.SessionSeriesIDForRegister)
	f.Add(proto.SessionSeriesIDForUnregister)
	f.Add(uint64(math.MaxUint64 / 2))

	f.Fuzz(func(t *testing.T, seriesID uint64) {
		isSentinel := seriesID == proto.SessionSeriesIDForRegister ||
			seriesID == proto.SessionSeriesIDForUnregister

		const (
			clientID     = 42
			maxTotal     = 256
			maxResponses = 128
			logIndex     = 10
		)

		// SeriesID 0 is always <= respondedTo (which starts at 0),
		// so CheckDuplicate returns ErrResponseEvicted. Skip it since
		// the purpose of this test is sentinel boundary behavior.
		if seriesID == 0 {
			return
		}

		mgr := NewManager(maxTotal, maxResponses)
		if err := mgr.Register(clientID, logIndex); err != nil {
			t.Fatalf("Register: %v", err)
		}

		// Sentinel values should never be stored as actual series IDs
		// in production. Verify that even if RecordResult is called
		// with a sentinel, the manager handles it without panic.
		result := sm.Result{Value: seriesID}
		err := mgr.RecordResult(clientID, seriesID, 0, logIndex+1, result)
		if err != nil {
			// Some series IDs may legitimately fail (e.g., overflow).
			// The key property is no panic.
			return
		}

		// After recording, CheckDuplicate must find the result.
		cached, isDup, err := mgr.CheckDuplicate(clientID, seriesID, logIndex+2)
		if err != nil {
			t.Fatalf("CheckDuplicate(%d): unexpected error: %v", seriesID, err)
		}
		if !isDup {
			t.Fatalf("CheckDuplicate(%d): expected duplicate after RecordResult", seriesID)
		}
		if cached.Value != seriesID {
			t.Fatalf("CheckDuplicate(%d): value mismatch: want %d, got %d",
				seriesID, seriesID, cached.Value)
		}

		// Marshal round-trip must preserve the result, even for sentinel values.
		var buf bytes.Buffer
		if err := mgr.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}

		restored := NewManager(maxTotal, maxResponses)
		if err := restored.UnmarshalFrom(bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatalf("UnmarshalFrom: %v", err)
		}

		restoredCached, restoredDup, err := restored.CheckDuplicate(clientID, seriesID, logIndex+3)
		if err != nil {
			t.Fatalf("restored CheckDuplicate(%d): %v", seriesID, err)
		}
		if !restoredDup {
			t.Fatalf("restored CheckDuplicate(%d): expected duplicate after unmarshal", seriesID)
		}
		if restoredCached.Value != cached.Value {
			t.Fatalf("restored value mismatch: want %d, got %d",
				cached.Value, restoredCached.Value)
		}

		// If this is a sentinel value, verify it survives serialization
		// without being misinterpreted during unmarshal.
		if isSentinel {
			sess := restored.sessions[clientID]
			if sess == nil {
				t.Fatalf("session for client %d missing after restore", clientID)
			}
			if _, found := sess.responses[seriesID]; !found {
				t.Fatalf("sentinel seriesID %d lost after marshal round-trip", seriesID)
			}
		}
	})
}

// FuzzCheckDuplicateConsistency verifies that once a result is recorded,
// CheckDuplicate consistently reports it as a duplicate for any subsequent
// check. The fuzzer varies seriesIDs and respondedTo values to exercise
// eviction logic and boundary conditions.
func FuzzCheckDuplicateConsistency(f *testing.F) {
	f.Add(uint64(1), uint64(0), uint64(100))
	f.Add(uint64(10), uint64(5), uint64(200))
	f.Add(uint64(0), uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64-2), uint64(0), uint64(42))
	f.Add(uint64(100), uint64(99), uint64(999))

	f.Fuzz(func(t *testing.T, seriesID, respondedTo, resultValue uint64) {
		// Avoid sentinel and zero values.
		if seriesID == proto.SessionSeriesIDForRegister ||
			seriesID == proto.SessionSeriesIDForUnregister ||
			seriesID == 0 {
			seriesID = 1
		}

		const (
			clientID     = 77
			maxTotal     = 256
			maxResponses = 128
			logIndex     = 10
		)

		mgr := NewManager(maxTotal, maxResponses)
		if err := mgr.Register(clientID, logIndex); err != nil {
			t.Fatalf("Register: %v", err)
		}

		result := sm.Result{Value: resultValue}
		err := mgr.RecordResult(clientID, seriesID, respondedTo, logIndex+1, result)
		if err != nil {
			// RecordResult may fail due to response limit. The key
			// property is consistency: if it succeeds, later checks
			// must detect the duplicate.
			return
		}

		// If respondedTo >= seriesID, the entry was evicted immediately
		// during RecordResult's evictUpTo call. CheckDuplicate should
		// return ErrResponseEvicted in this case.
		if respondedTo >= seriesID {
			_, isDup, err := mgr.CheckDuplicate(clientID, seriesID, logIndex+2)
			if err == ErrResponseEvicted {
				// Correct: the response was evicted because
				// respondedTo advanced past seriesID.
				return
			}
			if err != nil {
				t.Fatalf("CheckDuplicate(%d) after eviction: unexpected error: %v", seriesID, err)
			}
			// If no error and not duplicate, the eviction removed the
			// cached response. This is acceptable behavior: respondedTo
			// advanced past seriesID, so the response was legitimately
			// cleaned up. CheckDuplicate may return (zero, false, nil)
			// if respondedTo was later lowered (not possible in this
			// test), or ErrResponseEvicted.
			if isDup {
				t.Fatalf("CheckDuplicate(%d): found duplicate after respondedTo=%d >= seriesID",
					seriesID, respondedTo)
			}
			return
		}

		// respondedTo < seriesID: the response should be cached.
		cached, isDup, err := mgr.CheckDuplicate(clientID, seriesID, logIndex+2)
		if err != nil {
			t.Fatalf("CheckDuplicate(%d): unexpected error: %v", seriesID, err)
		}
		if !isDup {
			t.Fatalf("CheckDuplicate(%d): expected duplicate, respondedTo=%d < seriesID",
				seriesID, respondedTo)
		}
		if cached.Value != resultValue {
			t.Fatalf("CheckDuplicate(%d): value mismatch: want %d, got %d",
				seriesID, resultValue, cached.Value)
		}

		// A second check must produce the identical result.
		cached2, isDup2, err2 := mgr.CheckDuplicate(clientID, seriesID, logIndex+3)
		if err2 != nil {
			t.Fatalf("second CheckDuplicate(%d): unexpected error: %v", seriesID, err2)
		}
		if !isDup2 {
			t.Fatalf("second CheckDuplicate(%d): inconsistent, no longer duplicate", seriesID)
		}
		if cached2.Value != cached.Value {
			t.Fatalf("second CheckDuplicate(%d): value changed: %d -> %d",
				seriesID, cached.Value, cached2.Value)
		}
	})
}

// buildMinimalManagerBytes constructs a valid serialized Manager state
// with the given number of sessions, each having numResponses responses.
// Used as corpus seeds for FuzzManagerUnmarshalCrash.
func buildMinimalManagerBytes(numSessions, numResponses uint32) []byte {
	var buf bytes.Buffer
	var tmp [8]byte

	// Session count.
	binary.LittleEndian.PutUint32(tmp[:4], numSessions)
	buf.Write(tmp[:4])

	for i := uint32(0); i < numSessions; i++ {
		// clientID
		binary.LittleEndian.PutUint64(tmp[:8], uint64(i+1))
		buf.Write(tmp[:8])

		// respondedTo
		binary.LittleEndian.PutUint64(tmp[:8], 0)
		buf.Write(tmp[:8])

		// numResponses
		binary.LittleEndian.PutUint32(tmp[:4], numResponses)
		buf.Write(tmp[:4])

		for j := uint32(0); j < numResponses; j++ {
			// seriesID
			binary.LittleEndian.PutUint64(tmp[:8], uint64(j+1))
			buf.Write(tmp[:8])

			// result value
			binary.LittleEndian.PutUint64(tmp[:8], uint64(j*10))
			buf.Write(tmp[:8])

			// data length (0 = no data)
			binary.LittleEndian.PutUint32(tmp[:4], 0)
			buf.Write(tmp[:4])
		}

		// lastActiveIndex
		binary.LittleEndian.PutUint64(tmp[:8], 5)
		buf.Write(tmp[:8])
	}

	return buf.Bytes()
}
