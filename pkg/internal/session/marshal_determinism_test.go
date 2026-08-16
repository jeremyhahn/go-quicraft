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
	"io"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// The session table is snapshot payload. Two replicas holding the same session
// state must encode it to the SAME bytes, because the encoded snapshot is what
// a third replica installs and what the snapshot integrity digest is taken
// over: an encoding that depends on Go's randomized map iteration makes two
// honest replicas disagree about a byte-for-byte identical logical state, and
// the disagreement surfaces as a snapshot that fails to verify rather than as
// anything that points back here.
//
// MarshalTo therefore sorts twice — the client IDs in MarshalTo and the series
// IDs in marshalSession — and NEITHER sort was covered. Both could be deleted
// with the whole repository's tests still passing (verified by deleting each in
// turn and running ./pkg/internal/session/... and ./pkg/internal/rsm/... at
// -count=3). The existing round-trip coverage could not see it: the fuzz
// round-trip registers exactly ONE session with exactly ONE cached response, and
// a one-element map has only one iteration order.
//
// These constants are sized so the gate cannot pass by luck. Go randomizes both
// the starting bucket and the offset within it on every range over a map, so an
// unsorted encoding of a 24-entry map reproduces a given order with probability
// well under 1/24 per marshal; requiring 12 independent marshals to agree makes
// a false pass impossible in practice while a sorted encoding passes with
// certainty.
const (
	determinismSessions = 24
	determinismSeries   = 24
	determinismMarshals = 12
)

// buildDeterminismManager registers determinismSessions sessions each holding
// determinismSeries cached responses, inserting both in the order the caller
// dictates. The LOGICAL state is identical for any permutation — only the map
// insertion order differs, which is precisely the axis the encoding must not
// depend on.
func buildDeterminismManager(t *testing.T, sessionOrder, seriesOrder []uint64) *Manager {
	t.Helper()

	// maxResponses must exceed determinismSeries or RecordResult starts
	// evicting and the two managers would no longer hold the same state.
	m := NewManager(determinismSessions*2, determinismSeries*2, 0)
	for _, c := range sessionOrder {
		clientID := c + 1
		if err := m.Register(clientID, clientID*10, 1); err != nil {
			t.Fatalf("Register(%d): %v", clientID, err)
		}
		for _, s := range seriesOrder {
			seriesID := s + 1
			// respondedTo stays 0: advancing it would evict responses and make
			// the surviving set depend on the insertion order, which is the one
			// thing this test must hold constant.
			result := sm.Result{
				Value: seriesID * 1000,
				Data:  []byte{byte(clientID), byte(seriesID)},
			}
			if err := m.RecordResult(clientID, seriesID, 0, 2, result); err != nil {
				t.Fatalf("RecordResult(%d, %d): %v", clientID, seriesID, err)
			}
		}
	}
	return m
}

// ascending returns [0, n).
func ascending(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i)
	}
	return out
}

// descending returns (n, 0] — a different INSERTION order for the same set.
func descending(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(n - 1 - i)
	}
	return out
}

// TestMarshalTo_IsByteIdenticalAcrossReplicas is the gate for both sorts. It
// builds the same logical session table twice, inserting the sessions and each
// session's cached responses in OPPOSITE orders, then marshals each manager
// repeatedly. Every one of the resulting encodings must be byte-identical: the
// first marshal is the reference, and any divergence — between the two managers
// or between two marshals of the same manager — is a replica-visible encoding
// that depends on map iteration order.
//
// Deleting slices.Sort(clientIDs) in MarshalTo or slices.Sort(seriesIDs) in
// marshalSession fails this test.
func TestMarshalTo_IsByteIdenticalAcrossReplicas(t *testing.T) {
	replicaA := buildDeterminismManager(t,
		ascending(determinismSessions), ascending(determinismSeries))
	replicaB := buildDeterminismManager(t,
		descending(determinismSessions), descending(determinismSeries))

	var reference []byte
	for i := range determinismMarshals {
		for name, m := range map[string]*Manager{"A": replicaA, "B": replicaB} {
			var buf bytes.Buffer
			if err := m.MarshalTo(&buf); err != nil {
				t.Fatalf("replica %s, marshal %d: %v", name, i, err)
			}
			got := buf.Bytes()
			if reference == nil {
				reference = bytes.Clone(got)
				continue
			}
			if !bytes.Equal(reference, got) {
				t.Fatalf("replica %s, marshal %d: encoded %d bytes that differ from "+
					"the reference encoding of the SAME session state at byte %d; the "+
					"session table's encoding depends on Go's map iteration order, so "+
					"two replicas snapshot identical state to different bytes",
					name, i, len(got), firstDifference(reference, got))
			}
		}
	}
}

// TestMarshalTo_SessionOrderIsSortedByClientID pins the ORDER itself, not just
// its stability. Byte equality alone would be satisfied by any encoding that is
// merely self-consistent, so this reads the client IDs back off the wire and
// requires them ascending — the property a reader (or a human diffing two
// snapshots) can rely on.
func TestMarshalTo_SessionOrderIsSortedByClientID(t *testing.T) {
	m := buildDeterminismManager(t,
		descending(determinismSessions), descending(determinismSeries))

	var buf bytes.Buffer
	if err := m.MarshalTo(&buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	clientIDs, seriesPerSession := decodeMarshalOrder(t, buf.Bytes())
	if len(clientIDs) != determinismSessions {
		t.Fatalf("decoded %d sessions, want %d", len(clientIDs), determinismSessions)
	}
	for i := 1; i < len(clientIDs); i++ {
		if clientIDs[i-1] >= clientIDs[i] {
			t.Fatalf("session %d has clientID %d after clientID %d: MarshalTo must "+
				"emit sessions in ascending clientID order",
				i, clientIDs[i], clientIDs[i-1])
		}
	}
	for s, seriesIDs := range seriesPerSession {
		if len(seriesIDs) != determinismSeries {
			t.Fatalf("session %d carries %d responses, want %d",
				s, len(seriesIDs), determinismSeries)
		}
		for i := 1; i < len(seriesIDs); i++ {
			if seriesIDs[i-1] >= seriesIDs[i] {
				t.Fatalf("session %d, response %d has seriesID %d after seriesID %d: "+
					"marshalSession must emit responses in ascending seriesID order",
					s, i, seriesIDs[i], seriesIDs[i-1])
			}
		}
	}
}

// decodeMarshalOrder reads a MarshalTo payload and returns the client IDs in
// the order they appear, plus each session's series IDs in the order they
// appear. It deliberately re-implements the read rather than calling
// UnmarshalFrom: UnmarshalFrom decodes into maps, which is exactly the
// information this test needs preserved.
func decodeMarshalOrder(t *testing.T, data []byte) (clientIDs []uint64, seriesPerSession [][]uint64) {
	t.Helper()
	r := bytes.NewReader(data)

	numSessions := readU32(t, r)
	for i := uint32(0); i < numSessions; i++ {
		clientIDs = append(clientIDs, readU64(t, r))
		readU64(t, r) // ownerID
		readU64(t, r) // registeredIndex
		readU64(t, r) // respondedTo

		numResponses := readU32(t, r)
		seriesIDs := make([]uint64, 0, numResponses)
		for j := uint32(0); j < numResponses; j++ {
			seriesIDs = append(seriesIDs, readU64(t, r))
			readU64(t, r) // result value
			dataLen := readU32(t, r)
			if _, err := r.Seek(int64(dataLen), 1); err != nil {
				t.Fatalf("skip result data: %v", err)
			}
		}
		seriesPerSession = append(seriesPerSession, seriesIDs)

		readU64(t, r) // lastActiveIndex
	}
	if r.Len() != 0 {
		t.Fatalf("%d trailing bytes after decoding %d sessions", r.Len(), numSessions)
	}
	return clientIDs, seriesPerSession
}

// readU32 reads one little-endian uint32 off the wire, failing the test on a
// short read so a layout mistake in this decoder surfaces as an error rather
// than as a silently wrong ordering verdict.
func readU32(t *testing.T, r io.Reader) uint32 {
	t.Helper()
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		t.Fatalf("read uint32: %v", err)
	}
	return binary.LittleEndian.Uint32(buf[:])
}

// readU64 reads one little-endian uint64 off the wire. See readU32.
func readU64(t *testing.T, r io.Reader) uint64 {
	t.Helper()
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		t.Fatalf("read uint64: %v", err)
	}
	return binary.LittleEndian.Uint64(buf[:])
}

// firstDifference returns the index of the first differing byte, or the length
// of the shorter slice when one is a prefix of the other.
func firstDifference(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
