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

package proto

import (
	"errors"
	"testing"
)

// TestSnapshotChunkRejectsChunkSizeMismatch pins the only thing stopping a
// peer from advertising a ChunkSize header that disagrees with the payload it
// actually sent.
//
// The guard had NO test anywhere in the repository — neutering it left the
// whole package suite green — while the decoder's own comment states that
// consumers must not trust the header for allocation or offset math. Twenty of
// the twenty-five mutations run against this package were killed, which is why
// this one stood out.
func TestSnapshotChunkRejectsChunkSizeMismatch(t *testing.T) {
	sc := SnapshotChunk{BinVer: WireVersion, ChunkSize: 4, Data: []byte("data")}
	buf := make([]byte, sc.Size())
	n, err := sc.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	// Round-trip intact first, so a failure below is attributable to the
	// tampering rather than to the fixture.
	var ok SnapshotChunk
	if _, err := ok.UnmarshalFrom(buf[:n]); err != nil {
		t.Fatalf("intact chunk did not round-trip: %v", err)
	}

	// Find and rewrite the ChunkSize field by locating its encoded value,
	// rather than hardcoding an offset that a layout change would silently
	// invalidate.
	idx := -1
	for i := 0; i+8 <= n; i += 8 {
		if getUint64(buf[i:]) == 4 {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatal("could not locate the ChunkSize field in the encoding")
	}
	putUint64(buf[idx:], 5) // claims 5 bytes; Data holds 4

	var got SnapshotChunk
	if _, err := got.UnmarshalFrom(buf[:n]); !errors.Is(err, ErrChunkSizeMismatch) {
		t.Fatalf("UnmarshalFrom with a ChunkSize that disagrees with len(Data) "+
			"returned %v, want ErrChunkSizeMismatch", err)
	}
}

// TestSnapshotUnmarshalRejectsInvalidType pins the read-side range check on
// Snapshot.Type. It also had no test: the write-side check at snapshot.go:106
// is covered, the read-side one was not, and Type is a switch-dispatch input
// downstream.
func TestSnapshotUnmarshalRejectsInvalidType(t *testing.T) {
	s := Snapshot{Index: 1, Term: 1, Type: OnDiskStateMachine}
	buf := make([]byte, s.Size())
	n, err := s.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	var ok Snapshot
	if _, err := ok.UnmarshalFrom(buf[:n]); err != nil {
		t.Fatalf("intact snapshot did not round-trip: %v", err)
	}

	// Type is the first of the five trailing fixed fields.
	typeOff := n - 5*8
	putUint64(buf[typeOff:], maxStateMachineType+1)

	var got Snapshot
	_, err = got.UnmarshalFrom(buf[:n])
	if !errors.Is(err, ErrInvalidSnapshotType) {
		t.Fatalf("UnmarshalFrom with Type=%d returned %v, want "+
			"ErrInvalidSnapshotType", maxStateMachineType+1, err)
	}
	// The typed error must name the offending value, and its Is method must
	// actually match the sentinel — neither was exercised before.
	var typErr *InvalidSnapshotTypeError
	if !errors.As(err, &typErr) {
		t.Fatalf("err = %T, want *InvalidSnapshotTypeError", err)
	}
	if typErr.Type != maxStateMachineType+1 {
		t.Errorf("error names Type %d, want %d", typErr.Type, maxStateMachineType+1)
	}
}
