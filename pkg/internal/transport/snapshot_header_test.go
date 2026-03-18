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

package transport

import (
	"errors"
	"math"
	"testing"
)

// TestSnapshotHeaderMarshalUnmarshalRoundTrip verifies that marshaling
// and unmarshaling a SnapshotHeader produces identical values.
func TestSnapshotHeaderMarshalUnmarshalRoundTrip(t *testing.T) {
	original := SnapshotHeader{
		ShardID:    1,
		ReplicaID:  2,
		Index:      100,
		Term:       5,
		ChunkCount: 42,
		Epoch:      7,
	}

	buf := make([]byte, SnapshotHeaderSize)
	n, err := original.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}
	if n != SnapshotHeaderSize {
		t.Fatalf("expected %d bytes written, got %d", SnapshotHeaderSize, n)
	}

	var decoded SnapshotHeader
	n, err = decoded.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom failed: %v", err)
	}
	if n != SnapshotHeaderSize {
		t.Fatalf("expected %d bytes consumed, got %d", SnapshotHeaderSize, n)
	}

	if decoded.ShardID != original.ShardID {
		t.Fatalf("ShardID: expected %d, got %d", original.ShardID, decoded.ShardID)
	}
	if decoded.ReplicaID != original.ReplicaID {
		t.Fatalf("ReplicaID: expected %d, got %d", original.ReplicaID, decoded.ReplicaID)
	}
	if decoded.Index != original.Index {
		t.Fatalf("Index: expected %d, got %d", original.Index, decoded.Index)
	}
	if decoded.Term != original.Term {
		t.Fatalf("Term: expected %d, got %d", original.Term, decoded.Term)
	}
	if decoded.ChunkCount != original.ChunkCount {
		t.Fatalf("ChunkCount: expected %d, got %d", original.ChunkCount, decoded.ChunkCount)
	}
	if decoded.Epoch != original.Epoch {
		t.Fatalf("Epoch: expected %d, got %d", original.Epoch, decoded.Epoch)
	}
}

// TestSnapshotHeaderMarshalToBufferTooSmall verifies MarshalTo rejects
// buffers smaller than SnapshotHeaderSize.
func TestSnapshotHeaderMarshalToBufferTooSmall(t *testing.T) {
	h := SnapshotHeader{ShardID: 1}
	buf := make([]byte, SnapshotHeaderSize-1)
	_, err := h.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("expected ErrBufferTooSmall, got %v", err)
	}
}

// TestSnapshotHeaderMarshalToNilBuffer verifies MarshalTo rejects nil buffer.
func TestSnapshotHeaderMarshalToNilBuffer(t *testing.T) {
	h := SnapshotHeader{ShardID: 1}
	_, err := h.MarshalTo(nil)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("expected ErrBufferTooSmall, got %v", err)
	}
}

// TestSnapshotHeaderUnmarshalFromBufferTooSmall verifies UnmarshalFrom
// rejects buffers smaller than SnapshotHeaderSize.
func TestSnapshotHeaderUnmarshalFromBufferTooSmall(t *testing.T) {
	var h SnapshotHeader
	buf := make([]byte, SnapshotHeaderSize-1)
	_, err := h.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("expected ErrBufferTooSmall, got %v", err)
	}
}

// TestSnapshotHeaderUnmarshalFromNilBuffer verifies UnmarshalFrom rejects
// nil buffer.
func TestSnapshotHeaderUnmarshalFromNilBuffer(t *testing.T) {
	var h SnapshotHeader
	_, err := h.UnmarshalFrom(nil)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("expected ErrBufferTooSmall, got %v", err)
	}
}

// TestSnapshotHeaderMaxValues verifies correct handling of maximum uint64
// values for all fields (boundary condition test).
func TestSnapshotHeaderMaxValues(t *testing.T) {
	original := SnapshotHeader{
		ShardID:    math.MaxUint64,
		ReplicaID:  math.MaxUint64,
		Index:      math.MaxUint64,
		Term:       math.MaxUint64,
		ChunkCount: math.MaxUint64,
		Epoch:      math.MaxUint64,
	}

	buf := make([]byte, SnapshotHeaderSize)
	if _, err := original.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}

	var decoded SnapshotHeader
	if _, err := decoded.UnmarshalFrom(buf); err != nil {
		t.Fatalf("UnmarshalFrom failed: %v", err)
	}

	if decoded.ShardID != math.MaxUint64 {
		t.Fatalf("ShardID: expected MaxUint64, got %d", decoded.ShardID)
	}
	if decoded.ReplicaID != math.MaxUint64 {
		t.Fatalf("ReplicaID: expected MaxUint64, got %d", decoded.ReplicaID)
	}
	if decoded.Index != math.MaxUint64 {
		t.Fatalf("Index: expected MaxUint64, got %d", decoded.Index)
	}
	if decoded.Term != math.MaxUint64 {
		t.Fatalf("Term: expected MaxUint64, got %d", decoded.Term)
	}
	if decoded.ChunkCount != math.MaxUint64 {
		t.Fatalf("ChunkCount: expected MaxUint64, got %d", decoded.ChunkCount)
	}
	if decoded.Epoch != math.MaxUint64 {
		t.Fatalf("Epoch: expected MaxUint64, got %d", decoded.Epoch)
	}
}

// TestSnapshotHeaderZeroValues verifies correct handling of all-zero fields.
func TestSnapshotHeaderZeroValues(t *testing.T) {
	original := SnapshotHeader{}

	buf := make([]byte, SnapshotHeaderSize)
	if _, err := original.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}

	var decoded SnapshotHeader
	if _, err := decoded.UnmarshalFrom(buf); err != nil {
		t.Fatalf("UnmarshalFrom failed: %v", err)
	}

	if decoded.ShardID != 0 || decoded.ReplicaID != 0 || decoded.Index != 0 ||
		decoded.Term != 0 || decoded.ChunkCount != 0 || decoded.Epoch != 0 {
		t.Fatalf("zero-value round-trip failed: %+v", decoded)
	}
}

// TestSnapshotHeaderLargerBuffer verifies that MarshalTo and UnmarshalFrom
// work correctly with a buffer larger than SnapshotHeaderSize.
func TestSnapshotHeaderLargerBuffer(t *testing.T) {
	original := SnapshotHeader{
		ShardID:    10,
		ReplicaID:  20,
		Index:      300,
		Term:       4,
		ChunkCount: 50,
		Epoch:      11,
	}

	// Buffer with extra room.
	buf := make([]byte, SnapshotHeaderSize+100)
	n, err := original.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}
	if n != SnapshotHeaderSize {
		t.Fatalf("expected %d bytes written, got %d", SnapshotHeaderSize, n)
	}

	var decoded SnapshotHeader
	n, err = decoded.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom failed: %v", err)
	}
	if n != SnapshotHeaderSize {
		t.Fatalf("expected %d bytes consumed, got %d", SnapshotHeaderSize, n)
	}
	if decoded != original {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", decoded, original)
	}
}

// TestSnapshotHeaderSizeConstant verifies the documented constant value.
func TestSnapshotHeaderSizeConstant(t *testing.T) {
	expected := 6 * 8 // 6 uint64 fields at 8 bytes each
	if SnapshotHeaderSize != expected {
		t.Fatalf("SnapshotHeaderSize: expected %d, got %d", expected, SnapshotHeaderSize)
	}
}

// TestSnapshotHeaderExactBufferSize verifies marshaling into an exactly-
// sized buffer succeeds without issue.
func TestSnapshotHeaderExactBufferSize(t *testing.T) {
	h := SnapshotHeader{
		ShardID:    7,
		ReplicaID:  3,
		Index:      999,
		Term:       12,
		ChunkCount: 128,
		Epoch:      4,
	}
	buf := make([]byte, SnapshotHeaderSize) // exact size
	n, err := h.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo with exact buffer failed: %v", err)
	}
	if n != SnapshotHeaderSize {
		t.Fatalf("expected %d bytes, got %d", SnapshotHeaderSize, n)
	}
}

// TestSnapshotHeaderFieldIsolation verifies that each field is stored
// independently and does not bleed into adjacent fields.
func TestSnapshotHeaderFieldIsolation(t *testing.T) {
	tests := []struct {
		name   string
		header SnapshotHeader
	}{
		{
			name: "only ShardID",
			header: SnapshotHeader{
				ShardID: 42,
			},
		},
		{
			name: "only ReplicaID",
			header: SnapshotHeader{
				ReplicaID: 99,
			},
		},
		{
			name: "only Index",
			header: SnapshotHeader{
				Index: 12345,
			},
		},
		{
			name: "only Term",
			header: SnapshotHeader{
				Term: 67890,
			},
		},
		{
			name: "only ChunkCount",
			header: SnapshotHeader{
				ChunkCount: 1000,
			},
		},
		{
			name: "only Epoch",
			header: SnapshotHeader{
				Epoch: 42,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, SnapshotHeaderSize)
			if _, err := tc.header.MarshalTo(buf); err != nil {
				t.Fatalf("MarshalTo failed: %v", err)
			}

			var decoded SnapshotHeader
			if _, err := decoded.UnmarshalFrom(buf); err != nil {
				t.Fatalf("UnmarshalFrom failed: %v", err)
			}

			if decoded != tc.header {
				t.Fatalf("field isolation failed: got %+v, want %+v", decoded, tc.header)
			}
		})
	}
}
