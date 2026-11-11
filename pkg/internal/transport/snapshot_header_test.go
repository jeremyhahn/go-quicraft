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

// TestSnapshotHeaderMarshalUnmarshalRoundTrip verifies that a header survives
// MarshalTo followed by UnmarshalFrom unchanged, that both report exactly
// SnapshotHeaderSize bytes, and that neither depends on the buffer being
// exactly that long. The single-field cases catch a field written at, or read
// from, another field's offset.
func TestSnapshotHeaderMarshalUnmarshalRoundTrip(t *testing.T) {
	headers := []struct {
		name   string
		header SnapshotHeader
	}{
		{
			name: "typical",
			header: SnapshotHeader{
				ShardID:    1,
				ReplicaID:  2,
				Index:      100,
				Term:       5,
				ChunkCount: 42,
				Epoch:      7,
			},
		},
		{
			name: "max values",
			header: SnapshotHeader{
				ShardID:    math.MaxUint64,
				ReplicaID:  math.MaxUint64,
				Index:      math.MaxUint64,
				Term:       math.MaxUint64,
				ChunkCount: math.MaxUint64,
				Epoch:      math.MaxUint64,
			},
		},
		{name: "zero values", header: SnapshotHeader{}},
		{name: "only ShardID", header: SnapshotHeader{ShardID: 42}},
		{name: "only ReplicaID", header: SnapshotHeader{ReplicaID: 99}},
		{name: "only Index", header: SnapshotHeader{Index: 12345}},
		{name: "only Term", header: SnapshotHeader{Term: 67890}},
		{name: "only ChunkCount", header: SnapshotHeader{ChunkCount: 1000}},
		{name: "only Epoch", header: SnapshotHeader{Epoch: 42}},
	}
	buffers := []struct {
		name string
		size int
	}{
		{name: "exact buffer", size: SnapshotHeaderSize},
		{name: "larger buffer", size: SnapshotHeaderSize + 100},
	}

	for _, tc := range headers {
		for _, b := range buffers {
			t.Run(tc.name+"/"+b.name, func(t *testing.T) {
				buf := make([]byte, b.size)
				n, err := tc.header.MarshalTo(buf)
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
				if decoded != tc.header {
					t.Fatalf("round-trip mismatch: got %+v, want %+v", decoded, tc.header)
				}
			})
		}
	}
}

// TestSnapshotHeaderBufferTooSmall verifies that MarshalTo and UnmarshalFrom
// both return ErrBufferTooSmall for a nil buffer and for one a single byte
// short of SnapshotHeaderSize, instead of writing or reading out of bounds.
func TestSnapshotHeaderBufferTooSmall(t *testing.T) {
	buffers := []struct {
		name string
		buf  []byte
	}{
		{name: "nil buffer", buf: nil},
		{name: "one byte short", buf: make([]byte, SnapshotHeaderSize-1)},
	}

	for _, b := range buffers {
		t.Run("MarshalTo/"+b.name, func(t *testing.T) {
			h := SnapshotHeader{ShardID: 1}
			if _, err := h.MarshalTo(b.buf); !errors.Is(err, ErrBufferTooSmall) {
				t.Fatalf("expected ErrBufferTooSmall, got %v", err)
			}
		})
		t.Run("UnmarshalFrom/"+b.name, func(t *testing.T) {
			var h SnapshotHeader
			if _, err := h.UnmarshalFrom(b.buf); !errors.Is(err, ErrBufferTooSmall) {
				t.Fatalf("expected ErrBufferTooSmall, got %v", err)
			}
		})
	}
}
