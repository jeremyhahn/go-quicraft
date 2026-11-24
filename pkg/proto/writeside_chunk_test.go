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
	"strings"
	"testing"
)

// TestSnapshotChunkMarshalMirrorsItsOwnReadBounds pins the write/read symmetry
// every other type in this package already had.
//
// SnapshotChunk was the one encoder that could emit frames its own decoder
// rejects: an overlong Filepath, oversized Data, or a ChunkSize disagreeing
// with len(Data) all marshalled cleanly and failed on unmarshal. A producer
// whose values drift out of bounds then emits chunks every peer rejects, and
// at the sender a rejection is indistinguishable from a broken connection —
// the exact failure the other seven types were changed to prevent.
func TestSnapshotChunkMarshalMirrorsItsOwnReadBounds(t *testing.T) {
	cases := []struct {
		name string
		sc   SnapshotChunk
		want error
	}{
		{
			name: "overlong filepath",
			sc: SnapshotChunk{
				BinVer:   WireVersion,
				Filepath: strings.Repeat("p", MaxFilepathLen+1),
			},
			want: ErrFilepathTooLong,
		},
		{
			name: "oversized data",
			sc: SnapshotChunk{
				BinVer:    WireVersion,
				ChunkSize: uint64(MaxSnapshotChunkDataLen) + 1,
				Data:      make([]byte, int(MaxSnapshotChunkDataLen)+1),
			},
			want: ErrChunkDataTooLong,
		},
		{
			name: "ChunkSize disagrees with len(Data)",
			sc: SnapshotChunk{
				BinVer:    WireVersion,
				ChunkSize: 99,
				Data:      []byte("abcd"),
			},
			want: ErrChunkSizeMismatch,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			buf := make([]byte, c.sc.Size())
			_, err := c.sc.MarshalTo(buf)
			if !errors.Is(err, c.want) {
				t.Fatalf("MarshalTo = %v, want %v: the encoder emitted a frame "+
					"its own UnmarshalFrom refuses", err, c.want)
			}
		})
	}

	// Control: a well-formed chunk still marshals, so the cases above are
	// rejections rather than a blanket failure.
	ok := SnapshotChunk{
		BinVer: WireVersion, ChunkSize: 4, Data: []byte("data"), Filepath: "/s.dat",
	}
	buf := make([]byte, ok.Size())
	if _, err := ok.MarshalTo(buf); err != nil {
		t.Fatalf("a well-formed chunk was refused: %v", err)
	}
}
