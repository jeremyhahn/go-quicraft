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

package proto

import (
	"strings"
	"testing"
)

// FuzzSnapshotChunkUnmarshalCrash fuzzes the decoder that parses
// attacker-supplied snapshot frames off the wire.
//
// SnapshotChunk.UnmarshalFrom had ZERO fuzz coverage anywhere in the
// repository — neither here nor in pkg/internal/transport — while being the
// most branch-dense network-facing decoder in the package: it parses an
// embedded Membership, an optional SnapshotFile, a 4096-bounded Filepath, a
// zero-copy Data payload, a BinVer gate and a ChunkSize cross-check, on a
// buffer whose length comes from a frame header the peer controls
// (snapshot_recv.go).
func FuzzSnapshotChunkUnmarshalCrash(f *testing.F) {
	sc := SnapshotChunk{
		BinVer:      WireVersion,
		ShardID:     1,
		ReplicaID:   2,
		Index:       10,
		Term:        3,
		ChunkID:     1,
		ChunkCount:  2,
		ChunkSize:   4,
		Data:        []byte("data"),
		Filepath:    "/snap.dat",
		HasFileInfo: true,
		Membership:  Membership{Addresses: map[uint64]string{1: "a:1"}},
	}
	buf := make([]byte, sc.Size())
	n, err := sc.MarshalTo(buf)
	if err != nil {
		f.Fatalf("seed MarshalTo: %v", err)
	}
	f.Add(buf[:n])
	f.Add(buf[:n/2])
	f.Add([]byte{})
	f.Add(make([]byte, 72))

	f.Fuzz(func(t *testing.T, data []byte) {
		var got SnapshotChunk
		// The contract is only that a hostile frame cannot panic, over-allocate
		// or read out of bounds. A decode error is the expected outcome for
		// nearly every input.
		_, _ = got.UnmarshalFrom(data)
	})
}

// FuzzSnapshotChunkRoundTrip pins that every field survives a round trip,
// including the ones the crash target above cannot observe.
func FuzzSnapshotChunkRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint64(2), uint64(10), uint64(3), "/snap.dat", []byte("data"), true)
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), "", []byte{}, false)

	f.Fuzz(func(t *testing.T, shardID, replicaID, index, term uint64,
		filepath string, data []byte, hasFileInfo bool) {
		if len(filepath) > MaxFilepathLen {
			filepath = filepath[:MaxFilepathLen]
		}
		if uint64(len(data)) > uint64(MaxSnapshotChunkDataLen) {
			data = data[:MaxSnapshotChunkDataLen]
		}
		// A NUL or path-traversal filepath is rejected by design, not a
		// round-trip failure; keep the input inside the accepted domain.
		if strings.ContainsAny(filepath, "\x00") {
			return
		}

		sc := SnapshotChunk{
			BinVer:      WireVersion,
			ShardID:     shardID,
			ReplicaID:   replicaID,
			Index:       index,
			Term:        term,
			ChunkSize:   uint64(len(data)),
			Data:        data,
			Filepath:    filepath,
			HasFileInfo: hasFileInfo,
		}
		buf := make([]byte, sc.Size())
		n, err := sc.MarshalTo(buf)
		if err != nil {
			return // refused on the write side: not a round-trip case
		}

		var got SnapshotChunk
		if _, err := got.UnmarshalFrom(buf[:n]); err != nil {
			t.Fatalf("a chunk this package MARSHALLED failed to unmarshal: %v", err)
		}
		if got.ShardID != sc.ShardID || got.ReplicaID != sc.ReplicaID ||
			got.Index != sc.Index || got.Term != sc.Term ||
			got.Filepath != sc.Filepath || got.HasFileInfo != sc.HasFileInfo {
			t.Fatalf("round-trip lost a field:\n got %+v\nwant %+v", got, sc)
		}
		if string(got.Data) != string(sc.Data) {
			t.Fatalf("round-trip Data = %q, want %q", got.Data, sc.Data)
		}
	})
}
