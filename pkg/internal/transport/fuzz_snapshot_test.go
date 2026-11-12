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
	"bytes"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// fuzzSnapshotMemory is the receive memory budget the chunk fuzz targets run
// under: small enough that fuzzed frame lengths reach the budget rejection.
const fuzzSnapshotMemory = 1 << 20

// fuzzSnapshotPeer is the peer key the chunk fuzz targets charge, so the
// per-peer accounting is exercised alongside the global budget.
const fuzzSnapshotPeer = "fuzz-peer"

// encodeSnapshotChunkFrames frames chunks the way the sender does: each chunk
// marshalled by the encoder and written as one frame.
func encodeSnapshotChunkFrames(tb testing.TB, chunks []proto.SnapshotChunk) ([]byte, error) {
	tb.Helper()
	var buf bytes.Buffer
	for i := range chunks {
		payload := make([]byte, chunks[i].Size())
		n, err := chunks[i].MarshalTo(payload)
		if err != nil {
			return nil, err
		}
		if err := WriteFrame(&buf, payload[:n], FlagNone); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// seedSnapshotHeader is a valid header the seeds are built from.
func seedSnapshotHeader() SnapshotHeader {
	return SnapshotHeader{ShardID: 1, ReplicaID: 2, Index: 100, Term: 5, ChunkCount: 2, Epoch: 7, From: 3}
}

// seedSnapshotChunks returns valid chunks matching hdr.
func seedSnapshotChunks(hdr SnapshotHeader) []proto.SnapshotChunk {
	chunks := make([]proto.SnapshotChunk, 0, hdr.ChunkCount)
	for i := uint64(0); i < hdr.ChunkCount; i++ {
		data := []byte("chunk-data")
		chunks = append(chunks, proto.SnapshotChunk{
			BinVer:     proto.WireVersion,
			ShardID:    hdr.ShardID,
			ReplicaID:  hdr.ReplicaID,
			From:       hdr.From,
			Index:      hdr.Index,
			Term:       hdr.Term,
			Epoch:      hdr.Epoch,
			ChunkID:    i,
			ChunkCount: hdr.ChunkCount,
			ChunkSize:  uint64(len(data)),
			Filepath:   "/var/lib/snap",
			Data:       data,
		})
	}
	return chunks
}

// FuzzSnapshotHeaderUnmarshalCrash feeds arbitrary bytes to the snapshot
// header decoder. It must never panic, must refuse input shorter than
// SnapshotHeaderSize, and a decoded header must re-encode to exactly the bytes
// it was decoded from.
func FuzzSnapshotHeaderUnmarshalCrash(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add(make([]byte, SnapshotHeaderSize-1))
	f.Add(make([]byte, SnapshotHeaderSize))
	f.Add(bytes.Repeat([]byte{0xFF}, SnapshotHeaderSize+8))
	hdr := seedSnapshotHeader()
	valid := make([]byte, SnapshotHeaderSize)
	if _, err := hdr.MarshalTo(valid); err != nil {
		f.Fatalf("seed MarshalTo: %v", err)
	}
	f.Add(valid)
	f.Add(append(append([]byte{}, valid...), 0xAA, 0xBB))

	f.Fuzz(func(t *testing.T, data []byte) {
		var h SnapshotHeader
		n, err := h.UnmarshalFrom(data)
		if len(data) < SnapshotHeaderSize {
			if !errors.Is(err, ErrBufferTooSmall) || n != 0 {
				t.Fatalf("short input (%d bytes): n=%d err=%v, want 0 and ErrBufferTooSmall", len(data), n, err)
			}
			return
		}
		if err != nil || n != SnapshotHeaderSize {
			t.Fatalf("full input (%d bytes): n=%d err=%v", len(data), n, err)
		}
		out := make([]byte, SnapshotHeaderSize)
		if m, err := h.MarshalTo(out); err != nil || m != SnapshotHeaderSize {
			t.Fatalf("re-encode: n=%d err=%v", m, err)
		}
		if !bytes.Equal(out, data[:SnapshotHeaderSize]) {
			t.Fatalf("re-encode differs:\n got %x\nwant %x", out, data[:SnapshotHeaderSize])
		}
	})
}

// FuzzSnapshotHeaderRoundTrip encodes a header built from fuzzed fields and
// decodes it again: the decoded header must equal the input.
func FuzzSnapshotHeaderRoundTrip(f *testing.F) {
	h := seedSnapshotHeader()
	f.Add(h.ShardID, h.ReplicaID, h.Index, h.Term, h.ChunkCount, h.Epoch, h.From)
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0))
	m := ^uint64(0)
	f.Add(m, m, m, m, m, m, m)

	f.Fuzz(func(t *testing.T, shardID, replicaID, index, term, chunkCount, epoch, from uint64) {
		in := SnapshotHeader{
			ShardID: shardID, ReplicaID: replicaID, Index: index, Term: term,
			ChunkCount: chunkCount, Epoch: epoch, From: from,
		}
		short := make([]byte, SnapshotHeaderSize-1)
		if _, err := in.MarshalTo(short); !errors.Is(err, ErrBufferTooSmall) {
			t.Fatalf("MarshalTo short buffer: err=%v, want ErrBufferTooSmall", err)
		}
		buf := make([]byte, SnapshotHeaderSize)
		if n, err := in.MarshalTo(buf); err != nil || n != SnapshotHeaderSize {
			t.Fatalf("MarshalTo: n=%d err=%v", n, err)
		}
		var out SnapshotHeader
		if n, err := out.UnmarshalFrom(buf); err != nil || n != SnapshotHeaderSize {
			t.Fatalf("UnmarshalFrom: n=%d err=%v", n, err)
		}
		if out != in {
			t.Fatalf("round trip differs: got %+v want %+v", out, in)
		}
	})
}

// FuzzSnapshotChunkStreamCrash feeds arbitrary bytes, as the chunk stream that
// follows a snapshot header, to the receiver's chunk decoding loop (frame
// header, chunk unmarshal, chunk validation). It must never panic, must return
// every byte it reserved from the global and per-peer memory budgets, and may
// deliver a snapshot only when exactly the declared number of valid chunks
// arrived, each within the chunk data cap and consistent with the header.
func FuzzSnapshotChunkStreamCrash(f *testing.F) {
	hdr := seedSnapshotHeader()
	valid, err := encodeSnapshotChunkFrames(f, seedSnapshotChunks(hdr))
	if err != nil {
		f.Fatalf("seed frames: %v", err)
	}
	f.Add(uint8(hdr.ChunkCount), valid)
	f.Add(uint8(hdr.ChunkCount+1), valid)
	f.Add(uint8(1), valid)
	f.Add(uint8(0), valid)
	f.Add(uint8(2), valid[:len(valid)/2])
	f.Add(uint8(2), []byte{})
	f.Add(uint8(4), bytes.Repeat([]byte{0xFF}, 64))
	oversize := make([]byte, FrameHeaderSize)
	copy(oversize, valid[:FrameHeaderSize])
	oversize[4], oversize[5], oversize[6], oversize[7] = 0x7F, 0xFF, 0xFF, 0xFF
	f.Add(uint8(1), oversize)

	f.Fuzz(func(t *testing.T, chunkCount uint8, data []byte) {
		handler := newTestHandler()
		sr := newSnapshotReceiver(4, fuzzSnapshotMemory, 0, 0, handler)
		sr.maxPerPeerMemory = fuzzSnapshotMemory / 2
		h := seedSnapshotHeader()
		h.ChunkCount = uint64(chunkCount % 8)
		stopC := make(chan struct{})

		sr.receiveChunks(&testChunkReader{data: data}, &h, fuzzSnapshotPeer, PeerAuth{}, stopC)

		if got := sr.memoryBudget.Load(); got != fuzzSnapshotMemory {
			t.Fatalf("memory budget not restored: %d, want %d", got, fuzzSnapshotMemory)
		}
		sr.peerMu.Lock()
		leftover := len(sr.peerUse)
		sr.peerMu.Unlock()
		if leftover != 0 {
			t.Fatalf("per-peer reservations left behind: %d peers", leftover)
		}

		snaps := handler.getSnapshots()
		if len(snaps) > 1 {
			t.Fatalf("one stream delivered %d snapshots", len(snaps))
		}
		if len(snaps) == 0 {
			return
		}
		if uint64(len(snaps[0])) != h.ChunkCount || h.ChunkCount == 0 {
			t.Fatalf("delivered %d chunks, header declared %d", len(snaps[0]), h.ChunkCount)
		}
		var total int
		for i := range snaps[0] {
			c := &snaps[0][i]
			total += len(c.Data)
			if len(c.Data) > maxSnapshotChunkDataLen || len(c.Filepath) > maxSnapshotFilepathLen {
				t.Fatalf("chunk %d past caps: data %d filepath %d", i, len(c.Data), len(c.Filepath))
			}
			if c.ShardID != h.ShardID || c.ReplicaID != h.ReplicaID || c.From != h.From ||
				c.Index != h.Index || c.Epoch != h.Epoch {
				t.Fatalf("chunk %d inconsistent with header: %+v vs %+v", i, c, h)
			}
		}
		if total > len(data) {
			t.Fatalf("delivered %d data bytes from a %d byte stream", total, len(data))
		}
	})
}

// FuzzSnapshotChunkRoundTrip builds a chunk from fuzzed fields, frames it with
// the encoder, and passes it through the receiver's chunk decoding loop: a
// chunk the receiver's validation admits must be delivered equal to the input.
func FuzzSnapshotChunkRoundTrip(f *testing.F) {
	hdr := seedSnapshotHeader()
	c := seedSnapshotChunks(hdr)[0]
	f.Add(c.ShardID, c.ReplicaID, c.From, c.Index, c.Term, c.ChunkID, c.FileSize,
		c.OnDiskIndex, c.Epoch, c.Filepath, c.Data)
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0),
		uint64(0), uint64(0), "", []byte{})
	m := ^uint64(0)
	f.Add(m, m, m, m, m, m, m, m, m, "relative/path", bytes.Repeat([]byte{0x5A}, 4096))

	f.Fuzz(func(t *testing.T, shardID, replicaID, from, index, term, chunkID, fileSize,
		onDiskIndex, epoch uint64, filepath string, data []byte,
	) {
		in := proto.SnapshotChunk{
			BinVer:      proto.WireVersion,
			ShardID:     shardID,
			ReplicaID:   replicaID,
			From:        from,
			Index:       index,
			Term:        term,
			ChunkID:     chunkID,
			ChunkCount:  1,
			ChunkSize:   uint64(len(data)),
			FileSize:    fileSize,
			Filepath:    filepath,
			OnDiskIndex: onDiskIndex,
			Epoch:       epoch,
			Data:        data,
		}
		h := SnapshotHeader{
			ShardID: shardID, ReplicaID: replicaID, Index: index, Term: term,
			ChunkCount: 1, Epoch: epoch, From: from,
		}
		handler := newTestHandler()
		sr := newSnapshotReceiver(4, 1<<30, 0, 0, handler)
		if err := sr.validateChunk(&in, &h); err != nil {
			t.Skipf("receiver refuses this chunk by policy: %v", err)
		}
		frames, err := encodeSnapshotChunkFrames(t, []proto.SnapshotChunk{in})
		if err != nil {
			t.Skipf("encoder refuses this chunk: %v", err)
		}

		sr.receiveChunks(&testChunkReader{data: frames}, &h, fuzzSnapshotPeer, PeerAuth{}, make(chan struct{}))

		snaps := handler.getSnapshots()
		if len(snaps) != 1 || len(snaps[0]) != 1 {
			t.Fatalf("valid chunk not delivered: %d snapshots", len(snaps))
		}
		got := snaps[0][0]
		if !bytes.Equal(got.Data, in.Data) {
			t.Fatalf("data differs: got %d bytes want %d", len(got.Data), len(in.Data))
		}
		if !snapshotChunksEqual(&got, &in) {
			t.Fatalf("round trip differs:\n got %+v\nwant %+v", got, in)
		}
		if b := sr.memoryBudget.Load(); b != 1<<30 {
			t.Fatalf("memory budget not restored: %d", b)
		}
	})
}

// snapshotChunksEqual compares the scalar fields of two chunks; the caller
// compares Data, and the round-trip target leaves Membership and FileInfo zero.
func snapshotChunksEqual(a, b *proto.SnapshotChunk) bool {
	return a.ShardID == b.ShardID && a.ReplicaID == b.ReplicaID && a.From == b.From &&
		a.Index == b.Index && a.Term == b.Term && a.ChunkID == b.ChunkID &&
		a.ChunkCount == b.ChunkCount && a.ChunkSize == b.ChunkSize && a.FileSize == b.FileSize &&
		a.Filepath == b.Filepath && a.FileChunkID == b.FileChunkID &&
		a.FileChunkCount == b.FileChunkCount && a.HasFileInfo == b.HasFileInfo &&
		a.OnDiskIndex == b.OnDiskIndex && a.DeploymentID == b.DeploymentID &&
		a.BinVer == b.BinVer && a.Epoch == b.Epoch
}
