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
	"encoding/binary"
	"errors"
	"runtime"
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

// denseSnapshotMembership returns a Membership whose five string maps each
// hold n zero-length entries: the shape that costs the decoder the most heap
// per wire byte (pkg/proto getMap; MaxDecodeMemoryFactor).
func denseSnapshotMembership(n int) proto.Membership {
	mk := func() map[uint64]string {
		m := make(map[uint64]string, n)
		for i := 0; i < n; i++ {
			m[uint64(i+1)] = ""
		}
		return m
	}
	return proto.Membership{
		Addresses:         mk(),
		Observers:         mk(),
		Witnesses:         mk(),
		Identities:        mk(),
		RemovedIdentities: mk(),
	}
}

// denseMembershipEntries is the per-map entry count of the membership-dense
// seed; its frame fits the fuzz receiver's per-peer budget.
const denseMembershipEntries = 897

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
		// The header is fixed-size: decoding it, valid or not, allocates
		// nothing.
		if allocs := testing.AllocsPerRun(1, func() {
			var h SnapshotHeader
			_, _ = h.UnmarshalFrom(data)
		}); allocs != 0 {
			t.Fatalf("UnmarshalFrom of %d bytes allocated %v times, want 0", len(data), allocs)
		}
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

	// Frames rejected before delivery. Each declares its length in a frame
	// header copied from a valid frame, with no body or a short one.
	frameWithLength := func(length uint32, body []byte) []byte {
		fr := make([]byte, FrameHeaderSize, FrameHeaderSize+len(body))
		copy(fr, valid[:FrameHeaderSize])
		binary.BigEndian.PutUint32(fr[4:8], length)
		return append(fr, body...)
	}
	// Past the per-frame cap: refused by the frame header check.
	f.Add(uint8(1), frameWithLength(MaxFrameSize+1, nil))
	// Within the per-frame cap but past the per-peer budget
	// (fuzzSnapshotMemory/2): refused by reserveChunkMemory before the frame
	// buffer exists. The MaxFrameSize one is what an allocation made ahead
	// of the budget check would trip on.
	f.Add(uint8(1), frameWithLength(MaxFrameSize, []byte{1, 2, 3}))
	f.Add(uint8(2), frameWithLength(fuzzSnapshotMemory/2+1, nil))
	// A valid frame followed by a truncated one.
	firstLen := FrameHeaderSize + int(binary.BigEndian.Uint32(valid[4:8]))
	truncated := append([]byte{}, valid[:firstLen]...)
	truncated = append(truncated, valid[firstLen:firstLen+FrameHeaderSize+1]...)
	f.Add(uint8(2), truncated)
	// A valid one-chunk stream whose Membership holds five dense maps of
	// zero-length entries: within the budget, and decoded into far more heap
	// than its wire length.
	denseHdr := hdr
	denseHdr.ChunkCount = 1
	denseChunks := seedSnapshotChunks(denseHdr)
	denseChunks[0].Membership = denseSnapshotMembership(denseMembershipEntries)
	dense, err := encodeSnapshotChunkFrames(f, denseChunks)
	if err != nil {
		f.Fatalf("dense membership seed: %v", err)
	}
	if int64(len(dense))+snapshotChunkStructBytes > fuzzSnapshotMemory/2 {
		f.Fatalf("dense membership seed of %d bytes exceeds the per-peer budget", len(dense))
	}
	f.Logf("dense membership seed: n=%d, %d wire bytes", denseMembershipEntries, len(dense))
	f.Add(uint8(1), dense)
	// The same frame cut 8 bytes short, its length field to match: admitted,
	// then the decode fails past Membership, after the maps are built.
	denseShort := append([]byte{}, dense[:len(dense)-8]...)
	binary.BigEndian.PutUint32(denseShort[4:8], binary.BigEndian.Uint32(dense[4:8])-8)
	f.Add(uint8(1), denseShort)

	f.Fuzz(func(t *testing.T, chunkCount uint8, data []byte) {
		handler := newTestHandler()
		sr := newSnapshotReceiver(4, fuzzSnapshotMemory, 0, 0, handler)
		sr.maxPerPeerMemory = fuzzSnapshotMemory / 2
		h := seedSnapshotHeader()
		h.ChunkCount = uint64(chunkCount % 8)
		stopC := make(chan struct{})

		// admitted is the sum of the frame lengths receiveChunks reserves
		// against the budget, computed by walking a copy of the stream with
		// the receiver's own functions. A frame buffer is allocated only
		// after reserveChunkMemory admits its length (snapshot_recv.go), so
		// a buffer allocated before that check, or for a frame the budget
		// refuses, lands past allocBound.
		admitted, meta := fuzzAdmittedFrameBytes(sr, &h, data)
		// k is 1: SnapshotChunk.UnmarshalFromWindow aliases Data into the
		// frame buffer (pkg/proto getBytes returns a sub-slice) rather than
		// copying it, so Data costs nothing beyond its frame buffer. The rest
		// of a decoded frame, meta, is not aliased: Membership maps and
		// copied strings are built from it, and proto.MaxDecodeMemoryFactor
		// is the decoder's stated bound on that expansion per wire byte, so
		// meta is charged at that factor. allocSlack covers the chunk
		// slice, the timer, errors and log records, and stays below every
		// rejected seed frame's length.
		const k, allocSlack = 1, 64 << 10
		allocBound := uint64(k*admitted + proto.MaxDecodeMemoryFactor*meta + allocSlack)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		sr.receiveChunks(&testChunkReader{data: data}, &h, fuzzSnapshotPeer, PeerAuth{}, stopC)
		runtime.ReadMemStats(&after)
		if delta := after.TotalAlloc - before.TotalAlloc; delta > allocBound {
			if admitted == 0 {
				t.Fatalf("receiveChunks allocated %d bytes from a %d byte stream refused before any frame buffer was admitted, bound %d",
					delta, len(data), allocBound)
			}
			t.Fatalf("receiveChunks allocated %d bytes from a %d byte stream with %d frame bytes admitted, bound %d",
				delta, len(data), admitted, allocBound)
		}

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

// fuzzAdmittedFrameBytes returns the sum of the frame lengths receiveChunks
// reserves against its memory budget for data, walking a copy of the stream
// with the receiver's own header, budget, decode and validation steps. A
// frame the budget refuses is refused before its buffer exists, so it is not
// counted; a frame that is admitted and then fails to read, decode or
// validate is counted and ends the walk.
//
// meta is, for each admitted frame that decodes, its length minus
// len(chunk.Data): the bytes the decoder expands rather than aliases. A
// frame admitted and then failing to read or decode adds to admitted only.
func fuzzAdmittedFrameBytes(sr *snapshotReceiver, h *SnapshotHeader, data []byte) (admitted, meta int) {
	r := bytes.NewReader(data)
	var charged int64
	for i := uint64(0); i < h.ChunkCount; i++ {
		length, _, err := readFrameHeaderWindow(r, sr.wire)
		if err != nil {
			break
		}
		if length == 0 {
			continue
		}
		charge := int64(length) + snapshotChunkStructBytes
		if charged+charge > sr.maxPerPeerMemory || charged+charge > fuzzSnapshotMemory {
			break
		}
		charged += charge
		admitted += int(length)
		if r.Len() < int(length) {
			break
		}
		body := make([]byte, length)
		_, _ = r.Read(body)
		var chunk proto.SnapshotChunk
		if _, err := chunk.UnmarshalFromWindow(body, sr.wire.Proto); err != nil {
			break
		}
		// The frame decoded: everything in it but Data (membership,
		// header fields, filepath) is what the decoder may expand.
		meta += int(length) - len(chunk.Data)
		if err := sr.validateChunk(&chunk, h); err != nil {
			break
		}
	}
	return admitted, meta
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
