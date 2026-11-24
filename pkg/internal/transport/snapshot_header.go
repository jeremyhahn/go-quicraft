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

import "encoding/binary"

// SnapshotHeaderSize is the wire size of SnapshotHeader: 7 uint64 fields.
const SnapshotHeaderSize = 7 * 8 // 56 bytes

// SnapshotHeader is the header at the start of a snapshot stream, letting the receiver validate and allocate before chunks arrive.
type SnapshotHeader struct {
	// ShardID identifies the Raft shard.
	ShardID uint64
	// ReplicaID identifies the target replica.
	ReplicaID uint64
	// Index is the snapshot log index.
	Index uint64
	// Term is the snapshot term.
	Term uint64
	// ChunkCount is the total number of chunks in this snapshot.
	ChunkCount uint64
	// Epoch is the barrier encryption epoch of the snapshot; zero means plaintext.
	Epoch uint64
	// From is the sending replica, authorized before any slot or memory is taken; every chunk must match its From and ReplicaID.
	From uint64
}

// MarshalTo writes the snapshot header into buf and returns the number
// of bytes written. Returns ErrBufferTooSmall if buf has fewer than
// SnapshotHeaderSize bytes.
func (h *SnapshotHeader) MarshalTo(buf []byte) (int, error) {
	if len(buf) < SnapshotHeaderSize {
		return 0, ErrBufferTooSmall
	}
	binary.BigEndian.PutUint64(buf[0:8], h.ShardID)
	binary.BigEndian.PutUint64(buf[8:16], h.ReplicaID)
	binary.BigEndian.PutUint64(buf[16:24], h.Index)
	binary.BigEndian.PutUint64(buf[24:32], h.Term)
	binary.BigEndian.PutUint64(buf[32:40], h.ChunkCount)
	binary.BigEndian.PutUint64(buf[40:48], h.Epoch)
	binary.BigEndian.PutUint64(buf[48:56], h.From)
	return SnapshotHeaderSize, nil
}

// UnmarshalFrom reads the snapshot header from buf and returns the number
// of bytes consumed. Returns ErrBufferTooSmall if buf has fewer than
// SnapshotHeaderSize bytes.
func (h *SnapshotHeader) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < SnapshotHeaderSize {
		return 0, ErrBufferTooSmall
	}
	h.ShardID = binary.BigEndian.Uint64(buf[0:8])
	h.ReplicaID = binary.BigEndian.Uint64(buf[8:16])
	h.Index = binary.BigEndian.Uint64(buf[16:24])
	h.Term = binary.BigEndian.Uint64(buf[24:32])
	h.ChunkCount = binary.BigEndian.Uint64(buf[32:40])
	h.Epoch = binary.BigEndian.Uint64(buf[40:48])
	h.From = binary.BigEndian.Uint64(buf[48:56])
	return SnapshotHeaderSize, nil
}
