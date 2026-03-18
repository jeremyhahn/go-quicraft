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

// SnapshotChunk is used for streaming snapshots between nodes via
// transport. The wire format has 16 fixed uint64 fields plus HasFileInfo
// (bool as uint64), followed by variable-length fields:
//
//	[ShardID:8][ReplicaID:8][From:8][Index:8][Term:8]
//	[ChunkID:8][ChunkCount:8][ChunkSize:8][FileSize:8]
//	[Filepath:len+data][FileChunkID:8][FileChunkCount:8]
//	[HasFileInfo:8][FileInfo:var (if HasFileInfo)]
//	[Membership:var][OnDiskIndex:8][DeploymentID:8][BinVer:8]
//	[Epoch:8][Data:len+data]
type SnapshotChunk struct {
	// ShardID identifies the Raft shard.
	ShardID uint64
	// ReplicaID identifies the target replica.
	ReplicaID uint64
	// From is the sender replica ID.
	From uint64
	// Index is the snapshot log index.
	Index uint64
	// Term is the snapshot term.
	Term uint64
	// ChunkID is the chunk sequence number (0-based).
	ChunkID uint64
	// ChunkCount is the total number of chunks.
	ChunkCount uint64
	// ChunkSize is the byte size of this chunk's data.
	ChunkSize uint64
	// FileSize is the total snapshot file size.
	FileSize uint64
	// Filepath is the destination path for the snapshot.
	Filepath string
	// FileChunkID is the chunk ID within the current file.
	FileChunkID uint64
	// FileChunkCount is the total chunks in the current file.
	FileChunkCount uint64
	// HasFileInfo indicates whether FileInfo is present.
	HasFileInfo bool
	// FileInfo carries metadata for external snapshot files.
	FileInfo SnapshotFile
	// Membership is the cluster membership at the snapshot index.
	Membership Membership
	// OnDiskIndex is the on-disk state machine index.
	OnDiskIndex uint64
	// DeploymentID identifies the deployment.
	DeploymentID uint64
	// BinVer is the wire format version.
	BinVer uint64
	// Epoch is the barrier encryption epoch used when the snapshot was
	// saved. Zero indicates no encryption (plaintext snapshot). The
	// receiver uses this to decrypt barrier-encrypted snapshot data.
	Epoch uint64
	// Data is the chunk payload.
	Data []byte
}

// Size returns the total marshaled size of the snapshot chunk in bytes.
func (sc *SnapshotChunk) Size() int {
	// 9 fixed uint64 fields: ShardID through FileSize
	sz := 9 * 8
	// Filepath: length-prefixed string
	sz += 4 + len(sc.Filepath)
	// FileChunkID + FileChunkCount
	sz += 2 * 8
	// HasFileInfo
	sz += 8
	if sc.HasFileInfo {
		sz += sc.FileInfo.Size()
	}
	// Membership
	sz += sc.Membership.Size()
	// OnDiskIndex + DeploymentID + BinVer + Epoch
	sz += 4 * 8
	// Data: length-prefixed bytes
	sz += 4 + len(sc.Data)
	return sz
}

// MarshalTo writes the snapshot chunk into buf and returns bytes written.
func (sc *SnapshotChunk) MarshalTo(buf []byte) (int, error) {
	required := sc.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], sc.ShardID)
	putUint64(buf[8:], sc.ReplicaID)
	putUint64(buf[16:], sc.From)
	putUint64(buf[24:], sc.Index)
	putUint64(buf[32:], sc.Term)
	putUint64(buf[40:], sc.ChunkID)
	putUint64(buf[48:], sc.ChunkCount)
	putUint64(buf[56:], sc.ChunkSize)
	putUint64(buf[64:], sc.FileSize)
	offset := 72
	offset += putString(buf[offset:], sc.Filepath)
	putUint64(buf[offset:], sc.FileChunkID)
	offset += 8
	putUint64(buf[offset:], sc.FileChunkCount)
	offset += 8
	putBool(buf[offset:], sc.HasFileInfo)
	offset += 8
	if sc.HasFileInfo {
		n, err := sc.FileInfo.MarshalTo(buf[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	n, err := sc.Membership.MarshalTo(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	putUint64(buf[offset:], sc.OnDiskIndex)
	offset += 8
	putUint64(buf[offset:], sc.DeploymentID)
	offset += 8
	putUint64(buf[offset:], sc.BinVer)
	offset += 8
	putUint64(buf[offset:], sc.Epoch)
	offset += 8
	offset += putBytes(buf[offset:], sc.Data)
	return offset, nil
}

// UnmarshalFrom reads the snapshot chunk from buf and returns bytes consumed.
func (sc *SnapshotChunk) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 72 { // minimum: 9 uint64 fields
		return 0, ErrBufferTooSmall
	}
	sc.ShardID = getUint64(buf[0:])
	sc.ReplicaID = getUint64(buf[8:])
	sc.From = getUint64(buf[16:])
	sc.Index = getUint64(buf[24:])
	sc.Term = getUint64(buf[32:])
	sc.ChunkID = getUint64(buf[40:])
	sc.ChunkCount = getUint64(buf[48:])
	sc.ChunkSize = getUint64(buf[56:])
	sc.FileSize = getUint64(buf[64:])
	offset := 72
	fp, n, err := getString(buf[offset:], MaxFilepathLen)
	if err != nil {
		return 0, err
	}
	sc.Filepath = fp
	offset += n
	if len(buf) < offset+24 { // FileChunkID + FileChunkCount + HasFileInfo
		return 0, ErrBufferTooSmall
	}
	sc.FileChunkID = getUint64(buf[offset:])
	offset += 8
	sc.FileChunkCount = getUint64(buf[offset:])
	offset += 8
	sc.HasFileInfo = getBool(buf[offset:])
	offset += 8
	if sc.HasFileInfo {
		n, err = sc.FileInfo.UnmarshalFrom(buf[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	} else {
		sc.FileInfo.Reset()
	}
	n, err = sc.Membership.UnmarshalFrom(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	if len(buf) < offset+32 { // OnDiskIndex + DeploymentID + BinVer + Epoch
		return 0, ErrBufferTooSmall
	}
	sc.OnDiskIndex = getUint64(buf[offset:])
	offset += 8
	sc.DeploymentID = getUint64(buf[offset:])
	offset += 8
	sc.BinVer = getUint64(buf[offset:])
	offset += 8
	sc.Epoch = getUint64(buf[offset:])
	offset += 8
	data, n, err := getBytes(buf[offset:], MaxSnapshotChunkDataLen)
	if err != nil {
		return 0, err
	}
	sc.Data = data
	offset += n
	return offset, nil
}

// Reset clears all fields for reuse.
func (sc *SnapshotChunk) Reset() {
	sc.ShardID = 0
	sc.ReplicaID = 0
	sc.From = 0
	sc.Index = 0
	sc.Term = 0
	sc.ChunkID = 0
	sc.ChunkCount = 0
	sc.ChunkSize = 0
	sc.FileSize = 0
	sc.Filepath = ""
	sc.FileChunkID = 0
	sc.FileChunkCount = 0
	sc.HasFileInfo = false
	sc.FileInfo.Reset()
	sc.Membership.Reset()
	sc.OnDiskIndex = 0
	sc.DeploymentID = 0
	sc.BinVer = 0
	sc.Epoch = 0
	sc.Data = nil
}
