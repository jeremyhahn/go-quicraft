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

// Snapshot is a shard's snapshot metadata: [ShardID:8][ReplicaID:8][Index:8][Term:8][Membership:var][Filepath:len+data][FileSize:8][OnDiskIndex:8][Epoch:8][Compression:8]. An empty Filepath means metadata only; integrity rests on TLS in transit and barrier AEAD at rest.
type Snapshot struct {
	// ShardID identifies the Raft shard this snapshot belongs to.
	ShardID uint64
	// ReplicaID identifies the replica within the shard.
	ReplicaID uint64
	// Index is the last included log index in this snapshot.
	Index uint64
	// Term is the term of the last included log entry.
	Term uint64
	// Membership is the cluster membership at the snapshot index.
	Membership Membership
	// Filepath is the path to the snapshot data file on disk.
	Filepath string
	// FileSize is the size of the snapshot data file in bytes.
	FileSize uint64
	// OnDiskIndex is the index tracked for on-disk state machines.
	OnDiskIndex uint64
	// Epoch is the barrier encryption epoch the snapshot was saved under; zero means plaintext.
	Epoch uint64
	// Compression is the config.CompressionType the data file was written with, widened to uint64; recovery reads it from the artifact.
	Compression uint64
}

// Size returns the total marshaled size of the snapshot in bytes.
func (s *Snapshot) Size() int {
	// ShardID, ReplicaID, Index, Term; Membership; Filepath (4-byte len + data); FileSize; OnDiskIndex, Epoch, Compression.
	return 4*8 + s.Membership.Size() +
		(4 + len(s.Filepath)) + 8 +
		3*8
}

// MarshalTo writes the snapshot into buf (at least Size() bytes) and returns bytes written; it applies the read-side limits (ErrBufferTooSmall, ErrFilepathTooLong, Membership's).
func (s *Snapshot) MarshalTo(buf []byte) (int, error) {
	if len(s.Filepath) > MaxFilepathLen {
		return 0, ErrFilepathTooLong
	}
	required := s.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], s.ShardID)
	putUint64(buf[8:], s.ReplicaID)
	putUint64(buf[16:], s.Index)
	putUint64(buf[24:], s.Term)
	offset := 32
	n, err := s.Membership.MarshalTo(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	offset += putString(buf[offset:], s.Filepath)
	putUint64(buf[offset:], s.FileSize)
	offset += 8
	putUint64(buf[offset:], s.OnDiskIndex)
	offset += 8
	putUint64(buf[offset:], s.Epoch)
	offset += 8
	putUint64(buf[offset:], s.Compression)
	offset += 8
	return offset, nil
}

// UnmarshalFrom reads the snapshot metadata from buf and returns bytes
// consumed. Returns ErrBufferTooSmall on truncated input.
func (s *Snapshot) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 32 {
		return 0, ErrBufferTooSmall
	}
	s.ShardID = getUint64(buf[0:])
	s.ReplicaID = getUint64(buf[8:])
	s.Index = getUint64(buf[16:])
	s.Term = getUint64(buf[24:])
	offset := 32
	n, err := s.Membership.UnmarshalFrom(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	var fp string
	fp, n, err = getString(buf[offset:], MaxFilepathLen, ErrFilepathTooLong)
	if err != nil {
		return 0, err
	}
	s.Filepath = fp
	offset += n
	// FileSize
	if len(buf) < offset+8 {
		return 0, ErrBufferTooSmall
	}
	s.FileSize = getUint64(buf[offset:])
	offset += 8
	// 3 trailing fixed fields: OnDiskIndex, Epoch, Compression
	if len(buf) < offset+3*8 {
		return 0, ErrBufferTooSmall
	}
	s.OnDiskIndex = getUint64(buf[offset:])
	offset += 8
	s.Epoch = getUint64(buf[offset:])
	offset += 8
	s.Compression = getUint64(buf[offset:])
	offset += 8
	return offset, nil
}

// IsEmpty reports whether Index and Term are both zero; Message.MarshalTo uses it as the snapshot presence flag.
func (s *Snapshot) IsEmpty() bool {
	return s.Index == 0 && s.Term == 0
}

// CheckBounds returns ErrLogIndexOutOfRange or ErrTermOutOfRange when Index or Term is at or above MaxLogIndex or MaxTerm, else nil.
func (s *Snapshot) CheckBounds() error {
	if s.Index >= MaxLogIndex {
		return ErrLogIndexOutOfRange
	}
	if s.Term >= MaxTerm {
		return ErrTermOutOfRange
	}
	return nil
}

// hasContentBeyondPosition reports whether any field besides Index and Term is set.
func (s *Snapshot) hasContentBeyondPosition() bool {
	return s.ShardID != 0 || s.ReplicaID != 0 || s.Filepath != "" ||
		s.FileSize != 0 || s.OnDiskIndex != 0 ||
		s.Epoch != 0 || s.Compression != 0 ||
		!s.Membership.isZero()
}

// Reset clears all fields for reuse.
func (s *Snapshot) Reset() {
	s.ShardID = 0
	s.ReplicaID = 0
	s.Index = 0
	s.Term = 0
	s.Membership.Reset()
	s.Filepath = ""
	s.FileSize = 0
	s.OnDiskIndex = 0
	s.Epoch = 0
	s.Compression = 0
}
