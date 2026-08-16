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

// Snapshot represents snapshot metadata for a Raft shard. It contains
// fixed fields for identification and state, an inline Membership, and
// one variable-length field, Filepath. The wire format is:
//
//	[ShardID:8][ReplicaID:8][Index:8][Term:8][Membership:var]
//	[Filepath:len+data][FileSize:8]
//	[Type:8][Imported:8][OnDiskIndex:8][Witness:8][Epoch:8]
//
// # The removed Checksum field
//
// This type carried a `Checksum []byte` through an earlier pre-release layout.
// It was decoded,
// bounded, bulk-copied out of the pooled receive buffer and propagated through
// the whole transfer pipeline — and it was NEVER computed and NEVER verified,
// anywhere in the repository. Nothing wrote it, so it was empty on every real
// snapshot, and the receive path paid one allocation and two copies per message
// to keep an empty slice alive.
//
// It was removed rather than implemented, at the format freeze, for one reason
// that outweighs the cost of a future format break if a real one is wanted: a
// field named Checksum that rides the wire and is never checked reads as an
// integrity control to anyone auditing this format, and an integrity control
// that does not exist is worse than an absent one. Populating it properly is
// also not a codec change — the value has to be computed over the FINAL artifact
// bytes (post-compression, post-encryption), stamped where the artifact is
// produced, and verified in three separate consumers (recovery, streamed
// receive, ImportSnapshot). Wiring one of those and not the others reproduces
// exactly the state being removed, so it belongs in its own change with its own
// tests, not as a side effect of a layout revision.
//
// What snapshot integrity rests on meanwhile, stated plainly: TLS in transit,
// and barrier AEAD at rest. A shard configured with a PLAINTEXT barrier
// (seal.None) therefore has NO at-rest integrity check on its snapshot
// artifacts — silent bit rot or a truncated artifact is recovered into the state
// machine. That was equally true while the field existed; removing it makes the
// gap visible instead of apparently covered.
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
	// Type identifies the state machine type.
	Type uint64
	// Imported indicates whether this snapshot was imported externally.
	Imported bool
	// OnDiskIndex is the index tracked for on-disk state machines.
	OnDiskIndex uint64
	// Witness indicates whether this is a witness replica snapshot.
	Witness bool
	// Epoch is the barrier encryption epoch used when this snapshot was
	// saved. Zero indicates no encryption (plaintext snapshot). This field
	// must be propagated through the snapshot transfer pipeline so the
	// receiver can decrypt barrier-encrypted snapshot data.
	Epoch uint64
}

// Size returns the total marshaled size of the snapshot in bytes.
func (s *Snapshot) Size() int {
	// 4 fixed uint64 fields at start: ShardID, ReplicaID, Index, Term
	// Membership (variable)
	// Filepath: 4-byte len + data
	// FileSize: 8
	// Type, Imported, OnDiskIndex, Witness, Epoch: 5 * 8
	return 4*8 + s.Membership.Size() +
		(4 + len(s.Filepath)) + 8 +
		5*8
}

// MarshalTo writes the snapshot metadata into buf and returns bytes written.
// buf must be at least Size() bytes. Returns ErrBufferTooSmall if buf is too
// short, ErrFilepathTooLong if Filepath exceeds MaxFilepathLen,
// InvalidSnapshotTypeError for a Type UnmarshalFrom would reject, or whatever
// the embedded Membership's own write-side limits report — the read side's
// checks applied on write, so this type cannot emit metadata it refuses to
// read back.
func (s *Snapshot) MarshalTo(buf []byte) (int, error) {
	if len(s.Filepath) > MaxFilepathLen {
		return 0, ErrFilepathTooLong
	}
	if s.Type > maxStateMachineType {
		return 0, &InvalidSnapshotTypeError{Type: s.Type}
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
	putUint64(buf[offset:], s.Type)
	offset += 8
	putBool(buf[offset:], s.Imported)
	offset += 8
	putUint64(buf[offset:], s.OnDiskIndex)
	offset += 8
	putBool(buf[offset:], s.Witness)
	offset += 8
	putUint64(buf[offset:], s.Epoch)
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
	// 5 trailing fixed fields: Type, Imported, OnDiskIndex, Witness, Epoch
	if len(buf) < offset+5*8 {
		return 0, ErrBufferTooSmall
	}
	s.Type = getUint64(buf[offset:])
	if s.Type > maxStateMachineType {
		return 0, &InvalidSnapshotTypeError{Type: s.Type}
	}
	offset += 8
	s.Imported = getBool(buf[offset:])
	offset += 8
	s.OnDiskIndex = getUint64(buf[offset:])
	offset += 8
	s.Witness = getBool(buf[offset:])
	offset += 8
	s.Epoch = getUint64(buf[offset:])
	offset += 8
	return offset, nil
}

// IsEmpty returns true if the snapshot has no meaningful content
// (zero Index and zero Term).
//
// The log POSITION is the whole test, and that is a deliberate semantic the rest
// of the engine depends on: a snapshot is identified by the (Index, Term) it
// covers, and one that covers nothing is not a snapshot regardless of what else
// happens to be filled in. Message.MarshalTo uses it as the presence flag for
// the embedded snapshot, so it also decides whether those other fields cross the
// wire at all — see hasContentBeyondPosition, which exists so that combination
// is refused rather than silently dropped.
func (s *Snapshot) IsEmpty() bool {
	return s.Index == 0 && s.Term == 0
}

// hasContentBeyondPosition reports whether any field other than Index and Term
// is populated. Together with IsEmpty it identifies the one contradictory shape
// this type can hold: content with no log position, which the containing
// Message cannot encode and would otherwise discard in silence.
func (s *Snapshot) hasContentBeyondPosition() bool {
	return s.ShardID != 0 || s.ReplicaID != 0 || s.Filepath != "" ||
		s.FileSize != 0 || s.Type != 0 || s.Imported ||
		s.OnDiskIndex != 0 || s.Witness || s.Epoch != 0 ||
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
	s.Type = 0
	s.Imported = false
	s.OnDiskIndex = 0
	s.Witness = false
	s.Epoch = 0
}
