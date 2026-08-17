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
//	[OnDiskIndex:8][Epoch:8][Compression:8]
//
// # The removed Imported field
//
// Imported rode this layout and was read by NOTHING outside marshal, unmarshal,
// IsEmpty and Reset. It had exactly one writer, and that writer set it from the
// save request's `exported` flag: it recorded "this save is an EXPORT", which is
// close to the OPPOSITE of what the field's name and doc comment claimed. A
// field that is written wrongly and read nowhere is worse than an absent one,
// because the next reader to want the fact takes it and gets the wrong answer.
//
// If a wire-level statement that a snapshot was imported is wanted later — an
// imported snapshot is one whose membership may have been rewritten by a
// disaster-recovery override, which is genuinely worth knowing — it must arrive
// with its consumer in the same change, and with a logdb.Snapshot counterpart so
// it survives a restart.
//
// # The removed Type field
//
// Type claimed to name the state machine kind (regular/concurrent/on-disk) and
// had NO writer anywhere in the repository: it was zero on every snapshot ever
// produced, while both MarshalTo and UnmarshalFrom spent a comparison
// validating it against maxStateMachineType — a range check on a value no code
// path could set. The kind of state machine a shard runs is a property of the
// LOCAL process (it is chosen by the StateMachineFactory the host was
// constructed with, and proto.Bootstrap already records it durably), not of an
// artifact streamed from a peer, so there is no reader for it to grow either.
// Removing it orphaned maxStateMachineType in constants.go and
// InvalidSnapshotTypeError in errors.go, which went with it.
//
// # The removed Witness field
//
// Witness WAS written — sendSnapshot set it in the same three-line branch that
// cleared Filepath and zeroed FileSize when the target was a witness — and read
// by nothing. It is not merely unused but redundant: an empty Filepath is
// ALREADY the repository-wide discriminator for a metadata-only snapshot
// (forEachSnapshotChunk branches on it to emit the single empty chunk, and
// recoverLogReader documents that leaving Filepath empty is what turns a
// replica into a state eraser), so a second, independently-settable statement
// of the same fact could only ever be a way for the two to disagree.
//
// Empty Filepath is deliberately the WIDER predicate: it also covers a state
// machine that genuinely saved nothing. That is the right shape here, because
// every consumer acts on "is there payload to stream", not on "was the peer a
// witness". The one question Witness could have answered that Filepath cannot —
// a stripped snapshot arriving at a replica that does not consider itself a
// witness — is a role disagreement, and the raft role-mismatch check
// (checkSelfRole) owns that, working from replicated membership rather than
// from a flag the sender chooses.
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
	// OnDiskIndex is the index tracked for on-disk state machines.
	OnDiskIndex uint64
	// Epoch is the barrier encryption epoch used when this snapshot was
	// saved. Zero indicates no encryption (plaintext snapshot). This field
	// must be propagated through the snapshot transfer pipeline so the
	// receiver can decrypt barrier-encrypted snapshot data.
	Epoch uint64
	// Compression is the algorithm the snapshot data file was written with,
	// as a config.CompressionType value widened to uint64 (this package
	// cannot import config — config imports it).
	//
	// It is a property of what the WRITER did, not of the reader's own
	// configuration, so a recovery must take it from the artifact's record.
	// Without the field on this type, every snapshot record built by
	// converting a proto.Snapshot recorded NoCompression whatever the data
	// actually was — and a compressed artifact read as uncompressed is fed to
	// the state machine as garbage.
	Compression uint64
}

// Size returns the total marshaled size of the snapshot in bytes.
func (s *Snapshot) Size() int {
	// 4 fixed uint64 fields at start: ShardID, ReplicaID, Index, Term
	// Membership (variable)
	// Filepath: 4-byte len + data
	// FileSize: 8
	// OnDiskIndex, Epoch, Compression: 3 * 8
	return 4*8 + s.Membership.Size() +
		(4 + len(s.Filepath)) + 8 +
		3*8
}

// MarshalTo writes the snapshot metadata into buf and returns bytes written.
// buf must be at least Size() bytes. Returns ErrBufferTooSmall if buf is too
// short, ErrFilepathTooLong if Filepath exceeds MaxFilepathLen, or whatever
// the embedded Membership's own write-side limits report — the read side's
// checks applied on write, so this type cannot emit metadata it refuses to
// read back.
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
