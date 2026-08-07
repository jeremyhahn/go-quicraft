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

package engine

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// snapshot.meta is the record written beside every snapshot artifact. A
// snapshot directory MUST describe itself: it outlives its LogDB record (up to
// maxSnapshotsRetained directories are retained while only the newest is
// recorded), and the DEK purge floor is computed by SCANNING these directories
// and reading the Epoch field back as fact.
//
// # Layout
//
// The record is SELF-DESCRIBING. A 16-byte header precedes a body of
// little-endian uint64 fields:
//
//	off  size  field
//	  0     8  magic "QRSNAPMD"
//	  8     2  version (uint16, format version of the BODY)
//	 10     2  reserved (uint16, 0)
//	 12     4  body length (uint32, bytes of body that follow)
//	 16     8  Index
//	 24     8  Term
//	 32     8  ShardID
//	 40     8  ReplicaID
//	 48     8  CreatedAt (unix seconds)
//	 56     8  Epoch
//	 64     8  Compression
//
// # Evolution rule
//
// Body fields are APPEND-ONLY: an existing field never moves and never changes
// meaning. A new field is appended and the version is incremented. Because the
// body length is recorded, a reader can always tell a field that is ABSENT
// (written by an older version) from one that is CORRUPT (a truncated or
// foreign record) — the distinction the magic/version prefix exists to make.
// Changing the meaning of an existing field is NOT permitted under this rule;
// that requires a new magic.
//
// # Compatibility
//
// Two pre-magic ("v0") layouts were written by earlier builds and are still
// accepted verbatim, positionally, at their original offsets:
//
//	40 bytes  Index|Term|ShardID|ReplicaID|CreatedAt
//	          (written by the snapshot RECEIVE and IMPORT paths; carries NO
//	          Epoch, even though the artifact beside it may be encrypted)
//	48 bytes  ... |Epoch
//	56 bytes  ... |Epoch|Compression
//	          (both written by the snapshot SAVE path)
//
// The 40-byte layout is why "shorter record" cannot be read as "no epoch, so no
// purge floor": the receive path re-encrypted the artifact under the receiver's
// own barrier and recorded the epoch ONLY in LogDB, so such a directory can
// hold ciphertext at an epoch nothing on disk names. It is therefore reported
// as UNEVALUABLE (see snapshotEpochLegacy), never as epoch 0.
//
// Nothing rewrites an existing record: a v0 record stays v0 until its snapshot
// directory rolls off, and every new record is written at the current version.
const (
	// snapshotMetaMagic identifies a self-describing snapshot.meta record. It
	// is 8 bytes so that a v0 record can never be mistaken for one: a v0
	// record's first 8 bytes are its Index, and matching would require an
	// index of ~4.9e18.
	snapshotMetaMagic = "QRSNAPMD"

	// snapshotMetaVersion is the body version this binary WRITES.
	snapshotMetaVersion uint16 = 1

	// Header field offsets and the total header size.
	snapshotMetaMagicOff    = 0
	snapshotMetaVersionOff  = 8
	snapshotMetaReservedOff = 10
	snapshotMetaBodyLenOff  = 12
	snapshotMetaHeaderSize  = 16

	// Body field offsets, relative to the start of the body.
	metaBodyIndexOff       = 0
	metaBodyTermOff        = 8
	metaBodyShardIDOff     = 16
	metaBodyReplicaIDOff   = 24
	metaBodyCreatedAtOff   = 32
	metaBodyEpochOff       = 40
	metaBodyCompressionOff = 48

	// snapshotMetaBodySize is the body size at snapshotMetaVersion.
	snapshotMetaBodySize = 7 * 8

	// snapshotMetaSize is the total size of a record this binary writes.
	snapshotMetaSize = snapshotMetaHeaderSize + snapshotMetaBodySize

	// Accepted pre-magic (v0) record sizes. See the compatibility note above.
	legacyMetaSizeNoEpoch     = 5 * 8
	legacyMetaSizeEpoch       = 6 * 8
	legacyMetaSizeCompression = 7 * 8
)

// SnapshotMeta is the decoded content of a snapshot.meta record.
//
// The *Recorded flags distinguish a field the record actually CARRIED from one
// that was absent because an older version wrote it. A zero Epoch with
// EpochRecorded false means UNKNOWN, not plaintext — the two are opposite in
// consequence for DEK purging, so they must never be conflated.
type SnapshotMeta struct {
	Index       uint64
	Term        uint64
	ShardID     uint64
	ReplicaID   uint64
	CreatedAt   uint64
	Epoch       uint64
	Compression config.CompressionType

	// Version is the body version of the record as read. 0 identifies a
	// pre-magic (v0) record.
	Version uint16

	// EpochRecorded reports whether the record carried the Epoch field.
	EpochRecorded bool

	// CompressionRecorded reports whether the record carried the Compression
	// field.
	CompressionRecorded bool
}

// MarshalSnapshotMeta encodes m as a snapshot.meta record at the current
// format version. It is the single encoder for every writer of a snapshot
// directory — the save path, the receive path and the operator import path —
// so no writer can produce a record the purge-floor scan cannot read.
//
// CreatedAt is stamped with the current wall clock when m.CreatedAt is 0. The
// Version and *Recorded fields of m are ignored: the output is always written
// at snapshotMetaVersion with every field present.
func MarshalSnapshotMeta(m SnapshotMeta) []byte {
	buf := make([]byte, snapshotMetaSize)
	copy(buf[snapshotMetaMagicOff:], snapshotMetaMagic)
	binary.LittleEndian.PutUint16(buf[snapshotMetaVersionOff:], snapshotMetaVersion)
	binary.LittleEndian.PutUint16(buf[snapshotMetaReservedOff:], 0)
	binary.LittleEndian.PutUint32(buf[snapshotMetaBodyLenOff:], snapshotMetaBodySize)

	createdAt := m.CreatedAt
	if createdAt == 0 {
		createdAt = uint64(time.Now().Unix())
	}
	body := buf[snapshotMetaHeaderSize:]
	binary.LittleEndian.PutUint64(body[metaBodyIndexOff:], m.Index)
	binary.LittleEndian.PutUint64(body[metaBodyTermOff:], m.Term)
	binary.LittleEndian.PutUint64(body[metaBodyShardIDOff:], m.ShardID)
	binary.LittleEndian.PutUint64(body[metaBodyReplicaIDOff:], m.ReplicaID)
	binary.LittleEndian.PutUint64(body[metaBodyCreatedAtOff:], createdAt)
	binary.LittleEndian.PutUint64(body[metaBodyEpochOff:], m.Epoch)
	binary.LittleEndian.PutUint64(body[metaBodyCompressionOff:], uint64(m.Compression))
	return buf
}

// ParseSnapshotMeta decodes a snapshot.meta image.
//
// It accepts a record written by this binary, one written by a DIFFERENT
// version of the format (older or newer — see the append-only evolution rule),
// and the pre-magic v0 layouts. Anything else is rejected with
// ErrSnapshotMetaCorrupt rather than parsed positionally: reading foreign bytes
// as an epoch would feed a fabricated value to the DEK purge floor.
//
// A field the record does not carry is left zero and its *Recorded flag is
// false. Callers that act on Epoch MUST consult EpochRecorded.
func ParseSnapshotMeta(data []byte) (SnapshotMeta, error) {
	if len(data) >= snapshotMetaHeaderSize &&
		string(data[snapshotMetaMagicOff:snapshotMetaMagicOff+len(snapshotMetaMagic)]) == snapshotMetaMagic {
		return parseVersionedSnapshotMeta(data)
	}
	return parseLegacySnapshotMeta(data)
}

// parseVersionedSnapshotMeta decodes a record carrying the magic. The declared
// body length — not len(data) — bounds every field read, so a record whose
// body was truncated after the header is rejected instead of read past its end.
//
// bodyLen stays a uint32 and every comparison is made in unsigned arithmetic.
// Converting it to int first was safe only on a 64-bit platform: on a 32-bit
// GOARCH a declared length of 2^31 or more becomes NEGATIVE, the "claims more
// body than the file holds" guard is then false, and body[:bodyLen] panics —
// inside the DEK purge-floor scan, which walks every snapshot directory on the
// host and is driven by bytes on disk. waldb's replayBootstrap avoids the same
// truncation deliberately.
func parseVersionedSnapshotMeta(data []byte) (SnapshotMeta, error) {
	version := binary.LittleEndian.Uint16(data[snapshotMetaVersionOff:])
	bodyLen := binary.LittleEndian.Uint32(data[snapshotMetaBodyLenOff:])
	body := data[snapshotMetaHeaderSize:]
	if uint64(bodyLen) > uint64(len(body)) {
		// The record claims more body than the file holds: truncated.
		return SnapshotMeta{}, ErrSnapshotMetaCorrupt
	}
	body = body[:bodyLen]

	// Every version of the body carries at least the identity fields. A record
	// shorter than that is not a record this format ever produced.
	if bodyLen < metaBodyCreatedAtOff+8 {
		return SnapshotMeta{}, ErrSnapshotMetaCorrupt
	}
	m := SnapshotMeta{
		Version:   version,
		Index:     binary.LittleEndian.Uint64(body[metaBodyIndexOff:]),
		Term:      binary.LittleEndian.Uint64(body[metaBodyTermOff:]),
		ShardID:   binary.LittleEndian.Uint64(body[metaBodyShardIDOff:]),
		ReplicaID: binary.LittleEndian.Uint64(body[metaBodyReplicaIDOff:]),
		CreatedAt: binary.LittleEndian.Uint64(body[metaBodyCreatedAtOff:]),
	}
	// Fields are append-only, so a field present in a LONGER body (written by
	// a newer version) sits at the offset this binary already knows and is
	// read here; anything beyond the known fields is ignored.
	if bodyLen >= metaBodyEpochOff+8 {
		m.Epoch = binary.LittleEndian.Uint64(body[metaBodyEpochOff:])
		m.EpochRecorded = true
	}
	if bodyLen >= metaBodyCompressionOff+8 {
		m.Compression = config.CompressionType(binary.LittleEndian.Uint64(body[metaBodyCompressionOff:]))
		m.CompressionRecorded = true
	}
	return m, nil
}

// parseLegacySnapshotMeta decodes a pre-magic (v0) record. Only the EXACT
// sizes earlier builds wrote are accepted: a v0 record has no length prefix, so
// its size is the only evidence of which fields it carries, and accepting an
// arbitrary size would read a torn write as valid.
func parseLegacySnapshotMeta(data []byte) (SnapshotMeta, error) {
	switch len(data) {
	case legacyMetaSizeNoEpoch, legacyMetaSizeEpoch, legacyMetaSizeCompression:
	default:
		return SnapshotMeta{}, ErrSnapshotMetaCorrupt
	}
	m := SnapshotMeta{
		Version:   0,
		Index:     binary.LittleEndian.Uint64(data[metaBodyIndexOff:]),
		Term:      binary.LittleEndian.Uint64(data[metaBodyTermOff:]),
		ShardID:   binary.LittleEndian.Uint64(data[metaBodyShardIDOff:]),
		ReplicaID: binary.LittleEndian.Uint64(data[metaBodyReplicaIDOff:]),
		CreatedAt: binary.LittleEndian.Uint64(data[metaBodyCreatedAtOff:]),
	}
	if len(data) >= legacyMetaSizeEpoch {
		m.Epoch = binary.LittleEndian.Uint64(data[metaBodyEpochOff:])
		m.EpochRecorded = true
	}
	if len(data) >= legacyMetaSizeCompression {
		m.Compression = config.CompressionType(binary.LittleEndian.Uint64(data[metaBodyCompressionOff:]))
		m.CompressionRecorded = true
	}
	return m, nil
}

// snapshotEpochStatus classifies what a snapshot directory can tell the DEK
// purge floor about the epoch its artifact needs.
type snapshotEpochStatus uint8

const (
	// snapshotEpochKnown means the epoch is authoritative. Epoch 0 is then a
	// genuine statement that the artifact is unencrypted (or that the
	// directory is gone), so the directory imposes no floor.
	snapshotEpochKnown snapshotEpochStatus = iota

	// snapshotEpochLegacy means the record was RECOGNISED but predates the
	// Epoch field, so the artifact's epoch is unknown. Unknown is not "no
	// floor": the artifact may be ciphertext at any epoch, so the caller must
	// not purge. It is reported separately from snapshotEpochUnknown because
	// it is an expected, self-clearing upgrade condition rather than damage.
	snapshotEpochLegacy

	// snapshotEpochUnknown means the record could not be read at all: an I/O
	// error, or bytes that are not a snapshot.meta record. The caller must not
	// purge.
	snapshotEpochUnknown
)

// String renders the status for structured logs.
func (s snapshotEpochStatus) String() string {
	switch s {
	case snapshotEpochKnown:
		return "known"
	case snapshotEpochLegacy:
		return "legacy_record_without_epoch"
	case snapshotEpochUnknown:
		return "unreadable"
	default:
		return "invalid"
	}
}

// readSnapshotEpoch reads the epoch a snapshot directory's artifact was
// encrypted under, together with how much that answer can be trusted:
//
//   - (epoch, snapshotEpochKnown)  — the record was read and carries the field.
//     epoch == 0 means the artifact is unencrypted and imposes no floor;
//     epoch > 0 pins the floor at that epoch. A MISSING metadata file also
//     reports (0, known): the snapshot was garbage-collected between directory
//     enumeration and this read, so there is no artifact to protect.
//   - (0, snapshotEpochLegacy)     — a recognised pre-Epoch-field record. The
//     epoch is unknown; the caller must fail safe.
//   - (0, snapshotEpochUnknown)    — unreadable or corrupt. The epoch is
//     unknown; the caller must fail safe.
//
// The two failing statuses have IDENTICAL consequences for purging and differ
// only in what they tell an operator. Dropping either from the floor would
// over-purge a DEK the artifact may still need.
func readSnapshotEpoch(dir string) (uint64, snapshotEpochStatus) {
	metaPath := filepath.Join(dir, snapshotMetadataFile)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Snapshot GC'd between enumeration and read: benign, no floor.
			return 0, snapshotEpochKnown
		}
		// Real read error: epoch indeterminate, fail safe.
		return 0, snapshotEpochUnknown
	}
	meta, parseErr := ParseSnapshotMeta(data)
	if parseErr != nil {
		return 0, snapshotEpochUnknown
	}
	if !meta.EpochRecorded {
		return 0, snapshotEpochLegacy
	}
	return meta.Epoch, snapshotEpochKnown
}
