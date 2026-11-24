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
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// ErrSnapshotMetaTooLarge is returned for a snapshot.meta longer than any valid record; the purge floor treats it as unreadable.
var ErrSnapshotMetaTooLarge = errors.New(
	"engine: snapshot metadata file exceeds the maximum record size")

// maxSnapshotMetaFileLen bounds how many bytes readSnapshotEpoch reads from a snapshot.meta file.
const maxSnapshotMetaFileLen = snapshotMetaHeaderSize + maxSnapshotMetaBodyLen

// snapshot.meta: 16-byte header (magic QRSNAPMD, uint16 version, uint16 reserved, uint32 body length), then little-endian uint64 Index, Term, ShardID, ReplicaID, CreatedAt, Epoch, Compression, DataLen, DataHash; fields past the body are absent, a record without the magic is corrupt.
const (
	// snapshotMetaMagic identifies a self-describing snapshot.meta record.
	snapshotMetaMagic = "QRSNAPMD"

	// snapshotMetaVersion is the body version this binary writes.
	snapshotMetaVersion uint16 = 1

	// previousSnapshotMetaVersion is the previous minor release's body version; before a previous release it equals snapshotMetaVersion.
	previousSnapshotMetaVersion = snapshotMetaVersion
)

// acceptsSnapshotMetaVersion reports whether v is in the closed read set
// {snapshotMetaVersion, previousSnapshotMetaVersion}. It is a set, not a
// range, so it never widens when a constant moves.
func acceptsSnapshotMetaVersion(v uint16) bool {
	return v == snapshotMetaVersion || v == previousSnapshotMetaVersion
}

const (

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
	metaBodyDataLenOff     = 56
	metaBodyDataHashOff    = 64

	// snapshotMetaBodySize is the body size at snapshotMetaVersion.
	snapshotMetaBodySize = 9 * 8

	// snapshotMetaSize is the total size of a record this binary writes.
	snapshotMetaSize = snapshotMetaHeaderSize + snapshotMetaBodySize

	// maxSnapshotMetaBodyLen caps the declared body length so it can never overflow int on 32-bit platforms.
	maxSnapshotMetaBodyLen = 1 << 16
)

// SnapshotMeta is a decoded snapshot.meta record; a false *Recorded flag means the field is unknown, not zero.
type SnapshotMeta struct {
	Index       uint64
	Term        uint64
	ShardID     uint64
	ReplicaID   uint64
	CreatedAt   uint64
	Epoch       uint64
	Compression config.CompressionType

	// Version is the body version of the record as read.
	Version uint16

	// EpochRecorded reports whether the record carried the Epoch field.
	EpochRecorded bool

	// CompressionRecorded reports whether the record carried the Compression
	// field.
	CompressionRecorded bool

	// DataLen and DataHash are the length and xxhash64 of snapshot.dat as written, after compression and encryption.
	DataLen  uint64
	DataHash uint64

	// DataHashRecorded reports whether the record carried DataLen and DataHash; recovery refuses a record without them.
	DataHashRecorded bool
}

// MarshalSnapshotMeta encodes m at the current version with every field; CreatedAt defaults to now.
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
	binary.LittleEndian.PutUint64(body[metaBodyDataLenOff:], m.DataLen)
	binary.LittleEndian.PutUint64(body[metaBodyDataHashOff:], m.DataHash)
	return buf
}

// ParseSnapshotMeta decodes a snapshot.meta image; records without the magic are ErrSnapshotMetaCorrupt, and callers acting on Epoch must check EpochRecorded.
func ParseSnapshotMeta(data []byte) (SnapshotMeta, error) {
	if len(data) >= snapshotMetaHeaderSize &&
		string(data[snapshotMetaMagicOff:snapshotMetaMagicOff+len(snapshotMetaMagic)]) == snapshotMetaMagic {
		return parseVersionedSnapshotMeta(data)
	}
	return SnapshotMeta{}, ErrSnapshotMetaCorrupt
}

// parseVersionedSnapshotMeta decodes a record with the magic, bounding the declared body length first and comparing unsigned.
func parseVersionedSnapshotMeta(data []byte) (SnapshotMeta, error) {
	version := binary.LittleEndian.Uint16(data[snapshotMetaVersionOff:])
	if !acceptsSnapshotMetaVersion(version) {
		// A body version outside the read window is a layout this build
		// does not know; its fields must not be read at these offsets.
		return SnapshotMeta{}, ErrSnapshotMetaCorrupt
	}
	bodyLen := binary.LittleEndian.Uint32(data[snapshotMetaBodyLenOff:])
	if bodyLen > maxSnapshotMetaBodyLen {
		// No record this format produces declares a body anywhere near this;
		// the value is corruption or a foreign file.
		return SnapshotMeta{}, ErrSnapshotMetaCorrupt
	}
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
	// Each field is read only if the declared body reaches it; anything
	// beyond the known fields is ignored.
	if bodyLen >= metaBodyEpochOff+8 {
		m.Epoch = binary.LittleEndian.Uint64(body[metaBodyEpochOff:])
		m.EpochRecorded = true
	}
	if bodyLen >= metaBodyCompressionOff+8 {
		raw := binary.LittleEndian.Uint64(body[metaBodyCompressionOff:])
		// CompressionType is a uint16 and the encoder writes one widened:
		// a wider value is not a record it wrote, and converting it would
		// truncate it into a different (possibly known) type.
		if raw > math.MaxUint16 {
			return SnapshotMeta{}, ErrSnapshotMetaCorrupt
		}
		m.Compression = config.CompressionType(raw)
		m.CompressionRecorded = true
	}
	// Both integrity fields are read together or not at all: a hash without
	// its length, or a length without its hash, is not a check.
	if bodyLen >= metaBodyDataHashOff+8 {
		m.DataLen = binary.LittleEndian.Uint64(body[metaBodyDataLenOff:])
		m.DataHash = binary.LittleEndian.Uint64(body[metaBodyDataHashOff:])
		m.DataHashRecorded = true
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

	// snapshotEpochLegacy is a well-formed record whose body ends before Epoch; the epoch is unknown, so the caller must not purge.
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

// readSnapshotEpoch returns a directory's artifact epoch and whether it is known; legacy or unknown means do not purge, and a missing record is known-0 only when the artifact is gone too.
func readSnapshotEpoch(dir string) (uint64, snapshotEpochStatus) {
	metaPath := filepath.Join(dir, snapshotMetadataFile)
	data, err := readBoundedSnapshotMeta(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return missingMetaEpochStatus(dir)
		}
		// Real read error, or a file too large to be a record (foreign): epoch
		// indeterminate, fail safe.
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

// readBoundedSnapshotMeta reads snapshot.meta up to one byte past the largest valid record; larger is ErrSnapshotMetaTooLarge.
func readBoundedSnapshotMeta(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot: metadata file close failed", "path", path, "error", closeErr)
		}
	}()
	// Read one byte past the maximum a valid record can occupy: if the file
	// yields that many bytes it is longer than any record this format writes.
	data, err := io.ReadAll(io.LimitReader(f, maxSnapshotMetaFileLen+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxSnapshotMetaFileLen {
		return nil, ErrSnapshotMetaTooLarge
	}
	return data, nil
}

// missingMetaEpochStatus classifies a directory without snapshot.meta by whether its artifact still exists; a stat error is unknown.
func missingMetaEpochStatus(dir string) (uint64, snapshotEpochStatus) {
	if _, err := os.Stat(filepath.Join(dir, snapshotDataFile)); err != nil {
		if os.IsNotExist(err) {
			// No record and no artifact: the snapshot was garbage-collected
			// between directory enumeration and this read. Nothing to protect.
			return 0, snapshotEpochKnown
		}
		return 0, snapshotEpochUnknown
	}
	// The artifact outlived its record — a crash between the two writes, or a
	// partial removal. It may be ciphertext at any epoch and the file that
	// would have said which is the one that is missing.
	slog.Warn("snapshot: artifact present with no metadata record; "+
		"treating its epoch as unknown so no DEK is purged",
		"dir", dir,
	)
	return 0, snapshotEpochUnknown
}

// ---------------------------------------------------------------------------
// Snapshot frame context-AAD
// ---------------------------------------------------------------------------

// snapshotAADMagic tags the context-AAD scheme inside the authenticated context, so it cannot be rewritten.
const snapshotAADMagic = "QRSNAP1"

// SnapshotFrameAADLen is the frame context length: magic QRSNAP1, big-endian uint64 ShardID, ReplicaID, SnapshotIndex, ChunkSeq, then a Final byte that detects truncation.
const SnapshotFrameAADLen = len(snapshotAADMagic) + 4*8 + 1

const (
	snapshotAADShardOff   = len(snapshotAADMagic)
	snapshotAADReplicaOff = snapshotAADShardOff + 8
	snapshotAADIndexOff   = snapshotAADReplicaOff + 8
	snapshotAADSeqOff     = snapshotAADIndexOff + 8
	snapshotAADFinalOff   = snapshotAADSeqOff + 8
)

// noContextAADOnce limits the capability warning to once per process.
var noContextAADOnce sync.Once

// contextAADBarrier reports whether a barrier can authenticate caller-supplied context.
type contextAADBarrier interface {
	SupportsContextAAD() bool
}

// plaintextModeReporter optionally tells no-encryption mode apart from an encryptor that cannot bind context; only the warning text differs.
type plaintextModeReporter interface {
	IsPlaintextMode() bool
}

// SnapshotAAD builds the positional context for one artifact's frames on both encrypt and decrypt sides; there is no weaker fallback.
type SnapshotAAD struct {
	buf      [SnapshotFrameAADLen]byte
	suppress bool
}

// NewSnapshotAAD returns the context builder for one artifact; it builds no context when the barrier cannot authenticate AAD.
func NewSnapshotAAD(shardID, replicaID, index uint64, barrier contextAADBarrier) *SnapshotAAD {
	suppress := barrier == nil || !barrier.SupportsContextAAD()
	a := &SnapshotAAD{suppress: suppress}
	if suppress {
		plaintext := false
		if pm, ok := barrier.(plaintextModeReporter); ok {
			plaintext = pm.IsPlaintextMode()
		}
		noContextAADOnce.Do(func() {
			if plaintext {
				slog.Warn("snapshot: barrier performs no encryption (plaintext mode); " +
					"snapshot frames carry no positional binding because there is no " +
					"authentication to bind it to")
				return
			}
			slog.Warn("snapshot: barrier cannot authenticate caller context; " +
				"snapshot frames keep only the barrier's own binding, without positional " +
				"(shard/replica/index/ordinal) binding")
		})
		return a
	}
	copy(a.buf[:], snapshotAADMagic)
	binary.BigEndian.PutUint64(a.buf[snapshotAADShardOff:], shardID)
	binary.BigEndian.PutUint64(a.buf[snapshotAADReplicaOff:], replicaID)
	binary.BigEndian.PutUint64(a.buf[snapshotAADIndexOff:], index)
	return a
}

// Frame returns the context for frame seq, valid until the next call, or nil when the barrier cannot bind context.
func (a *SnapshotAAD) Frame(seq uint64, final bool) []byte {
	if a.suppress {
		return nil
	}
	binary.BigEndian.PutUint64(a.buf[snapshotAADSeqOff:], seq)
	a.buf[snapshotAADFinalOff] = 0
	if final {
		a.buf[snapshotAADFinalOff] = 1
	}
	return a.buf[:]
}
