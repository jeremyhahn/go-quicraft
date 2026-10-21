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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// ErrSnapshotMetaTooLarge is returned when a snapshot.meta file is longer than
// any record this format can produce. It is treated as unreadable (unevaluable)
// by the DEK purge floor, never parsed: reading a multi-GB foreign file as a
// metadata record would defeat the parser's bounded-before-alloc discipline
// inside the per-save purge-floor scan, which walks every snapshot directory on
// the host.
var ErrSnapshotMetaTooLarge = errors.New(
	"engine: snapshot metadata file exceeds the maximum record size")

// maxSnapshotMetaFileLen bounds how many bytes readSnapshotEpoch reads from a
// snapshot.meta file. The largest record this format writes is the header plus a
// body no larger than maxSnapshotMetaBodyLen; a file longer than that is foreign
// or corrupt, so it is rejected rather than slurped whole.
const maxSnapshotMetaFileLen = snapshotMetaHeaderSize + maxSnapshotMetaBodyLen

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
//	 72     8  DataLen (bytes of snapshot.dat as written)
//	 80     8  DataHash (xxhash64 of snapshot.dat as written)
//
// # Evolution rule
//
// snapshotMetaVersion is pinned at its current value until the first stable
// v1 release, like every format version in this repository. Until then a
// layout change is made IN PLACE, with every writer and reader updated in
// lockstep and development data directories wiped rather than migrated; the
// version is not incremented to record pre-release churn.
//
// The reader is driven by the recorded body length, not by the version: a
// field whose offset lies past the end of the body is reported ABSENT (its
// *Recorded flag false), and fields past the last one this binary knows are
// ignored. Because the body length is recorded, a reader can always tell a
// field that is absent from a record that is CORRUPT (a truncated or foreign
// record) — the distinction the magic/length prefix exists to make.
//
// # Compatibility
//
// A record that does not begin with the magic is refused with
// ErrSnapshotMetaCorrupt, whatever its size: there is no positional decode of
// headerless layouts. The remedy for such a directory is to wipe it and
// re-sync from peers.
const (
	// snapshotMetaMagic identifies a self-describing snapshot.meta record.
	snapshotMetaMagic = "QRSNAPMD"

	// snapshotMetaVersion is the body version this binary WRITES.
	snapshotMetaVersion uint16 = 1

	// previousSnapshotMetaVersion is the second member of the closed read
	// window (docs/compatibility, "The accept sets"): the body version the
	// previous MINOR release wrote. v1.0 has no previous release, so it
	// equals snapshotMetaVersion and the set has one member.
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

	// maxSnapshotMetaBodyLen bounds the body length a record may DECLARE.
	//
	// The declared length is a uint32 read from a file and is used to slice the
	// body, so it must be bounded before it is converted to any narrower type:
	// on a 32-bit GOARCH a declared length of 2^31 or more becomes a NEGATIVE
	// int, "claims more body than the file holds" is then false, and the slice
	// panics — inside the DEK purge-floor scan, which walks every snapshot
	// directory on the host and whose entire input is bytes on disk. Rejecting
	// above this cap makes that truncation UNREACHABLE by construction rather
	// than merely unreached on a 64-bit build, so the guarantee does not
	// depend on the width of int.
	//
	// 64 KiB is 8192 uint64 body fields against the current 9, so no
	// legitimate record can approach it; anything that does is corruption or
	// a foreign file.
	// The same bounded-then-parsed shape waldb's replayBootstrap uses.
	maxSnapshotMetaBodyLen = 1 << 16
)

// SnapshotMeta is the decoded content of a snapshot.meta record.
//
// The *Recorded flags distinguish a field the record actually CARRIED from one
// that was absent because the record's body ended before it. A zero Epoch with
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

	// Version is the body version of the record as read.
	Version uint16

	// EpochRecorded reports whether the record carried the Epoch field.
	EpochRecorded bool

	// CompressionRecorded reports whether the record carried the Compression
	// field.
	CompressionRecorded bool

	// DataLen and DataHash are the length and xxhash64 of snapshot.dat AS
	// WRITTEN — after compression and after encryption. They are the only
	// at-rest integrity check a snapshot has when no barrier is configured:
	// with a barrier, per-frame AEAD covers it, but a plaintext deployment had
	// nothing at all, and a single flipped byte inside a serialized value
	// leaves the file length unchanged so no structural check trips. The
	// replica then recovers the corrupt value into the state machine and
	// serves it, diverging from its peers at the same log index with every
	// Raft invariant intact.
	//
	// Hashing the FINAL bytes (rather than the pre-encryption ones) is what
	// makes one value verifiable by all three consumers — recovery, the
	// streamed receive path and ImportSnapshot — without any of them needing a
	// key.
	DataLen  uint64
	DataHash uint64

	// DataHashRecorded reports whether the record carried DataLen/DataHash.
	// Every current writer stamps them, so a record that leaves it false
	// cannot vouch for its artifact, and recovery
	// refuses it (see VerifySnapshotArtifact). The purge-floor scan, which
	// reads only Epoch, still accepts such a record.
	DataHashRecorded bool
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
	binary.LittleEndian.PutUint64(body[metaBodyDataLenOff:], m.DataLen)
	binary.LittleEndian.PutUint64(body[metaBodyDataHashOff:], m.DataHash)
	return buf
}

// ParseSnapshotMeta decodes a snapshot.meta image.
//
// It accepts a record carrying the magic whose declared body holds at least the
// identity fields (see the evolution rule: fields are read by body length).
// Anything else, a record without the magic included, is rejected with
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
	return SnapshotMeta{}, ErrSnapshotMetaCorrupt
}

// parseVersionedSnapshotMeta decodes a record carrying the magic. The declared
// body length — not len(data) — bounds every field read, so a record whose
// body was truncated after the header is rejected instead of read past its end.
//
// The declared length is bounded by maxSnapshotMetaBodyLen FIRST, and every
// comparison is then made in unsigned arithmetic. Both matter: converting the
// uint32 to int before the bounds check is safe only where int is 64 bits, and
// on a 32-bit GOARCH a declared length of 2^31 or more becomes NEGATIVE, the
// "claims more body than the file holds" guard is false, and body[:bodyLen]
// panics — inside the DEK purge-floor scan, which walks every snapshot
// directory on the host and is driven by bytes on disk. The cap makes that
// value unrepresentable here on every GOARCH; the unsigned comparison keeps the
// remaining check honest. waldb's replayBootstrap has the same shape.
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
		m.Compression = config.CompressionType(binary.LittleEndian.Uint64(body[metaBodyCompressionOff:]))
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

	// snapshotEpochLegacy means the record was RECOGNISED but its declared
	// body ends before the Epoch field, so the artifact's epoch is unknown. Unknown is not "no
	// floor": the artifact may be ciphertext at any epoch, so the caller must
	// not purge. It is reported separately from snapshotEpochUnknown because
	// the record itself is well formed rather than damaged.
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
//     epoch > 0 pins the floor at that epoch. A missing metadata file reports
//     (0, known) ONLY when the ARTIFACT is missing too (see below).
//   - (0, snapshotEpochLegacy)     — a recognised record without Epoch. The
//     epoch is unknown; the caller must fail safe.
//   - (0, snapshotEpochUnknown)    — unreadable, corrupt, or absent beside an
//     artifact that is still there. The epoch is unknown; the caller must fail
//     safe.
//
// The two failing statuses have IDENTICAL consequences for purging and differ
// only in what they tell an operator. Dropping either from the floor would
// over-purge a DEK the artifact may still need.
//
// # Why a missing record is not by itself a "no floor" answer
//
// The GC race the missing-file case exists for is real — a directory can be
// removed between enumeration and this read — but observing that the METADATA
// is absent is strictly weaker than observing that the ARTIFACT is. Half-built
// snapshot directories with snapshot.dat present and snapshot.meta not yet
// written are producible by any crash between the two writes, on the receive
// path and on the local save path alike, and such a directory holds ciphertext
// at an epoch nobody can name. Answering "imposes no floor" for it is the exact
// inversion purgeOldEpochs warns about: "cannot be evaluated" can only mean "do
// not purge"; it can never mean "this directory imposes no floor".
//
// So the artifact is what decides. Absent artifact: the directory really is
// gone or was reclaimed, and there is nothing to protect. Present artifact with
// no record: unknown, fail safe.
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

// readBoundedSnapshotMeta reads a snapshot.meta file, refusing to read more than
// one byte past the largest record this format can produce. os.ReadFile would
// slurp a corrupt or substituted multi-GB file whole — inside the per-save DEK
// purge-floor scan that walks every snapshot directory on the host — defeating
// the parser's own bounded-before-alloc discipline. A file that yields more than
// the maximum is foreign and is rejected with ErrSnapshotMetaTooLarge; the
// caller treats that identically to any other unreadable record (fail safe).
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

// missingMetaEpochStatus classifies a snapshot directory whose snapshot.meta
// does not exist, by looking at whether the ARTIFACT it would describe is still
// there.
//
// A stat error other than not-exist leaves the artifact's presence unknown,
// which is the same operational state as an unreadable record: fail safe.
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

// snapshotAADMagic tags the context-AAD scheme. It is part of the authenticated
// context rather than a byte on disk, so the scheme identifies ITSELF: a frame
// sealed under a future layout can never authenticate against this one, and
// there is no unauthenticated version marker for an attacker to rewrite.
const snapshotAADMagic = "QRSNAP1"

// SnapshotFrameAADLen is the exact length of the context every snapshot frame
// is bound to: the magic, four big-endian uint64 fields, and the terminal
// flag. Every field is fixed width, so no delimiter is needed and no two
// distinct positions can produce the same bytes.
//
//	off  size  field
//	  0     7  magic "QRSNAP1"
//	  7     8  ShardID
//	 15     8  ReplicaID
//	 23     8  SnapshotIndex
//	 31     8  ChunkSeq (0-based ordinal within snapshot.dat)
//	 39     1  Final (1 on the artifact's last frame, 0 on every other)
//
// Final is what makes TAIL TRUNCATION detectable. The ordinal alone binds a
// frame to its position, but a shortened artifact is still a sequence of
// frames at consecutive ordinals, every one of which authenticates. With the
// flag, exactly one frame was sealed as the last, and a reader that decrypts
// the frame it finds last under Final=1 fails authentication whenever frames
// after it were dropped. Appending a frame after the terminal one fails the
// same way in reverse.
const SnapshotFrameAADLen = len(snapshotAADMagic) + 4*8 + 1

const (
	snapshotAADShardOff   = len(snapshotAADMagic)
	snapshotAADReplicaOff = snapshotAADShardOff + 8
	snapshotAADIndexOff   = snapshotAADReplicaOff + 8
	snapshotAADSeqOff     = snapshotAADIndexOff + 8
	snapshotAADFinalOff   = snapshotAADSeqOff + 8
)

// noContextAADOnce bounds the capability warning to a single record per
// process. The condition is a DEPLOYMENT property (the barrier delegates to an
// encryptor that cannot bind context, or encrypts nothing at all, for its whole
// life), not a per-artifact or per-frame one, so repeating it per snapshot would
// say nothing new.
var noContextAADOnce sync.Once

// contextAADBarrier is the capability NewSnapshotAAD asks of the sealing
// barrier: can it ACTUALLY authenticate a caller-supplied context? A barrier
// that cannot must not be handed one — it fails the write closed rather than
// silently dropping the binding.
type contextAADBarrier interface {
	SupportsContextAAD() bool
}

// plaintextModeReporter is an OPTIONAL capability a barrier may implement to
// distinguish the two reasons it cannot bind a context: no-encryption mode,
// where there is no AEAD and therefore nothing a position could be bound to,
// versus a delegated encryptor that IS protecting the data but cannot
// authenticate where it belongs. Only the second is a security limitation, and
// only the warning text differs. A barrier that does not implement this is
// reported as the latter, which is the conservative reading.
type plaintextModeReporter interface {
	IsPlaintextMode() bool
}

// SnapshotAAD builds the per-frame positional context every snapshot frame is
// bound to. One SnapshotAAD belongs to one snapshot artifact; Frame stamps the
// chunk ordinal into a reused scratch buffer, so a multi-gigabyte snapshot
// costs exactly one allocation for the whole file rather than one per frame.
//
// The SAME builder type is used on the encrypt and the decrypt side, which is
// what makes the two sets of bytes incapable of drifting apart.
//
// The context is UNCONDITIONAL: there is no on-disk version marker and no
// weaker-AAD retry. A reader that fails to authenticate a frame fails — it
// never falls back to an epoch-only AAD, because that fallback IS the
// vulnerability the context closes (a frame could then be replayed at another
// ordinal, another snapshot index, or another shard). The scheme is fixed by
// code and artifact class alone.
type SnapshotAAD struct {
	buf      [SnapshotFrameAADLen]byte
	suppress bool
}

// NewSnapshotAAD returns the context builder for one snapshot artifact,
// identified by the shard, replica and snapshot index its frames belong to.
//
// barrier decides whether the context is built at all. A barrier that cannot
// authenticate caller-supplied AAD — a delegated encryptor that does not
// implement seal.AADSymmetricEncrypter, or no-encryption mode — returns
// seal.ErrHardwareMode for ANY non-empty context, so the builder suppresses the
// context and the frames keep only whatever binding the barrier applies on its
// own. That is a capability of the DEPLOYMENT's barrier, resolved once here; it
// is NEVER selected per artifact and NEVER reached by way of a decryption
// failure. A barrier that CAN bind context gets the positional binding whether
// it encrypts in software or delegates to hardware.
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

// Frame returns the context for the frame at 0-based ordinal seq; final
// reports whether it is the artifact's last frame. The returned slice aliases
// the builder's scratch buffer and is only valid until the next call; the
// barrier folds it into the AEAD AAD and never retains it.
//
// It returns nil when the barrier cannot bind context, which is byte-identical
// to what that barrier does with no context at all. Such a deployment gets no
// positional binding and therefore no truncation detection beyond the
// structural rule that an artifact holds at least one frame.
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
