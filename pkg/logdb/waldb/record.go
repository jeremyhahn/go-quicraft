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

// Package waldb implements write-ahead log storage for the quicraft Raft engine.
//
// WAL records use a CRC32 + length + type + durability-sequence +
// generation-start + data format within fixed-size blocks, following the
// LevelDB/Pebble log format. Records that do not fit within a single block are
// split into multiple chunks (First, Middle, Last). CRC32 uses the Castagnoli
// polynomial for hardware acceleration on amd64. The durability sequence and
// the generation start are what let recovery tell an interrupted write from
// corruption of acknowledged data — exactly, not probabilistically; see the
// durability-sequence commentary below and segment.ReadAll.
package waldb

import (
	"encoding/binary"
	"hash/crc32"
)

// Record chunk types. A logical record may span one or more physical chunks.
// Each chunk is self-contained with its own CRC, length, and type.
const (
	// RecordFull indicates a complete record fits in one chunk.
	RecordFull byte = 1
	// RecordFirst indicates the first chunk of a multi-chunk record.
	RecordFirst byte = 2
	// RecordMiddle indicates a middle chunk of a multi-chunk record.
	RecordMiddle byte = 3
	// RecordLast indicates the last chunk of a multi-chunk record.
	RecordLast byte = 4
	// RecordSyncMark is a header-only chunk the writer appends immediately
	// after every completed fdatasync. It carries no record; it exists so
	// the segment's FINAL durability generation can testify for itself. See
	// the sync-mark commentary below.
	RecordSyncMark byte = 5
)

// The durability sequence carried by every chunk.
//
// Recovery has to answer one question about any damage it finds: would
// truncating to the clean prefix destroy a record this node already
// ACKNOWLEDGED? The framing alone cannot answer it. A batch larger than one
// block issues one WriteAt per block before a single fdatasync, and writeback
// may persist an arbitrary subset of those pages, so an ordinary power loss can
// leave decodable records sitting above a CRC failure. That is physically a
// torn write — nothing above the damage was ever acknowledged — yet it is
// byte-for-byte indistinguishable from bit rot in the middle of a live segment,
// where everything above the damage WAS acknowledged. Without a discriminator
// recovery must assume the worst and refuse to start, which is what it did.
//
// durSeq is that discriminator. It is the number of Syncs the segment has
// COMPLETED at the moment the chunk is encoded, so it partitions the segment
// into durability generations:
//
//   - A record with durSeq g was written before Sync #(g+1) was called.
//   - The writer only writes a durSeq g+1 record AFTER Sync #(g+1) returned.
//   - Therefore the existence on disk of ANY record with durSeq > g PROVES that
//     Sync #(g+1) completed, which proves every durSeq <= g record was durable
//     and acknowledged.
//
// The contrapositive is the whole fix: records carrying the segment's HIGHEST
// durSeq have no such proof behind them — they belong to the one batch that was
// still in flight — so discarding them destroys nothing that was acknowledged.
// A torn write is exactly a suffix of that final generation; interior rot leaves
// records of EARLIER generations above the damage, and those are acknowledged.
// See segment.ReadAll.
//
// Granularity is per CHUNK rather than per block. A block is filled by many
// independent batches (a 32 KiB block holds hundreds of 48-byte HardState
// records, each with its own fdatasync), so a per-block generation would be
// stamped once at the top of the block and be wrong for everything after it —
// it cannot resolve a boundary that lies inside a block, which is where both the
// damage and the batch boundary usually are. Per chunk also survives losing the
// batch's leading page: every record carries its own generation, so the evidence
// does not depend on any single record surviving. The cost is measured, not
// assumed; see docs/storage/wal.md.
//
// The generation start is the second half of the discriminator, and it is what
// makes the verdict EXACT. The sequence alone can say that a completed fdatasync
// lies somewhere inside the destroyed bytes (the survivors carry a generation
// the clean prefix never reached), but not whether any acknowledged byte was
// among them: the ordinary power loss that drops the in-flight batch's leading
// page produces exactly that shape with nothing acknowledged lost, and media
// damage that begins a few bytes lower produces it with acknowledged records
// gone. Recovery used to truncate in both cases and report a "residual". Every
// chunk therefore also carries genStart: the file offset at which its
// generation began, i.e. the acknowledged end when the fdatasync that closed the
// previous generation returned. Any surviving chunk above the damage then says
// exactly where acknowledged bytes ended, and the damage either begins at or
// above that offset (an interrupted write; truncate) or below it (acknowledged
// data was destroyed; fail closed). See segment.ReadAll.
//
// The sync mark closes the one gap the two fields leave. Every chunk names the
// generation it belongs to, but a generation is proven CLOSED only by a chunk
// of a HIGHER generation, and the last generation of a segment has none: after
// the fdatasync that acknowledged it, the writer may simply have written
// nothing else — an idle node, a clean shutdown, a segment rotated away. Rot
// inside that generation then looks exactly like a torn tail, and recovery
// would truncate records this node had acknowledged. So Sync, after the
// fdatasync returns, appends one RecordSyncMark chunk stamped with the new
// generation and the acknowledged end. It is written through to the file (one
// WriteAt, no fdatasync) so it reaches the page cache at once and the medium
// with the next writeback. Whenever it survives, the final generation has the
// higher-generation witness every earlier one has, and damage inside it takes
// the fail-closed path. When it does not survive — a crash inside the writeback
// window after the last fdatasync — recovery sees what it saw before the mark
// existed and truncates, which is the interrupted-write verdict that window
// deserves. ReadAll never replays a mark; it is evidence only.
const (
	// recordSeqSize is the width of the durability sequence on disk.
	recordSeqSize = 4

	// recordGenStartSize is the width of the generation-start offset on disk.
	// It bounds a segment file at 4 GiB - 1 (maxSegmentFileSize), which
	// validateOpenOptions enforces.
	recordGenStartSize = 4

	// maxDurabilitySeq is the largest durability sequence a chunk can carry.
	// segment.CanFit refuses to write past it so the counter can never wrap and
	// present an OLD generation as the newest one, which would invert the proof
	// above. Reaching it takes 2^32 fdatasyncs into a single segment, roughly
	// three orders of magnitude more than the record count any legal segment
	// size admits, so the guard is a proof obligation rather than a live path.
	maxDurabilitySeq = uint32(1<<32 - 1)
)

// recordHeaderSize is the fixed overhead per chunk:
// [CRC32:4] [Length:2] [Type:1] [DurSeq:4] [GenStart:4] = 15 bytes.
const recordHeaderSize = 7 + recordSeqSize + recordGenStartSize

// syncMarkReserve is the room segment.CanFit keeps free below maxFileSize for
// the sync mark the next Sync appends: the mark itself plus the largest
// padding a block change before it can cost (recordHeaderSize-1 bytes, since a
// mark starts only in a block with a full header's worth of space). Without it
// the mark could land past maxFileSize and, at maxSegmentFileSize, name a
// generation start the width of the field cannot hold.
const syncMarkReserve = 2*recordHeaderSize - 1

// maxSegmentFileSize is the largest MaxFileSize a DB accepts: the generation
// start is a file offset stored in recordGenStartSize bytes, so a segment must
// not grow past what that width can name.
const maxSegmentFileSize = int64(1<<(8*recordGenStartSize) - 1)

// maxRecordDataLen is the maximum payload length per chunk, limited by the
// 2-byte length field.
const maxRecordDataLen = (1 << 16) - 1

// crc32Table is the Castagnoli CRC32 table, which is hardware-accelerated
// via SSE4.2 on amd64 processors.
//
// Security note: CRC32-C provides protection against accidental data corruption
// (bit flips, partial writes, torn pages) only. It is NOT a cryptographic
// integrity check and does not protect against intentional tampering. An
// adversary with disk access can trivially recompute a valid CRC32 after
// modifying record contents.
//
// For tamper protection of WAL data at rest, the crypto.Barrier must be
// configured. When enabled, the barrier encrypts each WAL record payload with
// AES-256-GCM, which provides authenticated encryption (confidentiality +
// integrity + authenticity). The CRC32 remains as a fast first-pass corruption
// detector to avoid feeding garbage into the decryption path.
//
// This is a deliberate design decision: CRC32-C runs at ~30 GB/s on modern
// hardware (hardware-accelerated), while HMAC-SHA256 runs at ~1-3 GB/s. Using
// CRC32 for the common case (accidental corruption) and deferring tamper
// protection to the barrier layer keeps the WAL fast path efficient.
var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// encodeRecord writes a single chunk to buf.
// Format: [CRC32:4][Length:2][Type:1][DurSeq:4][GenStart:4][Data:N]
// CRC32 covers [Type:1][DurSeq:4][GenStart:4][Data:N].
// Returns the number of bytes written and any error.
//
// The durability sequence and the generation start sit INSIDE the checksummed
// region deliberately: they are the values recovery uses to decide whether
// truncating is safe, so a flipped bit in either must invalidate the chunk
// rather than silently move it into another generation or move the
// acknowledged end. They are placed after the type byte so the covered region
// stays one contiguous slice and the CRC is still computed in a single pass.
func encodeRecord(buf []byte, recordType byte, durSeq, genStart uint32, data []byte) (int, error) {
	dataLen := len(data)
	totalLen := recordHeaderSize + dataLen

	if totalLen > len(buf) {
		return 0, ErrBufferTooSmall
	}
	if dataLen > maxRecordDataLen {
		return 0, ErrBufferTooSmall
	}
	if !isValidRecordType(recordType) {
		return 0, ErrInvalidRecordType
	}

	// Write type, sequence and generation start first so we can compute CRC
	// over [type][durSeq][genStart][data] as one range.
	buf[6] = recordType
	binary.LittleEndian.PutUint32(buf[7:7+recordSeqSize], durSeq)
	binary.LittleEndian.PutUint32(buf[7+recordSeqSize:recordHeaderSize], genStart)

	// Copy payload.
	copy(buf[recordHeaderSize:], data)

	// CRC32 covers type byte, durability sequence, generation start and payload.
	checksum := crc32.Checksum(buf[6:recordHeaderSize+dataLen], crc32Table)
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	// Length is the payload length (excludes the header itself).
	binary.LittleEndian.PutUint16(buf[4:6], uint16(dataLen))

	return totalLen, nil
}

// decodeRecord reads a single chunk from buf.
// Returns the payload data (a sub-slice of buf, not a copy), the chunk type,
// the chunk's durability sequence and generation start, the total number of
// bytes consumed, and any error.
func decodeRecord(buf []byte) (data []byte, recordType byte, durSeq, genStart uint32, n int, err error) {
	if len(buf) < recordHeaderSize {
		return nil, 0, 0, 0, 0, ErrCorruptedRecord
	}

	// Parse header fields.
	storedCRC := binary.LittleEndian.Uint32(buf[0:4])
	dataLen := int(binary.LittleEndian.Uint16(buf[4:6]))
	recordType = buf[6]

	totalLen := recordHeaderSize + dataLen
	if totalLen > len(buf) {
		return nil, 0, 0, 0, 0, ErrCorruptedRecord
	}

	if !isValidRecordType(recordType) {
		return nil, 0, 0, 0, 0, ErrInvalidRecordType
	}

	// Verify CRC over [type][durSeq][genStart][data].
	computed := crc32.Checksum(buf[6:totalLen], crc32Table)
	if computed != storedCRC {
		return nil, 0, 0, 0, 0, ErrCorruptedRecord
	}

	durSeq = binary.LittleEndian.Uint32(buf[7 : 7+recordSeqSize])
	genStart = binary.LittleEndian.Uint32(buf[7+recordSeqSize : recordHeaderSize])
	data = buf[recordHeaderSize:totalLen]
	return data, recordType, durSeq, genStart, totalLen, nil
}

// Positional context-AAD for encrypted WAL records.
//
// The barrier binds only the epoch into the AEAD AAD, so two records sealed
// under the same DEK and epoch authenticate identically no matter where they
// sit. An at-rest attacker with write access could therefore reorder, splice or
// truncate same-epoch records without the authentication tag noticing. Every
// encrypted WAL record additionally binds its POSITION — which WAL shard, which
// segment, and which record ordinal within that segment — into the AAD, so a
// record only authenticates at the exact place it was written.
//
// The layout is fixed-width and big-endian with no delimiters, so no two
// distinct positions can produce the same bytes:
//
//	"QRWAL1" (6) || WALShardIdx (8) || SegmentID (8) || RecordSeq (8)  = 30 bytes
//
// WALShardIdx is REQUIRED, not decorative: each of the DefaultShardCount WAL
// shards is its own directory with an INDEPENDENT %06d.log ID space, and all of
// them share ONE barrier, so (SegmentID, RecordSeq) alone is not unique across
// shards.
//
// The magic is a self-authenticating scheme tag. There is NO on-disk version
// marker and NO epoch-only fallback: the scheme is chosen by code alone, and a
// reader that fails to authenticate a record must NEVER retry without the
// context — that retry would be the vulnerability. WALs written before this
// scheme are not readable; see CHANGELOG ("Wipe existing data directories
// before upgrading").
const (
	// walContextMagic tags the context-AAD scheme. Changing it invalidates
	// every previously written encrypted WAL record by construction.
	walContextMagic = "QRWAL1"

	// walContextMagicSize is len(walContextMagic).
	walContextMagicSize = 6

	// walContextSize is the total fixed width of a record context.
	walContextSize = walContextMagicSize + 8 + 8 + 8
)

// walRecordContext fills dst with the positional context-AAD of a single
// logical WAL record and returns the filled prefix. It is the ONE place the
// context bytes are constructed, used by both the encrypt and the decrypt side
// so the two can never drift.
//
// dst must be at least walContextSize bytes; callers pass a reusable scratch
// buffer because this sits on the WAL write hot path and must not allocate.
//
// recordSeq is the ordinal of the LOGICAL record within its segment, 0-based:
// the number of logical records already committed to that segment. It is
// exactly the index at which segment.ReadAll returns the record during
// recovery, which is what lets the read side reproduce these bytes. Chunking a
// record across blocks does not affect it — ReadAll assembles First/Middle/Last
// chunks into one entry before appending.
func walRecordContext(dst []byte, shardIdx, segID, recordSeq uint64) []byte {
	_ = dst[walContextSize-1] // bounds check elimination hint
	copy(dst[0:walContextMagicSize], walContextMagic)
	binary.BigEndian.PutUint64(dst[6:14], shardIdx)
	binary.BigEndian.PutUint64(dst[14:22], segID)
	binary.BigEndian.PutUint64(dst[22:30], recordSeq)
	return dst[:walContextSize]
}

// isValidRecordType returns true if the given byte is a recognized chunk type.
// The accepted range is intentionally tight (RecordFull..RecordLast): a chunk
// type outside it is treated as corruption, which stops recovery at the bad
// record (a WAL is valid only as a prefix). Widening the set to include a
// never-written type would make a random/forged header byte more likely to
// pass the pre-CRC type check.
func isValidRecordType(t byte) bool {
	return t >= RecordFull && t <= RecordSyncMark
}
