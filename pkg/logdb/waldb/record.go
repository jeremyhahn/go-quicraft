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
// WAL records use a CRC32 + length + type + durability-sequence + data format
// within fixed-size blocks, following the LevelDB/Pebble log format. Records
// that do not fit within a single block are split into multiple chunks (First,
// Middle, Last). CRC32 uses the Castagnoli polynomial for hardware acceleration
// on amd64. The durability sequence is what lets recovery tell an interrupted
// write from corruption of acknowledged data; see the durability-sequence
// commentary below and segment.ReadAll.
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
const (
	// recordSeqSize is the width of the durability sequence on disk.
	recordSeqSize = 4

	// maxDurabilitySeq is the largest durability sequence a chunk can carry.
	// segment.CanFit refuses to write past it so the counter can never wrap and
	// present an OLD generation as the newest one, which would invert the proof
	// above. Reaching it takes 2^32 fdatasyncs into a single segment, roughly
	// three orders of magnitude more than the record count any legal segment
	// size admits, so the guard is a proof obligation rather than a live path.
	maxDurabilitySeq = uint32(1<<32 - 1)
)

// recordHeaderSize is the fixed overhead per chunk:
// [CRC32:4] [Length:2] [Type:1] [DurSeq:4] = 11 bytes.
const recordHeaderSize = 7 + recordSeqSize

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
// Format: [CRC32:4][Length:2][Type:1][DurSeq:4][Data:N]
// CRC32 covers [Type:1][DurSeq:4][Data:N].
// Returns the number of bytes written and any error.
//
// The durability sequence sits INSIDE the checksummed region deliberately: it is
// the value recovery uses to decide whether truncating is safe, so a flipped bit
// in it must invalidate the chunk rather than silently move the chunk into
// another generation. It is placed after the type byte so the covered region
// stays one contiguous slice and the CRC is still computed in a single pass.
func encodeRecord(buf []byte, recordType byte, durSeq uint32, data []byte) (int, error) {
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

	// Write type and sequence first so we can compute CRC over
	// [type][durSeq][data] as one range.
	buf[6] = recordType
	binary.LittleEndian.PutUint32(buf[7:recordHeaderSize], durSeq)

	// Copy payload.
	copy(buf[recordHeaderSize:], data)

	// CRC32 covers type byte, durability sequence and payload.
	checksum := crc32.Checksum(buf[6:recordHeaderSize+dataLen], crc32Table)
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	// Length is the payload length (excludes the header itself).
	binary.LittleEndian.PutUint16(buf[4:6], uint16(dataLen))

	return totalLen, nil
}

// decodeRecord reads a single chunk from buf.
// Returns the payload data (a sub-slice of buf, not a copy), the chunk type,
// the chunk's durability sequence, the total number of bytes consumed, and any
// error.
func decodeRecord(buf []byte) (data []byte, recordType byte, durSeq uint32, n int, err error) {
	if len(buf) < recordHeaderSize {
		return nil, 0, 0, 0, ErrCorruptedRecord
	}

	// Parse header fields.
	storedCRC := binary.LittleEndian.Uint32(buf[0:4])
	dataLen := int(binary.LittleEndian.Uint16(buf[4:6]))
	recordType = buf[6]

	totalLen := recordHeaderSize + dataLen
	if totalLen > len(buf) {
		return nil, 0, 0, 0, ErrCorruptedRecord
	}

	if !isValidRecordType(recordType) {
		return nil, 0, 0, 0, ErrInvalidRecordType
	}

	// Verify CRC over [type][durSeq][data].
	computed := crc32.Checksum(buf[6:totalLen], crc32Table)
	if computed != storedCRC {
		return nil, 0, 0, 0, ErrCorruptedRecord
	}

	durSeq = binary.LittleEndian.Uint32(buf[7:recordHeaderSize])
	data = buf[recordHeaderSize:totalLen]
	return data, recordType, durSeq, totalLen, nil
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
	return t >= RecordFull && t <= RecordLast
}
