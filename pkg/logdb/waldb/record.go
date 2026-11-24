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

// Package waldb implements write-ahead log storage for the quicraft Raft engine, in LevelDB-style blocks with durability sequences; see segment.ReadAll.
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
	// RecordSyncMark is a header-only chunk appended after every completed fdatasync, witnessing the segment's final generation.
	RecordSyncMark byte = 5
)

// Each chunk carries durSeq (completed Syncs) and genStart (acknowledged end), so recovery can tell a torn write from acknowledged-data damage; see docs/log-storage/engineering.md and segment.ReadAll.
const (
	// recordSeqSize is the width of the durability sequence on disk.
	recordSeqSize = 4

	// recordGenStartSize is the width of the generation-start offset on disk.
	// It bounds a segment file at 4 GiB - 1 (maxSegmentFileSize), which
	// validateOpenOptions enforces.
	recordGenStartSize = 4

	// maxDurabilitySeq is the largest durability sequence a chunk can carry; segment.CanFit stops writes before it wraps.
	maxDurabilitySeq = uint32(1<<32 - 1)
)

// recordHeaderSize is the fixed overhead per chunk:
// [CRC32:4] [Length:2] [Type:1] [DurSeq:4] [GenStart:4] = 15 bytes.
const recordHeaderSize = 7 + recordSeqSize + recordGenStartSize

// syncMarkReserve is the room segment.CanFit keeps below maxFileSize for the next sync mark and its padding.
const syncMarkReserve = 2*recordHeaderSize - 1

// maxSegmentFileSize is the largest MaxFileSize a DB accepts: the generation
// start is a file offset stored in recordGenStartSize bytes, so a segment must
// not grow past what that width can name.
const maxSegmentFileSize = int64(1<<(8*recordGenStartSize) - 1)

// maxRecordDataLen is the maximum payload length per chunk, limited by the
// 2-byte length field.
const maxRecordDataLen = (1 << 16) - 1

// crc32Table is the hardware-accelerated Castagnoli table; CRC32-C detects accidental corruption only, tamper protection is the seal.Barrier's.
var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// encodeRecord writes one chunk to buf as [CRC32:4][Length:2][Type:1][DurSeq:4][GenStart:4][Data:N], the CRC covering Type through Data.
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

// decodeRecord reads one chunk from buf, returning its data (a sub-slice of buf), type, durability sequence, generation start and bytes consumed.
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

// Encrypted WAL records bind their position into the AAD as "QRWAL1" || WALShardIdx || SegmentID || RecordSeq (30 bytes); a failed authentication is never retried without it.
const (
	// walContextMagic tags the context-AAD scheme.
	walContextMagic = "QRWAL1"

	// walContextMagicSize is len(walContextMagic).
	walContextMagicSize = 6

	// walContextSize is the total fixed width of a record context.
	walContextSize = walContextMagicSize + 8 + 8 + 8
)

// walRecordContext fills dst (at least walContextSize bytes) with the context-AAD of a logical record; recordSeq is its 0-based ordinal in the segment.
func walRecordContext(dst []byte, shardIdx, segID, recordSeq uint64) []byte {
	_ = dst[walContextSize-1] // bounds check elimination hint
	copy(dst[0:walContextMagicSize], walContextMagic)
	binary.BigEndian.PutUint64(dst[6:14], shardIdx)
	binary.BigEndian.PutUint64(dst[14:22], segID)
	binary.BigEndian.PutUint64(dst[22:30], recordSeq)
	return dst[:walContextSize]
}

// isValidRecordType reports whether b is a chunk type the writer emits (RecordFull..RecordSyncMark).
func isValidRecordType(t byte) bool {
	return t >= RecordFull && t <= RecordSyncMark
}
