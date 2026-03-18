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
// WAL records use a CRC32 + length + type + data format within fixed-size blocks,
// following the LevelDB/Pebble log format. Records that do not fit within a single
// block are split into multiple chunks (First, Middle, Last). CRC32 uses the
// Castagnoli polynomial for hardware acceleration on amd64.
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
	// RecordEpoch marks a key rotation epoch boundary (always plaintext).
	RecordEpoch byte = 5
)

// recordHeaderSize is the fixed overhead per chunk:
// [CRC32:4] [Length:2] [Type:1] = 7 bytes.
const recordHeaderSize = 7

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
// Format: [CRC32:4][Length:2][Type:1][Data:N]
// CRC32 covers [Type:1][Data:N].
// Returns the number of bytes written and any error.
func encodeRecord(buf []byte, recordType byte, data []byte) (int, error) {
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

	// Write type byte first so we can compute CRC over [type][data].
	buf[6] = recordType

	// Copy payload.
	copy(buf[recordHeaderSize:], data)

	// CRC32 covers type byte and payload: buf[6..6+1+dataLen).
	checksum := crc32.Checksum(buf[6:recordHeaderSize+dataLen], crc32Table)
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	// Length is the payload length (excludes the header itself).
	binary.LittleEndian.PutUint16(buf[4:6], uint16(dataLen))

	return totalLen, nil
}

// decodeRecord reads a single chunk from buf.
// Returns the payload data (a sub-slice of buf, not a copy), the chunk type,
// the total number of bytes consumed, and any error.
func decodeRecord(buf []byte) (data []byte, recordType byte, n int, err error) {
	if len(buf) < recordHeaderSize {
		return nil, 0, 0, ErrCorruptedRecord
	}

	// Parse header fields.
	storedCRC := binary.LittleEndian.Uint32(buf[0:4])
	dataLen := int(binary.LittleEndian.Uint16(buf[4:6]))
	recordType = buf[6]

	totalLen := recordHeaderSize + dataLen
	if totalLen > len(buf) {
		return nil, 0, 0, ErrCorruptedRecord
	}

	if !isValidRecordType(recordType) {
		return nil, 0, 0, ErrInvalidRecordType
	}

	// Verify CRC over [type][data].
	computed := crc32.Checksum(buf[6:totalLen], crc32Table)
	if computed != storedCRC {
		return nil, 0, 0, ErrCorruptedRecord
	}

	data = buf[recordHeaderSize:totalLen]
	return data, recordType, totalLen, nil
}

// isValidRecordType returns true if the given byte is a recognized chunk type.
func isValidRecordType(t byte) bool {
	return t >= RecordFull && t <= RecordEpoch
}
