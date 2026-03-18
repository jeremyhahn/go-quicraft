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

package waldb

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// TestEncodeDecodeSmallRecord verifies round-trip encoding and decoding of a
// small payload that fits within a single chunk.
func TestEncodeDecodeSmallRecord(t *testing.T) {
	data := []byte("hello world")
	buf := make([]byte, recordHeaderSize+len(data))

	n, err := encodeRecord(buf, RecordFull, data)
	if err != nil {
		t.Fatalf("encodeRecord: unexpected error: %v", err)
	}
	if n != recordHeaderSize+len(data) {
		t.Fatalf("encodeRecord: wrote %d bytes, want %d", n, recordHeaderSize+len(data))
	}

	decoded, recType, consumed, err := decodeRecord(buf)
	if err != nil {
		t.Fatalf("decodeRecord: unexpected error: %v", err)
	}
	if recType != RecordFull {
		t.Fatalf("decodeRecord: type = %d, want %d", recType, RecordFull)
	}
	if consumed != n {
		t.Fatalf("decodeRecord: consumed %d bytes, want %d", consumed, n)
	}
	if string(decoded) != "hello world" {
		t.Fatalf("decodeRecord: data = %q, want %q", decoded, "hello world")
	}
}

// TestEncodeDecodeLargeRecord verifies round-trip encoding and decoding of a
// larger payload to ensure CRC and length fields handle multi-byte payloads.
func TestEncodeDecodeLargeRecord(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	buf := make([]byte, recordHeaderSize+len(data))

	n, err := encodeRecord(buf, RecordFull, data)
	if err != nil {
		t.Fatalf("encodeRecord: unexpected error: %v", err)
	}

	decoded, recType, consumed, err := decodeRecord(buf[:n])
	if err != nil {
		t.Fatalf("decodeRecord: unexpected error: %v", err)
	}
	if recType != RecordFull {
		t.Fatalf("decodeRecord: type = %d, want %d", recType, RecordFull)
	}
	if consumed != n {
		t.Fatalf("decodeRecord: consumed = %d, want %d", consumed, n)
	}
	if len(decoded) != len(data) {
		t.Fatalf("decodeRecord: data length = %d, want %d", len(decoded), len(data))
	}
	for i := range decoded {
		if decoded[i] != data[i] {
			t.Fatalf("decodeRecord: mismatch at byte %d: got %d, want %d", i, decoded[i], data[i])
		}
	}
}

// TestEncodeBufferTooSmall verifies that encodeRecord returns ErrBufferTooSmall
// when the buffer cannot hold the header plus data.
func TestEncodeBufferTooSmall(t *testing.T) {
	data := []byte("too big for this buffer")
	buf := make([]byte, recordHeaderSize) // no room for data

	_, err := encodeRecord(buf, RecordFull, data)
	if err != ErrBufferTooSmall {
		t.Fatalf("encodeRecord: error = %v, want %v", err, ErrBufferTooSmall)
	}
}

// TestEncodeBufferExactFit verifies encoding works when the buffer is exactly
// the right size.
func TestEncodeBufferExactFit(t *testing.T) {
	data := []byte("exact")
	buf := make([]byte, recordHeaderSize+len(data))

	n, err := encodeRecord(buf, RecordFull, data)
	if err != nil {
		t.Fatalf("encodeRecord: unexpected error: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("encodeRecord: wrote %d, want %d", n, len(buf))
	}
}

// TestEncodeEmptyData verifies that encoding a zero-length payload succeeds.
func TestEncodeEmptyData(t *testing.T) {
	buf := make([]byte, recordHeaderSize)

	n, err := encodeRecord(buf, RecordFull, nil)
	if err != nil {
		t.Fatalf("encodeRecord: unexpected error: %v", err)
	}
	if n != recordHeaderSize {
		t.Fatalf("encodeRecord: wrote %d bytes, want %d", n, recordHeaderSize)
	}

	decoded, recType, consumed, err := decodeRecord(buf[:n])
	if err != nil {
		t.Fatalf("decodeRecord: unexpected error: %v", err)
	}
	if recType != RecordFull {
		t.Fatalf("decodeRecord: type = %d, want %d", recType, RecordFull)
	}
	if consumed != recordHeaderSize {
		t.Fatalf("decodeRecord: consumed = %d, want %d", consumed, recordHeaderSize)
	}
	if len(decoded) != 0 {
		t.Fatalf("decodeRecord: data length = %d, want 0", len(decoded))
	}
}

// TestEncodeEmptySliceData verifies that encoding a zero-length slice payload
// succeeds (distinct from nil).
func TestEncodeEmptySliceData(t *testing.T) {
	buf := make([]byte, recordHeaderSize)

	n, err := encodeRecord(buf, RecordFull, []byte{})
	if err != nil {
		t.Fatalf("encodeRecord: unexpected error: %v", err)
	}
	if n != recordHeaderSize {
		t.Fatalf("encodeRecord: wrote %d bytes, want %d", n, recordHeaderSize)
	}

	decoded, _, _, err := decodeRecord(buf[:n])
	if err != nil {
		t.Fatalf("decodeRecord: unexpected error: %v", err)
	}
	if len(decoded) != 0 {
		t.Fatalf("decodeRecord: data length = %d, want 0", len(decoded))
	}
}

// TestAllRecordTypes verifies that all valid record types can be encoded and
// decoded without error.
func TestAllRecordTypes(t *testing.T) {
	types := []struct {
		name string
		typ  byte
	}{
		{"Full", RecordFull},
		{"First", RecordFirst},
		{"Middle", RecordMiddle},
		{"Last", RecordLast},
		{"Epoch", RecordEpoch},
	}

	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte("type test")
			buf := make([]byte, recordHeaderSize+len(data))

			n, err := encodeRecord(buf, tc.typ, data)
			if err != nil {
				t.Fatalf("encodeRecord(%s): unexpected error: %v", tc.name, err)
			}

			_, recType, _, err := decodeRecord(buf[:n])
			if err != nil {
				t.Fatalf("decodeRecord(%s): unexpected error: %v", tc.name, err)
			}
			if recType != tc.typ {
				t.Fatalf("decodeRecord(%s): type = %d, want %d", tc.name, recType, tc.typ)
			}
		})
	}
}

// TestInvalidRecordType verifies that encoding with an unrecognized type byte
// returns ErrInvalidRecordType.
func TestInvalidRecordType(t *testing.T) {
	buf := make([]byte, recordHeaderSize+10)
	data := []byte("bad type")

	_, err := encodeRecord(buf, 0, data)
	if err != ErrInvalidRecordType {
		t.Fatalf("encodeRecord: error = %v, want %v", err, ErrInvalidRecordType)
	}

	_, err = encodeRecord(buf, 6, data)
	if err != ErrInvalidRecordType {
		t.Fatalf("encodeRecord: error = %v, want %v", err, ErrInvalidRecordType)
	}

	_, err = encodeRecord(buf, 255, data)
	if err != ErrInvalidRecordType {
		t.Fatalf("encodeRecord: error = %v, want %v", err, ErrInvalidRecordType)
	}
}

// TestDecodeBufferTooSmall verifies that decodeRecord returns ErrCorruptedRecord
// when the buffer is shorter than the record header.
func TestDecodeBufferTooSmall(t *testing.T) {
	buf := make([]byte, recordHeaderSize-1)

	_, _, _, err := decodeRecord(buf)
	if err != ErrCorruptedRecord {
		t.Fatalf("decodeRecord: error = %v, want %v", err, ErrCorruptedRecord)
	}
}

// TestDecodePayloadTruncated verifies that decodeRecord returns
// ErrCorruptedRecord when the length field indicates more data than the buffer
// contains.
func TestDecodePayloadTruncated(t *testing.T) {
	data := []byte("truncated")
	buf := make([]byte, recordHeaderSize+len(data))

	_, err := encodeRecord(buf, RecordFull, data)
	if err != nil {
		t.Fatalf("encodeRecord: unexpected error: %v", err)
	}

	// Truncate the buffer so the payload is incomplete.
	_, _, _, err = decodeRecord(buf[:recordHeaderSize+len(data)-3])
	if err != ErrCorruptedRecord {
		t.Fatalf("decodeRecord: error = %v, want %v", err, ErrCorruptedRecord)
	}
}

// TestDecodeBadTypeInRecord verifies that decodeRecord returns
// ErrInvalidRecordType when the type byte in the buffer is unrecognized,
// even if the CRC would otherwise be valid.
func TestDecodeBadTypeInRecord(t *testing.T) {
	// Manually construct a record with an invalid type byte and a valid CRC
	// over that invalid type.
	data := []byte("test")
	buf := make([]byte, recordHeaderSize+len(data))

	// Write invalid type.
	buf[6] = 0
	copy(buf[recordHeaderSize:], data)

	// Compute CRC over [type][data].
	crc := crc32Checksum(buf[6 : recordHeaderSize+len(data)])
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(data)))

	_, _, _, err := decodeRecord(buf)
	if err != ErrInvalidRecordType {
		t.Fatalf("decodeRecord: error = %v, want %v", err, ErrInvalidRecordType)
	}
}

// TestRecordHeaderSize confirms the header size constant matches the spec:
// [CRC32:4] + [Length:2] + [Type:1] = 7 bytes.
func TestRecordHeaderSize(t *testing.T) {
	if recordHeaderSize != 7 {
		t.Fatalf("recordHeaderSize = %d, want 7", recordHeaderSize)
	}
}

// crc32Checksum is a test helper that computes a CRC32 checksum using the
// Castagnoli table.
func crc32Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crc32Table)
}
