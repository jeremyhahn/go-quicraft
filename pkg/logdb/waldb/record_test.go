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
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestEncodeDecodeRoundTrip verifies round-trip encoding and decoding of an
// empty, a small and a larger payload (the larger one checks that the CRC
// and length fields handle multi-byte payloads), each encoded into a buffer
// of exactly the right size.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	large := make([]byte, 4096)
	for i := range large {
		large[i] = byte(i % 256)
	}
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"small", []byte("hello world")},
		{"large", large},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, recordHeaderSize+len(tc.data))
			n, err := encodeRecord(buf, RecordFull, 0, 0, tc.data)
			if err != nil {
				t.Fatalf("encodeRecord: unexpected error: %v", err)
			}
			if n != len(buf) {
				t.Fatalf("encodeRecord: wrote %d bytes, want %d", n, len(buf))
			}
			decoded, recType, _, _, consumed, err := decodeRecord(buf[:n])
			if err != nil {
				t.Fatalf("decodeRecord: unexpected error: %v", err)
			}
			if recType != RecordFull {
				t.Fatalf("decodeRecord: type = %d, want %d", recType, RecordFull)
			}
			if consumed != n {
				t.Fatalf("decodeRecord: consumed %d bytes, want %d", consumed, n)
			}
			if !bytes.Equal(decoded, tc.data) {
				t.Fatalf("decodeRecord: data = %q, want %q", decoded, tc.data)
			}
		})
	}
}

// TestEncodeBufferTooSmall verifies that encodeRecord returns ErrBufferTooSmall
// when the buffer cannot hold the header plus data.
func TestEncodeBufferTooSmall(t *testing.T) {
	data := []byte("too big for this buffer")
	buf := make([]byte, recordHeaderSize) // no room for data

	_, err := encodeRecord(buf, RecordFull, 0, 0, data)
	if err != ErrBufferTooSmall {
		t.Fatalf("encodeRecord: error = %v, want %v", err, ErrBufferTooSmall)
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
	}

	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte("type test")
			buf := make([]byte, recordHeaderSize+len(data))

			n, err := encodeRecord(buf, tc.typ, 0, 0, data)
			if err != nil {
				t.Fatalf("encodeRecord(%s): unexpected error: %v", tc.name, err)
			}

			_, recType, _, _, _, err := decodeRecord(buf[:n])
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

	_, err := encodeRecord(buf, 0, 0, 0, data)
	if err != ErrInvalidRecordType {
		t.Fatalf("encodeRecord: error = %v, want %v", err, ErrInvalidRecordType)
	}

	_, err = encodeRecord(buf, 6, 0, 0, data)
	if err != ErrInvalidRecordType {
		t.Fatalf("encodeRecord: error = %v, want %v", err, ErrInvalidRecordType)
	}

	_, err = encodeRecord(buf, 255, 0, 0, data)
	if err != ErrInvalidRecordType {
		t.Fatalf("encodeRecord: error = %v, want %v", err, ErrInvalidRecordType)
	}
}

// TestDecodeBufferTooSmall verifies that decodeRecord returns ErrCorruptedRecord
// when the buffer is shorter than the record header.
func TestDecodeBufferTooSmall(t *testing.T) {
	buf := make([]byte, recordHeaderSize-1)

	_, _, _, _, _, err := decodeRecord(buf)
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

	_, err := encodeRecord(buf, RecordFull, 0, 0, data)
	if err != nil {
		t.Fatalf("encodeRecord: unexpected error: %v", err)
	}

	// Truncate the buffer so the payload is incomplete.
	_, _, _, _, _, err = decodeRecord(buf[:recordHeaderSize+len(data)-3])
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

	_, _, _, _, _, err := decodeRecord(buf)
	if err != ErrInvalidRecordType {
		t.Fatalf("decodeRecord: error = %v, want %v", err, ErrInvalidRecordType)
	}
}

// TestRecordHeaderSize confirms the header size constant matches the spec:
// [CRC32:4] + [Length:2] + [Type:1] + [DurSeq:4] + [GenStart:4] = 15 bytes.
//
// The constant is pinned rather than derived because it IS the on-disk format:
// changing it leaves every already-written segment unreadable. Under the
// pre-release pinning policy that is a permitted change and walFormatVersion
// does NOT move for it — the framing changes in place at version 1 and every
// dev directory is wiped. What this test protects is that the change is
// deliberate: an accidental edit to recordHeaderSize would silently make this
// build unable to read segments it wrote an hour ago.
func TestRecordHeaderSize(t *testing.T) {
	if recordHeaderSize != 15 {
		t.Fatalf("recordHeaderSize = %d, want 15", recordHeaderSize)
	}
}

// TestRecordGenerationStartRoundTrip pins that the generation start is
// carried per chunk, covered by the CRC, and decoded exactly.
func TestRecordGenerationStartRoundTrip(t *testing.T) {
	buf := make([]byte, 64)
	payload := []byte("payload")
	for _, genStart := range []uint32{0, 1, 4096, 1<<32 - 1} {
		n, err := encodeRecord(buf, RecordFull, 3, genStart, payload)
		if err != nil {
			t.Fatalf("encodeRecord(genStart=%d): %v", genStart, err)
		}
		data, recType, seq, got, consumed, err := decodeRecord(buf[:n])
		if err != nil {
			t.Fatalf("decodeRecord(genStart=%d): %v", genStart, err)
		}
		if got != genStart || seq != 3 || recType != RecordFull || consumed != n || string(data) != "payload" {
			t.Fatalf("round trip = (gen %d seq %d type %d n %d %q), want (gen %d seq 3 type Full n %d payload)",
				got, seq, recType, consumed, data, genStart, n)
		}
		// A flipped bit in the generation start must fail the CRC rather than
		// move the acknowledged end.
		buf[7+recordSeqSize] ^= 0x01
		if _, _, _, _, _, err := decodeRecord(buf[:n]); err == nil {
			t.Fatalf("a corrupted generation start decoded cleanly at genStart=%d", genStart)
		}
		buf[7+recordSeqSize] ^= 0x01
	}
}

// crc32Checksum is a test helper that computes a CRC32 checksum using the
// Castagnoli table.
func crc32Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crc32Table)
}

// TestDecodeRecord_CRCMismatch reaches ErrCorruptedRecord the way the medium
// actually produces it: a bit flip in a well-formed record.
//
// The other ErrCorruptedRecord tests reach it through the FRAMING checks — a
// buffer shorter than a header, and a length field pointing past the buffer —
// which are reached before the checksum is ever computed. Neither exercises the
// CRC comparison, so the whole reason the checksum exists had no unit-level
// test at all: replacing the comparison with `computed != computed` would have
// left this file green.
//
// Every byte of the checksummed region is flipped in turn, so the test also
// pins WHAT the CRC covers. The header's own length and CRC fields sit outside
// it (a flip there is caught by the framing checks or by the comparison itself),
// but type, durability sequence and payload must every one of them be inside:
// a durability sequence outside the covered region could be flipped to move a
// record into another generation, which is exactly the evidence recovery uses
// to decide whether truncating destroys acknowledged data.
func TestDecodeRecord_CRCMismatch(t *testing.T) {
	data := []byte("payload-under-checksum")
	buf := make([]byte, recordHeaderSize+len(data))
	n, err := encodeRecord(buf, RecordFull, 0x11223344, 0, data)
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	if _, _, _, _, _, err := decodeRecord(buf[:n]); err != nil {
		t.Fatalf("fixture: the intact record does not decode: %v", err)
	}

	// buf[6:] is [Type][DurSeq][Data] — the region the checksum covers.
	for i := 6; i < n; i++ {
		corrupt := make([]byte, n)
		copy(corrupt, buf[:n])
		corrupt[i] ^= 0x01

		_, _, _, _, _, err := decodeRecord(corrupt)
		if err == nil {
			t.Fatalf("a single flipped bit at offset %d decoded as a valid record: "+
				"that byte is not covered by the checksum, so the medium can change "+
				"it without recovery ever noticing", i)
		}
		// Offset 6 is the type byte, whose validity is checked before the CRC.
		if i == 6 {
			if err != ErrInvalidRecordType && err != ErrCorruptedRecord {
				t.Fatalf("flipping the type byte gave %v, want ErrInvalidRecordType "+
					"or ErrCorruptedRecord", err)
			}
			continue
		}
		if err != ErrCorruptedRecord {
			t.Fatalf("flipping the byte at offset %d gave %v, want ErrCorruptedRecord",
				i, err)
		}
	}
}

// TestRecordDurabilitySequenceOnDisk pins the durability sequence's on-disk
// position and encoding.
//
// It is the value recovery reads to decide whether truncating a damaged segment
// destroys records this node already acknowledged, and nothing asserted its
// concrete bytes: every other test either round-trips it through
// encode/decode — which agrees with itself whatever the layout is — or discards
// it at the call site. Moving the field, or writing it big-endian, would leave
// this package green and make every existing WAL's evidence read as a different
// generation.
func TestRecordDurabilitySequenceOnDisk(t *testing.T) {
	const durSeq = uint32(0x0A0B0C0D)
	data := []byte("x")
	buf := make([]byte, recordHeaderSize+len(data))
	if _, err := encodeRecord(buf, RecordFull, durSeq, 0, data); err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}

	// [CRC32:4][Length:2][Type:1][DurSeq:4][Data:N], little-endian.
	if got := buf[7:11]; !bytes.Equal(got, []byte{0x0D, 0x0C, 0x0B, 0x0A}) {
		t.Fatalf("durability sequence on disk = %x at buf[7:11], want 0d0c0b0a "+
			"(0x%08X little-endian at offset 7)", got, durSeq)
	}
	if got := binary.LittleEndian.Uint16(buf[4:6]); got != uint16(len(data)) {
		t.Fatalf("length field = %d, want %d: the durability sequence is not "+
			"counted as payload", got, len(data))
	}

	_, _, gotSeq, _, consumed, err := decodeRecord(buf)
	if err != nil {
		t.Fatalf("decodeRecord: %v", err)
	}
	if gotSeq != durSeq {
		t.Fatalf("decoded durability sequence = 0x%08X, want 0x%08X", gotSeq, durSeq)
	}
	if consumed != recordHeaderSize+len(data) {
		t.Fatalf("consumed = %d, want %d", consumed, recordHeaderSize+len(data))
	}
}

// TestEntryOverheadBytesTracksTheRecordHeader pins the two constants that must
// agree across a package boundary they cannot see each other over.
//
// logdb.EntryOverheadBytes is the per-entry overhead every LogDB charges
// against the IterateEntries maxSize budget, and its documentation states it
// MUST equal waldb's entryRecordHeaderSize — the bytes this package actually
// writes per entry. They are separate constants only because pkg/logdb cannot
// import this one (waldb imports logdb, so the dependency runs the other way),
// which means the compiler cannot hold them together and only a test can.
//
// Drift is not cosmetic. waldb charges what it really wrote (indexEntry.Size)
// while memdb and every caller computing a budget charge EntryOverheadBytes, so
// the same maxSize admits a different set of entries depending on the backend —
// and a replication batch sized against the wrong figure either overshoots the
// transport's limit or ships short reads forever.
//
// This assertion lives here because this is the only package where both names
// are in scope.
func TestEntryOverheadBytesTracksTheRecordHeader(t *testing.T) {
	if entryRecordHeaderSize != logdb.EntryOverheadBytes {
		t.Fatalf("waldb.entryRecordHeaderSize = %d but logdb.EntryOverheadBytes = %d.\n\n"+
			"These must be equal. waldb charges the bytes it actually wrote, every "+
			"other LogDB and every caller computing a size budget charges "+
			"logdb.EntryOverheadBytes, so while they differ the same maxSize admits "+
			"a different set of entries depending on which backend answers. Whichever "+
			"one changed, change the other in the same commit.",
			entryRecordHeaderSize, logdb.EntryOverheadBytes)
	}
}
