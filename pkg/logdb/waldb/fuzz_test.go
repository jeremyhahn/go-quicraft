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

//go:build quicraft_fuzz

package waldb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// buildValidRecord creates a valid encoded WAL record from the given type and
// payload. Returns the complete record bytes including CRC, length, and type
// header fields. This helper is used to seed fuzz corpora with known-good
// records.
func buildValidRecord(recordType byte, payload []byte) []byte {
	buf := make([]byte, recordHeaderSize+len(payload))
	n, err := encodeRecord(buf, recordType, payload)
	if err != nil {
		panic("buildValidRecord: " + err.Error())
	}
	return buf[:n]
}

// buildRecordWithBadCRC creates a record with a valid structure but an
// intentionally incorrect CRC. This forces the decode path to detect
// corruption and return an error rather than succeeding.
func buildRecordWithBadCRC(recordType byte, payload []byte) []byte {
	buf := buildValidRecord(recordType, payload)
	// Flip every bit in the stored CRC to guarantee mismatch.
	buf[0] ^= 0xFF
	buf[1] ^= 0xFF
	buf[2] ^= 0xFF
	buf[3] ^= 0xFF
	return buf
}

// buildRecordRaw manually constructs a record buffer with the given fields.
// This bypasses encodeRecord to create records with arbitrary type bytes that
// encodeRecord would reject (e.g., type 0 or 255).
func buildRecordRaw(recordType byte, payload []byte) []byte {
	buf := make([]byte, recordHeaderSize+len(payload))
	buf[6] = recordType
	copy(buf[recordHeaderSize:], payload)
	crc := crc32.Checksum(buf[6:recordHeaderSize+len(payload)], crc32Table)
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(payload)))
	return buf
}

// FuzzRecordRoundTrip verifies that encode followed by decode is an identity
// operation for all valid record types and arbitrary payloads. The fuzzer
// generates the payload bytes; the test iterates over all chunk types.
func FuzzRecordRoundTrip(f *testing.F) {
	// Seed: empty payload.
	f.Add([]byte{})
	// Seed: small ASCII payload.
	f.Add([]byte("hello"))
	// Seed: single byte.
	f.Add([]byte{0x00})
	// Seed: payload at maximum 2-byte length boundary minus one.
	f.Add(make([]byte, 255))
	// Seed: payload that fills a typical WAL block.
	f.Add(make([]byte, 32*1024-recordHeaderSize))

	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > maxRecordDataLen {
			t.Skip("payload exceeds max record data length")
		}

		types := []byte{RecordFull, RecordFirst, RecordMiddle, RecordLast, RecordEpoch}
		for _, rt := range types {
			buf := make([]byte, recordHeaderSize+len(payload))

			n, err := encodeRecord(buf, rt, payload)
			if err != nil {
				t.Fatalf("encodeRecord(type=%d, len=%d): %v", rt, len(payload), err)
			}

			decoded, recType, consumed, err := decodeRecord(buf[:n])
			if err != nil {
				t.Fatalf("decodeRecord(type=%d): %v", rt, err)
			}
			if recType != rt {
				t.Fatalf("round-trip type: got %d, want %d", recType, rt)
			}
			if consumed != n {
				t.Fatalf("round-trip consumed: got %d, want %d", consumed, n)
			}
			if !bytes.Equal(decoded, payload) {
				t.Fatalf("round-trip payload mismatch: decoded len=%d, original len=%d", len(decoded), len(payload))
			}
		}
	})
}

// FuzzRecordDecodeCrash feeds arbitrary bytes to decodeRecord. The decoder
// must never panic regardless of input. It must return a non-nil error for
// any malformed input.
func FuzzRecordDecodeCrash(f *testing.F) {
	// Seed: empty input.
	f.Add([]byte{})
	// Seed: less than header size.
	f.Add([]byte{0x01, 0x02, 0x03})
	// Seed: exactly header size, all zeros.
	f.Add(make([]byte, recordHeaderSize))
	// Seed: valid minimal record (empty payload, RecordFull).
	f.Add(buildValidRecord(RecordFull, nil))
	// Seed: valid record with small payload.
	f.Add(buildValidRecord(RecordFull, []byte("fuzz")))
	// Seed: record with bad CRC.
	f.Add(buildRecordWithBadCRC(RecordFull, []byte("bad crc")))
	// Seed: valid CRC but invalid type byte 0.
	f.Add(buildRecordRaw(0, []byte("type zero")))
	// Seed: valid CRC but invalid type byte 255.
	f.Add(buildRecordRaw(255, []byte("type max")))
	// Seed: header claims large length but buffer is small.
	seed := make([]byte, recordHeaderSize)
	binary.LittleEndian.PutUint16(seed[4:6], 0xFFFF) // max length
	seed[6] = RecordFull
	binary.LittleEndian.PutUint32(seed[0:4], crc32.Checksum(seed[6:7], crc32Table))
	f.Add(seed)
	// Seed: valid RecordFirst with payload.
	f.Add(buildValidRecord(RecordFirst, []byte("first chunk")))
	// Seed: valid RecordLast with payload.
	f.Add(buildValidRecord(RecordLast, []byte("last chunk")))
	// Seed: valid RecordEpoch with payload.
	f.Add(buildValidRecord(RecordEpoch, []byte("epoch marker")))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic. Errors are expected and acceptable.
		decoded, recType, consumed, err := decodeRecord(data)
		if err != nil {
			// Error is the expected path for most fuzz inputs.
			return
		}
		// If decode succeeded, verify invariants.
		if !isValidRecordType(recType) {
			t.Fatalf("decodeRecord returned invalid type %d without error", recType)
		}
		if consumed < recordHeaderSize {
			t.Fatalf("consumed %d < recordHeaderSize %d", consumed, recordHeaderSize)
		}
		if consumed > len(data) {
			t.Fatalf("consumed %d > input length %d", consumed, len(data))
		}
		if len(decoded) != consumed-recordHeaderSize {
			t.Fatalf("decoded length %d != consumed-header %d", len(decoded), consumed-recordHeaderSize)
		}
	})
}

// FuzzEntryRecordRoundTrip creates a WAL entry payload with fuzz-generated
// field values, encodes it as a WAL record, decodes the record, then
// deserializes the entry payload and verifies all fields match the original.
func FuzzEntryRecordRoundTrip(f *testing.F) {
	// Seed: minimal entry with no command.
	f.Add(uint64(1), uint64(1), uint64(1), uint64(1), uint64(0), []byte{})
	// Seed: entry with small command.
	f.Add(uint64(100), uint64(200), uint64(42), uint64(5), uint64(1), []byte("set key value"))
	// Seed: entry with zero-valued fields.
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), []byte{})
	// Seed: entry with max-ish field values.
	f.Add(uint64(0xFFFFFFFFFFFFFFFF), uint64(0xFFFFFFFFFFFFFFFF), uint64(0xFFFFFFFFFFFFFFFF),
		uint64(0xFFFFFFFFFFFFFFFF), uint64(0xFFFFFFFFFFFFFFFF), []byte{0xFF})
	// Seed: entry with binary command payload.
	f.Add(uint64(1), uint64(1), uint64(10), uint64(3), uint64(2), []byte{0x00, 0x01, 0x02, 0x03, 0xFF})

	f.Fuzz(func(t *testing.T, shardID, replicaID, raftIndex, term, entryType uint64, cmd []byte) {
		// Cap command size to avoid excessive memory in fuzz runs.
		if len(cmd) > 64*1024 {
			t.Skip("command too large for fuzz")
		}

		// Build the entry payload: [payloadType:1][ShardID:8][ReplicaID:8]
		// [Index:8][Term:8][Type:8][CmdLen:4][Cmd:N]
		payloadSize := 1 + entryRecordHeaderSize + len(cmd)
		payload := make([]byte, payloadSize)
		payload[0] = payloadTypeEntries
		binary.LittleEndian.PutUint64(payload[1:], shardID)
		binary.LittleEndian.PutUint64(payload[9:], replicaID)
		binary.LittleEndian.PutUint64(payload[17:], raftIndex)
		binary.LittleEndian.PutUint64(payload[25:], term)
		binary.LittleEndian.PutUint64(payload[33:], entryType)
		binary.LittleEndian.PutUint32(payload[41:], uint32(len(cmd)))
		copy(payload[45:], cmd)

		// Encode as a WAL record.
		buf := make([]byte, recordHeaderSize+payloadSize)
		n, err := encodeRecord(buf, RecordFull, payload)
		if err != nil {
			t.Fatalf("encodeRecord: %v", err)
		}

		// Decode the WAL record.
		decoded, recType, _, err := decodeRecord(buf[:n])
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		if recType != RecordFull {
			t.Fatalf("record type: got %d, want %d", recType, RecordFull)
		}

		// Parse the entry back from the decoded payload.
		if len(decoded) < 1 {
			t.Fatal("decoded payload is empty")
		}
		if decoded[0] != payloadTypeEntries {
			t.Fatalf("payload type: got %d, want %d", decoded[0], payloadTypeEntries)
		}
		data := decoded[1:]
		if len(data) < entryRecordHeaderSize {
			t.Fatal("decoded entry data shorter than header")
		}

		gotShardID := binary.LittleEndian.Uint64(data[0:])
		gotReplicaID := binary.LittleEndian.Uint64(data[8:])
		gotIndex := binary.LittleEndian.Uint64(data[16:])
		gotTerm := binary.LittleEndian.Uint64(data[24:])
		gotType := binary.LittleEndian.Uint64(data[32:])
		gotCmdLen := binary.LittleEndian.Uint32(data[40:])

		if gotShardID != shardID {
			t.Fatalf("shardID: got %d, want %d", gotShardID, shardID)
		}
		if gotReplicaID != replicaID {
			t.Fatalf("replicaID: got %d, want %d", gotReplicaID, replicaID)
		}
		if gotIndex != raftIndex {
			t.Fatalf("raftIndex: got %d, want %d", gotIndex, raftIndex)
		}
		if gotTerm != term {
			t.Fatalf("term: got %d, want %d", gotTerm, term)
		}
		if gotType != entryType {
			t.Fatalf("entryType: got %d, want %d", gotType, entryType)
		}
		if int(gotCmdLen) != len(cmd) {
			t.Fatalf("cmdLen: got %d, want %d", gotCmdLen, len(cmd))
		}
		gotCmd := data[entryRecordHeaderSize : entryRecordHeaderSize+int(gotCmdLen)]
		if !bytes.Equal(gotCmd, cmd) {
			t.Fatalf("command mismatch: got %x, want %x", gotCmd, cmd)
		}
	})
}

// FuzzSegmentReplayCrash writes random bytes to a segment file and calls
// ReadAll. The segment reader must never panic on arbitrary file contents.
// It should either return valid records or gracefully handle corruption.
func FuzzSegmentReplayCrash(f *testing.F) {
	// Seed: empty segment.
	f.Add([]byte{}, 4096)
	// Seed: all zeros (simulates pre-allocated but unused segment).
	f.Add(make([]byte, 4096), 4096)
	// Seed: single valid record in a block.
	validRec := buildValidRecord(RecordFull, []byte("replay test"))
	block := make([]byte, 4096)
	copy(block, validRec)
	f.Add(block, 4096)
	// Seed: corrupted CRC in first record.
	badRec := buildRecordWithBadCRC(RecordFull, []byte("bad crc"))
	badBlock := make([]byte, 4096)
	copy(badBlock, badRec)
	f.Add(badBlock, 4096)
	// Seed: random-looking header with truncated payload.
	truncBlock := make([]byte, 4096)
	truncBlock[0] = 0xDE
	truncBlock[1] = 0xAD
	truncBlock[2] = 0xBE
	truncBlock[3] = 0xEF
	binary.LittleEndian.PutUint16(truncBlock[4:6], 500) // claims 500 bytes payload
	truncBlock[6] = RecordFull
	f.Add(truncBlock, 4096)
	// Seed: multiple valid records packed into a block.
	multiBlock := make([]byte, 4096)
	rec1 := buildValidRecord(RecordFull, []byte("first"))
	rec2 := buildValidRecord(RecordFull, []byte("second"))
	copy(multiBlock[0:], rec1)
	copy(multiBlock[len(rec1):], rec2)
	f.Add(multiBlock, 4096)
	// Seed: valid RecordFirst without matching RecordLast (incomplete multi-chunk).
	firstOnly := buildValidRecord(RecordFirst, []byte("orphan first"))
	orphanBlock := make([]byte, 4096)
	copy(orphanBlock, firstOnly)
	f.Add(orphanBlock, 4096)
	// Seed: small block size.
	smallBlock := make([]byte, 128)
	copy(smallBlock, buildValidRecord(RecordFull, []byte("tiny")))
	f.Add(smallBlock, 128)

	f.Fuzz(func(t *testing.T, data []byte, blockSize int) {
		// Constrain block size to a reasonable range to avoid degenerate
		// cases that are not meaningful (e.g., block smaller than a header).
		if blockSize < recordHeaderSize+1 {
			blockSize = recordHeaderSize + 1
		}
		if blockSize > 1024*1024 {
			blockSize = 1024 * 1024
		}

		if len(data) == 0 {
			t.Skip("empty data")
		}

		dir := t.TempDir()
		segPath := filepath.Join(dir, segmentFilename(1))

		if err := os.WriteFile(segPath, data, 0600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		segFile, err := os.OpenFile(segPath, os.O_RDWR, 0600)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		defer segFile.Close()

		seg := &segment{
			f:           segFile,
			id:          1,
			dir:         dir,
			blockSize:   blockSize,
			blockBuf:    make([]byte, blockSize),
			blockOffset: 0,
			fileOffset:  0,
			maxFileSize: int64(len(data)) * 2,
		}

		// Must never panic. Records may or may not be returned.
		records, err := seg.ReadAll()
		_ = records
		_ = err
	})
}

// FuzzStateRecordRoundTrip creates a WAL state payload with fuzz-generated
// field values, encodes it, decodes the record, and verifies field integrity.
func FuzzStateRecordRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint64(1), uint64(1), uint64(0), uint64(0))
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), uint64(0))
	f.Add(uint64(0xFFFFFFFFFFFFFFFF), uint64(0xFFFFFFFFFFFFFFFF),
		uint64(0xFFFFFFFFFFFFFFFF), uint64(0xFFFFFFFFFFFFFFFF), uint64(0xFFFFFFFFFFFFFFFF))

	f.Fuzz(func(t *testing.T, shardID, replicaID, term, vote, commit uint64) {
		// Build state payload: [payloadType:1][ShardID:8][ReplicaID:8]
		// [Term:8][Vote:8][Commit:8]
		payload := make([]byte, 1+stateRecordSize)
		payload[0] = payloadTypeState
		binary.LittleEndian.PutUint64(payload[1:], shardID)
		binary.LittleEndian.PutUint64(payload[9:], replicaID)
		binary.LittleEndian.PutUint64(payload[17:], term)
		binary.LittleEndian.PutUint64(payload[25:], vote)
		binary.LittleEndian.PutUint64(payload[33:], commit)

		buf := make([]byte, recordHeaderSize+len(payload))
		n, err := encodeRecord(buf, RecordFull, payload)
		if err != nil {
			t.Fatalf("encodeRecord: %v", err)
		}

		decoded, recType, _, err := decodeRecord(buf[:n])
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		if recType != RecordFull {
			t.Fatalf("record type: got %d, want %d", recType, RecordFull)
		}

		if decoded[0] != payloadTypeState {
			t.Fatalf("payload type: got %d, want %d", decoded[0], payloadTypeState)
		}
		data := decoded[1:]
		if len(data) < stateRecordSize {
			t.Fatal("decoded state data shorter than expected")
		}

		gotShardID := binary.LittleEndian.Uint64(data[0:])
		gotReplicaID := binary.LittleEndian.Uint64(data[8:])
		gotTerm := binary.LittleEndian.Uint64(data[16:])
		gotVote := binary.LittleEndian.Uint64(data[24:])
		gotCommit := binary.LittleEndian.Uint64(data[32:])

		if gotShardID != shardID {
			t.Fatalf("shardID: got %d, want %d", gotShardID, shardID)
		}
		if gotReplicaID != replicaID {
			t.Fatalf("replicaID: got %d, want %d", gotReplicaID, replicaID)
		}
		if gotTerm != term {
			t.Fatalf("term: got %d, want %d", gotTerm, term)
		}
		if gotVote != vote {
			t.Fatalf("vote: got %d, want %d", gotVote, vote)
		}
		if gotCommit != commit {
			t.Fatalf("commit: got %d, want %d", gotCommit, commit)
		}
	})
}

// FuzzBootstrapRecordRoundTrip creates a WAL bootstrap payload with
// fuzz-generated field values, encodes it as a WAL record, decodes the
// record, then deserializes the bootstrap and verifies field integrity.
func FuzzBootstrapRecordRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint64(1), true, uint64(0), uint64(10), []byte("127.0.0.1:8080"))
	f.Add(uint64(0), uint64(0), false, uint64(0), uint64(0), []byte{})
	f.Add(uint64(99), uint64(42), false, uint64(3), uint64(1), []byte("node-1:9090"))

	f.Fuzz(func(t *testing.T, shardID, replicaID uint64, join bool, smType, addrRID uint64, addr []byte) {
		// Cap address length to avoid degenerate memory usage.
		if len(addr) > 4096 {
			t.Skip("address too large")
		}

		addrs := make(map[uint64]string)
		if len(addr) > 0 {
			addrs[addrRID] = string(addr)
		}

		// Build bootstrap payload.
		totalSize := 1 + bootstrapRecordBaseSize
		for _, a := range addrs {
			totalSize += 8 + 4 + len(a)
		}

		payload := make([]byte, totalSize)
		payload[0] = payloadTypeBootstrap
		binary.LittleEndian.PutUint64(payload[1:], shardID)
		binary.LittleEndian.PutUint64(payload[9:], replicaID)
		if join {
			payload[17] = 1
		} else {
			payload[17] = 0
		}
		binary.LittleEndian.PutUint64(payload[18:], smType)
		binary.LittleEndian.PutUint32(payload[26:], uint32(len(addrs)))

		pos := 1 + bootstrapRecordBaseSize
		for rid, a := range addrs {
			binary.LittleEndian.PutUint64(payload[pos:], rid)
			binary.LittleEndian.PutUint32(payload[pos+8:], uint32(len(a)))
			copy(payload[pos+12:], a)
			pos += 12 + len(a)
		}

		// Encode as WAL record.
		buf := make([]byte, recordHeaderSize+totalSize)
		n, err := encodeRecord(buf, RecordFull, payload[:pos])
		if err != nil {
			t.Fatalf("encodeRecord: %v", err)
		}

		// Decode.
		decoded, recType, _, err := decodeRecord(buf[:n])
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		if recType != RecordFull {
			t.Fatalf("record type: got %d, want %d", recType, RecordFull)
		}

		if decoded[0] != payloadTypeBootstrap {
			t.Fatalf("payload type: got %d, want %d", decoded[0], payloadTypeBootstrap)
		}
		data := decoded[1:]
		if len(data) < bootstrapRecordBaseSize {
			t.Fatal("decoded bootstrap data shorter than base size")
		}

		gotShardID := binary.LittleEndian.Uint64(data[0:])
		gotReplicaID := binary.LittleEndian.Uint64(data[8:])
		gotJoin := data[16] != 0
		gotSMType := binary.LittleEndian.Uint64(data[17:])
		gotNumAddrs := binary.LittleEndian.Uint32(data[25:])

		if gotShardID != shardID {
			t.Fatalf("shardID: got %d, want %d", gotShardID, shardID)
		}
		if gotReplicaID != replicaID {
			t.Fatalf("replicaID: got %d, want %d", gotReplicaID, replicaID)
		}
		if gotJoin != join {
			t.Fatalf("join: got %v, want %v", gotJoin, join)
		}
		if gotSMType != smType {
			t.Fatalf("smType: got %d, want %d", gotSMType, smType)
		}
		if int(gotNumAddrs) != len(addrs) {
			t.Fatalf("numAddrs: got %d, want %d", gotNumAddrs, len(addrs))
		}

		// Verify addresses.
		dpos := bootstrapRecordBaseSize
		for i := uint32(0); i < gotNumAddrs; i++ {
			if len(data)-dpos < 12 {
				t.Fatal("truncated address entry")
			}
			rid := binary.LittleEndian.Uint64(data[dpos:])
			aLen := binary.LittleEndian.Uint32(data[dpos+8:])
			dpos += 12
			if len(data)-dpos < int(aLen) {
				t.Fatal("truncated address value")
			}
			gotAddr := string(data[dpos : dpos+int(aLen)])
			dpos += int(aLen)

			expectedAddr, ok := addrs[rid]
			if !ok {
				t.Fatalf("unexpected replica ID %d in decoded addresses", rid)
			}
			if gotAddr != expectedAddr {
				t.Fatalf("address for replica %d: got %q, want %q", rid, gotAddr, expectedAddr)
			}
		}
	})
}

// FuzzAddrMapRoundTrip fuzzes the marshalAddrMap / unmarshalAddrMap pair
// with arbitrary address strings. The marshal-then-unmarshal cycle must
// reproduce the original map.
func FuzzAddrMapRoundTrip(f *testing.F) {
	f.Add(uint64(1), []byte("127.0.0.1:8080"), uint64(2), []byte("10.0.0.1:9090"))
	f.Add(uint64(0), []byte{}, uint64(0), []byte{})
	f.Add(uint64(0xFFFFFFFFFFFFFFFF), []byte("max-id-node"), uint64(1), []byte("a"))

	f.Fuzz(func(t *testing.T, rid1 uint64, addr1 []byte, rid2 uint64, addr2 []byte) {
		if len(addr1) > 4096 || len(addr2) > 4096 {
			t.Skip("address too large")
		}

		m := make(map[uint64]string)
		if len(addr1) > 0 {
			m[rid1] = string(addr1)
		}
		if len(addr2) > 0 {
			m[rid2] = string(addr2)
		}

		// Calculate size.
		size := membershipAddrMapSize(m)
		buf := make([]byte, size)
		endPos := marshalAddrMap(buf, 0, m)
		if endPos != size {
			t.Fatalf("marshalAddrMap wrote %d bytes, expected %d", endPos, size)
		}

		got, newPos := unmarshalAddrMap(buf, 0)
		if newPos == -1 {
			t.Fatal("unmarshalAddrMap returned -1 (truncated)")
		}
		if newPos != endPos {
			t.Fatalf("unmarshalAddrMap consumed %d bytes, expected %d", newPos, endPos)
		}
		if len(got) != len(m) {
			t.Fatalf("map size: got %d, want %d", len(got), len(m))
		}
		for k, v := range m {
			if got[k] != v {
				t.Fatalf("addr[%d]: got %q, want %q", k, got[k], v)
			}
		}
	})
}

// FuzzUnmarshalAddrMapCrash feeds arbitrary bytes to unmarshalAddrMap.
// The function must never panic on any input.
func FuzzUnmarshalAddrMapCrash(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00}) // count=0
	f.Add([]byte{0x01, 0x00, 0x00, 0x00}) // count=1, truncated
	// Seed: count=1 with valid entry.
	seed := make([]byte, 4+8+4+5) // count(4) + rid(8) + addrLen(4) + "hello"
	binary.LittleEndian.PutUint32(seed[0:], 1)
	binary.LittleEndian.PutUint64(seed[4:], 42)
	binary.LittleEndian.PutUint32(seed[12:], 5)
	copy(seed[16:], "hello")
	f.Add(seed)
	// Seed: count claims huge number of entries.
	huge := make([]byte, 4)
	binary.LittleEndian.PutUint32(huge[0:], 0xFFFFFFFF)
	f.Add(huge)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic.
		got, pos := unmarshalAddrMap(data, 0)
		_ = got
		_ = pos
	})
}

// FuzzUnmarshalRemovedMapCrash feeds arbitrary bytes to unmarshalRemovedMap.
// The function must never panic on any input.
func FuzzUnmarshalRemovedMapCrash(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00}) // count=0
	f.Add([]byte{0x01, 0x00, 0x00, 0x00}) // count=1, truncated
	// Seed: count claims huge number.
	huge := make([]byte, 4)
	binary.LittleEndian.PutUint32(huge[0:], 0xFFFFFFFF)
	f.Add(huge)
	// Seed: valid entry.
	valid := make([]byte, 4+8)
	binary.LittleEndian.PutUint32(valid[0:], 1)
	binary.LittleEndian.PutUint64(valid[4:], 99)
	f.Add(valid)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic.
		got, pos := unmarshalRemovedMap(data, 0)
		_ = got
		_ = pos
	})
}
