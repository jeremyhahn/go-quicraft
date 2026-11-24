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
	"os"
	"path/filepath"
	"testing"
)

// TestSegment_ZeroPaddingBoundaryIsExact: exactly recordHeaderSize zeros below data is legal padding, recordHeaderSize+1 is a hole; both sides are asserted.
func TestSegment_ZeroPaddingBoundaryIsExact(t *testing.T) {
	const blockSize = 1024

	// The legal side comes from the real writer: a payload of blockSize-2*header leaves avail == header and maximal padding.
	t.Run("exact_max_padding_recovers_in_full", func(t *testing.T) {
		dir := t.TempDir()
		seg, _, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024)
		if err != nil {
			t.Fatalf("createSegment: %v", err)
		}
		first := bytes.Repeat([]byte{0xA5}, blockSize-2*recordHeaderSize)
		if _, err := seg.Write(first); err != nil {
			t.Fatalf("Write(first): %v", err)
		}
		if seg.blockOffset != blockSize-recordHeaderSize {
			t.Fatalf("fixture: blockOffset = %d, want %d so the block ends with "+
				"exactly recordHeaderSize (%d) free bytes",
				seg.blockOffset, blockSize-recordHeaderSize, recordHeaderSize)
		}
		// Anything further cannot start in this block, so the writer pads the
		// remaining recordHeaderSize bytes with zeros and opens block 1.
		second := bytes.Repeat([]byte{0x5A}, 100)
		if _, err := seg.Write(second); err != nil {
			t.Fatalf("Write(second): %v", err)
		}
		if seg.fileOffset != blockSize {
			t.Fatalf("fixture: fileOffset = %d, want %d; the second write did not "+
				"cross a block boundary and the padding under test was never written",
				seg.fileOffset, blockSize)
		}
		if err := seg.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// The padding really is zeros, and really is recordHeaderSize long.
		raw, err := os.ReadFile(filepath.Join(dir, segmentFilename(1)))
		if err != nil {
			t.Fatalf("read segment: %v", err)
		}
		pad := raw[blockSize-recordHeaderSize : blockSize]
		if !bytes.Equal(pad, make([]byte, recordHeaderSize)) {
			t.Fatalf("fixture: block tail %x is not zero padding", pad)
		}
		if raw[blockSize-recordHeaderSize-1] == 0 {
			t.Fatal("fixture: the byte below the padding is zero, so the zero run " +
				"is longer than recordHeaderSize and the boundary is not being tested")
		}

		rs := openSegmentForTest(t, dir, 1, blockSize)
		defer rs.Close()
		got, end, err := rs.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if rs.stoppedAtCorruption {
			t.Fatalf("a healthy segment whose first block ends with the maximum "+
				"LEGAL padding (%d zero bytes) was diagnosed as holed at offset %d: "+
				"recovery would truncate a WAL that has nothing wrong with it and "+
				"destroy every higher segment with it", recordHeaderSize, end)
		}
		if len(got) != 2 {
			t.Fatalf("ReadAll returned %d records, want 2", len(got))
		}
		if !bytes.Equal(got[0], first) || !bytes.Equal(got[1], second) {
			t.Fatal("recovered records do not match what was written across the " +
				"maximally padded block boundary")
		}
	})

	// The illegal side is one byte wider, and has to be built by hand: the
	// writer cannot produce it, which is precisely why it is damage.
	t.Run("one_byte_past_max_padding_is_a_hole", func(t *testing.T) {
		dir := t.TempDir()
		first := bytes.Repeat([]byte{0xA5}, blockSize-2*recordHeaderSize-1)
		second := bytes.Repeat([]byte{0x5A}, 100)

		file := make([]byte, blockSize+recordHeaderSize+len(second))
		n, err := encodeRecord(file[0:blockSize], RecordFull, 0, 0, first)
		if err != nil {
			t.Fatalf("encodeRecord(first): %v", err)
		}
		if got := blockSize - n; got != recordHeaderSize+1 {
			t.Fatalf("fixture: zero run is %d bytes, want %d", got, recordHeaderSize+1)
		}
		if _, err := encodeRecord(file[blockSize:], RecordFull, 0, 0, second); err != nil {
			t.Fatalf("encodeRecord(second): %v", err)
		}
		path := filepath.Join(dir, segmentFilename(1))
		if err := os.WriteFile(path, file, 0o600); err != nil {
			t.Fatalf("write segment: %v", err)
		}

		rs := openSegmentForTest(t, dir, 1, blockSize)
		defer rs.Close()
		got, end, err := rs.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if !rs.stoppedAtCorruption {
			t.Fatalf("a zero run of %d bytes -- one byte MORE than any padding "+
				"this writer emits -- with data above it was accepted as ordinary "+
				"end-of-block padding: ReadAll returned %d records and replayed the "+
				"data on the far side of the hole, manufacturing a Raft log gap",
				recordHeaderSize+1, len(got))
		}
		if len(got) != 1 {
			t.Fatalf("ReadAll returned %d records across the hole, want only the "+
				"1-record clean prefix", len(got))
		}
		if end != int64(blockSize-recordHeaderSize-1) {
			t.Fatalf("clean-prefix end = %d, want %d (the first zero byte of the run)",
				end, blockSize-recordHeaderSize-1)
		}
		// One unsynced generation, so the hole is repairable by truncation.
		if rs.ackedDataAfterCorruption || rs.sequenceRegression {
			t.Fatalf("evidence flags set for a single-generation hole: acked=%v "+
				"regression=%v", rs.ackedDataAfterCorruption, rs.sequenceRegression)
		}
	})
}

// TestSegment_RepairOffsetIsTheFirstHoleNotTheLast: ReadAll cuts the segment at the first hole, so the repair leaves no interior hole.
func TestSegment_RepairOffsetIsTheFirstHoleNotTheLast(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 1024
	payload := bytes.Repeat([]byte{0x7E}, 200)

	// Block 0: one record, then a zero run far longer than any legal padding.
	// Block 1: wholly zero — the second hole, and the one a last-wins rule
	//          would latch onto.
	// Block 2: data, which is what turns the zeros below into holes at all.
	file := make([]byte, 3*blockSize)
	firstHole, err := encodeRecord(file[0:blockSize], RecordFull, 0, 0, payload)
	if err != nil {
		t.Fatalf("encodeRecord(block 0): %v", err)
	}
	if _, err := encodeRecord(file[2*blockSize:], RecordFull, 0, 0, payload); err != nil {
		t.Fatalf("encodeRecord(block 2): %v", err)
	}
	if blockSize-firstHole <= recordHeaderSize {
		t.Fatalf("fixture: block 0's zero run is %d bytes, which is legal padding",
			blockSize-firstHole)
	}

	path := filepath.Join(dir, segmentFilename(1))
	if err := os.WriteFile(path, file, 0o600); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	rs := openSegmentForTest(t, dir, 1, blockSize)
	records, cleanEnd, err := rs.ReadAll()
	stopped := rs.stoppedAtCorruption
	rs.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !stopped {
		t.Fatal("two zero-filled holes with data above them were not reported as corruption")
	}
	if len(records) != 1 {
		t.Fatalf("recovered %d records, want only the 1 below the first hole", len(records))
	}
	if cleanEnd != int64(firstHole) {
		t.Fatalf("repair offset = %d, want %d (the FIRST hole). Cutting at a LATER "+
			"hole leaves the first zero run inside the file recovery just called "+
			"repaired; the shard appends above it and the next boot finds zeros "+
			"with data above them again, cuts back to here, and destroys "+
			"everything written in between — once per boot, forever",
			cleanEnd, firstHole)
	}

	// The repair must actually remove the damage: after cutting at cleanEnd the
	// file has no interior zero run a later append could turn back into a hole,
	// and re-reading it is clean.
	if err := os.Truncate(path, cleanEnd); err != nil {
		t.Fatalf("truncate to the repair offset: %v", err)
	}
	repaired, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read repaired segment: %v", err)
	}
	if run := zeroRunEndingAt(repaired, len(repaired)); run > recordHeaderSize {
		t.Fatalf("the repaired segment still ends with a %d-byte zero run: the "+
			"damage was left in the file and the next append will sit above it",
			run)
	}
	rs2 := openSegmentForTest(t, dir, 1, blockSize)
	defer rs2.Close()
	again, _, err := rs2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after repair: %v", err)
	}
	if rs2.stoppedAtCorruption {
		t.Fatal("the repaired segment still reports corruption: the repair is not " +
			"idempotent and every boot will cut it again")
	}
	if len(again) != 1 {
		t.Fatalf("repaired segment holds %d records, want 1", len(again))
	}
}

// TestSegment_ShortNonZeroRemainderIsDamageNotPadding: a remainder too short for a header is padding only if all zero; otherwise it is damage and recorded as a hole.
func TestSegment_ShortNonZeroRemainderIsDamageNotPadding(t *testing.T) {
	const blockSize = 1024
	// A record leaving a 5-byte remainder: only its content distinguishes padding from damage.
	const remainder = 5
	payload := bytes.Repeat([]byte{0x3C}, blockSize-remainder-recordHeaderSize)

	// The legal control: the same layout with the remainder left as zeros must
	// still recover cleanly, so the test cannot pass by flagging everything.
	t.Run("zero_remainder_is_padding", func(t *testing.T) {
		dir := t.TempDir()
		file := make([]byte, 2*blockSize)
		n, err := encodeRecord(file[0:blockSize], RecordFull, 0, 0, payload)
		if err != nil {
			t.Fatalf("encodeRecord: %v", err)
		}
		if blockSize-n != remainder {
			t.Fatalf("fixture: remainder is %d bytes, want %d", blockSize-n, remainder)
		}
		if _, err := encodeRecord(file[blockSize:], RecordFull, 0, 0, payload); err != nil {
			t.Fatalf("encodeRecord(block 1): %v", err)
		}
		path := filepath.Join(dir, segmentFilename(1))
		if err := os.WriteFile(path, file, 0o600); err != nil {
			t.Fatalf("write segment: %v", err)
		}

		rs := openSegmentForTest(t, dir, 1, blockSize)
		defer rs.Close()
		got, _, err := rs.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if rs.stoppedAtCorruption {
			t.Fatal("a block padded with zeros to a 5-byte remainder — exactly what " +
				"padAndFlushBlock writes — was reported as corruption")
		}
		if len(got) != 2 {
			t.Fatalf("recovered %d records, want 2", len(got))
		}
	})

	// The interior case: the padding zeros are overwritten with garbage, and a
	// record sits in the block above. Replaying that record would create a gap.
	t.Run("interior_non_zero_remainder_stops_recovery", func(t *testing.T) {
		dir := t.TempDir()
		file := make([]byte, 2*blockSize)
		n, err := encodeRecord(file[0:blockSize], RecordFull, 0, 0, payload)
		if err != nil {
			t.Fatalf("encodeRecord: %v", err)
		}
		copy(file[n:blockSize], []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01})
		if _, err := encodeRecord(file[blockSize:], RecordFull, 0, 0, payload); err != nil {
			t.Fatalf("encodeRecord(block 1): %v", err)
		}
		path := filepath.Join(dir, segmentFilename(1))
		if err := os.WriteFile(path, file, 0o600); err != nil {
			t.Fatalf("write segment: %v", err)
		}

		rs := openSegmentForTest(t, dir, 1, blockSize)
		defer rs.Close()
		got, end, err := rs.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if !rs.stoppedAtCorruption {
			t.Fatalf("a %d-byte non-zero remainder in an INTERIOR block was accepted "+
				"as end-of-block padding: ReadAll returned %d records, so the record "+
				"in the block ABOVE the damage was replayed and the log now has a "+
				"hole where the damage is, with nothing to tell the caller",
				remainder, len(got))
		}
		if len(got) != 1 {
			t.Fatalf("recovered %d records across the damage, want only the "+
				"1-record clean prefix", len(got))
		}
		if end != int64(n) {
			t.Fatalf("clean-prefix end = %d, want %d (the first byte of the damaged "+
				"remainder)", end, n)
		}
		// One unsynced generation, so this is repairable by truncation.
		if rs.ackedDataAfterCorruption || rs.sequenceRegression {
			t.Fatalf("evidence flags set for a single-generation segment: acked=%v "+
				"regression=%v", rs.ackedDataAfterCorruption, rs.sequenceRegression)
		}
	})

	// The torn-tail case: the same damage at end of file, with nothing above.
	// Recovery must cut it off rather than leave it in a file it has just
	// declared repaired.
	t.Run("trailing_non_zero_remainder_is_truncated", func(t *testing.T) {
		dir := t.TempDir()
		buf := make([]byte, blockSize)
		n, err := encodeRecord(buf, RecordFull, 0, 0, payload)
		if err != nil {
			t.Fatalf("encodeRecord: %v", err)
		}
		file := append(buf[:n], 0xDE, 0xAD, 0xBE, 0xEF, 0x01)
		path := filepath.Join(dir, segmentFilename(1))
		if err := os.WriteFile(path, file, 0o600); err != nil {
			t.Fatalf("write segment: %v", err)
		}

		rs := openSegmentForTest(t, dir, 1, blockSize)
		defer rs.Close()
		got, end, err := rs.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if !rs.stoppedAtCorruption {
			t.Fatal("a torn fragment shorter than a record header at end of file was " +
				"accepted as a clean end-of-records, so recovery reported the segment " +
				"as needing no repair and left the fragment in it")
		}
		if len(got) != 1 {
			t.Fatalf("recovered %d records, want 1", len(got))
		}
		if end != int64(n) {
			t.Fatalf("clean-prefix end = %d, want %d (the fragment must be cut off)", end, n)
		}
	})
}
