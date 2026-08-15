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

// TestTornBlockTailIsAHoleNotAnEndOfRecords pins the torn-block guard in
// segment.ReadAll's zero-region handling.
//
// A zero region is legal ONLY as the final padding of the segment. The guard
// (`pos == 0 || len(block)-pos > recordHeaderSize`) distinguishes a torn block
// -- a zero region large enough to have held a record header, or one starting at
// the very top of a block -- from ordinary end-of-block padding. Without it,
// recovery treats the torn region as a clean end-of-records, KEEPS the records
// in every following block, and returns no hole. That silently manufactures a
// gap in the Raft log: entries before the tear and after it survive, the ones
// inside it do not, and nothing reports corruption.
//
// The guard was previously uncovered -- removing it left the entire waldb suite
// green.
//
// The segment is closed WITHOUT a Sync, so every record in it carries generation
// 0: nothing here was ever acknowledged, and truncating at the tear destroys
// nothing. The evidence flags are asserted below so that stays true by
// construction rather than by accident -- the acknowledged-data counterpart is
// TestSegment_ZeroHoleUnderAcknowledgedDataIsNotTruncatable, where the same
// damage must instead refuse to open.
func TestTornBlockTailIsAHoleNotAnEndOfRecords(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 1024
	seg, _, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}

	// Write enough records to span several blocks. Each record is a good
	// fraction of a block so the blocks fill predictably.
	const total = 120
	payload := make([]byte, 200)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	for range total {
		if _, err := seg.Write(payload); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Sanity: the intact segment reads back everything with no hole.
	intact := openSegmentForTest(t, dir, 1, blockSize)
	got, _, err := intact.ReadAll()
	intactCorrupt := intact.stoppedAtCorruption
	intact.Close()
	if err != nil {
		t.Fatalf("ReadAll (intact): %v", err)
	}
	if len(got) != total || intactCorrupt {
		t.Fatalf("fixture: intact segment read %d records (want %d), "+
			"stoppedAtCorruption=%v (want false)", len(got), total, intactCorrupt)
	}

	// Tear the MIDDLE of the segment: zero one whole block. Data follows it,
	// so this is a hole, not the segment's trailing padding.
	path := filepath.Join(dir, segmentFilename(1))
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat segment: %v", err)
	}
	blocks := info.Size() / blockSize
	if blocks < 4 {
		t.Fatalf("fixture: segment has only %d blocks, need >= 4 to tear the middle",
			blocks)
	}
	tornBlock := blocks / 2
	if _, err := f.WriteAt(make([]byte, blockSize), tornBlock*blockSize); err != nil {
		t.Fatalf("zero block: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	torn := openSegmentForTest(t, dir, 1, blockSize)
	defer torn.Close()
	records, _, err := torn.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll (torn): %v", err)
	}

	if !torn.stoppedAtCorruption {
		t.Fatalf("a zeroed block in the MIDDLE of the segment was not reported as "+
			"corruption, and ReadAll returned %d of %d records: recovery treated "+
			"the tear as a clean end-of-records and kept the records in every "+
			"FOLLOWING block, manufacturing a silent gap in the Raft log with "+
			"nothing to tell the caller", len(records), total)
	}
	if len(records) >= total {
		t.Fatalf("ReadAll returned %d of %d records across a tear: records after "+
			"the hole were replayed", len(records), total)
	}
	// The tear is inside a single unsynced generation, so recovery is licensed
	// to truncate at it. Both evidence flags must stay clear, or a routine power
	// loss becomes an unbootable node.
	if torn.ackedDataAfterCorruption {
		t.Fatal("ackedDataAfterCorruption is set for a tear inside one unsynced " +
			"generation: recovery would refuse to open a WAL it must repair")
	}
	if torn.sequenceRegression {
		t.Fatal("sequenceRegression is set for a single-generation segment")
	}

	// The surviving prefix must be the ORIGINAL prefix, byte for byte. A count
	// is not enough: a recovery that dropped records BELOW the tear and made up
	// the difference above it would satisfy the bound above.
	for i := range records {
		if !bytes.Equal(records[i], payload) {
			t.Fatalf("recovered record %d is not the record written at that "+
				"ordinal; the surviving prefix is not the original prefix", i)
		}
	}
}
