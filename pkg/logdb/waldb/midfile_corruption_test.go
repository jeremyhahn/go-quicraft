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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// flipByteAt corrupts one byte of a file in place, simulating a bit flip from
// bit rot, a misdirected write or a lying fsync. Unlike zeroing a whole block
// (corruptSegmentBlock) this leaves NO zero hole, so recovery sees it purely as
// a CRC failure.
func flipByteAt(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatalf("read byte at %d: %v", off, err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatalf("write flipped byte at %d: %v", off, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync corruption: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// writeSegmentRecords fills a fresh segment with n records of the given size
// and returns the segment path. The records are large enough that the segment
// spans many blocks, so corruption can be placed in the middle with valid data
// on both sides.
//
// Every record is FSYNCED before the next one is written, which is what
// walShard.writeRecord does outside batch mode. That is not incidental setup: it
// is what makes these records ACKNOWLEDGED, and acknowledged is the whole
// property the mid-file tests below assert must not be destroyed. Records
// written into a segment that was never synced carry one durability generation
// and are, correctly, discardable — a fixture that skipped the syncs would be
// asserting against data the WAL never promised anyone, and recovery would be
// right to truncate it.
func writeSegmentRecords(t *testing.T, dir string, id uint64, blockSize, n, recSize int) string {
	t.Helper()
	seg, _, err := createSegment(defaultFS(), dir, id, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	for i := range n {
		if _, err := seg.Write(bytes.Repeat([]byte{byte(i%250 + 1)}, recSize)); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		if err := seg.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return filepath.Join(dir, segmentFilename(id))
}

// TestReadAll_CorruptionWithValidDataAfterIsNotATornTail pins the mid-file
// corruption detection in segment.ReadAll.
//
// ReadAll used to return at the FIRST decode failure without looking at
// anything below it, which made every corruption indistinguishable from a torn
// tail. recover() then truncated the file to the clean prefix and fsynced,
// making the loss permanent: one flipped bit early in a 64 MiB active segment
// discarded everything after it — thousands of already-acknowledged entries
// plus the newest HardState.
//
// The zero-hole path has always applied the correct test (does data FOLLOW the
// suspect region?); this asserts the CRC path applies it too.
func TestReadAll_CorruptionWithValidDataAfterIsNotATornTail(t *testing.T) {
	dir := t.TempDir()
	const (
		blockSize = 1024
		total     = 60
		recSize   = 200
	)
	path := writeSegmentRecords(t, dir, 1, blockSize, total, recSize)

	// Baseline: the intact segment reads back every record.
	intact := openSegmentForTest(t, dir, 1, blockSize)
	baseline, _, err := intact.ReadAll()
	intact.Close()
	if err != nil {
		t.Fatalf("ReadAll (intact): %v", err)
	}
	if len(baseline) != total {
		t.Fatalf("fixture: intact segment read %d records, want %d", len(baseline), total)
	}

	// Flip one bit inside a record in the SECOND block. Ten more blocks of
	// valid records follow it.
	flipByteAt(t, path, int64(blockSize)+100)

	seg := openSegmentForTest(t, dir, 1, blockSize)
	defer seg.Close()
	records, cleanEnd, err := seg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll (corrupt): %v", err)
	}

	if !seg.stoppedAtCorruption {
		t.Fatal("a flipped bit inside a record was not detected as corruption")
	}
	if !seg.ackedDataAfterCorruption {
		t.Fatalf("ReadAll stopped at the corruption at ~%d and reported it as a torn "+
			"tail even though %d valid records follow it in later blocks; recovery "+
			"would truncate the segment there and permanently destroy every "+
			"acknowledged record after the flipped bit (clean prefix was %d of %d "+
			"records, clean end %d)",
			blockSize+100, total-len(records), len(records), total, cleanEnd)
	}
	if len(records) >= total {
		t.Fatalf("ReadAll returned %d of %d records across corruption: records after "+
			"the corrupt one were replayed", len(records), total)
	}
	if cleanEnd < int64(blockSize) || cleanEnd >= int64(2*blockSize) {
		t.Fatalf("clean end %d is not inside the corrupt block [%d,%d)",
			cleanEnd, blockSize, 2*blockSize)
	}
}

// TestReadAll_TornTailIsStillATornTail is the counterweight to the test above:
// corruption with NOTHING decodable after it is a genuinely interrupted write,
// and recovery must still be allowed to repair it by truncation. Without this,
// the mid-file fix would turn every ordinary crash into an unbootable node.
func TestReadAll_TornTailIsStillATornTail(t *testing.T) {
	dir := t.TempDir()
	const (
		blockSize = 1024
		total     = 20
		recSize   = 200
	)
	path := writeSegmentRecords(t, dir, 2, blockSize, total, recSize)

	// Corrupt a record in the LAST block that holds data. Close() truncated
	// the file to the written size, so nothing follows it at all.
	size := statSize(t, path)
	lastBlockStart := ((size - 1) / blockSize) * blockSize
	flipByteAt(t, path, lastBlockStart+8)

	seg := openSegmentForTest(t, dir, 2, blockSize)
	defer seg.Close()
	records, cleanEnd, err := seg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !seg.stoppedAtCorruption {
		t.Fatal("corruption in the final block was not detected")
	}
	if seg.ackedDataAfterCorruption {
		t.Fatal("nothing decodable follows the corruption, so this is a torn tail; " +
			"flagging it would make an ordinary crash unrecoverable")
	}
	if len(records) == 0 || len(records) >= total {
		t.Fatalf("torn tail recovered %d of %d records; want a non-empty clean prefix",
			len(records), total)
	}
	if cleanEnd < lastBlockStart {
		t.Fatalf("clean end %d is below the corrupt block start %d", cleanEnd, lastBlockStart)
	}
}

// TestRecovery_MidSegmentCorruptionFailsStopWithoutTruncating is the DB-level
// half of the fix. Corruption in the highest-numbered (final) segment is a
// legal torn tail ONLY when nothing decodable follows it. When records do
// follow, Open must fail-stop with ErrMidSegmentCorruption and leave the file
// byte-for-byte intact, so the operator can restore from snapshot/peers with
// the evidence still on disk — instead of the WAL silently amputating itself.
func TestRecovery_MidSegmentCorruptionFailsStopWithoutTruncating(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeMultiBlockSegment(t, db, shardID)
	if err := db.SaveNodeState(shardID, 1, logdb.State{Term: 7, Vote: 3, Commit: 15}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	seg0Path := segmentPath(db, shardID, 0)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sizeBefore := statSize(t, seg0Path)
	if sizeBefore < 4*blockSize {
		t.Fatalf("fixture: segment is %d bytes, need several blocks", sizeBefore)
	}
	flipByteAt(t, seg0Path, int64(blockSize)+100)
	// Snapshot the CONTENT, not just the length. A fail-stop that truncated and
	// re-extended, or that rewrote a block in place, leaves the size unchanged
	// while destroying the evidence the operator is being told to preserve.
	bytesBefore, err := os.ReadFile(seg0Path)
	if err != nil {
		t.Fatalf("read segment before recovery: %v", err)
	}

	db2, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err == nil {
		db2.Close()
		t.Fatal("Open recovered a segment whose middle is corrupt: the records " +
			"after the corruption (and the newest HardState) were silently " +
			"truncated away, so the node boots with a shorter log than it " +
			"acknowledged and a stale term/vote it can cast again")
	}
	if !errors.Is(err, ErrMidSegmentCorruption) {
		t.Fatalf("Open error = %v, want ErrMidSegmentCorruption", err)
	}

	openErr := err
	bytesAfter, readErr := os.ReadFile(seg0Path)
	if readErr != nil {
		t.Fatalf("read segment after recovery: %v", readErr)
	}
	if len(bytesAfter) != int(sizeBefore) {
		t.Fatalf("fail-stop mutated the segment on disk: size %d -> %d",
			sizeBefore, len(bytesAfter))
	}
	if !bytes.Equal(bytesAfter, bytesBefore) {
		t.Fatal("fail-stop rewrote the segment's contents; the evidence the " +
			"operator is told to preserve is no longer byte-for-byte what recovery found")
	}

	// The error names the exact repair the operator would have to authorize.
	var corrupt *SegmentCorruptionError
	if !errors.As(openErr, &corrupt) {
		t.Fatalf("Open error = %v, want a *SegmentCorruptionError naming shard, "+
			"segment and clean end so an operator has a procedure", openErr)
	}
	if corrupt.SegmentID != 0 || corrupt.ShardIndex != int(shardID%DefaultShardCount) {
		t.Fatalf("corruption reported at shard %d segment %d, want shard %d segment 0",
			corrupt.ShardIndex, corrupt.SegmentID, shardID%DefaultShardCount)
	}
	if corrupt.CleanEnd <= 0 || corrupt.CleanEnd >= sizeBefore {
		t.Fatalf("clean end %d is not inside the segment (size %d)",
			corrupt.CleanEnd, sizeBefore)
	}
}

// shortReadErrFile returns a PARTIAL read together with a non-EOF error on the
// first ReadAt at or past a given offset, the shape a failing sector produces.
type shortReadErrFile struct {
	File
	failAt  int64
	partial int
	err     error
	fired   bool
}

// ReadAt fills part of p and reports the injected I/O error alongside it.
func (f *shortReadErrFile) ReadAt(p []byte, off int64) (int, error) {
	if !f.fired && off >= f.failAt {
		f.fired = true
		n, _ := f.File.ReadAt(p[:min(f.partial, len(p))], off)
		return n, f.err
	}
	return f.File.ReadAt(p, off)
}

// TestReadAll_ShortReadWithErrorIsAnError pins the read guard in ReadAll.
//
// The guard used to be `err != nil && n == 0`, so a partial read that ALSO
// reported an error had the error dropped on the floor: the truncated block was
// parsed as if it simply ended there, the loop still strode a full block
// forward, and every following block was replayed with neither a hole nor a
// corruption recorded — a silently shortened WAL presented as a clean one.
func TestReadAll_ShortReadWithErrorIsAnError(t *testing.T) {
	dir := t.TempDir()
	const (
		blockSize = 1024
		total     = 40
		recSize   = 200
	)
	path := writeSegmentRecords(t, dir, 3, blockSize, total, recSize)

	f, err := defaultFS().Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	injected := errors.New("simulated EIO")
	seg := &segment{
		f: &shortReadErrFile{
			File:    f,
			failAt:  int64(blockSize),
			partial: 64,
			err:     injected,
		},
		id:        3,
		dir:       dir,
		blockSize: blockSize,
		blockBuf:  make([]byte, blockSize),
	}

	records, _, err := seg.ReadAll()
	if err == nil {
		t.Fatalf("ReadAll ignored an I/O error that came back with a partial read "+
			"and returned %d records as a clean recovery", len(records))
	}
	if !errors.Is(err, injected) {
		t.Fatalf("ReadAll error = %v, want it to wrap the injected I/O error", err)
	}
	var recErr *RecoveryError
	if !errors.As(err, &recErr) {
		t.Fatalf("ReadAll error = %v, want a *RecoveryError", err)
	}
	var readErr *SegmentReadError
	if !errors.As(err, &readErr) {
		t.Fatalf("ReadAll error = %v, want a *SegmentReadError carrying the offset", err)
	}
	if readErr.Offset != int64(blockSize) {
		t.Fatalf("SegmentReadError.Offset = %d, want %d", readErr.Offset, blockSize)
	}
}

// lastBlockRecordOffsets returns the in-block offsets of every chunk that
// decodes in the segment's final data block, so a test can place corruption
// with known-good records after it INSIDE that block.
func lastBlockRecordOffsets(t *testing.T, path string, blockSize int) (blockStart int64, offsets []int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	blockStart = ((int64(len(data)) - 1) / int64(blockSize)) * int64(blockSize)
	block := data[blockStart:]
	for pos := 0; pos+recordHeaderSize <= len(block); {
		_, _, _, n, err := decodeRecord(block[pos:])
		if err != nil {
			break
		}
		offsets = append(offsets, pos)
		pos += n
	}
	return blockStart, offsets
}

// TestReadAll_CorruptionInTheFinalBlockIsNotATornTail closes the blind spot in
// the torn-tail test.
//
// The post-corruption sweep probed the first record of each SUBSEQUENT block,
// which answers the question everywhere except where it is most likely to be
// asked: if the damage is in the LAST block holding data there are no later
// blocks to probe, so no evidence was gathered and recovery
// truncated. The records sitting after the damaged bytes inside that same block
// — up to a blockful of already-acknowledged records — were destroyed, and the
// segment was cut at a fixed prefix boundary that had nothing to do with where
// the writer actually stopped.
//
// The block is already in memory, and every candidate is validated by its own
// CRC, so finding one is evidence and not a guess.
func TestReadAll_CorruptionInTheFinalBlockIsNotATornTail(t *testing.T) {
	dir := t.TempDir()
	const (
		blockSize = 1024
		total     = 30
		recSize   = 100
	)
	path := writeSegmentRecords(t, dir, 5, blockSize, total, recSize)

	blockStart, offsets := lastBlockRecordOffsets(t, path, blockSize)
	if len(offsets) < 2 {
		t.Fatalf("fixture: final block holds %d decodable records, need at least 2 "+
			"so that one can survive after the corruption", len(offsets))
	}

	// Corrupt the payload of the FIRST record in the final block. Every later
	// record in that block is untouched and still passes its own CRC.
	flipByteAt(t, path, blockStart+int64(offsets[0])+recordHeaderSize)

	seg := openSegmentForTest(t, dir, 5, blockSize)
	defer seg.Close()
	records, cleanEnd, err := seg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !seg.stoppedAtCorruption {
		t.Fatal("a flipped bit in the final block was not detected as corruption")
	}
	if !seg.ackedDataAfterCorruption {
		t.Fatalf("corruption at %d was reported as a torn tail even though %d "+
			"records after it in the SAME block still pass CRC; recovery would "+
			"truncate at %d and destroy every one of them",
			blockStart+int64(offsets[0]), len(offsets)-1, cleanEnd)
	}
	if len(records) >= total {
		t.Fatalf("ReadAll returned %d of %d records across the corruption", len(records), total)
	}
}

// TestReadAll_ZeroPaddingAfterCorruptionIsStillATornTail guards the other
// direction of the intra-block probe: the pre-allocated zero tail that follows
// the last record in a block must not be mistaken for surviving data. A zeroed
// header decodes to record type 0, which is not a valid type, so it can never
// pass — but the whole value of the fail-closed path depends on that staying
// true, so it is pinned rather than assumed.
func TestReadAll_ZeroPaddingAfterCorruptionIsStillATornTail(t *testing.T) {
	dir := t.TempDir()
	const (
		blockSize = 1024
		total     = 30
		recSize   = 100
	)
	path := writeSegmentRecords(t, dir, 6, blockSize, total, recSize)

	blockStart, offsets := lastBlockRecordOffsets(t, path, blockSize)
	if len(offsets) < 2 {
		t.Fatalf("fixture: final block holds %d decodable records, need at least 2", len(offsets))
	}

	// Erase everything after the first record of the final block, the way an
	// interrupted write leaves it, then corrupt that first record.
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := f.Truncate(blockStart + int64(offsets[1])); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	flipByteAt(t, path, blockStart+int64(offsets[0])+recordHeaderSize)

	seg := openSegmentForTest(t, dir, 6, blockSize)
	defer seg.Close()
	if _, _, err := seg.ReadAll(); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !seg.stoppedAtCorruption {
		t.Fatal("corruption in the final block was not detected")
	}
	if seg.ackedDataAfterCorruption {
		t.Fatal("nothing decodable follows the corruption, so this is a torn tail; " +
			"flagging it would make an ordinary interrupted write unrecoverable")
	}
}
