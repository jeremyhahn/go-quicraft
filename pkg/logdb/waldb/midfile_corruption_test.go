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

// writeSegmentRecords fills a fresh segment with n records of size bytes and returns its path.
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

// TestReadAll_TornTailIsStillATornTail verifies corruption with nothing decodable after it is a torn tail.
func TestReadAll_TornTailIsStillATornTail(t *testing.T) {
	dir := t.TempDir()
	const (
		blockSize = 1024
		total     = 20
		recSize   = 200
	)
	path := writeSegmentRecords(t, dir, 2, blockSize, total, recSize)

	// The fixture synced the last record, so a sync mark follows it; a torn
	// tail is the record whose sync never returned, which leaves no mark.
	// Remove it, then corrupt the record that is now the file's last chunk —
	// Close() truncated the file to the written size, so nothing follows it.
	dropSyncMark(t, path, blockSize)
	last, typ := lastChunk(t, path, blockSize)
	if typ != RecordFull {
		t.Fatalf("fixture: last chunk is type %d, want a full record", typ)
	}
	flipByteAt(t, path, last+8)

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
	if cleanEnd != last {
		t.Fatalf("clean end %d is not the corrupt record's offset %d", cleanEnd, last)
	}
}

// TestRecovery_MidSegmentCorruptionFailsStopWithoutTruncating verifies corruption followed by valid records fails stop and leaves the file intact.
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
	// Snapshot the content, not just the length.
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
	// The clean end is the start of the damaged record, so it lies in the block
	// that holds the flipped bit: the valid records in the blocks above it are
	// what makes this mid-segment corruption and not a torn tail.
	if corrupt.CleanEnd < int64(blockSize) || corrupt.CleanEnd >= int64(2*blockSize) {
		t.Fatalf("clean end %d is not inside the corrupt block [%d,%d) (size %d)",
			corrupt.CleanEnd, blockSize, 2*blockSize, sizeBefore)
	}
}

// shortReadErrFile returns a partial read with a non-EOF error at or past an offset.
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

// TestReadAll_ShortReadWithErrorIsAnError verifies a partial read with an error is reported.
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

// lastBlockRecordOffsets returns the offsets of the chunks decoding in the segment's final block.
func lastBlockRecordOffsets(t *testing.T, path string, blockSize int) (blockStart int64, offsets []int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	blockStart = ((int64(len(data)) - 1) / int64(blockSize)) * int64(blockSize)
	block := data[blockStart:]
	for pos := 0; pos+recordHeaderSize <= len(block); {
		_, _, _, _, n, err := decodeRecord(block[pos:])
		if err != nil {
			break
		}
		offsets = append(offsets, pos)
		pos += n
	}
	return blockStart, offsets
}

// TestReadAll_CorruptionInTheFinalBlockIsNotATornTail verifies corruption followed by valid records in the final block is not a torn tail.
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

	// Corrupt the first record in the final block; later records still pass their CRC.
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

// TestReadAll_ZeroPaddingAfterCorruptionIsStillATornTail verifies zero padding after corruption is not mistaken for data.
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

// zeroBlockAt zeroes one whole block of a segment file at an explicit offset,
// simulating a lost or reallocated sector. Unlike flipByteAt it leaves no
// decode-level damage at all: the region simply reads as zeros, which is the
// shape a permissive zero-hole classifier would wave through.
func zeroBlockAt(t *testing.T, path string, off int64, blockSize int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	if _, err := f.WriteAt(make([]byte, blockSize), off); err != nil {
		t.Fatalf("zero block at %d: %v", off, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync corruption: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestRecovery_ZeroHoleUnderAcknowledgedDataFailsStop verifies a zeroed range under acknowledged data fails stop.
func TestRecovery_ZeroHoleUnderAcknowledgedDataFailsStop(t *testing.T) {
	const (
		blockSize = 1024
		entries   = 60
		holeAt    = 3 * blockSize
	)
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(1024*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// One SaveState per entry: each is independently fsynced, so each record
	// carries a generation a completed fdatasync separates from its neighbours'.
	for i := uint64(1); i <= entries; i++ {
		upd := logdb.Update{
			ShardID:   shardID,
			ReplicaID: 1,
			State:     logdb.State{Term: 7, Vote: 3, Commit: i},
			Entries:   []logdb.Entry{{Index: i, Term: 7, Cmd: bytes.Repeat([]byte{byte(i)}, 64)}},
		}
		if err := db.SaveState([]logdb.Update{upd}); err != nil {
			t.Fatalf("SaveState %d: %v", i, err)
		}
	}
	seg0Path := segmentPath(db, shardID, 0)

	// Take the baseline before closing; reopening would create segment 1.
	first, length, err := db.EntryRange(shardID, 1)
	if err != nil {
		t.Fatalf("EntryRange (baseline): %v", err)
	}
	st, _, err := db.ReadState(shardID, 1)
	if err != nil {
		t.Fatalf("ReadState (baseline): %v", err)
	}
	if first != 1 || length != entries || st.Commit != entries {
		t.Fatalf("baseline: first=%d length=%d commit=%d, want 1/%d/%d",
			first, length, st.Commit, entries, entries)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(segmentPath(db, shardID, 1)); !os.IsNotExist(err) {
		t.Fatalf("fixture: segment 1 exists (%v); segment 0 must be the final "+
			"segment or the non-final rule masks what this test measures", err)
	}

	sizeBefore := statSize(t, seg0Path)
	if sizeBefore <= holeAt+blockSize {
		t.Fatalf("fixture: segment is %d bytes, need data above offset %d",
			sizeBefore, holeAt+blockSize)
	}
	zeroBlockAt(t, seg0Path, holeAt, blockSize)
	bytesBefore, err := os.ReadFile(seg0Path)
	if err != nil {
		t.Fatalf("read segment before recovery: %v", err)
	}

	db2, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(1024*1024))
	if err == nil {
		gotFirst, gotLen, rangeErr := db2.EntryRange(shardID, 1)
		gotState, _, stateErr := db2.ReadState(shardID, 1)
		db2.Close()
		t.Fatalf("Open SUCCEEDED over a zero-filled hole sitting under acknowledged "+
			"data: EntryRange=(%d,%d,%v) state=%+v (%v). %d of %d committed entries "+
			"were destroyed and the commit index rolled back from %d — the identical "+
			"damage expressed as a CRC failure at the same offset correctly refuses "+
			"to open",
			gotFirst, gotLen, rangeErr, gotState, stateErr,
			entries-gotLen, entries, entries)
	}
	if !errors.Is(err, ErrMidSegmentCorruption) {
		t.Fatalf("Open error = %v, want ErrMidSegmentCorruption", err)
	}

	// A fail-stop must not have repaired anything: the operator restores from
	// snapshot/peers with the WAL intact for forensics.
	bytesAfter, err := os.ReadFile(seg0Path)
	if err != nil {
		t.Fatalf("read segment after recovery: %v", err)
	}
	if !bytes.Equal(bytesAfter, bytesBefore) {
		t.Fatalf("the fail-stop mutated the segment on disk (size %d -> %d)",
			len(bytesBefore), len(bytesAfter))
	}
}

// TestRecovery_ZeroHoleInsideOneGenerationStillBoots verifies a WAL from an ordinary power loss still opens.
func TestRecovery_ZeroHoleInsideOneGenerationStillBoots(t *testing.T) {
	const blockSize = 1024
	dir := t.TempDir()
	const shardID = uint64(1)

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeSingleGenerationSegment(t, db, shardID)
	seg0Path := segmentPath(db, shardID, 0)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dropSyncMark(t, seg0Path, blockSize)

	sizeBefore := statSize(t, seg0Path)
	zeroBlockAt(t, seg0Path, blockSize, blockSize)

	db2, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open after an interrupted multi-block write: %v — consulting the "+
			"durability generations on the hole path must not refuse a WAL that an "+
			"ordinary power loss produces", err)
	}
	defer db2.Close()

	// Replica 1's record is the first one written, so it is below the hole and
	// must have survived.
	first, length, err := db2.EntryRange(shardID, 1)
	if err != nil {
		t.Fatalf("EntryRange after recovery: %v", err)
	}
	if first != 1 || length != 1 {
		t.Fatalf("EntryRange = (%d, %d), want (1, 1)", first, length)
	}
	// And the segment was actually repaired on disk, so the next restart is a
	// plain recovery rather than a re-detection of the same damage.
	if got := statSize(t, seg0Path); got >= sizeBefore {
		t.Fatalf("corrupt segment was not truncated: size %d -> %d", sizeBefore, got)
	}
}
