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

// openSegmentForTest opens an existing segment file for reading in tests.
// This mirrors the production recovery path in db.go which constructs a
// read-only segment struct directly.
func openSegmentForTest(t *testing.T, dir string, id uint64, blockSize int) *segment {
	t.Helper()
	name := filepath.Join(dir, segmentFilename(id))
	f, err := os.Open(name)
	if err != nil {
		t.Fatalf("openSegmentForTest: %v", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		t.Fatalf("openSegmentForTest: stat: %v", err)
	}
	return &segment{
		f:           f,
		id:          id,
		dir:         dir,
		blockSize:   blockSize,
		blockBuf:    make([]byte, blockSize),
		blockOffset: 0,
		fileOffset:  fi.Size(),
		maxFileSize: fi.Size(),
	}
}

// TestCreateAndCloseSegment verifies that a segment can be created, produces
// a file on disk, and can be closed without error.
func TestCreateAndCloseSegment(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 32*1024, 64*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}

	if seg.ID() != 1 {
		t.Fatalf("segment ID = %d, want 1", seg.ID())
	}

	// Segment file must exist on disk.
	name := filepath.Join(dir, segmentFilename(1))
	if _, err := os.Stat(name); err != nil {
		t.Fatalf("segment file not found: %v", err)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}
}

// TestCloseAlreadyClosed verifies that closing a segment twice returns ErrClosed.
func TestCloseAlreadyClosed(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 32*1024, 64*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	if err := seg.Close(); err != ErrClosed {
		t.Fatalf("Close: error = %v, want %v", err, ErrClosed)
	}
}

// TestWriteAndReadAllRoundTrip writes a single record and verifies ReadAll
// returns it intact.
func TestWriteAndReadAllRoundTrip(t *testing.T) {
	dir := t.TempDir()
	blockSize := 1024
	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}

	data := []byte("hello raft")
	seq, err := seg.Write(data)
	if err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}
	if seq != 1 {
		t.Fatalf("Write: seq = %d, want 1", seq)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	// Reopen for reading.
	rseg := openSegmentForTest(t, dir, 1, blockSize)
	defer rseg.Close()

	records, err := rseg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("ReadAll: got %d records, want 1", len(records))
	}
	if !bytes.Equal(records[0], data) {
		t.Fatalf("ReadAll: data = %q, want %q", records[0], data)
	}
}

// TestMultipleWritesReadAll writes multiple records and verifies ReadAll
// returns them all in order.
func TestMultipleWritesReadAll(t *testing.T) {
	dir := t.TempDir()
	blockSize := 1024
	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}

	expected := []string{
		"record one",
		"record two",
		"record three",
		"record four",
		"record five",
	}

	for i, s := range expected {
		seq, err := seg.Write([]byte(s))
		if err != nil {
			t.Fatalf("Write[%d]: unexpected error: %v", i, err)
		}
		if seq != uint32(i+1) {
			t.Fatalf("Write[%d]: seq = %d, want %d", i, seq, i+1)
		}
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	rseg := openSegmentForTest(t, dir, 1, blockSize)
	defer rseg.Close()

	records, err := rseg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}
	if len(records) != len(expected) {
		t.Fatalf("ReadAll: got %d records, want %d", len(records), len(expected))
	}
	for i, rec := range records {
		if string(rec) != expected[i] {
			t.Fatalf("ReadAll[%d]: data = %q, want %q", i, rec, expected[i])
		}
	}
}

// TestLargeRecordSpansBlocks writes a record larger than one block and verifies
// it is correctly split and reassembled via ReadAll.
func TestLargeRecordSpansBlocks(t *testing.T) {
	dir := t.TempDir()
	blockSize := 128 // Small blocks to force multi-chunk splitting.
	maxPayloadPerBlock := blockSize - recordHeaderSize

	// Data large enough to span at least 3 blocks.
	dataLen := maxPayloadPerBlock*3 + 10
	data := make([]byte, dataLen)
	for i := range data {
		data[i] = byte(i % 251)
	}

	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}

	_, err = seg.Write(data)
	if err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	rseg := openSegmentForTest(t, dir, 1, blockSize)
	defer rseg.Close()

	records, err := rseg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("ReadAll: got %d records, want 1", len(records))
	}
	if !bytes.Equal(records[0], data) {
		t.Fatalf("ReadAll: data mismatch (len got=%d, want=%d)", len(records[0]), len(data))
	}
}

// TestReadAllCorruptedTail simulates a crash by truncating the segment file
// mid-record and verifies that ReadAll returns all records before the corruption.
func TestReadAllCorruptedTail(t *testing.T) {
	dir := t.TempDir()
	blockSize := 256

	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}

	// Write two records.
	_, err = seg.Write([]byte("first record"))
	if err != nil {
		t.Fatalf("Write[0]: unexpected error: %v", err)
	}
	_, err = seg.Write([]byte("second record"))
	if err != nil {
		t.Fatalf("Write[1]: unexpected error: %v", err)
	}

	// Record the size after two good records.
	sizeAfterTwo := seg.Size()

	// Write a third record.
	_, err = seg.Write([]byte("third record that will be corrupted"))
	if err != nil {
		t.Fatalf("Write[2]: unexpected error: %v", err)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	// Truncate the file to cut through the third record.
	name := filepath.Join(dir, segmentFilename(1))
	truncAt := sizeAfterTwo + 5 // cut into the third record header
	if err := os.Truncate(name, truncAt); err != nil {
		t.Fatalf("os.Truncate: unexpected error: %v", err)
	}

	rseg := openSegmentForTest(t, dir, 1, blockSize)
	defer rseg.Close()

	records, err := rseg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}

	// Only the first two records should survive.
	if len(records) != 2 {
		t.Fatalf("ReadAll: got %d records, want 2", len(records))
	}
	if string(records[0]) != "first record" {
		t.Fatalf("ReadAll[0]: data = %q, want %q", records[0], "first record")
	}
	if string(records[1]) != "second record" {
		t.Fatalf("ReadAll[1]: data = %q, want %q", records[1], "second record")
	}
}

// TestSegmentFilenameFormat verifies the filename format: zero-padded 6-digit
// number with .log extension.
func TestSegmentFilenameFormat(t *testing.T) {
	tests := []struct {
		id   uint64
		want string
	}{
		{0, "000000.log"},
		{1, "000001.log"},
		{42, "000042.log"},
		{999999, "999999.log"},
		{1000000, "1000000.log"},
	}
	for _, tc := range tests {
		got := segmentFilename(tc.id)
		if got != tc.want {
			t.Errorf("segmentFilename(%d) = %q, want %q", tc.id, got, tc.want)
		}
	}
}

// TestSegmentFilenameUnique verifies that different IDs produce different filenames.
func TestSegmentFilenameUnique(t *testing.T) {
	a := segmentFilename(1)
	b := segmentFilename(2)
	if a == b {
		t.Fatalf("segmentFilename(1) == segmentFilename(2): %q", a)
	}
}

// TestSegmentSizeTracking verifies that Size() reports the number of bytes
// written to the segment.
func TestSegmentSizeTracking(t *testing.T) {
	dir := t.TempDir()
	blockSize := 1024
	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}

	if seg.Size() != 0 {
		t.Fatalf("Size: got %d, want 0 (empty segment)", seg.Size())
	}

	_, err = seg.Write([]byte("some data"))
	if err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}

	if seg.Size() <= 0 {
		t.Fatalf("Size: got %d, want > 0 after write", seg.Size())
	}

	sizeAfterFirst := seg.Size()

	_, err = seg.Write([]byte("more data"))
	if err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}

	if seg.Size() <= sizeAfterFirst {
		t.Fatalf("Size: got %d, want > %d after second write", seg.Size(), sizeAfterFirst)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}
}

// TestWriteToFullSegment verifies that Write returns ErrSegmentFull when the
// record would exceed the maximum file size.
func TestWriteToFullSegment(t *testing.T) {
	dir := t.TempDir()
	blockSize := 64
	// Very small max file size so we fill it quickly.
	maxFileSize := int64(blockSize * 2)

	seg, err := createSegment(defaultFS(), dir, 1, blockSize, maxFileSize)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	defer seg.Close()

	// Fill the segment.
	data := make([]byte, blockSize-recordHeaderSize-1)
	_, err = seg.Write(data)
	if err != nil {
		t.Fatalf("Write[0]: unexpected error: %v", err)
	}

	// This write should exceed the max file size.
	bigData := make([]byte, blockSize)
	_, err = seg.Write(bigData)
	if err != ErrSegmentFull {
		t.Fatalf("Write: error = %v, want %v", err, ErrSegmentFull)
	}
}

// TestWriteToClosedSegment verifies that Write returns ErrClosed on a closed segment.
func TestWriteToClosedSegment(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	_, err = seg.Write([]byte("should fail"))
	if err != ErrClosed {
		t.Fatalf("Write: error = %v, want %v", err, ErrClosed)
	}
}

// TestSyncClosedSegment verifies that Sync returns ErrClosed on a closed segment.
func TestSyncClosedSegment(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	if err := seg.Sync(); err != ErrClosed {
		t.Fatalf("Sync: error = %v, want %v", err, ErrClosed)
	}
}

// TestReadAllClosedSegment verifies that ReadAll returns ErrClosed on a closed segment.
func TestReadAllClosedSegment(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	_, err = seg.ReadAll()
	if err != ErrClosed {
		t.Fatalf("ReadAll: error = %v, want %v", err, ErrClosed)
	}
}

// TestSyncNoSync verifies that Sync is a no-op when noSync is set.
func TestSyncNoSync(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	seg.noSync = true
	defer seg.Close()

	_, err = seg.Write([]byte("test data"))
	if err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}

	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: unexpected error: %v", err)
	}
}

// TestSyncAfterWrite verifies that Sync completes without error after a write.
func TestSyncAfterWrite(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	defer seg.Close()

	_, err = seg.Write([]byte("sync test"))
	if err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}

	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: unexpected error: %v", err)
	}
}

// TestSegmentID verifies the ID getter returns the correct segment ID.
func TestSegmentID(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 42, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	defer seg.Close()

	if seg.ID() != 42 {
		t.Fatalf("ID = %d, want 42", seg.ID())
	}
}

// TestSegmentIDZero verifies that ID 0 is a valid segment ID.
func TestSegmentIDZero(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 0, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	defer seg.Close()

	if seg.ID() != 0 {
		t.Fatalf("ID = %d, want 0", seg.ID())
	}
}

// TestMultipleRecordsAcrossBlocks writes enough records to span multiple blocks
// and verifies they are all recovered correctly.
func TestMultipleRecordsAcrossBlocks(t *testing.T) {
	dir := t.TempDir()
	blockSize := 64 // Small blocks.
	maxPayload := blockSize - recordHeaderSize

	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}

	// Write records that each fill exactly one block's payload.
	count := 10
	expected := make([][]byte, count)
	for i := 0; i < count; i++ {
		data := bytes.Repeat([]byte{byte(i)}, maxPayload-1)
		expected[i] = data
		_, err := seg.Write(data)
		if err != nil {
			t.Fatalf("Write[%d]: unexpected error: %v", i, err)
		}
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	rseg := openSegmentForTest(t, dir, 1, blockSize)
	defer rseg.Close()

	records, err := rseg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}
	if len(records) != count {
		t.Fatalf("ReadAll: got %d records, want %d", len(records), count)
	}
	for i := range records {
		if !bytes.Equal(records[i], expected[i]) {
			t.Fatalf("ReadAll[%d]: data mismatch", i)
		}
	}
}

// TestReadAllEmptySegment verifies that ReadAll returns no records for an
// empty segment file.
func TestReadAllEmptySegment(t *testing.T) {
	dir := t.TempDir()
	blockSize := 1024
	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	rseg := openSegmentForTest(t, dir, 1, blockSize)
	defer rseg.Close()

	records, err := rseg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("ReadAll: got %d records, want 0", len(records))
	}
}

// TestSequenceNumberIncrement verifies that sequential writes return
// monotonically increasing sequence numbers.
func TestSequenceNumberIncrement(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}
	defer seg.Close()

	for i := uint32(1); i <= 5; i++ {
		seq, err := seg.Write([]byte("seq test"))
		if err != nil {
			t.Fatalf("Write[%d]: unexpected error: %v", i, err)
		}
		if seq != i {
			t.Fatalf("Write[%d]: seq = %d, want %d", i, seq, i)
		}
	}
}

// TestSequenceNumberIncrementAfterClose verifies that sequence numbers start
// fresh for a new segment (they are per-segment, not global).
func TestSequenceNumberIncrementAfterClose(t *testing.T) {
	dir := t.TempDir()

	seg1, err := createSegment(defaultFS(), dir, 1, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment[1]: unexpected error: %v", err)
	}
	seq1, _ := seg1.Write([]byte("seg1"))
	seg1.Close()

	seg2, err := createSegment(defaultFS(), dir, 2, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment[2]: unexpected error: %v", err)
	}
	seq2, _ := seg2.Write([]byte("seg2"))
	seg2.Close()

	if seq1 != 1 {
		t.Fatalf("seg1 first seq = %d, want 1", seq1)
	}
	if seq2 != 1 {
		t.Fatalf("seg2 first seq = %d, want 1", seq2)
	}
}

// TestLargeRecordExactBlockBoundary writes a record whose payload exactly
// fills the remaining block space, forcing the next record into a new block.
func TestLargeRecordExactBlockBoundary(t *testing.T) {
	dir := t.TempDir()
	blockSize := 128
	maxPayload := blockSize - recordHeaderSize

	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: unexpected error: %v", err)
	}

	// First record fills the block exactly.
	data1 := bytes.Repeat([]byte{0xAA}, maxPayload)
	_, err = seg.Write(data1)
	if err != nil {
		t.Fatalf("Write[0]: unexpected error: %v", err)
	}

	// Second record must start at the next block.
	data2 := []byte("next block")
	_, err = seg.Write(data2)
	if err != nil {
		t.Fatalf("Write[1]: unexpected error: %v", err)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}

	rseg := openSegmentForTest(t, dir, 1, blockSize)
	defer rseg.Close()

	records, err := rseg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: unexpected error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("ReadAll: got %d records, want 2", len(records))
	}
	if !bytes.Equal(records[0], data1) {
		t.Fatalf("ReadAll[0]: data mismatch")
	}
	if !bytes.Equal(records[1], data2) {
		t.Fatalf("ReadAll[1]: data = %q, want %q", records[1], data2)
	}
}

// --- Benchmarks ---

// BenchmarkSegmentWrite benchmarks Write() with 128-byte records, measuring
// WAL block-based writing throughput.
func BenchmarkSegmentWrite(b *testing.B) {
	b.ReportAllocs()
	dir := b.TempDir()
	blockSize := 4096
	maxFileSize := int64(1024 * 1024 * 1024) // 1GB to avoid segment full.

	seg, err := createSegment(defaultFS(), dir, 1, blockSize, maxFileSize)
	if err != nil {
		b.Fatalf("createSegment: %v", err)
	}
	b.Cleanup(func() {
		seg.Close()
	})

	record := make([]byte, 128)
	for i := range record {
		record[i] = byte(i % 251)
	}

	b.ResetTimer()
	for b.Loop() {
		_, err := seg.Write(record)
		if err != nil {
			b.Fatalf("Write: %v", err)
		}
	}
}

// TestDiscardPendingResetsBlockBuffer verifies that DiscardPending zeros the
// block buffer, resets offsets, and truncates the file back to the checkpoint.
func TestDiscardPendingResetsBlockBuffer(t *testing.T) {
	dir := t.TempDir()
	blockSize := 4096
	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 64*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	defer seg.Close()

	// Write a record and sync to establish a clean checkpoint.
	record1 := []byte("durable-record-one")
	if _, err := seg.Write(record1); err != nil {
		t.Fatalf("Write record1: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	cleanFileOffset := seg.fileOffset
	cleanBlockOffset := seg.blockOffset

	// Write additional records that remain unflushed (simulating batch mode).
	record2 := []byte("ghost-record-two")
	if _, err := seg.Write(record2); err != nil {
		t.Fatalf("Write record2: %v", err)
	}

	// Verify the block buffer has data.
	if seg.blockOffset == cleanBlockOffset && seg.fileOffset == cleanFileOffset {
		t.Fatal("expected segment state to differ after second write")
	}

	// Discard back to the checkpoint.
	if err := seg.DiscardPending(cleanFileOffset, cleanBlockOffset); err != nil {
		t.Fatalf("DiscardPending: %v", err)
	}

	// Verify offsets are restored.
	if seg.fileOffset != cleanFileOffset {
		t.Errorf("fileOffset = %d, want %d", seg.fileOffset, cleanFileOffset)
	}
	if seg.blockOffset != cleanBlockOffset {
		t.Errorf("blockOffset = %d, want %d", seg.blockOffset, cleanBlockOffset)
	}

	// Verify the block buffer is zeroed.
	allZero := true
	for _, b := range seg.blockBuf {
		if b != 0 {
			allZero = false
			break
		}
	}
	if !allZero {
		t.Error("block buffer should be zeroed after DiscardPending")
	}

	// Write a new record and sync, then verify only record1 + record3 exist.
	record3 := []byte("real-record-three")
	if _, err := seg.Write(record3); err != nil {
		t.Fatalf("Write record3: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync after discard: %v", err)
	}

	// Re-open and read all records to verify no ghost data.
	readSeg := openSegmentForTest(t, dir, 1, blockSize)
	defer readSeg.Close()

	records, err := readSeg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records after discard, got %d", len(records))
	}
	if !bytes.Equal(records[0], record1) {
		t.Errorf("record[0] = %q, want %q", records[0], record1)
	}
	if !bytes.Equal(records[1], record3) {
		t.Errorf("record[1] = %q, want %q", records[1], record3)
	}
}

// TestDiscardPendingOnClosedSegment verifies that DiscardPending returns
// ErrClosed when called on a closed segment.
func TestDiscardPendingOnClosedSegment(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(defaultFS(), dir, 1, 4096, 64*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err = seg.DiscardPending(0, 0)
	if err != ErrClosed {
		t.Errorf("DiscardPending on closed segment: got %v, want ErrClosed", err)
	}
}

// TestDiscardPendingNoTruncateNeeded verifies DiscardPending works correctly
// when no file data was written (only block buffer changes).
func TestDiscardPendingNoTruncateNeeded(t *testing.T) {
	dir := t.TempDir()
	blockSize := 4096
	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 64*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	defer seg.Close()

	// Capture initial state (fileOffset=0, blockOffset=0).
	cleanFileOffset := seg.fileOffset
	cleanBlockOffset := seg.blockOffset

	// Write a small record that stays in the block buffer (no flush to file).
	if _, err := seg.Write([]byte("small")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if seg.fileOffset != 0 {
		t.Fatal("expected fileOffset=0 for small record in block buffer")
	}

	// Discard back to initial state.
	if err := seg.DiscardPending(cleanFileOffset, cleanBlockOffset); err != nil {
		t.Fatalf("DiscardPending: %v", err)
	}

	if seg.blockOffset != 0 {
		t.Errorf("blockOffset = %d, want 0", seg.blockOffset)
	}
	if seg.fileOffset != 0 {
		t.Errorf("fileOffset = %d, want 0", seg.fileOffset)
	}
}

// TestDiscardPendingWithMultiBlockRecord verifies DiscardPending correctly
// truncates when a large record caused padAndFlushBlock to write data to
// the file (fileOffset advanced beyond the checkpoint).
func TestDiscardPendingWithMultiBlockRecord(t *testing.T) {
	dir := t.TempDir()
	blockSize := 512
	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 64*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	defer seg.Close()

	// Write a small record, sync, and checkpoint.
	if _, err := seg.Write([]byte("anchor")); err != nil {
		t.Fatalf("Write anchor: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	cleanFileOffset := seg.fileOffset
	cleanBlockOffset := seg.blockOffset

	// Write a record larger than one block to force padAndFlushBlock.
	bigRecord := make([]byte, blockSize*2)
	for i := range bigRecord {
		bigRecord[i] = 0xAB
	}
	if _, err := seg.Write(bigRecord); err != nil {
		t.Fatalf("Write big: %v", err)
	}

	// Verify fileOffset advanced (padAndFlushBlock was called).
	if seg.fileOffset <= cleanFileOffset {
		t.Fatal("expected fileOffset to advance after multi-block write")
	}

	// Discard back.
	if err := seg.DiscardPending(cleanFileOffset, cleanBlockOffset); err != nil {
		t.Fatalf("DiscardPending: %v", err)
	}

	if seg.fileOffset != cleanFileOffset {
		t.Errorf("fileOffset = %d, want %d", seg.fileOffset, cleanFileOffset)
	}

	// Verify file size matches.
	fi, err := seg.f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != cleanFileOffset {
		t.Errorf("file size = %d, want %d", fi.Size(), cleanFileOffset)
	}
}

// BenchmarkSegmentSync benchmarks Sync() calls, measuring fdatasync overhead.
func BenchmarkSegmentSync(b *testing.B) {
	b.ReportAllocs()
	dir := b.TempDir()
	blockSize := 4096

	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 1024*1024*1024)
	if err != nil {
		b.Fatalf("createSegment: %v", err)
	}
	b.Cleanup(func() {
		seg.Close()
	})

	// Write some initial data so Sync has something to flush.
	record := make([]byte, 128)
	for i := range record {
		record[i] = byte(i % 251)
	}
	for i := 0; i < 10; i++ {
		if _, err := seg.Write(record); err != nil {
			b.Fatalf("Write: %v", err)
		}
	}

	b.ResetTimer()
	for b.Loop() {
		// Write a record then sync to measure the fdatasync cost per call.
		if _, err := seg.Write(record); err != nil {
			b.Fatalf("Write: %v", err)
		}
		if err := seg.Sync(); err != nil {
			b.Fatalf("Sync: %v", err)
		}
	}
}
