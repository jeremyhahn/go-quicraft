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
	iofs "io/fs"
	"testing"
)

// reopenReadOnly opens an existing segment file read-only exactly as
// replaySegment does (no O_TRUNC) and reads all records back.
func reopenReadOnly(t *testing.T, dir string, id uint64, blockSize int) [][]byte {
	t.Helper()
	f, err := defaultFS().Open(dir + "/" + segmentFilename(id))
	if err != nil {
		t.Fatalf("open %d: %v", id, err)
	}
	defer f.Close()
	rs := &segment{f: f, id: id, dir: dir, blockSize: blockSize, blockBuf: make([]byte, blockSize)}
	recs, _, err := rs.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return recs
}

// TestSegment_MidBlockSyncRecovery is the regression test for C1: a mid-block
// Sync must not move fileOffset off the block grid, so every record written
// across many interleaved syncs is recoverable. Before the fix this lost
// roughly half the records.
func TestSegment_MidBlockSyncRecovery(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 32 * 1024
	seg, _, err := createSegment(defaultFS(), dir, 1, blockSize, 64*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}

	var want [][]byte
	write := func(p []byte) {
		if _, err := seg.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
		cp := make([]byte, len(p))
		copy(cp, p)
		want = append(want, cp)
	}

	// Small record then a mid-block sync (the trigger for the old bug).
	write(bytes.Repeat([]byte{0xAB}, 5000))
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// Many records straddling the reader's block grid, with periodic syncs.
	for i := 0; i < 60; i++ {
		write(bytes.Repeat([]byte{byte(i)}, 900))
		if i%7 == 0 {
			if err := seg.Sync(); err != nil {
				t.Fatalf("Sync loop: %v", err)
			}
		}
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("final Sync: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := reopenReadOnly(t, dir, 1, blockSize)
	if len(got) != len(want) {
		t.Fatalf("recovery loss: wrote %d, recovered %d", len(want), len(got))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d mismatch: len want=%d got=%d", i, len(want[i]), len(got[i]))
		}
	}
}

// TestSegment_MidBlockSyncRecoveryWithoutClose verifies recovery from a
// crashed (never closed) segment whose final block is partial: the
// preallocated zero tail must terminate the scan cleanly with no loss.
func TestSegment_MidBlockSyncRecoveryWithoutClose(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 4 * 1024
	seg, _, err := createSegment(defaultFS(), dir, 2, blockSize, 8*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	var want [][]byte
	for i := 0; i < 200; i++ {
		p := bytes.Repeat([]byte{byte(i % 251)}, 100+i)
		if _, err := seg.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
		cp := make([]byte, len(p))
		copy(cp, p)
		want = append(want, cp)
		if i%5 == 0 {
			if err := seg.Sync(); err != nil {
				t.Fatalf("Sync: %v", err)
			}
		}
	}
	if err := seg.Sync(); err != nil { // durable, but no Close (simulates crash)
		t.Fatalf("Sync: %v", err)
	}

	got := reopenReadOnly(t, dir, 2, blockSize)
	if len(got) != len(want) {
		t.Fatalf("recovery loss after crash: wrote %d, recovered %d", len(want), len(got))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d mismatch", i)
		}
	}
}

// TestSegment_StopsAtFirstCorruption is the regression test for C2: recovery
// must return the clean prefix and stop at the first corrupt record rather
// than skipping ahead and recovering a non-prefix.
func TestSegment_StopsAtFirstCorruption(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 4 * 1024
	seg, _, err := createSegment(defaultFS(), dir, 3, blockSize, 8*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	// Write enough records to span several blocks.
	const n = 120
	for i := 0; i < n; i++ {
		if _, err := seg.Write(bytes.Repeat([]byte{byte(i)}, 200)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	clean := reopenReadOnly(t, dir, 3, blockSize)
	if len(clean) != n {
		t.Fatalf("baseline: wrote %d, recovered %d", n, len(clean))
	}

	// Corrupt a byte in the middle of the file (inside block 2's records).
	rw, err := defaultFS().OpenFile(dir+"/"+segmentFilename(3), 2 /*O_RDWR*/, 0600)
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	// Flip a byte well past the first block so a clean prefix exists.
	corruptAt := int64(blockSize + 512)
	bad := []byte{0xFF}
	if _, err := rw.WriteAt(bad, corruptAt); err != nil {
		t.Fatalf("corrupt write: %v", err)
	}
	_ = rw.Sync()
	_ = rw.Close()

	got := reopenReadOnly(t, dir, 3, blockSize)
	// Recovery must stop at the corruption: strictly fewer than the full set,
	// and a non-empty clean prefix that matches the originals.
	if len(got) >= n {
		t.Fatalf("expected recovery to stop early, got all %d records", len(got))
	}
	if len(got) == 0 {
		t.Fatalf("expected a non-empty clean prefix before the corruption")
	}
	for i := range got {
		if !bytes.Equal(got[i], clean[i]) {
			t.Fatalf("prefix record %d diverged from original; recovered a non-prefix", i)
		}
	}
}

// TestSegment_StopsAtZeroFilledHole verifies that a wholly-zero block
// followed by valid data (a write hole: block N lost in a crash while
// block N+1 reached the medium) terminates recovery at the hole instead
// of resurrecting post-hole records, and marks the segment as stopped at
// corruption so higher-numbered segments are not replayed either.
//
// Every record here is written before the ONE trailing Sync, so the whole
// segment is a single durability generation and nothing in it was ever
// acknowledged. That is deliberate — it is the interrupted-write half of the
// property — and the evidence flags are asserted rather than ignored, so this
// test also fails if the generation check ever starts refusing a WAL that a
// power loss legitimately produces. The acknowledged half lives in
// TestSegment_ZeroHoleUnderAcknowledgedDataIsNotTruncatable.
func TestSegment_StopsAtZeroFilledHole(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 4 * 1024
	seg, _, err := createSegment(defaultFS(), dir, 4, blockSize, 8*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	// Fill several full blocks with records sized so block boundaries are
	// crossed predictably.
	const n = 120
	for i := 0; i < n; i++ {
		if _, err := seg.Write(bytes.Repeat([]byte{byte(i + 1)}, 200)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	clean := reopenReadOnly(t, dir, 4, blockSize)
	if len(clean) != n {
		t.Fatalf("baseline: wrote %d, recovered %d", n, len(clean))
	}

	// Zero out the ENTIRE second block, simulating a lost write whose
	// following blocks made it to the medium.
	rw, err := defaultFS().OpenFile(dir+"/"+segmentFilename(4), 2 /*O_RDWR*/, 0600)
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	if _, err := rw.WriteAt(make([]byte, blockSize), int64(blockSize)); err != nil {
		t.Fatalf("hole write: %v", err)
	}
	_ = rw.Sync()
	_ = rw.Close()

	f, err := defaultFS().Open(dir + "/" + segmentFilename(4))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	rs := &segment{f: f, id: 4, dir: dir, blockSize: blockSize, blockBuf: make([]byte, blockSize)}
	got, _, err := rs.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	// Recovery must stop at the hole: only block 0's records survive.
	if len(got) >= n {
		t.Fatalf("expected recovery to stop at the hole, got all %d records", len(got))
	}
	if len(got) == 0 {
		t.Fatal("expected a non-empty clean prefix before the hole")
	}
	for i := range got {
		if !bytes.Equal(got[i], clean[i]) {
			t.Fatalf("prefix record %d diverged from original", i)
		}
	}
	if !rs.stoppedAtCorruption {
		t.Fatal("stoppedAtCorruption must be set so higher segments are not replayed")
	}
	// Every record in this segment shares generation 0, so no completed
	// fdatasync sits above the hole and the truncation destroys nothing that was
	// acknowledged. Asserting it keeps the evidence check from being inert here:
	// a change that set either flag for a single-generation hole would turn an
	// ordinary power loss into a WAL the node refuses to open.
	if rs.ackedDataAfterCorruption {
		t.Fatal("ackedDataAfterCorruption is set for a hole whose surviving records " +
			"all carry the segment's only generation: recovery would fail-stop on an " +
			"ordinary interrupted write")
	}
	if rs.sequenceRegression {
		t.Fatal("sequenceRegression is set for a single-generation segment; no " +
			"generation in it can be below any other")
	}
}

// TestSegment_ZeroTailIsNotAHole verifies the normal layouts are still
// recovered in full: intra-block zero padding (<= header size) before the
// next block's data, and a zero tail at the true end of the segment.
func TestSegment_ZeroTailIsNotAHole(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 4 * 1024
	seg, _, err := createSegment(defaultFS(), dir, 5, blockSize, 8*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	// Record sizes chosen so blocks end with < recordHeaderSize+1 free
	// bytes from time to time, exercising padAndFlushBlock padding, and
	// the final block has a long preallocated zero tail (no Close).
	var want [][]byte
	for i := 0; i < 300; i++ {
		p := bytes.Repeat([]byte{byte(i%250 + 1)}, 90+i%17)
		if _, err := seg.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
		cp := make([]byte, len(p))
		copy(cp, p)
		want = append(want, cp)
	}
	if err := seg.Sync(); err != nil { // durable, but no Close (crash)
		t.Fatalf("Sync: %v", err)
	}

	f, err := defaultFS().Open(dir + "/" + segmentFilename(5))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	rs := &segment{f: f, id: 5, dir: dir, blockSize: blockSize, blockBuf: make([]byte, blockSize)}
	got, _, err := rs.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("recovery loss: wrote %d, recovered %d", len(want), len(got))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d mismatch", i)
		}
	}
	if rs.stoppedAtCorruption {
		t.Fatal("clean segment must not be flagged as corrupt")
	}
}

// writeRecordingFS records every WriteAt issued against segment files so a test
// can assert what the flush path actually puts on the medium.
type writeRecordingFS struct {
	FS
	writes *[]writeRange
}

// OpenFile wraps the opened file so its WriteAt calls are recorded.
func (w writeRecordingFS) OpenFile(name string, flag int, perm iofs.FileMode) (File, error) {
	f, err := w.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &writeRecordingFile{File: f, writes: w.writes}, nil
}

// writeRange is one recorded WriteAt: where it started and how many bytes it
// carried.
type writeRange struct {
	off int64
	n   int
}

// writeRecordingFile records each WriteAt before delegating.
type writeRecordingFile struct {
	File
	writes *[]writeRange
}

// WriteAt records the range and delegates to the wrapped file.
func (w *writeRecordingFile) WriteAt(p []byte, off int64) (int, error) {
	*w.writes = append(*w.writes, writeRange{off: off, n: len(p)})
	return w.File.WriteAt(p, off)
}

// TestSegment_FlushWritesOnlyNewBytes pins the flush watermark.
//
// flushBlock used to write the open block FROM BYTE 0 on every Sync. That is a
// durability defect before it is a performance one: bytes an earlier fdatasync
// already made durable — a HardState carrying a vote grant, say — were put back
// in flight inside the next batch's write, so a power loss during that rewrite
// could destroy a record the caller had already been told was durable. It is
// also quadratic: filling one block over n syncs wrote ~n^2*recSize/2 bytes.
//
// The assertion is the durability one: no byte is ever written twice. The
// amplification bound follows from it.
func TestSegment_FlushWritesOnlyNewBytes(t *testing.T) {
	dir := t.TempDir()
	const (
		blockSize = 1024
		records   = 200
		recSize   = 48
	)
	var writes []writeRange
	fsys := writeRecordingFS{FS: defaultFS(), writes: &writes}

	seg, _, err := createSegment(fsys, dir, 9, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	seg.noSync = true // the fsync itself is not what is under test

	var want [][]byte
	for i := range records {
		p := bytes.Repeat([]byte{byte(i%250 + 1)}, recSize)
		if _, err := seg.Write(p); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		// One Sync per record is the WAL's actual non-batched write pattern.
		if err := seg.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
		want = append(want, p)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// No write may revisit a region an earlier write already covered.
	var total int
	var high int64
	for i, w := range writes {
		if w.off < high {
			t.Fatalf("write %d covers [%d,%d), which re-writes bytes already made "+
				"durable up to %d: an acknowledged record is back in flight and a "+
				"power loss during this write can destroy it",
				i, w.off, w.off+int64(w.n), high)
		}
		high = w.off + int64(w.n)
		total += w.n
	}

	// With the watermark, the bytes written are the bytes stored (plus block
	// padding). Without it the same workload wrote orders of magnitude more.
	logicalSize := int(seg.Size())
	if total > logicalSize+blockSize {
		t.Fatalf("wrote %d bytes to persist a %d-byte segment (%.1fx amplification)",
			total, logicalSize, float64(total)/float64(logicalSize))
	}

	// The block grid must be intact: recovery reads every record back.
	got := reopenReadOnly(t, dir, 9, blockSize)
	if len(got) != len(want) {
		t.Fatalf("recovery loss: wrote %d records, recovered %d", len(want), len(got))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d mismatch after incremental flushing", i)
		}
	}
}

// TestSegment_DiscardPendingResetsFlushWatermark verifies a rollback rewinds
// the flush watermark with the offsets. If it did not, the next flush would
// either skip bytes the rollback restored (leaving a hole the recovery scanner
// stops at) or re-write durable ones.
func TestSegment_DiscardPendingResetsFlushWatermark(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 1024
	seg, _, err := createSegment(defaultFS(), dir, 10, blockSize, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	seg.noSync = true

	keep := bytes.Repeat([]byte{0xa1}, 100)
	if _, err := seg.Write(keep); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	cleanFileOffset, cleanBlockOffset := seg.fileOffset, seg.blockOffset
	cleanCount := seg.RecordCount()

	// A failed batch appends into the open block, FLUSHES (so the watermark
	// genuinely advances past the checkpoint), and is rolled back.
	//
	// The flush is what gives this test its teeth. Without it the batch's bytes
	// never leave blockBuf, so flushed is still sitting at cleanBlockOffset when
	// DiscardPending runs and the rewind it is supposed to prove has nothing to
	// undo — the assertion below then holds whether or not the code rewinds
	// anything at all.
	if _, err := seg.Write(bytes.Repeat([]byte{0xb2}, 100)); err != nil {
		t.Fatalf("Write (batch): %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync (batch): %v", err)
	}
	if seg.flushed <= cleanBlockOffset {
		t.Fatalf("fixture: flush watermark %d is not ahead of the checkpoint %d, "+
			"so the rewind under test is unobservable", seg.flushed, cleanBlockOffset)
	}
	if err := seg.DiscardPending(cleanFileOffset, cleanBlockOffset, cleanCount); err != nil {
		t.Fatalf("DiscardPending: %v", err)
	}
	if seg.flushed != cleanBlockOffset {
		t.Fatalf("flush watermark = %d after rollback, want the restored block "+
			"offset %d", seg.flushed, cleanBlockOffset)
	}

	// Writes resume and must land contiguously after the kept record.
	after := bytes.Repeat([]byte{0xc3}, 100)
	if _, err := seg.Write(after); err != nil {
		t.Fatalf("Write (after rollback): %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := reopenReadOnly(t, dir, 10, blockSize)
	if len(got) != 2 {
		t.Fatalf("recovered %d records after rollback, want 2", len(got))
	}
	if !bytes.Equal(got[0], keep) || !bytes.Equal(got[1], after) {
		t.Fatal("rolled-back segment did not replay the kept record followed by the " +
			"post-rollback one")
	}
}

// writeHoleFixture builds a segment whose records are written one per Sync
// (syncPerRecord) or all under a single trailing Sync, zeroes one whole block in
// the middle of it, and returns the pristine record set alongside the reopened
// segment's scan.
//
// syncPerRecord is the whole variable under test. Every completed Sync closes a
// durability generation and stamps the next record with a higher one, so
// syncPerRecord=true produces a segment where recovery can PROVE each record was
// acknowledged, and syncPerRecord=false one where it can prove none of them was.
// The identical byte damage means opposite things in the two, and the returned
// segment carries the verdict in its flags.
func writeHoleFixture(t *testing.T, dir string, id uint64, blockSize, records, holeBlock int, syncPerRecord bool) (want [][]byte, got [][]byte, cleanEnd int64, rs *segment) {
	t.Helper()
	seg, _, err := createSegment(defaultFS(), dir, id, blockSize, 8*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	for i := 0; i < records; i++ {
		p := bytes.Repeat([]byte{byte(i%251 + 1)}, 200)
		if _, err := seg.Write(p); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		want = append(want, p)
		if syncPerRecord {
			if err := seg.Sync(); err != nil {
				t.Fatalf("Sync %d: %v", i, err)
			}
		}
	}
	if !syncPerRecord {
		if err := seg.Sync(); err != nil {
			t.Fatalf("Sync: %v", err)
		}
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Sanity: the intact segment reads back every record, byte for byte.
	intact := reopenReadOnly(t, dir, id, blockSize)
	if len(intact) != records {
		t.Fatalf("fixture: intact segment recovered %d records, want %d", len(intact), records)
	}
	for i := range intact {
		if !bytes.Equal(intact[i], want[i]) {
			t.Fatalf("fixture: intact record %d differs from what was written", i)
		}
	}

	path := dir + "/" + segmentFilename(id)
	rw, err := defaultFS().OpenFile(path, 2 /* os.O_RDWR */, 0600)
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	fi, err := rw.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if blocks := fi.Size() / int64(blockSize); blocks < int64(holeBlock)+2 {
		t.Fatalf("fixture: segment has %d blocks, need at least %d so data follows the hole",
			blocks, holeBlock+2)
	}
	if _, err := rw.WriteAt(make([]byte, blockSize), int64(holeBlock*blockSize)); err != nil {
		t.Fatalf("zero block %d: %v", holeBlock, err)
	}
	if err := rw.Sync(); err != nil {
		t.Fatalf("sync hole: %v", err)
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := defaultFS().Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	rs = &segment{f: f, id: id, dir: dir, blockSize: blockSize, blockBuf: make([]byte, blockSize)}
	got, cleanEnd, err = rs.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return want, got, cleanEnd, rs
}

// TestSegment_ZeroHoleUnderAcknowledgedDataIsNotTruncatable is the segment-level
// regression test for the asymmetric damage classifier.
//
// ReadAll had two classifiers and only one of them consulted the durability
// generations. A failed CRC swept the rest of the file, found that the surviving
// records spanned a completed fdatasync, and refused to truncate. A zero-filled
// hole returned on the spot, with ackedDataAfterCorruption and
// sequenceRegression both left false — so replaySegment reported an ordinary
// torn tail, recovery truncated the file to the clean prefix and deleted every
// segment above it, and acknowledged committed entries were destroyed while Open
// returned nil.
//
// A crash cannot produce this layout. Writes are strictly ascending in file
// offset, so a block sitting below a later generation was covered by that
// generation's fdatasync and its pages cannot revert to zeros. A zeroed sector
// under acknowledged data is a medium failure, a partially restored file or
// tampering, and the only safe verdict is the one the CRC path already gave.
func TestSegment_ZeroHoleUnderAcknowledgedDataIsNotTruncatable(t *testing.T) {
	const (
		blockSize = 1024
		records   = 60
		holeBlock = 3
	)
	dir := t.TempDir()
	want, got, cleanEnd, rs := writeHoleFixture(t, dir, 4, blockSize, records, holeBlock, true)

	if !rs.stoppedAtCorruption {
		t.Fatal("a zeroed block with data above it was not reported as corruption at all")
	}
	if !rs.ackedDataAfterCorruption {
		t.Fatalf("ackedDataAfterCorruption is false for a hole at offset %d whose "+
			"surviving records span %d completed fdatasyncs: recovery will read this "+
			"as an interrupted write, truncate the segment to %d bytes and delete "+
			"every segment above it, destroying %d acknowledged records and rolling "+
			"the commit index backwards — with Open returning nil",
			holeBlock*blockSize, records, cleanEnd, records-len(got))
	}
	if rs.sequenceRegression {
		t.Fatal("sequenceRegression must stay false: the generations above the hole " +
			"are higher than the prefix's, not lower")
	}
	if cleanEnd != int64(holeBlock*blockSize) {
		t.Fatalf("clean end = %d, want the hole offset %d", cleanEnd, holeBlock*blockSize)
	}

	// The prefix ReadAll returns must be the ORIGINAL records, byte for byte,
	// with nothing missing below the hole and nothing resurrected above it. A
	// count-only assertion would accept a recovery that dropped acknowledged
	// records BELOW the damage.
	if len(got) == 0 {
		t.Fatal("no clean prefix survived; the hole is above several full blocks")
	}
	if len(got) >= records {
		t.Fatalf("recovered %d of %d records: records above the hole were replayed",
			len(got), records)
	}
	for i := range got {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("recovered record %d is not the record that was written at "+
				"ordinal %d: the surviving prefix is not the original prefix", i, i)
		}
	}
	// And the ones above the hole are absent, not merely uncounted.
	for i := len(got); i < records; i++ {
		for _, rec := range got {
			if bytes.Equal(rec, want[i]) {
				t.Fatalf("record %d, which sits at or above the hole, was replayed", i)
			}
		}
	}
}

// TestSegment_ZeroHoleInsideOneGenerationStillTruncates is the other half of the
// same property, and the guard against over-tightening it.
//
// A batch spanning several blocks issues one WriteAt per block before a single
// fdatasync, and writeback may persist any subset of those pages. Losing an
// interior page while a later one survives leaves exactly a zero hole with
// decodable records above it — but every one of those records carries the
// segment's in-flight generation, so no completed fdatasync sits above the
// damage and nothing there was ever acknowledged. That is an ordinary
// interrupted write, and recovery must still truncate and boot.
func TestSegment_ZeroHoleInsideOneGenerationStillTruncates(t *testing.T) {
	const (
		blockSize = 1024
		records   = 60
		holeBlock = 3
	)
	dir := t.TempDir()
	want, got, cleanEnd, rs := writeHoleFixture(t, dir, 5, blockSize, records, holeBlock, false)

	if !rs.stoppedAtCorruption {
		t.Fatal("a zeroed block with data above it was not reported as corruption")
	}
	if rs.ackedDataAfterCorruption {
		t.Fatal("ackedDataAfterCorruption is set for a hole whose survivors all " +
			"belong to ONE unsynced generation: recovery now refuses to open a WAL " +
			"that an ordinary power loss produces and that used to boot")
	}
	if rs.sequenceRegression {
		t.Fatal("sequenceRegression is set for a single-generation segment")
	}
	if cleanEnd != int64(holeBlock*blockSize) {
		t.Fatalf("clean end = %d, want the hole offset %d", cleanEnd, holeBlock*blockSize)
	}
	if len(got) == 0 || len(got) >= records {
		t.Fatalf("recovered %d of %d records, want a non-empty proper prefix",
			len(got), records)
	}
	for i := range got {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("recovered record %d diverged from the original", i)
		}
	}
}
