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
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// The durability sequence tells a torn multi-block write (self-heals) from rot under acknowledged records (fails closed); one fixture, only the damage moves.

const (
	// seqTestBlockSize is the block size these fixtures are written on.
	seqTestBlockSize = 4096

	// seqTestPageSize is an eighth of the block, the production page-to-block ratio.
	seqTestPageSize = 512
)

// losePage zeroes one unaligned page-sized region of a segment, as a page that never reached the medium.
func losePage(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open for page loss: %v", err)
	}
	if _, err := f.WriteAt(make([]byte, seqTestPageSize), off); err != nil {
		t.Fatalf("zero page at %d: %v", off, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync page loss: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// dropSyncMark zeroes a segment's final sync mark, turning a synced batch into one whose fdatasync never returned; fails if there is no mark.
func dropSyncMark(t *testing.T, path string, blockSize int) {
	t.Helper()
	off, typ := lastChunk(t, path, blockSize)
	if typ != RecordSyncMark {
		t.Fatalf("fixture: the segment does not end in a sync mark (last chunk type %d at %d); "+
			"there is no acknowledgement evidence here to remove", typ, off)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open for mark removal: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteAt(make([]byte, recordHeaderSize), off); err != nil {
		t.Fatalf("zero sync mark at %d: %v", off, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync mark removal: %v", err)
	}
}

// chunkInfo is one chunk of a segment's clean prefix as walkChunks saw it.
type chunkInfo struct {
	off int64
	typ byte
	seq uint32
}

// walkChunks walks a segment's clean prefix the way ReadAll does and returns
// every chunk that decodes, in file order.
func walkChunks(t *testing.T, path string, blockSize int) []chunkInfo {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open for chunk walk: %v", err)
	}
	defer f.Close()

	var chunks []chunkInfo
	block := make([]byte, blockSize)
walk:
	for at := int64(0); ; at += int64(blockSize) {
		n, err := f.ReadAt(block, at)
		if n == 0 {
			break
		}
		for pos := 0; pos < n; {
			if block[pos] == 0 {
				break
			}
			_, recType, seq, _, consumed, err := decodeRecord(block[pos:n])
			if err != nil {
				break walk
			}
			chunks = append(chunks, chunkInfo{off: at + int64(pos), typ: recType, seq: seq})
			pos += consumed
		}
		if err != nil {
			break
		}
	}
	if len(chunks) == 0 {
		t.Fatalf("fixture: no chunk decodes in %s", path)
	}
	return chunks
}

// lastChunk returns the file offset and type of the last chunk of a segment's
// clean prefix.
func lastChunk(t *testing.T, path string, blockSize int) (off int64, typ byte) {
	t.Helper()
	chunks := walkChunks(t, path, blockSize)
	last := chunks[len(chunks)-1]
	return last.off, last.typ
}

// TestRecovery_RotInFinalSyncedGenerationFailsClosed: damage in a final synced generation fails closed, but truncates once the sync mark is gone.
func TestRecovery_RotInFinalSyncedGenerationFailsClosed(t *testing.T) {
	const (
		blockSize = 1024
		shardID   = uint64(1)
	)
	cmd := bytes.Repeat([]byte{0xab}, 200)
	build := func(t *testing.T) (dir, segPath string) {
		t.Helper()
		dir = t.TempDir()
		db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		// Generation 0: one acknowledged batch. Generation 1: a second
		// acknowledged batch of several records, and the segment's last.
		if err := db.SaveState([]logdb.Update{{
			ShardID: shardID, ReplicaID: 1,
			Entries: []logdb.Entry{{Index: 1, Term: 1, Cmd: cmd}},
			State:   logdb.State{Term: 1, Vote: 1, Commit: 1},
		}}); err != nil {
			t.Fatalf("SaveState (generation 0): %v", err)
		}
		var updates []logdb.Update
		for i := uint64(0); i < 4; i++ {
			updates = append(updates, logdb.Update{
				ShardID: shardID, ReplicaID: 1,
				Entries: []logdb.Entry{{Index: 2 + i, Term: 1, Cmd: cmd}},
				State:   logdb.State{Term: 1, Vote: 1, Commit: 2 + i},
			})
		}
		if err := db.SaveState(updates); err != nil {
			t.Fatalf("SaveState (generation 1): %v", err)
		}
		segPath = segmentPath(db, shardID, 0)
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return dir, segPath
	}
	// finalRecords returns the record chunks of generation 1, which the walk
	// must show closed by a generation-2 mark and nothing else.
	finalRecords := func(t *testing.T, segPath string) []chunkInfo {
		t.Helper()
		chunks := walkChunks(t, segPath, blockSize)
		last := chunks[len(chunks)-1]
		if last.typ != RecordSyncMark || last.seq != 2 {
			t.Fatalf("fixture: last chunk is type %d generation %d, want the generation-2 sync mark", last.typ, last.seq)
		}
		var recs []chunkInfo
		for _, c := range chunks {
			if c.seq == 1 && c.typ != RecordSyncMark {
				recs = append(recs, c)
			}
		}
		if len(recs) < 3 {
			t.Fatalf("fixture: generation 1 holds %d record chunks, want several", len(recs))
		}
		return recs
	}
	open := func(dir string) (*DB, error) {
		return Open(dir, WithBlockSize(blockSize), WithMaxFileSize(64*1024))
	}
	prefixEntries := func(t *testing.T, db *DB) uint64 {
		t.Helper()
		first, last, err := db.EntryRange(shardID, 1)
		if err != nil {
			t.Fatalf("EntryRange: %v", err)
		}
		if first == 0 {
			return 0
		}
		return last - first + 1
	}

	t.Run("interior record, mark survives", func(t *testing.T) {
		dir, segPath := build(t)
		recs := finalRecords(t, segPath)
		flipByteAt(t, segPath, recs[0].off+8)
		_, err := open(dir)
		if !errors.Is(err, ErrMidSegmentCorruption) {
			t.Fatalf("Open = %v, want ErrMidSegmentCorruption: the surviving records and the "+
				"sync mark span the fdatasync that acknowledged them", err)
		}
	})

	// Each update lands as an entries record followed by a hard-state record,
	// so the generation's last two chunks are entry 5 and its state.
	t.Run("last record, only the mark survives", func(t *testing.T) {
		dir, segPath := build(t)
		recs := finalRecords(t, segPath)
		flipByteAt(t, segPath, recs[len(recs)-1].off+8)
		_, err := open(dir)
		if !errors.Is(err, ErrAcknowledgedDataLost) {
			t.Fatalf("Open = %v, want ErrAcknowledgedDataLost: the mark names an acknowledged "+
				"end above the damage", err)
		}
	})

	t.Run("mark lost, then the same damage", func(t *testing.T) {
		dir, segPath := build(t)
		recs := finalRecords(t, segPath)
		dropSyncMark(t, segPath, blockSize)
		flipByteAt(t, segPath, recs[len(recs)-2].off+8)
		db, err := open(dir)
		if err != nil {
			t.Fatalf("Open = %v; without the mark this is the batch whose fdatasync never "+
				"returned, an interrupted write recovery must repair", err)
		}
		defer db.Close()
		if got := prefixEntries(t, db); got != 4 {
			t.Fatalf("recovered %d entries, want the 4 below the damaged entry record", got)
		}
	})
}

// seqFixture is a WAL whose segment holds an acknowledged prefix followed by a
// single multi-block batch, plus the offsets that separate the two.
type seqFixture struct {
	dir      string
	segment  string
	batchAt  int64 // first byte written by the unsynced-in-effect final batch
	fileSize int64
}

// writeSeqFixture writes a prefix of one fsynced record per generation, then one multi-update SaveState sharing a single generation.
func writeSeqFixture(t *testing.T) seqFixture {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, WithBlockSize(seqTestBlockSize), WithMaxFileSize(1024*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Acknowledged prefix: 8 records, 8 fdatasyncs, 8 generations. The commands
	// are large enough that the prefix spans more than one block, so damage can
	// be placed inside it with acknowledged records still above.
	prefixCmd := make([]byte, 700)
	for i := range prefixCmd {
		prefixCmd[i] = byte(i % 97)
	}
	for i := uint64(0); i < 8; i++ {
		e := []logdb.Entry{{Index: 1 + i, Term: 3, Cmd: prefixCmd}}
		if err := db.SaveEntries(1, 1, e); err != nil {
			t.Fatalf("SaveEntries %d: %v", i, err)
		}
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 9, Vote: 4, Commit: 8}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	segPath := segmentPath(db, 1, 0)
	// The logical write position, since segments are pre-allocated.
	batchAt := db.getShard(1).activeSegment.Size()

	// The final batch: four records, one fdatasync, spanning several blocks.
	cmd := make([]byte, 3000)
	for i := range cmd {
		cmd[i] = byte(i % 251)
	}
	var updates []logdb.Update
	for k := uint64(0); k < 4; k++ {
		updates = append(updates, logdb.Update{
			ShardID:   1 + k*DefaultShardCount,
			ReplicaID: 1,
			Entries:   []logdb.Entry{{Index: 100 + k, Term: 9, Cmd: cmd}},
			State:     logdb.State{Term: 9, Vote: 4, Commit: 100 + k},
		})
	}
	if err := db.SaveState(updates); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The batch's fdatasync completed here; the fixture models one that did
	// not, so the mark that completion appended must go.
	dropSyncMark(t, segPath, seqTestBlockSize)

	size := statSize(t, segPath)
	if batchAt < int64(seqTestBlockSize) {
		t.Fatalf("fixture: the acknowledged prefix is only %d bytes; it must span "+
			"at least one full block so damage can be placed inside it", batchAt)
	}
	if size-batchAt < 3*seqTestPageSize {
		t.Fatalf("fixture: the final batch is only %d bytes; it must be several "+
			"pages long so a page can be lost with records still following it",
			size-batchAt)
	}
	return seqFixture{dir: dir, segment: segPath, batchAt: batchAt, fileSize: size}
}

// TestRecovery_TornMultiBlockWriteSelfHeals: a lost interior page in the unacknowledged final batch truncates to the clean prefix and boots.
func TestRecovery_TornMultiBlockWriteSelfHeals(t *testing.T) {
	fx := writeSeqFixture(t)

	// Lose a page inside the final batch, off any record boundary.
	losePage(t, fx.segment, fx.batchAt+300)

	db, err := Open(fx.dir, WithBlockSize(seqTestBlockSize), WithMaxFileSize(1024*1024))
	if err != nil {
		t.Fatalf("Open refused to start after an ordinary torn multi-block write, "+
			"which is what an interrupted batch looks like on disk; every record "+
			"above the damage belongs to the same unsynced batch and none of them "+
			"was ever acknowledged, so there is nothing here to protect: %v", err)
	}
	defer db.Close()

	// The acknowledged prefix survived intact.
	st, _, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if st.Term != 9 || st.Vote != 4 {
		t.Fatalf("recovered state = %+v, want Term 9 Vote 4: the self-heal cut into "+
			"the acknowledged prefix", st)
	}
	entries, err := db.GetEntries(1, 1, 1, 9, 1<<20)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(entries) != 8 {
		t.Fatalf("recovered %d of the 8 acknowledged entries; the truncation went "+
			"below the damage", len(entries))
	}

	// The repair happened on disk, so the next boot does not re-detect it.
	if size := statSize(t, fx.segment); size >= fx.fileSize {
		t.Fatalf("segment is still %d bytes (was %d): the corrupt tail was not "+
			"truncated, so every later restart re-detects it and discards whatever "+
			"has been written since", size, fx.fileSize)
	}
}

// TestRecovery_RotBelowAcknowledgedDataStillFailsClosed: the same fixture with damage in the acknowledged prefix fails closed and leaves the evidence.
func TestRecovery_RotBelowAcknowledgedDataStillFailsClosed(t *testing.T) {
	fx := writeSeqFixture(t)

	// Same damage, placed in the acknowledged prefix instead of the batch.
	losePage(t, fx.segment, 300)

	before, err := os.ReadFile(fx.segment)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}

	db, err := Open(fx.dir, WithBlockSize(seqTestBlockSize), WithMaxFileSize(1024*1024))
	if err == nil {
		db.Close()
		t.Fatal("Open recovered a segment damaged BELOW records that a completed " +
			"fdatasync had already acknowledged: those records, and the newest " +
			"HardState, were truncated away, so the node boots with a shorter log " +
			"than it acknowledged and a stale term/vote it can cast again")
	}
	if !errors.Is(err, ErrMidSegmentCorruption) {
		t.Fatalf("Open error = %v, want ErrMidSegmentCorruption", err)
	}

	after, err := os.ReadFile(fx.segment)
	if err != nil {
		t.Fatalf("read segment after recovery: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("fail-stop mutated the segment: %d bytes -> %d", len(before), len(after))
	}
}

// TestReadAll_SequenceRegressionIsNotATornTail: a lower generation above the damage is refused on its own sentinel.
func TestReadAll_SequenceRegressionIsNotATornTail(t *testing.T) {
	dir := t.TempDir()
	const (
		blockSize = 1024
		total     = 30
		recSize   = 100
	)
	path := writeSegmentRecords(t, dir, 9, blockSize, total, recSize)

	blockStart, offsets := lastBlockRecordOffsets(t, path, blockSize)
	if len(offsets) < 3 {
		t.Fatalf("fixture: final block holds %d decodable records, need at least 3",
			len(offsets))
	}

	// Overwrite a record above the damage with a well-formed one carrying
	// generation 0 — a generation the clean prefix left behind long ago.
	span := offsets[2] - offsets[1]
	stale := make([]byte, span)
	if _, err := encodeRecord(stale, RecordFull, 0, 0, make([]byte, span-recordHeaderSize)); err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.WriteAt(stale, blockStart+int64(offsets[1])); err != nil {
		t.Fatalf("write stale record: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Damage the record below it so recovery has to classify what follows.
	flipByteAt(t, path, blockStart+int64(offsets[0])+recordHeaderSize)

	seg := openSegmentForTest(t, dir, 9, blockSize)
	defer seg.Close()
	if _, _, err := seg.ReadAll(); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !seg.stoppedAtCorruption {
		t.Fatal("the flipped bit was not detected as corruption")
	}
	if !seg.sequenceRegression {
		t.Fatal("a record above the damage carries a durability generation LOWER " +
			"than the clean prefix, which no writer of this segment can produce, " +
			"and recovery classified it as ordinary damage instead of refusing to " +
			"interpret bytes it cannot account for")
	}
}

// TestRecovery_SequenceRegressionFailsOpenWithItsOwnError: the self-healing fixture plus one foreign lower-generation record fails with its own error.
func TestRecovery_SequenceRegressionFailsOpenWithItsOwnError(t *testing.T) {
	fx := writeSeqFixture(t)
	losePage(t, fx.segment, fx.batchAt+300)

	// Plant a well-formed record carrying generation 0 above the damage, where
	// the next intact record of the interrupted batch sits.
	at, span := firstDecodableAt(t, fx.segment, fx.batchAt+300+seqTestPageSize)
	stale := make([]byte, span)
	if _, err := encodeRecord(stale, RecordFull, 0, 0, make([]byte, span-recordHeaderSize)); err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	f, err := os.OpenFile(fx.segment, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.WriteAt(stale, at); err != nil {
		t.Fatalf("write stale record: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err := Open(fx.dir, WithBlockSize(seqTestBlockSize), WithMaxFileSize(1024*1024))
	if err == nil {
		db.Close()
		t.Fatal("Open truncated past a record whose durability generation is lower " +
			"than the clean prefix it just replayed; those bytes were not written by " +
			"this log, and treating them as an interrupted write assumes exactly the " +
			"thing that is in question")
	}
	if !errors.Is(err, ErrRecordSequenceRegression) {
		t.Fatalf("Open error = %v, want ErrRecordSequenceRegression", err)
	}
	var corrupt *SegmentCorruptionError
	if !errors.As(err, &corrupt) {
		t.Fatalf("Open error = %v, want a *SegmentCorruptionError naming shard, "+
			"segment and clean end", err)
	}
}

// firstDecodableAt returns the offset of the first chunk that decodes at or
// after from, together with its total framed length.
func firstDecodableAt(t *testing.T, path string, from int64) (int64, int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	for at := from; at+int64(recordHeaderSize) <= int64(len(data)); at++ {
		if _, _, _, _, n, err := decodeRecord(data[at:]); err == nil {
			return at, n
		}
	}
	t.Fatalf("no decodable record at or after offset %d", from)
	return 0, 0
}

// TestWALFormatVersionIsPinnedAtOne pins walFormatVersion at 1 until release; layout changes take a new payload-type byte instead.
func TestWALFormatVersionIsPinnedAtOne(t *testing.T) {
	if walFormatVersion != 1 {
		t.Fatalf("walFormatVersion = %d, want 1: every format is pinned at 1 until "+
			"the first production release, and a pre-release framing change is made "+
			"IN PLACE, with detection carried by retiring the record's payload-type "+
			"byte rather than by bumping this constant (see "+
			"docs/compatibility/architecture.md)", walFormatVersion)
	}
}

// TestOpen_ForeignFormatVersionIsRefusedByName: any marker version other than walFormatVersion, above or below, is refused by name before a segment is read.
func TestOpen_ForeignFormatVersionIsRefusedByName(t *testing.T) {
	// 1 leads the sweep: it is the version 0.4.x stamped, so it is the one
	// foreign value a real operator will actually present to this build.
	for _, recorded := range []uint32{0, 2, 3, 5, 255, 1 << 16, math.MaxUint32} {
		t.Run("version_"+itoa(int(recorded)), func(t *testing.T) {
			if acceptsWALFormatVersion(recorded) {
				t.Fatalf("fixture: %d is the supported version, not a foreign one", recorded)
			}
			dir := t.TempDir()
			seedWAL(t, dir, 1024)
			sizesBefore := segmentSizes(t, dir)

			// Rewrite the marker byte for byte as a build recording that version
			// would have written it: right magic, right geometry, wrong version.
			buf := make([]byte, walFormatSize)
			copy(buf[0:8], walFormatMagic)
			binary.LittleEndian.PutUint32(buf[8:12], recorded)
			binary.LittleEndian.PutUint32(buf[12:16], 1024)
			if err := os.WriteFile(filepath.Join(dir, walFormatFilename), buf, 0o600); err != nil {
				t.Fatalf("write marker: %v", err)
			}

			db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
			if err == nil {
				db.Close()
				t.Fatal("Open accepted a directory written in another on-disk format; " +
					"its chunk framing is not this build's, so every record fails CRC " +
					"and the log is truncated away as a torn tail")
			}
			if !errors.Is(err, ErrWALFormatUnsupportedVersion) {
				t.Fatalf("Open error = %v, want ErrWALFormatUnsupportedVersion", err)
			}
			if errors.Is(err, ErrWALFormatMalformed) {
				t.Fatal("a foreign format version was reported as a MALFORMED marker; " +
					"the file is perfectly intelligible and the remedy is to wipe and " +
					"re-sync, not a corruption investigation")
			}
			var version *WALFormatVersionError
			if !errors.As(err, &version) {
				t.Fatalf("Open error = %v, want a *WALFormatVersionError naming both versions", err)
			}
			if version.Recorded != recorded || version.Supported != walFormatVersion {
				t.Fatalf("version error = recorded %d supported %d, want %d and %d",
					version.Recorded, version.Supported, recorded, walFormatVersion)
			}

			if got := segmentSizes(t, dir); !sameSizes(got, sizesBefore) {
				t.Fatalf("a refused open mutated the segments on disk: %v -> %v",
					sizesBefore, got)
			}
		})
	}
}

// TestOpen_SupportedFormatVersionIsAccepted is the positive control for the
// sweep above: the same fixture, the same hand-written marker, the supported
// version — and the directory opens with its data intact. Without it the sweep
// would pass just as well against a gate that refused every marker.
func TestOpen_SupportedFormatVersionIsAccepted(t *testing.T) {
	dir := t.TempDir()
	seedWAL(t, dir, 1024)

	buf := make([]byte, walFormatSize)
	copy(buf[0:8], walFormatMagic)
	binary.LittleEndian.PutUint32(buf[8:12], walFormatVersion)
	binary.LittleEndian.PutUint32(buf[12:16], 1024)
	if err := os.WriteFile(filepath.Join(dir, walFormatFilename), buf, 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open with the supported format version: %v", err)
	}
	defer db.Close()
	st, _, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if st.Term != 4 || st.Vote != 2 {
		t.Fatalf("state = %+v, want Term 4 Vote 2", st)
	}
}

// TestRecord_DurabilitySequenceRoundTripsAndIsChecksummed pins the framing
// itself. The sequence is what recovery bases a truncate-or-refuse decision on,
// so a flipped bit in it must invalidate the chunk rather than move the chunk
// silently into another generation.
func TestRecord_DurabilitySequenceRoundTripsAndIsChecksummed(t *testing.T) {
	for _, seq := range []uint32{0, 1, 4096, maxDurabilitySeq} {
		payload := []byte("durability sequence")
		buf := make([]byte, recordHeaderSize+len(payload))
		if _, err := encodeRecord(buf, RecordFull, seq, 0, payload); err != nil {
			t.Fatalf("encodeRecord(seq=%d): %v", seq, err)
		}
		data, recType, got, _, _, err := decodeRecord(buf)
		if err != nil {
			t.Fatalf("decodeRecord(seq=%d): %v", seq, err)
		}
		if got != seq {
			t.Fatalf("durability sequence round trip = %d, want %d", got, seq)
		}
		if recType != RecordFull || string(data) != string(payload) {
			t.Fatalf("round trip damaged the record: type %d, data %q", recType, data)
		}

		// Flip a bit in the sequence field. The CRC covers it, so the chunk must
		// stop decoding rather than decode into a different generation.
		buf[7] ^= 0x01
		if _, _, moved, _, _, err := decodeRecord(buf); err == nil {
			t.Fatalf("a flipped bit in the durability sequence still decoded, as "+
				"generation %d instead of %d; recovery would decide whether to "+
				"truncate on a number the checksum never covered", moved, seq)
		}
	}
}

// TestSegment_DurabilitySequenceAdvancesOnlyOnCompletedSync: the generation advances, and a mark is appended, only when a Sync acknowledges new records.
func TestSegment_DurabilitySequenceAdvancesOnlyOnCompletedSync(t *testing.T) {
	dir := t.TempDir()
	seg, _, err := createSegment(defaultFS(), dir, 1, 1024, 64*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	defer seg.Close()

	payload := make([]byte, 32)
	for i := 0; i < 3; i++ {
		if _, err := seg.Write(payload); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if seg.durabilitySeq != 0 {
		t.Fatalf("durability generation advanced to %d without a Sync; records "+
			"would claim an fdatasync that never happened", seg.durabilitySeq)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if seg.durabilitySeq != 1 {
		t.Fatalf("durability generation = %d after one completed Sync, want 1",
			seg.durabilitySeq)
	}
	markedEnd := seg.fileOffset + int64(seg.blockOffset)
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if seg.durabilitySeq != 1 {
		t.Fatalf("durability generation = %d after a Sync that acknowledged nothing new, want 1", seg.durabilitySeq)
	}
	if end := seg.fileOffset + int64(seg.blockOffset); end != markedEnd {
		t.Fatalf("an idle Sync appended %d bytes; it must add no mark", end-markedEnd)
	}
	if _, err := seg.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if seg.durabilitySeq != 2 {
		t.Fatalf("durability generation = %d after two Syncs with a write between, want 2", seg.durabilitySeq)
	}

	// Rollback must NOT rewind it. A completed Sync is a fact about the medium,
	// and reusing a generation whose records were already durable would let a
	// later batch masquerade as part of the one that is still in flight.
	if err := seg.DiscardPending(0, 0, 0); err != nil {
		t.Fatalf("DiscardPending: %v", err)
	}
	if seg.durabilitySeq != 2 {
		t.Fatalf("durability generation was rewound to %d by a rollback; the next "+
			"batch would reuse a generation that already reached the medium",
			seg.durabilitySeq)
	}
}

// TestSegment_ExhaustedDurabilitySequenceRotates pins the wrap guard. A wrapped
// counter would present an old generation as the newest one and invert the
// proof; the segment refuses further writes instead, which rotates to a fresh
// one whose counter starts at zero.
func TestSegment_ExhaustedDurabilitySequenceRotates(t *testing.T) {
	dir := t.TempDir()
	seg, _, err := createSegment(defaultFS(), dir, 1, 1024, 64*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	defer seg.Close()

	seg.durabilitySeq = maxDurabilitySeq
	if err := seg.CanFit(16); !errors.Is(err, ErrSegmentFull) {
		t.Fatalf("CanFit at the last durability generation = %v, want ErrSegmentFull "+
			"so the caller rotates; writing on would wrap the counter and make an "+
			"acknowledged generation indistinguishable from the newest one", err)
	}
}

// TestSegment_GenerationStartAdvancesOnlyOnCompletedSync pins the writer half
// of the exact interrupted-write discriminator: the generation start
// is the acknowledged end at the moment a Sync completed, every chunk written
// afterwards carries it, and a rollback does not rewind it.
func TestSegment_GenerationStartAdvancesOnlyOnCompletedSync(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 1024
	seg, _, err := createSegment(defaultFS(), dir, 1, blockSize, 64*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	defer seg.Close()

	payload := make([]byte, 32)
	for i := 0; i < 3; i++ {
		if _, err := seg.Write(payload); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if seg.genStart != 0 {
		t.Fatalf("generation start moved to %d without a Sync", seg.genStart)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// The acknowledged end is where the three records stop; the sync mark
	// Sync appended sits above it and is not part of what was acknowledged.
	wantStart := uint32(3 * (recordHeaderSize + 32))
	if seg.genStart != wantStart {
		t.Fatalf("generation start = %d after Sync, want the acknowledged end %d", seg.genStart, wantStart)
	}
	if end := uint32(seg.fileOffset + int64(seg.blockOffset)); end != wantStart+recordHeaderSize {
		t.Fatalf("write position = %d after Sync, want the acknowledged end plus one sync mark %d", end, wantStart+recordHeaderSize)
	}

	// The next chunk carries the new start; the earlier ones carry 0.
	if _, err := seg.Write(payload); err != nil {
		t.Fatalf("Write after Sync: %v", err)
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	image := make([]byte, seg.fileOffset+int64(seg.blockOffset))
	if _, err := seg.f.ReadAt(image, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	// The image is three generation-0 records, the mark that opened
	// generation 1 at wantStart, the fourth record, and the mark that opened
	// generation 2 at the second acknowledged end (which the second mark and
	// nothing else names).
	secondStart := wantStart + recordHeaderSize + recordHeaderSize + 32
	var starts []uint32
	var types []byte
	for off := 0; off+recordHeaderSize <= len(image); {
		_, typ, _, gs, n, derr := decodeRecord(image[off:])
		if derr != nil {
			t.Fatalf("decodeRecord at %d: %v", off, derr)
		}
		starts = append(starts, gs)
		types = append(types, typ)
		off += n
	}
	if want := []uint32{0, 0, 0, wantStart, wantStart, secondStart}; !equalU32(starts, want) {
		t.Fatalf("chunk generation starts = %v, want %v", starts, want)
	}
	if want := []byte{RecordFull, RecordFull, RecordFull, RecordSyncMark, RecordFull, RecordSyncMark}; !bytes.Equal(types, want) {
		t.Fatalf("chunk types = %v, want %v", types, want)
	}

	// Rollback must NOT rewind it, for the same reason durabilitySeq is not.
	before := seg.genStart
	if err := seg.DiscardPending(0, 0, 0); err != nil {
		t.Fatalf("DiscardPending: %v", err)
	}
	if seg.genStart != before {
		t.Fatalf("generation start was rewound %d -> %d by a rollback", before, seg.genStart)
	}
}

func equalU32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestSegment_SyncMarkNeverExceedsMaxFileSize pins CanFit's reserve against
// the marks Sync appends: a segment filled until ErrSegmentFull, synced by its
// last batch and again by rotation, never grows past maxFileSize — the second
// Sync acknowledges nothing new and adds no mark, and the first landed inside
// the room CanFit kept.
func TestSegment_SyncMarkNeverExceedsMaxFileSize(t *testing.T) {
	for _, geo := range []struct {
		blockSize int
		maxFile   int64
	}{
		{32, 240}, {32, 400}, {64, 300}, {1024, 4096}, {1024, 4097},
	} {
		dir := t.TempDir()
		seg, _, err := createSegment(defaultFS(), dir, 1, geo.blockSize, geo.maxFile)
		if err != nil {
			t.Fatalf("createSegment: %v", err)
		}
		payload := make([]byte, 40)
		for {
			if err := seg.CanFit(len(payload)); errors.Is(err, ErrSegmentFull) {
				break
			} else if err != nil {
				t.Fatalf("CanFit: %v", err)
			}
			if _, err := seg.Write(payload); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := seg.Sync(); err != nil {
				t.Fatalf("Sync: %v", err)
			}
		}
		// rotateSegment's closing Sync.
		if err := seg.Sync(); err != nil {
			t.Fatalf("closing Sync: %v", err)
		}
		if size := seg.Size(); size > geo.maxFile {
			t.Fatalf("geometry %+v: segment grew to %d bytes, past maxFileSize", geo, size)
		}
		if err := seg.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}
