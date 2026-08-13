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
	"errors"
	"os"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// The durability sequence exists to separate two kinds of damage that are
// byte-for-byte identical without it:
//
//   - A torn multi-block write. One batch issues one WriteAt per block before a
//     single fdatasync, and writeback may persist any subset of those pages.
//     Losing an interior page leaves decodable records ABOVE a CRC failure, and
//     none of them was ever acknowledged.
//   - Rot inside a segment whose later records WERE acknowledged. The same
//     shape on disk, and truncating destroys committed entries.
//
// The tests below use ONE fixture for both and move only the damage: a synced
// prefix (one fdatasync per record, so each carries its own generation) followed
// by a single multi-record batch that shares one generation. Damage inside the
// batch must self-heal; the identical damage inside the prefix must fail closed.
// Anything that made the two behave alike would break one of the pair.

const (
	// seqTestBlockSize is the block size these fixtures are written on.
	seqTestBlockSize = 4096

	// seqTestPageSize stands for the filesystem page a torn write loses. It is
	// an eighth of the block, the same ratio production runs at (4 KiB pages
	// inside 32 KiB blocks), which is what makes the loss land INSIDE a block
	// and straddle records rather than align to the block grid.
	seqTestPageSize = 512
)

// losePage zeroes one page-sized region of a segment, modelling a page that
// never reached the medium in a file that was pre-allocated (so unwritten
// content reads back as zeros).
//
// It is deliberately not block-aligned and not record-aligned. A record
// straddling the start of the lost region has its head on disk and its tail
// zeroed, so it fails CRC — the damage presents as corruption, not as the clean
// zero hole the older guard already handles.
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

// seqFixture is a WAL whose segment holds an acknowledged prefix followed by a
// single multi-block batch, plus the offsets that separate the two.
type seqFixture struct {
	dir      string
	segment  string
	batchAt  int64 // first byte written by the unsynced-in-effect final batch
	fileSize int64
}

// writeSeqFixture builds that WAL.
//
// The prefix is written one record per SaveEntries call, and SaveEntries fsyncs
// before returning, so every prefix record sits in its own durability
// generation and each is PROVED acknowledged by the existence of the next.
//
// The batch is a single SaveState carrying several updates. They land in one WAL
// shard (shard IDs congruent modulo DefaultShardCount share a shard), are
// written back to back, and are covered by ONE fdatasync at the end, so all of
// them carry the same generation — the highest in the file. That is precisely
// the state a crash mid-batch leaves behind, and the reason a torn multi-block
// write is a suffix in generation terms.
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
	// The LOGICAL write position, not the file size: segments are pre-allocated
	// to maxFileSize, so Stat reports the reservation rather than what has been
	// written into it.
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

// TestRecovery_TornMultiBlockWriteSelfHeals is the point of the whole scheme.
//
// A batch spanning several blocks issues one WriteAt per block before a single
// fdatasync, so an ordinary power loss can persist an arbitrary SUBSET of those
// pages. Losing an interior page leaves records that still pass CRC sitting
// above the damage, which recovery used to read as proof that acknowledged data
// lay above it — and refuse to start, permanently, on every subsequent boot. It
// was reachable from a routine crash, and on a single-node deployment there was
// no way out except an operator-authorized Repair.
//
// The durability generations say what the framing cannot: every record above the
// damage belongs to the ONE batch that was still in flight, with no completed
// fdatasync behind any of them, so nothing there was ever acknowledged. Recovery
// truncates to the clean prefix and the node boots.
//
// Note what the on-disk state does NOT depend on: this fixture's batch was
// actually synced before the page was zeroed. The generations it left behind are
// identical either way, and that is the honest statement of what the
// discriminator can see — which records had a completed fdatasync BETWEEN them,
// and nothing else.
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

// TestRecovery_RotBelowAcknowledgedDataStillFailsClosed is the counterweight,
// and shares the fixture exactly: only the damage moves.
//
// Damage inside the acknowledged PREFIX has records of several later
// generations above it. Each of those generations proves an fdatasync completed
// after the one below it, which proves those records were acknowledged.
// Truncating to the clean prefix would discard committed entries and roll
// term/vote backwards, so recovery must refuse — and must leave the evidence on
// disk for the operator.
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

// TestReadAll_SequenceRegressionIsNotATornTail covers the gap the sequence makes
// visible: a record above the damage whose generation is BELOW one already
// replayed underneath it.
//
// The counter only ever rises within a segment, so no writer of this WAL
// produced that layout. The bytes are a misdirected write from elsewhere on the
// device, a partial restore of an older file over a newer one, or tampering.
// Recovery cannot classify the damage — truncating might destroy acknowledged
// records, replaying might admit records this node never wrote — so it refuses
// on a sentinel of its own rather than filing the case under either.
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
	if _, err := encodeRecord(stale, RecordFull, 0, make([]byte, span-recordHeaderSize)); err != nil {
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

// TestRecovery_SequenceRegressionFailsOpenWithItsOwnError is the DB-level half,
// and it is deliberately the SELF-HEALING fixture with one thing added: a record
// above the damage that this WAL cannot have written.
//
// Without it, the same damage truncates and the node boots
// (TestRecovery_TornMultiBlockWriteSelfHeals). With it, recovery has bytes it
// cannot account for — a generation below what the clean prefix already
// replayed — and must stop, under a sentinel that says what was observed rather
// than folding the case into the acknowledged-data one, whose diagnosis and
// remedy differ.
func TestRecovery_SequenceRegressionFailsOpenWithItsOwnError(t *testing.T) {
	fx := writeSeqFixture(t)
	losePage(t, fx.segment, fx.batchAt+300)

	// Plant a well-formed record carrying generation 0 above the damage, where
	// the next intact record of the interrupted batch sits.
	at, span := firstDecodableAt(t, fx.segment, fx.batchAt+300+seqTestPageSize)
	stale := make([]byte, span)
	if _, err := encodeRecord(stale, RecordFull, 0, make([]byte, span-recordHeaderSize)); err != nil {
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
		if _, _, _, n, err := decodeRecord(data[at:]); err == nil {
			return at, n
		}
	}
	t.Fatalf("no decodable record at or after offset %d", from)
	return 0, 0
}

// TestOpen_OlderFormatVersionIsRefusedByName pins the version gate.
//
// A directory written under format version 1 carries 7-byte chunk headers. Under
// this build's parser every one of them fails CRC from the first byte, the clean
// prefix comes back empty, and — with nothing decodable above the damage — the
// torn-tail repair would truncate the whole log to zero. The version has to be
// checked before any segment byte is read, and the error has to say which
// version was found rather than reporting the directory as damaged.
func TestOpen_OlderFormatVersionIsRefusedByName(t *testing.T) {
	dir := t.TempDir()
	seedWAL(t, dir, 1024)
	sizesBefore := segmentSizes(t, dir)

	// Rewrite the marker as a version-1 one, byte for byte as that build wrote it.
	buf := make([]byte, walFormatSize)
	copy(buf[0:8], walFormatMagic)
	binary.LittleEndian.PutUint32(buf[8:12], 1)
	binary.LittleEndian.PutUint32(buf[12:16], 1024)
	if err := os.WriteFile(dir+string(os.PathSeparator)+walFormatFilename, buf, 0o600); err != nil {
		t.Fatalf("write v1 marker: %v", err)
	}

	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err == nil {
		db.Close()
		t.Fatal("Open accepted a directory written in an older on-disk format; its " +
			"chunk headers are four bytes shorter, so every record fails CRC and " +
			"the log is truncated away as a torn tail")
	}
	if !errors.Is(err, ErrWALFormatUnsupportedVersion) {
		t.Fatalf("Open error = %v, want ErrWALFormatUnsupportedVersion", err)
	}
	if errors.Is(err, ErrWALFormatMalformed) {
		t.Fatal("an older format version was reported as a MALFORMED marker; the " +
			"file is perfectly intelligible and the remedy is an upgrade procedure, " +
			"not a corruption investigation")
	}
	var version *WALFormatVersionError
	if !errors.As(err, &version) {
		t.Fatalf("Open error = %v, want a *WALFormatVersionError naming both versions", err)
	}
	if version.Recorded != 1 || version.Supported != walFormatVersion {
		t.Fatalf("version error = recorded %d supported %d, want 1 and %d",
			version.Recorded, version.Supported, walFormatVersion)
	}

	if got := segmentSizes(t, dir); !sameSizes(got, sizesBefore) {
		t.Fatalf("a refused open mutated the segments on disk: %v -> %v",
			sizesBefore, got)
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
		if _, err := encodeRecord(buf, RecordFull, seq, payload); err != nil {
			t.Fatalf("encodeRecord(seq=%d): %v", seq, err)
		}
		data, recType, got, _, err := decodeRecord(buf)
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
		if _, _, moved, _, err := decodeRecord(buf); err == nil {
			t.Fatalf("a flipped bit in the durability sequence still decoded, as "+
				"generation %d instead of %d; recovery would decide whether to "+
				"truncate on a number the checksum never covered", moved, seq)
		}
	}
}

// TestSegment_DurabilitySequenceAdvancesOnlyOnCompletedSync pins the meaning of
// the counter, which is the whole basis of the acknowledgement proof: a record
// carrying generation g+1 exists only because Sync #(g+1) RETURNED.
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
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if seg.durabilitySeq != 2 {
		t.Fatalf("durability generation = %d after two Syncs, want 2", seg.durabilitySeq)
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
