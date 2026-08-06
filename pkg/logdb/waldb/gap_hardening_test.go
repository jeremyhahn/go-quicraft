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

// Regression tests for the medium/low-severity hardening findings:
// fatal replay of malformed entry batches (M1), write-side Cmd size cap
// (M1), span-derived EntryRange and gap-rejecting IterateEntries (M1/M6),
// locked shard shutdown (M4), compaction-floor propagation through
// Reencrypt (M6), legacy snapshot format detection (M7), non-wedging
// segment rotation (L1), 32-bit-safe snapshot length guard (L2), and
// functional-option validation in Open (L3).

package waldb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// newBareShard returns a walShard with initialized maps and no directory,
// for unit tests that drive replay functions directly.
func newBareShard() *walShard {
	return &walShard{
		indexes:       make(map[nodeKey]*index),
		entries:       make(map[nodeKey]map[uint64]logdb.Entry),
		states:        make(map[nodeKey]logdb.State),
		bootstraps:    make(map[nodeKey]logdb.Bootstrap),
		snapshots:     make(map[nodeKey]logdb.Snapshot),
		compactedTo:   make(map[nodeKey]uint64),
		knownSegments: make(map[uint64]bool),
		segmentEpochs: make(map[uint64]uint64),
	}
}

// writeRawShardRecord writes one raw WAL record (payload type byte included
// in payload) into a fresh segment inside the given WAL shard directory, so
// a following Open replays it.
func writeRawShardRecord(t *testing.T, walDir string, shardIdx int, payload []byte) {
	t.Helper()
	shardDir := filepath.Join(walDir, shardDirName(shardIdx))
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	seg, _, err := createSegment(defaultFS(), shardDir, 0, 1024, 64*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	seg.noSync = true
	if _, err := seg.Write(payload); err != nil {
		t.Fatalf("segment.Write: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("segment.Close: %v", err)
	}
}

// findRawShardRecord returns the last raw WAL record of the given payload
// type from the WAL shard directory, so a test can assert on the bytes the
// write path actually emitted.
func findRawShardRecord(t *testing.T, walDir string, shardIdx int, payloadType byte) []byte {
	t.Helper()
	shardDir := filepath.Join(walDir, shardDirName(shardIdx))
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var found []byte
	for _, e := range entries {
		f, err := defaultFS().Open(filepath.Join(shardDir, e.Name()))
		if err != nil {
			t.Fatalf("open %s: %v", e.Name(), err)
		}
		seg := &segment{f: f, dir: shardDir, blockSize: 1024, blockBuf: make([]byte, 1024)}
		recs, _, err := seg.ReadAll()
		if closeErr := f.Close(); closeErr != nil {
			t.Fatalf("file.Close: %v", closeErr)
		}
		if err != nil {
			t.Fatalf("ReadAll %s: %v", e.Name(), err)
		}
		for _, rec := range recs {
			if len(rec) > 0 && rec[0] == payloadType {
				found = rec
			}
		}
	}
	if found == nil {
		t.Fatalf("no record of payload type %#x in %s", payloadType, shardDir)
	}
	return found
}

// --- M1: fatal replay of malformed entry batch records ---

// TestOpenFailsOnTruncatedEntryBatch verifies that a CRC-valid entry batch
// record that ends mid-entry fails recovery with ErrTruncatedEntryRecord
// instead of soft-skipping the remainder and leaving a silent log hole.
func TestOpenFailsOnTruncatedEntryBatch(t *testing.T) {
	dir := t.TempDir()

	// Payload type + 40 garbage bytes: shorter than the 76-byte entry header.
	payload := make([]byte, 1+40)
	payload[0] = payloadTypeEntries
	writeRawShardRecord(t, dir, 1, payload)

	_, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if !errors.Is(err, ErrTruncatedEntryRecord) {
		t.Fatalf("Open = %v, want ErrTruncatedEntryRecord", err)
	}
	var re *RecoveryError
	if !errors.As(err, &re) {
		t.Fatalf("Open error %v is not a RecoveryError", err)
	}
}

// TestOpenFailsOnOversizedReplayCmdLen verifies that an entry record whose
// cmdLen exceeds maxCmdLen fails recovery with ErrCmdTooLarge instead of
// soft-skipping the rest of the record.
func TestOpenFailsOnOversizedReplayCmdLen(t *testing.T) {
	dir := t.TempDir()

	payload := make([]byte, 1+entryRecordHeaderSize)
	payload[0] = payloadTypeEntries
	binary.LittleEndian.PutUint64(payload[1:], 1)                      // shardID
	binary.LittleEndian.PutUint64(payload[9:], 1)                      // replicaID
	binary.LittleEndian.PutUint64(payload[17:], 1)                     // raftIndex
	binary.LittleEndian.PutUint64(payload[25:], 1)                     // term
	binary.LittleEndian.PutUint32(payload[1+72:], uint32(maxCmdLen+1)) // cmdLen
	writeRawShardRecord(t, dir, 1, payload)

	_, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if !errors.Is(err, ErrCmdTooLarge) {
		t.Fatalf("Open = %v, want ErrCmdTooLarge", err)
	}
}

// TestReplayEntriesTruncatedHeaderIsFatal verifies the unit-level contract:
// a batch that ends mid-header aborts with ErrTruncatedEntryRecord.
func TestReplayEntriesTruncatedHeaderIsFatal(t *testing.T) {
	s := newBareShard()
	if err := s.replayEntries(make([]byte, entryRecordHeaderSize-1), 0); !errors.Is(err, ErrTruncatedEntryRecord) {
		t.Fatalf("replayEntries = %v, want ErrTruncatedEntryRecord", err)
	}
}

// TestReplayEntriesTruncatedCmdIsFatal verifies that a batch whose declared
// cmdLen extends past the record end aborts with ErrTruncatedEntryRecord.
func TestReplayEntriesTruncatedCmdIsFatal(t *testing.T) {
	s := newBareShard()
	buf := make([]byte, entryRecordHeaderSize+3) // cmdLen says 8, only 3 present
	binary.LittleEndian.PutUint64(buf[0:], 1)    // shardID
	binary.LittleEndian.PutUint64(buf[8:], 1)    // replicaID
	binary.LittleEndian.PutUint64(buf[16:], 1)   // raftIndex
	binary.LittleEndian.PutUint64(buf[24:], 1)   // term
	binary.LittleEndian.PutUint32(buf[72:], 8)   // cmdLen
	if err := s.replayEntries(buf, 0); !errors.Is(err, ErrTruncatedEntryRecord) {
		t.Fatalf("replayEntries = %v, want ErrTruncatedEntryRecord", err)
	}
}

// --- M1: write-side Cmd cap symmetry ---

// TestWriteEntriesRejectsOversizedCmd verifies SaveState refuses an entry
// whose Cmd exceeds maxCmdLen with ErrCmdTooLarge, keeping the write side
// symmetric with replay so no durable-but-unreadable entry can exist.
func TestWriteEntriesRejectsOversizedCmd(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	oversized := []logdb.Entry{{Index: 1, Term: 1, Cmd: make([]byte, maxCmdLen+1)}}
	if err := db.SaveEntries(1, 1, oversized); !errors.Is(err, ErrCmdTooLarge) {
		t.Fatalf("SaveEntries = %v, want ErrCmdTooLarge", err)
	}

	// Nothing may have been persisted, and the DB stays usable.
	if err := db.SaveEntries(1, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries after rejection: %v", err)
	}
	got, gerr := db.GetEntries(1, 1, 1, 4, 0)
	if gerr != nil || len(got) != 3 {
		t.Fatalf("GetEntries = (%d entries, %v), want 3 entries", len(got), gerr)
	}
}

// --- M1: EntryRange derives a contiguous span ---

// TestEntryRangeRejectsIndexGap verifies EntryRange returns
// ErrLogGapDetected when the entry index holds a hole, instead of a bare
// count that would make LogReader compute a short lastIndex.
func TestEntryRangeRejectsIndexGap(t *testing.T) {
	db := openTestDB(t)
	if err := db.SaveEntries(1, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	// Force a hole into the index (indices 1..3 then 10).
	shard := db.getShard(1)
	key := nodeKey{ShardID: 1, ReplicaID: 1}
	shard.mu.Lock()
	shard.indexes[key].Append(indexEntry{Index: 10, Term: 1, Size: 79})
	shard.mu.Unlock()

	if _, _, err := db.EntryRange(1, 1); !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("EntryRange = %v, want ErrLogGapDetected", err)
	}
}

// --- M6/L4: IterateEntries rejects shifted and holed ranges ---

// TestIterateEntriesRejectsShiftedRange verifies that a request starting
// below the first stored entry (with no compaction marker explaining the
// absence) fails with ErrLogGapDetected instead of silently returning a
// shifted range with a hole at its front.
func TestIterateEntriesRejectsShiftedRange(t *testing.T) {
	db := openTestDB(t)
	if err := db.SaveEntries(1, 1, makeEntries(5, 4, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 2, 9, 0); !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("IterateEntries = %v, want ErrLogGapDetected", err)
	}
}

// TestIterateEntriesRejectsInteriorHole verifies that a hole inside the
// requested range fails with ErrLogGapDetected instead of returning fewer
// entries with a nil error.
func TestIterateEntriesRejectsInteriorHole(t *testing.T) {
	db := openTestDB(t)
	if err := db.SaveEntries(1, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	// Force a hole: index and entry map jump from 3 to 10.
	shard := db.getShard(1)
	key := nodeKey{ShardID: 1, ReplicaID: 1}
	shard.mu.Lock()
	shard.indexes[key].Append(indexEntry{Index: 10, Term: 1, Size: 79})
	shard.entries[key][10] = logdb.Entry{Index: 10, Term: 1, Cmd: []byte("cmd")}
	shard.mu.Unlock()

	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 11, 0); !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("IterateEntries = %v, want ErrLogGapDetected", err)
	}
}

// --- M4: shard shutdown is atomic with respect to writers ---

// TestShardCloseGateBlocksSegmentResurrection verifies that after a shard
// is closed, a writer that raced past the DB-level gate cannot recreate an
// active segment: ensureActiveSegment refuses with ErrClosed.
func TestShardCloseGateBlocksSegmentResurrection(t *testing.T) {
	db := openTestDB(t)
	if err := db.SaveEntries(1, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	shard := db.getShard(1)
	if err := shard.close(); err != nil {
		t.Fatalf("shard close: %v", err)
	}

	shard.mu.Lock()
	err := shard.ensureActiveSegment()
	active := shard.activeSegment
	shard.mu.Unlock()

	if !errors.Is(err, logdb.ErrClosed) {
		t.Fatalf("ensureActiveSegment after close = %v, want ErrClosed", err)
	}
	if active != nil {
		t.Fatal("activeSegment recreated after close")
	}
}

// TestCloseConcurrentWithWriters exercises DB.Close racing SaveState
// writers. Run under -race this fails if shutdown touches segment state
// without the shard lock (the M4 data race on segment.closed /
// activeSegment). Writers must observe either success or ErrClosed — never
// a torn write or a panic.
func TestCloseConcurrentWithWriters(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				err := db.SaveEntries(uint64(w+1), 1, makeEntries(uint64(i*3+1), 3, 1))
				if err != nil {
					if !errors.Is(err, logdb.ErrClosed) && !errors.Is(err, ErrClosed) {
						t.Errorf("writer %d: unexpected error %v", w, err)
					}
					return
				}
			}
		}(w)
	}

	time.Sleep(5 * time.Millisecond)
	if err := db.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	wg.Wait()
}

// --- M6: Reencrypt carries the compaction floor ---

// TestReencryptPropagatesCompactionFloor verifies that a migrated WAL
// keeps the source's compaction marker: requests at or below the floor
// return ErrCompacted, and the live suffix survives intact.
func TestReencryptPropagatesCompactionFloor(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "src")
	dstDir := filepath.Join(t.TempDir(), "dst")

	src, err := Open(srcDir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open src: %v", err)
	}
	if err := src.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := src.RemoveEntriesTo(1, 1, 4); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	n, err := Reencrypt(srcDir, nil, dstDir, nil, nil)
	if err != nil {
		t.Fatalf("Reencrypt: %v", err)
	}
	if n != 1 {
		t.Fatalf("re-encrypted %d nodes, want 1", n)
	}

	dst, err := Open(dstDir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open dst: %v", err)
	}
	defer dst.Close()

	floor, err := dst.CompactedTo(1, 1)
	if err != nil {
		t.Fatalf("CompactedTo: %v", err)
	}
	if floor != 4 {
		t.Fatalf("dst compaction floor = %d, want 4", floor)
	}
	if _, _, err := dst.IterateEntries(nil, 0, 1, 1, 3, 11, 0); !errors.Is(err, logdb.ErrCompacted) {
		t.Fatalf("IterateEntries below floor = %v, want ErrCompacted", err)
	}
	got, _, err := dst.IterateEntries(nil, 0, 1, 1, 5, 11, 0)
	if err != nil {
		t.Fatalf("IterateEntries above floor: %v", err)
	}
	if len(got) != 6 || got[0].Index != 5 || got[5].Index != 10 {
		t.Fatalf("live suffix = %d entries [%d..], want 6 entries [5..10]",
			len(got), got[0].Index)
	}
}

// --- M7: legacy snapshot format fails loud ---

// TestOpenFailsOnLegacySnapshotFormat verifies that a snapshot record with
// the reserved V1 payload type fails recovery with ErrLegacySnapshotFormat
// instead of soft-skipping it and booting the node with empty membership.
func TestOpenFailsOnLegacySnapshotFormat(t *testing.T) {
	dir := t.TempDir()

	// A V1-layout snapshot record: base header + zero fpLen + old membership
	// section (ConfigChangeID + four empty map counts, no witness fields).
	payload := make([]byte, 1+snapshotRecordBaseSize+8+4*4)
	payload[0] = payloadTypeSnapshotV1
	binary.LittleEndian.PutUint64(payload[1:], 1)   // shardID
	binary.LittleEndian.PutUint64(payload[9:], 1)   // replicaID
	binary.LittleEndian.PutUint64(payload[17:], 10) // snapshot index
	binary.LittleEndian.PutUint64(payload[25:], 2)  // snapshot term
	writeRawShardRecord(t, dir, 1, payload)

	_, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if !errors.Is(err, ErrLegacySnapshotFormat) {
		t.Fatalf("Open = %v, want ErrLegacySnapshotFormat", err)
	}
}

// TestOpenFailsOnLegacySnapshotFormatV2 verifies that a snapshot record in
// the witness-policy (V2) layout — whose membership section has no version
// byte — fails recovery with ErrLegacySnapshotFormatV2 instead of being
// parsed with the current layout, which would consume the ConfigChangeID
// low byte as a version byte and shift every later field.
func TestOpenFailsOnLegacySnapshotFormatV2(t *testing.T) {
	dir := t.TempDir()

	// A V2-layout snapshot record: base header + zero fpLen + membership
	// section without the leading version byte
	// (ConfigChangeID + MaxWitnesses + PolicyFlags + four empty map counts).
	payload := make([]byte, 1+snapshotRecordBaseSize+8+8+1+4*4)
	payload[0] = payloadTypeSnapshotV2
	binary.LittleEndian.PutUint64(payload[1:], 1)   // shardID
	binary.LittleEndian.PutUint64(payload[9:], 1)   // replicaID
	binary.LittleEndian.PutUint64(payload[17:], 10) // snapshot index
	binary.LittleEndian.PutUint64(payload[25:], 2)  // snapshot term
	writeRawShardRecord(t, dir, 1, payload)

	_, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if !errors.Is(err, ErrLegacySnapshotFormatV2) {
		t.Fatalf("Open = %v, want ErrLegacySnapshotFormatV2", err)
	}
}

// TestOpenFailsOnUnsupportedMembershipVersion verifies the in-section
// version gate behind the payload-type gate: a current-payload-type record
// whose membership section carries an unknown version byte fails recovery
// rather than having its fields interpreted at the wrong offsets.
func TestOpenFailsOnUnsupportedMembershipVersion(t *testing.T) {
	dir := t.TempDir()

	payload := make([]byte, 1+snapshotRecordBaseSize+minMembershipSize)
	payload[0] = payloadTypeSnapshot
	binary.LittleEndian.PutUint64(payload[1:], 1)   // shardID
	binary.LittleEndian.PutUint64(payload[9:], 1)   // replicaID
	binary.LittleEndian.PutUint64(payload[17:], 10) // snapshot index
	binary.LittleEndian.PutUint64(payload[25:], 2)  // snapshot term
	payload[1+snapshotRecordBaseSize] = membershipSectionVersion + 1
	writeRawShardRecord(t, dir, 1, payload)

	_, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if !errors.Is(err, ErrUnsupportedMembershipVersion) {
		t.Fatalf("Open = %v, want ErrUnsupportedMembershipVersion", err)
	}
}

// TestSnapshotRecordCarriesMembershipVersionByte verifies the write path
// stamps membershipSectionVersion as the first byte of the membership
// section, at the offset the replayer reads it from.
func TestSnapshotRecordCarriesMembershipVersionByte(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	snap := logdb.Snapshot{
		Index: 4, Term: 1, Filepath: "snap-4.dat",
		Membership: logdb.Membership{ConfigChangeID: 3},
	}
	if err := db.SaveSnapshot(1, 1, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rec := findRawShardRecord(t, dir, 1, payloadTypeSnapshot)
	memberOff := 1 + snapshotRecordBaseSize + len(snap.Filepath)
	if len(rec) <= memberOff {
		t.Fatalf("record length %d does not reach membership section at %d", len(rec), memberOff)
	}
	if rec[memberOff] != membershipSectionVersion {
		t.Fatalf("membership version byte = %d, want %d", rec[memberOff], membershipSectionVersion)
	}
	if got, want := len(rec)-memberOff, membershipSize(snap.Membership); got != want {
		t.Fatalf("membership section = %d bytes, want membershipSize()=%d", got, want)
	}
}

// TestSnapshotRoundTripUsesCurrentPayloadType verifies the current write
// path emits the discriminated payload type and replays cleanly with
// the full witness-policy membership intact.
func TestSnapshotRoundTripUsesCurrentPayloadType(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	snap := logdb.Snapshot{
		Index: 7, Term: 3, Filepath: "snap-7.dat",
		Membership: logdb.Membership{
			ConfigChangeID:             9,
			MaxWitnesses:               2,
			RequireFullReplicaMajority: true,
			Addresses:                  map[uint64]string{1: "127.0.0.1:1"},
		},
	}
	if err := db.SaveSnapshot(1, 1, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	got, err := db2.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Index != 7 || got.Membership.MaxWitnesses != 2 || !got.Membership.RequireFullReplicaMajority {
		t.Fatalf("snapshot after reopen = %+v, want witness policy preserved", got)
	}
}

// --- L1: failed rotation must not wedge the shard ---

// truncateFailFile wraps a File and fails Truncate, driving the
// segment.Close error path inside rotateSegment.
type truncateFailFile struct {
	File
}

var errInjectedTruncate = errors.New("injected truncate failure")

func (f *truncateFailFile) Truncate(int64) error { return errInjectedTruncate }

// TestRotateSegmentClearsSegmentOnCloseFailure verifies that when the old
// segment's Close fails mid-rotation, the shard drops the (already closed)
// segment instead of keeping it active — otherwise every later write would
// fail ErrClosed forever without poisoning (a wedged shard).
func TestRotateSegmentClearsSegmentOnCloseFailure(t *testing.T) {
	dir := t.TempDir()
	s := newBareShard()
	s.fs = defaultFS()
	s.dir = dir
	s.opts = dbOptions{blockSize: 1024, maxFileSize: 64 * 1024, noSync: true, fs: defaultFS()}

	seg, _, err := createSegment(s.fs, dir, 0, 1024, 64*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	seg.noSync = true
	if _, err := seg.Write([]byte("payload")); err != nil {
		t.Fatalf("segment.Write: %v", err)
	}
	seg.f = &truncateFailFile{File: seg.f}
	s.activeSegment = seg
	s.activeSegmentID = 0
	s.nextSegmentID = 1
	s.knownSegments[0] = true

	if err := s.rotateSegment(); !errors.Is(err, errInjectedTruncate) {
		t.Fatalf("rotateSegment = %v, want injected truncate failure", err)
	}
	if s.activeSegment != nil {
		t.Fatal("activeSegment still installed after failed Close; shard would be wedged")
	}

	// The shard must recover on the next write by creating a fresh segment.
	if _, err := s.writeRecord([]byte{payloadTypeCompact, 0}); err != nil {
		t.Fatalf("writeRecord after failed rotation: %v", err)
	}
	if s.activeSegment == nil || s.activeSegmentID != 1 {
		t.Fatalf("expected fresh segment 1 after recovery, got id=%d", s.activeSegmentID)
	}
}

// --- L2: 32-bit-safe snapshot filepath length guard ---

// TestReplaySnapshotHugeFpLenDoesNotPanic verifies a corrupt fpLen near
// MaxUint32 is rejected as a truncated record without a slice panic. The
// uint64-domain guard protects 32-bit platforms, where int(fpLen)
// sign-truncates and snapshotRecordBaseSize+fpLen wraps in uint32 arithmetic.
func TestReplaySnapshotHugeFpLenDoesNotPanic(t *testing.T) {
	s := newBareShard()
	data := make([]byte, snapshotRecordBaseSize)
	binary.LittleEndian.PutUint64(data[0:], 1)           // shardID
	binary.LittleEndian.PutUint64(data[8:], 1)           // replicaID
	binary.LittleEndian.PutUint64(data[16:], 5)          // index
	binary.LittleEndian.PutUint16(data[56:], 0)          // compression: none
	binary.LittleEndian.PutUint32(data[58:], 0xFFFFFFFF) // fpLen

	if err := s.replaySnapshot(data, 0); !errors.Is(err, ErrTruncatedSnapshotRecord) {
		t.Fatalf("replaySnapshot error = %v, want ErrTruncatedSnapshotRecord", err)
	}
	if len(s.snapshots) != 0 {
		t.Fatal("corrupt snapshot record was stored")
	}
}

// --- L3: functional-option validation in Open ---

// TestOpenValidatesFunctionalOptions verifies Open rejects invalid
// blockSize/maxFileSize values with the same typed errors as
// Options.Validate. An unvalidated zero block size would make
// segment.bytesNeeded loop forever and hang the commit worker.
func TestOpenValidatesFunctionalOptions(t *testing.T) {
	cases := []struct {
		name string
		opts []Option
		want error
	}{
		{"zero block size", []Option{WithBlockSize(0)}, ErrInvalidBlockSize},
		{"non-power-of-two block size", []Option{WithBlockSize(3000)}, ErrInvalidBlockSize},
		{"block size below header", []Option{WithBlockSize(4)}, ErrInvalidBlockSize},
		{"zero max file size", []Option{WithMaxFileSize(0)}, ErrInvalidMaxFileSize},
		{"negative max file size", []Option{WithMaxFileSize(-1)}, ErrInvalidMaxFileSize},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]Option{WithNoSync(true)}, tc.opts...)
			if _, err := Open(t.TempDir(), opts...); !errors.Is(err, tc.want) {
				t.Fatalf("Open = %v, want %v", err, tc.want)
			}
		})
	}
}

// --- Fatal replay of every malformed metadata record type ---

// TestOpenFailsOnMalformedMetadataRecords verifies that a parse failure in
// ANY record type is fatal, not a warn-and-skip. Every record here already
// passed its chunk CRC, so a decode failure is corruption or format skew: a
// skipped State record lets an OLDER state win last-writer-wins (Term/Vote
// regression => double vote in one term), a skipped Snapshot record boots the
// node with empty membership, a skipped compaction marker resets the floor to
// 0, and a skipped tombstone resurrects a removed replica.
func TestOpenFailsOnMalformedMetadataRecords(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		want    error
	}{
		{
			name:    "state too short",
			payload: append([]byte{payloadTypeState}, make([]byte, stateRecordSize-1)...),
			want:    ErrTruncatedStateRecord,
		},
		{
			name:    "state trailing bytes",
			payload: append([]byte{payloadTypeState}, make([]byte, stateRecordSize+1)...),
			want:    ErrTruncatedStateRecord,
		},
		{
			name:    "compaction too short",
			payload: append([]byte{payloadTypeCompact}, make([]byte, compactionRecordSize-1)...),
			want:    ErrTruncatedCompactionRecord,
		},
		{
			name:    "compaction trailing bytes",
			payload: append([]byte{payloadTypeCompact}, make([]byte, compactionRecordSize+1)...),
			want:    ErrTruncatedCompactionRecord,
		},
		{
			name:    "remove-node too short",
			payload: append([]byte{payloadTypeRemoveNode}, make([]byte, removeNodeRecordSize-1)...),
			want:    ErrTruncatedRemoveNodeRecord,
		},
		{
			name:    "remove-node trailing bytes",
			payload: append([]byte{payloadTypeRemoveNode}, make([]byte, removeNodeRecordSize+1)...),
			want:    ErrTruncatedRemoveNodeRecord,
		},
		{
			name:    "bootstrap too short",
			payload: append([]byte{payloadTypeBootstrap}, make([]byte, bootstrapRecordBaseSize-1)...),
			want:    ErrTruncatedBootstrapRecord,
		},
		{
			// Zero declared addresses but a trailing byte the writer never emits.
			name:    "bootstrap trailing bytes",
			payload: append([]byte{payloadTypeBootstrap}, make([]byte, bootstrapRecordBaseSize+1)...),
			want:    ErrTruncatedBootstrapRecord,
		},
		{
			name:    "snapshot too short",
			payload: append([]byte{payloadTypeSnapshot}, make([]byte, snapshotRecordBaseSize-1)...),
			want:    ErrTruncatedSnapshotRecord,
		},
		{
			// Base header only: the V2 writer ALWAYS emits at least 33
			// membership bytes, so a wholly absent section is corruption.
			name:    "snapshot missing membership section",
			payload: append([]byte{payloadTypeSnapshot}, make([]byte, snapshotRecordBaseSize)...),
			want:    ErrTruncatedSnapshotRecord,
		},
		{
			name:    "unknown payload type",
			payload: []byte{0xFE, 0x01, 0x02, 0x03},
			want:    ErrUnknownPayloadType,
		},
		{
			// The byte a keyed record decodes to when the barrier is missing.
			name:    "keyed record replayed without a barrier",
			payload: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xAB},
			want:    ErrUnknownPayloadType,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeRawShardRecord(t, dir, 1, tc.payload)

			_, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
			if !errors.Is(err, tc.want) {
				t.Fatalf("Open = %v, want %v", err, tc.want)
			}
			var re *RecoveryError
			if !errors.As(err, &re) {
				t.Fatalf("Open error %v is not a RecoveryError", err)
			}
		})
	}
}

// TestReplaySnapshotAcceptsFullMembershipSection is the positive control for
// the stricter snapshot parser: a record written by writeSnapshot must still
// round-trip, including an empty (but present) membership section.
func TestReplaySnapshotAcceptsFullMembershipSection(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ss := logdb.Snapshot{
		Index: 9, Term: 3, Filepath: "snap-9.bin", FileSize: 128,
		Membership: logdb.Membership{
			ConfigChangeID: 4,
			MaxWitnesses:   2,
			Addresses:      map[uint64]string{1: "n1", 2: "n2"},
		},
	}
	if err := db.SaveSnapshot(1, 1, ss); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	got, err := db2.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Index != 9 || got.Filepath != "snap-9.bin" || got.Membership.ConfigChangeID != 4 ||
		got.Membership.MaxWitnesses != 2 || len(got.Membership.Addresses) != 2 {
		t.Fatalf("snapshot round-trip mismatch: %+v", got)
	}
}
