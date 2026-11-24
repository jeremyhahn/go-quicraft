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

//go:build quicraft_errfs

// Fault modes that put the medium in a state the plain error injection cannot
// produce, and the WAL recovery paths that only those states reach.
//
// The rest of this suite returns an error INSTEAD of performing the operation,
// which models an open or an unlink faithfully and models a write or a flush
// not at all:
//
//   - A pwrite(2) that hits ENOSPC or EIO part way through returns n < len(p)
//     with those n bytes on the medium. errfs.ShortWrite reproduces that, and
//     the half-written record it leaves behind is the only input that reaches
//     the torn-tail branch of segment.ReadAll and the on-disk repair that
//     follows it.
//   - A failed fsync does not leave the data pending. The kernel drops the
//     dirty pages (fsyncgate), so everything written since the last successful
//     flush is gone. errfs.LoseUnsyncedData reproduces that, and it is the
//     only fault under which a WAL that acknowledges before flushing behaves
//     differently from one that flushes first.
//
// Both tests run with real fsyncs — WithNoSync is deliberately absent — and
// neither closes the faulted DB: the process that owned it is modelled as
// gone, which is also what keeps the damaged tail on disk for the reopen to
// find.
package errfs_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/test/errfs"
)

// tornPayloadSize keeps every entry these tests write larger than a block, so
// each SaveEntries seals at least one block and the WriteAt the fault lands on
// is a record flush rather than an incidental metadata write.
const tornPayloadSize = walBlockSize + 128

// shardDirFor returns the WAL sub-directory a Raft shard hashes to. It mirrors
// waldb's own shardDirName over DefaultShardCount buckets.
func shardDirFor(t *testing.T, dir string, shardID uint64) string {
	t.Helper()
	return filepath.Join(dir, "shard-"+itoa(int(shardID%16)))
}

// itoa renders a small non-negative int without pulling fmt into the helper.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// activeSegmentPath returns the highest-numbered segment file in a shard
// directory, which is the one the WAL is currently appending to. Every reopen
// seals the recovered segment and starts a new one, so the segment a write
// lands in is not the segment the previous run wrote.
func activeSegmentPath(t *testing.T, shardDir string) string {
	t.Helper()
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		t.Fatalf("read shard dir %s: %v", shardDir, err)
	}
	active := ""
	for _, de := range entries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".log" {
			continue
		}
		if de.Name() > active {
			active = de.Name()
		}
	}
	if active == "" {
		t.Fatalf("shard dir %s holds no segment file", shardDir)
	}
	return filepath.Join(shardDir, active)
}

// segmentBytes returns the full on-disk image of a segment. Segments are
// pre-allocated to their maximum size on creation, so their LENGTH says
// nothing about what they hold and only the bytes can answer whether a write
// reached the medium.
func segmentBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment %s: %v", path, err)
	}
	return data
}

// runLength returns the longest run of byte b in data. The tests use it to ask
// whether a specific entry's payload is still on the medium: testEntryWithPayload
// fills Cmd with a single repeated byte, so a long run of that byte is that
// entry's payload and nothing else.
func runLength(data []byte, b byte) int {
	best, cur := 0, 0
	for _, c := range data {
		if c != b {
			cur = 0
			continue
		}
		cur++
		if cur > best {
			best = cur
		}
	}
	return best
}

// TestErrorFS_WAL_ShortWriteLeavesTornTailAndIsRepaired drives the WAL's
// torn-tail recovery with a real torn tail.
//
// A short write puts a fragment of a record on the medium and then fails, so
// the segment ends with bytes that decode to nothing. The failing batch is
// never acknowledged, and recovery must (a) replay the acknowledged prefix
// whole, (b) admit nothing from the fragment, and (c) CUT the fragment off the
// segment, because a torn tail left in place is re-detected on every
// subsequent boot and each of those boots discards the segments written since
// the last one.
//
// The rollback the write failure would normally run is faulted too. That is
// not belt-and-braces: rollback truncates the fragment away, so with it
// working the torn tail never survives to be recovered from, and the recovery
// path this test exists for is unreachable. A process killed between the
// failed write and its rollback is exactly the state modelled here.
func TestErrorFS_WAL_ShortWriteLeavesTornTailAndIsRepaired(t *testing.T) {
	const (
		shardID   = uint64(41)
		replicaID = uint64(1)
		acked     = uint64(6)
	)

	dir := t.TempDir()
	shardDir := shardDirFor(t, dir, shardID)

	// Phase 1: an acknowledged, really-fsynced baseline.
	baseline := make([]logdb.Entry, 0, acked)
	db := openDB(t, dir)
	for i := uint64(1); i <= acked; i++ {
		e := testEntryWithPayload(i, 1, tornPayloadSize)
		if err := db.SaveEntries(shardID, replicaID, []logdb.Entry{e}); err != nil {
			t.Fatalf("baseline SaveEntries(%d): %v", i, err)
		}
		baseline = append(baseline, e)
	}
	mustClose(t, db)

	// Phase 2: reopen and tear the next record. The fault is armed AFTER Open
	// so recovery runs against a healthy filesystem and the first faulted
	// WriteAt is the record flush.
	efs := errfs.New(waldb.DefaultFS(), 0, errInjectedIO).
		OnlyOps(errfs.OpWriteAt, errfs.OpTruncate).
		ShortWrite(walBlockSize / 2).
		Times(0)

	faulted, err := waldb.Open(dir,
		waldb.WithFS(efs),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(64*1024),
	)
	if err != nil {
		t.Fatalf("reopen with the fault disarmed: %v", err)
	}
	efs.Reset(1, errInjectedIO)

	// Every reopen seals the recovered segment and appends to a fresh one, and
	// a fresh segment is pre-allocated to its full size, so the torn record has
	// to be found in the segment's BYTES rather than in its length.
	torn := activeSegmentPath(t, shardDir)
	tornPattern := byte((acked + 1) & 0xFF)
	if before := runLength(segmentBytes(t, torn), tornPattern); before >= tornPayloadSize/4 {
		t.Fatalf("segment %s already holds a %d-byte run of 0x%02x before the faulted write; "+
			"the pattern does not identify the torn record", torn, before, tornPattern)
	}

	writeErr := faulted.SaveEntries(shardID, replicaID,
		[]logdb.Entry{testEntryWithPayload(acked+1, 2, tornPayloadSize)})
	if writeErr == nil {
		t.Fatal("SaveEntries returned nil after a short write: a partially written record was acknowledged")
	}
	if !errors.Is(writeErr, errInjectedIO) {
		t.Fatalf("SaveEntries error does not wrap the injected fault: %v", writeErr)
	}
	if efs.Fires() < 2 {
		t.Fatalf("fault fired %d times, want at least 2 (the torn write and the rollback truncate); "+
			"with the rollback allowed to run the torn tail is erased and this test proves nothing", efs.Fires())
	}

	// The short write must have reached the medium. This is the property the
	// whole mode exists for: an injection that returns before delegating
	// leaves the file byte-identical, and then there is no torn tail to
	// recover from and everything below is vacuous.
	tornRun := runLength(segmentBytes(t, torn), tornPattern)
	if tornRun < tornPayloadSize/4 {
		t.Fatalf("segment %s holds only a %d-byte run of the torn record's payload; "+
			"the short write never reached the medium, so the fault modelled a failure "+
			"the disk never saw (fires=%d, err=%v)", torn, tornRun, efs.Fires(), writeErr)
	}

	// The owning process is modelled as dead: faulted is deliberately never
	// closed, so nothing rewrites or truncates the torn tail behind our back.

	// Phase 3: recover on a healthy filesystem.
	recovered := openDB(t, dir)

	// The fragment must be CUT from the segment, not merely skipped in memory.
	// A torn tail left on disk is re-detected by every later boot, and each of
	// those boots discards every segment written since the previous one.
	if _, err := os.Stat(torn); err == nil {
		if left := runLength(segmentBytes(t, torn), tornPattern); left >= tornPayloadSize/4 {
			t.Fatalf("segment %s still holds a %d-byte run of the torn record after recovery: "+
				"the torn tail was not truncated away", torn, left)
		}
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat torn segment %s after recovery: %v", torn, err)
	}

	assertEntriesPresentWithCmd(t, recovered, shardID, replicaID, 1, acked+1, 1, baseline)
	assertEntryAbsent(t, recovered, shardID, replicaID, acked+1)

	// Phase 4: the repair must be durable and the WAL must extend from it.
	next := testEntryWithPayload(acked+1, 3, tornPayloadSize)
	if err := recovered.SaveEntries(shardID, replicaID, []logdb.Entry{next}); err != nil {
		t.Fatalf("SaveEntries after torn-tail repair: %v", err)
	}
	mustClose(t, recovered)

	final := openDB(t, dir)
	defer mustClose(t, final)
	assertEntriesPresentWithCmd(t, final, shardID, replicaID, 1, acked+1, 1, baseline)

	got, err := final.GetEntries(shardID, replicaID, acked+1, acked+2, 0)
	if err != nil {
		t.Fatalf("GetEntries(%d) after repair: %v", acked+1, err)
	}
	if len(got) != 1 {
		t.Fatalf("GetEntries(%d) after repair: got %d entries, want 1", acked+1, len(got))
	}
	if got[0].Term != 3 || !bytes.Equal(got[0].Cmd, next.Cmd) {
		t.Fatalf("entry %d written after the repair came back wrong: term=%d (want 3), cmd equal=%v",
			acked+1, got[0].Term, bytes.Equal(got[0].Cmd, next.Cmd))
	}
}

// TestErrorFS_WAL_FailedFsyncLosesOnlyUnacknowledgedData drives the WAL's
// fail-stop-on-fsync-failure path with a fault that has the cost a real fsync
// failure has.
//
// Under LoseUnsyncedData the failing flush takes every byte written since the
// previous successful flush with it, so what survives on the medium is exactly
// what an EARLIER fsync covered. That makes the acknowledgement contract
// testable: everything SaveEntries returned nil for must come back, and the
// batch that failed must not.
//
// It is also the only fault in this suite that fails when the WAL stops
// flushing. Delete the flush before the acknowledgement and the acknowledged
// records join the lost set — the returning-an-error mode cannot tell the
// difference, because it leaves the data on the medium either way.
func TestErrorFS_WAL_FailedFsyncLosesOnlyUnacknowledgedData(t *testing.T) {
	const (
		shardID   = uint64(42)
		replicaID = uint64(1)
		acked     = uint64(4)
	)

	dir := t.TempDir()
	shardDir := shardDirFor(t, dir, shardID)

	efs := errfs.New(waldb.DefaultFS(), 0, errInjectedIO).
		OnlyOps(errfs.OpSync).
		LoseUnsyncedData()

	db, err := waldb.Open(dir,
		waldb.WithFS(efs),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(64*1024),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Phase 1: acknowledged writes, each one flushed for real.
	baseline := make([]logdb.Entry, 0, acked)
	for i := uint64(1); i <= acked; i++ {
		e := testEntryWithPayload(i, 1, tornPayloadSize)
		if err := db.SaveEntries(shardID, replicaID, []logdb.Entry{e}); err != nil {
			t.Fatalf("baseline SaveEntries(%d): %v", i, err)
		}
		baseline = append(baseline, e)
	}

	ackedState := testState(4, 1, acked)
	if err := db.SaveNodeState(shardID, replicaID, ackedState); err != nil {
		t.Fatalf("baseline SaveNodeState: %v", err)
	}

	// Segments are pre-allocated to their full size on creation, so the
	// acknowledged state has to be captured as BYTES: the file's length is
	// already its maximum and never moves.
	segment := activeSegmentPath(t, shardDir)
	ackedImage := segmentBytes(t, segment)

	// Phase 2: the next flush fails and takes its batch with it.
	efs.Reset(1, errInjectedIO)

	lostErr := db.SaveEntries(shardID, replicaID,
		[]logdb.Entry{testEntryWithPayload(acked+1, 9, tornPayloadSize)})
	if lostErr == nil {
		t.Fatal("SaveEntries returned nil after the fsync failed: a batch the medium never took was acknowledged")
	}
	if !errors.Is(lostErr, errInjectedIO) {
		t.Fatalf("SaveEntries error does not wrap the injected fault: %v", lostErr)
	}
	if efs.Fires() != 1 {
		t.Fatalf("fault fired %d times, want exactly 1", efs.Fires())
	}

	// The lost batch must be gone from the medium, not merely unreported: the
	// segment has to be byte-identical to what the last successful fsync left.
	if lostImage := segmentBytes(t, segment); !bytes.Equal(lostImage, ackedImage) {
		t.Fatalf("segment %s differs from its last acknowledged image after the failed fsync: "+
			"the unsynced bytes were not discarded, so the fault did not model fsyncgate "+
			"and the acknowledgement contract below is untested", segment)
	}

	// A failed fsync is terminal: neither the record nor a rollback of it can
	// be proven durable, so the DB must refuse every further write.
	poisonedErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(acked+2, 9)})
	if !errors.Is(poisonedErr, waldb.ErrDBPoisoned) {
		t.Fatalf("SaveEntries after an fsync failure returned %v, want ErrDBPoisoned", poisonedErr)
	}

	// The owning process is modelled as dead; db is deliberately not closed.

	// Phase 3: recovery keeps every acknowledgement and invents nothing.
	recovered := openDB(t, dir)
	defer mustClose(t, recovered)

	assertEntriesPresentWithCmd(t, recovered, shardID, replicaID, 1, acked+1, 1, baseline)
	assertEntryAbsent(t, recovered, shardID, replicaID, acked+1)
	assertEntryAbsent(t, recovered, shardID, replicaID, acked+2)

	gotState, err := recovered.GetState(shardID, replicaID)
	if err != nil {
		t.Fatalf("GetState after recovery: %v", err)
	}
	if gotState != ackedState {
		t.Fatalf("hard state after recovery is %+v, want the acknowledged %+v", gotState, ackedState)
	}
}
