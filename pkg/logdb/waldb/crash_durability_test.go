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
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// openCreateFlags mirrors the flags createSegment uses.
const openCreateFlags = os.O_RDWR | os.O_CREATE | os.O_TRUNC

const (
	// crashDurBlockSize and crashDurMaxFileSize are deliberately tiny so
	// entries span blocks (every shard write seals at least one block, which
	// is what makes the WriteAt crash point deterministic) and segments rotate
	// every few batches.
	crashDurBlockSize   = 1024
	crashDurMaxFileSize = 8 * 1024

	crashDurReplicaID = 7
	// crashDurRoot is the virtual mount point the WAL directory is created
	// inside. crashFS is purely in-memory; no real path is touched.
	crashDurRoot = "/mnt/crashfs"
)

// crashDurShardIDs hash to four distinct WAL shards, so one SaveState batch
// spans four segment files and a crash can land between two shards' writes.
var crashDurShardIDs = []uint64{1, 2, 3, 4}

// crashDurEntriesPerBatch keeps each shard's per-batch payload above one block
// so a block seal (segment.WriteAt) is guaranteed.
//
// It is a var, not a const, so crashPhaseMidBatchRotation can widen a single
// batch far enough to rotate INSIDE itself. At the default of 2 every rotation
// falls between batches, which is precisely why the suite could not see the
// one flush a mid-batch rotation depends on.
var crashDurEntriesPerBatch = 2

// crashDurEntry regenerates the entry a writer stores at the given position.
// Content is a pure function of (shardID, index, term), so a recovered entry
// can be compared byte for byte against what the caller was told was durable.
func crashDurEntry(shardID, index, term uint64) logdb.Entry {
	cmd := make([]byte, 700+int((shardID*31+index*37)%600))
	for i := range cmd {
		cmd[i] = byte(shardID*7 + index + uint64(i))
	}
	return logdb.Entry{
		Index:       index,
		Term:        term,
		Type:        index % 2,
		Key:         shardID*1000 + index,
		ClientID:    shardID,
		SeriesID:    index,
		RespondedTo: index / 2,
		Cmd:         cmd,
	}
}

// crashDurWriter drives SaveState batches and remembers exactly what the
// caller was TOLD is durable. Only a batch that returned nil is recorded: that
// is the acknowledgement whose loss the durability contract forbids.
type crashDurWriter struct {
	db    *DB
	next  map[uint64]uint64
	acked map[uint64]map[uint64]logdb.Entry
	state map[uint64]logdb.State
	floor map[uint64]uint64
	term  uint64
	batch uint64
}

func newCrashDurWriter(db *DB) *crashDurWriter {
	w := &crashDurWriter{
		db:    db,
		next:  make(map[uint64]uint64),
		acked: make(map[uint64]map[uint64]logdb.Entry),
		state: make(map[uint64]logdb.State),
		floor: make(map[uint64]uint64),
		term:  1,
	}
	for _, sid := range crashDurShardIDs {
		w.next[sid] = 1
		w.acked[sid] = make(map[uint64]logdb.Entry)
	}
	return w
}

// save writes one batch spanning every shard. A nil return is an ack and is
// recorded as durable; any error leaves the writer's expectations untouched,
// because a failed batch may legally lose everything it wrote.
func (w *crashDurWriter) save() error {
	w.batch++
	// Advance the term every third batch so hard-state regression is
	// detectable, and keep Vote a function of the term.
	if w.batch%3 == 0 {
		w.term++
	}

	updates := make([]logdb.Update, 0, len(crashDurShardIDs))
	staged := make(map[uint64][]logdb.Entry, len(crashDurShardIDs))
	states := make(map[uint64]logdb.State, len(crashDurShardIDs))
	for _, sid := range crashDurShardIDs {
		entries := make([]logdb.Entry, 0, crashDurEntriesPerBatch)
		idx := w.next[sid]
		for i := 0; i < crashDurEntriesPerBatch; i++ {
			entries = append(entries, crashDurEntry(sid, idx, w.term))
			idx++
		}
		st := logdb.State{Term: w.term, Vote: w.term%5 + 1, Commit: idx - 1}
		staged[sid] = entries
		states[sid] = st
		updates = append(updates, logdb.Update{
			ShardID:   sid,
			ReplicaID: crashDurReplicaID,
			State:     st,
			Entries:   entries,
		})
	}

	if err := w.db.SaveState(updates); err != nil {
		return err
	}

	for _, sid := range crashDurShardIDs {
		for _, e := range staged[sid] {
			w.acked[sid][e.Index] = e
			w.next[sid] = e.Index + 1
		}
		w.state[sid] = states[sid]
	}
	return nil
}

// mustSave fails the test when a batch that is expected to be acked is not.
func (w *crashDurWriter) mustSave(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := w.save(); err != nil {
			t.Fatalf("SaveState batch %d: %v", w.batch, err)
		}
	}
}

// compactTo records a compaction floor: entries at or below index are no
// longer expected to be recoverable.
func (w *crashDurWriter) compactTo(shardID, index uint64) {
	w.floor[shardID] = index
	for idx := range w.acked[shardID] {
		if idx <= index {
			delete(w.acked[shardID], idx)
		}
	}
}

// ackedIndexes returns the acked indexes for a shard in ascending order.
func (w *crashDurWriter) ackedIndexes(shardID uint64) []uint64 {
	idxs := make([]uint64, 0, len(w.acked[shardID]))
	for idx := range w.acked[shardID] {
		idxs = append(idxs, idx)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] < idxs[j] })
	return idxs
}

// verify asserts the durability contract against a freshly reopened DB: every
// acked entry present and byte-identical, a contiguous range starting at the
// compaction floor, and no hard-state regression. Extra records that a crash
// happened to make durable are permitted — only loss and corruption are not.
func (w *crashDurWriter) verify(t *testing.T, db *DB, phase string) {
	t.Helper()

	for _, sid := range crashDurShardIDs {
		idxs := w.ackedIndexes(sid)
		if len(idxs) == 0 {
			continue
		}
		wantFirst, wantLast := idxs[0], idxs[len(idxs)-1]

		first, span, err := db.EntryRange(sid, crashDurReplicaID)
		if err != nil {
			t.Fatalf("%s: EntryRange(shard %d) after crash: %v (a gap in the "+
				"recovered log means acked entries were lost or holed)", phase, sid, err)
		}
		if first != wantFirst {
			t.Fatalf("%s: shard %d recovered first index %d, want %d: the WAL "+
				"lost an entry it had acknowledged as durable", phase, sid, first, wantFirst)
		}
		if last := first + span - 1; last < wantLast {
			t.Fatalf("%s: shard %d recovered range [%d,%d], want it to cover the "+
				"last acked index %d: the WAL lost acknowledged entries",
				phase, sid, first, last, wantLast)
		}

		got, err := db.GetEntries(sid, crashDurReplicaID, wantFirst, wantLast+1, 0)
		if err != nil {
			t.Fatalf("%s: GetEntries(shard %d, [%d,%d]): %v",
				phase, sid, wantFirst, wantLast, err)
		}
		recovered := make(map[uint64]logdb.Entry, len(got))
		for _, e := range got {
			recovered[e.Index] = e
		}
		for _, idx := range idxs {
			want := w.acked[sid][idx]
			have, ok := recovered[idx]
			if !ok {
				t.Fatalf("%s: shard %d entry %d was acknowledged durable but is "+
					"missing after the crash", phase, sid, idx)
			}
			if !crashDurEntriesEqual(want, have) {
				t.Fatalf("%s: shard %d entry %d differs after the crash:\n want %+v\n have %+v",
					phase, sid, idx, crashDurSummary(want), crashDurSummary(have))
			}
		}

		if floor := w.floor[sid]; floor > 0 {
			compacted, err := db.CompactedTo(sid, crashDurReplicaID)
			if err != nil {
				t.Fatalf("%s: CompactedTo(shard %d): %v", phase, sid, err)
			}
			if compacted < floor {
				t.Fatalf("%s: shard %d compaction floor regressed from %d to %d",
					phase, sid, floor, compacted)
			}
		}

		want := w.state[sid]
		st, err := db.GetState(sid, crashDurReplicaID)
		if err != nil {
			t.Fatalf("%s: GetState(shard %d): %v", phase, sid, err)
		}
		if st.Term < want.Term || st.Commit < want.Commit {
			t.Fatalf("%s: shard %d hard state regressed: recovered %+v, last acked %+v",
				phase, sid, st, want)
		}
		if st.Term == want.Term && st.Vote != want.Vote {
			t.Fatalf("%s: shard %d recovered vote %d in term %d, acked vote %d: "+
				"a lost vote permits a double vote in one term",
				phase, sid, st.Vote, st.Term, want.Vote)
		}
	}
}

// crashDurEntriesEqual compares two entries field by field including payload
// bytes.
func crashDurEntriesEqual(a, b logdb.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && a.Type == b.Type &&
		a.Key == b.Key && a.ClientID == b.ClientID && a.SeriesID == b.SeriesID &&
		a.RespondedTo == b.RespondedTo && bytes.Equal(a.Cmd, b.Cmd)
}

// crashDurSummary renders an entry without dumping its payload.
func crashDurSummary(e logdb.Entry) string {
	return "index=" + itoa(int(e.Index)) + " term=" + itoa(int(e.Term)) +
		" key=" + itoa(int(e.Key)) + " cmdlen=" + itoa(len(e.Cmd))
}

// crashDurHarness owns the modelled filesystem and the DB under test.
type crashDurHarness struct {
	t       *testing.T
	cfs     *crashFS
	dir     string
	barrier WALBarrier
	db      *DB
	w       *crashDurWriter
}

func (h *crashDurHarness) open() *DB {
	h.t.Helper()
	opts := []Option{
		WithFS(h.cfs),
		WithBlockSize(crashDurBlockSize),
		WithMaxFileSize(crashDurMaxFileSize),
	}
	if h.barrier != nil {
		opts = append(opts, WithBarrier(h.barrier))
	}
	db, err := Open(h.dir, opts...)
	if err != nil {
		h.t.Fatalf("Open: %v", err)
	}
	return db
}

// shardDir returns the WAL shard directory a Raft shard hashes to.
func (h *crashDurHarness) shardDir(shardID uint64) string {
	return filepath.Join(h.dir, shardDirName(int(shardID%DefaultShardCount)))
}

// TestWALCrashDurability_AckedWritesSurviveModelledPowerLoss is the durability
// regression test the error-injection doubles cannot express. crashFS keeps a
// durable image that only File.Sync advances, so a write the WAL acked without
// flushing is GONE after Crash — the exact and only symptom of a missing
// fsync, which neither error injection (no failing call exists) nor a SIGKILL
// crash test (page-cache data survives process death) can detect.
//
// MODELLED is not a hedge, it is the boundary. Power loss here is a property
// of crashFS, which is entirely in memory and defines durability as "File.Sync
// returned": no disk is written, no fsync(2) is issued, and the test is
// therefore blind to whether the production osFile.Sync issues one either.
// Stub osFile.Sync to return nil and every case below still passes. What it
// does prove — that the WAL calls Sync on the right file before it
// acknowledges, and that recovery keeps every acknowledgement afterwards — is
// the half of durability that lives in this package. The other half, that the
// call reaches the medium, is asserted by TestOSFileSyncIssuesRealSyscall in
// fs_sync_test.go, and neither test is sufficient without the other.
func TestWALCrashDurability_AckedWritesSurviveModelledPowerLoss(t *testing.T) {
	cases := []struct {
		name      string
		encrypted bool
		crash     func(t *testing.T, h *crashDurHarness)
	}{
		{name: "AfterAck", crash: crashPhaseAfterAck},
		{name: "MidBatch", crash: crashPhaseMidBatch},
		{name: "MidRotation", crash: crashPhaseMidRotation},
		{name: "AfterCompaction", crash: crashPhaseAfterCompaction},
		{name: "MidBatchRotation", crash: crashPhaseMidBatchRotation},
		{name: "EncryptedAfterAck", encrypted: true, crash: crashPhaseAfterAck},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &crashDurHarness{
				t:   t,
				cfs: newCrashFS(crashDurRoot),
				dir: filepath.Join(crashDurRoot, "wal"),
			}
			if tc.encrypted {
				h.barrier = newTestBarrier(t)
			}

			h.db = h.open()
			h.w = newCrashDurWriter(h.db)

			tc.crash(t, h)

			// Reopen over the post-crash image. The pre-crash DB is
			// deliberately never closed: the process that owned it is gone.
			reopened := h.open()
			h.w.db = reopened
			h.w.verify(t, reopened, tc.name+"/reopen")

			// A second run must extend the recovered log, and a clean
			// close/reopen must retain both runs.
			h.w.mustSave(t, 3)
			if err := reopened.Close(); err != nil {
				t.Fatalf("Close after crash recovery: %v", err)
			}
			final := h.open()
			defer final.Close()
			h.w.db = final
			h.w.verify(t, final, tc.name+"/second-reopen")
		})
	}
}

// crashPhaseAfterAck cuts power immediately after SaveState reported success.
func crashPhaseAfterAck(t *testing.T, h *crashDurHarness) {
	t.Helper()
	h.w.mustSave(t, 6)
	h.cfs.Crash()
}

// crashPhaseMidBatch cuts power between the per-shard writes of a multi-shard
// SaveState: two shards have written records that were never fsynced, and the
// batch never returns nil, so nothing in it is acked.
func crashPhaseMidBatch(t *testing.T, h *crashDurHarness) {
	t.Helper()
	h.w.mustSave(t, 4)

	target := h.shardDir(crashDurShardIDs[2])
	var once sync.Once
	h.cfs.setHook(func(op crashOp, path string) {
		if op == crashOpWriteAt && filepath.Dir(path) == target {
			once.Do(h.cfs.Crash)
		}
	})
	err := h.w.save()
	h.cfs.setHook(nil)
	if err == nil {
		t.Fatal("the batch interrupted mid-write returned success; the crash never landed inside it")
	}
}

// crashPhaseMidBatchRotation acks ONE batch large enough to rotate INSIDE
// itself, then cuts power.
//
// This is the state no other phase in this suite produces, and it is the only
// one in which rotateSegment's fsync is load-bearing. SaveState sets batchMode,
// which suppresses writeRecord's inline sync (db.go), and syncSegment flushes
// only s.activeSegment — so when a batch rotates part-way through, the fsync in
// rotateSegment is the ONLY flush the OUTGOING segment ever receives.
// segment.Close does flushBlock + Truncate + Close and does not fsync.
//
// With the default 2 entries per batch every rotation falls BETWEEN batches, so
// deleting that fsync left both the waldb suite and the errfs suite fully
// green while a crash lost entries the WAL had already acknowledged — and Raft
// had already told peers were committed.
func crashPhaseMidBatchRotation(t *testing.T, h *crashDurHarness) {
	t.Helper()
	h.w.mustSave(t, 2)

	prev := crashDurEntriesPerBatch
	// Each entry is comfortably over a block; 12 of them span more than one
	// crashDurMaxFileSize segment, forcing at least one rotation inside the
	// single batch below.
	crashDurEntriesPerBatch = 12
	defer func() { crashDurEntriesPerBatch = prev }()

	h.w.mustSave(t, 1)
	h.cfs.Crash()
}

// crashPhaseMidRotation cuts power while a full segment is being rolled over
// to a fresh one, after the old segment was synced and closed but before the
// new segment's directory entry is durable.
func crashPhaseMidRotation(t *testing.T, h *crashDurHarness) {
	t.Helper()
	h.w.mustSave(t, 3)

	target := h.shardDir(crashDurShardIDs[0])
	var once sync.Once
	fired := make(chan struct{})
	h.cfs.setHook(func(op crashOp, path string) {
		if op == crashOpCreate && filepath.Dir(path) == target {
			once.Do(func() {
				h.cfs.Crash()
				close(fired)
			})
		}
	})
	var err error
	for i := 0; i < 32 && err == nil; i++ {
		err = h.w.save()
	}
	h.cfs.setHook(nil)
	select {
	case <-fired:
	default:
		t.Fatal("no segment rotation occurred; the mid-rotation crash never fired")
	}
	if err == nil {
		t.Fatal("the batch interrupted by the rotation crash returned success")
	}
}

// crashPhaseAfterCompaction cuts power right after a compaction removed
// entries and garbage-collected the segments holding them. The compaction
// floor must survive with the surviving entries.
func crashPhaseAfterCompaction(t *testing.T, h *crashDurHarness) {
	t.Helper()
	h.w.mustSave(t, 8)

	sid := crashDurShardIDs[0]
	idxs := h.w.ackedIndexes(sid)
	floor := idxs[len(idxs)-3]
	if err := h.db.RemoveEntriesTo(sid, crashDurReplicaID, floor); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if err := h.db.Compact(sid, crashDurReplicaID); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	h.w.compactTo(sid, floor)
	h.cfs.Crash()
}

// TestCrashFS_ModelsPOSIXCrashSemantics proves the model itself: unsynced
// bytes vanish, synced bytes persist, a file whose parent directory was never
// fsynced disappears with everything under it, and stale handles fail closed.
func TestCrashFS_ModelsPOSIXCrashSemantics(t *testing.T) {
	const root = "/mnt/model"
	dir := filepath.Join(root, "d")

	cfs := newCrashFS(root)
	if err := cfs.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := syncDirFS(cfs, root); err != nil {
		t.Fatalf("syncDirFS(root): %v", err)
	}

	// durable.bin: written, synced, then written again without a sync.
	durable := filepath.Join(dir, "durable.bin")
	f, err := cfs.OpenFile(durable, openCreateFlags, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteAt([]byte("SYNCED"), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := f.WriteAt([]byte("LOST"), 6); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := syncDirFS(cfs, dir); err != nil {
		t.Fatalf("syncDirFS(dir): %v", err)
	}

	// orphan.bin is created and fully fsynced, but its directory entry is
	// never made durable by a parent fsync.
	orphan := filepath.Join(dir, "orphan.bin")
	g, err := cfs.OpenFile(orphan, openCreateFlags, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := g.WriteAt([]byte("DATA"), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := g.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// A whole directory whose parent was never fsynced takes its contents.
	lostDir := filepath.Join(root, "lostdir")
	if err := cfs.MkdirAll(lostDir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	child, err := cfs.OpenFile(filepath.Join(lostDir, "child.bin"), openCreateFlags, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := child.WriteAt([]byte("CHILD"), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := child.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := syncDirFS(cfs, lostDir); err != nil {
		t.Fatalf("syncDirFS(lostDir): %v", err)
	}

	cfs.Crash()

	// Stale handles must fail closed rather than mutate the post-crash image.
	if _, err := f.WriteAt([]byte("X"), 0); err == nil {
		t.Fatal("a handle opened before the crash accepted a write")
	}

	r, err := cfs.Open(durable)
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	info, err := r.Stat()
	if err != nil {
		t.Fatalf("Stat after crash: %v", err)
	}
	if info.Size() != 6 {
		t.Fatalf("durable.bin size %d after crash, want 6: unsynced bytes survived", info.Size())
	}
	buf := make([]byte, 6)
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after crash: %v", err)
	}
	if !bytes.Equal(buf, []byte("SYNCED")) {
		t.Fatalf("durable.bin content %q after crash, want %q", buf, "SYNCED")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := cfs.Stat(orphan); err == nil {
		t.Fatal("a file whose parent directory was never fsynced survived the crash")
	}
	if _, err := cfs.Stat(lostDir); err == nil {
		t.Fatal("a directory whose parent was never fsynced survived the crash")
	}
	if _, err := cfs.Stat(filepath.Join(lostDir, "child.bin")); err == nil {
		t.Fatal("a file under a lost directory survived the crash")
	}
}

// TestCrashFS_RemovalDurability verifies unlink semantics: a removal reaches
// the medium only once the parent directory is fsynced.
func TestCrashFS_RemovalDurability(t *testing.T) {
	const root = "/mnt/unlink"
	cfs := newCrashFS(root)

	create := func(name string) {
		t.Helper()
		f, err := cfs.OpenFile(filepath.Join(root, name), openCreateFlags, 0o600)
		if err != nil {
			t.Fatalf("OpenFile(%s): %v", name, err)
		}
		if _, err := f.WriteAt([]byte(name), 0); err != nil {
			t.Fatalf("WriteAt(%s): %v", name, err)
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("Sync(%s): %v", name, err)
		}
	}
	create("a.log")
	create("b.log")
	if err := syncDirFS(cfs, root); err != nil {
		t.Fatalf("syncDirFS: %v", err)
	}

	if err := cfs.Remove(filepath.Join(root, "a.log")); err != nil {
		t.Fatalf("Remove(a.log): %v", err)
	}
	if err := cfs.Remove(filepath.Join(root, "b.log")); err != nil {
		t.Fatalf("Remove(b.log): %v", err)
	}
	if err := syncDirFS(cfs, root); err != nil {
		t.Fatalf("syncDirFS after removals: %v", err)
	}
	create("c.log")
	if err := cfs.Remove(filepath.Join(root, "c.log")); err != nil {
		t.Fatalf("Remove(c.log): %v", err)
	}

	cfs.Crash()

	for _, name := range []string{"a.log", "b.log", "c.log"} {
		if _, err := cfs.Stat(filepath.Join(root, name)); err == nil {
			t.Fatalf("%s survived a removal that was made durable", name)
		}
	}

	// A removal that was never dir-fsynced may be undone by the crash.
	create("d.log")
	if err := syncDirFS(cfs, root); err != nil {
		t.Fatalf("syncDirFS: %v", err)
	}
	if err := cfs.Remove(filepath.Join(root, "d.log")); err != nil {
		t.Fatalf("Remove(d.log): %v", err)
	}
	cfs.Crash()
	if _, err := cfs.Stat(filepath.Join(root, "d.log")); err != nil {
		t.Fatalf("an unproven removal was treated as durable: %v", err)
	}
}

// TestWALCrashDurability_PartialWritebackLeavesDataAboveAHole is the crash the
// durability-sequence scheme (record.go) exists to classify, driven end to end
// through the real WAL rather than by editing bytes in a file.
//
// crashFS used to lose an unsynced file wholesale, which can only produce one
// shape of power loss: the log ends early at a clean record boundary. That
// shape needs no discriminator — there is no damage under acknowledged data to
// judge. The shape that DOES need one is the other one, and it is the common
// one: a batch spanning several blocks issues one WriteAt per block before a
// single fdatasync, so writeback is free to persist page N+1 and drop page N.
// What is left on the medium is a zero-filled hole with perfectly decodable
// records above it — byte-for-byte what interior bit rot under acknowledged
// data looks like, and the reason recovery used to refuse to open after an
// ordinary power loss.
//
// crashWriteback closes that modelling gap, and this test spends it. It drops
// exactly one page of one in-flight batch, asserts the post-crash image really
// has decodable data above a zero-filled hole (otherwise the case is vacuous),
// and then requires the WAL to open, repair itself, and keep every
// acknowledged entry. WHICH page is dropped selects between the two
// classifications the generation evidence can reach:
//
//   - Drop a page INSIDE the batch. The page below it holds the batch's head,
//     so the clean prefix already carries the in-flight generation and the
//     survivors carry the same one. Nothing above the damage has a completed
//     fdatasync behind it: an ordinary interrupted write, repaired silently.
//   - Drop the batch's FIRST page — the one straddling the boundary between
//     acknowledged bytes and the batch. The clean prefix then ends in the
//     acknowledged generation while the survivors carry the next one, so an
//     fdatasync completed somewhere inside the destroyed bytes. Recovery still
//     truncates (refusing here would refuse the commonest power loss there is),
//     but the node may now be shorter than the log it promised its peers, and
//     that has to be reported: see DB.RecoveryResiduals.
//
// The companion direction — the SAME hole shape, but with survivors whose
// generations prove a completed fdatasync ABOVE the damage, which must refuse
// to open — is TestSegment_ZeroHoleUnderAcknowledgedDataIsNotTruncatable.
func TestWALCrashDurability_PartialWritebackLeavesDataAboveAHole(t *testing.T) {
	cases := []struct {
		name string
		// pageOffset selects the dropped page relative to the first page the
		// in-flight batch wrote to.
		pageOffset   int
		wantResidual bool
	}{
		{name: "InteriorPageLost", pageOffset: 1, wantResidual: false},
		{name: "BatchHeadPageLost", pageOffset: 0, wantResidual: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			partialWritebackCase(t, tc.pageOffset, tc.wantResidual)
		})
	}
}

func partialWritebackCase(t *testing.T, pageOffset int, wantResidual bool) {
	const (
		root      = "/mnt/partial-writeback"
		blockSize = 1024
		// One segment for the whole test: a rotation would move the in-flight
		// batch to a fresh file and there would be no acknowledged prefix
		// beneath the hole.
		maxFile   = 1 << 20
		shardID   = 1
		replicaID = 7
	)
	dir := filepath.Join(root, "wal")
	cfs := newCrashFS(root)

	open := func() *DB {
		t.Helper()
		db, err := Open(dir,
			WithFS(cfs),
			WithBlockSize(blockSize),
			WithMaxFileSize(maxFile),
		)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}
	entry := func(index uint64) logdb.Entry {
		return crashDurEntry(shardID, index, 1)
	}
	save := func(db *DB, from, count uint64) error {
		entries := make([]logdb.Entry, 0, count)
		for i := uint64(0); i < count; i++ {
			entries = append(entries, entry(from+i))
		}
		return db.SaveState([]logdb.Update{{
			ShardID:   shardID,
			ReplicaID: replicaID,
			State:     logdb.State{Term: 1, Vote: 2, Commit: from + count - 1},
			Entries:   entries,
		}})
	}

	db := open()

	// Six acknowledged batches, each its own fdatasync, so the segment carries
	// several closed durability generations under the in-flight one.
	const ackedBatches, perBatch = 6, 2
	var acked uint64
	for b := uint64(0); b < ackedBatches; b++ {
		if err := save(db, acked+1, perBatch); err != nil {
			t.Fatalf("acked batch %d: %v", b, err)
		}
		acked += perBatch
	}

	segPath := onlySegmentPath(t, cfs, filepath.Join(dir, shardDirName(int(shardID%DefaultShardCount))))

	// The in-flight batch. Power is cut at its fdatasync, after every WriteAt
	// it issued has reached the page cache and none of it has reached the
	// medium — the exact window writeback is free to reorder inside.
	cfs.recordWrites(true)
	var once sync.Once
	var lostPage int
	cfs.setHook(func(op crashOp, path string) {
		if op != crashOpSync || path != segPath {
			return
		}
		once.Do(func() {
			lo, hi, ok := pageSpan(cfs.writesTo(segPath), blockSize)
			if !ok || hi-lo < 2 {
				t.Errorf("the in-flight batch touched pages [%d,%d]; it must span "+
					"at least three so a page can be dropped with data surviving "+
					"above it", lo, hi)
				return
			}
			lostPage = lo + pageOffset
			cfs.setWriteback(blockSize, func(p string, page int) bool {
				return p != segPath || page != lostPage
			})
			cfs.Crash()
		})
	})
	if err := save(db, acked+1, 8); err != nil {
		// A batch interrupted by power loss must not report success.
		t.Logf("in-flight batch failed as expected: %v", err)
	} else {
		t.Fatal("the batch whose fdatasync was interrupted returned success")
	}
	cfs.setHook(nil)
	cfs.recordWrites(false)
	if t.Failed() {
		return
	}

	// The model must actually have produced the failure mode, or everything
	// below is vacuous: a zero-filled hole with decodable data ABOVE it.
	image := readWholeFile(t, cfs, segPath)
	hiOff := (lostPage + 1) * blockSize
	if hiOff+blockSize > len(image) {
		t.Fatalf("post-crash image is %d bytes, too short to hold the dropped page "+
			"and a surviving page above it", len(image))
	}
	if run := zeroRunEndingAt(image, hiOff); run <= recordHeaderSize {
		t.Fatalf("the zero run ending at offset %d is %d bytes, which is legal "+
			"block padding rather than a hole: the writeback model did not lose "+
			"the page", hiOff, run)
	}
	if allZero(image[hiOff : hiOff+blockSize]) {
		t.Fatal("nothing survived ABOVE the dropped page, so this is an ordinary " +
			"truncated tail and not the partial-writeback case under test")
	}
	if allZero(image[:lostPage*blockSize]) {
		t.Fatal("nothing survived BELOW the dropped page")
	}

	// Recovery must read the hole as an interrupted write and repair it, in
	// both classifications: the acknowledged records all sit below the damage.
	reopened := open()
	defer reopened.Close()

	first, span, err := reopened.EntryRange(shardID, replicaID)
	if err != nil {
		t.Fatalf("EntryRange after partial-writeback crash: %v", err)
	}
	if first != 1 || first+span-1 < acked {
		t.Fatalf("recovered range [%d,%d] does not cover the acknowledged range "+
			"[1,%d]: the WAL lost entries it reported durable",
			first, first+span-1, acked)
	}
	got, err := reopened.GetEntries(shardID, replicaID, 1, acked+1, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if uint64(len(got)) != acked {
		t.Fatalf("GetEntries returned %d of %d acknowledged entries", len(got), acked)
	}
	for i, e := range got {
		if want := entry(uint64(i) + 1); !crashDurEntriesEqual(want, e) {
			t.Fatalf("acknowledged entry %d came back changed:\n want %s\n have %s",
				i+1, crashDurSummary(want), crashDurSummary(e))
		}
	}

	// The residual signal. A node that came up across a completed fdatasync may
	// be missing entries it already acknowledged to peers, and it cannot tell
	// at runtime: it serves reads and accepts writes exactly as a healthy node
	// does. A log line is not a signal — nothing polls it — so recovery has to
	// leave the fact where an operator (or the host's health reporting) can
	// find it hours later.
	residuals := reopened.RecoveryResiduals()
	if !wantResidual {
		if len(residuals) != 0 {
			t.Fatalf("recovery raised %d residual(s) for damage that lies wholly "+
				"inside ONE unsynced generation: %+v. No fdatasync completed above "+
				"or inside it, so nothing acknowledged was lost, and reporting one "+
				"here would make an ordinary power loss look like data loss on "+
				"every boot", len(residuals), residuals)
		}
		return
	}
	if len(residuals) == 0 {
		t.Fatal("recovery truncated ACROSS a completed fdatasync — the records " +
			"above the damage carry a generation the clean prefix never reached, " +
			"so acknowledged records were destroyed with it — and reported " +
			"nothing: RecoveryResiduals() is empty, so this node comes up " +
			"potentially shorter than the log it promised its peers with no " +
			"programmatic signal of any kind")
	}
	if len(residuals) != 1 {
		t.Fatalf("want exactly 1 residual, got %d: %+v", len(residuals), residuals)
	}
	r := residuals[0]
	if want := int(shardID % DefaultShardCount); r.StorageShardIndex != want {
		t.Fatalf("residual names WAL shard %d, want %d", r.StorageShardIndex, want)
	}
	if r.SurvivingGeneration <= r.PrefixGeneration {
		t.Fatalf("residual reports surviving generation %d not above prefix "+
			"generation %d; that gap IS the evidence and must be reported",
			r.SurvivingGeneration, r.PrefixGeneration)
	}
	if r.CleanEnd <= 0 || r.CleanEnd >= int64(len(image)) {
		t.Fatalf("residual clean end %d is outside the segment (%d bytes)",
			r.CleanEnd, len(image))
	}
	var found bool
	for _, n := range r.Nodes {
		if n.ShardID != shardID || n.ReplicaID != replicaID {
			continue
		}
		found = true
		if n.FirstIndex != first || n.LastIndex != first+span-1 {
			t.Fatalf("residual reports surviving range [%d,%d] for shard %d "+
				"replica %d, but the node actually came up with [%d,%d]: an "+
				"operator cannot tell which entries are at risk from a range "+
				"that is not the node's own", n.FirstIndex, n.LastIndex,
				shardID, replicaID, first, first+span-1)
		}
	}
	if !found {
		t.Fatalf("residual lists %d replica range(s), none of them the replica "+
			"whose log was damaged (shard %d replica %d): %+v",
			len(r.Nodes), shardID, replicaID, r.Nodes)
	}

	// The signal must survive whatever the caller does next. It reports what
	// the OPEN found, and an operator may only look hours later.
	if err := save(reopened, acked+1, 2); err != nil {
		t.Fatalf("SaveState after repair: %v", err)
	}
	if err := reopened.Compact(shardID, replicaID); err != nil {
		t.Fatalf("Compact after repair: %v", err)
	}
	if after := reopened.RecoveryResiduals(); len(after) != len(residuals) {
		t.Fatalf("the residual was cleared by subsequent successful operations "+
			"(%d -> %d): the condition it reports is not repaired by anything the "+
			"node can do to itself, so nothing may clear it",
			len(residuals), len(after))
	}
}

// zeroRunEndingAt returns the length of the run of zero bytes ending
// immediately before offset end.
func zeroRunEndingAt(b []byte, end int) int {
	n := 0
	for i := end - 1; i >= 0 && b[i] == 0; i-- {
		n++
	}
	return n
}

// onlySegmentPath returns the single segment file inside a WAL shard
// directory, failing the test when there is not exactly one.
func onlySegmentPath(t *testing.T, cfs *crashFS, shardDir string) string {
	t.Helper()
	entries, err := cfs.ReadDir(shardDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", shardDir, err)
	}
	var segs []string
	for _, de := range entries {
		if _, ok := parseSegmentFilename(de.Name()); ok {
			segs = append(segs, filepath.Join(shardDir, de.Name()))
		}
	}
	if len(segs) != 1 {
		t.Fatalf("shard dir %s holds %d segments, want exactly 1: %v",
			shardDir, len(segs), segs)
	}
	return segs[0]
}

// pageSpan reduces a batch's recorded WriteAt ranges to the first and last
// page it touched at the given page size.
func pageSpan(writes []crashWrite, pageSize int) (lo, hi int, ok bool) {
	for i, w := range writes {
		first := int(w.off) / pageSize
		last := int(w.off+int64(w.n)-1) / pageSize
		if i == 0 {
			lo, hi, ok = first, last, true
			continue
		}
		if first < lo {
			lo = first
		}
		if last > hi {
			hi = last
		}
	}
	return lo, hi, ok
}

// readWholeFile reads a modelled file in full.
func readWholeFile(t *testing.T, cfs *crashFS, path string) []byte {
	t.Helper()
	f, err := cfs.Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat(%s): %v", path, err)
	}
	buf := make([]byte, fi.Size())
	if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt(%s): %v", path, err)
	}
	return buf
}

// allZero reports whether every byte of b is zero.
func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// TestWALCrashDurability_RecoveryTruncationSurvivesTheNextCrash pins the fsync
// inside truncateCorruptSegment, which nothing else in this repository held:
// deleting it left the whole waldb suite, and the errfs fault-injection suite,
// green.
//
// Recovery repairs a torn segment by cutting it back to the clean prefix. That
// cut is a WRITE, and a write is not a repair until it is durable. Without the
// flush the next power loss brings the corrupt tail straight back — and it
// comes back UNDERNEATH the segments written since, which is what makes the
// consequence permanent rather than merely repeated work: a corruption in a
// NON-FINAL segment cannot be a torn tail (rotation fsyncs a segment in full
// before any higher one is created), so recovery correctly refuses to open the
// database at all. A node that repaired itself, acknowledged more entries and
// then lost power comes back dead.
//
// The whole sequence runs against crashFS, so "durable" means exactly "Sync
// returned" and nothing else.
func TestWALCrashDurability_RecoveryTruncationSurvivesTheNextCrash(t *testing.T) {
	const (
		root      = "/mnt/repair-durability"
		blockSize = 1024
		maxFile   = 1 << 20
		shardID   = 1
		replicaID = 7
	)
	dir := filepath.Join(root, "wal")
	cfs := newCrashFS(root)

	open := func() (*DB, error) {
		return Open(dir, WithFS(cfs), WithBlockSize(blockSize), WithMaxFileSize(maxFile))
	}
	mustOpen := func() *DB {
		t.Helper()
		db, err := open()
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}
	entry := func(index uint64) logdb.Entry { return crashDurEntry(shardID, index, 1) }
	save := func(db *DB, from, count uint64) error {
		entries := make([]logdb.Entry, 0, count)
		for i := uint64(0); i < count; i++ {
			entries = append(entries, entry(from+i))
		}
		return db.SaveState([]logdb.Update{{
			ShardID:   shardID,
			ReplicaID: replicaID,
			State:     logdb.State{Term: 1, Vote: 2, Commit: from + count - 1},
			Entries:   entries,
		}})
	}

	db := mustOpen()
	var acked uint64
	for b := 0; b < 6; b++ {
		if err := save(db, acked+1, 2); err != nil {
			t.Fatalf("acked batch %d: %v", b, err)
		}
		acked += 2
	}

	shardDir := filepath.Join(dir, shardDirName(int(shardID%DefaultShardCount)))
	segPath := onlySegmentPath(t, cfs, shardDir)

	// Damage the segment the way an interrupted multi-block write does: drop
	// the in-flight batch's first page, leaving a zero hole with the batch's
	// surviving pages decodable above it. Recovery must (and does) repair this
	// by truncating to the clean prefix.
	cfs.recordWrites(true)
	var once sync.Once
	cfs.setHook(func(op crashOp, path string) {
		if op != crashOpSync || path != segPath {
			return
		}
		once.Do(func() {
			lo, hi, ok := pageSpan(cfs.writesTo(segPath), blockSize)
			if !ok || hi-lo < 2 {
				t.Errorf("the in-flight batch touched pages [%d,%d]; it must span at least three", lo, hi)
				return
			}
			cfs.setWriteback(blockSize, func(p string, page int) bool {
				return p != segPath || page != lo
			})
			cfs.Crash()
		})
	})
	if err := save(db, acked+1, 8); err == nil {
		t.Fatal("the batch whose fdatasync was interrupted returned success")
	}
	cfs.setHook(nil)
	cfs.recordWrites(false)
	// From here on power loss is ordinary: unsynced bytes vanish, synced bytes
	// stay. The repair below has to survive on its own flush, not on a
	// writeback the model was told to keep.
	cfs.setWriteback(0, nil)
	if t.Failed() {
		return
	}

	// Recovery repairs the torn segment, and the repaired node then
	// acknowledges more entries. Those land in a FRESH segment: recover()
	// advances nextSegmentID past every observed segment before replay.
	repaired := mustOpen()
	if err := save(repaired, acked+1, 2); err != nil {
		t.Fatalf("SaveState after the repair: %v", err)
	}
	acked += 2

	entries, err := cfs.ReadDir(shardDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", shardDir, err)
	}
	var segs int
	for _, de := range entries {
		if _, ok := parseSegmentFilename(de.Name()); ok {
			segs++
		}
	}
	if segs < 2 {
		t.Fatalf("fixture: %d segment(s) after the repair, want at least 2 so the "+
			"repaired one is NON-final and a resurrected tail in it is fatal", segs)
	}

	// Ordinary power loss. Everything acknowledged is durable — including,
	// necessarily, the repair.
	cfs.Crash()

	final, err := open()
	if err != nil {
		t.Fatalf("the node did not come back up after a power loss that followed a "+
			"successful self-repair: %v\n\nThe truncation recovery performed was "+
			"never flushed, so the crash restored the corrupt tail — now sitting "+
			"UNDER the segment written after the repair. Corruption in a non-final "+
			"segment cannot be a torn tail, so recovery refuses to open, and it "+
			"will refuse on every subsequent boot.", err)
	}
	defer final.Close()

	first, span, err := final.EntryRange(shardID, replicaID)
	if err != nil {
		t.Fatalf("EntryRange after the second crash: %v", err)
	}
	if first != 1 || first+span-1 < acked {
		t.Fatalf("recovered range [%d,%d] does not cover the acknowledged range "+
			"[1,%d]", first, first+span-1, acked)
	}
	got, err := final.GetEntries(shardID, replicaID, 1, acked+1, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if uint64(len(got)) != acked {
		t.Fatalf("GetEntries returned %d of %d acknowledged entries", len(got), acked)
	}
	for i, e := range got {
		if want := entry(uint64(i) + 1); !crashDurEntriesEqual(want, e) {
			t.Fatalf("acknowledged entry %d came back changed after the second crash", i+1)
		}
	}
}

// TestWALCrashDurability_RolledBackBatchStaysRolledBack pins the fsync inside
// segment.DiscardPending, which no test held: deleting it left the whole
// waldb suite and the errfs suite green.
//
// When one shard of a SaveState batch fails, every shard that already wrote is
// rolled back by truncating its segment to the pre-batch checkpoint. The
// truncation alone is not enough. Nothing says the discarded bytes are still
// only in the page cache — writeback flushes dirty pages on its own schedule,
// with no fsync involved — so they may already be on the medium when the
// rollback happens. Truncating the live image does not unwrite them; only
// flushing the truncation does. Without that flush a crash restores the file
// as writeback left it, and recovery replays records from a batch the caller
// was explicitly told had FAILED, as committed entries.
//
// crashFS.Writeback supplies the missing event: the batch's bytes reach the
// medium BEFORE the failure, exactly as a real page-cache flush would.
func TestWALCrashDurability_RolledBackBatchStaysRolledBack(t *testing.T) {
	const (
		root      = "/mnt/rollback-durability"
		blockSize = 1024
		maxFile   = 64 * 1024
		replicaID = 7
		// Shard IDs 1 and 2 hash to different WAL shards, so one SaveState
		// spans two segment files and the second can fail after the first has
		// written.
		shardA, shardB = uint64(1), uint64(2)
	)
	dir := filepath.Join(root, "wal")
	cfs := newCrashFS(root)

	open := func() *DB {
		t.Helper()
		db, err := Open(dir, WithFS(cfs), WithBlockSize(blockSize), WithMaxFileSize(maxFile))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}
	entry := func(shardID, index uint64, size int) logdb.Entry {
		cmd := make([]byte, size)
		for i := range cmd {
			cmd[i] = byte(shardID*13 + index + uint64(i))
		}
		return logdb.Entry{Index: index, Term: 1, Cmd: cmd}
	}

	db := open()

	// One acknowledged batch, so both shards have a durable checkpoint.
	if err := db.SaveState([]logdb.Update{
		{ShardID: shardA, ReplicaID: replicaID, State: logdb.State{Term: 1, Vote: 1, Commit: 1},
			Entries: []logdb.Entry{entry(shardA, 1, 2000)}},
		{ShardID: shardB, ReplicaID: replicaID, State: logdb.State{Term: 1, Vote: 1, Commit: 1},
			Entries: []logdb.Entry{entry(shardB, 1, 2000)}},
	}); err != nil {
		t.Fatalf("acked batch: %v", err)
	}

	segA := onlySegmentPath(t, cfs, filepath.Join(dir, shardDirName(int(shardA%DefaultShardCount))))

	// Writeback flushes shard A's dirty pages the moment the batch below writes
	// them — before the batch fails, and with no fsync involved.
	// crashOpWriteAtDone, not crashOpWriteAt: the flush has to capture what the
	// write just put in the page cache, and every other hook fires BEFORE its
	// operation applies.
	cfs.setHook(func(op crashOp, path string) {
		if op == crashOpWriteAtDone && path == segA {
			cfs.Writeback(segA)
		}
	})

	// Shard A is listed FIRST and writes successfully; shard B then fails
	// permanently, because a record whose framed size exceeds the whole segment
	// can never fit. The batch is rolled back, shard A included.
	//
	// Shard A gets TWO updates, and that is load-bearing. A batch never flushes
	// a partial block, so the LAST record it writes always keeps its final
	// chunk in the block buffer and never reaches the medium; recovery discards
	// an unassembled First/Middle run without a word, so a single-record batch
	// looks correctly rolled back whether or not the rollback was ever made
	// durable. The second update's writes seal the blocks holding the first
	// update's record, which is what puts a COMPLETE, replayable record from a
	// failed batch on the disk — the only thing the rollback's flush can be
	// tested against.
	err := db.SaveState([]logdb.Update{
		{ShardID: shardA, ReplicaID: replicaID,
			Entries: []logdb.Entry{entry(shardA, 2, 2000)}},
		{ShardID: shardA, ReplicaID: replicaID, State: logdb.State{Term: 2, Vote: 2, Commit: 3},
			Entries: []logdb.Entry{entry(shardA, 3, 2000)}},
		{ShardID: shardB, ReplicaID: replicaID,
			Entries: []logdb.Entry{entry(shardB, 2, 2*maxFile)}},
	})
	cfs.setHook(nil)
	if err == nil {
		t.Fatal("the batch carrying an unfittable record returned success")
	}
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("SaveState = %v, want ErrRecordTooLarge", err)
	}

	// Power loss right after the failed batch.
	cfs.Crash()

	reopened := open()
	defer reopened.Close()

	first, span, rerr := reopened.EntryRange(shardA, replicaID)
	if rerr != nil {
		t.Fatalf("EntryRange(shardA): %v", rerr)
	}
	if first != 1 || span != 1 {
		t.Fatalf("shard A came back holding entries [%d,%d] after a crash that "+
			"followed a FAILED batch, want only the acknowledged entry 1.\n\n"+
			"The rolled-back record was written, reached the medium through "+
			"ordinary writeback, and the rollback's truncation was never flushed — "+
			"so the crash restored it and recovery replayed, as a committed entry, "+
			"a write the caller was told had failed.", first, first+span-1)
	}

	// The acknowledged entry is intact, and the hard state did not advance to
	// the failed batch's term.
	got, gerr := reopened.GetEntries(shardA, replicaID, 1, 2, 0)
	if gerr != nil || len(got) != 1 {
		t.Fatalf("GetEntries(shardA): %d entries, err %v", len(got), gerr)
	}
	if want := entry(shardA, 1, 2000); !crashDurEntriesEqual(want, got[0]) {
		t.Fatal("the acknowledged entry did not survive the rollback and crash")
	}
	st, _, serr := reopened.ReadState(shardA, replicaID)
	if serr != nil {
		t.Fatalf("ReadState(shardA): %v", serr)
	}
	if st.Term != 1 || st.Commit != 1 {
		t.Fatalf("shard A hard state = %+v after a failed batch and a crash, want "+
			"term 1 commit 1: the failed batch's state was resurrected", st)
	}
}
