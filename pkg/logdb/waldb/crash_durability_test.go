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
