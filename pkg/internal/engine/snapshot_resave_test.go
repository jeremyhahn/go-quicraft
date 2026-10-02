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

package engine

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// resaveFixture is a saveSnapshot harness: one node with an in-memory
// snapshotter, an in-memory LogDB, and a single-worker pool.
type resaveFixture struct {
	node    *Node
	sm      *snapshotTestSnapshotter
	pool    *snapshotPool
	ldb     *snapshotTestLogDB
	snapDir string
	req     snapshotRequest
}

func newResaveFixture(t *testing.T, lastApplied uint64) *resaveFixture {
	t.Helper()
	return newResaveFixtureDB(t, lastApplied, nil)
}

// newResaveFixtureDB is newResaveFixture over a WRAPPED LogDB: wrap receives
// the in-memory one the fixture keeps in ldb and returns the implementation the
// pool talks to, which is how the record-read failure is injected. A nil wrap
// uses the in-memory LogDB directly.
func newResaveFixtureDB(t *testing.T, lastApplied uint64,
	wrap func(*snapshotTestLogDB) logdb.LogDB) *resaveFixture {
	t.Helper()
	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 7, ReplicaID: 3, CompactionOverhead: 10}
	cfg.SetDefaults()

	sm := newSnapshotTestSnapshotter(lastApplied)
	node := NewNode(nil, nil, nil, cfg, sm, snapDir, nil)
	ldb := newSnapshotTestLogDB()
	var poolDB logdb.LogDB = ldb
	if wrap != nil {
		poolDB = wrap(ldb)
	}
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })

	return &resaveFixture{
		node:    node,
		sm:      sm,
		pool:    newSnapshotPool(1, poolDB, nil, nil, nil, stopC, nil),
		ldb:     ldb,
		snapDir: snapDir,
		req: snapshotRequest{
			shardID:   cfg.ShardID,
			replicaID: cfg.ReplicaID,
			node:      node,
			save:      true,
		},
	}
}

// saveDirFor returns the directory saveSnapshot resolves for the given index.
func (f *resaveFixture) saveDirFor(index uint64) string {
	return snapshotSaveDir(f.snapDir, f.req.shardID, f.req.replicaID, index)
}

// TestSaveSnapshot_NonAdvancingResaveIsNoOp is the regression test for the
// data-loss defect where a second snapshot at an index LogDB already records
// resolved to the SAME directory and opened its snapshot.dat with O_TRUNC,
// destroying the only artifact covering the compacted log.
func TestSaveSnapshot_NonAdvancingResaveIsNoOp(t *testing.T) {
	f := newResaveFixture(t, 200)

	index, err := f.pool.saveSnapshot(f.req)
	if err != nil {
		t.Fatalf("first saveSnapshot: %v", err)
	}
	if index != 200 {
		t.Fatalf("first saveSnapshot index = %d, want 200", index)
	}

	saveDir := f.saveDirFor(200)
	dataPath := filepath.Join(saveDir, snapshotDataFile)
	before, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("read snapshot data: %v", err)
	}
	if len(before) == 0 {
		t.Fatal("first save wrote an empty snapshot data file")
	}

	// Make a re-serialization DETECTABLE. The snapshotter writes a fixed-width
	// record, so a truncate-and-rewrite produces a file of identical LENGTH:
	// only the CONTENT distinguishes the artifact that survived from one that
	// was destroyed and written again.
	f.sm.state.Store(0xDEADBEEF)

	// Nothing has been applied since, so the second request resolves to the
	// SAME index and directory.
	index, err = f.pool.saveSnapshot(f.req)
	if err != nil {
		t.Fatalf("non-advancing saveSnapshot must be a silent no-op, got error: %v", err)
	}
	if index != 0 {
		t.Fatalf("non-advancing saveSnapshot index = %d, want 0 (no snapshot taken)", index)
	}

	after, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("snapshot data file is gone after a non-advancing re-save: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("DATA LOSS: the recorded snapshot.dat was rewritten by a non-advancing "+
			"re-save (%x, was %x)", after, before)
	}
	if _, err := os.Stat(filepath.Join(saveDir, snapshotMetadataFile)); err != nil {
		t.Fatalf("snapshot.meta is gone after a non-advancing re-save: %v", err)
	}

	rec, err := f.ldb.GetSnapshot(f.req.shardID, f.req.replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if rec.Index != 200 || rec.Filepath != saveDir {
		t.Fatalf("LogDB record changed: index=%d filepath=%q, want 200 / %q",
			rec.Index, rec.Filepath, saveDir)
	}
}

// TestSaveSnapshot_NonAdvancingExportFails asserts that an EXPORT at a
// non-advancing index fails loudly instead of reporting a success that
// produced no export directory -- and that it does so without touching the
// recorded artifact.
func TestSaveSnapshot_NonAdvancingExportFails(t *testing.T) {
	f := newResaveFixture(t, 120)

	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("first saveSnapshot: %v", err)
	}
	dataPath := filepath.Join(f.saveDirFor(120), snapshotDataFile)
	before, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("read snapshot data: %v", err)
	}

	// See TestSaveSnapshot_NonAdvancingResaveIsNoOp: a rewrite is only visible
	// in the CONTENT, never in the length.
	f.sm.state.Store(0xFEEDFACE)

	exportDir := filepath.Join(t.TempDir(), "exported")
	exportReq := f.req
	exportReq.exportPath = exportDir
	exportReq.exported = true

	if _, err := f.pool.saveSnapshot(exportReq); !errors.Is(err, ErrSnapshotIndexNotAdvanced) {
		t.Fatalf("export at a non-advancing index: err = %v, want ErrSnapshotIndexNotAdvanced", err)
	}
	if _, err := os.Stat(exportDir); !os.IsNotExist(err) {
		t.Fatalf("failed export must not leave an export directory behind: %v", err)
	}
	after, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("snapshot data file is gone after a refused export: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("DATA LOSS: a refused export rewrote the recorded snapshot.dat "+
			"(%x, was %x)", after, before)
	}
}

// errRecordRead is the injected LogDB snapshot-record read failure.
var errRecordRead = errors.New("resave test: snapshot record read failed")

// recordReadFailLogDB fails GetSnapshot once armed and otherwise behaves like
// the in-memory fixture. Every read the save path makes — the non-advancing
// guard AND the publishStagedSave backstop — goes through this one method, which
// is why a fail-OPEN guard defeats both layers at once.
type recordReadFailLogDB struct {
	*snapshotTestLogDB
	failing bool
}

func (db *recordReadFailLogDB) GetSnapshot(shardID, replicaID uint64) (logdb.Snapshot, error) {
	if db.failing {
		return logdb.Snapshot{}, errRecordRead
	}
	return db.snapshotTestLogDB.GetSnapshot(shardID, replicaID)
}

// TestSaveSnapshot_RecordReadErrorFailsClosed asserts that a save whose LogDB
// record cannot be READ refuses to run.
//
// Falling through on the read error would run a save whose publish must then
// decide whether the canonical directory is the recorded one — and
// publishStagedSave, the second line of defence, consults the record through
// the SAME call, so one persistent read error defeats both layers.
func TestSaveSnapshot_RecordReadErrorFailsClosed(t *testing.T) {
	var wrapper *recordReadFailLogDB
	f := newResaveFixtureDB(t, 400, func(base *snapshotTestLogDB) logdb.LogDB {
		wrapper = &recordReadFailLogDB{snapshotTestLogDB: base}
		return wrapper
	})

	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("first saveSnapshot: %v", err)
	}
	saveDir := f.saveDirFor(400)
	dataPath := filepath.Join(saveDir, snapshotDataFile)
	before, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("read snapshot data: %v", err)
	}

	f.sm.state.Store(0x0BADC0DE)
	wrapper.failing = true

	if _, err := f.pool.saveSnapshot(f.req); !errors.Is(err, errRecordRead) {
		t.Fatalf("saveSnapshot error = %v, want the injected record-read failure", err)
	}

	after, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("DATA LOSS: the recorded snapshot.dat is gone after a record-read "+
			"failure: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("DATA LOSS: the recorded snapshot.dat was rewritten while the record "+
			"state was unknown (%x, was %x)", after, before)
	}
	if _, err := os.Stat(filepath.Join(saveDir, snapshotMetadataFile)); err != nil {
		t.Fatalf("snapshot.meta is gone after a record-read failure: %v", err)
	}
}

// TestSaveSnapshot_MissingRecordedArtifactIsRebuilt asserts that the
// non-advancing skip is gated on the ARTIFACT, not on the record alone.
//
// A record whose directory was lost (operator cleanup, a partial restore) would
// otherwise never be rebuilt: the skip is permanent for a shard taking no
// further writes, leaving a single-node deployment repairable only by streaming
// from a peer it does not have.
func TestSaveSnapshot_MissingRecordedArtifactIsRebuilt(t *testing.T) {
	f := newResaveFixture(t, 250)

	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("first saveSnapshot: %v", err)
	}
	saveDir := f.saveDirFor(250)
	if err := os.RemoveAll(saveDir); err != nil {
		t.Fatalf("remove the recorded artifact: %v", err)
	}

	index, err := f.pool.saveSnapshot(f.req)
	if err != nil {
		t.Fatalf("rebuild saveSnapshot: %v", err)
	}
	if index != 250 {
		t.Fatalf("rebuild saveSnapshot index = %d, want 250 (the record's index)", index)
	}
	if _, err := os.Stat(filepath.Join(saveDir, snapshotDataFile)); err != nil {
		t.Fatalf("the missing artifact was not rebuilt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(saveDir, snapshotMetadataFile)); err != nil {
		t.Fatalf("the rebuilt artifact is not self-describing: %v", err)
	}

	// The record still names the same directory at the same index: LogDB
	// rejects a non-advancing write, and the rebuild does not need one.
	rec, err := f.ldb.GetSnapshot(f.req.shardID, f.req.replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if rec.Index != 250 || rec.Filepath != saveDir {
		t.Fatalf("LogDB record = index %d path %q, want 250 / %q",
			rec.Index, rec.Filepath, saveDir)
	}

	// And the rebuilt artifact is once again the thing that suppresses a
	// second re-save.
	if index, err := f.pool.saveSnapshot(f.req); err != nil || index != 0 {
		t.Fatalf("save after the rebuild: index=%d err=%v, want 0 / nil", index, err)
	}
}

// TestSaveSnapshot_RebuildRefusesAFormatTheRecordDoesNotDescribe: a rebuild
// keeps the existing LogDB record — LogDB rejects a same-index update — so
// recovery reads the rebuilt bytes through the OLD record's encryption and
// compression. Rebuilding in a format the record does not describe (here: the
// shard switched to Snappy since the recorded snapshot was taken) produced an
// artifact that reported success and could never be recovered. It is refused.
func TestSaveSnapshot_RebuildRefusesAFormatTheRecordDoesNotDescribe(t *testing.T) {
	f := newResaveFixture(t, 250)
	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("first saveSnapshot: %v", err)
	}
	saveDir := f.saveDirFor(250)
	if err := os.RemoveAll(saveDir); err != nil {
		t.Fatalf("remove the recorded artifact: %v", err)
	}
	f.node.cfg.SnapshotCompressionType = config.Snappy

	index, err := f.pool.saveSnapshot(f.req)
	if err != nil || index != 0 {
		t.Fatalf("rebuild in a mismatched format: index=%d err=%v, want the no-op 0 / nil", index, err)
	}
	if _, statErr := os.Stat(saveDir); !os.IsNotExist(statErr) {
		t.Fatalf("an artifact the record cannot describe was published at %s (stat err %v)", saveDir, statErr)
	}
	rec, recErr := f.ldb.GetSnapshot(f.req.shardID, f.req.replicaID)
	if recErr != nil || rec.Compression != config.NoCompression {
		t.Fatalf("the record changed: %+v (err %v)", rec, recErr)
	}
}

// TestSaveSnapshot_RebuildRefusesWhatTheRecordCannotDescribe: a rebuild keeps
// the surviving record, and recovery reads the rebuilt artifact through it —
// its path and the FileSize it measures the file against. A rebuild whose
// output size differs (the same state serialized differently), or whose
// record names a directory the rebuild does not write, reported success and
// left an artifact recovery could never read. Both are refused.
func TestSaveSnapshot_RebuildRefusesWhatTheRecordCannotDescribe(t *testing.T) {
	for _, tt := range []struct {
		name   string
		tamper func(*logdb.Snapshot, string)
	}{
		{"recorded size differs", func(rec *logdb.Snapshot, _ string) { rec.FileSize++ }},
		{"recorded path is not the rebuild's", func(rec *logdb.Snapshot, saveDir string) {
			rec.Filepath = filepath.Join(filepath.Dir(saveDir), "legacy-snapshot-250")
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newResaveFixture(t, 250)
			if _, err := f.pool.saveSnapshot(f.req); err != nil {
				t.Fatalf("first saveSnapshot: %v", err)
			}
			saveDir := f.saveDirFor(250)
			if err := os.RemoveAll(saveDir); err != nil {
				t.Fatalf("remove the recorded artifact: %v", err)
			}
			f.ldb.mu.Lock()
			rec := f.ldb.snapshots[f.req.shardID]
			tt.tamper(&rec, saveDir)
			f.ldb.snapshots[f.req.shardID] = rec
			f.ldb.mu.Unlock()

			index, err := f.pool.saveSnapshot(f.req)
			if err != nil || index != 0 {
				t.Fatalf("rebuild the record cannot describe: index=%d err=%v, want the no-op 0 / nil", index, err)
			}
			if _, statErr := os.Stat(saveDir); !os.IsNotExist(statErr) {
				t.Fatalf("an artifact the record cannot describe was published at %s (stat err %v)", saveDir, statErr)
			}
			if stages, _ := filepath.Glob(filepath.Join(filepath.Dir(saveDir), SaveStageDirPrefix+"*")); len(stages) != 0 {
				t.Fatalf("the refused rebuild left its stage behind: %v", stages)
			}
		})
	}
}

// TestSaveSnapshot_RebuildAcceptsALegacyUnknownSize: a record with FileSize 0
// predates size recording — recovery does not validate it and restart
// re-measures the file — so it must not refuse the rebuild that repairs a
// shard upgraded with such a record and then missing its artifact.
func TestSaveSnapshot_RebuildAcceptsALegacyUnknownSize(t *testing.T) {
	f := newResaveFixture(t, 250)
	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("first saveSnapshot: %v", err)
	}
	saveDir := f.saveDirFor(250)
	if err := os.RemoveAll(saveDir); err != nil {
		t.Fatalf("remove the recorded artifact: %v", err)
	}
	f.ldb.mu.Lock()
	rec := f.ldb.snapshots[f.req.shardID]
	rec.FileSize = 0
	f.ldb.snapshots[f.req.shardID] = rec
	f.ldb.mu.Unlock()

	index, err := f.pool.saveSnapshot(f.req)
	if err != nil || index != 250 {
		t.Fatalf("rebuild under a legacy size-unknown record: index=%d err=%v, want 250 / nil", index, err)
	}
	if _, statErr := os.Stat(filepath.Join(saveDir, snapshotDataFile)); statErr != nil {
		t.Fatalf("the missing artifact was not rebuilt: %v", statErr)
	}
}

// TestSaveSnapshot_RebuildRefreshesTheLogReaderSize: a shard restarted with
// its artifact missing loads its log reader with the recorded path and size 0,
// and a rebuild cannot re-create the reader's snapshot at the same index —
// so the reader kept declaring 0 and every send of the rebuilt artifact failed
// its declared-size check until the next restart. The rebuild refreshes the
// declared size.
func TestSaveSnapshot_RebuildRefreshesTheLogReaderSize(t *testing.T) {
	f := newResaveFixture(t, 250)
	lr := logdb.NewLogReader(f.req.shardID, f.req.replicaID, f.ldb)
	f.node.logReader = lr
	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("first saveSnapshot: %v", err)
	}
	saveDir := f.saveDirFor(250)
	fi, err := os.Stat(filepath.Join(saveDir, snapshotDataFile))
	if err != nil {
		t.Fatalf("stat the artifact: %v", err)
	}
	if err := os.RemoveAll(saveDir); err != nil {
		t.Fatalf("remove the recorded artifact: %v", err)
	}
	// What a restart with the artifact missing leaves: the path, size 0.
	if err := lr.RefreshSnapshotFileSize(250, 0); err != nil {
		t.Fatalf("simulate the restart's size-0 load: %v", err)
	}

	if index, err := f.pool.saveSnapshot(f.req); err != nil || index != 250 {
		t.Fatalf("rebuild: index=%d err=%v, want 250 / nil", index, err)
	}
	if got := lr.Snapshot().FileSize; got != uint64(fi.Size()) {
		t.Fatalf("the log reader declares %d bytes for the rebuilt artifact of %d: every send of it would fail",
			got, fi.Size())
	}
}

// TestSaveSnapshot_FirstSaveProceedsWithNoRecord pins the branch a fixture
// returning a zero record would hide: every real LogDB reports "no snapshot yet" as ErrNoSnapshot,
// not as a zero record with a nil error, and the guard must read that as "this
// is the first save" rather than as a record-read failure.
func TestSaveSnapshot_FirstSaveProceedsWithNoRecord(t *testing.T) {
	f := newResaveFixture(t, 60)

	if _, err := f.ldb.GetSnapshot(f.req.shardID, f.req.replicaID); !errors.Is(err, logdb.ErrNoSnapshot) {
		t.Fatalf("fixture GetSnapshot = %v, want ErrNoSnapshot (memdb/waldb behaviour)", err)
	}

	index, err := f.pool.saveSnapshot(f.req)
	if err != nil {
		t.Fatalf("first saveSnapshot: %v", err)
	}
	if index != 60 {
		t.Fatalf("first saveSnapshot index = %d, want 60", index)
	}
}

// TestPublishStagedSave_NeverReplacesTheRecordedDirectory asserts the backstop:
// publishing a staged save never replaces the recorded directory (except to
// rebuild it) or one a receive has claimed, and does replace an orphan.
func TestPublishStagedSave_NeverReplacesTheRecordedDirectory(t *testing.T) {
	f := newResaveFixture(t, 80)
	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("saveSnapshot: %v", err)
	}
	recorded := f.saveDirFor(80)
	stageWith := func(t *testing.T, content string) string {
		t.Helper()
		stage, err := MkdirStageDir(filepath.Dir(recorded), SaveStageDirPattern)
		if err != nil {
			t.Fatalf("MkdirStageDir: %v", err)
		}
		if wErr := os.WriteFile(filepath.Join(stage, snapshotDataFile), []byte(content), 0o600); wErr != nil {
			t.Fatalf("write stage: %v", wErr)
		}
		return stage
	}
	occupy := func(t *testing.T, dir, content string) {
		t.Helper()
		if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
			t.Fatalf("mkdir: %v", mkErr)
		}
		if wErr := os.WriteFile(filepath.Join(dir, snapshotDataFile), []byte(content), 0o600); wErr != nil {
			t.Fatalf("write: %v", wErr)
		}
	}
	read := func(dir string) string {
		got, _ := os.ReadFile(filepath.Join(dir, snapshotDataFile))
		return string(got)
	}
	f.node.LockSnapshotDir()
	defer f.node.UnlockSnapshotDir()

	// The RECORDED directory is refused.
	if err := f.pool.publishStagedSave(stageWith(t, "save"), recorded, 80, f.req, false); !errors.Is(err, ErrSnapshotIndexNotAdvanced) {
		t.Fatalf("publishStagedSave onto the recorded directory = %v, want ErrSnapshotIndexNotAdvanced", err)
	}
	if _, statErr := os.Stat(filepath.Join(recorded, snapshotDataFile)); statErr != nil {
		t.Fatalf("DATA LOSS: publishing a staged save replaced the snapshot LogDB records: %v", statErr)
	}

	// A CLAIMED occupant — a receive's published artifact awaiting raft's
	// verdict, unrecorded now and recorded a moment from now — is refused.
	claimed := f.saveDirFor(81)
	occupy(t, claimed, "received")
	f.node.ClaimSnapshotDir(81)
	if err := f.pool.publishStagedSave(stageWith(t, "save"), claimed, 81, f.req, false); !errors.Is(err, ErrSnapshotIndexNotAdvanced) {
		t.Fatalf("publishStagedSave over a claimed occupant = %v, want ErrSnapshotIndexNotAdvanced", err)
	}
	if got := read(claimed); got != "received" {
		t.Fatalf("the claimed occupant was replaced (holds %q)", got)
	}

	// An unclaimed, unrecorded occupant is an ORPHAN and is replaced: a
	// quiescent shard keeps re-saving at that index, and GC never reclaims
	// the newest directory.
	f.node.ReleaseSnapshotDir(81)
	if err := f.pool.publishStagedSave(stageWith(t, "save"), claimed, 81, f.req, false); err != nil {
		t.Fatalf("publishStagedSave over an orphan: %v", err)
	}
	if got := read(claimed); got != "save" {
		t.Fatalf("the orphan was not replaced (holds %q)", got)
	}

	// And the rebuild of the RECORDED directory is allowed.
	if err := f.pool.publishStagedSave(stageWith(t, "rebuilt"), recorded, 80, f.req, true); err != nil {
		t.Fatalf("rebuild publish: %v", err)
	}
	if got := read(recorded); got != "rebuilt" {
		t.Fatalf("the rebuild was not published (holds %q)", got)
	}
}

// TestPublishStagedSave_RefusesADirectoryARecoveryIsReading: a recovery whose
// record names no path falls back to the newest directory on disk, which can
// be unrecorded and unclaimed — exactly what the save replaces as an orphan.
// A pinned directory is refused instead.
func TestPublishStagedSave_RefusesADirectoryARecoveryIsReading(t *testing.T) {
	f := newResaveFixture(t, 80)
	dir := f.saveDirFor(95)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotDataFile), []byte("being recovered"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	stage, err := MkdirStageDir(filepath.Dir(dir), SaveStageDirPattern)
	if err != nil {
		t.Fatalf("MkdirStageDir: %v", err)
	}

	f.node.LockSnapshotDir()
	defer f.node.UnlockSnapshotDir()
	f.node.PinSnapshotDir(dir)
	if pErr := f.pool.publishStagedSave(stage, dir, 95, f.req, false); !errors.Is(pErr, ErrSnapshotIndexNotAdvanced) {
		t.Fatalf("publishStagedSave over a pinned directory = %v, want ErrSnapshotIndexNotAdvanced", pErr)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, snapshotDataFile)); string(got) != "being recovered" {
		t.Fatalf("the directory a recovery is reading was replaced (holds %q)", got)
	}
}

// TestSaveSnapshot_SMErrorLeavesNoPlaintextDirectory is the regression test
// for the leaked save directories: a failure before the metadata write left
// snapshot-<N>/snapshot.dat holding partial PLAINTEXT state-machine bytes with
// no snapshot.meta beside it, which readSnapshotEpoch classifies UNEVALUABLE
// and which therefore suppresses DEK purging host-wide for every shard.
func TestSaveSnapshot_SMErrorLeavesNoPlaintextDirectory(t *testing.T) {
	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 4, ReplicaID: 1, CompactionOverhead: 10}
	cfg.SetDefaults()

	errSave := &ErrSnapshotSaveFailed{Msg: "simulated state machine failure"}
	node := NewNode(nil, nil, nil, cfg, newFailingSnapshotter(300, errSave), snapDir, nil)
	stopC := make(chan struct{})
	defer close(stopC)
	pool := newSnapshotPool(1, newSnapshotTestLogDB(), nil, nil, nil, stopC, nil)

	req := snapshotRequest{shardID: 4, replicaID: 1, node: node, save: true}
	_, err := pool.saveSnapshot(req)
	var cause *ErrSnapshotSaveFailed
	if !errors.As(err, &cause) {
		t.Fatalf("saveSnapshot err = %v (%T), want *ErrSnapshotSaveFailed", err, err)
	}

	saveDir := snapshotSaveDir(snapDir, 4, 1, 300)
	if _, err := os.Stat(saveDir); !os.IsNotExist(err) {
		t.Fatalf("a failed save left its directory behind (%s): %v", saveDir, err)
	}
}

// TestSaveSnapshot_MetadataErrorLeavesNoDirectory covers the last pre-record
// failure point: the artifact is written but not yet self-describing, so the
// directory must not survive. snapshot.meta is pre-created as a DIRECTORY to
// make writeSnapshotMetadata fail.
func TestSaveSnapshot_MetadataErrorLeavesNoDirectory(t *testing.T) {
	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 5, ReplicaID: 2, CompactionOverhead: 10}
	cfg.SetDefaults()

	saveDir := snapshotSaveDir(snapDir, 5, 2, 150)
	stagePattern := filepath.Join(filepath.Dir(saveDir), SaveStageDirPrefix+"*")
	// Mid-save, occupy snapshot.meta inside the save's stage with a directory,
	// so the metadata write fails after the data file is complete.
	ss := &canarySnapshotter{
		lastApplied: 150,
		canary:      []byte("state"),
		midSave: func() {
			stages, err := filepath.Glob(stagePattern)
			if err != nil || len(stages) != 1 {
				t.Errorf("found %d save stages (glob err %v), want 1", len(stages), err)
				return
			}
			if err := os.MkdirAll(filepath.Join(stages[0], snapshotMetadataFile), 0o750); err != nil {
				t.Errorf("pre-create meta as directory: %v", err)
			}
		},
	}
	node := NewNode(nil, nil, nil, cfg, ss, snapDir, nil)
	stopC := make(chan struct{})
	defer close(stopC)
	pool := newSnapshotPool(1, newSnapshotTestLogDB(), nil, nil, nil, stopC, nil)

	req := snapshotRequest{shardID: 5, replicaID: 2, node: node, save: true}
	if _, err := pool.saveSnapshot(req); err == nil {
		t.Fatal("saveSnapshot must fail when the metadata write fails")
	}
	if _, err := os.Stat(saveDir); !os.IsNotExist(err) {
		t.Fatalf("a metadata failure published its directory (%s): %v", saveDir, err)
	}
	if stages, _ := filepath.Glob(stagePattern); len(stages) != 0 {
		t.Fatalf("a metadata failure left its staging directory behind: %v", stages)
	}
}

// slotProbeCallback records, from inside OnSnapshotCompleted, whether the
// pool's per-shard in-flight slot and the node's snapshotting flag are still
// claimed.
type slotProbeCallback struct {
	testCallback
	pool         *snapshotPool
	node         *Node
	shardID      uint64
	inFlightHeld bool
	nodeHeld     bool
}

func (c *slotProbeCallback) OnSnapshotCompleted(shardID uint64, index uint64, err error) {
	_, c.inFlightHeld = c.pool.inFlight.Load(c.shardID)
	c.nodeHeld = c.node.snapshotting.Load()
	c.testCallback.OnSnapshotCompleted(shardID, index, err)
}

// TestHandleRequest_CompletionFiresBeforeSlotsAreReleased pins the ordering
// that closes the misattribution window: the completion callback resolves
// pending requests for a shard WITHOUT matching them to a specific request, so
// releasing the in-flight/snapshotting slots first would let a request
// registered microseconds later be completed with this snapshot's result.
func TestHandleRequest_CompletionFiresBeforeSlotsAreReleased(t *testing.T) {
	f := newResaveFixture(t, 90)
	probe := &slotProbeCallback{pool: f.pool, node: f.node, shardID: f.req.shardID}
	f.pool.setCallback(probe)

	f.pool.inFlight.Store(f.req.shardID, struct{}{})
	f.node.snapshotting.Store(true)

	f.pool.handleRequest(f.req)

	if probe.snapshotCallCount.Load() != 1 {
		t.Fatalf("OnSnapshotCompleted called %d times, want 1", probe.snapshotCallCount.Load())
	}
	if !probe.inFlightHeld {
		t.Error("the pool's in-flight slot was released BEFORE OnSnapshotCompleted; " +
			"a request registered in that window is completed with this snapshot's result")
	}
	if !probe.nodeHeld {
		t.Error("node.snapshotting was cleared BEFORE OnSnapshotCompleted; " +
			"a request registered in that window is completed with this snapshot's result")
	}
	if _, held := f.pool.inFlight.Load(f.req.shardID); held {
		t.Error("the in-flight slot was never released")
	}
	if f.node.snapshotting.Load() {
		t.Error("node.snapshotting was never cleared")
	}
}
