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

package quicraft

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// An import must never damage a snapshot it did not create.

// liveImportFixture is a stopped shard on a host, wired to a real LogDB, ready to
// be imported into.
type liveImportFixture struct {
	host      *Host
	db        *memdb.MemDB
	shardID   uint64
	replicaID uint64
	base      string
}

// newLiveImportFixture builds a memdb host with a written bootstrap record.
func newLiveImportFixture(t *testing.T, withBarrier bool) *liveImportFixture {
	t.Helper()
	const shardID, replicaID uint64 = 7, 3

	cfg := testHostConfig(t)
	db := memdb.New()
	opts := []HostOption{WithLogDB(db), WithoutTransport()}
	if withBarrier {
		opts = append(opts, WithBarrier(createTestBarrier(t)))
	} else {
		opts = append(opts, WithoutEncryption())
	}
	h, err := NewHost(cfg, opts...)
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	if bsErr := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
		Addresses: map[uint64]string{replicaID: "127.0.0.1:5000"},
	}); bsErr != nil {
		t.Fatalf("SaveBootstrap failed: %v", bsErr)
	}

	return &liveImportFixture{
		host:      h,
		db:        db,
		shardID:   shardID,
		replicaID: replicaID,
		base:      shardSnapshotBaseDir(cfg.NodeHostDir, "", shardID),
	}
}

// importArtifact imports an artifact at the given index carrying payload.
func (f *liveImportFixture) importArtifact(t *testing.T, index uint64, payload []byte) error {
	t.Helper()
	artifact := buildSnapshotArtifact(t, index, 5, 0, proto.Membership{},
		snapshotEnvelope(payload))
	return f.host.ImportSnapshot(context.Background(), f.shardID,
		bytes.NewReader(artifact))
}

// snapshotState is everything about a recorded snapshot that must be true of
// the file it points at.
type snapshotState struct {
	record logdb.Snapshot
	bytes  []byte
	meta   []byte
}

// capture reads the shard's recorded snapshot together with the on-disk artifact
// and metadata it describes.
func (f *liveImportFixture) capture(t *testing.T) snapshotState {
	t.Helper()
	rec, err := f.db.GetSnapshot(f.shardID, f.replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	data, readErr := os.ReadFile(filepath.Join(rec.Filepath, "snapshot.dat"))
	if readErr != nil {
		t.Fatalf("reading the recorded snapshot artifact: %v", readErr)
	}
	meta, metaErr := os.ReadFile(filepath.Join(rec.Filepath, "snapshot.meta"))
	if metaErr != nil {
		t.Fatalf("reading the recorded snapshot metadata: %v", metaErr)
	}
	return snapshotState{record: rec, bytes: data, meta: meta}
}

// assertUnchanged fails with the specific damage a destructive import causes.
func assertUnchanged(t *testing.T, before, after snapshotState) {
	t.Helper()
	// Compared field by field: logdb.Snapshot is not comparable.
	if after.record.Index != before.record.Index ||
		after.record.Term != before.record.Term ||
		after.record.Filepath != before.record.Filepath ||
		after.record.FileSize != before.record.FileSize ||
		after.record.OnDiskIndex != before.record.OnDiskIndex ||
		after.record.Epoch != before.record.Epoch ||
		after.record.Compression != before.record.Compression {
		t.Errorf("the LogDB snapshot record changed: %+v, want %+v", after.record, before.record)
	}
	if !bytes.Equal(after.bytes, before.bytes) {
		t.Errorf("the LIVE snapshot artifact was rewritten: %d bytes on disk, want the "+
			"original %d; the LogDB record still describes the original, so its FileSize "+
			"and Epoch no longer match the file",
			len(after.bytes), len(before.bytes))
	}
	if !bytes.Equal(after.meta, before.meta) {
		t.Error("the LIVE snapshot's snapshot.meta was rewritten; the DEK purge floor is " +
			"computed by reading it back as fact")
	}
	if uint64(len(after.bytes)) != after.record.FileSize {
		t.Errorf("the recorded FileSize %d no longer matches the %d bytes on disk: the "+
			"send path ships a truncated snapshot and erases the receiving follower's state",
			after.record.FileSize, len(after.bytes))
	}
}

// TestImportSnapshot_StaleRetryLeavesTheLiveSnapshotIntact verifies a non-advancing import is refused and the live snapshot is unchanged.
func TestImportSnapshot_StaleRetryLeavesTheLiveSnapshotIntact(t *testing.T) {
	tests := []struct {
		name    string
		barrier bool
	}{
		{name: "plaintext"},
		{name: "encrypted at rest", barrier: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newLiveImportFixture(t, tt.barrier)

			if err := f.importArtifact(t, 900, []byte("ORIGINAL")); err != nil {
				t.Fatalf("first ImportSnapshot failed: %v", err)
			}
			before := f.capture(t)
			if tt.barrier && before.record.Epoch == 0 {
				t.Fatal("the first import recorded Epoch 0 although a barrier is configured; " +
					"this test cannot observe the Epoch damage it guards")
			}

			// Retry at the same index with a longer, different payload.
			retryErr := f.importArtifact(t, 900,
				[]byte("A-COMPLETELY-DIFFERENT-AND-MUCH-LONGER-REPLACEMENT-PAYLOAD"))
			if retryErr == nil {
				t.Fatal("the retried import reported success although the LogDB cannot " +
					"record a snapshot that does not advance the index")
			}
			if !errors.Is(retryErr, logdb.ErrSnapshotOutOfDate) {
				t.Fatalf("retried import error = %v, want logdb.ErrSnapshotOutOfDate", retryErr)
			}

			assertUnchanged(t, before, f.capture(t))
			assertNoImportStagingDirs(t, filepath.Dir(before.record.Filepath))

			// A refusal must also be recoverable: the shard still starts from the
			// snapshot the operator believes it has.
			assertShardRecoversPayload(t, f, []byte("ORIGINAL"))
		})
	}
}

// TestImportSnapshot_RefusesAPreExistingDestinationDirectory verifies an unreferenced directory at the artifact's index is refused.
func TestImportSnapshot_RefusesAPreExistingDestinationDirectory(t *testing.T) {
	f := newLiveImportFixture(t, false)

	// A snapshot recorded at 500 with its directory on disk...
	if err := f.importArtifact(t, 500, []byte("RECORDED-AT-500")); err != nil {
		t.Fatalf("ImportSnapshot at 500 failed: %v", err)
	}
	// ...and a newer directory at 900 that LogDB does not reference.
	orphan := engine.SnapshotSaveDir(f.base, f.shardID, f.replicaID, 900)
	if err := os.MkdirAll(orphan, 0o750); err != nil {
		t.Fatalf("seeding the colliding directory: %v", err)
	}
	orphanData := []byte("PRE-EXISTING-ARTIFACT-AT-900")
	if err := os.WriteFile(filepath.Join(orphan, "snapshot.dat"), orphanData, 0o600); err != nil {
		t.Fatalf("seeding the colliding artifact: %v", err)
	}
	before := f.capture(t)

	// 900 advances the recorded index (500), so the staleness check passes and
	// only the destination check stands between the import and the directory.
	err := f.importArtifact(t, 900, []byte("REPLACEMENT"))
	if !errors.Is(err, ErrImportSnapshotDirExists) {
		t.Fatalf("import onto an existing directory: err = %v, want ErrImportSnapshotDirExists", err)
	}
	var existsErr *ImportSnapshotDirExistsError
	if !errors.As(err, &existsErr) {
		t.Fatalf("error = %v (%T), want *ImportSnapshotDirExistsError", err, err)
	}
	if existsErr.Dir != orphan || existsErr.Index != 900 {
		t.Errorf("error = %+v, want Dir %q and Index 900", existsErr, orphan)
	}

	got, readErr := os.ReadFile(filepath.Join(orphan, "snapshot.dat"))
	if readErr != nil {
		t.Fatalf("the pre-existing artifact at %s is GONE after a refused import: %v "+
			"(a failed import must never delete a directory it did not create)", orphan, readErr)
	}
	if !bytes.Equal(got, orphanData) {
		t.Errorf("the pre-existing artifact was rewritten: %q, want %q", got, orphanData)
	}
	assertUnchanged(t, before, f.capture(t))
	assertNoImportStagingDirs(t, filepath.Dir(orphan))
}

// TestImportSnapshot_FailureMidStreamDiscardsOnlyItsOwnStaging pins the
// discard's blast radius directly: an import that dies while copying the
// artifact removes what IT created and nothing else.
func TestImportSnapshot_FailureMidStreamDiscardsOnlyItsOwnStaging(t *testing.T) {
	f := newLiveImportFixture(t, false)

	if err := f.importArtifact(t, 900, []byte("LIVE-SNAPSHOT")); err != nil {
		t.Fatalf("ImportSnapshot at 900 failed: %v", err)
	}
	before := f.capture(t)

	// An artifact at a higher index whose body fails mid-copy.
	artifact := buildSnapshotArtifact(t, 1200, 6, 0, proto.Membership{},
		snapshotEnvelope([]byte("this payload is never fully delivered")))
	wantErr := errors.New("import test: reader failed mid-stream")
	body := io.MultiReader(
		bytes.NewReader(artifact[:len(artifact)-10]),
		&erroringReader{err: wantErr},
	)

	err := f.host.ImportSnapshot(context.Background(), f.shardID, body)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ImportSnapshot error = %v, want the reader's error", err)
	}

	assertUnchanged(t, before, f.capture(t))
	if _, statErr := os.Stat(engine.SnapshotSaveDir(f.base, f.shardID, f.replicaID, 1200)); statErr == nil {
		t.Error("the failed import published a directory at its index; a half-written " +
			"artifact is what latestSnapshotDir would recover from")
	}
	assertNoImportStagingDirs(t, filepath.Dir(before.record.Filepath))
}

// TestImportSnapshot_ConcurrentImportsOfTheSameIndex verifies the third trigger:
// nothing outside ImportSnapshot serialises two operators (or one retrying
// automation) importing the same artifact at once. Exactly one may win, and the
// artifact the winner recorded must describe the file on disk.
func TestImportSnapshot_ConcurrentImportsOfTheSameIndex(t *testing.T) {
	f := newLiveImportFixture(t, false)

	const n = 4
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = f.importArtifact(t, 900, []byte("CONCURRENT-RESTORE"))
		}(i)
	}
	wg.Wait()

	winners := 0
	for i, err := range errs {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, logdb.ErrSnapshotOutOfDate),
			errors.Is(err, ErrImportSnapshotDirExists):
			// The two ways a loser is refused, both non-destructive.
		default:
			t.Errorf("concurrent import %d failed unexpectedly: %v", i, err)
		}
	}
	if winners != 1 {
		t.Fatalf("%d concurrent imports of the same index succeeded, want exactly 1", winners)
	}

	// The survivor must be self-consistent: the record describes the file.
	after := f.capture(t)
	if uint64(len(after.bytes)) != after.record.FileSize {
		t.Errorf("recorded FileSize %d does not match the %d bytes on disk",
			after.record.FileSize, len(after.bytes))
	}
	assertNoImportStagingDirs(t, filepath.Dir(after.record.Filepath))
	assertShardRecoversPayload(t, f, []byte("CONCURRENT-RESTORE"))
}

// TestImportSnapshot_RejectsIndexZero verifies index 0, meaning no snapshot, is refused.
func TestImportSnapshot_RejectsIndexZero(t *testing.T) {
	f := newLiveImportFixture(t, false)

	err := f.importArtifact(t, 0, []byte("payload"))
	var artErr *SnapshotArtifactError
	if !errors.As(err, &artErr) {
		t.Fatalf("import of an index-0 artifact: err = %v (%T), want *SnapshotArtifactError", err, err)
	}
	if _, getErr := f.db.GetSnapshot(f.shardID, f.replicaID); !errors.Is(getErr, logdb.ErrNoSnapshot) {
		t.Fatal("a refused index-0 import still recorded a snapshot")
	}
	entries, readErr := os.ReadDir(filepath.Join(f.base, "shard-7", "replica-3"))
	if readErr == nil && len(entries) != 0 {
		t.Errorf("a refused index-0 import left %v on disk", entries)
	}
}

// TestMockLogDB_RejectsStaleSnapshotSave verifies mockLogDB refuses a non-advancing save like memdb and waldb.
func TestMockLogDB_RejectsStaleSnapshotSave(t *testing.T) {
	cases := []struct {
		name string
		db   logdb.LogDB
	}{
		{name: "mockLogDB", db: newMockLogDB()},
		{name: "memdb", db: memdb.New()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 900, FileSize: 16}); err != nil {
				t.Fatalf("first SaveSnapshot: %v", err)
			}
			for _, stale := range []uint64{899, 900} {
				err := tc.db.SaveSnapshot(1, 1, logdb.Snapshot{Index: stale, FileSize: 4096})
				if !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
					t.Fatalf("SaveSnapshot(index %d) over a recorded 900 = %v, "+
						"want logdb.ErrSnapshotOutOfDate", stale, err)
				}
			}
			got, err := tc.db.GetSnapshot(1, 1)
			if err != nil {
				t.Fatalf("GetSnapshot: %v", err)
			}
			if got.Index != 900 || got.FileSize != 16 {
				t.Fatalf("a refused save mutated the record: %+v, want Index 900 FileSize 16", got)
			}
			// A save that does advance still lands.
			if err := tc.db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 901, FileSize: 4096}); err != nil {
				t.Fatalf("SaveSnapshot(index 901): %v", err)
			}
		})
	}
}

// assertNoImportStagingDirs fails if an import staging directory survived. A
// leaked one is a full copy of the state machine kept forever beside the
// snapshot tree.
func assertNoImportStagingDirs(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), strings.TrimSuffix(importStageDirPattern, "*")) {
			t.Errorf("import staging directory %q survived in %s", e.Name(), dir)
		}
	}
}

// assertShardRecoversPayload starts the shard and proves the state machine is
// recovered from the artifact the LogDB record names.
func assertShardRecoversPayload(t *testing.T, f *liveImportFixture, want []byte) {
	t.Helper()
	memSM := &recordingMemSM{want: want}
	create := func(_, _ uint64) interface{} { return memSM }
	scfg := testShardConfig(f.shardID, f.replicaID)
	if err := f.host.StartShard(map[uint64]string{f.replicaID: "127.0.0.1:5000"},
		false, create, scfg); err != nil {
		t.Fatalf("StartShard after the refused import failed: %v", err)
	}
	t.Cleanup(func() { _ = f.host.StopShard(f.shardID) })
	if !memSM.recoverCalled.Load() {
		t.Fatal("RecoverFromSnapshot was not called: the recorded snapshot was not recovered")
	}
}

// erroringReader fails every Read with a fixed error, modelling a truncated or
// broken artifact stream.
type erroringReader struct{ err error }

func (r *erroringReader) Read([]byte) (int, error) { return 0, r.err }
