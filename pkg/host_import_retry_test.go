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
	"reflect"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// A failed import must not block its own retry
// ---------------------------------------------------------------------------

// errImportTransient models the transient failure a disaster-recovery restore
// hits: a LogDB write error, a failed directory fsync, a cancelled request.
var errImportTransient = errors.New("import test: transient failure")

// flakySaveSnapshotDB fails the first n SaveSnapshot calls and then behaves
// exactly like the LogDB it wraps, modelling a transient write error.
type flakySaveSnapshotDB struct {
	logdb.LogDB
	mu        sync.Mutex
	remaining int
}

func (d *flakySaveSnapshotDB) SaveSnapshot(shardID, replicaID uint64, ss logdb.Snapshot) error {
	d.mu.Lock()
	if d.remaining > 0 {
		d.remaining--
		d.mu.Unlock()
		return errImportTransient
	}
	d.mu.Unlock()
	return d.LogDB.SaveSnapshot(shardID, replicaID, ss)
}

// TestImportSnapshot_TransientLogDBFailureDoesNotBlockItsOwnRetry is the
// regression guard.
//
// The publishing rename creates the canonical, index-named directory. An error
// path AFTER it that returned without touching that directory would leave the
// destination PUBLISHED, and the retry — the same artifact, the same index,
// therefore the same directory name — would be refused by the
// destination-exists precheck with *ImportSnapshotDirExistsError.
//
// That refusal is correct for a directory an import did not create, and it is
// exactly wrong here: the import created it. A transient LogDB error during a
// DR restore would become a hard lockout whose only remedy was a manual rm -rf,
// performed against an error message telling the operator that an import never
// removes a directory it did not touch.
//
// The assertion is that the RETRY SUCCEEDS, not merely that some directory is
// absent: recovering the restore is the property.
func TestImportSnapshot_TransientLogDBFailureDoesNotBlockItsOwnRetry(t *testing.T) {
	const shardID, replicaID uint64 = 7, 3
	const index uint64 = 900
	payload := []byte("DISASTER-RECOVERY-RESTORE")

	cfg := testHostConfig(t)
	real := memdb.New()
	db := &flakySaveSnapshotDB{LogDB: real, remaining: 1}
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	if bsErr := real.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
		Addresses: map[uint64]string{replicaID: "127.0.0.1:5000"},
	}); bsErr != nil {
		t.Fatalf("SaveBootstrap failed: %v", bsErr)
	}

	artifact := func() []byte {
		return buildSnapshotArtifact(t, index, 5, 0, proto.Membership{},
			snapshotEnvelope(payload))
	}

	firstErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact()))
	if !errors.Is(firstErr, errImportTransient) {
		t.Fatalf("first ImportSnapshot error = %v, want the transient LogDB error", firstErr)
	}

	base := shardSnapshotBaseDir(cfg.NodeHostDir, "", shardID)
	snapDir := engine.SnapshotSaveDir(base, shardID, replicaID, index)
	if _, statErr := os.Stat(snapDir); statErr == nil {
		t.Errorf("the failed import left %s published; the retry is then refused with "+
			"ImportSnapshotDirExistsError and the only remedy is a manual rm -rf", snapDir)
	} else if !os.IsNotExist(statErr) {
		t.Fatalf("stat %s: %v", snapDir, statErr)
	}
	assertNoImportStagingDirs(t, filepath.Dir(snapDir))

	// The retry the operator performs. The LogDB is healthy again; nothing the
	// failed attempt left behind may stand in its way.
	if retryErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact())); retryErr != nil {
		t.Fatalf("the retry after a transient failure was refused: %v; a transient "+
			"error has become a permanent lockout", retryErr)
	}

	rec, getErr := real.GetSnapshot(shardID, replicaID)
	if getErr != nil {
		t.Fatalf("GetSnapshot after the retry: %v", getErr)
	}
	if rec.Index != index || rec.Filepath != snapDir {
		t.Fatalf("recorded snapshot = %+v, want Index %d at %s", rec, index, snapDir)
	}
	data, readErr := os.ReadFile(filepath.Join(snapDir, "snapshot.dat"))
	if readErr != nil {
		t.Fatalf("reading the retried artifact: %v", readErr)
	}
	if uint64(len(data)) != rec.FileSize {
		t.Errorf("recorded FileSize %d does not match the %d bytes on disk",
			rec.FileSize, len(data))
	}
}

// flakyRemoveEntriesToDB fails the first n RemoveEntriesTo calls and then
// behaves exactly like the LogDB it wraps, modelling a transient error while
// an import advances the log floor after the snapshot is already recorded.
type flakyRemoveEntriesToDB struct {
	logdb.LogDB
	mu        sync.Mutex
	remaining int
}

func (d *flakyRemoveEntriesToDB) RemoveEntriesTo(shardID, replicaID, index uint64) error {
	d.mu.Lock()
	if d.remaining > 0 {
		d.remaining--
		d.mu.Unlock()
		return errImportTransient
	}
	d.mu.Unlock()
	return d.LogDB.RemoveEntriesTo(shardID, replicaID, index)
}

// TestImportSnapshot_RemoveEntriesToFailureDoesNotBlockItsOwnRetry: a
// transient failure while advancing the log floor arrives after the snapshot
// is recorded. The retry of the same artifact must complete the floor advance
// rather than be refused as out of date, or the entries at or below the
// snapshot index stay behind and the next restart fails with a log gap.
func TestImportSnapshot_RemoveEntriesToFailureDoesNotBlockItsOwnRetry(t *testing.T) {
	const shardID, replicaID uint64 = 7, 3
	const index uint64 = 900
	payload := []byte("DISASTER-RECOVERY-RESTORE")

	cfg := testHostConfig(t)
	real := memdb.New()
	db := &flakyRemoveEntriesToDB{LogDB: real, remaining: 1}
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	if bsErr := real.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
		Addresses: map[uint64]string{replicaID: "127.0.0.1:5000"},
	}); bsErr != nil {
		t.Fatalf("SaveBootstrap failed: %v", bsErr)
	}
	// The bootstrap entry StartShard writes before an offline import.
	if seedErr := real.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		Entries:   []logdb.Entry{{Index: 1, Term: 1}},
	}}); seedErr != nil {
		t.Fatalf("seed bootstrap entry: %v", seedErr)
	}

	artifact := func(term uint64) []byte {
		return buildSnapshotArtifact(t, index, term, 0, proto.Membership{},
			snapshotEnvelope(payload))
	}

	firstErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact(5)))
	if !errors.Is(firstErr, errImportTransient) {
		t.Fatalf("first ImportSnapshot error = %v, want the transient LogDB error", firstErr)
	}

	t.Run("different term at the same index is refused", func(t *testing.T) {
		err := h.ImportSnapshot(context.Background(), shardID,
			bytes.NewReader(artifact(6)))
		var outOfDate *ImportSnapshotOutOfDateError
		if !errors.As(err, &outOfDate) {
			t.Fatalf("ImportSnapshot of a different artifact at the recorded index = %v, "+
				"want *ImportSnapshotOutOfDateError", err)
		}
	})

	if retryErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact(5))); retryErr != nil {
		t.Fatalf("the retry after a RemoveEntriesTo failure was refused: %v; the "+
			"entries below the recorded snapshot can then never be removed", retryErr)
	}

	rec, getErr := real.GetSnapshot(shardID, replicaID)
	if getErr != nil {
		t.Fatalf("GetSnapshot after the retry: %v", getErr)
	}
	if rec.Index != index {
		t.Fatalf("recorded snapshot Index = %d, want %d", rec.Index, index)
	}
	first, count, rangeErr := real.EntryRange(shardID, replicaID)
	if rangeErr != nil {
		t.Fatalf("EntryRange after the retry: %v", rangeErr)
	}
	if count != 0 && first <= index {
		t.Fatalf("EntryRange after the retry = (first=%d, count=%d): an entry at or "+
			"below the snapshot index %d survived", first, count, index)
	}

	t.Run("completed import is not retried again", func(t *testing.T) {
		err := h.ImportSnapshot(context.Background(), shardID,
			bytes.NewReader(artifact(5)))
		var outOfDate *ImportSnapshotOutOfDateError
		if !errors.As(err, &outOfDate) {
			t.Fatalf("re-import after a completed import = %v, "+
				"want *ImportSnapshotOutOfDateError", err)
		}
	})
}

// importRetryFixture is a host whose first import of artifact (with
// firstOpts) recorded its snapshot and then failed in RemoveEntriesTo, so the
// LogDB still holds the bootstrap entry below the recorded snapshot index.
type importRetryFixture struct {
	h     *Host
	real  logdb.LogDB
	index uint64
}

const importRetryShard, importRetryReplica uint64 = 7, 3

func newImportRetryFixture(t *testing.T, index uint64, artifact []byte,
	firstOpts ...ImportOption) *importRetryFixture {
	t.Helper()
	real := memdb.New()
	db := &flakyRemoveEntriesToDB{LogDB: real, remaining: 1}
	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if bsErr := real.SaveBootstrap(importRetryShard, importRetryReplica, logdb.Bootstrap{
		Addresses: map[uint64]string{importRetryReplica: "127.0.0.1:5000"},
	}); bsErr != nil {
		t.Fatalf("SaveBootstrap failed: %v", bsErr)
	}
	if seedErr := real.SaveState([]logdb.Update{{
		ShardID:   importRetryShard,
		ReplicaID: importRetryReplica,
		Entries:   []logdb.Entry{{Index: 1, Term: 1}},
	}}); seedErr != nil {
		t.Fatalf("seed bootstrap entry: %v", seedErr)
	}
	firstErr := h.ImportSnapshot(context.Background(), importRetryShard,
		bytes.NewReader(artifact), firstOpts...)
	if !errors.Is(firstErr, errImportTransient) {
		t.Fatalf("first ImportSnapshot error = %v, want the transient LogDB error", firstErr)
	}
	return &importRetryFixture{h: h, real: real, index: index}
}

// recorded returns the recorded snapshot's membership.
func (f *importRetryFixture) recorded(t *testing.T) logdb.Membership {
	t.Helper()
	rec, err := f.real.GetSnapshot(importRetryShard, importRetryReplica)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	return rec.Membership
}

// requireFloorUntouched fails unless an entry at or below the recorded
// snapshot index is still in the LogDB.
func (f *importRetryFixture) requireFloorUntouched(t *testing.T) {
	t.Helper()
	first, count, err := f.real.EntryRange(importRetryShard, importRetryReplica)
	if err != nil {
		t.Fatalf("EntryRange: %v", err)
	}
	if count == 0 || first > f.index {
		t.Fatalf("EntryRange = (first=%d, count=%d): the refused retry advanced the "+
			"log floor past the snapshot index %d", first, count, f.index)
	}
}

// TestImportSnapshot_RetryWithChangedMembershipIsRefused checks that the
// retry of an import that recorded its snapshot is only completed when it
// resolves the same membership: a retry asking for another membership would
// otherwise return nil while the shard restarts into the recorded one.
func TestImportSnapshot_RetryWithChangedMembershipIsRefused(t *testing.T) {
	const index uint64 = 900
	artifact := buildSnapshotArtifact(t, index, 5, 0, proto.Membership{},
		snapshotEnvelope([]byte("DISASTER-RECOVERY-RESTORE")))
	members := map[uint64]string{importRetryReplica: "127.0.0.1:5000"}

	cases := []struct {
		name  string
		first []ImportOption
		retry []ImportOption
	}{
		{
			name:  "changed members",
			first: []ImportOption{WithImportMembership(members)},
			retry: []ImportOption{WithImportMembership(map[uint64]string{
				importRetryReplica: "127.0.0.1:6000",
			})},
		},
		{
			name:  "changed identities",
			first: []ImportOption{WithImportMembership(members)},
			retry: []ImportOption{
				WithImportMembership(members),
				WithImportMemberIdentities(map[uint64]string{importRetryReplica: "node-retry"}),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newImportRetryFixture(t, index, artifact, tc.first...)
			before := f.recorded(t)

			err := f.h.ImportSnapshot(context.Background(), importRetryShard,
				bytes.NewReader(artifact), tc.retry...)
			var outOfDate *ImportSnapshotOutOfDateError
			if !errors.As(err, &outOfDate) {
				t.Fatalf("retry with a changed membership = %v, "+
					"want *ImportSnapshotOutOfDateError", err)
			}
			if after := f.recorded(t); !reflect.DeepEqual(after, before) {
				t.Fatalf("recorded membership changed by a refused retry: %+v, want %+v",
					after, before)
			}
			f.requireFloorUntouched(t)
		})
	}
}

// TestImportSnapshot_RetryWithMalformedArtifactIsRefused checks that the
// retry of an import that recorded its snapshot verifies the whole artifact
// before it advances the log floor: a stream with a matching header and a
// truncated, damaged or unreadable body is not the artifact that was recorded.
func TestImportSnapshot_RetryWithMalformedArtifactIsRefused(t *testing.T) {
	const index uint64 = 900
	full := buildSnapshotArtifact(t, index, 5, 0, proto.Membership{},
		snapshotEnvelope([]byte("DISASTER-RECOVERY-RESTORE")))
	hdr, err := marshalSnapshotArtifactHeader(snapshotArtifactHeader{Index: index, Term: 5})
	if err != nil {
		t.Fatalf("marshalSnapshotArtifactHeader: %v", err)
	}
	flipped := bytes.Clone(full)
	flipped[len(hdr)+2] ^= 0xff
	errRead := errors.New("artifact source failed")

	cases := []struct {
		name string
		r    func() io.Reader
	}{
		{"header only", func() io.Reader { return bytes.NewReader(full[:len(hdr)]) }},
		{"flipped payload byte", func() io.Reader { return bytes.NewReader(flipped) }},
		{"no trailer", func() io.Reader {
			return bytes.NewReader(full[:len(full)-snapshotArtifactTrailerSize])
		}},
		{"reader error", func() io.Reader {
			return io.MultiReader(bytes.NewReader(full[:len(hdr)+4]), iotest.ErrReader(errRead))
		}},
	}
	f := newImportRetryFixture(t, index, full)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := f.h.ImportSnapshot(context.Background(), importRetryShard,
				tc.r()); err == nil {
				t.Fatal("retry with a malformed artifact returned nil")
			}
			f.requireFloorUntouched(t)
		})
	}

	if err := f.h.ImportSnapshot(context.Background(), importRetryShard,
		bytes.NewReader(full)); err != nil {
		t.Fatalf("retry with the intact artifact after the refusals: %v", err)
	}
}

// TestImportSnapshot_RetryWithDifferentPayloadIsRefused: a retry whose
// header matches the recorded snapshot (Index, Term, OnDiskIndex,
// membership) but whose payload differs, with a valid trailer of its own, is
// a different artifact. Completing it would report success for a restore
// whose bytes were never adopted; it is refused, the floor stays where it
// was, and the retry of the recorded artifact still completes.
func TestImportSnapshot_RetryWithDifferentPayloadIsRefused(t *testing.T) {
	const index uint64 = 900
	artifact := func(payload string) []byte {
		return buildSnapshotArtifact(t, index, 5, 0, proto.Membership{},
			snapshotEnvelope([]byte(payload)))
	}
	f := newImportRetryFixture(t, index, artifact("DISASTER-RECOVERY-RESTORE"))

	err := f.h.ImportSnapshot(context.Background(), importRetryShard,
		bytes.NewReader(artifact("A-DIFFERENT-RESTORE-PAYLOAD")))
	var outOfDate *ImportSnapshotOutOfDateError
	if !errors.As(err, &outOfDate) {
		t.Fatalf("retry with a different payload = %v, want *ImportSnapshotOutOfDateError", err)
	}
	f.requireFloorUntouched(t)

	if err := f.h.ImportSnapshot(context.Background(), importRetryShard,
		bytes.NewReader(artifact("DISASTER-RECOVERY-RESTORE"))); err != nil {
		t.Fatalf("retry with the recorded artifact after the refusal: %v", err)
	}
}

// TestImportSnapshot_RetryWithEmptyEntryRangeCompletes: the pending floor
// advance is recorded explicitly, not inferred from the log. A shard whose
// log holds no entry at all (bootstrap only) still has a floor advance
// pending after RemoveEntriesTo fails, and the identical retry completes it.
func TestImportSnapshot_RetryWithEmptyEntryRangeCompletes(t *testing.T) {
	const index uint64 = 900
	real := memdb.New()
	db := &flakyRemoveEntriesToDB{LogDB: real, remaining: 1}
	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if bsErr := real.SaveBootstrap(importRetryShard, importRetryReplica, logdb.Bootstrap{
		Addresses: map[uint64]string{importRetryReplica: "127.0.0.1:5000"},
	}); bsErr != nil {
		t.Fatalf("SaveBootstrap failed: %v", bsErr)
	}
	artifact := buildSnapshotArtifact(t, index, 5, 0, proto.Membership{},
		snapshotEnvelope([]byte("DISASTER-RECOVERY-RESTORE")))

	firstErr := h.ImportSnapshot(context.Background(), importRetryShard, bytes.NewReader(artifact))
	if !errors.Is(firstErr, errImportTransient) {
		t.Fatalf("first ImportSnapshot error = %v, want the transient LogDB error", firstErr)
	}
	if err := h.ImportSnapshot(context.Background(), importRetryShard,
		bytes.NewReader(artifact)); err != nil {
		t.Fatalf("retry with an empty entry range was refused: %v", err)
	}
	err = h.ImportSnapshot(context.Background(), importRetryShard, bytes.NewReader(artifact))
	var outOfDate *ImportSnapshotOutOfDateError
	if !errors.As(err, &outOfDate) {
		t.Fatalf("re-import after the completed retry = %v, want *ImportSnapshotOutOfDateError", err)
	}
}

// TestImportSnapshot_PostPublishFailuresRollBackThePublish covers the other two
// error paths after the rename — the directory-chain fsync and the
// context check — through the injected filesystem, and asserts the same
// property: the canonical directory the import created does not survive the
// failure, so the retry is not refused.
func TestImportSnapshot_PostPublishFailuresRollBackThePublish(t *testing.T) {
	const shardID, replicaID uint64 = 5, 3
	const index uint64 = 42

	tests := []struct {
		name string
		// arm installs the failure on the fixture's filesystem; it returns the
		// context the import must be called with.
		arm func(t *testing.T, cfs *crashFS, snapDir string) (context.Context, error)
	}{
		{
			name: "directory chain fsync fails",
			arm: func(_ *testing.T, cfs *crashFS, snapDir string) (context.Context, error) {
				cfs.failOnce(opDirSync, filepath.Base(snapDir), errImportTransient)
				return context.Background(), errImportTransient
			},
		},
		{
			name: "context cancelled after the publish",
			arm: func(_ *testing.T, cfs *crashFS, snapDir string) (context.Context, error) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				// Cancel as the published directory is fsynced, so the import
				// reaches the ctx.Err() check with the rename already done.
				name := filepath.Base(snapDir)
				cfs.setHook(func(op crashOp, path string) error {
					if op == opDirSync && filepath.Base(path) == name {
						cancel()
					}
					return nil
				})
				return ctx, ErrCanceled
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testHostConfig(t)
			db := memdb.New()
			cfs := newCrashFS(cfg.NodeHostDir)
			h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), withSnapshotFS(cfs), WithoutEncryption())
			if err != nil {
				t.Fatalf("NewHost failed: %v", err)
			}
			defer h.Close()

			if bsErr := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
				Addresses: map[uint64]string{replicaID: "127.0.0.1:5000"},
			}); bsErr != nil {
				t.Fatalf("SaveBootstrap failed: %v", bsErr)
			}

			base := shardSnapshotBaseDir(cfg.NodeHostDir, "", shardID)
			snapDir := engine.SnapshotSaveDir(base, shardID, replicaID, index)
			payload := []byte("ROLLBACK-THE-PUBLISH")
			artifact := func() []byte {
				return buildSnapshotArtifact(t, index, 5, 0, proto.Membership{},
					snapshotEnvelope(payload))
			}

			ctx, wantErr := tt.arm(t, cfs, snapDir)
			firstErr := h.ImportSnapshot(ctx, shardID, bytes.NewReader(artifact()))
			if !errors.Is(firstErr, wantErr) {
				t.Fatalf("ImportSnapshot error = %v, want %v", firstErr, wantErr)
			}
			if cfs.exists(snapDir) {
				t.Fatalf("the failed import left %s published; its own retry is then "+
					"refused with ImportSnapshotDirExistsError", snapDir)
			}
			if _, recErr := db.GetSnapshot(shardID, replicaID); !errors.Is(recErr, logdb.ErrNoSnapshot) {
				t.Fatalf("GetSnapshot after the failed import = %v, want ErrNoSnapshot", recErr)
			}

			// The retry, with the fault cleared.
			cfs.setHook(nil)
			if retryErr := h.ImportSnapshot(context.Background(), shardID,
				bytes.NewReader(artifact())); retryErr != nil {
				t.Fatalf("the retry after a transient failure was refused: %v", retryErr)
			}
			if !cfs.exists(filepath.Join(snapDir, "snapshot.dat")) {
				t.Fatal("the retry recorded a snapshot with no artifact on disk")
			}
		})
	}
}

// TestImportSnapshot_CrashAtEveryStepLeavesAConsistentTree walks the crash point
// across EVERY filesystem operation an import performs and asserts the invariant
// the import's fsync discipline exists to hold: the canonical directory is never
// reachable after a crash unless it is COMPLETE — both the artifact and the
// self-describing snapshot.meta, with the content that was written.
//
// A half-complete canonical directory is not a cosmetic problem. latestSnapshotDir
// recovers the highest-numbered directory, and the DEK purge-floor scan reads
// snapshot.meta from each directory it enumerates and treats a missing file as
// "imposes no floor" — so an artifact published without its metadata loses the
// DEK it needs at the next PurgeEpochsBefore.
//
// The sweep refuses to pass vacuously: it must reach the point where the
// directory IS published, otherwise every case is trivially satisfied by an
// absent directory.
func TestImportSnapshot_CrashAtEveryStepLeavesAConsistentTree(t *testing.T) {
	const shardID, replicaID uint64 = 5, 3
	const index uint64 = 42
	payload := []byte("crash-matrix-artifact")

	published := 0
	for step := 1; step <= 40; step++ {
		cfg := testHostConfig(t)
		db := memdb.New()
		cfs := newCrashFS(cfg.NodeHostDir)
		h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), withSnapshotFS(cfs), WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost failed: %v", err)
		}
		if bsErr := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
			Addresses: map[uint64]string{replicaID: "127.0.0.1:5000"},
		}); bsErr != nil {
			t.Fatalf("SaveBootstrap failed: %v", bsErr)
		}

		var mu sync.Mutex
		seen := 0
		fired := false
		cfs.setHook(func(_ crashOp, _ string) error {
			mu.Lock()
			defer mu.Unlock()
			seen++
			if seen != step || fired {
				return nil
			}
			fired = true
			cfs.Crash()
			return errCrashed
		})

		artifact := buildSnapshotArtifact(t, index, 5, 0, proto.Membership{},
			snapshotEnvelope(payload))
		importErr := h.ImportSnapshot(context.Background(), shardID, bytes.NewReader(artifact))

		base := shardSnapshotBaseDir(cfg.NodeHostDir, "", shardID)
		snapDir := engine.SnapshotSaveDir(base, shardID, replicaID, index)
		if cfs.exists(snapDir) {
			published++
			for _, name := range []string{"snapshot.dat", "snapshot.meta"} {
				path := filepath.Join(snapDir, name)
				data, ok := cfs.durableBytes(path)
				if !ok {
					t.Fatalf("crash at step %d (import err %v): %s is published but %s did "+
						"not survive; recovery picks the highest-numbered directory and the "+
						"purge-floor scan reads a MISSING snapshot.meta as 'imposes no floor'",
						step, importErr, snapDir, path)
				}
				if len(data) == 0 {
					t.Fatalf("crash at step %d: %s survived empty", step, path)
				}
			}
		}
		if rec, recErr := db.GetSnapshot(shardID, replicaID); recErr == nil {
			if !cfs.exists(rec.Filepath) {
				t.Fatalf("crash at step %d: LogDB records a snapshot at %s that did not "+
					"survive; StartShard then fails closed on snapshot_open",
					step, rec.Filepath)
			}
		}
		_ = h.Close()
		if fired {
			continue
		}
		// The import ran to completion without reaching the injected step: every
		// crash point has been covered.
		break
	}

	if published == 0 {
		t.Fatal("no crash point left the canonical directory published, so the invariant " +
			"was never actually evaluated")
	}
}
