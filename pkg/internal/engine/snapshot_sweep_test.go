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
	"encoding/binary"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestSweepStaleDecryptTmp_FailureIsReported is the L2 regression guard.
//
// The sweeper cannot report a failure to any caller — it must never abort
// startup or recovery — so a failed removal is invisible unless the sweeper
// itself raises it. A read-only mount, a permissions change, or an ownership
// change therefore leaves a COMPLETE PLAINTEXT COPY of the state machine on
// disk with nothing anywhere to say so. The counter is the observable a monitor
// can alert on; the WARN is what an operator reading logs sees.
// The failure is injected through sweepRemove rather than by chmod-ing the
// directory. A chmod does not stop root, and the CI container runs the suite
// as root, so the permission-based version of this test SKIPPED there — which
// is precisely the environment the regression would ship from. The injected
// error is the same EACCES a read-only mount produces, and the assertion is
// unchanged: the sweeper must count and log what it could not reclaim.
func TestSweepStaleDecryptTmp_FailureIsReported(t *testing.T) {
	dir := t.TempDir()
	orphan := filepath.Join(dir, ".send-decrypt-abc.tmp")
	if err := os.WriteFile(orphan, []byte("plaintext state machine"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	var attempted []string
	restore := sweepRemove
	sweepRemove = func(path string) error {
		attempted = append(attempted, path)
		return &os.PathError{Op: "remove", Path: path, Err: syscall.EACCES}
	}
	t.Cleanup(func() { sweepRemove = restore })

	before := StaleDecryptSweepFailures()
	SweepStaleDecryptTmp(dir)

	if len(attempted) != 1 || attempted[0] != orphan {
		t.Fatalf("sweeper attempted to remove %v, want exactly [%s]: it never "+
			"reached the removal, so the failure branch was not exercised",
			attempted, orphan)
	}
	after := StaleDecryptSweepFailures()
	if after <= before {
		t.Fatalf("StaleDecryptSweepFailures = %d, was %d: a plaintext copy of the "+
			"state machine was left on disk and nothing counted it", after, before)
	}
}

// TestSweepStaleDecryptTmp_UnreadableDirectoryIsReported covers the other way
// the sweep can silently not happen: the directory itself cannot be listed, so
// whether a plaintext copy is sitting in it is unknown. That is the same
// operational state as a failed removal and must be reported the same way.
// The unlistable path is a REGULAR FILE where a directory was expected, not a
// chmod 000 directory. ENOTDIR is refused for root as well, so this exercises
// the branch in every environment; the chmod version skipped under the CI
// container's root user, leaving the reporting path unverified exactly where
// it ships from. Operationally the two are the same class of fault — the sweep
// cannot look, so whether a plaintext copy is sitting there is unknown — and
// the sweeper must report both the same way.
func TestSweepStaleDecryptTmp_UnreadableDirectoryIsReported(t *testing.T) {
	parent := t.TempDir()
	notADir := filepath.Join(parent, "snapshot-dir")
	if err := os.WriteFile(notADir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Guard the premise: a listing that SUCCEEDS would make the assertion
	// below vacuous, and the failure must not be a missing path (which the
	// sweeper deliberately ignores).
	if _, err := os.ReadDir(notADir); err == nil {
		t.Fatal("os.ReadDir on a regular file succeeded; the failure branch " +
			"cannot be exercised and this test would assert nothing")
	} else if os.IsNotExist(err) {
		t.Fatalf("os.ReadDir reported not-exist (%v); the sweeper ignores that "+
			"case by design, so the counter would not move", err)
	}

	before := StaleDecryptSweepFailures()
	SweepStaleDecryptTmp(notADir)

	if after := StaleDecryptSweepFailures(); after <= before {
		t.Fatalf("StaleDecryptSweepFailures = %d, was %d: the sweep could not "+
			"inspect the directory and nothing counted it", after, before)
	}
}

// TestRecoverSnapshot_SweepsEveryRetainedSnapshotDirectory is the TG-18
// regression guard.
//
// recoverSnapshot sweeps TWICE: the whole shard tree, and then the single
// directory it is about to recover from. Only the tree sweep reclaims a
// plaintext scratch file orphaned in a DIFFERENT retained directory — which is
// where the snapshot SEND path leaves one when a leader is killed mid-stream,
// because it decrypts inside whichever directory it is streaming, not the
// newest. Losing the tree call leaves that plaintext copy on disk for the life
// of the snapshot directory while the recovery itself succeeds, so nothing else
// in the system notices.
//
// The startup sweep (Host.StartShard) is covered elsewhere; this pins the
// recovery-path call, which is the one that runs on a follower that just fell
// behind — exactly when a stale send scratch file is most likely to exist.
func TestRecoverSnapshot_SweepsEveryRetainedSnapshotDirectory(t *testing.T) {
	const shardID, replicaID uint64 = 10, 1
	baseDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	// An OLDER retained snapshot directory holding an orphaned plaintext
	// scratch file. Recovery will not read from this directory at all.
	staleDir := snapshotSaveDir(baseDir, shardID, replicaID, 50)
	if err := os.MkdirAll(staleDir, 0o750); err != nil {
		t.Fatalf("mkdir stale dir: %v", err)
	}
	staleOrphan := filepath.Join(staleDir, ".send-decrypt-orphan.tmp")
	if err := os.WriteFile(staleOrphan, []byte("plaintext state machine"), 0o600); err != nil {
		t.Fatalf("write stale orphan: %v", err)
	}
	if err := writeSnapshotMetadata(staleDir, 50, 1, shardID, replicaID, 0, config.NoCompression); err != nil {
		t.Fatalf("write stale metadata: %v", err)
	}

	// The directory recovery WILL read from, with its own orphan so the
	// single-directory sweep is exercised too.
	recoverDir := snapshotSaveDir(baseDir, shardID, replicaID, 100)
	if err := os.MkdirAll(recoverDir, 0o750); err != nil {
		t.Fatalf("mkdir recover dir: %v", err)
	}
	recoverOrphan := filepath.Join(recoverDir, ".decrypt-orphan.tmp")
	if err := os.WriteFile(recoverOrphan, []byte("plaintext state machine"), 0o600); err != nil {
		t.Fatalf("write recover orphan: %v", err)
	}
	var payload [8]byte
	binary.LittleEndian.PutUint64(payload[:], 100)
	if err := os.WriteFile(filepath.Join(recoverDir, snapshotDataFile), payload[:], 0o600); err != nil {
		t.Fatalf("write snapshot data: %v", err)
	}
	if err := writeSnapshotMetadata(recoverDir, 100, 3, shardID, replicaID, 0, config.NoCompression); err != nil {
		t.Fatalf("write recover metadata: %v", err)
	}

	ldb := newSnapshotTestLogDB()
	if err := ldb.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index:    100,
		Term:     3,
		Filepath: recoverDir,
		FileSize: uint64(len(payload)),
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
	ss := newSnapshotTestSnapshotter(0)
	node := NewNode(nil, nil, nil, cfg, 0, ss, baseDir, nil)

	if err := p.recoverSnapshot(snapshotRequest{
		shardID: shardID, replicaID: replicaID, node: node, save: false,
	}); err != nil {
		t.Fatalf("recoverSnapshot failed: %v", err)
	}
	if got := ss.LastApplied(); got != 100 {
		t.Fatalf("recovered lastApplied = %d, want 100: the recovery did not run, so "+
			"this test would not have reached the sweep either", got)
	}

	if _, err := os.Stat(staleOrphan); !os.IsNotExist(err) {
		t.Fatalf("orphaned plaintext %s survived recovery (err=%v): recovery swept only "+
			"the directory it recovered from, leaving a full plaintext copy of the state "+
			"machine in every other retained snapshot directory", staleOrphan, err)
	}
	if _, err := os.Stat(recoverOrphan); !os.IsNotExist(err) {
		t.Fatalf("orphaned plaintext %s survived recovery (err=%v)", recoverOrphan, err)
	}
	// The canonical artifacts must be untouched.
	if _, err := os.Stat(filepath.Join(recoverDir, snapshotDataFile)); err != nil {
		t.Fatalf("the sweep destroyed the canonical snapshot data file: %v", err)
	}
}

// TestRecoverSnapshot_ImportStageSweepRespectsTheImportLock is the M1
// regression guard. It covers a call site that previously had NO test at all —
// deleting the sweep from recoverSnapshot left the package green.
//
// The staging directory holds the imported state machine in PLAINTEXT and is
// indistinguishable by NAME from one a crash orphaned, so the only thing that
// can tell a live one from an orphan is the shard's import lock. recoverSnapshot
// used to sweep without taking it, justified by "no import can be in flight
// here — Host.ImportSnapshot refuses a RUNNING shard". That is a TOCTOU: the
// refusal is in importPreflight, which runs BEFORE the import lock is acquired
// and is not held for the import's duration, and StartShard registers the node
// hundreds of lines after its own sweep. So an import that passed preflight and
// is midway through a multi-gigabyte write coexists with a running shard, and a
// leader's InstallSnapshot then RemoveAll'd its staging directory — a
// spuriously failed disaster-recovery restore.
//
// Both directions are asserted: the sweep must still reclaim an orphan when the
// lock is free (which is what makes deleting the call fail), and must leave the
// directory alone when an import holds it.
func TestRecoverSnapshot_ImportStageSweepRespectsTheImportLock(t *testing.T) {
	tests := []struct {
		name string
		// importRunning models an ImportSnapshot holding the shard's lock.
		importRunning bool
		wantSurvives  bool
	}{
		{
			name:          "no import in flight: the orphan is reclaimed",
			importRunning: false,
			wantSurvives:  false,
		},
		{
			name:          "import in flight: its staging directory survives",
			importRunning: true,
			wantSurvives:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const shardID, replicaID uint64 = 11, 1
			baseDir := t.TempDir()
			stopC := make(chan struct{})
			defer close(stopC)

			// The staging directory, holding the imported state machine in
			// plaintext exactly as ImportSnapshot leaves it mid-write.
			stageDir := filepath.Join(snapshotBaseDir(baseDir, shardID, replicaID),
				ImportStageDirPrefix+"live")
			if err := os.MkdirAll(stageDir, 0o750); err != nil {
				t.Fatalf("mkdir stage dir: %v", err)
			}
			stagedArtifact := filepath.Join(stageDir, snapshotDataFile)
			if err := os.WriteFile(stagedArtifact, []byte("plaintext state machine"), 0o600); err != nil {
				t.Fatalf("write staged artifact: %v", err)
			}

			// The snapshot the leader's InstallSnapshot delivered, which is
			// what triggers the recovery.
			recoverDir := snapshotSaveDir(baseDir, shardID, replicaID, 100)
			if err := os.MkdirAll(recoverDir, 0o750); err != nil {
				t.Fatalf("mkdir recover dir: %v", err)
			}
			var payload [8]byte
			binary.LittleEndian.PutUint64(payload[:], 100)
			if err := os.WriteFile(filepath.Join(recoverDir, snapshotDataFile), payload[:], 0o600); err != nil {
				t.Fatalf("write snapshot data: %v", err)
			}
			if err := writeSnapshotMetadata(recoverDir, 100, 3, shardID, replicaID, 0, config.NoCompression); err != nil {
				t.Fatalf("write recover metadata: %v", err)
			}

			ldb := newSnapshotTestLogDB()
			if err := ldb.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
				Index:    100,
				Term:     3,
				Filepath: recoverDir,
				FileSize: uint64(len(payload)),
			}); err != nil {
				t.Fatalf("SaveSnapshot: %v", err)
			}

			p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
			// The host's real lock is a capacity-one channel; model it with the
			// same try-semantics Host.tryAcquireImportLock provides.
			lock := make(chan struct{}, 1)
			if tt.importRunning {
				lock <- struct{}{}
			}
			var released atomic.Bool
			p.tryImportLock = func(id uint64) (func(), bool) {
				if id != shardID {
					t.Errorf("import lock taken for shard %d, want %d", id, shardID)
				}
				select {
				case lock <- struct{}{}:
					return func() {
						<-lock
						released.Store(true)
					}, true
				default:
					return nil, false
				}
			}

			cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
			ss := newSnapshotTestSnapshotter(0)
			node := NewNode(nil, nil, nil, cfg, 0, ss, baseDir, nil)

			if err := p.recoverSnapshot(snapshotRequest{
				shardID: shardID, replicaID: replicaID, node: node, save: false,
			}); err != nil {
				t.Fatalf("recoverSnapshot failed: %v", err)
			}
			if got := ss.LastApplied(); got != 100 {
				t.Fatalf("recovered lastApplied = %d, want 100: the recovery did not run, "+
					"so this test would not have reached the sweep either", got)
			}

			_, statErr := os.Stat(stagedArtifact)
			switch {
			case tt.wantSurvives && statErr != nil:
				t.Fatalf("recovery deleted the staging directory of a RUNNING import "+
					"(%v): the operator's disaster-recovery restore fails for no reason",
					statErr)
			case !tt.wantSurvives && !os.IsNotExist(statErr):
				t.Fatalf("the orphaned import staging directory survived recovery "+
					"(err=%v): it holds a COMPLETE PLAINTEXT COPY of the state machine "+
					"under a name no scan on the host enumerates, so nothing else ever "+
					"reclaims it", statErr)
			}
			if !tt.importRunning && !released.Load() {
				t.Fatal("the import lock was taken and never released; the next " +
					"ImportSnapshot for this shard would block forever")
			}

			// The canonical artifact must be untouched either way.
			if _, err := os.Stat(filepath.Join(recoverDir, snapshotDataFile)); err != nil {
				t.Fatalf("the sweep destroyed the canonical snapshot data file: %v", err)
			}
		})
	}
}
