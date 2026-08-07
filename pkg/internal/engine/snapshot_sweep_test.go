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
func TestSweepStaleDecryptTmp_FailureIsReported(t *testing.T) {
	dir := t.TempDir()
	orphan := filepath.Join(dir, ".send-decrypt-abc.tmp")
	if err := os.WriteFile(orphan, []byte("plaintext state machine"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	// Deny write on the directory so the unlink fails, modelling a read-only
	// mount or a permissions change.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Logf("restore dir mode: %v", err)
		}
	})

	before := StaleDecryptSweepFailures()
	SweepStaleDecryptTmp(dir)

	if _, err := os.Stat(orphan); err != nil {
		t.Skipf("the orphan was removed despite a non-writable directory "+
			"(%v): this process bypasses directory permissions, so the failure "+
			"path cannot be exercised here", err)
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
func TestSweepStaleDecryptTmp_UnreadableDirectoryIsReported(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "snapshot-dir")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".decrypt-abc.tmp"), []byte("plaintext"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Logf("restore dir mode: %v", err)
		}
	})

	before := StaleDecryptSweepFailures()
	if entries, err := os.ReadDir(dir); err == nil {
		t.Skipf("the unreadable directory listed %d entries: this process bypasses "+
			"directory permissions, so the failure path cannot be exercised here",
			len(entries))
	}
	SweepStaleDecryptTmp(dir)

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
