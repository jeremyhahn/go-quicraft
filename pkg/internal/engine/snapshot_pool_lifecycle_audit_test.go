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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// newAuditNode returns a live engine node over a test snapshotter.
func newAuditNode(t *testing.T) *Node {
	t.Helper()
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	return NewNode(nil, nil, nil, cfg, newSnapshotTestSnapshotter(100), t.TempDir(), nil)
}

// awaitLeasesWithin fails the test unless the node's write leases drain
// within d: a lease leaked by the code under test hangs UnloadNode.
func awaitLeasesWithin(t *testing.T, node *Node, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		node.awaitWriteLeases()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("the node's write leases never drained: a lease leaked")
	}
}

// TestAudit_OpenLeasedSaveStage_StoppedNodeCreatesNothing checks the
// StopShard interaction of the save stage: a node that has stopped is
// refused the lease, and the open creates neither the stage's parent (the
// replica directory RemoveData deletes) nor a stage.
func TestAudit_OpenLeasedSaveStage_StoppedNodeCreatesNothing(t *testing.T) {
	node := newAuditNode(t)
	node.Stop()
	parent := filepath.Join(t.TempDir(), "replica-1")

	stageDir, f, cleanupErr, err := openLeasedSaveStage(node, parent)
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("openLeasedSaveStage on a stopped node = %v, want ErrStopped", err)
	}
	if f != nil || stageDir != "" || cleanupErr != nil {
		t.Fatalf("openLeasedSaveStage on a stopped node returned (%q, %v, %v), want nothing", stageDir, f, cleanupErr)
	}
	if _, statErr := os.Stat(parent); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("stage parent after a refused open: stat = %v, want not exist", statErr)
	}
	awaitLeasesWithin(t, node, 2*time.Second)
}

// TestAudit_OpenLeasedSaveStage_LiveNodeReleasesLease checks that a
// successful open releases its lease, so a StopShard that follows is not
// held by a save that is still streaming into the stage.
func TestAudit_OpenLeasedSaveStage_LiveNodeReleasesLease(t *testing.T) {
	node := newAuditNode(t)
	parent := filepath.Join(t.TempDir(), "replica-1")

	stageDir, f, cleanupErr, err := openLeasedSaveStage(node, parent)
	if err != nil || cleanupErr != nil || f == nil {
		t.Fatalf("openLeasedSaveStage = (%v, %v, %v), want an open file", f, cleanupErr, err)
	}
	t.Cleanup(func() {
		_ = f.Close()
		_ = os.RemoveAll(stageDir)
	})
	if filepath.Dir(stageDir) != parent {
		t.Fatalf("stage %q is not under its parent %q", stageDir, parent)
	}
	node.Stop()
	awaitLeasesWithin(t, node, 2*time.Second)
}

// TestAudit_WriteLeasedSnapshotMetadata_StoppedNodeWritesNothing checks the
// StopShard interaction of the metadata write: a node that stopped after
// the capture is refused, and no metadata file appears in the save dir.
func TestAudit_WriteLeasedSnapshotMetadata_StoppedNodeWritesNothing(t *testing.T) {
	node := newAuditNode(t)
	node.Stop()
	dir := t.TempDir()

	err := writeLeasedSnapshotMetadata(node, dir, 5, 1, 10, 1, 0, config.NoCompression)
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("writeLeasedSnapshotMetadata on a stopped node = %v, want ErrStopped", err)
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatalf("ReadDir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("save dir holds %d entries after a refused metadata write, want 0", len(entries))
	}
	awaitLeasesWithin(t, node, 2*time.Second)
}

// TestAudit_WriteLeasedSnapshotMetadata_LiveNodeWritesAndReleases checks the
// live path: the metadata is written and the lease is released afterwards.
func TestAudit_WriteLeasedSnapshotMetadata_LiveNodeWritesAndReleases(t *testing.T) {
	node := newAuditNode(t)
	dir := t.TempDir()
	// The metadata records the data file's checksum, so the save's data file
	// is in place first, as it is on the save path.
	if err := os.WriteFile(filepath.Join(dir, snapshotDataFile), []byte("snapshot"), 0o600); err != nil {
		t.Fatalf("write data file: %v", err)
	}

	if err := writeLeasedSnapshotMetadata(node, dir, 5, 1, 10, 1, 0, config.NoCompression); err != nil {
		t.Fatalf("writeLeasedSnapshotMetadata = %v", err)
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatalf("ReadDir: %v", readErr)
	}
	if len(entries) < 2 {
		t.Fatal("no metadata file written on a live node")
	}
	node.Stop()
	awaitLeasesWithin(t, node, 2*time.Second)
}

// TestAudit_RunRecovery_StoppedNodeReportsNoIndex checks the snapshot
// recovery / StopShard interaction of the recovered-index propagation: a
// recovery refused because the node stopped reports index 0 with a
// *SnapshotError wrapping ErrStopped, never a recovered index, and leaks no
// lease.
func TestAudit_RunRecovery_StoppedNodeReportsNoIndex(t *testing.T) {
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })
	p := newSnapshotPool(2, newSnapshotTestLogDB(), nil, nil, nil, stopC, nil)
	node := newAuditNode(t)
	node.Stop()

	index, err := p.runRecovery(snapshotRequest{shardID: 10, replicaID: 1, node: node})
	if index != 0 {
		t.Fatalf("runRecovery on a stopped node reported index %d, want 0", index)
	}
	var se *SnapshotError
	if !errors.As(err, &se) || !errors.Is(err, ErrStopped) || se.Save {
		t.Fatalf("runRecovery on a stopped node = %v, want a recovery *SnapshotError wrapping ErrStopped", err)
	}
	awaitLeasesWithin(t, node, 2*time.Second)
}

// TestAudit_HandleSnapshot_RecoverWithNothingToRestoreReportsZero checks the
// recovery index path when no snapshot is recorded: the recovery restores
// nothing and reports index 0, with no compaction.
func TestAudit_HandleSnapshot_RecoverWithNothingToRestoreReportsZero(t *testing.T) {
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })
	p := newSnapshotPool(2, newSnapshotTestLogDB(), nil, nil, nil, stopC, nil)
	node := newAuditNode(t)
	t.Cleanup(node.Stop)

	index, compaction, err := p.handleSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: node})
	if err != nil {
		t.Fatalf("handleSnapshot(recover, no snapshot) = %v", err)
	}
	if index != 0 || compaction != (LogCompaction{}) {
		t.Fatalf("handleSnapshot(recover, no snapshot) = (%d, %+v), want (0, no compaction)", index, compaction)
	}
}
