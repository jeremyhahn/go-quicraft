package quicraft

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
)

// TestStartShard_DataDirChangeKeepsRecordedSnapshotsRemovable: a replica
// snapshotted under DataDir A, stopped and started again with DataDir B
// would recover its snapshot from A (the LogDB record's Filepath) while the
// bootstrap record said B, so a later RemoveData deleted B, retired the
// replica and left A's snapshots behind. StartShard refuses the changed
// layout before anything is overwritten, so the bootstrap keeps stating A
// and RemoveData removes the snapshots that are really there.
func TestStartShard_DataDirChangeKeepsRecordedSnapshotsRemovable(t *testing.T) {
	const f = 1
	fid := uint64(f + 1)
	c := newRemoveDataCluster(t)
	dirA, dirB := t.TempDir(), t.TempDir()
	for i, h := range c.hosts {
		scfg := c.shardConfig(uint64(i + 1))
		scfg.CompactionOverhead = 10
		if i == f {
			scfg.DataDir = dirA
		}
		if err := h.StartShard(c.members(), false, hashingCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	all := []int{0, 1, 2}
	c.propose(all, "before", 60)
	c.waitHashMatch(f, all)
	fh := c.hosts[f]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := fh.SyncRequestSnapshot(ctx, removeDataShard, SnapshotOption{}); err != nil {
		t.Fatalf("SyncRequestSnapshot under DataDir A: %v", err)
	}
	snapDirA := engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(c.cfgs[f].NodeHostDir, dirA, removeDataShard), removeDataShard, fid)
	snapDirB := engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(c.cfgs[f].NodeHostDir, dirB, removeDataShard), removeDataShard, fid)
	if entries, err := os.ReadDir(snapDirA); err != nil || len(entries) == 0 {
		t.Fatalf("no snapshot under DataDir A before the restart (entries %d, err %v): %s",
			len(entries), err, snapDirA)
	}
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}

	scfgB := c.shardConfig(fid)
	scfgB.CompactionOverhead = 10
	scfgB.DataDir = dirB
	restartErr := fh.StartShard(c.members(), false, hashingCreateFunc, scfgB)
	if restartErr == nil {
		if err := fh.StopReplica(removeDataShard, fid); err != nil {
			t.Fatalf("StopReplica after the DataDir B restart: %v", err)
		}
	}
	assertDataDirChangeRefused(t, restartErr)

	if err := fh.SyncRemoveData(context.Background(), removeDataShard, fid); err != nil {
		t.Fatalf("SyncRemoveData: %v", err)
	}
	if _, err := os.Stat(snapDirA); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("RemoveData succeeded but left the snapshots under the recorded DataDir A (stat err %v): %s",
			err, snapDirA)
	}
	if _, err := os.Stat(snapDirB); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused start created a snapshot directory under DataDir B (stat err %v): %s",
			err, snapDirB)
	}
}

// assertDataDirChangeRefused fails the test unless StartShard refused the
// changed DataDir.
func assertDataDirChangeRefused(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("StartShard accepted DataDir B for a replica recorded under DataDir A")
	}
}
