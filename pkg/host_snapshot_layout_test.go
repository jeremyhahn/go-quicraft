package quicraft

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// legacyBootstrapLogDB models a replica joined before joins were recorded:
// once hide is set, GetBootstrap reports no record until a new one is saved.
type legacyBootstrapLogDB struct {
	logdb.LogDB
	mu     sync.Mutex
	hidden bool
}

func (w *legacyBootstrapLogDB) GetBootstrap(shardID, replicaID uint64) (logdb.Bootstrap, bool, error) {
	w.mu.Lock()
	hidden := w.hidden
	w.mu.Unlock()
	if hidden {
		return logdb.Bootstrap{}, false, nil
	}
	return w.LogDB.GetBootstrap(shardID, replicaID)
}

func (w *legacyBootstrapLogDB) SaveBootstrap(shardID, replicaID uint64, bs logdb.Bootstrap) error {
	if err := w.LogDB.SaveBootstrap(shardID, replicaID, bs); err != nil {
		return err
	}
	w.mu.Lock()
	w.hidden = false
	w.mu.Unlock()
	return nil
}

func (w *legacyBootstrapLogDB) hide() {
	w.mu.Lock()
	w.hidden = true
	w.mu.Unlock()
}

// TestStartShard_LegacyJoinDataDirChangeKeepsSnapshotsRemovable: a replica
// joined under DataDir A before joins were recorded has a snapshot record
// under A and no bootstrap record. Restarting it (join=true) with DataDir B
// must be refused, since recording B would make a later RemoveData delete
// B's directory and report success while A's snapshots survive.
func TestStartShard_LegacyJoinDataDirChangeKeepsSnapshotsRemovable(t *testing.T) {
	const f = 2
	fid := uint64(f + 1)
	var w *legacyBootstrapLogDB
	c := newRemoveDataClusterWith(t, func(i int, cfg config.HostConfig) []HostOption {
		if i != f {
			return nil
		}
		wal, err := waldb.Open(cfg.WALDir)
		if err != nil {
			t.Fatalf("waldb.Open: %v", err)
		}
		w = &legacyBootstrapLogDB{LogDB: wal}
		return []HostOption{WithLogDB(w)}
	})
	dirA, dirB := t.TempDir(), t.TempDir()
	for i, h := range c.hosts {
		scfg := c.shardConfig(uint64(i + 1))
		members, join := c.members(), false
		if i == f {
			scfg.DataDir = dirA
			members, join = nil, true
		}
		if err := h.StartShard(members, join, hashingCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d] (join=%v): %v", i, join, err)
		}
	}
	all := []int{0, 1, 2}
	c.propose(all, "legacy-join", 20)
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

	// The replica now looks as if it joined before joins were recorded.
	w.hide()
	scfgB := c.shardConfig(fid)
	scfgB.DataDir = dirB
	restartErr := fh.StartShard(nil, true, hashingCreateFunc, scfgB)
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
		t.Fatalf("RemoveData succeeded but left the snapshots under DataDir A (stat err %v): %s",
			err, snapDirA)
	}
	if _, err := os.Stat(snapDirB); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused start created a snapshot directory under DataDir B (stat err %v): %s",
			err, snapDirB)
	}
}
