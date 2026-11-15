package engine

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestSnapshotPool_SaveStagingIsFencedByUnload holds a save at the point
// just before it creates its staging parent, unloads the node (Stop, then
// the write-lease fence UnloadNode and RemoveData rely on), and then lets
// the save continue. The save must be refused with ErrStopped and must
// create nothing under the node's snapshot directory: a parent created after
// the fence would reappear inside a replica directory RemoveData has just
// removed, or is removing.
func TestSnapshotPool_SaveStagingIsFencedByUnload(t *testing.T) {
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	dir := t.TempDir()
	node := NewNode(nil, nil, nil, cfg, newSnapshotTestSnapshotter(100), dir, nil)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hook := func(n *Node) {
		if n != node {
			return
		}
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	saveStageHook.Store(&hook)
	t.Cleanup(func() { saveStageHook.Store(nil) })

	if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: node, save: true}); err != nil {
		t.Fatalf("requestSnapshot: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the save never reached its staging step")
	}

	fenced := make(chan struct{})
	go func() {
		node.Stop()
		node.awaitWriteLeases()
		close(fenced)
	}()
	select {
	case <-fenced:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("the unload fence waited for a save that holds no lease yet")
	}
	close(release)

	res := awaitSnapshotResult(t, p, "snapshot")
	if !errors.Is(res.err, ErrStopped) {
		t.Fatalf("a save whose node unloaded before its staging step reported %v, want ErrStopped", res.err)
	}
	if snap, err := ldb.GetSnapshot(10, 1); err == nil {
		t.Fatalf("a stopped node's save recorded snapshot %d", snap.Index)
	}
	canonical := snapshotSaveDir(dir, 10, 1, 100)
	for _, path := range []string{
		filepath.Join(dir, "shard-10"),
		filepath.Dir(canonical),
		canonical,
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("a save refused at the unload fence created %s (stat err %v)", path, err)
		}
	}
}
