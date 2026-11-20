// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
)

// blockRemoveAllFS blocks the RemoveAll of the armed directory until
// release is closed, signalling entered when it starts: it holds RemoveData's
// filesystem step open for as long as the test needs.
type blockRemoveAllFS struct {
	osSnapshotFS
	mu      sync.Mutex
	target  string
	entered chan struct{}
	release chan struct{}
}

func (f *blockRemoveAllFS) arm(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.target = filepath.Clean(path)
	f.entered = make(chan struct{})
	f.release = make(chan struct{})
}

func (f *blockRemoveAllFS) RemoveAll(path string) error {
	f.mu.Lock()
	hit := f.target != "" && filepath.Clean(path) == f.target
	var entered, release chan struct{}
	if hit {
		f.target = ""
		entered, release = f.entered, f.release
	}
	f.mu.Unlock()
	if hit {
		close(entered)
		<-release
	}
	return f.osSnapshotFS.RemoveAll(path)
}

// TestHost_CloseJoinsRemoveData: Close must not release NodeHostDir while a
// RemoveData is still deleting snapshots and clearing its intent under it,
// and a RemoveData issued once Close has begun is refused with ErrClosed.
// Close returns only after the held removal finished, so the directory a
// new Host would open sees no further mutation.
func TestHost_CloseJoinsRemoveData(t *testing.T) {
	fsys := &blockRemoveAllFS{}
	c := newRemoveDataCluster(t, withSnapshotFS(fsys))
	dataDirs := make([]string, len(c.hosts))
	for i, h := range c.hosts {
		dataDirs[i] = t.TempDir()
		scfg := c.shardConfig(uint64(i + 1))
		scfg.DataDir = dataDirs[i]
		if err := h.StartShard(c.members(), false, hashingCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	const f = 1
	fid := uint64(f + 1)
	fh := c.hosts[f]
	snapDir := engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(c.cfgs[f].NodeHostDir, dataDirs[f], removeDataShard), removeDataShard, fid)
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}

	fsys.arm(snapDir)
	removeErr := make(chan error, 1)
	go func() { removeErr <- fh.RemoveData(removeDataShard, fid) }()
	select {
	case <-fsys.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("RemoveData never reached the snapshot directory removal")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- fh.Close() }()
	select {
	case err := <-closeDone:
		close(fsys.release)
		t.Fatalf("Close returned (err %v) while RemoveData was still removing %s", err, snapDir)
	case <-time.After(300 * time.Millisecond):
	}

	if err := fh.RemoveData(removeDataShard, fid); !errors.Is(err, ErrClosed) {
		t.Errorf("RemoveData during Close: err = %v, want %v", err, ErrClosed)
	}

	close(fsys.release)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Logf("Close: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Close did not return after the held RemoveData was released")
	}
	// Close has returned: the removal must already be complete, so nothing
	// mutates the directory after the host released it.
	select {
	case err := <-removeErr:
		if err != nil {
			t.Fatalf("held RemoveData: %v", err)
		}
	case <-time.After(10 * time.Second):
		// Close already waited for the held removal (checked above); the
		// goroutine sends its result only after RemoveData returns, so the
		// receive is bounded rather than immediate.
		t.Fatal("held RemoveData never delivered its result after Close returned")
	}
	intent := removeDataIntentFile(c.cfgs[f].NodeHostDir, removeDataShard, fid)
	if _, err := os.Stat(intent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the removal intent survived the joined RemoveData (stat err %v): %s", err, intent)
	}
	if _, err := os.Stat(snapDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the snapshot directory survived the joined RemoveData (stat err %v): %s", err, snapDir)
	}
}
