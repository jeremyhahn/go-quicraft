// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
)

// TestDataDirChangedError_IsErrDataDirChanged pins that StartShard's
// changed-DataDir refusal matches the exported ErrDataDirChanged sentinel,
// directly and through a %w wrap, so a caller need not read its message.
func TestDataDirChangedError_IsErrDataDirChanged(t *testing.T) {
	err := error(&dataDirChangedError{
		ShardID:   7,
		ReplicaID: 3,
		Recorded:  "/data/a",
		Requested: "/data/b",
	})
	if !errors.Is(err, ErrDataDirChanged) {
		t.Fatalf("errors.Is(%v, ErrDataDirChanged) = false, want true", err)
	}
	wrapped := fmt.Errorf("start shard: %w", err)
	if !errors.Is(wrapped, ErrDataDirChanged) {
		t.Fatalf("errors.Is(%v, ErrDataDirChanged) = false, want true", wrapped)
	}
	if errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("errors.Is(%v, ErrInvalidConfig) = true, want false", err)
	}
	var dc *dataDirChangedError
	if !errors.As(wrapped, &dc) || dc.Recorded != "/data/a" || dc.Requested != "/data/b" {
		t.Fatalf("errors.As(%v) = %+v, want the original fields", wrapped, dc)
	}
}

// closeBlockingSM blocks its Close until release is closed, signalling entered
// once Close has been reached.
type closeBlockingSM struct {
	mockStateMachine
	entered chan struct{}
	release chan struct{}
}

func (m *closeBlockingSM) Close(_ context.Context) error {
	close(m.entered)
	<-m.release
	return nil
}

// TestRemoveData_RefusedWhileStoppingStateMachineCloses: StopShard removes the
// nodeState from the host's node map before the state machine's Close
// returns. RemoveData must still refuse the replica in that window, keep its
// snapshot directory, and only succeed once the stop — including the
// OnNodeUnloaded event — has finished, so OnNodeDeleted never precedes it.
func TestRemoveData_RefusedWhileStoppingStateMachineCloses(t *testing.T) {
	var (
		mu     sync.Mutex
		events []string
	)
	record := func(name string) {
		mu.Lock()
		events = append(events, name)
		mu.Unlock()
	}
	cfg := testHostConfig(t)
	cfg.EventListener = &config.EventListener{
		OnNodeUnloaded: func(config.EventShardInfo) { record("OnNodeUnloaded") },
		OnNodeDeleted:  func(config.EventShardInfo) { record("OnNodeDeleted") },
	}
	h, err := NewHost(cfg, WithLogDB(memdb.New()), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer func() { _ = h.Close() }()

	sm := &closeBlockingSM{entered: make(chan struct{}), release: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(sm.release)
		}
	}()
	create := func(_, _ uint64) interface{} { return sm }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	snapDir, _, err := h.removeDataSnapshotDir(1, 1)
	if err != nil {
		t.Fatalf("removeDataSnapshotDir: %v", err)
	}
	marker := filepath.Join(snapDir, "snapshot-kept")
	if err := os.MkdirAll(marker, 0o750); err != nil {
		t.Fatalf("create snapshot directory: %v", err)
	}

	stopErr := make(chan error, 1)
	go func() { stopErr <- h.StopShard(1) }()

	select {
	case <-sm.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("StopShard did not reach the state machine's Close")
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, loaded := h.nodes.Load(uint64(1)); !loaded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stopping shard still in the node map while Close runs")
		}
		runtime.Gosched()
	}

	if err := h.RemoveData(1, 1); !errors.Is(err, ErrShardNotStopped) {
		t.Fatalf("RemoveData while the state machine is closing = %v, want ErrShardNotStopped", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("snapshot directory removed while the state machine was closing: %v", err)
	}

	released = true
	close(sm.release)
	select {
	case err := <-stopErr:
		if err != nil {
			t.Fatalf("StopShard: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("StopShard did not return after Close was released")
	}

	if err := h.RemoveData(1, 1); err != nil {
		t.Fatalf("RemoveData after the stop finished: %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("snapshot directory survived a successful RemoveData (stat err %v)", err)
	}
	mu.Lock()
	got := fmt.Sprint(events)
	mu.Unlock()
	if want := "[OnNodeUnloaded OnNodeDeleted]"; got != want {
		t.Fatalf("events = %s, want %s", got, want)
	}
}
