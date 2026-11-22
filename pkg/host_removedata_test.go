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

// TestRemoveData_RefusedWhileHostCloseClosesStateMachine: Host.Close removes
// each nodeState from the host's node map before that shard's state machine
// Close returns. A RemoveData admitted before Close set the closed flag must
// still refuse the replica in that window and keep its snapshot directory.
// The admitted call is emulated by clearing the closed flag around it: Close
// is past its admission barrier only after every state machine has closed,
// so the call is admitted and counted exactly as an earlier one would be.
func TestRemoveData_RefusedWhileHostCloseClosesStateMachine(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithLogDB(memdb.New()), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}

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

	closeErr := make(chan error, 1)
	go func() { closeErr <- h.Close() }()

	select {
	case <-sm.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("Host.Close did not reach the state machine's Close")
	}
	if _, loaded := h.nodes.Load(uint64(1)); loaded {
		t.Fatal("closing shard still in the node map while its state machine closes")
	}

	h.closed.Store(false)
	removeErr := h.RemoveData(1, 1)
	h.closed.Store(true)
	if !errors.Is(removeErr, ErrShardNotStopped) {
		t.Fatalf("RemoveData while Host.Close closes the state machine = %v, want ErrShardNotStopped", removeErr)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("snapshot directory removed while the state machine was closing: %v", err)
	}

	released = true
	close(sm.release)
	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatalf("Host.Close: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Host.Close did not return after the state machine's Close was released")
	}
	if _, closing := h.closingShards.Load(uint64(1)); closing {
		t.Fatal("closing marker left behind after Host.Close returned")
	}
}

// TestRemoveData_ReplacementStartKeepsClosingFence: while StopShard is still
// closing a replica's state machine, after its nodeState has left the node
// map, a replacement start of the same shard is refused transiently. Were it
// admitted, stopping it would overwrite and then clear the shard's closing
// marker, and RemoveData of the first replica would delete its snapshots while
// its Close still ran. The test drives that sequence and checks both: the
// start is refused, and RemoveData of the first replica stays refused.
func TestRemoveData_ReplacementStartKeepsClosingFence(t *testing.T) {
	cfg := testHostConfig(t)
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

	replacement := func(_, _ uint64) interface{} { return &mockStateMachine{} }
	startErr := h.StartShard(map[uint64]string{2: "127.0.0.1:5000"}, false,
		replacement, testShardConfig(1, 2))
	if startErr == nil {
		// The replacement was admitted: stop it, as the defect needs, so the
		// fence check below shows what that admission cost.
		if err := h.StopShard(1); err != nil {
			t.Fatalf("StopShard of the replacement: %v", err)
		}
	}
	if err := h.RemoveData(1, 1); !errors.Is(err, ErrShardNotStopped) {
		t.Errorf("RemoveData of the first replica after a replacement start = %v, want ErrShardNotStopped", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("snapshot directory removed while the first state machine was closing: %v", err)
	}
	if !errors.Is(startErr, ErrShardNotReady) {
		t.Errorf("replacement StartShard while the first Close runs = %v, want ErrShardNotReady", startErr)
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
	if err := h.StartShard(map[uint64]string{2: "127.0.0.1:5000"}, false,
		replacement, testShardConfig(1, 2)); err != nil {
		t.Fatalf("replacement StartShard after the stop finished: %v", err)
	}
}
