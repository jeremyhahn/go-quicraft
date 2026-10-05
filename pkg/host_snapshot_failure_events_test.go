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
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
)

// EventListener.OnShardFailed reports a shard whose failure latch has tripped:
// a replica that makes no further progress until an operator reloads it. The
// tests below drive a real snapshot pool on a real Host, so the event count is
// what an embedder observes, not what a single callback would emit in
// isolation.

var errInjectedSave = errors.New("quicraft_test: injected snapshot save failure")

// saveFailingSM is an ordinary state machine whose SaveSnapshot always fails.
type saveFailingSM struct {
	mockStateMachine
}

func (*saveFailingSM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return errInjectedSave
}

// TestSnapshotSaveFailure_DoesNotReportShardFailed: a failed save does not
// latch the shard — it keeps applying and serving, and the next save is simply
// attempted again — so reporting it through OnShardFailed tells an operator a
// healthy replica is finished. The failure stays observable to the caller that
// asked for the snapshot, as a *SnapshotFailedError.
func TestSnapshotSaveFailure_DoesNotReportShardFailed(t *testing.T) {
	var failed atomic.Int32
	cfg := testHostConfig(t)
	cfg.EventListener = &config.EventListener{
		OnShardFailed: func(config.ShardFailedInfo) { failed.Add(1) },
	}
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			h.Close()
		}
	}()

	create := func(uint64, uint64) interface{} { return &saveFailingSM{} }
	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, snapErr := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	var sfe *SnapshotFailedError
	if !errors.As(snapErr, &sfe) || sfe.Recovery {
		t.Fatalf("SyncRequestSnapshot = %v, want a save *SnapshotFailedError", snapErr)
	}
	if !errors.Is(snapErr, errInjectedSave) {
		t.Fatalf("SyncRequestSnapshot = %v, want the state machine's own error reachable", snapErr)
	}

	// The shard did not latch: an ordinary proposal still commits and applies.
	if _, proposeErr := h.SyncPropose(ctx, 1, []byte("after-failed-save")); proposeErr != nil {
		t.Fatalf("SyncPropose after a failed save: %v; the save failure latched the shard", proposeErr)
	}

	// Close stops every notification producer and then drains the listener
	// dispatcher, so every event the save produced has been delivered.
	h.Close()
	closed = true
	if n := failed.Load(); n != 0 {
		t.Fatalf("OnShardFailed fired %d times for a failed snapshot save, want 0: "+
			"the shard kept running", n)
	}
}

var errInjectedGetSnapshot = errors.New("quicraft_test: injected LogDB GetSnapshot failure")

// getSnapshotFailingLogDB fails GetSnapshot once armed, which makes the next
// snapshot recovery fail inside the pool exactly as a damaged record does.
type getSnapshotFailingLogDB struct {
	logdb.LogDB
	fail atomic.Bool
}

func (d *getSnapshotFailingLogDB) GetSnapshot(shardID, replicaID uint64) (logdb.Snapshot, error) {
	if d.fail.Load() {
		return logdb.Snapshot{}, errInjectedGetSnapshot
	}
	return d.LogDB.GetSnapshot(shardID, replicaID)
}

// TestSnapshotRecoveryPreRestoreFailure_RejectsWithoutFailingTheShard: a
// recovery that fails before the state machine's RecoverFromSnapshot is invoked
// (here, reading the snapshot record) has not touched the state machine. It
// does not trip the failure latch, so OnShardFailed does not fire; the
// recovery is reported through OnSnapshotRejected, exactly once, and the shard
// keeps committing and applying proposals. A recovery that fails once the
// restore has begun still latches (engine
// TestHandleRequest_PostRestoreErrorLatches).
func TestSnapshotRecoveryPreRestoreFailure_RejectsWithoutFailingTheShard(t *testing.T) {
	var (
		failed        atomic.Int32
		rejected      atomic.Int32
		firstRejected = make(chan config.SnapshotInfo, 1)
	)
	cfg := testHostConfig(t)
	cfg.EventListener = &config.EventListener{
		OnShardFailed: func(config.ShardFailedInfo) { failed.Add(1) },
		OnSnapshotRejected: func(info config.SnapshotInfo) {
			if rejected.Add(1) == 1 {
				firstRejected <- info
			}
		},
	}
	db := &getSnapshotFailingLogDB{LogDB: memdb.New()}
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			h.Close()
		}
	}()

	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)

	db.fail.Store(true)
	node := h.engine.GetNode(1)
	if node == nil {
		t.Fatal("engine has no node for shard 1")
	}
	if reqErr := h.engine.RequestSnapshotRecovery(1, 1, node); reqErr != nil {
		t.Fatalf("RequestSnapshotRecovery: %v", reqErr)
	}

	select {
	case info := <-firstRejected:
		if info.ShardID != 1 || info.ReplicaID != 1 {
			t.Errorf("OnSnapshotRejected named shard %d replica %d, want 1/1", info.ShardID, info.ReplicaID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnSnapshotRejected never fired for the rejected recovery")
	}
	if node.IsFailed() {
		t.Fatal("a recovery that failed before the restore tripped the shard's failure latch")
	}

	// The shard keeps serving.
	db.fail.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, proposeErr := h.SyncPropose(ctx, 1, []byte("after-rejected-recovery")); proposeErr != nil {
		t.Fatalf("SyncPropose after a rejected recovery: %v", proposeErr)
	}

	// Close waits out the pool worker and then drains the dispatcher, so a
	// late or duplicate report cannot arrive afterwards.
	h.Close()
	closed = true
	if n := failed.Load(); n != 0 {
		t.Fatalf("OnShardFailed fired %d times for a rejected recovery, want 0", n)
	}
	if n := rejected.Load(); n != 1 {
		t.Fatalf("OnSnapshotRejected fired %d times for one rejected recovery, want exactly 1", n)
	}
}

// savePanickingSM is an ordinary state machine whose SaveSnapshot panics.
type savePanickingSM struct {
	mockStateMachine
}

func (*savePanickingSM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	panic("quicraft_test: injected SaveSnapshot panic")
}

// TestSnapshotSavePanic_ResolvesRequesterAsFailedSave: a SaveSnapshot that
// panics is a failed save. The caller of SyncRequestSnapshot gets a save
// *SnapshotFailedError wrapping the *SnapshotSavePanicError at once — it used
// to wait out its context, because the panic path never resolved it — and the
// error does not claim a failed shard, because none happened: the save only
// read the state machine, and the shard keeps accepting proposals.
func TestSnapshotSavePanic_ResolvesRequesterAsFailedSave(t *testing.T) {
	var failed, rejected atomic.Int32
	cfg := testHostConfig(t)
	cfg.EventListener = &config.EventListener{
		OnShardFailed:      func(config.ShardFailedInfo) { failed.Add(1) },
		OnSnapshotRejected: func(config.SnapshotInfo) { rejected.Add(1) },
	}
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			h.Close()
		}
	}()

	create := func(uint64, uint64) interface{} { return &savePanickingSM{} }
	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, proposeErr := h.SyncPropose(ctx, 1, []byte("before-panicked-save")); proposeErr != nil {
		t.Fatalf("SyncPropose: %v", proposeErr)
	}

	// Promptly: far inside the caller's own deadline.
	snapCtx, snapCancel := context.WithTimeout(ctx, 5*time.Second)
	defer snapCancel()
	_, snapErr := h.SyncRequestSnapshot(snapCtx, 1, SnapshotOption{})
	var sfe *SnapshotFailedError
	if !errors.As(snapErr, &sfe) || sfe.Recovery {
		t.Fatalf("SyncRequestSnapshot = %v, want a save *SnapshotFailedError", snapErr)
	}
	var panicErr *SnapshotSavePanicError
	if !errors.As(snapErr, &panicErr) || panicErr.Value != "quicraft_test: injected SaveSnapshot panic" {
		t.Fatalf("SyncRequestSnapshot = %v, want it to wrap the *SnapshotSavePanicError", snapErr)
	}
	if errors.Is(snapErr, ErrShardFailed) {
		t.Fatalf("SyncRequestSnapshot = %v matches ErrShardFailed; the shard was not failed", snapErr)
	}

	// The shard did not latch: an ordinary proposal still commits and applies.
	if _, proposeErr := h.SyncPropose(ctx, 1, []byte("after-panicked-save")); proposeErr != nil {
		t.Fatalf("SyncPropose after a panicked save: %v; the save panic latched the shard", proposeErr)
	}

	h.Close()
	closed = true
	if n := rejected.Load(); n != 1 {
		t.Fatalf("OnSnapshotRejected fired %d times for a panicked save, want 1", n)
	}
	if n := failed.Load(); n != 0 {
		t.Fatalf("OnShardFailed fired %d times for a panicked save, want 0", n)
	}
}
