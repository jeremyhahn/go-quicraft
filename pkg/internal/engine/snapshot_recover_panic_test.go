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
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// halfRestoringSnapshotter stands in for a user state machine whose
// RecoverFromSnapshot overwrites part of its state and then panics: the
// condition a recovery panic leaves behind, and the reason it cannot be
// treated as a rejected request the shard can shrug off.
type halfRestoringSnapshotter struct {
	overwritten atomic.Bool
}

func (*halfRestoringSnapshotter) SaveSnapshot(context.Context, io.Writer, <-chan struct{}) error {
	panic("snapshotter save panic")
}

func (s *halfRestoringSnapshotter) RecoverFromSnapshot(context.Context, io.Reader, <-chan struct{}) error {
	s.overwritten.Store(true)
	panic("snapshotter recover panic after a partial restore")
}

func (*halfRestoringSnapshotter) LastApplied() uint64 { return 42 }

var _ Snapshotter = (*halfRestoringSnapshotter)(nil)

// newRecoverableSnapshotLogDB writes a valid plaintext snapshot artifact for
// shard 1 replica 1 at index under baseDir and returns a LogDB recording it,
// so a recovery request reaches RecoverFromSnapshot.
func newRecoverableSnapshotLogDB(t *testing.T, baseDir string, index, term uint64) *snapshotTestLogDB {
	t.Helper()
	dir := snapshotSaveDir(baseDir, 1, 1, index)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	payload := snapshotPayload(index, 64)
	if err := os.WriteFile(filepath.Join(dir, snapshotDataFile), payload, 0o600); err != nil {
		t.Fatalf("write snapshot data: %v", err)
	}
	if err := writeTestSnapshotMetadata(dir, index, term, 1, 1, 0, config.NoCompression); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	ldb := newSnapshotTestLogDB()
	if err := ldb.SaveSnapshot(1, 1, logdb.Snapshot{
		Index: index, Term: term, Filepath: dir, FileSize: uint64(len(payload)),
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	return ldb
}

// TestHandleRequest_RecoverPanicLatchesShard: a panic inside
// RecoverFromSnapshot leaves a state machine the engine cannot roll back, so
// resuming applies on it would apply committed entries onto an undefined base.
// It must fail the shard exactly like a recovery that returned an error: the
// latch trips, OnShardFailed fires once carrying an *SMPanicError, and no
// OnSnapshotRejected invites a retry against the damaged state machine.
func TestHandleRequest_RecoverPanicLatchesShard(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	stopC := make(chan struct{})
	defer close(stopC)
	ws := NewWorkSignal(1)
	cb := newTestCallback()

	baseDir := t.TempDir()
	ldb := newRecoverableSnapshotLogDB(t, baseDir, 100, 3)
	pool := newSnapshotPool(1, ldb, nil, cb, ws, stopC, nil)

	var shardFailed atomic.Int64
	listener := &config.EventListener{
		OnShardFailed: func(config.ShardFailedInfo) { shardFailed.Add(1) },
	}
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	snap := &halfRestoringSnapshotter{}
	node := NewNode(nil, nil, nil, cfg, snap, baseDir, listener)
	node.snapshotting.Store(true)
	pool.inFlight.Store(uint64(1), node)

	pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node})

	if !snap.overwritten.Load() {
		t.Fatal("fixture: RecoverFromSnapshot was never reached")
	}
	if !node.IsFailed() {
		t.Fatal("a panic inside RecoverFromSnapshot did not trip the failure latch: " +
			"applies resume on a half-restored state machine")
	}
	calls := cb.getShardFailedCalls()
	if len(calls) != 1 {
		t.Fatalf("engine OnShardFailed called %d times, want 1", len(calls))
	}
	var pe *SMPanicError
	if !errors.As(calls[0].err, &pe) || !errors.Is(calls[0].err, ErrShardFailed) {
		t.Fatalf("engine OnShardFailed err = %v, want an *SMPanicError", calls[0].err)
	}
	if n := shardFailed.Load(); n != 1 {
		t.Fatalf("EventListener.OnShardFailed fired %d times, want 1", n)
	}
	if n := cb.snapshotRejected.Load(); n != 0 {
		t.Fatalf("OnSnapshotRejected fired %d times for a latched recovery, want 0", n)
	}
	if node.IsRecovering() {
		t.Fatal("the recovering gate was left raised")
	}
	if node.snapshotting.Load() {
		t.Fatal("the snapshotting flag was left set")
	}
	if _, held := pool.inFlight.Load(uint64(1)); held {
		t.Fatal("the in-flight slot was left claimed")
	}
}

// TestHandleRequest_SavePanicDoesNotLatch: SaveSnapshot only reads the state
// machine, so a panic in it leaves nothing damaged. The shard keeps running
// and the request is reported rejected, so it can be retried.
func TestHandleRequest_SavePanicDoesNotLatch(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	stopC := make(chan struct{})
	defer close(stopC)
	cb := newTestCallback()
	pool := newSnapshotPool(1, nil, nil, cb, NewWorkSignal(1), stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, &halfRestoringSnapshotter{}, t.TempDir(), nil)
	node.snapshotting.Store(true)
	pool.inFlight.Store(uint64(1), node)

	pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node, save: true})

	if node.IsFailed() {
		t.Fatal("a save panic tripped the failure latch")
	}
	if n := cb.shardFailedCount.Load(); n != 0 {
		t.Fatalf("OnShardFailed called %d times for a save panic, want 0", n)
	}
	if n := cb.snapshotRejected.Load(); n != 1 {
		t.Fatalf("OnSnapshotRejected called %d times for a save panic, want 1", n)
	}
}

// restoreRecordingSnapshotter records whether RecoverFromSnapshot was reached
// and restores successfully when it is.
type restoreRecordingSnapshotter struct {
	restored atomic.Bool
}

func (*restoreRecordingSnapshotter) SaveSnapshot(context.Context, io.Writer, <-chan struct{}) error {
	return nil
}

func (s *restoreRecordingSnapshotter) RecoverFromSnapshot(context.Context, io.Reader, <-chan struct{}) error {
	s.restored.Store(true)
	return nil
}

func (*restoreRecordingSnapshotter) LastApplied() uint64 { return 0 }

var _ Snapshotter = (*restoreRecordingSnapshotter)(nil)

// panickingSnapshotLogDB panics on the panicOn-th GetSnapshot call, after
// running onPanic, and serves normally otherwise. panicOn == 0 never panics.
type panickingSnapshotLogDB struct {
	*snapshotTestLogDB
	panicOn atomic.Int64
	calls   atomic.Int64
	onPanic func()
}

func (db *panickingSnapshotLogDB) GetSnapshot(shardID, replicaID uint64) (logdb.Snapshot, error) {
	if n := db.calls.Add(1); n == db.panicOn.Load() {
		db.onPanic()
		panic("logdb panic before the restore")
	}
	return db.snapshotTestLogDB.GetSnapshot(shardID, replicaID)
}

// TestHandleRequest_PreRestorePanicDoesNotLatch: a recovery that panics in a
// step before the state machine's RecoverFromSnapshot is invoked — LogDB access
// before or under the recovering gate, or barrier resolution — has not touched
// the state machine. It must not trip the failure latch: the recovery is
// rejected, the recovering gate comes down, the slots are released, and the
// shard keeps serving, so the next recovery of the same snapshot succeeds.
func TestHandleRequest_PreRestorePanicDoesNotLatch(t *testing.T) {
	const index, term = uint64(100), uint64(3)
	cases := []struct {
		name string
		// arm makes the next recovery panic before the restore and returns
		// disarm; gateUp reports whether the gate was raised at the panic.
		arm        func(pool *snapshotPool, db *panickingSnapshotLogDB, node *Node, gateUp *atomic.Bool) (disarm func())
		wantGateUp bool
	}{
		{
			name: "LogDB read before the recovering gate",
			arm: func(_ *snapshotPool, db *panickingSnapshotLogDB, node *Node, gateUp *atomic.Bool) func() {
				db.onPanic = func() { gateUp.Store(node.IsRecovering()) }
				db.calls.Store(0)
				db.panicOn.Store(1)
				return func() { db.panicOn.Store(0) }
			},
			wantGateUp: false,
		},
		{
			name: "LogDB read under the recovering gate",
			arm: func(_ *snapshotPool, db *panickingSnapshotLogDB, node *Node, gateUp *atomic.Bool) func() {
				db.onPanic = func() { gateUp.Store(node.IsRecovering()) }
				db.calls.Store(0)
				db.panicOn.Store(2)
				return func() { db.panicOn.Store(0) }
			},
			wantGateUp: true,
		},
		{
			name: "barrier resolution",
			arm: func(pool *snapshotPool, _ *panickingSnapshotLogDB, node *Node, gateUp *atomic.Bool) func() {
				pool.shardBarriers = func(uint64) SnapshotBarrier {
					gateUp.Store(node.IsRecovering())
					panic("barrier resolver panic before the restore")
				}
				return func() { pool.shardBarriers = nil }
			},
			wantGateUp: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, restore := captureLogOutput(t)
			defer restore()

			stopC := make(chan struct{})
			defer close(stopC)
			cb := newTestCallback()
			baseDir := t.TempDir()
			db := &panickingSnapshotLogDB{snapshotTestLogDB: newRecoverableSnapshotLogDB(t, baseDir, index, term)}
			pool := newSnapshotPool(1, db, nil, cb, NewWorkSignal(1), stopC, nil)

			cfg := config.Config{ShardID: 1, ReplicaID: 1}
			cfg.SetDefaults()
			snap := &restoreRecordingSnapshotter{}
			node := NewNode(nil, nil, nil, cfg, snap, baseDir, nil)
			var gateUp atomic.Bool
			disarm := tc.arm(pool, db, node, &gateUp)

			node.snapshotting.Store(true)
			pool.inFlight.Store(uint64(1), node)
			pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node})

			if gateUp.Load() != tc.wantGateUp {
				t.Fatalf("fixture: recovering gate at the panic = %v, want %v", gateUp.Load(), tc.wantGateUp)
			}
			if snap.restored.Load() {
				t.Fatal("fixture: RecoverFromSnapshot was reached")
			}
			if node.IsFailed() {
				t.Fatal("a panic before the restore tripped the failure latch on an untouched state machine")
			}
			if n := cb.shardFailedCount.Load(); n != 0 {
				t.Fatalf("OnShardFailed called %d times, want 0", n)
			}
			if n := cb.snapshotRejected.Load(); n != 1 {
				t.Fatalf("OnSnapshotRejected called %d times, want 1", n)
			}
			cb.mu.Lock()
			cause := cb.rejectedCauses[0]
			cb.mu.Unlock()
			var panicErr *SnapshotRecoveryPanicError
			if !errors.As(cause, &panicErr) {
				t.Fatalf("OnSnapshotRejected cause = %T (%v), want *SnapshotRecoveryPanicError", cause, cause)
			}
			if node.IsRecovering() {
				t.Fatal("the recovering gate was left raised")
			}
			if node.snapshotting.Load() {
				t.Fatal("the snapshotting flag was left set")
			}
			if _, held := pool.inFlight.Load(uint64(1)); held {
				t.Fatal("the in-flight slot was left claimed")
			}

			// The shard keeps serving: the next recovery restores the snapshot.
			disarm()
			node.snapshotting.Store(true)
			pool.inFlight.Store(uint64(1), node)
			pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node})
			if !snap.restored.Load() || node.IsFailed() {
				t.Fatalf("retry after the rejected recovery: restored=%v failed=%v, want restored and not failed",
					snap.restored.Load(), node.IsFailed())
			}
			if got := node.lastApplied.Load(); got != index {
				t.Fatalf("lastApplied after the retried recovery = %d, want %d", got, index)
			}
		})
	}
}
