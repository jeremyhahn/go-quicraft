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
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// erroringSnapshotLogDB fails the failOn-th GetSnapshot call and serves
// normally otherwise. failOn == 0 never fails.
type erroringSnapshotLogDB struct {
	*snapshotTestLogDB
	failOn atomic.Int64
	calls  atomic.Int64
}

var errSnapshotRecordRead = errors.New("logdb: snapshot record read failed")

func (db *erroringSnapshotLogDB) GetSnapshot(shardID, replicaID uint64) (logdb.Snapshot, error) {
	if n := db.calls.Add(1); n == db.failOn.Load() {
		return logdb.Snapshot{}, errSnapshotRecordRead
	}
	return db.snapshotTestLogDB.GetSnapshot(shardID, replicaID)
}

// TestHandleRequest_PreRestoreErrorDoesNotLatch: a recovery that fails with an
// error in a step before the state machine's RecoverFromSnapshot is invoked has
// not touched the state machine, exactly like one that panics there
// (TestHandleRequest_PreRestorePanicDoesNotLatch). It is settled the same way:
// the failure latch stays open, OnShardFailed and OnSnapshotCompleted do not
// fire, OnSnapshotRejected fires once, the recovering gate comes down, the
// slots are released, and the next recovery of the snapshot succeeds.
func TestHandleRequest_PreRestoreErrorDoesNotLatch(t *testing.T) {
	const index, term = uint64(100), uint64(3)
	dataPath := func(baseDir string) string {
		return filepath.Join(snapshotSaveDir(baseDir, 1, 1, index), snapshotDataFile)
	}
	cases := []struct {
		name string
		// arm makes the next recovery fail before the restore and returns
		// disarm, which makes the snapshot recoverable again.
		arm     func(t *testing.T, pool *snapshotPool, db *erroringSnapshotLogDB, baseDir string) (disarm func())
		wantErr error
	}{
		{
			name: "LogDB read under the recovering gate",
			arm: func(_ *testing.T, _ *snapshotPool, db *erroringSnapshotLogDB, _ string) func() {
				db.calls.Store(0)
				db.failOn.Store(2) // the first read is the pre-gate bounds probe
				return func() { db.failOn.Store(0) }
			},
			wantErr: errSnapshotRecordRead,
		},
		{
			name: "artifact verification",
			arm: func(t *testing.T, _ *snapshotPool, _ *erroringSnapshotLogDB, baseDir string) func() {
				path := dataPath(baseDir)
				good, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read artifact: %v", err)
				}
				bad := append([]byte(nil), good...)
				bad[len(bad)/2] ^= 0xFF
				if err := os.WriteFile(path, bad, 0o600); err != nil {
					t.Fatalf("corrupt artifact: %v", err)
				}
				return func() {
					if err := os.WriteFile(path, good, 0o600); err != nil {
						t.Fatalf("restore artifact: %v", err)
					}
				}
			},
			wantErr: ErrSnapshotCorrupt,
		},
		{
			name: "barrier resolution",
			arm: func(_ *testing.T, pool *snapshotPool, _ *erroringSnapshotLogDB, _ string) func() {
				pool.shardBarriers = func(uint64) SnapshotBarrier { return nil }
				return func() { pool.shardBarriers = nil }
			},
			wantErr: ErrShardBarrierUnresolved,
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
			db := &erroringSnapshotLogDB{snapshotTestLogDB: newRecoverableSnapshotLogDB(t, baseDir, index, term)}
			pool := newSnapshotPool(1, db, nil, cb, NewWorkSignal(1), stopC, nil)

			cfg := config.Config{ShardID: 1, ReplicaID: 1}
			cfg.SetDefaults()
			snap := &restoreRecordingSnapshotter{}
			node := NewNode(nil, nil, nil, cfg, snap, baseDir, nil)
			disarm := tc.arm(t, pool, db, baseDir)

			node.snapshotting.Store(true)
			pool.inFlight.Store(uint64(1), node)
			pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node})

			res := <-pool.CompletedC()
			if !errors.Is(res.err, tc.wantErr) {
				t.Fatalf("fixture: recovery error = %v, want %v", res.err, tc.wantErr)
			}
			if snap.restored.Load() {
				t.Fatal("fixture: RecoverFromSnapshot was reached")
			}
			if node.IsFailed() {
				t.Fatal("an error before the restore tripped the failure latch on an untouched state machine")
			}
			if n := cb.shardFailedCount.Load(); n != 0 {
				t.Fatalf("OnShardFailed called %d times, want 0", n)
			}
			if n := cb.snapshotCallCount.Load(); n != 0 {
				t.Fatalf("OnSnapshotCompleted called %d times for a rejected recovery, want 0", n)
			}
			if n := cb.snapshotRejected.Load(); n != 1 {
				t.Fatalf("OnSnapshotRejected called %d times, want 1", n)
			}
			cb.mu.Lock()
			cause := cb.rejectedCauses[0]
			cb.mu.Unlock()
			if !errors.Is(cause, tc.wantErr) {
				t.Fatalf("OnSnapshotRejected cause = %v, want %v", cause, tc.wantErr)
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
			if res := <-pool.CompletedC(); res.err != nil {
				t.Fatalf("retried recovery error = %v, want nil", res.err)
			}
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

// TestHandleRequest_PostRestoreErrorLatches is the boundary's other side: an
// error returned once RecoverFromSnapshot has been invoked may leave a
// half-restored state machine, so it still fails the shard and is not offered
// for retry.
func TestHandleRequest_PostRestoreErrorLatches(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	stopC := make(chan struct{})
	defer close(stopC)
	cb := newTestCallback()
	baseDir := t.TempDir()
	pool := newSnapshotPool(1, newRecoverableSnapshotLogDB(t, baseDir, 100, 3), nil, cb, NewWorkSignal(1), stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	snap := &failingRecoverSnapshotter{
		snapshotTestSnapshotter: newSnapshotTestSnapshotter(0),
		err:                     errors.New("user state machine refused the snapshot"),
	}
	node := NewNode(nil, nil, nil, cfg, snap, baseDir, nil)
	node.snapshotting.Store(true)
	pool.inFlight.Store(uint64(1), node)
	pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node})

	if !node.IsFailed() {
		t.Fatal("an error from RecoverFromSnapshot left the shard alive")
	}
	if n := cb.shardFailedCount.Load(); n != 1 {
		t.Fatalf("OnShardFailed called %d times, want 1", n)
	}
	if n := cb.snapshotRejected.Load(); n != 0 {
		t.Fatalf("OnSnapshotRejected called %d times for a failed restore, want 0", n)
	}
}
