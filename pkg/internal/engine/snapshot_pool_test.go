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
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// snapshotTestSnapshotter is a minimal Snapshotter implementation for
// unit testing the snapshot pool. It saves/recovers a single uint64
// value representing the state.
type snapshotTestSnapshotter struct {
	lastApplied atomic.Uint64
	state       atomic.Uint64
}

func newSnapshotTestSnapshotter(lastApplied uint64) *snapshotTestSnapshotter {
	s := &snapshotTestSnapshotter{}
	s.lastApplied.Store(lastApplied)
	s.state.Store(lastApplied)
	return s
}

func (s *snapshotTestSnapshotter) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], s.state.Load())
	_, err := w.Write(buf[:])
	return err
}

func (s *snapshotTestSnapshotter) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	val := binary.LittleEndian.Uint64(buf[:])
	s.state.Store(val)
	s.lastApplied.Store(val)
	return nil
}

func (s *snapshotTestSnapshotter) LastApplied() uint64 {
	return s.lastApplied.Load()
}

var _ Snapshotter = (*snapshotTestSnapshotter)(nil)

// snapshotTestLogDB is a LogDB implementation that stores snapshot
// metadata per shard in memory. Used by snapshot pool tests.
type snapshotTestLogDB struct {
	mu        sync.Mutex
	snapshots map[uint64]logdb.Snapshot // keyed by shardID
	removedTo uint64                    // last compaction index passed to RemoveEntriesTo
}

func newSnapshotTestLogDB() *snapshotTestLogDB {
	return &snapshotTestLogDB{
		snapshots: make(map[uint64]logdb.Snapshot),
	}
}

func (db *snapshotTestLogDB) Name() string { return "snapshot-test" }

func (db *snapshotTestLogDB) SaveState(_ []logdb.Update) error { return nil }

func (db *snapshotTestLogDB) IterateEntries(entries []logdb.Entry, size uint64, _, _, _, _ uint64, _ uint64) ([]logdb.Entry, uint64, error) {
	return entries, size, nil
}

func (db *snapshotTestLogDB) ReadState(_, _ uint64) (logdb.State, logdb.Membership, error) {
	return logdb.State{}, logdb.Membership{}, nil
}

func (db *snapshotTestLogDB) EntryRange(_, _ uint64) (uint64, uint64, error) { return 0, 0, nil }

func (db *snapshotTestLogDB) RemoveEntriesTo(_, _ uint64, index uint64) error {
	db.mu.Lock()
	db.removedTo = index
	db.mu.Unlock()
	return nil
}

func (db *snapshotTestLogDB) Compact(_, _ uint64) error { return nil }

func (db *snapshotTestLogDB) SaveSnapshot(shardID, _ uint64, snap logdb.Snapshot) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.snapshots[shardID] = snap
	return nil
}

func (db *snapshotTestLogDB) GetSnapshot(shardID, _ uint64) (logdb.Snapshot, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	snap, ok := db.snapshots[shardID]
	if !ok {
		return logdb.Snapshot{}, nil
	}
	return snap, nil
}

func (db *snapshotTestLogDB) RemoveNodeData(_, _ uint64) error { return nil }

func (db *snapshotTestLogDB) ListNodeInfo() ([]logdb.NodeInfo, error) { return nil, nil }

func (db *snapshotTestLogDB) SaveBootstrap(_, _ uint64, _ logdb.Bootstrap) error { return nil }

func (db *snapshotTestLogDB) GetBootstrap(_, _ uint64) (logdb.Bootstrap, bool, error) {
	return logdb.Bootstrap{}, false, nil
}

func (db *snapshotTestLogDB) Close() error { return nil }

var _ logdb.LogDB = (*snapshotTestLogDB)(nil)

// epochAwareTestLogDB extends snapshotTestLogDB with EpochAwareLogDB support
// for testing the purgeOldEpochs WAL epoch floor logic.
type epochAwareTestLogDB struct {
	snapshotTestLogDB
	minLiveEpoch    uint64
	minLiveEpochErr error
}

func newEpochAwareTestLogDB(minEpoch uint64) *epochAwareTestLogDB {
	return &epochAwareTestLogDB{
		snapshotTestLogDB: snapshotTestLogDB{
			snapshots: make(map[uint64]logdb.Snapshot),
		},
		minLiveEpoch: minEpoch,
	}
}

// MinLiveEpoch implements logdb.EpochAwareLogDB.
func (db *epochAwareTestLogDB) MinLiveEpoch() (uint64, error) {
	return db.minLiveEpoch, db.minLiveEpochErr
}

var _ logdb.LogDB = (*epochAwareTestLogDB)(nil)
var _ logdb.EpochAwareLogDB = (*epochAwareTestLogDB)(nil)

// ---------------------------------------------------------------------------
// Pool constructor tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_NewWithMinimumWorkers(t *testing.T) {
	stopC := make(chan struct{})
	p := newSnapshotPool(0, nil, nil, nil, nil, stopC, nil)
	if p.workers < 2 {
		t.Fatalf("expected at least 2 workers, got %d", p.workers)
	}
}

func TestSnapshotPool_NewWithValidWorkers(t *testing.T) {
	stopC := make(chan struct{})
	p := newSnapshotPool(4, nil, nil, nil, nil, stopC, nil)
	if p.workers != 4 {
		t.Fatalf("expected 4 workers, got %d", p.workers)
	}
}

func TestSnapshotPool_StartAndStop(t *testing.T) {
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// Workers should be running.
	close(stopC)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Expected: all workers stopped.
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot pool workers did not stop within timeout")
	}
}

// ---------------------------------------------------------------------------
// Save snapshot tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_SaveSnapshot_Success(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(100)
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.shardID != 10 {
			t.Fatalf("result.shardID = %d, want 10", result.shardID)
		}
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
		if result.index != 100 {
			t.Fatalf("result.index = %d, want 100", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}

	// Verify snapshot data file was created.
	dataPath := filepath.Join(
		snapshotSaveDir(tmpDir, 10, 1, 100),
		snapshotDataFile,
	)
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Fatalf("snapshot data file not created: %s", dataPath)
	}

	// Verify metadata file was created.
	metaPath := filepath.Join(
		snapshotSaveDir(tmpDir, 10, 1, 100),
		snapshotMetadataFile,
	)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Fatalf("snapshot metadata file not created: %s", metaPath)
	}

	// Verify LogDB was updated.
	snap, err := ldb.GetSnapshot(10, 1)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if snap.Index != 100 {
		t.Fatalf("logdb snapshot index = %d, want 100", snap.Index)
	}

	// Verify node tracking was updated.
	if node.LastSnapshotIndex() != 100 {
		t.Fatalf("node.LastSnapshotIndex() = %d, want 100", node.LastSnapshotIndex())
	}
}

func TestSnapshotPool_SaveSnapshot_NilSnapshotter(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, nil, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 10, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err == nil {
			t.Fatal("expected error for nil snapshotter")
		}
		var snapErr *SnapshotError
		if !errors.As(result.err, &snapErr) {
			t.Fatalf("expected SnapshotError, got %T: %v", result.err, result.err)
		}
		if !snapErr.Save {
			t.Fatal("expected Save=true in SnapshotError")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}
}

func TestSnapshotPool_SaveSnapshot_ExceedsMaxSnapshotSize(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// The test snapshotter writes 8 bytes. Set MaxSnapshotSize to 4 to
	// trigger the too-large check.
	ss := newSnapshotTestSnapshotter(100)
	cfg := config.Config{ShardID: 10, ReplicaID: 1, MaxSnapshotSize: 4}
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err == nil {
			t.Fatal("expected SnapshotTooLargeError, got nil")
		}
		if !errors.Is(result.err, ErrSnapshotTooLarge) {
			t.Fatalf("expected ErrSnapshotTooLarge, got: %v", result.err)
		}
		var tooLarge *SnapshotTooLargeError
		if !errors.As(result.err, &tooLarge) {
			t.Fatalf("expected *SnapshotTooLargeError, got %T: %v", result.err, result.err)
		}
		if tooLarge.Size != 8 {
			t.Fatalf("expected Size=8, got %d", tooLarge.Size)
		}
		if tooLarge.Limit != 4 {
			t.Fatalf("expected Limit=4, got %d", tooLarge.Limit)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}

	// Verify the oversized snapshot directory was cleaned up.
	saveDir := snapshotSaveDir(tmpDir, 10, 1, 100)
	if _, err := os.Stat(saveDir); !os.IsNotExist(err) {
		t.Fatalf("expected oversized snapshot dir to be removed: %s", saveDir)
	}
}

func TestSnapshotPool_SaveSnapshot_MaxSnapshotSizeZeroDisablesCheck(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// MaxSnapshotSize=0 means the check is disabled (default after SetDefaults
	// sets it to 4GB, but 0 here means not set).
	ss := newSnapshotTestSnapshotter(100)
	cfg := config.Config{ShardID: 10, ReplicaID: 1, MaxSnapshotSize: 0}
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
		if result.index != 100 {
			t.Fatalf("result.index = %d, want 100", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}
}

func TestSnapshotPool_SaveSnapshot_ZeroLastApplied(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, nil, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(0) // zero means nothing to snapshot
	cfg := config.Config{ShardID: 10, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, ss, t.TempDir(), nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
		if result.index != 0 {
			t.Fatalf("result.index = %d, want 0", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}
}

// ---------------------------------------------------------------------------
// Recover snapshot tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_RecoverSnapshot_Success(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// First, save a snapshot to create the data on disk.
	saveSS := newSnapshotTestSnapshotter(200)
	cfg := config.Config{ShardID: 5, ReplicaID: 2, CompactionOverhead: 50}
	saveNode := NewNode(nil, nil, nil, cfg, 100, saveSS, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 5, replicaID: 2, node: saveNode, save: true,
	})
	if err != nil {
		t.Fatalf("save request failed: %v", err)
	}
	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("save failed: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("save timeout")
	}

	// Now recover into a fresh snapshotter.
	recoverSS := newSnapshotTestSnapshotter(0)
	recoverNode := NewNode(nil, nil, nil, cfg, 100, recoverSS, tmpDir, nil)

	err = p.requestSnapshot(snapshotRequest{
		shardID: 5, replicaID: 2, node: recoverNode, save: false,
	})
	if err != nil {
		t.Fatalf("recover request failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("recover failed: %v", result.err)
		}
		if result.shardID != 5 {
			t.Fatalf("result.shardID = %d, want 5", result.shardID)
		}
		if result.replicaID != 2 {
			t.Fatalf("result.replicaID = %d, want 2", result.replicaID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive recover result within timeout")
	}

	// Verify the state was recovered.
	if recoverSS.state.Load() != 200 {
		t.Fatalf("recovered state = %d, want 200", recoverSS.state.Load())
	}
	if recoverNode.lastApplied.Load() != 200 {
		t.Fatalf("node.lastApplied = %d, want 200", recoverNode.lastApplied.Load())
	}
	if recoverNode.LastSnapshotIndex() != 200 {
		t.Fatalf("node.LastSnapshotIndex = %d, want 200", recoverNode.LastSnapshotIndex())
	}
}

func TestSnapshotPool_RecoverSnapshot_NoSnapshot(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(0)
	cfg := config.Config{ShardID: 5, ReplicaID: 2}
	node := NewNode(nil, nil, nil, cfg, 100, ss, t.TempDir(), nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 5, replicaID: 2, node: node, save: false,
	})
	if err != nil {
		t.Fatalf("recover request failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive recover result within timeout")
	}
}

func TestSnapshotPool_RecoverSnapshot_NilSnapshotter(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 5, ReplicaID: 2}
	node := NewNode(nil, nil, nil, cfg, 100, nil, t.TempDir(), nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 5, replicaID: 2, node: node, save: false,
	})
	if err != nil {
		t.Fatalf("recover request failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("unexpected error for nil snapshotter: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive recover result within timeout")
	}
}

func TestSnapshotPool_RecoverSnapshot_NilLogDB(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)

	// nil logdb means nothing to recover from.
	p := newSnapshotPool(2, nil, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(0)
	cfg := config.Config{ShardID: 5, ReplicaID: 2}
	node := NewNode(nil, nil, nil, cfg, 100, ss, t.TempDir(), nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 5, replicaID: 2, node: node, save: false,
	})
	if err != nil {
		t.Fatalf("recover request failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive recover result within timeout")
	}
}

// ---------------------------------------------------------------------------
// Pool dedup and concurrency tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_PerShardDedup(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(1, nil, nil, nil, nil, stopC, nil)

	// Do NOT start workers yet so the request stays in-flight.
	cfg := config.Config{ShardID: 10, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 10, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	// Second request for same shard should be rejected.
	err = p.requestSnapshot(snapshotRequest{
		shardID: 10, replicaID: 1, node: node, save: true,
	})
	if err == nil {
		t.Fatal("expected duplicate error for second request")
	}
	var dupErr *SnapshotDuplicateError
	if !errors.As(err, &dupErr) {
		t.Fatalf("expected SnapshotDuplicateError, got %T", err)
	}
	if dupErr.ShardID != 10 {
		t.Fatalf("dupErr.ShardID = %d, want 10", dupErr.ShardID)
	}
}

func TestSnapshotPool_DedupClearedAfterCompletion(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(50)
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 10}
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	// First request.
	err := p.requestSnapshot(snapshotRequest{
		shardID: 10, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	select {
	case <-p.CompletedC():
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive first result within timeout")
	}

	// Update lastApplied so the second save creates a new snapshot.
	ss.lastApplied.Store(60)
	ss.state.Store(60)

	// After completion, in-flight should be cleared.
	err = p.requestSnapshot(snapshotRequest{
		shardID: 10, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("second request should succeed after first completes: %v", err)
	}

	select {
	case <-p.CompletedC():
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive second result within timeout")
	}
}

func TestSnapshotPool_ConcurrentRequests_DifferentShards(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(8, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	const numShards = 8
	for i := range numShards {
		ss := newSnapshotTestSnapshotter(uint64(100 + i))
		cfg := config.Config{ShardID: uint64(i + 1), ReplicaID: 1, CompactionOverhead: 50}
		node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)
		err := p.requestSnapshot(snapshotRequest{
			shardID:   uint64(i + 1),
			replicaID: 1,
			node:      node,
			save:      true,
		})
		if err != nil {
			t.Fatalf("request for shard %d failed: %v", i+1, err)
		}
	}

	results := make(map[uint64]bool)
	for range numShards {
		select {
		case result := <-p.CompletedC():
			if result.err != nil {
				t.Fatalf("shard %d failed: %v", result.shardID, result.err)
			}
			results[result.shardID] = true
		case <-time.After(5 * time.Second):
			t.Fatal("did not receive all results within timeout")
		}
	}

	if len(results) != numShards {
		t.Fatalf("expected %d results, got %d", numShards, len(results))
	}
}

func TestSnapshotPool_CompletedC_ReturnsChannel(t *testing.T) {
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, nil, nil, nil, stopC, nil)
	ch := p.CompletedC()
	if ch == nil {
		t.Fatal("CompletedC should not return nil")
	}
}

func TestSnapshotPool_RequestFull_ReturnsError(t *testing.T) {
	stopC := make(chan struct{})
	p := newSnapshotPool(1, nil, nil, nil, nil, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 1, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("first request should succeed: %v", err)
	}

	// Second request for different shard should fail because requestC
	// is full (size 1) and no workers are draining.
	err = p.requestSnapshot(snapshotRequest{
		shardID: 2, replicaID: 1, node: node, save: true,
	})
	if err == nil {
		t.Fatal("expected error when request channel is full")
	}
	var busyErr *SnapshotPoolBusyError
	if !errors.As(err, &busyErr) {
		t.Fatalf("expected SnapshotPoolBusyError, got %T: %v", err, err)
	}
	if busyErr.ShardID != 2 {
		t.Fatalf("busyErr.ShardID = %d, want 2", busyErr.ShardID)
	}
}

// ---------------------------------------------------------------------------
// SetCallback tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_SetCallback(t *testing.T) {
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, nil, nil, nil, stopC, nil)

	if p.callback != nil {
		t.Fatal("expected nil callback initially")
	}

	cb := newTestCallback()
	p.setCallback(cb)

	if p.callback == nil {
		t.Fatal("expected non-nil callback after setCallback")
	}
}

func TestSnapshotPool_CallbackFiredOnSave(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	cb := newTestCallback()
	p := newSnapshotPool(2, ldb, nil, cb, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(42)
	cfg := config.Config{ShardID: 7, ReplicaID: 1, CompactionOverhead: 10}
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 7, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case <-p.CompletedC():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// The callback is fired BEFORE sending to CompletedC, so by the
	// time we receive from CompletedC the callback has already run.
	calls := cb.getSnapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 snapshot callback, got %d", len(calls))
	}
	if calls[0].shardID != 7 {
		t.Fatalf("callback shardID = %d, want 7", calls[0].shardID)
	}
	if calls[0].index != 42 {
		t.Fatalf("callback index = %d, want 42", calls[0].index)
	}
	if calls[0].err != nil {
		t.Fatalf("callback err = %v, want nil", calls[0].err)
	}
}

// ---------------------------------------------------------------------------
// Garbage collection tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_GarbageCollection(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 1, ReplicaID: 1, CompactionOverhead: 10}

	// Create 5 snapshots sequentially. Only 3 should be retained.
	for i := uint64(1); i <= 5; i++ {
		ss := newSnapshotTestSnapshotter(i * 100)
		node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)
		err := p.requestSnapshot(snapshotRequest{
			shardID: 1, replicaID: 1, node: node, save: true,
		})
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		select {
		case result := <-p.CompletedC():
			if result.err != nil {
				t.Fatalf("snapshot %d failed: %v", i, result.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for snapshot %d", i)
		}
	}

	dirs, err := listSnapshotDirs(tmpDir, 1, 1)
	if err != nil {
		t.Fatalf("listSnapshotDirs failed: %v", err)
	}
	if len(dirs) != maxSnapshotsRetained {
		t.Fatalf("expected %d snapshots retained, got %d", maxSnapshotsRetained, len(dirs))
	}

	// Verify the retained snapshots are the most recent ones.
	expectedIndices := []uint64{300, 400, 500}
	for i, d := range dirs {
		if d.index != expectedIndices[i] {
			t.Fatalf("dir[%d].index = %d, want %d", i, d.index, expectedIndices[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Metadata and directory helpers tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_MetadataWritten(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(42)
	cfg := config.Config{ShardID: 7, ReplicaID: 3, CompactionOverhead: 10}
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 7, replicaID: 3, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Read and verify metadata.
	metaPath := filepath.Join(
		snapshotSaveDir(tmpDir, 7, 3, 42),
		snapshotMetadataFile,
	)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata failed: %v", err)
	}
	if len(data) != snapshotMetaSize {
		t.Fatalf("metadata size = %d, want %d", len(data), snapshotMetaSize)
	}

	index := binary.LittleEndian.Uint64(data[0:])
	if index != 42 {
		t.Fatalf("metadata index = %d, want 42", index)
	}
	shardID := binary.LittleEndian.Uint64(data[16:])
	if shardID != 7 {
		t.Fatalf("metadata shardID = %d, want 7", shardID)
	}
	replicaID := binary.LittleEndian.Uint64(data[24:])
	if replicaID != 3 {
		t.Fatalf("metadata replicaID = %d, want 3", replicaID)
	}
}

func TestSnapshotPool_SnapshotSaveDir(t *testing.T) {
	dir := snapshotSaveDir("/data/snapshots", 10, 2, 500)
	expected := filepath.Join("/data/snapshots", "shard-10", "replica-2", "snapshot-00000000000000000500")
	if dir != expected {
		t.Fatalf("snapshotSaveDir = %s, want %s", dir, expected)
	}
}

func TestSnapshotPool_SnapshotSaveDir_ZeroIndex(t *testing.T) {
	dir := snapshotSaveDir("/tmp", 1, 1, 0)
	expected := filepath.Join("/tmp", "shard-1", "replica-1", "snapshot-00000000000000000000")
	if dir != expected {
		t.Fatalf("snapshotSaveDir = %s, want %s", dir, expected)
	}
}

func TestSnapshotPool_LatestSnapshotDir(t *testing.T) {
	tmpDir := t.TempDir()

	for _, idx := range []uint64{100, 200, 300} {
		dir := snapshotSaveDir(tmpDir, 1, 1, idx)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
	}

	dir, index, err := latestSnapshotDir(tmpDir, 1, 1)
	if err != nil {
		t.Fatalf("latestSnapshotDir failed: %v", err)
	}
	if index != 300 {
		t.Fatalf("latest index = %d, want 300", index)
	}
	expectedDir := snapshotSaveDir(tmpDir, 1, 1, 300)
	if dir != expectedDir {
		t.Fatalf("latest dir = %s, want %s", dir, expectedDir)
	}
}

func TestSnapshotPool_LatestSnapshotDir_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	_, _, err := latestSnapshotDir(tmpDir, 1, 1)
	if err == nil {
		t.Fatal("expected error for empty snapshot directory")
	}
}

func TestSnapshotPool_ListSnapshotDirs_NonExistent(t *testing.T) {
	dirs, err := listSnapshotDirs("/nonexistent/path", 1, 1)
	if err != nil {
		t.Fatalf("expected nil error for non-existent directory, got: %v", err)
	}
	if len(dirs) != 0 {
		t.Fatalf("expected 0 dirs, got %d", len(dirs))
	}
}

func TestSnapshotPool_GCSnapshotDirs_NothingToClean(t *testing.T) {
	tmpDir := t.TempDir()
	dir := snapshotSaveDir(tmpDir, 1, 1, 100)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	err := gcSnapshotDirs(tmpDir, 1, 1)
	if err != nil {
		t.Fatalf("gcSnapshotDirs should succeed with nothing to clean: %v", err)
	}

	dirs, _ := listSnapshotDirs(tmpDir, 1, 1)
	if len(dirs) != 1 {
		t.Fatalf("expected 1 dir retained, got %d", len(dirs))
	}
}

func TestSnapshotPool_WriteSnapshotMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	err := writeSnapshotMetadata(tmpDir, 42, 5, 10, 3, 0)
	if err != nil {
		t.Fatalf("writeSnapshotMetadata failed: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(tmpDir, snapshotMetadataFile))
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(data) != snapshotMetaSize {
		t.Fatalf("size = %d, want %d", len(data), snapshotMetaSize)
	}

	if binary.LittleEndian.Uint64(data[0:]) != 42 {
		t.Fatal("index mismatch")
	}
	if binary.LittleEndian.Uint64(data[8:]) != 5 {
		t.Fatal("term mismatch")
	}
	if binary.LittleEndian.Uint64(data[16:]) != 10 {
		t.Fatal("shardID mismatch")
	}
	if binary.LittleEndian.Uint64(data[24:]) != 3 {
		t.Fatal("replicaID mismatch")
	}
	createdAt := int64(binary.LittleEndian.Uint64(data[32:]))
	now := time.Now().Unix()
	if createdAt < now-5 || createdAt > now+5 {
		t.Fatalf("createdAt = %d, expected near %d", createdAt, now)
	}
}

func TestSnapshotPool_WriteSnapshotMetadata_InvalidDir(t *testing.T) {
	err := writeSnapshotMetadata("/nonexistent/dir/that/does/not/exist", 1, 1, 1, 1, 0)
	if err == nil {
		t.Fatal("expected error for invalid directory")
	}
}

// ---------------------------------------------------------------------------
// Barrier/encryption test helpers
// ---------------------------------------------------------------------------

// testBarrier is a minimal SnapshotBarrier implementation for testing.
// It XORs data with a fixed key per epoch (not cryptographically secure,
// but deterministic and round-trip safe for tests).
type testBarrier struct {
	mu             sync.Mutex
	epoch          uint64
	sealed         bool
	inFlight       map[uint64]int64
	purgedBefore   uint64 // tracks the last minEpoch passed to PurgeEpochsBefore
	purgeCallCount int
}

func newTestBarrier(epoch uint64) *testBarrier {
	return &testBarrier{
		epoch:    epoch,
		inFlight: make(map[uint64]int64),
	}
}

// Encrypt XORs plaintext with epoch byte and prepends 8-byte epoch prefix.
func (b *testBarrier) Encrypt(dst, plaintext []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return nil, errors.New("barrier sealed")
	}
	return b.encryptForEpoch(plaintext, b.epoch), nil
}

func (b *testBarrier) EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return nil, 0, errors.New("barrier sealed")
	}
	return b.encryptForEpoch(plaintext, b.epoch), b.epoch, nil
}

func (b *testBarrier) encryptForEpoch(plaintext []byte, epoch uint64) []byte {
	key := byte(epoch & 0xFF)
	out := make([]byte, 8+len(plaintext))
	binary.LittleEndian.PutUint64(out[:8], epoch)
	for i, v := range plaintext {
		out[8+i] = v ^ key
	}
	return out
}

func (b *testBarrier) Decrypt(dst, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < 8 {
		return nil, errors.New("ciphertext too short")
	}
	epoch := binary.LittleEndian.Uint64(ciphertext[:8])
	return b.DecryptForEpoch(dst, ciphertext, epoch)
}

func (b *testBarrier) DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return nil, errors.New("barrier sealed")
	}
	if len(ciphertext) < 8 {
		return nil, errors.New("ciphertext too short")
	}
	key := byte(epoch & 0xFF)
	data := ciphertext[8:]
	out := make([]byte, len(data))
	for i, v := range data {
		out[i] = v ^ key
	}
	return out, nil
}

func (b *testBarrier) CurrentEpoch() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.epoch
}

func (b *testBarrier) IsSealed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sealed
}

func (b *testBarrier) RegisterInFlightEpoch(epoch uint64) {
	b.mu.Lock()
	b.inFlight[epoch]++
	b.mu.Unlock()
}

func (b *testBarrier) DeregisterInFlightEpoch(epoch uint64) {
	b.mu.Lock()
	b.inFlight[epoch]--
	if b.inFlight[epoch] <= 0 {
		delete(b.inFlight, epoch)
	}
	b.mu.Unlock()
}

func (b *testBarrier) PurgeEpochsBefore(minEpoch uint64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.purgedBefore = minEpoch
	b.purgeCallCount++
	return 0, nil
}

func (b *testBarrier) InFlightCount(epoch uint64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlight[epoch]
}

func (b *testBarrier) PurgedBefore() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.purgedBefore
}

func (b *testBarrier) PurgeCallCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.purgeCallCount
}

func (b *testBarrier) SetEpoch(epoch uint64) {
	b.mu.Lock()
	b.epoch = epoch
	b.mu.Unlock()
}

var _ SnapshotBarrier = (*testBarrier)(nil)

// failAfterNBarrier wraps testBarrier and returns an error after N
// successful Encrypt calls, used to test atomic file write rollback.
type failAfterNBarrier struct {
	*testBarrier
	failAfter int
	calls     int
}

func (b *failAfterNBarrier) Encrypt(dst, plaintext []byte) ([]byte, error) {
	b.testBarrier.mu.Lock()
	defer b.testBarrier.mu.Unlock()
	b.calls++
	if b.calls > b.failAfter {
		return nil, errors.New("injected encrypt failure")
	}
	return b.testBarrier.encryptForEpoch(plaintext, b.testBarrier.epoch), nil
}

func (b *failAfterNBarrier) EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error) {
	ct, err := b.Encrypt(dst, plaintext)
	if err != nil {
		return nil, 0, err
	}
	return ct, b.testBarrier.epoch, nil
}

var _ SnapshotBarrier = (*failAfterNBarrier)(nil)

// assertNoTempFiles verifies that no temporary files matching the
// encrypt/decrypt temp file patterns remain in the directory.
func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read dir %s: %v", dir, err)
	}
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) == ".tmp" {
			t.Fatalf("temp file %q was not cleaned up", name)
		}
	}
}

// ---------------------------------------------------------------------------
// I.3: Snapshot encryption round-trip tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_EncryptedSaveAndRecover(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	barrier := newTestBarrier(5)
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// Save a snapshot with encryption.
	ss := newSnapshotTestSnapshotter(100)
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	saveNode := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 10, replicaID: 1, node: saveNode, save: true,
	})
	if err != nil {
		t.Fatalf("save request failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("save failed: %v", result.err)
		}
		if result.index != 100 {
			t.Fatalf("result.index = %d, want 100", result.index)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("save timeout")
	}

	// Verify LogDB snapshot has the epoch.
	snap, err := ldb.GetSnapshot(10, 1)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if snap.Epoch != 5 {
		t.Fatalf("logdb snapshot epoch = %d, want 5", snap.Epoch)
	}

	// Verify the on-disk data file is encrypted (not raw uint64 "100").
	dataPath := filepath.Join(snapshotSaveDir(tmpDir, 10, 1, 100), snapshotDataFile)
	encData, readErr := os.ReadFile(dataPath)
	if readErr != nil {
		t.Fatalf("failed to read encrypted data: %v", readErr)
	}
	// The encrypted file should have frame headers (4 bytes + encrypted chunk),
	// not the raw 8-byte little-endian value.
	if len(encData) == 8 {
		t.Fatal("data file appears to be unencrypted (8 bytes raw)")
	}

	// Verify the metadata contains the epoch.
	metaPath := filepath.Join(snapshotSaveDir(tmpDir, 10, 1, 100), snapshotMetadataFile)
	metaData, metaErr := os.ReadFile(metaPath)
	if metaErr != nil {
		t.Fatalf("failed to read metadata: %v", metaErr)
	}
	if len(metaData) != snapshotMetaSize {
		t.Fatalf("metadata size = %d, want %d", len(metaData), snapshotMetaSize)
	}
	metaEpoch := binary.LittleEndian.Uint64(metaData[40:])
	if metaEpoch != 5 {
		t.Fatalf("metadata epoch = %d, want 5", metaEpoch)
	}

	// Now recover into a fresh snapshotter using the same barrier.
	recoverSS := newSnapshotTestSnapshotter(0)
	recoverNode := NewNode(nil, nil, nil, cfg, 100, recoverSS, tmpDir, nil)

	err = p.requestSnapshot(snapshotRequest{
		shardID: 10, replicaID: 1, node: recoverNode, save: false,
	})
	if err != nil {
		t.Fatalf("recover request failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("recover failed: %v", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recover timeout")
	}

	// Verify the recovered state matches the original.
	if recoverSS.state.Load() != 100 {
		t.Fatalf("recovered state = %d, want 100", recoverSS.state.Load())
	}
	if recoverNode.lastApplied.Load() != 100 {
		t.Fatalf("node.lastApplied = %d, want 100", recoverNode.lastApplied.Load())
	}
}

func TestSnapshotPool_NoBarrier_PlaintextRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	// No barrier = plaintext snapshot.
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(42)
	cfg := config.Config{ShardID: 1, ReplicaID: 1, CompactionOverhead: 10}
	saveNode := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 1, replicaID: 1, node: saveNode, save: true,
	})
	if err != nil {
		t.Fatalf("save request failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("save failed: %v", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("save timeout")
	}

	// Verify LogDB snapshot has epoch 0 (no encryption).
	snap, err := ldb.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if snap.Epoch != 0 {
		t.Fatalf("logdb snapshot epoch = %d, want 0", snap.Epoch)
	}

	// Recover the plaintext snapshot.
	recoverSS := newSnapshotTestSnapshotter(0)
	recoverNode := NewNode(nil, nil, nil, cfg, 100, recoverSS, tmpDir, nil)

	err = p.requestSnapshot(snapshotRequest{
		shardID: 1, replicaID: 1, node: recoverNode, save: false,
	})
	if err != nil {
		t.Fatalf("recover request failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("recover failed: %v", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recover timeout")
	}

	if recoverSS.state.Load() != 42 {
		t.Fatalf("recovered state = %d, want 42", recoverSS.state.Load())
	}
}

func TestSnapshotPool_EncryptDecryptFile_LargePayload(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(3)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Create a file larger than snapshotEncryptChunkSize (64KB) to test
	// multi-chunk encryption.
	dataPath := filepath.Join(tmpDir, "test.dat")
	original := make([]byte, snapshotEncryptChunkSize*3+1234)
	for i := range original {
		original[i] = byte(i % 251)
	}
	if err := os.WriteFile(dataPath, original, 0o644); err != nil {
		t.Fatalf("write test data failed: %v", err)
	}

	// Encrypt.
	if err := p.encryptSnapshotFile(dataPath); err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	// Verify encrypted file differs from original.
	encrypted, _ := os.ReadFile(dataPath)
	if len(encrypted) == len(original) {
		t.Fatal("encrypted file should be larger than original due to framing")
	}

	// Decrypt.
	if err := p.decryptSnapshotFile(dataPath, 3); err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	// Verify decrypted content matches original.
	decrypted, _ := os.ReadFile(dataPath)
	if len(decrypted) != len(original) {
		t.Fatalf("decrypted length = %d, want %d", len(decrypted), len(original))
	}
	for i := range original {
		if decrypted[i] != original[i] {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, decrypted[i], original[i])
		}
	}
}

func TestSnapshotPool_EncryptFile_AtomicPreservesOriginalOnFailure(t *testing.T) {
	tmpDir := t.TempDir()
	// Use a barrier that fails on the second encrypt call (second chunk).
	barrier := &failAfterNBarrier{
		testBarrier: newTestBarrier(1),
		failAfter:   1, // succeed on first chunk, fail on second
	}
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Create a multi-chunk file so the second chunk triggers the error.
	dataPath := filepath.Join(tmpDir, "atomic-encrypt.dat")
	original := make([]byte, snapshotEncryptChunkSize*2)
	for i := range original {
		original[i] = byte(i % 199)
	}
	if err := os.WriteFile(dataPath, original, 0o644); err != nil {
		t.Fatalf("write test data failed: %v", err)
	}

	// Encrypt should fail on the second chunk.
	err := p.encryptSnapshotFile(dataPath)
	if err == nil {
		t.Fatal("expected encrypt to fail on second chunk")
	}
	var encErr *SnapshotEncryptError
	if !errors.As(err, &encErr) {
		t.Fatalf("expected SnapshotEncryptError, got %T: %v", err, err)
	}

	// Original file must be preserved intact (atomic write guarantees this).
	preserved, readErr := os.ReadFile(dataPath)
	if readErr != nil {
		t.Fatalf("read preserved file failed: %v", readErr)
	}
	if len(preserved) != len(original) {
		t.Fatalf("preserved length = %d, want %d", len(preserved), len(original))
	}
	for i := range original {
		if preserved[i] != original[i] {
			t.Fatalf("preserved file corrupted at byte %d: got %d, want %d",
				i, preserved[i], original[i])
		}
	}

	// Verify no temp files left behind.
	assertNoTempFiles(t, tmpDir)
}

func TestSnapshotPool_DecryptFile_AtomicPreservesOriginalOnFailure(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Create an encrypted file with a valid first frame but a truncated
	// second frame to trigger ErrSnapshotCorrupt during decryption.
	dataPath := filepath.Join(tmpDir, "atomic-decrypt.dat")

	// Build a valid first frame.
	chunk1 := make([]byte, 64)
	for i := range chunk1 {
		chunk1[i] = byte(i % 37)
	}
	enc1 := barrier.encryptForEpoch(chunk1, 1)
	var frameBuf [4]byte
	binary.LittleEndian.PutUint32(frameBuf[:], uint32(len(enc1)))

	// Build the file: valid frame 1 + truncated frame 2 (header claims
	// 1000 bytes but only 2 bytes follow).
	var fileData []byte
	fileData = append(fileData, frameBuf[:]...)
	fileData = append(fileData, enc1...)
	var corruptFrame [4]byte
	binary.LittleEndian.PutUint32(corruptFrame[:], 1000)
	fileData = append(fileData, corruptFrame[:]...)
	fileData = append(fileData, 0x01, 0x02)

	if err := os.WriteFile(dataPath, fileData, 0o644); err != nil {
		t.Fatalf("write test data failed: %v", err)
	}

	// Save the original encrypted content for comparison.
	encryptedContent := make([]byte, len(fileData))
	copy(encryptedContent, fileData)

	// Decrypt should fail on the corrupt second frame.
	err := p.decryptSnapshotFile(dataPath, 1)
	if err == nil {
		t.Fatal("expected decrypt to fail on corrupt frame")
	}
	var encErr *SnapshotEncryptError
	if !errors.As(err, &encErr) {
		t.Fatalf("expected SnapshotEncryptError, got %T: %v", err, err)
	}

	// Encrypted file must be preserved intact (atomic write ensures this).
	preserved, readErr := os.ReadFile(dataPath)
	if readErr != nil {
		t.Fatalf("read preserved file failed: %v", readErr)
	}
	if len(preserved) != len(encryptedContent) {
		t.Fatalf("preserved length = %d, want %d", len(preserved), len(encryptedContent))
	}
	for i := range encryptedContent {
		if preserved[i] != encryptedContent[i] {
			t.Fatalf("encrypted file corrupted at byte %d", i)
		}
	}

	// Verify no temp files left behind.
	assertNoTempFiles(t, tmpDir)
}

func TestSnapshotPool_EncryptFile_AtomicTempCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	barrier.sealed = true // Sealed barrier causes Encrypt to return error
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	dataPath := filepath.Join(tmpDir, "temp-cleanup.dat")
	if err := os.WriteFile(dataPath, []byte("test data"), 0o644); err != nil {
		t.Fatalf("write test data failed: %v", err)
	}

	// Encrypt should fail because barrier is sealed.
	err := p.encryptSnapshotFile(dataPath)
	if err == nil {
		t.Fatal("expected encrypt to fail with sealed barrier")
	}

	// Original file must still exist.
	if _, err := os.Stat(dataPath); err != nil {
		t.Fatalf("original file should still exist: %v", err)
	}

	// No temp files should remain.
	assertNoTempFiles(t, tmpDir)
}

func TestSnapshotPool_DecryptFile_CorruptFrameLen(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Write a corrupt file with invalid frame length.
	dataPath := filepath.Join(tmpDir, "corrupt.dat")
	// Frame header says 1000 bytes but file only has 6 bytes.
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], 1000)
	if err := os.WriteFile(dataPath, append(buf[:], 0x01, 0x02), 0o644); err != nil {
		t.Fatalf("write corrupt file failed: %v", err)
	}

	err := p.decryptSnapshotFile(dataPath, 1)
	if err == nil {
		t.Fatal("expected error for corrupt frame")
	}
	var encErr *SnapshotEncryptError
	if !errors.As(err, &encErr) {
		t.Fatalf("expected SnapshotEncryptError, got %T: %v", err, err)
	}
}

func TestSnapshotPool_EncryptFile_SealedBarrier(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	barrier.sealed = true
	stopC := make(chan struct{})
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// Even with barrier sealed, save should work (encryption is skipped
	// when barrier.IsSealed() returns true).
	ss := newSnapshotTestSnapshotter(50)
	cfg := config.Config{ShardID: 1, ReplicaID: 1, CompactionOverhead: 10}
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 1, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("save failed: %v", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	// Epoch should be 0 because barrier was sealed during save.
	snap, _ := ldb.GetSnapshot(1, 1)
	if snap.Epoch != 0 {
		t.Fatalf("epoch = %d, want 0 (barrier sealed)", snap.Epoch)
	}
}

// ---------------------------------------------------------------------------
// I.4: Key rotation tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_EpochInMetadata(t *testing.T) {
	tmpDir := t.TempDir()

	// Write metadata with epoch.
	err := writeSnapshotMetadata(tmpDir, 100, 5, 10, 1, 7)
	if err != nil {
		t.Fatalf("writeSnapshotMetadata failed: %v", err)
	}

	// Read and verify epoch.
	epoch := readSnapshotEpoch(tmpDir)
	if epoch != 7 {
		t.Fatalf("readSnapshotEpoch = %d, want 7", epoch)
	}
}

func TestSnapshotPool_ReadSnapshotEpoch_OldFormat(t *testing.T) {
	tmpDir := t.TempDir()

	// Write a 40-byte (old format) metadata file without epoch.
	metaPath := filepath.Join(tmpDir, snapshotMetadataFile)
	var buf [40]byte                            // old 5-field format
	binary.LittleEndian.PutUint64(buf[0:], 100) // Index
	binary.LittleEndian.PutUint64(buf[8:], 5)   // Term
	if err := os.WriteFile(metaPath, buf[:], 0o644); err != nil {
		t.Fatalf("write old-format metadata failed: %v", err)
	}

	// Should return 0 since the old format doesn't have the epoch field.
	epoch := readSnapshotEpoch(tmpDir)
	if epoch != 0 {
		t.Fatalf("readSnapshotEpoch for old format = %d, want 0", epoch)
	}
}

func TestSnapshotPool_ReadSnapshotEpoch_MissingFile(t *testing.T) {
	tmpDir := t.TempDir()
	epoch := readSnapshotEpoch(tmpDir)
	if epoch != 0 {
		t.Fatalf("readSnapshotEpoch for missing file = %d, want 0", epoch)
	}
}

// ---------------------------------------------------------------------------
// I.5: Epoch purge after compaction tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_PurgeOldEpochs_AfterCompaction(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	// Create 3 snapshots with different epochs.
	for _, info := range []struct {
		index uint64
		epoch uint64
	}{
		{100, 2},
		{200, 3},
		{300, 5},
	} {
		dir := snapshotSaveDir(tmpDir, 1, 1, info.index)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := writeSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch); err != nil {
			t.Fatalf("write metadata failed: %v", err)
		}
	}

	// Call purgeOldEpochs - should purge epochs before the minimum (2).
	p.purgeOldEpochs(tmpDir, 1, 1)

	if barrier.PurgedBefore() != 2 {
		t.Fatalf("purgedBefore = %d, want 2", barrier.PurgedBefore())
	}
}

func TestSnapshotPool_PurgeOldEpochs_NoBarrier(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, nil, nil, nil, stopC, nil)

	// Should not panic when barrier is nil.
	p.purgeOldEpochs(tmpDir, 1, 1)
}

func TestSnapshotPool_PurgeOldEpochs_SealedBarrier(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	barrier.sealed = true
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Should not call PurgeEpochsBefore when sealed.
	p.purgeOldEpochs(tmpDir, 1, 1)
	if barrier.PurgeCallCount() != 0 {
		t.Fatalf("expected 0 purge calls, got %d", barrier.PurgeCallCount())
	}
}

func TestSnapshotPool_PurgeOldEpochs_AllPlaintext(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Create snapshots with epoch=0 (plaintext).
	for _, idx := range []uint64{100, 200} {
		dir := snapshotSaveDir(tmpDir, 1, 1, idx)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := writeSnapshotMetadata(dir, idx, 1, 1, 1, 0); err != nil {
			t.Fatalf("write metadata failed: %v", err)
		}
	}

	// Should not purge since min epoch is 0 (all plaintext).
	p.purgeOldEpochs(tmpDir, 1, 1)
	if barrier.PurgeCallCount() != 0 {
		t.Fatalf("expected 0 purge calls for plaintext snapshots, got %d", barrier.PurgeCallCount())
	}
}

func TestSnapshotPool_PurgeWiredAfterSave(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	barrier := newTestBarrier(3)
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// Save 4 snapshots. GC retains 3 (maxSnapshotsRetained).
	for i := uint64(1); i <= 4; i++ {
		ss := newSnapshotTestSnapshotter(i * 100)
		cfg := config.Config{ShardID: 1, ReplicaID: 1, CompactionOverhead: 10}
		node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)
		err := p.requestSnapshot(snapshotRequest{
			shardID: 1, replicaID: 1, node: node, save: true,
		})
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		select {
		case result := <-p.CompletedC():
			if result.err != nil {
				t.Fatalf("save %d failed: %v", i, result.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for snapshot %d", i)
		}
	}

	// PurgeEpochsBefore should have been called at least once during
	// the save+GC+compaction cycle.
	if barrier.PurgeCallCount() < 1 {
		t.Fatalf("expected at least 1 purge call, got %d", barrier.PurgeCallCount())
	}
}

// ---------------------------------------------------------------------------
// WAL epoch floor tests (purgeOldEpochs bug fix)
// ---------------------------------------------------------------------------

func TestSnapshotPool_PurgeOldEpochs_WALEpochFloor(t *testing.T) {
	// Scenario: snapshot min epoch is 4, but WAL still has records
	// encrypted at epoch 2. The purge floor should be 2, not 4.
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	ldb := newEpochAwareTestLogDB(2) // WAL has records at epoch 2
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	// Create snapshots at epoch 4 and 5.
	for _, info := range []struct {
		index uint64
		epoch uint64
	}{
		{100, 4},
		{200, 5},
	} {
		dir := snapshotSaveDir(tmpDir, 1, 1, info.index)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := writeSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch); err != nil {
			t.Fatalf("write metadata failed: %v", err)
		}
	}

	p.purgeOldEpochs(tmpDir, 1, 1)

	// The purge floor should be 2 (WAL min), not 4 (snapshot min).
	if barrier.PurgedBefore() != 2 {
		t.Fatalf("purgedBefore = %d, want 2 (WAL epoch floor)", barrier.PurgedBefore())
	}
}

func TestSnapshotPool_PurgeOldEpochs_WALEpochHigherThanSnapshot(t *testing.T) {
	// Scenario: WAL min epoch (5) is higher than snapshot min epoch (3).
	// The purge floor should be 3 (snapshot min).
	tmpDir := t.TempDir()
	barrier := newTestBarrier(7)
	stopC := make(chan struct{})
	ldb := newEpochAwareTestLogDB(5) // WAL epoch is higher
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	for _, info := range []struct {
		index uint64
		epoch uint64
	}{
		{100, 3},
		{200, 7},
	} {
		dir := snapshotSaveDir(tmpDir, 1, 1, info.index)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := writeSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch); err != nil {
			t.Fatalf("write metadata failed: %v", err)
		}
	}

	p.purgeOldEpochs(tmpDir, 1, 1)

	// Snapshot min (3) is lower than WAL min (5), so floor stays at 3.
	if barrier.PurgedBefore() != 3 {
		t.Fatalf("purgedBefore = %d, want 3 (snapshot epoch floor)", barrier.PurgedBefore())
	}
}

func TestSnapshotPool_PurgeOldEpochs_WALEpochZeroIgnored(t *testing.T) {
	// Scenario: WAL returns epoch 0 (no encrypted records or no barrier).
	// The purge floor should use snapshot min epoch only.
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	ldb := newEpochAwareTestLogDB(0) // no encrypted WAL records
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	for _, info := range []struct {
		index uint64
		epoch uint64
	}{
		{100, 3},
		{200, 5},
	} {
		dir := snapshotSaveDir(tmpDir, 1, 1, info.index)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := writeSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch); err != nil {
			t.Fatalf("write metadata failed: %v", err)
		}
	}

	p.purgeOldEpochs(tmpDir, 1, 1)

	// WAL epoch 0 is ignored; purge floor is snapshot min (3).
	if barrier.PurgedBefore() != 3 {
		t.Fatalf("purgedBefore = %d, want 3", barrier.PurgedBefore())
	}
}

func TestSnapshotPool_PurgeOldEpochs_WALQueryError(t *testing.T) {
	// Scenario: WAL epoch query fails. Purge should be skipped entirely.
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	ldb := newEpochAwareTestLogDB(0)
	ldb.minLiveEpochErr = errors.New("disk I/O error")
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	for _, info := range []struct {
		index uint64
		epoch uint64
	}{
		{100, 3},
		{200, 5},
	} {
		dir := snapshotSaveDir(tmpDir, 1, 1, info.index)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := writeSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch); err != nil {
			t.Fatalf("write metadata failed: %v", err)
		}
	}

	p.purgeOldEpochs(tmpDir, 1, 1)

	// Purge should NOT have been called due to error.
	if barrier.PurgeCallCount() != 0 {
		t.Fatalf("expected 0 purge calls when WAL query fails, got %d", barrier.PurgeCallCount())
	}
}

func TestSnapshotPool_PurgeOldEpochs_NonEpochAwareLogDB(t *testing.T) {
	// Scenario: LogDB does not implement EpochAwareLogDB (e.g., MemDB).
	// Should fall back to snapshot-only epoch floor.
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	ldb := newSnapshotTestLogDB() // does NOT implement EpochAwareLogDB
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	for _, info := range []struct {
		index uint64
		epoch uint64
	}{
		{100, 2},
		{200, 5},
	} {
		dir := snapshotSaveDir(tmpDir, 1, 1, info.index)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := writeSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch); err != nil {
			t.Fatalf("write metadata failed: %v", err)
		}
	}

	p.purgeOldEpochs(tmpDir, 1, 1)

	// Without EpochAwareLogDB, purge floor is snapshot min (2).
	if barrier.PurgedBefore() != 2 {
		t.Fatalf("purgedBefore = %d, want 2", barrier.PurgedBefore())
	}
}

func TestSnapshotPool_PurgeOldEpochs_NilLogDB(t *testing.T) {
	// Scenario: LogDB is nil. Should still work with snapshot-only logic.
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	for _, info := range []struct {
		index uint64
		epoch uint64
	}{
		{100, 3},
		{200, 5},
	} {
		dir := snapshotSaveDir(tmpDir, 1, 1, info.index)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := writeSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch); err != nil {
			t.Fatalf("write metadata failed: %v", err)
		}
	}

	p.purgeOldEpochs(tmpDir, 1, 1)

	// Nil logdb means no EpochAwareLogDB check; purge floor is snapshot min (3).
	if barrier.PurgedBefore() != 3 {
		t.Fatalf("purgedBefore = %d, want 3", barrier.PurgedBefore())
	}
}

// ---------------------------------------------------------------------------
// I.6: In-flight epoch protection tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_InFlightEpochTracking(t *testing.T) {
	barrier := newTestBarrier(5)

	// Register an in-flight epoch.
	barrier.RegisterInFlightEpoch(3)
	if barrier.InFlightCount(3) != 1 {
		t.Fatalf("in-flight count = %d, want 1", barrier.InFlightCount(3))
	}

	// Register again.
	barrier.RegisterInFlightEpoch(3)
	if barrier.InFlightCount(3) != 2 {
		t.Fatalf("in-flight count = %d, want 2", barrier.InFlightCount(3))
	}

	// Deregister once.
	barrier.DeregisterInFlightEpoch(3)
	if barrier.InFlightCount(3) != 1 {
		t.Fatalf("in-flight count = %d, want 1", barrier.InFlightCount(3))
	}

	// Deregister again, should be removed from map.
	barrier.DeregisterInFlightEpoch(3)
	if barrier.InFlightCount(3) != 0 {
		t.Fatalf("in-flight count = %d, want 0", barrier.InFlightCount(3))
	}
}

// ---------------------------------------------------------------------------
// Error type tests
// ---------------------------------------------------------------------------

func TestSnapshotEncryptError_Error(t *testing.T) {
	err := &SnapshotEncryptError{Op: "encrypt", Err: ErrSnapshotCorrupt}
	want := "engine: snapshot encrypt failed: engine: snapshot data corrupt"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
	}
}

func TestSnapshotEncryptError_Unwrap(t *testing.T) {
	inner := ErrSnapshotCorrupt
	err := &SnapshotEncryptError{Op: "decrypt", Err: inner}
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatal("Unwrap should chain to ErrSnapshotCorrupt")
	}
}

func TestSnapshotPool_DecryptFile_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Write an empty file.
	dataPath := filepath.Join(tmpDir, "empty.dat")
	if err := os.WriteFile(dataPath, []byte{}, 0o644); err != nil {
		t.Fatalf("write empty file failed: %v", err)
	}

	// Decrypting an empty file should succeed (no-op).
	if err := p.decryptSnapshotFile(dataPath, 1); err != nil {
		t.Fatalf("decrypt empty file failed: %v", err)
	}
}

func TestSnapshotPool_WriteMetadataWithEpoch(t *testing.T) {
	tmpDir := t.TempDir()

	err := writeSnapshotMetadata(tmpDir, 42, 5, 10, 3, 7)
	if err != nil {
		t.Fatalf("writeSnapshotMetadata failed: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(tmpDir, snapshotMetadataFile))
	if err != nil {
		t.Fatalf("read metadata failed: %v", err)
	}
	if len(data) != snapshotMetaSize {
		t.Fatalf("metadata size = %d, want %d", len(data), snapshotMetaSize)
	}

	// Verify all 6 fields.
	if binary.LittleEndian.Uint64(data[0:]) != 42 {
		t.Fatal("index mismatch")
	}
	if binary.LittleEndian.Uint64(data[8:]) != 5 {
		t.Fatal("term mismatch")
	}
	if binary.LittleEndian.Uint64(data[16:]) != 10 {
		t.Fatal("shardID mismatch")
	}
	if binary.LittleEndian.Uint64(data[24:]) != 3 {
		t.Fatal("replicaID mismatch")
	}
	createdAt := int64(binary.LittleEndian.Uint64(data[32:]))
	now := time.Now().Unix()
	if createdAt < now-5 || createdAt > now+5 {
		t.Fatalf("createdAt = %d, expected near %d", createdAt, now)
	}
	if binary.LittleEndian.Uint64(data[40:]) != 7 {
		t.Fatal("epoch mismatch")
	}
}

func TestSnapshotPool_SaveSnapshot_PersistsMembership(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// Create a real peer with a known membership (single-node cluster).
	var shardID, replicaID uint64 = 10, 1
	peer, lr := newTestPeer(t, shardID, replicaID)
	// Tick the single-node cluster to leader so the peer is fully
	// initialized with its membership.
	tickPeerToLeader(peer)

	ss := newSnapshotTestSnapshotter(100)
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   shardID,
		replicaID: replicaID,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
		if result.index != 100 {
			t.Fatalf("result.index = %d, want 100", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}

	// Verify LogDB snapshot contains the membership.
	snap, err := ldb.GetSnapshot(shardID, replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if snap.Index != 100 {
		t.Fatalf("logdb snapshot index = %d, want 100", snap.Index)
	}

	// The peer was bootstrapped with Addresses{replicaID: "localhost:0"}.
	// Verify this membership was captured in the snapshot.
	if len(snap.Membership.Addresses) == 0 {
		t.Fatal("snapshot membership Addresses is empty; membership not persisted")
	}
	addr, ok := snap.Membership.Addresses[replicaID]
	if !ok {
		t.Fatalf("snapshot membership missing replicaID %d", replicaID)
	}
	if addr != "localhost:0" {
		t.Fatalf("snapshot membership address = %q, want %q", addr, "localhost:0")
	}
}

func TestSnapshotPool_SaveSnapshot_NilPeer_EmptyMembership(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// Create a node with nil peer (e.g. during recovery or test scenarios).
	var shardID, replicaID uint64 = 20, 2
	ss := newSnapshotTestSnapshotter(50)
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 10}
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   shardID,
		replicaID: replicaID,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
		if result.index != 50 {
			t.Fatalf("result.index = %d, want 50", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}

	// Verify LogDB snapshot has zero-value membership (not nil maps, just empty).
	snap, err := ldb.GetSnapshot(shardID, replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if snap.Index != 50 {
		t.Fatalf("logdb snapshot index = %d, want 50", snap.Index)
	}

	// Empty membership from nil peer: ConfigChangeID=0 and no addresses.
	empty := logdb.Membership{ConfigChangeID: 0}
	if snap.Membership.ConfigChangeID != empty.ConfigChangeID {
		t.Fatalf("ConfigChangeID = %d, want 0", snap.Membership.ConfigChangeID)
	}
	if len(snap.Membership.Addresses) != 0 {
		t.Fatalf("expected empty Addresses, got %v", snap.Membership.Addresses)
	}
}

func TestSnapshotPool_SaveSnapshot_MembershipMultiNode(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// Create a 3-node membership via LogReader and Peer.
	var shardID, replicaID uint64 = 30, 1
	ldb2 := newTestLogDB()
	lr := logdb.NewLogReader(shardID, replicaID, ldb2)
	ms := proto.Membership{
		ConfigChangeID: 5,
		Addresses: map[uint64]string{
			1: "node1:9000",
			2: "node2:9000",
			3: "node3:9000",
		},
		Observers: map[uint64]string{
			4: "observer1:9000",
		},
	}
	lr.ApplySnapshot(proto.Snapshot{
		Index:      0,
		Term:       0,
		Membership: ms,
	})
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}

	ss := newSnapshotTestSnapshotter(200)
	node := NewNode(peer, nil, lr, cfg, 100, ss, tmpDir, nil)

	err = p.requestSnapshot(snapshotRequest{
		shardID:   shardID,
		replicaID: replicaID,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("unexpected error: %v", result.err)
		}
		if result.index != 200 {
			t.Fatalf("result.index = %d, want 200", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}

	// Verify LogDB snapshot captured the full 3-node membership.
	snap, snapErr := ldb.GetSnapshot(shardID, replicaID)
	if snapErr != nil {
		t.Fatalf("GetSnapshot failed: %v", snapErr)
	}
	if snap.Membership.ConfigChangeID != 5 {
		t.Fatalf("ConfigChangeID = %d, want 5", snap.Membership.ConfigChangeID)
	}
	if len(snap.Membership.Addresses) != 3 {
		t.Fatalf("Addresses count = %d, want 3", len(snap.Membership.Addresses))
	}
	for id, expected := range map[uint64]string{1: "node1:9000", 2: "node2:9000", 3: "node3:9000"} {
		got, ok := snap.Membership.Addresses[id]
		if !ok {
			t.Fatalf("missing replica %d in Addresses", id)
		}
		if got != expected {
			t.Fatalf("Addresses[%d] = %q, want %q", id, got, expected)
		}
	}
	if len(snap.Membership.Observers) != 1 {
		t.Fatalf("Observers count = %d, want 1", len(snap.Membership.Observers))
	}
	if snap.Membership.Observers[4] != "observer1:9000" {
		t.Fatalf("Observers[4] = %q, want %q", snap.Membership.Observers[4], "observer1:9000")
	}
}

// ---------------------------------------------------------------------------
// I.8: Streaming encrypt/decrypt tests (memory amplification fix)
// ---------------------------------------------------------------------------

// TestSnapshotPool_EncryptFile_StreamingMultiChunk verifies that the
// streaming encryptSnapshotFile correctly processes files larger than a
// single snapshotEncryptChunkSize without loading the entire file into
// memory. Validates round-trip correctness with varied chunk boundaries.
func TestSnapshotPool_EncryptFile_StreamingMultiChunk(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Create a file spanning 5 chunks plus a partial final chunk to
	// exercise boundary handling in the streaming read loop.
	dataPath := filepath.Join(tmpDir, "streaming-encrypt.dat")
	original := make([]byte, snapshotEncryptChunkSize*5+7777)
	for i := range original {
		original[i] = byte((i * 13) % 251)
	}
	if err := os.WriteFile(dataPath, original, 0o644); err != nil {
		t.Fatalf("write test data failed: %v", err)
	}

	// Encrypt.
	if err := p.encryptSnapshotFile(dataPath); err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	// Encrypted file should differ and be larger due to framing overhead.
	encrypted, readErr := os.ReadFile(dataPath)
	if readErr != nil {
		t.Fatalf("read encrypted file failed: %v", readErr)
	}
	if len(encrypted) <= len(original) {
		t.Fatal("encrypted file should be larger than original due to framing")
	}

	// Decrypt and verify round-trip integrity.
	if err := p.decryptSnapshotFile(dataPath, 5); err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	decrypted, readErr := os.ReadFile(dataPath)
	if readErr != nil {
		t.Fatalf("read decrypted file failed: %v", readErr)
	}
	if len(decrypted) != len(original) {
		t.Fatalf("decrypted length = %d, want %d", len(decrypted), len(original))
	}
	for i := range original {
		if decrypted[i] != original[i] {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, decrypted[i], original[i])
		}
	}

	assertNoTempFiles(t, tmpDir)
}

// TestSnapshotPool_EncryptFile_StreamingSingleByte verifies encrypt/decrypt
// round-trip for a file smaller than snapshotEncryptChunkSize (single
// partial read).
func TestSnapshotPool_EncryptFile_StreamingSingleByte(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	dataPath := filepath.Join(tmpDir, "single-byte.dat")
	original := []byte{0x42}
	if err := os.WriteFile(dataPath, original, 0o644); err != nil {
		t.Fatalf("write test data failed: %v", err)
	}

	if err := p.encryptSnapshotFile(dataPath); err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}
	if err := p.decryptSnapshotFile(dataPath, 1); err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	decrypted, readErr := os.ReadFile(dataPath)
	if readErr != nil {
		t.Fatalf("read decrypted file failed: %v", readErr)
	}
	if len(decrypted) != 1 || decrypted[0] != 0x42 {
		t.Fatalf("decrypted = %v, want [0x42]", decrypted)
	}
}

// TestSnapshotPool_EncryptFile_StreamingExactChunkBoundary verifies correct
// behavior when the file size is an exact multiple of snapshotEncryptChunkSize.
func TestSnapshotPool_EncryptFile_StreamingExactChunkBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(2)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	dataPath := filepath.Join(tmpDir, "exact-boundary.dat")
	original := make([]byte, snapshotEncryptChunkSize*3)
	for i := range original {
		original[i] = byte(i % 199)
	}
	if err := os.WriteFile(dataPath, original, 0o644); err != nil {
		t.Fatalf("write test data failed: %v", err)
	}

	if err := p.encryptSnapshotFile(dataPath); err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}
	if err := p.decryptSnapshotFile(dataPath, 2); err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	decrypted, readErr := os.ReadFile(dataPath)
	if readErr != nil {
		t.Fatalf("read decrypted file failed: %v", readErr)
	}
	if len(decrypted) != len(original) {
		t.Fatalf("decrypted length = %d, want %d", len(decrypted), len(original))
	}
	for i := range original {
		if decrypted[i] != original[i] {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, decrypted[i], original[i])
		}
	}
}

// TestSnapshotPool_EncryptFile_MissingFile verifies that encryptSnapshotFile
// returns a SnapshotEncryptError when the source file does not exist.
func TestSnapshotPool_EncryptFile_MissingFile(t *testing.T) {
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	err := p.encryptSnapshotFile("/nonexistent/path/snapshot.dat")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	var encErr *SnapshotEncryptError
	if !errors.As(err, &encErr) {
		t.Fatalf("expected SnapshotEncryptError, got %T: %v", err, err)
	}
	if encErr.Op != "read" {
		t.Errorf("Op = %q, want %q", encErr.Op, "read")
	}
}

// TestSnapshotPool_DecryptFile_MissingFile verifies that decryptSnapshotFile
// returns a SnapshotEncryptError when the source file does not exist.
func TestSnapshotPool_DecryptFile_MissingFile(t *testing.T) {
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	err := p.decryptSnapshotFile("/nonexistent/path/snapshot.dat", 1)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	var encErr *SnapshotEncryptError
	if !errors.As(err, &encErr) {
		t.Fatalf("expected SnapshotEncryptError, got %T: %v", err, err)
	}
	if encErr.Op != "read" {
		t.Errorf("Op = %q, want %q", encErr.Op, "read")
	}
}

// TestSnapshotPool_DecryptFile_TruncatedFrameHeader verifies that a file
// with fewer than 4 bytes remaining (partial frame header) is detected as
// corrupt during streaming decryption.
func TestSnapshotPool_DecryptFile_TruncatedFrameHeader(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Write a file with only 2 bytes (not enough for a frame header).
	dataPath := filepath.Join(tmpDir, "truncated-header.dat")
	if err := os.WriteFile(dataPath, []byte{0x01, 0x02}, 0o644); err != nil {
		t.Fatalf("write test data failed: %v", err)
	}

	err := p.decryptSnapshotFile(dataPath, 1)
	if err == nil {
		t.Fatal("expected error for truncated frame header")
	}
	var encErr *SnapshotEncryptError
	if !errors.As(err, &encErr) {
		t.Fatalf("expected SnapshotEncryptError, got %T: %v", err, err)
	}
	if !errors.Is(encErr.Err, ErrSnapshotCorrupt) {
		t.Errorf("expected ErrSnapshotCorrupt in chain, got: %v", encErr.Err)
	}
}

// TestSnapshotPool_DecryptFile_TruncatedFrameData verifies that a file
// with a valid frame header but insufficient data bytes is detected as
// corrupt during streaming decryption.
func TestSnapshotPool_DecryptFile_TruncatedFrameData(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Write a frame header claiming 100 bytes, but only 5 bytes of data.
	dataPath := filepath.Join(tmpDir, "truncated-data.dat")
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], 100)
	fileData := append(header[:], []byte{1, 2, 3, 4, 5}...)
	if err := os.WriteFile(dataPath, fileData, 0o644); err != nil {
		t.Fatalf("write test data failed: %v", err)
	}

	err := p.decryptSnapshotFile(dataPath, 1)
	if err == nil {
		t.Fatal("expected error for truncated frame data")
	}
	var encErr *SnapshotEncryptError
	if !errors.As(err, &encErr) {
		t.Fatalf("expected SnapshotEncryptError, got %T: %v", err, err)
	}
	if !errors.Is(encErr.Err, ErrSnapshotCorrupt) {
		t.Errorf("expected ErrSnapshotCorrupt in chain, got: %v", encErr.Err)
	}
}

// --- Snapshot Pool Metrics Tests ---

// snapshotTestMetrics is a minimal RaftMetrics for snapshot pool tests.
type snapshotTestMetrics struct {
	snapshotCounts sync.Map // shardID -> *atomic.Int64
}

func newSnapshotTestMetrics() *snapshotTestMetrics {
	return &snapshotTestMetrics{}
}

func (m *snapshotTestMetrics) ObserveProposalLatency(_ uint64, _ time.Duration) {}
func (m *snapshotTestMetrics) ObserveCommitLatency(_ uint64, _ time.Duration)   {}
func (m *snapshotTestMetrics) ObserveApplyLatency(_ uint64, _ time.Duration)    {}
func (m *snapshotTestMetrics) IncProposalDropped(_ uint64)                      {}
func (m *snapshotTestMetrics) ObserveProposalBatchSize(_ uint64, _ int)         {}
func (m *snapshotTestMetrics) ObserveCommitBatchSize(_ int)                     {}
func (m *snapshotTestMetrics) IncElection(_ uint64)                             {}

func (m *snapshotTestMetrics) IncSnapshot(shardID uint64) {
	v, _ := m.snapshotCounts.LoadOrStore(shardID, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (m *snapshotTestMetrics) IncPiggybackCommit(_ uint64) {}

func (m *snapshotTestMetrics) getSnapshotCount(shardID uint64) int64 {
	v, ok := m.snapshotCounts.Load(shardID)
	if !ok {
		return 0
	}
	return v.(*atomic.Int64).Load()
}

var _ RaftMetrics = (*snapshotTestMetrics)(nil)

// TestSnapshotPool_Metrics_IncSnapshot verifies that IncSnapshot is
// called on successful snapshot save when metrics are enabled.
func TestSnapshotPool_Metrics_IncSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	ldb := newSnapshotTestLogDB()
	stopC := make(chan struct{})
	defer close(stopC)
	metrics := newSnapshotTestMetrics()

	var wg sync.WaitGroup
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, metrics)
	p.start(&wg)

	snapshotter := newSnapshotTestSnapshotter(100)
	cfg := config.Config{ShardID: 7, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, tmpDir, nil)
	node.snapshotter = snapshotter

	err := p.requestSnapshot(snapshotRequest{
		shardID: 7, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("snapshot save failed: %v", result.err)
		}
		if result.shardID != 7 {
			t.Fatalf("result.shardID = %d, want 7", result.shardID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot result not received within timeout")
	}

	count := metrics.getSnapshotCount(7)
	if count != 1 {
		t.Fatalf("expected 1 snapshot metric, got %d", count)
	}
}

// TestSnapshotPool_Metrics_NilSafe verifies that snapshot save works
// correctly without a metrics collector.
func TestSnapshotPool_Metrics_NilSafe(t *testing.T) {
	tmpDir := t.TempDir()
	ldb := newSnapshotTestLogDB()
	stopC := make(chan struct{})
	defer close(stopC)

	var wg sync.WaitGroup
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.start(&wg)

	snapshotter := newSnapshotTestSnapshotter(100)
	cfg := config.Config{ShardID: 7, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, 100, nil, tmpDir, nil)
	node.snapshotter = snapshotter

	err := p.requestSnapshot(snapshotRequest{
		shardID: 7, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("snapshot save failed: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot result not received within timeout")
	}
	// No panic = success with nil metrics.
}

// ---------------------------------------------------------------------------
// M4: Snapshot term resolved from Peer.AppliedToTerm, not LogReader
// ---------------------------------------------------------------------------

func TestSnapshotPool_SaveSnapshot_TermFromAppliedToTerm(t *testing.T) {
	// When a snapshot is saved, the term must come from the Peer's
	// AppliedToTerm (cached in inMemory) rather than the LogReader.
	// If the entry at the snapshot index has been compacted, the LogReader
	// returns term=0, corrupting snapshot metadata. This test verifies
	// that the term is correctly resolved even when LogReader cannot
	// provide it.
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	pool := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	pool.start(&wg)

	// Create a Peer with a known snapshot at index 100, term 7.
	// The snapshot restore sets appliedToTerm=7 in inMemory.
	var shardID, replicaID uint64 = 50, 1
	testLDB := newTestLogDB()
	lr := logdb.NewLogReader(shardID, replicaID, testLDB)
	ms := proto.Membership{
		Addresses: map[uint64]string{replicaID: "localhost:0"},
	}
	// Bootstrap with initial snapshot at index 0.
	lr.ApplySnapshot(proto.Snapshot{
		Index:      0,
		Term:       0,
		Membership: ms,
	})
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}

	// Install a snapshot at index 100, term 7. This sets
	// appliedToTerm=7 via inmem.restore().
	snapMsg := proto.Message{
		Type:    proto.InstallSnapshot,
		From:    2,
		To:      1,
		ShardID: shardID,
		Term:    7,
		Snapshot: proto.Snapshot{
			Index:      100,
			Term:       7,
			Membership: ms,
		},
	}
	if handleErr := peer.Handle(snapMsg); handleErr != nil {
		t.Fatalf("Handle snapshot failed: %v", handleErr)
	}

	// Verify precondition: AppliedToTerm is 7.
	if peer.AppliedToTerm() != 7 {
		t.Fatalf("precondition: expected AppliedToTerm 7, got %d", peer.AppliedToTerm())
	}

	// The LogReader has no entries at index 100 (they were never
	// persisted), so lr.Term(100) would fail. The term is now captured
	// at request creation time (before pool dispatch) to eliminate the
	// data race between the snapshot pool worker and step worker.
	ss := newSnapshotTestSnapshotter(100)
	node := NewNode(peer, nil, lr, cfg, 100, ss, tmpDir, nil)

	err = pool.requestSnapshot(snapshotRequest{
		shardID:       shardID,
		replicaID:     replicaID,
		node:          node,
		appliedToTerm: peer.AppliedToTerm(),
		save:          true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-pool.CompletedC():
		if result.err != nil {
			t.Fatalf("snapshot save failed: %v", result.err)
		}
		if result.index != 100 {
			t.Fatalf("result.index = %d, want 100", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot result not received within timeout")
	}

	// Verify LogDB snapshot has term=7 (from AppliedToTerm, not 0 from LogReader).
	snap, snapErr := ldb.GetSnapshot(shardID, replicaID)
	if snapErr != nil {
		t.Fatalf("GetSnapshot failed: %v", snapErr)
	}
	if snap.Term != 7 {
		t.Fatalf("snapshot term = %d, want 7 (from AppliedToTerm)", snap.Term)
	}
}

func TestSnapshotPool_SaveSnapshot_NilPeerTermZero(t *testing.T) {
	// When the Peer is nil (e.g. during recovery), the term should be 0
	// as a safe fallback. This verifies the nil check in the fix.
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	pool := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	pool.start(&wg)

	ss := newSnapshotTestSnapshotter(50)
	cfg := config.Config{ShardID: 20, ReplicaID: 1, CompactionOverhead: 10}
	cfg.SetDefaults()
	// Node with nil peer.
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := pool.requestSnapshot(snapshotRequest{
		shardID:   20,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-pool.CompletedC():
		if result.err != nil {
			t.Fatalf("snapshot save failed: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot result not received within timeout")
	}

	// Term should be 0 since there's no peer.
	snap, snapErr := ldb.GetSnapshot(20, 1)
	if snapErr != nil {
		t.Fatalf("GetSnapshot failed: %v", snapErr)
	}
	if snap.Term != 0 {
		t.Fatalf("snapshot term = %d, want 0 for nil peer", snap.Term)
	}
}

// ---------------------------------------------------------------------------
// snapshotRequest.appliedToTerm: captured at creation, not read from Peer
// ---------------------------------------------------------------------------

func TestSnapshotRequest_AppliedToTermCarriedInRequest(t *testing.T) {
	// Verify that saveSnapshot uses the term from the request struct
	// rather than reading it from the Peer. This eliminates a data race
	// between the snapshot pool worker (reader) and the step worker
	// (writer of inMemory.appliedToTerm).
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	pool := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	pool.start(&wg)

	// Create a Peer and install a snapshot at index 100, term 7.
	var shardID, replicaID uint64 = 60, 1
	testLDB := newTestLogDB()
	lr := logdb.NewLogReader(shardID, replicaID, testLDB)
	ms := proto.Membership{
		Addresses: map[uint64]string{replicaID: "localhost:0"},
	}
	lr.ApplySnapshot(proto.Snapshot{
		Index:      0,
		Term:       0,
		Membership: ms,
	})
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}
	snapMsg := proto.Message{
		Type:    proto.InstallSnapshot,
		From:    2,
		To:      1,
		ShardID: shardID,
		Term:    7,
		Snapshot: proto.Snapshot{
			Index:      100,
			Term:       7,
			Membership: ms,
		},
	}
	if handleErr := peer.Handle(snapMsg); handleErr != nil {
		t.Fatalf("Handle snapshot failed: %v", handleErr)
	}

	ss := newSnapshotTestSnapshotter(100)
	node := NewNode(peer, nil, lr, cfg, 100, ss, tmpDir, nil)

	// Capture the term at creation time, as production code does.
	capturedTerm := peer.AppliedToTerm()
	if capturedTerm != 7 {
		t.Fatalf("captured term = %d, want 7", capturedTerm)
	}

	// Pass the captured term in the request. The pool worker must
	// use this value, not call peer.AppliedToTerm() itself.
	err = pool.requestSnapshot(snapshotRequest{
		shardID:       shardID,
		replicaID:     replicaID,
		node:          node,
		appliedToTerm: capturedTerm,
		save:          true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-pool.CompletedC():
		if result.err != nil {
			t.Fatalf("snapshot save failed: %v", result.err)
		}
		if result.index != 100 {
			t.Fatalf("result.index = %d, want 100", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot result not received within timeout")
	}

	// Verify the LogDB snapshot has the term from the request.
	snap, snapErr := ldb.GetSnapshot(shardID, replicaID)
	if snapErr != nil {
		t.Fatalf("GetSnapshot failed: %v", snapErr)
	}
	if snap.Term != 7 {
		t.Fatalf("snapshot term = %d, want 7 (from request.appliedToTerm)", snap.Term)
	}
}

func TestSnapshotRequest_ZeroTermWhenNoPeer(t *testing.T) {
	// When no peer exists (nil), the appliedToTerm in the request
	// should be zero, and the snapshot should be saved with term=0.
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	pool := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	pool.start(&wg)

	ss := newSnapshotTestSnapshotter(50)
	cfg := config.Config{ShardID: 25, ReplicaID: 1, CompactionOverhead: 10}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	// With nil peer, appliedToTerm is zero (the default).
	err := pool.requestSnapshot(snapshotRequest{
		shardID:       25,
		replicaID:     1,
		node:          node,
		appliedToTerm: 0,
		save:          true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-pool.CompletedC():
		if result.err != nil {
			t.Fatalf("snapshot save failed: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot result not received within timeout")
	}

	snap, snapErr := ldb.GetSnapshot(25, 1)
	if snapErr != nil {
		t.Fatalf("GetSnapshot failed: %v", snapErr)
	}
	if snap.Term != 0 {
		t.Fatalf("snapshot term = %d, want 0 for nil peer", snap.Term)
	}
}

func TestSnapshotRequest_TermOverridesStaleValue(t *testing.T) {
	// Verify that the term in the request is used even when it differs
	// from what the Peer would currently return. This simulates the case
	// where the step worker has advanced appliedToTerm after the request
	// was created but before the pool worker processes it.
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	pool := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	pool.start(&wg)

	var shardID, replicaID uint64 = 70, 1
	testLDB := newTestLogDB()
	lr := logdb.NewLogReader(shardID, replicaID, testLDB)
	ms := proto.Membership{
		Addresses: map[uint64]string{replicaID: "localhost:0"},
	}
	lr.ApplySnapshot(proto.Snapshot{
		Index:      0,
		Term:       0,
		Membership: ms,
	})
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}

	// Install snapshot at index 100, term 5.
	snapMsg := proto.Message{
		Type:    proto.InstallSnapshot,
		From:    2,
		To:      1,
		ShardID: shardID,
		Term:    5,
		Snapshot: proto.Snapshot{
			Index:      100,
			Term:       5,
			Membership: ms,
		},
	}
	if handleErr := peer.Handle(snapMsg); handleErr != nil {
		t.Fatalf("Handle snapshot failed: %v", handleErr)
	}
	if peer.AppliedToTerm() != 5 {
		t.Fatalf("precondition: AppliedToTerm = %d, want 5", peer.AppliedToTerm())
	}

	ss := newSnapshotTestSnapshotter(100)
	node := NewNode(peer, nil, lr, cfg, 100, ss, tmpDir, nil)

	// Capture term 5 at request creation time. Even if the peer's
	// appliedToTerm changes later, the snapshot must use term 5.
	err = pool.requestSnapshot(snapshotRequest{
		shardID:       shardID,
		replicaID:     replicaID,
		node:          node,
		appliedToTerm: 5,
		save:          true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-pool.CompletedC():
		if result.err != nil {
			t.Fatalf("snapshot save failed: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot result not received within timeout")
	}

	snap, snapErr := ldb.GetSnapshot(shardID, replicaID)
	if snapErr != nil {
		t.Fatalf("GetSnapshot failed: %v", snapErr)
	}
	if snap.Term != 5 {
		t.Fatalf("snapshot term = %d, want 5 (from captured appliedToTerm)", snap.Term)
	}
}

// ---------------------------------------------------------------------------
// M9: LogReader.CreateSnapshot error handling
// ---------------------------------------------------------------------------

func TestSnapshotPool_SaveSnapshot_CreateSnapshotOutOfDate(t *testing.T) {
	// Set up a LogReader that already has a snapshot at index 200.
	// When saveSnapshot saves at index 100, lr.CreateSnapshot will
	// return ErrSnapshotOutOfDate. Verify the save succeeds (snapshot
	// data and LogDB are written) and the error is logged at Warn level.
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	pool := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	pool.start(&wg)

	// Create a real LogReader with a pre-existing snapshot at index 200.
	lr := logdb.NewLogReader(10, 1, nil)
	lr.SetState(proto.State{Commit: 200})
	lr.ApplySnapshot(proto.Snapshot{Index: 200, Term: 3, Membership: proto.Membership{
		ConfigChangeID: 1,
		Addresses:      map[uint64]string{1: "addr1"},
	}})

	ss := newSnapshotTestSnapshotter(100) // will save at index 100
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	node := NewNode(nil, nil, lr, cfg, 100, ss, tmpDir, nil)

	err := pool.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-pool.CompletedC():
		if result.err != nil {
			t.Fatalf("snapshot save failed: %v", result.err)
		}
		if result.index != 100 {
			t.Fatalf("result.index = %d, want 100", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}

	// Verify that the snapshot was persisted to LogDB despite the
	// CreateSnapshot ErrSnapshotOutOfDate. The saveSnapshot method
	// should not propagate the error.
	snap, snapErr := ldb.GetSnapshot(10, 1)
	if snapErr != nil {
		t.Fatalf("GetSnapshot failed: %v", snapErr)
	}
	if snap.Index != 100 {
		t.Fatalf("logdb snapshot index = %d, want 100", snap.Index)
	}
}

func TestSnapshotPool_SaveSnapshot_CreateSnapshotSuccess(t *testing.T) {
	// Verify that when CreateSnapshot succeeds (snapshot index is newer
	// than the LogReader's current snapshot), the save completes normally.
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	pool := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	pool.start(&wg)

	// Create a LogReader with no pre-existing snapshot.
	lr := logdb.NewLogReader(10, 1, nil)

	ss := newSnapshotTestSnapshotter(100)
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	node := NewNode(nil, nil, lr, cfg, 100, ss, tmpDir, nil)

	err := pool.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-pool.CompletedC():
		if result.err != nil {
			t.Fatalf("snapshot save failed: %v", result.err)
		}
		if result.index != 100 {
			t.Fatalf("result.index = %d, want 100", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}

	// Verify the LogReader's snapshot was updated.
	lrSnap := lr.Snapshot()
	if lrSnap.Index != 100 {
		t.Fatalf("LogReader snapshot index = %d, want 100", lrSnap.Index)
	}
}

// ---------------------------------------------------------------------------
// Snapshot persistence safety tests
// ---------------------------------------------------------------------------

// failingSnapshotter is a Snapshotter that returns an error from
// SaveSnapshot after writing partial data. This simulates a state
// machine failure mid-write.
type failingSnapshotter struct {
	lastApplied atomic.Uint64
	saveErr     error
}

func newFailingSnapshotter(lastApplied uint64, saveErr error) *failingSnapshotter {
	s := &failingSnapshotter{saveErr: saveErr}
	s.lastApplied.Store(lastApplied)
	return s
}

func (s *failingSnapshotter) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	// Write partial data before returning the error, simulating a
	// mid-write failure where some bytes reach the file.
	_, _ = w.Write([]byte{0x01, 0x02})
	return s.saveErr
}

func (s *failingSnapshotter) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (s *failingSnapshotter) LastApplied() uint64 {
	return s.lastApplied.Load()
}

var _ Snapshotter = (*failingSnapshotter)(nil)

// ErrSnapshotSaveFailed is a typed error for test assertions when
// simulating a state machine snapshot save failure.
type ErrSnapshotSaveFailed struct {
	Msg string
}

func (e *ErrSnapshotSaveFailed) Error() string {
	return e.Msg
}

func TestSnapshotPool_SaveSnapshot_NoPartialFileOnError(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	// Use a failing snapshotter that errors after writing partial data.
	saveErr := &ErrSnapshotSaveFailed{Msg: "simulated state machine failure"}
	ss := newFailingSnapshotter(100, saveErr)
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	node := NewNode(nil, nil, nil, cfg, 100, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err == nil {
			t.Fatal("expected error from failing snapshotter, got nil")
		}
		var snapErr *SnapshotError
		if !errors.As(result.err, &snapErr) {
			t.Fatalf("expected *SnapshotError, got %T: %v", result.err, result.err)
		}
		if !snapErr.Save {
			t.Fatal("expected Save=true in SnapshotError")
		}
		// Verify the underlying cause is our simulated error.
		var cause *ErrSnapshotSaveFailed
		if !errors.As(snapErr.Err, &cause) {
			t.Fatalf("expected *ErrSnapshotSaveFailed as cause, got %T: %v", snapErr.Err, snapErr.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}

	// Verify the snapshot was NOT recorded in LogDB. A partial snapshot
	// must never be treated as a valid recovery point.
	snap, err := ldb.GetSnapshot(10, 1)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if snap.Index != 0 {
		t.Fatalf("logdb snapshot index = %d, want 0 (no snapshot persisted)", snap.Index)
	}

	// Verify the snapshot metadata file was NOT created. The metadata
	// file is written only after a successful save and fsync, so on
	// a mid-write error it must not exist.
	saveDir := snapshotSaveDir(tmpDir, 10, 1, 100)
	metaPath := filepath.Join(saveDir, snapshotMetadataFile)
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Fatalf("metadata file should not exist after save error: %s (err=%v)", metaPath, err)
	}

	// Verify the node's last snapshot index was NOT updated.
	if node.LastSnapshotIndex() != 0 {
		t.Fatalf("node.LastSnapshotIndex() = %d, want 0", node.LastSnapshotIndex())
	}
}

func TestSnapshotPool_GCSnapshotDirs_RemovesOrphanedDirs(t *testing.T) {
	tmpDir := t.TempDir()

	const shardID uint64 = 5
	const replicaID uint64 = 1

	// Create 3 valid snapshot directories with metadata files. These
	// represent properly completed snapshots that should be retained
	// by GC (maxSnapshotsRetained = 3).
	validIndices := []uint64{100, 200, 300}
	for _, idx := range validIndices {
		dir := snapshotSaveDir(tmpDir, shardID, replicaID, idx)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed for index %d: %v", idx, err)
		}
		if err := writeSnapshotMetadata(dir, idx, 1, shardID, replicaID, 0); err != nil {
			t.Fatalf("writeSnapshotMetadata failed for index %d: %v", idx, err)
		}
		// Write a data file to make it a complete snapshot directory.
		dataPath := filepath.Join(dir, snapshotDataFile)
		if err := os.WriteFile(dataPath, []byte{0x01, 0x02, 0x03}, 0o640); err != nil {
			t.Fatalf("write data file failed for index %d: %v", idx, err)
		}
	}

	// Create 2 orphaned snapshot directories. These simulate incomplete
	// writes from crashed or failed snapshot operations. They have the
	// standard snapshot-XXXXX naming but lack valid metadata or contain
	// only partial data. GC treats them as regular entries sorted by
	// index and removes the oldest when count exceeds maxSnapshotsRetained.
	orphanedIndices := []uint64{50, 75}
	for _, idx := range orphanedIndices {
		dir := snapshotSaveDir(tmpDir, shardID, replicaID, idx)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed for orphan index %d: %v", idx, err)
		}
		// Write a partial data file with no metadata, simulating an
		// incomplete snapshot that was interrupted before metadata
		// was written.
		partialPath := filepath.Join(dir, snapshotDataFile)
		if err := os.WriteFile(partialPath, []byte{0xDE, 0xAD}, 0o640); err != nil {
			t.Fatalf("write partial file failed for orphan index %d: %v", idx, err)
		}
	}

	// Verify we have 5 total snapshot directories before GC.
	dirsBefore, err := listSnapshotDirs(tmpDir, shardID, replicaID)
	if err != nil {
		t.Fatalf("listSnapshotDirs before GC failed: %v", err)
	}
	if len(dirsBefore) != 5 {
		t.Fatalf("expected 5 dirs before GC, got %d", len(dirsBefore))
	}

	// Run garbage collection.
	if err := gcSnapshotDirs(tmpDir, shardID, replicaID); err != nil {
		t.Fatalf("gcSnapshotDirs failed: %v", err)
	}

	// Verify that GC retained only maxSnapshotsRetained (3) directories.
	dirsAfter, err := listSnapshotDirs(tmpDir, shardID, replicaID)
	if err != nil {
		t.Fatalf("listSnapshotDirs after GC failed: %v", err)
	}
	if len(dirsAfter) != maxSnapshotsRetained {
		t.Fatalf("expected %d dirs after GC, got %d", maxSnapshotsRetained, len(dirsAfter))
	}

	// Verify the orphaned directories (indices 50 and 75) were removed
	// because they are the oldest entries. GC sorts by index ascending
	// and removes everything except the last maxSnapshotsRetained.
	for _, idx := range orphanedIndices {
		dir := snapshotSaveDir(tmpDir, shardID, replicaID, idx)
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("orphaned dir should be removed after GC: %s (err=%v)", dir, err)
		}
	}

	// Verify all 3 valid snapshot directories are retained with their
	// metadata and data files intact.
	for _, idx := range validIndices {
		dir := snapshotSaveDir(tmpDir, shardID, replicaID, idx)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Fatalf("valid snapshot dir should be retained: %s", dir)
		}
		metaPath := filepath.Join(dir, snapshotMetadataFile)
		if _, err := os.Stat(metaPath); os.IsNotExist(err) {
			t.Fatalf("metadata file should exist in retained dir: %s", metaPath)
		}
		dataPath := filepath.Join(dir, snapshotDataFile)
		if _, err := os.Stat(dataPath); os.IsNotExist(err) {
			t.Fatalf("data file should exist in retained dir: %s", dataPath)
		}
	}
}

// ---------------------------------------------------------------------------
// exportSnapshotToPath
// ---------------------------------------------------------------------------

func TestExportSnapshotToPath_Success(t *testing.T) {
	// Create a fake snapshot directory with data and metadata files.
	saveDir := t.TempDir()
	dataContent := []byte("snapshot-data-for-export-test")
	if err := os.WriteFile(filepath.Join(saveDir, snapshotDataFile), dataContent, 0o600); err != nil {
		t.Fatalf("write data file: %v", err)
	}
	metaContent := make([]byte, snapshotMetaSize)
	binary.LittleEndian.PutUint64(metaContent[0:], 100) // Index
	binary.LittleEndian.PutUint64(metaContent[8:], 5)   // Term
	if err := os.WriteFile(filepath.Join(saveDir, snapshotMetadataFile), metaContent, 0o600); err != nil {
		t.Fatalf("write metadata file: %v", err)
	}

	exportDir := filepath.Join(t.TempDir(), "export-dest")

	if err := exportSnapshotToPath(saveDir, exportDir); err != nil {
		t.Fatalf("exportSnapshotToPath failed: %v", err)
	}

	// Verify data file was copied.
	exported, readErr := os.ReadFile(filepath.Join(exportDir, snapshotDataFile))
	if readErr != nil {
		t.Fatalf("read exported data file: %v", readErr)
	}
	if string(exported) != string(dataContent) {
		t.Errorf("exported data = %q, want %q", string(exported), string(dataContent))
	}

	// Verify metadata file was copied.
	exportedMeta, metaErr := os.ReadFile(filepath.Join(exportDir, snapshotMetadataFile))
	if metaErr != nil {
		t.Fatalf("read exported metadata file: %v", metaErr)
	}
	if len(exportedMeta) != snapshotMetaSize {
		t.Errorf("exported metadata size = %d, want %d", len(exportedMeta), snapshotMetaSize)
	}
}

func TestExportSnapshotToPath_MissingSourceData(t *testing.T) {
	// saveDir exists but has no data file.
	saveDir := t.TempDir()
	exportDir := filepath.Join(t.TempDir(), "export-dest")

	err := exportSnapshotToPath(saveDir, exportDir)
	if err == nil {
		t.Fatal("exportSnapshotToPath should fail when source data file is missing")
	}
	var exportErr *SnapshotExportError
	if !errors.As(err, &exportErr) {
		t.Fatalf("expected *SnapshotExportError, got %T: %v", err, err)
	}
}

func TestExportSnapshotToPath_InvalidExportDir(t *testing.T) {
	// Create source data file.
	saveDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(saveDir, snapshotDataFile), []byte("data"), 0o600); err != nil {
		t.Fatalf("write data file: %v", err)
	}

	// Use a path that cannot be created (file exists where directory is needed).
	blockingFile := filepath.Join(t.TempDir(), "blocking-file")
	if err := os.WriteFile(blockingFile, []byte("block"), 0o600); err != nil {
		t.Fatalf("write blocking file: %v", err)
	}
	exportDir := filepath.Join(blockingFile, "subdir")

	err := exportSnapshotToPath(saveDir, exportDir)
	if err == nil {
		t.Fatal("exportSnapshotToPath should fail when export dir cannot be created")
	}
	var exportErr *SnapshotExportError
	if !errors.As(err, &exportErr) {
		t.Fatalf("expected *SnapshotExportError, got %T: %v", err, err)
	}
}

// ---------------------------------------------------------------------------
// saveSnapshot with per-request CompactionOverhead override
// ---------------------------------------------------------------------------

func TestSaveSnapshot_CompactionOverheadOverride(t *testing.T) {
	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	cfg.SetDefaults()

	snapshotter := &snapshotTestSnapshotter{}
	snapshotter.lastApplied.Store(200)
	snapshotter.state.Store(42)

	node := NewNode(nil, nil, nil, cfg, 100, snapshotter, snapDir, nil)

	ldb := newSnapshotTestLogDB()
	stopC := make(chan struct{})
	pool := newSnapshotPool(1, ldb, nil, nil, nil, stopC, nil)

	// Request with compaction override of 10 instead of shard config's 50.
	req := snapshotRequest{
		shardID:            10,
		replicaID:          1,
		node:               node,
		save:               true,
		compactionOverhead: 10,
		overrideCompaction: true,
	}

	index, err := pool.saveSnapshot(req)
	if err != nil {
		t.Fatalf("saveSnapshot failed: %v", err)
	}
	if index != 200 {
		t.Fatalf("snapshot index = %d, want 200", index)
	}

	// Verify that log compaction used the override value (10) not the config value (50).
	// With override=10: compactIndex = 200-10 = 190.
	// With config=50:   compactIndex = 200-50 = 150.
	// The mock logdb records the compact index.
	ldb.mu.Lock()
	removedTo := ldb.removedTo
	ldb.mu.Unlock()

	expectedCompactIdx := uint64(200 - 10)
	if removedTo != expectedCompactIdx {
		t.Errorf("RemoveEntriesTo called with %d, want %d (compaction overhead override should be 10, not 50)",
			removedTo, expectedCompactIdx)
	}
}

func TestSaveSnapshot_CompactionOverheadDefault(t *testing.T) {
	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	cfg.SetDefaults()

	snapshotter := &snapshotTestSnapshotter{}
	snapshotter.lastApplied.Store(200)
	snapshotter.state.Store(42)

	node := NewNode(nil, nil, nil, cfg, 100, snapshotter, snapDir, nil)

	ldb := newSnapshotTestLogDB()
	stopC := make(chan struct{})
	pool := newSnapshotPool(1, ldb, nil, nil, nil, stopC, nil)

	// Request WITHOUT compaction override should use config's 50.
	req := snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	}

	index, err := pool.saveSnapshot(req)
	if err != nil {
		t.Fatalf("saveSnapshot failed: %v", err)
	}
	if index != 200 {
		t.Fatalf("snapshot index = %d, want 200", index)
	}

	ldb.mu.Lock()
	removedTo := ldb.removedTo
	ldb.mu.Unlock()

	expectedCompactIdx := uint64(200 - 50)
	if removedTo != expectedCompactIdx {
		t.Errorf("RemoveEntriesTo called with %d, want %d (should use shard config overhead of 50)",
			removedTo, expectedCompactIdx)
	}
}

// ---------------------------------------------------------------------------
// saveSnapshot with ExportPath
// ---------------------------------------------------------------------------

func TestSaveSnapshot_WithExportPath(t *testing.T) {
	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	cfg.SetDefaults()

	snapshotter := &snapshotTestSnapshotter{}
	snapshotter.lastApplied.Store(100)
	snapshotter.state.Store(77)

	node := NewNode(nil, nil, nil, cfg, 100, snapshotter, snapDir, nil)

	ldb := newSnapshotTestLogDB()
	stopC := make(chan struct{})
	pool := newSnapshotPool(1, ldb, nil, nil, nil, stopC, nil)

	exportDir := filepath.Join(t.TempDir(), "exported")

	req := snapshotRequest{
		shardID:    10,
		replicaID:  1,
		node:       node,
		save:       true,
		exportPath: exportDir,
		exported:   true,
	}

	index, err := pool.saveSnapshot(req)
	if err != nil {
		t.Fatalf("saveSnapshot with export failed: %v", err)
	}
	if index != 100 {
		t.Fatalf("snapshot index = %d, want 100", index)
	}

	// Verify the export directory was created and the data file was copied.
	exportedData := filepath.Join(exportDir, snapshotDataFile)
	if _, statErr := os.Stat(exportedData); os.IsNotExist(statErr) {
		t.Fatal("exported snapshot data file does not exist at export path")
	}
}

func TestSaveSnapshot_WithExportPath_InvalidDir(t *testing.T) {
	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	cfg.SetDefaults()

	snapshotter := &snapshotTestSnapshotter{}
	snapshotter.lastApplied.Store(100)
	snapshotter.state.Store(77)

	node := NewNode(nil, nil, nil, cfg, 100, snapshotter, snapDir, nil)

	ldb := newSnapshotTestLogDB()
	stopC := make(chan struct{})
	pool := newSnapshotPool(1, ldb, nil, nil, nil, stopC, nil)

	// Use a blocking file path to force mkdir failure.
	blockingFile := filepath.Join(t.TempDir(), "block")
	if err := os.WriteFile(blockingFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocking file: %v", err)
	}
	exportDir := filepath.Join(blockingFile, "subdir")

	req := snapshotRequest{
		shardID:    10,
		replicaID:  1,
		node:       node,
		save:       true,
		exportPath: exportDir,
		exported:   true,
	}

	_, err := pool.saveSnapshot(req)
	if err == nil {
		t.Fatal("saveSnapshot with invalid export path should fail")
	}
}

// ---------------------------------------------------------------------------
// handleRequest panic recovery tests
// ---------------------------------------------------------------------------

// panickingSnapshotter is a Snapshotter that panics during SaveSnapshot
// and RecoverFromSnapshot.
type panickingSnapshotter struct{}

func (s *panickingSnapshotter) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	panic("snapshotter save panic")
}

func (s *panickingSnapshotter) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	panic("snapshotter recover panic")
}

func (s *panickingSnapshotter) LastApplied() uint64 { return 42 }

var _ Snapshotter = (*panickingSnapshotter)(nil)

// TestHandleRequest_PanicRecovery_SaveRequest verifies that handleRequest
// recovers from a panic in the save path, clears in-flight and snapshotting
// flags, and notifies the callback via OnSnapshotRejected.
func TestHandleRequest_PanicRecovery_SaveRequest(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	cb := newTestCallback()

	pool := newSnapshotPool(1, nil, nil, cb, ws, stopC, nil)

	var wg sync.WaitGroup
	pool.start(&wg)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	snapDir := t.TempDir()
	node := NewNode(nil, nil, nil, cfg, 100, &panickingSnapshotter{}, snapDir, nil)
	node.snapshotting.Store(true)

	err := pool.requestSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	// Wait for the request to be processed (panic recovered).
	deadline := time.After(5 * time.Second)
	for node.snapshotting.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for panic recovery")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	// snapshotting should be cleared.
	if node.snapshotting.Load() {
		t.Fatal("snapshotting should be cleared after panic recovery")
	}

	// OnSnapshotCompleted should NOT be called (panic path uses OnSnapshotRejected).
	if cb.snapshotCallCount.Load() != 0 {
		t.Fatal("OnSnapshotCompleted should not be called on panic path")
	}

	close(stopC)
	wg.Wait()
}

// TestHandleRequest_PanicRecovery_RecoverRequest verifies that handleRequest
// recovers from a panic in the recovery path, clears recovering and
// snapshotting flags, and notifies the work signal.
func TestHandleRequest_PanicRecovery_RecoverRequest(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	cb := newTestCallback()

	// Provide a logdb with a snapshot entry so recoverSnapshot proceeds
	// past the logdb == nil and Index == 0 guards and actually calls
	// RecoverFromSnapshot on the panicking snapshotter.
	ldb := newSnapshotTestLogDB()
	snapDir := t.TempDir()
	ldb.snapshots[1] = logdb.Snapshot{
		Index:    100,
		Term:     5,
		Filepath: snapDir,
	}

	// Create the snapshot data file that recoverSnapshot opens.
	dataPath := filepath.Join(snapDir, "snapshot.dat")
	if err := os.WriteFile(dataPath, []byte("fake-snapshot-data"), 0o644); err != nil {
		t.Fatalf("failed to create snapshot data file: %v", err)
	}

	pool := newSnapshotPool(1, ldb, nil, cb, ws, stopC, nil)

	var wg sync.WaitGroup
	pool.start(&wg)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, 100, &panickingSnapshotter{}, snapDir, nil)
	node.snapshotting.Store(true) // pre-set so we can poll for completion

	err := pool.requestSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      false, // recovery request
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	// Wait for the request to be processed. The observable signal is
	// that snapshotting is cleared (it was set by requestSnapshot).
	deadline := time.After(5 * time.Second)
	for node.snapshotting.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for panic recovery")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	// recovering should be cleared.
	if node.IsRecovering() {
		t.Fatal("recovering should be cleared after panic recovery")
	}

	// snapshotting should be cleared.
	if node.snapshotting.Load() {
		t.Fatal("snapshotting should be cleared after panic recovery")
	}

	// Work signal should have been notified for recovery requests.
	if !ws.HasWork(0) {
		t.Fatal("work signal should be notified after recovery panic")
	}

	close(stopC)
	wg.Wait()
}

// TestHandleRequest_CompletedCFull_DropsResult verifies that when the
// completedC channel is full, handleRequest drops the result with a
// warning instead of blocking.
func TestHandleRequest_CompletedCFull_DropsResult(t *testing.T) {
	stopC := make(chan struct{})
	ws := NewWorkSignal(1)
	cb := newTestCallback()

	pool := newSnapshotPool(1, nil, nil, cb, ws, stopC, nil)

	// Fill the completedC channel.
	for range cap(pool.completedC) {
		pool.completedC <- snapshotResult{}
	}

	var wg sync.WaitGroup
	pool.start(&wg)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, 100, nil, "", nil)

	// Submit a save request (no snapshotter -> will return error but
	// still try to send on completedC which is full).
	err := pool.requestSnapshot(snapshotRequest{
		shardID:   1,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	// Wait for callback to confirm processing.
	deadline := time.After(5 * time.Second)
	for cb.snapshotCallCount.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for snapshot completion callback")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	// The callback was called even though completedC is full.
	if cb.snapshotCallCount.Load() == 0 {
		t.Fatal("OnSnapshotCompleted should be called even when completedC is full")
	}

	close(stopC)
	wg.Wait()
}
