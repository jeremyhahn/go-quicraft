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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"

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

func (s *snapshotTestSnapshotter) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	return s.SaveSnapshotGated(ctx, w, stopper, nil)
}

func (s *snapshotTestSnapshotter) SaveSnapshotGated(_ context.Context, w io.Writer, _ <-chan struct{}, afterCapture func()) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], s.state.Load())
	_, err := w.Write(buf[:])
	if err == nil && afterCapture != nil {
		afterCapture()
	}
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
	// nodeInfos is what ListNodeInfo reports: every (shard, replica) with
	// durable state, loaded or not. Empty unless a test sets it.
	nodeInfos []logdb.NodeInfo
}

func newSnapshotTestLogDB() *snapshotTestLogDB {
	return &snapshotTestLogDB{
		snapshots: make(map[uint64]logdb.Snapshot),
	}
}

// AtRestProtection declares this test LogDB non-durable: it holds every record
// in a map and writes nothing to disk, so no durable encrypted record can be
// stranded by a DEK purge. The DECLARATION is what the purge floor keys on —
// silence is read as "durable and possibly encrypted" and fails safe.
func (db *snapshotTestLogDB) AtRestProtection() logdb.AtRestProtection {
	return logdb.AtRestNonDurable
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

// SaveSnapshot mirrors memdb and waldb: a snapshot whose index does not ADVANCE
// past the stored one is rejected with ErrSnapshotOutOfDate. Accepting it made
// the fixture disagree with every real implementation and let a test claim that
// a non-advancing export "already failed" when it in fact succeeded.
func (db *snapshotTestLogDB) SaveSnapshot(shardID, _ uint64, snap logdb.Snapshot) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if existing, ok := db.snapshots[shardID]; ok && snap.Index <= existing.Index {
		return logdb.ErrSnapshotOutOfDate
	}
	db.snapshots[shardID] = snap
	return nil
}

// GetSnapshot mirrors memdb and waldb: no record is ErrNoSnapshot, NOT a zero
// Snapshot with a nil error. Returning (zero, nil) made "no record yet" and "a
// record at index 0" indistinguishable, so no test ever reached the production
// first-save branch that has to tell them apart.
func (db *snapshotTestLogDB) GetSnapshot(shardID, _ uint64) (logdb.Snapshot, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	snap, ok := db.snapshots[shardID]
	if !ok {
		return logdb.Snapshot{}, logdb.ErrNoSnapshot
	}
	return snap, nil
}

func (db *snapshotTestLogDB) RemoveNodeData(_, _ uint64) error { return nil }

func (db *snapshotTestLogDB) NodeRemoved(_, _ uint64) (bool, error) { return false, nil }

func (db *snapshotTestLogDB) ListNodeInfo() ([]logdb.NodeInfo, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return append([]logdb.NodeInfo(nil), db.nodeInfos...), nil
}

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
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
	if result.shardID != 10 {
		t.Fatalf("result.shardID = %d, want 10", result.shardID)
	}
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	if result.index != 100 {
		t.Fatalf("result.index = %d, want 100", result.index)
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
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
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
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
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
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	if result.index != 100 {
		t.Fatalf("result.index = %d, want 100", result.index)
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
	node := NewNode(nil, nil, nil, cfg, ss, t.TempDir(), nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	if result.index != 0 {
		t.Fatalf("result.index = %d, want 0", result.index)
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
	saveNode := NewNode(nil, nil, nil, cfg, saveSS, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 5, replicaID: 2, node: saveNode, save: true,
	})
	if err != nil {
		t.Fatalf("save request failed: %v", err)
	}
	result := awaitSnapshotResult(t, p, "save")
	if result.err != nil {
		t.Fatalf("save failed: %v", result.err)
	}

	// Now recover into a fresh snapshotter.
	recoverSS := newSnapshotTestSnapshotter(0)
	recoverNode := NewNode(nil, nil, nil, cfg, recoverSS, tmpDir, nil)

	err = p.requestSnapshot(snapshotRequest{
		shardID: 5, replicaID: 2, node: recoverNode, save: false,
	})
	if err != nil {
		t.Fatalf("recover request failed: %v", err)
	}

	result = awaitSnapshotResult(t, p, "recover")
	if result.err != nil {
		t.Fatalf("recover failed: %v", result.err)
	}
	if result.shardID != 5 {
		t.Fatalf("result.shardID = %d, want 5", result.shardID)
	}
	if result.replicaID != 2 {
		t.Fatalf("result.replicaID = %d, want 2", result.replicaID)
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
	node := NewNode(nil, nil, nil, cfg, ss, t.TempDir(), nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 5, replicaID: 2, node: node, save: false,
	})
	if err != nil {
		t.Fatalf("recover request failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "recover")
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
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
	node := NewNode(nil, nil, nil, cfg, nil, t.TempDir(), nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 5, replicaID: 2, node: node, save: false,
	})
	if err != nil {
		t.Fatalf("recover request failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "recover")
	if result.err != nil {
		t.Fatalf("unexpected error for nil snapshotter: %v", result.err)
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
	node := NewNode(nil, nil, nil, cfg, ss, t.TempDir(), nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 5, replicaID: 2, node: node, save: false,
	})
	if err != nil {
		t.Fatalf("recover request failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "recover")
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
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
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

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
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	// First request.
	err := p.requestSnapshot(snapshotRequest{
		shardID: 10, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	awaitSnapshotResult(t, p, "first save")

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

	awaitSnapshotResult(t, p, "second save")
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
		node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)
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
		result := awaitSnapshotResult(t, p, "save")
		if result.err != nil {
			t.Fatalf("shard %d failed: %v", result.shardID, result.err)
		}
		results[result.shardID] = true
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
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

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

// orderedCallback records the order OnSnapshotCompleted and OnLogCompacted
// arrive in, on top of testCallback's own bookkeeping.
type orderedCallback struct {
	*testCallback
	mu    sync.Mutex
	order []string
}

func (c *orderedCallback) OnSnapshotCompleted(shardID uint64, index uint64, op SnapshotOperation, err error) {
	c.mu.Lock()
	c.order = append(c.order, "completed")
	c.mu.Unlock()
	c.testCallback.OnSnapshotCompleted(shardID, index, op, err)
}

func (c *orderedCallback) OnLogCompacted(shardID, replicaID uint64, compaction LogCompaction) {
	c.mu.Lock()
	c.order = append(c.order, "compacted")
	c.mu.Unlock()
	c.testCallback.OnLogCompacted(shardID, replicaID, compaction)
}

// TestSnapshotPool_DeliversLogCompactionAfterCompletion drives saves through
// the pool worker and pins the delivery contract listeners depend on:
// OnLogCompacted arrives once, AFTER OnSnapshotCompleted (so the snapshot is
// seen before the compaction it enabled), with the index compaction ran
// through — and not at all for a save within CompactionOverhead.
func TestSnapshotPool_DeliversLogCompactionAfterCompletion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		overhead uint64
		want     []compactionCall
		order    []string
	}{
		{"compacts past the overhead", 10,
			[]compactionCall{{shardID: 7, replicaID: 1, compaction: LogCompaction{Index: 32, Reader: true, LogDB: true}}},
			[]string{"completed", "compacted"}},
		{"within the overhead compacts nothing", 100, nil, []string{"completed"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stopC := make(chan struct{})
			defer close(stopC)
			cb := &orderedCallback{testCallback: newTestCallback()}
			p := newSnapshotPool(2, newSnapshotTestLogDB(), nil, cb, nil, stopC, nil)
			var wg sync.WaitGroup
			p.start(&wg)

			cfg := config.Config{ShardID: 7, ReplicaID: 1, CompactionOverhead: tc.overhead}
			node := NewNode(nil, nil, nil, cfg, newSnapshotTestSnapshotter(42), t.TempDir(), nil)
			// A reader that can really advance: compaction is reported only when
			// the save moves its marker.
			node.logReader = advancingReader(t, 7, 1, newSnapshotTestLogDB(), 42)
			if err := p.requestSnapshot(snapshotRequest{shardID: 7, replicaID: 1, node: node, save: true}); err != nil {
				t.Fatalf("requestSnapshot: %v", err)
			}
			awaitSnapshotResult(t, p, "snapshot")

			got := cb.getCompactionCalls()
			if len(got) != len(tc.want) {
				t.Fatalf("OnLogCompacted calls = %+v, want %+v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("OnLogCompacted[%d] = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
			cb.mu.Lock()
			order := append([]string(nil), cb.order...)
			cb.mu.Unlock()
			if len(order) != len(tc.order) {
				t.Fatalf("callback order = %v, want %v", order, tc.order)
			}
			for i := range order {
				if order[i] != tc.order[i] {
					t.Fatalf("callback order = %v, want %v", order, tc.order)
				}
			}
		})
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
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 7, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	awaitSnapshotResult(t, p, "snapshot")

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
		node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)
		err := p.requestSnapshot(snapshotRequest{
			shardID: 1, replicaID: 1, node: node, save: true,
		})
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		result := awaitSnapshotResult(t, p, "snapshot %d", i)
		if result.err != nil {
			t.Fatalf("snapshot %d failed: %v", i, result.err)
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
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 7, replicaID: 3, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
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

	meta := mustParseSnapshotMeta(t, data)
	if meta.Index != 42 {
		t.Fatalf("metadata index = %d, want 42", meta.Index)
	}
	if meta.ShardID != 7 {
		t.Fatalf("metadata shardID = %d, want 7", meta.ShardID)
	}
	if meta.ReplicaID != 3 {
		t.Fatalf("metadata replicaID = %d, want 3", meta.ReplicaID)
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

	err := gcSnapshotDirs(tmpDir, 1, 1, "")
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
	err := writeTestSnapshotMetadata(tmpDir, 42, 5, 10, 3, 0, config.NoCompression)
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

	meta := mustParseSnapshotMeta(t, data)
	if meta.Index != 42 {
		t.Fatal("index mismatch")
	}
	if meta.Term != 5 {
		t.Fatal("term mismatch")
	}
	if meta.ShardID != 10 {
		t.Fatal("shardID mismatch")
	}
	if meta.ReplicaID != 3 {
		t.Fatal("replicaID mismatch")
	}
	createdAt := int64(meta.CreatedAt)
	now := time.Now().Unix()
	if createdAt < now-5 || createdAt > now+5 {
		t.Fatalf("createdAt = %d, expected near %d", createdAt, now)
	}
}

func TestSnapshotPool_WriteSnapshotMetadata_InvalidDir(t *testing.T) {
	err := writeSnapshotMetadata("/nonexistent/dir/that/does/not/exist", 1, 1, 1, 1, 0, config.NoCompression)
	if err == nil {
		t.Fatal("expected error for invalid directory")
	}
}

// ---------------------------------------------------------------------------
// Barrier/encryption test helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Context-AAD support for the test barriers
//
// The real seal.Barrier folds the caller context into the AEAD AAD, so a frame
// sealed with one context cannot be opened with another. The XOR-based test
// barriers have no AEAD, so they stand in for that binding by appending a
// fixed-width tag derived from the context and rejecting a frame whose tag does
// not match the context offered at decryption. That is what makes the negative
// assertions (wrong ordinal / wrong index / wrong shard) meaningful here.
// ---------------------------------------------------------------------------

const testAADTagLen = 8

// testAADTag folds a frame context into a fixed-width tag.
func testAADTag(contextAAD []byte) [testAADTagLen]byte {
	h := fnv.New64a()
	_, _ = h.Write(contextAAD)
	var out [testAADTagLen]byte
	binary.BigEndian.PutUint64(out[:], h.Sum64())
	return out
}

// appendTestAADTag binds ciphertext to contextAAD.
func appendTestAADTag(ciphertext, contextAAD []byte) []byte {
	tag := testAADTag(contextAAD)
	return append(append([]byte(nil), ciphertext...), tag[:]...)
}

// stripTestAADTag verifies the binding and returns the framed ciphertext.
func stripTestAADTag(ciphertext, contextAAD []byte) ([]byte, error) {
	if len(ciphertext) < testAADTagLen {
		return nil, errors.New("ciphertext too short for context tag")
	}
	body := ciphertext[:len(ciphertext)-testAADTagLen]
	want := testAADTag(contextAAD)
	if !bytes.Equal(ciphertext[len(body):], want[:]) {
		return nil, errors.New("context AAD mismatch")
	}
	return body, nil
}

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

func (b *testBarrier) EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpoch(dst, plaintext)
	if err != nil {
		return nil, 0, err
	}
	return appendTestAADTag(ct, contextAAD), epoch, nil
}

func (b *testBarrier) DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error) {
	body, err := stripTestAADTag(ciphertext, contextAAD)
	if err != nil {
		return nil, err
	}
	return b.Decrypt(dst, body)
}

// IsHardwareMode is false: the test barrier binds context, like the software
// seal.Barrier it stands in for.
func (b *testBarrier) IsHardwareMode() bool { return false }

// SupportsContextAAD is true: this barrier authenticates the caller context.
func (b *testBarrier) SupportsContextAAD() bool { return true }

func (b *testBarrier) encryptForEpoch(plaintext []byte, epoch uint64) []byte {
	key := byte(epoch & 0xFF)
	out := make([]byte, 8+len(plaintext))
	binary.LittleEndian.PutUint64(out[:8], epoch)
	for i, v := range plaintext {
		out[8+i] = v ^ key
	}
	return out
}

// sealFrameAt builds one snapshot frame exactly as the frame writer would:
// sealed at the given epoch and bound to the artifact identity
// (shardID, replicaID, index), the frame's 0-based ordinal, and whether it is
// the artifact's terminal frame. Tests that hand-assemble a snapshot.dat must
// use it, or the frames carry no positional binding and the decrypt side
// correctly rejects them.
func (b *testBarrier) sealFrameAt(plaintext []byte, epoch, shardID, replicaID, index, seq uint64, final bool) []byte {
	aad := NewSnapshotAAD(shardID, replicaID, index, aadCapability{supports: true})
	return appendTestAADTag(b.encryptForEpoch(plaintext, epoch), aad.Frame(seq, final))
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

func (b *testBarrier) PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error) {
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
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if b.calls > b.failAfter {
		return nil, errors.New("injected encrypt failure")
	}
	return b.encryptForEpoch(plaintext, b.epoch), nil
}

func (b *failAfterNBarrier) EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error) {
	ct, err := b.Encrypt(dst, plaintext)
	if err != nil {
		return nil, 0, err
	}
	return ct, b.epoch, nil
}

func (b *failAfterNBarrier) EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpoch(dst, plaintext)
	if err != nil {
		return nil, 0, err
	}
	return appendTestAADTag(ct, contextAAD), epoch, nil
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
// I.3: Snapshot save/recover round-trip over the artifact matrix
//
// saveSnapshot builds its writer chain as
//
//	snapshotter -> [snappy.NewBufferedWriter] -> file -> [encrypt in place]
//
// and recoverSnapshot must mirror it in reverse: decrypt to a sibling file,
// then wrap the plaintext in snappy.NewReader. The plaintext + uncompressed
// cell is the ONE configuration in which a missing decrypt AND a missing
// decompress are both invisible, so the round trip is driven over the whole
// {plaintext, encrypted} x {uncompressed, snappy} matrix instead.
// ---------------------------------------------------------------------------

// snapshotRoundTripVariants is the at-rest artifact matrix the runtime
// save/recover path is exercised over.
var snapshotRoundTripVariants = []struct {
	name      string
	encrypted bool
	snappy    bool
}{
	{name: "plaintext_uncompressed"},
	{name: "plaintext_snappy", snappy: true},
	{name: "encrypted_uncompressed", encrypted: true},
	{name: "encrypted_snappy", encrypted: true, snappy: true},
}

// snappyFrame returns data as snappy.NewBufferedWriter would have written it,
// which is exactly what saveSnapshot puts on disk before encryption when the
// shard configures compression.
func snappyFrame(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := snappy.NewBufferedWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("snappy write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("snappy close: %v", err)
	}
	return buf.Bytes()
}

// TestSnapshotPool_SaveAndRecoverRoundTrip drives the runtime snapshot path
// through every at-rest artifact configuration: save, assert the on-disk
// artifact is exactly what the writer chain should have produced (and that the
// LogDB/metadata epoch records whether it is encrypted), then recover into a
// fresh snapshotter and assert the state round-tripped.
func TestSnapshotPool_SaveAndRecoverRoundTrip(t *testing.T) {
	const barrierEpoch = uint64(5)
	for _, v := range snapshotRoundTripVariants {
		t.Run(v.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			stopC := make(chan struct{})
			defer close(stopC)

			ldb := newSnapshotTestLogDB()
			var barrier SnapshotBarrier
			var wantEpoch uint64
			if v.encrypted {
				barrier = newTestBarrier(barrierEpoch)
				wantEpoch = barrierEpoch
			}
			p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

			var wg sync.WaitGroup
			p.start(&wg)

			cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
			if v.snappy {
				cfg.SnapshotCompressionType = config.Snappy
			}

			ss := newSnapshotTestSnapshotter(100)
			saveNode := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

			if err := p.requestSnapshot(snapshotRequest{
				shardID: 10, replicaID: 1, node: saveNode, save: true,
			}); err != nil {
				t.Fatalf("save request failed: %v", err)
			}
			result := awaitSnapshotResult(t, p, "save")
			if result.err != nil {
				t.Fatalf("save failed: %v", result.err)
			}
			if result.index != 100 {
				t.Fatalf("result.index = %d, want 100", result.index)
			}

			// The LogDB record carries the at-rest epoch, and recovery keys off
			// it: non-zero exactly when the artifact was encrypted.
			snap, err := ldb.GetSnapshot(10, 1)
			if err != nil {
				t.Fatalf("GetSnapshot failed: %v", err)
			}
			if snap.Epoch != wantEpoch {
				t.Fatalf("logdb snapshot epoch = %d, want %d", snap.Epoch, wantEpoch)
			}

			// The on-disk artifact must be exactly what the writer chain
			// produces for this cell: the snapshotter's bytes, Snappy-framed
			// when compression is on, then encrypted when a barrier is wired.
			saveDir := snapshotSaveDir(tmpDir, 10, 1, 100)
			onDisk, readErr := os.ReadFile(filepath.Join(saveDir, snapshotDataFile))
			if readErr != nil {
				t.Fatalf("failed to read snapshot data: %v", readErr)
			}
			raw := make([]byte, 8)
			binary.LittleEndian.PutUint64(raw, 100)
			preEncrypt := raw
			if v.snappy {
				preEncrypt = snappyFrame(t, raw)
			}
			if v.encrypted {
				if bytes.Contains(onDisk, preEncrypt) {
					t.Fatal("data file holds the pre-encryption bytes; it was not encrypted at rest")
				}
			} else if !bytes.Equal(onDisk, preEncrypt) {
				t.Fatalf("data file = %x, want the writer chain's output %x", onDisk, preEncrypt)
			}

			// The metadata mirrors the LogDB epoch so a recovery that cannot
			// reach LogDB still knows whether the artifact is ciphertext.
			metaData, metaErr := os.ReadFile(filepath.Join(saveDir, snapshotMetadataFile))
			if metaErr != nil {
				t.Fatalf("failed to read metadata: %v", metaErr)
			}
			if len(metaData) != snapshotMetaSize {
				t.Fatalf("metadata size = %d, want %d", len(metaData), snapshotMetaSize)
			}
			if metaEpoch := mustParseSnapshotMeta(t, metaData).Epoch; metaEpoch != wantEpoch {
				t.Fatalf("metadata epoch = %d, want %d", metaEpoch, wantEpoch)
			}

			// Recover into a fresh snapshotter through the same barrier and the
			// same compression setting: the reader chain must undo the writer
			// chain exactly.
			recoverSS := newSnapshotTestSnapshotter(0)
			recoverNode := NewNode(nil, nil, nil, cfg, recoverSS, tmpDir, nil)

			if err := p.requestSnapshot(snapshotRequest{
				shardID: 10, replicaID: 1, node: recoverNode, save: false,
			}); err != nil {
				t.Fatalf("recover request failed: %v", err)
			}
			result = awaitSnapshotResult(t, p, "recover")
			if result.err != nil {
				t.Fatalf("recover failed: %v", result.err)
			}

			if recoverSS.state.Load() != 100 {
				t.Fatalf("recovered state = %d, want 100", recoverSS.state.Load())
			}
			if recoverNode.lastApplied.Load() != 100 {
				t.Fatalf("node.lastApplied = %d, want 100", recoverNode.lastApplied.Load())
			}

			// The decrypt scratch sibling must not survive recovery.
			assertNoTempFiles(t, saveDir)
		})
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
	sealSnapshotFile(t, dataPath, original, 1, 1, 1, barrier)

	// Verify encrypted file differs from original.
	encrypted, _ := os.ReadFile(dataPath)
	if len(encrypted) == len(original) {
		t.Fatal("encrypted file should be larger than original due to framing")
	}

	// Decrypt to a sibling temp file; the source dataPath stays encrypted.
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 3, barrier)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(plainPath) })

	// Verify the decrypted sibling content matches the original plaintext.
	decrypted, _ := os.ReadFile(plainPath)
	if len(decrypted) != len(original) {
		t.Fatalf("decrypted length = %d, want %d", len(decrypted), len(original))
	}
	for i := range original {
		if decrypted[i] != original[i] {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, decrypted[i], original[i])
		}
	}
}

// TestSnapshotPool_DecryptToSiblingDoesNotMutateEncryptedSource is the
// regression guard for the crash/retry-safety fix. The old in-place
// decryptSnapshotFile renamed plaintext over snapshot.dat, so a recovery
// failure (or crash) left plaintext on disk while LogDB still recorded
// Epoch>0 — every later recovery would then re-decrypt plaintext and fail
// permanently. decryptSnapshotToScratch decrypts to a throwaway sibling and
// leaves the canonical encrypted file untouched, so it must:
//  1. leave snapshot.dat byte-for-byte unchanged (still ciphertext),
//  2. return the plaintext in a distinct sibling file, and
//  3. be safe to run again on the same source (retry-safe) — which an
//     in-place decrypt would fail, because the source would then be plaintext.
func TestSnapshotPool_DecryptToSiblingDoesNotMutateEncryptedSource(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(7)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Write a multi-chunk plaintext snapshot, then encrypt it in place so
	// dataPath holds the canonical ciphertext (as saveSnapshot would leave it).
	dataPath := filepath.Join(tmpDir, snapshotDataFile)
	original := make([]byte, snapshotEncryptChunkSize*2+321)
	for i := range original {
		original[i] = byte((i * 7) % 251)
	}
	sealSnapshotFile(t, dataPath, original, 1, 1, 1, barrier)

	// Capture the encrypted bytes for an exact before/after comparison.
	ciphertextBefore, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("read encrypted source failed: %v", err)
	}
	if len(ciphertextBefore) <= len(original) {
		t.Fatal("encrypted source should be larger than plaintext due to framing")
	}

	// First decryption: must produce a distinct sibling containing the plaintext.
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 7, barrier)
	if err != nil {
		t.Fatalf("first decrypt failed: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(plainPath) })
	if plainPath == dataPath {
		t.Fatal("sibling path must differ from the source path")
	}

	plain, err := os.ReadFile(plainPath)
	if err != nil {
		t.Fatalf("read decrypted sibling failed: %v", err)
	}
	if len(plain) != len(original) {
		t.Fatalf("decrypted length = %d, want %d", len(plain), len(original))
	}
	for i := range original {
		if plain[i] != original[i] {
			t.Fatalf("decrypted mismatch at byte %d: got %d, want %d", i, plain[i], original[i])
		}
	}

	// The source snapshot.dat must be byte-for-byte unchanged (still ciphertext).
	ciphertextAfter, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("read source after decrypt failed: %v", err)
	}
	if !bytes.Equal(ciphertextBefore, ciphertextAfter) {
		t.Fatal("source snapshot.dat was mutated by decryptSnapshotToScratch")
	}

	// Retry-safety: a second decrypt of the same (still encrypted) source must
	// also succeed. An in-place decrypt would have left the source plaintext,
	// so this second decrypt would fail.
	plainPath2, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 7, barrier)
	if err != nil {
		t.Fatalf("second (retry) decrypt failed: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(plainPath2) })
	if plainPath2 == plainPath {
		t.Fatal("each decrypt must produce its own sibling temp file")
	}

	plain2, err := os.ReadFile(plainPath2)
	if err != nil {
		t.Fatalf("read second decrypted sibling failed: %v", err)
	}
	if !bytes.Equal(plain, plain2) {
		t.Fatal("retry decrypt produced different plaintext")
	}
}

// rotateAfterNBarrier wraps testBarrier and advances the barrier epoch by one
// after the first N EncryptWithEpoch calls succeed. This deterministically
// simulates a key rotation that lands between snapshot chunks: the first N
// frames are sealed at epoch E, every later frame at epoch E+1, producing a
// genuinely mixed-epoch encrypted snapshot file via the real save path.
type rotateAfterNBarrier struct {
	*testBarrier
	rotateAfter int
	calls       int
}

func (b *rotateAfterNBarrier) EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error) {
	b.mu.Lock()
	if b.sealed {
		b.mu.Unlock()
		return nil, 0, errors.New("barrier sealed")
	}
	epoch := b.epoch
	ct := b.encryptForEpoch(plaintext, epoch)
	b.calls++
	if b.calls == b.rotateAfter {
		// Rotate: every subsequent frame is sealed at the next epoch.
		b.epoch++
	}
	b.mu.Unlock()
	return ct, epoch, nil
}

func (b *rotateAfterNBarrier) EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpoch(dst, plaintext)
	if err != nil {
		return nil, 0, err
	}
	return appendTestAADTag(ct, contextAAD), epoch, nil
}

var _ SnapshotBarrier = (*rotateAfterNBarrier)(nil)

// TestSnapshotPool_MixedEpochSnapshotDecryptsAllFrames is the regression test
// for the rotation-during-save data-durability bug. A snapshot saved while a
// key rotation runs concurrently can contain frames sealed at different epochs
// (earlier chunks at E, later chunks at E+1). The fix decrypts each frame with
// the epoch embedded in its own ciphertext header (barrier.Decrypt) rather than
// the single recorded snapshot epoch, and records the MINIMUM frame epoch so
// the DEK-purge floor protects every frame.
//
// This test builds a single file containing frames at epoch E and E+1, then
// asserts decryptSnapshotToScratch recovers ALL frames correctly. Under the old
// behavior (DecryptForEpoch with one recorded epoch), the frame whose true
// epoch differs from the recorded epoch would fail AEAD and brick the snapshot.
func TestSnapshotPool_MixedEpochSnapshotDecryptsAllFrames(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Two distinct plaintext chunks; frame 1 sealed at epoch 5, frame 2 at 6.
	chunk1 := bytes.Repeat([]byte{0xA1}, 128)
	chunk2 := bytes.Repeat([]byte{0xB2}, 200)
	enc1 := barrier.sealFrameAt(chunk1, 5, 1, 1, 1, 0, false)
	enc2 := barrier.sealFrameAt(chunk2, 6, 1, 1, 1, 1, true)

	dataPath := filepath.Join(tmpDir, snapshotDataFile)
	var fileData []byte
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(enc1)))
	fileData = append(fileData, lenBuf[:]...)
	fileData = append(fileData, enc1...)
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(enc2)))
	fileData = append(fileData, lenBuf[:]...)
	fileData = append(fileData, enc2...)
	if err := os.WriteFile(dataPath, fileData, 0o600); err != nil {
		t.Fatalf("write mixed-epoch data failed: %v", err)
	}

	// Decrypt using the MINIMUM recorded epoch (5). Per-frame embedded-epoch
	// decryption must recover both frames despite frame 2 being at epoch 6.
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 5, barrier)
	if err != nil {
		t.Fatalf("mixed-epoch decrypt failed (per-frame epoch not honored): %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(plainPath) })

	plain, readErr := os.ReadFile(plainPath)
	if readErr != nil {
		t.Fatalf("read decrypted sibling failed: %v", readErr)
	}
	want := append(append([]byte{}, chunk1...), chunk2...)
	if !bytes.Equal(plain, want) {
		t.Fatalf("recovered plaintext mismatch: got %d bytes, want %d", len(plain), len(want))
	}
}

// TestSnapshotPool_EncryptFile_RecordsMinEpochAcrossRotation verifies the save
// path records the MINIMUM frame epoch (not a post-hoc CurrentEpoch read) when
// a rotation lands mid-save, and that the resulting mixed-epoch file round-trips
// through decryptSnapshotToScratch using only that minimum epoch.
func TestSnapshotPool_EncryptFile_RecordsMinEpochAcrossRotation(t *testing.T) {
	tmpDir := t.TempDir()
	// Rotate after the first chunk: frame 1 at epoch 5, all later frames at 6.
	barrier := &rotateAfterNBarrier{
		testBarrier: newTestBarrier(5),
		rotateAfter: 1,
	}
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	// Multi-chunk plaintext so the rotation lands between frames.
	original := make([]byte, snapshotEncryptChunkSize*2+777)
	for i := range original {
		original[i] = byte((i * 3) % 251)
	}
	dataPath := filepath.Join(tmpDir, snapshotDataFile)
	minEpoch := sealSnapshotFile(t, dataPath, original, 1, 1, 1, barrier)
	// First frame was sealed at epoch 5 (before the rotation), so the recorded
	// epoch must be the minimum, 5 — NOT the post-rotation CurrentEpoch of 6.
	if minEpoch != 5 {
		t.Fatalf("recorded epoch = %d, want min frame epoch 5", minEpoch)
	}
	if barrier.CurrentEpoch() != 6 {
		t.Fatalf("expected barrier to have rotated to epoch 6, got %d", barrier.CurrentEpoch())
	}

	// The mixed-epoch file must round-trip using only the recorded min epoch.
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, minEpoch, barrier)
	if err != nil {
		t.Fatalf("decrypt of mixed-epoch file failed: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(plainPath) })

	plain, readErr := os.ReadFile(plainPath)
	if readErr != nil {
		t.Fatalf("read decrypted sibling failed: %v", readErr)
	}
	if !bytes.Equal(plain, original) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(plain), len(original))
	}
}

func TestSnapshotPool_DecryptFile_PreservesSourceOnFailure(t *testing.T) {
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
	enc1 := barrier.sealFrameAt(chunk1, 1, 1, 1, 1, 0, false)
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

	// Decrypt should fail on the corrupt second frame and return an empty
	// path (decryptSnapshotToScratch never mutates the source).
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 1, barrier)
	if err == nil {
		t.Fatal("expected decrypt to fail on corrupt frame")
	}
	if plainPath != "" {
		t.Fatalf("expected empty path on error, got %q", plainPath)
	}
	var encErr *SnapshotEncryptError
	if !errors.As(err, &encErr) {
		t.Fatalf("expected SnapshotEncryptError, got %T: %v", err, err)
	}

	// Encrypted source file must be preserved intact.
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

	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 1, barrier)
	if err == nil {
		t.Fatal("expected error for corrupt frame")
	}
	if plainPath != "" {
		t.Fatalf("expected empty path on error, got %q", plainPath)
	}
	var encErr *SnapshotEncryptError
	if !errors.As(err, &encErr) {
		t.Fatalf("expected SnapshotEncryptError, got %T: %v", err, err)
	}
}

// TestSnapshotPool_EncryptFile_SealedBarrier pins the fail-closed save.
//
// Asserting the opposite — "even with barrier sealed, save should work
// (encryption is skipped)" and epoch == 0 — would encode a plaintext-downgrade
// defect rather than a requirement: a configured barrier that is sealed means encryption is UNAVAILABLE, not disabled, so a snapshot
// written then would be durable plaintext for a shard required to be encrypted
// at rest. The assertion is inverted deliberately.
func TestSnapshotPool_EncryptFile_SealedBarrier(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	barrier.sealed = true
	stopC := make(chan struct{})
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	var wg sync.WaitGroup
	p.start(&wg)

	ss := newSnapshotTestSnapshotter(50)
	cfg := config.Config{ShardID: 1, ReplicaID: 1, CompactionOverhead: 10}
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID: 1, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
	if !errors.Is(result.err, ErrSnapshotBarrierSealed) {
		t.Fatalf("save error = %v, want ErrSnapshotBarrierSealed", result.err)
	}

	if snap, _ := ldb.GetSnapshot(1, 1); snap.Index != 0 {
		t.Fatalf("LogDB recorded a snapshot at index %d while the barrier was sealed", snap.Index)
	}
	assertNoSnapshotArtifact(t, tmpDir)
}

// ---------------------------------------------------------------------------
// I.4: Key rotation tests
// ---------------------------------------------------------------------------

func TestSnapshotPool_EpochInMetadata(t *testing.T) {
	tmpDir := t.TempDir()

	// Write metadata with epoch.
	err := writeTestSnapshotMetadata(tmpDir, 100, 5, 10, 1, 7, config.NoCompression)
	if err != nil {
		t.Fatalf("writeSnapshotMetadata failed: %v", err)
	}

	// Read and verify epoch (status=known: metadata parsed cleanly).
	epoch, status := readSnapshotEpoch(tmpDir)
	if status != snapshotEpochKnown || epoch != 7 {
		t.Fatalf("readSnapshotEpoch = (%d, %v), want (7, known)", epoch, status)
	}
}

func TestSnapshotPool_ReadSnapshotEpoch_ShortFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Write a 40-byte pre-magic record: the layout earlier builds' snapshot
	// RECEIVE path wrote, which carries no epoch field at all. The epoch is
	// therefore UNKNOWN, and under the fail-safe contract the purge-floor
	// callers must refuse to purge rather than drop this snapshot from the
	// floor. It is reported as legacy (a recognised record), not as corrupt.
	metaPath := filepath.Join(tmpDir, snapshotMetadataFile)
	var buf [40]byte
	binary.LittleEndian.PutUint64(buf[0:], 100) // Index
	binary.LittleEndian.PutUint64(buf[8:], 5)   // Term
	if err := os.WriteFile(metaPath, buf[:], 0o644); err != nil {
		t.Fatalf("write short metadata failed: %v", err)
	}

	epoch, status := readSnapshotEpoch(tmpDir)
	if status == snapshotEpochKnown {
		t.Fatalf("readSnapshotEpoch for legacy epoch-less file = (%d, known), "+
			"want a fail-safe status", epoch)
	}
	if status != snapshotEpochLegacy {
		t.Fatalf("readSnapshotEpoch status = %v, want legacy", status)
	}
}

func TestSnapshotPool_ReadSnapshotEpoch_MissingFile(t *testing.T) {
	tmpDir := t.TempDir()
	// A missing metadata file with NO artifact beside it (snapshot GC'd
	// between enumeration and read) is a benign skip: (0, known).
	epoch, status := readSnapshotEpoch(tmpDir)
	if status != snapshotEpochKnown || epoch != 0 {
		t.Fatalf("readSnapshotEpoch for missing file = (%d, %v), want (0, known)", epoch, status)
	}
}

// TestSnapshotPool_ReadSnapshotEpoch_ArtifactWithoutRecord pins the distinction
// the missing-record case must not erase: "the METADATA is absent" is strictly
// weaker than "the ARTIFACT is absent", and only the latter means the directory
// imposes no purge floor.
//
// A snapshot.dat with no snapshot.meta is producible by any crash between the
// two writes. Its bytes may be ciphertext at any epoch and the record that
// would have said which is exactly the one that is missing, so reporting "no
// floor" lets the next PurgeEpochsBefore destroy the DEK it needs — the
// artifact becomes permanently undecryptable and StartShard fails closed on it.
func TestSnapshotPool_ReadSnapshotEpoch_ArtifactWithoutRecord(t *testing.T) {
	tmpDir := t.TempDir()
	dataPath := filepath.Join(tmpDir, snapshotDataFile)
	if err := os.WriteFile(dataPath, []byte("ciphertext-at-an-unknown-epoch"), 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	epoch, status := readSnapshotEpoch(tmpDir)
	if status != snapshotEpochUnknown {
		t.Fatalf("readSnapshotEpoch with snapshot.dat present and snapshot.meta absent = (%d, %v), "+
			"want (0, unknown): an unevaluable directory can only mean 'do not purge'",
			epoch, status)
	}
	if epoch != 0 {
		t.Fatalf("readSnapshotEpoch epoch = %d, want 0 alongside an unknown status", epoch)
	}
}

// TestSnapshotPool_PurgeFloor_ArtifactWithoutRecordBlocksPurge asserts the
// CONSEQUENCE of the classification above: the DEK purge is suppressed, so the
// key the orphaned artifact may need survives.
func TestSnapshotPool_PurgeFloor_ArtifactWithoutRecordBlocksPurge(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(9)
	stopC := make(chan struct{})
	p := newSnapshotPool(1, newSnapshotTestLogDB(), barrier, nil, nil, stopC, nil)
	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

	// A well-formed recent snapshot at a high epoch, which on its own would put
	// the floor at 7 and purge every older DEK.
	writeShardSnapshot(t, tmpDir, 1, 1, 200, 7)

	// An older directory left half-built by a crash: artifact, no record.
	orphanDir := snapshotSaveDir(tmpDir, 1, 1, 100)
	if err := os.MkdirAll(orphanDir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(orphanDir, snapshotDataFile),
		[]byte("ciphertext-at-an-unknown-epoch"), 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	if floor, ok := p.globalSafePurgeFloor(); ok {
		t.Fatalf("globalSafePurgeFloor = (%d, true) with an artifact whose epoch cannot be "+
			"established; want ok=false so no DEK is purged", floor)
	}
}

// ---------------------------------------------------------------------------
// Global safe-purge floor test helpers
// ---------------------------------------------------------------------------

// snapshotDirNode builds a minimal *Node whose snapshot directory and IDs
// the global-floor scan can read. It carries no peer, SM, or log reader.
func snapshotDirNode(baseDir string, shardID, replicaID uint64) *Node {
	return NewNode(nil, nil, nil,
		config.Config{ShardID: shardID, ReplicaID: replicaID},
		nil, baseDir, nil)

}

// wireSnapshotNodes installs a node enumerator on the pool so the global
// safe-purge floor can scan the given nodes' snapshot directories. Tests
// that exercise purgeOldEpochs/globalSafePurgeFloor must wire this, since a
// nil enumerator fails safe (no purge).
func wireSnapshotNodes(p *snapshotPool, nodes ...*Node) {
	p.nodes = func() []*Node { return nodes }
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
	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

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
		if err := writeTestSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch, config.NoCompression); err != nil {
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
	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

	// Create snapshots with epoch=0 (plaintext).
	for _, idx := range []uint64{100, 200} {
		dir := snapshotSaveDir(tmpDir, 1, 1, idx)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := writeTestSnapshotMetadata(dir, idx, 1, 1, 1, 0, config.NoCompression); err != nil {
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
	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

	var wg sync.WaitGroup
	p.start(&wg)

	// Save 4 snapshots. GC retains 3 (maxSnapshotsRetained).
	for i := uint64(1); i <= 4; i++ {
		ss := newSnapshotTestSnapshotter(i * 100)
		cfg := config.Config{ShardID: 1, ReplicaID: 1, CompactionOverhead: 10}
		node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)
		err := p.requestSnapshot(snapshotRequest{
			shardID: 1, replicaID: 1, node: node, save: true,
		})
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		result := awaitSnapshotResult(t, p, "snapshot %d", i)
		if result.err != nil {
			t.Fatalf("save %d failed: %v", i, result.err)
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
	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

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
		if err := writeTestSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch, config.NoCompression); err != nil {
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
	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

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
		if err := writeTestSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch, config.NoCompression); err != nil {
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
	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

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
		if err := writeTestSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch, config.NoCompression); err != nil {
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
	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

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
		if err := writeTestSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch, config.NoCompression); err != nil {
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
	// Scenario: LogDB does not implement EpochAwareLogDB and DECLARES itself
	// non-durable (e.g., MemDB). No durable artifact means no DEK can be
	// stranded, so the floor falls back to the snapshot-only epoch floor.
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	ldb := newSnapshotTestLogDB() // does NOT implement EpochAwareLogDB
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)
	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

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
		if err := writeTestSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch, config.NoCompression); err != nil {
			t.Fatalf("write metadata failed: %v", err)
		}
	}

	p.purgeOldEpochs(tmpDir, 1, 1)

	// Without EpochAwareLogDB, purge floor is snapshot min (2).
	if barrier.PurgedBefore() != 2 {
		t.Fatalf("purgedBefore = %d, want 2", barrier.PurgedBefore())
	}
}

// durableEncryptedTestLogDB is a non-epoch-aware LogDB that DECLARES its
// records durable and barrier-encrypted. It cannot report a WAL epoch floor, so
// the purge floor has no way to know which DEKs its live records still need.
type durableEncryptedTestLogDB struct {
	snapshotTestLogDB
}

func (db *durableEncryptedTestLogDB) AtRestProtection() logdb.AtRestProtection {
	return logdb.AtRestBarrierEncrypted
}

// unprotectedTestLogDB declares NOTHING. It embeds the logdb.LogDB INTERFACE
// rather than the concrete store, so only the interface's methods are in its
// method set and logdb.AtRestProtectedLogDB is not satisfied. Silence must read
// as "durable, encrypted, forgot to declare" -- the exact hazard -- and block
// the purge.
type unprotectedTestLogDB struct {
	logdb.LogDB
}

// TestSnapshotPool_PurgeOldEpochs_DurableNonEpochAwareLogDBBlocksPurge is the
// A non-nil LogDB that cannot report an epoch floor must not contribute
// walMinEpoch=0 silently, which would let the purge proceed on the snapshot
// floor alone and IRREVERSIBLY destroy DEKs a durable, encrypted log may
// still need. Only a positive AtRestNonDurable declaration proves otherwise;
// anything else -- including AtRestUnprotected and total silence -- must fail
// safe (ok == false, nothing purged).
func TestSnapshotPool_PurgeOldEpochs_DurableNonEpochAwareLogDBBlocksPurge(t *testing.T) {
	for _, tc := range []struct {
		name string
		ldb  logdb.LogDB
	}{
		{"barrier_encrypted", &durableEncryptedTestLogDB{
			snapshotTestLogDB: snapshotTestLogDB{snapshots: make(map[uint64]logdb.Snapshot)}}},
		{"undeclared", &unprotectedTestLogDB{LogDB: newSnapshotTestLogDB()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			barrier := newTestBarrier(5)
			stopC := make(chan struct{})
			p := newSnapshotPool(2, tc.ldb, barrier, nil, nil, stopC, nil)
			wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

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
				if err := writeTestSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch,
					config.NoCompression); err != nil {
					t.Fatalf("write metadata failed: %v", err)
				}
			}

			if _, ok := p.globalSafePurgeFloor(); ok {
				t.Fatal("globalSafePurgeFloor reported ok=true for a LogDB that neither " +
					"reports an epoch floor nor declares its records non-durable")
			}

			p.purgeOldEpochs(tmpDir, 1, 1)
			if barrier.PurgeCallCount() != 0 {
				t.Fatalf("purge ran %d time(s); a durable non-epoch-aware LogDB must "+
					"suppress it", barrier.PurgeCallCount())
			}
			if barrier.PurgedBefore() != 0 {
				t.Fatalf("purgedBefore = %d, want 0", barrier.PurgedBefore())
			}
		})
	}
}

func TestSnapshotPool_PurgeOldEpochs_NilLogDB(t *testing.T) {
	// Scenario: LogDB is nil. Should still work with snapshot-only logic.
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)
	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

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
		if err := writeTestSnapshotMetadata(dir, info.index, 1, 1, 1, info.epoch, config.NoCompression); err != nil {
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
// Global safe-purge floor: cross-shard safety regression tests
// ---------------------------------------------------------------------------

// TestSnapshotPool_PurgeOldEpochs_CrossShardFloor proves the data-loss bug
// is fixed: two shards share one barrier, shard A retains a snapshot at a
// LOW epoch (2) while shard B retains a snapshot at a HIGH epoch (5).
// Triggering the snapshot-GC purge path for the HIGH-epoch shard B must
// NOT purge shard A's still-needed DEK. The global floor is min(2, 5) = 2.
func TestSnapshotPool_PurgeOldEpochs_CrossShardFloor(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(7)
	stopC := make(chan struct{})
	ldb := newEpochAwareTestLogDB(0) // no encrypted WAL records
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	// Shard A (1/1) retains a snapshot encrypted at the lower epoch 2.
	dirA := snapshotSaveDir(tmpDir, 1, 1, 100)
	if err := os.MkdirAll(dirA, 0o750); err != nil {
		t.Fatalf("mkdir A: %v", err)
	}
	if err := writeTestSnapshotMetadata(dirA, 100, 1, 1, 1, 2, config.NoCompression); err != nil {
		t.Fatalf("write metadata A: %v", err)
	}

	// Shard B (2/1) retains a snapshot encrypted at the higher epoch 5.
	dirB := snapshotSaveDir(tmpDir, 2, 1, 200)
	if err := os.MkdirAll(dirB, 0o750); err != nil {
		t.Fatalf("mkdir B: %v", err)
	}
	if err := writeTestSnapshotMetadata(dirB, 200, 1, 2, 1, 5, config.NoCompression); err != nil {
		t.Fatalf("write metadata B: %v", err)
	}

	// Both shards are loaded and share the barrier.
	wireSnapshotNodes(p,
		snapshotDirNode(tmpDir, 1, 1),
		snapshotDirNode(tmpDir, 2, 1),
	)

	// Trigger the purge via shard B's GC path (the higher-epoch shard).
	// The pre-fix per-shard scan would compute floor=5 and purge epoch 2's
	// DEK, bricking shard A's snapshot. The global floor must be 2.
	p.purgeOldEpochs(tmpDir, 2, 1)

	if barrier.PurgedBefore() != 2 {
		t.Fatalf("purgedBefore = %d, want 2 (global cross-shard floor); "+
			"shard A's epoch-2 DEK must not be purged", barrier.PurgedBefore())
	}
}

// TestSnapshotPool_PurgeOldEpochs_CrossShardWALFloor verifies the WAL floor
// participates in the global minimum across shards. Snapshots are at epochs
// 4 and 6, but the (global) WAL still references epoch 3 — so the floor is 3.
func TestSnapshotPool_PurgeOldEpochs_CrossShardWALFloor(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(8)
	stopC := make(chan struct{})
	ldb := newEpochAwareTestLogDB(3) // global WAL min epoch is 3
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	dirA := snapshotSaveDir(tmpDir, 1, 1, 100)
	if err := os.MkdirAll(dirA, 0o750); err != nil {
		t.Fatalf("mkdir A: %v", err)
	}
	if err := writeTestSnapshotMetadata(dirA, 100, 1, 1, 1, 4, config.NoCompression); err != nil {
		t.Fatalf("write metadata A: %v", err)
	}
	dirB := snapshotSaveDir(tmpDir, 2, 1, 200)
	if err := os.MkdirAll(dirB, 0o750); err != nil {
		t.Fatalf("mkdir B: %v", err)
	}
	if err := writeTestSnapshotMetadata(dirB, 200, 1, 2, 1, 6, config.NoCompression); err != nil {
		t.Fatalf("write metadata B: %v", err)
	}

	wireSnapshotNodes(p,
		snapshotDirNode(tmpDir, 1, 1),
		snapshotDirNode(tmpDir, 2, 1),
	)

	p.purgeOldEpochs(tmpDir, 1, 1)

	if barrier.PurgedBefore() != 3 {
		t.Fatalf("purgedBefore = %d, want 3 (global WAL floor below snapshots)", barrier.PurgedBefore())
	}
}

// TestSnapshotPool_GlobalSafePurgeFloor_NilEnumerator verifies the fail-safe
// contract: with no node enumerator the floor is unknown and ok is false,
// so callers must not purge.
func TestSnapshotPool_GlobalSafePurgeFloor_NilEnumerator(t *testing.T) {
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, newEpochAwareTestLogDB(2), barrier, nil, nil, stopC, nil)
	// p.nodes is nil.

	if floor, ok := p.globalSafePurgeFloor(); ok {
		t.Fatalf("globalSafePurgeFloor with nil enumerator = (%d, true), want (_, false)", floor)
	}

	// And purgeOldEpochs must not call PurgeEpochsBefore.
	p.purgeOldEpochs("", 1, 1)
	if barrier.PurgeCallCount() != 0 {
		t.Fatalf("expected 0 purge calls with nil enumerator, got %d", barrier.PurgeCallCount())
	}
}

// TestSnapshotPool_GlobalSafePurgeFloor_ScanError verifies that a snapshot
// dir scan failure fails safe (ok=false, no purge). A node whose snapshot
// base path is a regular file (not a directory) makes ReadDir error.
func TestSnapshotPool_GlobalSafePurgeFloor_ScanError(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(5)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, newEpochAwareTestLogDB(2), barrier, nil, nil, stopC, nil)

	// Create the shard/replica base path as a FILE so os.ReadDir fails with
	// ENOTDIR rather than a benign IsNotExist.
	base := snapshotBaseDir(tmpDir, 1, 1)
	if err := os.MkdirAll(filepath.Dir(base), 0o750); err != nil {
		t.Fatalf("mkdir parent: %v", err)
	}
	if err := os.WriteFile(base, []byte("not a dir"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	wireSnapshotNodes(p, snapshotDirNode(tmpDir, 1, 1))

	if floor, ok := p.globalSafePurgeFloor(); ok {
		t.Fatalf("globalSafePurgeFloor on scan error = (%d, true), want (_, false)", floor)
	}
	p.purgeOldEpochs(tmpDir, 1, 1)
	if barrier.PurgeCallCount() != 0 {
		t.Fatalf("expected 0 purge calls on scan error, got %d", barrier.PurgeCallCount())
	}
}

// ---------------------------------------------------------------------------
// Stale decrypt scratch-file sweep tests
// ---------------------------------------------------------------------------

// TestSweepStaleDecryptTmp verifies that the sweep removes orphaned
// .decrypt-*.tmp files left by a crash during recovery while preserving the
// canonical snapshot files and any unrelated content.
func TestSweepStaleDecryptTmp(t *testing.T) {
	dir := t.TempDir()

	// Orphaned decrypt scratch files (should be removed). BOTH decrypt paths
	// must be swept: the send path's scratch file is a full plaintext copy of
	// the state machine, and a sweeper matching only the recovery path's
	// prefix would leave it behind forever.
	stale := []string{
		".decrypt-123.tmp", ".decrypt-abcdef.tmp",
		".send-decrypt-123.tmp", ".send-decrypt-abcdef.tmp",
		// The save path's encrypt scratch holds a full CIPHERTEXT copy of the
		// snapshot. Were its name not matched by a prefix in the swept set, a
		// crash inside the encrypt loop would orphan it until the whole
		// snapshot directory rolled off.
		".encrypt-999.tmp",
	}
	for _, name := range stale {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	// Files that must be preserved.
	keep := []string{
		snapshotDataFile,     // snapshot.dat
		snapshotMetadataFile, // snapshot.meta
		"decrypt-noprefix.tmp",
		".decrypt-missing-suffix",
	}
	for _, name := range keep {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("y"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	SweepStaleDecryptTmp(dir)

	for _, name := range stale {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("stale file %s was not removed (err=%v)", name, err)
		}
	}
	for _, name := range keep {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("file %s should have been preserved: %v", name, err)
		}
	}
}

// TestSweepStaleDecryptTmp_MissingDir verifies the sweep is a safe no-op on
// a nonexistent directory.
func TestSweepStaleDecryptTmp_MissingDir(t *testing.T) {
	// Must not panic or error.
	SweepStaleDecryptTmp(filepath.Join(t.TempDir(), "does-not-exist"))
}

// TestSweepStaleDecryptTmpTree_ReclaimsEveryRetainedSnapshot verifies the sweep
// covers EVERY retained snapshot directory, not just the newest.
//
// The send path decrypts inside whichever snapshot directory it is streaming,
// which need not be the one recovery will load. A sweep scoped to the recovered
// directory leaves a full plaintext copy of the state machine in every other
// retained directory, permanently.
func TestSweepStaleDecryptTmpTree_ReclaimsEveryRetainedSnapshot(t *testing.T) {
	base := t.TempDir()
	const shardID, replicaID uint64 = 3, 1

	orphans := make([]string, 0, 3)
	for _, index := range []uint64{100, 200, 300} {
		dir := snapshotSaveDir(base, shardID, replicaID, index)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("MkdirAll(%s): %v", dir, err)
		}
		orphan := filepath.Join(dir, ".send-decrypt-abc.tmp")
		if err := os.WriteFile(orphan, []byte("plaintext"), 0o600); err != nil {
			t.Fatalf("write orphan: %v", err)
		}
		orphans = append(orphans, orphan)
		if err := os.WriteFile(filepath.Join(dir, snapshotDataFile), []byte("ct"), 0o600); err != nil {
			t.Fatalf("write snapshot.dat: %v", err)
		}
	}

	SweepStaleDecryptTmpTree(base, shardID, replicaID)

	for _, orphan := range orphans {
		if _, err := os.Stat(orphan); !os.IsNotExist(err) {
			t.Fatalf("orphaned plaintext %s survived the tree sweep (err=%v)", orphan, err)
		}
	}
	for _, index := range []uint64{100, 200, 300} {
		dataPath := filepath.Join(snapshotSaveDir(base, shardID, replicaID, index), snapshotDataFile)
		if _, err := os.Stat(dataPath); err != nil {
			t.Fatalf("canonical %s was destroyed by the sweep: %v", dataPath, err)
		}
	}
}

// TestIsSnapshotScratchTemp pins the single matcher every snapshot scratch
// path's os.CreateTemp pattern must satisfy — the encrypt side included. A
// name a writer can create but the sweeper does not match is an orphan nothing
// on the host will ever reclaim.
func TestIsSnapshotScratchTemp(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{".decrypt-123.tmp", true},
		{".send-decrypt-123.tmp", true},
		{".decrypt-.tmp", true},
		{".recv-encrypt-123.tmp", true},
		{".reencrypt-snapshot-123.tmp", true},
		{"decrypt-123.tmp", false},
		{".decrypt-123", false},
		{".recv-encrypt-123", false},
		{".encrypt-123.tmp", true},
		{snapshotDataFile, false},
		{snapshotMetadataFile, false},
	}
	for _, tt := range tests {
		if got := IsSnapshotScratchTemp(tt.name); got != tt.want {
			t.Errorf("IsSnapshotScratchTemp(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}

	// Every pattern a snapshot scratch path can create must be reclaimable.
	for _, pattern := range []string{
		RecoverDecryptTempPattern,
		SendDecryptTempPattern,
		RecvEncryptTempPattern,
		ReencryptTempPattern,
	} {
		f, err := os.CreateTemp(t.TempDir(), pattern)
		if err != nil {
			t.Fatalf("CreateTemp(%q): %v", pattern, err)
		}
		name := filepath.Base(f.Name())
		if err := f.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if !IsSnapshotScratchTemp(name) {
			t.Errorf("pattern %q created %q, which the sweeper does not match", pattern, name)
		}
	}
}

// TestSweepStaleImportStageDirs_ReclaimsOrphansAndSparesCanonical pins the
// import staging sweep's two halves: an orphaned staging directory (a plaintext
// state machine nothing else on the host enumerates) is removed whole, and a
// canonical snapshot directory is never touched.
func TestSweepStaleImportStageDirs_ReclaimsOrphansAndSparesCanonical(t *testing.T) {
	const shardID, replicaID uint64 = 3, 9
	base := t.TempDir()
	replicaDir := snapshotBaseDir(base, shardID, replicaID)

	canonical := snapshotSaveDir(base, shardID, replicaID, 42)
	if err := os.MkdirAll(canonical, 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	canonicalData := filepath.Join(canonical, snapshotDataFile)
	if err := os.WriteFile(canonicalData, []byte("ciphertext"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// A staging directory named exactly as an import names one, holding a
	// nested tree so a shallow removal is visible.
	stage, err := os.MkdirTemp(replicaDir, ImportStageDirPattern)
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	if !IsSnapshotImportStage(filepath.Base(stage)) {
		t.Fatalf("pattern %q created %q, which the sweeper does not match",
			ImportStageDirPattern, filepath.Base(stage))
	}
	nested := filepath.Join(stage, "nested")
	if err := os.MkdirAll(nested, 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nested, snapshotDataFile),
		[]byte("plaintext state machine"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	before := StaleDecryptSweepFailures()
	SweepStaleImportStageDirs(base, shardID, replicaID)

	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Errorf("staging directory %s survived the sweep (stat err %v)", stage, err)
	}
	if _, err := os.Stat(canonicalData); err != nil {
		t.Errorf("the canonical snapshot artifact was destroyed by the sweep: %v", err)
	}
	if got := StaleDecryptSweepFailures(); got != before {
		t.Errorf("StaleDecryptSweepFailures %d -> %d on a clean sweep", before, got)
	}
}

// TestIsSnapshotImportStage pins the matcher against the names the sweeper must
// and must not reclaim.
func TestIsSnapshotImportStage(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{".import-123456", true},
		{".import-", true},
		{"import-123456", false},
		{".imports-123", false},
		{"snapshot-00000000000000000900", false},
		{snapshotDataFile, false},
		{snapshotMetadataFile, false},
	}
	for _, tt := range tests {
		if got := IsSnapshotImportStage(tt.name); got != tt.want {
			t.Errorf("IsSnapshotImportStage(%q) = %v, want %v", tt.name, got, tt.want)
		}
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
	want := "engine: snapshot encrypt failed: quicraft: snapshot frame corrupt"
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

// TestSnapshotPool_DecryptFile_EmptyFileIsTruncation pins that a sealed
// artifact with no frames is refused. The writer always emits a terminal
// frame, so an empty encrypted snapshot.dat can only be one truncated to
// nothing, and decrypting it to an empty state machine would silently discard
// every entry the snapshot held.
func TestSnapshotPool_DecryptFile_EmptyFileIsTruncation(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	dataPath := filepath.Join(tmpDir, "empty.dat")
	if err := os.WriteFile(dataPath, []byte{}, 0o644); err != nil {
		t.Fatalf("write empty file failed: %v", err)
	}
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 1, barrier)
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("decrypt of a frameless artifact = %v, want ErrSnapshotCorrupt", err)
	}
	if plainPath != "" {
		t.Fatalf("expected empty path on error, got %q", plainPath)
	}
}

// TestSnapshotPool_DecryptFile_EmptyPlaintextRoundTrips is the other half: a
// state machine that writes nothing still produces an artifact, one empty
// terminal frame, and it decrypts back to nothing.
func TestSnapshotPool_DecryptFile_EmptyPlaintextRoundTrips(t *testing.T) {
	tmpDir := t.TempDir()
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	var sealed bytes.Buffer
	epoch, pinned, err := SealSnapshotFrames(barrier, 1, 1, 1, bytes.NewReader(nil), &sealed)
	if err != nil {
		t.Fatalf("SealSnapshotFrames(empty): %v", err)
	}
	for _, e := range pinned {
		barrier.DeregisterInFlightEpoch(e)
	}
	if epoch == 0 || sealed.Len() == 0 {
		t.Fatalf("empty plaintext sealed to epoch %d, %d bytes; want one terminal frame", epoch, sealed.Len())
	}
	dataPath := filepath.Join(tmpDir, "empty-plaintext.dat")
	if err := os.WriteFile(dataPath, sealed.Bytes(), 0o644); err != nil {
		t.Fatalf("write sealed file failed: %v", err)
	}
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, epoch, barrier)
	if err != nil {
		t.Fatalf("decrypt of an empty-plaintext artifact failed: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(plainPath) })
	plain, readErr := os.ReadFile(plainPath)
	if readErr != nil {
		t.Fatalf("read decrypted sibling failed: %v", readErr)
	}
	if len(plain) != 0 {
		t.Fatalf("decrypted sibling length = %d, want 0", len(plain))
	}
}

func TestSnapshotPool_WriteMetadataWithEpoch(t *testing.T) {
	tmpDir := t.TempDir()

	err := writeTestSnapshotMetadata(tmpDir, 42, 5, 10, 3, 7, config.NoCompression)
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

	// Verify every field the record carries.
	meta := mustParseSnapshotMeta(t, data)
	if meta.Index != 42 {
		t.Fatal("index mismatch")
	}
	if meta.Term != 5 {
		t.Fatal("term mismatch")
	}
	if meta.ShardID != 10 {
		t.Fatal("shardID mismatch")
	}
	if meta.ReplicaID != 3 {
		t.Fatal("replicaID mismatch")
	}
	createdAt := int64(meta.CreatedAt)
	now := time.Now().Unix()
	if createdAt < now-5 || createdAt > now+5 {
		t.Fatalf("createdAt = %d, expected near %d", createdAt, now)
	}
	if !meta.EpochRecorded || meta.Epoch != 7 {
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
	node := NewNode(peer, nil, lr, cfg, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   shardID,
		replicaID: replicaID,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	if result.index != 100 {
		t.Fatalf("result.index = %d, want 100", result.index)
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
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   shardID,
		replicaID: replicaID,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	if result.index != 50 {
		t.Fatalf("result.index = %d, want 50", result.index)
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
	node := NewNode(peer, nil, lr, cfg, ss, tmpDir, nil)

	err = p.requestSnapshot(snapshotRequest{
		shardID:   shardID,
		replicaID: replicaID,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	if result.index != 200 {
		t.Fatalf("result.index = %d, want 200", result.index)
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
	sealSnapshotFile(t, dataPath, original, 1, 1, 1, barrier)
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 1, barrier)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(plainPath) })

	decrypted, readErr := os.ReadFile(plainPath)
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
	sealSnapshotFile(t, dataPath, original, 1, 1, 1, barrier)
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 2, barrier)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(plainPath) })

	decrypted, readErr := os.ReadFile(plainPath)
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

// TestSnapshotPool_DecryptFile_MissingFile verifies that
// decryptSnapshotToScratch returns a SnapshotEncryptError and an empty path
// when the source file does not exist.
func TestSnapshotPool_DecryptFile_MissingFile(t *testing.T) {
	barrier := newTestBarrier(1)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir("/nonexistent/path/snapshot.dat"), "/nonexistent/path/snapshot.dat", 1, 1, 1, 1, barrier)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	if plainPath != "" {
		t.Fatalf("expected empty path on error, got %q", plainPath)
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

	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 1, barrier)
	if err == nil {
		t.Fatal("expected error for truncated frame header")
	}
	if plainPath != "" {
		t.Fatalf("expected empty path on error, got %q", plainPath)
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

	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, 1, 1, 1, 1, barrier)
	if err == nil {
		t.Fatal("expected error for truncated frame data")
	}
	if plainPath != "" {
		t.Fatalf("expected empty path on error, got %q", plainPath)
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
	node := NewNode(nil, nil, nil, cfg, nil, tmpDir, nil)
	node.snapshotter = snapshotter

	err := p.requestSnapshot(snapshotRequest{
		shardID: 7, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
	if result.err != nil {
		t.Fatalf("snapshot save failed: %v", result.err)
	}
	if result.shardID != 7 {
		t.Fatalf("result.shardID = %d, want 7", result.shardID)
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
	node := NewNode(nil, nil, nil, cfg, nil, tmpDir, nil)
	node.snapshotter = snapshotter

	err := p.requestSnapshot(snapshotRequest{
		shardID: 7, replicaID: 1, node: node, save: true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
	if result.err != nil {
		t.Fatalf("snapshot save failed: %v", result.err)
	}
	// No panic = success with nil metrics.
}

// ---------------------------------------------------------------------------
// Snapshot term resolved from Peer.AppliedToTerm, not LogReader
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
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
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
	// persisted), so lr.Term(100) would fail. The term is captured
	// at request creation time (before pool dispatch) to eliminate the
	// data race between the snapshot pool worker and step worker.
	ss := newSnapshotTestSnapshotter(100)
	node := NewNode(peer, nil, lr, cfg, ss, tmpDir, nil)

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

	result := awaitSnapshotResult(t, pool, "snapshot")
	if result.err != nil {
		t.Fatalf("snapshot save failed: %v", result.err)
	}
	if result.index != 100 {
		t.Fatalf("result.index = %d, want 100", result.index)
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
	// as a safe fallback. This verifies the nil check.
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
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	err := pool.requestSnapshot(snapshotRequest{
		shardID:   20,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, pool, "snapshot")
	if result.err != nil {
		t.Fatalf("snapshot save failed: %v", result.err)
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
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
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
	node := NewNode(peer, nil, lr, cfg, ss, tmpDir, nil)

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

	result := awaitSnapshotResult(t, pool, "snapshot")
	if result.err != nil {
		t.Fatalf("snapshot save failed: %v", result.err)
	}
	if result.index != 100 {
		t.Fatalf("result.index = %d, want 100", result.index)
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
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

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

	result := awaitSnapshotResult(t, pool, "snapshot")
	if result.err != nil {
		t.Fatalf("snapshot save failed: %v", result.err)
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
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
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
	node := NewNode(peer, nil, lr, cfg, ss, tmpDir, nil)

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

	result := awaitSnapshotResult(t, pool, "snapshot")
	if result.err != nil {
		t.Fatalf("snapshot save failed: %v", result.err)
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
// LogReader.CreateSnapshot error handling
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
	node := NewNode(nil, nil, lr, cfg, ss, tmpDir, nil)

	err := pool.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, pool, "snapshot")
	if result.err != nil {
		t.Fatalf("snapshot save failed: %v", result.err)
	}
	if result.index != 100 {
		t.Fatalf("result.index = %d, want 100", result.index)
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
	node := NewNode(nil, nil, lr, cfg, ss, tmpDir, nil)

	err := pool.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, pool, "snapshot")
	if result.err != nil {
		t.Fatalf("snapshot save failed: %v", result.err)
	}
	if result.index != 100 {
		t.Fatalf("result.index = %d, want 100", result.index)
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
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	err := p.requestSnapshot(snapshotRequest{
		shardID:   10,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	result := awaitSnapshotResult(t, p, "snapshot")
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

	// Verify the snapshot was NOT recorded in LogDB. A partial snapshot
	// must never be treated as a valid recovery point.
	snap, err := ldb.GetSnapshot(10, 1)
	if err != nil && !errors.Is(err, logdb.ErrNoSnapshot) {
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
		if err := writeTestSnapshotMetadata(dir, idx, 1, shardID, replicaID, 0, config.NoCompression); err != nil {
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
	if err := gcSnapshotDirs(tmpDir, shardID, replicaID, ""); err != nil {
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

	node := NewNode(nil, nil, nil, cfg, snapshotter, snapDir, nil)

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

	node := NewNode(nil, nil, nil, cfg, snapshotter, snapDir, nil)

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

	node := NewNode(nil, nil, nil, cfg, snapshotter, snapDir, nil)

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

	node := NewNode(nil, nil, nil, cfg, snapshotter, snapDir, nil)

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
	node := NewNode(nil, nil, nil, cfg, &panickingSnapshotter{}, snapDir, nil)
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
	node := NewNode(nil, nil, nil, cfg, &panickingSnapshotter{}, snapDir, nil)
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
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

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

// panicOnCompletedCallback panics inside OnSnapshotCompleted, standing in for
// a user EventListener that panics after the snapshot itself has finished.
type panicOnCompletedCallback struct {
	*testCallback
	rejected atomic.Int64
}

func (c *panicOnCompletedCallback) OnSnapshotCompleted(_ uint64, _ uint64, _ SnapshotOperation, _ error) {
	panic("listener boom")
}

func (c *panicOnCompletedCallback) OnSnapshotRejected(_, _ uint64, _ error) {
	c.rejected.Add(1)
}

// TestHandleRequest_PanicRecovery_PostCompletionCallback verifies that panic
// containment extends past handleSnapshot to the completion notification.
// OnSnapshotCompleted reaches user EventListener code through the host; an
// unrecovered panic there would abort the whole host process from a pool
// worker goroutine, taking every unrelated shard down with it.
func TestHandleRequest_PanicRecovery_PostCompletionCallback(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	stopC := make(chan struct{})
	defer close(stopC)
	cb := &panicOnCompletedCallback{testCallback: newTestCallback()}
	pool := newSnapshotPool(1, nil, nil, cb, NewWorkSignal(1), stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	// No snapshotter: saveSnapshot returns an error, so handleSnapshot
	// completes and the panic can only come from the completion callback.
	node := NewNode(nil, nil, nil, cfg, nil, t.TempDir(), nil)
	node.snapshotting.Store(true)
	pool.inFlight.Store(uint64(1), node)

	// Must not panic out of handleRequest.
	pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node, save: true})

	if node.snapshotting.Load() {
		t.Fatal("snapshotting must be cleared after a post-completion panic")
	}
	if _, ok := pool.inFlight.Load(uint64(1)); ok {
		t.Fatal("in-flight must be cleared after a post-completion panic")
	}
	if cb.rejected.Load() != 0 {
		t.Fatal("OnSnapshotRejected must not fire for a snapshot that completed")
	}
}

// TestHandleRequest_CompletionNamesItsOperation: a save that wrote nothing and
// a recovery both complete at index 0 without error, so the index cannot say
// which happened. The completion carries the operation so the host fires
// OnSnapshotRecovered only for a recovery.
func TestHandleRequest_CompletionNamesItsOperation(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	cb := newTestCallback()
	pool := newSnapshotPool(1, nil, nil, cb, NewWorkSignal(1), stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	// Nothing applied: the save is a no-op. No LogDB: the recovery has no
	// record to restore and completes without touching the state machine.
	node := NewNode(nil, nil, nil, cfg, newSnapshotTestSnapshotter(0), t.TempDir(), nil)

	for _, save := range []bool{true, false} {
		node.snapshotting.Store(true)
		pool.inFlight.Store(uint64(1), node)
		pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node, save: save})
	}

	calls := cb.getSnapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("OnSnapshotCompleted called %d times, want 2", len(calls))
	}
	if c := calls[0]; c.op != SnapshotSave || c.index != 0 || c.err != nil {
		t.Fatalf("no-op save completed as %+v, want SnapshotSave at index 0 without error", c)
	}
	if c := calls[1]; c.op != SnapshotRecover || c.err != nil {
		t.Fatalf("recovery completed as %+v, want SnapshotRecover without error", c)
	}
}

// TestHandleRequest_FailedSaveIsLoggedNotLatched: a failed save only READ the
// state machine, so it must not trip the failure latch or report a shard
// failure. It must still be reported: an automatic save has no requester to
// hand the error to, so the pool's log line is its only report.
func TestHandleRequest_FailedSaveIsLoggedNotLatched(t *testing.T) {
	buf, restore := captureWarnOutput(t)
	defer restore()

	stopC := make(chan struct{})
	defer close(stopC)
	cb := newTestCallback()
	pool := newSnapshotPool(1, nil, nil, cb, NewWorkSignal(1), stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	// No snapshotter: the save fails without touching anything.
	node := NewNode(nil, nil, nil, cfg, nil, t.TempDir(), nil)
	node.snapshotting.Store(true)
	pool.inFlight.Store(uint64(1), node)

	pool.handleRequest(snapshotRequest{shardID: 1, replicaID: 1, node: node, save: true})

	if node.IsFailed() {
		t.Fatal("a failed save tripped the shard's failure latch")
	}
	if n := cb.shardFailedCount.Load(); n != 0 {
		t.Fatalf("OnShardFailed called %d times for a failed save, want 0", n)
	}
	if n := cb.snapshotCallCount.Load(); n != 1 {
		t.Fatalf("OnSnapshotCompleted called %d times, want 1: the requester must get the error", n)
	}
	if !strings.Contains(buf.String(), "snapshot save failed") {
		t.Fatalf("the failed save was not logged; log output: %q", buf.String())
	}
}

// TestSnapshotPool_ShutdownReleasesQueuedRequests verifies that a request
// still queued when the pool shuts down does not strand the per-shard state
// requestSnapshot established for it. The in-flight entry and the node's
// snapshotting flag are cleared only by the worker that runs the request, so
// a pool that stops without draining requestC leaves the shard permanently
// unable to snapshot and the caller's pending request waiting for a
// completion callback that can never fire.
func TestSnapshotPool_ShutdownReleasesQueuedRequests(t *testing.T) {
	t.Run("shutdown races the producer", func(t *testing.T) {
		// stopC is already closed when the request is submitted: the send
		// still succeeds (the buffer has room) and no worker will ever read
		// it, so requestSnapshot itself must release it.
		stopC := make(chan struct{})
		close(stopC)
		cb := newTestCallback()
		p := newSnapshotPool(1, nil, nil, cb, NewWorkSignal(1), stopC, nil)

		cfg := config.Config{ShardID: 7, ReplicaID: 1}
		cfg.SetDefaults()
		node := NewNode(nil, nil, nil, cfg, nil, "", nil)
		if !node.TryStartSnapshot() {
			t.Fatal("TryStartSnapshot should succeed on a fresh node")
		}

		if err := p.requestSnapshot(snapshotRequest{
			shardID: 7, replicaID: 1, node: node, save: true,
		}); err != nil {
			t.Fatalf("requestSnapshot failed: %v", err)
		}

		if node.snapshotting.Load() {
			t.Fatal("node left marked snapshotting by a request no worker will run")
		}
		if _, loaded := p.inFlight.Load(uint64(7)); loaded {
			t.Fatal("shard left marked in-flight: no further snapshot could ever be accepted")
		}
		if cb.snapshotRejected.Load() == 0 {
			t.Fatal("expected OnSnapshotRejected so the caller stops waiting")
		}
		if len(p.requestC) != 0 {
			t.Fatal("the stranded request was left on requestC")
		}
	})

	t.Run("workers drain on exit", func(t *testing.T) {
		// The request is queued while the pool is live, then the pool is
		// stopped. Whichever branch the worker's select takes, the request
		// must not be left holding the shard's snapshot state. (A worker
		// that picks up the request runs it normally; one that observes
		// stopC first drains it.)
		stopC := make(chan struct{})
		cb := newTestCallback()
		p := newSnapshotPool(1, nil, nil, cb, NewWorkSignal(1), stopC, nil)

		cfg := config.Config{ShardID: 8, ReplicaID: 1}
		cfg.SetDefaults()
		node := NewNode(nil, nil, nil, cfg, nil, "", nil)
		if !node.TryStartSnapshot() {
			t.Fatal("TryStartSnapshot should succeed on a fresh node")
		}
		if err := p.requestSnapshot(snapshotRequest{
			shardID: 8, replicaID: 1, node: node, save: true,
		}); err != nil {
			t.Fatalf("requestSnapshot failed: %v", err)
		}

		close(stopC)
		var wg sync.WaitGroup
		p.start(&wg)
		wg.Wait()

		if node.snapshotting.Load() {
			t.Fatal("node left marked snapshotting after the pool stopped")
		}
		if _, loaded := p.inFlight.Load(uint64(8)); loaded {
			t.Fatal("shard left marked in-flight after the pool stopped")
		}
		if len(p.requestC) != 0 {
			t.Fatal("a request was left on requestC after every worker exited")
		}
	})
}

// ---------------------------------------------------------------------------
// Positional context-AAD for snapshot frames
// ---------------------------------------------------------------------------

// aadCapability is a minimal contextAADBarrier stand-in for the SnapshotAAD
// builder tests: it answers only the two capability questions the builder asks,
// so a test can pin "binds context", "cannot bind" and "plaintext mode"
// independently of any full barrier fake.
type aadCapability struct {
	supports  bool
	plaintext bool
}

func (c aadCapability) SupportsContextAAD() bool { return c.supports }

func (c aadCapability) IsPlaintextMode() bool { return c.plaintext }

// TestSnapshotAAD_Layout pins the wire-order of the context. It is not stored
// anywhere, so it can only be reproduced from code — which makes an accidental
// field reorder a silent, total loss of every existing encrypted snapshot.
func TestSnapshotAAD_Layout(t *testing.T) {
	aad := NewSnapshotAAD(0x0102030405060708, 0x1112131415161718, 0x2122232425262728, aadCapability{supports: true})
	prefix := append([]byte("QRSNAP1"),
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
		0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38)
	// Terminal first, then non-terminal from the SAME builder: the flag byte
	// lives in a reused scratch buffer, so the second call must clear it.
	for _, tc := range []struct {
		final bool
		flag  byte
	}{{true, 0x01}, {false, 0x00}} {
		want := append(append([]byte{}, prefix...), tc.flag)
		if len(want) != SnapshotFrameAADLen {
			t.Fatalf("fixture is %d bytes, SnapshotFrameAADLen = %d", len(want), SnapshotFrameAADLen)
		}
		if got := aad.Frame(0x3132333435363738, tc.final); !bytes.Equal(got, want) {
			t.Fatalf("Frame(final=%v) = %x, want %x", tc.final, got, want)
		}
	}
}

// TestSnapshotAAD_DistinctPerPosition asserts no two positions collide, which
// is what the fixed-width, delimiter-free encoding buys.
func TestSnapshotAAD_DistinctPerPosition(t *testing.T) {
	seen := map[string]string{}
	for _, tc := range []struct {
		name                      string
		shardID, replicaID, index uint64
		seq                       uint64
		final                     bool
	}{
		{"base", 1, 2, 3, 4, false},
		{"shard", 2, 2, 3, 4, false},
		{"replica", 1, 3, 3, 4, false},
		{"index", 1, 2, 4, 4, false},
		{"seq", 1, 2, 3, 5, false},
		// The terminal flag is what makes a truncated artifact fail: the frame
		// sealed as non-terminal must not authenticate as the last one.
		{"final", 1, 2, 3, 4, true},
		// The classic delimiter-free ambiguity: adjacent fields borrowing a
		// digit from one another. Fixed-width fields make it impossible.
		{"shift", 1, 2, 30, 4, false},
	} {
		key := string(NewSnapshotAAD(tc.shardID, tc.replicaID, tc.index, aadCapability{supports: true}).Frame(tc.seq, tc.final))
		if prev, dup := seen[key]; dup {
			t.Fatalf("%s and %s produce identical context bytes", tc.name, prev)
		}
		seen[key] = tc.name
	}
}

// TestSnapshotAAD_UnbindableBarrierSuppressesContext pins the documented
// capability limitation: a barrier that cannot authenticate caller AAD rejects
// ANY non-empty context, so the frames it seals keep only its own binding. The
// decision belongs to the deployment and is taken at builder construction —
// never per artifact, and never as a reaction to a decryption failure.
func TestSnapshotAAD_UnbindableBarrierSuppressesContext(t *testing.T) {
	for _, tc := range []struct {
		name    string
		barrier aadCapability
	}{
		{"delegated encryptor without AAD", aadCapability{}},
		{"plaintext mode", aadCapability{plaintext: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if ctx := NewSnapshotAAD(1, 2, 3, tc.barrier).Frame(4, false); ctx != nil {
				t.Fatalf("produced a %d-byte context; the barrier would reject it "+
					"with ErrHardwareMode and every snapshot save would fail", len(ctx))
			}
		})
	}
}

// TestSnapshotAAD_CapableBarrierBindsContext is the other half: a barrier that
// CAN authenticate caller AAD gets the positional binding regardless of whether
// it encrypts in software or delegates to hardware.
func TestSnapshotAAD_CapableBarrierBindsContext(t *testing.T) {
	ctx := NewSnapshotAAD(1, 2, 3, aadCapability{supports: true}).Frame(4, false)
	if len(ctx) != SnapshotFrameAADLen {
		t.Fatalf("context = %d bytes, want %d", len(ctx), SnapshotFrameAADLen)
	}
}

// TestSnapshotAAD_FrameReusesScratchBuffer pins the no-per-frame-allocation
// property of the bulk encrypt/decrypt path.
func TestSnapshotAAD_FrameReusesScratchBuffer(t *testing.T) {
	aad := NewSnapshotAAD(1, 2, 3, aadCapability{supports: true})
	first := aad.Frame(0, false)
	second := aad.Frame(1, true)
	if &first[0] != &second[0] {
		t.Fatal("Frame allocated a new buffer per call; the snapshot bulk path would " +
			"allocate once per frame")
	}
}

// TestSnapshotPool_FrameContext_RejectsSplicedArtifact drives the file-level
// encrypt/decrypt pair. A snapshot.dat that decrypts under its OWN identity must
// not decrypt under any other, and a frame moved to a different ordinal must
// fail — with no fallback to a weaker AAD anywhere in the reader.
func TestSnapshotPool_FrameContext_RejectsSplicedArtifact(t *testing.T) {
	const (
		shardID   = uint64(4)
		replicaID = uint64(2)
		index     = uint64(900)
	)
	tmpDir := t.TempDir()
	barrier := newTestBarrier(3)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	original := make([]byte, snapshotEncryptChunkSize*3+21)
	for i := range original {
		original[i] = byte(i*13 + 2)
	}
	dataPath := filepath.Join(tmpDir, snapshotDataFile)
	sealSnapshotFile(t, dataPath, original, shardID, replicaID, index, barrier)

	// Positive: the artifact's own identity opens it.
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, shardID, replicaID, index, 3, barrier)
	if err != nil {
		t.Fatalf("decrypt under the matching identity: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(plainPath) })
	round, readErr := os.ReadFile(plainPath)
	if readErr != nil {
		t.Fatalf("read decrypted sibling: %v", readErr)
	}
	if !bytes.Equal(round, original) {
		t.Fatalf("round-trip mismatch: %d bytes back, want %d", len(round), len(original))
	}

	// Negative: a foreign artifact identity.
	for _, tc := range []struct {
		name                      string
		shardID, replicaID, index uint64
	}{
		{"other_shard", 5, replicaID, index},
		{"other_replica", shardID, 3, index},
		{"other_index", shardID, replicaID, index + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, decErr := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, tc.shardID, tc.replicaID, tc.index, 3, barrier); decErr == nil {
				t.Fatalf("snapshot sealed for (%d,%d,%d) decrypted as (%d,%d,%d)",
					shardID, replicaID, index, tc.shardID, tc.replicaID, tc.index)
			}
		})
	}

	// Negative: the same frames, reordered on disk.
	t.Run("reordered_frames", func(t *testing.T) {
		blob, rdErr := os.ReadFile(dataPath)
		if rdErr != nil {
			t.Fatalf("read artifact: %v", rdErr)
		}
		var frames [][]byte
		for off := 0; off < len(blob); {
			n := int(binary.LittleEndian.Uint32(blob[off : off+4]))
			off += 4
			frames = append(frames, blob[off:off+n])
			off += n
		}
		if len(frames) < 3 {
			t.Fatalf("fixture produced %d frames, want at least 3", len(frames))
		}
		frames[0], frames[1] = frames[1], frames[0]

		var swapped []byte
		var lb [4]byte
		for _, f := range frames {
			binary.LittleEndian.PutUint32(lb[:], uint32(len(f)))
			swapped = append(swapped, lb[:]...)
			swapped = append(swapped, f...)
		}
		swappedPath := filepath.Join(t.TempDir(), snapshotDataFile)
		if wrErr := os.WriteFile(swappedPath, swapped, 0o600); wrErr != nil {
			t.Fatalf("write swapped artifact: %v", wrErr)
		}
		if _, decErr := p.decryptSnapshotToScratch(filepath.Dir(swappedPath), swappedPath, shardID, replicaID, index, 3, barrier); decErr == nil {
			t.Fatal("a reordered artifact decrypted: frames are not bound to their ordinal")
		}
	})
}

// canarySnapshotter writes a recognisable plaintext marker, then enough filler
// to force at least two frames out of the writer, then calls midSave so a test
// can inspect snapshot.dat WHILE the save is still running.
//
// The filler matters. A canary alone stays inside the frame writer's chunk
// buffer and the file is still empty at the hook, which would let the test pass
// for the wrong reason — "nothing on disk yet" rather than "what is on disk is
// sealed". Writing past the chunk size forces frames to the file first.
type canarySnapshotter struct {
	lastApplied uint64
	canary      []byte
	filler      []byte
	midSave     func()
}

func (s *canarySnapshotter) LastApplied() uint64 { return s.lastApplied }

func (s *canarySnapshotter) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	return s.SaveSnapshotGated(ctx, w, stopper, nil)
}

func (s *canarySnapshotter) SaveSnapshotGated(_ context.Context, w io.Writer, _ <-chan struct{}, afterCapture func()) error {
	if afterCapture != nil {
		defer afterCapture()
	}
	if _, err := w.Write(s.canary); err != nil {
		return err
	}
	if _, err := w.Write(s.filler); err != nil {
		return err
	}
	if s.midSave != nil {
		s.midSave()
	}
	_, err := w.Write(s.canary)
	return err
}

func (s *canarySnapshotter) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	_, err := io.ReadAll(r)
	return err
}

// TestSnapshotPool_SaveSnapshot_NeverWritesPlaintextToCanonicalPath pins
// sealing on the save path.
//
// Writing the state machine to the canonical snapshot.dat in the clear,
// fsyncing it, and encrypting in place afterwards would leave a COMPLETE
// PLAINTEXT STATE MACHINE durable between that fsync and the rename, under the
// name every consumer, backup and operator reads — hundreds of milliseconds
// under load, and permanent after a kill in the window, because the directory
// is then left with no snapshot.meta and the in-process discard guard never
// runs.
//
// The assertion is deliberately about the CANONICAL PATH DURING THE SAVE, not
// about the finished artifact. A test that only inspects the result passes just
// as happily against a write-then-encrypt form, because the artifact is
// correctly encrypted by the time the save returns — the hazard is not in the
// artifact, it is in what the path holds on the way there.
func TestSnapshotPool_SaveSnapshot_NeverWritesPlaintextToCanonicalPath(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	const (
		shardID   = 10
		replicaID = 1
		index     = 100
		epoch     = 7
	)

	canary := bytes.Repeat([]byte("PLAINTEXT-STATE-MACHINE-CANARY!!"), 4)
	// Two chunks' worth, so frames are on disk by the time midSave runs.
	filler := bytes.Repeat([]byte{0xA5}, snapshotEncryptChunkSize*2)

	dataPath := filepath.Join(
		snapshotSaveDir(tmpDir, shardID, replicaID, index),
		snapshotDataFile,
	)

	var (
		midSaveSize      int64
		midSaveHadCanary bool
		midSaveStatErr   error
	)
	ss := &canarySnapshotter{
		lastApplied: index,
		canary:      canary,
		filler:      filler,
		midSave: func() {
			// The save stages: mid-save the canonical name must not exist
			// yet, and the bytes being written live in the stage.
			if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
				midSaveStatErr = fmt.Errorf("canonical %s exists mid-save (stat err %v)", snapshotDataFile, err)
				return
			}
			staged, err := filepath.Glob(filepath.Join(filepath.Dir(filepath.Dir(dataPath)),
				SaveStageDirPrefix+"*", snapshotDataFile))
			if err != nil || len(staged) != 1 {
				midSaveStatErr = fmt.Errorf("found %d staged %s files (glob err %v), want 1",
					len(staged), snapshotDataFile, err)
				return
			}
			onDisk, err := os.ReadFile(staged[0])
			if err != nil {
				midSaveStatErr = err
				return
			}
			midSaveSize = int64(len(onDisk))
			midSaveHadCanary = bytes.Contains(onDisk, canary)
		},
	}

	barrier := newTestBarrier(epoch)
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)

	if _, err := p.saveSnapshot(snapshotRequest{
		shardID:   shardID,
		replicaID: replicaID,
		node:      node,
		save:      true,
	}); err != nil {
		t.Fatalf("saveSnapshot failed: %v", err)
	}

	if midSaveStatErr != nil {
		t.Fatalf("reading %s mid-save: %v", snapshotDataFile, midSaveStatErr)
	}
	// Without this the canary check below is vacuous: an empty file trivially
	// contains no plaintext.
	if midSaveSize == 0 {
		t.Fatalf("nothing had reached %s by mid-save, so the plaintext check "+
			"asserts nothing; the filler is meant to force at least one frame out",
			snapshotDataFile)
	}
	if midSaveHadCanary {
		t.Fatalf("the canonical %s held the state machine IN THE CLEAR during the "+
			"save (%d bytes on disk), and it is durable under the name "+
			"every consumer, backup and operator reads", snapshotDataFile, midSaveSize)
	}

	// And the finished artifact is still readable, so the test cannot pass by
	// writing something that merely is not the canary.
	final, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("reading the finished artifact: %v", err)
	}
	if bytes.Contains(final, canary) {
		t.Fatalf("the finished %s holds the state machine in the clear", snapshotDataFile)
	}
	plainPath, err := p.decryptSnapshotToScratch(filepath.Dir(dataPath), dataPath, shardID, replicaID, index, epoch, barrier)
	if err != nil {
		t.Fatalf("decrypting the artifact back: %v", err)
	}
	defer func() { _ = os.Remove(plainPath) }()
	roundTripped, err := os.ReadFile(plainPath)
	if err != nil {
		t.Fatalf("reading the decrypted artifact: %v", err)
	}
	want := append(append(append([]byte{}, canary...), filler...), canary...)
	if !bytes.Equal(roundTripped, want) {
		t.Fatalf("decrypted artifact is %d bytes, want %d: sealing inline must not "+
			"change what the state machine wrote", len(roundTripped), len(want))
	}
}

// TestSnapshotPool_MaxSnapshotSize_MeasuresPostCompressionNotCiphertext pins
// the contract inline sealing must preserve.
//
// MaxSnapshotSize is documented against the state machine's output after
// compression -- "the actual on-disk size (post-compression when Snappy is
// enabled)". With sealing inline the file holds framed ciphertext, ~41 bytes
// larger per 64 KiB chunk, so a stat at the point of the check would measure
// encryption overhead against a limit that is not defined to include it. A
// snapshot exactly at the limit -- documented as admitted -- would be refused, and SnapshotTooLargeError.Size
// would report a number the operator's configuration does not describe.
//
// The save path measures what the frame writer was handed, which is the
// post-compression size exactly; this test sets the limit to
// the payload size and asserts the save is admitted with a barrier configured.
func TestSnapshotPool_MaxSnapshotSize_MeasuresPostCompressionNotCiphertext(t *testing.T) {
	snapDir := t.TempDir()
	// Two full chunks plus a tail, so there are three frames' worth of AEAD
	// overhead on disk -- enough that a stat-based check would refuse this.
	const payload = snapshotEncryptChunkSize*2 + 4096

	cfg := config.Config{ShardID: 14, ReplicaID: 1, CompactionOverhead: 50}
	cfg.SetDefaults()
	cfg.MaxSnapshotSize = payload // exactly at the limit, not over

	snapshotter := &bulkSnapshotter{bytes: payload}
	snapshotter.lastApplied.Store(300)
	snapshotter.state.Store(42)
	node := NewNode(nil, nil, nil, cfg, snapshotter, snapDir, nil)

	ldb := newSnapshotTestLogDB()
	stopC := make(chan struct{})
	pool := newSnapshotPool(1, ldb, newTestBarrier(2), nil, nil, stopC, nil)

	if _, err := pool.saveSnapshot(snapshotRequest{
		shardID:   14,
		replicaID: 1,
		node:      node,
		save:      true,
	}); err != nil {
		t.Fatalf("a snapshot whose post-compression size EQUALS MaxSnapshotSize "+
			"must be admitted with encryption on; the limit is defined against "+
			"the state machine's output, not the ciphertext: %v", err)
	}

	// And the on-disk artifact really is larger than the limit, so this test
	// is exercising the distinction rather than a case where both agree.
	dataPath := filepath.Join(snapshotSaveDir(snapDir, 14, 1, 300), snapshotDataFile)
	fi, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("stat sealed artifact: %v", err)
	}
	if uint64(fi.Size()) <= payload {
		t.Fatalf("sealed artifact is %d bytes, not larger than the %d-byte payload; "+
			"the test cannot tell a plaintext measurement from a ciphertext one",
			fi.Size(), payload)
	}
	// The LogDB record still carries the on-disk size, which is what sizes a
	// transfer.
	snap, err := ldb.GetSnapshot(14, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snap.FileSize != uint64(fi.Size()) {
		t.Fatalf("LogDB FileSize = %d, want the on-disk %d", snap.FileSize, fi.Size())
	}
}

// sealSnapshotFile writes plaintext to path through snapshotFrameWriter -- the
// sealed artifact exactly as saveSnapshot produces it -- and
// returns the epoch the artifact would record. The tests that read an
// artifact back (decryptSnapshotToScratch, the frame-context binding, the
// mixed-epoch minimum) build their input here, so they exercise the writer
// that is actually on the save path.
func sealSnapshotFile(t *testing.T, path string, plaintext []byte, shardID, replicaID, index uint64, barrier SnapshotBarrier) uint64 {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("sealSnapshotFile: open %s: %v", path, err)
	}
	w := newSnapshotFrameWriter(f, barrier, shardID, replicaID, index)
	defer w.ReleasePins()
	if _, err := w.Write(plaintext); err != nil {
		t.Fatalf("sealSnapshotFile: write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("sealSnapshotFile: close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("sealSnapshotFile: close file: %v", err)
	}
	return w.MinEpoch()
}

// EncryptWithEpochPinnedContext composes this fake's OWN seal and register, so
// whatever it injects or counts in either still takes effect on the pinned path
// the frame writer now always uses.
func (b *testBarrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpochContext(dst, plaintext, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	b.RegisterInFlightEpoch(epoch)
	return ct, epoch, nil
}

// EncryptWithEpochPinnedContext composes this fake's OWN seal and register, so
// whatever it injects or counts in either still takes effect on the pinned path
// the frame writer now always uses.
func (b *failAfterNBarrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpochContext(dst, plaintext, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	b.RegisterInFlightEpoch(epoch)
	return ct, epoch, nil
}

// EncryptWithEpochPinnedContext composes this fake's OWN seal and register, so
// whatever it injects or counts in either still takes effect on the pinned path
// the frame writer now always uses.
func (b *rotateAfterNBarrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpochContext(dst, plaintext, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	b.RegisterInFlightEpoch(epoch)
	return ct, epoch, nil
}
