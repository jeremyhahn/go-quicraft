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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// ---------------------------------------------------------------------------
// Per-barrier (per-tenant) WAL/snapshot test helpers
// ---------------------------------------------------------------------------

// tenantBarrier is a SnapshotBarrier whose ciphertext is cryptographically
// bound to a per-instance id, so one tenant's barrier CANNOT decrypt another
// tenant's frames. Unlike testBarrier (which keys purely on the epoch and is
// therefore interchangeable across instances), tenantBarrier proves Invariant 2
// (frame-level encryption) and Invariant 3 (per-shard isolation): a decrypt
// attempt with the wrong barrier fails. Frames embed their own epoch so
// recovery after a key rotation is still correct.
//
// Frame layout: [epoch:8][id:1][data XOR (byte(epoch) ^ id)].
type tenantBarrier struct {
	mu             sync.Mutex
	id             byte
	epoch          uint64
	sealed         bool
	inFlight       map[uint64]int64
	purgedBefore   uint64
	purgeCallCount int
}

func newTenantBarrier(id byte, epoch uint64) *tenantBarrier {
	return &tenantBarrier{id: id, epoch: epoch, inFlight: make(map[uint64]int64)}
}

func (b *tenantBarrier) seal(plaintext []byte, epoch uint64) []byte {
	key := byte(epoch) ^ b.id
	out := make([]byte, 9+len(plaintext))
	binary.LittleEndian.PutUint64(out[:8], epoch)
	out[8] = b.id
	for i, v := range plaintext {
		out[9+i] = v ^ key
	}
	return out
}

func (b *tenantBarrier) Encrypt(_, plaintext []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return nil, errors.New("barrier sealed")
	}
	return b.seal(plaintext, b.epoch), nil
}

func (b *tenantBarrier) EncryptWithEpoch(_, plaintext []byte) ([]byte, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return nil, 0, errors.New("barrier sealed")
	}
	return b.seal(plaintext, b.epoch), b.epoch, nil
}

func (b *tenantBarrier) EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpoch(dst, plaintext)
	if err != nil {
		return nil, 0, err
	}
	return appendTestAADTag(ct, contextAAD), epoch, nil
}

func (b *tenantBarrier) DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error) {
	body, err := stripTestAADTag(ciphertext, contextAAD)
	if err != nil {
		return nil, err
	}
	return b.Decrypt(dst, body)
}

// IsHardwareMode is false: this barrier binds caller context, like the software
// seal.Barrier it stands in for.
func (b *tenantBarrier) IsHardwareMode() bool { return false }

// SupportsContextAAD is true: this barrier authenticates the caller context.
func (b *tenantBarrier) SupportsContextAAD() bool { return true }

func (b *tenantBarrier) Decrypt(_, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < 9 {
		return nil, errors.New("ciphertext too short")
	}
	return b.DecryptForEpoch(nil, ciphertext, binary.LittleEndian.Uint64(ciphertext[:8]))
}

func (b *tenantBarrier) DecryptForEpoch(_, ciphertext []byte, epoch uint64) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return nil, errors.New("barrier sealed")
	}
	if len(ciphertext) < 9 {
		return nil, errors.New("ciphertext too short")
	}
	if ciphertext[8] != b.id {
		// Frame was sealed by a DIFFERENT tenant barrier: reject. This is what
		// makes cross-barrier decryption fail and proves per-tenant isolation.
		return nil, errors.New("tenantBarrier: foreign frame id")
	}
	key := byte(epoch) ^ b.id
	data := ciphertext[9:]
	out := make([]byte, len(data))
	for i, v := range data {
		out[i] = v ^ key
	}
	return out, nil
}

func (b *tenantBarrier) CurrentEpoch() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.epoch
}

func (b *tenantBarrier) IsSealed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sealed
}

func (b *tenantBarrier) RegisterInFlightEpoch(epoch uint64) {
	b.mu.Lock()
	b.inFlight[epoch]++
	b.mu.Unlock()
}

func (b *tenantBarrier) DeregisterInFlightEpoch(epoch uint64) {
	b.mu.Lock()
	b.inFlight[epoch]--
	if b.inFlight[epoch] <= 0 {
		delete(b.inFlight, epoch)
	}
	b.mu.Unlock()
}

func (b *tenantBarrier) PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.purgedBefore = minEpoch
	b.purgeCallCount++
	return 0, nil
}

func (b *tenantBarrier) rotate(newEpoch uint64) {
	b.mu.Lock()
	b.epoch = newEpoch
	b.mu.Unlock()
}

func (b *tenantBarrier) PurgedBefore() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.purgedBefore
}

func (b *tenantBarrier) PurgeCallCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.purgeCallCount
}

var _ SnapshotBarrier = (*tenantBarrier)(nil)

// perBarrierTestLogDB is a LogDB that reports a WAL min-live epoch per barrier
// identity, modelling a multiplexing LogDB (one dedicated single-barrier WAL
// per tenant). It satisfies logdb.PerBarrierEpochAwareLogDB.
type perBarrierTestLogDB struct {
	snapshotTestLogDB
	floors map[any]uint64 // barrier identity -> WAL min-live epoch (key present == owned)
	// shardsByBarrier optionally models a per-barrier shard registry
	// (logdb.BarrierShardEnumerator). A barrier present here is treated as a
	// fixed, fully-enumerable shard set; absent barriers report known=false so
	// the snapshot pool's completeness guard is skipped (matching a LogDB that
	// cannot enumerate shards). Left empty by tests that do not exercise the
	// guard.
	shardsByBarrier map[any][]uint64
	queryErr        error
}

func newPerBarrierTestLogDB() *perBarrierTestLogDB {
	return &perBarrierTestLogDB{
		snapshotTestLogDB: snapshotTestLogDB{snapshots: make(map[uint64]logdb.Snapshot)},
		floors:            make(map[any]uint64),
		shardsByBarrier:   make(map[any][]uint64),
	}
}

// MinLiveEpoch returns the global minimum across all tenant barriers, used by
// the single-barrier (global) path; the per-barrier path uses the method below.
func (db *perBarrierTestLogDB) MinLiveEpoch() (uint64, error) {
	if db.queryErr != nil {
		return 0, db.queryErr
	}
	var min uint64
	for _, v := range db.floors {
		if v > 0 && (min == 0 || v < min) {
			min = v
		}
	}
	return min, nil
}

// MinLiveEpochForBarrier returns the WAL floor restricted to the given barrier.
// A barrier present in floors (even with value 0) is OWNED -> known=true; an
// absent barrier is not owned -> known=false (fail safe).
func (db *perBarrierTestLogDB) MinLiveEpochForBarrier(barrier any) (uint64, bool, error) {
	if db.queryErr != nil {
		return 0, false, db.queryErr
	}
	epoch, owned := db.floors[barrier]
	return epoch, owned, nil
}

// ShardsForBarrier models logdb.BarrierShardEnumerator: a barrier configured in
// shardsByBarrier has a fixed, fully-enumerable shard set; any other barrier is
// not enumerable (known=false).
func (db *perBarrierTestLogDB) ShardsForBarrier(barrier any) ([]uint64, bool) {
	shards, ok := db.shardsByBarrier[barrier]
	return shards, ok
}

var _ logdb.EpochAwareLogDB = (*perBarrierTestLogDB)(nil)
var _ logdb.PerBarrierEpochAwareLogDB = (*perBarrierTestLogDB)(nil)
var _ logdb.BarrierShardEnumerator = (*perBarrierTestLogDB)(nil)

// writeShardSnapshot writes a snapshot metadata file recording the given epoch
// for (shardID, replicaID) at index, under baseDir.
func writeShardSnapshot(t *testing.T, baseDir string, shardID, replicaID, index, epoch uint64) {
	t.Helper()
	dir := snapshotSaveDir(baseDir, shardID, replicaID, index)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := writeTestSnapshotMetadata(dir, index, 1, shardID, replicaID, epoch, config.NoCompression); err != nil {
		t.Fatalf("write metadata failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Invariant 1: per-barrier safe-purge floor computation
// ---------------------------------------------------------------------------

// TestSnapshotPool_PerBarrierSafePurgeFloor_IndependentOfOtherBarrier proves a
// barrier's floor is unaffected by another barrier's much older epochs.
func TestSnapshotPool_PerBarrierSafePurgeFloor_IndependentOfOtherBarrier(t *testing.T) {
	baseDir := t.TempDir()
	bA := newTenantBarrier(1, 10)
	bB := newTenantBarrier(2, 10)

	ldb := newPerBarrierTestLogDB()
	ldb.floors[bA] = 0 // BarrierA has no encrypted WAL records
	ldb.floors[bB] = 1 // BarrierB still references epoch 1 in its WAL

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.shardBarriers = func(shardID uint64) SnapshotBarrier {
		if shardID == 1 {
			return bA
		}
		return bB
	}
	wireSnapshotNodes(p, snapshotDirNode(baseDir, 1, 1), snapshotDirNode(baseDir, 2, 1))

	// BarrierA retains snapshot epoch 8; BarrierB retains a very old epoch 1.
	writeShardSnapshot(t, baseDir, 1, 1, 100, 8)
	writeShardSnapshot(t, baseDir, 2, 1, 100, 1)

	// BarrierA's floor must be 8 (its own snapshot), NOT lowered to 1 by
	// BarrierB's retained epoch.
	floorA, okA := p.perBarrierSafePurgeFloor(bA)
	if !okA || floorA != 8 {
		t.Fatalf("perBarrierSafePurgeFloor(bA) = (%d, %v), want (8, true)", floorA, okA)
	}
}

// ---------------------------------------------------------------------------
// Invariant 3: per-shard isolation — purging one barrier never touches another
// ---------------------------------------------------------------------------

func TestSnapshotPool_PerBarrier_PurgeIsolation(t *testing.T) {
	baseDir := t.TempDir()
	bA := newTenantBarrier(1, 4)
	bB := newTenantBarrier(2, 9)

	ldb := newPerBarrierTestLogDB()
	ldb.floors[bA] = 2
	ldb.floors[bB] = 9

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.shardBarriers = func(shardID uint64) SnapshotBarrier {
		if shardID == 1 {
			return bA
		}
		return bB
	}
	wireSnapshotNodes(p, snapshotDirNode(baseDir, 1, 1), snapshotDirNode(baseDir, 2, 1))
	writeShardSnapshot(t, baseDir, 1, 1, 100, 3)
	writeShardSnapshot(t, baseDir, 2, 1, 100, 7)

	// Purge for shard 1 -> dispatches to BarrierA only at floor 2.
	p.purgeOldEpochs("", 1, 1)
	if bA.PurgeCallCount() != 1 || bA.PurgedBefore() != 2 {
		t.Fatalf("BarrierA purge = (calls=%d, before=%d), want (1, 2)", bA.PurgeCallCount(), bA.PurgedBefore())
	}
	if bB.PurgeCallCount() != 0 {
		t.Fatalf("BarrierB was purged by a shard-1 purge: calls=%d, want 0", bB.PurgeCallCount())
	}

	// Purge for shard 2 -> dispatches to BarrierB only at floor 7; BarrierA
	// must be left exactly as it was.
	p.purgeOldEpochs("", 2, 1)
	if bB.PurgeCallCount() != 1 || bB.PurgedBefore() != 7 {
		t.Fatalf("BarrierB purge = (calls=%d, before=%d), want (1, 7)", bB.PurgeCallCount(), bB.PurgedBefore())
	}
	if bA.PurgeCallCount() != 1 || bA.PurgedBefore() != 2 {
		t.Fatalf("BarrierA changed after a shard-2 purge: (calls=%d, before=%d), want (1, 2)", bA.PurgeCallCount(), bA.PurgedBefore())
	}
}

// TestSnapshotPool_PerBarrier_FailSafeWithoutPerBarrierLogDB verifies that when
// per-shard barriers are configured but the LogDB does NOT support per-barrier
// epoch queries, the floor is indeterminate and NO purge happens (fail-safe).
func TestSnapshotPool_PerBarrier_FailSafeWithoutPerBarrierLogDB(t *testing.T) {
	baseDir := t.TempDir()
	bA := newTenantBarrier(1, 4)

	// Plain LogDB: implements neither PerBarrierEpochAwareLogDB.
	ldb := newSnapshotTestLogDB()

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.shardBarriers = func(uint64) SnapshotBarrier { return bA }
	wireSnapshotNodes(p, snapshotDirNode(baseDir, 1, 1))
	writeShardSnapshot(t, baseDir, 1, 1, 100, 3)

	if floor, ok := p.perBarrierSafePurgeFloor(bA); ok {
		t.Fatalf("perBarrierSafePurgeFloor with non-per-barrier LogDB = (%d, true), want (_, false)", floor)
	}
	p.purgeOldEpochs("", 1, 1)
	if bA.PurgeCallCount() != 0 {
		t.Fatalf("expected fail-safe (no purge), got %d purge calls", bA.PurgeCallCount())
	}
}

// TestSnapshotPool_PerBarrier_WALQueryErrorFailSafe verifies that a WAL query
// error fails safe (no purge), never purging on uncertainty.
func TestSnapshotPool_PerBarrier_WALQueryErrorFailSafe(t *testing.T) {
	baseDir := t.TempDir()
	bA := newTenantBarrier(1, 4)
	ldb := newPerBarrierTestLogDB()
	ldb.queryErr = errors.New("injected WAL query failure")

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.shardBarriers = func(uint64) SnapshotBarrier { return bA }
	wireSnapshotNodes(p, snapshotDirNode(baseDir, 1, 1))
	writeShardSnapshot(t, baseDir, 1, 1, 100, 3)

	if _, ok := p.perBarrierSafePurgeFloor(bA); ok {
		t.Fatal("perBarrierSafePurgeFloor on WAL query error = (_, true), want (_, false)")
	}
	p.purgeOldEpochs("", 1, 1)
	if bA.PurgeCallCount() != 0 {
		t.Fatalf("expected fail-safe (no purge) on WAL error, got %d purge calls", bA.PurgeCallCount())
	}
}

// TestSnapshotPool_PerBarrier_UnknownBarrierFailSafe verifies that when the
// LogDB does not OWN the target barrier (MinLiveEpochForBarrier reports
// known=false), the floor is indeterminate and no purge happens — even though
// the same call returns epoch 0. Mistaking a bare epoch-0 return for a "zero
// live records" non-constraint would over-purge the barrier's DEKs.
func TestSnapshotPool_PerBarrier_UnknownBarrierFailSafe(t *testing.T) {
	baseDir := t.TempDir()
	bKnown := newTenantBarrier(1, 4)
	bUnknown := newTenantBarrier(2, 4) // never registered in the LogDB's floors

	ldb := newPerBarrierTestLogDB()
	ldb.floors[bKnown] = 2 // only bKnown is owned by the LogDB

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.shardBarriers = func(uint64) SnapshotBarrier { return bUnknown }
	wireSnapshotNodes(p, snapshotDirNode(baseDir, 1, 1))
	// A retained encrypted snapshot exists for the shard, so the ONLY thing
	// preventing a purge is the unknown-barrier WAL fail-safe.
	writeShardSnapshot(t, baseDir, 1, 1, 100, 3)

	if floor, ok := p.perBarrierSafePurgeFloor(bUnknown); ok {
		t.Fatalf("perBarrierSafePurgeFloor(unknown barrier) = (%d, true), want fail-safe (_, false)", floor)
	}
	p.purgeOldEpochs("", 1, 1)
	if bUnknown.PurgeCallCount() != 0 {
		t.Fatalf("expected fail-safe (no purge) for unknown barrier, got %d purge calls", bUnknown.PurgeCallCount())
	}
}

// TestSnapshotPool_PerBarrier_CorruptSnapshotMetaFailSafe verifies that an
// unreadable/corrupt snapshot.meta makes the per-barrier floor fail safe (no
// purge), rather than silently dropping that snapshot from the floor and
// over-purging a DEK it still needs.
func TestSnapshotPool_PerBarrier_CorruptSnapshotMetaFailSafe(t *testing.T) {
	baseDir := t.TempDir()
	bA := newTenantBarrier(1, 9)
	ldb := newPerBarrierTestLogDB()
	ldb.floors[bA] = 9 // WAL alone would yield a floor of 9

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.shardBarriers = func(uint64) SnapshotBarrier { return bA }
	wireSnapshotNodes(p, snapshotDirNode(baseDir, 1, 1))

	// Create a snapshot directory whose metadata file is a TORN write: a valid
	// record cut short so its body no longer reaches the epoch field. The
	// header still declares the full body length, so the record decodes as
	// CORRUPT — distinct from a recognised older version, and equally
	// fail-safe.
	dir := snapshotSaveDir(baseDir, 1, 1, 100)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	full := MarshalSnapshotMeta(SnapshotMeta{Index: 100, ShardID: 1, ReplicaID: 1, Epoch: 3})
	corrupt := full[:len(full)-9]
	if err := os.WriteFile(filepath.Join(dir, snapshotMetadataFile), corrupt, 0o600); err != nil {
		t.Fatalf("write corrupt meta: %v", err)
	}

	if floor, ok := p.perBarrierSafePurgeFloor(bA); ok {
		t.Fatalf("perBarrierSafePurgeFloor with corrupt meta = (%d, true), want fail-safe (_, false)", floor)
	}
	p.purgeOldEpochs("", 1, 1)
	if bA.PurgeCallCount() != 0 {
		t.Fatalf("expected fail-safe (no purge) on corrupt meta, got %d purge calls", bA.PurgeCallCount())
	}
}

// TestSnapshotPool_PerBarrier_SharedBarrierAllShardsCounted verifies that when
// a SINGLE barrier maps to MULTIPLE shards, the per-barrier snapshot scan must
// include ALL of them. With every shard loaded the floor reflects the oldest
// shard's retained snapshot; if a mapped shard is transiently absent (so its
// older snapshot cannot be scanned) the floor MUST fail safe rather than rising
// and over-purging the DEK that absent shard's snapshot still needs.
func TestSnapshotPool_PerBarrier_SharedBarrierAllShardsCounted(t *testing.T) {
	baseDir := t.TempDir()
	bShared := newTenantBarrier(1, 10) // one barrier shared by shards 1, 2, 3

	ldb := newPerBarrierTestLogDB()
	ldb.floors[bShared] = 9                          // WAL min-live epoch for the shared barrier
	ldb.shardsByBarrier[bShared] = []uint64{1, 2, 3} // complete, fixed shard set

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.shardBarriers = func(uint64) SnapshotBarrier { return bShared }

	// Shard 3 retains the OLDEST snapshot (epoch 2); shards 1 and 2 are newer.
	writeShardSnapshot(t, baseDir, 1, 1, 100, 8)
	writeShardSnapshot(t, baseDir, 2, 1, 100, 7)
	writeShardSnapshot(t, baseDir, 3, 1, 100, 2)

	// All three shards loaded: the floor includes shard 3's epoch-2 snapshot.
	wireSnapshotNodes(p,
		snapshotDirNode(baseDir, 1, 1),
		snapshotDirNode(baseDir, 2, 1),
		snapshotDirNode(baseDir, 3, 1),
	)
	floor, ok := p.perBarrierSafePurgeFloor(bShared)
	if !ok || floor != 2 {
		t.Fatalf("floor with all shards loaded = (%d, %v), want (2, true)", floor, ok)
	}

	// Shard 3 transiently absent from loadedNodes(): its epoch-2 snapshot can no
	// longer be scanned. The floor MUST fail safe (no purge) — NOT rise to
	// min(snapshot 8, snapshot 7, WAL 9) = 7 and over-purge epoch 2's DEK.
	wireSnapshotNodes(p,
		snapshotDirNode(baseDir, 1, 1),
		snapshotDirNode(baseDir, 2, 1),
	)
	if floor, ok := p.perBarrierSafePurgeFloor(bShared); ok {
		t.Fatalf("floor with shard 3 absent = (%d, true), want fail-safe (_, false)", floor)
	}
	p.purgeOldEpochs("", 1, 1)
	if bShared.PurgeCallCount() != 0 {
		t.Fatalf("expected fail-safe (no purge) with a mapped shard absent, got %d purge calls", bShared.PurgeCallCount())
	}
}

// TestSnapshotPool_PerBarrier_NilNodesFailSafe verifies the nil-enumerator
// fail-safe is preserved on the per-barrier path.
func TestSnapshotPool_PerBarrier_NilNodesFailSafe(t *testing.T) {
	bA := newTenantBarrier(1, 4)
	ldb := newPerBarrierTestLogDB()
	ldb.floors[bA] = 2

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.shardBarriers = func(uint64) SnapshotBarrier { return bA }
	// p.nodes intentionally left nil.

	if _, ok := p.perBarrierSafePurgeFloor(bA); ok {
		t.Fatal("perBarrierSafePurgeFloor with nil node enumerator = (_, true), want (_, false)")
	}
	p.purgeOldEpochs("", 1, 1)
	if bA.PurgeCallCount() != 0 {
		t.Fatalf("expected fail-safe (no purge) with nil nodes, got %d purge calls", bA.PurgeCallCount())
	}
}

// ---------------------------------------------------------------------------
// Invariant 2 + 4: crash recovery with per-shard barriers (incl. post-save
// key rotation), and cross-barrier decryption is rejected.
// ---------------------------------------------------------------------------

func TestSnapshotPool_PerBarrier_CrashRecovery(t *testing.T) {
	baseDir := t.TempDir()
	bA := newTenantBarrier(1, 2) // tenant A, current epoch 2
	bB := newTenantBarrier(2, 5) // tenant B, current epoch 5

	// A plain LogDB is sufficient: the post-save purge fails safe (no
	// per-barrier support) and does not interfere with the save/recover
	// round-trip under test.
	ldb := newSnapshotTestLogDB()

	shardBarriers := func(shardID uint64) SnapshotBarrier {
		switch shardID {
		case 5:
			return bA
		case 9:
			return bB
		}
		return nil
	}

	stopC := make(chan struct{})
	defer close(stopC)

	// --- pre-crash: save both shards' snapshots, each under its own barrier ---
	p1 := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p1.shardBarriers = shardBarriers
	var wg1 sync.WaitGroup
	p1.start(&wg1)

	saveShard := func(p *snapshotPool, shardID, replicaID, state uint64) {
		ss := newSnapshotTestSnapshotter(state)
		cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
		node := NewNode(nil, nil, nil, cfg, ss, baseDir, nil)
		if err := p.requestSnapshot(snapshotRequest{shardID: shardID, replicaID: replicaID, node: node, save: true}); err != nil {
			t.Fatalf("save request (shard %d): %v", shardID, err)
		}
		res := awaitSnapshotResult(t, p, "save (shard %d)", shardID)
		if res.err != nil {
			t.Fatalf("save (shard %d) failed: %v", shardID, res.err)
		}
	}
	saveShard(p1, 5, 1, 200)
	saveShard(p1, 9, 1, 350)

	// Confirm each snapshot recorded its barrier's epoch at save time.
	if snap, _ := ldb.GetSnapshot(5, 1); snap.Epoch != 2 {
		t.Fatalf("shard 5 recorded epoch = %d, want 2", snap.Epoch)
	}
	if snap, _ := ldb.GetSnapshot(9, 1); snap.Epoch != 5 {
		t.Fatalf("shard 9 recorded epoch = %d, want 5", snap.Epoch)
	}

	// --- simulate crash + restart + a key rotation on tenant A ---
	// BarrierA rotates to epoch 3 AFTER its snapshot was sealed at epoch 2.
	// Recovery must still decrypt using the per-frame embedded epoch.
	bA.rotate(3)

	p2 := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p2.shardBarriers = shardBarriers
	var wg2 sync.WaitGroup
	p2.start(&wg2)

	recoverShard := func(p *snapshotPool, shardID, replicaID, wantState uint64) {
		ss := newSnapshotTestSnapshotter(0)
		cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
		node := NewNode(nil, nil, nil, cfg, ss, baseDir, nil)
		if err := p.requestSnapshot(snapshotRequest{shardID: shardID, replicaID: replicaID, node: node, save: false}); err != nil {
			t.Fatalf("recover request (shard %d): %v", shardID, err)
		}
		res := awaitSnapshotResult(t, p, "recover (shard %d)", shardID)
		if res.err != nil {
			t.Fatalf("recover (shard %d) failed: %v", shardID, res.err)
		}
		if got := ss.state.Load(); got != wantState {
			t.Fatalf("shard %d recovered state = %d, want %d", shardID, got, wantState)
		}
	}
	// BarrierA recovers across the rotation (epoch 2 frame, current epoch 3).
	recoverShard(p2, 5, 1, 200)
	// BarrierB recovers independently under its own key.
	recoverShard(p2, 9, 1, 350)

	// --- cross-barrier isolation: BarrierB cannot decrypt BarrierA's snapshot ---
	dataPathA := snapshotSaveDir(baseDir, 5, 1, 200) + "/" + snapshotDataFile
	if _, err := p2.decryptSnapshotToScratch(filepath.Dir(dataPathA), dataPathA, 5, 1, 200, 2, bB); err == nil {
		t.Fatal("BarrierB decrypted BarrierA's snapshot — per-tenant isolation breached")
	}
	// And BarrierA still decrypts its own snapshot correctly post-rotation.
	plainA, err := p2.decryptSnapshotToScratch(filepath.Dir(dataPathA), dataPathA, 5, 1, 200, 2, bA)
	if err != nil {
		t.Fatalf("BarrierA failed to decrypt its own snapshot post-rotation: %v", err)
	}
	_ = os.Remove(plainA)
}

// EncryptWithEpochPinnedContext composes this fake's OWN seal and register, so
// whatever it injects or counts in either still takes effect on the pinned path
// the frame writer now always uses.
func (b *tenantBarrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpochContext(dst, plaintext, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	b.RegisterInFlightEpoch(epoch)
	return ct, epoch, nil
}

// TestSnapshotPool_PerBarrierFloor_CountsStoppedShardsOfItsOwnBarrier covers
// stopped shards on the per-barrier path, where the shard enumerator is
// OPTIONAL: without it, a floor that scanned loaded shards only would miss
// them. In the production layout each shard has its own snapshot base, so a
// stopped shard's artifacts would be invisible and its DEK purgeable. The
// LogDB's records must hold each barrier's floor at its own stopped shard's epoch — and ONLY
// its own: the resolver attributes an unloaded shard to its barrier, so
// tenant B's older stopped shard must not lower tenant A's floor.
func TestSnapshotPool_PerBarrierFloor_CountsStoppedShardsOfItsOwnBarrier(t *testing.T) {
	root := t.TempDir()
	base := func(shardID uint64) string {
		return filepath.Join(root, "snapshots", fmt.Sprintf("shard-%d", shardID))
	}
	bA := newTenantBarrier(1, 9)
	bB := newTenantBarrier(2, 9)

	ldb := newPerBarrierTestLogDB() // no shardsByBarrier: the enumerator knows nothing
	ldb.floors[bA] = 8
	ldb.floors[bB] = 9

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.shardBarriers = func(shardID uint64) SnapshotBarrier {
		switch shardID {
		case 1, 3:
			return bA
		case 2, 4:
			return bB
		}
		return nil
	}
	// Loaded: shard 3 (A) at epoch 8 and shard 2 (B) at epoch 9.
	writeShardSnapshot(t, base(3), 3, 1, 300, 8)
	writeShardSnapshot(t, base(2), 2, 1, 300, 9)
	wireSnapshotNodes(p, snapshotDirNode(base(3), 3, 1), snapshotDirNode(base(2), 2, 1))

	// Stopped: shard 1 (A) recorded at epoch 7, shard 4 (B) at epoch 2.
	writeShardSnapshot(t, base(1), 1, 1, 100, 7)
	writeShardSnapshot(t, base(4), 4, 1, 100, 2)
	for shardID, epoch := range map[uint64]uint64{1: 7, 4: 2} {
		if err := ldb.SaveSnapshot(shardID, 1, logdb.Snapshot{Index: 100, Epoch: epoch}); err != nil {
			t.Fatalf("SaveSnapshot(%d): %v", shardID, err)
		}
	}
	ldb.nodeInfos = []logdb.NodeInfo{
		{ShardID: 1, ReplicaID: 1}, {ShardID: 2, ReplicaID: 1},
		{ShardID: 3, ReplicaID: 1}, {ShardID: 4, ReplicaID: 1},
	}

	if floorA, ok := p.perBarrierSafePurgeFloor(bA); !ok || floorA != 7 {
		t.Fatalf("perBarrierSafePurgeFloor(A) = (%d, %v), want (7, true): stopped shard 1's "+
			"epoch-7 snapshot must hold A's floor, and B's stopped shard must not lower it", floorA, ok)
	}
	if floorB, ok := p.perBarrierSafePurgeFloor(bB); !ok || floorB != 2 {
		t.Fatalf("perBarrierSafePurgeFloor(B) = (%d, %v), want (2, true): stopped shard 4's "+
			"epoch-2 snapshot must hold B's floor", floorB, ok)
	}
}

// valueTenantBarrier is a valid SnapshotBarrier whose dynamic value is NOT
// comparable (a struct holding a slice); its methods are promoted from the
// embedded pointer, so every copy shares one keyring. Comparing two of them
// with == panics at run time.
type valueTenantBarrier struct {
	*tenantBarrier
	labels []string
}

// TestSnapshotPool_PerBarrier_NonComparableBarrierDeclinesPurge verifies the
// per-barrier purge never compares barrier interface values. A barrier without
// pointer identity cannot be matched to the shards that share its keyring, so
// whether it is the purge target or another shard's barrier, the floor is
// unprovable and the purge is declined: no panic, and no DEK purged that a
// shard holding a copy of the barrier still needs (shard 2's epoch-2 snapshot
// here).
func TestSnapshotPool_PerBarrier_NonComparableBarrierDeclinesPurge(t *testing.T) {
	keyring := newTenantBarrier(1, 10)
	copyOf := valueTenantBarrier{tenantBarrier: keyring, labels: []string{"tenant"}}
	cases := []struct {
		name   string
		target SnapshotBarrier
		shard2 SnapshotBarrier
	}{
		{name: "target without identity", target: copyOf, shard2: copyOf},
		{name: "other shard without identity", target: keyring, shard2: copyOf},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			ldb := newPerBarrierTestLogDB()
			ldb.floors[keyring] = 9

			stopC := make(chan struct{})
			defer close(stopC)
			p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
			p.shardBarriers = func(shardID uint64) SnapshotBarrier {
				if shardID == 2 {
					return tc.shard2
				}
				return tc.target
			}
			writeShardSnapshot(t, baseDir, 1, 1, 100, 8)
			writeShardSnapshot(t, baseDir, 2, 1, 100, 2)
			wireSnapshotNodes(p, snapshotDirNode(baseDir, 1, 1), snapshotDirNode(baseDir, 2, 1))

			if floor, ok := p.perBarrierSafePurgeFloor(tc.target); ok {
				t.Fatalf("floor = (%d, true), want fail-safe (_, false)", floor)
			}
			p.purgeOldEpochs(baseDir, 1, 1)
			if got := keyring.PurgeCallCount(); got != 0 {
				t.Fatalf("purge calls = %d, want 0", got)
			}
		})
	}
}
