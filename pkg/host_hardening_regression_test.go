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
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// ---------------------------------------------------------------------------
// Speculative address pre-registration must restore, not delete
// ---------------------------------------------------------------------------

// TestRequestConfigChange_RollbackRestoresPriorAddress verifies a rolled-back promotion restores the replica's prior registry address.
func TestRequestConfigChange_RollbackRestoresPriorAddress(t *testing.T) {
	h := newTestHost(t)

	const shardID, localReplicaID, observerID = uint64(1), uint64(1), uint64(3)
	const observerAddr = "127.0.0.1:7002"
	const promotedAddr = "127.0.0.1:7099"

	// CheckQuorum off: this node is deliberately left as the only live
	// member of a two-voter shard below, and must stay leader for the
	// duration rather than stepping down on lost quorum.
	scfg := testShardConfig(shardID, localReplicaID)
	scfg.CheckQuorum = config.BoolPtr(false)

	if err := h.StartShard(map[uint64]string{localReplicaID: "127.0.0.1:5000"},
		false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)

	// Commit a second, nonexistent voter so quorum is 2 and later entries never commit.
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer seedCancel()
	if err := h.SyncRequestAddNode(seedCtx, shardID, 2, "127.0.0.1:7001", 0); err != nil {
		t.Fatalf("seeding a second voter: %v", err)
	}

	// The observer is already replicating under its own address.
	h.registry.Register(shardID, observerID, observerAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rs, err := h.RequestAddNode(ctx, shardID, observerID, promotedAddr, 0)
	if err != nil {
		t.Fatalf("RequestAddNode: %v", err)
	}

	// Guard the premise: the entry must still be uncommitted, or the
	// rollback path under test is not the one being exercised.
	select {
	case <-rs.ResultC():
		t.Fatal("config change resolved before the rollback could run; " +
			"the quorum-blocking premise of this test no longer holds")
	default:
	}

	// The speculative pre-registration must be visible so the leader can
	// resolve the promoted replica the moment the entry commits.
	if got, resolveErr := h.registry.Resolve(shardID, observerID); resolveErr != nil || got != promotedAddr {
		t.Fatalf("pre-registered address = %q (err %v), want %q", got, resolveErr, promotedAddr)
	}

	// The entry never commits: the leader is deposed and every pending
	// request is failed. The rollback must run and restore the prior address.
	h.failPendingRequests(testLoadNode(h, shardID), &NotLeaderError{ShardID: shardID})

	if _, resultErr := rs.Result(); resultErr == nil {
		t.Fatal("config change resolved successfully; expected the deposed-leader failure")
	}
	rs.Release()

	got, resolveErr := h.registry.Resolve(shardID, observerID)
	if resolveErr != nil {
		t.Fatalf("registry entry for the existing observer was DELETED by the rollback: %v", resolveErr)
	}
	if got != observerAddr {
		t.Fatalf("address after rollback = %q, want the prior %q", got, observerAddr)
	}
}

// TestRequestConfigChange_RollbackRemovesFirstTimeAdd is the other half of
// When the target replica had NO prior registry entry, the rollback of a
// never-committed add must remove the speculative entry rather than leave a
// mapping for a replica that never became a member.
func TestRequestConfigChange_RollbackRemovesFirstTimeAdd(t *testing.T) {
	h := newTestHost(t)

	const shardID, localReplicaID, newReplicaID = uint64(1), uint64(1), uint64(3)
	const newAddr = "127.0.0.1:7003"

	// CheckQuorum=false: the single live voter must keep the lease for the
	// test's duration rather than stepping down on lost quorum.
	scfg := testShardConfig(shardID, localReplicaID)
	scfg.CheckQuorum = config.BoolPtr(false)

	if err := h.StartShard(map[uint64]string{localReplicaID: "127.0.0.1:5000"},
		false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)

	// Commit a second, nonexistent voter so quorum is 2 and the add below never commits.
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer seedCancel()
	if err := h.SyncRequestAddNode(seedCtx, shardID, 2, "127.0.0.1:7001", 0); err != nil {
		t.Fatalf("seeding a second voter: %v", err)
	}

	if _, resolveErr := h.registry.Resolve(shardID, newReplicaID); resolveErr == nil {
		t.Fatal("precondition: the new replica must have no registry entry")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rs, err := h.RequestAddNode(ctx, shardID, newReplicaID, newAddr, 0)
	if err != nil {
		t.Fatalf("RequestAddNode: %v", err)
	}

	// Guard the premise: the entry must still be uncommitted, or the
	// rollback path under test is not the one being exercised.
	select {
	case <-rs.ResultC():
		t.Fatal("config change resolved before the rollback could run; " +
			"the quorum-blocking premise of this test no longer holds")
	default:
	}

	h.failPendingRequests(testLoadNode(h, shardID), &NotLeaderError{ShardID: shardID})
	if _, resultErr := rs.Result(); resultErr == nil {
		t.Fatal("config change resolved successfully; expected the deposed-leader failure")
	}
	rs.Release()

	if got, resolveErr := h.registry.Resolve(shardID, newReplicaID); resolveErr == nil {
		t.Fatalf("stale registry entry %q left for a replica that was never added", got)
	}
}

// ---------------------------------------------------------------------------
// Rollbacks must claim the pending entry before releasing references
// ---------------------------------------------------------------------------

// TestRequestSnapshot_RollbackDoesNotDoubleReleaseOnConcurrentCompletion verifies the submit-failure rollback does not release an entry a completion already released.
func TestRequestSnapshot_RollbackDoesNotDoubleReleaseOnConcurrentCompletion(t *testing.T) {
	h := newTestHost(t)
	addTestNodeState(h, 1, 1)
	ns := testLoadNode(h, 1)
	pool := NewRequestStatePool()

	rs := newRequestState(pool, 500, time.Time{}, false)
	rs.acquireRef()
	ns.pendingSnapshotsMu.Lock()
	ns.pendingSnapshots[500] = rs
	ns.pendingSnapshotsMu.Unlock()
	h.addPendingOp()

	// A concurrent completion drains the map and drops the map's reference,
	// exactly as OnSnapshotCompleted does.
	ns.pendingSnapshotsMu.Lock()
	delete(ns.pendingSnapshots, 500)
	ns.pendingSnapshotsMu.Unlock()
	rs.complete(RequestResult{Value: 42})
	rs.releaseRef()
	h.completePendingOp()

	if got := rs.outstanding.Load(); got != 0 {
		t.Fatalf("outstanding after the completion = %d, want 0", got)
	}

	// The rollback path now runs against an entry that is already gone. It
	// must claim before releasing, so the counters stay balanced.
	ns.pendingSnapshotsMu.Lock()
	_, claimed := ns.pendingSnapshots[500]
	if claimed {
		delete(ns.pendingSnapshots, 500)
	}
	ns.pendingSnapshotsMu.Unlock()
	if claimed {
		rs.releaseRef()
		h.completePendingOp()
	}
	rs.Release()

	if got := rs.outstanding.Load(); got != 0 {
		t.Fatalf("outstanding after the rollback = %d, want 0 "+
			"(a negative count permanently blocks tryRecycle)", got)
	}
	if got := h.pendingOps.Load(); got != 0 {
		t.Fatalf("pendingOps = %d, want 0 (double completePendingOp lets Drain "+
			"close drainC with work in flight)", got)
	}
	if !rs.recycled.Load() {
		t.Fatal("RequestState was not returned to the pool")
	}
}

// TestReadIndex_QueueFullRollbackReleasesCallerReference pins the LOW half of
// the same shape on the ReadIndex path: when a concurrent sweep already
// claimed the entry, the caller's own rs.Release() must still run so the
// object can be recycled rather than left for the GC.
func TestReadIndex_QueueFullRollbackReleasesCallerReference(t *testing.T) {
	h := newTestHost(t)
	addTestNodeState(h, 1, 1)
	ns := testLoadNode(h, 1)
	pool := NewRequestStatePool()

	rs := newRequestState(pool, 900, time.Time{}, false)
	rs.acquireRef()
	ns.pendingReadsMu.Lock()
	ns.pendingReads[900] = &pendingRead{rs: rs}
	ns.pendingReadsMu.Unlock()
	h.addPendingOp()

	// A concurrent sweep claims and completes the entry first.
	ns.pendingReadsMu.Lock()
	delete(ns.pendingReads, 900)
	ns.pendingReadsMu.Unlock()
	rs.complete(RequestResult{Err: ErrTimeout})
	rs.releaseRef()
	h.completePendingOp()

	// The delivery-failure rollback: claim first, then always release the
	// caller's own reference.
	ns.pendingReadsMu.Lock()
	_, claimed := ns.pendingReads[900]
	if claimed {
		delete(ns.pendingReads, 900)
	}
	ns.pendingReadsMu.Unlock()
	if claimed {
		rs.releaseRef()
		h.completePendingOp()
	}
	rs.Release()

	if got := rs.outstanding.Load(); got != 0 {
		t.Fatalf("outstanding = %d, want 0", got)
	}
	if !rs.recycled.Load() {
		t.Fatal("the caller's reference was not dropped, so the object never returned to the pool")
	}
}

// ---------------------------------------------------------------------------
// ShardError/ShardLagging/ShardNoLeader must actually be reported
// ---------------------------------------------------------------------------

// TestShardHealth_DerivesLiveState verifies error, no-leader and lagging states are derived from the engine node.
func TestShardHealth_DerivesLiveState(t *testing.T) {
	h := newTestHost(t)

	const shardID, replicaID = uint64(1), uint64(1)
	if err := h.StartShard(map[uint64]string{replicaID: "127.0.0.1:5000"},
		false, testCreateFunc, testShardConfig(shardID, replicaID)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	ns := testLoadNode(h, shardID)

	// No leader known yet.
	ns.leaderID.Store(0)
	if got := h.buildShardInfo(ns).Health; got != ShardNoLeader {
		t.Errorf("health with no leader = %v, want ShardNoLeader", got)
	}

	waitForLeader(t, h, shardID)
	if got := h.buildShardInfo(ns).Health; got != ShardHealthy {
		t.Errorf("health of a led, caught-up shard = %v, want ShardHealthy", got)
	}

	// A state machine far behind commit is lagging; re-inject the committed backlog each tick until it is observed.
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		t.Fatal("engine node missing")
	}
	var lagging ShardHealth
	laggingDeadline := time.Now().Add(10 * time.Second)
	for {
		engNode.SetCommittedIndex(ns.lastApplied.Load() + shardLaggingThreshold)
		lagging = h.buildShardInfo(ns).Health
		if lagging == ShardLagging || time.Now().After(laggingDeadline) {
			break
		}
	}
	if lagging != ShardLagging {
		t.Errorf("health with a %d-entry apply backlog = %v, want ShardLagging",
			shardLaggingThreshold, lagging)
	}

	// ShardUnloaded is a terminal lifecycle state and must not be refined.
	ns.health.Store(uint32(ShardUnloaded))
	if got := h.buildShardInfo(ns).Health; got != ShardUnloaded {
		t.Errorf("health of an unloaded shard = %v, want ShardUnloaded", got)
	}
}

// TestHealthCheck_ReportsDegradedForErroredShard pins the HealthCheck
// contract that live health derivation makes reachable: a shard reporting ShardError degrades the
// whole host so /healthz stops returning 200 for a dead shard.
func TestHealthCheck_ReportsDegradedForErroredShard(t *testing.T) {
	h := newTestHost(t)
	addTestNodeState(h, 1, 1)
	ns := testLoadNode(h, 1)

	if got := h.HealthCheck().Status; got != "ok" {
		t.Fatalf("status of a healthy host = %q, want %q", got, "ok")
	}

	ns.health.Store(uint32(ShardError))
	status := h.HealthCheck()
	if status.Status != "degraded" {
		t.Errorf("status with an errored shard = %q, want %q", status.Status, "degraded")
	}
	if len(status.Shards) != 1 || status.Shards[0].Health != ShardError {
		t.Errorf("ShardInfo.Health = %v, want ShardError", status.Shards[0].Health)
	}
}

// ---------------------------------------------------------------------------
// ExportSnapshot must pin the in-flight epoch while it decrypts
// ---------------------------------------------------------------------------

// TestExportSnapshot_PinsInFlightEpoch verifies ExportSnapshot pins the epochs it reads against a concurrent purge.
func TestExportSnapshot_PinsInFlightEpoch(t *testing.T) {
	cfg := testHostConfig(t)
	db := newMockLogDB()
	real := createTestBarrier(t)
	pinning := &pinRecordingBarrier{Barrier: real}

	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithBarrier(pinning))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"},
		false, testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}

	snapDir := filepath.Join(t.TempDir(), "snap-encrypted")
	if mkErr := os.MkdirAll(snapDir, 0o750); mkErr != nil {
		t.Fatalf("MkdirAll: %v", mkErr)
	}
	plaintext := []byte("export must pin the epoch it is decrypting under")
	writeEncryptedSnapshotFileFor(t, pinning, filepath.Join(snapDir, "snapshot.dat"), plaintext, 1, 1, 900)
	stampTestSnapshotMeta(t, filepath.Dir(filepath.Join(snapDir, "snapshot.dat")), 1, 1)

	const recordedEpoch = uint64(1)
	db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 900, Filepath: snapDir, Epoch: recordedEpoch})

	var buf bytes.Buffer
	if exportErr := h.ExportSnapshot(context.Background(), 1, &buf); exportErr != nil {
		t.Fatalf("ExportSnapshot: %v", exportErr)
	}

	pinning.mu.Lock()
	registered := append([]uint64(nil), pinning.registered...)
	deregistered := append([]uint64(nil), pinning.deregistered...)
	pinning.mu.Unlock()

	if !containsEpoch(registered, recordedEpoch) {
		t.Fatalf("registered epochs = %v, want the recorded snapshot epoch %d pinned",
			registered, recordedEpoch)
	}
	if !containsEpoch(deregistered, recordedEpoch) {
		t.Fatalf("deregistered epochs = %v, want epoch %d released after the export",
			deregistered, recordedEpoch)
	}
}

func containsEpoch(epochs []uint64, want uint64) bool {
	for _, e := range epochs {
		if e == want {
			return true
		}
	}
	return false
}

// TestImportSnapshot_PinsInFlightEpoch verifies ImportSnapshot pins its epoch against a concurrent rotate and purge.
func TestImportSnapshot_PinsInFlightEpoch(t *testing.T) {
	cfg := testHostConfig(t)
	db := newMockLogDB()
	real := createTestBarrier(t)
	pinning := &pinRecordingBarrier{Barrier: real}

	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithBarrier(pinning))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"},
		false, testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}

	// Produce a valid exported artifact to import back.
	snapDir := filepath.Join(t.TempDir(), "snap-import-pin")
	if mkErr := os.MkdirAll(snapDir, 0o750); mkErr != nil {
		t.Fatalf("MkdirAll: %v", mkErr)
	}
	writeEncryptedSnapshotFileFor(t, pinning, filepath.Join(snapDir, "snapshot.dat"),
		[]byte("import must pin the epoch it seals under"), 1, 1, 1700)
	stampTestSnapshotMeta(t, snapDir, 1, 1)
	db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 1700, Filepath: snapDir, Epoch: 1})

	var exported bytes.Buffer
	if exportErr := h.ExportSnapshot(context.Background(), 1, &exported); exportErr != nil {
		t.Fatalf("ExportSnapshot: %v", exportErr)
	}

	if stopErr := h.StopShard(1); stopErr != nil {
		t.Fatalf("StopShard: %v", stopErr)
	}
	db.mu.Lock()
	db.nodeInfos = []logdb.NodeInfo{{ShardID: 1, ReplicaID: 1}}
	delete(db.snapshots, mockNodeKey{1, 1})
	db.mu.Unlock()

	// Only observe the import's pinning, not the export's -- nor the fixture
	// write above, which seals frames through this same recording barrier
	// before any pin exists and would otherwise arm sawEncryptBeforePin.
	pinning.mu.Lock()
	pinning.registered = nil
	pinning.deregistered = nil
	pinning.sawEncryptBeforePin = false
	pinning.mu.Unlock()

	if importErr := h.ImportSnapshot(context.Background(), 1, bytes.NewReader(exported.Bytes())); importErr != nil {
		t.Fatalf("ImportSnapshot: %v", importErr)
	}

	snap, snapErr := db.GetSnapshot(1, 1)
	if snapErr != nil {
		t.Fatalf("GetSnapshot after import: %v", snapErr)
	}
	if snap.Epoch == 0 {
		t.Fatal("import recorded Epoch 0 for a shard WITH a barrier: the pinning " +
			"contract is not reachable and the test would pass vacuously")
	}

	pinning.mu.Lock()
	registered := append([]uint64(nil), pinning.registered...)
	deregistered := append([]uint64(nil), pinning.deregistered...)
	pinning.mu.Unlock()

	if !containsEpoch(registered, snap.Epoch) {
		t.Fatalf("registered epochs = %v, want the recorded snapshot epoch %d pinned "+
			"across the import; a concurrent purge could otherwise drop its DEK before "+
			"SaveSnapshot records it", registered, snap.Epoch)
	}
	if !containsEpoch(deregistered, snap.Epoch) {
		t.Fatalf("deregistered epochs = %v, want epoch %d released after the import",
			deregistered, snap.Epoch)
	}
	// The pin must be armed before re-encryption.
	pinning.mu.Lock()
	sawEarly := pinning.sawEncryptBeforePin
	pinning.mu.Unlock()
	if sawEarly {
		t.Fatal("a snapshot frame was encrypted BEFORE any in-flight epoch was pinned: " +
			"the import pins only after re-encryption, leaving the frames exposed to a " +
			"concurrent rotate+purge for the duration of the encrypt loop")
	}
}

// ---------------------------------------------------------------------------
// ImportSnapshot must bound the artifact and map context errors
// ---------------------------------------------------------------------------

// TestImportSnapshot_RejectsWhenDiskFull pins the disk-full precondition: the
// import writes a full snapshot artifact, so starting one while the WAL disk
// monitor reports the volume full would only deepen the exhaustion and leave
// a truncated artifact behind.
func TestImportSnapshot_RejectsWhenDiskFull(t *testing.T) {
	h := newTestHost(t)
	h.diskFull.Store(true)
	t.Cleanup(func() { h.diskFull.Store(false) })

	err := h.ImportSnapshot(context.Background(), 1, bytes.NewReader([]byte("data")))
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("error = %v, want ErrDiskFull", err)
	}
}

// TestSnapshotTooLargeError_MatchesSentinel pins the typed/sentinel pairing
// used by the bounded ImportSnapshot copy.
func TestSnapshotTooLargeError_MatchesSentinel(t *testing.T) {
	err := error(&SnapshotTooLargeError{ShardID: 3, MaxSize: config.DefaultMaxSnapshotSize})
	if !errors.Is(err, ErrSnapshotTooLarge) {
		t.Errorf("errors.Is(%v, ErrSnapshotTooLarge) = false, want true", err)
	}
	if err.Error() == "" {
		t.Error("Error() returned an empty string")
	}
}

// ---------------------------------------------------------------------------
// FollowerRead's slow path must observe shutdown
// ---------------------------------------------------------------------------

// pollReachedCtx closes reached on its first Done call, which FollowerRead makes only in its slow-path poll loop.
type pollReachedCtx struct {
	context.Context
	once    sync.Once
	reached chan struct{}
}

func (c *pollReachedCtx) Done() <-chan struct{} {
	c.once.Do(func() { close(c.reached) })
	return c.Context.Done()
}

func TestFollowerRead_SlowPathExitsOnShardStop(t *testing.T) {
	h := newTestHost(t)

	const shardID, replicaID = uint64(1), uint64(1)
	if err := h.StartShard(map[uint64]string{replicaID: "127.0.0.1:5000"},
		false, testCreateFunc, testShardConfig(shardID, replicaID)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)

	ns := testLoadNode(h, shardID)

	// Force the slow path by injecting lag on the applied side; see injectApplyLag.
	proposeCtx, proposeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer proposeCancel()
	if _, err := h.SyncPropose(proposeCtx, shardID, []byte("committed-write")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}
	injectApplyLag(t, h, shardID)

	done := make(chan error, 1)
	// A deadline-less context: only the shutdown check can end this call.
	ctx := &pollReachedCtx{Context: context.Background(), reached: make(chan struct{})}
	go func() {
		_, err := h.FollowerRead(ctx, shardID, "k")
		done <- err
	}()

	// Wait for the reader to reach the poll loop, then stop the shard.
	select {
	case <-ctx.reached:
	case err := <-done:
		t.Fatalf("FollowerRead returned %v before reaching the poll loop", err)
	case <-time.After(5 * time.Second):
		t.Fatal("FollowerRead never reached the poll loop")
	}
	ns.stopped.Store(true)

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("FollowerRead error = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FollowerRead did not observe the shard stopping; it spins until ctx.Done()")
	}
}

// ---------------------------------------------------------------------------
// The exported TransportDisabled field must actually disable transport
// ---------------------------------------------------------------------------

// TestNewHost_ExportedTransportDisabledSkipsTransport pins the gate. The field
// is exported, so a caller can set it without going through WithoutTransport;
// config validation then skips the MTLSConfig requirement, so transport
// creation has to honour the same flag. Otherwise the field would do nothing
// but disable the mTLS check — exactly backwards.
func TestNewHost_ExportedTransportDisabledSkipsTransport(t *testing.T) {
	cfg := testHostConfig(t)
	cfg.TransportConfig.TransportDisabled = true

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	if h.transport != nil {
		t.Fatal("transport was created despite TransportConfig.TransportDisabled")
	}
}

// TestProposalKeysAreUniqueAcrossTheShard pins that proposal keys are unique across replicas and incarnations.
func TestProposalKeysAreUniqueAcrossTheShard(t *testing.T) {
	// Three hosts stand for replicas A, B and A's next incarnation; keys are random, so no state is needed.
	hosts := map[string]*Host{"A": {}, "B": {}, "A-restarted": {}}
	seen := map[uint64]string{}
	for range 1 << 16 {
		for id := range hosts {
			key := nextRequestKey()
			if key == 0 {
				t.Fatalf("host %s minted key 0, which marks an entry nothing is waiting on", id)
			}
			if prev, dup := seen[key]; dup {
				t.Fatalf("hosts %s and %s both minted key %d: an entry proposed by one "+
					"completes the other's unrelated pending request", prev, id, key)
			}
			seen[key] = id
		}
	}
}

// TestConfigRejectsReplicaIDThatWouldTruncateTheProposalKey pins the bound that
// keeps the packing exact. Without it a replica ID above 2^32 would silently
// overlap the sequence half and reintroduce the collision.
func TestConfigRejectsReplicaIDThatWouldTruncateTheProposalKey(t *testing.T) {
	cfg := testShardConfig(1, 1)
	cfg.ReplicaID = 1 << 32
	if err := cfg.Validate(); err == nil {
		t.Fatal("a ReplicaID that overlaps the proposal key's sequence half was " +
			"accepted: its keys silently collide with another replica's")
	}
}

// TestProposeRejectsAlreadyCanceledContext verifies submission refuses a done context deterministically and appends nothing.
func TestProposeRejectsAlreadyCanceledContext(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	before, err := h.LogRange(1)
	if err != nil {
		t.Fatalf("LogRange failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Repeated because the defect was probabilistic: one attempt against the
	// old code passed roughly half the time.
	const attempts = 50
	for i := range attempts {
		if _, err := h.Propose(ctx, 1, []byte("canceled")); !errors.Is(err, ErrCanceled) {
			t.Fatalf("attempt %d: Propose with a canceled context returned %v, want ErrCanceled", i, err)
		}
		if _, err := h.SyncPropose(ctx, 1, []byte("canceled")); !errors.Is(err, ErrCanceled) {
			t.Fatalf("attempt %d: SyncPropose with a canceled context returned %v, want ErrCanceled", i, err)
		}
		if _, err := h.ReadIndex(ctx, 1); !errors.Is(err, ErrCanceled) {
			t.Fatalf("attempt %d: ReadIndex with a canceled context returned %v, want ErrCanceled", i, err)
		}
	}

	after, err := h.LogRange(1)
	if err != nil {
		t.Fatalf("LogRange failed: %v", err)
	}
	if after.LastIndex != before.LastIndex {
		t.Fatalf("a canceled proposal still reached the log: last index moved %d -> %d",
			before.LastIndex, after.LastIndex)
	}
}

// TestCancelledConfigChange_SubmitsNothing verifies a cancelled config change returns ErrCanceled and leaves log and membership unchanged.
func TestCancelledConfigChange_SubmitsNothing(t *testing.T) {
	h := newTestHost(t)

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	before, err := h.ShardMembership(1)
	if err != nil {
		t.Fatalf("ShardMembership: %v", err)
	}
	// Take the baseline from a published diagnostics snapshot; the first DiagRaftState call only arms publication.
	waitForDiagLive(t, h, 1)
	rsBefore, err := h.DiagRaftState(1)
	if err != nil {
		t.Fatalf("DiagRaftState: %v", err)
	}
	lastIdxBefore := rsBefore.LastIndex

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	addErr := h.SyncRequestAddNode(ctx, 1, 2, "127.0.0.1:5001", 0)
	if !errors.Is(addErr, ErrCanceled) {
		t.Fatalf("SyncRequestAddNode with an already-cancelled context "+
			"returned %v, want ErrCanceled", addErr)
	}

	// Give any entry that WAS submitted time to commit and apply, so the
	// assertion below cannot pass merely by racing an in-flight append. On a
	// single-node shard commit is immediate; this drains the pipeline rather
	// than guessing at a duration.
	if !waitUntil(3*time.Second, func() bool {
		p, derr := h.DiagPipelineState(1)
		if derr != nil {
			t.Fatalf("DiagPipelineState: %v", derr)
		}
		return p == (PipelineDiag{})
	}) {
		t.Fatal("the shard's pipeline never drained")
	}

	rsAfter, err := h.DiagRaftState(1)
	if err != nil {
		t.Fatalf("DiagRaftState after: %v", err)
	}
	lastIdxAfter := rsAfter.LastIndex
	if lastIdxAfter != lastIdxBefore {
		t.Fatalf("a cancelled config change appended to the log: last index "+
			"moved %d -> %d. ErrCanceled told the caller the membership was "+
			"unchanged while the change was committing",
			lastIdxBefore, lastIdxAfter)
	}

	after, err := h.ShardMembership(1)
	if err != nil {
		t.Fatalf("ShardMembership after: %v", err)
	}
	if len(after.Nodes) != len(before.Nodes) {
		t.Fatalf("a cancelled config change altered the membership: %d -> %d nodes",
			len(before.Nodes), len(after.Nodes))
	}
	if _, added := after.Nodes[2]; added {
		t.Fatal("a cancelled SyncRequestAddNode added the replica anyway")
	}
}
