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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// ---------------------------------------------------------------------------
// H1: speculative address pre-registration must restore, not delete
// ---------------------------------------------------------------------------

// TestRequestConfigChange_RollbackRestoresPriorAddress is the H1 regression
// test. Observer -> voter promotion targets a replicaID that ALREADY has a
// live registry entry. requestConfigChange pre-registers the new address
// speculatively (Register is a Swap), so a rollback that unconditionally
// Removed the entry deleted the address of a replica that is still a member
// and still being replicated to. A follower/observer never initiates traffic,
// so learnSourceAddress cannot heal the deletion: the replica falls behind
// until the next committed config change, a snapshot recovery, or a restart.
//
// The rollback must restore the PRIOR address instead. This exercises the
// failure-completion wiring too: the config change is appended but never
// commits (the leader is deposed), and failPendingRequests runs the hook.
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

	// Commit a second voter so quorum becomes 2. This entry commits under
	// the OLD single-voter config, but from here on nothing else can commit:
	// replica 2 does not exist, so no further entry ever reaches quorum.
	// That makes "appended but never committed" DETERMINISTIC — without it
	// the shard is single-node, the config change below commits immediately,
	// and the rollback under test never runs.
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
// H1: when the target replica had NO prior registry entry, the rollback of a
// never-committed add must remove the speculative entry rather than leave a
// mapping for a replica that never became a member.
func TestRequestConfigChange_RollbackRemovesFirstTimeAdd(t *testing.T) {
	h := newTestHost(t)

	const shardID, localReplicaID, newReplicaID = uint64(1), uint64(1), uint64(3)
	const newAddr = "127.0.0.1:7003"

	if err := h.StartShard(map[uint64]string{localReplicaID: "127.0.0.1:5000"},
		false, testCreateFunc, testShardConfig(shardID, localReplicaID)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)

	if _, resolveErr := h.registry.Resolve(shardID, newReplicaID); resolveErr == nil {
		t.Fatal("precondition: the new replica must have no registry entry")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rs, err := h.RequestAddNode(ctx, shardID, newReplicaID, newAddr, 0)
	if err != nil {
		t.Fatalf("RequestAddNode: %v", err)
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
// M4: rollbacks must claim the pending entry before releasing references
// ---------------------------------------------------------------------------

// TestRequestSnapshot_RollbackDoesNotDoubleReleaseOnConcurrentCompletion is
// the M4 regression test for the snapshot path. OnSnapshotCompleted drains
// the ENTIRE pendingSnapshots map on any completion for the shard, so a
// completion can complete, remove and release an entry between the Store in
// RequestSnapshot and its submit-failure rollback. An unconditional
// releaseRef there drove outstanding to -1, which permanently fails
// tryRecycle's outstanding==0 check (the object never returns to the pool),
// and double-counted completePendingOp so Drain could close drainC with work
// still in flight.
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
// M5: ShardError/ShardLagging/ShardNoLeader must actually be reported
// ---------------------------------------------------------------------------

// TestShardHealth_DerivesLiveState is the M5 regression test. ns.health only
// records the coarse lifecycle state (healthy once started, unloaded once
// stopped), so ShardError, ShardNoLeader and ShardLagging were never assigned
// and HealthCheck's "degraded" branch was dead code: a wedged shard still
// reported ok/200 and orchestrators kept routing writes to it. The live
// conditions are derived from the engine node instead.
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

	// A state machine far behind the committed index is lagging.
	//
	// This backlog has to be injected on the COMMITTED side: the threshold is
	// a thousand entries and a freshly elected single-node shard has applied
	// one, so there is no room to move the applied marker back instead (which
	// is what the follower-read fixtures do — see injectApplyLag). The step
	// worker republishes the real committed index from the raft log on EVERY
	// tick (Node.publishLogMarkers), so an injected value survives at most one
	// tick interval. Re-inject until the derivation is observed rather than
	// racing that window once: the loop can only ever end by shardHealth
	// actually returning ShardLagging for an injected backlog.
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
// contract that M5 made reachable: a shard reporting ShardError degrades the
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
// M6: ExportSnapshot must pin the in-flight epoch while it decrypts
// ---------------------------------------------------------------------------

// TestExportSnapshot_PinsInFlightEpoch is the M6 regression test.
// decryptSnapshotForSend and ReencryptSnapshotFile both pin the epochs they
// read, but ExportSnapshot decrypted with no pin at all: a long multi-GB
// export racing keyRotationLoop -> rotateBarrierKey -> PurgeEpochsBefore
// could have mid-stream frames fail AEAD, because GlobalSafePurgeFloor does
// not account for an in-progress export. The operator's disaster-recovery
// export would silently fail.
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

// TestImportSnapshot_PinsInFlightEpoch is the regression test for the one at-rest
// persistence path that pinned nothing. ImportSnapshot re-encrypts the imported
// plaintext under the shard's barrier at the current epoch and records that epoch
// in LogDB — but the shard is STOPPED during the import, so the purge-floor scan
// (which counts only LOADED nodes) contributes zero floor for it. Without a pin,
// a concurrent Rotate + purge on another shard sharing the barrier can drop the
// epoch's DEK between the re-encryption and the SaveSnapshot, leaving the recorded
// snapshot permanently undecryptable — total loss of a disaster-recovery restore.
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
	// The pin must be armed BEFORE re-encryption, not only after: a rotate+purge
	// landing mid-loop would otherwise strand frames sealed under a dropped DEK.
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
// LOW: ImportSnapshot must bound the artifact and map context errors
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
// LOW: FollowerRead's slow path must observe shutdown
// ---------------------------------------------------------------------------

// TestFollowerRead_SlowPathExitsOnShardStop pins the shutdown exit. The poll
// loop previously had no terminator other than ctx.Done(), so a caller with a
// deadline-less context on a partitioned follower spun at 1000Hz forever and
// Host.Close could not interrupt it.
func TestFollowerRead_SlowPathExitsOnShardStop(t *testing.T) {
	h := newTestHost(t)

	const shardID, replicaID = uint64(1), uint64(1)
	if err := h.StartShard(map[uint64]string{replicaID: "127.0.0.1:5000"},
		false, testCreateFunc, testShardConfig(shardID, replicaID)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)

	ns := testLoadNode(h, shardID)

	// Force the slow path. The lag is injected on the APPLIED side: an injected
	// committed index is republished from the raft log on the next step-worker
	// tick, after which the read completes on the FAST path and the shutdown
	// exit this test exists for is never reached. See injectApplyLag.
	proposeCtx, proposeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer proposeCancel()
	if _, err := h.SyncPropose(proposeCtx, shardID, []byte("committed-write")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}
	injectApplyLag(t, h, shardID)

	done := make(chan error, 1)
	go func() {
		// A deadline-less context: only the shutdown check can end this call.
		_, err := h.FollowerRead(context.Background(), shardID, "k")
		done <- err
	}()

	// Let the reader reach the poll loop, then stop the shard.
	time.Sleep(20 * time.Millisecond)
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
// LOW: the exported TransportDisabled field must actually disable transport
// ---------------------------------------------------------------------------

// TestNewHost_ExportedTransportDisabledSkipsTransport pins the gate. The field
// is exported, so a caller can set it without going through WithoutTransport;
// config validation then skips the MTLSConfig requirement, so transport
// creation has to honour the same flag. Otherwise the field would do nothing
// but disable the mTLS check — exactly backwards.
func TestNewHost_ExportedTransportDisabledSkipsTransport(t *testing.T) {
	cfg := testHostConfig(t)
	cfg.TransportConfig.TransportDisabled = true

	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	if h.transport != nil {
		t.Fatal("transport was created despite TransportConfig.TransportDisabled")
	}
}

// TestProposalKeysAreUniqueAcrossTheShard pins the identity of a proposal key.
//
// entry.Key is how an applied entry is matched back to the pending RequestState
// that proposed it, and the key travels through the raft log to every replica.
// A per-host counter collides by construction: every replica minted 1, 2, 3, so
// replica B -- with its own request pending under key 7 -- completed that caller
// the moment it applied replica A's unrelated entry also carrying key 7. B's
// client received a success and A's RESULT VALUE for an operation it never
// issued, and B's real entry later found no pending proposal at all.
func TestProposalKeysAreUniqueAcrossTheShard(t *testing.T) {
	hostA, hostB := &Host{}, &Host{}
	const replicaA, replicaB = uint64(1), uint64(2)

	seen := map[uint64]string{}
	for range 64 {
		ka := hostA.nextProposalKey(replicaA)
		kb := hostB.nextProposalKey(replicaB)
		for id, key := range map[string]uint64{"A": ka, "B": kb} {
			if prev, dup := seen[key]; dup {
				t.Fatalf("replicas %s and %s both minted proposal key %d: an entry "+
					"proposed by one completes the other's unrelated pending request",
					prev, id, key)
			}
			seen[key] = id
		}
	}

	// The replica ID must be recoverable, which is what makes the halves
	// non-overlapping rather than merely unlikely to collide.
	if got := hostA.nextProposalKey(replicaA) >> proposalKeySeqBits; got != replicaA {
		t.Fatalf("replica ID half = %d, want %d", got, replicaA)
	}

	// A wrapped sequence stays within its own replica's half, so it can never
	// collide with another replica no matter how long the host runs.
	hostA.proposalKeySeq.Store(1<<proposalKeySeqBits - 1)
	wrapped := hostA.nextProposalKey(replicaA)
	if got := wrapped >> proposalKeySeqBits; got != replicaA {
		t.Fatalf("a wrapped sequence escaped its replica's half: key %d has replica "+
			"half %d, want %d", wrapped, got, replicaA)
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
