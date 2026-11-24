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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// waitUntil polls cond until it returns true or the timeout expires,
// yielding the processor between polls. Returns false on timeout.
func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		runtime.Gosched()
	}
}

// ---------------------------------------------------------------------------
// sweepPendingReads must expire a batch key's unreachable aliases
// ---------------------------------------------------------------------------

// TestSweepPendingReads_ExpiresUnconfirmedAliasGroup checks expiring an unconfirmed batch key also expires its aliases.
func TestSweepPendingReads_ExpiresUnconfirmedAliasGroup(t *testing.T) {
	h := newTestHost(t)
	addTestNodeState(h, 1, 1)
	ns := testLoadNode(h, 1)
	pool := NewRequestStatePool()

	// Batch key with a passed deadline; two deadline-less aliases.
	deadline := time.Now().Add(-1 * time.Second)
	batchRS := newRequestState(pool, 100, deadline, true)
	alias1RS := newRequestState(pool, 101, time.Time{}, true)
	alias2RS := newRequestState(pool, 102, time.Time{}, true)
	batchRS.acquireRef()
	alias1RS.acquireRef()
	alias2RS.acquireRef()

	ns.pendingReadsMu.Lock()
	ns.pendingReads[100] = &pendingRead{rs: batchRS, aliases: []uint64{101, 102}}
	ns.pendingReads[101] = &pendingRead{rs: alias1RS}
	ns.pendingReads[102] = &pendingRead{rs: alias2RS}
	ns.pendingReadsMu.Unlock()
	h.addPendingOp()
	h.addPendingOp()
	h.addPendingOp()

	h.sweepExpiredPending()

	for i, rs := range []*RequestState{batchRS, alias1RS, alias2RS} {
		select {
		case result := <-rs.ResultC():
			if !errors.Is(result.Err, ErrTimeout) {
				t.Errorf("read %d result.Err = %v, want ErrTimeout", i, result.Err)
			}
		default:
			t.Fatalf("read %d was not completed — orphaned alias would hang forever", i)
		}
		rs.Release()
	}

	ns.pendingReadsMu.Lock()
	remaining := len(ns.pendingReads)
	ns.pendingReadsMu.Unlock()
	if remaining != 0 {
		t.Errorf("pendingReads has %d entries, want 0", remaining)
	}
	if got := h.pendingOps.Load(); got != 0 {
		t.Errorf("pendingOps = %d, want 0", got)
	}
}

// TestSweepPendingReads_KeepsConfirmedAliases checks confirmed aliases survive their batch key's expiry.
func TestSweepPendingReads_KeepsConfirmedAliases(t *testing.T) {
	h := newTestHost(t)
	addTestNodeState(h, 1, 1)
	ns := testLoadNode(h, 1)
	pool := NewRequestStatePool()

	deadline := time.Now().Add(-1 * time.Second)
	batchRS := newRequestState(pool, 200, deadline, true)
	aliasRS := newRequestState(pool, 201, time.Time{}, true)
	batchRS.acquireRef()
	aliasRS.acquireRef()

	// Confirmation happens for the batch key and all aliases atomically
	// under pendingReadsMu (OnReadyToRead), so a confirmed alias always
	// coexists with a confirmed batch entry.
	ns.pendingReadsMu.Lock()
	ns.pendingReads[200] = &pendingRead{rs: batchRS, aliases: []uint64{201}, confirmed: true, readIndex: 7}
	ns.pendingReads[201] = &pendingRead{rs: aliasRS, confirmed: true, readIndex: 7}
	ns.pendingReadsMu.Unlock()
	h.addPendingOp()
	h.addPendingOp()

	h.sweepExpiredPending()

	// The expired batch key is gone; the confirmed alias survives.
	ns.pendingReadsMu.Lock()
	_, batchPresent := ns.pendingReads[200]
	_, aliasPresent := ns.pendingReads[201]
	ns.pendingReadsMu.Unlock()
	if batchPresent {
		t.Error("expired batch key should have been removed")
	}
	if !aliasPresent {
		t.Fatal("confirmed alias must survive the batch key's expiry (OnApplied completes it)")
	}

	// The OnApplied read scan completes the surviving confirmed alias.
	cb := &hostEngineCallback{h: h}
	cb.OnApplied(1, []proto.Entry{{Key: 999, Index: 8}}, []sm.Result{{}})

	select {
	case result := <-aliasRS.ResultC():
		if result.Err != nil {
			t.Errorf("alias result.Err = %v, want nil", result.Err)
		}
		if result.Value != 7 {
			t.Errorf("alias result.Value = %d, want readIndex 7", result.Value)
		}
	default:
		t.Fatal("confirmed alias was not completed by OnApplied")
	}
	batchRS.Release()
	aliasRS.Release()
}

// TestOnReadIndexBatched_BatchKeyAlreadyRemovedFailsAliases checks aliases of an already-swept batch key are failed.
func TestOnReadIndexBatched_BatchKeyAlreadyRemovedFailsAliases(t *testing.T) {
	h := newTestHost(t)
	addTestNodeState(h, 1, 1)
	ns := testLoadNode(h, 1)
	pool := NewRequestStatePool()

	aliasRS := newRequestState(pool, 301, time.Time{}, true)
	aliasRS.acquireRef()
	ns.pendingReadsMu.Lock()
	ns.pendingReads[301] = &pendingRead{rs: aliasRS} // batch key 300 absent
	ns.pendingReadsMu.Unlock()
	h.addPendingOp()

	cb := &hostEngineCallback{h: h}
	cb.OnReadIndexBatched(1, 300, []uint64{301})

	select {
	case result := <-aliasRS.ResultC():
		if !errors.Is(result.Err, ErrTimeout) {
			t.Errorf("alias result.Err = %v, want ErrTimeout", result.Err)
		}
	default:
		t.Fatal("alias attached to a removed batch key was not failed — deadline-less caller would hang")
	}
	aliasRS.Release()
	if got := h.pendingOps.Load(); got != 0 {
		t.Errorf("pendingOps = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// Registration racing Host.Close must never orphan the caller
// ---------------------------------------------------------------------------

// TestPendingRegistration_CloseRace_CallerNeverOrphaned is a
// regression test: ReadIndex, RequestSnapshot, and config-change
// registrations racing Host.Close() must always resolve — either with
// ErrClosed from the post-store re-check, or with a completion from the
// close sweep — never leaving a deadline-less caller blocked forever.
func TestPendingRegistration_CloseRace_CallerNeverOrphaned(t *testing.T) {
	for iter := 0; iter < 8; iter++ {
		cfg := testHostConfig(t)
		h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost failed: %v", err)
		}
		members := map[uint64]string{1: "127.0.0.1:5000"}
		scfg := testShardConfig(1, 1)
		if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard failed: %v", err)
		}
		waitForLeader(t, h, 1)

		ctx := context.Background()
		var wg sync.WaitGroup
		ops := []func() (*RequestState, error){
			func() (*RequestState, error) { return h.ReadIndex(ctx, 1) },
			func() (*RequestState, error) { return h.RequestSnapshot(ctx, 1, SnapshotOption{}) },
			func() (*RequestState, error) {
				return h.RequestAddNode(ctx, 1, uint64(100+iter), "127.0.0.1:9999", 0)
			},
			func() (*RequestState, error) { return h.ReadIndex(ctx, 1) },
		}
		for _, op := range ops {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rs, opErr := op()
				if opErr != nil {
					return // rejected synchronously — nothing pending
				}
				select {
				case <-rs.ResultC():
					// resolved (success, ErrClosed, timeout, ...)
				case <-time.After(20 * time.Second):
					t.Error("registration racing Close was orphaned — caller blocked forever")
				}
				rs.Release()
			}()
		}
		// ErrShutdownTimeout is an allowed Close result here; any other Close error fails.
		if err := h.Close(); err != nil && !errors.Is(err, ErrShutdownTimeout) {
			t.Fatalf("Close failed: %v", err)
		}
		wg.Wait()

		if got := h.pendingOps.Load(); got != 0 {
			t.Fatalf("iter %d: pendingOps = %d, want 0 after Close", iter, got)
		}
	}
}

// ---------------------------------------------------------------------------
// StartShard bootstrap failure must mark the nodeState stopped
// ---------------------------------------------------------------------------

// stashBootstrapLogDB fails SaveBootstrap after stashing the nodeState the
// Host published for the shard, so the test can assert its post-failure
// state.
type stashBootstrapLogDB struct {
	mockLogDB
	h       *Host
	stashed *nodeState
}

func (f *stashBootstrapLogDB) SaveBootstrap(shardID, replicaID uint64, bootstrap logdb.Bootstrap) error {
	if v, ok := f.h.nodes.Load(shardID); ok {
		if ns, nsOK := v.(*nodeState); nsOK {
			f.stashed = ns
		}
	}
	return errors.New("bootstrap write failed")
}

// TestStartShard_BootstrapFailureMarksNodeStateStopped checks ns.stopped is set before the failed node is deleted.
func TestStartShard_BootstrapFailureMarksNodeStateStopped(t *testing.T) {
	cfg := testHostConfig(t)
	db := &stashBootstrapLogDB{mockLogDB: *newMockLogDB()}
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()
	db.h = h

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	err = h.StartShard(members, false, testCreateFunc, scfg)
	if !errors.Is(err, ErrLogDBWrite) {
		t.Fatalf("StartShard error = %v, want ErrLogDBWrite", err)
	}
	if db.stashed == nil {
		t.Fatal("SaveBootstrap never observed the published nodeState")
	}
	if !db.stashed.stopped.Load() {
		t.Fatal("nodeState.stopped = false after bootstrap failure; a racing reader could call into the closing state machine")
	}
}

// ---------------------------------------------------------------------------
// Queue-full rollbacks must claim with LoadAndDelete
// ---------------------------------------------------------------------------

// TestProposeEntry_QueueFullRollback_RacesSweeper checks exactly one of rollback and sweeper releases the entry, so pool and pendingOps balance.
func TestProposeEntry_QueueFullRollback_RacesSweeper(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	ns := testLoadNode(h, 1)
	engNode := h.engine.GetNode(1)
	if engNode == nil {
		t.Fatal("engine node should not be nil")
	}

	// Mark the node recovering so the step worker does not drain the queue between the fill and the proposal.
	engNode.SetRecovering(true)
	defer func() {
		// Discard the entries the test injected before handing the queue
		// back to the step worker. Safe as the single consumer: the worker
		// still skips this node while recovering is set.
		engNode.DrainProposals(nil)
		engNode.SetRecovering(false)
	}()

	// Fill the proposal queue to its exact capacity through the real API.
	filled := 0
	for engNode.DeliverProposal([]proto.Entry{{Type: proto.EntryNormal}}) {
		filled++
	}
	if filled == 0 {
		t.Fatal("proposal queue rejected the first entry; nothing was filled")
	}

	// Pin the RequestState storage for this shard so every proposal below
	// uses the same object and its reference count can be inspected
	// directly. newRequestState resets the object on every handout, so the
	// production semantics are unchanged; only the pool's identity choice
	// is made deterministic.
	pool, storage := pinnedStatePool()
	ns.requestPool = pool

	if !waitUntil(5*time.Second, func() bool { return h.pendingOps.Load() == 0 }) {
		t.Fatalf("pendingOps = %d before the test began; baseline must be 0", h.pendingOps.Load())
	}

	// Hammer the sweeper concurrently with expired-deadline proposals that
	// hit the queue-full rollback. Every proposal registered with an
	// already-passed deadline is immediately eligible for the sweep, so the
	// sweep and the rollback race for the same claim.
	stop := make(chan struct{})
	var sweeperWG sync.WaitGroup
	sweeperWG.Add(1)
	go func() {
		defer sweeperWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				h.sweepExpiredPending()
			}
		}
	}()

	expiredCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
	defer cancel()

	// Run until the sweeper has demonstrably won the claim race enough
	// times (that is the branch where an unconditional Delete double-rolls
	// back), bounded by a wall-clock deadline.
	const wantSweeperWins = 20
	rollbacks, sweeperWins := 0, 0
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && sweeperWins < wantSweeperWins {
		rs, propErr := h.proposeEntry(expiredCtx, ns, 1, proto.Entry{Type: proto.EntryNormal, Cmd: []byte("x")}, nil)
		// ErrDropped is the queue-overflow answer; ErrSystemBusy is the
		// global rate limiter, which is not what is being exercised here.
		if !errors.Is(propErr, ErrDropped) {
			if rs != nil {
				rs.Release()
			}
			t.Fatalf("proposeEntry with a full proposal queue: error = %v, want ErrDropped", propErr)
		}
		rollbacks++

		// Fence before inspecting the shared storage: both claim sites run
		// releaseRef immediately before completePendingOp, and the caller's
		// Release runs inside proposeEntry, so a zero pendingOps means the
		// RequestState has fully settled on both goroutines.
		if !waitUntil(5*time.Second, func() bool { return h.pendingOps.Load() == 0 }) {
			t.Fatalf("rollback %d: pendingOps = %d, want 0; the claimed entry was never completed",
				rollbacks, h.pendingOps.Load())
		}

		// Exactly one of the rollback and the sweeper claims the entry and
		// drops the pending-map reference, so the object always makes it
		// back to the pool. A double releaseRef drives outstanding to -1,
		// and tryRecycle then never puts the object back.
		if got := storage.outstanding.Load(); got != 0 {
			t.Fatalf("rollback %d: RequestState outstanding = %d, want 0; the rollback and the sweeper both dropped the pending-map reference", rollbacks, got)
		}
		if !storage.recycled.Load() {
			t.Fatalf("rollback %d: RequestState never returned to the pool (outstanding = %d, released = %v)",
				rollbacks, storage.outstanding.Load(), storage.released.Load())
		}
		if n := countSyncMap(&ns.pendingProposals); n != 0 {
			t.Fatalf("rollback %d: pendingProposals holds %d entries, want 0", rollbacks, n)
		}
		// The sweeper completes the RequestState when it wins the claim;
		// the rollback path never completes it. Under-counts when the
		// caller's Release beats the sweeper's complete, which only makes
		// this gate stricter.
		if storage.completed.Load() {
			sweeperWins++
		}
	}
	close(stop)
	sweeperWG.Wait()

	if rollbacks == 0 {
		t.Fatal("no queue-full rollback was exercised")
	}
	if sweeperWins == 0 {
		t.Fatalf("the sweeper never won the claim race over %d rollbacks; the contended branch was not exercised", rollbacks)
	}
	t.Logf("queue-full rollbacks: %d (%d claimed by the sweeper)", rollbacks, sweeperWins)

	// Let in-flight sweeps quiesce, then require an exact balance: a double
	// completePendingOp undercounts the pending operations so Drain can
	// close early.
	if !waitUntil(5*time.Second, func() bool {
		h.sweepExpiredPending()
		return h.pendingOps.Load() == 0
	}) {
		t.Fatalf("pendingOps = %d after %d rollbacks (%d swept), want exactly 0",
			h.pendingOps.Load(), rollbacks, sweeperWins)
	}
}
