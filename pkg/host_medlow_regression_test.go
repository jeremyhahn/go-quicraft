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
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// M14: sweepPendingReads must expire a batch key's unreachable aliases
// ---------------------------------------------------------------------------

// TestSweepPendingReads_ExpiresUnconfirmedAliasGroup is the M14 regression
// test. Coalesced ReadIndex alias keys are reachable ONLY through the batch
// key's pendingRead: OnReadyToRead looks up the batch key and walks
// pr.aliases. When the sweeper expires the batch key while the group is
// unconfirmed, deadline-less aliases would otherwise hang forever and their
// RequestStates would never return to the pool.
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

// TestSweepPendingReads_KeepsConfirmedAliases verifies the M14 fix leaves
// CONFIRMED aliases in place when their batch key expires: a confirmed
// alias carries its own readIndex and is independently completable by the
// OnApplied read scan, so timing it out would discard a read that is about
// to resolve legitimately.
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

// TestOnReadIndexBatched_BatchKeyAlreadyRemovedFailsAliases covers the M14
// edge where the batch key was swept BEFORE the aliases could attach to it:
// the raft confirmation arrives keyed by the batch key, so aliases
// registered against a removed batch entry are unreachable from birth and
// must be failed rather than left to hang.
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
// M15: registration racing Host.Close must never orphan the caller
// ---------------------------------------------------------------------------

// TestPendingRegistration_CloseRace_CallerNeverOrphaned is the M15
// regression test: ReadIndex, RequestSnapshot, and config-change
// registrations racing Host.Close() must always resolve — either with
// ErrClosed from the post-store re-check, or with a completion from the
// close sweep — never leaving a deadline-less caller blocked forever.
func TestPendingRegistration_CloseRace_CallerNeverOrphaned(t *testing.T) {
	for iter := 0; iter < 8; iter++ {
		cfg := testHostConfig(t)
		h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
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
			op := op
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
		if err := h.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
		wg.Wait()

		if got := h.pendingOps.Load(); got != 0 {
			t.Fatalf("iter %d: pendingOps = %d, want 0 after Close", iter, got)
		}
	}
}

// ---------------------------------------------------------------------------
// L13: StartShard bootstrap failure must mark the nodeState stopped
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

// TestStartShard_BootstrapFailureMarksNodeStateStopped is the L13
// regression test: the SaveBootstrap failure path deletes the node from
// h.nodes and the deferred rollback closes the state machine. A reader
// that resolved the nodeState just before the Delete must fail its
// stopped check instead of calling into the closing state machine, so
// ns.stopped must be set before the Delete (matching StopShard/Close).
func TestStartShard_BootstrapFailureMarksNodeStateStopped(t *testing.T) {
	cfg := testHostConfig(t)
	db := &stashBootstrapLogDB{mockLogDB: *newMockLogDB()}
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport())
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
// L14: queue-full rollbacks must claim with LoadAndDelete
// ---------------------------------------------------------------------------

// TestProposeEntry_QueueFullRollback_RacesSweeper is the L14 regression
// test. The queue-full rollback used an unconditional Delete, so when the
// sweeper had already claimed and completed the entry the rollback still
// ran releaseRef (driving outstanding negative — the RequestState is
// permanently lost from the pool) and completePendingOp a second time
// (undercounting pendingOps so Drain can close early). With the
// LoadAndDelete claim, pendingOps always balances back to exactly zero.
func TestProposeEntry_QueueFullRollback_RacesSweeper(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
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

	sawQueueFull := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		// Fill the proposal queue so DeliverProposal fails.
		for i := 0; i < 8192; i++ {
			if !engNode.DeliverProposal([]proto.Entry{{Type: proto.EntryNormal}}) {
				break
			}
		}
		_, propErr := h.proposeEntry(expiredCtx, ns, 1, proto.Entry{Type: proto.EntryNormal, Cmd: []byte("x")})
		if errors.Is(propErr, ErrSystemBusy) {
			sawQueueFull = true
		}
	}
	close(stop)
	sweeperWG.Wait()
	if !sawQueueFull {
		t.Skip("proposal queue never filled; rollback path not exercised on this run")
	}

	// Let in-flight applies/sweeps quiesce, then require an exact balance:
	// a double completePendingOp drives the counter negative.
	waitDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(waitDeadline) {
		if h.pendingOps.Load() == 0 {
			break
		}
		h.sweepExpiredPending()
		time.Sleep(10 * time.Millisecond)
	}
	if got := h.pendingOps.Load(); got != 0 {
		t.Fatalf("pendingOps = %d, want exactly 0 (negative means a double rollback)", got)
	}
}
