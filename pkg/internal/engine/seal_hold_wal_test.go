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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// The SealHold.IsSealed contract against the real stack: real WALs under real
// barriers, the classifier the host installs (isSealedLogWrite is
// waldb.IsCleanRefusal bound to ErrBarrierSealed), and a two-shard batch on
// one commit worker. holdSealedBatch re-writes each member alone, so the
// single-update refusals must classify exactly as the batch refusal did: the
// sealed shard is held, never latched, and no retry backoff runs for it.

// realBarrier is an initialized, unsealed seal.Barrier and its strategy.
type realBarrier struct {
	*seal.Barrier
	strategy seal.SealingStrategy
}

func newRealBarrier(t *testing.T) realBarrier {
	t.Helper()
	_ = seal.AllowWeakKDFParams()
	strategy, err := seal.NewSoftwareStrategyFIPS(context.Background(), []byte("seal-hold"), 1000)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyFIPS: %v", err)
	}
	t.Cleanup(func() { strategy.Close() })
	cfg := seal.DefaultBarrierConfig(1)
	cfg.StatePersister = func(*seal.SealedRootKey) error { return nil }
	b := seal.MustNewBarrier(cfg)
	if err := b.Initialize(context.Background(), strategy, seal.Credentials{}); err != nil {
		t.Fatalf("barrier.Initialize: %v", err)
	}
	return realBarrier{Barrier: b, strategy: strategy}
}

func (b realBarrier) unseal(t *testing.T) {
	t.Helper()
	if err := b.Unseal(context.Background(), b.strategy, seal.Credentials{}); err != nil {
		t.Fatalf("barrier.Unseal: %v", err)
	}
}

func openBarrierWAL(t *testing.T, b realBarrier) *waldb.DB {
	t.Helper()
	db, err := waldb.Open(t.TempDir(), waldb.WithNoSync(true), waldb.WithBarrier(b.Barrier))
	if err != nil {
		t.Fatalf("waldb.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// shardMuxLogDB routes each shard's writes to its own WAL, as a per-tenant
// LogDB does. It wraps a multi-WAL failure differently from a single-WAL one —
// joined and annotated rather than bare — which is the shape difference the
// IsSealed contract has to survive. Everything but SaveState goes to the
// embedded default WAL; the commit worker needs only SaveState.
type shardMuxLogDB struct {
	*waldb.DB
	routes map[uint64]*waldb.DB
}

func (m *shardMuxLogDB) SaveState(updates []logdb.Update) error {
	groups := make(map[*waldb.DB][]logdb.Update)
	var order []*waldb.DB
	for _, u := range updates {
		db := m.routes[u.ShardID]
		if _, ok := groups[db]; !ok {
			order = append(order, db)
		}
		groups[db] = append(groups[db], u)
	}
	if len(order) == 1 {
		return order[0].SaveState(updates)
	}
	var errs []error
	for _, db := range order {
		if err := db.SaveState(groups[db]); err != nil {
			errs = append(errs, fmt.Errorf("wal %p: %w", db, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("mux: %w", errors.Join(errs...))
	}
	return nil
}

type realWALWorker struct {
	cw      *commitWorker
	cb      *testCallback
	retries atomic.Int64
}

func newRealWALWorker(t *testing.T, ldb logdb.LogDB, barriers map[uint64]realBarrier) *realWALWorker {
	t.Helper()
	stopC := make(chan struct{})
	w := &realWALWorker{cb: newTestCallback()}
	w.cw = newCommitWorker(0, ldb, newTestSender(), make(chan *commitItem, 16), make(chan *applyItem, 16),
		&sync.Pool{New: func() interface{} { return &applyItem{} }}, NewWorkSignal(1), w.cb, stopC, nil)
	w.cw.noRetryDelay = true
	w.cw.retryHookForTest = func(int) { w.retries.Add(1) }
	w.cw.sealHold = SealHold{
		IsSealed:  func(err error) bool { return waldb.IsCleanRefusal(err, seal.ErrBarrierSealed) },
		UnsealedC: func(shardID uint64) <-chan struct{} { return barriers[shardID].UnsealedC() },
	}
	t.Cleanup(func() {
		close(stopC)
		w.cw.sealWaiters.Wait()
	})
	return w
}

func (w *realWALWorker) process(nodes []*Node, updates []proto.Update) {
	w.cw.batch = append(w.cw.batch[:0], updates...)
	w.cw.batchNodes = append(w.cw.batchNodes[:0], nodes...)
	w.cw.batchUnpersisted = true
	w.cw.processBatch()
}

// releaseAll waits until every held shard's waiter has fired and released it.
func (w *realWALWorker) releaseAll(t *testing.T) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for len(w.cw.held) > 0 {
		select {
		case <-w.cw.sealWakeC:
			w.cw.releaseWoken()
		case <-deadline:
			t.Fatalf("%d held shard(s) never released after the unseal", len(w.cw.held))
		}
	}
}

// rederive returns the update a released node re-derives, ready to submit.
func rederive(t *testing.T, node *Node) proto.Update {
	t.Helper()
	u, err := node.peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate after release: %v", err)
	}
	if u.ReadOnly {
		t.Fatal("re-derived update is ReadOnly: the refused write would never reach the WAL")
	}
	node.commitPending.Store(true)
	return u
}

func persistedTerm(t *testing.T, db *waldb.DB, shardID uint64) uint64 {
	t.Helper()
	st, _, err := db.ReadState(shardID, 1)
	if err != nil {
		return 0
	}
	return st.Term
}

func (w *realWALWorker) requireNoLatchNoBackoff(t *testing.T) {
	t.Helper()
	if n := w.cb.shardFailedCount.Load(); n != 0 {
		t.Fatalf("a sealed refusal latched %d shard(s) failed", n)
	}
	if n := w.retries.Load(); n != 0 {
		t.Fatalf("%d retry backoff(s) ran for a sealed refusal", n)
	}
}

func TestSealHold_RealWAL_BatchHeldNotLatched(t *testing.T) {
	t.Run("one WAL, sealed barrier", func(t *testing.T) {
		b := newRealBarrier(t)
		db := openBarrierWAL(t, b)
		w := newRealWALWorker(t, db, map[uint64]realBarrier{1: b, 2: b})
		nodeA, uA := newLeaderNode(t, 1)
		nodeB, uB := newLeaderNode(t, 2)
		if err := b.Seal(); err != nil {
			t.Fatalf("Seal: %v", err)
		}

		w.process([]*Node{nodeA, nodeB}, []proto.Update{uA, uB})
		w.requireNoLatchNoBackoff(t)
		for _, n := range []*Node{nodeA, nodeB} {
			if n.IsFailed() || !n.sealHeld.Load() || !n.commitPending.Load() {
				t.Fatalf("shard %d failed=%v sealHeld=%v, want held", n.shardID, n.IsFailed(), n.sealHeld.Load())
			}
		}
		if persistedTerm(t, db, 1) != 0 || persistedTerm(t, db, 2) != 0 {
			t.Fatal("a refused write reached the sealed WAL")
		}

		b.unseal(t)
		w.releaseAll(t)
		w.process([]*Node{nodeA, nodeB}, []proto.Update{rederive(t, nodeA), rederive(t, nodeB)})
		w.requireNoLatchNoBackoff(t)
		if persistedTerm(t, db, 1) == 0 || persistedTerm(t, db, 2) == 0 {
			t.Fatal("the retried writes did not persist after the unseal")
		}
	})

	t.Run("per-shard WALs, one sealed", func(t *testing.T) {
		bA, bB := newRealBarrier(t), newRealBarrier(t)
		walA, walB := openBarrierWAL(t, bA), openBarrierWAL(t, bB)
		mux := &shardMuxLogDB{DB: walB, routes: map[uint64]*waldb.DB{1: walA, 2: walB}}
		w := newRealWALWorker(t, mux, map[uint64]realBarrier{1: bA, 2: bB})
		nodeA, uA := newLeaderNode(t, 1)
		nodeB, uB := newLeaderNode(t, 2)
		if err := bA.Seal(); err != nil {
			t.Fatalf("Seal: %v", err)
		}

		w.process([]*Node{nodeA, nodeB}, []proto.Update{uA, uB})
		w.requireNoLatchNoBackoff(t)
		if nodeA.IsFailed() || !nodeA.sealHeld.Load() {
			t.Fatalf("sealed shard A failed=%v sealHeld=%v, want held", nodeA.IsFailed(), nodeA.sealHeld.Load())
		}
		if nodeB.IsFailed() || nodeB.sealHeld.Load() {
			t.Fatal("unsealed shard B was held or failed with A")
		}
		if len(w.cw.held) != 1 || w.cw.held[0].node != nodeA {
			t.Fatalf("held=%d after the batch, want shard A alone", len(w.cw.held))
		}
		if persistedTerm(t, walB, 2) == 0 {
			t.Fatal("shard B's update was not persisted while A's barrier was sealed")
		}
		if persistedTerm(t, walA, 1) != 0 {
			t.Fatal("shard A's refused write reached its sealed WAL")
		}

		bA.unseal(t)
		w.releaseAll(t)
		w.process([]*Node{nodeA}, []proto.Update{rederive(t, nodeA)})
		w.requireNoLatchNoBackoff(t)
		if persistedTerm(t, walA, 1) == 0 || nodeA.IsFailed() {
			t.Fatal("shard A's retried write did not persist after the unseal")
		}
	})
}
