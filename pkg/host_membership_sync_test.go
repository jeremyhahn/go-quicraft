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
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// startSingleVoterShard starts shard 1 with replica 1 as its only voter and
// waits for it to elect itself leader. CheckQuorum is disabled so a membership
// change that raises the voter count does not depose the leader before the
// test finishes.
func startSingleVoterShard(t *testing.T) *Host {
	t.Helper()
	h := newTestHost(t)
	scfg := testShardConfig(1, 1)
	scfg.CheckQuorum = config.BoolPtr(false)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)
	return h
}

// ---------------------------------------------------------------------------
// SyncRequestRemoveNode
// ---------------------------------------------------------------------------

// TestSyncRequestRemoveNode_RemovesObserver verifies a synchronous removal returns once applied and the observer is gone.
func TestSyncRequestRemoveNode_RemovesObserver(t *testing.T) {
	h := startSingleVoterShard(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.SyncRequestAddObserver(ctx, 1, 10, "127.0.0.1:6000", 0); err != nil {
		t.Fatalf("SyncRequestAddObserver: %v", err)
	}

	added, err := h.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership after add: %v", err)
	}
	if _, ok := added.Observers[10]; !ok {
		t.Fatalf("observer 10 missing from membership after add: %+v", added.Observers)
	}

	if err := h.SyncRequestRemoveNode(ctx, 1, 10, added.ConfigChangeID); err != nil {
		t.Fatalf("SyncRequestRemoveNode: %v", err)
	}

	removed, err := h.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership after delete: %v", err)
	}
	if _, ok := removed.Observers[10]; ok {
		t.Fatalf("observer 10 still present after SyncRequestRemoveNode: %+v", removed.Observers)
	}
	if _, ok := removed.Nodes[10]; ok {
		t.Fatalf("replica 10 reappeared as a voter after removal: %+v", removed.Nodes)
	}
	if removed.ConfigChangeID == added.ConfigChangeID {
		t.Fatalf("ConfigChangeID did not advance across the removal (%d)", removed.ConfigChangeID)
	}
}

// TestSyncRequestRemoveNode_ContextEndsUncommittedWait verifies an uncommittable removal returns ErrCanceled or ErrTimeout with ctx.
func TestSyncRequestRemoveNode_ContextEndsUncommittedWait(t *testing.T) {
	tests := []struct {
		name    string
		ctxFn   func() (context.Context, context.CancelFunc)
		wantErr error
	}{
		{
			name: "canceled",
			ctxFn: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			wantErr: ErrCanceled,
		},
		{
			name: "deadline exceeded",
			ctxFn: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			wantErr: ErrTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := startSingleVoterShard(t)

			setupCtx, setupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer setupCancel()
			if err := h.SyncRequestAddNode(setupCtx, 1, 4, "127.0.0.1:6001", 0); err != nil {
				t.Fatalf("SyncRequestAddNode: %v", err)
			}
			// Read locally: the shard has no quorum.
			members, err := h.ShardMembership(1)
			if err != nil {
				t.Fatalf("ShardMembership: %v", err)
			}
			if _, ok := members.Nodes[4]; !ok {
				t.Fatalf("replica 4 not a voter, removal would be rejected early: %+v", members.Nodes)
			}

			ctx, cancel := tt.ctxFn()
			defer cancel()

			err = h.SyncRequestRemoveNode(ctx, 1, 4, 0)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("SyncRequestRemoveNode error = %v, want %v", err, tt.wantErr)
			}

			// The uncommitted change must not have mutated local membership.
			after, err := h.ShardMembership(1)
			if err != nil {
				t.Fatalf("ShardMembership after abandoned removal: %v", err)
			}
			if _, ok := after.Nodes[4]; !ok {
				t.Fatalf("replica 4 removed from membership by an uncommitted config change: %+v", after.Nodes)
			}
		})
	}
}

// TestSyncRequestRemoveNode_ConfigChangeIndexRoundTripsUnderContention verifies configChangeIndex advances before the caller is completed.
func TestSyncRequestRemoveNode_ConfigChangeIndexRoundTripsUnderContention(t *testing.T) {
	h := startSingleVoterShard(t)

	const iterations = 500
	const noiseWorkers = 8

	stopNoise := make(chan struct{})
	var noiseWG sync.WaitGroup
	noiseWG.Add(2 * noiseWorkers)
	for n := 0; n < noiseWorkers; n++ {
		go func() {
			defer noiseWG.Done()
			for {
				select {
				case <-stopNoise:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, _ = h.SyncPropose(ctx, 1, []byte("noise"))
				cancel()
			}
		}()
		go func() {
			defer noiseWG.Done()
			for {
				select {
				case <-stopNoise:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, _ = h.SyncGetShardMembership(ctx, 1)
				cancel()
			}
		}()
	}
	defer func() {
		close(stopNoise)
		noiseWG.Wait()
	}()

	for i := 0; i < iterations; i++ {
		// A removed replica ID is tombstoned and can never be reused, and an
		// address may not be held by more than one member of the shard at
		// once, so every iteration needs its own of each.
		observerID := uint64(10 + i)
		addr := fmt.Sprintf("127.0.0.1:%d", 20000+i)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		if err := h.SyncRequestAddObserver(ctx, 1, observerID, addr, 0); err != nil {
			cancel()
			t.Fatalf("iteration %d: SyncRequestAddObserver: %v", i, err)
		}

		added, err := h.SyncGetShardMembership(ctx, 1)
		if err != nil {
			cancel()
			t.Fatalf("iteration %d: SyncGetShardMembership after add: %v", i, err)
		}

		if err := h.SyncRequestRemoveNode(ctx, 1, observerID, added.ConfigChangeID); err != nil {
			cancel()
			t.Fatalf("iteration %d: SyncRequestRemoveNode: %v", i, err)
		}
		cancel()
	}
}

// TestOnApplied_ClaimsProposalBeforeRoutingSoTheSweeperCannotStealIt verifies OnApplied claims proposals before routing.
func TestOnApplied_ClaimsProposalBeforeRoutingSoTheSweeperCannotStealIt(t *testing.T) {
	h := startSingleVoterShard(t)
	ns := testLoadNode(h, 1)

	// Block applyMembershipRouting from ever reaching the point where it
	// would register the new observer's address: it takes this exact mutex
	// to do so. Holding it here means that once OnApplied's claim pass has
	// run, routing is provably stalled — nothing further can happen until
	// this goroutine releases it.
	ns.nodesMu.Lock()
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			ns.nodesMu.Unlock()
		}
	}
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// RequestAddObserver returns after the proposal is stored and delivered.
	rs, err := h.RequestAddObserver(ctx, 1, 10, "127.0.0.1:6000", 0)
	if err != nil {
		unlock()
		t.Fatalf("RequestAddObserver: %v", err)
	}
	defer rs.Release()

	pendingCount := func() int {
		n := 0
		ns.pendingProposals.Range(func(_, _ any) bool { n++; return true })
		return n
	}

	// Wait for the claim; routing is blocked on ns.nodesMu, held here.
	claimed := false
	ticker := time.NewTicker(time.Millisecond)
	timeout := time.NewTimer(5 * time.Second)
pollClaimed:
	for {
		if pendingCount() == 0 {
			claimed = true
			break
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			break pollClaimed
		}
	}
	ticker.Stop()
	timeout.Stop()
	if !claimed {
		unlock()
		t.Fatal("proposal was not claimed out of ns.pendingProposals while routing was blocked on ns.nodesMu — " +
			"claim must happen before applyMembershipRouting, not after")
	}

	// Routing is still stalled; a sweep far past the deadline must find nothing.
	h.sweepPendingProposals(ns, time.Now().Add(24*time.Hour))

	unlock()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			t.Fatalf("RequestAddObserver: %v (the deadline sweep incorrectly reaped an already-claimed, "+
				"already-committed config change)", result.Err)
		}
	case <-ctx.Done():
		t.Fatal("RequestAddObserver never completed after routing unblocked")
	}

	members, err := h.ShardMembership(1)
	if err != nil {
		t.Fatalf("ShardMembership: %v", err)
	}
	if _, ok := members.Observers[10]; !ok {
		t.Fatalf("observer 10 missing after a successful add — rollbackPreRegister ran on an applied, "+
			"committed change: %+v", members.Observers)
	}
}

// TestOnApplied_ClaimsProposalBeforeReadsSoTheSweeperCannotStealIt verifies OnApplied claims proposals before the reads scan.
func TestOnApplied_ClaimsProposalBeforeReadsSoTheSweeperCannotStealIt(t *testing.T) {
	h := startSingleVoterShard(t)
	ns := testLoadNode(h, 1)

	// Block the pending-reads scan from ever running: it takes this exact
	// mutex first thing. Holding it here means that once OnApplied's claim
	// pass has run, the reads scan (and everything after it, including
	// routing) is provably stalled — nothing further can happen until this
	// goroutine releases it.
	ns.pendingReadsMu.Lock()
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			ns.pendingReadsMu.Unlock()
		}
	}
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// See the sibling test above: RequestAddObserver returning proves the
	// proposal is registered in ns.pendingProposals, in program order on
	// this goroutine, with no polling needed.
	rs, err := h.RequestAddObserver(ctx, 1, 10, "127.0.0.1:6000", 0)
	if err != nil {
		unlock()
		t.Fatalf("RequestAddObserver: %v", err)
	}
	defer rs.Release()

	pendingCount := func() int {
		n := 0
		ns.pendingProposals.Range(func(_, _ any) bool { n++; return true })
		return n
	}

	// Wait for the claim; the reads scan is blocked on ns.pendingReadsMu, held here.
	claimed := false
	ticker := time.NewTicker(time.Millisecond)
	timeout := time.NewTimer(5 * time.Second)
pollClaimed:
	for {
		if pendingCount() == 0 {
			claimed = true
			break
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			break pollClaimed
		}
	}
	ticker.Stop()
	timeout.Stop()
	if !claimed {
		unlock()
		t.Fatal("proposal was not claimed out of ns.pendingProposals while the pending-reads scan was blocked " +
			"on ns.pendingReadsMu — claim must happen before that scan, not after")
	}

	// The reads scan is still stalled; a sweep far past the deadline must find nothing.
	h.sweepPendingProposals(ns, time.Now().Add(24*time.Hour))

	unlock()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			t.Fatalf("RequestAddObserver: %v (the deadline sweep incorrectly reaped an already-claimed, "+
				"already-committed config change)", result.Err)
		}
	case <-ctx.Done():
		t.Fatal("RequestAddObserver never completed after the reads scan unblocked")
	}

	members, err := h.ShardMembership(1)
	if err != nil {
		t.Fatalf("ShardMembership: %v", err)
	}
	if _, ok := members.Observers[10]; !ok {
		t.Fatalf("observer 10 missing after a successful add — rollbackPreRegister ran on an applied, "+
			"committed change: %+v", members.Observers)
	}
}

// panicOnMembershipChangedHandler is an slog.Handler that panics on the
// specific "membership changed" record applyMembershipRouting logs, and is a
// well-behaved no-op handler for every other record — so shard startup and
// leader-election logging (different messages) do not also panic and
// obscure the one call this test targets.
type panicOnMembershipChangedHandler struct{}

func (panicOnMembershipChangedHandler) Enabled(context.Context, slog.Level) bool { return true }

func (panicOnMembershipChangedHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "membership changed" {
		panic("injected panic: user-supplied slog.Handler blew up")
	}
	return nil
}

func (h panicOnMembershipChangedHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h panicOnMembershipChangedHandler) WithGroup(string) slog.Handler      { return h }

// TestOnApplied_RoutingPanicFailsClaimedProposalsInsteadOfStrandingThem verifies a routing panic completes the claimed proposals.
func TestOnApplied_RoutingPanicFailsClaimedProposalsInsteadOfStrandingThem(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithLogger(slog.New(panicOnMembershipChangedHandler{})), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	scfg := testShardConfig(1, 1)
	scfg.CheckQuorum = config.BoolPtr(false)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addErr := h.SyncRequestAddObserver(ctx, 1, 10, "127.0.0.1:6000", 0)
	if addErr == nil {
		t.Fatal("SyncRequestAddObserver: expected the injected applyMembershipRouting panic to surface as an " +
			"error, got nil — either the panic was not triggered or the claimed proposal completed as if nothing happened")
	}
	var panicErr *LoopPanicError
	if !errors.As(addErr, &panicErr) {
		t.Fatalf("SyncRequestAddObserver error = %T (%v), want a *LoopPanicError wrapping the recovered routing panic",
			addErr, addErr)
	}
	if panicErr.LoopName != "OnApplied(membership)" {
		t.Fatalf("LoopPanicError.LoopName = %q, want %q", panicErr.LoopName, "OnApplied(membership)")
	}

	// The claimed proposal's rs.releaseRef/completePendingOp must have run
	// for the caller to see addErr at all (RequestState.complete alone does
	// not unblock ResultC's buffer past a leak in the pool accounting), but
	// Drain is the direct assertion on the pendingOps invariant: if the claimed proposal's completePendingOp were
	// skipped, pendingOps would never reach zero and Drain would time out.
	if drainErr := h.Drain(); drainErr != nil {
		t.Fatalf("Drain did not complete — a claimed proposal's pendingOp was stranded by the routing panic: %v", drainErr)
	}

	// The AddNonVoting's registry.Register/ns.nodes update ran before the
	// panicking log call in the same loop iteration, so it already landed
	// when the panic fired. It must not have been rolled back.
	ns := testLoadNode(h, 1)
	ns.nodesMu.RLock()
	addr, ok := ns.nodes[10]
	ns.nodesMu.RUnlock()
	if !ok || addr != "127.0.0.1:6000" {
		t.Fatalf("ns.nodes[10] = (%q, %v), want (\"127.0.0.1:6000\", true) — the routing panic's recovery "+
			"rolled back a registration that had already committed", addr, ok)
	}
	if resolved, resolveErr := h.registry.Resolve(1, 10); resolveErr != nil || resolved != "127.0.0.1:6000" {
		t.Fatalf("registry.Resolve(1, 10) = (%q, %v), want (\"127.0.0.1:6000\", nil) — rollbackPreRegister "+
			"reverted an already-committed config change's registration", resolved, resolveErr)
	}
}

// TestOnApplied_RoutingPanicDoesNotStrandPendingReads verifies a routing panic leaves no confirmed read stranded.
func TestOnApplied_RoutingPanicDoesNotStrandPendingReads(t *testing.T) {
	h := newTestHost(t)
	h.logger = slog.New(panicOnMembershipChangedHandler{})

	ns := &nodeState{
		shardID:      1,
		replicaID:    1,
		nodes:        map[uint64]string{1: "127.0.0.1:5000"},
		pendingReads: make(map[uint64]*pendingRead),
		requestPool:  NewRequestStatePool(),
		openStopper:  make(chan struct{}),
	}
	ns.health.Store(uint32(ShardHealthy))
	h.nodes.Store(uint64(1), ns)
	h.activeNodes.Add(1)

	// Register a pending read exactly as ReadIndex does, pre-confirmed
	// against an index this batch's entry will cover — the state OnApplied's
	// pending-reads scan exists to complete.
	const readKey = 999999
	rs := newRequestState(ns.requestPool, readKey, time.Now().Add(time.Hour), false)
	h.addPendingOp()
	rs.acquireRef()
	ns.pendingReadsMu.Lock()
	ns.pendingReads[readKey] = &pendingRead{rs: rs, confirmed: true, readIndex: 1}
	ns.pendingReadsMu.Unlock()

	cc := proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 10, Address: "127.0.0.1:6000"}
	ccBuf := make([]byte, cc.Size())
	n, err := cc.MarshalTo(ccBuf)
	if err != nil {
		t.Fatalf("ConfigChange.MarshalTo: %v", err)
	}
	entries := []proto.Entry{{Index: 5, Term: 1, Type: proto.EntryConfigChange, Cmd: ccBuf[:n]}}

	cb := &hostEngineCallback{h: h}
	func() {
		// The routing panic is expected to propagate out of OnApplied by
		// design (apply.go's own recover normally handles it in production;
		// there is no apply worker here since OnApplied is invoked
		// directly). This test only inspects the aftermath.
		defer func() { recover() }()
		cb.OnApplied(1, entries, []sm.Result{{}})
	}()

	select {
	case <-rs.ResultC():
	default:
		rs.Release()
		t.Fatal("the pending read was not completed before the routing panic propagated out of OnApplied — it was stranded")
	}
	rs.Release()

	if drainErr := h.Drain(); drainErr != nil {
		t.Fatalf("Drain did not complete — the pending read's pendingOp was stranded by the routing panic: %v", drainErr)
	}
}

// panicOnSecondObserveProposalLatency panics on its second call and is a
// no-op (via NoOpMetricsCollector) otherwise. Not concurrency-safe — only
// meant for a test that drives calls from a single goroutine, as OnApplied's
// completion loop does.
type panicOnSecondObserveProposalLatency struct {
	NoOpMetricsCollector
	calls *int
}

func (p panicOnSecondObserveProposalLatency) ObserveProposalLatency(uint64, time.Duration) {
	*p.calls++
	if *p.calls == 2 {
		panic("injected panic: second proposal completion blew up")
	}
}

// TestOnApplied_RecoveryDoesNotDoubleReleaseAlreadyCompletedClaims verifies the recovery completes only the not-yet-completed claims.
func TestOnApplied_RecoveryDoesNotDoubleReleaseAlreadyCompletedClaims(t *testing.T) {
	h := newTestHost(t)
	calls := 0
	h.metrics = panicOnSecondObserveProposalLatency{calls: &calls} // fault injection — see doc comment

	ns := &nodeState{
		shardID:      1,
		replicaID:    1,
		nodes:        map[uint64]string{1: "127.0.0.1:5000"},
		pendingReads: make(map[uint64]*pendingRead),
		requestPool:  NewRequestStatePool(),
		openStopper:  make(chan struct{}),
	}
	ns.health.Store(uint32(ShardHealthy))
	h.nodes.Store(uint64(1), ns)
	h.activeNodes.Add(1)

	const keyA, keyB uint64 = 1, 2
	rsA := newRequestState(ns.requestPool, keyA, time.Now().Add(time.Hour), true)
	h.addPendingOp()
	rsA.acquireRef()
	ns.pendingProposals.Store(keyA, &pendingProposal{rs: rsA})

	rsB := newRequestState(ns.requestPool, keyB, time.Now().Add(time.Hour), true)
	h.addPendingOp()
	rsB.acquireRef()
	ns.pendingProposals.Store(keyB, &pendingProposal{rs: rsB})

	cc := proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 10, Address: "127.0.0.1:6000"}
	ccBuf := make([]byte, cc.Size())
	n, err := cc.MarshalTo(ccBuf)
	if err != nil {
		t.Fatalf("ConfigChange.MarshalTo: %v", err)
	}
	entries := []proto.Entry{
		{Index: 5, Term: 1, Key: keyA, Type: proto.EntryNormal},
		{Index: 6, Term: 1, Key: keyB, Type: proto.EntryConfigChange, Cmd: ccBuf[:n]},
	}

	cb := &hostEngineCallback{h: h}
	func() {
		// Expected to propagate by design (see the sibling tests). This test
		// only inspects the aftermath.
		defer func() { recover() }()
		cb.OnApplied(1, entries, []sm.Result{{}, {}})
	}()

	// A completed normally: its RequestState was released exactly once.
	select {
	case result := <-rsA.ResultC():
		if result.Err != nil {
			t.Fatalf("proposal A result.Err = %v, want nil (it completed before B's panic)", result.Err)
		}
	default:
		t.Fatal("proposal A was never completed")
	}

	// B failed with the panic, via the recovery path.
	// B was applied and completed with its real result before its latency
	// was observed — the collector is user code and runs only after the
	// caller is answered — and the recovery did not touch it again.
	select {
	case result := <-rsB.ResultC():
		if result.Err != nil {
			t.Fatalf("proposal B result.Err = %T (%v), want nil: it was completed before "+
				"the collector panicked", result.Err, result.Err)
		}
	default:
		t.Fatal("proposal B was never completed")
	}
	if outstanding := rsB.outstanding.Load(); outstanding != 0 {
		t.Fatalf("rsB.outstanding = %d, want exactly 0 — the recovery handler released proposal B's "+
			"RequestState a second time after its normal completion had already done so", outstanding)
	}

	// pendingOps has a floor guard, so check the RequestState pool instead.
	if outstanding := rsA.outstanding.Load(); outstanding != 0 {
		t.Fatalf("rsA.outstanding = %d, want exactly 0 — the recovery handler released proposal A's "+
			"RequestState a second time after its normal completion had already done so", outstanding)
	}

	if drainErr := h.Drain(); drainErr != nil {
		t.Fatalf("Drain did not complete: %v", drainErr)
	}
}

// ---------------------------------------------------------------------------
// awaitResult
// ---------------------------------------------------------------------------

// TestWaitResultCtx verifies the bridge used by the session APIs: a context
// that ends first unblocks the caller with the mapped error rather than
// waiting on a request that will never resolve. A resolved request returning
// its result and embedded error is covered through SyncRequestRemoveNode and
// SyncRequestAddObserver above.
func TestWaitResultCtx(t *testing.T) {
	pool := NewRequestStatePool()

	t.Run("context canceled", func(t *testing.T) {
		rs := newRequestState(pool, 3, time.Time{}, false)
		defer rs.Release()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result, err := awaitResult(ctx, rs)
		if !errors.Is(err, ErrCanceled) {
			t.Fatalf("awaitResult error = %v, want ErrCanceled", err)
		}
		if result.Value != 0 || result.Data != nil || result.Err != nil {
			t.Fatalf("result = %+v, want the zero value on cancellation", result)
		}
	})

	t.Run("context deadline exceeded", func(t *testing.T) {
		rs := newRequestState(pool, 4, time.Time{}, false)
		defer rs.Release()

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		result, err := awaitResult(ctx, rs)
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("awaitResult error = %v, want ErrTimeout", err)
		}
		if result.Value != 0 || result.Data != nil || result.Err != nil {
			t.Fatalf("result = %+v, want the zero value on timeout", result)
		}
	})
}
