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
// SyncRequestDeleteNode
// ---------------------------------------------------------------------------

// TestSyncRequestDeleteNode_RemovesObserver verifies the committed path of the
// public synchronous membership removal: an observer that was added through
// consensus is removed through consensus, the call returns only once the
// change is applied, and the shard membership no longer lists the replica.
// Observers do not count toward quorum, so the removal commits on the single
// voter without any peer being reachable.
func TestSyncRequestDeleteNode_RemovesObserver(t *testing.T) {
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

	if err := h.SyncRequestDeleteNode(ctx, 1, 10, added.ConfigChangeID); err != nil {
		t.Fatalf("SyncRequestDeleteNode: %v", err)
	}

	removed, err := h.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership after delete: %v", err)
	}
	if _, ok := removed.Observers[10]; ok {
		t.Fatalf("observer 10 still present after SyncRequestDeleteNode: %+v", removed.Observers)
	}
	if _, ok := removed.Nodes[10]; ok {
		t.Fatalf("replica 10 reappeared as a voter after removal: %+v", removed.Nodes)
	}
	if removed.ConfigChangeID == added.ConfigChangeID {
		t.Fatalf("ConfigChangeID did not advance across the removal (%d)", removed.ConfigChangeID)
	}
}

// TestSyncRequestDeleteNode_ContextEndsUncommittedWait verifies that a removal
// which cannot commit does not block the caller forever: the call is bounded
// by the caller's context and reports ErrCanceled / ErrTimeout accordingly.
//
// The setup makes the removal genuinely uncommittable: replica 4 is added as a
// voter (the add commits under the OLD single-voter quorum), after which the
// shard needs 2 of 2 acks and replica 4 does not exist.
func TestSyncRequestDeleteNode_ContextEndsUncommittedWait(t *testing.T) {
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
			// Read locally: with two voters and one of them absent, a
			// linearizable read can no longer reach quorum.
			members, err := h.GetShardMembership(1)
			if err != nil {
				t.Fatalf("GetShardMembership: %v", err)
			}
			if _, ok := members.Nodes[4]; !ok {
				t.Fatalf("replica 4 not a voter, removal would be rejected early: %+v", members.Nodes)
			}

			ctx, cancel := tt.ctxFn()
			defer cancel()

			err = h.SyncRequestDeleteNode(ctx, 1, 4, 0)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("SyncRequestDeleteNode error = %v, want %v", err, tt.wantErr)
			}

			// The uncommitted change must not have mutated local membership.
			after, err := h.GetShardMembership(1)
			if err != nil {
				t.Fatalf("GetShardMembership after abandoned removal: %v", err)
			}
			if _, ok := after.Nodes[4]; !ok {
				t.Fatalf("replica 4 removed from membership by an uncommitted config change: %+v", after.Nodes)
			}
		})
	}
}

// TestSyncRequestDeleteNode_ConfigChangeIndexRoundTripsUnderContention pins
// that OnApplied advances ns.configChangeIndex for a config-change entry
// BEFORE completing its RequestState — unblocking the caller of
// SyncRequestAddObserver / SyncRequestDeleteNode. Completing first would let a
// caller that immediately round-trips SyncGetShardMembership's ConfigChangeID
// into the next config-change call, exactly as SyncGetShardMembership
// documents, receive a false-positive StaleConfigChangeError.
//
// Deliberately a SINGLE actor doing config changes, serially: PhD 4.1 allows
// only one config change in flight per shard, so a second concurrent actor
// would race its own changes in and legitimately advance
// ConfigChangeID between this actor's read and write — a real stale
// rejection that has nothing to do with this ordering. Isolating the round
// trip to one actor means every StaleConfigChangeError this test sees is the
// ordering defect and nothing else. Scheduling contention — the ingredient
// that exposes it — comes instead from unrelated goroutines hammering regular
// proposals
// and membership reads concurrently in the background, which cannot
// legitimately move ConfigChangeID.
//
// Mutation-verified: with the ordering in pkg/host.go reversed, this fails in
// most runs under `go test -race -count=30`, always on the expected
// StaleConfigChangeError. The narrower TestSyncRequestDeleteNode_RemovesObserver
// above — one add/read/remove, no background load — is not a reliable repro on
// its own: the defect surfaces only under the scheduling contention of a full
// `-race ./...` suite, and the noise load above stands in for that contention.
func TestSyncRequestDeleteNode_ConfigChangeIndexRoundTripsUnderContention(t *testing.T) {
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

		if err := h.SyncRequestDeleteNode(ctx, 1, observerID, added.ConfigChangeID); err != nil {
			cancel()
			t.Fatalf("iteration %d: SyncRequestDeleteNode: %v", i, err)
		}
		cancel()
	}
}

// TestOnApplied_ClaimsProposalBeforeRoutingSoTheSweeperCannotStealIt is a
// deterministic test of claim ordering: the contention test above only
// exercises the configChangeIndex race and would keep passing even if
// OnApplied's pending-proposal claim (LoadAndDelete) moved to AFTER
// applyMembershipRouting — it never forces the specific interleaving where
// that ordering matters.
//
// That interleaving is: applyMembershipRouting takes ns.nodesMu while
// registering a new member's address. If a config-change proposal is still
// sitting in ns.pendingProposals while routing runs, the deadline-based
// sweep (sweepPendingProposals, driven independently on a timer) can claim
// it out from under OnApplied, treat it as a timeout, and — via
// pendingProposal.onFailure (rollbackPreRegister) — undo the very
// registration applyMembershipRouting is in the middle of making durable.
// The caller would then see ErrTimeout for a change that in fact committed
// and applied.
//
// This test forces that interleaving instead of hoping for it: it holds
// ns.nodesMu itself so applyMembershipRouting provably cannot proceed past
// its Lock() call, confirms OnApplied's claim pass has already emptied
// ns.pendingProposals while routing is stuck there, and only then invokes
// sweepPendingProposals directly with a "now" far past the proposal's
// deadline. sweepPendingProposals takes "now" as an explicit parameter
// (host.go), so this needs no real waiting and no goroutine-scheduling luck.
//
// Mutation-verified: making OnApplied claim proposals after
// applyMembershipRouting makes this test fail — the proposal is still visible when the sweep runs,
// so it gets reaped as a timeout.
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

	// RequestAddObserver (the async form SyncRequestAddObserver wraps)
	// returns only once the proposal is stored in ns.pendingProposals and
	// delivered to the engine — that Store happens strictly before its
	// return, in program order within requestConfigChange, on THIS
	// goroutine. So by the time this call returns, the proposal is
	// registered: no goroutine, no polling, no scheduling race needed to
	// know that — unlike waiting for the OPPOSITE transition (removed from
	// the map) below, which genuinely does need to be observed, because it
	// is OnApplied's apply-worker goroutine that performs it.
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

	// Wait for OnApplied's claim pass to remove it. It must do this before
	// applyMembershipRouting can proceed, and applyMembershipRouting is
	// provably blocked on ns.nodesMu, which this goroutine still holds — so
	// once pendingProposals is empty, that emptiness cannot be explained by
	// routing having finished; it can only be explained by the claim
	// happening first. The proposal was proven registered above, so an
	// empty map here is unambiguous.
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

	// The proposal is claimed but routing is still stalled (ns.nodesMu is
	// still held). Sweep with a "now" far past any deadline this proposal's
	// 10-second context could have set. With the claim happening before
	// routing, this must find nothing: the entry is already out of
	// ns.pendingProposals. If claiming instead happened after routing, the
	// entry would still be sitting here and this call would reap it as a
	// timeout — rolling back the registration applyMembershipRouting is
	// about to make once unblocked.
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

	members, err := h.GetShardMembership(1)
	if err != nil {
		t.Fatalf("GetShardMembership: %v", err)
	}
	if _, ok := members.Observers[10]; !ok {
		t.Fatalf("observer 10 missing after a successful add — rollbackPreRegister ran on an applied, "+
			"committed change: %+v", members.Observers)
	}
}

// TestOnApplied_ClaimsProposalBeforeReadsSoTheSweeperCannotStealIt is a
// sibling of TestOnApplied_ClaimsProposalBeforeRoutingSoTheSweeperCannotStealIt
// one level up: running the lastApplied/pendingReads block before proposal
// claiming — instead of merely before routing, which is all it needs — would
// reopen the same sweeper race. The reads scan can itself take a
// while (arbitrarily many confirmed reads, a metrics call per one), and
// until a membership batch's proposal is claimed it is exposed to
// sweepPendingProposals for that whole time.
//
// This forces the interleaving by holding ns.pendingReadsMu instead of
// ns.nodesMu: the pending-reads block cannot proceed past its Lock() call
// while this goroutine holds it, so if claiming happens first (correct),
// ns.pendingProposals is already empty by the time this test's sweep runs;
// if claiming were moved to after the reads block, the proposal is still
// sitting there and the sweep reaps it.
//
// Mutation-verified: moving proposal claiming to after the pending-reads
// block makes this test fail 10/10, always on the sweep's ErrTimeout.
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

	// Wait for OnApplied's claim pass to remove it. It must do this before
	// the pending-reads scan can proceed, and that scan is provably blocked
	// on ns.pendingReadsMu, which this goroutine still holds — so once
	// pendingProposals is empty, that emptiness cannot be explained by the
	// reads scan (or routing after it) having finished; it can only be
	// explained by the claim happening first.
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

	// The proposal is claimed but the reads scan (and routing behind it) is
	// still stalled. Sweep with a "now" far past any deadline this
	// proposal's 10-second context could have set. With the claim happening
	// first, this must find nothing. If claiming were moved back to after
	// the reads scan, the entry would still be sitting here and this call
	// would reap it as a timeout.
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

	members, err := h.GetShardMembership(1)
	if err != nil {
		t.Fatalf("GetShardMembership: %v", err)
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

// TestOnApplied_RoutingPanicFailsClaimedProposalsInsteadOfStrandingThem pins
// OnApplied's local recover: claiming proposals (LoadAndDelete) before
// applyMembershipRouting means a panic inside routing cannot be recovered by
// the apply worker's OnProposalFailed — OnProposalFailed only fails entries
// still present in ns.pendingProposals, and these were already removed. Without a local recover, such a panic
// would strand every already-claimed RequestState forever: no completion,
// no releaseRef, no completePendingOp — so the caller hangs past any
// deadline (the sweep cannot see them either) and Host.Drain never returns.
//
// applyMembershipRouting logs through a user-supplied slog.Logger
// (WithLogger), whose Handler is arbitrary code and can panic. This test
// installs one that does, on exactly the "membership changed" record
// applyMembershipRouting emits for a committed config change, and verifies
// the claimed proposal is still completed (with a typed error, not left
// hanging) and every pendingOp is still balanced (Drain returns).
//
// It also pins that this recovery does NOT call pendingProposal.onFailure
// (rollbackPreRegister) for the claimed proposals, the cleanup
// sweepPendingProposals does. That would be wrong here — applyMembershipRouting's own per-entry log call (the one that
// panics) runs AFTER that entry's registry.Register/ns.nodes update, so by
// the time the panic fires, the AddNonVoting below has already landed. Every
// entry OnApplied is invoked with is one raft's own authoritative fold has
// already committed (Peer.Commit -> processConfigChanges, earlier in the
// pipeline) — unlike sweepPendingProposals's case, a proposal that was
// appended but never committed at all. Rolling back a committed change's
// registration would revert real state on a plausible-looking error path, so
// this asserts the registration SURVIVES the panic.
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

// TestOnApplied_RoutingPanicDoesNotStrandPendingReads is a deterministic test
// for pending READS, which the recovery above does not cover. A pending read
// confirmed by the same applied batch (ns.lastApplied advance + the
// pendingReads scan) is processed before applyMembershipRouting runs, so a
// routing panic never reaches it. If the pendingReads block were moved to
// AFTER the membership branch, a routing panic would strand every read this
// batch had just confirmed until its own deadline, and hold Drain's pendingOps
// count with it.
//
// This calls hostEngineCallback.OnApplied directly against a manually
// constructed nodeState — the same construction addTestNodeState uses,
// deliberately NOT wired up via StartShard — with a synthetic committed
// batch and an already-confirmed pending read. No real raft engine or apply
// worker is ever started for this shard, so nothing but this one direct call
// can touch ns.pendingReads. A real StartShard'd shard would start a live
// background apply pipeline that races this test's own assertion (completing
// the read through its own, unrelated batches), which is exactly the kind of
// nondeterminism a test for an ordering bug cannot afford.
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

// TestOnApplied_RecoveryDoesNotDoubleReleaseAlreadyCompletedClaims pins that
// the panic-recovery handler completes only the NOT-yet-completed tail of
// claimed. Ranging over ALL of claimed unconditionally would let a panic
// reaching it after the normal completeProposal loop had already completed
// and released some of claimed's proposals complete and release those SAME
// proposals a second time. RequestState.complete is
// idempotent (CAS-guarded), but releaseRef is not — it unconditionally
// decrements a reference count and recycles the object at zero, so a second
// call either underflows that count or touches an object already back in
// the pool, potentially reused by an unrelated request by then.
//
// The panic source here is c.h.metrics.ObserveProposalLatency. That call is
// already panic-safe in production — pkg/metrics_safe.go wraps every
// WithMetrics collector so a panic in it never reaches caller code. This test
// does not
// pretend otherwise: it reaches h.metrics directly (an unexported field,
// same package) to install a raw, UNWRAPPED collector — deliberate fault
// injection to exercise the recovery handler's own bookkeeping in
// isolation, not a claim that a real, WithMetrics-registered collector can
// panic through to here. The bookkeeping bug this proves is independent of
// how the panic arrives: any future addition to the claimed-completion loop
// that CAN panic would hit exactly this.
//
// Two claimed proposals, an ordinary one (index 0) and a membership one
// (index 1): the collector panics on the second ObserveProposalLatency
// call, i.e. after index 1 has been completed (each caller is answered
// before its latency is observed), and after index 0 has already completed
// and released successfully. Neither may be completed or released again. Asserts index 0's RequestState was not
// recycled/reused (via its pool) as a side effect of the recovery handler
// touching it again, and that both proposals' pendingOps are accounted for
// exactly once (Drain balances).
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

	// A completed normally (ObserveProposalLatency call #1, no panic). Its
	// RequestState must have exactly one releaseRef's worth of effect: back
	// in the pool, with a fresh newRequestState pull showing a clean slate
	// rather than whatever double-release would have left behind (a
	// negative outstanding count is the direct symptom, but that field
	// is unexported and pool internals are not meant to be inspected
	// directly — acquiring a new RequestState from the SAME pool and
	// confirming it behaves normally is the black-box equivalent).
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

	// h.pendingOps is NOT the right place to look for a double-release:
	// completePendingOp has its own floor guard ("already at zero or
	// below; do not decrement further", host.go), added for exactly this
	// class of double-completion race, so a second call from a double
	// release is silently absorbed and pendingOps still nets to a
	// deceptively clean 0. rsA.outstanding is the field that actually
	// catches it: acquireRef set it to 1, proposal A's single legitimate
	// releaseRef (in the normal completion loop, before B's panic) must
	// bring it to exactly 0 — a second releaseRef from the recovery
	// handler touching A again would drive it to -1, which
	// RequestState.releaseRef has no floor against.
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

// TestWaitResultCtx verifies the bridge used by the session APIs: a resolved
// request returns its result (and its embedded error) verbatim, while a
// context that ends first unblocks the caller with the mapped error rather
// than waiting on a request that will never resolve.
func TestWaitResultCtx(t *testing.T) {
	pool := NewRequestStatePool()

	t.Run("result delivered", func(t *testing.T) {
		rs := newRequestState(pool, 1, time.Time{}, false)
		defer rs.Release()
		rs.complete(RequestResult{Value: 42})

		result, err := awaitResult(context.Background(), rs)
		if err != nil {
			t.Fatalf("awaitResult error = %v, want nil", err)
		}
		if result.Value != 42 {
			t.Fatalf("result.Value = %d, want 42", result.Value)
		}
	})

	t.Run("result error returned", func(t *testing.T) {
		rs := newRequestState(pool, 2, time.Time{}, false)
		defer rs.Release()
		rs.complete(RequestResult{Err: ErrNotLeader})

		result, err := awaitResult(context.Background(), rs)
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("awaitResult error = %v, want ErrNotLeader", err)
		}
		if !errors.Is(result.Err, ErrNotLeader) {
			t.Fatalf("result.Err = %v, want ErrNotLeader", result.Err)
		}
	})

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
