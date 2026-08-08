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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// ---------------------------------------------------------------------------
// FollowerRead
// ---------------------------------------------------------------------------

// TestFollowerRead_Rejections covers every guard FollowerRead applies before
// it reads local state. Each one protects a caller from reading through a
// half-built or shut-down shard.
func TestFollowerRead_Rejections(t *testing.T) {
	t.Run("host closed", func(t *testing.T) {
		h := newTestHost(t)
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := h.FollowerRead(context.Background(), 1, "q"); !errors.Is(err, ErrClosed) {
			t.Fatalf("FollowerRead error = %v, want ErrClosed", err)
		}
	})

	t.Run("shard not found", func(t *testing.T) {
		h := newTestHost(t)
		if _, err := h.FollowerRead(context.Background(), 999, "q"); !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("FollowerRead error = %v, want ErrShardNotFound", err)
		}
	})

	t.Run("witness cannot serve reads", func(t *testing.T) {
		// A witness stores no log data, so serving a read from it would
		// return state that does not exist.
		h := newTestHost(t)
		addTestNodeState(h, 42, 1)
		testLoadNode(h, 42).cfg.IsWitness = true

		if _, err := h.FollowerRead(context.Background(), 42, "q"); !errors.Is(err, ErrInvalidOperation) {
			t.Fatalf("FollowerRead error = %v, want ErrInvalidOperation", err)
		}
	})

	t.Run("state machine not ready", func(t *testing.T) {
		h := newTestHost(t)
		addTestNodeState(h, 42, 1)

		if _, err := h.FollowerRead(context.Background(), 42, "q"); !errors.Is(err, ErrShardNotReady) {
			t.Fatalf("FollowerRead error = %v, want ErrShardNotReady", err)
		}
	})

	t.Run("engine node unloaded", func(t *testing.T) {
		// The state machine outlives the engine node during teardown; a read
		// in that window must fail rather than read a detached SM.
		h := startReadBufShard(t, testCreateFunc)
		h.engine.UnloadNode(1)

		if _, err := h.FollowerRead(context.Background(), 1, "q"); !errors.Is(err, ErrShardNotReady) {
			t.Fatalf("FollowerRead error = %v, want ErrShardNotReady", err)
		}
	})
}

// TestFollowerRead_FastPathServesAppliedState verifies the fast path: when the
// state machine has already applied everything committed, the read is answered
// immediately from local state without polling.
func TestFollowerRead_FastPathServesAppliedState(t *testing.T) {
	h := startReadBufShard(t, testCreateFunc)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := h.SyncPropose(ctx, 1, []byte("committed-write")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	// mockStateMachine.Lookup echoes the query.
	got, err := h.FollowerRead(ctx, 1, "follower-query")
	if err != nil {
		t.Fatalf("FollowerRead: %v", err)
	}
	if got != "follower-query" {
		t.Fatalf("FollowerRead = %v, want %q", got, "follower-query")
	}
}

// TestFollowerRead_LaggingApplyRespectsDeadline verifies the slow path: when
// the state machine is behind the committed index the read polls, backs off
// after the initial burst, and ultimately honours the caller's deadline
// instead of spinning forever.
func TestFollowerRead_LaggingApplyRespectsDeadline(t *testing.T) {
	h := startReadBufShard(t, testCreateFunc)

	proposeCtx, proposeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer proposeCancel()
	if _, err := h.SyncPropose(proposeCtx, 1, []byte("committed-write")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	ns := testLoadNode(h, 1)
	committed := h.engine.GetNode(1).CommittedIndex()
	if committed == 0 {
		t.Fatal("committed index is 0 after a successful proposal; the lag cannot be modelled")
	}
	// Wait for the apply callback to publish its lastApplied BEFORE injecting
	// the lag. SyncPropose returns from rs.complete, which OnApplied runs
	// several statements EARLIER than ns.lastApplied.Store — so a store of 0
	// here can be overwritten by the still-running callback, and the read then
	// completes immediately instead of timing out. That ordering is fine in
	// production (a caller that observes a not-yet-updated marker makes a
	// follower read wait, which is the safe direction); it is only the test's
	// setup that needs to be sequenced.
	applyDeadline := time.Now().Add(5 * time.Second)
	for ns.lastApplied.Load() < committed {
		if time.Now().After(applyDeadline) {
			t.Fatalf("apply callback never published lastApplied >= %d (got %d)",
				committed, ns.lastApplied.Load())
		}
		time.Sleep(time.Millisecond)
	}

	// Model a replica whose apply loop has fallen behind the commit index.
	ns.lastApplied.Store(0)

	// The deadline is far longer than the 50 fast polls (1ms each) that
	// precede the backoff, so the backoff is exercised before it expires.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := time.Now()
	if _, err := h.FollowerRead(ctx, 1, "q"); !errors.Is(err, ErrTimeout) {
		t.Fatalf("FollowerRead error = %v, want ErrTimeout", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("FollowerRead returned after %v; it did not wait for the state machine to catch up", elapsed)
	}
}

// ---------------------------------------------------------------------------
// QueryRaftLog
// ---------------------------------------------------------------------------

// TestQueryRaftLog_ReturnsOwnedEntryCopies verifies that the entries handed to
// a caller own their command bytes. GetRange serves them from reusable
// internal buffers, so a shallow copy would let the next query — or the log
// reader itself — mutate data the caller still holds.
func TestQueryRaftLog_ReturnsOwnedEntryCopies(t *testing.T) {
	h := startReadBufShard(t, testCreateFunc)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := []byte("query-raft-log-payload")
	if _, err := h.SyncPropose(ctx, 1, payload); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	rng, err := h.GetLogRange(1)
	if err != nil {
		t.Fatalf("GetLogRange: %v", err)
	}

	entries, err := h.QueryRaftLog(ctx, 1, rng.FirstIndex, rng.LastIndex+1, 1<<20)
	if err != nil {
		t.Fatalf("QueryRaftLog: %v", err)
	}

	var found int
	for i := range entries {
		if bytes.Equal(entries[i].Cmd, payload) {
			found++
			// Scribble on the caller's copy; the log must not observe it.
			entries[i].Cmd[0] = 'X'
		}
	}
	if found != 1 {
		t.Fatalf("found %d entries carrying the proposed payload, want 1", found)
	}

	again, err := h.QueryRaftLog(ctx, 1, rng.FirstIndex, rng.LastIndex+1, 1<<20)
	if err != nil {
		t.Fatalf("second QueryRaftLog: %v", err)
	}
	var stillIntact bool
	for i := range again {
		if bytes.Equal(again[i].Cmd, payload) {
			stillIntact = true
		}
	}
	if !stillIntact {
		t.Fatal("mutating the returned Cmd corrupted the Raft log: QueryRaftLog aliased internal buffers")
	}
}

// TestQueryRaftLog_RangeErrors verifies that an unreadable range is reported
// rather than silently truncated, and that a shard whose log reader is not
// wired yet is reported as not found.
func TestQueryRaftLog_RangeErrors(t *testing.T) {
	t.Run("range beyond the log", func(t *testing.T) {
		h := startReadBufShard(t, testCreateFunc)
		ctx := context.Background()

		entries, err := h.QueryRaftLog(ctx, 1, 10_000, 10_010, 1<<20)
		if err == nil {
			t.Fatalf("QueryRaftLog returned %d entries for a range past the log, want an error", len(entries))
		}
	})

	t.Run("log reader not wired", func(t *testing.T) {
		// An engine node exists but carries no log reader — the window
		// during shard construction before the reader is attached.
		h := newTestHost(t)
		h.engine.LoadNode(newMinimalEngineNode(9, 1))
		t.Cleanup(func() { h.engine.UnloadNode(9) })

		if _, err := h.QueryRaftLog(context.Background(), 9, 1, 2, 1<<20); !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("QueryRaftLog error = %v, want ErrShardNotFound", err)
		}
	})
}

// ---------------------------------------------------------------------------
// RequestLeaderTransfer
// ---------------------------------------------------------------------------

// TestRequestLeaderTransfer_ObserverAndRemovedTargets verifies that leadership is never
// handed to a replica that cannot hold it: an observer does not vote, and a
// removed replica is no longer part of the shard. Accepting either would stall
// the shard until the election timeout.
func TestRequestLeaderTransfer_ObserverAndRemovedTargets(t *testing.T) {
	h := startSingleVoterShard(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.SyncRequestAddObserver(ctx, 1, 10, "127.0.0.1:6000", 0); err != nil {
		t.Fatalf("SyncRequestAddObserver: %v", err)
	}

	err := h.RequestLeaderTransfer(ctx, 1, 10)
	var invalid *InvalidMemberError
	if !errors.As(err, &invalid) {
		t.Fatalf("RequestLeaderTransfer to an observer = %v (%T), want *InvalidMemberError", err, err)
	}
	if invalid.ReplicaID != 10 {
		t.Fatalf("InvalidMemberError.ReplicaID = %d, want 10", invalid.ReplicaID)
	}

	membership, err := h.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	if err := h.SyncRequestDeleteNode(ctx, 1, 10, membership.ConfigChangeID); err != nil {
		t.Fatalf("SyncRequestDeleteNode: %v", err)
	}

	err = h.RequestLeaderTransfer(ctx, 1, 10)
	if !errors.As(err, &invalid) {
		t.Fatalf("RequestLeaderTransfer to a removed replica = %v (%T), want *InvalidMemberError", err, err)
	}
}

// ---------------------------------------------------------------------------
// Witness membership / policy wrappers
// ---------------------------------------------------------------------------

// TestSyncWitnessChanges_ContextEndsUncommittedWait verifies that the
// synchronous witness wrappers are bounded by the caller's context when the
// change cannot commit. The shard is put into a state where quorum is
// unreachable (a second voter that does not exist), so neither change can
// ever be committed.
func TestSyncWitnessChanges_ContextEndsUncommittedWait(t *testing.T) {
	newUncommittableShard := func(t *testing.T) *Host {
		t.Helper()
		h := startSingleVoterShard(t)
		setupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := h.SyncRequestAddNode(setupCtx, 1, 4, "127.0.0.1:6001", 0); err != nil {
			t.Fatalf("SyncRequestAddNode: %v", err)
		}
		members, err := h.GetShardMembership(1)
		if err != nil {
			t.Fatalf("GetShardMembership: %v", err)
		}
		if _, ok := members.Nodes[4]; !ok {
			t.Fatalf("replica 4 is not a voter, quorum is still reachable: %+v", members.Nodes)
		}
		return h
	}

	canceledCtx := func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, cancel
	}
	expiredCtx := func() (context.Context, context.CancelFunc) {
		return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	}

	t.Run("add witness canceled", func(t *testing.T) {
		h := newUncommittableShard(t)
		ctx, cancel := canceledCtx()
		defer cancel()
		if err := h.SyncRequestAddWitness(ctx, 1, 7, "127.0.0.1:6002", 0); !errors.Is(err, ErrCanceled) {
			t.Fatalf("SyncRequestAddWitness error = %v, want ErrCanceled", err)
		}
		members, err := h.GetShardMembership(1)
		if err != nil {
			t.Fatalf("GetShardMembership: %v", err)
		}
		if _, ok := members.Witnesses[7]; ok {
			t.Fatal("an uncommitted witness addition became visible in membership")
		}
	})

	t.Run("add witness deadline exceeded", func(t *testing.T) {
		h := newUncommittableShard(t)
		ctx, cancel := expiredCtx()
		defer cancel()
		if err := h.SyncRequestAddWitness(ctx, 1, 7, "127.0.0.1:6002", 0); !errors.Is(err, ErrTimeout) {
			t.Fatalf("SyncRequestAddWitness error = %v, want ErrTimeout", err)
		}
	})

	t.Run("update witness policy canceled", func(t *testing.T) {
		h := newUncommittableShard(t)
		ctx, cancel := canceledCtx()
		defer cancel()
		policy := config.WitnessConfig{MaxWitnesses: 2}
		if err := h.SyncRequestUpdateWitnessPolicy(ctx, 1, policy, 0); !errors.Is(err, ErrCanceled) {
			t.Fatalf("SyncRequestUpdateWitnessPolicy error = %v, want ErrCanceled", err)
		}
	})

	t.Run("update witness policy deadline exceeded", func(t *testing.T) {
		h := newUncommittableShard(t)
		ctx, cancel := expiredCtx()
		defer cancel()
		policy := config.WitnessConfig{MaxWitnesses: 2}
		if err := h.SyncRequestUpdateWitnessPolicy(ctx, 1, policy, 0); !errors.Is(err, ErrTimeout) {
			t.Fatalf("SyncRequestUpdateWitnessPolicy error = %v, want ErrTimeout", err)
		}
	})
}

// ---------------------------------------------------------------------------
// WithMetrics
// ---------------------------------------------------------------------------

// typedNilCollector exists only so a TYPED nil can be passed as a
// MetricsCollector interface value.
type typedNilCollector struct{ NoOpMetricsCollector }

// TestWithMetrics_RejectsNilCollectors verifies that neither an untyped nil
// nor a typed nil pointer is installed as the metrics collector. A typed nil
// is non-nil as an interface value, so storing it would turn every metric
// call on the proposal hot path into a nil-pointer dereference.
func TestWithMetrics_RejectsNilCollectors(t *testing.T) {
	t.Run("untyped nil", func(t *testing.T) {
		var opts hostOptions
		WithMetrics(nil)(&opts)
		if opts.metrics != nil {
			t.Fatalf("metrics = %#v, want nil", opts.metrics)
		}
	})

	t.Run("typed nil pointer", func(t *testing.T) {
		var opts hostOptions
		var collector MetricsCollector = (*typedNilCollector)(nil)
		WithMetrics(collector)(&opts)
		if opts.metrics != nil {
			t.Fatalf("metrics = %#v, want nil for a typed-nil collector", opts.metrics)
		}
	})

	t.Run("real collector installed", func(t *testing.T) {
		var opts hostOptions
		collector := NoOpMetricsCollector{}
		WithMetrics(collector)(&opts)
		if opts.metrics == nil {
			t.Fatal("a usable collector was discarded")
		}
	})
}
