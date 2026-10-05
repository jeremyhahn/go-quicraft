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
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// The apply path is unconditionally FAIL-FAST
//
// A state machine that refuses a committed entry gets ONE chance at it. The
// shard latches failed on that first refusal and goes offline until it is
// reloaded or recovered from a snapshot; lastApplied is not advanced. That is
// forced by Raft's state-machine safety property, not a tuning choice: a
// deterministic apply failure cannot be made to succeed by retrying, and
// advancing lastApplied past a batch this replica skipped would diverge it from
// its peers.
//
// The attempt COUNT below is what makes this test non-vacuous. Nothing about
// the engine's current shape is asserted second-hand — the state machine itself
// records every offer of the poisoned entry, so a retry loop introduced later
// drives the count above one and fails here on a number.
// ---------------------------------------------------------------------------

// errPoisonedApply is the failure a poisonApplySM reports for the one command
// the test feeds it.
var errPoisonedApply = errors.New("quicraft_test: poisoned apply")

// poisonCmd is the payload poisonApplySM refuses. Config-change and no-op
// entries carry different payloads, so the shard reaches the leader state
// normally and only the test's own proposal trips the latch.
var poisonCmd = []byte("poison-this-apply")

// poisonApplySM fails Update for exactly one command and counts how many times
// that command is offered to it.
//
// The count is the whole assertion. A fail-fast apply path offers the poisoned
// entry ONCE; a retrying apply path re-offers the same entry, so the count rises
// and the test fails on a number rather than passing quietly.
type poisonApplySM struct {
	// panicOnPoison selects HOW the state machine refuses the entry: false
	// returns errPoisonedApply, true panics. The engine builds a different
	// error for each (*engine.ApplyError and *engine.SMPanicError), and only
	// one of them is remapped onto the public ErrShardFailed, so both halves
	// are exercised.
	panicOnPoison bool

	poisonApplies atomic.Int64
	// holdPoison, when non-nil, parks Update on the poison entry until it is
	// closed. poisonHeld, when non-nil, is closed the moment Update parks, so
	// the test knows the entry is committed and the apply is in flight.
	holdPoison chan struct{}
	poisonHeld chan struct{}
}

func (p *poisonApplySM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		if bytes.Equal(entries[i].Cmd, poisonCmd) {
			p.poisonApplies.Add(1)
			// A test that needs a read PENDING ACROSS the halt parks the
			// poison apply here: the entry is committed (so a later read
			// index lands past it) but not yet failed, and the read waits
			// on lastApplied, which this entry will never advance.
			if p.holdPoison != nil {
				if p.poisonHeld != nil {
					close(p.poisonHeld)
				}
				<-p.holdPoison
			}
			if p.panicOnPoison {
				panic(errPoisonedApply)
			}
			return errPoisonedApply
		}
		if i < len(results) {
			results[i] = sm.Result{Value: entries[i].Index}
		}
	}
	return nil
}

func (p *poisonApplySM) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return query, nil
}

func (p *poisonApplySM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (p *poisonApplySM) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (p *poisonApplySM) Close(_ context.Context) error { return nil }

var _ sm.StateMachine = (*poisonApplySM)(nil)

// TestApplyFailureFailsTheShardWithoutRetrying pins the fail-fast apply policy:
// the poisoned entry is offered to the state machine exactly once, the proposal
// that carried it fails, and the shard latches.
//
// Each assertion closes a way the others could pass for the wrong reason:
//
//   - "exactly one offer" is the property under test. Zero offers would also
//     satisfy "no retries", so the proposal is required to FAIL — a proposal
//     that succeeded, or one still in flight at its deadline, means the entry
//     never reached apply and the count of 0 says nothing.
//   - one offer is also what a retry that has not run its second attempt YET
//     looks like, so OnShardFailed is required to fire. The latch is terminal:
//     once it is set the shard is offline, which is only reachable if the single
//     attempt was the last one.
func TestApplyFailureFailsTheShardWithoutRetrying(t *testing.T) {
	tests := []struct {
		name          string
		panicOnPoison bool
		// wantErr is the identity the SyncPropose caller must be able to match.
		wantErr error
	}{
		{
			// A returned error becomes *engine.ApplyError, which carries no Is
			// method and is therefore NOT remapped: the caller receives the
			// state machine's own error, wrapped.
			name:          "Update returns an error",
			panicOnPoison: false,
			wantErr:       errPoisonedApply,
		},
		{
			// A panic becomes *engine.SMPanicError, whose Is method matches
			// engine.ErrShardFailed; Host.translateProposalError maps that onto
			// the public ErrShardFailed. This is the only path by which a caller
			// outside the module sees the public sentinel.
			name:          "Update panics",
			panicOnPoison: true,
			wantErr:       ErrShardFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testHostConfig(t)
			failed := make(chan config.ShardFailedInfo, 4)
			cfg.EventListener = &config.EventListener{
				OnShardFailed: func(info config.ShardFailedInfo) {
					select {
					case failed <- info:
					default:
					}
				},
			}

			h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer h.Close()

			machine := &poisonApplySM{panicOnPoison: tt.panicOnPoison}
			create := func(shardID, replicaID uint64) interface{} { return machine }
			if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
				create, testShardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard: %v", startErr)
			}
			waitForLeader(t, h, 1)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// The proposal must be refused, and refused with an identity the
			// caller can match. A nil error here would mean the state machine
			// never saw the poison, which would also produce a count of 0 below.
			_, proposeErr := h.SyncPropose(ctx, 1, poisonCmd)
			if proposeErr == nil {
				t.Fatal("SyncPropose of a command the state machine refuses returned nil; " +
					"the apply error never reached the caller")
			}
			if !errors.Is(proposeErr, tt.wantErr) {
				t.Fatalf("SyncPropose = %v (%T), want it to match %v",
					proposeErr, proposeErr, tt.wantErr)
			}

			// The latch must have tripped. This is what proves the single
			// attempt was terminal rather than a retry still pending.
			select {
			case <-failed:
			case <-time.After(5 * time.Second):
				t.Fatal("OnShardFailed never fired: the apply failure did not fail the shard")
			}

			// Give any retry loop that might exist room to run before counting.
			// Re-applying an in-memory entry costs microseconds, so a count of 1
			// after this window is a real absence of retries and not a race the
			// test happened to win.
			time.Sleep(200 * time.Millisecond)

			if got := machine.poisonApplies.Load(); got != 1 {
				t.Fatalf("the poisoned entry was offered to the state machine %d times, "+
					"want exactly 1. The apply path is fail-fast by design: retrying a "+
					"deterministic apply failure cannot succeed, and advancing lastApplied "+
					"past the batch would diverge this replica from its peers. If a retry "+
					"policy is now intended, it needs its own design, not a louder counter.",
					got)
			}
		})
	}
}

// TestProposalsAfterTheLatchAreRefusedImmediately pins the propose-time gate.
//
// The latch is terminal, and the step worker skips a failed node from then on.
// Without a gate the proposal is still accepted, registered in the pending map
// and delivered to an inbox nothing drains, so it is answered only when the
// sweeper reclaims it at its deadline: every call after the failure blocks for
// its full timeout and presents as a slow shard rather than a dead one.
//
// Two things make this non-vacuous. The deadline is generous relative to the
// assertion — the call must return in a small fraction of it, so a regression
// that reinstates the wait fails on elapsed time rather than passing slowly.
// And the state machine's counter must not move, which distinguishes "refused
// before proposing" from "proposed, applied, and refused again".
func TestProposalsAfterTheLatchAreRefusedImmediately(t *testing.T) {
	cfg := testHostConfig(t)
	failed := make(chan config.ShardFailedInfo, 4)
	cfg.EventListener = &config.EventListener{
		OnShardFailed: func(info config.ShardFailedInfo) {
			select {
			case failed <- info:
			default:
			}
		},
	}

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	machine := &poisonApplySM{}
	create := func(shardID, replicaID uint64) interface{} { return machine }
	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)

	// Trip the latch.
	trip, cancelTrip := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelTrip()
	if _, proposeErr := h.SyncPropose(trip, 1, poisonCmd); proposeErr == nil {
		t.Fatal("SyncPropose of the poisoned command returned nil; the latch never tripped")
	}
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnShardFailed never fired: the shard did not latch")
	}

	applied := machine.poisonApplies.Load()

	// A perfectly ordinary command, one the state machine would accept. On a
	// latched shard it must be refused by identity, not by timeout.
	const budget = 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	start := time.Now()
	_, err = h.SyncPropose(ctx, 1, []byte("ordinary-command"))
	elapsed := time.Since(start)

	if !errors.Is(err, ErrShardFailed) {
		t.Fatalf("SyncPropose on a latched shard = %v (%T), want ErrShardFailed", err, err)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrTimeout) {
		t.Fatalf("SyncPropose on a latched shard timed out (%v) instead of being "+
			"refused; the propose-time gate is missing", err)
	}
	// The gate short-circuits before any queueing, so this returns in
	// microseconds. A tenth of the budget is a wide margin that still fails
	// decisively if the call goes back to waiting out its deadline.
	if elapsed > budget/10 {
		t.Fatalf("SyncPropose on a latched shard took %v of a %v budget; a refusal "+
			"should be immediate, so this call is still being queued and swept",
			elapsed, budget)
	}
	if got := machine.poisonApplies.Load(); got != applied {
		t.Fatalf("the state machine saw %d poison applies after the latch, was %d: "+
			"a refused proposal must never reach apply", got, applied)
	}

	// Temporary must stay false: retrying a latched shard spins forever.
	if Temporary(err) {
		t.Fatal("Temporary(ErrShardFailed) is true; a latched shard does not recover " +
			"on its own, so a retry loop against it would never terminate")
	}
}

// ---------------------------------------------------------------------------
// Reads against a latched shard fail fast too
// ---------------------------------------------------------------------------
//
// The read path needs the same gate as the propose path above. A linearizable
// read completes only when lastApplied reaches its read index, and a halted
// shard never applies again — so without the gate a SyncRead against a latched
// shard would be accepted, confirmed by a leader that is still perfectly
// healthy, and then wait for an apply that cannot come, returning at the
// caller's deadline with ErrTimeout (5m6s under the default). That message
// points at consensus, the network or a lost leader; the cause is an Apply
// that failed earlier, on a healthy key, on the same replica.
//
// Both tests assert on LATENCY and not only on the error, for the reason the
// issue gives: a message-only assertion passes against the old behaviour right
// up until the deadline expires. The budget-fraction bound is what fails
// decisively if the read goes back to waiting out its deadline.

// latchTheShard trips the failure latch on shard 1 through a poisoned
// proposal and waits for the host to report it. Shared by the two read tests.
func latchTheShard(t *testing.T, h *Host, failed <-chan config.ShardFailedInfo) {
	t.Helper()
	trip, cancelTrip := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelTrip()
	if _, err := h.SyncPropose(trip, 1, poisonCmd); err == nil {
		t.Fatal("SyncPropose of the poisoned command returned nil; the latch never tripped")
	}
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnShardFailed never fired: the shard did not latch")
	}
}

// parkPoison arms the state machine to park on the poison entry and returns
// the release. The release is ALSO registered with t.Cleanup, because a test
// that fails an assertion while the apply is parked has left a goroutine
// blocked on holdPoison inside SM.Update -- and h.Close in the host's own
// cleanup then waits on that apply forever, turning an ordinary test failure
// into a hang until the suite timeout. The Once makes
// the explicit release and the cleanup release safe together.
func parkPoison(t *testing.T, m *poisonApplySM) (release func()) {
	t.Helper()
	m.holdPoison = make(chan struct{})
	m.poisonHeld = make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(m.holdPoison) }) }
	t.Cleanup(release)
	return release
}

// newLatchableHost builds a single-replica host over a poisonApplySM with the
// failure listener wired, and elects it.
func newLatchableHost(t *testing.T) (*Host, *poisonApplySM, chan config.ShardFailedInfo) {
	t.Helper()
	failed := make(chan config.ShardFailedInfo, 1)
	cfg := testHostConfig(t)
	cfg.EventListener = &config.EventListener{
		OnShardFailed: func(info config.ShardFailedInfo) {
			select {
			case failed <- info:
			default:
			}
		},
	}
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	machine := &poisonApplySM{}
	create := func(shardID, replicaID uint64) interface{} { return machine }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	return h, machine, failed
}

// TestReadsAfterTheLatchAreRefusedImmediately covers a read that ARRIVES
// after the halt: the gate in ReadIndex refuses it before anything is
// registered, so it returns in microseconds naming the cause.
func TestReadsAfterTheLatchAreRefusedImmediately(t *testing.T) {
	h, _, failed := newLatchableHost(t)

	// A read on the healthy shard works, so the refusal below is the latch
	// and not a broken read path.
	pre, cancelPre := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPre()
	if _, err := h.SyncRead(pre, 1, "any-key"); err != nil {
		t.Fatalf("SyncRead on the healthy shard: %v", err)
	}

	latchTheShard(t, h, failed)

	const budget = 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	start := time.Now()
	_, err := h.SyncRead(ctx, 1, "any-key")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrShardFailed) {
		t.Fatalf("SyncRead on a latched shard = %v (%T), want ErrShardFailed", err, err)
	}
	if errors.Is(err, ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SyncRead on a latched shard timed out (%v) instead of being refused; "+
			"the reader paid the deadline to learn nothing", err)
	}
	if elapsed > budget/10 {
		t.Fatalf("SyncRead on a latched shard took %v of a %v budget; a refusal should "+
			"be immediate, so this read is still being registered and waited on",
			elapsed, budget)
	}
	if Temporary(err) {
		t.Fatal("Temporary(ErrShardFailed) is true; a retry loop against a latched " +
			"shard would never terminate")
	}
}

// TestPendingReadsAreFailedWhenTheLatchTrips covers the other half: a read
// that is already INSIDE — registered and confirmed — when the halt happens.
// The ReadIndex gate cannot help it. OnShardFailed has to find it in
// pendingReads and complete it, or it waits for its deadline exactly as
// before.
//
// Getting a read genuinely pending across the halt needs the apply to be held
// open: on a single replica the poison entry commits and fails in well under a
// millisecond, and a read issued alongside it simply races it — confirmed
// before the poison committed, its read index already applied, returning nil
// and proving nothing. So the state machine parks on the poison entry, the
// test issues its read once the park is confirmed (the entry is committed, so
// the read index lands past it and cannot be satisfied), and only then
// releases the apply to fail.
func TestPendingReadsAreFailedWhenTheLatchTrips(t *testing.T) {
	h, machine, failed := newLatchableHost(t)
	releasePoison := parkPoison(t, machine)

	const budget = 10 * time.Second
	trip, cancelTrip := context.WithTimeout(context.Background(), budget)
	defer cancelTrip()
	go func() { _, _ = h.SyncPropose(trip, 1, poisonCmd) }()

	// The poison entry is committed and its apply is parked.
	select {
	case <-machine.poisonHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("the poison apply never parked; the proposal did not commit")
	}

	// Now the read: its read index is past the parked entry, so it can
	// complete only when lastApplied advances past the poison — which it
	// never will.
	readCtx, cancelRead := context.WithTimeout(context.Background(), budget)
	defer cancelRead()
	type outcome struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan outcome, 1)
	go func() {
		start := time.Now()
		_, err := h.SyncRead(readCtx, 1, "any-key")
		done <- outcome{err: err, elapsed: time.Since(start)}
	}()

	// Prove the read is genuinely pending rather than already answered.
	select {
	case got := <-done:
		t.Fatalf("the read returned (%v) while the apply was still parked; the "+
			"fixture did not get it pending across the halt", got.err)
	case <-time.After(300 * time.Millisecond):
	}

	// Release the apply; it fails, the latch trips, OnShardFailed fires.
	released := time.Now()
	releasePoison()
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnShardFailed never fired: the shard did not latch")
	}

	var got outcome
	select {
	case got = <-done:
	case <-time.After(budget + 2*time.Second):
		t.Fatal("the pending read never returned at all")
	}
	sinceHalt := time.Since(released)

	if errors.Is(got.err, ErrTimeout) || errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatalf("a read pending across the halt timed out (%v) after %v; OnShardFailed "+
			"did not fail it, so it waited for an apply that can never come",
			got.err, got.elapsed)
	}
	if !errors.Is(got.err, ErrShardFailed) {
		t.Fatalf("a read pending across the halt = %v (%T), want ErrShardFailed", got.err, got.err)
	}
	// A read appends nothing to the log, so there is nothing that may still
	// apply: MaybeApplied is a proposal's classification, not a read's.
	if MaybeApplied(got.err) {
		t.Fatalf("a read pending across the halt = %v reports MaybeApplied; a read "+
			"has no entry that could commit", got.err)
	}
	// Prompt means measured from the halt, not from when the read began: the
	// read spent 300ms deliberately pending before the release.
	if sinceHalt > budget/10 {
		t.Fatalf("a read pending across the halt returned %v after the latch tripped, "+
			"against a %v budget; it should return the moment the latch trips, not "+
			"on the next sweep or at its deadline", sinceHalt, budget)
	}
}

// TestReadsRacingTheLatchNeverTimeOut hammers the interleaving in which a read
// passes the IsFailed check at the top of ReadIndex, then has the latch trip BEFORE its pendingReads entry is
// stored. OnShardFailed scans an empty map and returns; the failed node stops
// processing its queue; nothing ever completes the read, and it waits for its
// deadline.
//
// The guard is a post-registration re-check with rollback, the same guard
// ReadIndex already had for close/stop. The window is a few instructions
// wide, so it cannot be hit on demand without a hook in production code —
// which is not worth adding for a test. Instead this parks the poison apply,
// starts many readers that each issue a read the instant the latch is
// released, and requires EVERY read to come back as ErrShardFailed well
// inside its budget. Across enough readers the interleaving is exercised; a
// single ErrTimeout is the defect.
func TestReadsRacingTheLatchNeverTimeOut(t *testing.T) {
	h, machine, failed := newLatchableHost(t)
	releasePoison := parkPoison(t, machine)

	const budget = 5 * time.Second
	trip, cancelTrip := context.WithTimeout(context.Background(), budget)
	defer cancelTrip()
	go func() { _, _ = h.SyncPropose(trip, 1, poisonCmd) }()
	select {
	case <-machine.poisonHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("the poison apply never parked")
	}

	const readers = 64
	go_ := make(chan struct{})
	results := make(chan error, readers)
	for i := 0; i < readers; i++ {
		go func() {
			<-go_
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			_, err := h.SyncRead(ctx, 1, "any-key")
			results <- err
		}()
	}

	// Release the readers and the latch together, so reads land on every
	// side of the halt: some before the IsFailed check, some between the
	// check and the store, some after.
	start := time.Now()
	close(go_)
	releasePoison()
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnShardFailed never fired")
	}

	var timedOut, refused, other int
	for i := 0; i < readers; i++ {
		select {
		case err := <-results:
			switch {
			case errors.Is(err, ErrTimeout) || errors.Is(err, context.DeadlineExceeded):
				timedOut++
			case errors.Is(err, ErrShardFailed):
				refused++
			default:
				other++
				t.Logf("reader returned %v (%T)", err, err)
			}
		case <-time.After(budget + 2*time.Second):
			t.Fatalf("only %d of %d readers returned", i, readers)
		}
	}
	elapsed := time.Since(start)
	if timedOut > 0 {
		t.Fatalf("%d of %d reads racing the latch timed out (%v elapsed): a read "+
			"registered between the IsFailed check and the drain was orphaned; the "+
			"post-registration re-check is missing", timedOut, readers, elapsed)
	}
	if other > 0 {
		t.Fatalf("%d of %d reads returned something other than ErrShardFailed", other, readers)
	}
	if refused != readers {
		t.Fatalf("refused=%d, want %d", refused, readers)
	}
	// All 64 must have returned promptly — none waited toward the deadline.
	if elapsed > budget/2 {
		t.Fatalf("readers took %v against a %v budget; some waited toward their deadline", elapsed, budget)
	}
}

// TestFollowerReadsAgainstALatchedShardFailFast covers the read path that does
// not go through ReadIndex. FollowerRead polls lastApplied against
// CommittedIndex, and a halted shard freezes lastApplied below committed
// forever, so without its own gate the wait could never end: with a deadline it
// would return ErrTimeout, and with context.Background() it would never return
// at all.
//
// Both halves are exercised on the one fixture: a read that ARRIVES after
// the halt is refused before entering the wait, and a read that is already
// INSIDE the wait when the latch trips leaves it on the next poll.
func TestFollowerReadsAgainstALatchedShardFailFast(t *testing.T) {
	h, machine, failed := newLatchableHost(t)
	releasePoison := parkPoison(t, machine)

	const budget = 10 * time.Second
	trip, cancelTrip := context.WithTimeout(context.Background(), budget)
	defer cancelTrip()
	go func() { _, _ = h.SyncPropose(trip, 1, poisonCmd) }()
	select {
	case <-machine.poisonHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("the poison apply never parked")
	}

	// A follower read issued NOW is inside the wait: the poison entry is
	// committed (so committed > lastApplied) and parked (so lastApplied will
	// never catch up). Deadline-less on purpose -- that is the case that hung
	// forever, and a deadline here would let the test pass by timing out.
	type outcome struct {
		err     error
		elapsed time.Duration
	}
	// FollowerRead captures CommittedIndex at entry and waits for lastApplied
	// to reach THAT. poisonHeld fires when the apply parks, which is after
	// the entry was handed to the apply worker but the committed marker can
	// still be one step behind -- and a read that captures committed=N-1
	// is legitimately satisfied at N-1 and proves nothing. Wait until the
	// committed index is past lastApplied, so the read's target is the
	// entry that will never apply.
	engNode := h.engine.GetNode(1)
	ns, err := h.getNode(1)
	if err != nil {
		t.Fatalf("getNode: %v", err)
	}
	waitFor := time.Now().Add(5 * time.Second)
	for engNode.CommittedIndex() <= ns.lastApplied.Load() {
		if time.Now().After(waitFor) {
			t.Fatalf("committed (%d) never advanced past lastApplied (%d); the poison "+
				"entry did not commit", engNode.CommittedIndex(), ns.lastApplied.Load())
		}
		time.Sleep(time.Millisecond)
	}

	inside := make(chan outcome, 1)
	go func() {
		start := time.Now()
		_, err := h.FollowerRead(context.Background(), 1, "any-key")
		inside <- outcome{err: err, elapsed: time.Since(start)}
	}()
	select {
	case got := <-inside:
		t.Fatalf("the follower read returned (%v) while the apply was still parked; "+
			"it should be waiting on lastApplied", got.err)
	case <-time.After(300 * time.Millisecond):
	}

	released := time.Now()
	releasePoison()
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnShardFailed never fired")
	}

	// The read that was inside the wait must leave it promptly.
	select {
	case got := <-inside:
		if !errors.Is(got.err, ErrShardFailed) {
			t.Fatalf("follower read pending across the halt = %v (%T), want ErrShardFailed", got.err, got.err)
		}
		if since := time.Since(released); since > time.Second {
			t.Fatalf("follower read left its wait %v after the latch tripped; the poll "+
				"loop should notice within a poll interval", since)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the follower read never returned: with context.Background() and " +
			"no latch check in the poll loop, the read hangs")
	}

	// And one that ARRIVES after the halt is refused before the wait.
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()
	_, err = h.FollowerRead(ctx, 1, "any-key")
	elapsed := time.Since(start)
	if !errors.Is(err, ErrShardFailed) {
		t.Fatalf("follower read after the halt = %v (%T), want ErrShardFailed", err, err)
	}
	if elapsed > budget/10 {
		t.Fatalf("follower read after the halt took %v; it should be refused before entering the wait", elapsed)
	}
}

// pendingProposalCount reports how many proposals ns holds in its pending map.
func pendingProposalCount(ns *nodeState) int {
	n := 0
	ns.pendingProposals.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

// TestProposalRetriedFromItsFailureIsRefusedAtAdmission: the proposal whose
// apply trips the latch is failed only once the latch has tripped, so a retry
// issued the instant it is failed — here from inside its own completion, on
// the apply goroutine — is refused at admission with ErrShardFailed. It is
// never registered: a halted shard answers nothing it registers, so a
// registered retry would wait out the pending sweep and report a timeout.
func TestProposalRetriedFromItsFailureIsRefusedAtAdmission(t *testing.T) {
	h, machine, failed := newLatchableHost(t)
	releasePoison := parkPoison(t, machine)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rs, err := h.Propose(ctx, 1, poisonCmd)
	if err != nil {
		t.Fatalf("Propose of the poisoned command: %v", err)
	}
	defer rs.Release()
	select {
	case <-machine.poisonHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("the poison apply never parked")
	}

	// Retry from inside the poisoned proposal's failure, before anything else
	// can run: the completion site claims the entry and runs onFailure.
	ns := testLoadNode(h, 1)
	var (
		retryRS  *RequestState
		retryErr error
	)
	retried := make(chan struct{})
	installed := 0
	ns.pendingProposals.Range(func(key, value any) bool {
		pp := value.(*pendingProposal)
		ns.pendingProposals.Store(key, &pendingProposal{
			rs:       pp.rs,
			deadline: pp.deadline,
			floor:    pp.floor,
			onFailure: func() {
				retryRS, retryErr = h.Propose(ctx, 1, []byte("retry"))
				close(retried)
			},
		})
		installed++
		return true
	})
	if installed != 1 {
		t.Fatalf("fixture: %d pending proposals, want the poisoned one only", installed)
	}

	releasePoison()
	select {
	case <-retried:
	case <-time.After(5 * time.Second):
		t.Fatal("the poisoned proposal was never failed")
	}
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnShardFailed never fired")
	}
	if retryRS != nil {
		retryRS.Release()
		t.Fatal("a proposal retried from its predecessor's failure was admitted: the " +
			"latch had not tripped when the failure woke the caller")
	}
	if !errors.Is(retryErr, ErrShardFailed) {
		t.Fatalf("retried Propose = %v, want ErrShardFailed", retryErr)
	}
	if n := pendingProposalCount(ns); n != 0 {
		t.Fatalf("%d proposals left pending on the halted shard, want 0", n)
	}
}

// TestProposalsAdmittedBeforeTheLatchAreFailedPromptly: a proposal that passed
// admission and registered before the latch tripped — queued behind the
// apply that trips it — is answered ErrShardFailed when the shard halts, not
// left for the pending sweep to time out.
func TestProposalsAdmittedBeforeTheLatchAreFailedPromptly(t *testing.T) {
	h, machine, failed := newLatchableHost(t)
	releasePoison := parkPoison(t, machine)
	const budget = 5 * time.Second
	trip, cancelTrip := context.WithTimeout(context.Background(), budget)
	defer cancelTrip()
	go func() { _, _ = h.SyncPropose(trip, 1, poisonCmd) }()
	select {
	case <-machine.poisonHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("the poison apply never parked")
	}

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	rs, err := h.Propose(ctx, 1, []byte("ordinary-command"))
	if err != nil {
		t.Fatalf("Propose before the latch: %v", err)
	}
	defer rs.Release()

	releasePoison()
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnShardFailed never fired")
	}
	select {
	case res := <-rs.ResultC():
		if !errors.Is(res.Err, ErrShardFailed) {
			t.Fatalf("proposal admitted before the latch completed with %v, want ErrShardFailed", res.Err)
		}
	case <-time.After(budget / 2):
		t.Fatal("a proposal admitted before the latch was never answered: the halt " +
			"drained reads only, and the halted shard never applies it")
	}
	if n := pendingProposalCount(testLoadNode(h, 1)); n != 0 {
		t.Fatalf("%d proposals left pending on the halted shard, want 0", n)
	}
}

// TestProposalRegisteredAfterTheLatchIsRolledBack covers the last window: the
// admission check passed, then the latch tripped and the halt drained the
// pending map, and only then did the registration land. The registration
// re-checks the latch after storing, so it rolls itself back and refuses with
// ErrShardFailed instead of leaving an entry nothing will drain.
func TestProposalRegisteredAfterTheLatchIsRolledBack(t *testing.T) {
	h, _, failed := newLatchableHost(t)
	ns := testLoadNode(h, 1)
	engNode := h.engine.GetNode(1)
	// The latch trips, and the halt drains, between the admission check and
	// the registration.
	h.engine.FailShard(engNode, errors.New("latched between the check and the registration"))
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnShardFailed never fired")
	}

	before := h.pendingOps.Load()
	rs := newRequestState(ns.requestPool, nextRequestKey(), time.Now().Add(time.Minute), false)
	err := h.registerPendingProposal(ns, engNode, rs.key, &pendingProposal{rs: rs, deadline: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrShardFailed) {
		t.Fatalf("registration after the latch = %v, want ErrShardFailed", err)
	}
	if MaybeApplied(err) {
		t.Fatal("a registration rolled back at admission reports MaybeApplied: it never " +
			"reached raft, so the refusal is definite and a retry is safe")
	}
	if n := pendingProposalCount(ns); n != 0 {
		t.Fatalf("%d proposals left pending after a rolled-back registration, want 0", n)
	}
	if after := h.pendingOps.Load(); after != before {
		t.Fatalf("pending ops %d after the rollback, was %d", after, before)
	}
}

// heldCmd is a command heldApplySM parks on, applies, and counts.
var heldCmd = []byte("held-command")

// heldApplySM is poisonApplySM with one more command: heldCmd is applied
// normally, but its apply can be parked first, so a test can hold an entry
// that is committed and not yet applied.
type heldApplySM struct {
	*poisonApplySM
	// hold, when non-nil, parks the apply of heldCmd until it is closed;
	// held is closed the moment it parks.
	hold chan struct{}
	held chan struct{}
	// heldApplies counts the applies of heldCmd on this instance.
	heldApplies atomic.Int64
}

func (s *heldApplySM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		if !bytes.Equal(entries[i].Cmd, heldCmd) {
			continue
		}
		if s.hold != nil {
			close(s.held)
			<-s.hold
		}
		s.heldApplies.Add(1)
	}
	return s.poisonApplySM.Update(ctx, entries, results)
}

// TestProposalDrainedByTheHaltReportsMaybeApplied: a proposal still pending
// when the shard halts may already be committed — here its entry is committed
// and mid-apply when the latch trips — and a reload replays and applies it.
// Its caller must therefore be told the outcome is unknown: the error still
// matches ErrShardFailed, and MaybeApplied reports true, so an unsessioned
// caller does not retry a mutation that is going to take effect.
func TestProposalDrainedByTheHaltReportsMaybeApplied(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	first := &heldApplySM{poisonApplySM: &poisonApplySM{}, hold: make(chan struct{}), held: make(chan struct{})}
	release := sync.OnceFunc(func() { close(first.hold) })
	t.Cleanup(release)
	reloaded := &heldApplySM{poisonApplySM: &poisonApplySM{}}
	machines := []*heldApplySM{first, reloaded}
	var started atomic.Int64
	create := func(shardID, replicaID uint64) interface{} {
		return machines[started.Add(1)-1]
	}
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rs, err := h.Propose(ctx, 1, heldCmd)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	defer rs.Release()
	select {
	case <-first.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the entry never reached apply")
	}

	// The entry is committed and not yet applied when the shard halts.
	h.engine.FailShard(h.engine.GetNode(1), errors.New("halted with a committed entry pending"))
	select {
	case res := <-rs.ResultC():
		if !errors.Is(res.Err, ErrShardFailed) {
			t.Fatalf("drained proposal = %v, want it to match ErrShardFailed", res.Err)
		}
		if !MaybeApplied(res.Err) {
			t.Fatalf("drained proposal = %v reports MaybeApplied false: its entry is committed, "+
				"and a caller told it was not applied would retry a mutation a reload applies", res.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the halt never answered the pending proposal")
	}

	// Reload: the replay applies the entry the caller was told may apply.
	release()
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	if err := h.StartShard(members, false, create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard after the halt: %v", err)
	}
	waitForLeader(t, h, 1)
	deadline := time.Now().Add(5 * time.Second)
	for reloaded.heldApplies.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the reloaded shard never applied the committed entry")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// registerTestProposal registers a proposal on shard 1 of h exactly as the
// admission path does, deadline included, and returns its RequestState.
func registerTestProposal(t *testing.T, h *Host, deadline time.Time) *RequestState {
	t.Helper()
	ns := testLoadNode(h, 1)
	key := nextRequestKey()
	rs := newRequestState(ns.requestPool, key, deadline, false)
	if err := h.registerPendingProposal(ns, h.engine.GetNode(1), key, &pendingProposal{rs: rs, deadline: deadline}); err != nil {
		t.Fatalf("registerPendingProposal: %v", err)
	}
	return rs
}

// awaitProposalErr waits for rs to complete and returns its error.
func awaitProposalErr(t *testing.T, rs *RequestState) error {
	t.Helper()
	select {
	case res := <-rs.ResultC():
		return res.Err
	case <-time.After(5 * time.Second):
		t.Fatal("the proposal was never completed")
		return nil
	}
}

// TestProposalFailuresAfterHandOffReportMaybeApplied: every failure delivered
// to a proposal once it was handed to raft reports MaybeApplied, and still
// matches its own error and sentinel. A committed entry whose apply or commit
// failed is replayed by a reload; an entry the leader could not persist may
// already be on a follower quorum (Replicate is sent before the local
// persist); a proposal that lost its wait, or that its shard's halt or
// teardown drained, may have reached the log.
func TestProposalFailuresAfterHandOffReportMaybeApplied(t *testing.T) {
	for name, tc := range map[string]struct {
		fail  func(h *Host, rs *RequestState)
		match func(err error) bool
	}{
		"ApplyError": {
			fail: func(h *Host, rs *RequestState) {
				(&hostEngineCallback{h: h}).OnProposalFailed(1, []proto.Entry{{Key: rs.key}},
					&engine.ApplyError{ShardID: 1, ReplicaID: 1, Err: errPoisonedApply})
			},
			match: func(err error) bool {
				var ae *ApplyError
				return errors.As(err, &ae) && errors.Is(err, errPoisonedApply)
			},
		},
		"CommitError": {
			fail: func(h *Host, rs *RequestState) {
				(&hostEngineCallback{h: h}).OnProposalFailed(1, []proto.Entry{{Key: rs.key}},
					&engine.CommitError{ShardID: 1, ReplicaID: 1, Err: errPoisonedApply})
			},
			match: func(err error) bool {
				var ce *CommitError
				return errors.As(err, &ce)
			},
		},
		"SaveStateError": {
			fail: func(h *Host, rs *RequestState) {
				(&hostEngineCallback{h: h}).OnProposalFailed(1, []proto.Entry{{Key: rs.key}},
					&engine.SaveStateError{ShardID: 1, ReplicaID: 1, Err: errPoisonedApply})
			},
			match: func(err error) bool {
				var se *SaveStateError
				return errors.As(err, &se)
			},
		},
		"deadline sweep": {
			fail: func(h *Host, _ *RequestState) {
				h.sweepPendingProposals(testLoadNode(h, 1), time.Now().Add(time.Hour))
			},
			match: func(err error) bool { return errors.Is(err, ErrTimeout) },
		},
		"teardown": {
			fail: func(h *Host, _ *RequestState) {
				h.failPendingRequests(testLoadNode(h, 1), ErrAborted)
			},
			match: func(err error) bool { return errors.Is(err, ErrAborted) },
		},
		"leadership change": {
			fail: func(h *Host, _ *RequestState) {
				h.failPendingClientRequests(testLoadNode(h, 1), &NotLeaderError{ShardID: 1, LeaderID: 2})
			},
			match: func(err error) bool { return errors.Is(err, ErrNotLeader) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			h, _, _ := newLatchableHost(t)
			rs := registerTestProposal(t, h, time.Now().Add(time.Minute))
			defer rs.Release()
			tc.fail(h, rs)
			err := awaitProposalErr(t, rs)
			if !tc.match(err) {
				t.Fatalf("failure = %v (%T): it no longer matches its own error", err, err)
			}
			if !MaybeApplied(err) {
				t.Fatalf("failure = %v reports MaybeApplied false, but the proposal had been "+
					"handed to raft and may still commit", err)
			}
			var m *MaybeAppliedError
			if errors.As(err, &m) && errors.As(m.Err, new(*MaybeAppliedError)) {
				t.Fatalf("failure = %v is wrapped as maybe-applied twice", err)
			}
		})
	}
}

// TestProposalRejectedBeforeTheLogIsDefinite: a proposal raft refused before
// appending it never reached the log, so the refusal is definite — MaybeApplied
// is false and a retry is safe.
func TestProposalRejectedBeforeTheLogIsDefinite(t *testing.T) {
	h, _, _ := newLatchableHost(t)
	rs := registerTestProposal(t, h, time.Now().Add(time.Minute))
	defer rs.Release()
	(&hostEngineCallback{h: h}).OnProposalRejected(1, []proto.Entry{{Key: rs.key}},
		&raft.NotLeaderError{LeaderID: 2})
	err := awaitProposalErr(t, rs)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("rejection = %v, want it to match ErrNotLeader", err)
	}
	if MaybeApplied(err) {
		t.Fatalf("rejection = %v reports MaybeApplied: raft refused the entry before "+
			"appending it, so nothing can commit", err)
	}
}

// TestProposalFailedByItsApplyReportsMaybeApplied is the end-to-end form: a
// proposal whose committed entry failed to apply — returned error or panic —
// reports MaybeApplied, since a reload replays the committed entry.
func TestProposalFailedByItsApplyReportsMaybeApplied(t *testing.T) {
	for name, panics := range map[string]bool{"returned error": false, "panic": true} {
		t.Run(name, func(t *testing.T) {
			h, machine, _ := newLatchableHost(t)
			machine.panicOnPoison = panics
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := h.SyncPropose(ctx, 1, poisonCmd)
			if err == nil {
				t.Fatal("the poisoned proposal succeeded")
			}
			if !MaybeApplied(err) {
				t.Fatalf("apply failure = %v reports MaybeApplied false: the entry is committed "+
					"and a reload replays it", err)
			}
			if panics && !errors.Is(err, ErrShardFailed) {
				t.Fatalf("apply panic = %v no longer matches ErrShardFailed", err)
			}
			var smPanic *SMPanicError
			if panics && (!errors.As(err, &smPanic) || smPanic.Value != errPoisonedApply) {
				t.Fatalf("apply panic = %v (%T): errors.As does not reach the *SMPanicError "+
					"carrying the panic value", err, err)
			}
			if !panics && !errors.Is(err, errPoisonedApply) {
				t.Fatalf("apply error = %v no longer carries the state machine's error", err)
			}
		})
	}
}

// TestProposalWhoseWaitEndsReportsMaybeApplied: a caller whose own wait for a
// proposal ends — its context's deadline, its cancellation, or the request's
// own deadline in RequestState.Result — after the proposal was handed to raft
// is told the outcome is unknown, since the entry may still commit.
func TestProposalWhoseWaitEndsReportsMaybeApplied(t *testing.T) {
	t.Run("SyncPropose deadline", func(t *testing.T) {
		h, machine, _ := newLatchableHost(t)
		parkPoison(t, machine)
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, err := h.SyncPropose(ctx, 1, poisonCmd)
		if !errors.Is(err, ErrTimeout) || !MaybeApplied(err) {
			t.Fatalf("SyncPropose whose deadline passed with its entry committed = %v, "+
				"want ErrTimeout reporting MaybeApplied", err)
		}
	})
	t.Run("SyncPropose canceled", func(t *testing.T) {
		h, machine, _ := newLatchableHost(t)
		parkPoison(t, machine)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-machine.poisonHeld
			cancel()
		}()
		_, err := h.SyncPropose(ctx, 1, poisonCmd)
		if !errors.Is(err, ErrCanceled) || !MaybeApplied(err) {
			t.Fatalf("SyncPropose canceled with its entry committed = %v, "+
				"want ErrCanceled reporting MaybeApplied", err)
		}
	})
	t.Run("RequestState.Result deadline", func(t *testing.T) {
		h, _, _ := newLatchableHost(t)
		rs := registerTestProposal(t, h, time.Now().Add(50*time.Millisecond))
		defer rs.Release()
		_, err := rs.Result()
		if !errors.Is(err, ErrTimeout) || !MaybeApplied(err) {
			t.Fatalf("Result of a registered proposal past its deadline = %v, "+
				"want ErrTimeout reporting MaybeApplied", err)
		}
	})
}

// TestProposalWaitOnAClosedQueueIsDefinite: the caller's own wait is
// classified by the same rule as the drains. A proposal provably never taken
// from its node's queue, which is now closed, never reached raft, so a wait
// that ends on it is a definite failure; while the queue is open the step
// worker could still take it, so the failure stays maybe-applied.
func TestProposalWaitOnAClosedQueueIsDefinite(t *testing.T) {
	h, _, _ := newLatchableHost(t)
	open := registerTestProposal(t, h, time.Now().Add(50*time.Millisecond))
	defer open.Release()
	if _, err := open.Result(); !errors.Is(err, ErrTimeout) || !MaybeApplied(err) {
		t.Fatalf("wait on a queued proposal with the queue open = %v, want maybe-applied ErrTimeout", err)
	}

	closed := registerTestProposal(t, h, time.Now().Add(50*time.Millisecond))
	defer closed.Release()
	// The node stops — its queue closes — without the host draining.
	h.engine.GetNode(1).Stop()
	_, err := closed.Result()
	if !errors.Is(err, ErrTimeout) || MaybeApplied(err) {
		t.Fatalf("wait on a proposal never taken from a closed queue = %v, want a definite ErrTimeout", err)
	}
}

// exitingReadMetrics is a metrics collector whose read-latency observation
// exits abnormally.
type exitingReadMetrics struct {
	NoOpMetricsCollector
	exit func()
}

func (m exitingReadMetrics) ObserveReadIndexLatency(uint64, time.Duration) { m.exit() }

// TestOnReadyToReadNeverHoldsItsLockAcrossUserCode: the host's OnReadyToRead
// completes reads under pendingReadsMu. It used to call the metrics collector
// with the lock held and release it explicitly, so a collector that panicked
// or called runtime.Goexit left the lock held, and the step worker's recovery
// — failing the same reads through OnReadIndexFailed — deadlocked on it. The
// read is completed, and the lock released, whatever the collector does.
func TestOnReadyToReadNeverHoldsItsLockAcrossUserCode(t *testing.T) {
	for exitName, exit := range map[string]func(){
		"panic":  func() { panic("metrics panicked") },
		"Goexit": runtime.Goexit,
	} {
		t.Run(exitName, func(t *testing.T) {
			cfg := testHostConfig(t)
			h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption(),
				WithMetrics(exitingReadMetrics{exit: exit}))
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			t.Cleanup(func() { h.Close() })
			if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
				func(uint64, uint64) interface{} { return &poisonApplySM{} }, testShardConfig(1, 1)); err != nil {
				t.Fatalf("StartShard: %v", err)
			}
			waitForLeader(t, h, 1)

			ns := testLoadNode(h, 1)
			key := nextRequestKey()
			rs := newRequestState(ns.requestPool, key, time.Now().Add(time.Minute), true)
			defer rs.Release()
			h.addPendingOp()
			rs.acquireRef()
			ns.pendingReadsMu.Lock()
			ns.pendingReads[key] = &pendingRead{rs: rs}
			ns.pendingReadsMu.Unlock()

			cb := &hostEngineCallback{h: h}
			index := ns.lastApplied.Load()
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				cb.OnReadyToRead(1, []proto.ReadyToRead{{Key: key, Index: index}})
			}()
			<-done

			if !ns.pendingReadsMu.TryLock() {
				t.Fatal("pendingReadsMu is still held after the collector exited abnormally")
			}
			ns.pendingReadsMu.Unlock()
			select {
			case res := <-rs.ResultC():
				if res.Err != nil || res.Value != index {
					t.Fatalf("read completed with %+v, want value %d", res, index)
				}
			default:
				t.Fatal("the confirmed read was not completed")
			}
			failed := make(chan struct{})
			go func() {
				cb.OnReadIndexFailed(1, []uint64{key}, errors.New("recovery"))
				close(failed)
			}()
			select {
			case <-failed:
			case <-time.After(5 * time.Second):
				t.Fatal("OnReadIndexFailed deadlocked on pendingReadsMu")
			}
		})
	}
}

// exitingProposalMetrics is a metrics collector whose proposal-latency
// observation exits abnormally.
type exitingProposalMetrics struct {
	NoOpMetricsCollector
	exit func()
}

func (m exitingProposalMetrics) ObserveProposalLatency(uint64, time.Duration) { m.exit() }

// TestAppliedProposalsAreAnsweredWhateverTheCollectorDoes: OnApplied claims
// an applied proposal out of pendingProposals and then completes it. The
// proposal latency used to be observed between the two, so a collector that
// called runtime.Goexit left a claimed proposal that no path would ever
// complete — the apply worker's recovery fails only proposals still pending,
// and the membership branch's own recovery recognised only a panic. Every
// claimed caller is now answered before any latency is observed, and a
// Goexit in the membership branch completes its unanswered tail too.
func TestAppliedProposalsAreAnsweredWhateverTheCollectorDoes(t *testing.T) {
	cc := proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 10, Address: "127.0.0.1:6000"}
	ccBuf := make([]byte, cc.Size())
	n, err := cc.MarshalTo(ccBuf)
	if err != nil {
		t.Fatalf("ConfigChange.MarshalTo: %v", err)
	}
	const keyA, keyB uint64 = 1, 2
	batches := map[string][]proto.Entry{
		"ordinary": {
			{Index: 5, Term: 1, Key: keyA, Type: proto.EntryNormal},
			{Index: 6, Term: 1, Key: keyB, Type: proto.EntryNormal},
		},
		"membership": {
			{Index: 5, Term: 1, Key: keyA, Type: proto.EntryNormal},
			{Index: 6, Term: 1, Key: keyB, Type: proto.EntryConfigChange, Cmd: ccBuf[:n]},
		},
	}
	for batchName, entries := range batches {
		for exitName, exit := range map[string]func(){
			"panic":  func() { panic("metrics panicked") },
			"Goexit": runtime.Goexit,
		} {
			t.Run(batchName+"/"+exitName, func(t *testing.T) {
				h := newTestHost(t)
				// A raw, unwrapped collector: the Goexit is what no wrapper
				// can contain, and the panic exercises the same ordering.
				h.metrics = exitingProposalMetrics{exit: exit}
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

				register := func(key uint64) *RequestState {
					rs := newRequestState(ns.requestPool, key, time.Now().Add(time.Hour), true)
					h.addPendingOp()
					rs.acquireRef()
					ns.pendingProposals.Store(key, &pendingProposal{rs: rs})
					return rs
				}
				rsA, rsB := register(keyA), register(keyB)

				cb := &hostEngineCallback{h: h}
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer func() { _ = recover() }()
					cb.OnApplied(1, entries, []sm.Result{{Value: 7}, {Value: 8}})
				}()
				<-done

				// A was claimed and completed with its applied result before
				// the collector ran.
				select {
				case res := <-rsA.ResultC():
					if res.Err != nil || res.Value != 7 {
						t.Fatalf("proposal A completed with %+v, want value 7", res)
					}
				default:
					t.Fatal("proposal A was claimed but never completed")
				}
				if outstanding := rsA.outstanding.Load(); outstanding != 0 {
					t.Fatalf("rsA.outstanding = %d, want exactly 0", outstanding)
				}
				// B is still pending on the ordinary path — the apply
				// worker's recovery fails it — or, claimed on the membership
				// path, was answered by that branch's recovery.
				if _, pending := ns.pendingProposals.Load(keyB); pending {
					return
				}
				select {
				case res := <-rsB.ResultC():
					if res.Err == nil {
						t.Fatalf("proposal B completed with %+v; the branch exited before "+
							"answering it, so its recovery must report an error", res)
					}
				default:
					t.Fatal("proposal B was claimed but never completed")
				}
			})
		}
	}
}

// exitingLogHandler is a default slog handler that exits abnormally on every
// record.
type exitingLogHandler struct{ exit func() }

func (h exitingLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h exitingLogHandler) Handle(context.Context, slog.Record) error {
	h.exit()
	return nil
}
func (h exitingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h exitingLogHandler) WithGroup(string) slog.Handler      { return h }

// TestShardHaltFailsPendingRequestsBeforeLogging: OnShardFailed logged the
// halt before failing the shard's pending requests, so a default slog
// handler that panicked or called runtime.Goexit stranded every one of them.
// The requests are failed first; the diagnostic follows, contained.
func TestShardHaltFailsPendingRequestsBeforeLogging(t *testing.T) {
	for exitName, exit := range map[string]func(){
		"panic":  func() { panic("slog handler panicked") },
		"Goexit": runtime.Goexit,
	} {
		t.Run(exitName, func(t *testing.T) {
			h := newTestHost(t)
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
			key := nextRequestKey()
			rs := newRequestState(ns.requestPool, key, time.Now().Add(time.Hour), true)
			h.addPendingOp()
			rs.acquireRef()
			ns.pendingReadsMu.Lock()
			ns.pendingReads[key] = &pendingRead{rs: rs}
			ns.pendingReadsMu.Unlock()

			prev := slog.Default()
			slog.SetDefault(slog.New(exitingLogHandler{exit: exit}))
			t.Cleanup(func() { slog.SetDefault(prev) })

			cb := &hostEngineCallback{h: h}
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				cb.OnShardFailed(1, errors.New("halted"))
			}()
			<-done
			select {
			case res := <-rs.ResultC():
				if !errors.Is(res.Err, ErrShardFailed) {
					t.Fatalf("pending read failed with %v, want ErrShardFailed", res.Err)
				}
			default:
				t.Fatal("the pending read was stranded by the halt's log line")
			}
		})
	}
}

// asPanicsError is a user error whose As method panics when asked for a
// *MaybeAppliedError, standing in for a buggy state machine error type.
type asPanicsError struct{}

func (asPanicsError) Error() string { return "quicraft_test: As panics" }

func (asPanicsError) As(target any) bool {
	if _, ok := target.(**MaybeAppliedError); ok {
		panic("quicraft_test: As panicked")
	}
	return false
}

// TestProposalFailedWithAPanickingErrorIsStillCompleted: classifying a
// failure must not run user code once the proposal has been claimed. A user
// error whose As panics would otherwise strand the claimed proposal: never
// completed, its reference never released, and pendingOps never decremented,
// so every later Drain waits out its full timeout.
func TestProposalFailedWithAPanickingErrorIsStillCompleted(t *testing.T) {
	h, _, _ := newLatchableHost(t)
	rs := registerTestProposal(t, h, time.Now().Add(time.Minute))
	defer rs.Release()

	func() {
		defer func() { _ = recover() }()
		(&hostEngineCallback{h: h}).OnProposalFailed(1, []proto.Entry{{Key: rs.key}},
			&engine.ApplyError{ShardID: 1, ReplicaID: 1, Err: asPanicsError{}})
	}()

	select {
	case res := <-rs.ResultC():
		var ae *ApplyError
		if res.Err == nil || (!errors.As(res.Err, &ae) && res.Err.Error() == "") {
			t.Fatalf("proposal completed with %v, want its ApplyError", res.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("the claimed proposal was never completed: classifying its error panicked " +
			"after the claim")
	}
	if got := rs.outstanding.Load(); got != 0 {
		t.Fatalf("the proposal holds %d pending-map references, want 0", got)
	}
	if got := h.pendingOps.Load(); got != 0 {
		t.Fatalf("pendingOps = %d after the proposal completed, want 0: Drain would wait it out", got)
	}
}

// TestLeaderChangeLeavesUnhandedProposalsToTheStepWorker: a leadership change
// fails only the proposals the step worker has already taken from the queue
// (handed to raft, so maybe-applied). One registered after the leaderless
// drain and before the new leader is known has not reached raft; the drain
// leaves it pending, and the step worker resolves it — forwarded to the new
// leader, or refused definitely through OnProposalRejected — so it never gets
// a maybe-applied result it does not deserve.
func TestLeaderChangeLeavesUnhandedProposalsToTheStepWorker(t *testing.T) {
	h, _, _ := newLatchableHost(t)
	cb := &hostEngineCallback{h: h}
	ns := testLoadNode(h, 1)
	term := ns.term.Load()

	// The leader is lost: the leaderless drain runs.
	cb.OnLeaderUpdated(1, 0, term)
	// A proposal registers in the leaderless window.
	rs := registerTestProposal(t, h, time.Now().Add(time.Minute))
	defer rs.Release()
	// The new leader is observed: the leader-change drain runs again.
	cb.OnLeaderUpdated(1, 2, term+1)

	select {
	case res := <-rs.ResultC():
		t.Fatalf("the leader-change drain completed a proposal raft never received, with %v", res.Err)
	default:
	}
	if n := pendingProposalCount(ns); n != 1 {
		t.Fatalf("%d proposals pending after the drain, want the unhanded one", n)
	}

	// The step worker refuses it (no leader it can forward to): definite.
	cb.OnProposalRejected(1, []proto.Entry{{Key: rs.key}}, &raft.NotLeaderError{LeaderID: 0})
	err := awaitProposalErr(t, rs)
	if !errors.Is(err, ErrNotLeader) || MaybeApplied(err) {
		t.Fatalf("refused proposal = %v, want a definite NotLeader refusal", err)
	}
}

// TestLeaderChangeFailsHandedOffProposalsAsMaybeApplied: a proposal the step
// worker has taken from the queue reached raft, so a leadership change fails
// it maybe-applied.
func TestLeaderChangeFailsHandedOffProposalsAsMaybeApplied(t *testing.T) {
	h, machine, _ := newLatchableHost(t)
	parkPoison(t, machine)
	trip, cancelTrip := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelTrip()
	go func() { _, _ = h.SyncPropose(trip, 1, poisonCmd) }()
	select {
	case <-machine.poisonHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("the poison apply never parked")
	}

	rs, err := h.Propose(trip, 1, []byte("handed-off"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	defer rs.Release()
	ns := testLoadNode(h, 1)
	v, ok := ns.pendingProposals.Load(rs.key)
	if !ok {
		t.Fatal("fixture: the proposal is not pending")
	}
	pp := v.(*pendingProposal)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if taken, _ := pp.node.ProposalTaken(&pp.enqueuedAt); taken {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture: the step worker never took the proposal from the queue")
		}
		time.Sleep(time.Millisecond)
	}

	(&hostEngineCallback{h: h}).OnLeaderUpdated(1, 0, ns.term.Load())
	err = awaitProposalErr(t, rs)
	if !errors.Is(err, ErrNotLeader) || !MaybeApplied(err) {
		t.Fatalf("handed-off proposal at a leadership change = %v, want NotLeader reporting MaybeApplied", err)
	}
}

// TestHaltedProposalNeverQueuedIsDefinite: a proposal still outside the step
// worker's reach when the shard halts — registered, but never taken from the
// queue, which the halt closes — can never reach raft, so its failure is
// definite. The halt fences the queue before it drains.
func TestHaltedProposalNeverQueuedIsDefinite(t *testing.T) {
	h, _, failed := newLatchableHost(t)
	rs := registerTestProposal(t, h, time.Now().Add(time.Minute))
	defer rs.Release()
	h.engine.FailShard(h.engine.GetNode(1), errors.New("halted"))
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnShardFailed never fired")
	}
	err := awaitProposalErr(t, rs)
	if !errors.Is(err, ErrShardFailed) || MaybeApplied(err) {
		t.Fatalf("never-queued proposal on a halted shard = %v, want a definite ErrShardFailed", err)
	}
}

// TestStoppedProposalNeverQueuedIsDefinite: the same for teardown — StopShard
// stops the node, which closes its proposal queue before pending requests are
// failed, so a proposal that was never taken from it gets a definite result.
func TestStoppedProposalNeverQueuedIsDefinite(t *testing.T) {
	h, _, _ := newLatchableHost(t)
	rs := registerTestProposal(t, h, time.Now().Add(time.Minute))
	defer rs.Release()
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	err := awaitProposalErr(t, rs)
	if !errors.Is(err, ErrAborted) || MaybeApplied(err) {
		t.Fatalf("never-queued proposal on a stopped shard = %v, want a definite ErrAborted", err)
	}
}
