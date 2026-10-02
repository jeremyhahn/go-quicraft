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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
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
			"this is #185 — the reader paid the deadline to learn nothing", err)
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
			"did not fail it, so it waited for an apply that can never come (#185)",
			got.err, got.elapsed)
	}
	if !errors.Is(got.err, ErrShardFailed) {
		t.Fatalf("a read pending across the halt = %v (%T), want ErrShardFailed", got.err, got.err)
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
			"post-registration re-check is missing (#185)", timedOut, readers, elapsed)
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
			"no latch check in the poll loop, this is the hang #185's second review found")
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
