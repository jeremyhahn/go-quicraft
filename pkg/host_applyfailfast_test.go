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
}

func (p *poisonApplySM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		if bytes.Equal(entries[i].Cmd, poisonCmd) {
			p.poisonApplies.Add(1)
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

			h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
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

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
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
