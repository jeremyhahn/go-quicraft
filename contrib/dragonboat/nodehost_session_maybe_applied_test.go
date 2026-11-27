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

package dragonboat

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// applyGateMarker is the command whose Update blocks on the gate.
var applyGateMarker = []byte("apply-gate-marker")

// blockingApplySM blocks Update on the gate for applyGateMarker, so every
// later entry of the shard waits behind it.
type blockingApplySM struct {
	blockingLookupSM
}

func (sm *blockingApplySM) Update(entry dbSM.Entry) (dbSM.Result, error) {
	if bytes.Equal(entry.Cmd, applyGateMarker) {
		sm.gate.wait()
	}
	return sm.blockingLookupSM.Update(entry)
}

// startApplyBlockedShard starts shard 1 with a gated state machine, opens
// n tracked sessions and then stalls the apply pipeline: a no-op proposal
// of applyGateMarker is blocked inside Update until the gate opens. The
// gate opens at cleanup before the host closes.
func startApplyBlockedShard(t *testing.T, n int) (*NodeHost, *lookupGate, []*dbClient.Session) {
	t.Helper()
	nh := newTestNodeHost(t)
	gate := newLookupGate()
	t.Cleanup(gate.open)
	startTestShardWith(t, nh, 1, 1, func(_, _ uint64) dbSM.IStateMachine {
		return &blockingApplySM{blockingLookupSM{gate: gate}}
	})

	sessions := make([]*dbClient.Session, 0, n)
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cs, err := nh.SyncGetSession(ctx, 1)
		cancel()
		if err != nil {
			t.Fatalf("SyncGetSession: %v", err)
		}
		sessions = append(sessions, cs)
	}

	gate.block()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = nh.SyncPropose(ctx, nh.GetNoOPSession(1), applyGateMarker)
	}()
	t.Cleanup(func() {
		gate.open()
		<-done
	})
	waitEntered(t, gate)
	return nh, gate, sessions
}

// lostWaitCases runs call once with a deadline and once with a
// cancellation issued after the call is queued, and asserts each lost
// wait is reported as maybe-applied with the matching sentinel.
func lostWaitCases(t *testing.T, sessions []*dbClient.Session, call func(context.Context, *dbClient.Session) error) {
	t.Helper()
	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := call(ctx, sessions[0])
		assertLostWait(t, err, ErrTimeout)
	})
	t.Run("cancel", func(t *testing.T) {
		// The adapter refuses a context without a deadline, so the
		// deadline is far past the cancellation.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()
		err := call(ctx, sessions[1])
		assertLostWait(t, err, ErrCanceled)
	})
}

func assertLostWait(t *testing.T, err, want error) {
	t.Helper()
	if err == nil {
		t.Fatal("err = nil, want a lost-wait error")
	}
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want errors.Is(err, %v)", err, want)
	}
	if !quicraft.MaybeApplied(err) {
		t.Fatalf("quicraft.MaybeApplied(%v) = false, want true: the request was already submitted", err)
	}
}

func TestNodeHost_SyncPropose_TrackedSession_LostWaitIsMaybeApplied(t *testing.T) {
	nh, gate, sessions := startApplyBlockedShard(t, 2)
	lostWaitCases(t, sessions, func(ctx context.Context, cs *dbClient.Session) error {
		_, err := nh.SyncPropose(ctx, cs, []byte("tracked"))
		return err
	})
	gate.open()
}

func TestNodeHost_SyncCloseSession_LostWaitIsMaybeApplied(t *testing.T) {
	nh, gate, sessions := startApplyBlockedShard(t, 2)
	lostWaitCases(t, sessions, func(ctx context.Context, cs *dbClient.Session) error {
		return nh.SyncCloseSession(ctx, cs)
	})
	gate.open()
}
