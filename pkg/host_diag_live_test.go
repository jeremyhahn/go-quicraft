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
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// diagLiveWait bounds every poll in this file.
const diagLiveWait = 10 * time.Second

// holdableSM is a state machine whose Update and SaveSnapshot can each be held
// on a channel. A hold is armed by setting the flag; the held call signals on
// entered and blocks until release is closed.
type holdableSM struct {
	mockStateMachine

	holdUpdate   atomic.Bool
	updateIn     chan struct{}
	updateGo     chan struct{}
	updateOnce   sync.Once
	holdSave     atomic.Bool
	saveIn       chan struct{}
	saveGo       chan struct{}
	saveOnce     sync.Once
	releaseCalls sync.Once
}

func newHoldableSM() *holdableSM {
	return &holdableSM{
		updateIn: make(chan struct{}),
		updateGo: make(chan struct{}),
		saveIn:   make(chan struct{}),
		saveGo:   make(chan struct{}),
	}
}

func (m *holdableSM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	if m.holdUpdate.Load() {
		m.updateOnce.Do(func() { close(m.updateIn) })
		<-m.updateGo
	}
	return m.mockStateMachine.Update(ctx, entries, results)
}

func (m *holdableSM) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	if m.holdSave.Load() {
		m.saveOnce.Do(func() { close(m.saveIn) })
		<-m.saveGo
	}
	return m.mockStateMachine.SaveSnapshot(ctx, w, stopper)
}

// release unblocks every held call; safe to call more than once.
func (m *holdableSM) release() {
	m.releaseCalls.Do(func() {
		close(m.updateGo)
		close(m.saveGo)
	})
}

// startDiagLiveShard starts a single-replica shard on a fresh test Host with a
// holdableSM, waits for it to lead, and returns both.
func startDiagLiveShard(t *testing.T, shardID uint64) (*Host, *holdableSM) {
	t.Helper()
	h := newTestHost(t)
	m := newHoldableSM()
	t.Cleanup(m.release) // never leave a held call behind Close
	create := func(_, _ uint64) interface{} { return m }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create, testShardConfig(shardID, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)
	waitForDiagLive(t, h, shardID)
	return h, m
}

// pollDiag calls probe until it reports true or diagLiveWait elapses, and
// returns the final verdict.
func pollDiag(ctx context.Context, t *testing.T, probe func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(diagLiveWait)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return false
		}
		if probe() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return probe()
}

func TestDiagLive_IdleShardReportsNoGatesAndNoPendingProposals(t *testing.T) {
	const shardID = uint64(1)
	h, _ := startDiagLiveShard(t, shardID)

	ctx, cancel := context.WithTimeout(context.Background(), diagLiveWait)
	defer cancel()
	if _, err := h.SyncPropose(ctx, shardID, []byte("idle")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	var gates ShardGatesDiag
	var pipe PipelineDiag
	ok := pollDiag(ctx, t, func() bool {
		var err error
		if gates, err = h.DiagShardGates(shardID); err != nil {
			t.Fatalf("DiagShardGates: %v", err)
		}
		if pipe, err = h.DiagPipelineState(shardID); err != nil {
			t.Fatalf("DiagPipelineState: %v", err)
		}
		return !gates.Saving && !gates.Recovering && gates.ApplyInFlight == 0 && pipe.PendingProposals == 0
	})
	if !ok {
		t.Fatalf("idle shard: gates=%+v pipeline=%+v, want Saving/Recovering false, ApplyInFlight 0, PendingProposals 0", gates, pipe)
	}
}

func TestDiagLive_SavingGateFollowsHeldSaveSnapshot(t *testing.T) {
	const shardID = uint64(1)
	h, m := startDiagLiveShard(t, shardID)

	ctx, cancel := context.WithTimeout(context.Background(), 3*diagLiveWait)
	defer cancel()
	if _, err := h.SyncPropose(ctx, shardID, []byte("before-snapshot")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	m.holdSave.Store(true)
	snapDone := make(chan error, 1)
	go func() {
		_, err := h.SyncRequestSnapshot(ctx, shardID, SnapshotOption{})
		snapDone <- err
	}()
	select {
	case <-m.saveIn:
	case err := <-snapDone:
		t.Fatalf("SyncRequestSnapshot returned %v before SaveSnapshot was entered", err)
	case <-time.After(diagLiveWait):
		t.Fatal("SaveSnapshot was never entered")
	}

	var gates ShardGatesDiag
	if !pollDiag(ctx, t, func() bool {
		var err error
		if gates, err = h.DiagShardGates(shardID); err != nil {
			t.Fatalf("DiagShardGates: %v", err)
		}
		return gates.Saving
	}) {
		t.Fatalf("with SaveSnapshot held: gates=%+v, want Saving true", gates)
	}
	if gates.Recovering {
		t.Errorf("with SaveSnapshot held: Recovering true, want false")
	}

	m.release()
	if err := <-snapDone; err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	if !pollDiag(ctx, t, func() bool {
		var err error
		if gates, err = h.DiagShardGates(shardID); err != nil {
			t.Fatalf("DiagShardGates: %v", err)
		}
		return !gates.Saving
	}) {
		t.Fatalf("after SaveSnapshot released: gates=%+v, want Saving false", gates)
	}
}

func TestDiagLive_PendingProposalsCountsHeldApplies(t *testing.T) {
	const (
		shardID = uint64(1)
		k       = 4
	)
	h, m := startDiagLiveShard(t, shardID)

	ctx, cancel := context.WithTimeout(context.Background(), 3*diagLiveWait)
	defer cancel()

	m.holdUpdate.Store(true)
	errs := make(chan error, k)
	for i := range k {
		go func() {
			_, err := h.SyncPropose(ctx, shardID, fmt.Appendf(nil, "held-%d", i))
			errs <- err
		}()
	}
	select {
	case <-m.updateIn:
	case <-time.After(diagLiveWait):
		t.Fatal("Update was never entered")
	}

	var pipe PipelineDiag
	if !pollDiag(ctx, t, func() bool {
		var err error
		if pipe, err = h.DiagPipelineState(shardID); err != nil {
			t.Fatalf("DiagPipelineState: %v", err)
		}
		return pipe.PendingProposals == k
	}) {
		t.Fatalf("with Update held: PendingProposals=%d, want %d (pipeline=%+v)", pipe.PendingProposals, k, pipe)
	}

	m.holdUpdate.Store(false)
	m.release()
	for range k {
		if err := <-errs; err != nil {
			t.Fatalf("SyncPropose: %v", err)
		}
	}
	if !pollDiag(ctx, t, func() bool {
		var err error
		if pipe, err = h.DiagPipelineState(shardID); err != nil {
			t.Fatalf("DiagPipelineState: %v", err)
		}
		return pipe.PendingProposals == 0
	}) {
		t.Fatalf("after Update released: PendingProposals=%d, want 0", pipe.PendingProposals)
	}
}

func TestDiagLive_RemotesTextOnSingleReplicaLeader(t *testing.T) {
	const shardID = uint64(1)
	h, _ := startDiagLiveShard(t, shardID)

	ctx, cancel := context.WithTimeout(context.Background(), diagLiveWait)
	defer cancel()

	var text string
	ok := pollDiag(ctx, t, func() bool {
		rd, err := h.DiagRemotes(shardID)
		if err != nil {
			t.Fatalf("DiagRemotes: %v", err)
		}
		text = rd.Text
		return text != "(diagnostics warming up)"
	})
	if !ok {
		t.Fatalf("DiagRemotes still warming up after %v", diagLiveWait)
	}
	// RemotesDiag.Text renders each remote the leader tracks; the leader does
	// not track itself, so a single-replica leader renders the empty string,
	// never "not leader".
	if text != "" {
		t.Fatalf("DiagRemotes.Text on a single-replica leader = %q, want \"\"", text)
	}
}

func TestDiagLive_ShardNotStartedReturnsErrShardNotReady(t *testing.T) {
	h, _ := startDiagLiveShard(t, 1)
	const absent = uint64(99)

	if _, err := h.DiagShardGates(absent); !errors.Is(err, ErrShardNotReady) {
		t.Errorf("DiagShardGates(absent) = %v, want ErrShardNotReady", err)
	}
	if _, err := h.DiagPipelineState(absent); !errors.Is(err, ErrShardNotReady) {
		t.Errorf("DiagPipelineState(absent) = %v, want ErrShardNotReady", err)
	}
	if _, err := h.DiagRemotes(absent); !errors.Is(err, ErrShardNotReady) {
		t.Errorf("DiagRemotes(absent) = %v, want ErrShardNotReady", err)
	}
}
