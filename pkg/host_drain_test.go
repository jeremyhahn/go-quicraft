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
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// TestHost_Drain_InFlightSyncProposeThenClose drives Drain while 16
// goroutines issue SyncPropose on an in-process single-replica shard. A call
// that starts before Drain returns ends in success or ErrDraining (the drain
// flag went up while it was being admitted); every call that starts after
// Drain returns is refused with ErrDraining. A second Drain is safe, and
// Close then returns within 5s and leaves no goroutine behind.
func TestHost_Drain_InFlightSyncProposeThenClose(t *testing.T) {
	const (
		workers         = 16
		minPreDrainOK   = 4 * workers
		closeDeadline   = 5 * time.Second
		perCallDeadline = 5 * time.Second
	)

	baseline := runtime.NumGoroutine()

	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	var (
		drained    atomic.Bool  // set once Drain has returned
		preOK      atomic.Int64 // successes from calls started before Drain returned
		preRefused atomic.Int64 // ErrDraining from calls started before Drain returned
		wg         sync.WaitGroup
		mu         sync.Mutex
		failures   []string
	)
	fail := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		failures = append(failures, fmt.Sprintf(format, args...))
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				afterDrain := drained.Load()
				ctx, cancel := context.WithTimeout(context.Background(), perCallDeadline)
				_, err := h.SyncPropose(ctx, 1, []byte("drain-813"))
				cancel()
				if afterDrain {
					if !errors.Is(err, ErrDraining) {
						fail("worker %d: SyncPropose after Drain = %v, want ErrDraining", w, err)
					}
					return
				}
				switch {
				case err == nil:
					preOK.Add(1)
				case errors.Is(err, ErrDraining):
					preRefused.Add(1)
				default:
					fail("worker %d: SyncPropose before Drain returned = %v, want success or ErrDraining", w, err)
					return
				}
			}
		}(w)
	}

	// Drain only once every worker is in its loop and proposals commit.
	deadline := time.Now().Add(5 * time.Second)
	for preOK.Load() < minPreDrainOK {
		if time.Now().After(deadline) {
			drained.Store(true)
			_ = h.Drain()
			wg.Wait()
			t.Fatalf("only %d proposals committed before Drain, want %d", preOK.Load(), minPreDrainOK)
		}
		runtime.Gosched()
	}

	if err := h.Drain(); err != nil {
		t.Fatalf("Drain = %v, want nil: every in-flight proposal should finish within ShutdownTimeout", err)
	}
	drained.Store(true)

	if !h.IsDraining() {
		t.Fatal("IsDraining = false after Drain")
	}
	if n := h.pendingOps.Load(); n > 0 {
		t.Errorf("pendingOps = %d right after Drain returned nil, want 0", n)
	}

	workersDone := make(chan struct{})
	go func() { wg.Wait(); close(workersDone) }()
	select {
	case <-workersDone:
	case <-time.After(perCallDeadline + time.Second):
		t.Fatal("workers did not finish after Drain")
	}
	mu.Lock()
	for _, f := range failures {
		t.Error(f)
	}
	mu.Unlock()

	// After Drain, a fresh SyncPropose is refused too.
	if _, err := h.SyncPropose(context.Background(), 1, []byte("late")); !errors.Is(err, ErrDraining) {
		t.Errorf("SyncPropose after Drain = %v, want ErrDraining", err)
	}

	// A second Drain is safe: it returns the first one's result at once.
	secondDone := make(chan error, 1)
	go func() { secondDone <- h.Drain() }()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Errorf("second Drain = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second Drain did not return")
	}
	if !h.IsDraining() {
		t.Error("IsDraining = false after the second Drain")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- h.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Errorf("Close after Drain = %v, want nil", err)
		}
	case <-time.After(closeDeadline):
		t.Fatalf("Close did not return within %v", closeDeadline)
	}

	// Every goroutine the host and the workers started must exit.
	leakDeadline := time.Now().Add(closeDeadline)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(leakDeadline) {
			buf := make([]byte, 64<<10)
			buf = buf[:runtime.Stack(buf, true)]
			t.Fatalf("goroutines after Close = %d, baseline %d:\n%s",
				runtime.NumGoroutine(), baseline, buf)
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Logf("before Drain returned: %d committed, %d refused with ErrDraining",
		preOK.Load(), preRefused.Load())
}

// drainHoldCmd is the proposal payload holdingStateMachine holds in Update.
var drainHoldCmd = []byte("drain-813-hold")

// holdingStateMachine holds an admitted proposal inside Update: when it meets
// drainHoldCmd it closes entered and blocks until release is closed, then
// sets applied before it returns.
type holdingStateMachine struct {
	mockStateMachine
	entered     chan struct{}
	release     chan struct{}
	applied     atomic.Bool
	enteredOnce sync.Once
}

func newHoldingStateMachine() *holdingStateMachine {
	return &holdingStateMachine{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (m *holdingStateMachine) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	held := false
	for i := range entries {
		if bytes.Equal(entries[i].Cmd, drainHoldCmd) {
			held = true
			m.enteredOnce.Do(func() { close(m.entered) })
			select {
			case <-m.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	if err := m.mockStateMachine.Update(ctx, entries, results); err != nil {
		return err
	}
	if held {
		m.applied.Store(true)
	}
	return nil
}

// TestHost_Drain_WaitsForHeldProposal holds one admitted SyncPropose inside
// the state machine's Update, so pendingOps is non-zero when Drain starts.
// Drain must stay blocked while the proposal is held and must return only
// after the released proposal has been applied.
func TestHost_Drain_WaitsForHeldProposal(t *testing.T) {
	const (
		deadline     = 5 * time.Second
		blockedProbe = 200 * time.Millisecond
	)

	hsm := newHoldingStateMachine()
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	// Cleanups run last-registered first: release a still-held Update
	// before the host is closed.
	var releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(hsm.release) }) }
	t.Cleanup(releaseHeld)

	members := map[uint64]string{1: "127.0.0.1:5000"}
	create := func(shardID, replicaID uint64) interface{} { return hsm }
	if err := h.StartShard(members, false, create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	proposeDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*deadline)
		defer cancel()
		_, err := h.SyncPropose(ctx, 1, drainHoldCmd)
		proposeDone <- err
	}()

	select {
	case <-hsm.entered:
	case err := <-proposeDone:
		t.Fatalf("SyncPropose returned %v before Update held it", err)
	case <-time.After(deadline):
		t.Fatal("the held proposal never reached Update")
	}
	if n := h.pendingOps.Load(); n <= 0 {
		t.Fatalf("pendingOps = %d while a proposal is held in Update, want > 0", n)
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- h.Drain() }()

	select {
	case err := <-drainDone:
		t.Fatalf("Drain returned %v while an admitted proposal was held", err)
	case <-time.After(blockedProbe):
	}
	if !h.IsDraining() {
		t.Fatal("IsDraining = false while Drain is waiting")
	}
	if _, err := h.SyncPropose(context.Background(), 1, []byte("late")); !errors.Is(err, ErrDraining) {
		t.Errorf("SyncPropose during Drain = %v, want ErrDraining", err)
	}
	if hsm.applied.Load() {
		t.Fatal("held proposal applied before it was released")
	}

	releaseHeld()

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain = %v, want nil", err)
		}
	case <-time.After(deadline):
		t.Fatal("Drain did not return after the held proposal was released")
	}
	if !hsm.applied.Load() {
		t.Error("Drain returned before the held proposal was applied")
	}
	select {
	case err := <-proposeDone:
		if err != nil {
			t.Errorf("held SyncPropose = %v, want success", err)
		}
	case <-time.After(deadline):
		t.Fatal("held SyncPropose did not return after Drain")
	}
}
