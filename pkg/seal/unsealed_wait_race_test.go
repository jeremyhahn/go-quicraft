package seal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// unsealedWaitBound bounds every wait on an UnsealedC channel in this file.
// It is a deadline on a context, not a sleep: a waiter that is woken returns
// at once, and only a waiter that is never woken runs into it.
const unsealedWaitBound = 10 * time.Second

// waitUnsealedC waits for c to close or ctx to end, and reports whether c
// closed. It does not touch t, so it is safe to call from any goroutine.
func waitUnsealedC(ctx context.Context, c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	case <-ctx.Done():
		return false
	}
}

// isClosedUnsealedC reports whether c is closed now, without blocking.
func isClosedUnsealedC(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// startUnsealedCWaiters starts n goroutines that each take UnsealedC once
// and wait on it until ctx ends. taken is done once every goroutine holds
// its channel; the returned function waits for all of them and returns how
// many were not woken.
func startUnsealedCWaiters(ctx context.Context, t *testing.T, b *Barrier, n int) (taken *sync.WaitGroup, wait func() int) {
	t.Helper()
	taken = &sync.WaitGroup{}
	var done sync.WaitGroup
	var missed atomic.Int64
	taken.Add(n)
	done.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer done.Done()
			c := b.UnsealedC()
			taken.Done()
			if !waitUnsealedC(ctx, c) {
				missed.Add(1)
			}
		}()
	}
	return taken, func() int {
		done.Wait()
		return int(missed.Load())
	}
}

// requireUnsealedNoWaiter fails t unless b is unsealed and a fresh UnsealedC
// channel is already closed, so nothing that waits on it now can block.
func requireUnsealedNoWaiter(ctx context.Context, t *testing.T, b *Barrier) {
	t.Helper()
	if ctx.Err() != nil {
		t.Fatalf("context ended before the final check: %v", ctx.Err())
	}
	if b.IsSealed() {
		t.Fatal("barrier is sealed at the end of the test")
	}
	if !isClosedUnsealedC(b.UnsealedC()) {
		t.Fatal("UnsealedC is open on an unsealed barrier")
	}
}

// (a) Every goroutine waiting on the sealed period's channel wakes on Unseal.
func TestBarrier_UnsealedCRace_UnsealWakesEveryWaiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), unsealedWaitBound)
	defer cancel()
	b, s := testInitializedBarrier(t)
	defer s.Close()
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	const waiters = 64
	taken, wait := startUnsealedCWaiters(ctx, t, b, waiters)
	taken.Wait()
	if err := b.Unseal(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}
	if missed := wait(); missed != 0 {
		t.Fatalf("%d of %d UnsealedC waiters not woken by Unseal within %v", missed, waiters, unsealedWaitBound)
	}
	requireUnsealedNoWaiter(ctx, t, b)
}

// (b) While one goroutine cycles Seal/Unseal, waiters that repeatedly take
// UnsealedC each wake within the bound, and no channel is closed twice.
func TestBarrier_UnsealedCRace_SealUnsealCycles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), unsealedWaitBound)
	defer cancel()
	b, s := testInitializedBarrier(t)
	defer s.Close()

	const (
		cycles  = 50
		waiters = 16
	)
	var stop atomic.Bool
	var missed, wakes atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// The last pass runs after stop is set: the barrier is unsealed
			// for good by then, so its channel must be closed.
			for {
				last := stop.Load()
				if !waitUnsealedC(ctx, b.UnsealedC()) {
					missed.Add(1)
					return
				}
				wakes.Add(1)
				if last {
					return
				}
			}
		}()
	}

	cycleErr := make(chan error, 1)
	go func() {
		defer close(cycleErr)
		for i := 0; i < cycles; i++ {
			if err := b.Seal(); err != nil {
				cycleErr <- err
				return
			}
			if err := b.Unseal(ctx, s, Credentials{}); err != nil {
				cycleErr <- err
				return
			}
		}
	}()
	if err := <-cycleErr; err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("Seal/Unseal cycle failed: %v", err)
	}
	stop.Store(true)
	wg.Wait()

	if n := missed.Load(); n != 0 {
		t.Fatalf("%d UnsealedC waiters not woken within %v while Seal/Unseal cycled", n, unsealedWaitBound)
	}
	if wakes.Load() < waiters {
		t.Fatalf("only %d wakes for %d waiters", wakes.Load(), waiters)
	}
	requireUnsealedNoWaiter(ctx, t, b)
}

// (c) A channel taken while the barrier is unsealed is already closed, also
// when many goroutines take it at once.
func TestBarrier_UnsealedCRace_ClosedWhileUnsealed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), unsealedWaitBound)
	defer cancel()
	b, s := testInitializedBarrier(t)
	defer s.Close()

	const takers = 32
	var open atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < takers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !isClosedUnsealedC(b.UnsealedC()) {
				open.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := open.Load(); n != 0 {
		t.Fatalf("%d of %d UnsealedC channels taken while unsealed were open", n, takers)
	}
	requireUnsealedNoWaiter(ctx, t, b)
}

// (d) Waiters taken after Reset are woken by the next Initialize, and so are
// waiters of the sealed period before the Reset.
func TestBarrier_UnsealedCRace_ResetThenInitialize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), unsealedWaitBound)
	defer cancel()
	b, s := testInitializedBarrier(t)
	defer s.Close()
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	const waiters = 16
	beforeTaken, beforeWait := startUnsealedCWaiters(ctx, t, b, waiters)
	beforeTaken.Wait()
	if err := b.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}
	if isClosedUnsealedC(b.UnsealedC()) {
		t.Fatal("UnsealedC closed on a reset (uninitialized) barrier")
	}
	afterTaken, afterWait := startUnsealedCWaiters(ctx, t, b, waiters)
	afterTaken.Wait()

	if err := b.Initialize(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Initialize after Reset failed: %v", err)
	}
	if missed := afterWait(); missed != 0 {
		t.Fatalf("%d of %d waiters taken after Reset not woken by Initialize within %v", missed, waiters, unsealedWaitBound)
	}
	if missed := beforeWait(); missed != 0 {
		t.Fatalf("%d of %d waiters taken before Reset not woken by Initialize within %v", missed, waiters, unsealedWaitBound)
	}
	requireUnsealedNoWaiter(ctx, t, b)
}
