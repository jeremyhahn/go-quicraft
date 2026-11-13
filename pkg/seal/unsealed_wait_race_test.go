package seal

import (
	"context"
	"runtime"
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
		// Each Unseal derives the key, about 150ms under -race: ten
		// periods keep the run well inside unsealedWaitBound.
		cycles  = 10
		waiters = 16
	)
	// sealed[p] closes once the cycler has sealed period p; taken[p] closes
	// once every waiter holds an UnsealedC channel that was still open in
	// period p. The cycler unseals period p only after taken[p], so every
	// waiter takes part in every sealed period.
	sealed := make([]chan struct{}, cycles)
	taken := make([]chan struct{}, cycles)
	takenN := make([]atomic.Int64, cycles)
	for p := range sealed {
		sealed[p] = make(chan struct{})
		taken[p] = make(chan struct{})
	}
	var missed atomic.Int64
	woke := make([]int, waiters)
	var wg sync.WaitGroup
	for w := 0; w < waiters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for p := 0; p < cycles; p++ {
				select {
				case <-sealed[p]:
				case <-ctx.Done():
					missed.Add(1)
					return
				}
				// The barrier stays sealed until every waiter is counted,
				// so a closed channel here means it was unsealed early:
				// retry, without counting, until the bound.
				var c <-chan struct{}
				for {
					c = b.UnsealedC()
					if !isClosedUnsealedC(c) {
						break
					}
					if ctx.Err() != nil {
						missed.Add(1)
						return
					}
				}
				if takenN[p].Add(1) == waiters {
					close(taken[p])
				}
				if !waitUnsealedC(ctx, c) {
					missed.Add(1)
					return
				}
				woke[w]++
			}
		}(w)
	}

	cycleErr := make(chan error, 1)
	go func() {
		defer close(cycleErr)
		for p := 0; p < cycles; p++ {
			if err := b.Seal(); err != nil {
				cycleErr <- err
				return
			}
			close(sealed[p])
			select {
			case <-taken[p]:
			case <-ctx.Done():
				cycleErr <- ctx.Err()
				return
			}
			if err := b.Unseal(ctx, s, Credentials{}); err != nil {
				cycleErr <- err
				return
			}
		}
	}()
	err := <-cycleErr
	wg.Wait()

	if n := missed.Load(); n != 0 {
		t.Fatalf("%d UnsealedC waiters not woken within %v while Seal/Unseal cycled", n, unsealedWaitBound)
	}
	if err != nil {
		t.Fatalf("Seal/Unseal cycle failed: %v", err)
	}
	for w, n := range woke {
		if n != cycles {
			t.Fatalf("waiter %d woke from an open UnsealedC channel in %d of %d sealed periods", w, n, cycles)
		}
	}
	requireUnsealedNoWaiter(ctx, t, b)
}

// unsealedCAcquire is an open UnsealedC channel a reader took, and the
// number of unseal transitions that must complete before it is closed.
type unsealedCAcquire struct {
	c        <-chan struct{}
	deadline int64
}

// unsealedCReaders is the result of startConcurrentUnsealedCReaders.
type unsealedCReaders struct {
	wg       sync.WaitGroup
	mu       sync.Mutex
	acquired []unsealedCAcquire
	// stale counts channels still open after the transition that had to
	// close them had completed.
	stale atomic.Int64
	// openAt[g] counts open channels taken while g transitions to unsealed
	// had completed.
	openAt []atomic.Int64
}

// startConcurrentUnsealedCReaders starts n goroutines that, from start until
// stop, call b.UnsealedC in a loop with no handshake against the transitions
// that run meanwhile. gen is the number of completed transitions to unsealed
// (Unseal or Initialize), stored by the transition goroutine once each one
// returns. A channel taken open while gen read g after the call must be
// closed once gen reaches g+1: the call ended before transition g+1
// completed, so that transition's wake-up follows the call or the call saw
// the barrier unsealed. Readers check this as they loop and record every open
// channel they took.
func startConcurrentUnsealedCReaders(ctx context.Context, t *testing.T, b *Barrier, n int, start, stop <-chan struct{}, gen *atomic.Int64, transitions int) *unsealedCReaders {
	t.Helper()
	r := &unsealedCReaders{openAt: make([]atomic.Int64, transitions+1)}
	r.wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer r.wg.Done()
			select {
			case <-start:
			case <-ctx.Done():
				return
			}
			var mine, pending []unsealedCAcquire
			var last <-chan struct{}
			for {
				select {
				case <-stop:
					r.mu.Lock()
					r.acquired = append(r.acquired, mine...)
					r.mu.Unlock()
					return
				case <-ctx.Done():
					r.mu.Lock()
					r.acquired = append(r.acquired, mine...)
					r.mu.Unlock()
					return
				default:
				}
				c := b.UnsealedC()
				g := gen.Load()
				if !isClosedUnsealedC(c) {
					r.openAt[g].Add(1)
					if c != last {
						a := unsealedCAcquire{c: c, deadline: g + 1}
						mine = append(mine, a)
						pending = append(pending, a)
						last = c
					}
				}
				kept := pending[:0]
				for _, a := range pending {
					switch {
					case a.deadline > g:
						kept = append(kept, a)
					case !isClosedUnsealedC(a.c):
						r.stale.Add(1)
					}
				}
				pending = kept
				runtime.Gosched()
			}
		}()
	}
	return r
}

// (e) Readers take UnsealedC continuously while Seal, Unseal, Reset and
// Initialize run, with no handshake ordering the calls against the
// transitions. Every open channel a reader took is closed by the first
// transition to unsealed that completes after the take, and all of them are
// closed after the final Unseal.
func TestBarrier_UnsealedCRace_ConcurrentAcquireDuringTransitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), unsealedWaitBound)
	defer cancel()
	b, s := testInitializedBarrier(t)
	defer s.Close()

	const (
		// Each Unseal derives the key, about 150ms under -race.
		cycles  = 4
		readers = 8
		// cycles Unseals, one Initialize after Reset, and the final Unseal.
		transitions = cycles + 2
	)
	start := make(chan struct{})
	stop := make(chan struct{})
	var gen atomic.Int64
	r := startConcurrentUnsealedCReaders(ctx, t, b, readers, start, stop, &gen, transitions)

	cycle := func() error {
		close(start)
		for p := 0; p < cycles; p++ {
			if err := b.Seal(); err != nil {
				return err
			}
			if err := b.Unseal(ctx, s, Credentials{}); err != nil {
				return err
			}
			gen.Add(1)
		}
		if err := b.Seal(); err != nil {
			return err
		}
		if err := b.Reset(); err != nil {
			return err
		}
		if err := b.Initialize(ctx, s, Credentials{}); err != nil {
			return err
		}
		gen.Add(1)
		if err := b.Seal(); err != nil {
			return err
		}
		if err := b.Unseal(ctx, s, Credentials{}); err != nil {
			return err
		}
		gen.Add(1)
		return nil
	}
	err := cycle()
	close(stop)
	r.wg.Wait()
	if err != nil {
		t.Fatalf("Seal/Unseal/Reset/Initialize cycle failed: %v", err)
	}

	missed := 0
	for _, a := range r.acquired {
		if !waitUnsealedC(ctx, a.c) {
			missed++
		}
	}
	if missed != 0 {
		t.Fatalf("%d of %d UnsealedC channels taken during transitions still open %v after the final Unseal", missed, len(r.acquired), unsealedWaitBound)
	}
	if n := r.stale.Load(); n != 0 {
		t.Fatalf("%d UnsealedC channels still open after the transition to unsealed that followed their take", n)
	}
	periods := 0
	for g := range r.openAt {
		if r.openAt[g].Load() > 0 {
			periods++
		}
	}
	if periods < 2 {
		t.Fatalf("readers took an open UnsealedC channel in %d sealed periods, want at least 2: the race had no reach", periods)
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
