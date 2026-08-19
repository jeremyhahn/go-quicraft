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

package enclave

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The tests in this file are the regression cover for issue #15: a reader
// (Bytes/Size) racing a concurrent Destroy on the same MemoryEnclave.
//
// WHY THE RACE IS REAL, at the memguard layer rather than ours. In memguard
// v0.23.0 `core.Buffer` embeds a `sync.RWMutex` carrying its author's own note
// — "this does not protect 'data' field". `Buffer.Data()`, which
// LockedBuffer.Bytes calls, returns `b.data` while holding NOTHING;
// `Buffer.destroy()` takes the write lock and sets `b.data = nil` after
// wiping, unlocking and `memcall.Free`ing the region. An unsynchronized read
// against a locked write is still a data race, and because the region is
// genuinely unmapped rather than merely zeroed, the consequence of losing it
// is a SIGSEGV rather than a stale read.
//
// An atomic.Pointer around the LockedBuffer cannot fix that: it makes access to
// the POINTER race-free and says nothing about the lifetime of what the pointer
// addresses. Serializing reads against mutators is what fixes it, which is what
// the enclave's borrow counter does — readers register a borrow, and Destroy,
// Freeze and SealMemoryEnclave wait for the count to fall to zero.
//
// These tests must be run under -race to be meaningful. Without it they are
// still useful (they can catch a segfault or a nil deref) but they will not
// report the race itself. `make test-race` covers this package.

// TestMemoryEnclave_BytesDestroyRace drives Bytes concurrently with Destroy.
//
// Before the fix this reports a race on memguard's `data` field, or crashes.
// After it, Bytes either observes a live buffer or observes nil — never a
// buffer mid-destruction.
func TestMemoryEnclave_BytesDestroyRace(t *testing.T) {
	const iterations = 200

	for i := 0; i < iterations; i++ {
		p := NewMemoryEnclave(32)

		var wg sync.WaitGroup
		start := make(chan struct{})

		// Readers. A returned slice is only touched inside the call's own
		// critical section via len(), so this test isolates window 1 from
		// issue #15 (the dereference-after-load) rather than window 2 (a
		// caller holding the slice past the call).
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for k := 0; k < 50; k++ {
					if b := p.Bytes(); b != nil {
						_ = len(b)
					}
					_ = p.Size()
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p.Destroy()
		}()

		close(start)
		wg.Wait()

		// Destroy is idempotent; a second call must not double-free.
		p.Destroy()
		if got := p.Bytes(); got != nil {
			t.Fatalf("Bytes after Destroy = %v, want nil", got)
		}
		if got := p.Size(); got != 0 {
			t.Fatalf("Size after Destroy = %d, want 0", got)
		}
	}
}

// TestMemoryEnclave_WithBytesExcludesDestroy covers window 2 — a caller USING
// the bytes while another goroutine tears the enclave down. This is the window
// a plain `Bytes()` accessor cannot close by any amount of internal locking,
// because the returned slice aliases the protected region and outlives the
// call. WithBytes closes it by holding the read lock across the callback.
//
// WHY THIS ASSERTS AN INVARIANT INSTEAD OF DEREFERENCING THE BYTES. The
// obvious test — walk every byte inside the callback and let the tooling catch
// the fault — does not work, and quietly passes against a broken
// implementation. `-race` instruments Go-allocated memory; memguard's region
// comes from `memcall.Alloc` (mmap), which the detector does not track, so a
// read of freed protected memory raises no warning. Its real failure mode is a
// SIGSEGV, which crashes the whole test binary rather than failing one test,
// and only when the timing lands. Mutation-testing this file confirmed it:
// releasing the lock before invoking the callback still passed the walking
// version.
//
// So the property checked here is the one that actually defines correctness and
// is observable from Go: WHILE A CALLBACK IS RUNNING, DESTROY CANNOT HAVE
// COMPLETED. With the lock held across fn, Destroy blocks on the write lock
// until fn returns, so a callback can never observe a finished teardown. With
// the lock released early, the callback holds a slice into a region Destroy has
// already unmapped — which is precisely the bug — and that is detectable
// without touching the freed memory.
func TestMemoryEnclave_WithBytesExcludesDestroy(t *testing.T) {
	const iterations = 200

	for i := 0; i < iterations; i++ {
		p := NewMemoryEnclave(32)

		var (
			destroyReturned atomic.Bool
			violations      atomic.Int64
			wg              sync.WaitGroup
		)
		start := make(chan struct{})

		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for k := 0; k < 25; k++ {
					_ = p.WithBytes(func(b []byte) error {
						// Widen the window so a lock-less implementation is
						// reliably caught. len() reads the local slice header,
						// never the protected pages, so this stays safe even
						// under a deliberately broken implementation.
						for spin := 0; spin < 64; spin++ {
							_ = len(b)
							runtime.Gosched()
						}
						if destroyReturned.Load() {
							violations.Add(1)
						}
						return nil
					})
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p.Destroy()
			destroyReturned.Store(true)
		}()

		close(start)
		wg.Wait()

		if got := violations.Load(); got != 0 {
			t.Fatalf("callback ran %d time(s) after Destroy had already returned; "+
				"the enclave was torn down while a reader held its bytes", got)
		}
	}
}

// TestMemoryEnclave_WithBytesAfterDestroy pins the destroyed-enclave contract:
// the callback does not run and a typed error comes back, rather than the
// callback receiving a nil slice it might mistake for an empty secret.
func TestMemoryEnclave_WithBytesAfterDestroy(t *testing.T) {
	p := NewMemoryEnclave(16)
	p.Destroy()

	called := false
	err := p.WithBytes(func([]byte) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("callback ran on a destroyed enclave")
	}
	if err == nil {
		t.Fatal("WithBytes on a destroyed enclave returned nil error")
	}
}

// TestMemoryEnclave_WithBytesPropagatesError checks the callback's error is
// returned unchanged, so a caller can distinguish its own failure from a
// destroyed enclave.
func TestMemoryEnclave_WithBytesPropagatesError(t *testing.T) {
	p := NewMemoryEnclave(16)
	defer p.Destroy()

	sentinel := &NilError{}
	if err := p.WithBytes(func([]byte) error { return sentinel }); err != sentinel {
		t.Fatalf("WithBytes error = %v, want the callback's own error", err)
	}
}

// TestMemoryEnclave_WithBytesNilReceiver keeps the nil-receiver behaviour in
// line with Bytes and Destroy, which are both nil-safe.
func TestMemoryEnclave_WithBytesNilReceiver(t *testing.T) {
	var p *MemoryEnclave

	called := false
	err := p.WithBytes(func([]byte) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("callback ran on a nil enclave")
	}
	if err == nil {
		t.Fatal("WithBytes on a nil enclave returned nil error")
	}
}

// TestMemoryEnclave_FreezeExcludesWritingCallback is the regression cover for
// the Freeze-vs-WithBytes hazard raised in review of PR #16.
//
// A fresh NewMemoryEnclave is deliberately MUTABLE, and WithBytes hands its
// callback a writable slice — that is the whole "allocate, write in place,
// freeze" pattern. While Freeze took only the read lock it could run
// concurrently with such a callback, flip the pages to read-only partway
// through, and the callback's next write would fault: a process kill, not a
// recoverable error.
//
// Same testing constraint as the Destroy case, and for the same reason: a real
// faulting write cannot be asserted on, because it terminates the binary rather
// than failing a test. So the invariant checked is the one that makes the write
// safe — FREEZE CANNOT COMPLETE WHILE A CALLBACK IS RUNNING.
//
// The callback deliberately does NOT write. A first draft did, to make the
// hazard vivid, and it crashed against the CORRECT implementation: once Freeze
// has legitimately completed, every later callback's write faults, which is the
// documented and desired behaviour of a frozen enclave, not a defect. There is
// no way for a writer to test-and-write atomically against a concurrent freeze
// — that is exactly why Freeze must exclude callbacks rather than merely be
// ordered against them. Asserting the exclusion covers the write hazard without
// depending on a fault to report it.
// It is deterministic rather than a probabilistic hammer. Hammering needs a
// flag set after Freeze returns, and that store is outside the lock, so it
// cannot be ordered against the callbacks — it reports overlaps that never
// happened. Blocking one callback open and observing whether Freeze can get
// past it tests the exclusion directly and cannot flake either way.
func TestMemoryEnclave_FreezeExcludesWritingCallback(t *testing.T) {
	p := NewMemoryEnclave(32)
	defer p.Destroy()

	var (
		entered        = make(chan struct{})
		release        = make(chan struct{})
		freezeReturned atomic.Bool
		wg             sync.WaitGroup
	)

	// A producer holding the enclave open, as a caller filling it in place
	// would. While this callback runs, no re-protection may complete.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = p.WithBytes(func(b []byte) error {
			close(entered)
			// len() only — see the note above on why this must not write.
			_ = len(b)
			<-release
			return nil
		})
	}()

	<-entered

	// Learn when Freeze has actually parked behind the borrow rather than
	// waiting an interval and assuming it. A sleep here would be a
	// synchronization primitive, which the testing convention forbids: on a
	// loaded runner it becomes either a hang or a vacuous pass.
	parked := make(chan struct{})
	var parkedOnce sync.Once
	// Filtered on p: the hook is package-global, so a mutator parking on any
	// other enclave in this binary would otherwise be mistaken for this one.
	restore := TestOnlyOnMutatorParked(func(e *MemoryEnclave) {
		if e == p {
			parkedOnce.Do(func() { close(parked) })
		}
	})
	defer restore()

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Freeze()
		freezeReturned.Store(true)
	}()

	// A Freeze that excludes readers reaches the park and stops there. One that
	// does not never parks at all — it sails past the in-flight callback and
	// sets freezeReturned — so the two outcomes are raced against each other and
	// whichever happens IS the answer, with no interval to guess at.
	for !freezeReturned.Load() {
		select {
		case <-parked:
		default:
			runtime.Gosched()
			continue
		}
		break
	}

	if freezeReturned.Load() {
		close(release)
		wg.Wait()
		t.Fatal("Freeze completed while a callback still held the enclave's bytes; " +
			"page protection can change under an in-flight writer")
	}

	close(release)
	wg.Wait()

	if !freezeReturned.Load() {
		t.Fatal("Freeze never completed after the callback returned")
	}
}

// TestMemoryEnclave_DestroyRaceWithFreeze covers the third mutator pair:
// Freeze changes page protection while Destroy melts and unmaps.
func TestMemoryEnclave_DestroyRaceWithFreeze(t *testing.T) {
	const iterations = 200

	for i := 0; i < iterations; i++ {
		p := NewMemoryEnclave(32)

		var wg sync.WaitGroup
		start := make(chan struct{})

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p.Freeze()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p.Destroy()
		}()

		close(start)
		wg.Wait()
	}
}

// TestMemoryEnclave_QueuedMutatorIsNotStarved is the regression cover for the
// fairness half of issue #15, raised by Codex review of PR #17.
//
// The first borrow-counter implementation admitted a new reader whenever the
// region was non-empty, regardless of whether a mutator was already queued. A
// steady stream of overlapping WithBytes callbacks could therefore hold the
// count above zero indefinitely and a parked Destroy would never run — the
// secret never wiped, the locked page never released. For a type whose whole
// purpose is that the wipe happens, an unbounded delay is not a fairness nicety
// but the guarantee failing silently.
//
// This is arranged to be deterministic rather than a load test. One borrow is
// held open; a Destroy is started and observed to PARK behind it; only then are
// the late readers started. The assertion is that every late reader observes a
// destroyed enclave — i.e. it was admitted AFTER the mutator finished, not
// before it. Under the admit-anyone policy a late reader slips in ahead of the
// parked Destroy and sees live bytes, which is exactly the starvation signature.
func TestMemoryEnclave_QueuedMutatorIsNotStarved(t *testing.T) {
	const lateReaders = 8

	p := NewMemoryEnclave(32)

	var (
		holding      = make(chan struct{})
		release      = make(chan struct{})
		parked       = make(chan struct{})
		allReadersIn = make(chan struct{})
		parkedOnce   sync.Once
		readersOnce  sync.Once
		readerParks  atomic.Int64
		wg           sync.WaitGroup
	)

	// Both hooks filter on p: they are package-global, so a mutator or reader
	// parking on any other enclave in this binary would otherwise be counted
	// here and let the test conclude a state it never reached.
	restoreMutator := TestOnlyOnMutatorParked(func(e *MemoryEnclave) {
		if e == p {
			parkedOnce.Do(func() { close(parked) })
		}
	})
	defer restoreMutator()

	// The reader hook is what makes the WAKEUP testable. Without it the test
	// releases the held borrow before the late readers have queued, and an
	// implementation that blocks them correctly and then never Broadcasts still
	// passes — the readers simply arrive after the mutator is gone and see
	// waiting == 0. Counting them into readable.Wait is what forces the
	// readmission path to run.
	restoreReader := TestOnlyOnReaderParked(func(e *MemoryEnclave) {
		if e == p && readerParks.Add(1) == lateReaders {
			readersOnce.Do(func() { close(allReadersIn) })
		}
	})
	defer restoreReader()

	// The borrow the mutator will have to wait for.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = p.WithBytes(func([]byte) error {
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding

	destroyed := make(chan struct{})
	go func() {
		defer close(destroyed)
		p.Destroy()
	}()

	// Observed, not assumed: Destroy is now queued behind the held borrow.
	<-parked

	// Late readers, all arriving while the mutator is parked. Every one of them
	// must be made to wait for it, and must later be woken by it.
	sawLive := make(chan int, lateReaders)
	var lateWG sync.WaitGroup
	for i := 0; i < lateReaders; i++ {
		lateWG.Add(1)
		go func(n int) {
			defer lateWG.Done()
			if b := p.Bytes(); b != nil {
				sawLive <- n
			}
		}(i)
	}

	lateDone := make(chan struct{})
	go func() { lateWG.Wait(); close(lateDone) }()

	// Both-sided, for the reason every wait in this file is: waiting only on
	// allReadersIn would HANG against an implementation that admits readers past
	// a queued mutator, because none of them would ever park — and a hang is not
	// a test failure. Whichever arm fires IS the answer.
	select {
	case <-allReadersIn:
		// Every late reader is parked behind the mutator: the intended state,
		// and the one that makes the Broadcast in unlockMutated load-bearing.
	case <-lateDone:
		// They all finished without parking. That is the starvation defect
		// itself; the sawLive check below names it.
	}

	close(release)
	wg.Wait()

	// The deadlines below are FAILURE GUARDS, not synchronization: each failure
	// they report is a hang, and without them a regression stalls the suite
	// instead of failing it.
	select {
	case <-destroyed:
	case <-time.After(30 * time.Second):
		t.Fatal("Destroy never completed: the queued mutator was starved by readers admitted after it")
	}

	select {
	case <-lateDone:
	case <-time.After(30 * time.Second):
		t.Fatal("late readers were never readmitted after the mutator finished: " +
			"they are still parked in readable.Wait, so unlockMutated did not Broadcast")
	}

	close(sawLive)
	if n, ok := <-sawLive; ok {
		t.Fatalf("late reader %d was admitted ahead of a queued Destroy and observed live bytes; "+
			"a steady stream of such readers would starve the wipe indefinitely", n)
	}
}

// --- Deterministic hold-off, alongside the probabilistic hammer above ---
//
// TestMemoryEnclave_WithBytesExcludesDestroy hammers: many readers, many
// interleavings, a widened window. That is the right shape for finding an
// interleaving nobody predicted, and it is what caught the original defect.
// What it cannot promise is that the state under test was reached on any
// particular run — a hammer that never happens to overlap passes for the wrong
// reason, and on a loaded CI runner that is not hypothetical.
//
// The probe below is the complement: it is arranged so the interesting state is
// reached on EVERY round. The destroyer is released from inside the callback,
// so WithBytes always wins the borrow and the callback always runs, and the
// callback then waits for the destroyer to REACH Destroy before it checks
// anything. Neither replaces the other; the hammer explores, this one pins.

// enclaveHoldRounds is the number of rounds the deterministic hold-off probes
// run. Repetition buys scheduler variety only — the probe is deterministic —
// and each round costs destroyProbeGrace.
const enclaveHoldRounds = 20

// TestMemoryEnclave_WithBytesHoldsOffDestroyDeterministically pins, on every
// round, the property the hammer above can only sample: while a callback holds
// the enclave, a Destroy that has already been entered cannot COMPLETE.
//
// It asserts the reason rather than the outcome. The outcome — reading wiped or
// unmapped memory — is a SIGSEGV that takes the whole test binary down, and as
// the hammer's own comment records, -race cannot see it either: memguard's
// region comes from memcall.Alloc (mmap), which the detector does not
// instrument. "Destroy completed while the callback held the enclave" is the
// property that defines correctness AND is observable from Go.
func TestMemoryEnclave_WithBytesHoldsOffDestroyDeterministically(t *testing.T) {
	for i := 0; i < enclaveHoldRounds; i++ {
		p := NewMemoryEnclave(32)

		entered := make(chan struct{})  // callback holds the borrow
		parked := make(chan struct{})   // Destroy is blocked behind that borrow
		released := make(chan struct{}) // Destroy returned
		var didRelease atomic.Bool

		var parkedOnce sync.Once
		restore := TestOnlyOnMutatorParked(func(e *MemoryEnclave) {
			if e == p {
				parkedOnce.Do(func() { close(parked) })
			}
		})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-entered
			p.Destroy()
			didRelease.Store(true)
			close(released)
		}()

		err := p.WithBytes(func(b []byte) error {
			if len(b) != 32 {
				return fmt.Errorf("callback saw a %d-byte slice, want 32", len(b))
			}
			close(entered)
			// Wait for whichever comes first: Destroy PARKING behind this
			// borrow, or Destroy finishing. Both arms are observed rather than
			// assumed — no interval, so nothing to tune and nothing to flake
			// under load. Waiting only on `parked` would HANG rather than fail
			// against an implementation that never parks, and a hang is not a
			// test failure.
			select {
			case <-parked:
			case <-released:
			}
			if didRelease.Load() {
				return errors.New("Destroy completed while the callback held the enclave")
			}
			// len reads the local slice header, never the protected pages, so
			// this stays safe even against a deliberately broken build.
			if len(b) != 32 {
				return fmt.Errorf("slice header changed during the callback: %d", len(b))
			}
			return nil
		})
		wg.Wait()
		restore()

		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if got := p.Bytes(); got != nil {
			t.Fatalf("round %d: enclave survived the Destroy the callback held off", i)
		}
	}
}

// BenchmarkMemoryEnclave_Bytes measures the accessor the seal barrier sits on.
// The borrow replaced an atomic load, and this is the number that says what
// that cost, so a future change to the barrier's hot path argues with a
// measured figure rather than an assumption.
func BenchmarkMemoryEnclave_Bytes(b *testing.B) {
	e := NewMemoryEnclave(32)
	defer e.Destroy()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = e.Bytes()
	}
}

// BenchmarkMemoryEnclave_WithBytes measures the scoped accessor against the
// plain one: the difference is the callback, not the borrow.
func BenchmarkMemoryEnclave_WithBytes(b *testing.B) {
	e := NewMemoryEnclave(32)
	defer e.Destroy()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = e.WithBytes(func([]byte) error { return nil })
	}
}

// BenchmarkMemoryEnclave_BytesParallel measures the borrow under contention,
// which an RWMutex read lock and a counted borrow do not price the same way:
// both serialize on one mutex here, and this is what says how much that costs
// when several goroutines read one enclave.
func BenchmarkMemoryEnclave_BytesParallel(b *testing.B) {
	e := NewMemoryEnclave(32)
	defer e.Destroy()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = e.Bytes()
		}
	})
}

// TestMemoryEnclave_TwoQueuedMutatorsBothComplete closes the Signal-vs-Broadcast
// gap, raised by the local Codex review of PR #17.
//
// Every other probe in this file parks at most ONE mutator, and one parked
// waiter cannot tell Broadcast from Signal. Swapping idle.Broadcast() for
// idle.Signal() would pass all of them and deadlock in production: Signal wakes
// one mutator, and when that one finishes `waiting` is still non-zero, so
// unlockMutated does not readmit readers either — the second mutator sleeps
// forever and so does every reader behind it.
//
// Deterministic by construction: one borrow is held, BOTH mutators are observed
// to park behind it, and only then is the borrow released. Both must finish.
func TestMemoryEnclave_TwoQueuedMutatorsBothComplete(t *testing.T) {
	p := NewMemoryEnclave(32)

	var (
		holding    = make(chan struct{})
		release    = make(chan struct{})
		bothParked = make(chan struct{})
		parkCount  atomic.Int64
		parkedOnce sync.Once
		wg         sync.WaitGroup
	)

	restore := TestOnlyOnMutatorParked(func(e *MemoryEnclave) {
		if e == p && parkCount.Add(1) == 2 {
			parkedOnce.Do(func() { close(bothParked) })
		}
	})
	defer restore()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = p.WithBytes(func([]byte) error {
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding

	// Two mutators, both of which must queue behind the held borrow. Freeze and
	// Destroy rather than two Destroys: Destroy is single-shot, so a second one
	// would return without ever parking.
	frozen := make(chan struct{})
	go func() { defer close(frozen); p.Freeze() }()
	destroyed := make(chan struct{})
	go func() { defer close(destroyed); p.Destroy() }()

	// Observed, not assumed: both are now parked on the idle condition.
	// Waiting on the count is what makes a single-waiter wakeup insufficient.
	select {
	case <-bothParked:
	case <-time.After(30 * time.Second):
		// Failure guard: if only one ever parks the premise is broken and the
		// test would otherwise hang rather than say so.
		close(release)
		wg.Wait()
		t.Fatalf("only %d mutator(s) parked behind the borrow; the probe never reached the state it tests", parkCount.Load())
	}

	close(release)
	wg.Wait()

	for name, done := range map[string]chan struct{}{"Freeze": frozen, "Destroy": destroyed} {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("%s never completed: with two mutators parked, waking only one leaves the other "+
				"asleep forever — idle must be Broadcast, not Signal", name)
		}
	}
}

// TestMemoryEnclave_SealExcludesInFlightCallback covers the third mutator.
//
// Destroy and Freeze are both exercised against an in-flight callback;
// SealMemoryEnclave was not, so deleting its lockIdle/unlockMutated pair passed
// the whole suite. Sealing ends the locked buffer's life exactly as Destroy
// does — it hands the memory to memguard's Seal — so it needs the same
// exclusion and the same proof.
func TestMemoryEnclave_SealExcludesInFlightCallback(t *testing.T) {
	p := NewMemoryEnclaveFromBytes([]byte("thirty-two bytes of key material"))

	var (
		parked     = make(chan struct{})
		sealed     = make(chan struct{})
		didSeal    atomic.Bool
		parkedOnce sync.Once
		key        *ProtectedKey
		wg         sync.WaitGroup
	)

	restore := TestOnlyOnMutatorParked(func(e *MemoryEnclave) {
		if e == p {
			parkedOnce.Do(func() { close(parked) })
		}
	})
	defer restore()

	entered := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-entered
		key = SealMemoryEnclave(p)
		didSeal.Store(true)
		close(sealed)
	}()

	err := p.WithBytes(func(b []byte) error {
		if len(b) != 32 {
			return fmt.Errorf("callback saw a %d-byte slice, want 32", len(b))
		}
		close(entered)
		// Park or finish, whichever comes first — a bare wait on the park would
		// hang against an implementation that seals straight through.
		select {
		case <-parked:
		case <-sealed:
		}
		if didSeal.Load() {
			return errors.New("SealMemoryEnclave completed while the callback held the enclave; " +
				"sealing ends the buffer's life and must exclude readers exactly as Destroy does")
		}
		return nil
	})
	wg.Wait()

	if err != nil {
		t.Fatal(err)
	}
	if key == nil {
		t.Fatal("SealMemoryEnclave returned nil for a live enclave")
	}
	if got := p.Bytes(); got != nil {
		t.Fatal("the enclave still has a buffer after being sealed away")
	}
	opened, oerr := key.Open()
	if oerr != nil {
		t.Fatalf("opening the sealed key: %v", oerr)
	}
	defer opened.Destroy()
	if got := string(opened.Bytes()); got != "thirty-two bytes of key material" {
		t.Fatalf("sealed key opened as %q", got)
	}
}
