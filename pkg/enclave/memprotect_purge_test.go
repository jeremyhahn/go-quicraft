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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Regression cover for issue #18: a memguard operation that can PURGE THE
// PROCESS must not run while any borrow is live, anywhere in the process.
//
// WHY THIS PROBE INTERROGATES THE LOCK INSTEAD OF PROVOKING A REAL PURGE.
// The defect's observable form is a SIGSEGV: Purge unmaps the pages a borrow is
// reading. A test cannot assert on that. Worse, the usual fallback is unavailable
// here — the race detector CANNOT SEE reads of memguard's region at all, because
// memcall.Alloc is mmap and the detector does not instrument it. A green -race
// run is therefore not evidence of anything for this class, and neither is the
// absence of a crash: an unsynchronized run that simply did not interleave badly
// looks identical to a correct one.
//
// So the invariant is asserted directly. purgeMu.TryRLock fails exactly when a
// writer holds the lock or is queued for it. The borrow below already holds the
// lock SHARED, so a recursive TryRLock can only fail because a purge-capable
// operation reached purgeMu.Lock while this borrow was live — which is precisely
// the interleaving the fix must make safe, observed rather than assumed.
//
// The probe is event-driven and carries a VACUITY CHECK: if it never observes a
// writer queued, it FAILS rather than passing quietly. Without that, an
// implementation with no coordination at all would let the allocation finish
// before the first sample and satisfy every postcondition without ever having
// been tested.
//
// WHAT THIS DOES NOT PROVE, stated so the coverage is not overread: it shows the
// purge-capable operation was BLOCKED and had not completed while the borrow was
// live. It does not prove the memguard call itself happened inside the exclusive
// section — an implementation that queued on purgeMu, released it, and only then
// called memguard would still satisfy these assertions. Closing that would need a
// seam inside memguard this repository does not own. The exclusive sections are
// verified by reading them (each takes purgeMu for the whole call via defer).
func TestPurgeCapableAllocationCannotRunDuringABorrow(t *testing.T) {
	enc := NewMemoryEnclave(32)
	defer enc.Destroy()

	var (
		allocCompleted atomic.Bool
		other          *MemoryEnclave
		wg             sync.WaitGroup
	)
	// The BORROW signals the goroutine, not the other way round. Signalling in
	// the other direction let the allocation finish before WithBytes had even
	// started, which fails the vacuity check below rather than testing anything
	// — a flake against correct code.
	borrowHeld := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-borrowHeld
		// A purge-capable operation on an UNRELATED enclave. Before #18 this
		// could unmap the pages the borrow below is holding, because memguard's
		// failure path is process-wide and holds none of this package's locks.
		other = NewMemoryEnclave(32)
		allocCompleted.Store(true)
	}()

	err := enc.WithBytes(func(b []byte) error {
		if len(b) == 0 {
			return errors.New("callback saw an empty region")
		}
		close(borrowHeld)

		// Signalling is still not enough on its own: the goroutine can be
		// descheduled between waking and NewMemoryEnclave, so observing the
		// writer actually queued is the only reliable evidence that the
		// interleaving happened. Poll until we see it.
		observed := false
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if !purgeMu.TryRLock() {
				observed = true
				break
			}
			purgeMu.RUnlock()
			runtime.Gosched()
		}

		if !observed {
			return errors.New(
				"no writer ever queued on purgeMu while the borrow was live — " +
					"the test would be vacuous, so it fails instead of passing quietly")
		}
		// THE INVARIANT. The allocation is queued on purgeMu; it must not have
		// completed, because completing means it ran its purge-capable memguard
		// call while this borrow held a slice into locked pages.
		if allocCompleted.Load() {
			return errors.New("a purge-capable allocation completed while a borrow was live")
		}
		return nil
	})

	wg.Wait()
	if other != nil {
		other.Destroy()
	}
	if err != nil {
		t.Fatal(err)
	}
	// It must complete once the borrow ends — the lock serializes, it does not
	// forbid.
	if !allocCompleted.Load() {
		t.Fatal("allocation never completed after the borrow was released")
	}
}

// Destroy is purge-capable too, and for a reason that is easy to miss: memguard's
// Buffer.Destroy calls Panic — hence Purge — when RELEASING the pages fails
// (core/buffer.go:182). So a Destroy on one enclave can unmap another enclave's
// pages mid-borrow, exactly as an allocation can, and holding only the
// per-enclave lock does not prevent it.
func TestDestroyOfAnotherEnclaveCannotRunDuringABorrow(t *testing.T) {
	enc := NewMemoryEnclave(32)
	defer enc.Destroy()
	victim := NewMemoryEnclave(32)

	var (
		destroyCompleted atomic.Bool
		wg               sync.WaitGroup
	)
	// Borrow-signals-goroutine, as in the probe above and for the same reason.
	borrowHeld := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-borrowHeld
		victim.Destroy()
		destroyCompleted.Store(true)
	}()

	err := enc.WithBytes(func([]byte) error {
		close(borrowHeld)
		observed := false
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if !purgeMu.TryRLock() {
				observed = true
				break
			}
			purgeMu.RUnlock()
			runtime.Gosched()
		}
		if !observed {
			return errors.New(
				"no writer ever queued on purgeMu while the borrow was live — " +
					"the test would be vacuous, so it fails instead of passing quietly")
		}
		if destroyCompleted.Load() {
			return errors.New("a Destroy of another enclave completed while a borrow was live")
		}
		return nil
	})

	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if !destroyCompleted.Load() {
		t.Fatal("Destroy never completed after the borrow was released")
	}
}
