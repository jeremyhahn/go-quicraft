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

package shamir

import (
	"bytes"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

// Regression cover for issue #15 on the Shamir path specifically. The defect
// lives in pkg/enclave, but ProtectedShare is how a custody ceremony actually
// reaches it, and that is the reachability the issue was filed about: a
// ceremony tearing shares down while a reader is still walking them is a
// realistic shape, not a contrived one.
//
// Meaningful under -race; `make test-race` covers this package.

// protectedTestShare builds one share to exercise.
func protectedTestShare(t *testing.T) *ProtectedShare {
	t.Helper()
	shares, err := SplitProtected([]byte("a-secret-worth-splitting-32-byte"), 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	// Destroy the ones this test does not drive, so the enclave count stays
	// bounded — locked memory is an RLIMIT_MEMLOCK resource.
	for _, s := range shares[1:] {
		s.Destroy()
	}
	return shares[0]
}

// TestProtectedShare_ValueDestroyRace drives Value concurrently with Destroy.
// Before the enclave fix this reports a race inside memguard reached through
// ProtectedShare.Value; after it, Value returns either the live value or nil.
func TestProtectedShare_ValueDestroyRace(t *testing.T) {
	const iterations = 100

	for i := 0; i < iterations; i++ {
		share := protectedTestShare(t)

		var wg sync.WaitGroup
		start := make(chan struct{})

		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for k := 0; k < 25; k++ {
					if v := share.Value(); v != nil {
						_ = len(v)
					}
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			share.Destroy()
		}()

		close(start)
		wg.Wait()

		share.Destroy()
		if v := share.Value(); v != nil {
			t.Fatalf("Value after Destroy = %q, want nil", v)
		}
	}
}

// TestProtectedShare_WithValueExcludesDestroy covers the window Value cannot:
// the caller is still READING the share's bytes when teardown arrives.
//
// It asserts the lifetime invariant — a callback must never be running once
// Destroy has returned — rather than dereferencing the value and waiting for a
// fault. See the long note on TestMemoryEnclave_WithBytesExcludesDestroy in
// pkg/enclave for why the dereferencing form silently passes against a broken
// implementation: -race does not instrument memguard's mmap'd region, so the
// only signal there is an intermittent SIGSEGV.
func TestProtectedShare_WithValueExcludesDestroy(t *testing.T) {
	const iterations = 100

	for i := 0; i < iterations; i++ {
		share := protectedTestShare(t)

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
					_ = share.WithValue(func(v []byte) error {
						for spin := 0; spin < 64; spin++ {
							_ = len(v)
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
			share.Destroy()
			destroyReturned.Store(true)
		}()

		close(start)
		wg.Wait()

		if got := violations.Load(); got != 0 {
			t.Fatalf("callback ran %d time(s) after Destroy had already returned; "+
				"the share was torn down while a reader held its value", got)
		}
	}
}

// TestProtectedShare_WithValueAfterDestroy pins the destroyed contract: the
// callback does not run and an error comes back.
func TestProtectedShare_WithValueAfterDestroy(t *testing.T) {
	share := protectedTestShare(t)
	share.Destroy()

	called := false
	err := share.WithValue(func([]byte) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("callback ran on a destroyed share")
	}
	if err == nil {
		t.Fatal("WithValue on a destroyed share returned nil error")
	}
}

// TestProtectedShare_WithValueNilReceiver keeps WithValue consistent with
// Value and Destroy, which are both nil-safe.
func TestProtectedShare_WithValueNilReceiver(t *testing.T) {
	var share *ProtectedShare

	called := false
	err := share.WithValue(func([]byte) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("callback ran on a nil share")
	}
	if err == nil {
		t.Fatal("WithValue on a nil share returned nil error")
	}
}

// TestProtectedShare_WithValueSeesTheSameBytes checks WithValue and Value
// agree, so the safe accessor is not a different view of the share.
func TestProtectedShare_WithValueSeesTheSameBytes(t *testing.T) {
	share := protectedTestShare(t)
	defer share.Destroy()

	want := string(share.Value())
	if want == "" {
		t.Fatal("share value is empty")
	}

	var got string
	if err := share.WithValue(func(v []byte) error {
		got = string(v)
		return nil
	}); err != nil {
		t.Fatalf("WithValue: %v", err)
	}
	if got != want {
		t.Fatalf("WithValue saw %q, Value saw %q", got, want)
	}
}

// TestCombineProtected_RacesShareDestroy is the regression cover for the
// reconstruction hazard raised in review of PR #16.
//
// WithValue was added as the teardown-safe accessor, and then CombineProtected
// — the one path in this package that reads share values on the caller's
// behalf — kept reading them through the bare alias. That is the scenario the
// issue was actually filed about: a ceremony cancelled or timed out while
// reconstruction is mid-decode. A caller cannot repair it from outside, because
// CombineProtected obtains the slices itself.
//
// Every read now runs inside the share's borrow, so a concurrent Destroy waits
// rather than unmapping the decode's source operand. The outcome of any given
// iteration is deliberately not asserted: whether reconstruction wins the race
// or reports a destroyed share is timing, and both are correct. What must never
// happen is a crash or a torn read, which is what running this under -race with
// teardown landing at every point in the decode is checking.
func TestCombineProtected_RacesShareDestroy(t *testing.T) {
	const iterations = 200

	secret := []byte("a-secret-worth-splitting-32-byte")

	for i := 0; i < iterations; i++ {
		shares, err := SplitProtected(secret, 2, 3)
		if err != nil {
			t.Fatalf("SplitProtected: %v", err)
		}

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := CombineProtected(shares[:2])
			if err == nil {
				// When reconstruction wins, it must produce the real secret —
				// a torn read that still returned nil error would be the worst
				// outcome, so check rather than discard.
				if got := string(out.Bytes()); got != string(secret) {
					t.Errorf("reconstructed %q, want %q", got, string(secret))
				}
				out.Destroy()
			}
		}()

		// Tear one share down underneath the reconstruction.
		wg.Add(1)
		go func() {
			defer wg.Done()
			shares[1].Destroy()
		}()

		wg.Wait()
		for _, s := range shares {
			s.Destroy()
		}
	}
}

// --- Deterministic hold-off, and the MarshalJSON alias (Codex round 3) ---

// shareHoldRounds mirrors the enclave-side deterministic probe. Repetition buys
// scheduler variety only; the probe reaches the state under test every round.
const shareHoldRounds = 20

// TestProtectedShare_WithValueHoldsOffDestroyDeterministically pins on every
// round what the hammer above samples: a Destroy already entered cannot
// COMPLETE while a WithValue callback is running.
func TestProtectedShare_WithValueHoldsOffDestroyDeterministically(t *testing.T) {
	for i := 0; i < shareHoldRounds; i++ {
		share := protectedTestShare(t)

		entered := make(chan struct{})  // callback holds the borrow
		parked := make(chan struct{})   // Destroy is blocked behind that borrow
		released := make(chan struct{}) // Destroy returned
		var didRelease atomic.Bool

		var parkedOnce sync.Once
		// The hook is package-global in pkg/enclave, so it fires for every
		// enclave in the binary. The share's value enclave is the only one this
		// probe may act on; anything else would let it conclude a park that
		// belonged to some other test's buffer.
		target := share.valueEnclaveForTest()
		restore := enclave.TestOnlyOnMutatorParked(func(e *enclave.MemoryEnclave) {
			if e == target {
				parkedOnce.Do(func() { close(parked) })
			}
		})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-entered
			share.Destroy()
			didRelease.Store(true)
			close(released)
		}()

		err := share.WithValue(func(b []byte) error {
			if len(b) == 0 {
				return errors.New("callback saw an empty value")
			}
			close(entered)
			// Observed, not assumed, and both-sided: whichever of "Destroy
			// parked behind this borrow" and "Destroy finished" happens first is
			// the answer. No interval, so nothing to tune and nothing to flake —
			// and no hang against an implementation that never parks.
			select {
			case <-parked:
			case <-released:
			}
			if didRelease.Load() {
				return errors.New("Destroy completed while the callback held the share")
			}
			return nil
		})
		wg.Wait()
		restore()

		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if got := share.Value(); got != nil {
			t.Fatalf("round %d: share survived the Destroy the callback held off", i)
		}
	}
}

// TestProtectedShare_MarshalJSONRacesDestroy is the regression cover for the
// third alias-lifetime finding. MarshalJSON used to take `val := s.Value()`,
// which ends the borrow at the point it returns, and then append `val` into its
// output buffer — a read of protected memory after the enclave was free to be
// unmapped. Same defect class as the CombineProtected finding, one call site
// further on, and unreachable by any caller-side discipline because MarshalJSON
// obtains the slice itself.
//
// THIS PROBE IS PROBABILISTIC, AND UNLIKE THE HOLD-OFF PROBES ABOVE IT CANNOT
// BE MADE DETERMINISTIC. The deterministic construction works by having the
// callback itself signal when the borrow is held; MarshalJSON runs no
// caller-supplied code inside its borrow, so there is no hook, and any
// arrangement that blocks Destroy from outside blocks it identically for a
// fixed and a broken implementation. Adding a test-only hook to MarshalJSON to
// manufacture one would be instrumenting production code to make a test
// convenient.
//
// So the window is widened instead, by the only lever the API offers: share
// SIZE. The value is the base64 of the largest secret this suite can split
// without straining RLIMIT_MEMLOCK, which makes the vulnerable `append` a
// ~44 KB copy rather than a ~44 byte one — roughly a thousandfold more time
// spent holding the alias.
//
// MEASURED catch rate against the faithful pre-fix mutation, recorded so a
// future reader knows what this test is and is not worth: 10/10 runs caught it
// under -race, 0/10 without. The detector's instrumentation is what stretches
// the window far enough, so THIS TEST IS ONLY MEANINGFUL UNDER -race — which is
// how the suite runs (GOTEST carries -race), but a reader who runs the package
// bare should know the probe is inert. The failure it produced was unambiguous:
// `invalid character '\x00' in string literal`, i.e. the wiped buffer being
// appended into the document mid-encode.
//
// A passing run is therefore weak evidence of correctness and a failing run is
// strong evidence of a defect. The reason the fix is trusted is that the encode
// provably runs inside the borrow, which is a property of the code; this probe
// is the backstop that would notice if that stopped being true.
//
// The assertion is on the RESULT, not on survival: marshalling either produces
// the whole share or refuses because the share was destroyed first. What it must
// never produce is a truncated or corrupted document, which is what reading a
// wiped buffer mid-append yields — a share record that round-trips as valid JSON
// and reconstructs garbage is a worse outcome than an error, because nothing
// downstream would flag it.
func TestProtectedShare_MarshalJSONRacesDestroy(t *testing.T) {
	const (
		iterations = 200
		// Large enough to make the append the dominant cost of MarshalJSON,
		// small enough that a 3-share set stays far inside MaxShareSetBytes.
		secretBytes = 32 << 10
	)

	secret := make([]byte, secretBytes)
	for i := range secret {
		secret[i] = byte(i)
	}

	for i := 0; i < iterations; i++ {
		shares, err := SplitProtected(secret, 2, 3)
		if err != nil {
			t.Fatalf("SplitProtected: %v", err)
		}
		for _, s := range shares[1:] {
			s.Destroy()
		}
		share := shares[0]
		want := append([]byte(nil), share.Value()...)

		var (
			wg       sync.WaitGroup
			out      []byte
			marshErr error
		)
		start := make(chan struct{})

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			out, marshErr = share.MarshalJSON()
		}()
		go func() {
			defer wg.Done()
			<-start
			share.Destroy()
		}()
		close(start)
		wg.Wait()

		if marshErr != nil {
			// Losing outright is legitimate, but only with this exact reason.
			var validationErr *ValidationError
			if !errors.As(marshErr, &validationErr) || validationErr.Reason != "share has been destroyed" {
				t.Fatalf("iteration %d: MarshalJSON failed with %v, want the destroyed-share ValidationError", i, marshErr)
			}
			continue
		}

		// It succeeded, so it must have produced the complete, correct document
		// — the value written verbatim into the JSON string literal.
		var decoded ProtectedShare
		if uerr := json.Unmarshal(out, &decoded); uerr != nil {
			t.Fatalf("iteration %d: MarshalJSON produced undecodable JSON (%d bytes): %v", i, len(out), uerr)
		}
		got := append([]byte(nil), decoded.Value()...)
		decoded.Destroy()
		if !bytes.Equal(got, want) {
			t.Fatalf("iteration %d: marshalled value differs from the share value — torn read of the protected buffer", i)
		}
	}
}

// valueEnclaveForTest exposes the share's value enclave so a probe can filter
// pkg/enclave's package-global park hooks down to its own target. It is in the
// test file, not the production one: the identity is needed to make a test
// honest, never to widen what a caller can reach.
func (s *ProtectedShare) valueEnclaveForTest() *enclave.MemoryEnclave { return s.value }
