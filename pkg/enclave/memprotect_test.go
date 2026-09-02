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

// Tests for what this package still owns after the go-enclave adoption:
// ProtectedKey, its errors, and the locked-to-locked bridge between memguard's
// encrypted-at-rest container and go-enclave's locked memory.
//
// WHAT WAS DROPPED, and why it is not a coverage regression. These tests went
// with the implementation they covered — TestProtectedBuffer_Lifecycle,
// _NilAfterDestroy, _FromBytes, _ConcurrentDestroy,
// TestMemoryEnclave_WriteInPlaceThenFreeze, _FreezeIsSafeOnDestroyed and
// _FromBytesWipesSourceAndFreezes, plus the whole of memprotect_race_test.go and
// memprotect_purge_test.go. Every one exercised this repository's own mlock,
// guard-page, canary, borrow and purge machinery, which no longer exists here:
// MemoryEnclave is now an alias for go-enclave's type, and those properties are
// go-enclave's to test. Re-asserting them here would be testing a dependency
// through its consumer. What replaces them is narrower and honest — the alias
// identity is pinned below, and the behaviour is pinned upstream.
package enclave

import (
	"bytes"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	goenclave "github.com/jeremyhahn/go-enclave"
)

// mustSized allocates n bytes of locked, writable memory, failing the test if
// the kernel refuses the lock.
func mustSized(t testing.TB, n int) *MemoryEnclave {
	t.Helper()
	e, err := goenclave.NewMemoryEnclaveSized(n)
	if err != nil {
		t.Fatalf("NewMemoryEnclaveSized(%d): %v", n, err)
	}
	return e
}

// mustFromBytes moves data into a locked, frozen enclave.
func mustFromBytes(t testing.TB, data []byte) *MemoryEnclave {
	t.Helper()
	e, err := goenclave.NewMemoryEnclave(data)
	if err != nil {
		t.Fatalf("NewMemoryEnclave: %v", err)
	}
	return e
}

func encBytes(t testing.TB, e *MemoryEnclave) []byte {
	t.Helper()
	b, err := e.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return b
}

// --- The alias, which is the whole adoption in one assertion ---

// TestMemoryEnclaveIsGoEnclaves pins that this package declares NO locked-memory
// type of its own. MemoryEnclave, the deprecated ProtectedBuffer and go-enclave's
// MemoryEnclave must be ONE type: assignment in every direction is the
// compile-time proof, and a defined type anywhere would fail to build. That is
// the docs/00 §R property — one locked-memory implementation in the portfolio —
// expressed as a test rather than as a promise.
// The compile-time half, in the blank-identifier form Go uses for this. These
// assignments only build if all three names denote ONE type; a defined type
// anywhere in the chain fails the build here rather than at a call site.
var (
	_ *MemoryEnclave           = (*goenclave.MemoryEnclave)(nil)
	_ *ProtectedBuffer         = (*goenclave.MemoryEnclave)(nil)
	_ *goenclave.MemoryEnclave = (*MemoryEnclave)(nil)
)

func TestMemoryEnclaveIsGoEnclaves(t *testing.T) {
	src := mustSized(t, 32)
	defer src.Destroy()

	// The runtime half is deliberately thin. Identity is a COMPILE-TIME property
	// and the block above is where it is proven; writing the same assignments
	// again inside the test would be no-ops that lint correctly flags as
	// unnecessary conversions — because with real aliases that is exactly what
	// they are. What is left worth asserting at runtime is that an enclave
	// constructed through go-enclave behaves through this package's name.
	if src.Size() != 32 {
		t.Fatalf("Size() = %d, want 32", src.Size())
	}
}

// TestErrEnclaveDestroyedIsGoEnclaves pins that the destroyed sentinel is
// go-enclave's, re-exported rather than redefined — so errors.Is matches whether
// the error arrived through this package or straight from go-enclave. Two
// sentinels meaning the same thing is how a caller ends up handling one and not
// the other.
func TestErrEnclaveDestroyedIsGoEnclaves(t *testing.T) {
	if !errors.Is(ErrEnclaveDestroyed, goenclave.ErrEnclaveDestroyed) {
		t.Fatal("ErrEnclaveDestroyed is not go-enclave's sentinel")
	}

	e := mustSized(t, 16)
	e.Destroy()
	if err := e.WithBytes(func([]byte) error { return nil }); !errors.Is(err, ErrEnclaveDestroyed) {
		t.Fatalf("WithBytes after Destroy = %v, want ErrEnclaveDestroyed", err)
	}
}

// --- ProtectedKey ---

// TestProtectedKey_OpenSeal verifies a key sealed at rest opens into locked
// memory holding exactly what went in.
//
// It is also the regression test for the freeze trap the adoption had to cross:
// memguard's Enclave.Open FREEZES the buffer it returns, and
// goenclave.NewMemoryEnclave wipes its source, so a bridge that forgot to Melt
// first would fault the process here rather than fail an assertion.
func TestProtectedKey_OpenSeal(t *testing.T) {
	key := []byte("a-protected-key-of-thirty-two-by")
	want := append([]byte(nil), key...)

	pk := NewProtectedKey(key)
	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer buf.Destroy()

	if !bytes.Equal(encBytes(t, buf), want) {
		t.Fatalf("opened key = %x, want %x", encBytes(t, buf), want)
	}
}

// TestProtectedKey_MultipleOpens verifies every Open yields an independent
// enclave with the same material, and that destroying one does not affect
// another.
func TestProtectedKey_MultipleOpens(t *testing.T) {
	want := []byte("multiple-opens-must-agree-32byte")
	pk := NewProtectedKey(append([]byte(nil), want...))

	first, err := pk.Open()
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	second, err := pk.Open()
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer second.Destroy()

	if !bytes.Equal(encBytes(t, first), want) || !bytes.Equal(encBytes(t, second), want) {
		t.Fatal("opened enclaves do not both hold the key")
	}

	// Independent: destroying the first must leave the second intact.
	first.Destroy()
	if !bytes.Equal(encBytes(t, second), want) {
		t.Fatal("destroying one opened enclave damaged another")
	}
}

// TestProtectedKey_OpenNilEnclave verifies Open reports ErrEnclaveNil rather
// than dereferencing a nil enclave.
func TestProtectedKey_OpenNilEnclave(t *testing.T) {
	pk := &ProtectedKey{}
	_, err := pk.Open()
	if !errors.Is(err, ErrEnclaveNil) {
		t.Fatalf("Open on an uninitialized key = %v, want ErrEnclaveNil", err)
	}
}

// TestProtectedKey_ClearDropsEnclaveReference verifies Clear revokes the ability
// to open the key.
func TestProtectedKey_ClearDropsEnclaveReference(t *testing.T) {
	pk := NewProtectedKey([]byte("a-key-that-is-about-to-be-cleard"))

	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open before Clear: %v", err)
	}
	buf.Destroy()

	pk.Clear()
	if _, err := pk.Open(); !errors.Is(err, ErrEnclaveNil) {
		t.Fatalf("Open after Clear = %v, want ErrEnclaveNil", err)
	}
}

// TestProtectedKey_ClearAlreadyClearedNoPanic verifies Clear is idempotent.
func TestProtectedKey_ClearAlreadyClearedNoPanic(t *testing.T) {
	pk := NewProtectedKey([]byte("idempotent-clear-key-of-32-bytes"))
	pk.Clear()
	pk.Clear()
	if _, err := pk.Open(); !errors.Is(err, ErrEnclaveNil) {
		t.Fatalf("Open after double Clear = %v, want ErrEnclaveNil", err)
	}
}

// TestProtectedKey_ConcurrentOpenClear verifies Open and Clear may race without
// a torn read: every Open either succeeds or reports ErrEnclaveNil, and the race
// detector stays quiet.
func TestProtectedKey_ConcurrentOpenClear(t *testing.T) {
	pk := NewProtectedKey([]byte("concurrent-open-and-clear-32byte"))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				buf, err := pk.Open()
				if err != nil {
					if !errors.Is(err, ErrEnclaveNil) {
						t.Errorf("Open = %v, want success or ErrEnclaveNil", err)
					}
					continue
				}
				buf.Destroy()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		pk.Clear()
	}()
	wg.Wait()
}

// TestNewProtectedKey_WipesSource verifies the caller's slice is wiped once the
// key has been sealed, so the material exists in exactly one place.
func TestNewProtectedKey_WipesSource(t *testing.T) {
	source := make([]byte, 32)
	for i := range source {
		source[i] = byte(i + 1)
	}

	_ = NewProtectedKey(source)

	for i, b := range source {
		if b != 0 {
			t.Fatalf("source byte %d = %#x after NewProtectedKey, want 0", i, b)
		}
	}
}

// --- SealMemoryEnclave ---

// TestSealMemoryEnclave verifies the enclave-to-key transfer: the material
// survives, and the source enclave is consumed.
func TestSealMemoryEnclave(t *testing.T) {
	want := []byte("seal-this-enclave-into-a-key-!!!")
	src := mustFromBytes(t, append([]byte(nil), want...))

	pk := SealMemoryEnclave(src)
	if pk == nil {
		t.Fatal("SealMemoryEnclave returned nil for a live enclave")
	}
	if err := src.WithBytes(func([]byte) error { return nil }); !errors.Is(err, ErrEnclaveDestroyed) {
		t.Fatal("the source enclave was not consumed")
	}

	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer buf.Destroy()
	if !bytes.Equal(encBytes(t, buf), want) {
		t.Fatalf("sealed key = %x, want %x", encBytes(t, buf), want)
	}
}

// TestSealMemoryEnclave_NilAndDestroyed verifies the refusals are nil returns
// rather than panics.
func TestSealMemoryEnclave_NilAndDestroyed(t *testing.T) {
	if SealMemoryEnclave(nil) != nil {
		t.Fatal("SealMemoryEnclave(nil) should be nil")
	}

	dead := mustSized(t, 16)
	dead.Destroy()
	if SealMemoryEnclave(dead) != nil {
		t.Fatal("SealMemoryEnclave on a destroyed enclave should be nil")
	}
}

// TestSealMemoryEnclave_ConsumedEnclaveReturnsNil verifies the documented
// once-only contract: a second seal of the same enclave gets nil.
func TestSealMemoryEnclave_ConsumedEnclaveReturnsNil(t *testing.T) {
	src := mustFromBytes(t, []byte("consume-me-exactly-once-32-bytes"))
	if pk := SealMemoryEnclave(src); pk == nil {
		t.Fatal("first SealMemoryEnclave returned nil")
	}
	if pk := SealMemoryEnclave(src); pk != nil {
		t.Fatal("second SealMemoryEnclave should return nil for a consumed enclave")
	}
}

// TestSealMemoryEnclaveAndDeprecatedAliasAgree verifies the deprecated wrapper
// is the same function, not a divergent copy.
func TestSealMemoryEnclaveAndDeprecatedAliasAgree(t *testing.T) {
	for name, seal := range map[string]func(*MemoryEnclave) *ProtectedKey{
		"SealMemoryEnclave":   SealMemoryEnclave,
		"SealProtectedBuffer": SealProtectedBuffer,
	} {
		t.Run(name, func(t *testing.T) {
			want := []byte("both-spellings-must-behave-alike")
			src := mustFromBytes(t, append([]byte(nil), want...))
			pk := seal(src)
			if pk == nil {
				t.Fatalf("%s returned nil", name)
			}
			buf, err := pk.Open()
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer buf.Destroy()
			if !bytes.Equal(encBytes(t, buf), want) {
				t.Fatalf("%s lost the material", name)
			}
		})
	}
}

// --- Errors ---

// TestErrEnclaveNil_Message verifies the sentinel's message.
func TestErrEnclaveNil_Message(t *testing.T) {
	if want := "memprotect: enclave is nil"; ErrEnclaveNil.Error() != want {
		t.Fatalf("ErrEnclaveNil = %q, want %q", ErrEnclaveNil.Error(), want)
	}
}

// TestNilError_TypedError verifies errors.As reaches the concrete type, so a
// caller can distinguish this from any other nil-ish failure.
func TestNilError_TypedError(t *testing.T) {
	var err error = ErrEnclaveNil
	var target *NilError
	if !errors.As(err, &target) {
		t.Fatal("errors.As failed for *NilError")
	}
	if !errors.Is(err, ErrEnclaveNil) {
		t.Fatal("errors.Is failed for ErrEnclaveNil")
	}
}

// TestOpenError_FormatAndUnwrap verifies the wrapper names its origin and
// exposes the cause.
func TestOpenError_FormatAndUnwrap(t *testing.T) {
	inner := errors.New("underlying failure")
	e := &OpenError{Err: inner}

	if want := "memprotect: failed to open enclave: underlying failure"; e.Error() != want {
		t.Fatalf("Error() = %q, want %q", e.Error(), want)
	}
	if !errors.Is(e, inner) {
		t.Fatal("errors.Is did not reach the wrapped cause")
	}
}

// --- WipeBytes ---

// TestWipeBytes verifies the re-exported wipe zeroes its argument and tolerates
// an empty slice.
func TestWipeBytes(t *testing.T) {
	b := []byte("material-that-must-be-zeroed!!!!")
	WipeBytes(b)
	for i, c := range b {
		if c != 0 {
			t.Fatalf("byte %d = %#x after WipeBytes, want 0", i, c)
		}
	}
	WipeBytes(nil)
	WipeBytes([]byte{})
}

// --- Process disposition ---

// TestNoSignalHandlerInstalled verifies nothing in this package resets the
// process signal disposition. memguard.CatchSignal calls signal.Reset() with no
// arguments, which would silently steal every handler the embedding application
// installed; this package must never call it. memguard is still linked in for
// ProtectedKey, so the guard is still needed.
func TestNoSignalHandlerInstalled(t *testing.T) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGUSR1)
	defer signal.Stop(sigC)

	pk := NewProtectedKey([]byte("signal-disposition-must-survive!"))
	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf.Destroy()
	pk.Clear()

	e := mustSized(t, 32)
	e.Destroy()

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("raising SIGUSR1: %v", err)
	}
	select {
	case got := <-sigC:
		if got != syscall.SIGUSR1 {
			t.Fatalf("received %v, want SIGUSR1", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SIGUSR1 was never delivered: something in pkg/enclave reset the process signal handlers " +
			"(memguard.CatchSignal calls signal.Reset() with no arguments)")
	}
}

// TestProtectedKey_BridgeUnderConcurrentMemguardWork hammers the
// memguard-to-go-enclave bridge in ProtectedKey.Open while other goroutines do
// the memguard work that can reach core.Panic — NewProtectedKey allocates, and
// SealMemoryEnclave allocates, seals and destroys. Every opened enclave must
// hold the exact key, and the race detector must stay quiet.
//
// WHAT THIS PROVES, AND WHAT IT DOES NOT, stated plainly because the gap is the
// point. It exercises the bridge concurrently with every other memguard caller
// in this package, so an unsynchronized access between them shows up here under
// -race. It does NOT prove the property the lock actually exists for: that a
// process-wide memguard PURGE cannot unmap the buffer mid-copy. Reaching a purge
// means driving the process to RLIMIT_MEMLOCK exhaustion, and memguard answers
// that by panicking after purging — a test that triggers it would take the test
// binary down rather than report, and would destroy every unrelated enclave in
// the process on its way. There is no seam in memguard v0.23.0 to inject one.
//
// So the purge invariant is carried by the argument at the lock, not by this
// test. That is a real coverage limit and is why the reasoning is written out
// there in full rather than summarized. It disappears with memguard itself when
// ProtectedKey moves to the go-enclave sealing seam (go-enclave #22).
func TestProtectedKey_BridgeUnderConcurrentMemguardWork(t *testing.T) {
	want := []byte("bridge-under-concurrent-pressure")
	pk := NewProtectedKey(append([]byte(nil), want...))

	var wg sync.WaitGroup

	// Readers: drive the bridge.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				buf, err := pk.Open()
				if err != nil {
					t.Errorf("Open: %v", err)
					return
				}
				if err := buf.WithBytes(func(got []byte) error {
					if !bytes.Equal(got, want) {
						return errors.New("opened enclave does not hold the key")
					}
					return nil
				}); err != nil {
					t.Error(err)
				}
				buf.Destroy()
			}
		}()
	}

	// Allocators: the memguard traffic the bridge has to survive.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				other := NewProtectedKey([]byte("unrelated-key-material-32-bytes!"))
				other.Clear()

				src := mustFromBytes(t, []byte("unrelated-enclave-to-be-sealed!!"))
				if sealed := SealMemoryEnclave(src); sealed == nil {
					t.Error("SealMemoryEnclave returned nil for a live enclave")
					return
				}
			}
		}()
	}

	wg.Wait()
}

// TestSealMemoryEnclave_ConcurrentConsumeExactlyOnce pins the consume-once
// contract against CONCURRENT callers, which is the case the sequential test
// above cannot reach.
//
// It is a regression test for a real defect, not a hypothetical: the first
// go-enclave port sealed inside a WithBytes borrow, and a borrow is the READ
// side — it admits concurrent callers by design. Two goroutines could both copy
// the plaintext and both return a distinct non-nil ProtectedKey, duplicating key
// material and leaving the documented "ownership transfers" contract false.
//
// Exactly one caller must win and the other must get nil, every round.
func TestSealMemoryEnclave_ConcurrentConsumeExactlyOnce(t *testing.T) {
	const rounds = 200

	for round := 0; round < rounds; round++ {
		src := mustFromBytes(t, []byte("exactly-one-caller-may-consume!!"))

		const callers = 4
		results := make([]*ProtectedKey, callers)
		start := make(chan struct{})
		var wg sync.WaitGroup

		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start // release them together, to make the collision likely
				results[i] = SealMemoryEnclave(src)
			}(i)
		}
		close(start)
		wg.Wait()

		won := 0
		var winner *ProtectedKey
		for _, r := range results {
			if r != nil {
				won++
				winner = r
			}
		}
		if won != 1 {
			t.Fatalf("round %d: %d of %d concurrent SealMemoryEnclave calls succeeded, want exactly 1",
				round, won, callers)
		}

		// The winner must hold the real material, not a torn or empty copy.
		buf, err := winner.Open()
		if err != nil {
			t.Fatalf("round %d: Open on the winning key: %v", round, err)
		}
		if !bytes.Equal(encBytes(t, buf), []byte("exactly-one-caller-may-consume!!")) {
			t.Fatalf("round %d: the winning key does not hold the source material", round)
		}
		buf.Destroy()
	}
}
