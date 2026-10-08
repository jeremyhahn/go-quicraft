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

//go:build linux

package seal

import (
	"context"
	"testing"

	"github.com/jeremyhahn/go-enclave"
	"github.com/jeremyhahn/go-quicraft/test/lockedmem"
)

// enclaveProtection asks the kernel how e's pages are mapped. The address is
// read inside a borrow, so the view never outlives it; ProtectionOf never
// dereferences the slice, only locates its mapping.
func enclaveProtection(t *testing.T, e *enclave.MemoryEnclave) (lockedmem.Protection, error) {
	t.Helper()
	var (
		prot lockedmem.Protection
		perr error
	)
	if err := e.WithBytes(func(view []byte) error {
		prot, perr = lockedmem.ProtectionOf(view)
		return nil
	}); err != nil {
		t.Fatalf("WithBytes: %v", err)
	}
	return prot, perr
}

// TestUnsealRootKeyProtected_ReturnsFrozenPages pins that the unsealed root key
// is returned in frozen (read-only) pages.
//
// UnsealRootKeyProtected's contract is that it hands back a key to READ and
// destroy, not a buffer to edit: a stray write through a retained slice must
// fault rather than silently corrupt the root key. It decrypts into one sized
// region and calls Freeze on it — one locked region and no copy.
//
// A missing Freeze is invisible to every other assertion in this package: the
// key still round-trips, Size and EqualTo still agree, and the only difference
// is a page protection nothing observes until something writes. So this asks the
// KERNEL what it believes about the page, which is the only witness that cannot
// be fooled by the library's own bookkeeping.
//
// Verified by mutation: deleting the `out.Freeze()` call in
// UnsealRootKeyProtected fails here with
//
//	strategy_freeze_linux_test.go:81: the unsealed root key is mapped "rw-p":
//	UnsealRootKeyProtected returned a WRITABLE region, so a stray write
//	corrupts the root key instead of faulting
func TestUnsealRootKeyProtected_ReturnsFrozenPages(t *testing.T) {
	ctx := context.Background()
	s, err := newTestStrategy([]byte("a-freeze-verification-passphrase"))
	if err != nil {
		t.Fatalf("newTestStrategy: %v", err)
	}
	defer s.Close()

	rootKey, _ := protectedRootKey(t)

	sealed, err := s.SealRootKeyProtected(ctx, rootKey, Credentials{})
	if err != nil {
		t.Fatalf("SealRootKeyProtected: %v", err)
	}

	out, err := s.UnsealRootKeyProtected(ctx, sealed, Credentials{})
	if err != nil {
		t.Fatalf("UnsealRootKeyProtected: %v", err)
	}
	defer out.Destroy()

	prot, err := enclaveProtection(t, out)
	if err != nil {
		t.Skipf("this kernel cannot report mapping protections: %v", err)
	}
	if !prot.ReadOnly() {
		t.Fatalf("the unsealed root key is mapped %q: UnsealRootKeyProtected "+
			"returned a WRITABLE region, so a stray write corrupts the root key "+
			"instead of faulting", prot)
	}
}

// TestInitializeRootKeyEnclaveStaysWritable is the deliberate counter-case, and
// it is here so that "freeze everything" cannot be applied as a rule.
//
// The enclave Barrier.Initialize holds the root key in is filled in place and
// its LAST act is to be consumed by enclave.NewSealed, which wipes what it is
// handed. A wipe of frozen pages faults the process, so freezing that one would
// trade a hypothetical stray-write fault for a certain fault on the happy path.
// The barrier's own comments say so; this is the executable half.
func TestInitializeRootKeyEnclaveStaysWritable(t *testing.T) {
	b := MustNewBarrier(testBarrierConfig(1))
	e, err := b.lockRootKey(rootKeySize)
	if err != nil {
		t.Skipf("could not allocate locked memory: %v", err)
	}
	defer e.Destroy()

	prot, err := enclaveProtection(t, e)
	if err != nil {
		t.Skipf("this kernel cannot report mapping protections: %v", err)
	}
	if prot.ReadOnly() {
		t.Fatalf("the root-key enclave is mapped %q: Initialize fills it in place "+
			"and hands it to enclave.NewSealed, which WIPES it — a wipe of frozen "+
			"pages faults the process", prot)
	}
}

// TestRootKeyProtected_ReturnsFrozenPages is the same property on the barrier's
// own accessor.
//
// This accessor hands a caller a key to read and destroy, exactly as
// UnsealRootKeyProtected above does, so its result must be frozen. The cost of a
// writable result is not hypothetical: a consumer that retains the borrowed
// slice past its window — the precise bug a frozen region turns into a fault —
// rewrites the root key it is about to derive a wrapping key from, every read
// succeeds, and the corruption surfaces as an unexplained ErrDecryptionFailed
// somewhere else entirely.
//
// The freeze is go-enclave's: SealedEnclave.Open returns a frozen enclave, and
// fails the open closed when the kernel refuses the freeze, so RootKeyProtected
// returns Open's result as it is. That makes this test the guard on a property
// this package takes from go-enclave rather than establishing itself: a
// go-enclave release that went back to writable plaintext, or an accessor that
// stopped returning Open's enclave and built a writable one instead, fails here.
//
// The kernel is the witness, for the reason given at the sibling above: a
// writable region is invisible to every other assertion, since the key still
// round-trips and only a page protection differs.
func TestRootKeyProtected_ReturnsFrozenPages(t *testing.T) {
	s := testStrategy(t)
	defer s.Close()

	b := MustNewBarrier(testBarrierConfig(1))
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	got, err := b.RootKeyProtected(context.Background())
	if err != nil {
		t.Fatalf("RootKeyProtected: %v", err)
	}
	defer got.Destroy()

	prot, err := enclaveProtection(t, got)
	if err != nil {
		t.Skipf("this kernel cannot report mapping protections: %v", err)
	}
	if !prot.ReadOnly() {
		t.Fatalf("RootKeyProtected returned a region mapped %q: a write through a "+
			"slice retained past the borrow rewrites the root key instead of "+
			"faulting", prot)
	}

	// The freeze must not have cost the caller the key itself. A frozen region
	// that reads as zeroes would satisfy the assertion above and be useless.
	plain, err := b.RootKey(context.Background())
	if err != nil {
		t.Fatalf("RootKey: %v", err)
	}
	defer enclave.Wipe(plain)
	if eq, err := got.EqualTo(plain); err != nil {
		t.Fatalf("EqualTo: %v", err)
	} else if !eq {
		t.Fatal("the frozen enclave does not hold the barrier's root key")
	}
}
