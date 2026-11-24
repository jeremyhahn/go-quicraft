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
	"errors"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// Every read of locked memory in this package is a borrow scoped to the code
// that uses the bytes: go-enclave hands out no view that outlives a callback.
// These tests pin the two things that scoping is for — a borrow ends when its
// callback returns, so the enclave can be frozen or destroyed straight after,
// and the scratches split and combine borrow are all released by the time they
// return.

// TestBorrow_EndsWithTheCallback pins that a borrow holds the enclave only for
// the callback's duration. A Destroy issued from another goroutine during the
// borrow must not complete inside it, and Freeze and Destroy on the borrowing
// goroutine must go through once borrow has returned: both wait out in-flight
// borrows, so a borrow that outlived its callback deadlocks this test.
func TestBorrow_EndsWithTheCallback(t *testing.T) {
	e, err := enclave.NewMemorySized(32)
	if err != nil {
		t.Skipf("could not allocate locked memory: %v", err)
	}

	destroyed := make(chan error, 1)
	if err := borrow(e, "test borrow", func(b []byte) error {
		copy(b, "written-inside-the-borrow")
		go func() { destroyed <- e.Destroy() }()
		select {
		case err := <-destroyed:
			t.Errorf("Destroy completed inside the borrow (err = %v): the borrow did not hold the enclave", err)
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("borrow: %v", err)
	}

	if err := <-destroyed; err != nil {
		t.Fatalf("Destroy after the borrow returned: %v", err)
	}
	if e.IsAlive() {
		t.Fatal("enclave still alive after Destroy returned")
	}
}

// TestBorrow_RefusedBorrowIsProtectedMemoryError pins the refusal path: a
// destroyed enclave is never handed to the callback, and the refusal is a
// *ProtectedMemoryError naming the operation that still unwraps to go-enclave's
// sentinel.
func TestBorrow_RefusedBorrowIsProtectedMemoryError(t *testing.T) {
	e, err := enclave.NewMemorySized(8)
	if err != nil {
		t.Skipf("could not allocate locked memory: %v", err)
	}
	if err := e.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}

	ran := false
	err = borrow(e, "opening scratch", func([]byte) error {
		ran = true
		return nil
	})
	if ran {
		t.Fatal("the callback ran on a destroyed enclave")
	}
	var pmErr *ProtectedMemoryError
	if !errors.As(err, &pmErr) || pmErr.Op != "opening scratch" {
		t.Fatalf("err = %v, want a *ProtectedMemoryError with Op %q", err, "opening scratch")
	}
	if !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Fatalf("err = %v, want it to wrap enclave.ErrEnclaveDestroyed", err)
	}
}

// TestBorrow_CallbackErrorIsNotWrapped pins that an error produced INSIDE the
// borrow reaches the caller as itself. SplitBytes and CombineBytes return typed
// *SplitError and *CombineError values from inside their borrows; wrapping them
// in a locked-memory failure would misreport a malformed share as a refused
// lock.
func TestBorrow_CallbackErrorIsNotWrapped(t *testing.T) {
	e, err := enclave.NewMemorySized(8)
	if err != nil {
		t.Skipf("could not allocate locked memory: %v", err)
	}
	defer release(e)

	want := &CombineError{Reason: "malformed share"}
	err = borrow(e, "opening decode buffer", func([]byte) error { return want })
	if err != want {
		t.Fatalf("err = %#v, want the callback's own *CombineError", err)
	}
	var pmErr *ProtectedMemoryError
	if errors.As(err, &pmErr) {
		t.Fatalf("a callback error was wrapped as a *ProtectedMemoryError: %v", err)
	}
}

// TestSplitProtectedWith_ReleasesEveryScratchOnReturn pins that the coefficient
// pool, coefficient vector and payload buffer — each borrowed for the whole of
// share evaluation, and each released when its own borrow scope ends — are all
// gone once the split returns. The allocator's charge must then be exactly the
// shares' values.
func TestSplitProtectedWith_ReleasesEveryScratchOnReturn(t *testing.T) {
	a := newBudget(t, 64)
	shares, err := SplitProtectedWith(a, []byte("0123456789abcdef0123456789abcdef"), 3, 5)
	if err != nil {
		t.Fatalf("SplitProtectedWith: %v", err)
	}

	var want int64
	for _, s := range shares {
		want += enclave.LockedSize(len(valueCopy(s)))
	}
	if got := a.InUse(); got != want {
		destroyAll(shares)
		t.Fatalf("InUse after split = %d, want %d (the shares alone): a scratch outlived its borrow", got, want)
	}

	destroyAll(shares)
	if got := a.InUse(); got != 0 {
		t.Fatalf("InUse after destroying every share = %d, want 0", got)
	}
}

// TestCombineProtectedWith_ReleasesTheDecodeBufferOnReturn is the combine half:
// the decode buffer is borrowed across both passes, and only the reconstructed
// secret may still be charged once combine returns.
func TestCombineProtectedWith_ReleasesTheDecodeBufferOnReturn(t *testing.T) {
	a := newBudget(t, 64)
	secret := []byte("0123456789abcdef0123456789abcdef")
	shares, err := SplitProtectedWith(a, secret, 3, 5)
	if err != nil {
		t.Fatalf("SplitProtectedWith: %v", err)
	}
	defer destroyAll(shares)
	before := a.InUse()

	got, err := CombineProtectedWith(a, shares[:3])
	if err != nil {
		t.Fatalf("CombineProtectedWith: %v", err)
	}
	if inUse, want := a.InUse(), before+enclave.LockedSize(got.Size()); inUse != want {
		release(got)
		t.Fatalf("InUse after combine = %d, want %d (shares plus the secret): the decode buffer outlived its borrow", inUse, want)
	}
	if eq, err := got.EqualTo(secret); err != nil || !eq {
		release(got)
		t.Fatalf("reconstructed secret EqualTo = %v, %v; want true, nil", eq, err)
	}

	release(got)
	if inUse := a.InUse(); inUse != before {
		t.Fatalf("InUse after destroying the secret = %d, want %d", inUse, before)
	}
}
