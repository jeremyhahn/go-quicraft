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

	"github.com/jeremyhahn/go-quicraft/test/lockedmem"
)

// TestUnsealRootKeyProtected_ReturnsFrozenPages pins the property #54 stage two
// changed the mechanism of and must not have lost.
//
// UnsealRootKeyProtected's contract is that it hands back a key to READ and
// destroy, not a buffer to edit: a stray write through a retained slice must
// fault rather than silently corrupt the root key. It used to get that by
// decrypting into a sized enclave and MOVING the result into a second,
// from-bytes one — two locked regions and a copy — because go-enclave had no
// post-fill freeze. It now decrypts into one region and calls Freeze on it.
//
// A missing Freeze is invisible to every other assertion in this package: the
// key still round-trips, Size and EqualTo still agree, and the only difference
// is a page protection nothing observes until something writes. So this asks the
// KERNEL what it believes about the page, which is the only witness that cannot
// be fooled by the library's own bookkeeping.
//
// Verified by mutation on 2026-09-03: deleting the `out.Freeze()` call in
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

	// Open, not WithBytes: the address is all that is wanted, and ProtectionOf
	// never dereferences it. The enclave is owned by this goroutine alone, which
	// is the condition Open documents.
	view, err := out.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	prot, err := lockedmem.ProtectionOf(view)
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

	view, err := e.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	prot, err := lockedmem.ProtectionOf(view)
	if err != nil {
		t.Skipf("this kernel cannot report mapping protections: %v", err)
	}
	if prot.ReadOnly() {
		t.Fatalf("the root-key enclave is mapped %q: Initialize fills it in place "+
			"and hands it to enclave.NewSealed, which WIPES it — a wipe of frozen "+
			"pages faults the process", prot)
	}
}
