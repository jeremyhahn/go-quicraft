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
func enclaveProtection(t *testing.T, e enclave.ByteAccess) (lockedmem.Protection, error) {
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

// TestUnsealRootKeyProtected_ReturnsFrozenPages checks with the kernel that the unsealed root key is mapped read-only.
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

// TestInitializeRootKeyEnclaveStaysWritable: Initialize's root-key enclave stays writable because enclave.NewSealed wipes it.
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

// TestRootKeyProtected_ReturnsFrozenPages checks with the kernel that RootKeyProtected returns go-enclave's frozen enclave.
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
