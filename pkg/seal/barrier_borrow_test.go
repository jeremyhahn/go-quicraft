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

package seal

import (
	"bytes"
	"context"
	"errors"
	"testing"

	goenclave "github.com/jeremyhahn/go-enclave"
	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

// errLockRefused stands in for the kernel refusing an mlock.
var errLockRefused = errors.New("mlock refused")

// plainStrategy exposes ONLY the SealingStrategy method set.
//
// It is a hand-written delegation rather than an embedding on purpose: an
// embedded *SoftwareStrategy would promote SealRootKeyProtected and
// UnsealRootKeyProtected too, and the type would silently satisfy
// ProtectedSealingStrategy — which is exactly the thing these tests must prove
// the barrier does not require. Go interfaces are structural, so the only way to
// assert "implements one and not the other" is to build a type that genuinely
// has one method set.
type plainStrategy struct {
	inner *SoftwareStrategy
	// lastUnsealed retains the exact slice header UnsealRootKey handed back, so
	// a test can inspect that memory after Unseal has returned.
	lastUnsealed []byte
}

func (p *plainStrategy) ID() StrategyID       { return p.inner.ID() }
func (p *plainStrategy) Available() bool      { return p.inner.Available() }
func (p *plainStrategy) HardwareBacked() bool { return p.inner.HardwareBacked() }
func (p *plainStrategy) Close() error         { return p.inner.Close() }

func (p *plainStrategy) SealRootKey(ctx context.Context, rootKey []byte, creds Credentials) (*SealedRootKey, error) {
	return p.inner.SealRootKey(ctx, rootKey, creds)
}

func (p *plainStrategy) UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error) {
	out, err := p.inner.UnsealRootKey(ctx, sealed, creds)
	p.lastUnsealed = out
	return out, err
}

func (p *plainStrategy) BarrierEncryptor(ctx context.Context, rootKey []byte) (SymmetricEncrypter, error) {
	return p.inner.BarrierEncryptor(ctx, rootKey)
}

// Compile-time proof of the split this file depends on.
var (
	_ SealingStrategy = (*plainStrategy)(nil)
	// plainStrategy must NOT satisfy ProtectedSealingStrategy. There is no
	// negative assertion in the type system, so the guard is the delegation
	// above plus this note: adding an embedded *SoftwareStrategy here would
	// break TestBarrier_WorksWithAPlainSealingStrategy's meaning without
	// breaking its build.
	_ = ProtectedSealingStrategy(nil)
)

func newPlainStrategy(t *testing.T) *plainStrategy {
	t.Helper()
	return &plainStrategy{inner: testStrategy(t)}
}

// TestBarrier_WorksWithAPlainSealingStrategy is #39's explicit requirement: the
// barrier's borrow rewrite must not quietly start depending on
// ProtectedSealingStrategy, because most strategies in the portfolio implement
// only the []byte pair — go-xkms's TPM2, PKCS#11 and cloud-KMS strategies, and
// go-qrdb's shamir and recovery strategies.
//
// Initialize now generates the root key into locked memory and Unseal copies the
// strategy's return into locked memory, but BOTH still hand the strategy an
// ordinary []byte, because SealingStrategy is deliberately out of this issue's
// scope. This test is what holds that line.
func TestBarrier_WorksWithAPlainSealingStrategy(t *testing.T) {
	s := newPlainStrategy(t)
	defer s.Close()

	b := MustNewBarrier(testBarrierConfig(1))
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize with a plain SealingStrategy: %v", err)
	}

	plaintext := []byte("round-trips through a plain strategy")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	sealedBlob := b.SealedRootKeyData()
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	b2 := MustNewBarrier(testBarrierConfig(1))
	if err := b2.SetSealedRootKey(sealedBlob); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b2.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal with a plain SealingStrategy: %v", err)
	}

	got, err := b2.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt after Unseal: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round trip = %q, want %q", got, plaintext)
	}
}

// TestUnseal_WipesTheStrategysHeapKey pins the one place the root key still
// reaches the ordinary heap, and how briefly.
//
// SealingStrategy.UnsealRootKey returns a plain []byte and is not moving (#39
// scope), so on the unseal path the key does exist outside protected memory.
// What the rewrite controls is the WINDOW: the slice is copied into locked pages
// and wiped immediately, instead of staying live across MAC verification,
// encryptor resolution, the load-generation claim and one DEK derivation per
// known epoch — nine hand-written wipes' worth of error paths.
//
// The strategy retains the exact slice header it returned, so this asserts on
// the same backing array the barrier was handed.
func TestUnseal_WipesTheStrategysHeapKey(t *testing.T) {
	s := newPlainStrategy(t)
	defer s.Close()

	b := MustNewBarrier(testBarrierConfig(1))
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sealedBlob := b.SealedRootKeyData()

	b2 := MustNewBarrier(testBarrierConfig(1))
	if err := b2.SetSealedRootKey(sealedBlob); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b2.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}

	if len(s.lastUnsealed) != rootKeySize {
		t.Fatalf("the strategy returned %d bytes, want %d — the fixture is not "+
			"observing the real unseal path", len(s.lastUnsealed), rootKeySize)
	}
	for i, c := range s.lastUnsealed {
		if c != 0 {
			t.Fatalf("byte %d of the strategy's heap key = %#x after Unseal "+
				"returned, want 0: the root key is still readable on the "+
				"ordinary heap", i, c)
		}
	}

	// And the barrier really did unseal from it, so the wipe is not passing by
	// virtue of the key never having been used.
	if b2.IsSealed() {
		t.Fatal("barrier is still sealed, so the wipe assertion proves nothing")
	}
}

// TestInitialize_LockedMemoryRefusalIsClean pins what happens when the kernel
// will not give Initialize its root-key enclave.
//
// The interesting property is not the error, it is the STATE: a barrier whose
// Initialize failed must stay uninitialized, so a retry after the operator has
// raised RLIMIT_MEMLOCK behaves like a first boot rather than hitting
// ErrBarrierAlreadyInit against a half-built barrier.
func TestInitialize_LockedMemoryRefusalIsClean(t *testing.T) {
	s := newPlainStrategy(t)
	defer s.Close()

	b := MustNewBarrier(testBarrierConfig(1))
	b.lockRootKey = func(int) (*enclave.MemoryEnclave, error) { return nil, errLockRefused }

	err := b.Initialize(context.Background(), s, Credentials{})
	if err == nil {
		t.Fatal("Initialize must fail when locked memory is refused")
	}
	if !errors.Is(err, errLockRefused) {
		t.Fatalf("error = %v, want it to wrap the allocator's refusal", err)
	}

	if b.IsInitialized() {
		t.Fatal("the barrier reports itself initialized after a failed Initialize")
	}
	if !b.IsSealed() {
		t.Fatal("the barrier is not sealed after a failed Initialize")
	}

	// And a retry once the allocator recovers must behave like a first boot.
	b.lockRootKey = goenclave.NewMemoryEnclaveSized
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("retry after the refusal cleared: %v", err)
	}
	if !b.IsInitialized() {
		t.Fatal("the retry did not initialize the barrier")
	}
}

// TestUnseal_LockedMemoryRefusalStillWipesTheHeapKey is the same failure on the
// unseal path, where it matters more: the strategy has ALREADY returned the root
// key on the ordinary heap by the time the enclave is asked for. A refusal that
// returned early without wiping would leave the root key readable in the heap of
// a process that is still running — and running in exactly the degraded state
// (locked memory exhausted) where a core dump is most likely.
func TestUnseal_LockedMemoryRefusalStillWipesTheHeapKey(t *testing.T) {
	s := newPlainStrategy(t)
	defer s.Close()

	b := MustNewBarrier(testBarrierConfig(1))
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	blob := b.SealedRootKeyData()

	b2 := MustNewBarrier(testBarrierConfig(1))
	if err := b2.SetSealedRootKey(blob); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	b2.lockRootKey = func(int) (*enclave.MemoryEnclave, error) { return nil, errLockRefused }

	err := b2.Unseal(context.Background(), s, Credentials{})
	if err == nil {
		t.Fatal("Unseal must fail when locked memory is refused")
	}
	if !errors.Is(err, errLockRefused) {
		t.Fatalf("error = %v, want it to wrap the allocator's refusal", err)
	}
	if !b2.IsSealed() {
		t.Fatal("the barrier is not sealed after a failed Unseal")
	}

	if len(s.lastUnsealed) != rootKeySize {
		t.Fatalf("the strategy returned %d bytes, want %d", len(s.lastUnsealed), rootKeySize)
	}
	for i, c := range s.lastUnsealed {
		if c != 0 {
			t.Fatalf("byte %d of the strategy's heap key = %#x after a refused "+
				"allocation: the root key was left readable on the heap on the "+
				"error path", i, c)
		}
	}
}
