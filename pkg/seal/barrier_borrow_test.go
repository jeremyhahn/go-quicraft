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

	"github.com/jeremyhahn/go-enclave"
)

// errLockRefused stands in for the kernel refusing an mlock.
var errLockRefused = errors.New("mlock refused")

// plainStrategy exposes only the SealingStrategy method set.
type plainStrategy struct {
	inner *SoftwareStrategy
	// lastUnsealed retains the exact slice header UnsealRootKey handed back, so
	// a test can inspect that memory after Unseal has returned.
	lastUnsealed []byte
	// onBarrierEncryptor, when set, runs at the start of BarrierEncryptor.
	onBarrierEncryptor func()
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
	if p.onBarrierEncryptor != nil {
		p.onBarrierEncryptor()
	}
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

// TestBarrier_WorksWithAPlainSealingStrategy verifies the barrier works without ProtectedSealingStrategy.
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

// TestUnseal_WipesTheStrategysHeapKey verifies Unseal wipes the root key the strategy returned on the heap.
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

// TestInitialize_LockedMemoryRefusalIsClean verifies Initialize fails cleanly when locked memory is refused.
func TestInitialize_LockedMemoryRefusalIsClean(t *testing.T) {
	s := newPlainStrategy(t)
	defer s.Close()

	b := MustNewBarrier(testBarrierConfig(1))
	b.lockRootKey = func(int) (enclave.ByteAccess, error) { return nil, errLockRefused }

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
	b.lockRootKey = enclave.NewMemorySized
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("retry after the refusal cleared: %v", err)
	}
	if !b.IsInitialized() {
		t.Fatal("the retry did not initialize the barrier")
	}
}

// TestUnseal_LockedMemoryRefusalStillWipesTheHeapKey verifies a locked-memory refusal on Unseal still wipes the heap key.
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
	b2.lockRootKey = func(int) (enclave.ByteAccess, error) { return nil, errLockRefused }

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

// TestUnseal_ShortCircuitedBorrowStillWipesTheHeapKey verifies a short-circuited borrow still wipes the heap key.
func TestUnseal_ShortCircuitedBorrowStillWipesTheHeapKey(t *testing.T) {
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
	// A refusal spelled as a nil enclave and a nil error. It is not a shape the
	// default allocator produces; it is the shape the borrow's own early return
	// is written to handle, and the point is that handling it must not skip the
	// wipe.
	b2.lockRootKey = func(int) (enclave.ByteAccess, error) { return nil, nil }

	err := b2.Unseal(context.Background(), s, Credentials{})
	if err == nil {
		t.Fatal("Unseal must fail when the allocator hands back no enclave")
	}
	if !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Fatalf("error = %v, want the borrow's ErrEnclaveDestroyed", err)
	}
	if !b2.IsSealed() {
		t.Fatal("the barrier is not sealed after a failed Unseal")
	}

	if len(s.lastUnsealed) != rootKeySize {
		t.Fatalf("the strategy returned %d bytes, want %d", len(s.lastUnsealed), rootKeySize)
	}
	for i, c := range s.lastUnsealed {
		if c != 0 {
			t.Fatalf("byte %d of the strategy's heap key = %#x after Unseal "+
				"returned through a short-circuited borrow: the plaintext root "+
				"key was left readable on the ordinary heap", i, c)
		}
	}
}

// TestUnseal_HeapKeyIsWipedBeforeTheRestOfUnsealRuns verifies the heap key is wiped before the rest of Unseal runs.
func TestUnseal_HeapKeyIsWipedBeforeTheRestOfUnsealRuns(t *testing.T) {
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

	var (
		observed bool
		snapshot []byte
	)
	s.onBarrierEncryptor = func() {
		observed = true
		snapshot = append([]byte(nil), s.lastUnsealed...)
	}
	if err := b2.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	s.onBarrierEncryptor = nil

	// Without this the test would pass vacuously if the callback stopped being
	// reached — which is exactly how the end-state test lost its teeth.
	if !observed {
		t.Fatal("BarrierEncryptor was never called, so nothing was observed")
	}
	if len(snapshot) != rootKeySize {
		t.Fatalf("observed %d bytes, want %d", len(snapshot), rootKeySize)
	}
	for i, c := range snapshot {
		if c != 0 {
			t.Fatalf("byte %d of the strategy's heap key = %#x when "+
				"unsealWithRootKey called back into the strategy: the eager wipe "+
				"inside the borrow is gone and only the deferred backstop is left", i, c)
		}
	}
}
