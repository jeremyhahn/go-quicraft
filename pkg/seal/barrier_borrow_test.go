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
	// onBarrierEncryptor, when set, runs at the start of BarrierEncryptor.
	//
	// It is the only observation point INSIDE unsealWithRootKey that a test can
	// reach without a new production seam, and it is what lets a test pin the
	// ORDER of the heap-key wipe rather than only its end state. See
	// TestUnseal_HeapKeyIsWipedBeforeTheRestOfUnsealRuns.
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

// TestBarrier_WorksWithAPlainSealingStrategy pins that the barrier's
// locked-memory handling does not quietly start depending on
// ProtectedSealingStrategy, because most strategies implement
// only the []byte pair — go-xkms's TPM2, PKCS#11 and cloud-KMS strategies, and
// go-qrdb's shamir and recovery strategies.
//
// Initialize generates the root key into locked memory and Unseal copies the
// strategy's return into locked memory, but BOTH still hand the strategy an
// ordinary []byte, because SealingStrategy is deliberately a []byte interface.
// This test is what holds that line.
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
// SealingStrategy.UnsealRootKey returns a plain []byte, so on the unseal path
// the key does exist outside protected memory. What the barrier controls is
// the WINDOW: the slice is copied into locked pages
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
	b.lockRootKey = enclave.NewMemoryEnclaveSized
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

// TestUnseal_ShortCircuitedBorrowStillWipesTheHeapKey is the sibling failure to
// the one above.
//
// A wipe of the strategy's heap key placed only INSIDE the borrow that copies
// it into locked memory is skipped on one path: MemoryEnclave.WithBytes returns
// ErrEnclaveDestroyed BEFORE it calls fn when the enclave is nil or wiped, so
// fn never runs and the full plaintext root key stays on the ordinary Go heap —
// swappable, core-dumpable, alive until the collector reclaims it. The deferred
// wipe is the floor that covers it.
//
// A (nil, nil) allocator is the shape that reaches it. The default lockRootKey
// is enclave.NewMemoryEnclaveSized, which cannot return one, so this is not
// reachable in production — but lockRootKey is an injectable field, and the
// comment at the borrow says the redesign exists precisely so "an error path
// added later cannot forget one". An allocator whose refusal is a nil enclave
// rather than an error is such a path.
//
// Verified by mutation: moving the wipe back inside the borrow —
// deleting `defer enclave.Wipe(heapKey)` and restoring the four hand-written
// calls — fails here with
//
//	barrier_borrow_test.go:319: byte 0 of the strategy's heap key = 0xa after
//	Unseal returned through a short-circuited borrow: the plaintext root key was
//	left readable on the ordinary heap
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
	b2.lockRootKey = func(int) (*enclave.MemoryEnclave, error) { return nil, nil }

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

// TestUnseal_NoEncryptionModeStillWipesTheHeapKey covers the branch that returns
// before the borrow exists at all. NoneStrategy's key is empty, so the wipe has
// nothing to erase — what this holds is that the deferred wipe leaves the
// branch's OUTCOME unchanged.
func TestUnseal_NoEncryptionModeStillWipesTheHeapKey(t *testing.T) {
	none := NewNoneStrategy()

	b := MustNewBarrier(testBarrierConfig(1))
	if err := b.Initialize(context.Background(), none, Credentials{}); err != nil {
		t.Fatalf("Initialize with NoneStrategy: %v", err)
	}
	blob := b.SealedRootKeyData()

	b2 := MustNewBarrier(testBarrierConfig(1))
	if err := b2.SetSealedRootKey(blob); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b2.Unseal(context.Background(), none, Credentials{}); err != nil {
		t.Fatalf("Unseal in no-encryption mode: %v", err)
	}
	if b2.IsSealed() {
		t.Fatal("the barrier is still sealed after a successful no-encryption unseal")
	}
}

// TestUnseal_HeapKeyIsWipedBeforeTheRestOfUnsealRuns pins the ORDER of the
// wipe, which no other test in this package does.
//
// TestUnseal_WipesTheStrategysHeapKey inspects s.lastUnsealed after Unseal
// RETURNS, so it is satisfied by any wipe that happens at all — including the
// deferred backstop. That alone would leave the eager wipe inside the borrow
// deletable with a green suite, which is the reverse of what the backstop is
// for: the defer is meant to be a floor UNDER the eager wipe, not a substitute
// that quietly retires it. A future edit dropping the eager wipe as dead code
// would reopen the long plaintext-on-heap window — across
// verifySealedStateMAC, the load-generation claim (which calls the embedder's
// StatePersister, so real I/O) and one DEK derivation per known epoch.
//
// BarrierEncryptor is the earliest point inside unsealWithRootKey a test can
// observe without adding a production seam. It runs after verifySealedStateMAC
// and long before the defer, so "already zero here" is a strictly stronger
// statement than the end-state assertion, and it is the one that dies when the
// eager wipe does.
//
// Verified by mutation: deleting the eager enclave.Wipe(heapKey)
// from inside the borrow leaves TestUnseal_WipesTheStrategysHeapKey PASSING and
// fails only here, with
//
//	barrier_borrow_test.go:425: byte 0 of the strategy's heap key = 0x8f when
//	unsealWithRootKey called back into the strategy: the eager wipe inside the
//	borrow is gone and only the deferred backstop is left
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
