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
	"sync"
	"testing"
)

// testSealedBarrier returns a barrier that was initialized and then sealed.
func testSealedBarrier(t *testing.T) (*Barrier, *SoftwareStrategy) {
	t.Helper()
	b, s := testInitializedBarrier(t)
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return b, s
}

func TestBarrier_ResetUninitialized(t *testing.T) {
	b := MustNewBarrier(testBarrierConfig(1))
	if err := b.Reset(); !errors.Is(err, ErrBarrierNotInit) {
		t.Fatalf("Reset on uninitialized barrier: got %v, want ErrBarrierNotInit", err)
	}
}

func TestBarrier_ResetUnsealed(t *testing.T) {
	b, _ := testInitializedBarrier(t)
	if err := b.Reset(); !errors.Is(err, ErrBarrierUnsealed) {
		t.Fatalf("Reset on unsealed barrier: got %v, want ErrBarrierUnsealed", err)
	}
	// The refusal must leave the barrier fully usable.
	if b.IsSealed() || !b.IsInitialized() {
		t.Fatal("refused Reset changed the barrier's state")
	}
	if _, err := b.Encrypt(nil, []byte("still works")); err != nil {
		t.Fatalf("Encrypt after refused Reset: %v", err)
	}
}

func TestBarrier_ResetTwice(t *testing.T) {
	b, _ := testSealedBarrier(t)
	if err := b.Reset(); err != nil {
		t.Fatalf("first Reset: %v", err)
	}
	if err := b.Reset(); !errors.Is(err, ErrBarrierNotInit) {
		t.Fatalf("second Reset: got %v, want ErrBarrierNotInit", err)
	}
}

func TestBarrier_ResetClearsState(t *testing.T) {
	b, _ := testInitializedBarrier(t)
	if _, err := b.Encrypt(nil, []byte("count me")); err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if b.invocations == nil || b.invocations.Count() == 0 {
		t.Fatal("precondition: invocation counter should have counted the Encrypt")
	}
	if b.nonceTracker == nil || b.nonceTracker.Count() == 0 {
		t.Fatal("precondition: nonce tracker should have recorded the Encrypt")
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	b.failedAttempts = 3

	if err := b.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	if b.IsInitialized() {
		t.Fatal("IsInitialized after Reset")
	}
	if !b.IsSealed() {
		t.Fatal("barrier must not report unsealed after Reset")
	}
	if b.state != barrierUninitialized {
		t.Fatalf("state = %d, want uninitialized", b.state)
	}
	if got := b.CurrentEpoch(); got != 0 {
		t.Fatalf("CurrentEpoch = %d, want 0", got)
	}
	if got := b.LoadGeneration(); got != 0 {
		t.Fatalf("LoadGeneration = %d, want 0", got)
	}
	if b.SealedRootKeyData() != nil {
		t.Fatal("SealedRootKeyData should be nil after Reset")
	}
	if b.rootKey != nil || b.currentDEK != nil || b.cipher != nil || b.salt != nil {
		t.Fatal("key material references survived Reset")
	}
	if len(b.deks) != 0 || len(b.ciphers) != 0 || len(b.knownEpochs) != 0 {
		t.Fatalf("epoch maps not cleared: deks=%d ciphers=%d known=%d",
			len(b.deks), len(b.ciphers), len(b.knownEpochs))
	}
	if b.invocationsAuthoritative {
		t.Fatal("invocationsAuthoritative survived Reset")
	}
	if b.failedAttempts != 0 || !b.lastFailedAt.IsZero() {
		t.Fatal("backoff state survived Reset")
	}
	if got := b.invocations.Count(); got != 0 {
		t.Fatalf("invocation count = %d, want 0", got)
	}
	if got := b.nonceTracker.Count(); got != 0 {
		t.Fatalf("nonce tracker count = %d, want 0", got)
	}
	if b.hkdfVersion != currentHKDFVersion {
		t.Fatalf("hkdfVersion = %d, want %d", b.hkdfVersion, currentHKDFVersion)
	}
}

// TestBarrier_ResetThenInitializeNewRootKey pins that after Reset, Initialize succeeds with a NEW root key, and nothing encrypted
// under the discarded key decrypts.
func TestBarrier_ResetThenInitializeNewRootKey(t *testing.T) {
	b, _ := testInitializedBarrier(t)
	oldRoot, err := b.RootKey(context.Background())
	if err != nil {
		t.Fatalf("RootKey: %v", err)
	}
	oldSealed := b.SealedRootKeyData()

	plaintext := []byte("written before the rollback")
	oldCT, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	// Unseal has nothing to unseal any more.
	if err := b.Unseal(context.Background(), testStrategy(t), Credentials{}); !errors.Is(err, ErrBarrierNotInit) {
		t.Fatalf("Unseal after Reset: got %v, want ErrBarrierNotInit", err)
	}

	if err := b.Initialize(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Initialize after Reset: %v", err)
	}
	if !b.IsInitialized() || b.IsSealed() {
		t.Fatal("barrier should be initialized and unsealed after re-Initialize")
	}
	if got := b.CurrentEpoch(); got != 1 {
		t.Fatalf("CurrentEpoch after re-Initialize = %d, want 1", got)
	}
	if got := b.LoadGeneration(); got != firstLoadGeneration {
		t.Fatalf("LoadGeneration after re-Initialize = %d, want %d", got, firstLoadGeneration)
	}

	newRoot, err := b.RootKey(context.Background())
	if err != nil {
		t.Fatalf("RootKey: %v", err)
	}
	if bytes.Equal(oldRoot, newRoot) {
		t.Fatal("re-Initialize produced the same root key")
	}
	newSealed := b.SealedRootKeyData()
	if bytes.Equal(oldSealed.Salt, newSealed.Salt) {
		t.Fatal("re-Initialize produced the same HKDF salt")
	}

	// Same epoch number (1), different key: the old frame must fail closed.
	if _, err := b.Decrypt(nil, oldCT); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("Decrypt of pre-Reset ciphertext: got %v, want ErrDecryptionFailed", err)
	}
	if _, err := b.DecryptForEpoch(nil, oldCT, 1); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("DecryptForEpoch of pre-Reset ciphertext: got %v, want ErrDecryptionFailed", err)
	}

	// And the new key works.
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt after re-Initialize: %v", err)
	}
	pt, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt after re-Initialize: %v", err)
	}
	if !bytes.Equal(pt, plaintext) {
		t.Fatal("round trip mismatch after re-Initialize")
	}
}

// assertResetRefusedIntact asserts that Reset refuses with ErrResetNotFirstBoot
// and leaves the sealed barrier exactly where it was: same epoch, same blob,
// still unsealable with strategy.
func assertResetRefusedIntact(t *testing.T, b *Barrier, strategy SealingStrategy) {
	t.Helper()
	epoch := b.CurrentEpoch()
	gen := b.LoadGeneration()
	if err := b.Reset(); !errors.Is(err, ErrResetNotFirstBoot) {
		t.Fatalf("Reset: got %v, want ErrResetNotFirstBoot", err)
	}
	if !b.IsInitialized() || !b.IsSealed() || b.SealedRootKeyData() == nil {
		t.Fatal("refused Reset changed the barrier's state")
	}
	if b.CurrentEpoch() != epoch || b.LoadGeneration() != gen {
		t.Fatalf("refused Reset moved epoch/generation: epoch %d->%d gen %d->%d",
			epoch, b.CurrentEpoch(), gen, b.LoadGeneration())
	}
	if err := b.Unseal(context.Background(), strategy, Credentials{}); err != nil {
		t.Fatalf("Unseal after refused Reset: %v", err)
	}
	ct, err := b.Encrypt(nil, []byte("still works"))
	if err != nil {
		t.Fatalf("Encrypt after refused Reset: %v", err)
	}
	if _, err := b.Decrypt(nil, ct); err != nil {
		t.Fatalf("Decrypt after refused Reset: %v", err)
	}
}

// TestBarrier_ResetAfterSetSealedRootKey: a barrier whose root key was loaded
// from a persisted blob is not a first boot, so Reset must refuse — otherwise
// it would discard the in-process rollback floor SetSealedRootKey enforces.
func TestBarrier_ResetAfterSetSealedRootKey(t *testing.T) {
	src, _ := testInitializedBarrier(t)
	blob := src.SealedRootKeyData()

	b := MustNewBarrier(testBarrierConfig(1))
	if err := b.SetSealedRootKey(blob); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	assertResetRefusedIntact(t, b, testStrategy(t))
}

// TestBarrier_ResetAfterLoadOverOwnInitialize: SetSealedRootKey applied on top
// of this process's own Initialize also ends the first boot.
func TestBarrier_ResetAfterLoadOverOwnInitialize(t *testing.T) {
	b, s := testSealedBarrier(t)
	if err := b.SetSealedRootKey(b.SealedRootKeyData()); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	assertResetRefusedIntact(t, b, s)
}

// TestBarrier_ResetAfterRotate: once an epoch above the first exists, Reset
// must refuse.
func TestBarrier_ResetAfterRotate(t *testing.T) {
	b, s := testInitializedBarrier(t)
	if _, err := b.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	assertResetRefusedIntact(t, b, s)
}

// TestBarrier_ResetAfterUnsealForcedRotation: Unseal's forced rotation (taken
// when the invocation count is not authoritative) advances the epoch, and that
// too ends the first boot. invocationsAuthoritative is cleared directly so the
// rotation is the ONLY thing that can have cleared firstBoot.
func TestBarrier_ResetAfterUnsealForcedRotation(t *testing.T) {
	b, s := testInitializedBarrier(t)
	b.invocationsAuthoritative = false
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if got := b.CurrentEpoch(); got != 2 {
		t.Fatalf("precondition: Unseal should have rotated to epoch 2, got %d", got)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	assertResetRefusedIntact(t, b, s)
}

// TestBarrier_ResetFirstBootSurvivesInProcessUnseal: an in-process Seal/Unseal
// cycle that does not advance the epoch keeps the first boot resettable.
func TestBarrier_ResetFirstBootSurvivesInProcessUnseal(t *testing.T) {
	b, s := testSealedBarrier(t)
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if got := b.CurrentEpoch(); got != 1 {
		t.Fatalf("precondition: epoch = %d, want 1", got)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if b.firstBoot {
		t.Fatal("Reset re-armed firstBoot")
	}
}

// TestBarrier_RefusedResetKeepsRollbackFloor pins the rollback floor across a
// refused Reset: a barrier
// that has reached epoch 3 refuses Reset, and the older authentic epoch-1 blob
// is still rejected by SetSealedRootKey's rollback floor afterwards.
func TestBarrier_RefusedResetKeepsRollbackFloor(t *testing.T) {
	b, s := testInitializedBarrier(t)
	oldBlob := b.SealedRootKeyData()
	for i := 0; i < 2; i++ {
		if _, err := b.Rotate(context.Background()); err != nil {
			t.Fatalf("Rotate: %v", err)
		}
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Reset(); !errors.Is(err, ErrResetNotFirstBoot) {
		t.Fatalf("Reset: got %v, want ErrResetNotFirstBoot", err)
	}
	if err := b.SetSealedRootKey(oldBlob); !errors.Is(err, ErrSealedEpochRollback) {
		t.Fatalf("SetSealedRootKey(epoch-1 blob) after refused Reset: got %v, want ErrSealedEpochRollback", err)
	}
	if got := b.CurrentEpoch(); got != 3 {
		t.Fatalf("CurrentEpoch = %d, want 3", got)
	}
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
}

// TestBarrier_ResetThenInitializeIsFirstBootAgain: the flag is re-armed only by
// a new Initialize, so Initialize -> Seal -> Reset -> Initialize -> Seal -> Reset
// succeeds.
func TestBarrier_ResetThenInitializeIsFirstBootAgain(t *testing.T) {
	b, _ := testSealedBarrier(t)
	if err := b.Reset(); err != nil {
		t.Fatalf("first Reset: %v", err)
	}
	if err := b.Initialize(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Initialize after Reset: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Reset(); err != nil {
		t.Fatalf("second Reset: %v", err)
	}
}

func TestBarrier_ResetRefusedWhileEpochPinned(t *testing.T) {
	b, _ := testInitializedBarrier(t)
	_, epoch, err := b.EncryptWithEpochPinned(nil, []byte("pinned frame"))
	if err != nil {
		t.Fatalf("EncryptWithEpochPinned: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	if err := b.Reset(); !errors.Is(err, ErrEpochsInFlight) {
		t.Fatalf("Reset with pinned epoch: got %v, want ErrEpochsInFlight", err)
	}
	if !b.IsInitialized() || b.SealedRootKeyData() == nil || b.CurrentEpoch() != epoch {
		t.Fatal("refused Reset changed the barrier's state")
	}

	// A pin taken through RegisterInFlightEpoch blocks it too, and Reset stays
	// refused until the LAST reference is released.
	b.RegisterInFlightEpoch(epoch)
	b.DeregisterInFlightEpoch(epoch)
	if err := b.Reset(); !errors.Is(err, ErrEpochsInFlight) {
		t.Fatalf("Reset with the pinned-encrypt reference still held: got %v, want ErrEpochsInFlight", err)
	}
	b.DeregisterInFlightEpoch(epoch)

	b.RegisterInFlightEpoch(epoch)
	if err := b.Reset(); !errors.Is(err, ErrEpochsInFlight) {
		t.Fatalf("Reset with a registered pin: got %v, want ErrEpochsInFlight", err)
	}
	b.DeregisterInFlightEpoch(epoch)

	if err := b.Reset(); err != nil {
		t.Fatalf("Reset after releasing every pin: %v", err)
	}
}

func TestBarrier_ResetRefusedWhenSuperseded(t *testing.T) {
	b, _ := testSealedBarrier(t)
	b.superseded.Store(true)

	if err := b.Reset(); !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("Reset on superseded barrier: got %v, want ErrBarrierSuperseded", err)
	}
	if !b.IsInitialized() || !b.IsSuperseded() {
		t.Fatal("refused Reset changed the barrier's state")
	}
}

// TestBarrier_ResetConcurrent drives Reset against readers and pinning under
// the race detector. Exactly one of the concurrent Resets may succeed.
func TestBarrier_ResetConcurrent(t *testing.T) {
	b, _ := testSealedBarrier(t)

	const workers = 8
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded int
	)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			err := b.Reset()
			switch {
			case err == nil:
				mu.Lock()
				succeeded++
				mu.Unlock()
			case errors.Is(err, ErrBarrierNotInit), errors.Is(err, ErrEpochsInFlight):
			default:
				t.Errorf("Reset: unexpected error %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = b.IsInitialized()
			_ = b.CurrentEpoch()
			_ = b.SealedRootKeyData()
			if _, err := b.Encrypt(nil, []byte("x")); !errors.Is(err, ErrBarrierSealed) {
				t.Errorf("Encrypt on sealed/reset barrier: got %v, want ErrBarrierSealed", err)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			b.RegisterInFlightEpoch(1)
			b.DeregisterInFlightEpoch(1)
		}()
	}
	close(start)
	wg.Wait()

	if b.IsInitialized() {
		// Every Reset raced a pin; the barrier is still sealed and resettable.
		if err := b.Reset(); err != nil {
			t.Fatalf("final Reset: %v", err)
		}
		succeeded++
	}
	if succeeded != 1 {
		t.Fatalf("%d Resets succeeded, want exactly 1", succeeded)
	}
}
