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

// TestBarrier_ResetThenInitializeNewRootKey is the issue #208 acceptance test:
// after Reset, Initialize succeeds with a NEW root key, and nothing encrypted
// under the discarded key decrypts.
func TestBarrier_ResetThenInitializeNewRootKey(t *testing.T) {
	b, _ := testInitializedBarrier(t)
	oldRoot, err := b.RootKey()
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

	newRoot, err := b.RootKey()
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

// TestBarrier_ResetAfterSetSealedRootKey covers the other route into the sealed
// state: a barrier that adopted a persisted blob, including an HKDFV1 one,
// must reset to a fresh barrier that stamps the current derivation layout.
func TestBarrier_ResetAfterSetSealedRootKey(t *testing.T) {
	src, _ := testInitializedBarrier(t)
	blob := src.SealedRootKeyData()
	blob.HKDFVersion = 0 // a pre-version blob, adopted as HKDFV1

	b := MustNewBarrier(testBarrierConfig(1))
	if err := b.SetSealedRootKey(blob); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if b.hkdfVersion != HKDFV1 {
		t.Fatalf("precondition: hkdfVersion = %d, want HKDFV1", b.hkdfVersion)
	}
	if err := b.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := b.Initialize(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Initialize after Reset: %v", err)
	}
	if got := b.SealedRootKeyData().HKDFVersion; got != currentHKDFVersion {
		t.Fatalf("re-initialized blob HKDFVersion = %d, want %d", got, currentHKDFVersion)
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
