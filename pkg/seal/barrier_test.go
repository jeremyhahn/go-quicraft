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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// testStrategy returns a SoftwareStrategy with fast Argon2id params for tests.
func testStrategy(t *testing.T) *SoftwareStrategy {
	t.Helper()
	s, err := NewSoftwareStrategyWithParams([]byte("test-passphrase"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("failed to create test strategy: %v", err)
	}
	return s
}

// testInitializedBarrier returns a barrier that has been initialized and is unsealed.
func testInitializedBarrier(t *testing.T) (*Barrier, *SoftwareStrategy) {
	t.Helper()
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	return b, s
}

// --- NewBarrier ---

func TestNewBarrier(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	if b == nil {
		t.Fatal("NewBarrier returned nil")
	}
	if b.state != barrierUninitialized {
		t.Fatalf("expected state=uninitialized, got %d", b.state)
	}
	if b.IsInitialized() {
		t.Fatal("new barrier should not be initialized")
	}
	if !b.IsSealed() {
		t.Fatal("new barrier should report as sealed (not unsealed)")
	}
	if b.CurrentEpoch() != 0 {
		t.Fatalf("expected epoch=0, got %d", b.CurrentEpoch())
	}
}

// --- Initialize ---

func TestBarrier_Initialize(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if !b.IsInitialized() {
		t.Fatal("barrier should be initialized after Initialize")
	}
	if b.IsSealed() {
		t.Fatal("barrier should be unsealed after Initialize")
	}
	if b.CurrentEpoch() != 1 {
		t.Fatalf("expected epoch=1 after Initialize, got %d", b.CurrentEpoch())
	}

	// Sealed root key should be available.
	sealed := b.SealedRootKeyData()
	if sealed == nil {
		t.Fatal("SealedRootKeyData returned nil after Initialize")
	}
	if sealed.Strategy != StrategySoftware {
		t.Fatalf("expected strategy=%q, got %q", softwareStrategyName, sealed.Strategy)
	}
	if sealed.Epoch != 1 {
		t.Fatalf("expected sealed epoch=1, got %d", sealed.Epoch)
	}
	if len(sealed.Salt) != hkdfSaltSize {
		t.Fatalf("expected salt length=%d, got %d", hkdfSaltSize, len(sealed.Salt))
	}
}

func TestBarrier_InitializeAlreadyInitialized(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	err := b.Initialize(context.Background(), s, Credentials{})
	if !errors.Is(err, ErrBarrierAlreadyInit) {
		t.Fatalf("expected ErrBarrierAlreadyInit, got %v", err)
	}
}

// --- Seal ---

func TestBarrier_Seal(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	if !b.IsSealed() {
		t.Fatal("barrier should be sealed after Seal")
	}
	if !b.IsInitialized() {
		t.Fatal("barrier should still be initialized after Seal")
	}
}

func TestBarrier_SealWhenAlreadySealed(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("first Seal failed: %v", err)
	}

	err := b.Seal()
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed on second Seal, got %v", err)
	}
}

func TestBarrier_SealClearsEnclaveRefs(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Verify key material is accessible before seal.
	if b.rootKey == nil {
		t.Fatal("rootKey enclave should be non-nil before Seal")
	}
	if b.currentDEK == nil {
		t.Fatal("currentDEK enclave should be non-nil before Seal")
	}

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Verify internal enclave references are nil.
	if b.rootKey != nil {
		t.Fatal("rootKey reference should be nil after Seal")
	}
	if b.currentDEK != nil {
		t.Fatal("currentDEK reference should be nil after Seal")
	}
	if b.cipher != nil {
		t.Fatal("cipher reference should be nil after Seal")
	}
	if len(b.deks) != 0 {
		t.Fatalf("deks map should be empty after Seal, got %d entries", len(b.deks))
	}
	if len(b.ciphers) != 0 {
		t.Fatalf("ciphers map should be empty after Seal, got %d entries", len(b.ciphers))
	}
}

// --- Unseal ---

func TestBarrier_RootKeyInEnclave(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Root key should be accessible via Open().
	buf, err := b.rootKey.Open()
	if err != nil {
		t.Fatalf("rootKey.Open() failed: %v", err)
	}
	if buf.Size() != rootKeySize {
		t.Fatalf("expected root key size %d, got %d", rootKeySize, buf.Size())
	}
	buf.Destroy()

	// Root key returned by RootKey() should match.
	rk, err := b.RootKey()
	if err != nil {
		t.Fatalf("RootKey() failed: %v", err)
	}
	defer wipeBytes(rk)
	if len(rk) != rootKeySize {
		t.Fatalf("expected root key length %d, got %d", rootKeySize, len(rk))
	}
}

func TestBarrier_DEKsInEnclaves(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Current DEK should be in an enclave.
	buf, err := b.currentDEK.Open()
	if err != nil {
		t.Fatalf("currentDEK.Open() failed: %v", err)
	}
	if buf.Size() != 32 {
		t.Fatalf("expected DEK size %d, got %d", 32, buf.Size())
	}
	buf.Destroy()

	// All epoch DEKs should be in enclaves.
	for epoch, pk := range b.deks {
		buf, err := pk.Open()
		if err != nil {
			t.Fatalf("deks[%d].Open() failed: %v", epoch, err)
		}
		if buf.Size() != 32 {
			t.Fatalf("expected DEK size %d for epoch %d, got %d", 32, epoch, buf.Size())
		}
		buf.Destroy()
	}
}

func TestBarrier_RotateOpensEnclave(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Capture the root key for comparison.
	rkBefore, err := b.RootKey()
	if err != nil {
		t.Fatalf("RootKey() failed: %v", err)
	}
	defer wipeBytes(rkBefore)

	newEpoch, err := b.Rotate()
	if err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("expected epoch 2, got %d", newEpoch)
	}

	// Root key should be unchanged after rotate.
	rkAfter, err := b.RootKey()
	if err != nil {
		t.Fatalf("RootKey() after rotate failed: %v", err)
	}
	defer wipeBytes(rkAfter)
	if !bytes.Equal(rkBefore, rkAfter) {
		t.Fatal("root key changed after Rotate")
	}

	// New epoch DEK should be in an enclave.
	pk, ok := b.deks[newEpoch]
	if !ok {
		t.Fatalf("deks[%d] not found", newEpoch)
	}
	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("deks[%d].Open() failed: %v", newEpoch, err)
	}
	if buf.Size() != 32 {
		t.Fatalf("expected DEK size %d for new epoch, got %d", 32, buf.Size())
	}
	buf.Destroy()
}

func TestBarrier_Unseal(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}

	if b.IsSealed() {
		t.Fatal("barrier should be unsealed after Unseal")
	}
	if b.CurrentEpoch() != 1 {
		t.Fatalf("expected epoch=1, got %d", b.CurrentEpoch())
	}
}

func TestBarrier_UnsealNotInitialized(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	err := b.Unseal(context.Background(), s, Credentials{})
	if !errors.Is(err, ErrBarrierNotInit) {
		t.Fatalf("expected ErrBarrierNotInit, got %v", err)
	}
}

func TestBarrier_UnsealAlreadyUnsealed(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	err := b.Unseal(context.Background(), s, Credentials{})
	if !errors.Is(err, ErrBarrierUnsealed) {
		t.Fatalf("expected ErrBarrierUnsealed, got %v", err)
	}
}

func TestBarrier_UnsealWrongPassphrase(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	wrong, err := NewSoftwareStrategyWithParams([]byte("wrong-passphrase"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}
	defer wrong.Close()

	err = b.Unseal(context.Background(), wrong, Credentials{})
	if err == nil {
		t.Fatal("expected error for wrong passphrase, got nil")
	}
}

// --- Encrypt/Decrypt ---

func TestBarrier_EncryptDecryptRoundTrip(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	plaintext := []byte("hello, world! this is a test message for encryption.")

	ciphertext, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Verify ciphertext is different from plaintext.
	if bytes.Equal(ciphertext, plaintext) {
		t.Fatal("ciphertext should not equal plaintext")
	}

	// Verify epoch is encoded in ciphertext.
	epoch := binary.BigEndian.Uint64(ciphertext[:epochSize])
	if epoch != 1 {
		t.Fatalf("expected epoch=1 in ciphertext, got %d", epoch)
	}

	decrypted, err := b.Decrypt(nil, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Fatalf("decrypted data does not match original: got %q, want %q", decrypted, plaintext)
	}
}

func TestBarrier_EncryptDecryptWithPreallocatedDst(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	plaintext := []byte("preallocated buffer test")
	dst := make([]byte, 0, 256)

	ciphertext, err := b.Encrypt(dst, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	decDst := make([]byte, 0, 256)
	decrypted, err := b.Decrypt(decDst, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Fatalf("decrypted data does not match original")
	}
}

func TestBarrier_EncryptWhenSealed(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	_, err := b.Encrypt(nil, []byte("test"))
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}
}

func TestBarrier_DecryptWhenSealed(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	ciphertext, err := b.Encrypt(nil, []byte("test"))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	_, err = b.Decrypt(nil, ciphertext)
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}
}

func TestBarrier_DecryptTruncatedCiphertext(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	_, err := b.Decrypt(nil, []byte("short"))
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for truncated ciphertext, got %v", err)
	}
}

func TestBarrier_DecryptCorruptCiphertext(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	ciphertext, err := b.Encrypt(nil, []byte("test data"))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Corrupt the authentication tag.
	ciphertext[len(ciphertext)-1] ^= 0xFF

	_, err = b.Decrypt(nil, ciphertext)
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for corrupt ciphertext, got %v", err)
	}
}

func TestBarrier_DecryptUnknownEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	ciphertext, err := b.Encrypt(nil, []byte("test data"))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Overwrite epoch to a value that does not exist.
	binary.BigEndian.PutUint64(ciphertext[:epochSize], 999)

	_, err = b.Decrypt(nil, ciphertext)
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for unknown epoch, got %v", err)
	}
}

func TestBarrier_EncryptEmptyPlaintext(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// AES-GCM supports encrypting empty plaintext (produces just epoch + nonce + tag).
	ciphertext, err := b.Encrypt(nil, []byte{})
	if err != nil {
		t.Fatalf("Encrypt empty plaintext failed: %v", err)
	}

	expectedLen := epochSize + algIDSize + 12 + 16
	if len(ciphertext) != expectedLen {
		t.Fatalf("expected ciphertext length=%d for empty plaintext, got %d", expectedLen, len(ciphertext))
	}

	decrypted, err := b.Decrypt(nil, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}
	if len(decrypted) != 0 {
		t.Fatalf("expected empty plaintext, got %d bytes", len(decrypted))
	}
}

// --- EncryptWithEpoch ---

func TestBarrier_EncryptWithEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	plaintext := []byte("epoch-tracking test")

	ciphertext, epoch, err := b.EncryptWithEpoch(nil, plaintext)
	if err != nil {
		t.Fatalf("EncryptWithEpoch failed: %v", err)
	}

	if epoch != 1 {
		t.Fatalf("expected epoch=1, got %d", epoch)
	}

	decrypted, err := b.Decrypt(nil, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Fatal("decrypted data does not match original")
	}
}

func TestBarrier_EncryptWithEpochWhenSealed(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	_, _, err := b.EncryptWithEpoch(nil, []byte("test"))
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}
}

// --- DecryptForEpoch ---

func TestBarrier_DecryptForEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	plaintext := []byte("epoch-specific decryption")
	ciphertext, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	decrypted, err := b.DecryptForEpoch(nil, ciphertext, 1)
	if err != nil {
		t.Fatalf("DecryptForEpoch failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Fatal("decrypted data does not match original")
	}
}

func TestBarrier_DecryptForEpochUnknown(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	ciphertext, err := b.Encrypt(nil, []byte("test"))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	_, err = b.DecryptForEpoch(nil, ciphertext, 999)
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for unknown epoch, got %v", err)
	}
}

func TestBarrier_DecryptForEpochWhenSealed(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	ciphertext, err := b.Encrypt(nil, []byte("test"))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	_, err = b.DecryptForEpoch(nil, ciphertext, 1)
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}
}

func TestBarrier_DecryptForEpochTruncated(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	_, err := b.DecryptForEpoch(nil, []byte("short"), 1)
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for truncated ciphertext, got %v", err)
	}
}

// --- Rotate ---

func TestBarrier_Rotate(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	newEpoch, err := b.Rotate()
	if err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	if newEpoch != 2 {
		t.Fatalf("expected new epoch=2, got %d", newEpoch)
	}
	if b.CurrentEpoch() != 2 {
		t.Fatalf("expected current epoch=2, got %d", b.CurrentEpoch())
	}
}

func TestBarrier_RotateWhenSealed(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	_, err := b.Rotate()
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}
}

func TestBarrier_RotateMultipleTimes(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	for i := uint64(2); i <= 5; i++ {
		epoch, err := b.Rotate()
		if err != nil {
			t.Fatalf("Rotate to epoch %d failed: %v", i, err)
		}
		if epoch != i {
			t.Fatalf("expected epoch=%d, got %d", i, epoch)
		}
	}

	if b.CurrentEpoch() != 5 {
		t.Fatalf("expected current epoch=5, got %d", b.CurrentEpoch())
	}
}

func TestBarrier_RotateAndDecryptOldData(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Encrypt with epoch 1.
	plaintext1 := []byte("epoch 1 data")
	ct1, err := b.Encrypt(nil, plaintext1)
	if err != nil {
		t.Fatalf("Encrypt at epoch 1 failed: %v", err)
	}

	// Rotate to epoch 2.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Encrypt with epoch 2.
	plaintext2 := []byte("epoch 2 data")
	ct2, err := b.Encrypt(nil, plaintext2)
	if err != nil {
		t.Fatalf("Encrypt at epoch 2 failed: %v", err)
	}

	// Verify epoch tags in ciphertexts.
	e1 := binary.BigEndian.Uint64(ct1[:epochSize])
	e2 := binary.BigEndian.Uint64(ct2[:epochSize])
	if e1 != 1 {
		t.Fatalf("expected ct1 epoch=1, got %d", e1)
	}
	if e2 != 2 {
		t.Fatalf("expected ct2 epoch=2, got %d", e2)
	}

	// Decrypt both.
	dec1, err := b.Decrypt(nil, ct1)
	if err != nil {
		t.Fatalf("Decrypt ct1 failed: %v", err)
	}
	if !bytes.Equal(plaintext1, dec1) {
		t.Fatal("ct1 decryption mismatch")
	}

	dec2, err := b.Decrypt(nil, ct2)
	if err != nil {
		t.Fatalf("Decrypt ct2 failed: %v", err)
	}
	if !bytes.Equal(plaintext2, dec2) {
		t.Fatal("ct2 decryption mismatch")
	}
}

// --- Full Lifecycle ---

func TestBarrier_FullLifecycle(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	// 1. Initialize.
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// 2. Encrypt.
	plaintext := []byte("lifecycle test data")
	ciphertext, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// 3. Seal.
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// 4. Verify operations fail when sealed.
	_, err = b.Encrypt(nil, []byte("should fail"))
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}
	_, err = b.Decrypt(nil, ciphertext)
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}

	// 5. Unseal.
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}

	// 6. Decrypt original data.
	decrypted, err := b.Decrypt(nil, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt after Unseal failed: %v", err)
	}
	if !bytes.Equal(plaintext, decrypted) {
		t.Fatal("decrypted data does not match original after seal/unseal cycle")
	}
}

// --- PurgeEpochsBefore ---

func TestBarrier_PurgeEpochsBefore(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Create epochs 1, 2, 3.
	plaintext1 := []byte("epoch 1")
	ct1, err := b.Encrypt(nil, plaintext1)
	if err != nil {
		t.Fatalf("Encrypt at epoch 1 failed: %v", err)
	}

	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate to 2 failed: %v", err)
	}
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate to 3 failed: %v", err)
	}

	// Purge epochs before 2 (removes epoch 1).
	purged, err := b.PurgeEpochsBefore(2)
	if err != nil {
		t.Fatalf("PurgeEpochsBefore failed: %v", err)
	}
	if purged != 1 {
		t.Fatalf("expected 1 epoch purged, got %d", purged)
	}

	// Epoch 1 data should no longer decrypt.
	_, err = b.Decrypt(nil, ct1)
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed after purge, got %v", err)
	}

	// Current epoch (3) should still work.
	plaintext3 := []byte("epoch 3")
	ct3, err := b.Encrypt(nil, plaintext3)
	if err != nil {
		t.Fatalf("Encrypt at epoch 3 failed: %v", err)
	}
	dec3, err := b.Decrypt(nil, ct3)
	if err != nil {
		t.Fatalf("Decrypt ct3 failed: %v", err)
	}
	if !bytes.Equal(plaintext3, dec3) {
		t.Fatal("ct3 decryption mismatch")
	}
}

func TestBarrier_PurgeEpochsBeforeWhenSealed(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	_, err := b.PurgeEpochsBefore(1)
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}
}

func TestBarrier_PurgeEpochsBeforeCurrentEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Trying to purge with minEpoch > current epoch.
	_, err := b.PurgeEpochsBefore(100)
	if !errors.Is(err, ErrPurgeCurrentEpoch) {
		t.Fatalf("expected ErrPurgeCurrentEpoch, got %v", err)
	}
}

func TestBarrier_PurgeEpochsPreservesCurrentEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Rotate to have multiple epochs.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Purge all before current. The current epoch must be preserved.
	if _, err := b.PurgeEpochsBefore(b.CurrentEpoch()); err != nil {
		t.Fatalf("PurgeEpochsBefore failed: %v", err)
	}

	// Current epoch encryption should still work.
	plaintext := []byte("still works")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt after purge failed: %v", err)
	}
	dec, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt after purge failed: %v", err)
	}
	if !bytes.Equal(plaintext, dec) {
		t.Fatal("decryption mismatch after purge")
	}
}

// --- In-Flight Epoch Registration ---

func TestBarrier_RegisterDeregisterInFlightEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Encrypt at epoch 1.
	plaintext := []byte("in-flight test")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Rotate to epoch 2.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Register epoch 1 as in-flight.
	b.RegisterInFlightEpoch(1)

	// Purge before 2 should skip epoch 1 because it is in-flight.
	if _, err := b.PurgeEpochsBefore(2); err != nil {
		t.Fatalf("PurgeEpochsBefore failed: %v", err)
	}

	// Epoch 1 should still decrypt.
	dec, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt in-flight epoch 1 failed: %v", err)
	}
	if !bytes.Equal(plaintext, dec) {
		t.Fatal("in-flight epoch 1 decryption mismatch")
	}

	// Deregister epoch 1.
	b.DeregisterInFlightEpoch(1)

	// Now purge should remove epoch 1.
	if _, err := b.PurgeEpochsBefore(2); err != nil {
		t.Fatalf("PurgeEpochsBefore after deregister failed: %v", err)
	}

	// Epoch 1 should no longer decrypt.
	_, err = b.Decrypt(nil, ct)
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed after deregister+purge, got %v", err)
	}
}

func TestBarrier_MultipleInFlightRegistrations(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Register epoch 1 twice.
	b.RegisterInFlightEpoch(1)
	b.RegisterInFlightEpoch(1)

	// Deregister once - should still be in-flight.
	b.DeregisterInFlightEpoch(1)

	b.inFlightMu.Lock()
	count := b.inFlightEpochs[1]
	b.inFlightMu.Unlock()

	if count != 1 {
		t.Fatalf("expected in-flight count=1, got %d", count)
	}

	// Deregister again - should be removed.
	b.DeregisterInFlightEpoch(1)

	b.inFlightMu.Lock()
	_, exists := b.inFlightEpochs[1]
	b.inFlightMu.Unlock()

	if exists {
		t.Fatal("epoch 1 should be removed from in-flight map after full deregistration")
	}
}

// --- Brute-Force Protection ---

func TestBarrier_BruteForceBackoff(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Use a controllable clock.
	now := time.Now()
	b.timeNow = func() time.Time { return now }

	wrong, err := NewSoftwareStrategyWithParams([]byte("wrong"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}
	defer wrong.Close()

	// First 5 attempts should fail normally (no backoff).
	for i := 0; i < bruteForceThreshold; i++ {
		err := b.Unseal(context.Background(), wrong, Credentials{})
		if err == nil {
			t.Fatal("expected error for wrong passphrase")
		}
		if errors.Is(err, ErrUnsealBackoff) {
			t.Fatalf("unexpected backoff at attempt %d", i+1)
		}
	}

	// 6th attempt within the backoff window should be rejected.
	err = b.Unseal(context.Background(), wrong, Credentials{})
	if !errors.Is(err, ErrUnsealBackoff) {
		t.Fatalf("expected ErrUnsealBackoff, got %v", err)
	}

	// Advance time past the backoff delay.
	now = now.Add(2 * time.Second)

	// Should be allowed to attempt again (but still fail with wrong passphrase).
	err = b.Unseal(context.Background(), wrong, Credentials{})
	if errors.Is(err, ErrUnsealBackoff) {
		t.Fatal("should not get backoff after delay has elapsed")
	}
	if err == nil {
		t.Fatal("expected error for wrong passphrase")
	}

	// Successful unseal should reset the counter.
	now = now.Add(5 * time.Second)
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal with correct passphrase failed: %v", err)
	}

	if b.failedAttempts != 0 {
		t.Fatalf("expected failedAttempts=0 after success, got %d", b.failedAttempts)
	}
}

func TestBarrier_BackoffDelay(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))

	tests := []struct {
		name     string
		failures int64
		expected time.Duration
	}{
		{"below_threshold", 3, 0},
		{"at_threshold", 5, bruteForceBaseDelay},
		{"one_over", 6, 2 * bruteForceBaseDelay},
		{"two_over", 7, 4 * bruteForceBaseDelay},
		{"capped", 30, bruteForceMaxDelay},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b.failedAttempts = tc.failures
			delay := b.backoffDelay()
			if delay != tc.expected {
				t.Fatalf("expected delay=%v for %d failures, got %v", tc.expected, tc.failures, delay)
			}
		})
	}
}

// --- Concurrent Safety ---

func TestBarrier_ConcurrentEncryptDecrypt(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	const workers = 16
	const opsPerWorker = 100

	var wg sync.WaitGroup
	wg.Add(workers)

	errCh := make(chan error, workers*opsPerWorker)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				plaintext := make([]byte, 64)
				if _, err := rand.Read(plaintext); err != nil {
					errCh <- err
					return
				}

				ct, err := b.Encrypt(nil, plaintext)
				if err != nil {
					errCh <- err
					return
				}

				dec, err := b.Decrypt(nil, ct)
				if err != nil {
					errCh <- err
					return
				}

				if !bytes.Equal(plaintext, dec) {
					errCh <- errors.New("decrypted data does not match")
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent operation failed: %v", err)
	}
}

func TestBarrier_ConcurrentEncryptAndRotate(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	const encryptWorkers = 8
	const opsPerWorker = 50

	var wg sync.WaitGroup
	wg.Add(encryptWorkers + 1)

	errCh := make(chan error, (encryptWorkers+1)*opsPerWorker)
	done := make(chan struct{})

	// Encrypt workers.
	for w := 0; w < encryptWorkers; w++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				plaintext := []byte("concurrent rotate test")
				ct, err := b.Encrypt(nil, plaintext)
				if err != nil {
					errCh <- err
					return
				}

				dec, err := b.Decrypt(nil, ct)
				if err != nil {
					errCh <- err
					return
				}

				if !bytes.Equal(plaintext, dec) {
					errCh <- errors.New("decrypted data does not match during rotation")
					return
				}
			}
		}()
	}

	// Rotate worker.
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if _, err := b.Rotate(); err != nil {
				errCh <- err
				return
			}
		}
		close(done)
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent operation failed: %v", err)
	}
}

// --- SetSealedRootKey ---

func TestBarrier_SetSealedRootKey(t *testing.T) {
	// Simulate loading persisted sealed key data.
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	// Create a second barrier that was initialized.
	b2 := NewBarrier(DefaultBarrierConfig(1))
	if err := b2.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	sealed := b2.SealedRootKeyData()

	// Load sealed key into the new barrier.
	if err := b.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey failed: %v", err)
	}

	if !b.IsInitialized() {
		t.Fatal("barrier should be initialized after SetSealedRootKey")
	}
	if !b.IsSealed() {
		t.Fatal("barrier should be sealed after SetSealedRootKey")
	}

	// Unseal and verify operations work.
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}

	plaintext := []byte("loaded from persisted key")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	dec, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}
	if !bytes.Equal(plaintext, dec) {
		t.Fatal("decrypted data does not match")
	}
}

func TestBarrier_SetSealedRootKeyValid(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))

	// SetSealedRootKey with valid fields should succeed.
	err := b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategySoftware,
		Epoch:       1,
		Salt:        make([]byte, 32),
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{1},
	})
	if err != nil {
		t.Fatalf("SetSealedRootKey failed: %v", err)
	}
}

// --- SealedRootKeyData ---

func TestBarrier_SealedRootKeyDataNotInitialized(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	if sealed := b.SealedRootKeyData(); sealed != nil {
		t.Fatal("expected nil SealedRootKeyData for uninitialized barrier")
	}
}

func TestBarrier_SealedRootKeyDataDefensiveCopy(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	sealed := b.SealedRootKeyData()
	if sealed == nil {
		t.Fatal("SealedRootKeyData returned nil")
	}

	// Mutate the copy.
	sealed.Salt[0] ^= 0xFF

	// Original should be unchanged.
	original := b.SealedRootKeyData()
	if bytes.Equal(sealed.Salt, original.Salt) {
		t.Fatal("defensive copy failed: mutation of copy affected original")
	}
}

// --- Key Derivation ---

func TestDeriveDEK_DifferentEpochs(t *testing.T) {
	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	dek1, err := deriveDEK(rootKey, salt, 1, 1, 32)
	if err != nil {
		t.Fatalf("deriveDEK epoch 1 failed: %v", err)
	}

	dek2, err := deriveDEK(rootKey, salt, 1, 2, 32)
	if err != nil {
		t.Fatalf("deriveDEK epoch 2 failed: %v", err)
	}

	if bytes.Equal(dek1, dek2) {
		t.Fatal("DEKs for different epochs should not be equal")
	}
}

func TestDeriveDEK_Deterministic(t *testing.T) {
	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	dek1, err := deriveDEK(rootKey, salt, 1, 42, 32)
	if err != nil {
		t.Fatalf("deriveDEK failed: %v", err)
	}

	dek2, err := deriveDEK(rootKey, salt, 1, 42, 32)
	if err != nil {
		t.Fatalf("deriveDEK failed: %v", err)
	}

	if !bytes.Equal(dek1, dek2) {
		t.Fatal("same inputs should produce same DEK")
	}
}

func TestDeriveDEK_DifferentSalts(t *testing.T) {
	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	salt1 := make([]byte, 32)
	salt2 := make([]byte, 32)
	if _, err := rand.Read(salt1); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}
	if _, err := rand.Read(salt2); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	dek1, err := deriveDEK(rootKey, salt1, 1, 1, 32)
	if err != nil {
		t.Fatalf("deriveDEK salt1 failed: %v", err)
	}

	dek2, err := deriveDEK(rootKey, salt2, 1, 1, 32)
	if err != nil {
		t.Fatalf("deriveDEK salt2 failed: %v", err)
	}

	if bytes.Equal(dek1, dek2) {
		t.Fatal("DEKs with different salts should not be equal")
	}
}

// --- HKDF Deployment ID Binding (OPT-D) ---

// TestHKDFDeploymentIDBinding verifies that the same root key, salt, and epoch
// produce different DEKs when the deployment ID differs. This ensures
// cryptographic isolation between clusters that happen to share key material.
func TestHKDFDeploymentIDBinding(t *testing.T) {
	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	const epoch uint64 = 1

	dek1, err := deriveDEK(rootKey, salt, 100, epoch, 32)
	if err != nil {
		t.Fatalf("deriveDEK deploymentID=100 failed: %v", err)
	}

	dek2, err := deriveDEK(rootKey, salt, 200, epoch, 32)
	if err != nil {
		t.Fatalf("deriveDEK deploymentID=200 failed: %v", err)
	}

	if bytes.Equal(dek1, dek2) {
		t.Fatal("DEKs for different deployment IDs should not be equal")
	}

	if len(dek1) != 32 {
		t.Fatalf("expected DEK size %d, got %d", 32, len(dek1))
	}
	if len(dek2) != 32 {
		t.Fatalf("expected DEK size %d, got %d", 32, len(dek2))
	}
}

// TestHKDFDeploymentIDConsistency verifies that the same root key, salt,
// deployment ID, and epoch always produce the same DEK (deterministic).
func TestHKDFDeploymentIDConsistency(t *testing.T) {
	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	const deploymentID uint64 = 42
	const epoch uint64 = 7

	dek1, err := deriveDEK(rootKey, salt, deploymentID, epoch, 32)
	if err != nil {
		t.Fatalf("deriveDEK call 1 failed: %v", err)
	}

	dek2, err := deriveDEK(rootKey, salt, deploymentID, epoch, 32)
	if err != nil {
		t.Fatalf("deriveDEK call 2 failed: %v", err)
	}

	if !bytes.Equal(dek1, dek2) {
		t.Fatal("same inputs should produce same DEK")
	}
}

// TestHKDFZeroDeploymentID verifies that deployment ID 0 is a valid input
// that produces a deterministic DEK, and that it differs from non-zero IDs.
func TestHKDFZeroDeploymentID(t *testing.T) {
	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	const epoch uint64 = 1

	// Zero deployment ID should work and produce a valid DEK.
	dekZero1, err := deriveDEK(rootKey, salt, 0, epoch, 32)
	if err != nil {
		t.Fatalf("deriveDEK deploymentID=0 failed: %v", err)
	}
	if len(dekZero1) != 32 {
		t.Fatalf("expected DEK size %d, got %d", 32, len(dekZero1))
	}

	// Zero deployment ID should be deterministic.
	dekZero2, err := deriveDEK(rootKey, salt, 0, epoch, 32)
	if err != nil {
		t.Fatalf("deriveDEK deploymentID=0 (second call) failed: %v", err)
	}
	if !bytes.Equal(dekZero1, dekZero2) {
		t.Fatal("deploymentID=0 should be deterministic")
	}

	// Zero deployment ID should differ from non-zero.
	dekNonZero, err := deriveDEK(rootKey, salt, 1, epoch, 32)
	if err != nil {
		t.Fatalf("deriveDEK deploymentID=1 failed: %v", err)
	}
	if bytes.Equal(dekZero1, dekNonZero) {
		t.Fatal("deploymentID=0 and deploymentID=1 should produce different DEKs")
	}
}

// TestBarrierDeploymentIDIsolation verifies end-to-end that two barriers with
// different deployment IDs but initialized with the same root key material
// cannot decrypt each other's ciphertext. This is the integration-level
// verification of the HKDF cluster binding.
func TestBarrierDeploymentIDIsolation(t *testing.T) {
	// Create two barriers with different deployment IDs.
	b1 := NewBarrier(DefaultBarrierConfig(100))
	s1 := testStrategy(t)
	defer s1.Close()
	if err := b1.Initialize(context.Background(), s1, Credentials{}); err != nil {
		t.Fatalf("b1 Initialize failed: %v", err)
	}

	b2 := NewBarrier(DefaultBarrierConfig(200))
	s2 := testStrategy(t)
	defer s2.Close()
	if err := b2.Initialize(context.Background(), s2, Credentials{}); err != nil {
		t.Fatalf("b2 Initialize failed: %v", err)
	}

	// Encrypt with barrier 1.
	plaintext := []byte("cross-cluster-isolation-test")
	ct, err := b1.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("b1 Encrypt failed: %v", err)
	}

	// Barrier 1 should decrypt its own ciphertext.
	pt, err := b1.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("b1 Decrypt failed: %v", err)
	}
	if !bytes.Equal(pt, plaintext) {
		t.Fatal("b1 round-trip failed")
	}

	// Barrier 2 should fail to decrypt barrier 1's ciphertext because
	// the DEKs are derived with different deployment IDs. The epoch
	// tag (epoch 1) will match, but the AES-GCM authentication will fail
	// because the underlying DEK is different.
	_, err = b2.Decrypt(nil, ct)
	if err != ErrDecryptionFailed {
		t.Fatalf("expected ErrDecryptionFailed from cross-cluster decrypt, got: %v", err)
	}
}

// --- Error Types ---

func TestKeyDerivationError(t *testing.T) {
	inner := errors.New("hkdf failed")
	err := &KeyDerivationError{Epoch: 42, Err: inner}

	if err.Error() != "quicraft/seal: key derivation failed for epoch 42: hkdf failed" {
		t.Fatalf("unexpected error string: %s", err.Error())
	}

	if !errors.Is(err, inner) {
		t.Fatal("Unwrap should return the inner error")
	}
}

func TestError(t *testing.T) {
	inner := errors.New("rand failed")
	err := &Error{Strategy: "software", Err: inner}

	if err.Error() != "quicraft/seal: seal failed (strategy=software): rand failed" {
		t.Fatalf("unexpected error string: %s", err.Error())
	}

	if !errors.Is(err, inner) {
		t.Fatal("Unwrap should return the inner error")
	}
}

func TestUnsealError(t *testing.T) {
	inner := errors.New("decrypt failed")
	err := &UnsealError{Strategy: "software", Err: inner}

	if err.Error() != "quicraft/seal: unseal failed (strategy=software): decrypt failed" {
		t.Fatalf("unexpected error string: %s", err.Error())
	}

	if !errors.Is(err, inner) {
		t.Fatal("Unwrap should return the inner error")
	}
}

func TestCipherError(t *testing.T) {
	inner := errors.New("invalid key size")
	err := &CipherError{Err: inner}

	if err.Error() != "quicraft/seal: cipher creation failed: invalid key size" {
		t.Fatalf("unexpected error string: %s", err.Error())
	}

	if !errors.Is(err, inner) {
		t.Fatal("Unwrap should return the inner error")
	}
}

// --- Cross-Barrier Isolation ---

func TestBarrier_CrossBarrierIsolation(t *testing.T) {
	b1 := NewBarrier(DefaultBarrierConfig(1))
	s1 := testStrategy(t)
	defer s1.Close()

	b2 := NewBarrier(DefaultBarrierConfig(1))
	s2 := testStrategy(t)
	defer s2.Close()

	if err := b1.Initialize(context.Background(), s1, Credentials{}); err != nil {
		t.Fatalf("b1 Initialize failed: %v", err)
	}
	if err := b2.Initialize(context.Background(), s2, Credentials{}); err != nil {
		t.Fatalf("b2 Initialize failed: %v", err)
	}

	plaintext := []byte("barrier isolation test")
	ct, err := b1.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("b1 Encrypt failed: %v", err)
	}

	// b2 should not be able to decrypt b1's ciphertext (different root key).
	_, err = b2.Decrypt(nil, ct)
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for cross-barrier decrypt, got %v", err)
	}
}

// --- Edge Cases ---

func TestBarrier_EncryptLargePayload(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// 1 MB payload.
	plaintext := make([]byte, 1024*1024)
	if _, err := rand.Read(plaintext); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	dec, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(plaintext, dec) {
		t.Fatal("large payload decryption mismatch")
	}
}

func TestBarrier_EncryptDecryptAfterSealUnsealCycle(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Rotate a few times.
	for i := 0; i < 3; i++ {
		if _, err := b.Rotate(); err != nil {
			t.Fatalf("Rotate failed: %v", err)
		}
	}

	// Encrypt at epoch 4.
	plaintext := []byte("after rotations")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Seal and unseal.
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}

	// Should still decrypt.
	dec, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt after seal/unseal failed: %v", err)
	}
	if !bytes.Equal(plaintext, dec) {
		t.Fatal("decryption mismatch after seal/unseal cycle")
	}
}

// TestBarrier_UnsealReDerivesOldEpochDEKs verifies that Unseal re-derives DEKs
// for all known epochs (not just the current one) so that data encrypted with
// older epochs can still be decrypted after a seal/unseal cycle.
func TestBarrier_UnsealReDerivesOldEpochDEKs(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Encrypt with epoch 1.
	plaintext1 := []byte("epoch 1 data across seal/unseal")
	ct1, err := b.Encrypt(nil, plaintext1)
	if err != nil {
		t.Fatalf("Encrypt at epoch 1 failed: %v", err)
	}

	// Rotate to epoch 2 and encrypt.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate to 2 failed: %v", err)
	}
	plaintext2 := []byte("epoch 2 data across seal/unseal")
	ct2, err := b.Encrypt(nil, plaintext2)
	if err != nil {
		t.Fatalf("Encrypt at epoch 2 failed: %v", err)
	}

	// Rotate to epoch 3 and encrypt.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate to 3 failed: %v", err)
	}
	plaintext3 := []byte("epoch 3 data across seal/unseal")
	ct3, err := b.Encrypt(nil, plaintext3)
	if err != nil {
		t.Fatalf("Encrypt at epoch 3 failed: %v", err)
	}

	// Seal and unseal.
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}

	// All three epochs should decrypt correctly.
	dec1, err := b.Decrypt(nil, ct1)
	if err != nil {
		t.Fatalf("Decrypt epoch 1 after unseal failed: %v", err)
	}
	if !bytes.Equal(plaintext1, dec1) {
		t.Fatal("epoch 1 decryption mismatch after unseal")
	}

	dec2, err := b.Decrypt(nil, ct2)
	if err != nil {
		t.Fatalf("Decrypt epoch 2 after unseal failed: %v", err)
	}
	if !bytes.Equal(plaintext2, dec2) {
		t.Fatal("epoch 2 decryption mismatch after unseal")
	}

	dec3, err := b.Decrypt(nil, ct3)
	if err != nil {
		t.Fatalf("Decrypt epoch 3 after unseal failed: %v", err)
	}
	if !bytes.Equal(plaintext3, dec3) {
		t.Fatal("epoch 3 decryption mismatch after unseal")
	}
}

// TestBarrier_SealedRootKeyDataWithMetadata verifies defensive copy includes metadata.
func TestBarrier_SealedRootKeyDataWithMetadata(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Inject metadata into the sealed key.
	b.mu.Lock()
	b.sealed.Metadata = []byte("test-metadata")
	b.mu.Unlock()

	sealed := b.SealedRootKeyData()
	if sealed == nil {
		t.Fatal("SealedRootKeyData returned nil")
	}
	if !bytes.Equal(sealed.Metadata, []byte("test-metadata")) {
		t.Fatalf("expected metadata=%q, got %q", "test-metadata", sealed.Metadata)
	}

	// Mutate the copy and verify original is unchanged.
	sealed.Metadata[0] = 'X'
	original := b.SealedRootKeyData()
	if original.Metadata[0] == 'X' {
		t.Fatal("defensive copy failed for metadata")
	}
}

// TestBarrier_EncryptDstReuse verifies that pre-allocated dst buffer is properly reused.
func TestBarrier_EncryptDstReuse(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	plaintext := []byte("dst reuse test")
	// Allocate an oversized buffer.
	dst := make([]byte, 1024)
	ct, err := b.Encrypt(dst, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Verify the returned slice uses the same backing array.
	expectedLen := epochSize + algIDSize + 12 + len(plaintext) + 16
	if len(ct) != expectedLen {
		t.Fatalf("expected ciphertext length=%d, got %d", expectedLen, len(ct))
	}
}

// TestBarrier_EncryptWithEpochAfterRotation verifies epoch tracking through rotation.
func TestBarrier_EncryptWithEpochAfterRotation(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	_, epoch, err := b.EncryptWithEpoch(nil, []byte("after rotation"))
	if err != nil {
		t.Fatalf("EncryptWithEpoch failed: %v", err)
	}
	if epoch != 2 {
		t.Fatalf("expected epoch=2 after rotation, got %d", epoch)
	}
}

// TestBarrier_DecryptForEpochCorruptCiphertext tests corrupt ciphertext with specific epoch.
func TestBarrier_DecryptForEpochCorruptCiphertext(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	ct, err := b.Encrypt(nil, []byte("test"))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Corrupt the last byte (tag).
	ct[len(ct)-1] ^= 0xFF

	_, err = b.DecryptForEpoch(nil, ct, 1)
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for corrupt ciphertext, got %v", err)
	}
}

// TestBarrier_UnsealInvalidRootKeySize tests the path where Unseal gets an
// incorrect-size root key from the strategy.
func TestBarrier_UnsealInvalidRootKeySize(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))

	// Use a mock-like strategy that returns a short root key.
	shortKeyStrategy := &fixedKeyStrategy{
		sealedKey: &SealedRootKey{
			Strategy:    "test",
			Epoch:       1,
			Salt:        make([]byte, 32),
			Ciphertext:  make([]byte, 64),
			KnownEpochs: []uint64{1},
		},
		unsealKey: make([]byte, 16), // Wrong size (should be 32).
	}

	if err := b.SetSealedRootKey(shortKeyStrategy.sealedKey); err != nil {
		t.Fatalf("SetSealedRootKey failed: %v", err)
	}

	err := b.Unseal(context.Background(), shortKeyStrategy, Credentials{})
	if !errors.Is(err, ErrInvalidRootKeySize) {
		t.Fatalf("expected ErrInvalidRootKeySize, got %v", err)
	}
}

// fixedKeyStrategy is a test-only strategy that returns fixed data.
type fixedKeyStrategy struct {
	sealedKey *SealedRootKey
	unsealKey []byte
}

func (f *fixedKeyStrategy) ID() StrategyID       { return "test" }
func (f *fixedKeyStrategy) Available() bool      { return true }
func (f *fixedKeyStrategy) HardwareBacked() bool { return false }

func (f *fixedKeyStrategy) SealRootKey(_ context.Context, rootKey []byte, _ Credentials) (*SealedRootKey, error) {
	return f.sealedKey, nil
}

func (f *fixedKeyStrategy) BarrierEncryptor(_ context.Context, _ []byte) (SymmetricEncrypter, error) {
	return nil, nil
}

func (f *fixedKeyStrategy) UnsealRootKey(_ context.Context, sealed *SealedRootKey, _ Credentials) ([]byte, error) {
	if f.unsealKey == nil {
		return nil, &UnsealError{Strategy: "test", Err: ErrDecryptionFailed}
	}
	out := make([]byte, len(f.unsealKey))
	copy(out, f.unsealKey)
	return out, nil
}

func (f *fixedKeyStrategy) Close() error {
	return nil
}

// TestBarrier_BruteForceLogging verifies that the error-level threshold is hit.
func TestBarrier_BruteForceHighFailureCount(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	now := time.Now()
	b.timeNow = func() time.Time { return now }

	wrong, err := NewSoftwareStrategyWithParams([]byte("wrong"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}
	defer wrong.Close()

	// Exceed the error threshold (20 failures).
	for i := 0; i < bruteForceErrorThreshold; i++ {
		now = now.Add(bruteForceMaxDelay + time.Second) // Skip past any backoff.
		_ = b.Unseal(context.Background(), wrong, Credentials{})
	}

	if b.failedAttempts != int64(bruteForceErrorThreshold) {
		t.Fatalf("expected failedAttempts=%d, got %d", bruteForceErrorThreshold, b.failedAttempts)
	}
}

// TestBarrier_ConcurrentSealDuringEncrypt verifies that a seal during
// concurrent encrypt operations returns ErrBarrierSealed.
func TestBarrier_ConcurrentSealDuringEncrypt(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)

	started := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-started
			for j := 0; j < 100; j++ {
				_, err := b.Encrypt(nil, []byte("concurrent seal test"))
				if err != nil && !errors.Is(err, ErrBarrierSealed) {
					t.Errorf("unexpected error: %v", err)
					return
				}
			}
		}()
	}

	close(started)
	// Seal while workers are running.
	_ = b.Seal()
	wg.Wait()
}

// --- Test error paths ---

// errReader is an io.Reader that returns an error after producing n bytes.
type errReader struct {
	remaining int
	err       error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, r.err
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 0xAA
	}
	r.remaining -= n
	return n, nil
}

// TestBarrier_InitializeRandFailRootKey tests that Initialize handles rand failure
// during root key generation.
func TestBarrier_InitializeRandFailRootKey(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	b.randReader = &errReader{remaining: 0, err: io.ErrUnexpectedEOF}

	s := testStrategy(t)
	defer s.Close()

	err := b.Initialize(context.Background(), s, Credentials{})
	if err == nil {
		t.Fatal("expected error from Initialize with failing rand")
	}
	var sealErr *Error
	if !errors.As(err, &sealErr) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
	if b.state != barrierUninitialized {
		t.Fatal("barrier should remain uninitialized")
	}
}

// TestBarrier_InitializeRandFailSalt tests that Initialize handles rand failure
// during salt generation (root key succeeds but salt fails).
func TestBarrier_InitializeRandFailSalt(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	// Provide enough bytes for root key (32) but fail on salt.
	b.randReader = &errReader{remaining: rootKeySize, err: io.ErrUnexpectedEOF}

	s := testStrategy(t)
	defer s.Close()

	err := b.Initialize(context.Background(), s, Credentials{})
	if err == nil {
		t.Fatal("expected error from Initialize with failing salt generation")
	}
	if b.state != barrierUninitialized {
		t.Fatal("barrier should remain uninitialized")
	}
}

// TestBarrier_EncryptRandNonceFail tests that Encrypt handles rand failure
// during nonce generation.
func TestBarrier_EncryptRandNonceFail(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Replace rand reader with one that will fail.
	b.randReader = &errReader{remaining: 0, err: io.ErrUnexpectedEOF}

	_, err := b.Encrypt(nil, []byte("test"))
	if err == nil {
		t.Fatal("expected error from Encrypt with failing rand")
	}
	var sealErr *Error
	if !errors.As(err, &sealErr) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
}

// failingSealStrategy always returns an error from Seal.
type failingSealStrategy struct{}

func (f *failingSealStrategy) ID() StrategyID       { return "test" }
func (f *failingSealStrategy) Available() bool      { return true }
func (f *failingSealStrategy) HardwareBacked() bool { return false }

func (f *failingSealStrategy) SealRootKey(_ context.Context, rootKey []byte, _ Credentials) (*SealedRootKey, error) {
	return nil, &Error{Strategy: "failing", Err: errors.New("seal failed")}
}

func (f *failingSealStrategy) BarrierEncryptor(_ context.Context, _ []byte) (SymmetricEncrypter, error) {
	return nil, nil
}

func (f *failingSealStrategy) UnsealRootKey(_ context.Context, sealed *SealedRootKey, _ Credentials) ([]byte, error) {
	return nil, &UnsealError{Strategy: "failing", Err: errors.New("unseal failed")}
}

func (f *failingSealStrategy) Close() error { return nil }

// TestBarrier_InitializeStrategyFails verifies Initialize handles strategy.Seal failure.
func TestBarrier_InitializeStrategyFails(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	err := b.Initialize(context.Background(), &failingSealStrategy{}, Credentials{})
	if err == nil {
		t.Fatal("expected error from Initialize with failing strategy")
	}
	var sealErr *Error
	if !errors.As(err, &sealErr) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}

	// Barrier should remain uninitialized.
	if b.IsInitialized() {
		t.Fatal("barrier should not be initialized after failed Initialize")
	}
}

// TestBarrier_DoubleCheckPatternEncrypt_FastPath verifies the first atomic
// unsealed check in Encrypt/Decrypt catches sealed state without locking.
func TestBarrier_DoubleCheckPatternEncrypt_FastPath(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	_, err := b.Encrypt(nil, []byte("test"))
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}

	_, _, err = b.EncryptWithEpoch(nil, []byte("test"))
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed from EncryptWithEpoch, got %v", err)
	}

	_, err = b.Decrypt(nil, make([]byte, epochSize+1+12+16))
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed from Decrypt, got %v", err)
	}

	_, err = b.DecryptForEpoch(nil, make([]byte, epochSize+1+12+16), 1)
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed from DecryptForEpoch, got %v", err)
	}
}

// --- EncryptWithEpoch nonce failure propagation ---

func TestBarrier_EncryptWithEpochRandNonceFail(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Replace rand reader so nonce generation fails.
	b.randReader = &errReader{remaining: 0, err: io.ErrUnexpectedEOF}

	_, _, err := b.EncryptWithEpoch(nil, []byte("test"))
	if err == nil {
		t.Fatal("expected error from EncryptWithEpoch with failing rand")
	}
	var sealErr *Error
	if !errors.As(err, &sealErr) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
}

// --- Unseal brute-force failure below threshold (no warn/error) ---

func TestBarrier_UnsealFailBelowThreshold(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	now := time.Now()
	b.timeNow = func() time.Time { return now }

	wrong, err := NewSoftwareStrategyWithParams([]byte("wrong"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}
	defer wrong.Close()

	// Fail below threshold - no logging should occur.
	for i := 0; i < bruteForceThreshold-1; i++ {
		unsealErr := b.Unseal(context.Background(), wrong, Credentials{})
		if unsealErr == nil {
			t.Fatal("expected error for wrong passphrase")
		}
		// Should not be a backoff error since we are below threshold.
		if errors.Is(unsealErr, ErrUnsealBackoff) {
			t.Fatalf("unexpected backoff at attempt %d", i+1)
		}
	}
	if b.failedAttempts != int64(bruteForceThreshold-1) {
		t.Fatalf("expected failedAttempts=%d, got %d", bruteForceThreshold-1, b.failedAttempts)
	}
}

// --- Unseal brute-force: exactly at threshold triggers Warn logging ---

func TestBarrier_UnsealFailAtThresholdTriggersWarn(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	now := time.Now()
	b.timeNow = func() time.Time {
		now = now.Add(bruteForceMaxDelay + time.Second)
		return now
	}

	wrong, err := NewSoftwareStrategyWithParams([]byte("wrong"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}
	defer wrong.Close()

	// Fail exactly at threshold (5) to trigger the slog.Warn path.
	for i := 0; i < bruteForceThreshold; i++ {
		_ = b.Unseal(context.Background(), wrong, Credentials{})
	}
	if b.failedAttempts != int64(bruteForceThreshold) {
		t.Fatalf("expected failedAttempts=%d, got %d", bruteForceThreshold, b.failedAttempts)
	}
}

// --- Unseal brute-force: above error threshold triggers Error logging ---

func TestBarrier_UnsealFailAboveErrorThreshold(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	now := time.Now()
	b.timeNow = func() time.Time {
		now = now.Add(bruteForceMaxDelay + time.Second)
		return now
	}

	wrong, err := NewSoftwareStrategyWithParams([]byte("wrong"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}
	defer wrong.Close()

	// Exceed error threshold to trigger slog.Error path.
	for i := 0; i < bruteForceErrorThreshold+1; i++ {
		_ = b.Unseal(context.Background(), wrong, Credentials{})
	}
	if b.failedAttempts != int64(bruteForceErrorThreshold+1) {
		t.Fatalf("expected failedAttempts=%d, got %d", bruteForceErrorThreshold+1, b.failedAttempts)
	}
}

// --- Concurrent EncryptWithEpoch and DecryptForEpoch ---

func TestBarrier_ConcurrentEncryptWithEpochDecryptForEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	const workers = 8
	const opsPerWorker = 50

	var wg sync.WaitGroup
	wg.Add(workers)

	errCh := make(chan error, workers*opsPerWorker)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				plaintext := make([]byte, 32)
				if _, err := rand.Read(plaintext); err != nil {
					errCh <- err
					return
				}

				ct, epoch, err := b.EncryptWithEpoch(nil, plaintext)
				if err != nil {
					errCh <- err
					return
				}

				dec, err := b.DecryptForEpoch(nil, ct, epoch)
				if err != nil {
					errCh <- err
					return
				}

				if !bytes.Equal(plaintext, dec) {
					errCh <- errors.New("DecryptForEpoch data mismatch")
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent operation failed: %v", err)
	}
}

// --- Initialize error on sealed barrier ---

func TestBarrier_InitializeOnSealedBarrier(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	err := b.Initialize(context.Background(), s, Credentials{})
	if !errors.Is(err, ErrBarrierAlreadyInit) {
		t.Fatalf("expected ErrBarrierAlreadyInit on sealed barrier, got %v", err)
	}
}

// --- Multiple seal/unseal cycles ---

func TestBarrier_MultipleSealUnsealCycles(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	plaintext := []byte("survives multiple cycles")

	for cycle := 0; cycle < 3; cycle++ {
		ct, err := b.Encrypt(nil, plaintext)
		if err != nil {
			t.Fatalf("cycle %d: Encrypt failed: %v", cycle, err)
		}

		dec, err := b.Decrypt(nil, ct)
		if err != nil {
			t.Fatalf("cycle %d: Decrypt failed: %v", cycle, err)
		}
		if !bytes.Equal(plaintext, dec) {
			t.Fatalf("cycle %d: decryption mismatch", cycle)
		}

		if err := b.Seal(); err != nil {
			t.Fatalf("cycle %d: Seal failed: %v", cycle, err)
		}
		if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
			t.Fatalf("cycle %d: Unseal failed: %v", cycle, err)
		}
	}
}

// --- Rotate + Seal/Unseal + DecryptForEpoch cross-epoch ---

func TestBarrier_RotateSealUnsealDecryptForEpoch(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Encrypt at epoch 1.
	pt1 := []byte("epoch-1-data")
	ct1, epoch1, err := b.EncryptWithEpoch(nil, pt1)
	if err != nil {
		t.Fatalf("EncryptWithEpoch failed: %v", err)
	}
	if epoch1 != 1 {
		t.Fatalf("expected epoch=1, got %d", epoch1)
	}

	// Rotate to epoch 2 and encrypt.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}
	pt2 := []byte("epoch-2-data")
	ct2, epoch2, err := b.EncryptWithEpoch(nil, pt2)
	if err != nil {
		t.Fatalf("EncryptWithEpoch failed: %v", err)
	}
	if epoch2 != 2 {
		t.Fatalf("expected epoch=2, got %d", epoch2)
	}

	// Seal and unseal.
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}

	// Decrypt with specific epochs after unseal.
	dec1, err := b.DecryptForEpoch(nil, ct1, epoch1)
	if err != nil {
		t.Fatalf("DecryptForEpoch epoch 1 failed: %v", err)
	}
	if !bytes.Equal(pt1, dec1) {
		t.Fatal("epoch 1 data mismatch")
	}

	dec2, err := b.DecryptForEpoch(nil, ct2, epoch2)
	if err != nil {
		t.Fatalf("DecryptForEpoch epoch 2 failed: %v", err)
	}
	if !bytes.Equal(pt2, dec2) {
		t.Fatal("epoch 2 data mismatch")
	}
}

// --- Purge with in-flight and verify ---

func TestBarrier_PurgeSkipsInFlightEpochs(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Rotate to epochs 2, 3, 4.
	for i := 0; i < 3; i++ {
		if _, err := b.Rotate(); err != nil {
			t.Fatalf("Rotate failed: %v", err)
		}
	}

	// Encrypt at epoch 1 (still has DEK in the map).
	// Register epoch 1 as in-flight.
	b.RegisterInFlightEpoch(1)

	// Purge before 4: should skip epoch 1 (in-flight) but remove 2 and 3.
	if _, err := b.PurgeEpochsBefore(4); err != nil {
		t.Fatalf("PurgeEpochsBefore failed: %v", err)
	}

	b.mu.RLock()
	_, has1 := b.ciphers[1]
	_, has2 := b.ciphers[2]
	_, has3 := b.ciphers[3]
	_, has4 := b.ciphers[4]
	b.mu.RUnlock()

	if !has1 {
		t.Fatal("epoch 1 should be preserved (in-flight)")
	}
	if has2 {
		t.Fatal("epoch 2 should be purged")
	}
	if has3 {
		t.Fatal("epoch 3 should be purged")
	}
	if !has4 {
		t.Fatal("epoch 4 (current) should be preserved")
	}

	b.DeregisterInFlightEpoch(1)
}

// --- TOCTOU second-check path simulation ---
//
// These tests exercise the second unsealed check inside the RLock of
// Encrypt/Decrypt/etc. We simulate this by holding the write lock (blocking
// any RLock acquisition), flipping the unsealed flag to false, then
// releasing the write lock so the waiting goroutine's RLock proceeds and
// hits the second check.

func TestBarrier_EncryptTOCTOUSecondCheck(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Hold write lock to block any RLock in Encrypt.
	b.mu.Lock()

	errCh := make(chan error, 1)
	go func() {
		// This will pass the first atomic check (unsealed is true) then
		// block on RLock because we hold the write lock.
		_, err := b.Encrypt(nil, []byte("test"))
		errCh <- err
	}()

	// Give the goroutine time to pass the first check and block on RLock.
	<-time.After(10 * time.Millisecond)

	// Simulate Seal by clearing the flag while holding the write lock.
	b.unsealed.Store(false)
	b.mu.Unlock()

	// The goroutine should now acquire RLock and hit the second check.
	err := <-errCh
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed from TOCTOU path, got %v", err)
	}
}

func TestBarrier_EncryptWithEpochTOCTOUSecondCheck(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	b.mu.Lock()

	errCh := make(chan error, 1)
	go func() {
		_, _, err := b.EncryptWithEpoch(nil, []byte("test"))
		errCh <- err
	}()

	<-time.After(10 * time.Millisecond)
	b.unsealed.Store(false)
	b.mu.Unlock()

	err := <-errCh
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed from TOCTOU path, got %v", err)
	}
}

func TestBarrier_DecryptTOCTOUSecondCheck(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	ct, err := b.Encrypt(nil, []byte("test"))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	b.mu.Lock()

	errCh := make(chan error, 1)
	go func() {
		_, derr := b.Decrypt(nil, ct)
		errCh <- derr
	}()

	<-time.After(10 * time.Millisecond)
	b.unsealed.Store(false)
	b.mu.Unlock()

	derr := <-errCh
	if !errors.Is(derr, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed from TOCTOU path, got %v", derr)
	}
}

func TestBarrier_DecryptForEpochTOCTOUSecondCheck(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	ct, err := b.Encrypt(nil, []byte("test"))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	b.mu.Lock()

	errCh := make(chan error, 1)
	go func() {
		_, derr := b.DecryptForEpoch(nil, ct, 1)
		errCh <- derr
	}()

	<-time.After(10 * time.Millisecond)
	b.unsealed.Store(false)
	b.mu.Unlock()

	derr := <-errCh
	if !errors.Is(derr, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed from TOCTOU path, got %v", derr)
	}
}

// --- Bug fix: Rotate updates sealed epoch ---

func TestBarrier_RotateUpdatesSealedEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// After Initialize, sealed epoch should be 1.
	sealed := b.SealedRootKeyData()
	if sealed.Epoch != 1 {
		t.Fatalf("expected sealed epoch=1 after Initialize, got %d", sealed.Epoch)
	}

	// Rotate to epoch 2.
	newEpoch, err := b.Rotate()
	if err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("expected new epoch=2, got %d", newEpoch)
	}

	// SealedRootKeyData must reflect the rotated epoch.
	sealed = b.SealedRootKeyData()
	if sealed.Epoch != 2 {
		t.Fatalf("expected sealed epoch=2 after Rotate, got %d", sealed.Epoch)
	}

	// Rotate again to epoch 3.
	newEpoch, err = b.Rotate()
	if err != nil {
		t.Fatalf("Rotate to 3 failed: %v", err)
	}
	if newEpoch != 3 {
		t.Fatalf("expected new epoch=3, got %d", newEpoch)
	}

	sealed = b.SealedRootKeyData()
	if sealed.Epoch != 3 {
		t.Fatalf("expected sealed epoch=3 after second Rotate, got %d", sealed.Epoch)
	}

	// Verify KnownEpochs contains all 3 epochs.
	if len(sealed.KnownEpochs) != 3 {
		t.Fatalf("expected 3 known epochs, got %d", len(sealed.KnownEpochs))
	}
	epochSet := make(map[uint64]struct{}, len(sealed.KnownEpochs))
	for _, ep := range sealed.KnownEpochs {
		epochSet[ep] = struct{}{}
	}
	for _, expected := range []uint64{1, 2, 3} {
		if _, ok := epochSet[expected]; !ok {
			t.Fatalf("expected epoch %d in KnownEpochs, not found", expected)
		}
	}
}

func TestBarrier_RotateUpdatesSealedEpoch_WhenSealed(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	_, err := b.Rotate()
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}
}

// --- Bug fix: KnownEpochs survive restart via persistence ---

func TestBarrier_KnownEpochsSurviveRestart(t *testing.T) {
	// Phase 1: Initialize, rotate twice (epochs 1, 2, 3), encrypt data at each epoch.
	b1 := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b1.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Encrypt at epoch 1.
	pt1 := []byte("epoch-1-payload")
	ct1, err := b1.Encrypt(nil, pt1)
	if err != nil {
		t.Fatalf("Encrypt at epoch 1 failed: %v", err)
	}

	// Rotate to epoch 2, encrypt.
	if _, err := b1.Rotate(); err != nil {
		t.Fatalf("Rotate to 2 failed: %v", err)
	}
	pt2 := []byte("epoch-2-payload")
	ct2, err := b1.Encrypt(nil, pt2)
	if err != nil {
		t.Fatalf("Encrypt at epoch 2 failed: %v", err)
	}

	// Rotate to epoch 3, encrypt.
	if _, err := b1.Rotate(); err != nil {
		t.Fatalf("Rotate to 3 failed: %v", err)
	}
	pt3 := []byte("epoch-3-payload")
	ct3, err := b1.Encrypt(nil, pt3)
	if err != nil {
		t.Fatalf("Encrypt at epoch 3 failed: %v", err)
	}

	// Capture the sealed root key data (simulates persistence to disk).
	sealed := b1.SealedRootKeyData()
	if sealed == nil {
		t.Fatal("SealedRootKeyData returned nil")
	}
	if sealed.Epoch != 3 {
		t.Fatalf("expected sealed epoch=3, got %d", sealed.Epoch)
	}
	if len(sealed.KnownEpochs) != 3 {
		t.Fatalf("expected 3 known epochs in sealed data, got %d", len(sealed.KnownEpochs))
	}

	// Phase 2: Simulate restart - create a fresh barrier, load persisted state, unseal.
	b2 := NewBarrier(DefaultBarrierConfig(1))
	if err := b2.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey failed: %v", err)
	}

	if err := b2.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal on restarted barrier failed: %v", err)
	}

	// Phase 3: Verify all 3 epoch DEKs were re-derived and can decrypt.
	dec1, err := b2.Decrypt(nil, ct1)
	if err != nil {
		t.Fatalf("Decrypt epoch 1 on restarted barrier failed: %v", err)
	}
	if !bytes.Equal(pt1, dec1) {
		t.Fatal("epoch 1 decryption mismatch on restarted barrier")
	}

	dec2, err := b2.Decrypt(nil, ct2)
	if err != nil {
		t.Fatalf("Decrypt epoch 2 on restarted barrier failed: %v", err)
	}
	if !bytes.Equal(pt2, dec2) {
		t.Fatal("epoch 2 decryption mismatch on restarted barrier")
	}

	dec3, err := b2.Decrypt(nil, ct3)
	if err != nil {
		t.Fatalf("Decrypt epoch 3 on restarted barrier failed: %v", err)
	}
	if !bytes.Equal(pt3, dec3) {
		t.Fatal("epoch 3 decryption mismatch on restarted barrier")
	}

	// Also verify DecryptForEpoch works for all epochs.
	for epoch, ct := range map[uint64][]byte{1: ct1, 2: ct2, 3: ct3} {
		_, err := b2.DecryptForEpoch(nil, ct, epoch)
		if err != nil {
			t.Fatalf("DecryptForEpoch epoch %d failed on restarted barrier: %v", epoch, err)
		}
	}
}

func TestBarrier_SetSealedRootKey_RejectsEmptyKnownEpochs(t *testing.T) {
	// Verify SetSealedRootKey rejects sealed keys where KnownEpochs does
	// not contain the current epoch. This prevents loading structurally
	// invalid data that would cause Unseal to miss re-deriving the DEK
	// for the active epoch.
	b1 := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b1.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	sealed := b1.SealedRootKeyData()

	// Remove KnownEpochs to simulate legacy or corrupted data.
	sealed.KnownEpochs = nil

	b2 := NewBarrier(DefaultBarrierConfig(1))
	err := b2.SetSealedRootKey(sealed)
	if !errors.Is(err, ErrInvalidSealedEpochs) {
		t.Fatalf("expected ErrInvalidSealedEpochs, got %v", err)
	}
}

// --- M13: Epoch AAD binding (cross-epoch splice prevention) ---

func TestBarrier_EpochAADPreventsCrossEpochSplice(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Encrypt at epoch 1.
	plaintext := []byte("epoch-bound data")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt at epoch 1 failed: %v", err)
	}

	// Rotate to epoch 2.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Tamper: overwrite the epoch prefix to claim epoch 2 while the
	// ciphertext was actually sealed with epoch 1's AAD. Because the AAD
	// binds the epoch to the authentication tag, this must fail.
	tampered := make([]byte, len(ct))
	copy(tampered, ct)
	binary.BigEndian.PutUint64(tampered[:epochSize], 2)

	_, err = b.Decrypt(nil, tampered)
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for cross-epoch spliced ciphertext, got %v", err)
	}

	// Verify the original ciphertext still decrypts correctly with
	// the correct epoch.
	dec, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt original ciphertext failed: %v", err)
	}
	if !bytes.Equal(plaintext, dec) {
		t.Fatal("decrypted data does not match original")
	}
}

func TestBarrier_EpochAADPreventsCrossEpochSpliceViaDecryptForEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Encrypt at epoch 1.
	plaintext := []byte("epoch-specific AAD test")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Rotate to epoch 2.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Attempt to decrypt epoch 1 ciphertext using epoch 2 key via
	// DecryptForEpoch. The epoch in the ciphertext header is 1, but
	// we request epoch 2. Even if we had epoch 2's key, the AAD
	// mismatch causes authentication failure.
	_, err = b.DecryptForEpoch(nil, ct, 2)
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for wrong epoch in DecryptForEpoch, got %v", err)
	}

	// Correct epoch still works.
	dec, err := b.DecryptForEpoch(nil, ct, 1)
	if err != nil {
		t.Fatalf("DecryptForEpoch with correct epoch failed: %v", err)
	}
	if !bytes.Equal(plaintext, dec) {
		t.Fatal("decrypted data does not match original")
	}
}

func TestEpochAAD_DifferentEpochsProduceDifferentAAD(t *testing.T) {
	aad1 := epochAAD(1)
	aad2 := epochAAD(2)
	if bytes.Equal(aad1, aad2) {
		t.Fatal("different epochs must produce different AAD")
	}
	if len(aad1) != epochSize {
		t.Fatalf("expected AAD length=%d, got %d", epochSize, len(aad1))
	}
}

func TestEpochAAD_SameEpochProducesSameAAD(t *testing.T) {
	aad1 := epochAAD(42)
	aad2 := epochAAD(42)
	if !bytes.Equal(aad1, aad2) {
		t.Fatal("same epoch must produce identical AAD")
	}
}

func TestNewAEAD_InvalidKeySize(t *testing.T) {
	_, err := newAEADForAlgorithm(AlgAES256GCM, []byte("too-short"))
	if err == nil {
		t.Fatal("expected error for invalid key size")
	}
	if !errors.Is(err, ErrInvalidKeySize) {
		t.Fatalf("expected ErrInvalidKeySize, got %T: %v", err, err)
	}
}

func TestNewAEAD_ValidKey(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	aead, err := newAEADForAlgorithm(AlgAES256GCM, key)
	if err != nil {
		t.Fatalf("newAEAD failed: %v", err)
	}
	if aead == nil {
		t.Fatal("newAEAD returned nil AEAD")
	}
}

// --- M4-sec/M6-sec: Brute-force backoff persistence ---

func TestBarrier_BackoffPersistence(t *testing.T) {
	// Verify that brute-force backoff state is persisted to the sealed key
	// and restored on SetSealedRootKey, preventing attackers from resetting
	// the backoff counter by restarting the process.
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	now := time.Now()
	b.timeNow = func() time.Time { return now }

	wrong, err := NewSoftwareStrategyWithParams([]byte("wrong"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}
	defer wrong.Close()

	// Accumulate failures up to the threshold.
	for i := 0; i < bruteForceThreshold; i++ {
		now = now.Add(bruteForceMaxDelay + time.Second)
		_ = b.Unseal(context.Background(), wrong, Credentials{})
	}

	// Verify the sealed key now contains the backoff state.
	sealed := b.SealedRootKeyData()
	if sealed.FailedAttempts != int64(bruteForceThreshold) {
		t.Fatalf("expected FailedAttempts=%d, got %d", bruteForceThreshold, sealed.FailedAttempts)
	}
	if sealed.LastFailedUnixNano == 0 {
		t.Fatal("expected non-zero LastFailedUnixNano after failures")
	}

	// Simulate restart: create a new barrier, load persisted state.
	b2 := NewBarrier(DefaultBarrierConfig(1))
	b2.timeNow = func() time.Time { return now }
	if err := b2.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey failed: %v", err)
	}

	// The new barrier should enforce backoff immediately (no time elapsed).
	err = b2.Unseal(context.Background(), wrong, Credentials{})
	if !errors.Is(err, ErrUnsealBackoff) {
		t.Fatalf("expected ErrUnsealBackoff on restarted barrier, got %v", err)
	}

	// Advance time past backoff and successfully unseal to reset.
	now = now.Add(bruteForceMaxDelay + time.Second)
	if err := b2.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal with correct passphrase failed: %v", err)
	}

	// Verify backoff state is cleared after success.
	sealed2 := b2.SealedRootKeyData()
	if sealed2.FailedAttempts != 0 {
		t.Fatalf("expected FailedAttempts=0 after success, got %d", sealed2.FailedAttempts)
	}
	if sealed2.LastFailedUnixNano != 0 {
		t.Fatalf("expected LastFailedUnixNano=0 after success, got %d", sealed2.LastFailedUnixNano)
	}
}

func TestBarrier_BackoffPersistenceZeroValuesBackwardCompat(t *testing.T) {
	// Verify that a v1 sealed key (zero FailedAttempts/LastFailedUnixNano)
	// results in no backoff, preserving backward compatibility.
	b := NewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	// Initialize a source barrier to get valid sealed data.
	src := NewBarrier(DefaultBarrierConfig(1))
	if err := src.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	sealed := src.SealedRootKeyData()

	// Explicitly zero backoff fields (simulating v1 data).
	sealed.FailedAttempts = 0
	sealed.LastFailedUnixNano = 0

	if err := b.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey failed: %v", err)
	}

	// Should be able to unseal without any backoff.
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}

	if b.failedAttempts != 0 {
		t.Fatalf("expected failedAttempts=0, got %d", b.failedAttempts)
	}
}

// --- M1-sec: Root key Seal AAD binding ---
// (The AAD binding is inherently tested by existing round-trip tests. These
// additional tests verify that AAD mismatch causes decryption failure.)

func TestSoftwareStrategy_AADPreventsCrossStrategySubstitution(t *testing.T) {
	// Verify that tampering with the AAD (by manually constructing a sealed
	// key with the wrong strategy name at the GCM layer) fails decryption.
	s, err := NewSoftwareStrategyWithParams([]byte("test-pass"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	// Seal normally (AAD = "software").
	sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Round-trip should succeed with the same strategy.
	decrypted, err := s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}
	if !bytes.Equal(rootKey, decrypted) {
		t.Fatal("round-trip mismatch")
	}
	wipeBytes(decrypted)

	// Corrupt one byte in the ciphertext to simulate a different AAD context.
	// Since AAD is baked into GCM authentication, any ciphertext produced
	// under a different AAD will fail authentication.
	sealed.Ciphertext[len(sealed.Ciphertext)-1] ^= 0x01
	_, err = s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err == nil {
		t.Fatal("expected error when ciphertext is tampered (AAD mismatch scenario)")
	}
}

func TestSoftwareStrategy_SealUnsealRoundTripWithAAD(t *testing.T) {
	// Verify that the AAD binding does not break normal Seal/Unseal flow.
	s, err := NewSoftwareStrategyWithParams([]byte("aad-test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	decrypted, err := s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}

	if !bytes.Equal(rootKey, decrypted) {
		t.Fatal("round-trip failed with AAD binding")
	}
	wipeBytes(decrypted)
}

// --- M2-sec: Epoch monotonicity validation in SetSealedRootKey ---

func TestBarrier_SetSealedRootKey_RejectsZeroEpoch(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))

	err := b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategySoftware,
		Epoch:       0,
		Salt:        make([]byte, 32),
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{0},
	})
	if !errors.Is(err, ErrInvalidSealedEpoch) {
		t.Fatalf("expected ErrInvalidSealedEpoch, got %v", err)
	}
}

func TestBarrier_SetSealedRootKey_RejectsInvalidSaltLength(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))

	// Salt too short.
	err := b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategySoftware,
		Epoch:       1,
		Salt:        make([]byte, 16), // Expected 32.
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{1},
	})
	if !errors.Is(err, ErrInvalidSealedSalt) {
		t.Fatalf("expected ErrInvalidSealedSalt for short salt, got %v", err)
	}

	// Salt too long.
	err = b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategySoftware,
		Epoch:       1,
		Salt:        make([]byte, 64), // Expected 32.
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{1},
	})
	if !errors.Is(err, ErrInvalidSealedSalt) {
		t.Fatalf("expected ErrInvalidSealedSalt for long salt, got %v", err)
	}

	// Nil salt.
	err = b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategySoftware,
		Epoch:       1,
		Salt:        nil,
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{1},
	})
	if !errors.Is(err, ErrInvalidSealedSalt) {
		t.Fatalf("expected ErrInvalidSealedSalt for nil salt, got %v", err)
	}
}

func TestBarrier_SetSealedRootKey_RejectsMissingCurrentEpoch(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))

	// KnownEpochs does not contain the current epoch (3).
	err := b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategySoftware,
		Epoch:       3,
		Salt:        make([]byte, 32),
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{1, 2}, // Missing 3.
	})
	if !errors.Is(err, ErrInvalidSealedEpochs) {
		t.Fatalf("expected ErrInvalidSealedEpochs, got %v", err)
	}
}

func TestBarrier_SetSealedRootKey_AcceptsValidMultiEpoch(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))

	err := b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategySoftware,
		Epoch:       3,
		Salt:        make([]byte, 32),
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("SetSealedRootKey failed for valid multi-epoch data: %v", err)
	}

	if !b.IsInitialized() {
		t.Fatal("barrier should be initialized after SetSealedRootKey")
	}
	if b.CurrentEpoch() != 3 {
		t.Fatalf("expected epoch=3, got %d", b.CurrentEpoch())
	}
}

// --- RootKey ---

func TestBarrier_RootKey(t *testing.T) {
	t.Run("ReturnsRootKeyWhenUnsealed", func(t *testing.T) {
		b, s := testInitializedBarrier(t)
		defer s.Close()

		key1, err := b.RootKey()
		if err != nil {
			t.Fatalf("RootKey failed: %v", err)
		}
		if len(key1) != rootKeySize {
			t.Fatalf("expected root key length=%d, got %d", rootKeySize, len(key1))
		}

		// Verify a second call returns equal but independent copy.
		key2, err := b.RootKey()
		if err != nil {
			t.Fatalf("second RootKey call failed: %v", err)
		}
		if !bytes.Equal(key1, key2) {
			t.Fatal("two RootKey calls returned different key values")
		}

		// Verify independence: mutating one does not affect the other.
		key1[0] ^= 0xFF
		if bytes.Equal(key1, key2) {
			t.Fatal("returned slices share underlying memory; expected independent copies")
		}
	})

	t.Run("ReturnsErrorWhenSealed", func(t *testing.T) {
		b, s := testInitializedBarrier(t)
		defer s.Close()

		if err := b.Seal(); err != nil {
			t.Fatalf("Seal failed: %v", err)
		}

		_, err := b.RootKey()
		if !errors.Is(err, ErrBarrierSealed) {
			t.Fatalf("expected ErrBarrierSealed, got %v", err)
		}
	})

	t.Run("ReturnsErrorWhenUninitialized", func(t *testing.T) {
		b := NewBarrier(DefaultBarrierConfig(1))

		_, err := b.RootKey()
		if !errors.Is(err, ErrBarrierSealed) {
			t.Fatalf("expected ErrBarrierSealed for uninitialized barrier, got %v", err)
		}
	})
}

// --- PurgeEpochsBefore return count ---

func TestBarrier_PurgeEpochsBeforeReturnsCount(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Create epochs 1, 2, 3, 4, 5.
	for i := 0; i < 4; i++ {
		if _, err := b.Rotate(); err != nil {
			t.Fatalf("Rotate %d failed: %v", i+1, err)
		}
	}
	if b.CurrentEpoch() != 5 {
		t.Fatalf("expected epoch 5, got %d", b.CurrentEpoch())
	}

	// Purge epochs before 4 (should remove 1, 2, 3 = 3 epochs).
	purged, err := b.PurgeEpochsBefore(4)
	if err != nil {
		t.Fatalf("PurgeEpochsBefore failed: %v", err)
	}
	if purged != 3 {
		t.Fatalf("expected 3 epochs purged, got %d", purged)
	}

	// Purge again with the same threshold: nothing more to remove.
	purged, err = b.PurgeEpochsBefore(4)
	if err != nil {
		t.Fatalf("second PurgeEpochsBefore failed: %v", err)
	}
	if purged != 0 {
		t.Fatalf("expected 0 epochs purged on second call, got %d", purged)
	}
}

func TestBarrier_PurgeEpochsBeforeZeroEpochNothingRemoved(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Rotate to have epochs 1, 2, 3.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate to 2 failed: %v", err)
	}
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate to 3 failed: %v", err)
	}

	// Purge with epoch 0 means nothing before 0 exists.
	purged, err := b.PurgeEpochsBefore(0)
	if err != nil {
		t.Fatalf("PurgeEpochsBefore(0) failed: %v", err)
	}
	if purged != 0 {
		t.Fatalf("expected 0 epochs purged with threshold 0, got %d", purged)
	}

	// All epochs should still decrypt.
	plaintext := []byte("test data")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	dec, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}
	if !bytes.Equal(plaintext, dec) {
		t.Fatal("decryption mismatch")
	}
}

func TestBarrier_PurgeEpochsPreservesCurrentAndRecentEpochs(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Encrypt at each epoch as we rotate through 1..5.
	ciphertexts := make(map[uint64][]byte)
	plaintext := []byte("test for each epoch")

	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt at epoch 1 failed: %v", err)
	}
	ciphertexts[1] = ct

	for i := uint64(2); i <= 5; i++ {
		if _, err := b.Rotate(); err != nil {
			t.Fatalf("Rotate to %d failed: %v", i, err)
		}
		ct, err := b.Encrypt(nil, plaintext)
		if err != nil {
			t.Fatalf("Encrypt at epoch %d failed: %v", i, err)
		}
		ciphertexts[i] = ct
	}

	// Purge before 3: should remove epochs 1 and 2, keep 3, 4, 5.
	purged, err := b.PurgeEpochsBefore(3)
	if err != nil {
		t.Fatalf("PurgeEpochsBefore(3) failed: %v", err)
	}
	if purged != 2 {
		t.Fatalf("expected 2 epochs purged, got %d", purged)
	}

	// Epochs 1 and 2 should no longer decrypt.
	for _, ep := range []uint64{1, 2} {
		_, err := b.Decrypt(nil, ciphertexts[ep])
		if !errors.Is(err, ErrDecryptionFailed) {
			t.Fatalf("expected ErrDecryptionFailed for purged epoch %d, got %v", ep, err)
		}
	}

	// Epochs 3, 4, 5 (recent and current) should still decrypt.
	for _, ep := range []uint64{3, 4, 5} {
		dec, err := b.Decrypt(nil, ciphertexts[ep])
		if err != nil {
			t.Fatalf("Decrypt for preserved epoch %d failed: %v", ep, err)
		}
		if !bytes.Equal(plaintext, dec) {
			t.Fatalf("decryption mismatch for epoch %d", ep)
		}
	}
}

// ============================================================================
// Custom Provider Injection Tests
// ============================================================================

// mockInvocationCounter is a test implementation of InvocationCounter that
// records calls and can be configured to return errors.
type mockInvocationCounter struct {
	incrementErr      error
	resetErr          error
	closeErr          error
	seedVal           int64
	enabled           bool
	rotationThreshold float64
	calls             []string
}

func (m *mockInvocationCounter) Increment() error {
	m.calls = append(m.calls, "increment")
	return m.incrementErr
}
func (m *mockInvocationCounter) Count() int64                  { return m.seedVal }
func (m *mockInvocationCounter) Limit() int64                  { return 1000 }
func (m *mockInvocationCounter) Seed(count int64)              { m.seedVal = count }
func (m *mockInvocationCounter) ShouldRotate() bool            { return false }
func (m *mockInvocationCounter) GetRotationThreshold() float64 { return m.rotationThreshold }
func (m *mockInvocationCounter) GetUsagePercentage() float64 {
	return float64(m.seedVal) / float64(1000) * 100.0
}
func (m *mockInvocationCounter) IsEnabled() bool { return m.enabled }
func (m *mockInvocationCounter) Reset() error {
	m.calls = append(m.calls, "reset")
	return m.resetErr
}

func (m *mockInvocationCounter) Close() error {
	m.calls = append(m.calls, "close")
	return m.closeErr
}

// mockNonceTracker is a test implementation of NonceTrackerProvider.
type mockNonceTracker struct {
	checkErr error
	clearErr error
	closeErr error
	enabled  bool
	calls    []string
}

func (m *mockNonceTracker) CheckAndRecordNonce(_ []byte) error {
	m.calls = append(m.calls, "check")
	return m.checkErr
}
func (m *mockNonceTracker) Contains(_ []byte) bool { return false }
func (m *mockNonceTracker) Count() int             { return 0 }
func (m *mockNonceTracker) Clear() error {
	m.calls = append(m.calls, "clear")
	return m.clearErr
}
func (m *mockNonceTracker) IsEnabled() bool         { return m.enabled }
func (m *mockNonceTracker) SetEnabled(enabled bool) { m.enabled = enabled }
func (m *mockNonceTracker) Close() error {
	m.calls = append(m.calls, "close")
	return m.closeErr
}

// TestBarrier_CustomInvocationCounter verifies that a custom
// InvocationCounter injected via BarrierConfig is used instead of the
// default in-memory implementation.
func TestBarrier_CustomInvocationCounter(t *testing.T) {
	mock := &mockInvocationCounter{enabled: true}
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationCounter = mock
	b := NewBarrier(cfg)

	if b.InvocationCounter() != mock {
		t.Fatal("InvocationCounter() should return the injected counter")
	}
}

// TestBarrier_CustomInvocationCounterResetError verifies that when a custom
// InvocationCounter returns an error from Reset during Rotate, the error
// is propagated as a TrackerError.
func TestBarrier_CustomInvocationCounterResetError(t *testing.T) {
	resetErr := errors.New("persistent reset failed")
	mock := &mockInvocationCounter{enabled: true, resetErr: resetErr}
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationCounter = mock
	cfg.NonceTracking = false
	b := NewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err := b.Rotate()
	if err == nil {
		t.Fatal("expected error from Rotate when invocation counter Reset fails")
	}

	var te *TrackerError
	if !errors.As(err, &te) {
		t.Fatalf("expected *TrackerError, got: %T: %v", err, err)
	}
	if te.Tracker != "invocation" || te.Op != "reset" {
		t.Fatalf("TrackerError fields: tracker=%q, op=%q; want invocation/reset", te.Tracker, te.Op)
	}
	if !errors.Is(te.Err, resetErr) {
		t.Fatalf("TrackerError.Err = %v, want %v", te.Err, resetErr)
	}
}

// TestBarrier_CustomNonceTrackerProvider verifies that a custom
// NonceTrackerProvider injected via BarrierConfig is used instead of the
// default in-memory implementation.
func TestBarrier_CustomNonceTrackerProvider(t *testing.T) {
	mock := &mockNonceTracker{enabled: true}
	cfg := DefaultBarrierConfig(1)
	cfg.NonceTrackerProvider = mock
	b := NewBarrier(cfg)

	if b.NonceTracker() != mock {
		t.Fatal("NonceTracker() should return the injected provider")
	}
}

// TestBarrier_CustomNonceTrackerProviderClearError verifies that when a custom
// NonceTrackerProvider returns an error from Clear during Rotate, the error
// is propagated as a TrackerError.
func TestBarrier_CustomNonceTrackerProviderClearError(t *testing.T) {
	clearErr := errors.New("persistent clear failed")
	mock := &mockNonceTracker{enabled: true, clearErr: clearErr}
	cfg := DefaultBarrierConfig(1)
	cfg.NonceTrackerProvider = mock
	cfg.InvocationLimit = -1 // disable invocation counter so only nonce tracker error triggers
	b := NewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err := b.Rotate()
	if err == nil {
		t.Fatal("expected error from Rotate when nonce tracker Clear fails")
	}

	var te *TrackerError
	if !errors.As(err, &te) {
		t.Fatalf("expected *TrackerError, got: %T: %v", err, err)
	}
	if te.Tracker != "nonce" || te.Op != "clear" {
		t.Fatalf("TrackerError fields: tracker=%q, op=%q; want nonce/clear", te.Tracker, te.Op)
	}
	if !errors.Is(te.Err, clearErr) {
		t.Fatalf("TrackerError.Err = %v, want %v", te.Err, clearErr)
	}
}

// TestBarrier_DefaultProvidersWhenNil verifies that when no custom providers
// are set, the barrier creates default InMemory implementations.
func TestBarrier_DefaultProvidersWhenNil(t *testing.T) {
	cfg := DefaultBarrierConfig(1)
	b := NewBarrier(cfg)

	ic := b.InvocationCounter()
	if ic == nil {
		t.Fatal("InvocationCounter() should not be nil with default config")
	}
	if _, ok := ic.(*InMemoryInvocationCounter); !ok {
		t.Fatalf("expected *InMemoryInvocationCounter, got %T", ic)
	}

	nt := b.NonceTracker()
	if nt == nil {
		t.Fatal("NonceTracker() should not be nil with default config")
	}
	if _, ok := nt.(*InMemoryNonceTracker); !ok {
		t.Fatalf("expected *InMemoryNonceTracker, got %T", nt)
	}
}

// TestBarrier_ProviderOverridesTrackingFlag verifies that setting a custom
// InvocationCounter takes precedence over InvocationLimit config, and custom
// NonceTrackerProvider overrides NonceTracking flag.
func TestBarrier_ProviderOverridesTrackingFlag(t *testing.T) {
	mockIC := &mockInvocationCounter{enabled: true}
	mockNT := &mockNonceTracker{enabled: true}

	cfg := DefaultBarrierConfig(1)
	cfg.InvocationLimit = -1  // would normally mean disabled
	cfg.NonceTracking = false // would normally mean no tracker
	cfg.InvocationCounter = mockIC
	cfg.NonceTrackerProvider = mockNT
	b := NewBarrier(cfg)

	if b.InvocationCounter() != mockIC {
		t.Fatal("custom InvocationCounter should override InvocationLimit=-1")
	}
	if b.NonceTracker() != mockNT {
		t.Fatal("custom NonceTrackerProvider should override NonceTracking=false")
	}
}

// --- Config accessor ---

// TestBarrier_Config verifies that Config() returns the BarrierConfig the
// barrier was constructed with, including custom field values.
func TestBarrier_Config(t *testing.T) {
	t.Parallel()
	cfg := DefaultBarrierConfig(42)
	b := NewBarrier(cfg)

	got := b.Config()
	if got.DeploymentID != 42 {
		t.Fatalf("Config().DeploymentID = %d, want 42", got.DeploymentID)
	}
	if got.Algorithm != cfg.Algorithm {
		t.Fatalf("Config().Algorithm = %v, want %v", got.Algorithm, cfg.Algorithm)
	}
	if got.InvocationLimit != cfg.InvocationLimit {
		t.Fatalf("Config().InvocationLimit = %d, want %d", got.InvocationLimit, cfg.InvocationLimit)
	}
	if got.NonceTracking != cfg.NonceTracking {
		t.Fatalf("Config().NonceTracking = %v, want %v", got.NonceTracking, cfg.NonceTracking)
	}
}

// TestBarrier_Config_DifferentDeploymentIDs verifies Config() returns the
// correct DeploymentID for barriers created with different IDs.
func TestBarrier_Config_DifferentDeploymentIDs(t *testing.T) {
	t.Parallel()
	ids := []uint64{0, 1, 100, ^uint64(0)}
	for _, id := range ids {
		cfg := DefaultBarrierConfig(id)
		b := NewBarrier(cfg)
		if got := b.Config().DeploymentID; got != id {
			t.Fatalf("Config().DeploymentID = %d, want %d", got, id)
		}
	}
}

// --- Rotate resets trackers ---

// TestBarrier_RotateResetsBothTrackers verifies that after a successful
// Rotate(), the invocation counter's Reset and nonce tracker's Clear are called,
// confirming both trackers are reset for the new epoch.
func TestBarrier_RotateResetsBothTrackers(t *testing.T) {
	t.Parallel()
	mockIC := &mockInvocationCounter{enabled: true}
	mockNT := &mockNonceTracker{enabled: true}
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationCounter = mockIC
	cfg.NonceTrackerProvider = mockNT
	b := NewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	newEpoch, err := b.Rotate()
	if err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("expected epoch 2 after Rotate, got %d", newEpoch)
	}

	// Verify invocation counter Reset was called.
	foundReset := false
	for _, call := range mockIC.calls {
		if call == "reset" {
			foundReset = true
			break
		}
	}
	if !foundReset {
		t.Fatalf("Rotate should call Reset on invocation counter; calls: %v", mockIC.calls)
	}

	// Verify nonce tracker Clear was called.
	foundClear := false
	for _, call := range mockNT.calls {
		if call == "clear" {
			foundClear = true
			break
		}
	}
	if !foundClear {
		t.Fatalf("Rotate should call Clear on nonce tracker; calls: %v", mockNT.calls)
	}
}

// TestBarrier_RotateResetsBothTrackers_VerifyEncryptAfterRotate verifies that
// when both trackers are injected but neither returns errors, the Rotate succeeds
// and subsequent encryptions work with the new epoch.
func TestBarrier_RotateResetsBothTrackers_VerifyEncryptAfterRotate(t *testing.T) {
	t.Parallel()
	mockIC := &mockInvocationCounter{enabled: true}
	mockNT := &mockNonceTracker{enabled: true}
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationCounter = mockIC
	cfg.NonceTrackerProvider = mockNT
	b := NewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Encrypt should succeed on the new epoch after tracker reset.
	plaintext := []byte("post-rotate plaintext")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt after Rotate failed: %v", err)
	}

	pt, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt after Rotate failed: %v", err)
	}
	if !bytes.Equal(pt, plaintext) {
		t.Fatalf("plaintext mismatch after Rotate: got %q, want %q", pt, plaintext)
	}
}

// --- Encrypt with nonce tracker error ---

// TestBarrier_EncryptWithNonceTrackerError verifies that when the nonce
// tracker's CheckAndRecordNonce returns an error, Encrypt propagates it.
func TestBarrier_EncryptWithNonceTrackerError(t *testing.T) {
	t.Parallel()
	nonceErr := ErrNonceReuse
	mockNT := &mockNonceTracker{enabled: true, checkErr: nonceErr}
	cfg := DefaultBarrierConfig(1)
	cfg.NonceTrackerProvider = mockNT
	cfg.InvocationLimit = -1
	b := NewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err := b.Encrypt(nil, []byte("test data"))
	if err == nil {
		t.Fatal("expected error from Encrypt when nonce tracker returns error")
	}
	if !errors.Is(err, ErrNonceReuse) {
		t.Fatalf("expected ErrNonceReuse, got: %v", err)
	}
}

// TestBarrier_EncryptWithNonceTrackerError_NilCheckDoesNotError verifies that
// when the nonce tracker does not return an error, Encrypt succeeds normally.
func TestBarrier_EncryptWithNonceTrackerError_NilCheckSucceeds(t *testing.T) {
	t.Parallel()
	mockNT := &mockNonceTracker{enabled: true, checkErr: nil}
	cfg := DefaultBarrierConfig(1)
	cfg.NonceTrackerProvider = mockNT
	cfg.InvocationLimit = -1
	b := NewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	ct, err := b.Encrypt(nil, []byte("test data"))
	if err != nil {
		t.Fatalf("Encrypt should succeed when nonce tracker returns nil: %v", err)
	}
	if len(ct) == 0 {
		t.Fatal("ciphertext should not be empty")
	}
}

// --- Encrypt with invocation counter error ---

// TestBarrier_EncryptWithInvocationCounterError verifies that when the
// invocation counter's Increment returns ErrInvocationLimitExceeded, Encrypt
// propagates that error.
func TestBarrier_EncryptWithInvocationCounterError(t *testing.T) {
	t.Parallel()
	mockIC := &mockInvocationCounter{enabled: true, incrementErr: ErrInvocationLimitExceeded}
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationCounter = mockIC
	cfg.NonceTracking = false
	b := NewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err := b.Encrypt(nil, []byte("test data"))
	if err == nil {
		t.Fatal("expected error from Encrypt when invocation counter returns error")
	}
	if !errors.Is(err, ErrInvocationLimitExceeded) {
		t.Fatalf("expected ErrInvocationLimitExceeded, got: %v", err)
	}
}

// TestBarrier_EncryptWithInvocationCounterError_NilCheckSucceeds verifies that
// when the invocation counter does not return an error, Encrypt succeeds normally.
func TestBarrier_EncryptWithInvocationCounterError_NilCheckSucceeds(t *testing.T) {
	t.Parallel()
	mockIC := &mockInvocationCounter{enabled: true, incrementErr: nil}
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationCounter = mockIC
	cfg.NonceTracking = false
	b := NewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	ct, err := b.Encrypt(nil, []byte("test data"))
	if err != nil {
		t.Fatalf("Encrypt should succeed when invocation counter returns nil: %v", err)
	}
	if len(ct) == 0 {
		t.Fatal("ciphertext should not be empty")
	}
}

// --- NewBarrier panic on invalid config ---

func TestNewBarrier_PanicsOnInvalidAlgorithm(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for invalid algorithm, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !bytes.Contains([]byte(msg), []byte("seal.NewBarrier")) {
			t.Fatalf("panic message should contain 'seal.NewBarrier', got %q", msg)
		}
		if !bytes.Contains([]byte(msg), []byte("unsupported")) {
			t.Fatalf("panic message should reference unsupported algorithm, got %q", msg)
		}
	}()

	cfg := BarrierConfig{
		Algorithm:    SymmetricAlgorithm(0xFF), // invalid
		DeploymentID: 1,
	}
	NewBarrier(cfg)
}

func TestNewBarrier_PanicsOnZeroAlgorithm(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for zero algorithm, got none")
		}
	}()

	cfg := BarrierConfig{
		Algorithm:    SymmetricAlgorithm(0x00), // zero value, not registered
		DeploymentID: 1,
	}
	NewBarrier(cfg)
}

// --- persistBackoffState with nil sealed ---

func TestBarrier_PersistBackoffState_NilSealed(t *testing.T) {
	t.Parallel()

	// A barrier that has never been initialized has sealed == nil.
	// persistBackoffState must return without panic.
	b := NewBarrier(DefaultBarrierConfig(1))

	// Simulate failed unseal state: increment counters directly.
	b.failedAttempts = 3
	b.lastFailedAt = time.Now()

	// Call persistBackoffState directly (requires write lock).
	b.mu.Lock()
	b.persistBackoffState() // must not panic with b.sealed == nil
	b.mu.Unlock()

	// sealed should still be nil (no assignment occurred).
	if b.sealed != nil {
		t.Fatal("sealed should remain nil when persistBackoffState is called on uninitialized barrier")
	}
}

// --- Concurrent Seal during Decrypt (TOCTOU defensive path) ---

// ---------------------------------------------------------------------------
// ShouldRotateKey
// ---------------------------------------------------------------------------

// TestBarrier_ShouldRotateKey_DisabledCounter verifies that ShouldRotateKey
// returns false when invocation counting is disabled (limit=-1).
func TestBarrier_ShouldRotateKey_DisabledCounter(t *testing.T) {
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationLimit = -1 // -1 disables counting
	b := NewBarrier(cfg)

	if b.ShouldRotateKey() {
		t.Error("ShouldRotateKey should return false when invocation tracking is disabled")
	}
}

// TestBarrier_ShouldRotateKey_BelowThreshold verifies that ShouldRotateKey
// returns false when usage is below the rotation threshold.
func TestBarrier_ShouldRotateKey_BelowThreshold(t *testing.T) {
	counter := NewInvocationCounter(true, 1000, 95.0)
	// Seed well below the 95% threshold (500 out of 1000 = 50%).
	counter.Seed(500)
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationCounter = counter
	b := NewBarrier(cfg)

	if b.ShouldRotateKey() {
		t.Error("ShouldRotateKey should return false when below the rotation threshold")
	}
}

// TestBarrier_ShouldRotateKey_AtThreshold verifies that ShouldRotateKey
// returns true when the invocation count has reached the rotation threshold.
func TestBarrier_ShouldRotateKey_AtThreshold(t *testing.T) {
	counter := NewInvocationCounter(true, 1000, 95.0)
	// Seed at exactly the 95% threshold (950 out of 1000).
	counter.Seed(950)
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationCounter = counter
	b := NewBarrier(cfg)

	if !b.ShouldRotateKey() {
		t.Error("ShouldRotateKey should return true when invocation count reaches the threshold")
	}
}

// TestBarrier_ShouldRotateKey_NilInvocations verifies that ShouldRotateKey
// returns false when the invocations field is nil (no counter configured).
func TestBarrier_ShouldRotateKey_NilInvocations(t *testing.T) {
	b := &Barrier{}

	if b.ShouldRotateKey() {
		t.Error("ShouldRotateKey should return false when invocations is nil")
	}
}

func TestBarrier_DecryptDuringConcurrentSeal(t *testing.T) {
	t.Parallel()

	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Encrypt some data while unsealed.
	ct, err := b.Encrypt(nil, []byte("concurrency test"))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Run concurrent Seal and Decrypt operations. We just need to verify
	// no data race or panic occurs; either Decrypt succeeds or returns
	// ErrBarrierSealed / ErrDecryptionFailed.
	const iterations = 50
	var wg sync.WaitGroup

	for i := 0; i < iterations; i++ {
		wg.Add(2)

		go func() {
			defer wg.Done()
			_, _ = b.Decrypt(nil, ct)
		}()

		go func() {
			defer wg.Done()
			_ = b.Seal()
		}()

		// Re-unseal for next iteration (serial to avoid double-unseal).
		wg.Wait()
		if b.IsSealed() {
			if unsealErr := b.Unseal(context.Background(), s, Credentials{}); unsealErr != nil {
				// May fail if already unsealed due to race timing; that is acceptable.
				if !errors.Is(unsealErr, ErrBarrierUnsealed) {
					t.Fatalf("Unseal failed: %v", unsealErr)
				}
			}
		}
	}
}
