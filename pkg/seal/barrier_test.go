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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-enclave"
)

// testStrategy returns a SoftwareStrategy with fast KDF params for tests. The
// KDF itself depends on the build: see newTestStrategy.
func testStrategy(t *testing.T) *SoftwareStrategy {
	t.Helper()
	s, err := newTestStrategy([]byte("test-passphrase"))
	if err != nil {
		t.Fatalf("failed to create test strategy: %v", err)
	}
	return s
}

// testBarrierConfig returns DefaultBarrierConfig with a no-op StatePersister
// attached. Rotate and a purging PurgeEpochsBefore fail closed with
// ErrNoStatePersister without one, so every test barrier that mutates
// persisted state needs it. Tests that assert on the persisted blob install
// their own recording persister instead.
func testBarrierConfig(deploymentID uint64) BarrierConfig {
	cfg := DefaultBarrierConfig(deploymentID)
	cfg.StatePersister = func(*SealedRootKey) error { return nil }
	return cfg
}

// testInitializedBarrier returns a barrier that has been initialized and is unsealed.
func testInitializedBarrier(t *testing.T) (*Barrier, *SoftwareStrategy) {
	t.Helper()
	b := MustNewBarrier(testBarrierConfig(1))
	s := testStrategy(t)
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	return b, s
}

// --- NewBarrier ---

func TestNewBarrier(t *testing.T) {
	b := MustNewBarrier(DefaultBarrierConfig(1))
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
	b := MustNewBarrier(DefaultBarrierConfig(1))
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
	if sealed.Strategy != StrategyPassphrase {
		t.Fatalf("expected strategy=%q, got %q", passphraseStrategyName, sealed.Strategy)
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
	buf, err := b.rootKey.Open(context.Background())
	if err != nil {
		t.Fatalf("rootKey.Open() failed: %v", err)
	}
	if buf.Size() != rootKeySize {
		t.Fatalf("expected root key size %d, got %d", rootKeySize, buf.Size())
	}
	buf.Destroy()

	// Root key returned by RootKey() should match.
	rk, err := b.RootKey(context.Background())
	if err != nil {
		t.Fatalf("RootKey() failed: %v", err)
	}
	defer enclave.Wipe(rk)
	if len(rk) != rootKeySize {
		t.Fatalf("expected root key length %d, got %d", rootKeySize, len(rk))
	}
}

func TestBarrier_DEKsInEnclaves(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Current DEK should be in an enclave.
	buf, err := b.currentDEK.Open(context.Background())
	if err != nil {
		t.Fatalf("currentDEK.Open() failed: %v", err)
	}
	if buf.Size() != 32 {
		t.Fatalf("expected DEK size %d, got %d", 32, buf.Size())
	}
	buf.Destroy()

	// All epoch DEKs should be in enclaves.
	for epoch, pk := range b.deks {
		buf, err := pk.Open(context.Background())
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
	rkBefore, err := b.RootKey(context.Background())
	if err != nil {
		t.Fatalf("RootKey() failed: %v", err)
	}
	defer enclave.Wipe(rkBefore)

	newEpoch, err := b.Rotate(context.Background())
	if err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("expected epoch 2, got %d", newEpoch)
	}

	// Root key should be unchanged after rotate.
	rkAfter, err := b.RootKey(context.Background())
	if err != nil {
		t.Fatalf("RootKey() after rotate failed: %v", err)
	}
	defer enclave.Wipe(rkAfter)
	if !bytes.Equal(rkBefore, rkAfter) {
		t.Fatal("root key changed after Rotate")
	}

	// New epoch DEK should be in an enclave.
	pk, ok := b.deks[newEpoch]
	if !ok {
		t.Fatalf("deks[%d] not found", newEpoch)
	}
	buf, err := pk.Open(context.Background())
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
	b := MustNewBarrier(DefaultBarrierConfig(1))
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

	wrong, err := newTestStrategy([]byte("wrong-passphrase"))
	if err != nil {
		t.Fatalf("newTestStrategy failed: %v", err)
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

// TestBarrier_DecryptTamperedAlgIDDoesNotPanic is the regression test. The
// algID header byte is unauthenticated and does not select the cipher (the
// active AEAD is fixed by the epoch DEK). A tampered algID whose nonce-size
// class differs from the active 12-byte cipher (0x05 = XChaCha20, 24-byte) or
// that is invalid would, without the guard, slice a wrong-length nonce and
// PANIC inside aead.Open, crashing the host on corrupt WAL/snapshot input.
// Decrypt must instead fail closed with ErrDecryptionFailed.
func TestBarrier_DecryptTamperedAlgIDDoesNotPanic(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	ciphertext, err := b.Encrypt(nil, []byte("sensitive payload that must not crash on tamper"))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	for _, algID := range []byte{0x00, 0x05, 0x06, 0x7f, 0xff} {
		tampered := append([]byte(nil), ciphertext...)
		tampered[epochSize] = algID // algID lives at offset epochSize
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Decrypt panicked on tampered algID=0x%02x (must fail closed): %v", algID, r)
				}
			}()
			if _, derr := b.Decrypt(nil, tampered); !errors.Is(derr, ErrDecryptionFailed) {
				t.Fatalf("Decrypt(algID=0x%02x) = %v, want ErrDecryptionFailed", algID, derr)
			}
		}()
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

	newEpoch, err := b.Rotate(context.Background())
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

	_, err := b.Rotate(context.Background())
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}
}

func TestBarrier_RotateMultipleTimes(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	for i := uint64(2); i <= 5; i++ {
		epoch, err := b.Rotate(context.Background())
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
	if _, err := b.Rotate(context.Background()); err != nil {
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
	b := MustNewBarrier(DefaultBarrierConfig(1))
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

func TestBarrier_PurgeEpochsBeforeWhenSealed(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	_, err := b.PurgeEpochsBefore(context.Background(), 1)
	if !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("expected ErrBarrierSealed, got %v", err)
	}
}

func TestBarrier_PurgeEpochsBeforeCurrentEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Trying to purge with minEpoch > current epoch.
	_, err := b.PurgeEpochsBefore(context.Background(), 100)
	if !errors.Is(err, ErrPurgeCurrentEpoch) {
		t.Fatalf("expected ErrPurgeCurrentEpoch, got %v", err)
	}
}

func TestBarrier_PurgeEpochsPreservesCurrentEpoch(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Rotate to have multiple epochs.
	if _, err := b.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Purge all before current. The current epoch must be preserved.
	if _, err := b.PurgeEpochsBefore(context.Background(), b.CurrentEpoch()); err != nil {
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
	if _, err := b.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Register epoch 1 as in-flight.
	b.RegisterInFlightEpoch(1)

	// Purge before 2 should skip epoch 1 because it is in-flight.
	if _, err := b.PurgeEpochsBefore(context.Background(), 2); err != nil {
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
	if _, err := b.PurgeEpochsBefore(context.Background(), 2); err != nil {
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

	if _, err := b.Rotate(context.Background()); err != nil {
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
	b := MustNewBarrier(DefaultBarrierConfig(1))
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

	wrong, err := newTestStrategy([]byte("wrong"))
	if err != nil {
		t.Fatalf("newTestStrategy failed: %v", err)
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

// TestBarrier_BackoffBackwardClockStep verifies that a backward clock step
// (a persisted lastFailedAt in the future) does not permanently lock out
// Unseal. With a naive elapsed<delay check, a future lastFailedAt yields a
// negative elapsed that stays below delay forever, returning ErrUnsealBackoff
// indefinitely. The clamp must treat a non-monotonic clock as satisfying the
// delay and re-anchor lastFailedAt to now.
func TestBarrier_BackoffBackwardClockStep(t *testing.T) {
	b := MustNewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Controllable clock anchored at a fixed "now".
	now := time.Now()
	b.timeNow = func() time.Time { return now }

	// Simulate restored state: past the brute-force threshold with a
	// lastFailedAt stamped one hour in the FUTURE (clock stepped back / state
	// restored on an earlier-clock host).
	b.failedAttempts = bruteForceThreshold + 1
	b.lastFailedAt = now.Add(time.Hour)

	// Unseal with the correct strategy must NOT be locked out by the future
	// timestamp; the clamp allows the attempt and it succeeds.
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		if errors.Is(err, ErrUnsealBackoff) {
			t.Fatal("permanent backoff lockout on backward clock step")
		}
		t.Fatalf("Unseal failed: %v", err)
	}
	if b.failedAttempts != 0 {
		t.Fatalf("expected failedAttempts=0 after successful unseal, got %d", b.failedAttempts)
	}
}

func TestBarrier_BackoffDelay(t *testing.T) {
	b := MustNewBarrier(DefaultBarrierConfig(1))

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
			if _, err := b.Rotate(context.Background()); err != nil {
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
	// Simulate loading persisted sealed key data. Both barriers carry a
	// StatePersister: a restart-unseal cannot prove how much of the loaded
	// epoch's NIST invocation budget is already spent, so it rotates to a fresh
	// epoch — which requires a persister — rather than encrypt under an
	// unprovable budget. See Barrier.resolveInvocationBudget.
	b := MustNewBarrier(testBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	// Create a second barrier that was initialized.
	b2 := MustNewBarrier(testBarrierConfig(1))
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

// --- SealedRootKeyData ---

func TestBarrier_SealedRootKeyDataNotInitialized(t *testing.T) {
	b := MustNewBarrier(DefaultBarrierConfig(1))
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

// TestDeriveDEK_InputBinding verifies that deriveDEK is deterministic for the
// same inputs (deployment ID 0 included), returns a 32-byte AES-256 key, and
// separates keys by epoch, salt and deployment ID.
func TestDeriveDEK_InputBinding(t *testing.T) {
	random := func() []byte {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			t.Fatalf("rand.Read failed: %v", err)
		}
		return b
	}
	rootKey, salt1, salt2 := random(), random(), random()

	type in struct {
		salt         []byte
		deploymentID uint64
		epoch        uint64
	}
	tests := []struct {
		name      string
		a, b      in
		wantEqual bool
	}{
		{"same inputs are deterministic", in{salt1, 42, 7}, in{salt1, 42, 7}, true},
		{"deployment ID 0 is deterministic", in{salt1, 0, 1}, in{salt1, 0, 1}, true},
		{"different epochs", in{salt1, 1, 1}, in{salt1, 1, 2}, false},
		{"different salts", in{salt1, 1, 1}, in{salt2, 1, 1}, false},
		{"different deployment IDs", in{salt1, 100, 1}, in{salt1, 200, 1}, false},
		{"deployment ID 0 differs from 1", in{salt1, 0, 1}, in{salt1, 1, 1}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dek1, err := deriveDEK(rootKey, tc.a.salt, tc.a.deploymentID, tc.a.epoch, AlgAES256GCM, HKDFV2)
			if err != nil {
				t.Fatalf("deriveDEK(a) failed: %v", err)
			}
			dek2, err := deriveDEK(rootKey, tc.b.salt, tc.b.deploymentID, tc.b.epoch, AlgAES256GCM, HKDFV2)
			if err != nil {
				t.Fatalf("deriveDEK(b) failed: %v", err)
			}
			if len(dek1) != 32 || len(dek2) != 32 {
				t.Fatalf("DEK sizes = %d, %d, want 32", len(dek1), len(dek2))
			}
			if got := bytes.Equal(dek1, dek2); got != tc.wantEqual {
				t.Fatalf("DEKs equal = %v, want %v", got, tc.wantEqual)
			}
		})
	}
}

// --- Error Types ---

// --- Cross-Barrier Isolation ---

func TestBarrier_CrossBarrierIsolation(t *testing.T) {
	b1 := MustNewBarrier(DefaultBarrierConfig(1))
	s1 := testStrategy(t)
	defer s1.Close()

	b2 := MustNewBarrier(DefaultBarrierConfig(1))
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
	if _, err := b.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate to 2 failed: %v", err)
	}
	plaintext2 := []byte("epoch 2 data across seal/unseal")
	ct2, err := b.Encrypt(nil, plaintext2)
	if err != nil {
		t.Fatalf("Encrypt at epoch 2 failed: %v", err)
	}

	// Rotate to epoch 3 and encrypt.
	if _, err := b.Rotate(context.Background()); err != nil {
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

	if _, err := b.Rotate(context.Background()); err != nil {
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
	b := MustNewBarrier(DefaultBarrierConfig(1))

	// Use a mock-like strategy that returns a short root key.
	shortKeyStrategy := &fixedKeyStrategy{
		sealedKey: &SealedRootKey{
			Strategy:    "test",
			Epoch:       1,
			Salt:        make([]byte, 32),
			HKDFVersion: HKDFV2,
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
	b := MustNewBarrier(DefaultBarrierConfig(1))
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

	wrong, err := newTestStrategy([]byte("wrong"))
	if err != nil {
		t.Fatalf("newTestStrategy failed: %v", err)
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
	b := MustNewBarrier(DefaultBarrierConfig(1))
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
	b := MustNewBarrier(DefaultBarrierConfig(1))
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

// TestBarrier_EncryptDoesNotDrawTheNonceFromRandReader is deliberately phrased
// as an assertion about where the nonce comes from rather than about an error.
//
// Drawing the AES-GCM nonce from b.randReader and handing it to Seal would be a
// caller-supplied IV, which FIPS 140-3 (SP 800-38D, CMVP IG C.H) forbids: the
// IV must be generated inside the module. cipher.NewGCMWithRandomNonce
// generates it,
// and b.randReader is not on the data path at all — so a reader that cannot
// produce a single byte must NOT be able to affect Encrypt. If someone
// reintroduces a caller-supplied IV, this test fails.
func TestBarrier_EncryptDoesNotDrawTheNonceFromRandReader(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	b.randReader = &errReader{remaining: 0, err: io.ErrUnexpectedEOF}

	ct, err := b.Encrypt(nil, []byte("test"))
	if err != nil {
		t.Fatalf("Encrypt drew randomness from randReader; the nonce must come "+
			"from inside the FIPS module boundary, not from the barrier: %v", err)
	}

	pt, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(pt) != "test" {
		t.Fatalf("roundtrip mismatch: got %q, want %q", pt, "test")
	}

	// Two seals under one DEK must still differ, which is the property the
	// nonce exists to provide and the one a broken reader could have destroyed.
	other, err := b.Encrypt(nil, []byte("test"))
	if err != nil {
		t.Fatalf("second Encrypt: %v", err)
	}
	if bytes.Equal(ct, other) {
		t.Fatal("two seals of the same plaintext under the same DEK produced " +
			"identical frames; the module-generated nonce is not varying")
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
	b := MustNewBarrier(DefaultBarrierConfig(1))
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

// --- EncryptWithEpoch and the module-generated nonce ---

// TestBarrier_EncryptWithEpochDoesNotDrawTheNonceFromRandReader is
// TestBarrier_EncryptDoesNotDrawTheNonceFromRandReader for the epoch-reporting
// entry point, which funnels through the same encryptLocked. See there for why
// a failing reader must not be able to fail an encryption.
func TestBarrier_EncryptWithEpochDoesNotDrawTheNonceFromRandReader(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	b.randReader = &errReader{remaining: 0, err: io.ErrUnexpectedEOF}

	ct, epoch, err := b.EncryptWithEpoch(nil, []byte("test"))
	if err != nil {
		t.Fatalf("EncryptWithEpoch drew randomness from randReader: %v", err)
	}
	if epoch != b.CurrentEpoch() {
		t.Fatalf("EncryptWithEpoch reported epoch %d, want %d", epoch, b.CurrentEpoch())
	}

	pt, err := b.DecryptForEpoch(nil, ct, epoch)
	if err != nil {
		t.Fatalf("DecryptForEpoch: %v", err)
	}
	if string(pt) != "test" {
		t.Fatalf("roundtrip mismatch: got %q, want %q", pt, "test")
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
	b := MustNewBarrier(DefaultBarrierConfig(1))
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

func TestBarrier_UnsealedC(t *testing.T) {
	b := MustNewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	closed := func(c <-chan struct{}) bool {
		select {
		case <-c:
			return true
		default:
			return false
		}
	}

	// Uninitialized is sealed: the channel stays open.
	if closed(b.UnsealedC()) {
		t.Fatal("UnsealedC closed before the barrier was ever unsealed")
	}
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if !closed(b.UnsealedC()) {
		t.Fatal("UnsealedC not closed for an unsealed barrier")
	}

	for cycle := 0; cycle < 2; cycle++ {
		if err := b.Seal(); err != nil {
			t.Fatalf("cycle %d: Seal failed: %v", cycle, err)
		}
		first, second := b.UnsealedC(), b.UnsealedC()
		if closed(first) || closed(second) {
			t.Fatalf("cycle %d: UnsealedC closed while sealed", cycle)
		}
		if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
			t.Fatalf("cycle %d: Unseal failed: %v", cycle, err)
		}
		// Every waiter of the sealed period is woken, not just one.
		if !closed(first) || !closed(second) {
			t.Fatalf("cycle %d: Unseal did not wake every UnsealedC waiter", cycle)
		}
	}
}

// --- Rotate + Seal/Unseal + DecryptForEpoch cross-epoch ---

func TestBarrier_RotateSealUnsealDecryptForEpoch(t *testing.T) {
	b := MustNewBarrier(testBarrierConfig(1))
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
	if _, err := b.Rotate(context.Background()); err != nil {
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
		if _, err := b.Rotate(context.Background()); err != nil {
			t.Fatalf("Rotate failed: %v", err)
		}
	}

	// Encrypt at epoch 1 (still has DEK in the map).
	// Register epoch 1 as in-flight.
	b.RegisterInFlightEpoch(1)

	// Purge before 4: should skip epoch 1 (in-flight) but remove 2 and 3.
	if _, err := b.PurgeEpochsBefore(context.Background(), 4); err != nil {
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

func TestBarrier_TOCTOUSecondCheck(t *testing.T) {
	tests := []struct {
		name string
		op   func(b *Barrier, ct []byte) error
	}{
		{"Encrypt", func(b *Barrier, _ []byte) error {
			_, err := b.Encrypt(nil, []byte("test"))
			return err
		}},
		{"EncryptWithEpoch", func(b *Barrier, _ []byte) error {
			_, _, err := b.EncryptWithEpoch(nil, []byte("test"))
			return err
		}},
		{"Decrypt", func(b *Barrier, ct []byte) error {
			_, err := b.Decrypt(nil, ct)
			return err
		}},
		{"DecryptForEpoch", func(b *Barrier, ct []byte) error {
			_, err := b.DecryptForEpoch(nil, ct, 1)
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, s := testInitializedBarrier(t)
			defer s.Close()

			ct, err := b.Encrypt(nil, []byte("test"))
			if err != nil {
				t.Fatalf("Encrypt failed: %v", err)
			}

			// Hold the write lock so the operation passes the first atomic
			// check (unsealed is true) and then blocks on RLock.
			b.mu.Lock()
			errCh := make(chan error, 1)
			go func() { errCh <- tc.op(b, ct) }()
			waitBlockedOnRLock(t, "seal.TestBarrier_TOCTOUSecondCheck")

			// Simulate Seal by clearing the flag while holding the write lock.
			b.unsealed.Store(false)
			b.mu.Unlock()

			// The goroutine now acquires RLock and hits the second check.
			if err := <-errCh; !errors.Is(err, ErrBarrierSealed) {
				t.Fatalf("expected ErrBarrierSealed from TOCTOU path, got %v", err)
			}
		})
	}
}

// waitBlockedOnRLock waits until a goroutine created by creator is parked in
// sync.(*RWMutex).RLock, so a test that holds the write lock knows the
// goroutine has passed every check before the lock.
func waitBlockedOnRLock(t *testing.T, creator string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		for _, g := range bytes.Split(buf[:n], []byte("\n\n")) {
			if bytes.Contains(g, []byte("(*RWMutex).RLock")) &&
				bytes.Contains(g, []byte("created by ")) &&
				bytes.Contains(g, []byte(creator)) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("goroutine never blocked on RLock")
		}
		runtime.Gosched()
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
	newEpoch, err := b.Rotate(context.Background())
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
	newEpoch, err = b.Rotate(context.Background())
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

// --- Bug fix: KnownEpochs survive restart via persistence ---

func TestBarrier_KnownEpochsSurviveRestart(t *testing.T) {
	// Phase 1: Initialize, rotate twice (epochs 1, 2, 3), encrypt data at each epoch.
	b1 := MustNewBarrier(testBarrierConfig(1))
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
	if _, err := b1.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate to 2 failed: %v", err)
	}
	pt2 := []byte("epoch-2-payload")
	ct2, err := b1.Encrypt(nil, pt2)
	if err != nil {
		t.Fatalf("Encrypt at epoch 2 failed: %v", err)
	}

	// Rotate to epoch 3, encrypt.
	if _, err := b1.Rotate(context.Background()); err != nil {
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
	b2 := MustNewBarrier(DefaultBarrierConfig(1))
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
	b1 := MustNewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b1.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	sealed := b1.SealedRootKeyData()

	// Remove KnownEpochs to simulate legacy or corrupted data.
	sealed.KnownEpochs = nil

	b2 := MustNewBarrier(DefaultBarrierConfig(1))
	err := b2.SetSealedRootKey(sealed)
	if !errors.Is(err, ErrInvalidSealedEpochs) {
		t.Fatalf("expected ErrInvalidSealedEpochs, got %v", err)
	}
}

// --- Epoch AAD binding (cross-epoch splice prevention) ---

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
	if _, err := b.Rotate(context.Background()); err != nil {
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
	if _, err := b.Rotate(context.Background()); err != nil {
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

// --- Brute-force backoff persistence ---

func TestBarrier_BackoffPersistence(t *testing.T) {
	// Verify that brute-force backoff state is persisted to the sealed key
	// and restored on SetSealedRootKey, preventing attackers from resetting
	// the backoff counter by restarting the process.
	b := MustNewBarrier(DefaultBarrierConfig(1))
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

	wrong, err := newTestStrategy([]byte("wrong"))
	if err != nil {
		t.Fatalf("newTestStrategy failed: %v", err)
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
	b2 := MustNewBarrier(DefaultBarrierConfig(1))
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

// TestBarrier_BackoffPersistedThroughStatePersister verifies that the backoff
// counters reach the StatePersister on a failed Unseal, not only the
// in-memory blob, so an embedder that reloads what it persisted keeps the
// backoff across a restart, and that a successful Unseal persists the reset
// (#284).
func TestBarrier_BackoffPersistedThroughStatePersister(t *testing.T) {
	var persisted *SealedRootKey
	cfg := DefaultBarrierConfig(1)
	cfg.StatePersister = func(s *SealedRootKey) error {
		persisted = s
		return nil
	}
	b := MustNewBarrier(cfg)
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
	wrong, err := newTestStrategy([]byte("wrong"))
	if err != nil {
		t.Fatalf("newTestStrategy failed: %v", err)
	}
	defer wrong.Close()

	for i := 0; i < bruteForceThreshold; i++ {
		now = now.Add(bruteForceMaxDelay + time.Second)
		_ = b.Unseal(context.Background(), wrong, Credentials{})
	}
	if persisted == nil || persisted.FailedAttempts != int64(bruteForceThreshold) {
		t.Fatalf("persisted FailedAttempts = %v, want %d: a failed Unseal must "+
			"write the backoff through the StatePersister", persisted, bruteForceThreshold)
	}
	if persisted.LastFailedUnixNano == 0 {
		t.Fatal("persisted LastFailedUnixNano = 0 after failures")
	}

	// Restart from what was persisted, not from the live barrier's memory.
	b2 := MustNewBarrier(cfg)
	b2.timeNow = func() time.Time { return now }
	if err := b2.SetSealedRootKey(persisted); err != nil {
		t.Fatalf("SetSealedRootKey failed: %v", err)
	}
	if err := b2.Unseal(context.Background(), wrong, Credentials{}); !errors.Is(err, ErrUnsealBackoff) {
		t.Fatalf("Unseal after restart = %v, want ErrUnsealBackoff", err)
	}

	now = now.Add(bruteForceMaxDelay + time.Second)
	if err := b2.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal with the correct passphrase failed: %v", err)
	}
	if persisted.FailedAttempts != 0 || persisted.LastFailedUnixNano != 0 {
		t.Fatalf("persisted backoff after a successful Unseal = (%d, %d), want (0, 0)",
			persisted.FailedAttempts, persisted.LastFailedUnixNano)
	}
}

// TestBarrier_BackoffPersistFailureKeepsUnsealError verifies that a persister
// failure while recording a failed attempt does not replace the attempt's own
// error.
func TestBarrier_BackoffPersistFailureKeepsUnsealError(t *testing.T) {
	persistErr := errors.New("disk full")
	failPersist := false
	cfg := DefaultBarrierConfig(1)
	cfg.StatePersister = func(*SealedRootKey) error {
		if failPersist {
			return persistErr
		}
		return nil
	}
	b := MustNewBarrier(cfg)
	s := testStrategy(t)
	defer s.Close()
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}
	wrong, err := newTestStrategy([]byte("wrong"))
	if err != nil {
		t.Fatalf("newTestStrategy failed: %v", err)
	}
	defer wrong.Close()

	failPersist = true
	err = b.Unseal(context.Background(), wrong, Credentials{})
	if err == nil || errors.Is(err, persistErr) {
		t.Fatalf("Unseal with wrong credentials and a failing persister = %v, "+
			"want the credential failure, not the persist error", err)
	}
	if got := b.SealedRootKeyData().FailedAttempts; got != 1 {
		t.Fatalf("in-memory FailedAttempts = %d, want 1", got)
	}
}

func TestBarrier_BackoffPersistenceZeroValuesBackwardCompat(t *testing.T) {
	// Verify that a v1 sealed key (zero FailedAttempts/LastFailedUnixNano)
	// results in no backoff, preserving backward compatibility.
	b := MustNewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	// Initialize a source barrier to get valid sealed data.
	src := MustNewBarrier(DefaultBarrierConfig(1))
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

// --- Root key Seal AAD binding ---
// (The AAD binding is inherently tested by existing round-trip tests. These
// additional tests verify that AAD mismatch causes decryption failure.)

func TestSoftwareStrategy_AADPreventsCrossStrategySubstitution(t *testing.T) {
	// Verify that tampering with the AAD (by manually constructing a sealed
	// key with the wrong strategy name at the GCM layer) fails decryption.
	s, err := newTestStrategy([]byte("test-pass"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	// Seal normally (AAD = "passphrase").
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
	enclave.Wipe(decrypted)

	// Corrupt one byte in the ciphertext to simulate a different AAD context.
	// Since AAD is baked into GCM authentication, any ciphertext produced
	// under a different AAD will fail authentication.
	sealed.Ciphertext[len(sealed.Ciphertext)-1] ^= 0x01
	_, err = s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err == nil {
		t.Fatal("expected error when ciphertext is tampered (AAD mismatch scenario)")
	}
}

// --- Epoch monotonicity validation in SetSealedRootKey ---

func TestBarrier_SetSealedRootKey_RejectsZeroEpoch(t *testing.T) {
	b := MustNewBarrier(DefaultBarrierConfig(1))

	err := b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategyPassphrase,
		Epoch:       0,
		Salt:        make([]byte, 32),
		HKDFVersion: HKDFV2,
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{0},
	})
	if !errors.Is(err, ErrInvalidSealedEpoch) {
		t.Fatalf("expected ErrInvalidSealedEpoch, got %v", err)
	}
}

func TestBarrier_SetSealedRootKey_RejectsInvalidSaltLength(t *testing.T) {
	b := MustNewBarrier(DefaultBarrierConfig(1))

	// Salt too short.
	err := b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategyPassphrase,
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
		Strategy:    StrategyPassphrase,
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
		Strategy:    StrategyPassphrase,
		Epoch:       1,
		Salt:        nil,
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{1},
	})
	if !errors.Is(err, ErrInvalidSealedSalt) {
		t.Fatalf("expected ErrInvalidSealedSalt for nil salt, got %v", err)
	}
}

func TestBarrier_SetSealedRootKey_AcceptsValidMultiEpoch(t *testing.T) {
	b := MustNewBarrier(DefaultBarrierConfig(1))

	err := b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategyPassphrase,
		Epoch:       3,
		Salt:        make([]byte, 32),
		HKDFVersion: HKDFV2,
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

		key1, err := b.RootKey(context.Background())
		if err != nil {
			t.Fatalf("RootKey failed: %v", err)
		}
		if len(key1) != rootKeySize {
			t.Fatalf("expected root key length=%d, got %d", rootKeySize, len(key1))
		}

		// Verify a second call returns equal but independent copy.
		key2, err := b.RootKey(context.Background())
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

		_, err := b.RootKey(context.Background())
		if !errors.Is(err, ErrBarrierSealed) {
			t.Fatalf("expected ErrBarrierSealed, got %v", err)
		}
	})

	t.Run("ReturnsErrorWhenUninitialized", func(t *testing.T) {
		b := MustNewBarrier(DefaultBarrierConfig(1))

		_, err := b.RootKey(context.Background())
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
		if _, err := b.Rotate(context.Background()); err != nil {
			t.Fatalf("Rotate %d failed: %v", i+1, err)
		}
	}
	if b.CurrentEpoch() != 5 {
		t.Fatalf("expected epoch 5, got %d", b.CurrentEpoch())
	}

	// Purge epochs before 4 (should remove 1, 2, 3 = 3 epochs).
	purged, err := b.PurgeEpochsBefore(context.Background(), 4)
	if err != nil {
		t.Fatalf("PurgeEpochsBefore failed: %v", err)
	}
	if purged != 3 {
		t.Fatalf("expected 3 epochs purged, got %d", purged)
	}

	// Purge again with the same threshold: nothing more to remove.
	purged, err = b.PurgeEpochsBefore(context.Background(), 4)
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
	if _, err := b.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate to 2 failed: %v", err)
	}
	if _, err := b.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate to 3 failed: %v", err)
	}

	// Purge with epoch 0 means nothing before 0 exists.
	purged, err := b.PurgeEpochsBefore(context.Background(), 0)
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
		if _, err := b.Rotate(context.Background()); err != nil {
			t.Fatalf("Rotate to %d failed: %v", i, err)
		}
		ct, err := b.Encrypt(nil, plaintext)
		if err != nil {
			t.Fatalf("Encrypt at epoch %d failed: %v", i, err)
		}
		ciphertexts[i] = ct
	}

	// Purge before 3: should remove epochs 1 and 2, keep 3, 4, 5.
	purged, err := b.PurgeEpochsBefore(context.Background(), 3)
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

// TestBarrier_InvocationCounterResetError_CommitsNewEpoch verifies the
// commit-first, fail-safe-reset semantics: when the injected invocation
// counter's Reset returns an error, Rotate has ALREADY committed the new epoch
// before resetting the trackers, so it must advance to (and return) the new
// epoch with NO fatal error. A non-reset counter is the conservative direction
// (the new key re-rotates sooner; it never exceeds its limit), so the barrier
// must remain fully usable on the new epoch.
func TestBarrier_InvocationCounterResetError_CommitsNewEpoch(t *testing.T) {
	resetErr := errors.New("persistent reset failed")
	mock := &mockInvocationCounter{enabled: true, resetErr: resetErr}
	cfg := testBarrierConfig(1)
	cfg.InvocationCounter = mock
	cfg.NonceTracking = BoolPtr(false)
	b := MustNewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	oldEpoch := b.CurrentEpoch()
	newEpoch, err := b.Rotate(context.Background())
	if err != nil {
		t.Fatalf("Rotate should not return a fatal error on a fail-safe tracker reset failure: %v", err)
	}

	// The rotation has genuinely succeeded: the new key is active. Rotate must
	// return the new epoch (never oldEpoch) and the barrier must have advanced.
	if newEpoch != oldEpoch+1 {
		t.Fatalf("Rotate returned epoch %d, want newEpoch %d", newEpoch, oldEpoch+1)
	}
	if got := b.CurrentEpoch(); got != newEpoch {
		t.Fatalf("barrier stayed on epoch %d after Rotate, want newEpoch %d", got, newEpoch)
	}

	// Reset was still attempted (it is what failed, fail-safe).
	foundReset := false
	for _, call := range mock.calls {
		if call == "reset" {
			foundReset = true
			break
		}
	}
	if !foundReset {
		t.Fatalf("Rotate should still attempt invocation counter Reset; calls: %v", mock.calls)
	}

	// The barrier must encrypt/decrypt correctly under the new epoch.
	ct, err := b.Encrypt(nil, []byte("after fail-safe reset"))
	if err != nil {
		t.Fatalf("Encrypt after Rotate with failed reset: %v", err)
	}
	pt, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt after Rotate with failed reset: %v", err)
	}
	if !bytes.Equal(pt, []byte("after fail-safe reset")) {
		t.Fatalf("plaintext mismatch after Rotate with failed reset")
	}
}

// TestBarrier_NonceClearError_CommitsNewEpoch verifies the commit-first,
// fail-safe-reset semantics for the nonce tracker: when the injected nonce
// tracker's Clear fails during Rotate, the new epoch has ALREADY been committed
// before the clear, so Rotate must advance to (and return) the new epoch with
// NO fatal error. A non-cleared nonce set is the conservative direction (stale
// nonces are at worst rejected as false positives; a reused nonce is never
// accepted), so the barrier must remain fully usable on the new epoch.
func TestBarrier_NonceClearError_CommitsNewEpoch(t *testing.T) {
	clearErr := errors.New("persistent nonce clear failed")
	mock := &mockNonceTracker{enabled: true, clearErr: clearErr}
	cfg := testBarrierConfig(1)
	cfg.NonceTrackerProvider = mock
	b := MustNewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	oldEpoch := b.CurrentEpoch()
	newEpoch, err := b.Rotate(context.Background())
	if err != nil {
		t.Fatalf("Rotate should not return a fatal error on a fail-safe tracker clear failure: %v", err)
	}

	if newEpoch != oldEpoch+1 {
		t.Fatalf("Rotate returned epoch %d, want newEpoch %d", newEpoch, oldEpoch+1)
	}
	if got := b.CurrentEpoch(); got != newEpoch {
		t.Fatalf("barrier stayed on epoch %d after Rotate, want newEpoch %d", got, newEpoch)
	}

	// Clear was still attempted (it is what failed, fail-safe).
	foundClear := false
	for _, call := range mock.calls {
		if call == "clear" {
			foundClear = true
			break
		}
	}
	if !foundClear {
		t.Fatalf("Rotate should still attempt nonce tracker Clear; calls: %v", mock.calls)
	}

	// The barrier must encrypt/decrypt correctly under the new epoch.
	ct, err := b.Encrypt(nil, []byte("after failed clear"))
	if err != nil {
		t.Fatalf("Encrypt after Rotate with failed clear: %v", err)
	}
	pt, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt after Rotate with failed clear: %v", err)
	}
	if !bytes.Equal(pt, []byte("after failed clear")) {
		t.Fatalf("plaintext mismatch after Rotate with failed clear")
	}
}

// TestBarrier_DefaultProvidersWhenNil verifies that when no custom providers
// are set, the barrier creates default InMemory implementations.
func TestBarrier_DefaultProvidersWhenNil(t *testing.T) {
	cfg := DefaultBarrierConfig(1)
	b := MustNewBarrier(cfg)

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
	cfg.InvocationLimit = -1           // would normally mean disabled
	cfg.NonceTracking = BoolPtr(false) // would normally mean no tracker
	cfg.InvocationCounter = mockIC
	cfg.NonceTrackerProvider = mockNT
	b := MustNewBarrier(cfg)

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
	b := MustNewBarrier(cfg)

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
	if got.GetNonceTracking() != cfg.GetNonceTracking() {
		t.Fatalf("Config().NonceTracking = %v, want %v", got.GetNonceTracking(), cfg.GetNonceTracking())
	}
}

// TestBarrier_Config_DifferentDeploymentIDs verifies Config() returns the
// correct DeploymentID for barriers created with different IDs.
// --- Rotate resets trackers ---

// TestBarrier_RotateResetsBothTrackers verifies that after a successful
// Rotate(), the invocation counter's Reset and nonce tracker's Clear are called,
// confirming both trackers are reset for the new epoch, and that Encrypt and
// Decrypt work on the new epoch afterwards.
func TestBarrier_RotateResetsBothTrackers(t *testing.T) {
	t.Parallel()
	mockIC := &mockInvocationCounter{enabled: true}
	mockNT := &mockNonceTracker{enabled: true}
	cfg := testBarrierConfig(1)
	cfg.InvocationCounter = mockIC
	cfg.NonceTrackerProvider = mockNT
	b := MustNewBarrier(cfg)
	s := testStrategy(t)

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	newEpoch, err := b.Rotate(context.Background())
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
	b := MustNewBarrier(cfg)
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

// --- Encrypt with invocation counter error ---

// TestBarrier_EncryptWithInvocationCounterError verifies that when the
// invocation counter's Increment returns ErrInvocationLimitExceeded, Encrypt
// propagates that error.
func TestBarrier_EncryptWithInvocationCounterError(t *testing.T) {
	t.Parallel()
	mockIC := &mockInvocationCounter{enabled: true, incrementErr: ErrInvocationLimitExceeded}
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationCounter = mockIC
	cfg.NonceTracking = BoolPtr(false)
	b := MustNewBarrier(cfg)
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

// --- NewBarrier rejects an invalid config; MustNewBarrier panics on it ---

// TestNewBarrier_ReturnsErrorOnInvalidAlgorithm pins the reason NewBarrier's
// signature changed: pkg/seal is embedded in other products' processes, and an
// invalid algorithm — the normal outcome of a config read from a file or an
// API request — must be a recoverable error, never a process-killing panic.
func TestNewBarrier_ReturnsErrorOnInvalidAlgorithm(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		alg  SymmetricAlgorithm
	}{
		{"unknown algorithm", SymmetricAlgorithm(0xFF)},
		{"zero value algorithm", SymmetricAlgorithm(0x00)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var b *Barrier
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("NewBarrier must not panic on an invalid config, got %v", r)
					}
				}()
				b, err = NewBarrier(BarrierConfig{Algorithm: tc.alg, DeploymentID: 1})
			}()

			if !errors.Is(err, ErrUnsupportedAlgorithm) {
				t.Fatalf("NewBarrier err = %v, want ErrUnsupportedAlgorithm", err)
			}
			if b != nil {
				t.Fatal("NewBarrier must return a nil Barrier alongside the error")
			}
		})
	}
}

// TestNewBarrier_ValidConfig asserts the success path returns a usable barrier
// and a nil error, so the two-value signature is not merely error-shaped.
// TestMustNewBarrier_PanicsOnInvalidAlgorithm pins the MustXxx half of the
// pair: the panic still exists, but only behind the name that advertises it.
func TestMustNewBarrier_PanicsOnInvalidAlgorithm(t *testing.T) {
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
		if !bytes.Contains([]byte(msg), []byte("seal.MustNewBarrier")) {
			t.Fatalf("panic message should contain 'seal.MustNewBarrier', got %q", msg)
		}
		if !bytes.Contains([]byte(msg), []byte("unsupported")) {
			t.Fatalf("panic message should reference unsupported algorithm, got %q", msg)
		}
	}()

	cfg := BarrierConfig{
		Algorithm:    SymmetricAlgorithm(0xFF), // invalid
		DeploymentID: 1,
	}
	MustNewBarrier(cfg)
}

// TestMustNewBarrier_ValidConfig asserts the Must form returns the barrier
// rather than only being defined for its panic.
// --- persistBackoffState with nil sealed ---

func TestBarrier_PersistBackoffState_NilSealed(t *testing.T) {
	t.Parallel()

	// A barrier that has never been initialized has sealed == nil.
	// persistBackoffState must return without panic.
	b := MustNewBarrier(DefaultBarrierConfig(1))

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

// TestBarrier_ShouldRotateKey verifies that ShouldRotateKey reports true only
// once an enabled invocation counter reaches its rotation threshold.
func TestBarrier_ShouldRotateKey(t *testing.T) {
	seeded := func(n int64) func() *Barrier {
		return func() *Barrier {
			counter := NewInvocationCounter(true, 1000, 95.0)
			counter.Seed(n)
			cfg := DefaultBarrierConfig(1)
			cfg.InvocationCounter = counter
			return MustNewBarrier(cfg)
		}
	}
	tests := []struct {
		name    string
		barrier func() *Barrier
		want    bool
	}{
		{"disabled counter", func() *Barrier {
			cfg := DefaultBarrierConfig(1)
			cfg.InvocationLimit = -1 // -1 disables counting
			return MustNewBarrier(cfg)
		}, false},
		// 500 of 1000 is well below the 95% threshold.
		{"below threshold", seeded(500), false},
		// 950 of 1000 is exactly the 95% threshold.
		{"at threshold", seeded(950), true},
		{"nil invocations", func() *Barrier { return &Barrier{} }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.barrier().ShouldRotateKey(); got != tc.want {
				t.Errorf("ShouldRotateKey = %v, want %v", got, tc.want)
			}
		})
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

// --- Invocation budget across restarts (NIST SP 800-38D) ---

// TestBarrier_RestartRotatesRatherThanReGrantingBudget verifies the NIST
// SP 800-38D budget is not re-granted on restart.
//
// The old behaviour seeded the counter to the ROTATION THRESHOLD (95%) and let
// the process encrypt through the remaining 5% — 214,748,364 invocations — on
// every restart, against the SAME epoch DEK, forever. This asserts the
// replacement: a restart-unseal that cannot prove the loaded epoch's spent
// budget moves to a FRESH epoch whose count genuinely starts at zero.
func TestBarrier_RestartRotatesRatherThanReGrantingBudget(t *testing.T) {
	b, s := testInitializedBarrier(t)
	if _, err := b.Encrypt(nil, []byte("data")); err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	sealed := b.SealedRootKeyData()
	if sealed.Epoch != 1 {
		t.Fatalf("pre-restart epoch = %d, want 1", sealed.Epoch)
	}

	// Simulate a process restart: a brand-new barrier restored from the
	// persisted sealed root key, with a fresh in-memory counter.
	b2 := MustNewBarrier(testBarrierConfig(1))
	if err := b2.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b2.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}

	// The loaded epoch must not still be the write epoch: its spent budget is
	// unknowable, so continuing on it is exactly the defect.
	if got := b2.CurrentEpoch(); got != 2 {
		t.Fatalf("epoch after restart-unseal = %d, want 2 (a fresh epoch with a known-zero count)", got)
	}

	ic := b2.InvocationCounter()
	if ic == nil || !ic.IsEnabled() {
		t.Fatal("invocation counter must be enabled by default")
	}
	// Assert the ACTUAL counter, not a proxy: the fresh epoch starts at zero,
	// not 4080218932 of a 4294967296 budget "already spent" on a key that had
	// in truth performed one encryption.
	if got := ic.Count(); got != 0 {
		t.Fatalf("invocation count on the fresh epoch = %d, want 0", got)
	}
	if b2.ShouldRotateKey() {
		t.Fatal("a freshly rotated epoch must not already be at its rotation threshold")
	}

	// The new epoch is usable, and the old one is still readable.
	ct2, err := b2.Encrypt(nil, []byte("more data"))
	if err != nil {
		t.Fatalf("Encrypt on the fresh epoch: %v", err)
	}
	if epoch := binary.BigEndian.Uint64(ct2[:8]); epoch != 2 {
		t.Fatalf("ciphertext header epoch = %d, want 2", epoch)
	}
}

// TestBarrier_RestartWithoutPersisterRefusesToEncrypt is the other half: a
// barrier that CANNOT rotate (no StatePersister — what DefaultBarrierConfig
// produces) must refuse to encrypt under the loaded epoch rather than help
// itself to another slice of that DEK's budget. Decryption stays available, so
// the node can still read and serve its data.
func TestBarrier_RestartWithoutPersisterRefusesToEncrypt(t *testing.T) {
	b := MustNewBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer func() { _ = s.Close() }()
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	ct, err := b.Encrypt(nil, []byte("written before the restart"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	sealed := b.SealedRootKeyData()

	b2 := MustNewBarrier(DefaultBarrierConfig(1))
	if err := b2.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b2.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}

	// The counter is seeded to the FULL limit, not to the 95% threshold: the
	// remaining 5% would otherwise be headroom re-granted on every restart.
	ic := b2.InvocationCounter()
	if got, limit := ic.Count(), ic.Limit(); got != limit {
		t.Fatalf("seeded count = %d, want the full limit %d (no re-granted headroom)", got, limit)
	}
	if _, err := b2.Encrypt(nil, []byte("must not be written")); !errors.Is(err, ErrInvocationLimitExceeded) {
		t.Fatalf("Encrypt after unprovable-budget restart = %v, want ErrInvocationLimitExceeded", err)
	}

	// Reads are unaffected: failing closed must not cost the node its data.
	got, err := b2.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt after unprovable-budget restart: %v", err)
	}
	if !bytes.Equal(got, []byte("written before the restart")) {
		t.Fatal("decrypted plaintext does not match")
	}
}

// TestBarrier_SealUnsealInProcessKeepsAuthoritativeCount pins the boundary: an
// in-process seal/unseal cycle keeps the SAME counter object, which is still an
// accurate count for the same epoch, so it must NOT be re-seeded and must NOT
// force a rotation. A "count == 0 means unseeded" test cannot express this —
// zero is both "never encrypted" and "fresh process" — which is why the barrier
// tracks authority explicitly.
func TestBarrier_SealUnsealInProcessKeepsAuthoritativeCount(t *testing.T) {
	b, s := testInitializedBarrier(t)
	for i := 0; i < 3; i++ {
		if _, err := b.Encrypt(nil, []byte("data")); err != nil {
			t.Fatalf("Encrypt: %v", err)
		}
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if got := b.CurrentEpoch(); got != 1 {
		t.Fatalf("epoch after in-process seal/unseal = %d, want 1 (no rotation)", got)
	}
	if got := b.InvocationCounter().Count(); got != 3 {
		t.Fatalf("invocation count after in-process seal/unseal = %d, want the true 3", got)
	}
	if b.ShouldRotateKey() {
		t.Fatal("in-process unseal must not force rotation")
	}
}

// TestBarrier_UnsealTrustsInjectedCounter verifies that an embedder-
// injected InvocationCounter (which owns persistence per the documented
// contract) is never overridden by the pessimistic seed.
func TestBarrier_UnsealTrustsInjectedCounter(t *testing.T) {
	custom := NewInvocationCounter(true, 0, 0)
	custom.Seed(0) // embedder restored "0 invocations" from durable state

	cfg := DefaultBarrierConfig(1)
	cfg.InvocationCounter = custom
	b := MustNewBarrier(cfg)
	s := testStrategy(t)
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sealed := b.SealedRootKeyData()

	cfg2 := DefaultBarrierConfig(1)
	custom2 := NewInvocationCounter(true, 0, 0)
	cfg2.InvocationCounter = custom2
	b2 := MustNewBarrier(cfg2)
	if err := b2.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b2.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if got := custom2.Count(); got != 0 {
		t.Fatalf("injected counter was overridden: count = %d, want 0", got)
	}
	if b2.ShouldRotateKey() {
		t.Fatal("injected counter must be trusted; no forced rotation")
	}
}
