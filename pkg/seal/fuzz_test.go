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

//go:build quicraft_fuzz

package seal

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

const (
	// fuzzMaxPlaintextSize caps fuzzed plaintext to prevent excessive memory
	// usage during long fuzzing sessions.
	fuzzMaxPlaintextSize = 64 * 1024 // 64 KiB
)

// fuzzPassphrase is a fixed passphrase used by all fuzz helpers for the
// SoftwareStrategy. It never changes because the fuzz targets exercise the
// barrier's AEAD layer, not the KDF.
var fuzzPassphrase = []byte("fuzz-test-passphrase-32bytes!!")

// newFuzzBarrier returns an initialized, unsealed Barrier backed by the
// SoftwareStrategy with Argon2id at minimal parameters to keep fuzz
// iterations fast. The barrier uses AES-256-GCM with nonce tracking disabled
// for speed.
func newFuzzBarrier(t testing.TB) *Barrier {
	t.Helper()

	config := BarrierConfig{
		Algorithm:     AlgAES256GCM,
		DeploymentID:  42,
		NonceTracking: false,
	}

	barrier := NewBarrier(config)

	strategy, err := NewSoftwareStrategyWithParams(fuzzPassphrase, 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("fuzz: failed to create software strategy: %v", err)
	}

	ctx := context.Background()
	if err := barrier.Initialize(ctx, strategy, Credentials{}); err != nil {
		t.Fatalf("fuzz: failed to initialize barrier: %v", err)
	}

	return barrier
}

// FuzzBarrierEncryptDecryptRoundTrip verifies that encrypting arbitrary
// plaintext and decrypting the result always produces the original input.
// It also verifies that distinct plaintexts yield distinct ciphertexts
// (with overwhelming probability due to random nonces).
func FuzzBarrierEncryptDecryptRoundTrip(f *testing.F) {
	// Corpus seeds covering key boundaries.
	f.Add([]byte{})                              // empty
	f.Add([]byte{0x42})                          // 1 byte
	f.Add(make([]byte, 15))                      // AES block - 1
	f.Add(make([]byte, 16))                      // AES block aligned
	f.Add(make([]byte, 17))                      // AES block + 1
	f.Add(make([]byte, 32))                      // 2 AES blocks
	f.Add(make([]byte, 256))                     // multi-block
	f.Add(bytes.Repeat([]byte{0xff}, 1024))      // 1 KiB
	f.Add(bytes.Repeat([]byte{0xab, 0xcd}, 512)) // 1 KiB pattern

	f.Fuzz(func(t *testing.T, plaintext []byte) {
		if len(plaintext) > fuzzMaxPlaintextSize {
			t.Skip()
		}

		barrier := newFuzzBarrier(t)

		ciphertext, err := barrier.Encrypt(nil, plaintext)
		if err != nil {
			t.Fatalf("Encrypt failed: %v", err)
		}

		decrypted, err := barrier.Decrypt(nil, ciphertext)
		if err != nil {
			t.Fatalf("Decrypt failed: %v", err)
		}

		if !bytes.Equal(plaintext, decrypted) {
			t.Fatalf("round-trip mismatch: plaintext len=%d, decrypted len=%d",
				len(plaintext), len(decrypted))
		}

		// Encrypt the same plaintext a second time; ciphertexts must differ
		// because a fresh random nonce is generated each call.
		ciphertext2, err := barrier.Encrypt(nil, plaintext)
		if err != nil {
			t.Fatalf("second Encrypt failed: %v", err)
		}

		if bytes.Equal(ciphertext, ciphertext2) {
			t.Fatal("two encryptions of the same plaintext produced identical ciphertext (nonce reuse)")
		}
	})
}

// FuzzBarrierDecryptGarbage feeds random bytes to Decrypt and verifies that
// the barrier never panics and always returns an error for invalid input.
func FuzzBarrierDecryptGarbage(f *testing.F) {
	// Corpus seeds: various sizes that exercise different parse branches.
	f.Add([]byte{})                         // empty
	f.Add([]byte{0x00})                     // 1 byte
	f.Add(make([]byte, 8))                  // epoch only
	f.Add(make([]byte, 9))                  // epoch + algID
	f.Add(make([]byte, 21))                 // epoch + algID + nonce (partial)
	f.Add(make([]byte, 37))                 // minimum valid header size
	f.Add(make([]byte, 100))                // larger garbage
	f.Add(bytes.Repeat([]byte{0xff}, 256))  // all-ones pattern
	f.Add(bytes.Repeat([]byte{0x03}, 1024)) // algID=AES256GCM repeated

	f.Fuzz(func(t *testing.T, garbage []byte) {
		if len(garbage) > fuzzMaxPlaintextSize {
			t.Skip()
		}

		barrier := newFuzzBarrier(t)

		plaintext, err := barrier.Decrypt(nil, garbage)
		if err == nil {
			// If decryption succeeds on random input, that is a catastrophic
			// failure of the AEAD authentication. The probability is negligible
			// (~2^-128 for GCM tag) but we report it.
			t.Fatalf("Decrypt succeeded on garbage input (len=%d), got plaintext len=%d",
				len(garbage), len(plaintext))
		}

		// The only acceptable error is ErrDecryptionFailed.
		if !errors.Is(err, ErrDecryptionFailed) {
			t.Fatalf("Decrypt returned unexpected error type: %v", err)
		}
	})
}

// FuzzBarrierDecryptWrongEpoch encrypts plaintext at epoch 1, rotates to
// epoch 2, encrypts the same plaintext at epoch 2, then attempts to decrypt
// epoch-2 ciphertext using epoch-1's cipher context by tampering with the
// epoch header. The AEAD authentication must reject the tampered ciphertext.
func FuzzBarrierDecryptWrongEpoch(f *testing.F) {
	f.Add([]byte("epoch-cross-test"))
	f.Add([]byte{})
	f.Add(make([]byte, 16))
	f.Add(make([]byte, 32))
	f.Add(bytes.Repeat([]byte{0xde, 0xad}, 64))

	f.Fuzz(func(t *testing.T, plaintext []byte) {
		if len(plaintext) > fuzzMaxPlaintextSize {
			t.Skip()
		}

		barrier := newFuzzBarrier(t)

		// Encrypt at epoch 1.
		ct1, err := barrier.Encrypt(nil, plaintext)
		if err != nil {
			t.Fatalf("Encrypt at epoch 1 failed: %v", err)
		}

		// Rotate to epoch 2.
		newEpoch, err := barrier.Rotate()
		if err != nil {
			t.Fatalf("Rotate failed: %v", err)
		}
		if newEpoch != 2 {
			t.Fatalf("expected epoch 2, got %d", newEpoch)
		}

		// Encrypt at epoch 2.
		ct2, err := barrier.Encrypt(nil, plaintext)
		if err != nil {
			t.Fatalf("Encrypt at epoch 2 failed: %v", err)
		}

		// Verify both ciphertexts decrypt correctly with their own epochs.
		dec1, err := barrier.Decrypt(nil, ct1)
		if err != nil {
			t.Fatalf("Decrypt ct1 at epoch 1 failed: %v", err)
		}
		if !bytes.Equal(plaintext, dec1) {
			t.Fatal("epoch 1 round-trip mismatch")
		}

		dec2, err := barrier.Decrypt(nil, ct2)
		if err != nil {
			t.Fatalf("Decrypt ct2 at epoch 2 failed: %v", err)
		}
		if !bytes.Equal(plaintext, dec2) {
			t.Fatal("epoch 2 round-trip mismatch")
		}

		// Tamper: overwrite the epoch in ct2's header with epoch 1's value.
		// The wire format is [epoch:8][algID:1][nonce:N][ciphertext+tag].
		// Epoch is big-endian in the first 8 bytes.
		tampered := make([]byte, len(ct2))
		copy(tampered, ct2)
		// Write epoch 1 into the header (big-endian).
		tampered[0] = 0
		tampered[1] = 0
		tampered[2] = 0
		tampered[3] = 0
		tampered[4] = 0
		tampered[5] = 0
		tampered[6] = 0
		tampered[7] = 1

		// Decrypt must fail because the AAD binds ciphertext to its epoch.
		_, err = barrier.Decrypt(nil, tampered)
		if err == nil {
			t.Fatal("Decrypt succeeded with tampered epoch header (cross-epoch splice)")
		}
		if !errors.Is(err, ErrDecryptionFailed) {
			t.Fatalf("expected ErrDecryptionFailed for tampered epoch, got: %v", err)
		}
	})
}

// FuzzBarrierEncryptVariableSizes fuzzes the barrier with plaintext of varying
// lengths from 0 to fuzzMaxPlaintextSize. The fuzzer provides a seed byte to
// fill the plaintext and a length. This exercises buffer management, nonce
// generation, and AEAD seal/open across the full size range.
func FuzzBarrierEncryptVariableSizes(f *testing.F) {
	// Corpus seeds at key size boundaries.
	f.Add(uint16(0), byte(0x00))
	f.Add(uint16(1), byte(0x41))
	f.Add(uint16(15), byte(0xfe))
	f.Add(uint16(16), byte(0x00))
	f.Add(uint16(17), byte(0x01))
	f.Add(uint16(31), byte(0x80))
	f.Add(uint16(32), byte(0xff))
	f.Add(uint16(33), byte(0x42))
	f.Add(uint16(127), byte(0xab))
	f.Add(uint16(128), byte(0xcd))
	f.Add(uint16(255), byte(0xef))
	f.Add(uint16(256), byte(0x99))
	f.Add(uint16(1023), byte(0x77))
	f.Add(uint16(1024), byte(0x55))
	f.Add(uint16(4096), byte(0x33))
	f.Add(uint16(8192), byte(0x11))

	f.Fuzz(func(t *testing.T, size uint16, fill byte) {
		plaintextLen := int(size)
		if plaintextLen > fuzzMaxPlaintextSize {
			t.Skip()
		}

		plaintext := make([]byte, plaintextLen)
		for i := range plaintext {
			plaintext[i] = fill
		}

		barrier := newFuzzBarrier(t)

		ciphertext, err := barrier.Encrypt(nil, plaintext)
		if err != nil {
			t.Fatalf("Encrypt failed for size %d: %v", plaintextLen, err)
		}

		// Verify ciphertext is larger than plaintext by at least the header
		// overhead: epoch(8) + algID(1) + nonce(12) + tag(16) = 37 bytes.
		expectedMinSize := plaintextLen + epochSize + algIDSize + AlgAES256GCM.NonceSize() + 16
		if len(ciphertext) < expectedMinSize {
			t.Fatalf("ciphertext too short: got %d, want >= %d", len(ciphertext), expectedMinSize)
		}

		decrypted, err := barrier.Decrypt(nil, ciphertext)
		if err != nil {
			t.Fatalf("Decrypt failed for size %d: %v", plaintextLen, err)
		}

		if !bytes.Equal(plaintext, decrypted) {
			t.Fatalf("round-trip mismatch at size %d: plaintext len=%d, decrypted len=%d",
				plaintextLen, len(plaintext), len(decrypted))
		}

		// Verify that pre-allocated dst buffer works correctly.
		dst := make([]byte, 0, len(ciphertext)+64)
		ciphertext2, err := barrier.Encrypt(dst, plaintext)
		if err != nil {
			t.Fatalf("Encrypt with dst failed for size %d: %v", plaintextLen, err)
		}

		decrypted2, err := barrier.Decrypt(nil, ciphertext2)
		if err != nil {
			t.Fatalf("Decrypt of dst-buffered ciphertext failed for size %d: %v", plaintextLen, err)
		}

		if !bytes.Equal(plaintext, decrypted2) {
			t.Fatalf("dst-buffered round-trip mismatch at size %d", plaintextLen)
		}
	})
}
