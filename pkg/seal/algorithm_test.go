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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"testing"
)

// unknownAlg is a SymmetricAlgorithm value that is not registered in the
// algorithmRegistry and is used to exercise all error/zero-value paths.
const unknownAlg SymmetricAlgorithm = 0xFF

// randKey returns a freshly generated random key of the given size, failing
// the test immediately on any I/O error.
func randKey(t *testing.T, size int) []byte {
	t.Helper()
	key := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("randKey: %v", err)
	}
	return key
}

// ----- TestSymmetricAlgorithm_Accessors -------------------------------------

// TestSymmetricAlgorithm_Accessors checks every registry-backed accessor of each
// registered algorithm. The unregistered side of the same accessors is asserted
// by TestUnregisteredAlgorithmsAreRefused.
func TestSymmetricAlgorithm_Accessors(t *testing.T) {
	for _, tc := range knownAlgorithms {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.alg.Valid() {
				t.Fatalf("Valid() = false for known algorithm %s", tc.name)
			}
			if got := tc.alg.KeySize(); got != tc.keySize {
				t.Fatalf("KeySize() = %d, want %d", got, tc.keySize)
			}
			if got := tc.alg.NonceSize(); got != tc.nonceSize {
				t.Fatalf("NonceSize() = %d, want %d", got, tc.nonceSize)
			}
			if got := tc.alg.String(); got != tc.name {
				t.Fatalf("String() = %q, want %q", got, tc.name)
			}
		})
	}
}

// ----- TestNewAEADForAlgorithm -----------------------------------------------

func TestNewAEADForAlgorithm(t *testing.T) {
	t.Run("each algorithm constructs AEAD with correct nonce and overhead", func(t *testing.T) {
		for _, tc := range knownAlgorithms {
			t.Run(tc.name, func(t *testing.T) {
				key := randKey(t, tc.keySize)

				aead, err := newAEADForAlgorithm(tc.alg, key)
				if err != nil {
					t.Fatalf("newAEADForAlgorithm() unexpected error: %v", err)
				}
				if aead == nil {
					t.Fatal("newAEADForAlgorithm() returned nil AEAD")
				}

				// A caller-nonce AEAD reports the algorithm's nonce width; a
				// module-nonce AEAD reports zero, because it accepts no caller
				// nonce and carries its own inside the payload instead.
				if got := aead.NonceSize(); got != tc.wantAEADNonceSize() {
					t.Fatalf("AEAD.NonceSize() = %d, want %d", got, tc.wantAEADNonceSize())
				}

				// Every algorithm uses a 16-byte (128-bit) Poly1305 / GCM
				// authentication tag; a module-nonce AEAD counts its prepended
				// nonce in Overhead() on top of that, which is what keeps the
				// frame size identical to the caller-nonce layout it replaced.
				if got := aead.Overhead(); got != tc.wantAEADOverhead() {
					t.Fatalf("AEAD.Overhead() = %d, want %d", got, tc.wantAEADOverhead())
				}
			})
		}
	})
}

// ----- TestNewAEADForAlgorithm_InvalidKeySize --------------------------------

func TestNewAEADForAlgorithm_InvalidKeySize(t *testing.T) {
	for _, tc := range knownAlgorithms {
		// A key length that is valid for SOME algorithm but not this one, so
		// the rejection comes from the registry's exact-length check rather
		// than from the underlying cipher refusing a nonsense length.
		wrong := 16
		if tc.keySize == 16 {
			wrong = 32
		}
		t.Run(fmt.Sprintf("%s_keySize_%d", tc.alg, wrong), func(t *testing.T) {
			_, err := newAEADForAlgorithm(tc.alg, make([]byte, wrong))
			if err == nil {
				t.Fatal("newAEADForAlgorithm() expected error for wrong key size, got nil")
			}
			if !errors.Is(err, ErrInvalidKeySize) {
				t.Fatalf("errors.Is(err, ErrInvalidKeySize) = false, got: %v", err)
			}
		})
	}

	t.Run("empty key", func(t *testing.T) {
		_, err := newAEADForAlgorithm(knownAlgorithms[0].alg, nil)
		if !errors.Is(err, ErrInvalidKeySize) {
			t.Fatalf("errors.Is(err, ErrInvalidKeySize) = false, got: %v", err)
		}
	})
}

// TestUnregisteredAlgorithmsAreRefused is the assertion that makes the fips
// build tag mean something. In a FIPS build the ChaCha20 family is defined as a
// wire identifier but has no entry in algorithmRegistry, because
// algorithm_chacha.go is not compiled; every accessor must therefore treat it
// exactly as it treats an identifier nobody has ever defined (unknownAlg) and
// the zero value, and newAEADForAlgorithm must refuse rather than construct.
//
// In a non-FIPS build unregisteredAlgorithms is empty, so only unknownAlg and
// the zero value run: nothing is excluded there.
func TestUnregisteredAlgorithmsAreRefused(t *testing.T) {
	var zero SymmetricAlgorithm
	cases := append(append([]SymmetricAlgorithm{}, unregisteredAlgorithms...), unknownAlg, zero)
	for _, alg := range cases {
		t.Run(fmt.Sprintf("%#x", byte(alg)), func(t *testing.T) {
			if alg.Valid() {
				t.Fatal("Valid() = true for an algorithm this build does not register")
			}
			if got := alg.KeySize(); got != 0 {
				t.Fatalf("KeySize() = %d, want 0", got)
			}
			if got := alg.NonceSize(); got != 0 {
				t.Fatalf("NonceSize() = %d, want 0", got)
			}
			if got := alg.MaxPlaintextSize(); got != 0 {
				t.Fatalf("MaxPlaintextSize() = %d, want 0", got)
			}
			if want := fmt.Sprintf("unknown(0x%02x)", byte(alg)); alg.String() != want {
				t.Fatalf("String() = %q, want %q", alg.String(), want)
			}
			// 32 bytes is the correct key length for both excluded algorithms,
			// so a registry that still held them would succeed here.
			if _, err := newAEADForAlgorithm(alg, make([]byte, 32)); !errors.Is(err, ErrUnsupportedAlgorithm) {
				t.Fatalf("newAEADForAlgorithm() = %v, want ErrUnsupportedAlgorithm", err)
			}
		})
	}
}

// ----- TestHasAESNI ----------------------------------------------------------

// ----- TestSelectOptimalAlgorithm --------------------------------------------

func TestSelectOptimalAlgorithm(t *testing.T) {
	t.Run("hardware backed always selects AES256GCM", func(t *testing.T) {
		got := SelectOptimalAlgorithm(true)
		if got != AlgAES256GCM {
			t.Fatalf("SelectOptimalAlgorithm(true) = %s, want AES-256-GCM", got)
		}
	})

	t.Run("non-hardware backed returns AES256GCM or ChaCha20Poly1305", func(t *testing.T) {
		// On CPUs with AES-NI, AES256GCM is preferred; on others, ChaCha20-Poly1305.
		got := SelectOptimalAlgorithm(false)
		if got != AlgAES256GCM && got != AlgChaCha20Poly1305 {
			t.Fatalf("SelectOptimalAlgorithm(false) = %s, want AES-256-GCM or ChaCha20-Poly1305", got)
		}
	})
}

// ----- TestNewAESGCM_InvalidKeyWrapsCipherError ------------------------------

// TestNewAESGCM_InvalidKeyWrapsCipherError covers the cipher-construction
// error path that newAEADForAlgorithm's exact-length check never reaches. The
// valid AES-128/192/256 nonce and overhead shape is asserted per algorithm by
// TestNewAEADForAlgorithm.
func TestNewAESGCM_InvalidKeyWrapsCipherError(t *testing.T) {
	_, err := newAESGCM(make([]byte, 7)) // AES requires 16, 24, or 32 bytes
	if err == nil {
		t.Fatal("newAESGCM() expected error for invalid key, got nil")
	}
	var cipherErr *CipherError
	if !errors.As(err, &cipherErr) {
		t.Fatalf("expected *CipherError, got %T: %v", err, err)
	}
}

// ----- TestAlgorithmRoundtrip ------------------------------------------------

// TestAlgorithmRoundtrip_TamperingFails verifies, for every registered
// algorithm, that the authentication tag rejects both a flipped ciphertext
// byte and mismatched additional data.
func TestAlgorithmRoundtrip_TamperingFails(t *testing.T) {
	plaintext := []byte("authenticated payload")
	authData := []byte("original AD")
	wrongAD := []byte("tampered AD")

	for _, tc := range knownAlgorithms {
		t.Run(tc.name, func(t *testing.T) {
			key := randKey(t, tc.keySize)
			nonce := tc.sealNonce(t)

			aead, err := newAEADForAlgorithm(tc.alg, key)
			if err != nil {
				t.Fatalf("newAEADForAlgorithm() error: %v", err)
			}

			ciphertext := aead.Seal(nil, nonce, plaintext, authData)

			// Flip the first byte to simulate tampering.
			tampered := make([]byte, len(ciphertext))
			copy(tampered, ciphertext)
			tampered[0] ^= 0xFF
			if _, err := aead.Open(nil, nonce, tampered, authData); err == nil {
				t.Fatal("Open() expected error for tampered ciphertext, got nil")
			}

			if _, err := aead.Open(nil, nonce, ciphertext, wrongAD); err == nil {
				t.Fatal("Open() expected error for wrong additional data, got nil")
			}
		})
	}
}
