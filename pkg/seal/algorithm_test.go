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

	"golang.org/x/crypto/chacha20poly1305"
)

// knownAlgorithms enumerates all five defined SymmetricAlgorithm constants
// so table-driven subtests can iterate over them without repetition.
var knownAlgorithms = []struct {
	alg       SymmetricAlgorithm
	name      string
	keySize   int
	nonceSize int
}{
	{AlgAES128GCM, "AES-128-GCM", 16, 12},
	{AlgAES192GCM, "AES-192-GCM", 24, 12},
	{AlgAES256GCM, "AES-256-GCM", 32, 12},
	{AlgChaCha20Poly1305, "ChaCha20-Poly1305", chacha20poly1305.KeySize, chacha20poly1305.NonceSize},
	{AlgXChaCha20Poly1305, "XChaCha20-Poly1305", chacha20poly1305.KeySize, chacha20poly1305.NonceSizeX},
}

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

// ----- TestSymmetricAlgorithm_KeySize ----------------------------------------

func TestSymmetricAlgorithm_KeySize(t *testing.T) {
	t.Run("known algorithms return correct key size", func(t *testing.T) {
		for _, tc := range knownAlgorithms {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				got := tc.alg.KeySize()
				if got != tc.keySize {
					t.Fatalf("KeySize() = %d, want %d", got, tc.keySize)
				}
			})
		}
	})

	t.Run("unknown algorithm returns 0", func(t *testing.T) {
		if got := unknownAlg.KeySize(); got != 0 {
			t.Fatalf("KeySize() = %d, want 0 for unknown algorithm", got)
		}
	})
}

// ----- TestSymmetricAlgorithm_NonceSize --------------------------------------

func TestSymmetricAlgorithm_NonceSize(t *testing.T) {
	t.Run("known algorithms return correct nonce size", func(t *testing.T) {
		for _, tc := range knownAlgorithms {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				got := tc.alg.NonceSize()
				if got != tc.nonceSize {
					t.Fatalf("NonceSize() = %d, want %d", got, tc.nonceSize)
				}
			})
		}
	})

	t.Run("unknown algorithm returns 0", func(t *testing.T) {
		if got := unknownAlg.NonceSize(); got != 0 {
			t.Fatalf("NonceSize() = %d, want 0 for unknown algorithm", got)
		}
	})
}

// ----- TestSymmetricAlgorithm_String -----------------------------------------

func TestSymmetricAlgorithm_String(t *testing.T) {
	t.Run("known algorithms return readable names", func(t *testing.T) {
		for _, tc := range knownAlgorithms {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				got := tc.alg.String()
				if got != tc.name {
					t.Fatalf("String() = %q, want %q", got, tc.name)
				}
			})
		}
	})

	t.Run("unknown algorithm returns formatted unknown string", func(t *testing.T) {
		want := fmt.Sprintf("unknown(0x%02x)", byte(unknownAlg))
		got := unknownAlg.String()
		if got != want {
			t.Fatalf("String() = %q, want %q", got, want)
		}
	})

	t.Run("zero value algorithm returns formatted unknown string", func(t *testing.T) {
		var zero SymmetricAlgorithm
		want := fmt.Sprintf("unknown(0x%02x)", byte(zero))
		got := zero.String()
		if got != want {
			t.Fatalf("String() = %q, want %q", got, want)
		}
	})
}

// ----- TestSymmetricAlgorithm_Valid ------------------------------------------

func TestSymmetricAlgorithm_Valid(t *testing.T) {
	t.Run("known algorithms return true", func(t *testing.T) {
		for _, tc := range knownAlgorithms {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				if !tc.alg.Valid() {
					t.Fatalf("Valid() = false for known algorithm %s", tc.name)
				}
			})
		}
	})

	t.Run("unknown algorithm returns false", func(t *testing.T) {
		if unknownAlg.Valid() {
			t.Fatal("Valid() = true for unknown algorithm, want false")
		}
	})

	t.Run("zero value algorithm returns false", func(t *testing.T) {
		var zero SymmetricAlgorithm
		if zero.Valid() {
			t.Fatal("Valid() = true for zero-value algorithm, want false")
		}
	})
}

// ----- TestNewAEADForAlgorithm -----------------------------------------------

func TestNewAEADForAlgorithm(t *testing.T) {
	t.Run("each algorithm constructs AEAD with correct nonce and overhead", func(t *testing.T) {
		for _, tc := range knownAlgorithms {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				key := randKey(t, tc.keySize)

				aead, err := newAEADForAlgorithm(tc.alg, key)
				if err != nil {
					t.Fatalf("newAEADForAlgorithm() unexpected error: %v", err)
				}
				if aead == nil {
					t.Fatal("newAEADForAlgorithm() returned nil AEAD")
				}

				// The AEAD nonce size must match the algorithm's declared nonce size.
				if got := aead.NonceSize(); got != tc.nonceSize {
					t.Fatalf("AEAD.NonceSize() = %d, want %d", got, tc.nonceSize)
				}

				// All algorithms use a 16-byte (128-bit) Poly1305 / GCM authentication tag.
				const wantOverhead = 16
				if got := aead.Overhead(); got != wantOverhead {
					t.Fatalf("AEAD.Overhead() = %d, want %d", got, wantOverhead)
				}
			})
		}
	})
}

// ----- TestNewAEADForAlgorithm_InvalidKeySize --------------------------------

func TestNewAEADForAlgorithm_InvalidKeySize(t *testing.T) {
	tests := []struct {
		alg     SymmetricAlgorithm
		keySize int // intentionally wrong
	}{
		{AlgAES128GCM, 32},        // 32 instead of 16
		{AlgAES192GCM, 16},        // 16 instead of 24
		{AlgAES256GCM, 16},        // 16 instead of 32
		{AlgChaCha20Poly1305, 16}, // 16 instead of 32
		{AlgXChaCha20Poly1305, 0}, // empty key
	}

	for _, tc := range tests {
		tc := tc
		t.Run(fmt.Sprintf("%s_keySize_%d", tc.alg, tc.keySize), func(t *testing.T) {
			key := make([]byte, tc.keySize)

			_, err := newAEADForAlgorithm(tc.alg, key)
			if err == nil {
				t.Fatal("newAEADForAlgorithm() expected error for wrong key size, got nil")
			}
			if !errors.Is(err, ErrInvalidKeySize) {
				t.Fatalf("errors.Is(err, ErrInvalidKeySize) = false, got: %v", err)
			}
		})
	}
}

// ----- TestNewAEADForAlgorithm_UnsupportedAlgorithm --------------------------

func TestNewAEADForAlgorithm_UnsupportedAlgorithm(t *testing.T) {
	t.Run("unknown algorithm returns ErrUnsupportedAlgorithm", func(t *testing.T) {
		_, err := newAEADForAlgorithm(unknownAlg, make([]byte, 32))
		if err == nil {
			t.Fatal("newAEADForAlgorithm() expected error for unknown algorithm, got nil")
		}
		if !errors.Is(err, ErrUnsupportedAlgorithm) {
			t.Fatalf("errors.Is(err, ErrUnsupportedAlgorithm) = false, got: %v", err)
		}
	})

	t.Run("zero algorithm returns ErrUnsupportedAlgorithm", func(t *testing.T) {
		var zero SymmetricAlgorithm
		_, err := newAEADForAlgorithm(zero, make([]byte, 32))
		if err == nil {
			t.Fatal("newAEADForAlgorithm() expected error for zero algorithm, got nil")
		}
		if !errors.Is(err, ErrUnsupportedAlgorithm) {
			t.Fatalf("errors.Is(err, ErrUnsupportedAlgorithm) = false, got: %v", err)
		}
	})
}

// ----- TestHasAESNI ----------------------------------------------------------

func TestHasAESNI(t *testing.T) {
	t.Run("returns a bool without panicking", func(t *testing.T) {
		// HasAESNI is entirely hardware-dependent; we can only assert that it
		// returns a valid bool and does not panic on any supported architecture.
		_ = HasAESNI()
	})

	t.Run("return value is stable across repeated calls", func(t *testing.T) {
		first := HasAESNI()
		second := HasAESNI()
		if first != second {
			t.Fatalf("HasAESNI() is not stable: first=%v second=%v", first, second)
		}
	})
}

// ----- TestSelectOptimalAlgorithm --------------------------------------------

func TestSelectOptimalAlgorithm(t *testing.T) {
	t.Run("hardware backed always selects AES256GCM", func(t *testing.T) {
		got := SelectOptimalAlgorithm(true)
		if got != AlgAES256GCM {
			t.Fatalf("SelectOptimalAlgorithm(true) = %s, want AES-256-GCM", got)
		}
	})

	t.Run("non-hardware backed returns a valid algorithm", func(t *testing.T) {
		got := SelectOptimalAlgorithm(false)
		if !got.Valid() {
			t.Fatalf("SelectOptimalAlgorithm(false) returned invalid algorithm: %s", got)
		}
	})

	t.Run("non-hardware backed returns AES256GCM or ChaCha20Poly1305", func(t *testing.T) {
		// On CPUs with AES-NI, AES256GCM is preferred; on others, ChaCha20-Poly1305.
		got := SelectOptimalAlgorithm(false)
		if got != AlgAES256GCM && got != AlgChaCha20Poly1305 {
			t.Fatalf("SelectOptimalAlgorithm(false) = %s, want AES-256-GCM or ChaCha20-Poly1305", got)
		}
	})

	t.Run("hardware backed result matches AES-NI path when AES-NI present", func(t *testing.T) {
		// Regardless of AES-NI availability, hardware-backed always wins with
		// AES-256-GCM because the hardware token provides its own acceleration.
		hw := SelectOptimalAlgorithm(true)
		if hw != AlgAES256GCM {
			t.Fatalf("hardware-backed SelectOptimalAlgorithm = %s, want AES-256-GCM", hw)
		}
	})
}

// ----- TestNewAESGCM_AllKeySizes ---------------------------------------------

func TestNewAESGCM_AllKeySizes(t *testing.T) {
	tests := []struct {
		keySize int
		label   string
	}{
		{16, "AES-128"},
		{24, "AES-192"},
		{32, "AES-256"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			key := randKey(t, tc.keySize)

			aead, err := newAESGCM(key)
			if err != nil {
				t.Fatalf("newAESGCM(%d-byte key) unexpected error: %v", tc.keySize, err)
			}
			if aead == nil {
				t.Fatal("newAESGCM() returned nil AEAD")
			}

			// GCM always uses a 12-byte nonce.
			const wantNonce = 12
			if got := aead.NonceSize(); got != wantNonce {
				t.Fatalf("AEAD.NonceSize() = %d, want %d", got, wantNonce)
			}

			// GCM authentication tag is 16 bytes.
			const wantOverhead = 16
			if got := aead.Overhead(); got != wantOverhead {
				t.Fatalf("AEAD.Overhead() = %d, want %d", got, wantOverhead)
			}
		})
	}

	t.Run("invalid key size wraps error in CipherError", func(t *testing.T) {
		_, err := newAESGCM(make([]byte, 7)) // AES requires 16, 24, or 32 bytes
		if err == nil {
			t.Fatal("newAESGCM() expected error for invalid key, got nil")
		}
		var cipherErr *CipherError
		if !errors.As(err, &cipherErr) {
			t.Fatalf("expected *CipherError, got %T: %v", err, err)
		}
	})
}

// ----- TestAlgorithmRoundtrip ------------------------------------------------

// TestAlgorithmRoundtrip verifies that plaintext encrypted with each algorithm
// decrypts back to the original plaintext, ensuring the AEAD seal/open cycle
// is correct for every supported algorithm.
func TestAlgorithmRoundtrip(t *testing.T) {
	plaintext := []byte("the quick brown fox jumps over the lazy dog")
	additionalData := []byte("authenticated header data")

	for _, tc := range knownAlgorithms {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			key := randKey(t, tc.keySize)

			aead, err := newAEADForAlgorithm(tc.alg, key)
			if err != nil {
				t.Fatalf("newAEADForAlgorithm() error: %v", err)
			}

			// Generate a random nonce of the correct length for this algorithm.
			nonce := randKey(t, tc.nonceSize)

			// Encrypt.
			ciphertext := aead.Seal(nil, nonce, plaintext, additionalData)
			if len(ciphertext) == 0 {
				t.Fatal("Seal() returned empty ciphertext")
			}

			// Decrypt with correct key, nonce, and additional data.
			decrypted, err := aead.Open(nil, nonce, ciphertext, additionalData)
			if err != nil {
				t.Fatalf("Open() unexpected error: %v", err)
			}
			if string(decrypted) != string(plaintext) {
				t.Fatalf("roundtrip mismatch:\n  got:  %q\n  want: %q", decrypted, plaintext)
			}
		})
	}
}

// TestAlgorithmRoundtrip_TamperedCiphertextFails verifies that modifying even
// a single byte of the ciphertext causes authentication failure.
func TestAlgorithmRoundtrip_TamperedCiphertextFails(t *testing.T) {
	plaintext := []byte("sensitive payload")

	for _, tc := range knownAlgorithms {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			key := randKey(t, tc.keySize)
			nonce := randKey(t, tc.nonceSize)

			aead, err := newAEADForAlgorithm(tc.alg, key)
			if err != nil {
				t.Fatalf("newAEADForAlgorithm() error: %v", err)
			}

			ciphertext := aead.Seal(nil, nonce, plaintext, nil)

			// Flip the first byte to simulate tampering.
			tampered := make([]byte, len(ciphertext))
			copy(tampered, ciphertext)
			tampered[0] ^= 0xFF

			_, err = aead.Open(nil, nonce, tampered, nil)
			if err == nil {
				t.Fatal("Open() expected error for tampered ciphertext, got nil")
			}
		})
	}
}

// TestAlgorithmRoundtrip_WrongAdditionalDataFails verifies that mismatched
// additional data is detected by the authentication tag.
func TestAlgorithmRoundtrip_WrongAdditionalDataFails(t *testing.T) {
	plaintext := []byte("authenticated payload")
	authData := []byte("original AD")
	wrongAD := []byte("tampered AD")

	for _, tc := range knownAlgorithms {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			key := randKey(t, tc.keySize)
			nonce := randKey(t, tc.nonceSize)

			aead, err := newAEADForAlgorithm(tc.alg, key)
			if err != nil {
				t.Fatalf("newAEADForAlgorithm() error: %v", err)
			}

			ciphertext := aead.Seal(nil, nonce, plaintext, authData)

			_, err = aead.Open(nil, nonce, ciphertext, wrongAD)
			if err == nil {
				t.Fatal("Open() expected error for wrong additional data, got nil")
			}
		})
	}
}
