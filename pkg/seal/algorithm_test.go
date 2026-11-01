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

// ----- TestSymmetricAlgorithm_KeySize ----------------------------------------

func TestSymmetricAlgorithm_KeySize(t *testing.T) {
	t.Run("known algorithms return correct key size", func(t *testing.T) {
		for _, tc := range knownAlgorithms {
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
// exactly as it treats an identifier nobody has ever defined, and
// newAEADForAlgorithm must refuse rather than construct.
//
// In a non-FIPS build unregisteredAlgorithms is empty and this test is a no-op,
// which is the correct reading: nothing is excluded there.
func TestUnregisteredAlgorithmsAreRefused(t *testing.T) {
	for _, alg := range unregisteredAlgorithms {
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
			// 32 bytes is the correct key length for both excluded algorithms,
			// so a registry that still held them would succeed here.
			if _, err := newAEADForAlgorithm(alg, make([]byte, 32)); !errors.Is(err, ErrUnsupportedAlgorithm) {
				t.Fatalf("newAEADForAlgorithm() = %v, want ErrUnsupportedAlgorithm", err)
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
		t.Run(tc.label, func(t *testing.T) {
			key := randKey(t, tc.keySize)

			aead, err := newAESGCM(key)
			if err != nil {
				t.Fatalf("newAESGCM(%d-byte key) unexpected error: %v", tc.keySize, err)
			}
			if aead == nil {
				t.Fatal("newAESGCM() returned nil AEAD")
			}

			// newAESGCM builds a module-nonce GCM (cipher.NewGCMWithRandomNonce),
			// which takes no caller nonce at all.
			if got := aead.NonceSize(); got != 0 {
				t.Fatalf("AEAD.NonceSize() = %d, want 0 (module-generated nonce)", got)
			}

			// Overhead is the 12-byte nonce the module prepends plus the
			// 16-byte GCM tag, which is exactly the number of bytes the
			// pre-migration framing spent on nonce + tag.
			const wantOverhead = 12 + aeadTagSize
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
// TestAlgorithmRoundtrip_TamperedCiphertextFails verifies that modifying even
// a single byte of the ciphertext causes authentication failure.
func TestAlgorithmRoundtrip_TamperedCiphertextFails(t *testing.T) {
	plaintext := []byte("sensitive payload")

	for _, tc := range knownAlgorithms {
		t.Run(tc.name, func(t *testing.T) {
			key := randKey(t, tc.keySize)
			nonce := tc.sealNonce(t)

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
		t.Run(tc.name, func(t *testing.T) {
			key := randKey(t, tc.keySize)
			nonce := tc.sealNonce(t)

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
