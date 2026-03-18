// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package seal

import (
	"crypto/rand"
	"io"
	"testing"
)

func TestSoftwareAEADProvider_NewAEAD_AllAlgorithms(t *testing.T) {
	t.Parallel()

	provider := NewSoftwareAEADProvider()

	algorithms := []struct {
		name    string
		alg     SymmetricAlgorithm
		keySize int
	}{
		{"AES-128-GCM", AlgAES128GCM, 16},
		{"AES-192-GCM", AlgAES192GCM, 24},
		{"AES-256-GCM", AlgAES256GCM, 32},
		{"ChaCha20-Poly1305", AlgChaCha20Poly1305, 32},
		{"XChaCha20-Poly1305", AlgXChaCha20Poly1305, 32},
	}

	for _, tc := range algorithms {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := make([]byte, tc.keySize)
			if _, err := io.ReadFull(rand.Reader, key); err != nil {
				t.Fatalf("failed to generate key: %v", err)
			}

			aead, err := provider.NewAEAD(tc.alg, key)
			if err != nil {
				t.Fatalf("NewAEAD failed: %v", err)
			}

			// Verify AEAD round-trip.
			plaintext := []byte("hello world")
			nonce := make([]byte, aead.NonceSize())
			if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
				t.Fatalf("failed to generate nonce: %v", err)
			}

			ciphertext := aead.Seal(nil, nonce, plaintext, nil)
			decrypted, err := aead.Open(nil, nonce, ciphertext, nil)
			if err != nil {
				t.Fatalf("decryption failed: %v", err)
			}

			if string(decrypted) != string(plaintext) {
				t.Fatalf("decrypted = %q, want %q", decrypted, plaintext)
			}
		})
	}
}

func TestSoftwareAEADProvider_NewAEAD_InvalidAlgorithm(t *testing.T) {
	t.Parallel()

	provider := NewSoftwareAEADProvider()
	_, err := provider.NewAEAD(SymmetricAlgorithm(0xFF), make([]byte, 32))
	if err == nil {
		t.Fatal("expected error for invalid algorithm, got nil")
	}
}

func TestSoftwareAEADProvider_NewAEAD_InvalidKeySize(t *testing.T) {
	t.Parallel()

	provider := NewSoftwareAEADProvider()
	// AES-256-GCM requires 32 bytes; supply 16 to trigger ErrInvalidKeySize.
	_, err := provider.NewAEAD(AlgAES256GCM, make([]byte, 16))
	if err == nil {
		t.Fatal("expected error for invalid key size, got nil")
	}
}

func TestSoftwareAEADProvider_InterfaceCompliance(t *testing.T) {
	t.Parallel()
	var _ AEADProvider = NewSoftwareAEADProvider()
}
