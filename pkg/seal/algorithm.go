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
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"runtime"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/sys/cpu"
)

// SymmetricAlgorithm identifies a supported authenticated encryption algorithm.
// Values are single-byte identifiers suitable for embedding in ciphertext headers
// to enable algorithm-agile decryption without out-of-band negotiation.
type SymmetricAlgorithm byte

const (
	// AlgAES128GCM selects AES-128-GCM: 16-byte key, 12-byte nonce.
	AlgAES128GCM SymmetricAlgorithm = 0x01

	// AlgAES192GCM selects AES-192-GCM: 24-byte key, 12-byte nonce.
	AlgAES192GCM SymmetricAlgorithm = 0x02

	// AlgAES256GCM selects AES-256-GCM: 32-byte key, 12-byte nonce.
	AlgAES256GCM SymmetricAlgorithm = 0x03

	// AlgChaCha20Poly1305 selects ChaCha20-Poly1305: 32-byte key, 12-byte nonce.
	// Preferred on hardware without AES-NI acceleration.
	AlgChaCha20Poly1305 SymmetricAlgorithm = 0x04

	// AlgXChaCha20Poly1305 selects XChaCha20-Poly1305: 32-byte key, 24-byte nonce.
	// The extended 192-bit nonce eliminates nonce-collision risk for large key volumes.
	AlgXChaCha20Poly1305 SymmetricAlgorithm = 0x05
)

// Sentinel errors for algorithm operations.
var (
	// ErrInvalidKeySize is returned when a key length does not match
	// the algorithm's required key size.
	ErrInvalidKeySize = errors.New("quicraft/seal: key size does not match algorithm requirements")

	// ErrUnsupportedAlgorithm is returned when an unrecognised algorithm
	// identifier is encountered.
	ErrUnsupportedAlgorithm = errors.New("quicraft/seal: unsupported symmetric algorithm")
)

// algorithmParams holds the static parameters for a SymmetricAlgorithm.
type algorithmParams struct {
	keySize   int
	nonceSize int
	name      string
	newAEAD   func(key []byte) (cipher.AEAD, error)
}

// algorithmRegistry maps algorithm identifiers to their parameters and
// constructor functions for O(1) dispatch in newAEADForAlgorithm.
var algorithmRegistry = map[SymmetricAlgorithm]algorithmParams{
	AlgAES128GCM: {
		keySize:   16,
		nonceSize: 12,
		name:      "AES-128-GCM",
		newAEAD:   newAESGCM,
	},
	AlgAES192GCM: {
		keySize:   24,
		nonceSize: 12,
		name:      "AES-192-GCM",
		newAEAD:   newAESGCM,
	},
	AlgAES256GCM: {
		keySize:   32,
		nonceSize: 12,
		name:      "AES-256-GCM",
		newAEAD:   newAESGCM,
	},
	AlgChaCha20Poly1305: {
		keySize:   chacha20poly1305.KeySize,
		nonceSize: chacha20poly1305.NonceSize,
		name:      "ChaCha20-Poly1305",
		newAEAD:   chacha20poly1305.New,
	},
	AlgXChaCha20Poly1305: {
		keySize:   chacha20poly1305.KeySize,
		nonceSize: chacha20poly1305.NonceSizeX,
		name:      "XChaCha20-Poly1305",
		newAEAD:   chacha20poly1305.NewX,
	},
}

// KeySize returns the required key length in bytes for this algorithm.
// Returns 0 for unrecognised algorithm identifiers.
func (a SymmetricAlgorithm) KeySize() int {
	if p, ok := algorithmRegistry[a]; ok {
		return p.keySize
	}
	return 0
}

// NonceSize returns the required nonce length in bytes for this algorithm.
// Returns 0 for unrecognised algorithm identifiers.
func (a SymmetricAlgorithm) NonceSize() int {
	if p, ok := algorithmRegistry[a]; ok {
		return p.nonceSize
	}
	return 0
}

// String returns the human-readable algorithm name.
// Returns a formatted unknown string for unrecognised identifiers so that
// the value is always safe to include in log output and error messages.
func (a SymmetricAlgorithm) String() string {
	if p, ok := algorithmRegistry[a]; ok {
		return p.name
	}
	return fmt.Sprintf("unknown(0x%02x)", byte(a))
}

// Valid returns true when the algorithm identifier is recognised and supported.
func (a SymmetricAlgorithm) Valid() bool {
	_, ok := algorithmRegistry[a]
	return ok
}

// newAESGCM constructs an AES-GCM AEAD. The key length determines the AES
// variant (128, 192, or 256 bit) because aes.NewCipher selects the variant
// based on key length, so the same constructor covers all three AES modes.
func newAESGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, &CipherError{Err: err}
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, &CipherError{Err: err}
	}
	return aead, nil
}

// newAEADForAlgorithm constructs a cipher.AEAD for the given algorithm and key.
// The key length is validated against the algorithm's requirements before the
// cipher is constructed. Returns ErrInvalidKeySize for wrong key length and
// ErrUnsupportedAlgorithm for unrecognised algorithm identifiers.
func newAEADForAlgorithm(alg SymmetricAlgorithm, key []byte) (cipher.AEAD, error) {
	p, ok := algorithmRegistry[alg]
	if !ok {
		return nil, ErrUnsupportedAlgorithm
	}
	if len(key) != p.keySize {
		return nil, ErrInvalidKeySize
	}
	return p.newAEAD(key)
}

// HasAESNI reports whether the current CPU supports hardware AES acceleration.
// On x86-64, this checks the AES-NI instruction set extension. On ARM64, it
// checks the cryptographic extension flag. All other architectures return false.
func HasAESNI() bool {
	switch runtime.GOARCH {
	case "amd64":
		return cpu.X86.HasAES
	case "arm64":
		return cpu.ARM64.HasAES
	default:
		return false
	}
}

// SelectOptimalAlgorithm returns the best symmetric algorithm for the current
// execution environment. Hardware-backed operations always prefer AES-256-GCM
// because the hardware token provides its own acceleration. Software-only paths
// use AES-256-GCM on CPUs with AES-NI, and fall back to ChaCha20-Poly1305 on
// CPUs without hardware AES acceleration where ChaCha20 is constant-time and
// significantly faster.
func SelectOptimalAlgorithm(isHardwareBacked bool) SymmetricAlgorithm {
	if isHardwareBacked || HasAESNI() {
		return AlgAES256GCM
	}
	return AlgChaCha20Poly1305
}
