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

	"golang.org/x/sys/cpu"
)

// SymmetricAlgorithm identifies an AEAD algorithm by a single byte embedded in every barrier ciphertext header.
type SymmetricAlgorithm byte

const (
	// AlgAES128GCM selects AES-128-GCM: 16-byte key, 12-byte nonce.
	AlgAES128GCM SymmetricAlgorithm = 0x01

	// AlgAES192GCM selects AES-192-GCM: 24-byte key, 12-byte nonce.
	AlgAES192GCM SymmetricAlgorithm = 0x02

	// AlgAES256GCM selects AES-256-GCM: 32-byte key, 12-byte nonce.
	AlgAES256GCM SymmetricAlgorithm = 0x03

	// AlgChaCha20Poly1305 selects ChaCha20-Poly1305 (32-byte key, 12-byte nonce); absent from a FIPS build.
	AlgChaCha20Poly1305 SymmetricAlgorithm = 0x04

	// AlgXChaCha20Poly1305 selects XChaCha20-Poly1305 (32-byte key, 24-byte nonce); absent from a FIPS build.
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

	// ErrAlgorithmNotFIPSApproved is returned when FIPS 140 mode is in
	// effect and the configured algorithm (ChaCha20-Poly1305 or
	// XChaCha20-Poly1305) is not FIPS-approved.
	ErrAlgorithmNotFIPSApproved = errors.New(
		"quicraft/seal: algorithm is not FIPS-approved; FIPS 140 mode requires AES-GCM")
)

// AEAD per-invocation plaintext ceilings; exceeding one panics in the cipher, so the barrier rejects it first.
const (
	// maxPlaintextGCM is GCM's ceiling: the 32-bit block counter limits one
	// invocation to (2^32 - 2) 16-byte blocks, ~64 GiB. crypto/cipher panics
	// with "crypto/cipher: message too large for GCM" beyond it.
	maxPlaintextGCM uint64 = ((1 << 32) - 2) * 16
)

// algorithmParams holds a SymmetricAlgorithm's static parameters; nonceSize is the nonce width the wire format carries.
type algorithmParams struct {
	keySize      int
	nonceSize    int
	maxPlaintext uint64
	name         string
	newAEAD      func(key []byte) (cipher.AEAD, error)
}

// algorithmRegistry maps algorithm identifiers to their parameters and constructors; a FIPS build registers AES-GCM only.
var algorithmRegistry = map[SymmetricAlgorithm]algorithmParams{
	AlgAES128GCM: {
		keySize:      16,
		nonceSize:    12,
		maxPlaintext: maxPlaintextGCM,
		name:         "AES-128-GCM",
		newAEAD:      newAESGCM,
	},
	AlgAES192GCM: {
		keySize:      24,
		nonceSize:    12,
		maxPlaintext: maxPlaintextGCM,
		name:         "AES-192-GCM",
		newAEAD:      newAESGCM,
	},
	AlgAES256GCM: {
		keySize:      32,
		nonceSize:    12,
		maxPlaintext: maxPlaintextGCM,
		name:         "AES-256-GCM",
		newAEAD:      newAESGCM,
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

// NonceSize returns the nonce length the algorithm's wire format carries, or 0 for an unregistered algorithm.
func (a SymmetricAlgorithm) NonceSize() int {
	if p, ok := algorithmRegistry[a]; ok {
		return p.nonceSize
	}
	return 0
}

// MaxPlaintextSize returns the largest plaintext one invocation may seal, or 0 for an unregistered algorithm.
func (a SymmetricAlgorithm) MaxPlaintextSize() uint64 {
	if p, ok := algorithmRegistry[a]; ok {
		return p.maxPlaintext
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

// newAESGCM constructs an AES-GCM AEAD with a module-generated nonce (cipher.NewGCMWithRandomNonce); the key length selects AES-128, -192 or -256.
func newAESGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, &CipherError{Err: err}
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return nil, &CipherError{Err: err}
	}
	return aead, nil
}

// newAEADForAlgorithm constructs the AEAD for alg and key, returning ErrInvalidKeySize or ErrUnsupportedAlgorithm.
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

// SelectOptimalAlgorithm returns AES-256-GCM when hardware-backed, on AES-NI or in FIPS mode, else ChaCha20-Poly1305; pin BarrierConfig.Algorithm for persistent data, since the choice depends on the CPU.
func SelectOptimalAlgorithm(isHardwareBacked bool) SymmetricAlgorithm {
	if FIPSEnabled() {
		return AlgAES256GCM
	}
	if isHardwareBacked || HasAESNI() {
		return AlgAES256GCM
	}
	return softwareFallbackAlgorithm()
}
