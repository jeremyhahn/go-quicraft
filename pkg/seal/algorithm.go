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

// SymmetricAlgorithm identifies a supported authenticated encryption algorithm.
// Values are single-byte identifiers, and one is embedded in every barrier
// ciphertext header.
//
// That byte does NOT select the cipher, and must not be made to. It is
// UNAUTHENTICATED — it sits outside the AEAD tag and outside the AAD — so a
// decryptor that honoured it would be honouring an attacker-chosen algorithm
// identifier, i.e. a downgrade oracle: flip the byte, and a reader picks a
// different primitive for a key derived for another one. Barrier.Decrypt
// therefore selects the AEAD from the AUTHENTICATED epoch (the epoch's DEK
// determines the primitive) and uses the header byte only to cross-check the
// nonce width, rejecting any value whose nonce size disagrees with the active
// cipher — see Barrier.decryptLocked. The identifier's real jobs are forensic
// (identifying data on disk) and derivational (an HKDFV2 domain separator);
// runtime negotiation is not one of them, and a deployment is expected to use
// ONE algorithm for the life of its data (see SealedRootKey.Algorithm).
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
	//
	// NOT AVAILABLE IN A FIPS BUILD. ChaCha20-Poly1305 is not FIPS-approved, so
	// the implementation lives in algorithm_chacha.go behind //go:build !fips and
	// is not registered when the binary is built with -tags fips: Valid() reports
	// false and newAEADForAlgorithm returns ErrUnsupportedAlgorithm. The
	// identifier itself is retained in every build because it is a WIRE value —
	// it appears in the header of data already on disk, and a forensic reader
	// must still be able to name what it is looking at.
	AlgChaCha20Poly1305 SymmetricAlgorithm = 0x04

	// AlgXChaCha20Poly1305 selects XChaCha20-Poly1305: 32-byte key, 24-byte nonce.
	// The extended 192-bit nonce eliminates nonce-collision risk for large key volumes.
	//
	// NOT AVAILABLE IN A FIPS BUILD, for the same reason and by the same
	// mechanism as AlgChaCha20Poly1305.
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

// AEAD per-invocation plaintext ceilings. Exceeding one of these does not
// return an error from the underlying implementation — it PANICS inside
// crypto/cipher or x/crypto — so the barrier has to reject the message before
// it reaches Seal. See SymmetricAlgorithm.MaxPlaintextSize.
const (
	// maxPlaintextGCM is GCM's ceiling: the 32-bit block counter limits one
	// invocation to (2^32 - 2) 16-byte blocks, ~64 GiB. crypto/cipher panics
	// with "crypto/cipher: message too large for GCM" beyond it.
	maxPlaintextGCM uint64 = ((1 << 32) - 2) * 16
)

// algorithmParams holds the static parameters for a SymmetricAlgorithm.
//
// nonceSize is the nonce width the WIRE FORMAT carries, which is not always
// what the constructed cipher.AEAD reports. An AES-GCM AEAD built by newAESGCM
// generates its own nonce and prepends it to its own Seal output, so it reports
// NonceSize() == 0 and folds those 12 bytes into Overhead(); the frame still
// contains exactly nonceSize nonce bytes in exactly the same position. Keeping
// nonceSize as the wire width is what lets the barrier cross-check a header
// algID against the active cipher, and what keeps SymmetricAlgorithm.NonceSize
// meaningful to callers reading a frame they did not write.
type algorithmParams struct {
	keySize      int
	nonceSize    int
	maxPlaintext uint64
	name         string
	newAEAD      func(key []byte) (cipher.AEAD, error)
}

// algorithmRegistry maps algorithm identifiers to their parameters and
// constructor functions for O(1) dispatch in newAEADForAlgorithm.
//
// It holds ONLY FIPS-approved algorithms. The ChaCha20 family is added by the
// init in algorithm_chacha.go, which is compiled only when the fips build tag
// is absent, so a FIPS build has no way to reach a non-approved AEAD: the
// registry is the single lookup every constructor, key-size check and
// nonce-width check goes through. Registration, not a runtime branch, is what
// makes the FIPS build unable to execute the algorithm.
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

// NonceSize returns the nonce length in bytes that this algorithm's wire format
// carries. Returns 0 for unrecognised algorithm identifiers — including, in a
// FIPS build, the ChaCha20 family, which is not registered there.
//
// For AES-GCM this is the width of the nonce the FIPS module generates and
// prepends to the ciphertext; it is deliberately NOT the same as the
// cipher.AEAD.NonceSize() of the AEAD returned by newAEADForAlgorithm, which is
// zero because that AEAD accepts no caller nonce. See algorithmParams.
func (a SymmetricAlgorithm) NonceSize() int {
	if p, ok := algorithmRegistry[a]; ok {
		return p.nonceSize
	}
	return 0
}

// MaxPlaintextSize returns the largest plaintext, in bytes, that this algorithm
// may seal in a SINGLE invocation. Returns 0 for unrecognised identifiers.
//
// This is a hard limit of the construction, not a policy: GCM's block counter
// is 32 bits, and x/crypto's ChaCha20-Poly1305 reuses a 32-bit counter for both
// nonce widths. Both implementations answer an over-long message with a PANIC
// rather than an error — "crypto/cipher: message too large for GCM",
// "chacha20poly1305: plaintext too large" — so a caller that hands one an
// oversized frame crashes the embedding process instead of receiving a
// rejection. Barrier.Encrypt checks this bound before it allocates or seals so
// the outcome is a typed ErrPlaintextTooLarge; the decrypt path applies the
// same bound to the ciphertext payload, where x/crypto's Open panics too.
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

// newAESGCM constructs an AES-GCM AEAD. The key length determines the AES
// variant (128, 192, or 256 bit) because aes.NewCipher selects the variant
// based on key length, so the same constructor covers all three AES modes.
//
// It uses cipher.NewGCMWithRandomNonce, NOT cipher.NewGCM, and that choice is
// load-bearing rather than stylistic. FIPS 140-3 (SP 800-38D §8.2.2, CMVP IG
// C.H) requires a GCM IV to be generated INSIDE the module boundary; a nonce
// drawn by application code and stored beside the ciphertext is the exact
// construction the requirement prohibits, and Go answers it under
// GODEBUG=fips140=only with "use of GCM with arbitrary IVs is not allowed".
//
// The wire format does not change. NewGCMWithRandomNonce generates a 12-byte
// nonce per Seal and prepends it to its own output, which is byte-for-byte the
// layout this package already wrote by hand (nonce ‖ ciphertext ‖ tag), so
// every AES-GCM blob written before this migration is still readable and every
// blob written after it is readable by a pre-migration reader. See
// legacy_format_test.go, which decrypts checked-in pre-migration bytes.
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
//
// WARNING — do NOT use this to pick the DATA-PATH algorithm of any deployment
// whose data outlives the host that wrote it. The choice depends on the local
// CPU (AES-NI present or masked by the hypervisor), and the algorithm is both an
// HKDFV2 domain separator and the cipher Decrypt selects by epoch: restore the
// same sealed root key or data directory onto a host that chooses differently
// and every read fails with the generic ErrDecryptionFailed. Under HKDFV1 it is
// worse — the info string omits the algorithm, so the IDENTICAL DEK would be
// used as an AES-256-GCM key on one host and a ChaCha20-Poly1305 key on another.
// Pin BarrierConfig.Algorithm explicitly for anything persistent, and for a
// mixed-generation cluster pin the SAME value on every node.
// SealedRootKey.Algorithm records the choice so a later mismatch is refused at
// load (AlgorithmMismatchError) instead of surfacing as unreadable data; it
// cannot help a blob written before that field existed. This function remains
// appropriate for ephemeral, single-host uses (a transport or session cipher).
//
// Under FIPS 140 mode the data-path cipher MUST be FIPS-approved:
// AES-256-GCM is returned unconditionally, even on CPUs without AES-NI.
// ChaCha20-Poly1305 is not an approved algorithm, and a "FIPS"
// deployment silently selecting it on non-AES-NI hardware would violate
// the deployment's compliance claim.
//
// That is the RUNTIME guard, and it is retained. A binary built with -tags fips
// additionally has no ChaCha20 to select: softwareFallbackAlgorithm returns
// AES-256-GCM there, because the ChaCha20 implementation is not compiled in.
func SelectOptimalAlgorithm(isHardwareBacked bool) SymmetricAlgorithm {
	if FIPSEnabled() {
		return AlgAES256GCM
	}
	if isHardwareBacked || HasAESNI() {
		return AlgAES256GCM
	}
	return softwareFallbackAlgorithm()
}
