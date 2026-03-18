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
	"crypto/sha256"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/pbkdf2"
)

// KDFVersion identifies the key derivation function algorithm version.
// The version is stored in SealedRootKey metadata so that cross-version
// verification works correctly across FIPS and non-FIPS environments.
type KDFVersion uint8

const (
	// KDFArgon2id selects Argon2id for key derivation (standard mode).
	KDFArgon2id KDFVersion = 1

	// KDFPbkdf2SHA256 selects PBKDF2-SHA256 for key derivation (FIPS mode).
	KDFPbkdf2SHA256 KDFVersion = 2

	// minKDFSaltLen is the minimum salt length accepted by DeriveKey.
	minKDFSaltLen = 16
)

// KDFParams holds the parameters for both supported key derivation algorithms.
// Only the fields relevant to the selected KDFVersion are used during derivation.
type KDFParams struct {
	// KeyLen is the desired derived key length in bytes.
	KeyLen uint32

	// SaltLen is the desired random salt length in bytes (for generation only).
	SaltLen uint32

	// Argon2id-specific parameters.
	Time    uint32 // Number of iterations (Argon2id time parameter).
	Memory  uint32 // Memory usage in KiB (Argon2id memory parameter).
	Threads uint8  // Parallelism degree (Argon2id threads parameter).

	// PBKDF2-specific parameters.
	Iterations int // Number of PBKDF2 iterations.
}

// kdfDeriver is a function that derives a key from a passphrase and salt.
type kdfDeriver func(passphrase, salt []byte, params KDFParams) ([]byte, error)

// kdfDispatchers is a map-based dispatch table for KDF algorithms.
// Uses O(1) constant-time lookup instead of a switch statement.
var kdfDispatchers = map[KDFVersion]kdfDeriver{
	KDFArgon2id:     deriveArgon2id,
	KDFPbkdf2SHA256: derivePBKDF2SHA256,
}

// DefaultKDFParams returns the default Argon2id parameters for standard
// (non-FIPS) mode. These exceed OWASP 2024 minimums (time=3, memory=64MB)
// because the sealed root key protects all data at rest and is a high-value
// offline attack target.
func DefaultKDFParams() KDFParams {
	return KDFParams{
		Time:    4,
		Memory:  128 * 1024, // 128 MiB in KiB
		Threads: 4,
		KeyLen:  32,
		SaltLen: 32,
	}
}

// DefaultFIPSKDFParams returns the default PBKDF2-SHA256 parameters for FIPS
// mode. The 600,000 iteration count follows OWASP 2024 guidance for
// PBKDF2-HMAC-SHA256.
func DefaultFIPSKDFParams() KDFParams {
	return KDFParams{
		Iterations: 600_000,
		KeyLen:     32,
		SaltLen:    32,
	}
}

// DefaultKDFVersion returns KDFPbkdf2SHA256 when FIPS mode is enabled,
// or KDFArgon2id otherwise. This allows callers to transparently select
// the appropriate algorithm based on the runtime FIPS policy.
func DefaultKDFVersion() KDFVersion {
	if FIPSEnabled() {
		return KDFPbkdf2SHA256
	}
	return KDFArgon2id
}

// DeriveKey derives a cryptographic key from a passphrase and salt using
// the specified KDF version and parameters. The returned key length is
// determined by params.KeyLen.
func DeriveKey(passphrase, salt []byte, version KDFVersion, params KDFParams) ([]byte, error) {
	if len(passphrase) == 0 {
		return nil, ErrKDFInvalidPassphrase
	}
	if len(salt) < minKDFSaltLen {
		return nil, ErrKDFInvalidSalt
	}

	deriver, ok := kdfDispatchers[version]
	if !ok {
		return nil, ErrKDFUnsupportedVersion
	}

	return deriver(passphrase, salt, params)
}

// deriveArgon2id derives a key using the Argon2id algorithm.
func deriveArgon2id(passphrase, salt []byte, params KDFParams) ([]byte, error) {
	key := argon2.IDKey(passphrase, salt, params.Time, params.Memory, params.Threads, params.KeyLen)
	return key, nil
}

// derivePBKDF2SHA256 derives a key using PBKDF2 with HMAC-SHA256.
func derivePBKDF2SHA256(passphrase, salt []byte, params KDFParams) ([]byte, error) {
	key := pbkdf2.Key(passphrase, salt, params.Iterations, int(params.KeyLen), sha256.New)
	return key, nil
}
