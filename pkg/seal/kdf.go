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
	"sync/atomic"
	"testing"

	"golang.org/x/crypto/pbkdf2"
)

// KDFVersion identifies the key derivation function algorithm version.
// The version is stored in SealedRootKey metadata so that cross-version
// verification works correctly across FIPS and non-FIPS environments.
type KDFVersion uint8

const (
	// KDFArgon2id selects Argon2id key derivation; not available in a FIPS build.
	KDFArgon2id KDFVersion = 1

	// KDFPbkdf2SHA256 selects PBKDF2-SHA256 for key derivation (FIPS mode).
	KDFPbkdf2SHA256 KDFVersion = 2

	// minKDFSaltLen is the minimum salt length accepted by DeriveKey.
	minKDFSaltLen = 16

	// Production floors for KDF work factors, which DeriveKey enforces.
	minKDFKeyLen  = 16
	minPBKDF2Iter = 100_000
)

// allowWeakKDFParams bypasses the KDF floors; set only by tests.
var allowWeakKDFParams atomic.Bool

// AllowWeakKDFParams enables the test-only KDF floor bypass and returns a function restoring the previous setting.
func AllowWeakKDFParams() func() {
	if !testing.Testing() {
		panic("seal.AllowWeakKDFParams: test-only KDF bypass called outside a test binary")
	}
	prev := allowWeakKDFParams.Swap(true)
	return func() { allowWeakKDFParams.Store(prev) }
}

// validateKDFParams enforces the production work-factor floors for the
// selected KDF version. Returns ErrKDFWeakParams when a parameter is
// below its floor, unless the test-only bypass is active.
func validateKDFParams(version KDFVersion, params KDFParams) error {
	if allowWeakKDFParams.Load() {
		return nil
	}
	if kdfParamsBelowFloor(version, params) {
		return ErrKDFWeakParams
	}
	return nil
}

// kdfFloorChecks maps each KDF version to its below-floor predicate, registered with kdfDispatchers.
var kdfFloorChecks = map[KDFVersion]func(KDFParams) bool{
	KDFPbkdf2SHA256: func(params KDFParams) bool { return params.Iterations < minPBKDF2Iter },
}

// kdfParamsBelowFloor reports whether params fall below the version's production floor, ignoring the test bypass.
func kdfParamsBelowFloor(version KDFVersion, params KDFParams) bool {
	if params.KeyLen < minKDFKeyLen {
		return true
	}
	if below, ok := kdfFloorChecks[version]; ok {
		return below(params)
	}
	return false
}

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

// kdfDispatchers maps each registered KDF version to its derivation function.
var kdfDispatchers = map[KDFVersion]kdfDeriver{
	KDFPbkdf2SHA256: derivePBKDF2SHA256,
}

// DefaultKDFParams returns the default Argon2id parameters for standard mode.
func DefaultKDFParams() KDFParams {
	return KDFParams{
		Time:    4,
		Memory:  128 * 1024, // 128 MiB in KiB
		Threads: 4,
		KeyLen:  32,
		SaltLen: 32,
	}
}

// DefaultFIPSKDFParams returns the default PBKDF2-SHA256 parameters for FIPS mode: 600,000 iterations.
func DefaultFIPSKDFParams() KDFParams {
	return KDFParams{
		Iterations: 600_000,
		KeyLen:     32,
		SaltLen:    32,
	}
}

// checkKDFFIPS reports whether the KDF version is allowed under the current FIPS policy.
func checkKDFFIPS(version KDFVersion) error {
	if version == KDFArgon2id && FIPSEnabled() {
		return ErrKDFNotFIPSApproved
	}
	return nil
}

// DefaultKDFVersion returns KDFPbkdf2SHA256 in FIPS mode, else the build's standard KDF.
func DefaultKDFVersion() KDFVersion {
	if FIPSEnabled() {
		return KDFPbkdf2SHA256
	}
	return defaultStandardKDFVersion()
}

// DeriveKey derives a params.KeyLen-byte key from passphrase and salt; in FIPS mode KDFArgon2id is refused with ErrKDFNotFIPSApproved.
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

	if err := checkKDFFIPS(version); err != nil {
		return nil, err
	}

	if err := validateKDFParams(version, params); err != nil {
		return nil, err
	}

	return deriver(passphrase, salt, params)
}

// derivePBKDF2SHA256 derives a key using PBKDF2 with HMAC-SHA256.
func derivePBKDF2SHA256(passphrase, salt []byte, params KDFParams) ([]byte, error) {
	key := pbkdf2.Key(passphrase, salt, params.Iterations, int(params.KeyLen), sha256.New)
	return key, nil
}
