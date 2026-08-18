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
	// KDFArgon2id selects Argon2id for key derivation (standard mode).
	//
	// NOT AVAILABLE IN A FIPS BUILD. Argon2id is not a FIPS-approved KDF, so
	// its implementation lives in kdf_argon2.go behind //go:build !fips and is
	// not registered in kdfDispatchers when the binary is built with -tags fips;
	// DeriveKey answers it with ErrKDFUnsupportedVersion there. The identifier
	// is retained in every build because it is PERSISTED — it is stamped into
	// SealedRootKey.Metadata, and a FIPS build must still be able to read that
	// byte and say what it means rather than mis-attribute it to another KDF.
	KDFArgon2id KDFVersion = 1

	// KDFPbkdf2SHA256 selects PBKDF2-SHA256 for key derivation (FIPS mode).
	KDFPbkdf2SHA256 KDFVersion = 2

	// minKDFSaltLen is the minimum salt length accepted by DeriveKey.
	minKDFSaltLen = 16

	// Production floors for KDF work factors. The sealed root key protects
	// all data at rest and is a high-value OFFLINE attack target, so a
	// caller-supplied near-trivial cost (Time=1, Memory=1KiB, Iterations=1)
	// must be rejected rather than silently producing a near-free wrapping
	// key. These floors are conservative lower bounds (well below the
	// defaults); the test-only escape hatch AllowWeakKDFParams bypasses
	// them for fast unit tests.
	//
	// The Argon2id floors live with the Argon2id deriver in kdf_argon2.go, so a
	// build that cannot derive with Argon2id also carries no dead floor for it.
	minKDFKeyLen  = 16
	minPBKDF2Iter = 100_000
)

// allowWeakKDFParams, when true, bypasses the production KDF work-factor
// floors enforced by DeriveKey. It exists ONLY so unit tests can derive
// keys with trivially cheap parameters; production code must never set it.
// It is an atomic.Bool because DeriveKey reads it from every concurrent
// seal/unseal while a test may flip it. Guarded by AllowWeakKDFParams.
var allowWeakKDFParams atomic.Bool

// AllowWeakKDFParams enables the test-only bypass of the production KDF
// work-factor floors and returns a function that restores the previous
// setting.
//
// It PANICS outside a test binary. The bypass reduces the sealed-root-key
// wrapping KDF to a near-free cost, so an importer (or a dependency's init)
// must not be able to weaken a production deployment by calling it. The
// function stays exported only because the floors must also be bypassable
// from the test binaries of other packages that drive a real barrier.
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

// kdfFloorChecks holds, per KDF version, the predicate reporting whether the
// work factors fall below the production floor. It is keyed the same way as
// kdfDispatchers and registered from the same place, so a KDF that a build
// cannot derive with also has no floor to consult — the two tables cannot drift
// into a state where a version is derivable but unbounded.
var kdfFloorChecks = map[KDFVersion]func(KDFParams) bool{
	KDFPbkdf2SHA256: func(params KDFParams) bool { return params.Iterations < minPBKDF2Iter },
}

// kdfParamsBelowFloor reports whether the parameters fall below the
// production work-factor floor for the version, independent of the
// test-only bypass. Exposed (unexported) so the floor can be unit-tested
// without flipping the package-wide bypass.
//
// An unregistered version reports false: it has no floor because it has no
// deriver, and DeriveKey has already refused it with ErrKDFUnsupportedVersion
// before the floor is consulted.
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

// kdfDispatchers is a map-based dispatch table for KDF algorithms.
// Uses O(1) constant-time lookup instead of a switch statement.
//
// It holds ONLY FIPS-approved KDFs. Argon2id is added by the init in
// kdf_argon2.go, which is compiled only when the fips build tag is absent, so a
// FIPS build has no Argon2id deriver to reach: DeriveKey and every strategy
// constructor go through this table, and an absent entry is a refusal that no
// configuration can undo.
var kdfDispatchers = map[KDFVersion]kdfDeriver{
	KDFPbkdf2SHA256: derivePBKDF2SHA256,
}

// DefaultKDFParams returns the default Argon2id parameters for standard
// (non-FIPS) mode. These exceed OWASP 2024 minimums (time=3, memory=64MB)
// because the sealed root key protects all data at rest and is a high-value
// offline attack target.
//
// The parameters are plain numbers and are described in every build, so a
// reader of a stored blob can still say what work factors it was sealed under.
// A build with -tags fips has no Argon2id DERIVER (kdf_argon2.go), so passing
// these to DeriveKey there returns ErrKDFUnsupportedVersion; use
// DefaultFIPSKDFParams for anything that must actually derive.
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

// checkKDFFIPS reports whether the KDF version may be used under the current
// FIPS policy. It is the single place the rule lives, so DeriveKey and the
// strategy constructors cannot drift apart on which KDFs a FIPS deployment may
// derive with.
func checkKDFFIPS(version KDFVersion) error {
	if version == KDFArgon2id && FIPSEnabled() {
		return ErrKDFNotFIPSApproved
	}
	return nil
}

// DefaultKDFVersion returns KDFPbkdf2SHA256 when FIPS mode is enabled, or the
// build's standard-mode KDF otherwise. This allows callers to transparently
// select the appropriate algorithm based on the runtime FIPS policy.
//
// In a build with -tags fips there is no standard mode to fall back to:
// defaultStandardKDFVersion is KDFPbkdf2SHA256 there because Argon2id is not
// compiled in, so this returns the approved KDF whether or not the GOFIPS140
// environment says anything.
func DefaultKDFVersion() KDFVersion {
	if FIPSEnabled() {
		return KDFPbkdf2SHA256
	}
	return defaultStandardKDFVersion()
}

// DeriveKey derives a cryptographic key from a passphrase and salt using
// the specified KDF version and parameters. The returned key length is
// determined by params.KeyLen.
//
// FIPS: when FIPSEnabled() reports the module active, KDFArgon2id is REFUSED
// with ErrKDFNotFIPSApproved. Argon2id is not a FIPS-approved KDF, and the
// wrapping key it produces protects the root key that protects everything else,
// so deriving one in a deployment that declares FIPS is the silent
// non-compliance the mode exists to prevent — it is refused rather than logged.
//
// CONSEQUENCE for existing data: a blob wrapped under Argon2id cannot be
// unsealed while FIPS mode is active, because unsealing re-derives the same
// wrapping key. That is not a regression introduced here so much as the first
// point at which the deployment is told; migrate by unsealing once with FIPS
// off and re-sealing with NewSoftwareStrategyFIPS (PBKDF2-SHA256), which stamps
// KDFPbkdf2SHA256 into SealedRootKey.Metadata and unseals identically in either
// mode.
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
