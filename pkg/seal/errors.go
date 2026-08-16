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
	"errors"
	"fmt"
)

// Sentinel errors for barrier operations. These are the canonical definitions;
// the top-level quicraft package re-exports them for convenience.
var (
	// ErrBarrierSealed is returned when an encrypt/decrypt operation is
	// attempted while the barrier is sealed.
	ErrBarrierSealed = errors.New("quicraft/seal: barrier is sealed")

	// ErrNoRootKeyAvailable is returned by RootKey() when the barrier holds no
	// root key material — i.e. no-encryption mode (StrategyNone), where the
	// identity encryptor needs no key.
	ErrNoRootKeyAvailable = errors.New("quicraft/seal: no root key in no-encryption mode")

	// ErrBarrierNotInit is returned when the barrier has not been
	// initialized with a sealing strategy.
	ErrBarrierNotInit = errors.New("quicraft/seal: barrier not initialized")

	// ErrDecryptionFailed is returned when a decryption operation fails,
	// typically due to a corrupted ciphertext or incorrect key.
	ErrDecryptionFailed = errors.New("quicraft/seal: decryption failed")
)

// Sentinel errors specific to the crypto package.
var (
	// ErrBarrierAlreadyInit is returned when Initialize is called on a
	// barrier that has already been initialized.
	ErrBarrierAlreadyInit = errors.New("quicraft/seal: barrier already initialized")

	// ErrBarrierUnsealed is returned when Initialize or Unseal is called
	// on a barrier that is already unsealed.
	ErrBarrierUnsealed = errors.New("quicraft/seal: barrier already unsealed")

	// ErrInvalidSealedKey is returned when a SealedRootKey argument is
	// nil or otherwise structurally invalid.
	ErrInvalidSealedKey = errors.New("quicraft/seal: invalid sealed root key")

	// ErrStrategyMismatch is returned when a SealingStrategy is asked to
	// unseal a SealedRootKey that was produced by a different strategy.
	ErrStrategyMismatch = errors.New("quicraft/seal: sealed key strategy does not match unseal strategy")

	// ErrInvalidRootKeySize is returned when a root key does not have
	// the required 32-byte length.
	ErrInvalidRootKeySize = errors.New("quicraft/seal: invalid root key size")

	// ErrInvalidPassphrase is returned when an empty passphrase is
	// provided to the software sealing strategy.
	ErrInvalidPassphrase = errors.New("quicraft/seal: passphrase must not be empty")

	// ErrStrategyClosed is returned when a seal/unseal operation is
	// attempted on a SealingStrategy whose key material has already been
	// released by Close. Close is idempotent but not reversible; a closed
	// strategy must be re-created, not reused.
	ErrStrategyClosed = errors.New("quicraft/seal: sealing strategy is closed")

	// ErrNoneStrategyEncryptedData is returned when the no-encryption
	// strategy is asked to decrypt data carrying a non-zero epoch prefix,
	// which indicates the data was written by a real sealing strategy. A
	// barrier was misconfigured to None after running encrypted; decoding
	// would yield garbage (no AEAD tag exists to fail on), so this fails
	// closed.
	ErrNoneStrategyEncryptedData = errors.New(
		"quicraft/seal: none strategy cannot decrypt data with a non-zero epoch (data was encrypted by a real strategy)")

	// ErrCiphertextTooShort is returned when a ciphertext is too short
	// to contain the required epoch, nonce, and authentication tag.
	ErrCiphertextTooShort = errors.New("quicraft/seal: ciphertext too short")

	// ErrPlaintextTooLarge is returned by the Encrypt family when a single
	// frame exceeds the configured algorithm's per-invocation ceiling
	// (SymmetricAlgorithm.MaxPlaintextSize): ~64 GiB for AES-GCM, ~256 GiB for
	// the ChaCha20-Poly1305 family.
	//
	// It exists because the underlying implementations do not return an error
	// there — crypto/cipher and x/crypto both PANIC — so without this check an
	// over-long frame would crash the embedding process from inside the cipher.
	// A library must not kill its host over an input it can reject.
	ErrPlaintextTooLarge = errors.New("quicraft/seal: plaintext exceeds the algorithm's maximum message size")

	// ErrUnsealBackoff is returned when an Unseal attempt is rejected
	// due to brute-force protection backoff.
	ErrUnsealBackoff = errors.New("quicraft/seal: unseal rejected, backoff active")

	// ErrPurgeCurrentEpoch is returned when PurgeEpochsBefore attempts
	// to purge the current active epoch.
	ErrPurgeCurrentEpoch = errors.New("quicraft/seal: cannot purge current epoch")

	// ErrInvalidSealedEpoch is returned when SetSealedRootKey receives a
	// sealed key with Epoch == 0, or with 0 among its KnownEpochs. Epochs start
	// at 1 (Initialize) and only ever increment, so nothing was sealed under
	// epoch 0 and a DEK for it can never be legitimate.
	ErrInvalidSealedEpoch = errors.New("quicraft/seal: sealed root key has invalid epoch (must be > 0)")

	// ErrInvalidSealedSalt is returned when SetSealedRootKey receives a
	// sealed key whose Salt length does not match the expected hkdfSaltSize.
	ErrInvalidSealedSalt = errors.New("quicraft/seal: sealed root key has invalid salt length")

	// ErrInvalidSealedEpochs is returned when SetSealedRootKey receives a
	// sealed key whose KnownEpochs does not contain the current Epoch.
	ErrInvalidSealedEpochs = errors.New("quicraft/seal: sealed root key KnownEpochs missing current epoch")

	// ErrTooManyKnownEpochs is returned when SetSealedRootKey receives a sealed
	// key naming more than MaxKnownEpochs epochs. KnownEpochs is plaintext,
	// unauthenticated metadata and Unseal derives one HKDF DEK, one AEAD and
	// one mlock'd enclave per entry, so an inflated set is a durable
	// denial-of-service against every subsequent start — no passphrase needed.
	ErrTooManyKnownEpochs = errors.New("quicraft/seal: sealed root key names too many known epochs")

	// ErrKnownEpochAboveCurrent is returned when SetSealedRootKey receives a
	// sealed key whose KnownEpochs contains an epoch greater than its Epoch.
	// Epochs only ever advance, and the blob records the highest one reached,
	// so a larger entry can never be legitimate — it is either corruption or a
	// forged blob padding the derivation set.
	ErrKnownEpochAboveCurrent = errors.New("quicraft/seal: sealed root key names an epoch above its current epoch")

	// ErrSealedStateUnauthenticated is returned by Unseal when the persisted
	// SealedRootKey's metadata does not authenticate under the root key: either
	// the StateMAC does not match the metadata, or the blob carries no StateMAC
	// at all and BarrierConfig.AllowUnauthenticatedSealedState is not set.
	//
	// Only the root-key CIPHERTEXT is covered by the wrapping AEAD; Epoch,
	// KnownEpochs, Salt, HKDFVersion, Algorithm and Strategy sit beside it in
	// plaintext. The MAC is what binds them to the key they describe, so a
	// mismatch means the state was edited by something that does not hold the
	// root key.
	ErrSealedStateUnauthenticated = errors.New(
		"quicraft/seal: sealed root key metadata failed authentication (StateMAC mismatch or absent)")

	// ErrSealedEpochRollback is returned by SetSealedRootKey when the blob's
	// Epoch is below BarrierConfig.MinEpoch. Authentication cannot detect the
	// REPLAY of an older, genuinely authenticated blob; only a floor the
	// embedder derives from data the attacker would have to roll back in the
	// same step can. Resurrecting a retired epoch re-runs a spent DEK under a
	// fresh invocation counter and an empty nonce tracker.
	ErrSealedEpochRollback = errors.New("quicraft/seal: sealed root key epoch is below the configured rollback floor")

	// ErrInvocationBudgetUnknown is logged (not returned) when a restart cannot
	// establish how much of the current epoch's NIST SP 800-38D invocation
	// budget has already been spent AND cannot rotate to a fresh epoch. It is
	// defined so tests and embedders can name the condition; the barrier's
	// observable behaviour is that Encrypt fails with
	// ErrInvocationLimitExceeded until the epoch is rotated.
	ErrInvocationBudgetUnknown = errors.New(
		"quicraft/seal: AEAD invocation budget for the current epoch is unrecoverable and the epoch cannot rotate")

	// ErrFIPSUnavailable is returned by BarrierConfig.Validate when
	// RequireFIPS is set but FIPSEnabled() is false — the binary was not built
	// with the FIPS 140-3 module and GOFIPS140 is unset. Without this the two
	// states "FIPS not requested" and "FIPS requested but unavailable" are
	// indistinguishable and the second fails open.
	ErrFIPSUnavailable = errors.New("quicraft/seal: RequireFIPS is set but the FIPS 140-3 module is not active")

	// ErrKDFNotFIPSApproved is returned by DeriveKey (and by the software
	// strategy constructors) when Argon2id is selected while FIPS mode is
	// active. Argon2id is not a FIPS-approved KDF, and wrapping the root key
	// with it in a "FIPS" deployment is exactly the silent non-compliance the
	// mode exists to prevent.
	ErrKDFNotFIPSApproved = errors.New("quicraft/seal: KDF is not FIPS-approved (Argon2id) while FIPS mode is active")

	// ErrKDFUnsupportedVersion is returned when DeriveKey receives an
	// unknown KDFVersion value.
	ErrKDFUnsupportedVersion = errors.New("quicraft/seal: unsupported KDF version")

	// ErrUnsupportedHKDFVersion is returned when a SealedRootKey names a
	// DEK-derivation info layout this build does not implement.
	ErrUnsupportedHKDFVersion = errors.New("quicraft/seal: unsupported HKDF info version")

	// ErrKDFInvalidPassphrase is returned when DeriveKey receives an
	// empty passphrase.
	ErrKDFInvalidPassphrase = errors.New("quicraft/seal: KDF passphrase must not be empty")

	// ErrKDFInvalidSalt is returned when DeriveKey receives a nil or
	// too-short salt (minimum 16 bytes).
	ErrKDFInvalidSalt = errors.New("quicraft/seal: KDF salt too short (minimum 16 bytes)")

	// ErrKDFWeakParams is returned when DeriveKey receives work-factor
	// parameters below the production floor (e.g. Argon2 Time=1,
	// Memory=1KiB, or PBKDF2 Iterations=1). The sealed root key is an
	// offline attack target, so a near-free wrapping key is rejected.
	ErrKDFWeakParams = errors.New("quicraft/seal: KDF parameters below the production work-factor floor")

	// ErrNoStatePersister is returned by Rotate (and by a PurgeEpochsBefore
	// that would actually purge) when BarrierConfig.StatePersister is nil.
	// Both operations change state that must survive a restart, and
	// activating a new epoch that was never persisted makes every byte
	// written under it undecryptable after the restart. Failing closed keeps
	// the previous epoch active and fully usable.
	ErrNoStatePersister = errors.New("quicraft/seal: no state persister configured; refusing to change unpersistable barrier state")

	// ErrNoBarrierEncryptor is returned when a strategy that must supply its
	// own encryptor (no-encryption mode, hardware mode) returns none. The
	// barrier would otherwise report itself unsealed with no cipher and
	// nil-dereference on the first Encrypt.
	ErrNoBarrierEncryptor = errors.New("quicraft/seal: strategy provided no barrier encryptor")

	// ErrHardwareMode is returned when an operation (e.g., Rotate,
	// PurgeEpochsBefore) is not supported because the barrier is using a
	// hardware encryptor that manages its own key material.
	ErrHardwareMode = errors.New("quicraft/seal: operation not supported in hardware encryptor mode")

	// ErrContextAADUnsupported is returned by Initialize and Unseal when
	// BarrierConfig.RequireContextAAD is set but the constructed barrier cannot
	// authenticate a caller-supplied context: either the strategy delegates to
	// an encryptor that does not implement AADSymmetricEncrypter, or the
	// strategy is StrategyNone, which performs no encryption at all and
	// therefore has no AEAD that could bind a position. The barrier fails closed
	// at setup rather than on the first context-bearing write.
	ErrContextAADUnsupported = errors.New(
		"quicraft/seal: barrier cannot authenticate caller context but RequireContextAAD is set " +
			"(delegated encryptor does not implement AADSymmetricEncrypter, or no-encryption mode is selected)")

	// ErrAADProbeRoundTrip reports that a delegated encryptor claiming
	// AADSymmetricEncrypter could not even round-trip a probe under the SAME
	// aad — it is broken, not merely unbinding.
	ErrAADProbeRoundTrip = errors.New("quicraft/seal: AAD probe did not round-trip under the same AAD")

	// ErrAADProbeNotBound reports that a delegated encryptor claiming
	// AADSymmetricEncrypter DECRYPTED a probe under a different aad than it was
	// sealed with: the aad argument is accepted and dropped, so the positional
	// binding the interface promises does not exist.
	ErrAADProbeNotBound = errors.New(
		"quicraft/seal: AAD is not authenticated (ciphertext opened under a different AAD than it was sealed with)")

	// ErrShamirThresholdInvalid is returned when the Shamir threshold is
	// less than 2 or greater than the total number of shares.
	ErrShamirThresholdInvalid = errors.New("quicraft/seal: shamir threshold must be >= 2 and <= total shares")

	// ErrEmptyCredentials is returned when a deferred-passphrase strategy
	// receives empty credentials at Seal/Unseal time.
	ErrEmptyCredentials = errors.New("quicraft/seal: empty credentials for deferred-passphrase strategy")

	// Shamir sealing-strategy errors (used by ShamirStrategy and shared with
	// higher-layer Shamir flows such as recovery/rekey/quorum).

	// ErrNilSealedData is returned when sealed root-key data is nil.
	ErrNilSealedData = errors.New("quicraft/seal: sealed data is nil")

	// ErrShamirNilStorage is returned when a nil storage backend is provided.
	ErrShamirNilStorage = errors.New("quicraft/seal: shamir storage backend must not be nil")

	// ErrShamirSplitFailed is returned when splitting the root key into shares fails.
	ErrShamirSplitFailed = errors.New("quicraft/seal: failed to split root key into shares")

	// ErrShamirCombineFailed is returned when reconstructing the root key from shares fails.
	ErrShamirCombineFailed = errors.New("quicraft/seal: failed to combine shares")

	// ErrShamirSerializationFailed is returned when (de)serializing a share fails.
	ErrShamirSerializationFailed = errors.New("quicraft/seal: shamir share (de)serialization failed")

	// ErrShamirStorageFailed is returned when a share storage operation fails.
	ErrShamirStorageFailed = errors.New("quicraft/seal: shamir storage operation failed")

	// ErrShamirShareNotFound is returned when a requested share is absent.
	ErrShamirShareNotFound = errors.New("quicraft/seal: shamir share not found")

	// ErrShamirNoSharesFound is returned when no shares are present in storage.
	ErrShamirNoSharesFound = errors.New("quicraft/seal: no shamir shares found")

	// ErrShamirVerificationFailed is returned when persisted shares fail consistency checks.
	ErrShamirVerificationFailed = errors.New("quicraft/seal: shamir share verification failed")

	// ErrShamirQuorumIncomplete is returned when fewer than threshold shares are available.
	ErrShamirQuorumIncomplete = errors.New("quicraft/seal: quorum not yet complete, need more shares")
)

// TrackerError is returned when a tracker operation (reset, clear, close)
// fails. This allows persistent or hardware-backed tracker implementations
// to propagate failures through the barrier.
type TrackerError struct {
	// Tracker identifies the tracker type: "invocation" or "nonce".
	Tracker string
	// Op is the operation that failed (e.g., "reset", "clear", "close", "seed").
	Op string
	// Err is the underlying error.
	Err error
}

// Error returns a human-readable description of the tracker failure.
func (e *TrackerError) Error() string {
	return fmt.Sprintf("quicraft/seal: tracker %s %s failed: %v", e.Tracker, e.Op, e.Err)
}

// Unwrap returns the underlying error.
func (e *TrackerError) Unwrap() error {
	return e.Err
}

// StatePersistError is returned when BarrierConfig.StatePersister fails to
// durably write the sealed root-key state. The operation that triggered the
// write (rotation, purge) is aborted with the previous epoch fully intact.
type StatePersistError struct {
	// Op is the aborted operation: "rotate" or "purge".
	Op string
	// Epoch is the epoch the aborted operation tried to persist.
	Epoch uint64
	// Err is the underlying persistence error.
	Err error
}

// Error returns a human-readable description of the persistence failure.
func (e *StatePersistError) Error() string {
	return fmt.Sprintf("quicraft/seal: %s aborted, sealed state for epoch %d not persisted: %v", e.Op, e.Epoch, e.Err)
}

// Unwrap returns the underlying error.
func (e *StatePersistError) Unwrap() error {
	return e.Err
}

// AlgorithmMismatchError is returned by SetSealedRootKey when the sealed blob
// records a data-path algorithm different from the one this barrier is
// configured with.
//
// It exists because the algorithm is not a preference but part of the key
// derivation: it is an HKDFV2 domain separator AND the cipher Decrypt selects
// by epoch (never from the ciphertext header's algID). Loading a blob under the
// wrong algorithm therefore produces a different DEK, or the same DEK used as a
// key for a different primitive, and every read fails with the deliberately
// generic ErrDecryptionFailed — indistinguishable from tampering.
//
// The message names BOTH algorithms so the operator can pin the right one
// explicitly via BarrierConfig.Algorithm rather than guessing (the usual cause
// is a restore onto a host whose CPU makes SelectOptimalAlgorithm choose
// differently; see its documentation).
type AlgorithmMismatchError struct {
	// Sealed is the algorithm recorded in the persisted sealed root key.
	Sealed SymmetricAlgorithm
	// Configured is the algorithm this barrier was constructed with.
	Configured SymmetricAlgorithm
}

// Error returns a human-readable description of the algorithm mismatch.
func (e *AlgorithmMismatchError) Error() string {
	return fmt.Sprintf(
		"quicraft/seal: sealed root key was written with %s but this barrier is configured for %s; "+
			"set BarrierConfig.Algorithm to %s to read this data",
		e.Sealed.String(), e.Configured.String(), e.Sealed.String())
}

// AADBindingProbeError reports why the one-shot behavioural probe of a
// delegated encryptor's AAD binding did not hold. The barrier classifies such
// an encryptor as ContextBindingUnsupported rather than trusting its declared
// AADSymmetricEncrypter method set; see verifyAADBinding.
type AADBindingProbeError struct {
	// Stage is the step that failed: "probe-entropy", "encrypt",
	// "decrypt-matching-aad", or "decrypt-wrong-aad".
	Stage string
	// Err is the underlying cause.
	Err error
}

// Error returns a human-readable description of the probe failure.
func (e *AADBindingProbeError) Error() string {
	return fmt.Sprintf("quicraft/seal: delegated AAD binding probe failed at %s: %v", e.Stage, e.Err)
}

// Unwrap returns the underlying error.
func (e *AADBindingProbeError) Unwrap() error {
	return e.Err
}

// KeyDerivationError is returned when HKDF key derivation fails.
type KeyDerivationError struct {
	Epoch uint64
	Err   error
}

// Error returns a human-readable description of the key derivation failure.
func (e *KeyDerivationError) Error() string {
	return fmt.Sprintf("quicraft/seal: key derivation failed for epoch %d: %v", e.Epoch, e.Err)
}

// Unwrap returns the underlying error.
func (e *KeyDerivationError) Unwrap() error {
	return e.Err
}

// Error is returned when the sealing strategy fails to seal a root key.
type Error struct {
	Strategy string
	Err      error
}

// Error returns a human-readable description of the seal failure.
func (e *Error) Error() string {
	return fmt.Sprintf("quicraft/seal: seal failed (strategy=%s): %v", e.Strategy, e.Err)
}

// Unwrap returns the underlying error.
func (e *Error) Unwrap() error {
	return e.Err
}

// UnsealError is returned when the sealing strategy fails to unseal a root key.
type UnsealError struct {
	Strategy string
	Err      error
}

// Error returns a human-readable description of the unseal failure.
func (e *UnsealError) Error() string {
	return fmt.Sprintf("quicraft/seal: unseal failed (strategy=%s): %v", e.Strategy, e.Err)
}

// Unwrap returns the underlying error.
func (e *UnsealError) Unwrap() error {
	return e.Err
}

// CipherError is returned when AES-GCM cipher creation fails.
type CipherError struct {
	Err error
}

// Error returns a human-readable description of the cipher creation failure.
func (e *CipherError) Error() string {
	return fmt.Sprintf("quicraft/seal: cipher creation failed: %v", e.Err)
}

// Unwrap returns the underlying error.
func (e *CipherError) Unwrap() error {
	return e.Err
}
