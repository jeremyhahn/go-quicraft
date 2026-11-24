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

	// ErrNoRootKeyAvailable is returned by RootKey(ctx) when the barrier holds no
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

	// ErrStrategyClosed is returned when a SealingStrategy is used after Close; re-create it.
	ErrStrategyClosed = errors.New("quicraft/seal: sealing strategy is closed")

	// ErrNoneStrategyEncryptedData is returned when the no-encryption strategy is asked to decrypt data with a non-zero epoch prefix.
	ErrNoneStrategyEncryptedData = errors.New(
		"quicraft/seal: none strategy cannot decrypt data with a non-zero epoch (data was encrypted by a real strategy)")

	// ErrCiphertextTooShort is returned when a ciphertext is too short
	// to contain the required epoch, nonce, and authentication tag.
	ErrCiphertextTooShort = errors.New("quicraft/seal: ciphertext too short")

	// ErrPlaintextTooLarge is returned when one frame exceeds the algorithm's per-invocation ceiling (SymmetricAlgorithm.MaxPlaintextSize), where the cipher would otherwise panic.
	ErrPlaintextTooLarge = errors.New("quicraft/seal: plaintext exceeds the algorithm's maximum message size")

	// ErrBarrierSuperseded is returned by every write path once another loader claimed the sealed blob with a higher LoadGeneration; terminal for the instance, decryption unaffected. SetSealedRootKey errors wrap *SupersededError.
	ErrBarrierSuperseded = errors.New(
		"quicraft/seal: sealed root key was claimed by another loader; this barrier may no longer encrypt")

	// ErrUnsealBackoff is returned when an Unseal attempt is rejected
	// due to brute-force protection backoff.
	ErrUnsealBackoff = errors.New("quicraft/seal: unseal rejected, backoff active")

	// ErrPurgeCurrentEpoch is returned when PurgeEpochsBefore attempts
	// to purge the current active epoch.
	ErrPurgeCurrentEpoch = errors.New("quicraft/seal: cannot purge current epoch")

	// ErrEpochsInFlight is returned by Reset while any epoch holds an in-flight reference.
	ErrEpochsInFlight = errors.New("quicraft/seal: cannot reset barrier while epochs are pinned in flight")

	// ErrResetNotFirstBoot is returned by Reset unless the barrier is an uncompleted first boot of this process's Initialize.
	ErrResetNotFirstBoot = errors.New("quicraft/seal: reset only rolls back an uncompleted first-boot initialization")

	// ErrInvalidSealedEpoch is returned when SetSealedRootKey receives Epoch 0 or 0 among KnownEpochs.
	ErrInvalidSealedEpoch = errors.New("quicraft/seal: sealed root key has invalid epoch (must be > 0)")

	// ErrInvalidSealedSalt is returned when SetSealedRootKey receives a
	// sealed key whose Salt length does not match the expected hkdfSaltSize.
	ErrInvalidSealedSalt = errors.New("quicraft/seal: sealed root key has invalid salt length")

	// ErrInvalidSealedEpochs is returned when SetSealedRootKey receives a
	// sealed key whose KnownEpochs does not contain the current Epoch.
	ErrInvalidSealedEpochs = errors.New("quicraft/seal: sealed root key KnownEpochs missing current epoch")

	// ErrTooManyKnownEpochs is returned when SetSealedRootKey receives more than MaxKnownEpochs epochs.
	ErrTooManyKnownEpochs = errors.New("quicraft/seal: sealed root key names too many known epochs")

	// ErrKnownEpochAboveCurrent is returned when SetSealedRootKey receives a KnownEpochs entry above Epoch.
	ErrKnownEpochAboveCurrent = errors.New("quicraft/seal: sealed root key names an epoch above its current epoch")

	// ErrSealedStateUnauthenticated is returned by Unseal when the blob's plaintext metadata fails or lacks its StateMAC; wipe and reseal a blob without one.
	ErrSealedStateUnauthenticated = errors.New(
		"quicraft/seal: sealed root key metadata failed authentication (StateMAC mismatch or absent)")

	// ErrSealedEpochRollback is returned by SetSealedRootKey when the blob's Epoch is below BarrierConfig.MinEpoch.
	ErrSealedEpochRollback = errors.New("quicraft/seal: sealed root key epoch is below the configured rollback floor")

	// ErrInvocationBudgetUnknown is logged when a restart can neither establish the epoch's spent invocation budget nor rotate; Encrypt then fails with ErrInvocationLimitExceeded until rotation.
	ErrInvocationBudgetUnknown = errors.New(
		"quicraft/seal: AEAD invocation budget for the current epoch is unrecoverable and the epoch cannot rotate")

	// ErrFIPSUnavailable is returned by BarrierConfig.Validate when RequireFIPS is set but FIPSEnabled() is false.
	ErrFIPSUnavailable = errors.New("quicraft/seal: RequireFIPS is set but the FIPS 140-3 module is not active")

	// ErrKDFNotFIPSApproved is returned when Argon2id is selected while FIPS mode is active.
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

	// ErrKDFWeakParams is returned when DeriveKey receives work factors below the production floor.
	ErrKDFWeakParams = errors.New("quicraft/seal: KDF parameters below the production work-factor floor")

	// ErrNoStatePersister is returned by Rotate, and a purging PurgeEpochsBefore, when BarrierConfig.StatePersister is nil; the previous epoch stays active.
	ErrNoStatePersister = errors.New("quicraft/seal: no state persister configured; refusing to change unpersistable barrier state")

	// ErrNoBarrierEncryptor is returned when a strategy that must supply its own encryptor returns none.
	ErrNoBarrierEncryptor = errors.New("quicraft/seal: strategy provided no barrier encryptor")

	// ErrHardwareMode is returned when an operation (e.g., Rotate,
	// PurgeEpochsBefore) is not supported because the barrier is using a
	// hardware encryptor that manages its own key material.
	ErrHardwareMode = errors.New("quicraft/seal: operation not supported in hardware encryptor mode")

	// ErrContextAADUnsupported is returned by Initialize and Unseal when RequireContextAAD is set but the barrier cannot authenticate caller context.
	ErrContextAADUnsupported = errors.New(
		"quicraft/seal: barrier cannot authenticate caller context but RequireContextAAD is set " +
			"(delegated encryptor does not implement AADSymmetricEncrypter, or no-encryption mode is selected)")

	// ErrAADProbeRoundTrip reports a delegated AADSymmetricEncrypter that cannot round-trip a probe under the same aad.
	ErrAADProbeRoundTrip = errors.New("quicraft/seal: AAD probe did not round-trip under the same AAD")

	// ErrAADProbeNotBound reports a delegated AADSymmetricEncrypter that decrypted a probe under a different aad.
	ErrAADProbeNotBound = errors.New(
		"quicraft/seal: AAD is not authenticated (ciphertext opened under a different AAD than it was sealed with)")

	// ErrShamirThresholdInvalid is returned when the Shamir threshold is
	// less than 2 or greater than the total number of shares.
	ErrShamirThresholdInvalid = errors.New("quicraft/seal: shamir threshold must be >= 2 and <= total shares")

	// ErrEmptyCredentials is returned when a deferred-passphrase strategy
	// receives empty credentials at Seal/Unseal time.
	ErrEmptyCredentials = errors.New("quicraft/seal: empty credentials for deferred-passphrase strategy")

	// Shamir sealing-strategy errors, exported for the strategy and recovery flows in go-qrdb; ErrNilSealedData is also the Barrier's error for missing sealed state.

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

// StatePersistError is returned when BarrierConfig.StatePersister fails to write the sealed state; the triggering operation is aborted with the previous epoch intact.
type StatePersistError struct {
	// Op is the aborted operation: "rotate", "purge", or "claim" (the
	// load-generation claim Unseal persists).
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

// SupersededError reports that another loader claimed the sealed blob, naming both generations; it unwraps to ErrBarrierSuperseded.
type SupersededError struct {
	// Observed is the LoadGeneration carried by the blob just presented.
	Observed uint64
	// Claimed is the generation this barrier claimed when it last loaded or
	// initialized the blob.
	Claimed uint64
}

// Error returns a human-readable description of the lost claim.
func (e *SupersededError) Error() string {
	return fmt.Sprintf(
		"quicraft/seal: sealed root key load generation %d supersedes this barrier's claim of %d; "+
			"another loader holds this sealed state and this barrier may no longer encrypt",
		e.Observed, e.Claimed)
}

// Unwrap returns ErrBarrierSuperseded so errors.Is matches the sentinel.
func (e *SupersededError) Unwrap() error { return ErrBarrierSuperseded }

// AlgorithmMismatchError is returned by SetSealedRootKey when the blob's algorithm differs from the configured one (it is part of key derivation); pin BarrierConfig.Algorithm to the named one.
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

// AADBindingProbeError reports why the behavioural probe of a delegated encryptor's AAD binding failed; such an encryptor is ContextBindingUnsupported (see verifyAADBinding).
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
