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

	// ErrInvalidRootKeySize is returned when a root key does not have
	// the required 32-byte length.
	ErrInvalidRootKeySize = errors.New("quicraft/seal: invalid root key size")

	// ErrInvalidPassphrase is returned when an empty passphrase is
	// provided to the software sealing strategy.
	ErrInvalidPassphrase = errors.New("quicraft/seal: passphrase must not be empty")

	// ErrCiphertextTooShort is returned when a ciphertext is too short
	// to contain the required epoch, nonce, and authentication tag.
	ErrCiphertextTooShort = errors.New("quicraft/seal: ciphertext too short")

	// ErrUnsealBackoff is returned when an Unseal attempt is rejected
	// due to brute-force protection backoff.
	ErrUnsealBackoff = errors.New("quicraft/seal: unseal rejected, backoff active")

	// ErrPurgeCurrentEpoch is returned when PurgeEpochsBefore attempts
	// to purge the current active epoch.
	ErrPurgeCurrentEpoch = errors.New("quicraft/seal: cannot purge current epoch")

	// ErrInvalidSealedEpoch is returned when SetSealedRootKey receives a
	// sealed key with Epoch == 0. A valid sealed key must have epoch >= 1.
	ErrInvalidSealedEpoch = errors.New("quicraft/seal: sealed root key has invalid epoch (must be > 0)")

	// ErrInvalidSealedSalt is returned when SetSealedRootKey receives a
	// sealed key whose Salt length does not match the expected hkdfSaltSize.
	ErrInvalidSealedSalt = errors.New("quicraft/seal: sealed root key has invalid salt length")

	// ErrInvalidSealedEpochs is returned when SetSealedRootKey receives a
	// sealed key whose KnownEpochs does not contain the current Epoch.
	ErrInvalidSealedEpochs = errors.New("quicraft/seal: sealed root key KnownEpochs missing current epoch")

	// ErrKDFUnsupportedVersion is returned when DeriveKey receives an
	// unknown KDFVersion value.
	ErrKDFUnsupportedVersion = errors.New("quicraft/seal: unsupported KDF version")

	// ErrKDFInvalidPassphrase is returned when DeriveKey receives an
	// empty passphrase.
	ErrKDFInvalidPassphrase = errors.New("quicraft/seal: KDF passphrase must not be empty")

	// ErrKDFInvalidSalt is returned when DeriveKey receives a nil or
	// too-short salt (minimum 16 bytes).
	ErrKDFInvalidSalt = errors.New("quicraft/seal: KDF salt too short (minimum 16 bytes)")

	// ErrHardwareMode is returned when an operation (e.g., Rotate,
	// PurgeEpochsBefore) is not supported because the barrier is using a
	// hardware encryptor that manages its own key material.
	ErrHardwareMode = errors.New("quicraft/seal: operation not supported in hardware encryptor mode")

	// ErrShamirNotConfigured is returned when a Shamir operation is
	// attempted before SetShamirSupport has been called.
	ErrShamirNotConfigured = errors.New("quicraft/seal: shamir not configured")

	// ErrShamirThresholdInvalid is returned when the Shamir threshold is
	// less than 2 or greater than the total number of shares.
	ErrShamirThresholdInvalid = errors.New("quicraft/seal: shamir threshold must be >= 2 and <= total shares")

	// ErrEmptyCredentials is returned when a deferred-passphrase strategy
	// receives empty credentials at Seal/Unseal time.
	ErrEmptyCredentials = errors.New("quicraft/seal: empty credentials for deferred-passphrase strategy")
)

// Sentinel errors for ShareStore operations.
var (
	// ErrShareStoreInvalidDir is returned when an empty data directory
	// path is provided to NewFileShareStore.
	ErrShareStoreInvalidDir = errors.New("quicraft/seal: share store data directory must not be empty")

	// ErrShareStoreNotFound is returned when a requested share, metadata
	// entry, or version file does not exist.
	ErrShareStoreNotFound = errors.New("quicraft/seal: share store entry not found")

	// ErrShareStoreNilMetadata is returned when nil metadata is passed
	// to SaveMetadata.
	ErrShareStoreNilMetadata = errors.New("quicraft/seal: share metadata must not be nil")

	// ErrShareStoreInvalidIndex is returned when a share index less than
	// 1 is used. Shamir share indices are 1-based.
	ErrShareStoreInvalidIndex = errors.New("quicraft/seal: share index must be >= 1")

	// ErrShareStoreNilShare is returned when nil sealed share data is
	// passed to SaveSealedShare.
	ErrShareStoreNilShare = errors.New("quicraft/seal: sealed share data must not be nil")
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

// ShareStoreError is returned when a ShareStore operation fails.
type ShareStoreError struct {
	// Op is the operation that failed (e.g., "save_metadata", "get_sealed_share").
	Op string
	// Err is the underlying error.
	Err error
}

// Error returns a human-readable description of the share store failure.
func (e *ShareStoreError) Error() string {
	return fmt.Sprintf("quicraft/seal: share store %s failed: %v", e.Op, e.Err)
}

// Unwrap returns the underlying error.
func (e *ShareStoreError) Unwrap() error {
	return e.Err
}
