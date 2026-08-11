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
	"context"
	"crypto/cipher"
)

// StorageBackend is a minimal context-aware interface for key-value storage
// using string keys. go-qrdb's storage.Storage satisfies this via Go duck
// typing (no circular deps). When set on a barrier, all Put/Get operations
// are transparently encrypted.
type StorageBackend interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	Exists(ctx context.Context, key string) (bool, error)
	Scan(ctx context.Context, prefix string, fn func(key string, value []byte) error) error

	// PutIfAbsent atomically stores data under key only if key does not
	// already exist. It returns stored=true when this call created the key,
	// and stored=false (with a nil error) when the key already existed — in
	// that case the store is unchanged. A non-nil error means the outcome is
	// unknown and the caller must not assume either way.
	//
	// Atomicity is provided per consistency domain: single-process backends
	// serialize the check-then-write internally (e.g., with a mutex), while
	// distributed backends must use their native compare-and-set primitive
	// (e.g., a Raft transaction) so the guarantee holds across nodes.
	PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error)

	Close() error
}

// InvocationCounter tracks the number of AEAD encrypt operations performed
// with a single key and enforces the NIST SP 800-38D invocation limit.
// For AES-GCM with random 96-bit nonces, the limit is 2^32 invocations
// to keep nonce collision probability below 2^-32.
//
// Implementations must be safe for concurrent use.
type InvocationCounter interface {
	// Increment atomically increments the counter. Returns
	// ErrInvocationLimitExceeded if the limit would be breached.
	Increment() error

	// Count returns the current invocation count.
	Count() int64

	// Limit returns the configured max invocations. Returns -1 when disabled.
	Limit() int64

	// Seed sets the initial count without limit checking. Used to restore
	// state from on-disk data at startup.
	Seed(count int64)

	// ShouldRotate returns true when invocation count has reached the
	// configured rotation threshold percentage.
	ShouldRotate() bool

	// GetRotationThreshold returns the threshold percentage (e.g., 95.0).
	GetRotationThreshold() float64

	// GetUsagePercentage returns invocations/limit as percentage [0.0, 100.0].
	GetUsagePercentage() float64

	// Reset zeros the counter. Called after key rotation.
	Reset() error

	// IsEnabled reports whether counting is active.
	IsEnabled() bool

	// Close releases resources.
	Close() error
}

// AEADProvider creates cipher.AEAD instances for a given algorithm and key.
// The default SoftwareAEADProvider dispatches to the standard Go crypto
// library. Consumers may inject hardware-backed implementations (e.g.,
// go-xkms) via BarrierConfig.AEADProvider.
type AEADProvider interface {
	NewAEAD(alg SymmetricAlgorithm, key []byte) (cipher.AEAD, error)
}

// NonceTrackerProvider provides detection of nonce reuse for AEAD ciphers.
// Implementations must be safe for concurrent use.
//
// The default in-memory implementation (InMemoryNonceTracker) uses sync.Map
// with full [12]byte nonce keys for lock-free reads. Consumers may inject
// persistent or hardware-backed implementations via
// BarrierConfig.NonceTrackerProvider.
type NonceTrackerProvider interface {
	// CheckAndRecordNonce checks whether nonce has been seen before and, if
	// unique, records it. Returns ErrNonceReuse if the nonce was previously
	// recorded.
	CheckAndRecordNonce(nonce []byte) error

	// Contains reports whether nonce has been previously recorded without
	// modifying the tracker state.
	Contains(nonce []byte) bool

	// Count returns the number of unique nonces currently tracked.
	Count() int

	// Clear removes all tracked nonces. Must only be called after key
	// rotation. Returns an error if the underlying storage fails.
	Clear() error

	// IsEnabled reports whether nonce tracking is active.
	IsEnabled() bool

	// SetEnabled enables or disables nonce tracking.
	SetEnabled(enabled bool)

	// Close releases any resources held by the tracker.
	Close() error
}

// SymmetricEncrypter provides symmetric encryption/decryption operations.
// Hardware-backed strategies return this from BarrierEncryptor() to delegate
// encryption to hardware (keys never leave the hardware boundary).
// Software strategies return nil from BarrierEncryptor(), causing the barrier
// to use its own epoch-based HKDF DEK encryption.
//
// The signature carries no AAD, so a delegated encryptor that implements only
// this interface cannot authenticate the caller's positional context. Implement
// AADSymmetricEncrypter as well to declare that capability.
type SymmetricEncrypter interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// AADSymmetricEncrypter is the OPTIONAL capability a delegated (hardware,
// KMS, enclave) encryptor implements to DECLARE that it authenticates
// caller-supplied additional authenticated data end to end.
//
// A barrier in delegated mode bypasses its own AEAD entirely, so the positional
// binding the software path folds into the AAD (a snapshot frame's
// shard/replica/index/ordinal, a WAL record's shard/segment/ordinal) exists only
// if the delegated encryptor carries it. Implementing this interface is that
// promise: the aad bytes passed to EncryptWithAAD MUST be authenticated such
// that DecryptWithAAD fails when a different aad is supplied at decrypt time.
//
// DO NOT implement this interface on an encryptor that merely ACCEPTS an aad
// argument and drops it. Advertising a binding that does not exist is strictly
// worse than not advertising one: the barrier reports SupportsContextAAD, every
// caller stops suppressing its context, RequireContextAAD passes at startup, and
// the deployment believes it has splice/reorder protection it does not have. An
// encryptor that cannot bind AAD must implement only SymmetricEncrypter, which
// makes the barrier refuse non-empty contexts with ErrHardwareMode.
type AADSymmetricEncrypter interface {
	SymmetricEncrypter

	// EncryptWithAAD seals plaintext, authenticating aad without storing it.
	EncryptWithAAD(plaintext, aad []byte) ([]byte, error)

	// DecryptWithAAD opens a ciphertext sealed under the same aad. A different
	// aad — or none where one was present — must fail authentication.
	DecryptWithAAD(ciphertext, aad []byte) ([]byte, error)
}
