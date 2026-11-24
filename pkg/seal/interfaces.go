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

// StorageBackend is a context-aware string-keyed store an embedder can adapt to a StatePersister; the barrier never calls it, and its contract is pinned by a conformance test.
type StorageBackend interface {
	// Get returns the data stored under key.
	Get(ctx context.Context, key string) ([]byte, error)
	// Put stores data under key, replacing any existing value.
	Put(ctx context.Context, key string, data []byte) error
	// Delete removes key and its data.
	Delete(ctx context.Context, key string) error
	// List returns the keys that start with prefix.
	List(ctx context.Context, prefix string) ([]string, error)
	// Exists reports whether key is stored.
	Exists(ctx context.Context, key string) (bool, error)
	// Scan calls fn with each key that starts with prefix and its value,
	// stopping at the first error fn returns.
	Scan(ctx context.Context, prefix string, fn func(key string, value []byte) error) error

	// PutIfAbsent stores data under key only if absent, reporting whether it did; an error means the outcome is unknown. Atomic within the backend's consistency domain.
	PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error)

	// CompareAndDelete removes key only while its value equals expected (nil matches only an empty value), reporting whether it did; a mismatch is not an error. Encrypting backends guard on the stored ciphertext.
	CompareAndDelete(ctx context.Context, key string, expected []byte) (bool, error)

	// Close releases the resources held by the backend.
	Close() error
}

// InvocationCounter counts AEAD encryptions per key and enforces the NIST SP 800-38D limit of 2^32 for random 96-bit nonces; safe for concurrent use.
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

// AEADProvider creates cipher.AEAD instances (BarrierConfig.AEADProvider); NonceSize() > 0 means the barrier supplies nonces, 0 means the AEAD prepends exactly alg.NonceSize() bytes itself.
type AEADProvider interface {
	// NewAEAD returns an AEAD cipher for the algorithm keyed with key.
	NewAEAD(alg SymmetricAlgorithm, key []byte) (cipher.AEAD, error)
}

// NonceTrackerProvider detects AEAD nonce reuse; safe for concurrent use. Inject one via BarrierConfig.NonceTrackerProvider.
type NonceTrackerProvider interface {
	// CheckAndRecordNonce records nonce, returning ErrNonceReuse if it was seen before.
	CheckAndRecordNonce(nonce []byte) error

	// Contains reports whether nonce was recorded, without recording it.
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

// SymmetricEncrypter is a delegated encryptor returned by hardware strategies from BarrierEncryptor(); it carries no AAD (see AADSymmetricEncrypter).
type SymmetricEncrypter interface {
	// Encrypt returns the ciphertext of plaintext.
	Encrypt(plaintext []byte) ([]byte, error)
	// Decrypt returns the plaintext of ciphertext, or an error when it
	// fails authentication.
	Decrypt(ciphertext []byte) ([]byte, error)
}

// AADSymmetricEncrypter declares that a delegated encryptor authenticates aad end to end; implement it only if DecryptWithAAD fails on a different aad.
type AADSymmetricEncrypter interface {
	SymmetricEncrypter

	// EncryptWithAAD seals plaintext, authenticating aad without storing it.
	EncryptWithAAD(plaintext, aad []byte) ([]byte, error)

	// DecryptWithAAD opens a ciphertext sealed under the same aad. A different
	// aad — or none where one was present — must fail authentication.
	DecryptWithAAD(ciphertext, aad []byte) ([]byte, error)
}
