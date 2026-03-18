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
type SymmetricEncrypter interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}
