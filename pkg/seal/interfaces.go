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
// typing (no circular deps).
//
// The barrier never calls this: it is a SHAPE, declared here so an embedder
// that already has such a store can adapt it to a StatePersister in a few
// lines. There is no field or option that installs one on a Barrier, and
// nothing in this module implements it — which is also why the contract is
// pinned by an executable conformance test rather than by prose alone.
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

	// CompareAndDelete atomically removes key only when its stored value still
	// equals expected. It returns deleted=true when this call removed the key,
	// and deleted=false (with a nil error) when the key was absent or held a
	// different value — in that case the store is unchanged. A non-nil error
	// means the outcome is unknown and the caller must not assume either way.
	//
	// This is the release half of the pair PutIfAbsent opens, and the halves
	// are not independent: a guarded create is only as strong as its release.
	// With PutIfAbsent to acquire and a plain Delete to release, two releasers
	// can both read holder A, the first deletes, B legitimately claims the
	// now-absent key, and the second executes its unguarded delete and removes
	// B's value — B having acquired the key under the guarantee PutIfAbsent
	// advertises, and lost it to a releaser that never observed it. Re-reading
	// before the delete does not close this, because the window is BETWEEN the
	// read and the delete. Only a delete conditioned on the value the caller
	// last observed, evaluated where the store serializes writes, closes it.
	//
	// Atomicity is provided per consistency domain, exactly as for
	// PutIfAbsent: single-process backends serialize the compare-then-delete
	// internally, while distributed backends must evaluate the comparison in
	// their native compare-and-set primitive — for a replicated state machine,
	// at the entry's log position, so replicas cannot diverge.
	//
	// Two obligations that are easy to get wrong, both learned from
	// implementing this downstream (go-qrdb#41):
	//
	//   - An ENCRYPTING backend must guard on the ciphertext it read, never on
	//     a fresh encryption of expected. Since this package moved to
	//     cipher.NewGCMWithRandomNonce, encrypting the same plaintext twice
	//     produces different bytes, so a re-encrypted guard refuses EVERY
	//     release — every release silently a no-op, which is worse than the
	//     race it was meant to fix. Read the stored ciphertext, decrypt to
	//     compare in plaintext, and guard the delete on those exact bytes: any
	//     interleaved write replaces them, which is precisely the signal.
	//
	//   - A refused delete is a NORMAL OUTCOME, not an error, and must be
	//     reported through deleted=false. In a replicated state machine an
	//     error for a lost guard stalls consensus over a deterministic
	//     consequence of valid input. A failed guard READ is the opposite and
	//     must refuse rather than decide: treating a local read fault as
	//     "value differs" leaves the key on one replica while peers that read
	//     successfully removed it.
	CompareAndDelete(ctx context.Context, key string, expected []byte) (bool, error)

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
//
// TWO NONCE CONVENTIONS ARE SUPPORTED, and the returned AEAD declares which one
// it follows through NonceSize():
//
//   - NonceSize() > 0 — a caller-nonce AEAD. The barrier draws a nonce of that
//     width from its own random source, writes it into the frame ahead of the
//     payload, and passes it to Seal/Open.
//   - NonceSize() == 0 — a module-nonce AEAD, as returned by
//     cipher.NewGCMWithRandomNonce. It generates its own nonce, prepends it to
//     the Seal output, consumes it from the head of the payload on Open, and
//     counts it in Overhead(). The barrier passes a nil nonce. An implementation
//     that chooses this MUST prepend exactly alg.NonceSize() bytes, because that
//     is the width the barrier reports to a reader and the slice it hands the
//     nonce-reuse tracker.
//
// The two produce identical frames for AES-GCM, which is why the wire format is
// unversioned across the migration; see newAESGCM.
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
