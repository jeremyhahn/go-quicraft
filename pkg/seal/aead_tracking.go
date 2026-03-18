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
	"sync"
	"sync/atomic"
)

const (
	// DefaultInvocationLimit is the NIST SP 800-38D limit for AES-GCM with
	// random 96-bit nonces: 2^32 invocations keeps nonce collision probability
	// below 2^-32.
	//
	// Reference: https://csrc.nist.gov/pubs/sp/800/38/d/final
	DefaultInvocationLimit int64 = 1 << 32

	// DefaultRotationThreshold is the percentage of the invocation limit at
	// which ShouldRotate() returns true. At 95%, rotation triggers with 5%
	// headroom before the hard limit.
	DefaultRotationThreshold float64 = 95.0

	// NonceSize is the standard AEAD nonce size in bytes (96-bit for
	// AES-GCM and ChaCha20-Poly1305). The full nonce is used as the
	// sync.Map key ([12]byte) to eliminate false positives from hash
	// truncation.
	NonceSize = 12
)

// AEAD safety sentinel errors.
var (
	// ErrInvocationLimitExceeded is returned when the AEAD key has been used
	// for more encrypt operations than the configured limit. Key rotation is
	// required before further encryption.
	//
	// NIST SP 800-38D, Section 8.3: For AES-GCM with random 96-bit nonces,
	// the number of invocations per key is limited to 2^32 to keep the
	// probability of nonce collision below 2^-32.
	ErrInvocationLimitExceeded = errors.New(
		"quicraft/seal: AEAD invocation limit exceeded - key rotation required")

	// ErrNonceReuse is returned when a nonce is presented that was already used
	// with the current key. Nonce reuse is catastrophic for AEAD ciphers:
	//   - AES-GCM: breaks authentication and can leak the authentication key
	//   - ChaCha20-Poly1305: leaks keystream and compromises confidentiality
	ErrNonceReuse = errors.New("quicraft/seal: catastrophic nonce reuse detected - encryption rejected")
)

// Compile-time interface compliance checks.
var (
	_ InvocationCounter    = (*InMemoryInvocationCounter)(nil)
	_ NonceTrackerProvider = (*InMemoryNonceTracker)(nil)
)

// InMemoryInvocationCounter tracks the number of AEAD encrypt operations
// performed with a single key and enforces NIST SP 800-38D invocation limits.
//
// When the limit is reached the key must be rotated. After rotation call Reset
// so the counter starts fresh for the new key.
//
// NIST SP 800-38D, Section 8.3: For AES-GCM with random 96-bit nonces,
// the number of invocations per key is limited to 2^32 to keep the
// probability of nonce collision below 2^-32. The per-invocation plaintext
// limit is 2^39 - 256 bits (~64 GiB), enforced by GCM's 32-bit block counter.
//
// Thread-safe: all operations use atomic instructions — no mutex is required.
type InMemoryInvocationCounter struct {
	enabled           bool
	count             atomic.Int64
	limit             int64
	rotationThreshold float64
}

// NewInvocationCounter creates an InMemoryInvocationCounter with the given parameters.
// If limit is 0, defaults to DefaultInvocationLimit (2^32).
// If limit is -1, counting is disabled regardless of the enabled parameter.
// If rotationThreshold is 0, defaults to DefaultRotationThreshold (95.0).
// Pass enabled=false to disable tracking entirely (all operations become no-ops).
func NewInvocationCounter(enabled bool, limit int64, rotationThreshold float64) *InMemoryInvocationCounter {
	if limit == -1 {
		enabled = false
	}
	if limit <= 0 {
		limit = DefaultInvocationLimit
	}
	if rotationThreshold <= 0 || rotationThreshold > 100 {
		rotationThreshold = DefaultRotationThreshold
	}
	return &InMemoryInvocationCounter{
		enabled:           enabled,
		limit:             limit,
		rotationThreshold: rotationThreshold,
	}
}

// Increment atomically increments the invocation counter by one after
// verifying that doing so would not exceed the configured limit.
//
// If the limit would be exceeded the counter is rolled back and
// ErrInvocationLimitExceeded is returned. The caller must rotate the key
// before attempting further encryption.
//
// Thread-safe.
func (ic *InMemoryInvocationCounter) Increment() error {
	if !ic.enabled {
		return nil
	}
	newCount := ic.count.Add(1)
	if newCount > ic.limit {
		ic.count.Add(-1)
		return ErrInvocationLimitExceeded
	}
	return nil
}

// Count returns the current invocation count.
// Returns 0 when tracking is disabled.
//
// Thread-safe.
func (ic *InMemoryInvocationCounter) Count() int64 {
	if !ic.enabled {
		return 0
	}
	return ic.count.Load()
}

// Limit returns the configured invocation limit.
// Returns -1 when tracking is disabled.
func (ic *InMemoryInvocationCounter) Limit() int64 {
	if !ic.enabled {
		return -1
	}
	return ic.limit
}

// Seed sets the initial count without limit checking. This allows consumers
// (e.g., go-qrdb) to restore invocation counts from on-disk epoch data at
// startup without triggering the limit-check path.
//
// Thread-safe.
func (ic *InMemoryInvocationCounter) Seed(count int64) {
	if ic.enabled {
		ic.count.Store(count)
	}
}

// ShouldRotate returns true when the invocation count has reached the
// configured rotation threshold percentage of the limit.
//
// Thread-safe.
func (ic *InMemoryInvocationCounter) ShouldRotate() bool {
	if !ic.enabled {
		return false
	}
	return ic.GetUsagePercentage() >= ic.rotationThreshold
}

// GetRotationThreshold returns the configured rotation threshold percentage.
// Returns 0.0 when tracking is disabled.
func (ic *InMemoryInvocationCounter) GetRotationThreshold() float64 {
	if !ic.enabled {
		return 0.0
	}
	return ic.rotationThreshold
}

// GetUsagePercentage returns the fraction of the limit consumed as a
// percentage in the range [0.0, 100.0]. Returns 0.0 when tracking is disabled.
//
// Thread-safe.
func (ic *InMemoryInvocationCounter) GetUsagePercentage() float64 {
	if !ic.enabled || ic.limit == 0 {
		return 0.0
	}
	return (float64(ic.count.Load()) / float64(ic.limit)) * 100.0
}

// Reset sets the invocation counter back to zero. This must only be called
// after successfully rotating to a new key — resetting without key rotation
// invalidates the safety guarantee.
//
// Thread-safe. Always returns nil for the in-memory implementation.
func (ic *InMemoryInvocationCounter) Reset() error {
	if ic.enabled {
		ic.count.Store(0)
	}
	return nil
}

// IsEnabled reports whether invocation counting is active.
func (ic *InMemoryInvocationCounter) IsEnabled() bool {
	return ic.enabled
}

// Close is a no-op for the in-memory implementation. Always returns nil.
func (ic *InMemoryInvocationCounter) Close() error {
	return nil
}

// InMemoryNonceTracker provides lock-free detection of nonce reuse for AEAD
// ciphers (AES-GCM, ChaCha20-Poly1305).
//
// Nonce reuse in AEAD ciphers is catastrophic: reusing a nonce with the same key
// in AES-GCM completely breaks authentication and can expose the authentication
// key, enabling arbitrary message forgery. For ChaCha20-Poly1305, reuse leaks
// keystream and compromises confidentiality.
//
// Nonces are stored as full [12]byte array keys in a sync.Map for lock-free
// reads and zero false positives. Using the full nonce eliminates the ~2^32
// collision window that existed with truncated SHA-256 fingerprints.
// An atomic counter provides O(1) Count(). Memory grows linearly with the number
// of encryption operations; plan for key rotation to bound memory usage.
//
// Clear() uses an atomic pointer swap to replace the nonce map, eliminating the
// TOCTOU race that would exist with Range+Delete. Concurrent CheckAndRecordNonce
// calls either complete against the old map (their nonces are discarded with it)
// or operate on the new map (their nonces are correctly tracked).
//
// Thread-safe: reads use sync.Map's lock-free fast path; writes use
// sync.Map's internal sharding. The enabled flag is protected by atomic operations.
type InMemoryNonceTracker struct {
	enabled atomic.Bool
	nonces  atomic.Pointer[sync.Map]
	count   atomic.Int64
}

// NewNonceTracker creates an InMemoryNonceTracker. Pass enabled=false to disable
// tracking (all operations become no-ops returning nil / zero values).
func NewNonceTracker(enabled bool) *InMemoryNonceTracker {
	nt := &InMemoryNonceTracker{}
	nt.enabled.Store(enabled)
	nt.nonces.Store(&sync.Map{})
	return nt
}

// nonceKey converts a nonce byte slice to a [12]byte array key for use in
// sync.Map. Using the full nonce eliminates false positives from hash
// truncation. Nonces shorter than 12 bytes are zero-padded; nonces longer
// than 12 bytes are truncated to the standard AEAD nonce size.
func nonceKey(nonce []byte) [12]byte {
	var key [12]byte
	copy(key[:], nonce)
	return key
}

// CheckAndRecordNonce checks whether nonce has been seen before and, if unique,
// records it so future calls with the same bytes will be rejected.
//
// Returns ErrNonceReuse if the nonce was previously recorded.
// Returns nil and records the nonce when it is unique.
// Returns nil without recording when tracking is disabled.
//
// Thread-safe.
func (nt *InMemoryNonceTracker) CheckAndRecordNonce(nonce []byte) error {
	if !nt.enabled.Load() {
		return nil
	}

	key := nonceKey(nonce)

	if _, loaded := nt.nonces.Load().LoadOrStore(key, struct{}{}); loaded {
		return ErrNonceReuse
	}

	nt.count.Add(1)
	return nil
}

// Contains reports whether nonce has been previously recorded without modifying
// the tracker state. Returns false when tracking is disabled.
//
// Thread-safe.
func (nt *InMemoryNonceTracker) Contains(nonce []byte) bool {
	if !nt.enabled.Load() {
		return false
	}

	key := nonceKey(nonce)
	_, exists := nt.nonces.Load().Load(key)
	return exists
}

// Count returns the number of unique nonces currently tracked.
// Returns 0 when tracking is disabled.
//
// Thread-safe (O(1) via atomic counter).
func (nt *InMemoryNonceTracker) Count() int {
	if !nt.enabled.Load() {
		return 0
	}
	return int(nt.count.Load())
}

// Clear removes all tracked nonces by atomically swapping the internal map with
// a fresh empty one, letting the old map be garbage collected. This must only be
// called after rotating to a new key — clearing nonce history without changing
// the key eliminates the protection this tracker provides.
//
// The atomic pointer swap eliminates the TOCTOU race that would exist with
// Range+Delete: concurrent CheckAndRecordNonce calls either complete against the
// old map (their nonces are discarded with it) or operate on the new map (their
// nonces are correctly tracked in the new epoch).
//
// Is a no-op when tracking is disabled.
// Always returns nil for the in-memory implementation.
//
// Thread-safe.
func (nt *InMemoryNonceTracker) Clear() error {
	if !nt.enabled.Load() {
		return nil
	}

	nt.nonces.Store(&sync.Map{})
	nt.count.Store(0)
	return nil
}

// IsEnabled reports whether nonce tracking is active.
//
// Thread-safe.
func (nt *InMemoryNonceTracker) IsEnabled() bool {
	return nt.enabled.Load()
}

// SetEnabled enables or disables nonce tracking. Disabling does not clear existing
// nonce history; if re-enabled, the tracker resumes checking against all previously
// recorded nonces.
//
// Thread-safe.
func (nt *InMemoryNonceTracker) SetEnabled(enabled bool) {
	nt.enabled.Store(enabled)
}

// Close is a no-op for the in-memory implementation. Always returns nil.
func (nt *InMemoryNonceTracker) Close() error {
	return nil
}
