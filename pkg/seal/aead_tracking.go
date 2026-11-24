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
	"log/slog"
	"sync"
	"sync/atomic"
)

const (
	// DefaultInvocationLimit is the NIST SP 800-38D limit of 2^32 AES-GCM invocations with random 96-bit nonces.
	DefaultInvocationLimit int64 = 1 << 32

	// DefaultRotationThreshold is the percentage of the invocation limit at
	// which ShouldRotate() returns true. At 95%, rotation triggers with 5%
	// headroom before the hard limit.
	DefaultRotationThreshold float64 = 95.0

	// NonceSize is the standard 96-bit AEAD nonce size; nonces of any length are tracked in full.
	NonceSize = 12

	// DefaultMaxTrackedNonces bounds InMemoryNonceTracker; past it the tracker saturates, so exact reuse detection covers only a prefix of the epoch.
	DefaultMaxTrackedNonces int64 = 4 << 20
)

// AEAD safety sentinel errors.
var (
	// ErrInvocationLimitExceeded is returned when the key has reached its invocation limit (NIST SP 800-38D 8.3); rotate the key.
	ErrInvocationLimitExceeded = errors.New(
		"quicraft/seal: AEAD invocation limit exceeded - key rotation required")

	// ErrNonceReuse is returned when a nonce was already used with the current key.
	ErrNonceReuse = errors.New("quicraft/seal: catastrophic nonce reuse detected - encryption rejected")
)

// Compile-time interface compliance checks.
var (
	_ InvocationCounter    = (*InMemoryInvocationCounter)(nil)
	_ NonceTrackerProvider = (*InMemoryNonceTracker)(nil)
)

// InMemoryInvocationCounter counts AEAD encryptions under one key against the NIST SP 800-38D limit; call Reset after rotation. Lock-free.
type InMemoryInvocationCounter struct {
	enabled           bool
	count             atomic.Int64
	limit             int64
	rotationThreshold float64
}

// NewInvocationCounter creates a counter; limit 0 means 2^32, -1 or enabled=false disables it, rotationThreshold 0 means 95.
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

// Increment adds one invocation, returning ErrInvocationLimitExceeded without counting if the limit would be exceeded. Thread-safe.
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

// Count returns the invocation count, 0 when disabled. Thread-safe.
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

// Seed sets the count without a limit check, for restoring a persisted count at startup. Thread-safe.
func (ic *InMemoryInvocationCounter) Seed(count int64) {
	if ic.enabled {
		ic.count.Store(count)
	}
}

// ShouldRotate reports whether the count has reached the rotation threshold. Thread-safe.
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

// GetUsagePercentage returns the percentage of the limit used, 0 when disabled. Thread-safe.
func (ic *InMemoryInvocationCounter) GetUsagePercentage() float64 {
	if !ic.enabled || ic.limit == 0 {
		return 0.0
	}
	// Seed stores a restored count without a limit check, so the count can
	// exceed the limit; clamp to keep the documented range.
	pct := (float64(ic.count.Load()) / float64(ic.limit)) * 100.0
	return min(max(pct, 0.0), 100.0)
}

// Reset zeroes the counter; call it only after rotating to a new key. Thread-safe; always nil.
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

// InMemoryNonceTracker detects AEAD nonce reuse lock-free with full-length nonce keys; Clear swaps the whole epoch atomically, and past maxTracked it saturates.
type InMemoryNonceTracker struct {
	enabled atomic.Bool
	epoch   atomic.Pointer[nonceEpoch]

	// maxTracked is the ceiling on tracked nonces. Zero or negative disables
	// the ceiling.
	maxTracked atomic.Int64
}

// nonceEpoch is the tracker state that Clear must reset as one unit. Splitting
// it across independent atomics let a CheckAndRecordNonce racing Clear count
// against one epoch while inserting into another.
type nonceEpoch struct {
	nonces sync.Map
	count  atomic.Int64

	// saturated records that the ceiling was reached in this epoch; separate from the enabled flag.
	saturated atomic.Bool
}

// NewNonceTracker creates an InMemoryNonceTracker bounded at
// DefaultMaxTrackedNonces. Pass enabled=false to disable tracking (all
// operations become no-ops returning nil / zero values).
func NewNonceTracker(enabled bool) *InMemoryNonceTracker {
	nt := &InMemoryNonceTracker{}
	nt.enabled.Store(enabled)
	nt.epoch.Store(&nonceEpoch{})
	nt.maxTracked.Store(DefaultMaxTrackedNonces)
	return nt
}

// SetMaxTrackedNonces sets the tracked-nonce ceiling; <= 0 removes it. Thread-safe.
func (nt *InMemoryNonceTracker) SetMaxTrackedNonces(max int64) {
	nt.maxTracked.Store(max)
}

// MaxTrackedNonces returns the current ceiling on tracked nonces.
func (nt *InMemoryNonceTracker) MaxTrackedNonces() int64 {
	return nt.maxTracked.Load()
}

// nonceKey returns the full nonce as a string map key.
func nonceKey(nonce []byte) string {
	return string(nonce)
}

// CheckAndRecordNonce returns ErrNonceReuse for a tracked nonce, else records it unless saturated or disabled. Thread-safe.
func (nt *InMemoryNonceTracker) CheckAndRecordNonce(nonce []byte) error {
	if !nt.enabled.Load() {
		return nil
	}

	key := nonceKey(nonce)
	// One epoch for the whole call: a concurrent Clear replaces the epoch, never
	// the fields of the one this call is working against.
	e := nt.epoch.Load()

	// Saturated: check only. A new nonce is accepted unverified — it is outside
	// the covered prefix — but a repeat of a retained one is still caught.
	if e.saturated.Load() {
		if _, exists := e.nonces.Load(key); exists {
			return ErrNonceReuse
		}
		return nil
	}

	// Check for reuse first, so repeats are caught at the ceiling.
	if _, loaded := e.nonces.LoadOrStore(key, struct{}{}); loaded {
		return ErrNonceReuse
	}

	// This nonce is new. Admitting it past the ceiling is what would grow the
	// tracker without limit, so the entry that crosses the ceiling is rolled
	// back and the tracker saturates instead.
	n := e.count.Add(1)
	if max := nt.maxTracked.Load(); max > 0 && n > max {
		e.nonces.Delete(key)
		e.count.Add(-1)
		e.saturateAtCeiling(max)
	}
	return nil
}

// saturateAtCeiling stops admission at the ceiling and logs it once; Clear lifts it.
func (e *nonceEpoch) saturateAtCeiling(max int64) {
	if !e.saturated.CompareAndSwap(false, true) {
		return
	}
	slog.Error("quicraft/seal: nonce tracker ceiling reached; reuse detection now covers only the nonces already tracked",
		"tracked", e.count.Load(),
		"max_tracked", max,
		"remedy", "rotate the epoch (requires a StatePersister), raise SetMaxTrackedNonces, or disable NonceTracking",
	)
}

// Saturated reports whether the tracker reached its ceiling and admits no new nonces. Thread-safe.
func (nt *InMemoryNonceTracker) Saturated() bool {
	e := nt.epoch.Load()
	return e != nil && e.saturated.Load()
}

// Contains reports whether nonce is tracked, whatever the enabled flag. Thread-safe.
func (nt *InMemoryNonceTracker) Contains(nonce []byte) bool {
	// A zero-value tracker (one not built by NewNonceTracker) has no epoch. It
	// holds nothing, so the answer is false — and reaching through the nil
	// pointer to say so would panic.
	e := nt.epoch.Load()
	if e == nil {
		return false
	}
	_, exists := e.nonces.Load(nonceKey(nonce))
	return exists
}

// Count returns the number of tracked nonces in every state. Thread-safe, O(1).
func (nt *InMemoryNonceTracker) Count() int {
	e := nt.epoch.Load()
	if e == nil {
		return 0
	}
	return int(e.count.Load())
}

// Clear atomically swaps in an empty epoch; call it only after key rotation. When disabled it keeps history and lifts saturation. Thread-safe; always nil.
func (nt *InMemoryNonceTracker) Clear() error {
	// A saturated tracker regains full coverage here: the epoch has changed, so
	// there is no history left to retain and admission can resume from empty. A
	// tracker disabled explicitly via SetEnabled stays disabled.
	if !nt.enabled.Load() {
		if e := nt.epoch.Load(); e != nil {
			e.saturated.Store(false)
		}
		return nil
	}

	nt.epoch.Store(&nonceEpoch{})
	return nil
}

// IsEnabled reports whether nonce tracking is active.
//
// Thread-safe.
func (nt *InMemoryNonceTracker) IsEnabled() bool {
	return nt.enabled.Load()
}

// SetEnabled enables or disables tracking, keeping existing history. Thread-safe.
func (nt *InMemoryNonceTracker) SetEnabled(enabled bool) {
	nt.enabled.Store(enabled)
}

// Close is a no-op for the in-memory implementation. Always returns nil.
func (nt *InMemoryNonceTracker) Close() error {
	return nil
}
