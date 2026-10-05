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
	// AES-GCM and ChaCha20-Poly1305). XChaCha20-Poly1305 uses a 192-bit
	// (24-byte) nonce. The full nonce — of whatever length — is used as the
	// sync.Map key (a string) so that nonces of any length are tracked
	// exactly, with no truncation and therefore no false positives.
	NonceSize = 12

	// DefaultMaxTrackedNonces bounds InMemoryNonceTracker. Entries are
	// released only by Clear, which only a successful Rotate calls, so a
	// deployment that cannot rotate (no StatePersister) would otherwise grow
	// the tracker toward the 2^32 invocation limit — roughly 170 GB — and
	// exhaust memory long before that limit could fail closed. This ceiling is
	// far above any legitimate pre-rotation working set (~4M nonces, on the
	// order of 160 MB) and exists so the failure is a loud, bounded
	// degradation instead of an OOM.
	//
	// COVERAGE, STATED PLAINLY: 4Mi is 0.0977% of DefaultInvocationLimit. An
	// epoch that rides its NIST budget to the end therefore has fewer than one
	// invocation in a thousand covered by exact reuse detection, and the
	// remainder rests on the random-96-bit-nonce + invocation-cap argument
	// alone. That is not a defect of the ceiling — retaining 2^32 nonces is not
	// implementable — but it is the reason detection must never be described as
	// covering "the epoch": it covers a PREFIX of the epoch. Past the ceiling
	// the tracker becomes SATURATED rather than blind: it admits no new nonces
	// (so memory stays flat) and keeps rejecting repeats of the ones it already
	// holds, which costs nothing. See InMemoryNonceTracker.Saturated.
	DefaultMaxTrackedNonces int64 = 4 << 20
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
// Nonces are stored as full-length string keys in a sync.Map for lock-free
// reads and zero false positives. Using the entire nonce — regardless of its
// length — eliminates the ~2^32 collision window that existed with truncated
// SHA-256 fingerprints and the truncation collisions that a fixed [12]byte key
// would cause for 24-byte XChaCha20-Poly1305 nonces sharing a 12-byte prefix.
// An atomic counter provides O(1) Count(). Memory grows linearly with the number
// of encryption operations; plan for key rotation to bound memory usage.
//
// The nonce set, its count and its saturation flag form one nonceEpoch, and
// Clear replaces the whole epoch with a single atomic pointer swap. Every
// CheckAndRecordNonce loads the epoch once and does all of its work against
// that one value, so a call racing Clear either completes entirely against the
// old epoch (and is discarded with it) or entirely against the new one. It can
// never insert into the new set while counting against the old, or saturate a
// fresh epoch with a ceiling crossing from the one it replaced.
//
// Thread-safe and lock-free: reads use sync.Map's lock-free fast path; writes
// use sync.Map's internal sharding. The enabled flag is protected by atomic
// operations.
//
// COVERAGE IS A PREFIX, NOT AN EPOCH. Memory is bounded by maxTracked, and
// entries are released only by Clear, which is reached only through a successful
// key rotation. Past the ceiling the tracker becomes SATURATED: it stops
// ADMITTING new nonces — which is the only thing that would grow memory — and
// keeps CHECKING every presented nonce against the set it already holds, which
// is free. Exact detection therefore covers the first maxTracked encryptions of
// an epoch and no more; the remainder rests on the random-nonce plus
// invocation-cap argument (NIST SP 800-38D). Saturated reports this state
// distinctly from IsEnabled, so a monitor can tell "operator turned it off" from
// "ceiling reached, coverage now partial". Clear ends saturation, because the new
// epoch starts with no history to retain.
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

	// saturated records that the ceiling was reached, so the error is logged
	// once per epoch, no further nonces are admitted, and Clear knows to lift
	// it. It is deliberately NOT the enabled flag: an operator's SetEnabled
	// decision and a memory ceiling are different facts and conflating them
	// made a saturated tracker report "disabled, zero nonces tracked" — the
	// same reading as a tracker that was never turned on.
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

// SetMaxTrackedNonces sets the ceiling on tracked nonces. A value <= 0 removes
// the ceiling, which is only safe when the epoch is guaranteed to rotate (and
// therefore Clear the tracker) well before memory becomes a concern.
//
// Thread-safe.
func (nt *InMemoryNonceTracker) SetMaxTrackedNonces(max int64) {
	nt.maxTracked.Store(max)
}

// MaxTrackedNonces returns the current ceiling on tracked nonces.
func (nt *InMemoryNonceTracker) MaxTrackedNonces() int64 {
	return nt.maxTracked.Load()
}

// nonceKey converts a nonce byte slice to a string key for use in sync.Map.
// The full nonce of any length is used verbatim, so 12-byte (AES-GCM,
// ChaCha20-Poly1305) and 24-byte (XChaCha20-Poly1305) nonces are tracked
// exactly. There is no truncation, so two distinct nonces that share a 12-byte
// prefix never collide and a unique nonce is never falsely rejected as reuse.
func nonceKey(nonce []byte) string {
	return string(nonce)
}

// CheckAndRecordNonce checks whether nonce has been seen before and, if unique
// and there is room below the ceiling, records it so future calls with the same
// bytes will be rejected.
//
// Returns ErrNonceReuse if the nonce is present in the tracked set.
// Returns nil and records the nonce when it is unique and the tracker has room.
// Returns nil WITHOUT recording when the tracker is saturated (the ceiling has
// been reached) or when tracking is disabled.
//
// The reuse check runs against the tracked set unconditionally, saturated or
// not: looking a nonce up costs no memory, so there is no reason to stop
// checking the prefix the tracker does hold. What saturation stops is ADMISSION,
// which is the only operation that grows memory.
//
// Thread-safe.
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

	// The reuse check runs FIRST so a repeat of an already-recorded nonce is
	// still caught at the ceiling: it costs no additional memory.
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

// saturateAtCeiling stops admission once the ceiling is reached and reports it
// once, at error level. Continuing to admit would exhaust memory; the retained
// set stays checkable, so detection degrades to the covered prefix rather than
// vanishing. Rotating the epoch (Clear) lifts it.
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

// Saturated reports whether the tracker has reached its ceiling. A saturated
// tracker admits no new nonces — so exact detection covers only the invocations
// recorded before the ceiling — but still rejects repeats of what it holds.
//
// It is the honest coverage signal and is deliberately separate from IsEnabled:
// an operator who turned tracking off and a tracker that ran out of room are
// different conditions with different remedies.
//
// Thread-safe.
func (nt *InMemoryNonceTracker) Saturated() bool {
	e := nt.epoch.Load()
	return e != nil && e.saturated.Load()
}

// Contains reports whether nonce is present in the tracked set, without
// modifying tracker state.
//
// It reports the tracker's ACTUAL contents rather than gating on the enabled
// flag: a tracker that recorded nonces and was later disabled still holds them
// (SetEnabled preserves history, and re-enabling resumes checking against it),
// so answering "false" there would misreport the state an operator is asking
// about.
//
// Thread-safe.
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

// Count returns the number of unique nonces currently tracked.
//
// It reports the ACTUAL tracked count in every state, including saturated and
// disabled. Zeroing it whenever the enabled flag was false made the two states
// that matter most — "never turned on" and "ceiling reached, coverage now
// partial" — read identically as "0 nonces tracked", which is precisely the
// reading an operator must not be given.
//
// Thread-safe (O(1) via atomic counter).
func (nt *InMemoryNonceTracker) Count() int {
	e := nt.epoch.Load()
	if e == nil {
		return 0
	}
	return int(e.count.Load())
}

// Clear removes all tracked nonces by atomically swapping in a fresh, empty
// epoch — nonce set, count and saturation flag together — letting the old one
// be garbage collected. This must only be called after rotating to a new key —
// clearing nonce history without changing the key eliminates the protection
// this tracker provides.
//
// The single pointer swap is what makes the reset atomic: concurrent
// CheckAndRecordNonce calls either complete entirely against the old epoch
// (their nonces, count and any saturation are discarded with it) or entirely
// against the new one (their nonces are correctly tracked in the new epoch).
// Swapping the fields one at a time instead lets a racing call count against
// one epoch while inserting into the other, and lets a ceiling crossing from
// the old epoch saturate the new one.
//
// When tracking is disabled, Clear keeps the recorded history and only lifts
// saturation. Always returns nil for the in-memory implementation.
//
// Thread-safe.
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
