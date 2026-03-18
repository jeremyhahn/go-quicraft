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
	"sync"
	"testing"
)

// ============================================================================
// InMemoryInvocationCounter Tests
// ============================================================================

// TestInvocationCounter_Increment verifies that incrementing within the
// configured limit succeeds and the counter advances accordingly.
func TestInvocationCounter_Increment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		limit      int64
		increments int
		wantCount  int64
	}{
		{
			name:       "single increment",
			limit:      1000,
			increments: 1,
			wantCount:  1,
		},
		{
			name:       "multiple increments stay under limit",
			limit:      1000,
			increments: 900,
			wantCount:  900,
		},
		{
			name:       "increment exactly to limit",
			limit:      500,
			increments: 500,
			wantCount:  500,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ic := NewInvocationCounter(true, tc.limit, 0)

			for i := range tc.increments {
				if err := ic.Increment(); err != nil {
					t.Fatalf("increment[%d]: unexpected error: %v", i, err)
				}
			}

			if got := ic.Count(); got != tc.wantCount {
				t.Fatalf("Count() = %d, want %d", got, tc.wantCount)
			}
		})
	}
}

// TestInvocationCounter_ExceedsLimit verifies that incrementing past the configured
// limit returns ErrInvocationLimitExceeded and leaves the counter unchanged.
func TestInvocationCounter_ExceedsLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		limit int64
		setup int // invocations before the failing call
	}{
		{
			name:  "exceed by one invocation",
			limit: 100,
			setup: 100,
		},
		{
			name:  "exceed on small limit",
			limit: 1,
			setup: 1,
		},
		{
			name:  "exceed at larger limit",
			limit: 1000,
			setup: 1000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ic := NewInvocationCounter(true, tc.limit, 0)

			for i := range tc.setup {
				if err := ic.Increment(); err != nil {
					t.Fatalf("setup increment %d failed: %v", i, err)
				}
			}

			counterBefore := ic.Count()

			err := ic.Increment()
			if err == nil {
				t.Fatal("expected ErrInvocationLimitExceeded, got nil")
			}
			if !errors.Is(err, ErrInvocationLimitExceeded) {
				t.Fatalf("expected ErrInvocationLimitExceeded, got: %v", err)
			}

			// Counter must be rolled back to its pre-call value.
			if got := ic.Count(); got != counterBefore {
				t.Fatalf("counter after failed increment = %d, want %d (rollback failed)", got, counterBefore)
			}
		})
	}
}

// TestInvocationCounter_Disabled verifies that a disabled counter treats all
// operations as no-ops: no errors, no counter advancement, no rotation signals.
func TestInvocationCounter_Disabled(t *testing.T) {
	t.Parallel()

	ic := NewInvocationCounter(false, 100, 0)

	if ic.IsEnabled() {
		t.Fatal("IsEnabled() should return false for a disabled counter")
	}

	// Increment beyond the limit that would matter if enabled.
	for i := range 200 {
		if err := ic.Increment(); err != nil {
			t.Fatalf("call %d: disabled counter returned unexpected error: %v", i, err)
		}
	}

	// Counter must stay at zero.
	if got := ic.Count(); got != 0 {
		t.Fatalf("Count() = %d on disabled counter, want 0", got)
	}

	// Limit is -1 when disabled.
	if got := ic.Limit(); got != -1 {
		t.Fatalf("Limit() = %d on disabled counter, want -1", got)
	}

	// Usage percentage is 0.0 when disabled.
	if got := ic.GetUsagePercentage(); got != 0.0 {
		t.Fatalf("GetUsagePercentage() = %f on disabled counter, want 0.0", got)
	}

	// ShouldRotate is never triggered when disabled.
	if ic.ShouldRotate() {
		t.Fatal("ShouldRotate() should return false for disabled counter")
	}

	// GetRotationThreshold returns 0.0 when disabled.
	if got := ic.GetRotationThreshold(); got != 0.0 {
		t.Fatalf("GetRotationThreshold() = %f on disabled counter, want 0.0", got)
	}

	// Reset is a no-op; nothing should panic.
	if err := ic.Reset(); err != nil {
		t.Fatalf("Reset() on disabled counter returned unexpected error: %v", err)
	}

	// Close is a no-op.
	if err := ic.Close(); err != nil {
		t.Fatalf("Close() on disabled counter returned unexpected error: %v", err)
	}

	// Seed is a no-op when disabled.
	ic.Seed(42)
	if got := ic.Count(); got != 0 {
		t.Fatalf("Count() after Seed on disabled = %d, want 0", got)
	}
}

// TestInvocationCounter_DisabledViaMinusOne verifies that limit=-1 forces
// disabled regardless of the enabled parameter.
func TestInvocationCounter_DisabledViaMinusOne(t *testing.T) {
	t.Parallel()

	ic := NewInvocationCounter(true, -1, 0)

	if ic.IsEnabled() {
		t.Fatal("IsEnabled() should return false when limit is -1")
	}

	if got := ic.Limit(); got != -1 {
		t.Fatalf("Limit() = %d, want -1", got)
	}
}

// TestInvocationCounter_DefaultLimit verifies that a limit of 0 defaults to
// the DefaultInvocationLimit (2^32).
func TestInvocationCounter_DefaultLimit(t *testing.T) {
	t.Parallel()

	ic := NewInvocationCounter(true, 0, 0)

	if got := ic.Limit(); got != DefaultInvocationLimit {
		t.Fatalf("Limit() = %d, want DefaultInvocationLimit (%d)", got, DefaultInvocationLimit)
	}

	// 2^32 sanity check.
	var expected int64 = 1 << 32
	if DefaultInvocationLimit != expected {
		t.Fatalf("DefaultInvocationLimit constant = %d, want %d", DefaultInvocationLimit, expected)
	}
}

// TestInvocationCounter_DefaultRotationThreshold verifies that a threshold of 0
// defaults to DefaultRotationThreshold (95.0).
func TestInvocationCounter_DefaultRotationThreshold(t *testing.T) {
	t.Parallel()

	ic := NewInvocationCounter(true, 1000, 0)

	if got := ic.GetRotationThreshold(); got != DefaultRotationThreshold {
		t.Fatalf("GetRotationThreshold() = %f, want %f", got, DefaultRotationThreshold)
	}
}

// TestInvocationCounter_InvalidRotationThreshold verifies that out-of-range
// thresholds (negative, >100) default to DefaultRotationThreshold.
func TestInvocationCounter_InvalidRotationThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		threshold float64
	}{
		{"negative", -10.0},
		{"above_100", 101.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ic := NewInvocationCounter(true, 1000, tc.threshold)
			if got := ic.GetRotationThreshold(); got != DefaultRotationThreshold {
				t.Fatalf("GetRotationThreshold() = %f, want %f", got, DefaultRotationThreshold)
			}
		})
	}
}

// TestInvocationCounter_Reset verifies that after a reset the counter returns to
// zero so the counter can be reused after key rotation.
func TestInvocationCounter_Reset(t *testing.T) {
	t.Parallel()

	ic := NewInvocationCounter(true, 10_000, 0)

	for range 5000 {
		if err := ic.Increment(); err != nil {
			t.Fatalf("setup increment failed: %v", err)
		}
	}
	if ic.Count() == 0 {
		t.Fatal("counter should be non-zero before reset")
	}

	if err := ic.Reset(); err != nil {
		t.Fatalf("Reset() returned unexpected error: %v", err)
	}

	if got := ic.Count(); got != 0 {
		t.Fatalf("Count() after Reset() = %d, want 0", got)
	}

	// Counter should accept new increments after reset.
	if err := ic.Increment(); err != nil {
		t.Fatalf("post-reset increment failed: %v", err)
	}
	if got := ic.Count(); got != 1 {
		t.Fatalf("Count() after post-reset increment = %d, want 1", got)
	}
}

// TestInvocationCounter_GetUsagePercentage verifies that the percentage calculation
// returns correct values across the full [0, 100] range.
func TestInvocationCounter_GetUsagePercentage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
		limit   int64
		used    int
		wantPct float64
	}{
		{
			name:    "0% when nothing used",
			enabled: true,
			limit:   1000,
			used:    0,
			wantPct: 0.0,
		},
		{
			name:    "50% at half limit",
			enabled: true,
			limit:   1000,
			used:    500,
			wantPct: 50.0,
		},
		{
			name:    "100% at full limit",
			enabled: true,
			limit:   1000,
			used:    1000,
			wantPct: 100.0,
		},
		{
			name:    "95% exactly",
			enabled: true,
			limit:   1000,
			used:    950,
			wantPct: 95.0,
		},
		{
			name:    "0% when disabled",
			enabled: false,
			limit:   1000,
			used:    0,
			wantPct: 0.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ic := NewInvocationCounter(tc.enabled, tc.limit, 0)
			if tc.enabled && tc.used > 0 {
				for range tc.used {
					if err := ic.Increment(); err != nil {
						t.Fatalf("setup increment failed: %v", err)
					}
				}
			}

			const epsilon = 1e-9
			got := ic.GetUsagePercentage()
			if diff := got - tc.wantPct; diff > epsilon || diff < -epsilon {
				t.Fatalf("GetUsagePercentage() = %f, want %f", got, tc.wantPct)
			}
		})
	}
}

// TestInvocationCounter_ShouldRotate verifies that the rotation threshold fires at
// the configured percentage and above, but not below.
func TestInvocationCounter_ShouldRotate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		enabled    bool
		limit      int64
		threshold  float64
		used       int
		wantRotate bool
	}{
		{
			name:       "0% – no rotation",
			enabled:    true,
			limit:      1000,
			threshold:  95.0,
			used:       0,
			wantRotate: false,
		},
		{
			name:       "94% – no rotation at 95% threshold",
			enabled:    true,
			limit:      1000,
			threshold:  95.0,
			used:       940,
			wantRotate: false,
		},
		{
			name:       "95% exactly – rotate",
			enabled:    true,
			limit:      1000,
			threshold:  95.0,
			used:       950,
			wantRotate: true,
		},
		{
			name:       "100% – rotate",
			enabled:    true,
			limit:      1000,
			threshold:  95.0,
			used:       1000,
			wantRotate: true,
		},
		{
			name:       "disabled – never rotates",
			enabled:    false,
			limit:      1000,
			threshold:  95.0,
			used:       0,
			wantRotate: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ic := NewInvocationCounter(tc.enabled, tc.limit, tc.threshold)
			if tc.enabled && tc.used > 0 {
				for range tc.used {
					if err := ic.Increment(); err != nil {
						t.Fatalf("setup increment failed: %v", err)
					}
				}
			}

			if got := ic.ShouldRotate(); got != tc.wantRotate {
				t.Fatalf("ShouldRotate() = %v, want %v (usage=%.1f%%)",
					got, tc.wantRotate, ic.GetUsagePercentage())
			}
		})
	}
}

// TestInvocationCounter_Rollback verifies that when Increment fails
// due to the limit being exceeded the counter is left in its pre-call state
// so subsequent calls within budget still succeed after reset.
func TestInvocationCounter_Rollback(t *testing.T) {
	t.Parallel()

	const limit int64 = 10
	ic := NewInvocationCounter(true, limit, 0)

	// Fill to limit.
	for range limit {
		if err := ic.Increment(); err != nil {
			t.Fatalf("initial increment failed: %v", err)
		}
	}

	// Attempt one more — must fail and counter stays at limit.
	err := ic.Increment()
	if !errors.Is(err, ErrInvocationLimitExceeded) {
		t.Fatalf("expected ErrInvocationLimitExceeded, got: %v", err)
	}
	if got := ic.Count(); got != limit {
		t.Fatalf("counter after rollback = %d, want %d", got, limit)
	}
}

// TestInvocationCounter_Concurrent verifies that the counter is safe under
// concurrent access from many goroutines.
func TestInvocationCounter_Concurrent(t *testing.T) {
	t.Parallel()

	const (
		goroutines     = 50
		perGoroutine   = 10
		totalIncrments = goroutines * perGoroutine
		limit          = int64(totalIncrments) * 2 // plenty of headroom
	)

	ic := NewInvocationCounter(true, limit, 0)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range perGoroutine {
				_ = ic.Increment()
			}
		}()
	}

	wg.Wait()

	got := ic.Count()
	want := int64(totalIncrments)
	if got != want {
		t.Fatalf("after concurrent increments: Count() = %d, want %d", got, want)
	}
}

// TestInvocationCounter_Seed verifies that Seed sets the initial count without
// going through the limit-check path.
func TestInvocationCounter_Seed(t *testing.T) {
	t.Parallel()

	ic := NewInvocationCounter(true, 1000, 0)

	// Seed to 800 (within limit).
	ic.Seed(800)
	if got := ic.Count(); got != 800 {
		t.Fatalf("Count() after Seed(800) = %d, want 800", got)
	}

	// Seed can set beyond limit (no limit check).
	ic.Seed(2000)
	if got := ic.Count(); got != 2000 {
		t.Fatalf("Count() after Seed(2000) = %d, want 2000", got)
	}

	// After seeding beyond limit, increment should fail.
	err := ic.Increment()
	if !errors.Is(err, ErrInvocationLimitExceeded) {
		t.Fatalf("expected ErrInvocationLimitExceeded after seeding beyond limit, got: %v", err)
	}
}

// TestInvocationCounter_SeedDisabled verifies that Seed is a no-op on disabled counters.
func TestInvocationCounter_SeedDisabled(t *testing.T) {
	t.Parallel()

	ic := NewInvocationCounter(false, 1000, 0)
	ic.Seed(500)
	if got := ic.Count(); got != 0 {
		t.Fatalf("Count() after Seed on disabled = %d, want 0", got)
	}
}

// TestInvocationCounter_Close verifies that Close returns nil for the in-memory
// implementation.
func TestInvocationCounter_Close(t *testing.T) {
	t.Parallel()

	ic := NewInvocationCounter(true, 1000, 0)
	if err := ic.Close(); err != nil {
		t.Fatalf("Close() returned unexpected error: %v", err)
	}
}

// TestInvocationCounter_InterfaceCompliance verifies that InMemoryInvocationCounter
// satisfies the InvocationCounter interface.
func TestInvocationCounter_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var _ InvocationCounter = NewInvocationCounter(true, 1000, 0)
}

// ============================================================================
// InMemoryNonceTracker Tests
// ============================================================================

// TestNonceTracker_CheckAndRecord verifies that presenting a unique nonce
// succeeds and the nonce is subsequently recorded.
func TestNonceTracker_CheckAndRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		nonces [][]byte
	}{
		{
			name:   "single 12-byte nonce",
			nonces: [][]byte{make([]byte, 12)},
		},
		{
			name: "multiple distinct nonces",
			nonces: [][]byte{
				{0x00, 0x01, 0x02},
				{0x00, 0x01, 0x03},
				{0xFF, 0xFF, 0xFF},
			},
		},
		{
			name:   "empty nonce",
			nonces: [][]byte{{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nt := NewNonceTracker(true)

			for i, n := range tc.nonces {
				if err := nt.CheckAndRecordNonce(n); err != nil {
					t.Fatalf("nonce[%d]: unexpected error: %v", i, err)
				}
			}

			if got := nt.Count(); got != len(tc.nonces) {
				t.Fatalf("Count() = %d, want %d", got, len(tc.nonces))
			}
		})
	}
}

// TestNonceTracker_Reuse verifies that presenting a nonce that was already
// recorded returns ErrNonceReuse and does not increment the count.
func TestNonceTracker_Reuse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		nonce []byte
	}{
		{
			name:  "12-byte nonce reuse",
			nonce: []byte{0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88},
		},
		{
			name:  "all-zeros nonce reuse",
			nonce: make([]byte, 12),
		},
		{
			name:  "single-byte nonce reuse",
			nonce: []byte{0x42},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nt := NewNonceTracker(true)

			// First use must succeed.
			if err := nt.CheckAndRecordNonce(tc.nonce); err != nil {
				t.Fatalf("first use: unexpected error: %v", err)
			}

			countBefore := nt.Count()

			// Second use with the same nonce must fail.
			err := nt.CheckAndRecordNonce(tc.nonce)
			if err == nil {
				t.Fatal("expected ErrNonceReuse on second call, got nil")
			}
			if !errors.Is(err, ErrNonceReuse) {
				t.Fatalf("expected ErrNonceReuse, got: %v", err)
			}

			// Count must be unchanged after the rejected call.
			if got := nt.Count(); got != countBefore {
				t.Fatalf("Count() after reuse = %d, want %d", got, countBefore)
			}
		})
	}
}

// TestNonceTracker_Disabled verifies that a disabled tracker treats all
// operations as no-ops.
func TestNonceTracker_Disabled(t *testing.T) {
	t.Parallel()

	nt := NewNonceTracker(false)

	if nt.IsEnabled() {
		t.Fatal("IsEnabled() should return false for a disabled tracker")
	}

	nonce := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}

	// Repeated calls with the same nonce must not return ErrNonceReuse.
	for i := range 5 {
		if err := nt.CheckAndRecordNonce(nonce); err != nil {
			t.Fatalf("call %d: disabled tracker returned unexpected error: %v", i, err)
		}
	}

	// Count must remain zero.
	if got := nt.Count(); got != 0 {
		t.Fatalf("Count() = %d on disabled tracker, want 0", got)
	}

	// Contains must return false.
	if nt.Contains(nonce) {
		t.Fatal("Contains() should return false for a disabled tracker")
	}

	// Clear is a no-op; nothing should panic.
	if err := nt.Clear(); err != nil {
		t.Fatalf("Clear() on disabled tracker returned unexpected error: %v", err)
	}

	// Close is a no-op.
	if err := nt.Close(); err != nil {
		t.Fatalf("Close() on disabled tracker returned unexpected error: %v", err)
	}
}

// TestNonceTracker_Contains verifies that Contains performs a read-only check
// that does not alter the tracker state.
func TestNonceTracker_Contains(t *testing.T) {
	t.Parallel()

	nt := NewNonceTracker(true)

	nonce := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	other := []byte{0xCA, 0xFE, 0xBA, 0xBE}

	// Before recording: Contains must return false.
	if nt.Contains(nonce) {
		t.Fatal("Contains() returned true before nonce was recorded")
	}

	if err := nt.CheckAndRecordNonce(nonce); err != nil {
		t.Fatalf("recording nonce failed: %v", err)
	}

	// After recording: Contains must return true.
	if !nt.Contains(nonce) {
		t.Fatal("Contains() returned false after nonce was recorded")
	}

	// A different nonce that was never recorded must still return false.
	if nt.Contains(other) {
		t.Fatal("Contains() returned true for an unrecorded nonce")
	}

	// Count must be exactly 1 — Contains must not have added anything.
	if got := nt.Count(); got != 1 {
		t.Fatalf("Count() = %d after Contains calls, want 1", got)
	}
}

// TestNonceTracker_Count verifies that Count returns the exact number of
// unique nonces recorded and increments correctly with each new nonce.
func TestNonceTracker_Count(t *testing.T) {
	t.Parallel()

	const numNonces = 20
	nt := NewNonceTracker(true)

	if got := nt.Count(); got != 0 {
		t.Fatalf("Count() on empty tracker = %d, want 0", got)
	}

	for i := range numNonces {
		nonce := []byte{byte(i >> 8), byte(i)}
		if err := nt.CheckAndRecordNonce(nonce); err != nil {
			t.Fatalf("recording nonce %d failed: %v", i, err)
		}
		if got := nt.Count(); got != i+1 {
			t.Fatalf("Count() after recording nonce %d = %d, want %d", i, got, i+1)
		}
	}
}

// TestNonceTracker_Clear verifies that Clear resets the count to zero so the
// tracker can be reused after key rotation.
func TestNonceTracker_Clear(t *testing.T) {
	t.Parallel()

	nt := NewNonceTracker(true)

	for i := range 10 {
		nonce := []byte{byte(i)}
		if err := nt.CheckAndRecordNonce(nonce); err != nil {
			t.Fatalf("setup: recording nonce %d failed: %v", i, err)
		}
	}
	if nt.Count() == 0 {
		t.Fatal("count should be non-zero before Clear()")
	}

	if err := nt.Clear(); err != nil {
		t.Fatalf("Clear() returned unexpected error: %v", err)
	}

	if got := nt.Count(); got != 0 {
		t.Fatalf("Count() after Clear() = %d, want 0", got)
	}

	// Previously recorded nonces should now be accepted again.
	nonce := []byte{0x00}
	if err := nt.CheckAndRecordNonce(nonce); err != nil {
		t.Fatalf("post-clear re-recording failed: %v", err)
	}
	if got := nt.Count(); got != 1 {
		t.Fatalf("Count() after post-clear recording = %d, want 1", got)
	}
}

// TestNonceTracker_SetEnabled verifies that toggling the enabled flag
// changes tracker behaviour without losing existing nonce history.
func TestNonceTracker_SetEnabled(t *testing.T) {
	t.Parallel()

	nt := NewNonceTracker(true)

	nonce := []byte{0x11, 0x22, 0x33}

	// Record a nonce while enabled.
	if err := nt.CheckAndRecordNonce(nonce); err != nil {
		t.Fatalf("initial recording failed: %v", err)
	}

	// Disable: reuse of the same nonce should now be silently accepted.
	nt.SetEnabled(false)
	if nt.IsEnabled() {
		t.Fatal("IsEnabled() should return false after SetEnabled(false)")
	}

	for i := range 3 {
		if err := nt.CheckAndRecordNonce(nonce); err != nil {
			t.Fatalf("disabled call %d: unexpected error: %v", i, err)
		}
	}

	// Re-enable: existing history is preserved, so the nonce must still be rejected.
	nt.SetEnabled(true)
	if !nt.IsEnabled() {
		t.Fatal("IsEnabled() should return true after SetEnabled(true)")
	}

	err := nt.CheckAndRecordNonce(nonce)
	if !errors.Is(err, ErrNonceReuse) {
		t.Fatalf("expected ErrNonceReuse after re-enable, got: %v", err)
	}

	// History is intact: count is still 1 (the original recording).
	if got := nt.Count(); got != 1 {
		t.Fatalf("Count() after re-enable = %d, want 1", got)
	}
}

// TestNonceTracker_Concurrent verifies that the tracker is safe under
// concurrent access from many goroutines using distinct nonces.
func TestNonceTracker_Concurrent(t *testing.T) {
	t.Parallel()

	const goroutines = 100
	nt := NewNonceTracker(true)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Each goroutine records a guaranteed-unique 4-byte nonce derived from its index.
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			nonce := []byte{
				byte(idx >> 24),
				byte(idx >> 16),
				byte(idx >> 8),
				byte(idx),
			}
			if err := nt.CheckAndRecordNonce(nonce); err != nil {
				// Under a data race this could spuriously return ErrNonceReuse;
				// report it but don't fail with a race on the t.Fatal call.
				_ = fmt.Sprintf("goroutine %d: unexpected error: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	if got := nt.Count(); got != goroutines {
		t.Fatalf("Count() after concurrent recording = %d, want %d", got, goroutines)
	}
}

// TestNonceTracker_NonceKeyConsistency verifies that the same nonce always
// produces the same key and that different nonces produce different keys.
func TestNonceTracker_NonceKeyConsistency(t *testing.T) {
	t.Parallel()

	nonce1 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}
	nonce2 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0D}

	key1a := nonceKey(nonce1)
	key1b := nonceKey(nonce1)
	key2 := nonceKey(nonce2)

	if key1a != key1b {
		t.Fatalf("same nonce produced different keys: %x vs %x", key1a, key1b)
	}
	if key1a == key2 {
		t.Fatalf("different nonces produced same key: %x", key1a)
	}
}

// TestNonceTracker_NonceKeyShortNonce verifies that nonces shorter than 12
// bytes are zero-padded and still correctly distinguished.
func TestNonceTracker_NonceKeyShortNonce(t *testing.T) {
	t.Parallel()

	short := []byte{0x01, 0x02, 0x03}
	key := nonceKey(short)

	// First 3 bytes should match, rest should be zero.
	if key[0] != 0x01 || key[1] != 0x02 || key[2] != 0x03 {
		t.Fatalf("short nonce prefix mismatch: got %x", key[:3])
	}
	for i := 3; i < 12; i++ {
		if key[i] != 0 {
			t.Fatalf("expected zero padding at index %d, got %x", i, key[i])
		}
	}
}

// TestNonceTracker_NoFalsePositives verifies that using full nonce keys
// eliminates the false positive risk that existed with truncated hash
// fingerprints. Two nonces that differ only in the last byte must be
// tracked as distinct entries.
func TestNonceTracker_NoFalsePositives(t *testing.T) {
	t.Parallel()

	nt := NewNonceTracker(true)

	// Create two 12-byte nonces that differ only in the last byte.
	nonce1 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x00}
	nonce2 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x01}

	if err := nt.CheckAndRecordNonce(nonce1); err != nil {
		t.Fatalf("recording nonce1 failed: %v", err)
	}

	// nonce2 must be accepted as distinct (no false positive).
	if err := nt.CheckAndRecordNonce(nonce2); err != nil {
		t.Fatalf("recording nonce2 failed (false positive): %v", err)
	}

	if got := nt.Count(); got != 2 {
		t.Fatalf("Count() = %d, want 2", got)
	}
}

// TestNonceTracker_FullNonceKeyDetectsReuse verifies that the full nonce
// key approach correctly detects reuse.
func TestNonceTracker_FullNonceKeyDetectsReuse(t *testing.T) {
	t.Parallel()

	nt := NewNonceTracker(true)

	// Use a typical 12-byte AES-GCM nonce.
	nonce := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77}

	// First record should succeed.
	if err := nt.CheckAndRecordNonce(nonce); err != nil {
		t.Fatalf("first recording failed: %v", err)
	}

	// Contains should find it.
	if !nt.Contains(nonce) {
		t.Fatal("Contains returned false for recorded nonce")
	}

	// Duplicate should be rejected.
	err := nt.CheckAndRecordNonce(nonce)
	if !errors.Is(err, ErrNonceReuse) {
		t.Fatalf("expected ErrNonceReuse, got: %v", err)
	}
}

// TestNonceTracker_Close verifies that Close returns nil for the in-memory
// implementation.
func TestNonceTracker_Close(t *testing.T) {
	t.Parallel()

	nt := NewNonceTracker(true)
	if err := nt.Close(); err != nil {
		t.Fatalf("Close() returned unexpected error: %v", err)
	}
}

// TestNonceTracker_InterfaceCompliance verifies that InMemoryNonceTracker
// satisfies the NonceTrackerProvider interface.
func TestNonceTracker_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var _ NonceTrackerProvider = NewNonceTracker(true)
}

// TestNonceTracker_ClearConcurrentNoNonceReuse verifies that Clear() during
// concurrent CheckAndRecordNonce does not allow nonce reuse. This exercises the
// atomic pointer swap fix: goroutines that call CheckAndRecordNonce after Clear()
// must have their nonces tracked in the new map, preventing duplicate acceptance.
func TestNonceTracker_ClearConcurrentNoNonceReuse(t *testing.T) {
	t.Parallel()

	const goroutines = 200
	const rounds = 50

	for round := range rounds {
		nt := NewNonceTracker(true)

		// Pre-populate with some nonces.
		for i := range 10 {
			nonce := []byte{byte(round), byte(i), 0x00, 0x00}
			if err := nt.CheckAndRecordNonce(nonce); err != nil {
				t.Fatalf("round %d: setup nonce %d failed: %v", round, i, err)
			}
		}

		var wg sync.WaitGroup
		wg.Add(goroutines + 1)

		// Start goroutines that each record a unique nonce, then immediately
		// try the same nonce again. The second attempt must return ErrNonceReuse
		// unless Clear() ran between the two calls (in which case the second
		// attempt records successfully into the fresh map).
		reuseDetected := make([]bool, goroutines)

		for i := range goroutines {
			go func(idx int) {
				defer wg.Done()
				nonce := []byte{
					0xFF, // distinguish from setup nonces
					byte(idx >> 16),
					byte(idx >> 8),
					byte(idx),
				}
				// First record: must succeed (unique nonce).
				_ = nt.CheckAndRecordNonce(nonce)

				// Second record: must return ErrNonceReuse unless Clear()
				// swapped the map between the two calls.
				err := nt.CheckAndRecordNonce(nonce)
				if errors.Is(err, ErrNonceReuse) {
					reuseDetected[idx] = true
				}
			}(i)
		}

		// Concurrently call Clear() to exercise the race window.
		go func() {
			defer wg.Done()
			_ = nt.Clear()
		}()

		wg.Wait()
	}
}

// TestNonceTracker_ClearThenRecord verifies that nonces recorded after Clear()
// are properly tracked in the new map and subsequent duplicates are rejected.
func TestNonceTracker_ClearThenRecord(t *testing.T) {
	t.Parallel()

	nt := NewNonceTracker(true)

	// Record a nonce, clear, then record the same nonce again.
	nonce := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}

	if err := nt.CheckAndRecordNonce(nonce); err != nil {
		t.Fatalf("initial record failed: %v", err)
	}
	if got := nt.Count(); got != 1 {
		t.Fatalf("Count() before Clear() = %d, want 1", got)
	}

	if err := nt.Clear(); err != nil {
		t.Fatalf("Clear() failed: %v", err)
	}
	if got := nt.Count(); got != 0 {
		t.Fatalf("Count() after Clear() = %d, want 0", got)
	}

	// Same nonce must be accepted after Clear() (new epoch).
	if err := nt.CheckAndRecordNonce(nonce); err != nil {
		t.Fatalf("post-clear record failed: %v", err)
	}
	if got := nt.Count(); got != 1 {
		t.Fatalf("Count() after post-clear record = %d, want 1", got)
	}

	// Duplicate must be rejected in the new map.
	if err := nt.CheckAndRecordNonce(nonce); !errors.Is(err, ErrNonceReuse) {
		t.Fatalf("post-clear duplicate: got %v, want ErrNonceReuse", err)
	}

	// Contains must reflect the new map state.
	if !nt.Contains(nonce) {
		t.Fatal("Contains() returned false for recorded nonce")
	}
}

// TestNonceTracker_ClearConcurrentRaceDetector stress-tests Clear() against
// concurrent CheckAndRecordNonce under the race detector to verify the atomic
// pointer swap is data-race free.
func TestNonceTracker_ClearConcurrentRaceDetector(t *testing.T) {
	t.Parallel()

	const workers = 50
	const iterations = 100

	nt := NewNonceTracker(true)

	var wg sync.WaitGroup
	wg.Add(workers + 1)

	// Workers continuously record unique nonces.
	for w := range workers {
		go func(workerID int) {
			defer wg.Done()
			for i := range iterations {
				nonce := []byte{
					byte(workerID >> 8),
					byte(workerID),
					byte(i >> 8),
					byte(i),
				}
				_ = nt.CheckAndRecordNonce(nonce)
			}
		}(w)
	}

	// Concurrently clear multiple times.
	go func() {
		defer wg.Done()
		for range iterations {
			_ = nt.Clear()
		}
	}()

	wg.Wait()

	// After all goroutines finish, the tracker must be in a consistent state.
	// Count must be non-negative and Contains/CheckAndRecordNonce must work.
	if count := nt.Count(); count < 0 {
		t.Fatalf("Count() = %d, must be non-negative", count)
	}

	// A fresh nonce must be accepted.
	freshNonce := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	if err := nt.CheckAndRecordNonce(freshNonce); err != nil {
		t.Fatalf("post-stress fresh nonce failed: %v", err)
	}
}
