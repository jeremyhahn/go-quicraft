package seal

import (
	"errors"
	"testing"
)

// TestNonceTrackingEdge_NonPositiveCeilingRemovesBound pins the
// SetMaxTrackedNonces contract that a value <= 0 removes the ceiling: a
// tracker first bounded at 2 must, once the ceiling is lifted, admit past it
// without saturating.
func TestNonceTrackingEdge_NonPositiveCeilingRemovesBound(t *testing.T) {
	for _, max := range []int64{0, -1} {
		nt := NewNonceTracker(true)
		nt.SetMaxTrackedNonces(2)
		nt.SetMaxTrackedNonces(max)

		for i := uint64(0); i < 5; i++ {
			if err := nt.CheckAndRecordNonce(nonceN(i)); err != nil {
				t.Fatalf("max=%d: nonce %d rejected with no ceiling: %v", max, i, err)
			}
		}
		if got := nt.Count(); got != 5 {
			t.Fatalf("max=%d: Count() = %d, want 5 (every nonce admitted)", max, got)
		}
		if nt.Saturated() {
			t.Fatalf("max=%d: tracker saturated with the ceiling removed", max)
		}
		if got := nt.MaxTrackedNonces(); got != max {
			t.Fatalf("MaxTrackedNonces() = %d, want %d", got, max)
		}
	}
}

// TestNonceTrackingEdge_ZeroValueIsSafe pins that a tracker not built by
// NewNonceTracker has no epoch and answers every query as an empty, disabled
// tracker rather than panicking through the nil epoch pointer.
func TestNonceTrackingEdge_ZeroValueIsSafe(t *testing.T) {
	var nt InMemoryNonceTracker

	if nt.Contains(nonceN(0)) {
		t.Fatal("zero-value Contains() = true, want false")
	}
	if got := nt.Count(); got != 0 {
		t.Fatalf("zero-value Count() = %d, want 0", got)
	}
	if nt.Saturated() {
		t.Fatal("zero-value Saturated() = true, want false")
	}
	if nt.IsEnabled() {
		t.Fatal("zero-value IsEnabled() = true, want false")
	}
	if err := nt.CheckAndRecordNonce(nonceN(0)); err != nil {
		t.Fatalf("zero-value CheckAndRecordNonce() = %v, want nil", err)
	}
	if err := nt.Clear(); err != nil {
		t.Fatalf("zero-value Clear() = %v, want nil", err)
	}
}

// TestNonceTrackingEdge_ClearWhileDisabledKeepsHistory pins the Clear contract
// for a disabled tracker: it keeps the recorded history and only lifts
// saturation, so re-enabling resumes detection against that history.
func TestNonceTrackingEdge_ClearWhileDisabledKeepsHistory(t *testing.T) {
	captureLogs(t)

	nt := NewNonceTracker(true)
	nt.SetMaxTrackedNonces(2)
	for i := uint64(0); i < 3; i++ { // third crosses the ceiling
		if err := nt.CheckAndRecordNonce(nonceN(i)); err != nil {
			t.Fatalf("nonce %d: %v", i, err)
		}
	}
	if !nt.Saturated() {
		t.Fatal("tracker should be saturated after crossing the ceiling")
	}

	nt.SetEnabled(false)
	if err := nt.Clear(); err != nil {
		t.Fatalf("Clear while disabled: %v", err)
	}
	if nt.Saturated() {
		t.Fatal("Clear while disabled must lift saturation")
	}
	if got := nt.Count(); got != 2 {
		t.Fatalf("Count() after Clear while disabled = %d, want the 2 retained nonces", got)
	}
	if !nt.Contains(nonceN(0)) {
		t.Fatal("Clear while disabled dropped a recorded nonce")
	}
	if nt.IsEnabled() {
		t.Fatal("Clear must not re-enable a disabled tracker")
	}

	nt.SetEnabled(true)
	if err := nt.CheckAndRecordNonce(nonceN(0)); !errors.Is(err, ErrNonceReuse) {
		t.Fatalf("reuse of retained nonce after re-enable = %v, want ErrNonceReuse", err)
	}
}

// TestInvocationTrackingEdge_SeedPastLimitUsageCapped pins the documented
// [0.0, 100.0] range of GetUsagePercentage when Seed, which does no limit
// check, restores a count above the limit.
func TestInvocationTrackingEdge_SeedPastLimitUsageCapped(t *testing.T) {
	ic := NewInvocationCounter(true, 1000, 0)
	ic.Seed(2000)

	if got := ic.GetUsagePercentage(); got > 100.0 {
		t.Fatalf("GetUsagePercentage() after Seed(2000) with limit 1000 = %v, want <= 100.0", got)
	}
	if !ic.ShouldRotate() {
		t.Fatal("ShouldRotate() = false with the count past the limit")
	}
}
