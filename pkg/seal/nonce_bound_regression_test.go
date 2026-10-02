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
	"encoding/binary"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// recordingHandler captures slog records so tests can assert on the loud
// diagnostics the barrier and nonce tracker emit.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordingHandler) WithGroup(string) slog.Handler { return h }

// find reports whether a record at the given level contains substr in its
// message.
func (h *recordingHandler) find(level slog.Level, substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == level && strings.Contains(r.Message, substr) {
			return true
		}
	}
	return false
}

// captureLogs installs a recording slog handler for the duration of the test.
func captureLogs(t *testing.T) *recordingHandler {
	t.Helper()
	h := &recordingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}

// nonceN builds a distinct 12-byte nonce for index n.
func nonceN(n uint64) []byte {
	var b [12]byte
	binary.LittleEndian.PutUint64(b[:8], n)
	return b[:]
}

// TestNonceTrackerBoundedPastCeiling pins the memory bound. The tracker
// releases entries only via Clear, and Clear is reached only through a
// successful Rotate — which fails closed without a StatePersister — so an
// unbounded tracker grows ~40 bytes per encrypt toward the 2^32 invocation
// limit and exhausts memory long before that limit could fail closed.
func TestNonceTrackerBoundedPastCeiling(t *testing.T) {
	logs := captureLogs(t)

	nt := NewNonceTracker(true)
	nt.SetMaxTrackedNonces(4)

	for i := uint64(0); i < 4; i++ {
		if err := nt.CheckAndRecordNonce(nonceN(i)); err != nil {
			t.Fatalf("nonce %d rejected below the ceiling: %v", i, err)
		}
	}
	if got := nt.Count(); got != 4 {
		t.Fatalf("tracked count = %d, want 4", got)
	}

	if err := nt.CheckAndRecordNonce(nonceN(4)); err != nil {
		t.Fatalf("nonce past the ceiling must be admitted, not rejected: %v", err)
	}
	if !nt.Saturated() {
		t.Fatal("tracker must saturate at the ceiling instead of growing without limit")
	}
	// The bound that matters is on MEMORY: the nonce that crossed the ceiling
	// is rolled back, so the tracked set never exceeds maxTracked.
	if got := nt.Count(); got != 4 {
		t.Fatalf("tracked count past the ceiling = %d, want it pinned at the ceiling (4)", got)
	}
	if got := nt.MaxTrackedNonces(); got != 4 {
		t.Fatalf("MaxTrackedNonces = %d, want 4", got)
	}
	if !logs.find(slog.LevelError, "nonce tracker ceiling reached") {
		t.Fatal("reaching the ceiling must be reported at error level")
	}
}

// TestNonceTrackerSaturatedStillDetectsRetainedReuse pins the half of the
// degradation that is free: past the ceiling the tracker stops ADMITTING
// nonces, because admission is what consumes memory, but a lookup costs
// nothing. A repeat of a nonce it already holds must therefore still be
// rejected. Abandoning the check as well turned a bounded loss of coverage into
// a total one — a tracker that answers "no reuse" about a nonce sitting in its
// own map.
func TestNonceTrackerSaturatedStillDetectsRetainedReuse(t *testing.T) {
	captureLogs(t)

	nt := NewNonceTracker(true)
	nt.SetMaxTrackedNonces(2)
	for i := uint64(0); i < 2; i++ {
		if err := nt.CheckAndRecordNonce(nonceN(i)); err != nil {
			t.Fatalf("nonce %d rejected below the ceiling: %v", i, err)
		}
	}
	if err := nt.CheckAndRecordNonce(nonceN(2)); err != nil {
		t.Fatalf("nonce past the ceiling must be admitted: %v", err)
	}
	if !nt.Saturated() {
		t.Fatal("tracker must be saturated after crossing the ceiling")
	}

	// Catastrophic reuse of a RETAINED nonce must still fail closed.
	if err := nt.CheckAndRecordNonce(nonceN(0)); !errors.Is(err, ErrNonceReuse) {
		t.Fatalf("reuse of a retained nonce while saturated returned %v, want ErrNonceReuse: "+
			"the covered prefix stopped being checked", err)
	}
	// Rejecting it must not have grown the set.
	if got := nt.Count(); got != 2 {
		t.Fatalf("tracked count = %d after a saturated reuse check, want 2", got)
	}
	// A nonce outside the covered prefix is accepted unverified and, crucially,
	// still not admitted.
	if err := nt.CheckAndRecordNonce(nonceN(9)); err != nil {
		t.Fatalf("unseen nonce while saturated returned %v, want nil", err)
	}
	if got := nt.Count(); got != 2 {
		t.Fatalf("tracked count = %d after admitting past the ceiling, want the ceiling (2)", got)
	}
}

// TestNonceTrackerCountAndContainsReportTrueState pins metric honesty. Were
// Count and Contains gated on the enabled flag, a saturated tracker — one
// holding maxTracked nonces with detection degraded to a prefix — would report
// "0 tracked" and "false" for nonces sitting in its own map. That is
// the same reading as a tracker that was never enabled, which is the one
// conclusion an operator must not draw from it.
func TestNonceTrackerCountAndContainsReportTrueState(t *testing.T) {
	captureLogs(t)

	nt := NewNonceTracker(true)
	nt.SetMaxTrackedNonces(2)
	for i := uint64(0); i < 3; i++ { // third crosses the ceiling
		if err := nt.CheckAndRecordNonce(nonceN(i)); err != nil {
			t.Fatalf("nonce %d: %v", i, err)
		}
	}
	if got := nt.Count(); got != 2 {
		t.Fatalf("Count() while saturated = %d, want the 2 nonces actually held", got)
	}
	if !nt.Contains(nonceN(0)) {
		t.Fatal("Contains() while saturated denied a nonce the tracker holds")
	}

	// The same honesty after an explicit disable, which preserves history by
	// contract (SetEnabled documents that re-enabling resumes checking against
	// everything recorded).
	nt2 := NewNonceTracker(true)
	if err := nt2.CheckAndRecordNonce(nonceN(5)); err != nil {
		t.Fatalf("record: %v", err)
	}
	nt2.SetEnabled(false)
	if got := nt2.Count(); got != 1 {
		t.Fatalf("Count() after SetEnabled(false) = %d, want the 1 nonce still retained", got)
	}
	if !nt2.Contains(nonceN(5)) {
		t.Fatal("Contains() after SetEnabled(false) denied a nonce that is still retained")
	}
}

// TestNonceTrackerSaturationIsNotDisablement pins that the two states stay
// distinguishable in both directions: saturation must not flip the operator's
// enabled flag, and an explicit disable must not read as saturation.
func TestNonceTrackerSaturationIsNotDisablement(t *testing.T) {
	captureLogs(t)

	nt := NewNonceTracker(true)
	nt.SetMaxTrackedNonces(1)
	_ = nt.CheckAndRecordNonce(nonceN(0))
	_ = nt.CheckAndRecordNonce(nonceN(1))
	if !nt.Saturated() {
		t.Fatal("tracker should be saturated")
	}
	if !nt.IsEnabled() {
		t.Fatal("saturation must not clear the operator's enabled flag: the two are " +
			"different facts with different remedies")
	}

	off := NewNonceTracker(false)
	if off.Saturated() {
		t.Fatal("a tracker that was never enabled must not report saturation")
	}
}

// TestNonceTrackerClearReArmsAfterOverflow pins that rotation restores full
// coverage: the new epoch has no history to retain, so a saturated tracker must
// start admitting again rather than stay capped for the process lifetime.
func TestNonceTrackerClearReArmsAfterOverflow(t *testing.T) {
	captureLogs(t)

	nt := NewNonceTracker(true)
	nt.SetMaxTrackedNonces(1)
	if err := nt.CheckAndRecordNonce(nonceN(0)); err != nil {
		t.Fatalf("first nonce rejected: %v", err)
	}
	if err := nt.CheckAndRecordNonce(nonceN(1)); err != nil {
		t.Fatalf("overflow nonce rejected: %v", err)
	}
	if !nt.Saturated() {
		t.Fatal("tracker should be saturated after crossing the ceiling")
	}

	if err := nt.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if nt.Saturated() {
		t.Fatal("Clear must lift saturation so the new epoch is covered from empty")
	}
	if !nt.IsEnabled() {
		t.Fatal("Clear must leave an enabled tracker enabled")
	}
	if got := nt.Count(); got != 0 {
		t.Fatalf("count after Clear = %d, want 0", got)
	}
	// Detection works again in the new epoch.
	if err := nt.CheckAndRecordNonce(nonceN(0)); err != nil {
		t.Fatalf("re-armed tracker rejected a fresh nonce: %v", err)
	}
	if err := nt.CheckAndRecordNonce(nonceN(0)); err != ErrNonceReuse {
		t.Fatalf("re-armed tracker must detect reuse, got %v", err)
	}
}

// TestNonceTrackerExplicitDisableNotReArmedByClear pins that the re-arm is
// scoped to the overflow case: an operator who disabled tracking keeps it off.
func TestNonceTrackerExplicitDisableNotReArmedByClear(t *testing.T) {
	nt := NewNonceTracker(true)
	nt.SetEnabled(false)
	if err := nt.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if nt.IsEnabled() {
		t.Fatal("Clear must not re-enable an explicitly disabled tracker")
	}
}

// TestNewBarrierWarnsOnUnrotatableNonceTracking pins the second half of the
// hazard: nonce tracking with neither a StatePersister nor an injected
// InvocationCounter cannot rotate at all (Rotate fails closed with
// ErrNoStatePersister), so the tracked set can never be released. That is the
// combination DefaultBarrierConfig produces, so it must be reported.
func TestNewBarrierWarnsOnUnrotatableNonceTracking(t *testing.T) {
	logs := captureLogs(t)

	cfg := DefaultBarrierConfig(7)
	if !cfg.GetNonceTracking() || cfg.StatePersister != nil || cfg.InvocationCounter != nil {
		t.Fatalf("DefaultBarrierConfig no longer produces the unrotatable combination: %+v", cfg)
	}
	_ = MustNewBarrier(cfg)

	if !logs.find(slog.LevelWarn, "the epoch cannot rotate") {
		t.Fatal("a barrier that cannot rotate with nonce tracking on must be reported")
	}
}

// TestNewBarrierQuietWhenRotatable pins that the warning is specific: a
// configuration that CAN rotate does not produce it.
func TestNewBarrierQuietWhenRotatable(t *testing.T) {
	logs := captureLogs(t)

	cfg := DefaultBarrierConfig(7)
	cfg.StatePersister = func(*SealedRootKey) error { return nil }
	_ = MustNewBarrier(cfg)

	if logs.find(slog.LevelWarn, "the epoch cannot rotate") {
		t.Fatal("a barrier with a StatePersister must not be reported as unrotatable")
	}
}

// TestNonceTrackerDefaultCeiling documents the shipped bound so a change to it
// is a deliberate edit rather than a silent regression.
func TestNonceTrackerDefaultCeiling(t *testing.T) {
	if got := NewNonceTracker(true).MaxTrackedNonces(); got != DefaultMaxTrackedNonces {
		t.Fatalf("default ceiling = %d, want %d", got, DefaultMaxTrackedNonces)
	}
	if DefaultMaxTrackedNonces >= DefaultInvocationLimit {
		t.Fatalf("ceiling %d does not bound anything below the invocation limit %d",
			DefaultMaxTrackedNonces, DefaultInvocationLimit)
	}
}
