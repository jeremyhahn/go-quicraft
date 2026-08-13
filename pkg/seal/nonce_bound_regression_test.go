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
	if nt.IsEnabled() {
		t.Fatal("tracker must disable itself at the ceiling instead of growing without limit")
	}
	if got := nt.MaxTrackedNonces(); got != 4 {
		t.Fatalf("MaxTrackedNonces = %d, want 4", got)
	}
	if !logs.find(slog.LevelError, "nonce tracker ceiling reached") {
		t.Fatal("reaching the ceiling must be reported at error level")
	}
}

// TestNonceTrackerClearReArmsAfterOverflow pins that rotation restores
// detection: the new epoch has no history to retain, so an auto-disabled
// tracker must come back rather than stay off for the process lifetime.
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
	if nt.IsEnabled() {
		t.Fatal("tracker should be disabled after overflow")
	}

	if err := nt.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if !nt.IsEnabled() {
		t.Fatal("Clear must re-arm a tracker that auto-disabled on overflow")
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
	if !cfg.NonceTracking || cfg.StatePersister != nil || cfg.InvocationCounter != nil {
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
