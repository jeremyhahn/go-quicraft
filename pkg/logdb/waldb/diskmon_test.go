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

package waldb

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// mockDiskUsage returns a DiskUsageFunc that reports a controllable
// usage value. The returned atomic pointer allows tests to change the
// reported usage between calls.
func mockDiskUsage(usedBytes *atomic.Uint64) DiskUsageFunc {
	return func(dir string) (uint64, error) {
		return usedBytes.Load(), nil
	}
}

// mockDiskUsageError returns a DiskUsageFunc that always fails.
func mockDiskUsageError() DiskUsageFunc {
	return func(dir string) (uint64, error) {
		return 0, &DiskUsageError{Dir: dir, Err: errors.New("mock statfs failure")}
	}
}

// TestDiskMonitorThrottle verifies that MaybeCheck only performs a disk
// usage query every checkEvery writes.
func TestDiskMonitorThrottle(t *testing.T) {
	var callCount atomic.Uint64
	usage := func(dir string) (uint64, error) {
		callCount.Add(1)
		return 0, nil
	}

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(10),
		WithDiskUsageFunc(usage),
	)

	// Write 25 times. Should trigger checks at write 10 and 20.
	for i := 0; i < 25; i++ {
		if err := dm.MaybeCheck(); err != nil {
			t.Fatalf("MaybeCheck at write %d: %v", i+1, err)
		}
	}

	got := callCount.Load()
	if got != 2 {
		t.Fatalf("expected 2 disk usage checks, got %d", got)
	}

	// Verify write count is tracked correctly.
	if wc := dm.WriteCount(); wc != 25 {
		t.Fatalf("expected write count 25, got %d", wc)
	}
}

// TestDiskMonitorThrottleInvalidInterval verifies behavior when
// checkEvery is 1 (check on every write).
func TestDiskMonitorThrottleEveryWrite(t *testing.T) {
	var callCount atomic.Uint64
	usage := func(dir string) (uint64, error) {
		callCount.Add(1)
		return 0, nil
	}

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithDiskUsageFunc(usage),
	)

	for i := 0; i < 5; i++ {
		if err := dm.MaybeCheck(); err != nil {
			t.Fatalf("MaybeCheck: %v", err)
		}
	}

	if got := callCount.Load(); got != 5 {
		t.Fatalf("expected 5 checks (every write), got %d", got)
	}
}

// TestDiskMonitorWarningCallback verifies the warning callback fires
// when usage crosses the warning threshold.
func TestDiskMonitorWarningCallback(t *testing.T) {
	var usedBytes atomic.Uint64
	var warningCalled atomic.Bool
	var warningUsed, warningMax uint64

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithWarningPercent(0.80),
		WithFullPercent(0.95),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
		WithOnWarning(func(used, max uint64) {
			warningCalled.Store(true)
			warningUsed = used
			warningMax = max
		}),
	)

	// Start below warning threshold.
	usedBytes.Store(700)
	if err := dm.MaybeCheck(); err != nil {
		t.Fatalf("MaybeCheck at 70%%: %v", err)
	}
	if warningCalled.Load() {
		t.Fatal("warning callback should not fire at 70%")
	}

	// Cross warning threshold (80% of 1000 = 800).
	usedBytes.Store(850)
	if err := dm.MaybeCheck(); err != nil {
		t.Fatalf("MaybeCheck at 85%%: %v", err)
	}
	if !warningCalled.Load() {
		t.Fatal("warning callback should fire at 85%")
	}
	if warningUsed != 850 {
		t.Fatalf("expected warning used=850, got %d", warningUsed)
	}
	if warningMax != 1000 {
		t.Fatalf("expected warning max=1000, got %d", warningMax)
	}
}

// TestDiskMonitorWarningCallbackNotRepeated verifies the warning
// callback fires only once per state transition.
func TestDiskMonitorWarningCallbackNotRepeated(t *testing.T) {
	var usedBytes atomic.Uint64
	var warningCount atomic.Uint64

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithWarningPercent(0.80),
		WithFullPercent(0.95),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
		WithOnWarning(func(used, max uint64) {
			warningCount.Add(1)
		}),
	)

	// Cross warning threshold multiple times while staying above it.
	usedBytes.Store(850)
	for i := 0; i < 5; i++ {
		_ = dm.MaybeCheck()
	}

	if got := warningCount.Load(); got != 1 {
		t.Fatalf("expected warning to fire once, got %d times", got)
	}

	// Drop below threshold.
	usedBytes.Store(500)
	_ = dm.MaybeCheck()

	// Cross again -- should fire a second time.
	usedBytes.Store(900)
	_ = dm.MaybeCheck()

	if got := warningCount.Load(); got != 2 {
		t.Fatalf("expected warning to fire twice after re-crossing, got %d", got)
	}
}

// TestDiskMonitorFullCallback verifies the full callback fires and
// ErrDiskFull is returned when usage exceeds the full threshold.
func TestDiskMonitorFullCallback(t *testing.T) {
	var usedBytes atomic.Uint64
	var fullCalled atomic.Bool
	var fullUsed, fullMax uint64

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithWarningPercent(0.80),
		WithFullPercent(0.95),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
		WithOnFull(func(used, max uint64) {
			fullCalled.Store(true)
			fullUsed = used
			fullMax = max
		}),
	)

	// Cross full threshold (95% of 1000 = 950).
	usedBytes.Store(960)
	err := dm.MaybeCheck()
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("expected ErrDiskFull, got %v", err)
	}
	if !fullCalled.Load() {
		t.Fatal("full callback should fire at 96%")
	}
	if fullUsed != 960 {
		t.Fatalf("expected full used=960, got %d", fullUsed)
	}
	if fullMax != 1000 {
		t.Fatalf("expected full max=1000, got %d", fullMax)
	}
}

// TestDiskMonitorFullCallbackNotRepeated verifies the full callback
// fires only once per state transition.
func TestDiskMonitorFullCallbackNotRepeated(t *testing.T) {
	var usedBytes atomic.Uint64
	var fullCount atomic.Uint64

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithFullPercent(0.95),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
		WithOnFull(func(used, max uint64) {
			fullCount.Add(1)
		}),
	)

	usedBytes.Store(970)
	for i := 0; i < 5; i++ {
		_ = dm.MaybeCheck()
	}

	if got := fullCount.Load(); got != 1 {
		t.Fatalf("expected full to fire once, got %d times", got)
	}

	// Drop below full but still above warning.
	usedBytes.Store(900)
	_ = dm.MaybeCheck()

	// Cross full again.
	usedBytes.Store(980)
	_ = dm.MaybeCheck()

	if got := fullCount.Load(); got != 2 {
		t.Fatalf("expected full to fire twice after re-crossing, got %d", got)
	}
}

// TestDiskMonitorFullSetsWarning verifies that entering the full state
// also sets the warning state, and dropping below full but above warning
// does not re-fire the warning callback.
func TestDiskMonitorFullSetsWarning(t *testing.T) {
	var usedBytes atomic.Uint64
	var warningCount atomic.Uint64

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithWarningPercent(0.80),
		WithFullPercent(0.95),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
		WithOnWarning(func(used, max uint64) {
			warningCount.Add(1)
		}),
	)

	// Go directly to full (bypassing warning).
	usedBytes.Store(960)
	_ = dm.MaybeCheck()

	// Warning should NOT have been called because we went straight
	// to full state (warning is implicitly set but callback is not
	// fired since the full state handles it).
	if got := warningCount.Load(); got != 0 {
		t.Fatalf("expected warning count 0 when going straight to full, got %d", got)
	}

	// Drop to warning zone (above 80%, below 95%).
	usedBytes.Store(900)
	_ = dm.MaybeCheck()

	// Warning should NOT fire because lastWarning was already set
	// from the full state.
	if got := warningCount.Load(); got != 0 {
		t.Fatalf("expected warning count still 0 after dropping to warning zone, got %d", got)
	}
}

// TestDiskMonitorDisabledWhenMaxBytesZero verifies MaybeCheck is a
// no-op when maxBytes is 0 (unlimited).
func TestDiskMonitorDisabledWhenMaxBytesZero(t *testing.T) {
	var callCount atomic.Uint64
	usage := func(dir string) (uint64, error) {
		callCount.Add(1)
		return 999, nil
	}

	dm := NewDiskMonitor("/tmp/test-wal", 0,
		WithCheckEvery(1),
		WithDiskUsageFunc(usage),
	)

	for i := 0; i < 10; i++ {
		if err := dm.MaybeCheck(); err != nil {
			t.Fatalf("MaybeCheck: %v", err)
		}
	}

	if got := callCount.Load(); got != 0 {
		t.Fatalf("expected 0 disk usage checks when disabled, got %d", got)
	}
}

// TestDiskMonitorNilSafe verifies MaybeCheck on a nil DiskMonitor is
// safe and returns nil.
func TestDiskMonitorNilSafe(t *testing.T) {
	var dm *DiskMonitor
	if err := dm.MaybeCheck(); err != nil {
		t.Fatalf("nil DiskMonitor.MaybeCheck: %v", err)
	}
}

// TestDiskMonitorDiskUsageError verifies that a disk usage query error
// does not block writes. The monitor should gracefully ignore the error
// and return nil.
func TestDiskMonitorDiskUsageError(t *testing.T) {
	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithDiskUsageFunc(mockDiskUsageError()),
	)

	if err := dm.MaybeCheck(); err != nil {
		t.Fatalf("expected nil on disk usage error, got %v", err)
	}
}

// TestDiskMonitorForceCheck verifies ForceCheck performs an immediate
// check regardless of the write counter.
func TestDiskMonitorForceCheck(t *testing.T) {
	var usedBytes atomic.Uint64
	var fullCalled atomic.Bool

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(100),
		WithFullPercent(0.95),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
		WithOnFull(func(used, max uint64) {
			fullCalled.Store(true)
		}),
	)

	// ForceCheck should work even without any prior writes.
	usedBytes.Store(960)
	err := dm.ForceCheck()
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("ForceCheck: expected ErrDiskFull, got %v", err)
	}
	if !fullCalled.Load() {
		t.Fatal("full callback should fire on ForceCheck")
	}
}

// TestDiskMonitorForceCheckDisabled verifies ForceCheck on a disabled
// monitor (maxBytes=0) returns nil.
func TestDiskMonitorForceCheckDisabled(t *testing.T) {
	dm := NewDiskMonitor("/tmp/test-wal", 0)
	if err := dm.ForceCheck(); err != nil {
		t.Fatalf("ForceCheck on disabled monitor: %v", err)
	}
}

// TestDiskMonitorForceCheckNilSafe verifies ForceCheck on nil is safe.
func TestDiskMonitorForceCheckNilSafe(t *testing.T) {
	var dm *DiskMonitor
	if err := dm.ForceCheck(); err != nil {
		t.Fatalf("nil DiskMonitor.ForceCheck: %v", err)
	}
}

// TestDiskMonitorStateTransitionFullToOK verifies that dropping from
// full to below both thresholds clears both states and fires callbacks
// again on the next crossing.
func TestDiskMonitorStateTransitionFullToOK(t *testing.T) {
	var usedBytes atomic.Uint64
	var warningCount, fullCount atomic.Uint64

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithWarningPercent(0.80),
		WithFullPercent(0.95),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
		WithOnWarning(func(used, max uint64) {
			warningCount.Add(1)
		}),
		WithOnFull(func(used, max uint64) {
			fullCount.Add(1)
		}),
	)

	// Enter full state.
	usedBytes.Store(960)
	_ = dm.MaybeCheck()
	if fullCount.Load() != 1 {
		t.Fatal("full should fire once")
	}

	// Drop to healthy.
	usedBytes.Store(100)
	_ = dm.MaybeCheck()

	// Cross warning again.
	usedBytes.Store(850)
	_ = dm.MaybeCheck()
	if warningCount.Load() != 1 {
		t.Fatalf("expected warning to fire once after full->ok->warning, got %d", warningCount.Load())
	}

	// Cross full again.
	usedBytes.Store(960)
	_ = dm.MaybeCheck()
	if fullCount.Load() != 2 {
		t.Fatalf("expected full to fire twice, got %d", fullCount.Load())
	}
}

// TestDiskMonitorCustomThresholds verifies that custom threshold
// percentages are applied correctly.
func TestDiskMonitorCustomThresholds(t *testing.T) {
	var usedBytes atomic.Uint64
	var warningCalled atomic.Bool

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithWarningPercent(0.50),
		WithFullPercent(0.75),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
		WithOnWarning(func(used, max uint64) {
			warningCalled.Store(true)
		}),
	)

	// 50% warning threshold: 500 bytes.
	usedBytes.Store(499)
	_ = dm.MaybeCheck()
	if warningCalled.Load() {
		t.Fatal("warning should not fire at 49.9%")
	}

	usedBytes.Store(500)
	_ = dm.MaybeCheck()
	if !warningCalled.Load() {
		t.Fatal("warning should fire at 50%")
	}
}

// TestDiskMonitorIntegrationWithDB verifies that SaveState checks disk
// usage when a DiskMonitor is configured.
func TestDiskMonitorIntegrationWithDB(t *testing.T) {
	var usedBytes atomic.Uint64
	var fullCalled atomic.Bool

	dir := t.TempDir()
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithDiskMonitorOpts(1000,
			WithCheckEvery(1),
			WithFullPercent(0.95),
			WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
			WithOnFull(func(used, max uint64) {
				fullCalled.Store(true)
			}),
		),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Normal write should succeed.
	usedBytes.Store(100)
	err = db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 1, Vote: 1, Commit: 0},
	}})
	if err != nil {
		t.Fatalf("SaveState with low usage: %v", err)
	}

	// Simulate disk full.
	usedBytes.Store(960)
	err = db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 2, Vote: 1, Commit: 1},
	}})
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("expected ErrDiskFull, got %v", err)
	}
	if !fullCalled.Load() {
		t.Fatal("full callback should have fired")
	}
}

// TestDiskMonitorIntegrationWithDBNoMonitor verifies SaveState works
// correctly when no DiskMonitor is configured (default).
func TestDiskMonitorIntegrationWithDBNoMonitor(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	err = db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 1, Vote: 1, Commit: 0},
	}})
	if err != nil {
		t.Fatalf("SaveState without monitor: %v", err)
	}
}

// TestDiskMonitorNoCallbacksConfigured verifies the monitor works when
// no callbacks are registered (only the error is returned).
func TestDiskMonitorNoCallbacksConfigured(t *testing.T) {
	var usedBytes atomic.Uint64

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
	)

	// Cross full threshold without callbacks.
	usedBytes.Store(960)
	err := dm.MaybeCheck()
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("expected ErrDiskFull, got %v", err)
	}

	// Cross only warning threshold.
	usedBytes.Store(870)
	err = dm.MaybeCheck()
	if err != nil {
		t.Fatalf("expected nil at warning level, got %v", err)
	}
}

// TestDiskUsageError verifies the DiskUsageError type.
func TestDiskUsageError(t *testing.T) {
	inner := errors.New("mock error")
	err := &DiskUsageError{Dir: "/tmp/wal", Err: inner}

	expected := "waldb: failed to query disk usage for /tmp/wal: mock error"
	if got := err.Error(); got != expected {
		t.Fatalf("error message: got %q, want %q", got, expected)
	}

	if !errors.Is(err, inner) {
		t.Fatal("Unwrap should return inner error")
	}
}

// TestDiskUsageErrorUnwrap verifies the DiskUsageError Unwrap method.
func TestDiskUsageErrorUnwrap(t *testing.T) {
	inner := errors.New("statfs failed")
	err := &DiskUsageError{Dir: "/data/wal", Err: inner}

	var target *DiskUsageError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should match DiskUsageError")
	}
	if target.Dir != "/data/wal" {
		t.Fatalf("expected dir /data/wal, got %s", target.Dir)
	}
}

// TestDiskMonitorRecoveryCallback verifies that WithOnRecovery fires when
// disk usage transitions from above the full threshold to below it.
func TestDiskMonitorRecoveryCallback(t *testing.T) {
	var usedBytes atomic.Uint64
	var recoveryCalled atomic.Bool
	var recoveryUsed, recoveryMax uint64

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithFullPercent(0.90),
		WithOnFull(func(_, _ uint64) {}),
		WithOnRecovery(func(used, maxBytes uint64) {
			recoveryUsed = used
			recoveryMax = maxBytes
			recoveryCalled.Store(true)
		}),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
	)

	// Drive usage above full threshold (90% of 1000 = 900).
	usedBytes.Store(950)
	err := dm.MaybeCheck()
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("expected ErrDiskFull, got %v", err)
	}

	// Recovery callback should NOT fire while still full.
	if recoveryCalled.Load() {
		t.Fatal("recovery callback should not fire while full")
	}

	// Drop below full threshold but above warning.
	usedBytes.Store(860)
	err = dm.MaybeCheck()
	if err != nil {
		t.Fatalf("expected nil error after recovery, got %v", err)
	}

	// Recovery callback should have fired.
	if !recoveryCalled.Load() {
		t.Fatal("recovery callback should fire on full-to-normal transition")
	}
	if recoveryUsed != 860 {
		t.Errorf("recovery used = %d, want 860", recoveryUsed)
	}
	if recoveryMax != 1000 {
		t.Errorf("recovery maxBytes = %d, want 1000", recoveryMax)
	}
}

// TestDiskMonitorRecoveryNotCalledWhenNeverFull verifies recovery does not
// fire when usage was never above the full threshold.
func TestDiskMonitorRecoveryNotCalledWhenNeverFull(t *testing.T) {
	var usedBytes atomic.Uint64
	var recoveryCalled atomic.Bool

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithFullPercent(0.90),
		WithOnRecovery(func(_, _ uint64) {
			recoveryCalled.Store(true)
		}),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
	)

	// Usage below all thresholds.
	usedBytes.Store(100)
	if err := dm.MaybeCheck(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recoveryCalled.Load() {
		t.Fatal("recovery callback should not fire when usage was never full")
	}
}

// TestDiskMonitorRecoveryRepeated verifies recovery fires each time
// usage transitions from full to below-full.
func TestDiskMonitorRecoveryRepeated(t *testing.T) {
	var usedBytes atomic.Uint64
	var recoveryCount atomic.Uint64

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithFullPercent(0.90),
		WithOnFull(func(_, _ uint64) {}),
		WithOnRecovery(func(_, _ uint64) {
			recoveryCount.Add(1)
		}),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
	)

	for i := 0; i < 3; i++ {
		// Go full.
		usedBytes.Store(950)
		_ = dm.MaybeCheck()

		// Recover.
		usedBytes.Store(100)
		_ = dm.MaybeCheck()
	}

	if got := recoveryCount.Load(); got != 3 {
		t.Errorf("recovery count = %d, want 3", got)
	}
}
