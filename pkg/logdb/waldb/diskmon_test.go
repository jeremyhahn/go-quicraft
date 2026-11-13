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
	"slices"
	"sync/atomic"
	"testing"
	"time"

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
	var fullUsed, fullMax uint64

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
				fullUsed = used
				fullMax = max
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
	if fullUsed != 960 || fullMax != 1000 {
		t.Fatalf("full callback got used=%d max=%d, want 960 and 1000", fullUsed, fullMax)
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

// TestDiskMonitorConcurrentRecoveryAfterStaleFullSample pins that a full
// sample taken before a recovery cannot publish its state after it: caller A
// samples full and stalls inside onFull, caller B samples below the threshold,
// and once both return the monitor and the flag its callbacks drive both say
// not full.
func TestDiskMonitorConcurrentRecoveryAfterStaleFullSample(t *testing.T) {
	var calls atomic.Uint64
	usage := func(string) (uint64, error) {
		if calls.Add(1) == 1 {
			return 950, nil
		}
		return 100, nil
	}

	var full atomic.Bool
	inFull := make(chan struct{})
	release := make(chan struct{})
	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithFullPercent(0.90),
		WithOnFull(func(_, _ uint64) {
			close(inFull)
			<-release
			full.Store(true)
		}),
		WithOnRecovery(func(_, _ uint64) {
			full.Store(false)
		}),
		WithDiskUsageFunc(usage),
	)

	aErr := make(chan error, 1)
	go func() { aErr <- dm.MaybeCheck() }()
	select {
	case <-inFull:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("caller A never reached onFull")
	}

	bErr := make(chan error, 1)
	go func() { bErr <- dm.MaybeCheck() }()
	// B may finish now (no serialization) or block behind A (serialized);
	// either way give it a bounded window before releasing A.
	var bDone bool
	var errB error
	select {
	case errB = <-bErr:
		bDone = true
	case <-time.After(200 * time.Millisecond):
	}
	close(release)

	if errA := <-aErr; !errors.Is(errA, ErrDiskFull) {
		t.Fatalf("caller A: got %v, want ErrDiskFull", errA)
	}
	if !bDone {
		errB = <-bErr
	}
	if errB != nil {
		t.Fatalf("caller B: got %v, want nil", errB)
	}
	if dm.Full() {
		t.Error("monitor still full after a later sample below the threshold")
	}
	if full.Load() {
		t.Error("flag driven by onFull/onRecovery still full after a later recovery")
	}
}

// TestDiskMonitorOnFullCallbackMayForceCheck pins finding 2 of the #911
// round 2 review: a WithOnFull callback that frees space and calls
// ForceCheck to verify recovery must not deadlock on the monitor's mutex,
// and the recovery it causes is delivered after the full callback returns.
func TestDiskMonitorOnFullCallbackMayForceCheck(t *testing.T) {
	var usage atomic.Uint64
	usage.Store(950)
	var events []string
	var dm *DiskMonitor
	innerErr := make(chan error, 1)
	dm = NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(1),
		WithFullPercent(0.90),
		WithOnFull(func(_, _ uint64) {
			events = append(events, "full")
			usage.Store(100)
			innerErr <- dm.ForceCheck()
			events = append(events, "full-returned")
		}),
		WithOnRecovery(func(_, _ uint64) {
			events = append(events, "recovery")
		}),
		WithDiskUsageFunc(func(string) (uint64, error) { return usage.Load(), nil }),
	)

	outerErr := make(chan error, 1)
	go func() { outerErr <- dm.ForceCheck() }()
	select {
	case err := <-outerErr:
		if !errors.Is(err, ErrDiskFull) {
			t.Fatalf("outer ForceCheck: got %v, want ErrDiskFull", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ForceCheck from inside WithOnFull deadlocked on the monitor mutex")
	}
	if err := <-innerErr; err != nil {
		t.Fatalf("inner ForceCheck: got %v, want nil", err)
	}
	if dm.Full() {
		t.Error("monitor still full after the callback freed space")
	}
	want := []string{"full", "full-returned", "recovery"}
	if !slices.Equal(events, want) {
		t.Errorf("callback order = %v, want %v", events, want)
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

// TestWithCheckEveryZeroDoesNotDivideByZero pins the sampling-interval guard.
//
// MaybeCheck samples with count%checkEvery, so an unguarded zero interval
// would panic the commit worker on the very first monitored write: waldb.Open
// would accept WithCheckEvery(0) without complaint and SaveState would then
// die inside the runtime with an integer divide by zero. A configuration typo must not be a
// runtime outage.
func TestWithCheckEveryZeroDoesNotDivideByZero(t *testing.T) {
	var usedBytes atomic.Uint64
	usedBytes.Store(10)

	dm := NewDiskMonitor("/tmp/test-wal", 1000,
		WithCheckEvery(0),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
	)
	if dm.checkEvery != DefaultCheckEvery {
		t.Fatalf("checkEvery = %d after WithCheckEvery(0), want the default %d",
			dm.checkEvery, DefaultCheckEvery)
	}
	for i := 0; i < 3; i++ {
		if err := dm.MaybeCheck(); err != nil {
			t.Fatalf("MaybeCheck: %v", err)
		}
	}

	// An option that zeroes the field by another route is normalized too, so
	// the divisor can never reach MaybeCheck at zero.
	bare := NewDiskMonitor("/tmp/test-wal", 1000,
		DiskMonitorOption(func(dm *DiskMonitor) { dm.checkEvery = 0 }),
		WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
	)
	if bare.checkEvery == 0 {
		t.Fatal("NewDiskMonitor left checkEvery at zero; the first MaybeCheck " +
			"divides by zero on the commit worker")
	}
	if err := bare.MaybeCheck(); err != nil {
		t.Fatalf("MaybeCheck on a normalized monitor: %v", err)
	}

	// The full path through the DB must survive it as well.
	dir := t.TempDir()
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithDiskMonitorOpts(1000,
			WithCheckEvery(0),
			WithDiskUsageFunc(mockDiskUsage(&usedBytes)),
		),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	if err := db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 1, Vote: 1, Commit: 0},
	}}); err != nil {
		t.Fatalf("SaveState with WithCheckEvery(0): %v", err)
	}
}
