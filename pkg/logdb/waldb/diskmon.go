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
	"log/slog"
	"sync/atomic"
)

// Default disk monitor thresholds and check frequency.
const (
	// DefaultWarningPercent is the disk usage percentage at which the
	// warning callback fires.
	DefaultWarningPercent = 0.85

	// DefaultFullPercent is the disk usage percentage at which the full
	// callback fires and writes are rejected.
	DefaultFullPercent = 0.95

	// DefaultCheckEvery is the number of writes between disk usage
	// checks. Higher values reduce syscall overhead at the cost of
	// slower reaction to disk pressure.
	DefaultCheckEvery = 100
)

// DiskUsageFunc returns the number of bytes used in the given directory.
// This is injectable for testing without requiring actual disk operations.
type DiskUsageFunc func(dir string) (usedBytes uint64, err error)

// DiskMonitor periodically checks disk usage and fires callbacks when
// configured thresholds are exceeded. Checks are throttled to every N
// writes to avoid syscall overhead on every WAL write.
//
// Thread safety: all fields are either immutable after construction or
// use atomic operations. MaybeCheck is safe to call from multiple
// goroutines concurrently.
type DiskMonitor struct {
	dir        string
	maxBytes   uint64
	warningPct float64
	fullPct    float64
	checkEvery uint64

	writeCount atomic.Uint64

	// lastWarning and lastFull track the last emitted state to prevent
	// callback spam. A callback only fires on state transition (e.g.,
	// crossing from below to above threshold).
	lastWarning atomic.Bool
	lastFull    atomic.Bool

	onWarning func(used, maxBytes uint64)
	onFull    func(used, maxBytes uint64)

	onRecovery func(used, maxBytes uint64)

	// diskUsage is the function used to query disk usage. Defaults to
	// the platform-specific statfs implementation. Injectable for tests.
	diskUsage DiskUsageFunc
}

// DiskMonitorOption configures a DiskMonitor via the functional options
// pattern.
type DiskMonitorOption func(*DiskMonitor)

// WithWarningPercent sets the warning threshold as a fraction (0.0-1.0).
func WithWarningPercent(pct float64) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.warningPct = pct
	}
}

// WithFullPercent sets the full threshold as a fraction (0.0-1.0).
func WithFullPercent(pct float64) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.fullPct = pct
	}
}

// WithCheckEvery sets the number of writes between disk usage checks.
func WithCheckEvery(n uint64) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.checkEvery = n
	}
}

// WithOnWarning sets the callback invoked when usage crosses the
// warning threshold. The callback receives the current used and maxBytes
// bytes.
func WithOnWarning(fn func(used, maxBytes uint64)) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.onWarning = fn
	}
}

// WithOnFull sets the callback invoked when usage crosses the full
// threshold. The callback receives the current used and maxBytes bytes.
func WithOnFull(fn func(used, maxBytes uint64)) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.onFull = fn
	}
}

// WithOnRecovery sets the callback invoked when usage drops below the full
// threshold after being in the full state. This enables clearing external
// disk-full flags when disk space is freed. The callback receives the
// current used and maxBytes bytes.
func WithOnRecovery(fn func(used, maxBytes uint64)) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.onRecovery = fn
	}
}

// WithDiskUsageFunc overrides the platform disk usage function. This is
// primarily intended for testing.
func WithDiskUsageFunc(fn DiskUsageFunc) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.diskUsage = fn
	}
}

// NewDiskMonitor creates a DiskMonitor for the given WAL directory.
// If maxBytes is 0, the monitor is effectively disabled and MaybeCheck
// always returns nil.
func NewDiskMonitor(dir string, maxBytes uint64, opts ...DiskMonitorOption) *DiskMonitor {
	dm := &DiskMonitor{
		dir:        dir,
		maxBytes:   maxBytes,
		warningPct: DefaultWarningPercent,
		fullPct:    DefaultFullPercent,
		checkEvery: DefaultCheckEvery,
		diskUsage:  platformDiskUsage,
	}
	for _, opt := range opts {
		opt(dm)
	}
	return dm
}

// MaybeCheck increments the write counter and checks disk usage when
// the counter reaches the configured interval. Returns ErrDiskFull if
// the disk usage exceeds the full threshold. Returns nil otherwise.
//
// When maxBytes is 0 (unlimited), MaybeCheck always returns nil without
// any syscall overhead.
func (dm *DiskMonitor) MaybeCheck() error {
	if dm == nil || dm.maxBytes == 0 {
		return nil
	}

	count := dm.writeCount.Add(1)
	if count%dm.checkEvery != 0 {
		return nil
	}

	return dm.check()
}

// ForceCheck performs an immediate disk usage check regardless of the
// throttle counter. This is useful during initialization or after
// compaction to get an accurate reading.
func (dm *DiskMonitor) ForceCheck() error {
	if dm == nil || dm.maxBytes == 0 {
		return nil
	}
	return dm.check()
}

// check performs the actual disk usage query and fires callbacks as
// needed based on threshold crossings.
func (dm *DiskMonitor) check() error {
	used, err := dm.diskUsage(dm.dir)
	if err != nil {
		// If we cannot determine disk usage, do not block writes.
		// The warning/full state remains unchanged. Log for debugging.
		slog.Debug("disk usage check failed, continuing without blocking",
			"dir", dm.dir,
			"error", err)
		return nil
	}

	warningThreshold := uint64(float64(dm.maxBytes) * dm.warningPct)
	fullThreshold := uint64(float64(dm.maxBytes) * dm.fullPct)

	// Evaluate full threshold first (superset of warning).
	if used >= fullThreshold {
		// Transition to full state.
		if !dm.lastFull.Swap(true) {
			// State change: fire callback once.
			if dm.onFull != nil {
				dm.onFull(used, dm.maxBytes)
			}
		}
		// Also ensure warning is set when full.
		dm.lastWarning.Store(true)
		return ErrDiskFull
	}

	// Clear full state if we dropped below the full threshold.
	// Fire the recovery callback on the transition from full to non-full.
	if dm.lastFull.Swap(false) {
		if dm.onRecovery != nil {
			dm.onRecovery(used, dm.maxBytes)
		}
	}

	if used >= warningThreshold {
		// Transition to warning state.
		if !dm.lastWarning.Swap(true) {
			// State change: fire callback once.
			if dm.onWarning != nil {
				dm.onWarning(used, dm.maxBytes)
			}
		}
		return nil
	}

	// Below both thresholds: clear warning state.
	dm.lastWarning.Store(false)
	return nil
}

// WriteCount returns the current write count. Useful for testing.
func (dm *DiskMonitor) WriteCount() uint64 {
	return dm.writeCount.Load()
}
