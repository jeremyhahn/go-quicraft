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
	"sync"
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

// DiskMonitor checks disk usage every N writes and fires callbacks when thresholds are crossed.
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

	// mu serializes check, so the published state is the latest sample's.
	mu sync.Mutex

	// notifyMu guards pending and draining; callbacks run in sample order with neither lock held.
	notifyMu sync.Mutex
	pending  []func()
	draining bool

	// sampleClock is the source of DiskSample.Seq; ownClock when not set.
	sampleClock *atomic.Uint64
	ownClock    atomic.Uint64

	onWarning  func(used, maxBytes uint64)
	onFull     func(used, maxBytes uint64)
	onSample   func(s DiskSample) func()
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

// WithCheckEvery sets the writes between disk usage checks; zero is rejected.
func WithCheckEvery(n uint64) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		if n == 0 {
			slog.Warn("waldb: WithCheckEvery(0) is not a valid sampling interval; "+
				"using the default",
				"check_every", DefaultCheckEvery)
			n = DefaultCheckEvery
		}
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

// WithOnFull sets the callback run, with used and maxBytes, when usage crosses the full threshold.
func WithOnFull(fn func(used, maxBytes uint64)) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.onFull = fn
	}
}

// WithOnRecovery sets the callback run when usage drops back below the full threshold.
func WithOnRecovery(fn func(used, maxBytes uint64)) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.onRecovery = fn
	}
}

// DiskSample is one successful disk usage sample, as WithOnSample sees it.
type DiskSample struct {
	// Seq orders samples: it is drawn from the WithSampleClock clock (or the
	// monitor's own) when the sample starts, before usage is read.
	Seq uint64
	// Full reports whether the sample is at or above the full threshold.
	Full bool
	// Used is the disk usage the sample read, in bytes.
	Used uint64
	// MaxBytes is the configured ceiling the sample compared Used against,
	// in bytes.
	MaxBytes uint64
}

// WithOnSample sets a hook run under the monitor's mutex after every sample; it must not call back into the monitor.
func WithOnSample(fn func(s DiskSample) func()) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.onSample = fn
	}
}

// WithSampleClock makes the monitor draw DiskSample.Seq from clock, so the
// samples of several monitors that share it are ordered against each other.
func WithSampleClock(clock *atomic.Uint64) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.sampleClock = clock
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
	// A zero checkEvery falls back to DefaultCheckEvery, so MaybeCheck never divides by zero.
	if dm.checkEvery == 0 {
		dm.checkEvery = DefaultCheckEvery
	}
	if dm.sampleClock == nil {
		dm.sampleClock = &dm.ownClock
	}
	return dm
}

// MaybeCheck counts a write and checks usage at the configured interval, returning ErrDiskFull above the full threshold.
func (dm *DiskMonitor) MaybeCheck() error {
	if dm == nil || dm.maxBytes == 0 {
		return nil
	}

	count := dm.writeCount.Add(1)

	// Once full, check on every call until usage recovers.
	if count%dm.checkEvery != 0 && !dm.lastFull.Load() {
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
	// The callbacks run after mu is released: they may reach user code that
	// samples this monitor again, which would deadlock on mu.
	err := dm.checkLocked()
	dm.drain()
	return err
}

// drain runs queued callbacks in order, one drainer at a time.
func (dm *DiskMonitor) drain() {
	dm.notifyMu.Lock()
	if dm.draining {
		dm.notifyMu.Unlock()
		return
	}
	dm.draining = true
	for len(dm.pending) > 0 {
		fn := dm.pending[0]
		dm.pending[0] = nil
		dm.pending = dm.pending[1:]
		dm.notifyMu.Unlock()
		dm.runCallback(fn)
		dm.notifyMu.Lock()
	}
	dm.pending = nil
	dm.draining = false
	dm.notifyMu.Unlock()
}

// runCallback runs fn and, should it panic, clears the draining mark so the
// monitor's later callbacks are not silently dropped.
func (dm *DiskMonitor) runCallback(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			dm.notifyMu.Lock()
			dm.draining = false
			dm.notifyMu.Unlock()
			panic(r)
		}
	}()
	fn()
}

// enqueue queues the callbacks of one sample. It is called under mu, so the
// queue is in sample order.
func (dm *DiskMonitor) enqueue(fns ...func()) {
	dm.notifyMu.Lock()
	for _, fn := range fns {
		if fn != nil {
			dm.pending = append(dm.pending, fn)
		}
	}
	dm.notifyMu.Unlock()
}

// checkLocked samples usage and applies the transition under mu. It returns
// the notification to run once mu is released, or nil.
func (dm *DiskMonitor) checkLocked() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// The sample's position is taken when it starts, so a sample that stalls
	// between reading usage and publishing it is older than one that starts
	// during the stall.
	seq := dm.sampleClock.Add(1)
	used, err := dm.diskUsage(dm.dir)
	if err != nil {
		// If we cannot determine disk usage, do not block writes.
		// The warning/full state remains unchanged. Log for debugging.
		slog.Debug("disk usage check failed, continuing without blocking",
			"dir", dm.dir,
			"error", err)
		return nil
	}

	maxBytes := dm.maxBytes
	warningThreshold := uint64(float64(maxBytes) * dm.warningPct)
	fullThreshold := uint64(float64(maxBytes) * dm.fullPct)
	full := used >= fullThreshold

	var onFull, onRecovery, onWarning func()
	if full {
		// Transition to full state: fire the callback once. Full is a
		// superset of warning.
		if !dm.lastFull.Swap(true) && dm.onFull != nil {
			onFull = func() { dm.onFull(used, maxBytes) }
		}
		dm.lastWarning.Store(true)
	} else {
		// Fire the recovery callback on the transition from full to
		// non-full.
		if dm.lastFull.Swap(false) && dm.onRecovery != nil {
			onRecovery = func() { dm.onRecovery(used, maxBytes) }
		}
		if used >= warningThreshold {
			// Transition to warning state: fire the callback once.
			if !dm.lastWarning.Swap(true) && dm.onWarning != nil {
				onWarning = func() { dm.onWarning(used, maxBytes) }
			}
		} else {
			// Below both thresholds: clear warning state.
			dm.lastWarning.Store(false)
		}
	}

	var published func()
	if dm.onSample != nil {
		published = dm.onSample(DiskSample{Seq: seq, Full: full, Used: used, MaxBytes: maxBytes})
	}
	dm.enqueue(onFull, onRecovery, onWarning, published)
	if full {
		return ErrDiskFull
	}
	return nil
}

// Full reports whether the latest sample was at or above the full
// threshold, that is whether MaybeCheck is refusing. It is false for a nil
// or disabled monitor.
func (dm *DiskMonitor) Full() bool {
	return dm != nil && dm.lastFull.Load()
}

// WriteCount returns the current write count. Useful for testing.
func (dm *DiskMonitor) WriteCount() uint64 {
	return dm.writeCount.Load()
}
