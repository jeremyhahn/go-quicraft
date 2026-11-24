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

	// mu serializes check: the sample, the lastFull/lastWarning transition,
	// the onSample publication and the queueing of the sample's callbacks,
	// so the state last published is the one from the latest sample.
	// MaybeCheck reads lastFull without it.
	mu sync.Mutex

	// notifyMu guards pending and draining. The callbacks of each sample
	// are queued under mu, in sample order, and run by one draining caller
	// at a time with neither lock held, so a callback may sample this
	// monitor again: its own callbacks are queued and run after it returns.
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

// WithCheckEvery sets the number of writes between disk usage checks.
//
// Zero is rejected, not honored: MaybeCheck samples with count%checkEvery, so a
// zero interval divides by zero and panics the commit worker on the first
// monitored write — a configuration typo would take the node down at runtime
// rather than at setup. It is not read as "check every write" either; that is
// WithCheckEvery(1), and silently turning a typo into a statfs on every WAL
// write would be its own outage. Zero falls back to DefaultCheckEvery.
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

// WithOnFull sets the callback invoked when usage crosses the full
// threshold. The callback receives the current used and maxBytes bytes. It
// runs after the monitor's mutex is released, in sample order with the
// warning and recovery callbacks, so it may call back into this monitor
// (free space, then ForceCheck to verify recovery); the callbacks of a sample
// taken from inside it run after it returns.
func WithOnFull(fn func(used, maxBytes uint64)) DiskMonitorOption {
	return func(dm *DiskMonitor) {
		dm.onFull = fn
	}
}

// WithOnRecovery sets the callback invoked when usage drops below the full
// threshold after being in the full state. This enables clearing external
// disk-full flags when disk space is freed. The callback receives the
// current used and maxBytes bytes. It runs like WithOnFull's callback: after
// the mutex is released, in sample order.
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

// WithOnSample sets a hook for internal state publication. fn runs under the
// monitor's mutex after every successful sample, so the state it publishes
// is that of the monitor's latest sample; it must not call back into this
// monitor. The function it returns, if not nil, runs after the mutex is
// released, in sample order after the sample's other callbacks.
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
	// Defence in depth for the divisor MaybeCheck relies on: WithCheckEvery
	// already refuses zero, but any future option (or a hand-built monitor)
	// that leaves the field at its zero value must not turn the first
	// monitored write into a division by zero on the commit worker.
	if dm.checkEvery == 0 {
		dm.checkEvery = DefaultCheckEvery
	}
	if dm.sampleClock == nil {
		dm.sampleClock = &dm.ownClock
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

	// Once check() has tripped the full state, bypass the sample throttle and
	// re-check on EVERY call. Otherwise the next checkEvery-1 writes are
	// admitted UNCHECKED into an already-full disk: a follower's replication
	// writes reach here through DB.SaveState -> MaybeCheck (the host-level
	// diskFullFlag gates only client Propose/ReadIndex), so it would keep
	// accepting up-to-64MB batches until fdatasync hits hard ENOSPC and poisons
	// the DB. check() keeps returning ErrDiskFull until usage drops below the
	// threshold, and fires the recovery transition/callback on the way down —
	// after which lastFull is false and normal throttling resumes. The extra
	// syscall cost is confined to the already-degraded full state.
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

// drain runs the queued callbacks in order. One caller drains at a time; a
// caller that finds a drain in progress (another goroutine's, or its own
// further up the stack when a callback sampled this monitor) leaves its
// callbacks to that drain.
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
