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

package quicraft

import (
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// safeMetricsCollector wraps a caller-supplied MetricsCollector so a panic in
// it can never reach engine code.
//
// This is not defensive programming for its own sake. The engine calls the
// collector from INSIDE critical sections — notably between
// node.raftMu.Lock() and node.raftMu.Unlock() in the step and commit workers,
// where the unlock is a plain statement rather than a defer. A panic escaping
// a collector method there leaves raftMu held forever: the worker's
// recover-and-restart loop then blocks on it, and with it every shard that
// worker owns, and Host.Close, which waits on the same workers. One embedder
// bug becomes a permanent whole-partition deadlock with no diagnostic beyond a
// stuck process.
//
// MetricsCollector was the last embedder-implemented interface still called
// inline and unguarded; the state machine and the snapshot sender already have
// recover barriers of their own. The interface is deliberately not documented
// as "must not panic" — a library cannot enforce that, and a metrics backend
// that panics on a full buffer or a closed registry is an ordinary bug, not a
// contract violation.
//
// Cost is one deferred call per metric observation on paths that already do a
// map lookup and an atomic; measurable only in a microbenchmark of the wrapper
// itself. Correctness of the host outranks it.
type safeMetricsCollector struct {
	inner MetricsCollector

	// panics counts recovered panics so a broken collector is visible rather
	// than merely survived. Logging every one would let a collector that
	// panics on every call flood the log from a hot path.
	panics atomic.Uint64
}

// newSafeMetricsCollector wraps c. A nil collector, or one that is already
// wrapped, is returned unchanged so wrapping is idempotent and free for the
// no-op case.
func newSafeMetricsCollector(c MetricsCollector) MetricsCollector {
	if c == nil {
		return nil
	}
	switch c.(type) {
	case *safeMetricsCollector, NoOpMetricsCollector, *NoOpMetricsCollector:
		// NoOpMetricsCollector's methods are empty by construction and cannot
		// panic; wrapping it would add a defer to every hot-path metric call
		// for a guarantee that already holds.
		return c
	}
	return &safeMetricsCollector{inner: c}
}

// guard runs fn, converting any panic into a counted, rate-limited log line.
func (s *safeMetricsCollector) guard(op string, fn func()) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		n := s.panics.Add(1)
		// Log the first, then powers of two. A collector that panics on every
		// call is on the raft receive path; an unsampled line there is its own
		// denial of service.
		if n&(n-1) == 0 {
			slog.Error("quicraft: panic recovered in MetricsCollector; "+
				"metric discarded, consensus unaffected",
				"op", op,
				"recovered_panics", n,
				"panic", r,
				"stack", string(debug.Stack()),
			)
		}
	}()
	fn()
}

// PanicCount returns the number of panics recovered from the wrapped
// collector. Exposed for tests and for a health check that wants to surface a
// broken metrics backend.
func (s *safeMetricsCollector) PanicCount() uint64 { return s.panics.Load() }

// --- RaftMetrics ---

// ObserveProposalLatency forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) ObserveProposalLatency(shardID uint64, d time.Duration) {
	s.guard("ObserveProposalLatency", func() { s.inner.ObserveProposalLatency(shardID, d) })
}

// ObserveCommitLatency forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) ObserveCommitLatency(shardID uint64, d time.Duration) {
	s.guard("ObserveCommitLatency", func() { s.inner.ObserveCommitLatency(shardID, d) })
}

// ObserveApplyLatency forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) ObserveApplyLatency(shardID uint64, d time.Duration) {
	s.guard("ObserveApplyLatency", func() { s.inner.ObserveApplyLatency(shardID, d) })
}

// ObserveReadIndexLatency forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) ObserveReadIndexLatency(shardID uint64, d time.Duration) {
	s.guard("ObserveReadIndexLatency", func() { s.inner.ObserveReadIndexLatency(shardID, d) })
}

// IncProposalDropped forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncProposalDropped(shardID uint64) {
	s.guard("IncProposalDropped", func() { s.inner.IncProposalDropped(shardID) })
}

// ObserveProposalBatchSize forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) ObserveProposalBatchSize(shardID uint64, size int) {
	s.guard("ObserveProposalBatchSize", func() { s.inner.ObserveProposalBatchSize(shardID, size) })
}

// ObserveCommitBatchSize forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) ObserveCommitBatchSize(size int) {
	s.guard("ObserveCommitBatchSize", func() { s.inner.ObserveCommitBatchSize(size) })
}

// IncElection forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncElection(shardID uint64) {
	s.guard("IncElection", func() { s.inner.IncElection(shardID) })
}

// IncSnapshot forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncSnapshot(shardID uint64) {
	s.guard("IncSnapshot", func() { s.inner.IncSnapshot(shardID) })
}

// IncPiggybackCommit forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncPiggybackCommit(shardID uint64) {
	s.guard("IncPiggybackCommit", func() { s.inner.IncPiggybackCommit(shardID) })
}

// --- LogDBMetrics ---

// ObserveWriteLatency forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) ObserveWriteLatency(d time.Duration) {
	s.guard("ObserveWriteLatency", func() { s.inner.ObserveWriteLatency(d) })
}

// ObserveFsyncLatency forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) ObserveFsyncLatency(d time.Duration) {
	s.guard("ObserveFsyncLatency", func() { s.inner.ObserveFsyncLatency(d) })
}

// ObserveCompactionLatency forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) ObserveCompactionLatency(d time.Duration) {
	s.guard("ObserveCompactionLatency", func() { s.inner.ObserveCompactionLatency(d) })
}

// IncWriteBytes forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncWriteBytes(n int64) {
	s.guard("IncWriteBytes", func() { s.inner.IncWriteBytes(n) })
}

// --- TransportMetrics ---

// ObserveSendLatency forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) ObserveSendLatency(d time.Duration) {
	s.guard("ObserveSendLatency", func() { s.inner.ObserveSendLatency(d) })
}

// IncMessagesSent forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncMessagesSent(shardID, to uint64, msgType uint64) {
	s.guard("IncMessagesSent", func() { s.inner.IncMessagesSent(shardID, to, msgType) })
}

// IncMessagesReceived forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncMessagesReceived(shardID, from uint64, msgType uint64) {
	s.guard("IncMessagesReceived", func() { s.inner.IncMessagesReceived(shardID, from, msgType) })
}

// IncSnapshotsSent forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncSnapshotsSent(shardID uint64, bytes int64, d time.Duration) {
	s.guard("IncSnapshotsSent", func() { s.inner.IncSnapshotsSent(shardID, bytes, d) })
}

// IncSnapshotsReceived forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncSnapshotsReceived(shardID uint64, bytes int64, d time.Duration) {
	s.guard("IncSnapshotsReceived", func() { s.inner.IncSnapshotsReceived(shardID, bytes, d) })
}

// IncSnapshotsRejected forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncSnapshotsRejected(shardID uint64, reason string) {
	s.guard("IncSnapshotsRejected", func() { s.inner.IncSnapshotsRejected(shardID, reason) })
}

// IncMessagesDropped forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncMessagesDropped(shardID, to uint64, reason string) {
	s.guard("IncMessagesDropped", func() { s.inner.IncMessagesDropped(shardID, to, reason) })
}

// --- SecurityMetrics ---

// IncUnauthorizedMessageDropped forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncUnauthorizedMessageDropped(shardID, from uint64, reason string) {
	s.guard("IncUnauthorizedMessageDropped", func() {
		s.inner.IncUnauthorizedMessageDropped(shardID, from, reason)
	})
}

// IncUnauthorizedSnapshotDropped forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) IncUnauthorizedSnapshotDropped(shardID, from uint64) {
	s.guard("IncUnauthorizedSnapshotDropped", func() {
		s.inner.IncUnauthorizedSnapshotDropped(shardID, from)
	})
}

// --- lifecycle ---

// RegisterShard forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) RegisterShard(shardID uint64) {
	s.guard("RegisterShard", func() { s.inner.RegisterShard(shardID) })
}

// UnregisterShard forwards to the wrapped collector, containing any panic.
func (s *safeMetricsCollector) UnregisterShard(shardID uint64) {
	s.guard("UnregisterShard", func() { s.inner.UnregisterShard(shardID) })
}
