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
	"sync"
	"testing"
	"time"
)

// recordingCollector records which method was called, so a forward to the
// WRONG inner method is detectable rather than merely non-panicking.
type recordingCollector struct {
	NoOpMetricsCollector
	mu     sync.Mutex
	called []string
}

func (r *recordingCollector) note(op string) {
	r.mu.Lock()
	r.called = append(r.called, op)
	r.mu.Unlock()
}

func (r *recordingCollector) saw(op string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.called {
		if c == op {
			return true
		}
	}
	return false
}

func (r *recordingCollector) ObserveProposalLatency(uint64, time.Duration) {
	r.note("ObserveProposalLatency")
}
func (r *recordingCollector) ObserveCommitLatency(uint64, time.Duration) {
	r.note("ObserveCommitLatency")
}
func (r *recordingCollector) ObserveApplyLatency(uint64, time.Duration) {
	r.note("ObserveApplyLatency")
}
func (r *recordingCollector) ObserveReadIndexLatency(uint64, time.Duration) {
	r.note("ObserveReadIndexLatency")
}
func (r *recordingCollector) IncProposalDropped(uint64) { r.note("IncProposalDropped") }
func (r *recordingCollector) ObserveProposalBatchSize(uint64, int) {
	r.note("ObserveProposalBatchSize")
}
func (r *recordingCollector) ObserveCommitBatchSize(int) { r.note("ObserveCommitBatchSize") }
func (r *recordingCollector) IncElection(uint64)         { r.note("IncElection") }
func (r *recordingCollector) IncSnapshot(uint64)         { r.note("IncSnapshot") }
func (r *recordingCollector) IncPiggybackCommit(uint64)  { r.note("IncPiggybackCommit") }
func (r *recordingCollector) ObserveWriteLatency(time.Duration) {
	r.note("ObserveWriteLatency")
}
func (r *recordingCollector) ObserveFsyncLatency(time.Duration) {
	r.note("ObserveFsyncLatency")
}
func (r *recordingCollector) ObserveCompactionLatency(time.Duration) {
	r.note("ObserveCompactionLatency")
}
func (r *recordingCollector) IncWriteBytes(int64)              { r.note("IncWriteBytes") }
func (r *recordingCollector) ObserveSendLatency(time.Duration) { r.note("ObserveSendLatency") }
func (r *recordingCollector) IncMessagesSent(uint64, uint64, uint64) {
	r.note("IncMessagesSent")
}
func (r *recordingCollector) IncMessagesReceived(uint64, uint64, uint64) {
	r.note("IncMessagesReceived")
}
func (r *recordingCollector) IncSnapshotsSent(uint64, int64, time.Duration) {
	r.note("IncSnapshotsSent")
}
func (r *recordingCollector) IncSnapshotsReceived(uint64, int64, time.Duration) {
	r.note("IncSnapshotsReceived")
}
func (r *recordingCollector) IncSnapshotsRejected(uint64, string) {
	r.note("IncSnapshotsRejected")
}
func (r *recordingCollector) IncMessagesDropped(uint64, uint64, string) {
	r.note("IncMessagesDropped")
}
func (r *recordingCollector) IncUnauthorizedMessageDropped(uint64, uint64, string) {
	r.note("IncUnauthorizedMessageDropped")
}
func (r *recordingCollector) IncUnauthorizedSnapshotDropped(uint64, uint64) {
	r.note("IncUnauthorizedSnapshotDropped")
}
func (r *recordingCollector) RegisterShard(uint64)   { r.note("RegisterShard") }
func (r *recordingCollector) UnregisterShard(uint64) { r.note("UnregisterShard") }

// TestSafeMetricsCollector_ForwardsEveryMethod pins that each of the 25
// forwarding methods reaches its OWN inner method.
//
// The wrapper is 25 near-identical hand-written bodies, which is exactly the
// shape a copy-paste error survives in: forwarding ObserveCommitLatency to
// ObserveProposalLatency would keep compiling, keep the panic barrier intact,
// and silently mis-report two metrics forever. Recording the call by name is
// what makes that detectable.
func TestSafeMetricsCollector_ForwardsEveryMethod(t *testing.T) {
	rec := &recordingCollector{}
	c := newSafeMetricsCollector(rec)

	c.ObserveProposalLatency(1, time.Millisecond)
	c.ObserveCommitLatency(1, time.Millisecond)
	c.ObserveApplyLatency(1, time.Millisecond)
	c.ObserveReadIndexLatency(1, time.Millisecond)
	c.IncProposalDropped(1)
	c.ObserveProposalBatchSize(1, 10)
	c.ObserveCommitBatchSize(10)
	c.IncElection(1)
	c.IncSnapshot(1)
	c.IncPiggybackCommit(1)
	c.ObserveWriteLatency(time.Millisecond)
	c.ObserveFsyncLatency(time.Millisecond)
	c.ObserveCompactionLatency(time.Millisecond)
	c.IncWriteBytes(64)
	c.ObserveSendLatency(time.Millisecond)
	c.IncMessagesSent(1, 2, 3)
	c.IncMessagesReceived(1, 2, 3)
	c.IncSnapshotsSent(1, 100, time.Millisecond)
	c.IncSnapshotsReceived(1, 100, time.Millisecond)
	c.IncSnapshotsRejected(1, "reason")
	c.IncMessagesDropped(1, 2, "reason")
	c.IncUnauthorizedMessageDropped(1, 2, "reason")
	c.IncUnauthorizedSnapshotDropped(1, 2)
	c.RegisterShard(1)
	c.UnregisterShard(1)

	for _, op := range []string{
		"ObserveProposalLatency", "ObserveCommitLatency", "ObserveApplyLatency",
		"ObserveReadIndexLatency", "IncProposalDropped", "ObserveProposalBatchSize",
		"ObserveCommitBatchSize", "IncElection", "IncSnapshot", "IncPiggybackCommit",
		"ObserveWriteLatency", "ObserveFsyncLatency", "ObserveCompactionLatency",
		"IncWriteBytes", "ObserveSendLatency", "IncMessagesSent",
		"IncMessagesReceived", "IncSnapshotsSent", "IncSnapshotsReceived",
		"IncSnapshotsRejected", "IncMessagesDropped",
		"IncUnauthorizedMessageDropped", "IncUnauthorizedSnapshotDropped",
		"RegisterShard", "UnregisterShard",
	} {
		if !rec.saw(op) {
			t.Errorf("%s did not reach the wrapped collector: the wrapper "+
				"either dropped it or forwarded it to a different method", op)
		}
	}

	if safe, ok := c.(*safeMetricsCollector); ok && safe.PanicCount() != 0 {
		t.Errorf("PanicCount = %d after 25 well-behaved calls, want 0",
			safe.PanicCount())
	}
}
