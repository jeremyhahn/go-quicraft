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
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Compile-time interface compliance
// ---------------------------------------------------------------------------

func TestNoOpMetricsCollector_ImplementsMetricsCollector(t *testing.T) {
	var _ MetricsCollector = NoOpMetricsCollector{}
}

func TestNoOpMetricsCollector_ImplementsRaftMetrics(t *testing.T) {
	var _ RaftMetrics = NoOpMetricsCollector{}
}

func TestNoOpMetricsCollector_ImplementsLogDBMetrics(t *testing.T) {
	var _ LogDBMetrics = NoOpMetricsCollector{}
}

func TestNoOpMetricsCollector_ImplementsTransportMetrics(t *testing.T) {
	var _ TransportMetrics = NoOpMetricsCollector{}
}

// ---------------------------------------------------------------------------
// RaftMetrics no-ops
// ---------------------------------------------------------------------------

func TestNoOpMetricsCollector_ObserveProposalLatency(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveProposalLatency(1, 100*time.Millisecond)
}

func TestNoOpMetricsCollector_ObserveCommitLatency(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveCommitLatency(1, 50*time.Millisecond)
}

func TestNoOpMetricsCollector_ObserveApplyLatency(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveApplyLatency(1, 10*time.Millisecond)
}

func TestNoOpMetricsCollector_ObserveReadIndexLatency(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveReadIndexLatency(1, 5*time.Millisecond)
}

func TestNoOpMetricsCollector_ObserveReadIndexLatency_ZeroDuration(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveReadIndexLatency(1, 0)
}

func TestNoOpMetricsCollector_IncProposalDropped(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.IncProposalDropped(1)
}

func TestNoOpMetricsCollector_ObserveProposalBatchSize(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveProposalBatchSize(1, 10)
}

func TestNoOpMetricsCollector_ObserveCommitBatchSize(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveCommitBatchSize(100)
}

func TestNoOpMetricsCollector_IncElection(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.IncElection(1)
}

func TestNoOpMetricsCollector_IncSnapshot(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.IncSnapshot(1)
}

func TestNoOpMetricsCollector_IncPiggybackCommit(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.IncPiggybackCommit(1)
}

// ---------------------------------------------------------------------------
// LogDBMetrics no-ops
// ---------------------------------------------------------------------------

func TestNoOpMetricsCollector_ObserveWriteLatency(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveWriteLatency(1 * time.Millisecond)
}

func TestNoOpMetricsCollector_ObserveFsyncLatency(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveFsyncLatency(5 * time.Millisecond)
}

func TestNoOpMetricsCollector_ObserveCompactionLatency(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveCompactionLatency(100 * time.Millisecond)
}

func TestNoOpMetricsCollector_IncWriteBytes(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.IncWriteBytes(4096)
}

// ---------------------------------------------------------------------------
// TransportMetrics no-ops
// ---------------------------------------------------------------------------

func TestNoOpMetricsCollector_ObserveSendLatency(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.ObserveSendLatency(2 * time.Millisecond)
}

func TestNoOpMetricsCollector_IncMessagesSent(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.IncMessagesSent(1, 2, 3)
}

func TestNoOpMetricsCollector_IncMessagesReceived(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.IncMessagesReceived(1, 2, 3)
}

func TestNoOpMetricsCollector_IncSnapshotsSent(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.IncSnapshotsSent(1, 1024*1024, 5*time.Second)
}

func TestNoOpMetricsCollector_IncSnapshotsReceived(t *testing.T) {
	c := NoOpMetricsCollector{}
	c.IncSnapshotsReceived(1, 2*1024*1024, 10*time.Second)
}

// ---------------------------------------------------------------------------
// Embedding pattern test
// ---------------------------------------------------------------------------

type customCollector struct {
	NoOpMetricsCollector
	proposalCount int
}

func (c *customCollector) IncProposalDropped(shardID uint64) {
	c.proposalCount++
}

func TestNoOpMetricsCollector_EmbeddingOverride(t *testing.T) {
	c := &customCollector{}

	// The overridden method should work.
	c.IncProposalDropped(1)
	c.IncProposalDropped(2)
	if c.proposalCount != 2 {
		t.Errorf("proposalCount = %d, want 2", c.proposalCount)
	}

	// The inherited no-op methods should not panic.
	c.ObserveProposalLatency(1, time.Second)
	c.ObserveWriteLatency(time.Millisecond)
	c.ObserveSendLatency(time.Microsecond)
}

func TestNoOpMetricsCollector_EmbeddingInvalid(t *testing.T) {
	// Verify that a struct embedding NoOpMetricsCollector satisfies
	// MetricsCollector without implementing any methods.
	var _ MetricsCollector = &customCollector{}
}

// ---------------------------------------------------------------------------
// RegisterShard / UnregisterShard no-ops
// ---------------------------------------------------------------------------

func TestNoOpMetricsCollector_RegisterShard(t *testing.T) {
	c := NoOpMetricsCollector{}
	// Should not panic with any shard ID.
	c.RegisterShard(0)
	c.RegisterShard(1)
	c.RegisterShard(18446744073709551615) // max uint64
}

func TestNoOpMetricsCollector_RegisterShard_MultipleTimes(t *testing.T) {
	c := NoOpMetricsCollector{}
	// Registering the same shard multiple times should be a no-op.
	for range 100 {
		c.RegisterShard(42)
	}
}

func TestNoOpMetricsCollector_UnregisterShard(t *testing.T) {
	c := NoOpMetricsCollector{}
	// Should not panic with any shard ID.
	c.UnregisterShard(0)
	c.UnregisterShard(1)
	c.UnregisterShard(18446744073709551615) // max uint64
}

func TestNoOpMetricsCollector_UnregisterShard_WithoutRegister(t *testing.T) {
	c := NoOpMetricsCollector{}
	// Unregistering a never-registered shard should be a no-op.
	c.UnregisterShard(999)
}

func TestNoOpMetricsCollector_RegisterUnregisterCycle(t *testing.T) {
	c := NoOpMetricsCollector{}
	// Full lifecycle: register, use metrics, unregister.
	c.RegisterShard(1)
	c.ObserveProposalLatency(1, time.Millisecond)
	c.IncElection(1)
	c.UnregisterShard(1)
	// Using metrics after unregister should still be no-op.
	c.ObserveProposalLatency(1, time.Millisecond)
}
