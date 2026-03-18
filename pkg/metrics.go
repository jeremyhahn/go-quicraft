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

import "time"

// RaftMetrics covers proposal and consensus pipeline measurements.
type RaftMetrics interface {
	// ObserveProposalLatency records the end-to-end proposal latency
	// from Propose to Result for the given shard.
	ObserveProposalLatency(shardID uint64, d time.Duration)

	// ObserveCommitLatency records the time from proposal submission
	// to commit confirmation for the given shard.
	ObserveCommitLatency(shardID uint64, d time.Duration)

	// ObserveApplyLatency records the time from commit to state machine
	// apply for the given shard.
	ObserveApplyLatency(shardID uint64, d time.Duration)

	// ObserveReadIndexLatency records the end-to-end ReadIndex latency
	// from ReadIndex submission to read confirmation for the given shard.
	// This covers the full round-trip including heartbeat quorum and any
	// wait for the state machine to catch up to the committed index.
	ObserveReadIndexLatency(shardID uint64, d time.Duration)

	// IncProposalDropped increments the dropped proposal counter for
	// the given shard.
	IncProposalDropped(shardID uint64)

	// ObserveProposalBatchSize records the number of proposals batched
	// in a single Raft append for the given shard.
	ObserveProposalBatchSize(shardID uint64, size int)

	// ObserveCommitBatchSize records the number of entries committed
	// in a single batch.
	ObserveCommitBatchSize(size int)

	// IncElection increments the election counter for the given shard.
	IncElection(shardID uint64)

	// IncSnapshot increments the snapshot counter for the given shard.
	IncSnapshot(shardID uint64)

	// IncPiggybackCommit increments the piggyback commit counter for
	// the given shard. A piggyback commit occurs when the commit worker
	// detects newly committable entries during cycle 1 and applies them
	// in the same cycle, eliminating a second pipeline traversal.
	IncPiggybackCommit(shardID uint64)
}

// LogDBMetrics covers WAL write path measurements.
type LogDBMetrics interface {
	// ObserveWriteLatency records the latency of a WAL write operation.
	ObserveWriteLatency(d time.Duration)

	// ObserveFsyncLatency records the latency of a WAL fsync operation.
	ObserveFsyncLatency(d time.Duration)

	// ObserveCompactionLatency records the latency of a WAL compaction.
	ObserveCompactionLatency(d time.Duration)

	// IncWriteBytes increments the total bytes written to the WAL.
	IncWriteBytes(n int64)
}

// TransportMetrics covers inter-node communication measurements.
type TransportMetrics interface {
	// ObserveSendLatency records the latency of sending a message.
	ObserveSendLatency(d time.Duration)

	// IncMessagesSent increments the counter for messages sent from
	// shardID to the given target replica with the given message type.
	IncMessagesSent(shardID, to uint64, msgType uint64)

	// IncMessagesReceived increments the counter for messages received
	// by shardID from the given source replica with the given message type.
	IncMessagesReceived(shardID, from uint64, msgType uint64)

	// IncSnapshotsSent records a snapshot sent for the given shard,
	// including the byte count and duration of the transfer.
	IncSnapshotsSent(shardID uint64, bytes int64, d time.Duration)

	// IncSnapshotsReceived records a snapshot received for the given shard,
	// including the byte count and duration of the transfer.
	IncSnapshotsReceived(shardID uint64, bytes int64, d time.Duration)
}

// MetricsCollector is the full metrics interface passed to WithMetrics.
// It composes RaftMetrics, LogDBMetrics, and TransportMetrics. Internally,
// QuicRaft passes only the relevant sub-interface to each component.
//
// For best performance, implementations should pre-bind metrics per shard
// during RegisterShard and use cached metric references in the hot-path
// methods. See PrometheusMetricsCollector for a reference implementation.
type MetricsCollector interface {
	RaftMetrics
	LogDBMetrics
	TransportMetrics

	// RegisterShard is called when a shard is started on this host.
	// Implementations should pre-bind any per-shard metrics here to
	// avoid expensive label lookups in hot paths.
	RegisterShard(shardID uint64)

	// UnregisterShard is called when a shard is stopped on this host.
	// Implementations should clean up any pre-bound metrics for this shard.
	UnregisterShard(shardID uint64)
}

// NoOpMetricsCollector is a MetricsCollector where all methods are no-ops.
// Embed this struct and override only the methods you care about to avoid
// implementing the full interface.
type NoOpMetricsCollector struct{}

// Compile-time interface compliance check.
var _ MetricsCollector = NoOpMetricsCollector{}

// RaftMetrics no-ops.

// ObserveProposalLatency is a no-op.
func (NoOpMetricsCollector) ObserveProposalLatency(uint64, time.Duration) {}

// ObserveCommitLatency is a no-op.
func (NoOpMetricsCollector) ObserveCommitLatency(uint64, time.Duration) {}

// ObserveApplyLatency is a no-op.
func (NoOpMetricsCollector) ObserveApplyLatency(uint64, time.Duration) {}

// ObserveReadIndexLatency is a no-op.
func (NoOpMetricsCollector) ObserveReadIndexLatency(uint64, time.Duration) {}

// IncProposalDropped is a no-op.
func (NoOpMetricsCollector) IncProposalDropped(uint64) {}

// ObserveProposalBatchSize is a no-op.
func (NoOpMetricsCollector) ObserveProposalBatchSize(uint64, int) {}

// ObserveCommitBatchSize is a no-op.
func (NoOpMetricsCollector) ObserveCommitBatchSize(int) {}

// IncElection is a no-op.
func (NoOpMetricsCollector) IncElection(uint64) {}

// IncSnapshot is a no-op.
func (NoOpMetricsCollector) IncSnapshot(uint64) {}

// IncPiggybackCommit is a no-op.
func (NoOpMetricsCollector) IncPiggybackCommit(uint64) {}

// LogDBMetrics no-ops.

// ObserveWriteLatency is a no-op.
func (NoOpMetricsCollector) ObserveWriteLatency(time.Duration) {}

// ObserveFsyncLatency is a no-op.
func (NoOpMetricsCollector) ObserveFsyncLatency(time.Duration) {}

// ObserveCompactionLatency is a no-op.
func (NoOpMetricsCollector) ObserveCompactionLatency(time.Duration) {}

// IncWriteBytes is a no-op.
func (NoOpMetricsCollector) IncWriteBytes(int64) {}

// TransportMetrics no-ops.

// ObserveSendLatency is a no-op.
func (NoOpMetricsCollector) ObserveSendLatency(time.Duration) {}

// IncMessagesSent is a no-op.
func (NoOpMetricsCollector) IncMessagesSent(uint64, uint64, uint64) {}

// IncMessagesReceived is a no-op.
func (NoOpMetricsCollector) IncMessagesReceived(uint64, uint64, uint64) {}

// IncSnapshotsSent is a no-op.
func (NoOpMetricsCollector) IncSnapshotsSent(uint64, int64, time.Duration) {}

// IncSnapshotsReceived is a no-op.
func (NoOpMetricsCollector) IncSnapshotsReceived(uint64, int64, time.Duration) {}

// RegisterShard is a no-op.
func (NoOpMetricsCollector) RegisterShard(uint64) {}

// UnregisterShard is a no-op.
func (NoOpMetricsCollector) UnregisterShard(uint64) {}
