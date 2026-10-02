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

	// IncSnapshotsRejected counts an inbound snapshot transfer discarded
	// before it reached the state machine. reason comes from a closed set
	// defined by the transport: "shutdown", "concurrency_limit",
	// "memory_budget", "liveness", "malformed", "io_error", "policy",
	// "unauthorized", "not_accepting".
	// Implementations must treat it as a metric label and must not derive
	// new label values from it. shardID is 0 for a transfer rejected before
	// its sender was authorized: until then the shard the header names is
	// whatever the peer wrote, and is not used as a label.
	IncSnapshotsRejected(shardID uint64, reason string)

	// IncMessagesDropped counts a Raft message the transport discarded
	// instead of delivering. reason comes from a closed set defined by the
	// transport: "unresolved_target", "queue_full", "stream_unavailable",
	// "write_failed", "marshal_failed", "protocol_violation", "unsendable",
	// "peer_rejected", "identity_mismatch". Implementations must treat it as a metric label and
	// must not derive new label values from it. shardID and to are both 0
	// for "protocol_violation", where the frame is discarded before its
	// messages are parsed.
	IncMessagesDropped(shardID, to uint64, reason string)
}

// SecurityMetrics covers security-relevant drop counters. These events
// indicate either a misconfigured cluster or an active injection attempt
// by an authenticated-but-compromised peer; they are counted (not just
// logged) so operators can alert on them.
type SecurityMetrics interface {
	// IncUnauthorizedMessageDropped counts inbound Raft messages dropped
	// because the sender failed shard-membership authorization.
	//
	// reason comes from a closed set defined by the message handler:
	// "removed_replica", "removed_identity", "misrouted", "unknown_member",
	// "identity_mismatch", "missing_identity". The split is
	// operational, not cosmetic — one aggregate counter cannot tell a rolling
	// membership change from an attack:
	//
	//   - "unknown_member" is the benign config-change window (a voter added
	//     by a change this replica has not applied yet). It rises and falls on
	//     its own.
	//   - "identity_mismatch" is the strongest signal here. Committed
	//     membership records a node identity for the claimed replica and the
	//     connection presented a different one, so the sender does not hold
	//     that replica's private key. It is a forged From or a stale identity
	//     after a key rotation; it is never routine.
	//   - "missing_identity" means the shard records no identity for a
	//     replica the configuration names. Admission requires one on every
	//     host with a transport, so this is a configuration written by a
	//     build that did not; the remedy is wipe and re-bootstrap.
	//   - "address_mismatch" and "shared_address" are retired label values
	//     and are not reallocated.
	IncUnauthorizedMessageDropped(shardID, from uint64, reason string)

	// IncUnauthorizedSnapshotDropped counts inbound snapshots dropped
	// because the sender failed authorization or TLS identity binding.
	// shardID is 0 when the snapshot named a shard this host has not loaded:
	// that ID is unverified and is not used as a label.
	IncUnauthorizedSnapshotDropped(shardID, from uint64)
}

// MetricsCollector is the full metrics interface passed to WithMetrics.
// It composes RaftMetrics, LogDBMetrics, TransportMetrics, and
// SecurityMetrics. Internally, QuicRaft passes only the relevant
// sub-interface to each component.
//
// For best performance, implementations should pre-bind metrics per shard
// during RegisterShard and use cached metric references in the hot-path
// methods. See PrometheusMetricsCollector for a reference implementation.
type MetricsCollector interface {
	RaftMetrics
	LogDBMetrics
	TransportMetrics
	SecurityMetrics

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

// IncSnapshotsRejected is a no-op.
func (NoOpMetricsCollector) IncSnapshotsRejected(uint64, string) {}

// IncMessagesDropped is a no-op.
func (NoOpMetricsCollector) IncMessagesDropped(uint64, uint64, string) {}

// SecurityMetrics no-ops.

// IncUnauthorizedMessageDropped is a no-op.
func (NoOpMetricsCollector) IncUnauthorizedMessageDropped(uint64, uint64, string) {}

// IncUnauthorizedSnapshotDropped is a no-op.
func (NoOpMetricsCollector) IncUnauthorizedSnapshotDropped(uint64, uint64) {}

// RegisterShard is a no-op.
func (NoOpMetricsCollector) RegisterShard(uint64) {}

// UnregisterShard is a no-op.
func (NoOpMetricsCollector) UnregisterShard(uint64) {}
