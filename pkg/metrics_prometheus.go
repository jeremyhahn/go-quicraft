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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Compile-time interface compliance check.
var _ MetricsCollector = (*PrometheusMetricsCollector)(nil)

// latencyBuckets defines histogram buckets for latency observations in seconds.
// Range: 100us to 10s covering microsecond-level fast-path through slow I/O.
var latencyBuckets = []float64{
	0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
}

// batchSizeBuckets defines histogram buckets for batch size observations.
// Powers of two from 1 to 1024, suitable for entry/proposal batch counts.
var batchSizeBuckets = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}

// snapshotBytesBuckets defines histogram buckets for snapshot byte counts.
// Range: 1KB to 1GB covering typical snapshot sizes.
var snapshotBytesBuckets = []float64{
	1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824,
}

// shardMetrics holds pre-bound Prometheus metrics for a single shard.
// These are created once during RegisterShard and reused for all subsequent
// metric operations, eliminating the expensive WithLabelValues lookup.
type shardMetrics struct {
	proposalLatency    prometheus.Observer
	commitLatency      prometheus.Observer
	applyLatency       prometheus.Observer
	readIndexLatency   prometheus.Observer
	proposalDropped    prometheus.Counter
	proposalBatchSize  prometheus.Observer
	elections          prometheus.Counter
	snapshots          prometheus.Counter
	piggybackCommits   prometheus.Counter
	snapshotsSent      prometheus.Counter
	snapshotsBytesSent prometheus.Observer
	snapshotsReceived  prometheus.Counter
	snapshotsBytesRecv prometheus.Observer
}

// PrometheusMetricsCollector implements MetricsCollector using Prometheus
// client_golang. All metrics are registered with a custom registry to avoid
// polluting the global default registry.
//
// This implementation uses pre-bound metrics per shard for maximum performance.
// Call RegisterShard when a shard starts and UnregisterShard when it stops.
// The hot-path metric methods use O(1) sync.Map lookup instead of expensive
// WithLabelValues calls that involve hashing and map operations.
//
// Transport message metrics (IncMessagesSent/IncMessagesReceived) use simple
// atomic counters by default for performance. Set EnablePerMessageMetrics=true
// for detailed per-shard/target/type breakdown (impacts performance).
type PrometheusMetricsCollector struct {
	// Registry is the custom Prometheus registry holding all metrics.
	// Expose this via promhttp.HandlerFor(collector.Registry, ...) to
	// serve metrics on an HTTP endpoint.
	Registry *prometheus.Registry

	// EnablePerMessageMetrics enables detailed per-shard/target/type
	// message metrics. This is expensive and should only be enabled
	// for debugging. Default: false (uses aggregate counters).
	EnablePerMessageMetrics bool

	// Pre-bound metrics per shard for O(1) hot-path access.
	shardMetrics sync.Map // map[uint64]*shardMetrics

	// Raft metric vectors (for registration and pre-binding).
	proposalLatencyVec   *prometheus.HistogramVec
	commitLatencyVec     *prometheus.HistogramVec
	applyLatencyVec      *prometheus.HistogramVec
	readIndexLatencyVec  *prometheus.HistogramVec
	proposalDroppedVec   *prometheus.CounterVec
	proposalBatchSizeVec *prometheus.HistogramVec
	commitBatchSize      prometheus.Histogram
	electionsVec         *prometheus.CounterVec
	snapshotsVec         *prometheus.CounterVec
	piggybackCommitsVec  *prometheus.CounterVec

	// LogDB metrics (not per-shard, no pre-binding needed).
	writeLatency      prometheus.Histogram
	fsyncLatency      prometheus.Histogram
	compactionLatency prometheus.Histogram
	writeBytes        prometheus.Counter

	// Transport metrics - aggregate counters for hot path.
	sendLatency           prometheus.Histogram
	messagesSentTotal     prometheus.Counter
	messagesReceivedTotal prometheus.Counter

	// Transport metrics - detailed (only used if EnablePerMessageMetrics).
	messagesSentVec     *prometheus.CounterVec
	messagesReceivedVec *prometheus.CounterVec

	// Snapshot metrics vectors (for pre-binding).
	snapshotsSentVec      *prometheus.CounterVec
	snapshotsBytesSentVec *prometheus.HistogramVec
	snapshotsReceivedVec  *prometheus.CounterVec
	snapshotsBytesRecvVec *prometheus.HistogramVec
}

// NewPrometheusMetricsCollector creates a PrometheusMetricsCollector with all
// metrics registered on a dedicated prometheus.Registry. The caller should
// expose the Registry via promhttp.HandlerFor for scraping.
//
// By default, per-message transport metrics are disabled for performance.
// Set EnablePerMessageMetrics=true after creation to enable detailed
// per-shard/target/type message metrics.
func NewPrometheusMetricsCollector() *PrometheusMetricsCollector {
	reg := prometheus.NewRegistry()

	c := &PrometheusMetricsCollector{
		Registry: reg,

		// --- Raft metrics ---
		proposalLatencyVec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "raft",
			Name:      "proposal_latency_seconds",
			Help:      "End-to-end proposal latency from Propose to Result.",
			Buckets:   latencyBuckets,
		}, []string{"shard_id"}),

		commitLatencyVec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "raft",
			Name:      "commit_latency_seconds",
			Help:      "Latency from proposal submission to commit confirmation.",
			Buckets:   latencyBuckets,
		}, []string{"shard_id"}),

		applyLatencyVec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "raft",
			Name:      "apply_latency_seconds",
			Help:      "Latency from commit to state machine apply.",
			Buckets:   latencyBuckets,
		}, []string{"shard_id"}),

		readIndexLatencyVec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "raft",
			Name:      "read_index_latency_seconds",
			Help:      "End-to-end ReadIndex latency from submission to read confirmation.",
			Buckets:   latencyBuckets,
		}, []string{"shard_id"}),

		proposalDroppedVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "raft",
			Name:      "proposals_dropped_total",
			Help:      "Total number of dropped proposals.",
		}, []string{"shard_id"}),

		proposalBatchSizeVec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "raft",
			Name:      "proposal_batch_size",
			Help:      "Number of proposals batched in a single Raft append.",
			Buckets:   batchSizeBuckets,
		}, []string{"shard_id"}),

		commitBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "raft",
			Name:      "commit_batch_size",
			Help:      "Number of entries committed in a single batch.",
			Buckets:   batchSizeBuckets,
		}),

		electionsVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "raft",
			Name:      "elections_total",
			Help:      "Total number of elections triggered.",
		}, []string{"shard_id"}),

		snapshotsVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "raft",
			Name:      "snapshots_total",
			Help:      "Total number of snapshots taken.",
		}, []string{"shard_id"}),

		piggybackCommitsVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "raft",
			Name:      "piggyback_commits_total",
			Help:      "Total number of piggyback commits that eliminated a second pipeline cycle.",
		}, []string{"shard_id"}),

		// --- LogDB metrics ---
		writeLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "logdb",
			Name:      "write_latency_seconds",
			Help:      "WAL write operation latency.",
			Buckets:   latencyBuckets,
		}),

		fsyncLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "logdb",
			Name:      "fsync_latency_seconds",
			Help:      "WAL fsync operation latency.",
			Buckets:   latencyBuckets,
		}),

		compactionLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "logdb",
			Name:      "compaction_latency_seconds",
			Help:      "WAL compaction latency.",
			Buckets:   latencyBuckets,
		}),

		writeBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "logdb",
			Name:      "write_bytes_total",
			Help:      "Total bytes written to the WAL.",
		}),

		// --- Transport metrics ---
		sendLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "transport",
			Name:      "send_latency_seconds",
			Help:      "Latency of sending a message.",
			Buckets:   latencyBuckets,
		}),

		// Aggregate counters for hot path (no labels = fast).
		messagesSentTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "transport",
			Name:      "messages_sent_total",
			Help:      "Total messages sent (aggregate).",
		}),

		messagesReceivedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "transport",
			Name:      "messages_received_total",
			Help:      "Total messages received (aggregate).",
		}),

		// Detailed per-message metrics (only used if EnablePerMessageMetrics).
		messagesSentVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "transport",
			Name:      "messages_sent_detailed_total",
			Help:      "Total messages sent per shard, target, and message type (detailed, optional).",
		}, []string{"shard_id", "to", "msg_type"}),

		messagesReceivedVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "transport",
			Name:      "messages_received_detailed_total",
			Help:      "Total messages received per shard, source, and message type (detailed, optional).",
		}, []string{"shard_id", "from", "msg_type"}),

		// Snapshot metrics.
		snapshotsSentVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "transport",
			Name:      "snapshots_sent_total",
			Help:      "Total snapshots sent per shard.",
		}, []string{"shard_id"}),

		snapshotsBytesSentVec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "transport",
			Name:      "snapshots_sent_bytes",
			Help:      "Snapshot bytes sent per transfer.",
			Buckets:   snapshotBytesBuckets,
		}, []string{"shard_id"}),

		snapshotsReceivedVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "quicraft",
			Subsystem: "transport",
			Name:      "snapshots_received_total",
			Help:      "Total snapshots received per shard.",
		}, []string{"shard_id"}),

		snapshotsBytesRecvVec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "quicraft",
			Subsystem: "transport",
			Name:      "snapshots_received_bytes",
			Help:      "Snapshot bytes received per transfer.",
			Buckets:   snapshotBytesBuckets,
		}, []string{"shard_id"}),
	}

	// Register all collectors with the custom registry.
	reg.MustRegister(
		// Raft
		c.proposalLatencyVec,
		c.commitLatencyVec,
		c.applyLatencyVec,
		c.readIndexLatencyVec,
		c.proposalDroppedVec,
		c.proposalBatchSizeVec,
		c.commitBatchSize,
		c.electionsVec,
		c.snapshotsVec,
		c.piggybackCommitsVec,
		// LogDB
		c.writeLatency,
		c.fsyncLatency,
		c.compactionLatency,
		c.writeBytes,
		// Transport
		c.sendLatency,
		c.messagesSentTotal,
		c.messagesReceivedTotal,
		c.messagesSentVec,
		c.messagesReceivedVec,
		c.snapshotsSentVec,
		c.snapshotsBytesSentVec,
		c.snapshotsReceivedVec,
		c.snapshotsBytesRecvVec,
	)

	return c
}

// ---------------------------------------------------------------------------
// Shard Registration (pre-binding for hot-path performance)
// ---------------------------------------------------------------------------

// RegisterShard pre-binds all per-shard metrics for the given shardID.
// This MUST be called when a shard starts to enable fast metric access.
// Without registration, metrics for the shard will fall back to slow
// WithLabelValues lookup.
func (c *PrometheusMetricsCollector) RegisterShard(shardID uint64) {
	sid := strconv.FormatUint(shardID, 10)

	sm := &shardMetrics{
		proposalLatency:    c.proposalLatencyVec.WithLabelValues(sid),
		commitLatency:      c.commitLatencyVec.WithLabelValues(sid),
		applyLatency:       c.applyLatencyVec.WithLabelValues(sid),
		readIndexLatency:   c.readIndexLatencyVec.WithLabelValues(sid),
		proposalDropped:    c.proposalDroppedVec.WithLabelValues(sid),
		proposalBatchSize:  c.proposalBatchSizeVec.WithLabelValues(sid),
		elections:          c.electionsVec.WithLabelValues(sid),
		snapshots:          c.snapshotsVec.WithLabelValues(sid),
		piggybackCommits:   c.piggybackCommitsVec.WithLabelValues(sid),
		snapshotsSent:      c.snapshotsSentVec.WithLabelValues(sid),
		snapshotsBytesSent: c.snapshotsBytesSentVec.WithLabelValues(sid),
		snapshotsReceived:  c.snapshotsReceivedVec.WithLabelValues(sid),
		snapshotsBytesRecv: c.snapshotsBytesRecvVec.WithLabelValues(sid),
	}

	c.shardMetrics.Store(shardID, sm)
}

// UnregisterShard removes the pre-bound metrics for the given shardID.
// Call this when a shard stops to allow garbage collection of the metrics.
func (c *PrometheusMetricsCollector) UnregisterShard(shardID uint64) {
	c.shardMetrics.Delete(shardID)
}

// getShard returns the pre-bound metrics for shardID, or nil if not registered.
func (c *PrometheusMetricsCollector) getShard(shardID uint64) *shardMetrics {
	if sm, ok := c.shardMetrics.Load(shardID); ok {
		return sm.(*shardMetrics)
	}
	return nil
}

// ---------------------------------------------------------------------------
// RaftMetrics
// ---------------------------------------------------------------------------

// ObserveProposalLatency records the end-to-end proposal latency for the
// given shard. Uses pre-bound metrics for O(1) access.
func (c *PrometheusMetricsCollector) ObserveProposalLatency(shardID uint64, d time.Duration) {
	if sm := c.getShard(shardID); sm != nil {
		sm.proposalLatency.Observe(d.Seconds())
		return
	}
	// Fallback for unregistered shards (slow path).
	c.proposalLatencyVec.WithLabelValues(strconv.FormatUint(shardID, 10)).Observe(d.Seconds())
}

// ObserveCommitLatency records the commit latency for the given shard.
func (c *PrometheusMetricsCollector) ObserveCommitLatency(shardID uint64, d time.Duration) {
	if sm := c.getShard(shardID); sm != nil {
		sm.commitLatency.Observe(d.Seconds())
		return
	}
	c.commitLatencyVec.WithLabelValues(strconv.FormatUint(shardID, 10)).Observe(d.Seconds())
}

// ObserveApplyLatency records the apply latency for the given shard.
func (c *PrometheusMetricsCollector) ObserveApplyLatency(shardID uint64, d time.Duration) {
	if sm := c.getShard(shardID); sm != nil {
		sm.applyLatency.Observe(d.Seconds())
		return
	}
	c.applyLatencyVec.WithLabelValues(strconv.FormatUint(shardID, 10)).Observe(d.Seconds())
}

// ObserveReadIndexLatency records the end-to-end ReadIndex latency for the
// given shard.
func (c *PrometheusMetricsCollector) ObserveReadIndexLatency(shardID uint64, d time.Duration) {
	if sm := c.getShard(shardID); sm != nil {
		sm.readIndexLatency.Observe(d.Seconds())
		return
	}
	c.readIndexLatencyVec.WithLabelValues(strconv.FormatUint(shardID, 10)).Observe(d.Seconds())
}

// IncProposalDropped increments the dropped proposal counter for the given
// shard.
func (c *PrometheusMetricsCollector) IncProposalDropped(shardID uint64) {
	if sm := c.getShard(shardID); sm != nil {
		sm.proposalDropped.Inc()
		return
	}
	c.proposalDroppedVec.WithLabelValues(strconv.FormatUint(shardID, 10)).Inc()
}

// ObserveProposalBatchSize records the number of proposals batched in a
// single Raft append for the given shard.
func (c *PrometheusMetricsCollector) ObserveProposalBatchSize(shardID uint64, size int) {
	if sm := c.getShard(shardID); sm != nil {
		sm.proposalBatchSize.Observe(float64(size))
		return
	}
	c.proposalBatchSizeVec.WithLabelValues(strconv.FormatUint(shardID, 10)).Observe(float64(size))
}

// ObserveCommitBatchSize records the number of entries committed in a single
// batch.
func (c *PrometheusMetricsCollector) ObserveCommitBatchSize(size int) {
	c.commitBatchSize.Observe(float64(size))
}

// IncElection increments the election counter for the given shard.
func (c *PrometheusMetricsCollector) IncElection(shardID uint64) {
	if sm := c.getShard(shardID); sm != nil {
		sm.elections.Inc()
		return
	}
	c.electionsVec.WithLabelValues(strconv.FormatUint(shardID, 10)).Inc()
}

// IncSnapshot increments the snapshot counter for the given shard.
func (c *PrometheusMetricsCollector) IncSnapshot(shardID uint64) {
	if sm := c.getShard(shardID); sm != nil {
		sm.snapshots.Inc()
		return
	}
	c.snapshotsVec.WithLabelValues(strconv.FormatUint(shardID, 10)).Inc()
}

// IncPiggybackCommit increments the piggyback commit counter for the given shard.
func (c *PrometheusMetricsCollector) IncPiggybackCommit(shardID uint64) {
	if sm := c.getShard(shardID); sm != nil {
		sm.piggybackCommits.Inc()
		return
	}
	c.piggybackCommitsVec.WithLabelValues(strconv.FormatUint(shardID, 10)).Inc()
}

// ---------------------------------------------------------------------------
// LogDBMetrics
// ---------------------------------------------------------------------------

// ObserveWriteLatency records the latency of a WAL write operation.
func (c *PrometheusMetricsCollector) ObserveWriteLatency(d time.Duration) {
	c.writeLatency.Observe(d.Seconds())
}

// ObserveFsyncLatency records the latency of a WAL fsync operation.
func (c *PrometheusMetricsCollector) ObserveFsyncLatency(d time.Duration) {
	c.fsyncLatency.Observe(d.Seconds())
}

// ObserveCompactionLatency records the latency of a WAL compaction.
func (c *PrometheusMetricsCollector) ObserveCompactionLatency(d time.Duration) {
	c.compactionLatency.Observe(d.Seconds())
}

// IncWriteBytes increments the total bytes written to the WAL.
func (c *PrometheusMetricsCollector) IncWriteBytes(n int64) {
	c.writeBytes.Add(float64(n))
}

// ---------------------------------------------------------------------------
// TransportMetrics
// ---------------------------------------------------------------------------

// ObserveSendLatency records the latency of sending a message.
func (c *PrometheusMetricsCollector) ObserveSendLatency(d time.Duration) {
	c.sendLatency.Observe(d.Seconds())
}

// IncMessagesSent increments the counter for messages sent.
// By default, this increments an aggregate counter for performance.
// If EnablePerMessageMetrics is true, it also updates the detailed
// per-shard/target/type counter (slower).
func (c *PrometheusMetricsCollector) IncMessagesSent(shardID, to uint64, msgType uint64) {
	c.messagesSentTotal.Inc()

	if c.EnablePerMessageMetrics {
		c.messagesSentVec.WithLabelValues(
			strconv.FormatUint(shardID, 10),
			strconv.FormatUint(to, 10),
			strconv.FormatUint(msgType, 10),
		).Inc()
	}
}

// IncMessagesReceived increments the counter for messages received.
// By default, this increments an aggregate counter for performance.
// If EnablePerMessageMetrics is true, it also updates the detailed
// per-shard/source/type counter (slower).
func (c *PrometheusMetricsCollector) IncMessagesReceived(shardID, from uint64, msgType uint64) {
	c.messagesReceivedTotal.Inc()

	if c.EnablePerMessageMetrics {
		c.messagesReceivedVec.WithLabelValues(
			strconv.FormatUint(shardID, 10),
			strconv.FormatUint(from, 10),
			strconv.FormatUint(msgType, 10),
		).Inc()
	}
}

// IncSnapshotsSent records a snapshot sent for the given shard, including the
// byte count and duration of the transfer.
func (c *PrometheusMetricsCollector) IncSnapshotsSent(shardID uint64, bytes int64, _ time.Duration) {
	if sm := c.getShard(shardID); sm != nil {
		sm.snapshotsSent.Inc()
		sm.snapshotsBytesSent.Observe(float64(bytes))
		return
	}
	shard := strconv.FormatUint(shardID, 10)
	c.snapshotsSentVec.WithLabelValues(shard).Inc()
	c.snapshotsBytesSentVec.WithLabelValues(shard).Observe(float64(bytes))
}

// IncSnapshotsReceived records a snapshot received for the given shard,
// including the byte count and duration of the transfer.
func (c *PrometheusMetricsCollector) IncSnapshotsReceived(shardID uint64, bytes int64, _ time.Duration) {
	if sm := c.getShard(shardID); sm != nil {
		sm.snapshotsReceived.Inc()
		sm.snapshotsBytesRecv.Observe(float64(bytes))
		return
	}
	shard := strconv.FormatUint(shardID, 10)
	c.snapshotsReceivedVec.WithLabelValues(shard).Inc()
	c.snapshotsBytesRecvVec.WithLabelValues(shard).Observe(float64(bytes))
}

// ---------------------------------------------------------------------------
// SampledMetricsCollector - Ultra-high-performance variant
// ---------------------------------------------------------------------------

// SampledMetricsCollector wraps a MetricsCollector and samples expensive
// metrics to reduce overhead in ultra-high-throughput scenarios.
// Hot-path metrics (commit latency, apply latency, message counts) are
// sampled at the configured rate. Less frequent metrics (elections,
// snapshots) are always recorded.
type SampledMetricsCollector struct {
	MetricsCollector

	// SampleRate is the inverse sampling rate. For example, 100 means
	// record 1 in 100 observations. Default: 100.
	SampleRate uint64

	// counter tracks observations for sampling decisions.
	counter atomic.Uint64
}

// NewSampledMetricsCollector wraps an existing collector with sampling.
// sampleRate of 100 means record 1 in 100 observations.
func NewSampledMetricsCollector(inner MetricsCollector, sampleRate uint64) *SampledMetricsCollector {
	if sampleRate == 0 {
		sampleRate = 100
	}
	return &SampledMetricsCollector{
		MetricsCollector: inner,
		SampleRate:       sampleRate,
	}
}

func (s *SampledMetricsCollector) shouldSample() bool {
	return s.counter.Add(1)%s.SampleRate == 0
}

// ObserveCommitLatency samples commit latency observations.
func (s *SampledMetricsCollector) ObserveCommitLatency(shardID uint64, d time.Duration) {
	if s.shouldSample() {
		s.MetricsCollector.ObserveCommitLatency(shardID, d)
	}
}

// ObserveApplyLatency samples apply latency observations.
func (s *SampledMetricsCollector) ObserveApplyLatency(shardID uint64, d time.Duration) {
	if s.shouldSample() {
		s.MetricsCollector.ObserveApplyLatency(shardID, d)
	}
}

// IncMessagesSent samples message send counts.
func (s *SampledMetricsCollector) IncMessagesSent(shardID, to uint64, msgType uint64) {
	if s.shouldSample() {
		s.MetricsCollector.IncMessagesSent(shardID, to, msgType)
	}
}

// IncMessagesReceived samples message receive counts.
func (s *SampledMetricsCollector) IncMessagesReceived(shardID, from uint64, msgType uint64) {
	if s.shouldSample() {
		s.MetricsCollector.IncMessagesReceived(shardID, from, msgType)
	}
}
