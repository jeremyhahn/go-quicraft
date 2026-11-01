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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newPerShardTestCollector builds a collector with PerShardMetrics enabled.
// Most of the assertions in this file predate the cardinality switch and read
// per-shard child series by shard_id, which only exist in that mode; the
// default (aggregated) mode has its own tests further down.
func newPerShardTestCollector() *PrometheusMetricsCollector {
	return NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{PerShardMetrics: true})
}

// gatherMetric gathers a single metric family by name from the registry.
func gatherMetric(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() failed: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() == name {
			return mf
		}
	}
	t.Fatalf("metric family %q not found in registry", name)
	return nil
}

// counterValue extracts the counter value from a metric family with the given
// label values. Labels must be provided as key=value pairs in order.
func counterValue(t *testing.T, mf *dto.MetricFamily, labels map[string]string) float64 {
	t.Helper()
	for _, m := range mf.GetMetric() {
		if matchLabels(m, labels) {
			return m.GetCounter().GetValue()
		}
	}
	t.Fatalf("no metric with labels %v found in %q", labels, mf.GetName())
	return 0
}

// histogramCount extracts the sample count from a histogram metric family
// with the given label values.
func histogramCount(t *testing.T, mf *dto.MetricFamily, labels map[string]string) uint64 {
	t.Helper()
	for _, m := range mf.GetMetric() {
		if matchLabels(m, labels) {
			return m.GetHistogram().GetSampleCount()
		}
	}
	t.Fatalf("no metric with labels %v found in %q", labels, mf.GetName())
	return 0
}

// histogramSum extracts the sample sum from a histogram metric family.
func histogramSum(t *testing.T, mf *dto.MetricFamily, labels map[string]string) float64 {
	t.Helper()
	for _, m := range mf.GetMetric() {
		if matchLabels(m, labels) {
			return m.GetHistogram().GetSampleSum()
		}
	}
	t.Fatalf("no metric with labels %v found in %q", labels, mf.GetName())
	return 0
}

// simpleHistogramCount extracts the sample count from a histogram without labels.
func simpleHistogramCount(t *testing.T, mf *dto.MetricFamily) uint64 {
	t.Helper()
	metrics := mf.GetMetric()
	if len(metrics) == 0 {
		t.Fatalf("no metrics in %q", mf.GetName())
	}
	return metrics[0].GetHistogram().GetSampleCount()
}

// simpleHistogramSum extracts the sample sum from a histogram without labels.
func simpleHistogramSum(t *testing.T, mf *dto.MetricFamily) float64 {
	t.Helper()
	metrics := mf.GetMetric()
	if len(metrics) == 0 {
		t.Fatalf("no metrics in %q", mf.GetName())
	}
	return metrics[0].GetHistogram().GetSampleSum()
}

// simpleCounterValue extracts the counter value from a non-labeled counter.
func simpleCounterValue(t *testing.T, mf *dto.MetricFamily) float64 {
	t.Helper()
	metrics := mf.GetMetric()
	if len(metrics) == 0 {
		t.Fatalf("no metrics in %q", mf.GetName())
	}
	return metrics[0].GetCounter().GetValue()
}

func matchLabels(m *dto.Metric, labels map[string]string) bool {
	if len(labels) == 0 {
		return len(m.GetLabel()) == 0
	}
	pairs := m.GetLabel()
	if len(pairs) != len(labels) {
		return false
	}
	for _, lp := range pairs {
		want, ok := labels[lp.GetName()]
		if !ok || want != lp.GetValue() {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Compile-time interface compliance
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

func TestNewPrometheusMetricsCollector(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	if c == nil {
		t.Fatal("NewPrometheusMetricsCollector() returned nil")
	}
	if c.Registry == nil {
		t.Fatal("Registry is nil")
	}

	// Verify all expected metric families are registered by gathering. The
	// families themselves are inspected after the observations below; a fresh
	// registry may legitimately report none yet, so only the error matters.
	if _, err := c.Registry.Gather(); err != nil {
		t.Fatalf("Gather() failed: %v", err)
	}
	// Before any observations, gather may return empty for some metric types.
	// Trigger at least one observation per metric to populate all families.
	c.ObserveProposalLatency(1, time.Millisecond)
	c.ObserveCommitLatency(1, time.Millisecond)
	c.ObserveApplyLatency(1, time.Millisecond)
	c.ObserveReadIndexLatency(1, time.Millisecond)
	c.IncProposalDropped(1)
	c.ObserveProposalBatchSize(1, 10)
	c.ObserveCommitBatchSize(5)
	c.IncElection(1)
	c.IncSnapshot(1)
	c.ObserveWriteLatency(time.Millisecond)
	c.ObserveFsyncLatency(time.Millisecond)
	c.ObserveCompactionLatency(time.Millisecond)
	c.IncWriteBytes(100)
	c.ObserveSendLatency(time.Millisecond)
	c.IncMessagesSent(1, 2, 3)
	c.IncMessagesReceived(1, 2, 3)
	c.IncSnapshotsSent(1, 1024, time.Second)
	c.IncSnapshotsReceived(1, 2048, time.Second)

	families, err := c.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather() after observations failed: %v", err)
	}

	expected := map[string]bool{
		"quicraft_raft_proposal_latency_seconds":      false,
		"quicraft_raft_commit_latency_seconds":        false,
		"quicraft_raft_apply_latency_seconds":         false,
		"quicraft_raft_read_index_latency_seconds":    false,
		"quicraft_raft_proposals_dropped_total":       false,
		"quicraft_raft_proposal_batch_size":           false,
		"quicraft_raft_commit_batch_size":             false,
		"quicraft_raft_elections_total":               false,
		"quicraft_raft_snapshots_total":               false,
		"quicraft_logdb_write_latency_seconds":        false,
		"quicraft_logdb_fsync_latency_seconds":        false,
		"quicraft_logdb_compaction_latency_seconds":   false,
		"quicraft_logdb_write_bytes_total":            false,
		"quicraft_transport_send_latency_seconds":     false,
		"quicraft_transport_messages_sent_total":      false,
		"quicraft_transport_messages_received_total":  false,
		"quicraft_transport_snapshots_sent_total":     false,
		"quicraft_transport_snapshots_sent_bytes":     false,
		"quicraft_transport_snapshots_received_total": false,
		"quicraft_transport_snapshots_received_bytes": false,
	}

	for _, mf := range families {
		if _, ok := expected[mf.GetName()]; ok {
			expected[mf.GetName()] = true
		}
	}

	for name, found := range expected {
		if !found {
			t.Errorf("expected metric family %q not found in registry", name)
		}
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Proposal Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveProposalLatency(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveProposalLatency(42, 5*time.Millisecond)
	c.ObserveProposalLatency(42, 15*time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_latency_seconds")
	labels := map[string]string{"shard_id": "42"}
	count := histogramCount(t, mf, labels)
	if count != 2 {
		t.Errorf("proposal latency sample count = %d, want 2", count)
	}
	sum := histogramSum(t, mf, labels)
	wantSum := 0.020 // 5ms + 15ms
	if sum < wantSum*0.99 || sum > wantSum*1.01 {
		t.Errorf("proposal latency sum = %f, want ~%f", sum, wantSum)
	}
}

func TestPrometheusMetricsCollector_ObserveProposalLatency_MultipleShard(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveProposalLatency(1, time.Millisecond)
	c.ObserveProposalLatency(2, 2*time.Millisecond)
	c.ObserveProposalLatency(2, 3*time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_latency_seconds")
	if histogramCount(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("shard 1 should have 1 observation")
	}
	if histogramCount(t, mf, map[string]string{"shard_id": "2"}) != 2 {
		t.Error("shard 2 should have 2 observations")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Commit Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveCommitLatency(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveCommitLatency(10, 50*time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_commit_latency_seconds")
	labels := map[string]string{"shard_id": "10"}
	if histogramCount(t, mf, labels) != 1 {
		t.Error("expected 1 observation")
	}
	sum := histogramSum(t, mf, labels)
	if sum < 0.049 || sum > 0.051 {
		t.Errorf("commit latency sum = %f, want ~0.05", sum)
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Apply Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveApplyLatency(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveApplyLatency(7, 100*time.Microsecond)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_apply_latency_seconds")
	if histogramCount(t, mf, map[string]string{"shard_id": "7"}) != 1 {
		t.Error("expected 1 observation")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: ReadIndex Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveReadIndexLatency(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveReadIndexLatency(42, 5*time.Millisecond)
	c.ObserveReadIndexLatency(42, 15*time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_read_index_latency_seconds")
	labels := map[string]string{"shard_id": "42"}
	count := histogramCount(t, mf, labels)
	if count != 2 {
		t.Errorf("read index latency sample count = %d, want 2", count)
	}
	sum := histogramSum(t, mf, labels)
	wantSum := 0.020 // 5ms + 15ms
	if sum < wantSum*0.99 || sum > wantSum*1.01 {
		t.Errorf("read index latency sum = %f, want ~%f", sum, wantSum)
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Proposal Dropped
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncProposalDropped(t *testing.T) {
	c := newPerShardTestCollector()
	c.IncProposalDropped(5)
	c.IncProposalDropped(5)
	c.IncProposalDropped(5)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposals_dropped_total")
	val := counterValue(t, mf, map[string]string{"shard_id": "5"})
	if val != 3 {
		t.Errorf("proposals_dropped = %f, want 3", val)
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Proposal Batch Size
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveProposalBatchSize(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveProposalBatchSize(3, 16)
	c.ObserveProposalBatchSize(3, 32)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_batch_size")
	labels := map[string]string{"shard_id": "3"}
	if histogramCount(t, mf, labels) != 2 {
		t.Error("expected 2 observations")
	}
	sum := histogramSum(t, mf, labels)
	if sum != 48 {
		t.Errorf("proposal batch size sum = %f, want 48", sum)
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Commit Batch Size
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveCommitBatchSize(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveCommitBatchSize(100)
	c.ObserveCommitBatchSize(200)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_commit_batch_size")
	if simpleHistogramCount(t, mf) != 2 {
		t.Error("expected 2 observations")
	}
	sum := simpleHistogramSum(t, mf)
	if sum != 300 {
		t.Errorf("commit batch size sum = %f, want 300", sum)
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Elections
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncElection(t *testing.T) {
	c := newPerShardTestCollector()
	c.IncElection(99)
	c.IncElection(99)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_elections_total")
	if counterValue(t, mf, map[string]string{"shard_id": "99"}) != 2 {
		t.Error("expected 2 elections")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Snapshots
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncSnapshot(t *testing.T) {
	c := newPerShardTestCollector()
	c.IncSnapshot(50)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_snapshots_total")
	if counterValue(t, mf, map[string]string{"shard_id": "50"}) != 1 {
		t.Error("expected 1 snapshot")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Piggyback Commits
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncPiggybackCommit(t *testing.T) {
	c := newPerShardTestCollector()
	c.IncPiggybackCommit(60)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_piggyback_commits_total")
	if counterValue(t, mf, map[string]string{"shard_id": "60"}) != 1 {
		t.Error("expected 1 piggyback commit")
	}
}

// ---------------------------------------------------------------------------
// LogDBMetrics: Write Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveWriteLatency(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveWriteLatency(2 * time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_write_latency_seconds")
	if simpleHistogramCount(t, mf) != 1 {
		t.Error("expected 1 observation")
	}
	sum := simpleHistogramSum(t, mf)
	if sum < 0.0019 || sum > 0.0021 {
		t.Errorf("write latency sum = %f, want ~0.002", sum)
	}
}

// ---------------------------------------------------------------------------
// LogDBMetrics: Fsync Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveFsyncLatency(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveFsyncLatency(10 * time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_fsync_latency_seconds")
	if simpleHistogramCount(t, mf) != 1 {
		t.Error("expected 1 observation")
	}
}

// ---------------------------------------------------------------------------
// LogDBMetrics: Compaction Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveCompactionLatency(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveCompactionLatency(500 * time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_compaction_latency_seconds")
	if simpleHistogramCount(t, mf) != 1 {
		t.Error("expected 1 observation")
	}
}

// ---------------------------------------------------------------------------
// LogDBMetrics: Write Bytes
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncWriteBytes(t *testing.T) {
	c := newPerShardTestCollector()
	c.IncWriteBytes(4096)
	c.IncWriteBytes(8192)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_write_bytes_total")
	val := simpleCounterValue(t, mf)
	if val != 12288 {
		t.Errorf("write bytes = %f, want 12288", val)
	}
}

// ---------------------------------------------------------------------------
// TransportMetrics: Send Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveSendLatency(t *testing.T) {
	c := newPerShardTestCollector()
	c.ObserveSendLatency(250 * time.Microsecond)

	mf := gatherMetric(t, c.Registry, "quicraft_transport_send_latency_seconds")
	if simpleHistogramCount(t, mf) != 1 {
		t.Error("expected 1 observation")
	}
}

// ---------------------------------------------------------------------------
// TransportMetrics: Messages Sent
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncMessagesSent(t *testing.T) {
	c := newPerShardTestCollector()
	c.EnablePerMessageMetrics = true // Enable detailed per-message metrics
	c.IncMessagesSent(1, 2, 3)
	c.IncMessagesSent(1, 2, 3)

	// Check aggregate counter
	mf := gatherMetric(t, c.Registry, "quicraft_transport_messages_sent_total")
	if counterValue(t, mf, nil) != 2 {
		t.Error("expected 2 messages sent (aggregate)")
	}

	// Check detailed counter
	mf = gatherMetric(t, c.Registry, "quicraft_transport_messages_sent_detailed_total")
	labels := map[string]string{"shard_id": "1", "to": "2", "msg_type": "3"}
	if counterValue(t, mf, labels) != 2 {
		t.Error("expected 2 messages sent (detailed)")
	}
}

func TestPrometheusMetricsCollector_IncMessagesSent_DifferentLabels(t *testing.T) {
	c := newPerShardTestCollector()
	c.EnablePerMessageMetrics = true // Enable detailed per-message metrics
	c.IncMessagesSent(1, 2, 3)
	c.IncMessagesSent(1, 3, 3) // different target
	c.IncMessagesSent(2, 2, 3) // different shard

	// Check aggregate counter
	mf := gatherMetric(t, c.Registry, "quicraft_transport_messages_sent_total")
	if counterValue(t, mf, nil) != 3 {
		t.Error("expected 3 messages sent (aggregate)")
	}

	// Check detailed counter
	mf = gatherMetric(t, c.Registry, "quicraft_transport_messages_sent_detailed_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1", "to": "2", "msg_type": "3"}) != 1 {
		t.Error("shard=1,to=2 should have 1")
	}
	if counterValue(t, mf, map[string]string{"shard_id": "1", "to": "3", "msg_type": "3"}) != 1 {
		t.Error("shard=1,to=3 should have 1")
	}
	if counterValue(t, mf, map[string]string{"shard_id": "2", "to": "2", "msg_type": "3"}) != 1 {
		t.Error("shard=2,to=2 should have 1")
	}
}

// ---------------------------------------------------------------------------
// TransportMetrics: Messages Received
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncMessagesReceived(t *testing.T) {
	c := newPerShardTestCollector()
	c.EnablePerMessageMetrics = true // Enable detailed per-message metrics
	c.IncMessagesReceived(10, 20, 5)

	// Check aggregate counter
	mf := gatherMetric(t, c.Registry, "quicraft_transport_messages_received_total")
	if counterValue(t, mf, nil) != 1 {
		t.Error("expected 1 message received (aggregate)")
	}

	// Check detailed counter
	mf = gatherMetric(t, c.Registry, "quicraft_transport_messages_received_detailed_total")
	labels := map[string]string{"shard_id": "10", "from": "20", "msg_type": "5"}
	if counterValue(t, mf, labels) != 1 {
		t.Error("expected 1 message received (detailed)")
	}
}

// ---------------------------------------------------------------------------
// TransportMetrics: Snapshots Sent
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncSnapshotsSent(t *testing.T) {
	c := newPerShardTestCollector()
	c.IncSnapshotsSent(1, 1048576, 5*time.Second)

	mf := gatherMetric(t, c.Registry, "quicraft_transport_snapshots_sent_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("expected 1 snapshot sent")
	}

	bytesMF := gatherMetric(t, c.Registry, "quicraft_transport_snapshots_sent_bytes")
	labels := map[string]string{"shard_id": "1"}
	if histogramCount(t, bytesMF, labels) != 1 {
		t.Error("expected 1 bytes observation")
	}
	sum := histogramSum(t, bytesMF, labels)
	if sum != 1048576 {
		t.Errorf("snapshot bytes sent = %f, want 1048576", sum)
	}
}

// ---------------------------------------------------------------------------
// TransportMetrics: Snapshots Received
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncSnapshotsReceived(t *testing.T) {
	c := newPerShardTestCollector()
	c.IncSnapshotsReceived(2, 2097152, 10*time.Second)

	mf := gatherMetric(t, c.Registry, "quicraft_transport_snapshots_received_total")
	if counterValue(t, mf, map[string]string{"shard_id": "2"}) != 1 {
		t.Error("expected 1 snapshot received")
	}

	bytesMF := gatherMetric(t, c.Registry, "quicraft_transport_snapshots_received_bytes")
	if histogramCount(t, bytesMF, map[string]string{"shard_id": "2"}) != 1 {
		t.Error("expected 1 bytes observation")
	}
}

// ---------------------------------------------------------------------------
// Histogram bucket verification
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_LatencyBuckets(t *testing.T) {
	c := newPerShardTestCollector()
	// Observe 1ms - should land in the 0.001 bucket.
	c.ObserveProposalLatency(1, time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_latency_seconds")
	for _, m := range mf.GetMetric() {
		h := m.GetHistogram()
		// Find the 0.001 bucket and verify count >= 1.
		for _, b := range h.GetBucket() {
			if b.GetUpperBound() == 0.001 {
				if b.GetCumulativeCount() < 1 {
					t.Error("1ms observation should appear in 0.001s bucket")
				}
				return
			}
		}
		t.Error("0.001s bucket not found in histogram")
	}
}

func TestPrometheusMetricsCollector_BatchSizeBuckets(t *testing.T) {
	c := newPerShardTestCollector()
	// Observe batch size of 8 - should land in bucket 8.
	c.ObserveCommitBatchSize(8)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_commit_batch_size")
	h := mf.GetMetric()[0].GetHistogram()
	for _, b := range h.GetBucket() {
		if b.GetUpperBound() == 8 {
			if b.GetCumulativeCount() < 1 {
				t.Error("batch size 8 should appear in bucket 8")
			}
			return
		}
	}
	t.Error("bucket 8 not found in batch size histogram")
}

// ---------------------------------------------------------------------------
// Concurrent safety
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ConcurrentAccess(t *testing.T) {
	c := newPerShardTestCollector()
	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			shardID := uint64(id % 10)
			for range iterations {
				c.ObserveProposalLatency(shardID, time.Millisecond)
				c.ObserveCommitLatency(shardID, time.Millisecond)
				c.ObserveApplyLatency(shardID, time.Millisecond)
				c.ObserveReadIndexLatency(shardID, time.Millisecond)
				c.IncProposalDropped(shardID)
				c.ObserveProposalBatchSize(shardID, 10)
				c.ObserveCommitBatchSize(5)
				c.IncElection(shardID)
				c.IncSnapshot(shardID)
				c.ObserveWriteLatency(time.Millisecond)
				c.ObserveFsyncLatency(time.Millisecond)
				c.ObserveCompactionLatency(time.Millisecond)
				c.IncWriteBytes(100)
				c.ObserveSendLatency(time.Millisecond)
				c.IncMessagesSent(shardID, uint64(id), 1)
				c.IncMessagesReceived(shardID, uint64(id), 1)
				c.IncSnapshotsSent(shardID, 1024, time.Second)
				c.IncSnapshotsReceived(shardID, 2048, time.Second)
			}
		}(g)
	}

	wg.Wait()

	// Verify totals are consistent.
	mf := gatherMetric(t, c.Registry, "quicraft_logdb_write_bytes_total")
	val := simpleCounterValue(t, mf)
	expected := float64(goroutines * iterations * 100)
	if val != expected {
		t.Errorf("write bytes = %f, want %f", val, expected)
	}
}

func TestPrometheusMetricsCollector_ConcurrentGather(t *testing.T) {
	c := newPerShardTestCollector()
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 2) // half writers, half readers

	for range goroutines {
		go func() {
			defer wg.Done()
			for range 100 {
				c.IncElection(1)
				c.ObserveWriteLatency(time.Millisecond)
			}
		}()
		go func() {
			defer wg.Done()
			for range 100 {
				_, err := c.Registry.Gather()
				if err != nil {
					t.Errorf("concurrent Gather() failed: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// Isolation: separate instances do not interfere
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_RegistryIsolation(t *testing.T) {
	c1 := newPerShardTestCollector()
	c2 := newPerShardTestCollector()

	c1.IncElection(1)
	c1.IncElection(1)

	c2.IncElection(1)

	mf1 := gatherMetric(t, c1.Registry, "quicraft_raft_elections_total")
	mf2 := gatherMetric(t, c2.Registry, "quicraft_raft_elections_total")

	if counterValue(t, mf1, map[string]string{"shard_id": "1"}) != 2 {
		t.Error("c1 should have 2 elections")
	}
	if counterValue(t, mf2, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("c2 should have 1 election")
	}
}

func TestPrometheusMetricsCollector_RegistryIsolation_NoGlobalPollution(t *testing.T) {
	// Creating a collector should NOT register anything in the default registry.
	_ = newPerShardTestCollector()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() failed: %v", err)
	}
	for _, mf := range families {
		name := mf.GetName()
		if len(name) > 9 && name[:9] == "quicraft_" {
			t.Errorf("quicraft metric %q found in default registry", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: overhead comparison vs NoOp
// ---------------------------------------------------------------------------

func BenchmarkNoOpMetricsCollector_IncElection(b *testing.B) {
	c := NoOpMetricsCollector{}
	b.ResetTimer()
	for b.Loop() {
		c.IncElection(1)
	}
}

func BenchmarkPrometheusMetricsCollector_IncElection(b *testing.B) {
	c := newPerShardTestCollector()
	b.ResetTimer()
	for b.Loop() {
		c.IncElection(1)
	}
}

func BenchmarkPrometheusMetricsCollector_IncElection_PreBound(b *testing.B) {
	c := newPerShardTestCollector()
	c.RegisterShard(1)
	b.ResetTimer()
	for b.Loop() {
		c.IncElection(1)
	}
}

func BenchmarkNoOpMetricsCollector_ObserveProposalLatency(b *testing.B) {
	c := NoOpMetricsCollector{}
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveProposalLatency(1, d)
	}
}

func BenchmarkPrometheusMetricsCollector_ObserveProposalLatency(b *testing.B) {
	c := newPerShardTestCollector()
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveProposalLatency(1, d)
	}
}

func BenchmarkPrometheusMetricsCollector_ObserveProposalLatency_PreBound(b *testing.B) {
	c := newPerShardTestCollector()
	c.RegisterShard(1)
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveProposalLatency(1, d)
	}
}

func BenchmarkPrometheusMetricsCollector_IncMessagesSent(b *testing.B) {
	c := newPerShardTestCollector()
	b.ResetTimer()
	for b.Loop() {
		c.IncMessagesSent(1, 2, 3)
	}
}

func BenchmarkPrometheusMetricsCollector_ObserveReadIndexLatency(b *testing.B) {
	c := newPerShardTestCollector()
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveReadIndexLatency(1, d)
	}
}

func BenchmarkPrometheusMetricsCollector_ObserveWriteLatency(b *testing.B) {
	c := newPerShardTestCollector()
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveWriteLatency(d)
	}
}

func BenchmarkPrometheusMetricsCollector_AllRaftMetrics(b *testing.B) {
	c := newPerShardTestCollector()
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveProposalLatency(1, d)
		c.ObserveCommitLatency(1, d)
		c.ObserveApplyLatency(1, d)
		c.ObserveReadIndexLatency(1, d)
		c.IncProposalDropped(1)
		c.ObserveProposalBatchSize(1, 10)
		c.ObserveCommitBatchSize(5)
		c.IncElection(1)
		c.IncSnapshot(1)
	}
}

func BenchmarkPrometheusMetricsCollector_AllRaftMetrics_PreBound(b *testing.B) {
	c := newPerShardTestCollector()
	c.RegisterShard(1)
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveProposalLatency(1, d)
		c.ObserveCommitLatency(1, d)
		c.ObserveApplyLatency(1, d)
		c.ObserveReadIndexLatency(1, d)
		c.IncProposalDropped(1)
		c.ObserveProposalBatchSize(1, 10)
		c.ObserveCommitBatchSize(5)
		c.IncElection(1)
		c.IncSnapshot(1)
	}
}

// ---------------------------------------------------------------------------
// RegisterShard / UnregisterShard
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_RegisterShard(t *testing.T) {
	c := newPerShardTestCollector()
	c.RegisterShard(42)

	// After registration, metrics should use pre-bound metric references.
	// Verify by recording a metric and checking the result.
	c.ObserveProposalLatency(42, 5*time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_latency_seconds")
	labels := map[string]string{"shard_id": "42"}
	count := histogramCount(t, mf, labels)
	if count != 1 {
		t.Errorf("expected 1 observation, got %d", count)
	}
}

func TestPrometheusMetricsCollector_RegisterShard_MultipleTimes(t *testing.T) {
	c := newPerShardTestCollector()

	// Registering the same shard multiple times should not panic and
	// should overwrite the previous registration.
	c.RegisterShard(1)
	c.RegisterShard(1)
	c.RegisterShard(1)

	c.IncElection(1)
	mf := gatherMetric(t, c.Registry, "quicraft_raft_elections_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("expected 1 election")
	}
}

// TestPrometheusMetricsCollector_UnregisterShard is the ML8 regression test:
// a sample that arrives AFTER UnregisterShard (an OnApplied still in flight
// on the engine apply goroutine when StopShard runs) must be dropped, not
// re-create the child series the unregister just deleted. Re-creating it
// would resurrect the series permanently and reinstate the cardinality leak
// UnregisterShard exists to close.
func TestPrometheusMetricsCollector_UnregisterShard(t *testing.T) {
	c := newPerShardTestCollector()
	c.RegisterShard(99)
	c.ObserveProposalLatency(99, time.Millisecond)
	if !shardSeriesExists(t, c.Registry, "quicraft_raft_proposal_latency_seconds", "99") {
		t.Fatal("series should exist while the shard is registered")
	}

	c.UnregisterShard(99)

	// Residual in-flight samples across every per-shard hot-path method.
	c.ObserveProposalLatency(99, time.Millisecond)
	c.ObserveCommitLatency(99, time.Millisecond)
	c.ObserveApplyLatency(99, time.Millisecond)
	c.ObserveReadIndexLatency(99, time.Millisecond)
	c.IncProposalDropped(99)
	c.ObserveProposalBatchSize(99, 4)
	c.IncElection(99)
	c.IncSnapshot(99)
	c.IncPiggybackCommit(99)
	c.IncSnapshotsSent(99, 1024, time.Millisecond)
	c.IncSnapshotsReceived(99, 1024, time.Millisecond)

	for _, name := range []string{
		"quicraft_raft_proposal_latency_seconds",
		"quicraft_raft_commit_latency_seconds",
		"quicraft_raft_apply_latency_seconds",
		"quicraft_raft_read_index_latency_seconds",
		"quicraft_raft_proposals_dropped_total",
		"quicraft_raft_proposal_batch_size",
		"quicraft_raft_elections_total",
		"quicraft_raft_snapshots_total",
		"quicraft_raft_piggyback_commits_total",
		"quicraft_transport_snapshots_sent_total",
		"quicraft_transport_snapshots_sent_bytes",
		"quicraft_transport_snapshots_received_total",
		"quicraft_transport_snapshots_received_bytes",
	} {
		if shardSeriesExists(t, c.Registry, name, "99") {
			t.Errorf("%s: series resurrected by a post-unregister sample", name)
		}
	}
}

// TestPrometheusMetricsCollector_NeverRegisteredShardStillRecords pins the
// other half of the ML8 fix: only shards torn down by UnregisterShard are
// tombstoned. An embedder that never calls RegisterShard must still be
// metered through the fallback rather than silently unmetered.
func TestPrometheusMetricsCollector_NeverRegisteredShardStillRecords(t *testing.T) {
	c := newPerShardTestCollector()

	c.ObserveProposalLatency(7, time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_latency_seconds")
	if got := histogramCount(t, mf, map[string]string{"shard_id": "7"}); got != 1 {
		t.Errorf("observations for a never-registered shard = %d, want 1", got)
	}
}

func TestPrometheusMetricsCollector_UnregisterShard_NeverRegistered(t *testing.T) {
	c := newPerShardTestCollector()
	// Unregistering a never-registered shard should not panic.
	c.UnregisterShard(123)
	c.UnregisterShard(456)
}

func TestPrometheusMetricsCollector_RegisterUnregisterCycle(t *testing.T) {
	c := newPerShardTestCollector()

	// Full lifecycle: register, use, unregister, re-register. UnregisterShard
	// DELETES the per-shard series (bounding Prometheus cardinality under shard
	// churn), so the counter resets across the cycle: after re-register and one
	// IncSnapshot it reads 1, not the cumulative 2. A counter reset on shard
	// unload is correct and handled downstream by rate()/increase().
	c.RegisterShard(10)
	c.IncSnapshot(10)
	c.UnregisterShard(10)
	c.RegisterShard(10)
	c.IncSnapshot(10)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_snapshots_total")
	if got := counterValue(t, mf, map[string]string{"shard_id": "10"}); got != 1 {
		t.Errorf("snapshots after unregister/re-register = %v, want 1 (series reset on unregister)", got)
	}
}

// ---------------------------------------------------------------------------
// SampledMetricsCollector
// ---------------------------------------------------------------------------

func TestNewSampledMetricsCollector(t *testing.T) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 10)

	if s == nil {
		t.Fatal("NewSampledMetricsCollector returned nil")
	}
	if s.SampleRate != 10 {
		t.Errorf("SampleRate = %d, want 10", s.SampleRate)
	}
	if s.MetricsCollector != inner {
		t.Error("inner collector mismatch")
	}
}

func TestNewSampledMetricsCollector_ZeroRate(t *testing.T) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 0)

	// Zero rate should default to 100.
	if s.SampleRate != 100 {
		t.Errorf("SampleRate = %d, want 100 (default)", s.SampleRate)
	}
}

func TestSampledMetricsCollector_ImplementsMetricsCollector(t *testing.T) {
	inner := NoOpMetricsCollector{}
	s := NewSampledMetricsCollector(inner, 10)
	var _ MetricsCollector = s
}

func TestSampledMetricsCollector_ObserveCommitLatency_Sampling(t *testing.T) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 10) // 1 in 10

	// Record 100 observations; only 10 should be recorded.
	for range 100 {
		s.ObserveCommitLatency(1, time.Millisecond)
	}

	mf := gatherMetric(t, inner.Registry, "quicraft_raft_commit_latency_seconds")
	count := histogramCount(t, mf, map[string]string{"shard_id": "1"})
	if count != 10 {
		t.Errorf("expected 10 sampled observations, got %d", count)
	}
}

// TestSampledMetricsCollector_IncMessagesSent_Unsampled is a
// regression test: counters must record EVERY increment. Sampling a
// prometheus.Counter.Inc() makes *_total under-report by SampleRate, so
// every rate()/increase() built on it is off by orders of magnitude.
func TestSampledMetricsCollector_IncMessagesSent_Unsampled(t *testing.T) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 20)

	// Record 100 messages; ALL 100 must be recorded (counters are exact).
	for range 100 {
		s.IncMessagesSent(1, 2, 3)
	}

	mf := gatherMetric(t, inner.Registry, "quicraft_transport_messages_sent_total")
	val := counterValue(t, mf, nil)
	if val != 100 {
		t.Errorf("expected 100 messages sent (counters must not be sampled), got %f", val)
	}
}

// TestSampledMetricsCollector_IncMessagesReceived_Unsampled is the
// regression test for the receive counter; see IncMessagesSent_Unsampled.
func TestSampledMetricsCollector_IncMessagesReceived_Unsampled(t *testing.T) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 25)

	// Record 100 messages; ALL 100 must be recorded (counters are exact).
	for range 100 {
		s.IncMessagesReceived(1, 2, 3)
	}

	mf := gatherMetric(t, inner.Registry, "quicraft_transport_messages_received_total")
	val := counterValue(t, mf, nil)
	if val != 100 {
		t.Errorf("expected 100 messages received (counters must not be sampled), got %f", val)
	}
}

// TestSampledMetricsCollector_InterleavedSamplingIndependent is the
// regression test for the per-metric sampling counters. With a single
// counter shared across metrics, a strictly alternating commit/apply call
// pattern at an even SampleRate lands every even count on one metric —
// it gets sampled ALWAYS while the other is sampled NEVER. Dedicated
// counters guarantee each metric records 1 in SampleRate of its own
// observations regardless of interleaving.
func TestSampledMetricsCollector_InterleavedSamplingIndependent(t *testing.T) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 2) // 1 in 2

	for range 10 {
		s.ObserveCommitLatency(1, time.Millisecond)
		s.ObserveApplyLatency(1, time.Millisecond)
	}

	mf := gatherMetric(t, inner.Registry, "quicraft_raft_commit_latency_seconds")
	commitCount := histogramCount(t, mf, map[string]string{"shard_id": "1"})
	mf = gatherMetric(t, inner.Registry, "quicraft_raft_apply_latency_seconds")
	applyCount := histogramCount(t, mf, map[string]string{"shard_id": "1"})

	if commitCount != 5 || applyCount != 5 {
		t.Errorf("interleaved sampling: commit = %d, apply = %d, want 5 and 5 "+
			"(a shared counter would give 0 and 10)", commitCount, applyCount)
	}
}

func TestSampledMetricsCollector_PassThroughMethods(t *testing.T) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 1000)

	// Non-sampled methods should always pass through.
	s.IncElection(1)
	s.IncSnapshot(1)
	s.ObserveProposalLatency(1, time.Millisecond)
	s.RegisterShard(1)

	mf := gatherMetric(t, inner.Registry, "quicraft_raft_elections_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("IncElection should pass through")
	}

	mf = gatherMetric(t, inner.Registry, "quicraft_raft_snapshots_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("IncSnapshot should pass through")
	}

	// UnregisterShard also passes through; it deletes the per-shard series, so
	// it runs after the assertions above.
	s.UnregisterShard(1)
}

func TestSampledMetricsCollector_ConcurrentSampling(t *testing.T) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 100)

	const goroutines = 10
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				s.ObserveCommitLatency(1, time.Millisecond)
				s.ObserveApplyLatency(1, time.Millisecond)
				s.IncMessagesSent(1, 2, 3)
				s.IncMessagesReceived(1, 2, 3)
			}
		}()
	}

	wg.Wait()

	// Verify concurrent access does not panic/race, and that the message
	// counters — which are never sampled — recorded every increment.
	const wantMessages = float64(goroutines * iterations)
	mf := gatherMetric(t, inner.Registry, "quicraft_transport_messages_sent_total")
	if val := counterValue(t, mf, nil); val != wantMessages {
		t.Errorf("messages sent = %f, want %f (counters must not be sampled)", val, wantMessages)
	}
	mf = gatherMetric(t, inner.Registry, "quicraft_transport_messages_received_total")
	if val := counterValue(t, mf, nil); val != wantMessages {
		t.Errorf("messages received = %f, want %f (counters must not be sampled)", val, wantMessages)
	}
}

func TestSampledMetricsCollector_SampleRateOne(t *testing.T) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 1) // Every observation

	for range 10 {
		s.ObserveCommitLatency(1, time.Millisecond)
	}

	mf := gatherMetric(t, inner.Registry, "quicraft_raft_commit_latency_seconds")
	count := histogramCount(t, mf, map[string]string{"shard_id": "1"})
	if count != 10 {
		t.Errorf("expected all 10 observations with rate=1, got %d", count)
	}
}

func BenchmarkSampledMetricsCollector_ObserveCommitLatency(b *testing.B) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 100)
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		s.ObserveCommitLatency(1, d)
	}
}

func BenchmarkSampledMetricsCollector_IncMessagesSent(b *testing.B) {
	inner := newPerShardTestCollector()
	s := NewSampledMetricsCollector(inner, 100)
	b.ResetTimer()
	for b.Loop() {
		s.IncMessagesSent(1, 2, 3)
	}
}

// shardSeriesExists reports whether a per-shard series with the given shard_id
// label is present in the named metric family (false if the family or series is
// absent — e.g. after the child series has been deleted).
func shardSeriesExists(t *testing.T, reg *prometheus.Registry, name, shardID string) bool {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "shard_id" && l.GetValue() == shardID {
					return true
				}
			}
		}
	}
	return false
}

// TestPrometheus_UnauthorizedMessageMetric_BoundedCardinality is a
// regression test: the security drop counter must not key on the attacker
// controlled "from" replica id, or it accumulates unbounded child series.
func TestPrometheus_UnauthorizedMessageMetric_BoundedCardinality(t *testing.T) {
	c := newPerShardTestCollector()
	const shard = uint64(7)
	for from := uint64(1); from <= 500; from++ {
		c.IncUnauthorizedMessageDropped(shard, from, dropUnknownMember)
		c.IncUnauthorizedSnapshotDropped(shard, from)
	}

	// One series per (shard, reason) — the reason is a closed set the handler
	// picks, so it adds a fixed, small number of series. The "from" is not a
	// label on either counter.
	mf := gatherMetric(t, c.Registry, "quicraft_security_unauthorized_messages_dropped_total")
	if got := len(mf.GetMetric()); got != 1 {
		t.Fatalf("unauthorized_messages_dropped_total has %d series, want 1 "+
			"(the attacker-controlled 'from' must not be a label)", got)
	}
	if v := counterValue(t, mf, map[string]string{"shard_id": "7", "reason": dropUnknownMember}); v != 500 {
		t.Fatalf("unauthorized_messages_dropped_total shard 7 counter = %v, want 500", v)
	}

	mf = gatherMetric(t, c.Registry, "quicraft_security_unauthorized_snapshots_dropped_total")
	if got := len(mf.GetMetric()); got != 1 {
		t.Fatalf("unauthorized_snapshots_dropped_total has %d series, want 1", got)
	}
	if v := counterValue(t, mf, map[string]string{"shard_id": "7"}); v != 500 {
		t.Fatalf("unauthorized_snapshots_dropped_total shard 7 counter = %v, want 500", v)
	}
}

// TestPrometheus_UnauthorizedMessageMetric_ReasonsAreSeparateSeries pins the
// operational split. "unknown_member" is the benign config-change window and
// "identity_mismatch" is a sender the configuration names arriving on a
// connection with the wrong identity; an operator alerting on the second must
// not have the first summed into it.
func TestPrometheus_UnauthorizedMessageMetric_ReasonsAreSeparateSeries(t *testing.T) {
	c := newPerShardTestCollector()
	const shard = uint64(3)
	c.IncUnauthorizedMessageDropped(shard, 2, dropUnknownMember)
	c.IncUnauthorizedMessageDropped(shard, 2, dropUnknownMember)
	c.IncUnauthorizedMessageDropped(shard, 9, dropIdentityMismatch)

	mf := gatherMetric(t, c.Registry, "quicraft_security_unauthorized_messages_dropped_total")
	if v := counterValue(t, mf, map[string]string{"shard_id": "3", "reason": dropUnknownMember}); v != 2 {
		t.Fatalf("unknown_member counter = %v, want 2", v)
	}
	if v := counterValue(t, mf, map[string]string{"shard_id": "3", "reason": dropIdentityMismatch}); v != 1 {
		t.Fatalf("identity_mismatch counter = %v, want 1: a forged From is being summed "+
			"into the benign config-change window", v)
	}
}

// TestPrometheus_UnregisterShard_DeletesSeries is the regression test:
// UnregisterShard must delete the underlying per-shard Prometheus child series,
// not just the local cache entry, so shard churn does not leak series forever.
func TestPrometheus_UnregisterShard_DeletesSeries(t *testing.T) {
	c := newPerShardTestCollector()
	const shard = uint64(42)
	const name = "quicraft_raft_elections_total"

	c.RegisterShard(shard)
	if !shardSeriesExists(t, c.Registry, name, "42") {
		t.Fatal("per-shard series should exist after RegisterShard")
	}

	c.UnregisterShard(shard)
	if shardSeriesExists(t, c.Registry, name, "42") {
		t.Fatal("per-shard series must be deleted after UnregisterShard (cardinality leak)")
	}
}

// ---------------------------------------------------------------------------
// TransportMetrics: Snapshots Rejected / Messages Dropped
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncSnapshotsRejected(t *testing.T) {
	c := newPerShardTestCollector()
	c.IncSnapshotsRejected(1, "malformed")
	c.IncSnapshotsRejected(1, "malformed")
	c.IncSnapshotsRejected(1, "liveness")
	// A header-stage rejection has no parsed shard and lands on shard 0.
	c.IncSnapshotsRejected(0, "io_error")

	mf := gatherMetric(t, c.Registry, "quicraft_transport_snapshots_rejected_total")
	if v := counterValue(t, mf, map[string]string{"shard_id": "1", "reason": "malformed"}); v != 2 {
		t.Errorf("malformed counter = %v, want 2", v)
	}
	if v := counterValue(t, mf, map[string]string{"shard_id": "1", "reason": "liveness"}); v != 1 {
		t.Errorf("liveness counter = %v, want 1", v)
	}
	if v := counterValue(t, mf, map[string]string{"shard_id": "0", "reason": "io_error"}); v != 1 {
		t.Errorf("io_error counter = %v, want 1", v)
	}
}

// TestPrometheusMetricsCollector_IncMessagesDropped_BoundedCardinality asserts
// the drop counter keys on shard and reason only: the target replica id must
// not multiply the series count by the shard's membership size.
func TestPrometheusMetricsCollector_IncMessagesDropped_BoundedCardinality(t *testing.T) {
	c := newPerShardTestCollector()
	const shard = uint64(3)
	for to := uint64(1); to <= 100; to++ {
		c.IncMessagesDropped(shard, to, "queue_full")
	}

	mf := gatherMetric(t, c.Registry, "quicraft_transport_messages_dropped_total")
	if got := len(mf.GetMetric()); got != 1 {
		t.Fatalf("messages_dropped_total has %d series, want 1 ('to' must not be a label)", got)
	}
	if v := counterValue(t, mf, map[string]string{"shard_id": "3", "reason": "queue_full"}); v != 100 {
		t.Fatalf("queue_full counter = %v, want 100", v)
	}
}

// TestPrometheus_UnregisterShard_DeletesDropSeries asserts the reason-labeled
// drop counters are torn down with the shard. They cannot use
// DeleteLabelValues (which addresses one full label set), so a missed
// DeletePartialMatch would leak one series per reason per stopped shard.
func TestPrometheus_UnregisterShard_DeletesDropSeries(t *testing.T) {
	c := newPerShardTestCollector()
	const shard = uint64(11)

	c.RegisterShard(shard)
	c.IncSnapshotsRejected(shard, "malformed")
	c.IncSnapshotsRejected(shard, "liveness")
	c.IncMessagesDropped(shard, 2, "queue_full")
	c.IncMessagesDropped(shard, 3, "write_failed")

	for _, name := range []string{
		"quicraft_transport_snapshots_rejected_total",
		"quicraft_transport_messages_dropped_total",
	} {
		if !shardSeriesExists(t, c.Registry, name, "11") {
			t.Fatalf("%s series should exist before UnregisterShard", name)
		}
	}

	c.UnregisterShard(shard)

	for _, name := range []string{
		"quicraft_transport_snapshots_rejected_total",
		"quicraft_transport_messages_dropped_total",
	} {
		if shardSeriesExists(t, c.Registry, name, "11") {
			t.Fatalf("%s series must be deleted after UnregisterShard (cardinality leak)", name)
		}
	}

	// The tombstone must also stop a residual in-flight sample from
	// re-creating the deleted series.
	c.IncSnapshotsRejected(shard, "malformed")
	c.IncMessagesDropped(shard, 2, "queue_full")
	for _, name := range []string{
		"quicraft_transport_snapshots_rejected_total",
		"quicraft_transport_messages_dropped_total",
	} {
		if shardSeriesExists(t, c.Registry, name, "11") {
			t.Fatalf("%s series re-created by a post-unregister sample", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Metrics cardinality: MetricsConfig.PerShardMetrics
// ---------------------------------------------------------------------------

// aggregatableFamilies are the metric families whose shard_id label the
// aggregated mode removes.
var aggregatableFamilies = []string{
	"quicraft_raft_proposal_latency_seconds",
	"quicraft_raft_commit_latency_seconds",
	"quicraft_raft_apply_latency_seconds",
	"quicraft_raft_read_index_latency_seconds",
	"quicraft_raft_proposal_batch_size",
	"quicraft_raft_snapshots_total",
	"quicraft_raft_piggyback_commits_total",
	"quicraft_transport_snapshots_sent_total",
	"quicraft_transport_snapshots_sent_bytes",
	"quicraft_transport_snapshots_received_total",
	"quicraft_transport_snapshots_received_bytes",
}

// alertableFamilies keep a shard_id label in BOTH modes: an operator pages on
// them and the shard is the only thing that makes the page actionable.
var alertableFamilies = []string{
	"quicraft_raft_elections_total",
	"quicraft_raft_proposals_dropped_total",
	"quicraft_transport_messages_dropped_total",
	"quicraft_transport_snapshots_rejected_total",
	"quicraft_security_unauthorized_messages_dropped_total",
	"quicraft_security_unauthorized_snapshots_dropped_total",
}

// exerciseAllShardMetrics drives every shard-scoped metric method once.
func exerciseAllShardMetrics(c *PrometheusMetricsCollector, shardID uint64) {
	c.ObserveProposalLatency(shardID, time.Millisecond)
	c.ObserveCommitLatency(shardID, time.Millisecond)
	c.ObserveApplyLatency(shardID, time.Millisecond)
	c.ObserveReadIndexLatency(shardID, time.Millisecond)
	c.ObserveProposalBatchSize(shardID, 4)
	c.IncProposalDropped(shardID)
	c.IncElection(shardID)
	c.IncSnapshot(shardID)
	c.IncPiggybackCommit(shardID)
	c.IncSnapshotsSent(shardID, 1024, time.Second)
	c.IncSnapshotsReceived(shardID, 2048, time.Second)
	c.IncSnapshotsRejected(shardID, "policy")
	c.IncMessagesDropped(shardID, 2, "queue_full")
	c.IncUnauthorizedMessageDropped(shardID, 2, "misrouted")
	c.IncUnauthorizedSnapshotDropped(shardID, 2)
}

// labelNames returns the sorted label names carried by a family's first child.
func labelNames(mf *dto.MetricFamily) []string {
	metrics := mf.GetMetric()
	if len(metrics) == 0 {
		return nil
	}
	var names []string
	for _, lp := range metrics[0].GetLabel() {
		names = append(names, lp.GetName())
	}
	sort.Strings(names)
	return names
}

func hasLabel(mf *dto.MetricFamily, name string) bool {
	for _, n := range labelNames(mf) {
		if n == name {
			return true
		}
	}
	return false
}

// TestPrometheusMetrics_DefaultIsAggregated pins the DEFAULT down: the
// zero-value config must collapse the volume-and-latency telemetry to one
// host-level series with no shard_id, and samples from different shards must
// land in that single series rather than in one series each.
func TestPrometheusMetrics_DefaultIsAggregated(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.RegisterShard(1)
	c.RegisterShard(2)
	exerciseAllShardMetrics(c, 1)
	exerciseAllShardMetrics(c, 2)

	for _, name := range aggregatableFamilies {
		mf := gatherMetric(t, c.Registry, name)
		if got := len(mf.GetMetric()); got != 1 {
			t.Errorf("%s: got %d series across 2 shards, want 1 aggregated series", name, got)
		}
		if hasLabel(mf, "shard_id") {
			t.Errorf("%s: carries a shard_id label in the aggregated default (labels=%v)",
				name, labelNames(mf))
		}
	}

	// Both shards' samples must be present in the single series.
	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_latency_seconds")
	if got := histogramCount(t, mf, nil); got != 2 {
		t.Errorf("aggregated proposal latency count = %d, want 2 (one per shard)", got)
	}
	snaps := gatherMetric(t, c.Registry, "quicraft_raft_snapshots_total")
	if got := counterValue(t, snaps, nil); got != 2 {
		t.Errorf("aggregated snapshots_total = %v, want 2 (one per shard)", got)
	}
}

// TestPrometheusMetrics_AlertableSeriesKeepShardID checks that turning the
// cardinality switch off does not cost an alert: every page-worthy signal
// still identifies its shard.
func TestPrometheusMetrics_AlertableSeriesKeepShardID(t *testing.T) {
	for _, perShard := range []bool{false, true} {
		c := NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{PerShardMetrics: perShard})
		c.RegisterShard(7)
		exerciseAllShardMetrics(c, 7)

		for _, name := range alertableFamilies {
			mf := gatherMetric(t, c.Registry, name)
			if !hasLabel(mf, "shard_id") {
				t.Errorf("PerShardMetrics=%v: %s lost its shard_id label (labels=%v)",
					perShard, name, labelNames(mf))
			}
		}
	}
}

// TestPrometheusMetrics_PerShardModeSplitsByShard is the counterpart to
// TestPrometheusMetrics_DefaultIsAggregated: opting in must actually restore
// the per-shard breakdown rather than being silently ignored.
func TestPrometheusMetrics_PerShardModeSplitsByShard(t *testing.T) {
	c := newPerShardTestCollector()
	c.RegisterShard(1)
	c.RegisterShard(2)
	exerciseAllShardMetrics(c, 1)
	exerciseAllShardMetrics(c, 2)

	for _, name := range aggregatableFamilies {
		mf := gatherMetric(t, c.Registry, name)
		if !hasLabel(mf, "shard_id") {
			t.Errorf("PerShardMetrics=true: %s has no shard_id label", name)
		}
		if got := len(mf.GetMetric()); got != 2 {
			t.Errorf("PerShardMetrics=true: %s has %d series, want 2 (one per shard)", name, got)
		}
	}
}

// TestPrometheusMetrics_PerShardEnabledGauge checks the mode is discoverable
// from the scrape itself, so a dashboard does not have to read the host config
// to know whether shard_id will be present.
func TestPrometheusMetrics_PerShardEnabledGauge(t *testing.T) {
	for _, tc := range []struct {
		perShard bool
		want     float64
	}{{false, 0}, {true, 1}} {
		c := NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{PerShardMetrics: tc.perShard})
		mf := gatherMetric(t, c.Registry, "quicraft_metrics_per_shard_enabled")
		metrics := mf.GetMetric()
		if len(metrics) != 1 {
			t.Fatalf("PerShardMetrics=%v: got %d gauge series, want 1", tc.perShard, len(metrics))
		}
		if got := metrics[0].GetGauge().GetValue(); got != tc.want {
			t.Errorf("PerShardMetrics=%v: gauge = %v, want %v", tc.perShard, got, tc.want)
		}
	}
}

// TestPrometheusMetrics_AggregatedFallbackNeverPanics drives every metric
// method for a shard that was never registered while aggregation is on. That
// path has no pre-bound entry, so it reaches the fallback, where the per-shard
// vectors do not exist. Without a mode-aware fallback this is a nil-pointer
// dereference on the first observation.
func TestPrometheusMetrics_AggregatedFallbackNeverPanics(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	exerciseAllShardMetrics(c, 99999) // never registered

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_latency_seconds")
	if got := histogramCount(t, mf, nil); got != 1 {
		t.Errorf("fallback sample not recorded: count = %d, want 1", got)
	}
}

// TestPrometheusMetrics_UnregisterShardKeepsHostAggregates guards the
// invariant that stopping one shard must not reset telemetry describing all
// the others. In aggregated mode the host-level series outlive every shard.
func TestPrometheusMetrics_UnregisterShardKeepsHostAggregates(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.RegisterShard(1)
	c.RegisterShard(2)
	exerciseAllShardMetrics(c, 1)
	exerciseAllShardMetrics(c, 2)

	c.UnregisterShard(1)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_latency_seconds")
	if got := histogramCount(t, mf, nil); got != 2 {
		t.Errorf("host aggregate lost samples after UnregisterShard: count = %d, want 2", got)
	}
	snaps := gatherMetric(t, c.Registry, "quicraft_raft_snapshots_total")
	if got := counterValue(t, snaps, nil); got != 2 {
		t.Errorf("host aggregate counter reset by UnregisterShard: got %v, want 2", got)
	}

	// The still-running shard keeps its own alertable series.
	elections := gatherMetric(t, c.Registry, "quicraft_raft_elections_total")
	if got := counterValue(t, elections, map[string]string{"shard_id": "2"}); got != 1 {
		t.Errorf("surviving shard elections_total = %v, want 1", got)
	}
}

// TestPrometheusMetrics_AggregatedModeSeriesBudget is the reason the switch
// exists. It counts the series a single registered shard adds and asserts the
// aggregated mode keeps that number flat as shards are added, while the
// per-shard mode grows it by roughly a hundred per shard.
func TestPrometheusMetrics_AggregatedModeSeriesBudget(t *testing.T) {
	countSeries := func(c *PrometheusMetricsCollector) int {
		families, err := c.Registry.Gather()
		if err != nil {
			t.Fatalf("Gather() failed: %v", err)
		}
		total := 0
		for _, mf := range families {
			for _, m := range mf.GetMetric() {
				switch {
				case m.GetHistogram() != nil:
					// bucket series + _sum + _count
					total += len(m.GetHistogram().GetBucket()) + 2
				default:
					total++
				}
			}
		}
		return total
	}

	grow := func(perShard bool, shards int) int {
		c := NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{PerShardMetrics: perShard})
		for i := 1; i <= shards; i++ {
			c.RegisterShard(uint64(i))
			exerciseAllShardMetrics(c, uint64(i))
		}
		return countSeries(c)
	}

	aggOne, aggTen := grow(false, 1), grow(false, 10)
	perOne, perTen := grow(true, 1), grow(true, 10)

	aggPerShard := float64(aggTen-aggOne) / 9
	perPerShard := float64(perTen-perOne) / 9
	t.Logf("series per additional shard: aggregated=%.1f per-shard=%.1f", aggPerShard, perPerShard)

	if aggPerShard > 15 {
		t.Errorf("aggregated mode grows %.1f series per shard, want <= 15", aggPerShard)
	}
	if perPerShard < 5*aggPerShard {
		t.Errorf("per-shard mode grows %.1f series per shard vs aggregated %.1f; "+
			"the switch is not saving cardinality", perPerShard, aggPerShard)
	}
}
