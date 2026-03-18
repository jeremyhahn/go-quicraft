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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

func TestPrometheusMetricsCollector_ImplementsMetricsCollector(t *testing.T) {
	var _ MetricsCollector = (*PrometheusMetricsCollector)(nil)
}

func TestPrometheusMetricsCollector_ImplementsRaftMetrics(t *testing.T) {
	var _ RaftMetrics = (*PrometheusMetricsCollector)(nil)
}

func TestPrometheusMetricsCollector_ImplementsLogDBMetrics(t *testing.T) {
	var _ LogDBMetrics = (*PrometheusMetricsCollector)(nil)
}

func TestPrometheusMetricsCollector_ImplementsTransportMetrics(t *testing.T) {
	var _ TransportMetrics = (*PrometheusMetricsCollector)(nil)
}

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

	// Verify all expected metric families are registered by gathering.
	families, err := c.Registry.Gather()
	if err != nil {
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

	families, err = c.Registry.Gather()
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

func TestNewPrometheusMetricsCollector_RegistryNotNil(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	if c.Registry == nil {
		t.Fatal("Registry must not be nil")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Proposal Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveProposalLatency(t *testing.T) {
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()
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

func TestPrometheusMetricsCollector_ObserveCommitLatency_ZeroDuration(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveCommitLatency(1, 0)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_commit_latency_seconds")
	if histogramCount(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("zero duration should still be observed")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Apply Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveApplyLatency(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveApplyLatency(7, 100*time.Microsecond)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_apply_latency_seconds")
	if histogramCount(t, mf, map[string]string{"shard_id": "7"}) != 1 {
		t.Error("expected 1 observation")
	}
}

func TestPrometheusMetricsCollector_ObserveApplyLatency_LargeShardID(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveApplyLatency(18446744073709551615, time.Second)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_apply_latency_seconds")
	if histogramCount(t, mf, map[string]string{"shard_id": "18446744073709551615"}) != 1 {
		t.Error("max uint64 shard ID should be handled")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: ReadIndex Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveReadIndexLatency(t *testing.T) {
	c := NewPrometheusMetricsCollector()
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

func TestPrometheusMetricsCollector_ObserveReadIndexLatency_MultipleShard(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveReadIndexLatency(1, time.Millisecond)
	c.ObserveReadIndexLatency(2, 2*time.Millisecond)
	c.ObserveReadIndexLatency(2, 3*time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_read_index_latency_seconds")
	if histogramCount(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("shard 1 should have 1 observation")
	}
	if histogramCount(t, mf, map[string]string{"shard_id": "2"}) != 2 {
		t.Error("shard 2 should have 2 observations")
	}
}

func TestPrometheusMetricsCollector_ObserveReadIndexLatency_ZeroDuration(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveReadIndexLatency(1, 0)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_read_index_latency_seconds")
	if histogramCount(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("zero duration should still be observed")
	}
}

func TestPrometheusMetricsCollector_ObserveReadIndexLatency_LargeShardID(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveReadIndexLatency(18446744073709551615, time.Second)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_read_index_latency_seconds")
	if histogramCount(t, mf, map[string]string{"shard_id": "18446744073709551615"}) != 1 {
		t.Error("max uint64 shard ID should be handled")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Proposal Dropped
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncProposalDropped(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.IncProposalDropped(5)
	c.IncProposalDropped(5)
	c.IncProposalDropped(5)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposals_dropped_total")
	val := counterValue(t, mf, map[string]string{"shard_id": "5"})
	if val != 3 {
		t.Errorf("proposals_dropped = %f, want 3", val)
	}
}

func TestPrometheusMetricsCollector_IncProposalDropped_MultipleShard(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.IncProposalDropped(1)
	c.IncProposalDropped(2)
	c.IncProposalDropped(2)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposals_dropped_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("shard 1 should have 1 drop")
	}
	if counterValue(t, mf, map[string]string{"shard_id": "2"}) != 2 {
		t.Error("shard 2 should have 2 drops")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Proposal Batch Size
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveProposalBatchSize(t *testing.T) {
	c := NewPrometheusMetricsCollector()
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

func TestPrometheusMetricsCollector_ObserveProposalBatchSize_Zero(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveProposalBatchSize(1, 0)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_batch_size")
	if histogramCount(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("zero batch size should be recorded")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Commit Batch Size
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveCommitBatchSize(t *testing.T) {
	c := NewPrometheusMetricsCollector()
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

func TestPrometheusMetricsCollector_ObserveCommitBatchSize_Single(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveCommitBatchSize(1)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_commit_batch_size")
	if simpleHistogramCount(t, mf) != 1 {
		t.Error("expected 1 observation")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Elections
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncElection(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.IncElection(99)
	c.IncElection(99)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_elections_total")
	if counterValue(t, mf, map[string]string{"shard_id": "99"}) != 2 {
		t.Error("expected 2 elections")
	}
}

func TestPrometheusMetricsCollector_IncElection_MultipleShard(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.IncElection(1)
	c.IncElection(2)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_elections_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("shard 1 should have 1 election")
	}
	if counterValue(t, mf, map[string]string{"shard_id": "2"}) != 1 {
		t.Error("shard 2 should have 1 election")
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Snapshots
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncSnapshot(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.IncSnapshot(50)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_snapshots_total")
	if counterValue(t, mf, map[string]string{"shard_id": "50"}) != 1 {
		t.Error("expected 1 snapshot")
	}
}

func TestPrometheusMetricsCollector_IncSnapshot_Multiple(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	for i := range 5 {
		c.IncSnapshot(uint64(i))
	}

	mf := gatherMetric(t, c.Registry, "quicraft_raft_snapshots_total")
	for i := range 5 {
		labels := map[string]string{"shard_id": strconv.Itoa(i)}
		if counterValue(t, mf, labels) != 1 {
			t.Errorf("shard %d should have 1 snapshot", i)
		}
	}
}

// ---------------------------------------------------------------------------
// RaftMetrics: Piggyback Commits
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncPiggybackCommit(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.IncPiggybackCommit(60)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_piggyback_commits_total")
	if counterValue(t, mf, map[string]string{"shard_id": "60"}) != 1 {
		t.Error("expected 1 piggyback commit")
	}
}

func TestPrometheusMetricsCollector_IncPiggybackCommit_Multiple(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	for range 3 {
		c.IncPiggybackCommit(10)
	}

	mf := gatherMetric(t, c.Registry, "quicraft_raft_piggyback_commits_total")
	if counterValue(t, mf, map[string]string{"shard_id": "10"}) != 3 {
		t.Error("expected 3 piggyback commits")
	}
}

// ---------------------------------------------------------------------------
// LogDBMetrics: Write Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveWriteLatency(t *testing.T) {
	c := NewPrometheusMetricsCollector()
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

func TestPrometheusMetricsCollector_ObserveWriteLatency_Multiple(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveWriteLatency(time.Millisecond)
	c.ObserveWriteLatency(3 * time.Millisecond)
	c.ObserveWriteLatency(5 * time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_write_latency_seconds")
	if simpleHistogramCount(t, mf) != 3 {
		t.Error("expected 3 observations")
	}
}

// ---------------------------------------------------------------------------
// LogDBMetrics: Fsync Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveFsyncLatency(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveFsyncLatency(10 * time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_fsync_latency_seconds")
	if simpleHistogramCount(t, mf) != 1 {
		t.Error("expected 1 observation")
	}
}

func TestPrometheusMetricsCollector_ObserveFsyncLatency_SubMicrosecond(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveFsyncLatency(500 * time.Nanosecond)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_fsync_latency_seconds")
	if simpleHistogramCount(t, mf) != 1 {
		t.Error("sub-microsecond observation should be recorded")
	}
}

// ---------------------------------------------------------------------------
// LogDBMetrics: Compaction Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveCompactionLatency(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveCompactionLatency(500 * time.Millisecond)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_compaction_latency_seconds")
	if simpleHistogramCount(t, mf) != 1 {
		t.Error("expected 1 observation")
	}
}

func TestPrometheusMetricsCollector_ObserveCompactionLatency_Large(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveCompactionLatency(30 * time.Second)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_compaction_latency_seconds")
	sum := simpleHistogramSum(t, mf)
	if sum < 29.9 || sum > 30.1 {
		t.Errorf("compaction latency sum = %f, want ~30", sum)
	}
}

// ---------------------------------------------------------------------------
// LogDBMetrics: Write Bytes
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncWriteBytes(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.IncWriteBytes(4096)
	c.IncWriteBytes(8192)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_write_bytes_total")
	val := simpleCounterValue(t, mf)
	if val != 12288 {
		t.Errorf("write bytes = %f, want 12288", val)
	}
}

func TestPrometheusMetricsCollector_IncWriteBytes_Zero(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.IncWriteBytes(0)

	mf := gatherMetric(t, c.Registry, "quicraft_logdb_write_bytes_total")
	val := simpleCounterValue(t, mf)
	if val != 0 {
		t.Errorf("write bytes = %f, want 0", val)
	}
}

// ---------------------------------------------------------------------------
// TransportMetrics: Send Latency
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_ObserveSendLatency(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.ObserveSendLatency(250 * time.Microsecond)

	mf := gatherMetric(t, c.Registry, "quicraft_transport_send_latency_seconds")
	if simpleHistogramCount(t, mf) != 1 {
		t.Error("expected 1 observation")
	}
}

func TestPrometheusMetricsCollector_ObserveSendLatency_Multiple(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	for range 10 {
		c.ObserveSendLatency(time.Millisecond)
	}

	mf := gatherMetric(t, c.Registry, "quicraft_transport_send_latency_seconds")
	if simpleHistogramCount(t, mf) != 10 {
		t.Error("expected 10 observations")
	}
}

// ---------------------------------------------------------------------------
// TransportMetrics: Messages Sent
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncMessagesSent(t *testing.T) {
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()
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

func TestPrometheusMetricsCollector_IncMessagesReceived_Multiple(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.EnablePerMessageMetrics = true // Enable detailed per-message metrics
	for range 7 {
		c.IncMessagesReceived(1, 2, 0)
	}

	// Check aggregate counter
	mf := gatherMetric(t, c.Registry, "quicraft_transport_messages_received_total")
	if counterValue(t, mf, nil) != 7 {
		t.Error("expected 7 messages received (aggregate)")
	}

	// Check detailed counter
	mf = gatherMetric(t, c.Registry, "quicraft_transport_messages_received_detailed_total")
	labels := map[string]string{"shard_id": "1", "from": "2", "msg_type": "0"}
	if counterValue(t, mf, labels) != 7 {
		t.Error("expected 7 messages received (detailed)")
	}
}

// ---------------------------------------------------------------------------
// TransportMetrics: Snapshots Sent
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncSnapshotsSent(t *testing.T) {
	c := NewPrometheusMetricsCollector()
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

func TestPrometheusMetricsCollector_IncSnapshotsSent_Multiple(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.IncSnapshotsSent(1, 1024, time.Second)
	c.IncSnapshotsSent(1, 2048, 2*time.Second)

	mf := gatherMetric(t, c.Registry, "quicraft_transport_snapshots_sent_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1"}) != 2 {
		t.Error("expected 2 snapshots sent")
	}

	bytesMF := gatherMetric(t, c.Registry, "quicraft_transport_snapshots_sent_bytes")
	sum := histogramSum(t, bytesMF, map[string]string{"shard_id": "1"})
	if sum != 3072 {
		t.Errorf("total snapshot bytes sent = %f, want 3072", sum)
	}
}

// ---------------------------------------------------------------------------
// TransportMetrics: Snapshots Received
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_IncSnapshotsReceived(t *testing.T) {
	c := NewPrometheusMetricsCollector()
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

func TestPrometheusMetricsCollector_IncSnapshotsReceived_MultipleShard(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.IncSnapshotsReceived(1, 512, time.Second)
	c.IncSnapshotsReceived(2, 1024, time.Second)

	mf := gatherMetric(t, c.Registry, "quicraft_transport_snapshots_received_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("shard 1 should have 1 snapshot received")
	}
	if counterValue(t, mf, map[string]string{"shard_id": "2"}) != 1 {
		t.Error("shard 2 should have 1 snapshot received")
	}
}

// ---------------------------------------------------------------------------
// Histogram bucket verification
// ---------------------------------------------------------------------------

func TestPrometheusMetricsCollector_LatencyBuckets(t *testing.T) {
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()
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
	c1 := NewPrometheusMetricsCollector()
	c2 := NewPrometheusMetricsCollector()

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
	_ = NewPrometheusMetricsCollector()

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
	c := NewPrometheusMetricsCollector()
	b.ResetTimer()
	for b.Loop() {
		c.IncElection(1)
	}
}

func BenchmarkPrometheusMetricsCollector_IncElection_PreBound(b *testing.B) {
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveProposalLatency(1, d)
	}
}

func BenchmarkPrometheusMetricsCollector_ObserveProposalLatency_PreBound(b *testing.B) {
	c := NewPrometheusMetricsCollector()
	c.RegisterShard(1)
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveProposalLatency(1, d)
	}
}

func BenchmarkPrometheusMetricsCollector_IncMessagesSent(b *testing.B) {
	c := NewPrometheusMetricsCollector()
	b.ResetTimer()
	for b.Loop() {
		c.IncMessagesSent(1, 2, 3)
	}
}

func BenchmarkPrometheusMetricsCollector_ObserveReadIndexLatency(b *testing.B) {
	c := NewPrometheusMetricsCollector()
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveReadIndexLatency(1, d)
	}
}

func BenchmarkPrometheusMetricsCollector_ObserveWriteLatency(b *testing.B) {
	c := NewPrometheusMetricsCollector()
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		c.ObserveWriteLatency(d)
	}
}

func BenchmarkPrometheusMetricsCollector_AllRaftMetrics(b *testing.B) {
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()
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
	c := NewPrometheusMetricsCollector()

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

func TestPrometheusMetricsCollector_UnregisterShard(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	c.RegisterShard(99)
	c.UnregisterShard(99)

	// After unregister, metrics should still work (fall back to slow path).
	c.ObserveProposalLatency(99, time.Millisecond)
	mf := gatherMetric(t, c.Registry, "quicraft_raft_proposal_latency_seconds")
	labels := map[string]string{"shard_id": "99"}
	if histogramCount(t, mf, labels) != 1 {
		t.Error("expected 1 observation after unregister")
	}
}

func TestPrometheusMetricsCollector_UnregisterShard_NeverRegistered(t *testing.T) {
	c := NewPrometheusMetricsCollector()
	// Unregistering a never-registered shard should not panic.
	c.UnregisterShard(123)
	c.UnregisterShard(456)
}

func TestPrometheusMetricsCollector_RegisterUnregisterCycle(t *testing.T) {
	c := NewPrometheusMetricsCollector()

	// Full lifecycle: register, use, unregister, re-register.
	c.RegisterShard(10)
	c.IncSnapshot(10)
	c.UnregisterShard(10)
	c.RegisterShard(10)
	c.IncSnapshot(10)

	mf := gatherMetric(t, c.Registry, "quicraft_raft_snapshots_total")
	if counterValue(t, mf, map[string]string{"shard_id": "10"}) != 2 {
		t.Error("expected 2 snapshots across register/unregister cycle")
	}
}

// ---------------------------------------------------------------------------
// SampledMetricsCollector
// ---------------------------------------------------------------------------

func TestNewSampledMetricsCollector(t *testing.T) {
	inner := NewPrometheusMetricsCollector()
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
	inner := NewPrometheusMetricsCollector()
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
	inner := NewPrometheusMetricsCollector()
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

func TestSampledMetricsCollector_ObserveApplyLatency_Sampling(t *testing.T) {
	inner := NewPrometheusMetricsCollector()
	s := NewSampledMetricsCollector(inner, 5) // 1 in 5

	// Record 50 observations; only 10 should be recorded.
	for range 50 {
		s.ObserveApplyLatency(2, time.Millisecond)
	}

	mf := gatherMetric(t, inner.Registry, "quicraft_raft_apply_latency_seconds")
	count := histogramCount(t, mf, map[string]string{"shard_id": "2"})
	if count != 10 {
		t.Errorf("expected 10 sampled observations, got %d", count)
	}
}

func TestSampledMetricsCollector_IncMessagesSent_Sampling(t *testing.T) {
	inner := NewPrometheusMetricsCollector()
	s := NewSampledMetricsCollector(inner, 20) // 1 in 20

	// Record 100 messages; only 5 should be recorded.
	for range 100 {
		s.IncMessagesSent(1, 2, 3)
	}

	mf := gatherMetric(t, inner.Registry, "quicraft_transport_messages_sent_total")
	val := counterValue(t, mf, nil)
	if val != 5 {
		t.Errorf("expected 5 sampled messages sent, got %f", val)
	}
}

func TestSampledMetricsCollector_IncMessagesReceived_Sampling(t *testing.T) {
	inner := NewPrometheusMetricsCollector()
	s := NewSampledMetricsCollector(inner, 25) // 1 in 25

	// Record 100 messages; only 4 should be recorded.
	for range 100 {
		s.IncMessagesReceived(1, 2, 3)
	}

	mf := gatherMetric(t, inner.Registry, "quicraft_transport_messages_received_total")
	val := counterValue(t, mf, nil)
	if val != 4 {
		t.Errorf("expected 4 sampled messages received, got %f", val)
	}
}

func TestSampledMetricsCollector_PassThroughMethods(t *testing.T) {
	inner := NewPrometheusMetricsCollector()
	s := NewSampledMetricsCollector(inner, 1000)

	// Non-sampled methods should always pass through.
	s.IncElection(1)
	s.IncSnapshot(1)
	s.ObserveProposalLatency(1, time.Millisecond)
	s.RegisterShard(1)
	s.UnregisterShard(1)

	mf := gatherMetric(t, inner.Registry, "quicraft_raft_elections_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("IncElection should pass through")
	}

	mf = gatherMetric(t, inner.Registry, "quicraft_raft_snapshots_total")
	if counterValue(t, mf, map[string]string{"shard_id": "1"}) != 1 {
		t.Error("IncSnapshot should pass through")
	}
}

func TestSampledMetricsCollector_ConcurrentSampling(t *testing.T) {
	inner := NewPrometheusMetricsCollector()
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

	// Primary goal: verify concurrent access does not panic.
	// The test passes if we reach this point without a panic/race.
	// Sampling count verification is secondary since the shared counter
	// leads to non-deterministic distribution across metric types.
}

func TestSampledMetricsCollector_SampleRateOne(t *testing.T) {
	inner := NewPrometheusMetricsCollector()
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
	inner := NewPrometheusMetricsCollector()
	s := NewSampledMetricsCollector(inner, 100)
	d := time.Millisecond
	b.ResetTimer()
	for b.Loop() {
		s.ObserveCommitLatency(1, d)
	}
}

func BenchmarkSampledMetricsCollector_IncMessagesSent(b *testing.B) {
	inner := NewPrometheusMetricsCollector()
	s := NewSampledMetricsCollector(inner, 100)
	b.ResetTimer()
	for b.Loop() {
		s.IncMessagesSent(1, 2, 3)
	}
}
