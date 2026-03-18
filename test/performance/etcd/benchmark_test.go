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

//go:build benchmark

package etcd_perf

import (
	"os"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
)

// defaultPayloadSize is the proposal payload size used across all
// throughput and latency benchmarks.
const defaultPayloadSize = 128

// concurrentGoroutines is the number of goroutines for concurrent
// proposal benchmarks.
const concurrentGoroutines = 8

// suite is the global BenchmarkSuite populated by benchmark functions
// and serialized to stdout in TestMain.
var suite = perfresult.NewBenchmarkSuite("etcd", "v3.6.0")

// ---------------------------------------------------------------------------
// Throughput Benchmarks (WAL)
// ---------------------------------------------------------------------------

// BenchmarkProposeThroughput_EtcdRaft measures sequential proposal
// throughput for etcd/raft with WAL persistence.
func BenchmarkProposeThroughput_EtcdRaft(b *testing.B) {
	tn := setupEtcdRaftNode(b)
	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftPropose(b, tn, defaultPayloadSize, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioProposeSEQWAL, b.N, dur, rec)

	b.ReportMetric(float64(b.N)/dur.Seconds(), "ops/sec")
	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// ---------------------------------------------------------------------------
// Throughput Benchmarks (Memory)
// ---------------------------------------------------------------------------

// BenchmarkProposeThroughput_EtcdRaft_Memory measures sequential proposal
// throughput for etcd/raft with in-memory storage only (no WAL).
func BenchmarkProposeThroughput_EtcdRaft_Memory(b *testing.B) {
	tn := setupEtcdRaftNodeMemory(b)
	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftPropose(b, tn, defaultPayloadSize, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioProposeSEQMemory, b.N, dur, rec)

	b.ReportMetric(float64(b.N)/dur.Seconds(), "ops/sec")
	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// ---------------------------------------------------------------------------
// Concurrent Throughput Benchmarks (WAL)
// ---------------------------------------------------------------------------

// BenchmarkProposeThroughput_Concurrent_EtcdRaft measures concurrent
// proposal throughput for etcd/raft with 8 goroutines and WAL persistence.
func BenchmarkProposeThroughput_Concurrent_EtcdRaft(b *testing.B) {
	tn := setupEtcdRaftNode(b)
	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftConcurrentPropose(b, tn, defaultPayloadSize, concurrentGoroutines, rec)
	dur := time.Since(start)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete under
	// high replication latency.
	actualOps := rec.Count()
	suite.AddResult(perfresult.ScenarioProposeConc8WAL, actualOps, dur, rec)

	b.ReportMetric(float64(actualOps)/dur.Seconds(), "ops/sec")
	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// ---------------------------------------------------------------------------
// Concurrent Throughput Benchmarks (Memory)
// ---------------------------------------------------------------------------

// BenchmarkProposeThroughput_Concurrent_EtcdRaft_Memory measures concurrent
// proposal throughput for etcd/raft with in-memory storage only (no WAL)
// and 8 goroutines.
func BenchmarkProposeThroughput_Concurrent_EtcdRaft_Memory(b *testing.B) {
	tn := setupEtcdRaftNodeMemory(b)
	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftConcurrentPropose(b, tn, defaultPayloadSize, concurrentGoroutines, rec)
	dur := time.Since(start)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete under
	// high replication latency.
	actualOps := rec.Count()
	suite.AddResult(perfresult.ScenarioProposeConc8Memory, actualOps, dur, rec)

	b.ReportMetric(float64(actualOps)/dur.Seconds(), "ops/sec")
	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// ---------------------------------------------------------------------------
// Commit Latency Benchmarks (WAL)
// ---------------------------------------------------------------------------

// BenchmarkCommitLatency_EtcdRaft measures per-proposal commit latency
// for etcd/raft with WAL persistence (propose to committed entry applied).
func BenchmarkCommitLatency_EtcdRaft(b *testing.B) {
	tn := setupEtcdRaftNode(b)
	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftCommitLatency(b, tn, defaultPayloadSize, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioCommitLatencyWAL, b.N, dur, rec)

	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P90().Microseconds()), "p90-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// ---------------------------------------------------------------------------
// Commit Latency Benchmarks (Memory)
// ---------------------------------------------------------------------------

// BenchmarkCommitLatency_EtcdRaft_Memory measures per-proposal commit
// latency for etcd/raft with in-memory storage only (no WAL).
func BenchmarkCommitLatency_EtcdRaft_Memory(b *testing.B) {
	tn := setupEtcdRaftNodeMemory(b)
	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftCommitLatency(b, tn, defaultPayloadSize, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioCommitLatencyMemory, b.N, dur, rec)

	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P90().Microseconds()), "p90-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// ---------------------------------------------------------------------------
// Election Time Benchmarks (WAL)
// ---------------------------------------------------------------------------

// BenchmarkElectionTime_EtcdRaft measures the time from StartNode to
// leader appearing via natural election timeout for etcd/raft with WAL.
func BenchmarkElectionTime_EtcdRaft(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftElection(b, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioElectionWAL, b.N, dur, rec)

	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// ---------------------------------------------------------------------------
// Election Time Benchmarks (Memory)
// ---------------------------------------------------------------------------

// BenchmarkElectionTime_EtcdRaft_Memory measures the time from StartNode
// to leader appearing via natural election timeout for etcd/raft with
// in-memory storage only (no WAL).
func BenchmarkElectionTime_EtcdRaft_Memory(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftElectionMemory(b, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioElectionMemory, b.N, dur, rec)

	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// ---------------------------------------------------------------------------
// Multi-Node 3N Benchmarks (WAL + TCP+TLS Transport)
// ---------------------------------------------------------------------------

// BenchmarkElectionTime_3N_EtcdRaft measures the time from cluster creation
// to leader election in a 3-node etcd/raft cluster with TCP+TLS transport.
func BenchmarkElectionTime_3N_EtcdRaft(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftElectionCluster(b, 3, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioElection3NWAL, b.N, dur, rec)

	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// BenchmarkProposeThroughput_3N_EtcdRaft measures sequential proposal
// throughput for etcd/raft in a 3-node cluster with TCP+TLS transport.
func BenchmarkProposeThroughput_3N_EtcdRaft(b *testing.B) {
	cluster := SetupEtcdCluster(b, 3)
	b.Cleanup(func() { cluster.Close() })

	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftProposeCluster(b, cluster, defaultPayloadSize, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioProposeSEQ3NWAL, rec.Count(), dur, rec)

	b.ReportMetric(float64(rec.Count())/dur.Seconds(), "ops/sec")
	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// BenchmarkProposeThroughput_Concurrent_3N_EtcdRaft measures concurrent
// proposal throughput for etcd/raft in a 3-node cluster with 8 goroutines.
func BenchmarkProposeThroughput_Concurrent_3N_EtcdRaft(b *testing.B) {
	cluster := SetupEtcdCluster(b, 3)
	b.Cleanup(func() { cluster.Close() })

	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftConcurrentProposeCluster(b, cluster, defaultPayloadSize, concurrentGoroutines, rec)
	dur := time.Since(start)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete under
	// high replication latency.
	actualOps := rec.Count()
	suite.AddResult(perfresult.ScenarioProposeConc83NWAL, actualOps, dur, rec)

	b.ReportMetric(float64(actualOps)/dur.Seconds(), "ops/sec")
	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// BenchmarkCommitLatency_3N_EtcdRaft measures per-proposal commit latency
// for etcd/raft in a 3-node cluster with TCP+TLS transport and WAL.
func BenchmarkCommitLatency_3N_EtcdRaft(b *testing.B) {
	cluster := SetupEtcdCluster(b, 3)
	b.Cleanup(func() { cluster.Close() })

	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftCommitLatencyCluster(b, cluster, defaultPayloadSize, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioCommitLatency3NWAL, rec.Count(), dur, rec)

	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P90().Microseconds()), "p90-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// ---------------------------------------------------------------------------
// Multi-Node 5N Benchmarks (WAL + TCP+TLS Transport)
// ---------------------------------------------------------------------------

// BenchmarkElectionTime_5N_EtcdRaft measures the time from cluster creation
// to leader election in a 5-node etcd/raft cluster with TCP+TLS transport.
func BenchmarkElectionTime_5N_EtcdRaft(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftElectionCluster(b, 5, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioElection5NWAL, b.N, dur, rec)

	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// BenchmarkProposeThroughput_5N_EtcdRaft measures sequential proposal
// throughput for etcd/raft in a 5-node cluster with TCP+TLS transport.
func BenchmarkProposeThroughput_5N_EtcdRaft(b *testing.B) {
	cluster := SetupEtcdCluster(b, 5)
	b.Cleanup(func() { cluster.Close() })

	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftProposeCluster(b, cluster, defaultPayloadSize, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioProposeSEQ5NWAL, rec.Count(), dur, rec)

	b.ReportMetric(float64(rec.Count())/dur.Seconds(), "ops/sec")
	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// BenchmarkProposeThroughput_Concurrent_5N_EtcdRaft measures concurrent
// proposal throughput for etcd/raft in a 5-node cluster with 8 goroutines.
func BenchmarkProposeThroughput_Concurrent_5N_EtcdRaft(b *testing.B) {
	cluster := SetupEtcdCluster(b, 5)
	b.Cleanup(func() { cluster.Close() })

	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftConcurrentProposeCluster(b, cluster, defaultPayloadSize, concurrentGoroutines, rec)
	dur := time.Since(start)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete under
	// high replication latency.
	actualOps := rec.Count()
	suite.AddResult(perfresult.ScenarioProposeConc85NWAL, actualOps, dur, rec)

	b.ReportMetric(float64(actualOps)/dur.Seconds(), "ops/sec")
	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// BenchmarkCommitLatency_5N_EtcdRaft measures per-proposal commit latency
// for etcd/raft in a 5-node cluster with TCP+TLS transport and WAL.
func BenchmarkCommitLatency_5N_EtcdRaft(b *testing.B) {
	cluster := SetupEtcdCluster(b, 5)
	b.Cleanup(func() { cluster.Close() })

	rec := perfresult.NewLatencyRecorder(b.N)

	start := time.Now()
	benchEtcdRaftCommitLatencyCluster(b, cluster, defaultPayloadSize, rec)
	dur := time.Since(start)

	suite.AddResult(perfresult.ScenarioCommitLatency5NWAL, rec.Count(), dur, rec)

	b.ReportMetric(float64(rec.P50().Microseconds()), "p50-us")
	b.ReportMetric(float64(rec.P90().Microseconds()), "p90-us")
	b.ReportMetric(float64(rec.P99().Microseconds()), "p99-us")
}

// ---------------------------------------------------------------------------
// TestMain serializes the benchmark suite to stdout after all benchmarks.
// ---------------------------------------------------------------------------

func TestMain(m *testing.M) {
	exitCode := m.Run()

	if len(suite.Results) > 0 {
		perfresult.WriteSuiteToStdout(suite)
	}

	os.Exit(exitCode)
}
