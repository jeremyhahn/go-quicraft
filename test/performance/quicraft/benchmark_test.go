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

package quicraft_perf_test

import (
	"fmt"
	"os"
	"testing"

	perf "github.com/jeremyhahn/go-quicraft/test/performance/quicraft"

	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
)

// suite is the global benchmark suite populated by benchmarks and
// serialized to stdout by TestMain after all benchmarks complete.
var suite = newQuicRaftSuite()

func newQuicRaftSuite() *perfresult.BenchmarkSuite {
	s := perfresult.NewBenchmarkSuite("quicraft", "0.0.0")
	s.Config.TLS = true // mTLS is mandatory for all QUIC transport connections
	return s
}

// concurrentGoroutines is the number of goroutines for concurrent benchmarks.
const concurrentGoroutines = 8

// ---------------------------------------------------------------------------
// Propose Throughput (Sequential) -- WAL
// ---------------------------------------------------------------------------

func BenchmarkPropose_SEQ_WAL(b *testing.B) {
	h, cleanup := perf.SetupQuicRaftHost(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftPropose(b, h, rec)

	suite.AddResult(perfresult.ScenarioProposeSEQWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Sequential) -- Memory
// ---------------------------------------------------------------------------

func BenchmarkPropose_SEQ_Memory(b *testing.B) {
	h, cleanup := perf.SetupQuicRaftHostMemory(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftPropose(b, h, rec)

	suite.AddResult(perfresult.ScenarioProposeSEQMemory, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Concurrent/8) -- WAL
// ---------------------------------------------------------------------------

func BenchmarkPropose_Conc8_WAL(b *testing.B) {
	h, cleanup := perf.SetupQuicRaftHost(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftConcurrentProposeFixed(b, h, concurrentGoroutines, rec)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete.
	suite.AddResult(perfresult.ScenarioProposeConc8WAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Concurrent/8) -- Memory
// ---------------------------------------------------------------------------

func BenchmarkPropose_Conc8_Memory(b *testing.B) {
	h, cleanup := perf.SetupQuicRaftHostMemory(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftConcurrentProposeFixed(b, h, concurrentGoroutines, rec)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete.
	suite.AddResult(perfresult.ScenarioProposeConc8Memory, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// SyncRead (Linearizable Read) -- WAL
// ---------------------------------------------------------------------------

func BenchmarkSyncRead_WAL(b *testing.B) {
	h, cleanup := perf.SetupQuicRaftHost(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftSyncRead(b, h, rec)

	suite.AddResult(perfresult.ScenarioSyncReadWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// SyncRead (Linearizable Read) -- Memory
// ---------------------------------------------------------------------------

func BenchmarkSyncRead_Memory(b *testing.B) {
	h, cleanup := perf.SetupQuicRaftHostMemory(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftSyncRead(b, h, rec)

	suite.AddResult(perfresult.ScenarioSyncReadMemory, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// StaleRead (Non-linearizable Local Read) -- WAL
// ---------------------------------------------------------------------------

func BenchmarkStaleRead_WAL(b *testing.B) {
	h, cleanup := perf.SetupQuicRaftHost(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftStaleRead(b, h, rec)

	suite.AddResult(perfresult.ScenarioStaleReadWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// StaleRead (Non-linearizable Local Read) -- Memory
// ---------------------------------------------------------------------------

func BenchmarkStaleRead_Memory(b *testing.B) {
	h, cleanup := perf.SetupQuicRaftHostMemory(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftStaleRead(b, h, rec)

	suite.AddResult(perfresult.ScenarioStaleReadMemory, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Election Time -- WAL
// ---------------------------------------------------------------------------

func BenchmarkElection_WAL(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftElection(b, rec)

	suite.AddResult(perfresult.ScenarioElectionWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Election Time -- Memory
// ---------------------------------------------------------------------------

func BenchmarkElection_Memory(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftElectionMemory(b, rec)

	suite.AddResult(perfresult.ScenarioElectionMemory, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Commit Latency -- WAL
// ---------------------------------------------------------------------------

func BenchmarkCommitLatency_WAL(b *testing.B) {
	h, cleanup := perf.SetupQuicRaftHost(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftCommitLatency(b, h, rec)

	suite.AddResult(perfresult.ScenarioCommitLatencyWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Commit Latency -- Memory
// ---------------------------------------------------------------------------

func BenchmarkCommitLatency_Memory(b *testing.B) {
	h, cleanup := perf.SetupQuicRaftHostMemory(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftCommitLatency(b, h, rec)

	suite.AddResult(perfresult.ScenarioCommitLatencyMemory, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Election Time -- 3N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkElection_3N_WAL(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftElectionCluster(b, 3, rec)

	suite.AddResult(perfresult.ScenarioElection3NWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Election Time -- 5N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkElection_5N_WAL(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftElectionCluster(b, 5, rec)

	suite.AddResult(perfresult.ScenarioElection5NWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Sequential) -- 3N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkPropose_SEQ_3N_WAL(b *testing.B) {
	cluster := perf.SetupQuicRaftCluster(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftProposeCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioProposeSEQ3NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Sequential) -- 5N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkPropose_SEQ_5N_WAL(b *testing.B) {
	cluster := perf.SetupQuicRaftCluster(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftProposeCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioProposeSEQ5NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Concurrent/8) -- 3N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkPropose_Conc8_3N_WAL(b *testing.B) {
	cluster := perf.SetupQuicRaftCluster(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftConcurrentProposeFixedCluster(b, cluster, concurrentGoroutines, rec)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete.
	suite.AddResult(perfresult.ScenarioProposeConc83NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Concurrent/8) -- 5N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkPropose_Conc8_5N_WAL(b *testing.B) {
	cluster := perf.SetupQuicRaftCluster(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftConcurrentProposeFixedCluster(b, cluster, concurrentGoroutines, rec)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete.
	suite.AddResult(perfresult.ScenarioProposeConc85NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Commit Latency -- 3N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkCommitLatency_3N_WAL(b *testing.B) {
	cluster := perf.SetupQuicRaftCluster(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftCommitLatencyCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioCommitLatency3NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Commit Latency -- 5N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkCommitLatency_5N_WAL(b *testing.B) {
	cluster := perf.SetupQuicRaftCluster(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftCommitLatencyCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioCommitLatency5NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// SyncRead (Linearizable Read) -- 3N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkSyncRead_3N_WAL(b *testing.B) {
	cluster := perf.SetupQuicRaftCluster(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftSyncReadCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioSyncRead3NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// SyncRead (Linearizable Read) -- 5N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkSyncRead_5N_WAL(b *testing.B) {
	cluster := perf.SetupQuicRaftCluster(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftSyncReadCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioSyncRead5NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// StaleRead (Non-linearizable Local Read) -- 3N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkStaleRead_3N_WAL(b *testing.B) {
	cluster := perf.SetupQuicRaftCluster(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftStaleReadCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioStaleRead3NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// StaleRead (Non-linearizable Local Read) -- 5N Cluster (Real QUIC Transport)
// ---------------------------------------------------------------------------

func BenchmarkStaleRead_5N_WAL(b *testing.B) {
	cluster := perf.SetupQuicRaftCluster(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftStaleReadCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioStaleRead5NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// SyncRead (Linearizable Read) -- 3N Cluster + LeaseRead (QuicRaft-only)
// ---------------------------------------------------------------------------

func BenchmarkSyncRead_3N_WAL_LeaseRead(b *testing.B) {
	cluster := perf.SetupQuicRaftClusterLeaseRead(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftSyncReadCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioSyncRead3NWALLeaseRead, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// SyncRead (Linearizable Read) -- 5N Cluster + LeaseRead (QuicRaft-only)
// ---------------------------------------------------------------------------

func BenchmarkSyncRead_5N_WAL_LeaseRead(b *testing.B) {
	cluster := perf.SetupQuicRaftClusterLeaseRead(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchQuicRaftSyncReadCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioSyncRead5NWALLeaseRead, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// TestMain
// ---------------------------------------------------------------------------

func TestMain(m *testing.M) {
	exitCode := m.Run()

	fmt.Println()
	if err := perfresult.WriteSuiteToStdout(suite); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write benchmark suite: %v\n", err)
	}

	os.Exit(exitCode)
}
