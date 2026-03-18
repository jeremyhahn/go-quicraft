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

package dragonboat_perf_test

import (
	"os"
	"testing"

	perf "github.com/jeremyhahn/go-quicraft/test/performance/dragonboat"
	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
)

// suite is the global benchmark suite populated by each benchmark and
// serialized to stdout by TestMain after all benchmarks complete.
var suite = perfresult.NewBenchmarkSuite("dragonboat", "v4.0.0")

func init() {
	suite.Config.TLS = true
}

// concurrentGoroutines is the number of goroutines for concurrent benchmarks.
const concurrentGoroutines = 8

// ---------------------------------------------------------------------------
// Propose Throughput (Sequential, WAL)
// ---------------------------------------------------------------------------

func BenchmarkProposeThroughput_Dragonboat(b *testing.B) {
	nh, cleanup := perf.SetupDragonboatHost(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatPropose(b, nh, rec)

	suite.AddResult(perfresult.ScenarioProposeSEQWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Concurrent 8 goroutines, WAL)
// ---------------------------------------------------------------------------

func BenchmarkProposeThroughput_Concurrent_Dragonboat(b *testing.B) {
	nh, cleanup := perf.SetupDragonboatHost(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatConcurrentProposeFixed(b, nh, concurrentGoroutines, rec)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete.
	suite.AddResult(perfresult.ScenarioProposeConc8WAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// SyncRead (Linearizable Read, WAL)
// ---------------------------------------------------------------------------

func BenchmarkSyncRead_Dragonboat(b *testing.B) {
	nh, cleanup := perf.SetupDragonboatHost(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatSyncRead(b, nh, rec)

	suite.AddResult(perfresult.ScenarioSyncReadWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// StaleRead (Non-linearizable Local Read, WAL)
// ---------------------------------------------------------------------------

func BenchmarkStaleRead_Dragonboat(b *testing.B) {
	nh, cleanup := perf.SetupDragonboatHost(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatStaleRead(b, nh, rec)

	suite.AddResult(perfresult.ScenarioStaleReadWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Election Time (WAL)
// ---------------------------------------------------------------------------

func BenchmarkElectionTime_Dragonboat(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatElection(b, rec)

	suite.AddResult(perfresult.ScenarioElectionWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Commit Latency (WAL)
// ---------------------------------------------------------------------------

func BenchmarkCommitLatency_Dragonboat(b *testing.B) {
	nh, cleanup := perf.SetupDragonboatHost(b)
	defer cleanup()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatCommitLatency(b, nh, rec)

	suite.AddResult(perfresult.ScenarioCommitLatencyWAL, b.N, b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Election Time (3N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkElectionTime_3N_Dragonboat(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatElectionCluster(b, 3, rec)

	suite.AddResult(perfresult.ScenarioElection3NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Sequential, 3N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkProposeThroughput_3N_Dragonboat(b *testing.B) {
	cluster := perf.SetupDragonboatCluster(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatProposeCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioProposeSEQ3NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Concurrent 8 goroutines, 3N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkProposeThroughput_Concurrent_3N_Dragonboat(b *testing.B) {
	cluster := perf.SetupDragonboatCluster(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatConcurrentProposeFixedCluster(b, cluster, concurrentGoroutines, rec)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete.
	suite.AddResult(perfresult.ScenarioProposeConc83NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Commit Latency (3N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkCommitLatency_3N_Dragonboat(b *testing.B) {
	cluster := perf.SetupDragonboatCluster(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatCommitLatencyCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioCommitLatency3NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// SyncRead (Linearizable Read, 3N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkSyncRead_3N_Dragonboat(b *testing.B) {
	cluster := perf.SetupDragonboatCluster(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatSyncReadCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioSyncRead3NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// StaleRead (Non-linearizable Local Read, 3N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkStaleRead_3N_Dragonboat(b *testing.B) {
	cluster := perf.SetupDragonboatCluster(b, 3)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatStaleReadCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioStaleRead3NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Election Time (5N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkElectionTime_5N_Dragonboat(b *testing.B) {
	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatElectionCluster(b, 5, rec)

	suite.AddResult(perfresult.ScenarioElection5NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Sequential, 5N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkProposeThroughput_5N_Dragonboat(b *testing.B) {
	cluster := perf.SetupDragonboatCluster(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatProposeCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioProposeSEQ5NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Propose Throughput (Concurrent 8 goroutines, 5N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkProposeThroughput_Concurrent_5N_Dragonboat(b *testing.B) {
	cluster := perf.SetupDragonboatCluster(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatConcurrentProposeFixedCluster(b, cluster, concurrentGoroutines, rec)

	// Use actual completed operations (rec.Count()) instead of b.N to
	// avoid inflated ops/sec when not all b.N iterations complete.
	suite.AddResult(perfresult.ScenarioProposeConc85NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// Commit Latency (5N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkCommitLatency_5N_Dragonboat(b *testing.B) {
	cluster := perf.SetupDragonboatCluster(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatCommitLatencyCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioCommitLatency5NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// SyncRead (Linearizable Read, 5N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkSyncRead_5N_Dragonboat(b *testing.B) {
	cluster := perf.SetupDragonboatCluster(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatSyncReadCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioSyncRead5NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// StaleRead (Non-linearizable Local Read, 5N Cluster, MutualTLS, WAL)
// ---------------------------------------------------------------------------

func BenchmarkStaleRead_5N_Dragonboat(b *testing.B) {
	cluster := perf.SetupDragonboatCluster(b, 5)
	defer cluster.Close()

	rec := perfresult.NewLatencyRecorder(b.N)
	perf.BenchDragonboatStaleReadCluster(b, cluster, rec)

	suite.AddResult(perfresult.ScenarioStaleRead5NWAL, rec.Count(), b.Elapsed(), rec)
}

// ---------------------------------------------------------------------------
// TestMain
// ---------------------------------------------------------------------------

func TestMain(m *testing.M) {
	exitCode := m.Run()

	_ = perfresult.WriteSuiteToStdout(suite)

	os.Exit(exitCode)
}
