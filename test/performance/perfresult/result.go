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

package perfresult

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

// Sentinel markers used to delimit JSON output in mixed test output.
const (
	SentinelBegin = "===PERF_JSON_BEGIN==="
	SentinelEnd   = "===PERF_JSON_END==="
)

// Standardized scenario key constants.
const (
	// Single-node (1N) scenarios.
	ScenarioElectionWAL    = "election_wal"
	ScenarioElectionMemory = "election_memory"

	ScenarioProposeSEQWAL    = "propose_seq_wal"
	ScenarioProposeSEQMemory = "propose_seq_memory"

	ScenarioProposeConc8WAL    = "propose_conc8_wal"
	ScenarioProposeConc8Memory = "propose_conc8_memory"

	ScenarioCommitLatencyWAL    = "commit_latency_wal"
	ScenarioCommitLatencyMemory = "commit_latency_memory"

	ScenarioSyncReadWAL    = "sync_read_wal"
	ScenarioSyncReadMemory = "sync_read_memory"

	ScenarioStaleReadWAL    = "stale_read_wal"
	ScenarioStaleReadMemory = "stale_read_memory"

	// Multi-node 3N scenarios (WAL-only, production-realistic).
	ScenarioElection3NWAL      = "election_3n_wal"
	ScenarioProposeSEQ3NWAL    = "propose_seq_3n_wal"
	ScenarioProposeConc83NWAL  = "propose_conc8_3n_wal"
	ScenarioCommitLatency3NWAL = "commit_latency_3n_wal"
	ScenarioSyncRead3NWAL      = "sync_read_3n_wal"
	ScenarioStaleRead3NWAL     = "stale_read_3n_wal"

	// Multi-node 5N scenarios (WAL-only, production-realistic).
	ScenarioElection5NWAL      = "election_5n_wal"
	ScenarioProposeSEQ5NWAL    = "propose_seq_5n_wal"
	ScenarioProposeConc85NWAL  = "propose_conc8_5n_wal"
	ScenarioCommitLatency5NWAL = "commit_latency_5n_wal"
	ScenarioSyncRead5NWAL      = "sync_read_5n_wal"
	ScenarioStaleRead5NWAL     = "stale_read_5n_wal"

	// QuicRaft-only LeaseRead variant scenarios.
	ScenarioSyncRead3NWALLeaseRead = "sync_read_3n_wal_leaseread"
	ScenarioSyncRead5NWALLeaseRead = "sync_read_5n_wal_leaseread"

	// KV database benchmark scenarios (1N).
	ScenarioKVPutSEQ      = "kv_put_seq"
	ScenarioKVPutConc8    = "kv_put_conc8"
	ScenarioKVGetSEQ      = "kv_get_seq"
	ScenarioKVGetConc8    = "kv_get_conc8"
	ScenarioKVDeleteSEQ   = "kv_delete_seq"
	ScenarioKVDeleteConc8 = "kv_delete_conc8"
	ScenarioKVBatchPut10K = "kv_batch_put_10k"
	ScenarioKVScan10K     = "kv_scan_10k"

	// KV database benchmark scenarios (3N).
	ScenarioKVPutSEQ3N   = "kv_put_seq_3n"
	ScenarioKVPutConc83N = "kv_put_conc8_3n"
	ScenarioKVGetSEQ3N   = "kv_get_seq_3n"
	ScenarioKVGetConc83N = "kv_get_conc8_3n"

	// etcd API benchmark scenarios (1N).
	ScenarioEtcdPutSEQ       = "etcd_put_seq"
	ScenarioEtcdPutConc8     = "etcd_put_conc8"
	ScenarioEtcdGetSEQ       = "etcd_get_seq"
	ScenarioEtcdGetConc8     = "etcd_get_conc8"
	ScenarioEtcdDeleteSEQ    = "etcd_delete_seq"
	ScenarioEtcdDeleteConc8  = "etcd_delete_conc8"
	ScenarioEtcdTxnSEQ       = "etcd_txn_seq"
	ScenarioEtcdWatchLatency = "etcd_watch_latency"

	// gRPC benchmark scenarios (1N).
	ScenarioKVGRPCPutSEQ      = "kv_grpc_put_seq"
	ScenarioKVGRPCPutConc8    = "kv_grpc_put_conc8"
	ScenarioKVGRPCGetSEQ      = "kv_grpc_get_seq"
	ScenarioKVGRPCGetConc8    = "kv_grpc_get_conc8"
	ScenarioKVGRPCDeleteSEQ   = "kv_grpc_delete_seq"
	ScenarioKVGRPCDeleteConc8 = "kv_grpc_delete_conc8"
	ScenarioKVGRPCBatchPut10K = "kv_grpc_batch_put_10k"
	ScenarioKVGRPCScan10K     = "kv_grpc_scan_10k"

	// Coalesced gRPC benchmark scenarios (1N).
	ScenarioKVGRPCPutCoalesced    = "kv_grpc_put_coalesced"
	ScenarioKVGRPCGetCoalesced    = "kv_grpc_get_coalesced"
	ScenarioKVGRPCDeleteCoalesced = "kv_grpc_delete_coalesced"

	// Native QUIC benchmark scenarios (1N).
	ScenarioKVQUICPutSEQ      = "kv_quic_put_seq"
	ScenarioKVQUICPutConc8    = "kv_quic_put_conc8"
	ScenarioKVQUICGetSEQ      = "kv_quic_get_seq"
	ScenarioKVQUICGetConc8    = "kv_quic_get_conc8"
	ScenarioKVQUICDeleteSEQ   = "kv_quic_delete_seq"
	ScenarioKVQUICDeleteConc8 = "kv_quic_delete_conc8"
)

// CoreScenarios lists the scenario keys that every system should implement.
var CoreScenarios = []string{
	ScenarioElectionWAL,
	ScenarioElectionMemory,
	ScenarioProposeSEQWAL,
	ScenarioProposeSEQMemory,
	ScenarioProposeConc8WAL,
	ScenarioProposeConc8Memory,
	ScenarioCommitLatencyWAL,
	ScenarioCommitLatencyMemory,
}

// AllScenarios lists all standardized scenario keys in display order.
var AllScenarios = []string{
	// Single-node (1N) scenarios.
	ScenarioElectionWAL,
	ScenarioElectionMemory,
	ScenarioProposeSEQWAL,
	ScenarioProposeSEQMemory,
	ScenarioProposeConc8WAL,
	ScenarioProposeConc8Memory,
	ScenarioCommitLatencyWAL,
	ScenarioCommitLatencyMemory,
	ScenarioSyncReadWAL,
	ScenarioSyncReadMemory,
	ScenarioStaleReadWAL,
	ScenarioStaleReadMemory,

	// Multi-node 3N scenarios.
	ScenarioElection3NWAL,
	ScenarioProposeSEQ3NWAL,
	ScenarioProposeConc83NWAL,
	ScenarioCommitLatency3NWAL,
	ScenarioSyncRead3NWAL,
	ScenarioSyncRead3NWALLeaseRead,
	ScenarioStaleRead3NWAL,

	// Multi-node 5N scenarios.
	ScenarioElection5NWAL,
	ScenarioProposeSEQ5NWAL,
	ScenarioProposeConc85NWAL,
	ScenarioCommitLatency5NWAL,
	ScenarioSyncRead5NWAL,
	ScenarioSyncRead5NWALLeaseRead,
	ScenarioStaleRead5NWAL,

	// KV database benchmark scenarios (1N).
	ScenarioKVPutSEQ, ScenarioKVPutConc8,
	ScenarioKVGetSEQ, ScenarioKVGetConc8,
	ScenarioKVDeleteSEQ, ScenarioKVDeleteConc8,
	ScenarioKVBatchPut10K, ScenarioKVScan10K,
	// KV 3N.
	ScenarioKVPutSEQ3N, ScenarioKVPutConc83N,
	ScenarioKVGetSEQ3N, ScenarioKVGetConc83N,
	// etcd.
	ScenarioEtcdPutSEQ, ScenarioEtcdPutConc8,
	ScenarioEtcdGetSEQ, ScenarioEtcdGetConc8,
	ScenarioEtcdDeleteSEQ, ScenarioEtcdDeleteConc8,
	ScenarioEtcdTxnSEQ, ScenarioEtcdWatchLatency,
	// gRPC.
	ScenarioKVGRPCPutSEQ, ScenarioKVGRPCPutConc8,
	ScenarioKVGRPCGetSEQ, ScenarioKVGRPCGetConc8,
	ScenarioKVGRPCDeleteSEQ, ScenarioKVGRPCDeleteConc8,
	ScenarioKVGRPCBatchPut10K, ScenarioKVGRPCScan10K,
	// Coalesced gRPC.
	ScenarioKVGRPCPutCoalesced, ScenarioKVGRPCGetCoalesced, ScenarioKVGRPCDeleteCoalesced,
	// Native QUIC.
	ScenarioKVQUICPutSEQ, ScenarioKVQUICPutConc8,
	ScenarioKVQUICGetSEQ, ScenarioKVQUICGetConc8,
	ScenarioKVQUICDeleteSEQ, ScenarioKVQUICDeleteConc8,
}

// BenchmarkSuite holds the complete output of a benchmark run for one system.
type BenchmarkSuite struct {
	System    string                     `json:"system"`
	Version   string                     `json:"version"`
	Timestamp string                     `json:"timestamp"`
	Config    BenchmarkConfig            `json:"config"`
	Results   map[string]*ScenarioResult `json:"results"`
}

// BenchmarkConfig holds the configuration parameters used for a benchmark run.
type BenchmarkConfig struct {
	PayloadSize          int  `json:"payload_size"`
	ConcurrentGoroutines int  `json:"concurrent_goroutines"`
	TLS                  bool `json:"tls"`
	RTTMs                int  `json:"rtt_ms"`
	ElectionRTT          int  `json:"election_rtt"`
	HeartbeatRTT         int  `json:"heartbeat_rtt"`
	NodeCount            int  `json:"node_count"`
}

// ScenarioResult holds the measured performance of a single benchmark scenario.
type ScenarioResult struct {
	Ops        int          `json:"ops"`
	DurationNs int64        `json:"duration_ns"`
	OpsPerSec  float64      `json:"ops_per_sec"`
	Latency    LatencyStats `json:"latency"`
}

// LatencyStats holds percentile latency values in nanoseconds.
type LatencyStats struct {
	P50Ns  int64 `json:"p50_ns"`
	P90Ns  int64 `json:"p90_ns"`
	P95Ns  int64 `json:"p95_ns"`
	P99Ns  int64 `json:"p99_ns"`
	P999Ns int64 `json:"p999_ns"`
	MinNs  int64 `json:"min_ns"`
	MaxNs  int64 `json:"max_ns"`
	MeanNs int64 `json:"mean_ns"`
	Count  int64 `json:"count"`
}

// NewBenchmarkSuite creates a new BenchmarkSuite with the given system name
// and default configuration.
func NewBenchmarkSuite(system, version string) *BenchmarkSuite {
	return &BenchmarkSuite{
		System:    system,
		Version:   version,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Config: BenchmarkConfig{
			PayloadSize:          128,
			ConcurrentGoroutines: 8,
			TLS:                  false,
			RTTMs:                1,
			ElectionRTT:          10,
			HeartbeatRTT:         1,
			NodeCount:            1,
		},
		Results: make(map[string]*ScenarioResult),
	}
}

// AddResult records a scenario result computed from ops, duration, and
// a LatencyRecorder.
func (s *BenchmarkSuite) AddResult(scenario string, ops int, dur time.Duration, rec *LatencyRecorder) {
	opsPerSec := float64(0)
	if dur > 0 {
		opsPerSec = float64(ops) / dur.Seconds()
	}

	result := &ScenarioResult{
		Ops:        ops,
		DurationNs: dur.Nanoseconds(),
		OpsPerSec:  opsPerSec,
	}

	if rec != nil && rec.Count() > 0 {
		result.Latency = rec.Stats()
	}

	s.Results[scenario] = result
}

// WriteSuiteToStdout writes the BenchmarkSuite JSON to stdout, wrapped
// in sentinel markers so the orchestrator can extract it from mixed output.
func WriteSuiteToStdout(s *BenchmarkSuite) error {
	return WriteSuiteTo(os.Stdout, s)
}

// WriteSuiteTo writes the BenchmarkSuite JSON to the given writer,
// wrapped in sentinel markers.
func WriteSuiteTo(w io.Writer, s *BenchmarkSuite) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return &ErrParseFailure{Detail: fmt.Sprintf("marshal: %v", err)}
	}

	if _, err := fmt.Fprintln(w, SentinelBegin); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, SentinelEnd); err != nil {
		return err
	}
	return nil
}

// ReadSuiteFromOutput extracts and parses a BenchmarkSuite from output
// that contains sentinel markers.
func ReadSuiteFromOutput(output []byte) (*BenchmarkSuite, error) {
	beginIdx := bytes.Index(output, []byte(SentinelBegin))
	if beginIdx < 0 {
		return nil, &ErrNoSentinel{}
	}
	endIdx := bytes.Index(output[beginIdx:], []byte(SentinelEnd))
	if endIdx < 0 {
		return nil, &ErrNoSentinel{}
	}

	jsonData := output[beginIdx+len(SentinelBegin) : beginIdx+endIdx]
	jsonData = bytes.TrimSpace(jsonData)

	var suite BenchmarkSuite
	if err := json.Unmarshal(jsonData, &suite); err != nil {
		return nil, &ErrParseFailure{Detail: fmt.Sprintf("unmarshal: %v", err)}
	}

	return &suite, nil
}
