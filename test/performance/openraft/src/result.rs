use serde::Serialize;
use std::collections::HashMap;

/// Top-level benchmark suite output, matching the JSON schema expected by
/// the Go benchmark harness that parses results between sentinel markers.
#[derive(Serialize)]
pub struct BenchmarkSuite {
    pub system: String,
    pub version: String,
    pub timestamp: String,
    pub config: BenchmarkConfig,
    pub results: HashMap<String, ScenarioResult>,
}

/// Static parameters that describe the benchmark run configuration.
#[derive(Serialize)]
pub struct BenchmarkConfig {
    pub payload_size: usize,
    pub concurrent_goroutines: usize,
    pub tls: bool,
    pub rtt_ms: u64,
    pub election_rtt: u64,
    pub heartbeat_rtt: u64,
    pub node_count: usize,
}

/// Aggregated result for a single benchmark scenario.
#[derive(Serialize)]
pub struct ScenarioResult {
    pub ops: usize,
    pub duration_ns: u64,
    pub ops_per_sec: f64,
    pub latency: LatencyStats,
}

/// Percentile latency breakdown for a benchmark scenario.
#[derive(Serialize)]
pub struct LatencyStats {
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub min_ns: u64,
    pub max_ns: u64,
    pub mean_ns: u64,
    pub count: u64,
}
