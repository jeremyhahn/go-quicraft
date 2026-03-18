mod bench;
mod cluster;
mod sharded_disk_store;
mod network;
mod result;
mod scenarios;
mod store;
mod transport;
mod types;

use std::collections::HashMap;
use std::time::Duration;

use result::BenchmarkConfig;
use result::BenchmarkSuite;

// ── configuration constants ─────────────────────────────────────────────────

const PAYLOAD_SIZE: usize = 128;
const CONCURRENCY: usize = 8;

/// Default benchmark duration per scenario when no --benchtime is provided.
const DEFAULT_BENCHTIME: Duration = Duration::from_secs(30);

// ── benchtime parsing ───────────────────────────────────────────────────────

/// Parse the benchmark duration from CLI args.
///
/// Supports both the Go test harness format (`-test.benchtime=30s`) and
/// direct invocation (`--benchtime=30s`).  Falls back to DEFAULT_BENCHTIME.
fn parse_benchtime() -> Duration {
    for arg in std::env::args() {
        if let Some(val) = arg.strip_prefix("-test.benchtime=") {
            return parse_duration_str(val);
        }
        if let Some(val) = arg.strip_prefix("--benchtime=") {
            return parse_duration_str(val);
        }
    }
    DEFAULT_BENCHTIME
}

/// Parse a Go-style duration string into a `Duration`.
///
/// Supports: "30s", "2m", "1.5s", or bare seconds like "30".
fn parse_duration_str(s: &str) -> Duration {
    if let Some(secs) = s.strip_suffix('s') {
        if let Ok(n) = secs.parse::<f64>() {
            return Duration::from_secs_f64(n);
        }
    }
    if let Some(mins) = s.strip_suffix('m') {
        if let Ok(n) = mins.parse::<f64>() {
            return Duration::from_secs_f64(n * 60.0);
        }
    }
    if let Ok(n) = s.parse::<f64>() {
        return Duration::from_secs_f64(n);
    }
    eprintln!("[openraft-bench] WARNING: could not parse benchtime '{s}', using default");
    DEFAULT_BENCHTIME
}

// ── entry point ─────────────────────────────────────────────────────────────

fn main() {
    let benchtime = parse_benchtime();

    eprintln!(
        "[openraft-bench] benchtime={:.1}s per scenario ({} scenarios = ~{:.0}s total)",
        benchtime.as_secs_f64(),
        16,
        benchtime.as_secs_f64() * 16.0,
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime build failed");

    let mut results: HashMap<String, result::ScenarioResult> = HashMap::new();

    rt.block_on(async {
        // ── single-node (memory-only) scenarios ─────────────────────────────

        eprintln!("[openraft-bench] election_memory ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_election(benchtime).await;
        eprintln!(
            "[openraft-bench] election_memory: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert("election_memory".to_string(), r);

        eprintln!("[openraft-bench] propose_seq_memory ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_propose_sequential(benchtime, PAYLOAD_SIZE).await;
        eprintln!(
            "[openraft-bench] propose_seq_memory: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert("propose_seq_memory".to_string(), r);

        eprintln!(
            "[openraft-bench] propose_conc{CONCURRENCY}_memory ({:.0}s)",
            benchtime.as_secs_f64()
        );
        let r = scenarios::bench_propose_concurrent(benchtime, PAYLOAD_SIZE, CONCURRENCY).await;
        eprintln!(
            "[openraft-bench] propose_conc{CONCURRENCY}_memory: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert(format!("propose_conc{CONCURRENCY}_memory"), r);

        eprintln!("[openraft-bench] commit_latency_memory ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_commit_latency(benchtime, PAYLOAD_SIZE).await;
        eprintln!(
            "[openraft-bench] commit_latency_memory: {} ops, mean={}us  p99={}us",
            r.ops, r.latency.mean_ns / 1_000, r.latency.p99_ns / 1_000,
        );
        results.insert("commit_latency_memory".to_string(), r);

        // ── single-node WAL (16-shard WAL with parallel fsync) ──────────────

        eprintln!("[openraft-bench] election_wal ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_election_wal(benchtime).await;
        eprintln!(
            "[openraft-bench] election_wal: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert("election_wal".to_string(), r);

        eprintln!("[openraft-bench] propose_seq_wal ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_propose_sequential_wal(benchtime, PAYLOAD_SIZE).await;
        eprintln!(
            "[openraft-bench] propose_seq_wal: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert("propose_seq_wal".to_string(), r);

        eprintln!(
            "[openraft-bench] propose_conc{CONCURRENCY}_wal ({:.0}s)",
            benchtime.as_secs_f64()
        );
        let r = scenarios::bench_propose_concurrent_wal(benchtime, PAYLOAD_SIZE, CONCURRENCY).await;
        eprintln!(
            "[openraft-bench] propose_conc{CONCURRENCY}_wal: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert(format!("propose_conc{CONCURRENCY}_wal"), r);

        eprintln!("[openraft-bench] commit_latency_wal ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_commit_latency_wal(benchtime, PAYLOAD_SIZE).await;
        eprintln!(
            "[openraft-bench] commit_latency_wal: {} ops, mean={}us  p99={}us",
            r.ops, r.latency.mean_ns / 1_000, r.latency.p99_ns / 1_000,
        );
        results.insert("commit_latency_wal".to_string(), r);

        // ── 3-node TCP+TLS scenarios ────────────────────────────────────────

        eprintln!("[openraft-bench] election_3n_wal ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_election_cluster(benchtime, 3).await;
        eprintln!(
            "[openraft-bench] election_3n_wal: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert("election_3n_wal".to_string(), r);

        eprintln!("[openraft-bench] propose_seq_3n_wal ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_propose_sequential_cluster(benchtime, PAYLOAD_SIZE, 3).await;
        eprintln!(
            "[openraft-bench] propose_seq_3n_wal: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert("propose_seq_3n_wal".to_string(), r);

        eprintln!(
            "[openraft-bench] propose_conc{CONCURRENCY}_3n_wal ({:.0}s)",
            benchtime.as_secs_f64()
        );
        let r = scenarios::bench_propose_concurrent_cluster(
            benchtime,
            PAYLOAD_SIZE,
            CONCURRENCY,
            3,
        )
        .await;
        eprintln!(
            "[openraft-bench] propose_conc{CONCURRENCY}_3n_wal: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert(format!("propose_conc{CONCURRENCY}_3n_wal"), r);

        eprintln!("[openraft-bench] commit_latency_3n_wal ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_commit_latency_cluster(benchtime, PAYLOAD_SIZE, 3).await;
        eprintln!(
            "[openraft-bench] commit_latency_3n_wal: {} ops, mean={}us  p99={}us",
            r.ops, r.latency.mean_ns / 1_000, r.latency.p99_ns / 1_000,
        );
        results.insert("commit_latency_3n_wal".to_string(), r);

        // ── 5-node TCP+TLS scenarios ────────────────────────────────────────

        eprintln!("[openraft-bench] election_5n_wal ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_election_cluster(benchtime, 5).await;
        eprintln!(
            "[openraft-bench] election_5n_wal: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert("election_5n_wal".to_string(), r);

        eprintln!("[openraft-bench] propose_seq_5n_wal ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_propose_sequential_cluster(benchtime, PAYLOAD_SIZE, 5).await;
        eprintln!(
            "[openraft-bench] propose_seq_5n_wal: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert("propose_seq_5n_wal".to_string(), r);

        eprintln!(
            "[openraft-bench] propose_conc{CONCURRENCY}_5n_wal ({:.0}s)",
            benchtime.as_secs_f64()
        );
        let r = scenarios::bench_propose_concurrent_cluster(
            benchtime,
            PAYLOAD_SIZE,
            CONCURRENCY,
            5,
        )
        .await;
        eprintln!(
            "[openraft-bench] propose_conc{CONCURRENCY}_5n_wal: {} ops, {:.0} ops/sec  p99={}us",
            r.ops, r.ops_per_sec, r.latency.p99_ns / 1_000,
        );
        results.insert(format!("propose_conc{CONCURRENCY}_5n_wal"), r);

        eprintln!("[openraft-bench] commit_latency_5n_wal ({:.0}s)", benchtime.as_secs_f64());
        let r = scenarios::bench_commit_latency_cluster(benchtime, PAYLOAD_SIZE, 5).await;
        eprintln!(
            "[openraft-bench] commit_latency_5n_wal: {} ops, mean={}us  p99={}us",
            r.ops, r.latency.mean_ns / 1_000, r.latency.p99_ns / 1_000,
        );
        results.insert("commit_latency_5n_wal".to_string(), r);
    });

    let suite = BenchmarkSuite {
        system: "openraft".to_string(),
        version: "v0.9".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        config: BenchmarkConfig {
            payload_size: PAYLOAD_SIZE,
            concurrent_goroutines: CONCURRENCY,
            tls: true,
            rtt_ms: 1,
            election_rtt: 10,
            heartbeat_rtt: 1,
            node_count: 1,
        },
        results,
    };

    println!("===PERF_JSON_BEGIN===");
    println!(
        "{}",
        serde_json::to_string_pretty(&suite).expect("JSON serialisation is infallible")
    );
    println!("===PERF_JSON_END===");
}
