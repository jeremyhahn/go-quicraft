//! Benchmark scenarios for single-node and multi-node OpenRaft clusters.
//!
//! All scenarios are **time-based**: they run continuously for the specified
//! `Duration` and report ops/sec plus latency percentiles.  This matches Go's
//! `testing.B` auto-calibration and ensures fair cross-system comparisons.
//!
//! Single-node scenarios come in two flavours:
//!   - `*_memory`  -- in-memory storage, no-op network (framework overhead only)
//!   - `*_wal`     -- 16-shard disk WAL with parallel fsync, no-op network
//!
//! Multi-node scenarios use a real TCP+TLS transport over loopback with a
//! 16-shard disk WAL and are labelled `*_{n}n_wal`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::BasicNode;
use openraft::Raft;

use crate::bench::LatencyRecorder;
use crate::bench::Timer;
use crate::cluster::setup_cluster;
use crate::sharded_disk_store::new_sharded_disk_store;
use crate::network::NoopNetworkFactory;
use crate::result::ScenarioResult;
use crate::store::new_mem_store;
use crate::store::BenchRequest;
use crate::types::TypeConfig;

// ── constants ────────────────────────────────────────────────────────────────

/// Fixed warmup count for commit latency scenarios (reaches steady state).
const WARMUP_OPS: usize = 100;

/// Initial capacity hint for the latency recorder sample vector.
const RECORDER_CAP: usize = 65536;

/// Per-task sample capacity hint for concurrent benchmarks.
const TASK_SAMPLE_CAP: usize = 8192;

// ── type aliases ─────────────────────────────────────────────────────────────

type BenchRaft = Raft<TypeConfig>;

// ── shared raft config ──────────────────────────────────────────────────────

fn raft_config() -> Arc<openraft::Config> {
    Arc::new(
        openraft::Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        }
        .validate()
        .expect("raft config is valid"),
    )
}

// ── single-node helpers (in-memory) ─────────────────────────────────────────

async fn make_raft() -> BenchRaft {
    let (log_store, state_machine) = new_mem_store();
    let raft = Raft::new(1u64, raft_config(), NoopNetworkFactory::default(), log_store, state_machine)
        .await
        .expect("Raft::new failed");

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members)
        .await
        .expect("cluster initialisation failed");

    raft
}

// ── single-node helpers (16-shard WAL with parallel fsync) ──────────────────

async fn make_raft_wal() -> BenchRaft {
    let (log_store, state_machine) = new_sharded_disk_store();
    let raft = Raft::new(1u64, raft_config(), NoopNetworkFactory::default(), log_store, state_machine)
        .await
        .expect("Raft::new failed");

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members)
        .await
        .expect("cluster initialisation failed");

    raft
}

// ── shared helpers ──────────────────────────────────────────────────────────

async fn wait_for_leader(raft: &BenchRaft) {
    let mut rx = raft.metrics();
    loop {
        if rx.borrow().current_leader == Some(1) {
            break;
        }
        if rx.changed().await.is_err() {
            panic!("raft metrics channel closed before leader was elected");
        }
    }
}

async fn boot_single_node() -> BenchRaft {
    let raft = make_raft().await;
    wait_for_leader(&raft).await;
    raft
}

async fn boot_single_node_wal() -> BenchRaft {
    let raft = make_raft_wal().await;
    wait_for_leader(&raft).await;
    raft
}

fn payload(size: usize) -> Vec<u8> {
    vec![0u8; size]
}

// ── build_result ─────────────────────────────────────────────────────────────

fn build_result(ops: usize, total_ns: u64, mut recorder: LatencyRecorder) -> ScenarioResult {
    let ops_per_sec = if total_ns == 0 {
        0.0
    } else {
        ops as f64 / (total_ns as f64 / 1_000_000_000.0)
    };
    ScenarioResult {
        ops,
        duration_ns: total_ns,
        ops_per_sec,
        latency: recorder.to_stats(),
    }
}

// =============================================================================
// SINGLE-NODE: MEMORY
// =============================================================================

pub async fn bench_election(duration: Duration) -> ScenarioResult {
    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    let wall = Instant::now();

    while wall.elapsed() < duration {
        let t = Timer::start();
        let raft = make_raft().await;
        wait_for_leader(&raft).await;
        recorder.record(t.elapsed());
        let _ = raft.shutdown().await;
    }

    build_result(recorder.count(), wall.elapsed().as_nanos() as u64, recorder)
}

pub async fn bench_propose_sequential(duration: Duration, payload_size: usize) -> ScenarioResult {
    let raft = boot_single_node().await;
    let p = payload(payload_size);
    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    let wall = Instant::now();

    while wall.elapsed() < duration {
        let req = BenchRequest { payload: p.clone() };
        let t = Timer::start();
        raft.client_write(req).await.expect("client_write failed");
        recorder.record(t.elapsed());
    }

    let result = build_result(recorder.count(), wall.elapsed().as_nanos() as u64, recorder);
    let _ = raft.shutdown().await;
    result
}

pub async fn bench_propose_concurrent(
    duration: Duration,
    payload_size: usize,
    concurrency: usize,
) -> ScenarioResult {
    let raft = Arc::new(boot_single_node().await);
    let deadline = Instant::now() + duration;
    let wall = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let r = Arc::clone(&raft);
        let p = payload(payload_size);
        handles.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(TASK_SAMPLE_CAP);
            while Instant::now() < deadline {
                let req = BenchRequest { payload: p.clone() };
                let t = Timer::start();
                r.client_write(req).await.expect("client_write failed");
                samples.push(t.elapsed());
            }
            samples
        }));
    }

    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    for handle in handles {
        for sample in handle.await.expect("task panicked") {
            recorder.record(sample);
        }
    }

    let total_ns = wall.elapsed().as_nanos() as u64;
    let ops = recorder.count();
    let result = build_result(ops, total_ns, recorder);

    let raft = Arc::try_unwrap(raft)
        .unwrap_or_else(|_| panic!("all task handles have been joined: Arc still has owners"));
    let _ = raft.shutdown().await;
    result
}

pub async fn bench_commit_latency(duration: Duration, payload_size: usize) -> ScenarioResult {
    let raft = boot_single_node().await;
    let p = payload(payload_size);

    for _ in 0..WARMUP_OPS {
        let req = BenchRequest { payload: p.clone() };
        raft.client_write(req).await.expect("warmup write failed");
    }

    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    let wall = Instant::now();

    while wall.elapsed() < duration {
        let req = BenchRequest { payload: p.clone() };
        let t = Timer::start();
        raft.client_write(req).await.expect("client_write failed");
        recorder.record(t.elapsed());
    }

    let result = build_result(recorder.count(), wall.elapsed().as_nanos() as u64, recorder);
    let _ = raft.shutdown().await;
    result
}

// =============================================================================
// SINGLE-NODE: WAL (16-shard WAL with parallel fsync)
// =============================================================================

pub async fn bench_election_wal(duration: Duration) -> ScenarioResult {
    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    let wall = Instant::now();

    while wall.elapsed() < duration {
        let t = Timer::start();
        let raft = make_raft_wal().await;
        wait_for_leader(&raft).await;
        recorder.record(t.elapsed());
        let _ = raft.shutdown().await;
    }

    build_result(recorder.count(), wall.elapsed().as_nanos() as u64, recorder)
}

pub async fn bench_propose_sequential_wal(duration: Duration, payload_size: usize) -> ScenarioResult {
    let raft = boot_single_node_wal().await;
    let p = payload(payload_size);
    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    let wall = Instant::now();

    while wall.elapsed() < duration {
        let req = BenchRequest { payload: p.clone() };
        let t = Timer::start();
        raft.client_write(req).await.expect("client_write failed");
        recorder.record(t.elapsed());
    }

    let result = build_result(recorder.count(), wall.elapsed().as_nanos() as u64, recorder);
    let _ = raft.shutdown().await;
    result
}

pub async fn bench_propose_concurrent_wal(
    duration: Duration,
    payload_size: usize,
    concurrency: usize,
) -> ScenarioResult {
    let raft = Arc::new(boot_single_node_wal().await);
    let deadline = Instant::now() + duration;
    let wall = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let r = Arc::clone(&raft);
        let p = payload(payload_size);
        handles.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(TASK_SAMPLE_CAP);
            while Instant::now() < deadline {
                let req = BenchRequest { payload: p.clone() };
                let t = Timer::start();
                r.client_write(req).await.expect("client_write failed");
                samples.push(t.elapsed());
            }
            samples
        }));
    }

    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    for handle in handles {
        for sample in handle.await.expect("task panicked") {
            recorder.record(sample);
        }
    }

    let total_ns = wall.elapsed().as_nanos() as u64;
    let ops = recorder.count();
    let result = build_result(ops, total_ns, recorder);

    let raft = Arc::try_unwrap(raft)
        .unwrap_or_else(|_| panic!("all task handles have been joined: Arc still has owners"));
    let _ = raft.shutdown().await;
    result
}

pub async fn bench_commit_latency_wal(duration: Duration, payload_size: usize) -> ScenarioResult {
    let raft = boot_single_node_wal().await;
    let p = payload(payload_size);

    for _ in 0..WARMUP_OPS {
        let req = BenchRequest { payload: p.clone() };
        raft.client_write(req).await.expect("warmup write failed");
    }

    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    let wall = Instant::now();

    while wall.elapsed() < duration {
        let req = BenchRequest { payload: p.clone() };
        let t = Timer::start();
        raft.client_write(req).await.expect("client_write failed");
        recorder.record(t.elapsed());
    }

    let result = build_result(recorder.count(), wall.elapsed().as_nanos() as u64, recorder);
    let _ = raft.shutdown().await;
    result
}

// =============================================================================
// MULTI-NODE: WAL (TCP+TLS + 16-shard WAL)
// =============================================================================

pub async fn bench_election_cluster(duration: Duration, num_nodes: usize) -> ScenarioResult {
    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    let wall = Instant::now();

    while wall.elapsed() < duration {
        let t = Timer::start();
        let cluster = setup_cluster(num_nodes).await;
        recorder.record(t.elapsed());
        cluster.shutdown().await;
    }

    build_result(recorder.count(), wall.elapsed().as_nanos() as u64, recorder)
}

pub async fn bench_propose_sequential_cluster(
    duration: Duration,
    payload_size: usize,
    num_nodes: usize,
) -> ScenarioResult {
    let cluster = setup_cluster(num_nodes).await;
    let p = payload(payload_size);
    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    let wall = Instant::now();

    while wall.elapsed() < duration {
        let req = BenchRequest { payload: p.clone() };
        let t = Timer::start();
        cluster
            .leader()
            .client_write(req)
            .await
            .expect("client_write failed");
        recorder.record(t.elapsed());
    }

    let result = build_result(recorder.count(), wall.elapsed().as_nanos() as u64, recorder);
    cluster.shutdown().await;
    result
}

pub async fn bench_propose_concurrent_cluster(
    duration: Duration,
    payload_size: usize,
    concurrency: usize,
    num_nodes: usize,
) -> ScenarioResult {
    let cluster = setup_cluster(num_nodes).await;
    let leader = Arc::clone(&cluster.nodes[cluster.leader_idx]);
    let deadline = Instant::now() + duration;
    let wall = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let r = Arc::clone(&leader);
        let p = payload(payload_size);
        handles.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(TASK_SAMPLE_CAP);
            while Instant::now() < deadline {
                let req = BenchRequest { payload: p.clone() };
                let t = Timer::start();
                r.client_write(req).await.expect("client_write failed");
                samples.push(t.elapsed());
            }
            samples
        }));
    }

    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    for handle in handles {
        for sample in handle.await.expect("task panicked") {
            recorder.record(sample);
        }
    }

    let total_ns = wall.elapsed().as_nanos() as u64;
    let ops = recorder.count();
    let result = build_result(ops, total_ns, recorder);

    drop(leader);
    cluster.shutdown().await;
    result
}

pub async fn bench_commit_latency_cluster(
    duration: Duration,
    payload_size: usize,
    num_nodes: usize,
) -> ScenarioResult {
    let cluster = setup_cluster(num_nodes).await;
    let p = payload(payload_size);

    for _ in 0..WARMUP_OPS {
        let req = BenchRequest { payload: p.clone() };
        cluster
            .leader()
            .client_write(req)
            .await
            .expect("warmup write failed");
    }

    let mut recorder = LatencyRecorder::new(RECORDER_CAP);
    let wall = Instant::now();

    while wall.elapsed() < duration {
        let req = BenchRequest { payload: p.clone() };
        let t = Timer::start();
        cluster
            .leader()
            .client_write(req)
            .await
            .expect("client_write failed");
        recorder.record(t.elapsed());
    }

    let result = build_result(recorder.count(), wall.elapsed().as_nanos() as u64, recorder);
    cluster.shutdown().await;
    result
}
