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
//!
//! There is no single-node election scenario. A single-node OpenRaft group
//! cannot hold a timeout-driven election: `Raft::initialize` campaigns at
//! once, and a node that restarts as the only voter skips the election
//! timeout by design (`RaftCore::handle_tick_election`, "this is the only
//! voter, do election at once"), or resumes leadership outright if its vote
//! was committed. Rather than measure a different thing under the same
//! name, `election_memory` and `election_wal` are left out of the result
//! map, which perfresult renders as N/A and excludes from head-to-head.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::storage::RaftLogStorage;
use openraft::BasicNode;
use openraft::Raft;

use crate::bench::LatencyRecorder;
use crate::bench::Timer;
use crate::cluster::prepare_cluster;
use crate::cluster::setup_cluster;
use crate::cluster::setup_warmup;
use crate::cluster::start_cluster;
use crate::cluster::warmup_write;
use crate::sharded_disk_store::new_sharded_disk_store;
use crate::network::NoopNetworkFactory;
use crate::result::ScenarioResult;
use crate::store::encode_kv;
use crate::store::new_mem_store;
use crate::store::BenchRequest;
use crate::store::MemStateMachine;
use crate::types::raft_config;
use crate::types::TypeConfig;

// ── constants ────────────────────────────────────────────────────────────────

/// Fixed warmup count for commit latency scenarios (reaches steady state).
const WARMUP_OPS: usize = 100;

/// Key every `propose_seq_*` proposal writes.
const PROPOSE_KEY: &[u8] = b"bench-key";

/// Key every `commit_latency_*` proposal writes.
const COMMIT_KEY: &[u8] = b"commit-key";

/// Initial capacity hint for the latency recorder sample vector.
const RECORDER_CAP: usize = 65536;

/// Per-task sample capacity hint for concurrent benchmarks.
const TASK_SAMPLE_CAP: usize = 8192;

// ── type aliases ─────────────────────────────────────────────────────────────

type BenchRaft = Raft<TypeConfig>;

// ── single-node bootstrap ───────────────────────────────────────────────────

/// Start a single-node group on `log_store`, make it leader with
/// `Raft::initialize`, and wait until it reports itself leader.
///
/// Tick-driven election is disabled until leadership is established. A sole
/// voter that is still a candidate re-elects on EVERY tick, without waiting
/// out an election timeout (`RaftCore::handle_tick_election`, "this is the
/// only voter, do election at once"); with the pinned 7 ms internal tick, any
/// `save_vote` slower than a tick (the WAL store's vote write is
/// write + fsync + rename + directory fsync) supersedes each candidacy
/// before it is persisted and the node never becomes leader. `initialize`
/// campaigns directly, so the ticker's election is not needed here, and it
/// is re-enabled once the node leads. Single-node setups are never timed.
async fn start_single_node<LS>(log_store: LS, state_machine: MemStateMachine) -> BenchRaft
where
    LS: RaftLogStorage<TypeConfig>,
{
    let raft = Raft::new(1u64, raft_config(), NoopNetworkFactory::default(), log_store, state_machine)
        .await
        .expect("Raft::new failed");
    raft.runtime_config().elect(false);

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members)
        .await
        .expect("cluster initialisation failed");

    wait_for_leader(&raft).await;
    raft.runtime_config().elect(true);
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

/// Boot a single in-memory node, wait for leadership, and run the common
/// setup warmup.
async fn boot_single_node() -> BenchRaft {
    let (log_store, state_machine) = new_mem_store();
    let raft = start_single_node(log_store, state_machine).await;
    setup_warmup(&raft).await;
    raft
}

/// Boot a single node on the 16-shard WAL, wait for leadership, and run the
/// common setup warmup.
async fn boot_single_node_wal() -> BenchRaft {
    let (log_store, state_machine) = new_sharded_disk_store();
    let raft = start_single_node(log_store, state_machine).await;
    setup_warmup(&raft).await;
    raft
}

/// Build a KV Put command for `key` whose total wire size is exactly `size`
/// bytes, byte-for-byte matching the encoding proposed by the QuicRaft,
/// Dragonboat, and etcd benchmarks: `[op:1][keyLen:2 BE][key][value]`.
fn payload(key: &[u8], size: usize) -> Vec<u8> {
    let val_len = size.saturating_sub(3 + key.len());
    encode_kv(key, &vec![0u8; val_len])
}

/// Key for concurrent writer `i`: each of the writers owns its own key.
fn writer_key(i: usize) -> Vec<u8> {
    format!("key-{i}").into_bytes()
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

pub async fn bench_propose_sequential(duration: Duration, payload_size: usize) -> ScenarioResult {
    let raft = boot_single_node().await;
    let p = payload(PROPOSE_KEY, payload_size);
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
    for i in 0..concurrency {
        let r = Arc::clone(&raft);
        let p = payload(&writer_key(i), payload_size);
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
    let p = payload(COMMIT_KEY, payload_size);

    for _ in 0..WARMUP_OPS {
        warmup_write(&raft, &p).await;
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

pub async fn bench_propose_sequential_wal(duration: Duration, payload_size: usize) -> ScenarioResult {
    let raft = boot_single_node_wal().await;
    let p = payload(PROPOSE_KEY, payload_size);
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
    for i in 0..concurrency {
        let r = Arc::clone(&raft);
        let p = payload(&writer_key(i), payload_size);
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
    let p = payload(COMMIT_KEY, payload_size);

    for _ in 0..WARMUP_OPS {
        warmup_write(&raft, &p).await;
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
        // TLS material, listeners, transports and seeded WALs are built
        // outside the timer; the timer covers only starting every replica
        // and waiting until a leader is known.
        let prepared = prepare_cluster(num_nodes).await;
        let t = Timer::start();
        let cluster = start_cluster(prepared).await;
        recorder.record(t.elapsed());
        cluster.shutdown().await;
    }

    // Duration = sum of recorded election latencies so ops/sec excludes
    // per-iteration setup/teardown (matches the Go engines).
    let measured_ns = recorder.sum().as_nanos() as u64;
    build_result(recorder.count(), measured_ns, recorder)
}

pub async fn bench_propose_sequential_cluster(
    duration: Duration,
    payload_size: usize,
    num_nodes: usize,
) -> ScenarioResult {
    let cluster = setup_cluster(num_nodes).await;
    let p = payload(PROPOSE_KEY, payload_size);
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
    for i in 0..concurrency {
        let r = Arc::clone(&leader);
        let p = payload(&writer_key(i), payload_size);
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
    let p = payload(COMMIT_KEY, payload_size);

    for _ in 0..WARMUP_OPS {
        warmup_write(cluster.leader(), &p).await;
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
