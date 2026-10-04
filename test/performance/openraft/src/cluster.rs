//! Multi-node OpenRaft cluster for benchmark scenarios.
//!
//! Each cluster node runs in the same OS process on separate Tokio tasks,
//! communicating over loopback TCP+TLS.  This mirrors real network topology
//! without cross-machine coordination overhead while still exercising the full
//! Raft replication path.
//!
//! # Lifecycle
//!
//! Setup is split so the election benchmark can time only what the other
//! engines' harnesses time (replica start plus the wait for a leader):
//!
//! 1. [`prepare_cluster`] (untimed): generate TLS material, bind one
//!    `TcpListener` per node, build each node's `TlsNetworkFactory` with the
//!    full peer address map, and create each node's 16-shard WAL seeded with
//!    the initial membership entry.
//! 2. [`start_cluster`] (the election span): construct every `Raft` node,
//!    spawn its RPC server on the pre-bound listener, and wait until a leader
//!    is known. No node calls `Raft::initialize`, so the first leader is
//!    chosen by a natural randomized election timeout.
//! 3. [`setup_cluster`] adds the common setup warmup for the propose and
//!    commit scenarios.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::Raft;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

use crate::sharded_disk_store::new_sharded_disk_store;
use crate::sharded_disk_store::ShardedDiskLogStore;
use crate::store::encode_kv;
use crate::store::BenchRequest;
use crate::store::MemStateMachine;
use crate::transport::generate_tls_material;
use crate::transport::start_rpc_server;
use crate::transport::TlsNetworkFactory;
use crate::transport::TlsNetworkState;
use crate::types::raft_config;
use crate::types::TypeConfig;

/// Number of proposals in the common setup warmup, before the one
/// linearizable read.
const SETUP_WARMUP_PROPOSALS: usize = 10;

// ── OpenRaftCluster ───────────────────────────────────────────────────────────

/// A running multi-node OpenRaft cluster.
///
/// Call [`shutdown`](OpenRaftCluster::shutdown) when the benchmark is done to
/// stop all RPC servers and shut down every Raft node cleanly.
pub struct OpenRaftCluster {
    /// All Raft node handles, indexed 0..n.
    pub nodes: Vec<Arc<Raft<TypeConfig>>>,
    /// Index into `nodes` of the current leader.
    pub leader_idx: usize,
    stop_txs: Vec<tokio::sync::watch::Sender<bool>>,
}

impl OpenRaftCluster {
    /// Return the leader node.
    pub fn leader(&self) -> &Raft<TypeConfig> {
        &self.nodes[self.leader_idx]
    }

    /// Shut down all RPC servers and Raft nodes.
    pub async fn shutdown(self) {
        // Signal all RPC server tasks to stop.
        for tx in &self.stop_txs {
            let _ = tx.send(true);
        }
        // Shut down each Raft node.
        for node in self.nodes {
            let _ = node.shutdown().await;
        }
    }
}

// ── PreparedCluster ───────────────────────────────────────────────────────────

/// One node's untimed infrastructure: its transport, bound listener and
/// seeded storage, ready for `Raft::new`.
struct PreparedNode {
    id: u64,
    network: TlsNetworkFactory,
    listener: TcpListener,
    log_store: ShardedDiskLogStore,
    state_machine: MemStateMachine,
}

/// Everything a cluster needs before its replicas start: TLS material,
/// listeners, transports and storage. Built by [`prepare_cluster`] outside
/// any benchmark timer and consumed by [`start_cluster`].
pub struct PreparedCluster {
    nodes: Vec<PreparedNode>,
    acceptor: TlsAcceptor,
}

// ── cluster bootstrap ─────────────────────────────────────────────────────────

/// Stand up an `n`-node cluster with 16-shard disk WAL, elect a leader,
/// run the common setup warmup, and return the cluster handle.
pub async fn setup_cluster(n: usize) -> OpenRaftCluster {
    let cluster = start_cluster(prepare_cluster(n).await).await;
    setup_warmup(cluster.leader()).await;
    cluster
}

/// Build the untimed infrastructure for an `n`-node cluster: TLS material,
/// one bound listener per node, each node's TLS network factory, and each
/// node's WAL seeded with the initial membership entry.
///
/// # Panics
///
/// Panics if any network or storage operation fails, since a broken cluster
/// cannot produce meaningful benchmark data.
pub async fn prepare_cluster(n: usize) -> PreparedCluster {
    assert!(n >= 1, "cluster size must be at least 1");

    let material = generate_tls_material();
    let acceptor = TlsAcceptor::from(Arc::clone(&material.server_config));
    let connector = material.connector;

    // ── bind listeners ────────────────────────────────────────────────────────
    // Bind all listeners before creating any Raft node so every node knows all
    // peer addresses before the first heartbeat fires.
    let mut listeners: Vec<TcpListener> = Vec::with_capacity(n);
    // NodeId is 1-based; addr_map maps NodeId -> "127.0.0.1:port".
    let mut addr_map: BTreeMap<u64, String> = BTreeMap::new();

    for i in 0..n {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("TcpListener::bind failed");
        let addr = listener.local_addr().expect("local_addr failed").to_string();
        addr_map.insert((i + 1) as u64, addr);
        listeners.push(listener);
    }

    // ── initial membership ────────────────────────────────────────────────────
    // The same entry `Raft::initialize` would append: a membership config at
    // log id (0,0)-0. Seeding it on every node, instead of calling
    // `initialize` on one, starts all nodes as followers.
    let voters: BTreeSet<u64> = (1..=n as u64).collect();
    let membership_entry = Entry::<TypeConfig> {
        log_id: LogId::default(),
        payload: EntryPayload::Membership(Membership::new(vec![voters], None)),
    };

    // ── per-node infrastructure ───────────────────────────────────────────────
    let mut nodes: Vec<PreparedNode> = Vec::with_capacity(n);
    for (i, listener) in listeners.into_iter().enumerate() {
        let id = (i + 1) as u64;

        // Peer map for this node excludes itself.
        let peers: BTreeMap<u64, String> = addr_map
            .iter()
            .filter(|(node_id, _)| **node_id != id)
            .map(|(k, v)| (*k, v.clone()))
            .collect();

        let state = Arc::new(TlsNetworkState {
            peers,
            connector: connector.clone(),
        });

        let (log_store, state_machine) = new_sharded_disk_store();
        log_store.seed(membership_entry.clone());

        nodes.push(PreparedNode {
            id,
            network: TlsNetworkFactory { state },
            listener,
            log_store,
            state_machine,
        });
    }

    PreparedCluster { nodes, acceptor }
}

/// Start every replica of a prepared cluster and wait until a leader is
/// known. This is the span the election benchmark times.
///
/// # Panics
///
/// Panics if a node fails to start or no leader is elected within the
/// timeout.
pub async fn start_cluster(prepared: PreparedCluster) -> OpenRaftCluster {
    let config = raft_config();
    let n = prepared.nodes.len();
    let mut nodes: Vec<Arc<Raft<TypeConfig>>> = Vec::with_capacity(n);
    let mut stop_txs: Vec<tokio::sync::watch::Sender<bool>> = Vec::with_capacity(n);

    for node in prepared.nodes {
        let raft = Raft::new(
            node.id,
            Arc::clone(&config),
            node.network,
            node.log_store,
            node.state_machine,
        )
        .await
        .expect("Raft::new failed");
        let raft = Arc::new(raft);

        // Spawn the RPC server for this node on its pre-bound listener.
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let rpc_raft = Arc::clone(&raft);
        let rpc_acceptor = prepared.acceptor.clone();
        let listener = node.listener;
        tokio::spawn(async move {
            start_rpc_server(listener, rpc_acceptor, rpc_raft, stop_rx).await;
        });

        nodes.push(raft);
        stop_txs.push(stop_tx);
    }

    let leader_idx = wait_for_leader(&nodes).await;

    OpenRaftCluster {
        nodes,
        leader_idx,
        stop_txs,
    }
}

/// The common setup warmup shared by every benchmark engine: exactly
/// [`SETUP_WARMUP_PROPOSALS`] proposals, then one linearizable read, before
/// any timer starts. It establishes all follower connections and warms the
/// replication and read paths. Proposals use the `warmup-%d` keys the Go
/// harnesses use.
///
/// # Panics
///
/// Panics if a warmup proposal still fails after the shared retry budget.
pub async fn setup_warmup(leader: &Raft<TypeConfig>) {
    for i in 0..SETUP_WARMUP_PROPOSALS {
        let payload = encode_kv(format!("warmup-{i}").as_bytes(), b"ok");
        warmup_write(leader, &payload).await;
    }
    // Non-fatal, as in the QuicRaft harness's warmup SyncRead. In a cluster,
    // OpenRaft 0.9 confirms leadership for a read by opening a NEW network
    // client per voter (`RaftCore::handle_check_is_leader_request`), which
    // with this transport is a fresh TCP + mutual TLS 1.3 handshake, and
    // bounds it by `heartbeat_interval` (5 ms). It can miss that deadline
    // without anything being wrong with the group; no OpenRaft scenario
    // reads, so the read only warms the path.
    if let Err(e) = leader.ensure_linearizable().await {
        eprintln!("[openraft-bench] warmup linearizable read (non-fatal): {e}");
    }
}

/// Total attempts per warmup proposal. Mirrors `benchutil.WarmupAttempts`
/// (test/performance/benchutil/retry.go), the budget every Go harness uses,
/// so no engine's suite is discarded for a flake another would survive.
const WARMUP_ATTEMPTS: usize = 4;

/// Delay between warmup attempts. Mirrors `benchutil.WarmupBackoff`.
const WARMUP_BACKOFF: Duration = Duration::from_millis(500);

/// Propose one unmeasured warmup command with the shared retry budget:
/// up to [`WARMUP_ATTEMPTS`] attempts with [`WARMUP_BACKOFF`] between them,
/// exactly as `benchutil.Retry`, then fail with the last error.
///
/// # Panics
///
/// Panics with the last error when every attempt fails.
pub async fn warmup_write(raft: &Raft<TypeConfig>, payload: &[u8]) {
    let mut last_err = None;
    for attempt in 0..WARMUP_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(WARMUP_BACKOFF).await;
        }
        let req = BenchRequest {
            payload: payload.to_vec(),
        };
        match raft.client_write(req).await {
            Ok(_) => return,
            Err(e) => last_err = Some(e),
        }
    }
    panic!(
        "warmup write failed after {WARMUP_ATTEMPTS} attempts: {}",
        last_err.expect("at least one attempt ran"),
    );
}

// ── internal helpers ──────────────────────────────────────────────────────────

/// Leader poll interval, matching the 1 ms the Go harnesses poll at so the
/// election timer stops at the same observation granularity everywhere.
const LEADER_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Poll all nodes every [`LEADER_POLL_INTERVAL`] until one declares itself
/// leader, then return its index. Times out after 10 seconds to prevent a
/// stalled benchmark from hanging.
async fn wait_for_leader(nodes: &[Arc<Raft<TypeConfig>>]) -> usize {
    let fut = async {
        loop {
            for (i, node) in nodes.iter().enumerate() {
                let metrics = node.metrics().borrow().clone();
                // A node is a confirmed leader when it reports its own ID.
                if metrics.current_leader == Some((i + 1) as u64) {
                    return i;
                }
            }
            tokio::time::sleep(LEADER_POLL_INTERVAL).await;
        }
    };

    timeout(Duration::from_secs(10), fut)
        .await
        .expect("cluster failed to elect a leader within 10 seconds")
}
