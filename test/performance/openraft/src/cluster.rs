//! Multi-node OpenRaft cluster for benchmark scenarios.
//!
//! Each cluster node runs in the same OS process on separate Tokio tasks,
//! communicating over loopback TCP+TLS.  This mirrors real network topology
//! without cross-machine coordination overhead while still exercising the full
//! Raft replication path.
//!
//! # Lifecycle
//!
//! 1. Bind one `TcpListener` per node (OS assigns ephemeral ports).
//! 2. Construct each `Raft` node with a `TlsNetworkFactory` seeded with the
//!    full peer address map.
//! 3. Spawn the RPC server task for each node.
//! 4. Call `Raft::initialize` on node 1 to form the cluster.
//! 5. Wait for a stable leader, perform a warmup write, and return.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::RaftLogStorage;
use openraft::BasicNode;
use openraft::Config;
use openraft::Raft;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

use crate::sharded_disk_store::new_sharded_disk_store;
use crate::store::BenchRequest;
use crate::store::MemStateMachine;
use crate::transport::generate_tls_material;
use crate::transport::start_rpc_server;
use crate::transport::TlsNetworkFactory;
use crate::transport::TlsNetworkState;
use crate::types::TypeConfig;

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

// ── cluster bootstrap ─────────────────────────────────────────────────────────

/// Stand up an `n`-node cluster with 16-shard disk WAL, elect a leader,
/// perform one warmup write, and return the cluster handle.
pub async fn setup_cluster(n: usize) -> OpenRaftCluster {
    setup_cluster_inner(n, new_sharded_disk_store).await
}

/// Generic cluster bootstrap parameterized by the log store factory.
///
/// # Panics
///
/// Panics if any network or Raft operation fails during setup, since a broken
/// cluster cannot produce meaningful benchmark data.
async fn setup_cluster_inner<LS>(
    n: usize,
    store_factory: impl Fn() -> (LS, MemStateMachine),
) -> OpenRaftCluster
where
    LS: RaftLogStorage<TypeConfig>,
{
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

    // ── raft config shared by every node ──────────────────────────────────────
    let raft_config = Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        }
        .validate()
        .expect("raft config is valid"),
    );

    // ── create nodes ──────────────────────────────────────────────────────────
    let mut nodes: Vec<Arc<Raft<TypeConfig>>> = Vec::with_capacity(n);
    let mut stop_txs: Vec<tokio::sync::watch::Sender<bool>> = Vec::with_capacity(n);

    for i in 0..n {
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

        let network = TlsNetworkFactory { state };
        let (log_store, state_machine) = store_factory();

        let raft = Raft::new(id, Arc::clone(&raft_config), network, log_store, state_machine)
            .await
            .expect("Raft::new failed");
        let raft = Arc::new(raft);

        // Spawn the RPC server for this node.
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let listener = listeners.remove(0); // take in insertion order
        let rpc_raft = Arc::clone(&raft);
        let rpc_acceptor = acceptor.clone();
        tokio::spawn(async move {
            start_rpc_server(listener, rpc_acceptor, rpc_raft, stop_rx).await;
        });

        nodes.push(raft);
        stop_txs.push(stop_tx);
    }

    // ── initialize cluster ────────────────────────────────────────────────────
    // Only node 1 calls initialize; the others are invited via the membership
    // entry in the initial log.
    let mut members: BTreeMap<u64, BasicNode> = BTreeMap::new();
    for i in 0..n {
        members.insert((i + 1) as u64, BasicNode::default());
    }
    nodes[0]
        .initialize(members)
        .await
        .expect("cluster initialization failed");

    // ── wait for a stable leader ──────────────────────────────────────────────
    let leader_idx = wait_for_leader(&nodes).await;

    // ── warmup write ──────────────────────────────────────────────────────────
    // One write ensures the leader's replication pipeline is warm and all
    // follower connections are established before timing begins.
    let warmup_req = BenchRequest {
        payload: vec![0u8; 128],
    };
    nodes[leader_idx]
        .client_write(warmup_req)
        .await
        .expect("warmup write failed");

    OpenRaftCluster {
        nodes,
        leader_idx,
        stop_txs,
    }
}

// ── internal helpers ──────────────────────────────────────────────────────────

/// Poll all nodes until one declares itself leader, then return its index.
/// Times out after 10 seconds to prevent a stalled benchmark from hanging.
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
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };

    timeout(Duration::from_secs(10), fut)
        .await
        .expect("cluster failed to elect a leader within 10 seconds")
}
