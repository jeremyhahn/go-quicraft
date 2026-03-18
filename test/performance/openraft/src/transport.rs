//! TCP+TLS transport for multi-node OpenRaft benchmarks.
//!
//! Uses persistent connection pooling with one cached TLS stream per peer and
//! bincode serialization for all RPC messages.  Each `TlsNetworkConnection`
//! holds an `Option<TlsStream>` that is lazily established on the first RPC
//! and reused for all subsequent calls to the same peer.  If a connection
//! breaks (EOF, broken pipe, etc.) it is transparently re-established with a
//! single retry.
//!
//! Wire format: 4-byte big-endian length prefix followed by a bincode payload.
//!
//! TLS certificates are generated at runtime using `rcgen` so no on-disk PEM
//! files are needed.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::sync::Arc;

use openraft::error::Fatal;
use openraft::error::InstallSnapshotError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::BasicNode;
use openraft::Raft;
use openraft::Snapshot;
use openraft::Vote;
use rcgen::BasicConstraints;
use rcgen::IsCa;
use rcgen::KeyPair;
use rcgen::SanType;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::pki_types::PrivateKeyDer;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;

use crate::types::TypeConfig;

type NodeId = u64;
type C = TypeConfig;

// ── wire protocol ─────────────────────────────────────────────────────────────

/// Bincode message envelope sent from client to server over TLS.
#[derive(serde::Serialize, serde::Deserialize)]
enum RpcMessage {
    AppendEntries(AppendEntriesRequest<C>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<C>),
}

/// Bincode response envelope sent from server back to client.
#[derive(serde::Serialize, serde::Deserialize)]
enum RpcResponse {
    AppendEntries(AppendEntriesResponse<NodeId>),
    Vote(VoteResponse<NodeId>),
    InstallSnapshot(InstallSnapshotResponse<NodeId>),
    Error(String),
}

// ── TLS configuration ─────────────────────────────────────────────────────────

/// Runtime-generated TLS material: server config and a client connector backed
/// by the same CA so the client trusts the server certificate.
pub struct TlsMaterial {
    pub server_config: Arc<ServerConfig>,
    pub connector: TlsConnector,
}

/// Generate a self-signed CA and a node certificate signed by that CA.
/// The same cert is used for both server and client auth (mutual TLS),
/// matching the mTLS configuration of QuicRaft, Dragonboat, and etcd.
/// No files are written to disk.
pub fn generate_tls_material() -> TlsMaterial {
    // ── CA ──────────────────────────────────────────────────────────────────
    let ca_key = KeyPair::generate().expect("CA key generation failed");
    let mut ca_params =
        rcgen::CertificateParams::new(vec!["bench-ca".to_string()]).expect("CA params failed");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("CA self-sign failed");

    // ── end-entity (node) cert ───────────────────────────────────────────────
    // Used for both server and client auth (mutual TLS).
    let node_key = KeyPair::generate().expect("node key generation failed");
    let mut node_params =
        rcgen::CertificateParams::new(vec!["localhost".to_string()]).expect("node params failed");
    node_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let node_cert = node_params
        .signed_by(&node_key, &ca_cert, &ca_key)
        .expect("node cert signing failed");

    // Convert DER bytes into rustls types.
    let cert_der = CertificateDer::from(node_cert.der().to_vec());
    let ca_der = CertificateDer::from(ca_cert.der().to_vec());
    let key_der =
        PrivateKeyDer::Pkcs8(node_key.serialize_der().into());

    // ── CA root store (shared by server and client) ─────────────────────────
    let mut root_store = RootCertStore::empty();
    root_store.add(ca_der).expect("CA cert add to root store failed");

    // ── server TLS config (require + verify client certs) ───────────────────
    let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store.clone()))
        .build()
        .expect("client cert verifier build failed");
    let server_config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(vec![cert_der.clone()], key_der.clone_key())
        .expect("server TLS config failed");

    // ── client TLS config (present cert for mutual auth) ────────────────────
    let client_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![cert_der], key_der)
        .expect("client TLS config with cert failed");

    TlsMaterial {
        server_config: Arc::new(server_config),
        connector: TlsConnector::from(Arc::new(client_config)),
    }
}

// ── TlsNetworkState ───────────────────────────────────────────────────────────

/// Shared, cheaply-cloneable state for the TLS network: peer address map and
/// the client-side TLS connector.
pub struct TlsNetworkState {
    /// Map from NodeId to "host:port" string for each peer.
    pub peers: BTreeMap<NodeId, String>,
    pub connector: TlsConnector,
}

// ── TlsNetworkFactory ─────────────────────────────────────────────────────────

/// Produces [`TlsNetworkConnection`] instances on demand.
pub struct TlsNetworkFactory {
    pub state: Arc<TlsNetworkState>,
}

impl RaftNetworkFactory<C> for TlsNetworkFactory {
    type Network = TlsNetworkConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        let addr = self
            .state
            .peers
            .get(&target)
            .cloned()
            .unwrap_or_default();
        TlsNetworkConnection {
            target,
            addr,
            connector: self.state.connector.clone(),
            stream: None,
        }
    }
}

// ── TlsNetworkConnection ──────────────────────────────────────────────────────

/// A connection handle for one peer.  Caches a persistent TLS stream that is
/// reused across RPC calls.  Since `RaftNetwork` trait methods take `&mut self`,
/// we have exclusive access and no `Mutex` is needed.
pub struct TlsNetworkConnection {
    target: NodeId,
    addr: String,
    connector: TlsConnector,
    stream: Option<tokio_rustls::client::TlsStream<TcpStream>>,
}

/// Internal wire-level error type returned by `rpc_call`.
#[derive(Debug)]
struct WireError(String);

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WireError {}

impl TlsNetworkConnection {
    /// Establish a new TLS connection to the peer and cache it.
    async fn connect(&mut self) -> Result<(), WireError> {
        let tcp = TcpStream::connect(&self.addr)
            .await
            .map_err(|e| WireError(format!("connect {}: {e}", self.addr)))?;

        tcp.set_nodelay(true)
            .map_err(|e| WireError(format!("set TCP_NODELAY: {e}")))?;

        let server_name = ServerName::try_from("localhost")
            .expect("'localhost' is always a valid server name")
            .to_owned();

        let tls = self
            .connector
            .connect(server_name, tcp)
            .await
            .map_err(|e| WireError(format!("TLS handshake: {e}")))?;

        self.stream = Some(tls);
        Ok(())
    }

    /// Send a length-prefixed bincode message over the given stream and read
    /// the length-prefixed bincode response.
    async fn send_recv(
        tls: &mut tokio_rustls::client::TlsStream<TcpStream>,
        msg: &RpcMessage,
    ) -> Result<RpcResponse, WireError> {
        // Encode request as length-prefixed bincode.
        let req_bytes = bincode::serialize(msg)
            .map_err(|e| WireError(format!("serialize request: {e}")))?;
        let req_len = (req_bytes.len() as u32).to_be_bytes();
        tls.write_all(&req_len)
            .await
            .map_err(|e| WireError(format!("write len: {e}")))?;
        tls.write_all(&req_bytes)
            .await
            .map_err(|e| WireError(format!("write body: {e}")))?;
        tls.flush()
            .await
            .map_err(|e| WireError(format!("flush: {e}")))?;

        // Read length-prefixed bincode response.
        let mut len_buf = [0u8; 4];
        tls.read_exact(&mut len_buf)
            .await
            .map_err(|e| WireError(format!("read resp len: {e}")))?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        tls.read_exact(&mut resp_buf)
            .await
            .map_err(|e| WireError(format!("read resp body: {e}")))?;

        bincode::deserialize(&resp_buf)
            .map_err(|e| WireError(format!("deserialize response: {e}")))
    }

    /// Send `msg` over the cached connection, reconnecting once on failure.
    async fn rpc_call(&mut self, msg: &RpcMessage) -> Result<RpcResponse, WireError> {
        // Ensure we have a connection.
        if self.stream.is_none() {
            self.connect().await?;
        }

        // First attempt on the cached stream.
        let tls = self.stream.as_mut().expect("stream was just established");
        match Self::send_recv(tls, msg).await {
            Ok(resp) => return Ok(resp),
            Err(_) => {
                // Connection is broken; drop it and retry once.
                self.stream = None;
            }
        }

        // Reconnect and retry.
        self.connect().await?;
        let tls = self.stream.as_mut().expect("stream was just established");
        Self::send_recv(tls, msg).await
    }

    /// Map a `WireError` to an `RPCError::Unreachable`.
    fn unreachable<E: std::error::Error>(&self, e: WireError) -> RPCError<NodeId, BasicNode, E> {
        RPCError::Unreachable(Unreachable::new(&e))
    }
}

impl RaftNetwork<C> for TlsNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.rpc_call(&RpcMessage::AppendEntries(rpc)).await {
            Ok(RpcResponse::AppendEntries(r)) => Ok(r),
            Ok(other) => Err(self.unreachable(WireError(format!(
                "unexpected response variant from node {}: {:?}",
                self.target,
                std::mem::discriminant(&other),
            )))),
            Err(e) => Err(self.unreachable(e)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.rpc_call(&RpcMessage::Vote(rpc)).await {
            Ok(RpcResponse::Vote(r)) => Ok(r),
            Ok(other) => Err(self.unreachable(WireError(format!(
                "unexpected response variant from node {}: {:?}",
                self.target,
                std::mem::discriminant(&other),
            )))),
            Err(e) => Err(self.unreachable(e)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self.rpc_call(&RpcMessage::InstallSnapshot(rpc)).await {
            Ok(RpcResponse::InstallSnapshot(r)) => Ok(r),
            Ok(other) => Err(self.unreachable(WireError(format!(
                "unexpected response variant from node {}: {:?}",
                self.target,
                std::mem::discriminant(&other),
            )))),
            Err(e) => Err(self.unreachable(e)),
        }
    }

    async fn full_snapshot(
        &mut self,
        _vote: Vote<NodeId>,
        _snapshot: Snapshot<C>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<C, Fatal<NodeId>>> {
        // Snapshots are not triggered during benchmarks because entries are
        // few and no snapshot threshold is crossed.
        unreachable!("full_snapshot is not used in multi-node benchmarks")
    }
}

// ── RPC server ────────────────────────────────────────────────────────────────

/// Accept incoming TLS connections and dispatch RPCs to the local Raft node
/// until `stop_rx` signals shutdown.
pub async fn start_rpc_server(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    raft: Arc<Raft<TypeConfig>>,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            biased;

            _ = stop_rx.changed() => {
                // Shutdown requested.
                break;
            }

            result = listener.accept() => {
                let (tcp, _peer) = match result {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let _ = tcp.set_nodelay(true);
                let acceptor = acceptor.clone();
                let raft = Arc::clone(&raft);
                tokio::spawn(async move {
                    if let Ok(mut tls) = acceptor.accept(tcp).await {
                        handle_connection(&mut tls, &raft).await;
                    }
                });
            }
        }
    }
}

/// Handle a persistent RPC connection: loop reading messages, dispatching each
/// to the local Raft node, and writing back responses until the client
/// disconnects (EOF) or an I/O error occurs.
async fn handle_connection<S>(stream: &mut S, raft: &Raft<TypeConfig>)
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    const MAX_MSG_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

    loop {
        // Read request length prefix.  EOF here means the client closed the
        // connection gracefully, so we break without logging an error.
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        // Guard against pathological message sizes.
        if msg_len > MAX_MSG_BYTES {
            break;
        }

        let mut data = vec![0u8; msg_len];
        if stream.read_exact(&mut data).await.is_err() {
            break;
        }

        let msg: RpcMessage = match bincode::deserialize(&data) {
            Ok(m) => m,
            Err(_) => break,
        };

        let response = match msg {
            RpcMessage::AppendEntries(req) => match raft.append_entries(req).await {
                Ok(r) => RpcResponse::AppendEntries(r),
                Err(e) => RpcResponse::Error(e.to_string()),
            },
            RpcMessage::Vote(req) => match raft.vote(req).await {
                Ok(r) => RpcResponse::Vote(r),
                Err(e) => RpcResponse::Error(e.to_string()),
            },
            RpcMessage::InstallSnapshot(req) => match raft.install_snapshot(req).await {
                Ok(r) => RpcResponse::InstallSnapshot(r),
                Err(e) => RpcResponse::Error(e.to_string()),
            },
        };

        // Write response length prefix then body.
        let resp_bytes = match bincode::serialize(&response) {
            Ok(b) => b,
            Err(_) => break,
        };
        let resp_len = (resp_bytes.len() as u32).to_be_bytes();
        if stream.write_all(&resp_len).await.is_err() {
            break;
        }
        if stream.write_all(&resp_bytes).await.is_err() {
            break;
        }
        if stream.flush().await.is_err() {
            break;
        }
    }
}
