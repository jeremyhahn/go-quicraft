//! No-op Raft network for a single-node benchmark cluster.
//!
//! A single-node cluster never needs to send inter-node RPCs because it is
//! always the leader and there are no peers.  The implementation panics if any
//! RPC method is ever called so that a latent bug (unexpected peer
//! communication) is immediately visible in benchmark runs.

use openraft::error::{Fatal, InstallSnapshotError, RPCError, RaftError, ReplicationClosed, StreamingError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Snapshot, Vote};

use crate::types::TypeConfig;

type NodeId = u64;
type C = TypeConfig;

// ── NoopNetwork ────────────────────────────────────────────────────────────────

/// A no-op network connection.  All RPC methods are unreachable in a single
/// node cluster; reaching them indicates a programming error.
pub struct NoopNetwork;

impl RaftNetwork<C> for NoopNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        unreachable!("single-node benchmark: append_entries must never be called")
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        unreachable!("single-node benchmark: vote must never be called")
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        unreachable!("single-node benchmark: install_snapshot must never be called")
    }

    async fn full_snapshot(
        &mut self,
        _vote: Vote<NodeId>,
        _snapshot: Snapshot<C>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<C, Fatal<NodeId>>> {
        unreachable!("single-node benchmark: full_snapshot must never be called")
    }
}

// ── NoopNetworkFactory ────────────────────────────────────────────────────────

/// Produces [`NoopNetwork`] instances.  Never actually opens a connection.
#[derive(Default)]
pub struct NoopNetworkFactory;

impl RaftNetworkFactory<C> for NoopNetworkFactory {
    type Network = NoopNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        NoopNetwork
    }
}
