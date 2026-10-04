//! Central Raft type configuration for the benchmark binary.

use std::io::Cursor;
use std::sync::Arc;

use openraft::declare_raft_types;
use openraft::Config;

use crate::store::{BenchRequest, BenchResponse};

// Single-struct token that binds all openraft generic parameters together.
// SnapshotData is declared explicitly so the macro does not need to resolve
// `Cursor` in its own expansion scope (API changed in 0.9.18+).
declare_raft_types!(
    pub TypeConfig:
        D = BenchRequest,
        R = BenchResponse,
        NodeId = u64,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Raft timing pinned for every node the harness builds, single-node and
/// cluster alike.
///
/// The common benchmark configuration is a 1 ms tick, a 1-tick heartbeat and
/// a 10-tick election timeout randomized over [10, 20) ticks. OpenRaft has no
/// tick abstraction: these fields are milliseconds. The election timeout
/// matches the common configuration exactly: OpenRaft draws each node's
/// timeout once, uniformly from `election_timeout_min..election_timeout_max`.
///
/// THE HEARTBEAT DELIBERATELY DIFFERS: 5 ms here against 1 ms for the Go
/// engines. OpenRaft 0.9 uses `heartbeat_interval` not only as the heartbeat
/// period but also as the timeout of EVERY replication `append_entries` RPC
/// (`replication/mod.rs`), entries included, and of the per-read leadership
/// confirmation. The Go engines have no such coupling. At 1 ms any append
/// round trip slower than 1 ms (a follower's fdatasync included) is
/// abandoned and resent, and the transport reconnects (see
/// `TlsNetworkConnection::rpc_call`): measured at hundreds to thousands of
/// reconnects per 2 s cluster scenario, a handicap no other engine carries.
/// 5 ms keeps the RPC timeout above a normal append round trip while staying
/// below `election_timeout_min`, which OpenRaft requires. A side effect is
/// that OpenRaft's internal timer tick, `heartbeat_interval * 3 / 2` ms
/// (integer), is 7 ms, so election timeouts are checked at 7 ms granularity.
///
/// OpenRaft 0.9 has no PreVote and no CheckQuorum switch; its leader lease
/// (`election_timeout_max`) makes followers reject vote requests while they
/// still hear from a leader, which is the vote-rejection half of CheckQuorum.
pub fn raft_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 5,
            election_timeout_min: 10,
            election_timeout_max: 20,
            ..Default::default()
        }
        .validate()
        .expect("raft config is valid"),
    )
}
