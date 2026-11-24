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

/// Raft timing for every node: a 10-20 ms election timeout as in the common configuration, and a 5 ms heartbeat because OpenRaft 0.9 also uses it as the append RPC timeout.
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
