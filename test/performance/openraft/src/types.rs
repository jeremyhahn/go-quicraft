//! Central Raft type configuration for the benchmark binary.

use std::io::Cursor;

use openraft::declare_raft_types;

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
