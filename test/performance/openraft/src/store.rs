//! Pure in-memory Raft storage and state machine for benchmarking a single-node
//! OpenRaft cluster without any disk I/O.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine, RaftSnapshotBuilder};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, OptionalSend, RaftLogReader, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use crate::types::TypeConfig;

// ── type aliases ──────────────────────────────────────────────────────────────

type NodeId = u64;
type C = TypeConfig;
type StorageResult<T> = Result<T, StorageError<NodeId>>;

// The SnapshotData type as declared in TypeConfig.
type SnapshotData = Cursor<Vec<u8>>;

// ── application request / response ────────────────────────────────────────────

/// Opaque payload written by the benchmarks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchRequest {
    pub payload: Vec<u8>,
}

/// Acknowledgement returned after the entry is applied to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResponse {
    pub applied_index: u64,
}

// ── shared inner state ────────────────────────────────────────────────────────

/// All mutable state wrapped in a single `Mutex` so it can be shared between
/// the log store and state machine handles.
#[derive(Default)]
struct InnerState {
    // ── log store ────────────────────────────────────────────────────────────
    vote: Option<Vote<NodeId>>,
    /// Log entries keyed by their 1-based index.
    log: BTreeMap<u64, Entry<C>>,
    /// The last committed log id (optional, persisted across purges).
    committed: Option<LogId<NodeId>>,
    /// The last purged log id.
    last_purged_log_id: Option<LogId<NodeId>>,

    // ── state machine ────────────────────────────────────────────────────────
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    /// The most recent snapshot, if any.
    snapshot: Option<StoredSnapshot>,
    /// Monotonically increasing counter used as the snapshot id.
    snapshot_seq: u64,
}

/// The data we serialise into a snapshot blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotState {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

/// An in-memory snapshot blob with its metadata.
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

// ── MemLogStore ───────────────────────────────────────────────────────────────

/// Implements both [`RaftLogReader`] and [`RaftLogStorage`] backed by a
/// `Mutex<InnerState>`.  `RaftLogStorage` requires `RaftLogReader` as a
/// supertrait, so both must be present on the same type.
#[derive(Clone, Default)]
pub struct MemLogStore {
    inner: Arc<Mutex<InnerState>>,
}

/// A cheap clone of `MemLogStore` used by replication tasks.
#[derive(Clone)]
pub struct MemLogReader {
    inner: Arc<Mutex<InnerState>>,
}

/// Read log entries from a `MemLogReader`.
impl RaftLogReader<C> for MemLogReader {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<C>>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let inner = self.inner.lock().unwrap();
        let entries = inner.log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

/// `RaftLogStorage` requires `RaftLogReader<C>` as a supertrait, so
/// `MemLogStore` must implement it directly in addition to `MemLogReader`.
impl RaftLogReader<C> for MemLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<C>>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let inner = self.inner.lock().unwrap();
        let entries = inner.log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

impl RaftLogStorage<C> for MemLogStore {
    type LogReader = MemLogReader;

    async fn get_log_state(&mut self) -> StorageResult<LogState<C>> {
        let inner = self.inner.lock().unwrap();
        let last = inner.log.values().next_back().map(|e| e.log_id);
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: last.or(inner.last_purged_log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        MemLogReader {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.inner.lock().unwrap().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<NodeId>>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        {
            let mut inner = self.inner.lock().unwrap();
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // Notify openraft that entries are durable.  In-memory is always "durable".
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        let mut inner = self.inner.lock().unwrap();
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        let mut inner = self.inner.lock().unwrap();
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        inner.last_purged_log_id = Some(log_id);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> StorageResult<()> {
        self.inner.lock().unwrap().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(self.inner.lock().unwrap().committed)
    }
}

// ── MemStateMachine ───────────────────────────────────────────────────────────

/// Implements [`RaftStateMachine`] backed by the same `Mutex<InnerState>`.
#[derive(Clone, Default)]
pub struct MemStateMachine {
    inner: Arc<Mutex<InnerState>>,
}

/// Implements [`RaftSnapshotBuilder`].
pub struct MemSnapshotBuilder {
    inner: Arc<Mutex<InnerState>>,
}

impl RaftSnapshotBuilder<C> for MemSnapshotBuilder {
    async fn build_snapshot(&mut self) -> StorageResult<Snapshot<C>> {
        let mut inner = self.inner.lock().unwrap();
        inner.snapshot_seq += 1;

        let state = SnapshotState {
            last_applied: inner.last_applied,
            last_membership: inner.last_membership.clone(),
        };
        let data = serde_json::to_vec(&state).expect("snapshot serialisation is infallible");

        let snapshot_id = format!("snap-{}", inner.snapshot_seq);
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };

        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<C> for MemStateMachine {
    type SnapshotBuilder = MemSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> StorageResult<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>)> {
        let inner = self.inner.lock().unwrap();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> StorageResult<Vec<BenchResponse>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.lock().unwrap();
        let mut responses = Vec::new();

        for entry in entries {
            let applied_index = entry.log_id.index;
            inner.last_applied = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(_req) => {}
                EntryPayload::Membership(mem) => {
                    inner.last_membership =
                        StoredMembership::new(Some(entry.log_id), mem);
                }
            }

            responses.push(BenchResponse { applied_index });
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MemSnapshotBuilder {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<SnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<SnapshotData>,
    ) -> StorageResult<()> {
        let bytes = snapshot.into_inner();
        let state: SnapshotState =
            serde_json::from_slice(&bytes).expect("snapshot deserialisation is infallible");

        let mut inner = self.inner.lock().unwrap();
        inner.last_applied = state.last_applied;
        inner.last_membership = state.last_membership;
        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<C>>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

// ── shared constructor ────────────────────────────────────────────────────────

/// Creates a matched `(MemLogStore, MemStateMachine)` pair that share the same
/// inner state.  Both halves must be passed together to `Raft::new`.
pub fn new_mem_store() -> (MemLogStore, MemStateMachine) {
    let inner = Arc::new(Mutex::new(InnerState::default()));
    (
        MemLogStore {
            inner: Arc::clone(&inner),
        },
        MemStateMachine {
            inner: Arc::clone(&inner),
        },
    )
}
