//! 16-shard disk-backed WAL for OpenRaft benchmarks.
//!
//! Mirrors QuicRaft's 16-shard WAL architecture: entries are distributed across
//! 16 independent shards by `index % 16`, each with its own mutex, WAL file,
//! and BTreeMap index. Parallel fsync across touched shards uses
//! `std::thread::scope` for zero-dependency bounded OS threads.
//!
//! This isolates the sharded-WAL architecture contribution from Go-vs-Rust I/O
//! differences when comparing QuicRaft and OpenRaft WAL benchmarks.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{Entry, LogId, LogState, OptionalSend, RaftLogReader, StorageError, Vote};

use crate::store::MemStateMachine;
use crate::types::TypeConfig;

// ── type aliases ──────────────────────────────────────────────────────────────

type NodeId = u64;
type C = TypeConfig;
type StorageResult<T> = Result<T, StorageError<NodeId>>;

const NUM_SHARDS: usize = 16;

// ── TempDirGuard ──────────────────────────────────────────────────────────────

struct TempDirGuard {
    path: PathBuf,
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

// ── ShardState ────────────────────────────────────────────────────────────────

/// Per-shard mutable state: in-memory BTreeMap index, WAL file, and reusable
/// encode buffer to avoid per-entry allocation.
struct ShardState {
    log: BTreeMap<u64, Entry<C>>,
    wal: File,
    encode_buf: Vec<u8>,
}

// ── GlobalState ───────────────────────────────────────────────────────────────

/// State shared across all shards: purge tracking, committed index, and the
/// base directory for vote persistence.
struct GlobalState {
    last_purged_log_id: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
    dir: PathBuf,
}

// ── ShardedDiskLogStore ───────────────────────────────────────────────────────

/// 16-shard disk-backed log store that distributes entries by `index % 16`.
/// Each shard has its own WAL file and mutex. Parallel fsync across touched
/// shards provides the same I/O parallelism as QuicRaft's waldb.
#[derive(Clone)]
pub struct ShardedDiskLogStore {
    shards: Arc<[Mutex<ShardState>; NUM_SHARDS]>,
    global: Arc<Mutex<GlobalState>>,
    _guard: Arc<TempDirGuard>,
}

/// Read-only handle used by replication tasks.
#[derive(Clone)]
pub struct ShardedDiskLogReader {
    shards: Arc<[Mutex<ShardState>; NUM_SHARDS]>,
    global: Arc<Mutex<GlobalState>>,
}

/// Maps a log entry index to its shard.
fn shard_idx(index: u64) -> usize {
    (index as usize) % NUM_SHARDS
}

// ── shared read helper ────────────────────────────────────────────────────────

/// Collects entries matching `range` from all 16 shards, sorted by index.
/// Locks shards in order (0..15) to prevent deadlocks.
fn read_entries_from_shards<RB>(
    shards: &[Mutex<ShardState>; NUM_SHARDS],
    range: RB,
) -> StorageResult<Vec<Entry<C>>>
where
    RB: RangeBounds<u64> + Clone + std::fmt::Debug,
{
    let mut entries = Vec::new();
    let guards: Vec<_> = shards.iter().map(|s| s.lock().unwrap()).collect();
    for guard in &guards {
        entries.extend(guard.log.range(range.clone()).map(|(_, e)| e.clone()));
    }
    drop(guards);
    entries.sort_by_key(|e| e.log_id.index);
    Ok(entries)
}

// ── RaftLogReader for ShardedDiskLogReader ────────────────────────────────────

impl RaftLogReader<C> for ShardedDiskLogReader {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<C>>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        read_entries_from_shards(&self.shards, range)
    }
}

// ── RaftLogReader for ShardedDiskLogStore ─────────────────────────────────────

impl RaftLogReader<C> for ShardedDiskLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<C>>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        read_entries_from_shards(&self.shards, range)
    }
}

// ── RaftLogStorage for ShardedDiskLogStore ────────────────────────────────────

impl RaftLogStorage<C> for ShardedDiskLogStore {
    type LogReader = ShardedDiskLogReader;

    async fn get_log_state(&mut self) -> StorageResult<LogState<C>> {
        let global = self.global.lock().unwrap();
        let mut last: Option<LogId<NodeId>> = None;
        for shard in self.shards.iter() {
            let guard = shard.lock().unwrap();
            if let Some(entry) = guard.log.values().next_back() {
                match &last {
                    None => last = Some(entry.log_id),
                    Some(prev) if entry.log_id.index > prev.index => {
                        last = Some(entry.log_id);
                    }
                    _ => {}
                }
            }
        }
        Ok(LogState {
            last_purged_log_id: global.last_purged_log_id,
            last_log_id: last.or(global.last_purged_log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        ShardedDiskLogReader {
            shards: Arc::clone(&self.shards),
            global: Arc::clone(&self.global),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StorageResult<()> {
        let dir = self.global.lock().unwrap().dir.clone();
        atomic_write_bincode(&dir, "vote.bin", vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<NodeId>>> {
        let path = self.global.lock().unwrap().dir.join("vote.bin");
        if path.exists() {
            let data = fs::read(&path).expect("read vote.bin");
            let vote: Vote<NodeId> =
                bincode::deserialize(&data).expect("deserialize vote.bin");
            Ok(Some(vote))
        } else {
            Ok(None)
        }
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        // Distribute entries into per-shard buckets.
        let mut buckets: [Vec<Entry<C>>; NUM_SHARDS] = Default::default();
        for entry in entries {
            buckets[shard_idx(entry.log_id.index)].push(entry);
        }

        // Write to each touched shard, collecting dup'd file handles for fsync.
        let mut sync_files: Vec<File> = Vec::new();

        for (i, bucket) in buckets.iter_mut().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let mut guard = self.shards[i].lock().unwrap();
            for entry in bucket.drain(..) {
                // Length-prefixed bincode: [4-byte LE length][bincode payload]
                guard.encode_buf.clear();
                bincode::serialize_into(&mut guard.encode_buf, &entry)
                    .expect("bincode serialize");
                let len = (guard.encode_buf.len() as u32).to_le_bytes();
                // Split borrows: write length prefix, then payload from encode_buf.
                // We must avoid borrowing guard.wal (mut) and guard.encode_buf (shared)
                // simultaneously, so destructure the fields.
                let ShardState { wal, encode_buf, .. } = &mut *guard;
                wal.write_all(&len).expect("WAL write length");
                wal.write_all(encode_buf).expect("WAL write payload");
                guard.log.insert(entry.log_id.index, entry);
            }
            // dup() the fd so we can fsync without holding the shard lock.
            sync_files.push(guard.wal.try_clone().expect("File::try_clone"));
        }

        // Parallel fdatasync across all touched shards.
        std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(sync_files.len());
            for file in &sync_files {
                handles.push(s.spawn(|| {
                    file.sync_data().expect("shard fdatasync");
                }));
            }
            for handle in handles {
                handle.join().expect("fsync thread panicked");
            }
        });

        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        for shard in self.shards.iter() {
            let mut guard = shard.lock().unwrap();
            let keys: Vec<u64> = guard.log.range(log_id.index..).map(|(k, _)| *k).collect();
            for k in keys {
                guard.log.remove(&k);
            }
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        for shard in self.shards.iter() {
            let mut guard = shard.lock().unwrap();
            let keys: Vec<u64> = guard
                .log
                .range(..=log_id.index)
                .map(|(k, _)| *k)
                .collect();
            for k in keys {
                guard.log.remove(&k);
            }
        }
        self.global.lock().unwrap().last_purged_log_id = Some(log_id);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> StorageResult<()> {
        // In-memory only — matches QuicRaft/Dragonboat/etcd which batch the
        // committed index into their single WAL fsync.
        self.global.lock().unwrap().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(self.global.lock().unwrap().committed)
    }
}

// ── atomic_write_bincode ──────────────────────────────────────────────────────

/// Atomically writes bincode-serialized data to `dir/name` via tmp+fsync+rename.
fn atomic_write_bincode<T: serde::Serialize>(dir: &Path, name: &str, value: &T) {
    let tmp_path = dir.join(format!(".{}.tmp", name));
    let final_path = dir.join(name);

    let data = bincode::serialize(value).expect("bincode serialization");

    let mut f = File::create(&tmp_path).expect("create tmp file");
    f.write_all(&data).expect("write tmp file");
    f.sync_all().expect("fsync tmp file");
    drop(f);

    fs::rename(&tmp_path, &final_path).expect("atomic rename");

    let dir_f = File::open(dir).expect("open dir for fsync");
    dir_f.sync_all().expect("fsync dir");
}

// ── constructor ───────────────────────────────────────────────────────────────

/// Creates a `(ShardedDiskLogStore, MemStateMachine)` pair with 16 WAL shards.
/// Each shard lives in its own subdirectory under a temporary directory that is
/// automatically cleaned up on drop.
pub fn new_sharded_disk_store() -> (ShardedDiskLogStore, MemStateMachine) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let dir_path = dir.keep();

    // Create 16 shard subdirectories with individual WAL files.
    let shard_dirs: Vec<PathBuf> = (0..NUM_SHARDS)
        .map(|i| {
            let shard_dir = dir_path.join(format!("shard-{}", i));
            fs::create_dir_all(&shard_dir).expect("create shard dir");
            shard_dir
        })
        .collect();

    let shards: [Mutex<ShardState>; NUM_SHARDS] = std::array::from_fn(|i| {
        let wal_path = shard_dirs[i].join("wal.bin");
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .expect("open shard WAL file");
        Mutex::new(ShardState {
            log: BTreeMap::new(),
            wal,
            encode_buf: Vec::with_capacity(4096),
        })
    });

    let guard = Arc::new(TempDirGuard {
        path: dir_path.clone(),
    });

    let global = Arc::new(Mutex::new(GlobalState {
        last_purged_log_id: None,
        committed: None,
        dir: dir_path,
    }));

    let log_store = ShardedDiskLogStore {
        shards: Arc::new(shards),
        global,
        _guard: guard,
    };

    let state_machine = MemStateMachine::default();

    (log_store, state_machine)
}
