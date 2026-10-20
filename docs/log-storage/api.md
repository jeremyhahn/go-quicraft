# Log Storage — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/logdb`, `pkg/logdb/waldb`,
> `pkg/logdb/memdb` · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. The Go API is the API.

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the surface below |
| Unix socket / REST / gRPC / MCP | n/a: this repository serves no socket, HTTP handler, gRPC service or MCP tool |
| QUIC | n/a for storage. The QUIC listener carries Raft messages; entries reach the log through the engine's commit worker, never from a socket. |

These are not missing-capability rows. The feature is part of the go-quicraft
row covering Raft consensus, log replication and QUIC transport, which the
portfolio register marks as consumed by implementing interfaces, not by calling
a client.

## The interface

Import: `github.com/jeremyhahn/go-quicraft/pkg/logdb`

```go
type LogDB interface {
    Name() string
    SaveState(updates []Update) error
    IterateEntries(entries []Entry, size uint64, shardID, replicaID, low, high, maxSize uint64) ([]Entry, uint64, error)
    ReadState(shardID, replicaID uint64) (State, Membership, error)
    EntryRange(shardID, replicaID uint64) (firstIndex, length uint64, err error)
    RemoveEntriesTo(shardID, replicaID, index uint64) error
    Compact(shardID, replicaID uint64) error
    SaveSnapshot(shardID, replicaID uint64, snapshot Snapshot) error
    GetSnapshot(shardID, replicaID uint64) (Snapshot, error)
    RemoveNodeData(shardID, replicaID uint64) error
    NodeRemoved(shardID, replicaID uint64) (bool, error)
    ListNodeInfo() ([]NodeInfo, error)
    SaveBootstrap(shardID, replicaID uint64, bootstrap Bootstrap) error
    GetBootstrap(shardID, replicaID uint64) (Bootstrap, bool, error)
    Close() error
}
```

The doc comments on the declaration (`pkg/logdb/logdb.go`) carry the
per-method contract; [engineering.md](engineering.md) restates what an
implementer must get right. Four contracts are repeated here because getting
them wrong corrupts data silently instead of failing a test.

**No aliasing, in either direction.**

- `SaveState` must not retain or alias the caller's `Entry.Cmd` backing arrays.
  Callers reuse those buffers after it returns.
- `IterateEntries` must not return an `Entry` whose `Cmd` aliases the store's
  memory. The returned payloads are the caller's to keep and to modify, and what
  a caller would otherwise write through into is the live log this replica
  serves to followers and replays on restart. `CopyEntryCmd(e Entry) Entry`
  returns an entry that owns its `Cmd`.

**Size accounting in `IterateEntries`.** Each entry is charged
`EntryOverheadBytes + len(Cmd)` against `maxSize`, and `maxSize` 0 means
unlimited. Iteration stops before the entry that would cross the limit, except
that one entry is always returned when the result slice is still empty, even if
it alone exceeds `maxSize`.

**Retirement.** `RemoveNodeData` removes a node's data and retires its
`(shardID, replicaID)` for good. `NodeRemoved` reports that from then on, across
restarts, and every writer refuses the node with `ErrReplicaRetired`. A removed
replica must rejoin under a new replica ID: its term and vote went with its
data, so reusing the ID could let it vote twice in one term.

**Snapshot bounds.** `SaveSnapshot`, and a snapshot carried in an `Update`, are
refused when the index or term is at or above the protocol bound
(`Snapshot.CheckBounds`). `SaveSnapshot` refuses a snapshot at index 0, or at or
below the stored one, with `ErrSnapshotOutOfDate`; `SaveState` skips a stale
snapshot silently.

### Errors

| Error | Meaning |
|---|---|
| `ErrNoState` | no saved hard state for this node |
| `ErrNoSnapshot` | no saved snapshot |
| `ErrNoBootstrap` | no saved bootstrap record |
| `ErrClosed` | the store is closed |
| `ErrCompacted` | `low` is at or below the node's compaction floor |
| `ErrUnavailable` | unknown node, or a range holding no entries |
| `ErrSnapshotOutOfDate` | `SaveSnapshot` with a snapshot no newer than the stored one, or at index 0 |
| `ErrLogGapDetected` | the stored range does not begin exactly at `low`, an interior index is missing, or the stored count disagrees with the first..last span. A log hole is never served as a short result. |
| `ErrEntriesBelowCompactionFloor` (`*EntriesBelowCompactionFloorError`) | an `Update` whose first entry is at or below the floor. The whole batch is refused and nothing is stored: those indexes are already deleted, so writing them would replace the live suffix above the floor with entries nothing can serve. |
| `ErrReplicaRetired` | a write names a node `RemoveNodeData` removed |

`ErrCompacted` and `ErrUnavailable` are also the Raft log's own errors for a
compacted index and an index past the end of the log; it has no separate
sentinel. Match them with `errors.Is` wherever they surface, including in a
`ShardFailedInfo.Err` or a `CommitError`.

[engineering.md](engineering.md#recovery-error-reference) covers `waldb`'s
errors and which paths fail closed.

### Optional interfaces

The engine type-asserts for these. A `LogDB` that implements none still works;
each one enables a capability without changing a contract.

| Interface | Method | Enables |
|---|---|---|
| `AtRestProtectedLogDB` | `AtRestProtection() AtRestProtection` | the store's own statement of at-rest protection: `AtRestUnprotected` (the zero value, and what a missing implementation means), `AtRestNonDurable`, `AtRestBarrierEncrypted`, or `AtRestHostRouted` (each record goes through a dedicated per-shard barrier or through one host-wide route that may be plaintext; a multiplexing LogDB, or a `waldb` under a plaintext-mode barrier). A host that requires encryption checks this instead of inferring it from the presence of a barrier, and accepts `AtRestHostRouted` only under `WithoutEncryption` or when that route writes through its own `WithBarrier` barrier. |
| `EpochAwareLogDB` | `MinLiveEpoch() (uint64, error)` | the safe-purge floor for barrier DEKs. `0` with a nil error means no encrypted records, or no barrier. |
| `PerBarrierEpochAwareLogDB` | `MinLiveEpochForBarrier(barrier any) (epoch uint64, known bool, err error)` | the same floor, restricted to one barrier's records |
| `BarrierShardEnumerator` | `ShardsForBarrier(barrier any) (shards []uint64, known bool)` | the complete shard set behind a barrier, when that set is fixed |

The `barrier` handle in the last two is matched with `logdb.SameBarrier(a, b any)
bool`, the one identity rule the host, the snapshot pool and `waldb` also use:
identity is pointer identity, and a nil or non-pointer handle matches nothing,
not even a copy of itself. The handle's dynamic value need not be comparable,
so an implementation must never compare it with `==` or use it as a map key
(either panics on a value holding a slice or map), and must answer
`known == false` for a handle that is not a pointer.

`waldb.DB` implements the first three; `memdb.MemDB` implements
`AtRestProtectedLogDB` (reporting `AtRestNonDurable`). Nothing in this
repository implements `BarrierShardEnumerator`; it exists for a multiplexing
per-tenant `LogDB`.

The three-value form is a fail-safe contract. Callers must honour it before
purging any DEK:

| Result | Means | May purge below the floor? |
|---|---|---|
| `known == false` | the store cannot attribute the barrier: it holds no records under it, or cannot prove which records belong to it | No |
| any error | the value could not be determined | No |
| `known == true, epoch == 0` | the store owns the barrier and has no live encrypted records | yes, as far as this store goes; other floors still apply |
| `known == true, epoch > 0` | the oldest live record under this barrier is at `epoch` | not below `epoch` |

With two return values, "unknown barrier" and "zero live records" look the
same, and reading the first as the second over-purges.

### Types

`Entry`, `State`, `Membership`, `Snapshot`, `Bootstrap`, `Update`, `NodeInfo`.
They are deliberately separate from `pkg/proto`'s types so serialization and
persistence change independently. Every field must be persisted; see
[engineering.md](engineering.md#storage-layer-types) for what breaks when one
is dropped.

`LogReader` (`NewLogReader(shardID, replicaID, logdb)`) is the read-side
adapter the Raft peer uses.

```go
const EntryOverheadBytes = 84
```

## The durable implementation

Import: `github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb`

### Open a store

```go
func Open(dir string, opts ...Option) (*DB, error)
```

| Option | Effect | Default |
|---|---|---|
| `WithBlockSize(n)` | block size: a power of two, at least 16 bytes | 32 KiB. Immutable for the life of the directory; `Open` refuses a changed value (`ErrWALGeometryMismatch`). |
| `WithMaxFileSize(n)` | segment rotation threshold, at most 4 GiB − 1 (`ErrInvalidMaxFileSize` above) and at least two blocks (`ErrSegmentTooSmallForEntry` below) | 64 MiB. Safe to change on a populated directory: recovery never reads it, so it only moves future rotations. |
| `WithNoSync(true)` | skip every fsync and every directory sync | off. Tests and container benchmarks only. Nothing in such a segment is ever acknowledged, so recovery is always free to truncate it. |
| `WithBarrier(b WALBarrier)` | at-rest encryption | none. The barrier must be initialized and unsealed before `Open`; a sealed one fails with `ErrBarrierContextUnresolved`. |
| `WithMetrics(m LogDBMetrics)` | write, fsync and compaction latency, bytes written | nil, which skips collection |
| `WithFS(fs FS)` | filesystem abstraction | `DefaultFS()`. The fault-injection seam. An `FS` that also implements `DirLocker` (as `DefaultFS()` does) is used to lock the directory; one that does not opens unlocked, so a wrapper around the real filesystem should forward `LockDir`. |
| `WithDiskMonitorOpts(maxBytes, ...DiskMonitorOption)` | disk-usage ceiling | none |

`DiskMonitorOption`s: `WithWarningPercent` (default `DefaultWarningPercent` =
0.85), `WithFullPercent` (default `DefaultFullPercent` = 0.95), `WithCheckEvery`
(default `DefaultCheckEvery` = 100 writes), `WithOnWarning`, `WithOnFull`,
`WithOnRecovery`, `WithDiskUsageFunc`. Thresholds are fractions of `maxBytes`, compared with the used bytes of the whole
filesystem (`statfs` total minus available; a stub reporting zero on non-Linux).
`WithCheckEvery(0)` logs a WARN and uses the default. `DiskUsageFunc` is
`func(dir string) (usedBytes uint64, err error)`.
Past the full threshold `SaveState` returns `ErrDiskFull`.

A directory has one writer. `Open` takes a non-blocking exclusive lock on
`dir/LOCK` (flock on unix, `LockFileEx` on Windows) before it replays a
segment, and holds it until `Close`. A second `Open` of the same directory, in
this process or another, fails with a `*DBOpenError` wrapping
`fsutil.ErrDirLocked` (`quicraft.ErrDirLocked` is the same value) that names
the lock file. An `Open` that fails after taking the lock releases it, and the
kernel releases it when the process dies. The lock file is never removed.

`Options` is a declarative mirror of these (`WALDir`, `BlockSize`,
`MaxFileSize`, `NoSync`) with `DefaultOptions`, `SetDefaults` and `Validate`;
`Open` does not take it. A record is bounded by segment geometry, an entry's `Cmd` by 8 MiB, and
the batch Raft hands to `SaveState` by `config.Config.MaxApplyEntrySize`.

Besides the `LogDB` methods, `*DB` has `CompactedTo`, `MinLiveEpoch`,
`MinLiveEpochForBarrier`, `AtRestProtection`, and the single-node conveniences
`SaveEntries`, `GetEntries`, `SaveNodeState` and `GetState`.

`Compact` makes the shard directory's compaction floors durable in its
`COMPACTED` file before it unlinks any segment, and unlinks nothing when that
publication fails: the error is returned and the next call retries. The floor
`CompactedTo` reports after a restart is the higher of that file's and the
replayed marker's, so a torn marker block cannot lower it (see
[Reconciling a lost compaction floor](engineering.md#reconciling-a-lost-compaction-floor)).

### Repair a corrupt segment

```go
func Repair(dir string, target RepairTarget, opts ...Option) (RepairResult, error)

type RepairTarget struct {
    ShardIndex int    // WAL shard, in [0, DefaultShardCount)
    SegmentID  uint64
    TruncateAt int64  // must equal the clean-prefix end the scan computes
}
type RepairResult struct {
    RecordsKept     int
    BytesDiscarded  int64
    SegmentsDeleted []uint64
}
```

Run it with the database closed, with the same `WithBlockSize` the directory
was written with. Every field of `RepairTarget` comes from the
`*SegmentCorruptionError` `Open` returned (`ShardIndex`, `SegmentID`,
`CleanEnd`). It refuses a segment that is not corrupt (`ErrRepairNoCorruption`),
an offset that is not the clean-prefix end (`ErrRepairOffsetMismatch`,
`*RepairOffsetMismatchError`), and a shard index out of range
(`ErrRepairShardOutOfRange`). A repair discards data, which is why recovery
never runs one on its own.

### Re-encrypt a WAL

```go
func Reencrypt(srcDir string, srcBarrier WALBarrier, dstDir string, dstBarrier WALBarrier,
    snapFn SnapshotReencryptFunc, opts ...Option) (count int, err error)

type SnapshotReencryptFunc func(shardID, replicaID uint64, snap logdb.Snapshot) (dstEpoch uint64, err error)
```

Copies every node into an empty `dstDir` and returns how many nodes it copied.
Either barrier may be nil for a plaintext side. `opts` apply to both sides, so
pass the options the node normally opens its WAL with. This is the supported
path for a sealing-strategy change; changing it in place across a restart is
not.

### The filesystem seam

```go
type FS interface {
    OpenFile(name string, flag int, perm fs.FileMode) (File, error)
    Open(name string) (File, error)
    MkdirAll(path string, perm fs.FileMode) error
    ReadDir(name string) ([]fs.DirEntry, error)
    Stat(name string) (fs.FileInfo, error)
    Remove(name string) error
    Rename(oldpath, newpath string) error
}
func DefaultFS() FS
```

`File` is `Read`, `ReadAt`, `Write`, `WriteAt`, `Seek`, `Stat`, `Truncate`,
`Sync` and `Close`. `FileMetadataSyncer` (`SyncMetadata`) and
`FilePreAllocator` (`PreAllocate`) are optional and found by type assertion.
`FileMetadataSyncer` is an alias of `fsutil.MetadataSyncer`: every directory
fsync goes through `fsutil.SyncDirWith`, which uses `SyncMetadata` when the
handle has it.
`Rename` is required: an embedder's own `FS` must provide it.

### The barrier seam

```go
type WALBarrier interface {
    Encrypt(dst, plaintext []byte) ([]byte, error)
    Decrypt(dst, ciphertext []byte) ([]byte, error)
    EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)
    DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)
    CurrentEpoch() uint64
}
```

Declared here instead of imported from `pkg/seal`, so the two are tested
independently. At `Open`, a configured barrier must also provide
`EncryptWithContext`, `DecryptWithContext`, `IsHardwareMode`,
`SupportsContextAAD` and `ContextBindingResolved`; `*seal.Barrier` does. A
barrier missing only `ContextBindingResolved` fails with
`ErrBarrierContextBindingUnreportable`, one missing the rest with
`ErrBarrierContextUnsupported`. Note that `seal.ContextBinding`'s zero value is
`ContextBindingUnresolved`, not a binding.

### Errors

Sentinels: `ErrCorruptedRecord`, `ErrMidSegmentCorruption`,
`ErrNonFinalSegmentCorruption`, `ErrAcknowledgedDataLost`,
`ErrRecordSequenceRegression`, `ErrUnknownPayloadType`,
`ErrUnsupportedMembershipVersion`, `ErrUnsupportedSnapshotCompression`,
`ErrWALFormatMissing`, `ErrWALFormatMalformed`,
`ErrWALFormatUnsupportedVersion`, `ErrWALGeometryMismatch`,
`ErrRetiredRegistryMalformed`, `ErrCompactionFloorsMalformed`,
`ErrBarrierContextUnsupported`,
`ErrBarrierContextBindingUnreportable`, `ErrBarrierContextUnresolved`,
`ErrBarrierContextBindingChanged`, `ErrLandingSegmentOverflow`,
`ErrDBPoisoned`, `ErrDiskFull`, `ErrRecordTooLarge`, `ErrCmdTooLarge`,
`ErrMembershipTooLarge`, `ErrBootstrapDataDirProvenance`, `ErrSegmentFull`,
`ErrClosed`, `ErrBufferTooSmall`, `ErrInvalidRecordType`,
`ErrSnapshotReencryptRequired`, the replay decode errors
(`ErrTruncatedEntryRecord`, `ErrTruncatedStateRecord`,
`ErrTruncatedSnapshotRecord`, `ErrTruncatedBootstrapRecord`,
`ErrTruncatedCompactionRecord`), the option errors (`ErrEmptyWALDir`,
`ErrInvalidBlockSize`, `ErrInvalidMaxFileSize`, `ErrSegmentTooSmallForEntry`)
and the repair sentinels above.

Typed errors: `*SegmentCorruptionError`, `*RecoveryError`, `*DBOpenError`,
`*SegmentReadError`, `*SegmentCreateError`, `*SegmentDiscardError`,
`*WALEncryptError`, `*WALDecryptError`, `*WALWriteError`, `*BatchWriteError`,
`*DirSyncError`, `*DiskUsageError`, `*CompactionDeleteError`,
`*CompactionReadDirError`, `*ReencryptError`, `*WALFormatVersionError`,
`*WALGeometryMismatchError`, `*RepairOffsetMismatchError`,
`*WriteBootstrapBatchModeError`.

When each is raised, and which fail closed:
[engineering.md](engineering.md#recovery-error-reference).

### Constants

```go
const DefaultShardCount = 16
const (
    DefaultWarningPercent = 0.85
    DefaultFullPercent    = 0.95
    DefaultCheckEvery     = 100
)
const (
    RecordFull     byte = 1
    RecordFirst    byte = 2
    RecordMiddle   byte = 3
    RecordLast     byte = 4
    RecordSyncMark byte = 5
)
```

## The in-memory implementation

Import: `github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb`

```go
func New() *MemDB
```

Satisfies `logdb.LogDB` and `logdb.AtRestProtectedLogDB` with no durability.
The `Host` selects it with `WithMemoryLogDB()`.

## Versioning and deprecation

- `walFormatVersion` = 1, recorded in the `WALFORMAT` marker, compared for exact
  equality before any segment is read.
- The membership section inside a snapshot record has its own version byte,
  `membershipSectionVersion` = 1, also exact-match. It is separate from
  `proto.WireVersion` and from `proto.Membership`'s persisted format version.
- Every format version stays pinned at 1 until the first production release.
  Layout changes are made in place; a record layout change retires its old
  payload-type byte and takes the next free one, so a stale record fails replay
  with `ErrUnknownPayloadType`. The remedy is always the same: wipe the data
  directory and re-sync from peers.

The policy and the per-release upgrade notes are in
[`../compatibility/`](../compatibility/architecture.md).
