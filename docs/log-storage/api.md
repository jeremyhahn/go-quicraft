# Log Storage — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/logdb`, `pkg/logdb/waldb`,
> `pkg/logdb/memdb` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API.**

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the surface below. This is the transport. |
| Unix socket / REST / gRPC / MCP | n/a — this repository serves no socket, exposes no HTTP handler, defines no gRPC service and registers no MCP tools |
| QUIC | n/a for storage. The QUIC listener carries Raft messages; entries reach the log through the engine's commit worker, never from a socket directly. |

Not missing-capability rows. This feature is part of the go-quicraft capability
row covering Raft consensus, log replication and QUIC transport, which the
portfolio register marks as consumed by implementing interfaces rather than by
calling a client.

## Go API — the interface

Import: `github.com/jeremyhahn/go-quicraft/pkg/logdb`

```go
type LogDB interface {
    Name() string
    SaveState(updates []Update) error
    IterateEntries(entries []Entry, size uint64, shardID, replicaID, low, high, maxSize uint64) ([]Entry, uint64, error)
    // ... plus snapshot, bootstrap, node-info, compaction and lifecycle methods
}
```

The **authoritative signature list is the interface declaration itself**
(`pkg/logdb/logdb.go`), whose doc comments carry the per-method contract in
full; the semantics that matter to an implementer are restated in
[`engineering.md`](engineering.md). Two contracts are load-bearing enough to
repeat here, because getting either wrong is silent corruption rather than a
failed test:

**No aliasing, in either direction.**

- `SaveState` **must not retain or alias the caller's `Entry.Cmd` backing
  arrays.** Callers reuse those buffers after it returns, so the store holds its
  own copies.
- `IterateEntries` **must not return an `Entry` whose `Cmd` aliases the store's
  own memory.** The returned payloads are the caller's to keep *and to modify* —
  and what a caller would otherwise be writing through into is the live log this
  replica serves to its followers and replays to itself on restart.

**Size accounting in `IterateEntries`.** Each entry is charged
`EntryOverheadBytes + len(Cmd)` against `maxSize`; `maxSize` of 0 means
unlimited; **at least one entry is returned when any is available**, even if it
alone exceeds `maxSize`; otherwise iteration stops *before* the entry that would
cross the limit.

### Errors the interface defines

| Error | Meaning |
|---|---|
| `ErrNoState` | no saved hard state for this node |
| `ErrCompacted` | `low` is at or below the node's compaction floor |
| `ErrUnavailable` | unknown node, or a range holding no entries |
| `ErrLogGapDetected` | the stored range does not begin exactly at `low`, or an interior index is missing. **A log hole must never be served as a silently shortened result.** |
| `ErrEntriesBelowCompactionFloor` | an `Update` whose first entry sits at or below the floor. The **whole batch** is rejected without storing any of it: those indexes are already deleted, so honouring the write would replace the live suffix above the floor with entries nothing can serve. |

Full error semantics, including `waldb`'s own set, are in
[`engineering.md`](engineering.md).

### Capability interfaces — probed, not required

The engine type-asserts for these. A `LogDB` that implements none still works;
each one it implements enables a capability rather than changing a contract.

| Interface | Enables |
|---|---|
| `AtRestProtectedLogDB` | reporting `AtRestProtection()` — `AtRestUnprotected` or the protected states — so a caller checks the store's answer instead of inferring encryption from the presence of a barrier |
| `EpochAwareLogDB` | `MinLiveEpoch() (uint64, error)`, the safe-purge floor for barrier DEKs. `0` with a nil error means no encrypted records, or no barrier. |
| `PerBarrierEpochAwareLogDB` | `MinLiveEpochForBarrier(barrier any) (epoch uint64, known bool, err error)`, the same restricted to one barrier's records |
| `BarrierShardEnumerator` | enumerating which shards are behind which barrier |
| `RecoveryResidualReporter` | `RecoveryResiduals()` — non-empty exactly when the node may have come back **shorter than the log it acknowledged** |

The three-value form is a fail-safe contract callers **must** honour before
purging any DEK:

| Result | Means | May purge below the floor? |
|---|---|---|
| `known == false` | this store cannot **attribute** the barrier — it holds no records under it, or cannot prove which records belong to it | **No** |
| any error | the value could not be determined | **No** |
| `known == true, epoch == 0` | the store owns the barrier and has zero live encrypted records | yes, as far as this store is concerned; other floors still apply |
| `known == true, epoch > 0` | the oldest live record under this barrier sits at `epoch` | not below `epoch` |

The two-value and three-value distinction is the whole point of the second
interface: in a two-value signature, "unknown barrier" is indistinguishable from
"zero live records", and treating the first as the second **over-purges**.

The `barrier` argument is an opaque, comparable handle — the same concrete value
the caller installed — compared by identity.

### Storage-layer types

`Entry`, `State`, `Membership`, `Snapshot`, `Bootstrap`, `Update`, `NodeInfo`,
`LogRange`, `RecoveryResidual`, `ResidualNodeRange`.

These are **separate types from `pkg/proto`'s**, deliberately, so serialization
and persistence stay independently changeable. `CopyEntryCmd(e Entry) Entry`
returns an entry owning its own `Cmd`.

`LogReader` (`NewLogReader(shardID, replicaID, logdb)`) is the read-side adapter
the Raft peer uses.

```go
const EntryOverheadBytes = 84
```

## Go API — the durable implementation

Import: `github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb`

### Capability: open a store

```go
func Open(dir string, opts ...Option) (*DB, error)
```

| Option | Effect | Default |
|---|---|---|
| `WithBlockSize(n)` | record block size | 32 KiB. **Immutable for the life of the directory** — `Open` refuses a changed value rather than destroying the log. |
| `WithMaxFileSize(n)` | segment rotation threshold | 64 MiB. Safe to raise on a populated directory: recovery never reads it, so it only changes where future rotations land. |
| `WithNoSync(true)` | skip every fsync **and** every directory sync | off. **Tests and container benchmarks only.** Every record in such a segment carries durability generation 0, so recovery is always free to truncate it. |
| `WithBarrier(b WALBarrier)` | at-rest encryption | none. The barrier must be initialized **and unsealed** before `Open`; a sealed one fails with `ErrBarrierContextUnresolved` rather than latching a downgraded binding. |
| `WithMetrics(m LogDBMetrics)` | latency and throughput collection | nil, and nil skips collection entirely |
| `WithFS(fs FS)` | filesystem abstraction | `DefaultFS()`. The fault-injection seam. |
| `WithDiskMonitorOpts(maxBytes, ...)` | disk-usage monitoring | none |

`DiskMonitor` options: `WithWarningPercent` (default `DefaultWarningPercent` =
0.85), `WithFullPercent`, `WithCheckEvery(n)`, `WithOnWarning`, `WithOnFull`,
`WithOnRecovery`, `WithDiskUsageFunc`.

`Options.MaxBatchSize` **is vestigial and bounds nothing.** It is defaulted by
`SetDefaults` and rejected by `Validate` when non-positive, but no `Option`
carries it into `Open`, so no value a caller sets can reach a write. It is
documented rather than silently dropped because the name promises a cap that
already exists elsewhere under different owners: segment geometry bounds a
record, `maxCmdLen` (8 MB) bounds an entry's `Cmd`, and
`config.MaxApplyEntrySize` bounds the batch Raft hands to `SaveState`. Giving it
an effect would add a rejection on the durable write path, which is a durability
decision rather than a doc fix, so it is left inert until that decision is made.

### Capability: repair a corrupt segment

```go
func Repair(dir string, target RepairTarget, opts ...Option) (RepairResult, error)
```

Explicit target and offset, never automatic. Refuses a segment that is not
corrupt (`ErrRepairNoCorruption`), an offset that is not the clean-prefix end
(`ErrRepairOffsetMismatch`, `*RepairOffsetMismatchError`), and a shard index
that does not exist (`ErrRepairShardOutOfRange`). **A repair discards data**,
which is why recovery never performs one on its own.

### Capability: re-encrypt a WAL

```go
func Reencrypt(srcDir string, srcBarrier WALBarrier, dstDir string, dstBarrier WALBarrier,
    snapFn SnapshotReencryptFunc) (count int, err error)
```

Writes to a **new** directory. This is the supported path for a
sealing-strategy change; doing it across a restart in place is not.

### The filesystem seam

```go
type FS interface{ /* Open, Create, Remove, Rename, ReadDir, ... */ }
type File interface{ /* Write, Read, Sync, Close, ... */ }
func DefaultFS() FS

type FileMetadataSyncer interface{ /* optional */ }
type FilePreAllocator  interface{ /* optional */ }
```

`FS` **requires `Rename(oldpath, newpath string) error`** — an embedder
supplying its own implementation must provide it. `FileMetadataSyncer` and
`FilePreAllocator` are optional capabilities probed by type assertion.

### The barrier seam

```go
type WALBarrier interface{ /* the narrow subset waldb needs */ }
```

Declared here rather than imported from `pkg/seal`, so the two are
independently testable. It requires `ContextBindingResolved() bool` — a barrier
implementation must provide it, and `seal.ContextBinding`'s zero value is
`Unresolved`, **not** a binding.

### Errors

`ErrCorruptedRecord`, `ErrMidSegmentCorruption`, `ErrNonFinalSegmentCorruption`,
`ErrRecordSequenceRegression`, `ErrDBPoisoned`, `ErrDiskFull`,
`ErrWALFormatMissing`, `ErrWALFormatMalformed`,
`ErrWALFormatUnsupportedVersion`, `ErrWALGeometryMismatch`,
`ErrSnapshotReencryptRequired`, `ErrUnsupportedMembershipVersion`,
`ErrUnsupportedSnapshotCompression`, and the repair sentinels above — plus the
typed errors `*BatchWriteError`, `*SegmentCorruptionError`, `*RecoveryError`,
`*WALEncryptError`, `*WALDecryptError`, `*WALWriteError`, `*DBOpenError`,
`*DirSyncError`, `*DiskUsageError`, `*CompactionDeleteError`,
`*CompactionReadDirError`, `*SegmentCreateError`, `*SegmentReadError`,
`*SegmentDiscardError`, `*ReencryptError`, `*WALFormatVersionError`,
`*WALGeometryMismatchError`, `WriteBootstrapBatchModeError`.

Conditions and fail-closed behavior: [`engineering.md`](engineering.md).

### Constants

```go
const DefaultShardCount    = 16
const DefaultWarningPercent = 0.85
const RecordFull byte = 1   // plus First / Middle / Last
```

## Go API — the in-memory implementation

Import: `github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb`

```go
func New() *MemDB
```

Satisfies `logdb.LogDB` with **no durability**. Sharded by
`(shardID, replicaID)` with per-node RWMutexes and an atomic closed-check, so it
serializes the way production does rather than hiding races.

At the `Host` level, `WithMemoryLogDB()` selects it; a nil `LogDB` disables
persistence entirely and lets the engine inline `Peer.Commit`.

## Versioning and deprecation

- `walFormatVersion` = 1, persisted, **exact match**, checked first at `Open`.
  Pinned for the pre-release period: layout changes are made in place and
  development data directories are wiped rather than migrated.
- The WAL's own `membershipSectionVersion` is a **third** discriminator on the
  `Membership` layout, distinct from `proto.WireVersion` and
  `proto.membershipFormatVersion`, and gated the same way.
- **0.5.x cannot read a 0.4.x data directory**: the per-entry WAL record header
  grew from 76 to 84 bytes, `proto.Entry`'s from 60 to 68, and the seal MAC
  moved from v1 to v2. The break is detected at the **record** level by the
  payload-type ladder — which names the legacy format it found — rather than by
  the version number.
- `Options.MaxBatchSize` is inert, documented, and not deprecated; see above.
- Nothing else here is deprecated.

The repository-wide policy and per-release upgrade procedure are in
[`../compatibility.md`](../compatibility.md).
