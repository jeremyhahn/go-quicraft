# Snapshots — API

> **Status: NORMATIVE** · Scope: go-quicraft snapshot creation, streaming,
> import/export and re-encryption · Last verified: 2026-09-15
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API.**

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the surface below |
| **QUIC** | snapshot **transfer** between replicas: `proto.SnapshotChunk` frames on a dedicated frame path, outside the stream pool. Not a client-facing API — there is no "fetch a snapshot" request a caller can make over it. |
| Unix socket / REST / gRPC / MCP | n/a — this repository serves none of them |

Not missing-capability rows. This feature is part of the go-quicraft Raft row,
which the portfolio register marks as consumed by implementing interfaces rather
than by calling a client.

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg`

### Capability: request a snapshot

```go
func (h *Host) RequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (*RequestState, error)
func (h *Host) SyncRequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (uint64, error)
```

| | |
|---|---|
| Request | the shard and a `SnapshotOption`; `DefaultSnapshotOption` (the zero value) uses shard defaults |
| Response | a `*RequestState`, or the snapshot **index** from the sync form |
| Errors | `*SnapshotOptionError` (`ErrInvalidSnapshotOption`), `*SnapshotFailedError`, `*SnapshotTooLargeError`, `ErrSnapshotBarrierSealed`, `NotLeaderError`, `ErrBusy` |
| Permission | none — in-process call |

```go
type SnapshotOption struct {
    ExportPath                 string   // absolute; created if absent
    CompactionOverhead         uint64
    OverrideCompactionOverhead bool
    Exported                   bool     // set automatically; do not set directly
}
func (o *SnapshotOption) IsExport() bool
func (o *SnapshotOption) Validate() error
```

An exported snapshot is **additionally** written to the standard snapshot
directory, so log compaction still happens.

### Capability: force log compaction

```go
func (h *Host) RequestCompaction(shardID, replicaID uint64) error
```

Compacts below the most recent snapshot, retaining `CompactionOverhead`
entries.

### Capability: export a snapshot to a writer

```go
func (h *Host) ExportSnapshot(ctx context.Context, shardID uint64, w io.Writer) error
```

| | |
|---|---|
| Request | the shard and a destination writer |
| Response | none; the snapshot is streamed to `w` |
| Errors | `*SnapshotArtifactError`, `ErrSnapshotEncryptedBarrierUnavailable`, `ErrSnapshotBarrierSealed` |
| Permission | none |

Requires a live `Host` that knows the shard's applied index.

### Capability: import a snapshot

```go
func (h *Host) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader, opts ...ImportOption) error

func WithImportDataDir(dir string) ImportOption
func WithImportMembership(members map[uint64]string) ImportOption
```

| | |
|---|---|
| Request | the shard, a reader, and optionally a data directory and a membership override |
| Response | none |
| Errors | `*ImportSnapshotDirExistsError`, `*ImportSnapshotOutOfDateError`, `*ImportDataDirMismatchError`, `*ImportLayoutUnknownError`, `*ImportMembershipInvalidError`, `ErrSnapshotBarrierSealed` |
| Permission | none in this library |

**This rewrites the replica's membership and identity.** It is an offline,
deliberate operation. The refusals above are the feature: `ImportSnapshotDirExists`
stops it silently overwriting, `ImportSnapshotOutOfDate` stops it moving a
replica backwards, `ImportDataDirMismatch` stops it targeting the wrong replica.

### Capability: re-encrypt a snapshot under a different barrier

```go
func ReencryptSnapshotFile(srcPath, dstPath string, shardID, replicaID, index uint64,
    srcBarrier, dstBarrier Barrier) (uint64, error)
```

| | |
|---|---|
| Request | source and destination paths, the snapshot's identity, and both barriers |
| Response | the destination's **minimum** at-rest epoch across the written frames — record this as the migrated snapshot's `Epoch` so the DEK-purge floor protects every frame. `0` for an empty source or a plaintext destination. |
| Errors | `ErrSnapshotBarrierSealed`, `*SnapshotCommittedNotDurableError`, and the underlying I/O errors |
| Permission | none in this library; the barriers' credentials are the authorization |

Streamed through an in-memory pipe, so a large snapshot never loads fully into
memory and **its plaintext never lands on disk**. The write is atomic: temp file
→ fsync → rename → fsync the destination directory chain.

`shardID`, `replicaID` and `index` are **required, not optional**: snapshot
frames are bound to their position through the AEAD context AAD and there is no
weaker fallback for a caller that omits them.

**`nil` barrier means plaintext; a sealed barrier means fail closed.** A
non-nil, sealed barrier returns `ErrSnapshotBarrierSealed` rather than falling
through — reading an encrypted source under a sealed barrier would copy
ciphertext and record it as plaintext, and a sealed destination would durably
downgrade a snapshot that must be encrypted at rest.

Every epoch the source's frames reference is pinned on `srcBarrier` for the
duration, so a concurrent `PurgeEpochsBefore` cannot remove a DEK mid-migration.

#### The one error that carries a value you must not drop

```go
type SnapshotCommittedNotDurableError struct {
    Path  string   // already holds the migration
    Epoch uint64   // the value the call would have returned on success
    Err   error    // the underlying durability failure
}
```

The rename succeeded and only the directory fsync failed. **A caller that
receives a non-zero epoch alongside an error must record that epoch** — the
reflexive `if err != nil { return 0, err }` drops exactly the value this case
delivers, and dropping it lets the next DEK purge orphan the artifact's key.

### Errors

`ErrSnapshotBarrierSealed`, `ErrSnapshotEncryptedBarrierUnavailable`,
`ErrInvalidSnapshotOption`, and the typed errors `*SnapshotFailedError`,
`*SnapshotTooLargeError`, `*SnapshotSendError`, `*SnapshotReceiveError`,
`*SnapshotArtifactError`, `*SnapshotOptionError`,
`*SnapshotCommittedNotDurableError`, `*ImportSnapshotDirExistsError`,
`*ImportSnapshotOutOfDateError`, `*ImportDataDirMismatchError`,
`*ImportLayoutUnknownError`, `*ImportMembershipInvalidError`.

### The AAD helper

```go
const engine.SnapshotFrameAADLen = 40
func engine.NewSnapshotAAD(shardID, replicaID, index uint64, barrier contextAADBarrier) *SnapshotAAD
```

`pkg/internal/engine` is internal. Named here because the 40-byte layout is part
of the on-disk format contract, not because it is callable — see
[`architecture.md`](architecture.md).

### What your state machine implements

```go
PrepareSnapshot() (interface{}, error)                                   // tiers 1, 2
SaveSnapshot(ctx, [snapshotCtx,] w io.Writer, stopper <-chan struct{}) error
RecoverFromSnapshot(ctx, r io.Reader, stopper <-chan struct{}) error
```

`RecoverFromSnapshot` **always** receives plaintext, whether or not at-rest
encryption is enabled. See
[`../state-machines/api.md`](../state-machines/api.md).

## Configuration

| Field | Owner | Default |
|---|---|---|
| `SnapshotEntries` | `config.Config` | 100000. **Zero selects the default, not "off"** — use `config.SnapshotsDisabled`. |
| `CompactionOverhead` | `config.Config` | 5000 |
| `MaxSnapshotSize` | `config.Config` | 4 GB |
| `MaxSnapshotReceiveRate` | `config.TransportConfig` | 256 MB/s |
| `MaxConcurrentSnapshotReceives` | `config.TransportConfig` | 4 |
| `MaxSnapshotReceiveMemory` | `config.TransportConfig` | 1 GB |

## Versioning and deprecation

- **`snapshot.meta` is self-describing and evolves append-only.** A reader that
  finds an older record recovers what it can; `DataHashRecorded == false`
  distinguishes "predates verification" from "failed verification".
- The encrypted frame layout carries no version discriminator and needs none:
  each frame's epoch and `algID` are in its own header.
- `proto.SnapshotChunk` gates on `BinVer == proto.WireVersion` by exact match,
  and its embedded `Membership` carries its own section version byte.
- Nothing here is deprecated.
