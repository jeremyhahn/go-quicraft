# Snapshots — API

> **Status: NORMATIVE** · Scope: go-quicraft snapshot creation, streaming,
> import/export and re-encryption · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. The Go API is the API.

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the surface below |
| **QUIC** | snapshot transfer between replicas: `proto.SnapshotChunk` frames on a dedicated frame path, outside the stream pool. Not a client-facing API: no caller can request a snapshot over it. |
| Unix socket / REST / gRPC / MCP | n/a: this repository serves none of them |

These are not missing-capability rows. The feature is part of the go-quicraft
Raft row, which the portfolio register marks as consumed by implementing
interfaces, not by calling a client.

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg` (package `quicraft`)

### Request a snapshot

```go
func (h *Host) RequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (*RequestState, error)
func (h *Host) SyncRequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (uint64, error)
```

| | |
|---|---|
| Request | the shard and a `SnapshotOption`; `DefaultSnapshotOption` (the zero value) uses the shard's defaults |
| Response | a `*RequestState` whose result carries the snapshot index, or the index directly from the sync form |
| Errors | on submit: `*SnapshotOptionError` (`ErrInvalidSnapshotOption`), `ErrShardNotFound`, `ErrShardNotReady`, `ErrSnapshotInProgress`, `ErrBusy`, `ErrClosed`. From the save: `*SnapshotFailedError` (wrapping `*SnapshotSavePanicError` when `SaveSnapshot` panicked), `*SnapshotTooLargeError`, `ErrSnapshotBarrierSealed`. The sync form also returns `ErrTimeout` or `ErrCanceled`. |
| Permission | none; in-process call |

```go
type SnapshotOption struct {
    ExportPath                 string   // absolute; created if absent
    CompactionOverhead         uint64   // used only with OverrideCompactionOverhead
    OverrideCompactionOverhead bool
    Exported                   bool     // derived from ExportPath; do not set
}
func (o *SnapshotOption) IsExport() bool
func (o *SnapshotOption) Validate() error
```

An exported snapshot is also written to the standard snapshot directory, so log
compaction still happens.

### Force log compaction

```go
func (h *Host) RequestCompaction(shardID, replicaID uint64) error
```

Compacts the log below the most recent snapshot, keeping `CompactionOverhead`
entries.

### Export a snapshot to a writer

```go
func (h *Host) ExportSnapshot(ctx context.Context, shardID uint64, w io.Writer) error
```

| | |
|---|---|
| Request | the shard and a destination writer |
| Response | none; the artifact is streamed to `w` |
| Errors | `ErrSnapshotNotFound` (the shard has no snapshot yet), `ErrSnapshotCorrupt` (the stored file fails its recorded length or hash), `*SnapshotArtifactError`, `ErrSnapshotEncryptedBarrierUnavailable`, `ErrSnapshotBarrierSealed` |
| Permission | none |

Needs a live `Host` that knows the shard. The artifact is a header, the
plaintext data and a trailer with the length and xxhash64 of header and data.
The trailer is written last and only on success, so the output of a failed
export cannot be imported. On any error, discard what `w` received.

### Import a snapshot

```go
func (h *Host) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader, opts ...ImportOption) error

func WithImportDataDir(dir string) ImportOption
func WithImportMembership(members map[uint64]string) ImportOption
func WithImportMemberIdentities(identities map[uint64]string) ImportOption
```

| | |
|---|---|
| Request | the shard, a reader, and optionally the shard's data directory, a replacement membership, and node identities for the resulting membership |
| Response | none |
| Errors | `*SnapshotArtifactError` (also `ErrSnapshotCorrupt` on a length or hash mismatch), `*ImportSnapshotDirExistsError`, `*ImportSnapshotOutOfDateError`, `*ImportDataDirMismatchError`, `*ImportLayoutUnknownError`, `*ImportMembershipInvalidError`, `*ReplicaRetiredError`, `ErrSnapshotBarrierSealed` |
| Permission | none in this library |

**This rewrites the replica's membership.** It is an offline, deliberate
operation, and the refusals are the point: `*ImportSnapshotDirExistsError`
stops a silent overwrite, `*ImportSnapshotOutOfDateError` stops it moving a
replica backwards, and `*ImportDataDirMismatchError` stops it targeting the
wrong replica. The data is checked against the trailer as it is written, and a
truncated, damaged or trailerless artifact is refused before anything is
published or recorded.

On a host with a transport, every member of the resulting configuration needs a
node identity. `WithImportMemberIdentities` supplies or overrides them. An entry
for a replica the configuration gives no role, or for a replica the artifact
records as removed, is refused with `*ImportMembershipInvalidError`.

### Re-encrypt a snapshot under a different barrier

```go
func ReencryptSnapshotFile(srcPath, dstPath string, shardID, replicaID, index uint64,
    srcBarrier, dstBarrier Barrier) (uint64, error)
```

| | |
|---|---|
| Request | source and destination paths, the snapshot's identity, and both barriers |
| Response | the destination's lowest at-rest epoch across the written frames. Record it as the migrated snapshot's `Epoch` so the DEK-purge floor protects every frame. `0` for an empty source or a plaintext destination. |
| Errors | `ErrSnapshotBarrierSealed`, `*SnapshotCommittedNotDurableError`, and the underlying I/O errors |
| Permission | none in this library; the barriers' credentials are the authorization |

The data streams through an in-memory pipe, so a large snapshot is never fully
loaded and its plaintext never lands on disk. The write is atomic: temp file,
fsync, rename, fsync the destination directory chain.

`shardID`, `replicaID` and `index` are required. Frames are bound to their
position through the AEAD context, and there is no weaker fallback for a caller
that omits them.

A nil barrier means that side is plaintext. A non-nil, sealed barrier returns
`ErrSnapshotBarrierSealed` instead of falling through: reading an encrypted
source under it would copy ciphertext and record it as plaintext, and a sealed
destination would durably downgrade a snapshot that must be encrypted.

Every epoch the source's frames reference is pinned on `srcBarrier` for the
duration, so a concurrent `PurgeEpochsBefore` cannot remove a DEK mid-migration.

#### The error that carries a value you must keep

```go
type SnapshotCommittedNotDurableError struct {
    Path  string   // already holds the migration
    Epoch uint64   // what the call would have returned on success
    Err   error    // the underlying durability failure
}
```

The rename succeeded and only the directory fsync failed. **Record `Epoch`.**
The usual `if err != nil { return 0, err }` drops exactly the value this case
delivers, and dropping it lets the next DEK purge orphan the artifact's key.

### Errors

`ErrSnapshotBarrierSealed`, `ErrSnapshotEncryptedBarrierUnavailable`,
`ErrSnapshotCorrupt`, `ErrSnapshotInProgress`, `ErrSnapshotNotFound`,
`ErrInvalidSnapshotOption`, and
the typed errors `*SnapshotFailedError`, `*SnapshotTooLargeError`,
`*SnapshotSendError`, `*SnapshotReceiveError`, `*SnapshotArtifactError`,
`*SnapshotOptionError`, `*SnapshotCommittedNotDurableError`,
`*ImportSnapshotDirExistsError`, `*ImportSnapshotOutOfDateError`,
`*ImportDataDirMismatchError`, `*ImportLayoutUnknownError`,
`*ImportMembershipInvalidError`, `*ReplicaRetiredError`.

### Events and failure reporting

`config.EventListener` reports snapshot activity with a `config.SnapshotInfo`:
`OnSnapshotCreated`, `OnSnapshotCompacted`, `OnSnapshotRecovered`,
`OnSnapshotSendStarted`, `OnSnapshotSent`, `OnSnapshotSendAborted`,
`OnSnapshotReceived` and `OnSnapshotRejected`. `SnapshotInfo.Err` is the cause
of an `OnSnapshotRejected` when one exists and nil for every other event.
`OnSnapshotRejected` is delivered on the listener dispatcher, so a listener may
call `StopShard` from it. A recovery that fails before the state machine's
restore began is rejected and re-driven; after five consecutive rejections the
shard fails and `OnShardFailed` carries `*SnapshotRecoveryExhaustedError`
(`*SnapshotRecoveryPanicError` is a possible cause). A `SaveSnapshot` panic
reaches the requester as `*SnapshotSavePanicError` inside `*SnapshotFailedError`
and does not fail the shard. Details in
[engineering.md](engineering.md#7-observability).

### The AAD helper

```go
const engine.SnapshotFrameAADLen = 40
func engine.NewSnapshotAAD(shardID, replicaID, index uint64, barrier contextAADBarrier) *SnapshotAAD
```

`pkg/internal/engine` is internal. These are named because the 40-byte layout is
part of the on-disk format, not because they are callable; see
[architecture.md](architecture.md#positional-binding).

### What your state machine implements

```go
PrepareSnapshot() (interface{}, error)                                   // tiers 1 and 2
SaveSnapshot(ctx, [snapshotCtx,] w io.Writer, stopper <-chan struct{}) error
RecoverFromSnapshot(ctx, r io.Reader, stopper <-chan struct{}) error
```

`RecoverFromSnapshot` always receives plaintext, whether or not at-rest
encryption is on. See [the state-machine API](../state-machines/api.md).

## Configuration

| Field | Owner | Default |
|---|---|---|
| `SnapshotEntries` | `config.Config` | 100000. Zero selects the default; `config.SnapshotsDisabled` turns automatic snapshots off. |
| `CompactionOverhead` | `config.Config` | 5000 |
| `MaxSnapshotSize` | `config.Config` | 4 GiB |
| `SnapshotCompressionType` | `config.Config` | `config.NoCompression` |
| `MaxSnapshotReceiveRate` | `config.TransportConfig` | 256 MiB/s |
| `MaxConcurrentSnapshotReceives` | `config.TransportConfig` | 4 |
| `MaxSnapshotReceiveMemory` | `config.TransportConfig` | 1 GiB |

## Versioning and deprecation

- `snapshot.meta` is self-describing and grows only by appending fields. A
  record without `DataLen`/`DataHash` is refused at recovery
  (`ErrSnapshotCorrupt`).
- The export artifact header version is 2. Its trailer was added without
  changing that version; an artifact with no trailer is refused.
- The encrypted frame layout carries no version discriminator and needs none:
  each frame's epoch and algorithm ID are in its own header, and the AAD magic
  identifies the binding scheme.
- `proto.SnapshotChunk` is accepted only when `BinVer` equals
  `proto.WireVersion`, and its embedded `Membership` carries its own section
  version.
- Format versions stay pinned until the first production release; see
  [Compatibility](../compatibility.md).
- Nothing here is deprecated.
