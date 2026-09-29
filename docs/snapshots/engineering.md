# Snapshots — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft snapshot creation, streaming,
> import/export and re-encryption · Last verified: 2026-09-15
> Conflicts with this document are defects. Fix them here first.

Design rationale, the frame format, the AAD layout and the scope fence are in
[`architecture.md`](architecture.md). This document is the implementation
contract: the surface, the invariants, the errors, and the rules a change here
must not break.

## 1. Exported surface

```go
// Requesting a snapshot
func (h *Host) RequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (*RequestState, error)
func (h *Host) SyncRequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (uint64, error)
func (h *Host) RequestCompaction(shardID, replicaID uint64) error

type SnapshotOption struct {
    ExportPath                 string
    CompactionOverhead         uint64
    OverrideCompactionOverhead bool
    Exported                   bool
}
func (o *SnapshotOption) IsExport() bool
func (o *SnapshotOption) Validate() error
var DefaultSnapshotOption = SnapshotOption{}

// Export and import
func (h *Host) ExportSnapshot(ctx context.Context, shardID uint64, w io.Writer) error
func (h *Host) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader, opts ...ImportOption) error

type ImportOption func(*importOptions)
func WithImportDataDir(dir string) ImportOption
func WithImportMembership(members map[uint64]string) ImportOption

// Re-encryption (barrier migration)
func ReencryptSnapshotFile(srcPath, dstPath string, shardID, replicaID, index uint64,
    srcBarrier, dstBarrier Barrier) (uint64, error)

// AAD, from the engine package
const engine.SnapshotFrameAADLen = 40
func engine.NewSnapshotAAD(shardID, replicaID, index uint64, barrier contextAADBarrier) *SnapshotAAD
```

`SnapshotOption`'s zero value uses shard-level defaults. `Exported` is set
automatically when `ExportPath` is non-empty and **should not normally be set by
callers**; `ExportPath` must be absolute and is created if absent. An exported
snapshot is *additionally* written to the standard snapshot directory, so log
compaction still happens.

## 2. Invariants

**Atomicity.** Every artifact write is temp file → fsync file → rename → **fsync
directory**. The directory fsync is not optional: without it a crash can roll
the directory entry back to the deleted temp name even though the bytes were
fsynced.

**Decryption never replaces the source.** `decryptSnapshotToSibling` writes a
new sibling temp file and returns its path; the canonical encrypted
`snapshot.dat` is left intact and the caller removes the temp file when
recovery is done. In-place decryption is permanently rejected — see
[`architecture.md`](architecture.md).

**The `epoch` parameter of the decrypt path is advisory.** Each frame is
decrypted with the epoch in its own header, not with that value; it survives
only for log context. The caller uses the recorded epoch solely to decide
whether decryption is needed at all (`Epoch > 0`).

**`shardID`, `replicaID` and `index` are required, not optional**, on every
encrypt and decrypt path. They reproduce the positional context AAD. A mismatch
fails authentication and **there is no weaker retry**.

**Frames are pinned while in use.** Every epoch a snapshot's frames reference is
registered in-flight (`RegisterInFlightEpoch`) across encryption, decryption for
send, and re-encryption, so a concurrent `PurgeEpochsBefore` cannot drop a DEK
mid-operation. Every frame is sealed with `EncryptWithEpochPinnedContext`, which
does the seal and the pin under one lock. It is part of the `Barrier` contract:
the pin-before / encrypt / pin-after fallback for barriers without it only
narrowed the window, and was removed in 0.14.0.

**The purge floor counts shards that are not loaded.** A DEK may be purged
only below the minimum epoch every retained artifact still needs. That minimum
is taken over three things: the snapshot directories of every loaded shard;
the LogDB's recorded snapshot for every `(shard, replica)` it holds state for
that is **not** loaded; and the WAL's live floor. The middle term exists
because each shard has its own snapshot base (`…/snapshots/shard-N`, or its
`DataDir`), so no scan of loaded shards can see a stopped shard's tree — and
before 0.14.0 none did, which let a co-hosted shard's rotation purge the DEK a
stopped shard's snapshot needed, failing its restart forever. On the per-shard
barrier path each unloaded shard is attributed through the barrier resolver,
which is a function of the shard ID. If `ListNodeInfo` fails, or a record
cannot be read, nothing is purged. The record is the snapshot a restart
recovers from; a stopped shard's *older*, superseded snapshots are not counted
and may become undecryptable after a purge, which is accepted.

**Pin balance is an invariant, not an optimization.** Every
`RegisterInFlightEpoch` has a matching `DeregisterInFlightEpoch` on every path,
including error paths — a leaked pin permanently blocks a DEK purge, and
`pkg/snapshot_pin_balance_test.go` exists to hold that.

**nil means off; sealed means fail closed.** A `nil` barrier denotes a plaintext
side. A non-nil but **sealed** barrier is *not* a plaintext side: it means
encryption is configured and temporarily unavailable, and the operation returns
`ErrSnapshotBarrierSealed` rather than falling through. Reading an encrypted
source under a sealed barrier would copy ciphertext and record it as plaintext;
a sealed destination would durably downgrade a snapshot that must be encrypted
at rest. `saveSnapshot`, `HandleSnapshot`, `ImportSnapshot` and
`ReencryptSnapshotFile` all enforce this identically.

**A barrier that cannot bind context must not be handed one.**
`NewSnapshotAAD` asks `SupportsContextAAD()` first and the write fails closed
rather than silently dropping the binding. `ContextBinding()` and
`IsPlaintextMode()` separate the two reasons a barrier may be unable to bind:
no-encryption mode, where there is nothing a position could bind to, versus a
delegated encryptor that is protecting the data but cannot authenticate where it
belongs. **Only the second is a security limitation.**

**Integrity is verified against the file as stored.** `DataHash` is xxhash64 of
`snapshot.dat` *before* decryption and *before* decompression, checked with
`DataLen` at recovery. Every writer — save, receive, import and the export —
stamps both fields through `MarshalSnapshotMeta`, and a failure to hash the
artifact **fails the write before the directory is published**: nothing is
recorded in LogDB and nothing is compacted, and the next save retries. A
recorded snapshot whose `snapshot.meta` is missing, unreadable, malformed or
without the fields (`DataHashRecorded == false`, a pre-magic record included)
fails **closed** with `ErrSnapshotCorrupt` (`*SnapshotMetaInvalidError`); there
is no unverified path. A recorded directory whose `snapshot.dat` is gone is still
reported as a missing artifact first.

**The export artifact verifies itself.** `ExportSnapshot` ends the artifact
with a trailer — `QRSNTRLR` magic, the payload's length, and the xxhash64 of
the raw header followed by the payload, as written — and writes it last, only
once every check has passed; an export that fails never writes one. The header
is inside the hash because its fields decide where the snapshot is recorded: a
flipped `Index` or `Term` that still parses would otherwise import a sound
payload at the wrong log position. `ImportSnapshot` checks the header bytes it
parsed and the payload against the trailer
inside the copy that writes the staged `snapshot.dat`
(`artifactPayloadReader`), so a truncated, damaged, re-headed or trailerless artifact is
refused before the stage is published, recorded in LogDB or seen by raft. Both
sides share one encoder and decoder, `pkg/snapshot_artifact_trailer.go`. The
trailer was added in place, with the header version unchanged; an artifact from
an earlier build has none and is refused.

**Receive is bounded before it is committed.** `maxAdmissibleChunkCount` rejects
an inflated declared count before any memory is allocated; the receiver refuses
a transfer where `len(chunks) != hdr.ChunkCount`, where the count is zero, or
where the stream was marked truncated.

**Snapshot streams stay out of the stream pool.** They carry a dedicated frame
flag routing them to a separate handler, so a multi-gigabyte transfer cannot
occupy the pool ordinary replication uses.

**The snapshot pool is separate from the worker tiers.** `max(NumWorkers/2, 2)`
workers, so a large save or recover cannot stall step, commit or apply.

## 3. Error semantics

| Error | Raised when | Fail-closed? |
|---|---|---|
| `ErrSnapshotBarrierSealed` | a non-nil barrier on either side is sealed | **Yes** — never falls through to plaintext |
| `ErrSnapshotEncryptedBarrierUnavailable` | an encrypted artifact with no barrier to open it | Yes |
| `*SnapshotFailedError` | the save failed | Yes |
| `*SnapshotTooLargeError` | the state machine's output exceeds `MaxSnapshotSize` (4 GB), measured after compression and before sealing | Yes |
| `*SnapshotSendError` / `*SnapshotReceiveError` | the transfer failed | Yes |
| `*SnapshotArtifactError` | the artifact is malformed or fails verification: a bad header, a missing or malformed trailer, or a payload that does not match its trailer (which also matches `ErrSnapshotCorrupt`) | Yes |
| `*SnapshotOptionError` (`ErrInvalidSnapshotOption`) | `SnapshotOption.Validate` rejected the request | Yes |
| `*SnapshotCommittedNotDurableError` | see below | **partially — read this one** |
| `*ImportSnapshotDirExistsError` | the import destination directory already exists | Yes |
| `*ImportSnapshotOutOfDateError` | the imported snapshot is older than what the replica holds | Yes |
| `*ImportDataDirMismatchError` | `WithImportDataDir` names a directory that is not this replica's | Yes |
| `*ImportLayoutUnknownError` | the on-disk layout cannot be determined | Yes |
| `*ImportMembershipInvalidError` | `WithImportMembership` is not usable | Yes |

### The one error that is not simply a failure

`*SnapshotCommittedNotDurableError` reports a re-encryption whose destination is
**committed but not yet durable**: the rename succeeded, so `dstPath` already
holds the migrated artifact sealed at `Epoch`, and only the directory fsync that
makes the rename survive a crash failed.

It exists because the `(epoch, error)` pair `ReencryptSnapshotFile` returns is
unenforceable on its own — every caller writes `if err != nil { return 0, err }`,
which drops exactly the epoch this case delivers.

**A caller that receives a non-zero epoch alongside an error must record that
epoch.** Dropping it lets the next DEK purge orphan the artifact's key, and the
artifact then cannot be opened again. Treat the migration as unverified until
the destination is re-checked.

Every other failure before the rename removes the temp file and leaves both
`srcPath` and `dstPath` untouched.

## 4. Configuration

Per shard, on `config.Config`:

| Field | Default | Meaning |
|---|---|---|
| `SnapshotEntries` | 100000 | applied entries between automatic snapshots. **Zero does not disable it** — zero means "unset" and selects the default. Assign `config.SnapshotsDisabled` for manual-only. |
| `CompactionOverhead` | 5000 | entries retained after the compaction a snapshot triggers |
| `MaxSnapshotSize` | 4 GB | maximum snapshot size, measured on the state machine's output after compression and before sealing — not the framed ciphertext on disk |

Never snapshotting also means never reclaiming snapshot directories, never
purging an old epoch's DEK, and never invoking the at-rest barrier.

Per host, on `config.TransportConfig`:

| Field | Default | Meaning |
|---|---|---|
| `MaxSnapshotReceiveRate` | 256 MB/s | inbound bandwidth limit |
| `MaxConcurrentSnapshotReceives` | 4 | concurrent inbound transfers |
| `MaxSnapshotReceiveMemory` | 1 GB | total receive buffer pool |

Compression is `config.CompressionType` (`NoCompression` = 0, `Snappy` = 1) and
reaches the wire as `proto.Snapshot.Compression`, which describes a snapshot
data file. When both compression and encryption are on the order is
**compress-then-encrypt**.

## 5. Persistence

```
<NodeHostDir>/snapshots/shard-<N>/replica-<N>/snapshot-<index>/
    snapshot.dat     state machine bytes; when encrypted, a sequence of
                     [ChunkLen:4 LE | EncryptedChunk] frames, 64 KB plaintext each
    snapshot.meta    index, term, DataLen, DataHash, DataHashRecorded,
                     membership, compression, epoch, filepath
```

`snapshot.meta` is **self-describing and append-only** in its evolution: new
fields are added, and the parser reads an older record for what it carries — the
DEK purge-floor scan still learns an epoch from one. Recovery does not: a record
without `DataLen`/`DataHash` (`DataHashRecorded == false`) cannot vouch for its
artifact and is refused.

A `SnapshotOption.ExportPath` export writes the same pair. Each file is staged
as a temp in the export directory, fsynced and closed, then renamed onto its
name — `snapshot.dat` first, `snapshot.meta` last. The export directory is then
fsynced, together with every directory the export created for it and the
pre-existing ancestor's entry for the first of them, so a new export directory
survives a power loss too. A failure anywhere, including reading the source
record or writing the exported one, fails the export and with it the save.

Publication over a previous export is failure-safe. The previous pair is first
moved into a backup directory (`.snapshot-export-prev-*`) inside the export
directory, and that move is fsynced before the new `snapshot.dat` is renamed
into place, so a crash can never surface the new data beside the old record.
Any failure before the new pair is durable renames the previous pair back (a
fresh export removes the half it published), and the backup is removed only
once the new pair is durable. A crash leaves either a complete pair, or a
directory every verifier refuses — no `snapshot.meta`, or halves of two
exports whose recorded hash disagrees with the data — and until the new pair
is complete the previous one survives whole across the export and backup
directories, for an operator to restore.

Snapshot metadata is also persisted through `logdb.SaveSnapshot`, which is what
establishes the log compaction floor. A `Membership` section inside a snapshot
record carries the WAL's own `membershipSectionVersion`, a third discriminator
distinct from `proto.WireVersion` — see
[`../wire-protocol/architecture.md`](../wire-protocol/architecture.md).

## 6. Test Strategy

Target: `make test-snapshot`, plus the integration and fault-injection suites.

| File | Pins |
|---|---|
| `snapshot_durability_test.go` | the temp → fsync → rename → fsync-dir sequence |
| `snapshot_crashfs_test.go`, `snapshot_fsfailure_test.go` | crash and I/O failure at each step of that sequence |
| `snapshot_integrity_test.go` | `DataLen` / `DataHash` verification, and the fail-closed refusal of missing or unusable metadata |
| `snapshot_stamp_export_test.go` | a hash failure fails the save before publish, record or compaction; export metadata is required and verifiable |
| `snapshot_frames_test.go` | the length-prefixed frame layout |
| `snapshot_artifact_trailer_test.go` | the export artifact's trailer: round trip, refusal of truncated, damaged (payload or header), trailerless and failed-export artifacts with nothing adopted |
| `snapshot_decrypt_sweep_test.go` | decryption across every epoch combination |
| `snapshot_pin_balance_test.go` | **that every in-flight pin is released on every path**, including error paths |
| `snapshot_reencrypt_test.go`, `snapshot_reencrypt_commit_test.go` | migration, including the committed-not-durable case |
| `snapshot_recv_staging_test.go` | bounded receive and staging |
| `snapshot_sender_test.go` | chunked send |
| `snapshot_option_test.go` | `SnapshotOption.Validate` |
| `host_import_*_test.go` (eight files) | import: artifact, compaction, layout, membership, non-destructive, ordering, retry, staging sweep |
| `host_snapshot_catchup_wedge_test.go`, `host_snapshot_gc_test.go` | the catch-up wedge and snapshot GC |

**Fault-injection seams**: a crash-simulating filesystem (`snapshot_crashfs_test.go`)
and the `test/errfs` harness (`make ci-errfs`) for I/O errors; `test/netfault`
(`make ci-netfault`) for a peer that goes away mid-transfer.

**The test that must never be deleted**: `snapshot_pin_balance_test.go`. A
leaked pin has no symptom until a DEK purge silently does nothing, and by then
the cause is weeks old.

## 7. Observability

**Metrics** — via the host `MetricsCollector`; see
[`../observability/engineering.md`](../observability/engineering.md). The three
worth alerting on: snapshot save duration, transfers in flight against
`MaxConcurrentSnapshotReceives`, and the age of the newest snapshot per shard
(a shard that has stopped snapshotting is a shard whose log will not compact).

**Logging** — `log/slog`:

| Level | Event | Why |
|---|---|---|
| ERROR | snapshot save or recovery failed | the shard cannot compact, and a follower may be unrecoverable by log replay |
| ERROR | re-encryption committed but not durable | `*SnapshotCommittedNotDurableError` — **record the epoch** |
| ERROR | integrity verification failed | `DataHash` mismatch: the artifact is corrupt or truncated |
| WARN | barrier sealed during a snapshot operation | the operation failed closed |
| INFO | snapshot created / received / recovered, with index and epoch | |

No snapshot contents, no key material and no nonce appears in any log line.
`EventListener` also carries `config.SnapshotInfo` for user-visible notifications,
dispatched on the listener goroutine.
