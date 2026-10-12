# Snapshots — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft snapshot creation, streaming,
> import/export and re-encryption · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

Design rationale, the frame format, the AAD layout and the scope fence are in
[architecture.md](architecture.md). This document is the implementation
contract: the surface, the invariants, the errors, and the rules a change must
not break.

## 1. Exported surface

Package `quicraft` (`github.com/jeremyhahn/go-quicraft/pkg`):

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
func WithImportMemberIdentities(identities map[uint64]string) ImportOption

// Re-encryption (barrier migration)
func ReencryptSnapshotFile(srcPath, dstPath string, shardID, replicaID, index uint64,
    srcBarrier, dstBarrier Barrier) (uint64, error)
```

From the internal engine package, named because they fix the on-disk format:

```go
const engine.SnapshotFrameAADLen = 40
func engine.NewSnapshotAAD(shardID, replicaID, index uint64, barrier contextAADBarrier) *SnapshotAAD
```

`SnapshotOption`'s zero value uses the shard's defaults. `ExportPath` must be
absolute and is created if absent. `Exported` is derived from `ExportPath`
(`IsExport`) and callers should not set it. An exported snapshot is also written
to the standard snapshot directory, so log compaction still happens.
`CompactionOverhead` takes effect only with `OverrideCompactionOverhead`.

## 2. Invariants

**Atomicity.** Every artifact write is temp file, fsync file, rename, fsync
directory. The directory fsync is required: without it a crash can roll the
directory entry back to the temp name even though the bytes were synced.

**Decrypted plaintext never has a name.** Recovery (engine and `StartShard`) and
the transport send decrypt into an anonymous scratch file
(`CreateAnonymousScratch`): created in the replica's own snapshot directory and
unlinked before a byte is written. A failed unlink fails the operation before
anything is decrypted (`ErrScratchNotUnlinked`), and it is retried later; the
empty name is removed at once, or, if that fails too, counted on
`StaleDecryptSweepFailures` and reclaimed by the stale-scratch sweep. The only
named decrypt is the `ExportPath` copy, inside the save's staging directory,
which the save removes. The canonical `snapshot.dat` is never modified, and
in-place decryption is rejected (see
[architecture.md](architecture.md#rejected-decrypting-to-a-named-file-or-in-place)).

**The `epoch` argument of the decrypt path is advisory.** Each frame is opened
with the epoch in its own header; the recorded epoch only decides whether
decryption is needed at all (`Epoch > 0`).

**`shardID`, `replicaID` and `index` are required** on every encrypt and decrypt
path. They rebuild the positional AAD. A mismatch fails authentication, and
there is no weaker retry.

**Frames are pinned while in use.** Every epoch a snapshot's frames reference is
registered in flight (`RegisterInFlightEpoch`) across encryption, decryption for
send, and re-encryption, so a concurrent `PurgeEpochsBefore` cannot drop a DEK
mid-operation. Every frame is sealed with `EncryptWithEpochPinnedContext`, which
seals and pins under one lock; it is a required method of `Barrier`.

**Pin balance is an invariant.** Every `RegisterInFlightEpoch` has a matching
`DeregisterInFlightEpoch` on every path, error paths included. A leaked pin
blocks a DEK purge forever; `pkg/snapshot_pin_balance_test.go` holds the rule.

**The purge floor counts shards that are not loaded.** A DEK may be purged only
below the lowest epoch every retained artifact needs, taken over:

- the snapshot directories of every loaded shard;
- the LogDB's recorded snapshot for every `(shard, replica)` it holds that is
  not loaded;
- the WAL's live floor.

The middle term exists because each shard has its own snapshot tree
(`…/snapshots/shard-N`, or its `DataDir`), so no scan of loaded shards sees a
stopped shard's snapshots, and a co-hosted shard's rotation could otherwise
purge the key a stopped shard needs to restart. On the per-shard barrier path
each unloaded shard is attributed through the barrier resolver. If
`ListNodeInfo` fails or a record cannot be read, nothing is purged. Only the
recorded snapshot (the one a restart recovers from) is counted; a stopped
shard's older, superseded snapshots may become undecryptable after a purge,
which is accepted.

**nil means off; sealed means fail closed.** A nil barrier is a plaintext side.
A non-nil, sealed barrier means encryption is configured and unavailable, and
the operation returns `ErrSnapshotBarrierSealed` instead of falling through.
Reading an encrypted source under a sealed barrier would copy ciphertext and
record it as plaintext; a sealed destination would durably downgrade a snapshot
that must be encrypted. The save, the receive (`HandleSnapshot`),
`ImportSnapshot` and `ReencryptSnapshotFile` all enforce this. Reading an
encrypted artifact with no usable barrier is
`ErrSnapshotEncryptedBarrierUnavailable`.

**A barrier that cannot bind context is not handed one.** `NewSnapshotAAD` asks
`SupportsContextAAD()` first, and the write fails closed instead of dropping the
binding. `seal.Barrier`'s `ContextBinding()` and `IsPlaintextMode()` tell apart
the two reasons a barrier cannot bind: no-encryption mode, where there is
nothing a position could bind to, and a delegated encryptor that protects the
data but cannot authenticate where it belongs. Only the second is a security
limitation.

**Integrity is checked against the file as stored.** `DataHash` is xxhash64 of
`snapshot.dat` before decryption and decompression; `DataLen` is its length.

- Every writer (save, receive, import, `ExportPath` export) stamps both through
  `MarshalSnapshotMeta`. A failure to hash the artifact fails the write before
  the directory is published: nothing is recorded in LogDB, nothing is
  compacted, and the next save retries.
- Recovery verifies both. A recorded snapshot whose `snapshot.meta` is missing,
  unreadable, malformed, or without the fields (`DataHashRecorded == false`,
  which includes every pre-magic record) fails closed with `ErrSnapshotCorrupt`
  (internally `*SnapshotMetaInvalidError`). There is no unverified path. A
  recorded directory whose `snapshot.dat` is missing is reported as a missing
  artifact first.
- The transport send and `ExportSnapshot` verify while they read, with no second
  pass. A send withholds its final chunk until the check passes, so a follower
  installs nothing from a damaged file. A plaintext export withholds its final
  bytes; an encrypted export is authenticated frame by frame but has written
  its plaintext before the recorded-hash check runs, so on any `ExportSnapshot`
  error the caller must discard what its writer received.

**The export artifact verifies itself.** `ExportSnapshot` ends the artifact with
a trailer (`QRSNTRLR`, the payload length, the xxhash64 of the raw header
followed by the payload as written) and writes it last, only once every check
has passed. `ImportSnapshot` checks the header bytes it parsed and the payload
against the trailer inside the copy that writes the staged `snapshot.dat`
(`artifactPayloadReader`), so a truncated, damaged, re-headed or trailerless
artifact is refused before the stage is published, recorded or seen by raft.
Both sides use one encoder and decoder, `pkg/snapshot_artifact_trailer.go`.

**Receive is bounded before it is committed.** `maxAdmissibleChunkCount` rejects
an inflated declared count before any memory is allocated. The receiver refuses
a transfer whose chunk count differs from the header's, is zero, or was marked
truncated.

**Received snapshots are adopted by raft before anything else is recorded.** The
LogDB record, the raised log floor, directory reclamation and state-machine
recovery wait for raft's verdict on the install; a refused or declined install
discards the unrecorded directory and changes nothing else (see
[architecture.md](architecture.md#data-flow)).

**Snapshot streams stay out of the stream pool.** A dedicated frame flag routes
them to a separate handler, so a multi-gigabyte transfer cannot occupy the pool
ordinary replication uses.

**The snapshot pool is separate from the worker tiers**: `max(NumWorkers/2, 2)`
workers, so a large save or recovery cannot stall step, commit or apply.

## 3. Error semantics

| Error | Raised when | Fail closed? |
|---|---|---|
| `ErrSnapshotBarrierSealed` | a non-nil barrier on either side is sealed | yes; never falls through to plaintext |
| `ErrSnapshotEncryptedBarrierUnavailable` | an encrypted artifact and no barrier able to open it | yes |
| `ErrSnapshotCorrupt` | a frame or the file fails verification against `snapshot.meta`, or the record cannot vouch for the file | yes |
| `ErrSnapshotInProgress` | a snapshot is already running for this shard | n/a |
| `*SnapshotFailedError` | the save failed | yes |
| `*SnapshotTooLargeError` | the state machine's output exceeds `MaxSnapshotSize`, measured after compression and before sealing | yes |
| `*SnapshotSendError` / `*SnapshotReceiveError` | the transfer failed | yes |
| `*SnapshotArtifactError` | an export artifact is malformed or fails verification: bad header, missing or malformed trailer, or a payload that does not match its trailer (which also matches `ErrSnapshotCorrupt`) | yes |
| `*SnapshotOptionError` (`ErrInvalidSnapshotOption`) | `SnapshotOption.Validate` rejected the request | yes |
| `*SnapshotCommittedNotDurableError` | see below | partially; read this one |
| `*ImportSnapshotDirExistsError` | the import destination directory already exists | yes |
| `*ImportSnapshotOutOfDateError` | the imported snapshot is older than what the replica holds | yes |
| `*ImportDataDirMismatchError` | `WithImportDataDir` names a directory that is not this replica's | yes |
| `*ImportLayoutUnknownError` | the on-disk layout cannot be determined | yes |
| `*ImportMembershipInvalidError` | the membership or identities supplied are unusable | yes |
| `*ReplicaRetiredError` (`ErrReplicaRetired`) | the target replica ID was retired by `RemoveData` | yes |
| `engine.ErrScratchNotUnlinked` | a decrypt scratch could not be unlinked; recovery or send is retried later | yes |

### The one error that is not simply a failure

`*SnapshotCommittedNotDurableError` reports a re-encryption whose destination is
committed but not yet durable: the rename succeeded, so `dstPath` already holds
the migrated artifact sealed at `Epoch`, and only the directory fsync that makes
the rename survive a crash failed.

It exists because the `(epoch, error)` pair `ReencryptSnapshotFile` returns
cannot carry this case on its own: the usual `if err != nil { return 0, err }`
drops exactly the epoch it delivers.

**A caller that gets this error must record `Epoch`.** Dropping it lets the next
DEK purge orphan the artifact's key, and the artifact can then never be opened.
Treat the migration as unverified until the destination is checked again.

Every other failure before the rename removes the temp file and leaves both
`srcPath` and `dstPath` untouched.

## 4. Configuration

Per shard, on `config.Config`:

| Field | Default | Meaning |
|---|---|---|
| `SnapshotEntries` | 100000 | applied entries between automatic snapshots. Zero selects the default; `config.SnapshotsDisabled` turns automatic snapshots off. |
| `CompactionOverhead` | 5000 | entries kept below the snapshot when the log is compacted |
| `MaxSnapshotSize` | 4 GiB | measured on the state machine's output after compression and before sealing, not on the framed ciphertext |
| `SnapshotCompressionType` | `config.NoCompression` | `NoCompression` (0) or `Snappy` (1) |

Per host, on `config.TransportConfig`:

| Field | Default | Meaning |
|---|---|---|
| `MaxSnapshotReceiveRate` | 256 MiB/s | inbound bandwidth limit |
| `MaxConcurrentSnapshotReceives` | 4 | concurrent inbound transfers |
| `MaxSnapshotReceiveMemory` | 1 GiB | total receive buffer pool |

With compression and encryption both on, the order is compress, then encrypt.
The compression an artifact was written with is recorded with it and travels on
the wire as `proto.Snapshot.Compression`; recovery always uses the recorded
value, never the current configuration.

## 5. Persistence

```
<NodeHostDir>/snapshots/shard-<N>/replica-<N>/snapshot-<index:020d>/
    snapshot.dat     state machine bytes; when encrypted, [ChunkLen:4 LE | EncryptedChunk]
                     frames of 64 KiB plaintext each
    snapshot.meta    88 bytes: QRSNAPMD header + Index, Term, ShardID, ReplicaID,
                     CreatedAt, Epoch, Compression, DataLen, DataHash
```

The tree roots at `<Config.DataDir>/shard-<N>/` for a shard with a `DataDir`.

`snapshot.meta` is self-describing and grows only by appending body fields. The
parser still reads older records for what they carry (the DEK purge-floor scan
learns an epoch from one), but recovery refuses any record without
`DataLen`/`DataHash`. The byte layout is in the `snapshot.meta` section of
[Compatibility](../compatibility/architecture.md).

**`ExportPath` exports** write the same pair. Each file is staged as a temp in
the export directory, fsynced, closed and renamed into place, `snapshot.dat`
first and `snapshot.meta` last. The export directory is then fsynced, along with
every directory the export created and the existing ancestor's entry for the
first of them, so a new export directory survives a power loss. Any failure,
including reading the source record or writing the exported one, fails the
export and with it the save.

Publishing over a previous export is failure-safe. The previous pair is moved
into a backup directory (`.snapshot-export-prev-*`) inside the export directory,
and that move is fsynced before the new `snapshot.dat` is renamed in, so a crash
can never show the new data beside the old record. A failure before the new
pair is durable restores the previous pair (a first export removes the half it
published), and the backup is removed only once the new pair is durable. A crash
leaves either a complete pair, or a directory every verifier refuses (no
`snapshot.meta`, or halves whose recorded hash disagrees with the data), and the
previous pair survives whole across the export and backup directories until the
new one is complete. The next successful export into the directory removes any
leftover backup or temp file and fsyncs the directory.

Snapshot metadata is also persisted through `logdb.SaveSnapshot`, which sets the
log compaction floor. The membership inside that record carries the WAL's own
`membershipSectionVersion`, separate from `proto.WireVersion`; see
[the log-storage engineering doc](../log-storage/engineering.md#record-layouts).

## 6. Test strategy

Run with `go test ./pkg/ ./pkg/internal/engine/` (both are part of `make test`).

| File (in `pkg/` unless noted) | Pins |
|---|---|
| `snapshot_durability_test.go` | the temp, fsync, rename, fsync-directory sequence |
| `snapshot_crashfs_test.go`, `snapshot_fsfailure_test.go` | crash and I/O failure at each step of that sequence |
| `snapshot_integrity_test.go` | `DataLen`/`DataHash` verification, and refusal of missing or unusable metadata |
| `internal/engine/snapshot_stamp_export_test.go` | a hash failure fails the save before publish, record or compaction; export metadata is required and verifiable |
| `snapshot_frames_test.go` | the length-prefixed frame layout |
| `snapshot_artifact_trailer_test.go` | the export trailer: round trip, and refusal of truncated, damaged (payload or header), trailerless and failed-export artifacts with nothing adopted |
| `snapshot_decrypt_sweep_test.go` | decryption across epoch combinations |
| `snapshot_pin_balance_test.go` | every in-flight pin is released on every path, error paths included |
| `snapshot_reencrypt_test.go`, `snapshot_reencrypt_commit_test.go` | migration, including the committed-not-durable case |
| `snapshot_recv_staging_test.go` | bounded receive and staging |
| `snapshot_sender_test.go` | chunked send |
| `snapshot_option_test.go` | `SnapshotOption.Validate` |
| `host_import_*_test.go` (ten files) | import: artifact, compaction, layout, lock lifetime, membership, non-destructive, ordering, retry, staging sweep, start exclusion |
| `host_snapshot_catchup_wedge_test.go`, `host_snapshot_gc_test.go` | the catch-up wedge and snapshot GC |
| `host_snapshot_failure_events_test.go`, `host_snapshot_noop_save_events_test.go` | a failed save does not fail the shard; a pre-restore recovery failure is rejected, not latched; a `SaveSnapshot` panic reaches the requester as a failed save; a no-op save fires no event |
| `snapshot_listener_dispatch_test.go`, `host_snapshot_reject_dispatch_test.go` | the engine's snapshot callbacks reach the user's listener; an inbound transfer the receiver rejected is delivered on the listener dispatcher |
| `internal/engine/snapshot_recover_reject_test.go`, `internal/engine/recovery_redrive_test.go` | pre-restore errors reject, post-restore errors latch; the re-drive backoff, the five-attempt budget over one holding episode, and a busy pool not counted |

Fault-injection seams: the host's snapshot filesystem is an interface, so the
crash-simulating filesystem (`snapshot_crashfs_test.go`) and the failing
filesystems in `snapshot_fsfailure_test.go` reach every save, receive, import
and recovery path, including the decrypt scratch. The `test/errfs` and
`test/netfault` suites do not exercise snapshots.

**Never delete `snapshot_pin_balance_test.go`.** A leaked pin has no symptom
until a DEK purge silently does nothing, and by then the cause is weeks old.

## 7. Observability

**Metrics**, through the host's `MetricsCollector` (see
[observability](../observability/engineering.md)): `IncSnapshot` per shard,
`IncSnapshotsSent` and `IncSnapshotsReceived` with bytes and duration,
`IncSnapshotsRejected` with a reason, and `IncUnauthorizedSnapshotDropped`.
Watch three things:

- **Time since the newest snapshot, per shard.** A shard that has stopped
  snapshotting will not compact its log, and its old DEKs can never be purged.
  Track it from `IncSnapshot` or the `OnSnapshotCreated` event.
- **Rejected transfers** by reason.
- **Any `ErrSnapshotCorrupt`**, which means a damaged or truncated artifact, not
  a transient error.

**Events.** `config.EventListener` carries `OnSnapshotCreated`,
`OnSnapshotSendStarted`, `OnSnapshotSent`, `OnSnapshotSendAborted`, `OnSnapshotReceived`,
`OnSnapshotRecovered` and `OnSnapshotCompacted` with `config.SnapshotInfo`
(shard, replica, index, sender). A save with nothing new to capture writes no
snapshot and fires no event. A recovery that fails or panics once the state
machine's `RecoverFromSnapshot` has been invoked fails the shard and fires
`OnShardFailed` once. An error or panic in a step before that (reading the
snapshot record, artifact verification, barrier resolution, decryption, or the
raft log never reaching the snapshot index) has not touched the state machine:
the recovery is rejected (`OnSnapshotRejected`, with the cause as
`SnapshotInfo.Err` and logged at ERROR), the recovering gate comes down, the
slots are released and the shard keeps running. Entries above the snapshot stay
held until a recovery of it succeeds, so the replica applies nothing onto the
untouched state machine meanwhile.

Nothing outside the engine retries that recovery (the leader saw the snapshot
acknowledged), so the step worker re-drives it through the same request path a
snapshot receive uses: after 100 ms, then doubling, capped at 5 s, while the
node is still holding entries and no recovery is running. Five consecutive
attempts are made over one continuous episode of holding entries, the first
included and whatever snapshot each is made against: a newer snapshot adopted
mid-episode does not grant fresh attempts. The fifth rejection fails the shard
with `*SnapshotRecoveryExhaustedError`, whose `Err` is the last cause and
`SnapshotIndex` the snapshot the last attempt was made against, and fires
`OnShardFailed` once. The count restarts only on a successful recovery, when
the node is no longer holding entries, and with a restarted shard. A re-drive
the pool cannot admit (its request queue is full) is not an attempt: it is
retried after the same backoff, counts nothing and reports nothing, so each
`OnSnapshotRejected` is one real attempt. The re-drive uses no goroutine or
timer of its own, so a stopped or unloaded node simply stops being re-driven.

A recovery holds the node's write lease from before the recovering gate is
raised until `RecoverFromSnapshot` returns, so `UnloadNode`, which waits out
every lease before the host closes the state machine, cannot complete under a
running restore; the restore is handed the node's stop channel and should
return promptly once it closes. A node that has already stopped refuses the
lease, and its recovery never touches the state machine.

A save takes no lease, so `UnloadNode` does not wait for a snapshot save to
finish. That is deliberate. A save only reads the state machine, and it can
stream for a long time after its capture point, so it should not hold up a
shard stop. The state machine adapter covers the save instead: the stream
holds `streamGuard` until it ends, so `Close` waits for it, and a save that
starts after `Close` returns `ErrClosed` (see
[state-machines/engineering.md](../state-machines/engineering.md)).

The re-drive bound matters because a replica holding entries
still votes and can win an election, and as leader it commits entries it
does not apply: the window in which it can do so is the backoff schedule
(100 + 200 + 400 + 800 ms) plus the five attempts themselves, after which the
latch takes it out of the protocol. This covers
the snapshot pool's runtime recovery only: `StartShard` restores on its own path
and still returns integrity failures as `*HostInitError` (`ErrSnapshotCorrupt`).
A save that returns an error fires no event, because the
shard keeps running: it is logged at WARN and returned to the `RequestSnapshot`
caller as a `*SnapshotFailedError`; for automatic saves, alert on the log line
or on `OnSnapshotCreated` going quiet. `OnSnapshotRejected` covers the other
save outcomes: a request the pool refuses (one already in flight, or a full queue; the
requester sees `ErrSnapshotInProgress` or `ErrBusy`, and `SnapshotInfo.Err` the
engine's internal duplicate or pool-busy error), an accepted
request abandoned when the pool stops (`ErrStopped`), and a save whose
`SaveSnapshot` panicked (`*SnapshotSavePanicError`; the shard keeps running).
It carries the cause in `SnapshotInfo.Err`, is delivered on the listener
dispatcher whatever produced it (the pool or the snapshot receiver), so a
listener may call `StopShard` from it, and also reports an inbound transfer the
receiver rejected.

**Logging** uses `log/slog`. Failed saves and recoveries, verification failures,
receive verdicts (declined, abandoned, kept) and epoch purges are logged with
the shard, replica and index. No snapshot contents, key material or nonce appears
in any log line.
