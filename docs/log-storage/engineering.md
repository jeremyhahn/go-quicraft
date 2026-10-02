# Log Storage — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/logdb`, `pkg/logdb/waldb`,
> `pkg/logdb/memdb` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

Import paths:

- `github.com/jeremyhahn/go-quicraft/pkg/logdb`: the `LogDB` interface and the
  storage-layer types
- `github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb`: the durable sharded-WAL
  implementation
- `github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb`: the in-memory
  implementation

Design rationale, rejected alternatives and the scope fence are in
[architecture.md](architecture.md). The call surface is in [api.md](api.md).
This document is the implementation contract behind both.

## The LogDB contract

`logdb.LogDB` is a supported extension point. Implementations must be safe for
concurrent use, and every implementation must meet the same contract so the
engine behaves identically on any of them. `memdb.MemDB` and `waldb.DB` are
both held to it by `pkg/logdb/conformance_test.go`.

- **`SaveState`** persists a batch atomically per Raft shard. It must not retain
  the caller's `Entry.Cmd` arrays. An `Update` whose first entry is at or below
  the node's compaction floor fails the whole batch with
  `*EntriesBelowCompactionFloorError` (`ErrEntriesBelowCompactionFloor`), before
  anything is written. A snapshot in an `Update` is persisted exactly as
  `SaveSnapshot` would persist it when its `Index > 0` and it is newer than the
  stored one; a stale one is ignored. Its index and term are checked against the
  protocol bound first (`Snapshot.CheckBounds`, which returns
  `proto.ErrLogIndexOutOfRange` or `proto.ErrTermOutOfRange`).
- **`IterateEntries`** returns `[low, high)`. `low >= high` returns the input
  unchanged. `low` at or below the compaction floor is `ErrCompacted`, which is
  what makes Raft fall back to `InstallSnapshot`. An unknown node, or a range
  holding no entries, is `ErrUnavailable`. A stored range that does not start at
  `low`, or has an interior hole, is `ErrLogGapDetected`: a short result would
  be a Log Matching violation handed to a follower with a nil error. Each entry
  costs `EntryOverheadBytes + len(Cmd)` against `maxSize` (0 is unlimited);
  iteration stops before the entry that would cross it, but an entry is always
  appended while the result slice is empty. Returned `Cmd`s must not alias the
  store (`CopyEntryCmd`). On an error the whole result is discarded.
- **`EntryRange`** returns the first index and the contiguous span
  `last - first + 1`, or `(0, 0, nil)` when there are no entries. A stored count
  that disagrees with the span is `ErrLogGapDetected`, never a short length.
- **`ReadState`** returns the hard state and the membership of the latest stored
  snapshot (an empty `Membership` when there is none), or `ErrNoState`.
- **`RemoveEntriesTo`** raises the compaction floor. A floor at or below the
  current one is a no-op. The floor is monotonic.
- **`SaveSnapshot`** refuses an index of 0, or one at or below the stored
  snapshot, with `ErrSnapshotOutOfDate`, and applies the same bound check.
- **`RemoveNodeData`** removes a node's data and retires its
  `(shardID, replicaID)`. From then on `NodeRemoved` reports `true`, across
  restarts, and `SaveState` (the whole batch), `SaveSnapshot`, `SaveBootstrap`
  and `RemoveEntriesTo` refuse the node with `ErrReplicaRetired`. The host
  refuses to start or import a retired replica.
- After `Close`, every method returns `ErrClosed`.

### Storage-layer types

These field lists are complete. A backend that persists only some of them
compiles and fails silently at runtime; the notes say what breaks.

**`Entry`**: `Index`, `Term`, `Type`, `Key` (proposal completion key),
`ClientID` (0 means not session-managed), `SeriesID`, `RespondedTo`, `OwnerID`
(the principal the embedder declared for the session; 0 means none), `Cmd`.

`Key`, `ClientID`, `SeriesID` and `RespondedTo` must be persisted, or session
deduplication, and with it at-most-once delivery, breaks across a restart.
Dropping `OwnerID` fails more quietly. It feeds the deterministic per-owner
session cap (`MaxSessionsPerOwner`), and 0 is both "no owner" and what a dropped
field decodes to, so the replica lifts the cap on every replayed entry and
admits registrations its peers refuse.

`EntryOverheadBytes` (84) equals `waldb`'s per-entry record header and must
track it exactly, so the same `maxSize` admits the same entries on every
backend.

**`State`**: `Term`, `Vote`, `Commit`, `VoteWindow`. `VoteWindow` is the vote
window, in nanoseconds, this replica may still owe a leader's read lease. A
backend that drops it brings a restarted replica up owing nothing, so it can
help elect a new leader while an old leader still counts its acknowledgement
toward a lease.

**`Membership`**: `ConfigChangeID`, `MaxWitnesses`,
`RequireFullReplicaMajority`, `SessionExpiryEntries`, `MaxTotalSessions`,
`MaxSessionsPerClient` (a response-cache depth, not a session count),
`MaxSessionsPerOwner` (0 disables it), `Addresses`, `Observers`, `Witnesses`,
`Removed`, `Identities`, `RemovedIdentities`.

- The witness and session-policy fields are replicated state, not local
  configuration. The deterministic apply path reads them, so a replica that
  evaluates a different value diverges its state machine with every Raft
  invariant intact.
- `MaxTotalSessions` is non-zero in every committed policy, which makes it the
  marker for "a session policy was recorded at all". That lets
  `SessionExpiryEntries == 0` keep its own meaning: expiry disabled.
- `Identities` is the committed node identity per replica, across every role.
  A replica that rebuilds its configuration from a snapshot whose log was
  compacted past the configuration entries learns identities only from this
  record. Drop it and, on a host with a transport, every member is missing an
  identity: their messages are refused and the shard stops making progress.
- `RemovedIdentities` is the identity a removed replica held when its removal
  committed, and is the deny-list checked against the authenticated connection
  identity. Its keys are a subset of `Removed`'s. Dropping it revokes a
  revocation: the replica forgets which keypairs it ejected and readmits a
  decommissioned node that still holds a valid certificate.

**`Snapshot`**: `Index`, `Term`, `Membership`, `Filepath`, `FileSize`,
`OnDiskIndex`, `Epoch` (barrier epoch, 0 for plaintext), `Compression`.
`Compression` describes the artifact, not the writer's configuration: every
recovery path builds its reader from this field, never from
`Config.SnapshotCompressionType`, or an artifact written before a configuration
change is decoded wrongly. The zero value is `config.NoCompression`.

**`Bootstrap`**: `Addresses`, `Join`, `Type`, `DataDir` (the shard's
`Config.DataDir`; empty when the shard roots under `NodeHostDir`),
`DataDirRecorded`. `DataDirRecorded` is a property of the stored record, set by
the backend, and a value passed to `SaveBootstrap` is ignored. It separates
"recorded as rooting under `NodeHostDir`" from "the record says nothing about
`DataDir`", because `""` is itself a meaningful value.

**`Update`**: `ShardID`, `ReplicaID`, `State`, `Entries`, `Snapshot`.

### Optional interfaces

The engine finds these by type assertion; semantics and the fail-safe table are
in [api.md](api.md#optional-interfaces). `waldb.DB` implements
`AtRestProtectedLogDB`, `EpochAwareLogDB` and `PerBarrierEpochAwareLogDB`.
`memdb.MemDB` implements `AtRestProtectedLogDB` and reports `AtRestNonDurable`.

- `MinLiveEpoch` is what keeps the snapshot pool from purging a DEK that a live
  WAL record still needs; purging it would make the next recovery fail with
  `*WALDecryptError`.
- `BarrierShardEnumerator` is an extra, stricter check for a multiplexing
  backend: when it answers, the snapshot pool declines to purge a barrier while
  any shard mapped to it is unloaded. It is not what protects a stopped shard.
  The pool accounts for every unloaded shard through `ListNodeInfo` and
  `GetSnapshot`, which every `LogDB` has, and declines to purge when
  `ListNodeInfo` fails.
- `AtRestProtection` exists because `waldb.DB` satisfies the per-barrier
  interfaces whether or not it was opened `WithBarrier`, so a type assertion
  alone would accept a store writing every command in the clear. A barrier
  running `StrategyNone` reports `AtRestUnprotected`. The answer must not depend
  on what is stored: an empty encrypted store reports `AtRestBarrierEncrypted`.

### Metrics

```go
type LogDBMetrics interface {
    ObserveWriteLatency(d time.Duration)      // per record write
    ObserveFsyncLatency(d time.Duration)      // per sync
    ObserveCompactionLatency(d time.Duration) // per Compact
    IncWriteBytes(n int64)                    // bytes written
}
```

`nil` disables collection. The concrete collector is the public `quicraft`
package's, which the host wires in when it has a `MetricsCollector`.

## Sharding and locking

`DB` holds 16 `walShard`s (`DefaultShardCount`). A Raft shard maps to
`shardID % 16`. With 10,000 groups that is about 625 groups per WAL shard; the
groups that share a shard share one segment file and one `fdatasync`, which
amortizes durability across them.

Each shard has two locks:

- **`mu`** guards segment-file state: the active segment, segment IDs, rotation,
  record writes, fsync and rollback. `SaveState` holds it from checkpoint
  through write, fsync and commit or rollback. Releasing it between the durable
  write and a rollback would let a concurrent batch on the same WAL shard (two
  Raft shards hashing to one WAL shard are served by different commit workers)
  commit past this batch's checkpoint, and the rollback would then truncate away
  that batch's acknowledged records.
- **`dataMu`** (`RWMutex`) guards only the in-memory maps that reads use:
  indexes, entries, states, bootstraps, snapshots, compaction floors, segment
  pins, the committed-segment high-water mark and the epoch floor. Readers
  (`IterateEntries`, `ReadState`, `EntryRange`, `GetSnapshot`, `GetBootstrap`,
  `CompactedTo`, `ListNodeInfo`, `MinLiveEpoch`) take `dataMu.RLock` and no
  `mu`, so they never wait on an fsync.

Lock order is always `mu` then `dataMu`. `dataMu` is never held across I/O.
Every mutator of a `dataMu` field also holds `mu`.

## On-disk layout

```
<dir>/
  WALFORMAT        16-byte marker: block size + format version
  RETIRED          retired-replica registry (absent until a replica is removed)
  shard-0/
    000000.log
    000001.log
  ...
  shard-15/
```

Segments are named `%06d.log`, starting at 0. `WALFORMAT.tmp` and `RETIRED.tmp`
may appear while a file is being published. None of these names parses as a
segment, so recovery ignores them.

### The format marker

`WALFORMAT` is `magic "QRWALFMT" (8) || version (4, LE) || blockSize (4, LE)`.
It lives in the root, not in each segment, because a per-segment preamble would
shift every record off the block grid recovery strides over.

- **`BlockSize` is immutable for the life of a data directory.** Segments carry
  no header, so recovery rebuilds the block grid from configuration alone, and a
  different size misparses every boundary. `Open` refuses a mismatch with
  `*WALGeometryMismatchError` (`ErrWALGeometryMismatch`), naming both sizes.
  To change it, start from an empty directory and re-sync from peers.
- **`MaxFileSize` is freely tunable.** It only decides when the writer rotates;
  recovery never reads it, so it is not recorded. It is capped at 4 GiB − 1
  because the generation start is a 32-bit file offset.
- **`walFormatVersion` is 1** and covers block framing and chunk framing
  together. It is compared for exact equality before a segment byte is read; any
  other value is `*WALFormatVersionError` (`ErrWALFormatUnsupportedVersion`).
  Without that check, a build with different framing would fail every CRC from
  the first chunk, find an empty clean prefix, and truncate the whole log as a
  "torn tail". Every format version stays pinned at its current value until the
  first production release, so a clean marker check does not prove the records match
  this build: record layouts are policed by the payload-type byte, one layer
  down.
- **A directory with no marker** gets one of two outcomes, decided before `Open`
  writes anything. With no segments anywhere, the marker is stamped before the
  first segment exists, and a failure to stamp is fatal. With segments present,
  the directory predates the marker and its framing, so `Open` refuses with
  `ErrWALFormatMissing`; guessing the framing would produce an empty clean
  prefix, not a detectable mismatch.
- **Publication is atomic**: write `WALFORMAT.tmp`, fsync it, rename it over
  `WALFORMAT`, fsync the directory. Every error path unlinks the temp file. A
  marker that is all zero bytes (including empty) is read as absent, since that
  is what an interrupted stamp leaves and the real magic is non-zero. A marker
  with other bytes that are not this format, or a block size that is not a
  power of two, is `ErrWALFormatMalformed`. Under `noSync` the syncs are skipped
  but the rename is not.

### The retired-replica registry

```
magic "QRRETIRE"[8] | count:u64 | count × (shardID:u64 | replicaID:u64) | crc32c:u32
```

Little-endian, entries sorted, CRC over every byte before it. The file is
rewritten whole on each retirement and published like the marker (temp, fsync,
rename, directory fsync). A registry that is present but has the wrong length,
magic or checksum fails `Open` with `ErrRetiredRegistryMalformed`: opening
without it would hand retired IDs back.

## Segment format

Each shard writes to one active segment. When the next record would not fit
below `MaxFileSize` (keeping room for a sync mark), the segment is closed and a
new one created. New segments are pre-allocated (`fallocate` on Linux,
`ftruncate` elsewhere) and truncated to their written size on close.

| Parameter | Default | Option | Rule |
|---|---|---|---|
| `BlockSize` | 32768 | `WithBlockSize` | power of two, at least `recordHeaderSize + 1` (16) |
| `MaxFileSize` | 67108864 | `WithMaxFileSize` | positive, at most 4 GiB − 1 |
| `NoSync` | false | `WithNoSync` | tests only |

`Options.Validate` and `Open` (`validateOpenOptions`) enforce the same rules.
`Options.MaxBatchSize` is deprecated and reaches nothing.

### Blocks and chunks

A segment is a sequence of `BlockSize` blocks. Records are written as chunks
into blocks. When a block has fewer than `recordHeaderSize + 1` bytes left, the
rest is zero-padded and the writer moves to the next block, so padding is never
more than `recordHeaderSize` (15) bytes. That bound is what lets recovery treat
a longer zero run with data after it as a write hole, not padding.

```
Chunk header (15 bytes, little-endian):
+----------+---------+---------+------------+--------------+------------+
| CRC32: 4 | Len: 2  | Type: 1 | DurSeq: 4  | GenStart: 4  | Payload: N |
+----------+---------+---------+------------+--------------+------------+
```

- **CRC** is CRC32-C (Castagnoli, hardware-accelerated) over type, `DurSeq`,
  `GenStart` and payload. It detects accidental corruption only; tamper
  protection is the barrier's AEAD.
- **Len** is the payload length. A chunk carries at most 65535 bytes
  (`maxRecordDataLen`) whatever the block size; `segment.Write` and the fit
  calculation apply the same cap, so blocks of 64 KiB or more still frame
  correctly.
- **Type** is one of:

| Value | Constant | Meaning |
|---|---|---|
| 1 | `RecordFull` | a whole record in one chunk |
| 2 | `RecordFirst` | first chunk of a split record |
| 3 | `RecordMiddle` | middle chunk |
| 4 | `RecordLast` | last chunk |
| 5 | `RecordSyncMark` | header-only witness appended after a completed `fdatasync`; carries no record |

Anything outside 1..5 is rejected as corruption before the CRC is checked.
Keeping the range tight makes a random or forged header byte less likely to
pass that screen.

### Durability sequence and generation start

`DurSeq` is the number of `Sync` calls the segment had completed when the chunk
was encoded. It splits a segment into durability generations:

- a chunk in generation `g` was written before `Sync #(g+1)` was called;
- the writer writes generation `g+1` only after `Sync #(g+1)` returned;
- so any chunk on disk above generation `g` proves every chunk at or below `g`
  was durable and acknowledged.

`GenStart` is the file offset where the chunk's generation began, which is the
acknowledged end at the moment the previous `fdatasync` returned. Any chunk that
survives above damage therefore names the acknowledged end exactly.

Rules that keep the proof valid:

- Both are stamped per chunk, not per block. A block holds many independently
  synced batches, and the damage and the batch boundary usually sit inside one.
- Every chunk of one record carries the same generation. `Write` runs under the
  shard lock and never syncs, so a split record is wholly inside the in-flight
  generation or wholly below it.
- `Sync` advances `DurSeq` only after the `fdatasync` returns. A `Sync` with
  nothing new written since the last one advances nothing and appends no mark.
- `DurSeq` is capped at `maxDurabilitySeq`; at the cap `CanFit` reports the
  segment full, so the counter can never wrap and make an old generation look
  newest.
- `DiscardPending` (rollback) does not rewind `DurSeq`: a completed sync is a
  fact about the medium.

The cost is 8 bytes per chunk. Measured, it is within noise on end-to-end
`SaveState` and adds no allocations.

### The sync mark

After its `fdatasync` returns, `Sync` appends one `RecordSyncMark` chunk stamped
with the generation it just opened and the acknowledged end, and writes it to
the file without a second `fdatasync`. A generation is proven closed only by a
chunk of a higher generation, and a segment's last generation has none when
nothing follows it (an idle node, a clean shutdown, a rotated segment). With the
mark, damage inside that final generation takes the fail-closed path. If the
mark itself did not survive (a crash in the writeback window after the last
`fdatasync`), the file looks exactly like an unfinished batch, and truncating is
the right verdict.

- A mark starts only where a full header fits, so the padding it can force reads
  as block padding. `CanFit` keeps `syncMarkReserve` free below `MaxFileSize`
  so a mark never lands past the limit.
- Rotation's closing `Sync` still fdatasyncs, which puts the last batch's mark on
  the medium. `segment.Close` fdatasyncs once more for a segment closed without
  rotating.
- After a process crash, the last mark may still be only in the page cache.
  Recovery fdatasyncs the highest surviving segment before the next write can
  create a higher one, so a segment only becomes non-final with its mark on the
  medium.
- Replay never emits a mark. The cost is one 15-byte write per `fdatasync`,
  which the `fdatasync` dominates.

## Payload types

Each logical record starts with a one-byte payload type.

| Value | Constant | Status | Record |
|---|---|---|---|
| `0x01` | unallocated | retired | an entry format with no session fields |
| `0x02` | unallocated | retired | a 40-byte hard state without `VoteWindow` |
| `0x03` | unallocated | retired | a snapshot format with no `MaxWitnesses` or policy flags |
| `0x04` | `payloadTypeBootstrap` | current | bootstrap |
| `0x05` | `payloadTypeCompact` | current | compaction floor marker |
| `0x06` | unallocated | retired | the remove-node tombstone, replaced by the `RETIRED` registry |
| `0x07` | unallocated | retired | a 76-byte entry header without `OwnerID` |
| `0x08` | unallocated | retired | a membership section starting at `ConfigChangeID`, with no version byte |
| `0x09` | unallocated | retired | a snapshot record with no `Compression` field |
| `0x0a` | unallocated | retired | a three-field session-policy tail |
| `0x0b` | `payloadTypeEntries` | current | entry batch, 84-byte per-entry header |
| `0x0c` | `payloadTypeSnapshot` | current | snapshot metadata, four-field session-policy tail |
| `0x0d` | `payloadTypeState` | current | hard state, 48 bytes |

**The rule for changing a layout.** A record layout is never changed under an
existing payload type. The new layout takes the next free byte, and the old byte
is retired: left unallocated and never reused. Replay refuses any byte that is
not a current type with the standard `ErrUnknownPayloadType`, which is fatal to
the open. There is no per-layout constant or per-layout error, because every
stale layout has the same remedy: wipe the data directory and re-sync from
peers.

The danger with a stale record is not that it fails to parse but that it
parses. Offsets shift instead of running off the end, so an old record can pass
every length check and decode into plausible garbage: an `OwnerID` read from
the command length, a membership deny-list located eight bytes late. Reusing a
retired byte would make a stale record indistinguishable from a current one.

An unknown byte has one other common cause: a keyed WAL opened with no barrier.
Every ciphertext begins with a big-endian epoch whose first byte is `0x00`, so
every record would decode as type `0x00`. Skipping them would boot the node as
an empty replica with no memory of its votes, which is why the error is fatal.

### Record layouts

All little-endian. The payload-type byte precedes each layout.

**Entry batch** (`0x0b`), repeating per entry:

```
[ShardID:8][ReplicaID:8][Index:8][Term:8][Type:8]
[Key:8][ClientID:8][SeriesID:8][RespondedTo:8][OwnerID:8][CmdLen:4][Cmd:N]
|<---------------- 84-byte header (entryRecordHeaderSize) ---------------->|
```

`Cmd` is capped at 8 MiB (`maxCmdLen`, matching `proto.MaxEntrySize`) on both
sides. `writeEntries` checks every entry before touching a segment and returns
`ErrCmdTooLarge`; without the write-side check an oversized `Cmd` would be
stored durable but unreadable. This layout is not `proto.Entry`'s (68-byte
header); the two are converted at the storage boundary. See
[the wire format](../wire-protocol/engineering.md).

**Hard state** (`0x0d`), fixed width; any other length is
`ErrTruncatedStateRecord`:

```
[ShardID:8][ReplicaID:8][Term:8][Vote:8][Commit:8][VoteWindow:8]   = 48 bytes
```

**Snapshot metadata** (`0x0c`):

```
[ShardID:8][ReplicaID:8][Index:8][Term:8][FileSize:8][OnDiskIndex:8][Epoch:8]
[Compression:2][FilePathLen:4]                    = 62 bytes (snapshotRecordBaseSize)
[FilePath:N][Membership section]
```

A `Compression` value this build cannot decompress fails replay with
`ErrUnsupportedSnapshotCompression`.

The membership section:

```
[MembershipVersion:1][ConfigChangeID:8][MaxWitnesses:8][PolicyFlags:1]
[NumAddresses:4]{[ReplicaID:8][AddrLen:4][Addr:N]}...
[NumObservers:4]{...}
[NumWitnesses:4]{...}
[NumRemoved:4]{[ReplicaID:8]}...
[NumIdentities:4]{[ReplicaID:8][IdLen:4][Id:N]}...
[SessionExpiryEntries:8][MaxTotalSessions:8][MaxSessionsPerClient:8][MaxSessionsPerOwner:8]
[NumRemovedIdentities:4]{[ReplicaID:8][IdLen:4][Id:N]}...
```

- `MembershipVersion` is `membershipSectionVersion` = 1, exact match; anything
  else is `ErrUnsupportedMembershipVersion`. See
  [Compatibility](../compatibility.md#format-versions-are-pinned-until-the-first-production-release).
- The fixed region is 18 bytes (`membershipFixedSize`), the session-policy tail
  32 (`membershipSessionPolicySize`), and the smallest section replay accepts is
  74 (`minMembershipSize`): fixed region, six empty count headers, policy. That
  floor is checked before the version byte is read, so a section too short to
  hold the layout is reported as a torn record, not read past its end.
- This is deliberately not `proto.Membership`'s layout. `PolicyFlags` packs
  `RequireFullReplicaMajority` into one byte, and the session policy is a tail
  rather than part of the fixed region, so the section can grow from the end
  without moving every map.

**Bootstrap** (`0x04`):

```
[ShardID:8][ReplicaID:8][Join:1][Type:8][NumAddrs:4]   = 29 bytes (bootstrapRecordBaseSize)
{[ReplicaID:8][AddrLen:4][Addr:N]}...
[DataDirLen:4][DataDir:N]
```

`DataDir` is a trailing field. A record that ends at the address map replays
with `DataDirRecorded = false`; one that carries a zero-length `DataDir` replays
as `DataDirRecorded = true` with an empty value. `DataDirLen` is bounded at 4096
(`maxBootstrapDataDirLen`).

**Compaction marker** (`0x05`):

```
[ShardID:8][ReplicaID:8][CompactedTo:8]   = 24 bytes
```

## Write path

### SaveState

```
SaveState([]Update)
  refuse if closed or poisoned (ErrClosed, ErrDBPoisoned); DiskMonitor.MaybeCheck
  lock every touched shard's mu, ascending index
  validate the whole batch: retired replicas, snapshot bounds, compaction floors
  Phase 1, batchMode = true (no fsync):
      flush the open block and capture each shard's checkpoint before its first write
      saveUpdate: entries, then hard state, then snapshot (each only if present)
      stage in-memory changes in pendingMemUpdates
      any failure: roll every shard back to its checkpoint
  Phase 2: fdatasync every dirty shard, in parallel when more than one
      any failure: poison the DB, return ErrDBPoisoned joined with the cause
  Phase 3: apply staged changes under each shard's dataMu   <- commit point
  unlock, descending index
```

This makes the fsync count O(shards touched), at most 16, instead of
O(records). Properties that must hold:

- **Every touched shard's `mu` is held for the whole batch**, checkpoint through
  commit or rollback (see [Sharding and locking](#sharding-and-locking)).
- **Locks are taken in ascending shard order**, so batches with overlapping
  shard sets cannot deadlock. Single-shard lockers (`Compact`, `SaveSnapshot`,
  `RemoveEntriesTo`, `RemoveNodeData`) hold at most one `mu`.
- **Validation runs before Phase 1** under every touched lock, so a refusal
  costs no write and no rollback. Checking retirement under the shard locks
  means a concurrent `RemoveNodeData` either precedes the check or follows the
  whole batch and drops it.
- **Nothing in Phase 1 touches the in-memory maps.** A mid-batch failure cannot
  produce a phantom read.
- **The staged epochs are committed even when Phase 2 fails.** The records are
  on the medium and nothing truncates them, so they may replay on the next start
  and their DEKs must stay retained.

`SaveState` is atomic per WAL shard, not across shards. A crash after a
non-fsynced partial write loses only unacknowledged records. Raft tolerates
that: it re-sends uncommitted entries, each record is independently framed, and
hard state is idempotent to replay.

### Per update

`saveUpdate` writes entries, then hard state (when it is not the zero `State`),
then snapshot metadata (when `Index > 0` and newer than both the stored snapshot
and any earlier one in the same batch).

The entries of one update may span several records. A record must fit inside one
segment, its usable payload is smaller than the segment (every block it spans
costs a chunk header), and the WAL adds an 84-byte header per entry that Raft's
batch cap does not count. `writeEntries` splits a large batch into as many
records as it needs; a lagging follower catching up on many small entries
legitimately produces such a batch. A single entry too large for any record
returns `ErrRecordTooLarge`. Splitting is safe on replay, and across a crash,
because the entry records still precede the update's state record and the WAL
recovers at a prefix: a torn tail can lose the state while keeping entries,
never the reverse.

The write path copies every field of every `Entry`, all five session fields and
a fresh `Cmd`, before staging it. Dropping the session fields here would
silently disable deduplication across a restart.

### writeRecord

`writeRecord` is the lowest level: ensure the active segment exists (creating
the shard directory and segment on first write), reserve the landing segment,
seal the record when a barrier is configured, append it to the block buffer,
and return a `recordLanding` naming the segment and the epoch.

- **The landing is reserved before sealing** (`reserveLanding`), with the framed
  length bounded by `len(data) + maxCiphertextExpansion`. The context-AAD names
  the segment and record ordinal, so both must be final at seal time. `CanFit` is
  monotonic in the length, so a generous bound only rotates early.
- **A record is sealed once.** If a positionally bound record still does not
  fit, `writeRecord` returns `ErrLandingSegmentOverflow` instead of writing a
  ciphertext where its AAD does not point. An unbound record may rotate and
  retry.
- **Inside a batch, no fsync.** Standalone writers (`RemoveEntriesTo`,
  `SaveSnapshot`, `SaveBootstrap`) run with `batchMode` off and `writeRecord`
  syncs inline; a failure poisons the DB.
- **The landing is returned, not applied**, because the record is not durable
  yet. Each caller folds it into the epoch floor at its own commit point, so a
  rolled-back batch contributes nothing. The one exception: when the record was
  written and only the inline `fdatasync` failed, the epoch is applied at once.
  Those bytes may be on the medium, and an epoch floor above a record recovery
  must decrypt stops the node from starting.

### Sync and flush

`segment.Sync` flushes the open block's unflushed bytes, calls the file's `Sync`
(`fdatasync` on Linux, `fsync` elsewhere), and then, if anything was written
since the last sync, advances `DurSeq`, records the new generation start and
appends the sync mark.

`flushBlock` writes only `blockBuf[flushed:blockOffset]`, at its file position,
and advances the watermark. It never rewrites bytes an earlier `fdatasync`
already made durable. That is a durability rule first: rewriting an
acknowledged record, such as a hard state carrying a vote, inside a later batch
puts it back at risk of a power loss that has no right to touch it. It also
avoids quadratic write amplification when many small syncs fill one block.

`fileOffset` moves only in `padAndFlushBlock`, by exactly one block, after the
block is complete on disk. Recovery therefore always finds a chunk header at
every multiple of `BlockSize`. `DiscardPending` rewinds the watermark, offsets
and record ordinal to the checkpoint together.

Directory fsyncs:

- At `Open`, the WAL root's whole created ancestor chain is fsynced. `MkdirAll`
  may create several levels, and POSIX durability needs each new directory's
  parent synced, or a crash after an acknowledged write can take the tree with
  it and the node boots with no term and no vote.
- When a lazy shard creates its directory, the WAL root is fsynced.
- After each new segment is created, the shard directory is fsynced. If that
  fails, the segment is closed and the write fails.

## Read path

All reads are served from memory; the WAL is never read outside recovery.
Readers take `dataMu.RLock` only.

Each `(shardID, replicaID)` has an `index` of `indexEntry{Index, Term,
SegmentID, Offset, Size}` sorted by index, where `Size` is the `maxSize` charge.
`Lookup` and `Range` binary-search it. `Append` handles a leader overwrite: an
entry at or below the tail truncates everything from that index before
appending. `Compact(upTo)` drops entries at or below `upTo`. `AllSegmentIDs`
feeds garbage collection. Entry payloads live in a separate per-node map.

`IterateEntries` follows the contract above. On a gap it logs at `ERROR`
naming the shard, replica and indexes. Every returned entry goes through
`logdb.CopyEntryCmd`, one copy per entry, so a caller that keeps one `Cmd` does
not pin the whole batch.

## Encryption

`WithBarrier` encrypts every record before it is written and decrypts during
recovery. The barrier must be initialized and unsealed before `Open`. Each
ciphertext starts with the 8-byte big-endian epoch (`ciphertextEpochSize`), so
the epoch is readable without decrypting and a DEK rotation needs no rewrite of
the WAL. Keys, epochs and algorithms are in
[the barrier documentation](../seal-barrier/architecture.md).

A configured barrier does not by itself mean records are encrypted. Under
`StrategyNone` it is an identity pass-through. The WAL accepts that, because it
is not the layer that decides whether a deployment requires encryption, but it
reports `AtRestUnprotected`, so the host's requirement fails closed where the
policy lives.

### Positional context-AAD

The barrier binds only the epoch into the AEAD AAD, so two records under the
same DEK and epoch would authenticate anywhere, and an attacker with write
access could reorder, splice or drop records. Every encrypted WAL record also
binds its position:

```
"QRWAL1" (6) || WALShardIdx (8) || SegmentID (8) || RecordSeq (8)   = 30 bytes, big-endian
```

`WALShardIdx` is required because each shard has its own `%06d.log` ID space and
all share one barrier. `RecordSeq` is the 0-based ordinal of the logical record
within its segment, which is exactly the position `ReadAll` returns it at;
chunking does not affect it. `DiscardPending` rewinds the ordinal with the
offsets, or one rollback would leave the rest of the segment undecryptable.

The magic is a self-authenticating scheme tag. There is no on-disk version for
it and no epoch-only fallback: a reader that fails to authenticate never retries
without the context.

### Capability resolution at Open

`Open` resolves the barrier's context-binding capability once, before any shard
exists, and fails closed:

| Condition | Error |
|---|---|
| the barrier lacks the context methods entirely | `ErrBarrierContextUnsupported` |
| the barrier has the context methods but not `ContextBindingResolved` | `ErrBarrierContextBindingUnreportable` |
| the barrier is sealed, so the capability is not yet known | `ErrBarrierContextUnresolved` |

`SupportsContextAAD()` alone cannot be cached: `false` means both "cannot bind"
and "cannot say yet", and caching the second as the first would make a node that
starts sealed write unbound records forever. `ContextBindingResolved()`
separates them.

A barrier that resolves as unable to bind (a delegated hardware or KMS
encryptor without AAD support, or no-encryption mode) is accepted with a
one-time `WARN`, and its shards write unbound records. On that branch every
write re-checks the barrier; if it now reports it can bind, the write fails with
`ErrBarrierContextBindingChanged`. That is the one drift nothing else catches,
since `Host.EmergencySeal` can seal a barrier at runtime and an operator can
unseal it under a different strategy. The opposite drift fails closed already,
because `EncryptWithContext` refuses a context it cannot authenticate.

`maxCiphertextExpansion` (8 + 1 + 24 + 16 = 49 bytes) bounds `pkg/seal`'s
envelope `[epoch:8][algID:1][nonce:N][ciphertext+tag]` with the largest nonce
(XChaCha20-Poly1305) and a 16-byte tag. A delegated encryptor with a larger
envelope surfaces as `ErrLandingSegmentOverflow` near segment boundaries and
needs a segment size with headroom.

## Re-encryption

`Reencrypt` (`reencrypt.go`) copies a WAL from one barrier to another through
the public LogDB surface and returns the number of nodes copied.

1. Open the source with `srcBarrier`, the destination with `dstBarrier`.
2. Carry the source's retired replicas into the destination in one registry
   write.
3. Per node: bootstrap, latest snapshot metadata, compaction floor, the live
   entry range in 64 MiB batches (`reencryptBatchBytes`), then hard state.

- **Hard state goes last.** Each step is fsynced and the destination recovers at
  a prefix, so a crash mid-migration can lose state while keeping entries, never
  recover a commit index past the last durable entry.
- **`opts` apply to both sides**; `srcBarrier` and `dstBarrier` override any
  `WithBarrier` in them. Opening the source with the wrong block size is
  refused by the marker check, and opening the destination with the default
  would produce a directory the node's own configuration rejects.
- **The source is not opened read-only.** `Open` runs full crash recovery on it,
  which may repair a torn tail, delete orphaned segments and create an active
  segment. It never removes a record a normal open would have replayed.
- **Snapshot data files are not WAL records.** They are sealed by the host's
  barrier in the host's frame format. `SnapshotReencryptFunc` is the host's
  callback, called once per node with a snapshot, returning the destination
  epoch to record. It may be nil only when no barrier is involved or no node has
  a snapshot; otherwise `Reencrypt` fails with `ErrSnapshotReencryptRequired`
  rather than copy metadata pointing at a file the destination cannot decrypt.
  A node with no snapshot (`logdb.ErrNoSnapshot`) is copied without one.
- **`dstDir` must be empty.** The caller verifies the result and swaps the
  directories; a partial run never changes the source's Raft-visible content.
- **Closing the destination is part of the migration.** Its error is reported
  (`Op` `"close-dest"`), because `DB.Close` completes every active segment and a
  swallowed error would report success for an unfinished WAL. The source's close
  error is discarded.
- Errors are `*ReencryptError` with an `Op` naming the step (`open-source`,
  `open-dest`, `retire-nodes`, `list-nodes`, `get-bootstrap`, `save-bootstrap`,
  `get-snapshot`, `snapshot-reencrypt`, `save-snapshot`, `compacted-to`,
  `remove-entries-to`, `entry-range`, `iterate-entries`, `save-state`,
  `close-dest`).

## Durability guarantees

Data is durable when `fdatasync` returns: Phase 2 of a batch, or the inline sync
of a standalone write.

| Stage | After a crash |
|---|---|
| encoded into the block buffer | lost |
| written to the OS (`WriteAt`) | in the page cache; lost on power failure |
| `fdatasync` returned | durable; closes a generation, so later chunks prove it was acknowledged |
| in-memory maps updated | redundant; rebuilt by replay |

`WithNoSync(true)` skips every `fdatasync` and directory sync and leaves the WAL
root to be created by the first write. Nothing is ever acknowledged, every chunk
carries generation 0, and recovery may truncate anywhere. Tests only.

### Invariants recovery depends on

1. **A WAL is valid only as a prefix.** Nothing after the first damaged byte is
   replayed, whatever it decodes to.
2. **A chunk's durability generation never decreases within a segment.**
3. **An acknowledged byte is written exactly once** (the flush watermark).
4. **`fileOffset` moves only by whole blocks, after the block is complete on
   disk.**
5. **An fsync failure is terminal for the DB.** After `poison`, every write
   returns `ErrDBPoisoned` until the process restarts into recovery.
6. **Only `ErrNotExist` reads as absence.** Any other error from the `Stat` of a
   shard directory fails the open. Treating `EACCES`, `EIO` or `ENOTDIR` as
   absence would skip recovery, start segment IDs at 0, and let the first write
   recreate `000000.log` over the node's term, vote and entries.
7. **The compaction floor is monotonic.** A lower floor is dropped before it
   reaches the WAL, because persisting it would also move the garbage-collection
   pin and let the higher marker's segment be reclaimed.
8. **Entries at or below the floor are refused, not clamped.** Storing them would
   truncate the live suffix; clamping would discard entries the caller was told
   were persisted.

## Disk-full detection

`WithDiskMonitorOpts(maxBytes, ...)` installs a `DiskMonitor` when
`maxBytes > 0`. `SaveState` calls `MaybeCheck` before writing anything; a failed
segment pre-allocation forces an immediate check. Thresholds are fractions of
`maxBytes`, not of free space: past `DefaultWarningPercent` (0.85) the warning
callback fires; past `DefaultFullPercent` (0.95) the full callback fires and
`SaveState` returns `ErrDiskFull`. A check runs every `DefaultCheckEvery` (100)
writes. Callbacks fire on transitions only.

- **Once full, every call re-checks** until usage drops below the threshold.
  Followers' replication writes reach the monitor only through `SaveState` (the
  host's disk-full flag gates client `Propose` and `ReadIndex`), so sampling in
  the full state would admit writes until `fdatasync` hit ENOSPC and poisoned
  the DB.
- **If the usage query fails**, the monitor logs at `DEBUG` and does not block.
  That is fail-open on the monitor only: an unreadable `statfs` is not evidence
  of a full disk, and a real ENOSPC still surfaces from the write or the sync.

## Compaction

### RemoveEntriesTo

Writes a compaction marker, then prunes memory. It refuses a retired node,
ignores a floor at or below the current one, and captures a checkpoint first so
a failed write leaves nothing behind. The floor, index pruning, entry-map
pruning, segment pin, committed high-water mark and epoch floor all change in
one `dataMu` section, so a reader never sees the raised floor without the
pruning, which would read as a gap.

### Compact and garbage collection

`Compact(shardID, _)` deletes segment files that are:

- below the active segment (the active segment is never deleted, so an
  in-flight write never targets a deleted file);
- at or below `committedSegmentHWM`, the highest segment whose batch fully
  committed. Without this, a concurrent `Compact` could take the shard lock
  between a batch's durable write and its index update and reclaim the segment
  just written;
- not referenced by any entry index or by any node's meta-record pin.

**Meta-record pins.** State, snapshot, bootstrap and compaction records are not
in any entry index, so a segment whose entries were all compacted can still hold
the only copy of one. Losing a compaction marker, for example, resets the floor
on the next recovery. Each shard therefore keeps, per node, the segment holding
the latest record of each kind, with an explicit presence flag because segment 0
is a valid pin. Pins are set at each write's commit point and during replay,
last writer wins, and are folded into the live set before collection.

**Deletion durability.** After deleting, the shard directory is fsynced, or a
metadata-lazy filesystem could resurrect a deleted segment after a crash. A
failed directory fsync is logged at `WARN` and retried by the next compaction.

**The epoch floor is raised only on proven deletions.** After a successful
collection and directory fsync, the shard recomputes its minimum live epoch from
the segments actually on disk (never from the intended live set, since
collection deletes one file at a time and can stop partway). The engine purges
DEKs below that floor. Raising it on an unlink that a crash could undo would let
the DEK go while the segment reappears, and the next restart would fail with
`*WALDecryptError`. Deferring the raise is always safe.

### RemoveNodeData and retirement

`RemoveNodeData`, holding the shard's `mu`:

1. publishes the new `RETIRED` registry (temp, fsync, rename, root fsync);
2. drops the node's records and pins from memory;
3. only then makes the retirement visible to `NodeRemoved`, so a caller never
   sees it reported while readers still return the node's records;
4. reclaims the segments no live node references, as above.

A failure before the rename changes nothing. A failed root fsync after it
leaves the retirement's durability unknown, so the DB is poisoned. `RemoveNodeData`
writes no WAL record.

At `Open`, the registry is read before any segment, and once every shard has
replayed, `purgeRetired` drops each retired replica's records wherever they were
found. Replay finds such records only when a removal stopped before reclaiming
them, so `purgeRetired` runs the same reclaim, once per affected shard. Records
in a segment still shared with a live node are dropped on every open until that
segment can be collected.

## Recovery

### Open sequence

```
Open(dir)
  validateOpenOptions
  readWALFormat: block size and version must match           <- before any segment
  resolveBarrierContextBinding                                <- before any shard
  createWALRoot: MkdirAll, fsync the created ancestor chain
  establishWALFormat when no marker exists
  loadRetiredRegistry
  for each shard 0..15: openWALShard
      Stat(shardDir)
        exists       -> recover()
        ErrNotExist  -> lazy; nothing created
        other error  -> fail the open
  purgeRetired
```

`recover()`:

1. lists `%06d.log` files, sorts them, and advances `nextSegmentID` past every
   one observed, before replay. An orphan that could not be deleted can then
   never be ordered after a freshly written segment, where its stale hard state
   would win.
2. replays each segment in order; on damage, classifies it (below) and either
   repairs and stops or fails.
3. fdatasyncs the highest surviving segment (see [the sync mark](#the-sync-mark)).
4. reconciles compaction floors (below).

`recover()` takes no locks: the shard is not reachable by any caller yet. After
recovery, a shard that had segments gets a fresh active segment. If `Open` fails,
each shard already opened unlinks the empty segment it created, so repeated
failed opens during diagnosis do not pile up files.

### Scanning a segment

`segment.ReadAll` reads block by block and decodes chunks in order:

- an all-zero remainder of a block is padding;
- fewer than 15 bytes left moves to the next block;
- `RecordFirst`/`RecordMiddle`/`RecordLast` are reassembled; `RecordFull` is
  emitted as is; `RecordSyncMark` emits nothing;
- a CRC failure, an invalid type, or a zero run with data after it (a write
  hole) ends the clean prefix. The scan does not resynchronize and resume.

A real I/O error from a block read (EIO, a failing sector) is never treated as
end of data, however many bytes came back. It returns `*SegmentReadError` inside
`*RecoveryError`.

### Replay

Each record is decrypted (under its positional context) when a barrier is
configured, then dispatched by payload type:

- entries go into the index and entry map; a batch that starts at an existing
  index is a leader overwrite, and the superseded suffix is removed from both;
- state, snapshot and bootstrap records replace earlier ones, last writer wins;
- compaction markers prune the index and entry map;
- every record re-establishes its segment pin and epoch-floor contribution.

**No replay failure is skipped.** A record that passed its CRC and fails to
decode is corruption the CRC did not explain. Skipping it while later records
replay would leave a silent hole: a dropped entry batch is a Log Matching
violation; a dropped state record lets an older one win and regresses term and
vote (a double vote); a dropped snapshot record boots the node with empty
membership.

### Classifying damage

Truncating to the clean prefix is correct for an interrupted write and destroys
acknowledged data otherwise, and the framing alone cannot tell them apart. A
batch larger than a block is written one block at a time before a single
`fdatasync`, so writeback can persist any subset: losing a page of block N while
block N+1 survives leaves CRC-valid records above a CRC failure, exactly what
bit rot inside a live segment leaves.

After the first failure, the scan keeps probing for chunks that still decode
above the damage: byte by byte within the damaged block (each candidate
validated by its own CRC), then in later blocks, stopping as soon as the answer
is settled. Nothing it finds is replayed. The verdict:

| Found above the damage | Meaning | Action |
|---|---|---|
| nothing | torn tail | repair and continue |
| only the segment's highest generation, and the survivors' `GenStart` is at or below the damage | interrupted write: no completed `fdatasync` covers the damaged bytes | repair and continue |
| chunks spanning more than one generation | a completed `fdatasync` sits above the damage | refuse: `ErrMidSegmentCorruption` |
| a survivor whose `GenStart` is above the damage | the damage began below the acknowledged end | refuse: `ErrAcknowledgedDataLost` |
| a generation lower than one in the clean prefix | bytes this writer cannot have produced (misdirected write, partial restore, tampering) | refuse: `ErrRecordSequenceRegression` |
| damage in a segment that is not the highest | acknowledged by construction: a segment was fully synced before a higher one existed | refuse: `ErrNonFinalSegmentCorruption` |

A refusal leaves every file byte-for-byte intact and returns a
`*SegmentCorruptionError` (`ShardIndex`, `SegmentID`, `CleanEnd`) wrapped in
`*RecoveryError`.

A zero hole with data after it is judged by the same evidence as a CRC failure.
A crash cannot produce a hole under acknowledged data (writes ascend in file
offset, so a block below a later generation was covered by that generation's
`fdatasync`), so a hole whose survivors span a completed sync is media damage
or tampering.

**Repair runs high to low.** Segments above the damage are deleted first and the
directory fsynced; only then is the damaged segment truncated to its clean end
and fsynced. Every intermediate crash state is still a valid prefix: the damaged
segment keeps its damaged tail, so the next recovery stops at the same offset
and repeats the same repair. Truncating first would let a restart replay the cut
segment cleanly and then the higher segments, resurrecting records past a hole.
If a higher segment cannot be removed it is truncated to zero and fsynced; if
neither works, recovery fails, because an intact orphan would replay stale
records.

Truncating at all is what keeps a torn tail from being re-detected on every
boot, where the discard would delete every segment written since.

**Remaining limits:**

- Rot inside the final generation is truncated, not refused, when its sync mark
  did not persist. That needs rot and a crash inside the writeback window after
  the last `fdatasync`.
- Under `noSync`, truncation is always permitted, since nothing was ever
  acknowledged.

### Recovery error reference

| Error | Raised when | Recovery |
|---|---|---|
| `ErrWALGeometryMismatch` (`*WALGeometryMismatchError`) | configured `BlockSize` differs from the marker | restore the original `BlockSize`; the error names both |
| `ErrWALFormatMalformed` | the marker has non-zero bytes that are not this format | investigate the file. An empty or all-zero marker is read as absent. |
| `ErrWALFormatUnsupportedVersion` (`*WALFormatVersionError`) | the marker records another format version | wipe the data directory and re-sync. See the `WALFORMAT` section of [Compatibility](../compatibility.md). |
| `ErrWALFormatMissing` | segments exist but no marker | wipe and re-sync |
| `ErrRetiredRegistryMalformed` | `RETIRED` does not parse | investigate; opening without it would readmit retired replicas |
| `ErrMidSegmentCorruption` | damage below records a completed `fdatasync` acknowledged | re-sync from peers, or authorize `waldb.Repair` |
| `ErrAcknowledgedDataLost` | damage begins below the acknowledged end the survivors name | re-sync from peers, or authorize `waldb.Repair` |
| `ErrNonFinalSegmentCorruption` | damage in a segment that is not the highest | re-sync from peers, or authorize `waldb.Repair` |
| `ErrRecordSequenceRegression` | a generation lower than the clean prefix's | re-sync from peers, or authorize `waldb.Repair`; treat the device as suspect |
| `*SegmentReadError` in `*RecoveryError` | an I/O error during the scan | hardware fault; re-sync from peers |
| `*WALDecryptError` | a record failed authentication | wrong barrier, wrong epoch, or tampering. Never retried without the context. |
| `ErrBarrierContextUnresolved` | the barrier is sealed at `Open` | unseal it before opening |
| `ErrBarrierContextBindingUnreportable` | the barrier lacks `ContextBindingResolved()` | add the method to the out-of-tree barrier; see [Compatibility](../compatibility.md) |
| `ErrBarrierContextUnsupported` | the barrier cannot bind caller context | use a context-capable barrier; there is no epoch-only fallback |
| `ErrUnknownPayloadType` | a record's payload type is retired or unknown | most often a keyed WAL opened without its barrier: supply it. Otherwise the directory holds a stale layout: wipe and re-sync. |
| `ErrUnsupportedMembershipVersion` | a snapshot record's membership version is not 1 | build skew; wipe and re-sync |
| `ErrUnsupportedSnapshotCompression` | a snapshot record names an unknown compression | build skew; wipe and re-sync |

These surface from `Open`. Three fail a running DB instead:

| Error | Raised when | Recovery |
|---|---|---|
| `ErrDBPoisoned` | an `fdatasync`, or the directory fsync after publishing a retirement, failed | restart the process into recovery; there is no in-process reset |
| `ErrBarrierContextBindingChanged` | a shard latched "cannot bind" and the barrier now reports it can | the barrier was resealed under another strategy; restart the node |
| `ErrLandingSegmentOverflow` | a sealed record no longer fits the segment its AAD names | unreachable with the software barrier; give a delegated encryptor more segment headroom |

### Recovering from a refusal

`ErrMidSegmentCorruption`, `ErrAcknowledgedDataLost`,
`ErrNonFinalSegmentCorruption` and `ErrRecordSequenceRegression` stop the node
on purpose. Above the damage are records it acknowledged to its leader, and
truncating past them would shorten the log it promised and roll term and vote
back. An interrupted write repairs itself, so reaching one of these means damage
an interrupted write does not explain: bit rot, a misdirected write, a lying
fsync, or tampering.

**Preferred: re-sync from peers.** Confirm the other nodes hold a quorum, stop
the node, wipe its `WALDir` and `NodeHostDir`, and start it. It rejoins by
snapshot plus log and loses nothing its peers hold.

**With no peers: authorized repair.** For a single-node deployment or a cluster
that has lost quorum, `waldb.Repair` truncates one segment at one offset and
deletes the segments above it. It is a separate call made by hand: not an
`Option`, not reachable from `Open`.

```go
var corrupt *waldb.SegmentCorruptionError
if errors.As(err, &corrupt) {
    res, rerr := waldb.Repair(walDir, waldb.RepairTarget{
        ShardIndex: corrupt.ShardIndex,
        SegmentID:  corrupt.SegmentID,
        TruncateAt: corrupt.CleanEnd, // must match exactly
    }, waldb.WithBlockSize(blockSize))
    // res.RecordsKept, res.BytesDiscarded, res.SegmentsDeleted
}
```

1. Copy the data directory first. Repair is destructive and has no undo.
2. Take the shard, segment and clean end from the `*SegmentCorruptionError`
   (also logged at `ERROR` by the failed open).
3. Run `Repair` with the database closed and no other process on the directory,
   with the block size the directory was written with.
4. Restart the node and reconcile it with any peers.

Guards, each of which refuses instead of proceeding:

- `TruncateAt` must equal the clean end the scan computes
  (`*RepairOffsetMismatchError`, `ErrRepairOffsetMismatch`, reporting both). An
  operator confirms the offset; they cannot choose one.
- A segment that is not corrupt is refused (`ErrRepairNoCorruption`), so a stale
  or copied error cannot truncate a healthy segment.
- A shard index outside `[0, 16)` is refused (`ErrRepairShardOutOfRange`).
- The block size is checked against `WALFORMAT` as `Open` does, because a
  repair on the wrong grid computes a meaningless clean end.
- No barrier is needed: the scan works on the chunk framing and never decrypts.

Every record above `TruncateAt` is destroyed, including the shard's newest hard
state. A replicated shard catches up from peers; on a single node the loss is
final. `Repair` logs at `ERROR` before acting and at `WARN` after, with the
offset, bytes discarded and segments deleted.

### Reconciling a lost compaction floor

The floor is durable only as a marker record, but its effect (the compacted
segment being unlinked) is irreversible. Tear the block holding the marker and
replay comes back with a lower floor, often 0, while the entries are gone. That
state fails open: a missing prefix keeps span equal to count, so `EntryRange`
accepts it, reads below the real floor return `ErrLogGapDetected` instead of
`ErrCompacted`, and Raft never falls back to a snapshot.

The last step of `recover()` (`reconcileCompactionFloors`) re-derives the floor
as `firstIndex - 1` from the rebuilt index when a recorded snapshot covers the
missing prefix, and logs a `WARN`. It never anchors on the snapshot index,
because the snapshot pool keeps `CompactionOverhead` entries below the snapshot
for follower catch-up, and anchoring there would mark them unreadable. When no
snapshot covers the prefix, nothing is changed: that shape is indistinguishable
from a node that legitimately starts at a high index. Making it fail closed
would need the floor to be durable outside one WAL block, which is a format
change.

## Filesystem abstraction

`FS` and `File` (see [api.md](api.md#the-filesystem-seam)) cover every
filesystem call. `osFS` is the production implementation; `osFile.Sync` is
`fdatasync` on Linux and `fsync` elsewhere. `WithFS` injects another.

- `Rename` is required, not optional. The marker and the registry are published
  by temp-fsync-rename, and an in-place fallback would bring back the torn state
  the rename removes. An `FS` that cannot rename cannot host a WAL.
- The segment writer uses `WriteAt` only, so a file's implicit position never
  disturbs the block grid.
- Directory syncs use `FileMetadataSyncer.SyncMetadata` (a full `fsync`), never
  `Sync`: on Linux `Sync` is `fdatasync`, which may skip exactly the directory
  entry changes a directory sync exists to persist.
- `FilePreAllocator.PreAllocate` reserves segment space.

## Test strategy

| Target | What it covers |
|---|---|
| `make test-waldb`, `make test-logdb` | unit and conformance tests, including crash simulation on an in-process crash filesystem (`crashfs_test.go`, `crash_durability_test.go`), durability generations (`durability_seq_test.go`), torn blocks and holes, mid-segment corruption, repair, retirement (`retired_test.go`, `node_retired_test.go`), poisoning, rollback under concurrency, aliasing and gap hardening |
| `make ci-errfs` | `test/errfs`: an `FS` that injects I/O errors into a real filesystem, exercising every error path through the public API |
| `make fuzz-waldb` | fuzzing of the chunk, entry, state and bootstrap codecs, the address and removed maps, and segment replay |
| `make bench-waldb` | write, fsync and compaction benchmarks; the hot path is benchmarked on any change |

`pkg/logdb/conformance_test.go` runs the same contract suite against
`memdb.MemDB` and `waldb.DB`.

## Observability

Metrics are injected, never global. `WithMetrics(m LogDBMetrics)` installs a
collector; `nil` costs nothing on the write path. The Prometheus collector and
metric names are in [observability](../observability/engineering.md). Write and
fsync latency are the pair to alert on: they separate "the disk is slow" from
"the pipeline is slow".

Logging uses `log/slog` with the caller's handler:

| Level | Event | Why it matters |
|---|---|---|
| ERROR | `fdatasync` failure poisoning the DB | the store has stopped accepting writes |
| ERROR | corruption under acknowledged data | the open is refused; see [Recovering from a refusal](#recovering-from-a-refusal) |
| ERROR | a gap on the read path | a hole in the stored log; the read fails |
| WARN | compaction floor re-derived at recovery | a compaction marker was lost |
| WARN | directory sync failed after garbage collection | deletions not yet durable; the epoch floor was not raised |
| WARN | a barrier that cannot bind context | records are encrypted without positional binding |

No key material, nonce or record payload appears in any log line. There are no
per-record counters: the write path is the hottest loop in the repository, and
the latency histograms cover the same ground per batch.

## Source files

| File | Holds |
|---|---|
| `logdb/logdb.go` | `LogDB`, optional interfaces, `AtRestProtection`, `EntryOverheadBytes` |
| `logdb/types.go` | storage-layer types, `CopyEntryCmd` |
| `logdb/errors.go` | `logdb` sentinels, `EntriesBelowCompactionFloorError` |
| `logdb/reader.go` | `LogReader` |
| `logdb/memdb/memdb.go` | the in-memory implementation |
| `waldb/db.go` | `DB`, sharding, `SaveState`, reads, replay, record layouts, recovery errors |
| `waldb/segment.go` | segment I/O, blocks, `Sync`, sync mark, `ReadAll` and its damage classifier |
| `waldb/record.go` | chunk encoding, chunk types, durability sequence, positional context-AAD |
| `waldb/format.go` | the `WALFORMAT` marker |
| `waldb/retired.go` | the retired-replica registry |
| `waldb/repair.go` | `Repair` |
| `waldb/reencrypt.go` | `Reencrypt` |
| `waldb/index.go` | the in-memory index |
| `waldb/compaction.go` | segment garbage collection |
| `waldb/copy.go` | deep copies on the read boundary |
| `waldb/options.go` | `Options`, defaults, validation |
| `waldb/errors.go` | sentinels and typed errors |
| `waldb/fs.go`, `sync_linux.go`, `sync_other.go` | `FS`/`File`, `osFS`, `fdatasync`/`fallocate` |
| `waldb/diskmon.go` | `DiskMonitor` |

## Related documentation

- [Wire format](../wire-protocol/engineering.md): `proto.Entry` and
  `proto.Membership`, which are separate layouts from the ones stored here
- [Seal barrier](../seal-barrier/architecture.md): epoch DEKs and sealing
  strategies behind `WALBarrier`
- [Compatibility](../compatibility.md): the pinned-at-1 policy and the version
  discriminators that exist
- [Multi-group host](../multi-group-host/engineering.md): the commit worker's
  group commit, `Peer.Commit`, and the in-memory log buffer that sit above this
  layer
- [Testing infrastructure](../testing/infrastructure.md): the `test/errfs`
  harness
