# WAL Persistence Layer

## Overview

The WAL (Write-Ahead Log) persistence layer provides durable storage for Raft log entries, hard state, snapshot metadata, and bootstrap configuration. It is implemented in `pkg/logdb/waldb/` and satisfies the `logdb.LogDB` interface defined in `pkg/logdb/logdb.go`.

Design principles:

- **Sharded writes**: 16 independent WAL directory shards reduce fsync contention across Raft groups. Each shard has its own segment files, locks, and in-memory state.
- **Write-ahead durability**: All state is written to the WAL and fsynced before being acknowledged, and the in-memory state is only updated afterwards. On recovery, all in-memory state is rebuilt by replaying WAL segments in order.
- **In-memory reads**: Reads are served entirely from in-memory maps and take no writer lock, so a reader never waits on an fsync. The WAL exists solely for durability; it is never read during normal operation.
- **Fixed binary encoding**: All records use little-endian fixed-layout binary encoding with no external serialization dependency (no protobuf). The one exception is the positional context-AAD of an encrypted record, which is big-endian.
- **Lazy initialization**: Shard directories and segment files are created on first write, not at Open time. This avoids creating 16 directories for nodes that may only use a few shards.
- **Fail closed**: Anything recovery cannot prove safe stops the open rather than being skipped or guessed at. A store that cannot establish durability after a failed fsync stops accepting writes entirely.

The layer is versioned as a whole at `walFormatVersion` = 1, pinned for the pre-release period. Layout changes are made in place and development data directories are wiped and re-synced rather than migrated. 0.5.x is not compatible with 0.4.x data directories: the per-entry WAL record header grew from 76 to 84 bytes, `proto.Entry`'s header from 60 to 68, and the seal MAC moved from v1 to v2.

That break is detected at the **record** level rather than by the version number. The current entry and snapshot payload types were renumbered when their layouts changed, so a 0.4.x record is refused during replay by the payload-type ladder, naming the legacy format it found. See [Format Evolution and Fatal-on-Replay Types](#format-evolution-and-fatal-on-replay-types).

## LogDB Interface

The `logdb.LogDB` interface defines the storage contract. Implementations must be safe for concurrent use from multiple goroutines.

```go
type LogDB interface {
    // Name returns the name/type of the LogDB implementation.
    Name() string

    // SaveState persists a batch of Updates atomically per shard.
    // Implementations must not retain or alias the caller's Entry.Cmd
    // backing arrays. An Update whose first entry sits at or below the
    // node's compaction floor is refused whole with
    // ErrEntriesBelowCompactionFloor.
    SaveState(updates []Update) error

    // IterateEntries returns log entries in the range [low, high) for the given node,
    // up to maxSize bytes total. Results are appended to the entries slice.
    // Each entry is charged EntryOverheadBytes + len(Cmd); maxSize 0 means
    // unlimited. Returned Cmd payloads must not alias the store's own memory.
    IterateEntries(entries []Entry, size uint64, shardID uint64, replicaID uint64,
        low uint64, high uint64, maxSize uint64) ([]Entry, uint64, error)

    // ReadState reads the persisted hard state and membership for the given node.
    ReadState(shardID uint64, replicaID uint64) (State, Membership, error)

    // EntryRange returns the first index and the CONTIGUOUS span
    // lastIndex-firstIndex+1. A stored count that disagrees with that span
    // returns ErrLogGapDetected rather than a short length.
    EntryRange(shardID uint64, replicaID uint64) (firstIndex uint64, length uint64, err error)

    // RemoveEntriesTo removes log entries up to and including the given index.
    RemoveEntriesTo(shardID uint64, replicaID uint64, index uint64) error

    // Compact compacts the underlying storage for the given node.
    Compact(shardID uint64, replicaID uint64) error

    // SaveSnapshot persists snapshot metadata for the given node.
    SaveSnapshot(shardID uint64, replicaID uint64, snapshot Snapshot) error

    // GetSnapshot returns the most recent snapshot metadata for the given node.
    GetSnapshot(shardID uint64, replicaID uint64) (Snapshot, error)

    // RemoveNodeData removes all data for the given node.
    RemoveNodeData(shardID uint64, replicaID uint64) error

    // ListNodeInfo returns all NodeInfo entries in the LogDB.
    ListNodeInfo() ([]NodeInfo, error)

    // SaveBootstrap persists bootstrap information for a node.
    SaveBootstrap(shardID uint64, replicaID uint64, bootstrap Bootstrap) error

    // GetBootstrap returns the bootstrap information for a node.
    GetBootstrap(shardID uint64, replicaID uint64) (Bootstrap, bool, error)

    // Close closes the LogDB and releases resources.
    Close() error
}
```

### LogDB Types

The `logdb` package defines storage-layer types separate from proto types to maintain a clean abstraction boundary.

These lists are **complete**. `logdb.LogDB` is a supported extension point,
and an alternative backend that persists only a subset of these fields
compiles cleanly and fails silently at runtime — see the notes below each
type for what breaks.

**Entry** - represents a single Raft log entry:
- `Index` (uint64) - log position
- `Term` (uint64) - Raft term when created
- `Type` (uint64) - entry type (normal, config change, etc.)
- `Key` (uint64) - caller-assigned completion key for this proposal
- `ClientID` (uint64) - client session that proposed this entry; 0 means not session-managed
- `SeriesID` (uint64) - per-session sequence number used for deduplication
- `RespondedTo` (uint64) - highest responded-to series for this client
- `OwnerID` (uint64) - principal the embedder declared for the session this entry registers; 0 means none
- `Cmd` ([]byte) - serialized command payload

`Key`, `ClientID`, `SeriesID`, `RespondedTo` and `OwnerID` **must** be
persisted. A backend that drops the first four silently disables session
deduplication, breaking at-most-once delivery across a restart. `OwnerID` fails
differently and more quietly: it feeds the deterministic per-owner session cap
(`proto.SessionPolicy.MaxSessionsPerOwner`), and 0 is both "no owner" and what a
dropped field decodes to, so losing it lifts the cap on every replayed entry and
the replica admits registrations its peers refuse.

`logdb.EntryOverheadBytes` (84) is the per-entry byte overhead every backend
charges against the `IterateEntries` `maxSize` budget on top of `len(Cmd)`. It
equals `waldb.entryRecordHeaderSize` and must track it exactly, so the same
`maxSize` admits the same entries whatever the backing store is.

**State** - persisted Raft hard state:
- `Term` (uint64) - current Raft term
- `Vote` (uint64) - candidate voted for in current term
- `Commit` (uint64) - highest log index known to be committed

**Membership** - Raft cluster membership configuration:
- `ConfigChangeID` (uint64) - log index of the configuration change
- `MaxWitnesses` (uint64) - replicated witness cap (0 = no explicit cap)
- `RequireFullReplicaMajority` (bool) - replicated full-replica-majority witness policy
- `SessionExpiryEntries` (uint64) - replicated session expiry threshold, in applied entries
- `MaxTotalSessions` (uint64) - replicated cap on registered sessions
- `MaxSessionsPerClient` (uint64) - replicated cap on responses cached per session (a response-cache depth, not a session count)
- `MaxSessionsPerOwner` (uint64) - replicated cap on concurrent sessions per declared owner; 0 disables it
- `Addresses` (map[uint64]string) - full voting member addresses
- `Observers` (map[uint64]string) - non-voting observer addresses
- `Witnesses` (map[uint64]string) - witness-only member addresses
- `Removed` (map[uint64]bool) - removed replica IDs
- `Identities` (map[uint64]string) - committed cryptographic node identity per replica; SPARSE
- `RemovedIdentities` (map[uint64]string) - identity a removed replica held when its removal committed; the deny-list evaluated against the authenticated connection identity. Key set is a subset of `Removed`'s

`MaxTotalSessions` is non-zero in every committed policy, which is what makes
it the discriminator for "was a session policy recorded at all". That is what
lets `SessionExpiryEntries == 0` keep its own meaning (expiry explicitly
disabled cluster-wide) rather than being read as an absent policy.

The witness and session policy fields are **replicated state**, not local
configuration: the deterministic apply path reads them, so a replica that
evaluates a different value diverges its state machine with every Raft
invariant intact.

**Snapshot** - persisted snapshot metadata:
- `Index` (uint64) - last included log index
- `Term` (uint64) - term of last included log entry
- `Membership` (Membership) - cluster membership at snapshot index
- `Filepath` (string) - path to snapshot data file
- `FileSize` (uint64) - size of snapshot data file in bytes
- `OnDiskIndex` (uint64) - index tracked for on-disk state machines
- `Epoch` (uint64) - barrier encryption epoch (0 for plaintext)
- `Compression` (config.CompressionType) - algorithm the data file was written through

`Compression` describes the **artifact**, not the writer's configuration.
Every recovery path must build its reader chain from this field and not from
`Config.SnapshotCompressionType`, or an artifact written before a config
change is decoded through the wrong reader. The zero value is
`config.NoCompression`.

**Bootstrap** - initial cluster configuration:
- `Addresses` (map[uint64]string) - initial replica addresses
- `Join` (bool) - whether joining existing cluster
- `Type` (uint64) - state machine type
- `DataDir` (string) - the shard's `Config.DataDir`; empty when the shard roots under the host's `NodeHostDir`
- `DataDirRecorded` (bool) - whether the persisted record actually carried `DataDir`, as opposed to predating the field

`DataDirRecorded` is a property of the **record**, not of the request: a
value passed to `SaveBootstrap` is ignored. It exists so a reader can tell
"this shard was recorded as rooting under NodeHostDir" from "this record was
written before DataDir existed and says nothing about it".

**Update** - batch of state changes for a single shard:
- `ShardID` (uint64) - Raft shard ID
- `ReplicaID` (uint64) - replica ID within shard
- `State` (State) - hard state to persist
- `Entries` ([]Entry) - log entries to append
- `Snapshot` (Snapshot) - snapshot metadata to persist

### EpochAwareLogDB (Optional)

The optional `EpochAwareLogDB` interface exposes encryption epoch information:

```go
type EpochAwareLogDB interface {
    // MinLiveEpoch returns the minimum barrier encryption epoch across
    // all live (non-compacted, non-garbage-collected) WAL records.
    // Returns 0 when no encrypted records exist or no barrier is configured.
    MinLiveEpoch() (uint64, error)
}
```

This is used by the snapshot pool to prevent premature epoch DEK purging. Implementations that do not support encryption need not implement this interface. Callers should use a type assertion to check for support.

### The other optional interfaces

`waldb.DB` implements four more optional interfaces, all discovered by type
assertion:

`PerBarrierEpochAwareLogDB.MinLiveEpochForBarrier(barrier any) (epoch uint64, known bool, err error)`
restricts the epoch floor to the records sealed under one barrier handle, so a
per-tenant deployment computes an independent DEK-purge floor per barrier. The
three-valued return is a fail-safe contract: `known == false` means the store
cannot attribute the barrier and the caller **must not** purge, which is a
different answer from `known == true, epoch == 0` ("this store owns the barrier
and imposes no floor"). Any error is treated exactly like `known == false`.

`BarrierShardEnumerator.ShardsForBarrier(barrier any) ([]uint64, bool)` reports
the complete shard set mapped to a barrier when that set is fixed and
enumerable, so the snapshot pool can verify its retained-snapshot scan covered
every shard before purging a shared barrier's DEKs.

`AtRestProtectedLogDB.AtRestProtection() logdb.AtRestProtection` declares
whether records reach durable storage in cleartext. The three values are
`AtRestUnprotected` (the zero value, and what a missing implementation means),
`AtRestNonDurable`, and `AtRestBarrierEncrypted`. It exists because
`waldb.DB` satisfies the per-barrier interfaces whether or not it was opened
`WithBarrier`, so a type assertion alone would accept a store writing every
command to disk in the clear. A barrier running `StrategyNone` reports
`AtRestUnprotected`.

`RecoveryResidualReporter.RecoveryResiduals() []logdb.RecoveryResidual` reports
the one successful-recovery outcome a node cannot simply carry on from: it came
back holding a log that may be strictly shorter than the one it acknowledged to
its peers. See [Generation-boundary residuals](#generation-boundary-residuals).

### LogDB Metrics Interface

The `waldb.LogDBMetrics` interface enables latency and throughput monitoring:

```go
type LogDBMetrics interface {
    ObserveWriteLatency(d time.Duration)      // per-writeRecord duration
    ObserveFsyncLatency(d time.Duration)      // per-sync duration
    ObserveCompactionLatency(d time.Duration) // per-Compact duration
    IncWriteBytes(n int64)                    // cumulative bytes written
}
```

Pass `nil` to disable metrics collection. The concrete implementation lives in the public quicraft package to avoid import cycles.

## 16-Shard Architecture

The `DB` struct holds a fixed array of 16 `walShard` pointers. Sharding reduces fsync contention by distributing updates across independent locks and segment files:

```go
const DefaultShardCount = 16

type DB struct {
    dir    string
    opts   dbOptions
    shards [DefaultShardCount]*walShard
    closed atomic.Bool
}
```

Shard assignment uses modulo hash on the Raft shardID:

```go
func (db *DB) getShard(shardID uint64) *walShard {
    return db.shards[shardID%DefaultShardCount]
}
```

Each shard is an independent unit with its own segment files and in-memory state maps, guarded by **two** locks:

- `mu` protects the segment FILE state — the active segment, segment IDs, rotation, record writes, `fsync`, and rollback. `SaveState` holds it across write → fsync → commit, which is load-bearing: releasing it between the durable write and a rollback would let a concurrent same-shard batch commit past this batch's checkpoint, after which the rollback would truncate the segment and erase acknowledged records.
- `dataMu` (an `RWMutex`) protects only the in-memory maps that reads consult. Readers take `dataMu.RLock()` and **no** `mu` at all, so they are never blocked by a writer's `fsync`; `SaveState` takes `dataMu` only for its brief phase-3 commit, which keeps a batch atomic as far as readers are concerned.

Lock order is always `mu` → `dataMu`, never the reverse, and `dataMu` is never held across I/O.

```go
type walShard struct {
    mu     sync.Mutex   // segment file state
    dataMu sync.RWMutex // in-memory maps read by IterateEntries/ReadState/...

    fs       FS
    dir      string
    opts     dbOptions
    shardIdx uint64       // 0..15; bound into every encrypted record's AAD
    barrier  WALBarrier
    sealer   walContextBarrier // barrier's context-AAD capability, resolved at open

    activeSegment   *segment
    activeSegmentID uint64
    nextSegmentID   uint64
    knownSegments   map[uint64]bool // all segment IDs ever created
    marshalBuf      []byte          // reusable encode buffer
    batchMode       bool            // suppresses per-record fdatasync
    closed          bool

    // Guarded by dataMu.
    indexes             map[nodeKey]*index                 // per-node entry indexes
    entries             map[nodeKey]map[uint64]logdb.Entry // per-node entry data
    states              map[nodeKey]logdb.State            // per-node hard state
    bootstraps          map[nodeKey]logdb.Bootstrap        // per-node bootstrap info
    snapshots           map[nodeKey]logdb.Snapshot         // per-node snapshot metadata
    compactedTo         map[nodeKey]uint64                 // per-node compaction floors
    metaSegments        map[nodeKey]metaSegments           // GC pins for non-indexed records
    committedSegmentHWM uint64                             // highest fully committed segment ID
    minWriteEpoch       uint64                             // epoch floor across live records
    segmentEpochs       map[uint64]uint64                  // per-segment minimum epoch
    residuals           []logdb.RecoveryResidual           // set once by recover()
}

type nodeKey struct {
    ShardID   uint64
    ReplicaID uint64
}
```

Why 16 shards: The count balances write parallelism against resource overhead. With 10,000+ Raft groups, 16 shards ensure that no single directory's fsync serializes more than ~625 groups on average, while keeping the number of open file descriptors and directory entries manageable. Multiple Raft groups that hash to the same shard share a single segment file and fsync call, which amortizes the cost of durability.

On-disk layout:

```
<dir>/
  WALFORMAT          # 16-byte geometry + format-version marker
  shard-0/
    000000.log
    000001.log
  shard-1/
    000000.log
  ...
  shard-15/
    000000.log
```

`WALFORMAT` sits in the root beside the shard directories, not inside a segment,
because a per-segment preamble would shift every record off the
multiple-of-`BlockSize` alignment that recovery strides over. `WALFORMAT.tmp` may
appear transiently while the marker is being published; neither name parses as a
`%06d.log` segment, so `recover()` ignores both.

## Segment Format

Each shard writes to a single active segment file. Segments are named with a zero-padded 6-digit decimal ID (`%06d.log`). When a segment reaches its maximum file size, it is closed and a new segment is created (rotation).

### Segment Structure

```go
type segment struct {
    f           File
    id          uint64
    dir         string
    blockSize   int
    blockBuf    []byte       // pre-allocated block buffer
    blockOffset int          // write position within current block
    fileOffset  int64        // block-aligned start of the current block
    flushed     int          // bytes of the open block already written to the file
    maxFileSize int64
    noSync      bool
    recordSeq     atomic.Uint64 // logical records committed; rollback-aware
    durabilitySeq uint32        // Syncs COMPLETED; stamped into every chunk
    closed        bool

    // Recovery verdict, set by ReadAll.
    stoppedAtCorruption        bool
    ackedDataAfterCorruption   bool
    sequenceRegression         bool
    generationBoundaryInDamage bool
    prefixGeneration           uint32
    survivingGeneration        uint32
}
```

Production defaults from `waldb/options.go`:

| Parameter      | Default            | Option            | Description                                              |
|----------------|--------------------|-------------------|----------------------------------------------------------|
| `BlockSize`    | 32768 (32 KiB)     | `WithBlockSize`   | WAL block size; power of two, at least `recordHeaderSize+1` |
| `MaxFileSize`  | 67108864 (64 MiB)  | `WithMaxFileSize` | Segment rotation threshold                               |
| `MaxBatchSize` | 67108864 (64 MiB)  | none              | Maximum single write batch, validated but not plumbed into `dbOptions` |
| `NoSync`       | false              | `WithNoSync`      | Disables `fdatasync`. Tests only                         |

`Options.Validate` rejects an empty `WALDir`, a `BlockSize` that is not a power
of two or is smaller than `recordHeaderSize+1`, and a non-positive `MaxFileSize`
or `MaxBatchSize`. The remaining `Open` options (`WithBarrier`, `WithFS`,
`WithMetrics`, `WithDiskMonitorOpts`) carry no default beyond nil.

**`BlockSize` is immutable for the life of a data directory. `MaxFileSize` is freely tunable at any time.**

The asymmetry is structural, not a policy choice. `BlockSize` defines a grid that recovery must reconstruct from configuration alone, because segment files carry no header of their own — change it and the scanner misparses every boundary written under the old size. `MaxFileSize` only decides when the writer rotates to a new segment; recovery never consults it, so raising or lowering it affects future rotation and nothing already on disk. Changing `MaxFileSize` on a populated directory is safe and needs no migration.

To change `BlockSize`, start from an empty data directory (wipe and re-sync the node from its peers). There is no in-place migration.

`BlockSize` is recorded on disk in a `WALFORMAT` marker file in the WAL root directory. The 16 bytes are `magic "QRWALFMT" (8) || version (4, little-endian) || blockSize (4, little-endian)`. `Open` compares the configured block size against the marker and refuses a mismatch with `ErrWALGeometryMismatch`, which names both sizes so the fix is a configuration edit rather than a restore. `MaxFileSize` is deliberately **not** recorded: recording it would imply a guarantee about a value that carries none.

`walFormatVersion` is the version of the whole on-disk format, block framing and record framing together, and it is pinned at **1** for the pre-release period like every other format in this repository. It is compared for exact equality before a single segment byte is read, and a marker naming any other version fails the open with `ErrWALFormatUnsupportedVersion`. That check is what stops a build with different record framing from misparsing from the first chunk: every chunk would fail CRC, the clean prefix would be empty, and the torn-tail repair would then truncate the whole file to zero and delete every segment above it. A silent misparse presented as a repair is the one outcome the marker exists to make impossible.

The pin has a consequence worth stating: this marker does **not** separate 0.4.x data from 0.5.x data, because both builds write 1. A record-layout change is caught by the payload-type ladder instead, one layer down. Do not read a clean marker check as evidence that a data directory matches the binary.

A directory with no marker gets exactly two outcomes, decided before this `Open` writes anything into it:

- **No segments anywhere.** This process defines the format, so the marker is stamped now, before the first segment exists. A failure to stamp is fatal and leaves the directory empty: proceeding would create segments this build could never prove the format of.
- **Segments present.** Those bytes were written by a build predating the marker, and therefore predating the durability sequence in the chunk header, so this build cannot parse them. `Open` refuses with `ErrWALFormatMissing`. This is a change of posture from when the marker recorded only the block size and an unmarked populated directory was adopted once recovery had proved its geometry. Guessing the record framing wrong does not produce a detectable mismatch; it produces an empty clean prefix.

Stamping before the shards are opened is the point of the ordering. Stamping afterwards left a window, a crash or a failed stamp, in which segments existed with no marker above them, which the rule above then reads as a pre-marker directory and refuses forever.

The marker is published atomically: the 16 bytes are written to `WALFORMAT.tmp`, fsynced, renamed onto `WALFORMAT`, and the directory entry is fsynced last. A reader therefore sees either no marker or the whole verified value, never a partial one. Every error path unlinks the temp file. A marker that is entirely zero bytes (including zero-length) is treated as **absent**: that is the residue of an interrupted or failed stamp, and the valid magic is non-zero so no real marker can look like it. A marker with non-zero bytes that are not our format, or whose recorded block size is not a valid power of two, stays fatal (`ErrWALFormatMalformed`). Under `noSync` the syncs are skipped but the rename is not, because publication is a correctness property rather than a durability optimization.

Segment files are pre-allocated at creation using the `FilePreAllocator` interface. Production code uses `fallocate` on Linux or `ftruncate` on other platforms. Pre-allocation avoids filesystem metadata updates on each append. On close, the file is truncated to its actual written size to reclaim unused pre-allocated space.

### Block Structure

A segment file is logically divided into fixed-size blocks of `BlockSize` bytes. Records are written into blocks sequentially. When insufficient space remains in the current block for a chunk header plus at least 1 byte of payload (i.e., `available < recordHeaderSize + 1`), the remaining bytes are zero-padded and the block is flushed to disk.

```
Block (32768 bytes default):
+-------------------------------------------------------------------+
| Chunk 1 | Chunk 2 | ... | Chunk N | Zero padding (< 12 bytes)      |
+-------------------------------------------------------------------+
```

Padding is therefore never more than `recordHeaderSize` bytes, which is what
lets recovery treat a longer zero run inside a non-final block as a write hole
rather than as legitimate padding.

### Record Framing

Records are encoded as chunks within blocks. The chunk format follows the LevelDB/Pebble log format with CRC32 integrity:

```
Chunk Header (11 bytes):
+----------+---------+------+------------+-----------------+
| CRC32: 4 | Len:  2 | Type | DurSeq:  4 | Payload: N      |
+----------+---------+------+------------+-----------------+
```

From `waldb/record.go`:

```go
const recordHeaderSize = 11 // [CRC32:4][Length:2][Type:1][DurSeq:4]
const maxRecordDataLen = (1 << 16) - 1 // 65535 bytes
```

The CRC32 uses the Castagnoli polynomial (`crc32.Castagnoli`), which is hardware-accelerated via SSE4.2 on amd64 processors. The checksum covers the type byte, the durability sequence and the payload data (`buf[6:totalLen]`), not the CRC or length fields themselves. `Length` is the payload length only and excludes the header. A chunk's payload is capped at `maxRecordDataLen` even when the block has more room, because the length field is two bytes; `segment.Write` and `bytesNeeded` apply the identical cap so a block size of 64 KiB or more still frames correctly.

CRC32-C detects accidental corruption (bit flips, torn pages, partial writes) and nothing else. It is not a cryptographic integrity check, and an attacker with disk access recomputes it trivially. Tamper protection at rest comes from the barrier, which seals each record payload with AEAD; the CRC stays as a fast first-pass filter so garbage is not fed into the decryption path.

#### The durability sequence

`DurSeq` is the number of `Sync` calls the segment had COMPLETED when the chunk was encoded. It partitions a segment into durability generations and is what lets recovery tell an interrupted write from damage to acknowledged data:

- A record carrying generation `g` was written before `Sync #(g+1)` was called.
- The writer only writes a generation `g+1` record AFTER `Sync #(g+1)` returned.
- So the presence on disk of any record with a generation above `g` **proves** that `Sync #(g+1)` completed, which proves every generation `≤ g` record was durable and acknowledged.

Records carrying the segment's *highest* generation have no such proof behind them: they are the one batch that was still in flight. Discarding them destroys nothing that was acknowledged. See [Classifying corruption](#classifying-corruption-interrupted-write-vs-damage-to-acknowledged-data).

It is stamped per **chunk**, not per block. A block is filled by many independent batches — a 32 KiB block holds hundreds of 48-byte HardState records, each with its own `fdatasync` — so a per-block generation would be written once at the top of the block and be wrong for everything after it. It could not resolve a boundary lying inside a block, which is where both the damage and the batch boundary usually are. Per chunk also survives losing the batch's leading page, because every record carries its own generation rather than depending on one record surviving.

Cost, measured A/B on an idle 20-core bench node (Go 1.26.5, NVMe), three interleaved repetitions per arm:

| Measurement | With `DurSeq` | Without | Delta |
|---|---|---|---|
| `encodeRecord`, 48-byte payload | 36.5 ns/op | 35.5 ns/op | +1.1 ns (+3.0%) |
| `encodeRecord`, 1408-byte payload | 114.0 ns/op | 114.0 ns/op | +0.1% |
| `segment.Write`, 48-byte record, no I/O | 51.8 ns/op | 51.7 ns/op | +0.2% |
| `SaveNodeState` end to end (one fdatasync) | 2.525 ms/op | 2.529 ms/op | within noise |
| `SaveState`, 10×128-byte entries (one fdatasync) | 2.668 ms/op | 2.649 ms/op | within noise |
| Bytes written per `SaveNodeState` | 52 | 48 | +4 B (+8.3%) |
| Bytes written per `SaveState` batch | 2105 | 2096 | +9 B (+0.4%) |

Allocations are unchanged (`0 B/op` difference on every benchmark). Write **amplification** is unchanged at 1.0x — the flush watermark still hands each byte to `WriteAt` exactly once, so the 278x→1.0x improvement is intact. The four bytes do reduce how many minimum-size records fit in a segment by ~8%, which rotates segments marginally more often.

The `DurSeq` field also costs a segment ~8% of its minimum-size record capacity and is capped at `maxDurabilitySeq`; `CanFit` returns `ErrSegmentFull` at the cap so the counter can never wrap and present an old generation as the newest one.

### Chunk Types

```go
const (
    RecordFull   byte = 1  // complete record in one chunk
    RecordFirst  byte = 2  // first chunk of a multi-chunk record
    RecordMiddle byte = 3  // middle chunk of a multi-chunk record
    RecordLast   byte = 4  // last chunk of a multi-chunk record
)
```

A logical record that fits within the remaining block space is written as a single `RecordFull` chunk. A record too large for the remaining space is split across blocks: the first piece is `RecordFirst`, intermediate pieces are `RecordMiddle`, and the final piece is `RecordLast`.

The accepted range is exactly `RecordFull..RecordLast`, and `isValidRecordType` rejects anything else as corruption before the CRC is even checked. Keeping the set tight is deliberate: a never-written type in the range would make a random or forged header byte likelier to pass that pre-CRC screen.

Every chunk of one logical record carries the same durability generation. `Write` runs under the shard lock and never syncs, so no `Sync` can land between a record's chunks and a multi-block record is wholly inside the in-flight generation or wholly below it. That is what makes a torn multi-block write a clean suffix in generation terms instead of a straddle recovery would have to guess at.

### Payload Types

Each logical WAL record begins with a 1-byte payload type that identifies its content:

```go
const (
    payloadTypeState      byte = 0x02
    payloadTypeBootstrap  byte = 0x04
    payloadTypeCompact    byte = 0x05
    payloadTypeRemoveNode byte = 0x06
    // 0x07 and 0x0a are RETIRED and never reallocated; a record carrying
    // either fails replay with ErrUnknownPayloadType
    payloadTypeEntries    byte = 0x0b // current entry format (84-byte header, five session fields)
    payloadTypeSnapshot   byte = 0x0c // current snapshot format (four-field session policy tail)
)
```

#### Format Evolution and Fatal-on-Replay Types

The payload type byte is the WAL's only on-disk versioning mechanism, and with `walFormatVersion` pinned it is the only thing standing between a 0.4.x directory and this build's parser. A record layout is never changed in place: a new layout gets a NEW payload type, and the retired value stays reserved forever so an old WAL is *detected* rather than misparsed. Retired types are never written again, and encountering one during replay is fatal — recovery fails the open instead of skipping or reinterpreting the record.

0.5.0 broke that rule and it has been repaired. The entry record header grew 76 → 84 bytes (`OwnerID`) and the snapshot record's session policy tail 24 → 32 (`MaxSessionsPerOwner`) while both kept their existing bytes, so a 0.4.x record reached a parser that had no way to know what it was holding. Measured, a 0.4.x entry record came back with `err == nil`, an `OwnerID` of `0x4443424100000018` (the record's own `CmdLen` followed by four command bytes) and a command truncated from 24 bytes to 16. A 0.4.x snapshot record usually failed as truncated, which is a corruption diagnosis for a version mismatch, and in one realistic shape was accepted with a fabricated `MaxSessionsPerOwner` and a `RemovedIdentities` deny-list holding a replica nobody removed. That list is evaluated against the authenticated connection identity, so misreading it revokes a revocation.

The repair retires `0x07` and `0x0a` and moves the current formats to `0x0b` and `0x0c`. Note what that costs in the other direction: a WAL written by this build is not readable by 0.5.1-alpha either, which sees `0x0b`/`0x0c` as `ErrUnknownPayloadType`. Both directions are wipe-and-resync, which is the pre-release posture anyway.

| Value  | Constant                | Status  | Replay behaviour                                                    |
|--------|-------------------------|---------|---------------------------------------------------------------------|
| `0x01` | *(unallocated)* | retired | fatal, `ErrUnknownPayloadType` — it named an entry format with no session fields, so replaying it would zero them and silently break at-most-once delivery |
| `0x02` | `payloadTypeState`      | current | hard state (Term/Vote/Commit)                                       |
| `0x03` | *(unallocated)* | retired | fatal, `ErrUnknownPayloadType` — it named a snapshot format with no `MaxWitnesses`/`PolicyFlags` in the membership section |
| `0x04` | `payloadTypeBootstrap`  | current | bootstrap record                                                    |
| `0x05` | `payloadTypeCompact`    | current | compaction floor marker                                             |
| `0x06` | `payloadTypeRemoveNode` | current | remove-node tombstone                                               |
| `0x07` | *(unallocated)* | retired | fatal, `ErrUnknownPayloadType` — it named an entry header eight bytes shorter, so `OwnerID` would be read from the record's own `CmdLen` and the first four command bytes, and `CmdLen` then taken from inside the command |
| `0x08` | *(unallocated)* | retired | fatal, `ErrUnknownPayloadType` — its membership section started with `ConfigChangeID`, so the current parser would consume that field's low byte as a version byte and shift every subsequent read |
| `0x09` | *(unallocated)* | retired | fatal, `ErrUnknownPayloadType` — its `FilePathLen` sat where the current layout reads `Compression`, so the record would be sliced at the wrong offset |
| `0x0a` | *(unallocated)* | retired | fatal, `ErrUnknownPayloadType` — it named a three-field session policy tail, so `MaxSessionsPerOwner` would be read from the `RemovedIdentities` count and the deny-list then located eight bytes late |
| `0x0b` | `payloadTypeEntries`    | current | entry batch, 84-byte per-entry header                               |
| `0x0c` | `payloadTypeSnapshot`   | current | snapshot metadata, four-field session policy tail                   |

Any type byte outside this set is `ErrUnknownPayloadType`, which is also fatal. That case matters most for a KEYED WAL opened with `barrier == nil`: every ciphertext record's leading epoch byte decodes as type `0x00`, and skipping those records would boot the node as an empty replica with no memory of prior votes.

### Record Encoding Layouts

**Entry batch** (`payloadTypeEntries`, repeating):

```
[PayloadType:1][ShardID:8][ReplicaID:8][Index:8][Term:8][Type:8]
               [Key:8][ClientID:8][SeriesID:8][RespondedTo:8][OwnerID:8][CmdLen:4][Cmd:N]...
                |<--------------------- 84 bytes per entry header --------------------->|
```

`entryRecordHeaderSize = 84` bytes per entry (8×10 + 4), all little-endian. It
grew from 76 in 0.5.0-alpha when `OwnerID` was added, which is one of the
changes that breaks 0.4.x data directories. The five session fields (`Key`,
`ClientID`, `SeriesID`, `RespondedTo`, `OwnerID`) are persisted so at-most-once
delivery and the per-owner session cap survive crash recovery. Both earlier
layouts are gated rather than misparsed: the 44-byte format
(`0x01`, retired and unallocated) fails recovery with `ErrUnknownPayloadType`, and
the 76-byte one (`0x07`, retired and unallocated) with `ErrUnknownPayloadType`.

`Cmd` is capped at `maxCmdLen` (8 MiB, matching `proto.MaxEntrySize`) on both
sides. `writeEntries` checks every entry before touching a segment, because
without the write-side check the encoder would store, and for a payload past
4 GiB uint32-truncate, a `Cmd` that recovery then rejects as corrupt: an entry
that is durable but unreadable. An oversized `Cmd` returns `ErrCmdTooLarge`.

Note that this is not the same layout as `proto.Entry`, whose header is 68
bytes; the two are converted at the storage boundary. See
[Wire Format](../protocol/wire-format.md) for the proto side.

**Hard state** (`payloadTypeState`):

```
[PayloadType:1][ShardID:8][ReplicaID:8][Term:8][Vote:8][Commit:8]
                |<------------ 40 bytes ------------>|
```

**Snapshot metadata** (`payloadTypeSnapshot`):

```
[ShardID:8][ReplicaID:8][Index:8][Term:8][FileSize:8][OnDiskIndex:8][Epoch:8][Compression:2][FilePathLen:4][FilePath:N][Membership...]
 |<------------------------ 62 bytes base (snapshotRecordBaseSize) ---------------------->|
```

The base size is `snapshotRecordBaseSize = 62` bytes
(8+8+8+8+8+8+8+2+4), measured from the start of the record payload -- the
`payloadType` byte is part of the record framing, not the base. `Epoch`
stores the encryption key rotation epoch associated with the snapshot;
`Compression` is a 2-byte `config.CompressionType`. A record declaring an
algorithm this binary cannot decompress fails replay with
`ErrUnsupportedSnapshotCompression` rather than handing the state machine bytes
in an unknown encoding.

Membership is serialized after the filepath as a variable-length structure.
It carries its own version byte, `membershipSectionVersion`, pinned at **1**
and gated on **exact equality**: every wire and persisted format in this
repository stays at version 1 until the first production release, and a
layout change is made in place with dev clusters wiped and re-synced rather
than migrated (see
[Compatibility](../operations/compatibility.md#format-versions-are-pinned-at-1-until-the-first-production-release)).
A section stamped with anything else was written by a binary built from a
different pre-release commit and is refused with
`ErrUnsupportedMembershipVersion` rather than read at these offsets. The
section is:

```
[MembershipVersion:1][ConfigChangeID:8][MaxWitnesses:8][PolicyFlags:1]
[NumAddresses:4]{[ReplicaID:8][AddrLen:4][Addr:N]...}
[NumObservers:4]{...}
[NumWitnesses:4]{...}
[NumRemoved:4]{[ReplicaID:8]...}
[NumIdentities:4]{[ReplicaID:8][IdLen:4][Id:N]...}
[SessionExpiryEntries:8][MaxTotalSessions:8][MaxSessionsPerClient:8]
[MaxSessionsPerOwner:8]
[NumRemovedIdentities:4]{[ReplicaID:8][IdLen:4][Id:N]...}
```

Fixed region: `membershipFixedSize = 18` bytes (1+8+8+1). The session-policy
tail is `membershipSessionPolicySize = 32` bytes (4×8). The smallest
section this binary will **replay** is `minMembershipSize = 74` bytes -- the
fixed region, six empty map count headers and the session policy. That floor
is applied *before* the version byte is read, so a section too short to hold
the layout is reported as a torn record rather than being read past its own
end. It equals `membershipSize` of an empty membership, and must: there is
one layout, every field is written unconditionally, so the read floor and the
written size are the same number by construction.

Note that this section is **not** the same layout as `proto.Membership`,
and deliberately so. Here `RequireFullReplicaMajority` is packed into a
one-byte `PolicyFlags` where proto spends eight, and the session policy is
a **tail** where proto keeps it inside the fixed region. That placement is
what keeps the section growable from the end: this decoder parses
straight-line from a fixed offset, so growing the fixed region moves every
map, while appending to the tail does not.

Persisting the full membership means cluster membership is recoverable from
the latest snapshot even if the log is completely replayed.
`ReadState(shardID, replicaID)` returns the hard state and the `Membership`
from the latest stored snapshot. If no snapshot has been saved, the returned
Membership is an empty `logdb.Membership{}`.

A snapshot that drops `Identities` silently downgrades every member it
describes back to the address check. A snapshot that drops
`RemovedIdentities` does not merely lose information -- it revokes a
revocation, and the replica comes back having forgotten which keypairs it
ejected, readmitting a decommissioned node holding a still-valid certificate
on exactly the restart an operator performs after removing it.

**Bootstrap info** (`payloadTypeBootstrap`):

```
[PayloadType:1][ShardID:8][ReplicaID:8][Join:1][Type:8][NumAddrs:4]
                |<------------ 29 bytes base ----------->|
For each address: [ReplicaID:8][AddrLen:4][Addr:N]
Then:             [DataDirLen:4][DataDir:N]
```

`bootstrapRecordBaseSize = 29`. `DataDir` is a **trailing** extension: a record written before the field existed simply ends at the address map, and `replayBootstrap` reads that as `DataDirRecorded = false`. A record this version writes always carries the length prefix, so a zero-length `DataDir` is still explicit on the wire and reads back as `DataDirRecorded = true` with an empty value. That distinction is the whole point of the flag, because `DataDir == ""` is itself a meaningful value ("this shard roots its tree under the host's `NodeHostDir`"). `DataDirLen` is bounded at `maxBootstrapDataDirLen` = 4096 so a corrupt length cannot drive a huge allocation.

**Compaction marker** (`payloadTypeCompact`):

```
[PayloadType:1][ShardID:8][ReplicaID:8][CompactedTo:8]
                |<---------- 24 bytes ---------->|
```

## Write Path

The write path flows from `SaveState` through block buffering to durable storage.

### SaveState Entry Point

`SaveState` takes a batch of `logdb.Update` values, each targeting the shard at `shardID % 16`, and runs it in three phases. Sketching it with the error handling removed:

```go
func (db *DB) SaveState(updates []logdb.Update) error {
    // Refuse a closed or poisoned DB, then throttled disk-usage check.
    // Lock every touched shard, in ascending index order, for the whole batch.
    // Reject the batch outright if any Update carries entries at or below
    // its node's compaction floor (ErrEntriesBelowCompactionFloor).

    // Phase 1: write every record with batchMode=true, so writeRecord
    // buffers without fdatasync. Capture each shard's pre-batch segment
    // checkpoint first; any failure rolls the whole batch back.

    // Phase 2: fdatasync every dirty shard, in parallel above one shard.
    // A failure here poisons the DB.

    // Phase 3: commit the staged in-memory updates under each shard's
    // dataMu. This is the point reads can see the batch.

    // Unlock in descending index order.
}
```

This is what turns the fsync count from O(records) into O(shards touched), with `shards <= 16`.

Three properties of that structure are load-bearing:

- **Every touched shard's `mu` is held for the entire batch lifecycle**, checkpoint through commit or rollback. An earlier version released `mu` between the durable write and the rollback, and two Raft shards hashing to one WAL shard are served by different commit workers whenever `NumWorkers` does not divide 16. A concurrent same-shard batch could then write, fsync and commit a record past this batch's checkpoint, and a later partial-failure rollback would truncate the shared segment back and erase the other batch's acknowledged records.
- **Locks are taken in ascending shard index order**, so two batches with overlapping shard sets share a canonical order and cannot deadlock. The single-shard lockers (`Compact`, `SaveSnapshot`, `RemoveEntriesTo`, `RemoveNodeData`) hold at most one `mu` and cannot close a cycle.
- **The compaction-floor check runs before Phase 1**, under every touched shard's lock, so the rejection costs no WAL write and no rollback.

`SaveState` is atomic per shard, not across shards. If a write fails partway through the batch, shards written before the failure have records buffered but never fsynced, so a crash loses them. Raft tolerates that: it re-proposes uncommitted entries on leader election, each record is independently CRC-framed, and hard state is idempotent to replay.

An fsync failure in Phase 2 is different and does not return to normal service. It **poisons** the DB: `db.poison` sets a flag, every subsequent write is refused with `ErrDBPoisoned`, and the process must restart into WAL crash recovery. The reason is fsyncgate: a failed `fdatasync` leaves written-but-unsynced bytes whose durability the kernel no longer guarantees, since it may mark dirty pages clean without writing them. Rolling back is not sufficient, because the truncation's own durability cannot be established either. The batch's staged epochs are still committed on this path, deliberately: its records are on the medium and nothing truncates them, so they can replay on the next start and their DEKs must stay retained.

### Per-Update Processing

`saveUpdate` writes three kinds of WAL record per update, entries, hard state and snapshot metadata, each only when the corresponding field is non-zero:

The entry payload may span **several** records. A single record must fit inside
one segment, and its usable payload is strictly smaller than the segment size —
a record spends `recordHeaderSize` bytes in every block it spans — while the WAL
also adds an 84-byte header per entry that Raft's batch cap does not account for.
`writeEntries` therefore splits a large batch across as many records as it needs;
without that, a batch Raft legitimately produced (a lagging follower catching up
on many small entries is the realistic case) would fail permanently with
`ErrRecordTooLarge`. A lone entry too large for any record still returns that
permanent error.

Splitting is safe on replay, which already accepts several entry records per
update, and safe across a crash: the entry records still precede this update's
state record in log order and the WAL truncates at a prefix, so a torn tail can
only lose state while keeping entries — never the reverse, which would recover
with a commit index past the last entry. Each chunk is stamped with the segment
it individually landed in, because consecutive chunks can straddle a rotation.

```go
func (s *walShard) saveUpdate(u *logdb.Update, pending *pendingMemUpdates, cp *shardCheckpoint) error {
    key := nodeKey{ShardID: u.ShardID, ReplicaID: u.ReplicaID}

    if len(u.Entries) > 0 {
        if err := s.writeEntries(key, u.ShardID, u.ReplicaID, u.Entries, pending, cp); err != nil {
            return err
        }
    }
    if u.State.Term != 0 || u.State.Vote != 0 || u.State.Commit != 0 {
        if err := s.writeState(key, u.ShardID, u.ReplicaID, u.State, pending, cp); err != nil {
            return err
        }
    }
    if u.Snapshot.Index > 0 {
        // Skipped when the stored snapshot, or a pending one earlier in this
        // same batch, is already at or above u.Snapshot.Index.
        if err := s.writeSnapshot(key, u.ShardID, u.ReplicaID, u.Snapshot, pending, cp); err != nil {
            return err
        }
    }
    return nil
}
```

Snapshot presence is keyed on `Index > 0` alone, matching the in-memory backend. Writing a zero-index snapshot made `GetSnapshot` report a snapshot at index 0 and pinned its segment against garbage collection permanently.

Nothing in `saveUpdate` touches the in-memory maps. Everything it would change is staged into `pendingMemUpdates` and applied in Phase 3, once the whole batch is durable, so a mid-batch failure cannot produce a phantom read.

### Block Buffering and Sync

The `writeRecord` method is the lowest-level write path. It ensures the active segment exists (creating the shard directory and segment file on first write for lazy shards), reserves the landing segment, encrypts when a barrier is configured, appends to the block buffer, and returns a `recordLanding` naming the segment the bytes actually reached and the epoch its ciphertext carries:

```go
func (s *walShard) writeRecord(data []byte) (recordLanding, error) {
    if err := s.ensureActiveSegment(); err != nil { ... }

    // Resolve the LANDING before encrypting: the context-AAD binds segment ID
    // and record ordinal, so both must be final at seal time. The framed
    // length is bounded by len(data)+maxCiphertextExpansion, and CanFit is
    // monotonic in it, so a generous bound only rotates early.
    framedLen := len(data)
    if s.barrier != nil { framedLen += maxCiphertextExpansion }
    if err := s.reserveLanding(framedLen); err != nil { ... }

    // Seal exactly once. Re-sealing after a rotation would burn a second
    // nonce against the NIST invocation cap for nothing.
    writeData := data
    if s.barrier != nil { writeData, writeEpoch = seal(data, s.recordContext(...)) }

    _, err := s.activeSegment.Write(writeData)
    // ErrSegmentFull here is unreachable for a positionally bound record and
    // returns ErrLandingSegmentOverflow rather than writing a ciphertext at a
    // position its AAD does not name. An unbound record may be moved as is.

    landing := recordLanding{segID: s.activeSegmentID, epoch: writeEpoch}
    if !s.opts.noSync && !s.batchMode {
        // Inline fdatasync for the standalone callers. A failure poisons the DB.
    }
    return landing, nil
}
```

The `batchMode` guard is the whole point: inside a `SaveState` batch the fsync belongs to Phase 2, so `writeRecord` buffers only. The standalone writers (`RemoveEntriesTo`, `SaveSnapshot`, `SaveBootstrap`, `RemoveNodeData`) run with `batchMode` false and sync inline.

The landing is returned rather than applied, because the record is not durable yet. Each caller folds it into the shard's epoch floor at its own commit point, so a batch that rolls back contributes nothing. There is one deliberate exception: when the record was fully written and only the inline `fdatasync` failed, the epoch is applied inside `writeRecord`. Those bytes may well be on the medium, and an epoch floor above a record recovery still has to decrypt breaks the node outright (`WALDecryptError` at startup) rather than merely retaining a key longer than necessary.

`segment.Sync()` flushes the open block's not-yet-flushed bytes, calls the file's `Sync` (`fdatasync` on Linux, `fsync` elsewhere), and only then increments `durabilitySeq`:

```go
func (s *segment) Sync() error {
    if s.closed { return ErrClosed }
    if err := s.flushBlock(); err != nil { return err }
    if s.noSync { return nil }
    if err := s.f.Sync(); err != nil { return err }
    s.durabilitySeq++
    return nil
}
```

The counter advances after the sync returns, never before, because that return is the entire proof. Bumping it first would mark the next batch as separated by a completed sync from records a failed sync left in limbo.

`flushBlock` writes only the bytes added since the previous flush — `blockBuf[flushed:blockOffset]` at `fileOffset+flushed` — and advances the `flushed` watermark. It never rewrites the part of the open block an earlier `fdatasync` already made durable. That is a durability property first: re-writing an acknowledged record (a `HardState` carrying a vote grant, for example) inside a later batch's write puts it back at risk of a power loss that has no right to touch it. It also removes a quadratic write amplification, since filling one block over *n* syncs would otherwise write `~n²·recSize/2` bytes; with the default 32 KiB block and 48-byte records, persisting 110 KiB of log wrote 30.6 MB before the watermark and 110 KiB after.

Block-grid alignment is unaffected: the target offset stays inside the block that starts at `fileOffset`, and `fileOffset` is advanced only by `padAndFlushBlock`, by exactly one `blockSize`, after the block has been completed on disk. `DiscardPending` rewinds `flushed` to the checkpoint's block offset along with the offsets it restores.

On Linux, `fdatasync` is used instead of `fsync` because the file size does not change within pre-allocated space, saving ~10-30% on sync latency by skipping metadata updates.

Directory fsyncs are separate and there are three of them, all in
`ensureActiveSegment` and `createWALRoot`:

- The WAL root's whole created ancestor chain is fsynced at `Open`. POSIX durability needs the parent of *each* newly created directory made durable, and `MkdirAll` may create several levels; fsyncing only the immediate parent leaves the intermediate dirents un-durable, so a crash after an acknowledged `SaveState` can take the tree with it and the node boots with no term and no vote.
- When a lazy shard creates its directory on first write, the WAL root is fsynced so the shard directory's own entry is durable.
- After each new segment file is created, the shard directory is fsynced so the segment's dirent is durable. If that fsync fails the segment is closed and the write fails; a segment whose directory entry is not durable is not usable.

### Write Sequence Diagram

```
SaveState([]Update)
  |
  +-- lock every touched shard (ascending index)
  +-- reject entries below any node's compaction floor
  |
  +-- Phase 1, per Update (batchMode = true, no fsync):
  |     capture shard checkpoint (flushBlock first)
  |     writeEntries()   --> serialize to marshalBuf, split across records
  |     |                    --> writeRecord --> reserveLanding, seal, encodeRecord into blockBuf
  |     writeState()     --> writeRecord
  |     writeSnapshot()  --> writeRecord (skipped when stale)
  |     stage in-memory updates into pending[shardIdx]
  |
  +-- Phase 2: syncDirtyShardsLocked
  |     one goroutine per dirty shard --> segment.Sync() --> flushBlock() + fdatasync()
  |     any failure --> db.poison, return ErrDBPoisoned
  |
  +-- Phase 3: pending[i].applyTo(shard) under shard.dataMu  <-- commit point
  |
  +-- unlock (descending index)
```

Phase 3 is the only place in the batch where a lock-free reader can be blocked, and it holds no I/O.

### Marshal Buffer Reuse

Each shard maintains a reusable `marshalBuf` that is lazily allocated on first write and grown on demand. This avoids per-write heap allocation:

```go
func (s *walShard) getMarshalBuf(size int) []byte {
    if s.marshalBuf == nil {
        bufSize := 32 * 1024 // initial 32 KiB
        if size > bufSize {
            bufSize = size * 2
        }
        s.marshalBuf = make([]byte, bufSize)
    } else if size > len(s.marshalBuf) {
        s.marshalBuf = make([]byte, size*2)
    }
    return s.marshalBuf[:size]
}
```

### Anti-Aliasing

When storing entry commands in memory, the write path copies `Cmd` bytes to prevent aliasing with the caller's buffer:

```go
var cmdCopy []byte
if len(e.Cmd) > 0 {
    cmdCopy = make([]byte, len(e.Cmd))
    copy(cmdCopy, e.Cmd)
}
stagedEntries = append(stagedEntries, logdb.Entry{
    Index:       e.Index,
    Term:        e.Term,
    Type:        e.Type,
    Key:         e.Key,
    ClientID:    e.ClientID,
    SeriesID:    e.SeriesID,
    RespondedTo: e.RespondedTo,
    OwnerID:     e.OwnerID,
    Cmd:         cmdCopy,
})
```

Every field is carried, all five session fields included. Dropping them here is
not a lossy optimization: it silently disables session deduplication across a
restart.

The read side has the mirror obligation. `IterateEntries` returns each entry
through `logdb.CopyEntryCmd`, so the caller never receives a slice header
pointing at this shard's in-memory log. What a caller would otherwise be writing
through is the log this replica serves to its followers and replays to itself on
restart, and the corruption would be silent, unattributable, and different on
every replica. The copy is per entry rather than one shared array per batch, so
a caller retaining a single `Cmd` does not pin the whole batch's payloads.

## Group Commit Optimization

The commit worker in `pkg/internal/engine/commit.go` uses a **drain-then-batch** pattern to maximize WAL write throughput. This enables group commit: multiple updates arriving while the previous SaveState + fdatasync are in progress are automatically batched into a single SaveState call.

```go
func (w *commitWorker) run() {
    for {
        w.drainUnloadReload()
        select {
        case <-w.stopC:
            w.drainAndProcess()   // flush what is already queued before exiting
            return
        case m := <-w.membershipC:
            w.applyMembership(m)
            continue
        case item := <-w.commitC:
            w.batch = append(w.batch[:0], item.update)
            w.batchNodes = append(w.batchNodes[:0], item.node)
            w.captureOrderedMsgs(item)
            w.yieldFn()           // runtime.Gosched() to allow other goroutines to produce items
            w.drainCommitC()      // non-blocking drain of all pending items
            w.processBatch()      // single SaveState call for entire batch
        }
    }
}
```

The worker blocks on `commitC` until the first item arrives. After yielding via `runtime.Gosched()` to allow other step workers to produce items, it performs a non-blocking drain of all immediately available items. This means that updates arriving while the previous SaveState + fdatasync was in progress are automatically batched into the next call, achieving group commit semantics with zero allocations in steady state.

The yield approach prevents excessive context switching on idle systems (Gosched returns immediately when no other goroutines are runnable) while increasing average batch size under load.

## Read Path

All reads are served from in-memory maps. The WAL is never read during normal operation.

Every reader (`IterateEntries`, `ReadState`, `EntryRange`, `GetSnapshot`, `GetBootstrap`, `CompactedTo`, `ListNodeInfo`, `MinLiveEpoch`, `RecoveryResiduals`) takes `dataMu.RLock` and **no** `mu` at all. With 16 WAL shards serving 10k+ Raft groups, taking `mu` for a read would stall every group hashing to that shard for the duration of a batch's `fdatasync`.

### In-Memory Index

Each (shardID, replicaID) pair has its own `index` that tracks entry locations:

```go
type indexEntry struct {
    Index     uint64  // Raft log index
    Term      uint64  // Raft term
    SegmentID uint64  // which segment file contains this entry
    Offset    int64   // always 0; reads never seek, and replay never sets it
    Size      int64   // entryRecordHeaderSize + len(Cmd), the maxSize charge
}

type index struct {
    mu      sync.RWMutex
    entries []indexEntry  // sorted by Index, ascending
}
```

The index provides O(log n) lookup and range queries via `sort.Search`:

- `Lookup(raftIndex)` -- binary search for a single index
- `Range(low, high)` -- returns all entries in [low, high) as a copy
- `Compact(upTo)` -- removes entries with Index <= upTo
- `AllSegmentIDs()` -- returns the set of segment IDs referenced by any entry
- `Append(entries...)` -- handles both fast-path append and overwrite-truncate
- `Len()`, `First()`, `Last()`, `Bounds()` -- range accessors used by `EntryRange` and the gap checks

The `Append` method handles Raft leader changes where a new leader overwrites uncommitted entries. If the new entry's index is <= the tail, all entries from that position are truncated before appending.

### Entry Retrieval

`IterateEntries` looks up the index range, then fetches entry data from the in-memory `entries` map:

```go
func (db *DB) IterateEntries(
    entries []logdb.Entry, size uint64,
    shardID, replicaID, low, high, maxSize uint64,
) ([]logdb.Entry, uint64, error) {
    if db.closed.Load() { return entries, size, logdb.ErrClosed }
    if low >= high     { return entries, size, nil }

    shard := db.getShard(shardID)
    shard.dataMu.RLock()
    defer shard.dataMu.RUnlock()

    if low <= shard.compactedTo[key] { return entries, size, logdb.ErrCompacted }

    idx, ok := shard.indexes[key]
    if !ok { return entries, size, logdb.ErrUnavailable }

    rangeEntries := idx.Range(low, high)
    if len(rangeEntries) == 0        { return entries, size, logdb.ErrUnavailable }
    if rangeEntries[0].Index != low  { return entries, size, logdb.ErrLogGapDetected }

    totalSize, expected := size, low
    for _, ie := range rangeEntries {
        e, found := shard.entries[key][ie.Index]
        if !found || ie.Index != expected {
            return entries, size, logdb.ErrLogGapDetected
        }
        entryBytes := uint64(ie.Size)
        if maxSize > 0 && totalSize+entryBytes > maxSize && len(entries) > 0 {
            break
        }
        entries = append(entries, logdb.CopyEntryCmd(e))
        totalSize += entryBytes
        expected++
    }
    return entries, totalSize, nil
}
```

Three behaviours matter to callers:

- **`maxSize` stops iteration *before* the entry that would exceed the budget**, and `maxSize` of 0 means unlimited. At least one entry is returned whenever any is available, even if it alone exceeds `maxSize`; the guard is `len(entries) > 0`, which is on the outgoing slice, not on the incoming `size`, so an already-populated caller slice does not change the rule.
- **Compaction is checked first.** `low <= compactedTo` returns `logdb.ErrCompacted`, which is what makes raft fall back to `InstallSnapshot`.
- **A hole is never served as a short result.** A stored range that does not begin exactly at `low`, or that is missing an interior index, returns `logdb.ErrLogGapDetected` and logs at `ERROR`. Silently returning the shortened prefix would be a Log Matching violation handed to a follower with a nil error. An unknown node, or a range holding no entries at all, returns `logdb.ErrUnavailable`.

## Encryption Support

The WAL supports optional at-rest encryption via the `WALBarrier` interface:

```go
type WALBarrier interface {
    // Encrypt encrypts plaintext using the current epoch's DEK.
    Encrypt(dst, plaintext []byte) ([]byte, error)

    // Decrypt decrypts ciphertext using the epoch embedded in the prefix.
    Decrypt(dst, ciphertext []byte) ([]byte, error)

    // EncryptWithEpoch encrypts plaintext and returns ciphertext and epoch.
    EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)

    // DecryptForEpoch decrypts ciphertext using a specific epoch's DEK.
    DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)

    // CurrentEpoch returns the current key rotation epoch.
    CurrentEpoch() uint64
}
```

When a `WALBarrier` is configured via `WithBarrier()`, records are encrypted before writing and decrypted during recovery. The barrier must be initialized and unsealed before any WAL operations. Encrypted records embed an 8-byte big-endian epoch prefix in the ciphertext (`ciphertextEpochSize`) so the epoch can be extracted without decryption, which is what lets DEK rotation happen without re-encrypting the whole WAL.

A configured barrier does not by itself mean the records are encrypted. A barrier running `StrategyNone` installs an identity pass-through and the records reach the medium as plaintext. The WAL accepts that deliberately: it is not the layer that decides whether a deployment requires at-rest encryption. What it owes such a caller is the truth, so `AtRestProtection()` reports `AtRestUnprotected` for a plaintext-mode barrier, and the host's at-rest requirement fails closed at the layer that holds the policy.

For the barrier itself (epoch DEKs, HKDF derivation, AEAD selection, rotation and sealing strategies) see [Encryption](../security/encryption.md). What follows is only the WAL side.

### Positional context-AAD

The barrier binds only the epoch into the AEAD AAD, so two records sealed under the same DEK and epoch authenticate identically wherever they sit. An at-rest attacker with write access could reorder, splice or truncate same-epoch records without the tag noticing. Every encrypted WAL record therefore binds its **position** into the AAD as well:

```
"QRWAL1" (6) || WALShardIdx (8) || SegmentID (8) || RecordSeq (8)  = 30 bytes
```

Fixed-width, big-endian, no delimiters, so no two distinct positions produce the same bytes. `WALShardIdx` is required rather than decorative: each of the 16 WAL shards is its own directory with an independent `%06d.log` ID space while all of them share one barrier, so `(SegmentID, RecordSeq)` alone is not unique. `RecordSeq` is the 0-based ordinal of the **logical** record within its segment, which is exactly the index `ReadAll` returns it at during recovery. Chunking across blocks does not affect it, because `ReadAll` assembles First/Middle/Last into one entry before appending.

Two consequences run through the write path:

- The landing segment is resolved **before** the record is sealed (`reserveLanding`), because the AAD names the segment and the ordinal. Encrypting first and discovering the rotation afterwards would bind the record to a segment it never reached. If a sealed record somehow still does not fit, `writeRecord` returns `ErrLandingSegmentOverflow` rather than moving it, because a ciphertext stored at a position its AAD does not name could never be opened again.
- `segment.DiscardPending` rewinds the record ordinal in lockstep with the file offsets. Without that, one rolled-back batch would desynchronize the write and read sides permanently and make the whole segment tail undecryptable at startup.

The magic string is a self-authenticating scheme tag. There is no on-disk version marker for it and no epoch-only fallback: a reader that fails to authenticate a record must never retry without the context, because that retry would be the vulnerability.

### Barrier capability resolution at open

`Open` resolves the barrier's context-binding capability once, before any shard exists to latch it into, and fails closed on three conditions:

| Condition | Error |
|---|---|
| Barrier does not implement the context interface at all | `ErrBarrierContextUnsupported` |
| Barrier implements the pre-`ContextBindingResolved` method set | `ErrBarrierContextBindingUnreportable` |
| Barrier is still sealed, so the capability is not yet an answer | `ErrBarrierContextUnresolved` |

`SupportsContextAAD()` alone cannot be cached, because `false` collapses "cannot bind" and "cannot say yet" into one bit, and caching the second as the first is how a node that starts sealed writes epoch-only records forever even after being unsealed with a fully capable barrier. `ContextBindingResolved()` separates them.

A barrier that resolves as unable to bind (a delegated hardware or KMS encryptor without end-to-end AAD, or no-encryption mode) is accepted with a one-time `WARN`, and its shards latch `noContextBinding`. On that branch alone, every write re-checks `verifyBindingUnchanged`: if the barrier now reports it *can* bind, the write fails with `ErrBarrierContextBindingChanged`. That covers the one drift direction nothing else catches, since `Host.EmergencySeal` can seal a barrier at runtime and an operator can unseal it under a different strategy. The opposite direction needs no check because `EncryptWithContext` refuses a non-empty context it cannot authenticate, so it is already fail-closed.

`maxCiphertextExpansion` (8 + 1 + 24 + 16 = 49 bytes) bounds the envelope `pkg/seal` adds, mirroring its `[epoch:8][algID:1][nonce:N][ciphertext+tag]` layout with the largest nonce (24, XChaCha20-Poly1305) and a 16-byte tag. Over-estimating is safe because `CanFit` is monotonic in the length it is given. A delegated encryptor whose envelope exceeds the bound surfaces as `ErrLandingSegmentOverflow` on records near a segment boundary and should be paired with a segment size that leaves headroom.

## WAL Re-encryption Primitive

**Source:** `pkg/logdb/waldb/reencrypt.go`

The `Reencrypt` function performs logical re-encryption of an entire WAL from one barrier to another:

```go
func Reencrypt(
    srcDir string, srcBarrier WALBarrier,
    dstDir string, dstBarrier WALBarrier,
    snapFn SnapshotReencryptFunc,
    opts ...Option,
) (count int, err error)
```

It returns the number of Raft nodes (shard/replica pairs) copied, and enables key migration for cases like enabling encryption on a previously unencrypted WAL or rotating a WAL's key.

### How It Works

`Reencrypt` performs a logical copy through the public LogDB surface:

1. Open source LogDB at `srcDir` with `srcBarrier` (decryption)
2. Open destination LogDB at `dstDir` with `dstBarrier` (encryption)
3. For each node (shard/replica pair): bootstrap record, latest snapshot metadata, compaction floor, the full live log-entry range in bounded batches, then hard state

Because the source LogDB decrypts each record under `srcBarrier` and the destination encrypts under `dstBarrier`, the result is byte-for-byte identical at the Raft level while the at-rest encryption changes.

The hard state is written **last** on purpose. Each step is independently fsynced and the destination recovers at a record prefix, so a crash mid-migration can only lose the state while keeping the entries, never recover a commit index past the last durable entry.

`opts` are the same options `Open` takes and are applied to **both** sides, which is what makes a non-default geometry migratable at all: opening the source with the default block size when it was written with another is refused outright by the `WALFORMAT` check, and opening the destination with the default would produce a directory the caller's own configuration rejects on the next open. Pass the same options the node normally opens its WAL with. `srcBarrier` and `dstBarrier` are applied after `opts` and win over any `WithBarrier` inside them.

The source is **not** opened read-only. `Open` runs full crash recovery on `srcDir`, which may repair a corrupt segment tail, discard orphaned segments above it, create a new active segment and fsync the directory. That is the same recovery any normal open performs and it never removes records a normal open would have replayed, but `srcDir` is modified on disk, so the caller must not treat it as byte-for-byte immutable across a run.

### Snapshot data files

Snapshot **data files** are not WAL records. They live in the host's snapshot directory, in a host-defined frame format sealed by the host's `seal.Barrier`, so only that layer can re-encrypt them. `waldb` owns LogDB records (entries, hard state, bootstrap, and snapshot *metadata*) and copies those itself.

`SnapshotReencryptFunc` is the host's callback, invoked once per node that has a snapshot, returning the destination epoch to record in the migrated metadata:

```go
type SnapshotReencryptFunc func(shardID, replicaID uint64, snap logdb.Snapshot) (dstEpoch uint64, err error)
```

It may be nil only when no barrier is involved on either side or when no node has a snapshot. Otherwise `Reencrypt` fails closed with `ErrSnapshotReencryptRequired` rather than copy metadata pointing at a source-sealed file the destination cannot decrypt, which would be silent, unrecoverable snapshot loss.

### Barrier Flexibility

Either barrier may be `nil` for a plaintext side:

- `srcBarrier = nil, dstBarrier = *Barrier`: encrypt a plaintext WAL (e.g., none→keyed migration)
- `srcBarrier = *Barrier, dstBarrier = nil`: decrypt an encrypted WAL
- Both non-nil: re-encrypt with a different key

### Error Handling

`Reencrypt` wraps errors in `ReencryptError` with an operation name to identify the failure point (open-source, open-dest, list-nodes, get-bootstrap, get-snapshot, get-state, iterate-entries, save-*, close-dest). This allows operators to see exactly where a migration stopped.

Closing the **destination** is part of the migration and its failure is reported (`Op` "close-dest"). `DB.Close` flushes, truncates and closes every shard's active segment, so a swallowed error there would report success for a rekey whose newly written WAL was never completed on disk, and the caller's next act is to swap that directory into place. The source's close error is discarded, because the source was only read.

A node with no snapshot is tolerated (young shards whose log fits in the live range). `logdb.ErrNoSnapshot` is not a failure — the migration proceeds with hard state and entries alone.

### Durability and Safety

- `dstDir` must be empty; the caller verifies the result and performs the atomic directory swap, so a partial run never touches the source
- All written records are durably fsynced before the function returns
- The result is safe for atomic `rename(srcDir, dstDir_backup); rename(dstDir_new, dstDir)` on success

### Memory Bounds

Entry iteration uses bounded batches (`reencryptBatchBytes = 64 MiB`) to keep peak memory flat regardless of WAL size, making re-encryption safe for large logs.

## Post-Save Flow

After a successful `SaveState`, the commit worker executes a critical sequence defined in `Peer.Commit()` (in `pkg/internal/raft/peer.go`):

```
Peer.Commit(update)
  |
  +-- logReader.ApplySnapshot(Snapshot)  // only when the update carries one
  +-- logReader.Append(EntriesToSave)    // populate termRingBuffer (O(1) append)
  |     ^ the only FALLIBLE steps, so they run FIRST
  +-- appliedTo(lastProcessed)           // advance processed marker
  +-- processConfigChanges()             // commit-time membership; clears pendingConfigChange
  +-- raftLog.commitUpdate(uc)           // advance savedTo, appliedLogTo
  +-- tryCommit + broadcastAppend        // leader only, when StableLogTo moved
```

The ordering is load-bearing. The two `logReader` calls are the only steps that can fail, and they run before any marker advances, which keeps an error fully re-deliverable: the processed marker has not moved, so `entriesToApply()` offers the same batch again. If they ran after `appliedTo`, a failure would strand committed entries that can never be re-emitted, and the state machine would silently miss a range while the shard kept serving reads.

The `ApplySnapshot` call is not optional bookkeeping either. Without it the LogReader keeps its initial empty range and every later `Append` for entries after the snapshot index fails with `ErrLogGapDetected`.

The trailing `tryCommit` closes the persistence edge: `tryCommit` uses the leader's own durable index (`inmem.savedTo`) as its match, so a commit blocked only on local durability becomes possible exactly here, once `savedTo` has advanced.

`logReader.Append` populates the LogReader's `termRingBuffer`, a fixed-capacity circular buffer of (index, term) pairs that provides O(log n) term lookups via binary search. This replaces the prior `map[uint64]uint64` term cache, which caused O(n) iteration on every trim and dominated CPU under concurrent load. Capacity is `maxTermCacheEntries = 100_000` pairs of two `uint64`s, a 1.6 MB flat slice with zero GC pressure.

### In-Memory Log Lifecycle

The `inMemory` struct (`pkg/internal/raft/inmemory.go`) tracks three markers:

- `savedTo` -- highest index persisted to LogDB
- `appliedTo` -- highest index applied to state machine
- `markerIndex` -- index of first entry still in memory

`entriesToSave()` returns entries with `Index > savedTo`. After `Peer.Commit` calls `savedLogTo`, those entries are marked as persisted. After `appliedLogTo`, entries at or below the applied index are truncated from the front of the in-memory slice, freeing memory.

`appliedLogTo` uses lazy compaction via a `wastedPrefix` counter that tracks logically truncated entries without immediate reallocation. The backing slice is reallocated only when **both** conditions hold: `wastedPrefix >= entrySliceSize` (512) **and** `wastedPrefix >= 2 * live`. The second condition is what stops a 512-entry allocation when 512 entries are wasted but 500 are still live. This reduces allocation frequency by roughly 128x under steady load compared to allocating a fresh buffer on every apply.

## Durability Guarantees

Data becomes durable at a precise point: when `fdatasync` returns successfully. Inside a `SaveState` batch that is Phase 2 (`syncDirtyShardsLocked`); for the standalone writers it is the inline sync at the end of `writeRecord`. Here is the crash analysis for each stage:

| Stage                         | Crash Behavior                              |
|-------------------------------|---------------------------------------------|
| Record serialized to marshalBuf | Lost. Only in process memory.             |
| encodeRecord into blockBuf    | Lost. Block not yet written to OS.          |
| f.Write (blockBuf to OS)      | In OS page cache. Lost on power failure.    |
| fdatasync returns             | Durable. Survives power failure. Closes a durability generation: the next record is stamped with a higher sequence, which is what proves on recovery that everything below it was acknowledged. |
| In-memory maps updated        | Redundant. Rebuilt from WAL on recovery.    |

The `noSync` option (`WithNoSync(true)`) disables `fdatasync`, leaving data in the OS page cache. This is safe only for testing. It also suppresses every directory fsync and leaves the WAL root to be created lazily by the first write.

The in-memory maps are a read cache and are never the source of truth. They are updated only at a batch's Phase 3 commit point, after the WAL data is durable, so a crash between the fsync and the in-memory update loses nothing: recovery rebuilds the maps from the WAL.

### The invariants recovery depends on

These hold across the whole write path and are what the recovery rules below are entitled to assume:

1. **A WAL is valid only as a prefix.** Nothing after the first damaged byte is ever replayed, whatever it decodes to.
2. **A record's durability generation never decreases within a segment.** `Sync` increments the counter only after `fdatasync` returns, and `DiscardPending` deliberately does *not* rewind it: a completed sync is a fact about the medium.
3. **An acknowledged byte is written exactly once.** `flushBlock` starts at the `flushed` watermark, so no later batch's write puts already-fsynced bytes back in flight.
4. **`fileOffset` moves only by whole blocks, only in `padAndFlushBlock`, only after the block is complete on disk.** Recovery therefore always finds a chunk header at every multiple of `BlockSize`.
5. **An fsync failure is terminal for the DB.** After `db.poison`, every write returns `ErrDBPoisoned` and the process must restart into crash recovery. Continuing after a failed `fdatasync` would build on bytes the kernel may have marked clean without writing.
6. **Only `ErrNotExist` is read as absence.** `openWALShard` fails the open on `EACCES`, `EIO`, `ENOTDIR`, `ELOOP` or a stale handle from its `Stat` of the shard directory, because treating any of those as absence would skip recovery, leave `nextSegmentID` at 0, and let the first write recreate `000000.log` with `O_TRUNC`, erasing the node's term, vote and entries.
7. **The compaction floor is monotonic.** `RemoveEntriesTo` drops a request for a floor at or below the current one before it reaches the WAL, because persisting the lower marker would also re-pin garbage collection to it and let the higher marker's segment be reclaimed, so the regression would survive a restart.
8. **Entries at or below the floor are refused, not clamped.** `SaveState` returns `ErrEntriesBelowCompactionFloor` for the whole batch. Storing them would truncate-and-append over the live suffix above the floor; clamping would silently discard entries the caller was told were persisted.

### Disk Full Detection

`WithDiskMonitorOpts(maxBytes, ...)` installs a `DiskMonitor` when `maxBytes > 0`. `SaveState` calls `MaybeCheck` at entry, before writing anything; `Compact` and a failed `fallocate` force an immediate check. Thresholds are fractions of `maxBytes`, not free space: `DefaultWarningPercent` = 0.85 fires the warning callback, `DefaultFullPercent` = 0.95 fires the full callback and returns `ErrDiskFull`, and a check runs every `DefaultCheckEvery` = 100 writes. All three are overridable (`WithWarningPercent`, `WithFullPercent`, `WithCheckEvery`), along with the callbacks (`WithOnWarning`, `WithOnFull`, `WithOnRecovery`) and the usage query itself (`WithDiskUsageFunc`). Callbacks fire on threshold *transitions* only, so a sustained full disk does not spam them.

Once the monitor has tripped the full state it bypasses the sample throttle and re-checks on **every** call until usage drops back below the threshold. Without that, the next 99 writes would be admitted unchecked into an already-full disk: a follower's replication writes reach the monitor through `SaveState`, while the host-level disk-full flag gates only client `Propose` and `ReadIndex`, so the WAL would keep accepting batches until `fdatasync` hit hard ENOSPC and poisoned the DB.

If the usage query itself fails, the monitor logs at `DEBUG` and does not block writes. That is a fail-open on the *monitor*, not on durability: an unreadable `statfs` is not evidence of a full disk, and the real ENOSPC still surfaces from the write or the fsync.

Disk pressure has no special recovery path. A segment left short by ENOSPC is classified by the same rules as any other damage.

## Compaction

Compaction operates at two levels: logical entry removal and physical segment garbage collection.

### Logical Compaction: RemoveEntriesTo

`RemoveEntriesTo(shardID, replicaID, index)` writes a compaction marker to the WAL, then removes entries from in-memory state:

```go
func (db *DB) RemoveEntriesTo(shardID, replicaID, index uint64) error {
    shard := db.getShard(shardID)
    shard.mu.Lock()
    defer shard.mu.Unlock()

    // Write compaction marker to WAL for durability.
    buf := shard.getMarshalBuf(1 + compactionRecordSize)
    buf[0] = payloadTypeCompact
    // ... encode shardID, replicaID, index ...
    if err := shard.writeRecord(buf[:1+compactionRecordSize]); err != nil {
        return err
    }

    shard.compactedTo[key] = index
    idx.Compact(index)                      // remove from sorted index
    for raftIdx := range nodeEntries {      // remove from entry map
        if raftIdx <= index {
            delete(nodeEntries, raftIdx)
        }
    }
    return nil
}
```

The compaction marker is WAL-durable so that after recovery, the compaction is replayed and the same entries are excluded from the rebuilt in-memory state.

The real implementation adds three things the sketch above leaves out. It captures a segment checkpoint before writing, so a failed write leaves nothing durable behind, matching `SaveState`'s rollback discipline. It refuses a floor at or below the current one before touching the WAL (invariant 7 above). And its whole in-memory update, floor plus index pruning plus entry-map pruning plus the GC pin and the commit high-water mark, happens in one `dataMu` section, so a lock-free reader never sees the raised floor without the matching pruning, or the reverse, which would read as a log gap.

### Physical Compaction: Compact / GarbageCollect

`Compact(shardID, replicaID)` triggers garbage collection of obsolete segment files:

```go
func (db *DB) Compact(shardID, _ uint64) error {
    shard := db.getShard(shardID)
    shard.mu.Lock()
    defer shard.mu.Unlock()

    comp := newCompactor(shard.fs, shard.dir, shard.opts.blockSize)
    liveSegments := make(map[uint64]bool)
    for _, idx := range shard.indexes {
        for segID := range idx.AllSegmentIDs() {
            liveSegments[segID] = true
        }
    }
    // Pin segments holding the latest meta records (see below).
    shard.addMetaSegmentsToLive(liveSegments)

    deleted, err := comp.GarbageCollect(
        liveSegments, shard.activeSegmentID, shard.committedSegmentHWM)

    dirSynced := true
    if deleted > 0 {
        if syncErr := syncDirFS(shard.fs, shard.dir); syncErr != nil {
            dirSynced = false  // deletions unproven; skip the epoch-floor raise
        }
    }
    // Recompute the epoch floor from what actually survived ON DISK, and only
    // when GC succeeded, deleted something, and the directory sync proved it.
    if err == nil && deleted > 0 && dirSynced && shard.barrier != nil {
        shard.recomputeMinWriteEpochFromDisk()
    }
    return err
}
```

`GarbageCollect` scans the shard directory and deletes segment files that pass three tests:

```go
func (c *compactor) GarbageCollect(liveSegments map[uint64]bool, maxLiveID, maxCommittedID uint64) (int, error) {
    for _, de := range dirEntries {
        segID, ok := parseSegmentFilename(de.Name())
        if !ok { continue }
        if segID >= maxLiveID { continue }       // never delete the active segment
        if segID > maxCommittedID { continue }   // written but not yet indexed
        if liveSegments[segID] { continue }      // still referenced
        c.fs.Remove(path)                        // safe to delete
        deleted++
    }
    return deleted, nil
}
```

Two safety invariants sit in those tests:

**The active segment is never deleted**, even holding no referenced entries, so an in-flight write cannot target a deleted file.

**A segment above `committedSegmentHWM` is never deleted.** That mark is the highest segment ID whose batch has fully completed Phase 3 (or a non-batch commit). It closes a cross-goroutine window: a concurrent same-shard `Compact`, driven from the snapshot-pool goroutine or the public Host API, can acquire `shard.mu` after a batch's durable write but before its in-memory index update, compute `liveSegments` without the just-written segment, and reclaim it. The high-water mark advances monotonically at the commit points, under `mu`, and commits atomically with the index and pin updates it covers because it is also protected by `dataMu`.

The epoch-floor recompute is ordered after the directory fsync for a reason of its own. The recompute raises `minWriteEpoch` on the strength of the unlinks, and the engine purges the corresponding DEK once `MinLiveEpoch` reports the higher floor. If the directory fsync failed, or the process crashed first, the unlink is not durable, the segment reappears on restart, and replay hits a fatal `WALDecryptError` with the DEK already gone: the node cannot start at all. Deferring the floor raise is always safe; raising it on an unproven deletion is not. The recompute also reads what actually survived on disk rather than the intended `liveSegments` set, because GC deletes files one at a time and can abort partway.

#### Pinning Meta-Record Segments

The live-segment set is **not** computed from entry indexes alone. State, snapshot, bootstrap, compaction-marker and remove-node records are never tracked by any entry index, so a segment whose entries have all been compacted away can still hold the sole durable copy of one of them. Computing liveness only from `idx.AllSegmentIDs()` would let GC delete such a segment, losing that record: a lost compaction marker, for instance, resets `compactedTo` to 0 on the next recovery.

To prevent this, each `walShard` tracks, per node, the segment ID that holds the latest durable record of each of the five kinds:

```go
type metaSegments struct {
    state        uint64
    snapshot     uint64
    bootstrap    uint64
    compact      uint64
    remove       uint64
    hasState     bool   // segment IDs are 0-based, so presence needs a flag
    hasSnapshot  bool
    hasBootstrap bool
    hasCompact   bool
    hasRemove    bool
}

// walShard field, protected by dataMu
metaSegments map[nodeKey]metaSegments
```

Each record type carries an explicit presence flag rather than treating a zero ID as "none", because segment IDs are 0-based: segment 0 is a valid pin and must be distinguishable from the absence of a record.

These segment IDs are recorded in two places, both following last-writer-wins semantics:

- **On write-land**: each writer calls its `setXxxSegment` setter with the segment the record actually landed in, taken from the `recordLanding` that `writeRecord` returns, so a mid-write rotation is accounted for. For batched state and snapshot writes the pin is applied at the Phase 3 commit point via `pendingMemUpdates.applyTo`.
- **During recovery replay**: `replayState`, `replaySnapshot`, `replayBootstrap`, `replayCompaction` and `replayRemoveNode` call the same setters with the replayed segment ID. Because segments replay in ascending ID order, the last replayed record wins, matching last-writer-wins at write time.

Before running `GarbageCollect`, both `Compact` and `RemoveNodeData` fold these meta-segments into the live set via `addMetaSegmentsToLive`:

```go
func (s *walShard) addMetaSegmentsToLive(liveSegments map[uint64]bool) {
    for _, m := range s.metaSegments {
        if m.hasState     { liveSegments[m.state] = true }
        if m.hasSnapshot  { liveSegments[m.snapshot] = true }
        if m.hasBootstrap { liveSegments[m.bootstrap] = true }
        if m.hasCompact   { liveSegments[m.compact] = true }
        if m.hasRemove    { liveSegments[m.remove] = true }
    }
}
```

This guarantees that the segment holding a live node's latest meta record of any kind is never garbage-collected. When a node is removed (`RemoveNodeData`) or replayed past (`replayRemoveNode`), its `metaSegments` entry is dropped, so its now-obsolete meta-segments become eligible for reclamation if no surviving node references them.

#### Deletion Durability

When `GarbageCollect` deletes one or more segment files, both `Compact` and `RemoveNodeData` fsync the shard directory (`syncDirFS`) afterward. Without this, a metadata-lazy filesystem (ext4 in default ordered mode, for example) could lose the directory-entry removals on a crash and resurrect a deleted segment, replaying records that were supposed to be gone. The directory fsync is best-effort for the deletions themselves: a failure is logged at `WARN` but not fatal, because the next compaction retries them and the in-memory index already excludes the deleted segments. It is **not** best-effort for the epoch floor, which is skipped entirely on that path.

## Recovery

On `Open`, each shard directory is checked for existence. If it exists, `recover()` is called to rebuild all in-memory state from WAL segments.

### Recovery Sequence

The step order in `Open` is part of the contract, not incidental sequencing. The geometry and format-version checks run before any segment is touched, and the barrier's context-binding capability is resolved before any shard exists to latch it into.

```
Open(dir)
  |
  +-- validateOpenOptions
  +-- readWALFormat --> geometry + format version, or establishWALFormat  <-- before any segment
  +-- resolveBarrierContextBinding                                        <-- before any shard
  +-- createWALRoot --> MkdirAll + fsync the whole created ancestor chain
  |
  +-- for each shard (0..15):
        openWALShard(shardDir)
          |
          +-- fs.Stat(dir): exists?
          |     yes             --> recover()
          |                          +-- ReadDir to find %06d.log files
          |                          +-- sort segment IDs ascending
          |                          +-- for each segment in order:
          |                                replaySegment(segID)
          |                                  +-- fs.Open (read-only)
          |                                  +-- segment.ReadAll()
          |                                  +-- for each record: replayRecord(rec, segID)
          |                          +-- repair (truncate + discard higher segments) if warranted
          |                          +-- ensureActiveSegment() when segments were found
          |
          |     ErrNotExist     --> lazy init (no directory created yet)
          |     any other error --> FAIL THE OPEN (invariant 6)
```

`recover()` takes neither lock. It runs inside `openWALShard`, before the shard is reachable from any DB the caller can observe, so the whole replay and the repair it may perform are exclusive by construction. Locking per replayed record would cost a mutex round trip per WAL record on the recovery path and protect nothing.

A shard opened by a failed `Open` unlinks the segment that open created, if it holds no records. Without that, every failed open leaves one empty segment per shard and `nextSegmentID` advances past it, so the files accumulate. That was tolerable when a failed open was rare, but the fail-closed conditions this package raises (geometry mismatch, unresolved barrier binding, mid-segment corruption) are all conditions an operator retries against while diagnosing.

### ReadAll: Block-by-Block Scanning

`segment.ReadAll()` reads the segment file block by block. For each block, it decodes chunks sequentially, reassembling multi-chunk records:

1. Read `blockSize` bytes at the current offset
2. For each position in the block:
   - If the byte is zero and all remaining bytes are zero, this is EOF padding
   - If fewer than `recordHeaderSize` (11) bytes remain, skip to next block
   - Call `decodeRecord` to parse CRC, length, type, durability sequence, and payload
   - If CRC verification fails, discard any partial multi-chunk record and skip to the next block boundary
3. Reassemble multi-chunk records: `RecordFirst` starts accumulation, `RecordMiddle` appends, `RecordLast` completes and emits
4. `RecordFull` is emitted as a complete record immediately

A real I/O error from the block read (EIO, a permission failure, a failing sector) is **not** treated as end of data. It returns `*SegmentReadError` wrapped in `*RecoveryError`. Combined with prefix-only validity it would otherwise masquerade as data loss and drop all higher segments. That holds however many bytes came back: a short read carrying an error is still an error, and processing its prefix would stride a whole block forward and replay the following blocks with neither a hole nor a corruption recorded.

### Replay Dispatch

Each recovered record is dispatched by its 1-byte payload type prefix. When a barrier is configured the record is decrypted first, under the same positional context-AAD the write side sealed it with.

```go
func (s *walShard) replayRecord(rec []byte, segID uint64) (int, string, error) {
    switch rec[0] {
    case payloadTypeEntries:    return 0, "", s.replayEntries(rec[1:], segID)
    case payloadTypeState:      return 0, "", s.replayState(rec[1:], segID)
    case payloadTypeSnapshot:   return 0, "", s.replaySnapshot(rec[1:], segID)
    case payloadTypeBootstrap:  return 0, "", s.replayBootstrap(rec[1:], segID)
    case payloadTypeCompact:    return 0, "", s.replayCompaction(rec[1:], segID)
    case payloadTypeRemoveNode: return 0, "", s.replayRemoveNode(rec[1:], segID)

    default:                     return 0, "", ErrUnknownPayloadType
    }
}
```

Replay builds the same in-memory state that normal writes would:

- Entries are added to both the index and the entries map. A batch whose first replayed index is already present is a leader overwrite: `idx.Append` truncates the superseded index suffix, and replay additionally prunes the orphaned higher indices from the entry map, mirroring the write path.
- States, snapshots and bootstraps replace any previously replayed value, last writer wins.
- Compaction markers remove entries from the index and entries map; remove-node tombstones drop the node's state entirely.
- Every replayed record also re-establishes its GC pin and its contribution to the epoch floor.

**No replay failure is ever skipped.** A record that already passed its chunk CRC but fails to decode is corruption the CRC did not explain, and skipping it while later records and segments still replay would leave a silent hole. The consequences differ by type and all of them are fatal: a dropped entry batch is a Log Matching violation served to followers as a shortened, nil-error range; a dropped `State` record lets an older one win last-writer-wins, regressing term and vote and permitting a double vote in one term; a dropped `Snapshot` record boots the node with empty membership.

### Handling Partial and Corrupted Records

**A WAL is valid only as a prefix.** Nothing after the first damaged byte is ever replayed, whatever it decodes to: resurrecting a record from beyond a gap would create a hole in the Raft log and violate Log Matching. The clean prefix collected before the damage is final.

- **Partial records**: If `ReadAll` encounters a multi-chunk record where `RecordFirst` was read but `RecordLast` was not (e.g., crash during write), the partial record is discarded.
- **CRC failures**: `decodeRecord` returns `ErrCorruptedRecord`. The scan stops collecting records there and switches to classifying the damage (below). It does **not** resynchronize and resume.
- **Zero-filled blocks**: Pre-allocated but unwritten blocks are detected by checking for all-zero bytes. A zero region is normal EOF padding only when nothing follows it; a zero region with data after it is a **write hole** (a crash lost block N while block N+1 reached the medium) and recovery stops at the hole.
- **Individual record replay errors**: `replayRecord` reports a failure to decode a record that already passed its CRC as a fatal recovery error. It is not skipped: a dropped `State` record would let an older one win last-writer-wins (a term/vote regression, i.e. voting twice in one term) and a dropped `Snapshot` record would boot the node with empty membership.
- **Segments above the damage**: `stoppedAtCorruption` stops recovery replaying any higher-numbered segment, and the repair path deletes those segments (or, if `Remove` fails, truncates them to zero and fsyncs so they replay to nothing).

### Classifying Corruption: Interrupted Write vs Damage to Acknowledged Data

Finding damage is not the same as knowing what to do about it. Truncating to the clean prefix is correct for an **interrupted write** — one the medium never finished, whose records nothing was ever told about — and destroys acknowledged data when the records above the damage had already been fsynced.

The framing alone cannot tell these apart, and that is not a theoretical gap. A batch larger than one block issues one `WriteAt` per block before a single `fdatasync`, so writeback may persist an arbitrary subset of those pages. Losing an interior or leading page of block N while block N+1 survives leaves intact, CRC-passing records sitting above a CRC failure — byte-for-byte what bit rot in the middle of a live segment leaves. Recovery used to treat "something decodes above the damage" as proof of the second case and refuse to start, which made a routine power loss a permanent refuse-to-start.

The [durability sequence](#the-durability-sequence) in each chunk header settles it. After the first decode failure the scan keeps going, collecting the generation of every chunk that still decodes above the damage:

1. It probes forward **within the damaged block**, at byte granularity, resynchronizing on each chunk's own declared length. Each candidate is validated by its own CRC, so a hit is evidence rather than a guess. This matters most in the last block holding data, where there are no later blocks to consult.
2. It probes each **subsequent block** the same way, and stops early as soon as the generations settle the question.

Nothing found by either probe is ever replayed. The only output is the classification:

| Finding above the damage | Meaning | Action |
|---|---|---|
| Nothing decodes | Interrupted write, classic torn tail | Repair and continue (order below) |
| Everything that decodes carries the segment's single highest generation | Interrupted write — no completed `fdatasync` sits above the damage, so nothing there was acknowledged | Repair and continue (order below) |
| The records span more than one generation | A `Sync` completed between them, so the lower ones were acknowledged (`ErrMidSegmentCorruption`) | **Refuse to open.** The file is left byte-for-byte intact |
| A record carries a generation *lower* than one already replayed from the clean prefix | Impossible for this writer — the counter only rises. A misdirected write, a partial restore of an older file, or tampering (`ErrRecordSequenceRegression`) | **Refuse to open.** The file is left byte-for-byte intact |
| Damage in a segment that is **not** the highest-numbered one | Acknowledged by construction — a lower segment was fully fdatasynced before any higher one was created (`ErrNonFinalSegmentCorruption`) | **Refuse to open.** The file is left byte-for-byte intact |

**The repair runs high to low, and the order is a crash-safety requirement.** Segments above the damage are unreachable under the prefix rule, so they are deleted **first**, with the directory fsynced; only then is the corrupt segment truncated to its clean end, with its own fsync last. Every intermediate crash state is then a valid prefix: the corrupt segment still carries its damaged tail, so the next recovery stops at exactly the same offset and repeats exactly the same repair.

Truncating first is what a crash cannot survive. Segment K cut back to the clean end finishes at a record boundary with no damage left to detect, so a restart before the higher segments were deleted replays K in full and then K+1..N, resurrecting the records the prefix rule had just decided must never come back, with a hole where the cut region was. A discard failure is fatal for the same reason: an intact orphan left behind would replay its stale records on the next restart. If `Remove` fails, the orphan is truncated to zero and fsynced instead, so it replays to nothing.

Truncating at all is what stops the corrupt tail being frozen forever. Without it, every later restart re-detects the damage and the discard deletes every segment written since the previous restart: committed entries, hard state (a term/vote regression, so a double vote), snapshot metadata and compaction markers, recurring on every boot.

**What this changed.** The torn multi-block write above is now repaired automatically. It was the reachable-from-a-routine-crash case, and the one for which a single-node deployment had no good exit.

**Residuals, stated plainly.** The generation proves *when a completed fdatasync separated two records*, and nothing else. Four cases remain:

- **Rot inside the final, in-flight batch** is truncated rather than refused. Reaching it needs rot AND a crash before anything else was written, since any later batch would raise the segment's generation and expose the damage as interior.
- **A generation boundary inside the damaged region.** When the surviving records carry a generation *higher* than the clean prefix's, a completed `fdatasync` happened somewhere in the destroyed bytes, and any older-generation record the damage swallowed had been acknowledged — whether one existed is precisely what the destroyed bytes would have said. Recovery truncates anyway, because a crash cannot produce this shape from acknowledged bytes: a partial write to a page that already holds fsynced data leaves that page at its previously persisted content if the writeback is lost, so an interrupted write's damage begins at or above the acknowledged end. Refusing would instead refuse the ordinary case where the lost page starts exactly at a batch boundary — the common shape of the failure this scheme exists to repair. Media damage that straddles the boundary is the residual, and it logs a named `WARN` telling the operator to reconcile the node against its peers rather than passing silently.
- **A zero-filled hole with data after it** is classified by the SAME generation evidence as a failed CRC. The two classifiers share one verdict: `ReadAll` records the hole as the corruption point and lets the post-corruption sweep gather the durability generations of whatever still decodes above it, exactly as it does for a checksum failure (`segment.go`, "Data after a zero hole"). A crash cannot produce a hole under acknowledged data in the first place — writes are strictly ascending in file offset, so a block lying below a later generation was covered by that generation's `fdatasync` and its pages cannot revert to zeros — so a hole whose survivors span a completed `fdatasync` is media damage or tampering, and gets the fail-closed answer the CRC path gives.
- **`noSync` mode** (tests only) issues no `fdatasync`, so every record in such a segment carries generation 0 and truncation is always permitted. That is correct rather than a hole — nothing in a segment that was never synced was ever acknowledged — but it means a test that wants recovery to see acknowledged data must let the syncs run.

### Generation-boundary residuals

The second of those cases is the one an operator has to be told about, so it is carried out of `ReadAll` as structured data rather than left in a log line. `waldb.DB` implements `logdb.RecoveryResidualReporter`:

```go
if r, ok := db.(logdb.RecoveryResidualReporter); ok {
    for _, res := range r.RecoveryResiduals() { ... }
}
```

Each `logdb.RecoveryResidual` reports the storage shard index (the WAL directory index in `[0, 16)`, **not** a Raft ShardID), the segment ID, the byte offset it was truncated to, the two generations that straddle the destroyed region (`SurvivingGeneration` is always strictly greater than `PrefixGeneration`, and that gap is the evidence an fsync completed inside the damage), and `Nodes`: every replica whose log lives in that storage shard, with the index range that survived. `Nodes` is a **superset** of the truly affected replicas, because the destroyed bytes cannot say which replicas they belonged to, so every replica sharing the shard is reported rather than guessed at.

Treat a non-empty report as an operational fault requiring intervention, not a warning. The replica is not corrupt and not degraded in any way it can detect at runtime: it serves reads and accepts writes while missing entries it previously promised. The condition is only ever resolved from outside, by reconciling the replica against its peers or rebuilding it from a snapshot.

The report is a property of the **open**, fixed once recovery completed. Nothing a caller does clears it. It does not survive the process either, because recovery repaired the damage on disk and a later open of the same store reports nothing, so a caller that needs the fact to outlive the process must record it when it observes it.

### Recovery Error Reference

| Error | Raised when | Recovery |
|---|---|---|
| `ErrWALGeometryMismatch` (`*WALGeometryMismatchError`) | Configured `BlockSize` differs from the `WALFORMAT` marker | Restore the original `BlockSize`. The error names both values |
| `ErrWALFormatMalformed` | The marker exists with non-zero bytes that are not our format | Investigate the file. An empty or all-zero marker is read as ABSENT and never raises this |
| `ErrWALFormatUnsupportedVersion` (`*WALFormatVersionError`) | The marker records an on-disk format version this build does not read | Wipe the data directory and re-sync. The error names both versions. See [Compatibility](../operations/compatibility.md#4-walformat-marker-version--1--persisted-exact-match-checked-first) |
| `ErrWALFormatMissing` | The directory holds segments but no marker, so it predates the marker (and the durability sequence) | Wipe the data directory and re-sync. Opening anyway would misparse every chunk and truncate the log to zero |
| `ErrRecordSequenceRegression` (`*SegmentCorruptionError`) | A record above the damage carries a durability generation lower than the clean prefix — bytes this WAL cannot have written | Re-sync from peers, or authorize `waldb.Repair`. Treat the device as suspect |
| `ErrMidSegmentCorruption` (`*SegmentCorruptionError`) | Damage below records that a completed `fdatasync` had already acknowledged (they span more than one durability generation) | Re-sync from peers, or authorize `waldb.Repair` — see below |
| `ErrNonFinalSegmentCorruption` (`*SegmentCorruptionError`) | Damage in a segment that is not the highest-numbered one | Re-sync from peers, or authorize `waldb.Repair` — see below |
| `*SegmentReadError` (wrapped in `*RecoveryError`) | A real I/O error (EIO, a failing sector) during the block scan | Hardware fault. Treat the disk as suspect; re-sync from peers |
| `*WALDecryptError` | A record failed AEAD authentication during replay | Wrong barrier, wrong key epoch, or tampering. Do not retry without the context-AAD |
| `ErrBarrierContextUnresolved` | A barrier was configured but is still sealed at `Open` | Unseal the barrier **before** opening the WAL |
| `ErrBarrierContextBindingUnreportable` | The configured barrier lacks `ContextBindingResolved()` | Out-of-tree barrier; add the method. See [Compatibility](../operations/compatibility.md) |
| `ErrBarrierContextUnsupported` | The configured barrier cannot bind caller context at all | Configure a context-capable barrier. There is no epoch-only fallback by design |
| `ErrBarrierContextBindingChanged` | A shard latched "cannot bind" and the barrier now reports it can | The barrier was sealed and unsealed under a different strategy. Restart the node |
| `ErrUnknownPayloadType` | A replayed record's leading byte is not a known payload type | Most often a keyed WAL opened with `barrier == nil`, whose ciphertext epoch prefix decodes as `0x00`. Supply the barrier |
| `ErrUnknownPayloadType` | A retired or unrecognised payload type appears on replay | Same remedy. This is what an unreadable older directory produces, since retired bytes are left unallocated rather than named |
| `ErrUnsupportedMembershipVersion` | A snapshot record's membership section carries a version byte other than 1 | Written by a binary from a different pre-release commit. Wipe and re-sync |
| `ErrUnsupportedSnapshotCompression` | A snapshot record names a compression algorithm this build cannot decompress | Same: a pre-release build skew. Wipe and re-sync |

These errors surface from `Open`, not from a later write. Two others fail a running DB rather than an open:

| Error | Raised when | Recovery |
|---|---|---|
| `ErrDBPoisoned` | An `fdatasync` failed, so no subsequent write can establish durability | Restart the process into WAL crash recovery. There is no in-process reset |
| `ErrLandingSegmentOverflow` | A positionally sealed record no longer fits the segment its context-AAD names | Should be unreachable with the software barrier. A delegated encryptor whose envelope exceeds `maxCiphertextExpansion` needs a segment size with more headroom |

### Recovering from a Refuse-to-Start

`ErrMidSegmentCorruption`, `ErrNonFinalSegmentCorruption` and `ErrRecordSequenceRegression` stop the node on purpose: above the damage sit records the node already acknowledged to its leader, and truncating past them would shorten the log it promised and roll term/vote backwards. The node stays down until someone decides what to lose.

**What no longer reaches this page.** The common way to arrive here used to be an ordinary power loss. A batch spanning several blocks left intact records above a CRC failure, recovery could not tell that from bit rot, and it refused to start — permanently, on every boot, with `waldb.Repair` the only exit on a single-node deployment. The [durability sequence](#the-durability-sequence) makes that case self-healing: recovery truncates the interrupted batch and the node boots. Concretely, these are now unreachable:

- `ErrMidSegmentCorruption` from a torn multi-block write, where every record above the damage belongs to the batch that was still in flight. This was the dominant source of the error in practice.
- `ErrMidSegmentCorruption` from damage in a segment that no `Sync` ever completed on. Nothing in such a segment was acknowledged, so there is nothing to protect.

What still reaches it is damage below records that a completed `fdatasync` acknowledged — bit rot, a misdirected write, a lying fsync, or tampering — which is what the fail-closed posture was always for. If you land here now, the disk is suspect in a way an interrupted write does not explain.

**Preferred: re-sync from peers.** Confirm the remaining nodes hold a quorum, stop the node, wipe its `WALDir` and `NodeHostDir`, and start it. It rejoins by snapshot plus log replay and loses nothing, because its peers hold everything it acknowledged.

**When there are no peers: authorized repair.** For a single-node deployment, or a cluster that has lost quorum, `waldb.Repair` truncates one segment at one offset and deletes the segments above it. It is a separate call an operator makes by hand — not an `Option`, not reachable from `Open`, and impossible to enable by accident in a config file.

```go
// The error names everything the repair needs.
var corrupt *waldb.SegmentCorruptionError
if errors.As(err, &corrupt) {
    res, rerr := waldb.Repair(walDir, waldb.RepairTarget{
        ShardIndex: corrupt.ShardIndex,
        SegmentID:  corrupt.SegmentID,
        TruncateAt: corrupt.CleanEnd, // must match exactly
    }, waldb.WithBlockSize(cfg.BlockSize))
    // res.RecordsKept, res.BytesDiscarded, res.SegmentsDeleted
}
```

Procedure:

1. **Take a copy of the data directory first.** Repair is destructive and there is no undo.
2. Read the shard, segment and clean-prefix offset out of the `*SegmentCorruptionError` (also logged at `ERROR` on the failed open).
3. Run `Repair` with the database closed and no other process on the directory. `BlockSize` must match the directory's real geometry.
4. Restart the node and reconcile it against peers if any exist.

Guards, all of which refuse rather than proceed:

- `TruncateAt` must equal the clean-prefix end the scan itself computes (`ErrRepairOffsetMismatch`, which reports both offsets). An operator confirms the offset the WAL named; they cannot choose one.
- A segment that is not actually corrupt is refused (`ErrRepairNoCorruption`), so a stale or copied error cannot truncate a healthy segment.
- A shard index outside `[0, 16)` is refused (`ErrRepairShardOutOfRange`).
- The block size is held to the same `WALFORMAT` check `Open` enforces (`checkRepairGeometry`). A repair run on the wrong grid computes a meaningless clean end, and this is the one code path whose entire job is to truncate at that number. The offset interlock would catch it as a mismatch, but naming the real cause beats making an operator wonder why the offset from the error is being rejected.
- No barrier is required: the scan works at the CRC framing level and never decrypts, so a sealed or unavailable barrier does not stand between an operator and a bootable node.

**What repair costs.** Every record above `TruncateAt` is destroyed, including the newest `HardState` in that shard. The node's most recent writes are gone; on a replicated shard it catches up from peers, and on a single-node deployment the loss is final. The call logs at `ERROR` before it acts and at `WARN` after, naming the offset, the bytes discarded and the segments deleted.

### Reconciling a Lost Compaction Floor

The last thing `recover()` does, after every segment has replayed, is check each node's rebuilt index against its recovered compaction floor.

The floor is durable only as a WAL marker record, but its effect (the segment holding the compacted entries being unlinked) is irreversible. Tear the block holding that marker and replay comes back with a lower floor, often 0, while the entries it covered are physically gone. Nothing else re-derives it.

That state **fails open**, which is what makes it dangerous. The index is self-consistent, because a missing *prefix* keeps span equal to count unlike an interior hole, so `EntryRange` accepts it and the node starts. Reads below the real floor then return `ErrLogGapDetected` instead of `ErrCompacted`, and raft never falls back to `InstallSnapshot`. The regressed floor also defeats the monotonic guard in `RemoveEntriesTo`, so an even lower floor can afterwards be accepted and made durable.

`reconcileCompactionFloors` re-derives the floor as `firstIndex - 1` from the **rebuilt index**, never from the snapshot index. Anchoring at the snapshot would be wrong because the snapshot pool deliberately retains `CompactionOverhead` entries below it for cheap follower catch-up, so a snapshot-anchored floor would mark live, deliberately kept entries unreadable and force a full `InstallSnapshot` on every lagging follower after any restart.

The snapshot is the **validity check** on that derivation, not its source. The repair is applied, with a `WARN`, only when a recorded snapshot actually covers the missing prefix, which is the lost-marker case since compaction always follows a snapshot. Otherwise the prefix is unexplained and the derivation is skipped: an absent prefix with no snapshot is indistinguishable from a node that legitimately starts at a high index, which is exactly what `RemoveNodeData` followed by a rewrite produces, and rejecting it would refuse to start healthy replicas. Making that case fail closed needs the floor durable independently of one WAL block, which is a format change rather than a recovery heuristic.

### Post-Recovery Segment Creation

After recovery, if any segments were found, `ensureActiveSegment` is called to create a fresh active segment for new writes. `nextSegmentID` is advanced past **every observed** segment before replay, not merely past the ones that replayed successfully, so even an orphan that recovery could neither delete nor empty can never be ordered after a freshly written segment.

If the shard directory does not exist (no prior data), no directory or segment is created. The shard remains lazy until the first write.

### Crash Recovery in raftLog

The `raftLog` falls back to `logdb.EntryRange()` when the in-memory buffer is empty after a crash. This allows recovery to rebuild the tracked entry range without loading all entries into memory. The tracked range is used to initialize the LogReader's first/last index on startup.

## Filesystem Abstraction Layer

The `waldb` package defines `FS` and `File` interfaces that abstract all filesystem operations, enabling custom implementations for testing and alternative storage backends.

### FS and File Interfaces

**FS** interface:
- `OpenFile(name, flags, permissions)` — open file with specific flags
- `Open(name)` — open for reading
- `MkdirAll(path, permissions)` — create directory hierarchy
- `ReadDir(name)` — list directory contents
- `Stat(name)` — get file info
- `Remove(name)` — delete file or empty directory
- `Rename(oldpath, newpath)` — atomically replace `newpath` with `oldpath` (same directory)

`Rename` is required rather than optional because the `WALFORMAT` marker is published by writing a temp file, fsyncing it and renaming it into place — the construction that makes a partially-written marker unobservable. An optional interface with an in-place fallback would restore the torn state the rename removes, so an FS that cannot rename cannot host a WAL. Adding it is a **breaking change for out-of-tree `FS` implementations**; see [Compatibility](../operations/compatibility.md).

**File** interface:
- `Write()`, `WriteAt()`, `Read()`, `ReadAt()` — standard I/O operations. The segment writer uses `WriteAt` exclusively, so the file's implicit position is never disturbed and the block grid stays intact
- `Seek()`, `Stat()`, `Truncate()` — file positioning and sizing
- `Sync()` — flush to durable storage (fdatasync on Linux, fsync elsewhere)
- `Close()` — close file handle

**FileMetadataSyncer** optional interface:
- `SyncMetadata()` — full `fsync` including metadata. Directory syncs use this, never `Sync()`: on Linux `Sync()` is `fdatasync`, which may skip exactly the dirent changes a directory sync exists to persist

**FilePreAllocator** optional interface:
- `PreAllocate(size)` — pre-allocate disk space (fallocate on Linux, ftruncate on other platforms)

### Production Implementation

Production code uses `osFS` which wraps the `os` package. The `osFile` wrapper provides platform-optimized `Sync()` that calls `fdatasync` on Linux (data only, ~10-30% faster) and `fsync` on other platforms (data + metadata).

### Custom Implementations

Use `WithFS()` functional option to inject custom implementations:

```go
customFS := NewMyCustomFS()
db, err := Open(dir, WithFS(customFS))
```

This enables error injection testing and alternative storage strategies without modifying production code paths.

## Error Injection Testing

The `test/errfs` package provides `ErrorFS` for fault simulation. It wraps a real `FS` and injects errors (I/O failures, permission errors, etc.) on specific operations, useful for testing durability, recovery, and error handling paths.

## Source File Reference

| File                   | Purpose                                         |
|------------------------|-------------------------------------------------|
| `waldb/db.go`          | DB struct, sharding, SaveState, reads, replay, record layouts |
| `waldb/segment.go`     | Segment file I/O, block buffering, pre-alloc, `ReadAll` and its corruption classifier |
| `waldb/format.go`      | `WALFORMAT` marker: block geometry + format version |
| `waldb/repair.go`      | Operator-authorized truncation (`waldb.Repair`) |
| `waldb/record.go`      | CRC32 chunk encoding/decoding, chunk types, durability sequence, positional context-AAD |
| `waldb/index.go`       | In-memory sorted index with binary search       |
| `waldb/compaction.go`  | Segment garbage collection                      |
| `waldb/reencrypt.go`   | `Reencrypt` logical WAL migration between barriers |
| `waldb/residual.go`    | Generation-boundary residual capture and `DB.RecoveryResiduals` |
| `waldb/copy.go`        | Deep copies of Snapshot/Membership/Bootstrap on the read boundary |
| `waldb/options.go`     | Configuration defaults and validation           |
| `waldb/errors.go`      | Typed error definitions                         |
| `waldb/fs.go`          | FS and File interface definitions, osFS/osFile  |
| `waldb/sync_linux.go`  | Linux fdatasync + fallocate (used by osFile)    |
| `waldb/sync_other.go`  | Non-Linux fsync + ftruncate (used by osFile)    |
| `waldb/diskmon.go`     | Disk usage monitoring and `ErrDiskFull`         |
| `logdb/logdb.go`       | LogDB interface, optional interfaces, `EntryOverheadBytes` |
| `logdb/types.go`       | Storage-layer types (Entry, State, etc.)        |
| `logdb/errors.go`      | LogDB sentinel errors                           |
| `logdb/residual.go`    | `RecoveryResidual` and `RecoveryResidualReporter` |
| `logdb/reader.go`      | LogReader and the term ring buffer              |
| `logdb/memdb/`         | In-memory LogDB used by tests and the conformance suite |
| `engine/commit.go`     | Commit worker with group commit pattern         |
| `raft/peer.go`         | Peer.Commit() -- post-persistence bookkeeping   |
| `raft/inmemory.go`     | In-memory log buffer with savedTo/appliedTo     |

## Configuration and Operational Considerations

### Sizing Decisions

- **16 shards**: Balances parallelism (scales to 10K+ groups) against open file descriptors and directory entries
- **32 KiB blocks**: Typical SSD alignment; reduces fragmentation
- **64 MiB segments**: Balances compaction frequency against individual file management overhead

### Tuning for High-Throughput Clusters

For clusters with thousands of shards:
- Increase `MaxFileSize` to 256-512 MiB to reduce rotation frequency. Safe on a populated directory: recovery never reads it, so it only changes where future rotations land
- Leave `BlockSize` alone. It is immutable for the life of the data directory (see [Segment Format](#segment-format)); `Open` refuses a changed value rather than destroying the log
- Monitor disk space using `WithDiskMonitorOpts()`
- Tune compaction frequency based on your apply rate and snapshot intervals
- Use `WithMetrics()` to measure write/fsync latencies and identify bottlenecks

### Encryption Considerations

When using barrier encryption:
- The barrier must be initialized **and unsealed** before calling `Open`. A sealed barrier fails the open with `ErrBarrierContextUnresolved` rather than latching a downgraded binding
- DEK rotation is transparent; the epoch is embedded in the ciphertext's 8-byte big-endian prefix
- `MinLiveEpoch()` (and `MinLiveEpochForBarrier` for per-tenant deployments) prevents premature DEK purging. Treat `known == false` or any error as "do not purge"
- Check `AtRestProtection()` rather than inferring encryption from the presence of a barrier
- Changing the barrier's sealing strategy across a restart while the WAL holds records is not a supported migration. Use `Reencrypt`

### Testing and Debugging

- Use `WithNoSync(true)` only in tests, never in production. It disables every fsync and every directory sync, so every record in such a segment carries durability generation 0 and recovery is always free to truncate it. A test that needs recovery to see acknowledged data must let the syncs run
- Inject errors via custom FS implementations to test failure paths
- Monitor segment creation/deletion rates to tune compaction
- Verify `ListNodeInfo()` after crash recovery matches the expected set of shards
- Check `RecoveryResiduals()` after an open. It is empty on a clean recovery and non-empty exactly when the node may have come back shorter than the log it acknowledged

## Related Documentation

- [Wire Format](../protocol/wire-format.md) -- the canonical byte-layout spec, including `proto.Entry` and `proto.Membership`, which are separate layouts from the ones stored here
- [Encryption](../security/encryption.md) -- the barrier, epoch DEKs and sealing strategies behind `WALBarrier`
- [Compatibility and Upgrades](../operations/compatibility.md) -- the pinned-at-1 format policy and the version discriminators that do exist
- [Raft Protocol](../raft/protocol.md) -- snapshot transfer and the membership state this layer persists
- [Engine Pipeline](../engine/pipeline.md) -- where the commit worker sits relative to the step and apply workers
- [Testing Infrastructure](../testing/infrastructure.md) -- the `test/errfs` fault-injection harness that exercises these paths
