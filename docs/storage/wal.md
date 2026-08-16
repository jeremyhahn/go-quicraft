# WAL Persistence Layer

## Overview

The WAL (Write-Ahead Log) persistence layer provides durable storage for Raft log entries, hard state, snapshot metadata, and bootstrap configuration. It is implemented in `pkg/logdb/waldb/` and satisfies the `logdb.LogDB` interface defined in `pkg/logdb/logdb.go`.

Design principles:

- **Sharded writes**: 16 independent WAL directory shards eliminate fsync contention across Raft groups. Each shard has its own segment files, mutex, and in-memory state.
- **Write-ahead durability**: All state is written to the WAL before being acknowledged. On recovery, all in-memory state is rebuilt by replaying WAL segments in order.
- **In-memory reads**: Reads are served entirely from in-memory maps. The WAL exists solely for durability; it is never read during normal operation.
- **Fixed binary encoding**: All records use little-endian fixed-layout binary encoding with no external serialization dependency (no protobuf).
- **Lazy initialization**: Shard directories and segment files are created on first write, not at Open time. This avoids creating 16 directories for nodes that may only use a few shards.

## LogDB Interface

The `logdb.LogDB` interface defines the storage contract. Implementations must be safe for concurrent use from multiple goroutines.

```go
type LogDB interface {
    // Name returns the name/type of the LogDB implementation.
    Name() string

    // SaveState persists a batch of Updates atomically.
    // Each Update contains entries and/or hard state for a single shard.
    SaveState(updates []Update) error

    // IterateEntries returns log entries in the range [low, high) for the given node,
    // up to maxSize bytes total. Results are appended to the entries slice.
    IterateEntries(entries []Entry, size uint64, shardID uint64, replicaID uint64,
        low uint64, high uint64, maxSize uint64) ([]Entry, uint64, error)

    // ReadState reads the persisted hard state and membership for the given node.
    ReadState(shardID uint64, replicaID uint64) (State, Membership, error)

    // EntryRange returns the first index and total count of available log entries.
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
- `Cmd` ([]byte) - serialized command payload

`Key`, `ClientID`, `SeriesID` and `RespondedTo` **must** be persisted. A
backend that drops them silently disables session deduplication (breaking
at-most-once delivery across a restart) and downgrades sender authorization.

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
- `MaxSessionsPerClient` (uint64) - replicated cap on responses cached per session
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

    dir             string
    opts            dbOptions
    activeSegment   *segment
    activeSegmentID uint64
    nextSegmentID   uint64

    indexes       map[nodeKey]*index              // per-node entry indexes
    entries       map[nodeKey]map[uint64]logdb.Entry // per-node entry data
    states        map[nodeKey]logdb.State          // per-node hard state
    bootstraps    map[nodeKey]logdb.Bootstrap      // per-node bootstrap info
    snapshots     map[nodeKey]logdb.Snapshot       // per-node snapshot metadata
    compactedTo   map[nodeKey]uint64               // per-node compaction markers
    knownSegments map[uint64]bool                  // all segment IDs ever created
    marshalBuf    []byte                           // reusable encode buffer
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
  shard-0/
    000000.log
    000001.log
  shard-1/
    000000.log
  ...
  shard-15/
    000000.log
```

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
    recordSeq   atomic.Uint64
    closed      bool
}
```

Production defaults from `waldb/options.go`:

| Parameter     | Default          | Description                             |
|---------------|------------------|-----------------------------------------|
| `BlockSize`   | 32768 (32 KiB)   | WAL block size; must be a power of two  |
| `MaxFileSize` | 67108864 (64 MiB)| Segment rotation threshold              |

**`BlockSize` is immutable for the life of a data directory. `MaxFileSize` is freely tunable at any time.**

The asymmetry is structural, not a policy choice. `BlockSize` defines a grid that recovery must reconstruct from configuration alone, because segment files carry no header of their own — change it and the scanner misparses every boundary written under the old size. `MaxFileSize` only decides when the writer rotates to a new segment; recovery never consults it, so raising or lowering it affects future rotation and nothing already on disk. Changing `MaxFileSize` on a populated directory is safe and needs no migration.

To change `BlockSize`, start from an empty data directory (wipe and re-sync the node from its peers). There is no in-place migration.

`BlockSize` is recorded on disk in a `WALFORMAT` marker file in the WAL root directory (magic, format version, block size). `Open` compares the configured value against the marker and refuses a mismatch with `ErrWALGeometryMismatch`, which names both sizes so the fix is a configuration edit rather than a restore. `MaxFileSize` is deliberately **not** recorded: recording it would imply a guarantee about a value that carries none.

The marker is written only where this process has evidence for it:

- A directory with **no segments at all** is stamped immediately. Nothing is on disk for a wrong value to misparse, so the configured size *is* the geometry.
- A **populated** directory that predates the marker is stamped once recovery has replayed past a block boundary. "Every segment replayed cleanly" is not sufficient on its own — a directory whose records all fit inside a single block replays identically under every block size, because the scanner never strides the grid to read it. Stamping there would freeze a guess and lock out the size the directory was really written with.
- A directory with segments but no boundary crossed yet is left unmarked and gets its marker on a later open, once it has grown enough to prove its own geometry.

The marker is published atomically: the 16 bytes are written to `WALFORMAT.tmp`, fsynced, renamed onto `WALFORMAT`, and the directory entry is fsynced. A reader therefore sees either no marker or the whole verified value, never a partial one. A marker that is entirely zero bytes (including zero-length) is treated as **absent** and re-stamped on the next open — that is the residue of an interrupted or failed stamp, not a geometry, and the valid magic is non-zero so no real marker can look like it. A marker with non-zero bytes that are not our format stays fatal (`ErrWALFormatMalformed`).

Segment files are pre-allocated at creation using the `FilePreAllocator` interface. Production code uses `fallocate` on Linux or `ftruncate` on other platforms. Pre-allocation avoids filesystem metadata updates on each append. On close, the file is truncated to its actual written size to reclaim unused pre-allocated space.

### Block Structure

A segment file is logically divided into fixed-size blocks of `BlockSize` bytes. Records are written into blocks sequentially. When insufficient space remains in the current block for a chunk header plus at least 1 byte of payload (i.e., `available < recordHeaderSize + 1`), the remaining bytes are zero-padded and the block is flushed to disk.

```
Block (32768 bytes default):
+-------------------------------------------------------------------+
| Chunk 1 | Chunk 2 | ... | Chunk N | Zero Padding (if < 8 bytes)  |
+-------------------------------------------------------------------+
```

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

The CRC32 uses the Castagnoli polynomial (`crc32.Castagnoli`), which is hardware-accelerated via SSE4.2 on amd64 processors. The checksum covers the type byte, the durability sequence and the payload data (`buf[6:totalLen]`), not the CRC or length fields themselves.

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
    RecordEpoch  byte = 5  // key rotation epoch boundary (plaintext)
)
```

A logical record that fits within the remaining block space is written as a single `RecordFull` chunk. A record too large for the remaining space is split across blocks: the first piece is `RecordFirst`, intermediate pieces are `RecordMiddle`, and the final piece is `RecordLast`.

### Payload Types

Each logical WAL record begins with a 1-byte payload type that identifies its content:

```go
const (
    payloadTypeEntriesV1  byte = 0x01 // legacy entry format (44-byte header); rejected on replay
    payloadTypeState      byte = 0x02
    payloadTypeSnapshotV1 byte = 0x03 // original snapshot format; rejected on replay
    payloadTypeBootstrap  byte = 0x04
    payloadTypeCompact    byte = 0x05
    payloadTypeRemoveNode byte = 0x06
    payloadTypeEntries    byte = 0x07 // current entry format (76-byte header, session fields)
    payloadTypeSnapshotV2 byte = 0x08 // witness-policy snapshot format; rejected on replay
    payloadTypeSnapshotV3 byte = 0x09 // membership-version-byte snapshot format; rejected on replay
    payloadTypeSnapshot   byte = 0x0a // current snapshot format (adds artifact Compression)
)
```

#### Format Evolution and Fatal-on-Replay Types

The payload type byte is the WAL's only on-disk versioning mechanism. A record layout is never changed in place: a new layout gets a NEW payload type, and the retired value stays reserved forever so an old WAL is *detected* rather than misparsed. Retired types are never written again, and encountering one during replay is fatal — recovery fails the open instead of skipping or reinterpreting the record.

| Value  | Constant                | Status  | Replay behaviour                                                    |
|--------|-------------------------|---------|---------------------------------------------------------------------|
| `0x01` | `payloadTypeEntriesV1`  | retired | fatal, `ErrLegacyEntryFormat` — V1 lacks the four session fields, so replaying it would zero them and silently break at-most-once delivery |
| `0x02` | `payloadTypeState`      | current | hard state (Term/Vote/Commit)                                       |
| `0x03` | `payloadTypeSnapshotV1` | retired | fatal, `ErrLegacySnapshotFormat` — no `MaxWitnesses`/`PolicyFlags` in the membership section and no version discriminator of its own |
| `0x04` | `payloadTypeBootstrap`  | current | bootstrap record                                                    |
| `0x05` | `payloadTypeCompact`    | current | compaction floor marker                                             |
| `0x06` | `payloadTypeRemoveNode` | current | remove-node tombstone                                               |
| `0x07` | `payloadTypeEntries`    | current | entry batch, 76-byte per-entry header                               |
| `0x08` | `payloadTypeSnapshotV2` | retired | fatal, `ErrLegacySnapshotFormatV2` — its membership section starts with `ConfigChangeID`, so the current parser would consume that field's low byte as a version byte and shift every subsequent read |
| `0x09` | `payloadTypeSnapshotV3` | retired | fatal, `ErrLegacySnapshotFormatV3` — its `FilePathLen` sits where the current layout reads `Compression`, so the record would be sliced at the wrong offset |
| `0x0a` | `payloadTypeSnapshot`   | current | snapshot metadata, including the artifact `Compression` field       |

Any type byte outside this set is `ErrUnknownPayloadType`, which is also fatal. That case matters most for a KEYED WAL opened with `barrier == nil`: every ciphertext record's leading epoch byte decodes as type `0x00`, and skipping those records would boot the node as an empty replica with no memory of prior votes.

### Record Encoding Layouts

**Entry batch** (`payloadTypeEntries`, repeating):

```
[PayloadType:1][ShardID:8][ReplicaID:8][Index:8][Term:8][Type:8]
               [Key:8][ClientID:8][SeriesID:8][RespondedTo:8][CmdLen:4][Cmd:N]...
                |<------------------ 76 bytes per entry header ------------------>|
```

`entryRecordHeaderSize = 76` bytes per entry (8×9 + 4). The four session
fields (`Key`, `ClientID`, `SeriesID`, `RespondedTo`) are persisted so
at-most-once delivery survives crash recovery; the legacy 44-byte format
(`payloadTypeEntriesV1`) is gated and fails recovery with
`ErrLegacyEntryFormat` rather than being misparsed.

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
`Compression` is a 2-byte `config.CompressionType`.

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
[NumRemovedIdentities:4]{[ReplicaID:8][IdLen:4][Id:N]...}
```

Fixed region: `membershipFixedSize = 18` bytes (1+8+8+1). The smallest
section this binary will **replay** is `minMembershipSize = 66` bytes -- the
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
```

**Compaction marker** (`payloadTypeCompact`):

```
[PayloadType:1][ShardID:8][ReplicaID:8][CompactedTo:8]
                |<---------- 24 bytes ---------->|
```

## Write Path

The write path flows from `SaveState` through block buffering to durable storage.

### SaveState Entry Point

`SaveState` iterates over a batch of `logdb.Update` values. Each update targets a specific shard determined by `shardID % 16`. The shard mutex is held for the duration of each individual update:

```go
func (db *DB) SaveState(updates []logdb.Update) error {
    for i := range updates {
        u := &updates[i]
        shard := db.getShard(u.ShardID)
        shard.mu.Lock()
        err := shard.saveUpdate(u)
        shard.mu.Unlock()
        if err != nil {
            return err
        }
    }
    return nil
}
```

### Per-Update Processing

`saveUpdate` writes three KINDS of WAL record per update — entries, hard state, and snapshot metadata — each only when the corresponding field is non-zero:

The entry payload may span **several** records. A single record must fit inside
one segment, and its usable payload is strictly smaller than the segment size —
a record spends `recordHeaderSize` bytes in every block it spans — while the WAL
also adds a 76-byte header per entry that Raft's batch cap does not account for.
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
func (s *walShard) saveUpdate(u *logdb.Update) error {
    key := nodeKey{ShardID: u.ShardID, ReplicaID: u.ReplicaID}

    if len(u.Entries) > 0 {
        if err := s.writeEntries(key, u.ShardID, u.ReplicaID, u.Entries); err != nil {
            return err
        }
    }
    if u.State.Term != 0 || u.State.Vote != 0 || u.State.Commit != 0 {
        if err := s.writeState(key, u.ShardID, u.ReplicaID, u.State); err != nil {
            return err
        }
    }
    if u.Snapshot.Index != 0 || u.Snapshot.Term != 0 {
        if err := s.writeSnapshot(key, u.ShardID, u.ReplicaID, u.Snapshot); err != nil {
            return err
        }
    }
    return nil
}
```

### Block Buffering and Sync

The `writeRecord` method is the lowest-level write path. It ensures the active segment exists (creating the shard directory and segment file on first write for lazy shards), writes the record to the block buffer, handles segment rotation on `ErrSegmentFull`, and calls `Sync()` for durability:

```go
func (s *walShard) writeRecord(data []byte) error {
    if err := s.ensureActiveSegment(); err != nil {
        return err
    }
    _, err := s.activeSegment.Write(data)
    if err == ErrSegmentFull {
        if err := s.rotateSegment(); err != nil {
            return err
        }
        _, err = s.activeSegment.Write(data)
    }
    if err != nil {
        return err
    }
    if !s.opts.noSync {
        return s.activeSegment.Sync()
    }
    return nil
}
```

`segment.Sync()` first flushes the open block's buffered data to the OS page cache, then calls `fdatasync` (Linux) or `f.Sync` (other platforms):

```go
func (s *segment) Sync() error {
    if err := s.flushBlock(); err != nil {
        return err
    }
    if s.noSync {
        return nil
    }
    return fdatasync(s.f)
}
```

`flushBlock` writes only the bytes added since the previous flush — `blockBuf[flushed:blockOffset]` at `fileOffset+flushed` — and advances the `flushed` watermark. It never rewrites the part of the open block an earlier `fdatasync` already made durable. That is a durability property first: re-writing an acknowledged record (a `HardState` carrying a vote grant, for example) inside a later batch's write puts it back at risk of a power loss that has no right to touch it. It also removes a quadratic write amplification, since filling one block over *n* syncs would otherwise write `~n²·recSize/2` bytes; with the default 32 KiB block and 48-byte records, persisting 110 KiB of log wrote 30.6 MB before the watermark and 110 KiB after.

Block-grid alignment is unaffected: the target offset stays inside the block that starts at `fileOffset`, and `fileOffset` is advanced only by `padAndFlushBlock`, by exactly one `blockSize`, after the block has been completed on disk. `DiscardPending` rewinds `flushed` to the checkpoint's block offset along with the offsets it restores.

On Linux, `fdatasync` (`syscall.Fdatasync`) is used instead of `fsync` because the file size does not change within pre-allocated space, saving ~10-30% on sync latency by skipping metadata updates. Directory fsync is performed once after creating a new segment to ensure segment visibility after crash recovery.

### Write Sequence Diagram

```
SaveState([]Update)
  |
  +-- for each Update:
  |     getShard(shardID % 16)
  |     shard.mu.Lock()
  |     |
  |     +-- writeEntries() --> serialize to marshalBuf
  |     |     +-- encodeRecord() into blockBuf (may split across blocks)
  |     |     +-- segment.Sync() --> flushBlock() + fdatasync()
  |     |
  |     +-- writeState() --> serialize to marshalBuf
  |     |     +-- encodeRecord() into blockBuf
  |     |     +-- segment.Sync() --> flushBlock() + fdatasync()
  |     |
  |     +-- writeSnapshot() --> serialize to marshalBuf
  |     |     +-- encodeRecord() into blockBuf
  |     |     +-- segment.Sync() --> flushBlock() + fdatasync()
  |     |
  |     shard.mu.Unlock()
```

Each `writeRecord` call within a single `saveUpdate` triggers its own `fdatasync`. This means a single update with entries + state + snapshot can perform up to three syncs. This is a correctness-over-throughput tradeoff for the single-update case; batching across multiple updates amortizes the cost via group commit.

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
    Cmd:         cmdCopy,
})
```

Every field is carried, the four session fields included. Dropping them here
is not a lossy optimization — it silently disables session deduplication
across a restart.

## Group Commit Optimization

The commit worker in `pkg/internal/engine/commit.go` uses a **drain-then-batch** pattern to maximize WAL write throughput. This enables group commit: multiple updates arriving while the previous SaveState + fdatasync are in progress are automatically batched into a single SaveState call.

```go
func (w *commitWorker) run() {
    for {
        select {
        case <-w.stopC:
            return
        case item := <-w.commitC:
            w.batch = append(w.batch[:0], item.update)
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

### In-Memory Index

Each (shardID, replicaID) pair has its own `index` that tracks entry locations:

```go
type indexEntry struct {
    Index     uint64  // Raft log index
    Term      uint64  // Raft term
    SegmentID uint64  // which segment file contains this entry
    Offset    int64   // byte offset within segment (unused for reads)
    Size      int64   // encoded record size in bytes
}

type index struct {
    mu      sync.RWMutex
    entries []indexEntry  // sorted by Index, ascending
}
```

The index provides O(log n) lookup and range queries via `sort.Search`:

- `Lookup(raftIndex)` -- binary search for a single index
- `Range(low, high)` -- returns all entries in [low, high), returns a copy
- `Compact(upTo)` -- removes entries with Index <= upTo
- `AllSegmentIDs()` -- returns the set of segment IDs referenced by any entry
- `Append(entries...)` -- handles both fast-path append and overwrite-truncate

The `Append` method handles Raft leader changes where a new leader overwrites uncommitted entries. If the new entry's index is <= the tail, all entries from that position are truncated before appending.

### Entry Retrieval

`IterateEntries` and `GetEntries` both use the same pattern: look up the index range, then fetch entry data from the in-memory `entries` map:

```go
func (db *DB) IterateEntries(
    entries []logdb.Entry, size uint64,
    shardID, replicaID, low, high, maxSize uint64,
) ([]logdb.Entry, uint64, error) {
    shard := db.getShard(shardID)
    shard.mu.Lock()
    defer shard.mu.Unlock()

    idx := shard.indexes[key]
    rangeEntries := idx.Range(low, high)

    for _, ie := range rangeEntries {
        if maxSize > 0 && totalSize > 0 && totalSize >= maxSize {
            break
        }
        if e, found := nodeEntries[ie.Index]; found {
            entries = append(entries, e)
            totalSize += uint64(ie.Size)
        }
    }
    return entries, totalSize, nil
}
```

The `maxSize` parameter limits the total bytes returned. At least one entry is always returned if `totalSize` starts at 0 (the size check requires `totalSize > 0` before enforcing the limit).

Compaction is checked before iteration: if `low <= compactedTo`, `logdb.ErrCompacted` is returned.

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

When a `WALBarrier` is configured via `WithBarrier()`, records are encrypted before writing and decrypted during recovery. The barrier must be initialized and unsealed before any WAL operations. Encrypted records embed an 8-byte epoch prefix in the ciphertext to enable epoch extraction without decryption, supporting key rotation without re-encrypting the entire WAL.

## WAL Re-encryption Primitive

**Source:** `pkg/logdb/waldb/reencrypt.go`

The `Reencrypt` function performs logical re-encryption of an entire WAL from one barrier to another:

```go
func Reencrypt(srcDir string, srcBarrier WALBarrier, dstDir string, dstBarrier WALBarrier) (int, error)
```

This primitive enables zero-downtime key migration for scenarios like enabling encryption on a previously-unencrypted WAL or rotating a WAL's encryption key.

### How It Works

`Reencrypt` performs a logical copy through the public LogDB surface:

1. Open source LogDB at `srcDir` with `srcBarrier` (decryption)
2. Open destination LogDB at `dstDir` with `dstBarrier` (encryption)
3. For each node (shard/replica pair):
   - Copy bootstrap record (if present)
   - Copy latest snapshot metadata (membership, index, term)
   - Copy hard state and all live log entries

Because the source LogDB decrypts each record under `srcBarrier` and the destination encrypts under `dstBarrier`, the result is byte-for-byte identical at the Raft level while the at-rest encryption changes.

### Barrier Flexibility

Either barrier may be `nil` for a plaintext side:

- `srcBarrier = nil, dstBarrier = *Barrier`: encrypt a plaintext WAL (e.g., none→keyed migration)
- `srcBarrier = *Barrier, dstBarrier = nil`: decrypt an encrypted WAL
- Both non-nil: re-encrypt with a different key

### Error Handling

`Reencrypt` wraps errors in `ReencryptError` with an operation name to identify the failure point (open-source, open-dest, list-nodes, get-bootstrap, get-snapshot, get-state, iterate-entries, save-*). This allows operators to see exactly where a migration stopped.

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
  +-- appliedTo(lastProcessed)          // advance processed marker
  +-- processConfigChanges()            // clear pendingConfigChange flag
  +-- logReader.Append(EntriesToSave)   // populate termRingBuffer (O(1) append)
  +-- raftLog.commitUpdate(uc)          // advance savedTo, appliedLogTo
```

`logReader.Append` populates the LogReader's `termRingBuffer`, a fixed-capacity circular buffer of (index, term) pairs that provides O(log n) term lookups via binary search. This replaces the prior `map[uint64]uint64` term cache, which caused O(n) iteration on every trim and dominated CPU under concurrent load. The ring buffer uses a 1.6 MB flat slice with zero GC pressure.

### In-Memory Log Lifecycle

The `inMemory` struct (`pkg/internal/raft/inmemory.go`) tracks three markers:

- `savedTo` -- highest index persisted to LogDB
- `appliedTo` -- highest index applied to state machine
- `markerIndex` -- index of first entry still in memory

`entriesToSave()` returns entries with `Index > savedTo`. After `Peer.Commit` calls `savedLogTo`, those entries are marked as persisted. After `appliedLogTo`, entries at or below the applied index are truncated from the front of the in-memory slice, freeing memory.

`appliedLogTo` uses lazy compaction via a `wastedPrefix` counter that tracks logically truncated entries without immediate reallocation. The backing slice is only reallocated when wasted space exceeds live entries or reaches 512 entries. This reduces allocation frequency by approximately 128x under steady load compared to allocating a fresh buffer on every apply.

## Durability Guarantees

Data becomes durable at a precise point in the write path: when `fdatasync` returns successfully inside `writeRecord`. Here is the crash analysis for each stage:

| Stage                         | Crash Behavior                              |
|-------------------------------|---------------------------------------------|
| Record serialized to marshalBuf | Lost. Only in process memory.             |
| encodeRecord into blockBuf    | Lost. Block not yet written to OS.          |
| f.Write (blockBuf to OS)      | In OS page cache. Lost on power failure.    |
| fdatasync returns             | Durable. Survives power failure. Closes a durability generation: the next record is stamped with a higher sequence, which is what proves on recovery that everything below it was acknowledged. |
| In-memory maps updated        | Redundant. Rebuilt from WAL on recovery.    |

The `noSync` option (`WithNoSync(true)`) disables `fdatasync`, leaving data in the OS page cache. This is safe only for testing. In production, every `writeRecord` call performs `flushBlock()` followed by `fdatasync()`.

Note: The in-memory maps (entries, states, etc.) are updated in the same `saveUpdate` call as the WAL write. If the process crashes after `fdatasync` but before the in-memory update completes, the data is still durable on disk and will be restored on recovery. The in-memory state is purely a read cache and is never the source of truth.

### Disk Full Detection

The WAL supports disk full detection via `WithDiskMonitorOpts()`. When `maxBytes > 0`, a DiskMonitor is created to track available disk space. WriteRecord fails with `ErrDiskFull` when remaining space drops below the threshold. Recovery handles disk full gracefully: it detects partial writes at segment boundaries and continues replaying valid records.

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
    // Pin segments holding the latest State/Snapshot/Bootstrap records.
    shard.addMetaSegmentsToLive(liveSegments)

    deleted, err := comp.GarbageCollect(liveSegments, shard.activeSegmentID)
    if deleted > 0 {
        syncDirFS(shard.fs, shard.dir)  // make deletions crash-durable
    }
    return err
}
```

`GarbageCollect` scans the shard directory, identifies segment files not referenced by any node's index and with an ID strictly less than the active segment ID, and deletes them:

```go
func (c *compactor) GarbageCollect(liveSegments map[uint64]bool, maxLiveID uint64) (int, error) {
    for _, de := range dirEntries {
        segID, ok := parseSegmentFilename(de.Name())
        if !ok { continue }
        if segID >= maxLiveID { continue }   // never delete active segment
        if liveSegments[segID] { continue }  // still referenced
        os.Remove(path)                      // safe to delete
        deleted++
    }
    return deleted, nil
}
```

Safety invariant: The active segment (`activeSegmentID`) is never deleted, even if it contains no referenced entries. This prevents a race where in-flight writes target a deleted file.

#### Pinning Meta-Record Segments

The live-segment set is **not** computed from entry indexes alone. State, snapshot, and bootstrap records are never tracked by any entry index, so a segment whose entries have all been compacted away can still hold the sole durable copy of a node's hard state, snapshot metadata, or bootstrap configuration. Computing liveness only from `idx.AllSegmentIDs()` would let GC delete such a segment, losing that record.

To prevent this, each `walShard` tracks, per node, the segment ID that holds the latest durable State, latest Snapshot, and Bootstrap record:

```go
type metaSegments struct {
    state        uint64
    snapshot     uint64
    bootstrap    uint64
    hasState     bool   // segment IDs are 0-based, so presence needs a flag
    hasSnapshot  bool
    hasBootstrap bool
}

// walShard field
metaSegments map[nodeKey]metaSegments
```

Each record type carries an explicit presence flag rather than treating a zero ID as "none", because segment IDs are 0-based: segment 0 is a valid pin and must be distinguishable from the absence of a record.

These segment IDs are recorded in two places, both following last-writer-wins semantics:

- **On write-land**: `writeState`, `writeSnapshot`, and `writeBootstrap` call `setStateSegment` / `setSnapshotSegment` / `setBootstrapSegment` with the segment the record actually landed in (`s.activeSegmentID` after `writeRecord`, which accounts for any mid-write segment rotation). For batched state/snapshot writes the pin is applied at the commit point via `pendingMemUpdates.applyTo`, using the captured `landedSegID`.
- **During recovery replay**: `replayState`, `replaySnapshot`, and `replayBootstrap` call the same setters with the replayed segment ID. Because segments replay in ascending ID order, the last replayed record wins, matching last-writer-wins at write time.

Before running `GarbageCollect`, both `Compact` and `RemoveNodeData` fold these meta-segments into the live set via `addMetaSegmentsToLive`:

```go
func (s *walShard) addMetaSegmentsToLive(liveSegments map[uint64]bool) {
    for _, m := range s.metaSegments {
        if m.hasState     { liveSegments[m.state] = true }
        if m.hasSnapshot  { liveSegments[m.snapshot] = true }
        if m.hasBootstrap { liveSegments[m.bootstrap] = true }
    }
}
```

This guarantees that the segment holding a live node's latest State, Snapshot, or Bootstrap record is never garbage-collected. When a node is removed (`RemoveNodeData`) or replayed past (`replayRemoveNode`), its `metaSegments` entry is dropped, so its now-obsolete meta-segments become eligible for reclamation if no surviving node references them.

#### Deletion Durability

When `GarbageCollect` deletes one or more segment files, both `Compact` and `RemoveNodeData` fsync the shard directory (`syncDirFS`) afterward. Without this, a metadata-lazy filesystem (e.g., ext4 in default ordered mode) could lose the directory-entry removals on a crash and resurrect a deleted segment, replaying records that were supposed to be gone. The directory fsync is best-effort: a failure is logged but not fatal, because the next compaction retries the deletions and the in-memory index already excludes the deleted segments. `RemoveNodeData` previously already performed this fsync; `Compact` now does so as well whenever it deletes a segment.

## Recovery

On `Open`, each shard directory is checked for existence. If it exists, `recover()` is called to rebuild all in-memory state from WAL segments.

### Recovery Sequence

```
Open(dir)
  |
  +-- for each shard (0..15):
        openWALShard(shardDir)
          |
          +-- os.Stat(dir) exists?
          |     yes --> recover()
          |             +-- os.ReadDir() to find *.log files
          |             +-- sort segment IDs ascending
          |             +-- for each segment in order:
          |                   replaySegment(segID)
          |                     +-- os.Open (read-only)
          |                     +-- segment.ReadAll()
          |                     +-- for each record:
          |                           replayRecord(rec, segID)
          |                     +-- f.Close()
          |             +-- ensureActiveSegment() (if segments exist)
          |
          |     no  --> lazy init (no directory created yet)
```

### ReadAll: Block-by-Block Scanning

`segment.ReadAll()` reads the segment file block by block. For each block, it decodes chunks sequentially, reassembling multi-chunk records:

1. Read `blockSize` bytes at the current offset
2. For each position in the block:
   - If the byte is zero and all remaining bytes are zero, this is EOF padding
   - If fewer than `recordHeaderSize` (11) bytes remain, skip to next block
   - Call `decodeRecord` to parse CRC, length, type, durability sequence, and payload
   - If CRC verification fails, discard any partial multi-chunk record and skip to the next block boundary
3. Reassemble multi-chunk records: `RecordFirst` starts accumulation, `RecordMiddle` appends, `RecordLast` completes and emits
4. `RecordFull` and `RecordEpoch` are emitted as complete records immediately

### Replay Dispatch

Each recovered record is dispatched by its 1-byte payload type prefix:

```go
func (s *walShard) replayRecord(rec []byte, segID uint64) {
    payloadType := rec[0]
    data := rec[1:]

    switch payloadType {
    case payloadTypeEntries:   s.replayEntries(data, segID)
    case payloadTypeState:     s.replayState(data)
    case payloadTypeSnapshot:  s.replaySnapshot(data)
    case payloadTypeBootstrap: s.replayBootstrap(data)
    case payloadTypeCompact:   s.replayCompaction(data)
    }
}
```

Replay builds the same in-memory state that normal writes would:
- Entries are added to both the index and the entries map
- States, snapshots, and bootstraps replace any previously replayed value
- Compaction markers remove entries from the index and entries map

### Handling Partial and Corrupted Records

**A WAL is valid only as a prefix.** Nothing after the first damaged byte is ever replayed, whatever it decodes to: resurrecting a record from beyond a gap would create a hole in the Raft log and violate Log Matching. The clean prefix collected before the damage is final.

- **Partial records**: If `ReadAll` encounters a multi-chunk record where `RecordFirst` was read but `RecordLast` was not (e.g., crash during write), the partial record is discarded.
- **CRC failures**: `decodeRecord` returns `ErrCorruptedRecord`. The scan stops collecting records there and switches to classifying the damage (below). It does **not** resynchronize and resume.
- **Zero-filled blocks**: Pre-allocated but unwritten blocks are detected by checking for all-zero bytes. A zero region is normal EOF padding only when nothing follows it; a zero region with data after it is a **write hole** (a crash lost block N while block N+1 reached the medium) and recovery stops at the hole.
- **Individual record replay errors**: `replayRecord` reports a failure to decode a record that already passed its CRC as a fatal recovery error. It is not skipped: a dropped `State` record would let an older one win last-writer-wins (a term/vote regression, i.e. voting twice in one term) and a dropped `Snapshot` record would boot the node with empty membership.

### Classifying Corruption: Interrupted Write vs Damage to Acknowledged Data

Finding damage is not the same as knowing what to do about it. Truncating to the clean prefix is correct for an **interrupted write** — one the medium never finished, whose records nothing was ever told about — and destroys acknowledged data when the records above the damage had already been fsynced.

The framing alone cannot tell these apart, and that is not a theoretical gap. A batch larger than one block issues one `WriteAt` per block before a single `fdatasync`, so writeback may persist an arbitrary subset of those pages. Losing an interior or leading page of block N while block N+1 survives leaves intact, CRC-passing records sitting above a CRC failure — byte-for-byte what bit rot in the middle of a live segment leaves. Recovery used to treat "something decodes above the damage" as proof of the second case and refuse to start, which made a routine power loss a permanent refuse-to-start.

The [durability sequence](#the-durability-sequence) in each chunk header settles it. After the first decode failure the scan keeps going, collecting the generation of every chunk that still decodes above the damage:

1. It probes forward **within the damaged block**, at byte granularity, resynchronizing on each chunk's own declared length. Each candidate is validated by its own CRC, so a hit is evidence rather than a guess. This matters most in the last block holding data, where there are no later blocks to consult.
2. It probes each **subsequent block** the same way, and stops early as soon as the generations settle the question.

Nothing found by either probe is ever replayed. The only output is the classification:

| Finding above the damage | Meaning | Action |
|---|---|---|
| Nothing decodes | Interrupted write, classic torn tail | Truncate to the clean prefix, fsync, delete higher segments, continue |
| Everything that decodes carries the segment's single highest generation | Interrupted write — no completed `fdatasync` sits above the damage, so nothing there was acknowledged | Truncate to the clean prefix, fsync, delete higher segments, continue |
| The records span more than one generation | A `Sync` completed between them, so the lower ones were acknowledged (`ErrMidSegmentCorruption`) | **Refuse to open.** The file is left byte-for-byte intact |
| A record carries a generation *lower* than one already replayed from the clean prefix | Impossible for this writer — the counter only rises. A misdirected write, a partial restore of an older file, or tampering (`ErrRecordSequenceRegression`) | **Refuse to open.** The file is left byte-for-byte intact |
| Damage in a segment that is **not** the highest-numbered one | Acknowledged by construction — a lower segment was fully fdatasynced before any higher one was created (`ErrNonFinalSegmentCorruption`) | **Refuse to open.** The file is left byte-for-byte intact |

**What this changed.** The torn multi-block write above is now repaired automatically. It was the reachable-from-a-routine-crash case, and the one for which a single-node deployment had no good exit.

**Residuals, stated plainly.** The generation proves *when a completed fdatasync separated two records*, and nothing else. Three cases remain:

- **Rot inside the final, in-flight batch** is truncated rather than refused. Reaching it needs rot AND a crash before anything else was written, since any later batch would raise the segment's generation and expose the damage as interior.
- **A generation boundary inside the damaged region.** When the surviving records carry a generation *higher* than the clean prefix's, a completed `fdatasync` happened somewhere in the destroyed bytes, and any older-generation record the damage swallowed had been acknowledged — whether one existed is precisely what the destroyed bytes would have said. Recovery truncates anyway, because a crash cannot produce this shape from acknowledged bytes: a partial write to a page that already holds fsynced data leaves that page at its previously persisted content if the writeback is lost, so an interrupted write's damage begins at or above the acknowledged end. Refusing would instead refuse the ordinary case where the lost page starts exactly at a batch boundary — the common shape of the failure this scheme exists to repair. Media damage that straddles the boundary is the residual, and it logs a named `WARN` telling the operator to reconcile the node against its peers rather than passing silently.
- **A zero-filled hole with data after it** is classified by the SAME generation evidence as a failed CRC. The two classifiers share one verdict: `ReadAll` records the hole as the corruption point and lets the post-corruption sweep gather the durability generations of whatever still decodes above it, exactly as it does for a checksum failure (`segment.go`, "Data after a zero hole"). A crash cannot produce a hole under acknowledged data in the first place — writes are strictly ascending in file offset, so a block lying below a later generation was covered by that generation's `fdatasync` and its pages cannot revert to zeros — so a hole whose survivors span a completed `fdatasync` is media damage or tampering, and gets the fail-closed answer the CRC path gives.
- **`noSync` mode** (tests only) issues no `fdatasync`, so every record in such a segment carries generation 0 and truncation is always permitted. That is correct rather than a hole — nothing in a segment that was never synced was ever acknowledged — but it means a test that wants recovery to see acknowledged data must let the syncs run.

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
| `ErrBarrierContextBindingUnreportable` | The configured barrier lacks `ContextBindingResolved()` | Out-of-tree barrier; add the method. See [Compatibility](../operations/compatibility.md#breaking-for-embedders-unreleased-line) |
| `ErrBarrierContextUnsupported` | The configured barrier cannot bind caller context at all | Configure a context-capable barrier. There is no epoch-only fallback by design |
| `ErrBarrierContextBindingChanged` | A shard latched "cannot bind" and the barrier now reports it can | The barrier was sealed and unsealed under a different strategy. Restart the node |

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
- No barrier is required: the scan works at the CRC framing level and never decrypts, so a sealed or unavailable barrier does not stand between an operator and a bootable node.

**What repair costs.** Every record above `TruncateAt` is destroyed, including the newest `HardState` in that shard. The node's most recent writes are gone; on a replicated shard it catches up from peers, and on a single-node deployment the loss is final. The call logs at `ERROR` before it acts and at `WARN` after, naming the offset, the bytes discarded and the segments deleted.

### Post-Recovery Segment Creation

After recovery, if any segments were found, `ensureActiveSegment` is called to create a fresh active segment for new writes. The `nextSegmentID` is set to one past the highest recovered segment ID, ensuring monotonically increasing IDs.

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

`Rename` is required rather than optional because the `WALFORMAT` marker is published by writing a temp file, fsyncing it and renaming it into place — the construction that makes a partially-written marker unobservable. An optional interface with an in-place fallback would restore the torn state the rename removes, so an FS that cannot rename cannot host a WAL. Adding it is a **breaking change for out-of-tree `FS` implementations**; see [Compatibility](../operations/compatibility.md#breaking-for-embedders-unreleased-line).

**File** interface:
- `Write()`, `Read()`, `ReadAt()` — standard I/O operations
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
| `waldb/db.go`          | DB struct, sharding, SaveState, reads, recovery |
| `waldb/segment.go`     | Segment file I/O, block buffering, pre-alloc    |
| `waldb/format.go`      | `WALFORMAT` marker: block geometry + format version |
| `waldb/repair.go`      | Operator-authorized truncation (`waldb.Repair`) |
| `waldb/record.go`      | CRC32 record encoding/decoding, chunk types, durability sequence |
| `waldb/index.go`       | In-memory sorted index with binary search       |
| `waldb/compaction.go`  | Segment garbage collection                      |
| `waldb/options.go`     | Configuration defaults and validation           |
| `waldb/errors.go`      | Typed error definitions                         |
| `waldb/fs.go`          | FS and File interface definitions, osFS/osFile  |
| `waldb/sync_linux.go`  | Linux fdatasync + fallocate (used by osFile)    |
| `waldb/sync_other.go`  | Non-Linux fsync + ftruncate (used by osFile)    |
| `waldb/diskmon.go`     | Disk full detection and monitoring              |
| `logdb/logdb.go`       | LogDB interface definition                      |
| `logdb/types.go`       | Storage-layer types (Entry, State, etc.)        |
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
- The barrier must be initialized before calling Open
- DEK rotation is transparent; the epoch is embedded in ciphertext
- `MinLiveEpoch()` prevents premature DEK purging during recovery
- Encrypted records are fully durable; plaintext-only snapshots are allowed

### Testing and Debugging

- Use `WithNoSync(true)` only in tests; never in production
- Inject errors via custom FS implementations to test failure paths
- Monitor segment creation/deletion rates to tune compaction
- Verify `ListNodeInfo()` after crash recovery matches the expected set of shards
