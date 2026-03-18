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

**Entry** - represents a single Raft log entry:
- `Index` (uint64) - log position
- `Term` (uint64) - Raft term when created
- `Type` (uint64) - entry type (normal, config change, etc.)
- `Cmd` ([]byte) - serialized command payload

**State** - persisted Raft hard state:
- `Term` (uint64) - current Raft term
- `Vote` (uint64) - candidate voted for in current term
- `Commit` (uint64) - highest log index known to be committed

**Membership** - Raft cluster membership configuration:
- `ConfigChangeID` (uint64) - log index of the configuration change
- `Addresses` (map[uint64]string) - full voting member addresses
- `Observers` (map[uint64]string) - non-voting observer addresses
- `Witnesses` (map[uint64]string) - witness-only member addresses
- `Removed` (map[uint64]bool) - removed replica IDs

**Snapshot** - persisted snapshot metadata:
- `Index` (uint64) - last included log index
- `Term` (uint64) - term of last included log entry
- `Membership` (Membership) - cluster membership at snapshot index
- `Filepath` (string) - path to snapshot data file
- `FileSize` (uint64) - size of snapshot data file in bytes
- `OnDiskIndex` (uint64) - index tracked for on-disk state machines
- `Epoch` (uint64) - barrier encryption epoch (0 for plaintext)

**Bootstrap** - initial cluster configuration:
- `Addresses` (map[uint64]string) - initial replica addresses
- `Join` (bool) - whether joining existing cluster
- `Type` (uint64) - state machine type

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

Each shard is an independent unit with its own mutex, segment files, and in-memory state maps:

```go
type walShard struct {
    mu sync.Mutex

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
    f           *os.File
    id          uint64
    dir         string
    blockSize   int
    blockBuf    []byte       // pre-allocated block buffer
    blockOffset int          // write position within current block
    fileOffset  int64        // byte offset within the file
    maxFileSize int64
    noSync      bool
    recordSeq   atomic.Uint32
    closed      bool
}
```

Production defaults from `waldb/options.go`:

| Parameter     | Default          | Description                             |
|---------------|------------------|-----------------------------------------|
| `BlockSize`   | 32768 (32 KiB)   | WAL block size; must be a power of two  |
| `MaxFileSize` | 67108864 (64 MiB)| Segment rotation threshold              |

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
Chunk Header (7 bytes):
+----------+---------+------+-----------------+
| CRC32: 4 | Len:  2 | Type | Payload: N      |
+----------+---------+------+-----------------+
```

From `waldb/record.go`:

```go
const recordHeaderSize = 7 // [CRC32:4][Length:2][Type:1]
const maxRecordDataLen = (1 << 16) - 1 // 65535 bytes
```

The CRC32 uses the Castagnoli polynomial (`crc32.Castagnoli`), which is hardware-accelerated via SSE4.2 on amd64 processors. The checksum covers the type byte and payload data (`buf[6:totalLen]`), not the CRC or length fields themselves.

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
    payloadTypeEntries   byte = 0x01
    payloadTypeState     byte = 0x02
    payloadTypeSnapshot  byte = 0x03
    payloadTypeBootstrap byte = 0x04
    payloadTypeCompact   byte = 0x05
)
```

### Record Encoding Layouts

**Entry batch** (`payloadTypeEntries`, repeating):

```
[PayloadType:1][ShardID:8][ReplicaID:8][Index:8][Term:8][Type:8][CmdLen:4][Cmd:N]...
                |<------- 44 bytes per entry header -------->|
```

`entryRecordHeaderSize = 44` bytes per entry (8+8+8+8+8+4).

**Hard state** (`payloadTypeState`):

```
[PayloadType:1][ShardID:8][ReplicaID:8][Term:8][Vote:8][Commit:8]
                |<------------ 40 bytes ------------>|
```

**Snapshot metadata** (`payloadTypeSnapshot`):

```
[PayloadType:1][ShardID:8][ReplicaID:8][Index:8][Term:8][FileSize:8][OnDiskIndex:8][Epoch:8][FilePathLen:4][FilePath:N][Membership...]
                |<----------------- 60 bytes base ------------------>|
```

The base size is 60 bytes (1+8+8+8+8+8+8+4). The `Epoch` field stores the encryption key rotation epoch associated with the snapshot.

Membership is serialized after the filepath as a variable-length structure:

```
[ConfigChangeID:8][NumAddresses:4]{[ReplicaID:8][AddrLen:4][Addr:N]...}[NumObservers:4]{...}[NumWitnesses:4]{...}[NumRemoved:4]{[ReplicaID:8]...}
```

The snapshot record structure was hardened in 2026-02-19 to persist full Membership data (previously only Epoch was added). This ensures cluster membership is recoverable from the latest snapshot even if the log is completely replayed. `ReadState(shardID, replicaID)` returns the hard state and the `Membership` from the latest stored snapshot. If no snapshot has been saved, the returned Membership is an empty `logdb.Membership{}`.

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

`saveUpdate` writes up to three WAL records per update: entries, hard state, and snapshot metadata. Each record type is only written when the corresponding field is non-zero:

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

`segment.Sync()` first flushes any buffered data from `blockBuf` to the OS page cache via `f.Write`, then calls `fdatasync` (Linux) or `f.Sync` (other platforms):

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
nodeEntries[e.Index] = logdb.Entry{
    Index: e.Index, Term: e.Term, Type: e.Type, Cmd: cmdCopy,
}
```

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
| fdatasync returns             | Durable. Survives power failure.            |
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
func (db *DB) Compact(shardID, replicaID uint64) error {
    shard := db.getShard(shardID)
    shard.mu.Lock()
    defer shard.mu.Unlock()

    comp := newCompactor(shard.dir, shard.opts.blockSize)
    liveSegments := make(map[uint64]bool)
    for _, idx := range shard.indexes {
        for segID := range idx.AllSegmentIDs() {
            liveSegments[segID] = true
        }
    }
    _, err := comp.GarbageCollect(liveSegments, shard.activeSegmentID)
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
   - If fewer than `recordHeaderSize` (7) bytes remain, skip to next block
   - Call `decodeRecord` to parse CRC, length, type, and payload
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

- **Partial records**: If `ReadAll` encounters a multi-chunk record where `RecordFirst` was read but `RecordLast` was not (e.g., crash during write), the partial record is discarded.
- **CRC failures**: `decodeRecord` returns `ErrCorruptedRecord`. The scanner discards any in-progress multi-chunk record and skips to the next block boundary, attempting recovery of subsequent records.
- **Zero-filled blocks**: Pre-allocated but unwritten blocks are detected by checking for all-zero bytes, which is treated as normal EOF.
- **Individual record replay errors**: `replayRecord` silently skips records that cannot be decoded (e.g., truncated header, too-short payload). This is non-fatal to allow recovery to proceed past isolated corruption.

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

**File** interface:
- `Write()`, `Read()`, `ReadAt()` — standard I/O operations
- `Seek()`, `Stat()`, `Truncate()` — file positioning and sizing
- `Sync()` — flush to durable storage (fdatasync on Linux, fsync elsewhere)
- `Close()` — close file handle

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
| `waldb/record.go`      | CRC32 record encoding/decoding, chunk types     |
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
- Increase `MaxFileSize` to 256-512 MiB to reduce rotation frequency
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
