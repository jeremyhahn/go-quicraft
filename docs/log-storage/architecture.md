# Log Storage — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/logdb`, `pkg/logdb/waldb`,
> `pkg/logdb/memdb` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

## The problem

Raft's safety rests on one storage promise: when the algorithm is told an entry
and a term are persisted, they are. Commit, leader election and the state
machine's view of the world are all unsound if that promise is ever weaker than
it claims.

A single node runs thousands of independent Raft groups. Serializing their
fsyncs through one file makes one slow group everybody's problem. Giving each
group its own file means 10,000 file descriptors and 10,000 directory entries.

## The shape chosen

An append-only, block-aligned, CRC-checked segment log, sharded 16 ways, with
every read served from memory.

```
   <WALDir>/
     WALFORMAT                              block size + format version
     RETIRED                                retired-replica registry (once a replica is removed)
     shard-0/  000000.log 000001.log ...    own active segment, index, mutex
     shard-1/  ...
     ...
     shard-15/                              shardID % DefaultShardCount
```

- **Sharded writes.** 16 directory shards, selected by `shardID % 16`. Each has
  its own active segment, entry index and mutex, so one shard's fsync does not
  wait behind another's.
- **Write-ahead durability.** Everything is written and fsynced before it is
  acknowledged. In-memory state is updated only afterwards.
- **In-memory reads.** Reads come from in-memory maps and take no writer lock,
  so a reader never waits on an fsync. The WAL is read only by recovery.
- **Fixed binary encoding**, little-endian, no serialization dependency. The one
  exception is the positional context-AAD of an encrypted record, which is
  big-endian.
- **Lazy initialization.** Shard directories and segment files are created on
  first write, not at `Open`, so a node using three shards does not create
  sixteen directories.
- **Fail closed.** Anything recovery cannot prove safe stops the open rather
  than being skipped or guessed at.

### Rejected: an embedded KV store (Pebble, BoltDB, ...)

It would give indexing, compaction and crash recovery for free. It would also
bring a large dependency into a Layer-1 library whose dependency budget is near
zero, an LSM's write amplification on a workload that is already an append log,
and a compaction scheduler competing with Raft's own. The access pattern here is
append, then read back sequentially once at recovery: the narrowest case there
is, and the one a general KV store suits least.

### Rejected: one file per Raft group

Simple, and unusable at 10,000 groups: file descriptors, directory entries, and
a per-group fsync that cannot be batched with anything.

### Rejected: one file for everything

It batches every group's fsync together, which is good, and makes every group
wait on the slowest, which is not. Sixteen is the compromise: enough parallelism
to scale past 10,000 groups, few enough files to keep descriptor counts and
directory sizes ordinary.

### Rejected: reading the log on the read path

Serving reads from the log would make every read an I/O and put readers behind
the writer's lock. In-memory maps cost the memory of the live log, which
compaction bounds, and buy a read path with no disk in it.

### Rejected: a WAL record for each removed replica

A remove-node tombstone has to outlive every older segment that still holds the
removed replica's records, and other Raft groups on the same shard can keep
those segments alive indefinitely. The tombstone's segment would be pinned for
good: one whole segment (64 MiB by default) per removed replica. Removals are
kept in a small `RETIRED` file beside the segments instead (16 bytes per
replica), and replay drops a retired replica's records wherever it finds them.

## Telling an interrupted write from lost data

Every chunk header carries two values alongside the CRC: a **durability
sequence** (how many `fdatasync` calls on the segment had completed when the
chunk was written) and a **generation start** (the file offset at which those
acknowledged bytes ended). Recovery uses them to separate two cases that look
byte-identical:

- an **interrupted write**, whose records nothing was ever told about. Safe to
  truncate.
- **damage to acknowledged data**. Never safe to truncate, because the entry may
  already have counted toward a commit quorum.

A torn tail and a flipped bit in a synced record both show up as a CRC
mismatch. A recovery that truncates on CRC mismatch alone silently discards
acknowledged entries, which is the one failure Raft cannot tolerate. Any chunk
that survives above the damage names the acknowledged end exactly: damage at or
above it is an interrupted write, damage below it fails the open with
`ErrAcknowledgedDataLost`.

After each completed `fdatasync`, `Sync` appends a header-only **sync mark**
chunk. Without it, the last generation of a segment has nothing above it to
prove it was acknowledged, and rot inside it would read as a torn tail.

Both values sit inside the checksummed region, so a flipped bit in either
invalidates the chunk instead of silently moving it into another generation.
The rules are in [engineering.md](engineering.md#recovery).

## Fail-stop on fsync failure

An `fdatasync` failure **poisons** the whole DB: every later write returns
`ErrDBPoisoned` and the process must restart into crash recovery. There is no
in-process reset. A failed directory fsync after the retired-replica registry
was renamed into place poisons it the same way.

The reason is fsyncgate. After a failed `fdatasync` the kernel may have marked
dirty pages clean without writing them, so the durability of those bytes is no
longer knowable. Rolling back is not enough either, because the truncation's own
durability cannot be established. The only sound move is to stop and let crash
recovery work out the truth from what is actually on the medium.

## Data flow

```
 commit worker
      │  LogDB.SaveState([]Update)
      ▼
 shardID % 16 ─▶ walShard ──▶ barrier seal (optional) ──▶ segment.Write
                     │                                         │
                     │                                    fdatasync
                     │                                         │
                     └────────── in-memory index updated ◀─────┘
                                       (only after durability)

 reads ──▶ in-memory maps          (never touches a segment file)

 restart ─▶ Open ─▶ check WALFORMAT, load RETIRED
                ─▶ replay every segment of every shard, in order
                ─▶ barrier decrypt ─▶ rebuild every in-memory map
                ─▶ drop retired replicas' records
                ─▶ refuse to open (fail closed) rather than come up short
```

Nothing here crosses a host boundary. The only process boundary is the
filesystem, and the only external dependency is the `FS` seam.

## Two implementations behind one interface

| Implementation | Backing | For |
|---|---|---|
| `waldb.DB` | sharded segment files, fsync, encryption | production |
| `memdb.MemDB` | per-node maps, sorted slices, binary search | tests, development, ephemeral nodes, benchmarks |

`memdb` is not a toy. It keeps per-node read-write locks, an atomic closed flag,
the same compaction floor, the same retirement rules and the same error
contract, because a test harness that behaves differently from production hides
races instead of exposing them. It provides no durability, and that is the whole
difference. The `Host` selects it with `WithMemoryLogDB()`.

## Trust boundaries

- **The disk is not trusted for integrity.** CRC32-C detects accidental
  corruption: bit flips, partial writes, torn pages. It is not a cryptographic
  check, and anyone with disk access recomputes it. Tamper protection comes from
  the barrier's AEAD. The CRC stays as a cheap first-pass filter, so garbage is
  not fed into the decryption path, because it is hardware-accelerated and far
  cheaper than a MAC.
- **Encryption belongs to the barrier, not this layer.** `waldb` declares its
  own narrow `WALBarrier` interface instead of importing `pkg/seal`, so the two
  are tested independently.
- **Positional binding.** Every encrypted record binds `WALShardIdx`,
  `SegmentID` and `RecordSeq` into the AAD, so a record authenticates only where
  it was written. `WALShardIdx` is required: each shard has its own `%06d.log`
  ID space and all of them share one barrier. There is no epoch-only fallback; a
  reader that fails to authenticate never retries without the context.
- **Tail truncation at rest is not detected by the AEAD.** Nothing in the AAD
  says how many records follow. Someone who can write the data directory can
  remove trailing records and their sync mark, which leaves exactly what a crash
  inside the writeback window leaves. No in-file format can tell those apart.
  Raft assumes stable storage, so write access to a data directory is compromise
  of that replica. See
  [`../seal-barrier/architecture.md`](../seal-barrier/architecture.md).
- **Metadata leaks.** The chunk framing (CRC, length, type, durability sequence,
  generation start) and each record's epoch are unencrypted, so a reader of the
  file sees record sizes, record types, sync boundaries and the rotation epoch.
  This is accepted: local read access to the file is already a high-privilege
  position.
- **Decryption failure during replay is fatal**, never skipped. Dropping an
  unreadable record would be undetected data loss.

## Component boundaries

- **`pkg/logdb`**: the `LogDB` interface and the storage-layer types (`Entry`,
  `State`, `Membership`, `Snapshot`, `Bootstrap`, `Update`, `NodeInfo`). These
  are separate from `pkg/proto`'s types so serialization and persistence can
  change independently. It also declares the optional interfaces the engine
  probes for: `AtRestProtectedLogDB`, `EpochAwareLogDB`,
  `PerBarrierEpochAwareLogDB`, `BarrierShardEnumerator`.
- **`pkg/logdb/waldb`**: the durable implementation, the `FS`/`File`
  abstraction, `DiskMonitor`, the retired-replica registry, `Repair` and
  `Reencrypt`.
- **`pkg/logdb/memdb`**: the in-memory implementation.
- **Consumers**: the engine's commit worker, and `quicraft.Host` through
  `WithLogDB` and `WithMemoryLogDB`. With neither, the host opens a `waldb.DB`
  at `HostConfig.WALDir`.

## What this feature deliberately does not do

- **No queries.** No secondary indexes, no range scans by anything but log
  index, no predicates. It stores a log.
- **No replication.** Replication is Raft's; this layer stores what Raft has
  already decided.
- **No key management.** `WALBarrier` is a seam. DEKs, epochs and rotation
  belong to [`../seal-barrier/`](../seal-barrier/architecture.md).
- **No cryptographic integrity of its own.** CRC32-C is not a MAC, and this
  document says so rather than letting "checksummed" imply "tamper-evident".
- **No automatic repair.** `Repair` takes an explicit target and offset, and
  refuses a segment that is not corrupt (`ErrRepairNoCorruption`) or an offset
  that is not the clean-prefix end (`ErrRepairOffsetMismatch`). Recovery never
  repairs on its own, because a repair discards data.
- **No re-encryption in place.** `Reencrypt` writes to a new directory. Changing
  the sealing strategy across a restart while the WAL holds records is not a
  supported migration.
- **No format migration.** `walFormatVersion` is pinned at 1 until the first
  production release. Layout changes are made in place, and development data
  directories are wiped and re-synced from peers, not migrated. A record layout
  change stays detectable at the record level: its payload-type byte is retired
  (left unallocated, never reused) and the new layout takes the next free byte,
  so a stale record fails replay with `ErrUnknownPayloadType`.
- **`BlockSize` is immutable for the life of a data directory.** `Open` refuses
  a changed value rather than destroying the log.
- **A removed replica ID never comes back.** `RemoveNodeData` retires it, and
  every writer refuses it from then on.
