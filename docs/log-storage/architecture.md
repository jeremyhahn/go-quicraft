# Log Storage — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/logdb`, `pkg/logdb/waldb`,
> `pkg/logdb/memdb` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## The problem

Raft's safety argument rests on one storage promise: **when the algorithm is
told an entry and a term are persisted, they are.** Everything above — commit,
leader election, the state machine's view of the world — is unsound if that
promise is ever weaker than it claims.

A single node runs thousands of independent Raft groups. Serializing their
fsyncs through one file makes one slow group everybody's problem; giving each
group its own file makes 10,000 file descriptors and 10,000 directory entries.

## The shape chosen

An **append-only, block-aligned, CRC-checked segment log, sharded 16 ways**,
with every read served from memory.

```
   <WALDir>/
     shard-0/  000001.log 000002.log ...   own active segment, index, mutex
     shard-1/  ...
     ...
     shard-15/                              shardID % DefaultShardCount
```

- **Sharded writes.** 16 directory shards, selected by `shardID % 16`. Each has
  its own active segment, entry index and mutex, so one shard's fsync does not
  serialize behind another's.
- **Write-ahead durability.** Everything is written and fsynced *before* it is
  acknowledged; in-memory state is updated only afterwards.
- **In-memory reads.** Reads are served entirely from in-memory maps and take no
  writer lock, so a reader never waits on an fsync. **The WAL is never read
  during normal operation** — it exists solely for durability and is read only
  by recovery.
- **Fixed binary encoding**, little-endian, no external serialization
  dependency. The one exception is the positional context-AAD of an encrypted
  record, which is big-endian.
- **Lazy initialization.** Shard directories and segment files are created on
  first write, not at `Open`, so a node using three shards does not create
  sixteen directories.
- **Fail closed.** Anything recovery cannot prove safe stops the open rather
  than being skipped or guessed at.

### Rejected: an embedded KV store (Pebble, BoltDB, …)

It would give indexing, compaction and crash recovery for free. It also brings a
large dependency into a Layer-1 library whose dependency budget is deliberately
near zero, an LSM's write amplification on a workload that is already an append
log, and a compaction scheduler competing with Raft's own. The access pattern
here is *append and read back sequentially once, at recovery* — the narrowest
possible case, and the one a general KV store is least suited to.

### Rejected: one file per Raft group

Direct, obvious, and unusable at 10,000 groups: file descriptors, directory
entries, and a per-group fsync that cannot be batched with anything.

### Rejected: one file for everything

Batches every group's fsync together, which is good, and makes every group wait
on the slowest, which is not. Sixteen is the compromise: enough parallelism to
scale past 10,000 groups, few enough files to keep descriptor counts and
directory sizes ordinary.

### Rejected: reading the log on the read path

Serving reads from the log would make every read an I/O and would put readers
behind the writer's lock. In-memory maps cost the memory of the live log, which
compaction bounds, and buy a read path with no disk in it at all.

## The durability sequence, and why it exists

Each chunk header carries a **durability sequence** alongside the CRC. It is
what lets recovery distinguish *an interrupted write* — safe to truncate — from
*corruption of data that was already acknowledged* — never safe to truncate.

Without it the two are byte-identical: a torn tail and a flipped bit in a synced
record both present as a CRC mismatch, and a recovery that truncates on CRC
mismatch alone silently discards acknowledged entries. That is the one failure
Raft cannot tolerate, because the entry may already have been counted toward a
commit quorum.

The sequence sits **inside** the checksummed region, deliberately, and after the
type byte so the covered region stays one contiguous slice and the CRC is
computed in a single pass. A flipped bit in the sequence must invalidate the
chunk rather than silently move it into another generation.

## Fail-stop on fsync failure

An `fdatasync` failure **poisons** the whole DB: every subsequent write returns
`ErrDBPoisoned` and the process must restart into crash recovery. There is no
in-process reset.

This is not caution, it is fsyncgate. A failed `fdatasync` may leave the kernel
having marked dirty pages clean without writing them, so the bytes' durability
is no longer knowable — and rolling back is not sufficient either, because the
truncation's own durability cannot be established. The only sound move is to
stop and let crash recovery re-derive the truth from what is actually on the
medium.

## Data flow

```
 commit worker
      │  LogDB.SaveState(Update)
      ▼
 shardID % 16 ─▶ walShard ──▶ barrier.Encrypt (optional) ──▶ segment.Write
                     │                                            │
                     │                                       fdatasync
                     │                                            │
                     └────────── in-memory index updated ◀────────┘
                                       (only after durability)

 reads ──▶ in-memory maps          (never touches a segment file)

 restart ─▶ Open ─▶ replay every segment of every shard, in order
                    ─▶ barrier.Decrypt ─▶ rebuild every in-memory map
                    ─▶ RecoveryResiduals() reports anything it could not prove
```

Nothing here crosses a host boundary. The only process boundary is the
filesystem, and the only external dependency is the `FS` seam.

## Two implementations behind one interface

| Implementation | Backing | For |
|---|---|---|
| `waldb.DB` | sharded segment files, fsync, encryption | production |
| `memdb.MemDB` | sharded maps, sorted slices, binary search | tests, development, ephemeral nodes, benchmarks |

`memdb` is not a toy: it shards by `(shardID, replicaID)` with per-node RWMutexes
and a lock-free atomic closed-check, because a test harness that serializes
differently from production hides races rather than exposing them. It provides
**no durability**, and that is its entire difference.

A third mode exists at the `Host` level: `WithMemoryLogDB()`, and a nil `LogDB`
entirely, which lets the engine inline `Peer.Commit` and cut the pipeline from
three hops to two. See
[`../raft-consensus/architecture.md`](../raft-consensus/architecture.md).

## Trust boundaries

- **The disk is not trusted for integrity.** CRC32-C detects *accidental*
  corruption — bit flips, partial writes, torn pages. It is not a cryptographic
  check: an adversary with disk access recomputes it trivially. Tamper
  protection comes from the barrier's AEAD, and the CRC survives as a cheap
  first-pass detector so garbage is not fed into the decryption path. The split
  is a measured one: CRC32-C runs at ~30 GB/s hardware-accelerated where
  HMAC-SHA256 runs at ~1–3 GB/s.
- **Encryption is the barrier's, not this layer's.** `waldb` declares its own
  narrow `WALBarrier` interface rather than importing `pkg/seal`, so the two are
  independently testable and neither depends on the other's shape.
- **Positional binding.** Every encrypted record binds `WALShardIdx`,
  `SegmentID` and `RecordSeq` into the AAD, so a record only authenticates at
  the exact place it was written. `WALShardIdx` is required rather than
  decorative: each shard has an independent `%06d.log` ID space and all of them
  share one barrier. There is **no epoch-only fallback** — a reader that fails
  to authenticate must never retry without the context.
- **Tail truncation at rest is not detected by the AEAD.** Nothing in the AAD
  names how many records follow. The durability-sequence recovery rules are what
  this layer relies on instead; see
  [`../seal-barrier/architecture.md`](../seal-barrier/architecture.md).
- **Metadata leaks.** The outer framing — `CRC32`, `Length`, `Type` — and the
  `Epoch` are unencrypted, so a reader of the file sees record sizes, record
  types and the rotation epoch. Accepted: local file read access is already a
  high-privilege position.
- **Decryption failure during replay is fatal**, never skipped. Silently
  dropping an unreadable record would be undetected data loss.

## Component boundaries

- **`pkg/logdb`** — the `LogDB` interface and the storage-layer types (`Entry`,
  `State`, `Membership`, `Snapshot`, `Bootstrap`, `Update`, `NodeInfo`). These
  are **deliberately separate types from `pkg/proto`'s**, to keep serialization
  and persistence independently changeable. It also declares the capability
  interfaces the engine probes for: `AtRestProtectedLogDB`, `EpochAwareLogDB`,
  `PerBarrierEpochAwareLogDB`, `BarrierShardEnumerator`,
  `RecoveryResidualReporter`.
- **`pkg/logdb/waldb`** — the durable implementation, the `FS`/`File`
  abstraction, `DiskMonitor`, `Repair` and `Reencrypt`.
- **`pkg/logdb/memdb`** — the in-memory implementation.
- **Consumers**: `pkg/internal/engine`'s commit worker, and `pkg.Host` through
  `WithLogDB` / `WithMemoryLogDB`.

This feature is part of the go-quicraft capability row covering Raft consensus,
log replication and QUIC transport in `docs/00 §R`; it has no row of its own.

## What this feature deliberately does not do

- **No query capability.** No secondary indexes, no range scans by anything but
  log index, no predicates. It stores a log.
- **No replication.** Replication is Raft's; this layer stores what Raft has
  already decided.
- **No key management.** `WALBarrier` is a seam; the DEKs, epochs and rotation
  belong to [`../seal-barrier/`](../seal-barrier/architecture.md).
- **No cryptographic integrity of its own.** CRC32-C is not a MAC, and this
  document says so rather than letting "checksummed" imply "tamper-evident".
- **No automatic repair.** `Repair` exists, takes an explicit target and offset,
  and refuses a segment that is not corrupt (`ErrRepairNoCorruption`) or an
  offset that is not the clean-prefix end (`ErrRepairOffsetMismatch`). Recovery
  never repairs on its own, because a repair discards data.
- **No re-encryption in place.** `Reencrypt` writes to a new directory. A
  sealing-strategy change across a restart while the WAL holds records is not a
  supported migration.
- **No format migration.** `walFormatVersion` is 1 and pinned for the
  pre-release period; layout changes are made in place and development data
  directories are wiped rather than migrated. 0.5.x cannot read 0.4.x data —
  detected at the **record** level by the payload-type ladder, which names the
  legacy format it found, rather than by the version number.
- **`BlockSize` is immutable for the life of a data directory.** `Open` refuses
  a changed value rather than destroying the log.
