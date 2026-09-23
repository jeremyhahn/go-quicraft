# Log Storage — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/logdb`, `pkg/logdb/waldb`,
> `pkg/logdb/memdb` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

You reach this feature in three ways: choosing a store when you build a `Host`,
sizing and monitoring a data directory, and recovering one that came back wrong.

## Before you start

- Go 1.27.0 or later, and `github.com/jeremyhahn/go-quicraft` as a dependency.
- A directory on a filesystem whose `fdatasync` you trust. Everything below
  assumes it.
- A decision about **block size**, because it cannot be changed later.

## Task: choose a store

```go
// Durable, the production choice. HostConfig.WALDir is where it lives; the
// barrier encrypts it at rest (see ../seal-barrier/user.md).
host, err := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier))

// In-memory. Tests, development, ephemeral nodes, benchmarks.
host, err := quicraft.NewHost(cfg, quicraft.WithMemoryLogDB(), quicraft.WithoutEncryption())

// Your own logdb.LogDB implementation.
host, err := quicraft.NewHost(cfg, quicraft.WithLogDB(myStore), quicraft.WithoutEncryption())
```

Every host states its at-rest encryption: `WithBarrier` or `WithShardBarriers`
to encrypt, or `WithoutEncryption` to store plaintext. `NewHost` refuses a host
that states neither (`ErrAtRestEncryptionUnspecified`) and one that states both
(`ErrAtRestEncryptionConflict`), so plaintext at rest is never reached by
leaving an option out.

A nil `LogDB` disables persistence entirely, and the engine then inlines
`Peer.Commit` and cuts the pipeline from three hops to two — faster, and with no
durability at all.

## Task: size a data directory

The three numbers, and which of them you can change your mind about:

| Setting | Default | Changeable later? |
|---|---|---|
| `BlockSize` | 32 KiB | **No.** Immutable for the life of the directory; `Open` refuses a changed value rather than destroying the log. Match the filesystem block size if you are going to deviate — 4096 for ext4. |
| `MaxFileSize` | 64 MiB | Yes, safely, on a populated directory. Recovery never reads it, so it only changes where future rotations land. Raise it to 256–512 MiB for thousands of shards. |
| shard count | 16 | No, and it is not exposed. |

Set a disk ceiling. Without one, the WAL fills the volume and the first thing
you learn about it is `fdatasync` returning ENOSPC, which poisons the DB:

```go
waldb.WithDiskMonitorOpts(maxBytes,
    waldb.WithWarningPercent(0.85),
    waldb.WithOnWarning(func(used, max uint64) { alert(...) }),
    waldb.WithOnFull(func(used, max uint64) { page(...) }),
)
```

Past the full threshold, proposals are rejected with `ErrDiskFull` — a clean
refusal instead of an unrecoverable one.

## Task: turn on encryption

```go
// The barrier must be initialized AND unsealed before Open.
db, err := waldb.Open(dir, waldb.WithBarrier(barrier))
```

A sealed barrier fails the open with `ErrBarrierContextUnresolved` rather than
latching a downgraded binding. Everything about keys, epochs and rotation is in
[`../seal-barrier/user.md`](../seal-barrier/user.md); from this layer's side:

- Rotation is transparent. The epoch rides in each record's 8-byte big-endian
  prefix, so old records keep reading after a rotate.
- **Before purging DEKs, ask the store.** `MinLiveEpoch() (uint64, error)` is
  the floor for a single-barrier deployment; `MinLiveEpochForBarrier(barrier)
  (epoch uint64, known bool, err error)` is the per-tenant form. In the
  three-value form, `known == false` means the store cannot attribute the
  barrier at all — **not** that there is no constraint — and must never be read
  as a licence to purge. Treat any error the same way.
- **Check `AtRestProtection()`** rather than inferring encryption from the fact
  that you passed a barrier.
- Changing the sealing strategy across a restart while the WAL holds records is
  **not** a supported migration. Use `Reencrypt`, which writes to a new
  directory.

## Task: instrument it

```go
waldb.WithMetrics(myCollector)   // nil skips collection entirely
```

Write latency and fsync latency are the pair worth graphing: they separate "the
disk is slow" from "the pipeline is slow", and nothing else in this repository
distinguishes those.

## Task: check a node after every restart

One call, silent if you do not make it. (A node that may have come back shorter
than the log it acknowledged does not open at all — see the next task — so
there is no "came back short" signal to poll for.)

```go
nodes, err := db.ListNodeInfo()
// A shard you expected to be here is not.
```

## Task: recover a corrupt segment

Recovery **never repairs on its own**, because a repair discards data. It stops
and names what it found:

- `ErrMidSegmentCorruption` / `ErrNonFinalSegmentCorruption` — a bad record
  somewhere other than the tail of the last segment. This is not an interrupted
  write; it is corruption of data that was acknowledged.
- `ErrAcknowledgedDataLost` — a bad record at the tail of the last segment,
  with only the in-flight batch above it, but the damage begins *below* the
  acknowledged end that batch's records name. An interrupted write cannot do
  that (a lost page reverts to its fsynced content); media damage or tampering
  can. Opening would bring the node up shorter than the log it acknowledged.
- `ErrRecordSequenceRegression` — the durability sequence went backwards.

Only after you have decided the data loss is acceptable:

```go
res, err := waldb.Repair(dir, waldb.RepairTarget{ /* shard, segment, offset */ })
```

`Repair` refuses a segment that is not corrupt (`ErrRepairNoCorruption`), an
offset that is not the clean-prefix end (`ErrRepairOffsetMismatch`), and a shard
index that does not exist (`ErrRepairShardOutOfRange`). Those refusals are the
feature: they stop you truncating the wrong thing.

## Task: run tests against the I/O failure paths

```sh
make test-waldb     # unit
make test-logdb     # the interface package
make ci-errfs       # the test/errfs fault-injection suite
make fuzz-waldb     # record and segment decode
```

Supply your own `FS` via `WithFS` to inject failures in your own tests.

**Do not use `WithNoSync(true)` to make a durability test pass.** It disables
every fsync *and* every directory sync, so every record in such a segment
carries durability generation 0 and recovery is always free to truncate it. A
test that needs recovery to see acknowledged data must let the syncs run.

## Troubleshooting

**`ErrDBPoisoned`** — an `fdatasync` failed, so no subsequent write can
establish durability. Every write is refused and **the process must restart into
crash recovery**; there is no in-process reset. Do not treat this as transient:
a failed `fdatasync` may leave the kernel having marked dirty pages clean
without writing them, so continuing would build on bytes that are not there.
Check the disk.

**`ErrDiskFull`** — the monitor's full threshold was crossed. Proposals are
being rejected deliberately. Free space or raise the ceiling; the alternative
was ENOSPC inside `fdatasync`.

**`ErrWALGeometryMismatch` on `Open`** — you changed `BlockSize` on an existing
directory. Change it back. The refusal is what saved the log.

**`ErrWALFormatMissing` / `ErrWALFormatMalformed` /
`ErrWALFormatUnsupportedVersion`** — the directory was not written by this
format version. Until the first production release, formats are pinned at 1 and
changed in place, so this most often means two builds from different commits.
Rebuild everything from one commit; there is no migration.

**A replay error naming a legacy payload type** — a 0.4.x data directory under a
0.5.x binary. The per-entry record header grew from 76 to 84 bytes and
`proto.Entry`'s from 60 to 68. There is no migration; wipe and re-sync.

**`ErrLogGapDetected`** — the stored range does not begin exactly where the
caller asked, or an interior index is missing. This is refused rather than
served as a shortened result, on purpose: a silently short read is a log hole
the layer above cannot detect.

**`ErrEntriesBelowCompactionFloor`** — an update whose first entry is at or
below the compaction floor. The **whole batch** is rejected, not trimmed: those
indexes are already deleted, so honouring the write would replace the live
suffix with entries nothing can serve.

**`ErrCompacted` from a read** — the entries you asked for have been compacted
away. The follower that needs them needs a snapshot, not a log read; see
[`../snapshots/user.md`](../snapshots/user.md).

**A WAL decrypt failure during replay** — fatal by design. Skipping the record
would be undetected data loss. Check that the barrier is the same one, with the
same algorithm, that wrote the directory.

**Reads are slow** — they should never touch a disk. Reads are served from
in-memory maps and take no writer lock. If they are slow, the memory holding
the live log has grown past what the machine has, which means compaction is not
keeping up.

**Writes are slow on one shard only** — that is what the 16-way sharding exists
to make visible. One shard's fsync does not serialize behind another's, so a
single slow shard is a property of its Raft group's write rate, not of the
store.
