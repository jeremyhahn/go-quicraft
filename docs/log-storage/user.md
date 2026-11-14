# Log Storage — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/logdb`, `pkg/logdb/waldb`,
> `pkg/logdb/memdb` · Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

You deal with log storage in three ways: choosing a store when you build a
`Host`, sizing and watching its directory, and recovering one that came back
damaged.

## Before you start

- Go 1.27.4 or later, and `github.com/jeremyhahn/go-quicraft` as a dependency.
- A directory on a filesystem whose `fdatasync` you trust. Everything below
  assumes it.
- A decision about block size if you will not use the default, because it
  cannot be changed later.

## Choose a store

The Go blocks on this page are excerpts: `cfg`, `barrier`, `store`, `walDir`
and `blockSize` are your own values, and the first block shows three
alternatives, not one program.

```go
// Durable, the default. The log lives in cfg.WALDir; the barrier encrypts it.
host, err := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier))

// In memory: tests, development, ephemeral nodes, benchmarks. Nothing survives a restart.
host, err := quicraft.NewHost(cfg, quicraft.WithMemoryLogDB(), quicraft.WithoutEncryption())

// Your own logdb.LogDB, or a WAL you opened yourself with non-default options.
host, err := quicraft.NewHost(cfg, quicraft.WithLogDB(store), quicraft.WithoutEncryption())
```

Every host states its at-rest encryption: `WithBarrier` or `WithShardBarriers`
to encrypt, or `WithoutEncryption` to store plaintext. `NewHost` refuses a host
that says neither (`ErrAtRestEncryptionUnspecified`) or both `WithoutEncryption`
and `WithBarrier` (`ErrAtRestEncryptionConflict`), so you never get plaintext by
leaving an option out. `WithShardBarriers` is a per-shard axis and combines with
either host-wide choice.

The host's barrier is wired only into the default WAL. If you pass your own
store together with `WithBarrier`, that store must itself report that it is
encrypted (or that it is in memory), or that its host-wide route writes through
that same barrier, or `NewHost` fails with `ErrBarrierPlaintextLogDB`. Open the
WAL with the same barrier; this holds too while the barrier is in plaintext
mode (unsealed under the no-encryption strategy, `IsPlaintextMode()`):

```go
store, err := waldb.Open(walDir, waldb.WithBarrier(barrier))
host, err := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier), quicraft.WithLogDB(store))
```

## Size the directory

| Setting | Default | Change it later? |
|---|---|---|
| block size | 32 KiB | **No.** Fixed for the life of the directory; `Open` refuses a different value instead of destroying the log. If you deviate, match the filesystem block size (4096 for ext4). |
| segment size | 64 MiB | Yes, safely, on a populated directory. It only moves future rotations. For thousands of shards, 256–512 MiB. At most 4 GiB − 1. |
| shard count | 16 | No, and it is not configurable. |

The default WAL uses the default sizes. To use others, open the WAL yourself
and pass it with `WithLogDB`:

```go
store, err := waldb.Open(walDir,
    waldb.WithBarrier(barrier),
    waldb.WithMaxFileSize(256<<20),
)
```

`ExampleOpen` in `pkg/logdb/waldb/example_test.go` compiles this open, saves
one update, reopens and reads the state back, with checked output.

Set a disk ceiling. Without one, the WAL fills the volume and the first sign is
`fdatasync` returning ENOSPC, which stops the store until a restart:

```go
cfg.MaxWALDiskSize = 50 << 30 // bytes
cfg.EventListener = &config.EventListener{
    OnLogDBDiskWarning: func(info config.DiskInfo) { /* alert: 85% used */ },
    OnLogDBDiskFull:    func(info config.DiskInfo) { /* page: 90% used */ },
}
```

The ceiling is compared with the used space of the whole filesystem holding the
WAL (Linux `statfs`), not with the WAL's own size, so other files on the volume
count against it. Two thresholds apply:

- **90%: proposals are refused.** The host samples usage every 10 proposals;
  past 90% of the ceiling, `Propose`, `ProposeWithSession` and snapshot
  imports fail with `quicraft.ErrDiskFull`, `OnLogDBDiskFull` fires once and
  `HealthCheck` reports `DiskOK` false. The shard keeps serving reads.
  Proposals resume once a sample sees usage below 90%.
- **95%: the WAL refuses writes.** The WAL samples usage every 100 writes;
  past 95% it refuses the write with `waldb: disk full`. An entry that cannot
  be persisted must not be acknowledged, so the shard that issued the write
  fails (`OnShardFailed`) and must be restarted once space is freed.

Because the proposal threshold sits below the WAL's and the host samples ten
times as often, a workload driven by this host's proposals is refused before
the WAL fills. The shard still fails when the WAL reaches 95% first, which
happens when:

- usage jumps from below 90% to 95% or more between two proposal samples
  (another process filling the volume, a large snapshot, a large batch);
- the replica writes without proposing: a follower persisting the leader's
  entries, or a node catching up, samples only through the WAL.

Size `MaxWALDiskSize` so that 5% of it covers the volume's growth between
samples, and alert on `OnLogDBDiskWarning` (85%).

## Encrypt it

The barrier must be initialized and unsealed before the WAL opens. A sealed
barrier fails the open with `ErrBarrierContextUnresolved`. Keys, epochs and
rotation are covered in [the seal-barrier guide](../seal-barrier/user.md). What
matters on this side:

- Rotation needs nothing from you. Each record carries its epoch, so older
  records keep reading after a rotate.
- Before purging old keys, ask the store for its floor (`MinLiveEpoch`, or
  `MinLiveEpochForBarrier` per tenant). With the per-tenant form,
  `known == false` means the store cannot account for that barrier at all. It
  never means "no constraint". Treat it, and any error, as "do not purge".
- Check what the store reports (`AtRestProtection()`) rather than assuming it is
  encrypted because you passed a barrier.
- Changing the sealing strategy across a restart while the WAL holds records is
  not supported. Re-encrypt into a new directory with `waldb.Reencrypt` and swap
  it in.

## Instrument it

`quicraft.WithMetrics(collector)` wires the WAL's write, fsync and compaction
latency into your collector. Write and fsync latency are the pair worth
graphing: together they tell "the disk is slow" apart from "the pipeline is
slow".

## Remove a replica's data

`host.RemoveData(shardID, replicaID)` retires that replica ID on this host for
good. The store refuses any later write for it, and the host refuses to start
it again (`ErrReplicaRetired`). To bring that member back, add it under a **new**
replica ID. Reusing the old ID could let it vote twice in one term, because its
vote history was removed with its data.

## Recover a damaged segment

The store never repairs itself, because a repair discards data. When it finds
damage it cannot explain as an interrupted write, it refuses to open and says
why:

- `ErrMidSegmentCorruption` or `ErrNonFinalSegmentCorruption`: damage below
  records that were already acknowledged.
- `ErrAcknowledgedDataLost`: damage in the last write, but starting below what
  had already been acknowledged. A power cut cannot do that; failing media or
  tampering can.
- `ErrRecordSequenceRegression`: bytes this log cannot have written.

**First choice: re-sync from peers.** Make sure the rest of the cluster holds a
quorum, stop the node, wipe its WAL and host directories, and start it. It
rejoins from a snapshot plus log and loses nothing its peers hold.

**No peers: authorized repair.** Copy the data directory first; repair cannot
be undone. Then, with the node stopped:

```go
var corrupt *waldb.SegmentCorruptionError
if errors.As(openErr, &corrupt) {
    res, err := waldb.Repair(walDir, waldb.RepairTarget{
        ShardIndex: corrupt.ShardIndex,
        SegmentID:  corrupt.SegmentID,
        TruncateAt: corrupt.CleanEnd,
    }, waldb.WithBlockSize(blockSize))
    // res.RecordsKept, res.BytesDiscarded, res.SegmentsDeleted
}
```

`ExampleRepair` in `pkg/logdb/waldb/example_test.go` runs this end to end:
it damages a segment, matches the `*SegmentCorruptionError` from `Open`,
repairs, and reopens on the entries before the damage, with checked output.

Pass the block size the directory was written with. `Repair` refuses a segment
that is not damaged (`ErrRepairNoCorruption`), an offset other than the one the
error named (`ErrRepairOffsetMismatch`), and a shard index that does not exist
(`ErrRepairShardOutOfRange`). Those refusals stop you truncating the wrong
thing. Everything above the offset is lost, including the newest vote record in
that shard.

## Test against I/O failures

```sh
make test-waldb     # the WAL
make test-logdb     # the interface, conformance suite and in-memory store
make ci-errfs       # injected I/O errors
make fuzz-waldb     # record and segment decoding
```

Pass your own `waldb.FS` with `waldb.WithFS` to inject failures in your tests.

Do not use `waldb.WithNoSync(true)` (or `cfg.NoSyncWAL`) to make a durability
test pass. It skips every sync, so nothing is ever acknowledged and recovery may
truncate anywhere. A test that needs recovery to keep acknowledged data must let
the syncs run.

## Troubleshooting

**`ErrDBPoisoned`**: an `fdatasync` failed, so no later write can be proven
durable. Every write is refused and the process must restart; there is no
in-process reset. Do not treat it as transient: after a failed `fdatasync` the
kernel may have dropped the data while marking it clean. Check the disk.

**`ErrDiskFull`**: the disk ceiling was reached and proposals are refused on
purpose. Free space or raise `MaxWALDiskSize`.

**`ErrWALGeometryMismatch` on open**: the block size differs from the one the
directory was written with. Change it back; the error names both values.

**`ErrWALFormatMissing`, `ErrWALFormatMalformed`,
`ErrWALFormatUnsupportedVersion`, or `ErrUnknownPayloadType` on open**: the
directory was written by a build with a different on-disk layout. Until the
first production release, layouts change in place without a migration, so this
usually means two builds from different commits. Wipe the directory and re-sync
from peers. If the WAL was encrypted, `ErrUnknownPayloadType` can also mean it
was opened without its barrier: supply the barrier first.

**`ErrRetiredRegistryMalformed` on open**: the record of removed replicas is
damaged. Do not delete it to get the node up; that would let removed replica IDs
back in. Restore it, or re-sync the node from peers.

**`ErrLogGapDetected`**: the stored log has a hole where you asked to read. It
is reported instead of returning a shorter result, because a silently short read
is a hole nobody downstream can see.

**`ErrEntriesBelowCompactionFloor`**: a write tried to store entries the log has
already compacted. The whole batch is refused, not trimmed.

**`ErrCompacted` on a read**: those entries were compacted away. A follower that
needs them needs a snapshot; see [the snapshots guide](../snapshots/user.md).

**A decrypt failure on open**: fatal by design, since skipping the record would
lose data silently. Check that the barrier is the one that wrote the directory.

**Reads are slow**: they never touch the disk. Slow reads mean the live log has
outgrown memory, so compaction is not keeping up.

**Writes are slow on one shard only**: shards fsync independently, so one slow
shard reflects its own Raft groups' write rate, not the store as a whole.
