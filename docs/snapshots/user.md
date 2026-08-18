# Snapshots — Usage

> **Status: NORMATIVE** · Scope: go-quicraft snapshot creation, streaming,
> import/export and re-encryption · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Snapshots happen automatically. You reach this feature when you need to tune
the cadence, move a snapshot between machines, re-key one, or work out why a
follower keeps falling behind.

## Before you start

- Your state machine's `SaveSnapshot` and `RecoverFromSnapshot` must round-trip
  exactly. Nothing here checks that, and the failure surfaces as a replica that
  recovers into the wrong state.
- Disk space for `<NodeHostDir>/snapshots/`. Each snapshot is a full image.
- If at-rest encryption is on, the barrier must be **unsealed** for any snapshot
  operation. A sealed barrier fails the operation closed rather than falling
  through to plaintext.

## Task: set the cadence

```go
cfg.SnapshotEntries    = 100000   // applied entries between snapshots
cfg.CompactionOverhead = 5000     // entries kept after the compaction
```

**Zero does not disable snapshotting.** Zero means "unset" and selects the
default. To turn it off:

```go
cfg.SnapshotEntries = config.SnapshotsDisabled
```

Understand what disabling costs, because it is more than "no snapshots": the log
never compacts, snapshot directories are never reclaimed, old barrier epochs can
**never** be purged, and the at-rest barrier is never invoked. If you disable
automatic snapshots you are taking on `RequestSnapshot` yourself.

Snapshotting more often shortens recovery and lets the log compact sooner, at
the cost of writing a full image more often. Snapshotting less often does the
reverse and makes followers more likely to need a transfer rather than a log
replay — which is what `CompactionOverhead` exists to soften: it keeps that many
entries after compaction so a slightly-behind follower can still catch up from
the log.

## Task: take a snapshot on demand

```go
index, err := host.SyncRequestSnapshot(ctx, shardID, quicraft.DefaultSnapshotOption)
```

Or asynchronously with `RequestSnapshot`, which returns a `*RequestState`.

Override the compaction for this one snapshot:

```go
opt := quicraft.SnapshotOption{
    CompactionOverhead:         50000,
    OverrideCompactionOverhead: true,   // required — the field is ignored without it
}
```

`OverrideCompactionOverhead` is the switch; setting `CompactionOverhead` alone
does nothing.

## Task: export a snapshot

```go
opt := quicraft.SnapshotOption{ExportPath: "/var/backups/shard-1"}  // absolute
index, err := host.SyncRequestSnapshot(ctx, shardID, opt)
```

The directory is created if absent, and the snapshot is **also** written to the
standard snapshot directory so log compaction still happens. Do not set
`Exported` yourself; it is derived from `ExportPath`.

To stream one to a writer instead:

```go
err := host.ExportSnapshot(ctx, shardID, w)
```

## Task: import a snapshot

**Read this before running it.** `ImportSnapshot` rewrites the replica's
membership and identity. It is an offline, deliberate, destructive operation.

```go
err := host.ImportSnapshot(ctx, shardID, r,
    quicraft.WithImportDataDir(dir),
    quicraft.WithImportMembership(members),
)
```

The refusals are the safety net, not obstacles:

| Error | Means |
|---|---|
| `*ImportSnapshotDirExistsError` | the destination already exists — it will not silently overwrite |
| `*ImportSnapshotOutOfDateError` | the snapshot is older than what this replica holds — it will not move you backwards |
| `*ImportDataDirMismatchError` | `WithImportDataDir` names a directory that is not this replica's |
| `*ImportMembershipInvalidError` | the membership override is unusable |

If you find yourself wanting to work around one of these, stop and work out
which replica you are actually operating on.

## Task: re-key a snapshot to a new barrier

```go
epoch, err := quicraft.ReencryptSnapshotFile(srcPath, dstPath,
    shardID, replicaID, index, srcBarrier, dstBarrier)
```

Both barriers must be **unsealed**. A `nil` barrier means that side is
plaintext; a **sealed** barrier is not plaintext and fails closed.

`shardID`, `replicaID` and `index` must be the identity the frames were sealed
under — they reproduce the positional AAD, and there is no weaker fallback.

**Record the returned epoch as the migrated snapshot's `Epoch`.** It is the
destination's *minimum* at-rest epoch across the frames, and the DEK-purge floor
uses it to protect every frame.

**And handle this case specifically:**

```go
epoch, err := quicraft.ReencryptSnapshotFile(...)
if err != nil {
    var cnd *quicraft.SnapshotCommittedNotDurableError
    if errors.As(err, &cnd) {
        // The rename SUCCEEDED. dstPath holds the migrated artifact at
        // cnd.Epoch; only the directory fsync failed. RECORD cnd.Epoch,
        // then re-verify the destination.
        recordEpoch(cnd.Epoch)
    }
    return err
}
```

The reflexive `if err != nil { return 0, err }` drops exactly the epoch this
case delivers, and dropping it lets the next DEK purge orphan the artifact's
key.

## Task: tune snapshot transfer

```go
tc.MaxSnapshotReceiveRate        = 256 << 20  // bytes/sec
tc.MaxConcurrentSnapshotReceives = 4
tc.MaxSnapshotReceiveMemory      = 1 << 30
```

A recovering follower will use whatever bandwidth you allow. Snapshot transfers
ride a dedicated frame path outside the stream pool, so they will not block
ordinary replication — but they will compete for the link.

## Task: watch the right things

1. **Age of the newest snapshot, per shard.** A shard that has stopped
   snapshotting is a shard whose log will not compact and whose old DEKs can
   never be purged. This is the signal that matters most and the one nobody
   graphs.
2. **Transfers in flight** against `MaxConcurrentSnapshotReceives`.
3. **Any `DataHash` verification failure** — corrupt or truncated artifact, not
   a transient error.

## Troubleshooting

**`ErrSnapshotBarrierSealed`** — the barrier is configured but sealed. This is
fail-closed behavior, not a bug: falling through would copy ciphertext and
record it as plaintext, or durably downgrade a snapshot that must be encrypted.
Unseal the barrier.

**`ErrSnapshotEncryptedBarrierUnavailable`** — an encrypted artifact and no
barrier to open it. Usually a data directory moved to a node configured without
encryption, or configured with a different algorithm.

**`*SnapshotTooLargeError`** — the artifact exceeds `MaxSnapshotSize` (4 GB).
Either your state is genuinely that large, in which case raise the limit and
think about `DiskStateMachine`, or `SaveSnapshot` is writing more than it needs.

**A `DataHash` mismatch at recovery** — the artifact is corrupt or truncated.
Note that verification is skipped for artifacts written before `0.5.0`
(`DataHashRecorded == false`); those recover unverified, so a truncated file
from that era recovers as if complete.

**Recovery decrypted successfully but the state is wrong** — check that
`shardID`, `replicaID` and `index` match what the frames were sealed under. A
mismatch normally fails authentication, so state that is wrong *without* an
error points at your `SaveSnapshot`/`RecoverFromSnapshot` round trip.

**A DEK purge does nothing, indefinitely** — an in-flight epoch pin was leaked
by a snapshot operation that failed on an unusual path. Every pin has a matching
release on every path including error paths, and
`pkg/snapshot_pin_balance_test.go` exists to hold that; a leak is a defect worth
reporting.

**A follower keeps needing snapshot transfers instead of log replay** — raise
`CompactionOverhead` so more log survives compaction, or snapshot less often.

**Snapshots stopped happening after I set `SnapshotEntries = 0`** — they did
not stop; zero selected the default. Use `config.SnapshotsDisabled`.

**A snapshot save stalls the cluster** — it should not. Snapshot work runs on
its own pool of `max(NumWorkers/2, 2)` workers, separate from step, commit and
apply. If consensus stalls during a save, look at disk saturation rather than at
worker scheduling.

**Trailing frames of an at-rest snapshot were removed and nothing noticed** —
that is a known, documented limit: the AAD binds each frame's position but not
how many follow, so a truncated artifact's surviving frames still authenticate.
In-transit truncation *is* detected, via the declared chunk count. At rest it is
not, and closing it would need a frame count folded into every frame's context.
