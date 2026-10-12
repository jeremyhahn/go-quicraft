# Snapshots — Usage

> **Status: NORMATIVE** · Scope: go-quicraft snapshot creation, streaming,
> import/export and re-encryption · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

Snapshots happen on their own. You deal with them when you tune how often they
are taken, move one between machines, re-key one, or work out why a follower
keeps falling behind.

## Before you start

- Your state machine's `SaveSnapshot` and `RecoverFromSnapshot` must round-trip
  exactly. Nothing here can check that, and a mismatch shows up as a replica
  that recovers into the wrong state.
- Leave disk space under `<NodeHostDir>/snapshots/` (or the shard's `DataDir`).
  Each snapshot is a full image.
- With at-rest encryption on, the barrier must be unsealed for every snapshot
  operation. A sealed barrier fails the operation instead of falling back to
  plaintext.

## Set the cadence

```go
cfg.SnapshotEntries = 100000   // applied entries between snapshots
cfg.CompactionOverhead = 5000  // entries kept below the snapshot when the log is compacted
```

Zero does not disable snapshotting; it selects the default. To turn automatic
snapshots off:

```go
cfg.SnapshotEntries = config.SnapshotsDisabled
```

That costs more than "no snapshots": the log never compacts, snapshot
directories are never reclaimed, old encryption keys can never be purged, and
the at-rest barrier is never used. If you turn them off, calling
`RequestSnapshot` is your job.

Snapshotting more often shortens recovery and compacts the log sooner, at the
cost of writing a full image more often. Snapshotting less often does the
reverse and makes followers more likely to need a full transfer instead of a
log replay. `CompactionOverhead` softens that: the entries it keeps let a
slightly-behind follower catch up from the log.

## Take a snapshot on demand

```go
index, err := host.SyncRequestSnapshot(ctx, shardID, quicraft.DefaultSnapshotOption)
```

`RequestSnapshot` is the asynchronous form and returns a `*RequestState`. Only
one snapshot per shard runs at a time; a second request gets
`ErrSnapshotInProgress`. A request issued after the previous one has returned
is always admitted.

To keep a different amount of log for this one snapshot:

```go
opt := quicraft.SnapshotOption{
    CompactionOverhead:         50000,
    OverrideCompactionOverhead: true, // required: without it the field is ignored
}
```

## Export a snapshot

To a directory, as part of taking one:

```go
opt := quicraft.SnapshotOption{ExportPath: "/var/backups/shard-1"} // absolute
index, err := host.SyncRequestSnapshot(ctx, shardID, opt)
```

The directory is created if needed, and the snapshot is also written to the
normal snapshot directory, so log compaction still happens. Leave `Exported`
alone; it follows from `ExportPath`.

To a stream, from the latest snapshot:

```go
err := host.ExportSnapshot(ctx, shardID, w)
```

The stream is a header, the plaintext snapshot data, and a trailer with the
data's length and a hash covering header and data. The trailer is written last
and only when the export succeeds, so the output of a failed export can never be
imported. On any error, discard what `w` received anyway.

## Import a snapshot

**Read this first.** `ImportSnapshot` rewrites the replica's membership. It is
an offline, deliberate, destructive operation.

```go
err := host.ImportSnapshot(ctx, shardID, r,
    quicraft.WithImportDataDir(dir),
    quicraft.WithImportMembership(members),
    quicraft.WithImportMemberIdentities(identities),
)
```

All three options are optional. Use `WithImportMembership` to restore into a
different set of replicas, and `WithImportMemberIdentities` to supply the node
identity (what each node's `host.NodeIdentity()` reports) of any member the
artifact has none for: on a host with a transport, every member needs one.

The refusals are the safety net:

| Error | Means |
|---|---|
| `*ImportSnapshotDirExistsError` | the destination already exists; it will not overwrite it |
| `*ImportSnapshotOutOfDateError` | the snapshot is older than what this replica holds; it will not move you backwards |
| `*ImportDataDirMismatchError` | `WithImportDataDir` names a directory that is not this replica's |
| `*ImportMembershipInvalidError` | the membership or identities you passed cannot be used |
| `*ReplicaRetiredError` | this replica ID was retired by `RemoveData`; use a new one |
| `*SnapshotArtifactError` | the artifact is malformed, truncated, damaged or has no trailer. Nothing was adopted. A damaged payload also matches `ErrSnapshotCorrupt`. |

If you find yourself wanting to work around one of these, stop and check which
replica you are operating on.

## Re-key a snapshot

```go
epoch, err := quicraft.ReencryptSnapshotFile(srcPath, dstPath,
    shardID, replicaID, index, srcBarrier, dstBarrier)
```

Both barriers must be unsealed. A nil barrier means that side is plaintext; a
sealed one is not plaintext and fails the call.

`shardID`, `replicaID` and `index` must be the identity the snapshot was sealed
under. They are part of what each frame authenticates, and there is no weaker
fallback.

**Record the returned epoch as the migrated snapshot's `Epoch`.** It is the
destination's lowest epoch across all frames, and the key-purge floor uses it
to protect every frame.

Handle this case specifically:

```go
epoch, err := quicraft.ReencryptSnapshotFile(srcPath, dstPath,
    shardID, replicaID, index, srcBarrier, dstBarrier)
if err != nil {
    var cnd *quicraft.SnapshotCommittedNotDurableError
    if errors.As(err, &cnd) {
        // The rename succeeded: dstPath holds the migrated snapshot at
        // cnd.Epoch, and only the directory sync failed. Record cnd.Epoch,
        // then check the destination again.
        recordEpoch(cnd.Epoch)
    }
    return err
}
recordEpoch(epoch)
```

The usual `if err != nil { return err }` would drop that epoch, and the next
key purge could then orphan the snapshot's key.

## Tune snapshot transfer

```go
tc.MaxSnapshotReceiveRate = 256 << 20      // bytes per second
tc.MaxConcurrentSnapshotReceives = 4
tc.MaxSnapshotReceiveMemory = 1 << 30      // bytes
```

A recovering follower uses whatever bandwidth you allow. Snapshot transfers run
outside the stream pool, so they do not block ordinary replication, but they do
compete for the link.

## Watch the right things

1. **Time since the newest snapshot, per shard.** A shard that stopped
   snapshotting will not compact its log, and its old keys can never be purged.
   This matters most and is the one most often left off dashboards.
2. **Rejected snapshot transfers**, by reason.
3. **Any `ErrSnapshotCorrupt`**: a damaged or truncated snapshot, not a
   transient error.

## Troubleshooting

**`ErrSnapshotBarrierSealed`**: the barrier is configured but sealed. This is
deliberate: carrying on would copy ciphertext and record it as plaintext, or
store a snapshot unencrypted that must be encrypted. Unseal the barrier.

**`ErrSnapshotEncryptedBarrierUnavailable`**: an encrypted snapshot and no
barrier to open it. Usually a data directory moved to a node configured without
encryption, or with a different barrier.

**`*SnapshotTooLargeError`**: the state machine's output exceeds
`MaxSnapshotSize` (4 GiB by default), measured after compression and before
encryption, so the limit means the same with or without a barrier. Either your
state really is that large, in which case raise the limit and consider a
`DiskStateMachine`, or `SaveSnapshot` writes more than it needs.

**`ErrSnapshotCorrupt` at recovery or start**: the snapshot data does not match
its recorded length and hash, or its metadata is missing or unreadable, so
nothing can vouch for it. There is no way to load it unverified. Re-sync the
replica from its peers.

**`OnSnapshotRejected` with a non-nil `SnapshotInfo.Err`**: a request was
refused or abandoned, a `SaveSnapshot` panicked (the requester gets
`*SnapshotFailedError` wrapping `*SnapshotSavePanicError`), an inbound transfer
was rejected, or a recovery failed before your `RecoverFromSnapshot` was
called. None of these fails the shard; a rejected recovery is retried with
backoff. A save that returns an error fires no event: watch the WARN log line
or `OnSnapshotCreated` going quiet.

**`OnShardFailed` with `*SnapshotRecoveryExhaustedError`**: five recovery
attempts in a row failed before reaching your state machine. `Err` is the last
cause. Fix that cause (usually the barrier or a damaged artifact), then stop and
restart the shard. A failure or panic inside `RecoverFromSnapshot` fails the
shard on the first attempt.

**A snapshot fails to decrypt after losing trailing bytes**: that is the
truncation check working. Each frame records whether it is the last one, so a
snapshot that lost frames off the end, or had frames added, fails instead of
restoring a shorter state machine. Re-sync from another replica rather than
repairing the file.

**Recovery succeeds but the state is wrong**: check that the shard, replica and
index match what the snapshot was sealed under. A mismatch normally fails
decryption, so wrong state without an error points at your
`SaveSnapshot`/`RecoverFromSnapshot` round trip.

**Old keys are never purged**: some snapshot operation left its epoch marked in
use. Every operation releases what it marks on every path, including error
paths, so a leak is a defect worth reporting.

**A follower keeps needing full transfers instead of log replay**: raise
`CompactionOverhead` so more log survives compaction, or snapshot less often.

**Snapshots stopped after setting `SnapshotEntries = 0`**: they did not stop;
zero selected the default. Use `config.SnapshotsDisabled`.

**A snapshot save stalls the cluster**: it should not. Snapshot work runs on its
own pool of workers, separate from consensus. If consensus stalls during a save,
look at disk saturation.
