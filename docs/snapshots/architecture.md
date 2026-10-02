# Snapshots — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft snapshot creation, streaming,
> import/export and re-encryption: `pkg/snapshot_*.go`, the snapshot paths of
> `pkg/host.go` and `pkg/messagehandler.go`, `pkg/internal/engine/snapshot_*.go`,
> the snapshot paths of `pkg/internal/transport` and `pkg/logdb` ·
> Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

## The problem

A Raft log that is never truncated grows without bound, and a follower that
falls far enough behind can never catch up by replaying it. Snapshots solve
both: a point-in-time image of the state machine lets the log below it be
compacted, and brings a lagging replica up to date in one transfer instead of a
million entries.

Both uses need the same two things from the image: it must be consistent with a
specific log index, and it must survive a crash halfway through being written
or received.

## The shape chosen

A snapshot is a directory per `(shard, replica, index)` holding a data file the
state machine wrote and a metadata record describing it:

```
<NodeHostDir>/snapshots/shard-<N>/replica-<N>/snapshot-<index>/
    snapshot.dat     the state machine's bytes (optionally compressed,
                     optionally encrypted as length-prefixed frames)
    snapshot.meta    index, term, epoch, compression, DataLen, DataHash, ...
```

A shard with `Config.DataDir` set roots its tree at `<DataDir>/shard-<N>/`
instead.

- **The state machine owns the contents.** `SaveSnapshot` gets an `io.Writer`,
  and this library never looks inside.
- **Membership travels with the snapshot.** A replica recovering from one must
  learn the configuration as of that index, and it cannot read it from a log it
  does not have.
- **Every write is atomic**: temp file, fsync the file, rename, fsync the
  directory. Without the last step a crash can roll the directory entry back to
  the temp name even though the bytes were synced.
- **Integrity is checked against the file as stored.** `DataLen` and `DataHash`
  (xxhash64 of `snapshot.dat` before decryption and decompression) are recorded
  in `snapshot.meta` and checked on recovery, on send and on export.
- **Snapshot work runs on its own pool** of `max(NumWorkers/2, 2)` workers, not
  on the step, commit or apply workers, so a multi-gigabyte save cannot stall
  consensus.

Snapshots are taken automatically every `SnapshotEntries` applied entries
(default 100000), or on request with `RequestSnapshot`.

**`SnapshotEntries` zero does not disable it.** Zero means "unset" and selects
the default; `config.SnapshotsDisabled` turns it off. Never snapshotting also
means never compacting the log, never reclaiming snapshot directories, never
purging an old epoch's DEK, and never invoking the at-rest barrier.

## Rejected designs

### Rejected: copy-on-write of the library's own state

The library does not know what the state machine's state is. Tiers 1 and 2
solve this in the state machine: `PrepareSnapshot` captures a point-in-time
context, and `SaveSnapshot` writes it without blocking `Update`. Tier 0 pays for
its simpler interface by serializing.

### Rejected: encrypting snapshots for transport

Snapshots cross the wire as plaintext inside the mTLS session, and the receiver
re-encrypts under its own barrier. Shipping the leader's ciphertext instead
would require every replica to share key material, which is the property the
per-node barrier design refuses. Nodes cannot decrypt each other's at-rest data,
and that stays true through a snapshot transfer.

### Rejected: write plaintext, then encrypt in place

Writing the state machine's bytes to `snapshot.dat` in the clear and encrypting
the file afterwards leaves a complete plaintext state machine durable under the
name every consumer, backup and operator reads. The save seals each 64 KiB frame
as the state machine hands it over, so plaintext exists only in the writer's
buffer.

### Rejected: decrypting to a named file, or in place

Recovery and the transport send need plaintext to read from. Renaming plaintext
over `snapshot.dat` is neither crash- nor retry-safe: a failure after the rename
leaves plaintext on disk while LogDB still records `Epoch > 0`, so every later
restart tries to decrypt plaintext, fails, and the snapshot is lost. A named
plaintext scratch file is no better: a crash before it is removed leaves a
complete, readable copy of the state machine on disk.

Recovery and send therefore decrypt into an **anonymous scratch file**, created
in the replica's snapshot directory and unlinked before a byte is written. The
plaintext never has a name, and closing the handle is the whole cleanup. If the
unlink fails, the operation fails before anything is decrypted
(`ErrScratchNotUnlinked`) and is retried later; an empty name left behind is
removed by the stale-scratch sweep. The canonical encrypted `snapshot.dat` is
never modified. The one named decrypt is the `ExportPath` copy, which writes a
plaintext artifact by contract and decrypts inside the save's staging directory.

### Rejected: an epoch-only fallback when frame authentication fails

Snapshot frames bind their position into the AAD. A reader that fails to
authenticate never retries without that context, because the retry would let
in exactly the frame splicing the binding exists to stop.

## At-rest encryption

With a barrier configured, `snapshot.dat` is a sequence of length-prefixed
frames of 64 KiB of plaintext each:

```
[ChunkLen:4 LE | EncryptedChunk₁][ChunkLen:4 LE | EncryptedChunk₂]...

EncryptedChunk = [Epoch:8 BE | AlgID:1 | Nonce:N | Ciphertext | Tag:16]
```

Each frame carries its own epoch and nonce, so a file written across a key
rotation still reads.

### Positional binding

Epoch-only AAD would let an attacker reorder frames, substitute frames from
another snapshot at the same epoch, or truncate the stream. Every frame
therefore binds a positional context built by `engine.NewSnapshotAAD`;
`SnapshotFrameAADLen` is 40 bytes:

```
 off  size  field
   0     7  magic "QRSNAP1"
   7     8  ShardID
  15     8  ReplicaID
  23     8  SnapshotIndex
  31     8  ChunkSeq          (0-based ordinal within snapshot.dat)
  39     1  Final             (1 on the artifact's last frame, else 0)

AAD = [Epoch:8 little-endian] || context      = 48 bytes
```

The context fields are fixed-width and big-endian with no delimiters, so no two
positions produce the same bytes. A wrong `ChunkSeq` (reordering), `SnapshotIndex`
(cross-snapshot substitution) or `ShardID`/`ReplicaID` (cross-replica
substitution) fails authentication.

`NewSnapshotAAD` asks the barrier `SupportsContextAAD()` first. A barrier that
cannot bind is not handed a context; the write fails closed instead of dropping
the binding.

### Truncation is caught where it can happen, by different means

- **In transit.** The stream header declares `ChunkCount`, and the receiver
  refuses a transfer whose chunk count differs, is zero, or was marked
  truncated. `maxAdmissibleChunkCount` rejects an inflated count before any
  memory is committed.
- **At rest.** The `Final` flag. Exactly one frame of every sealed artifact is
  sealed as the last: the writer holds a full chunk back until more plaintext
  proves it is not the last, and `Close` always emits a terminal frame, an empty
  one if need be. `engine.OpenSnapshotFrames`, the only reader, reads one frame
  ahead and authenticates the frame the file ends on as the last. A truncated
  artifact fails on the frame it now ends with, frames appended after a terminal
  frame fail on that terminal frame, and an artifact truncated to nothing is
  `ErrSnapshotCorrupt`, since a sealed artifact always has at least one frame. A
  barrier that cannot bind context gets no positional binding and so only the
  zero-frame rule.

### Epoch pinning

`EncryptWithEpochPinnedContext` seals a frame and registers its epoch as in use
under one lock. Without that, a concurrent `Rotate` plus `PurgeEpochsBefore`
between the seal and the pin could drop the DEK of a frame already written into
the artifact. It is a required method of the `Barrier` interface, not an
optional capability, so no barrier can fall back to a pin-before/pin-after
sequence that only narrows the window.

## The portable export artifact

`ExportSnapshot` writes, and `ImportSnapshot` reads, an artifact that stands on
its own between two hosts with unrelated barrier keys:

```
header   magic "QRSNAP01" | version:u32 (2) | Index | Term | OnDiskIndex |
         Compression:u16 | membLen:u32 | Membership[membLen]
payload  the snapshot data, PLAINTEXT (decrypted, never decompressed)
trailer  magic "QRSNTRLR" | payloadLen:u64 | hash:u64 (xxhash64 of header || payload)
```

The trailer makes the artifact verifiable on its own. The export streams, so
the length and hash are not known when the header is written; they follow the
payload instead. They are computed over the bytes as written, and the hash
covers the header because a flipped `Index` or `Term` that still parses would
otherwise import a sound payload at the wrong log position.

The trailer is written last, only after the export's own check of the source
against its `snapshot.meta` has passed. An export that fails never writes one.
That matters most for an encrypted export: its frames authenticate one at a
time, so all its plaintext has reached the writer before the recorded-hash
check can fail it, and the missing trailer is what keeps that output from being
importable.

`ImportSnapshot` holds back the last 24 bytes of the stream while it copies the
payload into its staging directory, hashes what it copies, and checks both
against the trailer when the stream ends. The check runs inside the copy, so a
mismatch fails the staged write and the stage is discarded before anything is
synced, published, recorded in LogDB or seen by raft. A truncated artifact, or
one without a trailer, ends in payload bytes where the trailer magic should be,
and is refused as `*SnapshotArtifactError`.

## Data flow

```
 SAVE
   apply worker: appliedIndex crosses SnapshotEntries
        ▼ snapshot pool worker
   PrepareSnapshot (tiers 1, 2) ─▶ SM.SaveSnapshot(w)          [into a .save-* stage]
        w = [snappy ─▶] snapshot frame writer ─▶ snapshot.dat
            64 KiB of plaintext per frame, sealed as it is written
        ▼ fsync ─▶ MaxSnapshotSize checked on the bytes the writer consumed
   hash the file ─▶ snapshot.meta (DataLen, DataHash, Epoch = lowest frame epoch)
        ─▶ [ExportPath copy]
        ▼ under the node's write lease: rename stage ─▶ snapshot-<index>
   LogDB.SaveSnapshot ─▶ epoch pins released ─▶ log compaction below index

 TRANSFER
   leader: open snapshot.dat, checking DataLen/DataHash as it reads
        [encrypted: decrypt into an anonymous scratch]
        ─▶ 2 MiB chunks over QUIC (mTLS); the final chunk is withheld until
           the check passes ─▶ follower
   follower: reassemble ─▶ re-encrypt under the RECEIVER'S barrier
        ─▶ staged snapshot.dat + snapshot.meta, fsync ─▶ publish the directory
        ─▶ InstallSnapshot to raft ─▶ await raft's verdict
             adopted:  LogDB.SaveSnapshot ─▶ log floor ─▶ state-machine recovery
             refused / declined / superseded: discard the unrecorded directory
        ─▶ SnapshotReceived back to the leader

 RECOVER
   snapshot.meta must exist and carry DataLen + DataHash; verify them
        ─▶ [encrypted: decrypt into an anonymous scratch] ─▶ Snappy decompress if recorded
        ─▶ SM.RecoverFromSnapshot(r)     [always plaintext]
        ─▶ update lastApplied ─▶ close the scratch
```

**The receive is two-phase.** Everything durable about a received snapshot
beyond its own directory (the LogDB record, the raised log floor, reclaiming
older directories, state-machine recovery) waits for raft to adopt the install,
which the step worker reports back per install. Raft can refuse one that passed
every host check (its sender was deposed while the stream was in flight, or a
position or membership is out of bounds) or decline one it is already past.
Either way the unrecorded directory is discarded and nothing else changes. Once
raft has adopted the install, the artifact is never rolled back: a record that
cannot then be written fails the shard instead, and a restart replays the log.

The verdict is bound to one shard incarnation. The install is injected into, and
recovery queued on, exactly the node the verdict is awaited on, never whichever
node holds the shard ID by then.

- A node that stops or fails, or a step-worker panic that interrupts the
  install's own step (which also fails the node), answers **abandoned**. That
  does not prove raft never stepped it, so the directory is kept: if the stopped
  incarnation's commit pipeline records it, the next start recovers from it;
  otherwise it is an orphan the next receive or GC reclaims.
- The next receive cannot race that record. A receive for a stopping shard is
  refused before it stages anything, its publish and post-adoption writes run
  under the node's write lease, and `UnloadNode` does not return until the
  stopped incarnation can no longer write LogDB (see the commit fence in
  [the multi-group host engineering doc](../multi-group-host/engineering.md)).
- Installs a panic dropped before they were stepped answer **superseded** and
  are discarded. An install whose step the panic interrupted fails the shard,
  because raft's state after a partial restore is unknown.

**Every recorded snapshot is verified.** A save, receive, import or export that
cannot hash its artifact fails before anything is published, recorded or
compacted. A recorded snapshot whose `snapshot.meta` is missing, unreadable,
malformed or lacks `DataLen`/`DataHash` fails closed with `ErrSnapshotCorrupt`:
nothing can vouch for its artifact, and there is no unverified path.

## Trust boundaries

- **A received snapshot is untrusted input.** The sender is mTLS-authenticated
  and a committed member; its payload is not trusted. Chunk count, declared
  sizes and memory are bounded before anything is committed:
  `MaxConcurrentSnapshotReceives`, `MaxSnapshotReceiveMemory`,
  `MaxSnapshotReceiveRate`, `maxAdmissibleChunkCount`.
- **The state machine always sees plaintext** in `RecoverFromSnapshot`, whether
  or not encryption is on.
- **Transport is plaintext under TLS.** Confidentiality in transit belongs to the
  transport; confidentiality at rest belongs to each node's own barrier.
- **Plaintext on disk has no name.** Decryption for recovery and send goes into
  an unlinked scratch file, so a crash cannot leave a readable copy.
- **A snapshot carries membership**, so importing one changes who this replica
  believes the cluster is. `ImportSnapshot` is therefore a deliberate,
  offline-only operation.
- **No at-rest tail-truncation detection without positional binding.** The
  `Final` flag needs a barrier that binds context; a plaintext `snapshot.dat` is
  protected by `DataLen`/`DataHash` only.

## Component boundaries

| Location | Holds |
|---|---|
| `pkg/internal/engine/snapshot_pool.go` | the pool, the save, recovery, decrypt-to-scratch, the DEK purge floor, snapshot GC |
| `pkg/internal/engine/snapshot_frame_writer.go` | the frame writer the state machine saves into: frames, epoch pins, the lowest frame epoch, the plaintext count the size limit uses |
| `pkg/internal/engine/snapshot_frame_reader.go` | `OpenSnapshotFrames`, the only frame reader |
| `pkg/internal/engine/snapshot_meta.go` | `SnapshotMeta`, `SnapshotAAD`, `NewSnapshotAAD`, `SnapshotFrameAADLen` |
| `pkg/internal/engine/snapshot_integrity.go` | `DataLen`/`DataHash` hashing and verification |
| `pkg/internal/engine/snapshot_scratch.go` | `CreateAnonymousScratch` |
| `pkg/snapshot_option.go`, `pkg/snapshot_reencrypt.go`, `pkg/snapshot_artifact_trailer.go` | `SnapshotOption`, `ReencryptSnapshotFile`, the export trailer |
| `pkg/host.go`, `pkg/messagehandler.go` | request, export, import, send and receive |
| `pkg/internal/transport` | chunked send and bounded receive |
| `pkg/logdb` | snapshot metadata persistence, and the compaction floor a snapshot sets |
| `pkg/sm` | `SaveSnapshot`, `PrepareSnapshot`, `RecoverFromSnapshot` |

## What this feature deliberately does not do

- **No snapshot contents.** The state machine writes them; this library never
  parses them.
- **No incremental or differential snapshots.** Each one is a full image.
- **No scheduling beyond an applied-entry count.** No time-based cadence: a wall
  clock would make replicas snapshot at different log positions.
- **No encryption in transit at this layer.** TLS covers it.
- **No cross-node key sharing.** The receiver re-encrypts under its own barrier.
- **No automatic import.** `ImportSnapshot` is an explicit, offline operation
  that rewrites a replica's membership.
- **No backup or retention policy.** Which exported snapshots to keep, and
  where, belongs to the layer that owns the data directory.
