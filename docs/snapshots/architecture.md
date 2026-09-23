# Snapshots — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft snapshot creation, streaming,
> import/export and re-encryption — `pkg/snapshot_*.go`,
> `pkg/internal/engine/snapshot_pool.go`, `pkg/internal/engine/snapshot_meta.go`,
> the snapshot paths of `pkg/internal/transport` and `pkg/logdb` ·
> Last verified: 2026-09-15
> Conflicts with this document are defects. Fix them here first.

## The problem

A Raft log that is never truncated grows without bound, and a follower that
falls far enough behind can never catch up by replaying it. Snapshots solve
both: a point-in-time image of the state machine lets the log below it be
compacted, and lets a lagging replica be brought up to date in one transfer
instead of a million entries.

Both uses put the same requirement on the mechanism: the image has to be
**consistent with a specific log index**, and it has to survive a crash halfway
through being written or received.

## The shape chosen

A snapshot is a directory per `(shard, replica, index)` holding a data file the
state machine wrote and a metadata record describing it:

```
<NodeHostDir>/snapshots/shard-<N>/replica-<N>/snapshot-<index>/
    snapshot.dat     the state machine's own bytes (optionally compressed,
                     optionally encrypted as length-prefixed frames)
    snapshot.meta    index, term, DataLen, DataHash, membership, epoch, ...
```

- **The state machine owns the contents.** `SaveSnapshot` gets an `io.Writer`
  and this library does not look inside.
- **Membership travels with the snapshot**, because a replica recovering from
  one has to learn the configuration as of that index — it cannot read it from a
  log it does not have.
- **Every write is atomic**: temp file, fsync the file, rename, **fsync the
  directory**. The last step matters because without it a crash can roll the
  directory entry back to the deleted temp name even though the bytes were
  fsynced.
- **Snapshot work runs on its own pool** — `max(NumWorkers/2, 2)` workers —
  rather than on the step, commit or apply workers, so a multi-gigabyte save
  cannot stall consensus.

Snapshots are triggered automatically every `SnapshotEntries` applied entries
(default 100000), or explicitly by `RequestSnapshot`.

**`SnapshotEntries` zero does not disable it.** Zero means "unset" and selects
the default; `config.SnapshotsDisabled` is the value that turns it off. Never
snapshotting also means never reclaiming snapshot directories, never purging an
old epoch's DEK, and never invoking the at-rest barrier.

## Rejected designs

### Rejected: copy-on-write of the library's own state

The library does not know what the state machine's state *is*. Tiers 1 and 2
solve this in the state machine instead: `PrepareSnapshot` captures a
point-in-time context and `SaveSnapshot` writes it without blocking `Update`.
Tier 0 pays for the simpler interface by serializing.

### Rejected: encrypting snapshots for transport

Snapshots cross the wire as **plaintext over the mTLS session**, and the
receiver re-encrypts under its **own** barrier. The alternative — shipping the
leader's ciphertext — would require every replica to share key material, which
is precisely the property the per-node barrier design refuses. Nodes cannot
decrypt each other's at-rest data, and that stays true through a snapshot
transfer.

### Rejected: write plaintext, then encrypt in place

The save used to write the state machine's bytes to the canonical
`snapshot.dat` in the clear, fsync them, and encrypt the file in place
afterwards. Between the fsync and the rename a complete plaintext state machine
was durable under the name every consumer, backup and operator reads — the one
path in this feature that had that window, while the receive path always sealed
inline from memory (#116). The writer now seals each 64 KB frame as the state
machine hands it over; the plaintext exists only in the writer's buffer.

### Rejected: in-place decryption for recovery

Renaming plaintext over `snapshot.dat` is neither crash- nor retry-safe: a
recovery failure or a crash after the rename leaves plaintext on disk while
LogDB still records `Epoch > 0`, so **every subsequent restart re-attempts
decryption on plaintext and fails permanently**, losing an otherwise-valid
snapshot. Decryption therefore writes a **sibling temp file** and leaves the
canonical artifact intact.

### Rejected: an epoch-only fallback when frame authentication fails

Snapshot frames bind their position into the AAD. A reader that fails to
authenticate must **never** retry without the context — that retry would readmit
exactly the frame-splicing the binding exists to prevent.

## At-rest encryption of snapshots

When a barrier is configured, `snapshot.dat` is a sequence of length-prefixed
frames, 64 KB of plaintext each:

```
[ChunkLen:4 LE | EncryptedChunk₁][ChunkLen:4 LE | EncryptedChunk₂]...

EncryptedChunk = [Epoch:8 BE | AlgID:1 | Nonce:N | Ciphertext | Tag:16]
```

Each frame is self-describing for decryption: it carries its own epoch and
nonce, so a file written across a key rotation still reads.

### Positional binding

Epoch-only AAD would let an attacker reorder chunks, substitute chunks from
another snapshot at the same epoch, or truncate the stream without failing
per-chunk authentication. Every frame therefore binds a positional context built
by `engine.NewSnapshotAAD` — `SnapshotFrameAADLen` is **40 bytes**:

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

Every field is fixed-width and big-endian with no delimiters, so no two distinct
identities produce the same bytes. This makes a wrong `ChunkSeq` (reordering), a
wrong `SnapshotIndex` (cross-snapshot substitution) and a wrong
`ShardID`/`ReplicaID` (cross-replica substitution) all fail authentication.

`NewSnapshotAAD` asks the barrier `SupportsContextAAD()` first. A barrier that
cannot bind must not be handed a context: the write **fails closed** rather than
silently dropping the binding.

### Truncation is detected in both places it can happen, by different means

- **In transit**: the stream header declares a `ChunkCount` and the receiver
  refuses a transfer where `len(chunks) != hdr.ChunkCount`, where the count is
  zero, or where the stream was marked truncated. A separate
  `maxAdmissibleChunkCount` bound rejects an inflated declared count *before*
  any memory is committed.
- **At rest**: the `Final` flag. Exactly one frame of every sealed artifact is
  sealed as the last: the frame writer holds a full chunk back until more
  plaintext proves it is not, and `Close` always emits a terminal frame, an
  empty one if need be. `engine.OpenSnapshotFrames`, the only reader, reads one
  frame ahead and authenticates the frame after which the file ends as the
  last. A truncated artifact therefore fails on the frame it now ends with,
  which was sealed non-terminal; frames spliced after a terminal frame from a
  longer artifact of the same identity fail on that terminal frame; and an
  artifact truncated to nothing is refused as `ErrSnapshotCorrupt`, since a
  sealed artifact always has at least one frame. A barrier that cannot bind
  caller context gets no positional binding and so only the zero-frame rule.

### The epoch-pinning window

`EncryptWithEpochPinnedContext` seals a frame **and** registers its epoch
in-flight under one lock. Without that atomicity, a concurrent `Rotate` plus
`PurgeEpochsBefore` between the seal and the pin can drop the DEK of a frame
already written into the artifact. The method is therefore part of the
`Barrier` interface, not an optional capability: until 0.14.0 a barrier without
it fell back to pin-before / encrypt / pin-after, which left that window narrow
rather than closed, and a custom barrier that missed the optional method was
exposed to it.

## Data flow

```
 SAVE
   apply worker: appliedIndex crosses SnapshotEntries
        ▼ snapshot pool worker
   PrepareSnapshot (tiers 1,2) ─▶ SM.SaveSnapshot(w)      [into .save-* stage]
        w = [snappy ─▶] snapshotFrameWriter ─▶ snapshot.dat
            64 KB of plaintext per frame, sealed under the pinned epoch
            AS IT IS WRITTEN: no plaintext is ever durable on disk (#116)
        ▼ fsync ─▶ MaxSnapshotSize on the PLAINTEXT the writer consumed
   snapshot.meta (Epoch = lowest frame epoch) ─▶ [export]
        ▼ under the node's write lease: rename stage ─▶ snapshot-<index>
   LogDB.SaveSnapshot ─▶ epoch pins released ─▶ log compaction below index

 TRANSFER
   leader: decryptSnapshotToSibling ─▶ plaintext temp
        ─▶ 2 MB chunks over QUIC (mTLS) ─▶ follower
   follower: reassemble ─▶ re-encrypt under the RECEIVER'S barrier
        ─▶ staged snapshot.dat + snapshot.meta, fsync ─▶ publish the dir
        ─▶ InstallSnapshot to raft ─▶ await raft's verdict
             adopted:  LogDB.SaveSnapshot ─▶ log floor ─▶ RequestSnapshotRecovery
             refused / declined / superseded: discard the unrecorded dir
        ─▶ SnapshotReceived back to the leader's inbox

 RECOVER
   verify DataLen + DataHash (xxhash64 of the file AS STORED)
        ─▶ decryptSnapshotToSibling ─▶ Snappy decompress if configured
        ─▶ SM.RecoverFromSnapshot(r)     [always plaintext]
        ─▶ update lastApplied, lastSnapshotIndex ─▶ remove the temp file
```

**The receive is two-phase.** Everything durable about a received snapshot
beyond its own directory — the LogDB record, the log floor raised to it,
reclaiming older directories, the state-machine recovery — waits for raft to
ADOPT the install, which the step worker reports back per install. Raft can
refuse one that passed every host check (its sender deposed while the stream
was in flight, a position or membership out of bounds) or decline one it is
already past; either way the unrecorded directory is discarded and nothing
else changed. Until 0.14.0 those steps ran on delivery, so a refused snapshot
stayed recorded with the floor raised under it and a recovery queued that the
log could never catch up to, and the shard failed. Once raft has adopted the
install the artifact is never rolled back: a record that cannot then be
written fails the shard instead, and a restart replays the log.

The verdict is bound to one shard incarnation. The install is injected into,
and recovery queued on, exactly the node the verdict is awaited on — never
whichever node holds the shard ID by then. A node that stops or fails, or a
step-worker panic that interrupts the install's own step (which also fails
the node), answers **abandoned**, and that does not prove raft never stepped it, so the directory
is kept rather than discarded: if the stopped incarnation's commit pipeline
records it, the next start recovers from it; if not, it is an orphan the next
receive or GC reclaims. The next receive cannot race that record: a receive
for a stopping shard is refused before staging anything, its publish and its
post-adoption writes run under the node's write lease, and `UnloadNode` does
not return until the stopped incarnation can no longer write LogDB (see the
commit fence in the multi-group-host engineering doc). Installs a panic
dropped before they were stepped answer **superseded** and are discarded; an
install whose step the panic interrupted fails the shard, because raft's state
after a partial restore is unknown.

`DataHash` is computed over the file **as stored** — before decryption and
before decompression — so it verifies the artifact rather than its
interpretation.

Verification is skipped for a `snapshot.meta` written before those fields
existed (`DataHashRecorded` is false). Such a record is not an error — it is a
pre-`0.5.0` artifact and recovers unverified — but a truncated file from that
era still recovers as if complete. Snapshots taken by this build are verified.

## Trust boundaries

- **A received snapshot is untrusted input.** The sender is mTLS-authenticated
  and a committed member; its payload is not. Chunk count, declared sizes and
  memory are all bounded before anything is committed —
  `MaxConcurrentSnapshotReceives`, `MaxSnapshotReceiveMemory`,
  `MaxSnapshotReceiveRate`, `maxAdmissibleChunkCount`.
- **The state machine always sees plaintext** during `RecoverFromSnapshot`,
  whether or not encryption is on.
- **Snapshot transport is plaintext under TLS.** Confidentiality in transit is
  the transport's; confidentiality at rest is the barrier's, and each node's is
  its own.
- **A snapshot carries membership**, so importing one changes who this replica
  believes the cluster is. `ImportSnapshot` is therefore a deliberate,
  offline-only operation — see the scope fence.

## Component boundaries

- **`pkg/internal/engine/snapshot_pool.go`** — the pool, `saveSnapshot`,
  `decryptSnapshotToSibling`, recovery.
- **`pkg/internal/engine/snapshot_frame_writer.go`** — `snapshotFrameWriter`,
  the `io.Writer` the state machine saves into: frames, epoch pins, the lowest
  frame epoch, and the plaintext count the size limit is measured on.
- **`pkg/internal/engine/snapshot_meta.go`** — `SnapshotAAD`,
  `NewSnapshotAAD`, `SnapshotFrameAADLen`, the metadata record.
- **`pkg/snapshot_*.go`** — the public `SnapshotOption`, `ImportOption`,
  `ReencryptSnapshotFile`, and the snapshot error vocabulary.
- **`pkg/internal/transport`** — chunked send and bounded receive.
- **`pkg/logdb`** — snapshot metadata persistence, and the compaction floor a
  snapshot establishes.
- **`pkg/sm`** — `SaveSnapshot`, `PrepareSnapshot`, `RecoverFromSnapshot`.

This feature is part of the go-quicraft capability row covering Raft consensus
and log replication in `docs/00 §R`.

## What this feature deliberately does not do

- **No snapshot contents.** The state machine writes them; this library never
  parses them.
- **No incremental or differential snapshots.** Each one is a full image.
- **No automatic scheduling beyond an applied-entry count.** No time-based
  cadence, because a wall clock would make replicas snapshot at different log
  positions.
- **No encryption in transit at this layer.** TLS covers it.
- **No cross-node key sharing.** The receiver re-encrypts under its own barrier.
- **No at-rest tail-truncation detection.** Stated as a limit, not a footnote.
- **No automatic import.** `ImportSnapshot` is an explicit, offline operation
  that rewrites a replica's identity and membership.
- **No backup or retention policy.** Which exported snapshots to keep, and
  where, belongs to the layer that owns the data directory.
