# QuicRaft Architecture Overview

## System Overview

QuicRaft (github.com/jeremyhahn/go-quicraft) is a multi-group Raft consensus
library in Go. A single `Host` manages thousands of independent Raft shards,
each with its own state machine, log, and membership. It provides:

- Multi-group Raft with per-shard configuration
- QUIC-only transport (no TCP fallback) with mutual TLS
- Custom fixed-layout binary wire format (no protobuf)
- Sharded WAL: 16 directory shards keyed by `shardID` hash, each with its own
  segment files, index and mutex
- Algorithm-agile AEAD encryption (AES-256-GCM or ChaCha20-Poly1305) with hardware auto-detection
- Session-based at-most-once proposal delivery
- Three state machine tiers: in-memory, concurrent, on-disk

What it does not do: peer discovery, quorum validation, and cluster bootstrap
orchestration are the caller's problem. See [Cluster Formation](#cluster-formation-lives-in-go-qrdb).

```
+---------------------------------------------------------------------+
|                                Host                                 |
|                                                                     |
|   +------------+    +------------+    +------------+                |
|   |  Shard 1   |    |  Shard 2   |    |  Shard N   |     ...        |
|   +------+-----+    +------+-----+    +------+-----+                |
|          |                 |                 |                      |
|          +-----------------+-----------------+                      |
|                            |                                        |
|                     +------+------+        +--------------------+   |
|                     |   Engine    |------->|     Listener       |   |
|                     | Step Commit |        |     Dispatcher     |   |
|                     |    Apply    |        | (user callbacks)   |   |
|                     +------+------+        +--------------------+   |
|                            |                                        |
|          +-----------------+-----------------+                      |
|          |                 |                 |                      |
|   +------+-----+   +-------+-----+   +-------+-----+                |
|   |    WAL     |   |  Transport  |   |  Snapshot   |                |
|   | (16-shard) |   |   (QUIC)    |   |    Pool     |                |
|   +------------+   +-------------+   +-------------+                |
+---------------------------------------------------------------------+
```

## Package Structure

**Public API**

- `pkg/` — Host, RequestState, Session, Membership, ShardInfo, errors, metrics
- `pkg/config/` — HostConfig, Config, TransportConfig, MTLSConfig, EventListener
- `pkg/sm/` — StateMachine, ConcurrentStateMachine, DiskStateMachine interfaces
- `pkg/sm/kv/` — Reference KV implementations: MemoryStore, ConcurrentStore
- `pkg/proto/` — Custom binary wire format: Entry, Message, MessageBatch, Snapshot
- `pkg/logdb/` — LogDB interface, LogReader, storage layer types
- `pkg/logdb/waldb/` — WAL-based LogDB (16 directory shards, segment files)
- `pkg/logdb/memdb/` — In-memory LogDB for testing
- `pkg/seal/` — Barrier (epoch DEK, HKDF, AEAD, rotation), algorithm selection
  and hardware detection, `SealingStrategy`/`ShareSplitter`/`ShareAccumulator`
- `pkg/crypto/` — Type aliases re-exporting `pkg/seal` for backward
  compatibility. No implementation lives here.
- `pkg/crypto/shamir/` — Pure GF(256) split/combine primitive. Share custody,
  M-of-N quorum and recovery ceremonies are owned by go-qrdb, not by this
  library.
- `pkg/batch/` — Proposal batching aggregator
- `pkg/enclave/` — Memory-protected key containers (memguard enclave, mlock)
- `pkg/writemode/` — Write durability modes

**Internal Implementation**

- `pkg/internal/engine/` — Step/commit/apply/snapshot workers, listener
  dispatcher, `Node` struct, `ReadyMap`
- `pkg/internal/raft/` — Raft protocol, `Peer`, log, in-memory log, membership,
  remotes, quiescence
- `pkg/internal/transport/` — QUIC transport, connection/stream pool, frame I/O,
  snapshot receive
- `pkg/internal/rsm/` — Replicated state machine adapter, session routing
- `pkg/internal/session/` — Client session manager (at-most-once delivery)
- `pkg/internal/registry/` — (shardID, replicaID) → address mapping
- `pkg/internal/netaddr/` — Address canonicalization so two spellings of one
  endpoint compare equal
- `pkg/internal/queue/` — `EntryQueue` (double-buffered MPSC), `SwapBuffer`
- `pkg/internal/server/` — Global in-memory-log rate limiter
- `pkg/internal/invariant/` — Debug assertions
- `pkg/internal/transport/revocation/` — Certificate revocation (CRL, OCSP)

**Compatibility Layers**

- `contrib/dragonboat/` — Dragonboat API compatibility layer

## Data Flow

### Proposal Path

```
Client
  |
  | h.Propose(ctx, shardID, cmd) or SyncPropose
  v
Host.Propose()
  | ctx already done? closed? draining?
  | Find nodeState (h.nodes sync.Map)
  | Reject witnesses (ErrInvalidOperation) and full disks (ErrDiskFull)
  | len(cmd) <= min(MaxProposalPayloadSize, MaxEntrySize)?
  | No leader known at all? → NotLeaderError (fail fast)
  v
Host.proposeEntry()
  | Global rate limiter, then per-shard rate limiter
  | proposalKey = replicaID<<proposalKeySeqBits | proposalKeySeq.Add(1)
  | entry.Key = proposalKey
  | rs = newRequestState(ns.requestPool, proposalKey, deadline)
  | ns.pendingProposals.Store(proposalKey, &pendingProposal{rs, deadline})
  | engNode.DeliverProposal([]proto.Entry{entry}) — MPSC proposalQ, non-blocking
  | engine.NotifyWork(shardID)
  v
Engine Step Worker (shardID % NumWorkers)
  | Drain proposalQ, inboxQ and readIndexQ into reusable buffers
  | peer.Handle(...) under node.raftMu
  | peer.GetUpdate() → proto.Update
  | Send FREE-ORDER messages now, before persist (Replicate, Heartbeat, ...)
  |   Ordered messages (VoteResp, HeartbeatResp) are held for the commit worker
  | commitPending.Store(true)
  | commitC ← {update, node}
  v
Engine Commit Worker
  | logdb.SaveState(batch) — persist entries + term/vote (5 attempts)
  | Send ORDERED messages now that the state they promise is durable
  | peer.Commit() — logReader.Append, advance markers, truncate inmem
  | applyC ← {update, node}
  v
Engine Apply Worker → per-shard goroutine (bounded by semaphore)
  | rsm.Apply(entries) → session routing, then user SM
  | callback.OnApplied(shardID, entries, results)
  | commitPending.Store(false); workSignal.Notify()
  v
Host.OnApplied() callback
  | For each entry: match entry.Key → pendingProposals[key]
  | rs.complete(Result{Value, Data})
  v
Client ← rs.Result() unblocks
```

The proposal key is not a bare counter. It packs the replica ID into the high
bits so that two hosts in the same shard cannot mint the same key: every host
counts 1, 2, 3, ..., so a plain sequence would let replica B complete its own
pending request the moment it applied replica A's unrelated entry carrying the
same key. See `Host.nextProposalKey`.

### Read Path (Linearizable)

```
Client
  |
  | h.SyncRead(ctx, shardID, query)
  v
Host.ReadIndex()
  | Witnesses rejected here (ErrInvalidOperation) — they store no log data
  | readKey = readKeySeq.Add(1)   [dedicated counter, not proposalKeySeq]
  | ns.pendingReads[readKey] = &pendingRead{rs}   (under ns.pendingReadsMu)
  | engNode.DeliverReadIndex(readKey) — MPSC readIndexQ
  | engine.NotifyWork(shardID)
  v
Engine Step Worker
  | Drain readIndexQ as a batch; the first key becomes the batchKey
  | callback.OnReadIndexBatched(shardID, batchKey, aliasKeys)  [BEFORE Handle]
  | peer.Handle(ReadIndex{Hint: batchKey})
  |   LeaseRead enabled and lease still valid → resolves inline, no heartbeat
  |   Otherwise the leader broadcasts a heartbeat carrying its own monotonic
  |   readIndexCtx (ctx 0 is reserved for plain heartbeats, so an ordinary
  |   heartbeat response can never satisfy a ReadIndex quorum)
  | On quorum of HeartbeatResp: ReadyToRead{Key: batchKey, Index: commitIndex}
  v
callback.OnReadyToRead(shardID, []ReadyToRead)
  | Resolve batchKey and every alias registered against it
  | If lastApplied >= readIndex: rs.complete immediately
  | Else: wait for OnApplied
  v
Host.SyncRead() continues
  | rs.Result() ← read index confirmed and applied
  | ns.rsm.Lookup(ctx, query) — reads the SM directly, no log traversal
  v
Client ← query result
```

Concurrent readers on one shard share a single heartbeat round. The step worker
drains the whole `readIndexQ` in one cycle, submits the first key to raft, and
registers the rest as aliases through `OnReadIndexBatched`. Alias registration
happens before `Peer.Handle` on purpose: on a single-node shard `Handle` resolves
the ReadIndex synchronously, so registering afterwards would let `OnReadyToRead`
complete and delete the batch key before the aliases were attached, stranding
every aliased caller until its context deadline.

Witnesses cannot serve a ReadIndex, but their heartbeat acknowledgements still
count toward the quorum that confirms one. See
[Witnesses and Membership](#witnesses-and-membership).

## Key Design Decisions

### Custom Binary Wire Format

All wire types (`Entry`, `Message`, `MessageBatch`, `Snapshot`) use fixed-layout
binary encoding (no protobuf, no code generation). Design properties:

- **Fixed field positions**: struct layout determines byte offsets, no type tags
- **Little-endian uint64**: all large integers are 8 bytes, no varints
- **Zero-copy Cmd**: `Entry.Cmd` aliases source buffer after unmarshal
- **Hand-written serialization**: MarshalTo/UnmarshalFrom methods, ~300 lines per type
- **Deterministic maps**: key-value pairs encoded in sorted order
- **Bounded sizes**: MaxEntrySize (8 MB), MaxBatchSize (64 MB), 10000 messages per batch

Fixed header sizes:

| Type         | Fixed header | Content                                |
|--------------|--------------|----------------------------------------|
| Entry        | 68           | Cmd (variable, length-prefixed)        |
| Message      | 88           | Entries + inline Snapshot              |
| MessageBatch | 24           | SourceAddress + Messages               |

`Entry` is eight uint64 fields plus a uint32 `CmdLen`. `Message` is eleven
uint64 fields; a heartbeat (no entries, no snapshot) encodes to 100 bytes and is
the minimum message on the wire. The MessageBatch header is `BinVer:8`,
`DeploymentID:8` and `SourceEpoch:8`, with `BinVer` first so a receiver can
reject a foreign wire version before parsing anything else.

See [Wire Format](../protocol/wire-format.md) for the full field tables.

### QUIC-Only Transport

No TCP fallback, and no `Transport` interface to swap one in. The Host holds a
concrete `*transport.QUICTransport` and wraps it in an internal
`transportSender` that satisfies the engine's `MessageSender`. Whenever the
transport is enabled, mTLS configuration is required — validation fails with
`MTLSConfigRequiredError` otherwise.

QUIC 1.0 (RFC 9000) buys:

- Built-in TLS 1.3, with optional 0-RTT reconnection (off by default; 0-RTT data
  is replayable)
- Multiplexed streams, so one stalled shard does not head-of-line-block others
- Stream 0 reserved for heartbeats, pre-opened per connection and reused across
  ticks. Heartbeats also ride a separate priority channel (`hbCh`) that is
  drained ahead of data, so a large replication batch cannot delay a heartbeat
  past the election timeout.
- `StreamPoolSize` (default 16) data streams per connection, growable to
  `MaxStreamPoolSize` (256), to reduce mutex contention
- Pooled byte buffers on send and receive paths
- `singleflight.Group` to coalesce concurrent dials to the same peer

Snapshot transfers carry a dedicated frame flag that routes them to a separate
handler, keeping multi-gigabyte streams out of the stream pool.

### Sharded WAL

The `waldb` package splits writes across `DefaultShardCount` = 16 directory
shards, selected by `shardID % 16`. The on-disk layout is `shard-0` through
`shard-15`, each holding its own segment files. Design:

- Each shard has its own active segment, entry index and mutex
- Segments are append-only, block-aligned, CRC32-checked
- Sharding exists to reduce fsync contention: one slow shard's fsync does not
  serialize behind another's
- Recovery reconstructs all in-memory state by replaying the segments
- An fsync failure poisons the whole DB (`ErrDBPoisoned`) rather than
  continuing, because a failed `fdatasync` may leave the kernel marking dirty
  pages clean without writing them

### Engine Worker Pipeline

Shards are partitioned across `N = NumWorkers` workers by `shardID % NumWorkers`.
Three worker tiers plus a snapshot pool and a listener dispatcher:

```
Step Workers (N)   — tick, drain proposalQ/inboxQ/readIndexQ, produce Updates
       |
   Commit Channels (CommitCBufferSize, default 64, per worker)
       |
Commit Workers (N) — persist entries to LogDB, Peer.Commit()
       |
   Apply Channels (max(CommitCBufferSize*4, 256), per worker)
       |
Apply Workers (N)  — dispatcher fans out to per-shard goroutines (chan cap 64),
                     each gated by the MaxApplyWorkers semaphore
       |
Snapshot Pool      — max(NumWorkers/2, 2) workers, async save/recover
```

When the LogDB is nil (memory mode), step workers inline `Peer.Commit` and
forward straight to `applyC`, cutting the pipeline from three hops to two.

Every tier restarts on panic. A panicking step, commit or apply worker would
otherwise stall every shard mapped to it permanently, so the recovery clears
`commitPending` for the affected nodes and re-enters the loop after a short
delay.

Ordered shutdown, six steps: stop all nodes (unblocks in-flight snapshot I/O) →
step → commit → apply → snapshot → listener dispatcher, draining at each stage.
The dispatcher goes last so a leadership loss or shard failure observed during
teardown still reaches the listener.

### Listener dispatcher

User-visible cold-path notifications (`OnLeaderUpdated`, `OnCampaignLaunched`,
shard failure) run on one dedicated goroutine rather than on the worker that
produced them.

This is not a throughput optimization. The step worker fires callbacks while
holding `stepWorker.nodesMu.RLock`, and `Host.StopShard` re-enters the same
goroutine's `nodesMu.Lock()` through `Engine.UnloadNode`. Go's `RWMutex` is not
reentrant, so a handler that reacted to an event by stopping the shard
deadlocked — and not only that shard: every shard sharing the worker stopped
ticking, so heartbeats, elections and inbox drain all stopped with it, and a
later `Host.Close` never returned. Reacting to a failed shard by stopping it is
the obvious handler to write, which is what made it reachable.

The dispatcher goroutine holds no engine lock, so a listener may call any Host
API, including the synchronous `StopShard`. Notifications are FIFO on a single
consumer, giving a total order and therefore a per-shard order. There is
deliberately no ordering guarantee between dispatched notifications and the
callbacks that stay synchronous (`OnApplied`, `OnCommitted`, `OnReadyToRead`,
`OnProposalFailed`, `OnReadIndexBatched`, `OnReadIndexFailed`) — those resolve
pending client requests and have happens-before requirements against
`Peer.Handle`, so they cannot queue behind user code.

### Lock-Free Synchronization

- `Host.proposalKeySeq` / `readKeySeq` / `snapshotKeySeq`: separate
  `atomic.Uint64` counters so the three request classes do not contend
- `Node.commitPending` and `Node.lastApplied`: padded to 128 bytes (the
  prefetch-adjacent line pair on x86 and ARM) to prevent false sharing between
  the step worker, which reads them, and the apply worker, which writes them
- `nodeState` fields: leaderID, term, lastApplied, isLeader all atomic
- `Host.nodes`: sync.Map (lock-free reads for shard lookup), with
  `Host.activeNodes atomic.Int64` for the count
- `Host.registry`: `registry.Registry` over a sync.Map, lock-free on the
  `Resolve` hot path
- `Barrier.unsealed`: atomic.Bool fast-path before RWMutex acquisition

Not on this list, deliberately: the `EntryQueue` behind `proposalQ`, `inboxQ`
and `readIndexQ` is **mutex-guarded**. Producers hold a `sync.Mutex` for a
slice append and the consumer takes it once per drain cycle; the design goal
is amortizing acquisitions over a batch, not eliminating them. Nor is the
per-node `raftMu`, which serializes `Peer.Tick()` against `Peer.Commit()` and
also covers the step worker's `Peer.Step` and inbox-handling paths. It is not
reentrant, which is why event callbacks are invoked outside it.

## Configuration

### HostConfig (per-host)

- `WALDir` (required) — Write-ahead log directory
- `NodeHostDir` (required) — Metadata, snapshots, host ID
- `ListenAddress` (required) — Local bind address (e.g., `:4001`)
- `RaftAddress` — External address seen by peers (default: ListenAddress)
- `RTTMillisecond` (200ms) — Estimated round-trip time for timeout calculations
- `DeploymentID` (required) — Cluster identity token (must match all nodes)
- `NumWorkers` (0 = runtime.NumCPU()) — Worker goroutines per tier (step, commit, apply)
- `MaxApplyWorkers` (0 = runtime.NumCPU()) — Apply semaphore capacity
- `CommitCBufferSize` (64) — Commit channel buffer per worker
- `ShutdownTimeout` (30s) — Graceful shutdown timeout
- `MaxTotalInMemLogSize` (0 = disabled) — Global rate limiter threshold
- `NotifyCommit` (false) — Fire `OnCommitted` after WAL persist, before apply
- `MaxWALDiskSize` (0 = unbounded) — Reject proposals once the WAL disk fills
- `NoSyncWAL` (false) — Skip WAL fsync. Test and container-benchmark use only.
- `KeyRotationInterval` (0 = no scheduled rotation; 24h is a reasonable
  production value) — Time-based barrier DEK rotation. Invocation-threshold
  rotation (the NIST SP 800-38D AEAD budget) runs regardless whenever a barrier
  is configured.
- `MaxRetainedEpochs` (0 = retain all) — Old epochs beyond
  `currentEpoch - MaxRetainedEpochs` are purged from memory

There is no apply-retry setting. The apply path is fail-fast: a shard is marked
failed on the first `SM.Apply` error, with no retry counting. See
[Engine Pipeline](../engine/pipeline.md).

See `pkg/config/hostconfig.go`.

### Config (per-shard)

- `ElectionRTT` (10) — Election timeout as multiples of RTTMillisecond
- `HeartbeatRTT` (1) — Heartbeat interval as multiples of RTTMillisecond
- `CheckQuorum` (`*bool`, nil = enabled) — Leader steps down if quorum becomes unavailable
- `PreVote` (`*bool`, nil = enabled) — Use pre-vote extension to prevent disruptive elections
- `OrderedConfigChange` (`*bool`, nil = enabled) — Serialize membership changes (no concurrent config changes)
- `LeaseRead` (false) — Lease-based linearizable reads (PhD §6.4). Requires
  `CheckQuorum` and `ElectionRTT >= 5`.
- `SnapshotEntries` (100000) — Applied entries between automatic snapshots.
  **Zero does not disable it.** Zero means "unset" and selects the default;
  assign `config.SnapshotsDisabled` for manual-only snapshotting. Never
  snapshotting also means never reclaiming snapshot directories, never purging
  an old epoch's DEK, and never invoking the at-rest barrier.
- `CompactionOverhead` (5000) — Entries retained after compaction
- `MaxEntrySize` (8 MB) — Maximum single proposal size, and the decompression
  ceiling on the apply path. May not exceed `proto.MaxEntrySize`.
- `MaxProposalPayloadSize` (8 MB) — User-facing proposal cap. The effective
  limit at `Propose()` is `min(MaxProposalPayloadSize, MaxEntrySize)`.
- `MaxApplyEntrySize` (`MaxReplicationPayloadBytes` = 16,711,672 bytes, ~15.94 MB) — Maximum batch size per SM.Update call
- `MaxSnapshotSize` (4 GB) — Maximum snapshot file size
- `MaxSessionsPerClient` (128) — Cached responses retained per session. The name
  is wrong and is kept only because renaming a public field would break
  compatibility; read it as `MaxResponsesPerSession`. It bounds nothing about
  how many sessions exist.
- `MaxTotalSessions` (16384) — Per-shard total session limit
- `MaxSessionsPerOwner` (128) — Concurrent sessions one declared owner may hold.
  Sessions registered without an owner are exempt.
- `SessionExpiryEntries` (100000) — Session inactivity threshold measured in
  applied log entries, so it is deterministic across replicas. Zero selects the
  default; assign `config.SessionExpiryDisabled` to turn it off.
- `Witness` (`WitnessConfig`) — `MaxWitnesses` and `RequireFullReplicaMajority`.
  This is only the bootstrap seed; the live policy is replicated state.
- `IsObserver` / `IsWitness` — Mutually exclusive replica roles
- `Quiesce` (false) — Enable quiescence for idle shards
- `ZeroCopyEntryCmd` (false) — Let `Entry.Cmd` alias the internal buffer. When
  true the state machine must copy `Cmd` to retain it past `Update()`.

See `pkg/config/config.go`.

### TransportConfig

- `StreamPoolSize` (16) — Streams per QUIC connection
- `MaxStreamPoolSize` (256) — Upper bound for dynamic pool growth
- `MaxSnapshotReceiveRate` (256 MB/s) — Inbound snapshot bandwidth limit
- `MaxConcurrentSnapshotReceives` (4) — Concurrent inbound snapshot transfers
- `MaxSnapshotReceiveMemory` (1 GB) — Total snapshot receive buffer pool
- `MaxMessageReceiveMemory` (256 MB) — Bounds memory committed to inbound
  message frames, so a peer cannot declare a huge frame length and withhold the body
- `SendBatchMaxSize` (64 KB) — Maximum batch size for sending to one peer
- `MaxIncomingConnections` (256) — Total inbound connections allowed
- `MaxConnectionsPerIP` (16) — Inbound connections from a single source IP
- `MaxStreamsPerConnection` (256) — QUIC stream limit per connection
- `StreamReadIdleTimeout` (30s) — Idle timeout on a receive stream
- `UDPRecvBufSize` / `UDPSendBufSize` (7 MB each) — Socket buffers; 0 uses the
  OS default
- `Enable0RTT` (false) — QUIC 0-RTT (replayable, use with caution)
- `DisableCompression` (false) — Disable QUIC header compression (for benchmarking)

See `pkg/config/transportconfig.go`.

## State Machine Interface

QuicRaft supports three state machine tiers (detected at startup via type assertion):

See `pkg/sm/interfaces.go`.

### StateMachine (tier 0)

All state in memory, serialized access (Update and Lookup never run
concurrently — the RSM adapter holds its `readGuard` for writing across
`Apply` and for reading across every read entry point). Simplest to implement:

```go
type StateMachine interface {
    Update(ctx context.Context, entries []Entry, results []Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
}
```

### ConcurrentStateMachine (tier 1)

Concurrent Lookup while Update runs. Two-phase snapshotting: `PrepareSnapshot()`
captures a point-in-time view, then `SaveSnapshot()` writes without blocking Update.

```go
type ConcurrentStateMachine interface {
    Update(ctx context.Context, entries []Entry, results []Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    PrepareSnapshot() (interface{}, error)
    SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
}
```

### DiskStateMachine (tier 2)

Backed by persistent storage (e.g., PebbleDB). Must idempotently replay
already-applied entries after crash recovery. `Open()` is called at startup
with the last applied index from the Raft log.

```go
type DiskStateMachine interface {
    Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error)
    Update(ctx context.Context, entries []Entry, results []Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    Sync() error
    PrepareSnapshot() (interface{}, error)
    SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
}
```

### Factory Registration

Use `sm.CreateFunc` with type-safe wrappers:

```go
type CreateFunc func(shardID, replicaID uint64) interface{}

sm.NewCreateFunc(fn)            // StateMachine
sm.NewConcurrentCreateFunc(fn)  // ConcurrentStateMachine
sm.NewDiskCreateFunc(fn)        // DiskStateMachine
```

### Optional Interfaces

Detected via type assertion on the SM. **These are the signatures your state
machine must implement** — two return values, not three:

```go
// Cross-replica consistency checks.
type Hasher interface {
    GetHash() (uint64, error)
}

// Zero-alloc reads.
type NALookup interface {
    BytesLookup(key []byte) ([]byte, error)
}

// Zero-alloc reads into a caller-supplied buffer.
type NALookupInto interface {
    NALookup
    BytesLookupInto(key, dst []byte) ([]byte, error)
}
```

Because detection is by type assertion, an implementation with the wrong
signature does not satisfy the interface, produces **no compile error**, and
is simply never invoked — hash verification or the zero-alloc path silently
does not run. Match these signatures exactly.

Internally, the RSM adapter wraps each of these behind a
`(value, supported, error)` shape, where the `bool` reports whether the user
SM implemented the optional interface at all:

- SM implements the interface: `(result, true, error)`
- SM does not implement it: `(zero, false, nil)`
- SM errors are always propagated to the caller

That three-value form is an **internal** wrapper (`pkg/internal/rsm`). It is
not importable and is not the contract you implement. See
[State Machine Interfaces](../statemachine/interfaces.md) and
[RSM Adapter](../statemachine/rsm-adapter.md).

## Cluster Formation (lives in go-qrdb)

go-quicraft is a Raft-primitives library and does **not** discover peers or
orchestrate cluster formation. It exposes the primitive a cluster manager
builds on:

```go
// Caller supplies the member map (by discovery, static config, etc.).
err := host.StartShard(members, join, createFn, cfg)
```

Peer discovery (static / multicast / DNS SRV / token) and bootstrap
orchestration (discover → quorum → `StartShard`), plus opt-in dynamic
membership, live in **go-qrdb** (`pkg/discovery`, `pkg/cluster`) — the layer
that owns the cluster. See go-qrdb's `docs/discovery.md`.

## Witnesses and Membership

Initial membership is not out-of-band seed state. `StartShard` commits an
`EntryInitialMembership` (`proto.Entry.Type = 4`, `Cmd` = a marshaled
`proto.Membership`) at log index 1, so the member set a replica believes in is
the member set the log proves. Before this, a joining node could be authorized
against a locally seeded map that no quorum had agreed to.

Three replica roles:

- **Voter** — the default. Votes, stores log data, serves reads.
- **Observer** (`Config.IsObserver`) — replicates the log but does not vote.
- **Witness** (`Config.IsWitness`) — votes but stores no log data. Cannot
  propose and cannot serve a ReadIndex; both return `ErrInvalidOperation`.

Witnesses use a **unified quorum**: the same voting set (`remotes + witnesses +
self`) decides elections, commit, ReadIndex confirmation, and lease reads. A
witness's heartbeat acknowledgement counts toward the quorum that confirms a
linearizable read even though the witness could not serve that read itself. The
reason is overlap: the leadership-confirmation quorum has to intersect every
coalition that could win an election, and witnesses vote in elections. The
voting set is capped at `raft.MaxVotingMembers` = 64, because acknowledgements
are tracked in a 64-bit word.

Witness policy (`MaxWitnesses`, `RequireFullReplicaMajority`) is **replicated
state**, carried in `proto.Membership` through the log, snapshots and the WAL.
`Config.WitnessConfig` is only the bootstrap seed; after that the policy changes
through `Host.RequestUpdateWitnessPolicy` (a `ConfigChange` of type
`proto.UpdateWitnessPolicy`). A per-node policy value would let replicas
disagree about which config changes are legal.

## At-Rest Encryption

The `Barrier` in `pkg/seal/barrier.go` provides AEAD encryption for WAL data at rest.

**Algorithm selection** (automatic based on hardware):
- x86-64 with AES-NI or ARM64 with crypto extensions → AES-256-GCM
- Otherwise → ChaCha20-Poly1305
- Configurable via `BarrierConfig.Algorithm`

**Design**:
- Root key (32 bytes) from `crypto/rand`, held in a memguard enclave so it is
  encrypted at rest in process memory
- Data Encryption Keys (DEKs) derived via HKDF-SHA256 with a 32-byte salt and
  per-epoch info strings. The v2 info layout binds the algorithm identifier and
  DEK length into the derivation; v1 remains readable.
- Ciphertext format: `[epoch:8][algID:1][nonce:N][ciphertext+tag]`. The
  algorithm ID is inside the header, so a ciphertext names the AEAD it was
  written with instead of relying on the reader's configuration matching.
- Key rotation: `Rotate()` increments the epoch and derives a new DEK
- Old DEKs are retained for reading prior-epoch data, bounded by
  `MaxRetainedEpochs`, and purged once nothing on disk references them
- Brute-force protection: exponential backoff (1s doubling to a 60s cap) after 5
  consecutive failed Unseal attempts

**Lifecycle**: Initialize → Unseal → Encrypt/Decrypt → Seal

Root-key custody (where the sealed root key lives: TPM, HSM, KMS, passphrase)
is pluggable and documented in [Key Custody](../security/key-custody.md). This
library names only `StrategyPassphrase` and `StrategyNone`; hardware and cloud
strategy IDs are owned by go-xkms, and Shamir share custody by go-qrdb.
