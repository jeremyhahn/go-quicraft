# Multi-Group Host — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg` — the `Host`, its options,
> `RequestState`, `Membership`, `ShardInfo`, the error and metrics vocabulary,
> and `pkg/config` · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

The `Host` is the library's single entry point: one per process, managing shard
lifecycle, proposal routing, reads, sessions and membership. This document is
also the repository's system overview, because every other feature is composed
in the `Host`.

Per-feature design lives in the other folders:
[raft-consensus](../raft-consensus/architecture.md),
[quic-transport](../quic-transport/architecture.md),
[log-storage](../log-storage/architecture.md),
[snapshots](../snapshots/architecture.md),
[seal-barrier](../seal-barrier/architecture.md),
[state-machines](../state-machines/architecture.md),
[client-sessions](../client-sessions/architecture.md),
[membership-changes](../membership-changes/architecture.md),
[wire-protocol](../wire-protocol/architecture.md),
[write-batching](../write-batching/architecture.md),
[shamir-primitive](../shamir-primitive/architecture.md),
[memory-enclave](../memory-enclave/architecture.md),
[observability](../observability/architecture.md).

## System overview

QuicRaft (`github.com/jeremyhahn/go-quicraft`) is a multi-group Raft consensus
library in Go. One `Host` manages thousands of independent Raft shards, each
with its own state machine, log and membership. It provides:

- multi-group Raft with per-shard configuration
- QUIC-only transport with mutual TLS, and no TCP fallback
- a fixed-layout binary wire format (no protobuf)
- a sharded WAL: 16 directory shards selected by `shardID % 16`, each with its
  own segment files, index and mutex
- AEAD encryption at rest (AES-256-GCM or ChaCha20-Poly1305), chosen by
  hardware detection
- session-based at-most-once proposal delivery
- three state machine tiers: in-memory, concurrent, on-disk

It does not discover peers, validate a quorum of founders, or orchestrate
bootstrap. The caller hands `StartShard` the member set. See
[Cluster formation](#cluster-formation-and-the-discovery-boundary).

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

## Package structure

Public API:

| Package | Holds |
|---|---|
| `pkg/` (package `quicraft`) | `Host`, `RequestState`, `Session`, `Membership`, `ShardInfo`, `LeaderHandler`, errors, metrics |
| `pkg/config/` | `HostConfig`, `Config`, `TransportConfig`, `MTLSConfig`, `EventListener` |
| `pkg/sm/` | the `StateMachine`, `ConcurrentStateMachine` and `DiskStateMachine` interfaces |
| `pkg/sm/kv/` | reference stores: `MemoryStore`, `ConcurrentStore` |
| `pkg/proto/` | wire types: `Entry`, `Message`, `MessageBatch`, `Snapshot` |
| `pkg/logdb/` | the `LogDB` interface, `LogReader`, storage types |
| `pkg/logdb/waldb/` | the WAL-backed `LogDB` |
| `pkg/logdb/memdb/` | an in-memory `LogDB` for tests |
| `pkg/seal/` | the barrier (epoch DEK, HKDF, AEAD, rotation), algorithm selection, `SealingStrategy` and its protected-memory siblings |
| `pkg/crypto/` | type aliases re-exporting `pkg/seal`; no implementation |
| `pkg/crypto/shamir/` | the GF(256) split/combine primitive. Share custody, M-of-N quorum and recovery ceremonies belong to go-qrdb |
| `pkg/batch/` | proposal batching aggregator |
| `pkg/writemode/` | write durability modes |

Internal (not importable):

| Package | Holds |
|---|---|
| `pkg/internal/engine/` | step, commit, apply and snapshot workers, the listener dispatcher, `Node`, `ReadyMap` |
| `pkg/internal/raft/` | the Raft protocol, `Peer`, log, in-memory log, membership, remotes, quiescence |
| `pkg/internal/transport/` | QUIC transport, connection and stream pool, frame I/O, snapshot receive |
| `pkg/internal/transport/revocation/` | certificate revocation (CRL, OCSP) |
| `pkg/internal/rsm/` | the replicated state machine adapter and session routing |
| `pkg/internal/session/` | client session manager |
| `pkg/internal/registry/` | `(shardID, replicaID)` to address mapping |
| `pkg/internal/netaddr/` | address canonicalization, so two spellings of one endpoint compare equal |
| `pkg/internal/queue/` | `EntryQueue` (double-buffered MPSC) and `SwapBuffer` |
| `pkg/internal/server/` | the global in-memory-log rate limiter |
| `pkg/internal/invariant/` | debug assertions |
| `pkg/internal/supervise/` | Goexit-resilient supervision (`Supervise`, `ClassifyExit`, `RunResilient`, `RunEach`) and contained diagnostics (`LogContained`) for every goroutine that runs user code |

`contrib/dragonboat/` is a Dragonboat API compatibility layer.

## Data flow

### Proposal path

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
  | No leader known at all? -> NotLeaderError (fail fast)
  v
Host.proposeEntry()
  | Global rate limiter (ErrSystemBusy); failure latch (ErrShardFailed);
  |   per-shard rate limiter (*BusyError)
  | proposalKey = nextRequestKey()   -- random 64-bit draw, never zero
  | entry.Key = proposalKey
  | rs = newRequestState(ns.requestPool, proposalKey, deadline)
  | registerPendingProposal: ns.pendingProposals[proposalKey] =
  |     &pendingProposal{rs, deadline, floor: engNode.CommittedIndex()}
  | engNode.DeliverTrackedProposal(...) -- MPSC proposalQ; full -> ErrDropped
  | engine.NotifyWork(shardID)
  v
Engine step worker (shardID % NumWorkers)
  | Drain proposalQ, inboxQ and readIndexQ into reusable buffers
  | peer.Handle(...) under node.raftMu
  | peer.GetUpdate() -> proto.Update
  | Send free-order messages now, before persist (Replicate, Heartbeat, ...)
  |   Ordered messages (VoteResp, HeartbeatResp) wait for the commit worker
  | commitPending.Store(true)
  | commitC <- {update, node}
  v
Engine commit worker
  | logdb.SaveState(batch) -- entries + term/vote (5 attempts)
  | Send ordered messages, now that the state they promise is durable
  | peer.Commit() -- logReader.Append, advance markers, truncate inmem
  | applyC <- {update, node}
  v
Engine apply worker -> per-shard goroutine (bounded by a semaphore)
  | rsm.Apply(entries) -> session routing, then the user SM
  | callback.OnApplied(shardID, entries, results)
  | commitPending.Store(false); workSignal.Notify()
  v
Host.OnApplied() callback
  | For each entry: match entry.Key -> pendingProposals[key]
  | rs.complete(Result{Value, Data})
  v
Client <- rs.Result() unblocks
```

The request key is a random 64-bit draw, not a counter. `entry.Key` travels
through the log to every replica and is how an applied entry is matched back to
the caller waiting on it, so it must not repeat across hosts, nor across
incarnations of one host: a restarted replica replays its log through
`OnApplied`, and a replayed old entry must never complete a fresh proposal. A
random draw from `math/rand/v2` needs no persistence; uniqueness is
probabilistic at 2^-64 per pair of pending requests.

The committed-index floor makes the replay case certain. `pendingProposal.floor`
is the committed index last published to the host when the proposal registered
(`Node.CommittedIndex`, an atomic the step worker stores once per cycle, seeded
from the recovered log before `StartShard` returns). `OnApplied` and
`OnCommitted` refuse an entry at or below it (`pendingProposal.owns`): an entry
committed before the proposal registered cannot be that proposal, whatever its
key.

### Read path (linearizable)

```
Client
  |
  | h.SyncRead(ctx, shardID, query)
  v
Host.ReadIndex()
  | Witnesses rejected (ErrInvalidOperation): they store no log data
  | Refused at once with ErrShardFailed if the failure latch has tripped
  | readKey = nextRequestKey()
  | ns.pendingReads[readKey] = &pendingRead{rs}   (under ns.pendingReadsMu)
  | engNode.DeliverReadIndex(readKey) -- MPSC readIndexQ; full -> ErrSystemBusy
  | engine.NotifyWork(shardID)
  v
Engine step worker
  | Drain readIndexQ as a batch; the first key becomes the batchKey
  | callback.OnReadIndexBatched(shardID, batchKey, aliasKeys)  [BEFORE Handle]
  | peer.Handle(ReadIndex{Hint: batchKey})
  |   LeaseRead on and lease valid -> resolves inline, no heartbeat
  |   Otherwise the leader broadcasts a heartbeat carrying its own
  |   readIndexCtx. Context 0 is reserved for plain heartbeats, so an
  |   ordinary heartbeat response never satisfies a ReadIndex quorum.
  | On a quorum of HeartbeatResp: ReadyToRead{Key: batchKey, Index: commitIndex}
  v
callback.OnReadyToRead(shardID, []ReadyToRead)
  | Resolve batchKey and every alias registered against it
  | lastApplied >= readIndex -> rs.complete now
  | Failure latch trips while pending -> OnShardFailed drains it with
  |   ErrShardFailed (a halted shard never applies again)
  | Otherwise wait for OnApplied
  v
Host.SyncRead() continues
  | rs.Result() <- read index confirmed and applied
  | ns.rsm.Lookup(ctx, query) -- reads the SM directly
  v
Client <- query result
```

Concurrent readers on one shard share a heartbeat round. The step worker drains
the whole `readIndexQ` in one cycle, submits the first key to Raft, and
registers the rest as aliases through `OnReadIndexBatched`. Aliases are
registered before `Peer.Handle` on purpose: on a single-node shard `Handle`
resolves the ReadIndex synchronously, so registering afterwards would let
`OnReadyToRead` complete and delete the batch key first, stranding every aliased
caller until its deadline.

Witnesses cannot serve a ReadIndex, but their heartbeat acknowledgements count
toward the quorum that confirms one. See
[Witnesses and membership](#witnesses-and-membership).

A new leader refuses a ReadIndex with `ErrReadIndexNotReady` until it has
committed an entry of its own term (§6.4 of the Raft thesis). The edge is
published: `EventListener.OnLeaderReady` fires once per announced (leader, term)
on each loaded non-witness replica, and `Host.ShardReadReady` answers the same
question synchronously from the state those events delivered, so a caller that
installs the listener and then queries cannot miss the transition.

### Leader-executed requests

`Host.ExecuteOnLeader(ctx, shardID, request)` has the shard's leader run the
`LeaderHandler` registered for the shard with `RegisterLeaderHandler`, and
returns its reply. The request is never appended to the log, so a handler must
not change replicated state; it serves what the leader holds in memory for its
term. Called on the leader, the handler runs in-process on its own goroutine.
Called elsewhere, the request travels as `proto.LeaderExec` (message type 28)
and is answered with `LeaderExecResp` (29), through the same receive gate as a
forwarded proposal.

- A request executes only on the replica that leads the term it was sent under,
  and only after that leader has committed an entry of the term.
- It executes at most once per term. A deposed leader's reply is discarded and
  the request retried under the new term, so it may run once per term across a
  leader change, and handlers must be idempotent.
- A leader runs at most 1024 remote requests at once; beyond that it answers
  `ErrBusy`. Request and reply are held to the proposal payload limit
  (`*EntryTooLargeError`).
- A leader with no handler answers `ErrLeaderHandlerNotRegistered`; a failing
  handler surfaces as `*LeaderHandlerError`; a context that ends while no leader
  is settled yields `*LeaderUnavailableError` (`ErrNoLeader` or
  `ErrLeadershipChanged`).

Registration may precede `StartShard` and survives `StopShard`;
`UnregisterLeaderHandler` lets requests already executing finish.

## Key design decisions

### Fixed-layout binary wire format

`Entry`, `Message`, `MessageBatch` and `Snapshot` use hand-written fixed-layout
encoding with no protobuf and no code generation:

- struct layout fixes byte offsets; there are no type tags
- integers are 8-byte little-endian, with no varints
- `Entry.Cmd` aliases the source buffer after unmarshal (zero copy)
- maps are encoded in sorted key order, so encoding is deterministic
- sizes are bounded: `proto.MaxEntrySize` (8 MB), `proto.MaxBatchSize`
  (64 MB), `proto.MaxMessagesPerBatch` (10000)

| Type | Fixed header (bytes) | Variable content |
|---|---|---|
| `Entry` | 68 | `Cmd`, length-prefixed |
| `Message` | 88 | entries and an inline snapshot |
| `MessageBatch` | 24 | source address and messages |

`Entry` is eight uint64 fields plus a uint32 `CmdLen`. `Message` is eleven
uint64 fields; a heartbeat (no entries, no snapshot) encodes to 100 bytes. The
`MessageBatch` header is `BinVer`, `DeploymentID`, `SourceEpoch`, with `BinVer`
first so a receiver can reject a foreign wire version before parsing anything
else. Field tables are in [wire-protocol](../wire-protocol/engineering.md).

### QUIC-only transport

There is no TCP fallback and no `Transport` interface to swap one in. The Host
holds a concrete `*transport.QUICTransport` and wraps it in an internal
`transportSender` that satisfies the engine's `MessageSender`. Whenever the
transport is enabled, mTLS configuration is required; validation fails with
`*config.MTLSConfigRequiredError` otherwise.

What QUIC (RFC 9000) provides here:

- TLS 1.3 built in, with optional 0-RTT reconnection (off by default, because
  0-RTT data is replayable)
- multiplexed streams, so one stalled shard does not block others
- stream 0 reserved for heartbeats, pre-opened per connection. Heartbeats also
  use a priority channel drained ahead of data, so a large replication batch
  cannot delay one past the election timeout
- `StreamPoolSize` (default 16) data streams per connection, growable to
  `MaxStreamPoolSize` (256)
- pooled buffers on the send and receive paths, and `singleflight` to coalesce
  concurrent dials to the same peer

Snapshot transfers carry a frame flag that routes them to a separate handler,
keeping multi-gigabyte streams out of the stream pool.

### Sharded WAL

`waldb` splits writes across `DefaultShardCount` = 16 directory shards
(`shard-0` to `shard-15`), selected by `shardID % 16`:

- each shard has its own active segment, entry index and mutex, so one shard's
  fsync does not serialize behind another's
- segments are append-only, block-aligned and CRC32-checked
- recovery rebuilds all in-memory state by replaying segments
- an fsync failure poisons the whole DB (`ErrDBPoisoned`) rather than
  continuing, because a failed `fdatasync` may leave the kernel marking dirty
  pages clean without writing them

### Engine worker pipeline

Shards are partitioned across `N = NumWorkers` workers by `shardID % NumWorkers`:

```
Step workers (N)    tick; drain proposalQ/inboxQ/readIndexQ; produce Updates
       |
   commit channels  (CommitCBufferSize, default 64, per worker)
       |
Commit workers (N)  persist entries to LogDB, Peer.Commit()
       |
   apply channels   (max(CommitCBufferSize*4, 256), per worker)
       |
Apply workers (N)   fan out to per-shard goroutines (channel cap 64),
                    gated by the MaxApplyWorkers semaphore
       |
Snapshot pool       max(NumWorkers/2, 2) workers, async save and recover
```

A `Host` always has a LogDB: the default WAL, a `WithLogDB` store, or the
in-memory store from `WithMemoryLogDB`. The engine alone also supports running
with no LogDB, in which the step worker runs `Peer.Commit` itself and forwards
straight to `applyC`; no `Host` configuration reaches that mode.

Every tier runs under `supervise.Supervise` and restarts on a panic or a
`runtime.Goexit` (reported as `supervise.ErrGoexit`). A dead worker would
otherwise stall every shard mapped to it for good, so recovery clears
`commitPending` for the affected nodes and re-enters the loop on a fresh
goroutine after `supervise.RestartDelay` (100 ms).

Shutdown runs in six ordered steps: stop all nodes (unblocking in-flight
snapshot I/O), then step, commit, apply, snapshot, and finally the listener
dispatcher, draining at each stage. The dispatcher goes last so a leadership
loss or shard failure observed during teardown still reaches the listener.

### Listener dispatcher

Engine notifications for user code (leader changes, read readiness, campaign
launches, shard failure, log compaction, snapshot creation and rejection) run
on one dedicated goroutine rather than on the worker that produced them.

The reason is deadlock, not throughput. The step worker fires callbacks while
holding `stepWorker.nodesMu.RLock`, and `Host.StopShard` takes the same
goroutine's `nodesMu.Lock()` through `Engine.UnloadNode`. Go's `RWMutex` is not
reentrant, so a handler that stopped the shard from inside the callback would
deadlock that shard and every other shard on the same worker. Stopping a failed
shard is the obvious handler to write.

The dispatcher holds no engine lock, so a listener may call any Host method,
including the synchronous `StopShard`. Notifications are FIFO on a single
consumer, which gives a total order and therefore a per-shard order. There is
no ordering guarantee between dispatched notifications and the callbacks that
stay synchronous (`OnApplied`, `OnCommitted`, `OnReadyToRead`,
`OnProposalFailed`, `OnReadIndexBatched`, `OnReadIndexFailed`). Those resolve
pending client requests and have happens-before requirements against
`Peer.Handle`, so they cannot queue behind user code.

### Synchronization

Lock-free:

- request keys: draws from `math/rand/v2`'s process-wide generator
  (`nextRequestKey`), with no per-Host counter to contend on
- `Node.commitPending` and `Node.lastApplied`, each padded to 128 bytes (the
  adjacent-line prefetch pair on x86 and ARM) to prevent false sharing between
  the step worker, which reads them, and the apply worker, which writes them
- `nodeState` leader ID, term, last applied and leader flag, all atomics
- `Host.nodes`, a `sync.Map`, with `Host.activeNodes` (`atomic.Int64`) for the
  count
- `Host.registry`, a `registry.Registry` over a `sync.Map`, lock-free on the
  `Resolve` hot path
- the barrier's unsealed flag, an `atomic.Bool` checked before the `RWMutex`

Not lock-free, deliberately:

- the `EntryQueue` behind `proposalQ`, `inboxQ` and `readIndexQ`. Producers hold
  a `sync.Mutex` for one slice append and the consumer takes it once per drain;
  the goal is amortizing acquisitions, not eliminating them.
- the per-node `raftMu`, which serializes `Peer.Tick()`, `Peer.Commit()`,
  `Peer.Step` and inbox handling. It is not reentrant, which is why event
  callbacks run outside it.

## Configuration

The full field tables, with defaults, are in
[api.md](api.md#configuration). Design-level rules:

- `RaftAddress` and `ListenAddress` are both required. Peers dial
  `RaftAddress`; the listener binds `ListenAddress`. They differ under NAT,
  port mapping and containers.
- There is no apply-retry setting. The apply path is fail-fast: the first
  `SM.Update` error marks the shard failed. See
  [engineering.md](engineering.md#error-handling-and-shard-failure).
- `SnapshotEntries`, `SessionExpiryEntries` and `MaxSessionsPerOwner` treat zero
  as "use the default". Each has a named sentinel for "off". Never snapshotting
  also means never reclaiming snapshot directories, never purging an old
  epoch's DEK and never invoking the at-rest barrier.
- `CheckQuorum`, `PreVote` and `OrderedConfigChange` are `*bool`; nil means
  enabled.
- `Config.Witness` and the four session limits are bootstrap seeds. The live
  values are replicated state in `proto.Membership`.
- `MaxSessionsPerClient` bounds cached responses per session, not sessions. The
  name is kept because renaming a public field breaks callers.

## State machine tiers

Three tiers, detected by type assertion at `StartShard`. Full contracts are in
[state-machines](../state-machines/engineering.md).

| Tier | Interface | Concurrency |
|---|---|---|
| 0 | `sm.StateMachine` | serialized: `Update` and `Lookup` never overlap. The RSM adapter holds its `readGuard` for writing across `Apply` |
| 1 | `sm.ConcurrentStateMachine` | `Lookup` runs alongside `Update`; `Apply` holds the guard for reading. Two-phase snapshot (`PrepareSnapshot`, then `SaveSnapshot`) |
| 2 | `sm.DiskStateMachine` | as tier 1, plus `Open` returns the last applied index and replay after a crash must be idempotent |

Factories: `sm.NewCreateFunc`, `sm.NewConcurrentCreateFunc`,
`sm.NewDiskCreateFunc`, each wrapping a typed constructor into an
`sm.CreateFunc`.

Optional interfaces (`sm.Hasher`, `sm.NALookup`, `sm.NALookupInto`) are also
detected by type assertion, so a method with the wrong signature produces no
compile error and is never called. Match the signatures in
[api.md](api.md#optional-interfaces) exactly. Internally the RSM adapter wraps
each in a `(value, supported, error)` form; that wrapper is in
`pkg/internal/rsm` and is not the contract you implement.

## Witnesses and membership

`StartShard` commits the initial membership as an `EntryInitialMembership`
(`proto.Entry.Type = 4`, `Cmd` = a marshaled `proto.Membership`) at log index 1,
so the member set a replica believes in is the one its log proves. Membership is
a fold over the committed log.

Three replica roles:

- **Voter**: the default. Votes, stores log data, serves reads.
- **Observer** (`Config.IsObserver`): replicates the log but does not vote.
  `IsObserver` is only the initial hint. The committed membership decides, so an
  observer whose `AddNode` promotion commits becomes a voter in place, with no
  restart.
- **Witness** (`Config.IsWitness`): votes but stores no log data. It cannot
  propose or serve a ReadIndex (`ErrInvalidOperation`), and it is never
  promoted.

On the leader, `ShardInfo.Replicas` reports each remote member's replication
progress tagged with its `ReplicaRole` (`ReplicaRoleVoter`,
`ReplicaRoleObserver`, `ReplicaRoleWitness`); it is empty on a follower.

Witnesses use a unified quorum: the same voting set (remotes, witnesses and
self) decides elections, commit, ReadIndex confirmation and lease reads. The
confirmation quorum has to intersect every coalition that could win an
election, and witnesses vote. The voting set is capped at
`raft.MaxVotingMembers` = 64 because acknowledgements are tracked in a 64-bit
word.

Witness policy (`MaxWitnesses`, `RequireFullReplicaMajority`) is replicated
state, carried in `proto.Membership` through the log, snapshots and the WAL.
`Config.Witness` only seeds it; afterwards it changes through
`Host.RequestUpdateWitnessPolicy`, a `ConfigChange` of type
`proto.UpdateWitnessPolicy`. A per-node value would let replicas disagree about
which config changes are legal.

## At-rest encryption

The barrier in `pkg/seal/barrier.go` encrypts WAL records and snapshots.

The host's posture must be stated: `NewHost` fails with
`ErrAtRestEncryptionUnspecified` unless it gets `WithBarrier`,
`WithShardBarriers` or `WithoutEncryption`, and with
`ErrAtRestEncryptionConflict` when `WithBarrier` and `WithoutEncryption` are
both given. `WithShardBarriers` is a separate, per-shard axis that combines with
either host-wide option. A shard whose resolver returns nil takes the host-wide
route (the `WithBarrier` barrier, or plaintext under `WithoutEncryption`); with
no host-wide option it fails closed with `ErrShardBarrierUnresolved`. The
host's key-rotation loop rotates only the `WithBarrier` barrier, and
`Host.Close` seals every barrier the resolver returned.

Algorithm selection:

- x86-64 with AES-NI, or ARM64 with crypto extensions: AES-256-GCM
- otherwise ChaCha20-Poly1305, except in a `-tags fips` build, which does not
  compile ChaCha20 in at all and uses AES-256-GCM
  ([FIPS builds](../seal-barrier/engineering.md#fips-runtime-mode-vs-build-tag))
- override with `BarrierConfig.Algorithm`

Design:

- the 32-byte root key is generated by `crypto/rand` directly into locked
  memory and held in a go-enclave `SealedEnclave`, encrypted at rest in process
  memory
- per-epoch DEKs are derived with HKDF-SHA256 from a 32-byte salt. The v2 info
  layout binds the algorithm identifier and DEK length; v1 remains readable
- ciphertext is `[epoch:8][algID:1][nonce:N][ciphertext+tag]`, so a ciphertext
  names the AEAD that wrote it
- `Rotate(ctx)` increments the epoch and derives a new DEK. Old DEKs stay for
  reading prior-epoch data, bounded by `MaxRetainedEpochs`, and are purged once
  nothing on disk references them
- after 5 consecutive failed unseal attempts, backoff starts at 1s and doubles
  to a 60s cap

Lifecycle: Initialize, Unseal, Encrypt/Decrypt, Seal.

Root-key custody (TPM, HSM, KMS, passphrase) is pluggable; see
[key custody](../seal-barrier/user.md). This library implements only
`StrategyPassphrase` and `StrategyNone`. Hardware and cloud strategies are
owned by go-xkms, and Shamir share custody by go-qrdb.

## Rejected alternatives

Per-feature rationale lives in each feature folder. These decisions belong to
the `Host`.

### One `Host` per Raft group

Every shard would pay for its own transport, WAL, worker set and snapshot pool.
A host runs thousands of shards, so those resources are shared and the `Host`
owns them: one process, one listener, one 16-shard WAL, `NumWorkers` goroutines
per tier.

### A `Cluster` type that forms itself

`StartShard` takes an explicit member map because a consensus engine cannot
safely guess its own membership (see
[below](#why-the-boundary-sits-here)). Only something that can see all the
nodes can make that decision.

### Exposing the engine, the peer or the transport

`Host` is the only entry point and `pkg/internal/*` is not importable. The
narrow surface lets the execution model change without breaking consumers.

### A `Transport` interface

An interface implies a swappable implementation, and the one thing an embedder
would actually swap in is a transport without mTLS. See
[quic-transport](../quic-transport/architecture.md).

### Returning only `error` from every accessor

`GetLeaderID` returns `(uint64, bool, error)` and `StateMachineHash` returns
`(uint64, uint64, bool, error)`. The `bool` separates "no answer yet" from
"failed to ask"; collapsing them would make an ordinary startup race look like
a fault.

### Type-based sender exemptions

Admitting leader-originated pushes and vote requests from an unknown replica
ID, so a lagging replica could catch up, would let any holder of a cluster
certificate move a shard's term, install itself as leader on followers,
re-point routing and get forwarded proposals appended. The receive gate instead
admits only senders a replica can vouch for from facts it holds; see
[Every sender is bound](#every-sender-is-bound-to-a-replica-it-can-prove-it-is).

## Trust boundaries

Four trust boundaries meet at the `Host`. Each is owned by the feature behind
it:

| Boundary | Owner |
|---|---|
| the network: peer authentication, revocation, memory budgets | [quic-transport](../quic-transport/architecture.md) |
| the disk: at-rest encryption, key custody | [seal-barrier](../seal-barrier/architecture.md), [log-storage](../log-storage/architecture.md) |
| embedder code on the apply path | [state-machines](../state-machines/architecture.md) |
| membership and sender authorization | [membership-changes](../membership-changes/architecture.md) |

What the `Host` itself owns:

- **Rate limiting**, global (`MaxTotalInMemLogSize`) and per shard
  (`MaxInMemLogSize`), applied before a proposal is accepted. Without it one
  client can grow the in-memory log until the process dies.
- **Disk-full refusal** (`ErrDiskFull`), so proposals stop before the WAL fills
  the volume and a failed `fdatasync` poisons the store.
- **Proposal size bounds**: `min(MaxProposalPayloadSize, MaxEntrySize)`.
- **Draining and sealed states**, so a shutting-down host refuses new work
  instead of half-accepting it.
- **Request-key uniqueness**: random 64-bit keys plus the committed-index floor,
  so no replica or incarnation completes a pending request with another
  request's entry.

The `Host` performs no authentication or authorization of its callers. It is
in-process, and whoever holds the `*Host` has full access. Client identity,
tenancy and RBAC belong to the layers above.

## Cluster formation and the discovery boundary

go-quicraft does not discover peers and does not orchestrate cluster formation.
The primitive it exposes is:

```go
func (h *Host) StartShard(
    members map[uint64]string,
    join bool,
    create sm.CreateFunc,
    cfg config.Config,
) error
```

The caller supplies an explicit map of replica ID to Raft address. Where the map
comes from (a config file, multicast, DNS SRV, a token exchange) is outside this
library. Discovery, quorum validation and bootstrap orchestration live in
go-qrdb; see [Forming a cluster automatically](#forming-a-cluster-automatically-go-qrdb).

### Why the boundary sits here

The initial configuration is committed as a log entry that every founding
replica writes independently. The map therefore has to be an input, decided by
something that can see all the nodes, not something the engine infers.

### The primitive

`StartShard` takes `ShardID` and `ReplicaID` from `cfg`; both must be non-zero.
On a host with a transport, `cfg.MemberIdentities` must name every member's node
identity (the value each node reports from `Host.NodeIdentity`). The Host
enforces this by setting `cfg.RequireMemberIdentities` from whether it has a
transport, overriding the caller's value; every admission path (the founding
map, the joiner's anchor, every add, including one forwarded from another
replica) then refuses a member with no identity. A three-node shard, as run on
node 1:

```go
hostCfg := config.HostConfig{
    WALDir:        "/var/lib/quicraft/wal",
    NodeHostDir:   "/var/lib/quicraft/nodehost",
    RaftAddress:   "node1.example.com:63100",
    ListenAddress: "0.0.0.0:63100",
    DeploymentID:  1,
    TransportConfig: config.TransportConfig{
        MTLSConfig: &config.MTLSConfig{CACert: caCert, Cert: cert, Key: key},
    },
}

host, err := quicraft.NewHost(hostCfg, quicraft.WithoutEncryption())
if err != nil {
    return err
}
defer host.Close()

members := map[uint64]string{
    1: "node1.example.com:63100",
    2: "node2.example.com:63100",
    3: "node3.example.com:63100",
}

shardCfg := config.Config{
    ShardID:          1,
    ReplicaID:        1, // 2 on node2, 3 on node3
    MemberIdentities: identities, // replica ID -> NodeIdentity(), same on every founder
}

if err := host.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg); err != nil {
    return err
}
```

Every founder calls this with the same `members` map, the same
`MemberIdentities` and the same shard configuration, changing only `ReplicaID`.

#### `join` and the two ways in

`join = false` bootstraps: this replica is a founder and writes the
initial-membership entry itself.

`join = true` grows an existing shard. The joiner holds no configuration and
learns one by replicating the log. Its `MemberIdentities` is the operator
anchor: it must name at least one other replica it may accept the bootstrap
snapshot from. On an existing member, an operator first adds the joiner with
`RequestAddNodeWithIdentity` (or `SyncRequestAddNodeWithIdentity`), and the
config change commits before the joiner catches up.

A replica configured as a witness (`cfg.IsWitness`) or observer
(`cfg.IsObserver`) must not appear in `members` on a first start: that map seeds
the voting membership, and naming one is refused with
`*NonVotingRoleInMembersError`. Bootstrap the voters, then start the witness or
observer with `join = true` and add it with `RequestAddWitnessWithIdentity` or
`RequestAddObserverWithIdentity`.

Other primitives a cluster manager builds on: `Host.StopShard`,
`Host.GetLeaderID`, `Host.GetLeaderIDAndTerm`, `Host.GetShardMembership` and the
membership requests in `pkg/membership.go`.

### What StartShard commits, and what it refuses

#### Initial membership is a committed log entry

The initial configuration is one `EntryInitialMembership` at index 1, term 1,
whose `Cmd` is a marshaled `proto.Membership`. Every replica reconstructs its
membership by replaying the committed log, so a joiner learns the full
configuration from the log even if no snapshot ever reaches it. One entry
carrying the whole `Membership` is used, rather than one `ConfigChange` per
member, so the first real proposal lands at index 2.

**Every founder must be given identical bootstrap input.** `proto.Membership`
marshals its maps with sorted keys, so the entry is byte-identical across
founders given identical input. The input is the whole `Membership`: addresses,
`MemberIdentities`, the witness policy (`Config.Witness`) and the session policy
(`SessionExpiryEntries`, `MaxTotalSessions`, `MaxSessionsPerClient`,
`MaxSessionsPerOwner`), all seeded from each founder's local config. Founders
that disagree produce different entries at the same `(index=1, term=1)`, and
AppendEntries matches on `(index, term)` alone, so the divergence is never
reconciled.

See [wire-protocol](../wire-protocol/engineering.md) for the `Membership`
encoding and [client-sessions](../client-sessions/engineering.md) for the
session policy.

#### Per-node certificate exclusivity is checked, and fails closed

`StartShard` calls
`QUICTransport.VerifyPeerAddressExclusivity(RaftAddress, memberAddresses)` and
refuses to start when this node's own leaf certificate also covers another
member's host (`ErrSharedPeerCertificate`, a `*SharedPeerCertificateError`).

A wildcard certificate (`*.raft.svc.cluster.local`), or one carrying every
node's SAN, would make every in-band authorization decision in the transport
answer "authorized" for every peer address. The check fails closed rather than
warning because the weakness is invisible from a running cluster and the only
fix, re-issued certificates, has to happen before the cluster carries data.
Replicas that share this node's host (any spelling, or any loopback address) are
exempt, because no certificate policy can separate them.

This detects misconfiguration; it does not stop an attacker, since a malicious
joiner never runs it. The enforcing counterpart is on the receive path
(`refuseInboundPromiscuousCert`). See
[quic-transport](../quic-transport/engineering.md#per-node-sans-are-a-hard-deployment-precondition).

### Every sender is bound to a replica it can prove it is

Per-shard authorization first refuses the replicas, and the node identities,
whose removal the shard has committed: a tombstone deny-list, as etcd and Consul
keep. It then binds every inbound message's `From` to the authenticated peer.
Three facts can vouch for a sender, and each binds the connection's identity,
never an address (`senderBinding`):

1. **The committed configuration** names the sender with a node identity, and
   the connection presents it.
2. **The pending configuration** does: a config-carrying entry appended to this
   replica's own log but not yet applied admits the sender with an identity.
   This is the lagging follower that holds the `AddNode` for a new leader before
   committing it; it must admit that leader's `Replicate` and `Heartbeat` or it
   can never catch up. The entry reached this log through a sender bound by the
   same rule. A committed `RemoveNode` above the applied configuration is a
   pending tombstone, refused before anything else is consulted, by replica ID
   and by keypair (`Peer.PendingRemoval`). An uncommitted removal revokes
   nobody: admitting on an entry that may be truncated costs nothing, while
   revoking on one can wedge the shard.
3. **The operator anchor** (`Config.MemberIdentities`): the replicas the
   operator started this shard with, and their identities. A joiner holds no
   configuration, so nothing else can say who may bring it one. An established
   replica that was down while a member was added and elected holds no entry for
   that leader either; restarting it with a map naming the leader is how its
   operator vouches for it without a wipe.

On a restart the anchor is held to what was recovered
(`checkRecoveredIdentities`): it may not name a different identity for a
committed member, nor bind a committed member's identity to a second replica ID
(that would let one keypair send as two replicas), and the host must present the
identity the shard committed for its own replica, or the start is refused. For
bindings committed after recovery, the receive gate asks the committed
configuration (`Peer.IdentityHolder`) before the pending configuration or the
anchor, so a keypair bound to one replica is never admitted as another. A
committed binding for this replica that the host does not present is logged at
ERROR when applied. An anchor entry whose identity the shard has removed is inert
(the deny-list runs first) and tolerated with a WARN, since every founder's map
still names it after a routine `RemoveNode`.

The cost: a replica that never received the config change adding a new leader
refuses that leader until a replica its configuration names leads, or until it
is restarted with a `Config.MemberIdentities` naming the leader. The shard keeps
quorum without it.

Routing and authorization are kept separate:

- Routing state is learnable, from bound messages only. `learnFromSource`
  records a bound sender's address for the return path, and may replace an
  established entry only on the strength of the committed identity. A joiner
  records the anchored leader's address so it can ack.
- Authorization is never learned. Drops are counted by reason
  (`removed_replica`, `removed_identity`, `misrouted`, `unknown_member`,
  `identity_mismatch`, `missing_identity`), because one aggregate counter
  cannot tell a rolling membership change from an attack.

See [Compatibility](../compatibility/architecture.md#node-identities-are-mandatory) for why
identities are required at admission.

### Forming a cluster automatically (go-qrdb)

Everything above the member map lives in go-qrdb:

- `pkg/discovery` implements the discovery methods behind a common `Method`
  interface (static, multicast, DNS SRV, token), plus a `Manager` that chains
  several methods as a fallback list.
- `pkg/cluster` orchestrates formation. `Bootstrapper.Bootstrap` runs
  discover, validate addresses, check quorum, add self, build the member map,
  then `StartShard`. `BootstrapShard(ctx, node, opts, shardID, createFn,
  shardCfg)` is the reusable entry point that builds the discovery method from
  options and runs that sequence. `MinPeers` defaults to 3, and quorum is
  checked before any shard starts.
- Dynamic membership is opt-in, also in `pkg/cluster`: members register under
  the `__sys:cluster/members/` prefix with a lease (10 s TTL, refreshed every
  3 s), and a watcher drives Raft add and remove with bounded retries.

To form a multi-node cluster automatically, call go-qrdb's
`cluster.BootstrapShard` or run its `qrdbd` daemon. The reference is go-qrdb's
clustering feature documentation (`docs/clustering/` in that repository).

## Cross-references

- [API reference](api.md): `Host.StartShard`, membership requests, every type
- [Lifecycle](engineering.md): host and shard startup, recovery, shutdown
- [Usage](user.md): deployment, certificates, monitoring
- [Raft protocol](../raft-consensus/engineering.md): membership changes,
  config-in-log, witnesses
- [QUIC transport](../quic-transport/engineering.md): certificate exclusivity,
  sender authorization
- [Compatibility](../compatibility/architecture.md): node identities, format pinning
