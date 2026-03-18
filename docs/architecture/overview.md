# QuicRaft Architecture Overview

## System Overview

QuicRaft (github.com/jeremyhahn/go-quicraft) is a multi-group Raft consensus
library in Go. A single `Host` manages thousands of independent Raft shards,
each with its own state machine, log, and membership. It provides:

- Multi-group Raft with per-shard configuration
- QUIC-only transport (no TCP fallback) with mutual TLS
- Custom fixed-layout binary wire format (no protobuf)
- Sharded WAL with 16 segments for parallel I/O
- Algorithm-agile AEAD encryption (AES-256-GCM or ChaCha20-Poly1305) with hardware auto-detection
- Session-based at-most-once proposal delivery
- Three state machine tiers: in-memory, concurrent, on-disk

```
+------------------------------------------------------------------+
|                             Host                                 |
|                                                                  |
|   +------------+    +------------+    +------------+             |
|   |  Shard 1   |    |  Shard 2   |    |  Shard N   |    ...      |
|   +------+-----+    +------+-----+    +------+-----+             |
|          |                 |                 |                   |
|          +-----------------+-----------------+                   |
|                            |                                     |
|                     +------+------+                              |
|                     |   Engine    |                              |
|                     | Step Commit |                              |
|                     |    Apply    |                              |
|                     +------+------+                              |
|                            |                                     |
|          +-----------------+-----------------+                   |
|          |                 |                 |                   |
|   +------+-----+   +------+------+   +------+------+             |
|   |    WAL     |   |  Transport  |   |  Snapshot   |             |
|   | (16-shard) |   |   (QUIC)    |   |    Pool     |             |
|   +------------+   +-------------+   +-------------+             |
+------------------------------------------------------------------+
```

## Package Structure

**Public API**

- `pkg/` — Host, RequestState, Session, Membership, ShardInfo, errors, metrics
- `pkg/config/` — HostConfig, Config, TransportConfig, MTLSConfig, EventListener
- `pkg/sm/` — StateMachine, ConcurrentStateMachine, DiskStateMachine interfaces
- `pkg/sm/kv/` — Reference KV implementations: MemoryStore, ConcurrentStore
- `pkg/proto/` — Custom binary wire format: Entry, Message, MessageBatch, Snapshot
- `pkg/logdb/` — LogDB interface, LogReader, storage layer types
- `pkg/logdb/waldb/` — WAL-based LogDB (16 shards, segment files)
- `pkg/logdb/memdb/` — In-memory LogDB for testing
- `pkg/seal/` — Barrier (AEAD encryption), SealingStrategy
- `pkg/crypto/` — AES-GCM/ChaCha20-Poly1305, hardware detection
- `pkg/crypto/shamir/` — Shamir's Secret Sharing for key management
- `pkg/discovery/` — Static, multicast, DNS SRV peer discovery
- `pkg/bootstrap/` — Cluster formation orchestration
- `pkg/batch/` — Proposal batching utilities
- `pkg/enclave/` — Memory-protected containers (mlock)
- `pkg/writemode/` — Write durability modes

**Internal Implementation**

- `pkg/internal/engine/` — Step/commit/apply/snapshot workers, Node struct
- `pkg/internal/raft/` — Raft protocol, Peer, log, state machine
- `pkg/internal/transport/` — QUIC transport, connection/stream pool, frame I/O
- `pkg/internal/rsm/` — Replicated state machine adapter, session routing
- `pkg/internal/session/` — Client session manager (at-most-once delivery)
- `pkg/internal/registry/` — (shardID, replicaID) → address mapping
- `pkg/internal/queue/` — Lock-free MPSC queues, ReadyMap
- `pkg/internal/stopper/` — Graceful shutdown coordination
- `pkg/internal/server/` — Global rate limiter
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
  | Find nodeState (sync.Map)
  | Validate: closed? shard exists? leader?
  | Check global + per-shard rate limiters
  | proposalKey = proposalKeySeq.Add(1)
  | rs = newRequestState(pool, proposalKey, deadline)
  | pendingProposals[proposalKey] = rs
  | Create proto.Message{Type: Propose, Entries: [...]}
  | engNode.Deliver(msg) — non-blocking
  | engine.NotifyWork(shardID)
  v
Engine Step Worker (shardID % numWorkers)
  | Drain inbox, collect proto.Message(s)
  | peer.Handle(messages) — Raft processes Propose
  | peer.GetUpdate() → proto.Update
  | Batch update.Messages, send via QUIC transport
  | commitC ← {update, node}
  v
Engine Commit Worker
  | logdb.SaveState(update) — persist entries + term/vote
  | logReader.Append(entries) — update in-memory cache
  | applyC ← {update, node}
  v
Engine Apply Worker (bounded by semaphore)
  | rsm.Apply(entries) → apply through session manager to SM
  | callback.OnApplied(shardID, entries, results)
  v
Host.OnApplied() callback
  | For each entry: match Key → pendingProposals[key]
  | rs.complete(Result{Value, Data})
  v
Client ← rs.Result() unblocks
```

### Read Path (Linearizable)

```
Client
  |
  | h.SyncRead(ctx, shardID, query)
  v
Host.ReadIndex()
  | readKey = proposalKeySeq.Add(1)
  | pendingReads[readKey] = &readState{rs}
  | engNode.Deliver(Message{Type: ReadIndex, Hint: readKey})
  v
Engine Step Worker
  | peer.Handle(ReadIndex message)
  | Leader broadcasts Heartbeat with context = readKey
  | On quorum of HeartbeatResp: ReadyToRead{Key: readKey, Index: commitIndex}
  v
callback.OnReadyToRead(shardID, []ReadyToRead)
  | pendingReads[key].readIndex = rtr.Index
  | pendingReads[key].confirmed = true
  | If lastApplied >= readIndex: rs.complete immediately
  | Else: wait for OnApplied
  v
Host.SyncRead() continues
  | rs.Result() ← readIndex index confirmed
  | rsm.Lookup(ctx, query) — read from SM without going through log
  v
Client ← query result
```

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

| Type         | Header | Content              |
|--------------|--------|----------------------|
| Entry        | 60     | Cmd (variable)       |
| Message      | 88     | Entries + Snapshot   |
| MessageBatch | 16     | Address + Messages   |

### QUIC-Only Transport

No TCP fallback. QUIC 1.0 (RFC 9000) provides:

- Built-in TLS 1.3 with 0-RTT reconnection
- Multiplexed streams eliminating head-of-line blocking
- Stream 0 reserved for heartbeats (priority path)
- 16-shard stream pool per connection (reduces mutex contention)
- Pooled byte buffers on send and receive paths
- `singleflight.Group` to coalesce concurrent connection attempts
- Persistent heartbeat streams (pre-opened, reused across ticks)

Snapshot transfers use a dedicated frame flag to route to separate handler,
avoiding stream pool pressure.

### Sharded WAL

The `waldb` package splits writes across 16 segment files based on `shardID`
hash. Design:

- Each segment: append-only, block-aligned, CRC32 checksums
- Parallelizes I/O across shards (no single bottleneck)
- Avoids head-of-line blocking (one slow shard doesn't block others)
- 16 chosen as N where 2^N shards is rare (most clusters < 256 shards)

### Engine Worker Pipeline

Shards are partitioned across `N` workers by `shardID % numWorkers`. Three
independent worker tiers:

```
Step Workers (N)   — tick, process messages, produce Updates
       |
   Commit Channels (64 buffer per worker)
       |
Commit Workers (N) — persist entries to LogDB, update in-memory log cache
       |
   Apply Channels (256 buffer per worker)
       |
Apply Workers      — bounded by semaphore, feed entries to state machine
       |
Snapshot Pool      — async snapshot save/recover (non-blocking channel)
```

Ordered shutdown: step → commit → apply → snapshot, drain at each stage.

### Lock-Free Synchronization

- `Host.proposalKeySeq`: atomic.Uint64 for monotonic proposal IDs
- `Node.commitPending` and `Node.lastApplied`: 128-byte padded atomics (prevent
  false sharing between step and apply workers)
- `nodeState` fields: leaderID, term, lastApplied, isLeader all atomic
- `Host.nodes`: sync.Map (lock-free reads for shard lookup)
- `engine.registry`: sync.Map for (shardID, replicaID) → address
- `Barrier.unsealed`: atomic.Bool fast-path before RWMutex acquisition

## Configuration

### HostConfig (per-host)

- `WALDir` (required) — Write-ahead log directory
- `NodeHostDir` (required) — Metadata, snapshots, host ID
- `ListenAddress` (required) — Local bind address (e.g., `:4001`)
- `RaftAddress` — External address seen by peers (default: ListenAddress)
- `RTTMillisecond` (200ms) — Estimated round-trip time for timeout calculations
- `DeploymentID` (required) — Cluster identity token (must match all nodes)
- `NumWorkers` (runtime.NumCPU()) — Worker goroutines per tier (step, commit, apply)
- `MaxApplyWorkers` (runtime.NumCPU()) — Max concurrent apply workers
- `CommitCBufferSize` (64) — Commit channel buffer per worker
- `ShutdownTimeout` (30s) — Graceful shutdown timeout
- `MaxApplyRetries` (100) — Circuit breaker threshold before shard fails
- `MaxTotalInMemLogSize` (0 = disabled) — Global rate limiter threshold

See `pkg/config/hostconfig.go`.

### Config (per-shard)

- `ElectionRTT` (10) — Election timeout as multiples of RTTMillisecond
- `HeartbeatRTT` (1) — Heartbeat interval as multiples of RTTMillisecond
- `CheckQuorum` (true) — Leader steps down if quorum becomes unavailable
- `PreVote` (true) — Use pre-vote extension to prevent disruptive elections
- `OrderedConfigChange` (true) — Serialize membership changes (no concurrent config changes)
- `SnapshotEntries` (0 = disabled) — Auto-snapshot trigger interval
- `CompactionOverhead` (5000) — Entries retained after compaction
- `MaxEntrySize` (8 MB) — Maximum single proposal size
- `MaxApplyEntrySize` (64 MB) — Maximum batch size per SM.Update call
- `MaxSnapshotSize` (4 GB) — Maximum snapshot file size
- `MaxSessionsPerClient` (128) — Per-client session limit
- `MaxTotalSessions` (16384) — Per-shard total session limit
- `Quiesce` (false) — Enable quiescence for idle shards (leader sends fewer heartbeats)

See `pkg/config/config.go`.

### TransportConfig

- `StreamPoolSize` (16) — Streams per QUIC connection
- `MaxStreamPoolSize` (256) — Upper bound for dynamic pool growth
- `MaxSnapshotReceiveRate` (256 MB/s) — Inbound snapshot bandwidth limit
- `MaxConcurrentSnapshotReceives` (4) — Concurrent inbound snapshot transfers
- `MaxSnapshotReceiveMemory` (1 GB) — Total snapshot receive buffer pool
- `SendBatchMaxSize` (64 KB) — Maximum batch size for sending to one peer
- `MaxIncomingConnections` (256) — Total inbound connections allowed
- `Enable0RTT` (false) — QUIC 0-RTT (replayable, use with caution)
- `DisableCompression` (false) — Disable QUIC header compression (for benchmarking)

See `pkg/config/transportconfig.go`.

## State Machine Interface

QuicRaft supports three state machine tiers (detected at startup via type assertion):

See `pkg/sm/interfaces.go`.

### StateMachine (tier 0)

All state in memory, serialized access (Update and Lookup never run
concurrently). Simplest to implement:

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

Detected via type assertion on the SM:

- `Hasher` — `GetHash() (uint64, bool, error)` for cross-replica consistency checks
- `NALookup` — `BytesLookup(key []byte) ([]byte, bool, error)` for zero-alloc reads

The RSM adapter uses a `(value, supported, error)` pattern:
- If SM implements the interface: return (result, true, error)
- If SM does not implement: return (zero, false, nil)
- SM errors are always propagated to the caller

## Discovery and Bootstrap

### Discovery

Three built-in discovery methods in `pkg/discovery/`:

| Method               | Description                                     |
|---------------------|---------------------------------------------|
| `StaticDiscovery`   | Fixed peer list, known at deployment time   |
| `MulticastDiscovery`| UDP multicast with HMAC-SHA256 authentication |
| `DNSDiscovery`      | DNS SRV record lookup (`_raft._udp.domain`) |

All implement `discovery.Method`. The `Manager` tries methods in order with
configurable retry:

```go
mgr := discovery.NewManager(discovery.ManagerConfig{
    Methods:       []discovery.Method{static, dns},
    RetryInterval: time.Second,
    MaxRetries:    10,
})
peers, err := mgr.Discover()
```

### Bootstrap

The `Bootstrapper` in `pkg/bootstrap/` orchestrates cluster formation:

1. Discover peers via configured `discovery.Method`
2. Add self to peer list (deduplicated)
3. Validate quorum size
4. Build member map (replicaID → address)
5. Call `host.StartShard(shardID, replicaID, members, createFn, config)`

```go
bs := bootstrap.NewBootstrapper(bootstrap.Config{
    ShardID:     100,
    ReplicaID:   1,
    Address:     "10.0.0.1:4001",
    Discovery:   staticDiscovery,
    CreateFn:    sm.NewCreateFunc(newKVStore),
    ShardConfig: cfg,
    MinPeers:    3,
}, host)
err := bs.Bootstrap()
```

## At-Rest Encryption

The `Barrier` in `pkg/seal/barrier.go` provides AEAD encryption for WAL data at rest.

**Algorithm selection** (automatic based on hardware):
- x86-64 with AES-NI or ARM64 with crypto extensions → AES-256-GCM
- Otherwise → ChaCha20-Poly1305
- Configurable via `BarrierConfig.Algorithm`

**Design**:
- Root key (32 bytes) from `crypto/rand`
- Data Encryption Keys (DEKs) derived via HKDF-SHA256 with per-epoch info strings
- Ciphertext format: `Epoch(8) | Nonce(12) | EncryptedData | Tag(16)`
- Key rotation: `Rotate()` increments epoch, derives new DEK
- Old DEKs retained for reading prior-epoch data
- Brute-force protection: exponential backoff after 5 failed Unseal attempts

**Lifecycle**: Initialize → Unseal → Encrypt/Decrypt → Seal
