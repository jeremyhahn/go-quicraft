# Public API Reference

Source: `github.com/jeremyhahn/go-quicraft/pkg`

The `quicraft` package is the public entry point for QuicRaft, a multi-group Raft consensus library. One `Host` per process manages shard lifecycle, proposals, linearizable reads, sessions, membership, snapshots, and shutdown. The library uses QUIC transport, custom binary encoding, and fixed-layout serialization for zero-copy reads and high throughput.

## Host

`Host` is the main entry point. One Host per process manages thousands of independent Raft shards.

### Constructor

```go
func NewHost(cfg config.HostConfig, opts ...HostOption) (*Host, error)
```

Creates a Host with the given configuration. Validates the config, creates or loads a host ID from disk, initializes LogDB, registry, QUIC transport, engine, and the worker pipeline. Starts the transport listener and engine workers before returning.

**HostOption functions:**

| Option                | Description                                          |
|-----------------------|------------------------------------------------------|
| `WithLogDB(l)`        | Custom LogDB. Default: WAL-based from HostConfig dirs |
| `WithMemoryLogDB()`   | In-memory LogDB (benchmarks, CI/CD, ephemeral nodes)  |
| `WithBarrier(b)`      | At-rest encryption. Barrier must be initialized/unsealed |
| `WithMetrics(c)`      | Enable metrics collection via MetricsCollector        |
| `WithLogger(l)`       | Custom `*slog.Logger`. Default: built from LogConfig  |
| `WithoutTransport()`  | No-op sender (single-node testing only)               |

**Errors:** `*HostInitError` wrapping the component name and underlying error. Returns `*config.ValidationError` if config is invalid.

### Shard Lifecycle

#### StartShard

```go
func (h *Host) StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error
```

Starts a Raft shard on this host. `members` maps replicaID to address for initial cluster members. `join` is true when joining an existing cluster (skip bootstrap). `create` is the state machine factory. `cfg.ShardID` and `cfg.ReplicaID` must both be non-zero.

The method detects the SM type via probe instance, creates a LogReader, Raft Peer, RSM, and engine Node, then loads the node into the engine pipeline. Registers all members in the address registry and notifies the engine to begin processing.

**Errors:**

| Error                | Condition                                      |
|----------------------|------------------------------------------------|
| `ErrClosed`          | Host is closed                                 |
| `ErrInvalidConfig`   | ShardID==0, ReplicaID==0, or validation fails  |
| `ErrShardAlreadyExists` | Shard already loaded on this host           |
| `ErrInvalidSMType`   | CreateFunc returns nil or unknown SM type       |
| `*HostInitError`     | Peer, SM, or LogDB initialization failed        |
| `ErrLogDBWrite`      | Bootstrap save failed                          |

**Concurrency:** Acquires `nodesMu` write lock. Safe for concurrent calls with different shardIDs.

#### StopShard

```go
func (h *Host) StopShard(shardID uint64) error
```

Stops all replicas of the given shard. Fails all pending proposals, reads, and snapshots with `ErrAborted`. Unloads the engine node and removes registry entries.

**Errors:** `ErrClosed`, `ErrShardNotFound`.

### Proposal Path

#### Propose

```go
func (h *Host) Propose(ctx context.Context, shardID uint64, cmd []byte) (*RequestState, error)
```

Submits an asynchronous proposal through Raft. Returns a `*RequestState` that the caller must poll via `Result()` and release via `Release()`.

Validates: host not closed/draining, shard exists, entry size within `MaxEntrySize`, local node is leader, global and per-shard rate limiters not exceeded, engine node ready.

Generates a monotonic `proposalKey`, registers in `pendingProposals` before delivering to the engine inbox (single-owner invariant on Peer), then wakes the step worker.

**Errors:**

| Error                   | Condition                                    |
|-------------------------|----------------------------------------------|
| `ErrClosed`             | Host closed                                  |
| `ErrDraining`           | Host in drain mode                           |
| `ErrShardNotFound`      | Shard not loaded                             |
| `*EntryTooLargeError`   | `len(cmd) > MaxEntrySize`                    |
| `*NotLeaderError`       | Local node is not leader (carries leader hint)|
| `ErrSystemBusy`         | Global rate limit or inbox full              |
| `*BusyError`            | Per-shard rate limit exceeded                |
| `ErrShardNotReady`      | Engine node or Peer not initialized          |

**Concurrency:** Safe for concurrent calls. Each call gets a unique `proposalKey` via `atomic.Uint64`.

#### SyncPropose

```go
func (h *Host) SyncPropose(ctx context.Context, shardID uint64, cmd []byte) (sm.Result, error)
```

Synchronous wrapper around `Propose`. Blocks until the result is available or `ctx` is cancelled. Handles `RequestState` lifecycle internally (calls `Release()` on defer).

Returns `sm.Result{Value, Data}` from the state machine.

#### ProposeWithSession

```go
func (h *Host) ProposeWithSession(ctx context.Context, session *Session, cmd []byte) (*RequestState, error)
```

Submits an asynchronous session-managed proposal for at-most-once delivery. The session tracks the series ID for deduplication. The caller must call `session.ProposalCompleted()` after a successful result.

#### SyncProposeWithSession

```go
func (h *Host) SyncProposeWithSession(ctx context.Context, session *Session, cmd []byte) (sm.Result, error)
```

Synchronous wrapper around `ProposeWithSession`. Blocks until the result is available. Automatically calls `session.ProposalCompleted()` on success.

### Linearizable Read Path

#### ReadIndex

```go
func (h *Host) ReadIndex(ctx context.Context, shardID uint64) (*RequestState, error)
```

Submits a ReadIndex request to confirm leadership via heartbeat quorum. Returns a `*RequestState` that resolves when the local SM has applied up to the confirmed commit index. The result's `Value` field carries the confirmed read index.

Witnesses cannot serve reads (`ErrInvalidOperation`). Observers and followers forward the ReadIndex to the leader.

**Errors:** Same as `Propose` except no entry-size or per-shard rate limit check. `ErrInvalidOperation` if called on a witness.

#### SyncRead

```go
func (h *Host) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
```

Performs a linearizable read in a single call:

1. Submits `ReadIndex` to confirm leadership via heartbeat quorum
2. Waits for `RequestState` to resolve (lastApplied >= readIndex)
3. Executes `SM.Lookup(query)` on the caught-up state machine

Returns the query result from the state machine. This is the strongest consistency guarantee: the read reflects all writes committed before it.

#### SyncReadBuf

```go
func (h *Host) SyncReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error)
```

Zero-allocation variant of `SyncRead` for state machines implementing the `NALookup` interface. Returns a borrowed byte slice and a release function. The caller must call release to restore the buffer to the pool.

**Errors:** Same as `SyncRead`. Returns `ErrInvalidOperation` if the SM does not implement `NALookup`.

#### StaleRead

```go
func (h *Host) StaleRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
```

Reads local SM state directly via `SM.Lookup`. **NOT linearizable** -- the result may be arbitrarily stale. For linearizable reads, use `SyncRead`. Identical to `QueryLocalNode`.

#### StaleReadBuf

```go
func (h *Host) StaleReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error)
```

Zero-allocation variant of `StaleRead` for state machines implementing `NALookup`. Returns a borrowed byte slice and a release function.

#### QueryLocalNode

```go
func (h *Host) QueryLocalNode(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
```

Reads local SM state via `SM.Lookup`. **NOT linearizable** -- the result may be stale. For linearizable reads, use `SyncRead`.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrShardNotReady`.

### Leader Transfer

```go
func (h *Host) RequestLeaderTransfer(ctx context.Context, shardID, targetReplicaID uint64) error
```

Requests leadership transfer. Pass `targetReplicaID=0` to transfer to the most up-to-date follower. Fire-and-forget: returns immediately, transfer completes asynchronously.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `*NotLeaderError`, `ErrShardNotReady`, `ErrSystemBusy`.

### Membership Operations

All membership operations are config changes proposed through Raft. Each returns a `*RequestState` that resolves when the config change is committed and applied.

#### Async Config Changes

```go
func (h *Host) RequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestRemoveNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
```

| Method              | Config Change Type | Description                           |
|---------------------|--------------------|---------------------------------------|
| `RequestAddNode`    | `AddNode`          | Add a voting replica                  |
| `RequestRemoveNode` | `RemoveNode`       | Remove a replica                      |
| `RequestAddObserver`| `AddNonVoting`     | Add non-voting observer (receives log)|
| `RequestAddWitness` | `AddWitness`       | Add voting witness (no SM)            |

All share `requestConfigChange()` which serializes a `proto.ConfigChange`, wraps it in an `EntryConfigChange` entry, and delivers via the engine inbox.

**Errors:** Same as `Propose` (requires leader, rate limit checks, inbox capacity). `*HostInitError` if config change serialization fails.

**Concurrency:** When `OrderedConfigChange` is true (default), only one membership change is processed at a time per shard.

The `configChangeIndex` parameter provides an optimistic concurrency guard: pass 0 to skip the check, or pass the `ConfigChangeID` from `SyncGetShardMembership` to reject stale requests.

#### Sync Config Changes

```go
func (h *Host) SyncRequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestDeleteNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) error
func (h *Host) SyncRequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
```

Synchronous wrappers around the async config change methods. Block until the config change is applied.

#### Membership Queries

```go
func (h *Host) SyncGetShardMembership(ctx context.Context, shardID uint64) (*Membership, error)
func (h *Host) GetShardMembership(shardID uint64) (*Membership, error)
```

`SyncGetShardMembership` performs a linearizable membership read (ReadIndex round-trip). `GetShardMembership` reads the local in-memory membership without a quorum check (may be stale).

### Snapshots

#### RequestSnapshot

```go
func (h *Host) RequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (*RequestState, error)
```

Requests an on-demand snapshot for the given shard. Returns a `*RequestState` whose `Value` field carries the snapshot index on success.

Atomically claims the shard's snapshotting slot via `TryStartSnapshot()`.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrShardNotReady`, `ErrSnapshotInProgress`.

#### SyncRequestSnapshot

```go
func (h *Host) SyncRequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (uint64, error)
```

Synchronous wrapper around `RequestSnapshot`. Blocks until the snapshot completes and returns the snapshot index.

#### ExportSnapshot

```go
func (h *Host) ExportSnapshot(ctx context.Context, shardID uint64, w io.Writer) error
```

Streams the latest snapshot data to the provided writer. Reads from LogDB metadata to locate the snapshot file on disk.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrSnapshotNotFound`.

#### ImportSnapshot

```go
func (h *Host) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader) error
```

Imports a snapshot from the reader. The shard **must be stopped** before calling. Writes snapshot data to disk and records metadata in LogDB for recovery on next `StartShard`.

**Errors:** `ErrClosed`, `ErrInvalidOperation` (shard running or no LogDB), `ErrShardNotFound`.

### Informational

```go
func (h *Host) ID() string
func (h *Host) RaftAddress() string
func (h *Host) ShardCount() int
func (h *Host) GetLeaderID(shardID uint64) (leaderID uint64, known bool, err error)
func (h *Host) GetLeaderIDAndTerm(shardID uint64) (leaderID, term uint64, known bool, err error)
func (h *Host) GetShardInfo(shardID uint64) (*ShardInfo, error)
func (h *Host) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo
```

`GetLeaderID` and `GetLeaderIDAndTerm` return from local atomic state and may be stale. `GetNodeHostInfo` returns a snapshot of all loaded shards. Pass `NodeHostInfoOption{SkipLogInfo: true}` for faster health-check queries.

### Health Check

```go
func (h *Host) HealthCheck() *HealthStatus
```

Returns a point-in-time health assessment:

| Status      | Condition                              | HTTP Code |
|-------------|----------------------------------------|-----------|
| `"sealed"`  | Barrier configured and sealed          | 503       |
| `"unhealthy"` | No shards loaded                     | 503       |
| `"degraded"` | Any shard has `ShardError` health     | 503       |
| `"ok"`      | All shards healthy                     | 200       |

Checks barrier state, disk full flag, and per-shard health.

### Encryption / Barrier

```go
func (h *Host) EmergencySeal() error
func (h *Host) IsSealed() bool
```

`EmergencySeal` immediately seals the barrier without waiting for in-flight operations. All subsequent operations fail with `ErrBarrierSealed`. For security incident response.

`IsSealed` returns false if no barrier is configured.

### Data Removal

```go
func (h *Host) RemoveData(shardID, replicaID uint64) error
func (h *Host) SyncRemoveData(ctx context.Context, shardID, replicaID uint64) error
```

Removes all persistent data for a replica (logs, snapshots, metadata). The shard must be stopped first. `SyncRemoveData` is the synchronous variant (included for API consistency).

**Errors:** `ErrClosed`, `ErrShardNotFound`, `*ShardNotStoppedError` (shard still running).

### Replica Management

```go
func (h *Host) StopReplica(shardID, replicaID uint64) error
```

Stops a specific replica on a shard. Since each host runs at most one replica per shard, this validates the replicaID matches and delegates to `StopShard`.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `*ReplicaNotFoundError`.

#### RequestCompaction

```go
func (h *Host) RequestCompaction(shardID, replicaID uint64) error
```

Triggers explicit log compaction for the given shard/replica pair. This reclaims disk space by removing entries superseded by snapshots.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `*ReplicaNotFoundError`.

#### HasNodeInfo

```go
func (h *Host) HasNodeInfo(shardID, replicaID uint64) bool
```

Returns true if metadata exists for the given shard/replica pair on disk.

### Log Access

```go
func (h *Host) GetLogReader(shardID uint64) (ReadonlyLogReader, error)
```

Returns a `ReadonlyLogReader` for direct log access. The reader is point-in-time; call again to see new entries.

**Errors:** `ErrClosed`, `ErrShardNotFound`.

#### QueryRaftLog

```go
func (h *Host) QueryRaftLog(ctx context.Context, shardID uint64, firstIndex, lastIndex, maxSize uint64) ([]proto.Entry, error)
```

Queries a range of Raft log entries. `maxSize` limits the total returned bytes. Returns entries in the requested range, or as many as fit in `maxSize`.

**Errors:** `ErrClosed`, `ErrShardNotFound`.

#### GetLogRange

```go
func (h *Host) GetLogRange(shardID uint64) (LogRange, error)
```

Returns the first and last log indices for the shard.

**Errors:** `ErrClosed`, `ErrShardNotFound`.

### Diagnostics

```go
func (h *Host) DiagPipelineState(shardID uint64) (commitPending bool, proposalQLen int, inboxLen int, pendingProposals int, err error)
func (h *Host) DiagRaftState(shardID uint64) (term, committed, processed, lastIdx uint64, msgs int, state string, err error)
func (h *Host) DiagRemotes(shardID uint64) (string, error)
```

Low-level diagnostics for troubleshooting. `DiagPipelineState` shows commit queuing, proposal queue depth, and inbox backlog. `DiagRaftState` shows Raft term, commit index, processed index, log size, and state string (Follower/Candidate/Leader). `DiagRemotes` returns a textual representation of peer replication states.

### Shutdown

#### Drain

```go
func (h *Host) Drain() error
func (h *Host) IsDraining() bool
```

`Drain` transitions to drain mode: new proposals and ReadIndex requests are rejected with `ErrDraining`. In-flight work is allowed to complete. Blocks until all pending proposals, reads, and snapshots complete, or `ShutdownTimeout` expires (`ErrShutdownTimeout`). Idempotent.

`IsDraining` returns true if the host is in drain mode.

#### Close

```go
func (h *Host) Close() error
```

Shuts down the host. Calls `Drain()` first, then stops the key rotation goroutine, transport, engine, all nodes, closes LogDB, and seals the barrier. Idempotent via `sync.Once`.

**Shutdown order:** Drain -> Key rotation stop -> Transport stop -> Engine stop -> Node cleanup -> LogDB close -> Barrier seal.

## RequestState

```go
type RequestState struct { /* unexported fields */ }
```

Tracks a single asynchronous request through the Raft pipeline. Callers obtain a `*RequestState` from `Propose`, `ReadIndex`, `RequestSnapshot`, or membership operations.

### Lifecycle

1. **Created** by `newRequestState` from a `sync.Pool` with a pre-allocated buffered channel (capacity 1)
2. **Pending** -- registered in the Host's `pendingProposals`, `pendingReads`, or `pendingSnapshots` map, keyed by a unique monotonic `proposalKey`
3. **Completed** -- the engine callback (`OnApplied`, `OnReadyToRead`, `OnSnapshotCompleted`, or `OnLeaderUpdated`) sends a `RequestResult` to the channel via `complete()`
4. **Released** -- caller calls `Release()` to return the object to the pool

### Methods

```go
func (rs *RequestState) Result() (RequestResult, error)
```

Blocks until the request completes or the deadline expires. Returns `ErrReleased` if already released. Returns `ErrTimeout` if the deadline (from the originating context) expires.

When no deadline is set (zero `time.Time`), blocks indefinitely.

```go
func (rs *RequestState) ResultC() <-chan RequestResult
```

Returns a read-only channel that receives exactly one result. Returns `nil` if released. Use for `select`-based non-blocking consumption.

```go
func (rs *RequestState) CommittedC() <-chan RequestResult
```

Returns a read-only channel that receives an early WAL-durable notification (~1-2ms). The result carries `Value=0` and nil `Data` because the state machine has not yet applied the entry. Returns `nil` if released.

Only signaled when `NotifyCommit` is enabled on the host. When disabled, this channel never receives.

```go
func (rs *RequestState) ApplyResultC() <-chan RequestResult
```

Returns a read-only channel that receives the full state machine apply result. Identical to `ResultC()` and always returns the `completedC` channel. Provided for clarity when both `CommittedC()` and the apply result are needed in the same select.

```go
func (rs *RequestState) Release()
```

Returns the `RequestState` to the pool. Idempotent. After release, `Result()` returns `ErrReleased` and `ResultC()` returns nil.

```go
func (rs *RequestState) Key() uint64
func (rs *RequestState) Deadline() time.Time
```

Accessors for the request key and deadline.

### Pool Pattern

`NewRequestStatePool()` creates a `sync.Pool` that produces `RequestState` objects with pre-allocated channels. Each `nodeState` holds its own pool. The pool pattern reduces GC pressure on the proposal hot path.

## RequestResult

```go
type RequestResult struct {
    Value uint64   // Application-defined result code from sm.Result
    Data  []byte   // Optional result data from sm.Result
    Err   error    // Non-nil on failure (timeout, canceled, rejected, etc.)
}
```

Returned by `RequestState.Result()` and delivered on `RequestState.ResultC()`.

For proposals: `Value` and `Data` come from `sm.Result` written by the state machine's `Update()`.

For ReadIndex: `Value` carries the confirmed read index.

For snapshots: `Value` carries the snapshot index on success.

On failure: `Err` carries the error (`*NotLeaderError`, `ErrAborted`, `ErrTimeout`, etc.).

## Session API

Sessions provide **at-most-once** proposal semantics. The session manager uses `(clientID, seriesID)` to detect and deduplicate retransmissions.

### Session Type

```go
type Session struct { /* unexported fields */ }
```

Fields are unexported to prevent mutation that would break deduplication. Accessors:

```go
func (s *Session) ShardID() uint64
func (s *Session) ClientID() uint64
func (s *Session) SeriesID() uint64
func (s *Session) RespondedTo() uint64
func (s *Session) IsNoOPSession() bool
```

### Constructors

```go
func NewSession(shardID, clientID uint64) *Session
func NewNoOPSession(shardID uint64) *Session
```

`NewSession` creates a tracked Session for the given shard and client ID. The clientID must not be zero; zero is reserved for no-op sessions. The session is created in an unregistered state.

`NewNoOPSession` creates a no-op session with clientID=0 that bypasses deduplication.

### Registration Flow

Session registration and unregistration are proposed through Raft consensus so all replicas maintain consistent session state.

```go
func (h *Host) GetNewSession(ctx context.Context, shardID uint64) (*Session, error)
```

1. Generates a cryptographic random `clientID` (8 bytes from `crypto/rand`)
2. Sets `seriesID = SeriesIDForRegister` (sentinel: `math.MaxUint64-1`)
3. Proposes registration through Raft via `proposeSession`
4. Blocks until committed and applied
5. Validates result (`Value == clientID`)
6. Sets `seriesID = SeriesIDFirstProposal` (1) for normal proposals

**Errors:** `ErrClosed`, `ErrShardNotFound`, `*NotLeaderError`, `ErrSystemBusy`, `*SessionError`.

```go
func (h *Host) RegisterSession(ctx context.Context, session *Session) (*Session, error)
```

Registers a pre-created session through Raft. Returns the session on success.

```go
func (h *Host) CloseSession(ctx context.Context, shardID uint64, session *Session) (*RequestState, error)
```

Proposes unregistration through Raft. Sets `seriesID = SeriesIDForUnregister` (sentinel: `math.MaxUint64`). Returns a `*RequestState`.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrInvalidOperation` (nil session), `ErrInvalidSession` (wrong shard or no-op session).

### No-Op Sessions

```go
func (h *Host) GetNoOPSession(shardID uint64) *Session
```

Returns a no-op session with `clientID=0`. No-op sessions bypass deduplication entirely. Use only for idempotent operations.

### Session Constants

| Constant                  | Value               | Usage                        |
|---------------------------|---------------------|------------------------------|
| `SeriesIDForRegister`     | `math.MaxUint64-1`  | Register sentinel            |
| `SeriesIDForUnregister`   | `math.MaxUint64`    | Unregister sentinel          |
| `SeriesIDFirstProposal`   | `1`                 | First normal proposal series |

### Session Methods

```go
func (s *Session) PrepareForPropose()
func (s *Session) ProposalCompleted()
func (s *Session) PrepareForRegister()
func (s *Session) PrepareForUnregister()
```

`PrepareForPropose()` sets the session for normal proposal mode, advancing seriesID to `SeriesIDFirstProposal`.

`ProposalCompleted()` advances the series ID and updates respondedTo after a proposal completes.

`PrepareForRegister()` and `PrepareForUnregister()` set up the session for registration/unregistration operations.

### Session Validation

```go
func (s *Session) ValidForSessionOp(shardID uint64) bool
func (s *Session) ValidForProposal(shardID uint64) bool
```

`ValidForSessionOp` returns true if the session belongs to the given shard and is not a no-op session.

`ValidForProposal` returns true if the session belongs to the given shard.

## Membership

```go
type Membership struct {
    ConfigChangeID uint64                // Raft log index of this config change
    Nodes          map[uint64]string     // Full voting members (replicaID -> addr)
    Observers      map[uint64]string     // Non-voting learners (replicaID -> addr)
    Witnesses      map[uint64]string     // Voting members without SM (replicaID -> addr)
    Removed        map[uint64]struct{}   // Removed replica IDs
}
```

Describes the current cluster membership for a shard. Returned by `Host.SyncGetShardMembership`.

```go
func (m *Membership) ConfigChangeIndex() uint64
```

Returns the `ConfigChangeID`. This alias exists for backward compatibility.

```go
func NewMembership() *Membership
```

Creates a Membership with all maps initialized to empty.

## Error Types

### Sentinel Errors

All sentinel errors use the `"quicraft:"` prefix. Check with `errors.Is()`.

| Error                     | Returned When                                       |
|---------------------------|-----------------------------------------------------|
| `ErrClosed`               | Operation on a closed Host                          |
| `ErrDraining`             | New work submitted during drain                     |
| `ErrTimeout`              | Request deadline expired                            |
| `ErrCanceled`             | Request canceled via context                        |
| `ErrRejected`             | Proposal rejected (e.g., config change in progress) |
| `ErrAborted`              | Internal state transition (leadership change, stop) |
| `ErrDropped`              | Proposal dropped (queue overflow, shutdown)         |
| `ErrShardNotFound`        | Shard not loaded on this Host                       |
| `ErrShardNotStarted`      | Shard registered but not initialized                |
| `ErrShardNotReady`        | Shard initializing, cannot serve requests           |
| `ErrShardAlreadyExists`   | StartShard for an already-running shard             |
| `ErrShardFailed`          | SM circuit breaker tripped                          |
| `ErrBusy`                 | Per-shard in-memory log exceeded MaxInMemLogSize    |
| `ErrSystemBusy`           | Global rate limit exceeded or inbox full            |
| `ErrNotLeader`            | Sentinel for `errors.Is()` matching                 |
| `ErrInvalidConfig`        | Config validation failure                           |
| `ErrInvalidOperation`     | Invalid for current state (e.g., witness read)      |
| `ErrInvalidSMType`        | CreateFunc returns unrecognized type                |
| `ErrInvalidSession`       | Wrong shard or no-op session for session op         |
| `ErrReleased`             | Result/ResultC on released RequestState             |
| `ErrLogDBWrite`           | LogDB write failure                                 |
| `ErrShutdownTimeout`      | Drain exceeded ShutdownTimeout                      |
| `ErrEntryTooLarge`        | Sentinel for `errors.Is()` matching                 |
| `ErrSnapshotNotFound`     | No snapshot available on disk                       |
| `ErrSnapshotInProgress`   | Snapshot already in progress for this shard         |
| `ErrSnapshotRecoveryFailed` | Snapshot restore corruption/incompatibility       |
| `ErrBarrierSealed`        | Operation while barrier is sealed                   |
| `ErrBarrierNotInit`       | Barrier not initialized                             |
| `ErrDiskFull`             | WAL directory at configured max size                |
| `ErrDecryptionFailed`     | Corrupted ciphertext or wrong key                   |
| `ErrRotateInProgress`     | Concurrent key rotation attempt                     |
| `ErrDecompressionBomb`    | Decompressed payload exceeds limit                  |
| `ErrSessionLimitExceeded` | Per-client or total session limit reached           |
| `ErrSessionNotFound`      | Referenced session does not exist                   |
| `ErrSessionAlreadyExists` | Registering a duplicate session                     |
| `ErrResponseLimitExceeded`| Session response cache full                         |
| `ErrStopped`              | Stopper channel closed                              |
| `ErrReadIndexNotReady`    | Leader has no committed entry in current term       |

### Structured Error Types

#### NotLeaderError

```go
type NotLeaderError struct {
    ShardID       uint64
    LeaderID      uint64 // 0 if unknown
    LeaderAddress string // empty if unknown
}
```

Returned by `Propose`, `ReadIndex`, membership operations, and `RequestLeaderTransfer` when the local node is not the leader. Carries leader hint for immediate client redirect. Satisfies `errors.Is(err, ErrNotLeader)`.

#### EntryTooLargeError

```go
type EntryTooLargeError struct {
    Size    uint64
    MaxSize uint64
}
```

Returned by `Propose` when `len(cmd) > MaxEntrySize`. Satisfies `errors.Is(err, ErrEntryTooLarge)`.

#### BusyError

```go
type BusyError struct {
    ShardID     uint64
    CurrentSize uint64
    MaxSize     uint64
}
```

Returned by `Propose` when the per-shard in-memory log exceeds `MaxInMemLogSize`. Satisfies `errors.Is(err, ErrBusy)`.

#### SessionError

```go
type SessionError struct {
    ClientID uint64
    Op       string // "register" or "unregister"
    Err      error
}
```

Wraps session operation failures. Implements `Unwrap()`.

#### SessionExpiredError

```go
type SessionExpiredError struct {
    ClientID uint64
}
```

Returned when a session is expired on the leader. Satisfies `errors.Is(err, ErrSessionExpired)`.

#### ShardNotStoppedError

```go
type ShardNotStoppedError struct {
    ShardID   uint64
    ReplicaID uint64
}
```

Returned when attempting to remove data for a running shard. Satisfies `errors.Is(err, ErrShardNotStopped)`.

#### ReplicaNotFoundError

```go
type ReplicaNotFoundError struct {
    ShardID   uint64
    ReplicaID uint64
}
```

Returned when a replica ID does not match the local replica. Satisfies `errors.Is(err, ErrReplicaNotFound)`.

#### StaleConfigChangeError

```go
type StaleConfigChangeError struct {
    ShardID        uint64
    ExpectedIndex  uint64
    CurrentIndex   uint64
}
```

Returned when a config change index guard fails. Satisfies `errors.Is(err, ErrStaleConfigChange)`.

#### HostInitError

```go
type HostInitError struct {
    Field string
    Err   error
}
```

Wraps component initialization failures in `NewHost` and `StartShard`. Implements `Unwrap()`.

#### SnapshotSendError

```go
type SnapshotSendError struct {
    To  uint64
    Err error
}
```

Returned when a snapshot send fails. Carries the target replica ID and underlying error. Implements `Unwrap()`.

#### SnapshotReceiveError

```go
type SnapshotReceiveError struct {
    From uint64
    Err  error
}
```

Returned when a snapshot receive fails. Carries the sender replica ID and underlying error. Implements `Unwrap()`.

#### UnauthorizedMessageError

```go
type UnauthorizedMessageError struct {
    From uint64
    To   uint64
}
```

Returned when a message is received from an unauthorized source.

#### TypeAssertionError

```go
type TypeAssertionError struct {
    Context      string
    ExpectedType string
    ActualValue  interface{}
}
```

Wraps internal type assertion failures for debugging.

#### LoopPanicError

```go
type LoopPanicError struct {
    Loop  string
    Panic interface{}
}
```

Wraps recovered panics from goroutine loops.

#### EmptyMembersError

```go
type EmptyMembersError struct {}
```

Returned when initializing a shard with no members.

#### SelfNotInMembersError

```go
type SelfNotInMembersError struct {
    ShardID   uint64
    ReplicaID uint64
}
```

Returned when the local replica is not in the initial members list.

#### InvalidMemberError

```go
type InvalidMemberError struct {
    ShardID   uint64
    ReplicaID uint64
    Addr      string
}
```

Returned when a member address is invalid or empty.

## Metrics Interface

### MetricsCollector

```go
type MetricsCollector interface {
    RaftMetrics
    LogDBMetrics
    TransportMetrics
}
```

Composes three sub-interfaces. Passed to `WithMetrics()`. Internally, QuicRaft passes only the relevant sub-interface to each component.

### RaftMetrics (8 methods)

| Method                      | Signature                                           |
|-----------------------------|-----------------------------------------------------|
| `ObserveProposalLatency`    | `(shardID uint64, d time.Duration)`                 |
| `ObserveCommitLatency`      | `(shardID uint64, d time.Duration)`                 |
| `ObserveApplyLatency`       | `(shardID uint64, d time.Duration)`                 |
| `IncProposalDropped`        | `(shardID uint64)`                                  |
| `ObserveProposalBatchSize`  | `(shardID uint64, size int)`                        |
| `ObserveCommitBatchSize`    | `(size int)`                                        |
| `IncElection`               | `(shardID uint64)`                                  |
| `IncSnapshot`               | `(shardID uint64)`                                  |

### LogDBMetrics (4 methods)

| Method                    | Signature              |
|---------------------------|------------------------|
| `ObserveWriteLatency`     | `(d time.Duration)`    |
| `ObserveFsyncLatency`     | `(d time.Duration)`    |
| `ObserveCompactionLatency`| `(d time.Duration)`    |
| `IncWriteBytes`           | `(n int64)`            |

### TransportMetrics (5 methods)

| Method                   | Signature                                                  |
|--------------------------|------------------------------------------------------------|
| `ObserveSendLatency`     | `(d time.Duration)`                                        |
| `IncMessagesSent`        | `(shardID, to uint64, msgType uint64)`                     |
| `IncMessagesReceived`    | `(shardID, from uint64, msgType uint64)`                   |
| `IncSnapshotsSent`       | `(shardID uint64, bytes int64, d time.Duration)`           |
| `IncSnapshotsReceived`   | `(shardID uint64, bytes int64, d time.Duration)`           |

### NoOpMetricsCollector

```go
type NoOpMetricsCollector struct{}
```

All 17 methods are no-ops. Embed and override selectively to avoid implementing the full interface.

### PrometheusMetricsCollector

```go
func NewPrometheusMetricsCollector() *PrometheusMetricsCollector
```

Production implementation using `prometheus/client_golang`. All metrics are registered on a dedicated `prometheus.Registry` (exposed as `collector.Registry`) to avoid polluting the global default.

**Prometheus metric names:**

| Subsystem   | Metric Name                               | Type      | Labels                    |
|-------------|-------------------------------------------|-----------|---------------------------|
| `raft`      | `quicraft_raft_proposal_latency_seconds`  | Histogram | `shard_id`                |
| `raft`      | `quicraft_raft_commit_latency_seconds`    | Histogram | `shard_id`                |
| `raft`      | `quicraft_raft_apply_latency_seconds`     | Histogram | `shard_id`                |
| `raft`      | `quicraft_raft_proposals_dropped_total`   | Counter   | `shard_id`                |
| `raft`      | `quicraft_raft_proposal_batch_size`       | Histogram | `shard_id`                |
| `raft`      | `quicraft_raft_commit_batch_size`         | Histogram | --                        |
| `raft`      | `quicraft_raft_elections_total`           | Counter   | `shard_id`                |
| `raft`      | `quicraft_raft_snapshots_total`           | Counter   | `shard_id`                |
| `logdb`     | `quicraft_logdb_write_latency_seconds`    | Histogram | --                        |
| `logdb`     | `quicraft_logdb_fsync_latency_seconds`    | Histogram | --                        |
| `logdb`     | `quicraft_logdb_compaction_latency_seconds` | Histogram | --                      |
| `logdb`     | `quicraft_logdb_write_bytes_total`        | Counter   | --                        |
| `transport` | `quicraft_transport_send_latency_seconds` | Histogram | --                        |
| `transport` | `quicraft_transport_messages_sent_total`  | Counter   | `shard_id`, `to`, `msg_type` |
| `transport` | `quicraft_transport_messages_received_total` | Counter | `shard_id`, `from`, `msg_type` |
| `transport` | `quicraft_transport_snapshots_sent_total` | Counter   | `shard_id`                |
| `transport` | `quicraft_transport_snapshots_sent_bytes` | Histogram | `shard_id`                |
| `transport` | `quicraft_transport_snapshots_received_total` | Counter | `shard_id`              |
| `transport` | `quicraft_transport_snapshots_received_bytes` | Histogram | `shard_id`            |

Latency buckets: 100us to 10s. Batch size buckets: 1 to 1024 (powers of 2). Snapshot byte buckets: 1KB to 1GB.

## Configuration

### HostConfig (per-host)

Source: `pkg/config/hostconfig.go`

| Field                   | Type            | Default              | Description                              |
|-------------------------|-----------------|----------------------|------------------------------------------|
| `WALDir`                | `string`        | (required)           | Write-ahead log directory                |
| `NodeHostDir`           | `string`        | (required)           | Metadata, snapshots, host ID             |
| `RaftAddress`           | `string`        | `""`                 | External address seen by peers           |
| `ListenAddress`         | `string`        | (required)           | Local bind address (host:port)           |
| `RTTMillisecond`        | `uint64`        | `200`                | Estimated round-trip time (ms)           |
| `DeploymentID`          | `uint64`        | (required)           | Cluster identity token                   |
| `NumWorkers`            | `uint64`        | `runtime.NumCPU()`   | Workers per tier                         |
| `MaxApplyWorkers`       | `int`           | `runtime.NumCPU()`   | Max concurrent apply goroutines          |
| `CommitCBufferSize`     | `int`           | `64`                 | Commit channel buffer size               |
| `ShutdownTimeout`       | `time.Duration` | `30s`                | Graceful shutdown timeout                |
| `MaxWALDiskSize`        | `uint64`        | `0` (unlimited)      | Max WAL directory size (bytes)           |
| `MaxApplyRetries`       | `uint64`        | `100`                | Circuit breaker threshold                |
| `MaxTotalInMemLogSize`  | `uint64`        | `0` (disabled)       | Global rate limiter threshold (bytes)    |
| `KeyRotationInterval`   | `time.Duration` | `0` (disabled)       | Barrier key rotation interval            |
| `EnableMetrics`         | `bool`          | `false`              | Enable metrics collection                |
| `NotifyCommit`          | `bool`          | `false`              | Early WAL-durable notifications          |
| `EventListener`         | `*EventListener`| `nil`                | Event callbacks                          |
| `TransportConfig`       | `TransportConfig`| (see below)         | Transport tuning                         |
| `LogConfig`             | `LogConfig`     | text/info            | Logging format and level                 |

### Config (per-shard)

Source: `pkg/config/config.go`

| Field                   | Type              | Default    | Description                              |
|-------------------------|-------------------|------------|------------------------------------------|
| `ShardID`               | `uint64`          | (required) | Globally unique shard identifier         |
| `ReplicaID`             | `uint64`          | (required) | Unique replica ID within the shard       |
| `IsObserver`            | `bool`            | `false`    | Non-voting learner                       |
| `IsWitness`             | `bool`            | `false`    | Voting member, no state machine          |
| `CheckQuorum`           | `bool`            | `true`     | Leader step-down on quorum loss          |
| `PreVote`               | `bool`            | `true`     | Pre-vote protocol extension              |
| `OrderedConfigChange`   | `bool`            | `true`     | Serialize membership changes             |
| `ElectionRTT`           | `uint64`          | `10`       | Election timeout in RTT multiples        |
| `HeartbeatRTT`          | `uint64`          | `1`        | Heartbeat interval in RTT multiples      |
| `SnapshotEntries`       | `uint64`          | `0`        | Auto-snapshot interval (0 = disabled)    |
| `CompactionOverhead`    | `uint64`          | `5000`     | Entries retained after compaction        |
| `MaxInMemLogSize`       | `uint64`          | `0`        | Per-shard rate limit threshold (bytes)   |
| `MaxEntrySize`          | `uint64`          | `8 MB`     | Maximum proposal payload                 |
| `MaxApplyEntrySize`     | `uint64`          | `64 MB`    | Max batch size per SM.Update call        |
| `MaxSnapshotSize`       | `uint64`          | `4 GB`     | Maximum snapshot file size               |
| `MaxSessionsPerClient`  | `uint64`          | `128`      | Per-client session limit                 |
| `MaxTotalSessions`      | `uint64`          | `16384`    | Per-shard total session limit            |
| `Quiesce`               | `bool`            | `false`    | Quiescence for idle shards               |
| `ZeroCopyEntryCmd`      | `bool`            | `false`    | Zero-copy Entry.Cmd (unsafe if retained) |
| `LeaseRead`             | `bool`            | `false`    | Enable lease-based reads (PhD 6.4)       |

### TransportConfig

Source: `pkg/config/transportconfig.go`

| Field                          | Type            | Default     | Description                       |
|--------------------------------|-----------------|-------------|-----------------------------------|
| `StreamPoolSize`               | `int`           | `16`        | QUIC streams per connection       |
| `MaxStreamPoolSize`            | `int`           | `256`       | Upper bound for stream pool       |
| `MaxSnapshotReceiveRate`       | `int64`         | `256 MB/s`  | Inbound snapshot bandwidth limit  |
| `MaxConcurrentSnapshotReceives`| `int`           | `4`         | Concurrent inbound snapshots      |
| `MaxSnapshotReceiveMemory`     | `int64`         | `1 GB`      | Total snapshot receive buffer     |
| `MaxDecompressedSize`          | `uint32`        | `16 MB`     | Max Snappy decompressed payload   |
| `MaxConnectionsPerIP`          | `int`           | `16`        | Connections per source IP         |
| `MaxIncomingConnections`       | `int`           | `256`       | Total inbound connections         |
| `SendBatchMaxSize`             | `int`           | `64 KB`     | Max send buffer before flush      |
| `Enable0RTT`                   | `bool`          | `false`     | QUIC 0-RTT (replayable, caution)  |
| `DisableCompression`           | `bool`          | `false`     | Disable Snappy compression        |

## State Machine Registration

Source: `pkg/sm/`

### CreateFunc

```go
type CreateFunc func(shardID, replicaID uint64) interface{}
```

The factory function type passed to `Host.StartShard`. Must return one of `StateMachine`, `ConcurrentStateMachine`, or `DiskStateMachine`. The Host detects the SM type via type assertion at startup.

### Type-Safe Wrappers

```go
func NewCreateFunc(fn CreateStateMachineFunc) CreateFunc
func NewConcurrentCreateFunc(fn CreateConcurrentStateMachineFunc) CreateFunc
func NewDiskCreateFunc(fn CreateDiskStateMachineFunc) CreateFunc
```

Wrap typed factory functions into `CreateFunc`, catching type errors at compile time:

```go
type CreateStateMachineFunc func(shardID, replicaID uint64) StateMachine
type CreateConcurrentStateMachineFunc func(shardID, replicaID uint64) ConcurrentStateMachine
type CreateDiskStateMachineFunc func(shardID, replicaID uint64) DiskStateMachine
```

### State Machine Interfaces

Three tiers, detected by `detectSMType()` at `StartShard` time:

**StateMachine** (type code 0) -- All state in memory. Serialized access.

```go
type StateMachine interface {
    Update(ctx context.Context, entries []Entry, results []Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
}
```

**ConcurrentStateMachine** (type code 1) -- Concurrent Lookup during Update. Two-phase snapshotting.

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

**DiskStateMachine** (type code 2) -- Backed by persistent storage. Must handle idempotent replay after crash.

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

### Supporting Types

```go
type Entry struct {
    Index uint64
    Cmd   []byte
}

type Result struct {
    Value uint64
    Data  []byte
}
```

`Entry.CopyCmd()` returns an owned copy when `ZeroCopyEntryCmd` is enabled.

### Optional Interfaces

Detected via type assertion on the SM instance:

- `Hasher` -- `GetHash() (uint64, error)` for cross-replica consistency checks
- `NALookup` -- `BytesLookup(key []byte) ([]byte, error)` for zero-alloc reads

## Event Listener

Source: `pkg/config/hostconfig.go`

```go
type EventListener struct { /* function fields */ }
```

Holds per-type optional callbacks. Only non-nil callbacks are invoked. Callbacks are called **synchronously** on engine worker goroutines and **must not block**.

### Raft Events

| Callback               | Info Type         | Fired When                                   |
|------------------------|-------------------|----------------------------------------------|
| `OnLeaderUpdated`      | `LeaderInfo`      | Leader change detected for a shard           |
| `OnLeaderLost`         | `EventShardInfo`  | Local node lost leadership                   |
| `OnCampaignLaunched`   | `EventShardInfo`  | Election campaign started                    |
| `OnSnapshotRejected`   | `SnapshotInfo`    | Snapshot rejected by follower                |
| `OnProposalDropped`    | `EventShardInfo`  | Proposal dropped (queue full, etc.)          |
| `OnReadIndexDropped`   | `EventShardInfo`  | ReadIndex request dropped                    |

### System Events

| Callback                  | Info Type         | Fired When                                |
|---------------------------|-------------------|-------------------------------------------|
| `OnNodeReady`             | `EventShardInfo`  | Shard node ready for traffic              |
| `OnNodeUnloaded`          | `EventShardInfo`  | Shard node unloaded                       |
| `OnMembershipChanged`     | `EventShardInfo`  | Membership config change applied          |
| `OnNodeDeleted`           | `EventShardInfo`  | Node data removed from disk               |
| `OnConnectionEstablished` | `ConnectionInfo`  | QUIC connection established to peer       |
| `OnConnectionFailed`      | `ConnectionInfo`  | QUIC connection attempt failed            |
| `OnSnapshotSent`          | `SnapshotInfo`    | Snapshot transfer sent                    |
| `OnSnapshotReceived`      | `SnapshotInfo`    | Snapshot transfer received                |
| `OnSnapshotCreated`       | `SnapshotInfo`    | On-demand or auto snapshot completed      |
| `OnLogCompacted`          | `EventShardInfo`  | Log compaction completed                  |

### Failure Events

| Callback             | Info Type          | Fired When                                  |
|----------------------|--------------------|---------------------------------------------|
| `OnShardFailed`      | `ShardFailedInfo`  | Shard encountered fatal error               |
| `OnLogDBDiskWarning` | `DiskInfo`         | WAL disk usage crossed warning threshold    |
| `OnLogDBDiskFull`    | `DiskInfo`         | WAL disk usage crossed full threshold       |

### Event Info Types

```go
type LeaderInfo struct {
    ShardID, ReplicaID, Term, LeaderID uint64
}

type EventShardInfo struct {
    ShardID, ReplicaID, LeaderID, Term uint64
}

type SnapshotInfo struct {
    ShardID, ReplicaID, Index, From uint64
}

type ShardFailedInfo struct {
    ShardID, ReplicaID uint64
    Err                error
}

type DiskInfo struct {
    WALDir    string
    UsedBytes uint64
    MaxBytes  uint64
    Shards    []uint64
}

type ConnectionInfo struct {
    Address string
    Err     error
}
```

## Information Types

### ShardHealth

```go
type ShardHealth uint8
```

Indicates shard health status:

- `ShardHealthUnknown` -- Initial state
- `ShardHealthNormal` -- Healthy
- `ShardHealthError` -- SM failure or too many apply retries

### ShardInfo

```go
type ShardInfo struct {
    ShardID              uint64
    ReplicaID            uint64
    LeaderID             uint64
    Term                 uint64
    IsLeader             bool
    IsObserver           bool
    IsWitness            bool
    StateMachineType     uint64
    StateMachineTypeName string
    Pending              uint64
    Nodes                map[uint64]string
    Observers            map[uint64]string
    Witnesses            map[uint64]string
    Health               ShardHealth
    LastApplied          uint64
    ConfigChangeIndex    uint64
}
```

Point-in-time snapshot of a shard's state. Returned by `GetShardInfo`.

### NodeHostInfoOption

```go
type NodeHostInfoOption struct {
    SkipLogInfo bool
}
```

Options for `GetNodeHostInfo`. `SkipLogInfo=true` skips log information for faster responses.

### NodeHostInfo

```go
type NodeHostInfo struct {
    NodeID      string
    RaftAddress string
    Shards      []ShardInfo
}
```

Snapshot of all shards on the host.

### LogRange

```go
type LogRange struct {
    FirstIndex uint64
    LastIndex  uint64
}
```

First and last indices in the Raft log.

### ReadonlyLogReader

Provides read-only access to committed Raft log entries.

## Usage Patterns

### Synchronous Propose

```go
host, _ := quicraft.NewHost(hostCfg, quicraft.WithBarrier(barrier))
defer host.Close()

create := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
    return newKVStore()
})

host.StartShard(members, false, create, shardCfg)

result, err := host.SyncPropose(ctx, shardID, []byte("key=value"))
if err != nil {
    var nle *quicraft.NotLeaderError
    if errors.As(err, &nle) {
        // Redirect to nle.LeaderAddress
    }
    // Handle error
}
// result.Value, result.Data available
```

### Asynchronous Propose

```go
rs, err := host.Propose(ctx, shardID, cmd)
if err != nil {
    return err
}
defer rs.Release()

select {
case result := <-rs.ResultC():
    if result.Err != nil {
        return result.Err
    }
    // Use result.Value, result.Data
case <-ctx.Done():
    return ctx.Err()
}
```

### Linearizable Read

```go
value, err := host.SyncRead(ctx, shardID, "my-key")
if err != nil {
    return err
}
```

### Session-Based At-Most-Once Propose

```go
session, err := host.GetNewSession(ctx, shardID)
if err != nil {
    return err
}
defer host.CloseSession(ctx, shardID, session)

// Each proposal uses the session for deduplication
rs, err := host.ProposeWithSession(ctx, session, cmd)
if err != nil {
    return err
}
defer rs.Release()

result, err := rs.Result()
if err != nil {
    return err
}
session.ProposalCompleted()
```

### Membership Change

```go
rs, err := host.RequestAddNode(ctx, shardID, newReplicaID, "10.0.0.4:4001", 0)
if err != nil {
    return err
}
defer rs.Release()

result, err := rs.Result()
if err != nil {
    return err
}
```

### Graceful Shutdown

```go
// Drain first (optional, Close calls Drain internally)
if err := host.Drain(); err != nil {
    log.Warn("drain incomplete", "error", err)
}

// Close shuts down everything
if err := host.Close(); err != nil {
    log.Error("close failed", "error", err)
}
```

### Health Check

```go
status := host.HealthCheck()
if status.Status != "ok" {
    // Return HTTP 503
}
```

### Prometheus Metrics

```go
metrics := quicraft.NewPrometheusMetricsCollector()
host, _ := quicraft.NewHost(hostCfg, quicraft.WithMetrics(metrics))

// Expose metrics endpoint
http.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
```

## Cross-References

- [Architecture Overview](../architecture/overview.md) -- System design, data flow, package structure
- [Transport](../transport/quic.md) -- QUIC transport, connection management, frame format
