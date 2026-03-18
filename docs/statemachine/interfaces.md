# State Machine Interfaces

## Overview

QuicRaft defines three state machine interfaces in `pkg/sm`. Users implement exactly one interface and provide a factory function to create instances. QuicRaft handles consensus, replication, snapshotting, and recovery. State machines receive only committed entries and never interact with the Raft protocol directly.

| Tier | Interface | Concurrency | Persistence |
|------|-----------|-------------|-------------|
| In-memory | `StateMachine` | Serialized by QuicRaft | None (snapshots serialize full state) |
| Concurrent | `ConcurrentStateMachine` | Concurrent `Lookup` during `Update` | None (two-phase snapshots) |
| On-disk | `DiskStateMachine` | Concurrent `Lookup` during `Update` | Own persistent storage (e.g., PebbleDB) |

## StateMachine

The basic in-memory state machine. All state fits in memory and QuicRaft serializes all method calls: `Update` and `Lookup` never execute concurrently. No internal synchronization is needed.

```go
type StateMachine interface {
	Update(ctx context.Context, entries []Entry, results []Result) error
	Lookup(ctx context.Context, query interface{}) (interface{}, error)
	SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
	Close(ctx context.Context) error
}
```

**Update** applies committed entries to state. The `results` slice is pre-allocated with `len(results) == len(entries)`. The implementation must write `results[i]` for each `entries[i]`. Returns error only for fatal, non-recoverable failures.

**Lookup** reads state without Raft. For `StateMachine`, QuicRaft serializes Lookup with Update on the apply worker. Avoid `interface{}` boxing on hot paths by using pointer types as queries.

**SaveSnapshot** writes the entire state to the writer. The full state must be serialized since in-memory state machines have no persistent backing store.

**RecoverFromSnapshot** restores state from the reader, replacing all existing state.

**Close** releases all resources.

## ConcurrentStateMachine

For state machines that support concurrent read access during writes. `Lookup` may be called from any goroutine while `Update` runs on the apply worker. The state machine manages its own synchronization (e.g., `sync.RWMutex`, COW snapshots, sharded locks).

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

### Two-Phase Snapshot

`PrepareSnapshot` captures a point-in-time snapshot context passed to `SaveSnapshot`. This two-phase design allows the snapshot to proceed without blocking `Update`. `PrepareSnapshot` is called on the apply worker goroutine while `Update` is paused. The returned opaque value is passed unchanged to `SaveSnapshot`, which may run on a separate goroutine.

Common patterns:

- **Deep copy**: clone all data during `PrepareSnapshot`, write the clone in `SaveSnapshot`. O(n) memory but simple.
- **COW snapshot**: create a copy-on-write view during `PrepareSnapshot`. Writes after `PrepareSnapshot` allocate new backing storage.
- **Database snapshot**: if backed by a database with snapshot isolation, return the DB snapshot handle.

## DiskStateMachine

For state machines backed by persistent storage (e.g., PebbleDB, SQLite). The state machine manages persistence; QuicRaft manages consensus. Snapshots transfer database files rather than in-memory state.

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

**Open** initializes the state machine and returns the last applied log index. Returns 0 on fresh start. The `initialDir` is the working directory assigned by QuicRaft for persistent storage.

On-disk state machines may receive entries that were already applied before a crash. This happens when the SM applied entries but the node crashed before QuicRaft persisted the updated `lastApplied` index. The SM must handle replay idempotently.

Recommended pattern: persist `lastApplied` atomically with data writes inside the SM's own database, then return this value from `Open()`. The SM's own crash recovery (WAL/journal) ensures consistency.

**Sync** flushes pending writes to durable storage. Called before snapshot operations to ensure consistency.

**Three-Phase Snapshot**: `Sync()` → `PrepareSnapshot()` → `SaveSnapshot()`. This differs from `ConcurrentStateMachine` which skips the `Sync` step.

## Supporting Types

### Entry

```go
type Entry struct {
	Index uint64
	Cmd   []byte
}
```

Represents a committed log entry. This is a simplified view of the internal `proto.Entry` -- users don't see Raft-internal fields (`Term`, `Key`, `ClientID`, etc.). `Term` is excluded because state machines should be term-agnostic: the same entry applied at the same index must produce the same result regardless of which term proposed it.

**Buffer Ownership**

By default (`Config.ZeroCopyEntryCmd = false`), `Entry.Cmd` is an owned copy. The RSM adapter copies `Cmd` into a per-entry buffer before calling `Update()`. Safe to retain after the call returns.

When `Config.ZeroCopyEntryCmd = true`, `Entry.Cmd` aliases an internal buffer reused after `Update()` returns. The SM must copy `Cmd` if it needs the data to outlive the call.

```go
func (e *Entry) CopyCmd() []byte
```

Returns an owned copy of `Cmd`. Use this when `ZeroCopyEntryCmd = true` and you need to retain the data.

Safe usage in zero-copy mode:

```go
func (sm *MySM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		key := string(e.Cmd[:4])           // safe: converted to owned string
		val := make([]byte, len(e.Cmd[4:]))
		copy(val, e.Cmd[4:])              // safe: copied to owned buffer
		sm.store[key] = val
		results[i] = sm.Result{Value: 1}
	}
	return nil
}
```

Unsafe (retaining alias after Update returns):

```go
sm.pending = append(sm.pending, e.Cmd)  // UNSAFE: Cmd may be overwritten
```

### Result

```go
type Result struct {
	Value uint64
	Data  []byte
}
```

`Value` is an application-defined result code. `Data` carries optional result data returned to the caller through `RequestState.Result()`.

## Update All-or-Nothing Semantics

`Update` must be all-or-nothing: either all entries are applied and all results written, or the state machine reverts to its pre-call state and returns an error. Partial application is not supported.

Rollback patterns for database-backed state machines:

1. **Transaction wrap**: wrap the batch in a DB transaction. On failure, rollback.
2. **Staging table**: write to a staging table, atomically promote on success.
3. **Append-only truncation**: for append-only stores, record the pre-call position, truncate on failure.
4. **Snapshot-before-apply**: for in-memory machines, snapshot state before applying, restore on failure.

The apply worker pre-allocates `[]Result` per batch and passes it to `Update()`. This eliminates a heap allocation on every apply call. The `results` slice is reused across batches (reset, not reallocated).

## Lookup Concurrency Guarantees

`Lookup` is called by `Host.QueryLocalNode()` and `Host.StaleRead()`. Concurrency guarantees differ by SM type:

| SM Type | Update vs Lookup | Caller's Responsibility |
|---------|------------------|-------------------------|
| `StateMachine` | Serialized by QuicRaft on apply worker. Never concurrent. | None. |
| `ConcurrentStateMachine` | Concurrent. Lookup may be called from any goroutine while Update runs. | SM must use internal synchronization. |
| `DiskStateMachine` | Concurrent. Same as ConcurrentStateMachine. | SM must ensure backing database handles concurrent reads during writes. |

For `StateMachine`, the RSM adapter serializes Lookup with Update on the apply worker goroutine. For `ConcurrentStateMachine` and `DiskStateMachine`, no QuicRaft-level locking is applied.

The `StaleRead` path calls `Lookup` directly without going through the Raft ReadIndex protocol. The result may be stale (whatever state the local SM has applied so far).

## Optional Interfaces

Detected via type assertion at runtime. Not required, but enable optimizations.

### Hasher

```go
type Hasher interface {
	GetHash() (uint64, error)
}
```

Computes a deterministic hash of state machine state for cross-replica consistency verification.

### NALookup

```go
type NALookup interface {
	BytesLookup(key []byte) ([]byte, error)
}
```

Provides zero-allocation byte-slice lookup for hot paths. When implemented, the RSM adapter uses this instead of `Lookup` for `[]byte` queries to avoid heap allocation from `interface{}` boxing.

### NALookupInto

```go
type NALookupInto interface {
	NALookup
	BytesLookupInto(key, dst []byte) ([]byte, error)
}
```

An optional extension to `NALookup` that allows the caller to provide a destination buffer. The implementation must copy the value into `dst` (growing it via append if necessary) and return the populated sub-slice. Ownership of the returned slice belongs to the caller; the implementation must not retain a reference after returning.

When implemented alongside `NALookup`, the RSM adapter prefers this method on the pooled read path to eliminate per-call heap allocation.

## Factory Functions

### CreateFunc

```go
type CreateFunc func(shardID, replicaID uint64) interface{}
```

The single entry point for all three state machine types. The engine inspects the concrete type at startup via type assertion. The returned `interface{}` must implement exactly one of the three state machine interfaces. `StartShard` returns `ErrInvalidSMType` if none match.

### Type-Safe Wrappers

```go
type CreateStateMachineFunc           func(shardID, replicaID uint64) StateMachine
type CreateConcurrentStateMachineFunc func(shardID, replicaID uint64) ConcurrentStateMachine
type CreateDiskStateMachineFunc       func(shardID, replicaID uint64) DiskStateMachine

func NewCreateFunc(fn CreateStateMachineFunc) CreateFunc
func NewConcurrentCreateFunc(fn CreateConcurrentStateMachineFunc) CreateFunc
func NewDiskCreateFunc(fn CreateDiskStateMachineFunc) CreateFunc
```

Type-safe wrappers catch compile-time errors instead of runtime. The raw `CreateFunc` form still works for advanced use cases.

### Usage

```go
// In-memory state machine
host.StartShard(members, false, sm.NewCreateFunc(func(s, r uint64) sm.StateMachine {
	return NewMyKVStore()
}), cfg)

// On-disk state machine
host.StartShard(members, false, sm.NewDiskCreateFunc(func(s, r uint64) sm.DiskStateMachine {
	return NewMyPebbleStore(opts)
}), cfg)
```

## KV Backend Implementations

QuicRaft provides three reference KV state machine implementations.

### MemoryStore (StateMachine)

Plain `map[string][]byte` in-memory store. Uses `sync.RWMutex` to protect the data map because `SaveSnapshot` may be called concurrently with `Update`.

Implements: `sm.StateMachine`, `sm.Hasher`, `sm.NALookup`.

Use case: testing, small datasets, simplest implementation path.

### ConcurrentStore (ConcurrentStateMachine)

Sharded locking with 16 buckets, each containing `sync.RWMutex` and `map[string][]byte`. Bucket selection via `xxhash64(key) % 16`.

`Update` acquires per-entry write lock. `Lookup` acquires read lock on target bucket (single key) or all 16 (prefix scan). `PrepareSnapshot` deep-clones all buckets under read locks.

Implements: `sm.ConcurrentStateMachine`, `sm.Hasher`, `sm.NALookup`.

Use case: read-heavy workloads where `Lookup` must not block on `Update`.

### PebbleStore (DiskStateMachine)

PebbleDB-backed on-disk state machine. Persists `lastApplied` atomically with data writes for crash recovery. Uses native PebbleDB iterators for prefix scans. Point-in-time snapshots via `pebble.Snapshot`.

Implements: `sm.DiskStateMachine`, `sm.Hasher`, `sm.NALookup`.

Factory with functional options:

```go
func NewPebbleCreateFunc(opts ...Option) sm.CreateFunc {
	return sm.NewDiskCreateFunc(func(_, _ uint64) sm.DiskStateMachine {
		pOpts := &pebble.Options{}
		for _, opt := range opts {
			opt(pOpts)
		}
		return NewPebbleStore(pOpts)
	})
}
```

Source: [go-qrdb](https://github.com/jeremyhahn/go-qrdb)

### KV Factory Functions

```go
func NewMemoryCreateFunc() sm.CreateFunc
func NewConcurrentCreateFunc() sm.CreateFunc
```

Wrap constructors into `sm.CreateFunc` values suitable for `Host.StartShard()`.

**KV Command Protocol**

- `EncodePut(key, value)` -- sets key to value
- `EncodeGet(key)` -- returns value or `KeyNotFoundError`
- `EncodeDelete(key)` -- deletes key
- `EncodeList(prefix)` -- returns sorted `[]string` of keys matching prefix

## Snapshot Format

The RSM adapter writes a combined snapshot containing both session state and user SM state:

```
+-----------------------------------+
| Session data (Manager.MarshalTo)  |
|   [numSessions:4 LE]              |
|   [session1][session2]...         |
+-----------------------------------+
| User SM data (SM snapshot)        |
+-----------------------------------+
```

Session data is written first, user SM data second. Both `SaveSnapshot` and `RecoverFromSnapshot` check the stopper channel before starting and return `ErrStopped` if already closed.

On recovery, the session manager's `UnmarshalFrom` clears all existing sessions and replaces them with deserialized state before the user SM state is restored.

## Session-Managed Entry Routing

The RSM adapter's `Apply` method routes entries through the session lifecycle before they reach the user state machine. Session management entries are identified by sentinel `SeriesID` values:

| Constant | Value | Meaning |
|----------|-------|---------|
| `SessionSeriesIDForRegister` | `math.MaxUint64 - 1` | Register new client session |
| `SessionSeriesIDForUnregister` | `math.MaxUint64` | Unregister client session |

### Apply Flow

For each entry in a batch:

1. **Session register**: call `sessions.Register(clientID, index)`. Set result to `{Value: clientID}`. Skip user SM entirely.
2. **Session unregister**: call `sessions.Unregister(clientID)`. Set result to `{Value: clientID}`. Skip user SM entirely.
3. **Session-managed entry**: check `sessions.CheckDuplicate(clientID, seriesID, index)`. If duplicate, return cached result. If session not found (e.g., after snapshot restore), log a warning and return empty result. Otherwise, forward to user SM.
4. **Non-session entry** (`clientID == 0`): forward directly to user SM.

After the user SM processes entries, results are recorded in the session cache via `sessions.RecordResult()`. Register/unregister errors (`ErrSessionAlreadyExists`, `ErrSessionNotFound`) are handled inline with empty results -- not propagated as fatal errors because these are idempotent operations expected during replay.

`Apply()` reuses three pre-allocated buffers across calls via `[:0]` reset:

- `smEntryBuf` -- `[]sm.Entry` passed to user SM
- `smResultBuf` -- `[]sm.Result` filled by user SM
- `indexMapBuf` -- `[]int` maps SM result indices back to original positions

Safe because `Apply()` runs on a single apply goroutine per shard.

## Session Expiry Integration

`Config.SessionExpiryEntries` controls automatic cleanup of abandoned client sessions. Sessions inactive for this many applied log entries are expired during the apply path. Default 0 (disabled).

This is deterministic across all replicas because it's driven by log index, not wall-clock time. All replicas process the same entries, so they expire the same sessions at the same log point.

Each session tracks a `lastActiveIndex` field (log index, not wall-clock time). Updated whenever:

- The session is registered (initial index)
- A duplicate is detected (`CheckDuplicate` touches the session)
- A result is recorded (`RecordResult` touches the session)

A session is expired when `currentIndex - lastActiveIndex > inactivityThreshold`.

### Session Limits

| Limit | Config Field | Default | Error |
|-------|-------------|---------|-------|
| Max sessions per shard | `MaxTotalSessions` | 16,384 | `ErrSessionLimitExceeded` |
| Max cached responses per session | `MaxSessionsPerClient` | 128 | `ErrResponseLimitExceeded` |

`ErrResponseLimitExceeded` propagates from `Apply()` as a fatal error, indicating a misbehaving client not advancing its `respondedTo` watermark.

## Configuration Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ZeroCopyEntryCmd` | `bool` | `false` | When true, `Entry.Cmd` aliases internal buffer. SM must copy if retaining. |
| `SessionExpiryEntries` | `uint64` | `0` (disabled) | Sessions inactive for this many entries are expired. Deterministic across replicas. |
| `MaxSessionsPerClient` | `uint64` | `128` | Maximum cached responses per session. |
| `MaxTotalSessions` | `uint64` | `16,384` | Maximum concurrent sessions per shard. |
| `MaxApplyEntrySize` | `uint64` | `64 MB` | Maximum total bytes of entries in a single `Update()` call. |
| `MaxEntrySize` | `uint64` | `8 MB` | Maximum proposal payload size. Proposals exceeding this are rejected. |

## Error Types

### RSM Errors

| Error | Condition |
|-------|-----------|
| `ErrInvalidSMType` | `CreateFunc` returns a type that does not implement any known SM interface |
| `ErrStopped` | Stopper channel closed during snapshot operation |

### Session Errors

| Error | Condition |
|-------|-----------|
| `ErrSessionLimitExceeded` | Shard has reached max sessions |
| `ErrSessionNotFound` | Operation on a non-existent client session |
| `ErrSessionAlreadyExists` | Registering an already-registered client |
| `ErrResponseLimitExceeded` | Session response cache full (client not advancing `respondedTo`) |

### KV Errors

| Error | Condition |
|-------|-----------|
| `*KeyNotFoundError` | Lookup key does not exist |
| `*InvalidCommandError` | Malformed command encoding or unsupported query type |

## Cross-References

- [Architecture Overview](../architecture/overview.md) -- system overview and package structure
- [Engine Pipeline](../engine/pipeline.md) -- apply worker that calls `rsm.StateMachine.Apply()`
- [Session Management](../sessions/deduplication.md) -- session manager internals, deduplication protocol
- [Public API](../api/reference.md) -- `Host.StartShard`, `Host.QueryLocalNode`, `Host.StaleRead`
