# RSM Adapter Layer

## Purpose

The RSM adapter (`pkg/internal/rsm/`) bridges the engine's apply pipeline to user-implemented state machines. Users implement one of three public interfaces in `pkg/sm/` -- `StateMachine`, `ConcurrentStateMachine`, or `DiskStateMachine` -- and the adapter wraps whichever variant behind a unified internal interface (`smWrapper`). This lets the engine pipeline call a single `Apply()` method without caring which state machine tier is underneath.

The adapter owns three responsibilities beyond delegation:

1. **Session lifecycle** -- routes register/unregister entries to the session manager instead of the user SM
2. **Duplicate detection** -- checks the session cache before forwarding proposals to the user SM
3. **Snapshot coordination** -- serializes session state alongside SM state in a single snapshot stream

Source files:

| File | Role |
|------|------|
| `pkg/internal/rsm/statemachine.go` | `StateMachine` struct, `Apply`, `Lookup`, snapshot, `wrapStateMachine` |
| `pkg/internal/rsm/adapter.go` | `smWrapper` interface, three wrapper types, DRY helpers |
| `pkg/internal/rsm/errors.go` | Typed sentinel errors |
| `pkg/sm/interfaces.go` | Public SM interfaces |
| `pkg/sm/createfunc.go` | `CreateFunc` factory types |
| `pkg/internal/session/manager.go` | Session manager |
| `pkg/internal/session/session.go` | Per-client session state |

## Type Detection

### The Interface Hierarchy Problem

Go's `DiskStateMachine` is a strict superset of `ConcurrentStateMachine` -- it has all the same methods (`Update`, `Lookup`, `PrepareSnapshot`, `SaveSnapshot`, `RecoverFromSnapshot`, `Close`) plus `Open` and `Sync`. A naive type switch checking `ConcurrentStateMachine` first would match a `DiskStateMachine` and wrap it with the wrong adapter, losing the `Open` and `Sync` calls.

### wrapStateMachine()

The one-time type switch in `wrapStateMachine()` checks **most-specific first**:

```go
func wrapStateMachine(raw interface{}) (smWrapper, SMType, error) {
    switch typed := raw.(type) {
    case sm.DiskStateMachine:         // most specific -- has Open + Sync
        return &onDiskSMWrapper{sm: typed}, SMTypeOnDisk, nil
    case sm.ConcurrentStateMachine:   // middle -- has PrepareSnapshot
        return &concurrentSMWrapper{sm: typed}, SMTypeConcurrent, nil
    case sm.StateMachine:             // least specific -- basic interface
        return &regularSMWrapper{sm: typed}, SMTypeRegular, nil
    default:
        return nil, 0, ErrInvalidSMType
    }
}
```

The type code enum:

| Code | Const | String | SM Type |
|------|-------|--------|---------|
| 0 | `SMTypeRegular` | `"regular"` | `StateMachine` (in-memory, serialized access) |
| 1 | `SMTypeConcurrent` | `"concurrent"` | `ConcurrentStateMachine` (concurrent Lookup) |
| 2 | `SMTypeOnDisk` | `"on-disk"` | `DiskStateMachine` (own persistent storage) |

`SMType` implements `String()` for log output and `TypeCode()` for serialization into `logdb.Bootstrap.Type` and `ShardInfo.StateMachineType`. The proto package provides `StateMachineTypeName(uint64)` for converting wire-format type codes back to human-readable names.

### Factory Flow

`NewStateMachine()` calls the user's `CreateFunc`, then `wrapStateMachine()` on the returned value:

```
sm.CreateFunc(shardID, replicaID)
         |
         v
   raw interface{}
         |
    wrapStateMachine()
         |
         v
   smWrapper + smType
         |
         v
   rsm.StateMachine{sm: wrapper, sessions: NewManager(...)}
```

The `CreateFunc` type (`func(shardID, replicaID uint64) interface{}`) returns an untyped value. Type-safe wrappers (`sm.NewCreateFunc`, `sm.NewConcurrentCreateFunc`, `sm.NewDiskCreateFunc`) catch type errors at compile time by accepting typed factory functions and boxing the return value.

## smWrapper Internal Interface

All three wrapper types implement `smWrapper`, which is the engine-facing contract:

```go
type smWrapper interface {
    Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error)
    Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    NALookup(key []byte) ([]byte, bool, error)
    GetHash() (uint64, bool, error)
    SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
    Type() smType
}
```

The type switch happens once at construction. All subsequent calls go through direct method calls on the concrete wrapper, eliminating interface dispatch overhead on the hot path.

## Apply Pipeline

### Entry Flow Diagram

```
Engine Apply Worker
       |
       |  rsm.StateMachine.Apply([]proto.Entry, []sm.Result)
       v
  +--------------------------------------------+
  |  For each proto.Entry:                     |
  |                                            |
  |  1. IsNewSessionRequest()?                 |
  |     YES -> sessions.Register(clientID)     |
  |            result = {Value: clientID}      |
  |            continue (skip user SM)         |
  |                                            |
  |  2. IsEndOfSessionRequest()?               |
  |     YES -> sessions.Unregister(clientID)   |
  |            result = {Value: clientID}      |
  |            continue (skip user SM)         |
  |                                            |
  |  3. IsSessionManaged()?                    |
  |     YES -> sessions.CheckDuplicate()       |
  |            duplicate? -> return cached     |
  |            continue                        |
  |                                            |
  |  4. Forward to user SM                     |
  |     append to smEntries/smResults/indexMap |
  +--------------------------------------------+
       |
       |  smWrapper.Update(ctx, smEntries, smResults)
       v
  +--------------------------------------------+
  |  Copy results back to original positions   |
  |  Record results in session cache           |
  |  Update lastApplied (atomic)               |
  +--------------------------------------------+
```

### Session Entry Detection

Session management entries are identified by sentinel `SeriesID` values on `proto.Entry`:

| Constant | Value | Meaning |
|----------|-------|---------|
| `NotSessionManagedClientID` | `0` | Entry bypasses session deduplication |
| `SessionSeriesIDForRegister` | `math.MaxUint64 - 1` | Register new client session |
| `SessionSeriesIDForUnregister` | `math.MaxUint64` | Unregister client session |

Entry helper methods on `proto.Entry`:

- `IsNewSessionRequest()` -- `!IsConfigChange() && len(Cmd) == 0 && ClientID != 0 && SeriesID == SessionSeriesIDForRegister`
- `IsEndOfSessionRequest()` -- `!IsConfigChange() && len(Cmd) == 0 && ClientID != 0 && SeriesID == SessionSeriesIDForUnregister`
- `IsSessionManaged()` -- `ClientID != 0`

### Type Conversion

The adapter converts `proto.Entry` (internal, Raft-aware) to `sm.Entry` (public, user-facing). The user SM never sees Raft-internal fields:

| proto.Entry field | sm.Entry field | Notes |
|-------------------|----------------|-------|
| `Index` | `Index` | Preserved |
| `Cmd` | `Cmd` | Preserved (aliased, not copied) |
| `Term` | -- | Excluded (SMs are term-agnostic) |
| `Key` | -- | Internal dedup key |
| `ClientID` | -- | Session routing |
| `SeriesID` | -- | Session sequencing |
| `RespondedTo` | -- | Session eviction marker |
| `Type` | -- | Entry type flags |

### Buffer Reuse

`Apply()` reuses three pre-allocated buffers across calls via `[:0]` reset:

```go
smEntries := s.smEntryBuf[:0]
smResults := s.smResultBuf[:0]
indexMap  := s.indexMapBuf[:0]
```

- `smEntries` -- `[]sm.Entry` passed to the user SM
- `smResults` -- `[]sm.Result` filled by the user SM
- `indexMap` -- `[]int` maps smResults indices back to the original `results` positions

This is safe because `Apply()` is called from a single apply goroutine per shard and the buffers are fully consumed before `Apply()` returns. Buffers grow on demand and are retained at their grown capacity for subsequent calls.

### Duplicate Detection Flow

For session-managed entries (non-register, non-unregister):

```
sessions.CheckDuplicate(clientID, seriesID)
       |
       |-- session not found? -> treat as new (no dedup)
       |-- seriesID <= respondedTo? -> client already acked, not a dup
       |-- found in response cache? -> return cached result (duplicate)
       |-- not found? -> new proposal, forward to user SM
```

After the user SM processes the entry, the result is recorded:

```
sessions.RecordResult(clientID, seriesID, respondedTo, result)
       |
       |-- advance respondedTo watermark
       |-- evict cached responses with seriesID <= respondedTo
       |-- check response cache limit (returns ErrResponseLimitExceeded if full)
       |-- store result in session cache
```

### lastApplied Tracking

After processing the batch, `Apply()` atomically stores the highest entry index as `lastApplied`:

```go
if len(entries) > 0 {
    lastIndex := entries[len(entries)-1].Index
    if lastIndex > s.lastApplied.Load() {
        s.lastApplied.Store(lastIndex)
    }
}
```

Uses `atomic.Uint64` because `LastApplied()` may be read concurrently from the snapshot pool worker goroutine.

## Session Integration

The adapter delegates all session lifecycle operations to `session.Manager` rather than the user SM:

### Registration

When `entry.IsNewSessionRequest()` is true:

1. Call `sessions.Register(entry.ClientID)`
2. On success: `results[i] = sm.Result{Value: entry.ClientID}`
3. On `ErrSessionAlreadyExists`: `results[i] = sm.Result{}` (not fatal)
4. Skip user SM entirely

### Unregistration

When `entry.IsEndOfSessionRequest()` is true:

1. Call `sessions.Unregister(entry.ClientID)`
2. On success: `results[i] = sm.Result{Value: entry.ClientID}`
3. On `ErrSessionNotFound`: `results[i] = sm.Result{}` (not fatal)
4. Skip user SM entirely

### Why Not Fatal

Register/unregister errors (already exists, not found) produce empty results rather than propagating errors. These are idempotent operations -- a duplicate register after snapshot recovery or a double-unregister from a retry should not crash the node. The result simply reflects that the operation had no effect.

### Session Manager Limits

| Limit | Default | Error |
|-------|---------|-------|
| Max sessions per shard | 16,384 | `ErrSessionLimitExceeded` |
| Max cached responses per session | 128 | `ErrResponseLimitExceeded` |

`ErrResponseLimitExceeded` is propagated from `Apply()` as a fatal error, indicating a misbehaving client that is not advancing its `respondedTo` watermark.

## Snapshot Operations

### Snapshot Format

```
+-----------------------------------+
| Session data (Manager.MarshalTo)  |
|   [numSessions:4]                 |
|   [session1][session2]...         |
|   Each session:                   |
|     [clientID:8]                  |
|     [respondedTo:8]               |
|     [numResponses:4]              |
|     [{seriesID:8, value:8,        |
|       dataLen:4, data:N}...]      |
|     [lastActiveIndex:8]           |
+-----------------------------------+
| User SM data (smWrapper snapshot) |
+-----------------------------------+
```

Session data is written first, user SM data second. The format uses little-endian encoding with deterministic ordering (client IDs and series IDs sorted before serialization).

### SaveSnapshot

```go
func (s *StateMachine) SaveSnapshot(ctx, w, stopper) error {
    // 1. Check for cancellation
    // 2. Serialize session state -> w
    // 3. Delegate to smWrapper.SaveSnapshot -> w
}
```

The three wrapper types handle `SaveSnapshot` differently:

| Wrapper | SaveSnapshot Behavior |
|---------|----------------------|
| `regularSMWrapper` | Direct delegation to `sm.SaveSnapshot(ctx, w, stopper)` |
| `concurrentSMWrapper` | Two-phase: `PrepareSnapshot()` then `SaveSnapshot(ctx, snapCtx, w, stopper)` |
| `onDiskSMWrapper` | Three-phase: `Sync()` then `PrepareSnapshot()` then `SaveSnapshot(ctx, snapCtx, w, stopper)` |

The two-phase approach for `ConcurrentStateMachine` allows capturing a point-in-time snapshot context on the apply worker goroutine (while `Update` is paused), then writing the snapshot data asynchronously without blocking further `Update` calls.

The three-phase approach for `DiskStateMachine` adds `Sync()` before `PrepareSnapshot()` to flush pending writes to durable storage, ensuring the snapshot reflects a consistent state.

### RecoverFromSnapshot

```go
func (s *StateMachine) RecoverFromSnapshot(ctx, r, stopper) error {
    // 1. Check for cancellation
    // 2. Deserialize session state <- r  (replaces all sessions)
    // 3. Delegate to smWrapper.RecoverFromSnapshot <- r
}
```

Session state is read first, then user SM state. The session manager's `UnmarshalFrom` clears all existing sessions and replaces them with the deserialized state.

### Cancellation

Both `SaveSnapshot` and `RecoverFromSnapshot` check the stopper channel before starting. If already closed, they return `ErrStopped` immediately. The stopper channel is also passed through to the user SM for periodic cancellation checks during long-running snapshot operations.

## Optional Interfaces

### NALookup

The `sm.NALookup` interface provides zero-allocation byte-slice lookups for hot paths, avoiding the heap allocation from `interface{}` boxing in the standard `Lookup` method:

```go
type NALookup interface {
    BytesLookup(key []byte) ([]byte, error)
}
```

The adapter wraps this with a `(value, supported, error)` return pattern:

```go
func naLookup(underlying interface{}, key []byte) ([]byte, bool, error) {
    na, ok := underlying.(sm.NALookup)
    if !ok {
        return nil, false, nil     // not supported, no error
    }
    result, err := na.BytesLookup(key)
    if err != nil {
        return nil, true, err      // supported, but errored
    }
    return result, true, nil       // supported, success
}
```

The `Lookup` method on `rsm.StateMachine` uses NALookup as a fast path when the query is a `[]byte`:

```go
func (s *StateMachine) Lookup(ctx, query) (interface{}, error) {
    if key, ok := query.([]byte); ok {
        result, supported, err := s.sm.NALookup(key)
        if err != nil { return nil, err }
        if supported { return result, nil }
    }
    return s.sm.Lookup(ctx, query)
}
```

### GetHash

The `sm.Hasher` interface computes a deterministic hash of state machine state for cross-replica consistency verification:

```go
type Hasher interface {
    GetHash() (uint64, error)
}
```

Wrapped identically:

```go
func getHash(underlying interface{}) (uint64, bool, error) {
    h, ok := underlying.(sm.Hasher)
    if !ok {
        return 0, false, nil       // not supported
    }
    hash, err := h.GetHash()
    if err != nil {
        return 0, true, err        // supported, but errored
    }
    return hash, true, nil         // supported, success
}
```

### The Three-Value Return Pattern

Both optional interfaces follow the same convention:

| Return | Meaning |
|--------|---------|
| `(zero, false, nil)` | SM does not implement the interface |
| `(value, true, nil)` | SM implements it, call succeeded |
| `(zero, true, err)` | SM implements it, call failed |

This allows callers to distinguish "not supported" from "supported but errored" without introducing a separate capability-check method.

## DRY Helpers

The `naLookup()` and `getHash()` package-level functions in `adapter.go` are shared by all three wrapper types. Without these helpers, each wrapper would need its own copy of the type-assertion-and-dispatch logic:

```go
// Used by regularSMWrapper, concurrentSMWrapper, and onDiskSMWrapper
func (w *regularSMWrapper) NALookup(key []byte) ([]byte, bool, error) {
    return naLookup(w.sm, key)  // shared helper
}
func (w *concurrentSMWrapper) NALookup(key []byte) ([]byte, bool, error) {
    return naLookup(w.sm, key)  // same helper
}
func (w *onDiskSMWrapper) NALookup(key []byte) ([]byte, bool, error) {
    return naLookup(w.sm, key)  // same helper
}
```

This eliminated 6 copy-pasted method bodies (3 wrappers x 2 optional interfaces) in favor of 2 shared functions.

## Error Propagation

### RSM Sentinel Errors

Defined in `pkg/internal/rsm/errors.go`:

| Error | When |
|-------|------|
| `ErrInvalidSMType` | `CreateFunc` returns a type that does not implement any known SM interface |
| `ErrClosed` | Operation attempted on a closed state machine |
| `ErrStopped` | Stopper channel closed during snapshot operation |

### Session Sentinel Errors

Defined in `pkg/internal/session/errors.go`:

| Error | When |
|-------|------|
| `ErrSessionLimitExceeded` | Shard has reached max sessions (default 16,384) |
| `ErrSessionNotFound` | Operation on a non-existent client session |
| `ErrSessionAlreadyExists` | Registering an already-registered client |
| `ErrResponseLimitExceeded` | Session response cache full (client not advancing respondedTo) |

### Propagation Rules

- Errors from `smWrapper.Update()` propagate directly from `Apply()` -- the engine treats these as fatal for the shard.
- `ErrResponseLimitExceeded` from `sessions.RecordResult()` propagates from `Apply()` -- indicates a misbehaving client.
- Session register/unregister errors (`ErrSessionAlreadyExists`, `ErrSessionNotFound`) are handled inline with empty results -- not propagated. These are idempotent and expected during replay.
- Errors from `SaveSnapshot`, `RecoverFromSnapshot`, and `Close` propagate directly to the engine.
- The RSM never swallows errors with `_`. All error returns from user SM methods and session operations are either propagated or explicitly handled.

## Wrapper Behavior Summary

| Method | regularSMWrapper | concurrentSMWrapper | onDiskSMWrapper |
|--------|-----------------|---------------------|-----------------|
| `Open` | no-op `(0, nil)` | no-op `(0, nil)` | delegates to `sm.Open()` |
| `Update` | delegates | delegates | delegates |
| `Lookup` | delegates | delegates | delegates |
| `NALookup` | `naLookup(sm, key)` | `naLookup(sm, key)` | `naLookup(sm, key)` |
| `GetHash` | `getHash(sm)` | `getHash(sm)` | `getHash(sm)` |
| `SaveSnapshot` | direct | `PrepareSnapshot` + `SaveSnapshot(snapCtx)` | `Sync` + `PrepareSnapshot` + `SaveSnapshot(snapCtx)` |
| `RecoverFromSnapshot` | delegates | delegates | delegates |
| `Close` | delegates | delegates | delegates |
| `Type` | `smTypeRegular` | `smTypeConcurrent` | `smTypeOnDisk` |

## Cross-References

- [Architecture Overview](../architecture/overview.md) -- system overview and data flow
- [Engine Pipeline](../engine/pipeline.md) -- apply worker that calls `rsm.StateMachine.Apply()`
- [Session Management](../sessions/deduplication.md) -- session manager internals
