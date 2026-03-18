# Session-Based At-Most-Once Delivery

Client sessions in QuicRaft provide **at-most-once proposal delivery** by tracking (clientID, seriesID) pairs through Raft consensus. Session registration, unregistration, and result caches are replicated across the cluster so all nodes maintain identical deduplication state.

**Reference:** Raft PhD thesis Figure 6.1 (session-based client interaction)

**Source:** `pkg/session.go`, `pkg/host.go`, `pkg/internal/session/manager.go`, `pkg/internal/rsm/statemachine.go`, `pkg/proto/entry.go`, `pkg/proto/constants.go`

## Overview

Without sessions, a client that retries a proposal after a timeout may cause the proposal to be applied twice. The network acknowledged the commit but the response was lost, so the client has no way to know whether the proposal was applied. Sessions solve this:

```
Client                    Leader                   Followers
  │                         │                         │
  │─── Propose(cmd) ────────>│                         │
  │    clientID=42           │─── Replicate ──────────>│
  │    seriesID=5            │                         │
  │                          │<── Applied ─────────────│
  │<── Result ───────────────│                         │
  │    (network drops this)  │                         │
  │                          │                         │
  │─── Propose(cmd) ────────>│  seriesID=5 again       │
  │    clientID=42           │  CheckDuplicate: HIT    │
  │    seriesID=5            │  return cached result   │
  │<── Cached Result ────────│                         │
```

The session manager on every replica caches the result for each (clientID, seriesID). When a retransmission arrives, the cached result is returned without re-applying the command to the state machine.

## Session Lifecycle

### 1. Registration through Raft Consensus

**Source:** `pkg/host.go`

```go
session, err := host.GetNewSession(ctx, shardID)
```

`GetNewSession` creates a new session by generating a cryptographically random 64-bit clientID and proposing a register entry through Raft. All replicas apply the registration to their local session manager before the method returns. The flow:

1. Generate random 64-bit clientID
2. Set seriesID to sentinel `SeriesIDForRegister` (math.MaxUint64-1)
3. Propose registration entry: `Entry{ClientID, SeriesID=MaxUint64-1, Cmd: nil}`
4. Block until entry is committed and applied across the quorum
5. Advance seriesID to 1 via `PrepareForPropose()`
6. Return active session to caller

Alternative: Use `host.RegisterSession(ctx, session)` to register a pre-created session.

### 2. Deduplicated Proposals

After registration, use the session with deduplicated proposal methods:

```go
// Async proposal with deduplication
err := host.ProposeWithSession(ctx, session, cmd)

// Synchronous read-like proposal
result, err := host.SyncProposeWithSession(ctx, session, cmd)
```

The session's seriesID starts at 1 and advances with each proposal. After each result, update the session:

```go
session.ProposalCompleted()  // atomically: respondedTo = seriesID; seriesID++
```

The `respondedTo` field tracks the highest seriesID the client has acknowledged. The session manager uses this to evict stale cached results—once the client confirms seriesID N, all results for seriesID <= N can be discarded.

### 3. Session Closure through Raft

```go
err := host.CloseSession(ctx, shardID, session)
```

`CloseSession` proposes an unregister entry through Raft with seriesID set to `SeriesIDForUnregister` (math.MaxUint64). All replicas apply the entry, removing the session from their local session manager and freeing all cached results. The method blocks until the entry is applied.

## SeriesID Sentinels

Session control operations use sentinel values at the top of the uint64 range to distinguish from normal proposals:

**Source:** `pkg/session.go`, `pkg/proto/constants.go`

```go
SeriesIDForRegister   = math.MaxUint64 - 1  // 0xFFFFFFFFFFFFFFFE
SeriesIDForUnregister = math.MaxUint64       // 0xFFFFFFFFFFFFFFFF
SeriesIDFirstProposal = 1
```

Normal proposals use seriesID starting at 1 and incrementing. The sentinels occupy the top of the uint64 range, so a client cannot collide with them for 2^64 - 3 proposals.

| SeriesID | Purpose |
|----------|---------|
| 1 to MaxUint64-2 | Normal proposals (monotonically increasing) |
| MaxUint64-1 | Register session entry |
| MaxUint64 | Unregister session entry |

## Entry Detection

The RSM routes entries to the session manager or user state machine based on session indicators. Three helper methods on `proto.Entry` classify entries:

**Source:** `pkg/proto/entry.go`

```go
// True if entry has a real clientID (not a no-op session)
func (e *Entry) IsSessionManaged() bool {
    return e.ClientID != 0
}

// True if entry is a register request
func (e *Entry) IsNewSessionRequest() bool {
    return !e.IsConfigChange() && len(e.Cmd) == 0 &&
        e.ClientID != 0 && e.SeriesID == math.MaxUint64-1
}

// True if entry is an unregister request
func (e *Entry) IsEndOfSessionRequest() bool {
    return !e.IsConfigChange() && len(e.Cmd) == 0 &&
        e.ClientID != 0 && e.SeriesID == math.MaxUint64
}
```

**Criteria:**
- Register and unregister entries have empty Cmd (no user data)
- Both use sentinel SeriesID values
- Normal proposals have clientID != 0 and a user command

### Apply Routing Logic

**Source:** `pkg/internal/rsm/statemachine.go`

The RSM `Apply` method processes entries in the order they appear in the batch, routing based on session type:

```
for entry in batch:
    if entry.IsNewSessionRequest():
        sessions.Register(clientID)

    elif entry.IsEndOfSessionRequest():
        sessions.Unregister(clientID)

    elif entry.IsSessionManaged():
        result, isDup := sessions.CheckDuplicate(clientID, seriesID)
        if !isDup:
            // Add to user SM batch
            smEntries = append(smEntries, entry)
        // After SM completes:
        sessions.RecordResult(clientID, seriesID, respondedTo, result)

    else:  // clientID == 0, no-op session
        smEntries = append(smEntries, entry)  // No deduplication
```

The Apply method pre-allocates buffers (`smEntryBuf`, `smResultBuf`, `indexMapBuf`) which are reused across batches via slice truncation (`[:0]`), avoiding heap allocations since Apply runs from a single apply goroutine per shard.

## Session Manager

**Source:** `pkg/internal/session/manager.go`

The session manager is per-shard and tracks all active client sessions and their result caches. It is accessed only from the apply goroutine, so it does not require locks.

### Core Operations

**Register:** Creates a new session for a clientID.
- Returns `ErrSessionAlreadyExists` if the clientID is already registered
- Returns `ErrSessionLimitExceeded` if the shard exceeds `MaxTotalSessions` (default 16,384)

**CheckDuplicate:** Checks if a proposal is a duplicate.
- If `seriesID <= respondedTo`: Returns false (client already acknowledged this)
- If `seriesID` is in the result cache: Returns cached result and true
- Otherwise: Returns false (proposal is new, add to SM batch)

**RecordResult:** Stores the proposal result after SM application.
- Evicts stale responses (where seriesID <= respondedTo) before caching
- Returns `ErrResponseLimitExceeded` if response cache exceeds `MaxSessionsPerClient` after eviction
- Returns `ErrSessionNotFound` if clientID is not registered

**Unregister:** Removes a session and all its cached results.
- Called when an unregister entry is applied
- Returns `ErrSessionNotFound` if the session does not exist

**ExpireSessions:** Periodically removes idle sessions by TTL.
- Takes parameters `(ttl, maxExpire)` to limit batch size
- Returns slice of expired clientIDs
- Uses index-based `lastActiveIndex` instead of wall-clock time for determinism
- All replicas make identical expiry decisions at the same Raft indices

### Error Types

| Error | Scenario |
|-------|----------|
| `ErrSessionAlreadyExists` | Register a clientID that is already active |
| `ErrSessionNotFound` | Unregister or CheckDuplicate on a non-existent clientID |
| `ErrSessionLimitExceeded` / `ErrTooManySessions` | Shard exceeds `MaxTotalSessions` |
| `ErrResponseLimitExceeded` | RecordResult exceeds `MaxSessionsPerClient` cached responses |
| `ErrSessionExpired` | Proposal references a session that expired and was removed |
| `SessionExpiredError` | Wraps ShardID and ClientID for diagnostic purposes |

**Source:** `pkg/errors.go`, `pkg/internal/session/errors.go`

## Snapshot Integration

Session state is persisted in snapshots so that deduplication state survives recovery. The RSM writes session data before user state machine data, then reads in the same order.

**Source:** `pkg/internal/rsm/statemachine.go`

### Snapshot Format

Session data is serialized as:
```
[numSessions:4 bytes]
  for each session:
    [clientID:8][respondedTo:8][numResponses:4]
      for each cached response:
        [seriesID:8][result value:variable length]
    [lastActiveIndex:8]  // index-based, not wall-clock
```

All integers use little-endian encoding.

### Save Path

```go
func (s *StateMachine) SaveSnapshot(ctx, w, stopper) error {
    s.sessions.MarshalTo(w)           // session data first
    s.sm.SaveSnapshot(ctx, w, stopper)  // user SM data second
}
```

`MarshalTo` sorts clientIDs and seriesIDs before writing to ensure deterministic output. Checks the stopper channel; returns `ErrStopped` if cancelled.

### Recovery Path

```go
func (s *StateMachine) RecoverFromSnapshot(ctx, r, stopper) error {
    s.sessions.UnmarshalFrom(r)           // deserialize sessions first
    s.sm.RecoverFromSnapshot(ctx, r, stopper)  // user SM second
}
```

`UnmarshalFrom` clears the existing session state and replaces it entirely with the deserialized data. After recovery, the session manager has identical deduplication state as the snapshot source. Checks stopper; returns `ErrStopped` if cancelled.

### Expiration During Recovery

Sessions not included in the snapshot are considered expired. A proposal with an expired clientID returns `ErrSessionExpired`. This is correct because the snapshot became current via Raft consensus, which guarantees all expired sessions were removed before the snapshot was taken.

## No-Op Sessions

For idempotent operations that do not require at-most-once delivery, bypass the session manager:

```go
session := host.GetNoOPSession(shardID)
err := host.ProposeWithSession(ctx, session, cmd)
```

No-op sessions have a fixed `clientID` of 0. Entries with `clientID == 0` skip deduplication checks and go directly to the user state machine.

**Characteristics:**
- No Raft registration (no round-trip delay)
- No session manager memory overhead
- No at-most-once delivery guarantee
- Suitable for SET operations where re-application is idempotent

**Validation:** `ValidForSessionOp(shardID)` returns false for no-op sessions. `Host.CloseSession` rejects no-op sessions with `ErrInvalidSession`.

## Configuration

**Source:** `pkg/config/config.go`

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `MaxSessionsPerClient` | 128 | Maximum cached responses per session |
| `MaxTotalSessions` | 16,384 | Maximum concurrent sessions per shard |
| `SessionExpiryEntries` | 0 (disabled) | Index-based TTL for automatic session expiry |

**Note:** `MaxSessionsPerClient` limits the per-session response cache; `MaxTotalSessions` limits the total number of active sessions. Set `SessionExpiryEntries` to a positive value to enable deterministic session expiry (e.g., 10000 means expire idle sessions every 10K applied entries).

## Thread Safety and Concurrency

**Session struct:** Uses `atomic.Uint64` for `seriesID` and `respondedTo` to allow concurrent callers. However, concurrent proposals on the same session are discouraged because responses may arrive out of order.

**Manager struct:** Not thread-safe. Accessed only from the per-shard apply goroutine, which processes entries sequentially. This avoids lock contention on the hot apply path.

**ProposalCompleted atomicity:** Uses a CAS loop to ensure only forward progress on `respondedTo`, even if multiple goroutines call it concurrently.

## Deduplication Example

Without sessions, a network loss exposes the client to duplicate application:

```
Client                  Leader              Followers
  │                       │                    │
  ├─ Propose(cmd) ──────> │                    │
  │  clientID=42          ├─ Replicate ──────> │
  │  seriesID=5           │                    │
  │                       │ < Applied ─────────┤
  │ < Result ─────────────┤ (lost)
  │                       │
  ├─ Propose(cmd) ──────> │  Same cmd!
  │  clientID=42          │  CheckDuplicate:   │
  │  seriesID=5           │    Hit! Return     │
  │ < CachedResult ───────┤    cached result
```

With deduplication, the second proposal returns the cached result immediately without re-applying.

## End-to-End Flow

### 1. Registration
1. Client calls `host.GetNewSession(ctx, shardID)`
2. Host generates random clientID
3. Host proposes register entry with seriesID=MaxUint64-1
4. Raft replicates to all replicas
5. Each replica applies the entry and creates a session in the manager
6. Host returns active session with seriesID advanced to 1

### 2. Deduplicated Proposal
1. Client calls `host.ProposeWithSession(ctx, session, cmd)`
2. Host sets entry fields: ClientID, SeriesID, Cmd
3. Raft replicates entry to followers
4. Each replica applies:
   - Calls `CheckDuplicate(clientID, seriesID)`
   - If hit: returns cached result, skips SM
   - If miss: updates SM, calls `RecordResult` to cache
5. Client receives result
6. Client calls `session.ProposalCompleted()` to advance counters

### 3. Session Closure
1. Client calls `host.CloseSession(ctx, shardID, session)`
2. Host proposes unregister entry with seriesID=MaxUint64
3. Raft replicates to all replicas
4. Each replica applies and removes the session, freeing cached results
