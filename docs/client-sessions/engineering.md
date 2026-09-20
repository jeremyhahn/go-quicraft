# Client Sessions — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/session.go`,
> `pkg/internal/session`, and the session fields of `pkg/proto` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Design rationale, rejected alternatives and the scope fence are in
[`architecture.md`](architecture.md). The Go call surface is in
[`api.md`](api.md).

Client sessions in QuicRaft provide **at-most-once proposal delivery** by tracking (clientID, seriesID) pairs through Raft consensus. Session registration, unregistration, and result caches are replicated across the cluster so all nodes maintain identical deduplication state.

**Reference:** Raft PhD thesis Figure 6.1 (session-based client interaction)

**Source:** `pkg/session.go`, `pkg/host.go`, `pkg/internal/session/manager.go`,
`pkg/internal/session/session.go`, `pkg/internal/rsm/statemachine.go`,
`pkg/proto/entry.go`, `pkg/proto/constants.go`, `pkg/proto/membership.go`
(`SessionPolicy`)

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

// ...or, declaring the principal the session belongs to:
session, err := host.GetNewSessionWithOwner(ctx, shardID, tenantID)
```

`GetNewSession` generates a cryptographically random 64-bit clientID and
proposes a register entry through Raft. The registration is committed and
applied before the method returns. The flow:

1. Generate a random 64-bit clientID (`crypto/rand`).
2. `PrepareForRegister()`: seriesID becomes `SeriesIDForRegister`
   (`math.MaxUint64 - 1`), respondedTo is cleared.
3. Propose `Entry{ClientID, OwnerID, SeriesID: SeriesIDForRegister, Cmd: nil}`.
4. Block until the entry is committed and applied.
5. Verify the applied result's `Value` equals the clientID. A mismatch means the
   apply path refused the registration and produces a `*SessionError` wrapping
   `ErrInvalidOperation`.
6. `PrepareForPropose()`: advance seriesID to `SeriesIDFirstProposal` (1).

`GetNewSessionWithOwner` is identical except that it stamps `Entry.OwnerID`, so
the session counts against the shard's replicated per-owner cap. An `ownerID` of
zero declares no owner and is exactly `GetNewSession`. See
[Owners and the per-owner cap](#owners-and-the-per-owner-cap).

**Sessions are refused on disk-backed state machines.** `GetNewSession` returns
`ErrSessionsUnsupportedOnDisk` when the shard's state machine is an
`sm.DiskStateMachine`: an on-disk SM cannot carry the session table across a
restart, so a registered session would silently stop existing on any replica
that restarted. Use `Host.GetNoOPSession` on those shards.

Alternative: `host.RegisterSession(ctx, session)` registers a pre-created
`Session`, which is how a client that persists its own clientID survives its own
restart.

### 2. Deduplicated Proposals

After registration, use the session with deduplicated proposal methods:

```go
// Async proposal with deduplication
err := host.ProposeWithSession(ctx, session, cmd)

// Synchronous read-like proposal
result, err := host.SyncProposeWithSession(ctx, session, cmd)
```

The session's seriesID starts at 1 and advances with each proposal. After each
result, update the session:

```go
session.ProposalCompleted()  // CAS-claim seriesID, then advance respondedTo to it
```

`ProposalCompleted` claims the current seriesID with a CAS loop and then advances
`respondedTo` **monotonically** to the claimed value. A plain store would let a
goroutine that won an earlier CAS but ran its store later regress `respondedTo`
below a higher completion, breaking the `respondedTo == seriesID - 1` invariant
under concurrent completions.

The `respondedTo` field tracks the highest seriesID the client has acknowledged,
and it rides on every subsequent entry. The session manager uses it to evict
stale cached results: once the client confirms seriesID N, all results for
seriesID <= N are discarded.

### 3. Session Closure through Raft

```go
err := host.CloseSession(ctx, shardID, session)
```

`CloseSession` proposes an unregister entry through Raft with seriesID set to `SeriesIDForUnregister` (math.MaxUint64). All replicas apply the entry, removing the session from their local session manager and freeing all cached results. The method blocks until the entry is applied.

## SeriesID Sentinels

Session control operations use sentinel values at the top of the uint64 range to distinguish from normal proposals:

**Source:** `pkg/session.go`, `pkg/proto/constants.go`

```go
// Public API (pkg/session.go)
SeriesIDForRegister   uint64 = math.MaxUint64 - 1  // 0xFFFFFFFFFFFFFFFE
SeriesIDForUnregister uint64 = math.MaxUint64      // 0xFFFFFFFFFFFFFFFF
SeriesIDFirstProposal uint64 = 1

// Wire-level equivalents (pkg/proto/constants.go), used by the Entry helpers
SessionSeriesIDForRegister   uint64 = 1<<64 - 2
SessionSeriesIDForUnregister uint64 = 1<<64 - 1
NotSessionManagedClientID    uint64 = 0
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
    return e.ClientID != NotSessionManagedClientID
}

// True if entry is a register request
func (e *Entry) IsNewSessionRequest() bool {
    return !e.IsConfigChange() && len(e.Cmd) == 0 &&
        e.ClientID != NotSessionManagedClientID &&
        e.SeriesID == SessionSeriesIDForRegister
}

// True if entry is an unregister request
func (e *Entry) IsEndOfSessionRequest() bool {
    return !e.IsConfigChange() && len(e.Cmd) == 0 &&
        e.ClientID != NotSessionManagedClientID &&
        e.SeriesID == SessionSeriesIDForUnregister
}
```

**Criteria:**
- Register and unregister entries have empty Cmd (no user data)
- Both use sentinel SeriesID values
- Normal proposals have clientID != 0 and a user command

### Apply Routing Logic

**Source:** `pkg/internal/rsm/statemachine.go`

`Apply` first checks batch contiguity against `lastApplied` (a gap returns
`ErrApplyGap`), then adopts the shard's replicated session policy
(`refreshSessionPolicy`) so the limits this batch is evaluated against are the
*shard's* and not this node's. It then segments the batch at expiry checkpoints
and hands each segment to `applyBatch`. See
[Deterministic expiry](#deterministic-expiry).

`applyBatch` classifies entries in order:

```
for i, entry in segment:
    if entry.IsConfigChange() or entry.IsInitialMembership():
        results[i] = {}                       # membership is applied elsewhere
        continue
    if not session-managed and len(entry.Cmd) == 0:
        results[i] = {}                       # leader no-op entry
        continue

    if entry.IsNewSessionRequest():
        sessions.Register(clientID, entry.OwnerID, entry.Index)
        # on success: results[i].Value = clientID, and any in-batch dedup
        # bookkeeping for this clientID is discarded (new incarnation)
        continue

    if entry.IsEndOfSessionRequest():
        sessions.Unregister(clientID)
        # on failure: results[i] = SESSION_EXPIRED
        continue

    if entry.IsSessionManaged():
        result, isDup, err := sessions.CheckDuplicate(clientID, seriesID, index)
        if err != nil:                        # ErrSessionNotFound / ErrResponseEvicted
            results[i] = SESSION_EXPIRED
            continue
        if isDup:
            results[i] = owned copy of result # cached, skip the user SM
            continue

        # In-batch duplicate: a retry that committed alongside its original.
        # MUST be checked before the capacity gate and before admittedThisBatch++.
        if firstIdx, seen := batchSeen[clientID][seriesID]; seen:
            mirrors[i] = firstIdx             # copy the first occurrence's result
            continue

        # Capacity gate BEFORE mutating the user SM.
        if sessions.CanRecord(clientID, entry.RespondedTo,
                              admittedThisBatch[clientID]) != nil:
            results[i] = SESSION_EXPIRED      # ErrResponseLimitExceeded
            continue

        admittedThisBatch[clientID]++
        batchSeen[clientID][seriesID] = i
        smEntries = append(smEntries, entry)
        continue

    smEntries = append(smEntries, entry)      # clientID == 0, no deduplication

# after the batched user-SM Update:
#   RecordResult for each admitted entry, UNLESS sessionLifecycleAt[clientID] > i
#   then resolve mirrors[] from the first occurrences' results
```

Three pieces of batch-scoped state carry the correctness of this loop.

**`batchSeen` / `mirrors` — in-batch duplicates.** A client may retry a request
whose original proposal has not yet committed. If both commit into the same apply
batch they carry the same `(clientID, seriesID)` and neither is in the session
table yet, so `CheckDuplicate` reports *both* as new and the user operation would
run twice. `batchSeen` records the entry index of the first occurrence; later
occurrences are recorded in `mirrors` and copy that result without re-applying.
The check runs **before** the capacity gate because a retry consumes no new cache
slot — the first occurrence already accounts for it — so gating it could
spuriously reject it as "cache full".

**`admittedThisBatch` — projected capacity.** `RecordResult` runs only after the
batched user-SM `Update`, so the capacity gate must count new series for the same
client already admitted earlier in this batch. Without it, a batch of new series
could collectively overflow the response cache.

**`sessionLifecycleAt` — lifecycle transitions inside a batch.** An entry
admitted earlier in the batch may reach `RecordResult` after the session it was
admitted against is gone, replaced or removed by a later entry in the *same*
batch. An entry whose clientID has a transition at a higher index than its own
does not record a response: for a closed session the cache is moot, and for a
re-registered clientID, caching into the replacement would deduplicate a future
proposal of that series which the replacement session never applied.

`applyBatch` reuses three buffers (`smEntryBuf`, `smResultBuf`, `indexMapBuf`)
via `[:0]` truncation. That is safe because `Apply` runs on a single apply
goroutine per shard.

### Response Cache Overflow Preserves At-Most-Once

A new (non-duplicate) session series that cannot be cached is **rejected as `SESSION_EXPIRED` before the user state machine is mutated**. After `CheckDuplicate` reports the series is new, `Apply` calls `Manager.CanRecord` — a read-only projection of what `RecordResult` would do (advance `respondedTo`, evict acknowledged responses, then count survivors). If the response cache would still be full after eviction, `CanRecord` returns `ErrResponseLimitExceeded` and the entry is mapped to `SESSION_EXPIRED` (`sm.ResultSessionExpired`) and skipped — it never reaches `smEntries`, so the user SM never observes it.

This is the correct behavior per Raft PhD Figure 6.1, step 3 (treat a missing cache record as `SESSION_EXPIRED`). Applying an over-capacity entry without being able to cache its result would break at-most-once: a later replay would find no cached response to deduplicate against and re-apply the mutation. Rejecting it up front keeps the entry from ever mutating the state machine.

The gate is deterministic across replicas: it is driven by the entry's
`RespondedTo` and the cache contents, with no wall clock anywhere. It also
accounts for `admittedThisBatch[clientID]`, the count of new series for the same
client already admitted earlier in the current batch but not yet recorded
(`RecordResult` runs only after the batched user-SM `Update`). Without that
count, a single batch of new series could collectively overflow the cache.

The entry's own seriesID is not part of the question — `CanRecord` asks whether
the cache has **room** after eviction, and a new response always consumes a slot.
A duplicate seriesID never reaches the gate, having been answered from the cache
(or mirrored from an earlier occurrence in the same batch) already.

## Session Manager

**Source:** `pkg/internal/session/manager.go`

The session manager is per-shard and tracks all active client sessions and their
result caches. It is accessed only from the apply goroutine, so it holds no
locks.

Per-session state (`clientSession`):

| Field | Purpose |
|-------|---------|
| `clientID` | The session key |
| `ownerID` | The principal declared at registration; retained for the life of the session because removal has to return the slot to the right owner |
| `registeredIndex` | Log index of the Register entry that created **this incarnation** |
| `respondedTo` | Highest series the client has acknowledged |
| `responses` | `seriesID -> sm.Result` dedup cache |
| `lastActiveIndex` | Log index of last activity, for deterministic expiry |

### Core Operations

**`Register(clientID, ownerID, index)`** creates a session.
- `ErrSessionAlreadyExists` if the clientID is already registered.
- `ErrSessionLimitExceeded` if the shard is at `MaxTotalSessions` **or** the
  owner is at `MaxSessionsPerOwner`. Both exhaustion cases report the *same*
  sentinel on purpose: the caller must treat a refused registration identically
  whatever refused it, and a distinct error would invite a distinct code path —
  and a code path only one replica takes is the divergence the cap exists to
  prevent.
- `index` becomes both `registeredIndex` and `lastActiveIndex`.

**`CheckDuplicate(clientID, seriesID, index)`** classifies a proposal.
- `ErrSessionNotFound` if the clientID is not registered.
- `ErrSessionNotFound` if `index < registeredIndex` — the **incarnation floor**.
  An entry below the index that created this incarnation belongs to a *previous*
  one: the session expired and the client re-registered under an ID it had
  persisted (`Host.RegisterSession` accepts a caller-supplied ID, which is the
  documented way a client survives its own restart). The new incarnation starts
  with `respondedTo` zero and an empty cache, so without this the stale entry
  looks brand new and is applied a second time — at-most-once broken with no
  error raised anywhere. The comparison is index-based, so it is deterministic
  and replicated for free.
- `ErrResponseEvicted` if `seriesID <= respondedTo`: the client already
  acknowledged this and the cached response was evicted.
- Cached result and `true` if `seriesID` is in the response cache (and
  `lastActiveIndex` is advanced).
- `false` with no error if the proposal is new.

Both errors map to `SESSION_EXPIRED` per PhD Figure 6.1 step 3.

**`CanRecord(clientID, respondedTo, pending)`** is a read-only capacity check run
before applying a new series.
- Mirrors `RecordResult`'s `respondedTo` advance and eviction, mutating nothing.
- `pending` is the count of same-client new series already admitted in the
  current batch.
- `ErrResponseLimitExceeded` if `surviving + pending >= maxResponses`.
- `ErrSessionNotFound` if the clientID is not registered.
- The entry's own seriesID is deliberately **not** a parameter: the question is
  whether the cache has room after eviction, and a new response always consumes
  a slot. A duplicate seriesID is answered from the cache by the dedup path and
  never reaches this check.

**`RecordResult(clientID, seriesID, respondedTo, index, result)`** caches a
result after SM application.
- Advances `respondedTo`, then evicts every cached response at or below it.
- Advances `lastActiveIndex` **before** the limit check, so an active-but-full
  session is not treated as idle by deterministic expiry.
- `ErrResponseLimitExceeded` if the cache is still full after eviction;
  `ErrSessionNotFound` if the clientID is not registered.

**`Unregister(clientID)`** removes a session and its cached results, releasing
the owner slot. `ErrSessionNotFound` if it does not exist.

**`ExpireSessions(currentIndex, inactivityThreshold, maxExpire)`** removes idle
sessions and returns the expired clientIDs.
- Returns nil immediately if `maxExpire <= 0` or `inactivityThreshold == 0`.
- A session is expired when `currentIndex - lastActiveIndex > inactivityThreshold`.
- Collects **all** candidates, sorts them by clientID, and only then applies the
  `maxExpire` limit, so every replica selects and removes the identical subset at
  the same Raft index regardless of Go's randomized map iteration order.
- Releases each expired session's owner slot. Expiry is the one removal path not
  driven by an explicit client action, so missing it would leak owner counts
  upward until a long-lived shard refused every registration from an owner that
  in fact holds none.

**`SetLimits(maxTotal, maxResponses, maxPerOwner)`** replaces the manager's
limits. It exists for the one moment a replica learns them: a state machine
created before its shard's committed configuration was known
(`rsm.StateMachine.refreshSessionPolicy`). A zero `maxTotal` or `maxResponses`
is ignored, because no caller can mean "admit nothing" by either; `maxPerOwner`
is assigned unconditionally, because zero is a real value there — the cap off.
Existing sessions are left alone, so lowering `maxTotal` below the current count
stops further registrations rather than evicting anyone.

### Owners and the per-owner cap

`MaxTotalSessions` is a shard-wide bound and therefore no defence against a
single principal exhausting it: a clientID is eight bytes the caller picks, so
one client can register under as many as it likes until the table is full and
every other tenant on the shard loses at-most-once delivery. That is a denial of
service available to anyone with propose rights, and the shard-wide cap is
precisely the thing being exhausted, so it cannot also be the defence.

`MaxSessionsPerOwner` (default 128, zero disables) bounds concurrent sessions per
declared **owner**. The owner is whatever the embedder decides identifies a
principal — a tenant ID, an account number, a hash of a verified subject — passed
to `Host.GetNewSessionWithOwner` / `NewSessionWithOwner` and carried in the
replicated log as `Entry.OwnerID`.

Two properties matter:

- **It is a state-machine decision, not a leader-local one.** The owner rides in
  the entry because every replica must accept or refuse a registration
  identically from the log alone, including on replay after a restart. The
  authenticated peer on the connection that carried the proposal is not in the
  log and is not the same on a replica replaying it.
- **It is only as trustworthy as the embedder's own authentication.** This
  library authenticates *nodes*, not clients, and cannot derive an owner. A
  caller free to pick any `ownerID` is free to evade the cap, so the value must
  come from something the embedder verified.

`ownerID == 0` means unset. Such sessions are neither counted nor capped, which
is what keeps embedders that do not use the owner APIs working unchanged.

The manager keeps `ownerCounts` (ownerID -> live session count) as a **derived**
secondary index so `Register` is O(1) rather than a full scan on every
registration. Being derived, it has one correctness requirement: every mutation
of `sessions` moves it in step. `UnmarshalFrom` rebuilds it by counting the
decoded table rather than trusting anything on the wire — a persisted count is
one more thing a corrupt snapshot can get wrong, in a way no bounds check would
catch and that would show up only as registrations refused on one replica.

### Error Types

| Error | Scenario |
|-------|----------|
| `ErrSessionAlreadyExists` | Register a clientID that is already active |
| `ErrSessionNotFound` | Unregister or CheckDuplicate on a non-existent clientID, or an entry below the session's `registeredIndex` |
| `ErrSessionLimitExceeded` | Shard at `MaxTotalSessions`, or owner at `MaxSessionsPerOwner`. `quicraft.ErrTooManySessions` is a legacy alias for the public form |
| `ErrResponseLimitExceeded` | `CanRecord`/`RecordResult`: cache full after eviction (maps to `SESSION_EXPIRED`) |
| `ErrResponseEvicted` | `CheckDuplicate`: seriesID already acknowledged and response evicted (maps to `SESSION_EXPIRED`) |
| `ErrSnapshotCorrupted` | Sentinel for `*BoundsError`, raised when a snapshot's decoded counts exceed the configured or hardcoded limits |
| `ErrSessionExpired` | Public: a proposal referenced a session that expired or was removed |
| `ErrSessionsUnsupportedOnDisk` | Public: `GetNewSession` on a shard backed by an `sm.DiskStateMachine` |
| `ErrInvalidSession` | Public: `CloseSession` on a no-op session |
| `*SessionExpiredError` | Public: wraps ShardID and ClientID; `errors.Is` matches `ErrSessionExpired` |

The `pkg/internal/session` sentinels are separate from the `quicraft` package's
to avoid an import cycle (quicraft imports session transitively via rsm).

**Source:** `pkg/errors.go`, `pkg/internal/session/errors.go`

## Deterministic expiry

**Session expiry is driven by log index, never by wall-clock time.** That is not
a performance choice; it is what makes expiry a state-machine operation at all.
Every replica applies the same entries in the same order, so an index-driven rule
expires the same sessions at the same point in the log on every replica. A
wall-clock rule would not: two replicas would expire different sessions at
different moments, and from then on one answers a session-managed proposal from
its response cache while the other reports the session expired. That is a
permanent state-machine divergence that **no Raft invariant detects** — the logs
agree, the commit indexes agree, and the state machines do not.

Expiry runs at fixed **checkpoints**: the multiples of `SessionExpiryEntries`
that fall in `(prevIndex, lastIndex]`, where `prevIndex` is the last index
applied before this `Apply` call. For each checkpoint, `Apply` first flushes
every not-yet-applied entry at or below the checkpoint index, then runs
`ExpireSessions(boundary, interval, maxExpirePerCheckpoint)` — so expiry always
observes a fully-applied session table as of that exact index. The trailing
segment above the last checkpoint is flushed afterwards; its checkpoint is
reached in a future `Apply`. When expiry is disabled the whole batch goes through
in one shot, keeping the segmentation off the hot path.

The checkpoint set continues seamlessly across `Apply` calls and across restart,
but **only because `lastApplied` is explicitly seeded** on the two paths that
resume a state machine (`Host.StartShard` and the engine's `InstallSnapshot`
recovery, both via `SetLastApplied`). `RecoverFromSnapshot` does not restore it:
the session table is in the snapshot stream, this counter is not. A replica
resuming with it left at zero would re-walk every checkpoint its peers already
walked, and since `ExpireSessions` is capped per call, the re-walk would delete
sessions the shard still holds. The contiguity guard at the top of `Apply`
(`ErrApplyGap`) is what makes that unseeded state fail loudly instead of
diverging silently.

## Snapshot Integration

Session state is persisted in snapshots so that deduplication state survives recovery. The RSM writes session data before user state machine data, then reads in the same order.

**Source:** `pkg/internal/rsm/statemachine.go`

### Snapshot Format

Session data is serialized as:
```
[numSessions:4]
  for each session (client IDs written in sorted order):
    [clientID:8][ownerID:8][registeredIndex:8][respondedTo:8][numResponses:4]
      for each cached response (series IDs in sorted order):
        [seriesID:8][result.Value:8][dataLen:4][data:dataLen]
    [lastActiveIndex:8]     // index-based, not wall-clock
```

All integers are little-endian. `ownerCounts` is **not** written — it is derived
state, and `UnmarshalFrom` rebuilds it from the decoded table so a restored count
cannot disagree with the table it describes.

`ownerID` and `registeredIndex` both have to survive the snapshot:

- Without `ownerID`, restored sessions would all read as ownerless, their owners'
  counts would come back at zero, and a replica that recovered from a snapshot
  would admit registrations its peers refuse.
- Without `registeredIndex`, the incarnation floor in `CheckDuplicate` would
  disappear on restore — which is exactly the path (restart, then a stale entry
  commits) the floor exists to close.

Both were added by changing the layout in place, because every wire format in
this repository is pinned at v1 for the pre-release period and consumers are
updated in lockstep. See [Compatibility](../compatibility.md).

`UnmarshalFrom` bounds-checks everything it decodes against the configured
limits, so a corrupted or malicious snapshot cannot drive an allocation:
`numSessions <= maxTotal`, `numResponses <= maxResponses`, and
`dataLen <= maxResponseDataSize` (64 MiB). Violations return a `*BoundsError`,
which `errors.Is` matches against `ErrSnapshotCorrupted`.

Decoding parses into a **staging map** and swaps it in only after the whole
stream parses cleanly. Populating the live table incrementally would, on a
mid-stream failure, leave the node with a hybrid of new sessions and whatever
the caller did after the error, inconsistent with the pre-recovery user-SM state.

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
| `MaxSessionsPerClient` | 128 | Maximum cached **responses** per session |
| `MaxTotalSessions` | 16,384 | Maximum concurrent sessions per shard |
| `MaxSessionsPerOwner` | 128 | Maximum concurrent sessions per declared owner; 0 disables the cap |
| `SessionExpiryEntries` | 100,000 | Index-based inactivity threshold for automatic expiry (`config.SessionExpiryDisabled` turns it off) |

**`MaxSessionsPerClient` is misnamed and does not limit sessions.** It is the
response-cache depth — the number of cached results one registered session
retains — and reaches `session.Manager` as `maxResponses`. Read it as
"MaxResponsesPerSession". The name is kept only because renaming a public field
is a breaking change the pre-release pinning does not cover, and the same name
appears in `proto.SessionPolicy` because the wire format is pinned too.
Transposing it with `MaxTotalSessions` or `MaxSessionsPerOwner` has happened
here before and neither mistake fails loudly: one caps a shard at a response
depth, the other lets a single client pin the whole session table.

**Zero does not mean "off" for `SessionExpiryEntries`.** `SetDefaults` reads an
unset field as "I did not choose" and substitutes the default, because one value
cannot mean both that and "never expire anything". Assign
`config.SessionExpiryDisabled` (`math.MaxUint64`) to disable expiry — an
explicit, replicated choice. `Validate` rejects values below 100 (a threshold
that short expires sessions faster than a slow client can round-trip) and warns
when the value exceeds a non-zero `SnapshotEntries`, so expiry runs at least once
per snapshot interval and the session table cannot grow unboundedly between
snapshots. When expiry is disabled, a client that registers and dies without
`CloseSession` leaks an entry that is serialized into every subsequent snapshot
and counts against `MaxTotalSessions` forever.

**All four are SEEDS, not the values the apply path uses.** They are replicated:
a founding replica writes them into the shard's initial membership at bootstrap
(`proto.SessionPolicy`, carried in `proto.Membership` alongside the witness
policy), and from then on every replica applies the COMMITTED values. That is
what makes expiry deterministic in practice and not merely in principle — the
apply path derives its expiry checkpoints from the threshold, refuses
registrations past `MaxTotalSessions` and `MaxSessionsPerOwner`, and bounds the
response cache by `MaxSessionsPerClient`. Two replicas evaluating different
values expire different sessions or disagree on whether a session exists, and
diverge permanently with every Raft invariant intact.

`SessionPolicy.IsSet()` distinguishes a committed policy from the zero value a
membership section written without one decodes to, using `MaxTotalSessions` as
the discriminator: `Validate` rejects a zero `MaxTotalSessions` and `SetDefaults`
fills it, so every seeded policy carries a non-zero cap. That is what lets
`ExpiryEntries == 0` keep its own meaning — expiry explicitly disabled
cluster-wide — instead of being read as an absent policy and silently falling
back to whatever the local node happens to be configured with.

Consequences for an operator:

- All founders must supply the same four values, for the same reason they must supply the same witness policy: a differing value produces a different index-1 entry.
- A node whose `Config` disagrees with what its shard committed applies the shard's values and logs the disagreement at WARN. It does not refuse to start — the committed value can only be changed through consensus.
- `Host.GetShardMembership` reports the committed policy, which is how you check what a shard is actually running.
- There is no config change for these yet: the committed policy is fixed for the life of the shard. Raising `MaxTotalSessions` on a running shard is not currently possible.
- A shard bootstrapped by a pre-policy build has no committed policy, and its membership section does not decode at all under this build — every format is pinned at version 1 and gated on exact equality, so a section written before the session policy existed is refused rather than read at the wrong offsets. Wipe the data directory and re-sync; see [Compatibility](../compatibility.md).

See [Deployment](../multi-group-host/user.md).

## Thread Safety and Concurrency

**Session struct:** Uses `atomic.Uint64` for `seriesID` and `respondedTo`, so the value may be read from any goroutine, but it serves **one proposal at a time**. `ProposeWithSession` stamps the entry with the current `seriesID`, and the manager treats every entry with the same `(clientID, seriesID)` as a retransmission of one command: it applies the first and answers the rest with its result. Two concurrent proposals therefore both reported success while one was silently discarded as a duplicate (#197). The session now holds an `inFlight` reservation (the series number, 0 when idle): `beginProposal` claims it with a CAS, a second concurrent `ProposeWithSession` returns `ErrSessionBusy`, and `endProposal` releases exactly the series it was given — from `ProposalCompleted` on success, from `RequestState.complete` on an error result (before the caller can see the error, because the documented recovery is a retry on the same session), or from `RequestState.Release` as the catch-all. A release for any other series is a no-op, so a late `Release` of a completed request cannot clear the reservation of the proposal that followed it. The claim is `beginCurrentProposal`: read the series, claim it, then re-check that the series is still the one read — `ProposalCompleted` advances the series *before* it releases the reservation, so a claim racing it could otherwise succeed with the previous series and be answered as a retransmission. Series 0 is the "idle" value, so `ValidForProposal` refuses a session still at series 0 (never registered, or registered without `PrepareForPropose`): admitting it would let two concurrent proposals both reserve 0. `Release` itself pins the `RequestState` (an extra outstanding reference) before electing the releaser with a CAS on `released`, so the last pending-map reference cannot recycle the object while the binding is being released; `Release` is called exactly once per request — the object is pooled, so a second call, later or concurrent, is a use-after-free that no ordering inside `Release` can make safe.

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
1. Client calls `host.GetNewSession(ctx, shardID)` (or `...WithOwner`).
2. Host generates a random clientID.
3. Host proposes a register entry with `SeriesID = SeriesIDForRegister` and
   `OwnerID` set.
4. Raft replicates to all replicas.
5. Each replica applies the entry and registers the session, subject to
   `MaxTotalSessions` and `MaxSessionsPerOwner`.
6. Host checks the applied result's `Value` against the clientID and returns the
   active session with seriesID advanced to 1.

### 2. Deduplicated Proposal
1. Client calls `host.ProposeWithSession(ctx, session, cmd)`.
2. Host sets `ClientID`, `SeriesID`, `RespondedTo` and `Cmd` on the entry.
3. Raft replicates the entry to followers.
4. Each replica applies:
   - `CheckDuplicate(clientID, seriesID, index)`.
   - Hit: return an **owned copy** of the cached result and skip the user SM.
     The copy matters — the cache outlives this `Apply` and is serialized into
     snapshots, so handing the client the cache's backing array would let
     application-side mutation corrupt the dedup cache and the snapshot.
   - Miss: gate on `CanRecord`, apply to the user SM, then `RecordResult`.
5. Client receives the result.
6. Client calls `session.ProposalCompleted()` to advance the counters.

### 3. Session Closure
1. Client calls `host.CloseSession(ctx, shardID, session)`.
2. Host proposes an unregister entry with `SeriesID = SeriesIDForUnregister`.
3. Raft replicates to all replicas.
4. Each replica applies and removes the session, freeing its cached results and
   releasing its owner slot.

## Test Strategy

Target: `make test-session`, plus `make fuzz-session`.

| Layer | Covers |
|---|---|
| **Unit** (`pkg/internal/session/*_test.go`) | registration, the response cache, per-owner and per-shard caps, deterministic expiry, the sentinel `SeriesID` handling |
| **Unit** (`pkg/session_test.go`, `pkg/host_ondisk_sessions_test.go`, `pkg/host_session_policy_test.go`) | the public `Session` type, the on-disk refusal, and replicated policy changes |
| **Fuzz** (`make fuzz-session`) | session-bearing entry decode |
| **Integration** | duplicate proposals across a real cluster, and across a leader change |

**What a test here must always assert**: that expiry is driven by *applied
entries*, never by wall-clock time. A test that sleeps to force expiry is
testing something the implementation deliberately does not do, and will pass on
a fast machine while the real mechanism is broken.

**Fault-injection seams**: propose the same `(clientID, seriesID)` twice; restart
a node mid-series; take a snapshot between the two halves of a series.

## Observability

This feature emits **no metrics of its own**. What is observable, and where:

| Signal | Source |
|---|---|
| session count per shard | the host `MetricsCollector`, against `MaxTotalSessions` |
| a rejected registration | the error returned to the caller — `*SessionError`, `*SessionExpiredError` |
| a served duplicate | invisible by design; it is indistinguishable from the original from the caller's side, which is the entire point |

**Never log a `ClientID` alongside application data.** A `ClientID` is eight
bytes the embedder chooses and an `OwnerID` names the embedder's tenant or
account; both are the embedder's identifiers, not this library's, and this
library makes no promise about what they contain.

`OwnerID` is **not a security boundary on its own** — the value is whatever the
embedder claims — so the per-owner cap it drives is only as trustworthy as the
embedder's own authentication.

## Cross-References

- [API Reference](../multi-group-host/api.md) -- `Session`, `Host` session methods
- [RSM Adapter](../state-machines/engineering.md) -- how `Apply` drives the user state machine
- [State Machine Interfaces](../state-machines/engineering.md) -- `sm.Result`, `sm.DiskStateMachine`
- [Wire Format](../wire-protocol/engineering.md) -- `Entry` session fields and the `Membership` session policy
- [Raft Protocol](../raft-consensus/engineering.md) -- commit, apply and snapshot ordering
- [Compatibility](../compatibility.md) -- wire-format pinning and the session-policy layout change
- [Deployment](../multi-group-host/user.md) -- choosing the session limits for a shard
