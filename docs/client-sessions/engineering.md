# Client Sessions — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/session.go`,
> `pkg/internal/session`, and the session fields of `pkg/proto` ·
> Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

Client sessions give at-most-once proposal delivery by tracking
`(clientID, seriesID)` through Raft. Registration, unregistration and the
response cache are replicated, so every replica holds the same deduplication
state. Reference: Raft PhD thesis, Figure 6.1.

Rationale and scope are in [`architecture.md`](architecture.md), the call
surface in [`api.md`](api.md).

| Source | Holds |
|---|---|
| `pkg/session.go` | the public `Session` type |
| `pkg/host.go` | `GetNewSession`, `GetNewSessionWithOwner`, `RegisterSession`, `ProposeWithSession`, `SyncProposeWithSession`, `CloseSession`, `GetNoOPSession` |
| `pkg/internal/session/manager.go`, `session.go`, `errors.go` | the session manager |
| `pkg/internal/rsm/statemachine.go` | routing of session entries in `Apply` |
| `pkg/proto/entry.go`, `constants.go`, `membership.go` | entry fields, sentinels, `SessionPolicy` |

## Lifecycle

### Registration

`GetNewSessionWithOwner(ctx, shardID, ownerID)` (and `GetNewSession`, which is
the same with owner 0):

1. Refuse a `DiskStateMachine` shard with `ErrSessionsUnsupportedOnDisk`.
2. Draw a client ID from `crypto/rand`, redrawing on zero (zero is
   `NotSessionManagedClientID`; a session minted with it would silently carry no
   deduplication). Eight zero draws in a row return
   `*HostInitError` wrapping `ErrEntropyDegenerate`.
3. `PrepareForRegister()`: series becomes `SeriesIDForRegister`, `respondedTo`
   is cleared.
4. Propose `Entry{ClientID, SeriesID, RespondedTo, OwnerID}` with an empty
   `Cmd`, and wait for it to apply (or `ctx` to end: `ErrTimeout`,
   `ErrCanceled`).
5. Check that the applied result's `Value` is the client ID. Anything else means
   the apply path refused the registration, and the call returns `*SessionError`
   wrapping `ErrInvalidOperation`.
6. `PrepareForPropose()`: series becomes `SeriesIDFirstProposal` (1).

`RegisterSession(ctx, session)` does steps 1 and 3-6 for a caller-built session,
keeping its client ID. A zero client ID returns `*SessionError` wrapping
`ErrInvalidSession`.

A proposal that is already cancelled when submitted is refused before an entry
is built, so a session the caller abandoned is never registered or closed
behind its back.

### Proposals

`ProposeWithSession(ctx, session, cmd)` checks, in order: a context already
done (`ErrCanceled`/`ErrTimeout`), closed host, draining, nil session, shard, witness (`ErrInvalidOperation`), disk state
machine, `ValidForProposal` (`ErrInvalidSession`), disk full, sealed barrier
(`ErrBarrierSealed`), entry size, known leader. It then reserves the session's series and proposes
`Entry{Cmd, ClientID, SeriesID, RespondedTo}`.

After a successful result the caller calls `session.ProposalCompleted()`, which
advances the series with a CAS and raises `respondedTo` monotonically to the
completed series. `SyncProposeWithSession` does this itself. The next proposal
carries the new `respondedTo`, and the shard evicts every cached response at or
below it.

### One proposal per session

Every entry with the same `(clientID, seriesID)` is one command and its
retransmissions, so two concurrent proposals on one session would both be
reported successful with one applied. The session therefore holds an `inFlight`
reservation (the series number, 0 when idle):

- `beginCurrentProposal` reads the series, claims it with a CAS, then re-checks
  that the series has not moved. `ProposalCompleted` advances the series before
  releasing the reservation, so without the re-check a claim racing it could
  take the previous series and be answered as that command's retransmission.
  A failed claim returns `ErrSessionBusy` (`Temporary`).
- The reservation is released by `ProposalCompleted` on success, by the
  `RequestState` when it completes with an error (before the caller sees the
  error, so a retry on the same session is admitted), or by
  `RequestState.Release`. Releasing a series other than the reserved one is a
  no-op, so a late `Release` cannot clear the next proposal's reservation.
- Series 0 is the idle value, so `ValidForProposal` refuses a session at series
  0 (constructed and never registered). It also refuses the register and
  unregister sentinels, so a session whose registration did not complete, or
  that was closed, cannot propose.
- `Release` pins the `RequestState` before electing the releaser, so the
  binding is not recycled mid-release. Call `Release` exactly once: the object
  is pooled, and a second call is a use-after-free.

### Closure

`CloseSession(ctx, shardID, session)` refuses a disk state machine shard, a nil
session (`ErrInvalidOperation`) and a session failing `ValidForSessionOp`
(no-op or wrong shard: `ErrInvalidSession`). It then sets the series to
`SeriesIDForUnregister` and proposes, returning the `RequestState`. Every
replica removes the session, its cached responses and its owner slot.

## Sentinels and entry detection

```go
// package quicraft (pkg/session.go)
SeriesIDForRegister   uint64 = math.MaxUint64 - 1
SeriesIDForUnregister uint64 = math.MaxUint64
SeriesIDFirstProposal uint64 = 1

// package proto (pkg/proto/constants.go)
NotSessionManagedClientID    uint64 = 0
SessionSeriesIDForRegister   uint64 = 1<<64 - 2
SessionSeriesIDForUnregister uint64 = 1<<64 - 1
```

Normal proposals use series 1 to `MaxUint64 - 2`.

```go
func (e *Entry) IsSessionManaged() bool { return e.ClientID != NotSessionManagedClientID }

func (e *Entry) IsNewSessionRequest() bool {
	return !e.IsConfigChange() && len(e.Cmd) == 0 &&
		e.ClientID != NotSessionManagedClientID &&
		e.SeriesID == SessionSeriesIDForRegister
}
// IsEndOfSessionRequest is the same with SessionSeriesIDForUnregister.
```

## Routing in `Apply`

`rsm.StateMachine.Apply` checks contiguity (`ErrApplyGap`), adopts the shard's
replicated session policy, splits the batch at expiry checkpoints (see
[Deterministic expiry](#deterministic-expiry)) and passes each segment to
`applyBatch`, which classifies every entry in order:

```
config change or bootstrap membership  -> Result{}; skip
no session and empty Cmd (leader no-op) -> Result{}; skip

register   -> Register(clientID, OwnerID, index)
                ok:  Result{Value: clientID}; record lifecycle transition
                err: Result{}
              skip the user state machine
unregister -> Unregister(clientID)
                ok:  Result{Value: clientID}; record lifecycle transition
                err: Result{Value: ResultSessionExpired}
              skip

session-managed:
  CheckDuplicate(clientID, seriesID, index)
    error     -> ResultSessionExpired; skip
    duplicate -> owned copy of the cached result; skip
  same (clientID, seriesID) earlier in this batch -> mirror its result; skip
  CanRecord(clientID, RespondedTo, admittedThisBatch[clientID])
    full      -> ResultSessionExpired; skip
  admittedThisBatch[clientID]++; batchSeen[clientID][seriesID] = i

forward to the user state machine

after the single Update call:
  RecordResult for each admitted entry (Data deep-copied),
    unless its client has a lifecycle transition later in this batch
  copy each mirror's result from its first occurrence
```

Session register/unregister failures are per-entry results, never fatal: they
are expected on replay (a duplicate register after recovery, a double
unregister from a retry).

Three pieces of batch-scoped state carry the correctness of this loop:

- **`batchSeen` and `mirrors`: in-batch duplicates.** A retry whose original
  had not committed yet can commit into the same batch. Neither is in the
  session table yet, so `CheckDuplicate` reports both as new. The first
  occurrence is recorded in `batchSeen`; later ones become `mirrors` and copy its
  result without applying or recording again. This runs before the capacity
  gate, because a retry consumes no new cache slot and gating it could reject it
  as "cache full".
- **`admittedThisBatch`: projected capacity.** `RecordResult` runs only after
  the batched `Update`, so the gate counts new series already admitted earlier
  in the batch for the same client. Without it a batch of new series could
  overflow the cache together.
- **`sessionLifecycleAt`: transitions inside a batch.** It records the batch
  position of each client's last successful register or unregister. An entry
  admitted before a later transition for its client does not record a response:
  for a closed session the cache is moot, and for a re-registered client ID it
  would deduplicate a future proposal the new session never applied. A
  transition also clears that client's `batchSeen` and `admittedThisBatch`
  entries, since a new incarnation restarts its series.

### The admission gate preserves at-most-once

A new series that cannot be cached is answered `ResultSessionExpired` before the
user state machine sees it. Applying it without caching would let a later
replay find no cached response and apply it again. `CanRecord` is a read-only
projection of `RecordResult` (advance `respondedTo`, evict, count survivors) and
returns `ErrResponseLimitExceeded` when `surviving + pending >= maxResponses`.
It is driven by the entry's `RespondedTo` and the cache contents, never a clock,
so every replica reaches the same answer. The entry's own series is not an
input: a new response always takes a slot, and duplicates never reach the gate.

Because `CanRecord` already refused anything that cannot be cached,
`ErrResponseLimitExceeded` cannot come out of `RecordResult`. `ErrSessionNotFound`
from `RecordResult` is logged and ignored (a missing session is a property of
the log, and failing on it would fail the shard on every replica); any other
error is fatal.

### Copies

Cached results are deep-copied on the way into the cache and on the way out.
The client receives the slice the state machine returned; if the application
mutated it, an uncopied cache would corrupt deduplication and the next
snapshot. Under `ZeroCopyEntryCmd` the returned `Data` may also alias a buffer
the engine is about to recycle.

## Session manager

`pkg/internal/session/manager.go`. One per shard, used only from the apply
goroutine and snapshot paths, which the adapter serializes; it has no locks.

Per-session state:

| Field | Purpose |
|---|---|
| `clientID` | the key |
| `ownerID` | the declared owner, kept so removal returns the slot to the right owner |
| `registeredIndex` | log index of the register entry that created this incarnation |
| `respondedTo` | highest series the client acknowledged |
| `responses` | `seriesID -> sm.Result` cache |
| `lastActiveIndex` | log index of last activity, for expiry |

### Operations

**`Register(clientID, ownerID, index)`**: `ErrSessionAlreadyExists` if
registered; `ErrSessionLimitExceeded` if the shard is at `maxTotal` or the owner
is at `maxPerOwner`. Both caps report the same sentinel so the caller cannot
treat them differently, and a code path only one replica takes is a divergence.
Nothing is mutated unless both pass. `index` becomes `registeredIndex` and
`lastActiveIndex`.

**`CheckDuplicate(clientID, seriesID, index)`**:

- `ErrSessionNotFound` if the client is not registered.
- `ErrSessionNotFound` if `index < registeredIndex`: the entry belongs to an
  earlier incarnation of the same client ID (expired, then re-registered with a
  persisted ID). The new incarnation starts with an empty cache, so without this
  floor the stale entry would look new and apply twice.
- `ErrResponseEvicted` if `seriesID <= respondedTo`: already acknowledged and
  evicted.
- The cached result and `true` if present (`lastActiveIndex` advances).
- `false`, no error, if the series is new.

Both errors become `ResultSessionExpired`, per Figure 6.1 step 3.

**`CanRecord(clientID, respondedTo, pending)`**: see the admission gate above.

**`RecordResult(clientID, seriesID, respondedTo, index, result)`**: raises
`respondedTo`, evicts responses at or below it, advances `lastActiveIndex`
before the limit check (so an active session with a full cache is not idle to
expiry), then caches. `ErrResponseLimitExceeded` or `ErrSessionNotFound`.

**`Unregister(clientID)`**: removes the session and releases its owner slot;
`ErrSessionNotFound` if absent.

**`ExpireSessions(currentIndex, inactivityThreshold, maxExpire)`**: no-op if
`maxExpire <= 0` or the threshold is 0. A session is expired when
`currentIndex - lastActiveIndex > inactivityThreshold`. All candidates are
collected and sorted by client ID before the `maxExpire` cut, so every replica
removes the same subset. Each expired session's owner slot is released.

**`SetLimits(maxTotal, maxResponses, maxPerOwner)`**: used when a replica
learns the shard's replicated policy. Zero `maxTotal` or `maxResponses` is
ignored (no caller means "admit nothing"); `maxPerOwner` is always assigned,
since zero there means the cap is off. Existing sessions are kept; lowering
`maxTotal` below the current count only stops new registrations.

### Owners

`MaxTotalSessions` cannot defend itself against one principal: a client ID is
eight bytes the caller picks, so one client could register until the table is
full and every other tenant loses at-most-once delivery. `MaxSessionsPerOwner`
bounds concurrent sessions per declared owner, carried in the log as
`Entry.OwnerID` from `GetNewSessionWithOwner` or `NewSessionWithOwner`.

- It is a state machine decision. Every replica must reach the same verdict from
  the log alone, including on replay; the connection that carried the proposal
  is not in the log.
- It is only as trustworthy as the embedder's authentication. The library
  authenticates nodes, not clients.
- `ownerID == 0` is unset: never counted or capped.

`ownerCounts` (owner to live session count) is a derived index that makes
`Register` O(1). Every mutation of the session table moves it in step, and it is
not persisted: `UnmarshalFrom` rebuilds it from the decoded sessions, so a
restored count cannot disagree with the table.

### Errors

| Error | Where |
|---|---|
| `ErrSessionAlreadyExists` | `Register` on an active client ID |
| `ErrSessionNotFound` | unknown client, or an entry below `registeredIndex` |
| `ErrSessionLimitExceeded` | shard or owner cap; the public `quicraft.ErrTooManySessions` is an alias of `quicraft.ErrSessionLimitExceeded` |
| `ErrResponseLimitExceeded` | cache full after eviction; becomes `ResultSessionExpired` |
| `ErrResponseEvicted` | series already acknowledged and evicted; becomes `ResultSessionExpired` |
| `ErrSnapshotCorrupted` | sentinel matched by `*BoundsError` when a decoded count exceeds its limit |

These are internal; the public package has its own (`ErrSessionExpired`,
`*SessionExpiredError`, `*SessionError`, `ErrInvalidSession`, `ErrSessionBusy`,
`ErrSessionsUnsupportedOnDisk`) because the two packages cannot import each
other. On the host, `ResultSessionExpired` becomes `*SessionExpiredError` on the
`RequestState`.

## Deterministic expiry

Expiry is driven by log index, never wall-clock time. Every replica applies the
same entries in the same order, so an index rule expires the same sessions at
the same point on every replica. A clock rule would not, and from then on one
replica answers a proposal from its cache while another reports it expired: a
permanent divergence no Raft invariant detects.

Expiry runs at fixed checkpoints, the multiples of the expiry threshold in
`(lastApplied, lastIndex]`. For each, `Apply` first applies every entry at or
below it, then calls `ExpireSessions(checkpoint, threshold, maxExpirePerCheckpoint)`,
so expiry sees the table exactly as of that index. With expiry disabled the
batch goes through in one piece. `maxExpirePerCheckpoint` is
`MaxTotalSessions / 8` with a floor of 64.

The checkpoint walk continues across `Apply` calls and restarts only because
`lastApplied` is seeded on both resume paths (`Host.StartShard` and the engine's
snapshot recovery, through `SetLastApplied`). The counter is not in the
snapshot. A replica resuming with it at zero would re-walk checkpoints its peers
already passed and, since each walk is capped, delete sessions the shard still
holds. The `ErrApplyGap` check makes that state fail instead of diverge.

## Snapshots

The session table is written ahead of the user state machine's bytes in every
snapshot:

```
[numSessions:4]
  per session, sorted by clientID:
    [clientID:8][ownerID:8][registeredIndex:8][respondedTo:8][numResponses:4]
      per response, sorted by seriesID:
        [seriesID:8][value:8][dataLen:4][data]
    [lastActiveIndex:8]
```

Little-endian. Sorting makes identical tables produce identical bytes.
`ownerID` must survive so restored owner counts match the peers', and
`registeredIndex` so the incarnation floor survives a restart.

`UnmarshalFrom` bounds every count it decodes: `numSessions <= maxTotal`,
`numResponses <= maxResponses`, `dataLen <= 64 MiB` (`maxResponseDataSize`). A
violation returns `*BoundsError`, which matches `ErrSnapshotCorrupted`. It parses
into a staging map and swaps it in only after the whole table parses.

On recovery the adapter adopts the replicated policy first (the table was
written under the shard's limits, not this node's), decodes into a separate
staged manager, restores the user state machine, and adopts the staged table
only if that succeeds. Session metadata and user state therefore always
describe the same index. See
[`../state-machines/engineering.md`](../state-machines/engineering.md#snapshots).

A session absent from the snapshot does not exist on the recovered replica, and
its next proposal is answered `*SessionExpiredError`. That matches every other
replica, because the snapshot reflects the log.

## Configuration

`pkg/config/config.go`. All four are seeds for the replicated policy.

| Field | Default | Meaning |
|---|---|---|
| `MaxSessionsPerClient` | 128 | response-cache depth per session (`maxResponses`) |
| `MaxTotalSessions` | 16384 | sessions per shard |
| `MaxSessionsPerOwner` | 128 | sessions per declared owner; `config.SessionsPerOwnerUnlimited` disables the cap |
| `SessionExpiryEntries` | 100000 | inactivity threshold in applied entries; `config.SessionExpiryDisabled` (`math.MaxUint64`) disables expiry |

- **`MaxSessionsPerClient` does not limit sessions.** It is the number of cached
  results one session keeps. The name is kept because renaming a public field
  breaks callers, and it appears in `proto.SessionPolicy` too. Transposing it
  with `MaxTotalSessions` fails silently either way, which is why the adapter
  takes the policy as one struct.
- **Zero means "not chosen".** `SetDefaults` replaces zero with the default for
  all four fields. Disabling expiry or the owner cap takes the explicit
  constant.
- `Validate` rejects `SessionExpiryEntries` below 100, rejects a zero
  `MaxSessionsPerClient` or `MaxTotalSessions`, and warns when the threshold exceeds a non-zero
  `SnapshotEntries`. With expiry disabled, a client that dies without
  `CloseSession` holds its slot in `MaxTotalSessions`, and in every snapshot,
  forever.

### Replicated policy

A founding replica writes the four values into the shard's initial membership
(the session fields of `proto.Membership`, read as a `proto.SessionPolicy`
through `Membership.SessionPolicy()`), and every replica applies the
committed values from then on. That is what makes expiry deterministic in
practice: two replicas with different thresholds or caps would diverge with
every Raft invariant intact.

`SessionPolicy.IsSet()` uses `MaxTotalSessions` as the discriminator (always
non-zero once seeded), so `ExpiryEntries == 0` keeps its own meaning (expiry
disabled) instead of reading as "no policy".

- All founders must supply the same values, or their index-1 entries differ.
- A node whose configuration disagrees applies the shard's values and logs the
  disagreement at WARN; it does not refuse to start.
- `Host.GetShardMembership` reports the committed policy.
- There is no config change for the policy; it is fixed for the life of the
  shard.

## Concurrency

- `Session`: `seriesID`, `respondedTo` and `inFlight` are atomics, so a session
  can be read from any goroutine, but it serves one proposal at a time (see
  above). `ProposalCompleted` and `SetRespondedTo` only move `respondedTo`
  forward.
- `Manager`: not thread-safe; used only on the apply path, which processes
  entries sequentially.

## Test strategy

Targets: `make test-session`, `make fuzz-session`.

| Layer | Covers |
|---|---|
| Unit (`pkg/internal/session`) | registration, response cache, per-owner and per-shard caps, incarnations, deterministic expiry and marshal determinism |
| Unit (`pkg/session_test.go`, `pkg/host_ondisk_sessions_test.go`, `pkg/host_session_policy_test.go`) | the `Session` type, the on-disk refusal, the replicated policy |
| Unit (`pkg/internal/rsm`) | in-batch duplicates, the admission gate, lifecycle transitions within a batch |
| Fuzz (`make fuzz-session`) | marshal round trip, unmarshal of hostile input, sentinel series handling, `CheckDuplicate` consistency |
| Integration | duplicate proposals across a real cluster and across a leader change |

A test here must drive expiry with applied entries, never with sleeps: a
sleep-based test can pass while the real mechanism is broken.

Fault-injection seams: propose the same `(clientID, seriesID)` twice, in the
same batch and in different batches; restart a node mid-series; take a snapshot
between two halves of a series.

## Observability

No metrics of their own. The adapter logs at WARN when it answers a
session-managed proposal `ResultSessionExpired` and when a session disappears
mid-batch; a failed register or unregister is not logged.

| Signal | Source |
|---|---|
| refused registration | the `*SessionError` returned to the caller |
| expired session | `*SessionExpiredError` on the request's result |
| served duplicate | invisible by design; it looks like the original |

Never log a `ClientID` or `OwnerID` next to application data; they are the
embedder's identifiers.

## Cross-references

- [State machines](../state-machines/engineering.md): the adapter that drives `Apply`
- [Wire protocol](../wire-protocol/engineering.md): `Entry` session fields and `SessionPolicy`
- [Raft consensus](../raft-consensus/engineering.md): commit, apply and snapshot ordering
- [Compatibility](../compatibility/architecture.md): format pinning
- [Multi-group host](../multi-group-host/api.md): the `Host` session methods and proposal errors
