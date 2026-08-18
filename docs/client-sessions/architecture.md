# Client Sessions — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/session.go`,
> `pkg/internal/session`, and the session fields of `pkg/proto` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## The problem

A client proposes a command, the connection drops, and the client retries. Did
the first proposal commit? The client cannot tell, and neither can the leader it
retried to — it may not even be the same leader.

Without a mechanism, the honest guarantee a Raft library can offer is
*at-least-once*, which for any non-idempotent command means "your transfer may
have happened twice".

## The shape chosen: replicated dedup keyed by `(clientID, seriesID)`

A session is registered **through the log**, its response cache lives **in the
state machine**, and both are replicated. Every replica therefore holds
identical deduplication state, and a retry that lands on a different node after
a leader change is recognized there too.

```
  client                     leader                 every replica
    │  GetNewSession ──▶ EntryNormal (register) ──▶ session created
    │                        (through Raft)         identically
    │
    │  ProposeWithSession(seriesID=N) ──▶ log ──▶  seen N before?
    │                                              ├─ no  ──▶ SM.Update, cache result
    │                                              └─ yes ──▶ return CACHED result,
    │                                                          SM never called
    │
    │  RespondedTo=N acknowledges N, releasing its cache entry
```

The retry is served the **original** result. From the caller's side it is
indistinguishable from the first call, which is the point.

### Rejected: dedup at the leader only

A leader-local table is lost on leader change and on restart — exactly the two
events that cause the retry in the first place. Replicating it costs one log
entry per session and makes the guarantee hold across the failures that matter.

### Rejected: dedup by hashing the command

Content-addressed dedup cannot distinguish a retry from a genuine second
identical operation. "Increment by one" twice is a legitimate sequence; the
client's own series number is the only thing that knows which it was.

### Rejected: time-based session expiry

**Expiry is measured in applied log entries, not wall-clock time.** That is the
single most important design decision here, because expiry mutates replicated
state: two replicas whose clocks differ would expire a session at different
points in the log and diverge. `SessionExpiryEntries` is a count of applied
entries, so every replica reaches the same decision at the same index.

Zero selects the default (100000) rather than disabling; assign
`config.SessionExpiryDisabled` to turn it off.

### Rejected: sessions on disk state machines

`Host.GetNewSession` on a `DiskStateMachine` shard returns
`ErrSessionsUnsupportedOnDisk`. A disk state machine owns its own durability and
replays already-applied entries idempotently after a crash; the library's
notion of "applied" and the machine's are independent, so a replicated response
cache would be authoritative about results the store might have written
differently. Idempotent replay is the tier-2 machine's own contract, and it
subsumes what sessions provide.

## Sentinel values

Three `SeriesID` values are reserved, and they ride in `proto.Entry` rather than
in a separate message type so the whole lifecycle is ordered by the same log:

| Constant | Value | Meaning |
|---|---|---|
| `NotSessionManagedClientID` | 0 | no session; the proposal bypasses dedup entirely |
| `SessionSeriesIDForRegister` | `MaxUint64 - 1` | register this session |
| `SessionSeriesIDForUnregister` | `MaxUint64` | unregister it |

`Entry.RespondedTo` carries the highest series the client has acknowledged,
which is what lets the cache release entries without a separate round trip.

## The caps, and what each one bounds

Three limits with confusingly similar names, so they are laid out together:

| Setting | Default | Bounds |
|---|---|---|
| `MaxSessionsPerClient` | 128 | **cached responses retained per session.** The name is wrong. It is kept only because renaming a public field breaks compatibility; read it as `MaxResponsesPerSession`. **It bounds nothing about how many sessions exist.** |
| `MaxTotalSessions` | 16384 | total sessions on a shard |
| `MaxSessionsPerOwner` | 128 | concurrent sessions one declared `OwnerID` may hold. Sessions registered without an owner are exempt. |

`OwnerID` names the principal that owns a session — the embedder's tenant,
account or connection identity. It rides in the **replicated entry** rather than
being derived at the leader because the cap is a state machine decision: every
replica has to accept or refuse a registration identically from the log alone,
and a leader-local fact is neither in the log nor available to a replica
replaying after a restart.

**`OwnerID` is not a security boundary on its own.** The value is whatever the
embedder claims, so the cap it drives is only as trustworthy as the embedder's
own authentication. Zero means unset, and an unset owner is never counted or
capped.

## Replicated policy, not per-node configuration

The session policy travels in `proto.Membership` through the log, snapshots and
the WAL. `Config`'s session fields are only the **bootstrap seed**. A per-node
policy value would let replicas disagree about which registrations are legal,
which is the same class of bug as time-based expiry.

## Trust boundary

- A `ClientID` is eight bytes **the caller chooses**. This library has no notion
  of who a client is and performs no authentication; two clients that pick the
  same `ClientID` on one shard share a session and will see each other's cached
  results.
- Authentication and tenancy belong to the layers above. What this feature
  provides is a *correctness* mechanism, not an *authorization* one.
- The response cache holds application result data in the state machine, and is
  therefore snapshotted and shipped to followers like any other state.
- Neither `ClientID` nor `OwnerID` should be logged next to application data;
  they are the embedder's identifiers and this library makes no promise about
  what they contain.

## Component boundaries

- **`pkg/session.go`** — the public `Session` type and its constructors.
- **`pkg/internal/session`** — the session manager: registration, the response
  cache, the caps, deterministic expiry. Internal.
- **`pkg/internal/rsm`** — routes session-bearing entries before the user state
  machine sees them.
- **`pkg/proto`** — the `Entry` session fields (`ClientID`, `SeriesID`,
  `RespondedTo`, `OwnerID`) and the replicated session policy in `Membership`.
- **Consumers**: `Host.GetNewSession`, `ProposeWithSession`, `SyncProposeWithSession`,
  `RegisterSession`, `CloseSession`, `GetNoOPSession`.

This feature is part of the go-quicraft capability row covering Raft consensus
and log replication in `docs/00 §R`.

## What this feature deliberately does not do

- **No exactly-once.** The guarantee is **at-most-once**: a proposal is applied
  zero or one times, never twice. Whether it was applied at all is what
  `RequestState` tells you.
- **No client authentication, no identity, no tenancy.** `ClientID` and
  `OwnerID` are caller-supplied.
- **No cross-shard sessions.** A session belongs to one shard.
- **No time-based expiry, ever.**
- **No sessions on disk state machines.**
- **No transaction semantics.** A series orders one client's proposals; it does
  not group them.
