# Client Sessions — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/session.go`,
> `pkg/internal/session`, and the session fields of `pkg/proto` ·
> Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

## The problem

A client proposes a command, the connection drops, and the client retries. Did
the first proposal commit? The client cannot tell, and neither can the leader it
retries to, which may not be the same leader.

Without a mechanism, the honest guarantee is at-least-once, which for a
non-idempotent command means "your transfer may have happened twice".

## The shape chosen: replicated dedup keyed by `(clientID, seriesID)`

A session is registered through the log, its response cache lives in the state
machine layer, and both are replicated. Every replica holds the same
deduplication state, so a retry that lands on a different node after a leader
change is recognized there too.

```
  client                          log                   every replica
    │  GetNewSession ──▶ register entry ──────────▶ session created
    │
    │  ProposeWithSession(series N) ──▶ entry ──▶ seen N before?
    │                                             ├─ no  ──▶ Update, cache result
    │                                             └─ yes ──▶ cached result,
    │                                                         Update not called
    │
    │  next proposal carries RespondedTo = N, releasing N's cache entry
```

The retry gets the original result, so from the caller's side it is the same as
the first call.

A session carries one proposal at a time. Every entry with the same
`(clientID, seriesID)` is treated as a retransmission of one command, so two
concurrent proposals on one session would have one applied and both reported as
successful. The second concurrent call is refused with `ErrSessionBusy`.

### Rejected: dedup at the leader only

A leader-local table is lost on leader change and on restart, the two events
that cause retries. Replicating it costs one log entry per session and makes the
guarantee hold across those failures.

### Rejected: dedup by hashing the command

Content-addressed dedup cannot tell a retry from a genuine second identical
operation. "Increment by one" twice is a legitimate sequence; only the client's
series number knows which it was.

### Rejected: time-based session expiry

Expiry is measured in applied log entries, not wall-clock time. Expiry mutates
replicated state, so replicas with different clocks would expire a session at
different points in the log and diverge. `SessionExpiryEntries` counts applied
entries, so every replica decides at the same index.

Zero selects the default (100000); assign `config.SessionExpiryDisabled` to turn
expiry off.

### Rejected: sessions on disk state machines

Every session entry point refuses a `DiskStateMachine` shard with
`ErrSessionsUnsupportedOnDisk`. The session table travels only in the snapshot
stream and is restored by `RecoverFromSnapshot`. A disk state machine that
reports a non-zero applied index from `Open` skips that path on restart, because
recovering would roll its database back to the snapshot index. The replica would
come up with correct data and no sessions, then answer `SESSION_EXPIRED` for
entries its peers apply. The refusal reads only the shard's state machine type,
so every replica reaches it identically and nothing reaches the log.

On those shards, make commands idempotent and propose them without a session.

## Sentinel values

Session operations ride in `proto.Entry` rather than a separate message type, so
the whole lifecycle is ordered by the log:

| Constant | Value | Meaning |
|---|---|---|
| `NotSessionManagedClientID` | 0 | no session; the entry bypasses dedup |
| `SessionSeriesIDForRegister` | `MaxUint64 - 1` | register this session |
| `SessionSeriesIDForUnregister` | `MaxUint64` | unregister it |

`Entry.RespondedTo` carries the highest series the client has acknowledged,
which lets the cache release entries without a separate round trip.

## The limits

| Setting | Default | Bounds |
|---|---|---|
| `MaxSessionsPerClient` | 128 | cached responses retained per session. The name is wrong and kept for compatibility; read it as "max responses per session". It does not bound how many sessions exist. |
| `MaxTotalSessions` | 16384 | sessions on a shard |
| `MaxSessionsPerOwner` | 128 | concurrent sessions one declared `OwnerID` may hold; sessions without an owner are exempt |

`OwnerID` names the principal that owns a session: the embedder's tenant,
account or authenticated subject. It rides in the replicated entry, not as a
leader-local fact, because the cap is a state machine decision: every replica,
including one replaying after a restart, must accept or refuse a registration
from the log alone.

`OwnerID` is not a security boundary on its own. It is whatever the embedder
supplies, so the cap is only as trustworthy as the embedder's authentication.
Zero means unset, and an unset owner is never counted or capped.

## Replicated policy, not per-node configuration

The session policy (expiry threshold and the three limits) is written into the
shard's initial membership at bootstrap and replicated through the log,
snapshots and the WAL. `Config`'s session fields are only the bootstrap seed. A
per-node value would let replicas disagree about which registrations are legal,
the same class of bug as time-based expiry. The policy cannot be changed after
bootstrap.

## Trust boundary

- A `ClientID` is eight bytes. `GetNewSession` draws it from the system CSPRNG;
  `RegisterSession` uses one the caller chose. The library performs no client
  authentication. A second registration of a `ClientID` the shard still holds
  is refused, but once that session expires anyone can register the ID again,
  and a caller still holding the old session then proposes into the new one:
  two callers using the same `ClientID` on a shard share its deduplication
  state.
- Authentication and tenancy belong to the layers above. Sessions are a
  correctness mechanism, not an authorization one.
- The response cache holds application result data and is snapshotted and
  shipped to followers like any other state.
- `ClientID` and `OwnerID` are the embedder's identifiers. Do not log them next
  to application data.

## Component boundaries

- **`pkg/session.go`**: the public `Session` type and its constructors.
- **`pkg/internal/session`**: the session manager: registration, response
  cache, limits, deterministic expiry. Internal.
- **`pkg/internal/rsm`**: routes session entries before the user state machine
  sees them.
- **`pkg/proto`**: the `Entry` session fields (`ClientID`, `SeriesID`,
  `RespondedTo`, `OwnerID`) and the session policy fields of `Membership`,
  read as a `SessionPolicy` through `Membership.SessionPolicy()`.
- **Consumers**: `Host.GetNewSession`, `GetNewSessionWithOwner`,
  `RegisterSession`, `ProposeWithSession`, `SyncProposeWithSession`,
  `CloseSession`, `GetNoOPSession`.

## What this feature deliberately does not do

- No exactly-once. The guarantee is at-most-once: a proposal is applied zero or
  one times. Whether it was applied is what the result tells you.
- No client authentication, identity or tenancy.
- No cross-shard sessions.
- No time-based expiry.
- No sessions on disk state machines.
- No pipelining. One proposal per session at a time.
- No transactions. A series orders one client's proposals; it does not group
  them.
