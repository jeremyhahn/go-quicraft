# Client Sessions — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/session.go` and the `Host`
> session methods · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Use sessions when a proposal is **not idempotent** and a retry after a dropped
connection would be wrong. Skip them when it is.

## Before you start

Answer one question first: **is this command idempotent?**

- "Set balance to 100" — idempotent. Use `GetNoOPSession`; a retry is harmless
  and you save a log entry, a cache slot, and a round trip.
- "Add 100 to balance" — not idempotent. Use a session.

The guarantee sessions give is **at-most-once**: the command is applied zero or
one times, never twice. It is not exactly-once — whether it was applied at all
is what `RequestState` tells you.

Sessions are **not available on `DiskStateMachine` shards**
(`ErrSessionsUnsupportedOnDisk`). A disk state machine's own idempotent-replay
contract subsumes what sessions provide.

## Task: propose with a session

```go
session, err := host.GetNewSession(ctx, shardID)
if err != nil {
    return err
}
defer host.CloseSession(ctx, shardID, session)

result, err := host.SyncProposeWithSession(ctx, session, cmd)
```

`GetNewSession` registers through the log, so it is a consensus operation and
can fail like any proposal. Hold the session for the life of the client's
conversation with the shard — one per connection or per logical client, not one
per proposal.

## Task: retry safely

Just retry with the **same session**:

```go
result, err := host.SyncProposeWithSession(ctx, session, cmd)
if isRetryable(err) {
    result, err = host.SyncProposeWithSession(ctx, session, cmd)  // same session
}
```

If the first attempt committed, the second returns the **cached result** and the
state machine is never called again. If it did not, the second applies normally.
Either way the command lands at most once — including when the retry reaches a
different node after a leader change, because the session and its cache are
replicated.

**Do not create a new session to retry.** A new session is a new
`(clientID, seriesID)` space and dedup will not recognize the retry.

## Task: propose without one

```go
session := host.GetNoOPSession(shardID)
result, err := host.SyncProposeWithSession(ctx, session, cmd)
```

Or use `Propose` / `SyncPropose` directly. Both bypass dedup entirely and touch
no log entry for registration.

## Task: attach an owner

```go
session, err := host.GetNewSessionWithOwner(ctx, shardID, tenantID)
```

`OwnerID` groups sessions by the embedder's principal — a tenant, account, or
connection identity — so `MaxSessionsPerOwner` (128) can stop one of them
exhausting the shard's 16384.

Two things to be clear about:

- **`OwnerID` is not authentication.** The value is whatever your code passes,
  so the cap is only as trustworthy as the authentication you did before
  calling. This library has no notion of who a client is.
- **Zero means unset**, and unset owners are never counted or capped.

## Task: acknowledge results so the cache drains

```go
session.SetRespondedTo(seriesID)
```

`RespondedTo` tells the shard the client has the result for that series, which
releases its cache entry. Without it, a long-lived session accumulates cached
responses up to `MaxSessionsPerClient` (128) — which, despite the name, is the
**cached responses per session** and not a session count.

## Task: size the limits

All four are per-shard bootstrap seed on `config.Config`, and the live policy is
replicated thereafter:

| Setting | Default | Raise it when |
|---|---|---|
| `MaxTotalSessions` | 16384 | a busy shard refuses registrations |
| `MaxSessionsPerOwner` | 128 | one tenant legitimately needs more concurrent connections |
| `MaxSessionsPerClient` | 128 | clients pipeline more than 128 unacknowledged proposals |
| `SessionExpiryEntries` | 100000 | sessions expire under normal traffic |

**`SessionExpiryEntries` counts applied log entries, not seconds.** A busy shard
expires idle sessions quickly; an idle shard never expires anything. That is
deliberate — a wall clock would make two replicas expire at different points in
the log and diverge.

Zero selects the default rather than disabling. Assign
`config.SessionExpiryDisabled` to turn expiry off.

## Troubleshooting

**`ErrSessionsUnsupportedOnDisk`** — this shard runs a `DiskStateMachine`. Use
idempotent commands and `GetNoOPSession`; your state machine's own idempotent
replay is the equivalent guarantee.

**`*SessionExpiredError`** — the session went `SessionExpiryEntries` applied
entries without activity. Register a new one and re-propose. If it happens
under normal traffic, the threshold is too low for how busy the shard is.
Remember it is an entry count: a shard doing 100k entries/minute expires a
session after a minute of that client's silence.

**Sessions accumulate and registrations start failing** — clients are not
closing them, or not calling `SetRespondedTo`. Check `MaxTotalSessions` first
and `MaxSessionsPerOwner` second; the second is the one that fires when a single
tenant misbehaves.

**A retry applied twice** — you created a new session for the retry, or you are
using `Propose` rather than `ProposeWithSession`, or the two attempts went to
different shards. Sessions are per-shard.

**A retry returned a stale result** — that is the mechanism working. The cached
result *is* the result of that series. If you meant a new operation, advance the
series by making a new proposal rather than retrying the old one.

**Two clients see each other's results** — they chose the same `ClientID` on the
same shard. A `ClientID` is eight bytes **you** pick, and this library does not
allocate or authenticate them. Derive it from something your layer guarantees
unique.

**Sessions never expire on a quiet cluster** — correct, and by design. Expiry is
entry-counted. Close sessions explicitly rather than relying on expiry.

**Registration is slow** — it is a Raft proposal. Do it once per client, not
once per operation.
