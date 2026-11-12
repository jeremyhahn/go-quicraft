# Client Sessions — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/session.go` and the `Host`
> session methods · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

Use a session when a proposal is not idempotent and applying it twice after a
retry would be wrong. Skip it when the command is idempotent.

## Before you start

Decide whether the command is idempotent:

- "Set balance to 100" is idempotent. Propose it with `Propose` or `SyncPropose`;
  a retry is harmless and you save the registration and the cache slot.
- "Add 100 to balance" is not. Use a session.

Sessions give at-most-once: the command is applied zero or one times. They do
not give exactly-once; the result tells you whether it was applied.

Sessions are not available on shards whose state machine is a
`DiskStateMachine` (`ErrSessionsUnsupportedOnDisk`). Make commands idempotent
there.

## Propose with a session

```go
session, err := host.NewSession(ctx, shardID)
if err != nil {
	return err
}
defer func() {
	if rs, err := host.CloseSession(ctx, shardID, session); err == nil {
		rs.Release()
	}
}()

result, err := host.SyncProposeWithSession(ctx, session, cmd)
```

`NewSession` registers through the log, so it can fail like any proposal.
Keep one session per client or connection for its whole conversation with the
shard, not one per proposal.

A session carries one proposal at a time. A second concurrent call on the same
session returns `ErrSessionBusy`. For parallel proposals, open one session per
concurrent worker.

## Use the async form

```go
rs, err := host.ProposeWithSession(ctx, session, cmd)
if err != nil {
	return err
}
defer rs.Release()

r := <-rs.ResultC()
if r.Err != nil {
	return r.Err // the session is unchanged; retry with it
}
session.ProposalCompleted() // advance to the next series
```

`SyncProposeWithSession` calls `ProposalCompleted` for you. With the async form
you must call it after each successful result, or the next proposal reuses the
same series and gets the previous command's cached result.

## Retry safely

Retry with the same session:

```go
result, err := host.SyncProposeWithSession(ctx, session, cmd)
if err != nil && quicraft.Temporary(err) {
	result, err = host.SyncProposeWithSession(ctx, session, cmd) // same session
}
```

A failure after the proposal reached Raft (a timeout, a leadership change, a
stopped or halted shard) reports `quicraft.MaybeApplied(err) == true`: the
command may still commit. Without a session that is a hazard, because a retry
can apply it twice. With a session it is the case the session exists for, so
retry as above; check `MaybeApplied` only to learn that the first attempt may
have landed. `Temporary` still says whether a retry can succeed: a halted or
closed shard is `MaybeApplied` but not `Temporary`, and no retry helps until it
is reloaded.

If the first attempt was applied, the retry returns the cached result and the
state machine is not called again. If it was not, the retry applies normally.
Either way the command lands at most once, including when the retry reaches a
different node after a leader change.

Do not create a new session to retry: a new session is a new
`(clientID, seriesID)` space and the retry will not be recognized.

## Propose without a session

```go
result, err := host.SyncPropose(ctx, shardID, cmd)
```

`Propose` and `SyncPropose` bypass deduplication. `NoOPSession` returns a
no-op session for code that needs a session value, but `ProposeWithSession`
rejects it with `ErrInvalidSession`.

## Attach an owner

```go
session, err := host.NewSessionWithOwner(ctx, shardID, tenantID)
```

The owner groups sessions by your principal (tenant, account or authenticated
subject) so `MaxSessionsPerOwner` (default 128) stops one of them exhausting
the shard's `MaxTotalSessions` (default 16384).

- The owner is not authentication. It is whatever your code passes, so derive
  it from something you verified, not from the client.
- Zero means unset, and unset owners are never counted or capped.

## Choose the client ID yourself

`NewSession` picks a random client ID. To use one you persist (for example,
to re-register after your client restarts and its old session has expired),
register it explicitly:

```go
session, err := host.RegisterSession(ctx, quicraft.NewSession(shardID, savedClientID))
```

Registration fails with a `*SessionError` while the shard still holds that
client ID. Re-registering never re-applies what an earlier session under the
same ID committed, and while no session holds the ID, any entry sent under it
is answered as expired. Once the ID is registered again, an entry is a
proposal of the new session: the log records no session generation, so an
old session's proposal resent then is applied as a new one. Before you
re-register an ID, drop every unacknowledged proposal from its earlier session
and treat its outcome as unknown; resend the command only on the new session,
where it is applied as a new command.

## Size the limits

All four are bootstrap seeds on `config.Config`, replicated when the shard is
founded and fixed after that. Every founding replica must supply the same
values.

| Setting | Default | Raise it when |
|---|---|---|
| `MaxTotalSessions` | 16384 | a busy shard refuses registrations |
| `MaxSessionsPerOwner` | 128 | one owner legitimately needs more concurrent sessions |
| `MaxResponsesPerSession` | 128 | rarely: it is the response-cache depth per session, not a session count |
| `SessionExpiryEntries` | 100000 | sessions expire under normal traffic |

`SessionExpiryEntries` counts applied log entries, not seconds. A busy shard
expires idle sessions quickly; an idle shard never expires anything. Zero
selects the default; assign `config.SessionExpiryDisabled` to turn expiry off,
and then close every session yourself.

## Troubleshooting

**`ErrSessionsUnsupportedOnDisk`.** The shard runs a `DiskStateMachine`. Use
idempotent commands with `Propose`/`SyncPropose`.

**`ErrSessionBusy`.** Another proposal is in flight on this session. Wait for it,
or use a separate session per concurrent worker.

**`ErrInvalidSession`.** The session is a no-op session, belongs to another
shard, was never registered, or was closed.

**`*SessionExpiredError`.** The shard has no record of this session: it went
`SessionExpiryEntries` applied entries without activity, was closed, or was
replaced by a re-registration. Register a new session and propose again. If it
happens under normal traffic, the threshold is too low for the shard's rate: at
100k entries a minute, a session expires after a minute of silence.

**Registrations fail with a `*SessionError`.** The shard is at
`MaxTotalSessions` or the owner is at `MaxSessionsPerOwner`. Clients are probably
not closing sessions. Check the per-owner cap first if one owner is misbehaving.

**A retry was applied twice.** You retried with a new session, used `Propose`
instead of `ProposeWithSession`, or retried against a different shard. Sessions
are per shard.

**A proposal returned the previous command's result.** You used
`ProposeWithSession` and did not call `ProposalCompleted` after the previous
result, so the new command reused its series.

**Two clients see each other's results.** They registered the same client ID
with `RegisterSession`. Use `NewSession`, or derive IDs from something your
layer guarantees unique.

**Sessions never expire on a quiet shard.** By design: expiry counts entries.
Close sessions explicitly.

**Registration is slow.** It is a Raft proposal. Do it once per client.
