# Client Sessions — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/session.go`,
> `pkg/internal/session` · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

**No CLI surface.** go-quicraft is a library and ships no binaries (there is no
`cmd/` directory). A session belongs to an application's connection to a shard,
so there is nothing for an operator to open, and the feature contributes no
`CommandDefinition` to any parity suite.

There is deliberately no "list sessions" or "expire sessions" command either.
Sessions are replicated state: expiring one out of band would change replicated
state without going through the log, which is the divergence entry-counted
expiry exists to prevent.

## Settings

Bootstrap seeds on `config.Config`, written into the shard's replicated policy
when it is founded and fixed after that:

| Setting | Default | Watch for |
|---|---|---|
| `MaxTotalSessions` | 16384 | registrations refused on a busy shard |
| `MaxSessionsPerOwner` | 128 | one owner exhausting its quota |
| `MaxSessionsPerClient` | 128 | cached responses per session, not a session count |
| `SessionExpiryEntries` | 100000 | counted in applied entries, not seconds; an idle shard expires nothing |

## Build-time targets

| Target | Runs |
|---|---|
| `make test-session` | the `pkg/internal/session` unit tests |
| `make fuzz-session` | fuzzing of the session manager: snapshot round trip and decode, sentinel series handling, duplicate detection |
