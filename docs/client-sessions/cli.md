# Client Sessions — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/session.go`,
> `pkg/internal/session` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable.

This feature contributes **no** `CommandDefinition` to any integration parity
suite, and there is no verb above it either. A session belongs to a *connection*
between an application and a shard; an operator has nobody to open one on behalf
of, and a session opened from a shell would be closed the moment the shell
exited.

## What an operator can actually act on

Three settings, all of them replicated shard state rather than commands. They
are set as bootstrap seed on `config.Config` and changed thereafter through the
membership path, because a per-node value would let replicas disagree about
which registrations are legal:

| Setting | Default | Watch for |
|---|---|---|
| `MaxTotalSessions` | 16384 | registrations refused on a busy shard |
| `MaxSessionsPerOwner` | 128 | one tenant exhausting its own quota |
| `MaxSessionsPerClient` | 128 | **cached responses per session**, not a session count. The name is wrong and kept for compatibility. |
| `SessionExpiryEntries` | 100000 | measured in **applied entries**, not seconds. An idle shard never expires anything. |

The composing layer above owns whatever operator surface exposes these; those
verbs are documented in that repository.

## Build-time targets

| Target | Runs |
|---|---|
| `make test-session` | `go test ./pkg/internal/session/...` |
| `make fuzz-session` | session-bearing entry decode |

## The counter-intuitive one

There is no "list sessions" or "expire sessions" command anywhere in the stack,
and adding one at this layer would be a defect. Sessions are replicated state
machine content: enumerating them is a read of the state machine, and expiring
one out of band would mutate replicated state without going through the log,
which is exactly the divergence the entry-counted expiry design exists to
prevent.
