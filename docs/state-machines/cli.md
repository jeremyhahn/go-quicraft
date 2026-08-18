# State Machines — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/sm`, `pkg/sm/kv` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable.

The absence is structural here rather than incidental. This feature is an
**inward-facing** interface: you implement it and the library calls you. There
is no verb to expose, because there is no operation this repository performs on
a state machine that an operator initiates — `Update` is driven by consensus,
`SaveSnapshot` by the snapshot cadence, `RecoverFromSnapshot` by recovery.

This feature therefore contributes **no** `CommandDefinition` to any integration
parity suite.

## Where the operator verbs live

The commands that *look* like they belong here are the composing layer's, and
they operate on the data your state machine holds rather than on the state
machine itself:

| Verb-shaped operation | Owner |
|---|---|
| read and write application data | the database layer above (go-qrdb), whose own state machine this is |
| trigger a snapshot, inspect applied index | [`../snapshots/cli.md`](../snapshots/cli.md) and [`../multi-group-host/cli.md`](../multi-group-host/cli.md) |
| compare state across replicas | `Host.StateMachineHash`, a Go call — see below |

## Build-time targets

| Target | Runs |
|---|---|
| `make test-sm` | `go test ./pkg/sm/...` |
| `make test-rsm` | the adapter's suite |
| `make integration-test-linearizability` | the linearizability checker over a real cluster — the only thing that establishes your state machine's observable behavior is actually linearizable |

## The one thing worth wiring into operations

Not a command, one call, and the only detector for the failure mode this feature
is most exposed to:

```go
hash, appliedIndex, ok, err := host.StateMachineHash(shardID)
```

Compare across replicas at the same `appliedIndex`. A mismatch means a
non-deterministic `Update`, which has **no other symptom** until a restart
replays the log differently — by which time the divergent state has been
snapshotted and shipped to followers.

`ok` is false when your state machine does not implement `sm.Hasher`. Detection
is by type assertion, so a wrong method signature gives you `ok == false` rather
than a compile error.
