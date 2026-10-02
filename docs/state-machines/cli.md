# State Machines — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/sm`, `pkg/sm/kv` ·
> Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

**No CLI surface.** go-quicraft is a library and ships no binaries (there is no
`cmd/` directory). This feature is an interface you implement and the library
calls: `Update` is driven by consensus, `SaveSnapshot` by the snapshot cadence,
`RecoverFromSnapshot` by recovery. There is no operator verb to expose, so the
feature contributes no `CommandDefinition` to any parity suite.

Operations that look like they belong here are the embedding application's:

| Operation | Where |
|---|---|
| read and write application data | the application that implements the state machine |
| trigger a snapshot, inspect the applied index | [`../snapshots/cli.md`](../snapshots/cli.md), [`../multi-group-host/cli.md`](../multi-group-host/cli.md) |
| compare state across replicas | `Host.StateMachineHash`, a Go call |

## Build-time targets

| Target | Runs |
|---|---|
| `make test-sm` | the `pkg/sm` and `pkg/sm/kv` unit tests |
| `make test-rsm` | the adapter's unit tests |
| `make integration-test-linearizability` | the linearizability checker over a real cluster |

## Worth wiring into operations

```go
hash, appliedIndex, ok, err := host.StateMachineHash(shardID)
```

Compare across replicas at the same `appliedIndex`. A mismatch means a
non-deterministic `Update`, which has no other symptom until a restart replays
the log differently. `ok` is false when your state machine does not implement
`sm.Hasher`; with a wrong method signature you get `ok == false`, not a compile
error.
