# Raft consensus: CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/raft`,
> `pkg/internal/engine` · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: go-quicraft is a library and ships no binaries, and this
feature's packages are `internal/`.** There is no `cmd/` directory, and
`make build` runs `go build ./...` without producing an executable. An embedder
drives consensus through `pkg.Host`; see [`api.md`](api.md).

This feature contributes no `CommandDefinition` to any integration parity
suite. The nearest operator-facing operations belong to other feature folders:

| Operation | Folder |
|---|---|
| start / stop a shard, propose, read | [`../multi-group-host/`](../multi-group-host/cli.md) |
| add / remove a replica, transfer leadership | [`../membership-changes/`](../membership-changes/cli.md) |
| snapshot, compact | [`../snapshots/`](../snapshots/cli.md) |

## Build-time targets

| Target | Runs |
|---|---|
| `make test-raft` | the protocol suite |
| `make test-engine` | the worker pipeline |
| `make test-queue`, `make test-invariant`, `make test-server` | the supporting internals |
| `make bench-raft`, `make bench-engine` | protocol and pipeline benchmarks |
| `make fuzz-raft` | message handling against arbitrary inputs |
| `make integration-test-raft` | a real multi-node cluster |
| `make integration-test-linearizability` | the linearizability checker, the only test that can falsify the whole feature |
| `make ci-netfault` | partitions, delays and drops |

Timing, rate-limit and worker settings are configuration, not verbs; they are
listed in [`engineering.md`](engineering.md#configuration) and explained as
tasks in [`user.md`](user.md).
