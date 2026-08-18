# Raft Consensus — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/raft`,
> `pkg/internal/engine` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package, and its
packages are `internal/`.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable. `pkg/internal/raft`
and `pkg/internal/engine` are additionally not importable from outside the
module — an embedder drives consensus through `pkg.Host`, documented in
[`../multi-group-host/`](../multi-group-host/cli.md).

This feature contributes **no** `CommandDefinition` to any integration parity
suite. There is no verb for "run consensus"; the closest operator-facing
operations belong to other feature folders:

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
| `make integration-test-linearizability` | the linearizability checker — the only test that can falsify the whole feature |
| `make ci-netfault` | partitions, delays and drops |

## Operational levers that are not verbs

Every timing value is a **multiple of `RTTMillisecond`** rather than an absolute
duration, so one number retunes a cluster for its network:

| Lever | Default | Raise when |
|---|---|---|
| `RTTMillisecond` | 200 | the network is genuinely slower; this rescales everything below |
| `ElectionRTT` | 10 | spurious elections under load |
| `HeartbeatRTT` | 1 | rarely — lowering costs bandwidth, raising costs detection time |
| `CheckQuorum` | enabled | leave it |
| `PreVote` | enabled | leave it |
| `LeaseRead` | off | reads dominate and you accept the coupling; **requires `CheckQuorum` and `ElectionRTT >= 5`** |
| `NumWorkers`, `MaxApplyWorkers` | `NumCPU()` | apply is CPU-bound and starving the pipeline |
| `MaxTotalInMemLogSize` | 0 = disabled | **set this in production**; without it one client can grow the in-memory log until the process dies |
| `Quiesce` | off | thousands of mostly-idle shards |

`CheckQuorum`, `PreVote` and `OrderedConfigChange` are `*bool`, so their zero
value means "unset, use the default" rather than "off". Use
`config.BoolPtr(false)` to disable one deliberately.

## Diagnostics, for when a cluster is misbehaving

Four `Host` accessors that exist for incident response rather than monitoring.
Worth knowing they are there before you need them:

```go
host.DiagRaftState(shardID)      // term, committed, processed, lastIdx, msgs, state
host.DiagPipelineState(shardID)  // commitPending, queue depths, pending proposals
host.DiagShardGates(shardID)     // recovering, saving, applyInFlight
host.DiagRemotes(shardID)        // per-remote replication state
```

`DiagRaftState` and `DiagRemotes` together answer the two questions that matter
during an incident: what does this replica think the term and commit index are,
and which follower is it stuck on.
