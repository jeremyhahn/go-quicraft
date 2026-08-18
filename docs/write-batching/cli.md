# Write Batching — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/batch`, `pkg/writemode` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable.

This feature contributes **no** `CommandDefinition` to any integration parity
suite, and it is the feature least likely to want one: the aggregator is an
in-process component that sits *above* the `Host`, holding un-flushed commands
in memory on behalf of the calling application. There is nothing for an operator
to invoke, and a batch that outlived the process it was submitted to would be a
different feature entirely.

## Build-time targets

| Target | Runs |
|---|---|
| `make test-batch` | `go test ./pkg/batch/...` |
| `make bench-batch` | the aggregator benchmarks |
| `make test-writemode` | `go test ./pkg/writemode/...` |
| `make bench-writemode` | |

## Operational levers that are not verbs

All `batch.Config`, set by the embedding application:

| Lever | Default | What moving it costs |
|---|---|---|
| `FlushInterval` | 50 ms | **this is the latency you are trading for throughput.** Raising it batches more and adds up to that much delay to every write. |
| `MaxBatchSize` | 1000 | a flush rate pinned at the interval means this is too high for the load |
| `MaxBatchBytes` | 8 MB (`proto.MaxEntrySize`) | the ceiling one Raft entry can carry |
| `QueueSize` | 10000 | past it, `Submit` returns `*QueueFullError` — back-pressure rather than a silent drop |
| `ProposeTimeout` | 30 s | a **ceiling**, never applied in a way that cuts a caller short of its own deadline |

## What to watch

Supply a Prometheus registry and the aggregator registers its own collectors:

```go
batch.NewAggregator(proposer, batch.WithRegistry(reg))
```

This is the only package in the repository that registers collectors directly
rather than through the host's `MetricsCollector`, because it sits above the
`Host` rather than inside it.

Two signals:

1. **Flush rate against `FlushInterval`.** Flushes arriving at the interval mean
   batches are timing out rather than filling — `MaxBatchSize` is too high for
   the offered load, and every write is paying the full interval in latency for
   a batch that is not full.
2. **Queue depth against `QueueSize`.** At the ceiling, `Submit` starts
   returning `*QueueFullError`.

`BatchLen()` gives the current unflushed count for ad-hoc inspection.

## The one thing an operator must be told

**Delivery through the aggregator is at-least-once.** A batch may be proposed
more than once. That is what batching costs, and it is not a setting — a
deployment that cannot tolerate replay either makes its commands idempotent or
does not use the aggregator.
