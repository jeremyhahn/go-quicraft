# Write Batching — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/batch`, `pkg/writemode` ·
> Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

**No CLI surface.** go-quicraft is a library and ships no binaries (there is no
`cmd/` directory). The aggregator is an in-process component above the `Host`
that holds un-flushed commands in memory for the calling application, so there
is nothing for an operator to invoke and the feature contributes no
`CommandDefinition` to any parity suite.

## Build-time targets

| Target | Runs |
|---|---|
| `make test-batch` | the `pkg/batch` unit tests |
| `make bench-batch` | `go test -bench` over `pkg/batch`, which declares no benchmarks, so it runs none |
| `make test-writemode` | the `pkg/writemode` unit tests |
| `make bench-writemode` | `go test -bench` over `pkg/writemode`, which declares no benchmarks, so it runs none |

## Settings and signals

The embedding application sets `batch.Config` (defaults in
[`engineering.md`](engineering.md#3-configuration)). The aggregator registers
its Prometheus collectors with the registry passed to `batch.WithRegistry`, or
with `prometheus.DefaultRegisterer`:

| Metric | Watch for |
|---|---|
| `quicraft_batch_flush_reason_total{reason}` | mostly `time`: batches time out instead of filling, so `MaxBatchSize` is too high for the load |
| `quicraft_batch_queue_depth` | near `QueueSize`: `Submit` is about to return `*QueueFullError` |
| `quicraft_batch_batches_total{shard,status}` | `failed` flushes: batches of caller commands that did not commit |

Delivery through the aggregator is at-least-once. That is not a setting: a
deployment that cannot tolerate replay makes its commands idempotent or does not
use the aggregator.
