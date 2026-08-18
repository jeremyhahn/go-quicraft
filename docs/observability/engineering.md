# Observability — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft metrics, logging and event
> notifications · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Design rationale and the scope fence are in
[`architecture.md`](architecture.md).

## 1. Exported surface

```go
// Installation
func WithMetrics(c MetricsCollector) HostOption
func WithLogger(l *slog.Logger) HostOption

// The composed interface
type MetricsCollector interface {
    RaftMetrics
    LogDBMetrics
    TransportMetrics
    SecurityMetrics
    RegisterShard(shardID uint64)
    UnregisterShard(shardID uint64)
}

type NoOpMetricsCollector struct{}

// Reference implementation
func NewPrometheusMetricsCollector() *PrometheusMetricsCollector
func NewPrometheusMetricsCollectorWithConfig(cfg config.MetricsConfig) *PrometheusMetricsCollector

// Sampling wrapper
func NewSampledMetricsCollector(inner MetricsCollector, sampleRate uint64) *SampledMetricsCollector

// Logging helpers
func config.NewLogger(cfg LogConfig) *slog.Logger
func config.SetDefaultLogger(cfg LogConfig)
```

### The four sub-interfaces

```go
type RaftMetrics interface {
    ObserveProposalLatency(shardID uint64, d time.Duration)
    ObserveCommitLatency(shardID uint64, d time.Duration)
    ObserveApplyLatency(shardID uint64, d time.Duration)
    ObserveReadIndexLatency(shardID uint64, d time.Duration)
    // ... plus election, snapshot, batch-size and drop counters
}

type LogDBMetrics interface {
    ObserveWriteLatency(d time.Duration)
    ObserveFsyncLatency(d time.Duration)
    ObserveCompactionLatency(d time.Duration)
    IncWriteBytes(n int64)
}

type TransportMetrics interface {
    ObserveSendLatency(d time.Duration)
    IncMessagesSent(shardID, to uint64, msgType uint64)
    IncMessagesReceived(shardID, from uint64, msgType uint64)
    IncSnapshotsSent(shardID uint64, bytes int64, d time.Duration)
    IncSnapshotsReceived(shardID uint64, bytes int64, d time.Duration)
    IncSnapshotsRejected(shardID uint64, reason string)
    IncMessagesDropped(shardID, to uint64, reason string)
}

type SecurityMetrics interface {
    IncUnauthorizedMessageDropped(shardID, from uint64, reason string)
    IncUnauthorizedSnapshotDropped(shardID, from uint64)
}
```

`waldb.LogDBMetrics` is a **separate** interface with the same four methods,
declared in `pkg/logdb/waldb` so that package does not import `pkg`. The
concrete implementation lives in the public `quicraft` package to avoid the
import cycle.

## 2. Invariants

**Nil is a supported value everywhere.** A nil `MetricsCollector` means no
collection, with **no cost on the hot path** — the call sites check rather than
dispatching through a no-op. `waldb`'s `WithMetrics(nil)` skips collection
entirely.

**Each component receives only its own sub-interface.** The WAL is handed
`LogDBMetrics`, the transport `TransportMetrics`, and so on, so a component
cannot record another's metric.

**`RegisterShard` is called at shard start and `UnregisterShard` at shard
stop.** Implementations should pre-bind per-shard metrics in `RegisterShard`
and use cached references in the hot-path methods; a label lookup inside the
commit loop is measurable.

**`reason` labels come from closed sets and must not be extended.**

- `IncMessagesDropped`: `"unresolved_target"`, `"queue_full"`,
  `"stream_unavailable"`, `"write_failed"`, `"marshal_failed"`,
  `"protocol_violation"`, `"unsendable"`, `"peer_rejected"`.
  `shardID` and `to` are **both 0** for `"protocol_violation"`, where the frame
  is discarded before its messages are parsed.
- `IncSnapshotsRejected`: `"shutdown"`, `"concurrency_limit"`,
  `"memory_budget"`, `"liveness"`, `"malformed"`, `"io_error"`, `"policy"`.
  `shardID` is **0** when the transfer was rejected before its header parsed.
- `IncUnauthorizedMessageDropped`: `"removed_replica"`, `"misrouted"`,
  `"unknown_member"`, `"address_mismatch"`, `"shared_address"`,
  `"identity_mismatch"`, `"missing_identity"`.

**Implementations must treat `reason` as a metric label and must not derive new
label values from it.**

**The collector runs on the hot path and is embedder code.** `metrics_safe.go`
forwards through a guard rather than trusting the injection; a nil or partially
implemented collector must not panic the engine.

**`PrometheusMetricsCollector` uses a dedicated `prometheus.Registry`**, not the
default one. The caller exposes it (`promhttp.HandlerFor`). Nothing is
registered in an `init()`.

**`NewPrometheusMetricsCollector()` uses the zero `config.MetricsConfig`**, so
per-shard metrics are **off**.

**Cold-path notifications are dispatched; hot-path callbacks are synchronous.**
See [`architecture.md`](architecture.md). There is deliberately **no ordering
guarantee** between the two classes.

## 3. Error semantics

This feature returns **no errors**. Observation is best-effort by construction:
a metrics backend that fails must not fail a proposal.

Two consequences worth stating:

- A collector that panics is the embedder's bug, and the safe forwarder is what
  keeps it from being a cluster-wide one.
- `config.LogConfig` validation reports `*LogValidationError` (with `Field`,
  `Message` and `Value`) for an unrecognized `Format` or `Level`. That is a
  startup check, not a runtime one.

## 4. Configuration

### `config.MetricsConfig`

| Field | Default | Meaning |
|---|---|---|
| `PerShardMetrics` | `false` | per-shard `shard_id` labels instead of host-level aggregates. **Off by default because a host runs thousands of shards.** |

The mode is reported as `quicraft_metrics_per_shard_enabled`, so a dashboard can
distinguish "no per-shard breakdown configured" from "this shard has no
traffic". With it off, each metric's help text says the value is aggregated
across all shards on the host.

### `config.LogConfig`

| Field | Values | Meaning |
|---|---|---|
| `Format` | `LogFormatText`, `LogFormatJSON` | handler selection |
| `Level` | `LogLevelDebug`, `LogLevelInfo`, `LogLevelWarn`, `LogLevelError` | minimum level |

`NewLogger(cfg)` builds a `*slog.Logger`; `SetDefaultLogger(cfg)` installs it as
the process default. Neither opens a file or chooses a destination.

### `config.EventListener`

Carries the notification callbacks and their payload types: `LeaderInfo`,
`ShardFailedInfo`, `SnapshotInfo`, `ConnectionInfo`, `DiskInfo`,
`EventShardInfo`.

### Sampling

```go
c := quicraft.NewSampledMetricsCollector(inner, 100)   // 1 in 100
```

`SampleRate` is the **inverse** rate; default 100.

## 5. Persistence

**None.** Nothing here writes to disk or holds state across a restart. Metric
values live in the embedder's registry; log records go wherever the embedder's
`slog` handler sends them.

## 6. Test Strategy

Covered by `pkg/metrics_test.go`, `pkg/metrics_prometheus_test.go` and
`pkg/metrics_safe_forward_test.go` under `make test-quicraft`.

| Pins | Why it matters |
|---|---|
| every metric registers on the dedicated registry without collision | a duplicate registration panics at startup |
| the safe forwarder tolerates a nil and a partial collector | the collector is embedder code on the hot path |
| host-level vs. per-shard construction under both `MetricsConfig` values | the two paths build different collector types |
| `quicraft_metrics_per_shard_enabled` reports the active mode | a dashboard cannot otherwise tell the two states apart |
| sampling arithmetic in `SampledMetricsCollector` | |

**What a test here must never do**: assert on a metric value from a timing-
dependent path without controlling the clock. Latency histograms are observed
from real durations, and an assertion on a bucket is a flake.

**Fault-injection seam**: `MetricsCollector` is an interface, so a collector
that panics, blocks or counts invocations is one struct — which is exactly how
the safe forwarder is tested.

## 7. Observability of this feature itself

`quicraft_metrics_per_shard_enabled` is the only self-describing metric, and it
exists because the alternative is ambiguity a dashboard cannot resolve.

## 8. Metric reference

Namespace `quicraft`. Names below are the fully-qualified Prometheus names
emitted by the reference implementation. Per-shard forms carry a `shard_id`
label when `PerShardMetrics` is on; otherwise the same name is a label-free
host-level aggregate.

### Raft

| Metric | Type |
|---|---|
| `quicraft_raft_proposal_latency_seconds` | histogram |
| `quicraft_raft_commit_latency_seconds` | histogram |
| `quicraft_raft_apply_latency_seconds` | histogram |
| `quicraft_raft_read_index_latency_seconds` | histogram |
| `quicraft_raft_proposal_batch_size` | histogram |
| `quicraft_raft_commit_batch_size` | histogram |
| `quicraft_raft_elections_total` | counter |
| `quicraft_raft_snapshots_total` | counter |
| `quicraft_raft_proposals_dropped_total` | counter |
| `quicraft_raft_piggyback_commits_total` | counter |

### LogDB

| Metric | Type |
|---|---|
| `quicraft_logdb_write_latency_seconds` | histogram |
| `quicraft_logdb_fsync_latency_seconds` | histogram |
| `quicraft_logdb_compaction_latency_seconds` | histogram |
| `quicraft_logdb_write_bytes_total` | counter |

### Transport

| Metric | Type |
|---|---|
| `quicraft_transport_send_latency_seconds` | histogram |
| `quicraft_transport_messages_sent_total` | counter |
| `quicraft_transport_messages_received_total` | counter |
| `quicraft_transport_messages_sent_detailed_total` | counter |
| `quicraft_transport_messages_received_detailed_total` | counter |
| `quicraft_transport_messages_dropped_total` | counter, `reason` label |
| `quicraft_transport_snapshots_sent_total` | counter |
| `quicraft_transport_snapshots_received_total` | counter |
| `quicraft_transport_snapshots_sent_bytes` | counter |
| `quicraft_transport_snapshots_received_bytes` | counter |
| `quicraft_transport_snapshots_rejected_total` | counter, `reason` label |

### Security

| Metric | Type |
|---|---|
| `quicraft_security_unauthorized_messages_dropped_total` | counter, `reason` label |
| `quicraft_security_unauthorized_snapshots_dropped_total` | counter |

### Meta

| Metric | Type |
|---|---|
| `quicraft_metrics_per_shard_enabled` | gauge |

**The `_detailed_` transport counters are off by default.**
`IncMessagesSent` / `IncMessagesReceived` use simple atomic counters unless
`PrometheusMetricsCollector.EnablePerMessageMetrics` is set to `true` after
construction, which turns on a per-shard/target/message-type breakdown. It is
expensive and is a debugging surface, not a standing dashboard.
