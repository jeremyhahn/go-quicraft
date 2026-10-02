# Observability: engineering contract

> **Status: NORMATIVE** · Scope: go-quicraft metrics, logging and event
> notifications · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

Design rationale and the scope fence are in [`architecture.md`](architecture.md).

## 1. Exported surface

```go
// Installation (package quicraft, pkg/host.go)
func WithMetrics(c MetricsCollector) HostOption
func WithLogger(l *slog.Logger) HostOption

// The composed interface (pkg/metrics.go)
type MetricsCollector interface {
    RaftMetrics
    LogDBMetrics
    TransportMetrics
    SecurityMetrics
    RegisterShard(shardID uint64)
    UnregisterShard(shardID uint64)
}
type NoOpMetricsCollector struct{}

// Reference implementation and sampler (pkg/metrics_prometheus.go)
func NewPrometheusMetricsCollector() *PrometheusMetricsCollector
func NewPrometheusMetricsCollectorWithConfig(cfg config.MetricsConfig) *PrometheusMetricsCollector
func NewSampledMetricsCollector(inner MetricsCollector, sampleRate uint64) *SampledMetricsCollector

// Logging (pkg/config/logging.go)
func NewLogger(cfg LogConfig) *slog.Logger
func SetDefaultLogger(cfg LogConfig)
```

### The four sub-interfaces

```go
type RaftMetrics interface {
    ObserveProposalLatency(shardID uint64, d time.Duration)
    ObserveCommitLatency(shardID uint64, d time.Duration)
    ObserveApplyLatency(shardID uint64, d time.Duration)
    ObserveReadIndexLatency(shardID uint64, d time.Duration)
    IncProposalDropped(shardID uint64)
    ObserveProposalBatchSize(shardID uint64, size int)
    ObserveCommitBatchSize(size int)
    IncElection(shardID uint64)
    IncSnapshot(shardID uint64)
    IncPiggybackCommit(shardID uint64)
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

`waldb.LogDBMetrics` is a separate interface with the same four methods,
declared in `pkg/logdb/waldb` so that package does not import `pkg`. Install it
with `waldb.WithMetrics`.

## 2. Invariants

- **Nil is supported everywhere.** A nil `MetricsCollector` means no collection
  and no hot-path cost: call sites check for nil instead of dispatching through
  a no-op. `WithMetrics` normalizes a typed-nil collector (for example
  `(*PrometheusMetricsCollector)(nil)`) to nil. `waldb.WithMetrics(nil)` skips
  collection.
- **Each component receives only its own sub-interface.** The WAL gets
  `LogDBMetrics`, the transport `TransportMetrics`, and so on.
- **`RegisterShard` runs at shard start and `UnregisterShard` at shard stop.**
  Pre-bind per-shard metrics in `RegisterShard` and use cached references in
  the hot-path methods.
- **Panics are contained.** `WithMetrics` wraps the collector in
  `safeMetricsCollector`, which recovers each panic, counts it and logs at a
  bounded rate. A collector is still expected not to panic; the wrapper exists
  so an embedder bug cannot unwind an engine worker and drop its in-flight
  update. `NoOpMetricsCollector` and an already wrapped collector are not
  wrapped again.
- **`reason` labels come from closed sets** and are never extended by an
  implementation:
  - `IncMessagesDropped`: `unresolved_target`, `queue_full`,
    `stream_unavailable`, `write_failed`, `marshal_failed`,
    `protocol_violation`, `unsendable`, `peer_rejected`, `identity_mismatch`.
    `shardID` and `to` are both 0 for `protocol_violation`, where the frame is
    discarded before its messages are parsed.
  - `IncSnapshotsRejected`: `shutdown`, `concurrency_limit`, `memory_budget`,
    `liveness`, `malformed`, `io_error`, `policy`, `unauthorized`,
    `not_accepting`. `shardID` is 0 when the transfer was rejected before its
    sender was authorized, because the shard the header names is unverified.
  - `IncUnauthorizedMessageDropped`: `removed_replica`, `removed_identity`,
    `misrouted`, `unknown_member`, `identity_mismatch`, `missing_identity`.
    `address_mismatch` and `shared_address` are retired and not reallocated.
- **`IncUnauthorizedSnapshotDropped` reports `shardID` 0** when the snapshot
  named a shard this host has not loaded; that ID is unverified.
- **The sender replica ID is never a Prometheus label** on the security
  counters. It arrives unauthenticated, so labelling by it would let an attacker
  create unbounded series.
- **`PrometheusMetricsCollector` uses a dedicated `prometheus.Registry`**, never
  the default one. Its label mode is fixed at construction.
  `NewPrometheusMetricsCollector()` uses the zero `config.MetricsConfig`, so
  per-shard metrics are off.
- **`SampledMetricsCollector` samples only `ObserveCommitLatency` and
  `ObserveApplyLatency`**, each on its own counter so interleaving cannot bias
  the choice. Every other method, counters included, forwards every call. A
  `sampleRate` of 0 becomes 100.
- **Listener delivery is split.** Leader, campaign, shard-failure, snapshot
  save/recover and compaction notifications are dispatched on one goroutine
  holding no engine lock; the rest run inline. See
  [`architecture.md`](architecture.md#two-callback-classes). There is no
  ordering guarantee between the two classes.

## 3. Error semantics

Observation returns no errors. A metrics backend that fails must not fail a
proposal.

- A collector panic is recovered by the safe forwarder and counted; the engine
  keeps running.
- A dispatched notification refused by a full listener queue is dropped and
  logged at ERROR, except a leader change, which is retried on the next tick.
- `config.LogConfig.Validate` returns `*LogValidationError` (`Field`, `Message`,
  `Value`) for an unrecognized `Format` or `Level`. `NewHost` runs it as part of
  `HostConfig` validation, so it is a startup check.

## 4. Configuration

### `config.MetricsConfig`

| Field | Default | Meaning |
|---|---|---|
| `PerShardMetrics` | `false` | Label the volume-and-latency telemetry with `shard_id`. Off by default: measured, each active shard costs 113 series with it on and 6 with it off. |

`MetricsConfig` is passed to `NewPrometheusMetricsCollectorWithConfig`; it is
not a `HostConfig` field. The mode is reported as
`quicraft_metrics_per_shard_enabled`.

### `config.LogConfig` (`HostConfig.LogConfig`)

| Field | Values | Default |
|---|---|---|
| `Format` | `LogFormatText`, `LogFormatJSON` | `LogFormatText` |
| `Level` | `LogLevelDebug`, `LogLevelInfo`, `LogLevelWarn`, `LogLevelError` | `LogLevelInfo` |

`NewHost` builds its logger from `LogConfig` unless `WithLogger` supplies one.
`NewLogger` writes to stderr through `slog.NewTextHandler` or
`slog.NewJSONHandler`. `SetDefaultLogger` installs that logger as the `slog`
default. The host's own messages go to the host logger; lower layers (engine,
transport, WAL, seal) log through `slog.Default()`.

### `config.EventListener` (`HostConfig.EventListener`)

A struct of optional callbacks; nil fields are skipped and a nil listener costs
nothing. Payload types: `LeaderInfo`, `EventShardInfo`, `SnapshotInfo`,
`ShardFailedInfo`, `ConnectionInfo`, `DiskInfo`. `OnCampaignSkipped` is never
invoked.

### Sampling

```go
c := quicraft.NewSampledMetricsCollector(inner, 100) // 1 in 100 commit/apply latency observations
```

## 5. Persistence

None. Metric values live in the embedder's registry; log records go wherever
the `slog` handler sends them.

## 6. Test strategy

`pkg/metrics_test.go`, `pkg/metrics_prometheus_test.go` and
`pkg/metrics_safe_forward_test.go`, run by `make test-quicraft`.

| Pins | Test |
|---|---|
| every metric registers on the dedicated registry; no global pollution | `TestPrometheusMetricsCollector_RegistryIsolation_NoGlobalPollution` |
| default construction is aggregated; alertable series keep `shard_id` | `TestPrometheusMetrics_DefaultIsAggregated`, `TestPrometheusMetrics_AlertableSeriesKeepShardID` |
| per-shard mode splits by shard; the gauge reports the mode | `TestPrometheusMetrics_PerShardModeSplitsByShard`, `TestPrometheusMetrics_PerShardEnabledGauge` |
| series budget per shard in aggregated mode | `TestPrometheusMetrics_AggregatedModeSeriesBudget` |
| security and drop counters have bounded cardinality | `TestPrometheus_UnauthorizedMessageMetric_BoundedCardinality`, `TestPrometheusMetricsCollector_IncMessagesDropped_BoundedCardinality` |
| sampling covers latency only and samples metrics independently | `TestSampledMetricsCollector_IncMessagesSent_Unsampled`, `TestSampledMetricsCollector_InterleavedSamplingIndependent` |
| the safe forwarder contains panics, forwards every method, skips no-op | `TestSafeMetricsCollector_ContainsPanics`, `TestSafeMetricsCollector_ForwardsEveryMethod`, `TestSafeMetricsCollector_WrappingIsIdempotentAndSkipsNoOp` |

A test here must never assert on a latency bucket without controlling the
clock; histograms observe real durations.

Fault-injection seam: `MetricsCollector` is an interface, so a collector that
panics, blocks or counts invocations is one struct. Embedding
`NoOpMetricsCollector` keeps it short.

## 7. Observability of this feature

`quicraft_metrics_per_shard_enabled` is the only self-describing metric. Panics
recovered from a collector are counted by the safe forwarder and logged at a
bounded rate.

## 8. Metric reference

Namespace `quicraft`, as emitted by `PrometheusMetricsCollector`. "Per-shard
mode" means the `shard_id` label is present only when `PerShardMetrics` is on;
otherwise the series is a host-level aggregate.

| Metric | Type | Labels |
|---|---|---|
| `quicraft_raft_proposal_latency_seconds` | histogram | per-shard mode |
| `quicraft_raft_commit_latency_seconds` | histogram | per-shard mode |
| `quicraft_raft_apply_latency_seconds` | histogram | per-shard mode |
| `quicraft_raft_read_index_latency_seconds` | histogram | per-shard mode |
| `quicraft_raft_proposal_batch_size` | histogram | per-shard mode |
| `quicraft_raft_commit_batch_size` | histogram | none |
| `quicraft_raft_elections_total` | counter | `shard_id` |
| `quicraft_raft_snapshots_total` | counter | per-shard mode |
| `quicraft_raft_proposals_dropped_total` | counter | `shard_id` |
| `quicraft_raft_piggyback_commits_total` | counter | per-shard mode |
| `quicraft_logdb_write_latency_seconds` | histogram | none |
| `quicraft_logdb_fsync_latency_seconds` | histogram | none |
| `quicraft_logdb_compaction_latency_seconds` | histogram | none |
| `quicraft_logdb_write_bytes_total` | counter | none |
| `quicraft_transport_send_latency_seconds` | histogram | none |
| `quicraft_transport_messages_sent_total` | counter | none |
| `quicraft_transport_messages_received_total` | counter | none |
| `quicraft_transport_messages_sent_detailed_total` | counter | `shard_id`, `to`, `msg_type` |
| `quicraft_transport_messages_received_detailed_total` | counter | `shard_id`, `from`, `msg_type` |
| `quicraft_transport_messages_dropped_total` | counter | `shard_id`, `reason` |
| `quicraft_transport_snapshots_sent_total` | counter | per-shard mode |
| `quicraft_transport_snapshots_received_total` | counter | per-shard mode |
| `quicraft_transport_snapshots_sent_bytes` | histogram | per-shard mode |
| `quicraft_transport_snapshots_received_bytes` | histogram | per-shard mode |
| `quicraft_transport_snapshots_rejected_total` | counter | `shard_id`, `reason` |
| `quicraft_security_unauthorized_messages_dropped_total` | counter | `shard_id`, `reason` |
| `quicraft_security_unauthorized_snapshots_dropped_total` | counter | `shard_id` |
| `quicraft_metrics_per_shard_enabled` | gauge | none |

The `_detailed_` counters are registered but stay at zero unless
`PrometheusMetricsCollector.EnablePerMessageMetrics` is set to `true` after
construction. They are a debugging surface: expensive, and not for a standing
dashboard.
