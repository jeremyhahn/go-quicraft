# Observability — API

> **Status: NORMATIVE** · Scope: go-quicraft metrics, logging and event
> notifications · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API.**

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the interfaces below, all pointing *inward* — the library calls your collector and your listener |
| Unix socket / REST / gRPC / QUIC / MCP | n/a. **This library serves no scrape endpoint.** It hands the embedder a `prometheus.Registry`; exposing it over HTTP is the embedder's. |

Not missing-capability rows. A metrics endpoint inside an embedded library would
be the library opening a socket in someone else's process.

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg` and `.../pkg/config`

### Capability: install a metrics collector

```go
func WithMetrics(c MetricsCollector) HostOption
```

| | |
|---|---|
| Request | any `MetricsCollector`; **nil is supported** and means no collection, with no hot-path cost |
| Response | a `HostOption` |
| Errors | none |
| Permission | none — in-process call |

```go
type MetricsCollector interface {
    RaftMetrics
    LogDBMetrics
    TransportMetrics
    SecurityMetrics
    RegisterShard(shardID uint64)
    UnregisterShard(shardID uint64)
}
```

**Implementer obligations:**

- **Pre-bind per-shard metrics in `RegisterShard`** and use cached references in
  the hot-path methods. A label lookup inside the commit loop is measurable.
- **Never panic.** Your collector runs on the hot path. The library forwards
  through a guard, but that is a backstop, not a licence.
- **Treat `reason` as a label from a closed set** and do not derive new values
  from it — see below.
- **Do not block.** Observation is best-effort by construction; a metrics
  backend that stalls must not stall a proposal.

### The four sub-interfaces

Each component internally receives only its own, so a component cannot record
another's metric.

```go
type RaftMetrics interface {
    ObserveProposalLatency(shardID uint64, d time.Duration)
    ObserveCommitLatency(shardID uint64, d time.Duration)
    ObserveApplyLatency(shardID uint64, d time.Duration)
    ObserveReadIndexLatency(shardID uint64, d time.Duration)
    // ... election, snapshot, batch-size and drop counters
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

`waldb.LogDBMetrics` is a **separate interface with the same four methods**,
declared in `pkg/logdb/waldb` so that package need not import `pkg`. Install it
with `waldb.WithMetrics`.

### The closed `reason` vocabularies

| Method | Values | Zero-value caveat |
|---|---|---|
| `IncMessagesDropped` | `unresolved_target`, `queue_full`, `stream_unavailable`, `write_failed`, `marshal_failed`, `protocol_violation`, `unsendable`, `peer_rejected` | `shardID` and `to` are **both 0** for `protocol_violation` |
| `IncSnapshotsRejected` | `shutdown`, `concurrency_limit`, `memory_budget`, `liveness`, `malformed`, `io_error`, `policy` | `shardID` is **0** when rejected before the header parsed |
| `IncUnauthorizedMessageDropped` | `removed_replica`, `misrouted`, `unknown_member`, `address_mismatch`, `shared_address`, `identity_mismatch`, `missing_identity` | — |

The security split is **operational, not cosmetic**: one aggregate counter
cannot tell a rolling membership change from an attack.
`identity_mismatch` means the sender does not hold the claimed replica's private
key and is never routine; `unknown_member` is the benign config-change window.

### Capability: use the reference collector

```go
func NewPrometheusMetricsCollector() *PrometheusMetricsCollector
func NewPrometheusMetricsCollectorWithConfig(cfg config.MetricsConfig) *PrometheusMetricsCollector

type PrometheusMetricsCollector struct {
    Registry                *prometheus.Registry
    EnablePerMessageMetrics bool
    // ...
}
```

`Registry` is **dedicated**, not the default one. Serve it yourself:

```go
http.Handle("/metrics", promhttp.HandlerFor(c.Registry, promhttp.HandlerOpts{}))
```

`NewPrometheusMetricsCollector()` uses the zero `config.MetricsConfig`, so
**per-shard metrics are off**.

`EnablePerMessageMetrics` is set on the collector after construction and turns
`IncMessagesSent`/`IncMessagesReceived` from simple atomic counters into a
per-shard/target/message-type breakdown. **Expensive; for debugging.**

The full metric name list is in
[`engineering.md`](engineering.md#8-metric-reference).

### Capability: sample expensive observations

```go
func NewSampledMetricsCollector(inner MetricsCollector, sampleRate uint64) *SampledMetricsCollector
```

`SampleRate` is the **inverse** rate — 100 means one in 100. Default 100.

### Capability: no-op

```go
type NoOpMetricsCollector struct{}
```

Satisfies the whole interface and does nothing. Passing `nil` to `WithMetrics`
achieves the same with less indirection.

### Capability: configure logging

```go
func config.NewLogger(cfg LogConfig) *slog.Logger
func config.SetDefaultLogger(cfg LogConfig)
func WithLogger(l *slog.Logger) HostOption

type LogConfig struct {
    Format LogFormat   // LogFormatText | LogFormatJSON
    Level  LogLevel    // LogLevelDebug | LogLevelInfo | LogLevelWarn | LogLevelError
}

type LogValidationError struct{ Field, Message, Value string }
```

Neither opens a file nor chooses a destination.

### Capability: receive notifications

```go
type config.EventListener struct { /* the callbacks */ }
```

Payloads: `LeaderInfo`, `ShardFailedInfo`, `SnapshotInfo`, `ConnectionInfo`,
`DiskInfo`, `EventShardInfo`.

**Two classes, and the distinction matters:**

| Class | Callbacks | Runs on | You may |
|---|---|---|---|
| **cold path** | `OnLeaderUpdated`, `OnCampaignLaunched`, shard failure | a dedicated dispatcher goroutine holding **no engine lock** | call **any** `Host` API, including the synchronous `StopShard` |
| **hot path** | `OnApplied`, `OnCommitted`, `OnReadyToRead`, `OnProposalFailed`, `OnReadIndexBatched`, `OnReadIndexFailed` | the worker | **not** call back into the engine — these resolve pending client requests and have happens-before requirements against `Peer.Handle` |

Cold-path notifications are FIFO on a single consumer, giving a total order and
therefore a per-shard order. **There is deliberately no ordering guarantee
between the two classes.**

### Capability: inspect host state

```go
func (h *Host) HealthCheck() *HealthStatus
func (h *Host) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo
func (h *Host) GetShardInfo(shardID uint64) (*ShardInfo, error)
func (h *Host) ShardCount() int
func (h *Host) ID() string
func (h *Host) NodeIdentity() string
func (h *Host) RaftAddress() string

type ShardHealth uint8
const ShardHealthy ShardHealth = iota   // ...
```

`HealthCheck` returns a value; **serving it is the embedder's**.
`*InvalidShardHealthError` / `ErrInvalidShardHealth` guard an unrecognized
`ShardHealth`.

## Configuration

| Field | Owner | Default |
|---|---|---|
| `PerShardMetrics` | `config.MetricsConfig` | **false** |
| `Format`, `Level` | `config.LogConfig` | text, info |
| `EnablePerMessageMetrics` | `PrometheusMetricsCollector` | **false** |
| `SampleRate` | `SampledMetricsCollector` | 100 |

## Versioning and deprecation

- **Adding a method to `MetricsCollector` or any sub-interface breaks every
  embedder implementation.** These are consumed by implementation; sequence such
  changes bottom-up.
- **The `reason` vocabularies are part of the contract.** Adding a value changes
  what an embedder's dashboards see; removing one silently drops a series.
- Metric **names** are the reference implementation's, not the interface's. An
  embedder with its own collector chooses its own names.
- Nothing here is deprecated.
