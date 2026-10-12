# Observability: API

> **Status: NORMATIVE** · Scope: go-quicraft metrics, logging and event
> notifications · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library, so the Go API is the API.

| Transport | This feature |
|---|---|
| Embedded (Go call) | the interfaces below. They point inward: the library calls your collector and your listener. |
| Unix socket / REST / gRPC / QUIC / MCP | Not applicable. The library serves no scrape endpoint; it hands the embedder a `prometheus.Registry` to expose. |

These are not missing-capability rows. A metrics endpoint inside an embedded
library would mean the library opening a socket in someone else's process.

## Go API

Import `github.com/jeremyhahn/go-quicraft/pkg` (package `quicraft`) and
`github.com/jeremyhahn/go-quicraft/pkg/config`. The full interface listing is in
[`engineering.md`](engineering.md#1-exported-surface).

### Install a metrics collector

```go
func WithMetrics(c MetricsCollector) HostOption
```

| | |
|---|---|
| Request | any `MetricsCollector`. Nil, including a typed nil, means no collection and no hot-path cost. |
| Response | a `HostOption` for `NewHost` |
| Errors | none |
| Permission | none (in-process call) |

Implementer obligations:

- Pre-bind per-shard metrics in `RegisterShard` and use cached references in
  the hot-path methods.
- Do not panic. The library recovers and counts a collector panic, but that is
  a backstop.
- Do not block. A stalled metrics backend stalls the worker that called it.
- Treat `reason` as a label from a closed set and never derive new values from
  it.

Embedding `NoOpMetricsCollector` lets an implementation override only the
methods it cares about.

### The closed `reason` vocabularies

| Method | Values | Zero-value note |
|---|---|---|
| `IncMessagesDropped` | `unresolved_target`, `queue_full`, `stream_unavailable`, `write_failed`, `marshal_failed`, `protocol_violation`, `unsendable`, `peer_rejected`, `identity_mismatch` | `shardID` and `to` are 0 for `protocol_violation` |
| `IncSnapshotsRejected` | `shutdown`, `concurrency_limit`, `memory_budget`, `liveness`, `malformed`, `io_error`, `policy`, `unauthorized`, `not_accepting` | `shardID` is 0 when rejected before the sender was authorized |
| `IncUnauthorizedMessageDropped` | `removed_replica`, `removed_identity`, `misrouted`, `unknown_member`, `identity_mismatch`, `missing_identity` | none |

`identity_mismatch` and `removed_identity` are never routine. `unknown_member`
is the benign config-change window. The meaning of each security reason is in
[`architecture.md`](architecture.md#security-counters-are-split-on-purpose).

### Use the reference collector

```go
func NewPrometheusMetricsCollector() *PrometheusMetricsCollector
func NewPrometheusMetricsCollectorWithConfig(cfg config.MetricsConfig) *PrometheusMetricsCollector

type PrometheusMetricsCollector struct {
    Registry                *prometheus.Registry
    EnablePerMessageMetrics bool
    // unexported fields
}
```

`Registry` is dedicated, not the Prometheus default. Serve it yourself:

```go
http.Handle("/metrics", promhttp.HandlerFor(c.Registry, promhttp.HandlerOpts{}))
```

`NewPrometheusMetricsCollector()` uses the zero `config.MetricsConfig`, so
per-shard metrics are off. The label mode cannot change after construction.

Setting `EnablePerMessageMetrics` after construction populates the
`_detailed_` transport counters with a per-shard, per-peer, per-message-type
breakdown. It is expensive and meant for debugging.

Metric names and labels are listed in
[`engineering.md`](engineering.md#8-metric-reference).

### Sample latency observations

```go
func NewSampledMetricsCollector(inner MetricsCollector, sampleRate uint64) *SampledMetricsCollector
```

Records one in `sampleRate` commit-latency and apply-latency observations.
`sampleRate` is the inverse rate; 0 becomes 100. Counters are never sampled.

### No-op

```go
type NoOpMetricsCollector struct{}
```

Satisfies the whole interface and does nothing. Passing nil to `WithMetrics`
does the same with less indirection.

### Configure logging

```go
func config.NewLogger(cfg LogConfig) *slog.Logger
func config.SetDefaultLogger(cfg LogConfig)
func WithLogger(l *slog.Logger) HostOption

type LogConfig struct {
    Format LogFormat // LogFormatText (default) | LogFormatJSON
    Level  LogLevel  // LogLevelDebug | LogLevelInfo (default) | LogLevelWarn | LogLevelError
}

type LogValidationError struct{ Field, Message, Value string }
```

`NewLogger` writes to stderr. For any other destination, build your own
`slog.Logger` and pass it with `WithLogger`. Lower layers log through
`slog.Default()`, so install the same handler there to capture everything.

### Receive notifications

Set `HostConfig.EventListener` to a `*config.EventListener`. Every field is an
optional callback; nil fields are skipped.

| Group | Callbacks | Payload |
|---|---|---|
| Raft | `OnLeaderUpdated`, `OnLeaderReady` | `LeaderInfo` |
| | `OnLeaderLost`, `OnCampaignLaunched`, `OnCampaignSkipped`, `OnProposalDropped`, `OnReadIndexDropped` | `EventShardInfo` |
| | `OnSnapshotRejected` | `SnapshotInfo` |
| System | `OnNodeHostShuttingDown` | none |
| | `OnNodeReady`, `OnNodeUnloaded`, `OnNodeDeleted`, `OnMembershipChanged`, `OnLogCompacted`, `OnLogDBCompacted` | `EventShardInfo` |
| | `OnConnectionEstablished`, `OnConnectionFailed` | `ConnectionInfo` |
| | `OnSnapshotSendStarted`, `OnSnapshotSent`, `OnSnapshotSendAborted`, `OnSnapshotReceived`, `OnSnapshotRecovered`, `OnSnapshotCreated`, `OnSnapshotCompacted` | `SnapshotInfo` |
| Failure | `OnShardFailed` | `ShardFailedInfo` |
| | `OnLogDBDiskWarning`, `OnLogDBDiskFull` | `DiskInfo` |

`OnCampaignSkipped` is never invoked. Detect a leaderless shard with
`Host.GetLeaderID` or `ShardInfo.Health == ShardNoLeader`.

`OnLeaderReady` fires once the shard's leader for `LeaderInfo.Term` has
committed an entry of that term, which is when ReadIndex stops returning
`ErrReadIndexNotReady`. It follows the `OnLeaderUpdated` for the same pair and
lasts until the next one, which also covers the same leader re-elected at a
higher term, a case `OnLeaderLost` does not report.
`Host.ShardReadReady` is its synchronous form. The contract is in
[the host API](../multi-group-host/api.md#event-listener).

`ShardFailedInfo.Err` is one of the public error types `quicraft.ApplyError`,
`SMPanicError`, `SaveStateError`, `CommitError`, `WorkerPanicError`,
`SnapshotError`, `SnapshotRecoveryExhaustedError` or `CorruptConfigEntryError`;
match it with `errors.As`.

`SnapshotInfo.Err` is set only on `OnSnapshotRejected`, to the rejection's
cause when one exists: the snapshot pool's refusal (`*SnapshotDuplicateError`,
`*SnapshotPoolBusyError`), an abandonment (`ErrStopped`, a save's
`*SnapshotSavePanicError`), the error or `*SnapshotRecoveryPanicError` that
rejected a recovery before the state machine restore began, or the error
behind an inbound transfer the receiver refused. It is nil on every other
event and on a rejection with no underlying error (an incomplete transfer, a
shutdown or policy refusal). The full list, with causes, is in the
[host API errors](../multi-group-host/api.md#errors).

Delivery comes in two classes:

| Class | Callbacks | Runs on | You may |
|---|---|---|---|
| dispatched | `OnLeaderUpdated`, `OnLeaderReady`, `OnLeaderLost`, `OnCampaignLaunched`, `OnShardFailed`, `OnSnapshotCreated`, `OnSnapshotCompacted`, `OnSnapshotRecovered`, `OnLogCompacted`, `OnLogDBCompacted`, `OnSnapshotRejected` | one dispatcher goroutine holding no engine lock, FIFO | call any `Host` API, including `StopShard` |
| inline | every other callback | the goroutine that produced the event | return quickly; do not call a synchronous `Host` API that stops or unloads a shard |

There is no ordering guarantee between the two classes.

### Inspect host state

```go
func (h *Host) HealthCheck() *HealthStatus
func (h *Host) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo
func (h *Host) GetShardInfo(shardID uint64) (*ShardInfo, error)
func (h *Host) GetLeaderID(shardID uint64) (uint64, bool, error)
func (h *Host) ShardCount() int
func (h *Host) ID() string
func (h *Host) NodeIdentity() string
func (h *Host) RaftAddress() string

type ShardHealth uint8 // ShardHealthy, ShardNoLeader, ShardLagging, ShardUnloaded,
                       // ShardError, ShardRecovering, ShardSnapshotting

type HealthStatus struct {
    Status             string      `json:"status"` // ok | degraded | sealed | unhealthy
    BarrierSealed      bool        `json:"barrier_sealed"`
    DiskOK             bool        `json:"disk_ok"`
    ShardCount         int         `json:"shard_count"`
    Shards             []ShardInfo `json:"shards,omitempty"`
    DiagnosticsDropped uint64      `json:"diagnostics_dropped"` // process-wide
}
```

`HealthCheck` returns a value; serving it is the embedder's job.
`DiagnosticsDropped` is the number of library log records dropped since the
process started because the `slog.SetDefault` handler was not taking them (the
library never blocks on its own diagnostics: see
[`engineering.md`](engineering.md#9-contained-diagnostics-and-supervision)).
It is process-wide, so every `Host` in a process reports the same value, and it
does not affect `Status`. There is no Prometheus series for it.
`ShardHealth` unmarshals from JSON and rejects an unrecognized value with
`*InvalidShardHealthError`, which matches `ErrInvalidShardHealth`.

## Configuration

| Field | Owner | Default |
|---|---|---|
| `PerShardMetrics` | `config.MetricsConfig` | `false` |
| `Format`, `Level` | `config.LogConfig` | text, info |
| `EnablePerMessageMetrics` | `PrometheusMetricsCollector` | `false` |
| `SampleRate` | `SampledMetricsCollector` | 100 |

## Versioning and deprecation

- Adding a method to `MetricsCollector` or any sub-interface breaks every
  embedder implementation, because embedders implement these interfaces.
- The `reason` vocabularies are part of the contract. Adding a value changes
  what dashboards see; removing one silently drops a series.
- Metric names belong to the reference implementation, not the interface. An
  embedder with its own collector chooses its own names.
- `EventListener.OnCampaignSkipped` is reserved and never fires.
