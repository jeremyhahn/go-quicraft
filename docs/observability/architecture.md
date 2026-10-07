# Observability: architecture

> **Status: NORMATIVE** · Scope: go-quicraft metrics, logging and event
> notifications: `pkg/metrics.go`, `pkg/metrics_safe.go`,
> `pkg/metrics_prometheus.go`, `config.MetricsConfig`, `config.LogConfig`,
> `config.EventListener` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

## The problem

A library embedded in someone else's process cannot own that process's
telemetry. Picking a metrics backend, registering global collectors, choosing a
log destination and setting a cardinality budget are the embedder's decisions.

A consensus engine that reports nothing cannot be operated either. Election
churn, apply latency and drop counts are how an operator tells a slow disk from
a slow network from a compromised peer.

## The shape chosen: injected seams, nothing global

| Seam | Type | Nil means |
|---|---|---|
| metrics | `MetricsCollector` via `WithMetrics` | no collection and no hot-path cost |
| logging | `*slog.Logger` via `WithLogger` | a logger built from `HostConfig.LogConfig` |
| events | `*config.EventListener` on `HostConfig.EventListener` | no notifications |

The `Host` registers no global Prometheus collector, opens no socket and writes
no file. `PrometheusMetricsCollector` is a reference implementation the embedder
may use, ignore or replace.

### Rejected: a global metrics registry

`prometheus.MustRegister` in an `init()` makes a library impossible to embed
twice in one process and impossible to test in parallel. The one package that
registers collectors directly is `pkg/batch`, which sits above the `Host`: it
uses the `prometheus.Registerer` passed through `batch.WithRegistry`, and
`prometheus.DefaultRegisterer` when none is passed.

### Rejected: a callback system for logs

There is no `OnLog` hook and no log configuration beyond `config.LogConfig`.
Embedders configure an `slog` handler, the mechanism the standard library
already provides.

## The composed interface, and why it is split

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

The embedder supplies the whole interface. Internally each component receives
only its own sub-interface, so the WAL cannot record a Raft metric and the
transport cannot record a security one.

`RegisterShard` and `UnregisterShard` let an implementation pre-bind per-shard
metrics at shard start instead of paying a label lookup on every observation. A
label lookup inside the commit loop is measurable.

## Cardinality is a first-class decision

`MetricsConfig.PerShardMetrics` is off by default, because a host is designed
to run thousands of shards. With it off, the volume-and-latency telemetry
(latency and batch-size histograms, snapshot counts and sizes, piggyback
commits) aggregates to label-free host series, and each one's help text says
so. The series an operator pages on keep `shard_id` in both modes: elections,
dropped proposals, transport message and snapshot drops, and every security
counter.

`MetricsConfig` is not a `HostConfig` field. A Prometheus vector's label set is
fixed when the vector is built, and the embedder builds the collector before
the host sees its configuration, so the knob belongs to
`NewPrometheusMetricsCollectorWithConfig`.

The mode is reported as the gauge `quicraft_metrics_per_shard_enabled`, so a
dashboard can tell "this cluster has no per-shard breakdown" from "this shard
has no traffic".

`SampledMetricsCollector` wraps any collector and records one in `SampleRate`
commit-latency and apply-latency observations (default 100). It never samples
counters: a sampled counter would make every `rate()` built on it wrong.

## Drop reasons are closed sets

Three counters carry a `reason` label. Each vocabulary is closed and defined in
code beside the counter.

| Method | Values |
|---|---|
| `TransportMetrics.IncMessagesDropped` | `unresolved_target`, `queue_full`, `stream_unavailable`, `write_failed`, `marshal_failed`, `protocol_violation`, `unsendable`, `peer_rejected`, `identity_mismatch` |
| `TransportMetrics.IncSnapshotsRejected` | `shutdown`, `concurrency_limit`, `memory_budget`, `liveness`, `malformed`, `io_error`, `policy`, `unauthorized`, `not_accepting` |
| `SecurityMetrics.IncUnauthorizedMessageDropped` | `removed_replica`, `removed_identity`, `misrouted`, `unknown_member`, `identity_mismatch`, `missing_identity` |

Implementations treat `reason` as a metric label and never derive new label
values from it. An unbounded label is a cardinality explosion, and this is the
one place a library could hand an embedder one. For the same reason the sender
replica ID is never a label: it arrives unauthenticated, so an attacker could
mint unlimited series. It appears in the rate-limited drop log line instead.

## Security counters are split on purpose

One aggregate counter cannot tell a rolling membership change from an attack,
so `IncUnauthorizedMessageDropped` separates the cases:

| Reason | Meaning |
|---|---|
| `unknown_member` | No configuration this replica holds (committed, pending in its own log, or `Config.MemberIdentities`) names the sender. Transient during a membership change. If it persists for a leader, this replica missed the change that added it: restart it with a `Config.MemberIdentities` that names the leader. |
| `identity_mismatch` | Committed membership records a node identity for the claimed replica and the connection presented a different one, so the sender does not hold that replica's private key. A forged `From`, or a stale identity after a key rotation. Never routine. |
| `removed_identity` | The connection's authenticated identity belongs to a replica the shard removed, and it is claiming some other replica's ID. No accident produces this shape; alert on it. |
| `removed_replica` | A removed replica that has not yet learned of its removal. Usually benign. |
| `missing_identity` | Committed membership records no identity for a replica the configuration names. Admission requires one on every host with a transport, so the configuration was written without it; wipe and re-bootstrap. |
| `misrouted` | A message addressed to a different replica of this shard. |

The label values `address_mismatch` and `shared_address` are retired and will
not be reallocated.

These events are counted, not only logged, because a log line is not an
alerting surface.

## Two callback classes

Dispatched notifications run on a dedicated dispatcher goroutine that
holds no engine lock: `OnLeaderUpdated`, `OnLeaderReady`, `OnLeaderLost`,
`OnCampaignLaunched`, `OnShardFailed`, `OnSnapshotCreated`, `OnSnapshotCompacted`,
`OnSnapshotRecovered`, `OnLogCompacted`, `OnLogDBCompacted` and
`OnSnapshotRejected`, whether the snapshot pool refused a request or rejected
a recovery that failed before the state machine restore began, or the snapshot
receiver rejected an inbound transfer (the host installs the
dispatcher on the transport before starting it). A listener may call any `Host`
API from them, including the synchronous `StopShard`.

The reason is a deadlock. The step worker holds `stepWorker.nodesMu.RLock`
while it ticks, and `Host.StopShard` takes `nodesMu.Lock()` on the same worker;
a snapshot worker reporting a recovery holds the node's write lease, which an
unload waits out. The snapshot pool reports a rejection on its worker, on the
shard's apply goroutine (which an unload joins) for an automatic snapshot, or on
the goroutine requesting one. A handler that stopped a shard from any of them
would wait on itself forever, stalling every shard on that worker and leaving
`Host.Close` unable to return. Stopping a failed shard is the obvious handler to
write.

Dispatched notifications are delivered FIFO by one goroutine, so they are
totally ordered and therefore ordered per shard. The queue holds 1024 entries
and never blocks the producer. A refused leader change is re-published on the
next tick and is never lost; any other refused notification is dropped and
logged at ERROR with the shard and event.

The remaining callbacks run inline on the goroutine that produced the event and
must not block: connection events and the other snapshot-transfer events
(`OnSnapshotReceived`, `OnSnapshotSendStarted`, `OnSnapshotSent`,
`OnSnapshotSendAborted`) on transport goroutines, disk events on the WAL write path, `OnMembershipChanged` on the
apply path, and `OnNodeReady`, `OnNodeUnloaded`, `OnNodeDeleted`,
`OnProposalDropped`, `OnReadIndexDropped` and `OnNodeHostShuttingDown` on the
goroutine calling the `Host` method that caused them.

Internally, the engine's own request-resolving callbacks (`OnApplied`,
`OnCommitted`, `OnReadyToRead`, `OnProposalFailed`, `OnReadIndexBatched`,
`OnReadIndexFailed`) also stay inline: they have happens-before requirements
against `raft.Peer.Handle` and cannot queue behind user code. There is no
ordering guarantee between dispatched notifications and inline callbacks.

`EventListener.OnCampaignSkipped` exists but is never invoked. Detect a
leaderless shard with `Host.GetLeaderID` or `ShardInfo.Health == ShardNoLeader`.

## Trust boundary

- No key material, nonce, passphrase, share, certificate private key or command
  payload appears in any log line or metric label. Application data belongs to
  the embedder.
- Barrier decryption returns the single `ErrDecryptionFailed` for every failure
  mode, so neither the error nor its log line is an oracle.
- `shamir.Share` and `shamir.ProtectedShare` implement `LogValue()` as well as
  `String()`. `slog` resolves a `LogValuer` first; without one, a `JSONHandler`
  falls back to `json.Marshaler`, which would write the share material. See
  [`../shamir-primitive/engineering.md`](../shamir-primitive/engineering.md).
- A node identity in a log line is an SPKI hash, public by construction.
- The collector is embedder code on the hot path. `WithMetrics` wraps it in
  `safeMetricsCollector`, which recovers and counts panics so an embedder bug
  cannot unwind an engine worker, and normalizes a typed-nil collector to nil.

## Component boundaries

| Location | Holds |
|---|---|
| `pkg/metrics.go` | the interfaces and `NoOpMetricsCollector` |
| `pkg/metrics_safe.go` | the panic-containing forwarder |
| `pkg/metrics_prometheus.go` | `PrometheusMetricsCollector` on a dedicated registry, and `SampledMetricsCollector` |
| `pkg/config` | `MetricsConfig`, `LogConfig`, `NewLogger`, `SetDefaultLogger`, `EventListener` and its payload types |
| `pkg/internal/engine/dispatch.go` | the listener dispatcher |
| `pkg/logdb/waldb` | its own `LogDBMetrics` interface, so the package does not import `pkg` |
| `pkg/batch` | the one package that registers Prometheus collectors directly |

## What this feature deliberately does not do

- No global registration in the `Host`, and nothing registered in an `init()`.
- No metrics server, scrape endpoint or push. The embedder exposes the registry.
- No tracing and no OpenTelemetry integration. `context.Context` is threaded
  through the public API, so an embedder can carry its own spans.
- No audit log. Security events are counters and log lines; a tamper-evident
  audit trail belongs to a layer with an identity model.
- No log file, rotation or retention. `config.NewLogger` writes to stderr; any
  other destination is a handler the embedder supplies through `WithLogger`.
- No per-shard volume-and-latency metrics by default.
- No health endpoint. `Host.HealthCheck()` returns a value; serving it is the
  embedder's job.
