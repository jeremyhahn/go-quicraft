# Observability — Usage

> **Status: NORMATIVE** · Scope: go-quicraft metrics, logging and event
> notifications · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Everything here is opt-in and nothing is global. A `Host` with no collector,
no logger and no listener runs fine and tells you nothing.

## Before you start

- A Prometheus scrape target, or your own metrics backend behind the
  `MetricsCollector` interface.
- A `slog` handler configured the way your service wants its logs.
- One decision: **per-shard metrics or host aggregates.** A host runs thousands
  of shards, so this is a cardinality decision, not a preference.

## Task: turn on metrics

```go
c := quicraft.NewPrometheusMetricsCollector()
host, err := quicraft.NewHost(cfg, quicraft.WithMetrics(c))

// The library serves nothing. You expose the registry:
http.Handle("/metrics", promhttp.HandlerFor(c.Registry, promhttp.HandlerOpts{}))
```

The registry is **dedicated**, not the Prometheus default, and nothing is
registered in an `init()` — so two hosts in one process do not collide, and your
own metrics are untouched.

For per-shard breakdowns:

```go
c := quicraft.NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{
    PerShardMetrics: true,
})
```

**Understand what you are buying.** With it off, per-shard histograms and
counters become label-free host aggregates and each metric's help text says so.
With it on, every series is multiplied by your shard count. The active mode is
reported as `quicraft_metrics_per_shard_enabled`, so a dashboard can tell "no
per-shard breakdown configured" from "this shard has no traffic" — otherwise
identical.

## Task: reduce metric cost further

```go
c := quicraft.NewSampledMetricsCollector(inner, 100)   // 1 in 100
```

The rate is **inverse**. Use this when even the host aggregates cost too much.

Do **not** set `EnablePerMessageMetrics` in production:

```go
c.EnablePerMessageMetrics = true   // per-shard/target/message-type. DEBUGGING ONLY.
```

It turns two cheap atomic counters into a labelled breakdown across every peer
and message type.

## Task: write your own collector

```go
type myCollector struct{ /* ... */ }

func (m *myCollector) RegisterShard(shardID uint64) {
    // Pre-bind per-shard metrics HERE. A label lookup in the commit loop
    // is measurable.
}
func (m *myCollector) UnregisterShard(shardID uint64) { /* clean up */ }
// ... plus RaftMetrics, LogDBMetrics, TransportMetrics, SecurityMetrics
```

Three rules, because your code runs on the hot path:

1. **Never panic.** The library forwards through a guard, but that is a
   backstop.
2. **Never block.** A metrics backend that stalls must not stall a proposal.
3. **Treat `reason` as a label from a closed set.** Do not build new label
   values out of it — that is an unbounded-cardinality bug you would be giving
   yourself.

## Task: configure logging

```go
logger := config.NewLogger(config.LogConfig{
    Format: config.LogFormatJSON,
    Level:  config.LogLevelInfo,
})
host, err := quicraft.NewHost(cfg, quicraft.WithLogger(logger))
```

Or install a process default with `config.SetDefaultLogger(cfg)`. The library
chooses no destination, no rotation and no retention.

There is no `OnLog` hook and no per-subsystem level. Configure your `slog`
handler.

## Task: alert on the right things

**Alert, do not merely collect:**

| Signal | Why |
|---|---|
| `quicraft_security_unauthorized_messages_dropped_total{reason="identity_mismatch"}` | committed membership records one node identity and the connection presented another. The sender does not hold that replica's private key — a forged `From`, or a stale identity after a key rotation. **Never routine.** |
| `quicraft_raft_elections_total` rising steadily | heartbeats delayed past the election timeout |
| `quicraft_logdb_fsync_latency_seconds` p99 | the disk, distinguished from the pipeline |
| `quicraft_transport_messages_dropped_total{reason="queue_full"}` | a peer that cannot keep up |
| `quicraft_raft_proposals_dropped_total` | back-pressure reaching clients |
| barrier "claimed by another loader" (ERROR log) | two processes on one data directory; the barrier has stopped encrypting |
| WAL recovery residuals non-empty after a restart | the node may have come back **shorter than the log it acknowledged** |

**Do not alert on these:**

- `...{reason="unknown_member"}` — the benign config-change window. A voter
  added by a change this replica has not applied yet. Rises and falls on its
  own.
- `...{reason="missing_identity"}` — a migration gap, not an attack. Commit the
  identity with `RequestAddNodeWithIdentity`.
- Shards in quiesce on an idle cluster.

## Task: react to events in code

```go
cfg.EventListener = &config.EventListener{
    LeaderUpdated: func(info config.LeaderInfo) {
        // Safe: this runs on the dispatcher goroutine, which holds no
        // engine lock. You may call ANY Host API here, including StopShard.
    },
}
```

That safety is deliberate and was bought with a real bug: reacting to a shard
failure by stopping the shard used to deadlock every shard on that worker, and
`Host.Close` never returned.

**The synchronous callbacks are different.** `OnApplied`, `OnCommitted`,
`OnReadyToRead`, `OnProposalFailed`, `OnReadIndexBatched` and
`OnReadIndexFailed` run on the worker, resolve pending client requests, and
**must not call back into the engine**. And there is no ordering guarantee
between the two classes.

## Task: check health

```go
status := host.HealthCheck()   // *HealthStatus
```

The library returns a value and serves nothing. Wire it into your own readiness
endpoint.

## Troubleshooting

**No metrics appear** — you passed `nil` to `WithMetrics` (which is supported
and means no collection), or you are scraping the Prometheus default registry
rather than `collector.Registry`. The library never touches the default one.

**A metric registration panic at startup** — two collectors sharing a registry.
Each `NewPrometheusMetricsCollector` creates its own; do not merge them into one
registry without checking for name collisions.

**Cardinality blew up** — `PerShardMetrics` with thousands of shards, or
`EnablePerMessageMetrics` left on. Both are off by default for this reason.

**A `shard_id` of 0 on a drop counter** — expected for
`reason="protocol_violation"` (the frame was discarded before its messages were
parsed) and for snapshot rejections before the header parsed. Not a shard with
id 0.

**Every shard reports the same value and I set per-shard metrics** — check
`quicraft_metrics_per_shard_enabled`. That gauge exists precisely to answer this
question.

**A log line I need is missing** — this library logs at the level the event
warrants and never logs application payloads, key material, nonces, passphrases,
shares, or certificate private keys. The barrier's decryption-failure warning
deliberately omits the epoch so it cannot be used as an oracle. If you need
richer context, instrument your own code at your own layer.

**The whole engine stalled and my collector is on the stack** — your collector
blocked. Observation must be best-effort.

**I want tracing** — there is none, and no OpenTelemetry integration.
`context.Context` is threaded through the public API, so carry your own spans
through it.
