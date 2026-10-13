# Observability: usage

> **Status: NORMATIVE** · Scope: go-quicraft metrics, logging and event
> notifications · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

Everything here is opt-in. A host with no collector and no listener runs fine
and logs to stderr at info level.

## Before you start

- A Prometheus scrape target, or your own metrics backend behind the collector
  interface.
- An `slog` handler configured the way your service wants its logs.
- One decision: per-shard metrics or host aggregates. A host can run thousands
  of shards, so this is a cardinality decision.

## Turn on metrics

```go
c := quicraft.NewPrometheusMetricsCollector()
host, err := quicraft.NewHost(cfg, quicraft.WithMetrics(c), quicraft.WithoutEncryption())

// The library serves nothing. Expose the registry yourself:
http.Handle("/metrics", promhttp.HandlerFor(c.Registry, promhttp.HandlerOpts{}))
```

The registry is dedicated, not the Prometheus default, so two hosts in one
process do not collide and your own metrics are untouched. (`WithoutEncryption`
stands in for whichever at-rest encryption choice your deployment makes.)

For a per-shard breakdown of latency, batch size and snapshot volume:

```go
c := quicraft.NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{
    PerShardMetrics: true,
})
```

Know what this costs. With it off, each active shard adds about 6 series; with
it on, about 113. You lose no alerts by leaving it off: elections, dropped
proposals, transport drops and security counters keep their `shard_id` label in
both modes. The gauge `quicraft_metrics_per_shard_enabled` reports the active
mode.

## Reduce metric cost further

```go
s := quicraft.NewSampledMetricsCollector(c, 100) // record 1 in 100
```

This samples commit-latency and apply-latency observations only; counters stay
exact. Pass `s` to `WithMetrics`.

Do not enable per-message metrics in production:

```go
c.EnablePerMessageMetrics = true // per shard, peer and message type. Debugging only.
```

## Write your own collector

```go
type myCollector struct {
    quicraft.NoOpMetricsCollector // override only what you need
}

func (m *myCollector) RegisterShard(shardID uint64) {
    // Pre-bind per-shard metrics here. A label lookup in the commit loop is measurable.
}

func (m *myCollector) UnregisterShard(shardID uint64) { /* release them */ }
```

Your code runs on the hot path:

1. Don't panic. The library recovers and counts collector panics, but each one
   discards a metric.
2. Don't block. A stalled backend stalls the worker that called it.
3. Treat `reason` as a label from a closed set and never build new label values
   from it.

## Configure logging

```go
logger := config.NewLogger(config.LogConfig{
    Format: config.LogFormatJSON,
    Level:  config.LogLevelInfo,
})
host, err := quicraft.NewHost(cfg, quicraft.WithLogger(logger), quicraft.WithoutEncryption())
```

Without `WithLogger`, the host builds its logger from `cfg.LogConfig`.
`config.NewLogger` writes to stderr; for any other destination, pass your own
`*slog.Logger`. Lower layers log through `slog.Default()`, so call
`slog.SetDefault` with the same logger (or `config.SetDefaultLogger(cfg.LogConfig)`) if
you want every line in one place.

## Alert on the right things

| Signal | Why |
|---|---|
| `quicraft_security_unauthorized_messages_dropped_total{reason="identity_mismatch"}` | The connection presented a different node identity from the one committed for that replica, so the sender does not hold its private key: a forged sender or a stale identity after key rotation. Never routine. |
| `quicraft_security_unauthorized_messages_dropped_total{reason="removed_identity"}` | A removed node's identity claiming some other replica's ID. No accident produces this. |
| `quicraft_raft_elections_total` rising steadily | Heartbeats delayed past the election timeout. Check [`../quic-transport/user.md`](../quic-transport/user.md) and [`../log-storage/user.md`](../log-storage/user.md) before concluding a node failed. |
| `quicraft_logdb_fsync_latency_seconds` p99 | Separates a slow disk from a slow pipeline. |
| `quicraft_transport_messages_dropped_total{reason="queue_full"}` | A peer that cannot keep up. |
| `quicraft_raft_proposals_dropped_total` | Back-pressure reaching clients. |
| ERROR log "sealed root key was claimed by another loader" | Two processes on one data directory; this barrier may no longer encrypt. |
| `HealthStatus.DiagnosticsDropped` rising | Your `slog` handler is blocking or far slower than the process logs, so library log lines are being discarded. Fix the handler; the engine itself is not stalled by it. |
| ERROR log "worker panic recovered, restarting" | An engine worker ended by panic or `Goexit` and was restarted after 100 ms. The shard's `OnShardFailed` carries the typed cause. |
| ERROR log containing "notification dropped" | Your `EventListener` is not keeping up and a notification was lost. |

Don't page on these:

- `reason="unknown_member"`: the benign config-change window, a voter added by
  a change this replica has not applied yet. It rises and falls on its own. If
  it persists for the leader, this replica missed the change; restart it with a
  `Config.MemberIdentities` that names the leader.
- `reason="removed_replica"`: usually a removed replica that has not learned of
  its removal.
- Shards in quiesce on an idle cluster.

`reason="missing_identity"` is not an attack, but it does need action:
committed membership has no identity for a member. Wipe and re-bootstrap that
shard.

## React to events in code

```go
cfg.EventListener = &config.EventListener{
    OnShardFailed: func(info config.ShardFailedInfo) {
        // Runs on the listener dispatcher, which holds no engine lock,
        // so calling any Host API here is safe, including StopShard.
        _ = host.StopShard(info.ShardID)
    },
}
```

Leader, campaign, shard-failure, snapshot save/recover and compaction
notifications run on that dispatcher, one at a time and in order. Every other
callback (connections, snapshot transfers, disk usage, membership changes, node
lifecycle, dropped proposals and reads) runs inline on the goroutine that
produced it: return quickly and don't stop or unload shards from it.

`OnCampaignSkipped` never fires. Use `host.GetLeaderID` or
`ShardInfo.Health == ShardNoLeader` to detect a leaderless shard.

## Check health

```go
status := host.HealthCheck() // *HealthStatus
```

The library returns a value and serves nothing. Wire it into your own readiness
endpoint. `status.DiagnosticsDropped` counts library log records dropped
because your `slog` handler was not taking them; alert on it rising. It is
process-wide and is not exported as a metric, so copy it into your own gauge
if you want a graph. For incident response, `host.GetShardInfo`,
`host.GetNodeHostInfo` and `host.DiagRaftState` give per-shard detail; see
[`../raft-consensus/cli.md`](../raft-consensus/cli.md).

## Troubleshooting

**No metrics appear.** You passed nil to `WithMetrics`, or you are scraping the
Prometheus default registry instead of `c.Registry`.

**Registration panic at startup.** Two collectors registered on one registry.
Each `NewPrometheusMetricsCollector` creates its own; don't merge them. The
batch aggregator registers on the Prometheus default registry unless you pass
`batch.WithRegistry`.

**Cardinality blew up.** `PerShardMetrics` on with thousands of shards, or
`EnablePerMessageMetrics` left on. Both are off by default for this reason.

**A `shard_id` of 0 on a drop counter.** Expected for
`reason="protocol_violation"` (the frame was discarded before parsing), for a
snapshot rejected before its sender was authorized, and for an unauthorized
snapshot naming a shard this host has not loaded. It is not shard 0.

**Every shard reports the same latency.** Check
`quicraft_metrics_per_shard_enabled`; with it at 0, latency is host-level.

**A log line you need is missing.** The library never logs application
payloads, key material, nonces, passphrases, shares or certificate private keys.
Decryption failures surface as one generic error by design. Instrument your own
layer for richer context.

**The engine stalled with your collector or listener on the stack.** It
blocked. Observation and inline callbacks must return promptly.

**You want tracing.** There is none. `context.Context` is threaded through the
public API, so carry your own spans through it.
