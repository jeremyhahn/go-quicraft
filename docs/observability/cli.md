# Observability — CLI

> **Status: NORMATIVE** · Scope: go-quicraft metrics, logging and event
> notifications · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable.

This feature contributes **no** `CommandDefinition` to any integration parity
suite, and the absence is a design position rather than a gap: **the library
serves no scrape endpoint and writes no log file.** It hands the embedder a
`prometheus.Registry` and a `*slog.Logger` and takes no view on where either
goes. A `--metrics-port` flag here would be this library deciding to open a
socket inside someone else's process.

## What the embedder wires up

```go
c := quicraft.NewPrometheusMetricsCollector()
host, _ := quicraft.NewHost(cfg, quicraft.WithMetrics(c), quicraft.WithoutEncryption())

// The embedder serves it:
http.Handle("/metrics", promhttp.HandlerFor(c.Registry, promhttp.HandlerOpts{}))
```

The registry is **dedicated**, not the default one, and nothing is registered in
an `init()`.

Logging is the same shape: `config.NewLogger(cfg)` returns a `*slog.Logger` and
`config.SetDefaultLogger(cfg)` installs it as the process default. Destination,
rotation and retention are the embedder's.

## Build-time targets

| Target | Runs |
|---|---|
| `make test-quicraft` | includes `metrics_test.go`, `metrics_prometheus_test.go`, `metrics_safe_forward_test.go` |

## Operational levers that are not verbs

| Lever | Where | Note |
|---|---|---|
| `MetricsConfig.PerShardMetrics` | `config` | **off by default.** A host runs thousands of shards; turning it on multiplies every per-shard series by that. |
| `PrometheusMetricsCollector.EnablePerMessageMetrics` | set on the collector after construction | off by default. Per-shard/target/message-type breakdown; **expensive, for debugging.** |
| `SampledMetricsCollector` sample rate | `NewSampledMetricsCollector(inner, 100)` | inverse rate; 1 in 100 by default |
| `LogConfig.Format`, `LogConfig.Level` | `config` | text or JSON; debug/info/warn/error |

## The two signals to alert on, not just collect

1. **`quicraft_security_unauthorized_messages_dropped_total{reason="identity_mismatch"}`**
   — committed membership records a node identity for the claimed replica and
   the connection presented a different one. The sender does not hold that
   replica's private key. **This is never routine.**
2. **`quicraft_raft_elections_total` rising steadily** — heartbeats are being
   delayed past the election timeout. Look at
   [`../quic-transport/user.md`](../quic-transport/user.md) and
   [`../log-storage/user.md`](../log-storage/user.md) before concluding a node
   failed.

And one that looks alarming and is not:
`...{reason="unknown_member"}` is the benign config-change window — a voter
added by a change this replica has not applied yet. It rises and falls on its
own.

## Ad-hoc inspection

Not commands, but the calls an operator procedure is built from:

```go
host.HealthCheck()                 // *HealthStatus; the embedder serves it
host.GetShardInfo(shardID)
host.GetNodeHostInfo(opt)
host.ShardCount()
host.DiagRaftState(shardID)        // incident response — see ../raft-consensus/cli.md
```
