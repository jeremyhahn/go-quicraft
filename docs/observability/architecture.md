# Observability — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft metrics, logging and event
> notifications — `pkg/metrics.go`, `pkg/metrics_safe.go`,
> `pkg/metrics_prometheus.go`, `config.MetricsConfig`, `config.LogConfig`,
> `config.EventListener` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## The problem

A library that is embedded in someone else's process cannot own that process's
telemetry. It cannot pick a metrics backend, register global collectors, choose
a log destination, or decide a cardinality budget — those are the embedder's,
and a library that assumes otherwise is a library you have to fight.

But a consensus engine that reports nothing is unoperable. Election churn, apply
latency and dropped-message counts are not luxuries; they are the only way to
tell a slow disk from a slow network from a compromised peer.

## The shape chosen: interfaces in, nothing global

**Three seams, all injected, all optional.**

| Seam | Type | Nil means |
|---|---|---|
| metrics | `MetricsCollector` via `WithMetrics` | no collection, and no cost on the hot path |
| logging | `*slog.Logger` via `WithLogger` | the package default logger |
| events | `config.EventListener` on `HostConfig` | no notifications |

Nothing here registers a global Prometheus collector, opens a socket, or writes
a file. `PrometheusMetricsCollector` is a **reference implementation** the
embedder may use, ignore, or replace.

### Rejected: a global metrics registry

`prometheus.MustRegister` in an `init()` is how a library makes itself
un-embeddable twice in one process and un-testable in parallel. The one place
this repository registers Prometheus collectors directly is `pkg/batch`, which
sits *above* the `Host` and takes a `prometheus.Registerer` from the caller.

### Rejected: a callback system for logs

There is no `OnLog` hook and no log-level configuration beyond
`config.LogConfig`. Embedders configure their `slog` handler at startup, which
is the mechanism the standard library already provides.

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

The embedder supplies the whole thing; **internally each component is handed
only its own sub-interface**. The WAL cannot record a Raft metric, and the
transport cannot record a security one, because neither can name the method.

`RegisterShard` / `UnregisterShard` exist so an implementation can **pre-bind
per-shard metrics** at shard start rather than paying a label lookup on every
observation. A label lookup inside the commit loop is measurable.

## Cardinality is a first-class decision

`MetricsConfig.PerShardMetrics` is **off by default**, and a host runs thousands
of shards. With it off, per-shard histograms and counters are replaced by
label-free host-level aggregates, and each metric's help text says so.

The mode itself is reported as a gauge, `quicraft_metrics_per_shard_enabled`, so
a dashboard can tell "this cluster has no per-shard breakdown" from "this shard
has no traffic" — two states that otherwise look identical.

`SampledMetricsCollector` wraps any collector and samples the expensive
observations at `1/SampleRate` (default 100), for deployments where even the
aggregate histograms cost too much.

## Drop reasons are closed sets, not free text

Two counters carry a `reason` label, and both document their vocabulary as
**closed**:

`TransportMetrics.IncMessagesDropped` — `"unresolved_target"`, `"queue_full"`,
`"stream_unavailable"`, `"write_failed"`, `"marshal_failed"`,
`"protocol_violation"`, `"unsendable"`, `"peer_rejected"`.

`TransportMetrics.IncSnapshotsRejected` — `"shutdown"`, `"concurrency_limit"`,
`"memory_budget"`, `"liveness"`, `"malformed"`, `"io_error"`, `"policy"`.

**Implementations must treat `reason` as a metric label and must not derive new
label values from it.** An unbounded label is a cardinality explosion, and the
one place a library could hand an embedder one is exactly here.

## Security counters are split on purpose

`SecurityMetrics.IncUnauthorizedMessageDropped` carries seven reasons, and the
split is **operational, not cosmetic**: one aggregate counter cannot tell a
rolling membership change from an attack.

| Reason | What it means |
|---|---|
| `"unknown_member"` | the benign config-change window — a voter added by a change this replica has not applied yet. **Rises and falls on its own.** |
| `"identity_mismatch"` | **the strongest signal here.** Committed membership records a node identity for the claimed replica and the connection presented a different one, so the sender does not hold that replica's private key. A forged `From`, or a stale identity after a key rotation. **Never routine.** |
| `"missing_identity"` | the shard records no identity and `RequireMemberIdentity` is set. A migration gap, not an attack — commit the identity. |
| `"address_mismatch"` | fallback path: a certificate that does not cover the committed address |
| `"shared_address"` | fallback path: one address given to two active members |
| `"removed_replica"` | a node that has not noticed it was removed |
| `"misrouted"` | a message for a shard this replica does not hold |

These events indicate either a misconfigured cluster or an active injection
attempt by an authenticated-but-compromised peer. **They are counted, not just
logged, so operators can alert on them** — a log line is not an alerting
surface.

## Two callback classes, and the deadlock that separates them

**Cold-path notifications run on a dedicated dispatcher goroutine** that holds
no engine lock: `OnLeaderUpdated`, `OnCampaignLaunched`, shard failure. A
listener may therefore call any `Host` API, including the synchronous
`StopShard`.

That is not a convenience. The step worker fires callbacks while holding
`stepWorker.nodesMu.RLock`, and `Host.StopShard` re-enters the same goroutine's
`nodesMu.Lock()`. Go's `RWMutex` is not reentrant, so a handler that reacted to
a shard failure by stopping the shard **deadlocked every shard on that worker**
and a later `Host.Close` never returned. Reacting to a failed shard by stopping
it is the obvious handler to write, which is what made it reachable.

**Hot-path callbacks stay synchronous**: `OnApplied`, `OnCommitted`,
`OnReadyToRead`, `OnProposalFailed`, `OnReadIndexBatched`, `OnReadIndexFailed`.
They resolve pending client requests and have happens-before requirements
against `Peer.Handle`, so they cannot queue behind user code.

**There is deliberately no ordering guarantee between the two classes.**

## Trust boundary

- **No key material, nonce, passphrase, share, certificate private key, or
  command payload appears in any log line or metric label.** Application data is
  the embedder's, and secrets are nobody's.
- **The barrier's decryption-failure warning omits the epoch**, so a log reader
  cannot use it as an oracle.
- `shamir.Share` implements `LogValue()` as well as `String()`, because `slog`
  resolves a `LogValuer` first but a `JSONHandler` with none present encodes
  through `json.Marshaler` — which wrote full share material. See
  [`../shamir-primitive/engineering.md`](../shamir-primitive/engineering.md).
- **A node identity in a log line is an SPKI hash**, public by construction.
- **The collector is embedder code on the hot path.** `metrics_safe.go` exists
  because of it: the safe forwarder guards against a nil or partially
  implemented collector rather than trusting the injection.

## Component boundaries

- **`pkg/metrics.go`** — the interfaces and `NoOpMetricsCollector`.
- **`pkg/metrics_safe.go`** — the safe forwarding wrapper.
- **`pkg/metrics_prometheus.go`** — `PrometheusMetricsCollector`, the reference
  implementation, on a **dedicated registry** the caller exposes.
- **`config.MetricsConfig`** — `PerShardMetrics`.
- **`config.LogConfig`** — `Format`, `Level`, plus `NewLogger` and
  `SetDefaultLogger`.
- **`config.EventListener`** — the notification callbacks and their payloads.
- **`pkg/batch`** — the one package that registers Prometheus collectors
  directly, because it sits above the `Host`.

This feature is part of the go-quicraft capability row covering Raft consensus
and log replication in `docs/00 §R`.

## What this feature deliberately does not do

- **No global registration.** Nothing is registered in an `init()`.
- **No metrics server, no scrape endpoint, no push.** The embedder exposes the
  registry.
- **No tracing.** There is no OpenTelemetry integration and no span
  propagation. `context.Context` is threaded through the public API, so an
  embedder can carry its own.
- **No audit log.** Security-relevant events are counters and log lines; a
  tamper-evident audit trail belongs to the layer with an identity model.
- **No log destination, rotation or retention.** `slog` handler configuration is
  the embedder's.
- **No per-shard metrics by default.** Cardinality is opt-in.
- **No health endpoint.** `Host.HealthCheck()` returns a value; serving it is
  the embedder's.
