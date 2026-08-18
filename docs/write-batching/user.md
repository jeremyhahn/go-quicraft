# Write Batching — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/batch`, `pkg/writemode` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Use the aggregator when you have many small writes and the fixed cost of a Raft
proposal — a log entry, a WAL record, a replication round, an apply — dominates
your payload. Do not use it when you do not.

## Before you start

Two things have to be true, and neither is negotiable:

1. **Your commands must tolerate being applied more than once.** Delivery
   through the aggregator is **at-least-once**: a batch may be proposed twice.
   That is what batching costs, not a bug to be configured away.
2. **Your state machine must decode batches.** An aggregator on the write side
   is useless unless `Update` knows the entry it receives is a batch.

If either is a problem, use `Host.Propose` directly, or a session
([`../client-sessions/user.md`](../client-sessions/user.md)) — which is the
at-most-once mechanism, and is on the un-batched path.

## Task: wire up an aggregator

```go
import "github.com/jeremyhahn/go-quicraft/pkg/batch"

agg, err := batch.NewAggregator(&batch.HostProposer{Host: host})
if err != nil {
    return err
}
defer agg.Close()

err = agg.Submit(ctx, shardID, cmd)
```

**Note the adapter.** `batch.NewAggregator(host, ...)` does not compile:
`Proposer.SyncPropose` returns `error` alone while `Host.SyncPropose` returns
`(sm.Result, error)`. `HostProposer` is the wrapper.

## Task: decode batches in your state machine

The other half, and it is not optional:

```go
func (s *MySM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
    for i, e := range entries {
        cmds, err := batch.DecodeBatch(e.Cmd)
        if err != nil {
            return err
        }
        for _, cmd := range cmds {
            s.apply(cmd)
        }
        results[i] = sm.Result{Value: uint64(len(cmds))}
    }
    return nil
}
```

`DecodeBatch` on empty or nil input returns a nil slice with **no error**, so a
mixed workload of batched and un-batched entries needs a discriminator of your
own — a leading type byte, or a shard dedicated to batched writes.

## Task: tune the latency/throughput trade

```go
cfg := batch.DefaultConfig()
cfg.FlushInterval = 50 * time.Millisecond   // the latency you are trading
cfg.MaxBatchSize  = 1000
agg, err := batch.NewAggregator(proposer, batch.WithConfig(cfg))
```

**`FlushInterval` is the number that matters.** It is the maximum delay the
aggregator adds to a write, and raising it batches more.

The tell that you have it wrong: **a flush rate pinned at the interval**. That
means batches are timing out rather than filling, so `MaxBatchSize` is too high
for your offered load and every write is paying the full interval for a batch
that is not full. Lower `MaxBatchSize` or lower `FlushInterval`.

`QueueSize` (10000) is the back-pressure point — past it, `Submit` returns
`*QueueFullError` rather than dropping anything.

## Task: avoid the defensive copy

```go
buf := make([]byte, n)     // allocated per call
encodeInto(buf, op)
err := agg.SubmitNoCopy(ctx, shardID, buf)
// DO NOT touch buf again. Ever.
```

**`SubmitNoCopy` transfers ownership.** The aggregator reads the slice at an
unspecified later time — after your call has returned, on the flush goroutine.
Mutating or reusing that buffer commits a **wrong command on every replica**,
which is not a local bug and will not look like one.

Use it only for a buffer allocated per call and then dropped. **When in doubt
use `Submit`**; one copy per command is cheap next to a wrong entry replicated
everywhere.

## Task: shut down cleanly

```go
err := agg.Close()
```

Stops accepting proposals, flushes what is pending, and makes every subsequent
`Submit` return `ErrClosed`. It is one-way.

**A batch that has not flushed lives only in memory.** `Close` flushes it; a
`SIGKILL` does not. If un-flushed writes matter, keep `FlushInterval` short.

## Task: use `writemode` in your configuration

```go
m, err := writemode.FromString(cfg.DurabilityMode)
if err != nil {
    return err
}
if err := m.Validate(); err != nil {   // do this at the boundary
    return err
}
```

**Call `Validate` on any `Mode` that came from outside your own code.** `Mode`
is an open `uint8`, so a value decoded from configuration or a wire format can
be outside the three constants — and such a value describes **no durability
guarantee at all** while `IsSynchronous`, `IsAsync` and `RequiresQuorum` all
quietly report `false`.

Code that dispatches on `RequiresQuorum` therefore takes the "quorum not
required" branch for a mode that means nothing. The predicates cannot report an
error; `Validate` is the only place the mistake is catchable.

## Troubleshooting

**A command was applied twice** — expected. Delivery is at-least-once. Make the
command idempotent, or move it off the batched path.

**`*QueueFullError`** — you are submitting faster than the cluster commits.
This is back-pressure, not a drop. Raise `QueueSize` only if the burst is
genuinely transient; otherwise slow down.

**Latency went up after enabling batching** — that is `FlushInterval`. You
bought throughput with it.

**Throughput did not improve** — check the flush rate. If flushes arrive at the
interval, batches are timing out rather than filling.

**A `*FlushError`** — the underlying `SyncPropose` failed for that batch. Every
caller waiting on it sees it. Look at the `Host`-level cause: `NotLeaderError`,
`ErrBusy`, `ErrDiskFull`.

**Commands applied in an order I did not expect** — commands within one batch
keep their submission order; two batches are two independent proposals with no
ordering between them. A batch is a delivery optimization, **not a transaction**:
the state machine applies each command individually and one failing does not
roll back the others.

**Garbage in a decoded command** — you used `SubmitNoCopy` and reused the
buffer. There is no way to detect this after the fact; the wrong bytes are in
the log on every replica.

**`batch.NewAggregator(host)` will not compile** — correct, and it is the reason
`HostProposer` exists. Use `&batch.HostProposer{Host: host}`.
