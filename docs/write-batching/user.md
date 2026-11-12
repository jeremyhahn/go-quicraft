# Write Batching — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/batch`, `pkg/writemode` ·
> Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

Use the aggregator when you have many small writes and the fixed cost of each
Raft proposal (a log entry, a WAL write, a replication round, an apply)
dominates your payload. Otherwise propose directly.

## Before you start

Three things must hold:

1. **Your commands tolerate being applied twice.** Delivery is at-least-once: a
   batch whose propose timed out may still commit, and retrying it applies every
   command in it again.
2. **You do not need a per-command result.** `Submit` reports only whether the
   batch went through. A command your state machine rejected still returns nil.
3. **Your state machine decodes batches.** Each batch arrives as one entry.

If any of these is a problem, use `Host.SyncPropose`, or a session
([`../client-sessions/user.md`](../client-sessions/user.md)) for at-most-once.

## Wire up an aggregator

```go
agg, err := batch.NewAggregator(batch.HostProposer{Host: host})
if err != nil {
	return err
}
defer agg.Close()

if err := agg.Submit(ctx, shardID, cmd); err != nil {
	return err
}
```

`*Host` cannot be passed directly: its `SyncPropose` also returns an
`sm.Result`, which a batch cannot attribute to one command. `HostProposer`
adapts it.

## Decode batches in your state machine

```go
func (s *MySM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		cmds, err := batch.DecodeBatch(e.Cmd)
		if err != nil {
			// Committed on every replica: report it, do not fail the shard.
			results[i] = sm.Result{Value: sm.ResultInvalidCommand}
			continue
		}
		for _, cmd := range cmds {
			s.apply(cmd)
		}
		results[i] = sm.Result{Value: uint64(len(cmds))}
	}
	return nil
}
```

- `DecodeBatch` returns `nil, nil` for an empty entry, so if a shard carries
  both batched and unbatched entries you need your own discriminator, such as a
  leading type byte or a shard reserved for batched writes.
- Decoded commands point into `e.Cmd`. Copy any you keep past `Update` when
  `ZeroCopyEntryCmd` is on.
- Keep `Update` all-or-nothing across the whole batch, as for any entry.

## Tune the latency/throughput trade

```go
cfg := batch.DefaultConfig()
cfg.FlushInterval = 20 * time.Millisecond
cfg.MaxBatchSize = 500
agg, err := batch.NewAggregator(batch.HostProposer{Host: host}, batch.WithConfig(cfg))
```

`FlushInterval` (default 50 ms) is the most delay the aggregator adds to a
write; raising it batches more. A batch also flushes at `MaxBatchSize` commands
(default 1000) or `MaxBatchBytes` encoded bytes (default 8 MiB, the largest
entry).

If most flushes are timer flushes (the `quicraft_batch_flush_reason_total`
metric with `reason="time"`), batches are not filling: every write is paying
the full interval. Lower `MaxBatchSize` or `FlushInterval`.

`QueueSize` (default 10000) is the back-pressure point. When the queue is full,
`Submit` returns `*QueueFullError` immediately; nothing is dropped silently.

## Handle errors

```go
err := agg.Submit(ctx, shardID, cmd)
var fe *batch.FlushError
switch {
case err == nil:
	// the batch was proposed and applied
case errors.As(err, &fe) && fe.Indeterminate:
	// the outcome is unknown: the command may or may not have been applied
case errors.Is(err, batch.ErrQueueFull):
	// back off and retry
default:
	// definite failure: the command was not applied
}
```

An indeterminate failure is what you get when your own context ends while
waiting, or the propose times out. Retry it only if the command is safe to apply
twice.

## Avoid the copy

```go
buf := make([]byte, n) // allocated for this call only
encodeInto(buf, op)
err := agg.SubmitNoCopy(ctx, shardID, buf)
// never touch buf again
```

`Submit` copies your command. `SubmitNoCopy` does not: the aggregator reads the
slice later, on its own goroutine, possibly after your call returned. Changing
or reusing the buffer commits a wrong command on every replica, and nothing can
detect it afterwards. When in doubt, use `Submit`.

## Shut down

```go
agg.Close()
```

`Close` stops new submissions, flushes what is queued and waits for it. Later
calls to `Submit` return `*ClosedError`. Unflushed commands live only in memory,
so a crash loses them; a short `FlushInterval` limits how many.

## Carry a durability mode in configuration

```go
// name is your application's own setting: "sync", "wal_durable", "batched", ...
m, err := writemode.FromString(name)
if err != nil {
	return err
}
```

`FromString` validates the name. Any `Mode` that reaches you some other way,
such as a number decoded from a file or a message, must be checked:

```go
if err := m.Validate(); err != nil {
	return err
}
```

An unknown `Mode` reports false from `IsSynchronous`, `IsAsync` and
`RequiresQuorum`, so code that branches on `RequiresQuorum` would treat it as
"no quorum needed". `Validate` is the only place that mistake can be caught.

`Batched` reports `RequiresQuorum() == true`: once its batch commits, a batched
write has the same durability as `Sync`.

## Troubleshooting

**A command was applied twice.** You retried an indeterminate `*FlushError`.
Make the command idempotent, or move it off the batched path.

**`*QueueFullError`.** You are submitting faster than the cluster commits. Raise
`QueueSize` only for genuinely transient bursts; otherwise slow down.

**Latency went up after enabling batching.** That is `FlushInterval`, the price
of the throughput.

**Throughput did not improve.** Check the flush reasons. Timer flushes mean
batches are not filling.

**A definite `*FlushError`.** The propose failed for the whole batch, and every
caller in it gets the same error. Inspect the wrapped cause: `errors.As` for a
`*quicraft.NotLeaderError`, or a `*quicraft.EntryTooLargeError` for a single
command too large for an entry; `errors.Is` for `quicraft.ErrBusy` or
`quicraft.ErrDiskFull`.

**Submit returned nil but the state machine rejected the command.** Expected:
the batch has one result. Use `Host.SyncPropose` when you need per-command
results.

**Commands applied in an unexpected order.** Commands in one batch keep their
submission order. Two batches, and batches for different shards, are
independent proposals.

**Garbage in a decoded command.** You reused a buffer passed to
`SubmitNoCopy`. The wrong bytes are now in the log on every replica.
