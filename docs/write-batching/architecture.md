# Write Batching — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/batch`, `pkg/writemode` ·
> Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

## The problem

Every Raft proposal pays for a log entry, a WAL record, a share of an fsync, a
replication round and an apply. For many small writes that fixed cost dominates:
a hundred 40-byte commands pay a hundred times the overhead of one 4 KB command
carrying the same bytes.

## The shape chosen

An aggregator in front of the `Host` collects commands, groups them by shard,
encodes each shard's group into one payload and proposes it as a single Raft
entry. The state machine decodes the payload and applies the commands in order.

```
  Submit(shard, cmd) ──┐
  Submit(shard, cmd) ──┼─▶ Aggregator ── flush ──▶ per shard: EncodeBatch ──▶ SyncPropose
  Submit(shard, cmd) ──┘        │                                                │
                      callers wait for the result                        one Raft entry
                                                                                 │
                                                          your Update ──▶ DecodeBatch
```

A flush happens at whichever comes first: `MaxBatchSize` queued proposals,
`MaxBatchBytes` encoded bytes, `FlushInterval` elapsed, or `Close`. Each shard's
group is proposed concurrently, so a slow or leaderless shard does not hold up
the others.

Payload format: each command prefixed with its length as a 4-byte big-endian
`uint32`.

### Rejected: batching inside the engine

Batching in the proposal queue would add latency to every caller whether they
want it or not, and make the delivery semantics of `Propose` depend on load. A
separate component means callers opt in and `Host.Propose` keeps one meaning.

### Rejected: transparent batching under `Propose`

Same objection, and it would silently change `Propose`'s delivery and
result semantics (see below).

### Rejected: a per-shard byte cap

`MaxBatchBytes` applies to the whole accumulated batch, not per shard. A
per-shard subset is never larger than the whole, so the bound holds for every
shard's proposal; a multi-shard workload just flushes a little sooner. Tracking
per-shard sizes would add hot-path bookkeeping to buy a slightly later flush.

## Delivery semantics: at-least-once

The aggregator proposes a raw batch with no client session, so nothing
deduplicates it. It never re-proposes a batch itself. But when a propose times
out or is cancelled, the outcome is unknown: the batch may already be
replicated and may still apply. The caller gets a `*FlushError` with
`Indeterminate` set, and retrying applies every command in that batch again.

This is the cost of batching, not a defect to fix at this layer. A caller that
needs at-most-once makes commands safe to replay, or proposes through a session
on the un-batched path
([`../client-sessions/architecture.md`](../client-sessions/architecture.md)).

A batch also has one result. `HostProposer` discards the state machine's
`Result`, so a command the state machine rejected inside a batch is reported to
its submitter as successful. Use `Host.SyncPropose` when you need a per-command
result.

## Abandoned callers are dropped before encoding

When a caller's context ends while it waits, `Submit` returns at once with an
indeterminate `*FlushError`, or with a definite one if the flush has already
dropped the command. If its batch has not been encoded yet, the aggregator
removes the command, so a caller that gave up is not carried
into the log by whichever caller in the batch was still waiting. If no caller is
left, the batch is not proposed at all.

## Oversized proposals are isolated, not rejected

A proposal that would push the batch past `MaxBatchBytes` triggers a flush
first, so it ends up alone. If it is too large for one entry on its own, the
host rejects it and only its own caller sees the error. Appending it instead
would fail every unrelated command batched with it.

## The propose deadline is a ceiling

A batch is proposed under its own context, derived from the aggregator's
lifetime, never from one caller's: `min(latest live caller deadline,
now + ProposeTimeout)`. The aggregator never proposes for longer than
`ProposeTimeout`, and never cuts a waiting caller short of its own deadline.
Adopting one caller's context would let whichever caller happened to arrive
first govern every other caller in the batch.

## `writemode`: naming the durability choice

`pkg/writemode` names the three durability modes as a type, so an embedder can
carry the choice through configuration:

| Mode | Acknowledged after | `IsSynchronous` / `RequiresQuorum` | `IsAsync` |
|---|---|---|---|
| `Sync` | quorum commit (`Host.SyncPropose`) | true | false |
| `WALDurable` | local WAL persistence (`Host.Propose` plus `RequestState.CommittedC()`, which fires only with `HostConfig.NotifyCommit` set) | false | true |
| `Batched` | quorum commit of the batch that carries it (the aggregator) | true | false |

`Mode` is an open `uint8`, so a value outside the three constants is
representable; a field decoded from configuration or a wire format is the usual
way one arrives. Such a value describes no durability guarantee: `String`
reports `"unknown(N)"` and all three predicates report false. An embedder
dispatching on `RequiresQuorum` would take the "quorum not required" branch for
a mode that means nothing. The predicates cannot return an error, so `Validate`
is the boundary check, and a caller accepting a `Mode` from outside its own code
must run it. `TestMode_UnknownValueGuaranteesNothing` pins this behaviour.

## The `Proposer` adapter

`*Host` does not satisfy `batch.Proposer`: `Proposer.SyncPropose` returns
`error`, while `Host.SyncPropose` returns `(sm.Result, error)`. `HostProposer`
adapts it by discarding the result. `Proposer` stays minimal so tests can
substitute any implementation, and `HostProposer` depends on a one-method
`SyncProposer` interface rather than on `*Host`, which would be an import cycle.

## Trust boundary

- Batched commands are application data, bounded by `MaxBatchBytes` (default
  `proto.MaxEntrySize`, 8 MiB) and never inspected.
- `SubmitNoCopy` hands the caller's slice to the aggregator, which reads it
  later on the flush goroutine, possibly after the caller has returned. A caller
  that mutates or reuses the buffer commits a wrong command on every replica,
  which no later check can detect. Use it only for a buffer allocated per call
  and then dropped.
- One batch is one Raft entry, bounded by the same per-entry cap as any other
  proposal.

## Component boundaries

- **`pkg/batch`**: `Aggregator`, `Config`, `EncodeBatch`/`DecodeBatch`,
  `Proposer`, `SyncProposer`, `HostProposer`.
- **`pkg/writemode`**: the `Mode` type and its predicates. No behaviour.
- **Consumers**: applications above the `Host`. Nothing inside this repository
  uses the aggregator on its own proposal path.

## What this feature deliberately does not do

- No at-most-once, and no per-command result.
- No ordering between batches. Commands in one batch keep submission order; two
  batches are independent proposals.
- No transactions. Whether a batch applies atomically is up to your state
  machine's `Update`.
- No automatic use. The `Host` never batches on its own.
- No per-shard byte accounting.
- No behaviour in `writemode`. The modes are realized by which `Host` method the
  caller uses, or by the aggregator.
