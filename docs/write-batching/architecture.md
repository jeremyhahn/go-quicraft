# Write Batching — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/batch`, `pkg/writemode` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## The problem

Every Raft proposal costs a log entry, a WAL record, an fsync's share, a
replication round and an apply. For a workload of many small writes, that fixed
cost dominates the payload — a hundred 40-byte commands pay a hundred times the
overhead of one 4 KB command carrying the same bytes.

## The shape chosen

An **aggregator in front of the Host** that accumulates commands, encodes them
into one payload, and proposes that payload as a single Raft entry. The state
machine decodes it and applies the commands in order.

```
  Submit(cmd) ──┐
  Submit(cmd) ──┼─▶ Aggregator ── flush trigger ──▶ EncodeBatch ──▶ SyncPropose
  Submit(cmd) ──┘        │                                              │
                    caller blocks                              one Raft entry
                                                                       │
                                                    SM.Update ──▶ DecodeBatch
```

Three flush triggers, whichever comes first: `MaxBatchSize` proposals,
`MaxBatchBytes` encoded bytes, or `FlushInterval` elapsed.

Batch payload format: each command prefixed with its length as a **4-byte
big-endian uint32**. An empty slice produces zero-length output; an empty or
nil input to `DecodeBatch` returns a nil slice with no error.

### Rejected: batching inside the engine

Batching at the proposal queue would apply to every caller whether or not they
want the latency, and would make the delivery semantics of `Propose` depend on
load. Keeping it a separate, explicit component means a caller opts in, and the
`Host` API keeps one meaning.

### Rejected: transparent batching under `Propose`

Same objection, plus it would silently change the durability semantics
documented for `Propose` — see the delivery-semantics section below, which is
the reason this is not a free optimization.

### Rejected: a per-shard byte cap

`MaxBatchBytes` is applied to the **whole** accumulated batch rather than per
shard. A per-shard subset is never larger than the whole, so the bound holds for
every shard's proposal; a multi-shard workload simply flushes slightly sooner
than the per-shard arithmetic would require. Tracking per-shard sizes would add
bookkeeping to the hot path to buy a marginally later flush.

## Delivery semantics: at-least-once, and this is the whole tradeoff

**The aggregator proposes a batch and the batch may be proposed more than
once.** `Submit` blocks until the batch commits, but `Submit` can return before
that — its own context expiring is the common case — and a batch whose proposal
outcome is unknown may be retried.

This is not a defect to be fixed at this layer; it is what batching costs. A
caller that needs at-most-once wraps its commands so that replay is harmless, or
uses a session on the un-batched path
([`../client-sessions/architecture.md`](../client-sessions/architecture.md)).

The `Host`'s own `Propose` is unaffected — batching is opt-in, and a caller that
does not use the aggregator does not inherit its semantics.

## Oversized proposals are isolated, not rejected

A proposal that would push the batch past `MaxBatchBytes` triggers a flush
**first**, so it ends up alone in its own batch. The alternative — appending it
and letting the whole batch exceed the cap — would poison every unrelated
command batched with it: they would all fail on a limit none of them caused.

## The propose deadline is a ceiling, not a timeout

A batch is proposed under `min(latest live caller deadline, ProposeTimeout)`.
The aggregator **never proposes for longer than `ProposeTimeout`**, and **never
cuts a caller short of its own deadline**. A single number could not express
both.

## `writemode` — naming the durability choice

`pkg/writemode` defines the three durability modes as a type so an embedder can
carry the choice through configuration rather than through a comment:

| Mode | Waits for |
|---|---|
| `Sync` | quorum commit — the strongest |
| `WALDurable` | local WAL persistence only — lower latency |
| `Batched` | collection and proposal as a single Raft entry |

**`Mode` is an open `uint8`**, so a value outside the three constants is
representable — a field decoded from configuration or a wire format is the
ordinary way one arrives. Such a value describes **no durability guarantee at
all**: `String` reports `"unknown(N)"`, and `IsSynchronous`, `IsAsync` and
`RequiresQuorum` all report **false**.

That last part is the trap, stated plainly: an embedder dispatching on
`RequiresQuorum` takes the "quorum not required" branch for a `Mode` that in
fact means nothing. The predicates cannot report an error, so **`Validate` is
the boundary check**, and a caller accepting a `Mode` from outside its own code
**must** run it there. `TestMode_UnknownValueGuaranteesNothing` pins that
behavior so it cannot change silently.

## The adapter that had to exist

`*Host` does **not** satisfy `batch.Proposer`. `Proposer.SyncPropose` returns
`error` alone; `Host.SyncPropose` returns `(sm.Result, error)`.

`HostProposer` is the adapter, and it exists because the documented construction
did not compile: the README showed `batch.NewAggregator(host, ...)`, no adapter
existed anywhere in this repository or downstream, and nothing referenced the
package. A documented API that cannot be called is worse than an absent one,
because it looks supported.

`Proposer` is deliberately minimal — implementors wrap their `Host`'s
`SyncPropose` and discard the result value.

## Trust boundary

- **Batched commands are application data**, bounded by `MaxBatchBytes`
  (default `proto.MaxEntrySize`, 8 MB) and never inspected.
- **`SubmitNoCopy` moves ownership of the caller's slice to the aggregator**,
  which reads it at an unspecified later time — after `Submit`'s caller has
  returned, on the flush goroutine. A caller that mutates or reuses that buffer
  commits a **wrong command on every replica**, which is not a local bug. Use it
  only for a buffer allocated per call and then dropped; when in doubt use
  `Submit`.
- One batch is one Raft entry, so a single caller's oversized submission is
  bounded by the same per-entry cap as any other proposal.

## Component boundaries

- **`pkg/batch`** — `Aggregator`, `Config`, `EncodeBatch`/`DecodeBatch`, the
  `Proposer` / `SyncProposer` interfaces and `HostProposer`.
- **`pkg/writemode`** — the `Mode` type and its predicates. No behavior; it
  names a choice the embedder makes.
- **Consumers**: an application above the `Host`. Nothing inside this repository
  uses the aggregator — it is an optional component, not part of the proposal
  path.

This feature is part of the go-quicraft capability row covering Raft consensus
and log replication in `docs/00 §R`.

## What this feature deliberately does not do

- **No at-most-once.** At-least-once is the semantic; see above.
- **No ordering guarantee between batches.** Commands within one batch keep
  their submission order; two batches are two independent proposals.
- **No transactions.** A batch is a delivery optimization, not an atomic unit.
  The state machine applies each command individually and one failing does not
  roll back the others.
- **No automatic use.** The `Host` never batches on its own.
- **No per-shard byte accounting**, deliberately — see above.
- **No behavior in `writemode`.** It defines a vocabulary; the modes are
  implemented by the caller choosing which `Host` method to call.
