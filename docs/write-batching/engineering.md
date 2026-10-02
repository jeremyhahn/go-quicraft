# Write Batching — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/batch`, `pkg/writemode` ·
> Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

Rationale and the scope fence are in [`architecture.md`](architecture.md); the
full signatures are in [`api.md`](api.md).

| Source | Holds |
|---|---|
| `pkg/batch/aggregator.go` | `Aggregator`, `Config`, options, flush loop, metrics |
| `pkg/batch/hostproposer.go` | `SyncProposer`, `HostProposer` |
| `pkg/batch/decode.go` | `EncodeBatch`, `DecodeBatch` |
| `pkg/batch/errors.go` | sentinels and typed errors |
| `pkg/writemode/mode.go`, `errors.go` | `Mode`, `FromString`, predicates, `InvalidModeError` |

## 1. Invariants

**Delivery is at-least-once.** A batch is proposed with no client session. The
aggregator never re-proposes a batch, but a propose that times out or is
cancelled may still commit, and a caller retry then applies it twice.

**One result per batch.** `HostProposer` discards the `sm.Result`, so a command
the state machine rejected is reported to its submitter as successful.

**Flush triggers**, whichever comes first:

| Trigger | `flush_reason` |
|---|---|
| `len(currentBatch) >= MaxBatchSize` after an add | `size` |
| the next proposal would push the encoded size past `MaxBatchBytes` | `bytes` |
| `FlushInterval` elapsed with proposals pending | `time` |
| `Close` | `close` |

The encoded size of a proposal is `len(data) + 4`. The byte check runs before
the append and only when the batch is non-empty, so a proposal that is too large
on its own is flushed alone and fails alone.

**`MaxBatchSize` and `MaxBatchBytes` count the whole batch, across shards.** A
flush groups the batch by shard and proposes each shard's group concurrently
(inline when there is one shard), so a slow shard cannot stall the others. One
flush completes before the loop takes the next proposal.

**Callers that gave up are not proposed.** Before encoding, each shard's group
drops proposals whose context has ended (they receive a definite
`*FlushError`, though their `Submit` has normally already returned). If none
remain, nothing is proposed. If every remaining context ends before the propose
context is built, the batch is dropped with a definite `*FlushError` wrapping
`context.Canceled`.

**Propose context.** Derived from the aggregator's lifetime context, never from
a caller's, with deadline `min(latest deadline among live callers,
now + ProposeTimeout)`; a live caller with no deadline leaves only
`ProposeTimeout`. This keeps one caller from cutting the batch short for the
others, and keeps a caller with a very long deadline from parking the flush
goroutine past `ProposeTimeout`. The lifetime context is cancelled only after
the flush loop has exited, so `Close`'s final flush is not cancelled.

**`Submit` copies `data`; `SubmitNoCopy` takes it.** The aggregator reads the
slice later on the flush goroutine. Under `SubmitNoCopy` the caller must never
touch the slice again, on any return path: a context error means the proposal
was enqueued and may still be encoded.

**`Submit` never blocks on a full queue.** It enqueues without waiting and
returns `*QueueFullError` if the queue is at `QueueSize`.

**`Submit` waits for the result, its context, or `Close`.**

- A delivered result always wins a tie with the context: the result channel is
  drained before reporting the context.
- On its own context ending, `Submit` returns `*FlushError{Err: ctx.Err(),
  Indeterminate: true}`. A bare context error would read as "did not happen".
- If `Close` races the call, `Submit` still returns the real result if the
  close path flushes the proposal, the context error if the context ends first,
  or `*ClosedError` once the flush goroutine has exited without delivering one.

**`Close` is one-way and idempotent**: it marks the aggregator closed, drains
the queue into the batch (through the same byte check), flushes, waits for the
flush goroutine and returns nil.

**Payload format**: `[len:4 BE][cmd]...`. Encode and decode both refuse a
length above `math.MaxInt32` (on 32-bit platforms it would wrap negative and
panic the slice), so the encoder never writes what the decoder cannot read. An
undecodable committed entry would be undecodable on every replica forever.

**`writemode.Mode` is an open `uint8`.** The valid set is defined once
(`modeSpecs`), and `String`, `Validate` and the three predicates all read it.
An unknown value reports `"unknown(N)"` and false from every predicate;
`Validate` is the boundary check.

## 2. Error semantics

| Error | Returned when | Fail-closed |
|---|---|---|
| `*ClosedError` / `ErrClosed` | `Submit` after `Close`, or `Close` won the race | yes |
| `*NilProposerError` / `ErrNilProposer` | `NewAggregator(nil, ...)` | yes; construction fails |
| `*QueueFullError` / `ErrQueueFull` | the queue is at `QueueSize` | yes; back-pressure, nothing enqueued |
| `*FlushError` / `ErrFlush` | the batch failed: proposer error, encode error, abandoned caller, or the caller's own context | yes for that batch; `Indeterminate` marks an unknown outcome |
| `*EncodeError` / `ErrEncode` | `EncodeBatch` on a command above `math.MaxInt32`; reaches `Submit` inside a `*FlushError` | yes |
| `*DecodeError` / `ErrDecode` | `DecodeBatch` on malformed input | yes |
| `*InvalidModeError` | `FromString` on an unknown name, `Validate` on an unknown value | yes |

`FlushError.Indeterminate` is true when the proposer's error is
`context.DeadlineExceeded` or `context.Canceled`, when the propose context has
ended, or when the caller's own context ended while waiting. Otherwise the
failure is definite.

Sentinels are package-level values; each typed error implements `Is` against
its sentinel, and `FlushError` implements `Unwrap`.

The one path that is not fail-closed by itself: `writemode`'s predicates on an
unknown `Mode` report false instead of failing, which is why `Validate` exists.

## 3. Configuration

| Field | Default | Meaning |
|---|---|---|
| `MaxBatchSize` | `DefaultMaxBatchSize` = 1000 | proposals before a flush |
| `MaxBatchBytes` | `DefaultMaxBatchBytes` = `proto.MaxEntrySize` (8 MiB) | encoded bytes per batch; the batch never exceeds it |
| `FlushInterval` | `DefaultFlushInterval` = 50 ms | maximum time between flushes, and the latency added to a write |
| `QueueSize` | `DefaultQueueSize` = 10000 | queue capacity |
| `ProposeTimeout` | `DefaultProposeTimeout` = 30 s | ceiling on one batch's propose |

`DefaultMaxBatchBytes` is `proto.MaxEntrySize` because the batch is one entry
and the protocol rejects a larger `Cmd`; `Config.MaxEntrySize` and
`MaxProposalPayloadSize` on the shard can only lower that. Without a byte cap,
1000 commands of a few KiB build a batch the host rejects whole, and the next
batch forms the same way.

`WithConfig` copies only positive fields, so a zero or negative field keeps its
default. `WithLogger(nil)` keeps `slog.Default()`; `WithRegistry(nil)` keeps
`prometheus.DefaultRegisterer`.

`writemode` reads no configuration. `FromString` parses a name from the
embedder's own configuration: `""`/`sync`, `wal_durable`/`wal`/`async`,
`batched`/`batch`.

## 4. Persistence

None. An unflushed batch lives only in memory; `Close` flushes it, a crash
loses it. What reaches disk is one ordinary Raft entry per shard per flush,
through the normal proposal path
([`../log-storage/engineering.md`](../log-storage/engineering.md)).

## 5. Test strategy

Targets: `make test-batch`, `make bench-batch`, `make test-writemode`,
`make bench-writemode`.

| Covers | Where |
|---|---|
| each flush trigger, alone and combined | `pkg/batch` |
| oversized-proposal isolation | `pkg/batch` |
| propose deadline: ceiling, latest live caller, no caller | `pkg/batch` |
| abandoned callers dropped before encoding; indeterminate reporting | `pkg/batch` |
| concurrent `Submit` under `-race`; per-shard concurrent flush | `pkg/batch` |
| `Close` semantics and the `Close`/`Submit` race | `pkg/batch` |
| `EncodeBatch`/`DecodeBatch` round trips, malformed and oversized input | `pkg/batch` |
| `HostProposer` adapting the `Host` signature and driving an aggregator | `pkg/batch` |
| `TestMode_UnknownValueGuaranteesNothing` | `pkg/writemode` |

`TestMode_UnknownValueGuaranteesNothing` must not be deleted: it pins that an
unknown `Mode` reports false from every predicate, which embedder dispatch
depends on.

Fault-injection seam: `Proposer` is an interface, so a test proposer that fails,
blocks or counts calls is one struct.

## 6. Observability

Prometheus collectors, registered with the `WithRegistry` registry or
`prometheus.DefaultRegisterer`. If an identical collector is already registered
(several aggregators in one process), the existing one is reused so updates are
still scraped.

| Metric | Type | Labels |
|---|---|---|
| `quicraft_batch_batches_total` | counter | `shard`, `status` (`attempted`, `success`, `failed`) |
| `quicraft_batch_batch_size` | histogram | none |
| `quicraft_batch_flush_duration_seconds` | histogram | none |
| `quicraft_batch_queue_depth` | gauge | none |
| `quicraft_batch_flush_reason_total` | counter | `reason` (`size`, `bytes`, `time`, `close`) |

Only `batches_total` carries a shard label; the histograms are aggregate so a
host with thousands of shards does not multiply them into hundreds of thousands
of series.

What to watch: `flush_reason_total{reason="time"}` dominating means batches
time out rather than fill, so `MaxBatchSize` is too high for the load and every
write pays the full interval; `queue_depth` near `QueueSize` precedes
`*QueueFullError`. `BatchLen()` gives the current unflushed count.

Logging goes to the `WithLogger` logger: WARN for a failed flush (with
`indeterminate`), ERROR for an encode failure. A failed flush is a batch of
caller commands that did not commit, so it is the event worth alerting on.
Command payloads are never logged.
