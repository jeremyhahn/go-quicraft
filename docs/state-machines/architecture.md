# State Machines — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/sm`, `pkg/sm/kv`,
> `pkg/internal/rsm` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## The problem

Raft replicates an ordered log; it says nothing about what the log *means*. The
meaning is the embedder's, and the library has to call into embedder code on its
hottest path without letting that code break the consensus invariants that
everything above depends on.

Three kinds of state machine want three different bargains. A small in-memory
one wants the simplest possible interface. A large in-memory one wants reads to
run while writes apply. One backed by a disk store already has its own
durability and wants the library to stop duplicating it.

## The shape chosen: three tiers, detected by type assertion

You implement **exactly one** interface and hand `Host.StartShard` a factory.
The library owns consensus, replication, snapshotting and recovery; your state
machine only ever sees committed entries and never touches the Raft protocol.

| Tier | Interface | Bargain |
|---|---|---|
| 0 | `StateMachine` | all state in memory, serialized access — `Update` and `Lookup` never run concurrently. Simplest to implement correctly. |
| 1 | `ConcurrentStateMachine` | `Lookup` runs while `Update` runs. Two-phase snapshotting: `PrepareSnapshot` captures a point-in-time view, `SaveSnapshot` writes it without blocking `Update`. |
| 2 | `DiskStateMachine` | backed by persistent storage. `Open` returns the last applied index at startup, and the machine must replay already-applied entries **idempotently**. |

The tier is detected at startup by type assertion on what your factory returns,
and `sm.CreateFunc` has three type-safe wrappers — `NewCreateFunc`,
`NewConcurrentCreateFunc`, `NewDiskCreateFunc` — so the choice is explicit at
the call site rather than inferred from a method set you may have got wrong.

### Rejected: one interface with optional methods

A single interface with `PrepareSnapshot` and `Open` returning "unsupported"
would collapse three contracts into one and put the tier decision at runtime,
where a wrong answer is a silent divergence rather than a compile error. The
three interfaces make the bargain visible in the type.

### Rejected: the library owning durability for disk state machines

A `DiskStateMachine` already has a store with its own fsync discipline. Wrapping
it would mean two durability mechanisms disagreeing about what "applied" means
after a crash. Instead `Open` hands the machine's own last-applied index back to
Raft, and Raft replays from there — which is why idempotent replay is the
tier-2 obligation rather than an optimization.

### Rejected: retrying a failed `Update`

There is **no apply-retry setting**, deliberately. The apply path is fail-fast:
a shard is marked failed on the **first** `SM.Apply` error, with no retry
counting. A retry loop over embedder code that has already failed once either
loops forever or applies the same entry twice, and the second is a divergence.
Stopping the shard is loud, local, and recoverable; the alternative is neither.

## The RSM adapter — where the contract is actually enforced

`pkg/internal/rsm` sits between the engine and your code. It is internal and
not importable, and its behavior *is* your contract:

- **Type detection** and dispatch to the right tier.
- **Session routing.** Session-managed entries are resolved against the
  replicated session state before your `Update` sees them; a duplicate returns
  the cached result and your state machine is never called. See
  [`../client-sessions/architecture.md`](../client-sessions/architecture.md).
- **The read guard.** For tier 0 it holds a `readGuard` for writing across
  `Apply` and for reading across every read entry point, which is what makes
  "`Update` and `Lookup` never run concurrently" true rather than aspirational.
- **The `(value, supported, error)` wrapper shape** behind every optional
  interface, where the bool reports whether your state machine implemented it at
  all. That three-value form is internal; it is not the contract you implement.

### Optional interfaces, and the failure mode that makes them dangerous

```go
type Hasher       interface{ GetHash() (uint64, error) }
type NALookup     interface{ BytesLookup(key []byte) ([]byte, error) }
type NALookupInto interface{ NALookup; BytesLookupInto(key, dst []byte) ([]byte, error) }
```

Detection is by type assertion. **An implementation with the wrong signature
does not satisfy the interface, produces no compile error, and is simply never
invoked** — hash verification or the zero-alloc read path silently does not run,
and nothing anywhere says so.

Two return values, not three. Assert it at compile time:

```go
var _ sm.Hasher = (*MyStateMachine)(nil)
```

This is the single most common way to have a feature you believe is on and is
not.

## Determinism is the load-bearing requirement

Every replica applies the same entries in the same order and must reach the same
state. `Update` therefore must not read a clock, a random source, a network
service, map iteration order, or anything else that differs between processes.

Nothing in this library can check that for you. Divergence has **no symptom**
until either a `StateMachineHash` comparison catches it or a restart replays the
log and produces a different answer — by which time the divergent state has been
snapshotted and shipped to followers. `Host.StateMachineHash` exists for exactly
this, and it requires `sm.Hasher`.

## Data flow

```
  engine apply worker
        │  entries []proto.Entry (committed, ordered)
        ▼
   rsm adapter
        │  session routing ── duplicate? ──▶ cached result, SM not called
        │  readGuard (tier 0)
        ▼
   your Update(ctx, entries []sm.Entry, results []sm.Result) error
        │
        ├─ error ──▶ shard marked FAILED, no retry
        └─ nil   ──▶ results returned to the proposer

  reads:  Host.SyncRead / StaleRead / FollowerRead
            └─▶ rsm ──▶ your Lookup, or BytesLookup / BytesLookupInto
                        if you implement them

  snapshots: rsm ──▶ PrepareSnapshot (tiers 1, 2) ──▶ SaveSnapshot(w)
             recovery: rsm ──▶ RecoverFromSnapshot(r)   (always plaintext)
```

Your state machine always sees **plaintext** during `RecoverFromSnapshot`,
whether or not at-rest encryption is enabled — the barrier is decrypted to a
sibling temp file first. See
[`../snapshots/architecture.md`](../snapshots/architecture.md).

## Buffer ownership

`Entry.Cmd` may alias the engine's internal buffer, depending on
`Config.ZeroCopyEntryCmd`. When it is true, **you must copy `Cmd` to retain it
past `Update()`**. When false you already have a copy.

The zero-copy path exists because the alternative is one allocation and one copy
per applied entry on the hottest path in the system. The cost is this rule, and
it is a real one: retaining an aliased `Cmd` reads whatever the buffer holds
later, which is another shard's data.

`NALookupInto` is the read-side equivalent — a lookup into a caller-supplied
buffer, so a read serves bytes without allocating.

## Trust boundary

Your state machine is **embedder code running inside the consensus engine**, and
the boundary is a soft one:

- A panic in `Update` is not contained by this layer. Engine workers restart on
  panic to avoid stalling every shard mapped to them, but the shard's state
  after a panic mid-apply is whatever your code left behind.
- An error from `Update` fails the shard, which is a local denial of service you
  can inflict on yourself.
- Non-determinism corrupts the replicated state of the whole group, not just the
  local replica, because the divergent state is snapshotted and shipped.
- Command payloads are application data. This library does not inspect,
  validate, or size-check them beyond `MaxEntrySize`, and does not log them.

## Component boundaries

- **`pkg/sm`** — the three interfaces, the optional capability interfaces,
  `Entry`, `Result`, the `CreateFunc` wrappers, `CachedDiskStateMachine` and
  `CacheConfig`, and the `Result*` sentinel values.
- **`pkg/sm/kv`** — `MemoryStore` (tier 0) and `ConcurrentStore` (tier 1), the
  command codec, and factory functions. These are **reference implementations
  and fixtures**, not a product surface: a real KV store belongs to go-qrdb.
- **`pkg/internal/rsm`** — the adapter. Internal.
- **Consumers**: `Host.StartShard(members, join, create sm.CreateFunc, cfg)`.

This feature is part of the go-quicraft capability row covering Raft consensus
and log replication in `docs/00 §R`.

## What this feature deliberately does not do

- **No database.** `pkg/sm/kv` is a reference implementation used by tests and
  examples. All database operations belong to go-qrdb; do not grow this into
  one.
- **No transactions, no schema, no query language, no indexes.**
- **No apply retry, and no configuration to add one.**
- **No panic recovery around embedder code**, beyond the engine's own worker
  restart.
- **No determinism checking.** `StateMachineHash` reports divergence after the
  fact; nothing prevents it.
- **No serialization format for your state.** `SaveSnapshot` hands you an
  `io.Writer` and gets out of the way.
- **No concurrency for tier 0.** If you need concurrent reads, implement
  `ConcurrentStateMachine`; the adapter will not fake it for you.
