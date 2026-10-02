# State Machines — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/sm`, `pkg/sm/kv`,
> `pkg/internal/rsm` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

## The problem

Raft replicates an ordered log and says nothing about what the log means. The
meaning belongs to the embedder, so the library has to call embedder code on its
hottest path without letting that code break the consensus invariants above it.

State machines want different bargains. A small in-memory one wants the simplest
interface. A large in-memory one wants reads to run while writes apply. One
backed by a disk store already has its own durability and wants the library to
stop duplicating it.

## The shape chosen: three tiers, detected by type assertion

You implement exactly one interface and hand `Host.StartShard` a factory. The
library owns consensus, replication, snapshotting and recovery. Your state
machine only sees committed entries.

| Tier | Interface | Bargain |
|---|---|---|
| 0 | `StateMachine` | All state in memory. `Update` and `Lookup` never run concurrently. The simplest to get right. |
| 1 | `ConcurrentStateMachine` | `Lookup` runs while `Update` runs, and you synchronize them. Two-phase snapshots: `PrepareSnapshot` captures a point-in-time view and `SaveSnapshot` streams it without blocking `Update`. |
| 2 | `DiskStateMachine` | Backed by your own persistent store. `Open` returns the last applied index, and replay of already-applied entries must be idempotent. Client sessions are not available on this tier. |

The tier is detected once, at shard start, by a type switch on what your
factory returns. `sm.CreateFunc` has three typed wrappers (`NewCreateFunc`,
`NewConcurrentCreateFunc`, `NewDiskCreateFunc`) so the choice is checked by the
compiler at the call site instead of inferred from a method set.

### Rejected: one interface with optional methods

A single interface whose `PrepareSnapshot` and `Open` may answer "unsupported"
collapses three contracts into one and moves the tier decision to runtime, where
a wrong answer is a silent divergence instead of a compile error.

### Rejected: the library owning durability for disk state machines

A `DiskStateMachine` already has a store with its own fsync discipline. Wrapping
it would give two durability mechanisms that can disagree about what "applied"
means after a crash. `Open` hands the store's own applied index to Raft and Raft
replays from there, which is why idempotent replay is a tier-2 obligation.

### Rejected: retrying a failed `Update`

There is no apply-retry setting. The shard is marked failed on the first
`Update` error. A retry loop over embedder code that already failed either loops
forever or applies the same entry twice, and the second outcome is a divergence.
Stopping the shard is loud, local and recoverable.

## The RSM adapter enforces the contract

`pkg/internal/rsm` sits between the engine and your code. It is not importable,
but its behaviour is your contract:

- **Type detection** and dispatch to the right tier.
- **Session routing.** Session-managed entries are resolved against the
  replicated session table before `Update` sees them. A duplicate gets the
  cached result and your state machine is not called. See
  [`../client-sessions/architecture.md`](../client-sessions/architecture.md).
- **The read guard.** On tier 0, `Apply` holds the guard exclusively and every
  read entry point (`Lookup`, the byte lookups, `GetHash`) holds it shared, so
  `Update` never overlaps a read. Tiers 1 and 2 promise the opposite: there
  `Apply` holds the guard shared, which keeps out only `Close`, and reads run
  alongside `Update`.
- **Snapshot gating.** The engine pauses applies only for the capture phase of a
  snapshot. On tiers 1 and 2 applies resume as soon as `PrepareSnapshot`
  returns, while `SaveSnapshot` streams.

### Optional interfaces, and how they fail silently

```go
type Hasher       interface{ GetHash() (uint64, error) }
type NALookup     interface{ BytesLookup(key []byte) ([]byte, error) }
type NALookupInto interface{ NALookup; BytesLookupInto(key, dst []byte) ([]byte, error) }
```

These are detected by type assertion. A method with the wrong signature does not
satisfy the interface, produces no compile error, and is never called. Hash
verification or the zero-allocation read path is then off with nothing to say
so. Assert the interface at compile time:

```go
var _ sm.Hasher = (*MyStateMachine)(nil)
```

## Determinism is the load-bearing requirement

Every replica applies the same entries in the same order and must reach the same
state. `Update` must not read a clock, a random source, a network service, map
iteration order, or anything else that differs between processes.

The library cannot check this. Divergence has no symptom until a
`Host.StateMachineHash` comparison catches it or a restart replays the log to a
different answer, and by then the divergent state may have been snapshotted and
shipped to followers. `StateMachineHash` needs `sm.Hasher`.

## Data flow

```
  engine apply worker
        │  committed entries, in order
        ▼
   rsm adapter
        │  config changes, no-ops, session lifecycle ── handled here, SM not called
        │  session duplicate? ──────────────────────── cached result, SM not called
        ▼
   your Update(ctx, entries []sm.Entry, results []sm.Result) error
        │
        ├─ error or panic ──▶ shard marked failed, no retry
        └─ nil ─────────────▶ results returned to the proposer

  reads:  Host.SyncRead / StaleRead / FollowerRead / QueryLocalNode
            └─▶ rsm ──▶ your Lookup, or BytesLookup / BytesLookupInto

  snapshots: rsm ──▶ [Sync] ──▶ [PrepareSnapshot] ──▶ SaveSnapshot(w)
             recovery: rsm ──▶ RecoverFromSnapshot(r)   (always plaintext)
```

`RecoverFromSnapshot` always reads plaintext, whether or not at-rest encryption
is on; the barrier is decrypted before your code runs. See
[`../snapshots/architecture.md`](../snapshots/architecture.md).

## Buffer ownership

By default `Entry.Cmd` is an owned copy. When `Config.ZeroCopyEntryCmd` is true
it aliases an engine buffer that is reused after `Update` returns, and you must
copy anything you keep.

Zero-copy exists because the alternative is one allocation and one copy per
applied entry on the hottest path. Its cost is this rule: a retained alias reads
whatever the buffer holds later.

`NALookupInto` is the read-side counterpart: a lookup into a caller-supplied
buffer, so a read serves bytes without allocating.

## Trust boundary

Your state machine is embedder code running inside the consensus engine, and the
boundary is soft:

- A panic in `Update` is recovered by the apply worker, logged with its stack,
  and fails the shard, so other shards on the host keep running. The shard's
  state after a panic mid-apply is whatever your code left behind.
- An error from `Update` fails the shard: a local denial of service you can
  cause yourself.
- Non-determinism corrupts the replicated state of the whole group, not just the
  local replica, because divergent state is snapshotted and shipped.
- Command payloads are application data. The library does not inspect them,
  validates only their size (`MaxEntrySize`), and does not log them.

## Component boundaries

- **`pkg/sm`**: the three interfaces, the optional interfaces, `Entry`,
  `Result`, the `CreateFunc` wrappers, `CachedDiskStateMachine` and
  `CacheConfig`, and the reserved `Result*` values.
- **`pkg/sm/kv`**: `MemoryStore` (tier 0), `ConcurrentStore` (tier 1), their
  command codec and factories. They are reference implementations and test
  fixtures, not a database.
- **`pkg/internal/rsm`**: the adapter. Internal.
- **Consumer entry point**: `Host.StartShard(members, join, create sm.CreateFunc, cfg)`.

## What this feature deliberately does not do

- No database. `pkg/sm/kv` exists for tests and examples; a production
  key-value store belongs in a layer above this library.
- No transactions, schema, query language or indexes.
- No apply retry, and no setting to add one.
- No restart of a shard after a panic. The panic is contained to the shard,
  which stays failed.
- No determinism checking. `StateMachineHash` reports divergence after the fact.
- No serialization format for your state. `SaveSnapshot` hands you an
  `io.Writer`.
- No concurrency on tier 0. If you need concurrent reads, implement
  `ConcurrentStateMachine`.
