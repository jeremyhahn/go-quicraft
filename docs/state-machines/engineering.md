# State Machines — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/sm`, `pkg/sm/kv`,
> `pkg/internal/rsm` · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

- `github.com/jeremyhahn/go-quicraft/pkg/sm`: the interfaces you implement, the
  optional interfaces, the factory wrappers and `CachedDiskStateMachine`.
- `github.com/jeremyhahn/go-quicraft/pkg/sm/kv`: the reference KV
  implementations.
- `pkg/internal/rsm`: the adapter that drives your state machine. Internal and
  not importable; documented here because its behaviour is your contract.

Design rationale and the scope fence are in [`architecture.md`](architecture.md),
task guidance in [`user.md`](user.md). Session deduplication inside the apply
path is specified in
[`../client-sessions/engineering.md`](../client-sessions/engineering.md).

## Tiers

| Tier | Interface | Snapshot phases | Persistence | `Lookup` vs `Update` | Client sessions |
|---|---|---|---|---|---|
| In-memory | `StateMachine` | `SaveSnapshot` | none; the snapshot is the whole state | serialized by the adapter | supported |
| Concurrent | `ConcurrentStateMachine` | `PrepareSnapshot`, `SaveSnapshot` | none | concurrent; you synchronize | supported |
| On-disk | `DiskStateMachine` | `Sync`, `PrepareSnapshot`, `SaveSnapshot` | your own store | concurrent; you synchronize | not supported |

Source: `pkg/sm/interfaces.go`.

## Determinism

`Update` must be a pure function of (current state, entries). Every replica
applies the same entry sequence and must reach byte-identical state. `Update`
must not read the wall clock, `math/rand`, environment variables, host identity,
`GOMAXPROCS` or any other per-node value, and Go's randomized map iteration
order must not affect the state it produces or the `Result` values it writes.
This applies to all three tiers.

Raft does not detect a violation. The logs keep agreeing while replicas diverge
in the state they derive, and the usual first symptom is a linearizable read
whose answer depends on which replica served it. Implement `Hasher` and compare
replicas at the same applied index with `Host.StateMachineHash`.

## The interfaces

### `StateMachine`

```go
type StateMachine interface {
	Update(ctx context.Context, entries []Entry, results []Result) error
	Lookup(ctx context.Context, query interface{}) (interface{}, error)
	SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
	Close(ctx context.Context) error
}
```

- `Update` applies committed entries. `len(results) == len(entries)` and you
  must write every `results[i]`. Return an error only for an unrecoverable
  failure: the entry is committed, so every replica hits the same error on
  replay. An undecodable payload is a per-entry outcome: write
  `sm.ResultInvalidCommand` and leave state unchanged, as both reference stores
  do.
- `Lookup` runs on the caller's goroutine, but the adapter holds its read guard
  exclusively across `Apply`, so `Update` and `Lookup` never overlap. See
  [Concurrency](#concurrency).
- `SaveSnapshot` writes the entire state. It runs with applies paused: the
  engine's snapshot pool sets the saving gate and drains applies first, and the
  gate is released only after the save returns (`regularSMWrapper.saveSnapshotGated`
  in `pkg/internal/rsm/adapter.go`). `Lookup` can still run during it.
- `RecoverFromSnapshot` replaces all state with what it reads.
- `Close` releases resources. The adapter first takes its read guard
  exclusively to drain in-flight reads and applies and mark itself closed, then
  waits out any snapshot still streaming, then calls your `Close`. Later reads
  get `ErrClosed` instead of stale data.

### `ConcurrentStateMachine`

```go
type ConcurrentStateMachine interface {
	Update(ctx context.Context, entries []Entry, results []Result) error
	Lookup(ctx context.Context, query interface{}) (interface{}, error)
	PrepareSnapshot() (interface{}, error)
	SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
	Close(ctx context.Context) error
}
```

`PrepareSnapshot` runs with applies paused and returns an opaque point-in-time
value, passed unchanged to `SaveSnapshot`. As soon as it returns, the adapter
releases both the engine's apply gate and its own capture locks, and applies
resume while `SaveSnapshot` streams (`concurrentSMWrapper.saveSnapshotGated`).
So:

- The value `PrepareSnapshot` returns must be unaffected by later `Update`
  calls. A map handed back by reference and then mutated is a torn snapshot.
- `SaveSnapshot` runs concurrently with `Update` and must not touch live state.

Three shapes work: deep-copy in `PrepareSnapshot` (O(n) memory, what
`kv.ConcurrentStore` does), a copy-on-write view, or a snapshot handle from a
database with snapshot isolation.

`Lookup` and `GetHash` run concurrently with `Update`. Your locking must keep a
read from observing a half-applied batch.

### `DiskStateMachine`

```go
type DiskStateMachine interface {
	Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error)
	Update(ctx context.Context, entries []Entry, results []Result) error
	Lookup(ctx context.Context, query interface{}) (interface{}, error)
	Sync() error
	PrepareSnapshot() (interface{}, error)
	SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
	Close(ctx context.Context) error
}
```

- `Open` initializes the store in the directory QuicRaft assigns and returns the
  last applied index, or 0 on a fresh start. The adapter stores a non-zero
  value as its own applied index (`rsm.StateMachine.Open`). An index at or
  above the local snapshot's is authoritative and the snapshot is not
  recovered. An index below it means the store is behind the snapshot, and the
  entries in between may be compacted out of the log, so `Host.StartShard`
  recovers the store from the snapshot and seeds the applied markers to the
  snapshot index (#280).
- Replay must be idempotent. The store can commit its own write and the node
  crash before QuicRaft records the matching applied index. Persist your applied
  index atomically with the data and return it from `Open`.
- `Sync` flushes pending writes. The adapter calls it before `PrepareSnapshot`,
  so the captured point is one that survives a crash.
- Snapshot streaming and concurrency are as for `ConcurrentStateMachine`.

#### Client sessions are not available on this tier

Every session entry point on `Host` (`GetNewSession`, `GetNewSessionWithOwner`,
`RegisterSession`, `ProposeWithSession`, `SyncProposeWithSession`,
`CloseSession`) refuses a shard backed by a `DiskStateMachine` with
`ErrSessionsUnsupportedOnDisk`. Use idempotent commands proposed with
`Propose`/`SyncPropose`.

The reason is `Open`. The session table is replicated state that travels only
in the snapshot stream and is restored by `RecoverFromSnapshot`. A disk state
machine that reports a non-zero applied index at or above its local snapshot
from `Open` skips that path on restart, because recovering would roll its database back to the snapshot index.
The replica would come up with correct data and an empty session table, then
answer `ResultSessionExpired` for entries its peers apply normally. Dragonboat's
`IOnDiskStateMachine` has the same constraint.

## `Entry` and `Result`

```go
type Entry struct {
	Index uint64
	Cmd   []byte
}
func (e *Entry) CopyCmd() []byte // nil for an empty Cmd

type Result struct {
	Value uint64
	Data  []byte
}
```

`Entry` is a narrowed view of `proto.Entry` (68-byte header with `Term`, `Type`,
`Key`, `ClientID`, `SeriesID`, `RespondedTo`, `OwnerID`; `pkg/proto/entry.go`).
None of those fields reach you. `Term` is withheld so the same entry at the same
index produces the same result whichever term proposed it.

- **Indexes are not contiguous.** The adapter drops config changes, the
  bootstrap membership entry, the leader's empty no-op entry, session
  register/unregister entries and session duplicates before building the slice.
  Indexes are strictly increasing; gaps are normal.
- **`Update` may be called more than once per committed batch.** With session
  expiry enabled the adapter splits the batch at fixed checkpoints (see
  [Apply pipeline](#apply-pipeline)). Each call is all-or-nothing on its own.

### Buffer ownership of `Cmd`

`proto.Entry.Cmd` aliases the buffer it was decoded from, and the engine
recycles that buffer after the apply. `Config.ZeroCopyEntryCmd` decides what you
get:

- `false` (default): the adapter copies every non-empty `Cmd`. You may retain it.
- `true`: `Cmd` aliases the engine buffer at zero allocations. Copy anything you
  keep past `Update`.

The same applies to `Result.Data` under zero-copy: a `Data` slice pointing into
`Cmd` is recycled too. The adapter copies `Data` before caching it in the
session table, but the client receives the slice you returned, so keeping it
valid is your job.

```go
// Safe in either mode.
key := string(e.Cmd[:4])          // string conversion copies
val := make([]byte, len(e.Cmd)-4)
copy(val, e.Cmd[4:])

// Unsafe under ZeroCopyEntryCmd: retains aliases the engine will recycle.
c.pending = append(c.pending, e.Cmd)
c.store[string(e.Cmd[:4])] = e.Cmd[4:]
```

### Reserved result values

| Constant | Value | Meaning |
|---|---|---|
| `sm.ResultSessionExpired` | `math.MaxUint64 - 2` | set by the adapter when a session-managed entry names an unregistered or stale session. The host turns it into `*SessionExpiredError`. |
| `sm.ResultInvalidCommand` | `math.MaxUint64 - 3` | set by a state machine for a committed payload it cannot decode; state is unchanged for that entry |

The `Data` slice you return is handed to the client as-is; do not mutate it
later.

## `Update` is all-or-nothing

Every entry in the call is applied and every result written, or the state is
back at its pre-call value and you return an error. A replica that applied half
a batch and reported failure cannot be told where to resume.

Ways to get there: a database transaction rolled back on failure; decode and
validate everything first, then mutate in a pass that cannot fail (both
reference stores); for append-only stores, truncate back to the pre-call
position; for small state, snapshot and restore.

The `results` slice is reused between calls. Write every element.

## Concurrency

| Tier | `Update` vs `Lookup`/`GetHash` | `Update` vs `SaveSnapshot` |
|---|---|---|
| `StateMachine` | serialized: `Apply` holds the read guard exclusively, reads hold it shared | serialized: the whole save runs with applies paused |
| `ConcurrentStateMachine` | concurrent: `Apply` holds the guard shared, which excludes only `Close` | concurrent once `PrepareSnapshot` returns |
| `DiskStateMachine` | concurrent, as above | concurrent once `PrepareSnapshot` returns |

`Host.SyncRead`, `FollowerRead`, `QueryLocalNode` and `StaleRead` all call the
adapter's `Lookup` on the caller's goroutine. `StaleRead` and `QueryLocalNode`
skip the ReadIndex protocol and return whatever the local replica has applied;
`SyncRead` and `FollowerRead` confirm consistency first.

What the tier-0 exclusion costs: a local read waits behind an in-flight apply,
which during follower catch-up can carry up to 64 MiB of entries
(`maxEntriesToApplySize`, `pkg/internal/raft/log.go`). `sync.RWMutex` has no
context-aware acquire, so the read's deadline is not observed while it waits
for the lock. Go's `RWMutex` also blocks new readers once a writer is queued, so
one apply behind a slow read stalls every later read on the shard. An
implementation that needs reads during writes belongs on the concurrent tier;
there, neither wait applies.

## Optional interfaces

Found by type assertion on the value your `CreateFunc` returned. None are
required.

### `Hasher`

```go
type Hasher interface{ GetHash() (uint64, error) }
```

Surfaced by `Host.StateMachineHash(shardID) (hash, appliedIndex uint64, ok bool, err error)`.
`ok` is false when the state machine does not implement `Hasher`.

- The hash must be order-independent. Both reference stores sort keys and
  length-prefix each field before hashing.
- On the concurrent and disk tiers `GetHash` runs concurrently with `Update`, so
  it must synchronize with `Update` and hash whole batches. A hash of a
  half-applied batch reports a divergence that does not exist.
  `kv.ConcurrentStore` holds every bucket lock across `GetHash`, and its
  `Update` holds every affected bucket's lock across the batch.
- `StateMachineHash` reads the applied index before the hash, so a concurrent
  apply makes the pair conservative (older index, newer state) and a comparison
  rejects it as "not the same index" rather than reporting false divergence.
  Compare only replicas at the same `appliedIndex`.

### `NALookup`

```go
type NALookup interface{ BytesLookup(key []byte) ([]byte, error) }
```

When the query passed to `Lookup` is a `[]byte` and the state machine implements
this, the adapter calls it instead of `Lookup`. A `nil` result is returned as an
untyped `nil` interface, so `result == nil` tests for absence on both paths; a
non-nil empty slice means "present, empty".

### `NALookupInto`

```go
type NALookupInto interface {
	NALookup
	BytesLookupInto(key, dst []byte) ([]byte, error)
}
```

Copy the value into `dst` (growing it with `append`) and return the filled
sub-slice. The caller owns the result; do not retain it.

The copy is required. `Host.SyncReadBuf` and `Host.StaleReadBuf` hand the
pooled buffer to application code with a `release` function, and the adapter
drops its read guard before returning because the contract guarantees the bytes
do not alias state machine memory. Returning an internal slice lets a reader see
a value rewritten underneath it, even after `Close`.

```go
result, release, err := host.SyncReadBuf(ctx, shardID, key)
if err != nil {
	return err
}
defer release()
// use result before returning; it must not outlive release
```

Calling `release` twice is safe. Not calling it leaks a pooled buffer; using
`result` afterwards reads someone else's bytes. Without `NALookupInto` the
`*Buf` reads still work, through `BytesLookup` or `Lookup`, with an ordinary
allocation and a no-op `release`.

## `CachedDiskStateMachine`

`pkg/sm/cached.go` wraps a `DiskStateMachine` in a write-through LRU cache.
In-memory tiers gain nothing from it, so there is no wrapper for them.

```go
type CacheConfig struct {
	MaxEntries   int              // > 0, required
	ExtractKV    EntryKeyFunc     // required
	EntryApplied EntryAppliedFunc // optional; required for conditional operations
}
type EntryKeyFunc func(cmd []byte) (key string, value []byte)
type EntryAppliedFunc func(cmd []byte, result Result) bool

func NewCachedDiskStateMachine(inner DiskStateMachine, cfg CacheConfig) (*CachedDiskStateMachine, error)
```

- `ExtractKV` returns the key and value from a command. `("", nil)` skips the
  entry; a non-empty key with a `nil` value means delete.
- `EntryApplied` decides whether an entry took effect. The cache is filled from
  the value in the command, not from the store. For a conditional write
  (compare-and-set, put-if-absent, delete-if-match) the store may have declined
  it, and without the predicate a later `Lookup` returns a value the store never
  held. Only the state machine knows its outcome encoding: `kv.OpPutIfAbsent`
  reports `kv.ResultStored` or `kv.ResultNotStored`, and both are successful
  applications.
- `sm.ResultInvalidCommand` entries are never cached, whatever the predicate
  says.
- Affected keys are invalidated before the inner `Update` and repopulated after,
  so a racing `Lookup` gets a miss, not a stale hit. An `Update` error leaves
  them invalidated.
- The cache is cleared on `Open`, `RecoverFromSnapshot` and `Close`. Values are
  copied on the way in and out.
- `GetHash` delegates to the inner store, or returns `*InvalidCacheConfigError`
  if it does not implement `Hasher`.
- `BytesLookup` on a miss uses the inner `NALookup` if present, else the inner
  `Lookup`; a non-`[]byte` answer returns `*UnsupportedLookupResultError`
  naming the type instead of an empty result.
- `CacheStats() (hits, misses uint64)` reports cumulative counters.

```go
wrapped, err := sm.NewCachedDiskStateMachine(inner, sm.CacheConfig{
	MaxEntries: 100000,
	ExtractKV: func(cmd []byte) (string, []byte) {
		c, err := kv.DecodeCommand(cmd)
		if err != nil {
			return "", nil
		}
		switch c.Op {
		case kv.OpPut, kv.OpPutIfAbsent:
			return string(c.Key), c.Value
		case kv.OpDelete:
			return string(c.Key), nil
		default:
			return "", nil
		}
	},
	EntryApplied: func(cmd []byte, result sm.Result) bool {
		c, err := kv.DecodeCommand(cmd)
		if err != nil {
			return false
		}
		if c.Op == kv.OpPutIfAbsent {
			return result.Value == kv.ResultStored
		}
		return true
	},
})
if err != nil {
	return err
}
createFn := sm.NewDiskCreateFunc(func(_, _ uint64) sm.DiskStateMachine { return wrapped })
```

## Factories and wiring

```go
type CreateFunc func(shardID, replicaID uint64) interface{}

type CreateStateMachineFunc           func(shardID uint64, replicaID uint64) StateMachine
type CreateConcurrentStateMachineFunc func(shardID uint64, replicaID uint64) ConcurrentStateMachine
type CreateDiskStateMachineFunc       func(shardID uint64, replicaID uint64) DiskStateMachine

func NewCreateFunc(fn CreateStateMachineFunc) CreateFunc
func NewConcurrentCreateFunc(fn CreateConcurrentStateMachineFunc) CreateFunc
func NewDiskCreateFunc(fn CreateDiskStateMachineFunc) CreateFunc

func (h *Host) StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error
```

The factory is called once per shard start, with the shard and replica IDs, so
one `CreateFunc` can serve many shards. `StartShard` returns
`quicraft.ErrInvalidSMType` when the value implements none of the three
interfaces. Membership, joining and identities are specified in
[`../multi-group-host/api.md`](../multi-group-host/api.md).

## The RSM adapter

`pkg/internal/rsm` wraps your state machine behind an unexported `smWrapper`, so
the engine calls one `Apply` without caring about the tier. Besides delegation
it owns session lifecycle and deduplication, deterministic session expiry, and
snapshot coordination.

| File | Holds |
|---|---|
| `pkg/internal/rsm/statemachine.go` | `StateMachine`, `Apply`, `applyBatch`, reads, snapshots, `wrapStateMachine` |
| `pkg/internal/rsm/adapter.go` | `smWrapper`, the three wrapper types, the optional-interface helpers |
| `pkg/internal/rsm/errors.go` | sentinel errors |

### Type detection

```go
func wrapStateMachine(raw interface{}) (smWrapper, SMType, error) {
	switch typed := raw.(type) {
	case sm.DiskStateMachine:       // most specific: Open + Sync
		return &onDiskSMWrapper{sm: typed}, SMTypeOnDisk, nil
	case sm.ConcurrentStateMachine: // PrepareSnapshot
		return &concurrentSMWrapper{sm: typed}, SMTypeConcurrent, nil
	case sm.StateMachine:
		return &regularSMWrapper{sm: typed}, SMTypeRegular, nil
	default:
		return nil, 0, ErrInvalidSMType
	}
}
```

`DiskStateMachine`'s method set is a strict superset of
`ConcurrentStateMachine`'s, so the disk case must come first. Checked the other
way round, a disk state machine would be wrapped without `Open` and `Sync`, and
its recovered applied index would be ignored.

| Code | Constant | `String()` | Interface |
|---|---|---|---|
| 0 | `SMTypeRegular` | `"regular"` | `StateMachine` |
| 1 | `SMTypeConcurrent` | `"concurrent"` | `ConcurrentStateMachine` |
| 2 | `SMTypeOnDisk` | `"on-disk"` | `DiskStateMachine` |

`SMType.TypeCode()` is the value stored in `logdb.Bootstrap.Type` and reported
as `ShardInfo.StateMachineType`.

### Construction and session policy

```go
func NewStateMachine(
	shardID, replicaID uint64,
	createFn sm.CreateFunc,
	snapshotDir string,
	local proto.SessionPolicy,
	replicated func() proto.SessionPolicy,
	zeroCopyEntryCmd bool,
) (*StateMachine, error)
```

The factory runs once and the type switch runs once; every later call is a
direct method call on the concrete wrapper.

`local` is this node's configuration, used only while the shard has no
replicated policy (a `join=true` replica that has not yet received the shard's
configuration). `replicated` returns the shard's committed policy and always
wins, even when it is looser: a replica enforcing a stricter local limit would
refuse registrations its peers admit and then answer `ResultSessionExpired` for
entries the rest of the shard applies. The policy is one struct rather than
adjacent `uint64` parameters so the limits cannot be transposed.

The replicated policy is re-read at the start of every `Apply` and before
snapshot recovery, so a replica that learns it late converges without a
restart. That is safe only because the policy is fixed at bootstrap. A config
change that alters it would have to be adopted from the committed entry at the
exact index it applies, or replicas with lagging apply paths would switch
policies at different log positions. A disagreement with local configuration is
logged at WARN.

### The wrapper interface

```go
type smWrapper interface {
	Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error)
	Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error
	Lookup(ctx context.Context, query interface{}) (interface{}, error)
	NALookup(key []byte) ([]byte, bool, error)          // (result, supported, error)
	NALookupInto(key, dst []byte) ([]byte, bool, error) // (result, supported, error)
	GetHash() (uint64, bool, error)                     // (hash, supported, error)
	SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
	saveSnapshotGated(ctx context.Context, w io.Writer, stopper <-chan struct{}, afterCapture func()) error
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
	Close(ctx context.Context) error
	Type() SMType
}
```

This is not the interface you implement. The three-value form belongs to the
wrapper: the `bool` reports whether your state machine implemented the optional
interface. What you implement is two-valued; writing the three-value form
satisfies nothing and compiles silently.

The helpers `naLookup`, `naLookupInto` and `getHash` in `adapter.go` do the type
assertion once for all three wrappers and return `(zero, false, nil)` for "not
implemented", `(value, true, nil)` for success and `(zero, true, err)` for
failure.

| Method | `regularSMWrapper` | `concurrentSMWrapper` | `onDiskSMWrapper` |
|---|---|---|---|
| `Open` | `(0, nil)` | `(0, nil)` | delegates |
| `saveSnapshotGated` | save, then `afterCapture` | `PrepareSnapshot`, `afterCapture`, save | `Sync`, `PrepareSnapshot`, `afterCapture`, save |
| `Type` | `SMTypeRegular` | `SMTypeConcurrent` | `SMTypeOnDisk` |
| everything else | delegates | delegates | delegates |

### Locks

| Lock | Held by | Excludes |
|---|---|---|
| `readGuard` (`sync.RWMutex`) | `Apply`: exclusive on tier 0, shared on tiers 1 and 2. Reads and `GetHash`: shared, for the call only. Snapshot capture and recovery: shared. `Close`: exclusive. | `Close` from everything; on tier 0 also reads from `Apply` |
| `applyMu` (`sync.Mutex`) | `Apply`, snapshot capture, `RecoverFromSnapshot` | concurrent use of the session table, policy and expiry walk. Reads never take it. |
| `streamGuard` (`sync.RWMutex`) | snapshot save, capture through stream: shared. `Close`: exclusive, after `readGuard` | `Close` while a snapshot is still streaming, without holding up applies |

Lock order is `readGuard`, then `applyMu`, then `streamGuard`, everywhere.

### Apply pipeline

`Apply(entries []proto.Entry, results []sm.Result) error` is called by the
engine's apply worker with a pre-sized results slice.

1. **Guards.** Take the locks above in tier mode; return `ErrClosed` if `Close`
   already ran.
2. **Contiguity.** The batch must not start above `lastApplied + 1`, or `Apply`
   returns `ErrApplyGap` and the shard fails. `lastApplied` is the lower bound
   of the expiry checkpoint walk, so a hole would make this replica evaluate
   expiry over a different index range than its peers, a silent divergence.
   A hole means committed entries were dropped or `lastApplied` was never
   seeded. The engine's apply worker makes the same check against its own
   counter as an equality; the adapter's is an upper bound because an
   already-applied entry reaching it is the on-disk replay case and is a no-op.
3. **Policy.** Adopt the shard's replicated session policy.
4. **Segmentation.** With expiry disabled (`ExpiryEntries == 0`) the whole batch
   goes through `applyBatch` once. Otherwise expiry runs only at multiples of
   the expiry threshold, and the batch is split at those checkpoints:

   ```
   for k := lastApplied/interval + 1; k <= lastIndex/interval; k++ {
       applyBatch(entries at or below k*interval)
       sessions.ExpireSessions(k*interval, interval, maxExpirePerCheckpoint())
   }
   applyBatch(remaining entries)
   advanceLastApplied(lastIndex)
   ```

   Apply batching is replica-local (message timing, restart replay), so expiry
   evaluated at "the end of whatever batch arrived" would diverge. Fixed
   checkpoints make it a function of the log alone.
   `maxExpirePerCheckpoint()` is `MaxTotalSessions / 8`, floor 64, which bounds
   draining a full table at eight expiry windows for any capacity.
5. **Classification** (`applyBatch`), per entry:

   | Entry | Handling | Reaches `Update`? |
   |---|---|---|
   | config change, or the bootstrap membership entry | `Result{}`; membership is applied before the adapter | no |
   | no session, empty `Cmd` (leader no-op) | `Result{}` | no |
   | session register | `Register`; result is the client ID, or `Result{}` on refusal | no |
   | session unregister | `Unregister`; result is the client ID, or `ResultSessionExpired` | no |
   | session-managed, known duplicate | owned copy of the cached result | no |
   | session-managed, unknown or stale session | `ResultSessionExpired` | no |
   | session-managed, repeated in the same batch | mirrors the first occurrence's result | no |
   | session-managed, response cache full | `ResultSessionExpired` | no |
   | session-managed, new | admitted, result cached after `Update` | yes |
   | no session, non-empty `Cmd` | forwarded | yes |

   The forwarded entries are converted to `sm.Entry` (copying `Cmd` unless
   zero-copy is on), passed to one `Update` call with `context.Background()`,
   and their results copied back to the original positions. The session rules
   behind each row are in
   [`../client-sessions/engineering.md`](../client-sessions/engineering.md).
   `smEntryBuf`, `smResultBuf` and `indexMapBuf` are reused across calls; that
   is safe because one goroutine applies per shard.

`lastApplied` is atomic because the snapshot pool reads it from another
goroutine. Nothing restores it on resume except an explicit seed: `Open` sets it
from a disk store's own index, and `SetLastApplied` is called by
`Host.StartShard` (beside the engine node's `SeedLastApplied`) and by the
engine's snapshot pool after `RecoverFromSnapshot` installs state. The counter
is both the index of the next snapshot and the lower bound of the expiry walk;
left stale, a snapshot would pair an old index with new state, and the expiry
walk would re-run checkpoints the shard already passed and delete sessions its
peers still hold. The contiguity check is what makes an unseeded counter fail
instead of diverge.

### Read paths

`Lookup`, `NALookup`, `NALookupBuf` and `GetHash` take `readGuard` shared for
the duration of the call into your code and return `ErrClosed` after `Close`
(in-memory stores do not track closure and would otherwise return stale data).

- `Lookup`: a `[]byte` query goes to `NALookup` when implemented, else to
  `Lookup`.
- `NALookupBuf(key) (result []byte, release func(), supported bool, err error)`:
  the path behind `SyncReadBuf`/`StaleReadBuf`. It takes a buffer from a
  `sync.Pool` (initial capacity 256), calls `BytesLookupInto`, and returns the
  filled slice with a `release` function, falling back to `BytesLookup` with a
  no-op `release`. The guard is released before returning, because `release` is
  application code and a caller that never calls it would otherwise pin the
  guard and, once `Close` queued, block every lookup on the shard. `release` is
  wrapped in one `sync.Once` covering the pool `Put`, so a double release cannot
  put one buffer in the pool twice.
- `GetHash`: on tier 0 excludes `Apply`; on tiers 1 and 2 runs alongside it
  under the `Hasher` contract. It does not take `applyMu`, so a long hash never
  stalls applies.

### Snapshots

The adapter writes one stream: the session table, then your state.

```
+--------------------------------------------+
| session table (session.Manager.MarshalTo)  |
|   [numSessions:4]                          |
|   per session, sorted by clientID:         |
|     [clientID:8][ownerID:8]                |
|     [registeredIndex:8][respondedTo:8]     |
|     [numResponses:4]                       |
|     per response, sorted by seriesID:      |
|       [seriesID:8][value:8][dataLen:4][data]|
|     [lastActiveIndex:8]                    |
+--------------------------------------------+
| your SaveSnapshot output                   |
+--------------------------------------------+
```

Little-endian throughout; sorting makes identical tables produce identical
bytes. You never see the session section: your `RecoverFromSnapshot` reader
starts where your `SaveSnapshot` writer started.

**Save.** `SaveSnapshot` is `SaveSnapshotGated` with a nil callback; the engine
uses the gated form, passing the release of its apply gate as `afterCapture`.
The adapter takes `readGuard` (shared), `applyMu` and `streamGuard` (shared),
returns `ErrClosed` or `ErrStopped` if closed or stopped, marshals the session
table, then calls the wrapper's `saveSnapshotGated`. At capture it releases
`applyMu` and `readGuard` together with the engine gate, so applies resume
while the stream continues; `streamGuard` is held to the end so `Close` waits
for the stream. If the save fails before capture, `afterCapture` is not called
and a deferred release covers the engine gate.

**Recover.** `RecoverFromSnapshot` holds `readGuard` (shared) and `applyMu` for
the whole recovery, then:

1. returns `ErrClosed` or `ErrStopped` if closed or stopped;
2. adopts the replicated session policy, because the table is bounds-checked
   against the shard's limits, not this node's (for a replica bootstrapped by
   `InstallSnapshot` this is the first time a policy is known);
3. decodes the session table into a staged manager;
4. calls your `RecoverFromSnapshot`;
5. adopts the staged table only if step 4 succeeded.

Staging makes recovery all-or-nothing. Decoding into the live table first would,
on a failed user restore, leave session metadata from one index beside user
state from another, so a retry could be answered from a cache entry for an
operation never applied, or a duplicate could run twice. The stream is
sequential, so staging costs nothing.

The engine adds its own guarantees around your snapshot (refusing to install a
snapshot at or below the applied index, verifying artifact integrity and failing
closed); see [`../snapshots/engineering.md`](../snapshots/engineering.md).

### Inspection accessor

`StateMachine.Sessions()` returns the live `session.Manager`, which is not safe
for concurrent use. Call any method on it only when no apply or snapshot can be
in flight. It has no production caller; it exists for tests that assert the
limits the apply path resolved.

## Reference KV backends

`pkg/sm/kv` provides two state machines that share a command protocol and
snapshot format and implement `sm.Hasher`, `sm.NALookup` and
`sm.NALookupInto`. They are the shortest worked examples of this contract. There
is no `DiskStateMachine` in this repository.

| Store | Interface | Locking |
|---|---|---|
| `MemoryStore` | `sm.StateMachine` | one `RWMutex` over the map. Inside a shard the adapter already serializes reads against applies; the mutex is kept because the type is usable outside a shard, it guards the decode scratch slice, and it lets `SaveSnapshot` deep-copy under a read lock and serialize with no lock held. |
| `ConcurrentStore` | `sm.ConcurrentStateMachine` | 16 `RWMutex`-guarded buckets chosen by `xxhash64(key) % 16`; see below |

### Command protocol

```
[op:1][keyLen:2 LE][key:keyLen][value:remaining]
```

The header is 3 bytes; `MaxKeyLength` is 65535. Value length is implicit.

| Op | Code | `Update` result |
|---|---|---|
| `OpPut` | 1 | `Value: len(value)` |
| `OpGet` | 2 | no-op, `Value: 0`; reads go through `Lookup` |
| `OpDelete` | 3 | `Value: 1` |
| `OpPutIfAbsent` | 4 | `ResultStored` (1) if this entry created the key, `ResultNotStored` (0) if it existed |
| undecodable payload | | `sm.ResultInvalidCommand`, state unchanged |

`OpPutIfAbsent` is evaluated inside `Update`, against replicated state at that
log position, so every replica reaches the same answer. Deciding on the proposer
(read, then propose a plain put) leaves a window in which two proposers both
win. "Not stored" is a successful application, not an error.

Encoders `EncodePut`, `EncodePutIfAbsent`, `EncodeGet` and `EncodeDelete` return
`*KeyTooLongError` above `MaxKeyLength`. `EncodePutIfAbsent` differs from
`EncodePut` only in the opcode, so an older decoder rejects it instead of
applying it unconditionally. `DecodeCommand` returns `Command{Op, Key, Value}`
with `Key` and `Value` aliasing the input (copy them under zero-copy; both
stores clear their decode scratch after `Update`), and `*InvalidCommandError`
for input under 3 bytes, an opcode outside 1-4, or a key past the end.

`Lookup` accepts `[]byte` or `string` (the value, or `*KeyNotFoundError`) and
`ListQuery{Prefix}` (a sorted, non-nil `[]string` of matching keys). Anything
else is `*InvalidCommandError`. Listing is a read, so there is no `EncodeList`.

### `Update`: decode, then mutate

Pass one decodes every entry and writes `sm.ResultInvalidCommand` for failures,
touching no state. Pass two applies with handlers that cannot fail. No error can
occur after the first mutation, so `Update` is all-or-nothing without a
rollback. Put and PutIfAbsent copy the value before storing.

`MemoryStore` dispatches through a map of `opHandlerFunc`, so adding an opcode
does not touch the loop.

### `ConcurrentStore` locking

| Method | Lock | Scope |
|---|---|---|
| `Update` | write | every bucket the batch touches, all held for the whole apply pass |
| `Lookup` (key), `BytesLookup`, `BytesLookupInto` | read | one bucket |
| `Lookup` (`ListQuery`) | read | all 16 |
| `GetHash` | read | all 16, for the whole computation |
| `PrepareSnapshot` | read | all 16, while deep-copying into one map |
| `RecoverFromSnapshot` | write | all 16, after the stream is fully parsed |
| `Close` | write | one bucket at a time |

Multi-bucket paths lock in ascending index order, so they cannot deadlock.
`Update` holding its buckets for the whole batch and `GetHash` holding all 16
together are what keep reads and hashes from seeing half a batch.
`SaveSnapshot` serializes the cloned map with no locks held and returns
`*InvalidCommandError` if handed anything but a `map[string][]byte`. The clone
costs O(n) memory for the duration of the stream.

### Hash

xxhash64 over pairs in sorted key order, with a uint64 little-endian length
before both key and value (`hashLenPrefixed`, shared by both stores so equal
contents hash equally). Without the length prefixes `{"user": "1"}` and
`{"use": "r1"}` would hash the same. A digest write error returns
`*HashWriteError`.

### Snapshot format

```
[count:4 LE] then per pair: [keyLen:4 LE][key][valLen:4 LE][val]
```

- `MemoryStore` writes pairs in sorted key order, so identical state gives
  identical bytes. `ConcurrentStore` ranges its clone directly and does not;
  recovery is unaffected, only byte-level comparison.
- Both check the stopper per pair and return `context.Canceled` when it closes.
- Recovery parses the whole stream into staging maps before touching live
  state, so a truncated or corrupt snapshot errors and leaves data intact.
- Length prefixes come from a peer and are bounded: `keyLen > MaxKeyLength` or
  `valLen > MaxValueLength` (64 MiB) returns `errCorruptSnapshotLength` without
  allocating, and `count` is capped at `maxSnapshotMapHint` (1,048,576) as a
  capacity hint only. `MaxValueLength` is a denial-of-service bound, well above
  the 8 MiB a value can have when written, so lowering `MaxEntrySize` never
  makes an existing snapshot unrecoverable.

### Factories

```go
func NewMemoryCreateFunc() sm.CreateFunc     // sm.NewCreateFunc(... NewMemoryStore())
func NewConcurrentCreateFunc() sm.CreateFunc // sm.NewConcurrentCreateFunc(... NewConcurrentStore())
```

### Interface assertions

Both files carry compile-time assertions for their tier, `sm.Hasher`,
`sm.NALookup` and `sm.NALookupInto`. Copy the pattern into your own state
machine.

## Configuration

`pkg/sm` and `pkg/sm/kv` read no configuration. The `config.Config` fields that
change how a state machine is driven:

| Field | Default | Effect |
|---|---|---|
| `ZeroCopyEntryCmd` | `false` | when true, `Entry.Cmd` aliases engine memory; copy what you retain |
| `MaxEntrySize` | 8 MiB | cap on one proposal payload and the decompression ceiling on apply; `Validate` rejects values above `proto.MaxEntrySize` (8 MiB) |
| `MaxProposalPayloadSize` | `proto.MaxEntrySize` | user-facing propose limit; the effective limit is `min(MaxProposalPayloadSize, MaxEntrySize)` |
| `MaxApplyEntrySize` | `config.MaxReplicationPayloadBytes` (16,711,672) | bounds one outbound replication message. Despite the name it does not bound an `Update` call; `Validate` refuses larger values. |
| `SnapshotEntries` | 100000 | applied entries between automatic snapshots. Zero selects the default; `config.SnapshotsDisabled` turns automatic snapshots off, which also stops snapshot-directory GC, epoch purging and at-rest re-encryption. |
| `CompactionOverhead` | 5000 | entries kept after compaction; `SnapshotEntries` must exceed it |
| `MaxSnapshotSize` | 4 GiB | maximum snapshot file size |

One `Update` call is bounded by `maxEntriesToApplySize`, a fixed 64 MiB constant
in `pkg/internal/raft/log.go`, with each `Cmd` at most `MaxEntrySize`.

The session limits (`SessionExpiryEntries`, `MaxTotalSessions`,
`MaxSessionsPerClient`, `MaxSessionsPerOwner`) are bootstrap seeds for a
replicated policy; see
[`../client-sessions/engineering.md`](../client-sessions/engineering.md).
Snapshot cadence is covered in
[`../snapshots/engineering.md`](../snapshots/engineering.md).

## Errors

### Adapter (`pkg/internal/rsm/errors.go`)

| Error | When |
|---|---|
| `ErrInvalidSMType` | `CreateFunc` returned a value implementing none of the three interfaces. `StartShard` translates it to `quicraft.ErrInvalidSMType`. |
| `ErrStopped` | the stopper was already closed when a snapshot save or recovery started |
| `ErrClosed` | `Apply`, a read, `GetHash` or a snapshot operation after `Close` |
| `ErrApplyGap` | the batch starts above `lastApplied + 1`; fails the shard |

These live in the internal package to avoid an import cycle with `pkg`.

### Propagation

- An error from your `Update` propagates out of `Apply` and fails the shard;
  every proposal in the failed batch fails with an `*ApplyError` (`ShardID`,
  `ReplicaID`, `Err`) that unwraps to yours, so `errors.Is` finds your sentinel.
- A panic in your code is recovered by the apply worker, logged with its stack,
  and fails the shard; every proposal in the batch fails with the `*SMPanicError`
  itself (`Value`, `Stack`), which matches `ErrShardFailed`. Every other
  proposal pending on the shard fails with `ErrShardFailed`.
- Those entries are committed and a reload replays them, so each of those
  failures is wrapped in `*MaybeAppliedError` and `MaybeApplied(err)` is true.
  Pending reads get the bare error. Requests arriving after the shard has
  failed are refused with the bare `ErrShardFailed`.
- A panic in `SaveSnapshot` is the exception: it only reads your state, so the
  shard keeps running, and the requester gets a `*SnapshotSavePanicError` inside
  its save error, which does not match `ErrShardFailed`.
- `ErrApplyGap` fails the shard, deliberately.
- Session register and unregister failures are per-entry results, never fatal:
  they are expected on replay (a duplicate register after recovery, a double
  unregister from a retry).
- `ErrSessionNotFound` from recording a result is logged and ignored; any other
  error there is fatal.
- Errors from `SaveSnapshot`, `RecoverFromSnapshot` and `Close` propagate to the
  engine.

### `pkg/sm`

`*UnsupportedLookupResultError`, `*InvalidCacheConfigError`,
`*CacheMaxEntriesError`; see [`CachedDiskStateMachine`](#cacheddiskstatemachine).

### `pkg/sm/kv`

| Type | Raised by | When |
|---|---|---|
| `*KeyNotFoundError{Key}` | `Lookup`, `BytesLookup`, `BytesLookupInto` | key absent |
| `*InvalidCommandError{Reason}` | `DecodeCommand` | under 3 bytes, unknown opcode, key past end |
| `*InvalidCommandError{Reason}` | `Lookup` | query not `[]byte`, `string` or `ListQuery` |
| `*InvalidCommandError{Reason}` | `ConcurrentStore.SaveSnapshot` | snapshot context is not `map[string][]byte` |
| `*KeyTooLongError{Length}` | the `Encode*` functions | key over `MaxKeyLength` |
| `*HashWriteError{Err}` | `GetHash` | the digest returned a write error; implements `Unwrap` |

A decode failure inside `Update` is never an error; it is
`sm.ResultInvalidCommand` in that entry's slot.

## Test strategy

Targets: `make test-sm`, `make test-rsm`.

| Layer | Covers |
|---|---|
| Unit (`pkg/sm/...`) | the factory wrappers, `CachedDiskStateMachine`, the KV codec, both reference stores and their snapshot formats |
| Unit (`pkg/internal/rsm`) | type detection, the apply pipeline, tier-dependent locking, session routing, the `(value, supported, error)` helpers, snapshot save and recover |
| Integration (`test/integration`) | multi-node clusters applying to real state machines |
| Linearizability (`make integration-test-linearizability`) | that observed behaviour is linearizable, which no unit test can establish |

A test for your own state machine must cover what the adapter cannot catch:

1. `Update` is deterministic: replaying the same entries on a fresh instance
   gives the same state.
2. `RecoverFromSnapshot` after `SaveSnapshot` round-trips exactly.
3. A `DiskStateMachine` replays already-applied entries idempotently.
4. Optional interfaces are actually detected: assert them at compile time.
5. On the concurrent tiers, run `Lookup` and `GetHash` against `Update` under
   `-race`.

Fault injection: return an error or panic from `Update` to exercise the
fail-fast path.

## Observability

`pkg/sm` and `pkg/sm/kv` emit no logs and no metrics; they run once per applied
entry. The adapter logs at WARN when it answers `ResultSessionExpired`, when the
replicated session policy differs from local configuration, and when a session
disappears mid-batch; it logs at ERROR on `ErrApplyGap`.

| Signal | Source |
|---|---|
| proposal latency | the host's `MetricsCollector` |
| shard failed after an `Update` error or panic | `config.HostConfig` `EventListener.OnShardFailed` |
| cross-replica divergence | `Host.StateMachineHash(shardID)`, which needs `sm.Hasher` |
| last applied index | `Host.GetShardInfo` (`ShardInfo.LastApplied`) |

`StateMachineHash` is the one worth a periodic check: divergence from a
non-deterministic `Update` has no other symptom until a restart replays the log
differently.

Log from inside your own `Update` only at a level you would accept once per
entry, and never log the command payload.

## Cross-references

- [Client sessions](../client-sessions/engineering.md): session rules inside the apply pipeline
- [Raft consensus](../raft-consensus/engineering.md): the apply worker and snapshot pool
- [Snapshots](../snapshots/engineering.md): snapshot transfer, integrity and recovery
- [Multi-group host API](../multi-group-host/api.md): `Host.StartShard`, the read methods, `Host.StateMachineHash`
