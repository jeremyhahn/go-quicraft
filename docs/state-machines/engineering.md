# State Machines — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/sm`, `pkg/sm/kv`,
> `pkg/internal/rsm` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Import paths:
`github.com/jeremyhahn/go-quicraft/pkg/sm` — the three interfaces you
implement, plus the optional capability interfaces and the factory wrappers
`github.com/jeremyhahn/go-quicraft/pkg/sm/kv` — the reference KV
implementations
`pkg/internal/rsm` — the adapter that drives your state machine. **Internal**:
not importable, documented here because its behavior is your contract.

This document has three parts: the interfaces you implement, the adapter that
calls them, and the reference backends. Design rationale and the scope fence are
in [`architecture.md`](architecture.md); task-oriented guidance is in
[`user.md`](user.md).

## Overview

QuicRaft defines three state machine interfaces in `pkg/sm`. You implement exactly one and hand `Host.StartShard` a factory that returns it. QuicRaft owns consensus, replication, snapshotting and recovery; your state machine only ever sees committed entries and never touches the Raft protocol.

| Tier | Interface | Snapshot phases | Persistence | Client sessions |
|------|-----------|-----------------|-------------|-----------------|
| In-memory | `StateMachine` | `SaveSnapshot` | none, the snapshot is the whole state | supported |
| Concurrent | `ConcurrentStateMachine` | `PrepareSnapshot` then `SaveSnapshot` | none | supported |
| On-disk | `DiskStateMachine` | `Sync`, `PrepareSnapshot`, then `SaveSnapshot` | your own store | **not supported**, see below |

Source: `pkg/sm/interfaces.go`.

## Determinism is the whole contract

`Update` must be a pure function of (current state, entries). Every replica applies the identical entry sequence and must end up in byte-identical state. Concretely, `Update` must not read the wall clock, `math/rand`, environment variables, host identity, `GOMAXPROCS` or anything else that varies per node, and it must not let Go's randomized map iteration order affect either the state it produces or the `Result` values it writes.

Nothing in Raft catches a violation. Consensus keeps agreeing on the log while the replicas silently diverge in the state they derive from it. The usual first symptom is a linearizable read returning different values depending on which replica served it, long after the entry that caused it was compacted away. If you want a detector, implement `Hasher` and compare replicas at the same applied index with `Host.StateMachineHash` (`pkg/host.go:9789`).

The same rule applies to all three tiers.

## StateMachine

```go
type StateMachine interface {
	Update(ctx context.Context, entries []Entry, results []Result) error
	Lookup(ctx context.Context, query interface{}) (interface{}, error)
	SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
	Close(ctx context.Context) error
}
```

`Update` applies committed entries. `results` is pre-allocated with `len(results) == len(entries)` and you must write `results[i]` for every `entries[i]`. Return an error only for a failure you cannot recover from: the engine treats an `Update` error as terminal for the shard, and because the entry is already committed every replica will hit it on replay. A payload you cannot decode is not a fatal failure, it is a per-entry outcome. Write `sm.ResultInvalidCommand` into `results[i]` and leave your state alone, which is what both reference KV stores do (`pkg/sm/kv/memory.go:104`).

`Lookup` reads state without going through Raft. It runs on the caller's goroutine, not on the apply worker, but the adapter holds a write lock across the whole of `Update`, so the two never overlap. You do not need a mutex to separate them. See [Lookup concurrency](#lookup-concurrency) for what that serialization costs.

`SaveSnapshot` writes the entire state. There is no persistent backing store, so the snapshot is the only thing that survives log compaction.

`RecoverFromSnapshot` replaces all existing state with what it reads.

`Close` releases resources. The engine stops the apply worker before calling it, and the adapter takes a write lock to drain in-flight reads and mark itself closed before it reaches your `Close`, so no read is in progress when it runs. Reads that arrive afterwards get `ErrClosed` rather than stale data.

For this tier the whole `SaveSnapshot` call runs with applies quiesced: the snapshot pool sets the node's saving gate and waits for the apply drain before it starts (`pkg/internal/engine/snapshot_pool.go:693-708`), and the gate is released only after the save returns (`pkg/internal/rsm/adapter.go:141`). `Update` therefore cannot run concurrently with `SaveSnapshot` on a regular state machine. `Lookup` still can.

## ConcurrentStateMachine

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

`PrepareSnapshot` captures a point-in-time view and returns it as an opaque value. That value is passed unchanged to `SaveSnapshot`.

The split exists so a large snapshot does not pause applies for the length of the stream. `PrepareSnapshot` runs under the engine's saving gate, with applies drained. As soon as it returns, the adapter fires the `afterCapture` callback that releases the gate, and applies resume while `SaveSnapshot` streams the captured value (`pkg/internal/rsm/adapter.go:215`). Two consequences for your implementation:

- Whatever `PrepareSnapshot` returns must be immutable, or at least unaffected by subsequent `Update` calls. A map you hand back by reference and then mutate produces a torn snapshot.
- `SaveSnapshot` is contracted to run concurrently with `Update`, so it must not touch live state or take a lock that `Update` holds.

One caveat about the current adapter. `rsm.StateMachine.SaveSnapshotGated` holds the read guard for the whole save, streaming included, and `Apply` now takes that guard for writing. So while the engine's saving gate is released at `afterCapture` as described, an apply that arrives during the stream still blocks on the guard until the stream finishes, and reads queue behind it. Write your `SaveSnapshot` to the contract above regardless; the gate release is what the design intends and what the adapter is expected to deliver.

Three shapes work. Deep copy everything in `PrepareSnapshot` (O(n) memory, simplest, what `kv.ConcurrentStore` does). Take a copy-on-write view, so writes after the prepare allocate new backing storage. Or, if a database with snapshot isolation backs you, return its snapshot handle.

## DiskStateMachine

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

`Open` initializes the store and returns the last applied index, or 0 on a fresh start. `initialDir` is the working directory QuicRaft assigns. The returned index is stored on the managed state machine and becomes the floor for everything that follows (`pkg/internal/rsm/statemachine.go:351`).

An on-disk state machine can be handed entries it already applied: the SM committed its own write, then the node crashed before QuicRaft persisted the matching `lastApplied`. Replay must be idempotent. The pattern that works is to persist your applied index atomically with the data, in your own store, and return that value from `Open`. Your database's own WAL then does the crash-consistency work.

`Sync` flushes pending writes to durable storage. The adapter calls it before `PrepareSnapshot` so the captured point-in-time is one that survives a crash (`pkg/internal/rsm/adapter.go:291`).

### Client sessions are not available on this tier

Every session entry point on `Host` refuses a shard backed by a `DiskStateMachine` with `ErrSessionsUnsupportedOnDisk` (`pkg/host.go:4986`): `GetNewSession`, `RegisterSession`, `ProposeWithSession`, `SyncProposeWithSession`, `CloseSession`. Use idempotent operations proposed through a no-op session instead.

The reason is `Open`. The session table is replicated state that travels only inside the snapshot stream, so it is restored by `RecoverFromSnapshot`. A `DiskStateMachine` that reports a non-zero applied index from `Open` never takes that path on restart, because recovering would overwrite its database files and roll it back to the snapshot index. The replica would come up with correct data and no sessions, then answer `ResultSessionExpired` for entries its peers apply normally. Dragonboat's `IOnDiskStateMachine` has the same constraint.

## Supporting types

### Entry

```go
type Entry struct {
	Index uint64
	Cmd   []byte
}

func (e *Entry) CopyCmd() []byte
```

`Index` is the Raft log index. `Cmd` is the proposal payload.

This is a narrowed view of the internal `proto.Entry`, whose header is 68 bytes and carries `Term`, `Type`, `Key`, `ClientID`, `SeriesID`, `RespondedTo` and `OwnerID` (`pkg/proto/entry.go:86`). None of those reach you. `Term` in particular is withheld on purpose: the same entry at the same index must produce the same result regardless of which term proposed it.

**Indices are not contiguous across a batch.** The adapter drops config changes, the bootstrap initial-membership entry, the leader's empty no-op entry, session register/unregister entries and anything it recognizes as a session duplicate before it builds the `[]Entry` you receive (`pkg/internal/rsm/statemachine.go:576-778`). Indices are strictly increasing and never move backwards, but gaps are normal. Do not use `entries[i].Index` to derive a count or to assert contiguity.

**`Update` may be called more than once per committed batch.** When session expiry is enabled, `Apply` splits the batch at fixed log-index checkpoints and calls your `Update` once per segment so expiry always observes a fully applied state at the checkpoint index (`pkg/internal/rsm/statemachine.go:485-504`). Each call is still all-or-nothing on its own segment.

### Buffer ownership of `Cmd`

There are two aliasing layers, and only the outer one is yours to worry about.

Underneath, `proto.Entry.Cmd` aliases the buffer it was decoded from: `UnmarshalFrom` does not copy the payload, and the engine recycles both the decode buffer and the pooled `CommittedEntries` slice after the apply completes (`pkg/proto/entry.go:79`).

On top of that, `Config.ZeroCopyEntryCmd` decides what you get:

- **`false` (default).** For every non-empty `Cmd` the adapter allocates a fresh owned copy before it builds your `sm.Entry`. You may retain the slice past `Update`.
- **`true`.** Your `sm.Entry.Cmd` aliases the engine's buffer directly, at zero allocations. You must copy anything you keep past the return of `Update`, because the engine recycles that memory immediately afterwards.

The same rule reaches `Result.Data` in zero-copy mode. If your `Update` returns a `Result` whose `Data` points into `Cmd`, that memory is also recycled. The adapter deep-copies `Data` before caching it in the session table so at-most-once delivery is not corrupted (`pkg/internal/rsm/statemachine.go:815`), but the copy handed to the client is the one you returned, so it is still yours to keep valid.

Safe in either mode:

```go
func (c *Counter) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, e := range entries {
		if len(e.Cmd) < 4 {
			results[i] = sm.Result{Value: sm.ResultInvalidCommand}
			continue
		}
		key := string(e.Cmd[:4])          // string conversion copies
		val := make([]byte, len(e.Cmd[4:]))
		copy(val, e.Cmd[4:])              // explicit copy
		c.store[key] = val
		results[i] = sm.Result{Value: uint64(len(val))}
	}
	return nil
}
```

Unsafe under `ZeroCopyEntryCmd`, and the failure is silent data corruption some batches later:

```go
c.pending = append(c.pending, e.Cmd)     // retains an alias the engine will recycle
c.store[string(e.Cmd[:4])] = e.Cmd[4:]   // same problem, one level down
```

Use `CopyCmd()` when you want the whole payload:

```go
for i := range entries {
	r.pending = append(r.pending, entries[i].CopyCmd())
}
```

`CopyCmd` returns `nil` for an empty `Cmd` rather than an empty non-nil slice.

### Result

```go
type Result struct {
	Value uint64
	Data  []byte
}
```

`Value` is your own result code. `Data` carries optional payload back to the proposer through `RequestState.Result()`.

Two values at the top of the `uint64` range are reserved by the engine and application code must not use them (`pkg/sm/types.go:26`):

| Constant | Value | Meaning |
|----------|-------|---------|
| `sm.ResultSessionExpired` | `math.MaxUint64 - 2` | set by the RSM when a session-managed entry names an unregistered client. The host turns it into `ErrSessionExpired`, so the caller can tell it apart from a legitimate zero result. |
| `sm.ResultInvalidCommand` | `math.MaxUint64 - 3` | set by a state machine when a committed payload cannot be decoded. State is left unchanged for that entry. |

The `Data` slice you write into `results[i]` is handed to the client as-is. Do not hand out a slice you will mutate later.

## Update: all-or-nothing

Either every entry in the call is applied and every result written, or the state machine is back at its pre-call state and you return an error. Partial application is not supported, because a replica that applied half a batch and reported failure has no way to be told where to resume.

Patterns that get you there:

1. Wrap the batch in a database transaction and roll back on failure.
2. Decode and validate every entry in a first pass, then mutate in a second pass that cannot fail. Both reference KV stores do this (`pkg/sm/kv/concurrent.go:90`), which is also why an undecodable payload becomes `ResultInvalidCommand` instead of an error.
3. For append-only stores, record the pre-call position and truncate on failure.
4. For small in-memory state, snapshot before applying and restore on failure.

The engine pre-allocates one `[]sm.Result` per shard and reuses it across batches, so the slice you are handed is not fresh memory. Write every element you are responsible for; do not assume the zero value is already there.

## Lookup concurrency

**A basic `sm.StateMachine` never sees `Update` and `Lookup` overlap.** That is the contract `pkg/sm/interfaces.go` publishes, and the adapter enforces it: `rsm.StateMachine.Apply` takes the `readGuard` mutex for writing, while `Lookup`, `NALookup`, `NALookupBuf` and `GetHash` take it for reading. An apply waits out in-flight reads; a read waits out an in-flight apply. If separating those two was your only reason for a mutex, you do not need one.

`Host.SyncRead`, `Host.FollowerRead`, `Host.QueryLocalNode` and `Host.StaleRead` all reach the adapter's `Lookup` on the calling goroutine rather than on the apply worker. The serialization comes from the guard, not from those callers.

**`ConcurrentStateMachine` and `DiskStateMachine` do see them overlap.** Both publish the opposite contract — `Lookup` may be called concurrently with `Update`, and the implementation synchronizes itself — and the adapter honours it: on those tiers `Apply` takes the `readGuard` for reading, which keeps `Close` out and nothing else. A long read on those tiers, such as a range scan or an export, no longer holds up the shard's applies (#209). Your state machine's own locking is what keeps a `Lookup` from observing a half-applied batch.

`kv.MemoryStore` keeps its `RWMutex`, but not for this reason any more. See [MemoryStore](engineering.md#memorystore).

### What the exclusion costs

A local read parks behind an in-flight `Apply`. On a follower catching up, that apply can carry as much as `maxEntriesToApplySize` (64 MiB) of entries, and `sync.RWMutex` has no context-aware acquire, so the deadline on the context you passed to `SyncRead` is not observed while the goroutine sits on the lock. The ReadIndex wait ahead of the lock honours it, and your `Lookup` receives the same context once the lock is taken, but nothing cancels the wait itself.

Go's `RWMutex` also blocks new readers once a writer is queued, so a single apply behind a slow read stalls every subsequent read on the shard until it drains.

`ConcurrentStateMachine` is where an implementation that needs finer-grained read concurrency belongs; offering that choice is why the tier exists. On that tier, and on `DiskStateMachine`, neither wait applies: a read does not queue behind an apply, and an apply does not queue behind a read.

What differs per tier:

| Tier | `Update` vs `Lookup` | `Update` vs `SaveSnapshot` |
|------|----------------------|----------------------------|
| `StateMachine` | serialized by the adapter's guard, as the interface promises | serialized: the whole save runs under the engine's apply gate |
| `ConcurrentStateMachine` | concurrent; the adapter's guard excludes only `Close` | concurrent after `PrepareSnapshot` returns |
| `DiskStateMachine` | concurrent; the adapter's guard excludes only `Close` | concurrent after `PrepareSnapshot` returns |

`StaleRead` and `QueryLocalNode` bypass the ReadIndex protocol entirely and return whatever the local replica has applied so far. `SyncRead` and `FollowerRead` add a consistency check before calling the same `Lookup`.

## Optional interfaces

The adapter finds these by type assertion on the value your `CreateFunc` returned. None are required.

### Hasher

```go
type Hasher interface {
	GetHash() (uint64, error)
}
```

Returns a deterministic hash of the state machine's state, surfaced by `Host.StateMachineHash(shardID) (hash, appliedIndex uint64, ok bool, err error)`. The hash must be order-independent, or it reports divergence that does not exist; both reference stores sort their keys and length-prefix each field before hashing (`pkg/sm/kv/memory.go:239`). Compare replicas at the same `appliedIndex`, which is why the method returns it.

### NALookup

```go
type NALookup interface {
	BytesLookup(key []byte) ([]byte, error)
}
```

A byte-slice read path that skips the `interface{}` boxing of `Lookup`. When the query passed to `rsm.StateMachine.Lookup` is a `[]byte` and you implement this, it is used instead (`pkg/internal/rsm/statemachine.go:883`).

### NALookupInto

```go
type NALookupInto interface {
	NALookup
	BytesLookupInto(key, dst []byte) ([]byte, error)
}
```

Same idea, but the caller supplies the destination buffer. You must copy the value into `dst`, growing it with `append` if needed, and return the populated sub-slice. The returned slice belongs to the caller; do not retain a reference after returning.

The copy is load-bearing, not a suggestion. `Host.SyncReadBuf` and `Host.StaleReadBuf` hand the buffer to application code along with a `release` function, and the adapter drops its read lock before returning precisely because the contract guarantees the bytes do not alias state machine memory (`pkg/internal/rsm/statemachine.go:918`). An implementation that returns an internal slice instead lets a reader observe a value being rewritten underneath it, after `Close` has already run.

```go
result, release, err := host.SyncReadBuf(ctx, shardID, key)
if err != nil {
	return err
}
defer release()
// use result before returning; it must not escape the release
```

Calling `release` twice is safe (it is guarded by a `sync.Once`). Never calling it leaks a pooled buffer; using `result` after it hands the next reader your bytes.

## CachedDiskStateMachine

`pkg/sm/cached.go` wraps a `DiskStateMachine` in a write-through LRU cache, turning repeat reads of hot keys into memory lookups. In-memory tiers get nothing from it, which is why there is no equivalent wrapper for them.

```go
type CacheConfig struct {
	MaxEntries   int              // > 0, required
	ExtractKV    EntryKeyFunc     // required
	EntryApplied EntryAppliedFunc // optional
}

type EntryKeyFunc func(cmd []byte) (key string, value []byte)
type EntryAppliedFunc func(cmd []byte, result Result) bool

func NewCachedDiskStateMachine(inner DiskStateMachine, cfg CacheConfig) (*CachedDiskStateMachine, error)
```

`ExtractKV` pulls the cache key and value out of a command's bytes. Return `("", nil)` to skip an entry. A non-empty key with a `nil` value means "delete".

`EntryApplied` is what makes conditional writes safe. The cache is populated from the value in the *command*, not from the store. For an unconditional put those agree; for a compare-and-set, a put-if-absent or a delete-if-match they do not, because the command carries a value the store may have declined. Without the predicate a later `Lookup` can return a value the store never held. Nothing can infer the answer for you, since the outcome encoding is your own: `kv.OpPutIfAbsent` reports its verdict as `kv.ResultStored` versus `kv.ResultNotStored`, and both are successful applications.

`sm.ResultInvalidCommand` is refused whether or not you set the predicate.

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

The wrapper invalidates affected keys *before* the inner write and repopulates after, so a `Lookup` racing an `Update` degrades to a cache miss rather than a stale hit. It clears the cache on `Open`, `RecoverFromSnapshot` and `Close`, copies values on the way in and on the way out, and delegates `GetHash` to the inner store when that implements `Hasher`. `CacheStats() (hits, misses uint64)` reports cumulative counters.

If the inner store's `Lookup` answers `BytesLookup` with something other than `[]byte`, you get `*sm.UnsupportedLookupResultError` naming the type rather than an empty result, since "nil, nil" is indistinguishable from a genuinely empty value.

## Factory functions

```go
type CreateFunc func(shardID, replicaID uint64) interface{}
```

One factory type for all three tiers. `NewStateMachine` calls it once at shard start and type-switches the result, most specific first, so a `DiskStateMachine` is not mistaken for a `ConcurrentStateMachine` whose method set it contains. `StartShard` returns `quicraft.ErrInvalidSMType` when nothing matches (`pkg/host.go:3494`).

Prefer the typed wrappers, which turn a mismatch into a compile error:

```go
type CreateStateMachineFunc           func(shardID, replicaID uint64) StateMachine
type CreateConcurrentStateMachineFunc func(shardID, replicaID uint64) ConcurrentStateMachine
type CreateDiskStateMachineFunc       func(shardID, replicaID uint64) DiskStateMachine

func NewCreateFunc(fn CreateStateMachineFunc) CreateFunc
func NewConcurrentCreateFunc(fn CreateConcurrentStateMachineFunc) CreateFunc
func NewDiskCreateFunc(fn CreateDiskStateMachineFunc) CreateFunc
```

### Wiring a shard

```go
func (h *Host) StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error
```

`members` maps replica ID to address for the initial voting membership; `join` says whether this replica is joining an existing shard rather than founding one; `cfg.ShardID` and `cfg.ReplicaID` must both be non-zero. There is no discovery layer, and a replica configured as a witness or observer must not appear in `members`.

```go
members := map[uint64]string{1: "10.0.0.1:4001", 2: "10.0.0.2:4001", 3: "10.0.0.3:4001"}
cfg := config.Config{ShardID: 100, ReplicaID: 1}

err := host.StartShard(members, false, sm.NewCreateFunc(func(_, _ uint64) sm.StateMachine {
	return NewMyKVStore()
}), cfg)

err = host.StartShard(members, false, sm.NewDiskCreateFunc(func(_, _ uint64) sm.DiskStateMachine {
	return NewMyPebbleStore(opts)
}), cfg)
```

The factory receives `shardID` and `replicaID`, so one `CreateFunc` can serve many shards.

## Reference KV backends

`pkg/sm/kv` ships two implementations, both of which also satisfy `Hasher`, `NALookup` and `NALookupInto`:

- `MemoryStore` implements `StateMachine` over an `RWMutex`-guarded map. Use it for tests, small datasets, and as the shortest path to a working shard.
- `ConcurrentStore` implements `ConcurrentStateMachine` over 16 `RWMutex`-guarded buckets selected by `xxhash64(key) % 16`. Use it for read-heavy workloads.

```go
func NewMemoryCreateFunc() sm.CreateFunc
func NewConcurrentCreateFunc() sm.CreateFunc
```

There is no `DiskStateMachine` backend in this repository. The PebbleDB-backed store lives in [go-qrdb](https://github.com/jeremyhahn/go-qrdb).

Command encoding, snapshot format, result codes and error types are documented in [KV backends](engineering.md).

## Snapshots

The RSM adapter writes one stream containing session state followed by your state:

```
+-----------------------------------+
| session table (Manager.MarshalTo) |
+-----------------------------------+
| your SaveSnapshot output          |
+-----------------------------------+
```

You never see the session section. Your `RecoverFromSnapshot` reader is positioned exactly where your `SaveSnapshot` writer started, whether or not the session table was ultimately adopted.

Invariants the engine enforces around your snapshot, each of which closes a defect that has actually occurred:

- **A snapshot whose data file cannot be measured fails the save.** Recording an artifact with `FileSize` 0 made the replica stream an empty snapshot once it became leader (`pkg/internal/engine/snapshot_pool.go:1217`).
- **Recovery is refused when the snapshot index is at or below the local applied index**, with `SnapshotRecoveryStaleError`. Installing it would roll the state machine backwards and regress `lastApplied`, which fails the shard with `ErrApplyGap` on the next batch (`pkg/internal/engine/snapshot_pool.go:1432`).
- **Recovery is refused when the raft log has not yet installed the snapshot**, with `SnapshotRecoveryBehindError`. That one is merely early, not wrong, and is retried.
- **Artifact integrity is verified before anything reads it, and fails closed.** Falling back to an older snapshot directory is not the remedy, because that also rolls the replica backwards (`pkg/internal/engine/snapshot_pool.go:1611`).
- **Session recovery is staged.** The session table is decoded into a staging manager and adopted only after your `RecoverFromSnapshot` succeeds, so a truncated or rejected payload cannot leave session metadata from index M beside user state from index N (`pkg/internal/rsm/statemachine.go:1076`).

Both `SaveSnapshot` and `RecoverFromSnapshot` are refused with `ErrStopped` if the stopper channel is already closed when they are entered. Check it periodically inside long-running loops; the reference stores check it once per key-value pair and return `context.Canceled`.

## Configuration reference

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `ZeroCopyEntryCmd` | `bool` | `false` | when true, `Entry.Cmd` aliases engine memory and you must copy anything you retain |
| `MaxEntrySize` | `uint64` | 8 MiB | ceiling on a single proposal payload, and the decompression ceiling on apply. `Validate` rejects anything above `proto.MaxEntrySize` (8 MiB). |
| `MaxProposalPayloadSize` | `uint64` | `proto.MaxEntrySize` | user-facing propose limit; the effective limit is `min(MaxProposalPayloadSize, MaxEntrySize)` |
| `MaxApplyEntrySize` | `uint64` | `16711672` (~15.94 MiB) | bounds the leader's replication payload (raft's `maxReplicationPayload`). The default is `config.MaxReplicationPayloadBytes` and `Validate` refuses anything larger, so 64 MiB is not a value this field can legally hold. |
| `SessionExpiryEntries` | `uint64` | `100000` | bootstrap seed for the shard's replicated expiry threshold. `config.SessionExpiryDisabled` (`math.MaxUint64`) turns expiry off; zero means "not chosen" and selects the default. Values below 100 are rejected. |
| `MaxTotalSessions` | `uint64` | `16384` | seed for the shard-wide session cap |
| `MaxSessionsPerClient` | `uint64` | `128` | seed for the **response-cache depth** per session. The name is wrong and is kept because renaming a public field would break compatibility; read it as "max responses per session". |
| `MaxSessionsPerOwner` | `uint64` | `128` | seed for the per-owner concurrent-session cap. Sessions registered without an owner (`proto.Entry.OwnerID == 0`) are exempt. `config.SessionsPerOwnerUnlimited` disables it. |
| `SnapshotEntries` | `uint64` | `100000` | applied entries between automatic snapshots. `config.SnapshotsDisabled` turns it off, which also stops snapshot-directory GC, epoch purging and at-rest re-encryption. |

The single `Update` call is bounded by neither `MaxEntrySize` nor `MaxApplyEntrySize`. Raft caps one batch of committed-but-unapplied entries at `maxEntriesToApplySize`, a package constant of 64 MiB (`pkg/internal/raft/log.go:27`), and each individual `Cmd` inside it is at most `MaxEntrySize`.

The four session limits are seeds only. They are written into the shard's initial membership at bootstrap as `proto.SessionPolicy` and replicated from then on; the apply path reads the committed value and logs a warning when a node's own configuration disagrees (`pkg/internal/rsm/statemachine.go:311`). All founders must supply the same values or their index-1 entries differ. There is currently no config change that alters a committed policy.

## Errors

### From the adapter (`pkg/internal/rsm/errors.go`)

| Error | Condition |
|-------|-----------|
| `ErrInvalidSMType` | `CreateFunc` returned something that implements none of the three interfaces. Surfaced from `StartShard` as `quicraft.ErrInvalidSMType`. |
| `ErrStopped` | stopper channel was already closed when a snapshot operation started |
| `ErrClosed` | `Apply`, `Lookup` or a snapshot operation was attempted on a closed state machine |
| `ErrApplyGap` | the offered batch does not start at `lastApplied + 1`. Either committed entries were dropped, or `lastApplied` was never seeded from the index the shard resumed at. Fatal for the shard by design. |

### From the session layer (`pkg/internal/session/errors.go`)

`ErrSessionLimitExceeded`, `ErrSessionNotFound`, `ErrSessionAlreadyExists`, `ErrResponseLimitExceeded`, `ErrResponseEvicted`, `ErrSnapshotCorrupted`. None of these are yours to handle: the adapter turns them into `Result` values or handles them inline. See [session deduplication](../client-sessions/engineering.md).

### From the KV backends (`pkg/sm/kv/errors.go`)

`*KeyNotFoundError`, `*InvalidCommandError`, `*KeyTooLongError`, `*HashWriteError`. Documented in [KV backends](engineering.md).

## Cross-references

- [RSM adapter](engineering.md), the layer between the apply pipeline and your code
- [KV backends](engineering.md), the two reference implementations
- [Engine pipeline](../raft-consensus/engineering.md), the apply worker that calls `rsm.StateMachine.Apply`
- [Session deduplication](../client-sessions/engineering.md), the at-most-once protocol
- [Public API](../multi-group-host/api.md), `Host.StartShard`, the read methods, `Host.StateMachineHash`
- [Architecture overview](../multi-group-host/architecture.md)

---

## The RSM adapter layer


### Purpose

The RSM adapter (`pkg/internal/rsm`) sits between the engine's apply pipeline and user-implemented state machines. You implement one of the three public interfaces in `pkg/sm`; the adapter wraps whichever it is behind an unexported `smWrapper`, so the pipeline calls one `Apply` method without caring which tier is underneath.

Beyond delegation it owns four things:

1. **Session lifecycle.** Register and unregister entries go to the session manager, not to your state machine.
2. **Duplicate detection.** The session cache is consulted before a proposal reaches your state machine, and an admission gate runs before that.
3. **Deterministic session expiry.** The committed batch is split at fixed log-index checkpoints so expiry cannot depend on how a replica happened to group entries.
4. **Snapshot coordination.** Session state is serialized alongside your state in one stream, with a gate that releases applies as soon as the point-in-time is captured.

Source files:

| File | Role |
|------|------|
| `pkg/internal/rsm/statemachine.go` | `StateMachine` struct, `Apply`, `applyBatch`, lookups, snapshot, `wrapStateMachine` |
| `pkg/internal/rsm/adapter.go` | `smWrapper`, the three wrapper types, the shared optional-interface helpers |
| `pkg/internal/rsm/errors.go` | sentinel errors |
| `pkg/sm/interfaces.go` | the three public interfaces plus `Hasher`, `NALookup`, `NALookupInto` |
| `pkg/sm/types.go` | `Entry`, `Result`, the reserved result constants |
| `pkg/sm/createfunc.go` | `CreateFunc` and the typed wrappers |
| `pkg/sm/cached.go`, `pkg/sm/lru.go` | `CachedDiskStateMachine` and its LRU |
| `pkg/internal/session/manager.go` | session manager |
| `pkg/internal/session/session.go` | per-client session state |

### Type detection

#### Why the order matters

`DiskStateMachine`'s method set is a strict superset of `ConcurrentStateMachine`'s: it has `Update`, `Lookup`, `PrepareSnapshot`, `SaveSnapshot`, `RecoverFromSnapshot` and `Close`, plus `Open` and `Sync`. A type switch that checked `ConcurrentStateMachine` first would match an on-disk state machine and wrap it with the wrong adapter, silently dropping `Open` and `Sync`. Dropping `Open` means the applied index the store recovered from its own durable state is never adopted, and the replica re-applies from wherever the log happens to start.

#### wrapStateMachine

```go
func wrapStateMachine(raw interface{}) (smWrapper, SMType, error) {
	switch typed := raw.(type) {
	case sm.DiskStateMachine:       // most specific: Open + Sync
		return &onDiskSMWrapper{sm: typed}, SMTypeOnDisk, nil
	case sm.ConcurrentStateMachine: // middle: PrepareSnapshot
		return &concurrentSMWrapper{sm: typed}, SMTypeConcurrent, nil
	case sm.StateMachine:           // least specific
		return &regularSMWrapper{sm: typed}, SMTypeRegular, nil
	default:
		return nil, 0, ErrInvalidSMType
	}
}
```

| Code | Constant | `String()` | Interface |
|------|----------|-----------|-----------|
| 0 | `SMTypeRegular` | `"regular"` | `StateMachine` |
| 1 | `SMTypeConcurrent` | `"concurrent"` | `ConcurrentStateMachine` |
| 2 | `SMTypeOnDisk` | `"on-disk"` | `DiskStateMachine` |

`SMType.TypeCode()` returns the numeric code for serialization into `logdb.Bootstrap.Type` and `ShardInfo.StateMachineType`. `proto.StateMachineTypeName(uint64)` converts a wire-format code back to the readable name.

#### Construction

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

The factory is called once, `wrapStateMachine` runs once on its result, and every later call is a direct method call on the concrete wrapper.

The two policy arguments are deliberately asymmetric. `local` is this node's own configuration and is used only while the shard has no replicated policy of its own: a shard bootstrapped before the policy became replicated state, or a `join=true` replica that has not yet received the shard's configuration. `replicated` returns the shard's committed policy and always wins, even when it is the looser of the two. That is not a preference, it is the only choice that terminates. A replica that enforced its own stricter limit would refuse registrations its peers admit and then answer `ResultSessionExpired` for entries the rest of the shard applies normally. A local limit can only be proposed, at bootstrap.

The policy also travels as one value rather than as adjacent `uint64` parameters, because the previous signature let a caller transpose `MaxTotalSessions` and `MaxSessionsPerClient` and compile cleanly, capping a shard at a response depth or letting one client pin the whole session table.

`refreshSessionPolicy` re-reads the replicated policy at the top of every `Apply` and again before snapshot recovery, so a replica that learns its configuration late (a joiner replicating the shard's `EntryInitialMembership`, or one adopting an `InstallSnapshot`) converges without a restart. This is safe only because the replicated policy is bootstrap-only. If a config change that alters it is ever added, this has to become adoption from the apply stream itself, decoded out of the committed entry at the exact index where it applies, or two replicas with a lagging apply path would switch policies at different points in the log.

### The internal wrapper interface

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

> **This is not the interface you implement.** `smWrapper` is unexported and lives in `pkg/internal/rsm`, so it is not importable. The three-value `(value, supported, error)` shape belongs to the wrapper: the `bool` reports whether *your* state machine implemented the optional interface, and it comes from the wrapper's own type assertion.
>
> What you implement is two-valued. `sm.Hasher` is `GetHash() (uint64, error)` and `sm.NALookup` is `BytesLookup(key []byte) ([]byte, error)`. Writing the three-value form satisfies nothing, produces no compile error, and leaves the optimized path silently never invoked. See [State machine interfaces](engineering.md#optional-interfaces).

`saveSnapshotGated` writes your snapshot and invokes `afterCapture` exactly once, after the point-in-time is captured and before the streaming begins. For a regular in-memory state machine the whole save *is* the capture, so it fires at the end. For concurrent and on-disk machines it fires right after `PrepareSnapshot`, which lets the caller release the apply gate while the immutable snapshot streams.

### Apply pipeline

`Apply` is called by the engine's apply worker with the committed batch and a pre-allocated `[]sm.Result` of the same length.

#### Preconditions

Two checks run before a single entry is classified.

**Read guard, mode by tier, then `applyMu`.** `Apply` takes the state machine's `readGuard` and returns `ErrClosed` if `Close` already ran. On a basic `sm.StateMachine` it takes the guard in **write** mode; on `ConcurrentStateMachine` and `DiskStateMachine` in **read** mode.

The write mode is the enforcement of `sm.StateMachine`'s published contract: QuicRaft serializes all method calls, and `Update` and `Lookup` are never called concurrently. Every read entry point takes the same guard for reading, so an apply excludes them all. The other two tiers publish that `Lookup` may run concurrently with `Update`, so their apply takes the read mode and excludes only `Close`. See [Lookup concurrency](engineering.md#lookup-concurrency) for the tier-by-tier consequences and the latency the write mode adds.

`Apply` then takes `applyMu`, a plain mutex that serializes everything owning the apply stream's state: `Apply`, the capture phase of `SaveSnapshotGated`, and `RecoverFromSnapshot`. That state is the session table (not safe for concurrent use), the session policy and the expiry checkpoint walk. Reads never take it. The lock order is `readGuard`, then `applyMu`, then `streamGuard`.

The guard also carries the closed check. The engine's synchronous `UnloadNode` guarantees no `Apply` is in flight when `Close` runs, so that part is defence in depth, but holding the guard forces `Close` to wait out an in-flight `Apply` rather than closing the state machine underneath it.

**Contiguity guard.** The batch must not start above `lastApplied + 1`, or `Apply` returns `ErrApplyGap` and fails the shard.

`lastApplied` is not bookkeeping here. It is the lower bound of the expiry checkpoint walk below, so a batch that leaves a hole beneath it makes this replica evaluate expiry over a different index range than the replicas that applied the same log contiguously. That diverges the state machines with every Raft invariant intact. A hole has two causes and both are fatal: committed entries were dropped before reaching the state machine, or `lastApplied` was never seeded from the index the shard resumed at. Failing loudly is the point; the alternative is a replica that quietly answers `ResultSessionExpired` for entries its peers apply normally.

The engine's apply worker makes the same assertion one layer down against its own counter, as an equality rather than an upper bound, because the two layers see different streams: the worker filters entries at or below its marker before checking, whereas an already-applied entry that reaches the adapter is the documented on-disk replay case and stays a no-op (`pkg/internal/engine/apply.go:718`).

#### Deterministic expiry segmentation

Apply batching is replica-local. It varies with message timing, restart replay and the recovery skip-prefix filter. Evaluating session expiry at the last index of whatever batch happened to arrive would let two replicas expire a session at different points and diverge permanently.

So `Apply` evaluates expiry only at fixed log-index checkpoints, at multiples of the shard's replicated inactivity threshold, and splits the incoming entries at those checkpoints:

```
interval  = sessionPolicy.ExpiryEntries
prevIndex = lastApplied
for k := prevIndex/interval + 1; k <= lastIndex/interval; k++ {
    boundary = k * interval
    applyBatch(entries at or below boundary)   // flush first
    sessions.ExpireSessions(boundary, interval, maxExpirePerCheckpoint())
}
applyBatch(remaining entries above the last boundary)
advanceLastApplied(lastIndex)
```

Each expiry therefore runs against session state as of exactly the entries at or below the checkpoint index. When expiry is disabled (`ExpiryEntries == 0`) the whole batch goes through `applyBatch` once with no segmentation overhead.

**Your `Update` can be called more than once per committed batch.** Each call receives a contiguous run and is all-or-nothing on its own.

`maxExpirePerCheckpoint()` caps the reclaim per checkpoint at `MaxTotalSessions / 8`, with a floor of 64. Scaling to the replicated capacity rather than a fixed 64 bounds the drain of a full table at eight inactivity windows for any configured capacity. At the defaults a fixed 64 would need 256 windows, which is 25.6 million applied entries during which every new `GetNewSession` on the shard fails, reachable by nothing worse than a rolling deploy of a service that does not close its sessions.

#### Entry classification

```
For each proto.Entry in the segment:

  IsConfigChange() or IsInitialMembership()?
      result = {} ; skip user SM
      (membership is applied by processConfigChanges before entries reach here;
       the Cmd is a marshaled config change or Membership, not a user command)

  non-session, non-lifecycle, empty Cmd?
      result = {} ; skip user SM
      (the leader's no-op entry, committed on election to advance commit index)

  IsNewSessionRequest()?
      sessions.Register(clientID, ownerID, index)
      ok:  result = {Value: clientID} ; record lifecycle transition
      err: result = {}
      skip user SM

  IsEndOfSessionRequest()?
      sessions.Unregister(clientID)
      ok:  result = {Value: clientID} ; record lifecycle transition
      err: result = {Value: ResultSessionExpired}
      skip user SM

  IsSessionManaged()?
      sessions.CheckDuplicate(clientID, seriesID, index)
        session error        -> result = {Value: ResultSessionExpired} ; skip
        duplicate            -> result = owned copy of cached result ; skip
      in-batch duplicate?    -> mirror the first occurrence's result ; skip
      sessions.CanRecord(clientID, respondedTo, admittedThisBatch[clientID])
        over capacity        -> result = {Value: ResultSessionExpired} ; skip
        admitted             -> admittedThisBatch[clientID]++ ; record in batchSeen

  forward to user SM
      append to smEntries / smResults / indexMap

Then:
  smWrapper.Update(ctx, smEntries, smResults)
  copy results back to their original positions
  cache each session-managed result (deep-copied)
  resolve in-batch mirrors
```

#### Session entry detection

Session entries are identified by sentinel values on `proto.Entry` (`pkg/proto/constants.go:413`):

| Constant | Value | Meaning |
|----------|-------|---------|
| `NotSessionManagedClientID` | `0` | the entry bypasses session deduplication |
| `SessionSeriesIDForRegister` | `math.MaxUint64 - 1` | register a client session |
| `SessionSeriesIDForUnregister` | `math.MaxUint64` | unregister a client session |

Helper methods on `proto.Entry`:

- `IsSessionManaged()` is `ClientID != NotSessionManagedClientID`
- `IsNewSessionRequest()` is `!IsConfigChange() && len(Cmd) == 0 && ClientID != 0 && SeriesID == SessionSeriesIDForRegister`
- `IsEndOfSessionRequest()` is the same with `SessionSeriesIDForUnregister`

`Register` also reads `entry.OwnerID` straight from the replicated entry, which is what makes the per-owner session cap a state machine decision: the same log produces the same accept or reject on every replica and on every replay after a restart. Nothing about the connection that carried the proposal is consulted, because none of it is in the log.

#### proto.Entry to sm.Entry

| `proto.Entry` field | `sm.Entry` field | Why |
|---------------------|------------------|-----|
| `Index` | `Index` | preserved |
| `Cmd` | `Cmd` | preserved; owned copy by default, aliased under `ZeroCopyEntryCmd` |
| `Term` | dropped | state machines are term-agnostic |
| `Type` | dropped | entry type flags |
| `Key` | dropped | internal dedup key |
| `ClientID`, `SeriesID`, `RespondedTo`, `OwnerID` | dropped | session routing, sequencing, eviction watermark, owner cap |

Because whole classes of entry are filtered out, the `Index` values in one `[]sm.Entry` are strictly increasing but not contiguous.

#### Buffer reuse

```go
smEntries := s.smEntryBuf[:0]
smResults := s.smResultBuf[:0]
indexMap  := s.indexMapBuf[:0]
```

`indexMap` maps a position in `smResults` back to its position in the caller's `results`. All three are retained at their grown capacity for the next call. Safe because `Apply` runs on a single apply goroutine per shard and the buffers are fully consumed before it returns.

#### Zero-copy and result caching

`StateMachine.zeroCopyEntryCmd` mirrors `Config.ZeroCopyEntryCmd`:

- **`false` (default):** for each non-empty `Cmd`, `Apply` allocates an owned copy before building the `sm.Entry`, so the state machine may retain it past `Update`.
- **`true`:** `sm.Entry.Cmd` aliases the engine's pooled `CommittedEntries` buffer directly. The state machine must copy `Cmd` if it retains it, because the engine recycles that buffer after the apply.

Result data gets copied on the way into the session cache regardless of the mode, for two independent reasons (`pkg/internal/rsm/statemachine.go:815`):

1. The same `smResults[j].Data` was just handed to the client through `results[origIdx]`. If the application mutates that buffer it corrupts the replica-local dedup cache and, through `MarshalTo`, the next snapshot. That is a determinism hazard, not just a data race.
2. In zero-copy mode the state machine may have returned `Data` aliasing the pooled buffer the engine is about to recycle.

The client keeps the original buffer; only the cached copy is owned. Cached results handed back on a duplicate hit are likewise copied out of the cache before being written into `results` (`pkg/internal/rsm/statemachine.go:694`).

#### Duplicate detection

```
sessions.CheckDuplicate(clientID, seriesID, index)
   |-- ErrSessionNotFound / ErrResponseEvicted -> ResultSessionExpired
   |-- found in response cache                 -> return the cached result
   |-- not found                               -> new proposal, go to the admission gate
```

`ErrResponseEvicted` means the series already completed and its cached response was evicted because `seriesID <= respondedTo`. Both error cases map to `ResultSessionExpired` per Raft PhD figure 6.1 step 3: "Reply SESSION_EXPIRED if no record of clientId or if response for client's sequenceNum already discarded."

#### In-batch duplicates

A client may retry a request whose original proposal has not yet committed. If both the original and the retry commit and land in the same apply batch, both carry the same `(clientID, seriesID)` and neither is in the session table yet, so `CheckDuplicate` reports both as new. Applying both would run the operation twice.

`batchSeen` maps `(clientID, seriesID)` to the entry index of the first occurrence within the batch. A later occurrence is recorded in `mirrors` and skips the user state machine entirely; after `Update` returns, each mirror copies the result of its first occurrence. The mirror does not call `RecordResult` again, since the first occurrence already cached that response and a second record could push a genuine new series out of the cache.

This check runs *before* the admission gate. A retry that lands in the same batch as its original consumes no new cache slot, so gating it could reject it as "cache full", and applying it would mutate the state machine a second time.

#### The admission gate

```
sessions.CanRecord(clientID, respondedTo, admittedThisBatch[clientID])
   |-- room in the response cache -> admittedThisBatch[clientID]++ ; forward to user SM
   |-- over capacity              -> ResultSessionExpired ; do NOT apply
```

The gate runs before the state machine mutation, and that ordering is the whole point. `RecordResult` only runs after the batched `Update`. If the response cache is already full (a client not advancing its `respondedTo` watermark), applying the entry without being able to cache its result would let a later replay find no cached response and re-apply the mutation. Rejecting up front, treated identically to an evicted response, keeps the entry from ever reaching the state machine.

`admittedThisBatch` is a per-client counter, lazily allocated on the first session-managed entry, added to the in-cache count by `CanRecord` so a batch of new series cannot collectively overflow the cache. It is index- and series-driven with no wall clock, so it is deterministic across replicas.

#### Sessions that change within one batch

`sessionLifecycleAt` maps a client ID to the entry index of the most recent successful `Register` or `Unregister` in this batch. An entry admitted earlier in the batch can reach `RecordResult` after the session it was admitted against is gone, replaced or removed by a later entry in the same batch.

An entry whose client has a lifecycle transition at a *higher* index than its own must not record a response. The entry was legally applied, since it precedes the transition in the log, so its result stands. But for a closed session the cache is moot, and for a re-registered client ID, caching into the replacement session would deduplicate a future proposal of that series which the replacement session never applied.

A successful `Register` or `Unregister` also drops that client's `batchSeen` and `admittedThisBatch` entries. A fresh session incarnation starts its series numbering over, so stale in-batch bookkeeping from a prior incarnation would either mirror the new session's first proposal to the wrong result or reject it against a stale count.

#### Recording results

```
sessions.RecordResult(clientID, seriesID, respondedTo, index, result)
   |-- advance the respondedTo watermark
   |-- evict cached responses with seriesID <= respondedTo
   |-- store the result in the session cache
```

`ErrSessionNotFound` here is tolerated and logged: a missing session is a property of the committed log, not an infrastructure failure, so returning it would fail the shard deterministically on every replica. The `sessionLifecycleAt` check already covers every in-batch removal path; the tolerance is defence in depth against a future path that drops a session mid-batch. Any other error is an invariant violation and stays fatal. `ErrResponseLimitExceeded` is unreachable from here, because the `CanRecord` gate already refused anything that could not be cached.

#### lastApplied

```go
func (s *StateMachine) advanceLastApplied(idx uint64) {
	if idx > s.lastApplied.Load() {
		s.lastApplied.Store(idx)
	}
}
```

Stored atomically because `LastApplied()` is read from the snapshot pool worker goroutine.

`SetLastApplied(idx)` exists because nothing else sets this counter on a resume. `Open` seeds it from an on-disk state machine's own durable position, and `RecoverFromSnapshot` does not set it at all: the session table travels in the snapshot stream, this counter does not. The two callers are the engine's snapshot pool, after `RecoverFromSnapshot` installs state at `idx`, and `Host.StartShard`, beside the engine node's `SeedLastApplied` on the restart path.

Both matter for the same two reasons. The counter is the index source for the next snapshot save, so a snapshot taken before the next `Apply` would otherwise pair a stale index with post-recovery state. And it is the lower bound of the expiry checkpoint walk, so a stale value re-walks checkpoints the shard already walked, and `ExpireSessions` is capped per call, so the re-walk deletes sessions the rest of the shard still holds. The contiguity guard is what makes an unseeded counter fail instead of diverge.

### Read paths

`Lookup`, `NALookup`, `NALookupBuf` and `GetHash` all take the `readGuard` in read mode and return `ErrClosed` if `Close` already ran. In-memory backends do not self-track closure and would otherwise return stale data after `Close`. On a basic state machine `Apply` holds the same guard for writing, so none of the four can run while an apply is in flight; on the concurrent and on-disk tiers they run alongside it.

#### Lookup

```go
func (s *StateMachine) Lookup(ctx context.Context, query interface{}) (interface{}, error)
```

For a `[]byte` query it tries `NALookup` first and falls through to the generic `Lookup` when the state machine does not implement `sm.NALookup`. The read guard serializes this against `Apply`, which holds the same mutex for writing, so a user `Lookup` never overlaps a user `Update`.

#### NALookupBuf

```go
func (s *StateMachine) NALookupBuf(key []byte) (result []byte, release func(), supported bool, err error)
```

The pooled path behind `Host.SyncReadBuf` and `Host.StaleReadBuf`. It takes a `[]byte` from `lookupBufPool` (initial capacity 256, pooled as `*[]byte` so the pool entry does not escape), calls `sm.NALookupInto`, and hands back the filled slice with a release function. When the state machine does not implement `NALookupInto` it falls back to `NALookup` with a no-op release.

Two properties are load-bearing:

- **The read guard is dropped before returning, not held for the lifetime of the buffer.** `release` is called by application code, so a caller that never releases would otherwise pin the read lock forever. Once `Close` queues as a writer, every subsequent lookup on the shard queues behind it. Holding it is also unnecessary, because the `sm.NALookupInto` contract requires implementations to copy into `dst`, so the returned slice does not alias state machine memory.
- **`release` is guarded by a `sync.Once` covering the whole body, `Put` included.** Guarding only part of it would let a double release put the same pointer into the pool twice, after which two goroutines hold the same buffer and silently overwrite each other's read results.

#### GetHash

Delegates through the wrapper's type assertion and reports `(0, false, nil)` when the state machine does not implement `sm.Hasher`. `Host.StateMachineHash` pairs the hash with the applied index so a cross-replica comparison is meaningful.

It takes the read guard for the same reason `Lookup` does. Without it, the hash was computed concurrently with `Apply`, so a divergence check could observe a half-applied batch and report a mismatch that does not exist, and concurrently with `Close`, reading a closed state machine.

### Snapshots

#### Format

```
+-----------------------------------+
| session data (Manager.MarshalTo)  |
|   [numSessions:4]                 |
|   [session1][session2]...         |
|   each session:                   |
|     [clientID:8]                  |
|     [respondedTo:8]               |
|     [numResponses:4]              |
|     [{seriesID:8, value:8,        |
|       dataLen:4, data:N}...]      |
|     [lastActiveIndex:8]           |
+-----------------------------------+
| user SM data (smWrapper snapshot) |
+-----------------------------------+
```

Little-endian throughout, with client IDs and series IDs sorted before serialization so identical session tables produce identical bytes.

#### Save

```go
func (s *StateMachine) SaveSnapshot(ctx, w, stopper) error
func (s *StateMachine) SaveSnapshotGated(ctx, w, stopper, afterCapture func()) error
```

`SaveSnapshot` is `SaveSnapshotGated` with a nil callback. The gated form is what the engine uses (`pkg/internal/engine/snapshot_pool.go:976`).

Order of operations: check the closed guard, check the stopper, marshal the session table, then delegate to `smWrapper.saveSnapshotGated`. The session manager is not safe for concurrent use, so its `MarshalTo` must run with applies quiesced, which is the caller's gate. If the save errors before the capture completes, `afterCapture` is never called.

The engine sets the node's saving flag and waits for the apply drain before calling in, then passes `release` as `afterCapture` (`pkg/internal/engine/snapshot_pool.go:701`). The deferred release is a backstop that covers every error path.

| Wrapper | What `saveSnapshotGated` does | When the engine's apply gate is released |
|---------|-------------------------------|------------------------------------------|
| `regularSMWrapper` | `sm.SaveSnapshot(ctx, w, stopper)` | after the whole save |
| `concurrentSMWrapper` | `PrepareSnapshot()`, then `SaveSnapshot(ctx, snapCtx, w, stopper)` | after `PrepareSnapshot` |
| `onDiskSMWrapper` | `Sync()`, `PrepareSnapshot()`, then `SaveSnapshot(ctx, snapCtx, w, stopper)` | after `PrepareSnapshot` |

A regular state machine has no point-in-time prepare and is not safe against a concurrent `Update`, so its whole in-memory save is the consistent capture. For the other two the capture is cheap and the stream is not, which is the reason the split exists: a large on-disk snapshot is not meant to pause the shard's applies for the duration of its transfer.

The adapter's own guards follow the same split. `SaveSnapshotGated` holds `readGuard` (read mode) and `applyMu` for the capture only, releasing both in lockstep with the engine's apply gate, so an apply arriving during the stream proceeds. `streamGuard` is held across capture and stream and excludes only `Close`, so the state machine is never closed under a save in progress.

#### Recover

```go
func (s *StateMachine) RecoverFromSnapshot(ctx, r, stopper) error
```

1. `readGuard` (read mode) and `applyMu`, held for the whole recovery, so neither `Close` nor `Apply` can overlap it. Then the closed guard and the stopper check.
2. `refreshSessionPolicy()`. `UnmarshalFrom` bounds-checks the table against the manager's limits, and the table was written by a replica applying the *shard's* policy. Checking it against this node's configuration instead would reject a snapshot the rest of the shard considers well-formed, and that rejection is fatal to recovery. For a replica bootstrapped by an `InstallSnapshot` this is also the first point at which any policy is known.
3. Decode the session table into a **staged** manager.
4. `smWrapper.RecoverFromSnapshot`.
5. Only on success, adopt the staged manager.

Staging is what makes recovery all-or-nothing. Decoding straight into the live table replaced it before the user restore ran; if the user restore then failed on a truncated payload, the node kept user data from index N beside session metadata from a later index M. A client retry could be answered from a cached result for an operation the state machine never applied, or a cached response could vanish and let a duplicate command execute twice. Neither surfaces an error anywhere. The reader is sequential, so staging costs nothing: `UnmarshalFrom` advances `r` to exactly the position the user restore must start from whether or not the staged table is adopted.

### Optional interface plumbing

Three package-level helpers in `adapter.go` do the type assertion once and are shared by all three wrappers, so there is one copy of the dispatch rather than nine:

```go
func naLookup(underlying interface{}, key []byte) ([]byte, bool, error)
func naLookupInto(underlying interface{}, key, dst []byte) ([]byte, bool, error)
func getHash(underlying interface{}) (uint64, bool, error)
```

Each follows the same convention:

| Return | Meaning |
|--------|---------|
| `(zero, false, nil)` | the state machine does not implement the interface |
| `(value, true, nil)` | it does, and the call succeeded |
| `(zero, true, err)` | it does, and the call failed |

That lets callers tell "not supported" from "supported but errored" without a separate capability-check method.

### Errors

#### Adapter sentinels (`pkg/internal/rsm/errors.go`)

| Error | When |
|-------|------|
| `ErrInvalidSMType` | `CreateFunc` returned a type implementing none of the three interfaces |
| `ErrStopped` | the stopper channel was already closed when a snapshot operation started |
| `ErrClosed` | `Apply`, a read, or a snapshot operation was attempted on a closed state machine |
| `ErrApplyGap` | the offered batch does not start at `lastApplied + 1` |

These are defined here rather than in the public `quicraft` package (`pkg/`) to avoid an import cycle. `Host.StartShard` translates `rsm.ErrInvalidSMType` into the public `quicraft.ErrInvalidSMType` (`pkg/host.go:3494`).

#### Session sentinels (`pkg/internal/session/errors.go`)

| Error | When |
|-------|------|
| `ErrSessionLimitExceeded` | the shard is at `MaxTotalSessions` |
| `ErrSessionNotFound` | an operation named a client session that does not exist |
| `ErrSessionAlreadyExists` | registering an already-registered client |
| `ErrResponseLimitExceeded` | a session's response cache is full |
| `ErrResponseEvicted` | the series already completed and its response was evicted |
| `ErrSnapshotCorrupted` | sentinel behind `BoundsError` for `errors.Is` checks on a malformed session table |

#### Propagation rules

- Errors from `smWrapper.Update` propagate straight out of `Apply`; the engine treats them as fatal for the shard.
- `ErrApplyGap` fails the shard, deliberately.
- Register and unregister errors are handled inline and never fatal. They are idempotent operations expected during replay: a duplicate register after snapshot recovery, or a double unregister from a retry, must not kill the node. A failed register yields `sm.Result{}`; a failed unregister yields `sm.Result{Value: sm.ResultSessionExpired}`, so the host layer surfaces it as `ErrSessionExpired` and then `ErrRejected` to the caller.
- `ErrSessionNotFound` from `RecordResult` is logged and swallowed. Any other `RecordResult` error is fatal.
- Errors from `SaveSnapshot`, `RecoverFromSnapshot` and `Close` propagate to the engine.
- Nothing is discarded into `_`.

### Wrapper behavior summary

| Method | `regularSMWrapper` | `concurrentSMWrapper` | `onDiskSMWrapper` |
|--------|--------------------|-----------------------|-------------------|
| `Open` | no-op `(0, nil)` | no-op `(0, nil)` | delegates to `sm.Open` |
| `Update` | delegates | delegates | delegates |
| `Lookup` | delegates | delegates | delegates |
| `NALookup` | `naLookup(sm, key)` | same | same |
| `NALookupInto` | `naLookupInto(sm, key, dst)` | same | same |
| `GetHash` | `getHash(sm)` | same | same |
| `saveSnapshotGated` | save, then `afterCapture` | `PrepareSnapshot`, `afterCapture`, save | `Sync`, `PrepareSnapshot`, `afterCapture`, save |
| `RecoverFromSnapshot` | delegates | delegates | delegates |
| `Close` | delegates | delegates | delegates |
| `Type` | `SMTypeRegular` | `SMTypeConcurrent` | `SMTypeOnDisk` |

### Inspection accessor

`StateMachine.Sessions()` returns the live `session.Manager`, the same object `Apply`, `RecoverFromSnapshot` and `SaveSnapshotGated` mutate. `session.Manager` is not safe for concurrent use, and there is no lock here to make it so. Every method on it, including the read-only-looking `Count` and `IsRegistered`, may be called only when no apply and no snapshot save can be in flight for the shard.

It has no production caller. It exists for tests in this package and for the host's session-policy tests, which assert the limits the apply path actually resolved. A future caller wanting one of these numbers on a live shard needs a value snapshotted on the apply goroutine, not this pointer.

### Cross-references

- [State machine interfaces](engineering.md), the contract this adapter enforces
- [KV backends](engineering.md), two implementations of it
- [Engine pipeline](../raft-consensus/engineering.md), the apply worker and the snapshot pool
- [Session deduplication](../client-sessions/engineering.md), the session manager internals
- [Raft protocol](../raft-consensus/engineering.md), snapshot transfer and membership changes
- [Public API](../multi-group-host/api.md), `Host.StartShard` and the read methods

---

## Reference KV backends (`pkg/sm/kv`)


### Overview

`pkg/sm/kv` provides two reference state machine implementations: an in-memory store and a sharded concurrent store. Both speak the same binary command protocol, implement the optional `sm.Hasher`, `sm.NALookup` and `sm.NALookupInto` interfaces, and use the same snapshot format. They are usable as-is, and they are the shortest worked examples of the contract described in [State machine interfaces](engineering.md).

There is no `DiskStateMachine` backend in this repository. The PebbleDB-backed store lives in [go-qrdb](https://github.com/jeremyhahn/go-qrdb).

Source: `pkg/sm/kv/`.

### Which tier each one is

| Store | Interface | Type code | What that buys you |
|-------|-----------|-----------|--------------------|
| `MemoryStore` | `sm.StateMachine` | 0 | `SaveSnapshot` runs with applies quiesced, so the save is the point-in-time |
| `ConcurrentStore` | `sm.ConcurrentStateMachine` | 1 | two-phase snapshot: `PrepareSnapshot` under the apply gate, streaming afterwards |

Both are found by type assertion on the value returned by `sm.CreateFunc` (`pkg/internal/rsm/statemachine.go:335`).

Both take locks, but only `ConcurrentStore` is required to. The RSM adapter serializes `Update` against `Lookup` for every tier today, so `MemoryStore`'s mutex is no longer what keeps a read off a half-applied batch. `ConcurrentStore` is held to its interface's contract, which promises concurrent reads during `Update` whatever the adapter happens to do. See [Lookup concurrency](engineering.md#lookup-concurrency).

### Command protocol

Source: `pkg/sm/kv/protocol.go`.

```
[op:1][keyLen:2 LE][key:keyLen][value:remaining]
```

| Field | Size | Encoding | Description |
|-------|------|----------|-------------|
| `op` | 1 byte | unsigned | operation code |
| `keyLen` | 2 bytes | little-endian | key length |
| `key` | `keyLen` | raw bytes | key |
| `value` | remainder | raw bytes | value, for the write ops |

The header is 3 bytes. `MaxKeyLength` is 65535 (the uint16 ceiling). Value length is implicit: `len(data) - 3 - keyLen`.

#### Operation codes

```go
const (
	OpPut         byte = 1 // store a key-value pair
	OpGet         byte = 2 // reserved for read queries; no-op in Update
	OpDelete      byte = 3 // remove a key
	OpPutIfAbsent byte = 4 // store only when the key is absent
)
```

`OpGet` does nothing in `Update`, because reads go through `Lookup` rather than through Raft proposals. It exists so a caller can encode a read with the same helper family.

`OpPutIfAbsent` is the reason this protocol has a conditional write at all. The existence check runs inside `Update`, on every replica, against the replicated state at that log position, so every replica reaches the same answer. Evaluating it on the proposer instead (read, decide, propose a plain `OpPut`) leaves a window between the read and the commit in which a second proposer interleaves and both writes succeed.

Result codes for the conditional path:

```go
const (
	ResultStored    uint64 = 1 // this entry created the key
	ResultNotStored uint64 = 0 // the key already existed
)
```

Both are successful applications. "Not stored" is an answer, not an error, so it must not stall consensus.

#### Encoders

```go
func EncodePut(key, value []byte) ([]byte, error)
func EncodePutIfAbsent(key, value []byte) ([]byte, error)
func EncodeGet(key []byte) ([]byte, error)
func EncodeDelete(key []byte) ([]byte, error)
```

Each returns `*KeyTooLongError` when `len(key) > MaxKeyLength`. `EncodePutIfAbsent` produces the same wire shape as `EncodePut` and differs only in the opcode, so a reader that predates it rejects the command as an unknown op rather than silently applying a conditional write unconditionally.

#### Decoder

```go
type Command struct {
	Op    byte
	Key   []byte
	Value []byte
}

func DecodeCommand(data []byte) (Command, error)
```

`Key` and `Value` are sub-slices of `data`. Under `Config.ZeroCopyEntryCmd` that means they alias engine memory, so copy anything you keep. Both stores clear their decode scratch slices at the end of `Update` for exactly this reason.

`DecodeCommand` returns `*InvalidCommandError` for data shorter than 3 bytes, an opcode outside 1-4, or a key length that runs past the end of the buffer.

#### Lookup queries

| Query type | Behavior |
|------------|----------|
| `[]byte` | value for the key, or `*KeyNotFoundError` |
| `string` | converted to `[]byte`, same behavior |
| `ListQuery` | `[]string` of keys with the prefix, sorted |

```go
type ListQuery struct {
	Prefix string
}
```

`ListQuery` always returns a non-nil slice, empty when nothing matches. Anything else returns `*InvalidCommandError`.

`ListQuery` is passed through `Lookup` and never encoded, so there is no `EncodeList`. Listing is a read, not a proposal.

### Update: decode, then mutate

Both stores run `Update` in two passes, and the split is not stylistic.

Pass one decodes every entry and touches nothing. An entry whose payload fails to decode gets `sm.ResultInvalidCommand` written into its result slot and is marked to be skipped.

Pass two applies. The handlers cannot fail, so no error return is possible after the first mutation and the all-or-nothing contract of `Update` holds without a rollback path.

This is also why an undecodable payload does not produce an `Update` error. The entry is already committed, so returning an error would fail the shard deterministically on every replica and wedge it forever on replay (`pkg/sm/kv/memory.go:95`).

### MemoryStore

Source: `pkg/sm/kv/memory.go`.

```go
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
	cmds []Command // decode scratch, reused across Update calls
}

func NewMemoryStore() *MemoryStore
```

The `RWMutex` no longer separates reads from applies. `Lookup`, `BytesLookup`, `BytesLookupInto` and `GetHash` do arrive on client request goroutines rather than on the apply worker, but the RSM adapter takes its `readGuard` for writing across `Update` and for reading across all four, so none of them can overlap an apply. `SaveSnapshot` on this tier runs with applies quiesced under the engine's saving gate as well.

What the mutex still buys: `MemoryStore` is an ordinary exported type that nothing stops you using outside a shard, and the `cmds` decode scratch slice reused across `Update` calls is guarded by it. Inside the adapter it is defence in depth. The read lock in `SaveSnapshot` also lets the deep copy finish before serialization starts, so the write phase never holds a lock across I/O.

#### Update dispatch

Operations are dispatched through a map, so adding an opcode does not touch the loop:

```go
type opHandlerFunc func(m *MemoryStore, cmd Command) sm.Result

var opHandlers = map[byte]opHandlerFunc{
	OpPut:         handlePut,
	OpGet:         handleGetNoop,
	OpDelete:      handleDelete,
	OpPutIfAbsent: handlePutIfAbsent,
}
```

| Operation | `Result.Value` | `Result.Data` |
|-----------|----------------|---------------|
| Put | `len(value)` | nil |
| PutIfAbsent | `ResultStored` (1) or `ResultNotStored` (0) | nil |
| Get | 0 | nil |
| Delete | 1 | nil |
| undecodable payload | `sm.ResultInvalidCommand` | nil |

Put and PutIfAbsent copy the value before storing, so nothing in the map aliases an entry buffer.

#### Reads

`Lookup` and `BytesLookup` hold the read lock and return an owned copy, so a caller cannot mutate stored state through the returned slice. `BytesLookupInto` copies into the caller's buffer instead, growing it with `append`, which is what lets `Host.SyncReadBuf` and `Host.StaleReadBuf` drop the adapter's read guard before handing the slice out.

#### GetHash

The hash is xxhash64 over key-value pairs in sorted key order, with a fixed-width uint64 little-endian length prefix before both the key and the value:

```go
func hashLenPrefixed(h *xxhash.Digest, key string, val []byte) error {
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(key)))
	if _, err := h.Write(lenBuf[:]); err != nil {
		return &HashWriteError{Err: err}
	}
	if _, err := h.WriteString(key); err != nil {
		return &HashWriteError{Err: err}
	}
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(val)))
	if _, err := h.Write(lenBuf[:]); err != nil {
		return &HashWriteError{Err: err}
	}
	if _, err := h.Write(val); err != nil {
		return &HashWriteError{Err: err}
	}
	return nil
}
```

The length prefixes are not decoration. Without them the concatenated stream is ambiguous: `{"user": "1"}` and `{"use": "r1"}` hash identically, so genuine divergence goes undetected by the one API that exists to detect it. Both stores share this helper so they produce the same hash for the same logical contents.

Sorting makes the hash independent of insertion order, which is what `Host.StateMachineHash` needs for a cross-replica comparison to mean anything.

### ConcurrentStore

Source: `pkg/sm/kv/concurrent.go`.

```go
const numBuckets = 16

type bucket struct {
	mu   sync.RWMutex
	data map[string][]byte
}

type ConcurrentStore struct {
	buckets [numBuckets]bucket
	ops     []decodedOp // decode scratch, reused across Update calls
}

func NewConcurrentStore() *ConcurrentStore
```

Keys land in a bucket by `xxhash64(key) % 16`.

#### Locking

| Method | Lock | Scope |
|--------|------|-------|
| `Update` | write | every bucket the batch touches, all held together for the whole apply pass |
| `Lookup` (single key) | read | one bucket |
| `Lookup` (`ListQuery`) | read | all 16 |
| `BytesLookup`, `BytesLookupInto` | read | one bucket |
| `GetHash` | read | all 16, held for the whole computation |
| `PrepareSnapshot` | read | all 16 |
| `RecoverFromSnapshot` | write | all 16, after the stream has been fully parsed |
| `Close` | write | one bucket at a time |

`Update` takes the write locks of all affected buckets before it mutates anything and releases them only after the last entry, so a concurrent `Lookup` never observes a half-applied batch. Every multi-bucket path acquires in ascending index order, which is what makes deadlock impossible.

`GetHash` holds all 16 read locks for the whole computation rather than per key. Releasing and re-acquiring would let an `Update` interleave and produce a hash of a state no replica ever held, which reads as divergence that does not exist.

#### PrepareSnapshot

```go
func (cs *ConcurrentStore) PrepareSnapshot() (interface{}, error) {
	for i := range cs.buckets {
		cs.buckets[i].mu.RLock()
	}
	clone := make(map[string][]byte)
	for i := range cs.buckets {
		for k, v := range cs.buckets[i].data {
			cp := make([]byte, len(v))
			copy(cp, v)
			clone[k] = cp
		}
	}
	for i := range cs.buckets {
		cs.buckets[i].mu.RUnlock()
	}
	return clone, nil
}
```

The returned flat map is what `SaveSnapshot` serializes, on a different goroutine, holding no locks. `SaveSnapshot` returns `*InvalidCommandError` if it is handed anything other than a `map[string][]byte`.

The clone costs O(n) memory for the duration of the stream. That is the price of not pausing applies while a large snapshot is written.

### Snapshot format

Both stores write the same layout:

```
[count:4 LE][entry_1][entry_2]...[entry_N]
```

Each entry:

```
[keyLen:4 LE][key:keyLen][valLen:4 LE][val:valLen]
```

All lengths are little-endian uint32.

`MemoryStore.SaveSnapshot` emits pairs in sorted key order. Ranging a map directly produces different bytes on each run for identical state, which defeats byte-level snapshot comparison, deduplication, and any digest an operator wants to compare across replicas. The decoder is order-independent, so the ordering affects only what is written.

`ConcurrentStore.SaveSnapshot` currently ranges its cloned map directly and therefore does *not* produce byte-stable output. Recovery is unaffected; only byte-level comparison of two snapshots is.

#### Save

1. Write `uint32(len(data))`.
2. For each pair, check the stopper, then write key length, key, value length, value.
3. Return `context.Canceled` if the stopper closed mid-stream.

#### Recover

Both stores parse the entire stream into staging structures before touching live state, so a truncated or corrupt snapshot returns an error and leaves the existing data intact. `MemoryStore` swaps in one new map; `ConcurrentStore` takes all 16 write locks and swaps in 16 staged maps.

Two bounds guard the untrusted length prefixes, because a snapshot arrives from a peer over the transport:

- `keyLen > MaxKeyLength` (65535) and `valLen > MaxValueLength` (64 MiB) both return `errCorruptSnapshotLength` instead of attempting the allocation.
- The `count` prefix is not used directly as a map capacity. It is capped at `maxSnapshotMapHint` (1,048,576) for the pre-allocation; the map still grows past that if the snapshot genuinely holds more, and a count larger than the stream simply ends the loop early at EOF.

`MaxValueLength` is a denial-of-service ceiling, not the largest writable value. A KV value always travels inside one proposal's `Entry.Cmd`, which the engine caps at `proto.MaxEntrySize` (8 MiB) at propose time and the WAL caps at the same figure, so a legitimately stored value can never approach 64 MiB. Keeping the recovery bound above the write-time limit means tightening `MaxEntrySize` cannot retroactively make an already-persisted snapshot unrecoverable.

### Factory functions

Source: `pkg/sm/kv/factory.go`.

```go
func NewMemoryCreateFunc() sm.CreateFunc {
	return sm.NewCreateFunc(func(_, _ uint64) sm.StateMachine {
		return NewMemoryStore()
	})
}

func NewConcurrentCreateFunc() sm.CreateFunc {
	return sm.NewConcurrentCreateFunc(func(_, _ uint64) sm.ConcurrentStateMachine {
		return NewConcurrentStore()
	})
}
```

The typed wrappers turn an interface mismatch into a compile error instead of an `ErrInvalidSMType` at shard start.

```go
members := map[uint64]string{1: "10.0.0.1:4001", 2: "10.0.0.2:4001", 3: "10.0.0.3:4001"}
cfg := config.Config{ShardID: 100, ReplicaID: 1}

err := host.StartShard(members, false, kv.NewMemoryCreateFunc(), cfg)
// or
err = host.StartShard(members, false, kv.NewConcurrentCreateFunc(), cfg)
```

### Errors

Source: `pkg/sm/kv/errors.go`.

```go
type KeyNotFoundError struct{ Key string }
type InvalidCommandError struct{ Reason string }
type KeyTooLongError struct{ Length int }
type HashWriteError struct{ Err error } // implements Unwrap
```

| Type | Raised by | When |
|------|-----------|------|
| `*KeyNotFoundError` | `Lookup`, `BytesLookup`, `BytesLookupInto` | key absent |
| `*InvalidCommandError` | `DecodeCommand` | payload under 3 bytes, unknown opcode, key length past end of data |
| `*InvalidCommandError` | `Lookup` | query is not `[]byte`, `string` or `ListQuery` |
| `*InvalidCommandError` | `ConcurrentStore.SaveSnapshot` | `snapshotCtx` is not a `map[string][]byte` |
| `*KeyTooLongError` | the four `Encode*` functions | key over `MaxKeyLength` |
| `*HashWriteError` | `GetHash` | the xxhash digest returned a write error |

A decode failure inside `Update` does not surface as an error at all. It becomes `sm.ResultInvalidCommand` in that entry's result slot.

### Interface compliance

```go
// MemoryStore
var (
	_ sm.StateMachine = (*MemoryStore)(nil)
	_ sm.Hasher       = (*MemoryStore)(nil)
	_ sm.NALookup     = (*MemoryStore)(nil)
	_ sm.NALookupInto = (*MemoryStore)(nil)
)

// ConcurrentStore
var (
	_ sm.ConcurrentStateMachine = (*ConcurrentStore)(nil)
	_ sm.Hasher                 = (*ConcurrentStore)(nil)
	_ sm.NALookup               = (*ConcurrentStore)(nil)
	_ sm.NALookupInto           = (*ConcurrentStore)(nil)
)
```

Worth copying into your own state machine. The optional interfaces are found by type assertion at runtime, so a signature that drifts by one argument compiles cleanly and leaves the optimized path silently unused forever.

### Cross-references

- [State machine interfaces](engineering.md), the contract both stores implement
- [RSM adapter](engineering.md), what calls `Update`, `Lookup` and the snapshot methods
- [Engine pipeline](../raft-consensus/engineering.md), how apply and snapshot work is scheduled
- [Session deduplication](../client-sessions/engineering.md), at-most-once delivery around these calls
- [Deployment](../multi-group-host/user.md), configuration and CLI options

---

## Configuration

`pkg/sm` and `pkg/sm/kv` read no configuration of their own. The per-shard
settings that change how the adapter calls your state machine, all on
`config.Config`:

| Key | Default | Effect on a state machine |
|---|---|---|
| `MaxApplyEntrySize` | `MaxReplicationPayloadBytes` = 16,711,672 (~15.94 MB) | the maximum batch size handed to one `Update` call |
| `MaxEntrySize` | 8 MB | the per-entry cap, and the decompression ceiling on the apply path. May not exceed `proto.MaxEntrySize`. |
| `ZeroCopyEntryCmd` | `false` | when true, `Entry.Cmd` aliases the internal buffer and **you must copy it to retain it past `Update()`** |
| `SnapshotEntries` | 100000 | applied entries between automatic snapshots. **Zero does not disable it** — zero means "unset" and selects the default. Assign `config.SnapshotsDisabled` for manual-only. |
| `CompactionOverhead` | 5000 | entries retained after compaction |
| `MaxSnapshotSize` | 4 GB | maximum snapshot file size |

`sm.CacheConfig` configures `CachedDiskStateMachine` and is validated with
`InvalidCacheConfigError` / `CacheMaxEntriesError`.

Session-related settings are in
[`../client-sessions/engineering.md`](../client-sessions/engineering.md);
snapshot cadence in [`../snapshots/engineering.md`](../snapshots/engineering.md).

## Test Strategy

Targets: `make test-sm`, `make test-rsm`.

| Layer | Covers |
|---|---|
| **Unit** (`pkg/sm/*_test.go`, `pkg/sm/kv/*_test.go`) | the interface wrappers, `CachedDiskStateMachine`, the KV command codec, both reference stores, their snapshot formats |
| **Unit** (`pkg/internal/rsm/*_test.go`) | type detection, the apply pipeline, session routing, the `(value, supported, error)` wrapper shape, snapshot save/recover |
| **Integration** (`test/integration`) | a real multi-node cluster applying to real state machines |
| **Linearizability** (`make integration-test-linearizability`) | that what the state machines observe is actually linearizable, which no unit test can establish |

**What a test for your own state machine must cover**, because the adapter will
not catch it:

1. **`Update` is deterministic.** Replay the same entries on a fresh instance
   and get the same state. Anything reading a clock, a random source, a map
   iteration order or the network breaks this, and it breaks *silently* — the
   replicas diverge and nothing notices until a hash check or a restart.
2. **`RecoverFromSnapshot` after `SaveSnapshot` round-trips exactly.**
3. **A `DiskStateMachine` replays already-applied entries idempotently**, since
   after a crash it will be given entries it has already durably applied.
4. **Optional interfaces are actually detected.** Detection is by type
   assertion, so a wrong signature produces **no compile error** and is simply
   never invoked (see below). Assert the interface at compile time:
   `var _ sm.Hasher = (*MyStateMachine)(nil)`.

**Fault-injection seams**: return an error from `Update` to exercise the
fail-fast path — the shard is marked failed on the **first** `SM.Apply` error,
with no retry counting and no apply-retry setting anywhere.

**Fixtures**: `pkg/sm/kv`'s `MemoryStore` and `ConcurrentStore` are the fixtures
for everything above this layer, which is why their behavior is specified here
rather than left as example code.

## Observability

`pkg/sm` and `pkg/sm/kv` emit **no logs and no metrics**. They are called once
per applied entry; instrumentation belongs to the layers that can afford it.

What is observable about a state machine, and where it comes from:

| Signal | Source |
|---|---|
| apply latency and throughput | the engine's apply worker, via the host `MetricsCollector` |
| a shard marked failed after an `Update` error | the `EventListener` shard-failure notification, dispatched on the listener goroutine |
| cross-replica divergence | `Host.StateMachineHash(shardID)`, which returns `(hash, appliedIndex, ok, err)` and requires your state machine to implement `sm.Hasher` |
| last applied index | `Host.GetShardInfo` |

`StateMachineHash` is the one worth wiring into a periodic check. Divergence
between replicas is the failure mode a non-deterministic `Update` produces, and
it has no other symptom until a restart replays the log differently.

Log from inside your own `Update` only at a level you would accept once per
entry, and never log the command payload — it is application data this library
makes no promises about.
