# State machine interfaces

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

The guard belongs to the adapter, not to a tier. `ConcurrentStateMachine` and `DiskStateMachine` publish the opposite contract — `Lookup` may be called concurrently with `Update`, and the implementation synchronizes itself — and because there is one guard for all three, they currently get the exclusion too. Do not read that as permission to drop their locking. Those tiers are held to what their doc comments promise, and the adapter is free to stop excluding them.

`kv.MemoryStore` keeps its `RWMutex`, but not for this reason any more. See [MemoryStore](kv-backends.md#memorystore).

### What the exclusion costs

A local read parks behind an in-flight `Apply`. On a follower catching up, that apply can carry as much as `maxEntriesToApplySize` (64 MiB) of entries, and `sync.RWMutex` has no context-aware acquire, so the deadline on the context you passed to `SyncRead` is not observed while the goroutine sits on the lock. The ReadIndex wait ahead of the lock honours it, and your `Lookup` receives the same context once the lock is taken, but nothing cancels the wait itself.

Go's `RWMutex` also blocks new readers once a writer is queued, so a single apply behind a slow read stalls every subsequent read on the shard until it drains.

`ConcurrentStateMachine` is where an implementation that needs finer-grained read concurrency belongs; offering that choice is why the tier exists. It does not remove the wait today, though. The guard is on the adapter, so the concurrent tier queues behind an apply exactly as the basic tier does. Declaring the tier is what leaves your state machine ready for the adapter to stop excluding it.

What differs per tier:

| Tier | `Update` vs `Lookup` | `Update` vs `SaveSnapshot` |
|------|----------------------|----------------------------|
| `StateMachine` | serialized by the adapter's guard, as the interface promises | serialized: the whole save runs under the engine's apply gate |
| `ConcurrentStateMachine` | documented concurrent; serialized by the adapter today | contracted concurrent after `PrepareSnapshot` returns |
| `DiskStateMachine` | documented concurrent; serialized by the adapter today | contracted concurrent after `PrepareSnapshot` returns |

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

Command encoding, snapshot format, result codes and error types are documented in [KV backends](kv-backends.md).

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

`ErrSessionLimitExceeded`, `ErrSessionNotFound`, `ErrSessionAlreadyExists`, `ErrResponseLimitExceeded`, `ErrResponseEvicted`, `ErrSnapshotCorrupted`. None of these are yours to handle: the adapter turns them into `Result` values or handles them inline. See [session deduplication](../sessions/deduplication.md).

### From the KV backends (`pkg/sm/kv/errors.go`)

`*KeyNotFoundError`, `*InvalidCommandError`, `*KeyTooLongError`, `*HashWriteError`. Documented in [KV backends](kv-backends.md).

## Cross-references

- [RSM adapter](rsm-adapter.md), the layer between the apply pipeline and your code
- [KV backends](kv-backends.md), the two reference implementations
- [Engine pipeline](../engine/pipeline.md), the apply worker that calls `rsm.StateMachine.Apply`
- [Session deduplication](../sessions/deduplication.md), the at-most-once protocol
- [Public API](../api/reference.md), `Host.StartShard`, the read methods, `Host.StateMachineHash`
- [Architecture overview](../architecture/overview.md)
