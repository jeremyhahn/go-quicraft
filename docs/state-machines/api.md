# State Machines — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/sm`, `pkg/sm/kv` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library, and this feature is the clearest case of what that
means: **you consume it by implementing an interface, not by calling a client.**

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the interfaces below. This is the transport, and it points *inward* — the library calls you. |
| Unix socket / REST / gRPC / QUIC / MCP | n/a — this repository serves none of them |

Not missing-capability rows. The portfolio capability register records
go-quicraft's Raft row as "consumed by interface implementation, not client
calls", and this feature is that row's clearest instance.

## Go API — the interface you implement

Import: `github.com/jeremyhahn/go-quicraft/pkg/sm`

Implement **exactly one** of the three.

### Tier 0 — `StateMachine`

```go
type StateMachine interface {
    Update(ctx context.Context, entries []Entry, results []Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
}
```

All state in memory, serialized access — `Update` and `Lookup` never run
concurrently, because the adapter holds a read guard for writing across `Apply`
and for reading across every read entry point.

### Tier 1 — `ConcurrentStateMachine`

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

`Lookup` runs concurrently with `Update`. `PrepareSnapshot` captures a
point-in-time context that `SaveSnapshot` then writes **without blocking**
`Update`.

### Tier 2 — `DiskStateMachine`

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

`Open` is called at startup and returns the **last applied index** from the
machine's own storage. Raft replays from there, so the machine **must replay
already-applied entries idempotently**.

### The values that cross the boundary

```go
type Entry struct {
    Index uint64   // Raft log index
    Cmd   []byte   // proposal payload
}
func (e *Entry) CopyCmd() []byte

type Result struct {
    Value uint64   // application-defined result code
    Data  []byte   // optional data, returned through RequestState.Result()
}
```

`Entry` is a **deliberately simplified view** of `proto.Entry`: no `Term`, no
`Key`, no `ClientID`. `Term` is excluded because a state machine must be
term-agnostic — reading it is a route to non-determinism.

`Cmd` is an owned copy by default. **When `Config.ZeroCopyEntryCmd` is true it
aliases an internal buffer reused after `Update` returns**, and you must copy it
to retain it. `CopyCmd()` is the one-call form.

Reserved `Result.Value` constants are used internally by the RSM layer to signal
session outcomes — `ResultSessionExpired` and its neighbours. Do not return
them from application code.

### Registering your state machine

```go
type CreateFunc func(shardID, replicaID uint64) interface{}

func NewCreateFunc(fn CreateStateMachineFunc) CreateFunc
func NewConcurrentCreateFunc(fn CreateConcurrentStateMachineFunc) CreateFunc
func NewDiskCreateFunc(fn CreateDiskStateMachineFunc) CreateFunc

type CreateStateMachineFunc           func(shardID, replicaID uint64) StateMachine
type CreateConcurrentStateMachineFunc func(shardID, replicaID uint64) ConcurrentStateMachine
type CreateDiskStateMachineFunc       func(shardID, replicaID uint64) DiskStateMachine
```

Use the typed wrapper for your tier. It makes the choice explicit at the call
site rather than inferred from a method set you may have got wrong.

```go
err := host.StartShard(members, join, sm.NewCreateFunc(newMySM), cfg)
```

### Optional capability interfaces

```go
type Hasher       interface{ GetHash() (uint64, error) }
type NALookup     interface{ BytesLookup(key []byte) ([]byte, error) }
type NALookupInto interface {
    NALookup
    BytesLookupInto(key, dst []byte) ([]byte, error)
}
type EntryKeyFunc     func(cmd []byte) (key string, value []byte)
type EntryAppliedFunc func(cmd []byte, result Result) bool
```

| Interface | Enables |
|---|---|
| `Hasher` | `Host.StateMachineHash(shardID)` — cross-replica divergence detection |
| `NALookup` | the zero-alloc read path |
| `NALookupInto` | reads into a caller-supplied buffer |

**Two return values, not three.** Detection is by type assertion, so a wrong
signature produces **no compile error** and the capability is silently never
invoked. Assert it:

```go
var _ sm.Hasher = (*MyStateMachine)(nil)
```

The internal adapter wraps each of these behind a `(value, supported, error)`
shape where the bool reports whether you implemented the interface at all. That
three-value form is **internal** (`pkg/internal/rsm`), not importable, and not
the contract you implement.

### Capability: wrap a disk state machine in a write-through cache

```go
func NewCachedDiskStateMachine(inner DiskStateMachine, cfg CacheConfig) (*CachedDiskStateMachine, error)
func (c *CachedDiskStateMachine) CacheStats() (hits, misses uint64)
```

| | |
|---|---|
| Request | an inner `DiskStateMachine` and a `CacheConfig` |
| Response | a `DiskStateMachine` that also satisfies `NALookup` and delegates `Hasher` |
| Errors | `*InvalidCacheConfigError`, `*CacheMaxEntriesError` (`MaxEntries` not positive) |

Two behaviors worth knowing before using it:

- **Invalidation happens before the inner write**, not after. If the cache were
  only invalidated afterwards, a concurrent read between the two could repopulate
  it from the pre-write state.
- **Cache hits return an owned copy.** Handing out the live cache buffer would
  let a caller mutate cached state.
- **A state machine with conditional operations — compare-and-set,
  put-if-absent, delete-if-match — MUST set `CacheConfig.EntryApplied`.** The
  cache is populated from the value `ExtractKV` pulls out of the *command*, and
  a conditional write may not have applied; nothing here can infer the outcome,
  because the encoding is the state machine's own.

### Errors

```go
type UnsupportedLookupResultError struct{ ... }
type InvalidCacheConfigError       struct{ ... }
type CacheMaxEntriesError          struct{ ... }
```

An error returned from `Update` **fails the shard**. There is no retry and no
setting to add one.

## Go API — the reference KV backends

Import: `github.com/jeremyhahn/go-quicraft/pkg/sm/kv`

**Reference implementations and test fixtures, not a product surface.** A real
key-value store is go-qrdb's.

```go
func NewMemoryCreateFunc() sm.CreateFunc       // MemoryStore, tier 0
func NewConcurrentCreateFunc() sm.CreateFunc   // ConcurrentStore, tier 1

func NewMemoryStore() *MemoryStore
func NewConcurrentStore() *ConcurrentStore
```

Command codec:

```go
const OpPut byte = 1   // plus OpDelete, OpGet, OpPutIfAbsent

func EncodePut(key, value []byte) ([]byte, error)
func EncodePutIfAbsent(key, value []byte) ([]byte, error)
func EncodeDelete(key []byte) ([]byte, error)
func EncodeGet(key []byte) ([]byte, error)
func DecodeCommand(data []byte) (Command, error)

type Command   struct{ ... }
type ListQuery struct{ ... }
```

Errors: `*KeyNotFoundError`, `*KeyTooLongError`, `*InvalidCommandError`,
`*HashWriteError`.

Both stores implement `sm.Hasher`, which is what makes them usable as the
fixture for divergence tests.

## Versioning and deprecation

- Nothing here is deprecated.
- **A signature change to any of the three interfaces breaks every consumer in
  the portfolio**, because consumers implement them. Sequence such changes
  bottom-up and coordinate through the Software Department.
- Snapshot payloads written by your `SaveSnapshot` are **your** format; this
  library versions the envelope, not the contents. See
  [`../snapshots/api.md`](../snapshots/api.md).
- `pkg/sm/kv`'s command encoding and snapshot format are stable for the fixture
  role they serve and carry no compatibility promise to application code.
