# State Machines — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/sm`, `pkg/sm/kv` ·
> Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

You consume this feature by implementing an interface, not by calling a client.

| Transport | This feature |
|---|---|
| Embedded (Go call) | the interfaces below; the library calls you |
| Unix socket, REST, gRPC, QUIC, MCP | not applicable: this repository serves none of them |

## The interface you implement

Import: `github.com/jeremyhahn/go-quicraft/pkg/sm`. Implement exactly one of the
three.

### Tier 0: `StateMachine`

```go
type StateMachine interface {
    Update(ctx context.Context, entries []Entry, results []Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
}
```

All state in memory. `Update` and `Lookup` never run concurrently.

### Tier 1: `ConcurrentStateMachine`

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

`Lookup` and `GetHash` (if implemented) run concurrently with `Update`; you
synchronize them. `PrepareSnapshot` runs with applies paused and captures a
point-in-time value that `SaveSnapshot` then streams while applies continue.

### Tier 2: `DiskStateMachine`

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

`Open` returns the last index your store durably applied, or 0 on a fresh start.
Raft replays from there, so replay of already-applied entries must be
idempotent. Concurrency is as for tier 1. Client sessions are refused on this
tier with `ErrSessionsUnsupportedOnDisk`.

### Values that cross the boundary

```go
type Entry struct {
    Index uint64 // Raft log index
    Cmd   []byte // proposal payload
}
func (e *Entry) CopyCmd() []byte // nil for an empty Cmd

type Result struct {
    Value uint64 // application-defined result code
    Data  []byte // optional; returned through RequestState.Result()
}

const ResultSessionExpired uint64 = math.MaxUint64 - 2 // reserved, set by the adapter
const ResultInvalidCommand uint64 = math.MaxUint64 - 3 // reserved, for undecodable payloads
```

`Entry` carries no term, type or session fields. A state machine must be
term-agnostic.

`Cmd` is an owned copy by default. When `Config.ZeroCopyEntryCmd` is true it
aliases an engine buffer reused after `Update` returns, and you must copy what
you keep; `CopyCmd` is the one-call form.

Do not return `ResultSessionExpired`. Write `ResultInvalidCommand` for a
committed payload you cannot decode, instead of returning an error.

### Registering a state machine

```go
type CreateFunc func(shardID, replicaID uint64) interface{}

type CreateStateMachineFunc           func(shardID uint64, replicaID uint64) StateMachine
type CreateConcurrentStateMachineFunc func(shardID uint64, replicaID uint64) ConcurrentStateMachine
type CreateDiskStateMachineFunc       func(shardID uint64, replicaID uint64) DiskStateMachine

func NewCreateFunc(fn CreateStateMachineFunc) CreateFunc
func NewConcurrentCreateFunc(fn CreateConcurrentStateMachineFunc) CreateFunc
func NewDiskCreateFunc(fn CreateDiskStateMachineFunc) CreateFunc
```

Use the typed wrapper for your tier:

```go
err := host.StartShard(members, false, sm.NewCreateFunc(newMySM), cfg)
```

A raw `CreateFunc` whose value implements none of the three interfaces makes
`StartShard` return `quicraft.ErrInvalidSMType`.

### Optional interfaces

```go
type Hasher       interface{ GetHash() (uint64, error) }
type NALookup     interface{ BytesLookup(key []byte) ([]byte, error) }
type NALookupInto interface {
    NALookup
    BytesLookupInto(key, dst []byte) ([]byte, error)
}
```

| Interface | Enables |
|---|---|
| `Hasher` | `Host.StateMachineHash(shardID)`, cross-replica divergence detection. The hash must be order-independent and, on tiers 1 and 2, must cover whole batches. |
| `NALookup` | a `[]byte` read path without `interface{}` boxing |
| `NALookupInto` | reads into a pooled buffer for `Host.SyncReadBuf` and `Host.StaleReadBuf`. The value must be copied into `dst`. |

Detection is by type assertion, so a wrong signature compiles and the capability
is never called. Assert it:

```go
var _ sm.Hasher = (*MyStateMachine)(nil)
```

### Write-through cache for a disk state machine

```go
type EntryKeyFunc     func(cmd []byte) (key string, value []byte)
type EntryAppliedFunc func(cmd []byte, result Result) bool

type CacheConfig struct {
    MaxEntries   int              // required, > 0
    ExtractKV    EntryKeyFunc     // required
    EntryApplied EntryAppliedFunc // required if any operation is conditional
}

func NewCachedDiskStateMachine(inner DiskStateMachine, cfg CacheConfig) (*CachedDiskStateMachine, error)
func (c *CachedDiskStateMachine) CacheStats() (hits, misses uint64)
```

| | |
|---|---|
| Request | an inner `DiskStateMachine` and a `CacheConfig` |
| Response | a `DiskStateMachine` that also implements `NALookup` and `Hasher` |
| Errors | `*CacheMaxEntriesError` (`MaxEntries` not positive), `*InvalidCacheConfigError` (`ExtractKV` nil) |

- Affected keys are invalidated before the inner write, so a racing read
  misses instead of returning a stale value.
- Cache hits return an owned copy.
- A state machine with conditional operations (compare-and-set, put-if-absent,
  delete-if-match) must set `EntryApplied`. The cache is filled from the value
  in the command, and a conditional write may not have applied.
- `GetHash` delegates to the inner store. If the inner store does not implement
  `Hasher`, it returns `*InvalidCacheConfigError`, so `StateMachineHash` reports
  `ok == true` with that error.

### Errors

```go
type UnsupportedLookupResultError struct{ Got string } // BytesLookup got a non-[]byte Lookup result
type InvalidCacheConfigError       struct{ Reason string }
type CacheMaxEntriesError          struct{ Value int }
```

An error returned from `Update` fails the shard. There is no retry.

## The reference KV backends

Import: `github.com/jeremyhahn/go-quicraft/pkg/sm/kv`. These are reference
implementations and test fixtures, not a product surface.

```go
func NewMemoryCreateFunc() sm.CreateFunc     // MemoryStore, tier 0
func NewConcurrentCreateFunc() sm.CreateFunc // ConcurrentStore, tier 1
func NewMemoryStore() *MemoryStore
func NewConcurrentStore() *ConcurrentStore

const (
    OpPut         byte = 1
    OpGet         byte = 2 // no-op in Update
    OpDelete      byte = 3
    OpPutIfAbsent byte = 4
    ResultStored    uint64 = 1
    ResultNotStored uint64 = 0
    MaxKeyLength   = 65535
    MaxValueLength = 64 * 1024 * 1024 // snapshot-recovery bound
)

func EncodePut(key, value []byte) ([]byte, error)
func EncodePutIfAbsent(key, value []byte) ([]byte, error)
func EncodeDelete(key []byte) ([]byte, error)
func EncodeGet(key []byte) ([]byte, error)
func DecodeCommand(data []byte) (Command, error)

type Command   struct{ Op byte; Key, Value []byte }
type ListQuery struct{ Prefix string }
```

Errors: `*KeyNotFoundError`, `*KeyTooLongError`, `*InvalidCommandError`,
`*HashWriteError`. Both stores implement `sm.Hasher`, `sm.NALookup` and
`sm.NALookupInto`.

## Versioning

- Nothing here is deprecated.
- A signature change to any of the three interfaces breaks every implementation,
  so it is a breaking release.
- The bytes your `SaveSnapshot` writes are your format. The library versions the
  envelope around them, not the contents; see
  [`../snapshots/api.md`](../snapshots/api.md).
- The `pkg/sm/kv` command encoding and snapshot format carry no compatibility
  promise beyond their fixture role.
