# KV Store Backends

## Overview

The `pkg/sm/kv/` package provides two reference state machine implementations for QuicRaft: an in-memory store and a sharded concurrent store. Both implement the KV command protocol, optional `sm.Hasher` and `sm.NALookup` interfaces, and a binary snapshot format. They serve as production-ready examples of the state machine contract and as ready-to-use backends for applications built on QuicRaft.

Source: `pkg/sm/kv/`

## State Machine Interface Recap

QuicRaft defines three state machine tiers in `pkg/sm/interfaces.go`, detected at startup via type assertion on the value returned by `sm.CreateFunc`:

| Tier | Interface | Type Code | Key Properties |
|------|-----------|-----------|----------------|
| In-memory | `sm.StateMachine` | 0 | Serialized access for `Update`/`Lookup`. `SaveSnapshot` may run concurrently from snapshot pool. |
| Concurrent | `sm.ConcurrentStateMachine` | 1 | `Lookup` concurrent with `Update`. Two-phase snapshot via `PrepareSnapshot`. |
| On-disk | `sm.DiskStateMachine` | 2 | Owns persistent storage. `Open`/`Sync` lifecycle. Idempotent replay. |

Two optional interfaces are detected via type assertion:

- `sm.Hasher` -- `GetHash() (uint64, error)` for cross-replica consistency checks.
- `sm.NALookup` -- `BytesLookup(key []byte) ([]byte, error)` for zero-allocation reads.

Both KV backends implement `sm.Hasher` and `sm.NALookup`. There is no `DiskStateMachine` KV backend at this time.

## Command Protocol

Source: `pkg/sm/kv/protocol.go`

All KV operations use a compact binary encoding. The wire format is:

```
[op:1][keyLen:2 LE][key:keyLen][value:remaining]
```

### Layout

| Field    | Size      | Encoding       | Description                        |
|----------|-----------|----------------|------------------------------------|
| `op`     | 1 byte    | unsigned       | Operation code (1=Put, 2=Get, 3=Delete) |
| `keyLen` | 2 bytes   | little-endian  | Length of the key                   |
| `key`    | `keyLen`  | raw bytes      | Key data                           |
| `value`  | remainder | raw bytes      | Value data (Put only)              |

The fixed header is 3 bytes (`headerSize`). Maximum key length is 65535 bytes (uint16). Value length is implicit: `len(data) - headerSize - keyLen`.

### Operation Codes

```go
const (
    OpPut    byte = 1  // Store key-value pair
    OpGet    byte = 2  // Reserved for read queries (no-op in Update)
    OpDelete byte = 3  // Remove a key
)
```

`OpGet` is a no-op when processed through `Update` because reads are served through the `Lookup` path, not through Raft proposals.

### Encoder Functions

```go
func EncodePut(key, value []byte) ([]byte, error)  // [1][keyLen:2 LE][key][value]
func EncodeGet(key []byte) ([]byte, error)         // [2][keyLen:2 LE][key]
func EncodeDelete(key []byte) ([]byte, error)      // [3][keyLen:2 LE][key]
```

### Decoder

```go
func DecodeCommand(data []byte) (Command, error)
```

Returns `*InvalidCommandError` for:
- Data shorter than 3 bytes (`headerSize`)
- Unknown operation code (outside 1-3 range)
- Key length exceeding available data

The decoded `Command` struct:

```go
type Command struct {
    Op    byte
    Key   []byte
    Value []byte
}
```

### Lookup Queries

`Lookup` accepts three query types, dispatched by type assertion:

| Query Type   | Behavior                                         |
|--------------|--------------------------------------------------|
| `[]byte`     | Returns value for key, or `*KeyNotFoundError`    |
| `string`     | Converted to `[]byte`, same behavior             |
| `ListQuery`  | Returns `[]string` of keys matching prefix, sorted |

```go
type ListQuery struct {
    Prefix string
}
```

`ListQuery` always returns a non-nil `[]string` (empty slice when no matches).

## MemoryStore

Source: `pkg/sm/kv/memory.go`

`MemoryStore` implements `sm.StateMachine` with a `sync.RWMutex`-protected `map[string][]byte`. A mutex is required because `Lookup` and `SaveSnapshot` run on separate goroutines from `Update`: `Lookup` is called from client request goroutines (`SyncRead`, `QueryLocalNode`, `StaleRead`), while `Update` runs on the engine's apply worker. `SaveSnapshot` may also overlap with `Update` when triggered by the engine's snapshot pool. The `RWMutex` allows concurrent readers (`Lookup`, `BytesLookup`, `GetHash`, `SaveSnapshot`) while serializing writes (`Update`, `RecoverFromSnapshot`, `Close`).

### Structure

```go
type MemoryStore struct {
    mu   sync.RWMutex
    data map[string][]byte
}
```

### Update Dispatch

Operations are dispatched via a map-based handler table for O(1) lookup:

```go
type opHandlerFunc func(m *MemoryStore, cmd Command) sm.Result

var opHandlers = map[byte]opHandlerFunc{
    OpPut:    handlePut,
    OpGet:    handleGetNoop,
    OpDelete: handleDelete,
}
```

Each handler returns an `sm.Result`:

| Operation | Result.Value           | Result.Data |
|-----------|------------------------|-------------|
| Put       | `len(value)` (bytes written) | nil   |
| Get       | 0 (no-op)              | nil         |
| Delete    | 1 (success)            | nil         |

Put copies the value before storing to avoid aliasing the entry's `Cmd` buffer.

### Lookup

`Lookup` acquires a read lock and returns a copy of the stored value, preventing callers from mutating internal state. Both the `Lookup` and `BytesLookup` paths hold `mu.RLock` for the duration of the read and allocate a copy before returning.

### GetHash

Computes a deterministic xxhash64 over sorted key-value pairs. Keys are sorted lexicographically, then each key and value is fed into the hasher in order. Two stores with identical data produce identical hashes regardless of insertion order.

```go
func (m *MemoryStore) GetHash() (uint64, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()

    h := xxhash.New()
    keys := make([]string, 0, len(m.data))
    for k := range m.data {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    for _, k := range keys {
        _, _ = h.Write([]byte(k))
        _, _ = h.Write(m.data[k])
    }
    return h.Sum64(), nil
}
```

### Use Case

Testing, small datasets, and scenarios where all state fits comfortably in memory. Simplest implementation path for custom state machines.

## ConcurrentStore

Source: `pkg/sm/kv/concurrent.go`

`ConcurrentStore` implements `sm.ConcurrentStateMachine` with sharded locking for concurrent read access during writes.

### Structure

```go
const numBuckets = 16

type bucket struct {
    mu   sync.RWMutex
    data map[string][]byte
}

type ConcurrentStore struct {
    buckets [numBuckets]bucket
}
```

Keys are assigned to buckets via `xxhash64(key) % 16`. This distributes load across 16 independent RWMutex-protected maps.

### Concurrency Model

| Method | Lock Type | Scope |
|--------|-----------|-------|
| `Update` (Put/Delete) | Write lock | Single bucket per entry |
| `Lookup` (single key) | Read lock  | Single bucket |
| `Lookup` (ListQuery)  | Read lock  | All 16 buckets |
| `BytesLookup`         | Read lock  | Single bucket |
| `GetHash`             | Read lock  | All buckets (two passes) |
| `PrepareSnapshot`     | Read lock  | All 16 buckets |
| `RecoverFromSnapshot` | Write lock | All 16 buckets |
| `Close`               | Write lock | All 16 buckets |

`Update` acquires and releases the write lock per-entry, not per-batch. This allows concurrent `Lookup` calls to read from unaffected buckets while a batch is being applied.

### PrepareSnapshot

Acquires read locks on all 16 buckets simultaneously, deep-copies every key-value pair into a flat `map[string][]byte`, then releases all locks. The returned clone is passed to `SaveSnapshot` which runs on a separate goroutine without holding any locks.

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

### Use Case

Read-heavy workloads where `Lookup` must not block on `Update`. The 16-bucket sharding reduces write contention compared to a single `sync.RWMutex`. The two-phase snapshot design ensures that `SaveSnapshot` never blocks `Update`.

## Snapshot Format

Both backends use the same binary snapshot format:

```
[count:4 LE][entry_1][entry_2]...[entry_N]
```

Each entry:

```
[keyLen:4 LE][key:keyLen][valLen:4 LE][val:valLen]
```

All lengths are `uint32` little-endian. The format is not sorted; iteration order depends on Go's map randomization. Recovery reads `count` first, then reads exactly that many entries.

### SaveSnapshot

1. Write `uint32(len(data))` as the entry count.
2. For each key-value pair, check the stopper channel, then write key length, key bytes, value length, value bytes.
3. Returns `context.Canceled` if the stopper channel is closed.

### RecoverFromSnapshot

1. Read the `uint32` entry count.
2. Reset internal storage (`MemoryStore` replaces the map; `ConcurrentStore` locks all buckets, clears all maps, reads entries into the correct buckets by rehashing each key, then unlocks all buckets).
3. For each entry, check the stopper channel, then read key/value lengths and data.
4. Returns `context.Canceled` if the stopper channel is closed.
5. Returns an error on truncated data or I/O failure.

## Factory Functions

Source: `pkg/sm/kv/factory.go`

Factory functions wrap the concrete constructors into `sm.CreateFunc` values suitable for `Host.StartShard`:

```go
func NewMemoryCreateFunc() sm.CreateFunc {
    return sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
        return NewMemoryStore()
    })
}

func NewConcurrentCreateFunc() sm.CreateFunc {
    return sm.NewConcurrentCreateFunc(func(shardID, replicaID uint64) sm.ConcurrentStateMachine {
        return NewConcurrentStore()
    })
}
```

The type-safe wrapper functions (`sm.NewCreateFunc`, `sm.NewConcurrentCreateFunc`) catch interface mismatches at compile time. The engine inspects the returned concrete type at startup to determine which state machine tier to use.

### Usage Example

```go
members := map[uint64]string{1: "10.0.0.1:4001", 2: "10.0.0.2:4001", 3: "10.0.0.3:4001"}
shardCfg := config.Config{ShardID: 100, ReplicaID: 1}

// In-memory backend
err := host.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg)

// Concurrent backend
err := host.StartShard(members, false, kv.NewConcurrentCreateFunc(), shardCfg)
```

## Error Handling

Source: `pkg/sm/kv/errors.go`

Two typed error types cover all error conditions:

### KeyNotFoundError

Returned by `Lookup` and `BytesLookup` when the requested key does not exist.

```go
type KeyNotFoundError struct {
    Key string
}

func (e *KeyNotFoundError) Error() string {
    return "kv: key not found: " + e.Key
}
```

### InvalidCommandError

Returned by `DecodeCommand` and `Update` for malformed commands, and by `Lookup` for unsupported query types.

```go
type InvalidCommandError struct {
    Reason string
}

func (e *InvalidCommandError) Error() string {
    return "kv: invalid command: " + e.Reason
}
```

Conditions that produce `InvalidCommandError`:

| Caller | Condition |
|--------|-----------|
| `DecodeCommand` | Data shorter than 3 bytes |
| `DecodeCommand` | Unknown operation code |
| `DecodeCommand` | Key length exceeds data |
| `MemoryStore.Update` | `DecodeCommand` failure propagated |
| `MemoryStore.Lookup` | Query is not `[]byte`, `string`, or `ListQuery` |
| `ConcurrentStore.SaveSnapshot` | `snapshotCtx` is not `map[string][]byte` |

## Interface Compliance

Both backends declare compile-time interface assertions:

```go
// MemoryStore
var (
    _ sm.StateMachine = (*MemoryStore)(nil)
    _ sm.Hasher       = (*MemoryStore)(nil)
    _ sm.NALookup     = (*MemoryStore)(nil)
)

// ConcurrentStore
var (
    _ sm.ConcurrentStateMachine = (*ConcurrentStore)(nil)
    _ sm.Hasher                 = (*ConcurrentStore)(nil)
    _ sm.NALookup               = (*ConcurrentStore)(nil)
)
```

## Cross-References

- [Architecture](../architecture/overview.md) -- State machine interfaces, factory registration, engine pipeline
- [Engine Pipeline](../engine/pipeline.md) -- How Update/Lookup/Snapshot calls are scheduled by workers
- [Session](../sessions/deduplication.md) -- At-most-once delivery, session-managed entries skipped by SM
- [Operations](../operations/deployment.md) -- CLI `--sm-type` flag, deployment configuration
