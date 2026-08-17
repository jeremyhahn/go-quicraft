# KV store backends

## Overview

`pkg/sm/kv` provides two reference state machine implementations: an in-memory store and a sharded concurrent store. Both speak the same binary command protocol, implement the optional `sm.Hasher`, `sm.NALookup` and `sm.NALookupInto` interfaces, and use the same snapshot format. They are usable as-is, and they are the shortest worked examples of the contract described in [State machine interfaces](interfaces.md).

There is no `DiskStateMachine` backend in this repository. The PebbleDB-backed store lives in [go-qrdb](https://github.com/jeremyhahn/go-qrdb).

Source: `pkg/sm/kv/`.

## Which tier each one is

| Store | Interface | Type code | What that buys you |
|-------|-----------|-----------|--------------------|
| `MemoryStore` | `sm.StateMachine` | 0 | `SaveSnapshot` runs with applies quiesced, so the save is the point-in-time |
| `ConcurrentStore` | `sm.ConcurrentStateMachine` | 1 | two-phase snapshot: `PrepareSnapshot` under the apply gate, streaming afterwards |

Both are found by type assertion on the value returned by `sm.CreateFunc` (`pkg/internal/rsm/statemachine.go:335`).

Both take locks, but only `ConcurrentStore` is required to. The RSM adapter serializes `Update` against `Lookup` for every tier today, so `MemoryStore`'s mutex is no longer what keeps a read off a half-applied batch. `ConcurrentStore` is held to its interface's contract, which promises concurrent reads during `Update` whatever the adapter happens to do. See [Lookup concurrency](interfaces.md#lookup-concurrency).

## Command protocol

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

### Operation codes

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

### Encoders

```go
func EncodePut(key, value []byte) ([]byte, error)
func EncodePutIfAbsent(key, value []byte) ([]byte, error)
func EncodeGet(key []byte) ([]byte, error)
func EncodeDelete(key []byte) ([]byte, error)
```

Each returns `*KeyTooLongError` when `len(key) > MaxKeyLength`. `EncodePutIfAbsent` produces the same wire shape as `EncodePut` and differs only in the opcode, so a reader that predates it rejects the command as an unknown op rather than silently applying a conditional write unconditionally.

### Decoder

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

### Lookup queries

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

## Update: decode, then mutate

Both stores run `Update` in two passes, and the split is not stylistic.

Pass one decodes every entry and touches nothing. An entry whose payload fails to decode gets `sm.ResultInvalidCommand` written into its result slot and is marked to be skipped.

Pass two applies. The handlers cannot fail, so no error return is possible after the first mutation and the all-or-nothing contract of `Update` holds without a rollback path.

This is also why an undecodable payload does not produce an `Update` error. The entry is already committed, so returning an error would fail the shard deterministically on every replica and wedge it forever on replay (`pkg/sm/kv/memory.go:95`).

## MemoryStore

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

### Update dispatch

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

### Reads

`Lookup` and `BytesLookup` hold the read lock and return an owned copy, so a caller cannot mutate stored state through the returned slice. `BytesLookupInto` copies into the caller's buffer instead, growing it with `append`, which is what lets `Host.SyncReadBuf` and `Host.StaleReadBuf` drop the adapter's read guard before handing the slice out.

### GetHash

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

## ConcurrentStore

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

### Locking

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

### PrepareSnapshot

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

## Snapshot format

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

### Save

1. Write `uint32(len(data))`.
2. For each pair, check the stopper, then write key length, key, value length, value.
3. Return `context.Canceled` if the stopper closed mid-stream.

### Recover

Both stores parse the entire stream into staging structures before touching live state, so a truncated or corrupt snapshot returns an error and leaves the existing data intact. `MemoryStore` swaps in one new map; `ConcurrentStore` takes all 16 write locks and swaps in 16 staged maps.

Two bounds guard the untrusted length prefixes, because a snapshot arrives from a peer over the transport:

- `keyLen > MaxKeyLength` (65535) and `valLen > MaxValueLength` (64 MiB) both return `errCorruptSnapshotLength` instead of attempting the allocation.
- The `count` prefix is not used directly as a map capacity. It is capped at `maxSnapshotMapHint` (1,048,576) for the pre-allocation; the map still grows past that if the snapshot genuinely holds more, and a count larger than the stream simply ends the loop early at EOF.

`MaxValueLength` is a denial-of-service ceiling, not the largest writable value. A KV value always travels inside one proposal's `Entry.Cmd`, which the engine caps at `proto.MaxEntrySize` (8 MiB) at propose time and the WAL caps at the same figure, so a legitimately stored value can never approach 64 MiB. Keeping the recovery bound above the write-time limit means tightening `MaxEntrySize` cannot retroactively make an already-persisted snapshot unrecoverable.

## Factory functions

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

## Errors

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

## Interface compliance

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

## Cross-references

- [State machine interfaces](interfaces.md), the contract both stores implement
- [RSM adapter](rsm-adapter.md), what calls `Update`, `Lookup` and the snapshot methods
- [Engine pipeline](../engine/pipeline.md), how apply and snapshot work is scheduled
- [Session deduplication](../sessions/deduplication.md), at-most-once delivery around these calls
- [Deployment](../operations/deployment.md), configuration and CLI options
