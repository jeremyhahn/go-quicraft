# State Machines — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/sm`, `pkg/sm/kv` ·
> Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

This is the part of go-quicraft you write code against. You implement one
interface, hand the library a factory, and it handles consensus, replication,
snapshotting and recovery.

## Before you start

- Go 1.27 or later, with `github.com/jeremyhahn/go-quicraft` as a dependency.
- Make sure your `Update` is deterministic. Every replica applies the same
  entries in the same order and must reach the same state: no clock, no random
  source, no network call, no dependence on map iteration order. Nothing checks
  this for you, and divergence has no symptom until it is already replicated.

## Choose a tier

| If your state… | Implement | You get |
|---|---|---|
| fits in memory and reads are not hot | `StateMachine` | the simplest correct option; `Update` and `Lookup` never overlap |
| fits in memory and reads must run during writes | `ConcurrentStateMachine` | concurrent `Lookup`, and snapshots that do not pause `Update`; you do the locking |
| lives in a disk store with its own durability | `DiskStateMachine` | no duplicated fsync, and replay handed back to you; no client sessions |

Start with `StateMachine`. Moving up a tier later is a contained change.

## Write a basic state machine

```go
type Counter struct{ n uint64 }

func (c *Counter) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		if len(e.Cmd) != 8 {
			results[i] = sm.Result{Value: sm.ResultInvalidCommand}
			continue
		}
		c.n += binary.LittleEndian.Uint64(e.Cmd)
		results[i] = sm.Result{Value: c.n}
	}
	return nil
}

func (c *Counter) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	return c.n, nil
}

func (c *Counter) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	return binary.Write(w, binary.LittleEndian, c.n)
}

func (c *Counter) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	return binary.Read(r, binary.LittleEndian, &c.n)
}

func (c *Counter) Close(ctx context.Context) error { return nil }
```

Register it:

```go
err := host.StartShard(members, false,
	sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return &Counter{}
	}),
	cfg)
```

Use the typed wrapper for your tier (`NewCreateFunc`, `NewConcurrentCreateFunc`,
`NewDiskCreateFunc`) so the compiler checks that your type implements it.

## Fill in `results`

`results` has the same length as `entries`, and the slice is reused between
calls, so write every `results[i]`. What you write comes back to the proposer
through `RequestState.Result()`.

- `Data` is optional.
- A payload you cannot decode gets `sm.ResultInvalidCommand`, with your state
  left unchanged. Do not return an error for it: the entry is committed, so an
  error would fail the shard on every replica.
- Never return `sm.ResultSessionExpired`; it is reserved.

## Keep `Update` all-or-nothing

Either every entry in the call is applied and every result written, or your
state is back where it started and you return an error. The simplest way to get
there is to decode and validate the whole batch first, then apply in a pass that
cannot fail.

`Update` can be called several times for one committed batch, and the indexes it
sees can have gaps (configuration changes and session bookkeeping entries never
reach you). Do not use `Index` to count entries.

## Handle `Cmd` correctly

```go
value := e.Cmd       // safe by default: Cmd is an owned copy
value = e.CopyCmd()  // required when Config.ZeroCopyEntryCmd is true
```

Under `ZeroCopyEntryCmd`, anything you keep past `Update`, in a map, a queue or
another goroutine, must be copied. If you are not sure which mode you run under,
copy.

## Implement a disk state machine

Two obligations beyond the basic tier:

```go
func (d *DiskSM) Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error) {
	// Return the last index YOUR store durably applied. Raft replays from here.
	return d.store.LastAppliedIndex()
}
```

Replay must be idempotent. After a crash you can be handed entries your store
already applied. The usual fix is to persist the applied index in the same
atomic write as the data.

`Sync()` makes your pending writes durable. It is called before every snapshot
capture.

Client sessions are not available on this tier (`ErrSessionsUnsupportedOnDisk`).
Make your commands idempotent and propose them with `Propose` or `SyncPropose`.

## Implement a concurrent state machine

`Lookup` and `GetHash` run while `Update` runs, so protect your state with your
own locks. Two rules:

- A reader must never see a half-applied batch. Hold your write lock (or locks)
  for the whole `Update` call.
- `PrepareSnapshot` must return a value that later `Update` calls cannot change:
  a deep copy, a copy-on-write view, or a database snapshot handle.
  `SaveSnapshot` runs while applies continue and must not touch live state.

## Turn on divergence detection

```go
func (c *Counter) GetHash() (uint64, error) { return c.n, nil }

var _ sm.Hasher = (*Counter)(nil) // compile-time check
```

Then compare replicas at the same applied index:

```go
hash, appliedIndex, ok, err := host.StateMachineHash(shardID)
```

The hash must not depend on iteration order: sort keys before hashing. On a
concurrent or disk state machine it must also cover whole batches, so take the
same lock `Update` holds.

Keep the compile-time assertion. A wrong signature compiles, and the capability
is silently never called; you just get `ok == false`. The same applies to
`BytesLookup` and `BytesLookupInto`.

## Make reads allocation-free

```go
func (s *Store) BytesLookup(key []byte) ([]byte, error)          // sm.NALookup
func (s *Store) BytesLookupInto(key, dst []byte) ([]byte, error) // sm.NALookupInto
```

With `BytesLookupInto`, `Host.SyncReadBuf` and `Host.StaleReadBuf` read into a
pooled buffer and return it with a `release` function. Copy the value into
`dst` (growing it with `append`), never return your own internal slice, and call
`release` when you are done with the result.

## Cache a disk state machine

```go
cached, err := sm.NewCachedDiskStateMachine(inner, sm.CacheConfig{
	MaxEntries:   100000,
	ExtractKV:    extractKV,    // key and value from the COMMAND bytes
	EntryApplied: entryApplied, // did this entry actually change the store?
})
```

If any operation is conditional (compare-and-set, put-if-absent,
delete-if-match), you must set `EntryApplied`. The cache is filled from the
value in the command, and a conditional write may not have taken effect. Without
it the cache serves values that were never written.

## Start from a worked example

`pkg/sm/kv` has a basic (`MemoryStore`) and a concurrent (`ConcurrentStore`)
implementation with a command codec, a snapshot format and `sm.Hasher`. Read
them; do not ship them. They are reference implementations, not a database.

## Troubleshooting

**Replicas disagree.** `Update` is non-deterministic. Look for a clock, `rand`,
map iteration order, a network or file read, a pointer address, or goroutine
scheduling. Fixing the code is not enough: the wrong state has already been
snapshotted and shipped, so it has to be repaired too.

**A capability I implemented never runs.** The signature is wrong and the type
assertion fails silently. Add `var _ sm.Hasher = (*T)(nil)` and let the compiler
tell you.

**The shard stopped after one bad entry.** That is the design: the first
`Update` error or panic fails the shard, with no retry. Handle undecodable
payloads with `sm.ResultInvalidCommand` instead of an error.

**Reading garbage that changes between runs.** You kept an `Entry.Cmd` past
`Update` under `ZeroCopyEntryCmd`. Copy it.

**Snapshots never happen.** `Config.SnapshotEntries` defaults to 100000, and
zero selects that default rather than disabling. To disable automatic snapshots,
assign `config.SnapshotsDisabled`. Without snapshots, old snapshot directories
are never reclaimed and the log is never compacted.

**`Update` gets a huge batch.** One apply batch is capped at 64 MiB of entries,
and each `Cmd` at `Config.MaxEntrySize` (8 MiB). Keep per-entry work cheap or
expect latency proportional to batch size.

**`RecoverFromSnapshot` gets encrypted bytes.** It does not. Your state machine
always reads plaintext, with or without at-rest encryption.

**A panic in `Update`.** The panic is recovered and logged with its stack, and
the shard is marked failed. The proposal that was applying fails with the
`*SMPanicError` (`errors.As` reaches the panic value and stack), and every other
proposal pending on the shard with `ErrShardFailed`; both report
`MaybeApplied`, because the entries are committed and a reload replays them.
Other shards keep running. `OnShardFailed` fires once. Return errors instead of
panicking. A panic in `SaveSnapshot` is different: the shard keeps running and
the snapshot request fails.

**Lookups block during writes.** On the basic tier that is guaranteed. If you
need concurrent reads, implement `ConcurrentStateMachine`.

**Hash mismatches between replicas at the same index on a concurrent state
machine.** `GetHash` is reading part-way through a batch. Hold the same lock
`Update` holds for the whole hash.
