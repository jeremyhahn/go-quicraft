# State Machines — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/sm`, `pkg/sm/kv` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

This is the part of go-quicraft you actually write code against. You implement
one interface, hand the library a factory, and it handles consensus,
replication, snapshotting and recovery.

## Before you start

- Go 1.26.6 or later, and `github.com/jeremyhahn/go-quicraft` as a dependency.
- A clear answer to one question: **is your `Update` deterministic?** Every
  replica applies the same entries in the same order and must reach the same
  state. No clock, no random source, no network call, no map iteration order.
  Nothing in this library checks it for you, and divergence has no symptom until
  it is already replicated.

## Task: choose a tier

| If your state… | Implement | You get |
|---|---|---|
| fits in memory and reads are not hot | `StateMachine` | the simplest correct thing. `Update` and `Lookup` never overlap. |
| fits in memory and reads must run during writes | `ConcurrentStateMachine` | concurrent `Lookup`, and snapshots that do not block `Update` |
| lives in a disk store that already has durability | `DiskStateMachine` | the library stops duplicating your fsync, and hands replay back to you |

Start at tier 0. Moving up later is a contained change; discovering you needed
tier 0's serialization guarantee after building on tier 1 is not.

## Task: write a tier-0 state machine

```go
type Counter struct{ n uint64 }

func (c *Counter) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
    for i, e := range entries {
        delta := binary.LittleEndian.Uint64(e.Cmd)
        c.n += delta
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
err := host.StartShard(members, join,
    sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
        return &Counter{}
    }),
    cfg)
```

Use the typed wrapper for your tier — `NewCreateFunc`,
`NewConcurrentCreateFunc`, `NewDiskCreateFunc`. It makes the choice explicit
instead of inferred from a method set you may have got wrong.

## Task: fill in `results`

`results` is pre-sized to match `entries`. Write `results[i]` for `entries[i]`;
what you put there comes back to the proposer through
`RequestState.Result()`.

```go
results[i] = sm.Result{Value: applicationCode, Data: payload}
```

`Data` is optional. Do not return the reserved `Result.Value` constants the RSM
layer uses for session signalling (`ResultSessionExpired` and its neighbours).

## Task: handle `Cmd` correctly

```go
// Safe by default — Cmd is an owned copy.
value := e.Cmd

// When Config.ZeroCopyEntryCmd is true, Cmd aliases a reused internal buffer.
value := e.CopyCmd()
```

If you retain anything past `Update()` — putting it in a map, queueing it,
handing it to a goroutine — **copy it**. If you are not certain which mode you
run under, copy: the cost is one allocation, and the alternative is reading
another shard's bytes later.

## Task: implement a disk state machine

Two obligations beyond the tier-0 ones:

```go
func (d *DiskSM) Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error) {
    // Return the last index YOUR store durably applied. Raft replays from here.
    return d.store.LastAppliedIndex()
}
```

**Replay must be idempotent.** After a crash the library will hand you entries
your store has already applied, because its notion of "durable" and yours are
independent. Applying them twice must be a no-op — typically by persisting the
applied index in the same atomic write as the data.

`Sync()` is where you make your own writes durable. It is called by the library
at the points where durability matters.

## Task: turn on divergence detection

```go
func (c *Counter) GetHash() (uint64, error) { return c.n, nil }

var _ sm.Hasher = (*Counter)(nil)   // compile-time assertion — do this
```

Then compare across replicas at the same applied index:

```go
hash, appliedIndex, ok, err := host.StateMachineHash(shardID)
```

The compile-time assertion is not decoration. **Detection is by type assertion,
so a wrong signature produces no compile error and the capability is silently
never invoked** — you get `ok == false` and no explanation. The same applies to
`NALookup` and `NALookupInto`: two return values, exact signatures.

## Task: make reads allocation-free

```go
func (s *Store) BytesLookup(key []byte) ([]byte, error)              // NALookup
func (s *Store) BytesLookupInto(key, dst []byte) ([]byte, error)     // NALookupInto
```

The host then serves `SyncReadBuf` and `StaleReadBuf`, which return a buffer and
a `release` function instead of allocating. Call `release` when done.

## Task: cache a disk state machine

```go
cached, err := sm.NewCachedDiskStateMachine(inner, sm.CacheConfig{
    MaxEntries:   100000,
    ExtractKV:    func(cmd []byte) (string, []byte) { /* key + value from the COMMAND */ },
    EntryApplied: func(cmd []byte, r sm.Result) bool { /* did it actually apply? */ },
})
```

**If your state machine has conditional operations — compare-and-set,
put-if-absent, delete-if-match — you MUST set `EntryApplied`.** The cache is
populated from the value `ExtractKV` pulls out of the *command*, and a
conditional write may not have applied. Nothing in the library can infer the
outcome, because the encoding is yours. Omitting it caches values that were
never written.

## Task: start from a worked example

`pkg/sm/kv` has both a tier-0 (`MemoryStore`) and a tier-1 (`ConcurrentStore`)
implementation, with a command codec, a snapshot format, and `sm.Hasher`. Read
them; do not ship them. They are reference implementations and test fixtures —
a real key-value store is the database layer's job, not something to grow here.

## Troubleshooting

**Replicas disagree** — a non-deterministic `Update`. Look for, in order: a
clock, `rand`, map iteration order, a network or filesystem read, a pointer
address, and goroutine scheduling. Once you find it you also have to repair the
divergence, because the wrong state has been snapshotted and shipped.

**A capability I implemented never runs** — the signature is wrong, so the type
assertion fails silently. `GetHash() (uint64, error)`,
`BytesLookup(key []byte) ([]byte, error)`. Add
`var _ sm.Hasher = (*T)(nil)` and let the compiler tell you.

**The shard stopped after one bad entry** — that is the design. The apply path
is fail-fast: the shard is marked failed on the **first** `Update` error, with
no retry and no setting to add one. Fix the entry handling; a retry loop over
code that already failed either loops forever or applies twice.

**Reading garbage that changes between runs** — you kept an `Entry.Cmd` past
`Update()` under `ZeroCopyEntryCmd`. Copy it.

**Snapshots never happen** — `Config.SnapshotEntries` is 100000 by default and
**zero does not disable it**; zero means "unset" and selects the default. If you
meant to disable, assign `config.SnapshotsDisabled`. Never snapshotting also
means never reclaiming snapshot directories, never purging an old epoch's DEK,
and never invoking the at-rest barrier.

**`Update` is called with a huge batch** — bounded by
`Config.MaxApplyEntrySize`, default `MaxReplicationPayloadBytes` (~15.94 MB).
Lower it if your per-entry work is expensive enough that a full batch blows a
latency budget.

**`RecoverFromSnapshot` gets encrypted bytes** — it does not. Your state machine
always sees plaintext, whether or not at-rest encryption is on; the barrier
decrypts to a sibling temp file first.

**A panic in `Update` took out more than the shard** — engine workers restart on
panic so one shard cannot stall every shard mapped to that worker, but the state
your code left behind mid-apply is yours. Return errors; do not panic.

**Lookups block during writes on tier 0** — correct, and guaranteed. If you need
concurrent reads, implement `ConcurrentStateMachine`; the adapter will not fake
it.
