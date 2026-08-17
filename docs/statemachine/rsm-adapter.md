# RSM adapter layer

## Purpose

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

## Type detection

### Why the order matters

`DiskStateMachine`'s method set is a strict superset of `ConcurrentStateMachine`'s: it has `Update`, `Lookup`, `PrepareSnapshot`, `SaveSnapshot`, `RecoverFromSnapshot` and `Close`, plus `Open` and `Sync`. A type switch that checked `ConcurrentStateMachine` first would match an on-disk state machine and wrap it with the wrong adapter, silently dropping `Open` and `Sync`. Dropping `Open` means the applied index the store recovered from its own durable state is never adopted, and the replica re-applies from wherever the log happens to start.

### wrapStateMachine

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

### Construction

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

## The internal wrapper interface

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
> What you implement is two-valued. `sm.Hasher` is `GetHash() (uint64, error)` and `sm.NALookup` is `BytesLookup(key []byte) ([]byte, error)`. Writing the three-value form satisfies nothing, produces no compile error, and leaves the optimized path silently never invoked. See [State machine interfaces](interfaces.md#optional-interfaces).

`saveSnapshotGated` writes your snapshot and invokes `afterCapture` exactly once, after the point-in-time is captured and before the streaming begins. For a regular in-memory state machine the whole save *is* the capture, so it fires at the end. For concurrent and on-disk machines it fires right after `PrepareSnapshot`, which lets the caller release the apply gate while the immutable snapshot streams.

## Apply pipeline

`Apply` is called by the engine's apply worker with the committed batch and a pre-allocated `[]sm.Result` of the same length.

### Preconditions

Two checks run before a single entry is classified.

**Read guard, held for writing.** `Apply` takes the state machine's `readGuard` in **write** mode and returns `ErrClosed` if `Close` already ran.

The write mode is the enforcement of `sm.StateMachine`'s published contract: QuicRaft serializes all method calls, and `Update` and `Lookup` are never called concurrently. Every read entry point takes the same guard for reading, so an apply excludes them all. See [Lookup concurrency](interfaces.md#lookup-concurrency) for the tier-by-tier consequences and the latency it adds.

The guard also carries the closed check. The engine's synchronous `UnloadNode` guarantees no `Apply` is in flight when `Close` runs, so that part is defence in depth, but holding the guard forces `Close` to wait out an in-flight `Apply` rather than closing the state machine underneath it.

**Contiguity guard.** The batch must not start above `lastApplied + 1`, or `Apply` returns `ErrApplyGap` and fails the shard.

`lastApplied` is not bookkeeping here. It is the lower bound of the expiry checkpoint walk below, so a batch that leaves a hole beneath it makes this replica evaluate expiry over a different index range than the replicas that applied the same log contiguously. That diverges the state machines with every Raft invariant intact. A hole has two causes and both are fatal: committed entries were dropped before reaching the state machine, or `lastApplied` was never seeded from the index the shard resumed at. Failing loudly is the point; the alternative is a replica that quietly answers `ResultSessionExpired` for entries its peers apply normally.

The engine's apply worker makes the same assertion one layer down against its own counter, as an equality rather than an upper bound, because the two layers see different streams: the worker filters entries at or below its marker before checking, whereas an already-applied entry that reaches the adapter is the documented on-disk replay case and stays a no-op (`pkg/internal/engine/apply.go:718`).

### Deterministic expiry segmentation

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

### Entry classification

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

### Session entry detection

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

### proto.Entry to sm.Entry

| `proto.Entry` field | `sm.Entry` field | Why |
|---------------------|------------------|-----|
| `Index` | `Index` | preserved |
| `Cmd` | `Cmd` | preserved; owned copy by default, aliased under `ZeroCopyEntryCmd` |
| `Term` | dropped | state machines are term-agnostic |
| `Type` | dropped | entry type flags |
| `Key` | dropped | internal dedup key |
| `ClientID`, `SeriesID`, `RespondedTo`, `OwnerID` | dropped | session routing, sequencing, eviction watermark, owner cap |

Because whole classes of entry are filtered out, the `Index` values in one `[]sm.Entry` are strictly increasing but not contiguous.

### Buffer reuse

```go
smEntries := s.smEntryBuf[:0]
smResults := s.smResultBuf[:0]
indexMap  := s.indexMapBuf[:0]
```

`indexMap` maps a position in `smResults` back to its position in the caller's `results`. All three are retained at their grown capacity for the next call. Safe because `Apply` runs on a single apply goroutine per shard and the buffers are fully consumed before it returns.

### Zero-copy and result caching

`StateMachine.zeroCopyEntryCmd` mirrors `Config.ZeroCopyEntryCmd`:

- **`false` (default):** for each non-empty `Cmd`, `Apply` allocates an owned copy before building the `sm.Entry`, so the state machine may retain it past `Update`.
- **`true`:** `sm.Entry.Cmd` aliases the engine's pooled `CommittedEntries` buffer directly. The state machine must copy `Cmd` if it retains it, because the engine recycles that buffer after the apply.

Result data gets copied on the way into the session cache regardless of the mode, for two independent reasons (`pkg/internal/rsm/statemachine.go:815`):

1. The same `smResults[j].Data` was just handed to the client through `results[origIdx]`. If the application mutates that buffer it corrupts the replica-local dedup cache and, through `MarshalTo`, the next snapshot. That is a determinism hazard, not just a data race.
2. In zero-copy mode the state machine may have returned `Data` aliasing the pooled buffer the engine is about to recycle.

The client keeps the original buffer; only the cached copy is owned. Cached results handed back on a duplicate hit are likewise copied out of the cache before being written into `results` (`pkg/internal/rsm/statemachine.go:694`).

### Duplicate detection

```
sessions.CheckDuplicate(clientID, seriesID, index)
   |-- ErrSessionNotFound / ErrResponseEvicted -> ResultSessionExpired
   |-- found in response cache                 -> return the cached result
   |-- not found                               -> new proposal, go to the admission gate
```

`ErrResponseEvicted` means the series already completed and its cached response was evicted because `seriesID <= respondedTo`. Both error cases map to `ResultSessionExpired` per Raft PhD figure 6.1 step 3: "Reply SESSION_EXPIRED if no record of clientId or if response for client's sequenceNum already discarded."

### In-batch duplicates

A client may retry a request whose original proposal has not yet committed. If both the original and the retry commit and land in the same apply batch, both carry the same `(clientID, seriesID)` and neither is in the session table yet, so `CheckDuplicate` reports both as new. Applying both would run the operation twice.

`batchSeen` maps `(clientID, seriesID)` to the entry index of the first occurrence within the batch. A later occurrence is recorded in `mirrors` and skips the user state machine entirely; after `Update` returns, each mirror copies the result of its first occurrence. The mirror does not call `RecordResult` again, since the first occurrence already cached that response and a second record could push a genuine new series out of the cache.

This check runs *before* the admission gate. A retry that lands in the same batch as its original consumes no new cache slot, so gating it could reject it as "cache full", and applying it would mutate the state machine a second time.

### The admission gate

```
sessions.CanRecord(clientID, respondedTo, admittedThisBatch[clientID])
   |-- room in the response cache -> admittedThisBatch[clientID]++ ; forward to user SM
   |-- over capacity              -> ResultSessionExpired ; do NOT apply
```

The gate runs before the state machine mutation, and that ordering is the whole point. `RecordResult` only runs after the batched `Update`. If the response cache is already full (a client not advancing its `respondedTo` watermark), applying the entry without being able to cache its result would let a later replay find no cached response and re-apply the mutation. Rejecting up front, treated identically to an evicted response, keeps the entry from ever reaching the state machine.

`admittedThisBatch` is a per-client counter, lazily allocated on the first session-managed entry, added to the in-cache count by `CanRecord` so a batch of new series cannot collectively overflow the cache. It is index- and series-driven with no wall clock, so it is deterministic across replicas.

### Sessions that change within one batch

`sessionLifecycleAt` maps a client ID to the entry index of the most recent successful `Register` or `Unregister` in this batch. An entry admitted earlier in the batch can reach `RecordResult` after the session it was admitted against is gone, replaced or removed by a later entry in the same batch.

An entry whose client has a lifecycle transition at a *higher* index than its own must not record a response. The entry was legally applied, since it precedes the transition in the log, so its result stands. But for a closed session the cache is moot, and for a re-registered client ID, caching into the replacement session would deduplicate a future proposal of that series which the replacement session never applied.

A successful `Register` or `Unregister` also drops that client's `batchSeen` and `admittedThisBatch` entries. A fresh session incarnation starts its series numbering over, so stale in-batch bookkeeping from a prior incarnation would either mirror the new session's first proposal to the wrong result or reject it against a stale count.

### Recording results

```
sessions.RecordResult(clientID, seriesID, respondedTo, index, result)
   |-- advance the respondedTo watermark
   |-- evict cached responses with seriesID <= respondedTo
   |-- store the result in the session cache
```

`ErrSessionNotFound` here is tolerated and logged: a missing session is a property of the committed log, not an infrastructure failure, so returning it would fail the shard deterministically on every replica. The `sessionLifecycleAt` check already covers every in-batch removal path; the tolerance is defence in depth against a future path that drops a session mid-batch. Any other error is an invariant violation and stays fatal. `ErrResponseLimitExceeded` is unreachable from here, because the `CanRecord` gate already refused anything that could not be cached.

### lastApplied

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

## Read paths

`Lookup`, `NALookup`, `NALookupBuf` and `GetHash` all take the `readGuard` in read mode and return `ErrClosed` if `Close` already ran. In-memory backends do not self-track closure and would otherwise return stale data after `Close`. Because `Apply` holds the same guard for writing, none of the four can run while an apply is in flight.

### Lookup

```go
func (s *StateMachine) Lookup(ctx context.Context, query interface{}) (interface{}, error)
```

For a `[]byte` query it tries `NALookup` first and falls through to the generic `Lookup` when the state machine does not implement `sm.NALookup`. The read guard serializes this against `Apply`, which holds the same mutex for writing, so a user `Lookup` never overlaps a user `Update`.

### NALookupBuf

```go
func (s *StateMachine) NALookupBuf(key []byte) (result []byte, release func(), supported bool, err error)
```

The pooled path behind `Host.SyncReadBuf` and `Host.StaleReadBuf`. It takes a `[]byte` from `lookupBufPool` (initial capacity 256, pooled as `*[]byte` so the pool entry does not escape), calls `sm.NALookupInto`, and hands back the filled slice with a release function. When the state machine does not implement `NALookupInto` it falls back to `NALookup` with a no-op release.

Two properties are load-bearing:

- **The read guard is dropped before returning, not held for the lifetime of the buffer.** `release` is called by application code, so a caller that never releases would otherwise pin the read lock forever. Once `Close` queues as a writer, every subsequent lookup on the shard queues behind it. Holding it is also unnecessary, because the `sm.NALookupInto` contract requires implementations to copy into `dst`, so the returned slice does not alias state machine memory.
- **`release` is guarded by a `sync.Once` covering the whole body, `Put` included.** Guarding only part of it would let a double release put the same pointer into the pool twice, after which two goroutines hold the same buffer and silently overwrite each other's read results.

### GetHash

Delegates through the wrapper's type assertion and reports `(0, false, nil)` when the state machine does not implement `sm.Hasher`. `Host.StateMachineHash` pairs the hash with the applied index so a cross-replica comparison is meaningful.

It takes the read guard for the same reason `Lookup` does. Without it, the hash was computed concurrently with `Apply`, so a divergence check could observe a half-applied batch and report a mismatch that does not exist, and concurrently with `Close`, reading a closed state machine.

## Snapshots

### Format

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

### Save

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

The column says apply *gate* rather than applies for a reason. `SaveSnapshotGated` holds the `readGuard` in read mode for the whole call, streaming included, and `Apply` takes that guard for writing. So on the concurrent and on-disk wrappers the gate is released after `PrepareSnapshot` as the table says, and an apply arriving during the stream still blocks on the guard until the stream ends. Reads queue behind that apply.

### Recover

```go
func (s *StateMachine) RecoverFromSnapshot(ctx, r, stopper) error
```

1. Closed guard, then stopper check.
2. `refreshSessionPolicy()`. `UnmarshalFrom` bounds-checks the table against the manager's limits, and the table was written by a replica applying the *shard's* policy. Checking it against this node's configuration instead would reject a snapshot the rest of the shard considers well-formed, and that rejection is fatal to recovery. For a replica bootstrapped by an `InstallSnapshot` this is also the first point at which any policy is known.
3. Decode the session table into a **staged** manager.
4. `smWrapper.RecoverFromSnapshot`.
5. Only on success, adopt the staged manager.

Staging is what makes recovery all-or-nothing. Decoding straight into the live table replaced it before the user restore ran; if the user restore then failed on a truncated payload, the node kept user data from index N beside session metadata from a later index M. A client retry could be answered from a cached result for an operation the state machine never applied, or a cached response could vanish and let a duplicate command execute twice. Neither surfaces an error anywhere. The reader is sequential, so staging costs nothing: `UnmarshalFrom` advances `r` to exactly the position the user restore must start from whether or not the staged table is adopted.

## Optional interface plumbing

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

## Errors

### Adapter sentinels (`pkg/internal/rsm/errors.go`)

| Error | When |
|-------|------|
| `ErrInvalidSMType` | `CreateFunc` returned a type implementing none of the three interfaces |
| `ErrStopped` | the stopper channel was already closed when a snapshot operation started |
| `ErrClosed` | `Apply`, a read, or a snapshot operation was attempted on a closed state machine |
| `ErrApplyGap` | the offered batch does not start at `lastApplied + 1` |

These are defined here rather than in the public `quicraft` package (`pkg/`) to avoid an import cycle. `Host.StartShard` translates `rsm.ErrInvalidSMType` into the public `quicraft.ErrInvalidSMType` (`pkg/host.go:3494`).

### Session sentinels (`pkg/internal/session/errors.go`)

| Error | When |
|-------|------|
| `ErrSessionLimitExceeded` | the shard is at `MaxTotalSessions` |
| `ErrSessionNotFound` | an operation named a client session that does not exist |
| `ErrSessionAlreadyExists` | registering an already-registered client |
| `ErrResponseLimitExceeded` | a session's response cache is full |
| `ErrResponseEvicted` | the series already completed and its response was evicted |
| `ErrSnapshotCorrupted` | sentinel behind `BoundsError` for `errors.Is` checks on a malformed session table |

### Propagation rules

- Errors from `smWrapper.Update` propagate straight out of `Apply`; the engine treats them as fatal for the shard.
- `ErrApplyGap` fails the shard, deliberately.
- Register and unregister errors are handled inline and never fatal. They are idempotent operations expected during replay: a duplicate register after snapshot recovery, or a double unregister from a retry, must not kill the node. A failed register yields `sm.Result{}`; a failed unregister yields `sm.Result{Value: sm.ResultSessionExpired}`, so the host layer surfaces it as `ErrSessionExpired` and then `ErrRejected` to the caller.
- `ErrSessionNotFound` from `RecordResult` is logged and swallowed. Any other `RecordResult` error is fatal.
- Errors from `SaveSnapshot`, `RecoverFromSnapshot` and `Close` propagate to the engine.
- Nothing is discarded into `_`.

## Wrapper behavior summary

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

## Inspection accessor

`StateMachine.Sessions()` returns the live `session.Manager`, the same object `Apply`, `RecoverFromSnapshot` and `SaveSnapshotGated` mutate. `session.Manager` is not safe for concurrent use, and there is no lock here to make it so. Every method on it, including the read-only-looking `Count` and `IsRegistered`, may be called only when no apply and no snapshot save can be in flight for the shard.

It has no production caller. It exists for tests in this package and for the host's session-policy tests, which assert the limits the apply path actually resolved. A future caller wanting one of these numbers on a live shard needs a value snapshotted on the apply goroutine, not this pointer.

## Cross-references

- [State machine interfaces](interfaces.md), the contract this adapter enforces
- [KV backends](kv-backends.md), two implementations of it
- [Engine pipeline](../engine/pipeline.md), the apply worker and the snapshot pool
- [Session deduplication](../sessions/deduplication.md), the session manager internals
- [Raft protocol](../raft/protocol.md), snapshot transfer and membership changes
- [Public API](../api/reference.md), `Host.StartShard` and the read methods
