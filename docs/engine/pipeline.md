# Engine Worker Pipeline

Source: `pkg/internal/engine/`

## Overview

The engine drives Raft consensus through a three-stage pipeline: step worker (Raft logic), commit worker (persistence), apply worker (state machine). The design buys pipelining (free-order network I/O overlapped with disk I/O), batching, and single-owner semantics via the `commitPending` atomic flag.

```
Host.Propose()  Host.ReadIndex()  transport
      |               |               |
   proposalQ      readIndexQ       inboxQ        (MPSC EntryQueues)
      \               |               /
       +--------------+--------------+
                      |
                 STEP WORKER  ── Peer.Handle() ── Peer.GetUpdate()
                      |                 |
                      |                 +──> QUIC send: FREE-ORDER msgs
                      |                      (Heartbeat, Replicate,
                      |                       RequestVote) BEFORE persist
                 commitPending = true
                      |
                   commitC
                      |
                COMMIT WORKER ── logdb.SaveState(batch)
                      |                 |
                      |                 +──> QUIC send: ORDERED msgs
                      |                      (VoteResp, HeartbeatResp,
                      |                       ReplicateResp) AFTER persist
                      |                 +──> OnCommitted (if NotifyCommit)
                 Peer.Commit()
                      |
                   applyC
                      |
              APPLY DISPATCHER ──> per-shard goroutine
                                          |
                                     RSM adapter ──> user state machine
                                          |
                                     OnApplied ──> RequestState.complete()
                                          |
                                   commitPending = false
```

**Partitioning**: Shards hash to workers via `shardID % NumWorkers`. Shards in different partitions never contend; shards sharing a partition share one goroutine per tier.

**Message ordering is a safety boundary, not a tuning knob.** Free-order messages go out from the step worker before the WAL persist, which is what overlaps network and disk I/O. Ordered messages — a vote grant, a heartbeat or replicate acknowledgement — go out from the commit worker only after `SaveState` returns, because each of them promises a state change is durable. Sending a `VoteResp` before the vote is on disk lets a crash-and-restart node vote twice in one term. Raft PhD §10.2.1.

**Batching**: The commit worker blocks on the first item, then non-blocking-drains whatever accumulated while the previous persist was in flight, and persists the lot in one `SaveState`.

**Single-owner**: The `commitPending` flag ensures only one worker tier touches a shard's Peer at any time. `Peer.Tick()` is the deliberate exception; see the Step Worker section.


## Engine Lifecycle

**Initialization**: `NumWorkers` (workers per tier: step/commit/apply, 0 means `runtime.NumCPU()`), `MaxApplyWorkers` (apply semaphore), `CommitCBufferSize` (commit channel, default 64) and a derived apply channel buffer of `max(CommitCBufferSize*4, 256)`, `RTTMillisecond` (tick interval). The snapshot pool gets `max(NumWorkers/2, 2)` workers. Tick interval derived from RTTMillisecond maps Raft election/heartbeat timeouts to real network latency.

**Memory mode**: when the LogDB is nil, step workers set `memoryMode` and inline `Peer.Commit`, forwarding straight to `applyC`. The commit worker goroutine is bypassed and the pipeline is two hops instead of three.

**Startup**: The listener dispatcher starts FIRST — until its goroutine is running, every notification would be invoked inline on the producing worker, which is exactly the reentrancy hazard it exists to remove. Then workers launch in reverse pipeline order (snapshot, apply, commit, step), so downstream consumers are ready before upstream producers emit. Each tier is tracked by a `sync.WaitGroup` for sequential shutdown.

**Restart on panic**: all three worker tiers run inside a restart loop. A panicking worker would otherwise stall every shard mapped to it permanently. The step-worker recovery clears `commitPending` for the single node that was mid-enqueue; the commit-worker recovery clears it for every node that worker owns. Both re-enter after a short delay so a deterministic panic does not become a tight loop.

**Shutdown (ordered)**: Guarantees no in-flight items lost between tiers:
1. `Node.Stop()` for all (close stopC channels, unblock snapshot ops)
2. `close(stepStopC)` → wait (no new Updates)
3. `close(commitStopC)` → drainAndProcess → wait (persist remaining commitC items)
4. `close(applyStopC)` → drain remaining applyC → wait (all per-shard goroutines finish)
5. `close(snapshotStopC)` → wait
6. Stop the listener dispatcher → wait

The dispatcher is stopped LAST, once every tier that can produce a notification has exited, and it drains what they queued before returning. A leadership loss or shard failure observed during teardown therefore still reaches the listener. A listener that blocks here blocks `Stop` — the same way it would previously have blocked its producing worker, except the dispatcher goroutine holds no engine lock.

Commit workers forward to apply unconditionally (no stopC escape) after persist: entries MUST be applied. Ordered shutdown guarantees apply workers still running.

**Node management**: `LoadNode` adds to `allNodes` and to the step worker at partition `shardID % NumWorkers`. `UnloadNode` reverses via `unloadC`. `DeliverMessage` (transport boundary) looks up the node under a read lock and calls `node.Deliver(msg)`, which stamps the message `OriginRemote`.


## Step Worker

**Source**: `step.go`

Drains proposals from the MPSC `EntryQueue` and ReadIndex requests from the dedicated `readIndexQ`. For each input, calls `Peer.Handle()` to process the Raft state change. Also drives Raft logical clocks via `Tick()` at intervals derived from `RTTMillisecond`. Produces `Update` (entries to persist, messages to send, committed entries) forwarded to commit worker.

**Main loop**: Woken by WorkSignal (message arrival, apply completion) or ticker. Drains inbox into reusable buffer, calls `peer.Handle()` for each message, checks `peer.HasUpdate()`, retrieves `Update`, and sends to commit worker via `commitC` (blocking with `stopC` escape).

**ReadIndex batching (OPT-8)**: ReadIndex requests accumulated in dedicated MPSC queue, drained as a batch with single key used as batchKey for coalescing. Multiple callers alias complete when batchKey resolves via `OnReadIndexBatched` callback.

**ReadOnly bypass**: When `Update.ReadOnly` flag set (no entries/state/snapshot), skips commit/apply pipeline entirely. Reduces ReadIndex latency from ~300us (3 goroutine hops) to ~5us (inline callback).

**Campaign detection**: Scans outbound `RequestVote`/`RequestPreVote` messages and calls `callback.OnCampaignLaunched(shardID, replicaID, term)` — cold path (elections seconds apart).

**Message pipelining**: Separates `InstallSnapshot` messages (routed to `sender.SendSnapshot()` for chunked streaming) from regular protocol messages (batched through `sender.Send()`). Only free-order messages leave here, before the entries are persisted; that is what overlaps network and disk I/O. Ordered messages are carried on the `commitItem` and released by the commit worker after `SaveState`.

**Ownership guard**: Checks `node.commitPending.Load()` before calling `Handle()` or `processReady()`. When true, the node is skipped for `Handle`/ready processing to prevent concurrent Peer access with the commit and apply workers.

`Tick()` is the exception: it runs **unconditionally**, regardless of `commitPending` state. This is required for leader liveness — the leader must always advance its heartbeat timer and send heartbeats, even while WAL fsync and state machine apply are in progress. Without it, a commit+apply cycle (200-500us under load) starves heartbeat delivery, followers time out and campaign, and the leader then fails `checkQuorum` and steps down.

What prevents `Tick()` racing `Commit()` is the per-node `raftMu` mutex, which serializes the two. Contention is minimal: `Tick()` is ~100ns (counter increment, conditional heartbeat broadcast) and `Commit()` ~200ns (marker updates, inmem truncation), and both run at most once per RTT interval. `raftMu` is **not reentrant** — event callbacks such as `OnLeaderUpdated` are invoked outside it, because a callback running under `raftMu` would block the commit worker for every shard that worker owns.

When `commitPending` is true, a `Tick()` may still produce messages that must go out immediately. `Peer.DrainFreeOrderMessages()` extracts only the free-order set — `Replicate`, `Ping`, `Heartbeat`, `Quiesce`, `RequestPreVote`, `RequestPreVoteResp` — and leaves ordered messages in the outbound buffer for the next `GetUpdate` cycle. Using the plain `DrainMessages()` here would silently drop a `VoteResp` produced by `processCommitPendingInbox`, so elections could not complete during a commit+apply cycle. Nothing ordered is ever sent before the WAL persist completes.

**Deferred InstallSnapshot redelivery**: Inbound `InstallSnapshot` messages that arrive during a `commitPending` window are buffered per-shard in `deferredSnapshots` rather than re-delivered to the inbox (avoiding a CPU-burning drain-redeliver loop). When the shard becomes processable again, `redeliverDeferredSnapshots` re-injects them via `node.Deliver()`. Only messages that `Deliver()` reports as accepted are dropped; messages rejected because the inbox is full are retained (compacted to the front of the buffer) for a later retry, so a transient full inbox never permanently loses a deferred snapshot.

**Batched proposal queue**: `internal/queue.EntryQueue` is MPSC with a *brief* `sync.Mutex`, not lock-free. Multiple producers take the lock only for a slice append (typically <50ns); the single consumer takes it once per drain cycle to `Swap` the write buffer with a caller-provided read buffer, regardless of item count. That amortization — one acquisition per drain rather than per item — is what reduces step worker contention versus a shared proposals map. `maxLen` provides bounded backpressure: `Add`/`AddOne` return false at capacity and add nothing. A refused proposal surfaces as `ErrDropped` (with `IncProposalDropped` and the `OnProposalDropped` event); a refused ReadIndex surfaces as `ErrSystemBusy`. `ErrSystemBusy` is reserved for the global `MaxTotalInMemLogSize` rate limiter, so reporting it for a full per-shard proposal queue would send an operator to the wrong metric.


## Commit Worker

**Source**: `commit.go`

Receives `Update` from step worker, persists entries to LogDB (sharded WAL with 16 shards), calls `Peer.Commit()` to advance internal state, forwards to apply worker. Uses drain-then-batch pattern: blocks on first item, non-blocking drains remaining items while previous persist completes, batches them together.

**Ordered-message send**: The commit worker holds back every ordered message the step worker produced (`commitItem.orderedMsgs`, accumulated per shard in `orderedMsgsBuf`) and sends them only after `SaveState` succeeds. `InstallSnapshot` is routed to `SendSnapshot` here as it is in the step worker. The overlap between network and disk I/O comes from the step worker sending free-order messages before the persist, not from sending these concurrently with it.

**ReadOnly bypass**: When `Update.ReadOnly` flag set, skips persistence entirely and bounces back to step worker. Reduces latency since no entries need durability.

**Piggyback commit (O29)**: On a multi-node proposal, entries are persisted in cycle 1 and the commit index advances during the `commitPending` window via `processCommitPendingInbox`. Rather than making those entries wait for a second full pipeline traversal, `Peer.GetPiggybackUpdate(lastApplied)` extracts the newly committable entries and applies them in the same cycle. It is deliberately narrower than `GetUpdate`: it writes no `EntriesToSave` (the entries it applies are clamped to the durable marker), leaves `readStates` and pending snapshots alone, releases only the messages the durability rules permit, and does not advance `prevState` — the engine does that through `NotePiggybackPersisted` once the hard state is on disk, so a failed persist re-emits it.

**CommitNotify**: When `HostConfig.NotifyCommit` is enabled, the commit worker fires `Callback.OnCommitted(shardID, entries)` after `SaveState` succeeds and before the apply forward, per update so the callback is scoped to one shard. This lets a caller learn an entry is WAL-durable before it is applied.

**SaveState retry**: 5 attempts with 4 inter-attempt waits — 10ms, 50ms, 200ms, 500ms, for ~760ms of pre-failure grace. The final attempt is not followed by a wait, which is why the schedule has one fewer element than there are attempts. Both terminal outcomes — retry exhaustion AND shutdown cancellation (`stopC` fired during a retry backoff) — funnel through the shared `failBatchSaveState` helper. For every node in the batch it marks `failed = true`, clears `commitPending`, reports a `SaveStateError` via `handleError`, and fails the batch's pending proposals via `OnProposalFailed`, so callers see an immediate error rather than blocking until their context deadline. The batch's entries were extracted but never persisted, so the node is left permanently failed (recover by reloading the shard) to avoid acknowledging unpersisted entries.

**Peer.Commit()**: Advances internal Peer state (log truncation, stable storage markers). Populates `LogReader.termRingBuffer` (O(log n) binary search term lookups, zero GC pressure) for efficient term resolution on restart.

**Forward to apply (unconditional)**: After successful persist, sends `applyItem` to apply worker via blocking send WITHOUT `stopC` escape. Persisted entries MUST be applied (dropping them leaves committed-but-unapplied state). Ordered shutdown (apply workers stop after commit) guarantees consumer is running.


## Apply Worker

**Source**: `apply.go`

Per-shard goroutines gated by semaphore (MaxApplyWorkers limit). Fan-out dispatcher reads from `applyC` and routes each item to per-shard goroutine via `shardChans[shardID]`. Each shard applies committed entries sequentially to RSM adapter.

**RSM adapter routing**: Session register/unregister entries routed to `session.Manager`, normal entries forwarded to state machine. Register/unregister proposed through Raft (not local-only stub). Routing is by the `proto.Entry.SeriesID` sentinels `proto.SessionSeriesIDForRegister` (`math.MaxUint64 - 1`) and `proto.SessionSeriesIDForUnregister` (`math.MaxUint64`).

**Entry decompression**: Before each `SM.Apply()` call, decompresses entries in-place if compressed on propose path (Snappy). Bounded by `MaxEntrySize` to prevent decompression bomb attacks.

**Semaphore per-call (not per-goroutine)**: Acquired at apply time, released after. Idle shard goroutines do not hold slots, preventing deadlock when shards > MaxApplyWorkers.

**Fail-fast on apply error**: Any `SM.Apply()` or decompression error is terminal. The shared `failApply` helper fails the batch's pending proposals via `OnProposalFailed`, sets `node.failed = true` immediately (on the FIRST error — no retry counting), and reports an `ApplyError` via `handleError`. `lastApplied` is deliberately NOT advanced: skipping the batch while advancing the watermark would silently diverge this state machine from its peers, violating Raft's state-machine safety property. The step worker then skips the shard (`IsFailed()` check) until operator intervention (reload or snapshot recovery). This mirrors the SM-panic recovery path, which already took the shard offline on the first panic.

**Post-apply flow**:
1. Callback delivers results to Host layer for completing pending proposals
2. Updates `node.lastApplied` to highest index in batch
3. Auto-snapshot trigger: if `SnapshotEntries > 0` and `lastApplied - lastSnapshotIndex >= SnapshotEntries`, requests a snapshot (concurrent requests prevented by CAS). `config.SnapshotsDisabled` (`math.MaxUint64`) needs no special case: it is a threshold the `>=` comparison never reaches. Note that a zero `SnapshotEntries` reaching the engine means the config was never passed through `SetDefaults`, which substitutes `DefaultSnapshotEntries` (100000).
4. Clears `commitPending = false` (in defer) to return ownership to step worker
5. Wakes step worker via `workSignal.Notify()` for immediate re-processing without waiting for next tick

**UnloadNode cleanup**: When `engine.UnloadNode(shardID)` called, dispatcher receives via `unloadC` channel, closes per-shard goroutine channel, allowing exit and resource cleanup. Prevents goroutine leak on node unload.

**Skip-apply optimization (O29)**: An update whose first cycle carries no `CommittedEntries` bypasses the apply pipeline entirely rather than paying a dispatcher hop and a semaphore acquisition to apply nothing.


## Node Struct and Concurrency

Each shard has a `Node` containing Raft state, state machine instance, message inbox, and ownership atomics.

**Cache-line padding**: `commitPending` and `lastApplied` padded to 128 bytes (prefetch pair on x86/ARM) to prevent false sharing between step worker (reader) and apply worker (writer). Primary cross-worker communication path.

**commitPending ownership**:
- Normal: Step worker owns Peer, reads both `commitPending` and `lastApplied`
- commitPending=true: Commit/apply workers hold Peer, step worker skips shard
- commitPending=false: Ownership returned to step worker via `workSignal.Notify()`

Atomic flag blocks step worker from concurrent Peer access during commit/apply phase, enforcing single-owner semantics.

**Three inbound queues**, all `internal/queue.EntryQueue` (MPSC, double-buffered, mutex-guarded):

| Queue | Element | maxLen | Producers |
|-------|---------|--------|-----------|
| `proposalQ` | `proto.Entry` | 256 | `Host.Propose` callers, via `DeliverProposal` |
| `inboxQ` | `proto.Message` | 4096 | transport delivery goroutines, via `Deliver` |
| `readIndexQ` | `uint64` (read key) | 4096 | `Host.ReadIndex` callers, via `DeliverReadIndex` |

`inboxQ` replaced a 256-capacity buffered channel that silently dropped messages under burst load; the queue drains in batches via `Swap` and reuses its slices, so steady state is allocation-free. Backpressure is still by refusal at `maxLen` — dropping a Raft message is safe because the sender retransmits, and a refused proposal surfaces as `ErrDropped` rather than a silent loss. Proposals were split out of `inboxQ` (optimization O19) to remove the channel serialization bottleneck under concurrent proposal load; `inboxQ` now carries only non-proposal transport messages.

**Message origin**: `Deliver` stamps `proto.OriginRemote`, `DeliverLocal` stamps `OriginLocal`. `OriginRemote` is the zero value, so a message decoded from the wire or recycled through the pool is untrusted by construction. Local-origin messages carry no term and are exempt from the term gate in `raft.step`, so the exemption must not be reachable from the network — which is why the marshaller never reads or writes the field.

**Failed shard gate**: On any SM.Apply or decompression error (and on SM panic), the apply worker sets `failed = true` immediately — there is no retry counting and no retry budget to configure. The step worker checks `IsFailed()` in both `tick()` and `processReady()` and skips the node entirely, removing it from the pipeline until operator intervention (reload or snapshot recovery).

**Snapshot adoption gate**: A received `InstallSnapshot` moves the raft log's committed AND processed markers to the snapshot index the instant the step worker steps it, but the state machine adopts that index only when the snapshot pool finishes `RecoverFromSnapshot` and seeds `lastApplied`. Between the two, raft offers committed entries from `snapshotIndex+1` over a `lastApplied` that is still behind — a batch the apply worker refuses as `ErrApplyGap`, taking a replica that had just finished joining permanently out of apply. The `recovering` flag does not cover that window: the pool raises it from its own goroutine and cannot do so until it has observed the install. So both extraction sites — `processReady`'s `GetUpdate` and the commit worker's piggyback — additionally check `snapshotAdoptionPending()` (raft `processed` > `lastApplied`) on the Peer's owning goroutine. Withholding cannot wedge the log: extraction is the only thing that advances `processed`, so the entries are re-offered once the marker is seeded.

**stopC channel**: Closed by `Stop()` to signal cancellation for snapshot operations (used in SaveSnapshot/RecoverFromSnapshot context).


## Work Signaling

`WorkSignal` routes shard notifications to correct worker partition without contention.

**Notify fast path**: Atomic pre-filter `notified[workerID]` avoids ~10-15ns channel send when worker already notified (most calls hit this). Only sends to channel when transitioning from non-notified → notified state.

**ReadyMap (double-buffered)**: One `sync.Mutex` over one live map, plus a two-map rotation (`maps [2]`). `Set()` holds the lock for a single map insert; `Swap()` clears the alternate map outside the lock, then holds it only to exchange pointers, and returns the map that was live. Two-map rotation keeps the swap path at zero steady-state allocations. The returned map is owned by the caller only until the next `Swap()`, which recycles it.

**HasWork() utility**: Returns atomic load of WorkSignal state, ~1ns check for poll-based optimization.


## Listener Dispatcher

**Source**: `dispatch.go`

User-visible cold-path notifications run on one dedicated goroutine rather than on the worker that produced them. This is a correctness fix, not a throughput optimization.

Every callback the step worker fired ran synchronously on the step-worker goroutine *while that goroutine held `stepWorker.nodesMu.RLock`* — `tick()` and `processReady()` each hold it for their whole body. `Host.StopShard` re-enters the same goroutine's `nodesMu.Lock()` via `Engine.UnloadNode` → `stepWorker.unloadNode`, and Go's `RWMutex` is not reentrant. So an operator handler that reacted to an event by stopping the shard did not stall, it deadlocked — permanently, and not only for that shard. Every shard mapped to the same worker (`shardID % NumWorkers`) stopped ticking: no heartbeats, no elections, no inbox drain, and a later `Host.Close` never returned from `Engine.Stop`'s `stepWg.Wait()`. "React to a failed shard by stopping it" is the obvious handler to write, which is what made it reachable.

Moving the notification off the worker goroutine removes the class rather than the instance. The dispatcher goroutine holds no engine lock, so a listener may call any Host API, including the synchronous `StopShard`.

**Ordering**: notifications are consumed by exactly one goroutine from a FIFO channel, so delivery follows the order the engine produced them — a total order, and therefore a per-shard order. There is deliberately no ordering guarantee against the callbacks that stay synchronous; those were already unordered with respect to each other, since they run on three different worker tiers.

**Queue-full policy** (`listenerQueueSize` = 1024): `dispatch` never blocks the producing worker — a worker parked on a full queue still holds `nodesMu.RLock`, which would reintroduce the deadlock. It also never drops silently: `dispatch` reports acceptance and each caller picks a policy. `OnLeaderUpdated` leaves its bookkeeping uncommitted and re-publishes next tick; `OnCampaignLaunched` and `OnShardFailed` drop, counted and logged at ERROR. The queue only fills when a listener violates the documented non-blocking contract.


## Snapshot Pool

Asynchronous snapshot operations. Bounded pool (NumWorkers/2, minimum 2) with per-shard dedup via sync.Map.

**Non-blocking completedC send**: Uses select/default when forwarding snapshot completion results. Prevents full channel from blocking all snapshot workers. Result is logged at slog.Warn and dropped if channel full.

**Per-shard dedup**: `LoadOrStore` ensures only one snapshot per shard in-flight. Returns `SnapshotDuplicateError` if shard already has snapshot in-flight, `SnapshotPoolBusyError` if request channel full.

**Save flow**: Creates directory, opens data file, calls state machine `SaveSnapshot()`, fsyncs, enforces `MaxSnapshotSize` (post-compression), encrypts if barrier unsealed, writes the `snapshot.meta` record, persists to LogDB including full cluster membership, garbage collects old snapshots (keep 3), updates `lastSnapshotIndex`, compacts log, purges old barrier epochs.

### snapshot.meta

A snapshot directory MUST describe itself: it outlives its LogDB record (up to `maxSnapshotsRetained` directories are retained while only the newest is recorded), and the DEK purge floor is computed by scanning these directories and reading the `Epoch` field back as fact.

The record is **self-describing**: a 16-byte header followed by a body of little-endian uint64 fields. The body at version 1 is 9 fields (72 bytes), so a record this binary writes is **88 bytes**.

```
 off  size  field
   0     8  magic "QRSNAPMD"
   8     2  version (uint16, format version of the BODY)
  10     2  reserved (uint16, 0)
  12     4  body length (uint32, bytes of body that follow)
  16     8  Index
  24     8  Term
  32     8  ShardID
  40     8  ReplicaID
  48     8  CreatedAt (unix seconds)
  56     8  Epoch
  64     8  Compression
  72     8  DataLen
  80     8  DataHash (xxhash64)
```

`DataLen` and `DataHash` cover `snapshot.dat` **as written** — after compression and after encryption. They are the only at-rest integrity check a snapshot has when no barrier is configured: with a barrier, per-frame AEAD covers it, but a plaintext deployment had nothing, and a single flipped byte inside a serialized value leaves the file length unchanged, so no structural check trips. The replica then recovers the corrupt value into its state machine and serves it, diverging from its peers at the same log index with every Raft invariant intact. Hashing the final bytes rather than the pre-encryption ones is what lets all three consumers — recovery, the streamed receive path and `ImportSnapshot` — verify one value without holding a key. Records written before the field existed leave `DataHashRecorded` false and are recovered without the check rather than refused.

**Evolution rule**: body fields are append-only — an existing field never moves and never changes meaning. A new field is appended and the version is incremented. Because the body length is recorded, a reader can always tell a field that is ABSENT (written by an older version) from one that is CORRUPT (a truncated or foreign record). Changing the meaning of an existing field is not permitted under this rule; that requires a new magic.

**Compatibility**: three pre-magic ("v0") layouts are still accepted verbatim, positionally, at their original offsets — **40** bytes (`Index|Term|ShardID|ReplicaID|CreatedAt`), **48** bytes (`...|Epoch`) and **56** bytes (`...|Epoch|Compression`). The 40-byte layout was written by the snapshot receive and import paths and carries no `Epoch` even though the artifact beside it may be encrypted, so such a directory can hold ciphertext at an epoch nothing on disk names. It is therefore reported as UNEVALUABLE, never as epoch 0 — a zero `Epoch` with `EpochRecorded` false means UNKNOWN, not plaintext, and the two are opposite in consequence for DEK purging. Nothing rewrites an existing record: a v0 record stays v0 until its snapshot directory rolls off.

The declared body length is bounded at 64 KiB before use. It is a `uint32` read from a file and is used to slice the body, so on a 32-bit GOARCH an unbounded declared length could become a negative int and panic inside the purge-floor scan, whose entire input is bytes on disk.

**Recover flow**: Gets latest snapshot metadata from LogDB, locates the directory, verifies the artifact against `DataLen`/`DataHash` from `snapshot.meta` before anything reads it, then opens the data file, calls the state machine's `RecoverFromSnapshot()`, and updates `lastApplied` and `lastSnapshotIndex`. Sets the `recovering` atomic flag gating all SM operations during recovery (safety property S1).

When the LogDB record says `Epoch > 0`:

- If no barrier is available or the barrier is sealed, recovery fails closed with `ErrSnapshotEncryptedBarrierUnavailable`. The leader re-sends. Falling through would hand raw ciphertext to `RecoverFromSnapshot`.
- Otherwise the artifact is decrypted to a throwaway **sibling** file and recovery reads that, leaving the canonical `snapshot.dat` untouched. Decrypting in place is not crash-safe: a failure or crash after the rewrite would leave plaintext on disk while LogDB still records `Epoch > 0`, so every later recovery would re-attempt decryption on plaintext and fail, permanently losing an otherwise valid snapshot.

**Compression**: When `config.SnapshotCompressionType == config.Snappy`, wraps the file with `snappy.NewBufferedWriter` on save and `snappy.NewReader` on recover. `MaxSnapshotSize` check applied post-compression to measure actual disk usage. State machine unaware of compression.

**Encryption**: When `SnapshotBarrier` configured and unsealed, encrypts data file after fsync (ensures durable plaintext) and before metadata persist. Frame format: [ChunkLen:4 LE | EncryptedChunk]... per 64KB chunks, each including barrier's epoch+nonce+tag. Atomic temp-file + fsync + rename pattern. `DecryptForEpoch()` supports key rotation: old snapshots decrypted with epoch-specific DEK even after rotation.

**Epoch purge**: After garbage collection, scans remaining snapshot directories for oldest epoch in use, calls `barrier.PurgeEpochsBefore(minEpoch)` to zero/remove DEKs for epochs no longer referenced by retained snapshots or WAL segments. Limits key material exposure in memory.

**Leader snapshot streaming**: Step worker routes `InstallSnapshot` messages to `transport.SendSnapshot()`, chunked streaming transfer to follower. Receiver handles chunked reassembly on disk, saves to LogDB, triggers recovery. Prevents unbounded memory usage during snapshot transfer.


## Concurrency and Safety

**Single-owner Peer semantics**: Raft Peer is NOT thread-safe. `commitPending` atomic flag enforces exclusive ownership through pipeline topology:
- Normal: Step worker owns Peer (Tick, Handle, HasUpdate, GetUpdate)
- commitPending=true: Commit worker holds Peer via Peer.Commit(), then apply worker (SM.Apply only, Peer untouched)
- commitPending=false: Ownership returned to step worker

Step worker checks `commitPending.Load()` before touching Peer in `processReady()` and skips the node when set. No re-enqueue, to avoid spin loops; naturally re-checked on the next tick or WorkSignal notification. `tick()` is deliberately **not** gated this way — see the Step Worker ownership guard above.

**Happens-before proof (commitIndex safety)**: the `commitPending` flag blocks the step worker from producing a new Update until the apply worker finishes, so entries MUST be persisted AND applied before the step worker produces a new Update referencing them. It also prevents duplicate CommittedEntries: when `commitPending` is cleared, `lastApplied` has advanced past the committed entries, so the next `GetUpdate(lastApplied)` does not reproduce them.

The `Tick()`/`Commit()` race is prevented by the per-node `raftMu` mutex, not by the `commitPending` flag. Both `Peer.Tick()` and `Peer.Commit()` are called with `raftMu` held; `raftMu` also covers the step worker's `Peer.Step` and config-resolution paths and inbox message handling.

**Cross-worker atomics**: commitPending and lastApplied padded to 128B to prevent false sharing. Other fields (failed, snapshotting, lastSnapshotIndex) have specific reader/writer relationships enforced by pipeline topology.

**Channel topology**: `commitC` (step→commit, `CommitCBufferSize`, blocking with stopC escape), `applyC` (commit→apply, `max(CommitCBufferSize*4, 256)`, blocking WITHOUT escape), per-shard channels (apply dispatcher→goroutines, `defaultShardChanBuffer` = 64), `requestC`/`completedC` (snapshot workers), and the listener queue (1024, never blocking). Buffer sizes absorb normal bursts; sustained overload produces backpressure from apply through commit to step.

**Ordered shutdown**: Ensures no in-flight items lost between tiers:
1. `Node.Stop()` closes all stopC channels (unblocks snapshot ops)
2. `close(stepStopC)`, wait for step workers
3. `close(commitStopC)`, commit workers drain remaining commitC, wait
4. `close(applyStopC)`, apply workers drain remaining applyC, wait
5. `close(snapshotStopC)`, wait for snapshot workers
6. Stop the listener dispatcher, wait for it to drain


## End-to-End Data Flow

From client proposal to state machine apply:

```
Client Propose
   ↓
Host.Propose(shardID, cmd)
  • Validate, mint proposalKey, register in ns.pendingProposals
  • node.DeliverProposal(entry) → proposalQ (MPSC)
  • workSignal.Notify() → wakes step worker
   ↓
STEP WORKER (shardID % NumWorkers):
  • Drain proposalQ / inboxQ / readIndexQ → peer.Handle() under raftMu
  • peer.HasUpdate(lastApplied)?
  • peer.GetUpdate() → Update{EntriesToSave, CommittedEntries, Messages, ...}
  • OnLeaderUpdated       → listener dispatcher (if leader changed)
  • OnCampaignLaunched    → listener dispatcher (if vote messages)
  • OnReadyToRead         → inline (if reads confirmed)
  • sendMessages() [FREE-ORDER ONLY: send BEFORE persist]
    - Replicate/Heartbeat/PreVote → sender.Send()
    - InstallSnapshot → sender.SendSnapshot() (chunked streaming)
    - Ordered msgs held on commitItem.orderedMsgs
  • ReadOnly? → skip commit/apply entirely
  • commitPending.Store(true)
  • commitC ← commitItem (blocking, with stopC escape)
   ↓
COMMIT WORKER (same partition):
  • Batch drain (non-blocking collect accumulated items)
  • protoUpdatesToLogDBReuse() → Convert batch
  • logdb.SaveState(batch) [5 attempts; waits 10ms, 50ms, 200ms, 500ms]
  • Send ORDERED msgs — only now is the state they promise durable
  • OnCommitted (if NotifyCommit)
  • For each update:
    - peer.Commit() → Advance peer state, populate termRingBuffer
    - applyC ← applyItem (blocking, unconditional)
   ↓
APPLY DISPATCHER (same partition):
  • Route applyItem to per-shard goroutine
  • Listen for unloadC (shard removal)
   ↓
PER-SHARD GOROUTINE:
  • DecompressEntries() (if Snappy compressed)
  • sem ← (acquire, blocks if MaxApplyWorkers reached)
  • sm.Apply(entries, results) → State machine update
  • ← sem (release)
  • OnApplied callback (deliver results, complete proposals)
  • lastApplied.Store(lastIdx) → Advance watermark
  • Auto-snapshot check (SnapshotEntries > 0?)
  • commitPending.Store(false) [defer] → Return ownership
  • workSignal.Notify() [defer] → Wake step worker
   ↓
Step worker re-processes shard (inner loop)
```

**Error paths**:
- SaveState exhausts retries OR shutdown cancels mid-backoff: `failBatchSaveState` marks node failed, clears commitPending, reports SaveStateError to handleError, fails pending proposals (OnProposalFailed)
- Peer.Commit fails (log gap): commitPending cleared, CommitError to handleError, batch continues
- DecompressEntries fails: fail-fast — `failApply` marks node failed, reports ApplyError, fails pending proposals; lastApplied NOT advanced; step worker skips shard until operator intervention
- SM.Apply fails: fail-fast — same `failApply` path as decompression (shard offline on first error, no retry counting)
- SM.Apply panics: recovered, node failed, SMPanicError to handleError, step worker skips node


## Error Types

| Type                      | Trigger                                 | Effect                    |
|---------------------------|-----------------------------------------|---------------------------|
| `SaveStateError`          | SaveState retries exhausted / shutdown  | node.failed = true        |
| `CommitError`             | Peer.Commit() fails (log gap)           | commitPending cleared     |
| `ApplyError`              | SM.Apply or decompression fails         | node.failed = true        |
| `SMPanicError`            | SM.Apply panics (recovered)             | node.failed = true        |
| `SnapshotDuplicateError`  | Snapshot already in-flight for shard    | Request rejected          |
| `SnapshotPoolBusyError`   | Snapshot pool request channel full      | Request rejected (retry)  |
| `SnapshotError`           | Snapshot save/recover fails             | Reported via callback     |
| `SnapshotTooLargeError`   | Snapshot exceeds MaxSnapshotSize        | Snapshot dir cleaned up   |
| `MessageHandleError`      | Peer.Handle() returns error             | Logged at slog.Warn       |

Sentinels: `ErrShardFailed` (shard taken offline; `SMPanicError` matches it via `errors.Is`. `ApplyError` and `SaveStateError` also set `node.failed` but are distinct typed errors), `ErrStopped` (stopped), `ErrSnapshotCorrupt` (corrupt frame).

`SMPanicError` is the only type that matches `ErrShardFailed`, so an SM panic is the only apply failure a caller outside the module sees as the public `ErrShardFailed`. An `SM.Apply` error that is *returned* rather than panicked produces an `ApplyError`, which carries no `Is` method: it reaches the caller wrapping the state machine's own error, which is the more specific answer. Both fail the shard identically.

## Interfaces

**MessageSender**: `Send(msgs []proto.Message)` (batched protocol messages), `SendSnapshot(msg proto.Message)` (chunked streaming for InstallSnapshot). Non-blocking, concurrent-safe from multiple step workers. Undeliverable messages silently dropped (Raft retransmits).

**Callback**: all methods must be non-blocking and concurrent-safe. Two delivery classes:

*Synchronous, on the producing worker* — these resolve pending client requests and have happens-before requirements against `Peer.Handle`, so they cannot be queued behind user code:

| Method | Producer |
|--------|----------|
| `OnApplied(shardID, entries, results)` | per-shard apply goroutine |
| `OnCommitted(shardID, entries)` | commit worker, only when `NotifyCommit` is on |
| `OnReadyToRead(shardID, readyToRead)` | step worker |
| `OnReadIndexBatched(shardID, batchKey, aliasKeys)` | step worker, BEFORE `Peer.Handle` |
| `OnReadIndexFailed(shardID, keys, err)` | step worker |
| `OnProposalFailed(shardID, entries, err)` | step worker |
| `OnSnapshotCompleted(shardID, index, err)` | snapshot pool worker |
| `OnSnapshotRejected(shardID, replicaID)` | snapshot pool worker |
| `OnCampaignSkipped(shardID, replicaID, term)` | step worker — **NOT EMITTED** (see below) |

`OnCampaignSkipped` is declared on the interface and its doc comment names the step worker as the producer, but nothing in `pkg/internal/engine` calls it: every campaign-refusal branch in `startElection` and `handleTimeoutNow` logs and returns without notifying the engine, so `selfRemoved`, `roleMismatch`, `joining`, an unresolved stale config tail and an unreadable own-log term all produce no event. The Host-side plumbing down to `EventListener.OnCampaignSkipped` is wired and works if the hook is ever called, so implement against it if you like, but do not build monitoring that depends on receiving these events today.

*Via the listener dispatcher* — `OnLeaderUpdated(shardID, leaderID, term)` and `OnCampaignLaunched(shardID, replicaID, term)`. Their queue-full policies differ: a refused `OnLeaderUpdated` leaves the step worker's published-leader bookkeeping uncommitted, so the change is re-published on the next tick and never lost. `OnCampaignLaunched` is a cold path (elections are seconds apart), so a full queue drops it — counted and logged at ERROR — rather than reporting the election at the wrong time later. The election metric is incremented inline either way, so the counter stays exact.

**Applier**: `Apply(entries, results)` — State machine integration. Decouples engine from concrete rsm.StateMachine for testability.

**Snapshotter**: `SaveSnapshot(ctx, writer, stopper)`, `RecoverFromSnapshot(ctx, reader, stopper)`, `LastApplied()`. rsm.StateMachine satisfies both.

**SnapshotBarrier**: AEAD encryption (algorithm-agile: AES-256-GCM or ChaCha20-Poly1305) for snapshot data at rest. Concrete implementation: `seal.Barrier` (`crypto.Barrier` is a type alias for it). Nil means plaintext. `DecryptForEpoch()` supports key rotation, decrypting an old snapshot with its epoch-specific DEK after the epoch has advanced. `PurgeEpochsBefore()` zeros key material for epochs no longer in use.
