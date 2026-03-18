# Engine Worker Pipeline

Source: `pkg/internal/engine/`

## Overview

The engine drives Raft consensus through a three-stage pipeline: step worker (Raft logic), commit worker (persistence), apply worker (state machine). This design achieves pipelining (network I/O before disk I/O), batching, and single-owner semantics via the `commitPending` atomic flag.

```
Propose() → EntryQueue (MPSC) → Step Worker → Peer.Handle()
                                     ↓
                                Raft Update
                                     ↓
                          ┌──────────┴──────────┐
                          ↓                     ↓
                    Commit Worker          Apply Worker
                          ↓                     ↓
                  ┌───────┴────────┐      RSM Adapter
                  ↓                ↓           ↓
               WAL Persist    QUIC Send   State Machine
                  ↓                           ↓
              CommitNotify      RequestState.complete()
```

**Partitioning**: Shards hash to workers via `shardID % NumWorkers`, eliminating cross-shard contention.

**Pipelining**: Messages are sent before entries are persisted, overlapping network and disk I/O safely.

**Batching**: Commit worker drains pending items in batches while previous fsync is in-flight.

**Single-owner**: The `commitPending` flag ensures only one worker tier touches a shard's Peer at any time.


## Engine Lifecycle

**Initialization**: NumWorkers (workers per tier: step/commit/apply), MaxApplyWorkers (semaphore), CommitCBufferSize/applyBufSize (channel buffers), RTTMillisecond (tick interval). Tick interval derived from RTTMillisecond maps Raft election/heartbeat timeouts to real network latency.

**Startup**: Workers launched in reverse order (snapshot, apply, commit, step). Downstream consumers ready before upstream producers emit. Each tier tracked by sync.WaitGroup for sequential shutdown.

**Shutdown (ordered)**: Guarantees no in-flight items lost between tiers:
1. Node.Stop() for all (close stopC channels, unblock snapshot ops)
2. close(stepStopC) → wait (no new Updates)
3. close(commitStopC) → drainAndProcess → wait (persist remaining commitC items)
4. close(applyStopC) → drain remaining applyC → wg.Wait() (all per-shard goroutines finish)
5. close(snapshotStopC) → wait

Commit workers forward to apply unconditionally (no stopC escape) after persist: entries MUST be applied. Ordered shutdown guarantees apply workers still running.

**Node management**: `LoadNode` adds to `allNodes` and step worker at partition `shardID % NumWorkers`. `UnloadNode` reverses via unloadC. `DeliverMessage` (transport boundary) looks up node (read lock) and calls `node.Deliver(msg)`.


## Step Worker

**Source**: `step.go`

Drains proposals from lock-free MPSC `EntryQueue` and ReadIndex requests from dedicated `readIndexQ`. For each input, calls `Peer.Handle()` to process the Raft state change. Also drives Raft logical clocks via `Tick()` at intervals derived from `RTTMillisecond`. Produces `Update` (entries to persist, messages to send, committed entries) forwarded to commit worker.

**Main loop**: Woken by WorkSignal (message arrival, apply completion) or ticker. Drains inbox into reusable buffer, calls `peer.Handle()` for each message, checks `peer.HasUpdate()`, retrieves `Update`, and sends to commit worker via `commitC` (blocking with `stopC` escape).

**ReadIndex batching (OPT-8)**: ReadIndex requests accumulated in dedicated MPSC queue, drained as a batch with single key used as batchKey for coalescing. Multiple callers alias complete when batchKey resolves via `OnReadIndexBatched` callback.

**ReadOnly bypass**: When `Update.ReadOnly` flag set (no entries/state/snapshot), skips commit/apply pipeline entirely. Reduces ReadIndex latency from ~300us (3 goroutine hops) to ~5us (inline callback).

**Campaign detection**: Scans outbound `RequestVote`/`RequestPreVote` messages and calls `callback.OnCampaignLaunched(shardID, replicaID, term)` — cold path (elections seconds apart).

**Message pipelining**: Separates `InstallSnapshot` messages (routed to `sender.SendSnapshot()` for chunked streaming) from regular protocol messages (batched through `sender.Send()`). Sends before entries are persisted, overlapping network and disk I/O safely.

**Ownership guard**: Checks `node.commitPending.Load()` before calling `Tick()` or `Handle()`. When true, skips the node entirely to prevent concurrent Peer access with commit/apply workers.

**Lock-free proposal queue**: Proposal entry queue is MPSC without locks (zero-copy shard boundary). Reduces step worker contention vs shared proposals map.


## Commit Worker

**Source**: `commit.go`

Receives `Update` from step worker, persists entries to LogDB (sharded WAL with 16 shards), calls `Peer.Commit()` to advance internal state, forwards to apply worker. Uses drain-then-batch pattern: blocks on first item, non-blocking drains remaining items while previous persist completes, batches them together.

**Parallel WAL + QUIC send (O30)**: Messages sent to QUIC transport in parallel with WAL persist (overlapping I/O paths). Entries persisted to LogDB first, guaranteeing no message is sent before log entry exists.

**ReadOnly bypass**: When `Update.ReadOnly` flag set, skips persistence entirely and bounces back to step worker. Reduces latency since no entries need durability.

**Piggyback commit (O29)**: Detects newly committable entries detected during first cycle while awaiting leader confirmation, applies in same cycle without extra heartbeat round.

**CommitNotify**: When `config.Config.NotifyCommit` enabled, signals `committedC` channel after entries are persisted to WAL. Enables leader to notify client of committed (but not yet applied) state.

**DrainFreeOrderMessages**: On shutdown, preserves ordered messages (VoteResp, HeartbeatResp) that must stay in sequence. Only drains free-order messages to avoid correctness issues during rolling restart.

**SaveState retry**: Exponential backoff: 10ms, 50ms, 200ms, 500ms, 1s (total ~1.76s). On exhaustion, clears `commitPending` and forwards error to step worker via `handleError`, preventing hang.

**Peer.Commit()**: Advances internal Peer state (log truncation, stable storage markers). Populates `LogReader.termRingBuffer` (O(log n) binary search term lookups, zero GC pressure) for efficient term resolution on restart.

**Forward to apply (unconditional)**: After successful persist, sends `applyItem` to apply worker via blocking send WITHOUT `stopC` escape. Persisted entries MUST be applied (dropping them leaves committed-but-unapplied state). Ordered shutdown (apply workers stop after commit) guarantees consumer is running.


## Apply Worker

**Source**: `apply.go`

Per-shard goroutines gated by semaphore (MaxApplyWorkers limit). Fan-out dispatcher reads from `applyC` and routes each item to per-shard goroutine via `shardChans[shardID]`. Each shard applies committed entries sequentially to RSM adapter.

**RSM adapter routing**: Session register/unregister entries routed to `session.Manager`, normal entries forwarded to state machine. Register/unregister proposed through Raft (not local-only stub). Entries carry `SeriesIDForRegister = math.MaxUint64-1` and `SeriesIDForUnregister = math.MaxUint64` sentinels for session SM routing.

**Entry decompression**: Before each `SM.Apply()` call, decompresses entries in-place if compressed on propose path (Snappy). Bounded by `MaxEntrySize` to prevent decompression bomb attacks.

**Semaphore per-call (not per-goroutine)**: Acquired at apply time, released after. Idle shard goroutines do not hold slots, preventing deadlock when shards > MaxApplyWorkers.

**Circuit breaker**: Consecutive `SM.Apply()` failures increment `applyRetries`. At threshold (`maxApplyRetries`), sets `node.failed = true` and step worker skips node permanently. Sub-threshold failures call `handleError` and continue.

**Post-apply flow**:
1. Callback delivers results to Host layer for completing pending proposals
2. Updates `node.lastApplied` to highest index in batch
3. Auto-snapshot trigger: if `SnapshotEntries > 0` and `lastApplied - lastSnapshotIndex >= SnapshotEntries`, requests snapshot (preventing concurrent via CAS)
4. Clears `commitPending = false` (in defer) to return ownership to step worker
5. Wakes step worker via `workSignal.Notify()` for immediate re-processing without waiting for next tick

**UnloadNode cleanup**: When `engine.UnloadNode(shardID)` called, dispatcher receives via `unloadC` channel, closes per-shard goroutine channel, allowing exit and resource cleanup. Prevents goroutine leak on node unload.

**Skip-apply optimization (O29)**: When no `CommittedEntries` in cycle 1, bypasses apply pipeline entirely. Reduces first-cycle latency by 3N/5N nodes on startup.


## Node Struct and Concurrency

Each shard has a `Node` containing Raft state, state machine instance, message inbox, and ownership atomics.

**Cache-line padding**: `commitPending` and `lastApplied` padded to 128 bytes (prefetch pair on x86/ARM) to prevent false sharing between step worker (reader) and apply worker (writer). Primary cross-worker communication path.

**commitPending ownership**:
- Normal: Step worker owns Peer, reads both `commitPending` and `lastApplied`
- commitPending=true: Commit/apply workers hold Peer, step worker skips shard
- commitPending=false: Ownership returned to step worker via `workSignal.Notify()`

Atomic flag blocks step worker from concurrent Peer access during commit/apply phase, enforcing single-owner semantics.

**Inbox**: Buffered channel (capacity 256). Transport goroutines deliver messages via non-blocking `Deliver()` (drops on full). Step worker drains to reusable `[]proto.Message` buffer, feeds to `peer.Handle()` sequentially.

**Circuit breaker**: On SM.Apply failure, increments `applyRetries`. At threshold, sets `failed = true`. Step worker checks `IsFailed()` and skips node entirely, removing from pipeline until operator intervention.

**stopC channel**: Closed by `Stop()` to signal cancellation for snapshot operations (used in SaveSnapshot/RecoverFromSnapshot context).


## Work Signaling

`WorkSignal` routes shard notifications to correct worker partition without contention.

**Notify fast path**: Atomic pre-filter `notified[workerID]` avoids ~10-15ns channel send when worker already notified (most calls hit this). Only sends to channel when transitioning from non-notified → notified state.

**ReadyMap (64-bucket sharded)**: Double-buffered set with atomic pointer swap. Writers target active set, consumer targets swap set. Three-map rotation (O0 optimization) achieves zero steady-state allocations: map returned 2 swaps ago is guaranteed unreachable, recycled as drain target for current swap.

**spinMutex per bucket**: CAS loop with `runtime.Gosched()` on contention, optimized for sub-100ns critical sections (single map insert). Per-bucket locking scales to 64+ writers without sync.Mutex overhead.

**HasWork() utility**: Returns atomic load of WorkSignal state, ~1ns check for poll-based optimization.


## Snapshot Pool

Asynchronous snapshot operations. Bounded pool (NumWorkers/2, minimum 2) with per-shard dedup via sync.Map.

**Non-blocking completedC send**: Uses select/default when forwarding snapshot completion results. Prevents full channel from blocking all snapshot workers. Result is logged at slog.Warn and dropped if channel full.

**Per-shard dedup**: `LoadOrStore` ensures only one snapshot per shard in-flight. Returns `SnapshotDuplicateError` if shard already has snapshot in-flight, `SnapshotPoolBusyError` if request channel full.

**Save flow**: Creates directory, opens data file, calls state machine `SaveSnapshot()`, fsyncs, enforces `MaxSnapshotSize` (post-compression), encrypts if barrier unsealed, writes 60-byte binary metadata (Index|Term|ShardID|ReplicaID|CreatedAt|Epoch), persists to LogDB including full cluster membership, garbage collects old snapshots (keep 3), updates `lastSnapshotIndex`, compacts log, purges old barrier epochs.

**Recover flow**: Gets latest snapshot metadata from LogDB, locates directory, decrypts if epoch non-zero, opens data file, calls state machine `RecoverFromSnapshot()`, updates `lastApplied` and `lastSnapshotIndex`. Sets `recovering` atomic flag gating all SM operations during recovery (safety property S1).

**Compression**: When `config.SnapshotCompressionType == Snappy`, wraps writer/reader with snappy.BufferedWriter/snappy.Reader for transparent compression. `MaxSnapshotSize` check applied post-compression to measure actual disk usage. State machine unaware of compression.

**Encryption**: When `SnapshotBarrier` configured and unsealed, encrypts data file after fsync (ensures durable plaintext) and before metadata persist. Frame format: [ChunkLen:4 LE | EncryptedChunk]... per 64KB chunks, each including barrier's epoch+nonce+tag. Atomic temp-file + fsync + rename pattern. `DecryptForEpoch()` supports key rotation: old snapshots decrypted with epoch-specific DEK even after rotation.

**Epoch purge**: After garbage collection, scans remaining snapshot directories for oldest epoch in use, calls `barrier.PurgeEpochsBefore(minEpoch)` to zero/remove DEKs for epochs no longer referenced by retained snapshots or WAL segments. Limits key material exposure in memory.

**Leader snapshot streaming**: Step worker routes `InstallSnapshot` messages to `transport.SendSnapshot()`, chunked streaming transfer to follower. Receiver handles chunked reassembly on disk, saves to LogDB, triggers recovery. Prevents unbounded memory usage during snapshot transfer.


## Concurrency and Safety

**Single-owner Peer semantics**: Raft Peer is NOT thread-safe. `commitPending` atomic flag enforces exclusive ownership through pipeline topology:
- Normal: Step worker owns Peer (Tick, Handle, HasUpdate, GetUpdate)
- commitPending=true: Commit worker holds Peer via Peer.Commit(), then apply worker (SM.Apply only, Peer untouched)
- commitPending=false: Ownership returned to step worker

Step worker checks `commitPending.Load()` before touching Peer. When true, skips node in both `tick()` and `processReady()`. No re-enqueue to avoid spin loops; naturally re-checked on next tick or WorkSignal notification.

**Happens-before proof (commitIndex safety)**: `commitPending` flag blocks step worker from touching Peer until apply worker finishes, so entries MUST be persisted AND applied before step worker produces new Update referencing them. Also prevents duplicate CommittedEntries: when `commitPending` cleared, `lastApplied` has advanced past committed entries, so next `GetUpdate(lastApplied)` does not reproduce them. `tick()` guard essential: without it, `Peer.Tick()` would race with `Peer.Commit()` from commit worker.

**Cross-worker atomics**: commitPending and lastApplied padded to 128B to prevent false sharing. Other fields (failed, snapshotting, lastSnapshotIndex) have specific reader/writer relationships enforced by pipeline topology.

**Channel topology**: commitC (step→commit, blocking with stopC escape), applyC (commit→apply, blocking WITHOUT escape), per-shard channels (apply→goroutines, cap 16), requestC/completedC (snapshot workers). Buffer sizes absorb normal bursts; sustained overload produces backpressure from apply through commit to step.

**Ordered shutdown**: Ensures no in-flight items lost between tiers:
1. Node.Stop() closes all stopC channels (unblocks snapshot ops)
2. close(stepStopC), wait for step workers
3. close(commitStopC), commit workers drain remaining commitC, wait
4. close(applyStopC), apply workers drain remaining applyC, wait
5. close(snapshotStopC), wait for snapshot workers


## End-to-End Data Flow

From client proposal to state machine apply:

```
Client Propose
   ↓
Host.Propose(shardID, cmd)
   ↓
peer.Propose() → Entry appended to Raft log
   ↓
workSignal.Notify() → Wakes step worker
   ↓
STEP WORKER (shardID % NumWorkers):
  • Drain inbox → peer.Handle() for each message
  • peer.HasUpdate(lastApplied)?
  • peer.GetUpdate() → Update{EntriesToSave, CommittedEntries, Messages, ...}
  • OnLeaderUpdated callback (if leader changed)
  • OnReadyToRead callback (if reads confirmed)
  • OnCampaignLaunched callback (if vote messages)
  • sendMessages() [PIPELINED: send BEFORE persist]
    - Regular msgs → sender.Send()
    - InstallSnapshot → sender.SendSnapshot() (chunked streaming)
  • ReadOnly? → Skip if no persistence needed
  • commitPending.Store(true)
  • commitC ← applyItem (blocking)
   ↓
COMMIT WORKER (same partition):
  • Batch drain (non-blocking collect accumulated items)
  • protoUpdatesToLogDBReuse() → Convert batch
  • logdb.SaveState(batch) [retry: 10ms, 50ms, 200ms, 500ms, 1s]
  • Parallel: QUIC send while LogDB write in-flight (O30)
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
- SaveState exhausts retries: commitPending cleared, SaveStateError to handleError, step worker resumes
- Peer.Commit fails (log gap): commitPending cleared, CommitError to handleError, batch continues
- DecompressEntries fails: Same circuit breaker path as SM.Apply failures
- SM.Apply fails (retries < threshold): applyRetries++, ApplyError to handleError, commitPending cleared, step worker resumes
- SM.Apply fails (retries >= threshold): failed.Store(true), CircuitBreakerError to handleError, step worker skips node permanently


## Error Types

| Type                      | Trigger                                 | Effect                    |
|---------------------------|-----------------------------------------|---------------------------|
| `SaveStateError`          | LogDB SaveState exhausts retries        | commitPending cleared     |
| `CommitError`             | Peer.Commit() fails (log gap)           | commitPending cleared     |
| `ApplyError`              | SM.Apply fails (retries < threshold)    | commitPending cleared     |
| `CircuitBreakerError`     | SM.Apply fails (retries >= threshold)   | node.failed = true        |
| `SnapshotDuplicateError`  | Snapshot already in-flight for shard    | Request rejected          |
| `SnapshotPoolBusyError`   | Snapshot pool request channel full      | Request rejected (retry)  |
| `SnapshotError`           | Snapshot save/recover fails             | Reported via callback     |
| `SnapshotTooLargeError`   | Snapshot exceeds MaxSnapshotSize        | Snapshot dir cleaned up   |
| `MessageHandleError`      | Peer.Handle() returns error             | Logged at slog.Warn       |

Sentinels: `ErrShardFailed` (circuit breaker tripped), `ErrStopped` (stopped), `ErrSnapshotCorrupt` (corrupt frame).

## Interfaces

**MessageSender**: `Send(msgs []proto.Message)` (batched protocol messages), `SendSnapshot(msg proto.Message)` (chunked streaming for InstallSnapshot). Non-blocking, concurrent-safe from multiple step workers. Undeliverable messages silently dropped (Raft retransmits).

**Callback**: `OnApplied()` (per-shard apply goroutines), `OnLeaderUpdated()`, `OnReadyToRead()`, `OnCampaignLaunched()` (step workers), `OnSnapshotCompleted()`, `OnSnapshotRejected()` (snapshot pool workers). Must be concurrent-safe.

**Applier**: `Apply(entries, results)` — State machine integration. Decouples engine from concrete rsm.StateMachine for testability.

**Snapshotter**: `SaveSnapshot(ctx, writer, stopper)`, `RecoverFromSnapshot(ctx, reader, stopper)`, `LastApplied()`. rsm.StateMachine satisfies both.

**SnapshotBarrier**: AEAD encryption (algorithm-agile: AES-GCM or ChaCha20-Poly1305) for snapshot data at rest. Concrete impl: `crypto.Barrier`. Nil = plaintext. `DecryptForEpoch()` supports key rotation: old snapshots decrypted with epoch-specific DEK even after epoch advanced. `PurgeEpochsBefore()` zeros key material for epochs no longer in use.
