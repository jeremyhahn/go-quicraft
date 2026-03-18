# Lifecycle Management

This document describes the complete lifecycle of a QuicRaft host: startup, shard initialization, crash recovery, and graceful shutdown. Every section reflects the actual production code paths rather than design intent.

## Source Files

| File | Responsibility |
|---|---|
| `pkg/host.go` | NewHost, StartShard, Close, recoverLogReader |
| `pkg/messagehandler.go` | HandleSnapshot (received snapshot processing) |
| `pkg/internal/engine/engine.go` | Engine Start, Stop, LoadNode, UnloadNode |
| `pkg/internal/engine/step.go` | stepWorker run loop, tick, processReady |
| `pkg/internal/engine/commit.go` | commitWorker run loop, drainAndProcess |
| `pkg/internal/engine/apply.go` | applyWorker dispatcher, per-shard goroutines |
| `pkg/internal/engine/snapshot_pool.go` | Snapshot save, recover, log compaction |
| `pkg/internal/engine/node.go` | Node struct, stopC lifecycle |
| `pkg/internal/rsm/statemachine.go` | StateMachine Open, Apply, RecoverFromSnapshot |
| `pkg/logdb/waldb/db.go` | WAL Open, shard recovery, segment replay |

---

## Host Startup Sequence

`NewHost(cfg, opts...)` performs all initialization synchronously and returns a fully operational `*Host`. The transport is listening and the engine workers are running before the function returns. No shards are loaded yet -- that happens per-call via `StartShard`.

```
NewHost(cfg, opts...)
  |
  |  1. cfg.SetDefaults() + cfg.Validate()
  |     - Reject invalid WALDir, NodeHostDir, RaftAddress, RTTMillisecond
  |     - Worker counts default to GOMAXPROCS when 0
  |
  |  2. Configure logger
  |     - WithLogger(l) installs caller-provided *slog.Logger
  |     - Otherwise build from cfg.LogConfig
  |
  |  3. Create directories
  |     - os.MkdirAll(NodeHostDir, 0750)
  |     - os.MkdirAll(WALDir, 0750)
  |
  |  4. Open LogDB
  |     - Default: waldb.Open(WALDir, opts...) -- 16 WAL shards
  |     - WithLogDB(l): use caller-provided LogDB
  |     - WithMemoryLogDB(): use memdb.New()
  |     - Barrier wired into WAL via waldb.WithBarrier when configured
  |
  |  5. Load or generate host ID
  |     - Read <NodeHostDir>/host.id (32-char hex)
  |     - Generate UUID and persist on first boot
  |
  |  6. Create registry
  |     - registry.NewRegistry() for address resolution
  |
  |  7. Create transport (unless WithoutTransport)
  |     - Build TLS config from HostConfig
  |     - transport.NewQUICTransport(cfg, handler, registry)
  |     - Create hostMessageHandler (engine ref set below)
  |
  |  8. Create engine
  |     - engine.NewEngine(cfg, logdb, sender, barrier, nil)
  |     - Allocates per-tier stop channels (step/commit/apply/snapshot)
  |     - Allocates per-worker commitC and applyC channels
  |     - Creates snapshot pool (numWorkers/2, min 2)
  |     - Workers NOT started yet
  |
  |  9. Wire circular references
  |     - hostMessageHandler.engine = eng
  |     - transportSender.engine = eng
  |     - barrier epoch tracker set on transport
  |
  | 10. Start transport
  |     - tr.Start() begins accepting QUIC connections
  |     - Must happen after engine exists (inbound messages need a destination)
  |
  | 11. Build Host struct
  |     - All fields initialized, closed=false
  |
  | 12. Wire engine callback
  |     - eng.SetCallback(&hostEngineCallback{h})
  |     - Routes OnApplied, OnLeaderUpdated, OnReadyToRead, OnSnapshotCompleted
  |
  | 13. Start engine
  |     - eng.Start() launches all worker goroutines (see below)
  |
  | 14. Start key rotation (if barrier + interval configured)
  |     - Background goroutine: periodic barrier.Rotate()
  |
  | 15. Return *Host
```

### Engine Start Worker Launch Order

`Engine.Start()` launches workers in bottom-up order so consumers are ready before producers begin sending:

```go
func (e *Engine) Start() {
    e.snapshots.start(&e.snapshotWg)  // 1. snapshot pool workers
    // 2. apply workers
    for _, aw := range e.applyWorkers { ... go aw.run() }
    // 3. commit workers
    for _, cw := range e.commitWorkers { ... go cw.run() }
    // 4. step workers (producers -- started last)
    for _, sw := range e.stepWorkers { ... go sw.run() }
}
```

Each tier is tracked by its own `sync.WaitGroup` (`snapshotWg`, `applyWg`, `commitWg`, `stepWg`) for ordered shutdown. Step workers are started last because they are the producers: once running, they immediately begin ticking Raft peers and generating Updates that flow into commitC.

---

## StartShard: Node Initialization

`Host.StartShard(members, join, create, cfg)` loads a single Raft shard into the running engine. Multiple shards can be started concurrently on the same host.

```
StartShard(members, join, create, cfg)
  |
  |  1. Validate config
  |     - ShardID > 0, ReplicaID > 0, cfg.Validate()
  |     - Detect SM type via probe instance (regular/concurrent/on-disk)
  |
  |  2. Check for duplicate shard (h.nodes map)
  |
  |  3. Create LogReader
  |     - logdb.NewLogReader(shardID, replicaID, logdb)
  |     - Provides unified view of log (markerIndex, state, entries)
  |
  |  4. Recover LogReader from LogDB (see Crash Recovery below)
  |     - h.recoverLogReader(lr, shardID, replicaID)
  |
  |  5. Set initial membership (unless joining existing cluster)
  |     - lr.SetMembership(proto.Membership{Addresses: members})
  |
  |  6. Create Raft peer
  |     - raft.NewPeer(cfg, lr, globalRL)
  |     - Initializes as follower with recovered state
  |
  |  7. Create RSM (managed state machine)
  |     - rsm.NewStateMachine(shardID, replicaID, create, snapshotDir, ...)
  |     - One-time type switch: DiskSM > ConcurrentSM > RegularSM
  |
  |  8. Open state machine
  |     - rsm.StateMachine.Open(stopper)
  |     - On-disk SMs: open database, return lastApplied
  |     - In-memory SMs: no-op, returns (0, nil)
  |
  |  9. Create nodeState (host-level tracking)
  |     - Pending maps: proposals, reads, snapshots
  |     - Atomic fields: leaderID, term, lastApplied, isLeader, health
  |
  | 10. Save bootstrap to LogDB (first boot, non-join only)
  |     - logdb.SaveBootstrap(shardID, replicaID, bootstrap)
  |
  | 11. Create engine.Node
  |     - engine.NewNode(peer, sm, lr, cfg, maxRetries, snapshotter, ...)
  |     - Allocates inboxC (buffered channel for transport messages)
  |     - Creates stopC channel
  |
  | 12. Load into engine
  |     - engine.LoadNode(node)
  |     - Assigned to stepWorker[shardID % numWorkers]
  |     - Registered in engine.allNodes map
  |
  | 13. Register members in registry
  |     - h.registry.Register(shardID, replicaID, addr) for each member
  |
  | 14. Notify engine
  |     - engine.NotifyWork(shardID)
  |     - Triggers first election/heartbeat cycle
```

---

## Crash Recovery

### LogReader Recovery

`recoverLogReader` restores the LogReader to its pre-crash state from LogDB. On first boot all steps are no-ops (ErrNoSnapshot, ErrNoState, empty range).

```
recoverLogReader(lr, shardID, replicaID)
  |
  |  Step 1: Restore snapshot metadata
  |     logdb.GetSnapshot(shardID, replicaID)
  |     --> lr.ApplySnapshot(protoSnapshot)
  |     Sets: markerIndex, markerTerm, membership
  |
  |  Step 2: Restore hard state
  |     logdb.ReadState(shardID, replicaID)
  |     --> lr.SetState(proto.State{Term, Vote, Commit})
  |     --> lr.SetMembership(membership) if returned by ReadState
  |
  |  Step 3: Restore entry range (O(1), no payload loading)
  |     logdb.EntryRange(shardID, replicaID)
  |     --> lr.SetRange(firstIndex, entryCount)
  |     LogReader now knows which indices are available for reads
```

After recovery, the LogReader provides a complete view of the persisted log. The Raft peer initializes with this state and can serve entries from LogDB for follower catch-up or replay.

### WAL Recovery (waldb)

When `waldb.Open(dir)` is called, each of the 16 WAL shards recovers independently:

```
waldb.Open(dir)
  |
  for each shard-0 through shard-15:
    |
    openWALShard(shardDir, opts, barrier)
      |
      |  If directory exists:
      |    shard.recover()
      |      |
      |      |  1. os.ReadDir(dir) -- list segment files
      |      |  2. Parse segment filenames, sort by ID
      |      |  3. For each segment in order:
      |      |       replaySegment(segID)
      |      |         |  Open read-only
      |      |         |  seg.ReadAll() -- block-based record reading
      |      |         |  For each record:
      |      |         |    Decrypt if barrier configured
      |      |         |    replayRecord(data, segID)
      |      |         |      dispatch on payload type byte:
      |      |         |        0x01: entries  -> rebuild entry map + index
      |      |         |        0x02: state    -> restore hard state
      |      |         |        0x03: snapshot -> restore snapshot metadata
      |      |         |        0x04: bootstrap -> restore bootstrap info
      |      |         |        0x05: compact  -> replay compaction markers
      |      |         |        0x06: remove   -> replay node removal
      |      |
      |      |  4. Track nextSegmentID = max(segID) + 1
      |      |  5. ensureActiveSegment() if recovered data exists
      |
      |  If directory does not exist:
      |    Lazy initialization -- directory created on first write
```

The WAL shards rebuild all in-memory state (entries, hard state, bootstraps, snapshots, compaction markers) purely from segment replay. No separate metadata file is needed.

### On-Disk State Machine Recovery

On-disk state machines manage their own persistence. The recovery sequence during `StartShard` is:

```
rsm.StateMachine.Open(stopper)
  |
  |  Delegates to onDiskSMWrapper.Open(ctx, snapshotDir, stopper)
  |    -> user SM.Open(stopper) -> returns lastApplied
  |
  |  If lastApplied > 0:
  |    s.lastApplied.Store(lastApplied)
  |
  |  Returns (lastApplied, nil)
```

If a snapshot exists at a higher index than the SM's `lastApplied`, the snapshot pool handles recovery via `RecoverFromSnapshot` after the node is loaded (triggered when the Raft peer detects the gap).

---

## Snapshot Recovery Protocol

When a leader sends a snapshot to a slow follower, the complete lifecycle is:

```
Leader                     Transport                  Follower
  |                           |                          |
  | InstallSnapshot msg       |                          |
  |-- (step worker) --------->|                          |
  |  sendSnapshot(msg)        |  Split into 2MB chunks   |
  |                           |-- QUIC stream ---------->|
  |                           |                          |
  |                           |       snapshotReceiver   |
  |                           |       reassembles chunks |
  |                           |            |             |
  |                           |   HandleSnapshot(chunks) |
  |                           |            |             |
  |                           |  1. Reassemble data      |
  |                           |  2. Write snapshot.dat   |
  |                           |     to disk + fsync      |
  |                           |  3. Write snapshot.meta  |
  |                           |  4. Save to LogDB:       |
  |                           |     SaveSnapshot(meta)   |
  |                           |  5. Request SM recovery: |
  |                           |     engine.RequestSnap-  |
  |                           |     shotRecovery(...)    |
  |                           |            |             |
  |                           |     Snapshot pool worker: |
  |                           |       recoverSnapshot()  |
  |                           |         GetSnapshot()    |
  |                           |         Open data file   |
  |                           |         Decrypt if needed|
  |                           |         SM.RecoverFrom-  |
  |                           |           Snapshot(r)    |
  |                           |         Update tracking: |
  |                           |           lastApplied    |
  |                           |           lastSnapshot   |
  |                           |            |             |
  |                           |  6. Deliver Snapshot-    |
  |                           |     Received to inbox    |
  |                           |            |             |
  |<-- SnapshotReceived ------|------------|             |
  |                           |                          |
  | (leader transitions       |                          |
  |  remote out of snapshot   |                          |
  |  state, resumes normal    |                          |
  |  replication)             |                          |
```

The `HandleSnapshot` method in `pkg/messagehandler.go` handles the follower-side processing:

1. Reassemble chunk data and compute total size
2. Create snapshot directory: `<NodeHostDir>/snapshots/shard-<N>/replica-<N>/snapshot-<index>`
3. Write `snapshot.dat` with fsync
4. Write binary metadata file (`snapshot.meta`)
5. Persist snapshot metadata to LogDB via `SaveSnapshot`
6. Trigger recovery: `engine.RequestSnapshotRecovery`
7. Deliver `SnapshotReceived` message back to the sender's inbox

The snapshot pool's `recoverSnapshot` then:

1. Read snapshot metadata from LogDB (index, term, filepath, epoch)
2. Decrypt data file in-place if epoch > 0 and barrier is available
3. Open data file, apply Snappy decompression if configured
4. Call `snapshotter.RecoverFromSnapshot(ctx, reader, node.stopC)`
5. Update `node.lastApplied` and `node.lastSnapshotIndex`

---

## Graceful Shutdown

### Host.Close()

`Close` is idempotent (guarded by `atomic.CompareAndSwap` on `closed`). The shutdown proceeds through `stopOnce.Do` to ensure exactly-once execution.

```
Host.Close()
  |
  |  1. Set closed flag (atomic CAS, prevents new operations)
  |
  |  2. Drain in-flight work
  |     h.Drain() -> sets draining flag, polls allPendingDrained()
  |       Rejects new Propose/ReadIndex with ErrDraining
  |       Waits up to ShutdownTimeout for pending maps to empty
  |       Returns ErrShutdownTimeout if deadline exceeded (non-fatal)
  |
  |  3. Stop key rotation goroutine
  |     close(keyRotStopC) -> wait <-keyRotDone
  |
  |  4. Stop transport
  |     transport.Stop()
  |       - Stop accepting new QUIC connections
  |       - Close all active connections
  |       - No new inbound messages after this point
  |
  |  5. Stop engine (ordered tier-by-tier shutdown, see below)
  |     engine.Stop()
  |
  |  6. Drain pending requests with ErrClosed
  |     For each shard in h.nodes:
  |       ns.stopped = true
  |       ns.health = ShardUnloaded
  |       For each pending proposal: rs.complete(ErrClosed)
  |       For each pending read: pr.rs.complete(ErrClosed)
  |       For each pending snapshot: rs.complete(ErrClosed)
  |       Delete from h.nodes map
  |
  |  7. Close LogDB
  |     logdb.Close() -> flush, fsync, close file handles
  |
  |  8. Seal barrier (if configured)
  |     barrier.Seal() -> zero root key and DEKs from memory
```

### Engine.Stop() -- Ordered Per-Tier Shutdown

The engine uses per-tier stop channels and WaitGroups to guarantee that committed entries flow through the entire pipeline before shutdown completes. Each tier is stopped and fully drained before the next tier is signaled.

```
Engine.Stop()
  |
  |  1. Stop all loaded nodes
  |     for _, node := range allNodes:
  |       node.Stop()  // close(node.stopC)
  |     Unblocks snapshot operations selecting on node.stopC
  |
  |  2. close(stepStopC) -> stepWg.Wait()
  |     Step workers exit their run() loops
  |     No new Updates produced, no new items enter commitC
  |
  |  3. close(commitStopC) -> commitWg.Wait()
  |     Commit workers call drainAndProcess() before exiting:
  |       Non-blocking drain of all remaining commitC items
  |       Each drained batch: SaveState to LogDB + Peer.Commit
  |       Forward persisted entries to applyC (blocking send)
  |     All committed entries are persisted and forwarded
  |
  |  4. close(applyStopC) -> applyWg.Wait()
  |     Apply workers call drain() to process remaining applyC items
  |     Close all per-shard channels -> per-shard goroutines exit
  |     wg.Wait() blocks until all SM.Apply() calls complete
  |     No state machine access after this returns
  |
  |  5. close(snapshotStopC) -> snapshotWg.Wait()
  |     Snapshot pool workers exit
```

**Why this order matters:**

```
Step --> Commit --> Apply --> Snapshot
 (1)      (2)       (3)        (4)

- Step before Commit:  Step produces Updates. Stopping step first ensures
                       no new work enters commitC.

- Commit before Apply: Commit persists to LogDB and forwards to applyC.
                       Commit drains remaining items and MUST forward them.
                       The blocking send to applyC is safe because apply
                       workers are still running.

- Apply before Snap:   Snapshot reads SM state. Apply must finish first
                       to prevent reading partially applied state.
```

### Commit Worker Drain-on-Shutdown

The commit worker's shutdown behavior is critical for data integrity:

```go
func (w *commitWorker) run() {
    for {
        select {
        case <-w.stopC:
            w.drainAndProcess()  // drain remaining items before exit
            return
        case item := <-w.commitC:
            // ... normal batch processing
        }
    }
}

func (w *commitWorker) drainAndProcess() {
    for {
        select {
        case item := <-w.commitC:
            // batch, process, forward to applyC
        default:
            return  // channel empty, done
        }
    }
}
```

After `drainAndProcess` completes, the commit worker calls `Peer.Commit(u)` for each update and forwards to applyC via a **blocking send**. This is safe because the apply worker's stop channel has not been closed yet -- it continues consuming from applyC throughout the commit drain phase.

### Apply Worker Drain-on-Shutdown

```go
func (w *applyWorker) run() {
    for {
        select {
        case <-w.stopC:
            w.drain()                    // dispatch remaining applyC items
            for _, ch := range shardChans {
                close(ch)                // signal per-shard goroutines to exit
            }
            w.wg.Wait()                  // wait for SM.Apply() to finish
            return
        // ...
        }
    }
}
```

The `wg.Wait()` at the end ensures `Engine.Stop()` does not return while any `SM.Apply()` call is still executing. Without this, the host would proceed to close LogDB and seal the barrier while state machines are still writing.

### Node.Stop()

Each `engine.Node` has a `stopC` channel that is closed during shutdown:

```go
func (n *Node) Stop() {
    select {
    case <-n.stopC:
        // Already stopped.
    default:
        close(n.stopC)
    }
}
```

This unblocks any operations selecting on `node.stopC`, specifically:
- `snapshotter.SaveSnapshot(ctx, w, node.stopC)` -- cancels in-progress save
- `snapshotter.RecoverFromSnapshot(ctx, r, node.stopC)` -- cancels recovery
- `rsm.StateMachine.Open(stopper)` -- cancels open (if still in progress)

The select-with-default pattern makes `Stop()` idempotent.

---

## StopShard: Individual Shard Removal

`Host.StopShard(shardID)` removes a single shard from the running host without affecting other shards:

```
StopShard(shardID)
  |
  |  1. Mark node as stopped
  |     ns.stopped = true, ns.health = ShardUnloaded
  |
  |  2. Fail all pending requests with ErrAborted
  |     pendingProposals, pendingReads, pendingSnapshots
  |
  |  3. Remove from h.nodes map
  |
  |  4. engine.UnloadNode(shardID)
  |     stepWorker.unloadNode(shardID) -- remove from worker partition
  |     applyWorker.requestUnload(shardID) -- close per-shard channel
  |     engine.allNodes delete
  |
  |  5. Remove registry entries for all shard members
  |
  |  6. Fire OnNodeUnloaded event
```

### Apply Goroutine Drain on StopShard

When `engine.UnloadNode` calls `applyWorker.requestUnload(shardID)`, the following sequence occurs inside the apply worker's dispatcher goroutine:

1. The `shardID` is sent to `unloadC` (buffered channel, capacity 64).
2. The dispatcher's `select` loop reads from `unloadC` and calls `removeShardChan(shardID)`.
3. `removeShardChan` closes the per-shard channel. The per-shard goroutine drains any remaining `applyItem`s from the channel, applies them to the state machine, then exits and decrements the `WaitGroup`.
4. Any in-flight `SM.Apply()` call completes before the goroutine exits.

This means committed entries queued for the shard are **applied before removal**. Proposals that were already committed and dispatched to the per-shard channel will complete normally. Proposals that have not yet been dispatched (still in the global `applyC`) will be routed to a newly-spawned goroutine if needed, or discarded if the shard has been removed from the dispatcher's map.

On full engine shutdown (`Engine.Stop`), the apply worker performs a complete drain: all entries in `applyC` are dispatched, all per-shard channels are closed, and `wg.Wait()` blocks until every per-shard goroutine has finished its final `SM.Apply()` call. This guarantees no use-after-close on state machines.

---

## Update Lifecycle Through the Pipeline

The complete data flow for a single proposal, from client submission through state machine application:

```
Client
  |
  | Host.Propose(ctx, shardID, cmd)
  |   Validates: closed, draining, shard exists, entry size,
  |              leader check, rate limiter
  |   Creates RequestState with unique key
  |   Delivers entry to engine node inbox
  |   Returns RequestState to caller
  |
  v
Step Worker  (tick or work notification)
  |
  | DrainInbox -> peer.Handle(msgs)
  | peer.HasUpdate(lastApplied) -> true
  | peer.GetUpdate(true, lastApplied) -> Update
  |   EntriesToSave: unsaved entries from inMemory
  |   CommittedEntries: entries between lastApplied and commitIndex
  |   Messages: outbound Raft messages
  |   State: term/vote/commit diff
  |   ReadyToRead: confirmed ReadIndex results
  |
  | Send messages via transport (pipelined with persist)
  |   InstallSnapshot separated to SendSnapshot() path
  |
  | Skip commit pipeline for ReadOnly updates
  |
  | node.commitPending.Store(true)
  | commitC <- &commitItem{update, node}  (blocking with stopC escape)
  |
  v
Commit Worker  (receives from commitC)
  |
  | Drain-then-batch: collect all immediately available items
  | Convert proto.Update -> logdb.Update (reuse buffers)
  | logdb.SaveState(batch) with exponential backoff retry
  |   5 attempts: 10ms, 50ms, 200ms, 500ms, 1s
  |
  | For each update in batch:
  |   peer.Commit(update)
  |     inMemory.savedLogTo(lastSavedIndex, lastSavedTerm)
  |     LogReader can now serve these entries
  |   Forward to applyC (blocking send -- entries MUST be applied)
  |
  v
Apply Worker  (dispatcher goroutine)
  |
  | Route item to per-shard channel (spawn goroutine if needed)
  |
  v
Per-Shard Goroutine  (sequential ordering per Raft requirements)
  |
  | Decompress entries (Snappy, bounded by MaxEntrySize)
  | sem <- struct{}{}  (acquire semaphore, limits concurrency)
  | node.sm.Apply(entries, results)
  | <-sem  (release)
  |
  | callback.OnApplied(shardID, entries, results)
  |   Host matches entry.Key -> pendingProposal -> rs.complete(result)
  |   Host completes confirmed pendingReads where lastApplied >= readIndex
  |
  | node.lastApplied.Store(lastIdx)
  | node.commitPending.Store(false)  (via defer)
  | workSignal.Notify(shardID)  (wake step worker)
  |
  | Auto-snapshot trigger if SnapshotEntries configured and threshold met
```

---

## Error Handling and Circuit Breaker

The apply worker implements a circuit breaker pattern for SM.Apply failures:

- Each failure increments `node.applyRetries`
- When `applyRetries >= maxApplyRetries` (default from `HostConfig.MaxApplyRetries`):
  - `node.failed.Store(true)`
  - Step worker skips the shard entirely (`IsFailed()` check)
  - `CircuitBreakerError` reported via event listener
- Successful apply resets `node.applyRetries = 0`

LogDB write failures (`SaveState`) also clear `commitPending` for all shards in the failed batch and report `SaveStateError` via the node's error handler.

---

## Key Invariants

1. **Single-owner Peer**: The `Peer` struct is not thread-safe. Ownership transfers via `commitPending`: step worker owns when false, commit/apply pipeline owns when true. The step worker skips shards with `commitPending=true` during `tick()` and `processReady()`.

2. **Ordered shutdown**: Per-tier stop channels (`stepStopC`, `commitStopC`, `applyStopC`, `snapshotStopC`) with per-tier WaitGroups ensure each tier is fully drained before the next is stopped. A single shared `stopC` would allow apply workers to exit before commit workers finish forwarding.

3. **Blocking forward from commit to apply**: After entries are persisted to LogDB, the commit worker sends to `applyC` with a blocking send (no `stopC` escape). Dropping persisted-but-unapplied entries would cause client proposals to hang forever without a response.

4. **Node.stopC for cancellation**: Snapshot save and recover operations select on `node.stopC` for cancellation. `Engine.Stop()` closes all node `stopC` channels first, before stopping any worker tier, to unblock in-flight snapshot operations.

5. **Apply worker wg.Wait()**: The apply worker waits for all per-shard goroutines to complete their current `SM.Apply()` call before returning from `run()`. This prevents `Engine.Stop()` from returning while state machine writes are still in progress.
