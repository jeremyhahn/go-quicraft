# Multi-Group Host — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg` — host lifecycle, startup,
> crash recovery, shutdown, and the performance contract · Last verified: 2026-09-15
> Conflicts with this document are defects. Fix them here first.

Import path: `github.com/jeremyhahn/go-quicraft/pkg`

The exported surface is enumerated in [`api.md`](api.md); design rationale and
the scope fence are in [`architecture.md`](architecture.md). This document is
the lifecycle contract — what happens at startup, during crash recovery, and at
shutdown — followed by the performance properties an implementation change must
not regress.


The lifecycle of a QuicRaft host: startup, shard initialization, crash recovery,
graceful shutdown, and the runbook for a shard that has lost quorum. Each
sequence below is traced from the code, not from design intent.

This is alpha software at `0.5.1-alpha`. The shutdown and recovery paths
described here are exercised by the integration and fault-injection suites, but
the format situation those suites do not cover is in
[Compatibility and Upgrades](../compatibility.md): a data directory written by
`0.4.x` is not readable here, and the failure is a corruption-shaped error
rather than a version mismatch.

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
  |  0. Refuse an unstated or conflicting at-rest encryption choice
  |     - none of WithBarrier / WithShardBarriers / WithoutEncryption:
  |       ErrAtRestEncryptionUnspecified
  |     - WithoutEncryption with a barrier option: ErrAtRestEncryptionConflict
  |     - checked before step 3, so a refused host creates no directories
  |
  |  1. cfg.SetDefaults() + cfg.Validate()
  |     - Reject empty WALDir, NodeHostDir, RaftAddress, ListenAddress
  |     - Reject DeploymentID 0 unless AllowZeroDeploymentID
  |     - NumWorkers and MaxApplyWorkers default to runtime.NumCPU()
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
  | 10. Build Host struct
  |     - All fields initialized, closed=false
  |     - msgHandler.host = h
  |
  | 11. Wire engine callback and import locker
  |     - eng.SetCallback(&hostEngineCallback{h})
  |     - Routes OnApplied, OnLeaderUpdated, OnReadyToRead, OnSnapshotCompleted
  |     - eng.SetImportLocker(h.tryAcquireImportLock)
  |
  | 12. Start engine
  |     - eng.Start() launches all worker goroutines (see below)
  |
  | 13. Start transport -- LAST
  |     - tr.Start() begins accepting QUIC connections
  |     - On failure: eng.Stop(), then tr.Stop(), return HostInitError
  |
  | 14. Start key rotation (if barrier configured)
  |     - Background goroutine: periodic barrier.Rotate()
  |
  | 15. Return *Host
```

**The transport starts LAST, after every other wiring step.** The moment it
starts, its receive goroutines can call `HandleMessage`/`HandleSnapshot`,
which read `msgHandler.host` (registry, config, LogDB) and
`msgHandler.engine`. Starting the transport before those fields are set is a
data race between the receive goroutine reading them and `NewHost` writing
them -- not merely a matter of inbound messages having somewhere to go.

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
  |  5. Set initial membership (first start, non-join only)
  |     - lr.SetMembership(initialMembership) from the members map,
  |       Config.Witness and Config.MemberIdentities
  |     - Written down as a committed EntryInitialMembership at
  |       log index 1 -- membership lives IN the log, not out of band
  |     - On a restart the membership is RECONSTRUCTED from the log
  |       (that entry plus every committed config change) instead
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
      |      |         |        0x04: bootstrap -> restore bootstrap info
      |      |         |        0x05: compact   -> replay compaction markers
      |      |         |        0x0b: entries   -> rebuild entry map + index
      |      |         |        0x0c: snapshot  -> restore snapshot metadata
      |      |         |        0x0d: state     -> restore hard state
      |      |         |        0x01/0x02/0x03/0x06/0x07/0x08/0x09/0x0a: legacy -> FATAL
      |      |         |    then purgeRetired: drop every replica the RETIRED registry names
      |      |         |        anything else: ErrUnknownPayloadType (FATAL)
      |      |
      |      |  4. Track nextSegmentID = max(segID) + 1
      |      |  5. ensureActiveSegment() if recovered data exists
      |
      |  If directory does not exist:
      |    Lazy initialization -- directory created on first write
```

The WAL shards rebuild all in-memory state (entries, hard state, bootstraps,
snapshots, compaction markers) purely from segment replay. No separate metadata
file is needed. The one thing a segment does not carry about itself — the block
grid and the chunk framing — lives in the `WALFORMAT` marker at the WAL root,
which is read before any segment byte.

**Every decode failure during replay is fatal, and that is deliberate.** A
malformed entry batch, a bad state record and an unreadable snapshot record all
abort recovery rather than being skipped. Skipping any of them is worse than
failing: a skipped entry batch leaves a silent hole in the log that is later
served to followers as a shortened, nil-error range (a Log Matching violation);
a skipped state record lets an older `Term`/`Vote` win last-writer-wins replay,
which permits a double vote in one term (an Election Safety violation); and a
skipped snapshot record boots the node with empty membership.

`ErrUnknownPayloadType` has one dominant real-world cause, and it is not a
version skew: **opening a keyed WAL with `barrier == nil`.** Barrier ciphertext
begins with an 8-byte big-endian epoch whose first byte is `0x00` for any
realistic epoch, so with no decrypt attempted every record decodes as payload
type `0x00`. If those were skipped the node would boot as a brand-new empty
replica that votes with no memory of prior votes. Check your barrier wiring
before you suspect disk corruption.

The other way to reach it is a downgrade: `0x0b` and `0x0c` are the current
entry and snapshot types, and a binary predating that renumbering knows neither.
A directory written by a newer build is not readable by an older one, which the
pre-release wipe-and-resync posture already assumes.

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

### In-Memory State Machine Recovery (StartShard)

On-disk SMs persist their own state, but in-memory SMs start empty and must replay their last snapshot on restart. During `StartShard`, after the SM is opened, the host queries `logdb.GetSnapshot(shardID, replicaID)`. If a snapshot record exists (`Index > 0` and a non-empty `Filepath`), the host opens `<Filepath>/snapshot.dat` and feeds it to `RecoverFromSnapshot`.

A recorded snapshot whose data file cannot be opened is **fatal**: `StartShard` returns a `HostInitError` (`Field: "snapshot_open"`) rather than silently skipping recovery. This is required for correctness — the Raft layer initializes the processed index to the snapshot index, and the pre-snapshot entries are compacted away, so a skipped recovery would bring the in-memory state machine up empty and it could never replay them. A failed close of the snapshot file (`snapshot_close`) or a failed `RecoverFromSnapshot` (`snapshot_recovery`) are likewise fatal `HostInitError`s.

### Log Loss: a Replica That Recovers Committed-But-Empty

A replica can come up with a durable `HardState.Commit` and **no entries** —
a wiped or partially restored data directory, a truncated WAL segment set, a
restore that recovered the state record but not the entry records. Left alone
this is a Leader Completeness hazard: every index at or below `committed` is
skipped by conflict detection as "cannot conflict", so the replica answers a
backtracking leader with a **non-rejecting** `ReplicateResp` for entries it
does not have, and the leader counts that phantom match toward quorum.

`newRaftLog` clamps `committed` down to what the log actually holds, so a
replica in this state comes up at `committed = 0` and is indistinguishable
from a fresh one. It re-learns its log and its commit index from the leader.
The clamp logs at `ERROR` with the recovered and actual indices.

**The host does not refuse to start.** A single shard's corruption must not
take down a process that may be carrying thousands of shards, and self-healing
from the leader is the correct outcome for a wiped replica. The failure is
loud in the log and visible through `ShardInfo`; the operator diagnoses and
repairs the one shard while the rest of the host keeps serving.

**Operational procedure — and the residual this does NOT close.** The clamp
fixes the phantom acknowledgement. It does **not** fix Raft amnesia: the
replica keeps its recovered `Term` and `Vote`, so it can still vote in *new*
terms while holding no entries, and can therefore help elect a leader that is
missing committed entries. No code change closes that — a voter that has
forgotten its log is indistinguishable, from the outside, from one that never
had it.

When you see the clamp's `ERROR` line, treat the replica as having lost its
identity, not merely its data:

1. Stop the replica.
2. `RequestRemoveNode` (or `SyncRequestDeleteNode`) to remove it from the
   configuration, and wait for that change to commit.
3. Wipe its data directory.
4. Re-add it under a **new replica ID** (`RequestAddNode`), so the cluster
   never counts the amnesiac identity's vote again.

Restarting it in place — even with an empty directory — re-enters the same
identity into the configuration and leaves the residual open.

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
2. Verify `snapshot.dat` against the `DataLen` and `DataHash` recorded in
   `snapshot.meta` (xxhash64 of the file **as stored**, so before decryption and
   before decompression)
3. Decrypt data file in-place if epoch > 0 and barrier is available
4. Open data file, apply Snappy decompression if configured
5. Call `snapshotter.RecoverFromSnapshot(ctx, reader, node.stopC)`
6. Update `node.lastApplied` and `node.lastSnapshotIndex`

Step 2 is skipped for a `snapshot.meta` record written before those fields
existed (`DataHashRecorded` is false). Such a record is not an error — it is a
pre-`0.5.0` artifact and recovers unverified — but it means a truncated file
from that era still recovers as if complete. Snapshots taken by this build are
verified.

---

## Runbook: Recovering a Shard That Has Lost Quorum

**When this applies.** A shard has lost more than a minority of its voters to
*unrecoverable* failure — the machines and their disks are gone, not merely
unreachable. A 5-voter shard that has lost 3 is the canonical case.

**Why nothing else works.** Removing a dead replica is a configuration change, a
configuration change is a log entry, and committing a log entry needs the quorum
that no longer exists. Restoring a snapshot does not help either: the snapshot
carries the same configuration. Without the procedure below the data is intact
on disk and permanently unreachable.

### Before you start — this loses data by design

Rewriting a shard's membership discards Raft's safety argument. The excluded
replicas may hold committed entries the snapshot you are about to import does
not, and once they are out of the configuration nothing will ever reconcile
that: **acknowledged writes can be silently lost.** This is a deliberate
Leader Completeness violation, correct only when the alternative is losing the
shard entirely.

Two preconditions are not enforceable by the library and are yours to establish:

1. **The excluded replicas are permanently gone.** A partitioned replica that
   comes back to a configuration it was written out of will be told it is not a
   member, while still holding entries the new configuration never saw.
2. **Every survivor gets the SAME membership map.** Different maps on different
   survivors create two configurations that each believe they are
   authoritative. That is split-brain by construction.

### Procedure

1. **Stop every surviving replica.** `ImportSnapshot` requires the shard to be
   stopped on the host performing the import.

2. **Identify the most up-to-date survivor.** Compare `ShardInfo.LastApplied`
   across survivors, or the highest snapshot index each has recorded. Data held
   only by a less advanced replica is lost, so choose deliberately.

3. **Export a snapshot from that replica.**

   ```go
   f, err := os.Create("/tmp/shard-7.artifact")
   err = host.ExportSnapshot(ctx, 7, f)
   ```

4. **Import it on every survivor**, naming only the survivors:

   ```go
   survivors := map[uint64]string{
       3: "10.0.0.3:9000",
       4: "10.0.0.4:9000",
   }
   err := host.ImportSnapshot(ctx, 7, artifactReader,
       quicraft.WithImportMembership(survivors))
   ```

   The map is the complete new **voting** membership. Observers and witnesses
   from the artifact are dropped — a recovery configuration names exactly the
   replicas you verified. Shard policy (session limits, witness policy) is
   preserved; it is not membership.

   The importing replica must appear in its own map. Every refusal is by name
   and matches `ErrImportMembershipInvalid`; none is silently resolved, because
   a configuration accepted by accident is not detectable afterwards.

5. **Restart the survivors** with `StartShard(survivors, false, create, cfg)`,
   passing the same map you imported. The shard elects among the survivors and
   resumes.

6. **Re-add replacements normally** with `RequestAddNode` (or
   `SyncRequestAddNode`) once the shard has a leader. From this point ordinary
   membership change applies again.

### Verifying the outcome

`ShardInfo.Replicas` reports each follower's `Match`, `Next`, replication
`State` (`probe` / `replicate` / `snapshot`), `Paused`, `Active` and `Inflights`,
so you can confirm the recovered configuration is replicating rather than merely
elected.

Two traps in reading it: the slice is **empty on a follower**, and it is empty
on the first call for a shard because publication happens on the next tick. An
empty `Replicas` therefore means "ask the leader, and ask again", not "no
followers".

`Host.StateMachineHash(shardID)` returns `(hash, appliedIndex, ok, err)` for
comparing state machines across replicas. Compare hashes only at the **same**
`appliedIndex`; a mismatch at different indexes means one replica is behind, not
that they diverged. `ok` is false when the state machine does not implement
hashing.

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
  |     h.Drain() -> sets draining flag, waits on drainC
  |       Rejects new Propose/ReadIndex with ErrDraining
  |       Waits up to ShutdownTimeout for pending operations to complete
  |       On timeout: logged AND joined into Close's returned error
  |
  |  3. Stop key rotation goroutine
  |     close(keyRotStopC) -> wait <-keyRotDone
  |
  |  4. Stop pending-proposal sweeper
  |     close(sweepStopC) -> wait <-sweepDone
  |
  |  5. Stop engine (ordered tier-by-tier shutdown, see below) -- FIRST
  |     engine.Stop()
  |       - Step workers are the only producers of snapshot sends
  |       - After this, no new snapshotWg.Add(1) can occur
  |
  |  6. Drain in-flight snapshot sends
  |     transportSender.Wait()
  |       - Joins sends the step workers launched before exiting
  |       - Transport is still up, so these complete normally
  |
  |  7. Stop transport -- LAST
  |     transport.Stop()
  |       - Stop accepting new QUIC connections
  |       - Close all active connections
  |
  |  8. Drain pending requests with ErrClosed
  |     For each shard in h.nodes:
  |       ns.stopped = true (claimed via CompareAndSwap, so a concurrent
  |                          StopShard cannot tear the same shard down twice)
  |       ns.health = ShardUnloaded
  |       close(ns.openStopper) to cancel an in-progress DiskStateMachine.Open()
  |       For each pending proposal: rs.complete(ErrClosed)
  |       For each pending read: pr.rs.complete(ErrClosed)
  |       For each pending snapshot: rs.complete(ErrClosed)
  |       Delete from h.nodes map
  |
  |  9. Close LogDB
  |     logdb.Close() -> flush, fsync, close file handles
  |
  | 10. Seal barrier (if configured)
  |     barrier.Seal() -> zero root key and DEKs from memory
```

**The engine stops BEFORE the transport, and the transport stops LAST.** The
engine's step workers are the only producers of snapshot sends, and each send
does `snapshotWg.Add(1)`. `engine.Stop()` closes `stepStopC` and joins the
step workers, after which no new `Add` can occur -- so the subsequent
`Wait()` races with no concurrent `Add`. Stopping the transport first would
reintroduce the illegal "`WaitGroup.Add` concurrent with `Wait`" pattern and
would also leave send goroutines touching a torn-down transport.

`Close` returns an `error`: a drain that timed out is reported rather than
swallowed, because returning nil after abandoning in-flight work tells the
embedding application its shutdown was clean when proposals it accepted were
dropped.

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
  |     node.Stop() -- stopC closed; awaited installs answered
  |     node.awaitWriteLeases() -- a snapshot receive's writes finish
  |     stepWorker.unloadNode(shardID) -- remove from worker partition
  |     commitWorker.requestUnload(shardID) -- fence, waited on (below)
  |     applyWorker.requestUnload(shardID) -- close per-shard channel, waited on
  |     engine.allNodes delete
  |
  |  5. Remove registry entries for all shard members
  |
  |  6. Fire OnNodeUnloaded event
```

### Commit Fence on StopShard

`UnloadNode` waits for the commit worker to take the unload. The worker takes
membership changes between batches, and also while its forward to the apply
tier is blocked, so a stop never waits out another shard's busy state machine;
it never takes one while a `SaveState` is running. When the wait returns, no
`SaveState` of the stopped incarnation is running, every item of it still ahead
in the current batch is skipped by the per-item unloaded checks before it is
committed or persisted, and an update of that incarnation still queued in
`commitC` is dropped when it is admitted to a batch, before `SaveState` and together with its ordered
messages — exactly as a crash before the fsync would lose it. A `StartShard`
that follows therefore reads a LogDB the old incarnation can no longer write:
without the fence, an old vote could land over the new incarnation's
HardState, or an adopted snapshot could be recorded against a directory a
retried receive had already reclaimed.

Every incarnation guard in the pipeline — commit admission, the
post-`SaveState` commit guard and the apply dispatcher — asks whether the node
was **unloaded**, which `UnloadNode` marks before it stops the node, and not
whether it stopped. `Engine.Stop` stops every node too, long before the commit
and apply tiers exit, but no incarnation can follow a shutdown, so its drain
keeps persisting, committing and applying; a stop-based guard could not tell
the two apart and discarded the drain's committed work.

Two shutdown paths still drop committed items, deliberately: a dispatcher
blocked on a full per-shard channel takes the stop branch when the apply tier
stops (so a stuck shard cannot wedge `Engine.Stop`), and in memory mode the
step tier's stop can race its own apply hand-off. Their proposals are failed.
With a WAL both are crash-equivalent — the entries are durable and are applied
on the next start.

The commit pipeline is not the only writer. A local snapshot save captures
the state machine into a `.save-*` staging directory of its own, then
publishes it: renames it onto `snapshot-<index>` — the directory a snapshot
receive publishes into at that index — records it, moves the log reader's
marker, removes the entries below it and reclaims superseded directories. Only
the publish runs under the node's write lease (below); the capture is user
code, and `UnloadNode` never waits on it (the state machine's own `Close`
still waits for an in-flight stream, which is why a `SaveSnapshot` must honor
its stopper). Because the lease is shared, saves and receives serialize on a
separate per-node snapshot-directory lock, taken after the lease: a save never
replaces a directory it did not make, and a failed receive keeps a directory
LogDB records. A save on a stopped node publishes
nothing, and `StartShard` sweeps a crash's orphaned stages. A
snapshot receive publishes its
directory and, once raft adopts the install, records it in LogDB, raises the
log floor and reclaims superseded directories — all on the incarnation's
behalf, from a transport goroutine. Each of those steps runs under the node's
write lease (`Node.AcquireWriteLease`), which is refused once the node has
stopped, and `UnloadNode` waits for any lease already held right after it
stops the node. A recovery that then fails because its node stopped is
abandoned by the snapshot pool rather than reported as a failed restore: the
callbacks that report one resolve by shard ID, and would land on the next
incarnation.

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

## Error Handling and Shard Failure

The apply worker is fail-fast: any terminal apply-path error takes the shard offline immediately rather than retrying.

- On any `SM.Apply()` error or entry-decompression error (`failApply`):
  - The batch's pending proposals are failed via `OnProposalFailed`
  - `node.failed.Store(true)` is set on the FIRST error — there is no retry counting
  - An `ApplyError` is reported via the node's error handler
  - `node.lastApplied` is deliberately NOT advanced: skipping the batch while advancing the watermark would silently diverge this state machine from its peers, breaking Raft's state-machine safety property
- An SM panic during `Apply()` is recovered, sets `node.failed`, and reports `SMPanicError` (same offline outcome).
- The step worker checks `IsFailed()` in both `tick()` and `processReady()` and skips the shard entirely. Recovery requires operator intervention: stop and restart (reload) the shard, or recover it from a snapshot.

There is no retry budget to configure. `HostConfig.MaxApplyRetries` and `config.DefaultMaxApplyRetries` existed through 0.5.1-alpha, were never consulted by the apply path, and have been removed.

LogDB write failures (`SaveState`) — both retry exhaustion and shutdown cancellation during a retry backoff — funnel through `failBatchSaveState`: for every node in the batch it marks `failed = true`, clears `commitPending`, reports `SaveStateError`, and fails the batch's pending proposals (`OnProposalFailed`) so callers do not hang until their context deadline. The node is left permanently failed (recover by reloading the shard) because its Raft state advanced past stable storage.

---

## Key Invariants

1. **Single-owner Peer**: The `Peer` struct is not thread-safe. Ownership transfers via `commitPending`: step worker owns when false, commit/apply pipeline owns when true. The step worker skips shards with `commitPending=true` during `tick()` and `processReady()`.

2. **Ordered shutdown**: Per-tier stop channels (`stepStopC`, `commitStopC`, `applyStopC`, `snapshotStopC`) with per-tier WaitGroups ensure each tier is fully drained before the next is stopped. A single shared `stopC` would allow apply workers to exit before commit workers finish forwarding.

3. **Blocking forward from commit to apply**: After entries are persisted to LogDB, the commit worker sends to `applyC` with a blocking send (no `stopC` escape). Dropping persisted-but-unapplied entries would cause client proposals to hang forever without a response.

4. **Node.stopC for cancellation**: Snapshot save and recover operations select on `node.stopC` for cancellation. `Engine.Stop()` closes all node `stopC` channels first, before stopping any worker tier, to unblock in-flight snapshot operations.

5. **Apply worker wg.Wait()**: The apply worker waits for all per-shard goroutines to complete their current `SM.Apply()` call before returning from `run()`. This prevents `Engine.Stop()` from returning while state machine writes are still in progress.

---

## Cross-References

- [Deployment](user.md) -- configuration reference, monitoring, tuning, disaster recovery
- [Compatibility and Upgrades](../compatibility.md) -- format versions, what breaks across a version boundary
- [Engine Pipeline](../raft-consensus/engineering.md) -- step/commit/apply worker internals
- [WAL Persistence](../log-storage/engineering.md) -- segment format, chunk framing, compaction
- [Raft protocol](../raft-consensus/engineering.md) -- election safety, snapshot control, membership
- [State machine interfaces](../state-machines/engineering.md) -- `Open`, `Update`, `SaveSnapshot`, `RecoverFromSnapshot` contracts
- [Testing infrastructure](../testing/infrastructure.md) -- fault injection, partition primitive, linearizability suite

---

## Performance contract

This document describes the performance techniques implemented in QuicRaft, covering zero-allocation hot paths, compression, rate limiting, memory budget enforcement, and transport-level optimizations.

**On the numbers in this document.** Allocation counts (`B/op`, `allocs/op`) are
deterministic for a given build and are quoted here without qualification.
Timings and CPU percentages are not: absolute `ns/op` is a property of the
machine, and the few quoted below predate any provenance discipline in this
repository, so the hardware that produced them was not recorded. They are
flagged where they appear. New measurements belong in
[benchmarking.md](../testing/benchmarking.md), which records machine, governor, load, Go
version and commit alongside every sample.

### Hot Path Overview

The critical path for a proposal flows through every layer of the system:

```
Propose(cmd) -> EntryQueue[T].Add -> Step Worker -> Peer.Propose -> Peer.GetUpdate
  -> CommitWorker -> LogDB.write -> fsync
  -> Transport.Send -> QUIC stream write
  -> ApplyWorker -> StateMachine.Update -> Response
```

Each layer is designed for zero or near-zero allocations in steady state.

### Zero-Allocation Techniques

#### Entry Serialization (Zero-Copy)

The `proto.Entry` type uses a custom fixed-layout binary format with a 68-byte header (8 uint64 fields + 1 uint32 CmdLen). Serialization avoids reflection, protobuf, and dynamic allocation.

`MarshalTo(buf []byte)` writes directly into the caller's buffer using `binary.LittleEndian` and `copy()`. `UnmarshalFrom(buf []byte)` reads fields in place and zero-copy aliases `Entry.Cmd` into the source buffer. No allocation occurs for the payload. The receiver must copy `Cmd` before reusing the buffer.

Source: `pkg/proto/entry.go`

#### Per-Worker Commit Slots

The step worker maintains a `commitSlots` map (`map[uint64]*commitItem`) with pre-allocated `commitItem` structs per shard. When sending an Update through the commit channel, the worker reuses the existing slot rather than allocating a new struct. The pointer is sent through `commitC`, avoiding value copies of the Update struct on the channel hot path.

Source: `pkg/internal/engine/step.go`

#### Double-Buffered Message Slices

The raft state machine uses a double-buffer swap for outbound messages. Two pre-allocated slices (`msgs` and `msgsBuf`) alternate roles: one accumulates messages during a tick cycle while the other is drained by `GetUpdate`. The swap avoids allocation. Both slices are pre-allocated at raft initialization with capacity equal to the number of peers.

Source: `pkg/internal/raft/raft.go`

#### Pre-Allocated Sort Buffer

The `tryCommit()` function requires a sorted slice of match indices to determine the commit quorum. A pre-allocated `matched` slice on the raft struct is reused each cycle, avoiding per-commit allocation.

Source: `pkg/internal/raft/raft.go`

#### Reusable Inbox Buffer

The step worker reuses a single `inboxBuf []proto.Message` slice across all shard inbox drains. `DrainInbox()` resets the slice to `[:0]` and appends, reusing the backing array.

Source: `pkg/internal/engine/step.go` (field and buffer), `pkg/internal/engine/node.go` (`DrainInbox`)

#### Transport Buffer Pools

The QUIC transport uses `sync.Pool` for frame marshal buffers. The `getBuf()` and `putBuf()` methods manage pooled `*[]byte` pointers. Buffers exceeding 256 KB (`maxPoolBufSize`) are not returned to the pool to prevent unbounded pool growth. `Send()` no longer groups by target into a pooled map — grouping moved into the per-target send queues (see [Send Path](#send-path)).

Source: `pkg/internal/transport/quic.go`

#### ReadIndex Zero-Allocation Tracking

Heartbeat acknowledgments for ReadIndex quorum confirmation use a `uint64` bitfield instead of a map. Each voting replica is assigned a bit position (0..63) via `replicaToBit`, built in `becomeLeader()` and rebuilt by `rebuildReplicaToBit` when membership changes mid-term. Ack processing is a single bitwise OR and increment -- zero allocation, O(1) per response.

Source: `pkg/internal/raft/raft.go`

#### SwapBuffer Queue (not currently on any hot path)

The `SwapBuffer[T]` generic queue uses a lock-protected write buffer that is swapped with the previously returned read buffer, so a fresh slice is allocated only on the first `Swap`. The caller owns the returned slice until the next `Swap` recycles it. The backing array grows as needed but is never shrunk during normal operation.

`git grep SwapBuffer -- '*.go'` returns hits only inside `pkg/internal/queue` and its own tests. Nothing in the engine, raft, transport, or host layer uses it. It is described here because it exists, not because it carries traffic; the proposal path uses `EntryQueue[T]` (see [Double-Buffered MPSC Proposal Queue](#double-buffered-mpsc-proposal-queue-o19)).

Source: `pkg/internal/queue/swapbuffer.go`

### Compression Strategy

#### Entry Compression (Snappy)

When `Config.EntryCompressionType` is set to `Snappy`, entry `Cmd` payloads are compressed at proposal time on the leader before appending to the log. Only `EntryNormal` entries are compressed; config changes remain uncompressed for universal readability.

Compression and decompression are handled by `CompressEntry()` and `DecompressEntry()` in the raft package. Incompressible data (compressed output >= original size) is left uncompressed. The `EntryCompressFlag` is OR'd into the `Type` field, preserving the base entry type.

**Decompression safety**: `DecompressEntry()` accepts a `maxSize` parameter to reject decompression bombs -- crafted payloads that claim a small compressed size but decompress to gigabytes.

Source: `pkg/internal/raft/compress.go`

#### Transport-Level Compression

The transport layer applies Snappy compression to serialized `MessageBatch` payloads independently of entry-level compression. This compresses metadata, headers, and any uncompressed entries in the batch. Compression is only used when it reduces the payload size. The `FlagCompressed` bit in the frame header signals the receiver to decompress before deserialization.

Source: `pkg/internal/transport/quic.go`

#### Where Compression Occurs

| Stage | What | Direction |
|---|---|---|
| Leader proposal (`handleLeaderPropose`) | Entry.Cmd (Snappy) | Compress |
| WAL write | Stores compressed bytes | Pass-through |
| Transport send (`sendBatch`) | MessageBatch payload (Snappy) | Compress |
| Transport receive | MessageBatch payload | Decompress |
| Apply worker | Entry.Cmd before state machine | Decompress |

When both compression and encryption are enabled, the order is compress-then-encrypt.

### Rate Limiting

#### Per-Shard Rate Limiter (InMemRateLimiter)

Each shard maintains an `InMemRateLimiter` that tracks the in-memory Raft log size and follower feedback. The limiter uses hysteresis to prevent oscillation:

- **Trigger**: any tracked size (local or follower-reported) exceeds 100% of `MaxInMemLogSize`
- **Release**: all tracked sizes drop below 70% of `MaxInMemLogSize`
- **Debounce**: state transitions require at least `ChangeTickThreshold` (10) ticks between changes

Every mutation runs on the raft state machine goroutine, so the accounting needs no lock. The read side is not lock-free: `IsLimited()` is called from other goroutines and reads a `cachedLimited` atomic that the owning goroutine publishes. Follower feedback is sent via `RateLimit` messages every election timeout. The leader records each follower's reported in-memory log size and considers it when evaluating rate limit thresholds. Stale follower entries are garbage collected after `gcTick` (3) ticks without an update.

Source: `pkg/internal/server/rate.go`

#### Global Rate Limiter (RateLimiter)

A host-level `RateLimiter` tracks the sum of all per-shard in-memory log sizes. When the sum exceeds `HostConfig.MaxTotalInMemLogSize`, all shards enter rate-limited mode. This prevents OOM from moderate load distributed across many shards (e.g., 1000 shards at 200 MB each = 200 GB total).

The global limiter uses lock-free atomics with CAS-loop decrement to prevent unsigned underflow.

Source: `pkg/internal/server/rate.go`

#### Backpressure Flow

Every layer in the pipeline has a bounded queue or channel. When any layer is slow, backpressure propagates upward automatically:

```
Client (Propose)
  |
  v
EntryQueue[T].Add() -----> full? --> return ErrBusy
  |
  v
RateLimiter.IsLimited() --> yes? --> return ErrBusy
  |
  v
Step Worker --> Peer.Propose()
  |
  v
CommitWorker channel --> full? --> step worker blocks
  |
  v
LogDB.SaveState() --> fsync latency limits throughput
  |
  v
ApplyWorker channel --> full? --> commit worker blocks
  |
  v
StateMachine.Update() --> SM processing time limits throughput
  |
  v
RateLimiter.Decrease() --> release pressure
```

The only explicit check is `RateLimiter.IsLimited()` at the Propose entry point. Everything else is implicit via bounded buffers and channels.

#### Configuration

| Parameter | Scope | Default | Description |
|---|---|---|---|
| `Config.MaxInMemLogSize` | Per-shard | 0 (disabled) | In-memory log size threshold for rate limiting |
| `HostConfig.MaxTotalInMemLogSize` | Global | 0 (disabled) | Cross-shard aggregate memory threshold |

Source: `pkg/config/config.go`, `pkg/config/hostconfig.go`

### ReadOnly Bypass

ReadIndex requests that produce no entries, state changes, or snapshots skip the commit/apply pipeline entirely. `Peer.GetUpdate()` sets `Update.ReadOnly = true` when all persistence-related fields are empty. The step worker checks this flag and continues to the next shard without sending the update to the commit channel.

This eliminates three goroutine hops (step -> commit -> apply -> callback) and the associated LogDB write for pure read operations. ReadyToRead states are delivered inline via the `OnReadyToRead` callback during `processReady()`, before the ReadOnly check.

**Performance impact**: ReadIndex latency drops by orders of magnitude, from multiple goroutine hops plus a LogDB write to a single inline callback.

Source: `pkg/internal/engine/step.go`, `pkg/internal/raft/peer.go`

### Replication Chunking

`sendAppend()` bounds the total entry bytes in a single Replicate message using `maxReplicationPayload`, set from `Config.MaxApplyEntrySize` (default `config.MaxReplicationPayloadBytes` = 16,711,672 bytes, ~15.9 MiB). This prevents unbounded message sizes when a follower is far behind. The follower acknowledges each batch via `ReplicateResp`, which triggers the leader to send the next chunk via `sendAppend()` in `handleAppendResp()`. This provides natural flow control without explicit windowing.

**Configuration**:

| Parameter | Default | Description |
|---|---|---|
| `Config.MaxApplyEntrySize` | 16,711,672 (~15.9 MiB) | Maximum Replicate message payload size |
| `Config.MaxEntrySize` | 8 MB | Maximum single entry payload size |

Source: `pkg/internal/raft/raft.go`

### ReadyMap

The `ReadyMap` is a double-buffered notification map that signals which shards
have pending work.

#### Structure

One `sync.Mutex` guards one live map, `ready`. `maps [2]` holds the two maps that
alternate as the live one; `index` says which of the pair is currently live.
`NewReadyMap` allocates three maps, not two: the initial `ready` is a third map
that the first `Swap` hands to the caller and never reinstalls. From the second
swap onward the rotation is strictly between the pair.

`Set(shardID)` takes the mutex, inserts, and releases it. The critical section is
a single map insert, which is why one lock suffices: contention is on the *lock*,
not the map, and the hold time is short enough that the mutex never becomes the
bottleneck.

`Swap()` clears the alternate map outside the lock (the caller owns it), then
takes the lock only long enough to exchange the two pointers, and returns the
map that was live. This is O(1) — the caller iterates the returned map with no
lock held. `Swap()` is NOT safe for concurrent use: it must be called from the
single owning step worker.

The map returned by `Swap()` is owned by the caller only until the NEXT `Swap()`,
which recycles it via `clear()`. Retaining it past that point reads a map another
goroutine is refilling.

Source: `pkg/internal/engine/ready.go`, `pkg/internal/engine/signal.go`

### Cache-Line Padding

The `commitPending` and `lastApplied` fields on `engine.Node` are padded to 128 bytes each to prevent false sharing between the step worker (reader) and apply worker (writer). On modern x86 and ARM processors, cache lines are 64 bytes; padding to 128 bytes provides safety for adjacent-line prefetch pairs. `commitPending` is set by the step worker after queuing an Update, and cleared by the apply worker after applying entries. `lastApplied` is written by the apply worker and read by the step worker to determine which committed entries to include in the next `GetUpdate()` call. Without padding, these fields would share a cache line, causing cross-core invalidation traffic on every update.

Source: `pkg/internal/engine/node.go`

### Connection Pooling and Stream Reuse

#### QUIC Connection Pool

The transport maintains a `map[string]*quic.Conn` of active connections, protected by an `RWMutex`. Connection establishment is coalesced via `singleflight.Group` to prevent redundant dials when multiple goroutines attempt to connect to the same target simultaneously. A background `connectionCleanup()` goroutine runs every 30 seconds, scanning for connections whose context is done and evicting them along with all associated pooled streams.

#### Stream Pool

QUIC streams are pooled and reused across messages. The pool is sharded across 16 buckets (`streamShardCount = 16`) to reduce lock contention.

Stream routing separates heartbeats from data messages:
- **Heartbeats**: stream index 0 (dedicated, low-latency path)
- **Data messages**: `(shardID % (poolSize - 1)) + 1`

This ensures heartbeats are never blocked by large Replicate messages on the same stream.

Stream creation uses double-checked locking: the fast path acquires a read lock to check for an existing stream, and the slow path opens a new stream under a write lock with a re-check to handle races.

Eviction is scoped to the failure. A peer that rejects a frame costs only that stream: `evictStream()` removes the single entry and leaves the connection and its other streams alone. A write failure or a dead connection goes wider, and `evictStreamsForTarget()` bulk-removes every stream for that target so subsequent calls open fresh streams on a new connection rather than reusing stale handles.

#### Send Path

`Send()` does not group or fan out. It walks the messages once, deep-copies each message's entries and snapshot references (breaking the alias with the step worker's reusable `proposeBuf`), resolves the target address from the registry, and enqueues the message on that target's `sendQueue` — a non-blocking hand-off to a per-target channel pair (data, cap 2048; heartbeat, cap 64).

Batching happens in the per-target `sendQueueWorker` goroutine, which drains its queues and coalesces up to 16 heartbeats or 256 data messages into one batch. Heartbeats are drained before data by a non-blocking priority select, so a replication backlog cannot delay heartbeats past the election timeout. Each batch is framed (`FrameHeaderSize` + payload) and optionally Snappy compressed. A batch that fits in one frame is one write to the pooled stream; `sendBatch` splits an oversized batch across as many frames as it needs, one write each. A worker exits after `sendQueueIdleTimeout` (60s) of inactivity, so removed peers do not leak goroutines.

See [QUIC Transport: Send Path](../quic-transport/engineering.md#send-path) for the full sequence.

Source: `pkg/internal/transport/quic.go`, `pkg/internal/transport/conn.go`

### Single-Owner Pipeline Invariant

The engine enforces a single-owner invariant on the `Peer` struct across worker tiers using the `commitPending` atomic flag. While `commitPending` is true:

- The step worker skips the shard in `tick()` and `processReady()`
- The commit worker owns the Peer for `Commit()`
- The apply worker owns the Peer data for `Apply()`

This eliminates the need for mutexes on the Raft state machine itself. The step worker does not re-enqueue skipped shards via `WorkSignal` to avoid a tight spin loop that would starve downstream workers.

Source: `pkg/internal/engine/step.go`

### Function Pointer Dispatch

Role-specific message handling and tick functions are assigned as function pointers during role transitions, providing zero-branch dispatch. The common `step()` method handles term-based transitions and vote requests, then delegates to `r.stepFunc()` for role-specific logic. This avoids a switch statement on every message.

Source: `pkg/internal/raft/raft.go`

### Hot Path Optimizations

#### P0: LogReader termRingBuffer

The `LogReader` maintains a term cache so that `Term(index)` lookups avoid hitting LogDB. The original implementation used `map[uint64]uint64`, which required iterating up to 100K entries in `trimTermMap()` on every `appliedLogTo` call. A profile taken at the time attributed 82% of CPU to it under concurrent load; the machine was not recorded, so read that as "it dominated the profile", not as a portable figure.

The replacement `termRingBuffer` is a fixed-capacity circular buffer of `(index, term)` pairs held in a single `[]termRingEntry` slice of `maxTermCacheEntries` (100,000) elements, allocated once at 1.6 MB. Entries are appended at the tail and the buffer wraps when full, evicting the oldest entries automatically. Operations are:
- **Append**: O(1) -- write at `(head + count) % cap`, increment count
- **Lookup**: O(log n) -- binary search over the logical window
- **trimTo**: O(log n) -- binary search to find the cut point, advance head

Source: `pkg/logdb/reader.go`

#### P1: inMemory Lazy Compaction

The `inMemory` struct in the raft log tracks entries pending persistence and application. A `wastedPrefix` counter tracks how many entries have been logically truncated without physically reallocating the slice.

Compaction requires *both* thresholds to be met: `wastedPrefix >= entrySliceSize` (512) and `wastedPrefix >= 2*len(entries)`. Requiring twice the live length means the copy is amortized against at least as much dead space as live data. A separate fast path releases the backing array outright when no live entries remain.

Source: `pkg/internal/raft/inmemory.go`

#### P2: ReadyMap Two-Map Rotation

The `ReadyMap.Swap()` method returns a `map[uint64]struct{}` of notified shards. Two maps (`maps [2]`) alternate as the live one: the map handed to the caller last swap is `clear()`ed and installed as the new drain target on the next swap. This achieves zero steady-state allocations for the Swap path, at the cost of a one-swap ownership window on the returned map.

Source: `pkg/internal/engine/ready.go`

#### P3: Host.nodes sync.Map

The `Host` struct's node registry was protected by `sync.RWMutex` over a plain `map[uint64]*nodeState`. Replacing with `sync.Map` provides lock-free reads on the hot path. A separate `activeNodes atomic.Int64` counter provides O(1) shard counting without iterating the map.

Source: `pkg/host.go`

### Engine Pipeline Optimizations

#### Memory-Mode Commit Bypass (O5)

When `LogDB` is nil (memory-only mode), the step worker inlines `Peer.Commit()` and forwards directly to `applyC`, skipping the commit worker goroutine entirely. This eliminates one channel send/receive pair per update.

Source: `pkg/internal/engine/step.go`

#### ReadIndex Heartbeat Batching (O6)

Multiple concurrent ReadIndex requests that arrive in the same step worker inbox drain share a single heartbeat broadcast round. The raft layer defers `broadcastHeartbeat()` during ReadIndex processing and flushes once after all inbox messages are handled. Each heartbeat carries a `ctx` field that correlates with the ReadIndex request. Responses are matched to the correct ReadIndex by context.

Source: `pkg/internal/raft/raft.go`

#### Parallel Per-Target Sends (O17)

Sends to different peers proceed concurrently. The mechanism is no longer a goroutine spawned per `Send()` call: each target has a long-lived `sendQueueWorker` goroutine draining its own queue, so `Send()` only enqueues and returns, and the send phase costs `max(latencies)` rather than `sum(latencies)` without any per-call goroutine creation. A worker is created on first use for a target and retired after `sendQueueIdleTimeout` of inactivity.

Source: `pkg/internal/transport/quic.go`

#### LogDB Group Commit (O18)

The commit worker uses a flat `[]logdb.Entry` buffer that is sub-sliced across all updates in a batch, reducing allocation from O(N) per-update slices to O(1) in steady state. Benchmarks show 0 B/op and 0 allocs/op in steady state for both small (4 updates, 8 entries) and large (64 updates, 1024 entries) batches.

Sources: `pkg/internal/engine/commit.go`, `pkg/internal/engine/convert.go`

#### Double-Buffered MPSC Proposal Queue (O19)

Proposals now bypass the Go buffered channel (`inboxC`) and use a dedicated `EntryQueue[T]` double-buffered MPSC queue. It is guarded by a `sync.Mutex`, not lock-free; the win is amortization, not lock elimination. The queue has bounded capacity (`maxLen=256`) with backpressure -- `Add()` returns false at capacity, signaling the caller to return `ErrSystemBusy`. The double-buffer swap means producers hold the lock only for a slice append, and the consumer acquires the lock once per drain cycle. This eliminates channel serialization under 8+ concurrent writers.

Sources: `pkg/internal/queue/entryqueue.go`, `pkg/internal/engine/node.go`, `pkg/internal/engine/step.go`

#### LeaseRead (O20) -- Raft PhD Section 6.4

When `Config.LeaseRead = true`, the leader tracks, per voting member, the send time of the newest heartbeat round it acked in `remoteLastAck map[uint64]time.Time`, and the lease holds while a quorum's rounds were sent less than `ElectionRTT - 2` ticks of real time (less 1% for clock drift) ago on the leader's monotonic clock. A paused leader therefore loses the lease rather than keeping it across the pause (see `docs/raft-consensus/engineering.md` §15). When the lease is valid, ReadIndex requests are served immediately without a heartbeat round-trip. When the lease has expired, the standard ReadIndex path (heartbeat quorum confirmation) is used as a fallback.

Sources: `pkg/internal/raft/raft.go`, `pkg/config/config.go`

#### Persistent Heartbeat Streams (O21)

`dialConnection()` spawns `preOpenHeartbeatStream()` asynchronously after establishing a QUIC connection. This pre-opens a QUIC stream at index 0 (the heartbeat stream slot) and stores it in the sharded stream pool. This removes the `OpenStreamSync` round-trip from the first heartbeat send, which sits on the critical path for ReadIndex and liveness detection. The cost it saves was measured once at 20-40us on unrecorded hardware; the shape of the saving (one stream-open round-trip) is the durable claim, not the figure.

Source: `pkg/internal/transport/conn.go`

### Benchmark Results

For pairwise comparisons against Dragonboat v4, etcd/raft and OpenRaft, and for
per-scenario breakdowns, see [test/performance/README.md](../../test/performance/README.md).
That file states which machine and which date each set of numbers came from, and
which comparisons between them are legitimate. Two datasets exist and they are
not interchangeable.

Changes to the hot paths described above must be benchmarked before merge. See [Benchmark Regression Testing](../testing/benchmarking.md) for `make bench-compare`, the stored baselines under `test/performance/baselines/`, and why a single `make bench-<pkg>` run cannot answer the question.

### Allocation Budget Summary

The 0 allocs/op target applies to the uncompressed, steady-state hot path with warm pools. With compression enabled, the realistic budget is 1-3 allocs/op.

---

## Configuration

`pkg/config` holds three types. `SetDefaults` fills zero-valued fields and
`Validate` performs cross-field validation; `NewHost` and `StartShard` run both.

**`HostConfig` (per host)** — required: `WALDir`, `NodeHostDir`,
`ListenAddress`, `DeploymentID`. The full field table with defaults is in
[`architecture.md`](architecture.md#configuration); the ones whose default is a
trap:

| Field | Default | Why it matters |
|---|---|---|
| `MaxTotalInMemLogSize` | **0 = disabled** | without it one client can grow the in-memory log until the process dies. Set it. |
| `MaxWALDiskSize` | **0 = unbounded** | without it the WAL fills the volume and `fdatasync` poisons the store |
| `NoSyncWAL` | false | **test and container-benchmark use only** |
| `KeyRotationInterval` | 0 = no scheduled rotation | 24h is a reasonable production value. Invocation-threshold rotation runs regardless whenever a barrier is configured. |
| `MaxRetainedEpochs` | 0 = retain all | see [`../seal-barrier/`](../seal-barrier/user.md) |

**`Config` (per shard)** — the fields whose zero value does not mean what it
looks like:

| Field | Trap |
|---|---|
| `SnapshotEntries` | **zero does not disable it.** Zero means "unset" and selects 100000; use `config.SnapshotsDisabled`. |
| `SessionExpiryEntries` | same shape; use `config.SessionExpiryDisabled` |
| `CheckQuorum`, `PreVote`, `OrderedConfigChange` | `*bool`, so nil means **enabled**. Use `config.BoolPtr(false)` to disable. |
| `MaxSessionsPerClient` | bounds **cached responses per session**, not session count. The name is wrong and kept for compatibility. |
| `LeaseRead` | requires `CheckQuorum` and `ElectionRTT >= 5`; `ErrLeaseReadRequiresCheckQuorum` otherwise |
| `Witness` | bootstrap seed only; the live policy is replicated |

**`TransportConfig`** — [`../quic-transport/api.md`](../quic-transport/api.md).
`MTLSConfig` is required whenever the transport is enabled
(`ErrMTLSConfigRequired`).

**Validation errors**: `*ValidationError`, `*LogValidationError`,
`*MaxEntrySizeExceededError`, `*MaxApplyEntrySizeExceededError`,
`*MaxProposalPayloadSizeExceededError`, `*SessionExpiryEntriesTooSmallError`,
`*KeyRotationIntervalTooSmallError`, `*LeaseReadRequiresCheckQuorumError`,
`*MTLSConfigRequiredError`.

**There is no apply-retry setting**, and adding one would be a defect.

## Test Strategy

`make test` runs the whole unit suite; `make ci` is the gate. Per-package
targets are listed in each feature folder's `cli.md`.

| Layer | Command | Covers |
|---|---|---|
| unit | `make test` | every package; ~6,500 test functions across 27 packages |
| race | `go test -race` | the lock-free claims in the performance contract above |
| fuzz | `make fuzz-all` | 37 targets across proto, transport, waldb, seal, session, raft |
| integration | `make integration-test` | real clusters in containers |
| linearizability | `make integration-test-linearizability` | that observed histories are actually linearizable |
| I/O faults | `make ci-errfs` | the `test/errfs` harness |
| network faults | `make ci-netfault` | partitions, delays, drops |
| FIPS | `make verify-fips`, `make verify-fips-exempt` | approved-only enforcement inside the boundary (QUIC transport included), and `fips140=on` over the one exempt tree, `pkg/internal/transport/revocation` |
| benchmarks | `make bench-compare` | regression against stored baselines |

**The host-level regression suites are the institutional memory of this
repository** and must not be deleted when refactoring: `host_hardening_regression_test.go`,
`host_medlow_regression_test.go`, `host_review12_test.go`,
`host_review16_test.go`, `host_config_fault_regression_test.go`,
`messagehandler_hardening_test.go`. Each encodes a bug that was found once.

**Coverage.** The Makefile enforces one floor, `COVER_MIN = 91`, applied to
every coverage target rather than tuned per package. The module total clears it;
several packages measured alone do not — `pkg/internal/registry` 78.4%,
`pkg/internal/transport` 88.8%, `pkg/internal/engine` 89.1%,
`pkg/logdb/waldb` 89.2%, `pkg/logdb` 90.0%, `pkg/batch` 90.5%, `pkg` 90.8%,
module total 91.2% (measured at 0.5.1-alpha with `go test -coverprofile ./...`,
which is what `make coverage` uses: no build tags, so the integration and
fault-injection suites are excluded).

**The CI gate checks only the module total**, so no per-package gate can fail.
`pkg/internal/registry` is 37 statements, so one uncovered line moves it several
points. The gaps that matter are the WAL, the QUIC transport and the engine
pipeline — the layers whose failure modes are hardest to recover from. That is
**tracked work, not a settled state.**

**Rules for tests in this repository**: no `time.Sleep` to wait for consensus —
use the request channels, the listener notifications, or the diagnostic
accessors. No assertion on a latency histogram bucket. No `WithNoSync(true)` in
a durability test.

## Observability

The `Host` is where the collector, the logger and the event listener are
installed. The interfaces, the closed `reason` vocabularies, the full metric
name list and the alerting guidance are in
[`../observability/engineering.md`](../observability/engineering.md).

Host-level introspection, which is not metrics:

```go
host.HealthCheck()                 // *HealthStatus
host.GetNodeHostInfo(opt)          // *NodeHostInfo
host.GetShardInfo(shardID)         // *ShardInfo, including ShardHealth
host.ShardCount()
host.ID(); host.NodeIdentity(); host.RaftAddress()
host.IsSealed(); host.IsDraining()
host.DiagRaftState / DiagPipelineState / DiagShardGates / DiagRemotes
```

The four `Diag*` accessors exist for incident response rather than monitoring;
their signatures are diagnostic and may change.
