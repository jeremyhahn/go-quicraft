# Multi-Group Host — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg` — host lifecycle, startup,
> crash recovery, shutdown, and the performance contract · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

Import path: `github.com/jeremyhahn/go-quicraft/pkg`

The exported surface is in [api.md](api.md); design rationale and the scope
fence are in [architecture.md](architecture.md). This document is the lifecycle
contract (startup, shard initialization, crash recovery, shutdown, the
quorum-loss runbook) followed by the performance properties a change must not
regress. Each sequence is traced from the code.

Wire and on-disk formats are pinned until the first stable release and change in
place; the upgrade path across a format change is to wipe and re-sync. See
[Compatibility and Upgrades](../compatibility.md).

## Source files

| File | Responsibility |
|---|---|
| `pkg/host.go` | `NewHost`, `StartShard`, `Close`, `recoverLogReader` |
| `pkg/messagehandler.go` | inbound message and snapshot handling |
| `pkg/internal/engine/engine.go` | engine `Start`, `Stop`, `LoadNode`, `UnloadNode` |
| `pkg/internal/engine/step.go` | step worker loop, tick, `processReady` |
| `pkg/internal/engine/commit.go` | commit worker loop, `drainAndProcess` |
| `pkg/internal/engine/apply.go` | apply dispatcher, per-shard goroutines |
| `pkg/internal/engine/snapshot_pool.go` | snapshot save, recover, log compaction |
| `pkg/internal/engine/node.go` | `Node`, stop and write-lease lifecycle |
| `pkg/internal/rsm/statemachine.go` | the RSM adapter: `Open`, `Apply`, `RecoverFromSnapshot` |
| `pkg/logdb/waldb/db.go` | WAL open, shard recovery, segment replay |

## Host startup

`NewHost(cfg, opts...)` does all initialization synchronously. When it returns,
the engine workers are running and the transport is listening. No shards are
loaded until `StartShard`.

```
NewHost(cfg, opts...)
  |
  |  0. Refuse an unstated or conflicting at-rest encryption choice
  |     (ErrAtRestEncryptionUnspecified, ErrAtRestEncryptionConflict),
  |     before anything touches the disk
  |  1. cfg.SetDefaults(); cfg.Validate()
  |     - empty WALDir, NodeHostDir, RaftAddress, ListenAddress rejected
  |     - DeploymentID 0 rejected unless AllowZeroDeploymentID
  |     - MTLSConfig required unless the transport is disabled
  |  2. Logger: WithLogger, or built from cfg.LogConfig
  |  3. Create NodeHostDir and WALDir (0750) and fsync the created chain
  |  4. Refuse a plaintext log under a stated encryption choice
  |     (ErrShardBarriersPlaintextWAL, ErrBarrierPlaintextLogDB); a
  |     host_routed LogDB passes under WithoutEncryption, or when its
  |     host-wide route writes through the WithBarrier barrier
  |  5. Open LogDB
  |     - default: waldb.Open(WALDir, ...), with the host-wide barrier and
  |       the disk monitor wired in
  |     - WithLogDB(l) or WithMemoryLogDB()
  |  6. Load or generate the host ID: <NodeHostDir>/host.id, 32 hex
  |     characters (128 random bits), mode 0600
  |  7. registry.NewRegistry()
  |  8. Transport (unless disabled): message handler, QUICTransport,
  |     transportSender
  |  9. engine.NewEngine(...): per-tier stop channels, commit and apply
  |     channels, snapshot pool (max(NumWorkers/2, 2)). Not started
  | 10. Wire the message handler and sender to the engine and host
  | 11. eng.SetCallback(hostEngineCallback); import locker
  | 12. eng.Start()
  | 13. tr.Start() -- LAST. On failure: stop the engine, stop the
  |     transport, return *HostInitError
  | 14. Start key rotation when a barrier is configured
```

**The transport starts last.** Once it starts, receive goroutines call into the
message handler, which reads the host's registry, config, LogDB and engine.
Starting it earlier is a data race between those reads and `NewHost` still
writing the fields.

`Engine.Start()` launches workers bottom-up so consumers exist before producers
send: snapshot pool, apply workers, commit workers, then step workers. Each tier
has its own `sync.WaitGroup` (`snapshotWg`, `applyWg`, `commitWg`, `stepWg`) for
ordered shutdown.

## StartShard

`StartShard(members, join, create, cfg)` loads one shard into the running
engine. Different shards can start concurrently.

```
StartShard(members, join, create, cfg)
  |
  |  1. Validate: host open, ShardID and ReplicaID > 0, cfg.Validate(),
  |     members (bootstrap: non-empty, contains self, well-formed)
  |  2. Certificate exclusivity over the members' addresses (fails closed)
  |  3. Identities: with a transport, cfg.MemberIdentities must cover the
  |     members map (founder) or name another replica (joiner)
  |  4. Resolve the shard's barrier (WithShardBarriers): nil takes the
  |     declared host-wide route or fails (ErrShardBarrierUnresolved), a
  |     sealed one fails (ErrBarrierSealed), and the LogDB must hold the
  |     shard under it (ErrShardBarrierLogDBMismatch)
  |  5. Refuse: already running (ErrShardAlreadyExists), a concurrent start
  |     (ErrShardNotReady), an import in progress (ErrImportInProgress), a
  |     retired replica ID (*ReplicaRetiredError)
  |  6. recoverLogReader (see Crash recovery)
  |  7. Membership:
  |     - recovered from a snapshot: used as is
  |     - persisted entries but no snapshot: reconstructed from the
  |       committed log (the index-1 entry plus committed config changes)
  |     - first start: seeded from members, Config.Witness,
  |       Config.MemberIdentities and the session policy, and written as
  |       EntryInitialMembership at index 1
  |     First start only: a witness or observer listed in members is
  |     refused (*NonVotingRoleInMembersError). On a restart the anchor
  |     is checked against the recovered identities
  |  8. raft.NewPeer(cfg, lr, globalRateLimiter)
  |  9. Create the replica's snapshot directory; rsm.NewStateMachine(...)
  |     (type detection DiskSM > ConcurrentSM > StateMachine); Open
  | 10. In-memory tiers: recover the recorded snapshot, if any
  | 11. Save the bootstrap record (first start, non-join)
  | 12. engine.NewNode(...); engine.LoadNode(node), on step worker
  |     shardID % NumWorkers
  | 13. Register the effective membership's addresses in the registry
  | 14. engine.NotifyWork(shardID): the first tick begins
```

Address routing uses the effective (recovered) membership, not the caller's
map, which only supplies an address for a recovered member that has none.

## Crash recovery

### LogReader recovery

`recoverLogReader` restores the log reader from LogDB. On a first boot every
step finds nothing (`ErrNoSnapshot`, `ErrNoState`, an empty range).

1. `logdb.GetSnapshot`, then `lr.ApplySnapshot`: marker index and term,
   membership.
2. `logdb.ReadState`, then `lr.SetState(Term, Vote, Commit)`, plus the
   membership when the state record carries one.
3. `logdb.EntryRange`, then `lr.SetRange`, with no payload loading. A gap
   between the snapshot and the entries is repaired by removing entries up to
   the snapshot index.

### WAL recovery

`waldb.Open(dir)` checks the `WALFORMAT` marker, then each of the 16 WAL shards
replays its segments in order and rebuilds entries, hard state, bootstrap
records, snapshot records and compaction markers. Finally every replica the
`RETIRED` registry names is dropped. The record format and replay rules are in
[log-storage](../log-storage/engineering.md).

**Every decode failure during replay is fatal, by design.** Skipping is worse
than failing: a skipped entry batch leaves a hole later served to followers as a
shortened range (a Log Matching violation); a skipped state record lets an older
`Term`/`Vote` win last-writer-wins replay and permits a double vote (an Election
Safety violation); a skipped snapshot record boots the node with empty
membership.

`ErrUnknownPayloadType` has one common cause, and it is not version skew:
**opening a keyed WAL with no barrier.** Barrier ciphertext starts with an
8-byte big-endian epoch whose first byte is `0x00` for any realistic epoch, so
with no decryption every record reads as payload type `0x00`. Skipping them
would boot the node as an empty replica that votes with no memory of its prior
votes. Check the barrier wiring before suspecting the disk. The other cause is a
directory written by a build whose record layout differs (a retired payload type
is never reallocated, so it lands here too); wipe and re-sync.

### On-disk state machines

`rsm.StateMachine.Open` delegates to the user's `DiskStateMachine.Open`, which
returns its last applied index; the adapter records it. If a snapshot exists
above that index, the snapshot pool recovers it after the node is loaded, when
the Raft peer detects the gap.

### In-memory state machines

In-memory tiers start empty and must replay their last snapshot. After `Open`,
`StartShard` reads `logdb.GetSnapshot`; when a snapshot is recorded it verifies
the data against `snapshot.meta`, decrypts and decompresses as needed, and feeds
it to `RecoverFromSnapshot`.

A recorded snapshot that cannot be opened, verified, decrypted or recovered is
**fatal**: `StartShard` returns a `*HostInitError` (`Field` is
`snapshot_open`, `snapshot_integrity`, `snapshot_decrypt`,
`snapshot_compression`, `snapshot_close` or `snapshot_recovery`). The Raft layer
starts the processed index at the snapshot index and the earlier entries are
compacted, so a skipped recovery would bring the state machine up empty with no
way to replay them.

### A replica that recovers committed but empty

A replica can come up with a durable `HardState.Commit` and no entries: a wiped
or partially restored data directory, a truncated segment set, a restore that
kept the state record but not the entries. Left alone this breaks Leader
Completeness: every index at or below `committed` is skipped by conflict
detection as "cannot conflict", so the replica answers a backtracking leader
with a non-rejecting `ReplicateResp` for entries it lacks, and the leader counts
that phantom match toward quorum.

`newRaftLog` clamps `committed` down to what the log actually holds (0 for an
empty log), so the replica re-learns its log and commit index from the leader.
The clamp logs at ERROR with both indices.

**The host does not refuse to start.** One shard's corruption must not take down
a process carrying thousands of shards, and self-healing from the leader is the
right outcome for a wiped replica. The failure is loud in the log and visible in
`ShardInfo`.

**The clamp does not cure amnesia.** The replica keeps its recovered `Term` and
`Vote`, so it can still vote in new terms while holding no entries, and so help
elect a leader that lacks committed entries. No code change closes that: a voter
that has forgotten its log looks the same, from outside, as one that never had
it. When you see the clamp's ERROR line, treat the replica as having lost its
identity:

1. Stop the replica.
2. Remove it (`RequestRemoveNode` or `SyncRequestDeleteNode`) and wait for the
   change to commit.
3. `RemoveData` on its host, which retires the replica ID.
4. Re-add it under a **new replica ID** with
   `RequestAddNodeWithIdentity`. The removal tombstones the node's identity as
   well, so it also needs a **new keypair** (certificate and key); its old
   identity is refused with `ErrRemovedMemberIdentity`.

Restarting it in place, even from an empty directory, re-enters the same
identity and leaves the hazard open.

## Snapshot transfer

When a leader sends a snapshot to a lagging follower, the send path decrypts
the snapshot (verifying it against `snapshot.meta` as it reads) and streams it
as plaintext inside TLS in 2 MB chunks. On the follower, `HandleSnapshot`:

1. validates the chunk set (`*SnapshotChunkError`,
   `*SnapshotSizeMismatchError`) and authorizes the sender against the shard's
   membership, failing closed;
2. writes `snapshot.dat`, encrypted under the receiver's own barrier as it is
   written, and `snapshot.meta` into a staging directory, fsyncs them, and
   publishes the directory with one rename;
3. hands the install to Raft. Once Raft adopts it, the snapshot is recorded in
   LogDB, the log floor is raised and superseded directories are reclaimed, all
   under the node's write lease.

The snapshot pool then recovers the state machine: it verifies the data against
the `DataLen` and `DataHash` (xxhash64 of the file as stored) in
`snapshot.meta`, decrypts into an unnamed scratch file, decompresses, and calls
`RecoverFromSnapshot`, then updates `lastApplied` and the last snapshot index.
A `snapshot.meta` that is missing, unreadable or lacks the integrity fields
fails closed, as does a hash mismatch (`ErrSnapshotCorrupt`). The full contract
is in [snapshots](../snapshots/engineering.md).

## Runbook: recovering a shard that has lost quorum

**When this applies.** A shard has lost more than a minority of its voters to
unrecoverable failure: the machines and disks are gone, not merely unreachable.
A 5-voter shard that has lost 3 is the canonical case.

**Why nothing else works.** Removing a dead replica is a config change, a config
change is a log entry, and committing it needs the quorum that no longer exists.
A snapshot carries the same configuration. Without this procedure the data is
intact on disk and unreachable.

**This loses data by design.** Rewriting membership discards Raft's safety
argument. The excluded replicas may hold committed entries the imported snapshot
lacks, and nothing will reconcile that: acknowledged writes can be lost. Two
preconditions are yours to establish:

1. The excluded replicas are permanently gone. A partitioned replica that comes
   back to a configuration it was written out of is told it is not a member
   while still holding entries the new configuration never saw.
2. Every survivor gets the same membership map. Different maps create two
   configurations that each believe they are authoritative: split-brain.

Procedure:

1. **Stop every surviving replica.** `ImportSnapshot` needs the shard stopped.
2. **Find the most up-to-date survivor** by `ShardInfo.LastApplied` or its
   highest recorded snapshot index. Data held only by a less advanced replica is
   lost.
3. **Export a snapshot from it.**

   ```go
   f, err := os.Create("/var/backups/shard-7.artifact")
   if err != nil {
       return err
   }
   if err := host.ExportSnapshot(ctx, 7, f); err != nil {
       f.Close()
       return err // discard the partial file
   }
   if err := f.Close(); err != nil {
       return err
   }
   ```

4. **Import it on every survivor**, naming only the survivors:

   ```go
   survivors := map[uint64]string{
       3: "node3.example.com:63100",
       4: "node4.example.com:63100",
   }
   err := host.ImportSnapshot(ctx, 7, artifact,
       quicraft.WithImportMembership(survivors),
       quicraft.WithImportMemberIdentities(survivorIdentities))
   ```

   The map is the complete new voting membership. The artifact's observers and
   witnesses are dropped; shard policy (session limits, witness policy) is kept.
   The importing replica must be in the map. On a host with a transport every
   survivor needs an identity: the artifact's is kept unless
   `WithImportMemberIdentities` overrides it. Every refusal matches
   `ErrImportMembershipInvalid`; none is silently resolved, because a
   configuration accepted by accident cannot be detected afterwards.

5. **Restart the survivors** with `StartShard(survivors, false, create, cfg)`,
   with `cfg.MemberIdentities` naming the survivors' identities. The shard elects
   among them and resumes.
6. **Add replacements normally** with `RequestAddNodeWithIdentity` once the
   shard has a leader.

To verify, `ShardInfo.Replicas` on the leader reports each remote member's `Role`
(`voter`, `observer`, `witness`), `Match`, `Next`, `State` (`probe`, `replicate`, `snapshot`), `Paused`, `Active` and
`Inflights`. The slice is empty on a follower and on the first call for a shard,
so an empty `Replicas` means "ask the leader, and ask again". Compare
`Host.StateMachineHash` across replicas only at the same `appliedIndex`.

## Graceful shutdown

### Host.Close

`Close` runs once (`stopOnce`); a concurrent or repeated call waits for it and
returns the same error.

```
Host.Close()
  |  1. Fire OnNodeHostShuttingDown; set the closed flag
  |  2. Drain(): reject new work with ErrDraining, wait up to ShutdownTimeout.
  |     A timeout is logged and joined into Close's error
  |  3. Stop key rotation: close(keyRotStopC), <-keyRotDone
  |  4. Stop the pending-request sweeper: close(sweepStopC), <-sweepDone
  |  5. engine.Stop() (ordered, below)
  |  6. transportSender.Wait(): join snapshot sends the step workers started
  |  7. transport.Stop()
  |  8. For each shard: claim teardown (a concurrent StopShard owns a shard it
  |     claimed first), mark unloaded, cancel an in-progress
  |     DiskStateMachine.Open, fail pending proposals, reads and snapshots
  |     with ErrClosed; a second sweep catches requests registered during
  |     the first; close each state machine
  |  9. logdb.Close()
  | 10. barrier.Seal() when configured
```

**The engine stops before the transport, and the transport stops last.** The
step workers are the only producers of snapshot sends, and each send does
`snapshotWg.Add(1)`. `engine.Stop()` joins the step workers, after which no new
`Add` can happen, so the `Wait()` that follows races nothing. Stopping the
transport first would allow `WaitGroup.Add` concurrent with `Wait`, and send
goroutines touching a torn-down transport.

`Close` returns the `errors.Join` of every failure, including a drain that timed
out. Returning nil after abandoning accepted work would tell the application its
shutdown was clean when it was not.

`ShutdownTimeout` bounds the drain only. `Close` waits, with no timeout, for
every in-flight `SM.Apply` to return, so a state machine that blocks in `Apply`
blocks `Close`. The shards still applying are logged before that wait.

### Engine.Stop

Per-tier stop channels and WaitGroups make committed entries flow through the
whole pipeline before shutdown completes. Each tier is stopped and drained before
the next is signaled.

```
Engine.Stop()
  |  1. Stop every loaded node: close(node.stopC), unblocking snapshot I/O
  |  2. close(stepStopC); stepWg.Wait()      no new Updates
  |  3. close(commitStopC); commitWg.Wait()  drainAndProcess: persist and
  |                                         forward what is left (blocking
  |                                         send to applyC)
  |  4. close(applyStopC); applyWg.Wait()    drain applyC, close per-shard
  |                                         channels, wait for every SM.Apply
  |  5. close(snapshotStopC); snapshotWg.Wait()
  |  6. stop the listener dispatcher        last, so teardown events arrive
```

Why the order:

- step before commit: step produces Updates, so nothing new enters `commitC`
- commit before apply: commit must forward what it persisted, and the blocking
  send to `applyC` is safe because the apply workers are still running
- apply before snapshot: a snapshot reads state machine state and must not see a
  partially applied batch

The apply worker's `wg.Wait()` means `Engine.Stop` never returns while an
`SM.Apply` is running, so the host does not close LogDB or seal the barrier under
a state machine still writing.

`Node.Stop()` closes `stopC` once (select with default). That cancels
`SaveSnapshot`, `RecoverFromSnapshot` and an in-progress `Open`, all of which
select on the stopper.

## StopShard

`Host.StopShard(shardID)` removes one shard without affecting others:

```
StopShard(shardID)
  |  1. Claim teardown; mark the node stopped and ShardUnloaded
  |  2. Fail pending proposals, reads and snapshots with ErrAborted
  |  3. Remove from h.nodes
  |  4. engine.UnloadNode(shardID)
  |     - mark the node unloaded, then node.Stop(); awaited installs answered
  |     - node.awaitWriteLeases(): a snapshot receive's writes and a
  |       running snapshot recovery finish
  |     - step worker drops the node
  |     - commit fence, waited on (below)
  |     - apply unload, waited on (below)
  |  5. Remove the shard's registry entries
  |  6. Close the state machine; fire OnNodeUnloaded
```

### Commit fence

`UnloadNode` waits for the commit worker to take the unload. The worker takes it
between batches, and also while its forward to the apply tier is blocked, so a
stop never waits out another shard's busy state machine. It never takes one while
a `SaveState` runs. When the wait returns, no `SaveState` of the stopped
incarnation is running; items of it still in the current batch are skipped
before they are committed or persisted; and an update still queued in `commitC`
is dropped when admitted to a batch, before `SaveState` and with its ordered
messages, exactly as a crash before the fsync would lose it. A following
`StartShard` therefore reads a LogDB the old incarnation can no longer write.
Without the fence, an old vote could land over the new incarnation's hard
state, or an adopted snapshot could be recorded against a directory a retried
receive had already reclaimed.

Every incarnation guard in the pipeline (commit admission, the post-`SaveState`
commit guard and the apply dispatcher) asks whether the node was **unloaded**,
which `UnloadNode` sets before stopping the node, and not whether it stopped.
`Engine.Stop` stops every node too, long before the commit and apply tiers exit,
but no incarnation follows a shutdown, so its drain keeps persisting, committing
and applying.

Two shutdown paths drop committed items on purpose: a dispatcher blocked on a full
per-shard channel takes the stop branch when the apply tier stops (so one stuck
shard cannot wedge `Engine.Stop`), and in the engine's no-LogDB mode (which no
`Host` uses) the step tier's stop can race its own apply hand-off. Their
proposals are failed. With a WAL the first is crash-equivalent: the entries are
durable and are applied on the next start.

The commit pipeline is not the only writer. A local snapshot save captures the
state machine into a `.save-*` staging directory, then publishes it: renames it
onto `snapshot-<index>`, records it, moves the log reader's marker, removes the
entries below it and reclaims superseded directories. Only the publish runs
under the node's write lease (`Node.AcquireWriteLease`), which is refused once
the node has stopped; `UnloadNode` waits for any lease already held. The capture
is user code and `UnloadNode` never waits on it, which is why `SaveSnapshot` must
honor its stopper (the state machine's own `Close` still waits for an in-flight
stream). Saves and receives serialize on a per-node snapshot-directory lock,
taken after the lease, so a save never replaces a directory it did not make and a
failed receive keeps a directory LogDB records. A save on a stopped node publishes
nothing, and `StartShard` sweeps orphaned stages left by a crash. A runtime
snapshot recovery, unlike a save, holds the lease from before applies are gated
until `RecoverFromSnapshot` returns, so `UnloadNode` waits for the restore
(which is handed the node's stopper) instead of leaving
`rsm.StateMachine.Close` to; a recovery whose node has already stopped is
refused the lease and never touches the state machine. A recovery that
fails because its node stopped is abandoned by the snapshot pool rather than
reported, since the callbacks resolve by shard ID and would land on the next
incarnation.

### Apply drain

`requestUnload` sends the request to the apply dispatcher on its membership
channel. The dispatcher marks the shard unloaded and closes its per-shard
channel; the per-shard goroutine applies the items already queued and exits, and
a helper goroutine acknowledges the request once it has, so a slow `SM.Apply` on
the stopping shard does not block other shards. Items for the shard that reach
the dispatcher afterwards are discarded. If the engine is stopping, the request
joins the engine's drain instead.

On `Engine.Stop` the apply worker drains `applyC`, closes every per-shard
channel and waits for every final `SM.Apply`, so no state machine is used after
it is closed.

## Update lifecycle

```
Client
  | Host.Propose(ctx, shardID, cmd)
  |   checks: context, closed, draining, shard, witness, disk, size, leader,
  |   rate limiters, failure latch
  |   registers a RequestState under a random key; delivers to proposalQ
  v
Step worker (tick or work notification)
  | drain queues -> peer.Handle(...)
  | peer.GetUpdate() -> Update:
  |   EntriesToSave, CommittedEntries, Messages, State, ReadyToRead
  | send free-order messages (pipelined with persist); InstallSnapshot goes
  |   to the snapshot send path
  | ReadOnly update -> deliver ReadyToRead inline and skip the pipeline
  | commitPending.Store(true); commitC <- item (blocking, stopC escape)
  v
Commit worker
  | drain-then-batch all available items
  | logdb.SaveState(batch): 5 attempts, waits of 10, 50, 200 and 500 ms
  | send ordered messages
  | peer.Commit(update) for each; forward to applyC (blocking)
  v
Apply dispatcher -> per-shard goroutine (ordered per shard)
  | decompress entries (bounded by MaxEntrySize)
  | acquire the MaxApplyWorkers semaphore; sm.Apply(entries); release
  | callback.OnApplied: match entry.Key -> pending proposal -> complete;
  |   complete reads whose index is now applied
  | lastApplied.Store(last); commitPending.Store(false); notify step worker
  | automatic snapshot when SnapshotEntries is reached
```

## Error handling and shard failure

The apply path is fail-fast: a terminal apply error takes the shard offline
immediately.

- On an `SM.Update` error or an entry decompression error (`failApply`): the
  batch's pending proposals fail through `OnProposalFailed`, `node.failed` is
  set on the first error with no retry counting, and an `ApplyError` is
  reported. `lastApplied` is not advanced: skipping the batch while advancing the
  watermark would silently diverge this state machine from its peers.
- A panic in `Apply` is recovered, sets `node.failed` and reports an
  `SMPanicError`, with the same outcome.
- The step worker checks `IsFailed()` in `tick()` and `processReady()` and skips
  the shard. Recovery takes operator action: stop and restart the shard, or
  recover it from a snapshot.

There is no retry budget to configure.

A LogDB write failure in `SaveState` (retries exhausted, or shutdown during a
backoff) goes through `failBatchSaveState`: every node in the batch is marked
failed, `commitPending` is cleared, a `SaveStateError` is reported, and the
batch's pending proposals fail so callers do not wait for their deadline. The
node stays failed, because its Raft state advanced past stable storage.

## Key invariants

1. **Single-owner peer.** `Peer` is not thread-safe. Ownership moves through
   `commitPending`: the step worker owns it while false, the commit and apply
   pipeline while true, and the step worker skips a shard with
   `commitPending=true` in `tick()` and `processReady()`. It does not re-enqueue
   skipped shards, which would spin and starve the downstream workers.
2. **Ordered shutdown.** Per-tier stop channels and WaitGroups drain each tier
   before stopping the next. One shared `stopC` would let apply workers exit
   before commit workers finish forwarding.
3. **Blocking forward from commit to apply.** Persisted entries are sent to
   `applyC` with no `stopC` escape. Dropping persisted but unapplied entries
   would leave client proposals waiting forever.
4. **`Node.stopC` cancels snapshot work.** `Engine.Stop()` closes every node's
   `stopC` before stopping any tier.
5. **The apply worker joins every `SM.Apply`** before returning, so
   `Engine.Stop()` cannot return while a state machine is writing.

## Performance contract

Changes to the hot paths (append, replicate, apply) must be benchmarked before
merge with `make bench-compare` against the stored baselines; see
[benchmarking](../testing/benchmarking.md). A single `make bench-<pkg>` run cannot
answer the question. Comparisons with Dragonboat, etcd/raft and OpenRaft, with
the machine and date of every dataset, are in
[test/performance/README.md](../../test/performance/README.md).

Allocation counts (`B/op`, `allocs/op`) are deterministic for a build and are
stated here without qualification. Timings are properties of a machine and are
recorded only in the benchmark documents.

The target is 0 allocs/op on the uncompressed steady-state hot path with warm
pools; with compression on, 1 to 3 allocs/op.

```
Propose(cmd) -> EntryQueue.Add -> step worker -> Peer.Propose -> Peer.GetUpdate
  -> commit worker -> LogDB write -> fsync
  -> Transport.Send -> QUIC stream write
  -> apply worker -> StateMachine.Update -> response
```

### Allocation techniques

| Technique | Where | What it does |
|---|---|---|
| Zero-copy entry decode | `pkg/proto/entry.go` | 68-byte fixed header; `UnmarshalFrom` aliases `Entry.Cmd` into the source buffer. The receiver copies before reusing the buffer |
| Commit slots | `pkg/internal/engine/step.go` | `commitSlots` holds one reusable `commitItem` per shard; a pointer goes through `commitC` |
| Double-buffered messages | `pkg/internal/raft/raft.go` | `msgs` and `msgsBuf` swap roles each cycle |
| Reused sort buffer | `pkg/internal/raft/raft.go` | `tryCommit` sorts match indexes in the preallocated `matched` slice |
| Reused inbox buffer | `pkg/internal/engine/step.go` | one `inboxBuf` across all shard drains |
| Transport buffer pool | `pkg/internal/transport/quic.go` | `getBuf` / `putBuf`; buffers over 256 KB (`maxPoolBufSize`) are not pooled |
| ReadIndex ack bitfield | `pkg/internal/raft/raft.go` | acks are a `uint64` bitfield indexed through `replicaToBit` (built in `becomeLeader`, rebuilt by `rebuildReplicaToBit` on a mid-term membership change): one OR per response |
| LogDB group commit | `pkg/internal/engine/commit.go`, `convert.go` | one flat `[]logdb.Entry` sub-sliced across a batch: 0 B/op and 0 allocs/op in steady state |
| ReadyMap rotation | `pkg/internal/engine/ready.go` | two maps alternate as the live one; see below |

`SwapBuffer[T]` (`pkg/internal/queue/swapbuffer.go`) is on no hot path today;
the proposal path uses `EntryQueue[T]`.

### Compression

With `Config.EntryCompressionType = Snappy`, the leader compresses
`EntryNormal` payloads at proposal time (`handleLeaderPropose`); config changes
stay uncompressed. Incompressible data is left as is, and `EntryCompressFlag` is
OR'd into `Type`. `DecompressEntry` takes a `maxSize` to reject decompression
bombs. Separately, the transport Snappy-compresses each `MessageBatch` payload
when that makes it smaller, signaled by `FlagCompressed`.

| Stage | What | Direction |
|---|---|---|
| leader proposal | `Entry.Cmd` | compress |
| WAL write | stored compressed | pass-through |
| transport send (`sendBatch`) | `MessageBatch` payload | compress |
| transport receive | `MessageBatch` payload | decompress |
| apply worker | `Entry.Cmd` before the state machine | decompress |

With encryption on, the order is compress, then encrypt.

### Rate limiting and backpressure

**Per shard (`InMemRateLimiter`, `Config.MaxInMemLogSize`).** Tracks the
in-memory log size and follower reports, with hysteresis: limited when any
tracked size exceeds 100% of the limit, released when all are below 70%, with at
least `ChangeTickThreshold` (10) ticks between changes. Follower entries
not refreshed for `gcTick` (3) ticks are dropped. Mutations run on the Raft
goroutine; `IsLimited()` reads a published atomic.

**Global (`RateLimiter`, `HostConfig.MaxTotalInMemLogSize`).** Tracks the sum
across shards with lock-free atomics and a CAS loop that cannot underflow. It
guards against many shards each staying under their own limit while together
exhausting memory.

Both default to 0 (disabled). Every stage is bounded, so a slow stage pushes back
on the one before it:

```
Propose
  -> global limiter            ErrSystemBusy
  -> per-shard limiter         *BusyError
  -> proposalQ (256 entries)   full: ErrDropped
  -> step worker
  -> commitC (CommitCBufferSize)  full: step worker blocks
  -> LogDB.SaveState           fsync latency bounds throughput
  -> applyC                    full: commit worker blocks
  -> StateMachine.Update
```

### Read path

**ReadOnly bypass.** An update with no entries, state change or snapshot is
marked `ReadOnly` by `Peer.GetUpdate()`. The step worker delivers its
`ReadyToRead` inline through `OnReadyToRead` and skips the commit and apply
tiers and the LogDB write.

**Heartbeat batching.** Concurrent ReadIndex requests drained in one step cycle
share one heartbeat round; see [architecture](architecture.md#read-path-linearizable).

**LeaseRead.** With `Config.LeaseRead`, the leader records, per voter, the send
time of the newest heartbeat round it acked (`remoteLastAck`). The lease holds
while a quorum's acked rounds were sent less than `ElectionRTT - 2` ticks of real
time ago, less 1% for clock drift, on the leader's monotonic clock, and only
acks whose promised vote window covers the lease count. A paused leader loses
the lease. A valid lease serves ReadIndex with no heartbeat round; otherwise the
heartbeat path is used. See
[raft-consensus](../raft-consensus/engineering.md#15-leaseread).

### Replication chunking

`sendAppend()` bounds the entry bytes in one Replicate message by
`Config.MaxApplyEntrySize` (default `config.MaxReplicationPayloadBytes`,
16,711,672 bytes, the most one transport frame can carry). The follower's
`ReplicateResp` triggers the next chunk, which gives flow control without a
window.

### ReadyMap

One `sync.Mutex` guards the live map. `Set(shardID)` is one insert under the
lock. `Swap()` clears the alternate map outside the lock, exchanges pointers
under it, and returns the map that was live, which the caller iterates with no
lock held. `Swap` must be called only from the owning step worker, and the map
it returns is valid only until the next `Swap`, which recycles it with `clear()`.
`NewReadyMap` allocates three maps: the first `Swap` hands out the initial one
and rotation is between the other two from then on.

### Other hot-path structures

- **Cache-line padding.** `Node.commitPending` and `Node.lastApplied` are each
  padded to 128 bytes, the adjacent-line prefetch pair on x86 and ARM, so the
  step worker's reads and the apply worker's writes do not share a line.
- **`LogReader` term cache.** A fixed ring of `maxTermCacheEntries` (100,000)
  `(index, term)` pairs allocated once: O(1) append, O(log n) lookup and trim.
  Source: `pkg/logdb/reader.go`.
- **In-memory log compaction.** `inMemory` counts a `wastedPrefix` and compacts
  only when it is at least `entrySliceSize` (512) and at least twice the live
  length, so each copy is amortized against as much dead space as live data.
- **`Host.nodes` is a `sync.Map`** with an `atomic.Int64` count.
- **No-LogDB commit bypass.** The engine, run with no LogDB, has the step
  worker call `Peer.Commit()` and forward to `applyC`, skipping the commit
  worker. A `Host` always supplies a LogDB (`WithMemoryLogDB` is an in-memory
  one), so this mode is reachable only from engine-level code.
- **Proposal queue.** `EntryQueue[T]`, a double-buffered MPSC queue guarded by a
  `sync.Mutex`: producers hold it for one append, the consumer once per drain.
  Bounded at 256 entries; `Add` returns false when full.
- **Function-pointer dispatch.** Role-specific `stepFunc` and tick functions are
  set at role transitions, so `step()` does not switch on role per message.

### Transport

- **Connections.** A map of active connections under an `RWMutex`; dials are
  coalesced with `singleflight`. A cleanup pass every 30 s evicts dead
  connections and their pooled streams.
- **Stream pool.** Sharded over 16 buckets (`streamShardCount`). Heartbeats use
  stream 0; data uses `(shardID % (poolSize - 1)) + 1`, so heartbeats never queue
  behind a large Replicate. Lookup is double-checked: a read lock for an existing
  stream, a write lock with a re-check to open one. A peer that rejects a frame
  costs only that stream (`evictStream`); a write failure or dead connection
  evicts every stream for the target (`evictStreamsForTarget`).
- **Send path.** `Send()` walks the messages once, deep-copies entries and
  snapshot references (breaking the alias with the step worker's reused
  buffers), resolves the target, and enqueues on that target's queues (data
  capacity 2048, heartbeat capacity 64) without blocking. A per-target
  `sendQueueWorker` drains them, heartbeats first, coalescing up to 16
  heartbeats or 256 data messages per batch, frames and optionally compresses
  each batch, and splits an oversized batch across frames. Sends to different
  peers proceed in parallel. A worker exits after `sendQueueIdleTimeout` (60 s)
  idle. See [quic-transport](../quic-transport/engineering.md#send-path).
- **Persistent heartbeat stream.** `dialAndPool()` starts
  `preOpenHeartbeatStream()` after connecting, so the first heartbeat skips a
  stream-open round trip.

## Configuration

`pkg/config` holds `HostConfig`, `Config` and `TransportConfig`. `SetDefaults`
fills zero fields and `Validate` checks them; `NewHost` and `StartShard` run
both. Full tables are in [api.md](api.md#configuration). The settings whose
default is a trap:

| Field | Default | Why it matters |
|---|---|---|
| `HostConfig.MaxTotalInMemLogSize` | 0 (disabled) | without it one client can grow the in-memory log until the process dies |
| `HostConfig.MaxWALDiskSize` | 0 (unbounded) | without it the WAL fills the volume and a failed `fdatasync` poisons the store |
| `HostConfig.NoSyncWAL` | false | tests and container benchmarks only; `Validate` logs a warning |
| `HostConfig.KeyRotationInterval` | 0 | no scheduled rotation; 24h is a reasonable production value. Budget-driven rotation runs whenever a barrier is configured |
| `HostConfig.MaxRetainedEpochs` | 0 (retain all) | see [seal-barrier](../seal-barrier/user.md) |
| `Config.SnapshotEntries` | 100000 | zero selects the default; use `config.SnapshotsDisabled` |
| `Config.SessionExpiryEntries` | 100000 | zero selects the default; use `config.SessionExpiryDisabled` |
| `Config.CheckQuorum`, `PreVote`, `OrderedConfigChange` | nil = enabled | use `config.BoolPtr(false)` to disable; each logs a warning |
| `Config.MaxSessionsPerClient` | 128 | bounds cached responses per session, not sessions |
| `Config.LeaseRead` | false | requires `CheckQuorum` (`ErrLeaseReadRequiresCheckQuorum`) |
| `Config.Witness` | zero | bootstrap seed only; the live policy is replicated |
| `TransportConfig.MTLSConfig` | nil | required whenever the transport is enabled (`ErrMTLSConfigRequired`) |

There is no apply-retry setting, and adding one would be a defect.

## Test strategy

`make test` runs the unit suite and `make ci` is the gate. Per-package targets
are in each feature folder's `cli.md`.

| Layer | Command | Covers |
|---|---|---|
| unit | `make test` | every package (`go test ./...`, no build tags) |
| race | `go test -race` | the lock-free claims above |
| fuzz | `make fuzz-all` | 37 targets across proto, transport, waldb, seal, session, raft |
| integration | `make integration-test` | real clusters in containers |
| linearizability | `make integration-test-linearizability` | observed histories are linearizable |
| I/O faults | `make ci-errfs` | the `test/errfs` harness |
| network faults | `make ci-netfault` | partitions, delays, drops |
| FIPS | `make verify-fips`, `make verify-fips-exempt` | approved-only enforcement inside the boundary (QUIC transport included), and `fips140=on` over `pkg/internal/transport/revocation` |
| benchmarks | `make bench-compare` | regression against stored baselines |

The host-level regression suites each encode a bug that was found once, and must
not be deleted in a refactor: `host_hardening_regression_test.go`,
`host_pending_lifecycle_test.go`, `host_state_regression_test.go`,
`host_api_regression_test.go`, `host_config_fault_regression_test.go`,
`messagehandler_hardening_test.go`.

**Coverage.** The Makefile floor is `COVER_MIN = 91`: `make coverage` checks the
module total and `make coverage-packages` holds every package under `pkg/` to the
same floor. CI gates both the total and every package at its
`COVERAGE_THRESHOLD` (90). Coverage runs without build tags, so the integration
and fault-injection suites do not count toward it.

**Rules for tests here:** no `time.Sleep` to wait for consensus (use the request
channels, listener notifications or diagnostic accessors), no assertion on a
latency histogram bucket, and no `WithNoSync(true)` in a durability test.

## Observability

The `Host` is where the collector, the logger and the event listener are
installed. Interfaces, the closed `reason` vocabularies, metric names and
alerting guidance are in [observability](../observability/engineering.md) and
[api.md](api.md#metrics).

Host-level introspection, separate from metrics:

```go
host.HealthCheck()                // *HealthStatus
host.GetNodeHostInfo(opt)         // *NodeHostInfo
host.GetShardInfo(shardID)        // *ShardInfo, including ShardHealth
host.ShardCount()
host.ID(); host.NodeIdentity(); host.RaftAddress()
host.IsSealed(); host.IsDraining()
host.DiagRaftState / DiagPipelineState / DiagShardGates / DiagRemotes
```

The `Diag*` accessors are for incident response, not monitoring, and their
signatures may change.

## Cross-references

- [Usage](user.md): configuration, monitoring, tuning, disaster recovery
- [Compatibility and Upgrades](../compatibility.md): format versions
- [Raft protocol and engine pipeline](../raft-consensus/engineering.md)
- [Log storage](../log-storage/engineering.md): segment format, framing,
  compaction
- [Snapshots](../snapshots/engineering.md): save, transfer, recovery, integrity
- [State machines](../state-machines/engineering.md): `Open`, `Update`,
  `SaveSnapshot`, `RecoverFromSnapshot`
- [Testing infrastructure](../testing/infrastructure.md): fault injection,
  partitions, the linearizability suite
