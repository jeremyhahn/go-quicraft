# Raft consensus: architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/raft`,
> `pkg/internal/engine`, `pkg/internal/queue`, `pkg/internal/server`,
> `pkg/internal/invariant` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

## The problem

Raft as published is one group. This library runs thousands of them in one
process, sharing one set of CPUs, one WAL, one transport and one snapshot
budget. Every design decision below follows from that: the algorithm is
textbook, and the execution model is where the work is.

## The shape chosen

`pkg/internal/raft` is a **passive state machine**. `Peer` has no goroutines, no
timers and no I/O: you `Handle` messages into it, `Tick` it, and `GetUpdate`
out of it. Everything that touches the world (persisting, sending, applying)
is the engine's, outside the algorithm's lock.

```
   Peer.Handle(msgs) ─▶ Peer.GetUpdate() ─▶ proto.Update
      (pure)              (pure)               │
                                               ▼
                                     engine: persist, send, apply
                                               │
                                     Peer.Commit(update)
```

### Rejected: a goroutine and timers per Raft group

The obvious shape, and unusable at 10,000 groups: 10,000 goroutines each with a
ticker, each doing its own I/O, each scheduled independently. The passive-peer
design lets N workers drive M shards, with `N = NumWorkers` and `M` in the
thousands.

### Rejected: doing I/O inside the algorithm

If `Peer` persisted and sent, then `raftMu` would be held across an fsync and a
network write. Splitting it means the lock covers only state transitions, and
the slow parts happen on the worker's own time.

## The three-tier pipeline

Shards are partitioned across workers by `shardID % NumWorkers`.

```
Step Workers (N)    tick; drain proposalQ, inboxQ, readIndexQ; Peer.Handle;
                    Peer.GetUpdate; send FREE-ORDER messages NOW
       │            (Replicate, Heartbeat, …)
       ▼            commitC, cap CommitCBufferSize (64)
Commit Workers (N)  LogDB.SaveState — persist entries + term/vote;
                    THEN send ORDERED messages (VoteResp, HeartbeatResp);
       │            Peer.Commit
       ▼            applyC, cap max(CommitCBufferSize*4, 256)
Apply Workers (N)   dispatcher fans out to a per-shard goroutine (chan cap 64),
                    each gated by the MaxApplyWorkers semaphore
       │
Snapshot Pool       max(NumWorkers/2, 2) workers, async save and recover
Listener Dispatcher one goroutine, user-visible cold-path notifications
```

**The free-order / ordered split is a correctness rule, not a latency
optimization.** A `Replicate` or `Heartbeat` may go out before the sender's own
state is durable, because it promises nothing about the sender. A `VoteResp` or
`HeartbeatResp` promises "I have persisted this", so it is held until the commit
worker has actually persisted it. Sending a vote response before the vote is
durable is how a crash produces two leaders in one term.

**When the LogDB is nil** (memory mode), step workers inline `Peer.Commit` and
forward straight to `applyC`, cutting three hops to two.

**Every tier restarts on panic.** A panicking step, commit or apply worker would
otherwise stall every shard mapped to it permanently, so recovery clears
`commitPending` for the affected nodes and re-enters the loop after a short
delay.

**Shutdown is ordered, six steps**: stop all nodes (which unblocks in-flight
snapshot I/O) → step → commit → apply → snapshot → listener dispatcher, draining
at each stage. The dispatcher goes **last** so a leadership loss or shard
failure observed during teardown still reaches the listener.

## The listener dispatcher prevents a deadlock

User-visible cold-path notifications (`OnLeaderUpdated`, `OnLeaderReady`,
`OnCampaignLaunched`, shard failure) run on one dedicated goroutine rather than on the worker that
produced them. This is a correctness rule, not a throughput optimization.

The step worker holds `stepWorker.nodesMu.RLock` while it runs, and
`Host.StopShard` takes the same worker's `nodesMu.Lock()` through
`Engine.UnloadNode`. Go's `RWMutex` is not reentrant, so a handler invoked on
the worker that reacted to an event by stopping the shard would deadlock that
worker: every shard on it would stop ticking, and `Host.Close` would never
return. Stopping a failed shard is the obvious handler to write.

### Rejected: callbacks on the producing worker

Cheaper by one channel hop, and unsafe for the reason above.

The dispatcher goroutine holds no engine lock, so a listener may call any `Host`
API including the synchronous `StopShard`. Notifications are FIFO on a single
consumer, giving a total order and therefore a per-shard order.

## Read paths, and what each one costs

| Path | Guarantee | Cost |
|---|---|---|
| `SyncRead` / `ReadIndex` | linearizable | a heartbeat quorum round, or none under `LeaseRead` |
| `StaleRead` | none: local state as of whatever has applied | free |
| `FollowerRead` | waits until local apply reaches the local commit index; no leadership check, so it may lag the leader | no quorum round; may wait for apply |

### Read readiness is an event, not a retry loop

A new leader may not serve ReadIndex until it has committed an entry of its own
term (PhD §6.4): its commit index could otherwise trail an entry a previous
leader committed. Until then `ReadIndex` returns `ErrReadIndexNotReady`.
`OnLeaderReady` tells a consumer when that ends, so it never has to poll.

**Readiness is defined as the gate itself.** On the leader it is
`hasCommittedEntryAtCurrentTerm`, the very test `handleLeaderReadIndex`
applies, so the event cannot run ahead of what ReadIndex will accept. On a
follower or observer the same predicate holds with no extra protocol: a
follower learns its commit index only from a leader, and only the leader of its
current term T can have sent it an entry of term T, so a committed entry of
term T on the follower proves that leader had already committed it. A forwarded
read is therefore not refused as not-ready either. Every replica that serves
reads gets the event; witnesses store no log and are excluded.

**It is published with the leader and term, as one triple.** The peer's
leader/term seqlock carries `(leaderID, term, readReady)`, written together by
`syncLeaderID` after every step and again at the end of `Commit`, where a
leader's first commit of its term lands. A reader can never see readiness from
one term paired with another term's leader. The predicate is monotonic within
a term, so the peer caches the last answer and skips the log lookup until the
term or the commit index moves.

**Ordering rides on the leader change.** The step worker delivers
`OnLeaderReady(leader, term)` only after the `OnLeaderUpdated` for the same
pair was accepted by the dispatcher, so FIFO delivery puts it after that
announcement and before the next one. The end of readiness needs no event of
its own: every `OnLeaderUpdated` names a different `(leader, term)` pair and
ends it, including the same leader re-elected at a higher term. `OnLeaderLost`
does not fire there, because leadership never changed hands, which is why
readiness is not tied to it. The host mirrors the triple in `nodeState` under
its own seqlock, and every leader update clears the mirrored flag;
`Host.ShardReadReady` reads that mirror, so the query is never ahead of the
event.

**A refused readiness is retried, never dropped.** A full dispatcher queue
refuses a notification. A dropped leader change is harmless because the next
one supersedes it, but a consumer waiting for readiness has nothing else to
wake it, so the step worker re-publishes it on the next tick, behind its leader
change.

#### Rejected: readiness on the leader only

Followers forward reads, and a consumer on a follower would be left polling. The
follower's commit index already carries the proof, so propagating readiness
costs nothing on the wire.

#### Rejected: ending readiness with `OnLeaderLost`

It fires only when the local replica stops leading. It misses a follower whose
leader changed, and a leader re-elected at a higher term, which is not ready
again until that term's first commit.

### ReadIndex batching, and why alias registration comes first

Concurrent readers on one shard share a **single heartbeat round**. The step
worker drains the whole `readIndexQ` in one cycle, submits the first key to
raft, and registers the rest as aliases through `OnReadIndexBatched`.

**Alias registration happens before `Peer.Handle` on purpose.** On a single-node
shard `Handle` resolves the ReadIndex synchronously, so registering afterwards
would let `OnReadyToRead` complete and delete the batch key before the aliases
were attached, stranding every aliased caller until its context deadline.

The leader's heartbeat carries its own monotonic `readIndexCtx`, and **ctx 0 is
reserved for plain heartbeats**, so an ordinary heartbeat response can never
satisfy a ReadIndex quorum.

`LeaseRead` (PhD §6.4) resolves inline while the lease holds, with no heartbeat
at all. It requires `CheckQuorum` (`ErrLeaseReadRequiresCheckQuorum` otherwise:
a lease without quorum checking is a linearizability violation waiting for a
partition) and `ElectionRTT >= 5` (a `*config.ValidationError` otherwise). The lease is
measured in monotonic real time from when the acknowledged heartbeat round was
sent, so a paused leader loses it, and it assumes clock rates within 1% of each
other. Every heartbeat acknowledgement states the vote window its sender
promises, and the leader counts only acknowledgements whose window covers the
lease, so a replica configured to promise less backs no lease rather than an
unsound one. The promise is recorded in the replica's Raft hard state before
any acknowledgement makes it, and a restarted replica withholds its vote, and
does not campaign, for the recorded window, whatever configuration it
restarted into, so a full-shard restart elects about one election timeout
later.

## Request keys are random draws, and applied entries are placed by index too

`entry.Key` is a random 64-bit value from `nextRequestKey` (`math/rand/v2`,
never zero), the same draw for proposals, reads and snapshots.

### Rejected: `(replicaID, per-Host sequence)` keys

Unique across hosts, but they repeat across one host's incarnations: a
restarted replica would mint its previous incarnation's keys while replaying
that incarnation's log, and a replayed entry would complete a fresh proposal
with an old result.

Behind the draw, every pending proposal records the committed index as last published to
the host at registration as a floor; `OnApplied` and `OnCommitted` refuse an
entry at or below it, so an entry that was committed and published before the
proposal existed is never mistaken for it, whatever its key says. The published
marker can lag the peer's commit by the step cycle in progress, so this is a
bound that makes the replay case certain, not a guarantee for every entry
committed before registration.

## Lock-free where it is measured, mutexed where it is not

Deliberately lock-free:

- Request keys: random draws from `math/rand/v2`'s process-wide generator,
  which is lock-free for callers; there is no per-Host counter to contend on.
- `Node.commitPending` and `Node.lastApplied`, padded to 128 bytes (the
  prefetch-adjacent line pair on x86 and ARM) to prevent false sharing between
  the step worker that reads them and the apply worker that writes them.
- `nodeState` fields: leaderID, term, lastApplied, isLeader, all atomic.
- `Host.nodes`: a `sync.Map`, with `activeNodes atomic.Int64` for the count.
- `Host.registry`: over a `sync.Map`, lock-free on the `Resolve` hot path.
- `Barrier.unsealed`: an `atomic.Bool` fast path before the RWMutex.

Deliberately **not**, and this is stated so nobody "fixes" it:

- The `EntryQueue` behind `proposalQ`, `inboxQ` and `readIndexQ` is
  **mutex-guarded**. Producers hold a `sync.Mutex` for a slice append and the
  consumer takes it once per drain cycle. The design goal is **amortizing
  acquisitions over a batch**, not eliminating them.
- The per-node `raftMu`, which serializes `Peer.Tick` against `Peer.Commit` and
  covers the step worker's `Peer.Step` and inbox handling. It is not reentrant,
  which is why event callbacks are invoked outside it.

## Trust boundary

- **Inbound messages are authenticated by the transport**, not here. The peer
  identity is derived from the mTLS leaf and checked against committed
  membership; see
  [`../quic-transport/architecture.md`](../quic-transport/architecture.md).
  This layer trusts that a message it is handed came from the member it claims.
- **A message from a sender not authorized for the shard is dropped** and
  counted before it reaches `Peer`; an unauthorized snapshot transfer is
  refused with `*UnauthorizedMessageError`. The gate is in
  [`../membership-changes/engineering.md`](../membership-changes/engineering.md#per-shard-authorization-is-a-deny-list).
- **Rate limiting is two-level**: a global in-memory-log limiter
  (`MaxTotalInMemLogSize`) and a per-shard one, both applied before a proposal
  is accepted. Without them a single client can grow the in-memory log until the
  process dies.
- **Proposal payloads are application data.** Bounded by
  `min(MaxProposalPayloadSize, MaxEntrySize)` and never inspected or logged.
- **The apply path is embedder code.** See
  [`../state-machines/architecture.md`](../state-machines/architecture.md).

## Component boundaries

- `pkg/internal/raft`: the protocol: `Peer`, the log, the in-memory log,
  membership, remotes, quiescence.
- `pkg/internal/engine`: the workers, the listener dispatcher, `Node`,
  `ReadyMap`, the snapshot pool.
- `pkg/internal/queue`: `EntryQueue` (double-buffered MPSC), `SwapBuffer`.
- `pkg/internal/server`: the global in-memory-log rate limiter.
- `pkg/internal/invariant`: debug assertions.
- `pkg/internal/supervise`: restart-on-Goexit supervision, exit
  classification, resilient shutdown drains and contained diagnostics, shared
  with the transport and the host.
- Consumers: `pkg.Host`, the only exported surface.

All are `internal/`; the embedder drives them only through `pkg.Host`.

## What this feature deliberately does not do

- **No peer discovery and no cluster-formation bootstrap.** Those live in
  go-qrdb and must not be added here. `StartShard(members, join, create, cfg)` is
  the primitive a cluster manager builds on; the caller supplies the member map.
- **No quorum validation before starting a shard.** The caller's problem.
- **No apply retry**, and no configuration to add one.
- **No wall clock in the algorithm.** Election and heartbeat timeouts are
  multiples of `RTTMillisecond`, driven by ticks, because two replicas with
  different clocks would otherwise make different decisions. The only
  real-time measurements are the read lease and the post-restart vote
  hold-down, both on a monotonic clock and both bounded by the 1% clock-rate
  assumption.
- **No sleeping to wait for consensus.** The protocol never sleeps; the engine
  waits on channels and selects. The exceptions are fixed backoffs on the
  snapshot paths: a 50 µs yield while an apply drains around snapshot recovery,
  and retry backoffs (100 ms, 50 ms) when a snapshot control message or a
  recovery request meets a full queue.
- **No multi-Raft transactions.** Each shard is independent; there is no
  cross-shard atomicity.
- **No dynamic re-sharding.** A shard's identity is fixed at `StartShard`.
- **No exported Raft types.** `Peer`, `Node` and the rest are internal, so their
  shape is not a compatibility surface.
