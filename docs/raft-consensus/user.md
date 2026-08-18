# Raft Consensus — Usage

> **Status: NORMATIVE** · Scope: consensus behavior as an embedder experiences
> it, through `pkg.Host` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

This page is about making consensus behave: proposing, reading, tuning timing,
and reading the failures. Starting and stopping shards is
[`../multi-group-host/user.md`](../multi-group-host/user.md); adding replicas is
[`../membership-changes/user.md`](../membership-changes/user.md).

## Before you start

- A running `Host` with at least one started shard.
- A deterministic state machine. Everything below assumes it; see
  [`../state-machines/user.md`](../state-machines/user.md).
- One decision: **is your `RTTMillisecond` right for your network?** Every
  timeout in the system is a multiple of it, so it is the single number that
  retunes a cluster. The default of 200 ms suits a LAN.

## Task: propose a command

```go
result, err := host.SyncPropose(ctx, shardID, cmd)
```

Or asynchronously:

```go
rs, err := host.Propose(ctx, shardID, cmd)
// ... await rs
```

Proposals are rejected fast rather than left to time out: a done context, a
closed or draining host, an unknown shard, a witness replica, a full disk, an
oversized command, and — importantly — **no leader known at all**, which returns
`NotLeaderError` immediately.

```go
if leaderID, ok := quicraft.NotLeader(err); ok {
    // redirect to leaderID
}
```

## Task: handle `MaybeApplied` correctly

This is the error people get wrong, and getting it wrong duplicates writes.

```go
result, err := host.SyncPropose(ctx, shardID, cmd)
if quicraft.MaybeApplied(err) {
    // The proposal MAY have been applied. Retrying a non-idempotent
    // command here applies it twice.
}
```

The honest answer after certain failures is "unknown". If your command is not
idempotent, the fix is a **session** — see
[`../client-sessions/user.md`](../client-sessions/user.md) — which makes the
retry safe because the shard recognizes it.

`quicraft.Temporary(err)` tells you whether a retry is reasonable at all.

## Task: choose a read

| You want | Use | Costs |
|---|---|---|
| linearizable | `SyncRead` | a heartbeat quorum round, or none under `LeaseRead` |
| fast, may be stale | `StaleRead` or `QueryLocalNode` | nothing |
| read from a follower | `FollowerRead` | nothing; may lag |
| linearizable, no allocation | `SyncReadBuf` | as `SyncRead`, plus a `release` call you must make |

```go
val, err := host.SyncRead(ctx, shardID, query)

buf, release, err := host.SyncReadBuf(ctx, shardID, key)
defer release()   // do not skip this
```

The `*Buf` forms need your state machine to implement `sm.NALookup` or
`sm.NALookupInto`.

Concurrent linearizable readers on one shard already share a single heartbeat
round, so issuing many at once is cheap. You do not need to batch them yourself.

## Task: turn on `LeaseRead`

```go
cfg.LeaseRead   = true
cfg.CheckQuorum = nil        // nil means enabled, which is required
cfg.ElectionRTT = 10         // must be >= 5
```

Linearizable reads then resolve inline while the lease holds, with no heartbeat
round. It is rejected at validation with `ErrLeaseReadRequiresCheckQuorum` if
`CheckQuorum` is off or `ElectionRTT < 5` — a lease without quorum checking is a
linearizability violation waiting for a partition.

## Task: tune timing

Everything is a multiple of `RTTMillisecond` (200 ms):

```go
hostCfg.RTTMillisecond = 200
cfg.ElectionRTT  = 10   // 2s election timeout
cfg.HeartbeatRTT = 1    // 200ms heartbeats
```

If you see spurious elections, raise `ElectionRTT` before anything else. Lower
`HeartbeatRTT` only if you understand you are buying detection speed with
bandwidth on every shard.

**Leave `CheckQuorum` and `PreVote` alone.** Both default to enabled, and both
prevent real failure modes: `CheckQuorum` stops a partitioned leader serving
reads it cannot confirm; `PreVote` stops a rejoining node forcing an election in
a healthy term. They are `*bool`, so nil means enabled — use
`config.BoolPtr(false)` if you genuinely mean to disable one.

## Task: set the rate limiter

```go
hostCfg.MaxTotalInMemLogSize = 1 << 30   // 0 means DISABLED
```

**Set this in production.** Without it a single client that proposes faster than
the cluster commits grows the in-memory log until the process dies. With it,
proposals are rejected with `ErrBusy` — back-pressure instead of an OOM.

## Task: size the worker pool

```go
hostCfg.NumWorkers        = runtime.NumCPU()   // step, commit and apply tiers
hostCfg.MaxApplyWorkers   = runtime.NumCPU()   // apply semaphore
hostCfg.CommitCBufferSize = 64
```

Raise `MaxApplyWorkers` when apply is CPU-bound and the pipeline is starving on
it. Shards are partitioned across workers by `shardID % NumWorkers`, so a
worker count that divides badly into your shard count leaves some workers
busier than others.

## Task: quiesce idle shards

```go
cfg.Quiesce = true
```

Worth it with thousands of mostly-idle shards: an idle shard stops ticking, so
it stops sending heartbeats and stops consuming a worker's cycle. It wakes on
the first proposal.

## Task: react to leadership changes

```go
hostCfg.EventListener = &config.EventListener{
    LeaderUpdated: func(info config.LeaderInfo) { ... },
    // shard failure, campaign launched, ...
}
```

These run on a dedicated dispatcher goroutine that holds no engine lock, so your
handler **may call any `Host` API, including the synchronous `StopShard`**. That
is deliberate: reacting to a failed shard by stopping it is the obvious handler
to write, and it used to deadlock the whole worker.

Do not assume any ordering between these notifications and the synchronous
callbacks (`OnApplied`, `OnCommitted`, …). There is none, on purpose.

## Task: diagnose a stuck shard

```go
term, committed, processed, lastIdx, msgs, state, err := host.DiagRaftState(shardID)
remotes, err := host.DiagRemotes(shardID)
```

Those two answer the questions that matter: what does this replica think the
term and commit index are, and which follower is it stuck on.
`DiagPipelineState` and `DiagShardGates` add queue depths and the
recovering/saving gates.

## Troubleshooting

**`NotLeaderError` with no leader id** — no leader is known at all, which is the
fast-fail path rather than a timeout. The shard is mid-election or has lost
quorum.

**Frequent leader changes** — heartbeats are being delayed past the election
timeout. In order of likelihood: `ElectionRTT` too low for the network, a
CPU-starved process, or a slow disk making the commit worker back up. Look at
send-queue depth in [`../quic-transport/user.md`](../quic-transport/user.md)
before assuming a node failed.

**`ErrBusy`** — the rate limiter or a full queue. This is back-pressure working;
slow down or raise `MaxTotalInMemLogSize`.

**`ErrInvalidOperation` on a propose or a read** — the replica is a **witness**.
Witnesses vote but store no log data: they cannot propose and cannot serve a
ReadIndex. See [`../membership-changes/`](../membership-changes/user.md).

**`ErrDiskFull`** — the WAL disk monitor tripped. Proposals are rejected
deliberately rather than letting `fdatasync` hit ENOSPC and poison the store.

**A shard stopped and will not restart** — an apply error marked it failed.
There is no retry, by design. Check the `ShardFailedInfo` notification and your
state machine's error.

**Reads are slower than writes** — a linearizable read costs a heartbeat quorum
round. Use `LeaseRead` if you can meet its requirements, or `StaleRead` if you
can accept its guarantee.

**A read hangs until the context deadline on a single-node shard** — historically
an alias-registration ordering bug, fixed by registering aliases before
`Peer.Handle`. If you see it now, it is a defect worth reporting.

**Everything stalls after a listener callback** — you are calling back into the
engine from a **synchronous** callback (`OnApplied`, `OnCommitted`, …) rather
than from a dispatched one. The synchronous set has happens-before requirements
against `Peer.Handle` and cannot wait on engine work.

**`Host.Close` never returns** — historically the same deadlock the listener
dispatcher exists to prevent. Check that no synchronous callback is blocking.

**Two replicas disagree about applied state** — not a consensus bug. Consensus
guarantees the same entries in the same order; what a replica *does* with them
is your state machine's determinism. Use `StateMachineHash`.
