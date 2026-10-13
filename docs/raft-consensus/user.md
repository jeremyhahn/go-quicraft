# Raft consensus: usage

> **Status: NORMATIVE** · Scope: consensus behavior as an embedder experiences
> it, through `pkg.Host` · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

This page covers proposing, reading, tuning timing, and reading the failures.
Starting and stopping shards is in
[`../multi-group-host/user.md`](../multi-group-host/user.md); adding replicas is
in [`../membership-changes/user.md`](../membership-changes/user.md).

## Before you start

- A running `Host` with at least one started shard.
- A deterministic state machine; see
  [`../state-machines/user.md`](../state-machines/user.md).
- A value for `RTTMillisecond` that matches your network. Every timeout is a
  multiple of it, so it is the one number that retunes a cluster. The default,
  200 ms, is sized for a WAN; on a LAN 1 to 5 ms is typical.

## Propose a command

```go
result, err := host.SyncPropose(ctx, shardID, cmd)
```

Or asynchronously:

```go
rs, err := host.Propose(ctx, shardID, cmd)
// ... await rs.ResultC(), then rs.Release()
```

Proposals are rejected at once rather than left to time out: a done context, a
closed or draining host, an unknown shard, a witness replica, a full disk, an
oversized command, and no known leader, which returns `*NotLeaderError`
immediately.

```go
if leaderID, ok := quicraft.NotLeader(err); ok {
    // redirect to leaderID
}
```

## Handle `MaybeApplied` correctly

Getting this wrong duplicates writes.

```go
result, err := host.SyncPropose(ctx, shardID, cmd)
if quicraft.MaybeApplied(err) {
    // The proposal MAY have been applied. Retrying a non-idempotent
    // command here applies it twice.
}
```

After some failures the only honest answer is "unknown". If your command is not
idempotent, use a session
([`../client-sessions/user.md`](../client-sessions/user.md)): the shard
recognizes the retry and does not apply it twice.

`quicraft.Temporary(err)` tells you whether a retry is reasonable at all.

## Choose a read

| You want | Use | Costs |
|---|---|---|
| linearizable | `SyncRead` | a heartbeat quorum round, or none under `LeaseRead` |
| everything committed when the read started, from any replica | `FollowerRead` | waits for local apply; no leadership check, so it can lag during a partition |
| fast, may be stale | `StaleRead` | nothing |
| linearizable, pooled buffer | `SyncReadBuf` | as `SyncRead`, plus a `release` call you must make |

```go
val, err := host.SyncRead(ctx, shardID, query)

buf, release, err := host.SyncReadBuf(ctx, shardID, key)
defer release()   // do not skip this
```

The `*Buf` forms skip the allocation when your state machine implements
`sm.NALookupInto`, and fall back to an ordinary lookup when it does not.

Concurrent linearizable readers on one shard already share a heartbeat round,
so you do not need to batch them yourself.

## Turn on `LeaseRead`

```go
cfg.LeaseRead   = true
cfg.CheckQuorum = nil        // nil means enabled, which is required
cfg.ElectionRTT = 10         // must be >= 5
```

Linearizable reads then resolve without a heartbeat round while the lease
holds. Validation refuses `LeaseRead` with `config.ErrLeaseReadRequiresCheckQuorum` if
`CheckQuorum` is off, and with a `*config.ValidationError` if
`ElectionRTT < 5`.

The lease is measured on the leader's monotonic clock, so a leader that is
paused or descheduled loses it and falls back to the heartbeat round. It
assumes no two nodes' clocks run at rates more than 1% apart. Each follower
states in its acknowledgements how long it will withhold its vote, so replicas
configured differently stay safe; configure every replica with the same
`ElectionRTT`, `RTTMillisecond` and `LeaseRead` for reads to stay fast.

It costs availability after a restart. A restarted replica of a `LeaseRead`
shard, witnesses included, neither votes nor campaigns for about one election
timeout after it starts, because a leader may still be counting its
pre-restart acknowledgement toward a lease. This holds even if it restarts
with `LeaseRead` off. It keeps accepting replication meanwhile. After a
full-shard restart the first leader is elected about one election timeout
later than without `LeaseRead`.

## Tune timing

Everything is a multiple of `RTTMillisecond`:

```go
hostCfg.RTTMillisecond = 200
cfg.ElectionRTT  = 10   // 2 s election timeout
cfg.HeartbeatRTT = 1    // 200 ms heartbeats
```

If you see spurious elections, raise `ElectionRTT` first. Lower `HeartbeatRTT`
only if you accept paying bandwidth on every shard for faster detection.

**Leave `CheckQuorum` and `PreVote` alone.** Both default to enabled.
`CheckQuorum` stops a partitioned leader serving reads it cannot confirm;
`PreVote` stops a rejoining node forcing an election in a healthy term. They
are `*bool`, so nil means enabled. Use `config.BoolPtr(false)` only if you mean
to disable one.

## Set the rate limiter

```go
hostCfg.MaxTotalInMemLogSize = 1 << 30   // 0 means disabled
```

Set this in production. Without it, a client that proposes faster than the
cluster commits grows the in-memory log until the process dies. With it,
proposals are rejected with `ErrSystemBusy` (which matches `ErrBusy`): back
pressure instead of running out of memory.

## Size the worker pool

```go
hostCfg.NumWorkers        = uint64(runtime.NumCPU())   // step, commit and apply tiers
hostCfg.MaxApplyWorkers   = runtime.NumCPU()           // apply semaphore
hostCfg.CommitCBufferSize = 64
```

These are the defaults. Raise `MaxApplyWorkers` when apply is CPU-bound and the
pipeline is starving on it. Shards are assigned to workers by
`shardID % NumWorkers`, so a worker count that divides badly into your shard
IDs leaves some workers busier than others.

## Quiesce idle shards

```go
cfg.Quiesce = true
```

Worth it with thousands of mostly idle shards. An idle shard's leader replaces
regular heartbeats with infrequent liveness probes, and its followers stretch
their election timeouts. The shard wakes on the next proposal, read, config
change or inbound replication.

## React to leadership changes

```go
hostCfg.EventListener = &config.EventListener{
    OnLeaderUpdated: func(info config.LeaderInfo) { /* ... */ },
    OnLeaderReady:   func(info config.LeaderInfo) { /* ... */ },
    OnShardFailed:   func(info config.ShardFailedInfo) { /* ... */ },
}
```

`OnLeaderUpdated`, `OnLeaderReady`, `OnLeaderLost`, `OnCampaignLaunched`,
`OnShardFailed` and the snapshot and compaction notifications
(`OnSnapshotCreated`, `OnSnapshotCompacted`, `OnSnapshotRecovered`,
`OnLogCompacted`, `OnLogDBCompacted`, `OnSnapshotRejected`) run on a dedicated goroutine that holds no engine lock, so your handler may
call any `Host` API, including the synchronous `StopShard`. The other
`EventListener` callbacks run on internal worker goroutines and must return
quickly without calling back into the `Host`. Do not assume any ordering
between the two groups.

### Wait for a shard to serve reads

A new leader may not serve `ReadIndex` until it has committed an entry of its
own term (Raft thesis §6.4); until then `ReadIndex` returns
`ErrReadIndexNotReady`. `OnLeaderReady` is the event for that moment: it fires
on the leader once its first entry of the term commits, and on a follower or
observer once its own commit index reaches an entry of the current term. It
arrives after the `OnLeaderUpdated` for the same leader and term, at most once
per announcement, and the next `OnLeaderUpdated` ends it. That covers the same
leader re-elected at a higher term, which `OnLeaderLost` does not. Witnesses
never receive it. `host.ShardReadReady(shardID)` returns the same
`(leaderID, term, ready, err)` without blocking; `err` carries the refusals a
`ReadIndex` would make before reaching Raft.

## Diagnose a stuck shard

```go
term, committed, processed, lastIdx, msgs, state, err := host.DiagRaftState(shardID)
remotes, err := host.DiagRemotes(shardID)
```

Those two answer what this replica thinks the term and commit index are, and
which follower it is stuck on. `DiagPipelineState` and `DiagShardGates` add
queue depths and the recovering and saving gates.

## Troubleshooting

**`*NotLeaderError` with no leader ID**: no leader is known at all. The shard is
mid-election or has lost quorum.

**Frequent leader changes**: heartbeats are arriving later than the election
timeout. In order of likelihood: `ElectionRTT` too low for the network, a
CPU-starved process, or a slow disk backing up the commit worker. Check
send-queue depth ([`../quic-transport/user.md`](../quic-transport/user.md))
before assuming a node failed.

**`ErrSystemBusy` / `ErrBusy`**: the in-memory log limit was reached. This is
back pressure working; slow down or raise `MaxTotalInMemLogSize`.

**`ErrDropped`**: the shard's proposal queue was full. Retry with back-off.

**`ErrInvalidOperation` on a propose or a read**: the replica is a witness.
Witnesses vote but store no log data, so they cannot propose or serve a read.
See [`../membership-changes/user.md`](../membership-changes/user.md).

**`ErrTransferInProgress`**: a leadership transfer is running. It is temporary;
retry.

**`ErrDiskFull`**: the WAL disk monitor tripped. Proposals are refused rather
than letting `fdatasync` hit ENOSPC and poison the store.

**`ErrShardFailed`, or a shard that will not progress**: a step, commit or
apply failure marked it failed: an apply error or a state machine panic, a
`LogDB.SaveState` that still failed after its retries, or a committed entry the
log could no longer read. There is no retry, by design. Check the
`OnShardFailed` notification (`ShardFailedInfo.Err` names the cause), then stop
and restart the shard.

**Reads are slower than writes**: a linearizable read costs a heartbeat quorum
round. Use `LeaseRead` if you can meet its requirements, or `StaleRead` if you
can accept its guarantee.

**A read hangs until the context deadline on a single-node shard**: this is a
defect; report it.

**Everything stalls after a listener callback, or `Host.Close` never returns**:
a callback that runs on a worker goroutine is blocking or calling back into the
`Host`. Only the dispatched callbacks listed above may do that. `Host.Close`
also waits for every in-flight `Apply`, so a state machine that blocks in
`Apply` blocks `Close`.

**Two replicas disagree about applied state**: not a consensus bug. Consensus
guarantees the same entries in the same order; what a replica does with them is
your state machine's determinism. Compare `StateMachineHash` across replicas.
