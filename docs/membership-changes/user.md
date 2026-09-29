# Membership Changes — Usage

> **Status: NORMATIVE** · Scope: go-quicraft membership · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Adding, removing and re-roling replicas in a shard that already exists. Forming
the shard in the first place is `StartShard` —
[`../multi-group-host/user.md`](../multi-group-host/user.md).

## Before you start

- The change must be sent to the **current leader** (`GetLeaderID`).
- You need the current `ConfigChangeID`, read immediately before the change.
- The new replica needs a certificate whose SAN covers the address you are about
  to advertise for it, signed by the cluster's CA. See
  [`../quic-transport/user.md`](../quic-transport/user.md).

## The pattern every membership change follows

```go
m, err := host.SyncGetShardMembership(ctx, shardID)
if err != nil {
    return err
}
err = host.SyncRequestAddNodeWithIdentity(ctx, shardID,
    newReplicaID, newAddr, newIdentity, m.ConfigChangeID)
```

Two things that are not optional:

1. **Use the `Sync` read.** `GetShardMembership` reads local state, which may be
   behind, and a stale `ConfigChangeID` guarantees the change is refused.
2. **Pass `ConfigChangeID`.** It is the replay guard. If another change landed
   between your read and your write, you get `*StaleConfigChangeError` — re-read
   and retry rather than working around it.

Only one change at a time. Adding three replicas is three sequential
changes, each with a fresh `ConfigChangeID`.

## Task: add a voting member

```go
err := host.SyncRequestAddNodeWithIdentity(ctx, shardID,
    replicaID, "node4.example.com:4001", identity, m.ConfigChangeID)
```

Then start the shard **on the new node** with `join = true`, anchored to the
replicas it may take its bootstrap snapshot from — the leader at least:

```go
cfg.MemberIdentities = map[uint64]string{leaderReplicaID: leaderIdentity}
err := newHost.StartShard(nil, true, createFn, cfg)   // join, empty member map
```

The order matters: add first, then start. A node started with `join = true`
before the cluster knows about it has nobody to catch up from.

`identity` is the SPKI pin the transport derives from the peer's mTLS leaf, the
value the new node reports from `Host.NodeIdentity()`:

```
"spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))
```

**The identity is required** on a host with a transport: `RequestAddNode`
without one returns `ErrMemberIdentityRequired`, and a joiner started without
an anchor is refused. A member is authorized by its committed identity and by
nothing else.

## Task: add an observer or a witness

```go
// Observer: replicates and serves reads, does NOT vote.
err := host.SyncRequestAddObserver(ctx, shardID, id, addr, m.ConfigChangeID)

// Witness: votes, stores NO log data, cannot propose or serve reads.
err := host.SyncRequestAddWitness(ctx, shardID, id, addr, m.ConfigChangeID)
```

Choose by what you actually want:

- **An observer** is a read replica or a warm standby. It costs a full copy of
  the log.
- **A witness** is a tiebreaker. It costs almost nothing to run — it stores no
  log — and it makes a two-node cluster able to elect. Proposing to a witness or
  reading from one returns `ErrInvalidOperation`.

Both roles set `IsObserver` / `IsWitness` in that replica's own `Config`, and
the two are **mutually exclusive**.

**The voting set is capped at 64**, counting voters plus witnesses plus self,
because acknowledgements are tracked in a 64-bit word.

## Task: remove a replica

Three steps, in this order:

```go
// 1. If it is the leader, move leadership first.
err := host.RequestLeaderTransfer(ctx, shardID, otherReplicaID)

// 2. Remove it from the cluster. This IS a consensus operation.
m, _ := host.SyncGetShardMembership(ctx, shardID)
err = host.SyncRequestDeleteNode(ctx, shardID, replicaID, m.ConfigChangeID)

// 3. On that node: stop the shard, then delete its data.
err = removedHost.StopShard(shardID)
err = removedHost.RemoveData(shardID, replicaID)
```

**Steps 3's two calls do not reach consensus.** They act on the local copy and
`RemoveData` deletes it. `*ShardNotStoppedError` catches running the second
before the first; nothing catches running either before step 2, which leaves a
node that still believes it is a member. It also **retires the replica ID on this host**: `StartShard` refuses it from then on with `*ReplicaRetiredError` (`ErrReplicaRetired`) (`ImportSnapshot` finds no data left to import into and returns `ErrShardNotFound`), because the deleted data held the replica's term, vote and owed lease vote window, and Raft never lets a removed replica return under the same ID. Rejoin under a new replica ID.

## Task: change the witness policy

```go
policy := config.WitnessConfig{
    MaxWitnesses:               2,
    RequireFullReplicaMajority: config.BoolPtr(true),
}
err := host.SyncRequestUpdateWitnessPolicy(ctx, shardID, policy, m.ConfigChangeID)
```

`Config.Witness` is **only the bootstrap seed**. Once the shard is running the
policy is replicated state and changes only through this call — a per-node value
would let replicas disagree about which config changes are legal.

## Task: check who is in the shard

```go
m, err := host.SyncGetShardMembership(ctx, shardID)
// m.Nodes      — voting members
// m.Observers  — non-voting replicas
// m.Witnesses  — voting, log-less replicas
// m.Removed    — replicaIDs that have been removed
// m.ConfigChangeID — pass this as the next configChangeIndex
```

## Troubleshooting

**`*StaleConfigChangeError`** — another change landed between your read and your
write. Re-read `ConfigChangeID` and retry. Do not pass 0 to bypass it; the guard
is what stops two changes applying out of order.

**`ErrTooManyWitnesses`, or a member add that is refused near 64 members** — the
voting set is capped at 64. Voters and witnesses share the cap.

**`ErrInvalidOperation` on a propose or read** — you are talking to a witness.
Witnesses vote and store nothing; they cannot propose and cannot serve a
ReadIndex.

**A newly added node never catches up** — check, in order: was it added *before*
it was started with `join = true`; does its certificate SAN cover the address
you advertised; does the identity you committed for it match the one it reports
from `NodeIdentity()` (`identity_mismatch` in the drop counter).
`host.DiagRemotes(shardID)` shows whether the leader is replicating to it at
all.

**A removed node keeps sending messages** — it has not been restarted and does
not know. Its messages are refused for that shard; the log line naming an
unauthorized member is expected, once. In volume it means the node was never
actually stopped.

**`*NonVotingRoleInMembersError`** — an observer or witness appears in the
voting member map. The roles come from that replica's own `IsObserver` /
`IsWitness`, not from which map it is listed in.

**`*CorruptConfigEntryError` or `*BootstrapEntryMissingError` at startup** — the
log's own membership record at index 1 is unreadable or absent. The shard cannot
start, deliberately: a replica that cannot prove its configuration must not
guess one.

**An observer restarted at a new address and the leader cannot find it** — known
and documented: there is **no self-advertisement**. The leader learns addresses
only from committed config changes, so a replica that moves needs an
operator-issued change. Use a stable advertised `RaftAddress` — the documented
workaround — rather than expecting the cluster to rediscover it.

**Nothing evicts a node that is down** — correct. There is no failure detector
that removes members, deliberately: an automatic eviction during a partition is
how a cluster loses quorum permanently.
