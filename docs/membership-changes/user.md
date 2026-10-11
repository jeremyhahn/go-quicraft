# Membership changes: usage

> **Status: NORMATIVE** · Scope: go-quicraft membership · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

This page covers adding, removing and re-roling replicas in a shard that
already exists. Forming the shard in the first place is `StartShard`; see
[`../multi-group-host/user.md`](../multi-group-host/user.md).

## Before you start

- Send the change to the current leader (`GetLeaderID`).
- Read the current `ConfigChangeID` immediately before the change.
- Collect the new node's identity: the value its host reports from
  `NodeIdentity()`. It is a hash of the certificate's public key, safe to log
  and copy.
- The new node needs a certificate signed by the cluster's CA whose SAN covers
  the address you will advertise for it. See
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

Two rules:

1. **Use the `Sync` read.** `GetShardMembership` reads local state, which may
   be behind, and a stale `ConfigChangeID` guarantees the change is refused.
2. **Pass `ConfigChangeID`.** It is the replay guard. If another change landed
   between your read and your write you get `*StaleConfigChangeError`; re-read
   and retry. Passing 0 skips the guard; don't.

Only one change can be in flight. Adding three replicas is three sequential
changes, each with a fresh `ConfigChangeID`.

## Add a voting member

```go
err := host.SyncRequestAddNodeWithIdentity(ctx, shardID,
    replicaID, "node4.example.com:4001", identity, m.ConfigChangeID)
```

Then start the shard on the new node with `join = true` and an empty member
map, anchored to the replicas it may take its first configuration from (the
leader at least, or every member if leadership may move):

```go
cfg.MemberIdentities = map[uint64]string{leaderReplicaID: leaderIdentity}
err := newHost.StartShard(nil, true, createFn, cfg)
```

Add first, then start. A node started with `join = true` before the cluster
knows about it has nobody to catch up from.

The identity is required whenever the host has a transport. `RequestAddNode`
without one returns `ErrMemberIdentityRequired`, and a joiner started without
an anchor is refused. A member is authorized by its committed identity and by
nothing else, never by its address.

## Add an observer or a witness

```go
// Observer: replicates and serves reads, does not vote.
err := host.SyncRequestAddObserverWithIdentity(ctx, shardID, id, addr, identity, m.ConfigChangeID)

// Witness: votes, stores no log data, cannot propose or serve reads.
err := host.SyncRequestAddWitnessWithIdentity(ctx, shardID, id, addr, identity, m.ConfigChangeID)
```

Choose by what you want:

- An **observer** is a read replica or a warm standby. It costs a full copy of
  the log.
- A **witness** is a tiebreaker. It stores no log, so it is cheap to run, and it
  lets a two-node cluster elect. Proposing to a witness or reading from one
  returns `ErrInvalidOperation`.

Start the new replica with `join = true`, as for a voter, with `IsObserver` or
`IsWitness` set in its own `Config` (never both). Do not list it in a founding
member map: that map seeds the voting set, and `StartShard` refuses with
`*NonVotingRoleInMembersError`.

## Promote an observer to a voter

Once the observer has caught up, add it again as a voting member with the same
replica ID, address and identity:

```go
err := host.SyncRequestAddNodeWithIdentity(ctx, shardID,
    id, addr, identity, m.ConfigChangeID)
```

The running replica becomes a voter as soon as it applies the change: it votes,
can be elected leader, and serves linearizable reads. Do not restart it.
`ShardInfo.IsObserver` on its host turns `false` when the change is applied
there. Restarting it later with `IsObserver` still set is harmless; that bit is
only consulted while the membership does not name the replica.

A witness cannot be promoted; the add is refused with `ErrNodeAlreadyExists`.

### Know when the observer has caught up

The leader's `ShardInfo.Replicas` (from `Host.GetShardInfo`) lists every remote member it tracks, observers
and witnesses included, each with a `Role` (`ReplicaRoleVoter`,
`ReplicaRoleObserver` or `ReplicaRoleWitness`) and its `Match` index. Read it
on the leader, not a follower (a follower's list is empty), and filter on `Role`
wherever you do quorum or health arithmetic: observers and witnesses appear in
the list but only voters and witnesses count toward a quorum. It is published
on the leader's next tick, so the first call after a leader change can be empty.

## Replace a replica: learner first, then promote

To swap a machine out without ever shrinking the voting set below what the
shard needs:

1. Add the new machine as an observer (`SyncRequestAddObserverWithIdentity`) and
   start it with `join = true` and `IsObserver` set.
2. Poll the leader's `ShardInfo.Replicas` until the observer's `Match` reaches
   the leader's last log index.
3. Promote it (the `AddNode` above), then remove the old replica, transferring
   leadership away first if the old replica leads.

Each step is a separate change with a fresh `ConfigChangeID`. Skipping the wait
in step 2 still works, since the observer is promoted in place and catches up
as a voter, but until it has caught up it counts toward every quorum without
holding the log.

The voting set is capped at 64, counting voters, witnesses and self, because
acknowledgements are tracked in a 64-bit word.

## Remove a replica

Three steps, in this order:

```go
// 1. If it is the leader, move leadership first.
err := host.RequestLeaderTransfer(ctx, shardID, otherReplicaID)

// 2. Remove it from the cluster. This is a consensus operation.
m, _ := host.SyncGetShardMembership(ctx, shardID)
err = host.SyncRequestDeleteNode(ctx, shardID, replicaID, m.ConfigChangeID)

// 3. On that node: stop the shard, then delete its data.
err = removedHost.StopShard(shardID)
err = removedHost.RemoveData(shardID, replicaID)
```

Step 3 does not reach consensus. It acts on the local copy, and `RemoveData`
deletes it. `*ShardNotStoppedError` catches deleting before stopping; nothing
catches running step 3 before step 2, which leaves a cluster that still counts
the node as a member.

`RemoveData` also retires the replica ID on that host: `StartShard` refuses it
from then on with `*ReplicaRetiredError`, because the deleted data held the
replica's term, vote and lease vote window. To bring the machine back, rejoin
it under a new replica ID.

## Change the witness policy

```go
policy := config.WitnessConfig{
    MaxWitnesses:               2,
    RequireFullReplicaMajority: config.BoolPtr(true),
}
err := host.SyncRequestUpdateWitnessPolicy(ctx, shardID, policy, m.ConfigChangeID)
```

`Config.Witness` is only the bootstrap seed. Once the shard is running, the
policy is replicated state and changes only through this call, so every
replica agrees on which config changes are legal. The new policy must already
hold for the current membership; reshape the membership first, then tighten.

## Check who is in the shard

```go
m, err := host.SyncGetShardMembership(ctx, shardID)
// m.Nodes              voting members
// m.Observers          non-voting replicas
// m.Witnesses          voting, log-less replicas
// m.Removed            replica IDs that have been removed
// m.Identities         committed node identity per replica
// m.RemovedIdentities  identities banned from the shard
// m.ConfigChangeID     pass this as the next configChangeIndex
```

## Troubleshooting

**`*StaleConfigChangeError`**: another change landed between your read and
your write. Re-read `ConfigChangeID` and retry.

**`ErrRejected` on a config change**: an earlier change is still uncommitted.
Retry once it commits.

**`ErrTooManyVotingMembers`**: the voting set is capped at 64, shared by voters
and witnesses. **`ErrTooManyWitnesses`** is the shard's own `MaxWitnesses`
policy.

**`ErrInvalidOperation` on a propose or read**: you are talking to a witness.

**A newly added node never catches up.** Check, in order: was it added before
it was started with `join = true`; does its certificate SAN cover the address
you advertised; does the identity you committed for it match the one it
reports from `NodeIdentity()` (an `identity_mismatch` reason on the
unauthorized-drop counter says it does not). `host.DiagRemotes(shardID)` on
the leader shows whether it is replicating to the node at all.

**A removed node keeps sending messages.** It has not been stopped and does
not know it was removed. Its messages are refused and counted under
`removed_replica`, with a rate-limited warning. A steady stream means the node
was never stopped.

**`*NonVotingRoleInMembersError`**: the replica is configured as an observer or
witness and is also listed in the founding member map. Start it with
`join = true` and add it through a config change instead.

**`ErrRemovedMemberIdentity`**: the identity you are adding belongs to a
replica this shard removed. The ban is on the keypair, under any replica ID,
and is permanent; `Membership.RemovedIdentities` lists every banned identity.
Give the node a new keypair. Reusing its address under a new replica ID is
fine.

**`*CorruptConfigEntryError` or `*BootstrapEntryMissingError` at startup**: the
log's own membership record at index 1 is unreadable or absent. The shard
refuses to start, because a replica that cannot prove its configuration must
not guess one.

**An observer restarted at a new address and the leader cannot find it**: there
is no self-advertisement. The leader learns addresses only from committed
config changes, so a replica that moves needs an operator-issued change. Use a
stable advertised `RaftAddress`.

**Nothing evicts a node that is down.** That is deliberate: an automatic
eviction during a partition is how a cluster loses quorum permanently.
