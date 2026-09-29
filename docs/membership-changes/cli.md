# Membership Changes — CLI

> **Status: NORMATIVE** · Scope: go-quicraft membership · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable.

This feature contributes **no** `CommandDefinition` to any integration parity
suite. The reason is sharper here than elsewhere: a membership change needs a
`configChangeIndex` read from the **current** membership, and it must be sent to
the **current leader**. A command line that guesses either is a command line
that produces `*StaleConfigChangeError` most of the time and a wrong
configuration the rest of it.

## Where the operator verbs live

Cluster membership as an operator experiences it — join a node, evict one,
rebalance — belongs to the layer that owns cluster formation:

| Owner | Responsibility |
|---|---|
| **go-qrdb** | peer discovery (static, multicast, DNS SRV, token), the discover → quorum → `StartShard` bootstrap sequence, and opt-in dynamic membership |
| **go-xkms** | the CA that issues the certificates whose SPKI pins become node identities |
| **go-trusted-platform** | re-exports the composed stack's verbs |

Peer discovery and cluster-formation bootstrap were **relocated out of this
repository** and must not be added back. This layer changes an already-formed
group; something else decides what the group should be.

## Build-time targets

| Target | Runs |
|---|---|
| `make test-raft` | the config-change state machine |
| `make integration-test-raft` | membership changes across a real cluster |
| `make ci-netfault` | a config change that commits during a partition |

## Operational levers that are not verbs

| Lever | Where | Note |
|---|---|---|
| `OrderedConfigChange` | `config.Config` | `*bool`, nil = **enabled**. Leave it. |
| `IsObserver` / `IsWitness` | `config.Config` | mutually exclusive; set at `StartShard` |
| `Witness` (`MaxWitnesses`, `RequireFullReplicaMajority`) | `config.Config` | **bootstrap seed only** — change it live through `RequestUpdateWitnessPolicy` |
| `MemberIdentities` | `config.Config` | required on a host with a transport: every founder's members, or a joiner's anchor |

## The two calls an operator procedure is actually built from

```go
m, err := host.SyncGetShardMembership(ctx, shardID)   // confirmed through consensus
err = host.SyncRequestAddNodeWithIdentity(ctx, shardID, replicaID, addr, identity, m.ConfigChangeID)
```

Always read `ConfigChangeID` immediately before the change and pass it. That is
the replay guard, and `*StaleConfigChangeError` means another change landed
first — re-read and retry rather than forcing.

Use the `Sync` form of the read. The plain `GetShardMembership` reads local
state, which may be behind, and a stale `ConfigChangeID` guarantees the change
is refused.

## The dangerous pair

```go
host.StopReplica(shardID, replicaID)
host.RemoveData(shardID, replicaID)
```

**Neither reaches consensus.** They act on this node's own copy and
`RemoveData` deletes it. Remove the replica from the cluster through
`RequestRemoveNode` first, then stop it, then remove its data — in that order.
`*ShardNotStoppedError` catches the middle mistake; nothing catches the first.
It also **retires the replica ID on this host**: `StartShard` refuses it from then on with `*ReplicaRetiredError` (`ErrReplicaRetired`) (`ImportSnapshot` finds no data left to import into and returns `ErrShardNotFound`), because the deleted data held the replica's term, vote and owed lease vote window, and Raft never lets a removed replica return under the same ID. Rejoin under a new replica ID.
