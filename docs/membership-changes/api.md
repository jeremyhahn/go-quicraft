# Membership Changes — API

> **Status: NORMATIVE** · Scope: go-quicraft membership · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API.**

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the `Host` methods below. This is the transport. |
| **QUIC** | a committed `proto.ConfigChange` replicates like any other entry, and `proto.Membership` travels inside snapshots. There is no membership RPC. |
| Unix socket / REST / gRPC / MCP | n/a — this repository serves none of them |

Not missing-capability rows. This feature is part of the go-quicraft Raft row,
which the portfolio register marks as consumed by implementing interfaces rather
than by calling a client.

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg`

### Capability: add a voting member

```go
func (h *Host) RequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) error
```

| | |
|---|---|
| Request | the shard, the new replica id, its **advertised** address, optionally its committed node identity, and the `configChangeIndex` this change expects to follow |
| Response | a `*RequestState`, or nothing from the `Sync` form |
| Errors | `*StaleConfigChangeError` (the guard fired — re-read and retry), `*InvalidMemberError`, `*EmptyMembersError`, `*SelfNotInMembersError`, `ErrTooManyWitnesses`, `NotLeaderError`, `ErrBusy` |
| Permission | none in this library; the caller must be talking to the leader |

`identity` is the SPKI pin the transport derives from the peer's mTLS leaf:
`"spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))`, the value the node
reports from `Host.NodeIdentity()`. **It is required** on a host with a
transport: the identity-less forms return `ErrMemberIdentityRequired`, and a
member is authorized by its committed identity and by nothing else.

### Capability: add an observer or a witness

```go
func (h *Host) RequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddObserverWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
```

| Role | Votes | Stores log | Serves reads | Proposes |
|---|---|---|---|---|
| observer | no | yes | yes | yes |
| witness | **yes** | **no** | **no** — `ErrInvalidOperation` | **no** — `ErrInvalidOperation` |

The voting set (`remotes + witnesses + self`) is capped at **64**;
`ErrTooManyWitnesses` and the member validations enforce it.

### Capability: remove a replica

```go
func (h *Host) RequestRemoveNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestDeleteNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) error
```

Removal is a consensus operation. Afterwards, the removed replica's messages are
refused for that shard.

### Capability: change the witness policy

```go
func (h *Host) RequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) error

type config.WitnessConfig struct {
    MaxWitnesses               int
    RequireFullReplicaMajority *bool
}
```

Proposes a `ConfigChange` of type `proto.UpdateWitnessPolicy`. **The policy is
replicated state**; `Config.Witness` is only the bootstrap seed.

### Capability: read the current membership

```go
func (h *Host) GetShardMembership(shardID uint64) (*Membership, error)
func (h *Host) SyncGetShardMembership(ctx context.Context, shardID uint64) (*Membership, error)
func NewMembership() *Membership

type Membership struct {
    ConfigChangeID uint64                // the log index that produced this
    Nodes          map[uint64]string     // voting members
    Observers      map[uint64]string
    Witnesses      map[uint64]string
    Removed        map[uint64]struct{}
    // ... plus the replicated session policy and identity maps
}
```

**`ConfigChangeID` is what you pass as the next `configChangeIndex`.** The
`Sync` form confirms through consensus; the plain form reads local state, which
may be behind.

### Capability: local, destructive replica operations

```go
func (h *Host) StopReplica(shardID, replicaID uint64) error
func (h *Host) RemoveData(shardID, replicaID uint64) error
func (h *Host) SyncRemoveData(ctx context.Context, shardID, replicaID uint64) error
func (h *Host) HasNodeInfo(shardID, replicaID uint64) bool
```

**These do not reach consensus.** They act on this node's own copy, and
`RemoveData` deletes it. `*ShardNotStoppedError` is returned if the shard is
still running. Remove from the cluster first, then remove the data. It also **retires the replica ID on this host**: `StartShard` refuses it from then on with `*ReplicaRetiredError` (`ErrReplicaRetired`) (`ImportSnapshot` finds no data left to import into and returns `ErrShardNotFound`), because the deleted data held the replica's term, vote and owed lease vote window, and Raft never lets a removed replica return under the same ID. Rejoin under a new replica ID.

### Capability: transfer leadership

```go
func (h *Host) RequestLeaderTransfer(ctx context.Context, shardID, targetReplicaID uint64) error
```

Not a membership change, but the operation you run before removing a leader.

### Errors

| Error | Meaning |
|---|---|
| `*StaleConfigChangeError` | the `configChangeIndex` did not match; another change landed first. Re-read `Membership.ConfigChangeID` and retry. |
| `*InvalidMemberError` | the member specification is not usable |
| `*EmptyMembersError` | an empty member map |
| `*SelfNotInMembersError` | this replica is not in the map it was given |
| `*NonVotingRoleInMembersError` / `ErrNonVotingRoleInMembers` | an observer or witness appears in the voting map |
| `ErrTooManyWitnesses` | the 64-voting-member cap |
| `ErrRemovedMemberIdentity` | the add binds a node identity this shard has removed, under any replica ID; see `Membership.RemovedIdentities` |
| `*ReplicaNotFoundError` | no such replica in this shard |
| `*ShardNotStoppedError` | a local data operation on a running shard |
| `*CorruptConfigEntryError`, `*BootstrapEntryMissingError` | the log's own membership record is unreadable or absent — the shard cannot start |
| `*UnauthorizedMessageError` | a message from a sender not authorized for the shard |

### Wire types

```go
proto.ConfigChange                 // Type: AddNode | AddObserver | AddWitness |
                                   //       RemoveNode | UpdateWitnessPolicy
proto.Membership                   // replicated configuration
const proto.EntryInitialMembership uint64 = 4
const proto.MaxNodeIdentityLength  = 256
func proto.ConfigChangeTypeName(t uint64) string
```

## Configuration

| Field | Owner | Default | Note |
|---|---|---|---|
| `OrderedConfigChange` | `config.Config` | `*bool`, nil = **enabled** | serialize membership changes |
| `IsObserver` / `IsWitness` | `config.Config` | false | **mutually exclusive**. `IsObserver` is an initial hint: the committed membership decides once it names the replica |
| `Witness` | `config.Config` | `WitnessConfig{}` | bootstrap seed only |
| `MemberIdentities` | `config.Config` | `nil` | required on a host with a transport: every founder's members (join=false), at least one anchor replica (join=true); entries beyond the members map are the operator anchor the receive gate consults for a sender no configuration names |

## Versioning and deprecation

- `proto.Membership` carries a **persisted-scoped** version byte
  (`membershipFormatVersion` = 1), a separate namespace from
  `proto.WireVersion`, gated on exact equality. It is stamped into the WAL,
  snapshot records, exported artifacts, and the `EntryInitialMembership` entry
  at log index 1 — bytes that outlive the process that wrote them. See
  [`../wire-protocol/architecture.md`](../wire-protocol/architecture.md).
- The WAL has a **third** discriminator on the same layout,
  `membershipSectionVersion`.
- `RequireMemberIdentity` was removed in 0.13.0-alpha (#197): identities are
  required at admission and there is no address fallback — see
  [`../compatibility.md`](../compatibility.md).
- Nothing here is deprecated.
