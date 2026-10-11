# Membership changes: API

> **Status: NORMATIVE** · Scope: go-quicraft membership · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. The Go API is the API.

| Transport | This feature |
|---|---|
| Embedded (Go call) | the `Host` methods below |
| QUIC | a committed `proto.ConfigChange` replicates like any other entry, and `proto.Membership` travels inside snapshots. There is no membership RPC. |
| Unix socket / REST / gRPC / MCP | n/a: this repository serves none of them |

These are not missing-capability rows. The feature is part of the go-quicraft
Raft row, which the portfolio register marks as consumed by implementing
interfaces rather than by calling a client.

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg` (package `quicraft`).

Every change below is proposed to the shard's current leader and takes a
`configChangeIndex`: pass `Membership.ConfigChangeID` read immediately before
the call. A non-zero value that does not match the shard's current
configuration is refused with `*StaleConfigChangeError`; 0 skips the check.
`Sync*` forms block until the change commits and need a context with a
deadline.

### Capability: add a voting member

```go
func (h *Host) RequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) error
```

| | |
|---|---|
| Request | the shard, the new replica ID, its advertised address, its node identity, and the `configChangeIndex` |
| Response | a `*RequestState`, or only an error from the `Sync` form |
| Errors | see [Errors](#errors); before validation also `*NotLeaderError`, `ErrSystemBusy` (global rate limiter), `ErrShardNotReady`, `ErrDraining`, `ErrClosed` |
| Permission | none in this library; the caller must be talking to the leader |

`identity` is the SPKI pin the transport derives from the peer's mTLS leaf,
`"spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))`, which the node reports
from `Host.NodeIdentity()`. It is required on a host with a transport: the
identity-less forms, or an empty `identity`, return `ErrMemberIdentityRequired`
unless the configuration already holds an identity for the replica (a role
change such as promotion). The identity-less forms exist for transportless
hosts.

`AddNode` on an existing observer promotes it to a voter in place.

### Capability: add an observer or a witness

```go
func (h *Host) RequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddObserverWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestAddObserverWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) error

func (h *Host) RequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) error
```

| Role | Votes | Stores log | Serves reads | Proposes |
|---|---|---|---|---|
| observer | no | yes | yes | yes |
| witness | yes | no | no (`ErrInvalidOperation`) | no (`ErrInvalidOperation`) |

The identity rules are the same as for `AddNode`. The voting set
(`voters + witnesses + self`) is capped at 64 (`ErrTooManyVotingMembers`).

The leader reports the new member's catch-up through `Host.GetShardInfo`:
`ShardInfo.Replicas` holds a `ReplicaProgress` for every remote member it
tracks, observers and witnesses included, and `ReplicaProgress.Role` is one of
`ReplicaRoleVoter`, `ReplicaRoleObserver` or `ReplicaRoleWitness`. An embedder
replacing a replica by learner-then-promote waits for the observer's `Match` to
reach the leader's index before the promoting `AddNode`. `Replicas` is empty on
a follower and for the first call on a shard; the full type is in
[`../multi-group-host/api.md`](../multi-group-host/api.md#replicaprogress).
`Host.DiagRemotes` renders the same state as text with a `role=` field.

### Capability: remove a replica

```go
func (h *Host) RequestRemoveNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestDeleteNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) error
```

Removal is a consensus operation. Once it commits, the removed replica's
messages, and any connection presenting its committed identity, are refused for
that shard.

### Capability: change the witness policy

```go
func (h *Host) RequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) error

type config.WitnessConfig struct {
    MaxWitnesses               int   // 0 = no cap; negative is refused with *HostInitError
    RequireFullReplicaMajority *bool // nil = true
}
```

Proposes a `ConfigChange` of type `proto.UpdateWitnessPolicy`. The policy is
replicated state; `Config.Witness` is only the bootstrap seed. The new policy
must already hold for the current membership, or the change is refused with
`ErrTooManyWitnesses` or `ErrBreaksFullReplicaMajority`: reshape first, then
tighten.

### Capability: read the current membership

```go
func (h *Host) GetShardMembership(shardID uint64) (*Membership, error)
func (h *Host) SyncGetShardMembership(ctx context.Context, shardID uint64) (*Membership, error)
func NewMembership() *Membership
func (m *Membership) ConfigChangeIndex() uint64 // alias for ConfigChangeID

type Membership struct {
    ConfigChangeID    uint64              // log index of the change that produced this
    Nodes             map[uint64]string   // voting members
    Observers         map[uint64]string
    Witnesses         map[uint64]string
    Removed           map[uint64]struct{}
    Identities        map[uint64]string   // committed node identity per replica
    RemovedIdentities map[uint64]string   // banned identities, never evicted
    // plus the replicated witness policy (MaxWitnesses,
    // RequireFullReplicaMajority) and session policy fields
}
```

`ConfigChangeID` is what you pass as the next `configChangeIndex`. The `Sync`
form confirms through consensus; the plain form reads local state, which may be
behind.

### Capability: local, destructive replica operations

```go
func (h *Host) StopReplica(shardID, replicaID uint64) error
func (h *Host) RemoveData(shardID, replicaID uint64) error
func (h *Host) SyncRemoveData(ctx context.Context, shardID, replicaID uint64) error
func (h *Host) HasNodeInfo(shardID, replicaID uint64) bool
```

These do not reach consensus. They act on this node's own copy, and
`RemoveData` deletes it. Remove the replica from the cluster first, then stop
it, then remove its data.

- `RemoveData` on a running replica returns `*ShardNotStoppedError`; while a
  start or import holds the shard it returns `ErrShardNotReady` or
  `ErrImportInProgress`.
- `RemoveData` retires the replica ID on this host. From then on `StartShard`
  refuses it with `*ReplicaRetiredError` (matches `ErrReplicaRetired`), and
  `ImportSnapshot` finds no data to import into (`ErrShardNotFound`). The
  deleted data held the replica's term, vote and owed lease vote window, and
  Raft never lets a removed replica return under the same ID. Rejoin under a
  new replica ID.
- `StopReplica` returns `*ReplicaNotFoundError` when this host runs a different
  replica of the shard.

### Capability: transfer leadership

```go
func (h *Host) RequestLeaderTransfer(ctx context.Context, shardID, targetReplicaID uint64) error
```

Not a membership change, but the operation you run before removing a leader.

### Errors

All are exported from `quicraft` and match with `errors.Is` / `errors.As`.

| Error | Meaning |
|---|---|
| `*StaleConfigChangeError` (`ErrStaleConfigChange`) | the `configChangeIndex` did not match; another change landed first. Re-read `Membership.ConfigChangeID` and retry. |
| `ErrRejected` | another config change is still uncommitted. Retry once it commits. |
| `ErrMemberIdentityRequired` | an add without a node identity on a host with a transport |
| `ErrRemovedMemberIdentity` | the add binds an identity this shard has removed, under any replica ID; see `Membership.RemovedIdentities` |
| `ErrDuplicateMemberAddress` | the add names a new replica at an address another active member holds |
| `ErrDuplicateMemberIdentity` | the add binds an identity another active member holds |
| `ErrSharedPeerCertificate` / `*SharedPeerCertificateError` | this host's own certificate also covers the added replica's host |
| `ErrNodeAlreadyExists` | the target already holds the requested role, or is a witness (a witness cannot be promoted) |
| `ErrNodeRemoved` | the replica ID was removed from this shard; IDs are never reused |
| `ErrNodeNotFound` | a removal targets a replica that is not a member |
| `ErrRemoveLastFullReplica`, `ErrRemoveLastVoter` | the removal would leave the shard unable to elect |
| `ErrTooManyVotingMembers` | the 64-voting-member cap |
| `ErrTooManyWitnesses` | the replicated `MaxWitnesses` cap |
| `ErrBreaksFullReplicaMajority` | the change violates `RequireFullReplicaMajority` |
| `ErrZeroReplicaID`, `ErrEmptyAddress` | malformed request |
| `ErrReplicaRetired` / `*ReplicaRetiredError` | `StartShard` for a replica ID `RemoveData` retired on this host |
| `*ReplicaNotFoundError` | no such replica of this shard on this host |
| `*ShardNotStoppedError` | a local data operation on a running replica |
| `*EmptyMembersError`, `*SelfNotInMembersError`, `*InvalidMemberError` | `StartShard` was given an unusable founding member map |
| `*NonVotingRoleInMembersError` (`ErrNonVotingRoleInMembers`) | `StartShard` for a replica configured as observer or witness that is also listed in the founding member map |
| `*CorruptConfigEntryError`, `*BootstrapEntryMissingError` | the log's own membership record is unreadable or absent; the shard cannot start |
| `*UnauthorizedMessageError` | an inbound snapshot transfer from a sender not authorized for the shard. Ordinary Raft messages from such a sender are dropped and counted, not returned. |

### Wire types

```go
proto.ConfigChange                 // Type: AddNode | RemoveNode | AddNonVoting |
                                   //       AddWitness | UpdateWitnessPolicy
proto.Membership                   // replicated configuration
const proto.EntryInitialMembership uint64 = 4
const proto.MaxNodeIdentityLength = 256
func proto.ConfigChangeTypeName(t uint64) string
```

## Configuration

Per shard, on `config.Config`.

| Field | Default | Note |
|---|---|---|
| `OrderedConfigChange` | `*bool`, nil = enabled | serialize membership changes |
| `IsObserver` / `IsWitness` | false | mutually exclusive. `IsObserver` is an initial hint: the committed membership decides once it names the replica. `IsWitness` is binding. |
| `Witness` | `WitnessConfig{}` | bootstrap seed only |
| `MemberIdentities` | nil | required on a host with a transport: every founder's members (`join=false`), at least one anchor replica (`join=true`). Entries beyond the members map are the operator anchor the receive gate consults for a sender no configuration names. |

`Config.RequireMemberIdentities` is set by the Host from whether it has a
transport; whatever the caller puts there is overwritten.

## Versioning and deprecation

- `proto.Membership` carries a persisted-scoped version byte
  (`membershipFormatVersion` = 1), a separate namespace from
  `proto.WireVersion`, gated on exact equality. It is stamped into the WAL,
  snapshot records, exported artifacts, and the `EntryInitialMembership` entry
  at log index 1. See
  [`../wire-protocol/architecture.md`](../wire-protocol/architecture.md).
- The WAL has a third discriminator on the same layout,
  `membershipSectionVersion`.
- Identities are required at admission and there is no address fallback; see
  [`../compatibility.md`](../compatibility.md).
- Nothing here is deprecated.
