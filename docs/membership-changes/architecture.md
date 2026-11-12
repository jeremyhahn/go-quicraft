# Membership changes: architecture

> **Status: NORMATIVE** · Scope: go-quicraft membership: `pkg/membership.go`,
> the `Host` replica methods, `proto.ConfigChange`, `proto.Membership`,
> `config.WitnessConfig` · Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

## The problem

Changing who is in a Raft group is the part of the algorithm most likely to
produce two leaders. Two replicas that disagree about the member set compute
different quorums, and two different quorums can both elect, which is exactly
the failure consensus exists to prevent.

## The shape chosen

One change at a time, applied on commit, proved by the log.

- **Config-in-log bootstrap.** `StartShard` commits an
  `EntryInitialMembership` (`proto.Entry.Type = 4`, `Cmd` = a marshaled
  `proto.Membership`) at log index 1, so the member set a replica believes in
  is the member set the log proves.
- **One at a time** (PhD §4.1): a single addition or removal per change, so
  successive configurations always overlap and no two can both elect.
- **Applied on commit, not on append**, so a config change that is later
  truncated never took effect. Log truncation therefore needs no membership
  rollback.
- **`configChangeIndex` is a replay guard.** A change names the configuration
  index it expects to follow; a stale one is refused with
  `*StaleConfigChangeError` rather than applied out of order.

### Rejected: joint consensus

Joint consensus (the paper's C-old,new) supports arbitrary configuration
changes in one step. It also needs two quorums live at once, a second
configuration in flight, and a state machine with more states to get wrong.
One-at-a-time reaches the same configurations by composition, at the cost of
more round trips on a rare operation.

### Rejected: membership as out-of-band seed state

A member map handed to each node at startup and never agreed on lets a node be
authorized against a configuration no quorum ratified. Committing it at index 1
makes the log the single source.

### Rejected: per-node witness policy

`MaxWitnesses` and `RequireFullReplicaMajority` are replicated state, carried in
`proto.Membership` through the log, snapshots and the WAL. `Config.Witness` is
only the bootstrap seed. A per-node value would let replicas disagree about
which config changes are legal, the same class of bug as disagreeing about the
member set.

## Three roles, one voting set

| Role | Votes | Stores log data | Serves reads | Proposes |
|---|---|---|---|---|
| Voter (default) | yes | yes | yes | yes |
| Observer (`IsObserver`) | no | yes | yes | yes |
| Witness (`IsWitness`) | **yes** | no | no | no |

The role is replicated state. `IsObserver` only says what a replica starts as:
once the committed membership names the replica, the membership decides. An
observer promoted with `AddNode` becomes a voter in place, without a restart,
as etcd promotes a learner. `IsWitness` cannot change in place, because a
witness has no state machine; a disagreement between it and the membership is
latched and the replica refuses to campaign or serve reads.

The leader's progress view carries all three roles. `ShardInfo.Replicas` lists
every remote member the leader tracks, tagged with a `ReplicaRole`, so a
replacement by learner-then-promote is observable: the embedder watches the
observer's `Match` reach the leader's index, then promotes it. Only voters and
witnesses count toward a quorum, so a caller doing quorum arithmetic over the
list filters on the role.

A witness is a cheap tiebreaker: it takes part in elections and commit quorums
without storing the log. Proposing to one, or asking one for a ReadIndex,
returns `ErrInvalidOperation`.

**Witnesses use a unified quorum.** The same voting set (`remotes + witnesses +
self`) decides elections, commit, ReadIndex confirmation and lease reads. A
witness's heartbeat acknowledgement therefore counts toward the quorum that
confirms a linearizable read, even though the witness could not serve that read
itself.

That is required, not a loophole. The leadership-confirmation quorum has to
intersect every coalition that could win an election, and witnesses vote in
elections. A read quorum that excluded them would not intersect an electing
coalition that included them.

**The voting set is capped at 64** (`raft.MaxVotingMembers`), because
acknowledgements are tracked in a 64-bit word. `ErrTooManyVotingMembers` is the
refusal.

## Identity binding

A replica's address is not its identity. `RequestAddNodeWithIdentity` and its
observer and witness siblings commit a node identity alongside the address: the
SPKI pin the transport derives from the mTLS leaf.

```
"spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))
```

Inbound messages are checked against that committed binding, and the address is
never an authorization input. A sender the configuration names but holds no
identity for is dropped (`missing_identity`). Admission requires an identity on
every host with a transport, so only a shard on a transportless host can hold
such a configuration.

The pin is an SPKI hash rather than a SPIFFE URI SAN because the certificate
issuers this library is deployed with do not share a SAN naming scheme. A
certificate reissued with a new key changes the identity; renewal on the same
key does not.

## Trust boundary

- **Per-shard authorization is a deny-list**, not an allow-list. A removed
  member's messages, and any connection presenting a removed identity, are
  refused for that shard. Details in [`engineering.md`](engineering.md).
- **A config change is a proposal**, so it inherits the same authorization as
  any other: the sender must be a committed member of the shard.
- **Membership travels inside snapshots**, so importing a snapshot changes who
  this replica believes the cluster is. That is why `ImportSnapshot` is an
  explicit offline operation; see
  [`../snapshots/architecture.md`](../snapshots/architecture.md).
- **`RemoveData` and `StopReplica` are local and destructive.** They do not
  reach consensus; they act on this node's copy. `RemoveData` also retires the
  replica ID on this host: the WAL's `RETIRED` registry (16 bytes per retired
  replica, carried across re-encryption) records it, `LogDB.NodeRemoved`
  reports it, every LogDB writer refuses it, and `StartShard` refuses it with
  `*ReplicaRetiredError`. The deleted data held the replica's term, vote and
  owed vote window, so starting it again could break election safety or a
  leader's read lease. `RemoveData` takes the shard's import lock, which a
  start holds throughout, so the two cannot interleave.

## Component boundaries

- `pkg/membership.go`: the exported `Membership` type.
- `pkg/host.go`: the `RequestAdd*`, `RequestRemoveNode` and
  `RequestUpdateWitnessPolicy` surface and their `Sync*` forms.
- `pkg/proto`: `ConfigChange`, `Membership`, and the `EntryInitialMembership`
  entry type.
- `pkg/internal/raft`: the config-change state machine, the voting set, the
  64-member cap.
- `pkg/internal/registry`: `(shardID, replicaID) → address`, updated as
  membership changes.
- `pkg/internal/transport`: derives and checks node identities.
- `pkg/logdb/waldb`: the retired-replica registry.

Peer discovery and cluster-formation bootstrap live in go-qrdb and must not be
added here. This layer changes an already-formed group.

## What this feature deliberately does not do

- **No peer discovery, no bootstrap orchestration, no quorum validation before
  `StartShard`.** The caller supplies the member map. Discovery (static,
  multicast, DNS SRV, token) and the discover → quorum → `StartShard` sequence
  live in go-qrdb.
- **No automatic membership.** Nothing adds or removes a replica on its own: no
  failure detector that evicts, no autoscaler that joins.
- **No joint consensus**, so no multi-member reconfiguration in one step.
- **No certificate issuance or rotation.** Identity is a pin over a certificate
  someone else issued.
- **No self-advertisement.** An observer or witness that restarts at a new
  address cannot teach the leader its location; that needs an operator-issued
  config change. Use a stable advertised `RaftAddress`.
- **No cross-shard membership.** Each shard's configuration is its own.
