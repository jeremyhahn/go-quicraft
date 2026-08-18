# Membership Changes — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft membership: `pkg/membership.go`,
> the `Host` replica methods, `proto.ConfigChange`, `proto.Membership`,
> `config.WitnessConfig` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## The problem

Changing who is in a Raft group is the part of the algorithm most likely to
produce two leaders. Two replicas that disagree about the member set compute
different quorums, and two different quorums can both elect — which is exactly
the failure consensus exists to prevent.

## The shape chosen

**One change at a time, applied on commit, proved by the log.**

- **Config-in-log bootstrap.** `StartShard` commits an
  `EntryInitialMembership` (`proto.Entry.Type = 4`, `Cmd` = a marshaled
  `proto.Membership`) at **log index 1**, so the member set a replica believes
  in is the member set the log proves. Before this, a joining node could be
  authorized against a locally seeded map that no quorum had agreed to.
- **One-at-a-time** (PhD §4.1): a single addition or removal per change, so
  successive configurations always overlap and no two can both elect.
- **Applied on commit, not on append**, so a config change that is later
  truncated never took effect. This is why log truncation needs no membership
  rollback.
- **`configChangeIndex` is a replay guard**: a change names the index it
  expects to follow, and a stale one is refused with
  `*StaleConfigChangeError` rather than applied out of order.

### Rejected: joint consensus

Joint consensus (the paper's C-old,new) supports arbitrary configuration
changes in one step. It also requires two quorums live simultaneously, a second
configuration in flight, and a state machine with more states to get wrong.
One-at-a-time gets the same reachable configurations by composition, in
exchange for more round trips on a rare operation.

### Rejected: membership as out-of-band seed state

A member map handed to each node at startup and never agreed on is the design
where a node can be authorized against a configuration no quorum ratified.
Committing it at index 1 makes the log the single source.

### Rejected: per-node witness policy

`MaxWitnesses` and `RequireFullReplicaMajority` are **replicated state**,
carried in `proto.Membership` through the log, snapshots and the WAL.
`Config.Witness` is only the bootstrap seed. A per-node value would let replicas
disagree about which config changes are legal — the same class of bug as
disagreeing about the member set.

## Three roles, one voting set

| Role | Votes | Stores log data | Serves reads | Proposes |
|---|---|---|---|---|
| **Voter** (default) | yes | yes | yes | yes |
| **Observer** (`IsObserver`) | no | yes | yes | yes |
| **Witness** (`IsWitness`) | **yes** | **no** | **no** | **no** |

A witness is a tiebreaker that costs almost nothing to run: it participates in
elections and in commit quorums without storing the log. Both proposing and
serving a ReadIndex return `ErrInvalidOperation` on one.

**Witnesses use a unified quorum.** The same voting set — `remotes + witnesses +
self` — decides elections, commit, ReadIndex confirmation and lease reads. A
witness's heartbeat acknowledgement therefore counts toward the quorum that
confirms a linearizable read **even though the witness could not serve that read
itself**.

That looks wrong and is not. The requirement is **overlap**: the
leadership-confirmation quorum has to intersect every coalition that could win
an election, and witnesses vote in elections. A separate read quorum that
excluded them would not intersect an electing coalition that included them.

**The voting set is capped at 64** (`raft.MaxVotingMembers`), because
acknowledgements are tracked in a 64-bit word. `ErrTooManyWitnesses` and the
member-count validations enforce it.

## Identity binding

A replica's address is not its identity. `RequestAddNodeWithIdentity` and its
siblings commit a **node identity** alongside the address — the SPKI pin the
transport derives from the mTLS leaf:

```
"spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))
```

Inbound messages are checked against that committed binding. Where no identity
is committed for a sender, the fallback is a SAN check against the committed
Raft address; `TransportConfig.RequireMemberIdentity` (default `false`) drops
such messages instead.

The SPKI pin is deliberately not a SPIFFE URI SAN, because four downstream
products issue these certificates and none of them agree on a naming scheme. A
certificate reissued with a **new key** changes the identity; reissuing with the
same key does not.

## Trust boundary

- **Per-shard authorization is a deny-list**, not an allow-list. A removed
  member's messages are refused for that shard; the details are in
  [`engineering.md`](engineering.md).
- **A config change is a proposal**, so it inherits the same authorization as
  any other: the sender must be a committed member of the shard.
- **Membership travels inside snapshots**, so importing a snapshot changes who
  this replica believes the cluster is. That is why `ImportSnapshot` is an
  explicit offline operation — see
  [`../snapshots/architecture.md`](../snapshots/architecture.md).
- **`RemoveData` and `StopReplica` are local and destructive.** They do not
  reach consensus; they act on this node's copy.

## Component boundaries

- **`pkg/membership.go`** — the exported `Membership` type.
- **`pkg/host.go`** — the `RequestAdd*` / `RequestRemoveNode` /
  `RequestUpdateWitnessPolicy` surface and their `Sync*` forms.
- **`pkg/proto`** — `ConfigChange`, `Membership`, and the
  `EntryInitialMembership` entry type.
- **`pkg/internal/raft`** — the config-change state machine, the voting set, the
  64-member cap.
- **`pkg/internal/registry`** — `(shardID, replicaID) → address`, updated as
  membership changes.
- **`pkg/internal/transport`** — derives and checks node identities.

Peer discovery and cluster-formation bootstrap are **not here**: they were
relocated to go-qrdb and must not be added back. This layer changes an
already-formed group.

This feature is part of the go-quicraft capability row covering Raft consensus
and log replication in `docs/00 §R`.

## What this feature deliberately does not do

- **No peer discovery, no bootstrap orchestration, no quorum validation before
  `StartShard`.** The caller supplies the member map. Discovery (static,
  multicast, DNS SRV, token) and the discover → quorum → `StartShard` sequence
  live in go-qrdb.
- **No automatic membership.** Nothing adds or removes a replica on its own —
  no failure detector that evicts, no autoscaler that joins. A node that is down
  is a node that is down.
- **No joint consensus**, so no arbitrary multi-member reconfiguration in one
  step.
- **No certificate issuance or rotation.** Identity is a pin over a certificate
  someone else issued.
- **No self-advertisement.** An observer or witness that restarts at a **new**
  address cannot teach the leader its location; that needs an operator-issued
  config change. The documented workaround is a stable advertised
  `RaftAddress`. Periodic self-advertisement is recorded as a planned
  post-v1.0 enhancement, not present today.
- **No cross-shard membership.** Each shard's configuration is its own.
