# Membership changes: engineering contract

> **Status: NORMATIVE** · Scope: go-quicraft membership: `pkg/membership.go`,
> the `Host` replica methods, `proto.ConfigChange`, `proto.Membership`,
> `config.WitnessConfig`, and the membership half of `pkg/internal/raft` ·
> Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

Design rationale, the role model and the scope fence are in
[`architecture.md`](architecture.md). The Go call surface is in
[`api.md`](api.md). The Raft protocol around it is in
[`../raft-consensus/engineering.md`](../raft-consensus/engineering.md).

## Config-in-log bootstrap

A shard's initial configuration is a single committed entry at index 1
(`proto.EntryInitialMembership`, built by `BuildInitialMembershipEntry`), not
an out-of-band seed. Membership is therefore a pure fold over the log, which is
the property the whole design turns on: a joining replica learns the full
configuration by ordinary replication rather than only from a snapshot that may
never arrive. `BootstrapMembershipIndex` is 1 and
`bootstrapMembershipTerm` is 1, so the first real proposal (and the new
leader's no-op) land at index 2 or above.

One entry carrying the full `Membership` is used rather than one ConfigChange
per member, so the bootstrap occupies exactly index 1 and perturbs the index
arithmetic the recovery clamps depend on as little as possible.
`proto.Membership` marshals its maps with sorted keys, so every founder
produces a byte-identical entry from identical input. That last qualifier is an
unenforced operator contract: `MaxWitnesses` and `RequireFullReplicaMajority`
come from each founder's local `Config.Witness`, so founders configured with
different witness policy (or different member addresses) produce byte-DIFFERENT
entries at the same `(index=1, term=1)`, AppendEntries matches on
`(index, term)` only, and the divergence is never reconciled.

`applyMembershipEntry` refuses an `EntryInitialMembership` at any index other
than 1. Applying one elsewhere would REPLACE the membership wholesale,
discarding every config change committed since bootstrap. The refusal is a pure
function of the entry's index, so every replica reaches it identically.

`ReconstructMembership` (`bootstrap.go`) is the recovery-time fold of the same
entries, and it makes the identical decisions, deliberately: a committed
config-carrying entry that does not decode is FATAL on both paths
(`ErrCorruptConfigEntry`), because skipping one reconstructs a membership that
differs from the rest of the shard. A partial fold is not a degraded
membership, it is a different one. A zero-length `EntryConfigChange` payload is
a silent no-op on both paths: every replica reads the same nothing and skips
identically.

## One-at-a-time configuration (PhD 4.1, Paper Section 6)

Only one config change may be in flight at a time. Proposing a second while one
is uncommitted returns `ErrPendingConfigChange`, which the Host reports as
`quicraft.ErrRejected` (retryable: nothing entered the log). This prevents two
overlapping config change entries, which could produce two disjoint majorities.

The authoritative test is `raftLog.confChangeIndex > raftLog.processed`
(`hasConfigChangeToApply`), and `r.pendingConfigChange` is a cached view of it
that `handleLeaderPropose` re-derives UNCONDITIONALLY on every proposal. A
guarded form (`if r.pendingConfigChange`) could only relax the flag, never
restore it: `Peer.Commit` clears it for every committed `EntryConfigChange` it
applies without asking whether a later one is still unapplied, so a catch-up
batch containing an earlier change would clear it while one was still
outstanding.

The frontier is `processed`, NOT `committed`. Membership takes effect at commit
time, but "commit time" here means `Peer.Commit -> processConfigChanges`, a
stage BEHIND `r.log.committed`: the log's committed index advances in the step
worker while the commit/apply cycle is still in flight, and `becomeLeader` runs
inside that window. A test bounded at `committed` would report "nothing
pending" for a config change that has committed but has not been folded into
`p.membership` yet, so a new leader would arm `pendingConfigChange = false`,
accept a second config change, and compute its commit quorum over the OLD
voting set while the log's newest configuration is a larger one. This is
etcd/raft's `applied >= pendingConfIndex` shape.

`confChangeIndex` is maintained incrementally by `raftLog.append`, which is the
single path every entry enters the log through (leader propose, follower append
via `tryAppend`, the leadership no-op). `newRaftLog` seeds it CONSERVATIVELY
with the recovered last index, because nothing in this process classified the
recovered entries; that blocks new config-change proposals on a just-restarted
leader until it has applied the tail it started with, and releases
automatically. An O(N) log scan was rejected because it can fail to read, and a
failed read has to be treated as "a change might be pending".

## A config change takes effect on commit

Membership changes take effect when the config change entry COMMITS, not when
it is appended (the etcd/dragonboat model, diverging from PhD 4.1's append-time
rule). The single application point is
`Peer.Commit -> processConfigChanges -> applyMembershipEntry`: when an update's
`CommittedEntries` contain config-carrying entries, they are applied to the
membership, the immutable snapshot is republished, and `syncRaftRemotes()`
adds and removes remote trackers, identically on leaders and followers.

Commit-time application avoids the append-time trap where a just-added
(possibly not yet running) member immediately counts toward the quorum required
to commit its own addition, stalling commit and stepping the leader down via
CheckQuorum.

`Peer.ProposeConfigChange` does NOT apply the change to membership. It
validates, serializes to an `EntryConfigChange`, and steps a `Propose`; the
membership mutation happens later, on commit, on every replica alike.

## Log truncation needs no membership rollback

Because membership only mutates on COMMITTED entries and committed entries are
never truncated, conflict-driven log truncation cannot remove an applied config
change. No rollback or log-rescan machinery exists; an appended-but-uncommitted
config change that gets truncated was never applied in the first place.

## Dispatch tables

Config change handling uses map-based dispatch (`membership.go`), with a
mutating handler table and a read-only validator table:

```go
var configChangeHandlers = map[uint64]configChangeHandler{
    proto.AddNode:             handleAddNode,
    proto.RemoveNode:          handleRemoveNode,
    proto.AddNonVoting:        handleAddNonVoting,
    proto.AddWitness:          handleAddWitness,
    proto.UpdateWitnessPolicy: handleUpdateWitnessPolicy,
}
```

`configChangeValidators` mirrors it one-for-one. An unknown type is
`ErrInvalidConfigChangeType`. Every type except `UpdateWitnessPolicy` must name
a non-zero target replica (`ErrZeroReplicaID`); `UpdateWitnessPolicy` targets
the shard.

The handlers update `addresses`, `observers`, `witnesses`, `removed`,
`identities`, `removedIdentities`, the two witness-policy fields, and
`configChangeID`.

## Replay guard

`membership.apply(cc, logIndex)` skips any change at or below
`m.configChangeID`, which is the log index of the most recently applied config
change. This covers restart replay of committed entries on top of a
snapshot-seeded membership and a follower re-receiving entries. Re-applying
would be wrong for non-idempotent changes (a replayed `UpdateWitnessPolicy`
would revert a newer policy) and would regress `configChangeID`. The skip is
deterministic across replicas because `configChangeID` is itself replicated
state.

The initial membership's `ConfigChangeID` is left at 0, so a caller observes 0
until the first real membership change.

## Node types

**Voters (Remotes)**: full replicas that replicate the entire log and
participate in elections and the commit quorum.

**Observers (Non-Voting Learners)**: replicate the full log but do not vote.
Used to bring new nodes up to speed before promotion. They receive no
RequestVote, get no ReadIndex ack bit, and are never counted in `quorum()`.

### A replica's observer role follows the committed membership

`raft.isObserver` is the replica's EFFECTIVE role. It is seeded from
`Config.IsObserver`, which is only the initial hint, and `checkSelfRole`
re-derives it from the replicated membership wherever the configuration is
(re)established: construction, snapshot restore, and commit-time config change
application. The hint governs only while the configuration does not mention the
replica (a joiner that has not yet applied the entry adding it). This is etcd's
learner model, where the role is a property of the `ConfState`.

**Promotion happens in place.** An `AddNode` for an existing observer moves it
from `observers` to `addresses` (`handleAddNode`; `validateAddNode` exempts the
promotion from `ErrNodeAlreadyExists`). On the leader, `syncRaftRemotes` moves
the observer's `remote` into `remotes` with its replication progress intact. On
the promoted replica, `checkSelfRole` clears `isObserver` when it applies the
same entry, and from that moment the replica campaigns, can be elected and
serves ReadIndex, with no restart. Before that moment it was already replicated
with full payloads and already answered vote requests, so the only new
obligation is the vote window its heartbeat acks promise under `LeaseRead`.
`setObserver` raises the persisted `VoteWindow` to that promise (and sizes the
post-restart vote hold-down) before any ack can carry it, because the hard state
is persisted ahead of every message the next update sends.

**Why the observer role may follow the membership and the witness role may
not.** An observer and a voter are replicated identically, so they hold the
same state machine and differ only in which gates apply. Deriving the observer
role is also the safe answer to the quorum hazard: `quorum()` counts self
unconditionally, so a replica the configuration lists as an observer but that
believes itself a voter would size a quorum it is not in. A witness keeps no
state machine and the leader strips payloads for it, so a witness-role
disagreement is about what the replica holds and is still latched
(`roleMismatch`).

**The campaign gates test the observer role after the configuration fold.** A
replica restarted after its promotion committed, from a base that still lists it
as an observer, folds the committed tail when its election timer fires and then
campaigns as a voter. A replica seeded as a voter from the `StartShard` members
map (see Convergent Apply below) is likewise brought back to observer by
that fold before it can campaign, and every voter independently refuses to grant
a vote to a candidate it lists as an observer.

**Demotion** (a voter listed as an observer) cannot be asked of a live voter
(`validateAddNonVoting` refuses an existing member), but a snapshot, or a
committed `AddNonVoting` converging a members-map seed, can deliver it. The
replica then steps down if it was leading or campaigning.

**Witnesses**: vote in elections and count toward the unified commit quorum,
but store only entry metadata (Index/Term, no command payload). The durability
trade-off is in
[`../raft-consensus/engineering.md`](../raft-consensus/engineering.md#6-commit-calculation).

## Per-shard authorization is a deny-list

Per-shard authorization is a removed-node TOMBSTONE deny-list (the
etcd/Consul model), never a member allow-list. `Peer.IsRemoved` and
`Peer.IsRemovedIdentity` answer it. Joining is therefore not an auth bypass: a
replica the configuration has simply not heard of yet is not refused, which is
what a joining node and a member that recovered at a new address both depend
on. The `joining` flag exists for the two decisions an incomplete configuration
cannot safely support (campaigning and the candidate test in
`handleVoteRequest`), not to bypass authorization.

`removedIdentities` is what makes the deny-list answerable against the
AUTHENTICATED peer identity rather than the claimed `msg.From`.
`handleRemoveNode` MOVES the departed replica's identity from `identities` to
`removedIdentities` rather than deleting it: a removed replica still holds its
private key, so it authenticates perfectly well and would evade its own
tombstone by naming any other replica ID. It is moved rather than left in place
because `identities` is the ACTIVE binding that `identityHolder` scans to
refuse duplicates, and leaving a departed replica's identity there would refuse
the legitimate operation of recycling a decommissioned machine into a new
replica ID. Nothing is ever evicted from `removedIdentities`.

## Safety validations

Validate-time (`configChangeValidators`), evaluated on the leader before the
entry is proposed:

- Cannot remove the last full (non-witness) replica (`ErrRemoveLastFullReplica`).
- Cannot remove the last voting member (`ErrRemoveLastVoter`).
- Cannot add a previously removed node (`ErrNodeRemoved`).
- Cannot add a node that already exists (`ErrNodeAlreadyExists`). Observer-to-voter
  promotion is exempt on the `AddNode` path.
- Cannot remove a node that is not a member (`ErrNodeNotFound`).
- Reject a zero replica ID (`ErrZeroReplicaID`) or an empty address on an add
  (`ErrEmptyAddress`).
- Cannot exceed the 64-voting-member cap (`ErrTooManyVotingMembers`).
- Cannot give one ACTIVE address to two replicas (`ErrDuplicateMemberAddress`).
  The inbound authorization chain is replica ID -> committed address ->
  certificate, and two active replicas at one address collapse it: the single
  host behind the address is authorized to speak as BOTH, so it holds two votes
  and two match indices in a quorum that believes they are independent. That is
  quorum forgery reachable through the ordinary admin API, with no forged
  message anywhere. Comparison is on the whole address (two replicas on one
  machine at different ports are a supported deployment) and on the CANONICAL
  spelling (`netaddr.Canonical`), because the certificate match this chain ends
  in is done by `x509.VerifyHostname`, which already treats two spellings of
  one host as one. Removed replicas do not count, so recycling a departed
  replica's address is allowed.
- Cannot bind one identity to two active replicas
  (`ErrDuplicateMemberIdentity`), for the same reason and with no co-location
  exception: two replicas on one machine still need distinct keypairs.
- Cannot add an identity this shard has committed a REMOVAL for
  (`ErrRemovedMemberIdentity`). Without this the add commits and then every one
  of the new member's messages is dropped by `Peer.IsRemovedIdentity` on every
  peer: a voter counted in every quorum that can never be heard from.
- Witness policy violations (`ErrTooManyWitnesses`,
  `ErrBreaksFullReplicaMajority`). `validateRemoveNode` enforces the
  full-replica-majority rule too: without it, removing a full replica from a
  compliant `{3 full + 2 witnesses}` silently produces the witness-heavy
  `{2 full + 2 witnesses}` the policy exists to reject.

Apply-time (`configChangeHandlers`), the deterministic replicated-state-machine
transition:

- `ErrEmptyAddress`, `ErrNodeRemoved`, `ErrNodeNotFound`,
  `ErrRemoveLastFullReplica`, `ErrRemoveLastVoter`, and `ErrNodeAlreadyExists`
  for the two role transitions a bootstrap seed can never manufacture
  (`AddNode` over a witness, `AddWitness`/`AddNonVoting` over each other's
  role).

## The 64-voting-member cap

`MaxVotingMembers = 64` exists because ReadIndex ack tracking uses a `uint64`
`ackBits` bitfield, mapping replica IDs to bit positions 0..63. A 65th voting
member would alias bit positions and let a sub-quorum satisfy a ReadIndex
check. It is enforced at every point a voting set can GROW, and each of those
points fails in a way the operator can act on:

- `Host.StartShard` and `ImportSnapshot` (admission). Bootstrap does not run
  the config-change validators, so without this a 65-member map would be
  committed as the index-1 entry and break every future election on that
  shard, durably, across restarts.
- `validateAddNode` / `validateAddWitness` / `validateConfigChangePolicy`
  (propose time).
- `handleSnapshot` (the snapshot membership size gate; see
  [`../raft-consensus/engineering.md`](../raft-consensus/engineering.md#handlesnapshot)).
- `newRaft`, which refuses to build a replica whose RECOVERED configuration
  does not fit, returning `ErrTooManyVotingMembers` so `StartShard` reports the
  error and the host stays up. This is the durable form of the fault and the
  one an operator cannot escape on their own.
- `Peer.syncRaftRemotes`, which checks BEFORE touching any raft map and returns
  the error out through `Peer.Commit`, so the engine's `failCommit` marks the
  shard failed and fires `OnShardFailed` while the host and its other shards
  keep running. Leaving the raft mirror on the last configuration that fit is
  what makes this fail closed.
- `becomeLeader`, which declines the promotion before writing any field.
- `rebuildReplicaToBit`, the backstop, which RETURNS `ErrTooManyVotingMembers`
  rather than asserting: a panic there would kill the raft step worker, a
  goroutine shared by every shard that worker owns, over a fault in one shard,
  and again on every restart. Truncating the map to fit is not an option: bit
  positions above 63 would alias onto live ones.

**Apply deliberately does NOT re-check the cap.** `handleAddNode` and
`handleAddWitness` skip it by design. `membership.apply` runs on followers
replicating a committed entry and during recovery replay; rejecting an
already-committed config change at apply time would make that replica diverge
from the agreed membership.

## Convergent apply

The same reasoning that removes the cap check from apply shapes the
duplicate-add checks. `handleAddNode`, `handleAddNonVoting` and
`handleAddWitness` place the target in exactly the role the committed entry
names, whatever role the membership currently records for it. They refuse only
a `Removed` tombstone and the two role transitions the seed can never
manufacture.

This is required because a membership is not always built purely from committed
entries. `Host.StartShard` seeds `Membership.Addresses` from the caller's
`members` map whenever recovery restored nothing, which is the ordinary state
of a caught-up replica that has never snapshotted, since membership is
persisted only alongside a snapshot record. That map is the operator's list of
replica IDs and records no roles, so **every witness and observer in it is
seeded as a full voter**, including the local replica (`join=false` requires
self to be listed). The committed `AddWitness` / `AddNonVoting` entries that
gave those replicas their real roles are still in the log, since config changes
are compacted only behind a snapshot, and are re-applied on restart. Refusing
them would leave the wrong roles in place permanently: nothing else repairs the
seed, and `configChangeID` would never advance past the skipped entry. `checkSelfRole` is
not a substitute: it repairs nothing in the membership and acts only on the
local replica, whose observer role it then derives from the repaired
membership and whose witness disagreement it latches.

Converging is not a weakening. When the membership was built from committed
entries alone, `validate` ran on the leader before the entry was proposed and
guaranteed the target was not a member, so the role moves are provably no-ops.
The refusals that remain are exactly those whose precondition holds identically
on every replica, or that the seed can only LOOSEN:

- The add refusals read `removed` and `isWitness`/`isObserver`, and the seed
  only ever populates `addresses`, so it can manufacture neither a tombstone
  nor a non-voter role.
- `handleRemoveNode`'s three refusals read `len(m.addresses)`, `isMember` and
  `isWitness`. Seeding every listed non-voter as a voter makes `isMember` more
  likely true, `len(m.addresses)` larger, and `isWitness` less likely true, so
  each refusal is strictly LESS likely to fire than on the true membership: a
  committed remove the true membership would apply is never refused.

`processConfigChanges` reports two outcomes differently:

- A **decode failure** returns `ErrCorruptConfigEntry` and is FATAL. The bytes
  are committed replicated state every peer can read and this replica cannot,
  so continuing means running on a configuration that differs from the rest of
  the shard, silently, and durably once the next snapshot persists it.
  `Peer.Commit` propagates it and the engine's `failCommit` turns it into a
  shard fault. Recovery treats the identical bytes as fatal, and the two paths
  must agree. Whatever was
  folded before the corrupt entry is published first, so the shard's last
  coherent configuration is what its readers see.
- A **deterministic apply refusal** is logged at WARN and skipped. Every
  replica evaluates it identically, so it is agreement, not corruption.
  Faulting on those would kill shards over an outcome the whole cluster agrees
  on. A skip is still final (nothing re-delivers a committed entry), which is
  why the Host membership API pre-validates through `ValidateConfigChange`: a
  skip on the leader's own freshly proposed entry would hang the caller's
  `RequestState`.

## Leader self-removal (Paper Section 6)

When a committed config change removes the leader from membership,
`Peer.checkLeaderSelfRemoval` transitions it to follower. It runs during
`Peer.Commit`, after `processConfigChanges`, once per committed
`EntryConfigChange` in the batch, so self-removal always sees the post-change
membership. `syncRaftRemotes` sets `selfRemoved` in the same pass, so the
stepped-down leader also stops campaigning.

## Configuration

Per shard, on `config.Config`:

| Field | Default | Meaning |
|---|---|---|
| `OrderedConfigChange` | `*bool`, nil = **true** | not consulted outside `pkg/config`; one config change in flight is enforced unconditionally |
| `IsObserver` | `false` | this replica starts as an observer: replicates but does not vote. Initial hint only: the committed membership decides from the moment it names the replica |
| `IsWitness` | `false` | this replica votes but stores no log data |
| `Witness` | `WitnessConfig{}` | `MaxWitnesses` and `RequireFullReplicaMajority`; bootstrap seed only |

`IsObserver` and `IsWitness` are **mutually exclusive**. `IsWitness` is binding;
`IsObserver` is not (see "A replica's observer role follows the committed
membership").

`OrderedConfigChange` is a `*bool` so its zero value means "unset, use the
default" rather than "off". Nothing outside `pkg/config` reads it: the raft
layer refuses a second in-flight config change with `ErrRejected` regardless,
and `config.BoolPtr(false)` only makes `Config.Validate` log a warning.

**Witness policy is replicated state, not per-node configuration.**
`Config.Witness` seeds it at bootstrap; after that it changes only through
`Host.RequestUpdateWitnessPolicy`, which proposes a `ConfigChange` of type
`proto.UpdateWitnessPolicy`. A per-node policy value would let replicas disagree
about which config changes are legal.

`Config.MemberIdentities` is the founding map's identities (`join=false`) or
the join anchor (`join=true`). `Config.RequireMemberIdentities`, which the Host
sets from whether it has a transport, makes every admission require one. It is
validate-only: a committed entry is never refused on the apply path.

## Persistence

Membership is persisted in three places, and all three must agree:

1. **`EntryInitialMembership` at log index 1**, whose `Cmd` is a marshaled
   `proto.Membership`. Every shard writes it on first start, so the member set a
   replica believes in is the member set the log proves.
2. **Inside every snapshot**, so a replica recovering from one learns the
   configuration as of that index without the log.
3. **In the WAL's own membership section**, which carries a third version
   discriminator (`membershipSectionVersion`) distinct from
   `proto.WireVersion`; see
   [`../wire-protocol/architecture.md`](../wire-protocol/architecture.md).

`proto.Membership` carries the address map, the observer and witness sets, the
removed set, the replicated session policy, the identity maps, and the witness
policy. Maps are encoded with keys sorted ascending so identical configurations
produce identical bytes.

Replica retirement is host-local, not replicated. `RemoveData` →
`LogDB.RemoveNodeData` adds `(shardID, replicaID)` to the WAL's retired-replica
registry, a `RETIRED` file in the WAL root (`magic "QRRETIRE" | count |
count × (shardID, replicaID) | crc32c`, 16 bytes per entry, rewritten whole and
renamed into place). Replay drops every record of a retired replica,
`LogDB.NodeRemoved` reports it, every LogDB write naming it returns
`logdb.ErrReplicaRetired`, and re-encryption carries the registry across. A
malformed registry fails `Open` (`ErrRetiredRegistryMalformed`) rather than
handing retired IDs back.

## Test strategy

Unit coverage runs under `make test` (`pkg`) and `make test-raft`
(`pkg/internal/raft/membership_test.go` and the raft suites). The host-level
membership files in `pkg/` are:

| File | Pins |
|---|---|
| `host_membership_identity_test.go` | the identity binding |
| `host_membership_limits_test.go` | the 64-voting-member cap and the witness limits |
| `host_membership_reconstruct_test.go` | reconstructing membership from the log |
| `host_membership_sync_test.go` | the sync variants |
| `host_configchange_index_test.go` | `configChangeIndex` ordering |
| `host_configinlog_test.go` | config-in-log bootstrap |
| `host_config_fault_regression_test.go` | fault regressions |
| `host_nonvoting_bootstrap_test.go` | observers and witnesses at bootstrap |
| `host_bootstrap_entry_test.go` | `EntryInitialMembership` at index 1 |
| `messagehandler_identity_test.go`, `messagehandler_removed_identity_test.go` | sender authorization, including a removed member's identity |
| `membership_test.go` | the `Membership` type |

Integration: `test/integration/host` (`make integration-test-host`) covers add
and remove under load and with snapshots, and in-place observer promotion;
`test/integration/e2e` (`make integration-test-e2e`) covers add, remove and
replica replacement on a multi-node cluster. `test/membership` is the shared
helper that applies a change through whichever replica leads at each attempt
and treats a retried change that already committed as success.

**What a test here must always assert**: that a config change takes effect on
commit, not on append. Applying on append is how a config change that is later
truncated leaves a replica believing in a configuration no quorum agreed to.

**Fault-injection seams**: propose a stale `configChangeIndex` to exercise the
replay guard; the convergent-apply path is exercised by restarting a replica
whose membership was seeded from the `StartShard` members map.

## Observability

**Metrics.** `SecurityMetrics.IncUnauthorizedMessageDropped(shardID, from,
reason)` counts every inbound message refused by the sender gate. The reason
set is closed: `removed_replica` (usually a removed node that has not noticed),
`removed_identity` (a removed keypair claiming another replica ID; alert on
it), `misrouted`, `unknown_member` (the benign window before a config change
reaches this replica), `identity_mismatch` (a forged sender or a stale identity
after a key change; never routine) and `missing_identity`. There is no
dedicated membership collector; watch the voting-member count against the 64
cap.

**Diagnostics.** `Host.DiagRemotes(shardID)` reports per-remote replication
state, which tells a newly added replica that is catching up from one that is
not receiving anything. Each remote is rendered
`r<id>[role=<role> match=... next=... state=... paused=... active=...
inflights=...]`, and voters, observers and witnesses all appear. The structured
form is `ShardInfo.Replicas` (`[]ReplicaProgress`, `Role` of type `ReplicaRole`:
`ReplicaRoleVoter`, `ReplicaRoleObserver`, `ReplicaRoleWitness`), published by
the step worker with the rest of the leader's diagnostic snapshot, so it is
empty on a follower and on the first call for a shard. It lets an embedder
watch an observer's `Match` reach the leader's index before promoting it; a
caller computing a quorum or health figure must filter on `Role`.

**Logging** (`log/slog`):

| Level | Event | Why |
|---|---|---|
| WARN | a message dropped by the sender gate (rate-limited, with `drops_since_last_log`) | a stale removed node, or something impersonating a member |
| ERROR | `config change unmarshal failed`, `initial membership entry unmarshal failed` | committed membership is unreadable; the shard faults |
| ERROR | `refusing initial-membership entry outside the bootstrap index` | an `EntryInitialMembership` at an index other than 1; it is skipped identically on every replica and the shard does not fault |
| ERROR | `shard membership exceeds the ReadIndex ackBits word` | the 64-member cap was reached on a path that fails the shard |
| WARN | `config change apply skipped` | a deterministic apply refusal, identical on every replica |
| INFO | `replica promoted in place` / observer role derived from the membership | the replica's own role changed |

No certificate material appears in any log line. A node identity is an SPKI
hash, public by construction.

**Notifications.** `config.EventListener.OnMembershipChanged` fires on the
apply goroutine when a batch applied on this host contained a config change or
the bootstrap membership entry, after that batch's proposals completed; it must
not block. `OnNodeDeleted` fires after `RemoveData`,
and the leadership callbacks report the leader changes that removals and
transfers cause.
