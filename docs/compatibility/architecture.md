# Compatibility and upgrades: architecture

> **Status: NORMATIVE** · Scope: go-quicraft, all persisted and wire formats ·
> Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

What this library promises across versions, what it does not, and why. The
policy spans `pkg/proto`, the WAL, `snapshot.meta`, snapshot artifacts and the
sealed root key, so it has its own folder rather than a section in each
feature. Each feature's `api.md` summarises its own surface under "Versioning
and deprecation" and points here.

| Document | Holds |
|---|---|
| this file | the stability guarantee, the pinning policy, the identity requirement, rejected alternatives |
| [engineering.md](engineering.md) | every version discriminator, its byte layout, its check and its error; the payload-type ladder; tests |
| [api.md](api.md) | the interface requirements an embedder's own types must meet, and the error values each check returns |
| [user.md](user.md) | the upgrade path, upgrade notes for every release, bootstrapping a cluster with identities |

There is no `cli.md`: go-quicraft is a library and ships no binaries, and
nothing in it migrates or inspects a data directory, so there is no upgrade verb
to document.

Sources: `pkg/proto/constants.go`, `pkg/proto/membership.go`,
`pkg/internal/transport/frame.go`, `pkg/logdb/waldb/format.go`,
`pkg/logdb/waldb/db.go`, `pkg/logdb/waldb/record.go`,
`pkg/internal/engine/snapshot_meta.go`, `pkg/snapshot_artifact_trailer.go`,
`pkg/host.go`, `pkg/seal/barrier.go`, `CHANGELOG.md`, `VERSION`.

The current release is **0.21.0-rc.1**, a release candidate. The first stable
release will be 1.0.0.

## Stability guarantee

go-quicraft is pre-1.0. Under semantic versioning a minor version may break
anything, and many have broken the on-disk format.

| Surface | Guarantee before 1.0 |
|---|---|
| Exported Go API (`pkg`, `pkg/config`, `pkg/sm`, `pkg/seal`, `pkg/logdb`, ...) | None between minor versions. Breaks are listed in the CHANGELOG under BREAKING. |
| Run-time behaviour of an unchanged signature | None between minor versions. A change that compiles but alters what a caller observes (for example 0.21.0-rc.1's `ShardInfo.Replicas`) is listed under BREAKING too. |
| On-disk formats (WAL chunk framing and records, snapshots, `snapshot.meta`, exported artifacts, sealed root key) | None between minor versions. A format break means wipe and re-sync; see [the upgrade path](user.md#upgrade-path). |
| Wire format between nodes | Exact match only. Mixed-version clusters are not supported. |
| Patch releases | No intentional API or format break. Read the CHANGELOG anyway. |

There is no long-term-support line, no deprecation window and no compatibility
shim. Pin an exact version and upgrade deliberately.

### Released versions

- `v0.4.0-alpha` and `v0.5.0-alpha` resolve as Go module versions, but their
  release pipelines never completed and neither was published. The tags stay
  where they are, because module versions are immutable and repointing a
  checksummed tag breaks downstream builds. `v0.5.1-alpha` carries the same
  library content as `v0.5.0-alpha`.
- `v0.13.0-alpha` was tagged but not published; `v0.13.1-alpha` supersedes it
  with one test fix.
- From 0.16.0 every release is a candidate for staging, not a production
  release. A newer candidate supersedes the older ones.
- A future v2+ line takes the `/v2` module path suffix; the release workflow
  refuses a v2+ tag without it.

## Format versions are pinned until the first production release

Every wire and persisted format version is pinned at its current value for the
whole pre-release period: 1 for every version number except the exported
artifact header and the sealed-state MAC, which are pinned at 2. Every decoder
gates on exact equality.

- A layout change is made in place. The number does not move.
- Every consuming repository is updated in lockstep to read and write the new
  layout.
- Development clusters are wiped and re-synced, never migrated. There is no
  dual-read path and no legacy decode case, because no deployed data or peer is
  promised to stay readable.
- When a layout change must stay detectable, the record's payload-type byte is
  retired and the next free byte allocated. A retired byte is never reallocated,
  so a stale record falls through to the standard `ErrUnknownPayloadType`.
  There are no per-layout constants or per-layout error types: the remedy for
  every stale layout is the same.
- A new message type extends the type range in place, as `LeaderExec` (28) and
  `LeaderExecResp` (29) did in 0.21.0-rc.1. It changes no version number.
- Version numbers start incrementing at 1.0. From then a bump means a decoder
  that reads the old layout, a migration story, and a supported window.

The consequence: **two binaries built from different pre-release commits may be
mutually unreadable while both stamp the same version.** Mixing them over the
wire or over a data directory is unsupported, and the failure is a decode or
recovery error rather than a version mismatch that names itself. Build every
node, and every consuming repository, from the same commit.

The version byte is kept even though its value never moves, because it is what
makes the 1.0 bump possible. A format with no in-band discriminator cannot be
superseded, and refusing anything but the pinned value turns some commit skews
into a clean refusal instead of fields read at the wrong offsets.

### One discriminator per thing it protects

There is no single format version. Each discriminator is scoped to what it
protects, so a transport-only change never makes a node unable to replay its own
log. [engineering.md](engineering.md#version-discriminators) lists all of them.

### The transport version is a supremum

`Membership` is embedded in `Snapshot`, in `Message`, in `MessageBatch`, and
directly in `SnapshotChunk`. A `Membership` layout change therefore alters the
transport bytes.

> **From 1.0 onward:** bump `proto.WireVersion` whenever any type reachable from
> `MessageBatch` or `SnapshotChunk` changes layout, including the embedded
> persisted ones.

The relation is `>=`, not `==`: a transport-only change leaves persisted bytes
alone. The two constants stay separate and neither is derived from the other,
because deriving the persisted version from the wire version would change the
byte every replica expects at log index 1 on the first transport-only bump. The
compile-time guard and the test that hold this are in
[engineering.md](engineering.md#the-supremum-is-enforced-at-compile-time).

## Node identities are mandatory

`msg.From`, the replica ID Raft counts toward quorum, travels in-band and is
not authenticated by mTLS. Committed membership therefore records a node
identity per replica, and it is the only thing the receive path checks. The
identity is `sha256(leaf.RawSubjectPublicKeyInfo)`, rendered `spki:<hex>`: a
hash of the node's public key, unchanged by certificate renewal on the same
keypair.

On every host with a transport, identities are required at admission:

- `StartShard(join=false)` requires `Config.MemberIdentities` to name every
  member (`*InvalidMemberError` naming the replica), with this host's own entry
  equal to its `NodeIdentity()`, no identity bound to two replicas, and no entry
  for replica 0.
- `StartShard(join=true)` on a first start requires at least one entry: the
  **anchor**, the replicas the joiner accepts its bootstrap snapshot from, each
  bound to the identity its connection must present. The snapshot's own
  membership is not trusted for this. On any start, entries beyond the members
  map form the operator anchor the receive gate consults for a sender no held
  configuration names, which is how a replica that was down while a member was
  added and elected learns who leads.
- `RequestAddNode`, `RequestAddObserver` and `RequestAddWitness` return
  `ErrMemberIdentityRequired` unless the configuration already holds an identity
  for the replica; use the `*WithIdentity` forms. The leader's propose-time
  validator enforces the same rule (`config.Config.RequireMemberIdentities`,
  which the Host sets on every shard behind a transport).
- `ImportSnapshot` refuses a configuration with an identity-less active member;
  a `WithImportMembership` rewrite supplies identities through
  `WithImportMemberIdentities`.
- A member with no committed identity has every message dropped (reason
  `missing_identity`). There is no option to make identities optional.

A host started `WithoutTransport()` has no wire boundary and none of this
applies.

Consequences an operator plans for:

1. A wildcard certificate no longer lets its holder assert another member's
   `msg.From` or bootstrap a joining replica.
2. A member can move to a new address and keep its votes and acks counted
   without a config change.
3. An inbound connection whose certificate also covers this node's own host is
   refused at the transport, except from peers on the same machine or loopback.
   **A cluster running one wildcard or shared-SAN certificate across distinct
   hosts will stop connecting.** Every host needs its own keypair; two hosts
   sharing one share an identity, and the membership validator refuses to
   commit an identity twice (`ErrDuplicateMemberIdentity`).

The procedure is in [user.md](user.md#bootstrapping-and-growing-a-cluster).

## Rejected alternatives

| Alternative | Why not |
|---|---|
| Bump a version number on every pre-release layout change | v1 would ship at an arbitrary high version recording layout churn no deployment ever depended on. |
| A migration or dual-read path for each old layout | No pre-release data is promised to stay readable, and every migration is code that has to stay correct for a layout nobody runs. Wipe and re-sync is the one remedy. |
| A named constant or error type per retired layout | A maintenance burden with no payoff, since every stale layout has the same remedy. One standard error (`ErrUnknownPayloadType`) is the whole contract. |
| One format version for everything | A transport-only change would make a node unable to replay its own log. |
| Deriving the persisted `Membership` version from `proto.WireVersion` | The first transport-only bump would change the byte every replica expects at log index 1. |
| Binding a member by address instead of by key | A wildcard or multi-SAN certificate covers every member's address, two replica IDs at one address give one host two votes, and a member that moves address loses its votes until a config change commits, which cannot happen during quorum loss. |
| Learning a peer's identity from its own connection | It would let a holder of a promiscuous certificate register itself as any replica. |

## What this does not do

- No rolling upgrade: a new node and an old node never exchange traffic.
- No downgrade at any boundary.
- No migration of a data directory, by any tool. `waldb.Reencrypt` changes the
  barrier a WAL is encrypted under; it is not a format migration.
- No deprecation window: a removed name is gone in the release that removes it.

## Cross-references

- [Deployment](../multi-group-host/user.md): configuration, directory layout, disaster recovery
- [Lifecycle](../multi-group-host/engineering.md): startup, crash recovery, WAL replay, shutdown
- [Wire format](../wire-protocol/engineering.md): byte-level layouts of every proto type
- [WAL persistence](../log-storage/engineering.md): segment and chunk framing, the `WALFORMAT` marker
- [Encryption](../seal-barrier/architecture.md): barrier, epochs, sealed root key, AAD binding
- [Membership changes](../membership-changes/user.md): node identities in practice
- [API reference](../multi-group-host/api.md): exported surface and error types
