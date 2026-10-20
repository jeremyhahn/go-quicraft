# Compatibility and upgrades: architecture

> **Status: NORMATIVE** · Scope: go-quicraft, all persisted and wire formats ·
> Last verified: 2026-10-20
> Conflicts with this document are defects. Fix them here first.

What this library promises across versions, what it does not, and why. The
policy spans `pkg/proto`, the WAL, `snapshot.meta`, snapshot artifacts and the
sealed root key, so it has its own folder rather than a section in each
feature. Each feature's `api.md` summarises its own surface under "Versioning
and deprecation" and points here.

| Document | Holds |
|---|---|
| this file | the stability guarantee, the pinning policy, the policy from v1.0.0, the identity requirement, rejected alternatives |
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

The current release is **0.23.0-alpha**, an internal alpha. The first stable
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
- From 0.16.0 to 0.21.0-rc.2 every release was a candidate for staging, not
  a production release. A newer candidate supersedes the older ones.
- From 0.22.0-alpha releases are internal alphas again: no release candidate
  is cut until the stack is dev complete
  ([release-versioning.md §1a](../../../handbook/software/conventions/release-versioning.md#1a-release-taxonomy)).
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

## From v1.0.0

> **Status: ACKNOWLEDGED BY THE OWNER ON 2026-10-20 (#404), EVERY ANSWER AS
> WRITTEN.** This section restates what the company conventions already bind
> for a post-1.0 release. Where the conventions are silent it lists the gap
> under [Open questions for the owner](#open-questions-for-the-owner) with its
> answer. The SPEC v1.0.0 bar requires the policy to be acknowledged
> (release-versioning.md §1, §8); the owner acknowledged it, not the change
> that wrote it.

The binding source is
[release-versioning.md](../../../handbook/software/conventions/release-versioning.md).
Where this section and that document disagree, the handbook wins and this
section is the defect.

### Semantic versioning after 1.0

The bump rules change at 1.0
([§1](../../../handbook/software/conventions/release-versioning.md#1-semantic-versioning-with--alpha)):

| Bump | Pre-1.0 (today) | Post-1.0 |
|---|---|---|
| PATCH | Bug fix, no API change | Bug fix |
| MINOR | Added API or a breaking change | Added API, backward compatible |
| MAJOR | Reserved for 1.0 | Breaking change. "The module path gains `/vN` and every consumer's imports change, so take a major deliberately, never mechanically." |

For go-quicraft a breaking change after 1.0 is any of the kinds this library
already lists under BREAKING: an exported identifier removed or renamed, a
signature changed, an observable run-time behaviour changed, or an on-disk or
wire layout changed. Each is a MAJOR release under the `/vN` module path; the
release workflow already refuses a v2+ tag without the suffix
([Released versions](#released-versions)). A published tag is never moved or
deleted, and a consumer's pin never decreases
([§3](../../../handbook/software/conventions/release-versioning.md#3-version-monotonicity)).

### The breaking-change protocol, bottom-up

A breaking change follows
[§8](../../../handbook/software/conventions/release-versioning.md#8-breaking-change-protocol):
"Changes travel bottom-up (00 L6), in the build order of
[../docs/02-dependency-graph.md](../../../handbook/software/docs/02-dependency-graph.md#build-order),
and all consumers are updated before merging."
go-quicraft sits directly above go-enclave, so a break here starts the walk:

1. Land it here with a CHANGELOG entry carrying `BREAKING:` in its heading
   (`### Changed — BREAKING: …`, §5), and get `go build ./... && go test ./...`
   green.
2. Tag go-quicraft.
3. Walk up the graph through every consumer (go-qrdb, go-xkms, go-quicbaas,
   go-trusted-ca and on up): "at each layer bump the pin, adapt, verify build
   and tests, then move up. Never skip a layer or pin a consumer to a version
   its own dependency lacks."
4. Re-verify at the top: go-trusted-platform builds and passes.
5. Update the documents the change falsifies, in the same change.
6. "Never merge a partial walk. If a layer cannot be updated now, stop and
   report the gap (00 L8)."

§8 step 1 reads "bump MINOR", the pre-1.0 rule; how it reads after 1.0 is
[open question 1](#open-questions-for-the-owner).

### Format versions start moving

The pinning rule above ends when v1 ships. Until the first stable production
v1 release, every format version stays at its current value, and none moves
without the owner's explicit approval, asked for and granted each time
([CLAUDE.md](../../CLAUDE.md)). The constants this governs are those in
[engineering.md](engineering.md#version-discriminators): `proto.WireVersion`,
`transport.WireVersion`, the `Membership` section version and `waldb`
`membershipSectionVersion`, `walFormatVersion` (the `WALFORMAT` marker),
`snapshotMetaVersion`, `snapshotArtifactVersion`, and the sealed root key
versions and `sealedStateMACVersion`. After v1 the version numbers start
incrementing, and the supremum rule for `proto.WireVersion`
([above](#the-transport-version-is-a-supremum)) takes effect. The first
post-1.0 bump of any of them is still the owner's to approve.

### Migration discipline

§1: "Until a project's first public release, backward compatibility, data
migrations and upgrade paths are non-goals." and "After the first public
release, §8 and real migration discipline apply." Wipe and re-sync, the only
[upgrade path](user.md#upgrade-path) today, stops being an acceptable answer
for a released layout. The handbook does not yet say what "real migration
discipline" consists of; the gaps are open questions 2 to 6 below.

### What does not change

- All transport is QUIC. There is no TCP fallback before or after 1.0
  ([CLAUDE.md](../../CLAUDE.md): "All transport must use QUIC, never fall back
  to TCP").
- Node identities stay mandatory on every host with a transport
  ([Node identities are mandatory](#node-identities-are-mandatory)).
- One discriminator per thing it protects; the persisted `Membership` version
  is never derived from `proto.WireVersion`.
- Every decoder fails closed on a layout it does not read.

### Open questions for the owner

These are gaps in the conventions. Each carries an **answer**: a
recommendation with its reason and its source, which the owner acknowledged
as written on 2026-10-20 (#404) and which now binds. Every answer takes the
strict, simple option that keeps Raft safety,
the fail-closed decoders and the pinned-format rule intact, and none removes a
feature.

Sources cited: the Go modules reference, [go.dev/ref/mod](https://go.dev/ref/mod)
("Major version suffixes", the import compatibility rule, `retract`); the
handbook; this repository's code; and etcd's and dragonboat's documented
practice where it is known reliably. A claim about another engine that has not
been checked against its current documentation is marked *practice to
confirm*.

1. **§8 after 1.0.** §8 step 1 says "bump MINOR"; the post-1.0 bump table says a
   breaking change is MAJOR with a `/vN` path. Does §8 read MAJOR from 1.0, with
   every consumer's walk changing imports to the `/vN` path?

   **Answer (acknowledged 2026-10-20): yes.** From v1.0.0, §8 step 1 reads "bump MAJOR and move
   the module path to `/vN`", and every layer of the walk changes its imports.
   A format bump that keeps the read window of question 2 and the upgrade
   window of question 3 is not a break and ships as MINOR; one that drops
   either window is MAJOR. *Why:* go.dev/ref/mod states the import
   compatibility rule ("If an old package and a new package have the same
   import path, the new package must be backwards compatible with the old
   package") and requires a `/vN` suffix from v2; §1's own table already says
   so, and only §8's wording predates it. The pre-publication exception (MINOR
   with a `BREAKING` marker) ends at publication (§1), so nothing else can
   carry a break after v1.0.0. The release workflow already refuses a v2+ tag
   without the suffix. *Follow-up:* §8's wording is the handbook's to change,
   not this repository's.

2. **Supported read window.** After a bump of `walFormatVersion`, a WAL
   payload-type layout, `snapshotMetaVersion`, `snapshotArtifactVersion` or a
   sealed-root-key version, how many prior versions must a release still read,
   and for how long?

   **Answer (acknowledged 2026-10-20): exactly one prior MINOR.** A release `N.M.x` reads every
   layout that `N.(M-1)` wrote, and nothing older. It writes only its own
   layouts (subject to question 3's gate). A MAJOR may drop the window and
   says so under BREAKING. Data older than the window upgrades by stepping
   through each MINOR in turn, or is re-synced from upgraded peers. Each
   decoder still gates on exact equality, now against a closed set of two
   values (current and previous), and refuses everything else as it does
   today (`walFormatVersion` in `waldb/format.go`, `snapshotMetaVersion`,
   `snapshotArtifactVersion` in `pkg/host.go`, `sealedStateMACVersion`).
   *Why:* one prior version is the smallest window that makes a rolling
   upgrade possible, and every extra version is a decoder that has to stay
   correct for data nobody writes any more. The step-through rule is etcd's
   documented upgrade path, one minor version at a time with no skipping.

3. **Mixed-version clusters.** After a bump of `proto.WireVersion` or
   `transport.WireVersion`, must a release interoperate with the previous wire
   version so a cluster can be upgraded node by node (a rolling upgrade), and
   if so, by negotiation or by accepting a fixed window? Today the check is
   exact match and mixed-version clusters are unsupported.

   **Answer (acknowledged 2026-10-20): yes, a rolling upgrade across one MINOR, by a fixed
   window and never by negotiation.** A release `N.M` accepts both its own
   wire version and `N.(M-1)`'s. It keeps sending the previous version, and
   writing the previous persisted layouts, until a cluster-wide switch is
   committed through the Raft log once every member runs `N.M`. A new message
   type (such as `LeaderExec` was) is sent only after that switch. The switch
   never goes back. Across a MAJOR there is no rolling upgrade unless its
   CHANGELOG says otherwise. *Why:* a committed switch is decided once, in the
   log, so every replica applies it at the same index and replays it the same
   way after a crash. Per-connection negotiation would let a leader send
   different layouts to different followers on a decision that is not in the
   log. Mixed versions stay confined to the upgrade itself. This is etcd's
   model: a rolling upgrade one minor version at a time, with the cluster
   version raised only when every member supports it. The exact-match checks
   (`frame.go` `buf[2] != WireVersion`, `MessageBatch.BinVer` and
   `SnapshotChunk.BinVer` against `proto.WireVersion`) widen to the same
   two-value set and keep failing closed outside it.

4. **Migration mechanism.** Is a migration done in place on open, by a
   separate offline tool, or by re-sync from upgraded peers, and is a
   migration allowed to be one-way?

   **Answer (acknowledged 2026-10-20): in place, by the library, after the switch of
   question 3, and one-way.** The library reads the old layout and writes new
   records, segments and snapshots in the new one. It never rewrites an
   existing file in place: old records age out through compaction and
   snapshots, so a crash mid-migration leaves only layouts the release reads.
   Re-sync from an upgraded peer stays a valid remedy at every version. There
   is no offline tool. *Why:* go-quicraft is a library and ships no binary
   (this document's opening), so a tool would be a second product to keep
   correct. Lazy, append-only migration reuses the WAL's existing crash
   safety instead of adding a rewrite path with its own failure modes.
   Dragonboat, which this library rewrites, refuses a data directory whose
   binary version it does not match and offers no migration tool (*practice
   to confirm* against its current release).

5. **Downgrades.** Is any downgrade supported after 1.0?

   **Answer (acknowledged 2026-10-20): only to the previous MINOR, and only before the
   question 3 switch is committed.** Until then no new layout has been
   written, so a node can be replaced with the release it was upgraded from.
   After the switch, no downgrade: the remedy is to restore a backup or
   re-sync. *Why:* a downgrade after new layouts exist needs a reverse
   migration that is code nobody runs in normal operation. The switch draws
   the line at a point every replica agrees on. etcd added a cluster-wide
   downgrade procedure in later releases (*practice to confirm*); this
   answer stays stricter.

6. **Deprecation.** The company release-versioning convention (§5) has no
   deprecation policy yet; it is an open question there. Does a post-1.0
   exported name get a deprecation window before a MAJOR removes it?

   **Answer (acknowledged 2026-10-20): yes.** A name is marked with a `// Deprecated:` comment
   naming its replacement, in a MINOR release at least one release before the
   MAJOR that removes it, and the CHANGELOG lists it under `### Deprecated`
   (Keep a Changelog 1.1.0, which §5 already adopts). The name stays for the
   rest of that MAJOR line, which semver already forces. A deprecation never
   removes a capability without a replacement. *Why:* `// Deprecated:` is the
   Go convention that gopls and staticcheck (SA1019) already report, and
   Keep a Changelog already has the heading, so the policy adds no machinery.
   *Follow-up:* the rule belongs in the company release-versioning
   convention, where the question is open.

7. **Payload-type retirement after 1.0.** Before 1.0 a detectable layout
   change retires a payload-type byte instead of bumping a version. Does that
   rule continue after 1.0, or does a released layout change bump
   `walFormatVersion` instead?

   **Answer (acknowledged 2026-10-20): the rule continues, extended by the read window.** A
   record layout change allocates the next free payload-type byte. The old
   byte is still decoded for the window of question 2, then retired: left
   unallocated, never reused, so a stale record falls through to
   `ErrUnknownPayloadType`, still the one invalid-format error.
   `walFormatVersion` moves only when the chunk or segment framing changes.
   *Why:* one discriminator per thing it protects
   ([above](#one-discriminator-per-thing-it-protects)): a record change
   bumping the framing version would put every record type behind one switch.
   The payload-type ladder in [engineering.md](engineering.md) already records
   each byte's status.

8. **Which event starts it.** §1 ties migration discipline to "the first public
   release" and ends the pre-publication exception "when the module is pushed
   to public GitHub"; CLAUDE.md pins format versions until "the first stable
   production v1 release". Are these the same event, v1.0.0?

   **Answer (acknowledged 2026-10-20): yes, one event: the `v1.0.0` tag, published to GitHub
   with it and not before.** §1a: "a plain `vX.Y.Z` is always production", so
   `v1.0.0` is the first stable production release and every `-rc.N` and
   `-alpha` before it stays pre-release. *Why:* if the module were pushed to
   public GitHub earlier, §1 would start migration discipline while
   CLAUDE.md still pins every format version, which leaves no way to version a
   layout change. Making the two one event removes that conflict. If the
   owner publishes earlier anyway, §1 governs as the handbook wins, and this
   section is revised in the same change.

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
