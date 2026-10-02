# Compatibility and upgrades

> **Status: NORMATIVE** · Scope: go-quicraft, all persisted and wire formats ·
> Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

What this library promises across versions, what it does not, and what an
upgrade involves. It spans `pkg/proto`, the WAL, `snapshot.meta`, snapshot
artifacts and the sealed root key, so it sits at the `docs/` root instead of in
one feature folder. Each feature's `api.md` summarises its own surface under
"Versioning and deprecation" and points here.

Sources: `pkg/proto/constants.go`, `pkg/proto/membership.go`,
`pkg/internal/transport/frame.go`, `pkg/logdb/waldb/format.go`,
`pkg/logdb/waldb/db.go`, `pkg/logdb/waldb/record.go`,
`pkg/internal/engine/snapshot_meta.go`, `pkg/snapshot_artifact_trailer.go`,
`pkg/host.go`, `pkg/seal/barrier.go`, `CHANGELOG.md`, `VERSION`.

The current release is **0.16.0** (2026-10-01), a release candidate with the
same code as 0.16.0-alpha. The first stable release will be 1.0.0.

## Stability guarantee

go-quicraft is pre-1.0. Under semantic versioning a minor version may break
anything, and many have broken the on-disk format.

| Surface | Guarantee before 1.0 |
|---|---|
| Exported Go API (`pkg`, `pkg/config`, `pkg/sm`, `pkg/seal`, `pkg/logdb`, ...) | None between minor versions. Breaks are listed in the CHANGELOG under BREAKING. |
| On-disk formats (WAL chunk framing and records, snapshots, `snapshot.meta`, exported artifacts, sealed root key) | None between minor versions. A format break means wipe and re-sync; see [Upgrade path](#upgrade-path). |
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
- A future v2+ line takes the `/v2` module path suffix; the release workflow
  refuses a v2+ tag without it.

## Format versions are pinned until the first production release

Every wire and persisted format version is pinned at its current value for the
whole pre-release period: 1 for every version number except the exported
artifact header and the sealed-state MAC, which are pinned at 2. Every decoder gates on exact equality.

- A layout change is made in place. The number does not move.
- Every consuming repository is updated in lockstep to read and write the new
  layout.
- Development clusters are wiped and re-synced, never migrated. There is no
  dual-read path and no legacy decode case, because no deployed data or peer is
  promised to stay readable.
- When a layout change must stay detectable, the record's payload-type byte is
  retired and the next free byte allocated. A retired byte is never reallocated,
  so a stale record falls through to the standard `ErrUnknownPayloadType`.
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

## Version discriminators

There is no single format version. Each discriminator is scoped to what it
protects, so a transport-only change never makes a node unable to replay its own
log.

| # | Discriminator | Value | Scope | Check |
|---|---|---|---|---|
| 1 | `proto.WireVersion` | 1 | transport: `MessageBatch.BinVer`, `SnapshotChunk.BinVer` | exact match |
| 2 | `transport.WireVersion` (frame header byte 3) | `0x01` | transport frame header | exact match |
| 3 | `Membership` section version byte (`membershipFormatVersion`) | 1 | persisted `proto.Membership` | exact match |
| 3a | `waldb` `membershipSectionVersion` | 1 | the membership section inside WAL snapshot records | exact match |
| 4 | `WALFORMAT` marker (`walFormatVersion`) | 1 | WAL chunk framing and block size | exact match, checked before any segment is read |
| 5 | WAL payload-type byte | per record | each WAL record's layout | unknown or retired byte is fatal on replay |
| 6 | `snapshot.meta` (`QRSNAPMD`, `snapshotMetaVersion`) | 1 | the record beside each snapshot | self-describing, append-only |
| 7 | at-rest snapshot frame context (`QRSNAP1`, `SnapshotFrameAADLen`) | 40 bytes | encrypted snapshot frames | authenticated |
| 8 | exported artifact (`QRSNAP01`, `snapshotArtifactVersion`) | 2 | `ExportSnapshot` / `ImportSnapshot` | header version plus trailer |
| 9 | sealed root key (`Version`, `KDFVersion`, `HKDFVersion`, `Algorithm`) and state MAC (`sealedStateMACVersion`) | MAC version 2 | the sealed root key blob | see [Sealed root key](#sealed-root-key) |

### 1–2. Transport versions

`proto.WireVersion` is stamped into every `MessageBatch` and `SnapshotChunk`.
Both ends of a connection are live processes, so a mismatch costs one rejected
connection and nothing durable. A peer with another value is refused, not
misparsed; there is no negotiation.

Under the pinning policy a `BinVer` match does not mean two binaries agree on
the bytes. A rolling upgrade across a pre-release layout change does not work
and does not announce itself: nodes from different commits fail to decode each
other's batches or, for a length-preserving change, disagree about what a field
means. Stop the cluster, replace every binary, wipe and re-sync.

`transport.WireVersion` is the third byte of every QUIC frame, after the magic
bytes `Q` `C`. It is versioned separately because the frame header and the
payload encoding can change independently.

### 3. `Membership` section version

`Membership` is persisted: in the WAL, in snapshot records, in exported
artifacts, and in the `EntryInitialMembership` entry at log index 1. It is
always embedded in a larger buffer, so neither a length check nor a
trailing-byte check can tell one layout from another; hence the leading byte.
The decoder accepts exactly 1 and refuses anything else with
`*proto.UnsupportedMembershipVersionError` (matching `proto.ErrUnsupportedVersion`)
before reading a field.

`proto.Membership` layout:

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | version (`1`) |
| 1 | 8 | `ConfigChangeID` |
| 9 | 8 | `MaxWitnesses` |
| 17 | 8 | `RequireFullReplicaMajority` (bool as uint64) |
| 25 | 8 | `SessionExpiryEntries` |
| 33 | 8 | `MaxTotalSessions` |
| 41 | 8 | `MaxSessionsPerClient` |
| 49 | 8 | `MaxSessionsPerOwner` |
| 57 | var | `Addresses`, `Observers`, `Witnesses`, `Removed`, `Identities`, `RemovedIdentities` maps |

The fixed region is 57 bytes (`membershipFixedSize`) and an empty section is 81:
the fixed region plus six 4-byte map counts. Every map is written
unconditionally.

An unset session policy is distinguishable from one that disables expiry:
`MaxTotalSessions` can never be zero in a committed policy (`config.Validate`
rejects it), so it is the "policy recorded" marker (`SessionPolicy.IsSet`), and
an expiry threshold of zero keeps its meaning of expiry disabled.

`Identities` maps replica ID to node identity; the receive-side gate authorizes
`msg.From` against it. `RemovedIdentities` holds the identity each removed
replica held at removal, so the removal deny-list is checked against the
authenticated peer identity, not the claimed `msg.From`. Its entries are never
evicted: any eviction policy would also be a re-admission policy.

`proto.ConfigChange` has no version byte. It has one layout and every field is
mandatory, so a payload that does not span the whole layout is a truncation and
fails with `ErrBufferTooSmall` at every cut point. An identity-less change is an
explicit zero-length prefix, never an inferred short payload.

`waldb` pins its own `membershipSectionVersion` (1, exact match on replay) for
the membership inside WAL snapshot records. Its layout differs from
`proto.Membership`: policy flags are one byte, and the session policy is a tail
after `Identities`, because this decoder parses straight-line from a fixed
offset and growing the fixed region would move every map.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | version (`1`) |
| 1 | 8 | `ConfigChangeID` |
| 9 | 8 | `MaxWitnesses` |
| 17 | 1 | policy flags (`RequireFullReplicaMajority`) |
| 18 | var | `Addresses`, `Observers`, `Witnesses`, `Removed`, `Identities` maps |
| — | 32 | session policy: `SessionExpiryEntries`, `MaxTotalSessions`, `MaxSessionsPerClient`, `MaxSessionsPerOwner` |
| — | var | `RemovedIdentities` map |

Its fixed region is 18 bytes and an empty section 74 (`minMembershipSize`),
which is also the read floor replay enforces before reading the version byte. A
shorter section fails with `ErrTruncatedSnapshotRecord`.

#### The transport version is a supremum

`Membership` is embedded in `Snapshot`, in `Message`, in `MessageBatch`, and
directly in `SnapshotChunk`. A `Membership` layout change therefore alters the
transport bytes.

> **From 1.0 onward:** bump `proto.WireVersion` whenever any type reachable from
> `MessageBatch` or `SnapshotChunk` changes layout, including the embedded
> persisted ones.

The `Membership` half is enforced at compile time:
`const _ uint64 = WireVersion - uint64(membershipFormatVersion)` fails to
compile if the persisted version ever exceeds the wire version. The converse
does not hold (a transport-only change leaves persisted bytes alone), which is
why the relation is `>=`, not `==`. The two constants stay separate and neither
is derived from the other: deriving the persisted version from the wire version
would change the byte every replica expects at log index 1 on the first
transport-only bump. `pkg/proto/wireversion_test.go` fails the build on that
change.

<a id="4-walformat-marker-version--1--persisted-exact-match-checked-first"></a>

### 4. `WALFORMAT` marker

A 16-byte file in the WAL root: magic `QRWALFMT`, a uint32 format version and
the uint32 block size. It records what a segment does not carry itself, the
block grid and the chunk framing, and is read before any segment byte.

| Version | Chunk header | Status |
|---|---|---|
| 1 | 15 bytes: CRC32, length, type, durability sequence, generation start | current |
| anything else | | refused: `ErrWALFormatUnsupportedVersion` (`*WALFormatVersionError`, naming both versions) |

Two failures, both fatal before any replay:

- **Version mismatch**: `ErrWALFormatUnsupportedVersion`, distinct from
  `ErrWALFormatMalformed` because the file is intelligible and the remedy is an
  upgrade procedure, not a corruption investigation.
- **No marker over a populated directory**: `ErrWALFormatMissing`. Every
  directory this build creates is marked before its first segment, so unmarked
  segments come from a build whose framing this parser cannot read. Adopting
  them would truncate the whole log to zero and report success, so they are
  refused.

An empty directory is marked and opened normally; an empty or all-zero marker
file reads as absent. A marker that cannot be written fails the open, leaving
the directory empty.

A passing marker check is not evidence that a directory matches the binary. The
chunk framing has changed in place under version 1, and a directory written
with older framing passes the marker and then fails every chunk CRC. See the
0.13.0-alpha entry in [Upgrade notes by release](#upgrade-notes-by-release) for
what that does.

### 5. WAL payload-type bytes

Each WAL record starts with a payload-type byte. A retired or unknown byte is a
fatal recovery error (`ErrUnknownPayloadType`), because a stale layout would
otherwise decode into plausible garbage. The ladder covers what is inside a
chunk; the chunk framing is the `WALFORMAT` marker's job.

| Byte | Constant | Status on replay |
|---|---|---|
| `0x01` | retired | fatal: entry format without session fields |
| `0x02` | retired | fatal: 40-byte hard state without `VoteWindow` |
| `0x03` | retired | fatal: snapshot format without `MaxWitnesses` or policy flags |
| `0x04` | `payloadTypeBootstrap` | current |
| `0x05` | `payloadTypeCompact` | current |
| `0x06` | retired | fatal: remove-node tombstone. Removals are kept in the `RETIRED` registry file beside the segments (16 bytes per retired replica) |
| `0x07` | retired | fatal: 76-byte entry header |
| `0x08` | retired | fatal: membership section without a version byte |
| `0x09` | retired | fatal: `FilePathLen` where the current layout reads `Compression` |
| `0x0a` | retired | fatal: three-field session policy tail |
| `0x0b` | `payloadTypeEntries` | current: 84-byte record header with five session fields (`Key`, `ClientID`, `SeriesID`, `RespondedTo`, `OwnerID`) |
| `0x0c` | `payloadTypeSnapshot` | current: artifact `Compression` in the fixed region, four-field session policy tail |
| `0x0d` | `payloadTypeState` | current: `Term`, `Vote`, `Commit`, `VoteWindow` (48-byte record) |

Every fatal error names the remedy: restore from a snapshot, or re-sync from
peers. With every version number pinned, the ladder is the only thing that
detects a record-layout change, which is why a layout change must take a new
byte.

The most common real cause of `ErrUnknownPayloadType` is not version skew but
opening an encrypted WAL with no barrier, which makes every record decode as
type `0x00`.

<a id="6-snapshotmeta--persisted-self-describing-append-only"></a>

### 6. `snapshot.meta`

The record beside every snapshot: a 16-byte header (`QRSNAPMD` magic, uint16
body version 1, reserved uint16, uint32 body length) and a 72-byte body of nine
little-endian uint64 fields (`Index`, `Term`, `ShardID`, `ReplicaID`,
`CreatedAt`, `Epoch`, `Compression`, `DataLen`, `DataHash`): 88 bytes. Because
the body length is in-band, a reader can tell an absent field from corrupt
bytes.

Body fields are append-only: appending one bumps the version and leaves every
offset alone. Changing a field's meaning requires a new magic.

Three pre-magic layouts are still read positionally: 40 bytes (no `Epoch`), 48
(with `Epoch`) and 56 (with `Epoch` and `Compression`). None carries `DataLen`
or `DataHash`, so recovery refuses them (`ErrSnapshotCorrupt`) exactly as it
refuses a missing record; only the DEK purge-floor scan still reads them.

An older binary reads a current record as corrupt. That fails safe (it
suppresses DEK purging), but it means a downgrade is a wipe and restore.

### 7. At-rest snapshot frames

An encrypted snapshot is a sequence of frames whose AEAD context starts with the
`QRSNAP1` magic and is `SnapshotFrameAADLen` (40) bytes, ending in a terminal
flag. The writer always ends with a terminal frame and the reader authenticates
the last frame as terminal, so a snapshot that lost trailing frames, or had
frames spliced after its terminal frame, fails authentication instead of
restoring a shorter state machine. One truncated to nothing is
`ErrSnapshotCorrupt`.

### 8. Exported snapshot artifact

`Host.ExportSnapshot` writes, and `Host.ImportSnapshot` reads, three parts: a
header (`QRSNAP01` magic, uint32 version 2, then `Index`, `Term`, `OnDiskIndex`,
`Compression` and a length-prefixed `Membership`), the plaintext payload, and a
24-byte trailer:

```
magic "QRSNTRLR"[8] | payloadLen:u64 LE | hash:u64 LE (xxhash64 of header || payload)
```

The hash covers the raw header bytes and the payload, so a damaged `Index`,
`Term` or `Membership` that still parses is refused like a damaged payload
byte. The trailer is written last, only after every export check passes, and
found by position (the last 24 bytes); its magic makes that position
self-checking. `ImportSnapshot` verifies while streaming into staging and
refuses a mismatch, a truncation or a missing trailer with
`*SnapshotArtifactError` (a mismatch also matches `ErrSnapshotCorrupt`) before
anything is published, recorded in LogDB or seen by Raft. An artifact without a
trailer is refused; export it again with a current build.

### Sealed root key

`seal.SealedRootKey` carries `Version`, `KDFVersion`, `HKDFVersion` and
`Algorithm`, so a blob unseals correctly after a FIPS-mode change or on a host
whose CPU would auto-select a different AEAD. A zero value means the field was
not written, and the blob loads under the barrier's configured setting.
`SetSealedRootKey` rejects a blob whose `HKDFVersion` is newer than the running
build, and one whose `Algorithm` disagrees with the barrier configuration
(`AlgorithmMismatchError`).

`sealedStateMACVersion` is 2: the metadata MAC covers `LoadGeneration`, because
the barrier refuses to encrypt on the strength of that counter and an
unauthenticated field would be a passphrase-free way to stop a node writing.
`verifySealedStateMAC` still accepts a valid version-1 tag once, logs it, and
restamps at version 2 on the next persist. Accepting it is not a downgrade:
forging either tag needs the same HKDF-derived MAC key. If you persist
`SealedRootKeyData()` yourself, persist it again after the first successful
unseal.

A blob with no MAC at all is refused with `ErrSealedStateUnauthenticated`.
Accepting an absent MAC would be a downgrade an attacker triggers by deleting
the field. Embedders that re-wrap the root key themselves (password change,
rekey, key import, Shamir re-split) must stamp the MAC last, after the
ciphertext: `(*Barrier).StampSealedStateMAC(sealed)` with the barrier unsealed,
or `StampSealedStateMACWithRootKey(rootKey, deploymentID, sealed)` when only the
root key is in hand.

## Interface requirements for embedders

These are satisfied structurally, so a type check alone will not catch a type
that misses them.

### `waldb.FS` requires `Rename`

Any `waldb.FS` (test doubles, instrumented or fault-injecting filesystems)
provides `OpenFile`, `Open`, `MkdirAll`, `ReadDir`, `Stat`, `Remove` and
`Rename` (`pkg/logdb/waldb/fs.go`). `Rename` is required because the
`WALFORMAT` marker is published by writing a temporary file, fsyncing it and
renaming it into place; an in-place fallback would restore the torn state the
rename removes. Delegate to `os.Rename` or model its atomicity.

### Barriers given to `waldb` report `ContextBindingResolved() bool`

`waldb` asserts an unexported interface on the dynamic type of the barrier
passed to `WithBarrier`. A barrier missing the method compiles and fails at
`Open` with `waldb.ErrBarrierContextBindingUnreportable`.

The method reports whether `SupportsContextAAD()` is currently an answer: true
once the barrier is unsealed with its encryptor installed, false while it is
sealed or uninitialised. The WAL resolves the capability once and writes every
record against it, so it must tell "cannot bind" from "cannot say yet".
`*seal.Barrier` implements it.

### `seal.ContextBinding`'s zero value is `Unresolved`

| Constant | Value | Meaning |
|---|---|---|
| `ContextBindingUnresolved` | 0 | no answer yet: sealed or uninitialised. `Binds()` is false |
| `ContextBindingBarrier` | 1 | the barrier folds the caller's context into its own AEAD's AAD |
| `ContextBindingDelegated` | 2 | a delegated hardware or KMS encryptor owns the binding |
| `ContextBindingUnsupported` | 3 | the encryptor cannot bind context |
| `ContextBindingPlaintext` | 4 | not encrypting |

Compare against the named constants and never persist or transmit the numeric
value. A barrier can legitimately start sealed and be unsealed later, so any
consumer that caches the capability re-asks after `Unseal`.

### Custom `Barrier`s implement `EncryptWithEpochPinnedContext`

Sealing a snapshot frame and pinning its epoch must happen under one lock;
otherwise `Rotate` plus `PurgeEpochsBefore` could drop the DEK of a frame about
to be persisted. There is no fallback for a barrier without it.

### `seal.NewBarrier` returns `(*Barrier, error)`

An invalid configuration (unknown algorithm, non-approved cipher under FIPS
mode) is an error, never a panic, because such configurations come from files,
API requests and per-tenant settings.

| Constructor | Returns | Use when |
|---|---|---|
| `seal.NewBarrier(cfg)` | `(*Barrier, error)` | configuration assembled at run time |
| `seal.MustNewBarrier(cfg)` | `*Barrier`, panics | configuration from compile-time constants |
| `seal.NewEpochBarrier` / `seal.MustNewEpochBarrier` | aliases | the same functions under the epoch-barrier name |
| `crypto.NewBarrier` / `crypto.MustNewBarrier` | aliases | `pkg/crypto` re-exports |

## Node identities are mandatory

`msg.From`, the replica ID Raft counts toward quorum, travels in-band and is
not authenticated by mTLS. Committed membership therefore records a node
identity per replica, and it is the only thing the receive path checks. The
identity is `sha256(leaf.RawSubjectPublicKeyInfo)`, rendered `spki:<hex>`: a
hash of the node's public key, unchanged by certificate renewal on the same
keypair. Binding by address was rejected: a wildcard or multi-SAN certificate
covers every member's address, two replica IDs at one address give one host two
votes, and a member that moves address loses its votes until a config change
commits, which cannot happen during quorum loss.

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

Plan for these consequences:

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

### Bootstrapping and growing a cluster

1. **Collect** `Host.NodeIdentity()` on each node, with its replica ID. It is a
   hash of a public key and safe to log and store. It cannot be learned from
   the peer's own connection, since that would let a promiscuous-certificate
   holder register itself as any replica; the operator or provisioning system
   supplies it.
2. **Found** with an identical `Config.MemberIdentities` on every founding
   member. The bootstrap entry at index 1 is built from it, and founders that
   disagree produce divergent entries Log Matching never reconciles.
3. **Grow** with the identity:

   ```go
   err := leader.SyncRequestAddNodeWithIdentity(ctx, shardID, replicaID,
       addr, identity, membership.ConfigChangeID)
   ```

   Start the new replica with `join=true` and a `Config.MemberIdentities`
   naming at least the current leader, or every member if leadership may move
   before the join completes. Use `RequestAddObserverWithIdentity` and
   `RequestAddWitnessWithIdentity` for the other roles, and take a fresh
   `ConfigChangeID` from `SyncGetShardMembership` before each call.
4. **Verify** with `SyncGetShardMembership`, and alert on the unauthorized-drop
   counter with reason `identity_mismatch`: a committed identity that does not
   match the peer's keypair is a stale identity or a forgery, never routine.

### Identities and session policy across a restart

Identities and the session policy travel through the log (the index-1
`EntryInitialMembership` and every config change), so a replica whose log was
compacted past those entries relearns them from the snapshot-metadata record.
That record carries both end to end:

| Layer | `Identities` | Session policy |
|---|---|---|
| `logdb.Membership` | yes | yes |
| `memdb` | yes (deep copy) | yes |
| `waldb` snapshot-record membership section | yes | yes, as a tail after `Identities` |
| `host.go` `protoMembershipToLogDB` / `logdbMembershipToProto` | yes | yes |
| `engine` `protoMembershipToLogDB` (own snapshot save) | yes | yes |
| message handler (received `InstallSnapshot`) | | yes |

A dropped field here would come back empty with no error. For
`RemovedIdentities` that is a security regression: a decommissioned node with a
still-valid certificate would be readmitted on the restart after its removal.
A replica that loses the session policy falls back to its own `Config` and
expires sessions at log indexes its peers do not. The shared LogDB conformance
suite (`pkg/logdb/conformance_test.go`) runs `memdb` and `waldb` through one
assertion table and reopens the database between save and read, because `waldb`
answers reads from an in-memory map a save fills directly, which would hide an
encoding that dropped the fields.

When the committed membership carries no session policy (`SessionPolicy.IsSet`
is false), each replica applies its own `Config`. Set `SessionExpiryEntries`,
`MaxTotalSessions` and `MaxSessionsPerClient` identically on every replica in
that case; nothing enforces it, and disagreement diverges the state machine
silently.

## Upgrade path

There is none beyond wipe and re-sync. Nothing in the codebase migrates an old
data directory, and legacy formats fail closed.

Across a boundary that changes an on-disk format, per node:

1. Confirm the remaining nodes hold a quorum without this one.
2. Stop the node.
3. Delete its `WALDir` and `NodeHostDir` contents, including the `WALFORMAT`
   marker (segments left without it are refused with `ErrWALFormatMissing`).
   Keep the sealed root key unless the release notes list an encryption break.
4. Start the node on the new version and let it re-sync from the leader by
   snapshot and log replay.
5. Wait for `ShardInfo.Health` to leave recovering and `LastApplied` to catch
   up before moving to the next node.

This is a rolling replacement, not a rolling upgrade: a new node and an old node
should never exchange traffic. Across a pre-release layout change both binaries
stamp the same versions, so nothing refuses the skew for you. Expect reduced
redundancy for the duration, and never take down a majority at once.

For a whole-cluster cold upgrade, stop everything, wipe everything, and restore
from an application-level backup or from snapshots exported under the old
version and imported under the new one. `waldb.Reencrypt` re-serialises every
record for a barrier migration; it is not a format migration tool.

Across a boundary with no format change, replace the binaries; data directories
carry over. Still build every node and consuming repository from one commit.

### Upgrade notes by release

Read the CHANGELOG `BREAKING` sections between your version and the target;
this table summarises them.

| Target | Data directories | What changes for an embedder |
|---|---|---|
| **0.16.0** | carry over from 0.16.0-alpha (identical code) | release candidate; nothing beyond 0.16.0-alpha |
| **0.16.0-alpha** | carry over from 0.15.x; no format changed | No API removed. `ConcurrentStateMachine` and `DiskStateMachine` now see `Lookup` and `GetHash` concurrently with `Update`, as documented: synchronise them and hash whole batches in `GetHash`. `UnmarshalShareSet` returns `*ProtectedMemoryError` when the kernel refuses a lock. A replica's observer role follows committed membership, so a promoted observer becomes a voter without a restart. Added `ErrRemovedMemberIdentity`. |
| **0.15.0-alpha** | **wipe**: hard-state record moved `0x02` → `0x0d` (adds `VoteWindow`); `0x06` retired | `logdb.LogDB` gains `NodeRemoved`. `RemoveData` retires the replica ID for good; a replica that rejoins needs a new ID. Exported artifacts carry a trailer; artifacts from earlier builds are refused, so re-export them. |
| **0.14.1-alpha** | carry over from 0.14.0-alpha | fixes only |
| **0.14.0-alpha** | **wipe**: at-rest snapshot frame context grew 39 → 40 bytes in place, so 0.13.x encrypted snapshots no longer authenticate | `NewHost` requires `WithBarrier`, `WithShardBarriers` or `WithoutEncryption()` (`ErrAtRestEncryptionUnspecified`, `ErrAtRestEncryptionConflict`). A custom `Barrier` must implement `EncryptWithEpochPinnedContext`. |
| **0.13.1-alpha**, **0.13.2-alpha** | carry over from 0.13.0-alpha | test fix; dependency updates |
| **0.13.0-alpha** | **wipe**: WAL chunk header grew 11 → 15 bytes in place | Identities required at admission (see [Node identities](#node-identities-are-mandatory)); `TransportConfig.RequireMemberIdentity` removed. Every inbound message bound to a replica the receiver can vouch for. `WithMaxFileSize` above 4 GiB − 1 is `ErrInvalidMaxFileSize`. `logdb.RecoveryResidual`, `logdb.RecoveryResidualReporter` and `waldb.DB.RecoveryResiduals` removed. `ProposeWithSession` serves one proposal per session at a time (`ErrSessionBusy`); a session at series 0 is `ErrInvalidSession`. |
| **0.12.0-alpha** | carry over | no breaking change |
| **0.11.0-alpha** | carry over | `pkg/enclave` deleted: use `go-enclave` (`enclave.MemoryEnclave`, `enclave.SealedEnclave`) directly. `seal.Credentials.Secret` is a `*enclave.SealedEnclave`. `shamir.MaxShareSetBytes` is a `var`, not a `const`. |
| **0.10.0-alpha** | carry over | no breaking change |
| **0.9.0-alpha** | carry over; a stored Shamir init result must be re-created | `ShareSplitter` and `ShareAccumulator` removed in favour of the protected forms; `ShamirInitResult.Shares` is `[]*shamir.ProtectedShare` and its JSON `"shares"` is an array of share objects. `seal.StorageBackend` gains `CompareAndDelete`. |
| **0.8.0-alpha** | carry over | no breaking change |
| **0.7.0-alpha** | carry over | A `cipher.AEAD` returned by `pkg/seal` reports `NonceSize() == 0` and generates its own nonce; this package's own frames are unaffected. |
| **0.6.x** | **wipe** from 0.5.x: WAL payload types renumbered (`0x07` → `0x0b`, `0x0a` → `0x0c`) | `HostConfig.MaxApplyRetries`, `engine.CircuitBreakerError` and `proto.Bootstrap` removed; the revocation checker's enforcement field renamed and inverted. |

The 0.13.0-alpha chunk-header change is the dangerous kind. A 0.12.x directory
opened by 0.13.0-alpha or later is **not refused**: the marker passes, every
chunk fails the new CRC, which reads as a torn first batch, and recovery
truncates the segment to zero and opens with an empty log and no hard state.
Term and vote regress, so a quorum member upgraded in place can vote twice in
one term. Stop the node, wipe its WAL and NodeHost directories, upgrade and
re-sync from peers, one replica at a time.

<a id="what-05x-does-when-it-meets-04x-data"></a>

### Releases before 0.6.0

All are wipe and re-sync; none has a migration.

| Boundary | Nature |
|---|---|
| 0.4.x → 0.5.x | Layouts grew in place: `proto.Entry` header 60 → 68 bytes (`OwnerID`), WAL entry record header 76 → 84, `proto.SessionPolicy` 24 → 32 (`MaxSessionsPerOwner`), `proto.Membership` fixed region 49 → 57, `waldb` minimum membership section 66 → 74. `proto.Snapshot` lost `Type`, `Imported`, `Witness` and gained `Compression`; `snapshot.meta` gained `DataLen` and `DataHash`; the state MAC moved to version 2 (migrates itself). `0.5.x` still used payload types `0x07`/`0x0a`, so it reads 0.4.x records without refusing them and misparses them; do not run 0.5.x against 0.4.x data. |
| 0.3.1 → 0.4.0 | Every format version pinned at 1. The `WALFORMAT` marker went 2 → 1, so a pre-0.4 WAL is refused with `*WALFormatVersionError`. `Membership` grew `RemovedIdentities`; `ConfigChange` stopped being length-discriminated; `Snapshot.Checksum` removed. |
| 0.3.0 → 0.3.1 | CI and test infrastructure only; formats unchanged. |
| 0.2.0 → 0.3.0 | Four on-disk formats changed. Symptoms: `ErrWALFormatMissing`, `ErrUnknownPayloadType`, and `ErrDecryptionFailed` on every read (at-rest AAD binding changed). |

Changes from 0.4.0 that compile but fail at run time on a jump from 0.3.x:

| Symptom | Meaning | Remedy |
|---|---|---|
| `ErrSealedStateUnauthenticated` on first unseal | the sealed root key predates the state MAC | set `BarrierConfig.AllowUnauthenticatedSealedState = true`, start once, re-persist `SealedRootKeyData()`, clear the flag |
| `ErrSessionsUnsupportedOnDisk` | client sessions on an `sm.DiskStateMachine` shard; the session table lives only in the snapshot stream, which an on-disk state machine never recovers from on restart | use no-op sessions (`Host.GetNoOPSession`) and idempotent operations |
| `ErrKDFNotFIPSApproved` on unseal | an Argon2id-wrapped blob under FIPS mode | unseal once with FIPS off, re-seal with `NewSoftwareStrategyFIPS` (PBKDF2-SHA256) |

### Before upgrading

- Read the `BREAKING` section of every CHANGELOG entry between your version and
  the target.
- If you need a restore point, export a snapshot per shard
  (`Host.ExportSnapshot(ctx, shardID, w)`) while still on the old version.
  `ImportSnapshot` on a fresh disaster-recovery host requires
  `WithImportDataDir`; see
  [Back up and restore](multi-group-host/user.md#back-up-and-restore).
  Artifacts must carry the trailer, so export with a build that writes one.
- Start the target version against an empty data directory in staging first. A
  format break shows up at open time.
- Rebuild every consuming repository from the same commit: a repository built
  against the old layout writes bytes this build cannot parse while stamping
  the same version.

## Downgrades

Not supported at any boundary. A downgrade is a wipe and a restore from a backup
taken on the older version. An older binary reads a current `snapshot.meta` as
corrupt and fails closed on a payload-type byte it does not know; anything else
it accepts by version and then misreads.

## Cross-references

- [Deployment](multi-group-host/user.md): configuration, directory layout, disaster recovery
- [Lifecycle](multi-group-host/engineering.md): startup, crash recovery, WAL replay, shutdown
- [Wire format](wire-protocol/engineering.md): byte-level layouts of every proto type
- [WAL persistence](log-storage/engineering.md): segment and chunk framing, the `WALFORMAT` marker
- [Encryption](seal-barrier/architecture.md): barrier, epochs, sealed root key, AAD binding
- [Membership changes](membership-changes/user.md): node identities in practice
- [API reference](multi-group-host/api.md): exported surface and error types
