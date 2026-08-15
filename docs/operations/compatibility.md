# Compatibility and Upgrades

What this library promises across versions, what it does not, and what an
upgrade actually involves today.

**Sources:** `pkg/proto/constants.go`, `pkg/proto/membership.go`,
`pkg/internal/transport/frame.go`, `pkg/logdb/waldb/db.go`,
`pkg/internal/engine/snapshot_meta.go`, `pkg/seal/strategy.go`, `CHANGELOG.md`

## Stability guarantee

go-quicraft is **pre-1.0**. Under semantic versioning that means the minor
version may break anything, and it does: `0.2.0-alpha` broke the on-disk format,
and `0.3.0-alpha` breaks it again (positional AEAD binding, a new
`snapshot.meta` layout, and an 11-byte WAL chunk header carrying a durability
sequence).

Concretely, before 1.0:

| Surface | Guarantee |
|---|---|
| Exported Go API (`pkg`, `pkg/config`, `pkg/sm`, `pkg/seal`, `pkg/logdb`) | **None between minor versions.** Signature and semantic changes are called out in CHANGELOG under BREAKING |
| On-disk format (WAL chunk framing, WAL records, snapshots, `snapshot.meta`, sealed root key) | **None between minor versions.** A format break means wipe-and-resync — see [Upgrade path](#upgrade-path) |
| Wire format between nodes | Exact-match only. Mixed-version clusters are not supported at any version |
| Patch releases (`0.2.0` → `0.2.1`) | No intentional API or format break. Read the CHANGELOG anyway |

There is no long-term-support line, no deprecation window, and no compatibility
shim. Downstream products (go-qrdb, go-xkms, go-quicbaas, go-trusted-ca) pin an
exact version and upgrade deliberately.

If a v2+ line is ever cut, the module path takes the `/v2` suffix as Go
requires; the release workflow refuses a `v2+` tag without it.

## Format versions are PINNED AT 1 until the first production release

**Every wire and persisted format in this repository is pinned at version 1 for
the whole pre-release period, and every decoder gates on exact equality with
1.** That covers `proto.WireVersion`, the `proto.Membership` section version
byte, `transport.WireVersion` (the frame header) and `waldb`'s snapshot-record
membership section version.

The policy, stated as a rule:

- A layout change is made **IN PLACE, at version 1**. The number does not move.
- The consuming repositories are updated **in lockstep**, in the same change, to
  read and write the new layout at the same version.
- Development clusters are **wiped and re-synced**, never migrated. There is no
  dual-read path, no migration window and no legacy decode case, because there
  is no deployed data and no deployed peer whose bytes anyone has promised to
  keep reading.
- The version numbers increment **for the first time at the first production
  release**. From that point a bump means what it normally means: a decoder
  taught the old layout, a migration story, and a supported window that widens
  rather than moves.
- The transport version remains a **supremum** over the persisted ones — see
  [The transport version is a SUPREMUM](#the-transport-version-is-a-supremum).
  That relation is asserted at compile time and holds trivially while both are
  1; it is retained because it is what stops the two drifting apart at the 1.0
  bump.

**State the consequence plainly: two binaries built from different pre-release
commits may be mutually unreadable while BOTH stamp version 1, and that is
intended.** Mixing them — over the wire, or over a data directory one of them
wrote — is unsupported, and the failure it produces is a decode or recovery
error rather than a version mismatch that names itself. Rebuild every node, and
every consuming repo, from the same commit.

Why the version byte is kept even though the value never moves: it is the
mechanism that makes the 1.0 bump possible at all. A format with no in-band
discriminator cannot be superseded — that is precisely the state `Membership`
was in before it grew a leading byte, and a section without one begins at
`ConfigChangeID`, whose low byte is indistinguishable from a version. Keeping
the byte and refusing anything that is not 1 also turns a commit skew into a
clean refusal instead of fields read at the wrong offsets.

Every version number in the repository is now 1, the `WALFORMAT` marker
included; see
[section 4](#4-walformat-marker-version--1--persisted-exact-match-checked-first).

## Version discriminators that actually exist

There is no single "format version" for the system. Six independent
discriminators exist, each scoped to what it protects. The scoping is
deliberate: a transport-only bump must never make a node unable to replay its
own log. The four that are version NUMBERS — sections 1 to 4 — are all pinned at
1 by the policy above. The remaining two are not numbers of that kind: the WAL
payload-type ladder is a per-record type tag, and `snapshot.meta` is
self-describing.

### 1. `proto.WireVersion` = 1 — transport-scoped, exact match

Stamped into `MessageBatch.BinVer` and `SnapshotChunk.BinVer`. Both ends of a
QUIC connection are live processes, so a mismatch costs one rejected connection
and nothing durable. It is gated on **exact equality**: a peer speaking another
dialect is refused outright rather than misparsed, and the operator upgrades
both ends. There is no supported window and no negotiation.

**It does not move for a pre-release layout change.** The transport layout HAS
changed during development and will change again; under the pinning policy the
number stays at 1 and every peer is rebuilt. So a `BinVer` match is not a
guarantee that two binaries agree on the bytes — it is a guarantee only that
neither is from a different major format generation. Build every node from the
same commit.

**Operational consequence:** a rolling upgrade across a pre-release layout
change does not work, and it does not announce itself as a version mismatch
either. Nodes from different commits either fail to decode each other's batches
or, in the worst case for a change that preserves lengths, disagree about what
a field means. Stop the cluster, replace every binary, wipe, re-sync.

From 1.0 onward the number carries its normal meaning and a bump names a real
dialect boundary.

### 2. `transport.WireVersion` = 0x01 — transport-scoped frame header

The third byte of every QUIC frame, after the two magic bytes `Q` `C`. Same
scope and same reasoning as above; it is versioned separately because the frame
header can change without the payload encoding changing, and vice versa. It has
been 1 throughout and stays there.

### 3. `Membership` section version byte = 1 — persisted, exact match

The leading byte of every `Membership` section. Membership is persisted: it goes
into the WAL, into snapshot records, into exported snapshot artifacts, and into
the `EntryInitialMembership` entry at log index 1. Those bytes outlive the
process — and the binary — that wrote them, which is why the format carries an
explicit discriminator at all: a `Membership` is always EMBEDDED in a larger
buffer, so neither a length check nor a trailing-byte check can tell one layout
from another.

The decoder accepts **exactly 1** and refuses every other byte value with
`*proto.UnsupportedMembershipVersionError` (which matches
`proto.ErrUnsupportedVersion` under `errors.Is`), before a single field is
interpreted. There is no range and no per-version decode path: pre-release, no
bytes in another layout are meant to exist anywhere, because dev clusters are
wiped rather than migrated.

The v1 layout, field by field:

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | Version byte (`1`) |
| 1 | 8 | `ConfigChangeID` |
| 9 | 8 | `MaxWitnesses` |
| 17 | 8 | `RequireFullReplicaMajority` (bool as uint64) |
| 25 | 8 | `SessionExpiryEntries` |
| 33 | 8 | `MaxTotalSessions` |
| 41 | 8 | `MaxSessionsPerClient` |
| 49 | var | `Addresses` map |
| — | var | `Observers` map |
| — | var | `Witnesses` map |
| — | var | `Removed` bool map |
| — | var | `Identities` map |
| — | var | `RemovedIdentities` map |

The fixed region is **49 bytes** and an empty section is **73**: the fixed
region plus six 4-byte map count prefixes. Every map is written unconditionally,
so an empty map is four zero bytes rather than an absent field, and an unset
session policy is 24 zero bytes rather than an absent one.

An unset session policy is still distinguishable from a committed one that
disables expiry: `MaxTotalSessions` can never legitimately be zero in a
committed policy (`config.Validate` rejects it), so it is the "policy recorded"
discriminator (`SessionPolicy.IsSet`) and a threshold of zero keeps its own
meaning — expiry explicitly DISABLED cluster-wide.

`Identities` is the map of replica ID to stable node identity that the
receive-side sender gate authorizes `msg.From` against. `RemovedIdentities` is
the node identity every replica the shard has REMOVED held at the moment of its
removal: the removal deny-list has to be answerable against the AUTHENTICATED
peer identity rather than the CLAIMED `msg.From`, because a removed replica
still holds its private key and so still authenticates. Its entries are never
evicted — no eviction policy exists that is not also a re-admission policy. Both
maps are SPARSE: a shard whose members carry no committed identity records
nothing in either, and its deny-list remains a check on the claimed `msg.From`.
See [Node identities](#node-identities-and-requirememberidentity).

`proto.ConfigChange` carries no version byte, and needs none — but for the
opposite reason it used to. It has exactly ONE layout and every field in it is
mandatory, so there is nothing to discriminate: a payload that does not span the
whole layout is a truncation and errors with `ErrBufferTooSmall`, at every cut
point without exception.

It used to be LENGTH-discriminated, and that was a defect rather than a
convenience. A payload stopping after the witness-policy fields decoded
successfully with an EMPTY `NodeIdentity` — which is precisely the state that
falls back to the weaker address-based authorization — so a truncated config
change was indistinguishable from an identity-less one and silently downgraded
an authorization check. An identity-less change is now spelled with an explicit
zero-length prefix, which is a value a writer chose rather than a length a
reader inferred.

`waldb` pins its own `membershipSectionVersion` — also **1**, also exact-match
on replay — for the membership section inside its snapshot records. Its layout
is NOT byte-identical to `proto.Membership`: it packs the policy flags into one
byte where proto spends eight, and carries the session policy as a **tail**
after `Identities` rather than in its fixed region, because its decoder parses
straight-line from a fixed offset and a grown fixed region would move every map.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | Version byte (`1`) |
| 1 | 8 | `ConfigChangeID` |
| 9 | 8 | `MaxWitnesses` |
| 17 | 1 | Policy flags (`RequireFullReplicaMajority`) |
| 18 | var | `Addresses` map |
| — | var | `Observers` map |
| — | var | `Witnesses` map |
| — | var | `Removed` map |
| — | var | `Identities` map |
| — | 24 | Session policy: `SessionExpiryEntries`, `MaxTotalSessions`, `MaxSessionsPerClient` |
| — | var | `RemovedIdentities` map |

Its fixed region is **18 bytes** and an empty section is **66** — which is also
the read floor replay enforces before it looks at the version byte. The two
numbers are equal by construction: one layout, every field unconditional, so the
smallest thing that may be READ is exactly the size of what is WRITTEN.

#### The transport version is a SUPREMUM

Independent scoping does not mean the two namespaces are unrelated. `Membership`
is embedded in `Snapshot`, which is embedded in `Message`, which is embedded in
`MessageBatch` — and it is embedded directly in `SnapshotChunk`. Both of those
gate on exact equality, so a `Membership` layout change alters the transport
bytes without altering the discriminator that is supposed to describe them.

> **The rule, from the first production release onward:** bump
> `proto.WireVersion` whenever ANY type reachable from `MessageBatch` or
> `SnapshotChunk` changes layout, **including the embedded persisted ones**. The
> transport number is the supremum of every layout version on the wire, never a
> number that tracks only the transport-only fields.

The `Membership` half is mechanical rather than remembered: a compile-time
constant assertion enforces `proto.WireVersion >= membershipFormatVersion`, so
bumping the persisted Membership layout without bumping the wire version fails
the build. It holds trivially at `1 >= 1` and is kept for exactly that future
bump. The rest is judgement.

The converse does **not** hold. `WireVersion` may move without the persisted
version moving, because a transport-only change (a new `SnapshotChunk` field,
say) leaves every persisted byte alone. That asymmetry is why the assertion is
`>=` and not `==`.

The two constants stay SEPARATE even while they hold the same value, and neither
may be derived from the other. If the persisted version were `uint8(WireVersion)`
the first post-1.0 transport-only bump would silently change the version byte
every replica expects at log index 1, and a released cluster could not replay its
own log. A source-level test parses `membership.go` and fails the build on that
"simplification".

### 4. `WALFORMAT` marker version = 1 — persisted, exact match, checked first

A 16-byte file in the WAL root: magic `QRWALFMT`, a uint32 format version and
the uint32 block size. It records the two things a segment file does not carry
in itself — the block grid `ReadAll` strides over, and the CHUNK FRAMING the
records are written in — and it is read before a single segment byte is.

| Version | Chunk header | Status |
|---|---|---|
| 1 | 11 bytes: CRC32, length, type, durability sequence | Current |
| anything else | — | **Refused** — `ErrWALFormatUnsupportedVersion` (`*WALFormatVersionError`, naming both versions) |

Like every other number here it is pinned at 1 for the pre-release period: the
chunk framing HAS changed during development — an earlier 7-byte header carried
no durability sequence — and under the pinning policy that change was made in
place, with the dev directories that held the old framing wiped rather than
migrated.

It is gated on **exact equality** in both directions, and unlike the
payload-type ladder it cannot be a per-record decision: the framing is what
tells you where records begin, so there is no record to inspect until it has
been settled.

Two failure modes, both fatal, both before any replay:

- **Version mismatch.** `ErrWALFormatUnsupportedVersion`. Distinct from
  `ErrWALFormatMalformed` on purpose: the file is perfectly intelligible and
  names a format this build does not read, so the remedy is an upgrade procedure
  rather than a corruption investigation.
- **No marker over a populated directory.** `ErrWALFormatMissing`. Every
  directory this build creates is marked before its first segment exists, so
  segments with no marker above them come from a build that predates it — which
  is also a build that predates the durability sequence. Under this build's
  parser every one of those chunks fails CRC from the first byte, the clean
  prefix comes back empty, and the torn-tail repair would truncate the whole log
  to zero and delete every segment above it. **Silent misparse presented as a
  successful recovery is the one outcome that is not acceptable**, so this is
  refused rather than adopted.

An unmarked but **empty** directory is still marked and opened normally; that is
how a new data directory acquires its format. An empty or all-zero marker file
(the residue of a stamp that never delivered its content) is read as ABSENT,
which means it is adopted over an empty directory and refused over a populated
one. A stamp that cannot be written fails the open, leaving the directory empty
rather than creating segments whose format nothing records.

**There is no compatibility window here and no migration.** Wipe the data
directory and re-sync the node; see [Upgrade path](#upgrade-path).

### 5. WAL payload type bytes — persisted, fatal-on-replay ladder

Each WAL record is prefixed by a payload-type byte. Legacy formats are not
upgraded, tolerated, or best-effort parsed: encountering one during replay is a
**fatal recovery error**, because every one of them would misparse silently
under the current layout.

This ladder is scoped to what is INSIDE a chunk. The chunk framing around it is
versioned by the `WALFORMAT` marker above, and the two do not overlap: a record
whose framing this build cannot parse never reaches a payload-type check at all.
The chunk-framing change that introduced the durability sequence therefore adds
no entry here — no payload layout changed — and the marker version, not a new
type byte, is what detects it.

| Byte | Constant | Status on replay |
|---|---|---|
| `0x01` | `payloadTypeEntriesV1` | **Fatal** — `ErrLegacyEntryFormat`. No session fields, so replay would disable at-most-once delivery |
| `0x02` | `payloadTypeState` | Current |
| `0x03` | `payloadTypeSnapshotV1` | **Fatal** — `ErrLegacySnapshotFormat`. No `MaxWitnesses`/policy flags and no version discriminator |
| `0x04` | `payloadTypeBootstrap` | Current |
| `0x05` | `payloadTypeCompact` | Current |
| `0x06` | `payloadTypeRemoveNode` | Current |
| `0x07` | `payloadTypeEntries` | Current entry format (76-byte header, four session fields) |
| `0x08` | `payloadTypeSnapshotV2` | **Fatal** — `ErrLegacySnapshotFormatV2`. Membership section starts at `ConfigChangeID`, so V3 parsing would eat its low byte as a version byte |
| `0x09` | `payloadTypeSnapshotV3` | **Fatal** — `ErrLegacySnapshotFormatV3`. `FilePathLen` sits where the current layout reads `Compression` |
| `0x0a` | `payloadTypeSnapshot` | **Current snapshot format** — adds artifact `Compression` to the fixed region |

Every fatal error names the same remedy: **restore from a snapshot, or re-sync
this node from its peers.** Old byte values stay reserved forever so a legacy
WAL is *detected* rather than misparsed.

### 6. `snapshot.meta` — persisted, self-describing, append-only

The record beside every snapshot artifact. Current records carry a `QRSNAPMD`
magic, a uint16 body version (**1**), a reserved uint16 and a uint32 body length
ahead of the fields, for 72 bytes total. Because the body length is recorded, a
reader can distinguish a field that is ABSENT from bytes that are CORRUPT.

Body fields are **append-only**: appending one bumps the version and leaves
every existing offset alone. Changing an existing field's meaning is not
permitted under this rule — that requires a new magic.

Compatibility here is genuinely one-way and needs no migration step. Three
pre-magic ("v0") layouts are still read positionally at their original offsets:
40 bytes (receive/import paths, no `Epoch`), 48 bytes (save path, with `Epoch`)
and 56 bytes (save path, with `Epoch` and `Compression`). Nothing rewrites an
existing record; a legacy directory keeps its record until the snapshot rolls
off.

The reverse does not hold: **an older binary reads a current record as corrupt.**
That is fail-safe where it lands (it suppresses DEK purging) but it means a
downgrade must be treated as a wipe-and-restore.

### Sealed root key

`seal.SealedRootKey` carries its own `Version`, plus `KDFVersion`,
`HKDFVersion` and `Algorithm`. These let a blob be unsealed correctly after a
FIPS-mode change or on a host whose CPU would auto-select a different AEAD; a
zero value means "written before the field existed" and loads under the
barrier's configured setting. `SetSealedRootKey` rejects a blob whose
`HKDFVersion` is newer than the running build, and one whose `Algorithm`
disagrees with the barrier configuration (`AlgorithmMismatchError`).

## Breaking for embedders (`0.3.0-alpha`)

These break code that COMPILES against the previous release. They are listed
separately from the format breaks above because a Go type check will not catch
them: two are satisfied structurally at runtime, and one is a constant whose
numeric value moved. Each fails at node startup, not at build time.

### `waldb.FS` gained `Rename(oldpath, newpath string) error`

Any type implementing `waldb.FS` — test doubles, instrumented filesystems, the
error-injection harnesses downstream products build — must add `Rename`. Passing
one to `waldb.Open` via `WithFS` will not compile until it does.

`Rename` is required rather than optional because the WAL geometry marker
(`WALFORMAT`) is published by writing a temporary file, fsyncing it, and renaming
it into place. That sequence is what makes a partially-written marker
unobservable; an optional interface with an in-place fallback would restore
exactly the torn state the rename removes. Delegate to `os.Rename` (or model
rename's atomicity) and the requirement is satisfied.

### The barrier interface `waldb` asserts gained `ContextBindingResolved() bool`

`waldb` asserts an unexported interface on the DYNAMIC type of the barrier passed
to `WithBarrier`. A barrier written against the previous release still compiles,
then fails at `Open`.

The method must report whether `SupportsContextAAD()` is currently an ANSWER
rather than the absence of one — true whenever the barrier is unsealed with its
encryptor installed, false while it is sealed or uninitialized. The WAL resolves
the capability once and writes every record of the process against it, so it must
be able to tell "cannot bind" apart from "cannot say yet"; latching the second as
the first silently downgrades every record a node that starts sealed ever writes.

A barrier that implements the rest of the context-encryption surface but not this
method gets `waldb.ErrBarrierContextBindingUnreportable`, which names the missing
method, rather than the generic `ErrBarrierContextUnsupported` (which would be
false — such a barrier does support context-bound encryption).

`*seal.Barrier` implements it already; only out-of-tree barriers are affected.

### `seal.ContextBinding` constants shifted by one

`ContextBindingUnresolved` was inserted as the new zero value, so every other
constant moved up:

| Constant | Was | Is |
|---|---|---|
| `ContextBindingUnresolved` | — | 0 |
| `ContextBindingBarrier` | 0 | 1 |
| `ContextBindingDelegated` | 1 | 2 |
| `ContextBindingUnsupported` | 2 | 3 |
| `ContextBindingPlaintext` | 3 | 4 |

Compare against the named constants, never the numbers, and do not persist or
transmit the numeric value. Code that stored a `ContextBinding` as an integer
will read every value back as the wrong binding.

The semantic change matters as much as the renumbering: a sealed or uninitialized
barrier used to report `ContextBindingBarrier`, so `Binds()` was **true** for it.
It now reports `ContextBindingUnresolved` and `Binds()` is **false**. Any consumer
that caches the capability must re-ask after `Unseal` instead of latching what a
sealed barrier said.

**Ship together.** The `waldb` guard that refuses a sealed barrier
(`ErrBarrierContextUnresolved`) closes a hole the `ContextBindingUnresolved` state
opens. Reverting `pkg/seal/barrier_binding.go` on its own, with that guard in
place, makes every encrypted WAL refuse to open.

### `seal.NewBarrier` returns `(*Barrier, error)`

It used to return `*Barrier` and PANIC on an invalid configuration — an unknown
algorithm, or a non-FIPS-approved cipher while FIPS mode is active. A panic in an
exported constructor takes down the embedding process, and those two conditions
are exactly what a configuration read from a file, an API request, or per-tenant
settings produces. Calling `BarrierConfig.Validate` first was a convention, not a
guarantee.

This one DOES fail at build time, unlike the three above.

| Call site | Change |
|---|---|
| Config assembled at runtime | `b, err := seal.NewBarrier(cfg)` — surface the error |
| Config from compile-time constants | `b := seal.MustNewBarrier(cfg)` — panics, `regexp.MustCompile` precedent |
| `seal.NewEpochBarrier` | Alias of `NewBarrier`; changes signature with it. `MustNewEpochBarrier` added |
| `crypto.NewBarrier` | Re-exports the new signature. `crypto.MustNewBarrier` added |

`BarrierConfig.Validate` is unchanged and stays exported, but it is now an
admission-time convenience rather than a safety requirement: `NewBarrier` applies
the same rules and returns the same error identity, so forgetting it no longer
risks a panic.

**Known downstream call sites:** `go-qrdb/pkg/seal/barrier.go` (inside
`NewStorageBarrier`, which already returns an error — the new error threads
straight through) and `go-qrdb/pkg/seal/strategy.go`, whose
`var NewEpochBarrier = quicraftseal.NewBarrier` re-export changes signature with
it and so breaks go-qrdb's own API surface. go-xkms and go-quicbaas are
unaffected: their `seal.NewBarrier` is their own package's four-argument
function, and neither imports `go-quicraft/pkg/seal`.

## Node identities and `RequireMemberIdentity`

### What changed

`msg.From` — the replica ID Raft counts toward election and commit quorum —
travels in-band and is not authenticated by mTLS. The receive-side gate used to
bind it by requiring the peer's verified certificate to cover the address
COMMITTED MEMBERSHIP assigns that replica. That closed registry poisoning and
left three holes:

- A **promiscuous certificate** — a wildcard SAN (`*.raft.example.com`), or one
  certificate carrying every node's SAN — covers every member's address, so its
  holder satisfied the check for any replica it named.
- **Two replica IDs at one address** gave the single host behind it two quorum
  identities.
- **Address is not identity**: a member that recovered at a NEW address had its
  quorum-bearing responses dropped until a config change committed, and during
  quorum loss no config change can commit.

Committed membership now records a **node identity** per replica, and where one
is present it is the only thing checked. The identity is
`sha256(leaf.RawSubjectPublicKeyInfo)`, rendered `spki:<hex>` — a hash of the
node's PUBLIC KEY, so only the holder of the matching private key can present
it, and it is unchanged by certificate renewal on the same keypair.

Three consequences an operator should plan around:

1. A wildcard certificate no longer lets its holder assert another member's
   `msg.From`, for any replica whose identity is committed.
2. A member with a committed identity can move to a new address and keep its
   votes and acks counted, with no config change. The liveness cost the address
   binding carried is gone for migrated members.
3. Inbound connections whose certificate ALSO covers this node's own host are
   now refused at the transport, before a stream is opened. Peers that share
   this node's machine (same host, or any loopback address) are exempt, because
   no certificate policy can separate them. **A cluster running one wildcard or
   shared-SAN certificate across DISTINCT hosts will stop connecting** — that is
   the intended effect, and it is the same precondition `StartShard` has always
   asserted about the local certificate.

### Migrating an existing cluster

Existing clusters have no committed identities and keep working unchanged: a
replica with no recorded identity falls back to the committed-address check.
`RequireMemberIdentity` defaults to **false** for exactly that reason.

The migration is per-replica and needs no downtime:

1. **Collect.** On each node, read `Host.NodeIdentity()`. It is a hash of a
   public key — safe to log, print, and put in configuration management. Record
   the replica ID it belongs to.
2. **Commit, one replica at a time.** On the shard leader, re-issue the add for
   each existing member with its identity attached:

   ```go
   err := leader.SyncRequestAddNodeWithIdentity(ctx, shardID, replicaID,
       addr, identity, membership.ConfigChangeID)
   ```

   `AddNode` for a replica the configuration already names updates it in place —
   it is not a re-join, and it produces no tombstone. Use
   `RequestAddObserverWithIdentity` and `RequestAddWitnessWithIdentity` for
   observers and witnesses. Take a fresh `ConfigChangeID` from
   `SyncGetShardMembership` before each call.
3. **Verify.** `SyncGetShardMembership` reports the identity for every member.
   Watch the `unauthorized_message_dropped` counter: reason
   `identity_mismatch` means a committed identity does not match the keypair
   the peer is actually presenting, which is either a stale identity or a
   forgery. It is never normal.
4. **Tighten,** once and only once every member of every shard on a host has a
   committed identity: set
   `HostConfig.TransportConfig.RequireMemberIdentity = true` and restart that
   host. See [Enabling `RequireMemberIdentity`](#enabling-requirememberidentity)
   for the order to do it in.

Bootstrapping a NEW cluster skips all of it: pass
`Config.MemberIdentities` to `StartShard`. Every founding member must be given
an IDENTICAL map — the bootstrap entry at log index 1 is built from it, and
founders that disagree produce a divergent entry that Log Matching never
reconciles. This is the same rule that already applies to the members map and to
`Config.Witness`.

### What each posture costs

| `RequireMemberIdentity` | A member WITH an identity | A member WITHOUT one |
|---|---|---|
| `false` (default) | Authorized by identity. Wildcard certificates cannot assert it; address changes are free | Authorized by its committed address. A wildcard-certificate holder can still assert its `msg.From` |
| `true` | Same | **Quorum-bearing traffic dropped** (reason `missing_identity`). Enabling it before every member is migrated costs the shard its quorum |

The default is a compatibility choice, not a security preference. An identity
cannot be inferred or learned — deriving it from the peer's own connection would
let a promiscuous-certificate holder register ITSELF as any replica, which is
worse than no identity at all — so it must be supplied by an operator.
Defaulting to `true` would brick every existing cluster on upgrade and every new
cluster whose operator has not yet met the field, which produces a setting that
gets switched off rather than satisfied.

### Identities across a restart

Identities travel through the log — the index-1 `EntryInitialMembership` entry
and every `EntryConfigChange` — so a replica whose log has been compacted past
those entries has the `logdb` snapshot-metadata record and nothing else to
relearn them from. **That record carries them, end to end**, so a replica that
recovers from a snapshot comes back still bound by identity:

| Layer | Carries `Identities` |
|---|---|
| `logdb.Membership` | Yes |
| `memdb` | Yes, deep-copied |
| `waldb` snapshot-record membership section | Yes, at section version 1 |
| `host.go` `protoMembershipToLogDB` / `logdbMembershipToProto` | Yes, both directions |
| `engine` `protoMembershipToLogDB` (a replica's own snapshot save) | Yes |

The `waldb` section is versioned in its own right, at 1, and gates on **exact
equality** on replay. There is no tail this replayer may skip and no earlier
layout whose absent fields would decode as empty, which changes what a dropped
field looks like: it would come back as an EMPTY identity map with no error
anywhere. For these two maps that is a security regression rather than a
data-loss one — losing `RemovedIdentities` readmits a decommissioned node
holding a still-valid certificate, on exactly the restart an operator performs
after removing it — so the full-fidelity round trip is asserted directly rather
than inferred from a version check.

### Session policy across a restart

The shard's session policy travels the same path and fails the same way. It is
seeded at bootstrap into the index-1 `EntryInitialMembership`, so a replica whose
log has been compacted past that entry has only the snapshot-metadata record to
relearn it from — and a replica that relearns nothing falls back to its own
`Config` and expires sessions at log indexes its peers do not.

| Layer | Carries the session policy |
|---|---|
| `logdb.Membership` | Yes |
| `memdb` | Yes |
| `waldb` snapshot-record membership section | Yes, as a tail after `Identities` |
| `host.go` `protoMembershipToLogDB` / `logdbMembershipToProto` | Yes, both directions |
| `engine` `protoMembershipToLogDB` (a replica's own snapshot save) | Yes |
| `messagehandler` (a received `InstallSnapshot`) | Yes |

The `waldb` placement differs from `proto.Membership`'s deliberately: that
decoder parses straight-line from a fixed offset, so growing its fixed region
would move every map in the section, while appending to the tail does not. Both
placements are pinned by the shared LogDB conformance suite, which runs
`memdb` and `waldb` through one assertion table and reopens the database — a
save/read pair inside one process never reaches `waldb`'s record encoding at all.

A shard bootstrapped before this release has no committed policy, and its
replicas keep applying their own configuration. For those shards the old rule
still holds: set `SessionExpiryEntries`, `MaxTotalSessions` and
`MaxSessionsPerClient` identically on every replica, because nothing enforces
it and disagreement diverges the state machine silently.

This is pinned by the shared LogDB conformance suite
(`pkg/logdb/conformance_test.go`), which runs `memdb` and `waldb` through ONE
set of assertions. Those tests reopen the database rather than reading back
through the live handle, deliberately: `waldb` answers `GetSnapshot` and
`ReadState` from an in-memory map that `SaveSnapshot` fills from the caller's
value, so a save/read pair inside one process never reaches the record encoding
and would pass against a backend that serialized nothing.

### Enabling `RequireMemberIdentity`

Step 4 of the migration above is safe once every member carries a committed
identity. The order still matters:

1. Finish committing an identity for **every member of every shard on the
   host**, and confirm it with `SyncGetShardMembership`.
2. **Restart one host and re-check `SyncGetShardMembership`**, ideally for a
   shard whose log has been compacted past its config changes. This is what
   distinguishes identities that are merely committed from identities that are
   durable, and it costs one restart.
3. Set `RequireMemberIdentity = true` on that host and restart it. Watch
   `unauthorized_message_dropped`: any `missing_identity` means a member on that
   host is not migrated and the flag went on too early. Roll it back rather than
   waiting — the reason is a member whose quorum traffic is being discarded.
4. Repeat per host. There is no need to change every host at once: the flag is a
   local receive-side posture, and a strict host and a permissive host
   interoperate.

The flag is per HOST, but the condition it asserts is per SHARD: a host running
several shards refuses un-migrated members on all of them, so every shard it
hosts must be fully migrated before it is tightened.

## Upgrade path

**There is none beyond wipe-and-resync.** Stating that plainly is the point of
this document: nothing in the codebase migrates an old data directory to a new
format, and the legacy WAL payload types fail closed rather than attempting it.

For a version boundary that CHANGES the on-disk format (every minor release so
far), the procedure per node is:

1. Confirm the remaining nodes hold a quorum without this one.
2. Stop the node.
3. Delete its `WALDir` and `NodeHostDir` contents. Keep the sealed root key
   **only** if the release notes do not list an encryption break; the AAD
   binding change in `0.3.0-alpha` makes existing ciphertext unreadable
   regardless of the key. Deleting the WAL directory means deleting the
   `WALFORMAT` marker with it — leaving segments behind without it is refused
   (`ErrWALFormatMissing`), by design, since their chunk framing is the old one.
4. Start the node on the new version and let it re-sync from the leader via
   snapshot + log replay.
5. Wait for `ShardInfo.Health` to leave `recovering` and `LastApplied` to catch
   up before moving to the next node.

This is a rolling **replacement**, not a rolling upgrade: at no point should a
new node and an old node exchange traffic. Across a genuine `proto.WireVersion`
boundary the gate refuses it outright; across a PRE-RELEASE layout change it may
not, because both binaries stamp version 1 — which is why the procedure wipes
each node's data directory rather than relying on a version check to catch the
skew. Plan for the cluster to run with reduced redundancy for the duration, and
do not start step 2 on the next node until step 5 completes on the previous one.

For a whole-cluster cold upgrade, stop everything, wipe everything, and restore
from an application-level backup taken before the upgrade. `waldb.Reencrypt`
re-serializes every record and is the supported path for a **barrier** migration
specifically — it is not a format migration tool.

### `0.2.0-alpha` → `0.3.0-alpha`

Four independent on-disk formats changed, so a `0.2.0-alpha` data directory is
unreadable in every one of its parts. Follow the rolling-replacement procedure
above. What an operator will actually SEE, so a fail-closed refusal is not
mistaken for corruption:

| Symptom at start-up | Meaning |
|---|---|
| `ErrWALFormatMissing` | WAL segments written before the format marker existed. Expected on every `0.2.0-alpha` directory. Delete the WAL directory. |
| `*WALFormatVersionError` | A marker from another build's on-disk format. Same remedy; the message names both the version found and the version this build reads, so it is distinguishable from damage. |
| `ErrLegacySnapshotFormat` / `...V2` / `...V3` | A WAL snapshot record from an older payload-type generation. |
| `ErrDecryptionFailed` on every read | The at-rest AAD binding changed. The key is fine; the ciphertext is not readable. Wipe, do not hunt for a key problem. |

None of these is recoverable in place and none should be retried. All four are
deliberate refusals — the alternative for each was a silent misparse, and for
the WAL specifically a misparse would have presented as a torn tail and
truncated the whole log to zero.

Source compatibility is a separate question and is better than the format break
suggests: `go-qrdb`, `go-xkms` and `go-quicbaas` build against `0.3.0-alpha`
without source changes. The interface additions in that release's `BREAKING`
section (`waldb.FS.Rename`, `walContextBarrier.ContextBindingResolved`,
`SecurityMetrics.IncUnauthorizedMessageDropped`'s `reason` parameter) affect
only embedders that IMPLEMENT those interfaces. Verify with a build before
concluding you are unaffected.

### `0.3.0-alpha` → `0.3.1-alpha`

`0.3.1-alpha` was a CI and test-infrastructure release. It changed no wire
format, no persisted format and no public API. A `0.3.0-alpha` data directory is
read by `0.3.1-alpha` unchanged, and the two versions interoperate on the wire.

Upgrade in place, node by node, with no wipe. It is the only boundary in this
project's history where that is true.

### `0.3.1-alpha` → next release

**This one is a format break in both directions, and there is no rolling
upgrade.** Follow the rolling-replacement procedure above.

**Every format version was reset to 1 and pinned there** under the policy in
[Format versions are PINNED AT 1](#format-versions-are-pinned-at-1-until-the-first-production-release).
The version numbers therefore do NOT record the break — the layouts changed
underneath them, and both the old and the new binary stamp 1.

| What changed | Consequence |
|---|---|
| `proto.WireVersion` pinned to 1; the transport layout changed | Old and new nodes exchange **no** usable Raft traffic, and the mismatch does not announce itself as a version error. Both ends must be replaced together. |
| `Membership` layout: six maps including `RemovedIdentities`, session policy in the fixed region | An existing log, snapshot or WAL from an earlier commit does **not** replay. Wipe and re-sync. |
| `waldb` membership section: same collapse to version 1, exact-match gate | Same. The read floor rose to the current 66-byte minimum, so a shorter section is a torn record. |
| `Snapshot.Checksum` removed | Transport-layout change. Never computed, never verified — see the `proto.Snapshot` type doc. |
| Write-side limits added | `Membership.MarshalTo` now refuses what its own decoder would reject (`ErrTooManyMembershipEntries`, `ErrAddressTooLong`) instead of producing a durable-but-unreadable bootstrap entry. |

Two changes are **source-compatible but break at run time**, which is the
dangerous shape — nothing fails to compile:

| Symptom | Meaning | Remedy |
|---|---|---|
| `ErrSealedStateUnauthenticated` on first unseal | The sealed root key blob predates `state_mac` and carries none. Refusing an absent MAC is deliberate: accepting it is a downgrade an attacker triggers by deleting the field. | One-start migration — set `BarrierConfig.AllowUnauthenticatedSealedState = true`, start once, re-persist `SealedRootKeyData()` (Unseal restamps the MAC), then clear the flag. |
| `ErrSealedStateUnauthenticated` after a password change, rekey, key import or Shamir re-split | The embedder re-wrapped the root key itself. A `SealingStrategy` returns only the ciphertext and its own strategy fields, so the assembled blob carries no MAC — and an absent MAC is refused exactly like a stripped one. Not a migration problem: the blob was written by a current build. | Stamp before persisting. With the barrier unsealed, `(*Barrier).StampSealedStateMAC(sealed)`; when the root key is in hand but no barrier is unsealed yet (the import case), `StampSealedStateMACWithRootKey(rootKey, deploymentID, sealed)`. Stamp LAST — the MAC covers the ciphertext too. |
| `ErrSessionsUnsupportedOnDisk` from a call that used to succeed | Client sessions on an `sm.DiskStateMachine` shard. They silently diverged the cluster before, because the session table lives only in the snapshot stream and an on-disk SM never takes the recover path on restart. | Use NO-OP sessions (`Host.GetNoOPSession`) and make the operations idempotent. |
| `ErrKDFNotFIPSApproved` on unseal | An Argon2id-wrapped blob under FIPS mode; unsealing re-derives the same wrapping key. | Unseal once with FIPS **off**, re-seal with `NewSoftwareStrategyFIPS` (PBKDF2-SHA256), which unseals identically in either mode. |
| Nonce tracking silently absent | `BarrierConfig.NonceTracking` is now `*bool`. As a plain `bool` its zero value meant "off"; as a pointer, nil means "use the default", which is **enabled**. | Nothing, unless you relied on the old zero-value behaviour. Disable deliberately with `BoolPtr(false)`; read the effective value with `GetNonceTracking()`. |

Also source-breaking for embedders: `StopShard` and `Host.Close` now return
errors they previously swallowed (`Close` reports a drain that timed out);
`batch.EncodeBatch` returns `([]byte, error)`; and
`proto.MaxChecksumLen` / `proto.ErrChecksumTooLong` are removed.

### Before upgrading

- Read the `BREAKING` section of every CHANGELOG entry between your version and
  the target. Format breaks are recorded there and nowhere else.
- Export a snapshot per shard (`Host.ExportSnapshot`) if you need a restore
  point. `Host.ImportSnapshot` on a fresh disaster-recovery host requires
  `WithImportDataDir` — see [Deployment](deployment.md#disaster-recovery-importing-a-snapshot).
- Verify the target version starts against an EMPTY data directory in staging
  first. A format break shows up at open time, not at first write.

## Downgrades

Not supported at any version boundary. The `snapshot.meta` magic and the WAL
payload-type ladder both make a newer directory unreadable to an older binary,
and neither fails in a way an operator can repair in place. Downgrading means
wipe-and-restore from a backup taken on the older version.
