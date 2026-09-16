# Compatibility and Upgrades

> **Status: NORMATIVE** · Scope: go-quicraft, all persisted and wire formats ·
> Last verified: 2026-09-15
> Conflicts with this document are defects. Fix them here first.

This is a **cross-feature** document. It spans `pkg/proto`, the WAL,
`snapshot.meta` and the sealed root key, so it sits at the `docs/` root rather
than inside a feature folder — splitting the upgrade narrative across eleven
folders would destroy it. Each feature's own compatibility surface is summarized
in its `api.md` under "Versioning and deprecation" and points here.


What this library promises across versions, what it does not, and what an
upgrade actually involves today.

**Sources:** `pkg/proto/constants.go`, `pkg/proto/membership.go`,
`pkg/proto/entry.go`, `pkg/internal/transport/frame.go`,
`pkg/logdb/waldb/db.go`, `pkg/logdb/waldb/format.go`,
`pkg/internal/engine/snapshot_meta.go`, `pkg/seal/barrier.go`, `CHANGELOG.md`,
`VERSION`

The current version is **0.6.1-alpha**, published 2026-08-17. It breaks both `0.4.x`
and `0.5.x` on-disk data; see the upgrade section below.

## Stability guarantee

go-quicraft is **pre-1.0 alpha software**. Under semantic versioning the minor
version may break anything, and every minor release so far has broken the
on-disk format: `0.2.0-alpha`, `0.3.0-alpha`, `0.4.0-alpha` and `0.5.0-alpha`
each did.

Concretely, before 1.0:

| Surface | Guarantee |
|---|---|
| Exported Go API (`pkg`, `pkg/config`, `pkg/sm`, `pkg/seal`, `pkg/logdb`) | **None between minor versions.** Signature and semantic changes are called out in CHANGELOG under BREAKING |
| On-disk format (WAL chunk framing, WAL records, snapshots, `snapshot.meta`, sealed root key) | **None between minor versions.** A format break means wipe-and-resync — see [Upgrade path](#upgrade-path) |
| Wire format between nodes | Exact-match only. Mixed-version clusters are not supported at any version |
| Patch releases (`0.5.0` → `0.5.1`) | No intentional API or format break. Read the CHANGELOG anyway |

There is no long-term-support line, no deprecation window, and no compatibility
shim. Downstream products (go-qrdb, go-xkms, go-quicbaas, go-trusted-ca) pin an
exact version and upgrade deliberately.

### Which released versions actually exist

`v0.4.0-alpha` and `v0.5.0-alpha` are resolvable Go module versions whose
release pipelines never completed, so neither was published. Both tags stay
where they are rather than being moved: module versions are immutable, go-qrdb
had already recorded `v0.4.0-alpha` in its `go.sum`, and repointing a checksummed
tag fails a downstream build with a security error. Of that pair, depend on
`v0.5.1-alpha`; the current release is **`v0.11.0-alpha`** (2026-09-13), and a
consumer moving to it from anything before `v0.11.0-alpha` takes the `pkg/enclave`
deletion and the `Credentials.Secret` retype described in the CHANGELOG (#54, #41).

`v0.5.1-alpha` carries identical library content to `v0.5.0-alpha` plus two
fixes to the release gate itself, so the breaking format changes described in
the `0.5.0-alpha` CHANGELOG entry apply in full to `0.5.1-alpha`.

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
| 49 | 8 | `MaxSessionsPerOwner` |
| 57 | var | `Addresses` map |
| — | var | `Observers` map |
| — | var | `Witnesses` map |
| — | var | `Removed` bool map |
| — | var | `Identities` map |
| — | var | `RemovedIdentities` map |

The fixed region is **57 bytes** (`membershipFixedSize`, `pkg/proto/membership.go:109`)
and an empty section is **81**: the fixed region plus six 4-byte map count
prefixes. Every map is written unconditionally, so an empty map is four zero
bytes rather than an absent field, and an unset session policy is 32 zero bytes
rather than an absent one.

`MaxSessionsPerOwner` was added at `0.5.0-alpha`, in place, at version 1. That
moved the six-map region from offset 49 to 57 without changing the version byte,
which is the pinning policy working exactly as designed and exactly as
dangerously: see [What 0.5.x does when it meets 0.4.x data](#what-05x-does-when-it-meets-04x-data).

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
| — | 32 | Session policy: `SessionExpiryEntries`, `MaxTotalSessions`, `MaxSessionsPerClient`, `MaxSessionsPerOwner` |
| — | var | `RemovedIdentities` map |

Its fixed region is **18 bytes** (`membershipFixedSize`, `pkg/logdb/waldb/db.go:1933`)
and an empty section is **74** (`minMembershipSize`, `db.go:1944`) — which is
also the read floor replay enforces before it looks at the version byte. The two
numbers are equal by construction: one layout, every field unconditional, so the
smallest thing that may be READ is exactly the size of what is WRITTEN. A
section shorter than 74 bytes fails with `ErrTruncatedSnapshotRecord` before the
version byte is read.

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

**The marker did once catch a real skew, and it no longer does.** Builds before
`0.4.0-alpha` wrote version **2**; `0.4.0-alpha` renumbered it to 1 under the
pinning policy, so a pre-0.4 directory is refused with `*WALFormatVersionError`
naming 2 and 1. Every build from `0.4.0-alpha` onward writes 1, which means a
`0.4.x` directory opened by `0.5.x` **passes this check** even though its record
payloads are unreadable. Do not treat a clean marker check as evidence that a
data directory matches the binary. What catches that boundary is the
payload-type ladder in [section 5](#5-wal-payload-type-bytes--persisted-fatal-on-replay-ladder),
one layer down, at the first record replayed.

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
| `0x01` | *(retired, unallocated)* | **Fatal** — `ErrUnknownPayloadType`. It named an entry format with no session fields, so replay would disable at-most-once delivery |
| `0x02` | `payloadTypeState` | Current |
| `0x03` | *(retired, unallocated)* | **Fatal** — `ErrUnknownPayloadType`. It named a snapshot format with no `MaxWitnesses`/policy flags |
| `0x04` | `payloadTypeBootstrap` | Current |
| `0x05` | `payloadTypeCompact` | Current |
| `0x06` | `payloadTypeRemoveNode` | Current |
| `0x07` | *(retired, unallocated)* | **Fatal** — `ErrUnknownPayloadType`. It named a 76-byte entry header, so `OwnerID` would be read from the record's own `CmdLen` plus four command bytes and the command length then taken from inside the command |
| `0x08` | *(retired, unallocated)* | **Fatal** — `ErrUnknownPayloadType`. Its membership section started at `ConfigChangeID`, so current parsing would eat its low byte as a version byte |
| `0x09` | *(retired, unallocated)* | **Fatal** — `ErrUnknownPayloadType`. Its `FilePathLen` sat where the current layout reads `Compression` |
| `0x0a` | *(retired, unallocated)* | **Fatal** — `ErrUnknownPayloadType`. It named a three-field session policy tail, so `MaxSessionsPerOwner` would be read from the `RemovedIdentities` count and the deny-list located eight bytes late |
| `0x0b` | `payloadTypeEntries` | **Current entry format** — 84-byte record header, five session fields (`Key`, `ClientID`, `SeriesID`, `RespondedTo`, `OwnerID`) |
| `0x0c` | `payloadTypeSnapshot` | **Current snapshot format** — artifact `Compression` in the fixed region, four-field session policy tail |

Every fatal error names the same remedy: **restore from a snapshot, or re-sync
this node from its peers.** Old byte values stay reserved forever so a legacy
WAL is *detected* rather than misparsed.

The ladder only discriminates layouts that were given a NEW byte, and with every
version number pinned it is the ONLY thing that discriminates a `0.4.x` record
at all. `0x07` and `0x0a` changed layout in place at `0.5.0-alpha` and were
therefore blind to exactly the boundary they were needed for; retiring them and
moving the current formats to `0x0b` and `0x0c` is what restored the rule. See
[What 0.5.x does when it meets 0.4.x data](#what-05x-does-when-it-meets-04x-data)
for what the blind window actually produced.

The renumbering cuts both ways: a WAL written by this build reads as
`ErrUnknownPayloadType` under `0.5.1-alpha`, which knows `0x0b` and `0x0c` as
nothing at all. That is a wipe in either direction, which is the pre-release
posture regardless.

An unknown byte fails with `ErrUnknownPayloadType`; the dominant real cause of
that is still not a version skew but opening a **keyed** WAL with
`barrier == nil`, which makes every record decode as type `0x00`.

### 6. `snapshot.meta` — persisted, self-describing, append-only

The record beside every snapshot artifact. Current records carry a 16-byte
header — `QRSNAPMD` magic, a uint16 body version (**1**), a reserved uint16 and
a uint32 body length — ahead of a 72-byte body of nine little-endian uint64
fields (`Index`, `Term`, `ShardID`, `ReplicaID`, `CreatedAt`, `Epoch`,
`Compression`, `DataLen`, `DataHash`), for **88 bytes** total. Because the body
length is recorded, a reader can distinguish a field that is ABSENT from bytes
that are CORRUPT.

This is the one format here whose discriminator does its job across a version
change, because the body length is in-band and the rule below is honoured.

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

#### `state_mac` moved to v2, and it migrates itself

`sealedStateMACVersion` (`pkg/seal/barrier.go:2334`) is the version byte of the
canonical encoding the metadata MAC covers. It went **1 → 2** at
`0.5.0-alpha` to bring `LoadGeneration` into the covered set, because the
barrier now refuses to encrypt on the strength of that counter and an
unauthenticated plaintext field would be a passphrase-free way to stop a node
writing.

**This is the one 0.4.x → 0.5.x carry-over that does not need a wipe.**
`verifySealedStateMAC` accepts a valid v1 tag once, logs it, and restamps at v2
on the next persist. Verifying the old version is not a downgrade: forging
either tag needs the same HKDF-derived MAC key, which comes from the root key
the sealing strategy just recovered, and what v1 leaves uncovered is a field a
pre-fence blob does not carry at all.

Two things follow for an operator. First, keep the sealed root key across a
0.4.x → 0.5.x replacement even though everything else is wiped. Second, if you
persist `SealedRootKeyData()` yourself, persist it again after the first
successful unseal, or every subsequent start pays the legacy path.

A blob with **no** MAC at all is a different case and is still refused with
`ErrSealedStateUnauthenticated` — see the one-start migration in the
[0.3.x boundary notes](#version-boundaries-before-04x).

## Interface requirements for embedders

Three requirements are satisfied STRUCTURALLY rather than by a declared
implements-clause, so a Go type check will not catch a type that fails them.
Two fail at `waldb.Open`; one is a constant whose zero value is easy to
misread. All three date from `0.3.0-alpha` and all three still hold at
`0.6.1-alpha`. Older API breaks are recorded only in the CHANGELOG.

### `waldb.FS` requires `Rename(oldpath, newpath string) error`

Any type implementing `waldb.FS` — test doubles, instrumented filesystems, the
error-injection harnesses downstream products build — must provide `Rename`
along with `OpenFile`, `Open`, `MkdirAll`, `ReadDir`, `Stat` and `Remove`
(`pkg/logdb/waldb/fs.go:73`). Passing one to `waldb.Open` via `WithFS` will not
compile until it does.

`Rename` is required rather than optional because the WAL geometry marker
(`WALFORMAT`) is published by writing a temporary file, fsyncing it, and renaming
it into place. That sequence is what makes a partially-written marker
unobservable; an optional interface with an in-place fallback would restore
exactly the torn state the rename removes. Delegate to `os.Rename` (or model
rename's atomicity) and the requirement is satisfied.

### The barrier interface `waldb` asserts requires `ContextBindingResolved() bool`

`waldb` asserts an unexported interface on the DYNAMIC type of the barrier
passed to `WithBarrier` (`pkg/logdb/waldb/db.go:141`). A barrier missing the
method compiles fine, then fails at `Open`.

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

### `seal.ContextBinding`'s zero value is `Unresolved`, not a binding

| Constant | Value | Meaning |
|---|---|---|
| `ContextBindingUnresolved` | 0 | No answer yet: the barrier is sealed or uninitialized. `Binds()` is **false** |
| `ContextBindingBarrier` | 1 | Barrier folds the caller's context into its own AEAD's AAD (software path) |
| `ContextBindingDelegated` | 2 | A delegated hardware/KMS encryptor owns the binding |
| `ContextBindingUnsupported` | 3 | The encryptor cannot bind context |
| `ContextBindingPlaintext` | 4 | Not encrypting |

Compare against the named constants, never the numbers, and do not persist or
transmit the numeric value.

The trap is the zero value. A barrier legitimately spends time at
`Unresolved` — a node may start sealed and be unsealed later, which is the whole
point of the start-then-unseal flow — so **any consumer that caches the
capability must re-ask after `Unseal`** rather than latching what a sealed
barrier said. `Binds()` is false for `Unresolved`, which keeps the fail-safe
direction for callers that only ask the predicate.

### `seal.NewBarrier` returns `(*Barrier, error)`

An invalid configuration — an unknown algorithm, or a non-FIPS-approved cipher
under FIPS mode — is an error, never a panic. Those are exactly the conditions a
configuration read from a file, an API request, or per-tenant settings produces,
and a panic in an exported constructor takes down the embedding process.

| Constructor | Signature | Use when |
|---|---|---|
| `seal.NewBarrier(cfg)` | `(*Barrier, error)` | Config assembled at runtime — surface the error |
| `seal.MustNewBarrier(cfg)` | `*Barrier` | Config from compile-time constants; panics, `regexp.MustCompile` precedent |
| `seal.NewEpochBarrier` / `seal.MustNewEpochBarrier` | aliases | Same functions under the epoch-barrier name |
| `crypto.NewBarrier` / `crypto.MustNewBarrier` | aliases | `pkg/crypto` re-exports both |

`BarrierConfig.Validate` stays exported but is an admission-time convenience
rather than a safety requirement: `NewBarrier` applies the same rules and
returns the same error identity, so skipping it no longer risks a panic.

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
   unless the release notes list an encryption break: the AAD binding change in
   `0.3.0-alpha` makes existing ciphertext unreadable regardless of the key,
   while the `0.4.x` → `0.5.x` `state_mac` bump migrates itself and the key
   should be carried across. Deleting the WAL directory means deleting the
   `WALFORMAT` marker with it — leaving segments behind without it is refused
   (`ErrWALFormatMissing`), by design, since nothing then records their chunk
   framing.
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

### `0.4.x` and `0.5.x` → `0.6.0-alpha`

**0.5.x breaks all 0.4.x on-disk data. There is no in-place upgrade.** Follow
the rolling-replacement procedure above, or rebuild the cluster from scratch.

Every format version still reads 1 on both sides, so no version number records
this break. The WAL payload-type bytes do, since the current entry and snapshot
formats were renumbered when their layouts changed. The layouts moved underneath
the version numbers:

| Layout | 0.4.x | 0.5.x |
|---|---|---|
| `proto.Entry` header | 60 bytes | **68** — `OwnerID` added at +56 |
| WAL entry record header | 76 bytes | **84** — `logdb.EntryOverheadBytes` moved with it |
| WAL entry payload type | `0x07` | **`0x0b`** — `0x07` retired and left unallocated |
| `proto.SessionPolicy` | 24 bytes | **32** — `MaxSessionsPerOwner` |
| `proto.Membership` fixed region | 49 bytes | **57** — the six-map region moved with it |
| `waldb` membership section minimum | 66 bytes | **74** |
| WAL snapshot payload type | `0x0a` | **`0x0c`** — `0x0a` retired and left unallocated |
| `proto.Snapshot` | `Type`, `Imported`, `Witness` | those three **removed** (written or reserved, never read); `Compression` **added** |
| `snapshot.meta` body | 7 fields | **9** — `DataLen` and `DataHash`, version bumped, old records still read |
| Sealed root key `state_mac` | v1 | **v2** — covers `LoadGeneration`; **migrates itself** |

Two of those need separate treatment. The `snapshot.meta` record is
self-describing and genuinely backward compatible: old records keep working
until their snapshot directory rolls off. The sealed root key migrates in place;
**keep it** through the wipe (see [`state_mac` moved to
v2](#state_mac-moved-to-v2-and-it-migrates-itself)). Everything else in the
table is a wipe.

#### What 0.5.x does when it meets 0.4.x data

`Open` succeeds and **recovery fails at the first legacy record**, with
`ErrUnknownPayloadType`. The error names the unreadable record
they found and both name the remedy: restore from a snapshot, or re-sync from
peers.

No version gate is involved, and none was ever going to be. Every version number
here is pinned at 1 for the pre-release period, so both builds stamp the same
value in every one of them:

| Check | Verdict on a 0.4.x directory |
|---|---|
| `WALFORMAT` marker version | **Passes.** Both builds write 1 |
| WAL chunk framing (11-byte header) | **Passes.** Unchanged since 0.4.0 |
| `Membership` section version byte | **Passes.** Both builds write 1 |
| `MessageBatch.BinVer` on the wire | **Passes.** Both builds stamp `WireVersion` = 1 |
| **WAL payload type byte** | **Refuses.** `0x07` and `0x0a` are retired and left unallocated, so a record carrying either fails replay with `ErrUnknownPayloadType` |

That last row is the whole gate. It is the payload-type ladder doing the job the
pinned version numbers deliberately do not do, which is why a layout change must
always take a new type byte.

**`0.5.1-alpha` itself does not have this gate.** The renumbering landed after
the release. A `0.5.1-alpha` binary still writes `0x07`/`0x0a` and reads a 0.4.x
record with them, so everything in the next subsection applies to it. The
renumbering also runs the other way: a WAL written by a current build is
`ErrUnknownPayloadType` under `0.5.1-alpha`, which has never heard of `0x0b` or
`0x0c`.

##### What it did before the payload types were renumbered

Recorded because the failure was not "it errors cleanly", and because the same
shape returns the moment a layout is changed in place again.

`0.5.0-alpha` grew the entry record header 76 → 84 bytes and the snapshot
record's session policy tail 24 → 32 while leaving both payload-type bytes where
they were. Measured against real 0.4.x records:

- **An entry record was accepted with `err == nil`.** `OwnerID` decoded as
  `0x4443424100000018` — the record's own `CmdLen` concatenated with the first
  four bytes of its command — and `CmdLen` was then read from four bytes further
  into the command. The command reached the state machine truncated from 24
  bytes to 16. `OwnerID` is the session admission grouping key, and every replica
  has to agree on it.
- **A snapshot record usually failed as truncated**, which is a corruption
  diagnosis for what is really a version mismatch. In one realistic shape it was
  accepted in silence, with a fabricated `MaxSessionsPerOwner` and a
  `RemovedIdentities` deny-list read eight bytes off, populated with a replica
  nobody had removed. That list is checked against the authenticated connection
  identity, so misreading it revokes a revocation.

The wire has no equivalent gate and still does not: a 0.4.x and a 0.5.x node
accept each other's `BinVer` and then disagree about what the bytes mean.
Replace every node in the cluster, and rebuild every consuming repository, from
one commit.

#### Migration

Two supported routes, in preference order:

1. **Wipe and re-sync.** Rolling replacement per the procedure above, keeping
   the sealed root key. Cheapest, and the only route if the shard is healthy.
2. **Export under 0.4.x, import under 0.5.x.** Take `Host.ExportSnapshot` per
   shard while still running 0.4.x, replace every binary, then
   `Host.ImportSnapshot` onto empty data directories. Use this when the cluster
   is being rebuilt wholesale, or when you need a restore point that outlives
   the upgrade window.

There is no third route. `waldb.Reencrypt` re-serializes every record and is the
supported path for a **barrier** migration; it is not a format migration tool
and it will not read 0.4.x records.

### Version boundaries before `0.4.x`

Recorded here only so an old directory's failure mode is identifiable. All of
them are wipe-and-resync; none has a migration.

| Boundary | Nature |
|---|---|
| `0.2.0-alpha` → `0.3.0-alpha` | Four on-disk formats changed. Symptoms: `ErrWALFormatMissing` (segments predate the marker), `ErrUnknownPayloadType`, and `ErrDecryptionFailed` on every read from the at-rest AAD binding change |
| `0.3.0-alpha` → `0.3.1-alpha` | CI and test infrastructure only. **The one boundary in this project's history with no wipe** — same formats, same API, wire-interoperable |
| `0.3.1-alpha` → `0.4.0-alpha` | Every format version pinned at 1, and the multi-version machinery removed. The `WALFORMAT` marker specifically went **2 → 1**, so a pre-0.4 WAL directory is refused with `*WALFormatVersionError` naming both. `proto.Membership` grew `RemovedIdentities`; `proto.ConfigChange` stopped being length-discriminated; `Snapshot.Checksum` removed |
| `0.4.0-alpha` → `0.4.1-alpha` → `0.4.2-alpha` | Release-gate and CI repairs. No format change; upgrade in place |

Four `0.4.0-alpha` changes are **source-compatible but fail at run time**, which
is the dangerous shape — nothing fails to compile. They still apply on any jump
from 0.3.x:

| Symptom | Meaning | Remedy |
|---|---|---|
| `ErrSealedStateUnauthenticated` on first unseal | The sealed root key blob predates `state_mac` and carries none. Refusing an absent MAC is deliberate: accepting it is a downgrade an attacker triggers by deleting the field. | One-start migration — set `BarrierConfig.AllowUnauthenticatedSealedState = true`, start once, re-persist `SealedRootKeyData()` (Unseal restamps the MAC), then clear the flag. |
| `ErrSealedStateUnauthenticated` after a password change, rekey, key import or Shamir re-split | The embedder re-wrapped the root key itself. A `SealingStrategy` returns only the ciphertext and its own strategy fields, so the assembled blob carries no MAC — and an absent MAC is refused exactly like a stripped one. Not a migration problem: the blob was written by a current build. | Stamp before persisting. With the barrier unsealed, `(*Barrier).StampSealedStateMAC(sealed)`; when the root key is in hand but no barrier is unsealed yet (the import case), `StampSealedStateMACWithRootKey(rootKey, deploymentID, sealed)`. Stamp LAST — the MAC covers the ciphertext too. |
| `ErrSessionsUnsupportedOnDisk` from a call that used to succeed | Client sessions on an `sm.DiskStateMachine` shard. They silently diverged the cluster before, because the session table lives only in the snapshot stream and an on-disk SM never takes the recover path on restart. | Use NO-OP sessions (`Host.GetNoOPSession`) and make the operations idempotent. |
| `ErrKDFNotFIPSApproved` on unseal | An Argon2id-wrapped blob under FIPS mode; unsealing re-derives the same wrapping key. | Unseal once with FIPS **off**, re-seal with `NewSoftwareStrategyFIPS` (PBKDF2-SHA256), which unseals identically in either mode. |

### Before upgrading

- Read the `BREAKING` section of every CHANGELOG entry between your version and
  the target. Format breaks are recorded there and nowhere else.
- Export a snapshot per shard (`Host.ExportSnapshot(ctx, shardID, w)`) if you
  need a restore point, **while still running the old version**.
  `Host.ImportSnapshot` on a fresh disaster-recovery host requires
  `WithImportDataDir` — see [Deployment](multi-group-host/user.md#disaster-recovery-importing-a-snapshot).
- Verify the target version starts against an EMPTY data directory in staging
  first. A format break shows up at open time, not at first write.
- Rebuild every consuming repository from the same commit. go-qrdb, go-xkms,
  go-quicbaas and go-trusted-ca read and write these layouts too, and a repo
  built against the old one produces bytes this build cannot parse while
  stamping the same version number.

## Downgrades

Not supported at any version boundary. Downgrading means wipe-and-restore from a
backup taken on the older version.

Two mechanisms make a current directory unreadable to an older binary, and
neither fails in a way an operator can repair in place:

- **`snapshot.meta`.** The magic-and-version prefix is read as corrupt by a
  binary that predates it. That is fail-safe where it lands (it suppresses DEK
  purging) but it is not a recovery.
- **The WAL payload-type ladder.** An older binary meets a type byte it does not
  know and fails closed.

Everything else fails the way a downgrade always fails here: the older binary
accepts the version numbers, misreads the layouts, and produces a corruption
error from the record parser. Take the backup first.

---

## Cross-References

- [Deployment](multi-group-host/user.md) -- configuration reference, directory layout, disaster recovery
- [Lifecycle](multi-group-host/engineering.md) -- startup, crash recovery, WAL replay, shutdown ordering
- [Wire format](wire-protocol/engineering.md) -- byte-level layouts of every proto type
- [WAL Persistence](log-storage/engineering.md) -- segment and chunk framing, the `WALFORMAT` marker
- [Encryption](seal-barrier/architecture.md) -- barrier, epochs, sealed root key, AAD binding
- [Key custody](seal-barrier/user.md) -- sealing strategies and the Layer-1 boundary
- [API reference](multi-group-host/api.md) -- exported surface and error types
