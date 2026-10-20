# Compatibility and upgrades: engineering

> **Status: NORMATIVE** · Scope: go-quicraft, all persisted and wire formats ·
> Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

Every version discriminator, what it guards, how it is checked and what it
returns. The policy these implement is in [architecture.md](architecture.md);
the interfaces an embedder's own types must satisfy are in [api.md](api.md).

## Version discriminators

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
means. A message type one side does not know (`LeaderExec` and
`LeaderExecResp` to a build before 0.21.0-rc.1) is the same skew.

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
| 41 | 8 | `MaxResponsesPerSession` |
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
| — | 32 | session policy: `SessionExpiryEntries`, `MaxTotalSessions`, `MaxResponsesPerSession`, `MaxSessionsPerOwner` |
| — | var | `RemovedIdentities` map |

Its fixed region is 18 bytes (`membershipFixedSize` in `waldb`) and an empty
section 74 (`minMembershipSize`), which is also the read floor replay enforces
before reading the version byte. A shorter section fails with
`ErrTruncatedSnapshotRecord`.

#### The supremum is enforced at compile time

The rule is in [architecture.md](architecture.md#the-transport-version-is-a-supremum).
The `Membership` half is a constant expression in `pkg/proto/membership.go`:
`const _ uint64 = WireVersion - uint64(membershipFormatVersion)` fails to
compile if the persisted version ever exceeds the wire version.
`pkg/proto/wireversion_test.go` (`TestMembershipVersionNotDerivedFromWireVersion`)
fails if the persisted version is ever derived from the wire version.

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
0.13.0-alpha note in [user.md](user.md#upgrade-notes-by-release) for what that
does.

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

Retired bytes are left unallocated, not named, so `replayRecord`'s default case
refuses them. The next layout change takes `0x0e`.

Every fatal error names the remedy: restore from a snapshot, or re-sync from
peers. With every version number pinned, the ladder is the only thing that
detects a record-layout change, which is why a layout change must take a new
byte.

The most common real cause of `ErrUnknownPayloadType` is not version skew but
opening an encrypted WAL with no barrier, which makes every record decode as
type `0x00`.

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
(`*AlgorithmMismatchError`).

`sealedStateMACVersion` is 2: the metadata MAC covers `LoadGeneration`, because
the barrier refuses to encrypt on the strength of that counter and an
unauthenticated field would be a passphrase-free way to stop a node writing.
`verifySealedStateMAC` still accepts a valid version-1 tag on a blob whose
`LoadGeneration` is zero, logs it at WARN, and the next persist restamps it at
version 2. Accepting it is not a downgrade: forging either tag needs the same
HKDF-derived MAC key. If you persist `SealedRootKeyData()` yourself, persist it
again after the first successful unseal.

A blob with no MAC at all is refused with `ErrSealedStateUnauthenticated`,
unless `BarrierConfig.AllowUnauthenticatedSealedState` is set for a one-time
migration. Accepting an absent MAC by default would be a downgrade an attacker
triggers by deleting the field. Embedders that re-wrap the root key themselves
(password change, rekey, key import, Shamir re-split) must stamp the MAC last,
after the ciphertext: `(*Barrier).StampSealedStateMAC(ctx, sealed)` with the
barrier unsealed, or `StampSealedStateMACWithRootKey(rootKey, deploymentID,
sealed)` when only the root key is in hand.

## Identities and session policy across a restart

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
expires sessions at log indexes its peers do not.

When the committed membership carries no session policy (`SessionPolicy.IsSet`
is false), each replica applies its own `Config`. `SessionExpiryEntries`,
`MaxTotalSessions`, `MaxResponsesPerSession` and `MaxSessionsPerOwner` must then
be identical on every replica; nothing enforces it, and disagreement diverges the state machine
silently.

## Test strategy

| Test | Holds |
|---|---|
| `pkg/proto/wireversion_test.go` | `proto.WireVersion` is 1, the persisted `Membership` version is pinned and exact-match, not derived from the wire version, golden encodings stay byte-identical, and a `MessageBatch` with another `BinVer` is refused |
| `pkg/logdb/conformance_test.go` | `memdb` and `waldb` through one assertion table, reopening the database between save and read: `waldb` answers reads from an in-memory map a save fills directly, which would hide an encoding that dropped `Identities`, `RemovedIdentities` or the session policy |
| `pkg/logdb/waldb/format_test.go`, `durability_seq_test.go` | the `WALFORMAT` marker's round trip, malformed and zeroed markers, publish-by-rename, and `walFormatVersion` pinned at 1 (`TestWALFormatVersionIsPinnedAtOne`) |
| `pkg/logdb/waldb/legacy_04x_format_test.go`, `gap_hardening_test.go`, `db_test.go` | retired and unknown payload-type bytes fail replay with `ErrUnknownPayloadType` |
| `pkg/logdb/waldb/retired_test.go` | the `RETIRED` registry is 16 bytes per replica and a malformed one is refused |

A layout change updates the golden encodings in the same commit; a golden
change with no layout change is a defect.
