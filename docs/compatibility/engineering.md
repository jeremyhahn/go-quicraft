# Compatibility and upgrades: engineering

> **Status: NORMATIVE** · Scope: go-quicraft, all persisted and wire formats ·
> Last verified: 2026-10-20
> Conflicts with this document are defects. Fix them here first.

Every version discriminator, what it guards, how it is checked and what it
returns. The policy these implement is in [architecture.md](architecture.md);
the interfaces an embedder's own types must satisfy are in [api.md](api.md).

## Version discriminators

| # | Discriminator | Value | Scope | Check |
|---|---|---|---|---|
| 1 | `proto.WireVersion` | 1 | transport: `MessageBatch.BinVer`, `SnapshotChunk.BinVer` | closed set `{current, previous}`; sent at the shard's committed version |
| 2 | `transport.WireVersion` (frame header byte 3) | `0x01` | transport frame header | closed set `{current, previous}`; sent paired with `BinVer` |
| 3 | `Membership` section version byte (`membershipFormatVersion`) | 1 | persisted `proto.Membership` | closed set `{current, previous}` |
| 3a | `waldb` `membershipSectionVersion` | 1 | the membership section inside WAL snapshot records | closed set `{current, previous}` |
| 4 | `WALFORMAT` marker (`walFormatVersion`) | 1 | WAL chunk framing and block size | closed set `{current, previous}`, checked before any segment is read |
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

`waldb` pins its own `membershipSectionVersion` (1, closed set on replay) for
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
| `0x0c` | retired | fatal: policy tail without the committed `WireVersion` |
| `0x0d` | `payloadTypeState` | current: `Term`, `Vote`, `Commit`, `VoteWindow` (48-byte record) |
| `0x0e` | `payloadTypeSnapshot` | current: artifact `Compression` in the fixed region, four-field session policy tail followed by the committed `WireVersion` |

Retired bytes are left unallocated, not named, so `replayRecord`'s default case
refuses them. The next layout change takes `0x0f`.

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

## Rolling upgrade: what v1.0.0 carries

[Architecture question 3](architecture.md#open-questions-for-the-owner) binds
a rolling upgrade across one MINOR by a fixed window and a switch committed
through the Raft log. A v1.0 node is the *old* side of the first such upgrade,
so v1.0 must already carry everything the old side does; a v1.1 cannot add it
retroactively. In v1.0 every window below has one member and nothing switches.

### The accept sets

Each gate tests membership of a closed set, `{current, previous}`, and refuses
everything else with the error it returns today. In v1.0 `previous` equals
`current`, so each set has exactly one member; a v1.1 sets `previous` to the
v1.0 value. A range test (`v >= previous && v <= current`) is not used: it
widens silently when a constant moves.

| Gate | Set | Refusal |
|---|---|---|
| frame header byte 3 (`ValidateFrameHeader`, `frame.go`) | `transport.WireVersion`, `transport.PreviousWireVersion` | `ErrUnsupportedVersion` |
| `MessageBatch.BinVer` (`messagebatch.go`) | `proto.WireVersion`, `proto.PreviousWireVersion` (`proto.AcceptsWireVersion`) | `proto.ErrUnsupportedVersion` |
| `SnapshotChunk.BinVer` (`chunk.go`) | the same | `proto.ErrUnsupportedVersion` |
| `Membership` version byte (`membership.go`) | `membershipFormatVersion`, `previousMembershipFormatVersion` | `*UnsupportedMembershipVersionError` |
| `waldb` membership section (`db.go`) | `membershipSectionVersion` and its previous | the current corruption error |
| `WALFORMAT` (`format.go`) | `walFormatVersion` and its previous | the current marker error |
| `snapshot.meta` (`snapshot_meta.go`) | `snapshotMetaVersion` and its previous | the current decode error |
| exported artifact (`host.go`) | `snapshotArtifactVersion` and its previous | `*SnapshotArtifactError` |
| sealed state MAC (`seal/barrier.go`) | `sealedStateMACVersion` and its previous | the current MAC error |

The persisted sets are question 2's read window; the two wire sets are
question 3's upgrade window.

### Member-version knowledge

A v1.1 leader must know that every member of a shard runs v1.1 before it
commits the switch, and it learns that from v1.0 peers, so v1.0 sends it.
Every `MessageBatch` carries `MaxBinVer`, the highest `proto.WireVersion` the
sending build accepts (v1.0 stamps 1). The receiving host records the latest
`MaxBinVer` per `(shardID, replicaID)` of each request's `From`; the latest
report, not the highest, so a node downgraded before the switch lowers it
again. A member never heard from counts as previous. Recording is done only
by a host whose window spans two versions (`Host.wireSwitchEnabled`), so a
v1.0 receive path pays nothing for it.

### The switch is a config change

`Membership.WireVersion` holds the shard's committed wire version; zero means
never switched and reads as `PreviousWireVersion`. A config change of type
`UpdateWireVersion`, shaped like `UpdateWitnessPolicy` (no target replica),
carries the new value. Because it is a config change it is:

- applied at one log index by every replica, through the same membership
  apply path, and serialised against other membership changes by the
  one-pending-config-change rule, so the member set cannot change between the
  leader's check and the commit;
- persisted inside `Membership` in every snapshot, so a replica that restores
  past the entry sees the same value it would have applied, and a crash replays
  it identically;
- never reverted: apply refuses a value lower than the current one, and any
  value outside the accept set, with `proto.ErrUnsupportedVersion`.

The leader's host proposes it on its own (no API call) when the shard's
effective version is below `proto.WireVersion` and every voter, non-voting
member and witness last reported `MaxBinVer >= proto.WireVersion`
(`pkg/wireswitch.go`). It re-examines a shard on each received batch, at most
once a second per shard, which also covers a lost proposal and a leadership
change without a sweep loop. A shard with no other member receives no batch,
so its leader, once read-ready, retries on the same once-a-second pace until
the shard switches, gains a member, or the leader loses the term or stops. The
proposal goes through the same validation as any config change, against the
replica's own window rather than the build default, and it names the
membership the leader checked by its `ConfigChangeID`: the leader's step
worker refuses it before append, as a pending config change, if any config
change has applied since, so a member added between the check and the
proposal is never switched before it advertises the version. In v1.0 the effective version already equals
`proto.WireVersion`, so it never proposes.

Until the switch a node stamps each outbound `MessageBatch` and
`SnapshotChunk` with its shard's effective version, and the frame with the
matching transport version. The transport reads the committed version
through a per-shard lookup the host installs at construction
(`QUICTransport.SetWireVersionLookup`, `func(shardID uint64) uint64`); an
unknown shard returns zero, which reads as previous. Only a shard committed
at the build's current version sends the current pair. A batch mixing shards
takes the lowest effective version among them, and a snapshot stream fixes
its version when it starts, so a switch mid-transfer cannot split it. A message type new in the release is sent only on a
switched shard.

### A v1.0 node meeting the next version

It fails closed and touches nothing durable. A frame, batch or chunk at the
next version is refused with `ErrUnsupportedVersion`, which costs one
connection and is retried; this is what happens to a v1.0 node left in, or
added to, a switched cluster, and its remedy is to upgrade. An
`UpdateWireVersion` entry, or a snapshot whose `Membership.WireVersion` is
outside its set, is refused on apply with `proto.ErrUnsupportedVersion`, and
the replica stops that shard rather than apply it. A switched data directory
is therefore never run by a v1.0 build: no downgrade after the switch
(question 5).

### Testing the next build

A process holds one value of each constant, so the mixed cluster runs in one
process with each host carrying its build's window: the wire versions it
accepts and the one it advertises, taken from the constants by default. The
`quicraft_nextwire` build tag, used only by `_test.go` files, provides the
next build's window (`WireVersion+1` with the v1.0 value as previous, and the
matching transport pair). A release build never compiles it.

`pkg/wireswitch_nextwire_test.go` runs three hosts on QUIC from this build's
window and restarts them one at a time onto the next build's, on their own
directories, address and leaf. It asserts the shard stays on the previous
version while any replica runs the old build, commits the switch once all
three run the new one, and keeps committing afterwards:

```
go test -race -tags quicraft_nextwire -run TestNextWire ./pkg/
```

## Pinned layouts and their golden fixtures

Every layout another build must read, with the function that writes it, the
function that reads it, and the fixture that pins its bytes. Fixtures live under
the owning package's `testdata/golden/`.

| Layout | Encoder | Decoder | Fixture |
|---|---|---|---|
| `proto.Entry` | `(*Entry).MarshalTo` | `(*Entry).UnmarshalFrom` | `pkg/proto/testdata/golden/entry.bin` |
| `proto.ConfigChange` | `(*ConfigChange).MarshalTo` | `(*ConfigChange).UnmarshalFrom` | `pkg/proto/testdata/golden/configchange.bin` |
| `proto.Membership` | `(*Membership).MarshalTo` | `(*Membership).UnmarshalFrom` | `pkg/proto/testdata/golden/membership.bin` |
| `proto.Snapshot` | `(*Snapshot).MarshalTo` | `(*Snapshot).UnmarshalFrom` | `pkg/proto/testdata/golden/snapshot.bin` |
| `proto.State` | `(*State).MarshalTo` | `(*State).UnmarshalFrom` | `pkg/proto/testdata/golden/state.bin` |
| `proto.SnapshotFile` | `(*SnapshotFile).MarshalTo` | `(*SnapshotFile).UnmarshalFrom` | `pkg/proto/testdata/golden/snapshotfile.bin` |
| `proto.Message` | `(*Message).MarshalTo` | `(*Message).UnmarshalFrom` | `pkg/proto/testdata/golden/message.bin` |
| `proto.MessageBatch` | `(*MessageBatch).MarshalTo` | `(*MessageBatch).UnmarshalFrom` | `pkg/proto/testdata/golden/messagebatch.bin` |
| `proto.SnapshotChunk` | `(*SnapshotChunk).MarshalTo` | `(*SnapshotChunk).UnmarshalFrom` | `pkg/proto/testdata/golden/snapshotchunk.bin` |
| transport frame header | `transport.MarshalFrame` | `transport.ReadFrameHeader` | `pkg/internal/transport/testdata/golden/frame.bin` |
| snapshot stream header | `(*SnapshotHeader).MarshalTo` | `(*SnapshotHeader).UnmarshalFrom` | `pkg/internal/transport/testdata/golden/snapshot-header.bin` |
| `WALFORMAT` marker | `writeWALFormat` | `readWALFormat` | `pkg/logdb/waldb/testdata/golden/walformat.bin` |
| WAL chunk framing | `encodeRecord` | `decodeRecord` | `pkg/logdb/waldb/testdata/golden/chunk.bin` |
| WAL entries record (`0x0b`) | `(*walShard).writeEntries` | `(*walShard).replayEntries` | `pkg/logdb/waldb/testdata/golden/record-entries.bin` |
| WAL state record (`0x0d`) | `(*walShard).writeState` | `(*walShard).replayState` | `pkg/logdb/waldb/testdata/golden/record-state.bin` |
| WAL snapshot record (`0x0e`), with the `waldb` membership section | `(*walShard).writeSnapshot` | `(*walShard).replaySnapshot` | `pkg/logdb/waldb/testdata/golden/record-snapshot.bin` |
| WAL bootstrap record (`0x04`) | `(*walShard).writeBootstrap` | `(*walShard).replayBootstrap` | `pkg/logdb/waldb/testdata/golden/record-bootstrap.bin` |
| WAL compaction record (`0x05`) | `(*DB).RemoveEntriesTo` | `(*walShard).replayCompaction` | `pkg/logdb/waldb/testdata/golden/record-compact.bin` |
| `RETIRED` registry | `marshalRetiredRegistry` | `parseRetiredRegistry` | `pkg/logdb/waldb/testdata/golden/retired.bin` |
| `COMPACTED` floor file (`QRFLOORS`) | `marshalCompactionFloors` | `parseCompactionFloors` | `pkg/logdb/waldb/testdata/golden/floors.bin` |
| `snapshot.meta` | `engine.MarshalSnapshotMeta` | `engine.ParseSnapshotMeta` | `pkg/internal/engine/testdata/golden/snapshot-meta.bin` |
| at-rest snapshot frame context (`QRSNAP1`) | `(*SnapshotAAD).Frame` | rebuilt by the reader and authenticated | `pkg/internal/engine/testdata/golden/snapshot-frame-aad.bin` |
| exported artifact (`QRSNAP01` header, payload, `QRSNTRLR` trailer) | `marshalSnapshotArtifactHeader`, `snapshotArtifactTrailer.marshal` | `readSnapshotArtifactHeader`, `parseSnapshotArtifactTrailer` | `pkg/testdata/golden/export-artifact.bin` |
| barrier ciphertext frame (epoch, algorithm, nonce, ciphertext and tag) | `(*Barrier).encryptLocked` | `(*Barrier).decryptLocked` | `pkg/seal/testdata/golden/barrier-frame.bin` |
| sealed root key ciphertext (salt, nonce, wrapped key and tag) | `(*SoftwareStrategy).sealedFromCipher` | `(*SoftwareStrategy).UnsealRootKey` | `pkg/seal/testdata/golden/sealed-root-key.bin` |
| sealed state MAC (version 2) | `sealedStateMAC` | `verifySealedStateMAC` | `pkg/seal/testdata/golden/sealed-state-mac.bin` |

### Regenerating a fixture

Each owning package's `golden_test.go` holds one fixed input per layout: fixed
IDs, keys, nonces, salts and timestamps, and no wall clock or randomness. Every
map in a waldb input holds at most one entry, because the bootstrap and
membership-section encoders walk Go maps in iteration order. Each test checks
both directions: encoding the input reproduces the fixture byte for byte, and
decoding the fixture yields the input field for field and encodes back to it.
The shared helper is `pkg/internal/golden`.

The encrypted layouts (barrier frame, sealed root key) are written through
`fixedNonceAEAD`, a test-only AEAD in `pkg/seal/golden_test.go` with the
production module-nonce shape (`NonceSize() == 0`, the nonce prepended to the
`Seal` output) and a fixed nonce. Decoding uses the production AEAD, so it runs
under `GODEBUG=fips140=only` as well; only the re-encode step skips there,
because FIPS 140-only mode refuses a caller-chosen GCM nonce.

A fixture is regenerated only for a deliberate layout change, which takes a new
payload-type byte or record-level discriminator, never a version bump:

```
go test ./pkg/proto/ -run TestGolden -update-golden
go test -race ./pkg/proto/ -run TestGolden
```

The fixture diff is committed with the layout change and reviewed with it. CI
never passes `-update-golden`, so a layout that changed with no fixture diff
fails its package's tests. Recording a layout that has not shipped yet, such
as a new record type, adds a row here and a fixture in the same commit.

## Exported-API gate

`make api-check` (`scripts/api-check.sh`), which `make lint` runs, compares the
exported API of every public package of the root module and
`contrib/dragonboat` with the last release tag. A public package is any
non-`main` package outside an `internal/` or `test/` directory. The base is the
newest `v*` tag reachable from `HEAD`, or `API_BASE`. It is read with
`git archive`, and both sides are compared with `apidiff`, which the root
`go.mod` pins with a `tool` directive and the script builds into a temporary
directory. Nothing is installed on the host.

| Base tag | An incompatible change |
|---|---|
| none reachable | nothing to compare; reported, passes |
| before v1.0.0 | printed, passes |
| v1.0.0 or later | fails, unless the major in `VERSION` is above the base's major |

A removed public package is an incompatible change, and a new one is a
compatible change. `test/buildgates/api_check_test.go` runs the gate on a
temporary module with a planted removal and proves each row of the table.

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

Every shard bootstrapped by `StartShard` records the founder's
`SessionExpiryEntries`, `MaxTotalSessions`, `MaxResponsesPerSession` and
`MaxSessionsPerOwner` in its initial membership, so `SessionPolicy.IsSet` is
true from index 1. Each replica's `Config` must carry the same four values, and
a replica whose `Config` disagrees is refused with `ErrSessionPolicyMismatch`,
wrapped with the first disagreeing setting's name and both values:

| When the replica learns the committed policy | Where the refusal surfaces |
|---|---|
| at start, from its own log or snapshot record (every restart) | `StartShard` returns the error |
| later, as a joiner, from the replicated index-1 entry or an installed snapshot | the shard fails: `EventListener.OnShardFailed` with the error in `ShardFailedInfo.Err` |

The check runs once per change of the observed policy, never per entry, and
reads only committed state, so commit, apply and election order are untouched.
The remedy is to start the replica with the shard's values.

When the committed membership carries no session policy (`SessionPolicy.IsSet`
is false: a shard bootstrapped before the policy was recorded), there is
nothing to check against and each replica applies its own `Config`. The four
settings must then be identical on every replica; no check covers such a
shard, and disagreement diverges the state machine silently. No migration
records a policy for it.

## Test strategy

| Test | Holds |
|---|---|
| `pkg/proto/wireversion_test.go` | `proto.WireVersion` is 1, the persisted `Membership` version is pinned and exact-match, not derived from the wire version, golden encodings stay byte-identical, and a `MessageBatch` with another `BinVer` is refused |
| `pkg/logdb/conformance_test.go` | `memdb` and `waldb` through one assertion table, reopening the database between save and read: `waldb` answers reads from an in-memory map a save fills directly, which would hide an encoding that dropped `Identities`, `RemovedIdentities` or the session policy |
| `pkg/logdb/waldb/format_test.go`, `durability_seq_test.go` | the `WALFORMAT` marker's round trip, malformed and zeroed markers, publish-by-rename, and `walFormatVersion` pinned at 1 (`TestWALFormatVersionIsPinnedAtOne`) |
| `pkg/logdb/waldb/legacy_04x_format_test.go`, `gap_hardening_test.go`, `db_test.go` | retired and unknown payload-type bytes fail replay with `ErrUnknownPayloadType` |
| `pkg/logdb/waldb/retired_test.go` | the `RETIRED` registry is 16 bytes per replica and a malformed one is refused |
| `pkg/proto`, `pkg/internal/transport`, `pkg/logdb/waldb`, `pkg/internal/engine`, `pkg/seal`, `pkg`: `golden_test.go` (`golden_artifact_test.go` in `pkg`) | every layout in [Pinned layouts](#pinned-layouts-and-their-golden-fixtures) decodes from its fixture field for field and re-encodes to it byte for byte |
| `test/buildgates/api_check_test.go` | the exported-API gate fails on a planted removal after v1.0.0, and passes for a new major or before v1.0.0 |

A layout change updates the golden encodings in the same commit; a golden
change with no layout change is a defect.
