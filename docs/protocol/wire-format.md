# Proto Wire Format

## Design Philosophy

All serializable types in `pkg/proto/` use a custom fixed-layout binary
encoding. There is no protobuf, no varints, no type tags, and no code
generation. The design priorities are:

- **Fixed-width fields** -- every `uint64` is 8 bytes little-endian, every
  `uint32` is 4 bytes little-endian. Booleans occupy 8 bytes on the wire
  (stored as `uint64` 0 or 1). This eliminates branching during decode and
  makes field offsets statically known for fixed-header types.

- **Zero-copy where possible** -- `Entry.Cmd` aliases the source buffer
  after `UnmarshalFrom`. The caller must copy before returning the buffer
  to a pool (the transport receive path does this).

- **Deterministic encoding** -- Go maps have nondeterministic iteration
  order. All map types (`map[uint64]string`, `map[uint64]bool`) are
  serialized with keys sorted ascending so that identical membership
  configurations produce identical byte sequences.

- **Bounded sizes** -- every variable-length field is validated against a
  hard limit during unmarshal before any allocation occurs. This prevents
  memory exhaustion from crafted payloads.

- **Hand-written codecs** -- every type implements `MarshalTo(buf []byte)
  (int, error)` and `UnmarshalFrom(buf []byte) (int, error)`. No
  reflection, no code generation, no external dependencies beyond
  `encoding/binary` and `sort`.

Source: `pkg/proto/`

## Encoding Primitives

All encoding primitives are defined in `pkg/proto/encoding.go`.

### Scalar Types

| Primitive   | Wire Size | Encoding                                 |
|-------------|-----------|------------------------------------------|
| `uint64`    | 8 bytes   | `binary.LittleEndian.PutUint64`          |
| `uint32`    | 4 bytes   | `binary.LittleEndian.PutUint32`          |
| `bool`      | 8 bytes   | `uint64` 0 or 1 (little-endian)          |

Booleans are widened to 8 bytes so that all fields in fixed-header structs
stay aligned to 8-byte boundaries. The `getBool` function returns true for
any nonzero `uint64` value.

### Variable-Length Types

| Primitive   | Wire Format             | Notes                           |
|-------------|-------------------------|---------------------------------|
| `string`    | `[Len:4][Data:N]`       | Copied on read (Go immutability)|
| `[]byte`    | `[Len:4][Data:N]`       | Zero-copy alias into source buf |

Both use a 4-byte `uint32` length prefix. `getBytes` returns a slice that
aliases the source buffer. `getString` copies via `string(buf[4:total])`
because Go strings are immutable.

### Map Types

**`map[uint64]string`** (Membership address maps):

```
[Count:4]
  [Key_0:8][ValueLen_0:4][Value_0:N]
  [Key_1:8][ValueLen_1:4][Value_1:N]
  ...
```

Keys are sorted ascending via `sort.Slice`. Count is validated against
`MaxMembershipEntries` (10,000) and each value length against
`MaxAddressLength` (256) before allocation during unmarshal.

**`map[uint64]bool`** (Membership removed set):

```
[Count:4]
  [Key_0:8][Value_0:8]
  [Key_1:8][Value_1:8]
  ...
```

Fixed 16 bytes per entry (key + bool-as-uint64). Keys sorted ascending.

## Entry Wire Format

Source: `pkg/proto/entry.go`

Entry is the fundamental unit of replication. It has a 60-byte fixed header
followed by a variable-length command payload.

```
 0                   8                  16                  24
 +-------------------+-------------------+-------------------+
 |    Term (8)       |    Index (8)      |    Type (8)       |
 +-------------------+-------------------+-------------------+
24                  32                  40                  48
 +-------------------+-------------------+-------------------+
 |    Key (8)        |   ClientID (8)    |   SeriesID (8)    |
 +-------------------+-------------------+-------------------+
48                  56  60
 +-------------------+----+-------------------------------...
 | RespondedTo (8)   |CmdL|  Cmd (CmdLen bytes)
 +-------------------+----+-------------------------------...
```

### Field Offset Table

| Offset | Size | Field       | Type     | Description                        |
|--------|------|-------------|----------|------------------------------------|
| 0      | 8    | Term        | uint64   | Raft term when proposed            |
| 8      | 8    | Index       | uint64   | Log position                       |
| 16     | 8    | Type        | uint64   | Entry type + compression flag      |
| 24     | 8    | Key         | uint64   | Client request dedup key           |
| 32     | 8    | ClientID    | uint64   | Session client ID                  |
| 40     | 8    | SeriesID    | uint64   | Session series ID                  |
| 48     | 8    | RespondedTo | uint64   | Highest responded-to series        |
| 56     | 4    | CmdLen      | uint32   | Length of Cmd payload              |
| 60     | N    | Cmd         | []byte   | State machine command (zero-copy)  |

Total header: `7 * 8 + 4 = 60 bytes`. Total size: `60 + len(Cmd)`.

### Type Field Encoding

The `Type` field is a uint64 that encodes two pieces of information:

- **Lower 4 bits** (`Type & 0x0F`): the entry type
- **Bit 4** (`Type & 0x10`): the compression flag

```
  Bit:  63 .......................... 5   4        3   2   1   0
       [           reserved           ] [compress] [  entry type ]
```

Entry type constants:

| Value | Constant                 | Description                                        |
|-------|--------------------------|----------------------------------------------------|
| 0     | `EntryNormal`            | Normal state machine command                       |
| 1     | `EntryConfigChange`      | Membership change (ConfigChange)                   |
| 2     | `EntryEncodedEntry`      | Session-managed encoded entry                      |
| 3     | `EntryMetadata`          | Witness metadata entry (Index+Term only, no `Cmd`) |
| 4     | `EntryInitialMembership` | Bootstrap membership (`Cmd` = marshaled `Membership`) |

`UnmarshalFrom` rejects any masked type outside this set with
`ErrInvalidEntryType`. A reader that omits `EntryInitialMembership` cannot
replay any shard's log: every shard writes that entry at log index 1 on
first start, so the very first entry a replica reads back is refused.

Compression constants:

| Constant            | Value  | Description                         |
|---------------------|--------|-------------------------------------|
| `NoCompression`     | 0x00   | No compression applied              |
| `Snappy`            | 0x01   | Snappy compression algorithm        |
| `EntryTypeMask`     | 0x0F   | Mask to extract entry type          |
| `EntryCompressFlag` | 0x10   | Bit 4 indicates Cmd is compressed   |

Always mask before comparing: `entryType := e.Type & EntryTypeMask`

### Session Sentinel Values

Special `SeriesID` values identify session lifecycle entries:

| Constant                         | Value              | Description          |
|----------------------------------|--------------------|----------------------|
| `NotSessionManagedClientID`      | 0                  | No session (bypass)  |
| `SessionSeriesIDForRegister`     | `MaxUint64 - 1`    | Register session     |
| `SessionSeriesIDForUnregister`   | `MaxUint64`        | Unregister session   |

### Validation Rules

- `CmdLen > MaxEntrySize` (8 MB) returns `ErrEntrySizeLimitExceeded`
- `buf` shorter than `entryHeaderSize` (60) returns `ErrBufferTooSmall`
- `buf` shorter than `60 + CmdLen` returns `ErrBufferTooSmall`
- Unknown entry type (after masking) returns `ErrInvalidEntryType`

### Zero-Copy Semantics

After `UnmarshalFrom`, `Entry.Cmd` is a slice that aliases the source buffer:

```go
e.Cmd = buf[60:total] // zero-copy: aliases source buffer
```

The caller must copy `Cmd` before returning the buffer to a pool. The
transport receive path (`recv.go`) performs this copy explicitly.

## Message Wire Format

Source: `pkg/proto/message.go`

Message is a Raft protocol message exchanged between peers. It has an
88-byte fixed header (11 uint64 fields), followed by a count-prefixed
entry list and an optional inline snapshot.

```
 0                   8                  16                  24
 +-------------------+-------------------+-------------------+
 |    Type (8)       |    From (8)       |    To (8)         |
 +-------------------+-------------------+-------------------+
24                  32                  40                  48
 +-------------------+-------------------+-------------------+
 |   ShardID (8)     |  ReplicaID (8)    |    Term (8)       |
 +-------------------+-------------------+-------------------+
48                  56                  64                  72
 +-------------------+-------------------+-------------------+
 |   LogTerm (8)     |  LogIndex (8)     |   Commit (8)      |
 +-------------------+-------------------+-------------------+
72                  80                  88  92
 +-------------------+-------------------+----+
 |   Reject (8)      |    Hint (8)       |ECnt|
 +-------------------+-------------------+----+
92
 +--- entries[0..ECnt-1] ---+--- HasSnapshot (8) ---+--- snapshot (if present) ---+
```

### Field Offset Table

| Offset | Size | Field      | Go Type | Wire Type | Description                      |
|--------|------|------------|---------|-----------|----------------------------------|
| 0      | 8    | Type       | uint64  | uint64    | Message type constant            |
| 8      | 8    | From       | uint64  | uint64    | Sender replica ID                |
| 16     | 8    | To         | uint64  | uint64    | Recipient replica ID             |
| 24     | 8    | ShardID    | uint64  | uint64    | Raft shard identifier            |
| 32     | 8    | ReplicaID  | uint64  | uint64    | Source replica within shard      |
| 40     | 8    | Term       | uint64  | uint64    | Sender current Raft term         |
| 48     | 8    | LogTerm    | uint64  | uint64    | Term of entry at LogIndex        |
| 56     | 8    | LogIndex   | uint64  | uint64    | Log index (type-specific)        |
| 64     | 8    | Commit     | uint64  | uint64    | Sender commit index              |
| 72     | 8    | Reject     | bool    | uint64    | Rejection flag (0 or 1)          |
| 80     | 8    | Hint       | uint64  | uint64    | Type-specific hint data          |

Fixed header: `11 * 8 = 88 bytes`.

Note that `Reject` is a Go `bool` but encoded as `uint64` on the wire
to maintain fixed-width alignment. The `putBool`/`getBool` functions handle
the conversion.

### Variable Portion

After the 88-byte fixed header:

```
[EntriesCount:4]          -- uint32, number of Entry records
[Entry_0]...[Entry_N]     -- each entry self-describing via its Size()
[HasSnapshot:8]           -- bool as uint64 (0 = no snapshot, 1 = snapshot follows)
[Snapshot]                -- present only if HasSnapshot != 0
```

### Message Type Constants

| Value | Constant             | Description                              |
|-------|----------------------|------------------------------------------|
| 0     | `LocalTick`          | Timer tick (local only)                  |
| 1     | `Election`           | Trigger election (local only)            |
| 2     | `LeaderHeartbeat`    | Leader heartbeat (local only)            |
| 3     | `ConfigChangeEvent`  | Config change event (local only)         |
| 4     | `NoOP`               | No-operation                             |
| 5     | `Ping`               | Liveness probe                           |
| 6     | `Pong`               | Liveness response                        |
| 7     | `Propose`            | Client proposal                          |
| 8     | `SnapshotStatus`     | Snapshot transfer status                 |
| 9     | `Unreachable`        | Peer unreachable notification            |
| 10    | `CheckQuorum`        | Quorum check trigger                     |
| 11    | `BatchedReadIndex`   | Batched read index request               |
| 12    | `Replicate`          | Log replication (AppendEntries)          |
| 13    | `ReplicateResp`      | Replication response                     |
| 14    | `RequestVote`        | Vote request                             |
| 15    | `RequestVoteResp`    | Vote response                            |
| 16    | `InstallSnapshot`    | Snapshot installation                    |
| 17    | `Heartbeat`          | Heartbeat                                |
| 18    | `HeartbeatResp`      | Heartbeat response                       |
| 19    | `ReadIndex`          | Read index request                       |
| 20    | `ReadIndexResp`      | Read index response                      |
| 21    | `Quiesce`            | Quiescence notification                  |
| 22    | `SnapshotReceived`   | Snapshot received confirmation           |
| 23    | `LeaderTransfer`     | Leadership transfer request              |
| 24    | `TimeoutNow`         | Immediate election trigger               |
| 25    | `RequestPreVote`     | Pre-vote request                         |
| 26    | `RequestPreVoteResp` | Pre-vote response                        |
| 27    | `RateLimit`          | Rate limit notification                  |

Validation: `Message.Type > RateLimit` returns `ErrInvalidMessageType`.

### Validation Rules

- Buffer shorter than 92 bytes (88 header + 4 entries count) returns
  `ErrBufferTooSmall`
- `Message.Type > RateLimit` (27) returns `ErrInvalidMessageType`
- `EntriesCount > maxMessagesPerBatch` (10,000) returns `ErrTooManyMessages`
- Each entry is validated recursively via `Entry.UnmarshalFrom`
- Snapshot section requires at least 8 bytes for the `HasSnapshot` flag

### Slice Reuse

`UnmarshalFrom` reuses the existing `Entries` slice capacity when possible:

```go
if cap(m.Entries) >= int(entryCount) {
    m.Entries = m.Entries[:entryCount]
} else {
    m.Entries = make([]Entry, entryCount)
}
```

This avoids allocation when a pooled `Message` already has sufficient
capacity from a previous use.

## MessageBatch Wire Format

Source: `pkg/proto/messagebatch.go`

MessageBatch is the transport-level framing for a batch of Raft messages
sent in a single network write. One batch per QUIC stream write.

```
 0                   8                  16                  24  28
 +-------------------+-------------------+-------------------+----+
 |   BinVer (8)      | DeploymentID (8)  | SourceEpoch (8)   |ALen|
 +-------------------+-------------------+-------------------+----+
28                                       28+ALen    32+ALen
 +---------------------------------------+----------+
 |   SourceAddress (ALen bytes)          | ReqCnt(4)|
 +---------------------------------------+----------+
                                          |
     +--- Request_0 ---+--- Request_1 ---+...+--- Request_N ---+
```

### Field Layout

| Offset   | Size | Field         | Type   | Description                      |
|----------|------|---------------|--------|----------------------------------|
| 0        | 8    | BinVer        | uint64 | Wire format version              |
| 8        | 8    | DeploymentID  | uint64 | Deployment identity token        |
| 16       | 8    | SourceEpoch   | uint64 | Sender's per-process boot epoch  |
| 24       | 4    | AddrLen       | uint32 | Length of SourceAddress          |
| 28       | N    | SourceAddress | string | Sender network address           |
| 28+N     | 4    | ReqCount      | uint32 | Number of Message records        |
| 32+N     | var  | Requests      | []Message | Count-prefixed messages       |

Fixed portion: `messageBatchFixedSize = 3 * 8 = 24 bytes`
(BinVer + DeploymentID + SourceEpoch).
SourceAddress uses the standard `[Len:4][Data:N]` string encoding.

### SourceEpoch

`SourceEpoch` is the sender's per-process boot epoch. It changes only when
the sending node restarts, which lets a receiver tell a genuine peer restart
apart from a benign re-dial and evict the now-stale cached outbound
connection promptly instead of waiting for `MaxIdleTimeout`. See
[Transport](../transport/quic.md) for how the receive path uses it.

### Validation Rules

The receiver validates fields in order during `UnmarshalFrom`:

1. **Wire version**: `BinVer != WireVersion` returns `ErrUnsupportedVersion`.
   The gate is EXACT equality, not `<=`: there is no version negotiation and
   no legacy decode path, so batches from both older AND newer binaries are
   rejected rather than misparsed. BinVer is the FIRST field so the receiver
   rejects incompatible versions before parsing any further data.

2. **Source address length**: `AddrLen > MaxAddressLength` (256) returns
   `ErrAddressTooLong`.

3. **Request count**: `ReqCount > MaxMessagesPerBatch` (10,000) returns
   `ErrTooManyMessages`. The count is then checked for consistency with the
   bytes remaining in the frame -- `ReqCount * minMessageWireSize` must not
   exceed them -- so a short frame claiming thousands of requests cannot
   drive the slice allocation before failing.

4. **Cumulative size**: after unmarshaling each message, the running byte
   offset is checked against `MaxBatchSize` (64 MB). Exceeding returns
   `ErrBatchSizeLimitExceeded`.

5. Each message is validated recursively via `Message.UnmarshalFrom`.

### DeploymentID

The `DeploymentID` field allows cross-deployment message rejection. The
receiver compares the batch's `DeploymentID` against its own configured
value and drops the entire batch on mismatch. This prevents accidental
message routing between independent QuicRaft clusters sharing a network.

## State Wire Format

Source: `pkg/proto/state.go`

State is the Raft hard state: the persistent fields that must survive
a crash. Fixed 24-byte encoding.

```
 0                   8                  16                  24
 +-------------------+-------------------+-------------------+
 |    Term (8)       |    Vote (8)       |   Commit (8)      |
 +-------------------+-------------------+-------------------+
```

| Offset | Size | Field  | Type   | Description                         |
|--------|------|--------|--------|-------------------------------------|
| 0      | 8    | Term   | uint64 | Current Raft term                   |
| 8      | 8    | Vote   | uint64 | Voted-for candidate (0 = no vote)   |
| 16     | 8    | Commit | uint64 | Highest committed log index         |

Total: 24 bytes. `IsEmpty()` returns true when all three fields are zero.

## Snapshot Wire Format

Source: `pkg/proto/snapshot.go`

Snapshot carries metadata for a Raft snapshot. It has a 32-byte fixed
prefix, an inline Membership, and variable-length trailing fields.

```
 0                   8                  16                  24
 +-------------------+-------------------+-------------------+
 |   ShardID (8)     |  ReplicaID (8)    |    Index (8)      |
 +-------------------+-------------------+-------------------+
24                  32
 +-------------------+----------------------------------------------+
 |    Term (8)       |  Membership (variable)                       |
 +-------------------+----------------------------------------------+
                     |
 +--- Filepath (len+data) ---+--- FileSize (8) ---+
 +--- Type (8) ---+--- Imported (8) ---+--- OnDiskIndex (8) ---+
 +--- Witness (8) ---+--- Epoch (8) ---+
```

### Field Layout

| Section    | Field       | Size    | Type   | Description                      |
|------------|-------------|---------|--------|----------------------------------|
| Fixed      | ShardID     | 8       | uint64 | Raft shard identifier            |
| Fixed      | ReplicaID   | 8       | uint64 | Replica within shard             |
| Fixed      | Index       | 8       | uint64 | Last included log index          |
| Fixed      | Term        | 8       | uint64 | Term of last included entry      |
| Variable   | Membership  | var     | struct | Cluster membership (see below)   |
| Variable   | Filepath    | 4+N     | string | Snapshot data file path          |
| Fixed      | FileSize    | 8       | uint64 | Snapshot file size in bytes      |
| Fixed      | Type        | 8       | uint64 | State machine type code          |
| Fixed      | Imported    | 8       | bool   | External import flag             |
| Fixed      | OnDiskIndex | 8       | uint64 | On-disk SM tracked index         |
| Fixed      | Witness     | 8       | bool   | Witness replica flag             |
| Fixed      | Epoch       | 8       | uint64 | Barrier encryption epoch (0 = plaintext) |

Minimum size: 32 bytes (the 4 leading fixed fields). The trailing 5 fixed
fields (Type, Imported, OnDiskIndex, Witness, Epoch) contribute 40 bytes.
`Type` is validated against `maxStateMachineType` on both write and read;
an out-of-range value returns an `*InvalidSnapshotTypeError`.
`IsEmpty()` returns true when both `Index` and `Term` are zero.

### The removed Checksum field

Snapshot carried a `Checksum []byte` between `FileSize` and `Type` through
an earlier pre-release layout, and it was **removed**. It was decoded, bounded
and copied out of the pooled receive buffer on every message, and it was
never computed and never verified anywhere in the repository -- so it was
empty on every real snapshot, and the receive path paid an allocation and
two copies per message to keep an empty slice alive.

It was removed rather than implemented because a field named `Checksum` that
rides the wire and is never checked reads as an integrity control to anyone
auditing the format, and an integrity control that does not exist is worse
than an absent one. Populating it properly is not a codec change: the value
has to be computed over the final artifact bytes (post-compression,
post-encryption), stamped where the artifact is produced, and verified in
three separate consumers (recovery, streamed receive, `ImportSnapshot`).

What snapshot integrity rests on meanwhile, stated plainly: TLS in transit,
and barrier AEAD at rest. A shard configured with a plaintext barrier
(`seal.None`) has **no** at-rest integrity check on its snapshot artifacts.
That was equally true while the field existed.

## Membership Wire Format

Source: `pkg/proto/membership.go`

Membership encodes the cluster configuration: voting members, observers,
witnesses, removed replicas, committed node identities, and the replicated
witness and session policies. All six maps use deterministic sorted-key
encoding.

Membership is the one type in this package that is **persisted** as well as
transmitted -- it is written to the WAL, into snapshot records and exported
snapshot artifacts, and into the `EntryInitialMembership` entry at log index
1. It therefore carries its own layout version byte, independent of
`WireVersion`. See [Version Compatibility](#version-compatibility) below.

The layout, at version **1**:

```
 0     1                   9                  17                  25
 +-----+-------------------+-------------------+-------------------+
 |Ver:1| ConfigChangeID(8) |  MaxWitnesses(8)  |      RFRM(8)      |
 +-----+-------------------+-------------------+-------------------+
25                       33                  41                  49
 +-------------------------+-------------------+-------------------+
 | SessionExpiryEntries(8) | MaxTotalSessions  | MaxSessionsPerCli |
 +-------------------------+-------------------+-------------------+
49
 +-----------------------+
 |  Addresses map        |  -- map[uint64]string, sorted by key
 +-----------------------+
 |  Observers map        |  -- map[uint64]string, sorted by key
 +-----------------------+
 |  Witnesses map        |  -- map[uint64]string, sorted by key
 +-----------------------+
 |  Removed boolMap      |  -- map[uint64]bool, sorted by key
 +-----------------------+
 |  Identities map       |  -- map[uint64]string, sorted by key
 +-----------------------+
 | RemovedIdentities map |  -- map[uint64]string, sorted by key
 +-----------------------+
```

### Fixed Region

| Offset | Size | Field                        | Type   | Description                          |
|--------|------|------------------------------|--------|--------------------------------------|
| 0      | 1    | Version                      | uint8  | Layout version byte (1)              |
| 1      | 8    | ConfigChangeID               | uint64 | Log index that produced this config  |
| 9      | 8    | MaxWitnesses                 | uint64 | Replicated witness cap (0 = none)    |
| 17     | 8    | RequireFullReplicaMajority   | bool   | Replicated full-replica-majority policy |
| 25     | 8    | SessionExpiryEntries         | uint64 | Replicated session inactivity threshold |
| 33     | 8    | MaxTotalSessions             | uint64 | Replicated cap on registered sessions |
| 41     | 8    | MaxSessionsPerClient         | uint64 | Replicated cap on cached responses per session |

Fixed region: `membershipFixedSize = 49 bytes`. An empty membership --
all six maps present but zero-length -- is **73 bytes** (49 + 6 x 4). Every
map is written unconditionally, so an empty map is four zero bytes rather
than an absent field, and an unset session policy is 24 zero bytes rather
than an absent one.

The version byte is the sole layout discriminator. Membership is always
embedded in a larger buffer, so neither a length check nor a trailing-byte
check can tell one layout from another; the explicit byte is the only
in-band signal.

### The identity maps

`Identities` maps replica IDs to the stable cryptographic node identity the
shard has committed for them -- the value derived from a peer's leaf
certificate. It is the authorization key that the address is not: an address
is matched against certificate SANs, so a wildcard or shared-SAN certificate
covers every member's address, while an identity is derived from the peer's
public key.

`RemovedIdentities` maps the replica IDs in `Removed` to the identity the
configuration held for them at the moment of removal. It is the tombstone's
authorization key, and it is what lets the removal deny-list be evaluated
against the authenticated connection identity rather than the claimed
`msg.From`. Entries are never evicted: evicting a tombstone re-opens the
hole for exactly that replica.

Both maps are **sparse**. A shard bootstrapped before identities existed, or
by an operator who supplied none, has no entry for its members. A missing
entry is a compatibility state, never a permission.

### Version Compatibility

`Membership`'s decoder accepts **exactly version 1** and refuses every other
byte value with an `*UnsupportedMembershipVersionError` (which matches
`ErrUnsupportedVersion` under `errors.Is`), before a single field is
interpreted. There is no range, no per-version decode path and no legacy
layout.

That is the pre-release pinning policy: every wire and persisted format in
this repository stays at version 1 until the first production release, a
layout change is made IN PLACE and the consuming repos are rebuilt to match,
and development clusters are wiped and re-synced rather than migrated. See
[Compatibility](../operations/compatibility.md#format-versions-are-pinned-at-1-until-the-first-production-release)
for the full policy and its consequences -- chiefly that two binaries from
different pre-release commits may be mutually unreadable while both stamp 1.

The version byte itself is **kept**, and only its value is pinned. It is the
mechanism that makes the 1.0 bump possible: a format with no in-band
discriminator cannot be superseded, and a section without one would begin at
`ConfigChangeID`, whose low byte is indistinguishable from a version. It also
turns a commit skew into a clean refusal rather than fields read at the wrong
offsets.

`UnmarshalFrom` is **all or nothing**: decoding runs into a local value and
the receiver is assigned only on success, so a failure at any point leaves
the target exactly as it was rather than half-overwritten. On success every
field is overwritten, so a reused receiver cannot retain a stale `Identities`
or `RemovedIdentities` map -- one would authorize a replica the decoded
configuration never named, the other deny a sender it never tombstoned.

**`membershipFormatVersion` is deliberately not derived from `WireVersion`,**
even though both are 1. The two namespaces have different scopes:
`WireVersion` is transport-scoped and never read back off disk, while the
Membership version byte is written into the bootstrap entry at log index 1
and outlives the binary that wrote it. If the persisted version were
`uint8(WireVersion)`, the first post-1.0 transport-only bump would change the
version byte every replica expects at log index 1 and a released cluster
could not replay its own log. A source-level test enforces the separation.

### String Map Encoding (`map[uint64]string`)

```
[Count:4]
  for each entry (sorted by key ascending):
    [Key:8][ValueLen:4][Value:N]
```

- Count validated against `MaxMembershipEntries` (10,000)
- Each value length validated against `MaxAddressLength` (256). This applies
  to `Identities` and `RemovedIdentities` too: `getMap` is shared by all four
  string maps, and `MaxNodeIdentityLength` has the same value, so nothing is
  admitted or refused differently.
- Empty map: 4 bytes (the count prefix alone)

Both limits are mirrored on the **write** side by `MarshalTo`. A codec that
can emit what it cannot parse is a durability bug here, not a style
complaint: `Host.StartShard` copies the caller's members map straight into
`Addresses` and the result is committed at log index 1, so an unchecked
oversized value would produce a bootstrap entry no binary can read back.

### Bool Map Encoding (`map[uint64]bool`)

```
[Count:4]
  for each entry (sorted by key ascending):
    [Key:8][Value:8]
```

- Count validated against `MaxMembershipEntries` (10,000)
- Fixed 16 bytes per entry
- Empty map: 4 bytes

### Deterministic Encoding

Go map iteration is nondeterministic. To guarantee byte-identical output
for identical configurations, `putMap` and `putBoolMap` sort keys ascending
using `sort.Slice` before writing:

```go
func sortedKeys(m map[uint64]string) []uint64 {
    keys := make([]uint64, 0, len(m))
    for k := range m { keys = append(keys, k) }
    sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
    return keys
}
```

This is critical for WAL CRC integrity and for barrier AEAD authentication
-- the same logical membership must always produce the same bytes.

## ConfigChange Wire Format

Source: `pkg/proto/configchange.go`

ConfigChange is serialized as the `Cmd` payload of an `EntryConfigChange`
entry. It describes a membership change operation.

```
 0                   8                  16                  24
 +-------------------+-------------------+-------------------+
 | ConfigChangeID(8) |    Type (8)       |  ReplicaID (8)    |
 +-------------------+-------------------+-------------------+
24       28
 +-------+-----------------------+-------------------+
 |AdrLen |  Address (N bytes)    |  Initialize (8)   |
 +-------+-----------------------+-------------------+
 +--- MaxWitnesses (8) ---+--- RequireFullReplicaMajority (8) ---+
 +--- IdLen (4) ---+--- NodeIdentity (M bytes) ---+
```

| Offset | Size | Field                      | Type   | Description                     |
|--------|------|----------------------------|--------|---------------------------------|
| 0      | 8    | ConfigChangeID             | uint64 | Log index of this change        |
| 8      | 8    | Type                       | uint64 | See the type table below        |
| 16     | 8    | ReplicaID                  | uint64 | Target replica (0 for `UpdateWitnessPolicy`) |
| 24     | 4+N  | Address                    | string | Network address (empty for remove) |
| 28+N   | 8    | Initialize                 | bool   | Needs initial state flag        |
| 36+N   | 8    | MaxWitnesses               | uint64 | New witness cap for `UpdateWitnessPolicy` |
| 44+N   | 8    | RequireFullReplicaMajority | bool   | New policy for `UpdateWitnessPolicy` |
| 52+N   | 4+M  | NodeIdentity               | string | Committed node identity (always present; `M` may be 0) |

Fixed portion: `configChangeFixedSize = 6*8 + 4 = 52 bytes` -- the six
uint64-width fields plus the 4-byte address length prefix. The trailing
`NodeIdentity` adds its own 4-byte length prefix plus its bytes. The prefix is
**unconditional**: an empty identity is four zero bytes, never a shorter buffer.

`NodeIdentity` is the stable cryptographic node identity to commit for the
replica being added. It travels through consensus alongside `Address` so the
receive-side gate reads a value no inbound message can write. It is supplied
by the operator through the Host API, never captured from a connection.
It is empty for `RemoveNode` and `UpdateWitnessPolicy`, and empty for an add
whose caller supplied none.

### One layout, every field mandatory

`ConfigChange` carries no version byte and needs none: there is exactly one
layout to decode, so there is nothing for a discriminator to discriminate.
`UnmarshalFrom` refuses any payload that does not span the whole layout with
`ErrBufferTooSmall` -- at every cut point, including the two that used to mean
something else.

This replaces a LENGTH-discriminated scheme, and the replacement closes a
defect rather than merely retiring dead code. A payload ending after
`RequireFullReplicaMajority` used to decode successfully with an empty
`NodeIdentity`, and an empty identity is exactly the compatibility state that
falls back to the weaker address-based authorization -- so a **truncated**
`ConfigChange` was byte-indistinguishable from a legitimate identity-less one,
and truncation silently downgraded an authorization check. A payload ending
after `Initialize` used to be reported as a pre-witness-policy layout with
`ErrUnsupportedVersion`; under the pinning policy there is no other layout to
name, so it too is simply short.

An identity-less config change is still entirely legal and still commits --
`RemoveNode`, `UpdateWitnessPolicy` and an add made without
`RequestAddNodeWithIdentity` all produce one. It is written with an explicit
zero length prefix, so what the decoder sees is a value the writer chose rather
than a length the reader inferred.

Config change type constants:

| Value | Constant              | Description                                    |
|-------|-----------------------|------------------------------------------------|
| 0     | `AddNode`             | Add voting member                              |
| 1     | `RemoveNode`          | Remove replica                                 |
| 2     | `AddNonVoting`        | Add non-voting observer                        |
| 3     | `AddWitness`          | Add witness replica                            |
| 4     | `UpdateWitnessPolicy` | Replace the shard's replicated witness policy  |

`UpdateWitnessPolicy` carries no target replica; the `MaxWitnesses` and
`RequireFullReplicaMajority` fields hold the new values. A `Type` above
`UpdateWitnessPolicy` is rejected with `ErrInvalidConfigChangeType` on both
write and read.

`MarshalTo` mirrors the read side's per-field bounds --
`ErrInvalidConfigChangeType`, `ErrAddressTooLong` (`MaxAddressLength`, 256)
and `ErrNodeIdentityTooLong` (`MaxNodeIdentityLength`, 256). These are not
cosmetic: a `ConfigChange` is marshaled into an `Entry.Cmd` and committed, so
a payload this package can emit but not parse is a committed entry every
replica faults on -- a self-inflicted cluster-wide outage from an
over-length operator-supplied address.

## ReadyToRead Wire Format

Source: `pkg/proto/readindex.go`

ReadyToRead represents a confirmed linearizable read. Fixed 16-byte
encoding.

```
 0                   8                  16
 +-------------------+-------------------+
 |    Key (8)        |    Index (8)      |
 +-------------------+-------------------+
```

| Offset | Size | Field | Type   | Description                          |
|--------|------|-------|--------|--------------------------------------|
| 0      | 8    | Key   | uint64 | Client request correlation key       |
| 8      | 8    | Index | uint64 | Committed index safe for read        |

## UpdateCommit Wire Format

Source: `pkg/proto/update.go`

UpdateCommit tracks commit progress between the engine commit and apply
workers. Fixed 48-byte encoding.

```
 0                   8                  16                  24
 +-------------------+-------------------+-------------------+
 | StableLogTo (8)   | StableLogTerm (8) |StableSnapshotTo(8)|
 +-------------------+-------------------+-------------------+
24                  32                  40                  48
 +-------------------+-------------------+-------------------+
 |  Processed (8)    | LastApplied (8)   | ReadyToRead (8)   |
 +-------------------+-------------------+-------------------+
```

| Offset | Size | Field            | Go Type | Wire Type | Description                    |
|--------|------|------------------|---------|-----------|--------------------------------|
| 0      | 8    | StableLogTo      | uint64  | uint64    | Highest persisted log index    |
| 8      | 8    | StableLogTerm    | uint64  | uint64    | Term at StableLogTo            |
| 16     | 8    | StableSnapshotTo | uint64  | uint64    | Persisted snapshot index       |
| 24     | 8    | Processed        | bool    | uint64    | Update fully processed flag    |
| 32     | 8    | LastApplied      | uint64  | uint64    | Highest applied log index      |
| 40     | 8    | ReadyToRead      | uint64  | uint64    | Confirmed read index count     |

**Important**: `Processed` is a Go `bool` (not `uint64`). It is a flag
indicating the update was processed, not an index. `LastApplied` is the
actual applied index (`uint64`). On the wire, `Processed` is stored as
`uint64` (0 or 1) via `putBool`/`getBool` for fixed-width alignment.

## Update (In-Process Only)

Source: `pkg/proto/update.go`

The `Update` struct is the primary data structure flowing between the
engine step, commit, and apply workers. It is **never serialized to the
wire** -- it is passed by reference within the same process. It aggregates:

- `EntriesToSave` -- log entries to persist
- `CommittedEntries` -- entries ready to apply to the state machine
- `Messages` -- Raft messages to send via transport
- `Snapshot` -- snapshot metadata to persist
- `UpdateCommit` -- commit tracking state
- `ReadyToRead` -- confirmed read index requests
- `ReadOnly` -- flag for engine to bypass commit/apply pipeline

The `ReadOnly` flag is set by `Peer.GetUpdate` when the update contains
only `ReadyToRead` states with no entries, state changes, or snapshots.
The step worker bypasses the commit/apply pipeline for read-only updates.

## SnapshotFile Wire Format

Source: `pkg/proto/snapshotfile.go`

SnapshotFile carries metadata for an external file included in a snapshot.

```
 +--- Filepath (len+data) ---+--- FileSize (8) ---+--- FileID (8) ---+--- Metadata (len+data) ---+
```

| Section  | Field    | Size | Type   | Description                       |
|----------|----------|------|--------|-----------------------------------|
| Variable | Filepath | 4+N  | string | Path to external file             |
| Fixed    | FileSize | 8    | uint64 | File size in bytes                |
| Fixed    | FileID   | 8    | uint64 | Unique file identifier            |
| Variable | Metadata | 4+N  | []byte | Application-defined metadata      |

## SnapshotChunk Wire Format

Source: `pkg/proto/chunk.go`

SnapshotChunk is used for streaming snapshots between nodes via the
transport layer. It has a large variable-size layout with inline
Membership and optional FileInfo.

```
 0                   8                  16                  24
 +-------------------+-------------------+-------------------+
 |   ShardID (8)     |  ReplicaID (8)    |    From (8)       |
 +-------------------+-------------------+-------------------+
24                  32                  40                  48
 +-------------------+-------------------+-------------------+
 |    Index (8)      |    Term (8)       |   ChunkID (8)     |
 +-------------------+-------------------+-------------------+
48                  56                  64                  72
 +-------------------+-------------------+-------------------+
 |  ChunkCount (8)   |  ChunkSize (8)    |   FileSize (8)    |
 +-------------------+-------------------+-------------------+
72
 +--- Filepath (len+data) ---+
 +--- FileChunkID (8) ---+--- FileChunkCount (8) ---+--- HasFileInfo (8) ---+
 +--- FileInfo (var, if HasFileInfo) ---+
 +--- Membership (var) ---+
 +--- OnDiskIndex (8) ---+--- DeploymentID (8) ---+--- BinVer (8) ---+
 +--- Epoch (8) ---+
 +--- Data (len+data) ---+
```

Minimum header: 72 bytes (9 fixed uint64 fields before the first
variable-length Filepath).

`Epoch` sits between `BinVer` and `Data`. It is the barrier encryption epoch
the snapshot was saved under (0 = plaintext), and the receiver needs it to
decrypt barrier-encrypted snapshot data. Omitting it from a decoder reads
the length prefix of `Data` out of the epoch field, so the chunk payload is
mis-sized rather than merely mis-labeled.

`BinVer` is gated on **exact** equality with `WireVersion`, mirroring
`MessageBatch`. This is a deliberate choice and not the persisted-format
oversight the Membership version byte closes: `SnapshotChunk` is
transport-only. The receiver keeps only `chunk.Data` (written to
`snapshot.dat`) and re-derives the metadata into a `logdb.Snapshot` record in
waldb's own layout, so no marshaled `SnapshotChunk`, and no `BinVer` byte,
is ever written to disk. `BinVer` is required: an unstamped chunk (`BinVer`
0) is indistinguishable from a v1 sender's.

`ChunkSize` is redundant with the length prefix on `Data`. When a sender
populates it (non-zero), the receiver requires it to agree with the actual
payload length and returns `ErrChunkSizeMismatch` otherwise. A zero value
means "unspecified" and consumers fall back to `len(Data)`.

## Bootstrap Wire Format

Source: `pkg/proto/bootstrap.go`

Bootstrap contains the initial cluster configuration persisted to LogDB
during shard initialization.

```
 +--- Addresses map ---+--- Join (8) ---+--- Type (8) ---+
```

| Section  | Field     | Size | Type              | Description                     |
|----------|-----------|------|-------------------|---------------------------------|
| Variable | Addresses | var  | map[uint64]string | Initial replica-to-address map  |
| Fixed    | Join      | 8    | bool              | Joining existing cluster flag   |
| Fixed    | Type      | 8    | uint64            | State machine type code         |

State machine type constants:

| Value | Constant                   | Description           |
|-------|----------------------------|-----------------------|
| 0     | `RegularStateMachine`      | In-memory SM          |
| 1     | `ConcurrentStateMachine`   | Concurrent SM         |
| 2     | `OnDiskStateMachine`       | Persistent SM         |

## Size Limits and Constants

| Constant                  | Value      | Enforced In              | Description                      |
|---------------------------|------------|--------------------------|----------------------------------|
| `WireVersion`             | 1          | `MessageBatch.UnmarshalFrom`, `SnapshotChunk.UnmarshalFrom` | Current transport wire format version (exact match) |
| `MaxEntrySize`            | 8 MB       | `Entry.UnmarshalFrom`    | Max `Entry.Cmd` payload          |
| `MaxBatchSize`            | 64 MB      | `MessageBatch.UnmarshalFrom` | Max cumulative batch size    |
| `MaxAddressLength`        | 256 bytes  | `getString`, `getMap`    | Max node address string          |
| `MaxNodeIdentityLength`   | 256 bytes  | `ConfigChange`           | Max committed node identity string |
| `MaxFilepathLen`          | 4096 bytes | `Snapshot`, `SnapshotChunk`, `SnapshotFile` | Max filepath string |
| `MaxMetadataLen`          | 64 KB      | `SnapshotFile.UnmarshalFrom` | Max application metadata     |
| `MaxSnapshotChunkDataLen` | 8 MB       | `SnapshotChunk.UnmarshalFrom` | Max chunk payload (= `MaxEntrySize`) |
| `MaxMessagesPerBatch`     | 10,000     | `MessageBatch.UnmarshalFrom` | Max messages per batch       |
| `MaxEntriesPerMessage`    | 10,000     | `Message.UnmarshalFrom`  | Max log entries per message      |
| `MaxMembershipEntries`    | 10,000     | `getMap`, `getBoolMap`   | Max entries per membership map   |

All limits are validated **before** allocation during `UnmarshalFrom`.
This prevents a crafted payload from exhausting memory by claiming a
large count and triggering a proportional `make()`.

`MaxMessagesPerBatch`, `MaxEntriesPerMessage` and `MaxMembershipEntries` are
**exported** because they are properties of the wire protocol, not of the
decoder. A producer that exceeds one emits a payload every peer rejects, and
a rejection is indistinguishable from a broken connection at the sender.
Producers MUST bound themselves by these values.

## MarshalTo / UnmarshalFrom Pattern

Every serializable type follows the same interface contract:

```go
// MarshalTo writes the struct into buf and returns bytes written.
// buf must be at least Size() bytes.
func (t *T) MarshalTo(buf []byte) (int, error)

// UnmarshalFrom reads the struct from buf and returns bytes consumed.
func (t *T) UnmarshalFrom(buf []byte) (int, error)

// Size returns the total marshaled size in bytes.
func (t *T) Size() int

// Reset clears all fields for reuse (pool-friendly).
func (t *T) Reset()
```

### Buffer Management

The caller is responsible for providing a buffer of sufficient size.
The pattern is:

```go
buf := make([]byte, entry.Size())
n, err := entry.MarshalTo(buf)
// buf[:n] contains the serialized data
```

For nested types, `MarshalTo` recurses into child types and tracks a
running offset:

```go
offset := 32
n, err := s.Membership.MarshalTo(buf[offset:])
offset += n
offset += putString(buf[offset:], s.Filepath)
```

### Validation During Unmarshal

Every `UnmarshalFrom` validates input before use:

1. **Minimum size check** -- the buffer must be at least as large as the
   fixed header before reading any field.

2. **Count/length limits** -- variable-length counts are checked against
   hard limits before allocation (`MaxEntrySize`, `MaxMessagesPerBatch`,
   `MaxEntriesPerMessage`, `MaxMembershipEntries`, `MaxAddressLength`).

3. **Enum validation** -- `Entry.Type` and `Message.Type` are checked
   against the known range after decoding.

4. **Cumulative size tracking** -- `MessageBatch.UnmarshalFrom` tracks the
   running byte offset and validates against `MaxBatchSize` after each
   message.

5. **Incremental bounds checks** -- between variable-length sections, the
   remaining buffer length is re-checked before reading the next field.

### Error Types

All errors are package-level sentinel values (not `fmt.Errorf`):

| Error                          | Condition                                        |
|--------------------------------|--------------------------------------------------|
| `ErrBufferTooSmall`            | Input buffer shorter than required                |
| `ErrInvalidEntryType`          | Unknown `Entry.Type` (after masking)              |
| `ErrInvalidMessageType`        | Unknown `Message.Type`                            |
| `ErrInvalidConfigChangeType`   | `ConfigChange.Type` above `UpdateWitnessPolicy`   |
| `ErrAddressTooLong`            | Address string exceeds `MaxAddressLength`         |
| `ErrNodeIdentityTooLong`       | Node identity exceeds `MaxNodeIdentityLength`     |
| `ErrFilepathTooLong`           | Filepath exceeds `MaxFilepathLen`                 |
| `ErrMetadataTooLong`           | Metadata exceeds `MaxMetadataLen`                 |
| `ErrChunkDataTooLong`          | Chunk data exceeds `MaxSnapshotChunkDataLen`      |
| `ErrChunkSizeMismatch`         | `SnapshotChunk.ChunkSize` disagrees with `len(Data)` |
| `ErrEntrySizeLimitExceeded`    | `Entry.Cmd` length exceeds `MaxEntrySize`         |
| `ErrBatchSizeLimitExceeded`    | Cumulative batch size exceeds `MaxBatchSize`      |
| `ErrTooManyMessages`           | Message/entry count exceeds limit                 |
| `ErrTooManyMembershipEntries`  | Membership map count exceeds limit                |
| `ErrDuplicateMapKey`           | A membership map repeats a key                    |
| `ErrUnsupportedVersion`        | `BinVer != WireVersion`, or a `Membership` section version byte that is not 1 |

Two errors are typed rather than sentinel, and both match a sentinel under
`errors.Is`:

| Error                                 | Matches                    | Condition                                          |
|---------------------------------------|----------------------------|----------------------------------------------------|
| `*UnsupportedMembershipVersionError`  | `ErrUnsupportedVersion`    | Membership version byte is not `1`; names the version found and the one supported |
| `*InvalidSnapshotTypeError`           | `ErrInvalidSnapshotType`   | `Snapshot.Type` above `maxStateMachineType`        |

## Zero-Copy Semantics

### Entry.Cmd Aliasing

After `Entry.UnmarshalFrom`, the `Cmd` slice points directly into the
source buffer:

```go
if cmdLen > 0 {
    e.Cmd = buf[60:total] // zero-copy: aliases source buffer
}
```

This avoids copying command payloads (which can be up to 8 MB) during
deserialization. The trade-off is that the source buffer cannot be
returned to a pool or reused until all references to `Cmd` are released.

### When Copies Are Required

The transport receive path (`internal/transport/recv.go`) must copy
`Entry.Cmd` before returning the receive buffer to the pool. Without
the copy, a slow consumer could hold a reference to `Cmd` that aliases
a buffer already reused by the next receive.

### Buffer Pooling Implications

The zero-copy design interacts with `sync.Pool` buffer management:

1. **Send path**: the caller allocates a buffer via `Size()`, calls
   `MarshalTo`, sends the buffer, then returns it to the pool. No
   aliasing concerns because the data is fully written.

2. **Receive path**: after `UnmarshalFrom`, any `Entry.Cmd` slices alias
   the receive buffer. The transport layer must `copy()` these slices
   before returning the buffer to the pool.

3. **WAL path**: the WAL writes the marshaled buffer to disk and can
   immediately return it. The on-disk data is self-contained.

### getBytes vs getString

`getBytes` returns a zero-copy alias: `buf[4:total]`. This is used for
binary data like `SnapshotFile.Metadata` and `SnapshotChunk.Data`.

`getString` returns a copied string: `string(buf[4:total])`. This is
required because Go strings are immutable and must not alias mutable
buffers.

## Buffer Ownership Rules

Zero-copy unmarshal creates aliased references into shared buffers. Each
pipeline stage has distinct ownership semantics that must be respected to
prevent use-after-free and data corruption.

```
Transport receive buffer --> UnmarshalFrom --> Entry.Cmd aliases buffer
                                                   |
                                                   +-- Safe: read within same worker tick
                                                   +-- Safe: marshal into LogDB write buffer (copies data)
                                                   +-- UNSAFE: hold reference across tick boundary
```

### Transport Boundary (Inbound)

After `UnmarshalFrom`, `Entry.Cmd` aliases the transport receive buffer.
The transport receive path (`pkg/internal/transport/recv.go`) explicitly
copies every `Entry.Cmd` before returning the receive buffer to the pool:

```go
for i := range batch.Requests {
    for j := range batch.Requests[i].Entries {
        cmd := batch.Requests[i].Entries[j].Cmd
        if len(cmd) > 0 {
            cp := make([]byte, len(cmd))
            copy(cp, cmd)
            batch.Requests[i].Entries[j].Cmd = cp
        }
    }
}
```

Without this copy, a slow consumer could read stale or overwritten `Cmd`
data from a buffer already reused by the next receive.

### LogDB Boundary (Outbound)

When `LogDB.Entries()` returns entries, `Entry.Cmd` aliases the LogDB read
buffer. The caller must copy `Cmd` if the data is needed beyond the current
operation. The Raft protocol layer copies Cmd data into outbound Message
entries via `MarshalTo`.

### State Machine Boundary

Entries passed to `StateMachine.Update()` have `Cmd` data that aliases the
apply worker's buffer. The state machine must copy `Cmd` if the data is
needed after returning from `Update()`. This is documented in the
`sm.StateMachine` interface contract.

### Snapshot Boundary

Snapshot data is streamed through `io.Reader`/`io.Writer`. No aliasing
occurs -- data is copied through the stream.

### Key Invariant

No `Entry.Cmd` reference survives beyond the scope of the function that
received it unless explicitly copied. Each pipeline stage (step, commit,
apply) marshals or copies data at boundaries.

## CmdLen Field: uint32 vs uint64

The `Cmd` length prefix uses `uint32` (4 bytes, max ~4 GB). `MaxEntrySize`
is 8 MB. Using `uint64` would add 4 bytes to every entry for a limit that
is 500x beyond the configured maximum. If `MaxEntrySize` ever needs to
exceed 4 GB (highly unlikely for Raft proposals), the `WireVersion`
mechanism can introduce a new encoding with a `uint64` length field.
Until then, `uint32` saves 4 bytes per entry on the hot path.

Source: `pkg/proto/constants.go` -- `MaxEntrySize uint32 = 8 * 1024 * 1024`

## MessageBatch Size Enforcement

`MessageBatch.UnmarshalFrom` tracks the running byte offset after each
message and compares it against `MaxBatchSize` (64 MB). The comparison
uses `uint64` arithmetic to prevent silent truncation:

```go
if uint64(offset) > MaxBatchSize {
    return 0, ErrBatchSizeLimitExceeded
}
```

`MaxBatchSize` is declared as `uint64` (not `uint32`) because the
cumulative offset of a batch with many large messages can exceed 4 GB.
A `uint32` cast of the offset would silently wrap, bypassing the size
limit entirely. The `uint64` comparison ensures the check is accurate
on all platforms.

Source: `pkg/proto/messagebatch.go`, `pkg/proto/constants.go`

## Wire Format Versioning

**Every version in this package is pinned at 1 for the whole pre-release
period, and every gate on it is an exact match.** A layout change is made IN
PLACE at version 1, the consuming repos are rebuilt to match, and dev
clusters are wiped and re-synced rather than migrated. The numbers increment
for the first time at the first production release. The consequence is
intended and worth stating plainly: two binaries from different pre-release
commits may be mutually unreadable while both stamp 1. See
[Compatibility](../operations/compatibility.md#format-versions-are-pinned-at-1-until-the-first-production-release).

Within that, the package still carries **two independent version
namespaces**. Neither may be derived from the other, and a bump to one must
never change how the other is read.

**Transport-scoped: `WireVersion` = 1.** Stamped into `MessageBatch.BinVer`
and `SnapshotChunk.BinVer`, and mirrored by the frame header byte in
`internal/transport`. Both ends of a QUIC connection are live processes, so a
mismatch costs one rejected connection and nothing durable. Gated on **exact
equality** -- a peer speaking another dialect is refused outright rather than
misparsed.

**Persisted-scoped: the `Membership` version byte = 1.** Written to the WAL,
into snapshot records and exported snapshot artifacts, and into the
`EntryInitialMembership` entry at log index 1. Those bytes outlive the
process -- and the binary -- that wrote them, which is why the format carries
an explicit discriminator at all. Also gated on exact equality.

The distinction is not stylistic, and it is why the two survive as separate
constants while holding the same value. If the persisted version were derived
from `WireVersion`, the first post-1.0 transport-only bump would
simultaneously make every existing node unable to replay its own log -- with
no dual-read path and no migration, a permanently unstartable cluster.

### The transport version is a SUPREMUM

Independent does not mean unrelated. `Membership` is embedded in `Snapshot`,
which is embedded in `Message`, which is embedded in `MessageBatch`, and it
is embedded directly in `SnapshotChunk`. Both of those gate on exact
equality, so a `Membership` layout change alters the transport bytes without
altering the discriminator that is supposed to describe them.

**The rule: bump `WireVersion` whenever any type reachable from
`MessageBatch` or `SnapshotChunk` changes layout, including the embedded
persisted ones.** The transport number is the supremum of every layout
version on the wire, never a number that only tracks the transport-only
fields. A compile-time constant assertion in `membership.go` enforces
`WireVersion >= membershipFormatVersion`, so the `Membership` half of that is
mechanical rather than remembered. It holds trivially at `1 >= 1` and is kept
for exactly that future bump.

The converse does not hold: `WireVersion` may move without the persisted
version moving, because a transport-only change leaves every persisted byte
alone. That asymmetry is why the assertion is `>=` and not `==`.

### Upgrade Strategy

Because the transport gate is exact equality, there is **no rolling upgrade
across a `WireVersion` bump** and no mixed-version steady state. A cluster
spanning two wire versions does not degrade -- it stops exchanging Raft
messages entirely. The upgrade procedure is a rolling *replacement*, not a
rolling *restart*.

Pre-release the same is true across a LAYOUT change even though the number
does not move -- and it does not announce itself as a version mismatch, so
the procedure wipes each node's data directory rather than relying on the
gate to catch the skew.

See [Compatibility](../operations/compatibility.md) for the authoritative
per-release procedure and the version history.

### No Reserved Padding

The fixed-layout encoding does not include reserved padding bytes for
future fields. When a new field is needed, the relevant version constant is
incremented. Padding would waste space on every entry in the hot path -- at
100K entries/sec, even 8 bytes of padding equals 800 KB/sec of wasted I/O
and storage.

## Cross-References

- [Architecture Overview](../architecture/overview.md) -- system overview and data flow
- [WAL Persistence](../storage/wal.md) -- how serialized entries are stored
- [Engine Pipeline](../engine/pipeline.md) -- Update flow between workers
- [Transport](../transport/quic.md) -- how MessageBatch frames are sent via QUIC
- [Session](../sessions/deduplication.md) -- session management and Entry dedup fields
