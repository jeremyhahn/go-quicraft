# Wire Protocol — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/proto` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library, so the Go API is the API. For this feature that is
unusually literal: the "transport" is the byte layout below.

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the codec surface below |
| **QUIC** | the layouts below **are** what a QUIC frame carries. Framing, the frame header byte, and connection handling belong to [`../quic-transport/api.md`](../quic-transport/api.md); this package produces and consumes the payload. |
| Unix socket | n/a — this repository serves no socket |
| REST | n/a — this repository exposes no HTTP handler |
| gRPC | n/a — this repository defines no gRPC service, and deliberately does not: see [`architecture.md`](architecture.md) |
| MCP | n/a — this repository registers no MCP tools |

These are not missing capabilities: the codec is consumed in-process by the
transport and the WAL, not called by a client.

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg/proto`

### The codec contract every serializable type implements

```go
func (t *T) MarshalTo(buf []byte) (int, error)   // buf must be >= Size()
func (t *T) UnmarshalFrom(buf []byte) (int, error)
func (t *T) Size() int
func (t *T) Reset()                              // pool-friendly
```

| Type | Fixed header | Carries |
|---|---|---|
| `Entry` | 68 | the unit of replication: `Term`, `Index`, `Type`, `Key`, `ClientID`, `SeriesID`, `RespondedTo`, `OwnerID`, `CmdLen`, `Cmd` |
| `Message` | 88 | one Raft message, plus `Entries` and an inline `Snapshot` |
| `MessageBatch` | 24 | `BinVer`, `DeploymentID`, `SourceEpoch`, `SourceAddress`, `Requests` |
| `State` | 32 (whole) | persisted `Term`, `Vote`, `Commit`, `VoteWindow` |
| `Snapshot` | 32 | position (`ShardID`, `ReplicaID`, `Index`, `Term`), embedded `Membership`, `Filepath`, `FileSize`, `OnDiskIndex`, `Epoch`, `Compression` |
| `SnapshotFile` | — | one file within a snapshot, with application `Metadata` |
| `SnapshotChunk` | 72 | one streamed chunk, with `BinVer` and embedded `Membership` |
| `Membership` | 57 | version byte, addresses, observers, witnesses, removed set, session policy, identity maps |
| `ConfigChange` | 52 | a membership change proposal, with the added member's `NodeIdentity` |
| `ReadyToRead` | 16 (whole) | a resolved ReadIndex |
| `UpdateCommit` | 48 (whole) | commit bookkeeping |
| `Update` | — | **in-process only**; not a wire type |

Full field-offset tables, validation rules and per-type semantics are in
[`engineering.md`](engineering.md). Read offsets from there, not from memory:
a layout change before the first production release is made in place.

### Capability: encode a value

```go
buf := make([]byte, entry.Size())
n, err := entry.MarshalTo(buf)
// buf[:n] is the serialized form
```

| | |
|---|---|
| Request | a destination buffer of at least `Size()` bytes |
| Response | bytes written |
| Errors | `ErrBufferTooSmall`, plus every bound the type enforces on the write side: `ErrEntrySizeLimitExceeded`, `ErrInvalidEntryType`, `ErrTooManyMessages`, `ErrAddressTooLong`, … |
| Permission | none; in-process call |

**Write-side validation is not defensive programming here.** An entry is
committed to the Raft log before any peer parses it, so an encoder that could
emit a type its own decoder rejects would write an entry every replica faults on
forever.

### Capability: decode a value

```go
var e proto.Entry
n, err := e.UnmarshalFrom(buf)
```

| | |
|---|---|
| Request | a source buffer. **Treated as hostile.** |
| Response | bytes consumed |
| Errors | see the error table in [`engineering.md`](engineering.md#error-types) |
| Permission | none |
| Caller obligation | `Entry.Cmd`, `SnapshotChunk.Data` and `SnapshotFile.Metadata` **alias `buf`**. Copy before returning `buf` to a pool. |

### Capability: pool entry slices

```go
func GetEntrySlice() *[]Entry
func PutEntrySlice(s *[]Entry)
```

Reuse for the hot decode path. Anything aliasing a pooled buffer must be copied
first; see the buffer-ownership rules in
[`engineering.md`](engineering.md#buffer-ownership-rules).

### Capability: name a constant

```go
func MessageTypeName(t uint64) string        // "Unknown(N)" for unrecognized
func ConfigChangeTypeName(t uint64) string
func StateMachineTypeName(smType uint64) string
func IsFreeOrderMessage(msgType uint64) bool
```

`IsFreeOrderMessage` is the one with semantics rather than presentation: it
reports whether a message may be sent **before** the state it promises is
durable. The engine sends free-order messages from the step worker and holds the
rest for the commit worker.

### Constants: the protocol's bounds

```go
const WireVersion uint64 = 1

const MaxEntrySize            uint32 = 8 * 1024 * 1024
const MaxBatchSize            uint64 = 64 * 1024 * 1024
const MaxSnapshotChunkDataLen uint32 = MaxEntrySize
const MaxMetadataLen          uint32 = 65536
const MaxAddressLength               = 256
const MaxNodeIdentityLength          = 256
const MaxFilepathLen                 = 4096
const MaxMessagesPerBatch            = 10000
const MaxEntriesPerMessage           = 10000
const MaxMembershipEntries           = 10000
const MaxDecodeMemoryFactor          = 8
const HeartbeatMessageSize           = messageFixedSize + 4 + 8   // 100 bytes
const MaxTerm                 uint64 = math.MaxUint64 - 1024
const MaxLogIndex             uint64 = math.MaxUint64 - 1024
```

`MaxTerm` and `MaxLogIndex` are protocol bounds rather than decode limits: the
codec accepts any `uint64`, and the values are refused where they are admitted:
`raft.step` for a wire term, `Snapshot.CheckBounds` on every snapshot
admission path (chunk validation, LogDB `SaveSnapshot` and `SaveState`,
`handleSnapshot`), and a leader's proposals once its log reaches
`MaxLogIndex`, with `ErrTermOutOfRange` / `ErrLogIndexOutOfRange`. Persisted
state is refused at open only at the ceiling itself, a few below `MaxUint64`;
the headroom between the two is what local `term+1` / `index+1` transitions
spend without ever wrapping.

`MaxMessagesPerBatch`, `MaxEntriesPerMessage` and `MaxMembershipEntries` are
**exported because they are properties of the wire protocol, not of the
decoder**. A producer that exceeds one emits a payload every peer rejects, and a
rejection is indistinguishable from a broken connection at the sender.
**Producers MUST bound themselves by these values.**

`MaxDecodeMemoryFactor` is not checked by the codec. It is the measured
heap-per-wire-byte ceiling of a decode, and the transport charges every inbound
frame at that multiple against `TransportConfig.MaxMessageReceiveMemory`, so
the budget bounds decoded heap. `DecodeCharge(n)` is that charge for `n` wire
bytes, saturating at `MaxInt64`, and `FrameDecodeCharge(wireLen, decodedLen)` is
the charge for one frame including its decompression delta. The default budget
is four maximum-size frame charges, 512 MB, which admits at most 64 MB of
frames in flight.

### Entry types and masks

```go
const EntryNormal            uint64 = 0
const EntryConfigChange      uint64 = 1
const EntryEncodedEntry      uint64 = 2
const EntryMetadata          uint64 = 3
const EntryInitialMembership uint64 = 4

const EntryTypeMask     uint64 = 0x0F
const EntryCompressFlag uint64 = 0x10
```

**Always mask before comparing**: `entryType := e.Type & EntryTypeMask`. Bits 5
and above are reserved and **rejected**, not ignored, at both ends.

A reader that omits `EntryInitialMembership` cannot replay **any** shard's log:
every shard writes that entry at index 1 on first start.

### Session sentinels

```go
const NotSessionManagedClientID    uint64 = 0
const SessionSeriesIDForRegister   uint64 = math.MaxUint64 - 1
const SessionSeriesIDForUnregister uint64 = math.MaxUint64
```

(`quicraft.SeriesIDForRegister`, in `pkg`, is a separate constant on the
public API, not this package's.)

See [`../client-sessions/engineering.md`](../client-sessions/engineering.md).

### Message origin: process-local, never on the wire

```go
type MessageOrigin uint8
const OriginRemote MessageOrigin = iota
```

`Message.Origin` is a twelfth field that `MarshalTo` never writes,
`UnmarshalFrom` sets to `OriginRemote`, and `Reset` zeroes. `OriginRemote` is
the zero value, so a message decoded from the network, or recycled through the
pool, is never mistaken for a locally generated one. Putting it on the wire
would let a sender assert its own provenance.

### Error vocabulary

All sentinels, matched with `errors.Is`. One typed error,
`*UnsupportedMembershipVersionError`, matches `ErrUnsupportedVersion` under
`errors.Is` and names both the version found and the one supported. Full table
in [`engineering.md`](engineering.md#error-types).

## Versioning and deprecation

- `WireVersion` = 1, transport-scoped, **exact match** at
  `MessageBatch.UnmarshalFrom` and `SnapshotChunk.UnmarshalFrom`.
- The `Membership` section version byte = 1, persisted-scoped, **exact match**.
  It is a separate namespace and must not be derived from `WireVersion`.
- **Both are pinned at 1 until the first production release.** A layout change
  is made in place, at version 1, with every consuming repository updated in the
  same change. Two binaries from different pre-release commits may be mutually
  unreadable while both stamp 1.
- From the first production release: **bump `WireVersion` whenever any type
  reachable from `MessageBatch` or `SnapshotChunk` changes layout, including the
  embedded persisted ones.** It is a supremum, not a transport-only counter.
- `Update` is in-process only and carries no version.
- Nothing in this package is deprecated.

The repository-wide policy and per-release upgrade procedure are in
[`../compatibility.md`](../compatibility.md).
